//! Bringing the client up: where the bundled helpers are, starting the core,
//! forwarding its events, and the replay a double-click asked for.

use std::sync::Arc;

use faf_app::App;
use faf_domain::state::{
    AuthCommand, NavCommand, ReplayCommand, SessionCommand, SettingsCommand, Tab,
};
use faf_domain::AppCommand;
use tauri::{Emitter, Manager};
use tokio::sync::broadcast::error::RecvError;

use crate::{Core, FrontendMessage, EVENT_CHANNEL};

/// The replay file this process was asked to open, if it was asked to open one.
///
/// A file association starts the client with the path as an argument, and
/// nothing else this client is started with looks like one. The extension is
/// checked rather than "the first argument that is not a flag", because the
/// association is the only thing that should be able to make the client open a
/// file, and `faf-client.exe --some-flag some/path` should not.
///
/// `argv[0]` is the executable and is skipped. The backend refuses a path whose
/// extension it does not recognise anyway; this only decides whether to ask.
pub(crate) fn replay_argument(argv: &[String]) -> Option<&str> {
    argv.iter().skip(1).map(String::as_str).find(|argument| {
        let lowered = argument.to_ascii_lowercase();
        lowered.ends_with(".fafreplay") || lowered.ends_with(".scfareplay")
    })
}

/// Hand a replay path to the running client and show it.
///
/// Deliberately `try_dispatch` and not a wait: this is called from the
/// single-instance callback, which runs on Tauri's main thread, and from
/// startup. Neither is a place to block on a replay that may take seconds to
/// prepare.
pub(crate) fn open_replay_from_argument(app: &tauri::AppHandle, path: &str) {
    let Some(core) = app.try_state::<Core>() else {
        tracing::warn!("asked to open a replay before the backend was ready");
        return;
    };
    tracing::info!(%path, "opening a replay handed to the client as an argument");
    let _ = core
        .0
        .try_dispatch(AppCommand::Nav(NavCommand::Select { tab: Tab::Replays }));
    let _ = core
        .0
        .try_dispatch(AppCommand::Replays(ReplayCommand::OpenFile {
            path: path.to_string(),
        }));
}

/// Point the environment at the helpers the installer bundles.
///
/// Packaged builds place native helpers under Tauri's resource directory. The
/// lobby provider also searches the development `natives/` directory, while
/// each `FAF_*_PATH` variable remains an explicit override for custom
/// installations, which is why a value already set is left alone.
pub(crate) fn locate_bundled_helpers(app: &tauri::App) {
    // Packaged builds place native helpers under Tauri's resource
    // directory. The lobby provider also searches the development
    // `natives/` directory, while FAF_UID_PATH remains an explicit
    // override for custom installations.
    if std::env::var("FAF_UID_PATH").unwrap_or_default().is_empty() {
        let uid_name = if cfg!(windows) {
            "faf-uid.exe"
        } else if cfg!(target_os = "macos") {
            "faf-uid-macos"
        } else {
            "faf-uid"
        };
        if let Ok(resource_dir) = app.path().resource_dir() {
            let bundled_uid = resource_dir.join("natives").join(uid_name);
            if bundled_uid.is_file() {
                std::env::set_var("FAF_UID_PATH", bundled_uid);
            }
        }
    }

    if std::env::var("FAF_ICE_ADAPTER_JAR")
        .unwrap_or_default()
        .is_empty()
    {
        if let Ok(resource_dir) = app.path().resource_dir() {
            let bundled_adapter = resource_dir
                .join("natives")
                .join("java-ice-adapter")
                .join("faf-ice-adapter.jar");
            if bundled_adapter.is_file() {
                std::env::set_var("FAF_ICE_ADAPTER_JAR", bundled_adapter);
            }
        }
    }

    if std::env::var("FAF_JAVA_PATH")
        .unwrap_or_default()
        .is_empty()
    {
        if let Ok(resource_dir) = app.path().resource_dir() {
            let java_name = if cfg!(windows) { "java.exe" } else { "java" };
            let bundled_java = resource_dir
                .join("natives")
                .join("jre")
                .join("bin")
                .join(java_name);
            if bundled_java.is_file() {
                std::env::set_var("FAF_JAVA_PATH", bundled_java);
            }
        }
    }

    // Valve's library, for showing a game on Steam. Windows and Linux only,
    // and absent from a build that does not carry it, in which case the
    // setting that uses it simply does nothing.
    if let Some((library_name, _)) = faf_app::infra::steam_presence::LIBRARY {
        if std::env::var("FAF_STEAM_API_PATH")
            .unwrap_or_default()
            .is_empty()
        {
            if let Ok(resource_dir) = app.path().resource_dir() {
                let bundled_library = resource_dir
                    .join("natives")
                    .join("steam")
                    .join(library_name);
                if bundled_library.is_file() {
                    std::env::set_var("FAF_STEAM_API_PATH", bundled_library);
                }
            }
        }
    }
}

/// Build the core, drive its loop, forward its events, and kick off the
/// startup commands. Returns the handle `setup` puts under Tauri's management.
pub(crate) fn start_core(app: &tauri::App, backend_version: String) -> Arc<App> {
    // Real OAuth2 auth and the real lobby WebSocket
    // (`infra::LobbyClient`). Set FAF_FAKE_AUTH=1 to run fully offline
    // without a browser login during local dev.
    let ports = faf_app::infra::ports_from_env();
    let cleanup_files = !ports.offline_auth;
    let (core, app_loop) = App::new(backend_version, ports);
    let core = Arc::new(core);

    // Drive the command-processing loop on Tauri's async runtime.
    tauri::async_runtime::spawn(app_loop.run());

    // Forward backend events to the frontend.
    let handle = app.handle().clone();
    let (mut events, _) = core.subscribe_versioned_with_snapshot();
    let event_core = core.clone();
    tauri::async_runtime::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => {
                    let _ = handle.emit(
                        EVENT_CHANNEL,
                        FrontendMessage::Event {
                            revision: event.revision,
                            event: Box::new(event.event),
                        },
                    );
                }
                // Re-establish an atomic state/event boundary rather
                // than skipping deltas and permanently diverging from
                // the authoritative Rust state.
                // Logged, with its size and how long it took to send: it is
                // the whole state, map and mod catalogues included, and while
                // the webview takes it in, nothing else on screen updates. A
                // client that looked frozen for seconds left nothing behind to
                // say whether this was why. Sizing it serialises it once more,
                // which a lag, rare by design, can afford.
                Err(RecvError::Lagged(skipped)) => {
                    let started = std::time::Instant::now();
                    let (replacement, snapshot) = event_core.subscribe_versioned_with_snapshot();
                    events = replacement;
                    let bytes = serde_json::to_vec(&snapshot.state).map_or(0, |json| json.len());
                    let _ = handle.emit(
                        EVENT_CHANNEL,
                        FrontendMessage::Snapshot {
                            revision: snapshot.revision,
                            state: Box::new(snapshot.state),
                        },
                    );
                    tracing::warn!(
                        skipped,
                        bytes,
                        seconds = started.elapsed().as_secs_f32(),
                        "webview event stream fell behind; sent a full state snapshot"
                    );
                }
                Err(RecvError::Closed) => break,
            }
        }
    });

    // Persisted settings must be authoritative before the backend is
    // announced as ready. Otherwise a fast webview can migrate legacy
    // browser preferences into Rust defaults while the settings file
    // is still being read. Auth restore can proceed concurrently once
    // that dependency is satisfied.
    let startup_core = core.clone();
    tauri::async_runtime::spawn(async move {
        if let Err(reason) = startup_core
            .dispatch_and_wait(AppCommand::Settings(SettingsCommand::Load))
            .await
        {
            tracing::error!(%reason, "could not load startup settings");
            return;
        }
        if cleanup_files {
            let generator_output = startup_core.snapshot().settings.map_generator.output_path;
            let _ = tauri::async_runtime::spawn_blocking(move || {
                faf_app::infra::cleanup_interrupted_work(&generator_output);
            })
            .await;
        }
        let _ = startup_core.try_dispatch(AppCommand::Auth(AuthCommand::Restore));
        let _ = startup_core.try_dispatch(AppCommand::Session(SessionCommand::Hello));
    });

    core
}
