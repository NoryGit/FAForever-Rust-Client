//! Bringing the client up: where the bundled helpers are, starting the core,
//! forwarding its events, and the replay a double-click asked for.

use std::sync::Arc;

use faf_app::App;
use faf_domain::state::{
    AuthCommand, NavCommand, ReplayCommand, SessionCommand, SettingsCommand, SettingsEvent,
};
use faf_domain::{AppCommand, AppEvent};
use faf_ipc::shell::FrontendMessage;
use tauri::{Emitter, Manager};
use tokio::sync::broadcast::error::RecvError;

use crate::{Core, EVENT_CHANNEL};

/// Wait until the startup settings have been read: their `Loaded` event, or
/// the end of the whole load where events were missed. `false` when the load
/// failed before the settings were read.
async fn settings_read(
    mut events: tokio::sync::broadcast::Receiver<AppEvent>,
    mut load: tauri::async_runtime::JoinHandle<Result<(), String>>,
) -> bool {
    let finished = |result: Result<Result<(), String>, tauri::Error>| match result {
        Ok(Ok(())) => true,
        Ok(Err(reason)) => {
            tracing::error!(%reason, "could not load startup settings");
            false
        }
        Err(error) => {
            tracing::error!(%error, "the startup settings load stopped");
            false
        }
    };
    loop {
        tokio::select! {
            event = events.recv() => match event {
                Ok(AppEvent::Settings(SettingsEvent::Loaded { .. })) => return true,
                Ok(_) => {}
                // Events were missed: the end of the load is the signal then.
                Err(RecvError::Lagged(_) | RecvError::Closed) => return finished(load.await),
            },
            result = &mut load => return finished(result),
        }
    }
}

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
pub(crate) fn open_replay_from_argument(app: &tauri::AppHandle, path: &str) {
    dispatch_open_argument(
        app,
        ReplayCommand::OpenFile {
            path: path.to_string(),
        }
        .into(),
    );
}

/// Hand a `faf://` link to the client, whether it arrived with a second start
/// or started this one.
///
/// Only read here, never followed: the backend parses it and the page opens
/// what it points at once it is up and signed in, or says the link could not
/// be read. The operating system starts the client with the link as its one
/// argument when the scheme is registered (see `plugins.deep-link` in
/// `tauri.conf.json`), which any web page can ask it to do, so nothing about
/// the link is trusted beyond that check.
pub(crate) fn open_link_from_argument(app: &tauri::AppHandle, url: &str) {
    dispatch_open_argument(app, NavCommand::OpenLink { url: url.into() }.into());
}

/// Wait for queue capacity off the main thread. The replay command selects
/// its tab itself, so navigation only happens once the open reaches the core.
fn dispatch_open_argument(app: &tauri::AppHandle, command: AppCommand) {
    let Some(core) = app.try_state::<Core>() else {
        tracing::warn!("asked to open a file or link before the backend was ready");
        return;
    };
    let core = core.0.clone();
    tauri::async_runtime::spawn(async move {
        if let Err(reason) = core.dispatch(command).await {
            tracing::warn!(%reason, "could not queue a file or link from the operating system");
        }
    });
}

/// [`open_replay_from_argument`] for the replay a cold start was opened with.
///
/// That one arrives before the settings are read and before a remembered
/// sign-in is restored, and playback needs both: the replay install comes
/// from the settings, and the engine build a replay needs is downloaded with
/// the session's token. Opened at once, it started FA unprepared ("Unable to
/// load game replay") or failed with "not logged in". It waits for the backend
/// to be ready and, while a remembered sign-in is being restored, briefly for
/// that too, then opens as before.
pub(crate) fn open_replay_at_startup(app: &tauri::AppHandle, path: String) {
    use faf_domain::state::{AuthEvent, AuthMode, ConnectionStatus, SessionEvent};
    use faf_domain::AppEvent;
    use std::time::Duration;

    const READY_WAIT: Duration = Duration::from_secs(60);
    const SIGN_IN_WAIT: Duration = Duration::from_secs(20);

    let Some(core) = app.try_state::<Core>() else {
        tracing::warn!("asked to open a replay before the backend was ready");
        return;
    };
    let core = core.0.clone();
    let handle = app.clone();
    tauri::async_runtime::spawn(async move {
        // Subscribed before the state is read, so an event in between is seen.
        let mut events = core.subscribe();
        let ready = core.with_state(|state| state.session.status == ConnectionStatus::Connected);
        if !ready {
            wait_for_event(&mut events, READY_WAIT, |event| {
                matches!(event, AppEvent::Session(SessionEvent::BackendReady { .. }))
            })
            .await;
        }
        let restoring = core.with_state(|state| {
            state.settings.general.auto_login
                && state.auth.player.is_none()
                && state.auth.mode == AuthMode::Account
        });
        if restoring {
            // A restore that fails says nothing, so this is bounded.
            wait_for_event(&mut events, SIGN_IN_WAIT, |event| {
                matches!(event, AppEvent::Auth(AuthEvent::LoggedIn { .. }))
            })
            .await;
        }
        open_replay_from_argument(&handle, &path);
    });
}

/// Wait up to `limit` for an event `done` accepts.
async fn wait_for_event(
    events: &mut tokio::sync::broadcast::Receiver<faf_domain::AppEvent>,
    limit: std::time::Duration,
    done: impl Fn(&faf_domain::AppEvent) -> bool,
) {
    let _ = tokio::time::timeout(limit, async {
        loop {
            match events.recv().await {
                Ok(event) if done(&event) => return,
                Err(RecvError::Closed) => return,
                _ => {}
            }
        }
    })
    .await;
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

    // The two below are also found beside the executable, which is where the
    // Windows installer puts them. A Linux package does not: the binary goes
    // to /usr/bin and the resources to /usr/lib/<product>, so without this
    // the experimental adapter reported itself as not installed there and
    // faf-pioneer was looked up on PATH.
    if std::env::var("FAF_EXP_JAVA_ICE_ADAPTER_JAR")
        .unwrap_or_default()
        .is_empty()
    {
        if let Ok(resource_dir) = app.path().resource_dir() {
            let bundled_adapter = resource_dir
                .join("natives")
                .join("exp-java-ice-adapter")
                .join("faf-ice-adapter.jar");
            if bundled_adapter.is_file() {
                std::env::set_var("FAF_EXP_JAVA_ICE_ADAPTER_JAR", bundled_adapter);
            }
        }
    }

    if std::env::var("FAF_ICE_ADAPTER_PATH")
        .unwrap_or_default()
        .is_empty()
    {
        if let Ok(resource_dir) = app.path().resource_dir() {
            let pioneer_name = if cfg!(windows) {
                "faf-pioneer.exe"
            } else {
                "faf-pioneer"
            };
            let bundled_pioneer = resource_dir.join("natives").join(pioneer_name);
            if bundled_pioneer.is_file() {
                std::env::set_var("FAF_ICE_ADAPTER_PATH", bundled_pioneer);
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
                    if matches!(
                        &event.event,
                        AppEvent::Notifications(_) | AppEvent::Settings(_)
                    ) {
                        crate::notification_popup::refresh(&handle);
                    }
                    #[cfg(windows)]
                    if matches!(
                        event.event,
                        AppEvent::ClientUpdate(faf_domain::state::ClientUpdateEvent::Installing)
                    ) {
                        // Explicit exit bypasses close-to-tray and the window's
                        // recovery handler. Release the single-instance slot
                        // before the installer replaces files and restarts us.
                        handle.exit(0);
                        return;
                    }
                    // An AppImage update has already put the new image where
                    // the old one was, so restarting is what runs it. Tauri
                    // starts `$APPIMAGE` only once the exit has released the
                    // single-instance name, so the new process is not turned
                    // away as a second copy of this one.
                    #[cfg(target_os = "linux")]
                    if matches!(
                        event.event,
                        AppEvent::ClientUpdate(faf_domain::state::ClientUpdateEvent::Installing)
                    ) && faf_app::infra::client_update::running_appimage().is_some()
                    {
                        handle.request_restart();
                        return;
                    }
                    let revision = event.revision;
                    // The conformance cases keep the two halves of the policy
                    // in step, so this is not expected. If it ever happens the
                    // window misses one revision, sees the gap and recovers it
                    // from a snapshot, rather than this task panicking and the
                    // window receiving nothing more for the rest of the session.
                    let Some(update) = event
                        .update
                        .or_else(|| faf_domain::frontend::FrontendUpdate::from_delta(event.event))
                    else {
                        tracing::error!(
                            revision,
                            "event has neither a captured replacement nor a compact delta"
                        );
                        continue;
                    };
                    let _ = handle.emit_to(
                        tauri::EventTarget::webview_window(
                            crate::window::main_window(&handle)
                                .map(|w| w.label().to_string())
                                .unwrap_or_else(|| "main".into()),
                        ),
                        EVENT_CHANNEL,
                        FrontendMessage::Event {
                            revision,
                            event: Box::new(update),
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
                    #[cfg(windows)]
                    if matches!(
                        snapshot.state.client_update.status,
                        faf_domain::state::ClientUpdateStatus::Installing
                    ) {
                        handle.exit(0);
                        return;
                    }
                    #[cfg(target_os = "linux")]
                    if matches!(
                        snapshot.state.client_update.status,
                        faf_domain::state::ClientUpdateStatus::Installing
                    ) && faf_app::infra::client_update::running_appimage().is_some()
                    {
                        handle.request_restart();
                        return;
                    }
                    let bytes = serde_json::to_vec(&snapshot.state).map_or(0, |json| json.len());
                    let _ = handle.emit_to(
                        tauri::EventTarget::webview_window(
                            crate::window::main_window(&handle)
                                .map(|w| w.label().to_string())
                                .unwrap_or_else(|| "main".into()),
                        ),
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
    //
    // That dependency is the file, not the whole command: `Load` goes on to
    // measure the game cache and ask GitHub for a client update, which on a
    // cold disk or with GitHub slow or blocked took from seconds to over a
    // minute, and auto sign-in waited for all of it. Sign-in starts at
    // `Loaded` now and the rest of the load runs beside it.
    let startup_core = core.clone();
    // Subscribed before the load starts, so its `Loaded` cannot be missed.
    let settings_events = startup_core.subscribe();
    tauri::async_runtime::spawn(async move {
        let load_core = startup_core.clone();
        let load = tauri::async_runtime::spawn(async move {
            load_core
                .dispatch_and_wait(AppCommand::Settings(SettingsCommand::Load))
                .await
        });
        if !settings_read(settings_events, load).await {
            return;
        }
        let cleanup = if cleanup_files {
            let generator_output = startup_core.snapshot().settings.map_generator.output_path;
            tauri::async_runtime::spawn_blocking(move || {
                faf_app::infra::recover_interrupted_work(&generator_output)
            })
            .await
            .ok()
        } else {
            None
        };
        let _ = startup_core.try_dispatch(AppCommand::Auth(AuthCommand::Restore));
        let _ = startup_core.try_dispatch(AppCommand::Session(SessionCommand::Hello));
        // After the settings, because a launch command of the player's own
        // means the runtime is not wanted, and the original game's folder
        // picked under Paths is where the check looks for the game.
        if cfg!(target_os = "linux") {
            let _ = startup_core.try_dispatch(AppCommand::LinuxRuntime(
                faf_domain::state::LinuxRuntimeCommand::Prepare,
            ));
        }
        if let Some(cleanup) = cleanup {
            tauri::async_runtime::spawn_blocking(move || cleanup.finish());
        }
    });

    core
}
