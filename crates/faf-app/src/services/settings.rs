//! Settings service.
//!
//! Loads persisted settings at startup and persists changes. Note the persistence
//! pattern: the service emits the event first (so the single reduce chokepoint
//! updates the authoritative state), then reads the *post-reduce* settings back
//! from the sink and hands the whole slice to the port. This keeps services free
//! of any direct state mutation while still persisting the resulting state.
//!
//! It also owns the install check. Any path change ends in [`sync_installs`],
//! which pushes the new paths into the process port (so a freshly picked
//! install works immediately instead of at the next restart) and then stats
//! them, emitting an [`InstallEvent`] for the missing-install banner. Doing it
//! here rather than behind a separate command means there is no way to change a
//! path without the check running.

use faf_domain::state::{
    ChatEvent, ClientNotification, InstallEvent, MapGeneratorEvent, NavEvent, NotificationAction,
    NotificationEvent, NotificationKind, SettingsCommand, SettingsEvent,
};

use crate::runtime::{EventSink, ServiceCtx};

pub async fn handle(cmd: SettingsCommand, ctx: &ServiceCtx, out: &EventSink) {
    match cmd {
        SettingsCommand::Load => {
            let mut settings = ctx.ports.settings.load().await.normalized();
            let discovered = ctx.ports.process.discover_install_paths();
            let mut imported_reference_install = false;
            if !ctx
                .ports
                .process
                .install_path_is_present(&settings.game_path)
            {
                if let Some(path) = discovered.game {
                    settings.game_path = path;
                    imported_reference_install = true;
                }
            }
            if !ctx
                .ports
                .process
                .install_path_is_present(&settings.replay_game_path)
            {
                if let Some(path) = discovered.replay {
                    settings.replay_game_path = path;
                    imported_reference_install = true;
                }
            }
            // Persist the migration once. Explicit user choices always win, so
            // subsequent starts do not need to inspect the reference configs.
            if imported_reference_install {
                ctx.ports.settings.save(&settings).await;
            }
            let start_page = settings.general.start_page;
            let show_joins_parts = settings.chat.show_joins_parts;
            if let Ok(cache_root) = crate::infra::cache_dir() {
                let game_files_cache = cache_root.join("game_files");
                if let Some(days) = settings.game.cache_lifetime_days {
                    let _ = crate::infra::game_updater::clean_expired_cache_files(
                        &game_files_cache,
                        days,
                    )
                    .await;
                }
                let mut install_dirs = Vec::new();
                if !settings.game_path.is_empty() {
                    install_dirs.push(std::path::PathBuf::from(&settings.game_path));
                }
                if !settings.replay_game_path.is_empty() {
                    install_dirs.push(std::path::PathBuf::from(&settings.replay_game_path));
                }
                settings.cache_info = crate::infra::game_updater::inspect_game_cache(
                    &game_files_cache,
                    &install_dirs,
                )
                .await;
            }
            // The map generator keeps its own working copy of these options,
            // so a persisted set has to be handed over explicitly: without
            // this the dialog would open on defaults every session and "save
            // settings" would look like it had done nothing.
            let generator_options = settings.map_generator.clone();
            check_cache_size_alert(out, &settings.cache_info, settings.game.cache_size_alert_gb);
            out.emit(SettingsEvent::Loaded {
                settings: Box::new(settings),
            });
            out.emit(MapGeneratorEvent::OptionsChanged {
                options: generator_options,
            });
            out.emit(ChatEvent::JoinsPartsToggled {
                enabled: show_joins_parts,
            });
            out.emit(NavEvent::TabSelected { tab: start_page });
            sync_runtime_preferences(ctx, out);
            // Last, and deliberately here rather than in the session handshake:
            // the release channel is a preference, so a check that ran any
            // earlier would always use the stable default no matter what the
            // user picked. `Load` runs once at startup, which is exactly the
            // moment the Java client checks too.
            crate::services::client_update::check_on_startup(ctx, out).await;
        }
        SettingsCommand::SetTheme { theme } => {
            out.emit(SettingsEvent::ThemeChanged { theme });
            persist(ctx, out).await;
        }
        SettingsCommand::SetGamePath { path } => {
            out.emit(SettingsEvent::GamePathChanged { path });
            persist(ctx, out).await;
            sync_installs(ctx, out);
        }
        SettingsCommand::SetReplayGamePath { path } => {
            out.emit(SettingsEvent::ReplayGamePathChanged { path });
            persist(ctx, out).await;
            sync_installs(ctx, out);
        }
        SettingsCommand::SetPaths { preferences } => {
            let mut next = out.with_state(|state| state.settings.clone());
            next.paths = preferences;
            out.emit(SettingsEvent::PathsChanged {
                preferences: next.normalized().paths,
            });
            persist(ctx, out).await;
            // Before anything else can look one up. The maps list in
            // particular is read straight after this, and reading it out of
            // the old directory would show the user their change did nothing.
            sync_paths(ctx, out);
            // Re-reports the resolved locations, which the overrides just
            // moved: the tab shows those beside every field.
            sync_installs(ctx, out);
            refresh_content_after_path_change(ctx, out).await;
        }
        SettingsCommand::SetGeneral { preferences } => {
            out.emit(SettingsEvent::GeneralChanged { preferences });
            persist(ctx, out).await;
        }
        SettingsCommand::SetAppearance { preferences } => {
            out.emit(SettingsEvent::AppearanceChanged { preferences });
            persist(ctx, out).await;
        }
        SettingsCommand::SetPlayerNote {
            player_id,
            login,
            note,
        } => {
            let mut preferences = out.with_state(|state| state.settings.social.clone());
            preferences.set_player_note(player_id, login, note);
            out.emit(SettingsEvent::SocialChanged { preferences });
            persist(ctx, out).await;
        }
        SettingsCommand::SetNotifications { preferences } => {
            let mut next = out.with_state(|state| state.settings.clone());
            next.notifications = preferences;
            out.emit(SettingsEvent::NotificationsChanged {
                preferences: next.normalized().notifications,
            });
            persist(ctx, out).await;
        }
        SettingsCommand::SetChat { preferences } => {
            let mut next = out.with_state(|state| state.settings.clone());
            next.chat = *preferences;
            let preferences = next.normalized().chat;
            let show_joins_parts = preferences.show_joins_parts;
            out.emit(SettingsEvent::ChatChanged {
                preferences: Box::new(preferences),
            });
            out.emit(ChatEvent::JoinsPartsToggled {
                enabled: show_joins_parts,
            });
            persist(ctx, out).await;
        }
        SettingsCommand::SetConnectivity { preferences } => {
            out.emit(SettingsEvent::ConnectivityChanged { preferences });
            persist(ctx, out).await;
            sync_connectivity(ctx, out);
        }
        SettingsCommand::SetDebug { preferences } => {
            out.emit(SettingsEvent::DebugChanged { preferences });
            persist(ctx, out).await;
            sync_debug_windows(ctx, out);
        }
        SettingsCommand::SetUpdates { preferences } => {
            // No re-check on change: switching to the prerelease channel should
            // not fire a network request the user did not ask for. The Settings
            // section has an explicit "Check now" for that.
            out.emit(SettingsEvent::UpdatesChanged { preferences });
            persist(ctx, out).await;
        }
        SettingsCommand::SetBrowsing { preferences } => {
            let mut next = out.with_state(|state| state.settings.clone());
            next.browsing = *preferences;
            out.emit(SettingsEvent::BrowsingChanged {
                preferences: Box::new(next.normalized().browsing),
            });
            persist(ctx, out).await;
        }
        SettingsCommand::SetDiscord { preferences } => {
            // No `sync_*` call: the presence watcher observes this event like
            // any other and republishes (or clears) from the new state, so
            // turning presence off takes the status down immediately.
            out.emit(SettingsEvent::DiscordChanged { preferences });
            persist(ctx, out).await;
        }
        SettingsCommand::SetMapGenerator { preferences } => {
            out.emit(SettingsEvent::MapGeneratorChanged { preferences });
            persist(ctx, out).await;
        }
        SettingsCommand::SetGame { preferences } => {
            let mut next = out.with_state(|state| state.settings.clone());
            let old_lifetime = next.game.cache_lifetime_days;
            next.game = preferences;
            let next_game = next.normalized().game;
            let new_lifetime = next_game.cache_lifetime_days;
            out.emit(SettingsEvent::GameChanged {
                preferences: next_game,
            });
            persist(ctx, out).await;
            sync_launch_preferences(ctx, out);
            if old_lifetime != new_lifetime {
                if let Some(days) = new_lifetime {
                    if let Ok(cache_root) = crate::infra::cache_dir() {
                        let _ = crate::infra::game_updater::clean_expired_cache_files(
                            &cache_root.join("game_files"),
                            days,
                        )
                        .await;
                        sync_game_cache(out).await;
                    }
                }
            }
        }
        // Re-stat without changing anything: for the banner's "Check again"
        // after the user installs or restores the game outside the client.
        SettingsCommand::CheckInstalls => sync_installs(ctx, out),
        SettingsCommand::RefreshGameCache => sync_game_cache(out).await,
        SettingsCommand::ClearGameCache => {
            if let Ok(cache_root) = crate::infra::cache_dir() {
                let game_files_cache = cache_root.join("game_files");
                let _ = crate::infra::game_updater::clear_game_cache(&game_files_cache).await;
            }
            sync_game_cache(out).await;
        }
    }
}

pub(crate) async fn persist(ctx: &ServiceCtx, out: &EventSink) {
    let _guard = ctx.settings_persist.acquire().await;
    let settings = out.with_state(|state| state.settings.clone());
    ctx.ports.settings.save(&settings).await;
}

fn sync_runtime_preferences(ctx: &ServiceCtx, out: &EventSink) {
    sync_paths(ctx, out);
    sync_installs(ctx, out);
    sync_launch_preferences(ctx, out);
    sync_connectivity(ctx, out);
    sync_debug_windows(ctx, out);
}

/// Tell the helper processes which diagnostic windows they may open.
///
/// Applied on load as well as on change, for the same reason as
/// [`sync_connectivity`]: a switch flipped last session has to hold from the
/// first game and the first generator run of this one.
fn sync_debug_windows(ctx: &ServiceCtx, out: &EventSink) {
    let debug = out.with_state(|state| state.settings.debug);
    ctx.ports
        .ice
        .set_debug_windows(crate::ports::IceDebugWindows {
            debug: debug.ice_adapter_debug_window,
            info: debug.ice_adapter_info_window,
            console: debug.ice_adapter_console_window,
        });
    ctx.ports
        .map_generator
        .set_show_window(debug.map_generator_window);
}

/// Re-scan what the moved directories hold.
///
/// A path change silently invalidates two lists the user is probably looking
/// at. Without this the maps and mods tabs keep showing what was in the old
/// folder until something else happens to reload them, which reads as the
/// setting having done nothing.
async fn refresh_content_after_path_change(ctx: &ServiceCtx, out: &EventSink) {
    crate::services::maps::handle(faf_domain::state::MapsCommand::LoadInstalled, ctx, out).await;
    crate::services::mods::handle(faf_domain::state::ModsCommand::LoadInstalled, ctx, out).await;
}

/// Hand the configured directories to the path resolver.
///
/// Applied on load as well as on change, for the same reason as
/// [`sync_connectivity`]: a directory chosen in a previous session has to be
/// honoured from the first lookup, not the second.
fn sync_paths(ctx: &ServiceCtx, out: &EventSink) {
    ctx.ports
        .paths
        .set_overrides(out.with_state(|state| state.settings.paths.clone()));
}

/// Tell the connectivity port which backend to start next.
///
/// Applied on load as well as on change, so a preference chosen in a previous
/// session is honoured from the first game rather than the second.
fn sync_connectivity(ctx: &ServiceCtx, out: &EventSink) {
    ctx.ports
        .ice
        .set_backend(out.with_state(|state| state.settings.connectivity.adapter));
}

fn sync_launch_preferences(ctx: &ServiceCtx, out: &EventSink) {
    let (arguments, pipe_live_replay, auto_generate_maps) = out.with_state(|state| {
        (
            state.settings.game.additional_arguments.clone(),
            state.settings.game.pipe_live_replay,
            state.settings.game.auto_generate_maps,
        )
    });
    ctx.ports.process.set_additional_arguments(arguments);
    ctx.ports.replay.set_live_replay_pipe(pipe_live_replay);
    // The replay port rebuilds a generated map before playback, and has to
    // honour the same preference the live launcher does.
    ctx.ports.replay.set_auto_generate_maps(auto_generate_maps);
}

/// Push the current paths into the launcher and report what actually exists.
fn sync_installs(ctx: &ServiceCtx, out: &EventSink) {
    let settings = out.with_state(|state| state.settings.clone());
    ctx.ports
        .process
        .set_paths(settings.game_path, settings.replay_game_path);
    // The replay preparation steps patch the install they are about to launch,
    // so they follow the configured path rather than a startup environment
    // variable. Without this a replay install chosen in Settings left the
    // engine version unmatched and FA opened on the main menu.
    ctx.ports
        .replay
        .set_install_dir(ctx.ports.process.replay_install_dir());
    let present = ctx.ports.process.installs_present();
    let resolved = ctx.ports.paths.resolved();
    out.emit(InstallEvent::Checked {
        game_ready: present.game,
        replay_ready: present.replay,
        resolved,
    });
}

async fn sync_game_cache(out: &EventSink) {
    if let Ok(cache_root) = crate::infra::cache_dir() {
        let game_files_cache = cache_root.join("game_files");
        let (game_path, replay_path, alert_gb) = out.with_state(|state| {
            (
                std::path::PathBuf::from(&state.settings.game_path),
                std::path::PathBuf::from(&state.settings.replay_game_path),
                state.settings.game.cache_size_alert_gb,
            )
        });
        let mut install_dirs = Vec::new();
        if !game_path.as_os_str().is_empty() {
            install_dirs.push(game_path);
        }
        if !replay_path.as_os_str().is_empty() && Some(&replay_path) != install_dirs.first() {
            install_dirs.push(replay_path);
        }
        let info =
            crate::infra::game_updater::inspect_game_cache(&game_files_cache, &install_dirs).await;
        check_cache_size_alert(out, &info, alert_gb);
        out.emit(SettingsEvent::CacheInfoUpdated { info });
    }
}

fn check_cache_size_alert(
    out: &EventSink,
    cache_info: &faf_domain::state::GameCacheInfo,
    alert_gb: Option<u32>,
) {
    let Some(threshold_gb) = alert_gb else {
        return;
    };
    if threshold_gb == 0 {
        return;
    }
    let threshold_bytes = (threshold_gb as f64) * 1024.0 * 1024.0 * 1024.0;
    if cache_info.total_size_bytes >= threshold_bytes {
        let size_gb = cache_info.total_size_bytes / (1024.0 * 1024.0 * 1024.0);
        let id = format!("game-cache-alert-{}", threshold_gb);
        let already_notified =
            out.with_state(|state| state.notifications.items.iter().any(|item| item.id == id));
        if !already_notified {
            let notification = ClientNotification {
                id,
                kind: NotificationKind::GameCacheAlert,
                title: "Game Cache Size Alert".to_string(),
                body: format!(
                    "Cached game files are using {:.1} GB (alert threshold: {} GB). You can review or clear disk space in Settings.",
                    size_gb, threshold_gb
                ),
                created_at: chrono::Utc::now().to_rfc3339(),
                read: false,
                action: Some(NotificationAction::OpenSettings {
                    section: Some("gameCache".to_string()),
                }),
            };
            out.emit(NotificationEvent::Added { notification });
        }
    }
}
