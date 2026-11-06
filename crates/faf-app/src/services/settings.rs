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

use faf_domain::state::settings as domain;
use faf_domain::state::{
    is_release_version, ChatEvent, ClientNotification, InstallEvent, MapGeneratorEvent, NavEvent,
    NotificationAction, NotificationEvent, NotificationKind, SettingsCommand, SettingsEvent,
};

use crate::runtime::{EventSink, LoadedFromDisk, SerialMutation, ServiceCtx};
use crate::services::notifications;

/// The settings service's operational context: the locks that keep
/// concurrent settings commands from overtaking each other in state and on
/// disk, and whether the settings file has been read yet.
///
/// Owned by this service. Chat and lobby persist settings of their own from
/// spawned tasks; they ask whether the file has been read through
/// [`Self::has_loaded`] and queue behind every other settings write through
/// [`Self::write_order`].
#[derive(Default)]
pub struct SettingsContext {
    /// Settings commands run concurrently. Serializing the snapshot + write
    /// prevents an older command from reaching disk after a newer one.
    persist: SerialMutation,
    /// Prevent a favourite or keep-list update from landing during a sweep.
    pub(crate) generated_map_protection: SerialMutation,
    /// Held across one settings command's read, merge and emit. Commands run
    /// on their own tasks, so two patches could both read the group before
    /// either emitted, and the second would carry the first's field back to
    /// its old value. Synchronous: nothing in between awaits.
    merge: SettingsMerge,
    /// Added sounds whose removal is under way. A notifications change that
    /// would newly choose one is refused, so a dropdown that still lists it
    /// cannot leave a saved setting naming a file about to be deleted.
    sounds_being_removed: std::sync::Mutex<std::collections::HashSet<String>>,
    /// Whether the settings file has been read yet. Nothing may be persisted
    /// before it has, or a preference set during startup writes a document
    /// made of defaults over the user's own.
    loaded: LoadedFromDisk,
}

impl SettingsContext {
    /// Whether the settings file has been read yet. A service persisting
    /// settings of its own checks this first, for the reason `persist` gives:
    /// a write before the load is a document of defaults over the user's own.
    pub fn has_loaded(&self) -> bool {
        self.loaded.has_loaded()
    }

    /// The order every settings write queues in, for a service persisting
    /// settings of its own from a spawned task. A clone of the same lock this
    /// service writes under, so that write cannot reach disk out of order
    /// with a settings command's.
    pub fn write_order(&self) -> SerialMutation {
        self.persist.clone()
    }

    /// Read a settings group, change it and emit it, under the same lock the
    /// settings commands merge under, for a service that writes a group of its
    /// own (event reminders, chat read markers, the host form, vetoes).
    ///
    /// Without it such a service read the group, and a `Patch*` of the same
    /// group could land before it emitted: its emit then carried the old value
    /// of whatever the patch had just changed back into state. The
    /// architecture check refuses a settings event emitted any other way from
    /// outside this service.
    pub fn merge_and_emit<E: Into<Option<SettingsEvent>>, R>(
        &self,
        out: &EventSink,
        change: impl FnOnce(&faf_domain::state::SettingsState) -> (E, R),
    ) -> R {
        self.merge.merge_and_emit(out, change)
    }

    /// The same lock, for a service writing settings from a spawned task that
    /// cannot borrow this context. See [`Self::write_order`] for its twin.
    pub fn merger(&self) -> SettingsMerge {
        self.merge.clone()
    }
}

/// The lock every settings group is read, changed and emitted under, cloneable
/// so a spawned task can hold it. See [`SettingsContext::merge_and_emit`].
#[derive(Clone, Default)]
pub struct SettingsMerge(std::sync::Arc<std::sync::Mutex<()>>);

impl SettingsMerge {
    pub fn merge_and_emit<E: Into<Option<SettingsEvent>>, R>(
        &self,
        out: &EventSink,
        change: impl FnOnce(&faf_domain::state::SettingsState) -> (E, R),
    ) -> R {
        let _merging = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (event, result) = out.with_state(|state| change(&state.settings));
        if let Some(event) = event.into() {
            out.emit(event);
        }
        result
    }
}

pub async fn handle(cmd: SettingsCommand, ctx: &ServiceCtx, out: &EventSink) {
    match cmd {
        SettingsCommand::Load => {
            let stored = ctx.ports.settings.load().await;
            // No settings at all is the one sure sign of a fresh install: a
            // player who has run any build of this client before has a file.
            // A file nothing could be read from counts too, since the session
            // starts from defaults either way.
            let first_start = stored.is_none();
            let mut settings = stored.unwrap_or_default().normalized();
            // Filters outlive a restart only when the player asked for that
            // (#447); otherwise every list starts unfiltered. Only in state:
            // the file is written over with the cleared filters on the next
            // save anyway.
            if settings.general.filter_memory != domain::FilterMemory::Restart {
                settings.browsing = settings.browsing.without_filters();
            }
            let discovered = ctx.ports.process.discover_install_paths();
            // Where FAF's copy of the game goes when there is no copy yet.
            // Only reached when nothing else answers, and deliberately last:
            // an install another client already downloaded is a real install,
            // and a path this one has yet to create is a promise.
            let defaults = ctx.ports.process.default_install_paths();
            // A default only stands in for a path nobody has set. A path the
            // user chose that has since gone missing stays on screen as their
            // choice: replacing it would hide the move or uninstall that broke
            // it behind a folder they never asked for.
            let game_fallback = if settings.game_path.is_empty() {
                discovered.game.or(defaults.game)
            } else {
                discovered.game
            };
            let mut imported_reference_install = false;
            if !ctx
                .ports
                .process
                .install_path_is_present(&settings.game_path)
            {
                if let Some(path) = game_fallback {
                    settings.game_path = path;
                    imported_reference_install = true;
                }
            }
            // Deliberately after the game path is settled, because it is
            // derived from it. A replay install is not a second thing to go
            // and find: it is the `replaydata` half of the same FAF install,
            // and the reported confusion was a field demanding a path that
            // exists nowhere on the machine. Only the bare default is behind
            // it, for the case where there is no game path to derive from
            // either.
            let replay_fallback = if settings.replay_game_path.is_empty() {
                discovered
                    .replay
                    .or_else(|| {
                        ctx.ports
                            .process
                            .replay_path_beside_game(&settings.game_path)
                    })
                    .or(defaults.replay)
            } else {
                discovered.replay
            };
            if !ctx
                .ports
                .process
                .install_path_is_present(&settings.replay_game_path)
            {
                if let Some(path) = replay_fallback {
                    settings.replay_game_path = path;
                    imported_reference_install = true;
                }
            }
            // Persist the migration once. Explicit user choices always win, so
            // subsequent starts do not need to inspect the reference configs.
            if imported_reference_install {
                if let Err(reason) = ctx.ports.settings.save(&settings).await {
                    // Not fatal: the paths are in state for this session, and
                    // the next start simply discovers them again.
                    tracing::error!(%reason, "could not save the discovered install paths");
                }
            }
            // On a fresh install the running build counts as seen: everything
            // is new to it, and a list of what changed since some earlier build
            // would be measured against nothing. Recorded here, where "fresh"
            // is known, so that the What's new decision can read a missing
            // record as a settings file from a build that predates the record,
            // which is an update (`whats_new_on_startup`). Not for a
            // development build, which records nothing there either.
            let record_running_build = first_start && is_release_version(&ctx.backend_version);
            if record_running_build {
                settings.updates.last_seen_version = Some(ctx.backend_version.clone());
            }
            let start_page = settings.general.start_page;
            let show_joins_parts = settings.chat.show_joins_parts;
            // The map generator keeps its own working copy of these options,
            // so a persisted set has to be handed over explicitly: without
            // this the dialog would open on defaults every session and "save
            // settings" would look like it had done nothing.
            let generator_options = settings.map_generator.clone();
            // Nothing slow between reading the file and this line. `cache_info`
            // is measured further down instead, because measuring it walks the
            // whole game-files cache and both install directories, stat-ing
            // every file: seconds on a real install, longer on a cold disk or
            // behind a scanner. It used to sit here, ahead of the emit, and for
            // all that time the rest of the client saw `SettingsState::default`
            // and the webview was already rendering and taking clicks. That is
            // the window the "settings reset themselves" report came out of.
            // `cache_info` is not persisted (see `SettingsState`'s `Deserialize`),
            // so arriving a moment later costs nothing.
            out.emit(SettingsEvent::Loaded {
                settings: Box::new(settings),
            });
            // Only now may anything be written back: state finally holds the
            // player's settings rather than defaults.
            ctx.settings.loaded.mark_loaded();
            out.emit(MapGeneratorEvent::OptionsChanged {
                options: generator_options,
            });
            out.emit(ChatEvent::JoinsPartsToggled {
                enabled: show_joins_parts,
            });
            out.emit(NavEvent::TabSelected { tab: start_page });
            sync_runtime_preferences(ctx, out);
            // Written now rather than whenever something next changes, through
            // the guarded write (the load has just finished, so it goes
            // ahead). A fresh install the player never configures might
            // otherwise have no file by its first update, and that update
            // would read as a fresh install of its own: no popup for it.
            if record_running_build {
                persist(ctx, out).await;
            }
            expire_and_measure_game_cache(ctx, out).await;
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
            let path = redirect_retail_pick(&path, ctx, out, false);
            out.emit(SettingsEvent::GamePathChanged { path });
            persist(ctx, out).await;
            sync_installs(ctx, out);
        }
        SettingsCommand::SetReplayGamePath { path } => {
            let path = redirect_retail_pick(&path, ctx, out, true);
            let path = redirect_live_install_pick(&path, ctx, out);
            out.emit(SettingsEvent::ReplayGamePathChanged { path });
            persist(ctx, out).await;
            sync_installs(ctx, out);
        }
        // Every `Patch*` arm below merges into the group as state holds it
        // *now*, not as the webview last saw it, and normalises after the
        // merge. That is the whole fix for two quick changes reverting each
        // other; see `preference_patch!` in the domain.
        SettingsCommand::PatchPaths { patch } => {
            merge(ctx, out, |settings| {
                let mut next = settings.clone();
                next.paths = patch.apply_to(next.paths);
                let preferences = next.normalized().paths;
                (SettingsEvent::PathsChanged { preferences }, ())
            });
            persist(ctx, out).await;
            // Before anything else can look one up. The maps list in
            // particular is read straight after this, and reading it out of
            // the old directory would show the user their change did nothing.
            sync_paths(ctx, out);
            // Re-reports the resolved locations, which the overrides just
            // moved: the tab shows those beside every field.
            sync_installs(ctx, out);
            // The Wine prefix is one of these paths and the launcher holds a
            // copy of it, so changing it here has to reach the launcher too,
            // or the next game runs in the prefix that was configured when the
            // client started.
            sync_launch_preferences(ctx, out);
            refresh_content_after_path_change(ctx, out).await;
        }
        SettingsCommand::PatchGeneral { patch } => {
            merge(ctx, out, |settings| {
                let preferences = patch.apply_to(settings.general.clone());
                (SettingsEvent::GeneralChanged { preferences }, ())
            });
            persist(ctx, out).await;
        }
        // The calendar's own writes go through `services::events`, which is
        // where the reminder list is understood. This arm is the settings tab's
        // half of the same preferences: the week start. A patch that names only
        // that cannot carry a stale copy of the reminders, which a whole group
        // from the settings tab used to.
        SettingsCommand::PatchEvents { patch } => {
            merge(ctx, out, |settings| {
                let preferences = Box::new(patch.apply_to(settings.events.clone()));
                (SettingsEvent::EventsChanged { preferences }, ())
            });
            persist(ctx, out).await;
        }
        SettingsCommand::PatchAppearance { patch } => {
            // The reducer normalises appearance itself.
            merge(ctx, out, |settings| {
                let preferences = patch.apply_to(settings.appearance.clone());
                (SettingsEvent::AppearanceChanged { preferences }, ())
            });
            persist(ctx, out).await;
        }
        SettingsCommand::SetPlayerNote {
            player_id,
            login,
            note,
        } => {
            merge(ctx, out, |settings| {
                let mut preferences = settings.social.clone();
                preferences.set_player_note(player_id, login, note);
                (SettingsEvent::SocialChanged { preferences }, ())
            });
            persist(ctx, out).await;
        }
        SettingsCommand::SetReplayNote {
            replay_id,
            local_path,
            comment,
            tags,
        } => {
            merge(ctx, out, |settings| {
                let mut preferences = settings.social.clone();
                preferences.set_replay_note(replay_id, local_path.as_deref(), comment, tags);
                (SettingsEvent::SocialChanged { preferences }, ())
            });
            persist(ctx, out).await;
        }
        SettingsCommand::RenameReplayTag { from, to } => {
            merge(ctx, out, |settings| {
                let mut preferences = settings.social.clone();
                preferences.rename_replay_tag(&from, &to);
                (SettingsEvent::SocialChanged { preferences }, ())
            });
            persist(ctx, out).await;
        }
        SettingsCommand::PatchNotifications { patch } => {
            merge(ctx, out, |settings| {
                let mut next = settings.clone();
                next.notifications = patch.apply_to(next.notifications);
                let mut preferences = next.normalized().notifications;
                // A sound being removed, or already gone, cannot be newly
                // chosen: the dropdown may still list it while the removal
                // runs, and choosing it then would leave a saved setting
                // naming a deleted file. Checked under the merge lock, which
                // is also where the removal marks it, so the two cannot cross.
                let removing = ctx
                    .settings
                    .sounds_being_removed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if domain::refuse_new_custom_sounds(
                    &settings.notifications.sounds,
                    &mut preferences.sounds,
                    |name| removing.contains(name) || !ctx.ports.notification_sounds.exists(name),
                ) {
                    tracing::info!("refused a notification sound that is being removed or is gone");
                }
                (
                    SettingsEvent::NotificationsChanged {
                        preferences: Box::new(preferences),
                    },
                    (),
                )
            });
            persist(ctx, out).await;
        }
        SettingsCommand::RemoveNotificationSound { name } => {
            remove_notification_sound(&name, ctx, out).await;
        }
        SettingsCommand::PatchChat { patch } => {
            let show_joins_parts = merge(ctx, out, |settings| {
                let mut next = settings.clone();
                next.chat = patch.apply_to(next.chat);
                let preferences = next.normalized().chat;
                let show_joins_parts = preferences.show_joins_parts;
                (
                    SettingsEvent::ChatChanged {
                        preferences: Box::new(preferences),
                    },
                    show_joins_parts,
                )
            });
            out.emit(ChatEvent::JoinsPartsToggled {
                enabled: show_joins_parts,
            });
            persist(ctx, out).await;
        }
        SettingsCommand::PatchConnectivity { patch } => {
            merge(ctx, out, |settings| {
                let preferences = patch.apply_to(settings.connectivity);
                (SettingsEvent::ConnectivityChanged { preferences }, ())
            });
            persist(ctx, out).await;
            sync_connectivity(ctx, out);
        }
        SettingsCommand::PatchDebug { patch } => {
            merge(ctx, out, |settings| {
                let preferences = patch.apply_to(settings.debug);
                (SettingsEvent::DebugChanged { preferences }, ())
            });
            persist(ctx, out).await;
            sync_debug_windows(ctx, out);
        }
        SettingsCommand::PatchUpdates { patch } => {
            // No re-check on change: switching to the prerelease channel should
            // not fire a network request the user did not ask for. The Settings
            // section has an explicit "Check now" for that.
            merge(ctx, out, |settings| {
                let preferences = patch.apply_to(settings.updates.clone());
                (SettingsEvent::UpdatesChanged { preferences }, ())
            });
            persist(ctx, out).await;
        }
        SettingsCommand::PatchBrowsing { patch } => {
            let protection = if patch.favorite_maps.is_some() {
                Some(ctx.settings.generated_map_protection.acquire().await)
            } else {
                None
            };
            merge(ctx, out, |settings| {
                let mut next = settings.clone();
                next.browsing = patch.apply_to(next.browsing);
                let preferences = Box::new(next.normalized().browsing);
                (SettingsEvent::BrowsingChanged { preferences }, ())
            });
            drop(protection);
            persist(ctx, out).await;
        }
        SettingsCommand::PatchDiscord { patch } => {
            // No `sync_*` call: the presence watcher observes this event like
            // any other and republishes (or clears) from the new state, so
            // turning presence off takes the status down immediately.
            merge(ctx, out, |settings| {
                let preferences = patch.apply_to(settings.discord);
                (SettingsEvent::DiscordChanged { preferences }, ())
            });
            persist(ctx, out).await;
        }
        // One entry of a list or map, applied to the collection as state
        // holds it under the merge lock. The domain functions normalise the
        // group afterwards, exactly as a patch would.
        SettingsCommand::SetListMember {
            list,
            value,
            member,
        } => {
            let protection = if matches!(list, domain::PreferenceList::FavoriteMaps) {
                Some(ctx.settings.generated_map_protection.acquire().await)
            } else {
                None
            };
            // Nothing to sync afterwards: none of these lists is held by a
            // port, and the chat service reads the muted players from state.
            merge(ctx, out, |settings| {
                let event = domain::list_member_changed(settings, list, &value, member);
                (event, ())
            });
            drop(protection);
            persist(ctx, out).await;
        }
        SettingsCommand::SetPlayerNameColor { player, color } => {
            merge(ctx, out, |settings| {
                let event = domain::player_name_color_changed(settings, &player, color.as_deref());
                (event, ())
            });
            persist(ctx, out).await;
        }
        SettingsCommand::SaveModPreset { preset } => {
            merge(ctx, out, |settings| {
                (domain::mod_preset_saved(settings, preset), ())
            });
            persist(ctx, out).await;
        }
        SettingsCommand::DeleteModPreset { name } => {
            merge(ctx, out, |settings| {
                (domain::mod_preset_deleted(settings, &name), ())
            });
            persist(ctx, out).await;
        }
        SettingsCommand::SaveSearchPreset { preset } => {
            merge(ctx, out, |settings| {
                (domain::search_preset_saved(settings, preset), ())
            });
            persist(ctx, out).await;
        }
        SettingsCommand::DeleteSearchPreset { list, name } => {
            merge(ctx, out, |settings| {
                (domain::search_preset_deleted(settings, &list, &name), ())
            });
            persist(ctx, out).await;
        }
        SettingsCommand::SetMapGenerator { preferences } => {
            out.emit(SettingsEvent::MapGeneratorChanged { preferences });
            persist(ctx, out).await;
        }
        SettingsCommand::PatchGame { patch } => {
            let (old_lifetime, new_lifetime) = merge(ctx, out, |settings| {
                let mut next = settings.clone();
                let old_lifetime = next.game.cache_lifetime_days;
                next.game = patch.apply_to(next.game);
                let next_game = next.normalized().game;
                let new_lifetime = next_game.cache_lifetime_days;
                (
                    SettingsEvent::GameChanged {
                        preferences: next_game,
                    },
                    (old_lifetime, new_lifetime),
                )
            });
            persist(ctx, out).await;
            sync_launch_preferences(ctx, out);
            sync_generator_instances(ctx, out);
            if old_lifetime != new_lifetime {
                if let Some(days) = new_lifetime {
                    ctx.ports.game_cache.expire(days).await;
                    sync_game_cache(ctx, out).await;
                }
            }
        }
        // Re-stat without changing anything: for the banner's "Check again"
        // after the user installs or restores the game outside the client.
        SettingsCommand::CheckInstalls => sync_installs(ctx, out),
        SettingsCommand::RefreshGameCache => sync_game_cache(ctx, out).await,
        SettingsCommand::ClearGameCache => {
            ctx.ports.game_cache.clear().await;
            sync_game_cache(ctx, out).await;
        }
    }
}

/// Read the settings, work out the change, and emit it, as one step against
/// every other settings command.
///
/// Patching against the backend's state rather than the webview's copy is
/// only half the fix for two quick changes reverting each other: commands run
/// on their own tasks, so two patches could still both read the group before
/// either emitted, and the second would carry the first's field back. The lock
/// (`SettingsContext::merge`) closes that window. It is held only across
/// the read and the emit, which never await, and released before persisting.
///
/// The change may also be `None`, for a step that turns out to change nothing
/// and should not emit.
fn merge<E: Into<Option<SettingsEvent>>, R>(
    ctx: &ServiceCtx,
    out: &EventSink,
    change: impl FnOnce(&faf_domain::state::SettingsState) -> (E, R),
) -> R {
    ctx.settings.merge_and_emit(out, change)
}

/// Write the whole settings document back, once there is one to write.
///
/// The guard is not defensive programming. Settings are persisted as a whole
/// document read back out of state, and until [`SettingsCommand::Load`] has
/// finished that state is `SettingsState::default()`. Every command runs on
/// its own task, and the webview renders and accepts clicks as soon as it has
/// a snapshot, which is long before the load it is racing has emitted
/// anything. A preference set in that window used to save defaults for
/// everything else in the same breath: one click during startup and the
/// player's theme, paths, column widths, favourites and vetoes were gone. That
/// is the "my settings reset themselves" report.
///
/// Skipping the write rather than queueing it loses the early change itself,
/// which `Loaded` is about to overwrite in state anyway. Losing one deliberate
/// click beats erasing everything the player ever configured.
///
/// A failed write is logged and otherwise ignored: the change is in state for
/// this session, and the next successful write of the whole document carries
/// it to disk. The one caller that must not go on after a failure uses
/// [`try_persist`] instead.
pub(crate) async fn persist(ctx: &ServiceCtx, out: &EventSink) {
    if let Err(reason) = try_persist(ctx, out).await {
        tracing::warn!(%reason, "settings were not saved");
    }
}

/// [`persist`], reporting whether the document actually reached the store.
///
/// For [`remove_notification_sound`], which deletes a file on the strength of
/// the settings no longer naming it. "Attempted" is not enough there: a write
/// that failed on disk left the saved preferences still pointing at the sound,
/// and the file went anyway, so the next start played nothing under a
/// "(missing)" label.
async fn try_persist(ctx: &ServiceCtx, out: &EventSink) -> Result<(), String> {
    if !ctx.settings.loaded.has_loaded() {
        return Err(
            "a settings change arrived before the settings file was read; not writing \
             defaults over it"
                .to_string(),
        );
    }
    let _guard = ctx.settings.persist.acquire().await;
    let settings = out.with_state(|state| state.settings.clone());
    ctx.ports.settings.save(&settings).await
}

/// Clear every reference to one added sound, write that down, and only then
/// delete the file.
///
/// The order is the point, and is why this is one backend operation rather
/// than two webview calls. A file deleted while a saved setting still names it
/// leaves that row playing nothing under a "(missing)" label, with no way back
/// from the dropdown. So the file stays whenever the settings could not be
/// written first, and every failure is said out loud: the webview awaits this
/// command and reloads its list afterwards, but a list that still shows the
/// sound does not explain why.
async fn remove_notification_sound(name: &str, ctx: &ServiceCtx, out: &EventSink) {
    // Refuse a name that is not one stored file before touching any setting:
    // it cannot be one of ours, and nothing should change on its behalf.
    if !ctx.ports.notification_sounds.accepts(name) {
        notifications::add_required(
            out,
            NotificationKind::Error,
            "Could not remove the sound",
            format!("'{name}' is not a sound this client stored."),
            None,
        );
        return;
    }
    // Under the merge lock like every other settings change: read on its own,
    // a notifications patch landing between this read and the emit would be
    // overwritten by the copy taken here.
    merge(ctx, out, |settings| {
        // Marked in the same locked step that clears the references, so no
        // notifications change can choose the sound again in between. See
        // `SettingsContext::sounds_being_removed`.
        ctx.settings
            .sounds_being_removed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.to_owned());
        let current = &settings.notifications;
        let cleared = domain::without_custom_sound(&current.sounds, name).map(|sounds| {
            SettingsEvent::NotificationsChanged {
                preferences: Box::new(faf_domain::state::NotificationPreferences {
                    sounds,
                    ..current.clone()
                }),
            }
        });
        (cleared, ())
    });
    // Unmarked however the removal ends. After a deletion the file is gone,
    // which refuses it from then on; after a failure it is still there and
    // may be chosen again.
    let _unmark = BeingRemoved { ctx, name };
    // Written even when state named nothing, because state is not the disk:
    // before the settings file has been read state is all defaults, and after
    // a failed write the file can still name a sound state has let go of.
    // Either way only a successful write proves that nothing saved plays it.
    if let Err(reason) = try_persist(ctx, out).await {
        notifications::add_required(
            out,
            NotificationKind::Error,
            "Could not remove the sound",
            format!(
                "The settings could not be saved, so {name} may still be named by a saved \
                 notification setting. The file was kept; try again in a moment. ({reason})"
            ),
            None,
        );
        return;
    }
    // A name that is already gone is not an error, so the only failure left
    // is the filesystem refusing.
    if let Err(reason) = ctx.ports.notification_sounds.remove(name) {
        notifications::add_required(
            out,
            NotificationKind::Error,
            "Could not remove the sound",
            format!(
                "No notification plays {name} any more, but the file itself could not be \
                 deleted: {reason}"
            ),
            None,
        );
    }
}

/// Takes a sound back out of `SettingsContext::sounds_being_removed` when the
/// removal that put it there ends, on every return path.
struct BeingRemoved<'a> {
    ctx: &'a ServiceCtx,
    name: &'a str,
}

impl Drop for BeingRemoved<'_> {
    fn drop(&mut self) {
        self.ctx
            .settings
            .sounds_being_removed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(self.name);
    }
}

fn sync_runtime_preferences(ctx: &ServiceCtx, out: &EventSink) {
    sync_paths(ctx, out);
    sync_installs(ctx, out);
    sync_launch_preferences(ctx, out);
    sync_connectivity(ctx, out);
    sync_debug_windows(ctx, out);
    sync_generator_instances(ctx, out);
}

/// Tell the map generator how many runs it may keep going at once.
///
/// Applied on load as well as on change, so the number chosen last session
/// holds from the first generated map of this one.
fn sync_generator_instances(ctx: &ServiceCtx, out: &EventSink) {
    let instances = out.with_state(|state| state.settings.game.map_generator_instances);
    ctx.ports.map_generator.set_parallel_instances(instances);
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
    crate::runtime::run_command(
        faf_domain::state::MapsCommand::LoadInstalled.into(),
        ctx,
        out,
    )
    .await;
    crate::runtime::run_command(
        faf_domain::state::ModsCommand::LoadInstalled.into(),
        ctx,
        out,
    )
    .await;
}

/// Turn a pick of the *original* game into the FAF copy the user meant.
///
/// The reported dead end, and the whole of it. A fresh machine has Steam's
/// Forged Alliance and nothing else, so `steamapps/common/.../bin` is the only
/// `bin` folder there is to point at. The client cannot use it: the updater
/// derives its write target from the configured executable, and writing FAF's
/// patched engine into somebody's Steam install is not a thing this client
/// does. So it refused, said "Not usable", and left the user with no path that
/// would have worked, because the one that would have worked did not exist yet.
///
/// It exists now, or will: the updater creates the managed install on the first
/// launch. So the pick is answered rather than rejected: say what was picked
/// and why it cannot be the target, and configure the place FAF's own copy
/// goes. The retail install is not thrown away: it is remembered (see
/// [`remember_original_game`]), and the updater copies the engine's libraries
/// out of it and points the game at its `gamedata`, `movies`, `sounds` and
/// `fonts` (see `game_updater::retail_install_dir`).
///
/// A path that is already a managed install, or empty, is returned untouched:
/// this only fires on the retail game.
fn redirect_retail_pick(path: &str, ctx: &ServiceCtx, out: &EventSink, replay: bool) -> String {
    if !ctx.ports.process.is_original_game_install(path) {
        return path.to_string();
    }
    remember_original_game(path, ctx, out);
    let defaults = ctx.ports.process.default_install_paths();
    let target = if replay {
        defaults.replay
    } else {
        defaults.game
    };
    let Some(target) = target.filter(|target| !target.is_empty()) else {
        notifications::add_required(
            out,
            NotificationKind::Error,
            "That is the original game, not a FAF install",
            format!(
                "{path} is the unmodified Supreme Commander: Forged Alliance. FAF plays \
                 through its own patched copy of the engine and never writes into the \
                 original install. Point this at a FAF-managed ForgedAlliance.exe instead."
            ),
            None,
        );
        return path.to_string();
    };
    notifications::add_required(
        out,
        NotificationKind::GameInstall,
        "Using FAF's own copy of the game",
        format!(
            "{path} is the unmodified Supreme Commander: Forged Alliance, which FAF never \
             writes into. The {which} install now points at {target}, where the client \
             downloads FAF's patched engine the first time you {verb}. Your original install \
             is still used for the game's movies, sounds and fonts.",
            which = if replay { "replay" } else { "game" },
            verb = if replay { "watch a replay" } else { "play" },
        ),
        Some(NotificationAction::OpenSettings {
            section: Some("paths".to_string()),
        }),
    );
    target
}

/// Keep the original game the player just picked, for the updater (#474).
///
/// The pick used to be dropped once it had been redirected, on the promise
/// that the updater would find the original again. It only did when the game
/// sat in one of the places it guesses, and a new player's often does not: a
/// Steam library on a second drive is invisible to every guess, and no other
/// FAF client has configured anything on their machine. The updater then had
/// nothing to copy `BugSplat.dll` from, and the first game stopped there.
///
/// The executable is `bin/<exe>` inside the install, which is how
/// `is_original_game_install` recognised it, so the root is two levels up.
/// Persisted with the rest of the paths by the caller's `persist`, and handed
/// to the path resolver straight away, so the very next launch has it.
fn remember_original_game(executable: &str, ctx: &ServiceCtx, out: &EventSink) {
    let Some(root) = std::path::Path::new(executable)
        .parent()
        .and_then(std::path::Path::parent)
        .map(|root| root.to_string_lossy().into_owned())
        .filter(|root| !root.is_empty())
    else {
        return;
    };
    merge(ctx, out, |settings| {
        let event =
            (settings.paths.original_game_dir != root).then(|| SettingsEvent::PathsChanged {
                preferences: domain::PathPreferences {
                    original_game_dir: root.clone(),
                    ..settings.paths.clone()
                },
            });
        (event, ())
    });
    sync_paths(ctx, out);
}

/// Answer a replay pick that is the live install, the way a retail pick is
/// answered.
///
/// This is the whole of the reported confusion. The replay field asks for a
/// `ForgedAlliance.exe`, the only one on the machine is the one the game
/// install already names, so that is what gets picked, and it appears to work.
/// It does not: replay playback stages the replay's *exact* engine build into
/// the install it launches, so the first old replay quietly downgrades the copy
/// the user plays live games with, and the next live game has to download it
/// all back. The two installs are separate for that reason and no other.
///
/// So the pick is answered rather than taken: say what is wrong with it, and
/// configure `replaydata` beside the live install, which is the place the
/// updater fills in on the first replay anyway. That path is a directory FAF
/// owns, so nothing is written where the user did not expect it.
fn redirect_live_install_pick(path: &str, ctx: &ServiceCtx, out: &EventSink) -> String {
    if !ctx.ports.process.is_live_install(path) {
        return path.to_string();
    }
    let Some(target) = ctx
        .ports
        .process
        .replay_path_beside_game(path)
        .filter(|target| !target.is_empty())
    else {
        return path.to_string();
    };
    notifications::add_required(
        out,
        NotificationKind::GameInstall,
        "Replays get their own copy of the game",
        format!(
            "{path} is the install you play live games from. Watching a replay puts that \
             replay's own engine build in place first, which would downgrade it and leave \
             the next live game to download everything again. The replay install now points \
             at {target}, beside it, where the client downloads what a replay needs the \
             first time you watch one.",
        ),
        Some(NotificationAction::OpenSettings {
            section: Some("paths".to_string()),
        }),
    );
    target
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
    let connectivity = out.with_state(|state| state.settings.connectivity);
    ctx.ports.ice.set_backend(connectivity.adapter);
    ctx.ports.ice.set_host_backend(connectivity.hosting());
}

fn sync_launch_preferences(ctx: &ServiceCtx, out: &EventSink) {
    let (arguments, wrapper, wine_prefix, pipe_live_replay, auto_generate_maps, steam_presence) =
        out.with_state(|state| {
            (
                state.settings.game.additional_arguments.clone(),
                state.settings.game.launch_wrapper.clone(),
                state.settings.paths.wine_prefix.clone(),
                state.settings.game.pipe_live_replay,
                state.settings.game.auto_generate_maps,
                state.settings.game.steam_presence,
            )
        });
    let borderless_window = out.with_state(|state| state.settings.game.borderless_window);
    ctx.ports.process.set_additional_arguments(arguments);
    ctx.ports.process.set_steam_presence(steam_presence);
    ctx.ports.process.set_borderless_window(borderless_window);
    // The two halves of "run a Windows game on Linux" arrive from two
    // different preference groups, because that is where each one belongs: the
    // wrapper is about launching, the prefix is a path. The launcher needs
    // them together.
    ctx.ports.process.set_launch_wrapper(wrapper, wine_prefix);
    ctx.ports
        .replay_playback
        .set_live_replay_pipe(pipe_live_replay);
    // The replay port rebuilds a generated map before playback, and has to
    // honour the same preference the live launcher does.
    ctx.ports
        .replay_playback
        .set_auto_generate_maps(auto_generate_maps);
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
        .replay_playback
        .set_install_dir(ctx.ports.process.replay_install_dir());
    let present = ctx.ports.process.installs_present();
    let resolved = ctx.ports.paths.resolved();
    out.emit(InstallEvent::Checked {
        game_ready: present.game,
        replay_ready: present.replay,
        game_pending: present.game_pending,
        replay_pending: present.replay_pending,
        resolved,
    });
}

/// The startup cache pass: drop what has expired, then measure what is left.
///
/// Runs after `Loaded` rather than before it. Both halves walk directory trees
/// and stat every file they find, and doing that in front of the emit is what
/// made startup settings a several-second race rather than a file read.
async fn expire_and_measure_game_cache(ctx: &ServiceCtx, out: &EventSink) {
    let lifetime_days = out.with_state(|state| state.settings.game.cache_lifetime_days);
    if let Some(days) = lifetime_days {
        ctx.ports.game_cache.expire(days).await;
    }
    sync_game_cache(ctx, out).await;
}

async fn sync_game_cache(ctx: &ServiceCtx, out: &EventSink) {
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
    let Some(info) = ctx.ports.game_cache.inspect(&install_dirs).await else {
        return;
    };
    check_cache_size_alert(out, &info, alert_gb);
    out.emit(SettingsEvent::CacheInfoUpdated { info });
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
                text: Some(
                    notifications::Text::new("notifications.msg.gameCacheAlert")
                        .with("size", format!("{size_gb:.1}"))
                        .with("threshold", threshold_gb),
                ),
            };
            out.emit(NotificationEvent::Added { notification });
        }
    }
}
