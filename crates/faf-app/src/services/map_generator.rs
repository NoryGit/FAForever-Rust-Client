//! Map generator service.
//!
//! Bridges the streaming [`MapGeneratorPort`](crate::ports::MapGeneratorPort)
//! to events: same shape as the chat and lobby services: start a run, then
//! forward each status until the stream ends.
//!
//! Two things it owns beyond forwarding:
//!
//! * **Skipping work that isn't needed.** `GenerateNamed` returns immediately
//!   when the map is already on disk, so joining a lobby you have the map for
//!   costs nothing. The Java client's `generateIfNotInstalled` does the same.
//! * **Refreshing the map list afterwards.** A generated map is a new folder in
//!   the maps directory, so the maps slice is stale until it re-scans: without
//!   this the map you just generated wouldn't appear as installed.

use faf_domain::protocol::{map_generator, map_generator_name};
use faf_domain::state::{
    GeneratorOptionQuery, GeneratorStatus, MapGeneratorCommand, MapGeneratorEvent, MapsCommand,
    NotificationKind, SettingsEvent,
};

use crate::ports::GeneratorUpdate;
use crate::runtime::{EventSink, LatestRequest, ServiceCtx};
use crate::services;

/// How many previews one `LoadPreviews` reads. Tiles ask one map at a time;
/// this only keeps a burst of them well under the state's own ceiling,
/// `MAX_KEPT_PREVIEWS` in `faf_domain::state::map_generator`.
const MAX_PREVIEWS_PER_REQUEST: usize = 16;

#[derive(Default)]
pub struct MapGeneratorContext {
    generation: LatestRequest,
}

impl MapGeneratorContext {
    pub(crate) fn begin(&self) -> u64 {
        self.generation.begin()
    }
    pub(crate) fn is_current(&self, generation: u64) -> bool {
        self.generation.is_current(generation)
    }
}

pub async fn handle(cmd: MapGeneratorCommand, ctx: &ServiceCtx, out: &EventSink) {
    match cmd {
        MapGeneratorCommand::GenerateNamed { map_name } => {
            let generation = ctx.map_generator.begin();
            crate::runtime::expect_admitted(crate::runtime::Key::MapGenerator);
            // Announce the run before doing anything, so the status can never
            // still be reporting the *previous* run's result while this one is
            // under way. See `GeneratorStatus::Preparing`.
            out.emit(MapGeneratorEvent::StatusChanged {
                status: GeneratorStatus::Preparing,
            });
            if ctx.ports.map_generator.is_installed(&map_name) {
                // Already reproduced: report success without spawning Java.
                emit_previews(std::slice::from_ref(&map_name), ctx, out).await;
                if !ctx.map_generator.is_current(generation) {
                    return;
                }
                out.emit(MapGeneratorEvent::StatusChanged {
                    status: GeneratorStatus::Generated {
                        maps: vec![map_name],
                    },
                });
                return;
            }
            let updates = ctx.ports.map_generator.generate_named(map_name).await;
            if cancelled_before_start(out) || !ctx.map_generator.is_current(generation) {
                return;
            }
            // Kept or not on the same standing preference as a deliberate run.
            // This used to be exempt, on the grounds that a map reproduced for
            // a lobby join is not one the user sat down and asked for; with the
            // decision made once in Settings rather than per run, "keep
            // generated maps" means the ones on disk, however they got there.
            drain(updates, generation, ctx, out).await;
        }
        MapGeneratorCommand::Generate { options } => {
            let generation = ctx.map_generator.begin();
            crate::runtime::expect_admitted(crate::runtime::Key::MapGenerator);
            out.emit(MapGeneratorEvent::StatusChanged {
                status: GeneratorStatus::Preparing,
            });
            out.emit(MapGeneratorEvent::OptionsChanged {
                options: options.clone(),
            });
            // Straight to the run, as the Java client does (#596). A `--parse`
            // first cost a whole JVM start, about a second, on every
            // Generate. The run itself refuses options it cannot use the
            // moment it starts, with the generator's own complaint as the
            // failure reason, so nothing waits three minutes to hear it; the
            // dialog's own Preflight still works the name out on request.
            let updates = ctx.ports.map_generator.generate(options).await;
            // Dropping this receiver stops this run if cancellation landed
            // while the port was starting it.
            if cancelled_before_start(out) || !ctx.map_generator.is_current(generation) {
                return;
            }
            drain(updates, generation, ctx, out).await;
        }
        MapGeneratorCommand::SetOptions { options } => {
            out.emit(MapGeneratorEvent::ValidationChanged {
                issues: validation(&options, out),
            });
            // A name resolved for the options as they were is not a name for
            // the options as they are. The dialog sends this on every settled
            // edit, so clearing here is what keeps the prediction from
            // outliving the question it answered.
            out.emit(MapGeneratorEvent::NamePredicted {
                map_name: String::new(),
            });
            // Written through to the settings file, not just to the in-memory
            // slice: "save settings" that lasts until the next restart is
            // indistinguishable from a button that does nothing.
            let preferences = Box::new(options.clone());
            ctx.settings.merge_and_emit(out, |_| {
                (SettingsEvent::MapGeneratorChanged { preferences }, ())
            });
            out.emit(MapGeneratorEvent::OptionsChanged { options });
            services::settings::persist(ctx, out).await;
        }
        MapGeneratorCommand::Validate { options } => {
            // Pure arithmetic, so the dialog can call this on every keystroke.
            out.emit(MapGeneratorEvent::ValidationChanged {
                issues: validation(&options, out),
            });
        }
        MapGeneratorCommand::Preflight { options } => {
            match ctx.ports.map_generator.preflight(options).await {
                // Empty means the release cannot resolve a name at all. Worth
                // saying: the button was pressed, and would otherwise appear
                // to do nothing.
                Ok(map_name) if map_name.is_empty() => {
                    out.emit(MapGeneratorEvent::NamePredicted {
                        map_name: String::new(),
                    });
                    // Not a failure reason: a fact about the release, worded
                    // here and in the catalogue, with the version it needs.
                    services::notifications::add_text(
                        out,
                        NotificationKind::Error,
                        services::notifications::Text::new(
                            "notifications.msg.generatorCannotParse",
                        )
                        .with("version", map_generator::MIN_PARSE_VERSION),
                        "This generator cannot resolve a name",
                        format!(
                            "Working the map name out from the options needs generator {} or newer. Generating still works.",
                            map_generator::MIN_PARSE_VERSION
                        ),
                        None,
                    );
                }
                Ok(map_name) => out.emit(MapGeneratorEvent::NamePredicted { map_name }),
                Err(reason) => {
                    // Not a generation failure: nothing was started. Clearing
                    // the prediction and reporting the reason keeps a stale
                    // name from looking like it still applies.
                    out.emit(MapGeneratorEvent::NamePredicted {
                        map_name: String::new(),
                    });
                    services::notifications::add_failure(
                        out,
                        services::notifications::Text::new("notifications.msg.mapOptionsRejected"),
                        "Those options will not generate",
                        reason,
                    );
                }
            }
        }
        MapGeneratorCommand::LoadPreviews { map_names } => {
            let wanted: Vec<String> = out.with_state(|state| {
                map_names
                    .iter()
                    .filter(|name| {
                        faf_domain::protocol::map_generator::is_generated_map(name)
                            && !state.map_generator.previews.contains_key(*name)
                    })
                    .take(MAX_PREVIEWS_PER_REQUEST)
                    .cloned()
                    .collect()
            });
            if wanted.is_empty() {
                return;
            }
            emit_previews(&wanted, ctx, out).await;
        }
        MapGeneratorCommand::DecodeNames { map_names } => {
            // No IO at all: a generated map name carries its own parameters,
            // so a whole lobby list can be expanded in one pass.
            let decoded: std::collections::HashMap<_, _> = map_names
                .iter()
                .filter_map(|name| {
                    map_generator_name::decode(name).map(|parsed| (name.clone(), parsed))
                })
                .collect();
            if !decoded.is_empty() {
                out.emit(MapGeneratorEvent::NamesDecoded { decoded });
            }
        }
        MapGeneratorCommand::LoadHelp { version } => {
            match ctx.ports.map_generator.help(version).await {
                Ok(text) => out.emit(MapGeneratorEvent::HelpLoaded { text }),
                Err(reason) => services::notifications::add_failure(
                    out,
                    services::notifications::Text::new("notifications.msg.generatorHelpFailed"),
                    "Could not read the generator help",
                    reason,
                ),
            }
        }
        MapGeneratorCommand::Cancel => {
            ctx.map_generator.begin();
            // Invalidate progress first so a late terminal result cannot
            // replace the cancelled status or a newer run's progress.
            if out.with_state(|state| state.map_generator.status.is_busy()) {
                out.emit(MapGeneratorEvent::StatusChanged {
                    status: GeneratorStatus::Cancelled,
                });
            }
            ctx.ports.map_generator.cancel();
        }
        MapGeneratorCommand::SavePreset {
            name,
            options,
            request_id,
        } => {
            match ctx.ports.map_generator.save_preset(&name, &options).await {
                Ok(()) => {
                    // Saving a preset is also "these are my current options",
                    // so the dialog reopens on them without a second click.
                    out.emit(MapGeneratorEvent::OptionsChanged {
                        options: options.clone(),
                    });
                    let preferences = Box::new(options);
                    ctx.settings.merge_and_emit(out, |_| {
                        (SettingsEvent::MapGeneratorChanged { preferences }, ())
                    });
                    services::settings::persist(ctx, out).await;
                    reload_presets(ctx, out).await;
                    // After the reload, so the list already holds the preset
                    // when the dialog says it was saved.
                    out.emit(MapGeneratorEvent::PresetSaveFinished {
                        request_id,
                        saved: true,
                    });
                }
                Err(reason) => {
                    services::notifications::add_failure(
                        out,
                        services::notifications::Text::new(
                            "notifications.msg.generatorPresetSaveFailed",
                        ),
                        "Could not save the preset",
                        reason,
                    );
                    out.emit(MapGeneratorEvent::PresetSaveFinished {
                        request_id,
                        saved: false,
                    });
                }
            }
        }
        MapGeneratorCommand::LoadPresets => reload_presets(ctx, out).await,
        MapGeneratorCommand::RenamePreset { name, new_name } => {
            if let Err(reason) = ctx
                .ports
                .map_generator
                .rename_preset(&name, &new_name)
                .await
            {
                services::notifications::add_failure(
                    out,
                    services::notifications::Text::new(
                        "notifications.msg.generatorPresetRenameFailed",
                    ),
                    "Could not rename the preset",
                    reason,
                );
            }
            reload_presets(ctx, out).await;
        }
        MapGeneratorCommand::DeletePreset { name } => {
            if let Err(reason) = ctx.ports.map_generator.delete_preset(&name).await {
                services::notifications::add_failure(
                    out,
                    services::notifications::Text::new(
                        "notifications.msg.generatorPresetDeleteFailed",
                    ),
                    "Could not delete the preset",
                    reason,
                );
            }
            reload_presets(ctx, out).await;
        }
        MapGeneratorCommand::LoadOptions { version } => load_options(version, ctx, out).await,
        MapGeneratorCommand::CleanUp => {
            crate::runtime::expect_admitted(crate::runtime::Key::MapGenerator);
            let result = {
                let _protection = ctx.settings.generated_map_protection.acquire().await;
                clean_up_generated_maps(
                    ctx.ports.settings.as_ref(),
                    ctx.ports.map_generator.as_ref(),
                    || out.with_state(|state| protected_generated_maps(&state.settings)),
                )
                .await
            };
            match result {
                Ok(0) => services::notifications::add_text(
                    out,
                    NotificationKind::MapGenerated,
                    services::notifications::Text::new("notifications.msg.noGeneratedMaps"),
                    "Generated maps",
                    "There were no generated maps to remove.",
                    None,
                ),
                Ok(count) => {
                    services::notifications::add_text(
                        out,
                        NotificationKind::MapGenerated,
                        services::notifications::Text::new(
                            "notifications.msg.generatedMapsRemoved",
                        )
                        .with("count", count),
                        "Generated maps removed",
                        format!("Removed {count} generated map(s)."),
                        None,
                    );
                    refresh_installed_maps(ctx, out).await;
                }
                Err(reason) => services::notifications::add_failure(
                    out,
                    services::notifications::Text::new(
                        "notifications.msg.generatedMapsRemoveFailed",
                    ),
                    "Could not remove generated maps",
                    reason,
                ),
            }
        }
    }
}

/// Use both sources of protection. Startup may not have populated state yet,
/// and a favourite or keep entry may have reached state before its disk write.
/// The caller holds the protection guard through the deletion.
pub(crate) async fn clean_up_generated_maps(
    settings: &dyn crate::ports::SettingsPort,
    generator: &dyn crate::ports::MapGeneratorPort,
    live_protections: impl FnOnce() -> Vec<String>,
) -> Result<usize, String> {
    let saved = settings
        .load()
        .await
        .ok_or("the settings file could not be read")?;
    let mut protected = protected_generated_maps(&saved);
    protected.extend(live_protections());
    generator.clean_up(&protected).await
}

/// The generated maps a clean-up spares.
///
/// Two ways to be spared, and they mean different things: a favourite is a
/// map somebody marked in the vault, a kept map is one they asked the
/// generator to hold on to as it ran.
pub(crate) fn protected_generated_maps(settings: &faf_domain::state::SettingsState) -> Vec<String> {
    let mut protected = settings.browsing.favorite_maps.clone();
    protected.extend(settings.kept_generated_maps.iter().cloned());
    protected
}

/// Whether Cancel was pressed while the run was still being prepared. See the
/// `Cancel` arm: before a run exists, the status is where the request is kept.
fn cancelled_before_start(out: &EventSink) -> bool {
    out.with_state(|state| state.map_generator.status == GeneratorStatus::Cancelled)
}

/// Forward every status, and re-scan installed maps once a run succeeds.
///
/// A successful run's map names are recorded so the Maps tab's sweep spares
/// them, when `settings.game.keep_generated_maps` says to. That switch used to
/// be a checkbox in the dialog, decided per run; it is one standing preference
/// now, because whether a map is worth keeping is known after looking at it and
/// the dialog is closed by then.
///
/// Names rather than the switch alone: turning the switch off later must not
/// retroactively condemn maps that were kept while it was on.
async fn drain(
    mut updates: tokio::sync::mpsc::Receiver<GeneratorUpdate>,
    generation: u64,
    ctx: &ServiceCtx,
    out: &EventSink,
) {
    let mut succeeded_maps: Vec<String> = Vec::new();
    while let Some(GeneratorUpdate::Status(status)) = updates.recv().await {
        if !ctx.map_generator.is_current(generation) {
            return;
        }
        match &status {
            GeneratorStatus::Generated { maps } => {
                succeeded_maps = maps.clone();
                announce_background_result(
                    out,
                    match maps.as_slice() {
                        [one] => services::notifications::Text::new("notifications.msg.mapReady")
                            .with("name", one),
                        many => services::notifications::Text::new("notifications.msg.mapsReady")
                            .with("count", many.len()),
                    },
                    "Map ready",
                    match maps.as_slice() {
                        [one] => one.clone(),
                        many => format!("Generated {} maps.", many.len()),
                    },
                );
            }
            GeneratorStatus::Failed { reason } => services::notifications::add_failure(
                out,
                services::notifications::Text::new("notifications.msg.mapGenerationFailed"),
                "Map generation failed",
                reason.clone(),
            ),
            // A cancellation is the user's own doing; telling them about it
            // would be reporting their own click back to them.
            _ => {}
        }
        out.emit(MapGeneratorEvent::StatusChanged { status });
    }
    if !succeeded_maps.is_empty() {
        record_generated_maps(&succeeded_maps, ctx, out).await;
    }
}

/// Everything a finished run owes the rest of the client.
///
/// Three separate things, and every path that produces a generated map owes all
/// three: the names go on the keep list when the standing preference says to,
/// the preview art is read out of the new folders, and the maps slice re-scans
/// so the map counts as installed.
///
/// Split out of [`drain`] because [`drain`] is not the only place a map gets
/// generated. `services::launcher::ensure_generated_map` runs the generator
/// itself when a lobby join needs a map that is not on disk, and it forwarded
/// progress without doing any of this: the map was built, the game started, and
/// the client still showed it as missing with no preview until something else
/// happened to re-scan. That is the same work, so it is the same function.
pub(crate) async fn record_generated_maps(maps: &[String], ctx: &ServiceCtx, out: &EventSink) {
    if maps.is_empty() {
        return;
    }
    let keep = {
        let _protection = ctx.settings.generated_map_protection.acquire().await;
        let before = ctx.settings.merge_and_emit(out, |settings| {
            if !settings.game.keep_generated_maps {
                return (None, None);
            }
            (
                Some(SettingsEvent::KeptGeneratedMaps {
                    map_names: maps.to_vec(),
                }),
                Some(settings.kept_generated_maps.clone()),
            )
        });
        if let Some(before) = before {
            // Trim the capped keep list, while still sparing favourites.
            let protected = out.with_state(|state| protected_generated_maps(&state.settings));
            evict_generated_maps(&before, &protected, ctx, out).await;
            true
        } else {
            false
        }
    };
    if keep {
        services::settings::persist(ctx, out).await;
    }
    emit_previews(maps, ctx, out).await;
    refresh_installed_maps(ctx, out).await;
}

/// Delete the map folders that the keep list's cap pushed out.
///
/// Names only, and only ones the list itself dropped: this never touches a map
/// the user still has on the list, a favourite, or anything that was not
/// generated, because it acts on the difference between two states of one
/// list rather than on a scan of the folder.
async fn evict_generated_maps(
    before: &[String],
    protected: &[String],
    ctx: &ServiceCtx,
    out: &EventSink,
) {
    let protected: std::collections::HashSet<String> = protected
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect();
    let evicted: Vec<String> = before
        .iter()
        .filter(|name| !protected.contains(&name.to_ascii_lowercase()))
        .cloned()
        .collect();
    if evicted.is_empty() {
        return;
    }
    for name in &evicted {
        if let Err(reason) = ctx.ports.maps.uninstall_map(name.clone()).await {
            // Not a failure worth stopping the run for: the map is off the
            // keep list either way, so the next manual sweep will collect it.
            tracing::warn!(map = %name, %reason, "could not remove a capped generated map");
        }
    }
    announce_background_result(
        out,
        services::notifications::Text::new("notifications.msg.generatedMapsTrimmed")
            .with("count", evicted.len()),
        "Generated maps trimmed",
        format!(
            "Removed {} older generated map(s) to stay within the keep limit.",
            evicted.len()
        ),
    );
}

/// Announce something the generator did on its own, unless the player turned
/// that off (#307).
///
/// Only the background results go through here: a finished map and the keep
/// limit trimming old ones. The answers to the Maps tab's clean-up button stay
/// unconditional, because they reply to a click, and failures stay
/// unconditional because a lobby join blocked on a map that never arrives needs
/// explaining whatever the switch says.
fn announce_background_result(
    out: &EventSink,
    text: services::notifications::Text,
    title: &str,
    body: String,
) {
    if out.with_state(|state| state.settings.notifications.map_generated) {
        services::notifications::add_text(
            out,
            NotificationKind::MapGenerated,
            text,
            title,
            body,
            None,
        );
    }
}

/// Re-read the whole preset library and publish it.
///
/// Called after every change rather than mutating a cached list, so the state
/// always reflects the folder, including presets added or removed by hand.
async fn reload_presets(ctx: &ServiceCtx, out: &EventSink) {
    let presets = ctx.ports.map_generator.list_presets().await;
    out.emit(MapGeneratorEvent::PresetsLoaded { presets });
}

/// A generated map is a new folder on disk; the maps slice has to re-scan for
/// it to count as installed anywhere else in the client.
async fn refresh_installed_maps(ctx: &ServiceCtx, out: &EventSink) {
    crate::runtime::run_command(MapsCommand::LoadInstalled.into(), ctx, out).await;
}

/// Fetch available versions and option lists the generator reports.
/// Every rule the dialog checks before a run: the generator's own arithmetic,
/// and whether the chosen styles exist in the release the run would use.
fn validation(
    options: &map_generator::GeneratorOptions,
    out: &EventSink,
) -> Vec<map_generator::ValidationIssue> {
    let mut issues = map_generator::validate_options(options);
    issues.extend(out.with_state(|state| {
        let generator = &state.map_generator;
        let version = options
            .version
            .clone()
            .filter(|version| !version.is_empty())
            .or_else(|| generator.selected_version.clone())
            .unwrap_or_else(|| generator.latest_version.clone());
        faf_domain::state::options_missing_from(&generator.option_lists, options, &version)
    }));
    issues
}

async fn load_options(explicit_version: Option<String>, ctx: &ServiceCtx, out: &EventSink) {
    match ctx.ports.map_generator.available_versions().await {
        Ok(versions) => out.emit(MapGeneratorEvent::VersionsLoaded { versions }),
        // Not notified here: resolving the newest release is about to fail the
        // same way and reports it. Logged, because "the picker offers only
        // Latest" otherwise leaves nothing to look at.
        Err(reason) => tracing::warn!(%reason, "could not list the map generator releases"),
    }

    let resolved_version = if let Some(v) = explicit_version {
        Some(v)
    } else {
        match ctx.ports.map_generator.latest_version().await {
            Ok(version) => {
                out.emit(MapGeneratorEvent::VersionResolved {
                    version: version.clone(),
                });
                Some(version)
            }
            Err(reason) => {
                services::notifications::add_failure(
                    out,
                    services::notifications::Text::new("notifications.msg.generatorUnavailable"),
                    "Could not find a usable map generator",
                    reason,
                );
                None
            }
        }
    };

    // Without a version there is nothing to query: every list would resolve
    // the version again, fail the same way, and spend six more GitHub requests
    // against an hourly budget of sixty per address. The error is already
    // reported above.
    let Some(_) = resolved_version.as_ref() else {
        return;
    };

    // Run all option-list queries in parallel. A failed list is not skipped
    // silently: on a machine without a usable Java the dialog would otherwise
    // open with six empty pickers and no explanation.
    let mut failure: Option<String> = None;
    let queries = GeneratorOptionQuery::ALL;
    let futures = queries.into_iter().map(|query| {
        let map_gen = ctx.ports.map_generator.clone();
        let ver = resolved_version.clone();
        async move {
            let res = map_gen.query_options(query, ver, None).await;
            (query, res)
        }
    });

    let results = futures_util::future::join_all(futures).await;
    for (query, res) in results {
        match res {
            Ok(values) if !values.is_empty() => {
                out.emit(MapGeneratorEvent::OptionListLoaded { query, values });
            }
            Ok(_) => {}
            Err(reason) => {
                tracing::warn!(flag = query.flag(), %reason, "map generator option list failed");
                // The first reason is the informative one: the rest are the
                // same failure repeated once per list.
                failure.get_or_insert(reason);
            }
        }
    }
    // The lists now answer for this release, so the remembered options are
    // checked against them again: a style the previous release had and this
    // one lacks is said in the dialog before Generate, not after.
    let options = out.with_state(|state| state.map_generator.options.clone());
    out.emit(MapGeneratorEvent::ValidationChanged {
        issues: validation(&options, out),
    });
    if let Some(reason) = failure {
        services::notifications::add_failure(
            out,
            services::notifications::Text::new("notifications.msg.generatorOptionsFailed"),
            "Could not read the map generator options",
            reason,
        );
    }
}

/// One IPC payload at a time, including runs whose combined art exceeds the cache budget.
async fn emit_previews(names: &[String], ctx: &ServiceCtx, out: &EventSink) {
    let mut pending: Vec<_> = names.iter().take(64).cloned().collect();
    while !pending.is_empty() {
        let previews = ctx.ports.map_generator.map_previews(&pending).await;
        if previews.is_empty() {
            break;
        }
        let before = pending.len();
        pending.retain(|name| {
            !previews.contains_key(name) && !previews.contains_key(&name.to_lowercase())
        });
        out.emit(MapGeneratorEvent::PreviewsLoaded { previews });
        if pending.len() == before {
            break;
        }
        tokio::task::yield_now().await;
    }
}
