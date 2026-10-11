//! Maps service.
//!
//! Thin handler (like `services/replays.rs`): asks the [`MapsPort`] to do the
//! work, then emits the corresponding events. The actual API calls, folder
//! scan and zip extraction live entirely behind the port: see `infra/maps.rs`.

use faf_domain::state::{MapListStatus, MapsCommand, MapsEvent};

use crate::runtime::{CancellationSlot, EventSink, LatestRequest, ServiceCtx};

/// The map vault's request generation. Owned by this service.
#[derive(Default)]
pub struct MapsContext {
    /// Only the newest vault search may land. A slow earlier query answering
    /// after a fast later one would otherwise replace its page, its totals or
    /// its error with results for filters no longer on screen.
    search_generation: LatestRequest,
    catalogue: CancellationSlot,
}

pub async fn handle(cmd: MapsCommand, ctx: &ServiceCtx, out: &EventSink) {
    match cmd {
        MapsCommand::CancelVaultLoad => ctx.maps.catalogue.cancel(),
        MapsCommand::LoadVault => {
            crate::runtime::expect_admitted(crate::runtime::Key::MapVault);
            // Crawling the whole catalogue is the most expensive thing this
            // client does, so it happens once. Seven of the nine callers
            // checked `vaultStatus` themselves before sending this; the two on
            // the Play tab did not, so opening Play, and the host dialog, threw
            // a finished crawl away and started it again on every mount. The
            // check belongs here, where a new caller cannot forget it.
            //
            // A previous failure is still retried: only "already loaded" and
            // "already in flight" are reasons to do nothing. "In flight" is the
            // command policy's single flight (`Key::MapVault`), taken before
            // this runs, so two callers mounting together cannot both start a
            // crawl.
            if out.with_state(|state| matches!(state.maps.vault_status, MapListStatus::Ready)) {
                return;
            }
            let cancel = ctx.maps.catalogue.begin();
            out.emit(MapsEvent::VaultLoading);
            // Logged both ways, with how long it took: the crawl is many pages,
            // one failed page fails it, and every view that mounts afterwards
            // starts it again. Without a line here a "Loading map vault" that
            // keeps coming back left nothing to read afterwards.
            let started = std::time::Instant::now();
            tracing::info!("map vault: loading the catalogue");
            let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(32);
            let request = ctx.ports.maps.list_vault_with_progress(Some(progress_tx));
            tokio::pin!(request);
            let result = loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => {
                        out.emit(MapsEvent::VaultCancelled);
                        return;
                    }
                    result = &mut request => break result,
                    Some(progress) = progress_rx.recv() => out.emit(MapsEvent::VaultProgress { progress }),
                }
            };
            while let Ok(progress) = progress_rx.try_recv() {
                out.emit(MapsEvent::VaultProgress { progress });
            }
            match result {
                Ok(maps) => {
                    tracing::info!(
                        maps = maps.len(),
                        seconds = started.elapsed().as_secs_f32(),
                        "map vault: loaded"
                    );
                    out.emit(MapsEvent::VaultLoaded { maps })
                }
                Err(reason) => {
                    tracing::warn!(
                        %reason,
                        seconds = started.elapsed().as_secs_f32(),
                        "map vault: loading failed; the next view that needs it tries again"
                    );
                    out.emit(MapsEvent::VaultLoadFailed { reason })
                }
            }
        }
        MapsCommand::SearchVault { query } => {
            // No guard and no dedupe beyond the generation check: this is a
            // user-driven search, and asking again is exactly what the search
            // button means. Commands run concurrently, so a slow earlier query
            // can answer after a fast later one; whichever started last owns
            // the results, the totals and the error line, and anything older
            // is dropped whether it succeeded or failed.
            let generation = ctx.maps.search_generation.begin();
            out.emit(MapsEvent::VaultSearching);
            let result = ctx.ports.maps.search_vault(query.clone()).await;
            if !ctx.maps.search_generation.is_current(generation) {
                return;
            }
            match result {
                Ok(page) => out.emit(MapsEvent::VaultSearched {
                    maps: page.maps,
                    query,
                    total_pages: page.total_pages,
                    total_records: page.total_records,
                }),
                Err(reason) => out.emit(MapsEvent::VaultSearchFailed { reason }),
            }
        }
        MapsCommand::LoadInstalled => {
            // Every view that shows installed maps asks on mount, and the
            // host dialog asks on open, so two scans of the same folders can
            // be requested within a frame. One at a time; the result of the
            // one in flight is the answer both wanted. A finished scan is
            // repeated on purpose: the folder changes under the client.
            if out.with_state(|state| state.maps.installed_status == MapListStatus::Loading) {
                return;
            }
            out.emit(MapsEvent::InstalledLoading);
            match ctx.ports.maps.list_installed().await {
                // No previews here any more. Every generated map's picture
                // used to go out with every scan, the whole folder in one
                // event: see `MapGeneratorCommand::LoadPreviews`, which a tile
                // now sends for the map it shows (#402).
                Ok(maps) => out.emit(MapsEvent::InstalledLoaded { maps }),
                Err(reason) => out.emit(MapsEvent::InstalledLoadFailed { reason }),
            }
        }
        MapsCommand::ResolveVaultFolders { folder_names } => {
            // Only what the index still lacks: a sibling tile may have asked
            // for the same folder and been answered in the meantime.
            let wanted: Vec<String> = out.with_state(|state| {
                folder_names
                    .iter()
                    .filter(|name| {
                        let base = faf_domain::state::maps::base_folder_name(name);
                        !state.maps.vault.iter().any(|map| {
                            faf_domain::state::maps::base_folder_name(&map.folder_name) == base
                        })
                    })
                    .cloned()
                    .collect()
            });
            if wanted.is_empty() {
                return;
            }
            match ctx.ports.maps.find_vault_maps_by_folder(&wanted).await {
                Ok(maps) if !maps.is_empty() => {
                    out.emit(MapsEvent::VaultFoldersResolved { maps });
                }
                Ok(_) => tracing::debug!(?wanted, "no vault record for these map folders"),
                // Quiet on purpose: the tile already shows its placeholder, and
                // this lookup is a nicety on top of a lobby that works anyway.
                Err(reason) => {
                    tracing::debug!(%reason, ?wanted, "could not look map folders up");
                }
            }
        }
        MapsCommand::LoadLocalPreviews { folder_names } => {
            // Only what has not been looked at yet. The event records an empty
            // result too, so a map whose folder holds no art is asked about
            // once and never again, which matters because the UI asks from
            // an image's error handler, and that fires on every render.
            let wanted: Vec<String> = out.with_state(|state| {
                folder_names
                    .iter()
                    .filter(|name| {
                        !state
                            .maps
                            .local_previews
                            .contains_key(&faf_domain::state::maps::base_folder_name(name))
                    })
                    .cloned()
                    .collect()
            });
            if wanted.is_empty() {
                return;
            }
            let previews = ctx.ports.maps.local_previews(&wanted).await;
            if !previews.is_empty() {
                out.emit(MapsEvent::LocalPreviewsLoaded { previews });
            }
        }
        MapsCommand::LoadMatchmakerPools { queue_name } => {
            out.emit(MapsEvent::MatchmakerPoolsLoading);
            match ctx
                .ports
                .maps
                .list_matchmaker_pools(queue_name.clone())
                .await
            {
                Ok(pools) => out.emit(MapsEvent::MatchmakerPoolsLoaded { queue_name, pools }),
                Err(reason) => out.emit(MapsEvent::MatchmakerPoolsLoadFailed { reason }),
            }
        }
        MapsCommand::InstallMap {
            folder_name,
            download_url,
        } => {
            crate::runtime::expect_admitted(crate::runtime::Key::MapFiles);
            out.emit(MapsEvent::Installing {
                folder_name: folder_name.clone(),
            });
            match ctx.ports.maps.install_map(folder_name, download_url).await {
                Ok(installed) => out.emit(MapsEvent::Installed { installed }),
                Err(reason) => out.emit(MapsEvent::InstallFailed { reason }),
            }
        }
        MapsCommand::UninstallMap { folder_name } => {
            crate::runtime::expect_admitted(crate::runtime::Key::MapFiles);
            out.emit(MapsEvent::Installing {
                folder_name: folder_name.clone(),
            });
            match ctx.ports.maps.uninstall_map(folder_name).await {
                Ok(installed) => out.emit(MapsEvent::Uninstalled { installed }),
                Err(reason) => out.emit(MapsEvent::UninstallFailed { reason }),
            }
        }
        MapsCommand::SetMapVersionHidden { version_id, hidden } => {
            // One at a time: a second click while the first `PATCH` is in
            // flight would race the reducer's in-place correction, and the two
            // could settle on opposite flags.
            if out.with_state(|state| state.maps.visibility_status.working_on().is_some()) {
                return;
            }
            out.emit(MapsEvent::MapVisibilityChanging { version_id });
            match ctx
                .ports
                .maps
                .set_map_version_hidden(version_id, hidden)
                .await
            {
                Ok(()) => out.emit(MapsEvent::MapVisibilityChanged { version_id, hidden }),
                Err(reason) => out.emit(MapsEvent::MapVisibilityFailed { reason }),
            }
        }
    }
}
