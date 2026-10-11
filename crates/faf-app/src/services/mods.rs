//! Mods service.
//!
//! Thin handler (like `services/maps.rs`): asks the [`crate::ports::
//! ModsPort`] to do the work, then emits the corresponding events. The
//! actual API calls, folder scan, zip extraction, and `game.prefs`
//! read/write live entirely behind the port: see `infra/mods.rs`.

use faf_domain::state::{ModListStatus, ModsCommand, ModsEvent};

use crate::runtime::{CancellationSlot, EventSink, LatestRequest, ServiceCtx};

/// The mod vault's request generation. Owned by this service.
#[derive(Default)]
pub struct ModsContext {
    /// Only the newest vault search may land, for the same reason as the map
    /// vault's: a slow earlier query answering after a fast later one would
    /// otherwise replace its page with results for filters no longer on
    /// screen.
    search_generation: LatestRequest,
    catalogue: CancellationSlot,
}

pub async fn handle(cmd: ModsCommand, ctx: &ServiceCtx, out: &EventSink) {
    match cmd {
        ModsCommand::CancelVaultLoad => ctx.mods.catalogue.cancel(),
        ModsCommand::LoadVault => {
            // Same guard, same reason, as `services::maps`: one crawl.
            if out.with_state(|state| {
                matches!(
                    state.mods.vault_status,
                    ModListStatus::Loading | ModListStatus::Ready
                )
            }) {
                return;
            }
            crawl_vault(ctx, out).await;
        }
        ModsCommand::ReloadVault => {
            // Asked for by a person, so a loaded catalogue is not a reason to
            // refuse. A crawl already running is: it is the answer they want.
            if out.with_state(|state| state.mods.vault_status == ModListStatus::Loading) {
                return;
            }
            crawl_vault(ctx, out).await;
        }
        ModsCommand::SearchVault { query } => {
            // Same newest-wins rule as `services::maps`: an older query that
            // answers late must not replace the newer one's page, totals or
            // error. Separate from the catalogue crawl, which is single-flight
            // and has no newer request to lose to.
            let generation = ctx.mods.search_generation.begin();
            out.emit(ModsEvent::VaultSearching);
            let result = ctx.ports.mods.search_vault(query.clone()).await;
            if !ctx.mods.search_generation.is_current(generation) {
                return;
            }
            match result {
                Ok(page) => out.emit(ModsEvent::VaultSearched {
                    mods: page.mods,
                    query,
                    total_pages: page.total_pages,
                    total_records: page.total_records,
                }),
                Err(reason) => out.emit(ModsEvent::VaultSearchFailed { reason }),
            }
        }
        ModsCommand::QueryDownloadSizes { targets } => {
            // No "loading" event: the dialog that asks is drawn and usable
            // before the answer arrives, and a spinner on a courtesy number
            // would be more noise than the number is worth.
            if targets.is_empty() {
                return;
            }
            let sizes = ctx.ports.mods.download_sizes(targets).await;
            out.emit(ModsEvent::DownloadSizesResolved { sizes });
        }
        ModsCommand::LoadInstalled => {
            // Same as `services::maps`: one scan at a time, repeated on demand.
            if out.with_state(|state| state.mods.installed_status == ModListStatus::Loading) {
                return;
            }
            out.emit(ModsEvent::InstalledLoading);
            match ctx.ports.mods.list_installed().await {
                Ok(mods) => out.emit(ModsEvent::InstalledLoaded { mods }),
                Err(reason) => out.emit(ModsEvent::InstalledLoadFailed { reason }),
            }
        }
        ModsCommand::InstallMod { uid, download_url } => {
            crate::runtime::expect_admitted(crate::runtime::Key::ModFiles);
            out.emit(ModsEvent::Installing { uid: uid.clone() });
            match ctx.ports.mods.install_mod(uid, download_url).await {
                Ok(installed) => out.emit(ModsEvent::Installed { installed }),
                Err(reason) => out.emit(ModsEvent::InstallFailed { reason }),
            }
        }
        ModsCommand::UpdateMod {
            uid,
            folder_name,
            download_url,
        } => {
            crate::runtime::expect_admitted(crate::runtime::Key::ModFiles);
            // The same status an install shows: from the user's side this *is*
            // an install, and the row it belongs to is named by the new uid.
            out.emit(ModsEvent::Installing { uid: uid.clone() });
            match ctx
                .ports
                .mods
                .update_mod(uid, folder_name, download_url)
                .await
            {
                Ok(installed) => out.emit(ModsEvent::Installed { installed }),
                Err(reason) => out.emit(ModsEvent::InstallFailed { reason }),
            }
        }
        ModsCommand::UninstallMod { folder_name, uid } => {
            crate::runtime::expect_admitted(crate::runtime::Key::ModFiles);
            out.emit(ModsEvent::Installing { uid });
            match ctx.ports.mods.uninstall_mod(folder_name).await {
                Ok(installed) => out.emit(ModsEvent::Uninstalled { installed }),
                Err(reason) => out.emit(ModsEvent::UninstallFailed { reason }),
            }
        }
        ModsCommand::ToggleMod { uid, enabled } => {
            crate::runtime::expect_admitted(crate::runtime::Key::ModFiles);
            out.emit(ModsEvent::Toggling { uid: uid.clone() });
            match ctx.ports.mods.toggle_mod(uid, enabled).await {
                Ok(installed) => out.emit(ModsEvent::Toggled { installed }),
                Err(reason) => out.emit(ModsEvent::ToggleFailed { reason }),
            }
        }
        ModsCommand::SetActiveMods { uids } => {
            crate::runtime::expect_admitted(crate::runtime::Key::ModFiles);
            // No `Toggling` first: that status names a single uid, and this is
            // one short write rather than something worth showing progress for.
            match ctx.ports.mods.set_active_mods(uids).await {
                Ok(installed) => out.emit(ModsEvent::Toggled { installed }),
                Err(reason) => out.emit(ModsEvent::ToggleFailed { reason }),
            }
        }
    }
}

/// Read the whole catalogue and report it. The caller decides whether a crawl
/// is wanted; the data already loaded stays on screen until this replaces it.
async fn crawl_vault(ctx: &ServiceCtx, out: &EventSink) {
    crate::runtime::expect_admitted(crate::runtime::Key::ModVault);
    let cancel = ctx.mods.catalogue.begin();
    out.emit(ModsEvent::VaultLoading);
    // Logged both ways with its duration, as the map vault's is: the crawl is
    // many pages, and the Live and Play tabs start it as they open.
    let started = std::time::Instant::now();
    tracing::info!("mod vault: loading the catalogue");
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(32);
    let request = ctx.ports.mods.list_vault_with_progress(Some(progress_tx));
    tokio::pin!(request);
    let result = loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                out.emit(ModsEvent::VaultCancelled);
                return;
            }
            result = &mut request => break result,
            Some(progress) = progress_rx.recv() => out.emit(ModsEvent::VaultProgress { progress }),
        }
    };
    while let Ok(progress) = progress_rx.try_recv() {
        out.emit(ModsEvent::VaultProgress { progress });
    }
    match result {
        Ok(mods) => {
            tracing::info!(
                mods = mods.len(),
                seconds = started.elapsed().as_secs_f32(),
                "mod vault: loaded"
            );
            out.emit(ModsEvent::VaultLoaded { mods })
        }
        Err(reason) => {
            tracing::warn!(
                %reason,
                seconds = started.elapsed().as_secs_f32(),
                "mod vault: loading failed"
            );
            out.emit(ModsEvent::VaultLoadFailed { reason })
        }
    }
}
