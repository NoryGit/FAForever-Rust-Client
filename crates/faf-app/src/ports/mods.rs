//! Mods port: browsing the mod vault, managing locally installed mods,
//! and enabling/disabling them.
//!
//! The impl fetches vault listings from the FAF Data API, installs by
//! downloading + extracting a version's zip into the user's mods folder
//! (mirrors the Python client's `fa/mods.py`/`vaults/modvault/utils.py`),
//! and toggles by reading/rewriting FA's own `game.prefs` file's
//! `active_mods` table. See `infra/mods.rs` for the real implementation.

use std::collections::BTreeMap;

use async_trait::async_trait;
use faf_domain::protocol::vault_query::ModVaultQuery;
use faf_domain::state::{
    InstalledMod, ModDownloadSize, ModDownloadTarget, ModVersionConflict, VaultMod,
};

/// One page of a mod vault search. Mirrors `MapSearchPage`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ModSearchPage {
    pub mods: Vec<VaultMod>,
    pub total_pages: Option<i32>,
    pub total_records: Option<i32>,
}

#[async_trait]
pub trait ModsPort: Send + Sync {
    /// List the mod vault (FAF Data API `/data/mod`, `include=latestVersion`
    ///: mirrors the current default "newest first" posture of
    /// `MapsPort::list_vault`).
    async fn list_vault(&self) -> Result<Vec<VaultMod>, String>;

    async fn list_vault_with_progress(
        &self,
        _progress: Option<tokio::sync::mpsc::Sender<faf_domain::state::maps::CatalogueProgress>>,
    ) -> Result<Vec<VaultMod>, String> {
        self.list_vault().await
    }

    /// One page of a server-side vault search, as `MapsPort::search_vault`.
    async fn search_vault(&self, query: ModVaultQuery) -> Result<ModSearchPage, String>;

    /// Scan the user's mods folder, cross-referenced against `game.prefs`'s
    /// `active_mods` table for each mod's `enabled` state.
    async fn list_installed(&self) -> Result<Vec<InstalledMod>, String>;

    /// How big these archives are, without fetching them.
    ///
    /// A HEAD per target, because the API's `mod` resource carries a download
    /// URL and no file length: asking the storage server is the only way to
    /// answer "how much is this join going to download". One entry back per
    /// target, with `bytes: None` for the ones that did not say, so a caller
    /// can tell an answer of "unknown" from no answer at all.
    ///
    /// Never fails as a whole. A size is a courtesy on a dialog that works
    /// without it, and a mod vault that is slow to answer must not be able to
    /// hold up a join.
    async fn download_sizes(&self, targets: Vec<ModDownloadTarget>) -> Vec<ModDownloadSize>;

    /// Download and extract a mod version's zip. Returns the refreshed
    /// installed list so the caller doesn't need a separate rescan.
    async fn install_mod(
        &self,
        uid: String,
        download_url: String,
    ) -> Result<Vec<InstalledMod>, String>;

    /// Replace the version in `folder_name` with the one `uid` names.
    ///
    /// The archive is fully extracted and validated in staging before the
    /// installed copy is moved aside. A failed swap restores that copy. A mod
    /// that was enabled stays enabled under the new version's uid.
    async fn update_mod(
        &self,
        uid: String,
        folder_name: String,
        download_url: String,
    ) -> Result<Vec<InstalledMod>, String>;

    /// Delete a mod folder and remove its uid from `game.prefs`'s active
    /// set if present. Returns the refreshed installed list.
    async fn uninstall_mod(&self, folder_name: String) -> Result<Vec<InstalledMod>, String>;

    /// Enable or disable an installed mod without uninstalling it. Returns
    /// the refreshed installed list.
    async fn toggle_mod(&self, uid: String, enabled: bool) -> Result<Vec<InstalledMod>, String>;

    /// Replace the active set with exactly `uids`, and return the refreshed
    /// installed list. Bulk on purpose: see `ModsCommand::SetActiveMods`.
    async fn set_active_mods(&self, uids: Vec<String>) -> Result<Vec<InstalledMod>, String>;

    /// Install missing simulation mods required by a game and enable them.
    /// Already-installed versions are retained: this is compatibility
    /// preparation, not the intentionally excluded automatic mod updater.
    ///
    /// `mods` is the game's `sim_mods` table, uid to the name the host's client
    /// published, which is the only name available for the conflict prompt.
    ///
    /// Stops with [`ModPrepFailure::Conflicts`], having changed nothing, when a
    /// required version wants a folder a different version already occupies.
    /// `replace_conflicts` is the user's answer to that prompt and is the only
    /// way this ever deletes an installed mod.
    async fn ensure_game_mods(
        &self,
        mods: &BTreeMap<String, String>,
        replace_conflicts: bool,
    ) -> Result<(), ModPrepFailure>;
}

/// Why simulation-mod preparation stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModPrepFailure {
    /// Nothing was installed or deleted. The join can be retried with
    /// `replace_conflicts` once the user has approved these.
    Conflicts(Vec<ModVersionConflict>),
    /// Anything else: the vault lookup, the download, the extraction.
    Failed(String),
}

impl std::fmt::Display for ModPrepFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(reason) => f.write_str(reason),
            Self::Conflicts(conflicts) => {
                let names: Vec<&str> = conflicts
                    .iter()
                    .map(|conflict| conflict.required_name.as_str())
                    .collect();
                write!(
                    f,
                    "another version of {} is already installed",
                    names.join(", ")
                )
            }
        }
    }
}
