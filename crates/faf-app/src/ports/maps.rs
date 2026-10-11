//! Maps port: browsing the map vault and managing locally installed maps.
//!
//! The impl fetches vault listings from the FAF Data API, and installs by
//! downloading + extracting a version's zip into the user's maps folder
//! (mirrors the Python client's `fa/maps.py::_doDownloadMap` ->
//! `ZipDownloadExtract`). See `infra/maps.rs` for the real implementation.

use std::collections::BTreeMap;

use async_trait::async_trait;
use faf_domain::protocol::vault_query::MapVaultQuery;
use faf_domain::state::{InstalledMap, LocalMapPreview, MatchmakerMapPool, VaultMap};

/// One page of a vault search, plus what the server said about the rest.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MapSearchPage {
    pub maps: Vec<VaultMap>,
    pub total_pages: Option<i32>,
    pub total_records: Option<i32>,
}

#[async_trait]
pub trait MapsPort: Send + Sync {
    /// List the map vault (FAF Data API `/data/map`, `include=latestVersion`
    ///: mirrors the Python client's `MapApiConnector`'s default "All" browse
    /// query, newest first).
    async fn list_vault(&self) -> Result<Vec<VaultMap>, String>;

    async fn list_vault_with_progress(
        &self,
        _progress: Option<tokio::sync::mpsc::Sender<faf_domain::state::maps::CatalogueProgress>>,
    ) -> Result<Vec<VaultMap>, String> {
        self.list_vault().await
    }

    /// One page of a server-side vault search. This is what the Maps tab
    /// browses; [`Self::list_vault`] only feeds the folder-name lookup index.
    async fn search_vault(&self, query: MapVaultQuery) -> Result<MapSearchPage, String>;

    /// Scan the user's maps folder (mirrors `MapsManagerDialog::setup_maplist`
    /// / `fa.maps.getUserMaps`).
    async fn list_installed(&self) -> Result<Vec<InstalledMap>, String>;

    /// Read preview art out of the named installed map folders, as data URLs.
    ///
    /// The last resort behind the remote thumbnails: for the co-op campaign it
    /// is the only copy that exists (see [`LocalMapPreview`]). Folders are
    /// matched on their base name, so `scca_coop_a01` finds
    /// `scca_coop_a01.v0017` on disk. Never fails: a folder that cannot be read
    /// simply comes back empty, which is also how "we looked" is recorded.
    async fn local_previews(&self, _folder_names: &[String]) -> BTreeMap<String, LocalMapPreview> {
        BTreeMap::new()
    }

    /// The vault's record for each named map folder, hidden versions included.
    ///
    /// For folders [`Self::list_vault`] does not cover, which leaves out every
    /// map whose latest version was withdrawn. The record is the map's, with its
    /// latest version, the same shape the catalogue holds, so the lookups that
    /// match on a base folder name treat both alike. Defaults to finding
    /// nothing, which is what a stub needs.
    async fn find_vault_maps_by_folder(
        &self,
        _folder_names: &[String],
    ) -> Result<Vec<VaultMap>, String> {
        Ok(Vec::new())
    }

    /// Load the rating-bracket map pools and veto limits for one queue.
    async fn list_matchmaker_pools(
        &self,
        queue_name: String,
    ) -> Result<Vec<MatchmakerMapPool>, String>;

    /// Download and extract a map version's zip (mirrors
    /// `fa.maps._doDownloadMap`). Returns the refreshed installed list so the
    /// caller doesn't need a separate rescan.
    async fn install_map(
        &self,
        folder_name: String,
        download_url: String,
    ) -> Result<Vec<InstalledMap>, String>;

    /// Delete a map folder (mirrors `MapsManagerDialog::delete_map`). Returns
    /// the refreshed installed list.
    async fn uninstall_map(&self, folder_name: String) -> Result<Vec<InstalledMap>, String>;

    /// Withdraw a map version from the vault, or put it back (mirrors
    /// `MapService.hideMapVersion`, a `PATCH` of the version's `hidden` flag).
    ///
    /// The server authorises this, and it does not authorise the two directions
    /// alike: an author may hide, and only a map administrator may unhide.
    async fn set_map_version_hidden(&self, version_id: i32, hidden: bool) -> Result<(), String>;
}
