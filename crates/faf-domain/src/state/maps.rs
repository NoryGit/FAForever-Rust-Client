//! Maps slice: browsing the map vault and managing locally installed maps.
//!
//! Mirrors the Python client's `vaults/mapvault/` + `fa/maps.py`: the vault
//! list comes from the FAF Data API (`GET /data/map`, `include=latestVersion`),
//! installing downloads the version's zip and extracts it into the user's
//! maps folder (`fa/maps.py::_doDownloadMap` -> `ZipDownloadExtract`), and the
//! "installed" list mirrors the Python client's `MapsManagerDialog`, which
//! scans that same folder. Vault entries carry the discovery, rating and
//! version metadata used by the richer Python and Java vault views.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::protocol::vault_query::MapVaultQuery;
use specta::Type;

/// One map version, as listed from the FAF Data API (`GET /data/map`,
/// `include=latestVersion,author`). Mirrors the Python client's `Map` +
/// `MapVersion` models (`src/api/models/Map.py`, `MapVersion.py`): the
/// client always looks at `map.latestVersion`, never older versions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct VaultMap {
    pub map_id: i32,
    pub version_id: i32,
    pub display_name: String,
    /// `None` for maps without an uploader on record (mirrors `Map.author`
    /// being optional in the Python model).
    pub author: Option<String>,
    /// The uploader's player id, for deciding whether this is the signed-in
    /// player's own upload.
    ///
    /// By id rather than by login, which is what the Python client compares
    /// (`int(item_data.author.xd) == player.id`) before it offers the hide
    /// button: a login can be changed, and "is this mine" then silently stops
    /// being true.
    pub author_id: Option<i32>,
    /// The directory name this version installs as, e.g. `scmp_009.v0001`,
    /// the install/uninstall/"is this installed" key everywhere (mirrors
    /// `MapVersion.folder_name`).
    pub folder_name: String,
    pub version: String,
    pub description: String,
    pub map_type: String,
    pub max_players: i32,
    pub width: i32,
    pub height: i32,
    pub games_played: i32,
    pub version_games_played: i32,
    pub ranked: bool,
    /// Whether the author has withdrawn this version from the vault.
    ///
    /// Always `false` in an ordinary search, which filters hidden versions out
    /// server side; only "my maps" asks for them, so only there is this ever
    /// `true` (mirrors `MapVersion.hidden`, which both reference clients read
    /// on the detail view).
    pub hidden: bool,
    pub recommended: bool,
    /// Average community review score in tenths (for example, `43` = 4.3).
    pub rating_tenths: i32,
    pub reviews: i32,
    /// ISO-8601 creation timestamp from the latest map version.
    pub created_at: String,
    pub download_url: String,
    pub thumbnail_url: String,
    pub thumbnail_url_large: String,
}

/// A map folder already present in the user's maps folder (mirrors the
/// Python client's `MapsManagerDialog`/`fa.maps.getUserMaps`). Just the
/// folder name and a display name derived from it: the Python client's
/// `getDisplayName` fallback for non-official maps, not a full scenario.lua
/// parse (that's `InstalledMapsCache`, a later-phase nicety).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct InstalledMap {
    pub folder_name: String,
    pub display_name: String,
    #[serde(default)]
    pub max_players: i32,
    #[serde(default)]
    pub width: i32,
    #[serde(default)]
    pub height: i32,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    /// When the folder was last written, RFC 3339: in practice when the map
    /// was downloaded, which is what "recently installed" sorts by (#453).
    /// `None` when the file system would not say.
    #[serde(default)]
    pub installed_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct MatchmakerPoolMap {
    pub assignment_id: i32,
    pub display_name: String,
    pub folder_name: String,
    pub max_players: i32,
    pub width: i32,
    pub height: i32,
    pub thumbnail_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct MatchmakerMapPool {
    pub id: i32,
    pub name: String,
    pub min_rating: Option<i32>,
    pub max_rating: Option<i32>,
    pub veto_tokens_per_player: i32,
    pub max_tokens_per_map: i32,
    pub minimum_maps_after_veto: i32,
    pub maps: Vec<MatchmakerPoolMap>,
}

/// Status of a list fetch (vault or installed): separate from
/// [`InstallStatus`], since browsing and installing are independent
/// (mirrors [`crate::state::VaultStatus`] for replays, kept local to avoid
/// coupling the two slices).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum MapListStatus {
    Cancelled,
    #[default]
    Idle,
    Loading,
    Ready,
    Failed {
        reason: String,
    },
}

/// Status of a hide/unhide action for one map version.
///
/// Its own status rather than a reuse of [`MapInstallStatus`]: this touches the
/// vault, not the disk, and the two can be in flight at once.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum MapVisibilityStatus {
    #[default]
    Idle,
    #[serde(rename_all = "camelCase")]
    Working {
        version_id: i32,
    },
    Failed {
        reason: String,
    },
}

impl MapVisibilityStatus {
    /// The version a change is in flight for, if any.
    pub fn working_on(&self) -> Option<i32> {
        match self {
            Self::Working { version_id } => Some(*version_id),
            _ => None,
        }
    }
}

/// Status of an install/uninstall action for one map folder.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum MapInstallStatus {
    #[default]
    Idle,
    #[serde(rename_all = "camelCase")]
    Installing {
        folder_name: String,
    },
    Failed {
        reason: String,
    },
}

/// The preview art an installed map carries in its own folder.
///
/// Vault maps ship `<name>.small.png` and `<name>.large.png` alongside the
/// `.scmap`, and for the co-op campaign that is the *only* copy that exists:
/// the FAF API builds its `thumbnailUrl` from the folder name without ever
/// checking, and `content.faforever.com/maps/previews/` holds no image for any
/// of the campaign missions. Reading the folder is what makes their art appear
/// at all. Remote-first order still applies: this is the last resort.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct LocalMapPreview {
    /// `data:image/png;base64,...`, or `None` when the folder has no such file.
    pub small: Option<String>,
    pub large: Option<String>,
}

impl LocalMapPreview {
    pub fn is_empty(&self) -> bool {
        self.small.is_none() && self.large.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct CatalogueProgress {
    pub pages: u32,
    pub total_pages: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct MapsState {
    /// The whole catalogue, kept as a lookup index: nine features resolve a
    /// map from a folder name through it (`shared/mapPresentation.ts`). It is
    /// deliberately not what the Maps tab browses; see `browse` below.
    pub vault: Vec<VaultMap>,
    pub vault_status: MapListStatus,
    #[serde(default)]
    pub vault_progress: Option<crate::state::maps::CatalogueProgress>,
    /// One page of a server-side vault search, which is what the Maps tab
    /// shows. Both reference clients browse this way rather than filtering a
    /// downloaded catalogue.
    pub browse: Vec<VaultMap>,
    pub browse_status: MapListStatus,
    pub browse_query: MapVaultQuery,
    /// `None` when the server did not report one.
    pub browse_total_pages: Option<i32>,
    pub browse_total_records: Option<i32>,
    pub installed: Vec<InstalledMap>,
    pub installed_status: MapListStatus,
    pub install_status: MapInstallStatus,
    pub visibility_status: MapVisibilityStatus,
    pub matchmaker_pools: BTreeMap<String, Vec<MatchmakerMapPool>>,
    pub matchmaker_pools_status: MapListStatus,
    /// Preview art read out of installed map folders, keyed by the folder's
    /// *base* name (lowercase, `.vNNNN` stripped) so a mission named without a
    /// version still finds the installed copy. A key with an empty value means
    /// "looked, found nothing": it stops the UI asking again every render.
    pub local_previews: BTreeMap<String, LocalMapPreview>,
    /// The keys of `local_previews` in the order they were first read, oldest
    /// first: the eviction queue that keeps that cache bounded.
    ///
    /// A `BTreeMap` has no insertion order of its own, and eviction has to be
    /// identical in both reducers, so the order is state rather than something
    /// either side infers.
    pub local_preview_order: Vec<String>,
}

/// How many map folders' preview art to keep decoded in memory.
///
/// The art is held as `data:` URLs, so it costs its file size plus a third,
/// twice: once in this state and once again in the webview's mirror. Measured
/// on a real installation of 436 maps, the art averages 23 KiB small and
/// 126 KiB large per folder with a worst case above 2 MiB, so an unbounded
/// cache reached roughly 86 MiB on each side once somebody had scrolled the
/// whole Maps tab. One page of that tab is 36 tiles and one request may ask
/// for 64 folders, so this holds several pages' worth and still bounds the
/// cache at a few tens of megabytes. An evicted folder is simply read again.
pub const MAX_LOCAL_PREVIEWS: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum MapsEvent {
    VaultLoading,
    VaultProgress {
        progress: crate::state::maps::CatalogueProgress,
    },
    VaultCancelled,
    VaultSearching,
    /// One page of a vault search. Carries the query it answers so a late
    /// response cannot be mistaken for the current one.
    #[serde(rename_all = "camelCase")]
    VaultSearched {
        maps: Vec<VaultMap>,
        query: MapVaultQuery,
        total_pages: Option<i32>,
        total_records: Option<i32>,
    },
    VaultSearchFailed {
        reason: String,
    },
    VaultLoaded {
        maps: Vec<VaultMap>,
    },
    VaultLoadFailed {
        reason: String,
    },
    /// Catalogue records looked up by folder name for maps the catalogue does
    /// not hold, which in practice means versions withdrawn from the vault.
    /// Added to `vault`, never replacing anything in it.
    VaultFoldersResolved {
        maps: Vec<VaultMap>,
    },
    InstalledLoading,
    InstalledLoaded {
        maps: Vec<InstalledMap>,
    },
    InstalledLoadFailed {
        reason: String,
    },
    /// Keyed by base folder name. Every requested folder gets an entry, even
    /// an empty one, so a fruitless look is remembered rather than repeated.
    LocalPreviewsLoaded {
        previews: BTreeMap<String, LocalMapPreview>,
    },
    MatchmakerPoolsLoading,
    #[serde(rename_all = "camelCase")]
    MatchmakerPoolsLoaded {
        queue_name: String,
        pools: Vec<MatchmakerMapPool>,
    },
    MatchmakerPoolsLoadFailed {
        reason: String,
    },
    // `rename_all` on the enum only renames variant tags, not the fields of
    // struct-like variants (a serde/specta quirk): so multi-word fields need
    // their own per-variant `rename_all` to stay camelCase on the wire.
    #[serde(rename_all = "camelCase")]
    Installing {
        folder_name: String,
    },
    /// Install succeeded: carries the freshly-scanned installed list so the
    /// UI doesn't need a separate `LoadInstalled` round-trip (mirrors the
    /// Python client's `MapsManagerDialog` re-scanning after every change).
    Installed {
        installed: Vec<InstalledMap>,
    },
    InstallFailed {
        reason: String,
    },
    Uninstalled {
        installed: Vec<InstalledMap>,
    },
    UninstallFailed {
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    MapVisibilityChanging {
        version_id: i32,
    },
    /// The vault accepted the change. Carries the version and its new state so
    /// the reducer can correct the lists in place: re-running the search would
    /// move the page under the user, and for a freshly hidden map the entry
    /// would vanish from any view but "my maps".
    #[serde(rename_all = "camelCase")]
    MapVisibilityChanged {
        version_id: i32,
        hidden: bool,
    },
    MapVisibilityFailed {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum MapsCommand {
    CancelVaultLoad,
    /// Fetch the whole catalogue once, as the folder-name lookup index.
    LoadVault,
    /// Fetch one page of a vault search, the way both reference clients
    /// browse. Submit-driven: sent on search, sort and page changes.
    SearchVault {
        query: MapVaultQuery,
    },
    /// Scan the user's maps folder (mirrors `MapsManagerDialog::setup_maplist`).
    LoadInstalled,
    /// Look maps up by folder name that the catalogue does not hold (#323).
    ///
    /// The catalogue is fetched with `latestVersion.hidden=='false'`, which is
    /// right for browsing and wrong for everything else: a map withdrawn from
    /// the vault can still be hosted, joined and downloaded, and "hidden" only
    /// means the vault does not list it. A lobby on such a map found no record,
    /// and the preview service could not stand in, because it names previews
    /// after the version's zip rather than its folder. The record's own
    /// thumbnail URL is the only reliable address, so the UI asks for it once a
    /// tile has run out of other art.
    #[serde(rename_all = "camelCase")]
    ResolveVaultFolders {
        folder_names: Vec<String>,
    },
    /// Read preview art straight out of the named installed map folders.
    ///
    /// On demand rather than with the folder scan: a full maps folder is
    /// several hundred entries, and base64 for all of them would dwarf every
    /// other payload the client sends. Callers ask for the handful they are
    /// about to show, and each folder is read once for good: both sizes at a
    /// time, so a tile and a detail pane never race to re-read the same map.
    #[serde(rename_all = "camelCase")]
    LoadLocalPreviews {
        folder_names: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    LoadMatchmakerPools {
        queue_name: String,
    },
    /// Download and extract a map version's zip (mirrors `maps._doDownloadMap`).
    #[serde(rename_all = "camelCase")]
    InstallMap {
        folder_name: String,
        download_url: String,
    },
    /// Delete a map folder (mirrors `MapsManagerDialog::delete_map`).
    #[serde(rename_all = "camelCase")]
    UninstallMap {
        folder_name: String,
    },
    /// Withdraw a map version from the vault, or put it back (mirrors
    /// `MapService.hideMapVersion`, which only ever hides).
    ///
    /// Both reference clients offer this on your own uploads only, and neither
    /// offers the way back: the API lets an author set `hidden` to `true` and
    /// nothing else, so `hidden: false` needs a map administrator. The command
    /// carries the flag rather than assuming, because the client is not the
    /// authority on that: the server is.
    #[serde(rename_all = "camelCase")]
    SetMapVersionHidden {
        version_id: i32,
        hidden: bool,
    },
}

pub fn reduce(state: &mut MapsState, event: &MapsEvent) {
    match event {
        MapsEvent::VaultLoading => {
            state.vault_status = MapListStatus::Loading;
            state.vault_progress = None;
        }
        MapsEvent::VaultProgress { progress } => state.vault_progress = Some(progress.clone()),
        MapsEvent::VaultCancelled => {
            state.vault_status = MapListStatus::Cancelled;
            state.vault_progress = None;
        }
        MapsEvent::VaultLoaded { maps } => {
            // The catalogue arrives once, after a crawl of the whole vault
            // that takes long enough for the Play tab to have looked up the
            // folders of lobbies already on screen (`VaultFoldersResolved`).
            // Those are exactly the records the catalogue leaves out, and
            // replacing the list dropped them: the map lost its preview and
            // read as unranked "after a while", which is the second half of
            // #385, and nothing asked for the folder again. They are kept.
            let mut vault = maps.clone();
            for known in &state.vault {
                if !vault.iter().any(|map| map.version_id == known.version_id) {
                    vault.push(known.clone());
                }
            }
            state.vault = vault;
            state.vault_status = MapListStatus::Ready;
        }
        MapsEvent::VaultSearching => {
            state.browse_status = MapListStatus::Loading;
            // A refusal belongs to the page it happened on. Without this it
            // would sit above every later search for the rest of the session.
            if matches!(state.visibility_status, MapVisibilityStatus::Failed { .. }) {
                state.visibility_status = MapVisibilityStatus::Idle;
            }
        }
        MapsEvent::VaultSearched {
            maps,
            query,
            total_pages,
            total_records,
        } => {
            state.browse = maps.clone();
            state.browse_query = query.clone();
            state.browse_total_pages = *total_pages;
            state.browse_total_records = *total_records;
            state.browse_status = MapListStatus::Ready;
        }
        MapsEvent::VaultSearchFailed { reason } => {
            state.browse_status = MapListStatus::Failed {
                reason: reason.clone(),
            };
        }
        MapsEvent::VaultLoadFailed { reason } => {
            state.vault_status = MapListStatus::Failed {
                reason: reason.clone(),
            }
        }
        MapsEvent::VaultFoldersResolved { maps } => {
            for map in maps {
                if !state
                    .vault
                    .iter()
                    .any(|known| known.version_id == map.version_id)
                {
                    state.vault.push(map.clone());
                }
            }
        }
        MapsEvent::InstalledLoading => state.installed_status = MapListStatus::Loading,
        MapsEvent::InstalledLoaded { maps } => {
            state.installed = maps.clone();
            state.installed_status = MapListStatus::Ready;
        }
        MapsEvent::InstalledLoadFailed { reason } => {
            state.installed_status = MapListStatus::Failed {
                reason: reason.clone(),
            }
        }
        MapsEvent::LocalPreviewsLoaded { previews } => {
            // Merge per size: a later `large` read must not drop the `small`
            // one an earlier tile already paid for, and vice versa.
            for (folder, preview) in previews {
                let known = state.local_previews.contains_key(folder);
                let entry = state.local_previews.entry(folder.clone()).or_default();
                if preview.small.is_some() {
                    entry.small = preview.small.clone();
                }
                if preview.large.is_some() {
                    entry.large = preview.large.clone();
                }
                if !known {
                    state.local_preview_order.push(folder.clone());
                }
            }
            // Oldest first, so what the user is looking at now survives.
            while state.local_previews.len() > MAX_LOCAL_PREVIEWS
                && !state.local_preview_order.is_empty()
            {
                let evicted = state.local_preview_order.remove(0);
                state.local_previews.remove(&evicted);
            }
        }
        MapsEvent::MatchmakerPoolsLoading => state.matchmaker_pools_status = MapListStatus::Loading,
        MapsEvent::MatchmakerPoolsLoaded { queue_name, pools } => {
            state
                .matchmaker_pools
                .insert(queue_name.clone(), pools.clone());
            state.matchmaker_pools_status = MapListStatus::Ready;
        }
        MapsEvent::MatchmakerPoolsLoadFailed { reason } => {
            state.matchmaker_pools_status = MapListStatus::Failed {
                reason: reason.clone(),
            }
        }
        MapsEvent::Installing { folder_name } => {
            state.install_status = MapInstallStatus::Installing {
                folder_name: folder_name.clone(),
            }
        }
        MapsEvent::Installed { installed } => {
            state.install_status = MapInstallStatus::Idle;
            state.installed = installed.clone();
            state.installed_status = MapListStatus::Ready;
            // A map that had no art a moment ago may have some now, and the
            // empty "looked, found nothing" markers would otherwise outlive the
            // folder they describe.
            state.local_previews.clear();
            state.local_preview_order.clear();
        }
        MapsEvent::InstallFailed { reason } => {
            state.install_status = MapInstallStatus::Failed {
                reason: reason.clone(),
            }
        }
        MapsEvent::Uninstalled { installed } => {
            state.install_status = MapInstallStatus::Idle;
            state.installed = installed.clone();
            state.installed_status = MapListStatus::Ready;
            state.local_previews.clear();
            state.local_preview_order.clear();
        }
        MapsEvent::UninstallFailed { reason } => {
            state.install_status = MapInstallStatus::Failed {
                reason: reason.clone(),
            }
        }
        MapsEvent::MapVisibilityChanging { version_id } => {
            state.visibility_status = MapVisibilityStatus::Working {
                version_id: *version_id,
            }
        }
        MapsEvent::MapVisibilityChanged { version_id, hidden } => {
            state.visibility_status = MapVisibilityStatus::Idle;
            // Both lists: `browse` is what the tab shows, `vault` is the
            // folder-name index nine other features read, and a stale `hidden`
            // there would outlive the page the change was made on.
            for map in state
                .browse
                .iter_mut()
                .chain(state.vault.iter_mut())
                .filter(|map| map.version_id == *version_id)
            {
                map.hidden = *hidden;
            }
        }
        MapsEvent::MapVisibilityFailed { reason } => {
            state.visibility_status = MapVisibilityStatus::Failed {
                reason: reason.clone(),
            }
        }
    }
}

/// A map folder's name without its version suffix, lower-cased:
/// `SCCA_Coop_A01.v0017` is `scca_coop_a01`. Only a real `.v<digits>` is
/// removed; `some_map.version` is a map called that.
///
/// A naming rule, not an adapter detail, so it lives here where both the maps
/// service and the infra that reads folders off disk can use it. It used to be
/// reached by the service from infra by its full path, which the architecture
/// check did not catch.
pub fn base_folder_name(folder_name: &str) -> String {
    let lower = folder_name.trim().to_lowercase();
    match lower.rsplit_once(".v") {
        Some((base, version))
            if !version.is_empty() && version.chars().all(|c| c.is_ascii_digit()) =>
        {
            base.to_string()
        }
        _ => lower,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_folder_name_strips_only_a_real_version_suffix() {
        assert_eq!(base_folder_name("SCCA_Coop_A01.v0017"), "scca_coop_a01");
        assert_eq!(base_folder_name("scca_coop_a01"), "scca_coop_a01");
        // Not a version: the map is called that.
        assert_eq!(base_folder_name("some_map.version"), "some_map.version");
    }

    fn vault_map(folder_name: &str) -> VaultMap {
        VaultMap {
            map_id: 1,
            version_id: 1,
            display_name: "Seton's Clutch".into(),
            author: Some("Rackover".into()),
            author_id: Some(4711),
            folder_name: folder_name.into(),
            version: "1".into(),
            description: "A classic team map.".into(),
            map_type: "skirmish".into(),
            max_players: 8,
            width: 1024,
            height: 1024,
            games_played: 42,
            version_games_played: 40,
            ranked: true,
            hidden: false,
            recommended: false,
            rating_tenths: 45,
            reviews: 12,
            created_at: "2026-01-01T00:00:00Z".into(),
            download_url: "https://content.faforever.com/maps/setons_clutch.zip".into(),
            thumbnail_url: "https://content.faforever.com/maps/setons_clutch.small.png".into(),
            thumbnail_url_large: "https://content.faforever.com/maps/setons_clutch.large.png"
                .into(),
        }
    }

    fn installed_map(folder_name: &str) -> InstalledMap {
        InstalledMap {
            folder_name: folder_name.into(),
            display_name: "Setons Clutch".into(),
            max_players: 8,
            width: 1024,
            height: 1024,
            version: Some("1".into()),
            description: None,
            installed_at: None,
        }
    }

    #[test]
    fn vault_loading_then_loaded() {
        let mut s = MapsState::default();
        assert_eq!(s.vault_status, MapListStatus::Idle);
        reduce(&mut s, &MapsEvent::VaultLoading);
        assert_eq!(s.vault_status, MapListStatus::Loading);
        reduce(
            &mut s,
            &MapsEvent::VaultLoaded {
                maps: vec![vault_map("scmp_009.v0001")],
            },
        );
        assert_eq!(s.vault_status, MapListStatus::Ready);
        assert_eq!(s.vault.len(), 1);
    }

    #[test]
    fn vault_load_failure_records_reason() {
        let mut s = MapsState::default();
        reduce(
            &mut s,
            &MapsEvent::VaultLoadFailed {
                reason: "offline".into(),
            },
        );
        assert_eq!(
            s.vault_status,
            MapListStatus::Failed {
                reason: "offline".into()
            }
        );
    }

    #[test]
    fn hiding_a_version_updates_both_the_page_and_the_lookup_index() {
        // The index is what nine other features resolve a map through, so
        // leaving it stale there would outlive the page this was done on.
        let mut s = MapsState {
            browse: vec![vault_map("scmp_009.v0001")],
            vault: vec![vault_map("scmp_009.v0001")],
            ..MapsState::default()
        };

        reduce(&mut s, &MapsEvent::MapVisibilityChanging { version_id: 1 });
        assert_eq!(
            s.visibility_status,
            MapVisibilityStatus::Working { version_id: 1 }
        );
        assert_eq!(s.visibility_status.working_on(), Some(1));

        reduce(
            &mut s,
            &MapsEvent::MapVisibilityChanged {
                version_id: 1,
                hidden: true,
            },
        );
        assert_eq!(s.visibility_status, MapVisibilityStatus::Idle);
        assert!(s.browse[0].hidden);
        assert!(s.vault[0].hidden, "the lookup index is corrected too");

        // And back again, so the state carries no assumption that this is a
        // one-way door: the server decides that, not the reducer.
        reduce(
            &mut s,
            &MapsEvent::MapVisibilityChanged {
                version_id: 1,
                hidden: false,
            },
        );
        assert!(!s.browse[0].hidden);
    }

    #[test]
    fn a_visibility_change_leaves_other_versions_alone() {
        let other = VaultMap {
            version_id: 2,
            ..vault_map("open_palms.v0001")
        };
        let mut s = MapsState {
            browse: vec![vault_map("scmp_009.v0001"), other],
            ..MapsState::default()
        };

        reduce(
            &mut s,
            &MapsEvent::MapVisibilityChanged {
                version_id: 2,
                hidden: true,
            },
        );
        assert!(!s.browse[0].hidden);
        assert!(s.browse[1].hidden);
    }

    #[test]
    fn a_refused_visibility_change_keeps_the_servers_wording() {
        // The one refusal an author will actually meet: unhiding is a map
        // administrator's action, so the reason has to survive to the dialog.
        let mut s = MapsState::default();
        reduce(&mut s, &MapsEvent::MapVisibilityChanging { version_id: 7 });
        reduce(
            &mut s,
            &MapsEvent::MapVisibilityFailed {
                reason: "only a map administrator can unhide a version".into(),
            },
        );
        assert_eq!(
            s.visibility_status,
            MapVisibilityStatus::Failed {
                reason: "only a map administrator can unhide a version".into()
            }
        );
        assert_eq!(s.visibility_status.working_on(), None);
    }

    #[test]
    fn installed_loading_then_loaded() {
        let mut s = MapsState::default();
        reduce(&mut s, &MapsEvent::InstalledLoading);
        assert_eq!(s.installed_status, MapListStatus::Loading);
        reduce(
            &mut s,
            &MapsEvent::InstalledLoaded {
                maps: vec![installed_map("scmp_009.v0001")],
            },
        );
        assert_eq!(s.installed_status, MapListStatus::Ready);
        assert_eq!(s.installed.len(), 1);
    }

    #[test]
    fn install_flow_updates_installed_list_and_resets_status() {
        let mut s = MapsState::default();
        reduce(
            &mut s,
            &MapsEvent::Installing {
                folder_name: "scmp_009.v0001".into(),
            },
        );
        assert_eq!(
            s.install_status,
            MapInstallStatus::Installing {
                folder_name: "scmp_009.v0001".into()
            }
        );
        reduce(
            &mut s,
            &MapsEvent::Installed {
                installed: vec![installed_map("scmp_009.v0001")],
            },
        );
        assert_eq!(s.install_status, MapInstallStatus::Idle);
        assert_eq!(s.installed.len(), 1);
        assert_eq!(s.installed_status, MapListStatus::Ready);
    }

    #[test]
    fn install_failure_records_reason() {
        let mut s = MapsState::default();
        reduce(
            &mut s,
            &MapsEvent::InstallFailed {
                reason: "download failed".into(),
            },
        );
        assert_eq!(
            s.install_status,
            MapInstallStatus::Failed {
                reason: "download failed".into()
            }
        );
    }

    #[test]
    fn uninstall_updates_installed_list() {
        let mut s = MapsState {
            installed: vec![installed_map("scmp_009.v0001")],
            installed_status: MapListStatus::Ready,
            ..Default::default()
        };
        reduce(&mut s, &MapsEvent::Uninstalled { installed: vec![] });
        assert_eq!(s.install_status, MapInstallStatus::Idle);
        assert!(s.installed.is_empty());
    }

    fn preview_of(folder: &str) -> MapsEvent {
        MapsEvent::LocalPreviewsLoaded {
            previews: BTreeMap::from([(
                folder.to_string(),
                LocalMapPreview {
                    small: Some("data:art".into()),
                    large: None,
                },
            )]),
        }
    }

    /// Twin of "evicts the oldest entries once the cache is full" in
    /// `ui/src/store/reducers/maps.test.ts`.
    #[test]
    fn the_preview_cache_evicts_its_oldest_entries() {
        let mut s = MapsState::default();
        for index in 0..(MAX_LOCAL_PREVIEWS + 12) {
            reduce(&mut s, &preview_of(&format!("map_{index:04}")));
        }

        assert_eq!(s.local_previews.len(), MAX_LOCAL_PREVIEWS);
        assert_eq!(s.local_preview_order.len(), MAX_LOCAL_PREVIEWS);
        assert!(!s.local_previews.contains_key("map_0000"));
        assert!(!s.local_previews.contains_key("map_0011"));
        assert!(s.local_previews.contains_key("map_0012"));
        assert!(s.local_previews.contains_key("map_0139"));
        assert_eq!(
            s.local_preview_order.first().map(String::as_str),
            Some("map_0012")
        );
    }

    #[test]
    fn a_second_size_does_not_queue_the_same_folder_twice() {
        let mut s = MapsState::default();
        reduce(&mut s, &preview_of("one_map"));
        reduce(
            &mut s,
            &MapsEvent::LocalPreviewsLoaded {
                previews: BTreeMap::from([(
                    "one_map".to_string(),
                    LocalMapPreview {
                        small: None,
                        large: Some("data:large".into()),
                    },
                )]),
            },
        );

        assert_eq!(s.local_preview_order, vec!["one_map".to_string()]);
        assert_eq!(
            s.local_previews
                .get("one_map")
                .and_then(|p| p.small.clone()),
            Some("data:art".to_string())
        );
    }

    #[test]
    fn installing_empties_the_eviction_queue_with_the_cache() {
        let mut s = MapsState::default();
        reduce(&mut s, &preview_of("one_map"));
        reduce(&mut s, &MapsEvent::Installed { installed: vec![] });

        assert!(s.local_previews.is_empty());
        assert!(s.local_preview_order.is_empty());
    }

    #[test]
    fn a_resolved_folder_is_added_to_the_index_once() {
        let mut state = MapsState::default();
        reduce(
            &mut state,
            &MapsEvent::VaultLoaded {
                maps: vec![vault_map("scmp_009.v0001")],
            },
        );
        let mut withdrawn = vault_map("withdrawn.v0002");
        withdrawn.version_id = 2;
        withdrawn.hidden = true;
        for _ in 0..2 {
            reduce(
                &mut state,
                &MapsEvent::VaultFoldersResolved {
                    maps: vec![withdrawn.clone(), vault_map("scmp_009.v0001")],
                },
            );
        }
        assert_eq!(
            state
                .vault
                .iter()
                .map(|map| map.folder_name.as_str())
                .collect::<Vec<_>>(),
            ["scmp_009.v0001", "withdrawn.v0002"]
        );
    }

    #[test]
    fn the_catalogue_arriving_late_keeps_the_folders_already_resolved() {
        // The Play tab looks a lobby's map up while the catalogue is still
        // being crawled; the crawl finishing must not take that record away
        // again (#385: the preview vanished and the map read as unranked).
        let mut state = MapsState::default();
        let mut old_map = vault_map("phenom spartiate v2");
        old_map.version_id = 7;
        reduce(
            &mut state,
            &MapsEvent::VaultFoldersResolved {
                maps: vec![old_map],
            },
        );
        reduce(
            &mut state,
            &MapsEvent::VaultLoaded {
                maps: vec![vault_map("scmp_009.v0001")],
            },
        );
        assert_eq!(
            state
                .vault
                .iter()
                .map(|map| map.folder_name.as_str())
                .collect::<Vec<_>>(),
            ["scmp_009.v0001", "phenom spartiate v2"]
        );
    }
}
