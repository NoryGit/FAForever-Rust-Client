//! Mods slice: browsing the mod vault, local install management, and
//! enabling/disabling installed mods.
//!
//! Mirrors the Python client's `vaults/modvault/` + `fa/mods.py` (primary
//! source; cross-checked against the Java client's `ModService.java`,
//! which does the same thing more verbosely): the vault list comes from
//! the FAF Data API (`GET /data/mod`, `include=latestVersion`), installing
//! downloads the version's zip and extracts it into the user's mods
//! folder, and the "installed" list scans that same folder: same shape as
//! [`crate::state::MapsState`]. The one real delta from maps: mods can be
//! individually enabled/disabled without uninstalling: both reference
//! clients do this by reading/rewriting FA's own `game.prefs` file's
//! `active_mods = { ['uid'] = true, ... }` table (see
//! `context/python_client/src/vaults/modvault/utils.py::setActiveMods`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::protocol::vault_query::ModVaultQuery;
use specta::Type;

/// UI mods are cosmetic/interface-only and freely toggleable; SIM mods
/// affect game logic and matter for replay/multiplayer compatibility.
/// Mirrors the Python client's `ui_only` flag (`ModInfo.ui_only`) and the
/// Java client's `ModType` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub enum ModType {
    Ui,
    Sim,
}

/// One mod version, as listed from the FAF Data API (`GET /data/mod`,
/// `include=latestVersion`): the client always looks at `mod.latestVersion`,
/// same posture as [`crate::state::VaultMap`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct VaultMod {
    pub mod_id: i32,
    pub version_id: i32,
    pub display_name: String,
    /// Flat attribute on the `mod` resource itself, not a relationship
    /// (unlike [`crate::state::VaultMap`]'s `author`, which is a `player`
    /// relationship): confirmed against the Java client's `Mod.author`.
    pub author: String,
    pub uploader: String,
    /// The uploader's player id, for deciding whether this is the signed-in
    /// player's own upload.
    ///
    /// The uploader rather than `author`, because only this one is evidence:
    /// `author` is free text the mod's `mod_info.lua` declares, while the API
    /// treats `uploader` as the owner (`Mod.getEntityOwner`). By id rather than
    /// by login, because a login can be changed.
    pub uploader_id: Option<i32>,
    /// `latestVersion.uid`: the stable id matched against locally
    /// installed mods and `game.prefs`'s `active_mods` table. Distinct
    /// from the numeric JSON:API resource `id`.
    pub uid: String,
    /// Stored as text: FAF mod versions aren't reliably semver (often a
    /// bare integer).
    pub version: String,
    pub description: String,
    pub filename: String,
    pub mod_type: ModType,
    pub ranked: bool,
    pub recommended: bool,
    /// Average community review score in tenths (for example, `43` = 4.3).
    pub rating_tenths: i32,
    pub reviews: i32,
    pub created_at: String,
    pub updated_at: String,
    pub download_url: String,
    pub thumbnail_url: String,
}

/// A mod folder already present in the user's mods folder, cross-referenced
/// against `game.prefs`'s `active_mods` table for `enabled`. Mirrors the
/// Python client's `ModInfo` (`vaults/modvault/utils.py`) as parsed from
/// each folder's `mod_info.lua`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct InstalledMod {
    pub folder_name: String,
    pub uid: String,
    pub display_name: String,
    pub version: String,
    pub author: String,
    pub description: String,
    pub mod_type: ModType,
    pub enabled: bool,
}

/// A mod folder that already holds a different version than a game needs.
///
/// FAF simulation-mod uids are per *version*, so a host on an older release of
/// a mod asks for a uid nobody who has the newer one is holding. The folder is
/// the same either way, and the client cannot install both: it has to replace
/// one with the other, which destroys whatever the user had. That is a
/// decision for the user, so the join stops here and asks, exactly as the
/// Python client's `downloadMod` does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct ModVersionConflict {
    /// The uid the host's game requires.
    pub required_uid: String,
    /// What the vault calls that mod, for the prompt.
    pub required_name: String,
    /// The vault's version number for the required uid, so the prompt can set
    /// the host's version against the installed one. Empty when unknown.
    pub required_version: String,
    /// The folder both versions want, relative to the mods directory.
    pub folder_name: String,
    /// The version standing in the way.
    pub installed_uid: String,
    pub installed_name: String,
    pub installed_version: String,
}

/// A named set of mods the user can put back in one click.
///
/// Keyed by `uid` rather than folder name, because `uid` is what `game.prefs`'s
/// `active_mods` table stores and what survives a mod being reinstalled from the
/// vault under a different folder.
///
/// A preset describes the *complete* wanted state, so applying one also disables
/// every mod it does not name. That is the point of it: turn everything off to
/// watch an old replay, then get exactly the previous set back afterwards.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct ModPreset {
    pub name: String,
    pub uids: Vec<String>,
}

/// Status of a list fetch (vault or installed): separate from
/// [`ModInstallStatus`]/[`ModToggleStatus`], mirrors
/// [`crate::state::MapListStatus`].
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum ModListStatus {
    Cancelled,
    #[default]
    Idle,
    Loading,
    Ready,
    Failed {
        reason: String,
    },
}

/// Status of an install/uninstall action for one mod. Mirrors
/// [`crate::state::MapInstallStatus`].
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum ModInstallStatus {
    #[default]
    Idle,
    #[serde(rename_all = "camelCase")]
    Installing {
        uid: String,
    },
    Failed {
        reason: String,
    },
}

/// Status of an enable/disable action for one installed mod. Separate from
/// [`ModInstallStatus`] since toggling and installing are independent
/// actions a user could trigger back to back.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum ModToggleStatus {
    #[default]
    Idle,
    #[serde(rename_all = "camelCase")]
    Toggling {
        uid: String,
    },
    Failed {
        reason: String,
    },
}

/// One mod's archive, as the join dialog needs to ask about it.
///
/// The URL comes from the caller rather than being looked up here, the same way
/// [`ModsCommand::InstallMod`] takes one: the vault catalogue is already
/// mirrored in the frontend store, and a service is not allowed to read state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct ModDownloadTarget {
    pub uid: String,
    pub download_url: String,
}

/// What a HEAD request said about one of those archives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct ModDownloadSize {
    pub uid: String,
    /// Bytes, or `None` when the server answered without a length.
    ///
    /// Not zero: a mod whose size is unknown and a mod that is somehow empty
    /// are different facts, and only one of them is worth printing.
    ///
    /// `u32`, so four gigabytes, which is two orders of magnitude above the
    /// largest mod on the vault. It is also what crosses the IPC boundary
    /// without a `BigInt`, which specta refuses to generate.
    pub bytes: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct ModsState {
    /// The whole catalogue. Kept for the same reason as the map one, and
    /// deliberately not what the Mods tab browses; see `browse`.
    pub vault: Vec<VaultMod>,
    pub vault_status: ModListStatus,
    #[serde(default)]
    pub vault_progress: Option<crate::state::maps::CatalogueProgress>,
    /// One page of a server-side vault search, which is what the Mods tab
    /// shows.
    pub browse: Vec<VaultMod>,
    pub browse_status: ModListStatus,
    pub browse_query: ModVaultQuery,
    pub browse_total_pages: Option<i32>,
    pub browse_total_records: Option<i32>,
    pub installed: Vec<InstalledMod>,
    pub installed_status: ModListStatus,
    pub install_status: ModInstallStatus,
    pub toggle_status: ModToggleStatus,
    /// Archive sizes in bytes, by mod uid, for the ones anybody has asked
    /// about.
    ///
    /// Only ever grows within a session, and only holds answers: a uid that is
    /// absent has not been asked about or came back without a length, and both
    /// mean "do not print a size". Small by construction, because the only
    /// caller asks about the handful of mods one lobby is missing.
    pub download_sizes: BTreeMap<String, u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum ModsEvent {
    VaultLoading,
    VaultProgress {
        progress: crate::state::maps::CatalogueProgress,
    },
    VaultCancelled,
    VaultSearching,
    #[serde(rename_all = "camelCase")]
    VaultSearched {
        mods: Vec<VaultMod>,
        query: ModVaultQuery,
        total_pages: Option<i32>,
        total_records: Option<i32>,
    },
    VaultSearchFailed {
        reason: String,
    },
    VaultLoaded {
        mods: Vec<VaultMod>,
    },
    VaultLoadFailed {
        reason: String,
    },
    InstalledLoading,
    InstalledLoaded {
        mods: Vec<InstalledMod>,
    },
    InstalledLoadFailed {
        reason: String,
    },
    // `rename_all` on the enum only renames variant tags, not the fields of
    // struct-like variants (a serde/specta quirk): so multi-word fields
    // need their own per-variant `rename_all` to stay camelCase on the wire.
    #[serde(rename_all = "camelCase")]
    Installing {
        uid: String,
    },
    /// Install succeeded: carries the freshly-scanned installed list so
    /// the UI doesn't need a separate `LoadInstalled` round-trip (mirrors
    /// `MapsEvent::Installed`).
    Installed {
        installed: Vec<InstalledMod>,
    },
    InstallFailed {
        reason: String,
    },
    Uninstalled {
        installed: Vec<InstalledMod>,
    },
    UninstallFailed {
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    Toggling {
        uid: String,
    },
    Toggled {
        installed: Vec<InstalledMod>,
    },
    ToggleFailed {
        reason: String,
    },
    /// The answers to a [`ModsCommand::QueryDownloadSizes`].
    ///
    /// Every target gets an entry, including the ones the server answered
    /// without a length, so a caller can tell "asked and did not find out" from
    /// "not asked yet". Only the ones that did produce a number reach the
    /// state.
    DownloadSizesResolved {
        sizes: Vec<ModDownloadSize>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum ModsCommand {
    CancelVaultLoad,
    /// Fetch the whole catalogue once.
    LoadVault,
    /// Fetch the catalogue again even though it is loaded, for "Check for
    /// updates". `LoadVault` refuses a second crawl, so the check used to
    /// compare the installed mods against the catalogue from the start of the
    /// session and report "up to date" without having looked.
    ReloadVault,
    /// Fetch one page of a vault search. Submit-driven, as in both reference
    /// clients.
    SearchVault {
        query: ModVaultQuery,
    },
    /// Scan the user's mods folder (mirrors `MapsCommand::LoadInstalled`).
    LoadInstalled,
    /// Download and extract a mod version's zip (mirrors
    /// `MapsCommand::InstallMap`).
    #[serde(rename_all = "camelCase")]
    InstallMod {
        uid: String,
        download_url: String,
    },
    /// Ask how big these archives are, without downloading them.
    ///
    /// Exists for the dialog that asks whether a join may download mods: the
    /// vault's `mod` resource carries a download URL and no file length, so the
    /// only way to answer "how much is this going to cost me" is to ask the
    /// storage server. One HEAD per mod, and the answer is cached in
    /// [`ModsState::download_sizes`] for the session.
    #[serde(rename_all = "camelCase")]
    QueryDownloadSizes {
        targets: Vec<ModDownloadTarget>,
    },
    /// Replace an installed mod with the vault's current version.
    ///
    /// Not a client-side `uninstall` followed by an `install`, which is what
    /// the user had to do by hand: an install refuses a folder that already
    /// exists, so the two have to happen in that order, and doing it from the
    /// UI means a failed download leaves the mod gone. The port downloads
    /// first, removes the old folder second, and puts the mod back into
    /// `game.prefs` if the version it replaced was enabled.
    ///
    /// `folder_name` is the installed copy's folder, which is what has to be
    /// removed; `uid` and `download_url` name the version to put in its place.
    #[serde(rename_all = "camelCase")]
    UpdateMod {
        uid: String,
        folder_name: String,
        download_url: String,
    },
    /// Delete a mod folder (mirrors `MapsCommand::UninstallMap`).
    #[serde(rename_all = "camelCase")]
    UninstallMod {
        folder_name: String,
        uid: String,
    },
    /// Enable or disable an installed mod without uninstalling it (writes
    /// `game.prefs`'s `active_mods` table).
    #[serde(rename_all = "camelCase")]
    ToggleMod {
        uid: String,
        enabled: bool,
    },
    /// Replace the active set with exactly `uids`.
    ///
    /// Deliberately not a loop over [`Self::ToggleMod`]: every toggle rewrites
    /// `game.prefs` *and* rescans the whole mods folder, so applying a preset one
    /// mod at a time costs a rescan per mod and walks the list through every
    /// intermediate state on screen. This is one write and one rescan.
    SetActiveMods {
        uids: Vec<String>,
    },
}

pub fn reduce(state: &mut ModsState, event: &ModsEvent) {
    match event {
        ModsEvent::VaultLoading => {
            state.vault_status = ModListStatus::Loading;
            state.vault_progress = None;
        }
        ModsEvent::VaultProgress { progress } => state.vault_progress = Some(progress.clone()),
        ModsEvent::VaultCancelled => {
            state.vault_status = ModListStatus::Cancelled;
            state.vault_progress = None;
        }
        ModsEvent::VaultLoaded { mods } => {
            state.vault = mods.clone();
            state.vault_status = ModListStatus::Ready;
        }
        ModsEvent::VaultSearching => state.browse_status = ModListStatus::Loading,
        ModsEvent::VaultSearched {
            mods,
            query,
            total_pages,
            total_records,
        } => {
            state.browse = mods.clone();
            state.browse_query = query.clone();
            state.browse_total_pages = *total_pages;
            state.browse_total_records = *total_records;
            state.browse_status = ModListStatus::Ready;
        }
        ModsEvent::VaultSearchFailed { reason } => {
            state.browse_status = ModListStatus::Failed {
                reason: reason.clone(),
            };
        }
        ModsEvent::VaultLoadFailed { reason } => {
            state.vault_status = ModListStatus::Failed {
                reason: reason.clone(),
            }
        }
        ModsEvent::InstalledLoading => state.installed_status = ModListStatus::Loading,
        ModsEvent::InstalledLoaded { mods } => {
            state.installed = mods.clone();
            state.installed_status = ModListStatus::Ready;
        }
        ModsEvent::InstalledLoadFailed { reason } => {
            state.installed_status = ModListStatus::Failed {
                reason: reason.clone(),
            }
        }
        ModsEvent::Installing { uid } => {
            state.install_status = ModInstallStatus::Installing { uid: uid.clone() }
        }
        ModsEvent::Installed { installed } => {
            state.install_status = ModInstallStatus::Idle;
            state.installed = installed.clone();
            state.installed_status = ModListStatus::Ready;
        }
        ModsEvent::InstallFailed { reason } => {
            state.install_status = ModInstallStatus::Failed {
                reason: reason.clone(),
            }
        }
        ModsEvent::Uninstalled { installed } => {
            state.install_status = ModInstallStatus::Idle;
            state.installed = installed.clone();
            state.installed_status = ModListStatus::Ready;
        }
        ModsEvent::UninstallFailed { reason } => {
            state.install_status = ModInstallStatus::Failed {
                reason: reason.clone(),
            }
        }
        ModsEvent::Toggling { uid } => {
            state.toggle_status = ModToggleStatus::Toggling { uid: uid.clone() }
        }
        ModsEvent::Toggled { installed } => {
            state.toggle_status = ModToggleStatus::Idle;
            state.installed = installed.clone();
            state.installed_status = ModListStatus::Ready;
        }
        ModsEvent::ToggleFailed { reason } => {
            state.toggle_status = ModToggleStatus::Failed {
                reason: reason.clone(),
            }
        }
        ModsEvent::DownloadSizesResolved { sizes } => {
            for size in sizes {
                if let Some(bytes) = size.bytes {
                    state.download_sizes.insert(size.uid.clone(), bytes);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vault_mod(uid: &str) -> VaultMod {
        VaultMod {
            mod_id: 1,
            version_id: 1,
            display_name: "Total Mayhem".into(),
            author: "Some Author".into(),
            uploader: "Uploader".into(),
            uploader_id: Some(4711),
            uid: uid.into(),
            version: "12".into(),
            description: "Adds new units and experimentals.".into(),
            filename: "total_mayhem.zip".into(),
            mod_type: ModType::Sim,
            ranked: false,
            recommended: false,
            rating_tenths: 44,
            reviews: 21,
            created_at: "2025-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
            download_url: "https://content.faforever.com/mods/total_mayhem.zip".into(),
            thumbnail_url: "https://content.faforever.com/mods/total_mayhem.png".into(),
        }
    }

    fn installed_mod(uid: &str, enabled: bool) -> InstalledMod {
        InstalledMod {
            folder_name: "total_mayhem".into(),
            uid: uid.into(),
            display_name: "Total Mayhem".into(),
            version: "12".into(),
            author: "Some Author".into(),
            description: "Adds new units and experimentals.".into(),
            mod_type: ModType::Sim,
            enabled,
        }
    }

    #[test]
    fn vault_loading_then_loaded() {
        let mut s = ModsState::default();
        assert_eq!(s.vault_status, ModListStatus::Idle);
        reduce(&mut s, &ModsEvent::VaultLoading);
        assert_eq!(s.vault_status, ModListStatus::Loading);
        reduce(
            &mut s,
            &ModsEvent::VaultLoaded {
                mods: vec![vault_mod("abc-123")],
            },
        );
        assert_eq!(s.vault_status, ModListStatus::Ready);
        assert_eq!(s.vault.len(), 1);
    }

    #[test]
    fn vault_load_failure_records_reason() {
        let mut s = ModsState::default();
        reduce(
            &mut s,
            &ModsEvent::VaultLoadFailed {
                reason: "offline".into(),
            },
        );
        assert_eq!(
            s.vault_status,
            ModListStatus::Failed {
                reason: "offline".into()
            }
        );
    }

    #[test]
    fn installed_loading_then_loaded() {
        let mut s = ModsState::default();
        reduce(&mut s, &ModsEvent::InstalledLoading);
        assert_eq!(s.installed_status, ModListStatus::Loading);
        reduce(
            &mut s,
            &ModsEvent::InstalledLoaded {
                mods: vec![installed_mod("abc-123", false)],
            },
        );
        assert_eq!(s.installed_status, ModListStatus::Ready);
        assert_eq!(s.installed.len(), 1);
    }

    #[test]
    fn install_flow_updates_installed_list_and_resets_status() {
        let mut s = ModsState::default();
        reduce(
            &mut s,
            &ModsEvent::Installing {
                uid: "abc-123".into(),
            },
        );
        assert_eq!(
            s.install_status,
            ModInstallStatus::Installing {
                uid: "abc-123".into()
            }
        );
        reduce(
            &mut s,
            &ModsEvent::Installed {
                installed: vec![installed_mod("abc-123", false)],
            },
        );
        assert_eq!(s.install_status, ModInstallStatus::Idle);
        assert_eq!(s.installed.len(), 1);
        assert_eq!(s.installed_status, ModListStatus::Ready);
    }

    #[test]
    fn install_failure_records_reason() {
        let mut s = ModsState::default();
        reduce(
            &mut s,
            &ModsEvent::InstallFailed {
                reason: "download failed".into(),
            },
        );
        assert_eq!(
            s.install_status,
            ModInstallStatus::Failed {
                reason: "download failed".into()
            }
        );
    }

    #[test]
    fn uninstall_updates_installed_list() {
        let mut s = ModsState {
            installed: vec![installed_mod("abc-123", false)],
            installed_status: ModListStatus::Ready,
            ..Default::default()
        };
        reduce(&mut s, &ModsEvent::Uninstalled { installed: vec![] });
        assert_eq!(s.install_status, ModInstallStatus::Idle);
        assert!(s.installed.is_empty());
    }

    #[test]
    fn toggle_flow_updates_installed_list_and_resets_status() {
        let mut s = ModsState::default();
        reduce(
            &mut s,
            &ModsEvent::Toggling {
                uid: "abc-123".into(),
            },
        );
        assert_eq!(
            s.toggle_status,
            ModToggleStatus::Toggling {
                uid: "abc-123".into()
            }
        );
        reduce(
            &mut s,
            &ModsEvent::Toggled {
                installed: vec![installed_mod("abc-123", true)],
            },
        );
        assert_eq!(s.toggle_status, ModToggleStatus::Idle);
        assert!(s.installed[0].enabled);
    }

    #[test]
    fn toggle_failure_records_reason() {
        let mut s = ModsState::default();
        reduce(
            &mut s,
            &ModsEvent::ToggleFailed {
                reason: "could not write game.prefs".into(),
            },
        );
        assert_eq!(
            s.toggle_status,
            ModToggleStatus::Failed {
                reason: "could not write game.prefs".into()
            }
        );
    }
}
