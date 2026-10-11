//! Real mods client: vault browsing, local install management, and
//! enabling/disabling installed mods.
//!
//! Mirrors the Python client's `vaults/modvault/` + `fa/mods.py` (primary
//! source; cross-checked against the Java client's `ModService.java`):
//!
//! ## Vault listing
//! `GET {api_base}/data/mod`, including the latest version, uploader, and
//! review summary,
//! `filter=latestVersion.hidden=='false'`, sorted newest-first: same
//! JSON:API shape as the map vault (see `infra/maps.rs`).
//!
//! ## Installed mods
//! A folder scan of the user's mods folder: `<Documents>/My Games/Gas
//! Powered Games/Supreme Commander Forged Alliance/mods` (mirrors
//! `util.VAULTS_BASE_DIR` + a `mods` subfolder, same base as `maps_dir()`).
//! Each subfolder's `mod_info.lua` is parsed for `name`/`uid`/`version`/
//! `author`/`description`/`ui_only`: a small line-based key/value extractor,
//! **not** a full Lua parser (the reference clients' own `luaparser.py` handles
//! arbitrary nested tables; these seven fields are always flat `key = value`
//! assignments in practice, confirmed against
//! `context/python_client/src/vaults/modvault/utils.py::getModInfo`). Folders
//! without a valid `mod_info.lua` are skipped, not an error (mirrors
//! Python's `getInstalledMods` try/except-continue).
//!
//! ## Enable/disable
//! Unlike maps, mods can be toggled without uninstalling. Both reference
//! clients do this by reading/rewriting FA's own `game.prefs` file's
//! `active_mods = { ['uid'] = true, ... }` table: confirmed path via
//! Python's `util.LOCALFOLDER`/`PREFSFILENAME`
//! (`%LOCALAPPDATA%\Gas Powered Games\Supreme Commander Forged
//! Alliance\game.prefs`). Only *enabled* uids are ever written into the
//! table (mirrors `vaults/modvault/utils.py::setActiveMods` writing only
//! `['uid'] = true` entries and omitting disabled mods entirely): a plain
//! balanced-brace/string scan rather than a regex dependency, since the
//! shape being parsed is this simple, fixed pattern, not arbitrary Lua.
//!
//! ## Install / uninstall
//! Installing downloads the version's zip (unauthenticated CDN, like maps)
//! and extracts it directly into the mods folder: its own top-level zip
//! entry is the mod's folder name, same as maps. Uninstalling removes that
//! directory and also scrubs the mod's uid from `game.prefs`'s active set
//! if present (an uninstalled mod can't stay "enabled").

use std::collections::{BTreeMap, HashMap};
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use faf_domain::protocol::vault_query::ModVaultQuery;
use faf_domain::state::{
    InstalledMod, ModDownloadSize, ModDownloadTarget, ModType, ModVersionConflict, VaultMod,
};
use serde_json::Value;

use crate::infra::env_or;
use crate::infra::jsonapi::{
    fetch_all_pages_with_progress, fetch_document, find_rel_resource, meta_page_i32, rel_target,
    resource_index, total_pages, value_bool, value_f64, value_i32, JsonApiDoc, JsonApiResource,
};
use crate::infra::review_totals::{self, ReviewTotal, Subject};
use crate::infra::vault_install::{
    archive_root_name, bounded_body, install_archive, replace_archive, validate_url,
    MAX_DOWNLOAD_BYTES,
};
use crate::ports::{ModPrepFailure, ModSearchPage, ModsPort};

/// Mods per vault page fetched in [`ModsClient::list_vault`]: mirrors
/// `infra::maps`'s identical pagination constants.
const VAULT_PAGE_SIZE: usize = 100;
const MAX_VAULT_PAGES: u32 = 200;

#[derive(Debug, Clone)]
pub struct ModsConfig {
    /// FAF Data API base, which serves `/data/mod`: same host as the map
    /// and replay vaults.
    pub api_base: String,
    /// Trusted origin for mod archives returned by the Data API.
    pub content_base: String,
}

impl ModsConfig {
    pub fn faf() -> Self {
        Self {
            api_base: env_or("FAF_API_BASE", "https://api.faforever.com"),
            content_base: env_or("FAF_CONTENT_BASE", "https://content.faforever.com"),
        }
    }
}

pub struct ModsClient {
    config: ModsConfig,
    tokens: crate::infra::session::TokenStore,
    http: reqwest::Client,
}

impl ModsClient {
    pub fn new(config: ModsConfig, tokens: crate::infra::session::TokenStore) -> Self {
        Self {
            config,
            tokens,
            http: super::http::shared_http_client(),
        }
    }

    pub fn faf(tokens: crate::infra::session::TokenStore) -> Self {
        Self::new(ModsConfig::faf(), tokens)
    }

    /// Fetch a mod version's zip, with the vault's origin and size envelope.
    async fn download_mod_archive(&self, uid: &str, download_url: &str) -> Result<Vec<u8>, String> {
        validate_url(download_url, &self.config.content_base, "mods")?;
        let resp = self
            .http
            .get(download_url)
            .send()
            .await
            .map_err(|e| format!("could not download mod {uid}: {e}"))?;
        validate_url(resp.url().as_str(), &self.config.content_base, "mods")?;
        let status = resp.status();
        if !status.is_success() {
            return Err(format!("could not download mod {uid}: {status}"));
        }
        bounded_body(resp, &format!("mod {uid}"), MAX_DOWNLOAD_BYTES).await
    }

    /// Extract a fetched archive into the mods folder, refusing one whose
    /// `mod_info.lua` does not carry the uid that was asked for.
    async fn extract_mod_archive(&self, uid: &str, bytes: Vec<u8>) -> Result<(), String> {
        let dest = mods_dir();
        tokio::fs::create_dir_all(&dest)
            .await
            .map_err(|e| format!("could not create mods folder: {e}"))?;

        let expected_uid = uid.to_owned();
        tokio::task::spawn_blocking(move || {
            install_archive(&bytes, &dest, None, |staged_root| {
                let info_path = staged_root.join("mod_info.lua");
                let contents = std::fs::read_to_string(&info_path)
                    .map_err(|error| format!("could not read {}: {error}", info_path.display()))?;
                let info = parse_mod_info(&contents)
                    .ok_or_else(|| "downloaded mod has no valid mod_info.lua".to_string())?;
                if info.uid != expected_uid {
                    return Err(format!(
                        "downloaded mod uid {:?} does not match expected uid {:?}",
                        info.uid, expected_uid
                    ));
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("extraction task panicked: {e}"))??;
        Ok(())
    }

    /// Where to download the version a game names, and which version it is.
    ///
    /// The version rides along because the same record already carries it, and
    /// a replacement prompt that names only the installed version leaves the
    /// player guessing what the host is on (#330).
    async fn required_mod_version(&self, uid: &str) -> Result<RequiredModVersion, String> {
        if uid.is_empty()
            || !uid
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err("the game supplied an invalid simulation-mod uid".into());
        }
        let token = self
            .tokens
            .get()
            .ok_or_else(|| "not logged in".to_string())?;
        let mut url = url::Url::parse(&format!("{}/data/modVersion", self.config.api_base))
            .map_err(|error| format!("invalid API base: {error}"))?;
        url.query_pairs_mut()
            .append_pair("filter", &format!(r#"uid=="{uid}""#))
            .append_pair("page[size]", "1");
        let document = fetch_document(&self.http, url, &token).await?;
        document
            .data
            .into_iter()
            .next()
            .and_then(|resource| {
                let download_url = resource
                    .attributes
                    .get("downloadUrl")
                    .and_then(Value::as_str)?
                    .to_owned();
                let version = match resource.attributes.get("version") {
                    Some(Value::String(text)) => text.clone(),
                    Some(Value::Number(number)) => number.to_string(),
                    _ => String::new(),
                };
                Some(RequiredModVersion {
                    download_url,
                    version,
                })
            })
            .ok_or_else(|| format!("simulation mod {uid} was not found in the vault"))
    }
}

/// One vault `modVersion` record, reduced to what joining a game needs.
struct RequiredModVersion {
    download_url: String,
    /// Empty when the vault did not say, which the prompt draws as "?".
    version: String,
}

/// How big the file behind a URL is, asked two ways.
///
/// A HEAD first, because it is the cheap question. That was the whole
/// implementation and it reported every mod as `0 B`: the vault's content
/// storage answers HEAD with `200` and `Content-Length: 0`, which is what a
/// server that does not really implement HEAD does, and zero is a length as
/// far as `reqwest` is concerned.
///
/// So a zero is treated as no answer, and the fallback is a one-byte ranged
/// GET: `Range: bytes=0-0` comes back `206` with
/// `Content-Range: bytes 0-0/12345`, where the number after the slash is the
/// size of the whole file. One byte of body for an exact answer.
///
/// `None` covers every remaining way this can fail, and they are all the same
/// to the caller: a URL that is not https (the client fetches nothing else), a
/// server that refuses both requests, a redirect chain that drops the headers,
/// a response with no length in either form, or a timeout. No token: the mod
/// archives live on content storage rather than behind the API.
async fn head_content_length(http: &reqwest::Client, url: &str) -> Option<u32> {
    if !url.starts_with("https://") {
        return None;
    }

    let head = http
        .head(url)
        .timeout(std::time::Duration::from_secs(6))
        .send()
        .await
        .ok()
        .filter(|response| response.status().is_success())
        .and_then(|response| response.content_length())
        .filter(|length| *length > 0);
    if let Some(length) = head {
        return u32::try_from(length).ok();
    }

    let ranged = http
        .get(url)
        .header(reqwest::header::RANGE, "bytes=0-0")
        .timeout(std::time::Duration::from_secs(6))
        .send()
        .await
        .ok()?;
    if !ranged.status().is_success() {
        return None;
    }
    let total = content_range_total(
        ranged
            .headers()
            .get(reqwest::header::CONTENT_RANGE)?
            .to_str()
            .ok()?,
    )?;
    u32::try_from(total).ok()
}

/// The total out of a `Content-Range: bytes 0-0/12345`.
///
/// `None` for the two forms that carry no total: a header shaped differently
/// from the one in RFC 9110, and the `*/` a server sends when it will not say
/// how long the whole thing is.
fn content_range_total(header: &str) -> Option<u64> {
    header.rsplit_once('/')?.1.trim().parse().ok()
}

#[async_trait]
impl ModsPort for ModsClient {
    async fn list_vault(&self) -> Result<Vec<VaultMod>, String> {
        self.list_vault_with_progress(None).await
    }

    async fn list_vault_with_progress(
        &self,
        progress: Option<tokio::sync::mpsc::Sender<faf_domain::state::maps::CatalogueProgress>>,
    ) -> Result<Vec<VaultMod>, String> {
        let token = self
            .tokens
            .get()
            .ok_or_else(|| "not logged in".to_string())?;

        // Same "fetch every page up front" reasoning as `MapsClient::list_vault`,
        // minus its lookup-index duty: a mod search is useless if most of the
        // vault is missing and there is no paging UI yet.
        let api_base = self.config.api_base.clone();
        let docs = fetch_all_pages_with_progress(
            &self.http,
            &token,
            MAX_VAULT_PAGES,
            VAULT_PAGE_SIZE,
            |pages, total_pages| {
                if let Some(progress) = &progress {
                    let _ = progress.try_send(faf_domain::state::maps::CatalogueProgress {
                        pages,
                        total_pages,
                    });
                }
            },
            |page| {
                let mut url = url::Url::parse(&format!("{api_base}/data/mod"))
                    .map_err(|e| format!("invalid API base: {e}"))?;
                url.query_pairs_mut()
                    .append_pair("filter", "latestVersion.hidden=='false'")
                    .append_pair("sort", "-latestVersion.createTime")
                    .append_pair("page[size]", &VAULT_PAGE_SIZE.to_string())
                    .append_pair("page[number]", &page.to_string())
                    .append_pair("include", MOD_VAULT_INCLUDE);
                Ok(url)
            },
        )
        .await?;

        let mut all_mods = Vec::new();
        for doc in &docs {
            all_mods.extend(parse_vault_mods(doc));
        }
        // The installed view filters and sorts on these ratings, so they are
        // added up here as well, for the whole vault in one pass.
        match review_totals::fetch_whole_vault(&self.http, &token, &api_base, Subject::Mod).await {
            Ok(totals) => raise_ratings(&mut all_mods, &totals),
            Err(error) => {
                tracing::warn!(%error, "could not add up the mods' ratings over their versions")
            }
        }
        Ok(all_mods)
    }

    async fn download_sizes(&self, targets: Vec<ModDownloadTarget>) -> Vec<ModDownloadSize> {
        let mut sizes = Vec::with_capacity(targets.len());
        for target in targets {
            sizes.push(ModDownloadSize {
                bytes: head_content_length(&self.http, &target.download_url).await,
                uid: target.uid,
            });
        }
        sizes
    }

    async fn search_vault(&self, query: ModVaultQuery) -> Result<ModSearchPage, String> {
        let token = self
            .tokens
            .get()
            .ok_or_else(|| "not logged in".to_string())?;

        let mut url = url::Url::parse(&format!("{}/data/mod", self.config.api_base))
            .map_err(|e| format!("invalid API base: {e}"))?;
        {
            let mut pairs = url.query_pairs_mut();
            if let Some(filter) = query.build_filter() {
                pairs.append_pair("filter", &filter);
            }
            pairs
                .append_pair("sort", &query.sort_param())
                .append_pair("page[size]", &query.page_size.to_string())
                .append_pair("page[number]", &query.page.max(1).to_string())
                .append_key_only("page[totals]")
                .append_pair("include", MOD_VAULT_INCLUDE);
        }

        let doc = fetch_document(&self.http, url, &token).await?;
        let mut mods = parse_vault_mods(&doc);
        let ids: Vec<i32> = mods.iter().map(|vault_mod| vault_mod.mod_id).collect();
        match review_totals::fetch(
            &self.http,
            &token,
            &self.config.api_base,
            Subject::Mod,
            &ids,
        )
        .await
        {
            Ok(totals) => raise_ratings(&mut mods, &totals),
            Err(error) => {
                tracing::warn!(%error, "could not add up the mods' ratings over their versions")
            }
        }
        Ok(ModSearchPage {
            mods,
            total_pages: total_pages(&doc.meta, query.page_size),
            total_records: meta_page_i32(&doc.meta, "totalRecords"),
        })
    }

    async fn list_installed(&self) -> Result<Vec<InstalledMod>, String> {
        list_installed_dir(&mods_dir()).await
    }

    async fn install_mod(
        &self,
        uid: String,
        download_url: String,
    ) -> Result<Vec<InstalledMod>, String> {
        let bytes = self.download_mod_archive(&uid, &download_url).await?;
        self.extract_mod_archive(&uid, bytes).await?;
        list_installed_dir(&mods_dir()).await
    }

    async fn update_mod(
        &self,
        uid: String,
        folder_name: String,
        download_url: String,
    ) -> Result<Vec<InstalledMod>, String> {
        // Fetched before anything is deleted. An update that fails on a flaky
        // connection has to leave the installed copy alone: the whole reason
        // this exists is that doing it by hand meant uninstalling first and
        // discovering the download problem with nothing left on disk.
        let bytes = self.download_mod_archive(&uid, &download_url).await?;

        // Whether the version being replaced was switched on, read before the
        // uninstall scrubs its uid out of `game.prefs`. A new version is a new
        // uid, so the flag cannot simply be left in place.
        let previous = list_installed_dir(&mods_dir())
            .await?
            .into_iter()
            .find(|installed| installed.folder_name.eq_ignore_ascii_case(&folder_name));
        let destination = mods_dir();
        let old = safe_mod_target(&destination, &folder_name)?;
        let expected_uid = uid.clone();
        tokio::task::spawn_blocking(move || {
            replace_archive(&bytes, &destination, &old, |staged| {
                validate_mod_uid(staged, &expected_uid)
            })
        })
        .await
        .map_err(|error| format!("replacement task failed: {error}"))??;

        if let Some(previous) = previous {
            let mut active = read_active_mod_uids().await;
            active.retain(|candidate| candidate != &previous.uid);
            if previous.enabled && !active.contains(&uid) {
                active.push(uid.clone());
            }
            write_active_mod_uids_to_disk(&active).await?;
        }
        list_installed_dir(&mods_dir()).await
    }

    async fn uninstall_mod(&self, folder_name: String) -> Result<Vec<InstalledMod>, String> {
        let dir = mods_dir();
        let target = safe_mod_target(&dir, &folder_name)?;

        // Read the uid before deleting so we can also scrub it from
        // game.prefs: an uninstalled mod can't stay "enabled".
        let uid = tokio::fs::read_to_string(target.join("mod_info.lua"))
            .await
            .ok()
            .and_then(|contents| parse_mod_info(&contents))
            .map(|info| info.uid);

        if target.exists() {
            tokio::fs::remove_dir_all(&target)
                .await
                .map_err(|e| format!("could not remove {}: {e}", target.display()))?;
        }

        if let Some(uid) = uid {
            let mut uids = read_active_mod_uids().await;
            uids.retain(|u| u != &uid);
            write_active_mod_uids_to_disk(&uids).await?;
        }

        list_installed_dir(&dir).await
    }

    async fn toggle_mod(&self, uid: String, enabled: bool) -> Result<Vec<InstalledMod>, String> {
        let mut uids = read_active_mod_uids().await;
        uids.retain(|u| u != &uid);
        if enabled {
            uids.push(uid);
        }
        write_active_mod_uids_to_disk(&uids).await?;
        list_installed_dir(&mods_dir()).await
    }

    async fn set_active_mods(&self, uids: Vec<String>) -> Result<Vec<InstalledMod>, String> {
        // Written verbatim rather than filtered against the installed list: the
        // caller decides what is active, and a uid whose folder is gone is inert
        // to the game anyway.
        write_active_mod_uids_to_disk(&uids).await?;
        list_installed_dir(&mods_dir()).await
    }

    async fn ensure_game_mods(
        &self,
        mods: &BTreeMap<String, String>,
        replace_conflicts: bool,
    ) -> Result<(), ModPrepFailure> {
        if mods.is_empty() {
            return Ok(());
        }
        let installed = self
            .list_installed()
            .await
            .map_err(ModPrepFailure::Failed)?;
        let dest = mods_dir();

        // A mod uid names one *version*, so a folder already holding a
        // different uid is a collision only the user can settle: the download
        // is discarded and the conflict collected, rather than failing at
        // extraction time with "<folder> is already installed", which is all
        // this used to say. The archive's own top-level folder is what decides
        // it, so the check is exact rather than a guess from the mod's name.
        //
        // One archive is held at a time: a game can want several large mods,
        // and reading them all into memory to decide afterwards would be a
        // gigabyte for no benefit. Installing the mods that *do not* collide
        // before asking is harmless, since they are the versions this game
        // needs and nothing of the user's is overwritten to get them; only the
        // destructive step waits for an answer.
        let mut conflicts: Vec<ModVersionConflict> = Vec::new();
        for (uid, name) in mods {
            if installed.iter().any(|candidate| candidate.uid == *uid) {
                continue;
            }
            let required = self
                .required_mod_version(uid)
                .await
                .map_err(ModPrepFailure::Failed)?;
            let bytes = self
                .download_mod_archive(uid, &required.download_url)
                .await
                .map_err(ModPrepFailure::Failed)?;
            let root = archive_root_name(&bytes).map_err(ModPrepFailure::Failed)?;

            let target = safe_mod_target(&dest, &root).map_err(ModPrepFailure::Failed)?;
            if target.exists() {
                // Matched case-insensitively against the scan: Windows will
                // happily hand back a differently cased spelling of the same
                // directory than the one the archive names.
                let occupant = installed
                    .iter()
                    .find(|candidate| candidate.folder_name.eq_ignore_ascii_case(&root));
                if !replace_conflicts {
                    conflicts.push(ModVersionConflict {
                        required_uid: uid.clone(),
                        required_name: name.clone(),
                        required_version: required.version.clone(),
                        folder_name: root.clone(),
                        installed_uid: occupant.map(|m| m.uid.clone()).unwrap_or_default(),
                        // A folder with no readable `mod_info.lua` is not in
                        // the scan at all, and the prompt still has to name
                        // something the user can recognise.
                        installed_name: occupant
                            .map(|m| m.display_name.clone())
                            .unwrap_or_else(|| root.clone()),
                        installed_version: occupant.map(|m| m.version.clone()).unwrap_or_default(),
                    });
                    continue;
                }
                let destination = dest.clone();
                let expected_uid = uid.clone();
                tokio::task::spawn_blocking(move || {
                    replace_archive(&bytes, &destination, &target, |staged| {
                        validate_mod_uid(staged, &expected_uid)
                    })
                })
                .await
                .map_err(|error| {
                    ModPrepFailure::Failed(format!("replacement task failed: {error}"))
                })?
                .map_err(ModPrepFailure::Failed)?;
                if let Some(occupant) = occupant {
                    let mut active = read_active_mod_uids().await;
                    active.retain(|candidate| candidate != &occupant.uid);
                    if !active.contains(uid) {
                        active.push(uid.clone());
                    }
                    write_active_mod_uids_to_disk(&active)
                        .await
                        .map_err(ModPrepFailure::Failed)?;
                }
                continue;
            }
            self.extract_mod_archive(uid, bytes)
                .await
                .map_err(ModPrepFailure::Failed)?;
        }

        if !conflicts.is_empty() {
            return Err(ModPrepFailure::Conflicts(conflicts));
        }

        let mut active = read_active_mod_uids().await;
        for uid in mods.keys() {
            if !active.contains(uid) {
                active.push(uid.clone());
            }
        }
        write_active_mod_uids_to_disk(&active)
            .await
            .map_err(ModPrepFailure::Failed)
    }
}

fn validate_mod_uid(staged: &Path, expected: &str) -> Result<(), String> {
    let contents = std::fs::read_to_string(staged.join("mod_info.lua"))
        .map_err(|error| format!("could not read staged mod metadata: {error}"))?;
    let info = parse_mod_info(&contents)
        .ok_or_else(|| "downloaded mod has no valid mod_info.lua".to_string())?;
    if info.uid != expected {
        return Err("downloaded mod uid does not match the requested version".into());
    }
    Ok(())
}

pub(crate) fn safe_mod_target(root: &Path, folder_name: &str) -> Result<PathBuf, String> {
    let components = Path::new(folder_name).components().collect::<Vec<_>>();
    if components.is_empty()
        || components.len() > 2
        || components
            .iter()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err("refusing to use a path outside the mods folder".to_string());
    }
    Ok(root.join(folder_name))
}

/// Scans `dir` for installed mod folders, parsing each one's
/// `mod_info.lua` and cross-referencing `game.prefs` for `enabled`. The
/// testable body of [`ModsClient::list_installed`]/post-change rescans.
pub(crate) async fn list_installed_dir(dir: &Path) -> Result<Vec<InstalledMod>, String> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("could not read {}: {e}", dir.display())),
    };

    let active_uids = read_active_mod_uids().await;

    let mut mod_dirs = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| format!("could not list {}: {e}", dir.display()))?
    {
        let path = entry.path();
        if crate::infra::vault_install::is_install_staging_name(
            &entry.file_name().to_string_lossy(),
        ) {
            continue;
        }
        if !is_directory(&path).await {
            continue;
        }
        if path.join("mod_info.lua").is_file() {
            mod_dirs.push(path.clone());
            // Deliberately no `continue`. A folder that is a mod can still
            // contain one: the reported case is a simulation mod that ships
            // its own UI-only variant inside itself, so that one download
            // covers both rather than the author publishing the same mod
            // twice with a flag flipped. The game finds both, and so does the
            // Java client, whose `Files.walk(modsDirectory, 2)` collects every
            // `mod_info.lua` within two levels rather than stopping at the
            // first. This stopped at the first, and the inner mod was
            // invisible.
        }

        // Match the Java client's `Files.walk(modsDirectory, 2)`: archives
        // occasionally contain one extra wrapper directory, and a mod may
        // carry a second mod inside it.
        let Ok(mut children) = tokio::fs::read_dir(&path).await else {
            continue;
        };
        while let Ok(Some(child)) = children.next_entry().await {
            let child_path = child.path();
            if is_directory(&child_path).await && child_path.join("mod_info.lua").is_file() {
                mod_dirs.push(child_path);
            }
        }
    }

    let mut installed = Vec::new();
    for path in mod_dirs {
        let Ok(relative) = path.strip_prefix(dir) else {
            continue;
        };
        let folder_name = relative.to_string_lossy().replace('\\', "/");
        let Ok(contents) = tokio::fs::read_to_string(path.join("mod_info.lua")).await else {
            continue;
        };
        let Some(info) = parse_mod_info(&contents) else {
            continue;
        };
        let enabled = active_uids.contains(&info.uid);
        installed.push(InstalledMod {
            folder_name,
            uid: info.uid,
            display_name: info.name,
            version: info.version,
            author: info.author,
            description: info.description,
            mod_type: if info.ui_only {
                ModType::Ui
            } else {
                ModType::Sim
            },
            enabled,
        });
    }
    installed.sort_by(|a, b| a.display_name.cmp(&b.display_name));
    Ok(installed)
}

/// Whether a path is a directory, following a link if it is one.
///
/// `DirEntry::file_type` reports the entry itself and never follows: a
/// directory symlink answers `is_symlink`, and `is_dir` is false. On Windows a
/// junction answers the same way. So a mod folder that is a link into a
/// working tree somewhere else was skipped outright, which is the report: mod
/// authors keep the sources elsewhere and link them in, and the client found
/// nothing.
///
/// `metadata` follows, which is what the game itself does with these folders.
/// A link pointing at nothing, or at a file, answers false rather than
/// failing: it is not a mod, and a broken link in the mods folder is not
/// something to refuse the whole scan over.
///
/// Following cannot recurse away: the scan is two levels deep by construction,
/// so a link that points at its own parent costs one extra `read_dir` and
/// nothing more.
async fn is_directory(path: &Path) -> bool {
    tokio::fs::metadata(path)
        .await
        .map(|data| data.is_dir())
        .unwrap_or(false)
}

/// The subset of `mod_info.lua` fields this client needs (mirrors the
/// Python client's `getModInfo`'s search dict: see the module docs).
struct ModInfoFields {
    name: String,
    uid: String,
    version: String,
    author: String,
    description: String,
    ui_only: bool,
}

/// Parses a `mod_info.lua` file's flat `key = value` assignments. Not a
/// full Lua parser: see the module docs for why that's fine for these six
/// scalar fields. Returns `None` if `uid` is missing (mirrors Python
/// logging a warning and skipping the mod).
/// The `name` a `mod_info.lua` declares, which is what the vault files the mod
/// under. See [`crate::ports::UploadsPort::subject_name`].
pub(crate) fn mod_info_name(contents: &str) -> Option<String> {
    parse_mod_info(contents).map(|fields| fields.name)
}

/// The level of a Lua long-string opener at the start of `value` (`[[` is 0,
/// `[==[` is 2), and the text after it, or `None` when `value` is not one.
fn long_string_opener(value: &str) -> Option<(usize, &str)> {
    let rest = value.strip_prefix('[')?;
    let level = rest.chars().take_while(|c| *c == '=').count();
    let rest = rest[level..].strip_prefix('[')?;
    Some((level, rest))
}

/// A quoted value's contents, or an unquoted value with its trailing
/// `-- comment` and `,` removed. Quotes first, so a `--` inside a string
/// (a URL, "Tech 1 -- Tech 3") is part of the value rather than a comment.
fn scalar_value(value: &str) -> String {
    let value = value.trim();
    if let Some(quote) = value.chars().next().filter(|c| *c == '"' || *c == '\'') {
        let inner = &value[1..];
        return match inner.find(quote) {
            Some(end) => inner[..end].to_string(),
            None => inner.to_string(),
        };
    }
    let value = match value.find("--") {
        Some(idx) => &value[..idx],
        None => value,
    };
    let value = value.trim();
    value.strip_suffix(',').unwrap_or(value).trim().to_string()
}

fn parse_mod_info(contents: &str) -> Option<ModInfoFields> {
    let mut fields: HashMap<String, String> = HashMap::new();
    let mut lines = contents.lines();
    while let Some(raw_line) = lines.next() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with("--") {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim().to_lowercase();
        let value = value.trim();

        // A Lua long string, which is how most mods write a description
        // longer than a line: `description = [[` and the text on the lines
        // that follow, up to `]]`. Read line by line, only the `[[` survived,
        // so the details panel printed "[[" for the description.
        if let Some((level, rest)) = long_string_opener(value) {
            let closer = format!("]{}]", "=".repeat(level));
            let mut text = String::new();
            let mut remaining = rest.to_string();
            loop {
                if let Some(end) = remaining.find(&closer) {
                    text.push_str(&remaining[..end]);
                    break;
                }
                text.push_str(&remaining);
                match lines.next() {
                    Some(next) => {
                        text.push('\n');
                        remaining = next.to_string();
                    }
                    // Unterminated: keep what there is rather than nothing.
                    None => break,
                }
            }
            fields.insert(key, text.trim().to_string());
            continue;
        }

        fields.insert(key, scalar_value(value));
    }

    let uid = fields.get("uid")?.clone();
    let name = fields.get("name").cloned().unwrap_or_else(|| uid.clone());
    // Matches Python's `getModInfo` defaults exactly.
    let version = fields
        .get("version")
        .cloned()
        .unwrap_or_else(|| "1".to_string());
    let author = fields.get("author").cloned().unwrap_or_default();
    let description = fields.get("description").cloned().unwrap_or_default();
    let ui_only = fields.get("ui_only").is_some_and(|v| v == "true");

    Some(ModInfoFields {
        name,
        uid,
        version,
        author,
        description,
        ui_only,
    })
}

/// The user's mods folder: `<Documents>/My Games/Gas Powered Games/Supreme
/// Commander Forged Alliance/mods` (mirrors `infra::maps::maps_dir`'s
/// identical base, `mods` instead of `maps`). `FAF_MODS_DIR` overrides it.
pub(crate) fn mods_dir() -> PathBuf {
    if let Some(dir) = crate::infra::paths::mods_dir() {
        return dir;
    }
    if let Ok(dir) = std::env::var("FAF_MODS_DIR") {
        if !dir.is_empty() {
            return PathBuf::from(dir);
        }
    }
    crate::infra::faf_content::vault_dir().join("mods")
}

/// The folders the prefs file sits in, below whatever `%LOCALAPPDATA%` is.
const GAME_PREFS_DIR: [&str; 2] = ["Gas Powered Games", "Supreme Commander Forged Alliance"];

/// The prefs file's name as the game writes it.
///
/// Capital G. Windows does not care, so `game.prefs` found it there, and a
/// Linux filesystem does: under Wine the lower-case name matched nothing, the
/// search for the prefix user who has played found nobody, and every mod
/// toggle wrote a file the game never reads (#283).
const GAME_PREFS_FILE: &str = "Game.prefs";

/// The prefs file under `local`: the one on disk in whatever letter case it
/// has, or where the game would write it when there is none yet.
fn game_prefs_in(local: &Path) -> PathBuf {
    let dir = GAME_PREFS_DIR
        .iter()
        .fold(local.to_path_buf(), |path, part| path.join(part));
    let existing = std::fs::read_dir(&dir).ok().and_then(|entries| {
        entries.flatten().map(|entry| entry.path()).find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().eq_ignore_ascii_case(GAME_PREFS_FILE))
                && path.is_file()
        })
    });
    existing.unwrap_or_else(|| dir.join(GAME_PREFS_FILE))
}

/// FA's own `game.prefs` file: `%LOCALAPPDATA%\Gas Powered Games\Supreme
/// Commander Forged Alliance\game.prefs` (confirmed via the Python
/// client's `util.LOCALFOLDER`/`PREFSFILENAME`). `FAF_GAME_PREFS_PATH`
/// overrides it (tests, alternate installs).
///
/// Off Windows the game runs under Wine, and `%LOCALAPPDATA%` is then a
/// directory *inside the prefix*, not this machine's own local data directory.
/// Resolving it the Windows way there produces a real path that FA has never
/// written to, so the client reads no mods, writes an `active_mods` block
/// nothing loads, and every mod toggle silently does nothing. The configured
/// prefix therefore comes first: see [`wine_local_app_data`].
pub(crate) fn game_prefs_path() -> PathBuf {
    if let Some(path) = crate::infra::paths::game_prefs_path() {
        return path;
    }
    if let Ok(path) = std::env::var("FAF_GAME_PREFS_PATH") {
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    let local = if cfg!(windows) {
        None
    } else {
        wine_prefix_root().and_then(|prefix| wine_local_app_data(&prefix))
    };
    let local = local.unwrap_or_else(|| {
        directories::BaseDirs::new()
            .map(|b| b.data_local_dir().to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."))
    });
    game_prefs_in(&local)
}

/// The Wine prefix to look inside, off Windows.
///
/// The setting first, then `$WINEPREFIX`, then `~/.wine`, which is the prefix
/// `wine` itself creates when nothing says otherwise. Only the first of these
/// is a choice somebody made; the other two are where the answer usually is.
pub(crate) fn resolved_wine_prefix() -> Option<PathBuf> {
    (!cfg!(windows)).then(wine_prefix_root).flatten()
}

fn wine_prefix_root() -> Option<PathBuf> {
    if let Some(path) = crate::infra::paths::wine_prefix() {
        return Some(path);
    }
    if let Some(path) = std::env::var_os("WINEPREFIX").filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(path));
    }
    directories::BaseDirs::new().map(|dirs| dirs.home_dir().join(".wine"))
}

/// `%LOCALAPPDATA%` inside a Wine prefix.
///
/// A prefix holds one directory per Windows user under `drive_c/users`, and
/// which one it is depends on how the prefix was made: `wine` uses the Linux
/// login name, Proton always uses `steamuser`, and a prefix copied between
/// machines keeps whatever name it was made with. So this looks for the user
/// that actually has a `game.prefs` before it guesses, and only falls back to
/// naming one when the game has never run in this prefix, which is the case
/// where the path is being created rather than read.
fn wine_local_app_data(prefix: &Path) -> Option<PathBuf> {
    let users = prefix.join("drive_c").join("users");
    let local_of = |user: &Path| user.join("AppData").join("Local");
    let has_prefs = |local: &Path| game_prefs_in(local).is_file();

    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&users)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    // Deterministic, so two runs of a prefix with two users agree with each
    // other; `read_dir` order is the filesystem's business.
    candidates.sort();
    if let Some(found) = candidates
        .iter()
        .map(|user| local_of(user))
        .find(|local| has_prefs(local))
    {
        return Some(found);
    }

    let named = std::env::var("USER")
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "steamuser".to_string());
    let guess = local_of(&users.join(named));
    // Only worth returning when the prefix is real. Handing back a path under
    // a prefix that does not exist would send the fallback to this machine's
    // own local data directory anyway, and this way that decision is made
    // where it can be explained.
    users.is_dir().then_some(guess)
}

async fn read_active_mod_uids() -> Vec<String> {
    match tokio::fs::read_to_string(game_prefs_path()).await {
        Ok(contents) => parse_active_mod_uids(&contents),
        Err(_) => Vec::new(),
    }
}

/// The player's own active mods, set aside while a replay's sim mods stand in
/// for them in `game.prefs`, with a count of how often that has happened.
///
/// A modded replay needs its own sim mods active to play back at all, and it
/// used to leave them there. The next game the player hosted then carried a
/// replay's mods nobody chose, which is the accident the request was about
/// (#343). So the set the player had is kept here, once, however many modded
/// replays follow each other, and written back when the last of them closes.
///
/// Process memory rather than a file: a client restarted mid-replay forgets
/// it and leaves the replay's set in place, which is what every replay did
/// before and no worse.
static OWN_MODS_DURING_REPLAY: std::sync::Mutex<Option<Vec<String>>> = std::sync::Mutex::new(None);
static REPLAY_MODS_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Put a replay's mod set in place, keeping the player's own for later.
///
/// Returns the generation of this override. The restore that belongs to it
/// only acts while no later replay has replaced it: see
/// [`restore_own_mods_after_replay`].
pub(crate) async fn activate_replay_mods(
    own: Vec<String>,
    replay: &[String],
) -> Result<u64, String> {
    {
        let mut saved = OWN_MODS_DURING_REPLAY.lock().unwrap();
        // The first modded replay's view of the player's set is the real one;
        // a second one would only see the first replay's mods.
        if saved.is_none() {
            *saved = Some(own);
        }
    }
    let generation = REPLAY_MODS_GENERATION.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
    write_active_mod_uids_to_disk(replay).await?;
    Ok(generation)
}

/// Give the player's own mods back, if a replay set them aside.
///
/// `Some(generation)` is the restore a replay's watcher performs when its
/// window closes, and it does nothing if a later replay has taken over since:
/// that one owns the restore now. `None` restores unconditionally, which is
/// what a replay *without* sim mods does before it launches, so it does not
/// start with the previous replay's mods still active.
pub(crate) async fn restore_own_mods_after_replay(generation: Option<u64>) {
    let own = {
        let current = REPLAY_MODS_GENERATION.load(std::sync::atomic::Ordering::SeqCst);
        if generation.is_some_and(|expected| expected != current) {
            return;
        }
        OWN_MODS_DURING_REPLAY.lock().unwrap().take()
    };
    let Some(own) = own else {
        return;
    };
    REPLAY_MODS_GENERATION.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    if let Err(reason) = write_active_mod_uids_to_disk(&own).await {
        tracing::warn!(%reason, "could not give the player's own mods back after a replay");
    }
}

pub(crate) async fn write_active_mod_uids_to_disk(uids: &[String]) -> Result<(), String> {
    let path = game_prefs_path();
    // A read failure (missing file, locked, non-UTF-8 bytes, …) must abort,
    // never be treated as an empty file. `game.prefs` is FA's *entire*
    // config (hotkeys, video, audio, profiles); the previous
    // `.unwrap_or_default()` would have replaced all of it with a lone
    // `active_mods` block on any read hiccup. Mirrors Python's
    // `setActiveMods` returning `False` when it can't read the file, and
    // never creating one that doesn't exist.
    let contents = tokio::fs::read_to_string(&path).await.map_err(|e| {
        format!(
            "could not read {}: leaving it untouched: {e}",
            path.display()
        )
    })?;
    let updated = write_active_mod_uids(&contents, uids);
    tokio::fs::write(&path, updated)
        .await
        .map_err(|e| format!("could not write {}: {e}", path.display()))
}

/// Byte range `[start, end]` (inclusive) of the **whole**
/// `active_mods = { ... }` block: from the first byte of `active_mods` to
/// the closing brace: in `game.prefs`'s contents, if present. A balanced-
/// brace scan rather than the reference clients' regex
/// (`active_mods\s*=\s*{.*?}`), without adding a regex dependency for one
/// small parser.
///
/// Two hard-won properties, both mirroring what Python's regex gives for
/// free (and both confirmed live as corruption vectors when absent):
/// - The span *includes* the `active_mods = ` prefix. An earlier version
///   returned only the brace span while [`write_active_mod_uids`] spliced
///   in a replacement that itself starts with `active_mods = `: producing
///   `active_mods = active_mods = { … }`, which FA's Lua parser rejects,
///   whereupon FA discards the *entire* prefs file (renames it `.bad` and
///   regenerates defaults: every hotkey and setting gone).
/// - The key must be followed by `\s*=\s*{` to match, like the regex. A
///   bare `.find('{')` after any occurrence of the substring `active_mods`
///   could otherwise pair the key with some unrelated later table and
///   splice away everything in between.
fn find_active_mods_block(contents: &str) -> Option<(usize, usize)> {
    const KEY: &str = "active_mods";
    let mut from = 0;
    while let Some(rel) = contents[from..].find(KEY) {
        let start = from + rel;
        let after_key = contents[start + KEY.len()..].trim_start();
        if let Some(after_eq) = after_key.strip_prefix('=') {
            let after_eq = after_eq.trim_start();
            if after_eq.starts_with('{') {
                // All slices above borrow from `contents`, so the remaining
                // length gives the brace's byte offset directly.
                let brace_start = contents.len() - after_eq.len();
                let mut depth = 0i32;
                for (i, c) in contents[brace_start..].char_indices() {
                    match c {
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                return Some((start, brace_start + i));
                            }
                        }
                        _ => {}
                    }
                }
                return None; // unbalanced braces: refuse to splice blindly
            }
        }
        from = start + KEY.len();
    }
    None
}

/// Distinct uids with `['uid'] = true` in the `active_mods` table. Missing
/// file, missing section, or a malformed table all return an empty list
/// (mirrors the Python client's own graceful fallback), not an error.
fn parse_active_mod_uids(contents: &str) -> Vec<String> {
    let Some((start, end)) = find_active_mods_block(contents) else {
        return Vec::new();
    };
    let block = &contents[start..=end];

    let mut uids = Vec::new();
    let mut rest = block;
    while let Some(open) = rest.find("['") {
        rest = &rest[open + 2..];
        let Some(close) = rest.find("']") else {
            break;
        };
        let uid = rest[..close].to_string();
        rest = &rest[close + 2..];
        let Some(eq) = rest.find('=') else {
            break;
        };
        let after_eq = rest[eq + 1..].trim_start();
        if after_eq.starts_with("true") {
            uids.push(uid);
        }
        rest = after_eq;
    }
    uids
}

/// Rebuilds `game.prefs`'s `active_mods` block (or appends a fresh one if
/// absent), leaving the rest of the file untouched: mirrors Python's
/// `setActiveMods` regex-substitution approach exactly, since `game.prefs`
/// is a shared FA config file with many other unrelated keys we must not
/// clobber. Only enabled uids are ever written (disabled mods are simply
/// omitted, matching `setActiveMods` writing only `['uid'] = true` entries).
fn write_active_mod_uids(contents: &str, uids: &[String]) -> String {
    let block = build_active_mods_block(uids);
    match find_active_mods_block(contents) {
        Some((start, end)) => format!("{}{}{}", &contents[..start], block, &contents[end + 1..]),
        None => {
            let mut new_contents = contents.to_string();
            if !new_contents.is_empty() && !new_contents.ends_with('\n') {
                new_contents.push('\n');
            }
            new_contents.push_str(&block);
            new_contents.push('\n');
            new_contents
        }
    }
}

fn build_active_mods_block(uids: &[String]) -> String {
    let entries: Vec<String> = uids
        .iter()
        .map(|uid| format!("    ['{uid}'] = true"))
        .collect();
    format!("active_mods = {{\n{}\n}}", entries.join(",\n"))
}

/// What a vault listing has to bring back with each mod.
///
/// `latestVersion.reviewsSummary` is the part that was missing, and it is why
/// a mod with reviews on it showed "Rating N/A" in the search results while
/// opening the same mod showed the reviews. A review is written against a mod
/// *version*, so that is where the summary hangs; `mod.reviewsSummary` is the
/// whole mod's, and only the older mods have one at all.
///
/// The parser has always looked in both places. It reads them out of the
/// document's `included` block, which is the half this decides: a relationship
/// that is linked but not included is a dangling id, and the lookup answered
/// nothing.
const MOD_VAULT_INCLUDE: &str =
    "latestVersion,latestVersion.reviewsSummary,reviewsSummary,uploader";

fn parse_reviews_summary(summary: &JsonApiResource) -> (i32, i32) {
    let reviews = value_i32(&summary.attributes, "reviews")
        .or_else(|| value_i32(&summary.attributes, "numReviews"))
        .or_else(|| value_i32(&summary.attributes, "totalReviews"))
        .or_else(|| value_i32(&summary.attributes, "count"))
        .unwrap_or(0);

    let avg_score = value_f64(&summary.attributes, "averageScore")
        .or_else(|| {
            let score = value_f64(&summary.attributes, "score")?;
            if reviews > 0 {
                Some(score / f64::from(reviews))
            } else {
                Some(score)
            }
        })
        .or_else(|| value_f64(&summary.attributes, "rating"))
        .unwrap_or(0.0);

    let rating_tenths = (avg_score * 10.0).round() as i32;
    (rating_tenths, reviews)
}

/// Every version's reviews, not only the latest's: see `infra::review_totals`.
fn raise_ratings(mods: &mut [VaultMod], totals: &HashMap<i32, ReviewTotal>) {
    for vault_mod in mods {
        if let Some(total) = totals.get(&vault_mod.mod_id) {
            total.raise(&mut vault_mod.rating_tenths, &mut vault_mod.reviews);
        }
    }
}

fn parse_vault_mods(doc: &JsonApiDoc) -> Vec<VaultMod> {
    let index = resource_index(&doc.included);
    doc.data
        .iter()
        .filter_map(|mod_res| {
            let (_, version_id) = rel_target(&mod_res.relationships, "latestVersion")?;
            let version = index.get(&("modVersion".to_string(), version_id))?;
            let uploader_rel = rel_target(&mod_res.relationships, "uploader");
            // The relationship's own id, so ownership does not depend on the
            // `player` resource having been included: it is in the linkage
            // either way.
            let uploader_id = uploader_rel
                .as_ref()
                .and_then(|(_, id)| id.parse::<i32>().ok());
            let uploader = uploader_rel
                .and_then(|rel| find_rel_resource(doc, &index, Some(rel)))
                .and_then(|player| player.attributes.get("login"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            // Both summaries, not the first one found. A review is written
            // against a version, so that is where the count usually is, while
            // the older entries carry one on the parent as well. Taking the
            // parent's whenever it existed is what printed "N/A" over a
            // version with reviews on it: an empty summary is still a summary,
            // and it won.
            let reviews_summary = [
                rel_target(&mod_res.relationships, "reviewsSummary"),
                rel_target(&mod_res.relationships, "modReviewsSummary"),
                rel_target(&version.relationships, "reviewsSummary"),
                rel_target(&version.relationships, "modVersionReviewsSummary"),
            ]
            .into_iter()
            .flatten()
            .filter_map(|rel| find_rel_resource(doc, &index, Some(rel)))
            .map(parse_reviews_summary)
            // Ties keep the first, which is the mod's own: the order above is
            // the order to believe them in when they agree on how many, and a
            // summary hanging off the mod counts every version's reviews while
            // the latest version's counts only its own. `max_by_key` keeps the
            // *last* of equal elements, which is the other way round.
            .reduce(|best, next| if next.1 > best.1 { next } else { best });

            let (rating_tenths, reviews) =
                if let Some(summary) = reviews_summary.filter(|(_, reviews)| *reviews > 0) {
                    summary
                } else if let Some(summary_attr) = mod_res
                    .attributes
                    .get("reviewsSummary")
                    .or_else(|| version.attributes.get("reviewsSummary"))
                {
                    let r = value_i32(summary_attr, "reviews")
                        .or_else(|| value_i32(summary_attr, "numReviews"))
                        .unwrap_or(0);
                    let score = value_f64(summary_attr, "averageScore")
                        .or_else(|| {
                            let s = value_f64(summary_attr, "score")?;
                            if r > 0 {
                                Some(s / f64::from(r))
                            } else {
                                Some(s)
                            }
                        })
                        .unwrap_or(0.0);
                    ((score * 10.0).round() as i32, r)
                } else {
                    (0, 0)
                };

            // The exact wire value for `modType` (`"UI"`/`"SIM"` or
            // something else) couldn't be verified against a live
            // authenticated call this session: same caveat as the
            // leaderboard's `leagueLeaderboard` type name. Defaults to
            // `Sim`, matching the reference clients' own `ui_only`
            // default of `false`.
            let mod_type = version
                .attributes
                .get("type")
                .or_else(|| version.attributes.get("modType"))
                .and_then(Value::as_str)
                .map(|s| {
                    if s.eq_ignore_ascii_case("ui") {
                        ModType::Ui
                    } else {
                        ModType::Sim
                    }
                })
                .unwrap_or(ModType::Sim);

            let version_str = match version.attributes.get("version") {
                Some(Value::String(s)) => s.clone(),
                Some(Value::Number(n)) => n.to_string(),
                _ => "1".to_string(),
            };

            Some(VaultMod {
                mod_id: mod_res.id.parse().unwrap_or_default(),
                version_id: version.id.parse().unwrap_or_default(),
                display_name: mod_res
                    .attributes
                    .get("displayName")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown mod")
                    .to_string(),
                author: mod_res
                    .attributes
                    .get("author")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                uploader,
                uploader_id,
                uid: version
                    .attributes
                    .get("uid")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                version: version_str,
                description: version
                    .attributes
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                filename: version
                    .attributes
                    .get("filename")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                mod_type,
                ranked: version
                    .attributes
                    .get("ranked")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                recommended: value_bool(&mod_res.attributes, "recommended"),
                rating_tenths,
                reviews,
                // The mod's own creation time, not the latest version's.
                //
                // Both resources carry `createTime`, and the version's is the
                // moment that *version* was uploaded: for any mod whose author
                // has ever shipped an update, reading it here makes "Published"
                // and "Updated" the same date. Falling back to the version keeps
                // a mod the API answers without the field showing something.
                created_at: mod_res
                    .attributes
                    .get("createTime")
                    .and_then(Value::as_str)
                    .filter(|time| !time.is_empty())
                    .or_else(|| version.attributes.get("createTime").and_then(Value::as_str))
                    .unwrap_or("")
                    .to_string(),
                updated_at: version
                    .attributes
                    .get("updateTime")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                download_url: version
                    .attributes
                    .get("downloadUrl")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                thumbnail_url: version
                    .attributes
                    .get("thumbnailUrl")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            })
        })
        .collect()
}

/// Inert mods client: used offline and in tests (mirrors
/// [`crate::infra::FakeMaps`]).
#[derive(Debug, Clone, Default)]
pub struct FakeMods;

#[async_trait]
impl ModsPort for FakeMods {
    async fn list_vault(&self) -> Result<Vec<VaultMod>, String> {
        Err("mod vault is unavailable in offline mode".to_string())
    }

    async fn search_vault(&self, _query: ModVaultQuery) -> Result<ModSearchPage, String> {
        Err("mod vault is unavailable in offline mode".to_string())
    }

    async fn list_installed(&self) -> Result<Vec<InstalledMod>, String> {
        Err("mod install listing is unavailable in offline mode".to_string())
    }

    async fn download_sizes(&self, targets: Vec<ModDownloadTarget>) -> Vec<ModDownloadSize> {
        // Offline: every answer is "no idea", which the dialog already draws.
        targets
            .into_iter()
            .map(|target| ModDownloadSize {
                uid: target.uid,
                bytes: None,
            })
            .collect()
    }

    async fn install_mod(
        &self,
        _uid: String,
        _download_url: String,
    ) -> Result<Vec<InstalledMod>, String> {
        Err("mod install is unavailable in offline mode".to_string())
    }

    async fn update_mod(
        &self,
        _uid: String,
        _folder_name: String,
        _download_url: String,
    ) -> Result<Vec<InstalledMod>, String> {
        Err("mod update is unavailable in offline mode".to_string())
    }

    async fn uninstall_mod(&self, _folder_name: String) -> Result<Vec<InstalledMod>, String> {
        Err("mod uninstall is unavailable in offline mode".to_string())
    }

    async fn toggle_mod(&self, _uid: String, _enabled: bool) -> Result<Vec<InstalledMod>, String> {
        Err("mod toggling is unavailable in offline mode".to_string())
    }

    async fn set_active_mods(&self, _uids: Vec<String>) -> Result<Vec<InstalledMod>, String> {
        Err("mod toggling is unavailable in offline mode".to_string())
    }

    async fn ensure_game_mods(
        &self,
        _mods: &BTreeMap<String, String>,
        _replace_conflicts: bool,
    ) -> Result<(), ModPrepFailure> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_content_range_header_yields_the_whole_size() {
        // What a one-byte ranged GET answers with, and the reason it is asked:
        // the vault's storage answers HEAD with `Content-Length: 0`, so this is
        // the header the size actually comes from.
        assert_eq!(content_range_total("bytes 0-0/12345"), Some(12_345));
        assert_eq!(content_range_total("bytes 0-0/1"), Some(1));
    }

    #[test]
    fn a_header_with_no_total_in_it_is_no_answer() {
        // `*` is a server saying it will not tell you, and the other two are
        // not this header at all. None of them may be reported as a size.
        assert_eq!(content_range_total("bytes 0-0/*"), None);
        assert_eq!(content_range_total("bytes */*"), None);
        assert_eq!(content_range_total("12345"), None);
        assert_eq!(content_range_total(""), None);
    }
    use serde_json::json;

    /// Make `<prefix>/drive_c/users/<user>/AppData/Local`, and the `game.prefs`
    /// under it when asked, so these tests describe a prefix rather than a
    /// mock of one.
    fn wine_user(prefix: &Path, user: &str, with_prefs: bool) -> PathBuf {
        let local = prefix
            .join("drive_c")
            .join("users")
            .join(user)
            .join("AppData")
            .join("Local");
        // Spelled the way the game spells it, which is what the lookup has to
        // find on a case-sensitive filesystem.
        let prefs = GAME_PREFS_DIR
            .iter()
            .fold(local.clone(), |path, part| path.join(part))
            .join("Game.prefs");
        std::fs::create_dir_all(prefs.parent().unwrap()).unwrap();
        if with_prefs {
            std::fs::write(&prefs, "active_mods = { }").unwrap();
        }
        local
    }

    #[test]
    fn a_prefix_is_searched_for_the_user_that_has_actually_played() {
        let temp = tempfile::tempdir().unwrap();
        let prefix = temp.path();
        // Two users, which is what a prefix made by Wine and then used by
        // Proton looks like. Only one of them has ever run the game.
        wine_user(prefix, "player", false);
        let played = wine_user(prefix, "steamuser", true);
        assert_eq!(wine_local_app_data(prefix), Some(played));
    }

    #[test]
    fn a_prefix_nobody_has_played_in_still_names_a_place_to_write() {
        let temp = tempfile::tempdir().unwrap();
        let prefix = temp.path();
        let only = wine_user(prefix, "steamuser", false);
        // No `game.prefs` anywhere: the answer is where one would go, so
        // enabling a mod creates the file the game will read.
        let found = wine_local_app_data(prefix).expect("a real prefix has an answer");
        assert!(
            found.starts_with(prefix.join("drive_c").join("users")),
            "{found:?} is not inside the prefix"
        );
        assert!(found.ends_with("AppData/Local") || found.ends_with(r"AppData\Local"));
        let _ = only;
    }

    #[test]
    fn the_prefs_file_is_found_in_the_case_it_was_written_in() {
        let temp = tempfile::tempdir().unwrap();
        let local = wine_user(temp.path(), "steamuser", true);
        let found = game_prefs_in(&local);
        assert!(found.is_file(), "{found:?} is the file the game wrote");
        assert_eq!(found.file_name().unwrap(), "Game.prefs");
    }

    #[test]
    fn a_prefix_that_is_not_there_has_no_answer_at_all() {
        let temp = tempfile::tempdir().unwrap();
        // Nothing under it: not a prefix, so the caller falls back to this
        // machine's own local data directory rather than inventing a path
        // inside a directory that does not exist.
        assert_eq!(wine_local_app_data(&temp.path().join("nope")), None);
    }

    const SAMPLE_MOD_INFO: &str = r#"
        -- FAF mod
        name = "Total Mayhem"
        uid = "dcd9a5e5-5444-4266-a016-ccbbff528268"
        version = 12
        author = "Some Author"
        ui_only = false
        description = "Extended content"
    "#;

    #[test]
    fn parses_mod_info_flat_fields() {
        let info = parse_mod_info(SAMPLE_MOD_INFO).expect("should parse");
        assert_eq!(info.name, "Total Mayhem");
        assert_eq!(info.uid, "dcd9a5e5-5444-4266-a016-ccbbff528268");
        assert_eq!(info.version, "12");
        assert_eq!(info.author, "Some Author");
        assert_eq!(info.description, "Extended content");
        assert!(!info.ui_only);
    }

    #[test]
    fn a_long_string_description_is_read_to_its_end() {
        // The shape the report showed as "[[": the description opens a Lua
        // long string and its text is on the lines below.
        let info = parse_mod_info(
            "name = \"ACU Enhancements\"\nuid = \"acu-enhancements-v1.0.2\"\ndescription = [[\nBetter ACU upgrades.\nWorks with -- dashes.\n]],\nversion = 4\n",
        )
        .expect("should parse");
        assert_eq!(
            info.description,
            "Better ACU upgrades.\nWorks with -- dashes."
        );
        // The fields after the long string are still read.
        assert_eq!(info.version, "4");
    }

    #[test]
    fn long_strings_on_one_line_and_with_levels_are_read() {
        let one_line = parse_mod_info("uid = \"a\"\ndescription = [[Short one.]]").unwrap();
        assert_eq!(one_line.description, "Short one.");
        let levelled =
            parse_mod_info("uid = \"a\"\ndescription = [==[\nHas ]] inside\n]==]").unwrap();
        assert_eq!(levelled.description, "Has ]] inside");
    }

    #[test]
    fn quoted_values_keep_their_dashes_and_lose_trailing_comments() {
        let info = parse_mod_info(
            "uid = \"a\" -- the id\nauthor = \"Tech 1 -- Tech 3\",\nversion = 7, -- bumped\n",
        )
        .unwrap();
        assert_eq!(info.uid, "a");
        assert_eq!(info.author, "Tech 1 -- Tech 3");
        assert_eq!(info.version, "7");
    }

    #[test]
    fn parse_mod_info_none_without_uid() {
        assert!(parse_mod_info("name = \"No UID Mod\"").is_none());
    }

    #[tokio::test]
    async fn required_mod_lookup_rejects_untrusted_uids_before_network_access() {
        let client = ModsClient::new(
            ModsConfig {
                api_base: "https://api.invalid".into(),
                content_base: "https://content.invalid".into(),
            },
            crate::infra::session::TokenStore::new(),
        );
        let error = client
            .required_mod_version("valid-looking' || hidden==false")
            .await
            .err()
            .expect("a filter-injection uid must be rejected");
        assert!(error.contains("invalid simulation-mod uid"));
    }

    #[test]
    fn parse_mod_info_applies_defaults() {
        let info = parse_mod_info("uid = \"abc-123\"").expect("should parse");
        assert_eq!(info.name, "abc-123");
        assert_eq!(info.version, "1");
        assert_eq!(info.author, "");
        assert_eq!(info.description, "");
        assert!(!info.ui_only);
    }

    #[test]
    fn parse_mod_info_recognizes_ui_only() {
        let info = parse_mod_info("uid = \"abc-123\"\nui_only = true").expect("should parse");
        assert!(info.ui_only);
    }

    #[test]
    fn mod_folder_may_have_one_safe_wrapper_directory() {
        let root = Path::new("mods");
        assert_eq!(
            safe_mod_target(root, "total_mayhem"),
            Ok(root.join("total_mayhem"))
        );
        assert_eq!(
            safe_mod_target(root, "bundle/mod"),
            Ok(root.join("bundle/mod"))
        );
        assert!(safe_mod_target(root, "../outside").is_err());
        assert!(safe_mod_target(root, "nested/mod/deeper").is_err());
        assert!(safe_mod_target(root, ".").is_err());
    }

    #[test]
    fn active_mods_round_trips_through_write_then_parse() {
        let original = "some_other_setting = 1\nactive_mods = {\n    ['old-uid'] = true,\n}\nmore_settings = 2\n";
        let updated =
            write_active_mod_uids(original, &["new-uid".to_string(), "other-uid".to_string()]);
        assert!(updated.contains("some_other_setting = 1"));
        assert!(updated.contains("more_settings = 2"));
        assert!(!updated.contains("old-uid"));

        let uids = parse_active_mod_uids(&updated);
        assert_eq!(uids, vec!["new-uid".to_string(), "other-uid".to_string()]);
    }

    #[test]
    fn active_mods_appends_block_when_missing() {
        let original = "some_setting = 1\n";
        let updated = write_active_mod_uids(original, &["abc-123".to_string()]);
        assert!(updated.contains("some_setting = 1"));
        assert_eq!(parse_active_mod_uids(&updated), vec!["abc-123".to_string()]);
    }

    /// Regression: replacing an existing block must never leave a doubled
    /// `active_mods = active_mods = { … }` behind. Semantic round-trip
    /// tests can't catch this ([`parse_active_mod_uids`] skips to the first
    /// brace, so it parses the doubled form happily): but FA's Lua parser
    /// rejects it and then throws away the user's *entire* prefs file
    /// (renamed `.bad`, defaults regenerated: all hotkeys/settings lost).
    /// Confirmed live before this fix.
    #[test]
    fn active_mods_rewrite_emits_the_key_exactly_once() {
        let original =
            "keys = { ['F1'] = 'help' }\nactive_mods = {\n    ['old-uid'] = true\n}\ntail = 2\n";
        let updated = write_active_mod_uids(original, &["new-uid".to_string()]);
        assert_eq!(
            updated.matches("active_mods").count(),
            1,
            "doubled/leftover active_mods key in: {updated}"
        );
        assert!(updated.contains("keys = { ['F1'] = 'help' }"));
        assert!(updated.contains("tail = 2"));

        // Rewriting the rewrite must stay stable too (idempotence guards
        // against prefix duplication compounding across launches).
        let twice = write_active_mod_uids(&updated, &["new-uid".to_string()]);
        assert_eq!(twice, updated);
    }

    /// The key must be followed by `= {` to count as the block: a stray
    /// `active_mods` substring elsewhere (comment, other key) must not make
    /// the splice grab an unrelated table's braces.
    #[test]
    fn find_active_mods_block_requires_assignment_shape() {
        let contents = "my_active_mods_note = 1\nother = { ['x'] = true }\n";
        assert_eq!(find_active_mods_block(contents), None);

        let real = "note_about_active_mods = 1\nactive_mods = {\n    ['a'] = true\n}\n";
        let (start, end) = find_active_mods_block(real).expect("should find the real block");
        assert!(real[start..].starts_with("active_mods = {"));
        assert_eq!(&real[end..=end], "}");
    }

    #[test]
    fn parse_active_mod_uids_defaults_gracefully_without_section() {
        assert!(parse_active_mod_uids("no_active_mods_here = 1\n").is_empty());
    }

    /// A simulation mod that ships its UI-only variant inside itself. Both are
    /// mods, the game loads both, and the outer one used to hide the inner.
    #[tokio::test]
    async fn a_mod_inside_a_mod_is_found_as_well_as_its_parent() {
        let dir = std::env::temp_dir().join(format!("forge-mods-nested-{}", std::process::id()));
        let outer = dir.join("sim_speed_balancer");
        let inner = outer.join("ui_variant");
        tokio::fs::create_dir_all(&inner).await.unwrap();
        tokio::fs::write(outer.join("mod_info.lua"), SAMPLE_MOD_INFO)
            .await
            .unwrap();
        tokio::fs::write(
            inner.join("mod_info.lua"),
            "name = \"Sim Speed Balancer (UI)\"
uid = \"11111111-2222-3333-4444-555555555555\"
ui_only = true
",
        )
        .await
        .unwrap();

        let installed = list_installed_dir(&dir).await.expect("should list");
        let folders: Vec<&str> = installed
            .iter()
            .map(|entry| entry.folder_name.as_str())
            .collect();
        assert_eq!(installed.len(), 2, "found {folders:?}");
        assert!(folders.contains(&"sim_speed_balancer"));
        // Relative to the mods folder and with forward slashes, which is the
        // shape every other nested mod already uses.
        assert!(folders.contains(&"sim_speed_balancer/ui_variant"));
        let ui = installed
            .iter()
            .find(|entry| entry.folder_name.ends_with("ui_variant"))
            .unwrap();
        assert_eq!(ui.mod_type, ModType::Ui);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    /// Mod authors keep their sources elsewhere and link the folder in. The
    /// scan read the link's own type, which is "symlink" and not "directory",
    /// and skipped it.
    #[cfg(windows)]
    #[tokio::test]
    async fn a_linked_mod_folder_is_found() {
        let dir = std::env::temp_dir().join(format!("forge-mods-link-{}", std::process::id()));
        let real = std::env::temp_dir().join(format!("forge-mods-src-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::create_dir_all(&real).await.unwrap();
        tokio::fs::write(real.join("mod_info.lua"), SAMPLE_MOD_INFO)
            .await
            .unwrap();

        // Needs either developer mode or elevation; where neither is on, the
        // link cannot be made and there is nothing to assert.
        if std::os::windows::fs::symlink_dir(&real, dir.join("linked_mod")).is_err() {
            let _ = tokio::fs::remove_dir_all(&dir).await;
            let _ = tokio::fs::remove_dir_all(&real).await;
            return;
        }

        let installed = list_installed_dir(&dir).await.expect("should list");
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].folder_name, "linked_mod");

        let _ = tokio::fs::remove_dir_all(&dir).await;
        let _ = tokio::fs::remove_dir_all(&real).await;
    }

    #[tokio::test]
    async fn list_installed_dir_missing_folder_returns_empty() {
        let dir = std::env::temp_dir().join("forge-mods-does-not-exist");
        let installed = list_installed_dir(&dir)
            .await
            .expect("missing dir is not an error");
        assert!(installed.is_empty());
    }

    #[tokio::test]
    async fn list_installed_dir_parses_mod_info_and_skips_invalid_folders() {
        let dir = std::env::temp_dir().join(format!("forge-mods-test-{}", std::process::id()));
        let good = dir.join("total_mayhem");
        tokio::fs::create_dir_all(&good).await.unwrap();
        tokio::fs::write(good.join("mod_info.lua"), SAMPLE_MOD_INFO)
            .await
            .unwrap();

        let bad = dir.join("not_a_mod");
        tokio::fs::create_dir_all(&bad).await.unwrap();
        // No mod_info.lua at all: should be skipped.

        // A retained replacement backup must not appear as another installed mod.
        let staged = dir.join(".faf-install-0123456789abcdef/previous");
        tokio::fs::create_dir_all(&staged).await.unwrap();
        tokio::fs::write(staged.join("mod_info.lua"), SAMPLE_MOD_INFO)
            .await
            .unwrap();

        let installed = list_installed_dir(&dir).await.expect("should list");
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].uid, "dcd9a5e5-5444-4266-a016-ccbbff528268");
        assert_eq!(installed[0].folder_name, "total_mayhem");
        assert!(!installed[0].enabled); // no game.prefs override in this test env

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn list_installed_dir_finds_mod_below_one_wrapper_directory() {
        let dir = std::env::temp_dir().join(format!(
            "forge-nested-mods-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let nested = dir.join("download_bundle").join("total_mayhem");
        tokio::fs::create_dir_all(&nested).await.unwrap();
        tokio::fs::write(nested.join("mod_info.lua"), SAMPLE_MOD_INFO)
            .await
            .unwrap();

        let installed = list_installed_dir(&dir)
            .await
            .expect("should list nested mod");
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].folder_name, "download_bundle/total_mayhem");

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[test]
    fn the_uploader_id_arrives_even_when_the_player_is_not_included() {
        // It is in the relationship linkage rather than the included document,
        // so "is this mine" does not depend on the include list.
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [{
                "type": "mod",
                "id": "3",
                "attributes": { "displayName": "Total Mayhem", "author": "Someone Else" },
                "relationships": {
                    "latestVersion": { "data": { "type": "modVersion", "id": "9" } },
                    "uploader": { "data": { "type": "player", "id": "4711" } },
                },
            }],
            "included": [{
                "type": "modVersion",
                "id": "9",
                "attributes": { "uid": "abc-123" },
            }],
        }))
        .unwrap();

        let mods = parse_vault_mods(&doc);
        assert_eq!(mods[0].uploader_id, Some(4711));
        assert_eq!(mods[0].uploader, "");
        // And it is not the declared author, which anyone can write into
        // `mod_info.lua`.
        assert_eq!(mods[0].author, "Someone Else");
    }

    /// The report: a mod with reviews on it showed "Rating N/A" in the search
    /// results while opening it showed the reviews. A review belongs to a mod
    /// *version*, and a parent summary that exists but counts nothing used to
    /// win simply for being looked at first.
    #[test]
    fn a_version_summary_beats_an_empty_one_on_the_mod() {
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [{
                "type": "mod",
                "id": "77",
                "attributes": { "displayName": "Roguelike Mode", "author": "Someone" },
                "relationships": {
                    "latestVersion": { "data": { "type": "modVersion", "id": "9" } },
                    "reviewsSummary": { "data": { "type": "reviewsSummary", "id": "15" } },
                },
            }],
            "included": [
                {
                    "type": "modVersion",
                    "id": "9",
                    "attributes": { "uid": "abc-123", "version": 3 },
                    "relationships": {
                        "reviewsSummary": { "data": { "type": "reviewsSummary", "id": "16" } },
                    },
                },
                {
                    "type": "reviewsSummary",
                    "id": "15",
                    "attributes": { "averageScore": 0.0, "reviews": 0 }
                },
                {
                    "type": "reviewsSummary",
                    "id": "16",
                    "attributes": { "averageScore": 4.5, "reviews": 6 }
                }
            ],
        }))
        .unwrap();

        let mods = parse_vault_mods(&doc);
        assert_eq!(mods.len(), 1);
        assert_eq!(mods[0].reviews, 6);
        assert_eq!(mods[0].rating_tenths, 45);
    }

    #[test]
    fn parses_vault_mods_resolving_version_through_included() {
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [
                {
                    "type": "mod",
                    "id": "77",
                    "attributes": {
                        "displayName": "Total Mayhem",
                        "author": "Some Author",
                        "recommended": true,
                        "createTime": "2019-05-06T07:08:09Z"
                    },
                    "relationships": {
                        "latestVersion": { "data": { "type": "modVersion", "id": "9" } },
                        "uploader": { "data": { "type": "player", "id": "5" } },
                        "reviewsSummary": { "data": { "type": "reviewsSummary", "id": "15" } },
                    },
                },
            ],
            "included": [
                {
                    "type": "modVersion",
                    "id": "9",
                    "attributes": {
                        "uid": "dcd9a5e5-5444-4266-a016-ccbbff528268",
                        "version": 12,
                        "description": "Adds new units and experimentals.",
                        "filename": "total_mayhem.zip",
                        "type": "SIM",
                        "ranked": false,
                        "downloadUrl": "https://content.faforever.com/mods/total_mayhem.zip",
                        "thumbnailUrl": "https://content.faforever.com/mods/total_mayhem.png",
                        "createTime": "2025-01-02T03:04:05Z",
                        "updateTime": "2026-02-03T04:05:06Z"
                    },
                },
                {
                    "type": "player",
                    "id": "5",
                    "attributes": { "login": "VaultUploader" }
                },
                {
                    "type": "reviewsSummary",
                    "id": "15",
                    "attributes": { "averageScore": 4.46, "reviews": 31 }
                }
            ],
        }))
        .unwrap();

        let mods = parse_vault_mods(&doc);
        assert_eq!(mods.len(), 1);
        assert_eq!(mods[0].display_name, "Total Mayhem");
        assert_eq!(mods[0].mod_id, 77);
        assert_eq!(mods[0].version_id, 9);
        assert_eq!(mods[0].author, "Some Author");
        assert_eq!(mods[0].uploader, "VaultUploader");
        assert_eq!(
            mods[0].uploader_id,
            Some(5),
            "ownership is decided by the uploader's id, not their current login"
        );
        assert_eq!(mods[0].uid, "dcd9a5e5-5444-4266-a016-ccbbff528268");
        assert_eq!(mods[0].version, "12");
        assert_eq!(mods[0].description, "Adds new units and experimentals.");
        assert_eq!(mods[0].filename, "total_mayhem.zip");
        assert_eq!(mods[0].mod_type, ModType::Sim);
        assert!(mods[0].recommended);
        assert_eq!(mods[0].rating_tenths, 45);
        assert_eq!(mods[0].reviews, 31);
        // The mod was first published in 2019; its latest version went up in
        // 2025 and was edited in 2026. "Published" is the first of those.
        assert_eq!(mods[0].created_at, "2019-05-06T07:08:09Z");
        assert_eq!(mods[0].updated_at, "2026-02-03T04:05:06Z");
    }

    #[test]
    fn parse_vault_mods_falls_back_to_the_version_when_the_mod_has_no_create_time() {
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [
                {
                    "type": "mod",
                    "id": "77",
                    "attributes": { "displayName": "Total Mayhem" },
                    "relationships": {
                        "latestVersion": { "data": { "type": "modVersion", "id": "9" } },
                    },
                },
            ],
            "included": [
                {
                    "type": "modVersion",
                    "id": "9",
                    "attributes": {
                        "uid": "dcd9a5e5-5444-4266-a016-ccbbff528268",
                        "createTime": "2025-01-02T03:04:05Z"
                    },
                },
            ],
        }))
        .unwrap();

        let mods = parse_vault_mods(&doc);
        assert_eq!(mods.len(), 1);
        assert_eq!(mods[0].created_at, "2025-01-02T03:04:05Z");
    }

    #[test]
    fn parse_vault_mods_skips_entries_missing_latest_version() {
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [{ "type": "mod", "id": "1", "attributes": {}, "relationships": {} }],
        }))
        .unwrap();
        assert!(parse_vault_mods(&doc).is_empty());
    }

    #[tokio::test]
    async fn fake_mods_fails_cleanly() {
        let fake = FakeMods;
        assert!(fake.list_vault().await.is_err());
        assert!(fake.list_installed().await.is_err());
        assert!(fake
            .install_mod("x".into(), "http://x".into())
            .await
            .is_err());
        assert!(fake.uninstall_mod("x".into()).await.is_err());
        assert!(fake.toggle_mod("x".into(), true).await.is_err());
    }
}
