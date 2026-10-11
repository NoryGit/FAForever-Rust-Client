//! Real maps client: vault browsing + local install management.
//!
//! Mirrors the Python client's `vaults/mapvault/` + `fa/maps.py`:
//!
//! ## Vault listing
//! `GET {api_base}/data/map`, `include=latestVersion,author`,
//! `filter=latestVersion.hidden=='false'`, sorted newest-first: the same
//! JSON:API shape (and the same bearer-token requirement, since the Python
//! client's `ApiAccessManager.get` defaults `authorize=True` for every Data
//! API call) as the replay vault's `/data/game` (see `infra/replay.rs`).
//!
//! ## Installed maps
//! A plain directory scan of the user's maps folder: `<Documents>/My
//! Games/Gas Powered Games/Supreme Commander Forged Alliance/maps`, mirroring
//! `util.VAULTS_BASE_DIR` + `fa.maps.getUserMapsFolder()`. Display names are
//! derived from the folder name the same way as the non-official-map fallback
//! branch of `fa.maps.getDisplayName` (strip a trailing `.vNNNN` version
//! suffix, `_` -> space, title-case): not a full `scenario.lua` parse
//! (`InstalledMapsCache`), which is a later-phase nicety.
//!
//! ## Install / uninstall
//! Installing downloads the version's zip (unauthenticated: the vault CDN,
//! like the replay download host) and extracts it directly into the maps
//! folder, mirroring `fa.maps._doDownloadMap` -> `ZipDownloadExtract` (the
//! zip's own top-level entry is the map's folder name). Uninstalling just
//! removes that directory, mirroring `MapsManagerDialog::delete_map`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use faf_domain::protocol::vault_query::MapVaultQuery;
use faf_domain::state::{
    is_safe_folder_name, InstalledMap, LocalMapPreview, MatchmakerMapPool, MatchmakerPoolMap,
    VaultMap,
};
use serde_json::Value;

use crate::infra::jsonapi::{
    fetch_all_pages_with_progress, fetch_document, find_rel_resource, meta_page_i32,
    patch_resource, rel_target, rel_targets, resource_index, total_pages, value_bool, value_f64,
    value_i32, value_string, JsonApiDoc, JsonApiResource,
};
use crate::infra::review_totals::{self, Subject};
use crate::infra::vault_install::{
    bounded_body_to_file, install_archive_from_file, validate_url, MAX_DOWNLOAD_BYTES,
};
use crate::infra::{env_or, GENERATED_MAP_PLACEHOLDER_URL};
use crate::ports::{MapSearchPage, MapsPort};

/// Maps per vault page fetched in [`MapsClient::list_vault`].
const VAULT_PAGE_SIZE: usize = 100;
/// Upper bound on pages fetched: bounds worst-case work if the vault ever
/// grows huge, without silently truncating the list under normal size.
///
/// 50 was not enough. The live vault passed 5000 maps, which is exactly what
/// 50 pages of 100 holds, so the newest maps had quietly stopped appearing:
/// the cap was doing the silent truncation it exists to avoid. Pages are
/// fetched concurrently now (see `jsonapi::fetch_all_pages`), so headroom costs
/// far less than it did when this was a sequential crawl.
const MAX_VAULT_PAGES: u32 = 200;

#[derive(Debug, Clone)]
pub struct MapsConfig {
    /// FAF Data API base, which serves `/data/map` (vault listing): same
    /// host as the replay vault's `/data/game` (see `ReplayConfig::api_base`).
    pub api_base: String,
    /// Trusted origin for map archives returned by the Data API.
    pub content_base: String,
}

impl MapsConfig {
    pub fn faf() -> Self {
        Self {
            api_base: env_or("FAF_API_BASE", "https://api.faforever.com"),
            content_base: env_or("FAF_CONTENT_BASE", "https://content.faforever.com"),
        }
    }
}

pub struct MapsClient {
    config: MapsConfig,
    tokens: crate::infra::session::TokenStore,
    http: reqwest::Client,
}

impl MapsClient {
    pub fn new(config: MapsConfig, tokens: crate::infra::session::TokenStore) -> Self {
        Self {
            config,
            tokens,
            http: super::http::shared_http_client(),
        }
    }

    pub fn faf(tokens: crate::infra::session::TokenStore) -> Self {
        Self::new(MapsConfig::faf(), tokens)
    }

    /// Every version's reviews, not only the latest's: see
    /// `infra::review_totals`. Best effort: a failure leaves the ratings as
    /// the listing gave them.
    async fn raise_ratings(&self, token: &str, maps: &mut [VaultMap]) {
        let ids: Vec<i32> = maps.iter().map(|map| map.map_id).collect();
        match review_totals::fetch(&self.http, token, &self.config.api_base, Subject::Map, &ids)
            .await
        {
            Ok(totals) => {
                for map in maps {
                    if let Some(total) = totals.get(&map.map_id) {
                        total.raise(&mut map.rating_tenths, &mut map.reviews);
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "could not add up the maps' ratings over their versions")
            }
        }
    }
}

#[async_trait]
impl MapsPort for MapsClient {
    async fn list_vault(&self) -> Result<Vec<VaultMap>, String> {
        self.list_vault_with_progress(None).await
    }

    async fn list_vault_with_progress(
        &self,
        progress: Option<tokio::sync::mpsc::Sender<faf_domain::state::maps::CatalogueProgress>>,
    ) -> Result<Vec<VaultMap>, String> {
        let token = self
            .tokens
            .get()
            .ok_or_else(|| "not logged in".to_string())?;

        // The whole catalogue, because `maps.vault` is not only the Maps tab's
        // list: nine features look maps up in it by folder name to resolve the
        // art, size and player count a game or replay shows (see
        // `shared/mapPresentation.ts`). The reference clients page this vault
        // and have no such index; moving to that shape means giving those
        // lookups their own source first.
        //
        // `MAX_VAULT_PAGES` bounds the worst case; a page is `VAULT_PAGE_SIZE`
        // maps.
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
                let mut url = url::Url::parse(&format!("{api_base}/data/map"))
                    .map_err(|e| format!("invalid API base: {e}"))?;
                url.query_pairs_mut()
                    .append_pair("filter", "latestVersion.hidden=='false'")
                    .append_pair("sort", "-createTime")
                    .append_pair("page[size]", &VAULT_PAGE_SIZE.to_string())
                    .append_pair("page[number]", &page.to_string())
                    .append_pair("include", MAP_VAULT_INCLUDE);
                Ok(url)
            },
        )
        .await?;

        let mut all_maps = Vec::new();
        for doc in &docs {
            all_maps.extend(parse_vault_maps(doc));
        }
        Ok(all_maps)
    }

    async fn search_vault(&self, query: MapVaultQuery) -> Result<MapSearchPage, String> {
        let token = self
            .tokens
            .get()
            .ok_or_else(|| "not logged in".to_string())?;

        let mut url = url::Url::parse(&format!("{}/data/map", self.config.api_base))
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
                .append_pair("include", MAP_VAULT_INCLUDE);
        }

        let doc = fetch_document(&self.http, url, &token).await?;
        let mut maps = parse_vault_maps(&doc);
        self.raise_ratings(&token, &mut maps).await;
        Ok(MapSearchPage {
            maps,
            total_pages: total_pages(&doc.meta, query.page_size),
            total_records: meta_page_i32(&doc.meta, "totalRecords"),
        })
    }

    async fn find_vault_maps_by_folder(
        &self,
        folder_names: &[String],
    ) -> Result<Vec<VaultMap>, String> {
        let Some(filter) = folder_lookup_filter(folder_names) else {
            return Ok(Vec::new());
        };
        let token = self
            .tokens
            .get()
            .ok_or_else(|| "not logged in".to_string())?;
        let mut url = url::Url::parse(&format!("{}/data/map", self.config.api_base))
            .map_err(|e| format!("invalid API base: {e}"))?;
        url.query_pairs_mut()
            .append_pair("filter", &filter)
            .append_pair("page[size]", &MAX_FOLDER_LOOKUPS.to_string())
            // The same include as the listing, for the same reason (issue 356):
            // without `latestVersion.reviewsSummary` a map looked up by folder
            // carries only whichever summary hangs off the map itself, and
            // reads "N/A" whenever that one is missing.
            .append_pair("include", MAP_VAULT_INCLUDE);
        let doc = fetch_document(&self.http, url, &token).await?;
        let mut maps = parse_vault_maps(&doc);
        self.raise_ratings(&token, &mut maps).await;
        Ok(maps)
    }

    async fn list_installed(&self) -> Result<Vec<InstalledMap>, String> {
        list_installed_dir(&maps_dir()).await
    }

    async fn local_previews(&self, folder_names: &[String]) -> BTreeMap<String, LocalMapPreview> {
        local_previews_in(&maps_dir(), folder_names).await
    }

    async fn list_matchmaker_pools(
        &self,
        queue_name: String,
    ) -> Result<Vec<MatchmakerMapPool>, String> {
        let token = self
            .tokens
            .get()
            .ok_or_else(|| "not logged in".to_string())?;
        let mut url = url::Url::parse(&format!(
            "{}/data/matchmakerQueueMapPool",
            self.config.api_base
        ))
        .map_err(|e| format!("invalid API base: {e}"))?;
        url.query_pairs_mut()
            .append_pair(
                "include",
                "mapPool.mapPoolAssignments.mapVersion.map,matchmakerQueue",
            )
            // Validated, not escaped. Every other vault and replay filter runs
            // through the escapers in `faf_domain::protocol`; this one
            // interpolated a name that arrives over IPC straight into RSQL.
            // A queue's technical name is `ladder_1v1` or `tmm_4v4_full_share`,
            // so a whitelist says more than an escaper would and cannot be
            // widened by accident.
            .append_pair(
                "filter",
                &format!("matchmakerQueue.technicalName=='{}'", {
                    if queue_name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
                        && !queue_name.is_empty()
                    {
                        queue_name.clone()
                    } else {
                        return Err("that is not a matchmaker queue name".to_string());
                    }
                }),
            )
            .append_pair("page[size]", "100");

        let doc = fetch_document(&self.http, url, &token).await?;
        Ok(parse_matchmaker_pools(&doc))
    }

    async fn install_map(
        &self,
        folder_name: String,
        download_url: String,
    ) -> Result<Vec<InstalledMap>, String> {
        safe_map_target(&maps_dir(), &folder_name)?;
        validate_url(&download_url, &self.config.content_base, "maps")?;
        let resp = self
            .http
            .get(&download_url)
            .send()
            .await
            .map_err(|e| format!("could not download map {folder_name}: {e}"))?;
        validate_url(resp.url().as_str(), &self.config.content_base, "maps")?;
        let status = resp.status();
        if !status.is_success() {
            return Err(format!("could not download map {folder_name}: {status}"));
        }
        // To a file, not to a `Vec`: a map is allowed to be half a gigabyte,
        // and the zip reader only ever seeks around the archive. The handle
        // deletes the file when it drops, including on the error paths below.
        let archive = bounded_body_to_file(
            resp,
            &format!("map {folder_name}"),
            MAX_DOWNLOAD_BYTES,
            &|_, _| {},
        )
        .await?;

        let dest = maps_dir();
        tokio::fs::create_dir_all(&dest)
            .await
            .map_err(|e| format!("could not create maps folder: {e}"))?;

        let dest_clone = dest.clone();
        let expected_folder = folder_name.clone();
        let archive_path = archive.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            install_archive_from_file(&archive_path, &dest_clone, Some(&expected_folder), |_| {
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("extraction task panicked: {e}"))??;
        drop(archive);

        list_installed_dir(&dest).await
    }

    async fn uninstall_map(&self, folder_name: String) -> Result<Vec<InstalledMap>, String> {
        let dir = maps_dir();
        let target = safe_map_target(&dir, &folder_name)?;
        if target.exists() {
            tokio::fs::remove_dir_all(&target)
                .await
                .map_err(|e| format!("could not remove {}: {e}", target.display()))?;
        }
        list_installed_dir(&dir).await
    }

    async fn set_map_version_hidden(&self, version_id: i32, hidden: bool) -> Result<(), String> {
        let token = self
            .tokens
            .get()
            .ok_or_else(|| "not logged in".to_string())?;
        let url = url::Url::parse(&format!(
            "{}/data/mapVersion/{version_id}",
            self.config.api_base
        ))
        .map_err(|e| format!("invalid API base: {e}"))?;

        patch_resource(
            &self.http,
            url,
            &token,
            "mapVersion",
            &version_id.to_string(),
            serde_json::json!({ "hidden": hidden }),
        )
        .await
        .map_err(|error| explain_visibility_refusal(&error, hidden))
    }
}

/// Say why the vault refused, when the reason is one the API models as
/// permission rather than wording.
///
/// The asymmetry is worth spelling out: the API's `hidden` field is writable by
/// its owner *only in the direction of `true`* (`MapVersion.isHidden` is guarded
/// by `IsEntityOwner and boolean changed to true`), and putting a version back
/// needs the `ADMIN_MAP` role together with the `manage_vault` OAuth scope,
/// which no FAF client requests. "403" on its own reads like a bug in the
/// client; this reads like the rule it is.
fn explain_visibility_refusal(error: &str, hidden: bool) -> String {
    let refused = error.contains("403") || error.to_lowercase().contains("forbidden");
    if refused && !hidden {
        return "FAF only lets a map administrator put a hidden version back in the vault: \
                an author can withdraw one, but not restore it. Ask a moderator to unhide it."
            .to_string();
    }
    if refused {
        return format!("FAF refused the change: {error}");
    }
    error.to_string()
}

/// Resolve a user-controlled vault folder without allowing it to escape the
/// maps directory. IPC is a trust boundary even when the normal caller is our
/// own webview.
fn safe_map_target(root: &std::path::Path, folder_name: &str) -> Result<PathBuf, String> {
    if !is_safe_folder_name(folder_name) {
        return Err(format!("{folder_name:?} is not a valid map folder name"));
    }
    Ok(root.join(folder_name))
}

/// Scans `dir` for installed map folders: the testable body of
/// [`MapsClient::list_installed`]/post-install rescans.
async fn list_installed_dir(dir: &std::path::Path) -> Result<Vec<InstalledMap>, String> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("could not read {}: {e}", dir.display())),
    };

    let mut installed = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| format!("could not list {}: {e}", dir.display()))?
    {
        let is_dir = entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false);
        if !is_dir {
            continue;
        }
        let folder_path = entry.path();
        let folder_name = entry.file_name().to_string_lossy().to_lowercase();
        if crate::infra::vault_install::is_install_staging_name(&folder_name) {
            continue;
        }

        let scenario_info = find_and_parse_scenario_lua(&folder_path).await;
        let display_name = scenario_info
            .as_ref()
            .and_then(|s| s.name.clone())
            .filter(|n| !n.trim().is_empty())
            .unwrap_or_else(|| display_name_from_folder(&folder_name));

        let version = scenario_info
            .as_ref()
            .and_then(|s| s.version.clone())
            .or_else(|| version_from_folder(&folder_name));

        let (max_players, width, height) = if let Some(ref s) = scenario_info {
            (s.max_players, s.width, s.height)
        } else {
            (0, 0, 0)
        };

        let description = scenario_info.and_then(|s| s.description);
        // The folder's own modification time: an install unpacks into a fresh
        // folder, so this is when the map arrived (#453).
        let installed_at = entry
            .metadata()
            .await
            .and_then(|meta| meta.modified())
            .ok()
            .map(|modified| chrono::DateTime::<chrono::Utc>::from(modified).to_rfc3339());

        installed.push(InstalledMap {
            folder_name,
            display_name,
            max_players,
            width,
            height,
            version,
            description,
            installed_at,
        });
    }
    installed.sort_by(|a, b| a.display_name.cmp(&b.display_name));
    Ok(installed)
}

/// Largest preview file read into a data URL. A vault `.large.png` is around
/// 100 kB; anything past this is not a thumbnail and is not worth base64.
const MAX_PREVIEW_BYTES: u64 = 4 * 1024 * 1024;

/// Ceiling on one request, so a caller cannot ask for the whole maps folder.
const MAX_PREVIEW_REQUEST: usize = 64;

/// A folder name without its `.vNNNN` suffix, lowercased: the key both the
/// installed folders and the co-op missions are matched on.
/// How many folders one lookup names. A lobby list asks for a handful at a
/// time; the cap keeps a pathological page from building an unbounded URL.
const MAX_FOLDER_LOOKUPS: usize = 20;

/// `versions.folderName=in=(...)` over the folders worth asking about, or
/// `None` when none is.
///
/// On `versions` rather than `latestVersion`, and with no `hidden` clause: a
/// lobby can be on any version, and a withdrawn one is exactly what this is
/// for. The Java client's `getMapLatestVersion` asks the same way. A folder
/// name comes off the lobby wire, so anything outside the characters a map
/// folder is made of is dropped rather than escaped into the filter.
fn folder_lookup_filter(folder_names: &[String]) -> Option<String> {
    let mut names: Vec<String> = folder_names
        .iter()
        .map(|name| name.trim().to_lowercase())
        .filter(|name| {
            !name.is_empty()
                && name.len() <= 128
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ' '))
        })
        .collect();
    names.sort();
    names.dedup();
    names.truncate(MAX_FOLDER_LOOKUPS);
    if names.is_empty() {
        return None;
    }
    let quoted: Vec<String> = names.iter().map(|name| format!("\"{name}\"")).collect();
    Some(format!("versions.folderName=in=({})", quoted.join(",")))
}

pub(crate) use faf_domain::state::maps::base_folder_name;

/// Reads preview art out of installed map folders: the testable body of
/// [`MapsClient::local_previews`].
///
/// Matching is by base name and the newest version wins, because the names the
/// callers hold are not the names on disk: a co-op mission knows itself as
/// `scca_coop_a01` while the folder is `scca_coop_a01.v0017`. Within a folder
/// the files are found by suffix rather than built from the folder name, because
/// the two sizes do not agree on a spelling (`SCCA_Coop_A01.small.png` next to
/// `scca_coop_a01.v0017.large.png`).
async fn local_previews_in(
    dir: &std::path::Path,
    folder_names: &[String],
) -> BTreeMap<String, LocalMapPreview> {
    let mut out = BTreeMap::new();
    if folder_names.is_empty() {
        return out;
    }

    // Newest version per base name, in one pass over the maps folder.
    let mut newest: std::collections::HashMap<String, (u32, PathBuf)> =
        std::collections::HashMap::new();
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        // Still record the look, so the UI does not spin on a missing folder.
        for name in folder_names.iter().take(MAX_PREVIEW_REQUEST) {
            out.insert(base_folder_name(name), LocalMapPreview::default());
        }
        return out;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        if !entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let base = base_folder_name(&name);
        let version = version_from_folder(&name.to_lowercase())
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        match newest.get(&base) {
            Some((known, _)) if *known >= version => {}
            _ => {
                newest.insert(base, (version, entry.path()));
            }
        }
    }

    for requested in folder_names.iter().take(MAX_PREVIEW_REQUEST) {
        let base = base_folder_name(requested);
        if out.contains_key(&base) {
            continue;
        }
        // The folder is joined from a name off the wire, so it goes through the
        // same guard as install and uninstall.
        if !is_safe_folder_name(&base) {
            out.insert(base, LocalMapPreview::default());
            continue;
        }
        let mut preview = LocalMapPreview::default();
        if let Some((_, path)) = newest.get(&base) {
            // Both sizes in one go: about four in five folders carry both, and
            // the caller that wants the other one must not have to ask again.
            let (small_path, large_path) = preview_files_in(path).await;
            if let Some(path) = small_path {
                preview.small = read_preview_data_url(&path).await;
            }
            if let Some(path) = large_path {
                preview.large = read_preview_data_url(&path).await;
            }
        }
        out.insert(base, preview);
    }
    out
}

/// The `.small.png` / `.large.png` a map folder carries, if any.
async fn preview_files_in(folder: &std::path::Path) -> (Option<PathBuf>, Option<PathBuf>) {
    let mut small = None;
    let mut large = None;
    let Ok(mut entries) = tokio::fs::read_dir(folder).await else {
        return (None, None);
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_lowercase();
        if small.is_none() && name.ends_with(".small.png") {
            small = Some(entry.path());
        } else if large.is_none() && name.ends_with(".large.png") {
            large = Some(entry.path());
        }
    }
    (small, large)
}

async fn read_preview_data_url(path: &std::path::Path) -> Option<String> {
    use base64::Engine as _;
    let size = tokio::fs::metadata(path).await.ok()?.len();
    if size == 0 || size > MAX_PREVIEW_BYTES {
        return None;
    }
    let bytes = tokio::fs::read(path).await.ok()?;
    Some(format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(&bytes)
    ))
}

async fn find_and_parse_scenario_lua(folder_path: &std::path::Path) -> Option<ScenarioInfo> {
    let mut rd = tokio::fs::read_dir(folder_path).await.ok()?;
    while let Ok(Some(entry)) = rd.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_lowercase();
        if name.ends_with("_scenario.lua") || name == "scenario.lua" {
            if let Ok(contents) = tokio::fs::read_to_string(entry.path()).await {
                return Some(parse_scenario_lua(&contents));
            }
        }
    }
    None
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ScenarioInfo {
    pub name: Option<String>,
    pub width: i32,
    pub height: i32,
    pub max_players: i32,
    pub version: Option<String>,
    pub description: Option<String>,
}

pub(crate) fn parse_scenario_lua(content: &str) -> ScenarioInfo {
    let mut info = ScenarioInfo::default();

    // 1. Name: name = '...' or name = "..."
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("name =") || trimmed.starts_with("name=") {
            if let Some(val) = extract_quoted_string(trimmed) {
                if !val.is_empty() {
                    info.name = Some(val);
                    break;
                }
            }
        }
    }

    // 2. Size: size = { width, height }
    if let Some(size_idx) = content.find("size =").or_else(|| content.find("size=")) {
        let slice = &content[size_idx..];
        if let (Some(start), Some(end)) = (slice.find('{'), slice.find('}')) {
            let inner = &slice[start + 1..end];
            let dims = inner
                .split(',')
                .filter_map(|s| s.trim().parse::<i32>().ok())
                .collect::<Vec<_>>();
            if dims.len() >= 2 {
                info.width = dims[0];
                info.height = dims[1];
            }
        }
    }

    // 3. Map version: map_version = 1 or map_version = '1' or map_version = 1.0
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("map_version =") || trimmed.starts_with("map_version=") {
            let after = if let Some(a) = trimmed.strip_prefix("map_version =") {
                a.trim()
            } else if let Some(a) = trimmed.strip_prefix("map_version=") {
                a.trim()
            } else {
                ""
            };
            let cleaned = after
                .trim_matches(|c: char| c == '\'' || c == '"' || c == ',' || c.is_whitespace());
            if !cleaned.is_empty() {
                info.version = Some(cleaned.to_string());
                break;
            }
        }
    }

    // 4. Max players: the armies the teams list, which is what the Java client
    // counts too (`Configurations.standard.teams[].armies`).
    //
    // Counting every `ARMY_N` token in the file instead picks up the one in
    // `ExtraArmies = STRING( 'ARMY_9 NEUTRAL_CIVILIAN' )`, the civilians a map
    // places, so Seton's Clutch read as nine players (#345); over a hundred of
    // 451 maps in one real folder were off by one that way. It also found none
    // at all on a map whose armies are not called `ARMY_N`. The token scan
    // stays as the fallback for a file without a readable teams block.
    let team_armies = team_army_names(content);
    if !team_armies.is_empty() {
        info.max_players = team_armies.len() as i32;
    } else {
        let mut army_numbers = std::collections::BTreeSet::new();
        for token in content.split(|c: char| !c.is_alphanumeric() && c != '_') {
            if let Some(num_str) = token.strip_prefix("ARMY_") {
                if let Ok(num) = num_str.parse::<u32>() {
                    if (1..=16).contains(&num) {
                        army_numbers.insert(num);
                    }
                }
            }
        }
        if !army_numbers.is_empty() {
            info.max_players = army_numbers.len() as i32;
        }
    }

    // 5. Description
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("description =") || trimmed.starts_with("description=") {
            if let Some(val) = extract_quoted_string(trimmed) {
                let cleaned = if let Some(loc_end) = val.find('>') {
                    if val.starts_with("<LOC") {
                        val[loc_end + 1..].trim().to_string()
                    } else {
                        val
                    }
                } else {
                    val
                };
                if !cleaned.is_empty() {
                    info.description = Some(cleaned);
                    break;
                }
            }
        }
    }

    info
}

/// Every army named in an `armies = { ... }` list of the scenario's teams.
///
/// A set, because a scenario with more than one configuration lists the same
/// armies once per configuration. `ExtraArmies` does not match: the key is
/// case-sensitive and has to stand on its own, not end another word.
fn team_army_names(content: &str) -> std::collections::BTreeSet<String> {
    let mut names = std::collections::BTreeSet::new();
    let mut rest = content;
    while let Some(index) = rest.find("armies") {
        let starts_word = rest[..index]
            .chars()
            .next_back()
            .is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
        let after = &rest[index + "armies".len()..];
        rest = after;
        if !starts_word {
            continue;
        }
        let Some(list) = after.trim_start().strip_prefix('=') else {
            continue;
        };
        let Some(list) = list.trim_start().strip_prefix('{') else {
            continue;
        };
        let Some(end) = list.find('}') else {
            continue;
        };
        for entry in list[..end].split(',') {
            let name = entry
                .trim()
                .trim_matches(|c: char| c == '\'' || c == '"')
                .trim();
            if !name.is_empty() {
                names.insert(name.to_string());
            }
        }
    }
    names
}

fn extract_quoted_string(line: &str) -> Option<String> {
    let single = extract_between(line, '\'');
    if single.is_some() {
        return single;
    }
    extract_between(line, '"')
}

fn extract_between(s: &str, quote: char) -> Option<String> {
    let first = s.find(quote)?;
    let rest = &s[first + 1..];
    let second = rest.find(quote)?;
    Some(rest[..second].to_string())
}

fn version_from_folder(folder_name: &str) -> Option<String> {
    if let Some((_, ver)) = folder_name.rsplit_once(".v") {
        let digits = ver.trim_start_matches('0');
        if digits.is_empty() {
            Some("0".to_string())
        } else {
            Some(digits.to_string())
        }
    } else {
        None
    }
}

/// Mirrors the non-official-map fallback branch of the Python client's
/// `fa.maps.getDisplayName`: strip a trailing `.vNNNN` version suffix
/// (`rsplit(".v0", 1)[0]`), `_` -> space, title-case each word.
fn display_name_from_folder(folder_name: &str) -> String {
    let pretty = match folder_name.rsplit_once(".v0") {
        Some((before, _)) => before,
        None => folder_name,
    };
    let pretty = pretty.replace('_', " ");
    capwords(&pretty)
}

/// Mirrors Python's `string.capwords`: title-case each whitespace-separated word.
fn capwords(s: &str) -> String {
    s.split(' ')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// The user's maps folder: `<Documents>/My Games/Gas Powered Games/Supreme
/// Commander Forged Alliance/maps` (mirrors `util.VAULTS_BASE_DIR` +
/// `fa.maps.getUserMapsFolder()`). `FAF_MAPS_DIR` overrides it (tests,
/// alternate installs, custom vault path).
pub(crate) fn maps_dir() -> PathBuf {
    if let Some(dir) = crate::infra::paths::maps_dir() {
        return dir;
    }
    if let Ok(dir) = std::env::var("FAF_MAPS_DIR") {
        if !dir.is_empty() {
            return PathBuf::from(dir);
        }
    }
    crate::infra::faf_content::vault_dir().join("maps")
}

fn parse_matchmaker_pools(doc: &JsonApiDoc) -> Vec<MatchmakerMapPool> {
    let index = resource_index(&doc.included);
    let mut pools = doc
        .data
        .iter()
        .filter_map(|pool_link| {
            let pool_id = pool_link.id.parse().ok()?;
            let pool_key = rel_target(&pool_link.relationships, "mapPool")?;
            let pool = index.get(&pool_key)?;
            let maps = rel_targets(&pool.relationships, "mapPoolAssignments")
                .into_iter()
                .filter_map(|assignment_key| {
                    let assignment = index.get(&assignment_key)?;
                    let assignment_id = assignment.id.parse().ok()?;

                    if let Some(version_key) = rel_target(&assignment.relationships, "mapVersion") {
                        let version = index.get(&version_key)?;
                        let map_name = rel_target(&version.relationships, "map")
                            .and_then(|key| index.get(&key))
                            .and_then(|map| map.attributes.get("displayName"))
                            .and_then(Value::as_str)
                            .unwrap_or("Unknown map");
                        return Some(MatchmakerPoolMap {
                            assignment_id,
                            display_name: map_name.to_string(),
                            folder_name: version
                                .attributes
                                .get("folderName")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            max_players: value_i32(&version.attributes, "maxPlayers").unwrap_or(0),
                            width: value_i32(&version.attributes, "width").unwrap_or(0),
                            height: value_i32(&version.attributes, "height").unwrap_or(0),
                            thumbnail_url: version
                                .attributes
                                .get("thumbnailUrlSmall")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                        });
                    }

                    let empty_map = serde_json::Map::new();
                    let params_attrs = if let Some(obj) = assignment
                        .attributes
                        .get("mapParams")
                        .and_then(Value::as_object)
                    {
                        obj
                    } else {
                        let params_key = rel_target(&assignment.relationships, "mapParams")?;
                        index
                            .get(&params_key)
                            .map(|p| &p.attributes)
                            .and_then(Value::as_object)
                            .unwrap_or(&empty_map)
                    };

                    let generator_type = params_attrs
                        .get("type")
                        .or_else(|| params_attrs.get("name"))
                        .and_then(Value::as_str)
                        .unwrap_or("Generated map");
                    let version = params_attrs
                        .get("version")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let spawns = params_attrs
                        .get("spawns")
                        .and_then(Value::as_i64)
                        .unwrap_or(0) as i32;
                    let size = params_attrs
                        .get("size")
                        .and_then(Value::as_i64)
                        .unwrap_or(0) as i32;
                    let display_name = if generator_type.to_ascii_lowercase().starts_with("neroxis")
                    {
                        generator_type.to_string()
                    } else {
                        let mut chars = generator_type.chars();
                        let cap = match chars.next() {
                            None => String::new(),
                            Some(first) => {
                                first.to_uppercase().collect::<String>() + chars.as_str()
                            }
                        };
                        format!("Neroxis {cap}")
                    };
                    Some(MatchmakerPoolMap {
                        assignment_id,
                        display_name,
                        folder_name: format!(
                            "neroxis_map_generator_{version}_{generator_type}_{spawns}_{size}"
                        ),
                        max_players: spawns,
                        width: size,
                        height: size,
                        thumbnail_url: GENERATED_MAP_PLACEHOLDER_URL.to_string(),
                    })
                })
                .collect();

            Some(MatchmakerMapPool {
                id: pool_id,
                name: pool
                    .attributes
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("Map pool")
                    .to_string(),
                min_rating: value_i32(&pool_link.attributes, "minRating"),
                max_rating: value_i32(&pool_link.attributes, "maxRating"),
                veto_tokens_per_player: value_i32(&pool_link.attributes, "vetoTokensPerPlayer")
                    .unwrap_or(0),
                max_tokens_per_map: value_i32(&pool_link.attributes, "maxTokensPerMap")
                    .unwrap_or(1),
                minimum_maps_after_veto: value_i32(&pool_link.attributes, "minimumMapsAfterVeto")
                    .unwrap_or(0),
                maps,
            })
        })
        .collect::<Vec<_>>();
    pools.sort_by_key(|pool| pool.min_rating.unwrap_or(i32::MIN));
    pools
}

/// What a vault listing has to bring back with each map.
///
/// `latestVersion.reviewsSummary` for the reason given on `MOD_VAULT_INCLUDE`
/// in `infra::mods`: a review belongs to a map *version*, the summary hangs
/// there, and a relationship that is linked without being included is an id
/// the parser cannot resolve. The maps vault printed "N/A" for the same
/// reason the mods vault did.
const MAP_VAULT_INCLUDE: &str = "latestVersion,latestVersion.reviewsSummary,author,reviewsSummary";

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

fn parse_vault_maps(doc: &JsonApiDoc) -> Vec<VaultMap> {
    let index = resource_index(&doc.included);
    doc.data
        .iter()
        .filter_map(|map_res| {
            let (_, version_id) = rel_target(&map_res.relationships, "latestVersion")?;
            let version = index.get(&("mapVersion".to_string(), version_id))?;

            let author_rel = rel_target(&map_res.relationships, "author");
            // The relationship's own id, so ownership does not depend on the
            // `author` resource having been included: it is in the linkage
            // either way.
            let author_id = author_rel
                .as_ref()
                .and_then(|(_, id)| id.parse::<i32>().ok());
            let author = author_rel
                .and_then(|rel| find_rel_resource(doc, &index, Some(rel)))
                .and_then(|a| a.attributes.get("login"))
                .and_then(Value::as_str)
                .map(str::to_string);
            // Both summaries, not the first one found. A review is written
            // against a version, so that is where the count usually is, while
            // the older entries carry one on the parent as well. Taking the
            // parent's whenever it existed is what printed "N/A" over a
            // version with reviews on it: an empty summary is still a summary,
            // and it won.
            let reviews_summary = [
                rel_target(&map_res.relationships, "reviewsSummary"),
                rel_target(&map_res.relationships, "mapReviewsSummary"),
                rel_target(&version.relationships, "reviewsSummary"),
                rel_target(&version.relationships, "mapVersionReviewsSummary"),
            ]
            .into_iter()
            .flatten()
            .filter_map(|rel| find_rel_resource(doc, &index, Some(rel)))
            .map(parse_reviews_summary)
            // Ties keep the first, which is the map's own: the order above is
            // the order to believe them in when they agree on how many, and a
            // summary hanging off the map counts every version's reviews while
            // the latest version's counts only its own. `max_by_key` keeps the
            // *last* of equal elements, which is the other way round.
            .reduce(|best, next| if next.1 > best.1 { next } else { best });

            let (rating_tenths, reviews) =
                if let Some(summary) = reviews_summary.filter(|(_, reviews)| *reviews > 0) {
                    summary
                } else if let Some(summary_attr) = map_res
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

            Some(VaultMap {
                map_id: map_res.id.parse().unwrap_or_default(),
                version_id: version.id.parse().unwrap_or_default(),
                display_name: map_res
                    .attributes
                    .get("displayName")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown map")
                    .to_string(),
                author,
                author_id,
                folder_name: version
                    .attributes
                    .get("folderName")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                version: value_string(&version.attributes, "version"),
                description: version
                    .attributes
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                map_type: map_res
                    .attributes
                    .get("mapType")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                max_players: version
                    .attributes
                    .get("maxPlayers")
                    .and_then(Value::as_i64)
                    .unwrap_or(0) as i32,
                width: version
                    .attributes
                    .get("width")
                    .and_then(Value::as_i64)
                    .unwrap_or(0) as i32,
                height: version
                    .attributes
                    .get("height")
                    .and_then(Value::as_i64)
                    .unwrap_or(0) as i32,
                games_played: map_res
                    .attributes
                    .get("gamesPlayed")
                    .and_then(Value::as_i64)
                    .unwrap_or(0) as i32,
                version_games_played: version
                    .attributes
                    .get("gamesPlayed")
                    .and_then(Value::as_i64)
                    .unwrap_or(0) as i32,
                ranked: version
                    .attributes
                    .get("ranked")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                hidden: version
                    .attributes
                    .get("hidden")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                recommended: value_bool(&map_res.attributes, "recommended"),
                rating_tenths,
                reviews,
                created_at: version
                    .attributes
                    .get("createTime")
                    .or_else(|| map_res.attributes.get("createTime"))
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
                    .get("thumbnailUrlSmall")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                thumbnail_url_large: version
                    .attributes
                    .get("thumbnailUrlLarge")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            })
        })
        .collect()
}

/// Inert maps client: used offline and in tests (mirrors [`crate::infra::FakeReplay`]).
#[derive(Debug, Clone, Default)]
pub struct FakeMaps;

#[async_trait]
impl MapsPort for FakeMaps {
    async fn list_vault(&self) -> Result<Vec<VaultMap>, String> {
        Err("map vault is unavailable in offline mode".to_string())
    }

    async fn search_vault(&self, _query: MapVaultQuery) -> Result<MapSearchPage, String> {
        Err("map vault is unavailable in offline mode".to_string())
    }

    async fn list_installed(&self) -> Result<Vec<InstalledMap>, String> {
        Err("map install listing is unavailable in offline mode".to_string())
    }

    async fn list_matchmaker_pools(
        &self,
        _queue_name: String,
    ) -> Result<Vec<MatchmakerMapPool>, String> {
        Ok(vec![MatchmakerMapPool {
            id: 1,
            name: "Offline sample pool".into(),
            min_rating: None,
            max_rating: None,
            veto_tokens_per_player: 2,
            max_tokens_per_map: 1,
            minimum_maps_after_veto: 1,
            maps: vec![MatchmakerPoolMap {
                assignment_id: 1,
                display_name: "Open Palms".into(),
                folder_name: "open_palms".into(),
                max_players: 4,
                width: 512,
                height: 512,
                thumbnail_url: String::new(),
            }],
        }])
    }

    async fn install_map(
        &self,
        _folder_name: String,
        _download_url: String,
    ) -> Result<Vec<InstalledMap>, String> {
        Err("map install is unavailable in offline mode".to_string())
    }

    async fn uninstall_map(&self, _folder_name: String) -> Result<Vec<InstalledMap>, String> {
        Err("map uninstall is unavailable in offline mode".to_string())
    }

    async fn set_map_version_hidden(&self, _version_id: i32, _hidden: bool) -> Result<(), String> {
        Err("the map vault is unavailable in offline mode".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn derives_display_name_from_folder() {
        assert_eq!(display_name_from_folder("scmp_009.v0001"), "Scmp 009");
        assert_eq!(
            display_name_from_folder("setons_clutch.v0025"),
            "Setons Clutch"
        );
        assert_eq!(
            display_name_from_folder("no_version_suffix"),
            "No Version Suffix"
        );
    }

    #[tokio::test]
    async fn list_installed_dir_lists_subfolders_lowercased_and_sorted() {
        let dir = std::env::temp_dir().join(format!("forge-maps-test-{}", std::process::id()));
        tokio::fs::create_dir_all(dir.join("Scmp_009.v0001"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.join("adaptive_map.v0002"))
            .await
            .unwrap();
        tokio::fs::write(dir.join("not_a_dir.txt"), b"x")
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.join(".faf-install-0123456789abcdef"))
            .await
            .unwrap();

        let installed = list_installed_dir(&dir).await.expect("should list");
        assert_eq!(installed.len(), 2);
        let folders: Vec<_> = installed.iter().map(|m| m.folder_name.clone()).collect();
        assert!(folders.contains(&"scmp_009.v0001".to_string()));
        assert!(folders.contains(&"adaptive_map.v0002".to_string()));

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn list_installed_dir_missing_folder_returns_empty() {
        let dir = std::env::temp_dir().join("forge-maps-does-not-exist");
        let installed = list_installed_dir(&dir)
            .await
            .expect("missing dir is not an error");
        assert!(installed.is_empty());
    }

    #[test]
    fn folder_lookups_ask_every_version_and_refuse_filter_syntax() {
        assert_eq!(
            folder_lookup_filter(&[
                "Waters_Of_Isis.v0003".into(),
                "waters_of_isis.v0003".into(),
                "x\"),hidden==false;(a".into(),
                "  ".into(),
            ])
            .as_deref(),
            Some(r#"versions.folderName=in=("waters_of_isis.v0003")"#)
        );
        assert_eq!(folder_lookup_filter(&["a\"b".into()]), None);
    }

    #[tokio::test]
    async fn local_previews_finds_a_versioned_folder_from_an_unversioned_name() {
        // What the co-op catalogue hands over is the mission's folder without a
        // version; what is on disk carries one. And the two preview files do
        // not agree on a spelling, which is why they are found by suffix.
        let dir = std::env::temp_dir().join(format!("forge-previews-test-{}", std::process::id()));
        let map = dir.join("scca_coop_a01.v0017");
        tokio::fs::create_dir_all(&map).await.unwrap();
        tokio::fs::write(map.join("SCCA_Coop_A01.small.png"), b"small-bytes")
            .await
            .unwrap();
        tokio::fs::write(map.join("scca_coop_a01.v0017.large.png"), b"large-bytes")
            .await
            .unwrap();

        let previews = local_previews_in(&dir, &["scca_coop_a01".to_string()]).await;

        let preview = previews.get("scca_coop_a01").expect("the folder was found");
        assert_eq!(
            preview.small.as_deref(),
            Some("data:image/png;base64,c21hbGwtYnl0ZXM=")
        );
        assert_eq!(
            preview.large.as_deref(),
            Some("data:image/png;base64,bGFyZ2UtYnl0ZXM=")
        );

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn local_previews_records_a_fruitless_look() {
        // The UI asks from an image error handler, so "nothing there" has to be
        // an answer it can remember. An absent key would mean asking forever.
        let dir = std::env::temp_dir().join(format!("forge-previews-empty-{}", std::process::id()));
        tokio::fs::create_dir_all(dir.join("plain_map.v0001"))
            .await
            .unwrap();

        let previews = local_previews_in(
            &dir,
            &["plain_map".to_string(), "not_installed".to_string()],
        )
        .await;

        assert!(previews["plain_map"].is_empty());
        assert!(previews["not_installed"].is_empty());

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[test]
    fn parses_vault_maps_resolving_version_and_author_through_included() {
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [
                {
                    "type": "map",
                    "id": "77",
                    "attributes": {
                        "displayName": "Seton's Clutch",
                        "gamesPlayed": 12345,
                        "mapType": "skirmish",
                        "recommended": true,
                        "createTime": "2020-01-02T03:04:05Z"
                    },
                    "relationships": {
                        "latestVersion": { "data": { "type": "mapVersion", "id": "9" } },
                        "author": { "data": { "type": "player", "id": "1" } },
                        "reviewsSummary": { "data": { "type": "reviewsSummary", "id": "15" } },
                    },
                },
            ],
            "included": [
                {
                    "type": "mapVersion",
                    "id": "9",
                    "attributes": {
                        "folderName": "scmp_009.v0001",
                        "version": 7,
                        "description": "A classic team map.",
                        "gamesPlayed": 12000,
                        "maxPlayers": 8,
                        "width": 1024,
                        "height": 1024,
                        "ranked": true,
                        "downloadUrl": "https://content.faforever.com/maps/scmp_009.zip",
                        "thumbnailUrlSmall": "https://content.faforever.com/maps/scmp_009.small.png",
                        "thumbnailUrlLarge": "https://content.faforever.com/maps/scmp_009.large.png",
                        "createTime": "2021-02-03T04:05:06Z"
                    },
                },
                {
                    "type": "player",
                    "id": "1",
                    "attributes": { "login": "Rackover" },
                },
                {
                    "type": "reviewsSummary",
                    "id": "15",
                    "attributes": { "averageScore": 4.34, "reviews": 27 },
                },
            ],
        }))
        .unwrap();

        let maps = parse_vault_maps(&doc);
        assert_eq!(maps.len(), 1);
        assert_eq!(maps[0].display_name, "Seton's Clutch");
        assert_eq!(maps[0].map_id, 77);
        assert_eq!(maps[0].version_id, 9);
        assert_eq!(maps[0].folder_name, "scmp_009.v0001");
        assert_eq!(maps[0].version, "7");
        assert_eq!(maps[0].description, "A classic team map.");
        assert_eq!(maps[0].map_type, "skirmish");
        assert_eq!(maps[0].author, Some("Rackover".to_string()));
        assert_eq!(
            maps[0].author_id,
            Some(1),
            "ownership is decided by the author's id, not their current login"
        );
        assert!(
            !maps[0].hidden,
            "absent means visible: the vault only sends `hidden` as true for a withdrawn version"
        );
        assert_eq!(maps[0].max_players, 8);
        assert_eq!(maps[0].games_played, 12345);
        assert_eq!(maps[0].version_games_played, 12000);
        assert!(maps[0].ranked);
        assert!(maps[0].recommended);
        assert_eq!(maps[0].rating_tenths, 43);
        assert_eq!(maps[0].reviews, 27);
        assert_eq!(maps[0].created_at, "2021-02-03T04:05:06Z");
        assert!(maps[0].thumbnail_url_large.ends_with("large.png"));
    }

    #[test]
    fn a_withdrawn_version_is_read_as_hidden() {
        // Only "my maps" ever sees one of these: every other search filters
        // hidden versions out server side.
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [{
                "type": "map",
                "id": "77",
                "attributes": { "displayName": "Withdrawn" },
                "relationships": {
                    "latestVersion": { "data": { "type": "mapVersion", "id": "9" } },
                    "author": { "data": { "type": "player", "id": "4711" } },
                },
            }],
            "included": [{
                "type": "mapVersion",
                "id": "9",
                "attributes": { "folderName": "withdrawn.v0001", "hidden": true },
            }],
        }))
        .unwrap();

        let maps = parse_vault_maps(&doc);
        assert!(maps[0].hidden);
        // The author resource was not included, and the id still arrives: it is
        // in the relationship linkage rather than the included document.
        assert_eq!(maps[0].author_id, Some(4711));
        assert_eq!(maps[0].author, None);
    }

    #[test]
    fn a_refused_unhide_says_who_can_do_it_instead_of_showing_a_status() {
        // FAF guards `hidden` asymmetrically: its owner may set it to `true`,
        // and only ADMIN_MAP may set it back. "403" alone reads like our bug.
        let refusal =
            explain_visibility_refusal("/data/mapVersion/9 returned 403 Forbidden", false);
        assert!(refusal.contains("map administrator"), "{refusal}");
        assert!(!refusal.contains("403"), "{refusal}");

        // Hiding is the author's own right, so a refusal there is unexpected
        // and the server's own words are worth keeping.
        let hiding = explain_visibility_refusal("/data/mapVersion/9 returned 403 Forbidden", true);
        assert!(hiding.contains("403"), "{hiding}");

        // Anything that is not a refusal passes through untouched.
        let offline = explain_visibility_refusal("request failed: dns error", false);
        assert_eq!(offline, "request failed: dns error");
    }

    #[test]
    fn parse_vault_maps_skips_entries_missing_latest_version() {
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [{ "type": "map", "id": "1", "attributes": {}, "relationships": {} }],
        }))
        .unwrap();
        assert!(parse_vault_maps(&doc).is_empty());
    }

    #[test]
    fn uninstall_target_must_stay_inside_the_maps_directory() {
        let root = PathBuf::from("maps-root");
        assert_eq!(
            safe_map_target(&root, "open_palms.v0001").unwrap(),
            root.join("open_palms.v0001")
        );
        for name in ["..", "../outside", "nested/map", "C:\\Windows"] {
            assert!(safe_map_target(&root, name).is_err(), "accepted {name:?}");
        }
    }

    #[test]
    fn parses_matchmaker_pool_assignments_and_veto_limits() {
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [{
                "type": "matchmakerQueueMapPool",
                "id": "12",
                "attributes": {
                    "minRating": 800,
                    "maxRating": 1600,
                    "vetoTokensPerPlayer": 3,
                    "maxTokensPerMap": 2,
                    "minimumMapsAfterVeto": 5
                },
                "relationships": { "mapPool": { "data": { "type": "mapPool", "id": "4" } } }
            }],
            "included": [
                {
                    "type": "mapPool",
                    "id": "4",
                    "attributes": { "name": "Standard pool" },
                    "relationships": { "mapPoolAssignments": { "data": [{ "type": "mapPoolAssignment", "id": "91" }] } }
                },
                {
                    "type": "mapPoolAssignment",
                    "id": "91",
                    "relationships": { "mapVersion": { "data": { "type": "mapVersion", "id": "9" } } }
                },
                {
                    "type": "mapVersion",
                    "id": "9",
                    "attributes": { "folderName": "open_palms.v0001", "maxPlayers": 4, "width": 512, "height": 512, "thumbnailUrlSmall": "https://example.test/map.png" },
                    "relationships": { "map": { "data": { "type": "map", "id": "7" } } }
                },
                { "type": "map", "id": "7", "attributes": { "displayName": "Open Palms" } }
            ]
        }))
        .unwrap();

        let pools = parse_matchmaker_pools(&doc);
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].id, 12);
        assert_eq!(pools[0].veto_tokens_per_player, 3);
        assert_eq!(pools[0].maps[0].assignment_id, 91);
        assert_eq!(pools[0].maps[0].display_name, "Open Palms");
    }

    #[test]
    fn parses_matchmaker_pool_with_generated_maps() {
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [{
                "type": "matchmakerQueueMapPool",
                "id": "13",
                "attributes": {
                    "minRating": 0,
                    "maxRating": 500,
                    "vetoTokensPerPlayer": 1,
                    "maxTokensPerMap": 1,
                    "minimumMapsAfterVeto": 1
                },
                "relationships": { "mapPool": { "data": { "type": "mapPool", "id": "5" } } }
            }],
            "included": [
                {
                    "type": "mapPool",
                    "id": "5",
                    "attributes": { "name": "Generated 3v3 pool" },
                    "relationships": { "mapPoolAssignments": { "data": [{ "type": "mapPoolAssignment", "id": "105" }] } }
                },
                {
                    "type": "mapPoolAssignment",
                    "id": "105",
                    "relationships": { "mapParams": { "data": { "type": "mapParams", "id": "22" } } }
                },
                {
                    "type": "mapParams",
                    "id": "22",
                    "attributes": {
                        "type": "casual",
                        "version": "1.22.1",
                        "spawns": 6,
                        "size": 512
                    }
                }
            ]
        }))
        .unwrap();

        let pools = parse_matchmaker_pools(&doc);
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].id, 13);
        assert_eq!(pools[0].maps.len(), 1);
        assert_eq!(pools[0].maps[0].assignment_id, 105);
        assert_eq!(pools[0].maps[0].display_name, "Neroxis Casual");
        assert_eq!(
            pools[0].maps[0].folder_name,
            "neroxis_map_generator_1.22.1_casual_6_512"
        );
        assert_eq!(pools[0].maps[0].height, 512);
        assert_eq!(
            pools[0].maps[0].thumbnail_url,
            GENERATED_MAP_PLACEHOLDER_URL
        );
    }

    #[test]
    fn parses_matchmaker_pool_with_embedded_map_params_attribute() {
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [{
                "type": "matchmakerQueueMapPool",
                "id": "14",
                "attributes": {
                    "minRating": 0,
                    "maxRating": 500,
                    "vetoTokensPerPlayer": 1,
                    "maxTokensPerMap": 1,
                    "minimumMapsAfterVeto": 1
                },
                "relationships": { "mapPool": { "data": { "type": "mapPool", "id": "6" } } }
            }],
            "included": [
                {
                    "type": "mapPool",
                    "id": "6",
                    "attributes": { "name": "Generated 2v2 pool" },
                    "relationships": { "mapPoolAssignments": { "data": [{ "type": "mapPoolAssignment", "id": "106" }] } }
                },
                {
                    "type": "mapPoolAssignment",
                    "id": "106",
                    "attributes": {
                        "weight": 1,
                        "mapParams": {
                            "type": "blind",
                            "version": "1.21.2",
                            "spawns": 4,
                            "size": 512
                        }
                    },
                    "relationships": {}
                }
            ]
        }))
        .unwrap();

        let pools = parse_matchmaker_pools(&doc);
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].id, 14);
        assert_eq!(pools[0].maps.len(), 1);
        assert_eq!(pools[0].maps[0].assignment_id, 106);
        assert_eq!(pools[0].maps[0].display_name, "Neroxis Blind");
        assert_eq!(
            pools[0].maps[0].folder_name,
            "neroxis_map_generator_1.21.2_blind_4_512"
        );
        assert_eq!(pools[0].maps[0].max_players, 4);
        assert_eq!(pools[0].maps[0].width, 512);
        assert_eq!(pools[0].maps[0].height, 512);
        assert_eq!(
            pools[0].maps[0].thumbnail_url,
            GENERATED_MAP_PLACEHOLDER_URL
        );
    }

    #[test]
    fn parses_scenario_lua_fields() {
        let content = r#"
ScenarioInfo = {
    name = 'Adaptive Metir',
    description = '<LOC map_adaptive_metir_desc>A balanced battleground for 4 players.',
    type = 'skirmish',
    starts = true,
    size = {512, 512},
    map_version = 1,
    map = '/maps/adaptive_metir.v0001/adaptive_metir.scmap',
    Configurations = {
        ['standard'] = {
            ['teams'] = {
                { name = 'FFA', armies = {'ARMY_1','ARMY_2','ARMY_3','ARMY_4'} },
            },
        },
    }
}
"#;
        let info = parse_scenario_lua(content);
        assert_eq!(info.name.as_deref(), Some("Adaptive Metir"));
        assert_eq!(info.width, 512);
        assert_eq!(info.height, 512);
        assert_eq!(info.max_players, 4);
        assert_eq!(info.version.as_deref(), Some("1"));
        assert_eq!(
            info.description.as_deref(),
            Some("A balanced battleground for 4 players.")
        );
    }

    #[test]
    fn scenario_players_leave_out_the_civilian_extra_army() {
        // Seton's Clutch as it ships: eight players and the civilians as a
        // ninth army that nobody can take a seat in.
        let content = r#"
    Configurations = {
        ['standard'] = {
            teams = {
                { name = 'FFA', armies = {'ARMY_1','ARMY_2','ARMY_3','ARMY_4','ARMY_5','ARMY_6','ARMY_7','ARMY_8',} },
            },
            customprops = {
                ['ExtraArmies'] = STRING( 'ARMY_9 NEUTRAL_CIVILIAN' ),
            },
        },
    }
"#;
        assert_eq!(parse_scenario_lua(content).max_players, 8);
    }

    #[test]
    fn scenario_players_count_armies_not_called_army_n() {
        let content = r#"
    Configurations = {
        ['standard'] = {
            teams = {
                { name = 'FFA', armies = { "NORTH", "SOUTH" } },
            },
        },
    }
"#;
        assert_eq!(parse_scenario_lua(content).max_players, 2);
    }

    #[tokio::test]
    async fn fake_maps_fails_cleanly() {
        let fake = FakeMaps;
        assert!(fake.list_vault().await.is_err());
        assert!(fake.list_installed().await.is_err());
        assert_eq!(
            fake.list_matchmaker_pools("ladder1v1".into())
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(fake
            .install_map("x".into(), "http://x".into())
            .await
            .is_err());
        assert!(fake.uninstall_map("x".into()).await.is_err());
    }
}
