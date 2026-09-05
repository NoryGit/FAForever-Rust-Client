//! Real replay client: live spectating and local `.fafreplay` playback.
//!
//! Mirrors the Python client's `fa/replaylivestreamer.py`/`fa/replay.py`.
//!
//! ## Live-watch protocol
//! 1. `GET {user_api}/replay/access` (bearer token) → `{ accessUrl }` (same
//!    endpoint and response shape as the lobby's `/lobby/access`, see
//!    `infra::fetch_access_url`).
//! 2. Open a WebSocket to `accessUrl`; send one binary frame
//!    `G/{uid}/{player}.scfareplay\0` (the player name is irrelevant to the
//!    server, which merges every participant's stream: any non-empty string
//!    works, per the Python client's own comment).
//! 3. Wait for the first binary frame back before launching FA: otherwise FA
//!    connects to an empty stream (same ordering as the Python client).
//! 4. Bring the replay install up to the current build of the game's featured
//!    mod. A live stream carries no version of its own, and FA refuses a
//!    replay whose build does not match what is installed ("Ack! Unable to
//!    load replay from gpgnet"), so this asks for `latest` the way a live
//!    game launch does. It runs *before* the stream is opened: the server
//!    starts sending on the handshake, and a patch download after that point
//!    would be spent buffering frames nobody is reading.
//! 5. Launch FA with `/replay gpgnet://127.0.0.1:<port>/...` pointed at a
//!    local TCP proxy (mirrors the Java client's `LiveReplayProxyServer`).
//! 6. Once FA connects, feed it WebSocket frames until the connection closes.
//!
//! **Transport: a local TCP proxy by default; a named pipe only when the user
//! asks for it.** FA's engine has a confirmed bug: reading a replay's
//! `ScenarioInfo` (game options, often bloated by unused sim-mod options)
//! through a raw TCP/`gpgnet://` socket crashes it with "Premature EOF" once
//! that data exceeds roughly 2047 bytes (FAF Discord/Zulip, Gatsik, Jan 2026).
//! A Windows named pipe avoids that crash: we shipped that fix briefly: but it
//! introduced a worse regression: FA's named-pipe read is a *blocking* call on
//! (what appears to be) its main/render thread, so catching up to a live
//! game's current tick freezes the entire UI rather than just stalling the
//! simulation the way the TCP path does (confirmed both by community reports
//!: Nomander/Nuggets, FAF Discord, Jan 2026: and by reproducing it
//! ourselves). It also ends the replay abruptly, with no army selection or
//! post-game statistics, because FA never sees the stream close (see
//! [`PIPE_TERMINATOR`]). The actual fix for the root cause is upstream, in the
//! FA engine's lobby code (`FAForever/fa#7057`, stripping disabled mods' game
//! options before launch so `ScenarioInfo` never gets oversized in the first
//! place); once that lands, this TCP transport stops hitting the crash for
//! new games entirely, with none of the pipe's freeze downside.
//!
//! Neither transport is right for everyone until then, which is why the pipe
//! is a switch rather than a decision made here: Settings, "Live replays
//! workaround" (`GamePreferences::pipe_live_replay`, the Python client's
//! `game/pipe_live_replay`), off by default. A player whose games crash out of
//! live spectating turns it on and trades the ending for one that starts.
//!
//! Old replays recorded before the upstream fix may still have oversized
//! `ScenarioInfo` and could in principle still hit this crash live-spectating
//! them: but local *file* playback (see below) was never on this code path to
//! begin with: FA reads those directly from disk via `/replay "<path>"`, with
//! no TCP or pipe involved and no size limit or freeze concern either way.
//!
//! ## File playback
//! A `.fafreplay` is a JSON header line + `\n` + a compressed `.scfareplay`
//! sim-command stream. Two body formats exist, both mirrored from the Python
//! client's `uncompress()`: `compression: "zstd"` for vault-downloaded
//! replays, and everything else (in practice `null`: the format
//! `%ProgramData%\FAForever\replays` locally-recorded replays actually use)
//! falls back to base64 wrapping Qt's `qCompress` container (4-byte
//! big-endian length + raw zlib). The decompressed body is written to the
//! cache dir and FA is launched with `/replay "<path>"`.

use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash as _, Hasher as _};
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use faf_domain::protocol::map_generator::is_generated_map;
use faf_domain::protocol::replay_query;
use faf_domain::state::{
    LiveReplayTarget, LocalReplay, LocalReplayPlayer, LocalReplayStatus, LocalReplayTeam, ModType,
    ReplayChatMessage, ReplayDetails, ReplayGameOption, ReplayPlayer, ReplayQuery, ReplayTeam,
    VaultReplay,
};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::infra::jsonapi::{
    fetch_document, meta_page_i32, rel_target, rel_targets, resource_index, total_pages, value_i32,
    JsonApiDoc, JsonApiResource,
};
use crate::infra::session::TokenStore;
use crate::infra::vault_install::{bounded_body, validate_origin_url, MAX_DOWNLOAD_BYTES};
use crate::infra::{
    cache_dir, env_or, fetch_access_url, free_port, game_updater, validated_ws_url,
    GENERATED_MAP_PLACEHOLDER_URL,
};
use crate::ports::replay::VaultSearchResult;
use crate::ports::{ProcessPort, ReplayPort};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// How long to wait for the replay server's first byte, or for FA to connect
/// to the local TCP proxy, before giving up.
const READY_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_REPLAY_DOWNLOAD_REDIRECTS: usize = 5;

/// The pipe FA is pointed at when the "Live Replays Workaround" is on. Fixed
/// rather than per-game, and the same name the Python client uses, so the two
/// clients cannot end up streaming into each other's pipe: only one live
/// replay can run at a time either way (`playback_lock`).
#[cfg(windows)]
const LIVE_REPLAY_PIPE_PATH: &str = r"\\.\pipe\fafreplay";

/// How a live replay stream reaches FA.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveTransport {
    /// A local TCP proxy behind a `gpgnet://` URL. The default: FA ends the
    /// replay cleanly and the spectator keeps army selection and statistics.
    Tcp,
    /// A Windows named pipe. Sidesteps the engine's oversized-`ScenarioInfo`
    /// bug on the TCP path at the cost of a frozen window while catching up
    /// and an abrupt end. See the module docs.
    #[cfg(windows)]
    Pipe,
}

/// FA does not notice a named pipe closing the way it notices a socket close:
/// it simply hangs. The Python client found that a couple of kilobytes of
/// invalid replay data ends the session instead, and this is the same
/// terminator, byte for byte (`fa/replaylivestreamer.py`). The simulation
/// still stops rather than ending properly, which is the "abrupt end" the
/// setting's hint warns about.
#[cfg(windows)]
const PIPE_TERMINATOR: [u8; 2048] = [0u8; 2048];

/// The relationships a vault row needs to render: map, mod, the roster with
/// avatars, the rating changes and the review summary.
const VAULT_INCLUDE: &str = "mapVersion,mapVersion.map,featuredMod,playerStats.player,playerStats.player.avatarAssignments.avatar,playerStats.ratingChanges,reviewsSummary";

/// How many ids one page of a shared-games scan asks for. Half the API's 10 000
/// ceiling (`elide.max-page-size`): every page is now a bounded index range, so
/// the cost is in the round trips, and a decade of games is four requests
/// instead of twenty.
const ID_SCAN_PAGE_SIZE: u32 = 5000;

/// The point at which a shared-games scan stops and says so. Roughly twice the
/// game count of the busiest account on the server, so it exists to bound a
/// filter that went wrong (a one-letter substring matching thousands of
/// players), not to trim a real search.
const ID_SCAN_CAP: usize = 100_000;

/// How many accounts one shared-games search will look up. A substring name can
/// legitimately match a handful of logins; a two-letter one matches thousands,
/// and scanning all of their histories is not what the user meant.
const MAX_RESOLVED_PLAYERS: u32 = 200;

/// How large an intersection can be and still be reordered by a sort the scans
/// did not fetch. One request's worth: beyond it the result stays newest-first,
/// which is what the scans produce anyway.
const MAX_SORTABLE_SHARED_GAMES: usize = 1000;

#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// FAF *user* API base, which serves `/replay/access` (same host as the
    /// lobby's `/lobby/access`, see `LobbyConfig::user_api_base`).
    pub user_api_base: String,
    /// FAF Data API base, which serves `/data/game` (vault listing). Bearer-
    /// token authenticated, unlike the vault download host below.
    pub api_base: String,
    /// Vault replay-file host: `GET {vault_host}/{uid}` downloads a
    /// `.fafreplay` unauthenticated (mirrors the Python client's
    /// `replay_vault/host` setting).
    pub vault_host: String,
    /// Root of the replay game install the version updater targets: two
    /// directories up from `FAF_REPLAY_GAME_PATH` (…/replaydata/bin/FA.exe →
    /// …/replaydata). `None` if `FAF_REPLAY_GAME_PATH` isn't set; version
    /// updates are then skipped (replay launch still proceeds, matching the
    /// existing "the exe path just isn't configured" posture elsewhere).
    pub replay_target_dir: Option<PathBuf>,
    /// The FA executable's filename within the `bin` group, e.g.
    /// `ForgedAlliance.exe`: the file the version updater hex-patches.
    pub exe_name: String,
    /// Public content CDN: `GET {content_base}/maps/{name}.zip` downloads a
    /// map, unauthenticated (mirrors the Python client's `content/host` /
    /// `vault/map_download_url` settings).
    pub content_base: String,
}

impl ReplayConfig {
    pub fn faf() -> Self {
        Self {
            user_api_base: env_or("FAF_USER_API_BASE", "https://user.faforever.com"),
            api_base: env_or("FAF_API_BASE", "https://api.faforever.com"),
            vault_host: env_or("FAF_REPLAY_VAULT_BASE", "https://replay.faforever.com"),
            replay_target_dir: replay_target_dir_from_env(),
            exe_name: env_or("FAF_GAME_EXE_NAME", "ForgedAlliance.exe"),
            content_base: env_or("FAF_CONTENT_BASE", "https://content.faforever.com"),
        }
    }
}

/// Accept only the three replay routes used by FAF's current redirect chain:
/// the public replay endpoint, the Data API handoff, and the content CDN file.
/// All bases remain configurable for local and test environments.
fn validate_replay_download_url(
    url: &url::Url,
    uid: i32,
    config: &ReplayConfig,
) -> Result<(), String> {
    let uid = uid.to_string();
    let vault_path = format!("/{}", uid);
    let api_path = format!("/game/{uid}/replay");
    let content_file = format!("{uid}.fafreplay");

    let vault = matches_configured_route(url, &config.vault_host, &vault_path, false);
    let api = matches_configured_route(url, &config.api_base, &api_path, false);
    let content = matches_configured_route(
        url,
        &config.content_base,
        &format!("/replays/{content_file}"),
        true,
    );
    if vault || api || content {
        Ok(())
    } else {
        Err("refusing a replay download outside the configured FAF replay endpoints".into())
    }
}

fn matches_configured_route(
    url: &url::Url,
    configured_base: &str,
    route: &str,
    allow_sharded_path: bool,
) -> bool {
    if validate_origin_url(url.as_str(), configured_base).is_err() {
        return false;
    }
    let Ok(base) = url::Url::parse(configured_base) else {
        return false;
    };
    let base_path = base.path().trim_end_matches('/');
    let expected = format!("{base_path}{route}");
    if !allow_sharded_path {
        return url.path() == expected;
    }

    let Some((prefix, file)) = expected.rsplit_once('/') else {
        return false;
    };
    url.path()
        .strip_prefix(&format!("{prefix}/"))
        .is_some_and(|remainder| {
            !remainder.is_empty()
                && remainder.split('/').all(|component| !component.is_empty())
                && remainder.rsplit('/').next() == Some(file)
        })
}

/// Derives the version updater's target directory from `FAF_REPLAY_GAME_PATH`
/// (…/replaydata/bin/ForgedAlliance.exe → …/replaydata): two `parent()`
/// calls up from the exe. `FAF_REPLAY_UPDATE_DIR` overrides it directly.
fn replay_target_dir_from_env() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("FAF_REPLAY_UPDATE_DIR") {
        if !dir.is_empty() {
            return Some(PathBuf::from(dir));
        }
    }
    let exe = std::env::var("FAF_REPLAY_GAME_PATH").ok()?;
    if exe.is_empty() {
        return None;
    }
    PathBuf::from(exe).parent()?.parent().map(PathBuf::from)
}

pub struct ReplayClient {
    config: ReplayConfig,
    tokens: TokenStore,
    http: reqwest::Client,
    /// Replay downloads currently redirect across three FAF origins. This
    /// client does not follow automatically, so each hop can be validated
    /// before the next request is made.
    download_http: reqwest::Client,
    process: Arc<dyn ProcessPort>,
    /// The replay install every preparation step targets, behind a lock because
    /// Settings can repoint it at runtime (`ReplayPort::set_install_dir`).
    /// Seeded from the environment so a launch before the first settings sync
    /// still works, then overwritten by the configured path.
    install_dir: std::sync::Mutex<Option<PathBuf>>,
    /// Playback preparation writes shared cache and preference files. Keep one
    /// launch pipeline active per client so concurrent UI commands cannot race.
    playback_lock: Mutex<()>,
    /// The last shared-games intersection and the search that produced it, so
    /// turning a page does not scan every player's history again. Keyed by the
    /// query with its paging normalised away: page and page size change what is
    /// shown, not which games matched.
    ///
    /// It is a snapshot, deliberately: a game played while the user pages
    /// through the results appears on their next search, not underneath them.
    shared_games: std::sync::Mutex<Option<(ReplayQuery, Vec<i32>)>>,
    /// Whether a live replay stream is handed to FA over a named pipe instead
    /// of the local TCP proxy: the "Live Replays Workaround" setting, pushed in
    /// by the settings service (`ReplayPort::set_live_replay_pipe`).
    pipe_live_replay: std::sync::atomic::AtomicBool,
    /// Rebuilds a replay's map when it was made by the Neroxis generator.
    /// Generated maps exist in no vault, so staging cannot find them and the
    /// only way to put one on disk is to run the generator again.
    map_generator: Arc<dyn crate::ports::MapGeneratorPort>,
    /// The `auto_generate_maps` setting, pushed in the same way as
    /// [`Self::pipe_live_replay`]. Starts enabled to match the setting's own
    /// default.
    auto_generate_maps: std::sync::atomic::AtomicBool,
}

/// The cache key for a shared-games search: everything except which slice of
/// the answer is being displayed.
fn shared_games_key(query: &ReplayQuery) -> ReplayQuery {
    ReplayQuery {
        page: 1,
        page_size: 0,
        ..query.clone()
    }
}

impl ReplayClient {
    pub fn new(
        config: ReplayConfig,
        tokens: TokenStore,
        process: Arc<dyn ProcessPort>,
        map_generator: Arc<dyn crate::ports::MapGeneratorPort>,
    ) -> Self {
        Self {
            install_dir: std::sync::Mutex::new(config.replay_target_dir.clone()),
            shared_games: std::sync::Mutex::new(None),
            config,
            tokens,
            http: super::http::shared_http_client(),
            download_http: super::http::no_redirect_http_client(),
            process,
            playback_lock: Mutex::new(()),
            pipe_live_replay: std::sync::atomic::AtomicBool::new(false),
            map_generator,
            auto_generate_maps: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Where the engine update and map staging go for the next launch.
    fn install_dir(&self) -> Option<PathBuf> {
        self.install_dir.lock().unwrap().clone()
    }

    /// Which transport the next live replay uses, resolving the preference
    /// against what this platform can actually do.
    fn live_transport(&self) -> LiveTransport {
        if !self
            .pipe_live_replay
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return LiveTransport::Tcp;
        }
        #[cfg(windows)]
        {
            LiveTransport::Pipe
        }
        #[cfg(not(windows))]
        {
            // The Python client has a FIFO path for this; we do not, and
            // quietly ignoring the setting would be worse than saying so.
            tracing::warn!(
                "the live replay workaround uses a Windows named pipe; \
                 falling back to the TCP proxy on this platform"
            );
            LiveTransport::Tcp
        }
    }

    pub fn faf(
        tokens: TokenStore,
        process: Arc<dyn ProcessPort>,
        map_generator: Arc<dyn crate::ports::MapGeneratorPort>,
    ) -> Self {
        Self::new(ReplayConfig::faf(), tokens, process, map_generator)
    }

    /// Put `map_folder` on disk before FA is asked to load it.
    ///
    /// Two different mechanisms behind one call, because the replay does not
    /// say which kind of map it was played on:
    ///
    /// - A generated map exists in no vault and can only be rebuilt by running
    ///   the Neroxis generator again, exactly as the live launcher's
    ///   `ensure_generated_map` does for a game the server has already seated
    ///   the player in.
    /// - Anything else is staged from the vault CDN by
    ///   [`game_updater::ensure_map_available`], which is a no-op for the
    ///   base-game maps that ship inside the install.
    ///
    /// `Err` is reserved for a generated map that could not be produced. That
    /// is not the same posture as vault staging, whose failure is only a
    /// warning: a missing vault map may still be a base map FA can find on its
    /// own, whereas a generated map that is not on disk is a guaranteed
    /// `aborting session` a few seconds later, with nothing in the UI to
    /// explain it. Saying so up front beats launching into that.
    async fn ensure_replay_map(
        &self,
        target_dir: &Path,
        map_folder: &str,
    ) -> Result<Option<String>, String> {
        use faf_domain::protocol::map_generator::is_generated_map;

        if is_generated_map(map_folder) {
            self.ensure_generated_map(map_folder).await?;
            return Ok(None);
        }

        if let Err(e) = game_updater::ensure_map_available(
            &self.http,
            &self.config.content_base,
            target_dir,
            map_folder,
        )
        .await
        {
            return Ok(Some(format!("could not stage map {map_folder}: {e}")));
        }
        Ok(None)
    }

    /// Rebuild a generated map, unless it is already installed.
    ///
    /// Mirrors `services::launcher::ensure_generated_map`, minus the progress
    /// events: this side of the boundary has no event sink. The run is logged
    /// instead, since it routinely takes tens of seconds and a silent wait is
    /// otherwise indistinguishable from a hang.
    async fn ensure_generated_map(&self, map_name: &str) -> Result<(), String> {
        use faf_domain::state::GeneratorStatus;

        if self.map_generator.is_installed(map_name) {
            return Ok(());
        }
        if !self
            .auto_generate_maps
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(format!(
                "this replay was played on the generated map {map_name}, which is not \
                 installed, and automatic map generation is disabled in settings"
            ));
        }

        tracing::info!(map_name, "generating map required by replay playback");
        let mut updates = self
            .map_generator
            .generate_named(map_name.to_string())
            .await;
        let mut outcome = Err("the map generator produced no result".to_string());
        while let Some(crate::ports::GeneratorUpdate::Status(status)) = updates.recv().await {
            match status {
                GeneratorStatus::Generated { .. } => outcome = Ok(()),
                GeneratorStatus::Failed { reason } => {
                    outcome = Err(format!("could not generate {map_name}: {reason}"))
                }
                _ => {}
            }
        }
        outcome
    }

    async fn fetch_vault_replay(&self, uid: i32) -> Result<Vec<u8>, String> {
        let raw = format!("{}/{}", self.config.vault_host.trim_end_matches('/'), uid);
        let mut url = url::Url::parse(&raw)
            .map_err(|_| "configured FAF replay URL is invalid".to_string())?;

        for redirect_count in 0..=MAX_REPLAY_DOWNLOAD_REDIRECTS {
            validate_replay_download_url(&url, uid, &self.config)?;
            let response = self
                .download_http
                .get(url.clone())
                .send()
                .await
                .map_err(|error| format!("could not download replay {uid}: {error}"))?;
            let status = response.status();
            if status.is_redirection() {
                if redirect_count == MAX_REPLAY_DOWNLOAD_REDIRECTS {
                    return Err(format!(
                        "could not download replay {uid}: too many redirects"
                    ));
                }
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .ok_or_else(|| {
                        format!("could not download replay {uid}: redirect has no location")
                    })?
                    .to_str()
                    .map_err(|_| {
                        format!("could not download replay {uid}: redirect location is invalid")
                    })?;
                url = url.join(location).map_err(|_| {
                    format!("could not download replay {uid}: redirect location is invalid")
                })?;
                continue;
            }
            if !status.is_success() {
                return Err(format!("could not download replay {uid}: {status}"));
            }
            return bounded_body(response, &format!("replay {uid}"), MAX_DOWNLOAD_BYTES).await;
        }
        unreachable!("the bounded redirect loop always returns")
    }

    /// Every game the query's players were all in, as one page of results.
    ///
    /// The API cannot answer "A *and* B were in this game" in a single filter:
    /// Elide derives a join alias from the property path alone, so two clauses
    /// on `playerStats.player.login` share one join and match nothing (see
    /// `replay_query`'s module docs). So we ask it one player at a time and
    /// intersect the game ids ourselves.
    ///
    /// Completeness is the point of the feature, so the per-player scans read
    /// *every* page rather than stopping once the current page could be filled:
    /// two players who last met years ago must still show up, and the exact
    /// intersection is what lets the pager offer real page numbers and a real
    /// total. That costs a handful of round trips per search, which is the
    /// trade this was designed around: the intersection is then cached
    /// (`shared_games`) so paging through the results costs one request each,
    /// like any other search.
    ///
    /// The implicit date floor is dropped here for the same reason
    /// ([`ReplayQuery::fallback_months`] exists to keep *unbounded* searches
    /// off the slow path; a login is already a narrow index lookup). Explicit
    /// `after`/`before` bounds still apply.
    async fn search_shared_games(
        &self,
        query: &ReplayQuery,
        token: &str,
    ) -> Result<VaultSearchResult, String> {
        let key = shared_games_key(query);
        let cached = self
            .shared_games
            .lock()
            .expect("shared games cache poisoned")
            .as_ref()
            .filter(|(cached_key, _)| *cached_key == key)
            .map(|(_, ids)| ids.clone());

        let shared = match cached {
            Some(ids) => ids,
            None => {
                let names = query.player_names();
                // Timed at info level, per phase: this search is the one place
                // in the client that can legitimately take seconds, and "which
                // part" is otherwise unanswerable from a bug report.
                let started = std::time::Instant::now();
                let resolved = self
                    .resolve_player_ids(&names, query.exact_player, token)
                    .await?;
                tracing::info!(
                    names = names.len(),
                    exact = query.exact_player,
                    accounts = resolved
                        .as_ref()
                        .map(|r| r.iter().map(Vec::len).sum::<usize>()),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "shared games: resolved player names"
                );
                let ids = match resolved {
                    // A name nobody answers to cannot share a game with anyone.
                    None => Vec::new(),
                    Some(per_name) => {
                        let mut per_player = Vec::with_capacity(per_name.len());
                        for player_ids in &per_name {
                            let scan = std::time::Instant::now();
                            let games = self.collect_game_ids(query, player_ids, token).await?;
                            tracing::info!(
                                accounts = player_ids.len(),
                                games = games.len(),
                                elapsed_ms = scan.elapsed().as_millis() as u64,
                                "shared games: scanned one player"
                            );
                            per_player.push(games);
                        }
                        let shared = replay_query::intersect_game_ids(&per_player);
                        let ordered = self.sort_shared_games(query, shared, token).await?;
                        tracing::info!(
                            shared = ordered.len(),
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "shared games: search complete"
                        );
                        ordered
                    }
                };
                *self
                    .shared_games
                    .lock()
                    .expect("shared games cache poisoned") = Some((key, ids.clone()));
                ids
            }
        };

        let total_records = i32::try_from(shared.len()).unwrap_or(i32::MAX);
        let page_size = query.page_size.max(1);
        let total_pages = i32::try_from(shared.len().div_ceil(page_size as usize))
            .unwrap_or(i32::MAX)
            .max(1);
        let page_ids = replay_query::page_slice(&shared, query.page, page_size);
        let Some(filter) = replay_query::ids_filter(page_ids) else {
            return Ok(VaultSearchResult {
                replays: Vec::new(),
                total_pages: Some(total_pages),
                total_records: Some(total_records),
            });
        };

        // The ids are already the answer; this call only fetches the rows and
        // their relationships. Same `sort` as the scans, so the page arrives in
        // the order the intersection is in.
        let mut url = url::Url::parse(&format!("{}/data/game", self.config.api_base))
            .map_err(|e| format!("invalid API base: {e}"))?;
        url.query_pairs_mut()
            .append_pair("sort", &query.sort_param())
            .append_pair("page[size]", &page_size.to_string())
            .append_pair("include", VAULT_INCLUDE)
            .append_pair("filter", &filter);

        let doc = fetch_document(&self.http, url, token).await?;
        Ok(VaultSearchResult {
            replays: parse_vault_replays(&doc),
            total_pages: Some(total_pages),
            total_records: Some(total_records),
        })
    }

    /// The player ids behind each typed name, in the order they were typed.
    ///
    /// One request for all of them. This is what lets the scans filter on
    /// `playerStats.player.id` instead of a login wildcard: see
    /// [`replay_query::build_scan_filter`] for why that is the difference
    /// between seconds and minutes. A substring name can resolve to several
    /// accounts, so each name keeps a *set* of ids.
    ///
    /// `None` means one of the names matches no account at all, which makes the
    /// intersection empty without scanning anything.
    async fn resolve_player_ids(
        &self,
        names: &[&str],
        exact: bool,
        token: &str,
    ) -> Result<Option<Vec<Vec<i32>>>, String> {
        let Some(filter) = replay_query::logins_filter(names, exact) else {
            return Ok(None);
        };
        let mut url = url::Url::parse(&format!("{}/data/player", self.config.api_base))
            .map_err(|e| format!("invalid API base: {e}"))?;
        url.query_pairs_mut()
            .append_pair("filter", &filter)
            .append_pair("fields[player]", "login")
            .append_pair("page[size]", &MAX_RESOLVED_PLAYERS.to_string());

        let doc = fetch_document(&self.http, url, token).await?;
        if doc.data.len() >= MAX_RESOLVED_PLAYERS as usize {
            tracing::warn!(
                resolved = doc.data.len(),
                "a player name matched more accounts than one search resolves;                  narrow it or tick the exact-name box"
            );
        }
        let resolved: Vec<(i32, String)> = doc
            .data
            .iter()
            .filter_map(|player| {
                let id = player.id.parse::<i32>().ok()?;
                let login = player.attributes.get("login")?.as_str()?.to_string();
                Some((id, login))
            })
            .collect();

        let mut per_name = Vec::with_capacity(names.len());
        for name in names {
            let ids: Vec<i32> = resolved
                .iter()
                .filter(|(_, login)| replay_query::login_matches(login, name, exact))
                .map(|(id, _)| *id)
                .collect();
            if ids.is_empty() {
                return Ok(None);
            }
            per_name.push(ids);
        }
        Ok(Some(per_name))
    }

    /// The ids of every game these player ids appear in, newest first.
    ///
    /// Seeks by id rather than asking for page after page: each request carries
    /// the lowest id of the last one, so every page is the same bounded index
    /// range instead of an ever-growing `OFFSET`. Sorted by `-id`, which is the
    /// seek key; game ids and start times run in the same direction, so this is
    /// also the newest-first order the intersection is presented in.
    ///
    /// A sparse fieldset keeps the payload to ids: `startTime` is named only
    /// because a fieldset wants a field, and `infra::player_card` narrows the
    /// same type the same way.
    ///
    /// [`ID_SCAN_CAP`] is a runaway guard, not a page budget: it sits well above
    /// the busiest account on the server, so a real scan ends on a short page.
    async fn collect_game_ids(
        &self,
        query: &ReplayQuery,
        player_ids: &[i32],
        token: &str,
    ) -> Result<Vec<i32>, String> {
        let mut ids: Vec<i32> = Vec::new();
        let mut before_id: Option<i32> = None;
        loop {
            let mut url = url::Url::parse(&format!("{}/data/game", self.config.api_base))
                .map_err(|e| format!("invalid API base: {e}"))?;
            {
                let mut pairs = url.query_pairs_mut();
                pairs
                    .append_pair("sort", "-id")
                    .append_pair("page[size]", &ID_SCAN_PAGE_SIZE.to_string())
                    .append_pair("fields[game]", "startTime");
                if let Some(filter) = replay_query::build_scan_filter(query, player_ids, before_id)
                {
                    pairs.append_pair("filter", &filter);
                }
            }

            let doc = fetch_document(&self.http, url, token).await?;
            let received = doc.data.len();
            let page: Vec<i32> = doc
                .data
                .iter()
                .filter_map(|game| game.id.parse::<i32>().ok())
                .collect();
            let lowest = page.iter().copied().min();
            ids.extend(page);
            match lowest {
                // A short page is the end; so is a page whose ids cannot be
                // parsed, which would otherwise seek from the same place
                // forever.
                Some(lowest) if received >= ID_SCAN_PAGE_SIZE as usize => {
                    if ids.len() >= ID_SCAN_CAP {
                        tracing::warn!(
                            collected = ids.len(),
                            "replay id scan hit its cap; the shared-games result may be incomplete"
                        );
                        return Ok(ids);
                    }
                    before_id = Some(lowest);
                }
                _ => return Ok(ids),
            }
        }
    }

    /// The intersection in the order the user asked for.
    ///
    /// The scans run newest-first by id, which is already the answer for the
    /// default sort and its reverse. Any other ordering lives in columns the
    /// scan never fetched (duration, title, review score), so the server is
    /// asked to sort the ids it has just been handed: one extra request, and
    /// only when the intersection is small enough to order in a single one.
    async fn sort_shared_games(
        &self,
        query: &ReplayQuery,
        shared: Vec<i32>,
        token: &str,
    ) -> Result<Vec<i32>, String> {
        let by_id = matches!(
            query.sort_by,
            replay_query::ReplaySortField::StartTime | replay_query::ReplaySortField::Id
        );
        if by_id {
            let mut ordered = shared;
            if !query.sort_descending {
                ordered.reverse();
            }
            return Ok(ordered);
        }
        if shared.len() > MAX_SORTABLE_SHARED_GAMES {
            tracing::warn!(
                shared = shared.len(),
                "too many shared games to reorder in one request; keeping newest first"
            );
            return Ok(shared);
        }
        let Some(filter) = replay_query::ids_filter(&shared) else {
            return Ok(shared);
        };

        let mut url = url::Url::parse(&format!("{}/data/game", self.config.api_base))
            .map_err(|e| format!("invalid API base: {e}"))?;
        url.query_pairs_mut()
            .append_pair("sort", &query.sort_param())
            .append_pair("page[size]", &MAX_SORTABLE_SHARED_GAMES.to_string())
            .append_pair("fields[game]", "startTime")
            .append_pair("filter", &filter);

        let doc = fetch_document(&self.http, url, token).await?;
        let ordered: Vec<i32> = doc
            .data
            .iter()
            .filter_map(|game| game.id.parse::<i32>().ok())
            .collect();
        // Trust the server's order only if it returned the same set; anything
        // else (a dropped row, a page cap) would silently lose results.
        if ordered.len() == shared.len() {
            Ok(ordered)
        } else {
            Ok(shared)
        }
    }

    async fn download_vault_to(&self, uid: i32, directory: PathBuf) -> Result<PathBuf, String> {
        let bytes = self.fetch_vault_replay(uid).await?;
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|error| format!("could not create replay directory: {error}"))?;
        let path = directory.join(format!("{uid}.fafreplay"));
        let write_path = path.clone();
        tokio::task::spawn_blocking(move || write_replay_atomically(&write_path, &bytes))
            .await
            .map_err(|error| format!("replay write task failed: {error}"))??;
        Ok(path)
    }
}

fn write_replay_atomically(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "replay path has no parent directory".to_string())?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| format!("could not create temporary replay: {error}"))?;
    temporary
        .write_all(bytes)
        .map_err(|error| format!("could not write replay: {error}"))?;
    temporary
        .flush()
        .map_err(|error| format!("could not flush replay: {error}"))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|error| format!("could not sync replay: {error}"))?;
    temporary
        .persist(path)
        .map(|_| ())
        .map_err(|error| format!("could not publish replay: {}", error.error))
}

#[async_trait]
impl ReplayPort for ReplayClient {
    async fn watch_live(
        &self,
        target: LiveReplayTarget,
        player: String,
    ) -> Result<Option<String>, String> {
        let _playback_guard = self.playback_lock.lock().await;
        let token = self
            .tokens
            .get()
            .ok_or_else(|| "not logged in".to_string())?;

        let mod_name = normalize_mod(&target.mod_name);

        // Preparation runs before the stream is opened, deliberately. The
        // replay server starts sending the moment the handshake lands, and FA
        // is launched on the first frame, so a multi-minute patch or map
        // download between those two points would be spent buffering a live
        // stream nobody is reading yet.
        let mut warning = None;
        match self.install_dir() {
            Some(target_dir) => {
                // The gap behind "Ack! Unable to load replay from gpgnet".
                // Live spectating never updated the install it launches, so
                // the replay install stayed on whatever build was last staged
                // into it: in practice some old replay's version, since that
                // path pins an exact one and nothing else ever touches this
                // directory. FA refuses a replay whose build does not match,
                // and a live stream is always the *current* build. `play_file`
                // has resolved its replay's exact version since it was
                // written; this is the same step for a stream that carries no
                // version of its own to read, so it asks for `latest` the way
                // a live game launch does.
                //
                // Fatal for the same reason it is fatal in `play_file`:
                // launching anyway just reproduces the refusal, with the user
                // dropped on the main menu and no diagnostic anywhere.
                game_updater::ensure_latest_game_version(
                    &self.http,
                    &token,
                    &self.config.api_base,
                    &cache_dir()?.join("game_files"),
                    &target_dir,
                    &mod_name,
                    &self.config.exe_name,
                    false,
                    &|_| {},
                )
                .await
                .map_err(|e| format!("could not prepare the game for live spectating: {e}"))?;

                // Same gap `play_file` had before it got `ensure_map_available`:
                // live-spectating never staged the game's map at all, so a custom
                // (non-base) map FA can't already find leaves it stuck the same
                // way: confirmed live (`map /maps/hoey.v0002/Hoey.scmap failed.
                // aborting session.`), silently dumping the user back to the main
                // menu with no crash dialog and no error surfaced here either.
                // `target.map` is the FAF technical map name (e.g. `hoey.v0002`),
                // the same shape `ensure_map_available` expects.
                if let Some(warn) = self.ensure_replay_map(&target_dir, &target.map).await? {
                    warning = Some(warn);
                }
            }
            None => {
                tracing::warn!(
                    "no replay install is configured, so the engine version and map \
                     were not prepared; FA may refuse to load this live replay"
                );
            }
        }

        let transport = self.live_transport();

        let access_url = fetch_access_url(
            &self.http,
            &self.config.user_api_base,
            "/replay/access",
            &token,
        )
        .await?;
        let ws_url = validated_ws_url(&access_url)?;

        // Note: ws_url carries a one-time verify token: never log it verbatim.
        let (ws, _) = tokio_tungstenite::connect_async(ws_url.as_str())
            .await
            .map_err(|e| format!("could not open replay websocket: {e}"))?;
        let (mut write, mut read) = ws.split();

        let handshake = format!("G/{}/{}.scfareplay\0", target.uid, player).into_bytes();
        write
            .send(Message::Binary(handshake))
            .await
            .map_err(|e| format!("replay handshake failed: {e}"))?;

        // Gate FA's launch on the first byte actually arriving, mirroring the
        // Python client: otherwise FA connects to a proxy with nothing behind it.
        let first = wait_for_first_binary(&mut read).await?;

        let log_path = crate::infra::game_logs::next_path("live-replay", Some(target.uid))?;
        // Everything but the replay source is the same on both transports.
        let launch_args = |source: String| {
            vec![
                "/replay".to_string(),
                source,
                "/init".to_string(),
                format!("init_{mod_name}.lua"),
                "/nobugreport".to_string(),
                "/log".to_string(),
                log_path.display().to_string(),
                "/replayid".to_string(),
                target.uid.to_string(),
            ]
        };

        match transport {
            LiveTransport::Tcp => {
                let port =
                    free_port().ok_or_else(|| "could not reserve a local port".to_string())?;
                let listener = TcpListener::bind(("127.0.0.1", port))
                    .await
                    .map_err(|e| format!("could not bind local replay proxy: {e}"))?;

                self.process
                    .launch_replay(launch_args(format!(
                        "gpgnet://127.0.0.1:{port}/{}/{player}.SCFAreplay",
                        target.uid
                    )))
                    .await?;

                let (stream, _) = tokio::time::timeout(READY_TIMEOUT, listener.accept())
                    .await
                    .map_err(|_| "timed out waiting for the game to connect".to_string())?
                    .map_err(|e| format!("accept failed: {e}"))?;

                tokio::spawn(relay_session(stream, write, read, first));
            }
            #[cfg(windows)]
            LiveTransport::Pipe => {
                // Created before FA is launched: a client opening a pipe that
                // does not exist yet fails outright, where a TCP connect to a
                // bound listener simply waits.
                let pipe = tokio::net::windows::named_pipe::ServerOptions::new()
                    .first_pipe_instance(true)
                    .max_instances(1)
                    .access_outbound(true)
                    .access_inbound(false)
                    .create(LIVE_REPLAY_PIPE_PATH)
                    .map_err(|e| {
                        format!("could not open the live replay pipe: {e} (is another live replay still running?)")
                    })?;

                self.process
                    .launch_replay(launch_args(LIVE_REPLAY_PIPE_PATH.to_string()))
                    .await?;

                tokio::time::timeout(READY_TIMEOUT, pipe.connect())
                    .await
                    .map_err(|_| "timed out waiting for the game to connect".to_string())?
                    .map_err(|e| format!("the game could not open the replay pipe: {e}"))?;

                tokio::spawn(pipe_relay_session(pipe, write, read, first));
            }
        }
        Ok(warning)
    }

    async fn play_file(&self, path: PathBuf) -> Result<Option<String>, String> {
        let _playback_guard = self.playback_lock.lock().await;
        let replay = prepare_scfareplay(&path).await?;
        let mod_name = normalize_mod(&replay.mod_name);
        let mut warning = None;

        let target = self.install_dir();
        if target.is_none() {
            // Every step below is skipped without it, and a replay launched
            // against an unmatched engine build opens FA on the main menu with
            // no error of its own. Say so rather than reporting success.
            tracing::warn!(
                "no replay install is configured, so the engine version and map \
                 were not prepared; FA may refuse to load this replay"
            );
        }
        if let Some(target_dir) = target.as_deref() {
            // Old replays embed the exact engine build they need; FA refuses
            // to load one that doesn't match what's installed ("Ack! Unable
            // to load game replay"). Update before every launch: cheap when
            // already current, since files are skipped on a matching MD5
            // (see infra/game_updater.rs). Unlike map staging below, there is
            // no "expected to fail" case here: every replay's embedded
            // version is exact and required, so a failure here is fatal:
            // launching anyway would just reproduce the exact crash this is
            // supposed to prevent, with no diagnostic for the user.
            let token = self
                .tokens
                .get()
                .ok_or_else(|| "not logged in".to_string())?;
            let version_info = game_updater::ReplayVersionInfo {
                mod_name: mod_name.clone(),
                game_version: replay.game_version,
                git_sha: replay.git_sha,
                git_short_sha: replay.git_short_sha,
                build_signature: replay.build_signature,
                version_name: replay.version_name,
                launched_at: replay.launched_at,
            };
            if let Some(warn) = game_updater::resolve_and_stage_replay_version(
                &self.http,
                &token,
                &self.config.api_base,
                &cache_dir()?.join("game_files"),
                target_dir,
                &version_info,
                &self.config.exe_name,
            )
            .await
            .map_err(|e| format!("could not prepare game for replay: {e}"))?
            {
                warning = Some(warn);
            }

            // Old replays' init scripts predate the "custom vault path"
            // feature and always search two hardcoded default directories
            // for maps, ignoring the FAF client's configured vault location
            // entirely: a real, community-documented bug (see the plan).
            // Stage the map into both before launch. Unlike the version
            // update above, failure here is *not* fatal: official/base-game
            // maps are never found on the vault CDN and that's expected (see
            // `ensure_map_available`'s docs): but it's surfaced as a
            // warning rather than silently swallowed, since a genuinely
            // missing custom map is exactly what leaves FA stuck on a blank
            // loading screen with no explanation.
            if let Some(map_folder) = &replay.map_folder {
                if let Some(warn) = self.ensure_replay_map(target_dir, map_folder).await? {
                    warning = Some(warn);
                }
            }
        }

        // A replay recorded with sim mods needs those mods active in
        // `game.prefs` at launch. Mirrors the Python client's exact
        // semantics (`fa/check.py::check` → `fa/mods.py::checkMods` →
        // `setActiveMods(mods, keepuimods=True)`), both of which matter:
        // - Only replays that *have* sim mods touch `game.prefs` at all
        //   (`if sim_mods:` in `check()`): an unmodded replay leaves the
        //   user's mod setup completely alone.
        // - `keepuimods=True`: the user's currently-active *UI* mods stay
        //   active; only the sim-mod set is replaced by the replay's own.
        //   UI mods don't affect the simulation, so they can't desync
        //   playback: and silently wiping the user's UI setup (hotbuild,
        //   eco panels, …) on every modded replay is exactly the kind of
        //   surprise the reference client deliberately avoids.
        // Independent of `replay_target_dir`: mods live in the shared,
        // install-independent mods folder/`game.prefs`, same as
        // `infra::mods`'s own posture.
        if !replay.sim_mods.is_empty() {
            match crate::infra::mods::list_installed_dir(&crate::infra::mods::mods_dir()).await {
                Ok(installed) => {
                    let installed_uids: std::collections::HashSet<&str> =
                        installed.iter().map(|m| m.uid.as_str()).collect();
                    let (present, missing): (Vec<_>, Vec<_>) = replay
                        .sim_mods
                        .iter()
                        .partition(|(uid, _)| installed_uids.contains(uid.as_str()));
                    // keepuimods=True: active UI mods first, then the
                    // replay's sim mods (same order Python builds
                    // `keepTheseMods + mods`).
                    let mut active_uids: Vec<String> = installed
                        .iter()
                        .filter(|m| m.enabled && m.mod_type == ModType::Ui)
                        .map(|m| m.uid.clone())
                        .collect();
                    active_uids.extend(present.into_iter().map(|(uid, _)| uid.clone()));
                    if let Err(e) =
                        crate::infra::mods::write_active_mod_uids_to_disk(&active_uids).await
                    {
                        warning = Some(format!("could not set this replay's active mods: {e}"));
                    } else if !missing.is_empty() {
                        let names: Vec<&str> =
                            missing.iter().map(|(_, name)| name.as_str()).collect();
                        warning = Some(format!("missing mod(s): {}", names.join(", ")));
                    }
                }
                Err(e) => warning = Some(format!("could not check installed mods: {e}")),
            }
        }

        let log_path = crate::infra::game_logs::next_path("replay", replay.uid)?;
        let mut args = vec![
            "/replay".to_string(),
            replay.path.display().to_string(),
            "/init".to_string(),
            format!("init_{mod_name}.lua"),
            "/nobugreport".to_string(),
            // Mirrors the Python client passing `/log "<LOG_FILE_REPLAY>"`,
            // without it FA writes no log at all, so a hang like the one
            // this session spent a long time diagnosing blind is otherwise
            // completely opaque (no crash, no stderr, nothing on disk).
            "/log".to_string(),
            log_path.display().to_string(),
        ];
        if let Some(uid) = replay.uid {
            args.push("/replayid".to_string());
            args.push(uid.to_string());
        }
        self.process.launch_replay(args).await?;
        Ok(warning)
    }

    async fn search_vault(&self, query: ReplayQuery) -> Result<VaultSearchResult, String> {
        let token = self
            .tokens
            .get()
            .ok_or_else(|| "not logged in".to_string())?;

        // Two or more names is a different question ("games they shared") and
        // needs a different shape of request: see `search_shared_games`.
        if query.player_names().len() > 1 {
            return self.search_shared_games(&query, &token).await;
        }

        let mut url = url::Url::parse(&format!("{}/data/game", self.config.api_base))
            .map_err(|e| format!("invalid API base: {e}"))?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs
                .append_pair("sort", &query.sort_param())
                .append_pair("page[size]", &query.page_size.to_string())
                .append_pair("page[number]", &query.page.max(1).to_string())
                .append_key_only("page[totals]")
                .append_pair("include", VAULT_INCLUDE);
            // The date floor that keeps an otherwise unbounded filtered search
            // off the slow path: the rule lives in the query, the clock here.
            let fallback = query.fallback_months().map(months_ago);
            if let Some(filter) = replay_query::build_filter(&query, fallback.as_deref(), None) {
                pairs.append_pair("filter", &filter);
            }
        }

        let doc = fetch_document(&self.http, url, &token).await?;
        let replays = parse_vault_replays(&doc);
        let total_pages = total_pages(&doc.meta, query.page_size);
        let total_records = meta_page_i32(&doc.meta, "totalRecords");
        Ok(VaultSearchResult {
            replays,
            total_pages,
            total_records,
        })
    }

    async fn list_featured_mods(&self) -> Result<Vec<String>, String> {
        let token = self
            .tokens
            .get()
            .ok_or_else(|| "not logged in".to_string())?;

        let mut url = url::Url::parse(&format!("{}/data/featuredMod", self.config.api_base))
            .map_err(|e| format!("invalid API base: {e}"))?;
        url.query_pairs_mut()
            .append_pair("page[size]", "100")
            .append_pair("sort", "order");

        let doc = fetch_document(&self.http, url, &token).await?;
        Ok(parse_featured_mods(&doc))
    }

    async fn watch_vault(&self, uid: i32) -> Result<Option<String>, String> {
        let path = self.download_vault_to(uid, cache_dir()?).await?;
        self.play_file(path).await
    }

    async fn download_vault(&self, uid: i32) -> Result<LocalReplay, String> {
        let path = self.download_vault_to(uid, local_replays_dir()).await?;
        local_metadata_for_path(&path).await
    }

    async fn load_details(
        &self,
        uid: i32,
        local_path: Option<PathBuf>,
    ) -> Result<ReplayDetails, String> {
        let path = if let Some(path) = local_path.filter(|p| p.exists()) {
            path
        } else {
            let cached_scfa = cache_dir()?.join(format!("{uid}.scfareplay"));
            if cached_scfa.exists() {
                cached_scfa
            } else {
                let local_faf = local_replays_dir().join(format!("{uid}.fafreplay"));
                if local_faf.exists() {
                    local_faf
                } else {
                    let cached_faf = cache_dir()?.join(format!("{uid}.fafreplay"));
                    if cached_faf.exists() {
                        cached_faf
                    } else {
                        self.download_vault_to(uid, cache_dir()?).await?
                    }
                }
            }
        };

        // Detail loading is intentionally deferred until the user asks for it,
        // but once requested it must expose the complete replay metadata: game
        // options, in-game chat, and the FAF version.
        read_detailed_info(&path).await
    }

    async fn list_local(&self, limit: usize) -> Result<Vec<LocalReplay>, String> {
        list_local_dir(&local_replays_dir(), limit).await
    }

    fn set_install_dir(&self, dir: Option<PathBuf>) {
        // An explicit `FAF_REPLAY_UPDATE_DIR` is a deliberate override and
        // outranks the configured install, matching how it outranks
        // `FAF_REPLAY_GAME_PATH` when the directory is first derived.
        if std::env::var("FAF_REPLAY_UPDATE_DIR").is_ok_and(|value| !value.is_empty()) {
            return;
        }
        *self.install_dir.lock().unwrap() = dir;
    }

    fn set_live_replay_pipe(&self, enabled: bool) {
        self.pipe_live_replay
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    fn set_auto_generate_maps(&self, enabled: bool) {
        self.auto_generate_maps
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    async fn delete_local(&self, path: PathBuf) -> Result<(), String> {
        delete_local_file(&local_replays_dir(), &path).await
    }
}

/// Scans `dir` for `.fafreplay` and legacy `.scfareplay` files: the testable body of
/// [`ReplayClient::list_local`], split out so tests don't have to mutate the
/// process-global `FAF_REPLAYS_DIR`/`ALLUSERSPROFILE` env vars.
async fn list_local_dir(dir: &std::path::Path, limit: usize) -> Result<Vec<LocalReplay>, String> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("could not read {}: {e}", dir.display())),
    };

    // Sort by mtime first, which is cheap: directory metadata only.
    let mut files: Vec<(PathBuf, std::time::SystemTime, u64)> = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| format!("could not list {}: {e}", dir.display()))?
    {
        let path = entry.path();
        let is_replay = path.extension().and_then(|e| e.to_str()).is_some_and(|e| {
            e.eq_ignore_ascii_case("fafreplay") || e.eq_ignore_ascii_case("scfareplay")
        });
        if !is_replay {
            continue;
        }
        if let Ok(meta) = entry.metadata().await {
            if let Ok(modified) = meta.modified() {
                files.push((path, modified, meta.len()));
            }
        }
    }
    files.sort_by_key(|f| std::cmp::Reverse(f.1));

    // Every replay is listed, but only the newest `limit` have their headers
    // read. That read is the whole cost: one `open` plus a bounded read per
    // file, and a real archive is thousands of files, so doing all of them made
    // opening the Local tab a visible wait. Reading only what the first pages
    // show, and letting the view ask for more, keeps the list complete without
    // paying for all of it up front.
    //
    // Order is preserved (`buffered`), because the sort above is the "newest
    // first" the list is presented in.
    let detailed = files.len().min(limit);
    let mut replays: Vec<LocalReplay> = futures_util::stream::iter(files[..detailed].to_vec())
        .map(|(path, modified, file_size)| async move {
            read_local_metadata(&path, modified, file_size).await
        })
        .buffered(LOCAL_REPLAY_READ_CONCURRENCY)
        .collect()
        .await;

    // The rest carry what the directory entry already gave: name, date and
    // size. `Unread` says the details were not fetched, which is not the same
    // claim as `Broken`.
    replays.extend(files[detailed..].iter().map(|(path, modified, file_size)| {
        empty_local_replay(path, *modified, *file_size, LocalReplayStatus::Unread, true)
    }));
    Ok(replays)
}

pub const LOCAL_REPLAY_PAGE_LIMIT: usize = crate::ports::DEFAULT_LOCAL_REPLAY_LIMIT;

/// How many local replay headers to read at once.
///
/// Each is an `open` plus one bounded read, so the wall-clock cost of listing a
/// folder is dominated by how many of those can be in flight rather than by how
/// many files there are. Sixteen keeps a three-thousand-file archive responsive
/// without flooding the runtime.
const LOCAL_REPLAY_READ_CONCURRENCY: usize = 16;

/// The shared FAF replay folder every client writes to. Mirrors the Python
/// client's `APPDATA_DIR` (`%ALLUSERSPROFILE%\FAForever` on Windows, falling
/// back to `~/FAForever` elsewhere) plus `/replays`. `FAF_REPLAYS_DIR`
/// overrides it (tests, alternate installs).
pub(crate) fn local_replays_dir() -> PathBuf {
    if let Some(dir) = crate::infra::paths::replays_dir() {
        return dir;
    }
    if let Ok(dir) = std::env::var("FAF_REPLAYS_DIR") {
        if !dir.is_empty() {
            return PathBuf::from(dir);
        }
    }
    let base = if cfg!(windows) {
        std::env::var("ALLUSERSPROFILE").unwrap_or_else(|_| r"C:\ProgramData".to_string())
    } else {
        std::env::var("HOME").unwrap_or_default()
    };
    PathBuf::from(base).join("FAForever").join("replays")
}

fn unix_seconds(time: std::time::SystemTime) -> u32 {
    time.duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs().min(u32::MAX as u64) as u32)
        .unwrap_or_default()
}

fn replay_uid(header: &Value, file_name: &str) -> Option<i32> {
    header
        .get("uid")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .or_else(|| {
            let digits: String = file_name.chars().take_while(char::is_ascii_digit).collect();
            digits.parse().ok()
        })
}

fn local_teams(header: &Value) -> Vec<LocalReplayTeam> {
    header
        .get("teams")
        .and_then(Value::as_object)
        .map(|teams| {
            teams
                .iter()
                .filter_map(|(team, players)| {
                    let players: Vec<LocalReplayPlayer> = players
                        .as_array()?
                        .iter()
                        .filter_map(Value::as_str)
                        .map(|name| LocalReplayPlayer {
                            name: name.to_string(),
                            faction: None,
                            rating: None,
                        })
                        .collect();
                    (!players.is_empty()).then(|| LocalReplayTeam {
                        team: team.clone(),
                        players,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn local_sim_mods(header: &Value) -> Vec<String> {
    header
        .get("sim_mods")
        .and_then(Value::as_object)
        .map(|mods| {
            mods.values()
                .filter_map(|value| {
                    value
                        .as_str()
                        .or_else(|| value.get("name").and_then(Value::as_str))
                })
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

const LOCAL_REPLAY_BODY_PREFIX_BYTES: u64 = 512 * 1024;
const LOCAL_REPLAY_BODY_READ_BYTES: usize = 4 * 1024 * 1024;

/// Read the compact FA replay header from a compressed local replay body. The
/// JSON envelope has player names, but faction and displayed rating are stored
/// in the binary Lua army table that follows it.
#[derive(Default)]
struct LocalBodyInfo {
    player_stats: HashMap<String, (Option<i32>, Option<i32>)>,
    map_name: Option<String>,
    game_version: Option<i32>,
}

fn extract_map_folder(path: &str) -> String {
    let path = if let Some((_, after)) = path.split_once("\r\n") {
        after
    } else if let Some((_, after)) = path.split_once('\n') {
        after
    } else {
        path
    };
    let normalized = path.replace('\\', "/");
    let parts: Vec<&str> = normalized.split('/').filter(|p| !p.is_empty()).collect();
    if let Some(maps_idx) = parts.iter().position(|p| p.eq_ignore_ascii_case("maps")) {
        if let Some(folder) = parts.get(maps_idx + 1) {
            return folder.to_string();
        }
    }
    if let Some(folder) = parts
        .iter()
        .find(|p| p.to_ascii_lowercase().starts_with("neroxis_map_generator_"))
    {
        return folder.to_string();
    }
    if let Some(last) = parts.last() {
        return last.replace("_scenario.lua", "").replace(".scmap", "");
    }
    String::new()
}

/// Read the compact FA replay header from a compressed local replay body. The
/// JSON envelope has player names, but faction and displayed rating are stored
/// in the binary Lua army table that follows it, and the scenario file path is
/// in the game options table.
fn local_body_player_stats(body: &[u8], compression: &str) -> LocalBodyInfo {
    let prefix = if compression.eq_ignore_ascii_case("zstd") {
        zstd::stream::read::Decoder::new(body)
            .map(read_replay_body_prefix)
            .unwrap_or_default()
    } else {
        let mut decoded =
            base64::read::DecoderReader::new(body, &base64::engine::general_purpose::STANDARD);
        let mut uncompressed_size = [0; 4];
        if decoded.read_exact(&mut uncompressed_size).is_err() {
            return LocalBodyInfo::default();
        }
        read_replay_body_prefix(flate2::read::ZlibDecoder::new(decoded))
    };
    if prefix.is_empty() {
        return LocalBodyInfo::default();
    }
    parse_local_body_info(&prefix)
}

fn read_replay_body_prefix(mut reader: impl Read) -> Vec<u8> {
    read_replay_body_prefix_limit(&mut reader, LOCAL_REPLAY_BODY_PREFIX_BYTES as usize)
}

fn read_replay_body_prefix_limit(mut reader: impl Read, limit: usize) -> Vec<u8> {
    let mut prefix = Vec::new();
    let mut chunk = [0_u8; 16 * 1024];
    while prefix.len() < limit {
        let remaining = limit - prefix.len();
        let size = remaining.min(chunk.len());
        match reader.read(&mut chunk[..size]) {
            Ok(0) | Err(_) => break,
            Ok(read) => prefix.extend_from_slice(&chunk[..read]),
        }
    }
    prefix
}

fn parse_local_body_info(body: &[u8]) -> LocalBodyInfo {
    let mut cursor = Cursor::new(body);
    let game_version = game_version_from_string(replay_string(&mut cursor).as_deref());
    let _newline = replay_string(&mut cursor);
    let raw_map = replay_string(&mut cursor);
    let _garbage = replay_string(&mut cursor);
    let Some(_) = replay_u32(&mut cursor) else {
        return LocalBodyInfo {
            game_version,
            map_name: raw_map
                .as_deref()
                .map(extract_map_folder)
                .filter(|m| !m.is_empty()),
            ..Default::default()
        };
    };
    let _sim_mods = parse_replay_lua(&mut cursor, 0);
    let Some(_) = replay_u32(&mut cursor) else {
        return LocalBodyInfo {
            game_version,
            map_name: raw_map
                .as_deref()
                .map(extract_map_folder)
                .filter(|m| !m.is_empty()),
            ..Default::default()
        };
    };
    let game_options = parse_replay_lua(&mut cursor, 0);

    let map_name = game_options
        .as_ref()
        .and_then(|opts| opts.get("ScenarioFile"))
        .and_then(Value::as_str)
        .map(extract_map_folder)
        .filter(|m| !m.is_empty())
        .or_else(|| {
            raw_map
                .as_deref()
                .map(extract_map_folder)
                .filter(|m| !m.is_empty())
        });

    let Some(source_count) = replay_u8(&mut cursor) else {
        return LocalBodyInfo {
            player_stats: HashMap::new(),
            map_name,
            game_version,
        };
    };
    let mut sources = Vec::with_capacity(source_count as usize);
    for _ in 0..source_count {
        let Some(name) = replay_string(&mut cursor) else {
            return LocalBodyInfo {
                player_stats: HashMap::new(),
                map_name,
                game_version,
            };
        };
        let Some(_) = replay_u32(&mut cursor) else {
            return LocalBodyInfo {
                player_stats: HashMap::new(),
                map_name,
                game_version,
            };
        };
        sources.push(name);
    }
    if replay_u8(&mut cursor).is_none() {
        return LocalBodyInfo {
            player_stats: HashMap::new(),
            map_name,
            game_version,
        };
    }
    let Some(army_count) = replay_u8(&mut cursor) else {
        return LocalBodyInfo {
            player_stats: HashMap::new(),
            map_name,
            game_version,
        };
    };
    let mut stats = HashMap::new();
    for _ in 0..army_count {
        if replay_u32(&mut cursor).is_none() {
            break;
        }
        let Some(Value::Object(data)) = parse_replay_lua(&mut cursor, 0) else {
            break;
        };
        let Some(source) = replay_u8(&mut cursor) else {
            break;
        };
        if source != u8::MAX {
            let _ = replay_u8(&mut cursor);
        }
        let name = data
            .get("PlayerName")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| sources.get(source as usize).cloned());
        let Some(name) = name else { continue };
        let faction = data.get("Faction").and_then(replay_i32_value);
        let rating = replay_displayed_rating(&data);
        stats.insert(name, (faction, rating));
    }
    LocalBodyInfo {
        player_stats: stats,
        map_name,
        game_version,
    }
}

async fn read_detailed_info(path: &Path) -> Result<ReplayDetails, String> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let (header, body_bytes) = read_replay_header_and_body(&path)?;
        let mut details = parse_detailed_info_from_body(&body_bytes);
        details.sim_mods = header.as_ref().map(local_sim_mods).unwrap_or_default();
        Ok(details)
    })
    .await
    .map_err(|error| format!("could not parse replay details: {error}"))?
}

/// The decompressed command stream, plus the `.fafreplay` JSON header it came
/// wrapped in. The header is `None` for a legacy `.scfareplay`, which is the
/// bare stream with nothing in front of it.
fn read_replay_header_and_body(path: &Path) -> Result<(Option<Value>, Vec<u8>), String> {
    let file_size = std::fs::metadata(path)
        .map_err(|error| format!("could not inspect replay file {}: {error}", path.display()))?
        .len();
    if file_size > MAX_DOWNLOAD_BYTES {
        return Err(format!(
            "replay file exceeds the allowed size of {} MB",
            MAX_DOWNLOAD_BYTES / (1024 * 1024)
        ));
    }
    let bytes = std::fs::read(path)
        .map_err(|error| format!("could not read replay file {}: {error}", path.display()))?;

    let is_fafreplay = path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("fafreplay"));

    if !is_fafreplay {
        return Ok((None, bytes));
    }
    let (meta, body) = split_fafreplay_bytes(&bytes)?;
    let compression = meta
        .get("compression")
        .and_then(Value::as_str)
        .unwrap_or("qtcompress");
    let body_bytes = decompress_replay_body(body, compression)?;
    Ok((Some(meta), body_bytes))
}

fn split_fafreplay_bytes(bytes: &[u8]) -> Result<(Value, &[u8]), String> {
    let newline_idx = bytes
        .iter()
        .position(|&b| b == b'\n')
        .ok_or_else(|| "corrupted fafreplay: missing newline delimiter".to_string())?;
    let header_slice = &bytes[..newline_idx];
    let body_slice = &bytes[newline_idx + 1..];
    let meta: Value = serde_json::from_slice(header_slice)
        .map_err(|e| format!("corrupted fafreplay header: {e}"))?;
    Ok((meta, body_slice))
}

fn decompress_replay_body(body: &[u8], compression: &str) -> Result<Vec<u8>, String> {
    if compression.eq_ignore_ascii_case("zstd") {
        let decoder = zstd::stream::read::Decoder::new(body)
            .map_err(|e| format!("could not create zstd decoder: {e}"))?;
        let mut out = Vec::new();
        copy_bounded(decoder, &mut out)?;
        Ok(out)
    } else {
        let mut decoded =
            base64::read::DecoderReader::new(body, &base64::engine::general_purpose::STANDARD);
        let mut uncompressed_size = [0; 4];
        decoded
            .read_exact(&mut uncompressed_size)
            .map_err(|e| format!("invalid legacy replay body size: {e}"))?;
        let expected = u32::from_be_bytes(uncompressed_size);
        if u64::from(expected) > MAX_DECOMPRESSED_REPLAY_BYTES {
            return Err("replay expands beyond the allowed size".to_string());
        }
        let zlib = flate2::read::ZlibDecoder::new(decoded);
        let mut out = Vec::new();
        let written = copy_bounded(zlib, &mut out)?;
        if written != u64::from(expected) {
            return Err("legacy replay size does not match its qCompress header".to_string());
        }
        Ok(out)
    }
}

pub fn parse_detailed_info_from_body(body: &[u8]) -> ReplayDetails {
    let mut cursor = Cursor::new(body);
    let game_version = game_version_from_string(replay_string(&mut cursor).as_deref());
    if !skip_replay_bytes(&mut cursor, 3) {
        return replay_details_with_version(game_version);
    }
    let _raw_map = replay_string(&mut cursor);
    if !skip_replay_bytes(&mut cursor, 4) {
        return replay_details_with_version(game_version);
    }
    let Some(_) = replay_u32(&mut cursor) else {
        return replay_details_with_version(game_version);
    };
    if parse_replay_lua(&mut cursor, 0).is_none() {
        return replay_details_with_version(game_version);
    }
    let Some(_) = replay_u32(&mut cursor) else {
        return replay_details_with_version(game_version);
    };
    let game_options_lua = parse_replay_lua(&mut cursor, 0);

    let Some(source_count) = replay_u8(&mut cursor) else {
        return ReplayDetails {
            game_options: extract_game_options(game_options_lua.as_ref(), game_version),
            chat_messages: Vec::new(),
            sim_mods: Vec::new(),
            game_version,
        };
    };

    let mut sources = Vec::with_capacity(source_count as usize);
    for _ in 0..source_count {
        let Some(name) = replay_string(&mut cursor) else {
            break;
        };
        let Some(_) = replay_u32(&mut cursor) else {
            break;
        };
        sources.push(name);
    }
    let _ = replay_u8(&mut cursor);
    let army_count = replay_u8(&mut cursor).unwrap_or(0);

    let mut armies = Vec::with_capacity(army_count as usize);
    for _ in 0..army_count {
        if replay_u32(&mut cursor).is_none() {
            break;
        }
        let Some(Value::Object(data)) = parse_replay_lua(&mut cursor, 0) else {
            break;
        };
        let Some(source) = replay_u8(&mut cursor) else {
            break;
        };
        if source != u8::MAX {
            let _ = replay_u8(&mut cursor);
        }
        let name = data
            .get("PlayerName")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| sources.get(source as usize).cloned())
            .unwrap_or_else(|| format!("Player {}", armies.len() + 1));
        armies.push(name);
    }

    skip_replay_bytes(&mut cursor, 4);

    let game_options = extract_game_options(game_options_lua.as_ref(), game_version);
    let chat_messages = extract_chat_messages(&mut cursor, &armies, &sources);

    ReplayDetails {
        game_options,
        chat_messages,
        // Filled in by the caller, which is the only place that has seen the
        // `.fafreplay` header this body was unwrapped from.
        sim_mods: Vec::new(),
        game_version,
    }
}

fn replay_details_with_version(game_version: Option<i32>) -> ReplayDetails {
    ReplayDetails {
        game_options: extract_game_options(None, game_version),
        chat_messages: Vec::new(),
        sim_mods: Vec::new(),
        game_version,
    }
}

fn extract_game_options(
    game_options_lua: Option<&Value>,
    game_version: Option<i32>,
) -> Vec<ReplayGameOption> {
    let mut options = Vec::new();
    if let Some(v) = game_version {
        options.push(ReplayGameOption {
            key: "FAF Version".to_string(),
            value: v.to_string(),
        });
    }

    let mut collect_options = |map: &serde_json::Map<String, Value>| {
        for (k, v) in map {
            if k == "ScenarioFile" || k == "Options" {
                continue;
            }
            let val_str = match v {
                Value::String(s) => s.clone(),
                Value::Bool(b) => {
                    if *b {
                        "true".to_string()
                    } else {
                        "false".to_string()
                    }
                }
                Value::Number(n) => n.to_string(),
                Value::Null => "null".to_string(),
                Value::Array(_) | Value::Object(_) => format!("{v}"),
            };
            options.push(ReplayGameOption {
                key: k.clone(),
                value: val_str,
            });
        }
    };

    if let Some(Value::Object(top_map)) = game_options_lua {
        if let Some(Value::Object(nested)) = top_map.get("Options") {
            collect_options(nested);
        } else {
            collect_options(top_map);
        }
    }

    options.sort_by_key(|a| a.key.to_lowercase());
    options
}

fn extract_chat_messages(
    cursor: &mut Cursor<&[u8]>,
    armies: &[String],
    sources: &[String],
) -> Vec<ReplayChatMessage> {
    let mut current_ticks: u32 = 0;
    let mut messages: Vec<ReplayChatMessage> = Vec::new();
    let body = *cursor.get_ref();
    let len = body.len();

    while (cursor.position() + 3) <= len as u64 {
        let Some(cmd_type) = replay_u8(cursor) else {
            break;
        };
        let mut len_bytes = [0u8; 2];
        if Read::read_exact(cursor, &mut len_bytes).is_err() {
            break;
        }
        let cmd_len = u16::from_le_bytes(len_bytes) as usize;
        if cmd_len < 3 {
            break;
        }
        let payload_len = cmd_len - 3;
        let pos = cursor.position() as usize;
        if pos + payload_len > len {
            break;
        }
        let payload = &body[pos..pos + payload_len];
        cursor.set_position((pos + payload_len) as u64);

        if cmd_type == 0 {
            // CMDST_ADVANCE
            if payload.len() >= 4 {
                let ticks = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
                current_ticks = current_ticks.saturating_add(ticks);
            } else {
                current_ticks = current_ticks.saturating_add(1);
            }
        } else if cmd_type == 22 || cmd_type == 0x20 || cmd_type == 0x22 {
            // 22 (0x16) is CMDST_LuaSimCallback in Forged Alliance
            if let Some(chat) = try_parse_chat_payload(payload, current_ticks / 10, armies, sources)
            {
                if let Some(last) = messages.last() {
                    if last.sender == chat.sender
                        && last.message == chat.message
                        && chat.time_seconds <= last.time_seconds + 2
                    {
                        continue;
                    }
                }
                messages.push(chat);
            }
        }
    }

    messages
}

fn try_parse_chat_payload(
    payload: &[u8],
    time_seconds: u32,
    armies: &[String],
    sources: &[String],
) -> Option<ReplayChatMessage> {
    let mut p_cursor = Cursor::new(payload);
    let _func = replay_string(&mut p_cursor)?;

    let lua_val = parse_replay_lua(&mut p_cursor, 0)?;
    let Value::Object(args) = lua_val else {
        return None;
    };

    // 1. Check if Msg or text is present
    let mut message_text: Option<String> = None;
    if let Some(Value::Object(msg_map)) = args.get("Msg").or_else(|| args.get("msg")) {
        if let Some(Value::String(s)) = msg_map
            .get("text")
            .or_else(|| msg_map.get("Text"))
            .or_else(|| msg_map.get("msg"))
        {
            message_text = Some(s.clone());
        }
    } else if let Some(Value::String(s)) = args
        .get("Msg")
        .or_else(|| args.get("msg"))
        .or_else(|| args.get("text"))
        .or_else(|| args.get("Text"))
    {
        message_text = Some(s.clone());
    }

    let text = message_text?;
    if text.trim().is_empty() {
        return None;
    }

    // 2. Resolve sender
    let sender = if let Some(Value::String(s)) = args
        .get("Sender")
        .or_else(|| args.get("PlayerName"))
        .or_else(|| args.get("sender"))
    {
        s.clone()
    } else if let Some(n) = args.get("From").and_then(Value::as_i64) {
        // Lua army indices in GiveResourcesToPlayer: 1-based (or 0-based in some mods)
        let idx = if n > 0 { (n - 1) as usize } else { n as usize };
        armies
            .get(idx)
            .or_else(|| sources.get(idx))
            .cloned()
            .unwrap_or_else(|| format!("Player {}", n))
    } else {
        "Unknown".to_string()
    };

    Some(ReplayChatMessage {
        time_seconds,
        sender,
        message: text,
    })
}

fn replay_u8(cursor: &mut Cursor<&[u8]>) -> Option<u8> {
    let mut value = [0; 1];
    Read::read_exact(cursor, &mut value).ok()?;
    Some(value[0])
}

fn replay_u32(cursor: &mut Cursor<&[u8]>) -> Option<u32> {
    let mut value = [0; 4];
    Read::read_exact(cursor, &mut value).ok()?;
    Some(u32::from_le_bytes(value))
}

fn skip_replay_bytes(cursor: &mut Cursor<&[u8]>, count: u64) -> bool {
    cursor.set_position(cursor.position().saturating_add(count));
    cursor.position() <= cursor.get_ref().len() as u64
}

fn replay_string(cursor: &mut Cursor<&[u8]>) -> Option<String> {
    let start = cursor.position() as usize;
    let rest = cursor.get_ref().get(start..)?;
    let end = rest.iter().position(|byte| *byte == 0)?;
    cursor.set_position((start + end + 1) as u64);
    Some(String::from_utf8_lossy(&rest[..end]).into_owned())
}

fn game_version_from_string(version: Option<&str>) -> Option<i32> {
    let version = version?;
    version
        .starts_with("Supreme Commander v1")
        .then(|| version.rsplit('.').next()?.parse().ok())
        .flatten()
}

fn parse_replay_lua(cursor: &mut Cursor<&[u8]>, depth: u8) -> Option<Value> {
    if depth > 64 {
        return None;
    }
    match replay_u8(cursor)? {
        0 => {
            let mut bytes = [0; 4];
            Read::read_exact(cursor, &mut bytes).ok()?;
            serde_json::Number::from_f64(f32::from_le_bytes(bytes) as f64).map(Value::Number)
        }
        1 => Some(Value::String(replay_string(cursor)?)),
        2 => {
            replay_u8(cursor)?;
            Some(Value::Null)
        }
        3 => Some(Value::Bool(replay_u8(cursor)? != 0)),
        4 => {
            let mut object = serde_json::Map::new();
            loop {
                let next = *cursor.get_ref().get(cursor.position() as usize)?;
                if next == 5 {
                    cursor.set_position(cursor.position() + 1);
                    break;
                }
                let key = parse_replay_lua_with_type(cursor, depth + 1, next)?;
                let value = parse_replay_lua(cursor, depth + 1)?;
                let key = key
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| key.to_string());
                object.insert(key, value);
            }
            Some(Value::Object(object))
        }
        _ => None,
    }
}

fn parse_replay_lua_with_type(cursor: &mut Cursor<&[u8]>, depth: u8, kind: u8) -> Option<Value> {
    cursor.set_position(cursor.position() + 1);
    match kind {
        0 => {
            let mut bytes = [0; 4];
            Read::read_exact(cursor, &mut bytes).ok()?;
            serde_json::Number::from_f64(f32::from_le_bytes(bytes) as f64).map(Value::Number)
        }
        1 => Some(Value::String(replay_string(cursor)?)),
        2 => {
            replay_u8(cursor)?;
            Some(Value::Null)
        }
        3 => Some(Value::Bool(replay_u8(cursor)? != 0)),
        4 => {
            cursor.set_position(cursor.position() - 1);
            parse_replay_lua(cursor, depth)
        }
        _ => None,
    }
}

fn replay_i32_value(value: &Value) -> Option<i32> {
    value
        .as_f64()
        .and_then(|number| i32::try_from(number.round() as i64).ok())
}

fn replay_displayed_rating(data: &serde_json::Map<String, Value>) -> Option<i32> {
    let mean = data.get("MEAN").and_then(Value::as_f64)?;
    let deviation = data.get("DEV").and_then(Value::as_f64)?;
    // Truncated for the same reason as `displayed_rating_with_fields`: this is
    // the same number read from a local replay's header instead of the vault,
    // and the two must not disagree about the same player.
    let rating = mean - 3.0 * deviation;
    (rating.is_finite() && rating >= f64::from(i32::MIN) && rating <= f64::from(i32::MAX))
        .then_some(rating as i32)
}

fn empty_local_replay(
    path: &std::path::Path,
    modified: std::time::SystemTime,
    file_size: u64,
    status: LocalReplayStatus,
    watchable: bool,
) -> LocalReplay {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("replay")
        .to_string();
    let title = path
        .file_stem()
        .and_then(|name| name.to_str())
        .unwrap_or("Replay")
        .to_string();
    LocalReplay {
        path: path.display().to_string(),
        file_name,
        uid: None,
        map: String::new(),
        mod_name: guess_mod_from_filename(path),
        title,
        recorder: String::new(),
        start_time: None,
        modified_time: unix_seconds(modified),
        file_size_bytes: file_size.min(u32::MAX as u64) as u32,
        num_players: 0,
        teams: Vec::new(),
        average_rating: None,
        sim_mods: Vec::new(),
        status,
        watchable,
        game_version: None,
    }
}

/// Read the `.fafreplay` JSON envelope and compact binary body header. Bad and
/// incomplete files remain represented so the UI can explain them rather than
/// silently making files disappear from the archive.
async fn read_local_metadata(
    path: &std::path::Path,
    modified: std::time::SystemTime,
    file_size: u64,
) -> LocalReplay {
    let is_legacy = path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("scfareplay"));
    if is_legacy {
        return empty_local_replay(path, modified, file_size, LocalReplayStatus::Legacy, true);
    }

    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(_) => {
            return empty_local_replay(path, modified, file_size, LocalReplayStatus::Broken, false)
        }
    };
    let mut buf = vec![0u8; 64 * 1024];
    let n: usize = file.read(&mut buf).await.unwrap_or_default();
    buf.truncate(n);
    let Some(nl) = buf.iter().position(|&byte| byte == b'\n') else {
        return empty_local_replay(path, modified, file_size, LocalReplayStatus::Broken, false);
    };
    let Ok(header) = serde_json::from_slice::<Value>(&buf[..nl]) else {
        return empty_local_replay(path, modified, file_size, LocalReplayStatus::Broken, false);
    };
    let mut body = buf[nl + 1..].to_vec();
    if body.len() < LOCAL_REPLAY_BODY_READ_BYTES {
        let mut remainder = Vec::new();
        let remaining = (LOCAL_REPLAY_BODY_READ_BYTES - body.len()) as u64;
        let _ = file.take(remaining).read_to_end(&mut remainder).await;
        body.extend_from_slice(&remainder);
    }
    let compression = header
        .get("compression")
        .and_then(Value::as_str)
        .unwrap_or("");
    let body_info = local_body_player_stats(&body, compression);
    let mut teams = local_teams(&header);
    for team in &mut teams {
        for player in &mut team.players {
            if let Some((faction, rating)) = body_info.player_stats.get(&player.name) {
                player.faction = *faction;
                player.rating = *rating;
            }
        }
    }
    let ratings: Vec<i32> = teams
        .iter()
        .filter(|team| team.team != "-1" && team.team != "null")
        .flat_map(|team| &team.players)
        .filter_map(|player| player.rating)
        .collect();
    let average_rating =
        (!ratings.is_empty()).then(|| ratings.iter().sum::<i32>() / ratings.len() as i32);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("replay.fafreplay")
        .to_string();
    let team_player_count = teams
        .iter()
        .filter(|team| team.team != "-1" && team.team != "null")
        .map(|team| team.players.len() as i32)
        .sum();
    let start_time = header
        .get("launched_at")
        .or_else(|| header.get("game_time"))
        .and_then(Value::as_f64)
        .filter(|value| *value > 0.0)
        .map(|value| value.min(u32::MAX as f64).round() as u32);

    let header_map = header
        .get("mapname")
        .and_then(Value::as_str)
        .filter(|m| !m.is_empty() && !m.eq_ignore_ascii_case("none"));
    let map = body_info
        .map_name
        .or_else(|| header_map.map(str::to_string))
        .unwrap_or_default();

    // The envelope's featured-mod version identifies a mod release, not the
    // SupCom patch. Prefer an explicitly named game version, then the binary
    // replay header used by the Java client.
    let game_version = header
        .get("game_version")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .or(body_info.game_version);

    LocalReplay {
        path: path.display().to_string(),
        file_name: file_name.clone(),
        uid: replay_uid(&header, &file_name),
        map,
        mod_name: header
            .get("featured_mod")
            .and_then(Value::as_str)
            .unwrap_or("faf")
            .to_string(),
        title: header
            .get("title")
            .and_then(Value::as_str)
            .filter(|title| !title.is_empty())
            .unwrap_or_else(|| {
                path.file_stem()
                    .and_then(|name| name.to_str())
                    .unwrap_or("Replay")
            })
            .to_string(),
        recorder: header
            .get("recorder")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        start_time,
        modified_time: unix_seconds(modified),
        file_size_bytes: file_size.min(u32::MAX as u64) as u32,
        num_players: header
            .get("num_players")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .filter(|value| *value >= 0)
            .unwrap_or(team_player_count),
        teams,
        average_rating,
        sim_mods: local_sim_mods(&header),
        status: if header
            .get("complete")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            LocalReplayStatus::Complete
        } else {
            LocalReplayStatus::Incomplete
        },
        watchable: true,
        game_version,
    }
}

async fn local_metadata_for_path(path: &Path) -> Result<LocalReplay, String> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|error| format!("could not inspect downloaded replay: {error}"))?;
    let modified = metadata.modified().unwrap_or(std::time::UNIX_EPOCH);
    Ok(read_local_metadata(path, modified, metadata.len()).await)
}

async fn resolve_local_replay_file(dir: &Path, path: &Path) -> Result<PathBuf, String> {
    let canonical_dir = tokio::fs::canonicalize(dir)
        .await
        .map_err(|error| format!("could not resolve replay folder: {error}"))?;
    let canonical_path = tokio::fs::canonicalize(path)
        .await
        .map_err(|error| format!("could not resolve replay file: {error}"))?;
    let valid_extension = canonical_path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            extension.eq_ignore_ascii_case("fafreplay")
                || extension.eq_ignore_ascii_case("scfareplay")
        });
    if canonical_path.parent() != Some(canonical_dir.as_path()) || !valid_extension {
        return Err("refusing to access a file outside the replay folder".to_string());
    }
    Ok(canonical_path)
}

/// Resolve a local-library replay for a narrowly scoped desktop-shell action.
/// The canonical parent and extension checks are shared with deletion so the
/// webview cannot use "Show in folder" as a generic filesystem browser.
pub async fn validated_local_replay_path(path: &Path) -> Result<PathBuf, String> {
    resolve_local_replay_file(&local_replays_dir(), path).await
}

async fn delete_local_file(dir: &std::path::Path, path: &std::path::Path) -> Result<(), String> {
    let canonical_path = resolve_local_replay_file(dir, path).await?;
    tokio::fs::remove_file(&canonical_path)
        .await
        .map_err(|e| format!("could not delete replay: {e}"))
}

/// `game.relationships.mapVersion -> mapVersion.relationships.map -> map.attributes.displayName`.
/// Falls back to `mapVersion.attributes.folderName`, `mapVersion.attributes.description`,
/// `mapVersion.attributes.filename`, or `game.attributes.mapFolderName` / `mapName` when the
/// map relationship is absent (such as for generated mapgen maps or custom scenarios).
fn resolve_map_name(
    game_attributes: &Value,
    relationships: &Value,
    index: &HashMap<(String, String), &JsonApiResource>,
    mod_name: &str,
) -> String {
    if let Some(mv) = rel_target(relationships, "mapVersion").and_then(|k| index.get(&k)) {
        if let Some(folder_name) = mv
            .attributes
            .get("folderName")
            .or_else(|| mv.attributes.get("mapName"))
            .or_else(|| mv.attributes.get("name"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            if is_generated_map(folder_name)
                || folder_name.to_ascii_lowercase().starts_with("neroxis")
            {
                return folder_name.to_string();
            }
        }

        if let Some(map_name) = rel_target(&mv.relationships, "map")
            .and_then(|k| index.get(&k))
            .and_then(|m| m.attributes.get("displayName"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            return map_name.to_string();
        }

        if let Some(folder_name) = mv
            .attributes
            .get("folderName")
            .or_else(|| mv.attributes.get("mapName"))
            .or_else(|| mv.attributes.get("name"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            return folder_name.to_string();
        }

        if let Some(desc) = mv
            .attributes
            .get("description")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            return desc.to_string();
        }

        if let Some(filename) = mv
            .attributes
            .get("filename")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            return filename.to_string();
        }
    }

    for key in [
        "mapFolderName",
        "mapName",
        "map",
        "map_folder_name",
        "map_name",
        "scenarioFile",
        "scenario_file",
    ] {
        if let Some(val) = game_attributes.get(key) {
            if let Some(s) = val.as_str().filter(|s| !s.is_empty()) {
                return s.to_string();
            }
            if let Some(obj) = val.as_object() {
                if let Some(name) = obj
                    .get("displayName")
                    .or_else(|| obj.get("folderName"))
                    .or_else(|| obj.get("mapName"))
                    .or_else(|| obj.get("name"))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    return name.to_string();
                }
            }
        }
    }

    if let Some(title) = game_attributes.get("name").and_then(Value::as_str) {
        if is_generated_map(title) || title.to_ascii_lowercase().starts_with("neroxis") {
            return title.to_string();
        }
    }

    // Mirrors the Python client (src/replays/replayitem.py:257): in FAF API, games played on
    // generated maps do not have a mapVersion relationship (generated maps are never uploaded
    // to the map vault). When mapVersion is absent for non-coop games, it was a generated map.
    if !mod_name.eq_ignore_ascii_case("coop") {
        return "Neroxis Map Generator".to_string();
    }

    "unknown map".to_string()
}

/// `game.relationships.featuredMod -> featuredMod.attributes.technicalName`.
fn resolve_mod_name(
    relationships: &Value,
    index: &HashMap<(String, String), &JsonApiResource>,
) -> String {
    rel_target(relationships, "featuredMod")
        .and_then(|k| index.get(&k))
        .and_then(|m| m.attributes.get("technicalName"))
        .and_then(Value::as_str)
        .unwrap_or("faf")
        .to_string()
}

/// `game.relationships.mapVersion -> mapVersion.attributes.thumbnailUrlSmall`.
/// Same relationship chain as [`resolve_map_name`], but reads straight off
/// the `mapVersion` resource rather than following into `map`: mirrors
/// `infra::maps`'s exact attribute name for map thumbnails.
fn resolve_map_thumbnail(
    relationships: &Value,
    index: &HashMap<(String, String), &JsonApiResource>,
) -> String {
    rel_target(relationships, "mapVersion")
        .and_then(|k| index.get(&k))
        .and_then(|mv| mv.attributes.get("thumbnailUrlSmall"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// `game.relationships.playerStats[] -> playerStats.relationships.player ->
/// player.attributes.login`, grouped by `playerStats.attributes.team`.
///
fn resolve_player_avatar(
    player: Option<&JsonApiResource>,
    index: &HashMap<(String, String), &JsonApiResource>,
) -> Option<String> {
    let player = player?;
    let selected = rel_targets(&player.relationships, "avatarAssignments")
        .into_iter()
        .filter_map(|key| index.get(&key).copied())
        .find(|assignment| {
            assignment
                .attributes
                .get("selected")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        });
    let selected_url = selected
        .and_then(|assignment| rel_target(&assignment.relationships, "avatar"))
        .and_then(|key| index.get(&key).copied())
        .and_then(|avatar| avatar.attributes.get("url"))
        .and_then(Value::as_str)
        .filter(|url| !url.trim().is_empty())
        .map(str::to_string);
    selected_url.or_else(|| {
        player
            .attributes
            .get("avatarUrl")
            .and_then(Value::as_str)
            .filter(|url| !url.trim().is_empty())
            .map(str::to_string)
    })
}

fn resolve_teams(
    relationships: &Value,
    index: &HashMap<(String, String), &JsonApiResource>,
) -> Vec<ReplayTeam> {
    let mut by_team: HashMap<i32, Vec<ReplayPlayer>> = HashMap::new();
    for key in rel_targets(relationships, "playerStats") {
        let Some(stat) = index.get(&key) else {
            continue;
        };
        let team = stat
            .attributes
            .get("team")
            .and_then(team_value)
            .unwrap_or(0);
        let player = rel_target(&stat.relationships, "player").and_then(|k| index.get(&k).copied());
        let name = player
            .and_then(|p| p.attributes.get("login"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let avatar_url = resolve_player_avatar(player, index);
        let faction = stat.attributes.get("faction").and_then(faction_value);
        let journals: Vec<&JsonApiResource> = rel_targets(&stat.relationships, "ratingChanges")
            .into_iter()
            .filter_map(|key| index.get(&key).copied())
            .collect();
        let rating = journals
            .iter()
            .find_map(|journal| displayed_rating_before(&journal.attributes))
            // Older game resources expose the same values directly on the
            // player stat instead of including a rating journal relationship.
            .or_else(|| displayed_rating_from_stat(&stat.attributes));
        // The *first* journal, as Java takes it: a game touches one leaderboard
        // for the player, and the ordering the API returns is the one both
        // clients rely on.
        let rating_change = journals
            .first()
            .and_then(|journal| rating_change_of(&journal.attributes));
        by_team.entry(team).or_default().push(ReplayPlayer {
            name,
            avatar_url,
            faction,
            rating,
            rating_change,
            outcome: stat
                .attributes
                .get("result")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            score: value_i32(&stat.attributes, "score"),
        });
    }
    let mut teams: Vec<ReplayTeam> = by_team
        .into_iter()
        .map(|(team, players)| ReplayTeam { team, players })
        .collect();
    // Observers use the negative team bucket. Keep them visible but after the
    // competitive lineup, matching the reference clients' scoreboards.
    teams.sort_by_key(|team| (team.team < 0, team.team));
    teams
}

/// Match quality uses the same TrueSkill parameters as the Java client. The
/// result is meaningful only for two competitive teams whose first rating
/// journal contains both the mean and deviation.
fn resolve_match_quality(
    relationships: &Value,
    index: &HashMap<(String, String), &JsonApiResource>,
) -> Option<i32> {
    let mut by_team: HashMap<i32, Vec<(f64, f64)>> = HashMap::new();
    for key in rel_targets(relationships, "playerStats") {
        let stat = index.get(&key)?;
        let team = stat.attributes.get("team").and_then(team_value)?;
        if team < 0 {
            continue;
        }
        let (mean, deviation) = rel_targets(&stat.relationships, "ratingChanges")
            .into_iter()
            .filter_map(|rating_key| index.get(&rating_key))
            .find_map(|journal| rating_before(&journal.attributes))?;
        by_team.entry(team).or_default().push((mean, deviation));
    }

    let mut teams: Vec<Vec<(f64, f64)>> = by_team.into_values().collect();
    teams.sort_by_key(|team| team.len());
    calculate_match_quality(&teams)
}

/// The two-team TrueSkill quality formula used by `jskills`. FAF's Java
/// client configures beta to 240, while the other GameInfo parameters affect
/// rating updates rather than this quality calculation.
fn calculate_match_quality(teams: &[Vec<(f64, f64)>]) -> Option<i32> {
    if teams.len() != 2 || teams.iter().any(Vec::is_empty) {
        return None;
    }

    const BETA: f64 = 240.0;
    let total_players = teams.iter().map(Vec::len).sum::<usize>() as f64;
    let beta_term = total_players * BETA * BETA;
    let uncertainty = teams
        .iter()
        .flat_map(|team| team.iter().map(|(_, deviation)| deviation * deviation))
        .sum::<f64>();
    let denominator = beta_term + uncertainty;
    if !denominator.is_finite() || denominator <= 0.0 {
        return None;
    }

    let team_means = teams
        .iter()
        .map(|team| team.iter().map(|(mean, _)| mean).sum::<f64>())
        .collect::<Vec<_>>();
    let mean_difference = team_means[0] - team_means[1];
    let quality = (beta_term / denominator).sqrt()
        * (-(mean_difference * mean_difference) / (2.0 * denominator)).exp();
    (quality.is_finite() && quality >= 0.0)
        .then_some((quality * 100.0).round().clamp(0.0, 100.0) as i32)
}

fn faction_value(value: &Value) -> Option<i32> {
    if let Some(number) = value.as_i64() {
        return i32::try_from(number).ok();
    }
    let value = value.as_str()?.trim();
    value
        .parse()
        .ok()
        .or_else(|| match value.to_ascii_uppercase().as_str() {
            "UEF" => Some(1),
            "AEON" => Some(2),
            "CYBRAN" => Some(3),
            "SERAPHIM" => Some(4),
            "RANDOM" => Some(5),
            _ => None,
        })
}

fn team_value(value: &Value) -> Option<i32> {
    if let Some(number) = value.as_i64() {
        return i32::try_from(number).ok();
    }
    match value {
        Value::Null => Some(-1),
        Value::String(value) if value.trim().eq_ignore_ascii_case("null") => Some(-1),
        Value::String(value) => value.trim().parse().ok(),
        _ => None,
    }
}

fn rating_before(attributes: &Value) -> Option<(f64, f64)> {
    let numeric = |name: &str| {
        attributes.get(name).and_then(|value| {
            value
                .as_f64()
                .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
        })
    };
    Some((numeric("meanBefore")?, numeric("deviationBefore")?))
}

fn displayed_rating_before(attributes: &Value) -> Option<i32> {
    displayed_rating_with_fields(attributes, "meanBefore", "deviationBefore")
}

/// What one rating journal says this game did to the player's displayed rating.
///
/// Both ends go through the same `mean - 3*deviation` rounding before they are
/// subtracted, so the number matches what the two ratings would have read as,
/// rather than a rounded difference of unrounded means. `None` unless the
/// journal carries an "after" side: an unrated or unresolved game has none, and
/// reporting a change for it is exactly the bug this replaced. Mirrors Java's
/// `PlayerCardController::getRatingChange`.
fn rating_change_of(attributes: &Value) -> Option<i32> {
    let after = displayed_rating_with_fields(attributes, "meanAfter", "deviationAfter")?;
    let before = displayed_rating_before(attributes)?;
    Some(after - before)
}

fn displayed_rating_from_stat(attributes: &Value) -> Option<i32> {
    displayed_rating_with_fields(attributes, "beforeMean", "beforeDeviation")
}

fn displayed_rating_with_fields(
    attributes: &Value,
    mean_field: &str,
    deviation_field: &str,
) -> Option<i32> {
    let numeric = |name: &str| {
        attributes.get(name).and_then(|value| {
            value
                .as_f64()
                .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
        })
    };
    let mean = numeric(mean_field)?;
    let deviation = numeric(deviation_field)?;
    let rating = mean - 3.0 * deviation;
    // Truncated, not rounded. Both reference clients cast rather than round:
    // Java's `RatingUtil.getRating` is `(int) (mean - 3f * deviation)` and the
    // Python client's `rating_estimate` is `int(rating.displayed())`. Rounding
    // put us a point above them for every rating whose fraction was over .5,
    // and made a rating *change*, which subtracts two of these, disagree by one
    // in either direction.
    (rating.is_finite() && rating >= f64::from(i32::MIN) && rating <= f64::from(i32::MAX))
        .then_some(rating as i32)
}

/// `game.relationships.reviewsSummary -> reviewsSummary.attributes`.
fn resolve_reviews(
    relationships: &Value,
    index: &HashMap<(String, String), &JsonApiResource>,
) -> (Option<f32>, Option<i32>) {
    let Some(summary) = rel_target(relationships, "reviewsSummary").and_then(|k| index.get(&k))
    else {
        return (None, None);
    };
    let average = summary
        .attributes
        .get("averageScore")
        .and_then(Value::as_f64)
        .map(|v| v as f32);
    let count = summary
        .attributes
        .get("numReviews")
        .and_then(Value::as_i64)
        .map(|v| v as i32);
    (average, count)
}

/// Seconds between two RFC3339 timestamps, or `None` if either is missing or
/// unparseable (e.g. a still-in-progress game has no `endTime`).
fn duration_between(start: &str, end: Option<&str>) -> Option<i32> {
    let start = chrono::DateTime::parse_from_rfc3339(start).ok()?;
    let end = chrono::DateTime::parse_from_rfc3339(end?).ok()?;
    i32::try_from((end - start).num_seconds()).ok()
}

/// An RFC 3339 instant `months` months in the past: the clock half of the
/// query's [`ReplayQuery::fallback_months`] cost guard.
fn months_ago(months: u32) -> String {
    (chrono::Utc::now() - chrono::Months::new(months)).to_rfc3339()
}

/// Technical names from a `/data/featuredMod` document, in the order the API
/// returned them (which `sort=order` makes the mods' own display order).
fn parse_featured_mods(doc: &JsonApiDoc) -> Vec<String> {
    doc.data
        .iter()
        .filter_map(|entry| {
            entry
                .attributes
                .get("technicalName")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .map(str::to_string)
        })
        .collect()
}

fn parse_vault_replays(doc: &JsonApiDoc) -> Vec<VaultReplay> {
    let index = resource_index(&doc.included);
    doc.data
        .iter()
        .filter_map(|game| {
            let uid: i32 = game.id.parse().ok()?;
            let start_time = game
                .attributes
                .get("startTime")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let end_time = game.attributes.get("endTime").and_then(Value::as_str);
            let teams = resolve_teams(&game.relationships, &index);
            let quality = resolve_match_quality(&game.relationships, &index);
            let average_rating = {
                let ratings: Vec<i32> = teams
                    .iter()
                    .filter(|team| team.team >= 0)
                    .flat_map(|t| &t.players)
                    .filter_map(|p| p.rating)
                    .collect();
                if ratings.is_empty() {
                    None
                } else {
                    Some(ratings.iter().sum::<i32>() / ratings.len() as i32)
                }
            };
            let (reviews_average, reviews_count) = resolve_reviews(&game.relationships, &index);
            let title = game
                .attributes
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let mod_name = resolve_mod_name(&game.relationships, &index);
            let map = resolve_map_name(&game.attributes, &game.relationships, &index, &mod_name);
            let mut map_thumbnail_url = resolve_map_thumbnail(&game.relationships, &index);
            if map_thumbnail_url.is_empty()
                && (is_generated_map(&map) || map.to_ascii_lowercase().starts_with("neroxis"))
            {
                map_thumbnail_url = GENERATED_MAP_PLACEHOLDER_URL.to_string();
            }
            // The Java client parses the SupCom patch from the replay body.
            // `featuredModVersion` is a mod release identifier, not the game
            // patch, so it must never be presented as the game version.
            let game_version = value_i32(&game.attributes, "gameVersion");
            Some(VaultReplay {
                uid,
                title,
                map,
                map_thumbnail_url,
                mod_name,
                duration_seconds: duration_between(&start_time, end_time),
                game_duration_seconds: value_i32(&game.attributes, "replayTicks")
                    .and_then(|ticks| (ticks >= 0).then_some(ticks / 10)),
                start_time,
                // Missing/non-bool defaults to "not available": safer than
                // assuming a replay exists when we can't tell.
                replay_available: game
                    .attributes
                    .get("replayAvailable")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                teams,
                average_rating,
                quality,
                reviews_average,
                reviews_count,
                game_version,
                validity: game
                    .attributes
                    .get("validity")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            })
        })
        .collect()
}

/// Wait for the first binary WebSocket frame, ignoring ping/pong/text frames.
async fn wait_for_first_binary(read: &mut SplitStream<WsStream>) -> Result<Vec<u8>, String> {
    let wait = async {
        loop {
            match read.next().await {
                Some(Ok(Message::Binary(data))) => return Some(data),
                Some(Ok(_)) => continue,
                Some(Err(_)) | None => return None,
            }
        }
    };
    match tokio::time::timeout(READY_TIMEOUT, wait).await {
        Ok(Some(data)) => Ok(data),
        Ok(None) => Err("replay server closed the connection before sending data".to_string()),
        Err(_) => Err("timed out waiting for the replay server".to_string()),
    }
}

/// Bidirectionally pipe bytes between FA's local proxy socket and the replay
/// WebSocket (mirrors the Java client's `LiveReplayProxyServer`), starting
/// with the already-buffered first frame.
///
/// The WebSocket side runs in its own task, decoupled from the TCP write via
/// an mpsc queue: mirroring the Python client's `StreamWriter`, which pushes
/// server bytes onto a `Queue` from the Qt event loop and drains it on a
/// separate writer thread. FA is single-threaded and doesn't read from the
/// proxy socket while it's busy loading assets (tens of seconds at startup);
/// without this decoupling, `tcp.write_all` blocking on that meant we also
/// stopped polling the WebSocket, missed its keepalive pings, and the server
/// dropped the connection: the game then hit "Premature EOF" once it finally
/// got around to reading. TCP→WS (the game talking back) doesn't have the
/// same problem since replay playback is receive-only, but it's decoupled the
/// same way for symmetry and to keep this task cheap to reason about.
async fn relay_session(
    tcp: TcpStream,
    mut ws_write: SplitSink<WsStream, Message>,
    mut ws_read: SplitStream<WsStream>,
    first: Vec<u8>,
) {
    let (mut tcp_read, mut tcp_write) = tcp.into_split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    if tx.send(first).is_err() {
        return;
    }

    // WS → queue. Never blocks on the (possibly slow-to-drain) TCP side, so
    // the server's keepalive pings always get answered promptly.
    let ws_to_queue = tokio::spawn(async move {
        while let Some(msg) = ws_read.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if tx.send(data).is_err() {
                        break;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {} // ping/pong/text: ignore
            }
        }
    });

    // Queue → TCP. Can block on FA being busy without affecting the above.
    let queue_to_tcp = tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            if tcp_write.write_all(&data).await.is_err() {
                break;
            }
        }
    });

    // TCP → WS: the game talking back (rare during pure playback, but the
    // wire protocol is nominally bidirectional: see the module docs).
    let tcp_to_ws = tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        loop {
            match tcp_read.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if ws_write
                        .send(Message::Binary(buf[..n].to_vec()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });

    // The session ends as soon as any leg does: the other two are aborted.
    tokio::select! {
        _ = ws_to_queue => {}
        _ = queue_to_tcp => {}
        _ = tcp_to_ws => {}
    }
}

/// The named-pipe counterpart of [`relay_session`].
///
/// One leg fewer than the TCP version: the pipe is opened outbound-only, so
/// there is nothing to read back from FA. The other difference is the ending,
/// which the pipe does not deliver on its own (see [`PIPE_TERMINATOR`]).
#[cfg(windows)]
async fn pipe_relay_session(
    mut pipe: tokio::net::windows::named_pipe::NamedPipeServer,
    mut ws_write: SplitSink<WsStream, Message>,
    mut ws_read: SplitStream<WsStream>,
    first: Vec<u8>,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    if tx.send(first).is_err() {
        return;
    }

    // WS -> queue, exactly as on the TCP path: reading the socket must never
    // wait on FA draining the pipe, or the server's keepalive pings go
    // unanswered while the game is catching up.
    let ws_to_queue = tokio::spawn(async move {
        while let Some(msg) = ws_read.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if tx.send(data).is_err() {
                        break;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {} // ping/pong/text: ignore
            }
        }
    });

    let queue_to_pipe = tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            if pipe.write_all(&data).await.is_err() {
                return;
            }
        }
        // The stream ended rather than the pipe breaking, so FA is still
        // sitting there waiting for more. Unblock it.
        let _ = pipe.write_all(&PIPE_TERMINATOR).await;
        let _ = pipe.shutdown().await;
    });

    tokio::select! {
        _ = ws_to_queue => {}
        _ = queue_to_pipe => {}
    }

    // Nothing reads from FA on this transport, but the socket still has to be
    // closed rather than dropped mid-frame.
    let _ = ws_write.close().await;
}

/// `ladder1v1` isn't a real mod: the Python client folds it to `faf` when
/// building `/init`.
fn normalize_mod(mod_name: &str) -> String {
    if mod_name == "ladder1v1" {
        "faf".to_string()
    } else {
        mod_name.to_string()
    }
}

/// Everything extracted from a `.fafreplay`/`.scfareplay` source needed to
/// launch it and get it right for old replays: the playable `.scfareplay`
/// path, its featured mod, replay id, engine version, required map folder,
/// and required sim mods (all `Option`/empty beyond `path`/`mod_name` since
/// older or malformed files may not carry every field).
struct ScfaReplay {
    path: PathBuf,
    mod_name: String,
    uid: Option<i32>,
    game_version: Option<i32>,
    map_folder: Option<String>,
    sim_mods: Vec<(String, String)>,
    git_sha: Option<String>,
    git_short_sha: Option<String>,
    build_signature: Option<String>,
    version_name: Option<String>,
    launched_at: Option<u64>,
}

/// Resolve a `.fafreplay`/`.scfareplay` source to a playable `.scfareplay`
/// file plus its launch/update metadata.
async fn prepare_scfareplay(path: &std::path::Path) -> Result<ScfaReplay, String> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    match ext.as_str() {
        "fafreplay" => decode_fafreplay(path).await,
        "scfareplay" => {
            let mod_name = guess_mod_from_filename(path);
            let bytes = read_replay_prefix(path).await?;
            Ok(ScfaReplay {
                path: path.to_path_buf(),
                mod_name,
                uid: None,
                game_version: game_updater::extract_game_version(&bytes),
                map_folder: game_updater::extract_map_folder(&bytes),
                sim_mods: Vec::new(),
                git_sha: None,
                git_short_sha: None,
                build_signature: None,
                version_name: None,
                launched_at: None,
            })
        }
        other => Err(format!(
            "don't know how to play '{}': unrecognised extension '{other}'",
            path.display()
        )),
    }
}

/// `.fafreplay` = one JSON header line + `\n` + a compressed `.scfareplay`
/// body. Writes the decompressed body to the cache dir and returns its
/// launch/update metadata.
async fn decode_fafreplay(path: &std::path::Path) -> Result<ScfaReplay, String> {
    decode_fafreplay_to(path, &cache_dir()?).await
}

async fn decode_fafreplay_to(
    path: &std::path::Path,
    output_dir: &std::path::Path,
) -> Result<ScfaReplay, String> {
    let file_size = tokio::fs::metadata(path)
        .await
        .map_err(|e| format!("could not inspect {}: {e}", path.display()))?
        .len();
    if file_size > MAX_DOWNLOAD_BYTES {
        return Err("replay file is larger than the allowed size".into());
    }
    let mut bytes = tokio::fs::read(path)
        .await
        .map_err(|e| format!("could not read {}: {e}", path.display()))?;
    let nl = bytes
        .iter()
        .position(|&b| b == b'\n')
        .ok_or_else(|| "replay file has no header line".to_string())?;
    let header: Value =
        serde_json::from_slice(&bytes[..nl]).map_err(|e| format!("invalid replay header: {e}"))?;

    // Mirrors the Python client's `uncompress()`: `compression == "zstd"` for
    // vault-downloaded replays, anything else (including the `null` locally
    // recorded replays under `%ProgramData%\FAForever\replays` actually carry)
    // falls back to the legacy Qt `qCompress` format.
    let compression = header
        .get("compression")
        .and_then(Value::as_str)
        .unwrap_or("");
    let compressed_body = bytes.split_off(nl + 1);
    let is_zstd = compression == "zstd";
    let mut source_hash = DefaultHasher::new();
    path.hash(&mut source_hash);
    let out_path = output_dir.join(format!("replay_{:016x}.scfareplay", source_hash.finish()));
    if let Some(parent) = out_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("could not create cache dir: {e}"))?;
    }
    let blocking_path = out_path.clone();
    tokio::task::spawn_blocking(move || {
        decode_replay_body_to(&compressed_body, is_zstd, &blocking_path)
    })
    .await
    .map_err(|e| format!("replay decompression task failed: {e}"))??;
    let decompressed_prefix = read_replay_prefix(&out_path).await?;

    let mod_name = header
        .get("featured_mod")
        .and_then(Value::as_str)
        .unwrap_or("faf")
        .to_string();
    let uid = header
        .get("uid")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok());
    let sim_mods = header
        .get("sim_mods")
        .and_then(Value::as_object)
        .map(|obj| {
            obj.iter()
                .filter_map(|(uid, name)| Some((uid.clone(), name.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default();
    let git_sha = header
        .get("git_sha")
        .and_then(Value::as_str)
        .map(String::from);
    let git_short_sha = header
        .get("git_short_sha")
        .and_then(Value::as_str)
        .map(String::from);
    let build_signature = header
        .get("build_signature")
        .and_then(Value::as_str)
        .map(String::from);
    let version_name = header
        .get("version_name")
        .and_then(Value::as_str)
        .map(String::from);
    let launched_at = header
        .get("launched_at")
        .and_then(Value::as_f64)
        .map(|f| f as u64);

    Ok(ScfaReplay {
        path: out_path,
        mod_name,
        uid,
        game_version: game_updater::extract_game_version(&decompressed_prefix),
        map_folder: game_updater::extract_map_folder(&decompressed_prefix),
        sim_mods,
        git_sha,
        git_short_sha,
        build_signature,
        version_name,
        launched_at,
    })
}

const REPLAY_METADATA_PREFIX_BYTES: usize = 64 * 1024;
const MAX_DECOMPRESSED_REPLAY_BYTES: u64 = 1024 * 1024 * 1024;

async fn read_replay_prefix(path: &Path) -> Result<Vec<u8>, String> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let mut bytes = vec![0_u8; REPLAY_METADATA_PREFIX_BYTES];
    let read = file
        .read(&mut bytes)
        .await
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    bytes.truncate(read);
    Ok(bytes)
}

/// Stream a replay body into a private file and publish it only after the full
/// decode succeeds. This bounds both zstd and legacy qCompress expansion and
/// avoids holding the expanded command stream in memory.
fn decode_replay_body_to(body: &[u8], is_zstd: bool, output: &Path) -> Result<(), String> {
    let parent = output
        .parent()
        .ok_or_else(|| "replay cache path has no parent".to_string())?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| format!("could not create replay cache file: {error}"))?;

    let written = if is_zstd {
        let decoder = zstd::stream::read::Decoder::new(body)
            .map_err(|error| format!("could not decompress replay: {error}"))?;
        copy_bounded(decoder, &mut temporary)?
    } else {
        decode_legacy_qcompress_to(body, &mut temporary)?
    };
    if written > MAX_DECOMPRESSED_REPLAY_BYTES {
        return Err("replay expands beyond the allowed size".into());
    }
    temporary
        .flush()
        .map_err(|error| format!("could not finish replay cache file: {error}"))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|error| format!("could not sync replay cache file: {error}"))?;
    temporary
        .persist(output)
        .map(|_| ())
        .map_err(|error| format!("could not publish replay cache file: {}", error.error))
}

fn copy_bounded(
    mut reader: impl std::io::Read,
    writer: &mut impl std::io::Write,
) -> Result<u64, String> {
    let mut limited = reader.by_ref().take(MAX_DECOMPRESSED_REPLAY_BYTES + 1);
    let written = std::io::copy(&mut limited, writer)
        .map_err(|error| format!("could not decompress replay: {error}"))?;
    if written > MAX_DECOMPRESSED_REPLAY_BYTES {
        return Err("replay expands beyond the allowed size".into());
    }
    Ok(written)
}

/// Legacy `.fafreplay` body format: standard base64, decoding to Qt's
/// `qCompress` container: a 4-byte big-endian uncompressed-length prefix
/// followed by a raw zlib stream. Mirrors `qUncompress(QByteArray.fromBase64(..))`
/// in the Python client's `fa/replay.py`.
fn decode_legacy_qcompress_to(
    body: &[u8],
    writer: &mut impl std::io::Write,
) -> Result<u64, String> {
    use std::io::Read as _;

    let mut decoded =
        base64::read::DecoderReader::new(body, &base64::engine::general_purpose::STANDARD);
    let mut prefix = [0_u8; 4];
    decoded
        .read_exact(&mut prefix)
        .map_err(|error| format!("could not read legacy qCompress header: {error}"))?;
    let expected = u32::from_be_bytes(prefix);
    if u64::from(expected) > MAX_DECOMPRESSED_REPLAY_BYTES {
        return Err("replay expands beyond the allowed size".into());
    }
    let written = copy_bounded(flate2::read::ZlibDecoder::new(decoded), writer)?;
    if written != u64::from(expected) {
        return Err("legacy replay size does not match its qCompress header".into());
    }
    Ok(written)
}

/// Legacy `.scfareplay` files carry their mod in the filename, `<name>.<mod>.scfareplay`.
fn guess_mod_from_filename(path: &std::path::Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|stem| stem.rsplit_once('.'))
        .map(|(_, mod_name)| mod_name.to_string())
        .unwrap_or_else(|| "faf".to_string())
}

/// Inert replay client: used offline and in tests. Every call fails cleanly
/// (mirrors [`crate::infra::FakeGame`]'s posture: no game installed, no IO).
#[derive(Debug, Clone, Default)]
pub struct FakeReplay;

#[async_trait]
impl ReplayPort for FakeReplay {
    async fn watch_live(
        &self,
        _target: LiveReplayTarget,
        _player: String,
    ) -> Result<Option<String>, String> {
        Err("replay watching is unavailable in offline mode".to_string())
    }

    async fn play_file(&self, _path: PathBuf) -> Result<Option<String>, String> {
        Err("replay playback is unavailable in offline mode".to_string())
    }

    async fn search_vault(&self, _query: ReplayQuery) -> Result<VaultSearchResult, String> {
        Ok(VaultSearchResult::default())
    }

    async fn list_featured_mods(&self) -> Result<Vec<String>, String> {
        // The well-known set, so the offline path still exercises the filter.
        Ok(["faf", "ladder1v1", "coop", "fafbeta", "nomads"]
            .map(String::from)
            .to_vec())
    }

    async fn watch_vault(&self, _uid: i32) -> Result<Option<String>, String> {
        Err("no game install configured: set it in Settings → Paths".to_string())
    }

    async fn download_vault(&self, _uid: i32) -> Result<LocalReplay, String> {
        Err("replay downloading is unavailable in offline mode".to_string())
    }

    async fn load_details(
        &self,
        _uid: i32,
        _local_path: Option<PathBuf>,
    ) -> Result<ReplayDetails, String> {
        Ok(ReplayDetails {
            game_options: vec![
                ReplayGameOption {
                    key: "FAF Version".to_string(),
                    value: "3837".to_string(),
                },
                ReplayGameOption {
                    key: "AllowObservers".to_string(),
                    value: "true".to_string(),
                },
                ReplayGameOption {
                    key: "AutoTeams".to_string(),
                    value: "tvsb".to_string(),
                },
                ReplayGameOption {
                    key: "CheatsEnabled".to_string(),
                    value: "false".to_string(),
                },
                ReplayGameOption {
                    key: "UnitCap".to_string(),
                    value: "1000".to_string(),
                },
            ],
            chat_messages: vec![
                ReplayChatMessage {
                    time_seconds: 13,
                    sender: "Downlord".to_string(),
                    message: "gl hf".to_string(),
                },
                ReplayChatMessage {
                    time_seconds: 599,
                    sender: "Nojoke".to_string(),
                    message: "gg".to_string(),
                },
            ],
            sim_mods: vec!["No Rush Timer".to_string()],
            game_version: Some(3837),
        })
    }

    async fn list_local(&self, _limit: usize) -> Result<Vec<LocalReplay>, String> {
        Ok(Vec::new())
    }

    async fn delete_local(&self, _path: PathBuf) -> Result<(), String> {
        Err("local replay deletion is disabled".to_string())
    }

    fn set_install_dir(&self, _dir: Option<PathBuf>) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::GeneratorUpdate;
    use faf_domain::state::{
        GeneratorOptionQuery, GeneratorOptions, GeneratorPreset, GeneratorStatus,
    };
    use serde_json::json;

    /// A real generated-map folder name, as it appears in a replay header.
    const GENERATED_MAP: &str = "neroxis_map_generator_1.21.0_ualhhyfgnqw4u_cagaeaakbyaaaqd2";

    /// Records what the launch path asked of the generator. Everything the
    /// replay path never touches is left unimplemented rather than faked, so a
    /// future call through one of those reads as a test bug, not as a pass.
    struct StubGenerator {
        installed: bool,
        outcome: GeneratorStatus,
        asked_for: std::sync::Mutex<Vec<String>>,
    }

    impl StubGenerator {
        fn new(installed: bool, outcome: GeneratorStatus) -> Self {
            Self {
                installed,
                outcome,
                asked_for: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl crate::ports::MapGeneratorPort for StubGenerator {
        fn is_installed(&self, _map_name: &str) -> bool {
            self.installed
        }

        async fn generate_named(&self, map_name: String) -> mpsc::Receiver<GeneratorUpdate> {
            self.asked_for.lock().unwrap().push(map_name);
            let (tx, rx) = mpsc::channel(4);
            let _ = tx.send(GeneratorUpdate::Status(self.outcome.clone())).await;
            rx
        }

        async fn generate(&self, _options: GeneratorOptions) -> mpsc::Receiver<GeneratorUpdate> {
            unimplemented!("the replay path never generates from options")
        }
        async fn query_options(
            &self,
            _query: GeneratorOptionQuery,
            _version: Option<String>,
            _progress: Option<mpsc::Sender<GeneratorUpdate>>,
        ) -> Result<Vec<String>, String> {
            unimplemented!()
        }
        async fn preflight(&self, _options: GeneratorOptions) -> Result<String, String> {
            unimplemented!()
        }
        async fn help(&self, _version: Option<String>) -> Result<String, String> {
            unimplemented!()
        }
        fn cancel(&self) {
            unimplemented!()
        }
        async fn save_preset(
            &self,
            _name: &str,
            _options: &GeneratorOptions,
        ) -> Result<(), String> {
            unimplemented!()
        }
        async fn list_presets(&self) -> Vec<GeneratorPreset> {
            unimplemented!()
        }
        async fn delete_preset(&self, _name: &str) -> Result<(), String> {
            unimplemented!()
        }
        async fn latest_version(&self) -> Result<String, String> {
            unimplemented!()
        }
        async fn available_versions(&self) -> Result<Vec<String>, String> {
            unimplemented!()
        }
        async fn clean_up(&self, _protected: &[String]) -> Result<usize, String> {
            unimplemented!()
        }
        async fn map_previews(
            &self,
            _map_names: &[String],
        ) -> std::collections::HashMap<String, String> {
            unimplemented!()
        }
    }

    fn client_with(generator: Arc<StubGenerator>) -> ReplayClient {
        ReplayClient::new(
            replay_config(),
            TokenStore::new(),
            Arc::new(crate::infra::game::FakeGame),
            generator,
        )
    }

    /// The reported bug: a generated map has no `.vNNNN` suffix, so the vault
    /// staging path classes it as a base-game map and does nothing at all. FA
    /// then aborts the session on a map that was never written to disk.
    #[tokio::test]
    async fn a_replay_on_a_generated_map_rebuilds_it_before_launch() {
        let generator = Arc::new(StubGenerator::new(
            false,
            GeneratorStatus::Generated {
                maps: vec![GENERATED_MAP.to_string()],
            },
        ));
        let client = client_with(generator.clone());

        let warning = client
            .ensure_replay_map(Path::new("unused"), GENERATED_MAP)
            .await
            .expect("generation succeeded, so playback must proceed");

        assert_eq!(warning, None);
        assert_eq!(
            generator.asked_for.lock().unwrap().as_slice(),
            [GENERATED_MAP.to_string()],
            "the generator is the only source for this map; nothing else can supply it"
        );
    }

    #[tokio::test]
    async fn an_installed_generated_map_is_not_regenerated() {
        let generator = Arc::new(StubGenerator::new(
            true,
            GeneratorStatus::Failed {
                reason: "must not run".into(),
            },
        ));
        let client = client_with(generator.clone());

        client
            .ensure_replay_map(Path::new("unused"), GENERATED_MAP)
            .await
            .expect("an installed map needs no work");
        assert!(generator.asked_for.lock().unwrap().is_empty());
    }

    /// Unlike a failed vault download, this cannot be downgraded to a warning:
    /// launching anyway is a guaranteed `aborting session` with nothing on
    /// screen to explain it.
    #[tokio::test]
    async fn a_generated_map_that_cannot_be_produced_stops_the_launch() {
        let generator = Arc::new(StubGenerator::new(
            false,
            GeneratorStatus::Failed {
                reason: "no java".into(),
            },
        ));
        let client = client_with(generator);

        let error = client
            .ensure_replay_map(Path::new("unused"), GENERATED_MAP)
            .await
            .expect_err("an unloadable map must not reach FA");
        assert!(error.contains("no java"), "{error}");
    }

    #[tokio::test]
    async fn generation_disabled_in_settings_is_reported_rather_than_ignored() {
        let generator = Arc::new(StubGenerator::new(
            false,
            GeneratorStatus::Failed {
                reason: "must not run".into(),
            },
        ));
        let client = client_with(generator.clone());
        client.set_auto_generate_maps(false);

        let error = client
            .ensure_replay_map(Path::new("unused"), GENERATED_MAP)
            .await
            .expect_err("the setting forbids the only way to get this map");
        assert!(error.contains("disabled in settings"), "{error}");
        assert!(generator.asked_for.lock().unwrap().is_empty());
    }

    /// A base-game map is neither generated nor in the vault, so it must reach
    /// FA untouched: no generator run, and no CDN request to 404 on.
    #[tokio::test]
    async fn a_base_game_map_needs_neither_the_generator_nor_the_vault() {
        let generator = Arc::new(StubGenerator::new(
            false,
            GeneratorStatus::Failed {
                reason: "must not run".into(),
            },
        ));
        let client = client_with(generator.clone());

        let warning = client
            .ensure_replay_map(Path::new("unused"), "SCMP_009")
            .await
            .expect("a base map is always available");
        assert_eq!(warning, None);
        assert!(generator.asked_for.lock().unwrap().is_empty());
    }

    fn replay_config() -> ReplayConfig {
        ReplayConfig {
            user_api_base: "https://user.faforever.com".into(),
            api_base: "https://api.faforever.com".into(),
            vault_host: "https://replay.faforever.com".into(),
            replay_target_dir: None,
            exe_name: "ForgedAlliance.exe".into(),
            content_base: "https://content.faforever.com".into(),
        }
    }

    #[test]
    fn replay_redirect_chain_accepts_only_the_requested_replay_on_faf_origins() {
        let config = replay_config();
        for accepted in [
            "https://replay.faforever.com/27457062",
            "https://api.faforever.com/game/27457062/replay",
            "https://content.faforever.com/replays/27457062.fafreplay",
            "https://content.faforever.com/replays/0/27/45/70/27457062.fafreplay",
        ] {
            let url = url::Url::parse(accepted).unwrap();
            assert!(
                validate_replay_download_url(&url, 27_457_062, &config).is_ok(),
                "expected {accepted} to be accepted"
            );
        }

        for rejected in [
            "https://evil.invalid/replays/27457062.fafreplay",
            "http://replay.faforever.com/27457062",
            "https://user@replay.faforever.com/27457062",
            "https://api.faforever.com/game/1/replay",
            "https://content.faforever.com/maps/27457062.fafreplay",
            "https://content.faforever.com/replays/1.fafreplay",
            "https://content.faforever.com/replays/27457062.fafreplay/extra",
        ] {
            let url = url::Url::parse(rejected).unwrap();
            assert!(
                validate_replay_download_url(&url, 27_457_062, &config).is_err(),
                "expected {rejected} to be rejected"
            );
        }
    }

    fn lua_string(value: &str) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(value.as_bytes());
        bytes.push(0);
        bytes
    }

    fn lua_number(value: f32) -> Vec<u8> {
        let mut bytes = vec![0];
        bytes.extend_from_slice(&value.to_le_bytes());
        bytes
    }

    fn lua_army() -> Vec<u8> {
        let mut bytes = vec![4];
        bytes.extend(lua_string("PlayerName"));
        bytes.extend(lua_string("TestPlayer"));
        bytes.extend(lua_string("Faction"));
        bytes.extend(lua_number(1.0));
        bytes.extend(lua_string("MEAN"));
        bytes.extend(lua_number(1500.0));
        bytes.extend(lua_string("DEV"));
        bytes.extend(lua_number(100.0));
        bytes.push(5);
        bytes
    }

    fn local_body_with_army() -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(b"Supreme Commander v1.50.3764\0");
        body.extend_from_slice(b"\r\n\0");
        body.extend_from_slice(b"Replay v1.9\r\n/maps/SCMP_009/SCMP_009.scmap\0");
        body.extend_from_slice(b"\0");
        body.extend_from_slice(&0_u32.to_le_bytes());
        body.extend_from_slice(&[4, 5]);
        body.extend_from_slice(&0_u32.to_le_bytes());
        body.extend_from_slice(&[4, 5]);
        body.push(1);
        body.extend_from_slice(b"TestPlayer\0");
        body.extend_from_slice(&u32::MAX.to_le_bytes());
        body.push(0);
        body.push(1);
        body.extend_from_slice(&1_u32.to_le_bytes());
        body.extend(lua_army());
        body.extend_from_slice(&[0, 0]);
        body
    }

    #[test]
    fn local_body_parser_extracts_faction_and_displayed_rating() {
        use base64::Engine as _;
        use std::io::Write as _;

        let body = local_body_with_army();
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&body).unwrap();
        let mut qcompressed = (body.len() as u32).to_be_bytes().to_vec();
        qcompressed.extend(encoder.finish().unwrap());
        let encoded = base64::engine::general_purpose::STANDARD.encode(qcompressed);
        let stats = local_body_player_stats(encoded.as_bytes(), "");
        assert_eq!(
            stats.player_stats.get("TestPlayer"),
            Some(&(Some(1), Some(1200)))
        );
        assert_eq!(stats.map_name.as_deref(), Some("SCMP_009"));
    }

    #[test]
    fn extract_map_folder_handles_replay_version_prefix_and_paths() {
        assert_eq!(
            extract_map_folder("Replay v1.9\r\n/maps/scmp_001/scmp_001_scenario.lua"),
            "scmp_001"
        );
        assert_eq!(
            extract_map_folder(
                "Replay v1.9\r\n/maps/setons_clutch.v0001/setons_clutch_scenario.lua"
            ),
            "setons_clutch.v0001"
        );
        assert_eq!(
            extract_map_folder("Replay v1.9\r\n\\maps\\neroxis_map_generator_1.22.1_abc_xyz\\neroxis_map_generator_1.22.1_abc_xyz_scenario.lua"),
            "neroxis_map_generator_1.22.1_abc_xyz"
        );
        assert_eq!(
            extract_map_folder("/maps/dual_gap_adaptive.v0002/dual_gap_adaptive_scenario.lua"),
            "dual_gap_adaptive.v0002"
        );
        assert_eq!(extract_map_folder("SCMP_009.scmap"), "SCMP_009");
    }

    #[tokio::test]
    async fn list_local_dir_reads_metadata_and_keeps_problem_files_visible() {
        let dir = std::env::temp_dir().join(format!("forge-local-replays-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let header = |uid: i32, complete: bool| {
            format!(
                r#"{{"uid":{uid},"complete":{complete},"mapname":"scmp_009","featured_mod":"faf","title":"t{uid}","recorder":"host","launched_at":1700000000,"num_players":2,"teams":{{"1":["host"],"2":["guest"]}},"sim_mods":{{"mod-1":"UI Party"}}}}"#
            )
        };
        tokio::fs::write(
            dir.join("older.fafreplay"),
            format!("{}\nbody", header(1, true)),
        )
        .await
        .unwrap();
        // Ensure a distinct, later mtime than the first file.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        tokio::fs::write(
            dir.join("newer.fafreplay"),
            format!("{}\nbody", header(2, false)),
        )
        .await
        .unwrap();
        tokio::fs::write(dir.join("corrupt.fafreplay"), b"not even json\nbody")
            .await
            .unwrap();
        tokio::fs::write(dir.join("legacy.faf.scfareplay"), b"legacy replay body")
            .await
            .unwrap();

        let replays = list_local_dir(&dir, LOCAL_REPLAY_PAGE_LIMIT)
            .await
            .expect("should list");
        assert_eq!(replays.len(), 4, "every replay-shaped file stays visible");
        let complete = replays.iter().find(|replay| replay.uid == Some(1)).unwrap();
        assert_eq!(complete.status, LocalReplayStatus::Complete);
        assert_eq!(complete.map, "scmp_009");
        assert_eq!(complete.recorder, "host");
        assert_eq!(complete.num_players, 2);
        assert_eq!(complete.teams.len(), 2);
        assert_eq!(complete.sim_mods, vec!["UI Party"]);
        assert_eq!(complete.start_time, Some(1_700_000_000));

        let incomplete = replays.iter().find(|replay| replay.uid == Some(2)).unwrap();
        assert_eq!(incomplete.status, LocalReplayStatus::Incomplete);
        assert!(incomplete.watchable);

        let broken = replays
            .iter()
            .find(|replay| replay.file_name == "corrupt.fafreplay")
            .unwrap();
        assert_eq!(broken.status, LocalReplayStatus::Broken);
        assert!(!broken.watchable);

        let legacy = replays
            .iter()
            .find(|replay| replay.file_name == "legacy.faf.scfareplay")
            .unwrap();
        assert_eq!(legacy.status, LocalReplayStatus::Legacy);
        assert!(legacy.watchable);
        assert_eq!(legacy.mod_name, "faf");

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    /// A real archive is thousands of files. Listing only the newest hundred
    /// gave the Local tab three pages of a folder holding three thousand
    /// replays, with nothing saying the rest existed.
    #[tokio::test]
    async fn list_local_dir_returns_every_replay_not_just_the_newest_hundred() {
        let dir = std::env::temp_dir().join(format!("forge-local-all-{}", std::process::id()));
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        for index in 0..250 {
            tokio::fs::write(dir.join(format!("{index}.scfareplay")), b"body")
                .await
                .unwrap();
        }

        let replays = list_local_dir(&dir, LOCAL_REPLAY_PAGE_LIMIT).await.unwrap();
        assert_eq!(replays.len(), 250);

        // The limit bounds how many headers are *read*, never how many replays
        // are listed: a smaller limit still returns the whole archive, with the
        // remainder marked as not yet read rather than dropped.
        let bounded = list_local_dir(&dir, 10).await.unwrap();
        assert_eq!(bounded.len(), 250);
        assert_eq!(
            bounded
                .iter()
                .filter(|replay| replay.status == LocalReplayStatus::Unread)
                .count(),
            240
        );

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn list_local_dir_missing_folder_returns_empty() {
        let dir = std::env::temp_dir().join("forge-local-replays-does-not-exist");
        let replays = list_local_dir(&dir, LOCAL_REPLAY_PAGE_LIMIT)
            .await
            .expect("missing dir is not an error");
        assert!(replays.is_empty());
    }

    /// The contract between the two halves of "record a game locally": what
    /// [`crate::infra::replay_recorder`] writes has to be what this module's
    /// archive listing can read. They were written against the same format and
    /// were still incompatible, because the recorder emitted the bare stream and
    /// everything the list shows comes from the JSON header the stream has none
    /// of. Asserting the pair together is the only thing that catches that.
    #[tokio::test]
    async fn a_recorded_replay_lists_with_its_game_details() {
        let dir = std::env::temp_dir().join(format!("forge-recorded-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let metadata = crate::ports::ReplayMetadata {
            uid: 27_619_486,
            recorder: "Nory".into(),
            featured_mod: "faf".into(),
            title: "Turtle bowl".into(),
            map_name: "scmp_009".into(),
            game_type: "custom".into(),
            host: "Nory".into(),
            launched_at: Some(1_700_000_000),
            num_players: 2,
            teams: [
                ("1".to_string(), vec!["Nory".to_string()]),
                ("2".to_string(), vec!["Someone".to_string()]),
            ]
            .into_iter()
            .collect(),
            sim_mods: Default::default(),
            git_sha: None,
            git_short_sha: None,
            signature: None,
            version_name: None,
        };
        let file =
            crate::infra::replay_recorder::build_fafreplay(&metadata, b"body".to_vec(), true)
                .unwrap();
        let path = dir.join("27619486-Nory.fafreplay");
        tokio::fs::write(&path, &file).await.unwrap();

        let replays = list_local_dir(&dir, LOCAL_REPLAY_PAGE_LIMIT).await.unwrap();
        let replay = replays.first().expect("the recording should be listed");
        assert_eq!(replay.uid, Some(27_619_486));
        assert_eq!(replay.title, "Turtle bowl");
        assert_eq!(replay.map, "scmp_009");
        assert_eq!(replay.mod_name, "faf");
        assert_eq!(replay.recorder, "Nory");
        assert_eq!(replay.start_time, Some(1_700_000_000));
        assert_eq!(replay.num_players, 2);
        assert_eq!(replay.teams.len(), 2);
        assert_eq!(replay.status, LocalReplayStatus::Complete);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn delete_local_file_is_scoped_to_the_replay_folder() {
        let root = std::env::temp_dir().join(format!("forge-local-delete-{}", std::process::id()));
        let replay_dir = root.join("replays");
        tokio::fs::create_dir_all(&replay_dir).await.unwrap();
        let replay = replay_dir.join("42.fafreplay");
        let outside = root.join("keep.fafreplay");
        tokio::fs::write(&replay, b"replay").await.unwrap();
        tokio::fs::write(&outside, b"keep").await.unwrap();

        assert!(delete_local_file(&replay_dir, &outside).await.is_err());
        assert!(outside.exists());
        delete_local_file(&replay_dir, &replay).await.unwrap();
        assert!(!replay.exists());

        let _ = tokio::fs::remove_dir_all(&root).await;
    }

    #[test]
    fn replay_downloads_are_published_atomically_and_can_be_replaced() {
        let directory = tempfile::tempdir().expect("temporary replay directory");
        let path = directory.path().join("42.fafreplay");

        write_replay_atomically(&path, b"first").unwrap();
        write_replay_atomically(&path, b"second").unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"second");
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn local_replay_resolution_returns_only_direct_replay_children() {
        let directory = tempfile::tempdir().expect("temporary replay directory");
        let replay_dir = directory.path().join("replays");
        let nested_dir = replay_dir.join("nested");
        tokio::fs::create_dir_all(&nested_dir).await.unwrap();
        let replay = replay_dir.join("42.fafreplay");
        let nested = nested_dir.join("43.fafreplay");
        let text = replay_dir.join("notes.txt");
        tokio::fs::write(&replay, b"replay").await.unwrap();
        tokio::fs::write(&nested, b"nested").await.unwrap();
        tokio::fs::write(&text, b"notes").await.unwrap();

        assert_eq!(
            resolve_local_replay_file(&replay_dir, &replay)
                .await
                .unwrap(),
            tokio::fs::canonicalize(&replay).await.unwrap()
        );
        assert!(resolve_local_replay_file(&replay_dir, &nested)
            .await
            .is_err());
        assert!(resolve_local_replay_file(&replay_dir, &text).await.is_err());
    }

    #[test]
    fn parses_vault_replays_resolving_map_and_mod_through_included() {
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [
                {
                    "type": "game",
                    "id": "12345",
                    "attributes": {
                        "name": "all welcome",
                        "startTime": "2026-01-01T12:00:00Z",
                        "endTime": "2026-01-01T12:30:00Z",
                        "replayTicks": 12345,
                        "replayAvailable": true,
                        "featuredModVersion": 999,
                    },
                    "relationships": {
                        "mapVersion": { "data": { "type": "mapVersion", "id": "9" } },
                        "featuredMod": { "data": { "type": "featuredMod", "id": "1" } },
                        "playerStats": { "data": [
                            { "type": "gamePlayerStats", "id": "100" },
                            { "type": "gamePlayerStats", "id": "101" },
                            { "type": "gamePlayerStats", "id": "102" },
                        ] },
                        "reviewsSummary": { "data": { "type": "gameReviewsSummary", "id": "12345" } },
                    },
                },
            ],
            "included": [
                {
                    "type": "mapVersion",
                    "id": "9",
                    "attributes": { "thumbnailUrlSmall": "https://content.faforever.com/maps/scmp_009.small.png" },
                    "relationships": {
                        "map": { "data": { "type": "map", "id": "77" } },
                    },
                },
                {
                    "type": "map",
                    "id": "77",
                    "attributes": { "displayName": "Seton's Clutch" },
                },
                {
                    "type": "featuredMod",
                    "id": "1",
                    "attributes": { "technicalName": "faf" },
                },
                {
                    "type": "gamePlayerStats",
                    "id": "100",
                    "attributes": { "team": 2, "faction": 1, "result": "VICTORY", "score": 3210 },
                    "relationships": {
                        "player": { "data": { "type": "player", "id": "500" } },
                        "ratingChanges": { "data": [{ "type": "leaderboardRatingJournal", "id": "700" }] }
                    },
                },
                {
                    "type": "gamePlayerStats",
                    "id": "101",
                    "attributes": {
                        "team": 3,
                        "faction": "RANDOM",
                        "beforeMean": 1600.0,
                        "beforeDeviation": 100.0
                    },
                    "relationships": { "player": { "data": { "type": "player", "id": "501" } } },
                },
                {
                    "type": "gamePlayerStats",
                    "id": "102",
                    "attributes": { "team": "null", "faction": "SERAPHIM" },
                    "relationships": { "player": { "data": { "type": "player", "id": "502" } } },
                },
                {
                    "type": "player",
                    "id": "500",
                    "attributes": { "login": "Seraphim-Noob" },
                    "relationships": {
                        "avatarAssignments": { "data": [{ "type": "avatarAssignment", "id": "900" }] }
                    }
                },
                { "type": "player", "id": "501", "attributes": { "login": "Nomander" } },
                { "type": "player", "id": "502", "attributes": { "login": "Watcher" } },
                {
                    "type": "avatarAssignment",
                    "id": "900",
                    "attributes": { "selected": true },
                    "relationships": { "avatar": { "data": { "type": "avatar", "id": "901" } } }
                },
                {
                    "type": "avatar",
                    "id": "901",
                    "attributes": { "url": "https://content.faforever.com/faf/avatars/GW_Seraphim.png" }
                },
                {
                    "type": "leaderboardRatingJournal",
                    "id": "700",
                    "attributes": {
                        "meanBefore": 1600.0, "deviationBefore": 100.0,
                        "meanAfter": 1615.0, "deviationAfter": 98.0
                    }
                },
                {
                    "type": "gameReviewsSummary",
                    "id": "12345",
                    "attributes": { "averageScore": 4.5, "numReviews": 2 },
                },
            ],
        }))
        .unwrap();

        let replays = parse_vault_replays(&doc);
        assert_eq!(replays.len(), 1);
        let replay = &replays[0];
        assert_eq!(replay.uid, 12345);
        assert_eq!(replay.title, "all welcome");
        assert_eq!(replay.map, "Seton's Clutch");
        assert_eq!(
            replay.map_thumbnail_url,
            "https://content.faforever.com/maps/scmp_009.small.png"
        );
        assert_eq!(replay.mod_name, "faf");
        assert_eq!(replay.start_time, "2026-01-01T12:00:00Z");
        assert!(replay.replay_available);
        assert_eq!(replay.duration_seconds, Some(1800));
        assert_eq!(replay.game_duration_seconds, Some(1234));
        assert_eq!(replay.reviews_average, Some(4.5));
        assert_eq!(replay.reviews_count, Some(2));
        assert_eq!(replay.game_version, None);
        assert_eq!(replay.teams.len(), 3);
        assert_eq!(replay.teams[0].team, 2);
        assert_eq!(replay.teams[0].players[0].name, "Seraphim-Noob");
        assert_eq!(
            replay.teams[0].players[0].avatar_url.as_deref(),
            Some("https://content.faforever.com/faf/avatars/GW_Seraphim.png")
        );
        assert_eq!(replay.teams[0].players[0].faction, Some(1));
        assert_eq!(replay.teams[0].players[0].rating, Some(1300));
        // 1615 - 3*98 = 1321, against 1600 - 3*100 = 1300.
        assert_eq!(replay.teams[0].players[0].rating_change, Some(21));
        assert_eq!(replay.teams[0].players[0].outcome, "VICTORY");
        assert_eq!(replay.teams[0].players[0].score, Some(3210));
        assert_eq!(replay.average_rating, Some(1300));
        assert_eq!(replay.teams[1].team, 3);
        assert_eq!(replay.teams[1].players[0].name, "Nomander");
        assert_eq!(replay.teams[1].players[0].faction, Some(5));
        assert_eq!(replay.teams[1].players[0].rating, Some(1300));
        // No journal at all, so nothing to report: the old stat-level fields
        // carry a rating but never a change.
        assert_eq!(replay.teams[1].players[0].rating_change, None);
        assert_eq!(replay.teams[2].team, -1);
        assert_eq!(replay.teams[2].players[0].name, "Watcher");
    }

    #[test]
    fn a_rating_change_needs_both_ends_of_the_journal() {
        // Both ends are rounded the way a displayed rating is before they are
        // subtracted, so the number matches the two ratings a player sees.
        let rated = json!({
            "meanBefore": 1500.0, "deviationBefore": 90.0,
            "meanAfter": 1520.0, "deviationAfter": 88.0
        });
        assert_eq!(rating_change_of(&rated), Some(26));

        // An unrated or unresolved game has no "after" side. Reporting a
        // change for one is the bug this replaced: the score was shown
        // instead, which exists even for a game the server never resolved.
        let unrated = json!({ "meanBefore": 1500.0, "deviationBefore": 90.0 });
        assert_eq!(rating_change_of(&unrated), None);
        assert_eq!(rating_change_of(&json!({})), None);
    }

    #[test]
    fn match_quality_matches_two_team_true_skill_shape() {
        let even = calculate_match_quality(&[vec![(1500.0, 100.0)], vec![(1500.0, 100.0)]])
            .expect("two rated teams have a quality");
        assert!(even > 90, "even teams should be high quality, got {even}");

        let uneven = calculate_match_quality(&[vec![(1900.0, 100.0)], vec![(1100.0, 100.0)]])
            .expect("two rated teams have a quality");
        assert!(uneven < even);
        assert_eq!(calculate_match_quality(&[vec![(1500.0, 100.0)]]), None);
    }

    #[test]
    fn parse_vault_replays_defaults_gracefully_without_included() {
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [{ "type": "game", "id": "1", "attributes": {}, "relationships": {} }],
        }))
        .unwrap();

        let replays = parse_vault_replays(&doc);
        assert_eq!(replays.len(), 1);
        let replay = &replays[0];
        assert_eq!(replay.title, "");
        assert_eq!(replay.map, "Neroxis Map Generator");
        assert_eq!(replay.map_thumbnail_url, GENERATED_MAP_PLACEHOLDER_URL);
        assert_eq!(replay.mod_name, "faf");
        assert_eq!(replay.start_time, "");
        assert!(
            !replay.replay_available,
            "missing attribute defaults to unavailable"
        );
        assert_eq!(replay.duration_seconds, None);
        assert!(replay.teams.is_empty());
        assert_eq!(replay.average_rating, None);
        assert_eq!(replay.reviews_average, None);
        assert_eq!(replay.reviews_count, None);
    }

    #[test]
    fn parse_vault_replays_resolves_generated_map() {
        let doc: JsonApiDoc = serde_json::from_value(json!({
            "data": [{
                "type": "game",
                "id": "27634581",
                "attributes": {
                    "name": "2v2",
                    "replayAvailable": true
                },
                "relationships": {
                    "mapVersion": { "data": { "type": "mapVersion", "id": "999" } }
                }
            }],
            "included": [
                {
                    "type": "mapVersion",
                    "id": "999",
                    "attributes": {
                        "folderName": "neroxis_map_generator_1.21.2_ybufyzg64pai2_aqfqeai_aaaaaadkqocko"
                    },
                    "relationships": {}
                }
            ]
        }))
        .unwrap();

        let replays = parse_vault_replays(&doc);
        assert_eq!(replays.len(), 1);
        let replay = &replays[0];
        assert_eq!(replay.uid, 27634581);
        assert_eq!(replay.title, "2v2");
        assert_eq!(
            replay.map,
            "neroxis_map_generator_1.21.2_ybufyzg64pai2_aqfqeai_aaaaaadkqocko"
        );
        assert_eq!(replay.map_thumbnail_url, GENERATED_MAP_PLACEHOLDER_URL);
    }

    #[test]
    fn normalizes_ladder1v1_to_faf() {
        assert_eq!(normalize_mod("ladder1v1"), "faf");
        assert_eq!(normalize_mod("faf"), "faf");
        assert_eq!(normalize_mod("murderparty"), "murderparty");
    }

    #[test]
    fn guesses_mod_from_legacy_filename() {
        assert_eq!(
            guess_mod_from_filename(std::path::Path::new("12345.faf.scfareplay")),
            "faf"
        );
        // No embedded mod segment: falls back to "faf".
        assert_eq!(
            guess_mod_from_filename(std::path::Path::new("12345.scfareplay")),
            "faf"
        );
    }

    #[tokio::test]
    async fn fake_replay_fails_cleanly() {
        let fake = FakeReplay;
        let target = LiveReplayTarget {
            uid: 1,
            mod_name: "faf".into(),
            map: "scmp_007".into(),
        };
        assert!(fake.watch_live(target, "spectator".into()).await.is_err());
        assert!(fake.play_file(PathBuf::from("x.fafreplay")).await.is_err());
    }

    #[test]
    fn legacy_replay_rejects_an_oversized_advertised_expansion() {
        use base64::Engine as _;

        let dir = tempfile::tempdir().expect("temporary replay directory");
        let output = dir.path().join("expanded.scfareplay");
        let body = base64::engine::general_purpose::STANDARD
            .encode(((MAX_DECOMPRESSED_REPLAY_BYTES + 1) as u32).to_be_bytes());

        let error = decode_replay_body_to(body.as_bytes(), false, &output)
            .expect_err("oversized replay must fail");

        assert!(error.contains("expands beyond"));
        assert!(!output.exists(), "a rejected replay must not be published");
    }

    #[tokio::test]
    async fn decode_fafreplay_handles_legacy_and_zstd_bodies() {
        use base64::Engine as _;
        use std::io::Write as _;

        // qCompress = 4-byte big-endian uncompressed length + raw zlib stream,
        // then the whole thing is base64-wrapped for the `.fafreplay` body.
        let payload = b"fake-scfareplay-legacy-bytes";
        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        zlib.write_all(payload).unwrap();
        let compressed = zlib.finish().unwrap();
        let mut qcompressed = (payload.len() as u32).to_be_bytes().to_vec();
        qcompressed.extend_from_slice(&compressed);
        let body = base64::engine::general_purpose::STANDARD.encode(&qcompressed);

        let dir = std::env::temp_dir().join(format!("forge-replay-test-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("legacy.fafreplay");
        let mut file = br#"{"compression":null,"featured_mod":"faf","featured_mod_version":123,"uid":777,"sim_mods":{"abc-123":"Economy Unit Logger"}}"#.to_vec();
        file.push(b'\n');
        file.extend_from_slice(body.as_bytes());
        tokio::fs::write(&path, &file).await.unwrap();

        let replay = decode_fafreplay_to(&path, &dir.join("cache"))
            .await
            .expect("should decode");
        assert_eq!(replay.mod_name, "faf");
        assert_eq!(replay.uid, Some(777));
        assert_eq!(
            replay.game_version, None,
            "test payload has no SupCom version string"
        );
        assert_eq!(
            replay.sim_mods,
            vec![("abc-123".to_string(), "Economy Unit Logger".to_string())]
        );
        let written = tokio::fs::read(&replay.path).await.unwrap();
        assert_eq!(written, payload);
        let legacy_cache_path = replay.path.clone();

        let _ = tokio::fs::remove_dir_all(&dir).await;

        // A second source gets a distinct deterministic cache path.
        let dir =
            std::env::temp_dir().join(format!("forge-replay-test-{}", std::process::id() + 1));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("real.fafreplay");

        let scfa_body = [
            b"Supreme Commander v1.50.3828\0".as_slice(),
            b"\0",
            b"Replay v1.9\r\n/maps/adaptive_gadostb.v0002/adaptive_gadostb.scmap\0",
            b"garbage\0fake-scfareplay-bytes",
        ]
        .concat();
        let zbody = zstd::stream::encode_all(&scfa_body[..], 0).unwrap();
        let mut file = Vec::new();
        file.extend_from_slice(br#"{"compression":"zstd","featured_mod":"faf","uid":12345}"#);
        file.push(b'\n');
        file.extend_from_slice(&zbody);
        tokio::fs::write(&path, &file).await.unwrap();

        let replay = decode_fafreplay_to(&path, &dir.join("cache"))
            .await
            .expect("should decode");
        assert_eq!(replay.mod_name, "faf");
        assert_eq!(replay.uid, Some(12345));
        assert_eq!(replay.game_version, Some(3828));
        assert_eq!(replay.map_folder.as_deref(), Some("adaptive_gadostb.v0002"));
        assert_ne!(replay.path, legacy_cache_path);
        let written = tokio::fs::read(&replay.path).await.unwrap();
        assert_eq!(written, scfa_body);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_parse_detailed_replay_from_test_file() {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = manifest_dir.parent().unwrap().parent().unwrap();
        let test_file =
            workspace_root.join("context/java_client/src/test/resources/replay/test.fafreplay");
        if !test_file.exists() {
            return;
        }

        let details = read_detailed_info(&test_file)
            .await
            .expect("should parse details");
        assert!(
            !details.game_options.is_empty(),
            "game options should not be empty"
        );
        let version_opt = details.game_options.iter().find(|o| o.key == "FAF Version");
        assert!(version_opt.is_some(), "FAF Version should be present");
        assert_eq!(version_opt.unwrap().value, "3675");
        assert_eq!(details.game_version, Some(3675));

        println!(
            "Chat messages count in test.fafreplay: {}",
            details.chat_messages.len()
        );
        for msg in &details.chat_messages {
            println!(
                "Chat: [{}] {}: {}",
                msg.time_seconds, msg.sender, msg.message
            );
        }

        // Verify common options are present
        let allow_observers = details
            .game_options
            .iter()
            .find(|o| o.key == "AllowObservers");
        assert!(allow_observers.is_some());

        assert!(
            !details.chat_messages.is_empty(),
            "the detail parser should retain recorded in-game chat"
        );
    }
}
