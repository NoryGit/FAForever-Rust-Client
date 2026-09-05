//! Infrastructure: the only place that performs real IO.
//!
//! Concrete implementations of the [`crate::ports`] traits. Nothing outside this
//! module does IO directly (ARCHITECTURE.md §2 dependency rule).
//!
//! Auth now has a real provider ([`OAuthAuth`], FAF Ory Hydra) alongside the
//! offline [`FakeAuth`]. The real lobby and chat providers are selected for a
//! normal account session; the complete fake bundle remains available for
//! offline development (ARCHITECTURE.md §2).

/// The account the offline bundle is signed in as.
///
/// One identity for every fake, because the real system has one: the login, the
/// player API and the tournament service all describe the same person. They used
/// to disagree: login `42 TestCommander`, tournaments `101 Nuggets`, player card
/// `106 TestCommander`, and that is precisely why the fakes could not reveal
/// that the tournament tab took its identity from a `viewer` field the real
/// service never sends. Every fake handed out its own answer, so nothing ever
/// needed the login.
///
/// It matches an entry in `FakePlayerCard`'s account list, so the avatar and
/// rating resolve offline too.
pub const OFFLINE_FAF_ID: i32 = 101;
/// The login that goes with [`OFFLINE_FAF_ID`].
pub const OFFLINE_FAF_NAME: &str = "Nuggets";

pub mod auth;
pub mod changelog;
pub mod chat;
pub mod client_update;
pub mod coop;
pub mod discord;
pub(crate) mod faf_content;
pub mod galactic_war;
pub mod game;
pub mod game_logs;
pub mod game_updater;
pub mod guides;
pub(crate) mod http;
pub mod ice_java;
pub mod ice_pioneer;
pub mod ice_select;
pub mod irc;
pub(crate) mod irc_session;
pub(crate) mod java_runtime;
pub(crate) mod jsonapi;
pub mod jsonrpc;
pub mod leaderboard;
pub mod lobby;
pub mod lobby_ws;
pub mod map_generator;
pub mod maps;
pub mod mods;
pub mod oauth;
pub mod paths;
pub mod player_card;
pub mod relay;
pub mod replay;
pub(crate) mod replay_recorder;
pub mod reporting;
pub mod reviews;
pub mod session;
pub mod settings_fake;
pub mod settings_file;
pub mod tourney;
pub mod tourney_fake;
pub mod training;
pub mod tutorials;
pub mod updater;
pub mod uploads;
pub(crate) mod vault_install;

pub use auth::FakeAuth;
pub use changelog::{ChangelogClient, ChangelogConfig, FakeChangelog};
pub use chat::FakeChat;
pub use client_update::{ClientUpdateConfig, FakeClientUpdates, GitHubUpdates};
pub use coop::{CoopClient, CoopConfig, FakeCoop};
pub use discord::{DiscordClient, DiscordConfig, FakeDiscord};
pub use galactic_war::{FakeGalacticWar, GalacticWarConfig, GalacticWarGateway};
pub use game::{FakeGame, GameConfig, GameProcess};
pub use guides::{FakeGuides, GuidesClient, GuidesConfig};
pub use ice_java::{JavaAdapter, JavaConfig};
pub use ice_pioneer::{FakeIce, IceConfig, PioneerAdapter};
pub use ice_select::SelectableIce;
pub use irc::{IrcClient, IrcConfig};
pub use leaderboard::{FakeLeaderboard, LeaderboardClient, LeaderboardConfig};
pub use lobby::FakeLobby;
pub use lobby_ws::{LobbyClient, LobbyConfig};
pub use map_generator::{FakeMapGenerator, MapGeneratorConfig, NeroxisMapGenerator};
pub use maps::{FakeMaps, MapsClient, MapsConfig};
pub use mods::{FakeMods, ModsClient, ModsConfig};
pub use oauth::{OAuthAuth, OAuthConfig};
pub use paths::{ConfiguredPaths, FakePaths};
pub use player_card::{FakePlayerCard, PlayerCardClient, PlayerCardConfig};
pub use relay::{GpgRelayServer, RelayChannels};
pub use replay::{FakeReplay, ReplayClient, ReplayConfig};
pub use reporting::{FakeReporting, ReportingClient, ReportingConfig};
pub use reviews::{FakeReviews, ReviewsClient, ReviewsConfig};
pub use session::TokenStore;
pub use settings_fake::FakeSettings;
pub use settings_file::FileSettings;
pub use tourney::{TourneyClient, TourneyConfig};
pub use tourney_fake::FakeTourney;
pub use training::{FakeTraining, TrainingCatalogueClient, TrainingConfig};
pub use tutorials::{FakeTutorials, TutorialsClient, TutorialsConfig};
pub use updater::{FakeGameUpdater, GameUpdaterClient, UpdaterConfig};
pub use uploads::{FakeUploads, UploadsClient, UploadsConfig};

use std::sync::Arc;

use serde_json::Value;

use crate::ports::{
    ChatPort, GameUpdaterPort, IcePort, LobbyPort, MapGeneratorPort, MapsPort, ModsPort, Ports,
    ProcessPort, ReplayPort,
};

const MAX_ACCESS_RESPONSE_BYTES: u64 = 1024 * 1024;

/// Decide whether a console-subsystem child gets a window of its own.
///
/// The client is a GUI process, so Windows gives every console child it spawns
/// (`java.exe` for the ICE adapter and the map generator, the Go adapter,
/// `faf-uid`) a console window. That black window popping up on game start is
/// what users report as a debugger appearing; the output in it is already
/// captured and logged.
///
/// `show` is not merely the absence of that suppression, which is why this
/// takes a flag rather than being two callers of one hide function: a child
/// left to its own devices inherits the terminal's console in a development
/// build and is given none at all in the packaged one, so a debug switch that
/// only stopped suppressing would open nothing in either. Asking for a new
/// console makes the switch mean the same thing in both builds.
#[cfg(windows)]
pub(crate) fn console_window(command: &mut tokio::process::Command, show: bool) {
    /// From `processthreadsapi.h`. Tokio exposes `creation_flags` directly on
    /// Windows, so no extension trait is needed.
    const CREATE_NEW_CONSOLE: u32 = 0x0000_0010;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    command.creation_flags(if show {
        CREATE_NEW_CONSOLE
    } else {
        CREATE_NO_WINDOW
    });
}

#[cfg(not(windows))]
pub(crate) fn console_window(_command: &mut tokio::process::Command, _show: bool) {}

/// [`console_window`] for the helpers that never have a window to offer.
pub(crate) fn hide_console(command: &mut tokio::process::Command) {
    console_window(command, false);
}

/// Reserve a free loopback TCP port by binding then dropping. Used by the adapter
/// backends to pick GPGNet/RPC ports for subprocesses. Mirrors the Python client's
/// `tcp_server()` helper; the brief gap before the subprocess binds is the same
/// small race it accepts.
pub(crate) fn free_port() -> Option<u16> {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .ok()
        .and_then(|l| l.local_addr().ok())
        .map(|addr| addr.port())
}

/// On-disk identity. One definition, because these were duplicated across six
/// files and the product had already been renamed out from under them: the
/// bundle is `FAForever Client` / `com.faforever.rustclient` while every path
/// still said `forgeclient`/`forge-client`.
pub(crate) const APP_QUALIFIER: &str = "com";
pub(crate) const APP_ORGANIZATION: &str = "FAForever";
pub(crate) const APP_NAME: &str = "FAForever Client";

/// Short, space-free form for places that cannot take a display name: temp
/// directories, the keyring service, and the HTTP user agent.
pub(crate) const APP_SLUG: &str = "faforever-client";

/// Frontend asset used for generated maps until a generated preview is ready.
pub(crate) const GENERATED_MAP_PLACEHOLDER_URL: &str = "/assets/mapgen-placeholder.png";

/// What the client called itself before the rename. Only
/// [`migrate_legacy_directories`] and the keyring fallback read these.
const LEGACY_ORGANIZATION: &str = "forgeclient";
const LEGACY_NAME: &str = "forge-client";
pub(crate) const LEGACY_APP_SLUG: &str = "forge-client";

pub(crate) fn project_dirs() -> Option<directories::ProjectDirs> {
    directories::ProjectDirs::from(APP_QUALIFIER, APP_ORGANIZATION, APP_NAME)
}

fn legacy_project_dirs() -> Option<directories::ProjectDirs> {
    directories::ProjectDirs::from(APP_QUALIFIER, LEGACY_ORGANIZATION, LEGACY_NAME)
}

/// The client's cache root. Holds downloaded replays, game logs, and the
/// content-addressed featured-mod file cache: which the replay and live-game
/// updaters deliberately share, so patching for a live game also satisfies the
/// next replay at that version without re-downloading.
pub fn cache_dir() -> Result<std::path::PathBuf, String> {
    project_dirs()
        .map(|dirs| dirs.cache_dir().to_path_buf())
        .ok_or_else(|| "could not resolve a cache directory".to_string())
}

/// The client's data root: things the user would be annoyed to lose.
///
/// Distinct from [`cache_dir`] on purpose. A cache is disposable by
/// definition, and an installed application that a cache cleaner may delete
/// under the user is not an installation.
pub(crate) fn data_dir() -> Result<std::path::PathBuf, String> {
    project_dirs()
        .map(|dirs| dirs.data_dir().to_path_buf())
        .ok_or_else(|| "could not resolve a data directory".to_string())
}

/// Move the pre-rename directories into place, once, at startup.
///
/// A *move*, not a copy: the cache holds the content-addressed featured-mod
/// store, which is routinely hundreds of megabytes, and duplicating it to keep
/// a downgrade working is not worth the disk. The old client is not expected to
/// run again.
///
/// Every step is best effort and skipped when the new location already exists,
/// so this can never overwrite live data, and a failure leaves the old
/// directory untouched for a manual move. Running it a second time is a no-op.
pub fn migrate_legacy_directories() {
    let (Some(new), Some(old)) = (project_dirs(), legacy_project_dirs()) else {
        return;
    };
    for (from, to, what) in [
        (old.cache_dir(), new.cache_dir(), "cache"),
        (old.data_dir(), new.data_dir(), "data"),
        (old.config_dir(), new.config_dir(), "config"),
    ] {
        if migrate_directory(from, to) {
            tracing::info!(?from, ?to, "migrated the {what} directory");
        } else if from.exists() && !to.exists() {
            tracing::warn!(
                ?from,
                ?to,
                "could not migrate the {what} directory; it was left in place"
            );
        }
    }
}

/// Read an env var, falling back to `fallback` if unset or empty. Shared by the
/// lobby and replay clients for their `*_from_env`/`faf()` constructors.
pub(crate) fn env_or(key: &str, fallback: impl Into<String>) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| fallback.into())
}

/// A folder (or file) the client writes to and the user may want to open.
///
/// The Java client reveals six such locations from its main menu
/// (`menu.revealMapFolder` and friends). It matters more here: this client
/// stages maps into a vault path, writes mods beside them, keeps replays in a
/// shared FAF directory and patches a `game.prefs` none of which a user would
/// guess. Resolution lives here rather than in the shell so the paths cannot
/// drift from the ones the adapters actually use.
pub fn sanitize_folder_name(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            other => other,
        })
        .collect()
}

pub fn client_folder(kind: &str) -> Result<std::path::PathBuf, String> {
    let path = match kind {
        "maps" => faf_content::vault_dir().join("maps"),
        "mods" => faf_content::vault_dir().join("mods"),
        "replays" => replay::local_replays_dir(),
        "vault" => faf_content::vault_dir(),
        "gameCache" | "cache" => {
            let root = cache_dir()?;
            let versions = root.join("versions");
            if versions.is_dir() {
                versions
            } else {
                root.join("game_files")
            }
        }
        // A file, not a directory: the shell reveals it in its parent.
        "gamePrefs" => mods::game_prefs_path(),
        other => return Err(format!("unknown folder '{other}'")),
    };
    Ok(path)
}

/// Move `from` to `to` only when `to` does not exist yet.
///
/// Split out from [`migrate_legacy_directories`] so the "never overwrite live
/// data, never fail loudly" rule is testable without touching the real
/// per-user directories.
fn migrate_directory(from: &std::path::Path, to: &std::path::Path) -> bool {
    if !from.exists() || to.exists() {
        return false;
    }
    if let Some(parent) = to.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            return false;
        }
    }
    std::fs::rename(from, to).is_ok()
}

/// The user's language, as the OS reports it, or empty when it cannot be read.
///
/// Used to pick FAF's language channel (see `faf_domain::state::language_channel`),
/// which the Python client selects the same way. Deliberately env-var only, so
/// this needs no platform crate: `LC_ALL`/`LC_MESSAGES`/`LANG` are set on Linux
/// and macOS and are normally *unset* on Windows, where the account's country
/// flag is the fallback instead. `FAF_LANGUAGE` overrides both, which is also
/// the only way to exercise this in a test without touching the environment.
pub(crate) fn os_language() -> String {
    for key in ["FAF_LANGUAGE", "LC_ALL", "LC_MESSAGES", "LANG"] {
        let value = env_or(key, "");
        // "C" and "POSIX" are the absence of a locale, not a language.
        if !value.is_empty() && value != "C" && value != "POSIX" {
            return value;
        }
    }
    String::new()
}

/// Ask the FAF user API for a verified WebSocket access URL. Shared by the
/// lobby (`/lobby/access`) and replay (`/replay/access`) clients: both return
/// `{"accessUrl": "wss://…"}` (or the JSON:API-nested form) for a bearer token.
pub(crate) async fn fetch_access_url(
    http: &reqwest::Client,
    user_api_base: &str,
    path: &str,
    token: &str,
) -> Result<String, String> {
    let resp = http
        .get(format!("{user_api_base}{path}"))
        .bearer_auth(token)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|e| format!("request failed: {e}"))?;

    let status = resp.status();
    let body = vault_install::bounded_body(resp, "access URL response", MAX_ACCESS_RESPONSE_BYTES)
        .await
        .and_then(|bytes| {
            String::from_utf8(bytes)
                .map_err(|_| "access URL response was not valid UTF-8".to_string())
        })?;
    if !status.is_success() {
        // This error is logged by connection supervisors. Do not echo an
        // untrusted proxy/server body into local diagnostics; the status and
        // endpoint are enough to act on, and bodies can contain internal data.
        return Err(format!("{path} returned {status}"));
    }

    let value: Value = serde_json::from_str(&body).map_err(|e| format!("invalid JSON: {e}"))?;
    extract_access_url(&value).ok_or_else(|| "response had no accessUrl".to_string())
}

/// Pull `accessUrl` out of an `/…/access` response (top-level or JSON:API).
pub(crate) fn extract_access_url(value: &Value) -> Option<String> {
    value
        .get("accessUrl")
        .or_else(|| value.get("data").and_then(|d| d.get("accessUrl")))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Ensure there is a `/` path between the authority and the query, so a URL like
/// `wss://host?verify=x` becomes `wss://host/?verify=x`. Leaves URLs that already
/// have a path untouched, and never modifies the query (the verify token).
pub(crate) fn ensure_ws_path(raw: &str) -> String {
    if let Some(scheme_end) = raw.find("://") {
        let after = &raw[scheme_end + 3..];
        if let Some(q) = after.find('?') {
            if !after[..q].contains('/') {
                return format!("{}://{}/{}", &raw[..scheme_end], &after[..q], &after[q..]);
            }
        }
    }
    raw.to_string()
}

/// Validate a server-provided WebSocket endpoint before connecting to it.
/// Verification tokens must never cross a plaintext remote connection;
/// loopback `ws://` remains available for explicit local integration tests.
pub(crate) fn validated_ws_url(raw: &str) -> Result<String, String> {
    let normalized = ensure_ws_path(raw);
    let url = url::Url::parse(&normalized)
        .map_err(|_| "the access service returned an invalid WebSocket URL".to_string())?;
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err("the access service returned an unsafe WebSocket URL".into());
    }

    let loopback = url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    if url.scheme() != "wss" && !(url.scheme() == "ws" && loopback) {
        return Err("remote WebSocket connections must use wss://".into());
    }
    Ok(url.to_string())
}

/// Build a [`Ports`] bundle backed entirely by fakes. Fully offline; used by tests.
pub fn fake_ports() -> Ports {
    Ports {
        auth: Arc::new(FakeAuth::default()),
        chat: Arc::new(FakeChat::default()),
        coop: Arc::new(FakeCoop),
        discord: Arc::new(FakeDiscord),
        lobby: Arc::new(FakeLobby::default()),
        settings: Arc::new(FakeSettings::default()),
        ice: Arc::new(FakeIce),
        process: Arc::new(FakeGame),
        updater: Arc::new(FakeGameUpdater),
        replay: Arc::new(FakeReplay),
        maps: Arc::new(FakeMaps),
        map_generator: Arc::new(FakeMapGenerator),
        mods: Arc::new(FakeMods),
        paths: Arc::new(FakePaths),
        leaderboard: Arc::new(FakeLeaderboard),
        player_card: Arc::new(FakePlayerCard),
        reporting: Arc::new(FakeReporting),
        reviews: Arc::new(FakeReviews::default()),
        tourney: Arc::new(FakeTourney::default()),
        training: Arc::new(FakeTraining),
        guides: Arc::new(FakeGuides),
        tutorials: Arc::new(FakeTutorials),
        changelog: Arc::new(FakeChangelog),
        uploads: Arc::new(FakeUploads),
        client_update: Arc::new(FakeClientUpdates),
        galactic_war: Arc::new(FakeGalacticWar),
        offline_auth: true,
        // Deliberately not read from the environment: a test must not depend on
        // the locale of the machine running it. The same applies to the roles
        // below, which would otherwise make role-gated assertions pass or fail
        // depending on the developer's shell.
        os_language: String::new(),
        test_login_roles: Vec::new(),
    }
}

/// Build a [`Ports`] bundle with the real OAuth2 auth provider, sharing one
/// [`TokenStore`] so the lobby and chat clients authenticate with the logged-in
/// token. This mirrors the reference clients: after OAuth succeeds, the UI
/// connects to the live services automatically.
///
/// Set `FAF_FAKE_LOBBY=1` or `FAF_FAKE_CHAT=1` to keep one service local while
/// developing against the real account flow. `FAF_REAL_LOBBY` and
/// `FAF_REAL_CHAT` remain accepted as backwards-compatible no-op overrides;
/// real services are the default whenever `FAF_FAKE_AUTH` is not enabled.
pub fn real_ports() -> Ports {
    let tokens = TokenStore::new();
    let lobby: Arc<dyn LobbyPort> = if env_enabled("FAF_FAKE_LOBBY") {
        Arc::new(FakeLobby::default())
    } else {
        Arc::new(LobbyClient::faf(tokens.clone()))
    };

    // The real chat client speaks live IRC against production Ergochat. It is
    // selected by default for a real account; `FAF_FAKE_CHAT=1` is the local
    // override for development.
    let chat: Arc<dyn ChatPort> = if env_enabled("FAF_FAKE_CHAT") {
        Arc::new(FakeChat::default())
    } else {
        Arc::new(IrcClient::faf(tokens.clone()))
    };

    // An authenticated client is a game launcher, not merely a browser. This
    // used to be hidden behind `FAF_REAL_LAUNCH`, which installed inert ports
    // in ordinary production sessions: the server accepted the join and sent
    // `game_launch`, then the client intentionally did nothing. Offline
    // development already selects `fake_ports` through `FAF_FAKE_AUTH`, so a
    // real account always gets the real launch chain.
    let ice: Arc<dyn IcePort> = select_ice_adapter(&tokens);
    let process: Arc<dyn ProcessPort> = Arc::new(GameProcess::faf());

    // Follows `process`, not the other ports: patching writes into the live
    // install and only ever runs on the launch path, which is itself inert
    // without real launch. Sharing the same `process` handle is the point,
    // it patches exactly the install that is about to be started, including
    // after the user repoints it in Settings.
    let updater: Arc<dyn GameUpdaterPort> =
        Arc::new(GameUpdaterClient::faf(tokens.clone(), process.clone()));

    // The map generator is always real. It does spawn a subprocess (Java), but
    // unlike the ice/process pair it needs no FA install and no adapter: and
    // gating it would recreate exactly the bug that made the replay vault
    // unreachable: matchmaker pools contain generated maps, so a client that
    // cannot generate cannot start a ladder game. Missing Java surfaces as a
    // clear per-run error instead.
    let map_generator: Arc<dyn MapGeneratorPort> = Arc::new(NeroxisMapGenerator::faf());

    // Browsing the replay vault (`/data/game`) and listing
    // local `.fafreplay` files are a pure API read and a directory scan: they
    // need no game install and no subprocess, exactly like the maps/mods/
    // leaderboard ports below. These capabilities therefore stay available
    // even when no local game path has been configured.
    //
    // Playback still needs an install, and that is enforced where it belongs:
    // `GameProcess::launch_replay` fails with a message pointing at
    // Settings → Paths when `replay_game_path` is unset or gone. So the replay
    // client always shares the real process port (one child-tracking slot, so
    // relaunching kills the previous FA).
    //
    // It shares the map generator for the same reason the launcher holds one: a
    // replay recorded on a generated map has no vault archive to stage, so the
    // only way to put that map on disk before playback is to run the generator
    // again.
    let replay: Arc<dyn ReplayPort> = Arc::new(ReplayClient::faf(
        tokens.clone(),
        process.clone(),
        map_generator.clone(),
    ));

    // Vault browsing + local install management is pure API + filesystem,
    // no subprocess; it just needs the same bearer token.
    let maps: Arc<dyn MapsPort> = Arc::new(MapsClient::faf(tokens.clone()));

    // Same posture as maps: pure API + filesystem (game.prefs), no subprocess.
    let mods: Arc<dyn ModsPort> = Arc::new(ModsClient::faf(tokens.clone()));

    // Same posture as maps: pure API, no subprocess.
    let leaderboard: Arc<dyn crate::ports::LeaderboardPort> =
        Arc::new(LeaderboardClient::faf(tokens.clone()));
    let player_card: Arc<dyn crate::ports::PlayerCardPort> =
        Arc::new(PlayerCardClient::faf(tokens.clone()));
    let reporting: Arc<dyn crate::ports::ReportingPort> =
        Arc::new(ReportingClient::faf(tokens.clone()));
    let tourney: Arc<dyn crate::ports::TourneyPort> = Arc::new(TourneyClient::faf(tokens.clone()));
    // Same posture as maps and tournaments: pure API reads, no subprocess.
    let coop: Arc<dyn crate::ports::CoopPort> = Arc::new(CoopClient::faf(tokens.clone()));
    let tutorials: Arc<dyn crate::ports::TutorialsPort> =
        Arc::new(TutorialsClient::faf(tokens.clone()));
    // A plain document, and not FAF's: the training catalogue is a manifest the
    // training team publishes, so no token and no gating. Falls back to the
    // catalogue shipped with the client when none is configured.
    let training: Arc<dyn crate::ports::TrainingPort> = Arc::new(TrainingCatalogueClient::faf());
    // The write side of the same catalogue. Its own identity (GitHub's) and its
    // own keyring entry; unconfigured until an OAuth client id is supplied, in
    // which case it still reads the public submission queue.
    let guides: Arc<dyn crate::ports::GuidesPort> = Arc::new(GuidesClient::faf());
    // Public static documents, so no token and no gating: like the update check
    // below, this works before login.
    let changelog: Arc<dyn crate::ports::ChangelogPort> = Arc::new(ChangelogClient::faf());
    let reviews: Arc<dyn crate::ports::ReviewsPort> = Arc::new(ReviewsClient::faf(tokens.clone()));
    let uploads: Arc<dyn crate::ports::UploadsPort> = Arc::new(UploadsClient::faf(tokens.clone()));

    // Never gated, and never authenticated: the release list is a public
    // GitHub endpoint, so the update check works before login and in an
    // offline-ish session, which is when a broken client most needs replacing.
    let client_update: Arc<dyn crate::ports::ClientUpdatePort> = Arc::new(GitHubUpdates::faf());

    // Also never gated and never authenticated: Galactic War has its own
    // login, so downloading and starting it needs nothing from this session.
    // Falls back to the inert port only when no data directory can be
    // resolved, which is the one case where installing anywhere is wrong.
    let galactic_war: Arc<dyn crate::ports::GalacticWarPort> = match GalacticWarGateway::faf() {
        Ok(gateway) => Arc::new(gateway),
        Err(_) => Arc::new(FakeGalacticWar),
    };

    // Always real, and never gated: Rich Presence needs no account, no install
    // and no subprocess: just a local socket that is usually not there. When
    // Discord is not running it is a reconnect timer and nothing else, and the
    // user-facing off switch is the `discord.enabled` preference, which the
    // presence watcher honours before anything is published.
    let discord: Arc<dyn crate::ports::DiscordPort> = Arc::new(DiscordClient::faf());

    Ports {
        auth: Arc::new(OAuthAuth::new(OAuthConfig::from_env(), tokens)),
        chat,
        coop,
        discord,
        lobby,
        settings: Arc::new(FileSettings::faf()),
        ice,
        process,
        updater,
        replay,
        maps,
        map_generator,
        mods,
        paths: Arc::new(ConfiguredPaths),
        leaderboard,
        player_card,
        reporting,
        reviews,
        tourney,
        training,
        guides,
        tutorials,
        changelog,
        uploads,
        client_update,
        galactic_war,
        offline_auth: false,
        os_language: os_language(),
        test_login_roles: oauth::roles_from_env(),
    }
}

/// Build the connectivity backend.
///
/// Both adapters are constructed and handed to [`SelectableIce`], so the
/// Settings toggle changes which one starts the next game without a restart.
fn select_ice_adapter(tokens: &TokenStore) -> Arc<dyn IcePort> {
    Arc::new(SelectableIce::new(
        Arc::new(JavaAdapter::faf(tokens.clone())),
        Arc::new(PioneerAdapter::faf(tokens.clone())),
    ))
}

/// Treat a non-empty environment variable as enabled, matching the existing
/// local-development toggles (`FAF_FAKE_AUTH=1`, etc.).
fn env_enabled(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| !value.is_empty())
}

/// Pick the port bundle the shell should use. Defaults to real auth; set
/// `FAF_FAKE_AUTH=1` to run fully offline (no browser login) for local dev.
pub fn ports_from_env() -> Ports {
    if std::env::var("FAF_FAKE_AUTH").is_ok_and(|v| !v.is_empty()) {
        // Fake auth is the recommended local UI workflow, but game-location
        // settings are local capabilities rather than network services. Using
        // FakeSettings/FakeGame here made Browse appear to work while dropping
        // the choice immediately and permanently reporting "not installed".
        // Keep external services inert, while exercising the same persisted
        // paths and install discovery as a release build.
        let mut ports = fake_ports();
        ports.settings = Arc::new(FileSettings::faf());
        ports.process = Arc::new(GameProcess::faf());
        // Same reason the real provider honours it: role-gated UI has to be
        // reachable offline, and the roles authorise nothing on their own.
        let roles = oauth::roles_from_env();
        if !roles.is_empty() {
            ports.auth = Arc::new(FakeAuth::with_roles(roles.clone()));
            ports.test_login_roles = roles;
        }
        ports
    } else {
        real_ports()
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn the_migration_never_overwrites_an_existing_directory() {
        let root = std::env::temp_dir().join(format!("faf-migrate-{}", std::process::id()));
        let old = root.join("old");
        let new = root.join("new");
        std::fs::create_dir_all(old.join("inner")).unwrap();
        std::fs::write(old.join("inner/keep.txt"), b"old").unwrap();
        std::fs::create_dir_all(&new).unwrap();
        std::fs::write(new.join("live.txt"), b"new").unwrap();

        assert!(
            !migrate_directory(&old, &new),
            "a populated target is left alone"
        );
        assert!(
            old.join("inner/keep.txt").exists(),
            "the source is untouched"
        );
        assert_eq!(std::fs::read(new.join("live.txt")).unwrap(), b"new");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_migration_moves_the_tree_when_the_target_is_absent() {
        let root = std::env::temp_dir().join(format!("faf-migrate-move-{}", std::process::id()));
        let old = root.join("old");
        let new = root.join("new");
        std::fs::create_dir_all(old.join("inner")).unwrap();
        std::fs::write(old.join("inner/keep.txt"), b"payload").unwrap();

        assert!(migrate_directory(&old, &new));
        assert_eq!(
            std::fs::read(new.join("inner/keep.txt")).unwrap(),
            b"payload"
        );
        assert!(!old.exists(), "a move, not a copy: the cache can be large");
        // Idempotent: a second launch finds nothing to do.
        assert!(!migrate_directory(&old, &new));

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_new_identity_differs_from_the_old_one_everywhere() {
        assert_ne!(APP_ORGANIZATION, LEGACY_ORGANIZATION);
        assert_ne!(APP_NAME, LEGACY_NAME);
        assert_ne!(APP_SLUG, LEGACY_APP_SLUG);
        // The slug goes into a User-Agent product token and a keyring service
        // name; neither tolerates a space.
        assert!(!APP_SLUG.contains(' '));
    }
}
