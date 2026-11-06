//! Real lobby provider: the FAF lobby WebSocket protocol.
//!
//! Connects to `wss://ws.faforever.com`, performs the session handshake and
//! streams the open-games list behind the same [`LobbyPort`] the fake implements.
//! The connection lifecycle (and graceful teardown via `disconnect`) lives here;
//! the lobby service, slice and UI are unchanged.
//!
//! ## Protocol
//! First, `GET {api}/lobby/access` (bearer token) returns `{ accessUrl }`: a
//! one-time verified `wss://…/?verify=…` URL (connecting to the bare host 403s).
//! Then, newline-delimited JSON messages in WebSocket text frames, keyed by
//! `command`:
//!
//! 1. → `ask_session { version, user_agent }`
//! 2. ← `session { session }`
//! 3. → `auth { token, unique_id, session }`
//! 4. ← `welcome { me }` (or `authentication_failure` / `authentication_failed`)
//! 5. ← `game_info { … }` / `game_info { games: [ … ] }`: pushed continuously
//!
//! Since 2026-10-21 the socket ends at `faf-message-broker`, not at the lobby
//! server. The broker answers steps 1 to 3 itself and relays everything else
//! over RabbitMQ; `welcome` is the lobby server's, and arrives once it has
//! picked the new session up.
//!
//! ## `unique_id` (anti-smurf)
//! The lobby `auth` also requires a `unique_id`: a machine fingerprint produced by
//! FAF's `faf-uid` executable (we can't reproduce its encryption, but we don't need
//! to: we run the official binary). We invoke `faf-uid <session>` and use its
//! stdout. The binary path comes from `FAF_UID_PATH`; otherwise the provider
//! searches the development/package `natives` directory and finally `PATH`.
//! The live provider is selected for normal account sessions; without a working
//! `faf-uid`, lobby auth fails and the stream ends.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use faf_domain::protocol::rating::{displayed_points, whole_points};
use faf_domain::state::{
    AvailableAvatar, Game, GameLaunch, HostGameConfig, MatchmakerQueue, MatchmakingState,
    PartyMember, PartyState, PlayerLobbyRating, PlayerProfile, PlayerVeto, RatingRange, Relation,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::infra::session::TokenStore;
use crate::infra::{env_or, fetch_access_url, validated_ws_url};
use crate::ports::{LobbyPort, LobbyUpdate, ServerNoticeStyle};

const MAX_SERVER_MESSAGE_CHARS: usize = 4_000;
// Protocol identifier sent in the lobby handshake. This value is stored by
// the lobby server and shown on player cards to distinguish client types.
const LOBBY_USER_AGENT: &str = "faf-rust-client";

/// Reconnect schedule, from `handle_disconnected` in the Python client's
/// `ServerConnection`: the first few attempts go out immediately, because a
/// socket that has been working and then drops is usually a blip, and only a
/// run of failures backs off. The same shape the chat client already uses.
const RECONNECT_IMMEDIATE_ATTEMPTS: u32 = 3;
const RECONNECT_BACKOFF_STEP: std::time::Duration = std::time::Duration::from_secs(10);
const RECONNECT_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(60);
/// Give up, and close the update stream, after this many attempts in a row
/// that never reached an authenticated session: a bad token or a real outage
/// rather than a blip, and retrying forever would hide it.
const MAX_CONSECUTIVE_FAILURES: u32 = 10;

/// How long the lobby connection may stay silent before this client pings it,
/// and how long after that ping it may stay silent before the connection is
/// treated as dead.
///
/// Ten seconds because that is `keepalive_interval` in the Python client's
/// `ServerConnection`, and the interval has to be short enough to beat the
/// shortest idle timeout a home router is likely to apply to an open flow.
const LOBBY_KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// How long a connection may take from an open socket to `welcome` before it
/// is given up and opened again.
///
/// The lobby server used to enforce this itself (`LOGIN_TIMEOUT`, five
/// minutes) by closing the socket. The broker has no such limit, and it keeps
/// the socket open while the lobby server behind it is down: the sign-in the
/// broker announced in that time is lost, and the lobby server, once it is
/// back, takes the session over without a `welcome`. A client that waited for
/// one then waited for good, with no lobbies to show. A fresh connection is
/// announced again, and gets one. A minute leaves room for `faf-uid`, which
/// needs fifteen seconds on a cold first run.
const SIGN_IN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Configuration for the real lobby client.
#[derive(Debug, Clone)]
pub struct LobbyConfig {
    /// Explicit WebSocket URL override (e.g. a local test server). Empty means
    /// "derive a verified URL from the API" via `/lobby/access`, which FAF prod
    /// requires: connecting to the bare `wss://…` host returns 403.
    pub ws_url: String,
    /// FAF *user* API base (`user.faforever.com`), which serves `/lobby/access`.
    /// Note this is a different host from the main `api.faforever.com`.
    pub user_api_base: String,
    /// FAF data API base (`api.faforever.com`), for querying offline accounts.
    pub api_base: String,
    /// Client version reported in `ask_session`.
    pub version: String,
    /// Path to the `faf-uid` executable used to compute `unique_id`.
    pub uid_path: String,
}

impl LobbyConfig {
    pub fn faf() -> Self {
        Self {
            // Empty by default → fetch a verified URL from /lobby/access.
            ws_url: std::env::var("FAF_LOBBY_URL")
                .ok()
                .filter(|v| !v.is_empty())
                .unwrap_or_default(),
            user_api_base: env_or("FAF_USER_API_BASE", "https://user.faforever.com"),
            api_base: env_or("FAF_API_BASE", "https://api.faforever.com"),
            version: env_or("FAF_CLIENT_VERSION", env!("CARGO_PKG_VERSION")),
            uid_path: env_or("FAF_UID_PATH", default_uid_path()),
        }
    }
}

fn default_uid_path() -> String {
    let executable = if cfg!(windows) {
        "faf-uid.exe"
    } else if cfg!(target_os = "macos") {
        "faf-uid-macos"
    } else {
        "faf-uid"
    };

    let mut candidates = Vec::<PathBuf>::new();
    for root in crate::infra::helper_search_roots() {
        candidates.push(root.join("natives").join(executable));
        candidates.push(root.join(executable));
    }

    candidates
        .into_iter()
        .find(|path| path.is_file())
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|| executable.to_string())
}

/// See where the outgoing channel is made, in [`LobbyClient::connect`].
const OUTGOING_FRAME_CAPACITY: usize = 1024;

enum Outgoing {
    Frame(Value),
    ContactRelation(faf_domain::state::Contact, bool),
    /// Local lookup results, never serialized to the lobby protocol.
    ContactNames(Vec<(i64, String)>),
}

pub struct LobbyClient {
    config: LobbyConfig,
    tokens: TokenStore,
    http: reqwest::Client,
    cancel: Arc<Mutex<Option<CancellationToken>>>,
    /// Outgoing frames for the live connection. `join` pushes a `game_join` here;
    /// `run_session` drains it and writes to the socket. `None` when not connected.
    outgoing: Arc<Mutex<Option<mpsc::Sender<Outgoing>>>>,
}

impl LobbyClient {
    pub fn new(config: LobbyConfig, tokens: TokenStore) -> Self {
        Self {
            config,
            tokens,
            http: super::http::shared_http_client(),
            cancel: Arc::new(Mutex::new(None)),
            outgoing: Arc::new(Mutex::new(None)),
        }
    }

    pub fn faf(tokens: TokenStore) -> Self {
        Self::new(LobbyConfig::faf(), tokens)
    }

    /// Push a JSON frame onto the live connection's outgoing channel. Returns
    /// `false` if there is no active connection (so callers can log).
    fn send_frame(&self, frame: Value) -> bool {
        self.send(Outgoing::Frame(frame))
    }

    fn send(&self, request: Outgoing) -> bool {
        let guard = self.outgoing.lock().unwrap();
        let Some(tx) = guard.as_ref() else {
            return false;
        };
        match tx.try_send(request) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(request)) => {
                // Every caller logs a `false` as "disconnected", which is not
                // what happened here, so this says what did.
                tracing::warn!(
                    command = match &request {
                        Outgoing::Frame(frame) => command_of(frame),
                        Outgoing::ContactRelation(_, _) => "social",
                        Outgoing::ContactNames(_) => "contact_names",
                    },
                    "lobby frame dropped: the outgoing queue is full"
                );
                false
            }
            Err(_) => false,
        }
    }
}

#[async_trait]
impl LobbyPort for LobbyClient {
    async fn resolve_contacts(&self, ids: &[i32]) -> Result<(), String> {
        let outgoing = self
            .outgoing
            .lock()
            .unwrap()
            .clone()
            .ok_or("not connected")?;
        let cancel = self.cancel.lock().unwrap().clone().ok_or("not connected")?;
        let token = self.tokens.get();
        let ids: Vec<i64> = ids.iter().map(|id| i64::from(*id)).collect();
        let names = tokio::select! {
            () = cancel.cancelled() => return Err("not connected".into()),
            result = fetch_player_logins(&self.http, &self.config.api_base, token.as_deref(), &ids) => result?,
        };
        tokio::select! {
            () = cancel.cancelled() => Err("not connected".into()),
            result = outgoing.send(Outgoing::ContactNames(names)) => result.map_err(|_| "not connected".into()),
        }
    }

    async fn connect(&self) -> mpsc::Receiver<LobbyUpdate> {
        let token = CancellationToken::new();
        if let Some(prev) = self.cancel.lock().unwrap().replace(token.clone()) {
            prev.cancel();
        }

        let (tx, rx) = mpsc::channel(8);
        // Channel for client→server frames (game_join, …). The receiver is drained
        // inside `run_session`; the sender is stored so `join` can reach the socket.
        //
        // Sized for a game, not for a click. The ICE adapter's candidates and
        // the game's own `GameState` / `GameResult` reports all leave through
        // here, `try_send` drops what does not fit, and eight slots were one
        // burst of candidates away from losing `GameState Ended`, the report
        // the server ends a player's game on. Java's lobby writer queues
        // without a bound.
        let (out_tx, out_rx) = mpsc::channel::<Outgoing>(OUTGOING_FRAME_CAPACITY);
        *self.outgoing.lock().unwrap() = Some(out_tx);

        let config = self.config.clone();
        let http = self.http.clone();
        let tokens = self.tokens.clone();
        tokio::spawn(async move {
            // The reconnect loop, which is `handle_disconnected` in the Python
            // client. A dropped lobby connection used to end the client's
            // session outright and wait for the user to press connect; the
            // reference clients retry on their own, which is most of why they
            // feel steadier on a flaky line even when they drop just as often.
            //
            // Giving up drops `tx`; the receiver closes and the lobby service
            // emits `Disconnected`, exactly as it did when every drop ended
            // here.
            let mut mid_flight = out_rx;
            let mut failures = 0_u32;
            loop {
                // The token is re-read per attempt: a refresh may have landed
                // while the backoff was sleeping.
                let end = run_session(
                    config.clone(),
                    http.clone(),
                    tokens.get(),
                    tx.clone(),
                    &mut mid_flight,
                    token.clone(),
                )
                .await;

                match end {
                    SessionEnd::Cancelled | SessionEnd::Hopeless => break,
                    SessionEnd::Dropped { authenticated } => {
                        // A session that worked and then dropped starts the
                        // count again, so the next attempt is immediate. Only
                        // a run of attempts that never came up backs off.
                        failures = if authenticated { 1 } else { failures + 1 };
                    }
                }

                if failures >= MAX_CONSECUTIVE_FAILURES {
                    tracing::error!(failures, "lobby reconnect limit reached");
                    break;
                }
                // A frame was addressed to the connection that just went. A
                // `game_join` queued while the socket was dying must not be
                // replayed minutes later onto its replacement, joining a game
                // the player has long since stopped waiting for.
                while mid_flight.try_recv().is_ok() {}

                // Tell the client it is between connections. Not
                // `Disconnected`: that is the end of the session, and this is
                // the middle of one.
                if tx.send(LobbyUpdate::Reconnecting).await.is_err() {
                    break;
                }

                let delay = reconnect_delay(failures);
                if !delay.is_zero() {
                    tracing::info!(
                        delay_seconds = delay.as_secs(),
                        failures,
                        "lobby reconnect scheduled"
                    );
                }
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        });
        rx
    }

    fn join(&self, id: i32, password: Option<String>) -> bool {
        let mut frame = json!({ "command": "game_join", "uid": id, "gameport": 0 });
        if let Some(password) = password.filter(|value| !value.is_empty()) {
            frame["password"] = Value::String(password);
        }
        let sent = self.send_frame(frame);
        if !sent {
            tracing::warn!(
                game_id = id,
                "join ignored because the lobby is disconnected"
            );
        }
        sent
    }

    fn host(&self, config: HostGameConfig) {
        if !self.send_frame(host_frame(config)) {
            tracing::warn!("host request ignored because the lobby is disconnected");
        }
    }

    fn matchmake(&self, queue_name: String, start: bool) {
        let state = if start { "start" } else { "stop" };
        let frame = json!({
            "command": "game_matchmaking",
            "queue_name": queue_name,
            "state": state,
        });
        if !self.send_frame(frame) {
            tracing::warn!("matchmaker request ignored because the lobby is disconnected");
        }
    }

    fn leave_party(&self) {
        let _ = self.send_frame(json!({ "command": "leave_party" }));
    }

    fn kick_party_member(&self, player_id: i32) {
        let _ = self.send_frame(json!({
            "command": "kick_player_from_party",
            "kicked_player_id": player_id,
        }));
    }

    fn invite_to_party(&self, player_id: i32) {
        let _ = self.send_frame(json!({
            "command": "invite_to_party",
            "recipient_id": player_id,
        }));
    }

    fn accept_party_invite(&self, player_id: i32) {
        let _ = self.send_frame(json!({
            "command": "accept_party_invite",
            "sender_id": player_id,
        }));
    }

    fn set_party_factions(&self, factions: Vec<String>) {
        let _ = self.send_frame(party_factions_frame(factions));
    }

    fn set_contact_relation(
        &self,
        contact: faf_domain::state::Contact,
        member: bool,
        remove: Option<Relation>,
    ) -> bool {
        let outgoing = self.outgoing.lock().unwrap();
        let Some(tx) = outgoing.as_ref() else {
            return false;
        };
        let Ok(permits) = tx.try_reserve_many(1 + usize::from(remove.is_some())) else {
            return false;
        };
        let removal = remove.map(|relation| {
            Outgoing::ContactRelation(
                faf_domain::state::Contact {
                    relation,
                    ..contact.clone()
                },
                false,
            )
        });
        for (permit, change) in permits.zip(
            removal
                .into_iter()
                .chain(std::iter::once(Outgoing::ContactRelation(contact, member))),
        ) {
            permit.send(change);
        }
        true
    }

    fn set_player_vetoes(&self, vetoes: Vec<PlayerVeto>) {
        let vetoes = vetoes
            .into_iter()
            .map(|veto| {
                json!({
                    "matchmaker_queue_map_pool_id": veto.matchmaker_queue_map_pool_id,
                    "map_pool_map_version_id": veto.map_pool_map_version_id,
                    "veto_tokens_applied": veto.veto_tokens_applied,
                })
            })
            .collect::<Vec<_>>();
        let _ = self.send_frame(json!({ "command": "set_player_vetoes", "vetoes": vetoes }));
    }

    fn request_avatars(&self) -> bool {
        self.send_frame(avatar_list_frame())
    }

    fn select_avatar(&self, url: Option<String>) -> bool {
        self.send_frame(avatar_select_frame(url))
    }

    fn restore_game_session(&self, game_id: i32) -> bool {
        let sent = self.send_frame(restore_game_session_frame(game_id));
        if !sent {
            tracing::warn!(
                game_id,
                "could not ask the server to restore the game session"
            );
        }
        sent
    }

    fn send_game_relay(&self, command: String, args: Vec<Value>) {
        let frame = json!({ "command": command, "target": "game", "args": args });
        if !self.send_frame(frame) {
            tracing::warn!(%command, "game relay message dropped because the lobby is disconnected");
        }
    }

    fn disconnect(&self) {
        if let Some(token) = self.cancel.lock().unwrap().take() {
            token.cancel();
        }
        // Drop the outgoing sender so a `join` before reconnect is a no-op.
        *self.outgoing.lock().unwrap() = None;
    }
}

/// Why one lobby session ended, and what the reconnect loop should do next.
enum SessionEnd {
    /// `disconnect()`, or a newer `connect()` replacing this one. Stop.
    Cancelled,
    /// The socket ended, or the keepalive found it dead. Open another one.
    ///
    /// `authenticated` is whether this session ever became usable. A session
    /// that worked and then dropped reconnects at once; a run of attempts that
    /// never got that far is what the backoff is for. It is the same
    /// distinction `handle_connected` draws in the Python client by resetting
    /// `_connection_attempts` to zero once a connection is up.
    Dropped { authenticated: bool },
    /// Nothing another attempt would fix: nobody is signed in, or the access
    /// service answered with a URL that is not safe to open.
    Hopeless,
}

/// Drive one lobby connection from handshake to close. Returns when the socket
/// ends, auth fails, or `cancel` fires (which sends a graceful close frame).
async fn run_session(
    config: LobbyConfig,
    http: reqwest::Client,
    access_token: Option<String>,
    tx: mpsc::Sender<LobbyUpdate>,
    outgoing: &mut mpsc::Receiver<Outgoing>,
    cancel: CancellationToken,
) -> SessionEnd {
    // Borrowed rather than owned: the queue of client frames outlives any one
    // socket, so a `game_join` that was waiting when the connection dropped is
    // still there for the session that replaces it.
    let Some(access_token) = access_token else {
        tracing::warn!("lobby connection skipped because there is no access token");
        return SessionEnd::Hopeless; // not logged in: nothing to authenticate with
    };

    // Resolve the WebSocket URL. FAF prod requires a verified URL obtained from
    // the API (the bare host 403s); an explicit FAF_LOBBY_URL bypasses that.
    let ws_url = if config.ws_url.is_empty() {
        match fetch_access_url(&http, &config.user_api_base, "/lobby/access", &access_token).await {
            Ok(url) => url,
            Err(e) => {
                tracing::error!(error = %e, "could not obtain lobby access URL");
                // Worth another try: the access service having a bad minute is
                // not the same as this client being unable to sign in.
                return SessionEnd::Dropped {
                    authenticated: false,
                };
            }
        }
    } else {
        config.ws_url.clone()
    };
    // The server returns "wss://host?verify=…" with no path, which some clients
    // reject with 400. Insert the missing "/" (mirrors the reference client).
    let ws_url = match validated_ws_url(&ws_url) {
        Ok(url) => url,
        Err(error) => {
            tracing::error!(%error, "lobby access service returned an unsafe URL");
            return SessionEnd::Hopeless;
        }
    };

    // Note: ws_url carries a one-time verify token: never log it verbatim.
    let ws = match tokio_tungstenite::connect_async(ws_url.as_str()).await {
        Ok((ws, _)) => ws,
        Err(e) => {
            tracing::error!(error = %e, "could not open lobby WebSocket");
            return SessionEnd::Dropped {
                authenticated: false,
            };
        }
    };
    tracing::info!("lobby WebSocket connected");
    let (mut write, mut read) = ws.split();

    // 1. Ask for a session.
    let ask = json!({
        "command": "ask_session",
        "version": config.version,
        "user_agent": LOBBY_USER_AGENT,
    });
    if write.send(lobby_frame(&ask)).await.is_err() {
        tracing::error!("failed to request a lobby session");
        return SessionEnd::Dropped {
            authenticated: false,
        };
    }

    let mut games = GameSet::default();
    // `game_info` carries team membership, while the actual player ratings
    // arrive separately in `player_info`. Keep the latest displayed global
    // rating by login so game rows can mirror the reference clients' live
    // average-rating column.
    let mut player_ratings = PlayerRatings::new();
    // Identity (rather than rating) side of the same `player_info` stream,
    // what chat needs to rank its roster. See `PlayerDirectory`.
    let mut directory = PlayerDirectory::default();
    // Who this session is, from `welcome`. Only used to refuse the one
    // `player_info` that must never be believed: our own account going
    // offline. See the `player_info` arm.
    let mut own_player_id: Option<i64> = None;
    let mut matchmaking = MatchmakingState::Idle;
    // The machine proof is computed off this loop. `faf-uid` needs seconds on a
    // cold first run (15s measured on a freshly installed client, ~2.4s warm),
    // and awaiting it inline left the connection unable to answer the server's
    // keepalive for that entire window. The server dropped us, and the client
    // only read the rejection once the helper returned: hence a "command
    // rejected" error timestamped seconds after the command it blamed.
    let (proof_tx, mut proof_rx) = mpsc::channel::<Result<String, String>>(1);
    // The session id the pending proof was computed for, sent back in `auth`.
    let mut session_id = String::new();
    // Gates client→server frames. The broker only tolerates the handshake
    // (and `ping` / `pong`) before authentication and closes the socket on
    // anything else, so queued frames wait in `outgoing` until `welcome`
    // rather than racing `auth`.
    let mut authenticated = false;
    // See `SIGN_IN_TIMEOUT`. Only armed until `welcome`.
    let sign_in_deadline = tokio::time::sleep(SIGN_IN_TIMEOUT);
    tokio::pin!(sign_in_deadline);
    // Keepalive, as `ServerConnection`'s timer does it in the Python client.
    //
    // Nothing was sent from this side before: the connection only ever
    // answered the server's `ping`. An idle lobby connection carries no
    // traffic for minutes at a time, and a NAT table or a middlebox drops an
    // idle flow long before either end notices -- the socket stays open here
    // while nothing can reach it, until a write finally fails or the OS gives
    // up on the TCP retransmits, which is hours. That is the shape of the
    // report: random disconnections every few hours, far rarer on the clients
    // that do send something.
    //
    // Two jobs, one timer, exactly as `_ping_connection` has it. Traffic every
    // ten seconds keeps the flow alive, and a tick that arrives with the
    // previous ping still unanswered means the connection is already dead:
    // closing it then is what makes the loss visible in twenty seconds instead
    // of whenever the kernel notices.
    let mut keepalive = tokio::time::interval(LOBBY_KEEPALIVE_INTERVAL);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    keepalive.tick().await; // the first tick of an interval is immediate
    let mut ping_unanswered = false;
    // Every exit but the two deliberate ones is a drop worth reconnecting
    // after, so that is the default and only the deliberate ones assign.
    let mut ending = SessionEnd::Dropped {
        authenticated: false,
    };
    'connection: loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                let _ = write.send(Message::Close(None)).await;
                ending = SessionEnd::Cancelled;
                break;
            }
            () = &mut sign_in_deadline, if !authenticated => {
                tracing::warn!(
                    timeout_seconds = SIGN_IN_TIMEOUT.as_secs(),
                    session = %session_id,
                    "the lobby never confirmed the sign-in; opening a new connection"
                );
                let _ = write.send(Message::Close(None)).await;
                break 'connection;
            }
            // Ten seconds without a single frame from the server.
            _ = keepalive.tick(), if authenticated => {
                if ping_unanswered {
                    tracing::warn!(
                        "the lobby server went silent through two keepalive periods; \
                         treating the connection as dead"
                    );
                    break 'connection;
                }
                ping_unanswered = true;
                if write
                    .send(lobby_frame(&ping_frame()))
                    .await
                    .is_err()
                {
                    break 'connection;
                }
            }
            // Client→server frames (e.g. game_join from `join`). `None` means the
            // sender was dropped by `disconnect`: tear down gracefully. Disabled
            // until `welcome`: see `authenticated`.
            frame = outgoing.recv(), if authenticated => {
                let Some(frame) = frame else {
                    // `disconnect()` dropped the sender: asked for, not lost.
                    let _ = write.send(Message::Close(None)).await;
                    ending = SessionEnd::Cancelled;
                    break;
                };
                let frame = match frame {
                    Outgoing::Frame(frame) => frame,
                    Outgoing::ContactRelation(contact, member) => directory.contact_relation_frame(contact, member),
                    Outgoing::ContactNames(names) => {
                        for (id, login) in names { directory.record_login(id, login); }
                        if tx.send(directory.contact_update()).await.is_err() { break 'connection; }
                        continue;
                    }
                };
                // A relation the user just set is a change to state this
                // session re-broadcasts, and the server tells nobody about it.
                directory.note_local_relation(&frame);
                // The end of the game is the end of the match, and the server
                // says nothing about it: the search was over the moment the
                // match was made, so there is no `search_info` left to send.
                // This connection keeps its own copy of the matchmaking state
                // and would otherwise carry `Launching` for the rest of the
                // session, ready to be re-published by the next message that
                // touches it.
                //
                // `GameState Ended` is the client's own report that the process
                // is gone, relayed a few lines before `GameTerminated` reaches
                // the app state, so it is the same fact arriving on this side.
                if is_game_ended_relay(&frame)
                    && matches!(
                        matchmaking,
                        MatchmakingState::Launching { .. } | MatchmakingState::MatchFound { .. }
                    )
                {
                    matchmaking = MatchmakingState::Idle;
                    if tx
                        .send(LobbyUpdate::Matchmaking(matchmaking.clone()))
                        .await
                        .is_err()
                    {
                        break 'connection;
                    }
                }
                if write
                    .send(lobby_frame(&frame))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            // The machine proof finished: authenticate. A branch of its own so
            // the seconds it takes cost the connection nothing.
            proof = proof_rx.recv() => {
                let Some(proof) = proof else { break 'connection };
                let unique_id = match proof {
                    Ok(uid) => uid,
                    Err(e) => {
                        // Can't authenticate: end the stream (service emits
                        // Disconnected). Surfaced for the dev console.
                        tracing::error!(error = %e, "machine proof generation failed");
                        break 'connection;
                    }
                };
                if session_id.is_empty() {
                    // The session is the server's own handle on this
                    // connection. Without it `auth` is refused as malformed,
                    // and the refusal names nothing.
                    tracing::error!(
                        "cannot authenticate: the lobby server sent no session id"
                    );
                    break 'connection;
                }
                // Neither the token nor the proof itself, which are
                // credentials: their shape is what a rejected sign-in needs.
                tracing::debug!(
                    session = %session_id,
                    proof_length = unique_id.len(),
                    "authenticating with the lobby server"
                );
                let auth = json!({
                    "command": "auth",
                    "token": access_token,
                    "unique_id": unique_id,
                    "session": session_id,
                });
                if write
                    .send(lobby_frame(&auth))
                    .await
                    .is_err()
                {
                    break 'connection;
                }
            }
            incoming = read.next() => {
                let Some(Ok(message)) = incoming else { break 'connection };
                // Anything at all counts as the connection being alive, which
                // is `_receive_message` restarting the timer in the Python
                // client. A WebSocket control frame decodes to no lobby
                // messages and still proves the flow is open, so this is
                // deliberately before the decode rather than after it.
                ping_unanswered = false;
                keepalive.reset();
                let Some(values) = decode_lobby_messages(message) else { break 'connection };
                'message: for value in values {

                // Connectivity messages addressed to the game are relayed to the
                // local ICE adapter, regardless of their command name.
                if value.get("target").and_then(Value::as_str) == Some("game") {
                    let command = command_of(&value).to_string();
                    let args = value
                        .get("args")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    if tx
                        .send(LobbyUpdate::GameRelay { command, args })
                        .await
                        .is_err()
                    {
                        break 'connection;
                    }
                    continue 'message;
                }

                match command_of(&value) {
                    "ping" => {
                        let pong = json!({ "command": "pong" });
                        if write
                            .send(lobby_frame(&pong))
                            .await
                            .is_err()
                        {
                            break 'connection;
                        }
                    }
                    "session" => {
                        // Start the machine fingerprint; `auth` goes out from the
                        // proof branch below once the helper returns, so this loop
                        // keeps serving the socket in the meantime.
                        tracing::debug!("lobby session received; generating machine proof");
                        let session = value.get("session").cloned().unwrap_or(Value::Null);
                        // The Python client normalizes the server's numeric
                        // session id to a string before sending `auth`.
                        session_id = session_to_string(&session);
                        let proof_tx = proof_tx.clone();
                        let uid_path = config.uid_path.clone();
                        let session_arg = session_id.clone();
                        tokio::spawn(async move {
                            let proof = generate_unique_id(&uid_path, &session_arg).await;
                            let _ = proof_tx.send(proof).await;
                        });
                    }
                    // `authentication_failure` is the broker's name for it,
                    // `authentication_failed` the lobby server's.
                    "authentication_failed" | "authentication_failure" => {
                        let reason = server_text(
                            &value,
                            "The lobby server rejected authentication.",
                        );
                        tracing::error!(%reason, "lobby authentication failed");
                        let _ = tx.send(LobbyUpdate::ConnectionRejected { reason }).await;
                        break 'connection;
                    }
                    "welcome" => {
                        tracing::info!("lobby authenticated");
                        authenticated = true;
                        if tx.send(LobbyUpdate::Authenticated).await.is_err() {
                            break 'connection;
                        }
                        if let Some(player) = value.get("me") {
                            own_player_id = player.get("id").and_then(Value::as_i64);
                            update_player_ratings(&mut player_ratings, player);
                            if let Some(profile) = directory.observe(player) {
                                if tx.send(LobbyUpdate::PlayersSeen(vec![profile])).await.is_err() {
                                    break 'connection;
                                }
                            }
                        }
                        let _ = write
                            .send(lobby_frame(&json!({ "command": "matchmaker_info" })))
                            .await;
                    }
                    "player_info" => {
                        if let Some(players) = value.get("players").and_then(Value::as_array) {
                            // The lists as names, before this frame renames or
                            // introduces anybody: see below.
                            let relations_before = directory.relations();
                            let mut newly_seen = Vec::new();
                            let mut removed = Vec::new();
                            for player in players {
                                if player.get("state").and_then(Value::as_str) == Some("offline") {
                                    // Never our own account. The server marks
                                    // a dropped connection offline and
                                    // broadcasts it, and a reconnect is
                                    // exactly that: the old connection's
                                    // notice arrives on the new one, after
                                    // `welcome` has already introduced us. The
                                    // client then forgot its own profile --
                                    // avatar, country and clan all gone from a
                                    // session that was plainly still online,
                                    // which is what "my avatar got eaten"
                                    // looked like. We are receiving this
                                    // message, so we are not offline.
                                    let id = player.get("id").and_then(Value::as_i64);
                                    if id.is_some() && id == own_player_id {
                                        tracing::debug!(
                                            "ignoring an offline notice for our own account"
                                        );
                                        continue;
                                    }
                                    if let Some(profile) = directory.remove(player) {
                                        player_ratings.remove(&profile.login);
                                        removed.push(profile);
                                    }
                                    continue;
                                }
                                update_player_ratings(&mut player_ratings, player);
                                if let Some(profile) = directory.observe(player) {
                                    newly_seen.push(profile);
                                }
                            }
                            if !removed.is_empty()
                                && tx.send(LobbyUpdate::PlayersRemoved(removed)).await.is_err()
                            {
                                break 'connection;
                            }
                            if !newly_seen.is_empty()
                                && tx.send(LobbyUpdate::PlayersSeen(newly_seen)).await.is_err()
                            {
                                break 'connection;
                            }
                            // Sent again whenever this frame changed what the
                            // lists say. Two ways it does: an account the
                            // `social` message named only by id has a name now,
                            // and a friend or foe has been renamed (#516). The
                            // server keeps the lists by id, but they cross to
                            // the UI as names, and only the first case used to
                            // re-send them. A friend who changed their name then
                            // stayed listed under the old one, and the new one
                            // looked like a stranger: "the rename removed them
                            // from my friends".
                            let relations = directory.relations();
                            if relations != relations_before {
                                let (friends, foes) = relations;
                                if tx
                                    .send(LobbyUpdate::Relations { friends, foes, contacts: directory.contacts() })
                                    .await
                                    .is_err()
                                {
                                    break 'connection;
                                }
                            }
                            // Almost always nothing: the player this frame
                            // describes is usually not in a lobby at all, and
                            // this used to answer every one of them with both
                            // lists in full.
                            let mut delta = GameDelta::default();
                            games.refresh_ratings(&player_ratings, &mut delta);
                            if send_game_delta(&tx, delta).await.is_err() {
                                break 'connection;
                            }
                        }
                    }
                    "notice" => {
                        let (style, text) = parse_server_notice(&value);
                        tracing::info!(?style, %text, "lobby notice received");
                        if is_unofficial_client_notice(style, &text) {
                            // Logged above, shown nowhere: see the function.
                            continue;
                        }
                        if is_ban_notice(style, &text) {
                            // The server closes the socket next, and every
                            // reconnect would be refused the same way: the
                            // session ends here, for good.
                            tracing::error!("lobby: this account is banned; not reconnecting");
                            let _ = tx
                                .send(LobbyUpdate::Banned {
                                    text: plain_notice_text(&text),
                                })
                                .await;
                            ending = SessionEnd::Hopeless;
                            break 'connection;
                        }
                        if tx.send(LobbyUpdate::Notice { style, text }).await.is_err() {
                            break 'connection;
                        }
                    }
                    "invalid" => {
                        // FAF sends a bare `{"command": "invalid"}`, so the text
                        // below is usually the fallback and names nothing. What
                        // narrows it down is *when* it arrives: before `welcome`
                        // the only frame the client has sent is its own sign-in,
                        // and saying so is the difference between a report that
                        // can be acted on and one that cannot.
                        let reason = server_text(
                            &value,
                            if authenticated {
                                "The lobby server rejected an invalid client command."
                            } else {
                                "The lobby server rejected this client's sign-in."
                            },
                        );
                        tracing::error!(
                            %reason,
                            authenticated,
                            frame = %value,
                            "lobby protocol command rejected"
                        );
                        let _ = tx.send(LobbyUpdate::ConnectionRejected { reason }).await;
                        break 'connection;
                    }
                    "game_info" => {
                        let raws = extract_raw_games(&value);
                        // The opening dump arrives as one array and is the list,
                        // not a change to it. Anything else is an edit to a
                        // lobby this client already knows about.
                        let replaces_the_list = raws.len() > 1;
                        let mut delta = GameDelta::default();
                        for raw in raws {
                            games.apply(raw, &player_ratings, &mut delta);
                        }
                        let restored = directory.restore_seated(games.seated());
                        if !restored.is_empty() {
                            for profile in &restored {
                                restore_player_ratings(&mut player_ratings, profile);
                            }
                            games.refresh_ratings(&player_ratings, &mut delta);
                            if tx.send(LobbyUpdate::PlayersSeen(restored)).await.is_err() {
                                break 'connection;
                            }
                        }
                        if replaces_the_list {
                            if tx.send(LobbyUpdate::Games(games.snapshot())).await.is_err() {
                                break 'connection; // consumer gone
                            }
                            if tx
                                .send(LobbyUpdate::LiveGames(games.snapshot_live()))
                                .await
                                .is_err()
                            {
                                break 'connection;
                            }
                        } else if send_game_delta(&tx, delta).await.is_err() {
                            break 'connection;
                        }
                    }
                    "game_launch" => {
                        match parse_game_launch(&value) {
                            Some(launch) => {
                                tracing::info!(game_id = launch.uid, "lobby issued game launch");
                                if launch.game_type.eq_ignore_ascii_case("matchmaker") {
                                    if let Some(queue_name) =
                                        matchmaking.matched_queue().map(str::to_owned)
                                    {
                                        matchmaking = MatchmakingState::Launching { queue_name };
                                        if tx
                                            .send(LobbyUpdate::Matchmaking(matchmaking.clone()))
                                            .await
                                            .is_err()
                                        {
                                            break 'connection;
                                        }
                                    }
                                }
                                if tx.send(LobbyUpdate::Launch(launch)).await.is_err() {
                                    break 'connection;
                                }
                            }
                            None => tracing::warn!("game launch message was missing required fields"),
                        }
                    }
                    "game_join_failed" => {
                        let id = value.get("uid").and_then(Value::as_i64).unwrap_or(0) as i32;
                        let reason = value
                            .get("reason")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                            .to_string();
                        tracing::warn!(game_id = id, %reason, "game join rejected");
                        if tx
                            .send(LobbyUpdate::JoinFailed { id, reason })
                            .await
                            .is_err()
                        {
                            break 'connection;
                        }
                    }
                    "matchmaker_info" => {
                        let queues = parse_matchmaker_queues(&value);
                        if tx.send(LobbyUpdate::MatchmakerQueues(queues)).await.is_err() {
                            break 'connection;
                        }
                    }
                    "search_info" => {
                        let queue_name = value
                            .get("queue_name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let searching = value.get("state").and_then(Value::as_str) == Some("start");
                        // Only when it moved. `update_search` refuses to let a
                        // late `stop` erase a match that was already found, and
                        // sending the unchanged state anyway published that
                        // refusal as news: a `stop` arriving after the match had
                        // been played put `Launching` back into the app's state
                        // long after the game had ended, and the Play tab then
                        // sat on "Starting your match" until the client was
                        // restarted.
                        if matchmaking.update_search(queue_name, searching)
                            && tx
                                .send(LobbyUpdate::Matchmaking(matchmaking.clone()))
                                .await
                                .is_err()
                        {
                            break 'connection;
                        }
                    }
                    "match_found" => {
                        let queue_name = value
                            .get("queue_name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        matchmaking = MatchmakingState::MatchFound { queue_name };
                        if tx
                            .send(LobbyUpdate::Matchmaking(matchmaking.clone()))
                            .await
                            .is_err()
                        {
                            break 'connection;
                        }
                        let _ = write
                            .send(lobby_frame(&json!({ "command": "match_ready" })))
                            .await;
                    }
                    "match_cancelled" => {
                        matchmaking = MatchmakingState::Cancelled {
                            queue_name: matchmaking.matched_queue().map(str::to_owned),
                        };
                        if tx
                            .send(LobbyUpdate::Matchmaking(matchmaking.clone()))
                            .await
                            .is_err()
                        {
                            break 'connection;
                        }
                    }
                    "update_party" => {
                        if tx.send(LobbyUpdate::Party(parse_party(&value))).await.is_err() {
                            break 'connection;
                        }
                    }
                    "party_invite" => {
                        let player_id = value
                            .get("sender")
                            .or_else(|| value.get("sender_id"))
                            .and_then(|id| id.as_i64().or_else(|| id.as_str()?.parse().ok()))
                            .unwrap_or_default() as i32;
                        if player_id > 0 {
                            let login = directory
                                .login(player_id)
                                .unwrap_or_else(|| format!("Player {player_id}"));
                            if tx
                                .send(LobbyUpdate::PartyInvite { player_id, login })
                                .await
                                .is_err()
                            {
                                break 'connection;
                            }
                        }
                    }
                    "kicked_from_party" => {
                        match tx.send(LobbyUpdate::Party(PartyState::default())).await {
                            Ok(()) => {}
                            Err(_) => break 'connection,
                        }
                    }
                    "vetoes_info" => {
                        let forced = value.get("forced").and_then(Value::as_bool).unwrap_or(false);
                        match tx
                            .send(LobbyUpdate::Vetoes {
                                vetoes: parse_vetoes(&value),
                                forced,
                            })
                            .await
                        {
                            Ok(()) => {}
                            Err(_) => break 'connection,
                        }
                    }
                    "social" => {
                        directory.set_relations(&value);
                        let missing_ids = directory.unresolved_relation_ids();
                        if !missing_ids.is_empty() {
                            if let Ok(resolved) = fetch_player_logins(
                                &http,
                                &config.api_base,
                                Some(&access_token),
                                &missing_ids,
                            )
                            .await
                            {
                                for (id, login) in resolved {
                                    directory.record_login(id, login);
                                }
                            }
                        }
                        let (friends, foes) = directory.relations();
                        if tx
                            .send(LobbyUpdate::Relations { friends, foes, contacts: directory.contacts() })
                            .await
                            .is_err()
                        {
                            break 'connection;
                        }
                        // The same message assigns this account's channels
                        // (language, clan). Both reference clients take their
                        // auto-join list from here rather than deriving it.
                        if tx
                            .send(LobbyUpdate::AutoJoinChannels(parse_autojoin(&value)))
                            .await
                            .is_err()
                        {
                            break 'connection;
                        }
                    }
                    "avatar" => {
                        match tx
                            .send(LobbyUpdate::Avatars(parse_available_avatars(&value)))
                            .await
                        {
                            Ok(()) => {}
                            Err(_) => break 'connection,
                        }
                    }
                    "failed_message" => {
                        // The broker refused to pass a frame on: a command it
                        // has no schema for, or one that does not match it.
                        // The connection stays up, and the frame is lost; this
                        // is the only trace of it, so it says what it can.
                        // Read out here: inside the macro, `Value` is
                        // tracing's own trait, not serde_json's type.
                        let reason = value.get("message").and_then(Value::as_str).unwrap_or("");
                        let key = value.get("message_key").and_then(Value::as_str).unwrap_or("");
                        let request = value.get("request_id").and_then(Value::as_str).unwrap_or("");
                        tracing::warn!(
                            reason,
                            key,
                            request,
                            "the message broker refused a frame from this client"
                        );
                    }
                    _ => {} // player_info variants without ratings, …
                }
                }
            }
        }
    }
    tracing::info!("lobby connection closed");
    // `authenticated` is the loop's own flag: the session became usable the
    // moment the server said `welcome`.
    match ending {
        SessionEnd::Dropped { .. } => SessionEnd::Dropped { authenticated },
        other => other,
    }
}

fn command_of(value: &Value) -> &str {
    value.get("command").and_then(Value::as_str).unwrap_or("")
}

fn server_text(value: &Value, fallback: &str) -> String {
    let text = value
        .get("text")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .unwrap_or(fallback);
    text.chars().take(MAX_SERVER_MESSAGE_CHARS).collect()
}

fn parse_server_notice(value: &Value) -> (ServerNoticeStyle, String) {
    let style = match value.get("style").and_then(Value::as_str) {
        Some("warning" | "warn") => ServerNoticeStyle::Warning,
        Some("error") => ServerNoticeStyle::Error,
        Some("kill") => ServerNoticeStyle::Kill,
        Some("kick") => ServerNoticeStyle::Kick,
        _ => ServerNoticeStyle::Info,
    };
    (style, server_text(value, "The lobby server sent a notice."))
}

/// The server greets every client it does not recognise as the official one
/// with "You are using an unofficial client version!", on every sign-in,
/// including each silent reconnect. It is not addressed to anyone who could act
/// on it (this client is unofficial on purpose), and it arrived as a required
/// notification with a sound several times an hour (#310). It is dropped here,
/// by its text, so any other `info` notice from the server still comes through.
fn is_unofficial_client_notice(style: ServerNoticeStyle, text: &str) -> bool {
    style == ServerNoticeStyle::Info
        && text
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("you are using an unofficial client")
}

/// The server's answer to a banned account (#488): `BanError` in
/// `server/exceptions.py`, sent as an `error` notice by `lobbyconnection.py`
/// just before it aborts the connection, at sign-in and again on a join or a
/// host. Matched by its text, because an `error` notice on its own is also
/// how the server reports a request it merely refused, with the connection
/// left open.
fn is_ban_notice(style: ServerNoticeStyle, text: &str) -> bool {
    style == ServerNoticeStyle::Error
        && text
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("you are banned from faf")
}

/// A notice's text without the markup the server writes into it: `<br>` as a
/// line break, any other tag dropped, and no more than one empty line in a row.
fn plain_notice_text(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find('<') {
        plain.push_str(&rest[..start]);
        let Some(end) = rest[start..].find('>') else {
            plain.push_str(&rest[start..]);
            rest = "";
            break;
        };
        let tag = rest[start + 1..start + end]
            .trim()
            .trim_end_matches('/')
            .trim()
            .to_ascii_lowercase();
        if tag == "br" {
            plain.push('\n');
        }
        rest = &rest[start + end + 1..];
    }
    plain.push_str(rest);
    let mut lines: Vec<&str> = Vec::new();
    for line in plain.lines().map(str::trim) {
        if line.is_empty() && lines.last().is_none_or(|last| last.is_empty()) {
            continue;
        }
        lines.push(line);
    }
    while lines.last().is_some_and(|last| last.is_empty()) {
        lines.pop();
    }
    lines.join("\n")
}

/// How long to wait before the next attempt, given how many in a row have
/// failed to produce a working connection.
///
/// `handle_disconnected` in the Python client: the first few go out
/// immediately, then `attempts * 10s`. Capped, which the Python client does
/// not do, because its counter is reset by a user action that this loop has no
/// equivalent of.
fn reconnect_delay(failures: u32) -> std::time::Duration {
    if failures <= RECONNECT_IMMEDIATE_ATTEMPTS {
        return std::time::Duration::ZERO;
    }
    RECONNECT_BACKOFF_STEP
        .saturating_mul(failures - RECONNECT_IMMEDIATE_ATTEMPTS)
        .min(RECONNECT_BACKOFF_MAX)
}

/// The keepalive this client sends after ten seconds of silence.
///
/// The same one-word command the Python client sends from `_ping_connection`.
/// The server answers `pong`, which the receive loop ignores: the answer is
/// not what matters, a frame arriving at all is.
fn ping_frame() -> Value {
    json!({ "command": "ping" })
}

/// Whether an outgoing frame is the relay that reports the game process gone.
///
/// The launcher sends `GameState Ended` to the server the moment the process
/// exits, the way the Python client does in `GameSession._exited`. It is the
/// only signal this connection gets that a match it started is over, which is
/// why the session loop watches for it going past.
fn is_game_ended_relay(frame: &Value) -> bool {
    frame.get("command").and_then(Value::as_str) == Some("GameState")
        && frame.get("target").and_then(Value::as_str) == Some("game")
        && frame
            .get("args")
            .and_then(Value::as_array)
            .and_then(|args| args.first())
            .and_then(Value::as_str)
            .is_some_and(|state| state.eq_ignore_ascii_case("Ended"))
}

fn avatar_list_frame() -> Value {
    json!({ "command": "avatar", "action": "list_avatar" })
}

fn avatar_select_frame(url: Option<String>) -> Value {
    json!({ "command": "avatar", "action": "select", "avatar": url })
}

fn parse_available_avatars(message: &Value) -> Vec<AvailableAvatar> {
    let avatars = message
        .get("avatarlist")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| {
            let url = value.get("url")?.as_str()?.trim();
            if url.is_empty() {
                return None;
            }
            Some(AvailableAvatar {
                url: url.to_string(),
                tooltip: value
                    .get("tooltip")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .trim()
                    .to_string(),
            })
        })
        .take(500)
        .collect::<Vec<_>>();
    let mut by_url = BTreeMap::new();
    for avatar in avatars {
        by_url.entry(avatar.url.clone()).or_insert(avatar);
    }
    let mut avatars = by_url.into_values().collect::<Vec<_>>();
    avatars.sort_by(|left, right| {
        left.tooltip
            .to_lowercase()
            .cmp(&right.tooltip.to_lowercase())
            .then_with(|| left.url.cmp(&right.url))
    });
    avatars
}

/// The FAF lobby transport is newline-delimited JSON, including for WebSocket
/// text frames. The Python client appends this delimiter before every send and
/// the Java client uses a line encoder; keep the same wire contract here.
fn encode_lobby_message(value: &Value) -> String {
    format!("{value}\n")
}

/// One client frame as it goes onto the socket: a text frame, always.
///
/// These used to be binary frames, which the lobby server read as readily as
/// text. `faf-message-broker`, which took over the socket on 2026-10-21, has
/// only an `@OnTextMessage` handler, and Quarkus hands a binary frame to a
/// callback that does nothing. `ask_session` vanished without an answer, and
/// the client sat on "connecting" with no lobbies to show. The Java client
/// writes strings (`sendString` in `FafLobbyClient`).
fn lobby_frame(value: &Value) -> Message {
    Message::text(encode_lobby_message(value))
}

/// Decode the lobby's newline-delimited JSON transport. The reference clients
/// accept both text and binary WebSocket frames, and a frame may contain more
/// than one JSON object; accepting both here prevents a valid game snapshot
/// from being silently discarded.
fn decode_lobby_messages(message: Message) -> Option<Vec<Value>> {
    let text = match message {
        Message::Text(text) => text.to_string(),
        Message::Binary(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Message::Close(_) => return None,
        _ => return Some(Vec::new()),
    };

    Some(
        serde_json::Deserializer::from_str(&text)
            .into_iter::<Value>()
            .filter_map(Result::ok)
            .collect(),
    )
}

/// The session id may arrive as a JSON number or string; `faf-uid` wants it as a
/// plain string argument.
fn session_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => String::new(),
    }
}

/// Run `faf-uid <session>` and return its stdout: the `unique_id` blob. Errors
/// if the binary is missing, exits non-zero, or hands back something that is
/// not a proof.
async fn generate_unique_id(uid_path: &str, session: &str) -> Result<String, String> {
    let mut command = tokio::process::Command::new(uid_path);
    command.arg(session);
    // Runs on every launch. Without this it flashes a console window up in the
    // player's face each time.
    crate::infra::hide_console(&mut command);
    let output = command
        .output()
        .await
        .map_err(|e| format!("could not start machine proof helper: {e}"))?;

    // The helper's own message, which used to be discarded. When it fails it is
    // the only thing that says why.
    let complaint = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !output.status.success() {
        return Err(match complaint.is_empty() {
            true => format!("machine proof helper exited with {}", output.status),
            false => format!(
                "machine proof helper exited with {}: {complaint}",
                output.status
            ),
        });
    }

    let uid = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if uid.is_empty() {
        return Err(match complaint.is_empty() {
            true => "machine proof helper produced no output".into(),
            false => format!("machine proof helper produced no output: {complaint}"),
        });
    }
    if !looks_like_machine_proof(&uid) {
        // Sending it anyway is how this surfaced in the field: the lobby server
        // cannot decode it, answers a bare `{"command": "invalid"}` and drops
        // the connection, which names neither the helper nor the reason.
        return Err(format!(
            "machine proof helper produced something that is not a proof: {}",
            preview(&uid)
        ));
    }
    Ok(uid)
}

/// Whether the helper's output can be the proof rather than a message about
/// failing to produce one.
///
/// The blob is a single encoded token: long, and with nothing in it that a
/// sentence has. Anything else would be sent to the lobby server as a
/// credential, so it is refused here where the reason is still known.
fn looks_like_machine_proof(uid: &str) -> bool {
    const SHORTEST_PLAUSIBLE_PROOF: usize = 32;
    uid.len() >= SHORTEST_PLAUSIBLE_PROOF && !uid.chars().any(char::is_whitespace)
}

/// A short, quoted head of a value for a log line, so an unexpected proof can
/// be recognised without the whole of it (which may be a real credential)
/// reaching the log.
fn preview(value: &str) -> String {
    let head: String = value.chars().take(60).collect();
    if head.chars().count() < value.chars().count() {
        format!("{head:?}...")
    } else {
        format!("{head:?}")
    }
}

/// One game as it arrives in a `game_info` message. Only the fields we surface.
#[derive(Debug, Clone, Deserialize)]
struct RawGame {
    /// Some lobby payloads omit optional fields or send them as JSON `null`.
    /// Keep the wire model permissive and apply the client defaults in
    /// `into_game` instead of dropping the complete game from the snapshot.
    uid: Option<i32>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    num_players: Option<i32>,
    #[serde(default)]
    max_players: Option<i32>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    mapname: Option<String>,
    #[serde(default)]
    featured_mod: Option<String>,
    #[serde(default)]
    average_rating: Option<f64>,
    #[serde(default)]
    password_protected: Option<bool>,
    #[serde(default)]
    visibility: Option<String>,
    #[serde(default)]
    game_type: Option<String>,
    #[serde(default)]
    rating_type: Option<String>,
    #[serde(default)]
    launched_at: Option<f64>,
    #[serde(default)]
    hosted_at: Option<String>,
    #[serde(default)]
    rating_min: Option<f64>,
    #[serde(default)]
    rating_max: Option<f64>,
    #[serde(default)]
    enforce_rating_range: Option<bool>,
    #[serde(default)]
    teams: Option<std::collections::BTreeMap<String, Vec<String>>>,
    #[serde(default)]
    sim_mods: Option<std::collections::BTreeMap<String, String>>,
}

impl RawGame {
    /// Only games still open for joining belong in the list.
    fn is_open(&self) -> bool {
        self.state.as_deref() == Some("open")
            && self
                .game_type
                .as_deref()
                .is_none_or(|gt| !gt.eq_ignore_ascii_case("matchmaker"))
            && self
                .visibility
                .as_deref()
                .is_none_or(|v| !v.eq_ignore_ascii_case("matchmaker"))
    }

    /// Games actively in progress: not joinable, but watchable live.
    fn is_playing(&self) -> bool {
        self.state.as_deref() == Some("playing")
    }

    fn into_game(self, player_ratings: &PlayerRatings) -> Option<Game> {
        let id = self.uid?;
        // Empty is not a rating type. The server defaults the field to
        // `global`, and a payload that omits it entirely is an older one
        // saying the same thing.
        let rating_type = self
            .rating_type
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| GLOBAL_LEADERBOARD.to_string());
        let average_rating = average_game_rating(self.teams.as_ref(), player_ratings, &rating_type);
        Some(Game {
            id,
            title: self.title.unwrap_or_default(),
            host: self.host.unwrap_or_default(),
            players: self.num_players.unwrap_or_default(),
            max_players: self.max_players.unwrap_or_default(),
            map: self.mapname.unwrap_or_default(),
            mod_name: self.featured_mod.unwrap_or_else(|| "faf".into()),
            average_rating: self
                .average_rating
                .map(|value| value.round() as i32)
                .filter(|value| *value > 0)
                .unwrap_or(average_rating),
            password_protected: self.password_protected.unwrap_or(false),
            visibility: self.visibility.unwrap_or_else(|| "public".into()),
            game_type: self.game_type.unwrap_or_else(|| "custom".into()),
            rating_type,
            launched_at: self.launched_at.map(|value| value.round() as u32),
            hosted_at: self.hosted_at,
            rating_min: self.rating_min.map(|value| value.round() as i32),
            rating_max: self.rating_max.map(|value| value.round() as i32),
            enforce_rating_range: self.enforce_rating_range.unwrap_or(false),
            teams: self.teams.unwrap_or_default(),
            sim_mods: self.sim_mods.unwrap_or_default(),
        })
    }
}

/// Compute the displayed rating for a game from its active team members.
/// Observers (`-1`/`null`) are intentionally excluded, matching both reference
/// clients. A server-provided `average_rating` remains authoritative when
/// present; this is the fallback used by current lobby payloads, which do not
/// carry one at all.
///
/// Averaged over the game's *own* leaderboard rather than always over `global`.
/// A 1v1 ladder game whose two players are 466 and 500 on the ladder is not a
/// 900-rated game because that is what their global ratings happen to be, and
/// the number printed here is read beside the same players' ratings in the
/// lineup, which name the same leaderboard.
///
/// Rounded to nearest, which is not what a displayed rating does. The two are
/// different quantities: a displayed rating is truncated because that is what
/// the server and both reference clients print for it, while this is an
/// average of several of those and truncating one biases it downward by up to
/// a point every time. A server-supplied `average_rating` is rounded by the
/// caller, so a fallback that truncated also disagreed with the value it
/// stands in for.
fn average_game_rating(
    teams: Option<&BTreeMap<String, Vec<String>>>,
    player_ratings: &PlayerRatings,
    leaderboard: &str,
) -> i32 {
    let ratings = teams
        .into_iter()
        .flatten()
        .filter(|(team, _)| team.as_str() != "-1" && team.as_str() != "null")
        .flat_map(|(_, players)| players.iter())
        .filter_map(|login| {
            player_ratings
                .get(login)
                .and_then(|board| board.get(leaderboard))
                .copied()
        })
        .collect::<Vec<_>>();

    if ratings.is_empty() {
        return 0;
    }
    // Through `f64`, not integer division: `/` truncates toward zero, which
    // rounds a negative average the wrong way as well as a positive one.
    let total: i64 = ratings.iter().map(|rating| i64::from(*rating)).sum();
    (total as f64 / ratings.len() as f64).round() as i32
}

/// Store the conservative displayed global rating from a lobby `player_info`
/// entry. FAF sends TrueSkill `[mean, deviation]`; the displayed value is
/// `mean - 3 * deviation`, unclamped: see [`faf_domain::protocol::rating`].
/// Read the channel list out of a `social` message.
///
/// The server has used both `autojoin` and `channels` for this field, so both
/// are accepted; the Java client reads `channels` while the Python client reads
/// `autojoin`. Names arrive without the `#` prefix, which the domain adds when
/// it normalizes them.
fn parse_autojoin(value: &Value) -> Vec<String> {
    value
        .get("autojoin")
        .or_else(|| value.get("channels"))
        .and_then(Value::as_array)
        .map(|channels| {
            channels
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// Build the `social_add`/`social_remove` frame:
/// `{"command": "social_add"|"social_remove", "friend"|"foe": <id>}`. Verified
/// against both reference clients (`py-client`'s `IrcRelationController` and
/// the Java client's `ServerAccessorTest`).
fn relation_frame(player_id: i32, relation: Relation, member: bool) -> Value {
    let command = if member {
        "social_add"
    } else {
        "social_remove"
    };
    let key = match relation {
        Relation::Friend => "friend",
        Relation::Foe => "foe",
    };
    json!({ "command": command, key: player_id })
}

fn party_factions_frame(factions: Vec<String>) -> Value {
    let factions = factions
        .into_iter()
        .map(|faction| faction.to_ascii_lowercase())
        .collect::<Vec<_>>();
    json!({
        "command": "set_party_factions",
        "factions": factions,
    })
}

/// Tracks who's who, so the chat roster can tell a FAF account from an
/// IRC-only nickname and can rank friends above strangers.
///
/// The lobby announces relations (`social`) as account *ids* but announces
/// identities (`player_info`) separately, and in either order: so this keeps
/// the raw ids and re-resolves them each time the directory grows. Both
/// reference clients do the same join; the Java client's `PlayerService` is
/// this map under another name.
#[derive(Debug, Default)]
struct PlayerDirectory {
    /// Last known account name by stable player id. Unlike `profiles`, this is
    /// deliberately retained while a player is offline: social relation
    /// payloads contain ids, so forgetting the name would make an unrelated
    /// later player update silently drop offline friends from the social list.
    logins: BTreeMap<i64, String>,
    profiles: BTreeMap<i64, PlayerProfile>,
    /// The last profile of each account an offline notice removed, until it
    /// is seen again: see `restore_seated`.
    departed: BTreeMap<i64, PlayerProfile>,
    friend_ids: Vec<i64>,
    foe_ids: Vec<i64>,
}

/// How many removed profiles the directory keeps. A day's coming and going on
/// FAF, at a few hundred bytes each.
const DEPARTED_LIMIT: usize = 4096;

impl PlayerDirectory {
    fn contact_relation_frame(
        &mut self,
        contact: faf_domain::state::Contact,
        member: bool,
    ) -> Value {
        if let Some(login) = contact.login.filter(|login| !login.is_empty()) {
            // UI actions may outlive a rename; only seed an unknown identity.
            self.logins
                .entry(i64::from(contact.player_id))
                .or_insert(login);
        }
        relation_frame(contact.player_id, contact.relation, member)
    }
    fn contacts(&self) -> Vec<faf_domain::state::Contact> {
        [
            (Relation::Friend, &self.friend_ids),
            (Relation::Foe, &self.foe_ids),
        ]
        .into_iter()
        .flat_map(|(relation, ids)| {
            ids.iter().filter_map(move |id| {
                let player_id = i32::try_from(*id).ok().filter(|id| *id > 0)?;
                Some(faf_domain::state::Contact {
                    player_id,
                    login: self.logins.get(id).cloned(),
                    relation,
                })
            })
        })
        .collect()
    }

    fn contact_update(&self) -> LobbyUpdate {
        let (friends, foes) = self.relations();
        LobbyUpdate::Relations {
            friends,
            foes,
            contacts: self.contacts(),
        }
    }

    fn login(&self, id: i32) -> Option<String> {
        self.logins.get(&(id as i64)).cloned()
    }

    /// Remove an authoritative offline entry, returning the last full profile.
    /// Repeated/unknown offline messages are no-ops, which prevents duplicate
    /// notifications and mirrors both reference clients' player maps.
    fn remove(&mut self, player: &Value) -> Option<PlayerProfile> {
        let id = player.get("id").and_then(Value::as_i64)?;
        if let Some(login) = player
            .get("login")
            .or_else(|| player.get("name"))
            .and_then(Value::as_str)
            .filter(|login| !login.is_empty())
        {
            self.logins.insert(id, login.to_string());
        }
        let profile = self.profiles.remove(&id)?;
        if self.departed.len() >= DEPARTED_LIMIT {
            self.departed.pop_first();
        }
        self.departed.insert(id, profile.clone());
        Some(profile)
    }

    /// Bring back the removed profiles of players a game lists, and return
    /// them.
    ///
    /// An offline notice is not always true. When an account signs in again,
    /// the server registers the new connection and only then drops the old
    /// one, and dropping it deletes the player by id, which by then is the
    /// new connection's entry, and broadcasts the old one as `offline`
    /// (`player_service.remove_player`). Everyone else's client forgets a
    /// player who is still online, and the server sends no new `player_info`
    /// for them until their avatar or rating changes. Seated in a lobby, they
    /// showed with no flag, no rating and an unrated average (#595).
    ///
    /// `game_info` lists only connected players, so a seat is proof enough.
    fn restore_seated<'a>(&mut self, seated: impl Iterator<Item = &'a str>) -> Vec<PlayerProfile> {
        if self.departed.is_empty() {
            return Vec::new();
        }
        let wanted: std::collections::BTreeSet<String> = seated.map(str::to_lowercase).collect();
        let ids: Vec<i64> = self
            .departed
            .iter()
            .filter(|(_, profile)| wanted.contains(&profile.login.to_lowercase()))
            .map(|(id, _)| *id)
            .collect();
        let mut restored = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(profile) = self.departed.remove(&id) {
                if let std::collections::btree_map::Entry::Vacant(slot) = self.profiles.entry(id) {
                    slot.insert(profile.clone());
                    restored.push(profile);
                }
            }
        }
        restored
    }

    /// Record one `player_info`/`me` entry. Returns the profile only when it is
    /// new or has actually changed, so a `player_info` that repeats what we
    /// already know costs nothing on the update stream: this arrives for every
    /// online player at login and then continuously.
    fn observe(&mut self, player: &Value) -> Option<PlayerProfile> {
        let login = player
            .get("login")
            .or_else(|| player.get("name"))
            .and_then(Value::as_str)
            .filter(|login| !login.is_empty())?
            .to_string();
        let id = player.get("id").and_then(Value::as_i64)?;
        self.logins.insert(id, login.clone());
        self.departed.remove(&id);

        let string_field = |key: &str| {
            player
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        };
        // `avatar` is `{"url": …, "tooltip": …}` when set, and `null` otherwise.
        let avatar = player.get("avatar");
        let avatar_field = |key: &str| {
            avatar
                .and_then(|a| a.get(key))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        };

        let known = self.profiles.get(&id);
        let ratings = player_lobby_ratings(player)
            .or_else(|| known.map(|profile| profile.ratings.clone()))
            .unwrap_or_else(|| {
                player
                    .get("global_rating")
                    .and_then(value_as_f64)
                    .and_then(|rating| {
                        Some(vec![PlayerLobbyRating {
                            leaderboard: "global".into(),
                            rating: whole_points(rating)?,
                            mean: whole_points(rating)?,
                            deviation: 0,
                            games_played: 0,
                        }])
                    })
                    .unwrap_or_default()
            });

        let profile = PlayerProfile {
            // Ids are database serials, comfortably inside i32 (see `Player`).
            id: id as i32,
            login,
            // Some incremental `player_info` variants omit ratings. Preserve
            // the last known estimate instead of making the UI flash unrated.
            global_rating: player_rating_estimate(player)
                .or_else(|| known.map(|profile| profile.global_rating))
                .unwrap_or_default(),
            ratings,
            // Flag filenames are lowercase; normalise here so the UI doesn't
            // have to care that the server sends "DE".
            country: string_field("country").to_lowercase(),
            clan: string_field("clan"),
            avatar_url: avatar_field("url"),
            avatar_tooltip: avatar_field("tooltip"),
        };

        match self.profiles.get(&id) {
            Some(known) if *known == profile => None,
            _ => {
                self.profiles.insert(id, profile.clone());
                Some(profile)
            }
        }
    }

    fn set_relations(&mut self, value: &Value) {
        self.friend_ids = id_list(value, "friends");
        self.foe_ids = id_list(value, "foes");
    }

    /// Fold a `social_add`/`social_remove` this client is *sending* into the
    /// directory's own lists.
    ///
    /// The server acknowledges neither command and never sends a second
    /// `social` message, so the connection would otherwise never learn what
    /// the user just did: the id lists stay exactly as they arrived at login.
    /// That was invisible until the next `player_info` named an account the
    /// relation lists had been waiting for, because the resolved lists are
    /// re-sent then -- and they are built from these ids, so they replaced the
    /// change the user had just made with the state from login. Adding a foe
    /// put a friend back, removing a friend restored them, and adding a friend
    /// held for a second or two and then vanished.
    ///
    /// The two lists are kept mutually exclusive here, as they are in the
    /// slice: `services::social` sends the matching `social_remove` for the
    /// other list, so this is the local half of the same decision.
    fn note_local_relation(&mut self, frame: &Value) {
        let member = match frame.get("command").and_then(Value::as_str) {
            Some("social_add") => true,
            Some("social_remove") => false,
            _ => return,
        };
        for key in ["friend", "foe"] {
            let Some(id) = frame.get(key).and_then(Value::as_i64) else {
                continue;
            };
            let (list, other) = if key == "friend" {
                (&mut self.friend_ids, &mut self.foe_ids)
            } else {
                (&mut self.foe_ids, &mut self.friend_ids)
            };
            list.retain(|known| *known != id);
            if member {
                list.push(id);
                other.retain(|known| *known != id);
            }
        }
    }
    fn unresolved_relation_ids(&self) -> Vec<i64> {
        let mut missing = Vec::new();
        for &id in self.friend_ids.iter().chain(self.foe_ids.iter()) {
            if !self.logins.contains_key(&id) && !missing.contains(&id) {
                missing.push(id);
            }
        }
        missing
    }

    fn record_login(&mut self, id: i64, login: String) {
        // A lookup started before a rename must not replace a live profile.
        let login = self
            .profiles
            .get(&id)
            .map(|profile| profile.login.clone())
            .unwrap_or(login);
        self.logins.insert(id, login);
    }

    #[cfg(test)]
    fn has_relations(&self) -> bool {
        !self.friend_ids.is_empty() || !self.foe_ids.is_empty()
    }

    /// Relations as logins, dropping ids we have never been able to name,
    /// they resolve on a later `player_info`, which re-emits. Known identities
    /// survive an offline transition even though their online profile does not.
    fn relations(&self) -> (Vec<String>, Vec<String>) {
        let resolve = |ids: &Vec<i64>| {
            ids.iter()
                .filter_map(|id| self.logins.get(id).cloned())
                .collect()
        };
        (resolve(&self.friend_ids), resolve(&self.foe_ids))
    }
}

/// Fetch usernames for player IDs from the FAF API so offline friends and foes
/// are resolved by name.
async fn fetch_player_logins(
    http: &reqwest::Client,
    api_base: &str,
    token: Option<&str>,
    ids: &[i64],
) -> Result<Vec<(i64, String)>, String> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut results = Vec::new();
    for chunk in ids.chunks(100) {
        let id_str = chunk
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let filter = format!("id=in=({id_str})");
        let mut url =
            match url::Url::parse(&format!("{}/data/player", api_base.trim_end_matches('/'))) {
                Ok(url) => url,
                Err(e) => return Err(format!("invalid player URL: {e}")),
            };
        url.query_pairs_mut()
            .append_pair("filter", &filter)
            .append_pair("fields[player]", "login")
            .append_pair("page[size]", &chunk.len().to_string());

        let mut request = http.get(url);
        if let Some(tok) = token.filter(|t| !t.is_empty()) {
            request = request.bearer_auth(tok);
        }
        let response = match request.send().await {
            Ok(res) => res,
            Err(e) => return Err(format!("player query failed: {e}")),
        };
        if !response.status().is_success() {
            continue;
        }
        let doc: Value = match response.json().await {
            Ok(doc) => doc,
            Err(_) => continue,
        };
        if let Some(data) = doc.get("data").and_then(Value::as_array) {
            for item in data {
                let id = item
                    .get("id")
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse::<i64>().ok());
                let login = item
                    .get("attributes")
                    .and_then(|a| a.get("login"))
                    .and_then(Value::as_str);
                if let (Some(id), Some(login)) = (id, login) {
                    if !login.is_empty() {
                        results.push((id, login.to_string()));
                    }
                }
            }
        }
    }
    Ok(results)
}

/// The lobby sends relation ids as numbers, but has historically sent them as
/// strings too; accept both rather than silently losing a friends list.
fn id_list(value: &Value, key: &str) -> Vec<i64> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|ids| {
            ids.iter()
                .filter_map(|id| id.as_i64().or_else(|| id.as_str()?.parse().ok()))
                .collect()
        })
        .unwrap_or_default()
}

/// Every player's displayed rating on every leaderboard the server has sent
/// one for: login, then leaderboard (`global`, `ladder_1v1`, `tmm_2v2`, ...).
///
/// A single global number was enough while every game average was a global
/// average. It is not enough now that a game is averaged over the leaderboard
/// it is actually rated on.
type PlayerRatings = BTreeMap<String, BTreeMap<String, i32>>;

pub(crate) const GLOBAL_LEADERBOARD: &str = "global";

/// Put a restored player's ratings back, from the profile that kept them.
fn restore_player_ratings(ratings: &mut PlayerRatings, profile: &PlayerProfile) {
    if ratings.contains_key(&profile.login) {
        return;
    }
    let boards: BTreeMap<String, i32> = if profile.ratings.is_empty() {
        // Only the estimate `update_player_ratings` falls back to, if any.
        if profile.global_rating == 0 {
            return;
        }
        BTreeMap::from([(GLOBAL_LEADERBOARD.to_string(), profile.global_rating)])
    } else {
        profile
            .ratings
            .iter()
            .map(|rating| (rating.leaderboard.clone(), rating.rating))
            .collect()
    };
    ratings.insert(profile.login.clone(), boards);
}

fn update_player_ratings(ratings: &mut PlayerRatings, player: &Value) {
    let Some(login) = player
        .get("login")
        .or_else(|| player.get("name"))
        .and_then(Value::as_str)
        .filter(|login| !login.is_empty())
    else {
        return;
    };

    // A partial `player_info` that carries no ratings at all must not erase
    // what the last full one said, which is the same rule `observe` follows
    // for the profile it keeps.
    if let Some(boards) = player_lobby_ratings(player) {
        if !boards.is_empty() {
            ratings.insert(
                login.to_string(),
                boards
                    .into_iter()
                    .map(|rating| (rating.leaderboard, rating.rating))
                    .collect(),
            );
            return;
        }
    }
    if let Some(estimate) = player_rating_estimate(player) {
        ratings
            .entry(login.to_string())
            .or_default()
            .insert(GLOBAL_LEADERBOARD.to_string(), estimate);
    }
}

fn player_rating_estimate(player: &Value) -> Option<i32> {
    player
        .get("ratings")
        .and_then(|all| all.get("global"))
        .and_then(|global| global.get("rating"))
        .and_then(Value::as_array)
        .and_then(|rating| {
            let mean = rating.first().and_then(Value::as_f64)?;
            let deviation = rating.get(1).and_then(Value::as_f64)?;
            displayed_points(mean, deviation)
        })
        .or_else(|| {
            player
                .get("global_rating")
                .and_then(value_as_f64)
                .and_then(whole_points)
        })
}

/// Preserve every rating queue from the live player directory for cheap UI
/// summaries. `None` means this was a partial update with no ratings field;
/// `Some([])` means the server explicitly supplied an empty rating map.
fn player_lobby_ratings(player: &Value) -> Option<Vec<PlayerLobbyRating>> {
    let ratings = player.get("ratings")?.as_object()?;
    let mut summaries = ratings
        .iter()
        .filter_map(|(leaderboard, entry)| {
            let rating = entry.get("rating")?.as_array()?;
            let mean = rating.first().and_then(Value::as_f64)?;
            let deviation = rating.get(1).and_then(Value::as_f64)?;
            let games_played = entry
                .get("number_of_games")
                .and_then(|value| {
                    value
                        .as_i64()
                        .or_else(|| value.as_str()?.parse::<i64>().ok())
                })
                .and_then(|value| i32::try_from(value).ok())
                .unwrap_or_default()
                .max(0);
            Some(PlayerLobbyRating {
                leaderboard: leaderboard.clone(),
                rating: displayed_points(mean, deviation)?,
                mean: whole_points(mean.round())?,
                deviation: whole_points(deviation.round())?,
                games_played,
            })
        })
        .collect::<Vec<_>>();
    summaries.sort_by(|left, right| left.leaderboard.cmp(&right.leaderboard));
    Some(summaries)
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())
}

/// One queue's published rating windows, as `[[min, max], ...]`.
///
/// A malformed pair is dropped rather than defaulted: an entry that became
/// `0..0` would silently fail to contain anybody's rating, which reads as a
/// quiet queue rather than as bad data.
fn parse_rating_ranges(queue: &Value, key: &str) -> Vec<RatingRange> {
    queue
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|pair| {
            let pair = pair.as_array()?;
            Some(RatingRange {
                min: pair.first()?.as_f64()?.round() as i32,
                max: pair.get(1)?.as_f64()?.round() as i32,
            })
        })
        .collect()
}

fn parse_matchmaker_queues(message: &Value) -> Vec<MatchmakerQueue> {
    parse_matchmaker_queues_at(message, chrono::Utc::now())
}

/// [`parse_matchmaker_queues`] with the arrival time given, for the tests.
fn parse_matchmaker_queues_at(
    message: &Value,
    received: chrono::DateTime<chrono::Utc>,
) -> Vec<MatchmakerQueue> {
    message
        .get("queues")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|queue| MatchmakerQueue {
            queue_name: queue
                .get("queue_name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            team_size: queue.get("team_size").and_then(Value::as_i64).unwrap_or(1) as i32,
            num_players: queue
                .get("num_players")
                .and_then(Value::as_i64)
                .unwrap_or(0) as i32,
            queue_pop_time_seconds: queue
                .get("queue_pop_time_delta")
                .and_then(Value::as_f64)
                .unwrap_or_default()
                .round() as i32,
            queue_pops_at: queue_pops_at(queue, received),
            boundary_80s: parse_rating_ranges(queue, "boundary_80s"),
            boundary_75s: parse_rating_ranges(queue, "boundary_75s"),
        })
        .filter(|queue| !queue.queue_name.is_empty())
        .collect()
}

/// The instant the queue pops next: the server's delta, counted from when the
/// message arrived. See [`MatchmakerQueue::queue_pops_at`].
///
/// The delta rather than the server's own `queue_pop_time`, which is what the
/// Java client reads: both name the same moment, but the absolute one is only
/// right on a machine whose clock agrees with the server's, and the countdown
/// is read against this machine's clock.
fn queue_pops_at(queue: &Value, received: chrono::DateTime<chrono::Utc>) -> String {
    let Some(delta) = queue
        .get("queue_pop_time_delta")
        .and_then(Value::as_f64)
        .filter(|delta| delta.is_finite())
    else {
        return String::new();
    };
    // A pop already due is due now, not in the past: the server sends the
    // next one with the queue's next update.
    let millis = (delta.max(0.0) * 1000.0).round() as i64;
    (received + chrono::Duration::milliseconds(millis))
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn parse_party(message: &Value) -> PartyState {
    let owner_id = message
        .get("owner")
        .and_then(Value::as_i64)
        .map(|id| id as i32);
    let members = message
        .get("members")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|member| {
            let player_id = member.get("player")?.as_i64()? as i32;
            let factions = member
                .get("factions")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect();
            Some(PartyMember {
                player_id,
                // The server never sends one: `PartyMember.to_dict` is the
                // player id and the factions, nothing else. This is read
                // anyway in case that changes, but the label below is what
                // actually ships, and the UI resolves the id against the live
                // player directory rather than showing it.
                name: member
                    .get("name")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("Player {player_id}")),
                factions,
            })
        })
        .collect();
    PartyState { owner_id, members }
}

fn parse_vetoes(message: &Value) -> Vec<PlayerVeto> {
    message
        .get("vetoes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|veto| {
            Some(PlayerVeto {
                matchmaker_queue_map_pool_id: veto.get("matchmaker_queue_map_pool_id")?.as_i64()?
                    as i32,
                map_pool_map_version_id: veto.get("map_pool_map_version_id")?.as_i64()? as i32,
                veto_tokens_applied: veto.get("veto_tokens_applied")?.as_i64()? as i32,
            })
        })
        .collect()
}

/// A `game_info` message is either a single game or a batch under `games`.
fn extract_raw_games(message: &Value) -> Vec<RawGame> {
    if let Some(array) = message.get("games").and_then(Value::as_array) {
        array
            .iter()
            .filter_map(|v| serde_json::from_value(v.clone()).ok())
            .collect()
    } else {
        serde_json::from_value(message.clone())
            .into_iter()
            .collect()
    }
}

/// A `game_launch` message: the server's order to start a game. `args` arrives as
/// a mixed list of strings and numbers (`list[str | int]`), so we take it as raw
/// `Value`s and stringify. `uid` is required; everything else defaults.
#[derive(Debug, Clone, Deserialize)]
struct RawGameLaunch {
    uid: i32,
    #[serde(rename = "mod", default)]
    mod_name: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    mapname: String,
    #[serde(default)]
    game_type: String,
    #[serde(default)]
    rating_type: String,
    #[serde(default)]
    expected_players: Option<i32>,
    #[serde(default)]
    team: Option<i32>,
    #[serde(default)]
    faction: Option<i32>,
    #[serde(default)]
    map_position: Option<i32>,
    #[serde(default)]
    game_options: std::collections::BTreeMap<String, Value>,
    #[serde(default)]
    args: Vec<Value>,
}

impl RawGameLaunch {
    fn into_launch(self) -> GameLaunch {
        GameLaunch {
            uid: self.uid,
            mod_name: self.mod_name,
            name: self.name,
            mapname: self.mapname,
            game_type: self.game_type,
            rating_type: self.rating_type,
            expected_players: self.expected_players,
            team: self.team,
            faction: self.faction,
            map_position: self.map_position,
            game_options: self
                .game_options
                .into_iter()
                .map(|(name, value)| (name, value_to_arg(&value)))
                .collect(),
            args: self.args.iter().map(value_to_arg).collect(),
        }
    }
}

/// Render a launch arg as a string: bare for strings, JSON form for numbers/bools.
fn value_to_arg(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Parse a `game_launch` message into a [`GameLaunch`], or `None` if it lacks the
/// required `uid`.
fn parse_game_launch(message: &Value) -> Option<GameLaunch> {
    serde_json::from_value::<RawGameLaunch>(message.clone())
        .ok()
        .map(RawGameLaunch::into_launch)
}

/// The aggregated game lists, split into "open" (joinable) and "playing"
/// (watchable live). Keyed by id so updates replace in place and games that
/// leave a state are removed from it; snapshots are ordered by id for stable
/// rendering. A game moves between the two sets as its state changes (e.g.
/// `open` → `playing` when it launches).
#[derive(Debug, Default)]
struct GameSet {
    games: BTreeMap<i32, Game>,
    live_games: BTreeMap<i32, Game>,
}

/// Send whichever halves of a delta actually carry something.
///
/// Returns `Err` when the consumer is gone, so the caller can break the
/// connection loop the same way an ordinary `send` lets it.
async fn send_game_delta(
    tx: &tokio::sync::mpsc::Sender<LobbyUpdate>,
    delta: GameDelta,
) -> Result<(), ()> {
    let GameDelta {
        open_upserted,
        open_removed,
        live_upserted,
        live_removed,
    } = delta;
    if !open_upserted.is_empty() || !open_removed.is_empty() {
        tx.send(LobbyUpdate::GamesChanged {
            upserted: open_upserted,
            removed: open_removed,
        })
        .await
        .map_err(|_| ())?;
    }
    if !live_upserted.is_empty() || !live_removed.is_empty() {
        tx.send(LobbyUpdate::LiveGamesChanged {
            upserted: live_upserted,
            removed: live_removed,
        })
        .await
        .map_err(|_| ())?;
    }
    Ok(())
}

/// What one batch of `game_info` frames did to the two lists.
///
/// Accumulated across the frames in a batch and sent once, so a server dump of
/// fifty lobbies is one event rather than fifty. Empty halves are not sent at
/// all: the common frame touches the open list and leaves the live list alone,
/// and re-sending an unchanged live list is most of what this replaces.
#[derive(Default)]
struct GameDelta {
    open_upserted: Vec<Game>,
    open_removed: Vec<i32>,
    live_upserted: Vec<Game>,
    live_removed: Vec<i32>,
}

impl GameSet {
    fn apply(&mut self, raw: RawGame, player_ratings: &PlayerRatings, delta: &mut GameDelta) {
        let Some(uid) = raw.uid else {
            return;
        };
        let (open, playing) = (raw.is_open(), raw.is_playing());
        if open {
            if let Some(game) = raw.into_game(player_ratings) {
                // An unchanged repeat is not a change. The server re-announces a
                // lobby on any edit, including ones that leave everything this
                // client shows identical.
                if self.games.get(&uid) != Some(&game) {
                    delta.open_upserted.push(game.clone());
                    self.games.insert(uid, game);
                }
            }
            if self.live_games.remove(&uid).is_some() {
                delta.live_removed.push(uid);
            }
        } else if playing {
            if let Some(game) = raw.into_game(player_ratings) {
                if self.live_games.get(&uid) != Some(&game) {
                    delta.live_upserted.push(game.clone());
                    self.live_games.insert(uid, game);
                }
            }
            if self.games.remove(&uid).is_some() {
                delta.open_removed.push(uid);
            }
        } else {
            if self.games.remove(&uid).is_some() {
                delta.open_removed.push(uid);
            }
            if self.live_games.remove(&uid).is_some() {
                delta.live_removed.push(uid);
            }
        }
    }

    /// Re-average every game against ratings that just arrived, reporting only
    /// the games whose number actually moved.
    ///
    /// This runs on every `player_info`, which is the most frequent frame the
    /// lobby sends. It used to be followed by a full resend of both lists;
    /// almost always nothing had changed, because the player it described was
    /// not in a lobby at all.
    fn refresh_ratings(&mut self, player_ratings: &PlayerRatings, delta: &mut GameDelta) {
        for game in self.games.values_mut() {
            let average = average_game_rating(Some(&game.teams), player_ratings, &game.rating_type);
            if average > 0 && average != game.average_rating {
                game.average_rating = average;
                delta.open_upserted.push(game.clone());
            }
        }
        for game in self.live_games.values_mut() {
            let average = average_game_rating(Some(&game.teams), player_ratings, &game.rating_type);
            if average > 0 && average != game.average_rating {
                game.average_rating = average;
                delta.live_upserted.push(game.clone());
            }
        }
    }

    fn snapshot(&self) -> Vec<Game> {
        self.games.values().cloned().collect()
    }

    /// Every player in an open or a running game, observers included.
    fn seated(&self) -> impl Iterator<Item = &str> {
        self.games
            .values()
            .chain(self.live_games.values())
            .flat_map(|game| game.teams.values().flatten())
            .map(String::as_str)
    }

    fn snapshot_live(&self) -> Vec<Game> {
        self.live_games.values().cloned().collect()
    }
}

/// The `restore_game_session` frame, built apart from sending it so its shape
/// is testable without a live socket.
///
/// `game_id` is a number on the wire. The server reads it with `int(...)`, so a
/// string would work too, and both reference clients send a number.
fn restore_game_session_frame(game_id: i32) -> Value {
    json!({ "command": "restore_game_session", "game_id": game_id })
}

/// The `game_host` frame, built apart from sending it so the password rule is
/// testable without a live socket.
///
/// The password is null, not an empty string, when the host did not set one.
/// The server stores whatever arrives and then advertises the game as
/// `password_protected: self.password is not None`, so `""` produced a locked
/// game nobody could join: a joiner sends no password at all, and the server
/// compares `None` against `""` and refuses. The Python client sends `None`
/// here for exactly this reason.
fn host_frame(config: HostGameConfig) -> Value {
    let mut frame = json!({
        "command": "game_host",
        "title": config.title,
        "mod": config.mod_name,
        "visibility": config.visibility,
        "mapname": config.map,
        "password": match config.password {
            Some(password) => Value::String(password),
            None => Value::Null,
        },
    });
    // The flag, not just the bounds. The server reads all three
    // (`lobbyconnection.on_game_host`) and only its `enforce_rating_range`
    // makes `Game.is_visible_to_player` consult the range at all: sending the
    // bounds alone produced a lobby that advertised a rating window and
    // admitted anybody, which is what the report described. Sent either way,
    // as Java does (`NewGameInfo`), so an unenforced range is still the one
    // the lobby advertises.
    frame["enforce_rating_range"] = json!(config.enforce_rating_range);
    if let Some(min) = config.rating_min {
        frame["rating_min"] = json!(min);
    }
    if let Some(max) = config.rating_max {
        frame["rating_max"] = json!(max);
    }
    frame
}

#[cfg(test)]
mod tests {
    #[test]
    fn contact_switch_needs_space_for_both_frames() {
        use super::{LobbyClient, LobbyPort, Outgoing, Relation, TokenStore};
        use faf_domain::state::Contact;
        use tokio::sync::mpsc;

        let lobby = LobbyClient::faf(TokenStore::new());
        let contact = Contact {
            player_id: 7,
            login: Some("OfflineFixture".into()),
            relation: Relation::Foe,
        };
        let (tx, mut rx) = mpsc::channel(2);
        assert!(tx
            .try_send(Outgoing::Frame(serde_json::Value::Null))
            .is_ok());
        *lobby.outgoing.lock().unwrap() = Some(tx);

        assert!(!lobby.set_contact_relation(contact.clone(), true, Some(Relation::Friend)));
        assert!(matches!(rx.try_recv().unwrap(), Outgoing::Frame(_)));
        assert!(rx.try_recv().is_err());

        assert!(lobby.set_contact_relation(contact, true, Some(Relation::Friend)));
        let Outgoing::ContactRelation(old, false) = rx.try_recv().unwrap() else {
            panic!("expected removal of the old friend relation");
        };
        let Outgoing::ContactRelation(new, true) = rx.try_recv().unwrap() else {
            panic!("expected the new foe relation");
        };
        assert_eq!(old.relation, Relation::Friend);
        assert_eq!(new.relation, Relation::Foe);
        assert_eq!(new.player_id, old.player_id);
    }

    #[test]
    fn contacts_preserve_unresolved_ids_and_never_turn_lookups_into_online_profiles() {
        let mut directory = super::PlayerDirectory::default();
        directory.set_relations(&serde_json::json!({ "friends": [7], "foes": [99] }));
        let before = directory.contacts();
        assert_eq!(before.len(), 2);
        assert!(before.iter().all(|contact| contact.login.is_none()));
        directory.record_login(7, "Offline".into());
        let after = directory.contacts();
        assert_eq!(after[0].login.as_deref(), Some("Offline"));
        assert!(directory.profiles.is_empty());
        directory.note_local_relation(&super::relation_frame(
            99,
            faf_domain::state::Relation::Foe,
            false,
        ));
        // A lookup completing after removal must not put the contact back.
        directory.record_login(99, "Removed".into());
        assert_eq!(directory.contacts().len(), 1);
        let frame = directory.contact_relation_frame(
            faf_domain::state::Contact {
                player_id: 42,
                login: Some("NewOffline".into()),
                relation: faf_domain::state::Relation::Friend,
            },
            true,
        );
        directory.note_local_relation(&frame);
        assert_eq!(
            frame,
            serde_json::json!({ "command": "social_add", "friend": 42 })
        );
        assert!(directory
            .contacts()
            .iter()
            .any(|contact| contact.login.as_deref() == Some("NewOffline")));
        assert!(directory.profiles.is_empty());
        directory.record_login(42, "Renamed".into());
        directory.contact_relation_frame(
            faf_domain::state::Contact {
                player_id: 42,
                login: Some("Stale".into()),
                relation: faf_domain::state::Relation::Friend,
            },
            true,
        );
        assert_eq!(directory.login(42).as_deref(), Some("Renamed"));
        directory.observe(&serde_json::json!({ "id": 42, "login": "LiveName" }));
        directory.record_login(42, "StaleLookup".into());
        assert_eq!(directory.login(42).as_deref(), Some("LiveName"));
    }
    use super::*;
    use std::time::Duration;

    /// The frame that puts a running game back on a replacement socket.
    ///
    /// The server ties a player's game connection to the socket the game was
    /// launched on, so losing that socket -- a drop, or Disconnect followed by
    /// Reconnect -- leaves the running game relayed for nobody and the players
    /// unable to reconnect to each other. `server/lobbyconnection.py` reads
    /// exactly these two keys.
    #[test]
    fn restoring_a_game_session_names_the_game_by_number() {
        let frame = restore_game_session_frame(12345);
        assert_eq!(
            frame["command"],
            Value::String("restore_game_session".into())
        );
        assert_eq!(frame["game_id"], Value::from(12345));
    }

    #[test]
    fn hosting_without_a_password_sends_null_rather_than_an_empty_string() {
        let mut config = HostGameConfig {
            title: "Aeolus".into(),
            mod_name: "faf".into(),
            visibility: "public".into(),
            map: "scmp_007".into(),
            password: None,
            enforce_rating_range: false,
            rating_min: None,
            rating_max: None,
        };

        // The server stores whatever arrives and advertises the game as
        // protected when it is not null, so an empty string locks a game that
        // nobody, including the host, has a password for.
        let open = host_frame(config.clone());
        assert_eq!(open["password"], Value::Null);
        assert!(open["rating_min"].is_null());

        config.password = Some("hunter2".into());
        let locked = host_frame(config);
        assert_eq!(locked["password"], Value::String("hunter2".into()));
    }
    use crate::infra::extract_access_url;

    #[test]
    fn server_notices_preserve_actions_and_bound_untrusted_text() {
        let (style, text) = parse_server_notice(&json!({
            "style": "kill",
            "text": "  maintenance  "
        }));
        assert_eq!(style, ServerNoticeStyle::Kill);
        assert_eq!(text, "maintenance");

        let oversized = "ü".repeat(MAX_SERVER_MESSAGE_CHARS + 10);
        let (_, text) = parse_server_notice(&json!({ "style": "warning", "text": oversized }));
        assert_eq!(text.chars().count(), MAX_SERVER_MESSAGE_CHARS);
        assert!(text.is_char_boundary(text.len()));
    }

    #[test]
    fn only_the_unofficial_client_greeting_is_dropped() {
        let greeting = "You are using an unofficial client version! Some features might not             work as expected. If you experience any problems please download the latest             version of the official client from <a href=\"https://www.faforever.com\">";
        assert!(is_unofficial_client_notice(
            ServerNoticeStyle::Info,
            greeting
        ));
        // The same words as a warning are something the server chose to escalate.
        assert!(!is_unofficial_client_notice(
            ServerNoticeStyle::Warning,
            greeting
        ));
        assert!(!is_unofficial_client_notice(
            ServerNoticeStyle::Info,
            "The server restarts in 10 minutes."
        ));
    }

    /// `BanError.message()` in the lobby server, as it reaches the client.
    const BAN_TEXT: &str = "You are banned from FAF forever. <br>Reason: <br>Asked for it<br><br>\
        <i>If you would like to appeal this ban, please send an email to: \
        moderation@faforever.com</i>";

    #[test]
    fn a_ban_is_told_apart_from_a_refused_request() {
        assert!(is_ban_notice(ServerNoticeStyle::Error, BAN_TEXT));
        assert!(is_ban_notice(
            ServerNoticeStyle::Error,
            "  You are banned from FAF for 3 days. <br>Reason: <br>smurfing"
        ));
        // The same server sends `error` notices it means as a refusal, with
        // the connection left open; those are not a ban.
        assert!(!is_ban_notice(
            ServerNoticeStyle::Error,
            "You are already in a game"
        ));
        assert!(!is_ban_notice(ServerNoticeStyle::Info, BAN_TEXT));
    }

    #[test]
    fn a_ban_reads_as_plain_text() {
        assert_eq!(
            plain_notice_text(BAN_TEXT),
            "You are banned from FAF forever.\nReason:\nAsked for it\n\n\
             If you would like to appeal this ban, please send an email to: \
             moderation@faforever.com"
        );
        assert_eq!(plain_notice_text("a < b"), "a < b");
        assert_eq!(
            plain_notice_text("one<br/>two<BR >three"),
            "one\ntwo\nthree"
        );
    }

    #[test]
    fn unknown_notice_styles_are_safe_info_and_missing_text_has_context() {
        assert_eq!(
            parse_server_notice(&json!({ "style": "future-style" })),
            (
                ServerNoticeStyle::Info,
                "The lobby server sent a notice.".into()
            )
        );
        assert_eq!(
            server_text(&json!({ "text": "  " }), "fallback"),
            "fallback"
        );
    }

    #[test]
    fn directory_reports_only_profiles_that_are_new_or_changed() {
        // `player_info` repeats every online player at login and then keeps
        // arriving; re-emitting unchanged profiles would flood the UI.
        let mut directory = PlayerDirectory::default();
        assert_eq!(
            directory
                .observe(&json!({ "id": 1, "login": "Aurora" }))
                .map(|p| p.login),
            Some("Aurora".to_string())
        );
        assert_eq!(
            directory.observe(&json!({ "id": 1, "login": "Aurora" })),
            None
        );
        // A rename under the same id is a change.
        assert_eq!(
            directory
                .observe(&json!({ "id": 1, "login": "Aurora_" }))
                .map(|p| p.login),
            Some("Aurora_".to_string())
        );
        // So is gaining an avatar.
        assert_eq!(
            directory
                .observe(&json!({
                    "id": 1,
                    "login": "Aurora_",
                    "avatar": { "url": "https://x/a.png", "tooltip": "Cat" },
                }))
                .map(|p| p.avatar_url),
            Some("https://x/a.png".to_string())
        );
    }

    #[test]
    fn directory_reads_country_clan_and_avatar() {
        let mut directory = PlayerDirectory::default();
        let profile = directory
            .observe(&json!({
                "id": 7,
                "login": "Stormlord",
                // The server sends the country uppercased; flag assets are lowercase.
                "country": "DE",
                "clan": "BC",
                "avatar": { "url": "https://content/a.png", "tooltip": "Aeon" },
                "ratings": {
                    "global": { "rating": [1800, 200], "number_of_games": 374 },
                    "ladder_1v1": { "rating": [1500, 100], "number_of_games": "42" }
                },
            }))
            .unwrap();
        assert_eq!(profile.country, "de");
        assert_eq!(profile.clan, "BC");
        assert_eq!(profile.avatar_url, "https://content/a.png");
        assert_eq!(profile.avatar_tooltip, "Aeon");
        assert_eq!(profile.global_rating, 1200);
        assert_eq!(
            profile.ratings,
            vec![
                PlayerLobbyRating {
                    leaderboard: "global".into(),
                    rating: 1200,
                    mean: 1800,
                    deviation: 200,
                    games_played: 374,
                },
                PlayerLobbyRating {
                    leaderboard: "ladder_1v1".into(),
                    rating: 1200,
                    mean: 1500,
                    deviation: 100,
                    games_played: 42,
                },
            ]
        );

        assert_eq!(
            directory.observe(&json!({
                "id": 7,
                "login": "Stormlord",
                "country": "DE",
                "clan": "BC",
                "avatar": { "url": "https://content/a.png", "tooltip": "Aeon" },
            })),
            None,
            "a partial update must retain the last known rating"
        );
    }

    #[test]
    fn directory_tolerates_a_null_avatar_and_missing_country() {
        let mut directory = PlayerDirectory::default();
        let profile = directory
            .observe(&json!({ "id": 8, "login": "Aurora", "avatar": Value::Null }))
            .unwrap();
        assert_eq!(profile.country, "");
        assert_eq!(profile.clan, "");
        assert_eq!(profile.avatar_url, "");
    }

    #[test]
    fn directory_ignores_entries_without_an_id_or_login() {
        let mut directory = PlayerDirectory::default();
        assert_eq!(directory.observe(&json!({ "login": "Aurora" })), None);
        assert_eq!(directory.observe(&json!({ "id": 7 })), None);
        assert_eq!(directory.observe(&json!({ "id": 7, "login": "" })), None);
    }

    #[test]
    fn directory_offline_removal_is_authoritative_and_idempotent() {
        let mut directory = PlayerDirectory::default();
        directory.set_relations(&json!({ "friends": [7], "foes": [] }));
        directory
            .observe(&json!({ "id": 7, "login": "Aurora", "country": "DE" }))
            .unwrap();

        let removed = directory
            .remove(&json!({ "id": 7, "login": "Aurora", "state": "offline" }))
            .unwrap();
        assert_eq!(removed.login, "Aurora");
        assert_eq!(removed.country, "de");
        assert_eq!(directory.login(7).as_deref(), Some("Aurora"));
        assert_eq!(directory.relations().0, vec!["Aurora"]);
        assert!(directory
            .remove(&json!({ "id": 7, "login": "Aurora", "state": "offline" }))
            .is_none());
    }

    #[test]
    fn a_player_a_game_seats_comes_back_after_a_false_offline_notice() {
        // #595: a re-login's old connection is broadcast offline after the
        // new one registered. The player is still online, and a game lists
        // them; their profile and ratings come back from what was kept.
        let mut directory = PlayerDirectory::default();
        directory
            .observe(&json!({
                "id": 480156, "login": "OGlobal2500lader", "country": "DE",
                "ratings": { "global": { "rating": [2016.0, 95.0], "number_of_games": 4200 } }
            }))
            .unwrap();
        directory
            .remove(&json!({ "id": 480156, "login": "OGlobal2500lader", "state": "offline" }))
            .unwrap();

        // Not seated anywhere: stays gone.
        assert!(directory
            .restore_seated(["Somebody"].into_iter())
            .is_empty());

        let restored = directory.restore_seated(["oglobal2500lader"].into_iter());
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].country, "de");
        let mut ratings = PlayerRatings::new();
        restore_player_ratings(&mut ratings, &restored[0]);
        assert_eq!(ratings["OGlobal2500lader"]["global"], 1731);

        // Once only: the profile is back in the directory.
        assert!(directory
            .restore_seated(["OGlobal2500lader"].into_iter())
            .is_empty());
    }

    #[test]
    fn a_player_seen_again_is_no_longer_waiting_to_be_restored() {
        let mut directory = PlayerDirectory::default();
        directory
            .observe(&json!({ "id": 7, "login": "Aurora" }))
            .unwrap();
        directory
            .remove(&json!({ "id": 7, "login": "Aurora", "state": "offline" }))
            .unwrap();
        directory
            .observe(&json!({ "id": 7, "login": "Aurora" }))
            .unwrap();
        assert!(directory.departed.is_empty());
        assert!(directory.restore_seated(["Aurora"].into_iter()).is_empty());
    }

    #[test]
    fn relation_frames_match_the_server_protocol() {
        assert_eq!(
            relation_frame(7, Relation::Friend, true),
            json!({ "command": "social_add", "friend": 7 })
        );
        assert_eq!(
            relation_frame(7, Relation::Foe, true),
            json!({ "command": "social_add", "foe": 7 })
        );
        assert_eq!(
            relation_frame(7, Relation::Friend, false),
            json!({ "command": "social_remove", "friend": 7 })
        );
        assert_eq!(
            relation_frame(7, Relation::Foe, false),
            json!({ "command": "social_remove", "foe": 7 })
        );
    }

    #[test]
    fn a_relation_the_user_sets_survives_the_next_player_info() {
        // The report: adding a foe made them a friend, removing a friend put
        // them back, and adding a friend held for a moment and vanished. All
        // three are this re-broadcast, which used to replay the login state.
        let mut directory = PlayerDirectory::default();
        directory.record_login(7, "Aurora".into());
        directory.record_login(9, "Nomander".into());
        directory.set_relations(&json!({ "friends": [7], "foes": [] }));

        directory.note_local_relation(&relation_frame(7, Relation::Foe, true));
        assert_eq!(directory.relations(), (vec![], vec!["Aurora".to_string()]));

        directory.note_local_relation(&relation_frame(9, Relation::Friend, true));
        assert_eq!(
            directory.relations(),
            (vec!["Nomander".to_string()], vec!["Aurora".to_string()])
        );

        directory.note_local_relation(&relation_frame(9, Relation::Friend, false));
        assert_eq!(directory.relations(), (vec![], vec!["Aurora".to_string()]));

        // Anything else on the outgoing channel leaves the lists alone.
        directory.note_local_relation(&json!({ "command": "game_join", "uid": 7 }));
        assert_eq!(directory.relations(), (vec![], vec!["Aurora".to_string()]));
    }

    #[test]
    fn a_connection_that_worked_is_retried_at_once_and_a_dead_one_backs_off() {
        // `handle_disconnected` in the Python client: the first few attempts
        // are immediate, because a socket that has been working and then drops
        // is usually a blip and waiting ten seconds to find that out is ten
        // seconds of the client being wrong about its own state.
        assert_eq!(reconnect_delay(1), Duration::ZERO);
        assert_eq!(
            reconnect_delay(RECONNECT_IMMEDIATE_ATTEMPTS),
            Duration::ZERO
        );

        // Past that it is a real outage rather than a blip, and the schedule
        // is the reference client's `attempts * 10s`.
        assert_eq!(
            reconnect_delay(RECONNECT_IMMEDIATE_ATTEMPTS + 1),
            Duration::from_secs(10)
        );
        assert_eq!(
            reconnect_delay(RECONNECT_IMMEDIATE_ATTEMPTS + 2),
            Duration::from_secs(20)
        );

        // Capped, so a server that is down all evening is still polled once a
        // minute rather than once an hour.
        assert_eq!(reconnect_delay(1_000), RECONNECT_BACKOFF_MAX);
        // And it cannot overflow its way back to an immediate retry.
        assert_eq!(reconnect_delay(u32::MAX), RECONNECT_BACKOFF_MAX);
    }

    #[test]
    fn every_client_frame_is_a_newline_terminated_text_frame() {
        // The message broker drops binary frames without a word: see
        // `lobby_frame`.
        let frame = lobby_frame(&json!({ "command": "ask_session" }));
        let Message::Text(text) = frame else {
            panic!("the broker only reads text frames, got {frame:?}");
        };
        assert_eq!(text.as_str(), "{\"command\":\"ask_session\"}\n");
    }

    #[test]
    fn the_keepalive_is_the_same_command_the_reference_client_sends() {
        // The lobby answers this with `pong`, which the receive loop drops
        // through its catch-all arm: every frame resets the timer, so the
        // answer needs no handling of its own.
        assert_eq!(ping_frame(), json!({ "command": "ping" }));
    }

    #[test]
    fn avatar_frames_match_the_reference_protocol() {
        assert_eq!(
            avatar_list_frame(),
            json!({ "command": "avatar", "action": "list_avatar" })
        );
        assert_eq!(
            avatar_select_frame(Some("https://content.test/a.png".into())),
            json!({
                "command": "avatar",
                "action": "select",
                "avatar": "https://content.test/a.png"
            })
        );
        assert_eq!(
            avatar_select_frame(None),
            json!({ "command": "avatar", "action": "select", "avatar": null })
        );
    }

    #[test]
    fn available_avatars_are_sanitized_deduplicated_and_sorted() {
        let avatars = parse_available_avatars(&json!({
            "command": "avatar",
            "avatarlist": [
                { "url": "https://content.test/z.png", "tooltip": "Zulu" },
                { "url": "", "tooltip": "Invalid" },
                { "url": "https://content.test/a.png", "tooltip": "Alpha" },
                { "url": "https://content.test/a.png", "tooltip": "Duplicate" }
            ]
        }));
        assert_eq!(avatars.len(), 2);
        assert_eq!(avatars[0].tooltip, "Alpha");
        assert_eq!(avatars[1].tooltip, "Zulu");
    }

    #[test]
    fn faction_frame_matches_the_server_protocol() {
        assert_eq!(
            party_factions_frame(vec!["UEF".into(), "Cybran".into()]),
            json!({
                "command": "set_party_factions",
                "factions": ["uef", "cybran"],
            })
        );
    }

    #[test]
    fn relations_resolve_ids_that_arrive_after_the_social_message() {
        // `social` routinely lands before the `player_info` naming those ids.
        let mut directory = PlayerDirectory::default();
        directory.set_relations(&json!({ "friends": [2, 3], "foes": [4] }));
        assert_eq!(directory.relations(), (vec![], vec![]));

        directory.observe(&json!({ "id": 2, "login": "Stormlord" }));
        directory.observe(&json!({ "id": 4, "login": "Griefer" }));
        assert_eq!(
            directory.relations(),
            (vec!["Stormlord".to_string()], vec!["Griefer".to_string()])
        );

        directory.observe(&json!({ "id": 3, "login": "Sheikah" }));
        assert_eq!(
            directory.relations().0,
            vec!["Stormlord".to_string(), "Sheikah".to_string()]
        );
    }

    /// A friend renamed while this client runs: the lists are kept by id, so
    /// the next `player_info` moves the friend to the new name, and the lists
    /// read differently afterwards, which is what has them sent again (#516).
    #[test]
    fn a_renamed_friend_stays_a_friend_under_the_new_name() {
        let mut directory = PlayerDirectory::default();
        directory.set_relations(&json!({ "friends": [2], "foes": [4] }));
        directory.observe(&json!({ "id": 2, "login": "Stormlord" }));
        directory.observe(&json!({ "id": 4, "login": "Griefer" }));
        let before = directory.relations();

        directory.observe(&json!({ "id": 2, "login": "StormlordTheSecond" }));
        directory.observe(&json!({ "id": 4, "login": "Reformed" }));
        let after = directory.relations();

        assert_eq!(
            after,
            (
                vec!["StormlordTheSecond".to_string()],
                vec!["Reformed".to_string()]
            )
        );
        assert_ne!(after, before, "a rename has to re-send the lists");
    }

    #[test]
    fn relation_ids_are_accepted_as_numbers_or_strings() {
        let mut directory = PlayerDirectory::default();
        directory.set_relations(&json!({ "friends": [2, "3", "not-a-number"], "foes": [] }));
        directory.observe(&json!({ "id": 2, "login": "Stormlord" }));
        directory.observe(&json!({ "id": 3, "login": "Sheikah" }));
        assert_eq!(
            directory.relations().0,
            vec!["Stormlord".to_string(), "Sheikah".to_string()]
        );
    }

    #[test]
    fn a_social_message_without_relations_is_not_treated_as_having_any() {
        let mut directory = PlayerDirectory::default();
        assert!(!directory.has_relations());
        directory.set_relations(&json!({ "command": "social", "autojoin": ["#aeolus"] }));
        assert!(!directory.has_relations());
    }

    fn open_game_json(uid: i32, state: &str, players: i32) -> Value {
        json!({
            "command": "game_info",
            "uid": uid,
            "state": state,
            "num_players": players,
            "max_players": 8,
            "title": format!("Game {uid}"),
            "host": "Stormlord",
            "mapname": "Theta Passage",
            "launched_at": 1_700_000_000.4,
        })
    }

    #[test]
    fn parses_server_veto_allocations() {
        let vetoes = parse_vetoes(&json!({
            "vetoes": [{
                "matchmaker_queue_map_pool_id": 4,
                "map_pool_map_version_id": 91,
                "veto_tokens_applied": 2
            }]
        }));
        assert_eq!(vetoes.len(), 1);
        assert_eq!(vetoes[0].matchmaker_queue_map_pool_id, 4);
        assert_eq!(vetoes[0].map_pool_map_version_id, 91);
        assert_eq!(vetoes[0].veto_tokens_applied, 2);
    }

    #[test]
    fn extracts_single_game() {
        let raws = extract_raw_games(&open_game_json(7, "open", 3));
        assert_eq!(raws.len(), 1);
        assert_eq!(raws[0].uid, Some(7));
        assert_eq!(raws[0].num_players, Some(3));
    }

    #[test]
    fn extracts_batched_games() {
        let msg = json!({
            "command": "game_info",
            "games": [open_game_json(1, "open", 1), open_game_json(2, "open", 2)],
        });
        let raws = extract_raw_games(&msg);
        assert_eq!(raws.len(), 2);
        assert_eq!(raws[1].uid, Some(2));
    }

    #[test]
    fn raw_game_maps_to_domain_game() {
        let raw = extract_raw_games(&open_game_json(42, "open", 5))
            .pop()
            .unwrap();
        let game = raw.into_game(&PlayerRatings::new()).unwrap();
        assert_eq!(game.id, 42);
        assert_eq!(game.players, 5);
        assert_eq!(game.max_players, 8);
        assert_eq!(game.map, "Theta Passage");
        assert_eq!(game.host, "Stormlord");
        assert_eq!(game.launched_at, Some(1_700_000_000));
    }

    #[test]
    fn computes_average_rating_from_player_info_and_ignores_observers() {
        let mut ratings = PlayerRatings::new();
        update_player_ratings(
            &mut ratings,
            &json!({
                "login": "Alpha",
                "ratings": { "global": { "rating": [1800, 200] } }
            }),
        );
        update_player_ratings(
            &mut ratings,
            &json!({
                "login": "Bravo",
                "ratings": { "global": { "rating": [1700, 100] } }
            }),
        );
        update_player_ratings(
            &mut ratings,
            &json!({
                "login": "Spectator",
                "ratings": { "global": { "rating": [2500, 0] } }
            }),
        );

        let mut message = open_game_json(43, "open", 3);
        message["teams"] = json!({
            "1": ["Alpha"],
            "2": ["Bravo"],
            "-1": ["Spectator"]
        });
        let game = extract_raw_games(&message)
            .pop()
            .unwrap()
            .into_game(&ratings)
            .unwrap();

        // Alpha = 1200 and Bravo = 1400; observer is excluded.
        assert_eq!(game.average_rating, 1300);
    }

    #[test]
    fn accepts_legacy_player_rating_field() {
        let mut ratings = PlayerRatings::new();
        update_player_ratings(
            &mut ratings,
            &json!({ "login": "Legacy", "global_rating": 1234 }),
        );
        assert_eq!(ratings["Legacy"][GLOBAL_LEADERBOARD], 1234);
    }

    #[test]
    fn averages_a_matchmaker_game_over_the_queue_it_is_rated_on() {
        // The reported case: a 1v1 ladder game listing global ratings, which
        // are the numbers the game is not being played for.
        let mut ratings = PlayerRatings::new();
        update_player_ratings(
            &mut ratings,
            &json!({
                "login": "Valkyra",
                "ratings": {
                    "global": { "rating": [1100, 98] },
                    "ladder_1v1": { "rating": [662, 65] },
                }
            }),
        );
        update_player_ratings(
            &mut ratings,
            &json!({
                "login": "Rival",
                "ratings": {
                    "global": { "rating": [1600, 200] },
                    "ladder_1v1": { "rating": [1000, 100] },
                }
            }),
        );

        let mut message = open_game_json(44, "playing", 2);
        message["rating_type"] = json!("ladder_1v1");
        message["teams"] = json!({ "2": ["Valkyra"], "3": ["Rival"] });
        let game = extract_raw_games(&message)
            .pop()
            .unwrap()
            .into_game(&ratings)
            .unwrap();

        assert_eq!(game.rating_type, "ladder_1v1");
        // Ladder: 467 and 700, not global's 806 and 1000. 583.5 rounds up,
        // the way the server's own average does when it sends one.
        assert_eq!(game.average_rating, 584);
    }

    #[test]
    fn a_game_without_a_rating_type_is_a_global_one() {
        let mut ratings = PlayerRatings::new();
        update_player_ratings(
            &mut ratings,
            &json!({ "login": "Alpha", "ratings": { "global": { "rating": [1800, 200] } } }),
        );
        let mut message = open_game_json(45, "open", 1);
        message["teams"] = json!({ "1": ["Alpha"] });
        let game = extract_raw_games(&message)
            .pop()
            .unwrap()
            .into_game(&ratings)
            .unwrap();

        assert_eq!(game.rating_type, GLOBAL_LEADERBOARD);
        assert_eq!(game.average_rating, 1200);
    }

    #[test]
    fn a_rating_below_zero_is_kept_rather_than_flattened() {
        // The displayed rating of a new or long-idle account. The server's own
        // `displayed()` does not clamp it, the website prints it, and the
        // profile card in this client already showed it: only the lobby lists
        // clamped, so one player read -137 in one place and 0 in the other.
        let ratings = player_lobby_ratings(&json!({
            "ratings": {
                "global": { "rating": [1363.0, 500.0], "number_of_games": 3 },
            }
        }))
        .expect("a ratings table");

        assert_eq!(ratings[0].rating, -137);
    }

    #[test]
    fn a_partial_player_info_does_not_erase_the_ratings_already_known() {
        let mut ratings = PlayerRatings::new();
        update_player_ratings(
            &mut ratings,
            &json!({
                "login": "Alpha",
                "ratings": { "ladder_1v1": { "rating": [900, 100] } }
            }),
        );
        update_player_ratings(&mut ratings, &json!({ "login": "Alpha", "country": "de" }));
        assert_eq!(ratings["Alpha"]["ladder_1v1"], 600);
    }

    #[test]
    fn gameset_adds_open_and_removes_closed() {
        let mut set = GameSet::default();
        for raw in extract_raw_games(&open_game_json(1, "open", 1)) {
            set.apply(raw, &PlayerRatings::new(), &mut GameDelta::default());
        }
        for raw in extract_raw_games(&open_game_json(2, "open", 2)) {
            set.apply(raw, &PlayerRatings::new(), &mut GameDelta::default());
        }
        assert_eq!(set.snapshot().len(), 2);

        // Game 1 transitions to playing → drops out of the open list.
        for raw in extract_raw_games(&open_game_json(1, "playing", 2)) {
            set.apply(raw, &PlayerRatings::new(), &mut GameDelta::default());
        }
        let snap = set.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].id, 2); // ordered by id
    }

    #[test]
    fn gameset_update_replaces_in_place() {
        let mut set = GameSet::default();
        for raw in extract_raw_games(&open_game_json(5, "open", 1)) {
            set.apply(raw, &PlayerRatings::new(), &mut GameDelta::default());
        }
        for raw in extract_raw_games(&open_game_json(5, "open", 4)) {
            set.apply(raw, &PlayerRatings::new(), &mut GameDelta::default());
        }
        let snap = set.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].players, 4);
    }

    #[test]
    fn gameset_ignores_forming_matchmaker_games_until_playing() {
        let mut set = GameSet::default();
        let forming_mm = json!({
            "command": "game_info",
            "uid": 99,
            "title": "Team DietTaco Vs Team Balde",
            "state": "open",
            "game_type": "matchmaker",
            "num_players": 4,
            "max_players": 6,
        });
        for raw in extract_raw_games(&forming_mm) {
            set.apply(raw, &PlayerRatings::new(), &mut GameDelta::default());
        }
        assert_eq!(set.snapshot().len(), 0);
        assert_eq!(set.snapshot_live().len(), 0);

        let playing_mm = json!({
            "command": "game_info",
            "uid": 99,
            "title": "Team DietTaco Vs Team Balde",
            "state": "playing",
            "game_type": "matchmaker",
            "num_players": 6,
            "max_players": 6,
        });
        for raw in extract_raw_games(&playing_mm) {
            set.apply(raw, &PlayerRatings::new(), &mut GameDelta::default());
        }
        assert_eq!(set.snapshot().len(), 0);
        assert_eq!(set.snapshot_live().len(), 1);
    }

    #[test]
    fn parses_the_rating_windows_a_queue_publishes() {
        let message = json!({
            "command": "matchmaker_info",
            "queues": [{
                "queue_name": "ladder1v1",
                "team_size": 1,
                "num_players": 3,
                "queue_pop_time_delta": 42.4,
                "boundary_80s": [[800, 1200], [1100.0, 1500.0]],
                // Malformed entries are dropped rather than defaulted: a
                // `0..0` window silently contains nobody, which would read as
                // a quiet queue instead of as bad data.
                "boundary_75s": [[700, 1300], [900], "nonsense", []],
            }],
        });

        let queues = parse_matchmaker_queues(&message);
        assert_eq!(queues.len(), 1);
        assert_eq!(
            queues[0].boundary_80s,
            vec![
                RatingRange {
                    min: 800,
                    max: 1_200
                },
                RatingRange {
                    min: 1_100,
                    max: 1_500
                },
            ]
        );
        assert_eq!(
            queues[0].boundary_75s,
            vec![RatingRange {
                min: 700,
                max: 1_300
            }]
        );
    }

    #[test]
    fn the_next_pop_is_counted_from_when_the_message_arrived() {
        let message = json!({
            "command": "matchmaker_info",
            "queues": [
                { "queue_name": "ladder1v1", "queue_pop_time_delta": 42.4 },
                { "queue_name": "tmm2v2", "queue_pop_time_delta": -3.0 },
                { "queue_name": "tmm4v4" },
            ],
        });
        let received = chrono::DateTime::parse_from_rfc3339("2026-10-01T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        let queues = parse_matchmaker_queues_at(&message, received);

        assert_eq!(queues[0].queue_pops_at, "2026-10-01T12:00:42.400Z");
        assert_eq!(queues[1].queue_pops_at, "2026-10-01T12:00:00.000Z");
        assert_eq!(queues[2].queue_pops_at, "");
    }

    #[test]
    fn a_queue_without_rating_windows_still_parses() {
        // Older servers, and any queue the server has nothing queued for.
        let message = json!({
            "command": "matchmaker_info",
            "queues": [{ "queue_name": "tmm2v2", "team_size": 2, "num_players": 0 }],
        });
        let queues = parse_matchmaker_queues(&message);
        assert_eq!(queues.len(), 1);
        assert!(queues[0].boundary_80s.is_empty());
        assert!(queues[0].boundary_75s.is_empty());
    }

    #[test]
    fn parses_game_launch_with_mixed_args() {
        let msg = json!({
            "command": "game_launch",
            "uid": 12345,
            "mod": "faf",
            "name": "Big Team Game",
            "mapname": "scmp_009",
            "game_type": "matchmaker",
            "rating_type": "ladder_1v1",
            "expected_players": 2,
            "team": 1,
            "faction": 3,
            "map_position": 2,
            "game_options": { "Timeouts": 3, "Share": "FullShare" },
            "args": ["/numgames", 137, "/init", true],
        });
        let launch = parse_game_launch(&msg).expect("should parse");
        assert_eq!(launch.uid, 12345);
        assert_eq!(launch.mod_name, "faf"); // `mod` renamed
        assert_eq!(launch.name, "Big Team Game");
        assert_eq!(launch.mapname, "scmp_009");
        assert_eq!(launch.game_type, "matchmaker");
        assert_eq!(launch.rating_type, "ladder_1v1");
        assert_eq!(launch.expected_players, Some(2));
        assert_eq!(launch.team, Some(1));
        assert_eq!(launch.faction, Some(3));
        assert_eq!(launch.map_position, Some(2));
        assert_eq!(
            launch.game_options.get("Timeouts").map(String::as_str),
            Some("3")
        );
        assert_eq!(
            launch.game_options.get("Share").map(String::as_str),
            Some("FullShare")
        );
        // Mixed string/number/bool args all stringified.
        assert_eq!(launch.args, vec!["/numgames", "137", "/init", "true"]);
    }

    #[test]
    fn game_launch_requires_uid_but_tolerates_missing_optionals() {
        // No uid → cannot parse.
        assert!(parse_game_launch(&json!({ "command": "game_launch" })).is_none());

        // uid only → everything else defaults gracefully.
        let launch = parse_game_launch(&json!({ "uid": 7 })).expect("uid is enough");
        assert_eq!(launch.uid, 7);
        assert_eq!(launch.mod_name, "");
        assert!(launch.args.is_empty());
    }

    #[test]
    fn ensure_ws_path_inserts_missing_slash() {
        assert_eq!(
            validated_ws_url("wss://ws.faforever.com?verify=a.b-c_d").unwrap(),
            "wss://ws.faforever.com/?verify=a.b-c_d"
        );
        // Already has a path → unchanged (no double slash, token untouched).
        assert_eq!(
            validated_ws_url("wss://ws.faforever.com/?verify=a.b-c_d").unwrap(),
            "wss://ws.faforever.com/?verify=a.b-c_d"
        );
        assert_eq!(
            validated_ws_url("wss://host/path?verify=x").unwrap(),
            "wss://host/path?verify=x"
        );
        assert_eq!(validated_ws_url("wss://host").unwrap(), "wss://host/");
        assert!(validated_ws_url("ws://remote.example/verify").is_err());
        assert!(validated_ws_url("wss://user:password@remote.example/verify").is_err());
        assert!(validated_ws_url("ws://127.0.0.1:9876/verify").is_ok());
    }

    #[test]
    fn extracts_access_url_top_level_and_nested() {
        assert_eq!(
            extract_access_url(&json!({ "accessUrl": "wss://ws.faforever.com/?verify=abc" })),
            Some("wss://ws.faforever.com/?verify=abc".to_string())
        );
        assert_eq!(
            extract_access_url(&json!({ "data": { "accessUrl": "wss://x/?verify=y" } })),
            Some("wss://x/?verify=y".to_string())
        );
        assert_eq!(extract_access_url(&json!({ "nope": 1 })), None);
    }

    /// A proof is one long encoded token. Anything with a space in it is a
    /// sentence about failing to produce one, and sending that to the lobby
    /// server buys a bare `{"command": "invalid"}` and a closed connection.
    #[test]
    fn a_machine_proof_is_one_long_token() {
        assert!(looks_like_machine_proof(&"a".repeat(64)));
        assert!(looks_like_machine_proof(
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.abcdefghijklmnop"
        ));
    }

    #[test]
    fn a_message_is_not_a_machine_proof() {
        for output in [
            "",
            "error: could not read the machine id",
            "Zugriff verweigert",
            // Long enough, but a proof is a single token.
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbb",
            // A plausible looking but far too short answer.
            "no",
        ] {
            assert!(
                !looks_like_machine_proof(output),
                "{output:?} must not be sent as a credential"
            );
        }
    }

    #[test]
    fn a_preview_keeps_the_head_and_marks_the_rest() {
        assert_eq!(preview("short"), "\"short\"");
        let long = "x".repeat(80);
        let shown = preview(&long);
        assert!(shown.ends_with("\"..."), "{shown}");
        assert!(
            shown.len() < long.len(),
            "the whole value must not be logged"
        );
    }

    #[test]
    fn session_to_string_handles_number_and_string() {
        assert_eq!(session_to_string(&json!(123456)), "123456");
        assert_eq!(session_to_string(&json!("abc-789")), "abc-789");
        assert_eq!(session_to_string(&Value::Null), "");
    }

    #[test]
    fn decodes_binary_frames_and_multiple_json_messages() {
        let first = open_game_json(1, "open", 1).to_string();
        let second = open_game_json(2, "open", 2).to_string();
        let frame = Message::Binary(format!("{first}\n{second}\n").into_bytes());

        let messages = decode_lobby_messages(frame).expect("binary frames stay usable");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].get("uid").and_then(Value::as_i64), Some(1));
        assert_eq!(messages[1].get("uid").and_then(Value::as_i64), Some(2));
    }

    #[test]
    fn encodes_outgoing_lobby_messages_with_line_delimiter() {
        let encoded = encode_lobby_message(&json!({ "command": "ask_session" }));
        assert!(encoded.ends_with('\n'));
        let decoded: Value =
            serde_json::from_str(&encoded).expect("newline is valid JSON whitespace");
        assert_eq!(command_of(&decoded), "ask_session");
    }

    #[test]
    fn missing_fields_default_gracefully() {
        let msg = json!({ "command": "game_info", "uid": 9, "state": "open" });
        let raws = extract_raw_games(&msg);
        assert_eq!(raws.len(), 1);
        let game = raws
            .into_iter()
            .next()
            .unwrap()
            .into_game(&PlayerRatings::new())
            .unwrap();
        assert_eq!(game.id, 9);
        assert_eq!(game.title, "");
        assert_eq!(game.max_players, 0);
    }

    #[test]
    fn accepts_nullable_fields_from_reference_lobby_payload() {
        // The reference clients receive these fields as null for some open
        // games (notably sim_mods, visibility and game_type). A null optional
        // field must not make serde drop the entire game from the list.
        let msg = json!({
            "command": "game_info",
            "uid": 12,
            "state": "open",
            "num_players": 1,
            "max_players": 4,
            "title": "Open game",
            "host": "Commander",
            "mapname": "scmp_007",
            "featured_mod": "faf",
            "password_protected": false,
            "visibility": null,
            "game_type": null,
            "sim_mods": null,
            "teams": null,
            "rating_min": 0,
            "rating_max": 3000
        });

        let game = extract_raw_games(&msg)
            .into_iter()
            .next()
            .and_then(|raw| raw.into_game(&PlayerRatings::new()))
            .expect("nullable game payload should be retained");
        assert_eq!(game.id, 12);
        assert_eq!(game.visibility, "public");
        assert_eq!(game.game_type, "custom");
        assert!(game.sim_mods.is_empty());
        assert!(game.teams.is_empty());
    }
}
