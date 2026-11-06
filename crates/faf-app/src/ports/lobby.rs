//! Lobby port: a *bidirectional, streaming* boundary.
//!
//! Unlike [`AuthPort`](crate::ports::AuthPort) (request/response), the lobby pushes
//! data over time: `connect` returns a receiver that yields a [`LobbyUpdate`]
//! whenever the server's view changes. It is also bidirectional: [`Self::join`]
//! sends a `game_join` over the *same* authenticated connection, and the server's
//! `game_launch` / `game_join_failed` reply arrives back on the very same update
//! stream. The real impl wraps the FAF lobby WS protocol; the fake simulates it.
//! The service is identical against either.

use async_trait::async_trait;
use faf_domain::state::{
    AvailableAvatar, Contact, Game, GameLaunch, HostGameConfig, MatchmakerQueue, MatchmakingState,
    PartyState, PlayerProfile, PlayerVeto, Relation,
};
use serde_json::Value;
use tokio::sync::mpsc;

/// Operational severity attached to a lobby `notice` frame.
///
/// `Kill` and `Kick` are commands as well as presentation hints: the former
/// terminates the active FA process, while the latter ends this lobby session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerNoticeStyle {
    Info,
    Warning,
    Error,
    Kill,
    Kick,
}

/// One thing the lobby connection tells us about. Game-list snapshots, join
/// replies, and in-game relay traffic all travel the same socket, so they share
/// one stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LobbyUpdate {
    /// The server finished the handshake and accepted this client.
    ///
    /// Distinct from the socket being open, and the distinction matters: the
    /// lobby refuses everything but the handshake until this point and drops
    /// the connection over an early command. This, not `connect` returning, is
    /// when the lobby becomes usable.
    Authenticated,
    /// The connection was lost and the port is opening another one by itself.
    ///
    /// Distinct from the update stream ending, which is what the service reads
    /// as a real disconnection. This says the session is still alive and
    /// currently between sockets, so the client can stop claiming a connection
    /// it does not have without tearing down everything it knows. The next
    /// [`Self::Authenticated`] means the replacement is up.
    Reconnecting,
    /// A fresh full snapshot of the open-games list.
    ///
    /// Sent when the list is being *replaced*: the server's opening dump, and
    /// the first frames after a reconnect. Incremental changes go through
    /// [`Self::GamesChanged`] instead.
    Games(Vec<Game>),
    /// A fresh full snapshot of the in-progress ("playing") games list.
    LiveGames(Vec<Game>),
    /// Games that appeared, changed or left the open list since the last
    /// update. See `faf_domain::state::lobby::LobbyEvent::GamesChanged`.
    GamesChanged {
        upserted: Vec<Game>,
        removed: Vec<i32>,
    },
    /// The same, for the in-progress list.
    LiveGamesChanged {
        upserted: Vec<Game>,
        removed: Vec<i32>,
    },
    MatchmakerQueues(Vec<MatchmakerQueue>),
    Matchmaking(MatchmakingState),
    Party(PartyState),
    PartyInvite {
        player_id: i32,
        login: String,
    },
    /// The vetoes the server holds for this player, after it changed them.
    ///
    /// `forced` is the server's own flag: a token on a map was taken back
    /// because the pools changed under the selection, rather than a selection
    /// just sent being trimmed to the rules. Java tells the player only then
    /// (`TeamMatchmakingService.onVetoesChanged`).
    Vetoes {
        vetoes: Vec<PlayerVeto>,
        forced: bool,
    },
    /// Our friends/foes lists, resolved from account ids to logins. Re-sent
    /// whenever a `player_info` resolves an id we couldn't name before.
    Relations {
        friends: Vec<String>,
        foes: Vec<String>,
        contacts: Vec<faf_domain::state::Contact>,
    },
    /// Channels the server says this account belongs in (language, clan),
    /// from the same `social` message as the relations above.
    AutoJoinChannels(Vec<String>),
    /// Profiles newly announced or changed by `player_info`. Additive, not a
    /// snapshot (see `faf_domain::state::social`).
    PlayersSeen(Vec<PlayerProfile>),
    /// Profiles carrying the authoritative `state: offline` transition.
    /// Includes the last known profile so the service can classify the
    /// departure before removing it from online state.
    PlayersRemoved(Vec<PlayerProfile>),
    /// Available choices returned by `avatar/list_avatar`.
    Avatars(Vec<AvailableAvatar>),
    /// A server-authored operational message. Unlike ordinary event alerts,
    /// these must remain visible because they may explain a forced disconnect
    /// or game termination.
    Notice {
        style: ServerNoticeStyle,
        text: String,
    },
    /// The server refused this account because it is banned (#488), with
    /// its reason as plain text. The transport has stopped for good: another
    /// attempt would be refused the same way, and each refusal used to arrive
    /// as one more notification.
    Banned {
        text: String,
    },
    /// Authentication/protocol rejection with the server's authoritative
    /// reason. The transport closes immediately after sending this update.
    ConnectionRejected {
        reason: String,
    },
    /// The server accepted a join and issued the launch order.
    Launch(GameLaunch),
    /// The server rejected a join (game not ready, host left, bad password, …).
    JoinFailed {
        id: i32,
        reason: String,
    },
    /// A connectivity message addressed to the game (`target: "game"`),
    /// `HostGame`/`JoinGame`/`ConnectToPeer`/`IceMsg`/…: to be relayed to the
    /// ICE adapter. `args` keep their JSON types (ints vs strings) for the
    /// GPGNet codec.
    GameRelay {
        command: String,
        args: Vec<Value>,
    },
}

#[async_trait]
pub trait LobbyPort: Send + Sync {
    /// Retry names for unresolved relations without blocking the socket reader.
    async fn resolve_contacts(&self, _ids: &[i32]) -> Result<(), String> {
        Err("contact lookup is unavailable".into())
    }
    /// Connect to the lobby. The receiver yields a [`LobbyUpdate`] on each change;
    /// it closes when the connection ends (server-side or via [`Self::disconnect`]).
    async fn connect(&self) -> mpsc::Receiver<LobbyUpdate>;

    /// Request to join game `id` over the live connection (sends `game_join`). The
    /// reply arrives asynchronously on the [`Self::connect`] stream as
    /// [`LobbyUpdate::Launch`] or [`LobbyUpdate::JoinFailed`]. A no-op if there is
    /// no active connection.
    /// Returns whether the request entered the live socket's outgoing queue.
    /// A disconnected or saturated queue must become a visible join failure,
    /// not an indefinitely spinning optimistic state.
    fn join(&self, id: i32, password: Option<String>) -> bool;

    /// Create a custom game using the same `game_host` payload as the reference
    /// client. The accepted request eventually produces a `game_launch` update.
    fn host(&self, config: HostGameConfig);

    /// Start or stop searching a named matchmaker queue.
    fn matchmake(&self, queue_name: String, start: bool);

    fn leave_party(&self);

    fn kick_party_member(&self, player_id: i32);

    /// Invite a player to our party (`invite_to_party`).
    fn invite_to_party(&self, player_id: i32);

    fn accept_party_invite(&self, player_id: i32);

    /// Set the factions this player is willing to receive in matchmaker games.
    fn set_party_factions(&self, factions: Vec<String>);

    /// Queue a friend/foe change, including the removal of an opposite
    /// relation when switching lists. Returns false if neither could be queued.
    /// The server sends no acknowledgement for these messages.
    fn set_contact_relation(
        &self,
        contact: Contact,
        member: bool,
        remove: Option<Relation>,
    ) -> bool;

    fn set_player_vetoes(&self, vetoes: Vec<PlayerVeto>);

    /// Request the authenticated player's available avatars. Returns `false`
    /// when there is no live connection or the outgoing queue is full.
    fn request_avatars(&self) -> bool;

    /// Select an avatar by its server-provided URL, or clear it with `None`.
    /// Returns whether the command was accepted by the outgoing connection.
    fn select_avatar(&self, url: Option<String>) -> bool;

    /// Tell the server this client is still in game `game_id`
    /// (`restore_game_session`).
    ///
    /// The lobby server ties a player's game connection to the socket it was
    /// made on. When that socket goes -- a drop, or the user disconnecting and
    /// reconnecting by hand -- the connection goes with it, and the running
    /// game keeps talking to an adapter the server no longer relays for: the
    /// players never reconnect to each other. This is the frame that puts the
    /// session back, and the reference client sends it on every transition
    /// back to connected while a game is running.
    ///
    /// Returns whether it entered the live socket's outgoing queue.
    fn restore_game_session(&self, game_id: i32) -> bool;

    /// Relay a connectivity message to the server addressed to the game
    /// (`{ command, target: "game", args }`). Used by the launcher to forward
    /// GPGNet/ICE messages produced by the local adapter. A no-op if there is no
    /// active connection.
    fn send_game_relay(&self, command: String, args: Vec<Value>);

    /// Cancel the active connection, if any. Idempotent: closing an already-closed
    /// connection is a no-op. Closing drops the update sender, which ends the
    /// receiver returned by [`Self::connect`].
    fn disconnect(&self);
}
