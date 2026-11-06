//! Fake lobby provider: emits an evolving game list without any network.
//!
//! Stands in for the real FAF lobby protocol. On `connect` it sends an immediate
//! snapshot, then mutates the list every couple of seconds (player counts change,
//! games come and go) so the live-update path is visibly exercised. `join` pushes
//! a synthetic `game_launch` back on the same stream after a short delay, so the
//! join path is exercised end-to-end offline. `disconnect` cancels the loop,
//! exercising the same teardown path as the real client.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use faf_domain::state::{
    AvailableAvatar, Contact, Game, GameLaunch, HostGameConfig, MatchmakerQueue, MatchmakingState,
    PlayerProfile, PlayerVeto, RatingRange, Relation,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::ports::{LobbyPort, LobbyUpdate};

/// Interval between simulated lobby updates.
const TICK: Duration = Duration::from_secs(2);
/// Delay before the fake server "accepts" a join and replies with a launch order.
const JOIN_DELAY: Duration = Duration::from_millis(150);

/// A game relay message as sent: the command and its arguments.
pub type GameRelay = (String, Vec<serde_json::Value>);

#[derive(Debug, Clone, Default)]
pub struct FakeLobby {
    /// Cancels the in-flight connection's update loop. Shared so `disconnect`
    /// (a separate call) can reach the task started by `connect`.
    cancel: Arc<Mutex<Option<CancellationToken>>>,
    /// The live connection's update sender, so `join` (a separate call) can push a
    /// reply onto the same stream the service is draining.
    updates: Arc<Mutex<Option<mpsc::Sender<LobbyUpdate>>>>,
    matchmaking: Arc<Mutex<MatchmakingState>>,
    hosted: Arc<Mutex<Vec<HostGameConfig>>>,
    /// When set, `join` records the request and leaves the answer to the
    /// test (`answer_join`), so a test can act between the two.
    held_joins: Arc<Mutex<Option<Vec<i32>>>>,
    relays: Arc<Mutex<Vec<GameRelay>>>,
}

impl FakeLobby {
    /// Inject a server update into the active fake connection. Kept narrow so
    /// integration tests can exercise transition-only behavior (offline,
    /// invites, failures) without sleeping for the demo ticker.
    pub fn push_update(&self, update: LobbyUpdate) -> bool {
        self.updates
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|sender| sender.try_send(update).is_ok())
    }

    pub fn hosted_configs(&self) -> Vec<HostGameConfig> {
        self.hosted.lock().unwrap().clone()
    }

    /// Stop answering joins on a timer; see `held_joins` and `answer_join`.
    pub fn hold_join_answers(&self) {
        *self.held_joins.lock().unwrap() = Some(Vec::new());
    }

    /// The join requests sent while answers are held, in order.
    pub fn held_joins(&self) -> Vec<i32> {
        self.held_joins.lock().unwrap().clone().unwrap_or_default()
    }

    /// Answer a join with its launch order, as the server would.
    pub fn answer_join(&self, id: i32) -> bool {
        self.push_update(LobbyUpdate::Launch(fake_launch(id)))
    }

    /// Every game relay message sent, in order.
    pub fn game_relays(&self) -> Vec<GameRelay> {
        self.relays.lock().unwrap().clone()
    }
}

#[async_trait]
impl LobbyPort for FakeLobby {
    async fn connect(&self) -> mpsc::Receiver<LobbyUpdate> {
        let token = CancellationToken::new();
        // Replace (and cancel) any previous connection.
        if let Some(prev) = self.cancel.lock().unwrap().replace(token.clone()) {
            prev.cancel();
        }

        let (tx, rx) = mpsc::channel(8);
        *self.updates.lock().unwrap() = Some(tx.clone());
        *self.matchmaking.lock().unwrap() = MatchmakingState::Idle;
        tokio::spawn(async move {
            let mut games = seed_games();
            // The real transport only reports this once the server has accepted
            // the handshake; the fake has no handshake, but the service must not
            // be able to tell the two apart.
            if tx.send(LobbyUpdate::Authenticated).await.is_err() {
                return;
            }
            // Immediate first snapshot so the UI fills instantly.
            if tx.send(LobbyUpdate::Games(games.clone())).await.is_err() {
                return;
            }
            // Identity + relations, so the offline path exercises the chat
            // roster's ranking (friends above strangers, accounts above
            // IRC-only nicknames), its flags and its clan tags instead of
            // flattening everyone into one unadorned group.
            let _ = tx.send(LobbyUpdate::PlayersSeen(seed_profiles())).await;
            let _ = tx
                .send(LobbyUpdate::Relations {
                    friends: vec!["Stormlord".into()],
                    foes: vec![],
                    contacts: vec![faf_domain::state::Contact {
                        player_id: 2,
                        login: Some("Stormlord".into()),
                        relation: Relation::Friend,
                    }],
                })
                .await;
            // The real `social` message carries the channels the server has on
            // file for this account, without the `#`. Language channels are
            // *not* in it: those are derived client-side (see
            // `faf_domain::state::auto_join_channels`), so the fake list here is
            // the default channel plus a clan channel, which is what the real
            // one actually looks like.
            let _ = tx
                .send(LobbyUpdate::AutoJoinChannels(vec![
                    "aeolus".into(),
                    "clan_bc".into(),
                ]))
                .await;
            let _ = tx
                .send(LobbyUpdate::MatchmakerQueues(vec![
                    MatchmakerQueue {
                        queue_name: "ladder1v1".into(),
                        team_size: 1,
                        num_players: 18,
                        queue_pop_time_seconds: 95,
                        queue_pops_at: String::new(),
                        // A spread of searches so the offline shell shows the
                        // "in your range" count doing something. The 75%
                        // windows are the wider ones, as on the server.
                        boundary_80s: vec![
                            RatingRange {
                                min: 800,
                                max: 1_200,
                            },
                            RatingRange {
                                min: 1_100,
                                max: 1_500,
                            },
                            RatingRange {
                                min: 1_600,
                                max: 2_000,
                            },
                        ],
                        boundary_75s: vec![
                            RatingRange {
                                min: 700,
                                max: 1_300,
                            },
                            RatingRange {
                                min: 1_000,
                                max: 1_600,
                            },
                            RatingRange {
                                min: 1_500,
                                max: 2_100,
                            },
                        ],
                    },
                    MatchmakerQueue {
                        queue_name: "tmm2v2".into(),
                        team_size: 2,
                        num_players: 12,
                        queue_pop_time_seconds: 150,
                        queue_pops_at: String::new(),
                        boundary_80s: vec![RatingRange {
                            min: 900,
                            max: 1_300,
                        }],
                        boundary_75s: vec![RatingRange {
                            min: 800,
                            max: 1_400,
                        }],
                    },
                    MatchmakerQueue {
                        queue_name: "tmm4v4".into(),
                        team_size: 4,
                        num_players: 20,
                        queue_pop_time_seconds: 210,
                        queue_pops_at: String::new(),
                        boundary_80s: Vec::new(),
                        boundary_75s: Vec::new(),
                    },
                ]))
                .await;
            let mut tick: u32 = 0;
            loop {
                tokio::select! {
                    _ = token.cancelled() => break, // disconnect requested
                    _ = tokio::time::sleep(TICK) => {}
                }
                tick = tick.wrapping_add(1);
                evolve(&mut games, tick);
                if tx.send(LobbyUpdate::Games(games.clone())).await.is_err() {
                    break; // receiver dropped: consumer gone, stop.
                }
            }
        });
        rx
    }

    fn join(&self, id: i32, _password: Option<String>) -> bool {
        // Push a synthetic launch order back on the live stream, mimicking the
        // server's `game_launch`. No-op if there's no active connection.
        let Some(tx) = self.updates.lock().unwrap().clone() else {
            return false;
        };
        if let Some(held) = self.held_joins.lock().unwrap().as_mut() {
            held.push(id);
            return true;
        }
        tokio::spawn(async move {
            tokio::time::sleep(JOIN_DELAY).await;
            let _ = tx.send(LobbyUpdate::Launch(fake_launch(id))).await;
        });
        true
    }

    fn host(&self, config: HostGameConfig) {
        self.hosted.lock().unwrap().push(config.clone());
        let Some(tx) = self.updates.lock().unwrap().clone() else {
            return;
        };
        tokio::spawn(async move {
            tokio::time::sleep(JOIN_DELAY).await;
            let mut launch = fake_launch(200);
            launch.name = config.title;
            launch.mapname = config.map;
            launch.mod_name = config.mod_name;
            let _ = tx.send(LobbyUpdate::Launch(launch)).await;
        });
    }

    fn matchmake(&self, queue_name: String, start: bool) {
        let Some(tx) = self.updates.lock().unwrap().clone() else {
            return;
        };
        let state = {
            let mut matchmaking = self.matchmaking.lock().unwrap();
            matchmaking.update_search(queue_name, start);
            matchmaking.clone()
        };
        tokio::spawn(async move {
            let _ = tx.send(LobbyUpdate::Matchmaking(state)).await;
        });
    }

    fn leave_party(&self) {}

    fn kick_party_member(&self, _player_id: i32) {}

    fn invite_to_party(&self, _player_id: i32) {}

    fn accept_party_invite(&self, _player_id: i32) {}

    fn set_party_factions(&self, _factions: Vec<String>) {}

    /// No-op: the real server sends no acknowledgement either, so the social
    /// service's optimistic event is the whole observable effect.
    fn set_contact_relation(
        &self,
        _contact: Contact,
        _member: bool,
        _remove: Option<Relation>,
    ) -> bool {
        self.updates
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|sender| !sender.is_closed())
    }

    fn set_player_vetoes(&self, vetoes: Vec<PlayerVeto>) {
        let Some(tx) = self.updates.lock().unwrap().clone() else {
            return;
        };
        tokio::spawn(async move {
            let _ = tx
                .send(LobbyUpdate::Vetoes {
                    vetoes,
                    forced: false,
                })
                .await;
        });
    }

    fn request_avatars(&self) -> bool {
        let Some(tx) = self.updates.lock().unwrap().clone() else {
            return false;
        };
        tokio::spawn(async move {
            let _ = tx
                .send(LobbyUpdate::Avatars(vec![
                    AvailableAvatar {
                        url: "https://content.faforever.com/faf/avatars/GW_Cybran.png".into(),
                        tooltip: "Cybran Galactic War".into(),
                    },
                    AvailableAvatar {
                        url: "https://content.faforever.com/faf/avatars/GW_Aeon.png".into(),
                        tooltip: "Aeon Galactic War".into(),
                    },
                ]))
                .await;
        });
        true
    }

    fn select_avatar(&self, _url: Option<String>) -> bool {
        self.updates.lock().unwrap().is_some()
    }

    fn restore_game_session(&self, _game_id: i32) -> bool {
        // Nothing to restore: the fake has no server-side game connection to
        // lose in the first place.
        self.updates.lock().unwrap().is_some()
    }

    fn send_game_relay(&self, command: String, args: Vec<serde_json::Value>) {
        // The fake stops at the launch order; it doesn't simulate in-game
        // relay, only records what was sent.
        self.relays.lock().unwrap().push((command, args));
    }

    fn disconnect(&self) {
        if let Some(token) = self.cancel.lock().unwrap().take() {
            token.cancel();
        }
        // Drop the sender handle so a later `join` before reconnect is a no-op.
        *self.updates.lock().unwrap() = None;
    }
}

/// A handful of accounts matching `FakeChat`'s seeded roster, with countries,
/// a clan tag and an avatar so the roster's decorations are visible offline.
/// Deliberately does *not* cover every seeded nickname: the ones left out
/// stand in for IRC-only users.
fn seed_profiles() -> Vec<PlayerProfile> {
    let profile = |id, login: &str, country: &str, clan: &str, avatar: &str| PlayerProfile {
        id,
        login: login.into(),
        global_rating: 900 + id * 125,
        ratings: vec![faf_domain::state::PlayerLobbyRating {
            leaderboard: "global".into(),
            rating: 900 + id * 125,
            mean: 1_350 + id * 125,
            deviation: 150,
            games_played: 120 + id,
        }],
        country: country.into(),
        clan: clan.into(),
        avatar_url: avatar.into(),
        avatar_tooltip: if avatar.is_empty() { "" } else { "Seraphim" }.into(),
    };
    vec![
        profile(1, "ArchSupport", "us", "", ""),
        profile(2, "Stormlord", "de", "BC", ""),
        profile(3, "Aurora", "fr", "", ""),
        profile(4, "Sheikah", "gb", "", ""),
        profile(5, "BlackYps", "at", "", ""),
        profile(6, "Petricpwnz", "ru", "", ""),
    ]
}

fn seed_games() -> Vec<Game> {
    vec![
        Game {
            id: 1,
            title: "Ranked 1v1".into(),
            host: "Stormlord".into(),
            players: 1,
            max_players: 2,
            map: "Theta Passage".into(),
            mod_name: "faf".into(),
            average_rating: 1450,
            rating_type: "global".into(),
            password_protected: false,
            visibility: "public".into(),
            game_type: "custom".into(),
            launched_at: None,
            hosted_at: None,
            rating_min: None,
            rating_max: None,
            enforce_rating_range: false,
            teams: BTreeMap::from([("1".into(), vec!["Stormlord".into()])]),
            sim_mods: Default::default(),
        },
        Game {
            id: 2,
            title: "Team Battle".into(),
            host: "Aurora".into(),
            players: 5,
            max_players: 8,
            map: "Seton's Clutch".into(),
            mod_name: "faf".into(),
            average_rating: 1100,
            rating_type: "global".into(),
            password_protected: false,
            visibility: "public".into(),
            game_type: "custom".into(),
            launched_at: None,
            hosted_at: None,
            rating_min: Some(700),
            rating_max: Some(1600),
            enforce_rating_range: false,
            teams: BTreeMap::from([
                (
                    "1".into(),
                    vec!["Aurora".into(), "Sheikah".into(), "ArchSupport".into()],
                ),
                ("2".into(), vec!["BlackYps".into(), "Petricpwnz".into()]),
            ]),
            sim_mods: Default::default(),
        },
        Game {
            id: 3,
            title: "Sandbox".into(),
            host: "Vex".into(),
            players: 2,
            max_players: 12,
            map: "Open Palms".into(),
            mod_name: "faf".into(),
            average_rating: 900,
            rating_type: "global".into(),
            password_protected: true,
            visibility: "public".into(),
            game_type: "custom".into(),
            launched_at: None,
            hosted_at: None,
            rating_min: None,
            rating_max: None,
            enforce_rating_range: false,
            teams: Default::default(),
            sim_mods: Default::default(),
        },
    ]
}

/// A plausible `game_launch` for the joined game, so the offline join path lands
/// in `JoinState::Launched`.
fn fake_launch(id: i32) -> GameLaunch {
    GameLaunch {
        uid: id,
        mod_name: "faf".into(),
        name: format!("Game {id}"),
        mapname: "scmp_009".into(),
        game_type: "custom".into(),
        rating_type: "global".into(),
        expected_players: None,
        team: None,
        faction: None,
        map_position: None,
        game_options: Default::default(),
        args: vec!["/numgames".into(), "0".into()],
    }
}

/// Mutate the list a little each tick: bump a player count, and every few ticks
/// toggle a transient game in and out so additions/removals are exercised too.
fn evolve(games: &mut Vec<Game>, tick: u32) {
    if let Some(g) = games.first_mut() {
        g.players = 1 + (tick % g.max_players.max(1) as u32) as i32;
    }

    const TRANSIENT_ID: i32 = 99;
    let present = games.iter().any(|g| g.id == TRANSIENT_ID);
    if tick.is_multiple_of(3) && !present {
        games.push(Game {
            id: TRANSIENT_ID,
            title: "Quick Match".into(),
            host: "Nomad".into(),
            players: 3,
            max_players: 4,
            map: "Canis River".into(),
            mod_name: "faf".into(),
            average_rating: 1250,
            rating_type: "global".into(),
            password_protected: false,
            visibility: "public".into(),
            game_type: "custom".into(),
            launched_at: None,
            hosted_at: None,
            rating_min: None,
            rating_max: None,
            enforce_rating_range: false,
            teams: Default::default(),
            sim_mods: Default::default(),
        });
    } else if !tick.is_multiple_of(3) && present {
        games.retain(|g| g.id != TRANSIENT_ID);
    }
}
