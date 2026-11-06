//! Discord Rich Presence: driven through the real runtime.
//!
//! The presence watcher is the only part of the client with no command behind
//! it: it observes the event stream the runtime broadcasts. So these tests
//! drive the runtime with real commands and a lobby the test scripts, then
//! watch what reaches a recording [`DiscordPort`].

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use faf_app::infra::{fake_ports, FakeAuth};
use faf_app::ports::{DiscordPort, DiscordRequest, LobbyPort, LobbyUpdate};
use faf_app::{App, Ports};
use faf_domain::protocol::discord::Activity;
use faf_domain::state::{
    AuthCommand, DiscordPreferences, Game, HostGameConfig, LobbyCommand, MatchmakingState, Player,
    PlayerVeto, Relation, SettingsCommand,
};
use serde_json::Value;
use tokio::sync::mpsc;

/// Records every presence the watcher publishes, in order.
struct RecordingDiscord {
    published: Arc<Mutex<Vec<Option<Activity>>>>,
}

#[async_trait]
impl DiscordPort for RecordingDiscord {
    fn set_presence(&self, activity: Option<Activity>) {
        self.published.lock().unwrap().push(activity);
    }

    async fn requests(&self) -> mpsc::Receiver<DiscordRequest> {
        let (_tx, rx) = mpsc::channel(1);
        rx
    }
}

/// A lobby whose game snapshots the test pushes by hand.
struct ScriptedLobby {
    updates: Mutex<Option<mpsc::Sender<LobbyUpdate>>>,
}

#[async_trait]
impl LobbyPort for ScriptedLobby {
    async fn connect(&self) -> mpsc::Receiver<LobbyUpdate> {
        let (tx, rx) = mpsc::channel(16);
        *self.updates.lock().unwrap() = Some(tx);
        rx
    }
    fn join(&self, _id: i32, _password: Option<String>) -> bool {
        true
    }
    fn host(&self, _config: HostGameConfig) {}
    fn matchmake(&self, _queue_name: String, _start: bool) {}
    fn leave_party(&self) {}
    fn kick_party_member(&self, _player_id: i32) {}
    fn invite_to_party(&self, _player_id: i32) {}
    fn accept_party_invite(&self, _player_id: i32) {}
    fn set_party_factions(&self, _factions: Vec<String>) {}
    fn set_contact_relation(
        &self,
        _contact: faf_domain::state::Contact,
        _member: bool,
        _remove: Option<Relation>,
    ) -> bool {
        false
    }
    fn set_player_vetoes(&self, _vetoes: Vec<PlayerVeto>) {}
    fn request_avatars(&self) -> bool {
        self.updates.lock().unwrap().is_some()
    }
    fn select_avatar(&self, _url: Option<String>) -> bool {
        self.updates.lock().unwrap().is_some()
    }
    fn restore_game_session(&self, _game_id: i32) -> bool {
        true
    }

    fn send_game_relay(&self, _command: String, _args: Vec<Value>) {}
    fn disconnect(&self) {
        self.updates.lock().unwrap().take();
    }
}

struct Harness {
    app: App,
    published: Arc<Mutex<Vec<Option<Activity>>>>,
    lobby: Arc<ScriptedLobby>,
}

impl Harness {
    async fn start() -> Self {
        let published = Arc::new(Mutex::new(Vec::new()));
        let lobby = Arc::new(ScriptedLobby {
            updates: Mutex::new(None),
        });
        let ports = Ports {
            auth: Arc::new(FakeAuth {
                player: Player::new(7, "Ada"),
                delay: Duration::ZERO,
                fail_with: None,
            }),
            discord: Arc::new(RecordingDiscord {
                published: published.clone(),
            }),
            lobby: lobby.clone(),
            ..fake_ports()
        };
        let (app, app_loop) = App::new("test", ports);
        tokio::spawn(app_loop.run());

        let harness = Self {
            app,
            published,
            lobby,
        };
        harness
            .app
            .dispatch(AuthCommand::Login { remember: false }.into())
            .await
            .unwrap();
        harness
            .app
            .dispatch(LobbyCommand::Connect.into())
            .await
            .unwrap();
        // The connect handler installs the sender on its own task.
        harness.wait_for_connection().await;
        harness
    }

    async fn wait_for_connection(&self) {
        for _ in 0..200 {
            if self.lobby.updates.lock().unwrap().is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the lobby never connected");
    }

    async fn push(&self, update: LobbyUpdate) {
        let sender = self.lobby.updates.lock().unwrap().clone();
        sender.expect("connected").send(update).await.unwrap();
    }

    fn published_count(&self) -> usize {
        self.published.lock().unwrap().len()
    }

    /// The most recent presence, once more than `after` have been published.
    ///
    /// Polls because the watcher runs on its own task: the command that
    /// triggers it returns before the recompute lands.
    async fn next_presence(&self, after: usize) -> Option<Activity> {
        for _ in 0..300 {
            {
                let published = self.published.lock().unwrap();
                if published.len() > after {
                    return published.last().cloned().flatten();
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "expected more than {after} presence updates, saw {}",
            self.published_count()
        );
    }
}

fn game(id: i32, host: &str, members: &[&str]) -> Game {
    let mut teams = BTreeMap::new();
    teams.insert(
        "1".to_string(),
        members.iter().map(|m| m.to_string()).collect(),
    );
    Game {
        id,
        title: "all welcome".into(),
        host: host.into(),
        players: members.len() as i32,
        max_players: 8,
        map: "scmp_009".into(),
        mod_name: "faf".into(),
        average_rating: 1200,
        rating_type: "global".into(),
        password_protected: false,
        visibility: "public".into(),
        game_type: "custom".into(),
        launched_at: None,
        hosted_at: None,
        rating_min: None,
        rating_max: None,
        enforce_rating_range: false,
        teams,
        sim_mods: BTreeMap::new(),
    }
}

#[tokio::test]
async fn hosting_a_lobby_publishes_it_and_leaving_clears_it() {
    let h = Harness::start().await;

    let before = h.published_count();
    h.push(LobbyUpdate::Games(vec![game(42, "Ada", &["Ada", "Bob"])]))
        .await;

    let activity = h.next_presence(before).await.expect("a presence");
    assert_eq!(activity.state, "Hosting");
    assert_eq!(activity.details, "faf | all welcome");
    assert_eq!(activity.party, Some(("42".into(), 2, 8)));
    assert_eq!(activity.join_secret.as_deref(), Some(r#"{"gameId":42}"#));

    // Leaving the lobby removes us from the server's team lists.
    let before = h.published_count();
    h.push(LobbyUpdate::Games(vec![game(42, "Bob", &["Bob"])]))
        .await;
    assert_eq!(
        h.next_presence(before).await,
        None,
        "a status must be cleared, not left showing a game we left"
    );
}

#[tokio::test]
async fn a_game_that_starts_switches_to_playing_with_a_timer() {
    let h = Harness::start().await;

    let before = h.published_count();
    h.push(LobbyUpdate::Games(vec![game(42, "Ada", &["Ada"])]))
        .await;
    assert_eq!(h.next_presence(before).await.unwrap().state, "Hosting");

    // The server moves the game to the live list once it launches.
    let before = h.published_count();
    h.push(LobbyUpdate::Games(vec![])).await;
    h.push(LobbyUpdate::LiveGames(vec![Game {
        launched_at: Some(1_800_000_000),
        ..game(42, "Ada", &["Ada"])
    }]))
    .await;

    let activity = h.next_presence(before).await.expect("a presence");
    assert_eq!(activity.state, "Playing");
    assert_eq!(activity.start_timestamp, Some(1_800_000_000));
    assert_eq!(activity.join_secret, None, "the lobby is closed");
}

#[tokio::test]
async fn a_stranger_s_lobby_is_never_published() {
    // The lobby list is everyone's games. Publishing the first one would
    // advertise a stranger's lobby as our own.
    let h = Harness::start().await;
    let before = h.published_count();
    h.push(LobbyUpdate::Games(vec![game(42, "Bob", &["Bob", "Cid"])]))
        .await;
    assert_eq!(h.next_presence(before).await, None);
}

#[tokio::test]
async fn turning_rich_presence_off_clears_the_status_immediately() {
    let h = Harness::start().await;
    let before = h.published_count();
    h.push(LobbyUpdate::Games(vec![game(42, "Ada", &["Ada"])]))
        .await;
    assert!(h.next_presence(before).await.is_some());

    let before = h.published_count();
    h.app
        .dispatch(
            SettingsCommand::PatchDiscord {
                patch: DiscordPreferences {
                    enabled: false,
                    disallow_joins: false,
                }
                .into(),
            }
            .into(),
        )
        .await
        .unwrap();

    assert_eq!(
        h.next_presence(before).await,
        None,
        "the switch must take the status down, not merely stop refreshing it"
    );
}

#[tokio::test]
async fn disallowing_joins_withholds_the_secret_but_keeps_the_status() {
    let h = Harness::start().await;
    let before = h.published_count();
    h.push(LobbyUpdate::Games(vec![game(42, "Ada", &["Ada"])]))
        .await;
    assert!(h.next_presence(before).await.is_some());

    let before = h.published_count();
    h.app
        .dispatch(
            SettingsCommand::PatchDiscord {
                patch: DiscordPreferences {
                    enabled: true,
                    disallow_joins: true,
                }
                .into(),
            }
            .into(),
        )
        .await
        .unwrap();

    let activity = h.next_presence(before).await.expect("still published");
    assert_eq!(activity.state, "Hosting");
    assert_eq!(
        activity.join_secret, None,
        "the preference is about being joinable, not about being invisible"
    );
}

#[tokio::test]
async fn unrelated_lobby_traffic_leaves_the_status_alone() {
    // Recomputation is idempotent: matchmaker chatter arrives on the same
    // stream as game snapshots and is recomputed from, but must derive the
    // same activity. (Suppressing the redundant *write* is the port's job,
    // see `infra::discord`'s revision check.)
    let h = Harness::start().await;
    let before = h.published_count();
    h.push(LobbyUpdate::Games(vec![game(42, "Ada", &["Ada"])]))
        .await;
    let hosting = h.next_presence(before).await.expect("a presence");

    for _ in 0..5 {
        h.push(LobbyUpdate::Matchmaking(MatchmakingState::Idle))
            .await;
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let published = h.published.lock().unwrap();
    assert_eq!(
        published.last().cloned().flatten(),
        Some(hosting),
        "the status must still describe the lobby we are in"
    );
}
