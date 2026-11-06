//! The five-minute live-replay delay.
//!
//! An anti-ghosting rule: without it, anyone could open a live replay of a game
//! they are not in and read their opponent's scouting and army positions in
//! real time. The FAF replay server holds the stream back, and the client must
//! refuse to launch before then on *every* route: the Watch button, and a
//! Discord spectate click, both land on the same command.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use faf_app::infra::fake_ports;
use faf_app::ports::{LobbyPort, LobbyUpdate, ReplayPlaybackPort};
use faf_app::{App, Ports};
use faf_domain::state::{
    Game, HostGameConfig, LiveReplayTarget, LiveReplayTrackingAction, LobbyCommand,
    NotificationAction, NotificationKind, PlayerVeto, Relation, ReplayCommand, ReplayEvent,
    LIVE_REPLAY_DELAY_SECONDS,
};
use faf_domain::AppEvent;
use serde_json::Value;
use tokio::sync::mpsc;

/// Records whether the port was ever actually asked to open a live stream.
struct RecordingReplay {
    watched: Arc<Mutex<Vec<i32>>>,
}

#[async_trait]
impl ReplayPlaybackPort for RecordingReplay {
    async fn watch_live(
        &self,
        target: LiveReplayTarget,
        _player: String,
    ) -> Result<Option<String>, String> {
        self.watched.lock().unwrap().push(target.uid);
        Ok(None)
    }
    async fn play_file(&self, _path: PathBuf) -> Result<Option<String>, String> {
        Ok(None)
    }
    async fn watch_vault(&self, _uid: i32) -> Result<Option<String>, String> {
        Ok(None)
    }

    fn set_install_dir(&self, _dir: Option<PathBuf>) {}
}

/// A lobby whose live-game snapshots the test pushes by hand.
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
    watched: Arc<Mutex<Vec<i32>>>,
    lobby: Arc<ScriptedLobby>,
}

impl Harness {
    async fn start() -> Self {
        let watched = Arc::new(Mutex::new(Vec::new()));
        let lobby = Arc::new(ScriptedLobby {
            updates: Mutex::new(None),
        });
        let ports = Ports {
            replay_playback: Arc::new(RecordingReplay {
                watched: watched.clone(),
            }),
            lobby: lobby.clone(),
            ..fake_ports()
        };
        let (app, app_loop) = App::new("test", ports);
        tokio::spawn(app_loop.run());

        let harness = Self {
            app,
            watched,
            lobby,
        };
        harness
            .app
            .dispatch(LobbyCommand::Connect.into())
            .await
            .unwrap();
        for _ in 0..200 {
            if harness.lobby.updates.lock().unwrap().is_some() {
                return harness;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the lobby never connected");
    }

    /// Seed the live-game list with one match that started `age` seconds ago.
    async fn live_game_started(&self, uid: i32, age: u32) {
        let now = u32::try_from(chrono::Utc::now().timestamp()).unwrap();
        let sender = self
            .lobby
            .updates
            .lock()
            .unwrap()
            .clone()
            .expect("connected");
        sender
            .send(LobbyUpdate::LiveGames(vec![Game {
                launched_at: Some(now - age),
                ..game(uid)
            }]))
            .await
            .unwrap();

        for _ in 0..200 {
            if self
                .app
                .snapshot()
                .lobby
                .live_games
                .iter()
                .any(|g| g.id == uid)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the live game never reached the state");
    }

    async fn watch(&self, uid: i32) -> ReplayEvent {
        let mut events = self.app.subscribe();
        self.app
            .dispatch(
                ReplayCommand::WatchLive(LiveReplayTarget {
                    uid,
                    mod_name: "faf".into(),
                    map: "scmp_009".into(),
                })
                .into(),
            )
            .await
            .unwrap();

        let next = async {
            loop {
                if let Ok(AppEvent::Replays(event)) = events.recv().await {
                    return event;
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), next)
            .await
            .expect("no replay event arrived")
    }

    async fn track(&self, uid: i32, action: LiveReplayTrackingAction) {
        self.app
            .dispatch(
                ReplayCommand::TrackLive {
                    target: LiveReplayTarget {
                        uid,
                        mod_name: "faf".into(),
                        map: "scmp_009".into(),
                    },
                    action,
                }
                .into(),
            )
            .await
            .unwrap();
    }
}

fn game(uid: i32) -> Game {
    Game {
        id: uid,
        title: "ranked match".into(),
        host: "Bob".into(),
        players: 2,
        max_players: 2,
        map: "scmp_009".into(),
        mod_name: "faf".into(),
        average_rating: 1600,
        rating_type: "global".into(),
        password_protected: false,
        visibility: "public".into(),
        game_type: "matchmaker".into(),
        launched_at: None,
        hosted_at: None,
        rating_min: None,
        rating_max: None,
        enforce_rating_range: false,
        teams: BTreeMap::new(),
        sim_mods: BTreeMap::new(),
    }
}

#[tokio::test]
async fn a_match_that_just_started_cannot_be_watched() {
    let h = Harness::start().await;
    h.live_game_started(42, 10).await;

    match h.watch(42).await {
        ReplayEvent::Failed { reason } => {
            assert!(
                reason.contains("five minutes"),
                "the refusal should say why: {reason}"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    let notification = h
        .app
        .snapshot()
        .notifications
        .items
        .into_iter()
        .find(|notification| notification.kind == NotificationKind::Error)
        .expect("the delay refusal should create a notification");
    assert_eq!(
        notification.text.as_ref().map(|text| text.key.as_str()),
        Some("notifications.msg.liveReplayDelayed")
    );
    assert!(
        h.watched.lock().unwrap().is_empty(),
        "the stream must never be opened: a disabled button is not enforcement"
    );
    // The refusal offers to start the replay once the delay is over (#653).
    for _ in 0..200 {
        let snapshot = h.app.snapshot();
        if let Some(notification) = snapshot.notifications.items.first() {
            assert!(matches!(
                notification.action,
                Some(NotificationAction::WatchLiveWhenReady { ref target }) if target.uid == 42
            ));
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the refusal notice never arrived");
}

#[tokio::test]
async fn a_match_past_the_delay_can_be_watched() {
    let h = Harness::start().await;
    h.live_game_started(42, LIVE_REPLAY_DELAY_SECONDS + 1).await;

    assert!(matches!(h.watch(42).await, ReplayEvent::Connecting { .. }));
    for _ in 0..200 {
        if !h.watched.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(*h.watched.lock().unwrap(), vec![42]);
}

#[tokio::test]
async fn a_game_the_lobby_never_reported_is_not_blocked() {
    // Vault replays and games whose start the lobby never announced have no
    // known start time. The server enforces the delay regardless; refusing
    // here would break legitimate playback.
    let h = Harness::start().await;

    assert!(matches!(h.watch(999).await, ReplayEvent::Connecting { .. }));
    for _ in 0..200 {
        if !h.watched.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(*h.watched.lock().unwrap(), vec![999]);
}

#[tokio::test]
async fn notify_tracking_creates_an_actionable_notification_when_ready() {
    let h = Harness::start().await;
    h.live_game_started(42, LIVE_REPLAY_DELAY_SECONDS + 1).await;
    h.track(42, LiveReplayTrackingAction::Notify).await;

    for _ in 0..200 {
        let snapshot = h.app.snapshot();
        if let Some(notification) = snapshot.notifications.items.first() {
            assert_eq!(notification.kind, NotificationKind::ReplayAvailable);
            assert!(matches!(
                notification.action,
                Some(NotificationAction::WatchLive { ref target }) if target.uid == 42
            ));
            assert!(snapshot.replays.live_tracking.is_none());
            assert!(h.watched.lock().unwrap().is_empty());
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the ready notification never arrived");
}

#[tokio::test]
async fn auto_watch_tracking_opens_the_stream_when_ready() {
    let h = Harness::start().await;
    h.live_game_started(42, LIVE_REPLAY_DELAY_SECONDS + 1).await;
    h.track(42, LiveReplayTrackingAction::Watch).await;

    for _ in 0..200 {
        if !h.watched.lock().unwrap().is_empty() {
            assert_eq!(*h.watched.lock().unwrap(), vec![42]);
            assert!(h.app.snapshot().replays.live_tracking.is_none());
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the tracked replay never opened");
}

#[tokio::test]
async fn cancelling_a_delayed_action_removes_it_without_opening_the_stream() {
    let h = Harness::start().await;
    h.live_game_started(42, 10).await;
    h.track(42, LiveReplayTrackingAction::Watch).await;

    for _ in 0..200 {
        if h.app.snapshot().replays.live_tracking.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(h.app.snapshot().replays.live_tracking.is_some());

    h.app
        .dispatch(ReplayCommand::CancelLiveTracking.into())
        .await
        .unwrap();
    for _ in 0..200 {
        if h.app.snapshot().replays.live_tracking.is_none() {
            assert!(h.watched.lock().unwrap().is_empty());
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the delayed replay action was not cancelled");
}
