//! Sockets that drop and come back, and writes that fail and are tried again.
//!
//! A drop is not a disconnect: nobody asked for it, so the reconnect watchdog
//! brings the socket back on its own. What must not come back with it is work
//! that belonged to the old connection. A join still preparing when the lobby
//! dropped, or a search still being prepared, was for a connection the server
//! has forgotten; sending it on the new one would put the player in a game or
//! a queue they no longer see on screen. A join the user called off must not
//! be sent again either. Chat is the other way round: the channels the player
//! was in are what they expect to find again.
//!
//! The last test is the settings write that fails: the next write is the
//! whole document, so it has to carry the change the failed one lost.
//!
//! Every drop here is the adapter's (`disconnect` on the fake, from the
//! test's side), never a `Disconnect` command, which disarms the watchdog.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use faf_app::infra::{fake_ports, FakeAuth, FakeChat, FakeLobby};
use faf_app::ports::{
    ChatPort, ChatUpdate, GameLaunchParams, GamePreparation, GameUpdaterPort, InstallPresence,
    LobbyPort, LobbyUpdate, PreparationPhase, PreparationStep, ProcessPort, SettingsPort,
    UpdateProgress,
};
use faf_app::{App, Ports};
use faf_domain::state::settings::{BrowsingPreferencesPatch, GamePreferencesPatch};
use faf_domain::state::{
    AuthCommand, ChatCommand, ChatEvent, ChatStatus, GamePreferences, HostGameConfig, JoinState,
    LobbyCommand, LobbyEvent, MatchmakingState, Player, PlayerVeto, Relation, SettingsCommand,
    SettingsState,
};
use faf_domain::{AppCommand, AppEvent, AppState};
use tokio::sync::{broadcast, mpsc, Notify};

/// How long anything ordinary may take before the test calls it stuck.
const PATIENCE: Duration = Duration::from_secs(5);

/// How long to give the reconnect watchdog: two of its five-second ticks.
const WATCHDOG_WAIT: Duration = Duration::from_secs(11);

/// How long something that must not happen is given to happen anyway. A
/// negative cannot be awaited; this is the only place a window stands in for
/// a gate.
const WINDOW: Duration = Duration::from_millis(150);

/// Claims live launch so preparation really runs, without an FA install.
struct LaunchableProcess;

#[async_trait]
impl ProcessPort for LaunchableProcess {
    fn supports_live_launch(&self) -> bool {
        true
    }
    async fn launch_game(&self, _params: GameLaunchParams) -> Result<(), String> {
        Ok(())
    }
    async fn launch_offline(&self, _featured_mod: String, _map: String) -> Result<(), String> {
        Ok(())
    }
    async fn launch_replay(&self, _args: Vec<String>) -> Result<(), String> {
        Ok(())
    }
    fn kill(&self) {}
    async fn wait_for_exit(&self) {
        std::future::pending::<()>().await
    }
    fn set_paths(&self, _game_path: String, _replay_game_path: String) {}
    fn set_additional_arguments(&self, _arguments: Vec<String>) {}
    fn game_install_dir(&self) -> Option<PathBuf> {
        Some(PathBuf::from("C:/faf"))
    }
    fn replay_install_dir(&self) -> Option<PathBuf> {
        None
    }
    fn installs_present(&self) -> InstallPresence {
        InstallPresence::default()
    }
}

/// Holds the preparations named in `held` (by map folder, the empty name for
/// the featured mod alone) at a gate the test opens; every other preparation
/// finishes at once. A gate opened before anybody waits stays open for one.
#[derive(Clone, Default)]
struct GatedUpdater {
    held: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
}

impl GatedUpdater {
    fn hold(self, map: &str) -> Self {
        self.held
            .lock()
            .unwrap()
            .insert(map.to_string(), Arc::new(Notify::new()));
        self
    }

    fn open(&self, map: &str) {
        self.held.lock().unwrap()[map].notify_one();
    }
}

#[async_trait]
impl GameUpdaterPort for GatedUpdater {
    async fn prepare(&self, request: GamePreparation) -> mpsc::Receiver<UpdateProgress> {
        let map = request.map_folder.unwrap_or_default();
        let gate = self.held.lock().unwrap().get(&map).cloned();
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let step = UpdateProgress::Step(PreparationStep::indeterminate(
                PreparationPhase::Downloading,
                format!("preparing {map}"),
            ));
            if tx.send(step).await.is_err() {
                return;
            }
            if let Some(gate) = gate {
                gate.notified().await;
            }
            let _ = tx.send(UpdateProgress::Finished(Ok(()))).await;
        });
        rx
    }
}

/// Every lobby call goes to the offline fake; searches asked of the server
/// are recorded.
struct RecordingLobby {
    inner: FakeLobby,
    searches: Arc<Mutex<Vec<(String, bool)>>>,
}

#[async_trait]
impl LobbyPort for RecordingLobby {
    async fn connect(&self) -> mpsc::Receiver<LobbyUpdate> {
        self.inner.connect().await
    }
    fn join(&self, id: i32, password: Option<String>) -> bool {
        self.inner.join(id, password)
    }
    fn host(&self, config: HostGameConfig) {
        self.inner.host(config)
    }
    fn matchmake(&self, queue_name: String, start: bool) {
        self.searches
            .lock()
            .unwrap()
            .push((queue_name.clone(), start));
        self.inner.matchmake(queue_name, start)
    }
    fn leave_party(&self) {
        self.inner.leave_party()
    }
    fn kick_party_member(&self, player_id: i32) {
        self.inner.kick_party_member(player_id)
    }
    fn invite_to_party(&self, player_id: i32) {
        self.inner.invite_to_party(player_id)
    }
    fn accept_party_invite(&self, player_id: i32) {
        self.inner.accept_party_invite(player_id)
    }
    fn set_party_factions(&self, factions: Vec<String>) {
        self.inner.set_party_factions(factions)
    }
    fn set_contact_relation(
        &self,
        contact: faf_domain::state::Contact,
        member: bool,
        remove: Option<Relation>,
    ) -> bool {
        self.inner.set_contact_relation(contact, member, remove)
    }
    fn set_player_vetoes(&self, vetoes: Vec<PlayerVeto>) {
        self.inner.set_player_vetoes(vetoes)
    }
    fn request_avatars(&self) -> bool {
        self.inner.request_avatars()
    }
    fn select_avatar(&self, url: Option<String>) -> bool {
        self.inner.select_avatar(url)
    }
    fn restore_game_session(&self, game_id: i32) -> bool {
        self.inner.restore_game_session(game_id)
    }
    fn send_game_relay(&self, command: String, args: Vec<serde_json::Value>) {
        self.inner.send_game_relay(command, args)
    }
    fn disconnect(&self) {
        self.inner.disconnect()
    }
}

fn signed_in_auth() -> FakeAuth {
    FakeAuth {
        player: Player::new(7, "Ada"),
        delay: Duration::ZERO,
        fail_with: None,
    }
}

fn start(ports: Ports) -> Arc<App> {
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());
    Arc::new(app)
}

/// Every lobby event seen so far, so assertions can look back over all of it.
struct Log {
    events: broadcast::Receiver<AppEvent>,
    seen: Vec<LobbyEvent>,
}

impl Log {
    fn new(app: &App) -> Self {
        Self {
            events: app.subscribe(),
            seen: Vec::new(),
        }
    }

    /// Record events until one matches, or fail after `patience`.
    async fn until_within(
        &mut self,
        patience: Duration,
        what: &str,
        matches: impl Fn(&LobbyEvent) -> bool,
    ) {
        let events = &mut self.events;
        let seen = &mut self.seen;
        tokio::time::timeout(patience, async {
            loop {
                match events.recv().await {
                    Ok(AppEvent::Lobby(event)) => {
                        let found = matches(&event);
                        seen.push(event);
                        if found {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => panic!("the app stopped"),
                    _ => {}
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("never saw {what}"));
    }

    async fn until(&mut self, what: &str, matches: impl Fn(&LobbyEvent) -> bool) {
        self.until_within(PATIENCE, what, matches).await
    }

    fn drain(&mut self) {
        while let Ok(event) = self.events.try_recv() {
            if let AppEvent::Lobby(event) = event {
                self.seen.push(event);
            }
        }
    }
}

fn join(id: i32) -> AppCommand {
    LobbyCommand::Join {
        id,
        password: None,
        replace_mods: false,
    }
    .into()
}

async fn connected(app: &App, log: &mut Log) {
    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    log.until("the lobby connecting", |event| {
        matches!(event, LobbyEvent::GamesUpdated { .. })
    })
    .await;
}

/// The seeded game 1 is on this map.
const FIRST_MAP: &str = "Theta Passage";

/// A join is still downloading its map when the lobby socket drops, and the
/// watchdog brings the socket back before the download ends. The join was for
/// the connection that is gone: it must not narrate progress or reach the new
/// connection, and its slot must be free for the next join.
#[tokio::test]
async fn a_join_preparing_when_the_lobby_drops_is_not_sent_on_the_new_connection() {
    let updater = GatedUpdater::default().hold(FIRST_MAP);
    let lobby = FakeLobby::default();
    lobby.hold_join_answers();
    let app = start(Ports {
        auth: Arc::new(signed_in_auth()),
        lobby: Arc::new(lobby.clone()),
        process: Arc::new(LaunchableProcess),
        updater: Arc::new(updater.clone()),
        ..fake_ports()
    });
    let mut log = Log::new(&app);
    app.dispatch_and_wait(AuthCommand::Login { remember: false }.into())
        .await
        .unwrap();
    connected(&app, &mut log).await;

    let joining = tokio::spawn({
        let app = app.clone();
        async move { app.dispatch_and_wait(join(1)).await }
    });
    log.until(
        "the join preparing",
        |event| matches!(event, LobbyEvent::Preparing { detail, .. } if detail.contains(FIRST_MAP)),
    )
    .await;

    lobby.disconnect();
    log.until("the drop", |event| {
        matches!(event, LobbyEvent::Disconnected)
    })
    .await;
    let after_drop = log.seen.len();
    log.until_within(WATCHDOG_WAIT, "the watchdog bringing it back", |event| {
        matches!(event, LobbyEvent::Connected)
    })
    .await;

    // Only now does the download finish.
    updater.open(FIRST_MAP);
    tokio::time::timeout(PATIENCE, joining)
        .await
        .expect("the join's handler never finished")
        .unwrap()
        .unwrap();
    tokio::time::sleep(WINDOW).await;
    log.drain();

    assert!(
        lobby.held_joins().is_empty(),
        "the join was sent on the new connection"
    );
    let after: Vec<_> = log.seen[after_drop..].to_vec();
    assert!(
        !after.iter().any(|event| matches!(
            event,
            LobbyEvent::Preparing { .. } | LobbyEvent::Joining { .. }
        )),
        "the join from the old connection came back: {after:?}"
    );
    assert!(matches!(app.snapshot().lobby.join, JoinState::Idle));

    // Its slot went with the connection: the next join goes out.
    app.dispatch_and_wait(join(2)).await.unwrap();
    assert_eq!(lobby.held_joins(), vec![2]);
}

/// The join's request went out, the user called it off, and then the lobby
/// dropped and came back. Nothing about the reconnect may send it again.
#[tokio::test]
async fn a_reconnect_does_not_resend_a_called_off_join() {
    let lobby = FakeLobby::default();
    lobby.hold_join_answers();
    let app = start(Ports {
        auth: Arc::new(signed_in_auth()),
        lobby: Arc::new(lobby.clone()),
        process: Arc::new(LaunchableProcess),
        updater: Arc::new(GatedUpdater::default()),
        ..fake_ports()
    });
    let mut log = Log::new(&app);
    app.dispatch_and_wait(AuthCommand::Login { remember: false }.into())
        .await
        .unwrap();
    connected(&app, &mut log).await;

    app.dispatch_and_wait(join(1)).await.unwrap();
    assert_eq!(lobby.held_joins(), vec![1], "the request went out");
    app.dispatch_and_wait(LobbyCommand::CancelJoin.into())
        .await
        .unwrap();

    lobby.disconnect();
    log.until("the drop", |event| {
        matches!(event, LobbyEvent::Disconnected)
    })
    .await;
    // The user's own reconnect; the watchdog's is the same path.
    connected(&app, &mut log).await;
    tokio::time::sleep(WINDOW).await;

    assert_eq!(
        lobby.held_joins(),
        vec![1],
        "the called-off join was sent again"
    );
    assert!(matches!(app.snapshot().lobby.join, JoinState::Idle));
}

/// A search is still being prepared (the featured mod held at its gate) when
/// the lobby drops. The server forgot the player's queue state with the
/// socket, so the preparation finishing on the new connection must not ask
/// for the search. A search that was running when the socket dropped is not
/// shown as running afterwards, and is not asked for again either.
#[tokio::test]
async fn a_search_from_before_a_drop_is_not_asked_for_on_the_new_connection() {
    let updater = GatedUpdater::default().hold("");
    let lobby = FakeLobby::default();
    let searches = Arc::new(Mutex::new(Vec::new()));
    let app = start(Ports {
        lobby: Arc::new(RecordingLobby {
            inner: lobby.clone(),
            searches: searches.clone(),
        }),
        process: Arc::new(LaunchableProcess),
        updater: Arc::new(updater.clone()),
        ..fake_ports()
    });
    let mut log = Log::new(&app);
    connected(&app, &mut log).await;
    let start_search = || -> AppCommand {
        LobbyCommand::StartSearch {
            queue_names: vec!["ladder1v1".into()],
        }
        .into()
    };

    // Preparing when the socket drops.
    let preparing = tokio::spawn({
        let app = app.clone();
        let command = start_search();
        async move { app.dispatch_and_wait(command).await }
    });
    log.until("the search preparing", |event| {
        matches!(
            event,
            LobbyEvent::MatchmakingUpdated {
                state: MatchmakingState::Preparing { .. }
            }
        )
    })
    .await;
    lobby.disconnect();
    log.until("the drop", |event| {
        matches!(event, LobbyEvent::Disconnected)
    })
    .await;
    connected(&app, &mut log).await;
    updater.open("");
    tokio::time::timeout(PATIENCE, preparing)
        .await
        .expect("the search's handler never finished")
        .unwrap()
        .unwrap();
    tokio::time::sleep(WINDOW).await;
    assert!(
        searches.lock().unwrap().is_empty(),
        "the search prepared for the old connection was asked for on the new one"
    );
    assert_eq!(app.snapshot().lobby.matchmaking, MatchmakingState::Idle);

    // Searching when the socket drops.
    updater.open("");
    app.dispatch(start_search()).await.unwrap();
    log.until("the search running", |event| {
        matches!(
            event,
            LobbyEvent::MatchmakingUpdated {
                state: MatchmakingState::Searching { .. }
            }
        )
    })
    .await;
    assert_eq!(searches.lock().unwrap().len(), 1);
    lobby.disconnect();
    log.until("the drop", |event| {
        matches!(event, LobbyEvent::Disconnected)
    })
    .await;
    connected(&app, &mut log).await;
    tokio::time::sleep(WINDOW).await;
    assert_eq!(
        app.snapshot().lobby.matchmaking,
        MatchmakingState::Idle,
        "a search the server forgot is still shown as running"
    );
    assert_eq!(
        searches.lock().unwrap().len(),
        1,
        "the search was asked for again after the reconnect"
    );
}

/// Chat goes to the offline fake, shared so the test can drop it from the
/// adapter's side; connections and channel joins are recorded.
struct RecordingChat {
    inner: Arc<FakeChat>,
    connects: Arc<AtomicUsize>,
    joins: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl ChatPort for RecordingChat {
    async fn connect(&self, username: String) -> mpsc::Receiver<ChatUpdate> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        self.inner.connect(username).await
    }
    fn send_message(&self, channel: String, content: String, reply_to: String) {
        self.inner.send_message(channel, content, reply_to)
    }
    fn send_action(&self, channel: String, content: String) {
        self.inner.send_action(channel, content)
    }
    fn join_channel(&self, channel: String) {
        self.joins.lock().unwrap().push(channel.clone());
        self.inner.join_channel(channel)
    }
    fn leave_channel(&self, channel: String, reason: String) {
        self.inner.leave_channel(channel, reason)
    }
    fn set_topic(&self, channel: String, topic: String) {
        self.inner.set_topic(channel, topic)
    }
    fn disconnect(&self) {
        self.inner.disconnect()
    }
}

async fn until(app: &App, patience: Duration, what: &str, done: impl Fn(&AppState) -> bool) {
    tokio::time::timeout(patience, async {
        while !done(&app.snapshot()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("never saw {what}"));
}

/// Chat drops while the player is signed in, with the server's channels
/// joined. The watchdog brings it back with one new connection, every one of
/// those channels is joined again, and what was on screen in them (the
/// channel and its scrollback) is still there.
#[tokio::test]
async fn chat_comes_back_after_a_drop_with_its_channels() {
    let chat = Arc::new(FakeChat::default());
    let connects = Arc::new(AtomicUsize::new(0));
    let joins = Arc::new(Mutex::new(Vec::new()));
    let app = start(Ports {
        auth: Arc::new(signed_in_auth()),
        chat: Arc::new(RecordingChat {
            inner: chat.clone(),
            connects: connects.clone(),
            joins: joins.clone(),
        }),
        ..fake_ports()
    });
    let mut events = app.subscribe();
    app.dispatch_and_wait(AuthCommand::Login { remember: false }.into())
        .await
        .unwrap();
    // The lobby announces the channels this account is in.
    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    until(&app, PATIENCE, "the lobby's channel list", |state| {
        !state.chat.server_auto_join.is_empty()
    })
    .await;
    app.dispatch(
        ChatCommand::Connect {
            username: "Ada".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    let wanted: Vec<String> = {
        let state = app.snapshot();
        state.chat.server_auto_join.clone()
    };
    until(&app, PATIENCE, "the channels joined", |state| {
        state.chat.status == ChatStatus::Connected
            && wanted.iter().all(|channel| {
                joins
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|joined| joined.ends_with(channel.trim_start_matches('#')))
            })
    })
    .await;
    let joined_before: Vec<String> = joins.lock().unwrap().clone();
    let before = app.snapshot().chat;
    let scrollback: HashMap<String, usize> = joined_before
        .iter()
        .chain(std::iter::once(&"#aeolus".to_string()))
        .filter_map(|name| {
            before
                .channel(name)
                .map(|c| (name.clone(), c.messages.len()))
        })
        .collect();
    assert!(!scrollback.is_empty(), "nothing to keep: {joined_before:?}");

    // The adapter gives up: a status, then the end of the stream.
    joins.lock().unwrap().clear();
    chat.disconnect();
    tokio::time::timeout(PATIENCE, async {
        let mut seen = 0;
        while seen < 2 {
            if let AppEvent::Chat(ChatEvent::Disconnected) = events.recv().await.unwrap() {
                seen += 1;
            }
        }
    })
    .await
    .expect("the drop never ended the connection");

    until(
        &app,
        WATCHDOG_WAIT,
        "the watchdog bringing chat back",
        |state| state.chat.status == ChatStatus::Connected,
    )
    .await;
    until(&app, PATIENCE, "the channels joined again", |_| {
        let rejoined = joins.lock().unwrap();
        joined_before
            .iter()
            .all(|channel| rejoined.contains(channel))
    })
    .await;
    assert_eq!(
        connects.load(Ordering::SeqCst),
        2,
        "the reconnect opened more than one socket"
    );
    let after = app.snapshot().chat;
    for (name, lines) in &scrollback {
        let channel = after
            .channel(name)
            .unwrap_or_else(|| panic!("{name} was lost in the reconnect"));
        assert!(
            channel.messages.len() >= *lines,
            "{name} lost its scrollback in the reconnect"
        );
    }
}

/// Settings whose first write after arming fails, as a full disk or a locked
/// file would, and which records every write that succeeded.
struct FailingOnceSettings {
    armed: Arc<AtomicBool>,
    failed: AtomicBool,
    saved: Arc<Mutex<Vec<SettingsState>>>,
}

#[async_trait]
impl SettingsPort for FailingOnceSettings {
    async fn load(&self) -> Option<SettingsState> {
        // No cache lifetime, so loading cannot sweep the real game-files
        // cache of whoever runs the suite.
        Some(SettingsState {
            game: GamePreferences {
                cache_lifetime_days: None,
                ..GamePreferences::default()
            },
            ..SettingsState::default()
        })
    }

    async fn save(&self, settings: &SettingsState) -> Result<(), String> {
        if self.armed.load(Ordering::SeqCst) && !self.failed.swap(true, Ordering::SeqCst) {
            return Err("the settings file is locked".into());
        }
        self.saved.lock().unwrap().push(settings.clone());
        Ok(())
    }
}

/// A change whose write failed is still in state, and the next write is the
/// whole document: it carries both the change it was made for and the one
/// whose write failed.
#[tokio::test]
async fn a_failed_settings_write_is_carried_by_the_next_one() {
    let armed = Arc::new(AtomicBool::new(false));
    let saved = Arc::new(Mutex::new(Vec::new()));
    let app = start(Ports {
        settings: Arc::new(FailingOnceSettings {
            armed: armed.clone(),
            failed: AtomicBool::new(false),
            saved: saved.clone(),
        }),
        ..fake_ports()
    });
    app.dispatch_and_wait(SettingsCommand::Load.into())
        .await
        .unwrap();
    saved.lock().unwrap().clear();
    armed.store(true, Ordering::SeqCst);

    app.dispatch_and_wait(
        SettingsCommand::PatchGame {
            patch: Box::new(GamePreferencesPatch {
                steam_presence: Some(false),
                ..GamePreferencesPatch::default()
            }),
        }
        .into(),
    )
    .await
    .unwrap();
    assert!(saved.lock().unwrap().is_empty(), "the first write failed");
    assert!(
        !app.snapshot().settings.game.steam_presence,
        "the change whose write failed left state"
    );

    app.dispatch_and_wait(
        SettingsCommand::PatchBrowsing {
            patch: Box::new(BrowsingPreferencesPatch {
                vault_page_size: Some(48),
                ..BrowsingPreferencesPatch::default()
            }),
        }
        .into(),
    )
    .await
    .unwrap();
    let saved = saved.lock().unwrap().clone();
    assert_eq!(saved.len(), 1);
    assert!(
        !saved[0].game.steam_presence,
        "the next write did not carry the change whose write failed"
    );
    assert_eq!(saved[0].browsing.vault_page_size, 48);
}
