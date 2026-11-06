//! Lobby service test: drives the streaming port end-to-end.
//!
//! Proves the push path: Connect → Connecting → Connected → (at least one)
//! GamesUpdated with the seeded games, and that state reflects the snapshot.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use faf_app::infra::{fake_ports, FakeLobby};
use faf_app::ports::{
    GameLaunchParams, InstallPresence, LobbyUpdate, ProcessPort, ServerNoticeStyle, SettingsPort,
};
use faf_app::{App, Ports};
use faf_domain::state::{
    GameLaunch, HostGameConfig, JoinState, LobbyCommand, LobbyEvent, LobbyStatus, MatchmakingState,
    NotificationEvent, NotificationKind, PlayerProfile, SettingsState, SocialEvent,
};
use faf_domain::AppEvent;

#[tokio::test]
async fn a_refused_contact_change_leaves_the_list_alone_and_reports_failure() {
    use faf_app::infra::{LobbyClient, TokenStore};
    use faf_domain::state::{Relation, SocialCommand};

    let (app, app_loop) = App::new(
        "test",
        Ports {
            lobby: Arc::new(LobbyClient::faf(TokenStore::new())),
            ..fake_ports()
        },
    );
    let task = tokio::spawn(app_loop.run());
    app.dispatch_and_wait(
        SocialCommand::SetRelation {
            player_id: 23,
            login: "OfflineFixture".into(),
            relation: Relation::Foe,
            member: true,
        }
        .into(),
    )
    .await
    .unwrap();

    let state = app.snapshot();
    assert!(state.social.foes.is_empty());
    assert!(state.social.contacts.entries.is_empty());
    assert!(state.notifications.items.iter().any(|item| {
        item.text
            .as_ref()
            .is_some_and(|text| text.key == "notifications.msg.contactChangeFailed")
    }));
    task.abort();
}

#[derive(Default)]
struct RecordingSettings {
    saved: Arc<Mutex<Vec<SettingsState>>>,
}

#[derive(Default)]
struct RecordingProcess {
    kills: Arc<AtomicUsize>,
}

#[async_trait]
impl ProcessPort for RecordingProcess {
    fn supports_live_launch(&self) -> bool {
        false
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

    fn kill(&self) {
        self.kills.fetch_add(1, Ordering::SeqCst);
    }

    fn set_paths(&self, _game_path: String, _replay_game_path: String) {}

    fn set_additional_arguments(&self, _arguments: Vec<String>) {}

    fn game_install_dir(&self) -> Option<PathBuf> {
        None
    }

    fn replay_install_dir(&self) -> Option<PathBuf> {
        None
    }

    fn installs_present(&self) -> InstallPresence {
        InstallPresence::default()
    }
}

async fn wait_for_initial_games(events: &mut tokio::sync::broadcast::Receiver<AppEvent>) {
    loop {
        if matches!(
            events.recv().await.unwrap(),
            AppEvent::Lobby(LobbyEvent::GamesUpdated { .. })
        ) {
            return;
        }
    }
}

#[async_trait]
impl SettingsPort for RecordingSettings {
    async fn load(&self) -> Option<SettingsState> {
        Some(SettingsState {
            // Off, so the startup cache pass these tests have to run cannot
            // sweep the real game-files cache on whoever runs the suite. The
            // measuring half only reads. See `cache_dir` in `infra`, which is
            // not injectable.
            game: faf_domain::state::GamePreferences {
                cache_lifetime_days: None,
                ..faf_domain::state::GamePreferences::default()
            },
            ..SettingsState::default()
        })
    }

    async fn save(&self, settings: &SettingsState) -> Result<(), String> {
        self.saved.lock().unwrap().push(settings.clone());
        Ok(())
    }
}

fn host_config() -> HostGameConfig {
    HostGameConfig {
        title: "  Friday game  ".into(),
        mod_name: " faf ".into(),
        visibility: "PUBLIC".into(),
        map: " scmp_009 ".into(),
        password: Some(" secret ".into()),
        enforce_rating_range: true,
        rating_min: Some(800),
        rating_max: Some(1_500),
    }
}

#[tokio::test]
async fn valid_host_requests_are_normalized_sent_and_remembered() {
    let lobby = FakeLobby::default();
    let saved = Arc::new(Mutex::new(Vec::new()));
    let ports = Ports {
        lobby: Arc::new(lobby.clone()),
        settings: Arc::new(RecordingSettings {
            saved: saved.clone(),
        }),
        ..fake_ports()
    };
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());

    // The settings file has to have been read before anything may be written
    // back over it, so the startup load comes first here as it does in the
    // client. See `crates/faf-app/tests/settings_startup.rs`.
    app.dispatch_and_wait(faf_domain::state::SettingsCommand::Load.into())
        .await
        .unwrap();
    saved.lock().unwrap().clear();

    app.dispatch(
        LobbyCommand::Host {
            config: host_config(),
        }
        .into(),
    )
    .await
    .unwrap();

    for _ in 0..100 {
        if !saved.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let hosted = lobby.hosted_configs();
    assert_eq!(hosted.len(), 1);
    assert_eq!(hosted[0].title, "Friday game");
    assert_eq!(hosted[0].password.as_deref(), Some(" secret "));
    let snapshot = app.snapshot();
    let remembered = &snapshot.settings.browsing.host_game;
    assert_eq!(remembered.title, "Friday game");
    assert_eq!(remembered.featured_mod, "faf");
    assert_eq!(remembered.map, "scmp_009");
    assert_eq!(
        saved.lock().unwrap().last().unwrap().browsing.host_game,
        *remembered
    );
}

#[tokio::test]
async fn a_co_op_host_is_remembered_without_touching_the_custom_game_form() {
    // The two dialogs are separate surfaces, and mixing them is why the co-op
    // one used to remember nothing at all: rather than overwrite the skirmish
    // form with a mission's lobby name, it saved nowhere - so leaving the tab,
    // which unmounts the whole play view, threw the setup away.
    let lobby = FakeLobby::default();
    let saved = Arc::new(Mutex::new(Vec::new()));
    let ports = Ports {
        lobby: Arc::new(lobby.clone()),
        settings: Arc::new(RecordingSettings {
            saved: saved.clone(),
        }),
        ..fake_ports()
    };
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());

    let mut config = host_config();
    config.mod_name = "coop".into();
    config.map = "SCCA_Coop_A03.v0023".into();
    config.title = "Operation Ivy".into();
    app.dispatch(LobbyCommand::Host { config }.into())
        .await
        .unwrap();

    for _ in 0..100 {
        if !saved.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(lobby.hosted_configs().len(), 1);
    let browsing = app.snapshot().settings.browsing;
    assert_eq!(browsing.host_coop.title, "Operation Ivy");
    assert_eq!(browsing.host_coop.map, "SCCA_Coop_A03.v0023");
    assert_eq!(browsing.host_coop.featured_mod, "coop");
    // Untouched: the skirmish form is somebody else's setup.
    assert_eq!(browsing.host_game, Default::default());
}

#[tokio::test]
async fn invalid_host_requests_never_reach_the_lobby_port() {
    let lobby = FakeLobby::default();
    let ports = Ports {
        lobby: Arc::new(lobby.clone()),
        ..fake_ports()
    };
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();
    let mut config = host_config();
    config.rating_min = Some(1_501);

    app.dispatch(LobbyCommand::Host { config }.into())
        .await
        .unwrap();

    loop {
        if let AppEvent::Notifications(NotificationEvent::Added { notification }) =
            events.recv().await.unwrap()
        {
            assert_eq!(notification.kind, NotificationKind::Error);
            assert!(notification.body.contains("Minimum rating"));
            break;
        }
    }
    assert!(lobby.hosted_configs().is_empty());
}

#[tokio::test]
async fn connect_streams_game_snapshots() {
    let (app, app_loop) = App::new("test", fake_ports());
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();

    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();

    assert!(matches!(
        events.recv().await.unwrap(),
        AppEvent::Lobby(faf_domain::state::LobbyEvent::Connecting)
    ));
    assert!(matches!(
        events.recv().await.unwrap(),
        AppEvent::Lobby(faf_domain::state::LobbyEvent::Connected)
    ));
    // Login asks for the avatar list, which the avatar fallback needs.
    assert!(matches!(
        events.recv().await.unwrap(),
        AppEvent::Lobby(faf_domain::state::LobbyEvent::AvatarsLoading)
    ));

    // The fake sends an immediate first snapshot: no waiting on the tick.
    match events.recv().await.unwrap() {
        AppEvent::Lobby(faf_domain::state::LobbyEvent::GamesUpdated { games }) => {
            assert!(!games.is_empty(), "first snapshot should have games");
        }
        other => panic!("expected GamesUpdated, got {other:?}"),
    }

    let snap = app.snapshot();
    assert_eq!(snap.lobby.status, LobbyStatus::Connected);
    assert!(!snap.lobby.games.is_empty());
}

#[tokio::test]
async fn server_kill_notice_is_retained_and_terminates_the_game() {
    let lobby = FakeLobby::default();
    let kills = Arc::new(AtomicUsize::new(0));
    let ports = Ports {
        lobby: Arc::new(lobby.clone()),
        process: Arc::new(RecordingProcess {
            kills: kills.clone(),
        }),
        ..fake_ports()
    };
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();
    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    wait_for_initial_games(&mut events).await;

    assert!(lobby.push_update(LobbyUpdate::Notice {
        style: ServerNoticeStyle::Kill,
        text: "The match was closed by an administrator.".into(),
    }));

    let mut saw_notice = false;
    let mut saw_termination = false;
    while !saw_notice || !saw_termination {
        match events.recv().await.unwrap() {
            AppEvent::Notifications(NotificationEvent::Added { notification }) => {
                if notification.title == "Game stopped by server" {
                    assert_eq!(notification.kind, NotificationKind::Error);
                    assert!(notification.body.contains("administrator"));
                    saw_notice = true;
                }
            }
            AppEvent::Lobby(LobbyEvent::GameTerminated) => saw_termination = true,
            _ => {}
        }
    }
    assert_eq!(kills.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_ban_is_said_once_and_leaves_an_offline_session() {
    // #488: the server refuses a banned account at every sign-in, and the
    // client used to reconnect into the refusal and notify each time.
    let lobby = FakeLobby::default();
    let ports = Ports {
        lobby: Arc::new(lobby.clone()),
        ..fake_ports()
    };
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();
    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    wait_for_initial_games(&mut events).await;

    assert!(lobby.push_update(LobbyUpdate::Banned {
        text: "You are banned from FAF forever.
Reason:
Asked for it"
            .into(),
    }));

    let mut saw_notice = false;
    let mut saw_ban = false;
    while !saw_notice || !saw_ban {
        match events.recv().await.unwrap() {
            AppEvent::Notifications(NotificationEvent::Added { notification }) => {
                if notification.title == "Banned from FAF" {
                    assert_eq!(notification.kind, NotificationKind::Error);
                    assert!(notification.body.contains("Asked for it"));
                    saw_notice = true;
                }
            }
            AppEvent::Auth(faf_domain::state::AuthEvent::Banned { message }) => {
                assert!(message.starts_with("You are banned from FAF"));
                saw_ban = true;
            }
            _ => {}
        }
    }

    let snap = app.snapshot();
    assert_eq!(snap.auth.mode, faf_domain::state::AuthMode::Offline);
    assert_eq!(snap.auth.player, None);
}

#[tokio::test]
async fn connection_rejection_keeps_the_authoritative_reason() {
    let lobby = FakeLobby::default();
    let ports = Ports {
        lobby: Arc::new(lobby.clone()),
        ..fake_ports()
    };
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();
    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    wait_for_initial_games(&mut events).await;

    assert!(lobby.push_update(LobbyUpdate::ConnectionRejected {
        reason: "This client version is no longer supported.".into(),
    }));
    loop {
        if let AppEvent::Notifications(NotificationEvent::Added { notification }) =
            events.recv().await.unwrap()
        {
            if notification.title == "Lobby connection rejected" {
                assert!(notification.body.contains("no longer supported"));
                break;
            }
        }
    }
}

#[tokio::test]
async fn cancellation_after_matchmaker_launch_stops_the_spawned_game() {
    let lobby = FakeLobby::default();
    let kills = Arc::new(AtomicUsize::new(0));
    let ports = Ports {
        lobby: Arc::new(lobby.clone()),
        process: Arc::new(RecordingProcess {
            kills: kills.clone(),
        }),
        ..fake_ports()
    };
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();
    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    wait_for_initial_games(&mut events).await;

    assert!(
        lobby.push_update(LobbyUpdate::Matchmaking(MatchmakingState::Launching {
            queue_name: "ladder1v1".into(),
        },))
    );
    assert!(lobby.push_update(LobbyUpdate::Launch(GameLaunch {
        uid: 77,
        mod_name: "faf".into(),
        name: "Ranked match".into(),
        mapname: "scmp_007".into(),
        game_type: "matchmaker".into(),
        rating_type: "ladder_1v1".into(),
        expected_players: Some(2),
        team: Some(1),
        faction: Some(1),
        map_position: Some(1),
        game_options: Default::default(),
        args: Vec::new(),
    })));

    loop {
        if matches!(
            events.recv().await.unwrap(),
            AppEvent::Lobby(LobbyEvent::Launching { .. })
        ) {
            break;
        }
    }
    assert!(
        lobby.push_update(LobbyUpdate::Matchmaking(MatchmakingState::Cancelled {
            queue_name: Some("ladder1v1".into()),
        },))
    );

    loop {
        if matches!(
            events.recv().await.unwrap(),
            AppEvent::Lobby(LobbyEvent::GameTerminated)
        ) {
            break;
        }
    }
    assert_eq!(kills.load(Ordering::SeqCst), 1);
    assert_eq!(app.snapshot().lobby.join, JoinState::Idle);
}

#[tokio::test]
async fn an_authoritative_friend_offline_update_notifies_once_then_removes_the_profile() {
    let lobby = FakeLobby::default();
    let ports = Ports {
        lobby: Arc::new(lobby.clone()),
        ..fake_ports()
    };
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();

    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    loop {
        if matches!(
            events.recv().await.unwrap(),
            AppEvent::Social(SocialEvent::RelationsUpdated { .. })
        ) {
            break;
        }
    }
    assert!(app.snapshot().social.is_friend("Stormlord"));
    assert!(app.snapshot().social.player("Stormlord").is_some());

    assert!(
        lobby.push_update(LobbyUpdate::PlayersRemoved(vec![PlayerProfile {
            id: 2,
            login: "Stormlord".into(),
            ..Default::default()
        }]))
    );

    loop {
        if let AppEvent::Notifications(NotificationEvent::Added { notification }) =
            events.recv().await.unwrap()
        {
            assert_eq!(notification.kind, NotificationKind::FriendOffline);
            assert!(notification.body.contains("Stormlord"));
            break;
        }
    }
    loop {
        if matches!(
            events.recv().await.unwrap(),
            AppEvent::Social(SocialEvent::PlayersRemoved { .. })
        ) {
            break;
        }
    }
    let snapshot = app.snapshot();
    assert!(snapshot.social.player("Stormlord").is_none());
    assert!(snapshot.social.is_friend("Stormlord"));
}

#[tokio::test]
async fn terminate_game_command_resets_the_join_lifecycle() {
    let (app, app_loop) = App::new("test", fake_ports());
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();

    app.dispatch(LobbyCommand::TerminateGame.into())
        .await
        .unwrap();

    loop {
        if matches!(
            events.recv().await.unwrap(),
            AppEvent::Lobby(LobbyEvent::GameTerminated)
        ) {
            break;
        }
    }
    assert_eq!(app.snapshot().lobby.join, JoinState::Idle);
}

#[tokio::test]
async fn disconnect_ends_the_stream() {
    let (app, app_loop) = App::new("test", fake_ports());
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();

    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();

    // Drain until we've seen at least one snapshot (connection is live).
    loop {
        match events.recv().await.unwrap() {
            AppEvent::Lobby(faf_domain::state::LobbyEvent::GamesUpdated { .. }) => break,
            _ => continue,
        }
    }

    // Cancelling the connection must drive the slice back to Disconnected.
    app.dispatch(LobbyCommand::Disconnect.into()).await.unwrap();
    loop {
        if let AppEvent::Lobby(faf_domain::state::LobbyEvent::Disconnected) =
            events.recv().await.unwrap()
        {
            break;
        }
    }

    let snap = app.snapshot();
    assert_eq!(snap.lobby.status, LobbyStatus::Disconnected);
    assert!(snap.lobby.games.is_empty(), "games cleared on disconnect");
}

#[tokio::test]
async fn duplicate_connect_is_dropped_single_flight() {
    // Two near-simultaneous Connects (e.g. React StrictMode double-invoke) must
    // not both open a connection: the second loses the single-flight guard. So we
    // see exactly one Connecting → Connected → GamesUpdated, never a stray second
    // Connecting or a teardown that clobbers state.
    let (app, app_loop) = App::new("test", fake_ports());
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();

    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();

    assert!(matches!(
        events.recv().await.unwrap(),
        AppEvent::Lobby(faf_domain::state::LobbyEvent::Connecting)
    ));
    assert!(matches!(
        events.recv().await.unwrap(),
        AppEvent::Lobby(faf_domain::state::LobbyEvent::Connected)
    ));
    assert!(matches!(
        events.recv().await.unwrap(),
        AppEvent::Lobby(faf_domain::state::LobbyEvent::AvatarsLoading)
    ));
    assert!(matches!(
        events.recv().await.unwrap(),
        AppEvent::Lobby(faf_domain::state::LobbyEvent::GamesUpdated { .. })
    ));

    let snap = app.snapshot();
    assert_eq!(snap.lobby.status, LobbyStatus::Connected);
    assert!(!snap.lobby.games.is_empty());
}

#[tokio::test]
async fn join_emits_joining_then_launching() {
    let (app, app_loop) = App::new("test", fake_ports());
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();

    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();

    // Wait until the connection is live (a snapshot arrived) before joining, so
    // the fake has stored the live update sender.
    loop {
        if let AppEvent::Lobby(faf_domain::state::LobbyEvent::GamesUpdated { .. }) =
            events.recv().await.unwrap()
        {
            break;
        }
    }

    app.dispatch(
        LobbyCommand::Join {
            id: 2,
            password: None,
            replace_mods: false,
        }
        .into(),
    )
    .await
    .unwrap();

    // Optimistic Joining is emitted immediately by the service.
    loop {
        match events.recv().await.unwrap() {
            AppEvent::Lobby(faf_domain::state::LobbyEvent::Joining { id, prepared }) => {
                assert_eq!(id, 2);
                assert!(!prepared, "the fake process does not prepare local content");
                break;
            }
            _ => continue, // a GamesUpdated tick may interleave
        }
    }

    // The fake replies with a launch order on the same stream.
    loop {
        match events.recv().await.unwrap() {
            AppEvent::Lobby(faf_domain::state::LobbyEvent::Launching { launch }) => {
                assert_eq!(launch.uid, 2);
                break;
            }
            _ => continue,
        }
    }

    let snap = app.snapshot();
    match snap.lobby.join {
        JoinState::Launched { launch } => assert_eq!(launch.uid, 2),
        other => panic!("expected Launched, got {other:?}"),
    }
}

async fn search_started(
    app: &App,
    events: &mut tokio::sync::broadcast::Receiver<AppEvent>,
) -> Vec<MatchmakingState> {
    app.dispatch(
        LobbyCommand::StartSearch {
            queue_names: vec!["ladder1v1".into()],
        }
        .into(),
    )
    .await
    .unwrap();
    let mut seen = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let AppEvent::Lobby(LobbyEvent::MatchmakingUpdated { state }) =
                events.recv().await.unwrap()
            {
                let searching = matches!(state, MatchmakingState::Searching { .. });
                seen.push(state);
                if searching {
                    break;
                }
            }
        }
    })
    .await
    .expect("the search never started");
    seen
}

#[tokio::test]
async fn a_search_is_prepared_before_the_server_is_asked_for_it() {
    // Java's order: get ready, then queue. The bar shows the first half.
    let lobby = FakeLobby::default();
    let ports = Ports {
        lobby: Arc::new(lobby.clone()),
        ..fake_ports()
    };
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();
    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    wait_for_initial_games(&mut events).await;

    let seen = search_started(&app, &mut events).await;

    assert_eq!(
        seen.first(),
        Some(&MatchmakingState::Preparing {
            queue_names: vec!["ladder1v1".into()],
        })
    );
    assert_eq!(
        app.snapshot().lobby.matchmaking,
        MatchmakingState::Searching {
            queue_names: vec!["ladder1v1".into()],
        }
    );
}

#[tokio::test]
async fn a_party_invite_cannot_be_accepted_while_searching() {
    // The server would move the player into the party and leave their own
    // search running, which as a member they could not stop.
    let lobby = FakeLobby::default();
    let ports = Ports {
        lobby: Arc::new(lobby.clone()),
        ..fake_ports()
    };
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();
    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    wait_for_initial_games(&mut events).await;
    search_started(&app, &mut events).await;

    app.dispatch(LobbyCommand::AcceptPartyInvite { player_id: 5 }.into())
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let AppEvent::Notifications(NotificationEvent::Added { notification }) =
                events.recv().await.unwrap()
            {
                if notification.title == "Cannot join the party" {
                    assert!(notification.body.contains("Stop your search"));
                    break;
                }
            }
        }
    })
    .await
    .expect("the refusal was never shown");
}

/// The fake lobby's avatar list holds the two Galactic War avatars; the
/// tournament one below is in neither, which is what a lapsed one looks like.
const AEON_AVATAR: &str = "https://content.faforever.com/faf/avatars/GW_Aeon.png";
const CUP_AVATAR: &str = "https://content.faforever.com/faf/avatars/cup_winner.png";

struct AvatarHistorySettings {
    history: Vec<String>,
    saved: Arc<Mutex<Vec<SettingsState>>>,
}

#[async_trait]
impl SettingsPort for AvatarHistorySettings {
    async fn load(&self) -> Option<SettingsState> {
        Some(SettingsState {
            game: faf_domain::state::GamePreferences {
                cache_lifetime_days: None,
                ..faf_domain::state::GamePreferences::default()
            },
            avatar_history: self.history.clone(),
            ..SettingsState::default()
        })
    }

    async fn save(&self, settings: &SettingsState) -> Result<(), String> {
        self.saved.lock().unwrap().push(settings.clone());
        Ok(())
    }
}

#[tokio::test]
async fn a_lapsed_tournament_avatar_is_replaced_by_the_one_chosen_before_it() {
    let lobby = FakeLobby::default();
    let saved = Arc::new(Mutex::new(Vec::new()));
    let ports = Ports {
        lobby: Arc::new(lobby.clone()),
        settings: Arc::new(AvatarHistorySettings {
            history: vec![CUP_AVATAR.into(), AEON_AVATAR.into()],
            saved: saved.clone(),
        }),
        ..fake_ports()
    };
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());
    let mut events = app.subscribe();

    app.dispatch_and_wait(faf_domain::state::SettingsCommand::Load.into())
        .await
        .unwrap();
    app.dispatch_and_wait(faf_domain::state::AuthCommand::LoginTest.into())
        .await
        .unwrap();
    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    wait_for_initial_games(&mut events).await;

    // Login asked for the avatar list; our own `player_info` is the other half,
    // and it names no avatar: the server stopped sending the lapsed one.
    assert!(
        lobby.push_update(LobbyUpdate::PlayersSeen(vec![PlayerProfile {
            id: 42,
            login: "TestCommander".into(),
            ..PlayerProfile::default()
        }]))
    );

    let mut notified = false;
    for _ in 0..200 {
        if app
            .snapshot()
            .settings
            .avatar_history
            .first()
            .map(String::as_str)
            == Some(AEON_AVATAR)
            && notified
        {
            break;
        }
        while let Ok(event) = events.try_recv() {
            if let AppEvent::Notifications(NotificationEvent::Added { notification }) = event {
                notified |= notification.kind == NotificationKind::AvatarRestored;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let snapshot = app.snapshot();
    assert_eq!(
        snapshot.settings.avatar_history,
        vec![AEON_AVATAR.to_string(), CUP_AVATAR.to_string()]
    );
    let me = snapshot
        .social
        .players
        .iter()
        .find(|player| player.id == 42)
        .unwrap();
    assert_eq!(me.avatar_url, AEON_AVATAR);
    assert!(notified, "the switch is announced once");
    assert_eq!(
        saved.lock().unwrap().last().unwrap().avatar_history,
        snapshot.settings.avatar_history,
        "the new order is saved, so the next login does not fall back again"
    );
}
