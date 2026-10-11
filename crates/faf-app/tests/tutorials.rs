//! Tutorial service tests.
//!
//! The launch path is what matters: a lesson needs the `tutorials` featured
//! mod patched and its map on disk *before* the game opens, and it must run
//! offline: no lobby, no connectivity adapter.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use faf_app::infra::fake_ports;
use faf_app::ports::{
    GameLaunchParams, GamePreparation, GameUpdaterPort, InstallPresence, ProcessPort,
    TutorialsPort, UpdateProgress,
};
use faf_app::{App, Ports};
use faf_domain::state::{
    Tutorial, TutorialCategory, TutorialLaunchStatus, TutorialsCommand, TutorialsStatus,
};
use tokio::sync::mpsc;

/// Records what the game was asked to start, and whether it was online.
#[derive(Default)]
struct RecordingProcess {
    offline: Arc<Mutex<Vec<(String, String)>>>,
    online: Arc<Mutex<u32>>,
    /// Whether a game is already running, as the port reports it.
    running: bool,
}

#[async_trait]
impl ProcessPort for RecordingProcess {
    fn supports_live_launch(&self) -> bool {
        true
    }
    fn game_running(&self) -> bool {
        self.running
    }
    async fn launch_game(&self, _params: GameLaunchParams) -> Result<(), String> {
        *self.online.lock().unwrap() += 1;
        Ok(())
    }
    async fn launch_offline(&self, featured_mod: String, map: String) -> Result<(), String> {
        self.offline.lock().unwrap().push((featured_mod, map));
        Ok(())
    }
    async fn launch_replay(&self, _args: Vec<String>) -> Result<(), String> {
        Ok(())
    }
    fn kill(&self) {}
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

/// Records what preparation was requested, and reports a scripted outcome.
struct ScriptedUpdater {
    seen: Arc<Mutex<Vec<GamePreparation>>>,
    outcome: Result<(), String>,
}

#[async_trait]
impl GameUpdaterPort for ScriptedUpdater {
    async fn prepare(&self, request: GamePreparation) -> mpsc::Receiver<UpdateProgress> {
        self.seen.lock().unwrap().push(request);
        let (tx, rx) = mpsc::channel(4);
        tx.send(UpdateProgress::Step(
            faf_app::ports::PreparationStep::indeterminate(
                faf_app::ports::PreparationPhase::Downloading,
                "Updating tutorials…",
            ),
        ))
        .await
        .unwrap();
        tx.send(UpdateProgress::Finished(self.outcome.clone()))
            .await
            .unwrap();
        rx
    }
}

struct StubTutorials(Vec<Tutorial>);

#[derive(Default)]
struct HeldUpdater {
    sender: Mutex<Option<mpsc::Sender<UpdateProgress>>>,
    cancel: Mutex<Option<tokio_util::sync::CancellationToken>>,
}

#[async_trait]
impl GameUpdaterPort for HeldUpdater {
    async fn prepare(&self, _request: GamePreparation) -> mpsc::Receiver<UpdateProgress> {
        unreachable!("tutorial preparation must pass cancellation")
    }
    async fn prepare_cancellable(
        &self,
        _request: GamePreparation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> mpsc::Receiver<UpdateProgress> {
        let (tx, rx) = mpsc::channel(4);
        *self.sender.lock().unwrap() = Some(tx);
        *self.cancel.lock().unwrap() = Some(cancel);
        rx
    }
}

#[tokio::test]
async fn cancelling_tutorial_preparation_never_launches_the_game() {
    let updater = Arc::new(HeldUpdater::default());
    let process = Arc::new(RecordingProcess::default());
    let (app, app_loop) = App::new(
        "test",
        Ports {
            tutorials: Arc::new(StubTutorials(vec![tutorial(7)])),
            updater: updater.clone(),
            process: process.clone(),
            ..fake_ports()
        },
    );
    tokio::spawn(app_loop.run());
    app.dispatch_and_wait(TutorialsCommand::Load.into())
        .await
        .unwrap();
    let app = Arc::new(app);
    let launch = {
        let app = app.clone();
        tokio::spawn(async move {
            app.dispatch_and_wait(TutorialsCommand::Launch { tutorial_id: 7 }.into())
                .await
                .unwrap();
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while updater.cancel.lock().unwrap().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    app.dispatch_and_wait(TutorialsCommand::CancelLaunch.into())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), launch)
        .await
        .unwrap()
        .unwrap();
    assert!(updater
        .cancel
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .is_cancelled());
    assert_eq!(app.snapshot().tutorials.launch, TutorialLaunchStatus::Idle);
    let sender = updater.sender.lock().unwrap().take().unwrap();
    assert!(sender.send(UpdateProgress::Finished(Ok(()))).await.is_err());
    assert!(process.offline.lock().unwrap().is_empty());
}

#[async_trait]
impl TutorialsPort for StubTutorials {
    async fn list_tutorials(&self) -> Result<(Vec<TutorialCategory>, Vec<Tutorial>), String> {
        Ok((
            vec![TutorialCategory {
                id: 1,
                name: "Basics".into(),
            }],
            self.0.clone(),
        ))
    }
}

fn tutorial(id: i32) -> Tutorial {
    Tutorial {
        id,
        title: format!("Lesson {id}"),
        description: String::new(),
        link_url: String::new(),
        image_url: String::new(),
        ordinal: id,
        launchable: true,
        map_folder_name: format!("scmp_tut_{id}"),
        technical_name: format!("tut_{id}"),
        category_id: Some(1),
    }
}

struct Harness {
    app: App,
    offline: Arc<Mutex<Vec<(String, String)>>>,
    online: Arc<Mutex<u32>>,
    prepared: Arc<Mutex<Vec<GamePreparation>>>,
}

fn harness(tutorials: Vec<Tutorial>, outcome: Result<(), String>) -> Harness {
    harness_with(tutorials, outcome, false)
}

fn harness_with(tutorials: Vec<Tutorial>, outcome: Result<(), String>, running: bool) -> Harness {
    let offline = Arc::new(Mutex::new(Vec::new()));
    let online = Arc::new(Mutex::new(0));
    let prepared = Arc::new(Mutex::new(Vec::new()));
    let ports = Ports {
        tutorials: Arc::new(StubTutorials(tutorials)),
        process: Arc::new(RecordingProcess {
            offline: offline.clone(),
            online: online.clone(),
            running,
        }),
        updater: Arc::new(ScriptedUpdater {
            seen: prepared.clone(),
            outcome,
        }),
        ..fake_ports()
    };
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());
    Harness {
        app,
        offline,
        online,
        prepared,
    }
}

impl Harness {
    /// Wait until the launch status settles, either way.
    async fn settled(&self) -> TutorialLaunchStatus {
        for _ in 0..300 {
            let launch = self.app.snapshot().tutorials.launch;
            if matches!(
                launch,
                TutorialLaunchStatus::Launched { .. } | TutorialLaunchStatus::Failed { .. }
            ) {
                return launch;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!(
            "the launch never settled: {:?}",
            self.app.snapshot().tutorials
        );
    }

    async fn load(&self) {
        self.app
            .dispatch(TutorialsCommand::Load.into())
            .await
            .unwrap();
        for _ in 0..300 {
            if self.app.snapshot().tutorials.status == TutorialsStatus::Ready {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the catalog never loaded");
    }
}

#[tokio::test]
async fn playing_a_lesson_patches_the_mod_fetches_the_map_then_starts_offline() {
    let h = harness(vec![tutorial(7), tutorial(8)], Ok(()));
    h.load().await;
    h.app
        .dispatch(TutorialsCommand::Launch { tutorial_id: 8 }.into())
        .await
        .unwrap();

    assert_eq!(
        h.settled().await,
        TutorialLaunchStatus::Launched { tutorial_id: 8 }
    );

    let prepared = h.prepared.lock().unwrap().clone();
    assert_eq!(prepared.len(), 1);
    assert_eq!(prepared[0].featured_mod, "tutorials");
    assert_eq!(prepared[0].map_folder.as_deref(), Some("scmp_tut_8"));

    // Offline, with the scenario name: not a lobby launch.
    assert_eq!(
        *h.offline.lock().unwrap(),
        vec![("tutorials".to_string(), "tut_8".to_string())]
    );
    assert_eq!(
        *h.online.lock().unwrap(),
        0,
        "a tutorial must never go through the lobby launch path"
    );
}

#[tokio::test]
async fn a_failed_preparation_stops_the_launch() {
    // Starting anyway would open the game on a map it does not have.
    let h = harness(vec![tutorial(7)], Err("could not update tutorials".into()));
    h.load().await;
    h.app
        .dispatch(TutorialsCommand::Launch { tutorial_id: 7 }.into())
        .await
        .unwrap();

    match h.settled().await {
        TutorialLaunchStatus::Failed { reason, .. } => assert!(reason.contains("tutorials")),
        other => panic!("expected a failure, got {other:?}"),
    }
    assert!(h.offline.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_unplayable_lesson_is_refused_before_anything_is_downloaded() {
    // The button is disabled for these, but the button is not the only route
    // to the command: and a lesson can stop being playable after the list
    // loaded.
    let h = harness(
        vec![Tutorial {
            map_folder_name: String::new(),
            ..tutorial(7)
        }],
        Ok(()),
    );
    h.load().await;
    h.app
        .dispatch(TutorialsCommand::Launch { tutorial_id: 7 }.into())
        .await
        .unwrap();

    match h.settled().await {
        TutorialLaunchStatus::Failed { reason, .. } => assert!(reason.contains("cannot be played")),
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert!(
        h.prepared.lock().unwrap().is_empty(),
        "nothing should be downloaded for a lesson that cannot start"
    );
    assert!(h.offline.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_lesson_is_refused_while_the_game_is_running() {
    // Launching would end the game in progress, and patching the tutorials
    // mod under it is no better, so nothing is prepared at all.
    let h = harness_with(vec![tutorial(7)], Ok(()), true);
    h.load().await;
    h.app
        .dispatch(TutorialsCommand::Launch { tutorial_id: 7 }.into())
        .await
        .unwrap();

    match h.settled().await {
        TutorialLaunchStatus::Failed {
            tutorial_id,
            reason,
        } => {
            assert_eq!(tutorial_id, 7, "the failure names the lesson it is about");
            assert!(reason.contains("already running"), "{reason}");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert!(h.prepared.lock().unwrap().is_empty());
    assert!(h.offline.lock().unwrap().is_empty());
}

#[tokio::test]
async fn launching_an_unknown_lesson_fails_cleanly() {
    let h = harness(vec![tutorial(7)], Ok(()));
    h.load().await;
    h.app
        .dispatch(TutorialsCommand::Launch { tutorial_id: 99 }.into())
        .await
        .unwrap();

    match h.settled().await {
        TutorialLaunchStatus::Failed { reason, .. } => assert!(reason.contains("no longer")),
        other => panic!("expected a failure, got {other:?}"),
    }
    assert!(h.prepared.lock().unwrap().is_empty());
}

#[tokio::test]
async fn the_catalog_opens_the_first_lesson() {
    let h = harness(vec![tutorial(7), tutorial(8)], Ok(()));
    h.load().await;

    let state = h.app.snapshot().tutorials;
    assert_eq!(state.categories.len(), 1);
    assert_eq!(state.tutorials.len(), 2);
    assert_eq!(state.selected_id, Some(7));
}
