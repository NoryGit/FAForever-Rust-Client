//! Tutorial orchestration.
//!
//! Listing is an ordinary read. Launching is the interesting half: a lesson
//! needs the `tutorials` featured mod patched and its map on disk before the
//! game can open it, which is exactly what the launch path already does for a
//! live game: so this reuses [`GameUpdaterPort`](crate::ports::GameUpdaterPort)
//! rather than growing a second copy of that logic.
//!
//! Mirrors the Java client's `GameRunner::launchTutorial`, which waits on the
//! same two futures (`updateFeaturedModToLatest` + `downloadIfNecessary`)
//! before calling `launchOfflineGame`.

use faf_domain::state::{JoinState, TutorialsCommand, TutorialsEvent, TUTORIALS_FEATURED_MOD};

use crate::ports::{GamePreparation, UpdateProgress};
use crate::runtime::{CancellationSlot, EventSink, ServiceCtx};

#[derive(Default)]
pub struct TutorialsContext {
    launch: CancellationSlot,
}

pub async fn handle(cmd: TutorialsCommand, ctx: &ServiceCtx, out: &EventSink) {
    match cmd {
        TutorialsCommand::CancelLaunch => ctx.tutorials.launch.cancel(),
        TutorialsCommand::Load => {
            out.emit(TutorialsEvent::Loading);
            match ctx.ports.tutorials.list_tutorials().await {
                Ok((categories, tutorials)) => out.emit(TutorialsEvent::Loaded {
                    categories,
                    tutorials,
                }),
                Err(reason) => out.emit(TutorialsEvent::LoadFailed { reason }),
            }
        }
        TutorialsCommand::Select { tutorial_id } => {
            out.emit(TutorialsEvent::Selected { tutorial_id })
        }
        TutorialsCommand::Launch { tutorial_id } => launch(tutorial_id, ctx, out).await,
    }
}

async fn launch(tutorial_id: i32, ctx: &ServiceCtx, out: &EventSink) {
    crate::runtime::expect_admitted(crate::runtime::Key::TutorialLaunch);
    let cancel = ctx.tutorials.launch.begin();
    let Some(tutorial) = out.with_state(|state| {
        state
            .tutorials
            .tutorials
            .iter()
            .find(|tutorial| tutorial.id == tutorial_id)
            .cloned()
    }) else {
        out.emit(TutorialsEvent::LaunchFailed {
            tutorial_id,
            reason: "that tutorial is no longer in the list".into(),
        });
        return;
    };

    // Before anything is patched. The game slot holds one process, so
    // launching now would end the game in progress, and patching the
    // tutorials mod under a running game is no better. The same refusal
    // "Play offline" gives, and for the same reason.
    if game_is_running(ctx, out) {
        out.emit(TutorialsEvent::LaunchFailed {
            tutorial_id,
            reason: "Forged Alliance is already running. Close it before starting a lesson.".into(),
        });
        return;
    }

    // Checked here rather than only by disabling the button: the button is one
    // route to this command, and a tutorial can stop being playable between
    // the list loading and the click.
    if !tutorial.is_playable() {
        out.emit(TutorialsEvent::LaunchFailed {
            tutorial_id,
            reason: format!("“{}” cannot be played yet", tutorial.title),
        });
        return;
    }

    // Same preparation a live game gets: patch the featured mod, fetch the
    // map: narrated because it is slow the first time.
    out.emit(TutorialsEvent::LaunchPreparing {
        tutorial_id,
        detail: "Preparing tutorial".into(),
    });
    let mut updates = ctx
        .ports
        .updater
        .prepare_cancellable(
            GamePreparation {
                featured_mod: TUTORIALS_FEATURED_MOD.to_string(),
                map_folder: Some(tutorial.map_folder_name.clone()),
                cache_rolling_branches: false,
            },
            cancel.clone(),
        )
        .await;

    let mut outcome = Err("the game updater stopped without finishing".to_string());
    loop {
        let update = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                out.emit(TutorialsEvent::LaunchCancelled);
                return;
            }
            update = updates.recv() => update,
        };
        let Some(update) = update else {
            break;
        };
        match update {
            UpdateProgress::Step(step) => {
                out.emit(TutorialsEvent::LaunchPreparing {
                    tutorial_id,
                    detail: step.detail,
                });
            }
            UpdateProgress::Finished(result) => outcome = result,
        }
    }
    if cancel.is_cancelled() {
        out.emit(TutorialsEvent::LaunchCancelled);
        return;
    }
    if let Err(reason) = outcome {
        out.emit(TutorialsEvent::LaunchFailed {
            tutorial_id,
            reason,
        });
        return;
    }

    match ctx
        .ports
        .process
        .launch_offline(
            TUTORIALS_FEATURED_MOD.to_string(),
            tutorial.technical_name.clone(),
        )
        .await
    {
        Ok(()) => {
            super::replays::cancel_live_tracking(out);
            out.emit(TutorialsEvent::Launched { tutorial_id });
        }
        Err(reason) => out.emit(TutorialsEvent::LaunchFailed {
            tutorial_id,
            reason,
        }),
    }
}

/// Whether Forged Alliance is running for this client, or about to be: a
/// process the port started, a game the lobby is tracking, or a launch the
/// lobby has already ordered.
fn game_is_running(ctx: &ServiceCtx, out: &EventSink) -> bool {
    ctx.ports.process.game_running()
        || ctx.lobby.running_game_id().is_some()
        || out.with_state(|state| {
            matches!(
                state.lobby.join,
                JoinState::Launched { .. } | JoinState::InGame
            )
        })
}
