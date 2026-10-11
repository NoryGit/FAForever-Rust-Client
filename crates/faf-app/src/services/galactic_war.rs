//! Galactic War orchestration: refresh, install, launch.
//!
//! All three decisions this sequences: what to install, whether an update is
//! needed, whether a launch is allowed: live in the domain
//! ([`faf_domain::state::galactic_war`]). This file is only ordering and the
//! single-flight guards.
//!
//! The two failure domains stay apart, which is the whole reason the slice has
//! two statuses: statistics are fetched independently of the install machine,
//! and a gateway that is down leaves the Play button exactly as it was.

use faf_domain::state::{
    GalacticWarCommand, GalacticWarEvent, GalacticWarStatus, StatisticsStatus,
};

use crate::ports::InstallProgress;
use crate::runtime::{CancellationSlot, EventSink, ServiceCtx};

#[derive(Default)]
pub struct GalacticWarContext {
    install: CancellationSlot,
}

pub async fn handle(cmd: GalacticWarCommand, ctx: &ServiceCtx, out: &EventSink) {
    match cmd {
        GalacticWarCommand::CancelInstall => ctx.galactic_war.install.cancel(),
        GalacticWarCommand::Refresh => refresh(ctx, out).await,
        GalacticWarCommand::RefreshStatistics => refresh_statistics(ctx, out).await,
        GalacticWarCommand::Install => {
            install(ctx, out).await;
        }
        GalacticWarCommand::Play => play(ctx, out).await,
    }
}

/// Read what is installed, what the gateway wants, and the season statistics.
///
/// The installed version is published even when the gateway cannot be reached,
/// because it is the half that decides whether the client can be started at
/// all: an offline user with an install should still get a Play button.
async fn refresh(ctx: &ServiceCtx, out: &EventSink) {
    out.emit(GalacticWarEvent::InstallationChanged {
        version: ctx.ports.galactic_war.installed_version(),
    });
    publish_minimum_check(out);

    // Deliberately not joined with the statistics call below: a slow or broken
    // statistics endpoint must not delay the version answer the buttons
    // depend on.
    if !out.with_state(|state| state.galactic_war.status.is_busy()) {
        out.emit(GalacticWarEvent::StatusChanged {
            status: GalacticWarStatus::CheckingVersion,
        });
        match ctx.ports.galactic_war.versions().await {
            Ok(versions) => {
                out.emit(GalacticWarEvent::VersionsLoaded { versions });
                publish_minimum_check(out);
                out.emit(GalacticWarEvent::StatusChanged {
                    status: GalacticWarStatus::Idle,
                });
            }
            Err(reason) => out.emit(GalacticWarEvent::StatusChanged {
                status: GalacticWarStatus::Failed { reason },
            }),
        }
    }

    refresh_statistics(ctx, out).await;
}

/// Recompute whether the installed build is below the gateway's minimum, and
/// publish it.
///
/// Called after every change to either input. The version ordering lives in
/// the domain and stays there: publishing the one bit is what keeps the
/// frontend mirror from having to reimplement it (see the slice's module doc).
fn publish_minimum_check(out: &EventSink) {
    let below_minimum = out.with_state(|state| state.galactic_war.recheck_minimum());
    if below_minimum != out.with_state(|state| state.galactic_war.below_minimum) {
        out.emit(GalacticWarEvent::MinimumCheckChanged { below_minimum });
    }
}

async fn refresh_statistics(ctx: &ServiceCtx, out: &EventSink) {
    out.emit(GalacticWarEvent::StatisticsStatusChanged {
        status: StatisticsStatus::Loading,
    });
    match ctx.ports.galactic_war.statistics().await {
        // Carries the status with it, so a loaded document and the `loaded`
        // status cannot be set apart from each other.
        Ok(statistics) => out.emit(GalacticWarEvent::StatisticsLoaded { statistics }),
        Err(reason) => out.emit(GalacticWarEvent::StatisticsStatusChanged {
            status: StatisticsStatus::Failed { reason },
        }),
    }
}

/// Install whatever the gateway currently points at.
///
/// Returns whether an installation is now in place, so [`play`] can chain a
/// launch onto it without re-reading the state and re-deciding.
async fn install(ctx: &ServiceCtx, out: &EventSink) -> bool {
    crate::runtime::expect_admitted(crate::runtime::Key::GalacticWar);
    // Downloads are tens of megabytes and the extraction writes into the
    // install directory: two concurrent runs would fight over the same
    // staging area and the same manifest. `Install` and `Play`, the two
    // commands that reach this, are single-flight together in the command
    // policy (`Key::GalacticWar`).
    let state = out.with_state(|state| state.galactic_war.clone());
    if state.is_busy() || state.status == GalacticWarStatus::Running {
        return false;
    }
    let Some(version) = state.install_target().map(str::to_string) else {
        // Nothing to install: the gateway has not been read yet, or said
        // nothing usable. `refresh` has already reported why.
        return false;
    };
    let cancel = ctx.galactic_war.install.begin();

    out.emit(GalacticWarEvent::StatusChanged {
        status: GalacticWarStatus::Downloading {
            version: version.clone(),
            downloaded_bytes: 0,
            total_bytes: 0,
        },
    });

    let mut progress = ctx
        .ports
        .galactic_war
        .install_cancellable(version.clone(), cancel.clone())
        .await;
    // The port always ends with `Finished`. Treating a stream that closes
    // without one as a failure keeps a panicked task from leaving the UI on a
    // progress bar that will never move again.
    let mut settled = None;
    while let Some(step) = progress.recv().await {
        if cancel.is_cancelled() && !matches!(step, InstallProgress::Finished(_)) {
            continue;
        }
        match step {
            InstallProgress::Downloading {
                received_bytes,
                total_bytes,
            } => out.emit(GalacticWarEvent::StatusChanged {
                status: GalacticWarStatus::Downloading {
                    version: version.clone(),
                    downloaded_bytes: received_bytes,
                    total_bytes,
                },
            }),
            InstallProgress::Extracting => out.emit(GalacticWarEvent::StatusChanged {
                status: GalacticWarStatus::Installing {
                    version: version.clone(),
                },
            }),
            InstallProgress::Finished(outcome) => settled = Some(outcome),
        }
    }

    match settled {
        Some(Ok(installed)) => {
            out.emit(GalacticWarEvent::InstallationChanged {
                version: Some(installed),
            });
            publish_minimum_check(out);
            out.emit(GalacticWarEvent::StatusChanged {
                status: GalacticWarStatus::Idle,
            });
            !cancel.is_cancelled()
        }
        Some(Err(reason)) => {
            out.emit(GalacticWarEvent::StatusChanged {
                status: if cancel.is_cancelled() {
                    GalacticWarStatus::Idle
                } else {
                    GalacticWarStatus::Failed { reason }
                },
            });
            false
        }
        None => {
            out.emit(GalacticWarEvent::StatusChanged {
                status: if cancel.is_cancelled() {
                    GalacticWarStatus::Idle
                } else {
                    GalacticWarStatus::Failed {
                        reason: "the installation stopped without finishing".into(),
                    }
                },
            });
            false
        }
    }
}

/// The tab's single primary action: install or update if needed, then start.
async fn play(ctx: &ServiceCtx, out: &EventSink) {
    let state = out.with_state(|state| state.galactic_war.clone());
    if state.is_busy() || state.status == GalacticWarStatus::Running {
        return;
    }

    // The minimum check is included on purpose: a build below the gateway's
    // minimum would reach the login and be turned away there, which tells the
    // user less than updating it silently does. Recomputed rather than read
    // off the state, so a launch cannot act on a check that has gone stale.
    let needs_install =
        !state.is_installed() || state.update_available() || state.recheck_minimum();
    if needs_install && !install(ctx, out).await {
        return;
    }
    if needs_install && ctx.galactic_war.install.is_cancelled() {
        return;
    }

    launch(ctx, out).await;
}

async fn launch(ctx: &ServiceCtx, out: &EventSink) {
    if !out.with_state(|state| state.galactic_war.can_launch()) {
        return;
    }

    out.emit(GalacticWarEvent::StatusChanged {
        status: GalacticWarStatus::Launching,
    });
    if let Err(reason) = ctx.ports.galactic_war.launch().await {
        out.emit(GalacticWarEvent::StatusChanged {
            status: GalacticWarStatus::Failed { reason },
        });
        return;
    }
    out.emit(GalacticWarEvent::StatusChanged {
        status: GalacticWarStatus::Running,
    });

    // Without this the tab says "Running" until the client is restarted: the
    // process is external, so nothing else would ever tell us it ended. Same
    // reason `ProcessPort::wait_for_exit` exists for Forged Alliance.
    let ports = ctx.ports.clone();
    let sink = out.clone();
    tokio::spawn(async move {
        ports.galactic_war.wait_for_exit().await;
        sink.emit(GalacticWarEvent::StatusChanged {
            status: GalacticWarStatus::Idle,
        });
    });
}
