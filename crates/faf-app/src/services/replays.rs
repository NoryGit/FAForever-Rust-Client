//! Replays service.
//!
//! Thin handler (like `services/nav.rs`): asks the replay ports (vault,
//! library, details, playback; see [`crate::ports::replay`]) to do the work,
//! then emits `Connecting`/`Playing`/`Failed`. The actual protocol (WebSocket
//! relay, file decompression, launching FA) lives entirely behind the ports:
//! see `infra/replay/`.

use crate::ports::PreparationStep;
use faf_domain::state::ReplayPreparation;
use faf_domain::state::{
    live_replay_delay_remaining, LiveReplayTarget, LiveReplayTracking, LiveReplayTrackingAction,
    NavEvent, NotificationAction, NotificationKind, ReplayCommand, ReplayEvent, ReplayLaunch,
    ReplayQuery, ResolvedReplayMap, Tab, VaultReplay,
};
use std::collections::HashMap;
use std::{path::PathBuf, time::Duration};

use crate::runtime::{Cancellable, EventSink, LatestRequest, ServiceCtx};
use crate::services::notifications;

/// The replay service's operational context: which vault and local listings
/// may still land, and the replay launch in flight. Owned by this service.
#[derive(Default)]
pub struct ReplaysContext {
    /// Only the newest vault search may land. Commands run concurrently, so
    /// request order is not response order.
    vault_generation: LatestRequest,
    /// Only the newest local listing may land. Deleting a replay invalidates
    /// it as well, so an older directory scan cannot restore the deleted row.
    local_generation: LatestRequest,
    /// The in-flight replay launch, so the overlay's Cancel button has
    /// something to press. Same shape as the auth service's login
    /// cancellation, and for the same reason: dropping the future is the only
    /// thing that actually stops work that is several awaits deep inside a
    /// port. See [`LaunchSlot`] for which launch may settle it.
    launch: std::sync::Mutex<LaunchSlot>,
    /// The analysis read in flight, so the next request can call it off. See
    /// [`AnalysisSlot`].
    analysis: std::sync::Mutex<AnalysisSlot>,
    /// The details reads in flight, by read key, so closing the panel that
    /// asked for one can call it off (`CancelReads`). Several at once: two
    /// panels opened in turn can both have a read out.
    details: Cancellable<(String, Option<u32>)>,
    /// The downloads into the library in flight, by game id, so the status
    /// bar can call one off (`CancelDownload`).
    downloads: Cancellable<i32>,
}

/// The replay analysis being read, while it is.
///
/// The analysis is the expensive read: the whole command stream, fetched from
/// the vault first when no copy is on disk. Only the answer to the request
/// made last is kept (see `ReplayState::analysis`), so an older read still
/// running when a newer one starts was walking a file for nobody, to the end,
/// and the reducer then threw its answer away.
#[derive(Default)]
struct AnalysisSlot {
    current: u64,
    running: Option<AnalysisRequest>,
}

struct AnalysisRequest {
    key: String,
    request_id: Option<u32>,
    called_off: tokio_util::sync::CancellationToken,
}

pub(crate) struct AnalysisWork {
    id: u64,
    uid: i32,
    key: String,
    local_path: Option<String>,
    called_off: tokio_util::sync::CancellationToken,
}

pub(crate) struct DetailsWork {
    uid: i32,
    key: String,
    local_path: Option<String>,
    ticket: crate::runtime::CancelTicket,
}

pub(crate) fn begin_details(
    uid: i32,
    local_path: Option<String>,
    request_id: Option<u32>,
    ctx: &ServiceCtx,
    out: &EventSink,
) -> DetailsWork {
    let key = faf_domain::state::replay_read_key(uid, local_path.as_deref());
    let _slot = ctx.replays.analysis_slot();
    ctx.replays.details.cancel(|(running, _)| running == &key);
    let ticket = ctx.replays.details.begin((key.clone(), request_id));
    out.emit(ReplayEvent::DetailsLoading { key: key.clone() });
    DetailsWork {
        uid,
        key,
        local_path,
        ticket,
    }
}

pub(crate) async fn run_details(work: DetailsWork, ctx: &ServiceCtx, out: &EventSink) {
    let DetailsWork {
        uid,
        key,
        local_path,
        ticket,
    } = work;
    let path = local_path.map(PathBuf::from);
    let result = tokio::select! {
        biased;
        () = ticket.called_off.cancelled() => None,
        result = ctx.ports.replay_details.load_details(uid, path) => Some(result),
    };
    let _slot = ctx.replays.analysis_slot();
    ctx.replays.details.end(&ticket);
    if ticket.called_off.is_cancelled() {
        return;
    }
    match result {
        None => {}
        Some(Ok(details)) => out.emit(ReplayEvent::DetailsLoaded { key, details }),
        Some(Err(reason)) => out.emit(ReplayEvent::DetailsFailed { key, reason }),
    }
}

/// Which replay launch is the current one, and its cancellation while armed.
#[derive(Default)]
struct LaunchSlot {
    /// Bumped by every launch. A launch whose number is no longer here was
    /// replaced, and from then on owns nothing: not the progress sink, not
    /// the cancellation, not the status line. All three belong to its
    /// replacement.
    current: u64,
    cancellation: Option<tokio_util::sync::CancellationToken>,
    /// What the current launch is starting, so a second request for the same
    /// replay while it is still starting can be told apart from a new one.
    target: Option<LaunchTarget>,
}

/// Which replay a launch starts.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LaunchTarget {
    Vault(i32),
    Live(i32),
    File(String),
}

impl ReplaysContext {
    /// The Local tab's list generation, for a scan started outside a
    /// `LoadLocal` (the one after a game) to take its turn in the same order.
    pub(crate) fn local_list_generation(&self) -> LatestRequest {
        self.local_generation.clone()
    }

    fn launch_slot(&self) -> std::sync::MutexGuard<'_, LaunchSlot> {
        self.launch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn analysis_slot(&self) -> std::sync::MutexGuard<'_, AnalysisSlot> {
        self.analysis
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Claim a read before its task is spawned. Repeated requests from the same
/// owner reuse its running read; a replacement owner gets its own cancellation.
pub(crate) fn begin_analysis(
    uid: i32,
    local_path: Option<String>,
    request_id: Option<u32>,
    ctx: &ServiceCtx,
    out: &EventSink,
) -> Option<AnalysisWork> {
    let key = faf_domain::state::replay_read_key(uid, local_path.as_deref());
    let mut slot = ctx.replays.analysis_slot();
    if slot
        .running
        .as_ref()
        .is_some_and(|running| running.key == key && running.request_id == request_id)
    {
        return None;
    }
    let called_off = tokio_util::sync::CancellationToken::new();
    if let Some(previous) = slot.running.replace(AnalysisRequest {
        key: key.clone(),
        request_id,
        called_off: called_off.clone(),
    }) {
        previous.called_off.cancel();
    }
    slot.current = slot.current.wrapping_add(1);
    out.emit(ReplayEvent::AnalysisLoading { key: key.clone() });
    Some(AnalysisWork {
        id: slot.current,
        uid,
        key,
        local_path,
        called_off,
    })
}

pub(crate) async fn run_analysis(work: AnalysisWork, ctx: &ServiceCtx, out: &EventSink) {
    let AnalysisWork {
        id,
        uid,
        key,
        local_path,
        called_off,
    } = work;
    let path = local_path.map(PathBuf::from);
    let read = tokio::select! {
        biased;
        () = called_off.cancelled() => return,
        read = ctx.ports.replay_details.load_analysis_cancellable(uid, path, called_off.clone()) => read,
    };
    let mut slot = ctx.replays.analysis_slot();
    if slot.current != id {
        return;
    }
    slot.running = None;
    match read {
        Ok(analysis) => out.emit(ReplayEvent::AnalysisLoaded {
            analysis: faf_domain::state::ReplayAnalysis { key, ..analysis },
        }),
        Err(reason) => out.emit(ReplayEvent::AnalysisFailed { key, reason }),
    }
}

/// A close only owns its request. A late cleanup cannot clear the loading
/// state or stop a replacement panel reading the same replay.
fn cancel_reads(key: &str, request_id: Option<u32>, ctx: &ServiceCtx, out: &EventSink) {
    let mut slot = ctx.replays.analysis_slot();
    let owned = |owner: Option<u32>| request_id.is_none() || owner == request_id;
    let details = ctx
        .replays
        .details
        .cancel(|(running, owner)| running == key && owned(*owner));
    let analysis = slot
        .running
        .as_ref()
        .is_some_and(|running| running.key == key && owned(running.request_id));
    if analysis {
        if let Some(running) = slot.running.take() {
            running.called_off.cancel();
        }
        slot.current = slot.current.wrapping_add(1);
    }
    let newer_analysis = slot
        .running
        .as_ref()
        .is_some_and(|running| running.key == key);
    let newer_details = ctx
        .replays
        .details
        .any_running(|(running, _)| running == key);
    if (details || analysis) && !newer_analysis && !newer_details {
        out.emit(ReplayEvent::ReadsCancelled {
            key: key.to_string(),
        });
    }
}

/// One replay launch, from the moment it became the current one.
struct LaunchTicket {
    id: u64,
    cancelled: tokio_util::sync::CancellationToken,
}

/// How many replay heads are fetched at the same time.
const RESOLVE_MAPS_AT_ONCE: usize = 6;
/// How many games one resolve command will look up at all.
const MAX_MAPS_PER_RESOLVE: usize = 120;

/// A remaining wait, phrased for someone staring at a button.
fn describe(seconds: u32) -> String {
    match seconds {
        0..=59 => format!("{seconds}s"),
        _ => format!("{}m {}s", seconds / 60, seconds % 60),
    }
}

/// Make a new replay launch the current one: cancel the launch it replaces and
/// point the preparation progress at this one's starting dialog.
///
/// Called before anything about the new launch is emitted, which is the point.
/// The launch it replaces settles under the same lock and only while it is
/// still current (see [`launch`]), so once this has returned nothing the older
/// one does can land on top of this one's `Connecting`. Its cancellation
/// branch used to clear this launch's progress sink and emit `Closed` after
/// it, idling the dialog of the launch that was actually starting.
///
/// `None` when the same replay is already starting: a double click on Watch,
/// or Watch on the card and then in the panel. Replacing that launch with
/// itself threw away whatever it had downloaded and began again from nothing,
/// so the repeat is the same request and is dropped instead.
fn begin_launch(ctx: &ServiceCtx, out: &EventSink, target: LaunchTarget) -> Option<LaunchTicket> {
    let cancelled = tokio_util::sync::CancellationToken::new();
    let mut slot = ctx.replays.launch_slot();
    if slot.cancellation.is_some() && slot.target.as_ref() == Some(&target) {
        return None;
    }
    slot.target = Some(target);
    slot.current = slot.current.wrapping_add(1);
    // Replacing an armed token cancels it: two launches cannot be in flight,
    // and the older one is the one nobody is waiting for.
    if let Some(previous) = slot.cancellation.replace(cancelled.clone()) {
        previous.cancel();
    }

    // What the preparation is doing, onto the starting dialog (#392).
    let sink = out.clone();
    ctx.ports
        .replay_playback
        .set_preparation_progress(Some(std::sync::Arc::new(move |step: PreparationStep| {
            sink.emit(ReplayEvent::Preparing {
                step: ReplayPreparation {
                    detail: step.detail,
                    progress: step.progress,
                },
            });
        })));
    Some(LaunchTicket {
        id: slot.current,
        cancelled,
    })
}

/// Start a replay, and stop if the user calls it off.
///
/// Dropping the port's future is what cancels it: fetching the file,
/// decompressing it, preparing the map and opening the relay are a chain of
/// awaits, and the one it is sitting in when Cancel is pressed never resumes.
/// Once the port has returned, Forged Alliance has already been handed the
/// file and there is nothing left to call off, so a cancel arriving then loses
/// the race and the launch is reported as the launch it is.
///
/// Cancelling emits [`ReplayEvent::Closed`], which is the same "back to idle,
/// you can start another one" this uses when a replay session ends of its own
/// accord. A cancelled start is not a failure and must not be reported as one:
/// the overlay would turn into an error nobody asked about.
///
/// A launch that another one replaced settles nothing at all, however it
/// ended: the progress sink, the cancellation and the status line are its
/// replacement's by then.
async fn launch(
    ticket: LaunchTicket,
    work: impl std::future::Future<Output = Result<Option<String>, String>>,
    uid: Option<i32>,
    ctx: &ServiceCtx,
    out: &EventSink,
) {
    let result = tokio::select! {
        result = work => Some(result),
        () = ticket.cancelled.cancelled() => None,
    };

    // Checked and settled under one lock, with the events emitted inside it,
    // so a replacement cannot begin between the check and what follows it.
    let mut slot = ctx.replays.launch_slot();
    if slot.current != ticket.id {
        return;
    }
    ctx.ports.replay_playback.set_preparation_progress(None);
    let Some(result) = result else {
        out.emit(ReplayEvent::Closed);
        return;
    };
    // Disarmed, so a Cancel pressed after the game is up cannot idle the
    // status of a replay that is playing.
    slot.cancellation = None;
    match result {
        Ok(warning) => out.emit(ReplayEvent::Playing { uid, warning }),
        Err(reason) => fail(out, reason),
    }
}

/// Report a replay that did not start.
///
/// Both halves, and both on purpose. The status keeps the tab's own state
/// machine honest, which is what re-enables the buttons; the notification is
/// what the user actually reads, because the status line used to be a strip of
/// text above the search panel that a reader who had just double-clicked a game
/// was not looking at, and that the next successful action wiped without a
/// trace. Failures belong where every other operational message in this client
/// already collects.
///
/// `add_required`, not `add`: a launch that did not happen is not an event
/// alert, and turning match and chat notifications off must not silence the one
/// message explaining why nothing opened.
///
/// The reason is the client's own (a download, an install, the game's
/// process), so it is marked for the UI to word plainly; see
/// `notifications::add_failure`.
fn fail(out: &EventSink, reason: String) {
    notifications::add_required_failure(
        out,
        notifications::Text::new("notifications.msg.replayFailed"),
        "Replay failed",
        reason.clone(),
        None,
    );
    out.emit(ReplayEvent::Failed { reason });
}

/// Report a live replay that was refused before it became a launch: still
/// inside the anti-ghosting delay, or a game whose start is unknown.
///
/// Such a refusal owns no launch, so while another replay is starting it only
/// says why, and leaves the status to that launch. As `fail` alone it set the
/// status to `Failed` and closed the starting dialog of a replay that was
/// still on its way. Checked under the launch lock, so a launch cannot begin
/// or settle between the check and the event.
///
/// `text` is the catalogue entry the notice is shown in, and `action` is what
/// the notice offers instead, such as starting the replay once the delay is
/// over (#653).
fn refuse(
    ctx: &ServiceCtx,
    out: &EventSink,
    text: notifications::Text,
    reason: String,
    action: Option<NotificationAction>,
) {
    let slot = ctx.replays.launch_slot();
    notifications::add_required_text(
        out,
        NotificationKind::Error,
        text,
        "Replay failed",
        reason.clone(),
        action,
    );
    if slot.cancellation.is_none() {
        out.emit(ReplayEvent::Failed { reason });
    }
}

pub(crate) fn cancel_live_tracking(out: &EventSink) {
    if out.with_state(|state| state.replays.live_tracking.is_some()) {
        out.emit(ReplayEvent::LiveTrackingCleared);
    }
}

async fn watch_live(target: LiveReplayTarget, ctx: &ServiceCtx, out: &EventSink) {
    // Anti-ghosting, enforced here rather than only on the button: the Watch
    // button, notification action, scheduled auto-watch and Discord spectate
    // links all converge on this function.
    let (waiting, player) = out.with_state(|state| {
        let launched_at = state
            .lobby
            .live_games
            .iter()
            .find(|game| game.id == target.uid)
            .and_then(|game| game.launched_at);
        let waiting = live_replay_delay_remaining(launched_at, crate::services::now_seconds());
        let player = state
            .auth
            .player
            .as_ref()
            .map(|player| player.name.clone())
            .unwrap_or_else(|| "spectator".to_string());
        (waiting, player)
    });
    if waiting > 0 {
        let reason = format!(
            "Live replays are delayed by five minutes so nobody can watch \
             an ongoing game for an advantage. Try again in {}.",
            describe(waiting)
        );
        refuse(
            ctx,
            out,
            notifications::Text::new("notifications.msg.liveReplayDelayed")
                .with("minutes", waiting / 60)
                .with("seconds", waiting % 60),
            reason,
            Some(NotificationAction::WatchLiveWhenReady { target }),
        );
        return;
    }

    let Some(ticket) = begin_launch(ctx, out, LaunchTarget::Live(target.uid)) else {
        return;
    };
    out.emit(ReplayEvent::Connecting {
        launch: ReplayLaunch::Live {
            target: target.clone(),
        },
    });
    let uid = target.uid;
    launch(
        ticket,
        ctx.ports.replay_playback.watch_live(target, player),
        Some(uid),
        ctx,
        out,
    )
    .await;
}

pub async fn handle(cmd: ReplayCommand, ctx: &ServiceCtx, out: &EventSink) {
    match cmd {
        ReplayCommand::WatchLive(target) => {
            cancel_live_tracking(out);
            watch_live(target, ctx, out).await;
        }
        ReplayCommand::TrackLive { target, action } => {
            let now = crate::services::now_seconds();
            let game = out.with_state(|state| {
                state
                    .lobby
                    .live_games
                    .iter()
                    .find(|game| game.id == target.uid)
                    .map(|game| (game.title.clone(), game.launched_at))
            });
            let Some((title, Some(launched_at))) = game else {
                refuse(
                    ctx,
                    out,
                    notifications::Text::new("notifications.msg.replayFailed")
                        .with("reason", "That live game no longer has a known start time."),
                    "That live game no longer has a known start time.".into(),
                    None,
                );
                return;
            };
            let waiting = live_replay_delay_remaining(Some(launched_at), now);
            let tracking = LiveReplayTracking {
                target,
                title,
                action,
                ready_at: now.saturating_add(waiting),
            };
            out.emit(ReplayEvent::LiveTrackingScheduled {
                tracking: tracking.clone(),
            });

            if waiting > 0 {
                tokio::time::sleep(Duration::from_secs(u64::from(waiting))).await;
            }
            if !out.with_state(|state| state.replays.live_tracking.as_ref() == Some(&tracking)) {
                return;
            }
            out.emit(ReplayEvent::LiveTrackingCleared);

            match action {
                // A friend's game may have been announced as watchable at this
                // very second; one offer to watch it is enough.
                LiveReplayTrackingAction::Notify
                    if out.with_state(|state| {
                        notifications::announces_watch(state, tracking.target.uid)
                    }) => {}
                LiveReplayTrackingAction::Notify => notifications::add_text(
                    out,
                    NotificationKind::ReplayAvailable,
                    notifications::Text::new("notifications.msg.liveReplayReady")
                        .with("title", &tracking.title),
                    "Live replay ready",
                    format!("{} is now available to watch.", tracking.title),
                    Some(NotificationAction::WatchLive {
                        target: tracking.target,
                    }),
                ),
                LiveReplayTrackingAction::Watch => {
                    notifications::add_text(
                        out,
                        NotificationKind::ReplayAvailable,
                        notifications::Text::new("notifications.msg.liveReplayLaunching")
                            .with("title", &tracking.title),
                        "Live replay ready",
                        format!("Launching {} now.", tracking.title),
                        None,
                    );
                    watch_live(tracking.target, ctx, out).await;
                }
            }
        }
        ReplayCommand::CancelLiveTracking => out.emit(ReplayEvent::LiveTrackingCleared),
        ReplayCommand::OpenFile { path } => {
            out.emit(NavEvent::TabSelected { tab: Tab::Replays });
            cancel_live_tracking(out);
            let Some(ticket) = begin_launch(ctx, out, LaunchTarget::File(path.clone())) else {
                return;
            };
            out.emit(ReplayEvent::Connecting {
                launch: ReplayLaunch::File { path: path.clone() },
            });
            launch(
                ticket,
                ctx.ports.replay_playback.play_file(PathBuf::from(path)),
                None,
                ctx,
                out,
            )
            .await;
        }
        ReplayCommand::CancelWatch => {
            if let Some(token) = ctx.replays.launch_slot().cancellation.take() {
                token.cancel();
            }
        }
        ReplayCommand::SearchVault { query } => {
            let generation = ctx.replays.vault_generation.begin();
            out.emit(ReplayEvent::VaultLoading);
            let result = ctx.ports.replay_vault.search_vault((*query).clone()).await;
            if !ctx.replays.vault_generation.is_current(generation) {
                return;
            }
            match result {
                Ok(mut search) => {
                    // A full page is direct evidence that another one exists,
                    // and it outranks the reported count. The API's totals for
                    // a table this size can be capped or estimated, and
                    // deriving `has_more` from the count alone meant a capped
                    // total stranded the user on its last page with a dead
                    // Next button.
                    let full_page = search.replays.len() as u32 >= query.page_size;
                    // A game still being played has a record and no end yet.
                    // It belongs to the live tab, where it can be watched;
                    // here it was a replay that did not exist (#399). Counted
                    // for `full_page` above first, so dropping it never makes
                    // a page look like the last one.
                    search.replays.retain(|replay| !replay.end_time.is_empty());
                    let has_more = full_page
                        || search
                            .total_pages
                            .is_some_and(|pages| query.page < pages as u32);
                    out.emit(ReplayEvent::VaultLoaded {
                        replays: search.replays,
                        query,
                        has_more,
                        total_pages: search.total_pages,
                        total_records: search.total_records,
                    })
                }
                Err(reason) => out.emit(ReplayEvent::VaultLoadFailed { reason }),
            }
        }
        ReplayCommand::LoadRecentMatchmaker => {
            let Some(login) =
                out.with_state(|state| state.auth.player.as_ref().map(|p| p.name.clone()))
            else {
                return;
            };
            out.emit(ReplayEvent::RecentMatchmakerLoading);
            match ctx
                .ports
                .replay_vault
                .search_vault(recent_matchmaker_query(login))
                .await
            {
                Ok(search) => out.emit(ReplayEvent::RecentMatchmakerLoaded {
                    replays: search.replays,
                }),
                Err(reason) => out.emit(ReplayEvent::RecentMatchmakerFailed { reason }),
            }
        }
        ReplayCommand::LoadFeaturedMods => {
            // Best-effort: the filter falls back to a free-choice "any" when
            // the list can't be fetched, so a failure here isn't worth a
            // user-visible error of its own.
            if let Ok(mods) = ctx.ports.replay_vault.list_featured_mods().await {
                out.emit(ReplayEvent::FeaturedModsLoaded { mods });
            }
        }
        ReplayCommand::WatchVault { uid } => {
            cancel_live_tracking(out);
            let Some(ticket) = begin_launch(ctx, out, LaunchTarget::Vault(uid)) else {
                return;
            };
            out.emit(ReplayEvent::Connecting {
                launch: ReplayLaunch::Vault { uid },
            });
            // Watching a vault replay downloads it before launching FA. That
            // download is the launch's first preparation step now, narrated
            // with the others (and measured) on the starting dialog and the
            // status bar, and stopped by `CancelWatch` with them. It used to
            // set the library download's status, which a watch could not be
            // told apart from, and whose end then cleared a real library
            // download running beside it.
            launch(
                ticket,
                ctx.ports.replay_playback.watch_vault(uid),
                Some(uid),
                ctx,
                out,
            )
            .await;
        }
        ReplayCommand::DownloadVault { uid } => {
            // Reachable before it is on screen, so the status bar's cancel
            // never arrives before there is anything to stop.
            let ticket = ctx.replays.downloads.begin(uid);
            out.emit(ReplayEvent::VaultDownloadStarted { uid });
            let progress = {
                let sink = out.clone();
                let report = crate::services::whole_percent_reporter(move |progress| {
                    sink.emit(ReplayEvent::VaultDownloadProgressed { uid, progress })
                });
                std::sync::Arc::new(move |received: u64, total: Option<u64>| {
                    report(crate::services::percent_of(received, total))
                })
            };
            // Dropping the port's future is the cancel: the transfer stops,
            // and a file being written is not published (see
            // `ReplayVaultPort::download_vault_reporting`).
            let downloading = ctx
                .ports
                .replay_vault
                .download_vault_reporting(uid, progress);
            let result = tokio::select! {
                result = downloading => Some(result),
                () = ticket.called_off.cancelled() => None,
            };
            ctx.replays.downloads.end(&ticket);
            match result {
                // Not a failure, and nothing reached the library.
                None => out.emit(ReplayEvent::VaultDownloadCancelled { uid }),
                Some(Ok(replay)) => out.emit(ReplayEvent::VaultDownloaded { uid, replay }),
                Some(Err(reason)) => out.emit(ReplayEvent::VaultDownloadFailed { uid, reason }),
            }
        }
        ReplayCommand::CancelDownload { uid } => {
            ctx.replays.downloads.cancel(|running| *running == uid);
        }
        ReplayCommand::LoadLocal { limit } => {
            let generation = ctx.replays.local_generation.begin();
            out.emit(ReplayEvent::LocalLoading);
            let result = ctx.ports.replay_library.list_local(limit as usize).await;
            if !ctx.replays.local_generation.is_current(generation) {
                return;
            }
            match result {
                Ok(replays) => out.emit(ReplayEvent::LocalLoaded { replays }),
                Err(reason) => out.emit(ReplayEvent::LocalLoadFailed { reason }),
            }
        }
        ReplayCommand::DeleteLocal { path } => {
            // Prevent an older directory scan from restoring the deleted row.
            ctx.replays.local_generation.invalidate();
            match ctx
                .ports
                .replay_library
                .delete_local(PathBuf::from(&path))
                .await
            {
                Ok(()) => out.emit(ReplayEvent::LocalDeleted { path }),
                Err(reason) => out.emit(ReplayEvent::LocalLoadFailed { reason }),
            }
        }
        // Both reads are named by `replay_read_key`, not by the game id
        // alone: every file whose header carries no game id is uid 0, and
        // keyed by that, two of them shared one answer.
        ReplayCommand::LoadDetails {
            uid,
            local_path,
            request_id,
        } => {
            run_details(
                begin_details(uid, local_path, request_id, ctx, out),
                ctx,
                out,
            )
            .await;
        }
        ReplayCommand::LoadAnalysis {
            uid,
            local_path,
            request_id,
        } => {
            if let Some(work) = begin_analysis(uid, local_path, request_id, ctx, out) {
                run_analysis(work, ctx, out).await;
            }
        }
        ReplayCommand::CancelReads {
            uid,
            local_path,
            request_id,
        } => cancel_reads(
            &faf_domain::state::replay_read_key(uid, local_path.as_deref()),
            request_id,
            ctx,
            out,
        ),
        ReplayCommand::ResolveMaps { uids } => {
            // One small ranged download per game, run a few at a time. In
            // sequence a page of them would still be arriving after the reader
            // had moved on; all at once would be a burst at somebody else's
            // vault for a cosmetic detail. The cap is on the command as well,
            // so a page that grows never turns into a flood.
            let mut maps = Vec::with_capacity(uids.len().min(MAX_MAPS_PER_RESOLVE));
            for chunk in uids
                .iter()
                .take(MAX_MAPS_PER_RESOLVE)
                .collect::<Vec<_>>()
                .chunks(RESOLVE_MAPS_AT_ONCE)
            {
                let lookups = chunk.iter().map(|uid| async {
                    // A failure is recorded as "no map", not retried: the view
                    // asks once per game, and a row that keeps asking on every
                    // render is worse than a row that says it does not know.
                    let map = ctx
                        .ports
                        .replay_vault
                        .replay_map_name(**uid)
                        .await
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    ResolvedReplayMap { uid: **uid, map }
                });
                maps.extend(futures_util::future::join_all(lookups).await);
            }
            if !maps.is_empty() {
                out.emit(ReplayEvent::MapsResolved { maps });
            }
        }
        ReplayCommand::ResolveSimMods { uid } => {
            // A few kilobytes of the file rather than the file. A failure
            // emits nothing, so the details ask again the next time they open.
            match ctx.ports.replay_vault.replay_sim_mods(uid).await {
                Ok(mods) => out.emit(ReplayEvent::SimModsResolved { uid, mods }),
                Err(error) => tracing::debug!(uid, %error, "could not read a replay's mod list"),
            }
        }
        ReplayCommand::LookUpOnline { uid } => {
            // A single-id search rather than a port method of its own: the
            // vault query already has a `replay_id` field, and this way the
            // lookup goes down exactly the code path the search tab uses.
            // The result lands in `online_lookups`, never in `vault`, so
            // opening a local replay does not replace what the Online tab is
            // showing.
            out.emit(ReplayEvent::OnlineLookupStarted { uid });
            let query = ReplayQuery {
                replay_id: uid.to_string(),
                page_size: 1,
                ..ReplayQuery::default()
            };
            match ctx.ports.replay_vault.search_vault(query).await {
                Ok(search) => out.emit(ReplayEvent::OnlineLookupFinished {
                    uid,
                    replay: search
                        .replays
                        .into_iter()
                        .find(|replay| replay.uid == uid)
                        .map(Box::new),
                }),
                Err(reason) => out.emit(ReplayEvent::OnlineLookupFailed { uid, reason }),
            }
        }
        ReplayCommand::LookUpOnlineMany { uids } => look_up_many(uids, ctx, out).await,
    }
}

/// How many game ids one request asks about.
///
/// The API rewrites anything above 100 down to its default page size without
/// saying so, and each row here drags a dozen `playerStats` and their players
/// along with it: a page of twenty-five is a document worth reading in one go
/// rather than a megabyte of JSON for a list somebody is scrolling past.
const LOOKUP_BATCH: usize = 25;

/// Ask the vault about a set of game ids, in batches.
///
/// Every id is marked as being looked up first, so a second call about the
/// same games while this one is in flight finds them already claimed. An id
/// the vault does not answer for is recorded as missing rather than left
/// pending: a running game the API has not written a row for yet is a result,
/// and asking again every time the list refreshes is not.
///
/// The claim is checked here, too, not only promised: ids already claimed or
/// answered are dropped before anything is asked. A burst of overlapping
/// calls used to look every game up as many times as it was asked about.
async fn look_up_many(uids: Vec<i32>, ctx: &ServiceCtx, out: &EventSink) {
    let uids: Vec<i32> = out.with_state(|state| {
        uids.into_iter()
            .filter(|uid| !state.replays.online_lookups.contains_key(uid))
            .collect()
    });
    if uids.is_empty() {
        return;
    }
    for uid in &uids {
        out.emit(ReplayEvent::OnlineLookupStarted { uid: *uid });
    }
    for chunk in uids.chunks(LOOKUP_BATCH) {
        let query = ReplayQuery {
            replay_ids: chunk.iter().map(i32::to_string).collect(),
            page_size: chunk.len() as u32,
            ..ReplayQuery::default()
        };
        match ctx.ports.replay_vault.search_vault(query).await {
            Ok(search) => {
                let mut found: HashMap<i32, VaultReplay> = search
                    .replays
                    .into_iter()
                    .map(|replay| (replay.uid, replay))
                    .collect();
                for uid in chunk {
                    out.emit(ReplayEvent::OnlineLookupFinished {
                        uid: *uid,
                        replay: found.remove(uid).map(Box::new),
                    });
                }
            }
            Err(reason) => {
                for uid in chunk {
                    out.emit(ReplayEvent::OnlineLookupFailed {
                        uid: *uid,
                        reason: reason.clone(),
                    });
                }
            }
        }
    }
}
/// The matchmaker leaderboards, by the technical names a replay's rating
/// changes carry. `global` is left out: that is every custom game.
const MATCHMAKER_LEADERBOARDS: [&str; 4] =
    ["ladder_1v1", "tmm_2v2", "tmm_3v3", "tmm_4v4_full_share"];

/// How many games the matchmaker tab lists.
const RECENT_MATCHMAKER_GAMES: u32 = 10;

/// The player's own latest matchmaker games, newest first (#301).
///
/// An exact login rather than a part of one, so another player whose name
/// contains this one is not mixed in.
fn recent_matchmaker_query(login: String) -> ReplayQuery {
    ReplayQuery {
        player: login,
        exact_player: true,
        leaderboards: MATCHMAKER_LEADERBOARDS
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        page_size: RECENT_MATCHMAKER_GAMES,
        ..ReplayQuery::default()
    }
}
