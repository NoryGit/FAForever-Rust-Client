//! The runtime loop: command in → service → event out → reduce → broadcast.
//!
//! This is the closed unidirectional loop from ARCHITECTURE.md §1/§3.5. It owns the
//! authoritative [`AppState`] and is the only thing that calls [`faf_domain::reduce`].
//!
//! [`App::new`] returns a handle plus an [`AppLoop`]; the caller decides how to drive
//! it ([`tokio::spawn`] in tests, `tauri::async_runtime::spawn` in the shell). This
//! keeps the runtime free of any hard dependency on a particular executor.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};

use faf_domain::{AppCommand, AppEvent, AppState};
use serde::Serialize;
use tokio::sync::{broadcast, mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

use crate::ports::Ports;
use crate::services;

mod cancellation;
mod census;
mod command_policy;
mod policies;
pub use cancellation::CancellationSlot;
pub(crate) use command_policy::{end_turn, expect_admitted, Key};
use command_policy::{CommandAdmission, Lane};
pub use policies::{
    AutoReconnect, LatestRequest, LoadedFromDisk, LobbyOperation, LobbyOperations, RunningGame,
    SerialMutation, SingleFlight,
};

/// Context handed to every service: shared dependencies plus one operational
/// context per domain.
///
/// Holds the [`Ports`] bundle (network, fs, process, auth...) injected at
/// startup. Each service owns its context and that context's fields, which are
/// private to the service's module, so only the owner can touch its request
/// generations, locks and connection guards. Anything another service needs is
/// a named method on the owner's context, so a cross-domain dependency is
/// visible at the call.
pub struct ServiceCtx {
    pub backend_version: String,
    pub ports: Ports,
    /// Which commands may run together, enforced. Here rather than in the
    /// runtime loop alone so a service starting another service's command
    /// goes through it as well: see [`run_command`].
    pub(crate) admission: CommandAdmission,
    pub lobby: services::lobby::LobbyContext,
    pub chat: services::chat::ChatContext,
    pub settings: services::settings::SettingsContext,
    pub auth: services::auth::AuthContext,
    pub player_card: services::player_card::PlayerCardContext,
    pub leaderboard: services::leaderboard::LeaderboardContext,
    pub coop: services::coop::CoopContext,
    pub replays: services::replays::ReplaysContext,
    pub tourney: services::tourney::TourneyContext,
    pub reviews: services::reviews::ReviewsContext,
    pub reporting: services::reporting::ReportingContext,
    pub changelog: services::changelog::ChangelogContext,
    pub guides: services::guides::GuidesContext,
    pub training: services::training::TrainingContext,
    pub maps: services::maps::MapsContext,
    pub mods: services::mods::ModsContext,
    pub clan: services::clan::ClanContext,
    pub tutorials: services::tutorials::TutorialsContext,
    pub galactic_war: services::galactic_war::GalacticWarContext,
    pub map_generator: services::map_generator::MapGeneratorContext,
}

/// The sink a service emits events into.
///
/// `emit` is the single chokepoint where state changes: it reduces the event into
/// the authoritative state and then broadcasts the *same* event to subscribers
/// (the Tauri shell, which forwards it to the frontend).
#[derive(Clone)]
pub struct EventSink {
    state: Arc<RwLock<AppState>>,
    tx: broadcast::Sender<AppEvent>,
    versioned_tx: broadcast::Sender<VersionedEvent>,
    revision: Arc<AtomicU64>,
    /// Serialises delivery, so that revision N is on both channels before
    /// N+1 is handed out. Held by [`EventSink::emit`] across the whole
    /// operation; never taken by a reader. See the note on `emit`.
    send_order: Arc<Mutex<()>>,
}

/// One state delta with the exact authoritative-state revision it produced.
///
/// The ordinary service event stream intentionally stays as [`AppEvent`]. The
/// shell uses this versioned stream to hydrate a webview without either
/// replaying an event already present in its snapshot or dropping an event
/// that raced the snapshot IPC response.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionedEvent {
    pub revision: u64,
    pub event: AppEvent,
}

/// An authoritative state snapshot and the last event revision it contains.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionedSnapshot {
    pub revision: u64,
    pub state: AppState,
}

/// A reduction this slow, under the write lock, is worth a log line.
const SLOW_REDUCE: std::time::Duration = std::time::Duration::from_millis(25);

impl EventSink {
    /// Reduce an event into the authoritative state and broadcast it.
    ///
    /// Two locks, each held for exactly what it protects.
    ///
    /// `send_order` is taken first and held across the whole operation. It is
    /// what keeps revisions in order: without it two concurrent emitters can
    /// interleave and deliver N+1 before N, and the frontend mirror
    /// (`ui/src/ipc/revisionedMirror.ts`) reads any revision gap as corruption
    /// and asks for a fresh snapshot. A snapshot is a few megabytes: the map
    /// vault alone measures ~3.6 MiB of JSON at a realistic 5000-entry
    /// catalogue. No reader ever takes this lock, so holding it costs them
    /// nothing.
    ///
    /// The state write guard is held only across `reduce` and the revision
    /// bump, which is the shortest window that still leaves the two consistent
    /// for [`Self::versioned_snapshot`]: a reader must never see state that has
    /// already absorbed event N while being told the newest revision is N-1,
    /// or it would apply N a second time. Broadcasting happens after that guard
    /// is dropped, so a `with_state` reader is no longer blocked behind two
    /// channel sends. That was the review's point, and this is the version of
    /// it that does not reorder revisions.
    pub fn emit(&self, event: impl Into<AppEvent>) {
        let event = event.into();
        let _delivery = self
            .send_order
            .lock()
            .expect("event delivery lock poisoned");
        let started = std::time::Instant::now();
        let revision = {
            let mut guard = self.state.write().expect("app state lock poisoned");
            faf_domain::reduce(&mut guard, &event);
            self.revision
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                .wrapping_add(1)
        };
        // Every reader and every other emitter waits for this, so a reducer
        // that has become expensive at a populated catalogue should say so.
        let reduced = started.elapsed();
        if reduced >= SLOW_REDUCE {
            tracing::warn!(
                event = %variant_name(&event),
                milliseconds = reduced.as_millis() as u64,
                "reducing an event held the state write lock this long"
            );
        }
        // Err only means "no subscribers yet": fine to ignore. The clone is
        // skipped when nobody is listening on the plain stream, because some
        // events carry the whole player directory and this would otherwise
        // deep copy it for a channel with no receiver.
        if self.tx.receiver_count() > 0 {
            let _ = self.tx.send(event.clone());
        }
        let _ = self.versioned_tx.send(VersionedEvent { revision, event });
    }

    /// A snapshot of the authoritative state, for a test that wants to read
    /// the whole thing back after an `emit`.
    ///
    /// Not for services: every one of them uses [`Self::with_state`], which
    /// copies out the one slice it needs instead of cloning a state whose map
    /// catalogue alone is megabytes. The doc here used to point at "IPC
    /// hydration boundaries", and that boundary goes through
    /// `App::versioned_snapshot`, not through the sink.
    #[cfg(test)]
    pub fn snapshot(&self) -> AppState {
        self.state.read().expect("app state lock poisoned").clone()
    }

    /// Read a projection of the authoritative state without cloning unrelated
    /// slices. The closure executes while the read lock is held, so callers
    /// must copy out what they need and must not block or perform IO inside it.
    ///
    /// Prefer this for service decisions and persistence of a single slice;
    /// [`Self::snapshot`] remains appropriate at IPC hydration boundaries.
    pub fn with_state<T>(&self, read: impl FnOnce(&AppState) -> T) -> T {
        let state = self.state.read().expect("app state lock poisoned");
        read(&state)
    }

    /// Observe the same event stream the shell forwards to the frontend.
    ///
    /// For the rare service that is driven by state rather than by a command,
    /// Discord Rich Presence is one: nothing *asks* for a status update, it is
    /// a consequence of joining or leaving a game. Read-only, like
    /// [`Self::snapshot`]: an observer reacts, and any state change it causes
    /// still goes back through [`Self::emit`].
    pub fn subscribe(&self) -> broadcast::Receiver<AppEvent> {
        self.tx.subscribe()
    }
}

/// Handle to the application core. Created once, shared (behind `Arc`) by the shell.
pub struct App {
    state: Arc<RwLock<AppState>>,
    cmd_tx: mpsc::Sender<QueuedCommand>,
    /// Commands that call work off, kept apart so they never queue behind it.
    /// See [`is_urgent`].
    urgent_tx: mpsc::Sender<QueuedCommand>,
    /// See [`ReleaseOrder`].
    order: Arc<ReleaseOrder>,
    event_tx: broadcast::Sender<AppEvent>,
    versioned_event_tx: broadcast::Sender<VersionedEvent>,
    revision: Arc<AtomicU64>,
}

/// The command-processing loop. Spawn `run()` on any async runtime.
pub struct AppLoop {
    cmd_rx: mpsc::Receiver<QueuedCommand>,
    urgent_rx: mpsc::Receiver<QueuedCommand>,
    order: Arc<ReleaseOrder>,
    ctx: ServiceCtx,
    sink: EventSink,
}

struct QueuedCommand {
    command: AppCommand,
    completion: Option<oneshot::Sender<()>>,
    /// When it was dispatched, from [`ReleaseOrder::stamp`].
    seq: u64,
    /// When it entered the queue, for the late-start warning in
    /// [`spawn_command`].
    queued_at: std::time::Instant,
}

/// Which half of a start/stop pair a command is, and what the pair acts on.
#[derive(Debug, PartialEq, Eq)]
enum PairHalf {
    Start(String),
    Release(String),
}

/// The pair a command belongs to, if any.
///
/// Releases travel in the urgent queue (see [`is_urgent`]) and starts in the
/// ordinary one, so a release can overtake a start that was sent before it.
/// Without this, "Play" then "Stop" under load ran the stop first and the
/// start last, leaving the player queued after they had pressed Stop. The key
/// names what both halves act on, so only a release of the same thing counts.
fn pair_of(command: &AppCommand) -> Option<PairHalf> {
    use faf_domain::state::{
        AuthCommand, ChatCommand, GuidesCommand, LobbyCommand, MapGeneratorCommand, ReplayCommand,
    };
    let start = |key: &str| Some(PairHalf::Start(key.to_owned()));
    let release = |key: &str| Some(PairHalf::Release(key.to_owned()));
    match command {
        AppCommand::Lobby(LobbyCommand::Join { .. } | LobbyCommand::Host { .. }) => start("join"),
        AppCommand::Lobby(LobbyCommand::CancelJoin | LobbyCommand::DeclineModReplacement) => {
            release("join")
        }
        AppCommand::Lobby(LobbyCommand::Connect) => start("lobby"),
        AppCommand::Lobby(LobbyCommand::Disconnect) => release("lobby"),
        AppCommand::Lobby(LobbyCommand::Matchmake {
            queue_name,
            start: true,
        }) => Some(PairHalf::Start(format!("matchmake:{queue_name}"))),
        AppCommand::Lobby(LobbyCommand::Matchmake {
            queue_name,
            start: false,
        }) => Some(PairHalf::Release(format!("matchmake:{queue_name}"))),
        AppCommand::Chat(ChatCommand::Connect { .. }) => start("chat"),
        AppCommand::Chat(ChatCommand::Disconnect) => release("chat"),
        AppCommand::Auth(
            AuthCommand::Login { .. } | AuthCommand::LoginTest | AuthCommand::Restore,
        ) => start("auth"),
        AppCommand::Auth(
            AuthCommand::CancelLogin | AuthCommand::Logout | AuthCommand::LogoutTest,
        ) => release("auth"),
        AppCommand::Guides(GuidesCommand::SignIn) => start("guides"),
        AppCommand::Guides(GuidesCommand::CancelSignIn) => release("guides"),
        AppCommand::MapGenerator(
            MapGeneratorCommand::Generate { .. } | MapGeneratorCommand::GenerateNamed { .. },
        ) => start("map-generator"),
        AppCommand::MapGenerator(MapGeneratorCommand::Cancel) => release("map-generator"),
        AppCommand::Replays(
            ReplayCommand::WatchVault { .. }
            | ReplayCommand::WatchLive { .. }
            | ReplayCommand::OpenFile { .. },
        ) => start("replay-watch"),
        AppCommand::Replays(ReplayCommand::CancelWatch) => release("replay-watch"),
        AppCommand::Replays(ReplayCommand::TrackLive { .. }) => start("live-tracking"),
        AppCommand::Replays(ReplayCommand::CancelLiveTracking) => release("live-tracking"),
        _ => None,
    }
}

/// Dispatch order, so a start that a later release overtook is dropped.
///
/// Every command is stamped as it is dispatched; a release also records its
/// stamp against its pair. When a start finally leaves the ordinary queue, a
/// release of the same pair stamped after it means the user called it off
/// before it ever ran, and running it now would undo that.
#[derive(Debug, Default)]
struct ReleaseOrder {
    next: AtomicU64,
    released: std::sync::Mutex<std::collections::HashMap<String, u64>>,
}

impl ReleaseOrder {
    fn stamp(&self, command: &AppCommand) -> u64 {
        let seq = self.next.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1;
        if let Some(PairHalf::Release(key)) = pair_of(command) {
            self.released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key, seq);
        }
        seq
    }

    fn superseded(&self, command: &AppCommand, seq: u64) -> bool {
        let Some(PairHalf::Start(key)) = pair_of(command) else {
            return false;
        };
        self.released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .is_some_and(|&released| released > seq)
    }
}

impl App {
    /// Construct the core and its loop. The caller spawns `loop.run()`.
    pub fn new(backend_version: impl Into<String>, ports: Ports) -> (Self, AppLoop) {
        let state = Arc::new(RwLock::new(AppState::default()));
        let (event_tx, _) = broadcast::channel::<AppEvent>(256);
        // Four times the plain stream's room. A receiver that falls behind on
        // this one does not merely miss events: the mirror reads a revision
        // gap as corruption and asks for a whole `AppState` back, which is
        // megabytes of JSON requested exactly when the client is already
        // behind. Lag here is self-feeding, so the cheapest thing to spend on
        // it is queue.
        let (versioned_event_tx, _) = broadcast::channel::<VersionedEvent>(1024);
        let (cmd_tx, cmd_rx) = mpsc::channel::<QueuedCommand>(COMMAND_QUEUE);
        let (urgent_tx, urgent_rx) = mpsc::channel::<QueuedCommand>(URGENT_QUEUE);
        let revision = Arc::new(AtomicU64::new(0));
        let order = Arc::new(ReleaseOrder::default());
        let send_order = Arc::new(Mutex::new(()));

        let sink = EventSink {
            state: state.clone(),
            tx: event_tx.clone(),
            versioned_tx: versioned_event_tx.clone(),
            revision: revision.clone(),
            send_order: send_order.clone(),
        };
        let ctx = ServiceCtx {
            backend_version: backend_version.into(),
            ports,
            admission: CommandAdmission::default(),
            lobby: services::lobby::LobbyContext::default(),
            chat: services::chat::ChatContext::default(),
            settings: services::settings::SettingsContext::default(),
            auth: services::auth::AuthContext::default(),
            player_card: services::player_card::PlayerCardContext::default(),
            leaderboard: services::leaderboard::LeaderboardContext::default(),
            coop: services::coop::CoopContext::default(),
            replays: services::replays::ReplaysContext::default(),
            tourney: services::tourney::TourneyContext::default(),
            reviews: services::reviews::ReviewsContext::default(),
            reporting: services::reporting::ReportingContext::default(),
            changelog: services::changelog::ChangelogContext::default(),
            guides: services::guides::GuidesContext::default(),
            training: services::training::TrainingContext::default(),
            maps: services::maps::MapsContext::default(),
            mods: services::mods::ModsContext::default(),
            clan: services::clan::ClanContext::default(),
            tutorials: services::tutorials::TutorialsContext::default(),
            galactic_war: services::galactic_war::GalacticWarContext::default(),
            map_generator: services::map_generator::MapGeneratorContext::default(),
        };

        let app = Self {
            state,
            cmd_tx,
            urgent_tx,
            order: order.clone(),
            event_tx,
            versioned_event_tx,
            revision,
        };
        let app_loop = AppLoop {
            cmd_rx,
            urgent_rx,
            order,
            ctx,
            sink,
        };
        (app, app_loop)
    }

    /// Send a command into the loop, applying backpressure when the bounded
    /// queue is busy and reporting a stopped runtime to the caller.
    pub async fn dispatch(&self, cmd: AppCommand) -> Result<(), String> {
        self.queue_for(&cmd)
            .send(QueuedCommand {
                queued_at: std::time::Instant::now(),
                seq: self.order.stamp(&cmd),
                command: cmd,
                completion: None,
            })
            .await
            .map_err(|_| "application command loop is not running".to_string())
    }

    /// Execute a command and wait until its service effect has completed.
    ///
    /// Normal UI commands use [`Self::dispatch`] and remain asynchronous. This
    /// stronger boundary is reserved for startup dependencies such as loading
    /// persisted settings before announcing backend readiness.
    pub async fn dispatch_and_wait(&self, cmd: AppCommand) -> Result<(), String> {
        let (completion, finished) = oneshot::channel();
        self.queue_for(&cmd)
            .send(QueuedCommand {
                queued_at: std::time::Instant::now(),
                seq: self.order.stamp(&cmd),
                command: cmd,
                completion: Some(completion),
            })
            .await
            .map_err(|_| "application command loop is not running".to_string())?;
        finished
            .await
            .map_err(|_| "application command task stopped before completion".to_string())
    }

    /// Send a command without awaiting (for sync call sites like Tauri commands).
    pub fn try_dispatch(&self, cmd: AppCommand) -> Result<(), String> {
        self.queue_for(&cmd)
            .try_send(QueuedCommand {
                queued_at: std::time::Instant::now(),
                seq: self.order.stamp(&cmd),
                command: cmd,
                completion: None,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    "application command queue is full".to_string()
                }
                mpsc::error::TrySendError::Closed(_) => {
                    "application command loop is not running".to_string()
                }
            })
    }

    /// The queue a command waits in. Cancellations get their own, so a
    /// saturated ordinary queue cannot hold back the command meant to relieve
    /// it.
    fn queue_for(&self, cmd: &AppCommand) -> &mpsc::Sender<QueuedCommand> {
        if is_urgent(cmd) {
            &self.urgent_tx
        } else {
            &self.cmd_tx
        }
    }

    /// Subscribe to the event stream (the Tauri shell forwards this to the frontend).
    pub fn subscribe(&self) -> broadcast::Receiver<AppEvent> {
        self.event_tx.subscribe()
    }

    /// Atomically subscribe at the event-stream tail and clone the state at
    /// that exact boundary. Events represented by the snapshot precede the
    /// receiver; every later event is queued for it.
    ///
    /// The unversioned twin of [`Self::subscribe_versioned_with_snapshot`],
    /// which is what the shell uses: without a revision the frontend cannot
    /// tell a gap from a quiet moment, so this is kept for the tests that
    /// exercise the subscribe-and-snapshot boundary itself.
    #[cfg(test)]
    pub fn subscribe_with_snapshot(&self) -> (broadcast::Receiver<AppEvent>, AppState) {
        let guard = self.state.read().expect("app state lock poisoned");
        let events = self.event_tx.subscribe();
        let snapshot = guard.clone();
        (events, snapshot)
    }

    /// Atomically subscribe to the shell's revisioned stream and clone the
    /// state at the same boundary. Unlike a plain snapshot followed by a
    /// listener, this protocol is safe when event delivery and IPC responses
    /// are scheduled independently by the webview runtime.
    pub fn subscribe_versioned_with_snapshot(
        &self,
    ) -> (broadcast::Receiver<VersionedEvent>, VersionedSnapshot) {
        let guard = self.state.read().expect("app state lock poisoned");
        let events = self.versioned_event_tx.subscribe();
        let snapshot = VersionedSnapshot {
            revision: self.revision.load(std::sync::atomic::Ordering::Relaxed),
            state: guard.clone(),
        };
        (events, snapshot)
    }

    /// A revisioned snapshot for initial frontend hydration.
    pub fn versioned_snapshot(&self) -> VersionedSnapshot {
        let guard = self.state.read().expect("app state lock poisoned");
        VersionedSnapshot {
            revision: self.revision.load(std::sync::atomic::Ordering::Relaxed),
            state: guard.clone(),
        }
    }

    /// A consistent snapshot of current state (for initial frontend hydration).
    pub fn snapshot(&self) -> AppState {
        self.state.read().expect("app state lock poisoned").clone()
    }

    /// Read one projection of the state without cloning the rest of it.
    ///
    /// The twin of [`EventSink::with_state`], for the shell. Closing the
    /// window used to clone the whole `AppState` to read a single enum out of
    /// `lobby.join`: a few megabytes at a realistic catalogue size, to answer
    /// "is a game running".
    ///
    /// The closure runs under the read lock, so it must copy out what it needs
    /// and must not block or do IO.
    pub fn with_state<T>(&self, read: impl FnOnce(&AppState) -> T) -> T {
        let state = self.state.read().expect("app state lock poisoned");
        read(&state)
    }
}

impl AppLoop {
    /// Drive the loop until all command senders are dropped.
    ///
    /// Each command is handled on its own task so a slow effect (e.g. an
    /// interactive login) never blocks the processing of other commands. Ordering
    /// of *state* changes is still well-defined: every mutation goes through the
    /// single [`EventSink::emit`] chokepoint.
    pub async fn run(self) {
        let ctx = Arc::new(self.ctx);

        // Discord Rich Presence is the one feature no command drives: the
        // status mirrors state, so it observes the event stream instead. It
        // owns its own tasks and never blocks this loop.
        services::discord::spawn(ctx.clone(), self.sink.clone());

        // Likewise state-driven: a socket that dropped while the user is still
        // signed in should come back without them asking.
        services::reconnect::spawn(ctx.clone(), self.sink.clone());

        // And likewise: a calendar reminder is due at a moment, not in answer
        // to anything the user just did.
        services::events::spawn(ctx.clone(), self.sink.clone());

        // And a channel goes live when it goes live. Starts no task at all on a
        // build whose Twitch credentials are absent, which is most of them.
        services::streams::spawn(ctx.clone(), self.sink.clone());

        // And a release is published while the client is running. The check at
        // startup is the one the settings load performs; this is the one that
        // reaches a client nobody has restarted since Friday.
        services::client_update::spawn(ctx.clone(), self.sink.clone());

        // And how big the state has grown, written to the log now and then:
        // see `census`.
        census::spawn(self.sink.clone());

        // And the training hub's recommendations follow what they are read
        // from: a sign-in, a finished replay scan, a game that just ended.
        services::training::spawn(ctx.clone(), self.sink.clone());

        let sink = self.sink.clone();
        // Admitted here, synchronously, as the command leaves its queue: see
        // `CommandAdmission` for why the place is taken before the task starts.
        let handle = move |command: AppCommand| {
            let ctx = ctx.clone();
            let sink = sink.clone();
            let admission = command_policy::policy(&command).admission;
            let turn = ctx.admission.admit(admission);
            async move {
                let Some(mut turn) = turn else {
                    tracing::debug!("dropped a command whose kind is already running");
                    return;
                };
                turn.ready().await;
                command_policy::run_admitted(admission, turn, dispatch(command, &ctx, &sink)).await;
            }
        };
        drive(
            self.cmd_rx,
            self.urgent_rx,
            self.order,
            PRODUCTION_LIMITS,
            handle,
        )
        .await;
    }
}

/// How many ordinary commands may wait in the queue. Once it is full,
/// [`App::dispatch`] waits and [`App::try_dispatch`] reports it.
const COMMAND_QUEUE: usize = 64;

/// The same, for cancellations. Small: these are single clicks.
const URGENT_QUEUE: usize = 16;

/// See [`drive`]. Not a tuning knob: it exists so a runaway dispatcher
/// cannot open a thousand sockets, and is far above any honest workload.
const MAX_CONCURRENT_COMMANDS: usize = 64;

/// Cancellations running at once. Each one only flips a flag or closes a
/// socket, so a handful is plenty; the ceiling exists so that even these
/// cannot pile up without bound.
const MAX_CONCURRENT_URGENT: usize = 8;

/// How much work [`drive`] lets run at once, per queue.
#[derive(Debug, Clone, Copy)]
struct Limits {
    ordinary: usize,
    urgent: usize,
}

const PRODUCTION_LIMITS: Limits = Limits {
    ordinary: MAX_CONCURRENT_COMMANDS,
    urgent: MAX_CONCURRENT_URGENT,
};

/// Whether a command waits in the priority queue. The lane is part of the
/// command's policy: see [`command_policy::Lane`].
fn is_urgent(command: &AppCommand) -> bool {
    command_policy::policy(command).lane == Lane::Priority
}

/// Run commands from both queues until the ordinary one closes.
///
/// A permit is taken *before* a command leaves its queue, not inside the task
/// that runs it. Taking it inside the task bounded how many ran but not how
/// many waited: the loop kept draining the bounded channel into an unbounded
/// pile of spawned tasks, each parked on the semaphore, so the channel's
/// backpressure never reached the caller. Now a saturated pool leaves commands
/// in the channel, the channel fills, and `dispatch` waits.
///
/// Urgent commands are checked first and draw on their own permits, so they
/// are never stuck behind a full ordinary pool or a full ordinary queue.
/// That lets a release overtake its own start, so a start the user has since
/// called off is dropped here rather than run (see [`ReleaseOrder`]).
async fn drive<H, F>(
    mut ordinary: mpsc::Receiver<QueuedCommand>,
    mut urgent: mpsc::Receiver<QueuedCommand>,
    order: Arc<ReleaseOrder>,
    limits: Limits,
    handle: H,
) where
    H: Fn(AppCommand) -> F,
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let ordinary_permits = Arc::new(Semaphore::new(limits.ordinary));
    let urgent_permits = Arc::new(Semaphore::new(limits.urgent));
    let running = Running::default();
    let mut ordinary_permit: Option<OwnedSemaphorePermit> = None;
    let mut urgent_permit: Option<OwnedSemaphorePermit> = None;
    let mut urgent_open = true;

    loop {
        tokio::select! {
            biased;
            acquired = urgent_permits.clone().acquire_owned(),
                if urgent_open && urgent_permit.is_none() =>
            {
                urgent_permit = Some(acquired.expect("the urgent semaphore is never closed"));
            }
            queued = urgent.recv(), if urgent_open && urgent_permit.is_some() => match queued {
                Some(queued) => spawn_command(queued, urgent_permit.take(), &handle, &running),
                None => urgent_open = false,
            },
            acquired = ordinary_permits.clone().acquire_owned(), if ordinary_permit.is_none() => {
                ordinary_permit = Some(acquired.expect("the command semaphore is never closed"));
            }
            queued = ordinary.recv(), if ordinary_permit.is_some() => match queued {
                Some(queued) if order.superseded(&queued.command, queued.seq) => {
                    // Keep the permit for the next command, and still answer
                    // anyone waiting on this one: it is finished, by not running.
                    tracing::debug!(seq = queued.seq, "dropped a start that a later release called off");
                    if let Some(completion) = queued.completion {
                        let _ = completion.send(());
                    }
                }
                Some(queued) => spawn_command(queued, ordinary_permit.take(), &handle, &running),
                None => break,
            },
        }
    }
}

/// How late a command may start before it is logged. A click is a command, so
/// half a second of waiting is already a client that ignored somebody.
const LATE_START: std::time::Duration = std::time::Duration::from_millis(500);

/// The commands running now: a short name and when each started.
///
/// Only read when a command starts late, to say what it waited behind. A tab
/// that would not change for ten seconds left nothing in the log to say why;
/// this names the work that held every slot.
#[derive(Default, Clone)]
struct Running(Arc<std::sync::Mutex<RunningCommands>>);

#[derive(Default)]
struct RunningCommands {
    next: u64,
    commands: std::collections::HashMap<u64, (String, std::time::Instant)>,
}

impl Running {
    fn lock(&self) -> std::sync::MutexGuard<'_, RunningCommands> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn start(&self, name: String) -> u64 {
        let mut running = self.lock();
        running.next += 1;
        let id = running.next;
        running
            .commands
            .insert(id, (name, std::time::Instant::now()));
        id
    }

    fn finish(&self, id: u64) {
        self.lock().commands.remove(&id);
    }

    /// The longest-running few, as "Maps::LoadVault 12.3s".
    fn longest(&self, limit: usize) -> (usize, Vec<String>) {
        let running = self.lock();
        let mut all: Vec<_> = running.commands.values().collect();
        all.sort_by_key(|(_, started)| *started);
        let listed = all
            .iter()
            .take(limit)
            .map(|(name, started)| format!("{name} {:.1}s", started.elapsed().as_secs_f32()))
            .collect();
        (all.len(), listed)
    }
}

/// A command's slice and variant, as "Nav::SelectReplaysSection", without its
/// payload.
fn command_name(command: &AppCommand) -> String {
    variant_name(command)
}

/// A command's or event's slice and variant, as "Lobby::GamesUpdated",
/// without its payload. Read from the `Debug` text, cut off after a few dozen
/// characters so a value carrying a picture or a whole catalogue costs no more
/// to name than one that does not.
fn variant_name(value: &impl std::fmt::Debug) -> String {
    struct Capped(String);
    impl std::fmt::Write for Capped {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let room = 96usize.saturating_sub(self.0.len());
            self.0.extend(text.chars().take(room));
            if self.0.len() >= 96 {
                Err(std::fmt::Error)
            } else {
                Ok(())
            }
        }
    }
    let mut text = Capped(String::new());
    let _ = std::fmt::write(&mut text, format_args!("{value:?}"));
    let text = text.0;
    let (slice, rest) = text.split_once('(').unwrap_or((text.as_str(), ""));
    let variant: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if variant.is_empty() {
        slice.to_string()
    } else {
        format!("{slice}::{variant}")
    }
}

/// Run one command on its own task, holding `permit` until it finishes.
fn spawn_command<H, F>(
    queued: QueuedCommand,
    permit: Option<OwnedSemaphorePermit>,
    handle: &H,
    running: &Running,
) where
    H: Fn(AppCommand) -> F,
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let name = command_name(&queued.command);
    let waited = queued.queued_at.elapsed();
    if waited >= LATE_START {
        let (count, longest) = running.longest(12);
        tracing::warn!(
            command = %name,
            waited_seconds = waited.as_secs_f32(),
            running = count,
            ?longest,
            "a command waited for a free slot before it could start"
        );
    }
    let id = running.start(name);
    let work = handle(queued.command);
    let completion = queued.completion;
    let running = running.clone();
    tokio::spawn(async move {
        let _permit = permit;
        work.await;
        running.finish(id);
        if let Some(completion) = completion {
            let _ = completion.send(());
        }
    });
}

/// Run a command from inside a service, under the same policy as one from the
/// webview: a single-flight command is dropped while its kind runs, and a
/// serial one waits its turn.
///
/// The way for one service to start another's command. Calling the other
/// service's `handle` directly walks past the policy table, which is how the
/// training tab's catalogue load used to start a second crawl beside the Maps
/// tab's own.
///
/// Boxed because it recurses: a command run here may itself run another.
pub(crate) fn run_command<'a>(
    command: AppCommand,
    ctx: &'a ServiceCtx,
    out: &'a EventSink,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    let admission = command_policy::policy(&command).admission;
    let turn = ctx.admission.admit(admission);
    Box::pin(async move {
        let Some(mut turn) = turn else {
            return;
        };
        turn.ready().await;
        command_policy::run_admitted(admission, turn, dispatch(command, ctx, out)).await;
    })
}

/// Route a command to the owning service. One arm per slice (ARCHITECTURE.md §8).
async fn dispatch(cmd: AppCommand, ctx: &ServiceCtx, sink: &EventSink) {
    match cmd {
        AppCommand::Session(c) => services::session::handle(c, ctx, sink).await,
        AppCommand::Auth(c) => services::auth::handle(c, ctx, sink).await,
        AppCommand::Nav(c) => services::nav::handle(c, ctx, sink).await,
        AppCommand::Events(c) => services::events::handle(c, ctx, sink).await,
        AppCommand::Notifications(c) => services::notifications::handle(c, ctx, sink).await,
        AppCommand::Chat(c) => services::chat::handle(c, ctx, sink).await,
        AppCommand::Coop(c) => services::coop::handle(c, ctx, sink).await,
        AppCommand::Lobby(c) => services::lobby::handle(c, ctx, sink).await,
        AppCommand::Replays(c) => services::replays::handle(c, ctx, sink).await,
        AppCommand::Maps(c) => services::maps::handle(c, ctx, sink).await,
        AppCommand::MapGenerator(c) => services::map_generator::handle(c, ctx, sink).await,
        AppCommand::Mods(c) => services::mods::handle(c, ctx, sink).await,
        AppCommand::Leaderboard(c) => services::leaderboard::handle(c, ctx, sink).await,
        AppCommand::PlayerCard(c) => services::player_card::handle(c, ctx, sink).await,
        AppCommand::Reporting(c) => services::reporting::handle(c, ctx, sink).await,
        AppCommand::Clan(c) => services::clan::handle(c, ctx, sink).await,
        AppCommand::Reviews(c) => services::reviews::handle(c, ctx, sink).await,
        AppCommand::Tourney(c) => services::tourney::handle(c, ctx, sink).await,
        AppCommand::Guides(c) => services::guides::handle(c, ctx, sink).await,
        AppCommand::Training(c) => services::training::handle(c, ctx, sink).await,
        AppCommand::Tutorials(c) => services::tutorials::handle(c, ctx, sink).await,
        AppCommand::Changelog(c) => services::changelog::handle(c, ctx, sink).await,
        AppCommand::Uploads(c) => services::uploads::handle(c, ctx, sink).await,
        AppCommand::GalacticWar(c) => services::galactic_war::handle(c, ctx, sink).await,
        AppCommand::ClientUpdate(c) => services::client_update::handle(c, ctx, sink).await,
        AppCommand::Social(c) => services::social::handle(c, ctx, sink).await,
        AppCommand::Streams(c) => services::streams::handle(c, ctx, sink).await,
        AppCommand::Settings(c) => services::settings::handle(c, ctx, sink).await,
    }
}

#[cfg(test)]
mod tests {
    use faf_domain::state::{ConnectionStatus, SessionCommand, SessionEvent};

    use super::*;

    #[tokio::test]
    async fn dispatch_reports_a_stopped_command_loop() {
        let (app, app_loop) = App::new("test", crate::infra::fake_ports());
        drop(app_loop);

        let error = app
            .dispatch(SessionCommand::Hello.into())
            .await
            .expect_err("a dropped receiver must be reported");

        assert!(error.contains("not running"));
    }

    #[test]
    fn try_dispatch_reports_queue_saturation() {
        let (app, _app_loop) = App::new("test", crate::infra::fake_ports());
        for _ in 0..64 {
            app.try_dispatch(SessionCommand::Hello.into())
                .expect("the configured queue capacity should accept this command");
        }

        let error = app
            .try_dispatch(SessionCommand::Hello.into())
            .expect_err("the next command must observe a full queue");

        assert!(error.contains("full"));

        // A cancellation still gets through: it has a queue of its own.
        app.try_dispatch(faf_domain::state::LobbyCommand::CancelJoin.into())
            .expect("a cancellation must not wait behind a full ordinary queue");
    }

    #[test]
    fn a_command_is_named_by_slice_and_variant_without_its_payload() {
        assert_eq!(
            command_name(&faf_domain::state::MapsCommand::LoadVault.into()),
            "Maps::LoadVault"
        );
        let named = command_name(
            &faf_domain::state::LobbyCommand::Join {
                id: 42,
                password: Some("secret".into()),
                replace_mods: false,
            }
            .into(),
        );
        assert_eq!(named, "Lobby::Join");
    }

    #[test]
    fn an_event_is_named_the_same_way() {
        let event: AppEvent = faf_domain::state::MapsEvent::VaultLoading.into();
        assert_eq!(variant_name(&event), "Maps::VaultLoading");
    }

    #[test]
    fn only_the_releasing_half_of_a_command_pair_is_urgent() {
        use faf_domain::state::LobbyCommand;

        assert!(is_urgent(&LobbyCommand::CancelJoin.into()));
        assert!(is_urgent(&LobbyCommand::Disconnect.into()));
        assert!(is_urgent(
            &LobbyCommand::Matchmake {
                queue_name: "ladder1v1".into(),
                start: false,
            }
            .into()
        ));
        assert!(!is_urgent(
            &LobbyCommand::Matchmake {
                queue_name: "ladder1v1".into(),
                start: true,
            }
            .into()
        ));
        assert!(!is_urgent(&SessionCommand::Hello.into()));
        // A tab click never waits behind work it is leaving.
        assert!(is_urgent(
            &faf_domain::state::NavCommand::SelectReplaysSection {
                section: faf_domain::state::ReplaysSection::Online,
            }
            .into()
        ));
    }

    /// Saturated work leaves later commands in the channel instead of piling
    /// up as parked tasks, and a cancellation still runs past it.
    #[tokio::test]
    async fn a_saturated_pool_backs_up_into_the_channel_but_not_over_cancellations() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        use faf_domain::state::LobbyCommand;

        let (ordinary_tx, ordinary_rx) = mpsc::channel::<QueuedCommand>(2);
        let (urgent_tx, urgent_rx) = mpsc::channel::<QueuedCommand>(2);
        let started = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));

        let handle = {
            let started = started.clone();
            let gate = gate.clone();
            move |command: AppCommand| {
                let started = started.clone();
                let gate = gate.clone();
                async move {
                    if !is_urgent(&command) {
                        started.fetch_add(1, Ordering::SeqCst);
                        // Held until the test opens the gate: saturated work.
                        let _ = gate.acquire().await;
                    }
                }
            }
        };
        let limits = Limits {
            ordinary: 2,
            urgent: 1,
        };
        let driver = tokio::spawn(drive(
            ordinary_rx,
            urgent_rx,
            Arc::new(ReleaseOrder::default()),
            limits,
            handle,
        ));

        let queued = |command: AppCommand| QueuedCommand {
            queued_at: std::time::Instant::now(),
            command,
            completion: None,
            seq: 0,
        };
        // Two run, two more wait in the channel, which is then full.
        for _ in 0..4 {
            ordinary_tx
                .send(queued(SessionCommand::Hello.into()))
                .await
                .expect("the loop is running");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            started.load(Ordering::SeqCst),
            2,
            "only the permitted two run"
        );
        assert!(
            matches!(
                ordinary_tx.try_send(queued(SessionCommand::Hello.into())),
                Err(mpsc::error::TrySendError::Full(_))
            ),
            "waiting work stays in the bounded channel"
        );

        let (completion, finished) = oneshot::channel();
        urgent_tx
            .send(QueuedCommand {
                queued_at: std::time::Instant::now(),
                command: LobbyCommand::CancelJoin.into(),
                completion: Some(completion),
                seq: 0,
            })
            .await
            .expect("the loop is running");
        tokio::time::timeout(Duration::from_secs(5), finished)
            .await
            .expect("a cancellation runs while the pool is saturated")
            .expect("the cancellation completed");

        // Releasing the work lets the queued commands through.
        gate.add_permits(16);
        drop(ordinary_tx);
        drop(urgent_tx);
        tokio::time::timeout(Duration::from_secs(5), driver)
            .await
            .expect("the loop ends once its queues close")
            .expect("the loop did not panic");
        // The last two were spawned before the loop ended; give them a moment
        // to start.
        tokio::time::timeout(Duration::from_secs(5), async {
            while started.load(Ordering::SeqCst) < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the queued commands ran once the pool freed up");
    }

    /// "Play" then "Stop" while the pool is busy: the stop overtakes the start
    /// in its own queue, so the start must not run afterwards and requeue the
    /// player. A start sent after the stop still runs.
    #[tokio::test]
    async fn a_start_overtaken_by_its_release_is_dropped() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        use faf_domain::state::LobbyCommand;

        let order = Arc::new(ReleaseOrder::default());
        let matchmake = |start: bool| -> AppCommand {
            LobbyCommand::Matchmake {
                queue_name: "ladder1v1".into(),
                start,
            }
            .into()
        };
        let queued = |command: AppCommand, completion| QueuedCommand {
            queued_at: std::time::Instant::now(),
            seq: order.stamp(&command),
            command,
            completion,
        };

        let (ordinary_tx, ordinary_rx) = mpsc::channel::<QueuedCommand>(4);
        let (urgent_tx, urgent_rx) = mpsc::channel::<QueuedCommand>(4);
        let starts = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let handle = {
            let (starts, gate) = (starts.clone(), gate.clone());
            move |command: AppCommand| {
                let (starts, gate) = (starts.clone(), gate.clone());
                async move {
                    match command {
                        AppCommand::Session(_) => drop(gate.acquire().await),
                        AppCommand::Lobby(LobbyCommand::Matchmake { start: true, .. }) => {
                            starts.fetch_add(1, Ordering::SeqCst);
                        }
                        _ => {}
                    }
                }
            }
        };
        let limits = Limits {
            ordinary: 1,
            urgent: 1,
        };
        let driver = tokio::spawn(drive(ordinary_rx, urgent_rx, order.clone(), limits, handle));

        // The only ordinary permit is busy, so "Play" waits in the queue.
        ordinary_tx
            .send(queued(SessionCommand::Hello.into(), None))
            .await
            .unwrap();
        let (played, play_done) = oneshot::channel();
        ordinary_tx
            .send(queued(matchmake(true), Some(played)))
            .await
            .unwrap();
        // "Stop" overtakes it.
        urgent_tx
            .send(queued(matchmake(false), None))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        gate.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), play_done)
            .await
            .expect("the dropped start still answers its caller")
            .unwrap();
        assert_eq!(
            starts.load(Ordering::SeqCst),
            0,
            "the overtaken start never ran"
        );

        // Pressing Play again afterwards is a new start and runs.
        let (replayed, replay_done) = oneshot::channel();
        ordinary_tx
            .send(queued(matchmake(true), Some(replayed)))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), replay_done)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            starts.load(Ordering::SeqCst),
            1,
            "a start sent after the stop runs"
        );

        drop(ordinary_tx);
        drop(urgent_tx);
        tokio::time::timeout(Duration::from_secs(5), driver)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn releases_only_cancel_starts_of_the_same_pair() {
        use faf_domain::state::LobbyCommand;

        let matchmake = |queue: &str, start: bool| -> AppCommand {
            LobbyCommand::Matchmake {
                queue_name: queue.into(),
                start,
            }
            .into()
        };
        let order = ReleaseOrder::default();
        let start_a = matchmake("ladder1v1", true);
        let start_b = matchmake("tmm2v2", true);
        let (seq_a, seq_b) = (order.stamp(&start_a), order.stamp(&start_b));
        order.stamp(&matchmake("ladder1v1", false));
        assert!(order.superseded(&start_a, seq_a));
        assert!(
            !order.superseded(&start_b, seq_b),
            "another queue's start is untouched"
        );
        assert!(
            !order.superseded(&SessionCommand::Hello.into(), 0),
            "unpaired commands always run"
        );
    }

    #[tokio::test]
    async fn snapshot_subscription_draws_an_exact_event_boundary() {
        let (app, app_loop) = App::new("test", crate::infra::fake_ports());
        app_loop.sink.emit(SessionEvent::Connecting);

        let (mut events, snapshot) = app.subscribe_with_snapshot();
        assert_eq!(snapshot.session.status, ConnectionStatus::Connecting);

        app_loop.sink.emit(SessionEvent::BackendReady {
            version: "1.2.3".into(),
            offline_auth: false,
        });
        assert!(matches!(
            events.recv().await,
            Ok(AppEvent::Session(SessionEvent::BackendReady { version, .. })) if version == "1.2.3"
        ));
    }

    #[tokio::test]
    async fn revisioned_snapshot_deduplicates_earlier_events() {
        let (app, app_loop) = App::new("test", crate::infra::fake_ports());
        let (mut events, initial) = app.subscribe_versioned_with_snapshot();
        assert_eq!(initial.revision, 0);

        app_loop.sink.emit(SessionEvent::Connecting);
        let event = events.recv().await.expect("versioned event");
        assert_eq!(event.revision, 1);

        let snapshot = app.versioned_snapshot();
        assert_eq!(snapshot.revision, event.revision);
        assert_eq!(snapshot.state.session.status, ConnectionStatus::Connecting);
    }

    #[tokio::test]
    async fn dispatch_and_wait_observes_the_completed_service_effect() {
        let (app, app_loop) = App::new("test", crate::infra::fake_ports());
        tokio::spawn(app_loop.run());

        app.dispatch_and_wait(SessionCommand::Hello.into())
            .await
            .expect("command completes");

        assert_eq!(app.snapshot().session.status, ConnectionStatus::Connected);
    }
}
