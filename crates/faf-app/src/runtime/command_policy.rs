//! Which commands may run together, in one table.
//!
//! Commands run concurrently, each on its own task. Some must not: a second
//! catalogue crawl while the first is running, two writes to the same mod
//! folder, a second map generation fighting the first over the generator. Each
//! service used to decide that for itself, with a guard of its own taken
//! somewhere inside its handler, so adding a command meant remembering to look
//! for one, and a service that called another service's handler directly
//! walked straight past it.
//!
//! [`policy`] names, for every command there is, the lane it waits in and
//! whether it may run alongside its own kind. The match has no wildcard: a new
//! command does not compile until somebody has decided. [`CommandAdmission`]
//! enforces the decision, for commands from the webview and, through
//! [`super::run_command`], for the ones a service starts itself.
//!
//! Two concerns stay with the services and are not in this table:
//!
//! - **Stale answers.** A search whose answer arrives after a newer search was
//!   asked must not land. That is about responses, not admission, and the
//!   services keep their [`super::LatestRequest`] generations for it.
//! - **Guards over part of a command.** A few commands hold a guard across only
//!   some of their work, or across a connection that outlives them. Those are
//!   named here as [`Admission::ServiceGuarded`], with the reason on
//!   [`ServiceGuard`], so the table still tells the whole story.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use faf_domain::state::{
    AuthCommand, ChangelogCommand, ChatCommand, ClanCommand, ClientUpdateCommand, CoopCommand,
    EventsCommand, GalacticWarCommand, GuidesCommand, LeaderboardCommand, LobbyCommand,
    MapGeneratorCommand, MapsCommand, ModsCommand, NavCommand, NotificationCommand,
    PlayerCardCommand, ReplayCommand, ReportingCommand, ReviewsCommand, SessionCommand,
    SettingsCommand, SocialCommand, StreamsCommand, TourneyCommand, TourneyRead, TrainingCommand,
    TutorialsCommand, UploadsCommand,
};
use faf_domain::AppCommand;
use tokio::sync::oneshot;

/// Which queue a command waits in. See `drive` in the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lane {
    Ordinary,
    /// Its own queue and permits, checked first. For commands that call work
    /// off, which must not wait behind the work they are stopping, and for
    /// navigation, which only changes what is on screen.
    Priority,
}

/// What a command is exclusive with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Key {
    /// The whole map catalogue crawl.
    MapVault,
    /// The whole mod catalogue crawl.
    ModVault,
    /// The changelog index. A dropped load loses nothing: the one running ends
    /// by selecting the newest patch itself.
    Changelog,
    /// Map installs and uninstalls, which write the same maps folder.
    MapFiles,
    /// Mod installs, updates, uninstalls and activation, which write the same
    /// mods folder and `game.prefs`.
    ModFiles,
    /// The map generator: one run at a time, its status describes one run.
    MapGenerator,
    /// Vault uploads share one temporary archive path.
    Upload,
    /// Checking for, downloading and installing a client update.
    ClientUpdate,
    /// Installing and launching Galactic War share a staging directory.
    GalacticWar,
    /// A GitHub device-flow sign-in: a second one would issue a second code
    /// and leave the one on screen dead.
    GuidesSignIn,
    /// Accepting and rejecting guide submissions, which each read and patch
    /// the catalogue.
    GuidesVerdict,
    /// Launching a tutorial.
    TutorialLaunch,
    /// Loading the training hub: one at a time, since a second load repeats
    /// every request and the replay scan.
    TrainingLoad,
    /// Clan writes, each of which ends by reloading the clan.
    ClanWrite,
    /// Tournament writes: the server recomputes the bracket on every one.
    TourneyWrite,
    /// Party placement lookups: serialising them turns "already known" into
    /// "asked once".
    PartyPlacements,
}

/// A guard a service holds itself, over part of a command or over something
/// that outlives it. Declared here so the table is complete; enforced in the
/// service named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServiceGuard {
    /// The lobby socket, held by `Connect` for as long as it is open and
    /// released by `Disconnect` or the socket closing (`services::lobby`).
    LobbyConnection,
    /// The chat socket, the same way (`services::chat`).
    ChatConnection,
    /// The join slot: taken by `Join`, released by the server's answer, a
    /// cancel or a disconnect, all in other commands (`runtime::policies`,
    /// `LobbyOperations`).
    LobbyJoin,
    /// Signing in and out. `Logout` has to cancel a sign-in in progress before
    /// it waits for the lock, so the lock cannot cover the whole command
    /// (`services::auth`).
    Login,
    /// Settings changes merge under one lock and write under another, only
    /// around those two steps (`services::settings`).
    Settings,
}

/// Whether a command may run alongside others of its kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Admission {
    /// Runs alongside anything.
    Concurrent,
    /// At most one with this key runs; one that arrives meanwhile is dropped.
    /// For work that a second request would only repeat.
    SingleFlight(Key),
    /// Commands with this key run one after another, in the order they were
    /// dispatched. For writes, where each must see the last one's result.
    Serial(Key),
    /// Concurrent here; the service holds the guard named. See [`ServiceGuard`].
    ServiceGuarded(ServiceGuard),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommandPolicy {
    pub(crate) lane: Lane,
    pub(crate) admission: Admission,
}

const ORDINARY: CommandPolicy = CommandPolicy {
    lane: Lane::Ordinary,
    admission: Admission::Concurrent,
};

const PRIORITY: CommandPolicy = CommandPolicy {
    lane: Lane::Priority,
    admission: Admission::Concurrent,
};

const fn single_flight(key: Key) -> CommandPolicy {
    CommandPolicy {
        lane: Lane::Ordinary,
        admission: Admission::SingleFlight(key),
    }
}

const fn serial(key: Key) -> CommandPolicy {
    CommandPolicy {
        lane: Lane::Ordinary,
        admission: Admission::Serial(key),
    }
}

const fn service_guarded(guard: ServiceGuard) -> CommandPolicy {
    CommandPolicy {
        lane: Lane::Ordinary,
        admission: Admission::ServiceGuarded(guard),
    }
}

/// The policy for one command. Exhaustive on purpose: see the module note.
pub(crate) fn policy(command: &AppCommand) -> CommandPolicy {
    match command {
        AppCommand::Session(command) => session(command),
        AppCommand::Auth(command) => auth(command),
        AppCommand::Nav(command) => nav(command),
        AppCommand::Notifications(command) => notifications(command),
        AppCommand::Chat(command) => chat(command),
        AppCommand::Clan(command) => clan(command),
        AppCommand::Coop(command) => coop(command),
        AppCommand::Lobby(command) => lobby(command),
        AppCommand::Replays(command) => replays(command),
        AppCommand::Maps(command) => maps(command),
        AppCommand::MapGenerator(command) => map_generator(command),
        AppCommand::Mods(command) => mods(command),
        AppCommand::Leaderboard(command) => leaderboard(command),
        AppCommand::PlayerCard(command) => player_card(command),
        AppCommand::Reporting(command) => reporting(command),
        AppCommand::Reviews(command) => reviews(command),
        AppCommand::Social(command) => social(command),
        AppCommand::Streams(command) => streams(command),
        AppCommand::Tourney(command) => tourney(command),
        AppCommand::Training(command) => training(command),
        AppCommand::Tutorials(command) => tutorials(command),
        AppCommand::Changelog(command) => changelog(command),
        AppCommand::Events(command) => events(command),
        AppCommand::Uploads(command) => uploads(command),
        AppCommand::GalacticWar(command) => galactic_war(command),
        AppCommand::Guides(command) => guides(command),
        AppCommand::ClientUpdate(command) => client_update(command),
        AppCommand::Settings(command) => settings(command),
    }
}

fn session(command: &SessionCommand) -> CommandPolicy {
    match command {
        SessionCommand::Hello => ORDINARY,
    }
}

fn auth(command: &AuthCommand) -> CommandPolicy {
    match command {
        AuthCommand::Login { .. } | AuthCommand::Restore => service_guarded(ServiceGuard::Login),
        AuthCommand::CancelLogin => PRIORITY,
        AuthCommand::PlayOffline
        | AuthCommand::LaunchOfflineGame
        | AuthCommand::LoginTest
        | AuthCommand::LogoutTest => ORDINARY,
        AuthCommand::Logout => CommandPolicy {
            lane: Lane::Priority,
            admission: Admission::ServiceGuarded(ServiceGuard::Login),
        },
    }
}

fn nav(command: &NavCommand) -> CommandPolicy {
    match command {
        NavCommand::Select { .. }
        | NavCommand::SelectMapsSection { .. }
        | NavCommand::SelectModsSection { .. }
        | NavCommand::SelectReplaysSection { .. }
        | NavCommand::SelectSettingsSection { .. } => PRIORITY,
    }
}

fn notifications(command: &NotificationCommand) -> CommandPolicy {
    match command {
        NotificationCommand::MarkRead { .. }
        | NotificationCommand::Dismiss { .. }
        | NotificationCommand::Clear => ORDINARY,
    }
}

fn chat(command: &ChatCommand) -> CommandPolicy {
    match command {
        ChatCommand::Connect { .. } => service_guarded(ServiceGuard::ChatConnection),
        ChatCommand::SendMessage { .. }
        | ChatCommand::JoinChannel { .. }
        | ChatCommand::LeaveChannel { .. }
        | ChatCommand::SelectChannel { .. }
        | ChatCommand::SetShowJoinsParts { .. }
        | ChatCommand::SetTyping { .. }
        | ChatCommand::React { .. }
        | ChatCommand::Unreact { .. } => ORDINARY,
        ChatCommand::Disconnect => PRIORITY,
    }
}

fn clan(command: &ClanCommand) -> CommandPolicy {
    match command {
        ClanCommand::Load
        | ClanCommand::SearchCandidates { .. }
        | ClanCommand::Invite { .. }
        | ClanCommand::ClearInvitation => ORDINARY,
        ClanCommand::Create { .. }
        | ClanCommand::Edit { .. }
        | ClanCommand::AcceptInvitation { .. }
        | ClanCommand::Remove { .. }
        | ClanCommand::Leave
        | ClanCommand::HandOver { .. }
        | ClanCommand::Disband => serial(Key::ClanWrite),
    }
}

fn coop(command: &CoopCommand) -> CommandPolicy {
    match command {
        CoopCommand::LoadCatalog
        | CoopCommand::RefreshCatalog
        | CoopCommand::SelectMission { .. }
        | CoopCommand::SetPlayerCount { .. } => ORDINARY,
    }
}

fn lobby(command: &LobbyCommand) -> CommandPolicy {
    match command {
        LobbyCommand::Matchmake { start: false, .. } => PRIORITY,
        LobbyCommand::Connect => service_guarded(ServiceGuard::LobbyConnection),
        LobbyCommand::Join { .. } => service_guarded(ServiceGuard::LobbyJoin),
        LobbyCommand::Host { .. }
        | LobbyCommand::PrepareHost { .. }
        | LobbyCommand::ClearHostPrefill
        | LobbyCommand::Matchmake { .. }
        | LobbyCommand::StartSearch { .. }
        | LobbyCommand::LeaveParty
        | LobbyCommand::KickPartyMember { .. }
        | LobbyCommand::InviteToParty { .. }
        | LobbyCommand::AcceptPartyInvite { .. }
        | LobbyCommand::SetPartyFactions { .. }
        | LobbyCommand::SetPlayMode { .. }
        | LobbyCommand::SetPlayerVetoes { .. }
        | LobbyCommand::LoadAvatars
        | LobbyCommand::SelectAvatar { .. } => ORDINARY,
        LobbyCommand::DeclineModReplacement
        | LobbyCommand::CancelJoin
        | LobbyCommand::TerminateGame
        | LobbyCommand::Disconnect => PRIORITY,
    }
}

fn replays(command: &ReplayCommand) -> CommandPolicy {
    match command {
        ReplayCommand::WatchLive(..)
        | ReplayCommand::TrackLive { .. }
        | ReplayCommand::OpenFile { .. }
        | ReplayCommand::SearchVault { .. }
        | ReplayCommand::LoadFeaturedMods
        | ReplayCommand::LoadRecentMatchmaker
        | ReplayCommand::WatchVault { .. }
        | ReplayCommand::DownloadVault { .. }
        | ReplayCommand::LoadDetails { .. }
        | ReplayCommand::LoadAnalysis { .. }
        | ReplayCommand::LoadLocal { .. }
        | ReplayCommand::DeleteLocal { .. }
        | ReplayCommand::ResolveMaps { .. }
        | ReplayCommand::LookUpOnline { .. }
        | ReplayCommand::LookUpOnlineMany { .. } => ORDINARY,
        ReplayCommand::CancelLiveTracking | ReplayCommand::CancelWatch => PRIORITY,
    }
}

fn maps(command: &MapsCommand) -> CommandPolicy {
    match command {
        MapsCommand::CancelVaultLoad => PRIORITY,
        MapsCommand::LoadVault => single_flight(Key::MapVault),
        MapsCommand::SearchVault { .. }
        | MapsCommand::LoadInstalled
        | MapsCommand::ResolveVaultFolders { .. }
        | MapsCommand::LoadLocalPreviews { .. }
        | MapsCommand::LoadMatchmakerPools { .. }
        | MapsCommand::SetMapVersionHidden { .. } => ORDINARY,
        MapsCommand::InstallMap { .. } | MapsCommand::UninstallMap { .. } => serial(Key::MapFiles),
    }
}

fn map_generator(command: &MapGeneratorCommand) -> CommandPolicy {
    match command {
        MapGeneratorCommand::GenerateNamed { .. }
        | MapGeneratorCommand::Generate { .. }
        | MapGeneratorCommand::CleanUp => single_flight(Key::MapGenerator),
        MapGeneratorCommand::LoadOptions { .. }
        | MapGeneratorCommand::SetOptions { .. }
        | MapGeneratorCommand::Validate { .. }
        | MapGeneratorCommand::Preflight { .. }
        | MapGeneratorCommand::DecodeNames { .. }
        | MapGeneratorCommand::LoadPreviews { .. }
        | MapGeneratorCommand::LoadHelp { .. }
        | MapGeneratorCommand::SavePreset { .. }
        | MapGeneratorCommand::LoadPresets
        | MapGeneratorCommand::DeletePreset { .. } => ORDINARY,
        MapGeneratorCommand::Cancel => PRIORITY,
    }
}

fn mods(command: &ModsCommand) -> CommandPolicy {
    match command {
        ModsCommand::CancelVaultLoad => PRIORITY,
        ModsCommand::LoadVault | ModsCommand::ReloadVault => single_flight(Key::ModVault),
        ModsCommand::SearchVault { .. }
        | ModsCommand::LoadInstalled
        | ModsCommand::QueryDownloadSizes { .. } => ORDINARY,
        ModsCommand::InstallMod { .. }
        | ModsCommand::UpdateMod { .. }
        | ModsCommand::UninstallMod { .. }
        | ModsCommand::ToggleMod { .. }
        | ModsCommand::SetActiveMods { .. } => serial(Key::ModFiles),
    }
}

fn leaderboard(command: &LeaderboardCommand) -> CommandPolicy {
    match command {
        LeaderboardCommand::SetMode { .. }
        | LeaderboardCommand::LoadCatalog
        | LeaderboardCommand::LoadRatings { .. }
        | LeaderboardCommand::SelectLeague { .. }
        | LeaderboardCommand::SelectSeason { .. } => ORDINARY,
    }
}

fn player_card(command: &PlayerCardCommand) -> CommandPolicy {
    match command {
        PlayerCardCommand::Open { .. }
        | PlayerCardCommand::LookUpAccounts { .. }
        | PlayerCardCommand::Close
        | PlayerCardCommand::LoadHistory { .. }
        | PlayerCardCommand::LoadMatchmakerProfile { .. }
        | PlayerCardCommand::LoadMapStats { .. } => ORDINARY,
        PlayerCardCommand::LoadPartyPlacements { .. } => serial(Key::PartyPlacements),
    }
}

fn reporting(command: &ReportingCommand) -> CommandPolicy {
    match command {
        ReportingCommand::Open { .. }
        | ReportingCommand::OpenByLogin { .. }
        | ReportingCommand::Close
        | ReportingCommand::LoadHistory
        | ReportingCommand::Submit { .. } => ORDINARY,
    }
}

fn reviews(command: &ReviewsCommand) -> CommandPolicy {
    match command {
        ReviewsCommand::Open { .. }
        | ReviewsCommand::Close
        | ReviewsCommand::Submit { .. }
        | ReviewsCommand::Delete => ORDINARY,
    }
}

fn social(command: &SocialCommand) -> CommandPolicy {
    match command {
        SocialCommand::SetRelation { .. } => ORDINARY,
        // A read whose answer is keyed by its login, so two at once are two
        // answers, never one overwriting the other.
        SocialCommand::LookUpLogin { .. } => ORDINARY,
    }
}

fn streams(command: &StreamsCommand) -> CommandPolicy {
    match command {
        StreamsCommand::Check => ORDINARY,
    }
}

fn tourney(command: &TourneyCommand) -> CommandPolicy {
    match command {
        // Every write, as the type defines them: a new one is serial by being
        // a `TourneyWrite`, with nothing here to keep in step.
        TourneyCommand::Write(_) => serial(Key::TourneyWrite),
        TourneyCommand::Read(command) => tourney_read(command),
    }
}

fn tourney_read(command: &TourneyRead) -> CommandPolicy {
    match command {
        TourneyRead::Load
        | TourneyRead::Select { .. }
        | TourneyRead::RefreshDetail { .. }
        | TourneyRead::CheckRating { .. }
        | TourneyRead::LoadPlayerRatings { .. }
        | TourneyRead::LoadCopySources
        | TourneyRead::LoadPresets
        | TourneyRead::LoadSite { .. }
        | TourneyRead::LoadTemplate { .. }
        | TourneyRead::LoadCopySource { .. }
        | TourneyRead::LoadChat { .. }
        | TourneyRead::OpenRoom { .. }
        | TourneyRead::RefreshChat { .. }
        | TourneyRead::PinRoom { .. }
        | TourneyRead::LoadArticles
        | TourneyRead::LoadHosting
        | TourneyRead::LoadProfile
        | TourneyRead::SetDiscord { .. }
        | TourneyRead::SearchAccounts { .. }
        | TourneyRead::ClearAccountSearch
        | TourneyRead::CheckRenames { .. }
        | TourneyRead::LoadSeries
        | TourneyRead::OpenSeries { .. }
        | TourneyRead::CloseSeries
        | TourneyRead::MarkNewsRead { .. }
        | TourneyRead::DismissActionError => ORDINARY,
    }
}

fn training(command: &TrainingCommand) -> CommandPolicy {
    match command {
        TrainingCommand::Load => single_flight(Key::TrainingLoad),
        TrainingCommand::SetQuery { .. }
        | TrainingCommand::Select { .. }
        | TrainingCommand::ReadGuide { .. }
        | TrainingCommand::OpenReview { .. }
        | TrainingCommand::ComposeReview { .. }
        | TrainingCommand::CloseReview
        | TrainingCommand::OpenContribution
        | TrainingCommand::ChangeContribution { .. }
        | TrainingCommand::ComposeContribution { .. }
        | TrainingCommand::CloseContribution => ORDINARY,
    }
}

fn tutorials(command: &TutorialsCommand) -> CommandPolicy {
    match command {
        TutorialsCommand::CancelLaunch => PRIORITY,
        TutorialsCommand::Load | TutorialsCommand::Select { .. } => ORDINARY,
        TutorialsCommand::Launch { .. } => single_flight(Key::TutorialLaunch),
    }
}

fn changelog(command: &ChangelogCommand) -> CommandPolicy {
    match command {
        ChangelogCommand::Load => single_flight(Key::Changelog),
        ChangelogCommand::Select { .. } => ORDINARY,
    }
}

fn events(command: &EventsCommand) -> CommandPolicy {
    match command {
        EventsCommand::Load
        | EventsCommand::SetView { .. }
        | EventsCommand::SetAnchor { .. }
        | EventsCommand::SetQuery { .. }
        | EventsCommand::Select { .. }
        | EventsCommand::Remind { .. }
        | EventsCommand::Forget { .. } => ORDINARY,
    }
}

fn uploads(command: &UploadsCommand) -> CommandPolicy {
    match command {
        UploadsCommand::Open { .. } | UploadsCommand::Close | UploadsCommand::SetRanked { .. } => {
            ORDINARY
        }
        UploadsCommand::Start => single_flight(Key::Upload),
    }
}

fn galactic_war(command: &GalacticWarCommand) -> CommandPolicy {
    match command {
        GalacticWarCommand::CancelInstall => PRIORITY,
        GalacticWarCommand::Refresh | GalacticWarCommand::RefreshStatistics => ORDINARY,
        GalacticWarCommand::Install | GalacticWarCommand::Play => single_flight(Key::GalacticWar),
    }
}

fn guides(command: &GuidesCommand) -> CommandPolicy {
    match command {
        GuidesCommand::Restore
        | GuidesCommand::SignOut
        | GuidesCommand::LoadQueue
        | GuidesCommand::Submit { .. } => ORDINARY,
        GuidesCommand::SignIn => single_flight(Key::GuidesSignIn),
        GuidesCommand::CancelSignIn => PRIORITY,
        GuidesCommand::Accept { .. } | GuidesCommand::Reject { .. } => serial(Key::GuidesVerdict),
    }
}

fn client_update(command: &ClientUpdateCommand) -> CommandPolicy {
    match command {
        ClientUpdateCommand::Check
        | ClientUpdateCommand::Download
        | ClientUpdateCommand::Install => single_flight(Key::ClientUpdate),
        ClientUpdateCommand::Dismiss => ORDINARY,
    }
}

fn settings(command: &SettingsCommand) -> CommandPolicy {
    match command {
        SettingsCommand::Load
        | SettingsCommand::SetTheme { .. }
        | SettingsCommand::SetGamePath { .. }
        | SettingsCommand::SetReplayGamePath { .. }
        | SettingsCommand::PatchPaths { .. }
        | SettingsCommand::PatchGeneral { .. }
        | SettingsCommand::PatchAppearance { .. }
        | SettingsCommand::SetPlayerNote { .. }
        | SettingsCommand::SetReplayNote { .. }
        | SettingsCommand::RenameReplayTag { .. }
        | SettingsCommand::PatchNotifications { .. }
        | SettingsCommand::RemoveNotificationSound { .. }
        | SettingsCommand::PatchChat { .. }
        | SettingsCommand::PatchGame { .. }
        | SettingsCommand::PatchDiscord { .. }
        | SettingsCommand::PatchConnectivity { .. }
        | SettingsCommand::PatchDebug { .. }
        | SettingsCommand::PatchUpdates { .. }
        | SettingsCommand::SetMapGenerator { .. }
        | SettingsCommand::PatchBrowsing { .. }
        | SettingsCommand::PatchEvents { .. }
        | SettingsCommand::SetListMember { .. }
        | SettingsCommand::SetPlayerNameColor { .. }
        | SettingsCommand::SaveModPreset { .. }
        | SettingsCommand::DeleteModPreset { .. }
        | SettingsCommand::CheckInstalls
        | SettingsCommand::RefreshGameCache
        | SettingsCommand::ClearGameCache => service_guarded(ServiceGuard::Settings),
    }
}

tokio::task_local! {
    /// The admission the command running on this task came in under. Set by
    /// the runtime around every command, read by [`expect_admitted`].
    static ADMITTED_AS: Admission;

    /// The turn the command running on this task holds, until it finishes or
    /// hands it on early with [`end_turn`].
    static TURN: Arc<Mutex<Option<Turn>>>;
}

/// Run `work` as a command admitted under `admission`, holding `turn`, so that
/// the code it reaches can check with [`expect_admitted`] that the table
/// agrees with it, and can end its turn early with [`end_turn`].
///
/// `turn` must already be [`Turn::ready`]. It is dropped when `work` is
/// finished, unless `work` ended it first.
pub(crate) async fn run_admitted<F: std::future::Future>(
    admission: Admission,
    turn: Turn,
    work: F,
) -> F::Output {
    let slot = Arc::new(Mutex::new(Some(turn)));
    let output = TURN
        .scope(slot.clone(), ADMITTED_AS.scope(admission, work))
        .await;
    drop(slot.lock().unwrap_or_else(PoisonError::into_inner).take());
    output
}

/// Hand the running command's turn on before the command is finished.
///
/// For a serial command whose ordered part is over while it still has work
/// to do: a write that must not overtake another, followed by reads that
/// refresh what the write changed. Holding the turn across those reads makes
/// the next write wait for reads it has nothing to do with. A guide accepted
/// in the training queue is followed by a reload of the queue and of the
/// whole catalogue, and the trainer's next verdict waited for both.
///
/// The [`expect_admitted`] check still passes afterwards: the command was
/// admitted under its key, it just no longer holds it. Does nothing outside a
/// command, and a second call does nothing.
pub(crate) fn end_turn() {
    let _ = TURN.try_with(|slot| {
        drop(slot.lock().unwrap_or_else(PoisonError::into_inner).take());
    });
}

/// A debug check, for code that is only correct one at a time: the command
/// running it must have been admitted under `key`.
///
/// The table being exhaustive proves that every command has an entry, not that
/// the entry is right. Sixty-odd tournament writes once lost their ordering
/// because the table named only the five that called the lock-holding helper
/// directly, missing every one that reached it through a wrapper. A helper
/// that must run serially says so here, and any test that reaches it under the
/// wrong entry fails, whichever command it came from.
///
/// Silent outside a command (a unit test calling a service directly), and in
/// release builds.
pub(crate) fn expect_admitted(key: Key) {
    if !cfg!(debug_assertions) {
        return;
    }
    let Ok(admission) = ADMITTED_AS.try_with(|admission| *admission) else {
        return;
    };
    assert!(
        matches!(admission, Admission::SingleFlight(held) | Admission::Serial(held) if held == key),
        "this code needs a command admitted under {key:?}, but it runs under {admission:?}; \
         give the command that reached it that entry in runtime/command_policy.rs"
    );
}

/// Enforces [`Admission::SingleFlight`] and [`Admission::Serial`].
///
/// [`Self::admit`] is synchronous, and is called where the command is taken
/// off its queue, before its task is spawned. That is what fixes a serial
/// command's place: tasks start in whatever order the executor picks, so a
/// place taken from inside the task would follow that order rather than the
/// order the commands were sent in.
#[derive(Default)]
pub(crate) struct CommandAdmission {
    running: Arc<Mutex<HashSet<Key>>>,
    /// Per serial key, what the last command admitted under it signals when it
    /// finishes. The next one waits for that before it runs.
    tails: Mutex<HashMap<Key, oneshot::Receiver<()>>>,
}

impl CommandAdmission {
    /// A turn to run under `admission`, or `None` when a single-flight command
    /// of the same kind is already running and this one is dropped.
    pub(crate) fn admit(&self, admission: Admission) -> Option<Turn> {
        match admission {
            Admission::Concurrent | Admission::ServiceGuarded(_) => Some(Turn::default()),
            Admission::SingleFlight(key) => {
                let mut running = self.running.lock().unwrap_or_else(PoisonError::into_inner);
                if !running.insert(key) {
                    return None;
                }
                Some(Turn {
                    wait: None,
                    done: None,
                    flight: Some((self.running.clone(), key)),
                })
            }
            Admission::Serial(key) => {
                let (done, signal) = oneshot::channel();
                let previous = self
                    .tails
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(key, signal);
                Some(Turn {
                    wait: previous,
                    done: Some(done),
                    flight: None,
                })
            }
        }
    }
}

/// One command's admission. Held for as long as the command runs; dropping it
/// lets the next one of its kind go.
#[derive(Default)]
pub(crate) struct Turn {
    wait: Option<oneshot::Receiver<()>>,
    done: Option<oneshot::Sender<()>>,
    flight: Option<(Arc<Mutex<HashSet<Key>>>, Key)>,
}

impl Turn {
    /// Wait until the commands admitted before this one under the same serial
    /// key have finished. Immediate for every other admission.
    pub(crate) async fn ready(&mut self) {
        // Awaited in place rather than taken out first: if this wait is itself
        // cancelled, the receiver is still here for `Drop` to hand on.
        if let Some(previous) = self.wait.as_mut() {
            // An error only means the one before is gone, which is as finished
            // as it will get: a turn hands on its own wait when it drops.
            let _ = previous.await;
            self.wait = None;
        }
    }
}

impl Drop for Turn {
    fn drop(&mut self) {
        if let Some((running, key)) = self.flight.take() {
            running
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&key);
        }
        match (self.wait.take(), self.done.take()) {
            // Dropped before its turn came: the command behind it must still
            // wait for the one before it, not start the moment this one goes.
            // Signalling straight away let a third command run beside the first.
            (Some(previous), Some(done)) => match tokio::runtime::Handle::try_current() {
                Ok(runtime) => {
                    runtime.spawn(async move {
                        let _ = previous.await;
                        let _ = done.send(());
                    });
                }
                // No runtime, so nothing is running that could be overtaken.
                Err(_) => {
                    let _ = done.send(());
                }
            },
            (_, Some(done)) => {
                let _ = done.send(());
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faf_domain::state::ReplaysSection;

    #[test]
    fn the_releasing_half_of_a_pair_and_navigation_take_the_priority_lane() {
        let lane = |command: AppCommand| policy(&command).lane;
        assert_eq!(lane(LobbyCommand::CancelJoin.into()), Lane::Priority);
        assert_eq!(lane(LobbyCommand::Disconnect.into()), Lane::Priority);
        assert_eq!(lane(AuthCommand::Logout.into()), Lane::Priority);
        assert_eq!(
            lane(
                NavCommand::SelectReplaysSection {
                    section: ReplaysSection::Online,
                }
                .into()
            ),
            Lane::Priority
        );
        assert_eq!(
            lane(
                LobbyCommand::Matchmake {
                    queue_name: "ladder1v1".into(),
                    start: false,
                }
                .into()
            ),
            Lane::Priority
        );
        assert_eq!(
            lane(
                LobbyCommand::Matchmake {
                    queue_name: "ladder1v1".into(),
                    start: true,
                }
                .into()
            ),
            Lane::Ordinary,
            "starting a search is not a release"
        );
        assert_eq!(lane(SessionCommand::Hello.into()), Lane::Ordinary);
    }

    #[test]
    fn the_guards_services_used_to_hold_are_in_the_table() {
        let admission = |command: AppCommand| policy(&command).admission;
        assert_eq!(
            admission(MapsCommand::LoadVault.into()),
            Admission::SingleFlight(Key::MapVault)
        );
        assert_eq!(
            admission(ModsCommand::ReloadVault.into()),
            Admission::SingleFlight(Key::ModVault)
        );
        assert_eq!(
            admission(
                ModsCommand::ToggleMod {
                    uid: "x".into(),
                    enabled: true,
                }
                .into()
            ),
            Admission::Serial(Key::ModFiles)
        );
        assert_eq!(
            admission(ClientUpdateCommand::Check.into()),
            Admission::SingleFlight(Key::ClientUpdate)
        );
        assert_eq!(
            admission(LobbyCommand::Connect.into()),
            Admission::ServiceGuarded(ServiceGuard::LobbyConnection)
        );
        assert_eq!(
            admission(ChangelogCommand::Select { id: "1".into() }.into()),
            Admission::Concurrent,
            "only the index load is single-flight"
        );
    }

    #[test]
    fn a_single_flight_command_is_refused_while_its_kind_runs() {
        let admission = CommandAdmission::default();
        let first = admission.admit(Admission::SingleFlight(Key::MapVault));
        assert!(first.is_some());
        assert!(
            admission
                .admit(Admission::SingleFlight(Key::MapVault))
                .is_none(),
            "a second crawl while the first runs"
        );
        assert!(
            admission
                .admit(Admission::SingleFlight(Key::ModVault))
                .is_some(),
            "another kind is not held up"
        );
        drop(first);
        assert!(
            admission
                .admit(Admission::SingleFlight(Key::MapVault))
                .is_some(),
            "finished, so the next one runs"
        );
    }

    #[tokio::test]
    async fn serial_commands_run_in_the_order_they_were_admitted() {
        let admission = CommandAdmission::default();
        let order = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Notify::new());

        // Admitted in order 1, 2, 3, then spawned in reverse, so the executor
        // starts them in the wrong order on purpose.
        let turns: Vec<_> = (1..=3)
            .map(|n| {
                (
                    n,
                    admission.admit(Admission::Serial(Key::ModFiles)).unwrap(),
                )
            })
            .collect();
        let mut tasks = Vec::new();
        for (n, mut turn) in turns.into_iter().rev() {
            let order = order.clone();
            let gate = gate.clone();
            tasks.push(tokio::spawn(async move {
                turn.ready().await;
                if n == 1 {
                    // Holds its turn until released, so the others must wait.
                    gate.notified().await;
                }
                order.lock().unwrap().push(n);
            }));
        }
        tokio::task::yield_now().await;
        assert!(
            order.lock().unwrap().is_empty(),
            "nobody overtook the first"
        );
        gate.notify_one();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn a_turn_dropped_while_waiting_keeps_the_next_one_behind_the_one_before() {
        let admission = CommandAdmission::default();
        let first = admission.admit(Admission::Serial(Key::ModFiles)).unwrap();
        let mut middle = admission.admit(Admission::Serial(Key::ModFiles)).unwrap();
        let mut last = admission.admit(Admission::Serial(Key::ModFiles)).unwrap();

        // The middle one starts waiting, is cancelled mid-wait, and goes.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), middle.ready())
                .await
                .is_err()
        );
        drop(middle);

        // The first still runs, so the last must still wait for it.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), last.ready())
                .await
                .is_err(),
            "the last overtook the first because the middle one gave up"
        );
        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(1), last.ready())
            .await
            .expect("the last runs once the first has finished");
    }

    #[tokio::test]
    async fn code_that_needs_a_key_checks_the_command_came_in_under_it() {
        // Outside any command: nothing to check against.
        expect_admitted(Key::TourneyWrite);
        // Under the right entry.
        run_admitted(
            Admission::Serial(Key::TourneyWrite),
            Turn::default(),
            async { expect_admitted(Key::TourneyWrite) },
        )
        .await;
        // Under a wrong one, it says so.
        let wrong = tokio::spawn(run_admitted(
            Admission::Concurrent,
            Turn::default(),
            async { expect_admitted(Key::TourneyWrite) },
        ))
        .await;
        assert!(
            wrong.is_err(),
            "a concurrent command reached a serial write unnoticed"
        );
    }

    #[tokio::test]
    async fn a_command_that_ends_its_turn_lets_the_next_one_run_before_it_finishes() {
        // A write, then reads: the next write waits for the write only.
        let admission = CommandAdmission::default();
        let first = admission
            .admit(Admission::Serial(Key::GuidesVerdict))
            .unwrap();
        let mut second = admission
            .admit(Admission::Serial(Key::GuidesVerdict))
            .unwrap();
        let (reads_go, reads_wait) = oneshot::channel::<()>();
        let running = tokio::spawn(run_admitted(
            Admission::Serial(Key::GuidesVerdict),
            first,
            async move {
                end_turn();
                // Still admitted under its key for the rest of its work.
                expect_admitted(Key::GuidesVerdict);
                let _ = reads_wait.await;
            },
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), second.ready())
            .await
            .expect("the second runs while the first is still reading");
        let _ = reads_go.send(());
        running.await.expect("the first finishes");
        // Outside a command it does nothing, and says nothing.
        end_turn();
    }

    #[tokio::test]
    async fn a_dropped_turn_does_not_hold_up_the_next() {
        let admission = CommandAdmission::default();
        let first = admission.admit(Admission::Serial(Key::ClanWrite)).unwrap();
        let mut second = admission.admit(Admission::Serial(Key::ClanWrite)).unwrap();
        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(1), second.ready())
            .await
            .expect("the second runs once the first is gone");
    }
}
