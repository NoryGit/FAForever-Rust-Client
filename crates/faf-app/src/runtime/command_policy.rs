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
//! [`policy`] names, for every command there is, the lane it waits in, whether
//! it may run alongside its own kind, and which half of a start/release
//! [`Pair`] it is. The match has no wildcard: a new command does not compile
//! until somebody has decided. [`CommandAdmission`] enforces the admission, for
//! commands from the webview and, through [`super::run_command`], for the ones
//! a service starts itself; the runtime's `ReleaseOrder` enforces the pairs.
//!
//! Two concerns stay with the services and are not in this table:
//!
//! - **Stale answers.** Latest admission drops an older future, but concurrent
//!   reads and effects that outlive their caller still need response/commit
//!   ordering. Services keep their [`super::LatestRequest`] generations for it.
//! - **Guards over part of a command.** A few commands hold a guard across only
//!   some of their work, or across a connection that outlives them. Those are
//!   named here as [`Admission::ServiceGuarded`], with the reason on
//!   [`ServiceGuard`], so the table still tells the whole story.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use faf_domain::state::{
    AuthCommand, ChangelogCommand, ChatCommand, ClanCommand, ClientUpdateCommand,
    ConnectivityCommand, CoopCommand, EventsCommand, GalacticWarCommand, GuidesCommand,
    LeaderboardCommand, LinuxRuntimeCommand, LobbyCommand, MapGeneratorCommand, MapsCommand,
    ModsCommand, NavCommand, NotificationCommand, PlayerCardCommand, ReplayCommand,
    ReportingCommand, ReviewsCommand, SessionCommand, SettingsCommand, SocialCommand,
    StreamsCommand, TourneyCommand, TourneyRead, TrainingCommand, TutorialsCommand,
    UnitHistoryCommand, UploadsCommand,
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
    /// Contact-name lookup retries are single-flight.
    Contacts,
    /// Keep the local relation and its outgoing frames in the same order.
    ContactWrite,
    /// Replaceable reads: a newer query drops the older request future.
    ReplaySearch,
    MapSearch,
    ModSearch,
    RatingSearch,
    AccountLookup,
    /// Preserve the dispatch order of locally drafted calendar filters.
    EventQuery,
    /// The whole map catalogue crawl.
    MapVault,
    /// The whole mod catalogue crawl.
    ModVault,
    /// "Check for updates" on installed mods: a read of the mod catalogue that
    /// has to happen even while a background load is running. Its own key, so
    /// that load cannot drop it; it then waits for that load inside the
    /// service (`ModsContext::catalogue_writer`).
    ModVaultCheck,
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
    /// Reading the client's own release notes. Apart from `ClientUpdate`, so
    /// the changelog is not refused while an installer downloads; a second
    /// load while one asks GitHub would only ask again.
    ClientReleaseNotes,
    /// Installing and launching Galactic War share a staging directory.
    GalacticWar,
    /// Checking and installing the Linux runtime: one run owns its directory
    /// and its record of what is installed.
    LinuxRuntime,
    /// A GitHub device-flow sign-in: a second one would issue a second code
    /// and leave the one on screen dead.
    GuidesSignIn,
    /// Accepting and rejecting guide submissions, which each read and patch
    /// the catalogue.
    GuidesVerdict,
    /// Launching a tutorial or a catalogue lesson.
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
    /// The connectivity check. A second run while one is asking the relays
    /// would only ask them again, and its lines would interleave with the
    /// first run's in one list.
    ConnectivityCheck,
    /// The live relay view's `status` call. The page asks on a timer; a call
    /// the adapter is slow to answer must not stack up behind itself.
    RelayStatus,
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
    /// Only the latest read for this key is useful; cancel its predecessor.
    Latest(Key),
    /// Concurrent here; the service holds the guard named. See [`ServiceGuard`].
    ServiceGuarded(ServiceGuard),
}

/// What both halves of a start/release pair act on.
///
/// Releases travel in the priority lane and starts in the ordinary one, so a
/// release can overtake a start that was sent before it. Without the pair,
/// "Play" then "Stop" under load ran the stop first, found nothing to stop,
/// and then ran the start, leaving the player queued after they had pressed
/// Stop. The runtime's `ReleaseOrder` drops a start that leaves its queue after
/// a release of the same pair was dispatched.
///
/// Background work that can run for several items at once is keyed by the
/// item, so calling off one map's install never drops another's.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Pair {
    /// Joining or hosting a game.
    Join,
    /// The lobby socket.
    Lobby,
    /// A matchmaker search, by queue name.
    Matchmake(String),
    /// The chat socket.
    Chat,
    /// Signing in.
    Auth,
    /// The guides' GitHub device-flow sign-in.
    GuidesSignIn,
    MapGenerator,
    /// Starting a replay: from the vault, live, or from a file.
    ReplayWatch,
    LiveTracking,
    /// A replay downloaded into the library, by uid.
    ReplayDownload(i32),
    /// A map install, by folder name in lower case: the cancel may come in any
    /// letter case.
    MapInstall(String),
    /// A mod install or update, by uid.
    ModInstall(String),
    Upload,
    ClientDownload,
    /// The map catalogue load.
    MapVault,
    /// The mod catalogue load and "Check for updates", which share one
    /// cancellation.
    ModVault,
    /// A catalogue lesson's launch.
    LessonLaunch,
    /// A tutorial's launch. Apart from [`Self::LessonLaunch`]: each has a
    /// cancel of its own, which reaches only its own kind.
    TutorialLaunch,
    /// Galactic War's install, alone or as the first step of Play.
    GalacticWarInstall,
    /// Checking and setting up the Linux runtime.
    LinuxRuntime,
}

/// Which half of a [`Pair`] a command is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PairHalf {
    /// Begins the work the pair's release calls off.
    Start(Pair),
    /// Calls the work off. A start of the same pair dispatched before it and
    /// still waiting in the ordinary queue is dropped there, not run late.
    Release(Pair),
}

/// Why a priority command is no pair's release.
///
/// The priority lane has no shorthand without a pair: a command takes it as a
/// [`release`], or through [`unpaired_priority`] with one of these. Six cancels
/// once took the lane with no pair, each overtaking its own start under load,
/// finding nothing to stop, and letting the start run after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unpaired {
    /// Navigation only changes what is on screen. It calls nothing off, so a
    /// command it overtakes is not undone by running after it.
    Navigation,
    /// `Lobby::TerminateGame` ends the game process that runs now, whichever
    /// command started it: a join or host, an offline game, a replay, a
    /// tutorial, a lesson, Galactic War. It belongs to no one start.
    RunningGame,
    /// `Replays::CancelReads` is ordered against the reads it calls off by the
    /// runtime's replay read slots (`ReleaseOrder`), keyed by replay and panel.
    ReplayReads,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommandPolicy {
    pub(crate) lane: Lane,
    pub(crate) admission: Admission,
    pub(crate) pair: Option<PairHalf>,
}

impl CommandPolicy {
    /// The same policy, as the start of `pair`.
    fn starts(self, pair: Pair) -> Self {
        Self {
            pair: Some(PairHalf::Start(pair)),
            ..self
        }
    }
}

const ORDINARY: CommandPolicy = CommandPolicy {
    lane: Lane::Ordinary,
    admission: Admission::Concurrent,
    pair: None,
};

/// A command that calls off the work `pair`'s start begins: in the priority
/// lane, so it never waits behind that work, and the release of the pair, so
/// a start it overtakes is dropped instead of run after it.
fn release(pair: Pair) -> CommandPolicy {
    CommandPolicy {
        lane: Lane::Priority,
        admission: Admission::Concurrent,
        pair: Some(PairHalf::Release(pair)),
    }
}

/// The priority lane for a command that releases no pair, with the reason.
const fn unpaired_priority(_why: Unpaired) -> CommandPolicy {
    CommandPolicy {
        lane: Lane::Priority,
        admission: Admission::Concurrent,
        pair: None,
    }
}

const fn single_flight(key: Key) -> CommandPolicy {
    CommandPolicy {
        lane: Lane::Ordinary,
        admission: Admission::SingleFlight(key),
        pair: None,
    }
}

const fn serial(key: Key) -> CommandPolicy {
    CommandPolicy {
        lane: Lane::Ordinary,
        admission: Admission::Serial(key),
        pair: None,
    }
}

const fn latest(key: Key) -> CommandPolicy {
    CommandPolicy {
        lane: Lane::Ordinary,
        admission: Admission::Latest(key),
        pair: None,
    }
}

const fn service_guarded(guard: ServiceGuard) -> CommandPolicy {
    CommandPolicy {
        lane: Lane::Ordinary,
        admission: Admission::ServiceGuarded(guard),
        pair: None,
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
        AppCommand::UnitHistory(command) => unit_history(command),
        AppCommand::Events(command) => events(command),
        AppCommand::Uploads(command) => uploads(command),
        AppCommand::GalacticWar(command) => galactic_war(command),
        AppCommand::LinuxRuntime(command) => linux_runtime(command),
        AppCommand::Guides(command) => guides(command),
        AppCommand::ClientUpdate(command) => client_update(command),
        AppCommand::Settings(command) => settings(command),
        AppCommand::Connectivity(command) => connectivity(command),
    }
}

fn session(command: &SessionCommand) -> CommandPolicy {
    match command {
        SessionCommand::Hello => ORDINARY,
    }
}

fn auth(command: &AuthCommand) -> CommandPolicy {
    match command {
        AuthCommand::Login { .. } | AuthCommand::Restore => {
            service_guarded(ServiceGuard::Login).starts(Pair::Auth)
        }
        AuthCommand::CancelLogin => release(Pair::Auth),
        AuthCommand::PlayOffline | AuthCommand::LaunchOfflineGame => ORDINARY,
        AuthCommand::LoginTest => ORDINARY.starts(Pair::Auth),
        // In the ordinary lane, behind the test sign-in it ends; still the
        // release, so a test sign-in queued before it is dropped.
        AuthCommand::LogoutTest => CommandPolicy {
            pair: Some(PairHalf::Release(Pair::Auth)),
            ..ORDINARY
        },
        AuthCommand::Logout => CommandPolicy {
            admission: Admission::ServiceGuarded(ServiceGuard::Login),
            ..release(Pair::Auth)
        },
    }
}

fn nav(command: &NavCommand) -> CommandPolicy {
    match command {
        NavCommand::Select { .. }
        | NavCommand::SelectMapsSection { .. }
        | NavCommand::SelectModsSection { .. }
        | NavCommand::SelectReplaysSection { .. }
        | NavCommand::SelectSettingsSection { .. }
        | NavCommand::SelectChangelogSection { .. }
        | NavCommand::SelectChangelogClientRelease { .. }
        | NavCommand::OpenLink { .. }
        | NavCommand::LinkShown { .. } => unpaired_priority(Unpaired::Navigation),
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
        ChatCommand::Connect { .. } => {
            service_guarded(ServiceGuard::ChatConnection).starts(Pair::Chat)
        }
        ChatCommand::SendMessage { .. }
        | ChatCommand::JoinChannel { .. }
        | ChatCommand::LeaveChannel { .. }
        | ChatCommand::SelectChannel { .. }
        | ChatCommand::SetShowJoinsParts { .. }
        | ChatCommand::SetTyping { .. }
        | ChatCommand::React { .. }
        | ChatCommand::Unreact { .. } => ORDINARY,
        ChatCommand::Disconnect => release(Pair::Chat),
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
        LobbyCommand::Matchmake {
            queue_name,
            start: false,
        } => release(Pair::Matchmake(queue_name.clone())),
        LobbyCommand::Matchmake {
            queue_name,
            start: true,
        } => ORDINARY.starts(Pair::Matchmake(queue_name.clone())),
        LobbyCommand::Connect => service_guarded(ServiceGuard::LobbyConnection).starts(Pair::Lobby),
        LobbyCommand::Join { .. } => service_guarded(ServiceGuard::LobbyJoin).starts(Pair::Join),
        LobbyCommand::Host { .. } => ORDINARY.starts(Pair::Join),
        LobbyCommand::PrepareHost { .. }
        | LobbyCommand::ClearHostPrefill
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
        LobbyCommand::DeclineModReplacement | LobbyCommand::CancelJoin => release(Pair::Join),
        LobbyCommand::TerminateGame => unpaired_priority(Unpaired::RunningGame),
        LobbyCommand::Disconnect => release(Pair::Lobby),
    }
}

fn replays(command: &ReplayCommand) -> CommandPolicy {
    match command {
        ReplayCommand::SearchVault { .. } => latest(Key::ReplaySearch),
        ReplayCommand::WatchLive(..)
        | ReplayCommand::OpenFile { .. }
        | ReplayCommand::WatchVault { .. } => ORDINARY.starts(Pair::ReplayWatch),
        ReplayCommand::TrackLive { .. } => ORDINARY.starts(Pair::LiveTracking),
        ReplayCommand::DownloadVault { uid } => ORDINARY.starts(Pair::ReplayDownload(*uid)),
        ReplayCommand::LoadFeaturedMods
        | ReplayCommand::LoadRecentMatchmaker
        | ReplayCommand::LoadDetails { .. }
        | ReplayCommand::LoadAnalysis { .. }
        | ReplayCommand::LoadLocal { .. }
        | ReplayCommand::DeleteLocal { .. }
        | ReplayCommand::ResolveMaps { .. }
        | ReplayCommand::ResolveSimMods { .. }
        | ReplayCommand::LookUpOnline { .. }
        | ReplayCommand::LookUpOnlineMany { .. } => ORDINARY,
        ReplayCommand::CancelLiveTracking => release(Pair::LiveTracking),
        ReplayCommand::CancelWatch => release(Pair::ReplayWatch),
        ReplayCommand::CancelDownload { uid } => release(Pair::ReplayDownload(*uid)),
        ReplayCommand::CancelReads { .. } => unpaired_priority(Unpaired::ReplayReads),
    }
}

fn maps(command: &MapsCommand) -> CommandPolicy {
    match command {
        MapsCommand::CancelVaultLoad => release(Pair::MapVault),
        MapsCommand::LoadVault => single_flight(Key::MapVault).starts(Pair::MapVault),
        MapsCommand::SearchVault { .. } => latest(Key::MapSearch),
        MapsCommand::LoadInstalled
        | MapsCommand::ResolveVaultFolders { .. }
        | MapsCommand::ResolveVaultNames { .. }
        | MapsCommand::LoadLocalPreviews { .. }
        | MapsCommand::LoadMatchmakerPools { .. }
        | MapsCommand::SetMapVersionHidden { .. } => ORDINARY,
        MapsCommand::InstallMap { folder_name, .. } => {
            serial(Key::MapFiles).starts(Pair::MapInstall(folder_name.to_ascii_lowercase()))
        }
        MapsCommand::UninstallMap { .. } => serial(Key::MapFiles),
        // Not under `MapFiles`: it has to reach the install holding that key.
        MapsCommand::CancelInstall { folder_name } => {
            release(Pair::MapInstall(folder_name.to_ascii_lowercase()))
        }
    }
}

fn map_generator(command: &MapGeneratorCommand) -> CommandPolicy {
    match command {
        MapGeneratorCommand::GenerateNamed { .. } | MapGeneratorCommand::Generate { .. } => {
            single_flight(Key::MapGenerator).starts(Pair::MapGenerator)
        }
        MapGeneratorCommand::CleanUp => single_flight(Key::MapGenerator),
        MapGeneratorCommand::LoadOptions { .. }
        | MapGeneratorCommand::SetOptions { .. }
        | MapGeneratorCommand::Validate { .. }
        | MapGeneratorCommand::Preflight { .. }
        | MapGeneratorCommand::DecodeNames { .. }
        | MapGeneratorCommand::LoadPreviews { .. }
        | MapGeneratorCommand::LoadHelp { .. }
        | MapGeneratorCommand::SavePreset { .. }
        | MapGeneratorCommand::LoadPresets
        | MapGeneratorCommand::RenamePreset { .. }
        | MapGeneratorCommand::DeletePreset { .. } => ORDINARY,
        MapGeneratorCommand::Cancel => release(Pair::MapGenerator),
    }
}

fn mods(command: &ModsCommand) -> CommandPolicy {
    match command {
        ModsCommand::SearchVault { .. } => latest(Key::ModSearch),
        ModsCommand::CancelVaultLoad => release(Pair::ModVault),
        // A second load while one runs is dropped: every tab that shows mods
        // asks on mount, and queued they each ran a crawl of their own. A
        // manual check has its own key, so a running load cannot drop it; it
        // waits for that load in the service, then makes its own request.
        ModsCommand::LoadVault => single_flight(Key::ModVault).starts(Pair::ModVault),
        ModsCommand::ReloadVault => single_flight(Key::ModVaultCheck).starts(Pair::ModVault),
        ModsCommand::LoadInstalled | ModsCommand::QueryDownloadSizes { .. } => ORDINARY,
        ModsCommand::InstallMod { uid, .. } | ModsCommand::UpdateMod { uid, .. } => {
            serial(Key::ModFiles).starts(Pair::ModInstall(uid.clone()))
        }
        ModsCommand::UninstallMod { .. }
        | ModsCommand::ToggleMod { .. }
        | ModsCommand::SetActiveMods { .. } => serial(Key::ModFiles),
        // Not under `ModFiles`, for the same reason as the map vault's.
        ModsCommand::CancelInstall { uid } => release(Pair::ModInstall(uid.clone())),
    }
}

fn leaderboard(command: &LeaderboardCommand) -> CommandPolicy {
    match command {
        LeaderboardCommand::LoadRatings { .. } => latest(Key::RatingSearch),
        LeaderboardCommand::SetMode { .. }
        | LeaderboardCommand::LoadCatalog
        | LeaderboardCommand::SelectLeague { .. }
        | LeaderboardCommand::SelectSeason { .. } => ORDINARY,
    }
}

fn player_card(command: &PlayerCardCommand) -> CommandPolicy {
    match command {
        PlayerCardCommand::LookUpAccounts { .. } => latest(Key::AccountLookup),
        PlayerCardCommand::Open { .. }
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
        // Concurrent: a read for an earlier game id is superseded by the
        // service's `log_generation`, not queued behind, so changing the game
        // id while a log is read answers for the new one. Unticking stays in
        // the same lane as ticking on purpose: a priority lane would let an
        // untick overtake a tick still queued, which would then tick the box
        // again behind the user's back.
        ReportingCommand::AttachLog { .. } | ReportingCommand::DetachLog => ORDINARY,
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
        SocialCommand::SetContactsOpen { .. } => ORDINARY,
        SocialCommand::ResolveContacts => single_flight(Key::Contacts),
        SocialCommand::SetRelation { .. } => serial(Key::ContactWrite),
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

fn connectivity(command: &ConnectivityCommand) -> CommandPolicy {
    match command {
        ConnectivityCommand::RunCheck => single_flight(Key::ConnectivityCheck),
        ConnectivityCommand::RefreshRelayStatus => single_flight(Key::RelayStatus),
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
        // A lesson starts a game in the slot a tutorial uses, so the two share
        // one key: one launch at a time, whichever kind it is.
        TrainingCommand::LaunchLesson { .. } => {
            single_flight(Key::TutorialLaunch).starts(Pair::LessonLaunch)
        }
        TrainingCommand::CancelLessonLaunch => release(Pair::LessonLaunch),
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
        TutorialsCommand::CancelLaunch => release(Pair::TutorialLaunch),
        TutorialsCommand::Load | TutorialsCommand::Select { .. } => ORDINARY,
        TutorialsCommand::Launch { .. } => {
            single_flight(Key::TutorialLaunch).starts(Pair::TutorialLaunch)
        }
    }
}

fn changelog(command: &ChangelogCommand) -> CommandPolicy {
    match command {
        ChangelogCommand::Load => single_flight(Key::Changelog),
        ChangelogCommand::Select { .. } | ChangelogCommand::Search { .. } => ORDINARY,
    }
}

fn events(command: &EventsCommand) -> CommandPolicy {
    match command {
        EventsCommand::SetQuery { .. } => serial(Key::EventQuery),
        EventsCommand::Load
        | EventsCommand::SetView { .. }
        | EventsCommand::SetAnchor { .. }
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
        UploadsCommand::Start => single_flight(Key::Upload).starts(Pair::Upload),
        UploadsCommand::Cancel => release(Pair::Upload),
    }
}

fn galactic_war(command: &GalacticWarCommand) -> CommandPolicy {
    match command {
        GalacticWarCommand::CancelInstall => release(Pair::GalacticWarInstall),
        GalacticWarCommand::Refresh | GalacticWarCommand::RefreshStatistics => ORDINARY,
        // Play installs first when it has to, and a cancelled install stops
        // its launch too, so a Play the cancel overtook must not run either.
        GalacticWarCommand::Install | GalacticWarCommand::Play => {
            single_flight(Key::GalacticWar).starts(Pair::GalacticWarInstall)
        }
    }
}

fn linux_runtime(command: &LinuxRuntimeCommand) -> CommandPolicy {
    match command {
        LinuxRuntimeCommand::Prepare => single_flight(Key::LinuxRuntime).starts(Pair::LinuxRuntime),
        LinuxRuntimeCommand::Cancel => release(Pair::LinuxRuntime),
    }
}

/// All concurrent: reading the unit list twice costs a second read of a
/// local file, and a unit chosen twice is built once by the service itself
/// (`UnitHistoryContext::building`).
fn unit_history(command: &UnitHistoryCommand) -> CommandPolicy {
    match command {
        UnitHistoryCommand::LoadList
        | UnitHistoryCommand::Select { .. }
        | UnitHistoryCommand::SetSelection { .. } => ORDINARY,
    }
}

fn guides(command: &GuidesCommand) -> CommandPolicy {
    match command {
        GuidesCommand::Restore
        | GuidesCommand::SignOut
        | GuidesCommand::LoadQueue
        | GuidesCommand::Submit { .. } => ORDINARY,
        GuidesCommand::SignIn => single_flight(Key::GuidesSignIn).starts(Pair::GuidesSignIn),
        GuidesCommand::CancelSignIn => release(Pair::GuidesSignIn),
        GuidesCommand::Accept { .. } | GuidesCommand::Reject { .. } => serial(Key::GuidesVerdict),
    }
}

fn client_update(command: &ClientUpdateCommand) -> CommandPolicy {
    match command {
        ClientUpdateCommand::Download => {
            single_flight(Key::ClientUpdate).starts(Pair::ClientDownload)
        }
        ClientUpdateCommand::Check | ClientUpdateCommand::Install => {
            single_flight(Key::ClientUpdate)
        }
        ClientUpdateCommand::LoadNotes => single_flight(Key::ClientReleaseNotes),
        // `DismissWhatsNew` writes one settings field, under the settings
        // service's own locks (`ServiceGuard::Settings`).
        ClientUpdateCommand::Dismiss | ClientUpdateCommand::DismissWhatsNew => ORDINARY,
        // Outside `ClientUpdate`, which the download it stops is holding.
        ClientUpdateCommand::CancelDownload => release(Pair::ClientDownload),
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
        | SettingsCommand::SaveSearchPreset { .. }
        | SettingsCommand::DeleteSearchPreset { .. }
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
pub(crate) async fn run_admitted<F: std::future::Future<Output = ()>>(
    admission: Admission,
    turn: Turn,
    work: F,
) {
    let cancelled = turn.cancelled.clone();
    let slot = Arc::new(Mutex::new(Some(turn)));
    let run = async move {
        if let Some(cancelled) = cancelled {
            tokio::select! {
                biased;
                () = cancelled.cancelled() => {},
                () = work => {},
            }
        } else {
            work.await;
        }
    };
    TURN.scope(slot.clone(), ADMITTED_AS.scope(admission, run))
        .await;
    drop(slot.lock().unwrap_or_else(PoisonError::into_inner).take());
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

/// Enforces single-flight, serial and latest-read admission.
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
    /// Claimed before task spawn so an unpolled old read cannot cancel a newer one.
    latest: Mutex<HashMap<Key, tokio_util::sync::CancellationToken>>,
}

impl CommandAdmission {
    /// A turn to run under `admission`, or `None` when a single-flight command
    /// of the same kind is already running and this one is dropped.
    pub(crate) fn admit(&self, admission: Admission) -> Option<Turn> {
        match admission {
            Admission::Concurrent | Admission::ServiceGuarded(_) => Some(Turn::default()),
            Admission::Latest(key) => {
                let cancelled = tokio_util::sync::CancellationToken::new();
                let mut latest = self.latest.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(previous) = latest.insert(key, cancelled.clone()) {
                    previous.cancel();
                }
                Some(Turn {
                    cancelled: Some(cancelled),
                    wait: None,
                    done: None,
                    flight: None,
                })
            }
            Admission::SingleFlight(key) => {
                let mut running = self.running.lock().unwrap_or_else(PoisonError::into_inner);
                if !running.insert(key) {
                    return None;
                }
                Some(Turn {
                    wait: None,
                    done: None,
                    flight: Some((self.running.clone(), key)),
                    cancelled: None,
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
                    cancelled: None,
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
    cancelled: Option<tokio_util::sync::CancellationToken>,
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
mod documentation {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    const START: &str = "<!-- BEGIN GENERATED COMMAND POLICY -->";
    const END: &str = "<!-- END GENERATED COMMAND POLICY -->";
    const REGENERATE: &str =
        "cargo test -p faf-app --lib regenerate_concurrency_tables -- --ignored";

    #[derive(Debug, Default, PartialEq, Eq)]
    struct Description {
        lane: String,
        admission: String,
        key: Option<String>,
        guard: Option<String>,
        pair: Option<(String, String)>,
    }

    struct Row {
        command: String,
        policy: Description,
    }

    // Inspect the exhaustive match, not a second catalogue of example commands.
    // Like conformance_fixtures::declared_variants, this deliberately accepts a
    // small source grammar. Unsupported syntax fails instead of omitting rows.
    fn compact(source: &str) -> String {
        source.chars().filter(|c| !c.is_whitespace()).collect()
    }

    fn identifier(source: &str) -> &str {
        source
            .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
            .next()
            .unwrap()
    }

    fn closing(source: &str) -> usize {
        let mut depth = 0;
        for (index, byte) in source.bytes().enumerate() {
            match byte {
                b'{' | b'(' | b'[' => depth += 1,
                b'}' | b')' | b']' => {
                    depth -= 1;
                    if depth == 0 {
                        return index;
                    }
                }
                _ => {}
            }
        }
        panic!("Unbalanced policy source; update the documentation renderer");
    }

    fn top_level(source: &str, separator: char) -> Vec<&str> {
        let mut depth = 0;
        let mut start = 0;
        let mut parts = Vec::new();
        for (index, character) in source.char_indices() {
            match character {
                '{' | '(' | '[' => depth += 1,
                '}' | ')' | ']' => depth -= 1,
                _ => {}
            }
            if depth == 0 && character == separator {
                parts.push(source[start..index].trim());
                start = index + character.len_utf8();
            }
        }
        parts.push(source[start..].trim());
        parts
    }

    fn enum_variants(source: &str, name: &str) -> Vec<String> {
        let declaration = format!("enum {name} {{");
        let body = source.split_once(&declaration).unwrap().1;
        let end = closing(&format!("{{{body}")) - 1;
        top_level(&body[..end], ',')
            .into_iter()
            .filter(|part| !part.is_empty())
            .map(|part| identifier(part).to_owned())
            .collect()
    }

    fn argument<'a>(source: &'a str, call: &str) -> Option<&'a str> {
        let rest = source.split_once(call)?.1;
        Some(&rest[..closing(&format!("({rest}")) - 1])
    }

    fn describe(expression: &str) -> Description {
        let expression = expression.trim().trim_end_matches(';');
        if expression.starts_with('{') {
            return describe(&expression[1..closing(expression)]);
        }
        let expression = compact(expression);
        if let Some(body) = expression.strip_prefix("CommandPolicy{") {
            let fields = top_level(&body[..body.len() - 1], ',');
            let mut result = fields
                .iter()
                .find_map(|field| field.strip_prefix(".."))
                .map(describe)
                .unwrap_or_default();
            for field in fields {
                if field.is_empty() || field.starts_with("..") {
                    continue;
                }
                if let Some(value) = field.strip_prefix("lane:Lane::") {
                    result.lane = identifier(value).into();
                } else if let Some(value) = field.strip_prefix("admission:Admission::") {
                    result.admission = identifier(value).into();
                    result.guard = argument(value, "ServiceGuarded(ServiceGuard::")
                        .map(|s| identifier(s).into());
                    result.key = argument(value, &format!("{}(Key::", result.admission))
                        .map(|s| identifier(s).into());
                } else if let Some(value) = field.strip_prefix("pair:") {
                    result.pair = argument(value, "PairHalf::Release(Pair::")
                        .map(|s| ("Release".into(), identifier(s).into()));
                    assert!(
                        value == "None" || result.pair.is_some(),
                        "Unsupported pair field: {value}"
                    );
                } else {
                    panic!("Unsupported policy field: {field}");
                }
            }
            return result;
        }
        let base = expression.split(".starts(").next().unwrap();
        if base != "ORDINARY" {
            let open = base.find('(').expect("Expected policy helper call");
            assert_eq!(
                open + closing(&base[open..]) + 1,
                base.len(),
                "Unsupported policy expression: {expression}"
            );
        }
        if let Some((_, suffix)) = expression.split_once(".starts(") {
            assert_eq!(
                closing(&format!("({suffix}")),
                suffix.len(),
                "Unsupported policy method chain: {expression}"
            );
        }
        let mut result = Description {
            lane: "Ordinary".into(),
            admission: "Concurrent".into(),
            ..Default::default()
        };
        if expression.starts_with("release(") || expression.starts_with("unpaired_priority(") {
            result.lane = "Priority".into();
            result.pair = argument(&expression, "release(Pair::")
                .map(|s| ("Release".into(), identifier(s).into()));
        } else if expression.starts_with("single_flight(")
            || expression.starts_with("serial(")
            || expression.starts_with("latest(")
        {
            result.admission = if expression.starts_with("single_flight(") {
                "SingleFlight"
            } else if expression.starts_with("serial(") {
                "Serial"
            } else {
                "Latest"
            }
            .into();
            result.key = expression
                .split_once("Key::")
                .map(|(_, key)| identifier(key).into());
            assert!(result.key.is_some(), "Missing key in {expression}");
        } else if expression.starts_with("service_guarded(") {
            result.admission = "ServiceGuarded".into();
            result.guard = expression
                .split_once("ServiceGuard::")
                .map(|(_, guard)| identifier(guard).into());
            assert!(result.guard.is_some(), "Missing guard in {expression}");
        } else {
            assert!(
                base == "ORDINARY",
                "Unsupported policy expression: {expression}"
            );
        }
        if let Some(pair) = argument(&expression, ".starts(Pair::") {
            result.pair = Some(("Start".into(), identifier(pair).into()));
        }
        result
    }

    fn match_arms(body: &str) -> Vec<(&str, &str)> {
        let rest = body
            .strip_prefix("match command {")
            .expect("Policy functions must exhaustively match command");
        let mut rest = rest[..closing(&format!("{{{rest}")) - 1].trim();
        let mut arms = Vec::new();
        while !rest.is_empty() {
            let (pattern, expression) = rest.split_once("=>").expect("Policy match arm");
            let expression = expression.trim_start();
            let end = if expression.starts_with('{') {
                closing(expression) + 1
            } else {
                top_level(expression, ',')[0].len()
            };
            arms.push((pattern.trim(), expression[..end].trim()));
            rest = expression[end..]
                .trim_start()
                .trim_start_matches(',')
                .trim_start();
        }
        arms
    }

    fn rows(source: &str) -> Vec<Row> {
        let mut functions = BTreeMap::new();
        for function in source.split("fn ").skip(1) {
            let (signature, body) = function.split_once('{').unwrap();
            if signature.contains("command: &")
                && signature.trim_end().ends_with("-> CommandPolicy")
            {
                let end = closing(&format!("{{{body}")) - 1;
                functions.insert(identifier(signature), body[..end].trim());
            }
        }
        fn walk(functions: &BTreeMap<&str, &str>, name: &str, prefix: &str, output: &mut Vec<Row>) {
            for (patterns, expression) in match_arms(functions[name]) {
                for pattern in top_level(patterns, '|') {
                    let (_, variant) = pattern.split_once("::").unwrap();
                    let mut command = format!("{prefix}{}", identifier(variant));
                    if variant.contains("start:") {
                        command.push_str(if compact(variant).contains("start:false") {
                            " { start: false }"
                        } else {
                            " { start: true }"
                        });
                    }
                    let delegate = identifier(expression);
                    if functions.contains_key(delegate) {
                        walk(functions, delegate, &format!("{command}::"), output);
                    } else {
                        output.push(Row {
                            command,
                            policy: describe(expression),
                        });
                    }
                }
            }
        }
        let mut output = Vec::new();
        walk(&functions, "policy", "", &mut output);
        let commands: BTreeSet<_> = output.iter().map(|row| &row.command).collect();
        assert_eq!(
            commands.len(),
            output.len(),
            "Ambiguous policy patterns need explicit documentation labels"
        );
        output
    }

    fn command_list<'a>(rows: impl Iterator<Item = &'a Row>) -> String {
        let names: BTreeSet<_> = rows.map(|row| format!("`{}`", row.command)).collect();
        if names.is_empty() {
            "None".into()
        } else {
            names.into_iter().collect::<Vec<_>>().join(", ")
        }
    }

    fn render() -> String {
        // Scope the small source reader to declarations and exhaustive policy
        // matches, before admission machinery and this test module.
        let source = include_str!("command_policy.rs")
            .split("tokio::task_local!")
            .next()
            .unwrap();
        let source = source
            .lines()
            .map(|line| line.split("//").next().unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        let rows = rows(&source);
        let mut output = format!("{START}\n\nGenerated from `command_policy.rs`. Regenerate with `{REGENERATE}`. Counts refer to exhaustive match patterns; `Tourney::Write` covers every tournament write.\n\n### Lanes\n\n| Lane | Policy patterns |\n|---|---|\n");
        for lane in enum_variants(&source, "Lane") {
            output.push_str(&format!(
                "| `{lane}` | {} |\n",
                rows.iter().filter(|row| row.policy.lane == lane).count()
            ));
        }
        output.push_str("\n### Priority commands\n\n");
        let groups: BTreeSet<_> = rows
            .iter()
            .filter(|row| row.policy.lane == "Priority")
            .map(|row| row.command.split("::").next().unwrap())
            .collect();
        for group in groups {
            output.push_str(&format!(
                "- {}\n",
                command_list(rows.iter().filter(|row| row.policy.lane == "Priority"
                    && row.command.starts_with(&format!("{group}::"))))
            ));
        }
        output.push_str("\n### Admission kinds\n\n| Admission | Policy patterns |\n|---|---|\n");
        for admission in enum_variants(&source, "Admission") {
            output.push_str(&format!(
                "| `{admission}` | {} |\n",
                rows.iter()
                    .filter(|row| row.policy.admission == admission)
                    .count()
            ));
        }
        output.push_str("\n### Keys\n\n| Key | Admission | Commands |\n|---|---|---|\n");
        for key in enum_variants(&source, "Key") {
            let admissions: BTreeSet<_> = rows
                .iter()
                .filter(|row| row.policy.key.as_ref() == Some(&key))
                .map(|row| format!("`{}`", row.policy.admission))
                .collect();
            output.push_str(&format!(
                "| `{key}` | {} | {} |\n",
                admissions.into_iter().collect::<Vec<_>>().join(", "),
                command_list(
                    rows.iter()
                        .filter(|row| row.policy.key.as_ref() == Some(&key))
                )
            ));
        }
        output.push_str("\n### Service guards\n\n| Guard | Commands |\n|---|---|\n");
        for guard in enum_variants(&source, "ServiceGuard") {
            output.push_str(&format!(
                "| `{guard}` | {} |\n",
                command_list(
                    rows.iter()
                        .filter(|row| row.policy.guard.as_ref() == Some(&guard))
                )
            ));
        }
        output.push_str("\n### Start/release pairs\n\n`Auth::LogoutTest` releases its pair in the ordinary lane; the other releases below use priority. Payload-dependent pair keys remain defined by the policy expressions and the cancellation prose above.\n\n| Pair | Starts | Releases |\n|---|---|---|\n");
        for pair in enum_variants(&source, "Pair") {
            let half =
                |half: &str| {
                    command_list(rows.iter().filter(|row| {
                        row.policy.pair.as_ref() == Some(&(half.into(), pair.clone()))
                    }))
                };
            output.push_str(&format!(
                "| `{pair}` | {} | {} |\n",
                half("Start"),
                half("Release")
            ));
        }
        output.push_str(&format!("\n{END}"));
        output
    }

    fn document() -> (std::path::PathBuf, String) {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/CONCURRENCY.md");
        let text = std::fs::read_to_string(&path)
            .unwrap()
            .replace("\r\n", "\n");
        (path, text)
    }

    #[test]
    fn policy_documentation_understands_the_runtime_helpers() {
        fn actual(policy: CommandPolicy) -> Description {
            let mut result = Description {
                lane: format!("{:?}", policy.lane),
                ..Default::default()
            };
            match policy.admission {
                Admission::Concurrent => result.admission = "Concurrent".into(),
                Admission::SingleFlight(key) => {
                    result.admission = "SingleFlight".into();
                    result.key = Some(format!("{key:?}"));
                }
                Admission::Serial(key) => {
                    result.admission = "Serial".into();
                    result.key = Some(format!("{key:?}"));
                }
                Admission::Latest(key) => {
                    result.admission = "Latest".into();
                    result.key = Some(format!("{key:?}"));
                }
                Admission::ServiceGuarded(guard) => {
                    result.admission = "ServiceGuarded".into();
                    result.guard = Some(format!("{guard:?}"));
                }
            }
            result.pair = policy.pair.map(|half| match half {
                PairHalf::Start(pair) => ("Start".into(), identifier(&format!("{pair:?}")).into()),
                PairHalf::Release(pair) => {
                    ("Release".into(), identifier(&format!("{pair:?}")).into())
                }
            });
            result
        }
        for (expression, policy) in [
            ("ORDINARY", ORDINARY),
            ("single_flight(Key::Contacts)", single_flight(Key::Contacts)),
            ("serial(Key::Contacts)", serial(Key::Contacts)),
            ("latest(Key::Contacts)", latest(Key::Contacts)),
            ("service_guarded(ServiceGuard::Login)", service_guarded(ServiceGuard::Login)),
            ("release(Pair::Auth)", release(Pair::Auth)),
            ("unpaired_priority(Unpaired::Navigation)", unpaired_priority(Unpaired::Navigation)),
            ("ORDINARY.starts(Pair::Auth)", ORDINARY.starts(Pair::Auth)),
            ("CommandPolicy { pair: Some(PairHalf::Release(Pair::Auth)), ..ORDINARY }", super::policy(&AuthCommand::LogoutTest.into())),
            ("CommandPolicy { admission: Admission::ServiceGuarded(ServiceGuard::Login), ..release(Pair::Auth) }", super::policy(&AuthCommand::Logout.into())),
        ] {
            assert_eq!(describe(expression), actual(policy), "Documentation no longer matches runtime helper {expression}; update the renderer and run {REGENERATE}");
        }
    }

    #[test]
    fn concurrency_tables_match_the_command_policy() {
        let (_, text) = document();
        let (_, generated) = text
            .split_once(START)
            .expect("Missing generated policy start marker");
        let (generated, _) = generated
            .split_once(END)
            .expect("Missing generated policy end marker");
        assert_eq!(
            format!("{START}{generated}{END}"),
            render(),
            "Concurrency tables are stale. Regenerate with: {REGENERATE}"
        );
    }

    #[test]
    #[ignore = "writes the generated policy tables in docs/CONCURRENCY.md"]
    fn regenerate_concurrency_tables() {
        let (path, text) = document();
        let (before, rest) = text
            .split_once(START)
            .expect("Missing generated policy start marker");
        let (_, after) = rest
            .split_once(END)
            .expect("Missing generated policy end marker");
        std::fs::write(path, format!("{before}{}{after}", render())).unwrap();
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

    /// Both halves of every pair are in the table, under the same key: a pair
    /// with only one half there drops nothing.
    ///
    /// `halves` has no wildcard, so a new pair does not compile until it names
    /// a start and a release here (list it in `every_pair` too). The priority
    /// lane has no shorthand without a pair (see [`Unpaired`]), so a new cancel
    /// cannot take it unpaired by accident.
    #[test]
    fn every_pair_has_a_start_and_a_release_under_the_same_key() {
        use faf_domain::state::{LiveReplayTarget, LiveReplayTrackingAction};

        fn halves(pair: &Pair) -> (AppCommand, AppCommand) {
            match pair {
                Pair::Join => (
                    LobbyCommand::Join {
                        id: 42,
                        password: None,
                        replace_mods: false,
                    }
                    .into(),
                    LobbyCommand::CancelJoin.into(),
                ),
                Pair::Lobby => (
                    LobbyCommand::Connect.into(),
                    LobbyCommand::Disconnect.into(),
                ),
                Pair::Matchmake(queue_name) => (
                    LobbyCommand::Matchmake {
                        queue_name: queue_name.clone(),
                        start: true,
                    }
                    .into(),
                    LobbyCommand::Matchmake {
                        queue_name: queue_name.clone(),
                        start: false,
                    }
                    .into(),
                ),
                Pair::Chat => (
                    ChatCommand::Connect {
                        username: "player".into(),
                    }
                    .into(),
                    ChatCommand::Disconnect.into(),
                ),
                Pair::Auth => (
                    AuthCommand::LoginTest.into(),
                    AuthCommand::CancelLogin.into(),
                ),
                Pair::GuidesSignIn => (
                    GuidesCommand::SignIn.into(),
                    GuidesCommand::CancelSignIn.into(),
                ),
                Pair::MapGenerator => (
                    MapGeneratorCommand::GenerateNamed {
                        map_name: "neroxis_map_generator_1.0.0_abc".into(),
                    }
                    .into(),
                    MapGeneratorCommand::Cancel.into(),
                ),
                Pair::ReplayWatch => (
                    ReplayCommand::WatchVault { uid: 7 }.into(),
                    ReplayCommand::CancelWatch.into(),
                ),
                Pair::LiveTracking => (
                    ReplayCommand::TrackLive {
                        target: LiveReplayTarget {
                            uid: 7,
                            mod_name: "faf".into(),
                            map: "scmp_009".into(),
                        },
                        action: LiveReplayTrackingAction::Notify,
                    }
                    .into(),
                    ReplayCommand::CancelLiveTracking.into(),
                ),
                Pair::ReplayDownload(uid) => (
                    ReplayCommand::DownloadVault { uid: *uid }.into(),
                    ReplayCommand::CancelDownload { uid: *uid }.into(),
                ),
                // The cancel may name the folder in another letter case.
                Pair::MapInstall(_) => (
                    MapsCommand::InstallMap {
                        folder_name: "Setons_Clutch".into(),
                        download_url: "https://example.invalid/setons_clutch.zip".into(),
                    }
                    .into(),
                    MapsCommand::CancelInstall {
                        folder_name: "SETONS_CLUTCH".into(),
                    }
                    .into(),
                ),
                Pair::ModInstall(uid) => (
                    ModsCommand::InstallMod {
                        uid: uid.clone(),
                        download_url: "https://example.invalid/mod.zip".into(),
                    }
                    .into(),
                    ModsCommand::CancelInstall { uid: uid.clone() }.into(),
                ),
                Pair::Upload => (UploadsCommand::Start.into(), UploadsCommand::Cancel.into()),
                Pair::ClientDownload => (
                    ClientUpdateCommand::Download.into(),
                    ClientUpdateCommand::CancelDownload.into(),
                ),
                Pair::MapVault => (
                    MapsCommand::LoadVault.into(),
                    MapsCommand::CancelVaultLoad.into(),
                ),
                Pair::ModVault => (
                    ModsCommand::ReloadVault.into(),
                    ModsCommand::CancelVaultLoad.into(),
                ),
                Pair::LessonLaunch => (
                    TrainingCommand::LaunchLesson {
                        resource_id: "lesson-1".into(),
                    }
                    .into(),
                    TrainingCommand::CancelLessonLaunch.into(),
                ),
                Pair::TutorialLaunch => (
                    TutorialsCommand::Launch { tutorial_id: 7 }.into(),
                    TutorialsCommand::CancelLaunch.into(),
                ),
                Pair::GalacticWarInstall => (
                    GalacticWarCommand::Play.into(),
                    GalacticWarCommand::CancelInstall.into(),
                ),
                Pair::LinuxRuntime => (
                    LinuxRuntimeCommand::Prepare.into(),
                    LinuxRuntimeCommand::Cancel.into(),
                ),
            }
        }

        let every_pair = [
            Pair::Join,
            Pair::Lobby,
            Pair::Matchmake("ladder1v1".into()),
            Pair::Chat,
            Pair::Auth,
            Pair::GuidesSignIn,
            Pair::MapGenerator,
            Pair::ReplayWatch,
            Pair::LiveTracking,
            Pair::ReplayDownload(7),
            Pair::MapInstall("setons_clutch".into()),
            Pair::ModInstall("mod-uid".into()),
            Pair::Upload,
            Pair::ClientDownload,
            Pair::MapVault,
            Pair::ModVault,
            Pair::LessonLaunch,
            Pair::TutorialLaunch,
            Pair::GalacticWarInstall,
            Pair::LinuxRuntime,
        ];
        for pair in every_pair {
            let (start, release) = halves(&pair);
            let (start, release) = (policy(&start), policy(&release));
            assert_eq!(start.pair, Some(PairHalf::Start(pair.clone())));
            assert_eq!(start.lane, Lane::Ordinary, "{pair:?}: a start waits");
            assert_eq!(release.pair, Some(PairHalf::Release(pair.clone())));
            assert_eq!(
                release.lane,
                Lane::Priority,
                "{pair:?}: a release overtakes"
            );
        }
    }

    #[test]
    fn the_guards_services_used_to_hold_are_in_the_table() {
        let admission = |command: AppCommand| policy(&command).admission;
        assert_eq!(
            admission(MapsCommand::LoadVault.into()),
            Admission::SingleFlight(Key::MapVault)
        );
        assert_eq!(
            admission(ModsCommand::LoadVault.into()),
            Admission::SingleFlight(Key::ModVault)
        );
        assert_eq!(
            admission(ModsCommand::ReloadVault.into()),
            Admission::SingleFlight(Key::ModVaultCheck)
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
    fn search_efficiency_only_replaceable_reads_use_latest_admission() {
        let cases = [
            (
                ReplayCommand::SearchVault {
                    query: Box::default(),
                }
                .into(),
                Key::ReplaySearch,
            ),
            (
                MapsCommand::SearchVault {
                    query: Default::default(),
                }
                .into(),
                Key::MapSearch,
            ),
            (
                ModsCommand::SearchVault {
                    query: Default::default(),
                }
                .into(),
                Key::ModSearch,
            ),
            (
                LeaderboardCommand::LoadRatings {
                    query: Default::default(),
                }
                .into(),
                Key::RatingSearch,
            ),
            (
                PlayerCardCommand::LookUpAccounts {
                    query: "player".into(),
                }
                .into(),
                Key::AccountLookup,
            ),
        ];
        for (command, key) in cases {
            assert_eq!(policy(&command).admission, Admission::Latest(key));
        }
        assert_eq!(
            policy(
                &EventsCommand::SetQuery {
                    query: Default::default()
                }
                .into()
            )
            .admission,
            Admission::Serial(Key::EventQuery),
            "draft echoes must follow dispatch order"
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
    async fn search_efficiency_superseded_unpolled_reads_never_start() {
        let admission = CommandAdmission::default();
        let first = admission.admit(Admission::Latest(Key::MapSearch)).unwrap();
        let second = admission.admit(Admission::Latest(Key::MapSearch)).unwrap();
        run_admitted(Admission::Latest(Key::MapSearch), first, async {
            panic!("the superseded port call must not start");
        })
        .await;
        run_admitted(Admission::Latest(Key::MapSearch), second, async {}).await;
        let map = admission.admit(Admission::Latest(Key::MapSearch)).unwrap();
        let _replay = admission
            .admit(Admission::Latest(Key::ReplaySearch))
            .unwrap();
        assert!(
            !map.cancelled.as_ref().unwrap().is_cancelled(),
            "different search areas stay independent"
        );
    }

    #[tokio::test]
    async fn search_efficiency_replacement_drops_the_running_port_future() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let admission = CommandAdmission::default();
        let first = admission
            .admit(Admission::Latest(Key::ReplaySearch))
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let observed = dropped.clone();
        let (entered, started) = oneshot::channel();
        let running = tokio::spawn(run_admitted(
            Admission::Latest(Key::ReplaySearch),
            first,
            async move {
                let _owned = Dropped(observed);
                entered.send(()).unwrap();
                std::future::pending::<()>().await;
            },
        ));
        started.await.unwrap();
        let _replacement = admission
            .admit(Admission::Latest(Key::ReplaySearch))
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), running)
            .await
            .unwrap()
            .unwrap();
        assert!(dropped.load(Ordering::SeqCst));
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
