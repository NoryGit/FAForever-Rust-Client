//! The concurrency contract, checked against the real runtime.
//!
//! `runtime/command_policy.rs` says, for every command, which lane it waits in
//! and whether it may run beside its own kind. Its unit tests check the table
//! and the admission type in isolation. These drive the whole loop: `App::new`
//! with fake ports, where the port a command reaches is replaced by one that
//! holds the call at a gate the test opens. That is what makes "while one runs"
//! exact rather than a matter of timing.
//!
//! The guarantees, one test each, every one over a table with a row per key:
//!
//! - single-flight: a second command of the same kind while one runs is
//!   dropped, never queued, and a command of another kind is not held up;
//! - serial: commands of one kind run one after another, in the order they
//!   were dispatched, and another kind is not held up;
//! - priority: a release or a navigation is not held up when every ordinary
//!   slot is busy and the ordinary queue is full.
//!
//! Below the table are the guards services hold themselves
//! (`ServiceGuard` in the policy): the lobby and chat sockets, sign-in and
//! settings writes. The join slot is pinned by `tests/lobby_operations.rs`.
//! `docs/CONCURRENCY.md` names the test behind each row.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use faf_app::infra::{
    fake_ports, FakeChat, FakeClan, FakeClientUpdates, FakeConnectivity, FakeGalacticWar,
    FakeGuides, FakeIce, FakeLobby, FakeMapGenerator, FakeMaps, FakeMods, FakePlayerCard,
    FakeTourney, FakeUploads,
};
use faf_app::ports::{
    AdapterInventory, AdapterLogTail, AuthPort, AuthResult, ChangelogPort, ChatPort, ChatUpdate,
    ClanPort, ClientUpdatePort, ConnectivityPort, ConnectivitySession, DeviceCode,
    DownloadProgress, GalacticWarPort, GamePreparation, GameUpdaterPort, GeneratorUpdate,
    GuidesPort, IceParams, IcePort, InstallProgress, KnownRelayAddresses, LobbyPort, LobbyUpdate,
    MapGeneratorPort, MapSearchPage, MapsPort, ModPrepFailure, ModSearchPage, ModsPort,
    PlayerCardPort, ProbeAnswer, RelayListError, RelayServer, RequestError, SettingsPort,
    TourneyMatchPort, UpdateProgress, UploadsPort,
};
use faf_app::{App, Ports};
use faf_domain::protocol::changelog::{ChangelogEntry, ChangelogRelease};
use faf_domain::protocol::stun::IceUrl;
use faf_domain::protocol::vault_query::{MapVaultQuery, ModVaultQuery};
use faf_domain::state::settings::{BrowsingPreferencesPatch, GamePreferencesPatch};
use faf_domain::state::{
    AuthCommand, AuthEvent, AuthMode, AuthStatus, BracketConfig, ChangelogCommand, ChatCommand,
    ChatEvent, ChatStatus, ClanCommand, ClanDraft, ClanIdentity, ClientRelease, ClientReleaseNotes,
    ClientUpdateCommand, ClientVersions, FfaReport, GalacticWarCommand, GalacticWarStatistics,
    GamePreferences, GeneratorOptionQuery, GeneratorOptions, GeneratorPreset, GeneratorStatus,
    GuideSubmission, GuidesCommand, GuidesIdentity, HostGameConfig, InstalledMap, InstalledMod,
    LobbyCommand, LobbyEvent, MapGeneratorCommand, MapsCommand, MatchReport, MatchmakerMapPool,
    MatchmakerPlayerProfile, ModDownloadSize, ModDownloadTarget, ModsCommand, NavCommand, Player,
    PlayerCardCommand, PlayerCardProfile, PlayerClan, PlayerLeaguePlacement, PlayerMapStats,
    PlayerSummary, PlayerVeto, RatingHistoryPage, RatingHistoryQuery, RejectReason, Relation,
    ReleaseChannel, ReplayCommand, SettingsCommand, SettingsState, Tab, TourneyPhase, TourneyWrite,
    TrainingResource, TutorialsCommand, UploadKind, UploadRequest, UploadStatus, UploadsCommand,
    VaultMap, VaultMod,
};
use faf_domain::state::{ConnectivityCommand, ProbeFailure, RelayStatus};
use faf_domain::{AppCommand, AppEvent, AppState};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::task::JoinHandle;

/// How long anything may take before the test calls it stuck. Only a safety
/// net: every wait below is for something a gate makes certain.
const PATIENCE: Duration = Duration::from_secs(5);

/// How long a command is given to do something it must not do (start while
/// another of its kind holds the key) before the test checks it did not. A
/// negative cannot be awaited, so this is the one place a short window stands
/// in for a gate.
const WINDOW: Duration = Duration::from_millis(100);

const HELD_POOL_PREFIX: &str = "saturation-pool-";

// ── Gates ───────────────────────────────────────────────────────────────────

/// Port calls held until the test lets them through, and the order they
/// arrived in.
///
/// A gate opened before anybody reaches it stays open for one caller: each
/// gate is a semaphore that starts empty, and opening adds a permit.
#[derive(Default, Clone)]
struct Gates(Arc<Mutex<GateBook>>);

#[derive(Default)]
struct GateBook {
    gates: HashMap<String, Arc<Semaphore>>,
    entered: Vec<String>,
}

impl Gates {
    fn gate(book: &mut GateBook, key: &str) -> Arc<Semaphore> {
        book.gates
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(0)))
            .clone()
    }

    /// Called by a port: record the call, then wait to be let through.
    async fn pass(&self, key: &str) {
        let gate = {
            let mut book = self.0.lock().unwrap();
            book.entered.push(key.to_string());
            Self::gate(&mut book, key)
        };
        gate.acquire().await.expect("gate closed").forget();
    }

    fn open(&self, key: &str) {
        Self::gate(&mut self.0.lock().unwrap(), key).add_permits(1);
    }

    fn entered(&self) -> Vec<String> {
        self.0.lock().unwrap().entered.clone()
    }

    fn count(&self, key: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .entered
            .iter()
            .filter(|entered| *entered == key)
            .count()
    }

    /// Wait until `key` has been reached `times` times.
    async fn wait_entered(&self, key: &str, times: usize) {
        tokio::time::timeout(PATIENCE, async {
            while self.count(key) < times {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{key} was never reached {times} time(s): {:?}",
                self.entered()
            )
        });
    }
}

fn start(ports: Ports) -> Arc<App> {
    let (app, app_loop) = App::new("test", ports);
    tokio::spawn(app_loop.run());
    Arc::new(app)
}

/// Dispatch without blocking the test, and hand back the command's completion.
fn spawn_command(app: &Arc<App>, command: AppCommand) -> JoinHandle<()> {
    let app = app.clone();
    tokio::spawn(async move {
        app.dispatch_and_wait(command)
            .await
            .expect("command completes");
    })
}

async fn finish(what: &str, task: JoinHandle<()>) {
    tokio::time::timeout(PATIENCE, task)
        .await
        .unwrap_or_else(|_| panic!("{what} never finished"))
        .unwrap_or_else(|error| panic!("{what} panicked: {error}"));
}

/// `dispatch_and_wait`, failing the test instead of hanging when the command
/// is held up.
async fn completes(app: &App, what: &str, command: AppCommand) {
    tokio::time::timeout(PATIENCE, app.dispatch_and_wait(command))
        .await
        .unwrap_or_else(|_| panic!("{what} was held up"))
        .expect("the loop is running");
}

// ── The smallest double per keyed command ───────────────────────────────────
//
// Each replaces one port and holds the one call its key's commands reach;
// everything else goes to the offline fake.

struct GatedMaps(Gates);

#[async_trait]
impl MapsPort for GatedMaps {
    async fn list_vault(&self) -> Result<Vec<VaultMap>, String> {
        self.0.pass("map-vault").await;
        Ok(Vec::new())
    }
    async fn search_vault(&self, query: MapVaultQuery) -> Result<MapSearchPage, String> {
        self.0.pass(&format!("map-search:{}", query.search)).await;
        Ok(MapSearchPage::default())
    }
    async fn list_installed(&self) -> Result<Vec<InstalledMap>, String> {
        FakeMaps.list_installed().await
    }
    async fn list_matchmaker_pools(&self, queue: String) -> Result<Vec<MatchmakerMapPool>, String> {
        if queue.starts_with(HELD_POOL_PREFIX) {
            self.0.pass(&format!("map-pool:{queue}")).await;
        }
        FakeMaps.list_matchmaker_pools(queue).await
    }
    async fn install_map(&self, folder: String, _url: String) -> Result<Vec<InstalledMap>, String> {
        self.0.pass(&format!("map-files:{folder}")).await;
        Ok(Vec::new())
    }
    async fn uninstall_map(&self, folder: String) -> Result<Vec<InstalledMap>, String> {
        self.0.pass(&format!("map-files:{folder}")).await;
        Ok(Vec::new())
    }
    async fn set_map_version_hidden(&self, version: i32, hidden: bool) -> Result<(), String> {
        FakeMaps.set_map_version_hidden(version, hidden).await
    }
}

struct GatedMods(Gates);

#[async_trait]
impl ModsPort for GatedMods {
    async fn list_vault(&self) -> Result<Vec<VaultMod>, String> {
        self.0.pass("mod-vault").await;
        Ok(Vec::new())
    }
    async fn search_vault(&self, query: ModVaultQuery) -> Result<ModSearchPage, String> {
        FakeMods.search_vault(query).await
    }
    async fn list_installed(&self) -> Result<Vec<InstalledMod>, String> {
        FakeMods.list_installed().await
    }
    async fn download_sizes(&self, targets: Vec<ModDownloadTarget>) -> Vec<ModDownloadSize> {
        FakeMods.download_sizes(targets).await
    }
    async fn install_mod(&self, uid: String, _url: String) -> Result<Vec<InstalledMod>, String> {
        self.0.pass(&format!("mod-files:{uid}")).await;
        Ok(Vec::new())
    }
    async fn update_mod(
        &self,
        uid: String,
        _: String,
        _: String,
    ) -> Result<Vec<InstalledMod>, String> {
        self.0.pass(&format!("mod-files:{uid}")).await;
        Ok(Vec::new())
    }
    async fn uninstall_mod(&self, folder: String) -> Result<Vec<InstalledMod>, String> {
        self.0.pass(&format!("mod-files:{folder}")).await;
        Ok(Vec::new())
    }
    async fn toggle_mod(&self, uid: String, enabled: bool) -> Result<Vec<InstalledMod>, String> {
        FakeMods.toggle_mod(uid, enabled).await
    }
    async fn set_active_mods(&self, uids: Vec<String>) -> Result<Vec<InstalledMod>, String> {
        FakeMods.set_active_mods(uids).await
    }
    async fn ensure_game_mods(
        &self,
        mods: &BTreeMap<String, String>,
        replace: bool,
    ) -> Result<(), ModPrepFailure> {
        FakeMods.ensure_game_mods(mods, replace).await
    }
}

struct GatedChangelog(Gates);

#[async_trait]
impl ChangelogPort for GatedChangelog {
    async fn list_releases(&self) -> Result<Vec<ChangelogRelease>, String> {
        self.0.pass("changelog-index").await;
        Ok(Vec::new())
    }
    async fn load_entry(&self, release: ChangelogRelease) -> Result<ChangelogEntry, String> {
        Err(format!("{} is not served here", release.id))
    }
}

/// Holds the connectivity check at its first question; the rest is the
/// offline fake's.
struct GatedConnectivity(Gates);

#[async_trait]
impl ConnectivityPort for GatedConnectivity {
    async fn adapter_inventory(&self) -> AdapterInventory {
        self.0.pass("connectivity-check").await;
        FakeConnectivity.adapter_inventory().await
    }
    async fn relay_list(&self) -> Result<Vec<RelayServer>, RelayListError> {
        FakeConnectivity.relay_list().await
    }
    async fn relay_addresses(&self) -> KnownRelayAddresses {
        FakeConnectivity.relay_addresses().await
    }
    async fn probe(&self, url: &IceUrl) -> Result<ProbeAnswer, ProbeFailure> {
        FakeConnectivity.probe(url).await
    }
    async fn adapter_log(&self) -> Option<AdapterLogTail> {
        FakeConnectivity.adapter_log().await
    }
}

/// Holds the live relay view's status call; starting and stopping are the
/// offline fake's.
struct GatedIce(Gates);

#[async_trait]
impl IcePort for GatedIce {
    async fn start(&self, params: IceParams) -> Result<ConnectivitySession, String> {
        FakeIce.start(params).await
    }
    fn stop(&self) {
        FakeIce.stop();
    }
    async fn relay_status(&self) -> RelayStatus {
        self.0.pass("relay-status").await;
        RelayStatus::Idle
    }
}

/// A run that is held until the gate opens and then reports itself stopped,
/// which is the one ending that raises no notification and records no map.
fn generator_stopped() -> mpsc::Receiver<GeneratorUpdate> {
    let (tx, rx) = mpsc::channel(1);
    tx.try_send(GeneratorUpdate::Status(GeneratorStatus::Cancelled))
        .expect("room for one status");
    rx
}

struct GatedGenerator(Gates);

#[async_trait]
impl MapGeneratorPort for GatedGenerator {
    async fn generate_named(&self, map_name: String) -> mpsc::Receiver<GeneratorUpdate> {
        self.0.pass(&format!("generate:{map_name}")).await;
        generator_stopped()
    }
    async fn generate(&self, _options: GeneratorOptions) -> mpsc::Receiver<GeneratorUpdate> {
        self.0.pass("generate:options").await;
        generator_stopped()
    }
    async fn query_options(
        &self,
        query: GeneratorOptionQuery,
        version: Option<String>,
        progress: Option<mpsc::Sender<GeneratorUpdate>>,
    ) -> Result<Vec<String>, String> {
        FakeMapGenerator
            .query_options(query, version, progress)
            .await
    }
    async fn preflight(&self, options: GeneratorOptions) -> Result<String, String> {
        FakeMapGenerator.preflight(options).await
    }
    async fn help(&self, version: Option<String>) -> Result<String, String> {
        FakeMapGenerator.help(version).await
    }
    fn cancel(&self) {}
    async fn save_preset(&self, name: &str, options: &GeneratorOptions) -> Result<(), String> {
        FakeMapGenerator.save_preset(name, options).await
    }
    async fn list_presets(&self) -> Vec<GeneratorPreset> {
        FakeMapGenerator.list_presets().await
    }
    async fn delete_preset(&self, name: &str) -> Result<(), String> {
        FakeMapGenerator.delete_preset(name).await
    }
    async fn rename_preset(&self, name: &str, new_name: &str) -> Result<(), String> {
        FakeMapGenerator.rename_preset(name, new_name).await
    }
    async fn latest_version(&self) -> Result<String, String> {
        FakeMapGenerator.latest_version().await
    }
    async fn available_versions(&self) -> Result<Vec<String>, String> {
        FakeMapGenerator.available_versions().await
    }
    fn is_installed(&self, _map_name: &str) -> bool {
        false
    }
    async fn clean_up(&self, _protected: &[String]) -> Result<usize, String> {
        self.0.pass("generator-clean-up").await;
        Ok(0)
    }
    async fn map_previews(&self, names: &[String]) -> HashMap<String, String> {
        FakeMapGenerator.map_previews(names).await
    }
}

struct GatedUploads(Gates);

#[async_trait]
impl UploadsPort for GatedUploads {
    async fn publish(&self, request: UploadRequest) -> mpsc::Receiver<UploadStatus> {
        self.0
            .pass(&format!("upload:{}", request.folder_name))
            .await;
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(UploadStatus::Succeeded)
            .expect("room for one status");
        rx
    }
    async fn map_preview(&self, request: UploadRequest) -> String {
        FakeUploads.map_preview(request).await
    }
}

struct GatedClientUpdates(Gates);

#[async_trait]
impl ClientUpdatePort for GatedClientUpdates {
    async fn latest(&self, _channel: ReleaseChannel) -> Result<Option<ClientRelease>, String> {
        self.0.pass("client-update-check").await;
        Ok(None)
    }
    async fn release_notes(&self) -> Result<Vec<ClientReleaseNotes>, String> {
        self.0.pass("client-release-notes").await;
        Ok(vec![])
    }
    async fn download(
        &self,
        release: ClientRelease,
        called_off: tokio_util::sync::CancellationToken,
    ) -> mpsc::Receiver<DownloadProgress> {
        FakeClientUpdates.download(release, called_off).await
    }
    async fn install(&self, path: String) -> Result<(), String> {
        FakeClientUpdates.install(path).await
    }
}

/// A gateway that names a version to install, so `Install` has work to do.
struct GatedGalacticWar(Gates);

#[async_trait]
impl GalacticWarPort for GatedGalacticWar {
    async fn statistics(&self) -> Result<GalacticWarStatistics, String> {
        FakeGalacticWar.statistics().await
    }
    async fn versions(&self) -> Result<ClientVersions, String> {
        Ok(ClientVersions {
            required_version: "1.0".into(),
            latest_version: Some("1.0".into()),
        })
    }
    fn installed_version(&self) -> Option<String> {
        None
    }
    async fn install(&self, version: String) -> mpsc::Receiver<InstallProgress> {
        self.0.pass("galactic-war-install").await;
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(InstallProgress::Finished(Err(format!(
            "{version} is not installed by this test"
        ))))
        .expect("room for one step");
        rx
    }
    async fn launch(&self) -> Result<(), String> {
        FakeGalacticWar.launch().await
    }
    fn is_running(&self) -> bool {
        false
    }
}

struct GatedGuides(Gates);

#[async_trait]
impl GuidesPort for GatedGuides {
    fn repo(&self) -> String {
        FakeGuides.repo()
    }
    fn configured(&self) -> bool {
        true
    }
    async fn begin_login(&self) -> Result<DeviceCode, String> {
        self.0.pass("guides-sign-in").await;
        Err("no code for this test".into())
    }
    async fn complete_login(&self, code: DeviceCode) -> Result<GuidesIdentity, String> {
        FakeGuides.complete_login(code).await
    }
    fn cancel_login(&self) {}
    async fn restore_login(&self) -> Result<Option<GuidesIdentity>, String> {
        Ok(None)
    }
    async fn sign_out(&self) {}
    async fn list_submissions(&self) -> Result<Vec<GuideSubmission>, String> {
        Ok(Vec::new())
    }
    async fn accept(&self, submission: GuideSubmission) -> Result<(), String> {
        self.0
            .pass(&format!("guides-verdict:{}", submission.number))
            .await;
        Ok(())
    }
    async fn reject(&self, number: i32, _: RejectReason, _: String) -> Result<(), String> {
        self.0.pass(&format!("guides-verdict:{number}")).await;
        Ok(())
    }
    async fn submit(
        &self,
        entry: TrainingResource,
        guide: String,
        images: Vec<faf_domain::state::GuideImage>,
    ) -> Result<String, String> {
        FakeGuides.submit(entry, guide, images).await
    }
}

/// Holds a preparation for the map it is for, then fails it: the tutorial
/// launch ends there, which is all its key covers.
struct GatedUpdater(Gates);

#[async_trait]
impl GameUpdaterPort for GatedUpdater {
    async fn prepare(&self, request: GamePreparation) -> mpsc::Receiver<UpdateProgress> {
        let map = request.map_folder.unwrap_or_default();
        self.0.pass(&format!("prepare:{map}")).await;
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(UpdateProgress::Finished(Err(
            "not prepared by this test".into()
        )))
        .expect("room for one step");
        rx
    }
}

struct GatedClan(Gates);

#[async_trait]
impl ClanPort for GatedClan {
    async fn me(&self) -> Result<ClanIdentity, RequestError> {
        FakeClan.me().await
    }
    async fn clan(&self, player_id: i32) -> Result<PlayerClan, RequestError> {
        FakeClan.clan(player_id).await
    }
    async fn create(&self, draft: &ClanDraft) -> Result<String, RequestError> {
        FakeClan.create(draft).await
    }
    async fn edit(&self, clan_id: &str, draft: &ClanDraft) -> Result<(), RequestError> {
        FakeClan.edit(clan_id, draft).await
    }
    async fn hand_over(&self, clan_id: &str, player_id: i32) -> Result<(), RequestError> {
        FakeClan.hand_over(clan_id, player_id).await
    }
    async fn invite(&self, clan_id: &str, player_id: i32) -> Result<String, RequestError> {
        FakeClan.invite(clan_id, player_id).await
    }
    async fn accept_invitation(&self, token: &str) -> Result<(), RequestError> {
        self.0.pass(&format!("clan-write:{token}")).await;
        Ok(())
    }
    async fn remove_membership(&self, membership_id: &str) -> Result<(), RequestError> {
        FakeClan.remove_membership(membership_id).await
    }
    async fn disband(&self, clan_id: &str) -> Result<(), RequestError> {
        FakeClan.disband(clan_id).await
    }
}

struct GatedPlayers(Gates);

#[async_trait]
impl PlayerCardPort for GatedPlayers {
    async fn search_players(
        &self,
        query: &str,
        limit: i32,
    ) -> Result<Vec<PlayerSummary>, RequestError> {
        FakePlayerCard.search_players(query, limit).await
    }
    async fn players_by_login(
        &self,
        logins: &[String],
    ) -> Result<Vec<PlayerSummary>, RequestError> {
        FakePlayerCard.players_by_login(logins).await
    }
    async fn players_by_id(&self, ids: &[i32]) -> Result<Vec<PlayerSummary>, RequestError> {
        FakePlayerCard.players_by_id(ids).await
    }
    async fn load_profile(
        &self,
        player_id: Option<i32>,
        login: &str,
    ) -> Result<PlayerCardProfile, String> {
        FakePlayerCard.load_profile(player_id, login).await
    }
    async fn load_matchmaker_profile(
        &self,
        player_id: i32,
        login: &str,
    ) -> Result<MatchmakerPlayerProfile, String> {
        FakePlayerCard
            .load_matchmaker_profile(player_id, login)
            .await
    }
    async fn load_league_placements(
        &self,
        player_ids: &[i32],
    ) -> Result<BTreeMap<i32, Vec<PlayerLeaguePlacement>>, String> {
        let ids: Vec<String> = player_ids.iter().map(i32::to_string).collect();
        self.0.pass(&format!("placements:{}", ids.join(","))).await;
        Ok(BTreeMap::new())
    }
    async fn load_rating_history(
        &self,
        query: &RatingHistoryQuery,
    ) -> Result<RatingHistoryPage, String> {
        FakePlayerCard.load_rating_history(query).await
    }
    async fn load_map_stats(&self, player_id: i32) -> Result<PlayerMapStats, String> {
        FakePlayerCard.load_map_stats(player_id).await
    }
}

struct GatedTourneyMatch {
    gates: Gates,
    inner: FakeTourney,
}

#[async_trait]
impl TourneyMatchPort for GatedTourneyMatch {
    async fn advance(
        &self,
        tournament_id: &str,
        phase: TourneyPhase,
        config: Option<&BracketConfig>,
    ) -> Result<(), RequestError> {
        self.inner.advance(tournament_id, phase, config).await
    }
    async fn confirm_report(
        &self,
        tournament_id: &str,
        match_id: &str,
        accept: bool,
    ) -> Result<(), RequestError> {
        self.gates.pass(&format!("tourney-write:{match_id}")).await;
        self.inner
            .confirm_report(tournament_id, match_id, accept)
            .await
    }
    async fn decide_report(
        &self,
        tournament_id: &str,
        report: &MatchReport,
    ) -> Result<(), RequestError> {
        self.inner.decide_report(tournament_id, report).await
    }
    async fn submit_report(
        &self,
        tournament_id: &str,
        report: &MatchReport,
    ) -> Result<(), RequestError> {
        self.inner.submit_report(tournament_id, report).await
    }
    async fn report_ffa(
        &self,
        tournament_id: &str,
        report: &FfaReport,
    ) -> Result<(), RequestError> {
        self.inner.report_ffa(tournament_id, report).await
    }
}

/// The offline bundle with every keyed command's port held at a gate.
fn gated_ports(gates: &Gates) -> Ports {
    Ports {
        maps: Arc::new(GatedMaps(gates.clone())),
        mods: Arc::new(GatedMods(gates.clone())),
        changelog: Arc::new(GatedChangelog(gates.clone())),
        connectivity: Arc::new(GatedConnectivity(gates.clone())),
        ice: Arc::new(GatedIce(gates.clone())),
        map_generator: Arc::new(GatedGenerator(gates.clone())),
        uploads: Arc::new(GatedUploads(gates.clone())),
        client_update: Arc::new(GatedClientUpdates(gates.clone())),
        galactic_war: Arc::new(GatedGalacticWar(gates.clone())),
        guides: Arc::new(GatedGuides(gates.clone())),
        updater: Arc::new(GatedUpdater(gates.clone())),
        clan: Arc::new(GatedClan(gates.clone())),
        player_card: Arc::new(GatedPlayers(gates.clone())),
        tourney_match: Arc::new(GatedTourneyMatch {
            gates: gates.clone(),
            inner: FakeTourney::new(),
        }),
        ..fake_ports()
    }
}

// ── The policy table, key by key ────────────────────────────────────────────

/// A command of a key other than the one under test, whose gate the test
/// opens in advance: it must run to the end while the key under test is held.
struct Probe {
    command: AppCommand,
    gate: &'static str,
}

fn client_update_probe() -> Probe {
    Probe {
        command: ClientUpdateCommand::Check.into(),
        gate: "client-update-check",
    }
}

fn changelog_probe() -> Probe {
    Probe {
        command: ChangelogCommand::Load.into(),
        gate: "changelog-index",
    }
}

/// One single-flight key: the command that takes it, a second command of the
/// same kind sent while it runs, and the gate each would reach.
struct SingleFlightCase {
    key: &'static str,
    /// Commands that put the state where the first command has work to do.
    setup: Vec<AppCommand>,
    first: AppCommand,
    first_gate: String,
    again: AppCommand,
    again_gate: String,
    probe: Probe,
}

fn upload_request(folder: &str) -> UploadRequest {
    UploadRequest {
        kind: UploadKind::Map,
        folder_name: folder.into(),
        display_name: folder.into(),
        ranked: false,
        source_path: None,
        rename_to: String::new(),
    }
}

fn single_flight_cases() -> Vec<SingleFlightCase> {
    vec![
        SingleFlightCase {
            key: "MapVault",
            setup: vec![],
            first: MapsCommand::LoadVault.into(),
            first_gate: "map-vault".into(),
            again: MapsCommand::LoadVault.into(),
            again_gate: "map-vault".into(),
            probe: client_update_probe(),
        },
        SingleFlightCase {
            key: "ModVault",
            setup: vec![],
            first: ModsCommand::LoadVault.into(),
            first_gate: "mod-vault".into(),
            // A second load, as another tab mounting would send.
            again: ModsCommand::LoadVault.into(),
            again_gate: "mod-vault".into(),
            probe: client_update_probe(),
        },
        SingleFlightCase {
            key: "Changelog",
            setup: vec![],
            first: ChangelogCommand::Load.into(),
            first_gate: "changelog-index".into(),
            again: ChangelogCommand::Load.into(),
            again_gate: "changelog-index".into(),
            probe: client_update_probe(),
        },
        SingleFlightCase {
            key: "MapGenerator",
            setup: vec![],
            first: MapGeneratorCommand::GenerateNamed {
                map_name: "first_map".into(),
            }
            .into(),
            first_gate: "generate:first_map".into(),
            // A different map, and a different command under the same key:
            // the key is the generator, not the map.
            again: MapGeneratorCommand::CleanUp.into(),
            again_gate: "generator-clean-up".into(),
            probe: client_update_probe(),
        },
        SingleFlightCase {
            key: "Upload",
            setup: vec![UploadsCommand::Open {
                request: upload_request("first_map"),
            }
            .into()],
            first: UploadsCommand::Start.into(),
            first_gate: "upload:first_map".into(),
            again: UploadsCommand::Start.into(),
            again_gate: "upload:first_map".into(),
            probe: client_update_probe(),
        },
        SingleFlightCase {
            key: "ClientUpdate",
            setup: vec![],
            first: ClientUpdateCommand::Check.into(),
            first_gate: "client-update-check".into(),
            again: ClientUpdateCommand::Check.into(),
            again_gate: "client-update-check".into(),
            probe: changelog_probe(),
        },
        SingleFlightCase {
            key: "ClientReleaseNotes",
            setup: vec![],
            first: ClientUpdateCommand::LoadNotes.into(),
            first_gate: "client-release-notes".into(),
            again: ClientUpdateCommand::LoadNotes.into(),
            again_gate: "client-release-notes".into(),
            // The check reads the same port, under a key of its own: a notes
            // load in flight must not hold it up.
            probe: client_update_probe(),
        },
        SingleFlightCase {
            key: "GalacticWar",
            setup: vec![GalacticWarCommand::Refresh.into()],
            first: GalacticWarCommand::Install.into(),
            first_gate: "galactic-war-install".into(),
            again: GalacticWarCommand::Play.into(),
            again_gate: "galactic-war-install".into(),
            probe: client_update_probe(),
        },
        SingleFlightCase {
            key: "GuidesSignIn",
            setup: vec![],
            first: GuidesCommand::SignIn.into(),
            first_gate: "guides-sign-in".into(),
            again: GuidesCommand::SignIn.into(),
            again_gate: "guides-sign-in".into(),
            probe: client_update_probe(),
        },
        SingleFlightCase {
            key: "TutorialLaunch",
            setup: vec![TutorialsCommand::Load.into()],
            first: TutorialsCommand::Launch { tutorial_id: 1 }.into(),
            first_gate: "prepare:scmp_tut_1".into(),
            // Another lesson: one launch at a time, whichever it is.
            again: TutorialsCommand::Launch { tutorial_id: 2 }.into(),
            again_gate: "prepare:scmp_tut_2".into(),
            probe: client_update_probe(),
        },
        SingleFlightCase {
            key: "ConnectivityCheck",
            setup: vec![],
            first: ConnectivityCommand::RunCheck.into(),
            first_gate: "connectivity-check".into(),
            again: ConnectivityCommand::RunCheck.into(),
            again_gate: "connectivity-check".into(),
            probe: client_update_probe(),
        },
        SingleFlightCase {
            key: "RelayStatus",
            setup: vec![],
            first: ConnectivityCommand::RefreshRelayStatus.into(),
            first_gate: "relay-status".into(),
            again: ConnectivityCommand::RefreshRelayStatus.into(),
            again_gate: "relay-status".into(),
            probe: client_update_probe(),
        },
    ]
}

/// For every single-flight key: while one command holds it, a second of the
/// same kind is dropped (its dispatch completes at once and its port is never
/// reached, even after the first finishes), and a command of another key runs
/// to the end beside it.
#[tokio::test]
async fn a_second_single_flight_command_is_dropped_while_one_runs_and_other_keys_are_not_held_up() {
    for case in single_flight_cases() {
        let key = case.key;
        let gates = Gates::default();
        let app = start(gated_ports(&gates));
        for command in case.setup {
            app.dispatch_and_wait(command).await.unwrap();
        }

        let first = spawn_command(&app, case.first);
        gates.wait_entered(&case.first_gate, 1).await;
        let again_before = gates.count(&case.again_gate);

        // Dropped, not queued: it completes while the first is still held.
        completes(&app, &format!("{key}: the second command"), case.again).await;

        // Another key is not held up by this one.
        gates.open(case.probe.gate);
        completes(&app, &format!("{key}: the probe"), case.probe.command).await;
        assert_eq!(
            gates.count(case.probe.gate),
            1,
            "{key}: the probe never reached its port"
        );

        gates.open(&case.first_gate);
        finish(&format!("{key}: the first command"), first).await;
        assert_eq!(
            gates.count(&case.again_gate),
            again_before,
            "{key}: the second command ran: {:?}",
            gates.entered()
        );
        assert_eq!(
            gates.count(&case.first_gate),
            1,
            "{key}: the first command ran twice"
        );
    }
}

/// One serial key: three commands sent in this order, and the gate each
/// reaches.
struct SerialCase {
    key: &'static str,
    commands: [AppCommand; 3],
    gates: [String; 3],
    probe: Probe,
}

fn guides_reject(number: i32) -> AppCommand {
    GuidesCommand::Reject {
        number,
        reason: RejectReason::Duplicate,
        note: String::new(),
    }
    .into()
}

fn answer_report(match_id: &str) -> AppCommand {
    TourneyWrite::AnswerReport {
        tournament_id: "t1".into(),
        match_id: match_id.into(),
        accept: true,
    }
    .into()
}

fn serial_cases() -> Vec<SerialCase> {
    vec![
        SerialCase {
            key: "MapFiles",
            commands: [
                MapsCommand::InstallMap {
                    folder_name: "a".into(),
                    download_url: "https://example.invalid/a.zip".into(),
                }
                .into(),
                MapsCommand::UninstallMap {
                    folder_name: "b".into(),
                }
                .into(),
                MapsCommand::InstallMap {
                    folder_name: "c".into(),
                    download_url: "https://example.invalid/c.zip".into(),
                }
                .into(),
            ],
            gates: [
                "map-files:a".into(),
                "map-files:b".into(),
                "map-files:c".into(),
            ],
            probe: client_update_probe(),
        },
        SerialCase {
            key: "ModFiles",
            commands: [
                ModsCommand::InstallMod {
                    uid: "a".into(),
                    download_url: "https://example.invalid/a.zip".into(),
                }
                .into(),
                ModsCommand::UninstallMod {
                    folder_name: "b".into(),
                    uid: "b".into(),
                }
                .into(),
                ModsCommand::UpdateMod {
                    uid: "c".into(),
                    folder_name: "c".into(),
                    download_url: "https://example.invalid/c.zip".into(),
                }
                .into(),
            ],
            gates: [
                "mod-files:a".into(),
                "mod-files:b".into(),
                "mod-files:c".into(),
            ],
            probe: client_update_probe(),
        },
        SerialCase {
            key: "GuidesVerdict",
            commands: [guides_reject(1), guides_reject(2), guides_reject(3)],
            gates: [
                "guides-verdict:1".into(),
                "guides-verdict:2".into(),
                "guides-verdict:3".into(),
            ],
            probe: client_update_probe(),
        },
        SerialCase {
            key: "ClanWrite",
            commands: [
                ClanCommand::AcceptInvitation { token: "a".into() }.into(),
                ClanCommand::AcceptInvitation { token: "b".into() }.into(),
                ClanCommand::AcceptInvitation { token: "c".into() }.into(),
            ],
            gates: [
                "clan-write:a".into(),
                "clan-write:b".into(),
                "clan-write:c".into(),
            ],
            probe: client_update_probe(),
        },
        SerialCase {
            key: "TourneyWrite",
            commands: [
                answer_report("m1"),
                answer_report("m2"),
                answer_report("m3"),
            ],
            gates: [
                "tourney-write:m1".into(),
                "tourney-write:m2".into(),
                "tourney-write:m3".into(),
            ],
            probe: client_update_probe(),
        },
        SerialCase {
            key: "PartyPlacements",
            commands: [
                PlayerCardCommand::LoadPartyPlacements {
                    player_ids: vec![1],
                }
                .into(),
                PlayerCardCommand::LoadPartyPlacements {
                    player_ids: vec![2],
                }
                .into(),
                PlayerCardCommand::LoadPartyPlacements {
                    player_ids: vec![3],
                }
                .into(),
            ],
            gates: [
                "placements:1".into(),
                "placements:2".into(),
                "placements:3".into(),
            ],
            probe: client_update_probe(),
        },
    ]
}

/// For every serial key: the second and third commands wait for the first
/// even with their own gates already open, then run in the order they were
/// dispatched, and a command of another key runs to the end meanwhile.
#[tokio::test]
async fn serial_commands_wait_for_the_one_before_and_run_in_dispatch_order() {
    for case in serial_cases() {
        let key = case.key;
        let gates = Gates::default();
        let app = start(gated_ports(&gates));
        let [first, second, third] = case.commands;
        let [first_gate, second_gate, third_gate] = case.gates;

        // The later two would run straight through if they were let start.
        gates.open(&second_gate);
        gates.open(&third_gate);

        app.dispatch(first).await.unwrap();
        gates.wait_entered(&first_gate, 1).await;
        app.dispatch(second).await.unwrap();
        app.dispatch(third).await.unwrap();

        gates.open(case.probe.gate);
        completes(&app, &format!("{key}: the probe"), case.probe.command).await;

        tokio::time::sleep(WINDOW).await;
        let held: Vec<String> = gates
            .entered()
            .into_iter()
            .filter(|entered| [&first_gate, &second_gate, &third_gate].contains(&entered))
            .collect();
        assert_eq!(
            held,
            vec![first_gate.clone()],
            "{key}: a later command started while the first held the key"
        );

        gates.open(&first_gate);
        gates.wait_entered(&third_gate, 1).await;
        let order: Vec<String> = gates
            .entered()
            .into_iter()
            .filter(|entered| [&first_gate, &second_gate, &third_gate].contains(&entered))
            .collect();
        assert_eq!(
            order,
            vec![first_gate, second_gate, third_gate],
            "{key}: not in dispatch order"
        );
    }
}

/// Every command in the priority lane, as the table defines it.
fn priority_commands() -> Vec<(&'static str, AppCommand)> {
    vec![
        ("Nav::Select", NavCommand::Select { tab: Tab::Maps }.into()),
        ("Lobby::CancelJoin", LobbyCommand::CancelJoin.into()),
        (
            "Lobby::DeclineModReplacement",
            LobbyCommand::DeclineModReplacement.into(),
        ),
        ("Lobby::TerminateGame", LobbyCommand::TerminateGame.into()),
        (
            "Lobby::Matchmake stop",
            LobbyCommand::Matchmake {
                queue_name: "ladder1v1".into(),
                start: false,
            }
            .into(),
        ),
        ("Lobby::Disconnect", LobbyCommand::Disconnect.into()),
        ("Chat::Disconnect", ChatCommand::Disconnect.into()),
        ("Replays::CancelWatch", ReplayCommand::CancelWatch.into()),
        (
            "Replays::CancelLiveTracking",
            ReplayCommand::CancelLiveTracking.into(),
        ),
        ("MapGenerator::Cancel", MapGeneratorCommand::Cancel.into()),
        ("Guides::CancelSignIn", GuidesCommand::CancelSignIn.into()),
        ("Auth::CancelLogin", AuthCommand::CancelLogin.into()),
        ("Auth::Logout", AuthCommand::Logout.into()),
    ]
}

/// Every ordinary slot is taken by a pool load held at its gate and the
/// ordinary queue is full behind them, so no ordinary command can even be
/// queued. Each priority command still runs to the end.
#[tokio::test]
async fn a_priority_command_is_not_held_up_by_a_saturated_ordinary_lane() {
    let gates = Gates::default();
    let app = start(gated_ports(&gates));
    // Searches replace their predecessors, so they cannot hold all the slots.
    // Distinct pool loads run concurrently and leave each command at its gate.
    let load = |n: usize| -> AppCommand {
        MapsCommand::LoadMatchmakerPools {
            queue_name: format!("{HELD_POOL_PREFIX}{n}"),
        }
        .into()
    };

    // Fill the queue until it says so, let the loop hand the first ones every
    // slot, and top the queue up again, until the pool loads at their gates
    // stop growing and the queue stays full: nothing ordinary is moving.
    let mut sent = 0;
    tokio::time::timeout(PATIENCE, async {
        loop {
            while app.try_dispatch(load(sent)).is_ok() {
                sent += 1;
            }
            let running = gates.entered().len();
            tokio::time::sleep(WINDOW).await;
            if running > 0 && gates.entered().len() == running {
                if app.try_dispatch(load(sent)).is_err() {
                    return;
                }
                sent += 1;
            }
        }
    })
    .await
    .expect("the ordinary lane never settled");
    assert!(
        app.try_dispatch(load(sent)).is_err(),
        "the ordinary lane is not saturated"
    );

    for (name, command) in priority_commands() {
        completes(&app, name, command).await;
    }
}

// ── Service guards ──────────────────────────────────────────────────────────

/// Every lobby port call goes to the offline fake; `connect` is counted.
struct CountingLobby {
    inner: FakeLobby,
    connects: Arc<AtomicUsize>,
}

#[async_trait]
impl LobbyPort for CountingLobby {
    async fn connect(&self) -> mpsc::Receiver<LobbyUpdate> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        self.inner.connect().await
    }
    fn join(&self, id: i32, password: Option<String>) -> bool {
        self.inner.join(id, password)
    }
    fn host(&self, config: HostGameConfig) {
        self.inner.host(config)
    }
    fn matchmake(&self, queue_name: String, start: bool) {
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

async fn lobby_event(
    events: &mut broadcast::Receiver<AppEvent>,
    what: &str,
    matches: impl Fn(&LobbyEvent) -> bool,
) {
    tokio::time::timeout(PATIENCE, async {
        loop {
            match events.recv().await {
                Ok(AppEvent::Lobby(event)) if matches(&event) => return,
                Err(broadcast::error::RecvError::Closed) => panic!("the app stopped"),
                _ => {}
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("never saw {what}"));
}

/// The lobby socket is one connection for as long as it is open: a second
/// `Connect` while it is up does not open another. Once the adapter drops it
/// (nobody asked), two `Connect`s sent together bring back exactly one.
#[tokio::test]
async fn the_lobby_socket_is_single_flight_before_and_after_a_drop() {
    let lobby = FakeLobby::default();
    let connects = Arc::new(AtomicUsize::new(0));
    let app = start(Ports {
        lobby: Arc::new(CountingLobby {
            inner: lobby.clone(),
            connects: connects.clone(),
        }),
        ..fake_ports()
    });
    let mut events = app.subscribe();

    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    lobby_event(&mut events, "the lobby connecting", |event| {
        matches!(event, LobbyEvent::Connected)
    })
    .await;
    for _ in 0..2 {
        completes(&app, "a redundant connect", LobbyCommand::Connect.into()).await;
    }
    assert_eq!(connects.load(Ordering::SeqCst), 1, "a second socket opened");

    // The adapter gives up on its own, which ends the update stream.
    lobby.disconnect();
    lobby_event(&mut events, "the drop", |event| {
        matches!(event, LobbyEvent::Disconnected)
    })
    .await;
    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    app.dispatch(LobbyCommand::Connect.into()).await.unwrap();
    lobby_event(&mut events, "the lobby back", |event| {
        matches!(event, LobbyEvent::Connected)
    })
    .await;
    completes(&app, "a redundant connect", LobbyCommand::Connect.into()).await;
    assert_eq!(
        connects.load(Ordering::SeqCst),
        2,
        "connecting again after the drop opened more than one socket"
    );
}

/// Chat goes to the offline fake, shared so the test can drop it from the
/// adapter's side; `connect` is counted.
struct CountingChat {
    inner: Arc<FakeChat>,
    connects: Arc<AtomicUsize>,
}

#[async_trait]
impl ChatPort for CountingChat {
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

async fn until(app: &App, what: &str, done: impl Fn(&AppState) -> bool) {
    tokio::time::timeout(PATIENCE, async {
        while !done(&app.snapshot()) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("never saw {what}"));
}

fn chat_connect() -> AppCommand {
    ChatCommand::Connect {
        username: "Aurora".into(),
    }
    .into()
}

/// The chat socket keeps the same rule as the lobby's: one connection while
/// it is open, and exactly one again after the adapter drops it.
#[tokio::test]
async fn the_chat_socket_is_single_flight_before_and_after_a_drop() {
    let chat = Arc::new(FakeChat::default());
    let connects = Arc::new(AtomicUsize::new(0));
    let app = start(Ports {
        chat: Arc::new(CountingChat {
            inner: chat.clone(),
            connects: connects.clone(),
        }),
        ..fake_ports()
    });
    let mut events = app.subscribe();

    app.dispatch(chat_connect()).await.unwrap();
    until(&app, "chat connected", |state| {
        state.chat.status == ChatStatus::Connected
    })
    .await;
    for _ in 0..2 {
        completes(&app, "a redundant connect", chat_connect()).await;
    }
    assert_eq!(connects.load(Ordering::SeqCst), 1, "a second socket opened");

    // The adapter reports the drop as a status and then ends the stream; the
    // service frees its guard and says `Disconnected` a second time once the
    // stream has ended. `Connect` runs for as long as its socket does, so
    // the ones below are sent, not awaited.
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
    app.dispatch(chat_connect()).await.unwrap();
    app.dispatch(chat_connect()).await.unwrap();
    until(&app, "chat back", |state| {
        state.chat.status == ChatStatus::Connected
    })
    .await;
    completes(&app, "a redundant connect", chat_connect()).await;
    assert_eq!(
        connects.load(Ordering::SeqCst),
        2,
        "connecting again after the drop opened more than one socket"
    );
}

/// A sign-in that waits at a gate, as an interactive login waits for the
/// browser, and a restore that does the same.
struct HeldAuth {
    gates: Gates,
    logouts: Arc<AtomicUsize>,
}

#[async_trait]
impl AuthPort for HeldAuth {
    async fn login(
        &self,
        _remember: bool,
        _link: &(dyn Fn(String) + Send + Sync),
    ) -> AuthResult<Player> {
        self.gates.pass("login").await;
        Ok(Player::new(7, "Ada"))
    }
    async fn restore(&self) -> AuthResult<Option<Player>> {
        self.gates.pass("restore").await;
        Ok(Some(Player::new(7, "Ada")))
    }
    fn commit_session(&self) -> bool {
        true
    }
    fn discard_pending_session(&self) {}
    async fn logout(&self) -> AuthResult<()> {
        self.logouts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn held_auth() -> (Arc<App>, Gates, Arc<AtomicUsize>) {
    let gates = Gates::default();
    let logouts = Arc::new(AtomicUsize::new(0));
    let app = start(Ports {
        auth: Arc::new(HeldAuth {
            gates: gates.clone(),
            logouts: logouts.clone(),
        }),
        ..fake_ports()
    });
    (app, gates, logouts)
}

/// Each command that calls a sign-in off, and where it leaves the session.
fn sign_in_releases() -> Vec<(&'static str, AppCommand, AuthStatus, AuthMode)> {
    vec![
        (
            "Logout",
            AuthCommand::Logout.into(),
            AuthStatus::LoggedOut,
            AuthMode::default(),
        ),
        (
            "CancelLogin",
            AuthCommand::CancelLogin.into(),
            AuthStatus::LoggedOut,
            AuthMode::default(),
        ),
        (
            "LogoutTest",
            AuthCommand::LogoutTest.into(),
            AuthStatus::LoggedOut,
            AuthMode::default(),
        ),
        (
            "PlayOffline",
            AuthCommand::PlayOffline.into(),
            AuthStatus::LoggedIn,
            AuthMode::Offline,
        ),
    ]
}

/// A sign-in is waiting on the browser. Logging out, cancelling, the test
/// logout and going offline each call it off: they complete without waiting
/// for it (it holds the sign-in lock), the login's port call is dropped rather
/// than answered, and nobody is signed in when it would have answered. The lock
/// is free afterwards, so the next sign-in works.
#[tokio::test]
async fn calling_a_sign_in_off_cancels_it_instead_of_waiting_for_it() {
    for (name, release, status, mode) in sign_in_releases() {
        let (app, gates, logouts) = held_auth();

        let login = spawn_command(&app, AuthCommand::Login { remember: false }.into());
        gates.wait_entered("login", 1).await;
        completes(&app, name, release).await;
        // The login's own command ends without its port ever answering.
        finish(&format!("{name}: the cancelled login"), login).await;

        gates.open("login");
        tokio::time::sleep(WINDOW).await;
        let auth = app.snapshot().auth;
        assert_eq!(auth.status, status, "{name}");
        assert_eq!(auth.mode, mode, "{name}");
        assert_eq!(auth.player, None, "{name}: the called-off sign-in landed");
        if name == "Logout" {
            assert_eq!(
                logouts.load(Ordering::SeqCst),
                1,
                "the session was not torn down"
            );
        }

        // The lock is free: a new sign-in goes through.
        completes(
            &app,
            &format!("{name}: the next sign-in"),
            AuthCommand::Login { remember: false }.into(),
        )
        .await;
        assert_eq!(
            app.snapshot().auth.player.map(|player| player.id),
            Some(7),
            "{name}: the next sign-in did not land"
        );
    }
}

/// A restore (the remembered session) is not interactive and has no cancel:
/// `Logout` waits for its request to answer. The answer must still not sign
/// the user in over the logout.
#[tokio::test]
async fn a_restore_answering_after_a_logout_does_not_sign_the_user_in() {
    let (app, gates, logouts) = held_auth();
    let mut events = app.subscribe();

    let restore = spawn_command(&app, AuthCommand::Restore.into());
    gates.wait_entered("restore", 1).await;
    let logout = spawn_command(&app, AuthCommand::Logout.into());
    tokio::time::sleep(WINDOW).await;
    assert!(
        !logout.is_finished(),
        "the logout overtook the restore's lock"
    );

    gates.open("restore");
    finish("the restore", restore).await;
    finish("the logout", logout).await;

    assert_eq!(app.snapshot().auth.status, AuthStatus::LoggedOut);
    assert_eq!(logouts.load(Ordering::SeqCst), 1);
    while let Ok(event) = events.try_recv() {
        assert!(
            !matches!(event, AppEvent::Auth(AuthEvent::LoggedIn { .. })),
            "the restore signed the user in after they logged out"
        );
    }
}

/// Settings whose writes wait at a gate, one gate per write in arrival order,
/// once the test arms them.
struct HeldSettings {
    gates: Gates,
    armed: Arc<AtomicBool>,
    writes: AtomicUsize,
    saved: Arc<Mutex<Vec<SettingsState>>>,
}

#[async_trait]
impl SettingsPort for HeldSettings {
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
        if self.armed.load(Ordering::SeqCst) {
            let write = self.writes.fetch_add(1, Ordering::SeqCst);
            self.gates.pass(&format!("save-{write}")).await;
        }
        self.saved.lock().unwrap().push(settings.clone());
        Ok(())
    }
}

/// Two settings changes in quick succession. Each lands in state at once (the
/// merge is not held up by a write), but the second write waits for the first
/// to reach the store, and the last document written carries both changes.
#[tokio::test]
async fn settings_writes_reach_the_store_one_at_a_time_in_order() {
    let gates = Gates::default();
    let armed = Arc::new(AtomicBool::new(false));
    let saved = Arc::new(Mutex::new(Vec::new()));
    let app = start(Ports {
        settings: Arc::new(HeldSettings {
            gates: gates.clone(),
            armed: armed.clone(),
            writes: AtomicUsize::new(0),
            saved: saved.clone(),
        }),
        ..fake_ports()
    });
    app.dispatch_and_wait(SettingsCommand::Load.into())
        .await
        .unwrap();
    saved.lock().unwrap().clear();
    armed.store(true, Ordering::SeqCst);

    let first = spawn_command(
        &app,
        SettingsCommand::PatchGame {
            patch: Box::new(GamePreferencesPatch {
                steam_presence: Some(false),
                ..GamePreferencesPatch::default()
            }),
        }
        .into(),
    );
    gates.wait_entered("save-0", 1).await;
    let second = spawn_command(
        &app,
        SettingsCommand::PatchBrowsing {
            patch: Box::new(BrowsingPreferencesPatch {
                vault_page_size: Some(48),
                ..BrowsingPreferencesPatch::default()
            }),
        }
        .into(),
    );
    until(&app, "the second change in state", |state| {
        state.settings.browsing.vault_page_size == 48
    })
    .await;
    tokio::time::sleep(WINDOW).await;
    assert_eq!(
        gates.entered(),
        vec!["save-0".to_string()],
        "the second write started while the first was still writing"
    );

    gates.open("save-0");
    gates.wait_entered("save-1", 1).await;
    gates.open("save-1");
    finish("the first change", first).await;
    finish("the second change", second).await;

    let saved = saved.lock().unwrap().clone();
    assert_eq!(saved.len(), 2);
    assert!(
        !saved[0].game.steam_presence,
        "the first write lost its change"
    );
    let last = saved.last().unwrap();
    assert!(
        !last.game.steam_presence,
        "the last write dropped the first change"
    );
    assert_eq!(last.browsing.vault_page_size, 48);
}
