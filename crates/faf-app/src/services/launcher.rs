//! Launcher orchestration: turns a `game_launch` order into a running game.
//!
//! Backend-neutral: it asks the [`IcePort`](crate::ports::IcePort) for a
//! [`ConnectivitySession`](crate::ports::ConnectivitySession) (Go or Java decide
//! their own internals), launches the game on the session's GPGNet port, and
//! bridges the session's relay channels to the lobby:
//!
//! - `session.to_lobby` → `lobby.send_game_relay` (adapter → lobby)
//! - lobby `target: "game"` messages → [`LaunchSession::forward_to_adapter`] →
//!   `session.from_lobby` (lobby → adapter)
//!
//! The lobby connect loop owns the returned [`LaunchSession`] and feeds it the
//! relay messages arriving on the same socket. On any setup failure we stop the
//! adapter and emit `LaunchFailed`.

use faf_domain::state::{
    Game, GameLaunch, HostGameConfig, LobbyEvent, NotificationKind, PlayerProfile,
    PreparationPhase as DomainPreparationPhase, ReplayEvent,
};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::ports::{
    GameLaunchParams, GamePreparation, IceParams, ModPrepFailure, PreparationPhase, RelayMsg,
    ReplayMetadata, UpdateProgress, DEFAULT_LOCAL_REPLAY_LIMIT,
};
use crate::runtime::{EventSink, ServiceCtx};
use crate::services::notifications;

/// A live launch: the channel into the adapter, used to forward lobby
/// game-relay messages. Dropping it does not stop the game.
pub struct LaunchSession {
    from_lobby: mpsc::Sender<RelayMsg>,
}

impl LaunchSession {
    /// Forward a lobby `target: "game"` message to the connectivity backend.
    pub async fn forward_to_adapter(&self, command: String, args: Vec<Value>) {
        // ICE candidates are correctness-critical. A full bounded queue must
        // apply backpressure instead of silently dropping the one candidate a
        // peer needs to connect.
        tracing::trace!(%command, "launcher: forwarding lobby relay message to adapter");
        if self
            .from_lobby
            .send(RelayMsg { command, args })
            .await
            .is_err()
        {
            tracing::warn!("launcher: connectivity adapter stopped accepting lobby messages");
        }
    }
}

/// Run the launch chain. Emits `InGame` on success (returning the session) or
/// `LaunchFailed` on any failure (returning `None`, after stopping the adapter).
pub async fn start(
    launch: &GameLaunch,
    ctx: &ServiceCtx,
    out: &EventSink,
    already_prepared: bool,
) -> Option<LaunchSession> {
    let Some(player) = out.with_state(|state| state.auth.player.clone()) else {
        return fail(ctx, out, "not logged in".into());
    };
    let player_profile = out.with_state(|state| {
        state
            .social
            .players
            .iter()
            .find(|profile| profile.id == player.id)
            .cloned()
    });
    let init_mode = init_mode_for(&launch.game_type);

    // A launch order is new work: whatever an earlier join did, this one has
    // not been cancelled. Without this a cancelled custom join would silence
    // the progress of the next matchmaker or hosted game as well.
    ctx.lobby.clear_launch_cancellation();

    // 0. Reproduce a generated map before anything else.
    //
    // Matchmaker pools contain maps that are never distributed as files: the
    // server names them and every client rebuilds identical terrain from the
    // name (see `infra::map_generator`). Launching without the folder present
    // drops the player into a game they cannot load, so this is a hard gate on
    // the launch rather than a warning. Both reference clients do the same
    // check at the same point (Java's `MapService.generateIfNotInstalled`).
    if !already_prepared {
        if let Err(reason) = ensure_generated_map(&launch.mapname, ctx, out).await {
            return fail(ctx, out, reason);
        }

        // 1. Patch the featured mod and download the map.
        //
        // The server does not care whether this client is current: it will happily
        // seat a player on an old build or a map they have never seen, and the game
        // then fails to load or desyncs. Both reference clients update before every
        // game rather than tracking whether an update is due (Java's
        // `prepareAndLaunchGameWhenReady`, the Python client's `fa.check.check`);
        // it is cheap when nothing changed, because files matching by MD5 are
        // skipped and a present map is not re-fetched.
        if let Err(reason) = prepare_install(launch, ctx, out).await {
            return fail(ctx, out, reason);
        }

        // Cancelled while the files came down. Preparation reports success in
        // that case -- there is nothing wrong to report -- so without this the
        // launch would carry straight on and start the game somebody had just
        // asked it not to. No `fail`: the join state was already cleared by
        // the cancel, and a launch failure on top of it would be a second,
        // wrong explanation for something the user did on purpose.
        if ctx.lobby.launch_cancelled() {
            tracing::info!("launcher: the join was cancelled during preparation; not starting");
            return abandon(ctx, out);
        }
    }

    // 2. Bring up the connectivity backend (it picks its own ports / control plane).
    // A game this client asked to host runs on the hosting preference, which
    // is what its title's mark announced. Taken either way: the next launch
    // is somebody else's game unless a new host request says otherwise.
    let hosted = ctx
        .lobby
        .take_hosted_title()
        .is_some_and(|title| title == launch.name);
    let session = match ctx
        .ports
        .ice
        .start(IceParams {
            player_id: player.id,
            player_login: player.name.clone(),
            game_id: launch.uid,
            init_mode,
            game_title: launch.name.clone(),
            hosted,
        })
        .await
    {
        Ok(session) => session,
        Err(e) => return fail(ctx, out, format!("ice adapter: {e}")),
    };
    // The adapter takes seconds to come up, and a matchmaker launch can be
    // called off by the server in that time (`match_cancelled`). Starting the
    // game after that would seat the player in a match nobody else is in.
    if ctx.lobby.launch_cancelled() {
        tracing::info!(
            "launcher: the launch was cancelled while the adapter started; not starting"
        );
        return abandon(ctx, out);
    }

    // 3. Launch the game pointed at the adapter's GPGNet port.
    let game_params = GameLaunchParams {
        game_id: launch.uid,
        game_port: session.game_port,
        init_mode,
        featured_mod: launch.mod_name.clone(),
        player_id: player.id,
        player_login: player.name.clone(),
        args: launch_arguments(launch, player_profile.as_ref()),
        replay: replay_metadata(launch, &player.name, ctx, out),
    };
    if let Err(e) = ctx.ports.process.launch_game(game_params).await {
        ctx.ports.ice.stop();
        return fail(ctx, out, format!("game launch: {e}"));
    }
    // Java cancels delayed replay actions as soon as GameRunner becomes active.
    // Do the same so auto-watch can never replace a game the user just launched.
    super::replays::cancel_live_tracking(out);

    // 4. Pump adapter → lobby. The reverse direction is driven by the lobby loop
    //    via `forward_to_adapter`.
    let lobby = ctx.ports.lobby.clone();
    let mut to_lobby = session.to_lobby;
    tokio::spawn(async move {
        tracing::debug!("launcher: adapter->lobby pump started");
        while let Some(msg) = to_lobby.recv().await {
            tracing::trace!(command = %msg.command, "launcher: adapter -> server lobby");
            lobby.send_game_relay(msg.command, msg.args);
        }
        tracing::debug!("launcher: adapter->lobby pump ended (adapter session channel closed)");
    });

    // 5. Notice when the game exits.
    //
    // Nothing used to. The client stayed `InGame` until the user explicitly
    // terminated, so after a failed join the Play tab kept reporting a game in
    // progress and refused another attempt: the reported "stuck joining
    // forever, cannot try again".
    //
    // `GameState Ended` goes to the server first, as the Python client does in
    // `GameSession._exited`. Without it the server still believes this player
    // is in the game, which is its own reason a rejoin can be refused.
    let exit_ports = ctx.ports.clone();
    let exit_sink = out.clone();
    let exit_running_game = ctx.lobby.running_game_handle();
    tokio::spawn(async move {
        tracing::debug!("launcher: game exit watcher started");
        exit_ports.process.wait_for_exit().await;
        tracing::info!("the game process exited; releasing the launch");
        tracing::debug!("sending GameState Ended to the server");
        exit_ports
            .lobby
            .send_game_relay("GameState".into(), vec![Value::String("Ended".into())]);
        tracing::debug!("stopping ICE adapter");
        exit_ports.ice.stop();
        exit_running_game.clear();
        exit_sink.emit(LobbyEvent::GameTerminated);

        // The replay the game just streamed to the local recorder is on disk
        // now. Re-listing here is what makes it appear in the Local tab without
        // the user knowing to press refresh: the scan is a directory read, and
        // it only happens once per game.
        match exit_ports
            .replay_library
            .list_local(DEFAULT_LOCAL_REPLAY_LIMIT)
            .await
        {
            Ok(replays) => exit_sink.emit(ReplayEvent::LocalLoaded { replays }),
            Err(reason) => {
                tracing::warn!(%reason, "could not refresh the local replay list after the game")
            }
        }
        tracing::info!("launcher: game exit cleanup complete");
    });

    // Which game is being played, for as long as it is. Read by the lobby
    // service when the socket comes back: the server drops a player's game
    // connection with the socket it was made on, and without being told to
    // restore it the running game is relayed for nobody.
    ctx.lobby.set_running_game(launch.uid);

    out.emit(LobbyEvent::InGame);
    Some(LaunchSession {
        from_lobby: session.from_lobby,
    })
}

/// Prepare a selected custom game before asking the server for a seat. Java's
/// `prepareAndLaunchGameWhenReady` does this in the same order so a large patch,
/// map, or simulation-mod download cannot consume the server's launch window.
pub(crate) async fn prepare_custom_join(
    game: &Game,
    ctx: &ServiceCtx,
    out: &EventSink,
    replace_mods: bool,
) -> Result<(), ModPrepFailure> {
    // Validate this before generating or downloading anything. Apart from
    // producing a much more useful error, this prevents spending minutes on
    // preparation for a game that cannot possibly be launched.
    if ctx.ports.process.game_install_dir().is_none() {
        return Err(ModPrepFailure::Failed(
            "no game install configured: locate ForgedAlliance.exe in Settings → Paths".to_string(),
        ));
    }

    prepare_map_and_mod(&game.map, &game.mod_name, ctx, out)
        .await
        .map_err(ModPrepFailure::Failed)?;

    // Conflicts travel out untouched: the caller turns them into the prompt
    // that decides whether an installed mod version is allowed to be replaced.
    ctx.ports
        .mods
        .ensure_game_mods(&game.sim_mods, replace_mods)
        .await
        .map_err(|error| match error {
            ModPrepFailure::Conflicts(conflicts) => ModPrepFailure::Conflicts(conflicts),
            ModPrepFailure::Failed(reason) => {
                ModPrepFailure::Failed(format!("could not prepare simulation mods: {reason}"))
            }
        })
}

/// Prepare the map a host picked, before the host request reaches the server.
///
/// The server's `game_launch` names no map for a host: `mapname` is part of the
/// matchmaker's `GameLaunchOptions`, which is how the server tells a client
/// about a map *it* chose. A host already told the server which map to use, so
/// the reply carries none - and the launch path's map download, which reads
/// exactly that field, therefore had nothing to fetch. The host went straight
/// into a lobby whose scenario was not on disk.
///
/// Downloading here rather than at launch also matches the Java client, which
/// resolves the map in `GameRunner.host` before the host request goes out, and
/// it means a map that cannot be fetched is reported before a lobby exists for
/// other players to join.
pub(crate) async fn prepare_host(
    config: &HostGameConfig,
    ctx: &ServiceCtx,
    out: &EventSink,
) -> Result<(), String> {
    // Same reasoning as the join path: a missing install makes everything
    // below pointless, and says so far more clearly than a failed launch.
    if ctx.ports.process.game_install_dir().is_none() {
        return Err(
            "no game install configured: locate ForgedAlliance.exe in Settings → Paths".to_string(),
        );
    }

    prepare_map_and_mod(&config.map, &config.mod_name, ctx, out).await
}

/// Bring the featured mod up to date and put `map` on disk.
///
/// A generated map is rebuilt from its name and never exists in the vault, so
/// it is produced first and then deliberately kept out of the download request:
/// asking the CDN for one is a guaranteed 404.
async fn prepare_map_and_mod(
    map: &str,
    featured_mod: &str,
    ctx: &ServiceCtx,
    out: &EventSink,
) -> Result<(), String> {
    use faf_domain::protocol::map_generator::is_generated_map;

    let cache_rolling_branches = out.with_state(|state| state.settings.game.cache_rolling_branches);
    ensure_generated_map(map, ctx, out).await?;
    if ctx.lobby.launch_cancelled() {
        return Ok(());
    }
    prepare_request(
        GamePreparation {
            featured_mod: featured_mod.to_string(),
            map_folder: (!map.is_empty() && !is_generated_map(map)).then(|| map.to_string()),
            cache_rolling_branches,
        },
        ctx,
        out,
    )
    .await
}

/// Make sure a generated map exists locally, generating it if not.
///
/// A no-op for ordinary maps: those ship with the game or come from the vault,
/// and are handled elsewhere. Returns `Err` only when the map *is* generated and
/// could not be produced, since that is the one case where continuing would
/// strand the player in an unloadable game.
async fn ensure_generated_map(
    map_name: &str,
    ctx: &ServiceCtx,
    out: &EventSink,
) -> Result<(), String> {
    use faf_domain::protocol::map_generator::is_generated_map;
    use faf_domain::state::{GeneratorStatus, MapGeneratorEvent};

    if !is_generated_map(map_name) {
        return Ok(());
    }
    if ctx.ports.map_generator.is_installed(map_name) {
        return Ok(());
    }

    let settings = ctx.ports.settings.load().await;
    if !settings.game.auto_generate_maps {
        return Err(format!(
            "map {map_name} is not installed and automatic map generation is disabled in settings"
        ));
    }

    tracing::info!(map_name, "generating map required by launch");
    if ctx.lobby.launch_cancelled() {
        return Ok(());
    }
    let generation = ctx.map_generator.begin();
    let mut updates = ctx
        .ports
        .map_generator
        .generate_named(map_name.to_string())
        .await;

    // Forward progress so the UI can show what the wait is for: generation
    // routinely takes tens of seconds.
    let mut outcome = Err("the map generator produced no result".to_string());
    let mut generated: Vec<String> = Vec::new();
    loop {
        let update = tokio::select! {
            update = updates.recv() => update,
            () = tokio::time::sleep(CANCEL_POLL) => {
                if ctx.lobby.launch_cancelled() {
                    if ctx.map_generator.is_current(generation) {
                        out.emit(MapGeneratorEvent::StatusChanged { status: GeneratorStatus::Cancelled });
                    }
                    return Ok(());
                }
                continue;
            }
        };
        if ctx.lobby.launch_cancelled() {
            if ctx.map_generator.is_current(generation) {
                out.emit(MapGeneratorEvent::StatusChanged {
                    status: GeneratorStatus::Cancelled,
                });
            }
            return Ok(());
        }
        let Some(crate::ports::GeneratorUpdate::Status(status)) = update else {
            break;
        };
        match &status {
            GeneratorStatus::Generated { maps } => {
                generated = maps.clone();
                outcome = Ok(())
            }
            GeneratorStatus::Failed { reason } => {
                outcome = Err(format!("could not generate {map_name}: {reason}"))
            }
            _ => {}
        }
        if ctx.map_generator.is_current(generation) {
            out.emit(MapGeneratorEvent::StatusChanged { status });
        }
    }
    // The same bookkeeping a deliberate run gets. Joining a lobby whose map
    // had to be built used to skip all of it, which is why the client went on
    // showing the map as missing, with no preview, while the player was already
    // in the game: the folder was on disk and nothing had looked again.
    super::map_generator::record_generated_maps(&generated, ctx, out).await;
    outcome
}

/// Patch the featured mod to the current build and download the map, narrating
/// progress as [`LobbyEvent::Preparing`].
///
/// Fatal on failure, unlike the same work on the replay path. A replay is a
/// recording the user chose to watch; here the server has already seated them
/// in a game, and starting an out-of-date client means a failed load, a desync,
/// or a leave the other players see as a drop. Reporting why beats all three.
async fn prepare_install(
    launch: &GameLaunch,
    ctx: &ServiceCtx,
    out: &EventSink,
) -> Result<(), String> {
    use faf_domain::protocol::map_generator::is_generated_map;

    // A generated map was already rebuilt above and is never in the vault, so
    // asking the CDN for it would be a guaranteed 404.
    let map_folder = (!launch.mapname.is_empty() && !is_generated_map(&launch.mapname))
        .then(|| launch.mapname.clone());

    let cache_rolling_branches = out.with_state(|state| state.settings.game.cache_rolling_branches);

    prepare_request(
        GamePreparation {
            featured_mod: launch.mod_name.clone(),
            map_folder,
            cache_rolling_branches,
        },
        ctx,
        out,
    )
    .await
}

/// One preparation at a time, whoever asked for it.
///
/// The updater writes into the game install and the maps folder, and two runs
/// at once write the same files. That became possible when a search started
/// preparing the install before the queue (see [`prepare_search`]): a match
/// can be found while that run is still going, and the launch's own run must
/// wait for it rather than race it. Java chains the two the same way, the
/// launch being the continuation of the search's preparation
/// (`GameRunner.startSearchMatchmaker`). The second run is then cheap, because
/// files matching by MD5 are skipped.
static PREPARATION: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// How often a preparation that holds [`PREPARATION`] looks whether it was
/// called off while its updater is quiet.
const CANCEL_POLL: std::time::Duration = std::time::Duration::from_millis(100);

async fn prepare_request(
    request: GamePreparation,
    ctx: &ServiceCtx,
    out: &EventSink,
) -> Result<(), String> {
    let _one_at_a_time = loop {
        tokio::select! {
            lease = PREPARATION.lock() => break lease,
            () = tokio::time::sleep(CANCEL_POLL) => {
                if ctx.lobby.launch_cancelled() { return Ok(()); }
            }
        }
    };
    if ctx.lobby.launch_cancelled() {
        return Ok(());
    }
    let cancel = tokio_util::sync::CancellationToken::new();
    let mut updates = ctx
        .ports
        .updater
        .prepare_cancellable(request, cancel.clone())
        .await;
    let mut outcome = Err("the game updater stopped without finishing".to_string());
    loop {
        let update = tokio::select! {
            update = updates.recv() => update,
            () = tokio::time::sleep(CANCEL_POLL) => {
                if ctx.lobby.launch_cancelled() {
                    cancel.cancel();
                    return Ok(());
                }
                continue;
            }
        };
        if ctx.lobby.launch_cancelled() {
            cancel.cancel();
            return Ok(());
        }
        let Some(update) = update else {
            break;
        };
        match update {
            UpdateProgress::Step(step) => out.emit(LobbyEvent::Preparing {
                phase: preparation_phase(step.phase),
                detail: step.detail,
                progress: step.progress,
            }),
            UpdateProgress::Finished(result) => outcome = result,
        }
    }
    outcome
}

/// Get the install ready for a matchmaker search, before the server is asked
/// to queue anybody.
///
/// Java's `TeamMatchmakingService.joinQueues`: the featured mod is brought up
/// to date, then each queue's pool maps are downloaded, and only then is
/// `game_matchmaking start` sent. The reason is the server's launch window. A
/// match can be made at the very next pop, and the host then has sixty seconds
/// to start the game (`LadderService.launch_match`); a patch day or a pool map
/// nobody has yet takes longer than that, and the server cancels the match for
/// every player in it and records a violation against the one who was late.
///
/// The progress is not narrated: nothing is being joined yet, and the join
/// state is what the launch dialog opens on. Java shows it as a background
/// task, which is what the search bar's "Preparing" is here.
///
/// A pool map that cannot be fetched is not a reason to stay out of the
/// queue, as in Java, which only reports it: the match may well be on another
/// map, and if not, the launch tries again. A featured mod that cannot be
/// updated is, since every match in the queue needs it.
pub(crate) async fn prepare_search(
    queue_names: &[String],
    ctx: &ServiceCtx,
    out: &EventSink,
) -> Result<(), String> {
    use faf_domain::protocol::map_generator::is_generated_map;

    if ctx.ports.process.game_install_dir().is_none() {
        return Err(
            "no game install configured: locate ForgedAlliance.exe in Settings → Paths".to_string(),
        );
    }

    prepare_featured_mod(MATCHMAKER_FEATURED_MOD, ctx, out).await?;

    let mut folders: Vec<String> = Vec::new();
    for queue_name in queue_names {
        match ctx
            .ports
            .maps
            .list_matchmaker_pools(queue_name.clone())
            .await
        {
            Ok(pools) => {
                for map in pools.iter().flat_map(|pool| pool.maps.iter()) {
                    if !map.folder_name.is_empty()
                        && !is_generated_map(&map.folder_name)
                        && !folders.contains(&map.folder_name)
                    {
                        folders.push(map.folder_name.clone());
                    }
                }
            }
            // Not knowing the pool is not knowing which maps to fetch, which
            // the launch will make up for. Java goes on to the next queue too.
            Err(reason) => {
                tracing::warn!(queue = %queue_name, %reason, "could not read the map pool before the search")
            }
        }
    }

    let failures = {
        let _one_at_a_time = PREPARATION.lock().await;
        ctx.ports.updater.ensure_maps(&folders).await
    };
    for (folder, reason) in failures {
        tracing::warn!(%folder, %reason, "a pool map could not be downloaded before the search");
        notifications::add_text(
            out,
            NotificationKind::Error,
            notifications::Text::new("notifications.msg.mapDownloadFailed")
                .with("folder", &folder)
                .with("reason", &reason),
            "Map download failed",
            format!("{folder} could not be downloaded: {reason}"),
            None,
        );
    }
    Ok(())
}

/// Bring a featured mod up to date without narrating it as a join.
///
/// Also what a party member's client does when its leader starts a search:
/// Java's `GameRunner.startSearchMatchmaker` runs on every client whose queue
/// state turns to searching, not only on the one that pressed the button.
pub(crate) async fn prepare_featured_mod(
    featured_mod: &str,
    ctx: &ServiceCtx,
    out: &EventSink,
) -> Result<(), String> {
    let cache_rolling_branches = out.with_state(|state| state.settings.game.cache_rolling_branches);
    let _one_at_a_time = PREPARATION.lock().await;
    let mut updates = ctx
        .ports
        .updater
        .prepare(GamePreparation {
            featured_mod: featured_mod.to_string(),
            map_folder: None,
            cache_rolling_branches,
        })
        .await;
    let mut outcome = Err("the game updater stopped without finishing".to_string());
    while let Some(update) = updates.recv().await {
        if let UpdateProgress::Finished(result) = update {
            outcome = result;
        }
    }
    outcome
}

/// The featured mod every matchmaker game runs on. Java joins the queues after
/// `featuredModService.updateFeaturedModToLatest(FAF.getTechnicalName())`.
pub(crate) const MATCHMAKER_FEATURED_MOD: &str = "faf";

/// The port's phase as the domain names it.
///
/// Two enums rather than one shared type, because the port describes work and
/// the domain describes state: the adapter boundary is exactly where a new
/// kind of work should be free to appear without the reducer's snapshot
/// changing shape.
fn preparation_phase(phase: PreparationPhase) -> DomainPreparationPhase {
    match phase {
        PreparationPhase::Asking => DomainPreparationPhase::Asking,
        PreparationPhase::Verifying => DomainPreparationPhase::Verifying,
        PreparationPhase::Downloading => DomainPreparationPhase::Downloading,
        PreparationPhase::Map => DomainPreparationPhase::Map,
    }
}

/// Stop the adapter and emit `LaunchFailed`. Always returns `None` so call sites
/// can `return fail(..)`.
pub(crate) fn report_failure(ctx: &ServiceCtx, out: &EventSink, reason: String) {
    // The reason too: "details were sent to the client" left a log that could
    // not say why any launch had failed, and the reason is an error message,
    // nothing private.
    tracing::warn!(%reason, "game launch failed");
    ctx.ports.ice.stop();
    notifications::add_required_text(
        out,
        NotificationKind::Error,
        notifications::Text::new("notifications.msg.gameLaunchFailed"),
        "Game launch failed",
        reason.clone(),
        None,
    );
    out.emit(LobbyEvent::LaunchFailed { reason });
}

/// A launch order that will not become a game, for whatever reason.
///
/// The server seated the player when it sent the order, and it learns that the
/// seat is empty from the client alone: `GameState Ended` is what the game
/// itself would have reported on exit. Java sends it on every way out of
/// `GameRunner.startOnlineGame`, failures included (`notifyGameEnded` in its
/// `whenComplete`). Without it the server waits out its own launch window
/// first: the other players sit on a match that cannot start for a minute or
/// more, and the player who failed is still "in a game" when they try again.
///
/// The server ignores the report for a player it has no game connection for
/// (`LobbyConnection.on_message_received`), so sending it is never wrong.
fn release_seat(ctx: &ServiceCtx) {
    ctx.ports
        .lobby
        .send_game_relay("GameState".into(), vec![Value::String("Ended".into())]);
}

fn fail(ctx: &ServiceCtx, out: &EventSink, reason: String) -> Option<LaunchSession> {
    release_seat(ctx);
    report_failure(ctx, out, reason);
    None
}

/// A launch called off on purpose: by the player, or by the server's
/// `match_cancelled`. No failure to report, but the seat is released all the
/// same, and the join is closed, which for a matchmaker launch also frees the
/// search panel (see `LobbyEvent::JoinCancelled`). A cancel from the player
/// closed it already; one from the server did not.
fn abandon(ctx: &ServiceCtx, out: &EventSink) -> Option<LaunchSession> {
    ctx.ports.ice.stop();
    release_seat(ctx);
    out.emit(LobbyEvent::JoinCancelled);
    None
}

/// Custom games init in NORMAL mode (0); matchmaker games in AUTO (1).
fn init_mode_for(game_type: &str) -> i32 {
    if game_type == "matchmaker" {
        1
    } else {
        0
    }
}

/// Describe the game for the header of the replay this launch will record.
///
/// `game_launch` carries the identity of the game (uid, title, map, mod) but not
/// who is in it, so the lobby's own listing is consulted for the teams, the host
/// and the launch time. It is legitimately absent on the matchmaker path, where
/// the game exists before it is ever listed publicly; the recording still gets a
/// correct map, title and mod, and falls back to its own start time for the
/// date. Nothing here is worth failing a launch over.
fn replay_metadata(
    launch: &GameLaunch,
    player: &str,
    ctx: &ServiceCtx,
    out: &EventSink,
) -> ReplayMetadata {
    let game = out.with_state(|state| {
        state
            .lobby
            .games
            .iter()
            .find(|game| game.id == launch.uid)
            .cloned()
    });
    let (git_sha, git_short_sha, signature, version_name) =
        if let Some(build) = ctx.ports.updater.installed_build() {
            let short = build.git_short_sha;
            let name = if launch.mod_name == "fafdevelop" {
                short.as_ref().map(|s| format!("FAF Develop ({s})"))
            } else if launch.mod_name == "fafbeta" {
                short.as_ref().map(|s| format!("FAF Beta ({s})"))
            } else {
                None
            };
            (build.git_sha, short, build.signature, name)
        } else {
            (None, None, None, None)
        };
    ReplayMetadata {
        uid: launch.uid,
        recorder: player.to_string(),
        featured_mod: launch.mod_name.clone(),
        title: if launch.name.is_empty() {
            game.as_ref()
                .map(|game| game.title.clone())
                .unwrap_or_default()
        } else {
            launch.name.clone()
        },
        map_name: launch.mapname.clone(),
        game_type: launch.game_type.clone(),
        host: game
            .as_ref()
            .map(|game| game.host.clone())
            .unwrap_or_default(),
        launched_at: game.as_ref().and_then(|game| game.launched_at),
        num_players: game.as_ref().map(|game| game.players).unwrap_or_default(),
        teams: game
            .as_ref()
            .map(|game| game.teams.clone())
            .unwrap_or_default(),
        sim_mods: game.map(|game| game.sim_mods).unwrap_or_default(),
        git_sha,
        git_short_sha,
        signature,
        version_name,
    }
}

/// What Forged Alliance is told when this client cannot say what the player is
/// rated: TrueSkill's starting pair, which is what the server itself seeds a
/// new account with and what the Python client sends in the same situation.
const DEFAULT_MEAN: i32 = 1_500;
const DEFAULT_DEVIATION: i32 = 500;

/// Add the player and automatic-lobby arguments that the server deliberately
/// does not own. This mirrors Java's `LaunchCommandBuilder` and Python's
/// `handle_game_launch` rather than deriving a displayed rating from the game
/// list, which has already discarded the TrueSkill deviation.
fn launch_arguments(launch: &GameLaunch, profile: Option<&PlayerProfile>) -> Vec<String> {
    let mut args = launch.args.clone();
    let has = |args: &[String], flag: &str| {
        args.iter()
            .any(|argument| argument.eq_ignore_ascii_case(flag))
    };
    let push_pair = |args: &mut Vec<String>, flag: &str, value: String| {
        if !has(args, flag) {
            args.push(flag.to_string());
            args.push(value);
        }
    };

    // The rating pair is never omitted.
    //
    // A report of a rating that read wrong in the in-game lobby came with the
    // command line: `/mean` and `/deviation` were both missing, so Forged
    // Alliance fell back to whatever it makes of an unrated player. That is
    // what this used to do whenever the queue named by the launch was not in
    // the profile's table -- a queue the account has never played, a profile
    // that arrived without its ratings, or no profile at all. Neither
    // reference client leaves the pair out: Java sends the leaderboard's
    // numbers or zeroes, and the Python client falls back to the TrueSkill
    // starting values, which is what is mirrored here because it is the pair
    // that describes an unrated player rather than one rated nothing.
    let rating_type = if launch.rating_type.is_empty() {
        "global"
    } else {
        &launch.rating_type
    };
    let rating = profile.and_then(|profile| {
        profile
            .ratings
            .iter()
            .find(|rating| rating.leaderboard.eq_ignore_ascii_case(rating_type))
            // A queue nobody has played yet still starts everybody at the
            // global rating in the lobby, which is better than the default.
            .or_else(|| {
                profile
                    .ratings
                    .iter()
                    .find(|rating| rating.leaderboard.eq_ignore_ascii_case("global"))
            })
    });
    push_pair(
        &mut args,
        "/mean",
        rating
            .map_or(DEFAULT_MEAN, |rating| rating.mean)
            .to_string(),
    );
    push_pair(
        &mut args,
        "/deviation",
        rating
            .map_or(DEFAULT_DEVIATION, |rating| rating.deviation)
            .to_string(),
    );

    if let Some(profile) = profile {
        if !profile.country.is_empty() {
            push_pair(&mut args, "/country", profile.country.clone());
        }
        if !profile.clan.is_empty() {
            push_pair(&mut args, "/clan", profile.clan.clone());
        }
        let games = profile
            .ratings
            .iter()
            .map(|rating| rating.games_played)
            .sum::<i32>();
        push_pair(&mut args, "/numgames", games.max(0).to_string());
    }

    if launch.game_type.eq_ignore_ascii_case("matchmaker") {
        if let Some(faction) = launch.faction.and_then(faction_argument) {
            if !has(&args, faction) {
                args.push(faction.to_string());
            }
        }
        if let Some(team) = launch.team {
            push_pair(&mut args, "/team", team.to_string());
        }
        if let Some(players) = launch.expected_players {
            push_pair(&mut args, "/players", players.to_string());
        }
        if let Some(position) = launch.map_position {
            push_pair(&mut args, "/startspot", position.to_string());
        }
        if !launch.game_options.is_empty() && !has(&args, "/gameoptions") {
            args.push("/gameoptions".into());
            args.extend(
                launch
                    .game_options
                    .iter()
                    .map(|(name, value)| format!("{name}:{value}")),
            );
        }
    }

    args
}

fn faction_argument(faction: i32) -> Option<&'static str> {
    match faction {
        1 => Some("/uef"),
        2 => Some("/aeon"),
        3 => Some("/cybran"),
        4 => Some("/seraphim"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faf_domain::state::PlayerLobbyRating;
    use std::collections::BTreeMap;

    #[test]
    fn init_mode_normal_for_custom_auto_for_matchmaker() {
        assert_eq!(init_mode_for("custom"), 0);
        assert_eq!(init_mode_for(""), 0);
        assert_eq!(init_mode_for("matchmaker"), 1);
    }

    fn profile() -> PlayerProfile {
        PlayerProfile {
            id: 7,
            login: "Commander".into(),
            global_rating: 1_200,
            ratings: vec![PlayerLobbyRating {
                leaderboard: "global".into(),
                rating: 1_200,
                mean: 1_800,
                deviation: 200,
                games_played: 374,
            }],
            country: "de".into(),
            clan: "BC".into(),
            ..PlayerProfile::default()
        }
    }

    fn launch() -> GameLaunch {
        GameLaunch {
            uid: 1,
            mod_name: "faf".into(),
            name: "Game".into(),
            mapname: "scmp_007".into(),
            game_type: "custom".into(),
            rating_type: "global".into(),
            expected_players: None,
            team: None,
            faction: None,
            map_position: None,
            game_options: BTreeMap::new(),
            args: Vec::new(),
        }
    }

    #[test]
    fn custom_launch_includes_the_players_true_skill_identity() {
        let args = launch_arguments(&launch(), Some(&profile()));
        assert_eq!(
            args,
            [
                "/mean",
                "1800",
                "/deviation",
                "200",
                "/country",
                "de",
                "/clan",
                "BC",
                "/numgames",
                "374",
            ]
        );
    }

    #[test]
    fn matchmaker_launch_includes_automatic_lobby_seating() {
        let mut launch = launch();
        launch.game_type = "matchmaker".into();
        launch.faction = Some(3);
        launch.team = Some(2);
        launch.expected_players = Some(4);
        launch.map_position = Some(3);
        launch.game_options.insert("Timeouts".into(), "3".into());

        let args = launch_arguments(&launch, Some(&profile()));
        assert!(args.windows(2).any(|pair| pair == ["/team", "2"]));
        assert!(args.windows(2).any(|pair| pair == ["/players", "4"]));
        assert!(args.windows(2).any(|pair| pair == ["/startspot", "3"]));
        assert!(args.iter().any(|argument| argument == "/cybran"));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["/gameoptions", "Timeouts:3"]));
    }

    #[test]
    fn a_queue_the_account_never_played_still_launches_with_its_global_rating() {
        let mut launch = launch();
        launch.rating_type = "tmm_2v2".into();
        let args = launch_arguments(&launch, Some(&profile()));
        assert!(args.windows(2).any(|pair| pair == ["/mean", "1800"]));
        assert!(args.windows(2).any(|pair| pair == ["/deviation", "200"]));
    }

    #[test]
    fn an_unknown_rating_still_gets_the_true_skill_starting_pair() {
        // The report this fixes: both flags missing from the command line, and
        // a rating that reads wrong in the in-game lobby as a result.
        let args = launch_arguments(&launch(), None);
        assert!(args.windows(2).any(|pair| pair == ["/mean", "1500"]));
        assert!(args.windows(2).any(|pair| pair == ["/deviation", "500"]));

        let unrated = PlayerProfile {
            ratings: Vec::new(),
            ..profile()
        };
        let args = launch_arguments(&launch(), Some(&unrated));
        assert!(args.windows(2).any(|pair| pair == ["/mean", "1500"]));
        assert!(args.windows(2).any(|pair| pair == ["/deviation", "500"]));
    }

    #[test]
    fn server_supplied_values_are_not_duplicated() {
        let mut launch = launch();
        launch.args = vec![
            "/mean".into(),
            "1900".into(),
            "/numgames".into(),
            "8".into(),
        ];
        let args = launch_arguments(&launch, Some(&profile()));
        assert_eq!(args.iter().filter(|arg| *arg == "/mean").count(), 1);
        assert_eq!(args.iter().filter(|arg| *arg == "/numgames").count(), 1);
    }
}
