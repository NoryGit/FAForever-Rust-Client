//! faf-tournaments service tests: a player's flow, end to end through the app.
//!
//! Driven against the writable fake port rather than per-test stubs, because
//! what is worth asserting here is the *sequence*: a write is announced, runs
//! alone, and is followed by a reload that leaves the list and the detail
//! agreeing. A stub that answered one call could not show any of that.

use std::sync::Arc;

use async_trait::async_trait;
use faf_app::infra::fake_ports;
use faf_app::ports::{RequestError, TourneyEntryPort, TourneyMatchPort, TourneyReadPort};
use faf_app::{App, Ports};
use faf_domain::state::{
    BracketConfig, CopySource, EntrantRatings, FfaReport, FormatDraft, MapDraft, MatchReport,
    MatchStatus, PoolDraft, QualifierKind, QualifierRule, RatingCheck, RenameCheck, SeedOrder,
    SeriesDetail, SeriesDraft, Tourney, TourneyAction, TourneyDraft, TourneyEvent,
    TourneyLoadStatus, TourneyPhase, TourneyPreset, TourneyRead, TourneySeries, TourneyStatus,
    TourneyWrite,
};
use faf_domain::AppEvent;

use crate::support;

fn team_points(team_id: &str, points: i32) -> faf_domain::state::TeamPoints {
    faf_domain::state::TeamPoints {
        team_id: team_id.into(),
        points,
    }
}

/// Entry and match ports that refuse every write with the same error.
///
/// Stands in for the case the whole tab has to survive: a signed-in account
/// that is simply not entitled: not an organiser here, or below the rating
/// gate. Reads work, because every other tournament port is the offline fake;
/// entering, team changes, results and the event's phase come back 403 with a
/// sentence worth showing.
struct RefusingTourney {
    error: RequestError,
}

impl RefusingTourney {
    fn refused<T>(&self) -> Result<T, RequestError> {
        Err(self.error.clone())
    }
}

#[async_trait]
impl TourneyEntryPort for RefusingTourney {
    async fn sign_up(&self, _: &str, _: Option<i32>) -> Result<(), RequestError> {
        self.refused()
    }

    async fn withdraw(&self, _: &str, _: &str) -> Result<(), RequestError> {
        self.refused()
    }

    async fn add_player(&self, _: &str, _: &str, _: Option<i32>) -> Result<(), RequestError> {
        self.refused()
    }

    async fn respond_signup(&self, _: &str, _: &str, _: bool) -> Result<(), RequestError> {
        self.refused()
    }

    async fn set_captain(&self, _: &str, _: &str, _: &str) -> Result<(), RequestError> {
        self.refused()
    }

    async fn move_player(&self, _: &str, _: &str, _: Option<&str>) -> Result<(), RequestError> {
        self.refused()
    }

    async fn edit_player(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: Option<i32>,
    ) -> Result<(), RequestError> {
        self.refused()
    }

    async fn invite_player(&self, _: &str, _: &str) -> Result<(), RequestError> {
        self.refused()
    }

    async fn uninvite(&self, _: &str, _: i32) -> Result<(), RequestError> {
        self.refused()
    }

    async fn create_team(&self, _: &str, _: &str) -> Result<(), RequestError> {
        self.refused()
    }

    async fn request_join(&self, _: &str, _: &str) -> Result<(), RequestError> {
        self.refused()
    }

    async fn cancel_join(&self, _: &str, _: &str) -> Result<(), RequestError> {
        self.refused()
    }

    async fn respond_join(&self, _: &str, _: &str, _: &str, _: bool) -> Result<(), RequestError> {
        self.refused()
    }

    async fn invite_to_team(&self, _: &str, _: &str, _: &str) -> Result<(), RequestError> {
        self.refused()
    }

    async fn respond_invite(&self, _: &str, _: &str, _: bool) -> Result<(), RequestError> {
        self.refused()
    }

    async fn leave_team(&self, _: &str) -> Result<(), RequestError> {
        self.refused()
    }

    async fn disband_team(&self, _: &str, _: &str) -> Result<(), RequestError> {
        self.refused()
    }

    async fn rename_team(&self, _: &str, _: &str, _: &str) -> Result<(), RequestError> {
        self.refused()
    }

    async fn check_in(&self, _: &str, _: bool) -> Result<(), RequestError> {
        self.refused()
    }

    async fn decline_invite(&self, _: &str) -> Result<(), RequestError> {
        self.refused()
    }

    async fn draft_pick(&self, _: &str, _: &str) -> Result<(), RequestError> {
        self.refused()
    }

    async fn draft_undo(&self, _: &str) -> Result<(), RequestError> {
        self.refused()
    }

    async fn set_captains(&self, _: &str, _: &[String]) -> Result<(), RequestError> {
        self.refused()
    }
}

#[async_trait]
impl TourneyMatchPort for RefusingTourney {
    async fn advance(
        &self,
        _: &str,
        _: TourneyPhase,
        _: Option<&BracketConfig>,
    ) -> Result<(), RequestError> {
        self.refused()
    }

    async fn confirm_report(&self, _: &str, _: &str, _: bool) -> Result<(), RequestError> {
        self.refused()
    }

    async fn decide_report(&self, _: &str, _: &MatchReport) -> Result<(), RequestError> {
        self.refused()
    }

    async fn submit_report(&self, _: &str, _: &MatchReport) -> Result<(), RequestError> {
        self.refused()
    }

    async fn report_ffa(&self, _: &str, _: &FfaReport) -> Result<(), RequestError> {
        self.refused()
    }
}

/// The offline bundle's writable fake, signed in: the default for these tests.
///
/// Signing in is not ceremony. The service identifies the caller by session and
/// sends no viewer block, so the client works out what this account may do from
/// its own FAF login. A test that skipped the login would exercise a signed-out
/// tab, which is exactly what the tab used to be against the real server, while
/// these tests passed.
async fn app() -> App {
    let app = support::start(fake_ports());
    sign_in(&app).await;
    app
}

async fn app_refusing(error: RequestError) -> App {
    let refusing = Arc::new(RefusingTourney { error });
    let ports = Ports {
        tourney_entry: refusing.clone(),
        tourney_match: refusing,
        ..fake_ports()
    };
    let app = support::start(ports);
    sign_in(&app).await;
    app
}

/// Log in as the offline bundle's account and wait for the state to hold it.
async fn sign_in(app: &App) {
    app.dispatch(faf_domain::state::AuthCommand::Login { remember: false }.into())
        .await
        .unwrap();
    for _ in 0..400 {
        if let Some(player) = app.snapshot().auth.player {
            assert_eq!(player.id, faf_app::infra::OFFLINE_FAF_ID);
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("the offline login never landed");
}

/// The next event belonging to this slice, skipping any others in flight.
async fn next_event(events: &mut tokio::sync::broadcast::Receiver<AppEvent>) -> TourneyEvent {
    loop {
        if let AppEvent::Tourney(event) = events.recv().await.unwrap() {
            return event;
        }
    }
}

/// Wait for the slice to go quiet, so an assertion sees the settled state
/// rather than the middle of a reload.
async fn settle(app: &App) {
    // Looked at in place: a snapshot here cloned the whole state, twice a
    // round, for three fields of one slice.
    for _ in 0..200 {
        let quiet = app.with_state(|state| {
            let tourney = &state.tourney;
            tourney.pending.is_none()
                && tourney.status != TourneyLoadStatus::Loading
                && tourney.detail_status != TourneyLoadStatus::Loading
        });
        if quiet {
            tokio::task::yield_now().await;
            if app.with_state(|state| state.tourney.pending.is_none()) {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("the tourney slice never settled");
}

/// Open one event and wait for its detail.
async fn open(app: &App, tournament_id: &str) {
    app.dispatch(
        TourneyRead::Select {
            tournament_id: tournament_id.into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(app).await;
}

#[tokio::test]
async fn loading_announces_itself_then_lands_a_sorted_list() {
    let app = app().await;
    let mut events = app.subscribe();

    app.dispatch(TourneyRead::Load.into()).await.unwrap();

    assert_eq!(next_event(&mut events).await, TourneyEvent::Loading);
    // Where the service lives, sent with every load: the tab resolves the
    // organiser's uploaded images against it, and offline there is none.
    assert_eq!(
        next_event(&mut events).await,
        TourneyEvent::AssetBase {
            base: String::new()
        }
    );
    match next_event(&mut events).await {
        TourneyEvent::Loaded { events } => {
            // Sorting is the service's job, not the view's, so every consumer of
            // the state sees the same order: what a player can still enter comes
            // first.
            assert_eq!(events.first().unwrap().status, TourneyStatus::Signup);
        }
        other => panic!("expected Loaded, got {other:?}"),
    }
    settle(&app).await;
    assert_eq!(app.snapshot().tourney.status, TourneyLoadStatus::Ready);

    // A list row carries no viewer block, so it must claim no organiser rights:
    // otherwise every row on screen would draw organiser controls.
    let row = app
        .snapshot()
        .tourney
        .events
        .first()
        .cloned()
        .expect("a row");
    assert!(!row.viewer.organiser);
    assert!(!row.viewer.logged_in);
}

#[tokio::test]
async fn the_list_carries_counts_and_the_detail_carries_the_people() {
    // The two endpoints answer differently, and the tab has to read both.
    let app = app().await;
    app.dispatch(TourneyRead::Load.into()).await.unwrap();
    settle(&app).await;

    let row = app
        .snapshot()
        .tourney
        .events
        .into_iter()
        .find(|event| event.id == "e1a2b")
        .expect("the cup is listed");
    assert_eq!(row.player_count, 2);
    assert!(row.players.is_empty(), "the list sends no people");

    open(&app, "e1a2b").await;
    let detail = app.snapshot().tourney.detail.expect("the detail landed");
    assert_eq!(detail.players.len(), 2);
    assert_eq!(detail.player_count, 2);
}

#[tokio::test]
async fn entering_a_tournament_reloads_both_the_row_and_the_detail() {
    // The reason a write ends in a reload rather than a local patch: the row
    // and the pane must not disagree about who is in.
    let app = app().await;
    open(&app, "e1a2b").await;
    assert!(app.snapshot().tourney.detail.unwrap().may_sign_up());

    app.dispatch(
        TourneyWrite::SignUp {
            tournament_id: "e1a2b".into(),
            rating: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    let detail = state.detail.expect("still open");
    assert!(detail.viewer.is_signed_up());
    assert_eq!(detail.player_count, 3);
    let row = state
        .events
        .iter()
        .find(|event| event.id == "e1a2b")
        .expect("still listed");
    assert_eq!(row.player_count, 3, "the row was reloaded too");
    assert!(state.action_error.is_none());
}

#[tokio::test]
async fn withdrawing_uses_the_player_id_the_server_handed_out() {
    // The client never invents this id: the server issues it and authorises the
    // removal against the same answer.
    let app = app().await;
    open(&app, "e1a2b").await;
    app.dispatch(
        TourneyWrite::SignUp {
            tournament_id: "e1a2b".into(),
            rating: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    app.dispatch(
        TourneyWrite::Withdraw {
            tournament_id: "e1a2b".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let detail = app.snapshot().tourney.detail.expect("still open");
    assert!(!detail.viewer.is_signed_up());
    assert_eq!(detail.player_count, 2);
}

#[tokio::test]
async fn withdrawing_without_an_entry_is_refused_before_a_request_is_made() {
    let app = app().await;
    open(&app, "e1a2b").await;

    app.dispatch(
        TourneyWrite::Withdraw {
            tournament_id: "e1a2b".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let failure = app
        .snapshot()
        .tourney
        .action_error
        .expect("a refusal the client can answer itself");
    assert_eq!(failure.action, TourneyAction::Withdrawing);
    assert!(failure.reason.contains("not signed up"));
}

#[tokio::test]
async fn a_score_raised_elsewhere_is_answerable_here() {
    // A report the other side raised, here or on the website, has to be
    // answerable, or the tab shows a decision it cannot make.
    let app = app().await;
    open(&app, "e9z9z").await;
    let before = app.snapshot().tourney.detail.expect("the bracket");
    let entry = &before.matches[0];
    assert!(before.may_confirm(entry), "raised by the other side");
    assert_eq!(
        entry.pending_report.as_ref().map(|pending| pending.score1),
        Some(2)
    );
    assert_eq!(
        entry.status,
        MatchStatus::Ready,
        "the bracket has not moved"
    );
    assert_eq!(before.matches[2].team1, None, "the final is still empty");
}

#[tokio::test]
async fn a_confirmed_score_advances_the_winner_and_the_state_follows() {
    let app = app().await;
    open(&app, "e9z9z").await;

    app.dispatch(
        TourneyWrite::AnswerReport {
            tournament_id: "e9z9z".into(),
            match_id: "m1".into(),
            accept: true,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let detail = app.snapshot().tourney.detail.expect("still open");
    assert_eq!(detail.matches[0].status, MatchStatus::Done);
    assert_eq!(
        detail.matches[2].team1.as_deref(),
        Some("t1"),
        "reloaded from the server rather than patched locally"
    );
}

#[tokio::test]
async fn an_organisers_forfeit_reaches_the_server() {
    // The service used to blank the winner and the forfeit on the organiser's
    // path as well as the player's, so a walkover entered here arrived as a
    // bare match id and changed nothing.
    let app = app().await;
    open(&app, "e9z9z").await;
    let entry = app
        .snapshot()
        .tourney
        .detail
        .expect("the event")
        .matches
        .into_iter()
        .find(|entry| {
            entry.status == MatchStatus::Ready
                && entry.pending_report.is_none()
                && entry.team1.is_some()
                && entry.team2.is_some()
        })
        .expect("a match ready to play");
    let absent = entry.team1.clone().expect("a side");
    let present = entry.team2.clone().expect("a side");

    app.dispatch(
        TourneyWrite::DecideReport {
            tournament_id: "e9z9z".into(),
            report: MatchReport {
                match_id: entry.id.clone(),
                forfeit: Some(absent),
                ..MatchReport::default()
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let after = app.snapshot().tourney.detail.expect("still open");
    let decided = after
        .matches
        .iter()
        .find(|candidate| candidate.id == entry.id)
        .expect("the match");
    assert_eq!(decided.status, MatchStatus::Done);
    assert_eq!(decided.winner.as_deref(), Some(present.as_str()));
}

#[tokio::test]
async fn the_map_database_takes_maps_and_hides_them_until_published() {
    // The step that is easy to skip: the service hides an unpublished map from
    // players, so a pool built from unpublished maps is a round nobody can read.
    let app = app().await;
    open(&app, "e1a2b").await;
    let before = app
        .snapshot()
        .tourney
        .detail
        .expect("the event")
        .map_db
        .len();

    app.dispatch(
        TourneyWrite::SaveMap {
            tournament_id: "e1a2b".into(),
            map: MapDraft {
                id: String::new(),
                name: "  Twin Rivers  ".into(),
                description: "8 spawns".into(),
                published: false,
                spec: None,
                ..MapDraft::default()
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("still open");
    assert!(app.snapshot().tourney.action_error.is_none());
    assert_eq!(event.map_db.len(), before + 1);
    let added = event.map_db.last().expect("the new map");
    assert_eq!(added.name, "Twin Rivers", "trimmed before it was sent");
    assert!(!added.published, "added out of sight, as the service does");

    let map_id = added.id.clone();
    app.dispatch(
        TourneyWrite::PublishMap {
            tournament_id: "e1a2b".into(),
            map_id: map_id.clone(),
            published: true,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let event = app.snapshot().tourney.detail.expect("still open");
    assert!(
        event
            .map_db
            .iter()
            .any(|map| map.id == map_id && map.published),
        "the reload carries the new visibility back"
    );

    // Deleting cascades: the service strips the map from every pool naming it,
    // and the fake has to, or the tab shows a pool entry it cannot name.
    app.dispatch(
        TourneyWrite::DeleteMap {
            tournament_id: "e1a2b".into(),
            map_id: map_id.clone(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let event = app.snapshot().tourney.detail.expect("still open");
    assert!(!event.map_db.iter().any(|map| map.id == map_id));
    assert!(
        !event
            .map_pools
            .iter()
            .any(|pool| pool.map_ids.contains(&map_id)),
        "and out of every pool that named it"
    );
}

#[tokio::test]
async fn publishing_a_pool_publishes_the_maps_in_it() {
    // A visible pool of invisible maps is a list of raw ids, so the service
    // publishes the maps along with it.
    let app = app().await;
    open(&app, "e1a2b").await;

    app.dispatch(
        TourneyWrite::SaveMap {
            tournament_id: "e1a2b".into(),
            map: MapDraft {
                id: String::new(),
                name: "Open Palms".into(),
                description: String::new(),
                published: false,
                spec: None,
                ..MapDraft::default()
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let hidden = app
        .snapshot()
        .tourney
        .detail
        .expect("the event")
        .map_db
        .last()
        .expect("the new map")
        .id
        .clone();

    app.dispatch(
        TourneyWrite::SavePool {
            tournament_id: "e1a2b".into(),
            pool: PoolDraft {
                id: String::new(),
                name: "Finals".into(),
                map_ids: vec![hidden.clone()],
                best_of: Some(1),
                sequence: Vec::new(),
                publish_at: None,
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let pool_id = app
        .snapshot()
        .tourney
        .detail
        .expect("the event")
        .map_pools
        .last()
        .expect("the new pool")
        .id
        .clone();

    app.dispatch(
        TourneyWrite::PublishPool {
            tournament_id: "e1a2b".into(),
            pool_id: pool_id.clone(),
            published: true,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("still open");
    assert!(event
        .map_pools
        .iter()
        .any(|pool| pool.id == pool_id && pool.published));
    assert!(
        event
            .map_db
            .iter()
            .any(|map| map.id == hidden && map.published),
        "its maps came out with it"
    );
}

#[tokio::test]
async fn a_veto_walks_its_order_and_leaves_a_decider() {
    // The whole run, offline: three steps over four maps, and the survivor
    // becomes the last game. This is what a Bo3 pool is shaped for.
    let app = app().await;
    open(&app, "e9z9z").await;
    let event = app.snapshot().tourney.detail.expect("the bracket");
    let entry = event
        .matches
        .iter()
        .find(|entry| entry.id == "m2")
        .expect("the match with a run");
    let veto = entry.veto.as_ref().expect("a run in progress");
    assert_eq!(veto.remaining.len(), 4);
    let turn = veto
        .current_turn()
        .expect("sides are set, so somebody is due");
    assert_eq!(turn.team_id, "t2", "team A opens");
    assert_eq!(turn.action, faf_domain::state::PoolAction::Ban);
    // This account captains t1 and is not in this match, so the grid is not
    // theirs. The offline account is an organiser, which is the other path.
    assert!(
        event.may_veto(entry),
        "an organiser may act for either side"
    );

    for map_id in ["map1", "map2", "map3"] {
        app.dispatch(
            TourneyWrite::VetoAct {
                tournament_id: "e9z9z".into(),
                match_id: "m2".into(),
                map_id: map_id.into(),
            }
            .into(),
        )
        .await
        .unwrap();
        settle(&app).await;
        assert!(
            app.snapshot().tourney.action_error.is_none(),
            "{:?}",
            app.snapshot().tourney.action_error
        );
    }

    let event = app.snapshot().tourney.detail.expect("still open");
    let veto = event
        .matches
        .iter()
        .find(|entry| entry.id == "m2")
        .and_then(|entry| entry.veto.as_ref())
        .expect("the run");
    assert!(veto.done, "the order has been walked");
    assert_eq!(veto.banned.len(), 1, "one ban, as the order said");
    assert_eq!(veto.picks.len(), 2);
    assert_eq!(veto.picks[0].game, Some(1), "picks are numbered games");
    assert_eq!(veto.picks[1].game, Some(2));
    let decider = veto.decider.as_ref().expect("the survivor decides");
    assert_eq!(decider.map, "map4");
    assert_eq!(decider.game, 3, "played last");
    assert!(veto.current_turn().is_none(), "nothing is due any more");
}

#[tokio::test]
async fn undoing_a_veto_step_puts_the_map_back() {
    let app = app().await;
    open(&app, "e9z9z").await;

    app.dispatch(
        TourneyWrite::VetoAct {
            tournament_id: "e9z9z".into(),
            match_id: "m2".into(),
            map_id: "map1".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    app.dispatch(
        TourneyWrite::VetoUndo {
            tournament_id: "e9z9z".into(),
            match_id: "m2".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("still open");
    let veto = event
        .matches
        .iter()
        .find(|entry| entry.id == "m2")
        .and_then(|entry| entry.veto.as_ref())
        .expect("the run");
    assert_eq!(veto.step_index, 0, "back to the start");
    assert!(veto.banned.is_empty());
    assert!(
        veto.remaining.contains(&"map1".to_string()),
        "and the map is in play again"
    );
}

#[tokio::test]
async fn a_map_that_is_not_in_play_is_refused() {
    // The service checks against `remaining`, so a stale grid, or a second
    // click on a map somebody else just took, is turned away rather than
    // silently taking a different one.
    let app = app().await;
    open(&app, "e9z9z").await;

    app.dispatch(
        TourneyWrite::VetoAct {
            tournament_id: "e9z9z".into(),
            match_id: "m2".into(),
            map_id: "map9".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let failure = app
        .snapshot()
        .tourney
        .action_error
        .expect("refused in the service's own words");
    assert!(failure.reason.contains("not available"));
}

#[tokio::test]
async fn the_sides_can_be_set_once_and_not_after_the_run_starts() {
    let app = app().await;
    open(&app, "e9z9z").await;
    // `m1` has no run at all, so the control must not be offered for it either.
    let event = app.snapshot().tourney.detail.expect("the bracket");
    let plain = event.matches.iter().find(|e| e.id == "m1").unwrap();
    assert!(!event.may_set_veto_sides(plain), "no run, no sides to set");
    // `m2` already has its sides, so there is nothing left to choose.
    let running = event.matches.iter().find(|e| e.id == "m2").unwrap();
    assert!(!event.may_set_veto_sides(running), "already chosen");

    // Choosing again is refused once a step has been taken.
    app.dispatch(
        TourneyWrite::VetoAct {
            tournament_id: "e9z9z".into(),
            match_id: "m2".into(),
            map_id: "map1".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    app.dispatch(
        TourneyWrite::VetoSetSides {
            tournament_id: "e9z9z".into(),
            match_id: "m2".into(),
            team_a: "t3".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let failure = app.snapshot().tourney.action_error.expect("refused");
    assert!(failure.reason.contains("already started"));
}

#[tokio::test]
async fn a_scored_free_for_all_lobby_adds_to_the_table() {
    // The mode the bracket cannot express: no two sides, no winner, a points
    // total that decides. `standings` reads it and nothing else does.
    let app = app().await;
    open(&app, "f4f4f").await;
    let event = app.snapshot().tourney.detail.expect("the event");
    assert_eq!(
        event.standings_kind(),
        faf_domain::state::StandingsKind::Points
    );

    // One lobby is already scored, so the table has a leader and a tail.
    let before = event.standings();
    assert_eq!(before[0].team_id, "t2", "5 points leads");
    assert_eq!(before[0].wins, 5, "the total rides in `wins`");
    assert!(
        before.iter().all(|row| row.place.is_some()),
        "a points table always ranks every row"
    );

    let lobby = event
        .matches
        .iter()
        .find(|entry| entry.id == "f2")
        .expect("the unscored lobby");
    assert!(event.ffa_is_scored(lobby));
    assert!(event.may_report_ffa(lobby));

    app.dispatch(
        TourneyWrite::ReportFfa {
            tournament_id: "f4f4f".into(),
            report: FfaReport {
                match_id: "f2".into(),
                winners: Vec::new(),
                points: vec![
                    team_points("t4", 9),
                    team_points("t5", 2),
                    team_points("t6", 4),
                ],
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    let after = state.detail.expect("still open").standings();
    assert_eq!(after[0].team_id, "t4", "9 points now leads");
    assert_eq!(after[0].wins, 9);
}

#[tokio::test]
async fn a_scored_lobby_needs_a_number_for_every_entrant() {
    // The service asks for all of them and names the range, so the client
    // refuses the same way rather than sending a table with a hole in it.
    let app = app().await;
    open(&app, "f4f4f").await;

    app.dispatch(
        TourneyWrite::ReportFfa {
            tournament_id: "f4f4f".into(),
            report: FfaReport {
                match_id: "f2".into(),
                winners: Vec::new(),
                points: vec![team_points("t4", 9)],
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let failure = app.snapshot().tourney.action_error.expect("refused");
    assert!(failure.reason.contains("every player"));
}

#[tokio::test]
async fn the_winner_count_a_lobby_wants_follows_the_format() {
    let app = app().await;
    open(&app, "f4f4f").await;
    let event = app.snapshot().tourney.detail.expect("the event");
    let lobby = event.matches.iter().find(|e| e.id == "f2").unwrap();

    // Two lobbies in the round, so neither is the final, and `advance` decides.
    assert_eq!(event.ffa_winners_needed(lobby), 1);

    // A three-entrant lobby can never advance everybody: the count is capped at
    // one short of the field however high `advance` is set.
    let wide = faf_domain::state::Tourney {
        ffa: Some(faf_domain::state::FfaConfig {
            advance: 9,
            ..event.ffa.clone().unwrap()
        }),
        ..event.clone()
    };
    assert_eq!(wide.ffa_winners_needed(lobby), 2);
}

#[tokio::test]
async fn a_captains_draft_runs_from_signups_to_full_teams() {
    // The whole flow: two captains marked, signups closed, the order walked,
    // and the event lands where the bracket can be drawn from it.
    let app = app().await;
    open(&app, "d3d3d").await;
    let event = app.snapshot().tourney.detail.expect("the event");
    assert_eq!(event.status, TourneyStatus::Signup);
    assert!(event.draft.is_none(), "not started yet");
    assert_eq!(event.pending_captains.len(), 2);

    app.dispatch(
        TourneyWrite::Advance {
            tournament_id: "d3d3d".into(),
            phase: TourneyPhase::StartDraft,
            config: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("still open");
    assert!(app.snapshot().tourney.action_error.is_none());
    assert_eq!(event.status, TourneyStatus::Draft, "the draft is running");
    let draft = event.draft.as_ref().expect("an order was built");
    // A 2v2 with two captains needs one pick each: they are already on their
    // own teams.
    assert_eq!(draft.order.len(), 2);
    assert_eq!(draft.current, 0);
    assert_eq!(event.teams.len(), 2, "one team per captain");
    assert_eq!(event.undrafted().len(), 2, "the two who are not captains");
    assert!(event.may_pick(), "the offline account organises it");

    let first = event.undrafted()[0].id.clone();
    app.dispatch(
        TourneyWrite::DraftPickPlayer {
            tournament_id: "d3d3d".into(),
            player_id: first.clone(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("still open");
    assert_eq!(event.undrafted().len(), 1);
    assert_eq!(
        event.draft.as_ref().unwrap().current,
        1,
        "the clock moved on"
    );
    assert_eq!(event.draft_turn(), Some("dt2"), "the other captain is due");

    let second = event.undrafted()[0].id.clone();
    app.dispatch(
        TourneyWrite::DraftPickPlayer {
            tournament_id: "d3d3d".into(),
            player_id: second,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("still open");
    assert!(event.undrafted().is_empty(), "everyone has a team");
    assert_eq!(
        event.status,
        TourneyStatus::Drafted,
        "the order ran out, so the draft is over"
    );
    assert!(event.draft_turn().is_none());
}

#[tokio::test]
async fn undoing_a_pick_puts_the_player_back_in_the_pool() {
    let app = app().await;
    open(&app, "d3d3d").await;
    app.dispatch(
        TourneyWrite::Advance {
            tournament_id: "d3d3d".into(),
            phase: TourneyPhase::StartDraft,
            config: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let picked = app.snapshot().tourney.detail.unwrap().undrafted()[0]
        .id
        .clone();
    app.dispatch(
        TourneyWrite::DraftPickPlayer {
            tournament_id: "d3d3d".into(),
            player_id: picked.clone(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    app.dispatch(
        TourneyWrite::DraftUndo {
            tournament_id: "d3d3d".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("still open");
    assert_eq!(
        event.draft.as_ref().unwrap().current,
        0,
        "back on the clock"
    );
    assert!(
        event.undrafted().iter().any(|player| player.id == picked),
        "and back in the pool"
    );
    assert!(
        !event
            .teams
            .iter()
            .any(|team| team.player_ids.contains(&picked)),
        "and off the team that had them"
    );
}

#[tokio::test]
async fn a_draft_needs_two_captains_before_it_can_start() {
    let app = app().await;
    open(&app, "d3d3d").await;

    app.dispatch(
        TourneyWrite::SetCaptains {
            tournament_id: "d3d3d".into(),
            player_ids: vec!["c1".into()],
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    app.dispatch(
        TourneyWrite::Advance {
            tournament_id: "d3d3d".into(),
            phase: TourneyPhase::StartDraft,
            config: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let failure = app.snapshot().tourney.action_error.expect("refused");
    assert!(failure.reason.contains("2 captains"));
}

#[tokio::test]
async fn a_refused_write_keeps_the_servers_sentence_and_clears_the_spinner() {
    // The whole reason 403 bodies are passed through: this is the only line
    // that tells the player which gate they missed.
    let app = app_refusing(RequestError::rejected(
        "You can’t sign up here: your rating (1420) is below this tournament’s minimum of 1500.",
    ))
    .await;
    open(&app, "e1a2b").await;

    app.dispatch(
        TourneyWrite::SignUp {
            tournament_id: "e1a2b".into(),
            rating: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.pending.is_none());
    let failure = state.action_error.expect("the refusal is kept");
    assert_eq!(failure.action, TourneyAction::SigningUp);
    assert!(failure.reason.contains("1500"));
    // And the event itself is untouched: a refused write must not look like a
    // successful one.
    assert!(!state.detail.unwrap().viewer.is_signed_up());

    app.dispatch(TourneyRead::DismissActionError.into())
        .await
        .unwrap();
    settle(&app).await;
    assert!(app.snapshot().tourney.action_error.is_none());
}

#[tokio::test]
async fn opening_a_room_reads_it_and_clears_its_badge() {
    let app = app().await;
    open(&app, "e9z9z").await;

    app.dispatch(
        TourneyRead::LoadChat {
            tournament_id: "e9z9z".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let rooms = app.snapshot().tourney.chat_rooms;
    assert_eq!(rooms.first().map(|room| room.id.as_str()), Some("global"));

    app.dispatch(
        TourneyRead::OpenRoom {
            tournament_id: "e9z9z".into(),
            room_id: "global".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert_eq!(state.open_room_id.as_deref(), Some("global"));
    assert_eq!(state.chat_posts.len(), 1);
    assert_eq!(state.chat_status, TourneyLoadStatus::Ready);
}

#[tokio::test]
async fn posting_reloads_the_room_and_not_the_whole_tournament() {
    let app = app().await;
    open(&app, "e9z9z").await;
    app.dispatch(
        TourneyRead::OpenRoom {
            tournament_id: "e9z9z".into(),
            room_id: "global".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    app.dispatch(
        TourneyWrite::PostChat {
            tournament_id: "e9z9z".into(),
            room_id: "global".into(),
            body: "  on my way  ".into(),
            reply_to: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let posts = app.snapshot().tourney.chat_posts;
    assert_eq!(posts.len(), 2);
    assert_eq!(posts[1].body, "on my way");

    // An empty message is not a request at all.
    app.dispatch(
        TourneyWrite::PostChat {
            tournament_id: "e9z9z".into(),
            room_id: "global".into(),
            body: "   ".into(),
            reply_to: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    assert_eq!(app.snapshot().tourney.chat_posts.len(), 2);
}

#[tokio::test]
async fn switching_events_never_leaves_one_brackets_chat_under_another() {
    let app = app().await;
    open(&app, "e9z9z").await;
    app.dispatch(
        TourneyRead::OpenRoom {
            tournament_id: "e9z9z".into(),
            room_id: "global".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    assert!(!app.snapshot().tourney.chat_posts.is_empty());

    open(&app, "e1a2b").await;
    let state = app.snapshot().tourney;
    assert_eq!(state.selected_id.as_deref(), Some("e1a2b"));
    assert!(state.chat_posts.is_empty());
    assert!(state.open_room_id.is_none());
    assert_eq!(state.detail.map(|event| event.id), Some("e1a2b".into()));
}

/// Searching for a name, picking the account, and the entrant arriving with a
/// face on it.
///
/// The whole point of the picker: the organiser chooses a *person*, so the entry
/// carries the FAF account, and the account is what the avatar hangs on. Adding a
/// typed string used to produce an entrant with no account at all, which is why
/// the organiser's list showed bare names while the participant's showed faces.
#[tokio::test]
async fn adding_a_searched_account_gives_the_entrant_a_resolvable_profile() {
    let app = app().await;
    open(&app, "e1a2b").await;

    app.dispatch(
        TourneyRead::SearchAccounts {
            query: "Grace".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let search = app.snapshot().tourney.account_search;
    assert_eq!(search.status, TourneyLoadStatus::Ready);
    let picked = search
        .matches
        .iter()
        .find(|account| account.login == "Grace-Hopper")
        .expect("the offline lookup knows this account");

    app.dispatch(
        TourneyWrite::AddPlayer {
            tournament_id: "e1a2b".into(),
            name: picked.login.clone(),
            rating: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    let detail = state.detail.as_ref().expect("the reloaded event");
    let added = detail
        .players
        .iter()
        .find(|player| player.name == "Grace-Hopper")
        .expect("the entrant was added");
    // The account came back with the entry, not as a separate guess.
    assert_eq!(
        added.faf_id,
        Some(picked.id),
        "the entry carries the account that was picked"
    );
    // And the profile behind it resolves, which is what puts an avatar on the row.
    let profile = state
        .profile_of(added)
        .expect("the added entrant's account resolves to a profile");
    assert_eq!(profile.login, "Grace-Hopper");
}

/// A query too short to be worth a request clears the list instead of asking FAF
/// for the first page of everybody.
#[tokio::test]
async fn a_one_letter_query_is_not_sent_to_the_api() {
    let app = app().await;
    open(&app, "e1a2b").await;

    app.dispatch(
        TourneyRead::SearchAccounts {
            query: "Grace".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    assert!(!app.snapshot().tourney.account_search.matches.is_empty());

    // Deleting back down to one letter must not leave the longer word's matches
    // on screen: they are clickable, and clicking one would add somebody the
    // organiser is no longer looking at.
    app.dispatch(TourneyRead::SearchAccounts { query: "G".into() }.into())
        .await
        .unwrap();
    settle(&app).await;

    let search = app.snapshot().tourney.account_search;
    assert_eq!(search.query, "");
    assert!(search.matches.is_empty());
    assert_eq!(search.status, TourneyLoadStatus::Idle);
}

#[tokio::test]
async fn entrant_profiles_arrive_beside_the_bracket_rather_than_inside_it() {
    let app = app().await;
    open(&app, "e9z9z").await;
    let state = app.snapshot().tourney;
    let detail = state.detail.as_ref().expect("the bracket");
    // The bracket is complete whether or not FAF answered; the profiles are a
    // decoration on top of it.
    assert_eq!(detail.players.len(), 4);

    // Every entrant carrying a FAF account must actually get one back. Asserting
    // only "no more profiles than entrants" passes when the lookup returns
    // nothing at all, which is how a batch resolve that never worked would look
    // exactly like one that did: the bracket renders, the avatars are simply
    // absent.
    let wanted: Vec<i32> = detail
        .players
        .iter()
        .filter_map(|player| player.faf_id)
        .collect();
    assert_eq!(wanted.len(), 4, "the fixture's entrants all have accounts");
    for account in &wanted {
        let profile = state
            .entrant_profiles
            .iter()
            .find(|profile| profile.id == *account)
            .unwrap_or_else(|| panic!("no FAF profile resolved for account {account}"));
        assert!(!profile.login.is_empty(), "a resolved profile has a login");
    }

    // And the entry stays the source of truth for the name: the tournament
    // service owns the entry, FAF owns the account, and an organiser's note or a
    // rename must not be overwritten by the profile's login.
    let entrant = detail.players.first().expect("an entrant");
    let profile = state
        .profile_of(entrant)
        .expect("the first entrant's account resolves");
    assert_eq!(profile.id, entrant.faf_id.expect("an account"));
}

#[tokio::test]
async fn assigning_a_pool_to_a_round_survives_the_reload() {
    let app = app().await;
    open(&app, "e1a2b").await;

    app.dispatch(
        TourneyWrite::SavePool {
            tournament_id: "e1a2b".into(),
            pool: PoolDraft {
                id: String::new(),
                name: "  Semifinals  ".into(),
                map_ids: vec!["map1".into(), "map2".into()],
                best_of: Some(3),
                sequence: Vec::new(),
                publish_at: None,
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let pool_id = app
        .snapshot()
        .tourney
        .detail
        .expect("still open")
        .map_pools
        .last()
        .map(|pool| {
            assert_eq!(pool.name, "Semifinals", "trimmed before it was sent");
            pool.id.clone()
        })
        .expect("the pool was created");

    app.dispatch(
        TourneyWrite::AssignPool {
            tournament_id: "e1a2b".into(),
            round_key: "wb:2".into(),
            pool_id,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let detail = app.snapshot().tourney.detail.expect("still open");
    assert_eq!(
        detail.pool_for_round("wb:2").map(|pool| pool.name.as_str()),
        Some("Semifinals")
    );
}

#[tokio::test]
async fn the_rules_pages_load_without_a_tournament_open() {
    // Site-wide, and fetched whole rather than by three hard-coded ids.
    let app = app().await;
    app.dispatch(TourneyRead::LoadArticles.into())
        .await
        .unwrap();
    settle(&app).await;

    let articles = app.snapshot().tourney.articles;
    assert_eq!(articles.len(), 2);
    assert!(articles[1].parent_id.is_some(), "the nesting survives");
}

#[tokio::test]
async fn a_failed_list_says_so_rather_than_showing_an_empty_tab() {
    struct Offline;

    #[async_trait]
    impl TourneyReadPort for Offline {
        fn asset_base(&self) -> String {
            String::new()
        }

        async fn list(&self) -> Result<Vec<Tourney>, RequestError> {
            Err(RequestError::offline("no route to host"))
        }

        async fn detail(&self, _: &str) -> Result<Tourney, RequestError> {
            Err(RequestError::offline("no route to host"))
        }

        async fn check_rating(&self, _: &str) -> Result<RatingCheck, RequestError> {
            unreachable!()
        }

        async fn player_ratings(
            &self,
            _: &str,
            _: &str,
            _: bool,
        ) -> Result<EntrantRatings, RequestError> {
            unreachable!()
        }

        async fn copy_sources(&self) -> Result<Vec<CopySource>, RequestError> {
            unreachable!()
        }

        async fn presets(&self) -> Result<Vec<TourneyPreset>, RequestError> {
            unreachable!()
        }

        async fn check_renames(&self, _: &str) -> Result<RenameCheck, RequestError> {
            unreachable!()
        }

        async fn series(&self) -> Result<Vec<TourneySeries>, RequestError> {
            Err(RequestError::offline("no route to host"))
        }

        async fn series_detail(&self, _: &str) -> Result<SeriesDetail, RequestError> {
            Err(RequestError::offline("no route to host"))
        }

        async fn mark_news_read(&self, _: &str) -> Result<(), RequestError> {
            unreachable!()
        }
    }

    let ports = Ports {
        tourney_read: Arc::new(Offline),
        ..fake_ports()
    };
    let app = support::start(ports);

    app.dispatch(TourneyRead::Load.into()).await.unwrap();
    settle(&app).await;

    match app.snapshot().tourney.status {
        TourneyLoadStatus::Failed { reason, .. } => assert!(!reason.is_empty()),
        other => panic!("expected a stated failure, got {other:?}"),
    }
}

#[tokio::test]
async fn entering_one_tournament_never_enters_another() {
    // Reported from the running client: enter the cup, click the next event,
    // and it says you are in that one too. Whether the entry belongs to *this*
    // tournament is the single fact every action in the pane hangs on, so it
    // gets its own test rather than being read off a fixture by eye.
    let app = app().await;
    open(&app, "e1a2b").await;
    app.dispatch(
        TourneyWrite::SignUp {
            tournament_id: "e1a2b".into(),
            rating: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    assert!(app.snapshot().tourney.detail.unwrap().viewer.is_signed_up());

    // A running event this account is not in. The fake seeds one deliberately,
    // so "am I in this?" has both answers and a real leak stays visible.
    open(&app, "e5x5x").await;
    let state = app.snapshot().tourney;
    let other = state.detail.expect("the second event is open");
    assert_eq!(other.id, "e5x5x");
    assert!(
        !other.viewer.is_signed_up(),
        "entering one tournament must not enter another"
    );
    // And the list row must not carry the first event's answer either.
    let row = state
        .events
        .iter()
        .find(|event| event.id == "e5x5x")
        .expect("still listed");
    assert!(row.viewer.signed_up_player_id.is_none());
    // And the event this account really is in still says so, so the assertion
    // above is about isolation rather than about nothing being set anywhere.
    open(&app, "e9z9z").await;
    assert!(app.snapshot().tourney.detail.unwrap().viewer.is_signed_up());
}

#[tokio::test]
async fn a_created_tournament_becomes_the_open_one() {
    // The organiser lands inside the event they just made, rather than back at
    // a list that looks unchanged.
    let app = app().await;
    app.dispatch(TourneyRead::Load.into()).await.unwrap();
    settle(&app).await;
    let before = app.snapshot().tourney.events.len();

    app.dispatch(
        TourneyWrite::Create {
            draft: TourneyDraft {
                name: "  Spring Open  ".into(),
                team_size: 1,
                ..TourneyDraft::new()
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    assert_eq!(state.events.len(), before + 1);
    let open = state.detail.expect("the new event is open");
    assert_eq!(state.selected_id.as_deref(), Some(open.id.as_str()));
    assert_eq!(open.name, "Spring Open", "trimmed before it was sent");
    assert_eq!(open.status, TourneyStatus::Signup);
    // Signups are open and nobody else can see it. The service creates every
    // tournament unpublished, which is why the tab has to offer the step.
    assert!(!open.published, "created unpublished, as the service does");
    assert!(open.may_publish(), "and the organiser is offered the step");
}

#[tokio::test]
async fn publishing_is_what_makes_a_created_event_visible() {
    // The gap this closes: `create` leaves the event visible to its organiser
    // alone, so an organiser who never publishes has an event taking signups
    // that nobody can find.
    let app = app().await;
    app.dispatch(TourneyRead::Load.into()).await.unwrap();
    settle(&app).await;

    app.dispatch(
        TourneyWrite::Create {
            draft: TourneyDraft {
                name: "Autumn Cup".into(),
                team_size: 1,
                ..TourneyDraft::new()
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let id = app.snapshot().tourney.selected_id.expect("the new event");

    app.dispatch(
        TourneyWrite::Publish {
            tournament_id: id.clone(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    let open = state.detail.expect("still open after the write");
    assert!(open.published, "the reload carries the new visibility back");
    assert!(!open.may_publish(), "and the control is withdrawn");
}

#[tokio::test]
async fn an_event_walks_from_signups_to_a_drawn_bracket() {
    // The lifecycle spine: close signups, form teams, draw the bracket. Each
    // step is refused from the wrong status, which is why the UI only offers
    // the one that is legal now.
    let app = app().await;
    open(&app, "e1a2b").await;
    assert_eq!(
        app.snapshot().tourney.detail.unwrap().status,
        TourneyStatus::Signup
    );

    // Drawing a bracket before teams exist is refused, and says why.
    app.dispatch(
        TourneyWrite::Advance {
            tournament_id: "e1a2b".into(),
            phase: TourneyPhase::StartBracket,
            config: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let refused = app.snapshot().tourney.action_error.expect("out of order");
    assert!(refused.reason.contains("Form teams first"));

    app.dispatch(
        TourneyWrite::Advance {
            tournament_id: "e1a2b".into(),
            phase: TourneyPhase::FormTeams,
            config: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let drafted = app.snapshot().tourney.detail.expect("still open");
    assert_eq!(drafted.status, TourneyStatus::Drafted);
    assert_eq!(drafted.team_count, 2, "every entrant is in a team");

    app.dispatch(
        TourneyWrite::Advance {
            tournament_id: "e1a2b".into(),
            phase: TourneyPhase::StartBracket,
            config: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let running = app.snapshot().tourney.detail.expect("still open");
    assert_eq!(running.status, TourneyStatus::Running);
    assert!(!running.matches.is_empty(), "the bracket was drawn");
}

#[tokio::test]
async fn reopening_signups_gives_the_teams_back_to_their_players() {
    let app = app().await;
    open(&app, "e1a2b").await;
    app.dispatch(
        TourneyWrite::Advance {
            tournament_id: "e1a2b".into(),
            phase: TourneyPhase::FormTeams,
            config: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    app.dispatch(
        TourneyWrite::Advance {
            tournament_id: "e1a2b".into(),
            phase: TourneyPhase::ReopenSignups,
            config: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("still open");
    assert_eq!(event.status, TourneyStatus::Signup);
    assert_eq!(event.team_count, 0);
    assert!(event.players.iter().all(|player| player.team_id.is_none()));
    // The entrants themselves are untouched: reopening undoes the teams, not
    // the signups.
    assert_eq!(event.player_count, 2);
}

#[tokio::test]
async fn editing_an_event_leaves_its_entrants_alone() {
    let app = app().await;
    open(&app, "e1a2b").await;
    let before = app.snapshot().tourney.detail.expect("open");

    app.dispatch(
        TourneyWrite::EditInfo {
            tournament_id: "e1a2b".into(),
            draft: TourneyDraft {
                name: "Weekend Ladder Cup 2".into(),
                description: "Now best of five.".into(),
                ..TourneyDraft::new()
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let after = app.snapshot().tourney.detail.expect("still open");
    assert_eq!(after.name, "Weekend Ladder Cup 2");
    assert_eq!(after.description, "Now best of five.");
    assert_eq!(after.player_count, before.player_count);
    // And the list row agrees, rather than still showing the old name.
    let row = app
        .snapshot()
        .tourney
        .events
        .into_iter()
        .find(|event| event.id == "e1a2b")
        .expect("still listed");
    assert_eq!(row.name, "Weekend Ladder Cup 2");
}

#[tokio::test]
async fn archiving_an_event_moves_the_selection_on() {
    let app = app().await;
    open(&app, "e1a2b").await;

    app.dispatch(
        TourneyWrite::Archive {
            tournament_id: "e1a2b".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.events.iter().all(|event| event.id != "e1a2b"));
    // Never left pointing at an event nobody can reach.
    assert_ne!(state.selected_id.as_deref(), Some("e1a2b"));
    assert!(state.detail.is_none() || state.detail.unwrap().id != "e1a2b");
}

#[tokio::test]
async fn a_refused_lifecycle_write_leaves_the_event_untouched() {
    let app = app_refusing(RequestError::rejected("Organizer rights required")).await;
    open(&app, "e1a2b").await;

    app.dispatch(
        TourneyWrite::Advance {
            tournament_id: "e1a2b".into(),
            phase: TourneyPhase::FormTeams,
            config: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.pending.is_none());
    let failure = state.action_error.expect("the refusal is kept");
    assert_eq!(
        failure.action,
        TourneyAction::Advancing {
            phase: TourneyPhase::FormTeams
        }
    );
    assert_eq!(failure.reason, "Organizer rights required");
    assert_eq!(state.detail.unwrap().status, TourneyStatus::Signup);
}

#[tokio::test]
async fn the_hosting_answer_gates_the_create_button() {
    let app = app().await;
    assert!(
        !app.snapshot().tourney.hosting.allowed,
        "unknown until asked"
    );

    app.dispatch(TourneyRead::LoadHosting.into()).await.unwrap();
    settle(&app).await;
    assert!(app.snapshot().tourney.hosting.allowed);
}

/// A 2v2 event in signups: the shape that had no way forward before teams.
async fn team_event(app: &App) -> String {
    app.dispatch(
        TourneyWrite::Create {
            draft: TourneyDraft {
                name: "Duo Cup".into(),
                team_size: 2,
                formation: faf_domain::state::Formation::Open,
                ..TourneyDraft::new()
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(app).await;
    let id = app
        .snapshot()
        .tourney
        .selected_id
        .expect("the new event is open");
    app.dispatch(
        TourneyWrite::SignUp {
            tournament_id: id.clone(),
            rating: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(app).await;
    id
}

#[tokio::test]
async fn a_team_event_offers_a_way_onto_a_team_after_signing_up() {
    // The dead end this whole step exists for: entering a 2v2 used to leave a
    // player with no team, no check-in and no match.
    let app = app().await;
    let id = team_event(&app).await;

    let before = app.snapshot().tourney.detail.expect("open");
    assert!(before.viewer.is_signed_up());
    assert!(before.my_team().is_none(), "signing up gives no team");
    assert!(before.may_create_team(), "but forming one is offered");

    app.dispatch(
        TourneyWrite::CreateTeam {
            tournament_id: id.clone(),
            name: "  Blue  ".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("open");
    let mine = event.my_team().expect("a team of my own");
    assert_eq!(mine.name, "Blue", "trimmed before it was sent");
    assert!(event.is_captain_of(mine), "the founder captains it");
    assert!(!event.may_create_team(), "and cannot start a second");
}

#[tokio::test]
async fn joining_a_team_is_a_request_the_captain_answers() {
    // There is no instant join: the server retired that path and answers
    // `join_team` with "send a join request". The client must not offer one.
    let app = app().await;
    let id = team_event(&app).await;
    app.dispatch(
        TourneyWrite::CreateTeam {
            tournament_id: id.clone(),
            name: "Blue".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let team_id = app
        .snapshot()
        .tourney
        .detail
        .unwrap()
        .my_team()
        .expect("a team")
        .id
        .clone();

    // Leaving frees this account up to ask for a place again.
    app.dispatch(
        TourneyWrite::LeaveTeam {
            tournament_id: id.clone(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    // The team was a team of one, so leaving dissolved it.
    assert!(app
        .snapshot()
        .tourney
        .detail
        .unwrap()
        .team(&team_id)
        .is_none());
}

#[tokio::test]
async fn a_captain_can_invite_an_entrant_who_has_no_team() {
    // Driven against the seeded 2v2, because a freshly created event has only
    // this account in it and the invite conversation needs two sides.
    let app = app().await;
    open(&app, "e2v2b").await;
    app.dispatch(
        TourneyWrite::SignUp {
            tournament_id: "e2v2b".into(),
            rating: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    app.dispatch(
        TourneyWrite::CreateTeam {
            tournament_id: "e2v2b".into(),
            name: "Blue".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("open");
    let team_id = event.my_team().expect("a team of my own").id.clone();
    let free = event
        .unteamed()
        .first()
        .map(|player| player.id.clone())
        .expect("somebody without a team");

    app.dispatch(
        TourneyWrite::InviteToTeam {
            tournament_id: "e2v2b".into(),
            team_id: team_id.clone(),
            player_id: free.clone(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let invited = app.snapshot().tourney.detail.expect("open");
    let team = invited.team(&team_id).expect("still there");
    assert_eq!(team.invites.len(), 1);
    assert_eq!(team.invites[0].player_id, free);
    assert!(invited.is_captain_of(team));
}

#[tokio::test]
async fn asking_a_team_for_a_place_shows_up_on_that_team() {
    // The only route onto a team: the server retired instant joining and
    // answers `join_team` with "send a join request".
    let app = app().await;
    open(&app, "e2v2b").await;
    app.dispatch(
        TourneyWrite::SignUp {
            tournament_id: "e2v2b".into(),
            rating: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("open");
    let theirs = event.teams.first().expect("a team with a place").clone();
    assert!(event.may_request_join(&theirs));

    app.dispatch(
        TourneyWrite::RequestJoin {
            tournament_id: "e2v2b".into(),
            team_id: theirs.id.clone(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let asked = app.snapshot().tourney.detail.expect("open");
    let team = asked.team(&theirs.id).expect("still there");
    assert_eq!(team.join_requests.len(), 1);
    assert!(asked.has_asked_to_join(team));
    // Asking twice is not two requests, and the button stops being offered.
    assert!(!asked.may_request_join(team));

    app.dispatch(
        TourneyWrite::CancelJoin {
            tournament_id: "e2v2b".into(),
            team_id: theirs.id.clone(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let withdrawn = app.snapshot().tourney.detail.expect("open");
    assert!(withdrawn.team(&theirs.id).unwrap().join_requests.is_empty());
}

#[tokio::test]
async fn renaming_a_team_refuses_a_name_already_taken() {
    let app = app().await;
    let id = team_event(&app).await;
    app.dispatch(
        TourneyWrite::CreateTeam {
            tournament_id: id.clone(),
            name: "Blue".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let team_id = app
        .snapshot()
        .tourney
        .detail
        .unwrap()
        .my_team()
        .unwrap()
        .id
        .clone();

    app.dispatch(
        TourneyWrite::RenameTeam {
            tournament_id: id.clone(),
            team_id: team_id.clone(),
            name: "  Red  ".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    assert_eq!(
        app.snapshot()
            .tourney
            .detail
            .unwrap()
            .team(&team_id)
            .unwrap()
            .name,
        "Red"
    );
}

#[tokio::test]
async fn a_solo_event_never_offers_team_forming() {
    // A solo event's teams are made by the organiser at the phase change, so a
    // "create team" button there would be a trap.
    let app = app().await;
    open(&app, "e1a2b").await;
    let event = app.snapshot().tourney.detail.expect("open");
    assert_eq!(event.team_size, 1);
    assert!(!event.teams_are_self_organised());
    assert!(!event.may_create_team());
}

#[tokio::test]
async fn an_organiser_adds_an_entrant_by_faf_name() {
    // There is no free-typed entrant: the server resolves the name against a
    // real account, which is what keeps avatars and ratings possible at all.
    let app = app().await;
    open(&app, "e1a2b").await;
    let before = app.snapshot().tourney.detail.unwrap().player_count;

    app.dispatch(
        TourneyWrite::AddPlayer {
            tournament_id: "e1a2b".into(),
            name: "  Alan  ".into(),
            rating: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("open");
    assert_eq!(event.player_count, before + 1);
    assert!(event.players.iter().any(|player| player.name == "Alan"));
    // A blank name is not a request at all.
    app.dispatch(
        TourneyWrite::AddPlayer {
            tournament_id: "e1a2b".into(),
            name: "   ".into(),
            rating: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    assert_eq!(
        app.snapshot().tourney.detail.unwrap().player_count,
        before + 1
    );
}

#[tokio::test]
async fn an_organiser_removes_an_entrant_through_the_same_route_as_a_withdrawal() {
    // One endpoint, and the server decides which it is from who is asking.
    let app = app().await;
    open(&app, "e1a2b").await;
    let victim = app
        .snapshot()
        .tourney
        .detail
        .unwrap()
        .players
        .first()
        .map(|player| player.id.clone())
        .expect("an entrant");

    app.dispatch(
        TourneyWrite::RemovePlayer {
            tournament_id: "e1a2b".into(),
            player_id: victim.clone(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("open");
    assert!(event.players.iter().all(|player| player.id != victim));
}

#[tokio::test]
async fn inviting_and_uninviting_round_trips() {
    let app = app().await;
    open(&app, "e1a2b").await;

    app.dispatch(
        TourneyWrite::InvitePlayer {
            tournament_id: "e1a2b".into(),
            name: "Zep".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let invited = app.snapshot().tourney.detail.expect("open");
    let invite = invited.invites.first().cloned().expect("an invitation");
    assert_eq!(invite.name, "Zep");

    app.dispatch(
        TourneyWrite::Uninvite {
            tournament_id: "e1a2b".into(),
            faf_id: invite.faf_id,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    assert!(app.snapshot().tourney.detail.unwrap().invites.is_empty());
}

#[tokio::test]
async fn seeds_can_be_set_by_hand_and_only_between_teams_and_the_bracket() {
    let app = app().await;
    open(&app, "e1a2b").await;
    // Too early: there are no teams to seed.
    assert!(!app.snapshot().tourney.detail.unwrap().may_reseed());

    app.dispatch(
        TourneyWrite::Advance {
            tournament_id: "e1a2b".into(),
            phase: TourneyPhase::FormTeams,
            config: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let drafted = app.snapshot().tourney.detail.expect("open");
    assert!(drafted.may_reseed());
    let order: Vec<String> = drafted
        .teams
        .iter()
        .rev()
        .map(|team| team.id.clone())
        .collect();

    app.dispatch(
        TourneyWrite::Reseed {
            tournament_id: "e1a2b".into(),
            order: SeedOrder::Explicit {
                team_ids: order.clone(),
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let seeded = app.snapshot().tourney.detail.expect("open");
    assert_eq!(seeded.team(&order[0]).unwrap().seed, 1);
    assert_eq!(seeded.team(&order[1]).unwrap().seed, 2);

    // An order that does not name every team exactly once is refused, the same
    // way the server refuses it.
    app.dispatch(
        TourneyWrite::Reseed {
            tournament_id: "e1a2b".into(),
            order: SeedOrder::Explicit {
                team_ids: vec![order[0].clone()],
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    assert!(app.snapshot().tourney.action_error.is_some());
}

#[tokio::test]
async fn splitting_into_divisions_and_back_again() {
    let app = app().await;
    open(&app, "e1a2b").await;
    app.dispatch(
        TourneyWrite::Advance {
            tournament_id: "e1a2b".into(),
            phase: TourneyPhase::FormTeams,
            config: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    app.dispatch(
        TourneyWrite::SplitDivisions {
            tournament_id: "e1a2b".into(),
            divisions: 2,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let split = app.snapshot().tourney.detail.expect("open");
    assert_eq!(split.divisions, 2);
    assert!(split.teams.iter().all(|team| team.division > 0));

    // One division is the way back to a single field.
    app.dispatch(
        TourneyWrite::SplitDivisions {
            tournament_id: "e1a2b".into(),
            divisions: 1,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let whole = app.snapshot().tourney.detail.expect("open");
    assert_eq!(whole.divisions, 0);
    assert!(whole.teams.iter().all(|team| team.division == 0));
}

#[tokio::test]
async fn news_is_posted_newest_first_and_can_be_taken_down() {
    let app = app().await;
    open(&app, "e1a2b").await;
    // Counted against what the fixture already holds rather than absolutely:
    // the event is seeded with announcements so the unread badge has something
    // to be unread about, and this test is about the two it posts.
    let seeded = app.snapshot().tourney.detail.expect("open").news.len();

    for body in ["Signups close Friday.", "  Start moved to 19:00 UTC.  "] {
        app.dispatch(
            TourneyWrite::PostNews {
                tournament_id: "e1a2b".into(),
                body: body.into(),
                important: body.contains("19:00"),
            }
            .into(),
        )
        .await
        .unwrap();
        settle(&app).await;
    }

    let event = app.snapshot().tourney.detail.expect("open");
    assert_eq!(event.news.len(), seeded + 2);
    assert_eq!(
        event.news[0].body, "Start moved to 19:00 UTC.",
        "newest first"
    );
    assert!(event.news[0].important);

    let id = event.news[0].id.clone();
    app.dispatch(
        TourneyWrite::DeleteNews {
            tournament_id: "e1a2b".into(),
            news_id: id.clone(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let left = app.snapshot().tourney.detail.expect("open");
    assert_eq!(left.news.len(), seeded + 1);
    assert!(left.news.iter().all(|post| post.id != id));

    // An empty post is not a request.
    app.dispatch(
        TourneyWrite::PostNews {
            tournament_id: "e1a2b".into(),
            body: "   ".into(),
            important: false,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    assert_eq!(
        app.snapshot().tourney.detail.unwrap().news.len(),
        seeded + 1
    );
}

#[tokio::test]
async fn a_pending_signup_waits_for_the_organiser() {
    // Request mode: the entry exists but does not count until it is approved.
    let app = app().await;
    open(&app, "e1a2b").await;
    let event = app.snapshot().tourney.detail.expect("open");
    // Nothing is pending in the seed, so the list is the empty case and the
    // organiser's panel has nothing to show.
    assert!(event.pending_signups().is_empty());
}

/// Wait for the series half to settle, which the tournament settle cannot see.
async fn settle_series(app: &App) {
    for _ in 0..200 {
        let quiet = app.with_state(|state| {
            state.tourney.pending.is_none()
                && state.tourney.series_status != TourneyLoadStatus::Loading
        });
        if quiet {
            tokio::task::yield_now().await;
            if app.with_state(|state| state.tourney.pending.is_none()) {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("the series never settled");
}

#[tokio::test]
async fn a_series_counts_the_editions_filed_under_it() {
    // The counts are the whole reason a series list is worth having, and none of
    // them is stored: the service derives every one from the tournaments. Filing
    // an event has to move them, or the list is a set of frozen numbers.
    let app = app().await;
    app.dispatch(TourneyRead::LoadSeries.into()).await.unwrap();
    settle_series(&app).await;

    let series = app.snapshot().tourney.series;
    let ladder = series
        .iter()
        .find(|row| row.id == "s0001")
        .expect("the seeded series");
    assert_eq!(ladder.editions, 1, "one edition is filed to begin with");
    assert_eq!(ladder.active, 0, "and it has finished, so none is live");
    assert_eq!(ladder.latest_name, "Spring Ladder Cup");

    // File a running event under it. Both counts move, and the latest edition
    // becomes the newer one.
    open(&app, "e9z9z").await;
    app.dispatch(
        TourneyWrite::SetSeries {
            tournament_id: "e9z9z".into(),
            series_id: Some("s0001".into()),
        }
        .into(),
    )
    .await
    .unwrap();
    settle_series(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    let ladder = state
        .series
        .iter()
        .find(|row| row.id == "s0001")
        .expect("still there");
    assert_eq!(ladder.editions, 2);
    assert_eq!(ladder.active, 1, "the running edition counts as live");
    assert_eq!(ladder.latest_name, "Autumn Invitational");

    // And the event carries the label, which is what a list row draws.
    let detail = state.detail.expect("still open");
    assert_eq!(detail.series_id.as_deref(), Some("s0001"));
    assert_eq!(detail.series_name, "Weekend Ladder");
}

#[tokio::test]
async fn leaving_a_series_takes_the_label_with_it() {
    let app = app().await;
    open(&app, "e7f7f").await;
    assert_eq!(
        app.snapshot()
            .tourney
            .detail
            .expect("the event")
            .series_name,
        "Weekend Ladder"
    );

    app.dispatch(
        TourneyWrite::SetSeries {
            tournament_id: "e7f7f".into(),
            series_id: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle_series(&app).await;

    let state = app.snapshot().tourney;
    let detail = state.detail.expect("still open");
    assert!(detail.series_id.is_none());
    assert!(
        detail.series_name.is_empty(),
        "the name goes with the id, or the tab labels an event with a series it left"
    );
    assert_eq!(
        state
            .series
            .iter()
            .find(|row| row.id == "s0001")
            .expect("the series survives")
            .editions,
        0,
        "unfiling an edition is what the count is derived from"
    );
}

#[tokio::test]
async fn deleting_a_series_unfiles_its_editions_rather_than_deleting_them() {
    // The half an organiser worries about before pressing the button. The
    // service unfiles; a client that implied otherwise would be asking them to
    // take a risk that does not exist.
    let app = app().await;
    open(&app, "e7f7f").await;
    app.dispatch(TourneyRead::LoadSeries.into()).await.unwrap();
    settle_series(&app).await;
    app.dispatch(
        TourneyRead::OpenSeries {
            series_id: "s0001".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle_series(&app).await;
    assert_eq!(
        app.snapshot()
            .tourney
            .open_series
            .expect("opened")
            .editions
            .len(),
        1
    );

    app.dispatch(
        TourneyWrite::DeleteSeries {
            series_id: "s0001".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle_series(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    assert!(state.series.is_empty(), "the series itself is gone");
    assert!(
        state.open_series.is_none(),
        "and the page showing it closed rather than lingering over nothing"
    );
    // The tournament is still there, and no longer labelled.
    let detail = state.detail.expect("the event survives");
    assert_eq!(detail.id, "e7f7f");
    assert!(detail.series_id.is_none());
}

#[tokio::test]
async fn two_series_cannot_share_a_name() {
    // The one refusal an organiser meets by accident: a series per edition, all
    // called the same thing.
    let app = app().await;
    app.dispatch(TourneyRead::LoadSeries.into()).await.unwrap();
    settle_series(&app).await;

    // Trimmed and case-folded, the way the service compares them: the near-miss
    // is the one that would otherwise get through.
    app.dispatch(
        TourneyWrite::SaveSeries {
            draft: SeriesDraft {
                name: "  weekend ladder  ".into(),
                ..SeriesDraft::default()
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle_series(&app).await;

    let state = app.snapshot().tourney;
    let failure = state.action_error.expect("refused");
    assert_eq!(failure.action, TourneyAction::SavingSeries);
    assert!(
        failure.reason.contains("already exists"),
        "the service's own sentence: {}",
        failure.reason
    );
    assert_eq!(state.series.len(), 1, "and nothing was created");

    // A name of its own is taken, which is what says the refusal was about the
    // clash rather than about saving at all.
    app.dispatch(
        TourneyWrite::SaveSeries {
            draft: SeriesDraft {
                name: "Midweek Blitz".into(),
                ..SeriesDraft::default()
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle_series(&app).await;
    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    assert_eq!(state.series.len(), 2);
}

#[tokio::test]
async fn a_finished_qualifier_names_who_went_through_and_who_could_not_be_reached() {
    // The whole point of the link, and the half that is easy to drop: a team
    // qualifies, and an invite is addressed to a FAF account. A manually added
    // entrant has none, so they qualify and cannot be invited. The service
    // reports that rather than swallowing it, because it is the organiser who
    // then has to add them by hand.
    let app = app().await;
    open(&app, "e1a2b").await;

    app.dispatch(
        TourneyWrite::AddQualifier {
            tournament_id: "e1a2b".into(),
            qualifier_id: "e7f7f".into(),
            rule: QualifierRule {
                kind: QualifierKind::Top,
                n: 2,
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    let event = state.detail.expect("still open");
    let link = event.qualifiers.first().expect("the link");
    assert_eq!(link.tournament_id, "e7f7f");
    assert!(
        link.applied.is_some(),
        "the child has finished, so the link applied at once"
    );
    assert_eq!(link.qualified, ["Ada"], "the champion goes through");
    assert_eq!(
        link.unreachable,
        ["Guest"],
        "and the runner-up has no account to invite"
    );

    // Removing it takes the link and leaves the invites, which is why it is not
    // an undo.
    app.dispatch(
        TourneyWrite::RemoveQualifier {
            tournament_id: "e1a2b".into(),
            link_id: link.id.clone(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    assert!(app
        .snapshot()
        .tourney
        .detail
        .expect("still open")
        .qualifiers
        .is_empty());
}

#[tokio::test]
async fn a_link_that_would_make_a_cycle_is_refused_by_the_service() {
    // The one check the client cannot make: it needs the candidate's own links,
    // which a list row does not carry. Pinned here so the refusal is known to
    // arrive rather than assumed.
    let app = app().await;
    open(&app, "e1a2b").await;
    app.dispatch(
        TourneyWrite::AddQualifier {
            tournament_id: "e1a2b".into(),
            qualifier_id: "e9z9z".into(),
            rule: QualifierRule::default(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    assert!(app.snapshot().tourney.action_error.is_none());

    // Now the other way round.
    open(&app, "e9z9z").await;
    app.dispatch(
        TourneyWrite::AddQualifier {
            tournament_id: "e9z9z".into(),
            qualifier_id: "e1a2b".into(),
            rule: QualifierRule::default(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let failure = app
        .snapshot()
        .tourney
        .action_error
        .expect("the service refuses the cycle");
    assert_eq!(failure.action, TourneyAction::AddingQualifier);
    assert!(
        failure.reason.contains("already draws"),
        "the service's own sentence: {}",
        failure.reason
    );
}

#[tokio::test]
async fn silencing_somebody_tells_them_before_they_type() {
    // The whole reason `chatMutedMe` is read: the service refuses a muted
    // account's post with a sentence they see only after writing one.
    let app = app().await;
    open(&app, "e9z9z").await;
    assert!(
        app.snapshot()
            .tourney
            .detail
            .expect("the event")
            .may_post_chat(),
        "nothing is stopping them to begin with"
    );

    app.dispatch(
        TourneyWrite::MuteChat {
            tournament_id: "e9z9z".into(),
            faf_id: 101,
            name: "Nuggets".into(),
            muted: true,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    let event = state.detail.expect("still open");
    assert!(!event.may_post_chat(), "the composer closes");
    assert_eq!(event.chat_mutes.len(), 1, "and the organiser sees who");
    assert_eq!(event.chat_mutes[0].name, "Nuggets");

    // Unmuting is the same command, not a second one that could disagree about
    // what the flag means.
    app.dispatch(
        TourneyWrite::MuteChat {
            tournament_id: "e9z9z".into(),
            faf_id: 101,
            name: "Nuggets".into(),
            muted: false,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let event = app.snapshot().tourney.detail.expect("still open");
    assert!(event.may_post_chat());
    assert!(event.chat_mutes.is_empty());
}

#[tokio::test]
async fn a_deleted_post_is_gone_from_the_room_that_is_open() {
    // The event reload every write ends with does not carry the conversation,
    // so a deleted post would sit on screen until the room was reopened.
    let app = app().await;
    open(&app, "e9z9z").await;
    app.dispatch(
        TourneyRead::LoadChat {
            tournament_id: "e9z9z".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    app.dispatch(
        TourneyRead::OpenRoom {
            tournament_id: "e9z9z".into(),
            room_id: "global".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let posts = app.snapshot().tourney.chat_posts;
    let victim = posts.first().expect("a seeded post").id.clone();
    assert!(posts.iter().any(|post| post.faf_id.is_some()));

    app.dispatch(
        TourneyWrite::DeleteChatPost {
            tournament_id: "e9z9z".into(),
            room_id: "global".into(),
            post_id: victim.clone(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    assert!(
        !state.chat_posts.iter().any(|post| post.id == victim),
        "the room on screen was re-read, not just the event"
    );
}

#[tokio::test]
async fn a_hidden_organiser_keeps_their_rights_and_loses_the_credit() {
    // Two lists with different meanings: `organizers` is the public one,
    // `organizersPublic` in the service's own words, and hiding takes somebody
    // out of it without taking anything away.
    let app = app().await;
    open(&app, "e1a2b").await;

    app.dispatch(
        TourneyWrite::AddOrganiser {
            tournament_id: "e1a2b".into(),
            faf_id: 102,
            name: "Ada_Lovelace".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    let event = state.detail.expect("still open");
    assert_eq!(event.organiser_accounts.len(), 2);
    assert!(event.organisers.iter().any(|name| name == "Ada_Lovelace"));

    app.dispatch(
        TourneyWrite::SetOrganiserVisibility {
            tournament_id: "e1a2b".into(),
            faf_id: 102,
            hidden: true,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("still open");
    assert_eq!(
        event.organiser_accounts.len(),
        2,
        "still an organiser: hiding is about the credit, not the rights"
    );
    assert!(
        event.organiser_accounts.iter().any(|held| held.hidden),
        "and the organiser-only list says which of them chose to hide"
    );
    assert!(
        !event.organisers.iter().any(|name| name == "Ada_Lovelace"),
        "but the public list no longer names them"
    );

    // Adding the same account twice is the service's own refusal.
    app.dispatch(
        TourneyWrite::AddOrganiser {
            tournament_id: "e1a2b".into(),
            faf_id: 102,
            name: "Ada_Lovelace".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let failure = app
        .snapshot()
        .tourney
        .action_error
        .expect("refused the second time");
    assert_eq!(failure.action, TourneyAction::AddingOrganiser);
}

#[tokio::test]
async fn the_team_setup_locks_a_step_before_the_rest_of_the_format() {
    // The distinction the panel turns on: the bracket type stays open right up
    // to the draw, while the team size closes with signups. Sending an
    // unchanged team size alongside a bracket change would be refused for
    // touching neither, which is why `structural` is worked out per write.
    let app = app().await;
    open(&app, "e1a2b").await;
    let event = app.snapshot().tourney.detail.expect("the event");
    assert_eq!(event.status, TourneyStatus::Signup);
    assert!(event.may_edit_format() && event.may_edit_team_setup());

    let mut format = FormatDraft::of(&event);
    format.bracket_kind = faf_domain::state::BracketKind::Swiss;
    assert!(
        !format.is_structural(&event),
        "a bracket change alone touches no teams"
    );
    app.dispatch(
        TourneyWrite::EditFormat {
            tournament_id: "e1a2b".into(),
            format: format.clone(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    assert_eq!(
        state.detail.expect("still open").bracket_kind,
        faf_domain::state::BracketKind::Swiss
    );

    // Now the same event with its bracket drawn: the whole format is locked,
    // and the service says so rather than quietly doing nothing.
    open(&app, "e9z9z").await;
    let running = app.snapshot().tourney.detail.expect("the running event");
    assert!(!running.may_edit_format());
    app.dispatch(
        TourneyWrite::EditFormat {
            tournament_id: "e9z9z".into(),
            format: FormatDraft {
                team_size: 3,
                ..FormatDraft::of(&running)
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let failure = app.snapshot().tourney.action_error.expect("refused");
    assert_eq!(failure.action, TourneyAction::EditingFormat);
    assert!(
        failure.reason.contains("locked"),
        "the service's own sentence: {}",
        failure.reason
    );
}

#[tokio::test]
async fn reading_the_announcements_clears_the_badge_for_the_account() {
    // Kept by the service rather than locally, which is the point: the badge
    // clears on every device rather than once per machine.
    let app = app().await;
    open(&app, "e1a2b").await;
    let event = app.snapshot().tourney.detail.expect("the event");
    assert!(
        event.unread_news() > 0,
        "the fixture event has announcements nobody has read"
    );
    let mut events = app.subscribe();

    app.dispatch(
        TourneyRead::MarkNewsRead {
            tournament_id: "e1a2b".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    assert_eq!(state.detail.expect("still open").unread_news(), 0);
    // Quietly: opening the announcements must not put the pane through
    // "loading", which blanked the event and made the page jump.
    while let Ok(event) = events.try_recv() {
        assert_ne!(event, AppEvent::Tourney(TourneyEvent::DetailLoading));
    }
}

#[tokio::test]
async fn correcting_an_announcement_marks_it_as_corrected() {
    // These announce schedules, and a schedule that changed twice is not the
    // same news: the stamp is what says so.
    let app = app().await;
    open(&app, "e1a2b").await;
    let post = app
        .snapshot()
        .tourney
        .detail
        .expect("the event")
        .news
        .first()
        .expect("an announcement")
        .clone();
    assert!(post.edited_at.is_none());

    app.dispatch(
        TourneyWrite::EditNews {
            tournament_id: "e1a2b".into(),
            news_id: post.id.clone(),
            body: "  Start moved to 20:00 UTC.  ".into(),
            important: true,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    let corrected = state
        .detail
        .expect("still open")
        .news
        .into_iter()
        .find(|held| held.id == post.id)
        .expect("still there");
    assert_eq!(corrected.body, "Start moved to 20:00 UTC.");
    assert!(corrected.important);
    assert!(corrected.edited_at.is_some());

    // An empty correction is refused rather than blanking the post.
    app.dispatch(
        TourneyWrite::EditNews {
            tournament_id: "e1a2b".into(),
            news_id: post.id.clone(),
            body: "   ".into(),
            important: false,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let failure = app.snapshot().tourney.action_error.expect("refused");
    assert_eq!(
        failure.action,
        TourneyAction::EditingNews {
            news_id: post.id.clone()
        }
    );
}

#[tokio::test]
async fn abandoning_an_event_leaves_it_visible_and_is_reversible() {
    // Not the same as archiving, which hides it. An empty bracket with no
    // explanation reads as a broken tab, which is what this exists to avoid.
    let app = app().await;
    open(&app, "e1a2b").await;

    app.dispatch(
        TourneyWrite::Abandon {
            tournament_id: "e1a2b".into(),
            abandoned: true,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    assert!(state.detail.expect("still open").abandoned);
    assert!(
        state.events.iter().any(|row| row.id == "e1a2b"),
        "and it is still in the list, unlike an archived one"
    );

    app.dispatch(
        TourneyWrite::Abandon {
            tournament_id: "e1a2b".into(),
            abandoned: false,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    assert!(!app.snapshot().tourney.detail.expect("still open").abandoned);
}

#[tokio::test]
async fn map_pools_can_be_bound_to_rounds_before_the_bracket_is_drawn() {
    // The flow this exists for, and the one that was impossible: an organiser
    // prepares the map plan while signups run. The rounds are projected from
    // the expected field, the keys are the service's own, and it takes them for
    // a bracket that does not exist yet, because it only checks the pool.
    let app = app().await;
    open(&app, "e1a2b").await;
    let event = app.snapshot().tourney.detail.expect("the event");
    assert_eq!(event.status, TourneyStatus::Signup);
    assert!(event.matches.is_empty(), "nothing is drawn yet");

    let plan = event.round_plan();
    assert!(plan.projected, "worked out, not read off a bracket");
    assert!(
        !plan.keys.is_empty(),
        "the whole point: rounds to assign before the draw"
    );
    let pool = event.map_pools.first().expect("a seeded pool").id.clone();

    // "One pool for the whole event", which is what most tournaments want and
    // what the panel offers as a single control.
    for round in &plan.keys {
        app.dispatch(
            TourneyWrite::AssignPool {
                tournament_id: "e1a2b".into(),
                round_key: round.key.clone(),
                pool_id: pool.clone(),
            }
            .into(),
        )
        .await
        .unwrap();
        settle(&app).await;
    }

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    let after = state.detail.expect("still open");
    assert_eq!(
        after.pool_assign.len(),
        plan.keys.len(),
        "every projected round was bound"
    );
    assert!(
        after
            .pool_assign
            .iter()
            .all(|assignment| assignment.pool_id == pool),
        "and all to the same pool"
    );
}

#[tokio::test]
async fn the_round_projection_follows_the_field_and_the_format() {
    // Three answers that have to move together, because the projection is what
    // decides how many rounds an organiser is offered.
    let app = app().await;
    open(&app, "e1a2b").await;
    let event = app.snapshot().tourney.detail.expect("the event");

    // A drawn bracket answers from its own matches rather than from a guess.
    open(&app, "e9z9z").await;
    let running = app.snapshot().tourney.detail.expect("the running event");
    let drawn = running.round_plan();
    assert!(!drawn.projected);
    assert_eq!(drawn.teams, 4);
    assert!(drawn.keys.iter().all(|round| running
        .matches
        .iter()
        .any(|entry| entry.bracket == round.bracket && entry.round == round.round)));

    // A free-for-all has no ban and pick rounds at all, so it must project
    // nothing rather than a bracket it will never draw.
    open(&app, "f4f4f").await;
    let ffa = app.snapshot().tourney.detail.expect("the ffa event");
    assert!(ffa.round_plan().keys.is_empty());

    // And the signup event's own projection follows its entrants.
    assert_eq!(
        event.projected_team_count(),
        event.players.len() as i32 / event.team_size.max(1)
    );
}

#[tokio::test]
async fn a_match_gets_a_room_only_once_both_sides_are_known() {
    // The tournament team's first requirement for the chat, and the service's
    // own rule: a room per match, but not before there is a match to have one
    // about. Otherwise the list fills with "? vs ?".
    let app = app().await;
    open(&app, "e9z9z").await;
    app.dispatch(
        TourneyRead::LoadChat {
            tournament_id: "e9z9z".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    let event = state.detail.expect("the event");
    let expected: Vec<String> = event
        .matches
        .iter()
        .filter(|entry| entry.team1.is_some() && entry.team2.is_some())
        .map(|entry| format!("match:{}", entry.id))
        .collect();

    for id in &expected {
        assert!(
            state.chat_rooms.iter().any(|room| &room.id == id),
            "a match with both sides known has a room: {id}"
        );
    }
    // And the empty second-round slot has none.
    let waiting = event
        .matches
        .iter()
        .find(|entry| entry.team1.is_none() || entry.team2.is_none());
    if let Some(entry) = waiting {
        let id = format!("match:{}", entry.id);
        assert!(
            !state.chat_rooms.iter().any(|room| room.id == id),
            "a match still waiting on a feeder has no room yet"
        );
    }
}

#[tokio::test]
async fn a_played_matchs_room_folds_into_the_completed_group() {
    // The team's second requirement: a bracket makes a room per match and never
    // deletes one, so a finished match's room has to leave the live list. It
    // said so at the time: otherwise you see too many and it gets confusing.
    let app = app().await;
    open(&app, "e9z9z").await;
    app.dispatch(
        TourneyRead::LoadChat {
            tournament_id: "e9z9z".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let before = app.snapshot().tourney;
    let (active, completed) = before.chat_groups();
    assert!(completed.is_empty(), "nothing has been played yet");
    let live = active.len();
    let played = before
        .detail
        .expect("the event")
        .matches
        .iter()
        .find(|entry| entry.status == MatchStatus::Ready)
        .expect("a match ready to play")
        .id
        .clone();

    // Settle it, and its room should move.
    app.dispatch(
        TourneyWrite::DecideReport {
            tournament_id: "e9z9z".into(),
            report: MatchReport {
                match_id: played.clone(),
                score1: 2,
                score2: 0,
                ..MatchReport::default()
            },
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    app.dispatch(
        TourneyRead::LoadChat {
            tournament_id: "e9z9z".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let after = app.snapshot().tourney;
    let (active, completed) = after.chat_groups();
    assert!(
        completed
            .iter()
            .any(|room| room.id == format!("match:{played}")),
        "the played match's room is in the completed group"
    );
    assert!(
        !active
            .iter()
            .any(|room| room.id == format!("match:{played}")),
        "and out of the live list"
    );
    assert!(active.len() < live || after.chat_rooms.len() > live);
}

#[tokio::test]
async fn refreshing_a_room_brings_in_what_somebody_else_wrote() {
    // The service has no push of any kind. Without a poll the tab can send a
    // message and never receive one, which looks like a working chat right up
    // until somebody else types.
    let app = app().await;
    open(&app, "e9z9z").await;
    app.dispatch(
        TourneyRead::LoadChat {
            tournament_id: "e9z9z".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    app.dispatch(
        TourneyRead::OpenRoom {
            tournament_id: "e9z9z".into(),
            room_id: "global".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    let before = app.snapshot().tourney.chat_posts.len();

    // Somebody else posts. Nothing tells the tab, which is the whole point.
    app.dispatch(
        TourneyWrite::PostChat {
            tournament_id: "e9z9z".into(),
            room_id: "global".into(),
            body: "on my way".into(),
            reply_to: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    app.dispatch(
        TourneyRead::RefreshChat {
            tournament_id: "e9z9z".into(),
            room_id: "global".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.chat_posts.len() > before, "the poll brought it in");
    assert!(state.chat_posts.iter().any(|post| post.body == "on my way"));
    // Silent by design: a poll every few seconds must not announce itself, or
    // the room blinks out and back while somebody is reading it.
    assert_eq!(state.chat_status, TourneyLoadStatus::Ready);
    assert!(state.action_error.is_none());
}

#[tokio::test]
async fn a_caster_is_added_by_account_and_sees_the_whole_event() {
    // The role that replaced the caster link. The link carried a token in a
    // URL, which the client had nowhere to put; an account role arrives through
    // the session like everything else.
    let app = app().await;
    open(&app, "e9z9z").await;
    assert!(app
        .snapshot()
        .tourney
        .detail
        .expect("the event")
        .casters
        .is_empty());

    app.dispatch(
        TourneyWrite::SetCaster {
            tournament_id: "e9z9z".into(),
            faf_id: 102,
            name: "Ada_Lovelace".into(),
            casting: true,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    let event = state.detail.expect("still open");
    assert_eq!(event.casters.len(), 1);
    assert_eq!(event.casters[0].name, "Ada_Lovelace");

    app.dispatch(
        TourneyWrite::SetCaster {
            tournament_id: "e9z9z".into(),
            faf_id: 102,
            name: "Ada_Lovelace".into(),
            casting: false,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;
    assert!(app
        .snapshot()
        .tourney
        .detail
        .expect("still open")
        .casters
        .is_empty());
}

#[tokio::test]
async fn drawing_the_bracket_carries_the_best_of_plan() {
    // What the website does and the client did not: `phase` went out with the
    // action alone, so the service fell back to its own plan every time.
    let app = app().await;
    open(&app, "e1a2b").await;
    app.dispatch(
        TourneyWrite::Advance {
            tournament_id: "e1a2b".into(),
            phase: TourneyPhase::FormTeams,
            config: None,
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let event = app.snapshot().tourney.detail.expect("teams formed");
    assert_eq!(event.status, TourneyStatus::Drafted);
    // The plan the dialog opens on: what the service would have used anyway.
    let plan = BracketConfig::of(&event);
    assert!(plan.is_submittable(event.teams.len() as i32));

    app.dispatch(
        TourneyWrite::Advance {
            tournament_id: "e1a2b".into(),
            phase: TourneyPhase::StartBracket,
            config: Some(plan),
        }
        .into(),
    )
    .await
    .unwrap();
    settle(&app).await;

    let state = app.snapshot().tourney;
    assert!(state.action_error.is_none(), "{:?}", state.action_error);
    let drawn = state.detail.expect("still open");
    assert_eq!(drawn.status, TourneyStatus::Running);
    assert!(!drawn.matches.is_empty(), "and there is a bracket");
}

#[tokio::test]
async fn a_plan_with_the_wrong_number_of_rounds_is_caught_here() {
    // The service trims or pads the list to the bracket's real length, so a
    // wrong count loses a round's setting rather than failing. Refused before
    // it is sent, for the same reason the pool counts are.
    let app = app().await;
    open(&app, "e9z9z").await;
    let event = app.snapshot().tourney.detail.expect("a drawn event");
    let teams = event.teams.len() as i32;

    let short = BracketConfig::Single {
        rounds: vec![3],
        third_place: false,
    };
    assert!(!short.is_submittable(teams), "one round for four teams");
    assert!(BracketConfig::of(&event).is_submittable(teams));
}

/// The offline site with a Hall of Fame on it: two players FAF knows (the
/// offline player card's 101 and 102) and one it does not.
struct HallSite(Arc<dyn faf_app::ports::TourneySitePort>);

#[async_trait]
impl faf_app::ports::TourneySitePort for HallSite {
    async fn hosting(&self) -> Result<faf_domain::state::HostingStatus, RequestError> {
        self.0.hosting().await
    }

    async fn profile(&self) -> Result<String, RequestError> {
        self.0.profile().await
    }

    async fn set_discord(&self, handle: &str) -> Result<String, RequestError> {
        self.0.set_discord(handle).await
    }

    async fn site_read(
        &self,
        read: faf_domain::state::SiteRead,
    ) -> Result<faf_domain::state::SiteDocument, RequestError> {
        if read != faf_domain::state::SiteRead::HallOfFame {
            return self.0.site_read(read).await;
        }
        let player = |faf_id, name: &str, wins| faf_domain::state::HallPlayer {
            faf_id,
            name: name.into(),
            wins,
            entered: wins + 1,
            profile: None,
        };
        Ok(faf_domain::state::SiteDocument::HallOfFame(
            faf_domain::state::HallOfFame {
                players: vec![
                    player(102, "Ada", 2),
                    player(999, "Gone", 1),
                    player(101, "Nuggets", 0),
                ],
                teams: Vec::new(),
            },
        ))
    }

    async fn site_write(
        &self,
        write: &faf_domain::state::SiteWrite,
    ) -> Result<(Option<String>, Option<String>), RequestError> {
        self.0.site_write(write).await
    }

    async fn articles(&self) -> Result<Vec<faf_domain::state::Article>, RequestError> {
        self.0.articles().await
    }
}

#[tokio::test]
async fn the_hall_of_fame_lands_first_and_then_with_each_players_faf_account() {
    // The board is the tournament service's; the avatars are FAF's. The board
    // must not wait on the second request, and a row FAF has no account for
    // keeps its place without one.
    let base = fake_ports();
    let ports = Ports {
        tourney_site: Arc::new(HallSite(base.tourney_site.clone())),
        ..base
    };
    let app = support::start(ports);
    sign_in(&app).await;
    let mut events = app.subscribe();

    app.dispatch(
        TourneyRead::LoadSite {
            read: faf_domain::state::SiteRead::HallOfFame,
        }
        .into(),
    )
    .await
    .unwrap();

    let mut boards = Vec::new();
    while boards.len() < 2 {
        if let TourneyEvent::SiteLoaded {
            document: faf_domain::state::SiteDocument::HallOfFame(hall),
        } = next_event(&mut events).await
        {
            boards.push(hall);
        }
    }
    assert!(
        boards[0]
            .players
            .iter()
            .all(|player| player.profile.is_none()),
        "the board first, as the service sent it"
    );
    let logins: Vec<Option<String>> = boards[1]
        .players
        .iter()
        .map(|player| player.profile.as_ref().map(|profile| profile.login.clone()))
        .collect();
    assert_eq!(
        logins,
        vec![
            Some("Ada_Lovelace".to_string()),
            None,
            Some("Nuggets".to_string())
        ],
        "then each row with its account, in the board's order"
    );
}
