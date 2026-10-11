//! Generates the conformance fixture the frontend reducer is tested against.
//!
//! `ui/src/store/reducers/*.ts` are hand-written twins of the reducers in
//! `state/`. TypeScript's exhaustive `switch` catches a *missing* variant;
//! nothing catches the same variant taking a different transition, and nothing
//! reconciles the two at runtime (`src-tauri` re-sends a full snapshot only on
//! broadcast lag). One such divergence has already shipped: `userLeft` created
//! a chat channel in TS that the Rust reducer leaves alone.
//!
//! Rather than hand-write a TS test per slice: which would check the twin
//! against *someone's reading* of the Rust, the same reading that produced the
//! divergence: this records what `reduce` actually does and makes the
//! frontend replay it.
//!
//! Running `cargo test` rewrites the fixture. A reducer change therefore shows
//! up as a **failing frontend test** until the twin is updated, which is
//! exactly the signal that was missing.

use faf_domain::state::*;
use faf_domain::{reduce, AppEvent, AppState};
use serde::Serialize;
use serde_json::Value;

// The ordering fixture: these cases' events in other orders and from other
// states. A test crate root resolves `mod` next to itself, hence the path.
#[path = "conformance_fixtures/orderings.rs"]
mod orderings;

/// One scenario: a sequence of events applied to the default state, with the
/// state recorded after each.
///
/// Sequences rather than isolated events, because ordering is where the
/// interesting behaviour lives: "opened, then a reply for the *previous*
/// subject arrives" cannot be expressed as a single event.
#[derive(Serialize)]
struct Case {
    name: String,
    steps: Vec<Step>,
}

#[derive(Serialize)]
struct Step {
    event: AppEvent,
    /// The owning state slice afterwards. The frontend also verifies that all
    /// other slices equal their pre-event values, which preserves the
    /// cross-slice guard without repeating the full AppState for every step.
    expected: Value,
}

#[derive(Serialize)]
struct Fixture {
    /// Rust's `AppState::default()`. The frontend's hand-written `INITIAL`
    /// must equal this, or every session starts from a state the backend has
    /// never been in.
    initial: AppState,
    cases: Vec<Case>,
    helpers: HelperFixture,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HelperFixture {
    review_summaries: Vec<ReviewSummaryCase>,
    upload_busy: Vec<UploadBusyCase>,
    player_note_lookups: Vec<PlayerNoteLookupCase>,
    galactic_war_actions: Vec<GalacticWarActionCase>,
    tourney_rules: Vec<TourneyRuleCase>,
    tourney_open_events: Vec<TourneyOpenEventCase>,
    tourney_phase_legality: Vec<TourneyPhaseLegalityCase>,
    tourney_busy_matches: Vec<TourneyBusyMatchCase>,
    tourney_swiss_cuts: Vec<TourneySwissCutCase>,
    tourney_faction_configs: Vec<TourneyFactionConfigCase>,
    tourney_draft_rejections: Vec<TourneyDraftRejectionCase>,
    tourney_reports: Vec<TourneyReportCase>,
    tourney_map_matches: TourneyMapMatchFixture,
    tourney_standings: Vec<TourneyStandingsCase>,
    tourney_profiles: Vec<TourneyProfileCase>,
    tourney_pool_drafts: Vec<TourneyPoolDraftCase>,
    tourney_vetoes: Vec<TourneyVetoCase>,
    tourney_ffa: Vec<TourneyFfaCase>,
    tourney_drafts: Vec<TourneyDraftCase>,
    tourney_qualifiers: Vec<TourneyQualifierCase>,
    tourney_lifecycles: Vec<TourneyLifecycleCase>,
    tourney_rounds: Vec<TourneyRoundCase>,
    tourney_chat_rooms: Vec<TourneyChatRoomCase>,
    tourney_bracket_configs: Vec<TourneyBracketConfigCase>,
    tourney_match_plans: Vec<TourneyMatchPlanCase>,
    training_filters: TrainingFilterFixture,
    training_form_problems: TrainingFormProblemFixture,
}

/// What the two training forms refuse, and why.
///
/// These gate a submit button on every keystroke, so the frontend owns twins of
/// them. Recorded from the Rust functions for the same reason the filter cases
/// are: a hand-written expectation here would only pin someone's reading.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TrainingFormProblemFixture {
    reviews: Vec<TrainingReviewProblemCase>,
    contributions: Vec<TrainingContributionProblemCase>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TrainingReviewProblemCase {
    name: String,
    draft: Box<ReviewRequestDraft>,
    problem: Option<ReviewProblem>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TrainingContributionProblemCase {
    name: String,
    draft: Box<ContributionDraft>,
    problem: Option<ContributionProblem>,
}

/// The catalogue the filter cases run against, recorded alongside them so the
/// frontend twin filters the same entries rather than a hand-copied echo of
/// them.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TrainingFilterFixture {
    catalogue: Vec<TrainingResource>,
    cases: Vec<TrainingFilterCase>,
}

/// One library query and what `filter_resources` selects from a fixed
/// catalogue, plus the rating band each entry resolves to.
///
/// The library filter is one of the few rules that has to run on every
/// keystroke, so the frontend owns a twin of it rather than a round trip. This
/// is what stops the twin drifting: the ids are recorded from the Rust
/// function, not from anyone's reading of it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TrainingFilterCase {
    name: String,
    query: Box<TrainingQuery>,
    my_rating: Option<i32>,
    /// The reader's rating per game mode. Separate from `my_rating`, which is
    /// the overall one, because which number applies depends on the entry:
    /// a 1v1 guide is judged by a 1v1 rating.
    ratings: std::collections::BTreeMap<String, i32>,
    /// Ids selected, in the order the filter returns them.
    expected_ids: Vec<String>,
}

#[derive(Serialize)]
struct ReviewSummaryCase {
    reviews: Vec<Review>,
    expected: ReviewSummary,
}

#[derive(Serialize)]
struct UploadBusyCase {
    status: UploadStatus,
    expected: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PlayerNoteLookupCase {
    notes: Vec<PlayerNote>,
    player_id: i32,
    expected: String,
}

/// What the Galactic War panel derives from its slice.
///
/// The panel decides between "install", "update", "play" and "already
/// running" from these three answers, so a twin that drifts silently offers
/// the wrong button rather than failing visibly.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GalacticWarActionCase {
    state: GalacticWarState,
    install_target: String,
    update_available: bool,
    can_launch: bool,
}

/// What the tournament panes gate their controls on.
///
/// These rules decide whether a "join this team" button is drawn, whether the
/// seeding section exists, and how many entrants are waiting on the organiser.
/// The frontend re-derives every one of them from the same event, so a twin that
/// drifts offers a control the server then refuses: the player fills in a form
/// and loses it. Recorded per event rather than per function so one fixture
/// entry pins the whole rule set against the same input.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyRuleCase {
    name: String,
    event: Box<Tourney>,
    /// Team id the team-scoped rules were asked about, when the event has one.
    team_id: Option<String>,
    team_rating: i32,
    would_exceed_team_cap: bool,
    self_organised: bool,
    may_reseed: bool,
    /// `Tourney::may_shuffle_teams`: whether the organiser may still move people
    /// between teams. Refused once the bracket exists.
    may_shuffle_teams: bool,
    /// `Tourney::may_set_rating`: only an unrated event takes a typed rating.
    may_set_rating: bool,
    pending_signup_ids: Vec<String>,
    /// `Tourney::members` of the team above, in the order it holds them: a twin
    /// that iterated the entrant list instead would produce a different order.
    member_ids: Vec<String>,
    /// `Tourney::my_invites`: the teams waiting on *this* account's answer.
    my_invite_team_ids: Vec<String>,
    /// `Tourney::may_rename` for the team above. Until this was recorded the
    /// Rust half had no caller at all, and only the frontend twin ran.
    may_rename: bool,
    /// `Tourney::may_publish`: the service creates every tournament unpublished,
    /// so getting this wrong hides an event from everyone but its organiser.
    may_publish: bool,
    /// `Tourney::may_report` over every match of the event, in bracket order.
    /// Recorded as ids rather than a single bool so the `has_bracket` half of
    /// the rule is exercised: that half is the one the frontend twin had lost.
    reportable_match_ids: Vec<String>,
    /// `Tourney::may_submit` over every match: the players' own path.
    submittable_match_ids: Vec<String>,
    /// `Tourney::faction_veto_on`: 1v1 only, and never free-for-all.
    faction_veto_on: bool,
    /// `Tourney::may_configure_faction_veto`.
    may_configure_faction_veto: bool,
    /// `Tourney::third_place_on`: the plan before the draw, the match after.
    third_place_on: bool,
    /// `Tourney::may_add_third_place`.
    may_add_third_place: bool,
    /// `Tourney::may_remove_third_place`: only until anything happens in it.
    may_remove_third_place: bool,
    /// `Tourney::may_set_match_best_of` over every match, in bracket order.
    best_of_match_ids: Vec<String>,
    /// `Tourney::may_remove_organiser` over every organiser account.
    removable_organiser_ids: Vec<i32>,
    /// `Tourney::may_end_early`.
    may_end_early: bool,
    /// `Tourney::may_reopen_early`.
    may_reopen_early: bool,
    /// `Tourney::may_repull_ratings`.
    may_repull_ratings: bool,
    /// `Tourney::may_edit_veto`.
    may_edit_veto: bool,
    /// `Tourney::may_sign_up`: a ban withdraws the offer.
    may_sign_up: bool,
    /// `Tourney::may_decline_invite`.
    may_decline_invite: bool,
    /// `Tourney::may_check_rating`.
    may_check_rating: bool,
    /// `Tourney::signup_needs_rating`.
    signup_needs_rating: bool,
    /// The moment the check-in rules below were asked at, so that the twin
    /// asks at the same one rather than at the clock.
    check_in_now: u32,
    /// `Tourney::may_check_in` at `check_in_now`.
    may_check_in: bool,
    /// `Tourney::may_undo_check_in`.
    may_undo_check_in: bool,
    /// `Tourney::team_lineup`: who is in, who waits, who is still forming.
    team_lineup: TeamLineup,
    /// `TourneyState::unread_total` over the rooms below.
    rooms: Vec<ChatRoom>,
    unread_total: i32,
}

/// The stale-detail guard: a detail is only the open event's if the selection
/// still names it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyOpenEventCase {
    detail_id: Option<String>,
    selected_id: Option<String>,
    /// The id `TourneyState::open_event` yields, or `None` for no open event.
    open_id: Option<String>,
}

/// Which match, if any, a pending write belongs to.
///
/// Only the three reporting actions name one; everything else is event-wide and
/// must leave the rest of the bracket alone.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyBusyMatchCase {
    pending: Option<TourneyAction>,
    busy_match_id: Option<String>,
}

/// `FactionVetoConfig::is_submittable`: what `fveto_config` accepts.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyFactionConfigCase {
    config: FactionVetoConfig,
    submittable: bool,
}

/// `SwissCuts::rounds`: the round count the start dialog shows in place of
/// its own field when the plan has record cuts.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneySwissCutCase {
    cuts: SwissCuts,
    rounds: Option<i32>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyPhaseLegalityCase {
    phase: TourneyPhase,
    status: TourneyStatus,
    legal: bool,
}

/// Why the server would refuse a draft, and in which order.
///
/// The *first* refusal is the one the form shows, so the order matters as much
/// as the rules: a draft with two problems must name the same one the server
/// would. Held here because `DraftRejection` never reaches `AppState` and so is
/// absent from the generated bindings: the frontend spells the union out by
/// hand, which is exactly the kind of copy that drifts.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyDraftRejectionCase {
    name: String,
    draft: Box<TourneyDraft>,
    /// `None` when the server would take it.
    rejection: Option<DraftRejection>,
    submittable: bool,
}

/// Whether the server will take a match report.
///
/// The highest-stakes rule in the tab: a twin that says yes where the Rust says
/// no throws away a player's reported score, and one that says no where the Rust
/// says yes blocks a result that would have been accepted.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyReportCase {
    name: String,
    /// Only the fields both rules read.
    entry: TourneyReportEntry,
    score1: i32,
    score2: i32,
    replay_ids: Vec<String>,
    new_games: i32,
    submittable: bool,
    /// `MatchReport::is_player_submittable`: the same score through
    /// `report_submit`, which wants one replay id per new game and a score
    /// that only goes up.
    player_submittable: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyReportEntry {
    best_of: i32,
    handicap: i32,
    score1: Option<i32>,
    score2: Option<i32>,
}

/// The standings table, which is the same rule written three times: here, in
/// `Tourney::standings`, and in the browser twin. The service sends no table at
/// all, so nothing external would catch the three disagreeing.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyStandingsCase {
    name: String,
    event: Box<Tourney>,
    kind: StandingsKind,
    rows: Vec<Standing>,
}

/// Matching an entrant to the FAF account behind them.
///
/// The last of the tournament twins to be pinned. Cheap to get wrong in a way
/// nothing shouts about: a miss shows a bare name where every other list shows
/// an avatar, which reads as a gap in the vault rather than as a bug.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyProfileCase {
    name: String,
    profiles: Vec<PlayerSummary>,
    entrant: TourneyPlayer,
    /// The login `TourneyState::profile_of` resolves to, or `None`.
    resolved_login: Option<String>,
}

/// Whether the service would accept a pool.
///
/// Two counting rules that look like arithmetic and are not: every map but one
/// is consumed by a step, and every pick is a game. Getting them wrong sends the
/// organiser round a trip to be told numbers they then have to work backwards
/// from, which is exactly what the twin exists to avoid.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyPoolDraftCase {
    name: String,
    draft: PoolDraft,
    rejection: Option<PoolRejection>,
    submittable: bool,
}

/// The four answers an event gives about editing it, talking in it and reading
/// its news.
///
/// Grouped into one case because they are all read off the same event and all
/// four decide whether a control is drawn at all. The two format answers are
/// nested on purpose: the service locks the whole format once the bracket
/// exists, and locks the *team setup* one step earlier, at the end of signups.
/// A twin that collapsed them would offer the team size during a draft and be
/// answered "Reopen signups to change the team setup".
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyLifecycleCase {
    name: String,
    event: Box<Tourney>,
    may_edit_format: bool,
    may_edit_team_setup: bool,
    may_post_chat: bool,
    unread_news: i32,
    /// Whether changing the format to `format` touches the team setup.
    format: FormatDraft,
    structural: bool,
}

/// The best-of plan a draw would start from, and whether it would be taken.
///
/// Worth pinning because the round counts are the same arithmetic the round
/// projection uses and are wrong in the same quiet way: the service pads or
/// trims the list to the length the bracket really has, so a client that
/// offered one row too few would drop a round's setting without saying so.
/// The best-of template a bracket type starts from.
///
/// The service's own defaults, and they exist twice: the create form offers
/// them, and `MatchPlan::default_for` states them. A drift here would give the
/// organiser a form that disagrees with what the service would have done on its
/// own, which is exactly the kind of difference nobody notices until a final is
/// a Bo3.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyMatchPlanCase {
    kind: BracketKind,
    expected: MatchPlan,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyBracketConfigCase {
    name: String,
    event: Box<Tourney>,
    config: BracketConfig,
    submittable: bool,
}

/// How the room list is split, and what each room marks itself with.
///
/// The split is the tournament team's own requirement: a bracket makes a room
/// per match and never deletes one, so the played ones have to fold away or the
/// live list is unusable by the quarter-finals. The badge order matters for a
/// smaller reason that is easy to get backwards: being named by `@` replaces
/// the unread count rather than sitting beside it, because that is what makes
/// it findable.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyChatRoomCase {
    name: String,
    rooms: Vec<ChatRoom>,
    /// Ids, in order, of the rooms that stay in the live list.
    active: Vec<String>,
    /// Ids, in order, of the rooms folded into the completed group.
    completed: Vec<String>,
    /// Whether the collapsed group still has to announce itself.
    completed_wants_attention: bool,
    /// One badge per room, in the order the rooms came in.
    badges: Vec<RoomBadge>,
}

/// Which rounds a map pool can be bound to, before and after the draw.
///
/// The projected half is the one worth pinning: it decides how many rounds an
/// organiser is offered while signups are still open, and it is arithmetic
/// (`ceil(log2)`, and `2R-2` losers rounds) that is easy to get subtly wrong on
/// one side only. A client that offered one round too few would leave a round
/// of the real bracket with no map pool and nobody looking for it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyRoundCase {
    name: String,
    event: Box<Tourney>,
    plan: RoundPlan,
}

/// Whether a qualifier link would be taken, and why not where it would not.
///
/// Worth pinning for a reason the other rejections do not share: three of the
/// four answers mirror a refusal the service makes, and the fourth does not.
/// A points rule against an elimination bracket is *accepted* by the service
/// and then qualifies nobody, silently. If the two halves of the client drift
/// on that one, one of them starts offering a link that quietly does nothing.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyQualifierCase {
    name: String,
    /// The parent, which holds the links.
    event: Box<Tourney>,
    /// The child being linked in.
    candidate: Box<Tourney>,
    rule: QualifierRule,
    rejection: Option<QualifierRejection>,
}

/// Whose veto step is due, and who may take it.
///
/// Two captains act on one run concurrently, so a client that got the turn
/// wrong would show one of them a button that answers "Not your turn" and the
/// other nothing at all. The three refusals are pinned as carefully as the
/// permission: a finished run, a run with no sides chosen, and a run walked off
/// the end all read as "nobody is due" and must not be told apart by accident.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyVetoCase {
    name: String,
    event: Box<Tourney>,
    /// The match the run belongs to, always the first of the event.
    turn_team_id: Option<String>,
    may_veto: bool,
    may_set_sides: bool,
}

/// A free-for-all lobby: how many winners it wants, whether it is scored, and
/// whether a given report would be accepted.
///
/// Three rules that interlock and are each easy to get subtly wrong. The winner
/// count is capped by the field, a points event still decides its final by a
/// winner rather than a score, and a scored round needs a number for *every*
/// entrant rather than for the ones somebody typed.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyFfaCase {
    name: String,
    event: Box<Tourney>,
    report: FfaReport,
    winners_needed: i32,
    scored: bool,
    may_report: bool,
    submittable: bool,
}

/// Whose captains-draft pick is due, and who may take or undo it.
///
/// The undo rule is the subtle one: an organiser may take back any pick, but a
/// captain only their own and only while nobody has picked after them. Getting
/// that wrong lets one captain rewrite another's turn.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyDraftCase {
    name: String,
    event: Box<Tourney>,
    turn_team_id: Option<String>,
    may_pick: bool,
    may_undo: bool,
    /// `Tourney::undrafted`, in the order it returns them.
    undrafted_ids: Vec<String>,
}

/// Resolving an organiser's hand-typed map name against FAF's vault.
///
/// Pinned because the two sides fold the name differently by construction: Rust
/// filters on `char::is_alphanumeric`, the frontend on a `\p{L}|\p{N}` regex.
/// They agree today, and a divergence would not fail anything: it would silently
/// drop the map preview for one spelling, which reads as a vault gap rather than
/// as a bug.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyMapMatchFixture {
    /// The vault both sides resolve against.
    vault: Vec<TourneyMapVaultEntry>,
    cases: Vec<TourneyMapMatchCase>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct TourneyMapVaultEntry {
    display_name: String,
    folder_name: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TourneyMapMatchCase {
    /// The name as an organiser typed it into the tournament's map database.
    typed: String,
    /// `map_key` of that name.
    key: String,
    /// The vault entry it resolves to, or `None` for a map never uploaded.
    resolved_display_name: Option<String>,
}

fn galactic_war_state(
    installed: Option<&str>,
    versions: Option<(&str, Option<&str>)>,
    status: GalacticWarStatus,
    below_minimum: bool,
) -> GalacticWarState {
    GalacticWarState {
        status,
        installed_version: installed.map(Into::into),
        versions: versions.map(|(required, latest)| ClientVersions {
            required_version: required.into(),
            latest_version: latest.map(Into::into),
        }),
        below_minimum,
        ..Default::default()
    }
}

fn review(id: i32, score: i32, player: &str) -> Review {
    Review {
        id,
        score,
        text: String::new(),
        player: player.into(),
        version: "1".into(),
    }
}

fn tourney_player(
    id: &str,
    rating: Option<i32>,
    team: Option<&str>,
    pending: bool,
) -> TourneyPlayer {
    TourneyPlayer {
        id: id.into(),
        name: id.to_uppercase(),
        faf_id: None,
        rating,
        rating_actual: rating,
        team_id: team.map(Into::into),
        manual: false,
        late: false,
        pending,
        note: String::new(),
        signed_at: None,
        discord: String::new(),
        team_name: String::new(),
    }
}

fn tourney_team(id: &str, players: &[&str]) -> TourneyTeam {
    TourneyTeam {
        id: id.into(),
        name: id.to_uppercase(),
        seed: 0,
        captain_id: players.first().map(|first| (*first).to_string()),
        player_ids: players.iter().map(|id| (*id).to_string()).collect(),
        division: 0,
        checked_in: false,
        eliminated: false,
        out: None,
        final_rank: None,
        captain_renamed: false,
        join_requests: Vec::new(),
        invites: Vec::new(),
    }
}

/// The clock every check-in rule case is read at.
const CHECK_IN_NOW: u32 = 1_790_000_000;

/// One rule case, with every rule read off the same event.
fn tourney_rule_case(
    name: &str,
    event: Tourney,
    team_id: Option<&str>,
    rooms: Vec<ChatRoom>,
) -> TourneyRuleCase {
    let team = team_id.and_then(|id| event.team(id)).cloned();
    let state = TourneyState {
        chat_rooms: rooms.clone(),
        ..TourneyState::default()
    };
    TourneyRuleCase {
        name: name.to_string(),
        team_id: team_id.map(Into::into),
        team_rating: team.as_ref().map_or(0, |team| event.team_rating(team)),
        would_exceed_team_cap: team
            .as_ref()
            .is_some_and(|team| event.would_exceed_team_cap(team)),
        self_organised: event.teams_are_self_organised(),
        may_reseed: event.may_reseed(),
        may_shuffle_teams: event.may_shuffle_teams(),
        may_set_rating: event.may_set_rating(),
        pending_signup_ids: event
            .pending_signups()
            .into_iter()
            .map(|player| player.id.clone())
            .collect(),
        member_ids: team.as_ref().map_or_else(Vec::new, |team| {
            event
                .members(team)
                .into_iter()
                .map(|player| player.id.clone())
                .collect()
        }),
        my_invite_team_ids: event
            .my_invites()
            .into_iter()
            .map(|team| team.id.clone())
            .collect(),
        may_rename: team.as_ref().is_some_and(|team| event.may_rename(team)),
        may_publish: event.may_publish(),
        faction_veto_on: event.faction_veto_on(),
        may_configure_faction_veto: event.may_configure_faction_veto(),
        third_place_on: event.third_place_on(),
        may_add_third_place: event.may_add_third_place(),
        may_remove_third_place: event.may_remove_third_place(),
        best_of_match_ids: event
            .matches
            .iter()
            .filter(|entry| event.may_set_match_best_of(entry))
            .map(|entry| entry.id.clone())
            .collect(),
        removable_organiser_ids: event
            .organiser_accounts
            .iter()
            .filter(|organiser| event.may_remove_organiser(organiser.faf_id))
            .map(|organiser| organiser.faf_id)
            .collect(),
        may_end_early: event.may_end_early(),
        may_reopen_early: event.may_reopen_early(),
        may_repull_ratings: event.may_repull_ratings(),
        may_edit_veto: event.may_edit_veto(),
        may_sign_up: event.may_sign_up(),
        may_decline_invite: event.may_decline_invite(),
        may_check_rating: event.may_check_rating(),
        signup_needs_rating: event.signup_needs_rating(),
        check_in_now: CHECK_IN_NOW,
        may_check_in: event.may_check_in(CHECK_IN_NOW),
        may_undo_check_in: event.may_undo_check_in(),
        team_lineup: event.team_lineup(),
        reportable_match_ids: event
            .matches
            .iter()
            .filter(|entry| event.may_report(entry))
            .map(|entry| entry.id.clone())
            .collect(),
        submittable_match_ids: event
            .matches
            .iter()
            .filter(|entry| event.may_submit(entry))
            .map(|entry| entry.id.clone())
            .collect(),
        unread_total: state.unread_total(),
        rooms,
        event: Box::new(event),
    }
}

fn tourney_match(
    id: &str,
    bracket: BracketSide,
    team1: Option<&str>,
    team2: Option<&str>,
) -> TourneyMatch {
    TourneyMatch {
        id: id.into(),
        bracket,
        round: 1,
        index: 0,
        best_of: 3,
        handicap: 0,
        division: 0,
        team1: team1.map(Into::into),
        team2: team2.map(Into::into),
        score1: None,
        score2: None,
        status: MatchStatus::Ready,
        winner: None,
        loser: None,
        winner_to: None,
        loser_to: None,
        pending_report: None,
        veto: None,
        faction_veto: None,
        entrants: Vec::new(),
        winners: Vec::new(),
        points: Vec::new(),
        is_final: false,
        replay_ids: Vec::new(),
        draw_replay_ids: Vec::new(),
        forfeit: None,
    }
}

/// Two matches with two sides, plus a free-for-all round. Enough for
/// `may_report` to separate the three halves of its condition.
fn tourney_matches() -> Vec<TourneyMatch> {
    vec![
        tourney_match("m1", BracketSide::Winners, Some("t1"), Some("t2")),
        // No opponent drawn yet: the service refuses a result for a half-empty
        // slot, so the control must not be offered either.
        tourney_match("m2", BracketSide::Winners, Some("t1"), None),
        // `report` takes a different body for a free-for-all round, which this
        // client does not send.
        tourney_match("ffa1", BracketSide::FreeForAll, Some("t1"), Some("t2")),
    ]
}

fn tourney_rule_cases() -> Vec<TourneyRuleCase> {
    let four_single = four_team_single();
    let third = TourneyMatch {
        round: 2,
        team2: None,
        ..tourney_match("third", BracketSide::ThirdPlace, Some("t4"), None)
    };
    let with_third = Tourney {
        id: "with-third".into(),
        matches: [four_single.matches.clone(), vec![third.clone()]].concat(),
        ..four_single.clone()
    };
    let third_started = Tourney {
        id: "third-started".into(),
        matches: [
            four_single.matches.clone(),
            vec![TourneyMatch {
                veto: Some(MatchVeto {
                    banned: vec![VetoChoice {
                        map: "map1".into(),
                        by: "t4".into(),
                        game: None,
                    }],
                    ..running_veto(1, Some("t4"), false)
                }),
                ..third
            }],
        ]
        .concat(),
        ..four_single.clone()
    };
    let organiser = |faf_id: i32, name: &str| Organiser {
        faf_id,
        name: name.into(),
        hidden: false,
    };
    let two_organisers = Tourney {
        id: "two-organisers".into(),
        organiser_accounts: vec![organiser(1, "Nuggets"), organiser(2, "Seraphim")],
        ..four_single.clone()
    };
    let last_organiser = Tourney {
        id: "last-organiser".into(),
        organiser_accounts: vec![organiser(1, "Nuggets")],
        rating_kind: RatingKind::None,
        ..four_single.clone()
    };
    let stopped_early = Tourney {
        id: "stopped-early".into(),
        status: TourneyStatus::Finished,
        early_finish: Some(EarlyFinish {
            at: Some(1_790_000_000),
            by: "Nuggets".into(),
            automatic: false,
            target: 0,
            alive: 2,
            names: vec!["T1".into(), "T2".into()],
            unplayed: Some(Vec::new()),
        }),
        ..four_single.clone()
    };
    // Signups open on an unrated event, seen by a player with an invitation.
    let invited_player = Tourney {
        id: "invited-player".into(),
        status: TourneyStatus::Signup,
        rating_kind: RatingKind::None,
        viewer: TourneyViewer {
            logged_in: true,
            invited: true,
            ..TourneyViewer::default()
        },
        ..Tourney::default()
    };
    let third_planned = Tourney {
        id: "third-planned".into(),
        status: TourneyStatus::Drafted,
        matches: Vec::new(),
        plan: Some(MatchPlan::Single {
            early: 3,
            semi: 3,
            final_bo: 5,
            third_place: true,
        }),
        ..four_single.clone()
    };
    // An open 2v2 taking signups, with a combined-rating ceiling that this
    // account would push `t1` over but not `t2`.
    let capped = Tourney {
        id: "cap".into(),
        status: TourneyStatus::Signup,
        formation: Formation::Open,
        team_size: 2,
        published: true,
        rating: RatingGate {
            max_team: Some(3_000),
            ..RatingGate::default()
        },
        players: vec![
            tourney_player("me", Some(1_600), None, false),
            tourney_player("p1", Some(1_800), Some("t1"), false),
            tourney_player("p2", Some(900), Some("t2"), false),
            // No rating at all: must count as nothing rather than break the sum.
            tourney_player("p3", None, Some("t2"), false),
            tourney_player("p9", Some(1_000), None, true),
        ],
        teams: vec![
            tourney_team("t1", &["p1"]),
            // Has asked this account to join: the one thing in the teams pane
            // that is waiting on the reader rather than on somebody else.
            TourneyTeam {
                invites: vec![TeamRequest {
                    player_id: "me".into(),
                    name: "ME".into(),
                    at: None,
                }],
                ..tourney_team("t2", &["p2", "p3"])
            },
        ],
        viewer: TourneyViewer {
            logged_in: true,
            signed_up_player_id: Some("me".into()),
            ..TourneyViewer::default()
        },
        ..Tourney::default()
    };
    let under_cap = Tourney {
        id: "under".into(),
        ..capped.clone()
    };
    // Teams formed, bracket not drawn: the one window seeds can change in.
    let drafted = Tourney {
        id: "drafted".into(),
        published: true,
        status: TourneyStatus::Drafted,
        formation: Formation::Open,
        team_size: 2,
        players: vec![tourney_player("p1", Some(1_500), Some("t1"), false)],
        teams: vec![tourney_team("t1", &["p1"])],
        ..Tourney::default()
    };
    // A solo event: nothing to form, so no team controls at all, and no seeding
    // either until teams exist.
    let solo = Tourney {
        id: "solo".into(),
        published: true,
        status: TourneyStatus::Signup,
        formation: Formation::Solo,
        team_size: 1,
        players: vec![tourney_player("p1", Some(1_500), None, true)],
        ..Tourney::default()
    };

    // The organiser's own view of the same drafted event: this is where team
    // shuffling is offered, and nowhere else.
    let organised = Tourney {
        id: "organised".into(),
        viewer: TourneyViewer {
            logged_in: true,
            organiser: true,
            ..TourneyViewer::default()
        },
        // Matches exist in the model before the draw is run, so the status is
        // what decides, not their presence.
        matches: tourney_matches(),
        ..drafted.clone()
    };
    // The same, once the bracket exists: the draw was made from these teams, so
    // the service refuses to move anyone.
    let organised_running = Tourney {
        id: "organised-running".into(),
        status: TourneyStatus::Running,
        ..organised.clone()
    };
    // Freshly created: taking signups already, and visible to nobody else.
    let unpublished = Tourney {
        id: "unpublished".into(),
        status: TourneyStatus::Signup,
        published: false,
        ..organised.clone()
    };
    // The same event as any other reader sees it: no control, because publishing
    // is not theirs to do.
    let unpublished_visitor = Tourney {
        id: "unpublished-visitor".into(),
        viewer: TourneyViewer {
            logged_in: true,
            ..TourneyViewer::default()
        },
        ..unpublished.clone()
    };
    // An unrated event, which is the only kind that takes a typed rating.
    let unrated = Tourney {
        id: "unrated".into(),
        rating_kind: RatingKind::None,
        ..organised.clone()
    };

    // The captain's own view: one rename, and only while the team holds more
    // than one player.
    let captained = Tourney {
        id: "captained".into(),
        published: true,
        status: TourneyStatus::Signup,
        formation: Formation::Open,
        team_size: 2,
        players: vec![
            tourney_player("me", Some(1_500), Some("t1"), false),
            tourney_player("p2", Some(1_500), Some("t1"), false),
        ],
        teams: vec![TourneyTeam {
            captain_id: Some("me".into()),
            ..tourney_team("t1", &["me", "p2"])
        }],
        viewer: TourneyViewer {
            logged_in: true,
            signed_up_player_id: Some("me".into()),
            ..TourneyViewer::default()
        },
        ..Tourney::default()
    };
    // A player in a running event: a score of their own match can be
    // submitted, not one of a half-drawn slot or a free-for-all lobby.
    let player_running = Tourney {
        id: "player-running".into(),
        player_reporting: true,
        team_size: 1,
        faction_veto: FactionVetoConfig {
            enabled: true,
            bans: 1,
            picks: 2,
        },
        viewer: TourneyViewer {
            logged_in: true,
            member_team_id: Some("t1".into()),
            ..TourneyViewer::default()
        },
        ..organised_running.clone()
    };
    // The same player where the organiser keeps every result to themselves.
    let organiser_reports = Tourney {
        id: "organiser-reports".into(),
        player_reporting: false,
        ..player_running.clone()
    };
    // An open 2v2 with room for two teams and three that filled up, `td`
    // first: places go by when a team filled, so `tc` waits although it is
    // the strongest, and `tb` is still one short. Check-in opens in an hour.
    let open_lineup = Tourney {
        id: "open-lineup".into(),
        published: true,
        status: TourneyStatus::Signup,
        formation: Formation::Open,
        team_size: 2,
        max_teams: 2,
        check_in_opens_at: Some(CHECK_IN_NOW + 3_600),
        players: vec![
            tourney_player("p1", Some(1_200), Some("ta"), false),
            tourney_player("p2", Some(1_100), Some("ta"), false),
            tourney_player("p3", Some(1_900), Some("tb"), false),
            tourney_player("p4", Some(1_800), Some("tc"), false),
            tourney_player("p5", Some(1_700), Some("tc"), false),
            tourney_player("p6", Some(1_000), Some("td"), false),
            tourney_player("p7", None, Some("td"), false),
        ],
        teams: vec![
            tourney_team("ta", &["p1", "p2"]),
            tourney_team("tb", &["p3"]),
            tourney_team("tc", &["p4", "p5"]),
            tourney_team("td", &["p6", "p7"]),
        ],
        entry_order: vec!["td".into(), "ta".into(), "tc".into()],
        viewer: TourneyViewer {
            logged_in: true,
            signed_up_player_id: Some("p1".into()),
            member_team_id: Some("ta".into()),
            ..TourneyViewer::default()
        },
        ..Tourney::default()
    };
    // The same field once check-in has opened.
    let lineup_check_in = Tourney {
        id: "lineup-check-in".into(),
        check_in_opens_at: Some(CHECK_IN_NOW - 60),
        ..open_lineup.clone()
    };
    // The same field once the organiser has locked it: the service refuses
    // every team action from here on, check-in included.
    let lineup_locked = Tourney {
        id: "lineup-locked".into(),
        status: TourneyStatus::Drafted,
        ..lineup_check_in.clone()
    };
    // The organiser has seeded two teams and this one has checked in: seeds
    // decide the order then, the unseeded team goes last, and a check-in can
    // be taken back.
    let lineup_seeded = Tourney {
        id: "lineup-seeded".into(),
        teams: vec![
            TourneyTeam {
                seed: 2,
                checked_in: true,
                ..tourney_team("ta", &["p1", "p2"])
            },
            tourney_team("tb", &["p3"]),
            TourneyTeam {
                seed: 1,
                ..tourney_team("tc", &["p4", "p5"])
            },
            tourney_team("td", &["p6", "p7"]),
        ],
        ..lineup_check_in.clone()
    };
    let renamed_once = Tourney {
        id: "renamed-once".into(),
        teams: vec![TourneyTeam {
            captain_id: Some("me".into()),
            captain_renamed: true,
            ..tourney_team("t1", &["me", "p2"])
        }],
        ..captained.clone()
    };

    vec![
        tourney_rule_case(
            "an open 2v2 where joining would break the team cap",
            capped,
            Some("t1"),
            vec![tourney_room("global", 3), tourney_room("m1", 1)],
        ),
        tourney_rule_case(
            "the same event, for a team there is still room under",
            under_cap,
            Some("t2"),
            Vec::new(),
        ),
        tourney_rule_case(
            "teams formed, bracket not drawn",
            drafted,
            Some("t1"),
            vec![],
        ),
        tourney_rule_case("a solo event with a signup waiting", solo, None, vec![]),
        tourney_rule_case(
            "the organiser's view before the draw: teams can still be shuffled",
            organised,
            Some("t1"),
            vec![],
        ),
        tourney_rule_case(
            "the same event once the bracket exists: shuffling is refused",
            organised_running,
            Some("t1"),
            vec![],
        ),
        tourney_rule_case(
            "an unrated event, the only kind that takes a typed rating",
            unrated,
            Some("t1"),
            vec![],
        ),
        tourney_rule_case(
            "a freshly created event the organiser has not published yet",
            unpublished,
            Some("t1"),
            vec![],
        ),
        tourney_rule_case(
            "the same unpublished event, seen by someone who cannot publish it",
            unpublished_visitor,
            Some("t1"),
            vec![],
        ),
        tourney_rule_case(
            "a captain who has not yet spent their one rename",
            captained,
            Some("t1"),
            vec![],
        ),
        tourney_rule_case(
            "the same captain, once the service has counted the rename",
            renamed_once,
            Some("t1"),
            vec![],
        ),
        tourney_rule_case(
            "an open 2v2 with more full teams than places, before check-in opens",
            open_lineup,
            Some("ta"),
            vec![],
        ),
        tourney_rule_case(
            "the same field once check-in has opened",
            lineup_check_in,
            Some("ta"),
            vec![],
        ),
        tourney_rule_case(
            "the same field once it is locked: no check-in any more",
            lineup_locked,
            Some("ta"),
            vec![],
        ),
        tourney_rule_case(
            "the same field seeded by the organiser, this team checked in",
            lineup_seeded,
            Some("ta"),
            vec![],
        ),
        tourney_rule_case(
            "a player in a running event that takes players' scores",
            player_running,
            Some("t1"),
            vec![],
        ),
        tourney_rule_case(
            "the same player where only the organiser reports",
            organiser_reports,
            Some("t1"),
            vec![],
        ),
        tourney_rule_case(
            "a running single elimination of four, without a 3rd place match",
            four_single.clone(),
            None,
            vec![],
        ),
        tourney_rule_case(
            "the same bracket with a 3rd place match nobody has touched",
            with_third,
            None,
            vec![],
        ),
        tourney_rule_case(
            "the same, once a map has been banned in it: it stays",
            third_started,
            None,
            vec![],
        ),
        tourney_rule_case(
            "a single elimination planned with a 3rd place match, before the draw",
            third_planned,
            None,
            vec![],
        ),
        tourney_rule_case(
            "two organisers, either of whom may remove the other or leave",
            two_organisers,
            None,
            vec![],
        ),
        tourney_rule_case(
            "the last organiser, who may not leave the event with nobody",
            last_organiser,
            None,
            vec![],
        ),
        tourney_rule_case(
            "an elimination stopped early, which its organiser may reopen",
            stopped_early,
            None,
            vec![],
        ),
        tourney_rule_case(
            "an invited player, taking signups, who may decline",
            invited_player.clone(),
            None,
            vec![],
        ),
        tourney_rule_case(
            "the same player, banned: not offered Enter at all",
            Tourney {
                id: "banned-player".into(),
                my_ban: Some(OwnBan {
                    scope: BanScope::Series,
                    reason: "No show".into(),
                    expires: None,
                }),
                ..invited_player
            },
            None,
            vec![],
        ),
    ]
}

/// A single elimination of four, running, seen by its organiser: one
/// semi-final played, the other and the final still to come.
fn four_team_single() -> Tourney {
    let semi = |id: &str, index: i32, team1: &str, team2: &str| TourneyMatch {
        index,
        winner_to: Some(MatchLink {
            match_id: "final".into(),
            slot: index + 1,
        }),
        ..tourney_match(id, BracketSide::Winners, Some(team1), Some(team2))
    };
    Tourney {
        id: "four-single".into(),
        status: TourneyStatus::Running,
        competition: Competition::Team,
        bracket_kind: BracketKind::Single,
        team_size: 1,
        viewer: TourneyViewer {
            logged_in: true,
            organiser: true,
            ..TourneyViewer::default()
        },
        teams: ["t1", "t2", "t3", "t4"]
            .iter()
            .map(|id| tourney_team(id, &[]))
            .collect(),
        matches: vec![
            TourneyMatch {
                status: MatchStatus::Done,
                score1: Some(2),
                score2: Some(0),
                winner: Some("t1".into()),
                loser: Some("t4".into()),
                ..semi("s1", 0, "t1", "t4")
            },
            // A live score is a game played, so its length is locked.
            TourneyMatch {
                status: MatchStatus::Live,
                score1: Some(1),
                score2: Some(0),
                ..semi("s2", 1, "t2", "t3")
            },
            TourneyMatch {
                round: 2,
                best_of: 5,
                team2: None,
                ..tourney_match("final", BracketSide::Winners, Some("t1"), None)
            },
        ],
        ..Tourney::default()
    }
}

fn standings_team(id: &str, seed: i32, out: Option<(BracketSide, i32)>) -> TourneyTeam {
    TourneyTeam {
        seed,
        out: out.map(|(bracket, round)| TeamExit { bracket, round }),
        ..tourney_team(id, &[])
    }
}

fn standings_match(
    id: &str,
    status: MatchStatus,
    sides: (Option<&str>, Option<&str>),
    score: (Option<i32>, Option<i32>),
    decided: (Option<&str>, Option<&str>),
) -> TourneyMatch {
    TourneyMatch {
        status,
        score1: score.0,
        score2: score.1,
        winner: decided.0.map(Into::into),
        loser: decided.1.map(Into::into),
        ..tourney_match(id, BracketSide::Swiss, sides.0, sides.1)
    }
}

fn tourney_standings_cases() -> Vec<TourneyStandingsCase> {
    // A four-team double elimination, played out. Two teams went out at the
    // same depth, which is the case a naive index would rank 3 and 4.
    let finished = Tourney {
        id: "finished".into(),
        status: TourneyStatus::Finished,
        bracket_kind: BracketKind::Double,
        champion_team_id: Some("t1".into()),
        teams: vec![
            standings_team("t4", 4, Some((BracketSide::Losers, 1))),
            standings_team("t2", 2, Some((BracketSide::GrandFinal, 1))),
            standings_team("t3", 3, Some((BracketSide::Losers, 1))),
            standings_team("t1", 1, None),
        ],
        ..Tourney::default()
    };
    // Mid-event: nobody has won, so nobody may be called first.
    let running = Tourney {
        id: "running".into(),
        status: TourneyStatus::Running,
        bracket_kind: BracketKind::Single,
        champion_team_id: None,
        teams: vec![
            standings_team("t2", 2, Some((BracketSide::Winners, 1))),
            standings_team("t1", 1, None),
            standings_team("t3", 3, Some((BracketSide::Winners, 2))),
        ],
        ..Tourney::default()
    };
    // Swiss, including the bye that a team drawing the odd number gets.
    let swiss = Tourney {
        id: "swiss".into(),
        status: TourneyStatus::Running,
        bracket_kind: BracketKind::Swiss,
        teams: vec![
            standings_team("t1", 1, None),
            standings_team("t2", 2, None),
            standings_team("t3", 3, None),
        ],
        matches: vec![
            standings_match(
                "m1",
                MatchStatus::Done,
                (Some("t1"), Some("t2")),
                (Some(2), Some(0)),
                (Some("t1"), Some("t2")),
            ),
            standings_match(
                "m2",
                MatchStatus::Bye,
                (Some("t3"), None),
                (None, None),
                (None, None),
            ),
        ],
        ..Tourney::default()
    };
    // 2-0 on +2 against 2-1 on +3: the server ranks the unbeaten one first.
    let result = |id: &str, winner: &str, loser: &str, high: i32, low: i32| {
        standings_match(
            id,
            MatchStatus::Done,
            (Some(winner), Some(loser)),
            (Some(high), Some(low)),
            (Some(winner), Some(loser)),
        )
    };
    let fewer_losses = Tourney {
        id: "fewer-losses".into(),
        status: TourneyStatus::Running,
        bracket_kind: BracketKind::Swiss,
        teams: ["t1", "t2", "t3", "t4", "t5"]
            .iter()
            .enumerate()
            .map(|(index, id)| standings_team(id, index as i32 + 1, None))
            .collect(),
        matches: vec![
            result("m1", "t1", "t3", 1, 0),
            result("m2", "t1", "t4", 1, 0),
            result("m3", "t2", "t3", 2, 0),
            result("m4", "t2", "t4", 2, 0),
            result("m5", "t5", "t2", 1, 0),
        ],
        ..Tourney::default()
    };
    // The same table as the server ordered it, under the beaten tiebreak.
    let server_order = Tourney {
        id: "server-order".into(),
        swiss_order: vec![
            "t2".into(),
            "t1".into(),
            "t5".into(),
            "t3".into(),
            "t4".into(),
        ],
        swiss_tiebreak: SwissTiebreak::Beaten,
        swiss_beaten: [("t1".to_string(), 0), ("t2".to_string(), 1)]
            .into_iter()
            .collect(),
        ..fewer_losses.clone()
    };
    // An import, which often carries a final table and nothing else.
    let imported = Tourney {
        id: "imported".into(),
        imported: true,
        status: TourneyStatus::Finished,
        champion_team_id: Some("t1".into()),
        teams: vec![
            TourneyTeam {
                final_rank: Some(2),
                ..standings_team("t2", 2, None)
            },
            TourneyTeam {
                final_rank: Some(1),
                ..standings_team("t1", 1, None)
            },
            standings_team("t9", 9, None),
        ],
        ..Tourney::default()
    };
    // A scored free-for-all, where the total decides and nobody is knocked out.
    let points = Tourney {
        id: "points".into(),
        status: TourneyStatus::Running,
        competition: Competition::FreeForAll,
        ffa: Some(FfaConfig {
            per_match: 3,
            advance: 1,
            mode: FfaMode::Points,
            rounds: 2,
            cut_to: 0,
            final_size: 0,
        }),
        teams: vec![
            standings_team("t1", 1, None),
            standings_team("t2", 2, None),
            standings_team("t3", 3, None),
        ],
        matches: vec![TourneyMatch {
            points: vec![
                TeamPoints {
                    team_id: "t1".into(),
                    points: 3,
                },
                TeamPoints {
                    team_id: "t2".into(),
                    points: 7,
                },
                TeamPoints {
                    team_id: "t3".into(),
                    points: 7,
                },
            ],
            ..tourney_match("f1", BracketSide::FreeForAll, None, None)
        }],
        ..Tourney::default()
    };
    // A single elimination of four with a 3rd place match played: 1, 2, 3, 4
    // rather than a shared 3rd. The final loser is out in the final's round,
    // the two semi-final losers in the 3rd place match that carries the same
    // round number.
    let third_place = Tourney {
        id: "third-place".into(),
        status: TourneyStatus::Finished,
        bracket_kind: BracketKind::Single,
        champion_team_id: Some("t1".into()),
        teams: vec![
            standings_team("t4", 4, Some((BracketSide::ThirdPlace, 2))),
            standings_team("t3", 3, Some((BracketSide::ThirdPlace, 2))),
            standings_team("t2", 2, Some((BracketSide::Winners, 2))),
            standings_team("t1", 1, None),
        ],
        matches: vec![TourneyMatch {
            round: 2,
            status: MatchStatus::Done,
            winner: Some("t4".into()),
            loser: Some("t3".into()),
            ..tourney_match("third", BracketSide::ThirdPlace, Some("t3"), Some("t4"))
        }],
        ..Tourney::default()
    };
    // Signups: no table at all, and a pane that drew one would be inventing it.
    let early = Tourney {
        id: "early".into(),
        status: TourneyStatus::Signup,
        teams: vec![standings_team("t1", 1, None)],
        ..Tourney::default()
    };

    [
        (
            "a finished double elimination, with a shared third",
            finished,
        ),
        ("mid-event, where nobody has a place yet", running),
        ("a Swiss table, including a bye", swiss),
        (
            "a Swiss table, where fewer losses outrank game difference",
            fewer_losses,
        ),
        (
            "a Swiss table in the server's own order, beaten tiebreak",
            server_order,
        ),
        ("an import, which carries only its own placings", imported),
        (
            "a scored free-for-all, where seed breaks a points tie",
            points,
        ),
        ("signups, where there is no table", early),
        (
            "a single elimination whose 3rd place match was played",
            third_place,
        ),
    ]
    .into_iter()
    .map(|(name, event)| TourneyStandingsCase {
        name: name.to_string(),
        kind: event.standings_kind(),
        rows: event.standings(),
        event: Box::new(event),
    })
    .collect()
}

fn tourney_profile_cases() -> Vec<TourneyProfileCase> {
    let profiles = vec![
        PlayerSummary {
            id: 101,
            login: "Nuggets".into(),
            avatar_url: String::new(),
            country: "DE".into(),
            global_rating: Some(1_700),
            ladder_rating: None,
        },
        PlayerSummary {
            id: 102,
            login: "Ada".into(),
            avatar_url: String::new(),
            country: "GB".into(),
            global_rating: Some(1_900),
            ladder_rating: None,
        },
    ];

    [
        // The ordinary case, and the one that must not match on name.
        (
            "an entrant whose account is loaded",
            tourney_player_with_faf("p1", Some(102)),
        ),
        // An organiser can add a player by hand, and that entry is a name and
        // nothing else. A real case rather than a failure.
        (
            "an entrant added by hand, with no account",
            tourney_player_with_faf("p2", None),
        ),
        // Loaded profiles lag the entrant list: a signup that arrives before
        // the next profile fetch has an id nothing answers to yet.
        (
            "an account the profile list has not caught up with",
            tourney_player_with_faf("p3", Some(999)),
        ),
    ]
    .into_iter()
    .map(|(name, entrant)| {
        let state = TourneyState {
            entrant_profiles: profiles.clone(),
            ..TourneyState::default()
        };
        TourneyProfileCase {
            name: name.to_string(),
            resolved_login: state.profile_of(&entrant).map(|found| found.login.clone()),
            profiles: profiles.clone(),
            entrant,
        }
    })
    .collect()
}

fn tourney_player_with_faf(id: &str, faf_id: Option<i32>) -> TourneyPlayer {
    TourneyPlayer {
        faf_id,
        ..tourney_player(id, Some(1_500), None, false)
    }
}

fn pool_step(action: PoolAction, team: PoolSide) -> PoolStep {
    PoolStep { action, team }
}

fn tourney_pool_draft_cases() -> Vec<TourneyPoolDraftCase> {
    let maps =
        |count: usize| -> Vec<String> { (0..count).map(|index| format!("map{index}")).collect() };
    // A Bo3 over four maps: three steps, of which two are picks, leaving one
    // map as the decider. This is the shape the service wants.
    let good = PoolDraft {
        id: String::new(),
        name: "Round 1".into(),
        map_ids: maps(4),
        best_of: Some(3),
        sequence: vec![
            pool_step(PoolAction::Ban, PoolSide::A),
            pool_step(PoolAction::Pick, PoolSide::B),
            pool_step(PoolAction::Pick, PoolSide::A),
        ],
        publish_at: None,
    };

    [
        ("a Bo3 over four maps with two picks", good.clone()),
        (
            "no name",
            PoolDraft {
                name: "  ".into(),
                ..good.clone()
            },
        ),
        (
            "no maps",
            PoolDraft {
                map_ids: Vec::new(),
                ..good.clone()
            },
        ),
        (
            "no order at all, which is a plain list of maps",
            PoolDraft {
                sequence: Vec::new(),
                ..good.clone()
            },
        ),
        (
            "one step too few for the map count",
            PoolDraft {
                map_ids: maps(5),
                ..good.clone()
            },
        ),
        (
            "the right number of steps, the wrong number of picks",
            PoolDraft {
                sequence: vec![
                    pool_step(PoolAction::Ban, PoolSide::A),
                    pool_step(PoolAction::Ban, PoolSide::B),
                    pool_step(PoolAction::Pick, PoolSide::A),
                ],
                ..good.clone()
            },
        ),
        (
            "a Bo1, which wants no picks at all",
            PoolDraft {
                best_of: Some(1),
                map_ids: maps(2),
                sequence: vec![pool_step(PoolAction::Ban, PoolSide::A)],
                ..good
            },
        ),
    ]
    .into_iter()
    .map(|(name, draft)| TourneyPoolDraftCase {
        name: name.to_string(),
        rejection: draft.rejection(),
        submittable: draft.rejection().is_none(),
        draft,
    })
    .collect()
}

fn tourney_bracket_config_cases() -> Vec<TourneyBracketConfigCase> {
    let with_teams = |count: usize, competition, bracket| Tourney {
        id: "e1".into(),
        status: TourneyStatus::Drafted,
        competition,
        bracket_kind: bracket,
        teams: (0..count)
            .map(|index| tourney_team(&format!("t{index}"), &[]))
            .collect(),
        ..Tourney::default()
    };

    [
        (
            "eight teams, single elimination",
            with_teams(8, Competition::Team, BracketKind::Single),
        ),
        (
            "a field that is not a power of two",
            with_teams(6, Competition::Team, BracketKind::Single),
        ),
        (
            "double elimination, where the losers side is 2R - 2 rounds",
            with_teams(8, Competition::Team, BracketKind::Double),
        ),
        (
            "swiss, which is a round count rather than a tree",
            with_teams(8, Competition::Team, BracketKind::Swiss),
        ),
        (
            "a free-for-all, which is drawn from its own configuration",
            with_teams(8, Competition::FreeForAll, BracketKind::Single),
        ),
        (
            "the smallest bracket there is",
            with_teams(2, Competition::Team, BracketKind::Double),
        ),
    ]
    .into_iter()
    .map(|(name, event)| {
        let config = BracketConfig::of(&event);
        TourneyBracketConfigCase {
            name: name.to_string(),
            submittable: config.is_submittable(event.teams.len() as i32),
            config,
            event: Box::new(event),
        }
    })
    .collect()
}

fn tourney_chat_room_cases() -> Vec<TourneyChatRoomCase> {
    let mentioned = |id: &str| ChatRoom {
        mentioned: true,
        unread: 4,
        ..tourney_room(id, 4)
    };

    [
        (
            "a bracket part-way through: two live rooms and two played",
            vec![
                tourney_room("global", 2),
                tourney_room("match:m3", 0),
                tourney_room_done("match:m1", false),
                tourney_room_done("match:m2", false),
            ],
        ),
        (
            // The case the fold costs something, and the reason the group
            // carries a mark of its own.
            "named in a room that has been folded away",
            vec![
                tourney_room("global", 0),
                tourney_room_done("match:m1", true),
            ],
        ),
        (
            "named in a live room, where the mark replaces the count",
            vec![mentioned("global"), tourney_room("match:m1", 7)],
        ),
        (
            "nothing has happened anywhere",
            vec![tourney_room("global", 0)],
        ),
        (
            "every room is finished",
            vec![
                tourney_room_done("match:m1", false),
                tourney_room_done("match:m2", false),
            ],
        ),
    ]
    .into_iter()
    .map(|(name, rooms)| {
        let state = TourneyState {
            chat_rooms: rooms.clone(),
            ..TourneyState::default()
        };
        let (active, completed) = state.chat_groups();
        TourneyChatRoomCase {
            name: name.to_string(),
            active: active.iter().map(|room| room.id.clone()).collect(),
            completed: completed.iter().map(|room| room.id.clone()).collect(),
            completed_wants_attention: state.completed_wants_attention(),
            badges: rooms.iter().map(ChatRoom::badge).collect(),
            rooms,
        }
    })
    .collect()
}

fn tourney_round_cases() -> Vec<TourneyRoundCase> {
    let base = Tourney {
        id: "e1".into(),
        status: TourneyStatus::Signup,
        competition: Competition::Team,
        team_size: 2,
        bracket_kind: BracketKind::Single,
        ..Tourney::default()
    };
    let entrants = |count: usize| -> Vec<TourneyPlayer> {
        (0..count)
            .map(|index| tourney_player(&format!("p{index}"), Some(1_500), None, false))
            .collect()
    };

    [
        (
            "eight signups in a 2v2, so four teams and two rounds",
            Tourney {
                players: entrants(8),
                ..base.clone()
            },
        ),
        (
            // The cap answers before anybody has entered, which is what makes
            // preparing pools possible on the day the event is created.
            "no signups yet, but a cap of eight",
            Tourney {
                max_teams: 8,
                ..base.clone()
            },
        ),
        (
            "a field that does not divide into a power of two",
            Tourney {
                players: entrants(10),
                ..base.clone()
            },
        ),
        (
            "double elimination, which adds a losers side and a grand final",
            Tourney {
                max_teams: 8,
                bracket_kind: BracketKind::Double,
                ..base.clone()
            },
        ),
        (
            "swiss, which is rounds and a final rather than a tree",
            Tourney {
                max_teams: 8,
                bracket_kind: BracketKind::Swiss,
                ..base.clone()
            },
        ),
        (
            "too few entrants to draw anything",
            Tourney {
                players: entrants(1),
                ..base.clone()
            },
        ),
        (
            // A free-for-all has no ban/pick rounds, so it must not project a
            // bracket it will never draw.
            "a free-for-all",
            Tourney {
                max_teams: 8,
                competition: Competition::FreeForAll,
                ..base.clone()
            },
        ),
        (
            // Once the draw exists the projection is irrelevant: the real
            // rounds win, cap or no cap.
            "a drawn bracket, where the real rounds win",
            Tourney {
                max_teams: 64,
                status: TourneyStatus::Running,
                matches: tourney_matches(),
                ..base.clone()
            },
        ),
        (
            // The website projects a round of its own for it, so a pool can
            // be bound to it before the draw.
            "a single elimination planned with a 3rd place match",
            Tourney {
                max_teams: 8,
                plan: Some(MatchPlan::Single {
                    early: 3,
                    semi: 3,
                    final_bo: 5,
                    third_place: true,
                }),
                ..base.clone()
            },
        ),
    ]
    .into_iter()
    .map(|(name, event)| TourneyRoundCase {
        name: name.to_string(),
        plan: event.round_plan(),
        event: Box::new(event),
    })
    .collect()
}

fn tourney_lifecycle_cases() -> Vec<TourneyLifecycleCase> {
    let news = |id: &str, at: u32| NewsPost {
        id: id.into(),
        body: "Start moved an hour later.".into(),
        by: "Nuggets".into(),
        at: Some(at),
        edited_at: None,
        important: true,
    };
    let base = Tourney {
        id: "e1".into(),
        status: TourneyStatus::Signup,
        competition: Competition::Team,
        formation: Formation::Open,
        bracket_kind: BracketKind::Double,
        team_size: 2,
        news: vec![news("n1", 1_786_100_000), news("n2", 1_786_200_000)],
        viewer: TourneyViewer {
            logged_in: true,
            organiser: true,
            faf_id: Some(101),
            ..TourneyViewer::default()
        },
        ..Tourney::default()
    };
    let unchanged = FormatDraft::of(&base);
    let bracket_only = FormatDraft {
        bracket_kind: BracketKind::Swiss,
        ..unchanged.clone()
    };
    let team_setup = FormatDraft {
        team_size: 4,
        ..unchanged.clone()
    };

    [
        (
            "signups open, nothing read yet",
            base.clone(),
            bracket_only.clone(),
        ),
        (
            "signups open, changing the team size",
            base.clone(),
            team_setup.clone(),
        ),
        (
            "signups open, changing nothing at all",
            base.clone(),
            unchanged,
        ),
        (
            // The step that matters: the format is still editable, the team
            // setup is not.
            "teams formed, so only the bracket type is still open",
            Tourney {
                status: TourneyStatus::Drafted,
                ..base.clone()
            },
            team_setup.clone(),
        ),
        (
            "the bracket is drawn, and the format is locked",
            Tourney {
                status: TourneyStatus::Running,
                ..base.clone()
            },
            bracket_only.clone(),
        ),
        (
            "a reader who is not the organiser",
            Tourney {
                viewer: TourneyViewer {
                    organiser: false,
                    ..base.viewer.clone()
                },
                ..base.clone()
            },
            bracket_only.clone(),
        ),
        (
            "silenced in chat, which is not the same as a locked room",
            Tourney {
                chat_muted_me: true,
                ..base.clone()
            },
            bracket_only.clone(),
        ),
        (
            "the room locked two days after the event",
            Tourney {
                chat_locked: true,
                ..base.clone()
            },
            bracket_only.clone(),
        ),
        (
            "one announcement read, one still new",
            Tourney {
                viewer: TourneyViewer {
                    news_read_at: Some(1_786_100_000),
                    ..base.viewer.clone()
                },
                ..base.clone()
            },
            bracket_only.clone(),
        ),
        (
            // Nothing is remembered for a signed-out reader, so a badge would
            // never clear.
            "signed out, where nothing is remembered",
            Tourney {
                viewer: TourneyViewer {
                    logged_in: false,
                    organiser: false,
                    ..TourneyViewer::default()
                },
                ..base.clone()
            },
            bracket_only,
        ),
    ]
    .into_iter()
    .map(|(name, event, format)| TourneyLifecycleCase {
        name: name.to_string(),
        may_edit_format: event.may_edit_format(),
        may_edit_team_setup: event.may_edit_team_setup(),
        may_post_chat: event.may_post_chat(),
        unread_news: event.unread_news(),
        structural: format.is_structural(&event),
        format,
        event: Box::new(event),
    })
    .collect()
}

fn tourney_series(id: &str, name: &str, editions: i32, active: i32) -> TourneySeries {
    TourneySeries {
        id: id.into(),
        name: name.into(),
        description: "A monthly cup.".into(),
        colour: SeriesColour::Blue,
        category: Some(TourneyCategory::Official),
        editions,
        active,
        last_at: Some(1_786_212_000),
        latest_id: Some("e1".into()),
        latest_name: "Spring Cup".into(),
        latest_date: Some(1_786_212_000),
    }
}

fn tourney_series_detail(id: &str, name: &str) -> SeriesDetail {
    SeriesDetail {
        id: id.into(),
        name: name.into(),
        description: "A monthly cup.".into(),
        colour: SeriesColour::Blue,
        category: Some(TourneyCategory::Official),
        editions: vec![SeriesEdition {
            id: "e1".into(),
            name: "Spring Cup".into(),
            status: TourneyStatus::Finished,
            category: Some(TourneyCategory::Official),
            published: true,
            competition: Competition::Team,
            bracket_kind: BracketKind::Single,
            team_size: 1,
            player_count: 4,
            team_count: 4,
            event_date: Some(1_786_212_000),
            abandoned: false,
            champion_team_id: Some("t1".into()),
            champion: "Ada".into(),
            can_manage: true,
            signup_opens_at: None,
            event_days: Vec::new(),
        }],
        can_edit: true,
        bans: Vec::new(),
    }
}

fn tourney_qualifier_cases() -> Vec<TourneyQualifierCase> {
    let parent = Tourney {
        id: "parent".into(),
        name: "Grand Final".into(),
        qualifiers: vec![Qualifier {
            id: "link1".into(),
            tournament_id: "linked".into(),
            name: "Already In".into(),
            status: Some(TourneyStatus::Finished),
            rule: QualifierRule::default(),
            ..Qualifier::default()
        }],
        ..Tourney::default()
    };
    let elimination = Tourney {
        id: "child".into(),
        name: "Qualifier One".into(),
        competition: Competition::Team,
        bracket_kind: BracketKind::Double,
        ..Tourney::default()
    };
    let swiss = Tourney {
        id: "swiss".into(),
        name: "Swiss Open".into(),
        bracket_kind: BracketKind::Swiss,
        ..elimination.clone()
    };
    let ffa = Tourney {
        id: "ffa".into(),
        name: "Free For All".into(),
        competition: Competition::FreeForAll,
        ..elimination.clone()
    };
    let top = |n: i32| QualifierRule {
        kind: QualifierKind::Top,
        n,
    };
    let points = |n: i32| QualifierRule {
        kind: QualifierKind::Points,
        n,
    };

    [
        (
            "the top four of an elimination bracket",
            elimination.clone(),
            top(4),
        ),
        (
            "a tournament linked into itself",
            Tourney {
                id: "parent".into(),
                ..elimination.clone()
            },
            top(4),
        ),
        (
            "a child that is already linked",
            Tourney {
                id: "linked".into(),
                ..elimination.clone()
            },
            top(1),
        ),
        ("a cutoff of zero", elimination.clone(), top(0)),
        (
            "points against an elimination bracket, which scores nothing",
            elimination.clone(),
            points(3),
        ),
        ("points against a Swiss field", swiss, points(3)),
        ("points against a free-for-all", ffa, points(3)),
    ]
    .into_iter()
    .map(|(name, candidate, rule)| TourneyQualifierCase {
        name: name.to_string(),
        rejection: parent.qualifier_rejection(&candidate, rule),
        event: Box::new(parent.clone()),
        candidate: Box::new(candidate),
        rule,
    })
    .collect()
}

fn veto_event(
    name: &str,
    organiser: bool,
    my_player: Option<&str>,
    veto: Option<MatchVeto>,
    status: MatchStatus,
) -> TourneyVetoCase {
    let entry = TourneyMatch {
        status,
        veto,
        ..tourney_match("m1", BracketSide::Winners, Some("t1"), Some("t2"))
    };
    let event = Tourney {
        id: "veto".into(),
        status: TourneyStatus::Running,
        veto: VetoConfig {
            enabled: true,
            mode: VetoMode::Upfront,
            ..VetoConfig::default()
        },
        players: vec![
            tourney_player("cap1", Some(1_500), Some("t1"), false),
            tourney_player("cap2", Some(1_500), Some("t2"), false),
        ],
        teams: vec![
            TourneyTeam {
                captain_id: Some("cap1".into()),
                ..tourney_team("t1", &["cap1"])
            },
            TourneyTeam {
                captain_id: Some("cap2".into()),
                ..tourney_team("t2", &["cap2"])
            },
        ],
        matches: vec![entry],
        viewer: TourneyViewer {
            logged_in: true,
            organiser,
            signed_up_player_id: my_player.map(Into::into),
            ..TourneyViewer::default()
        },
        ..Tourney::default()
    };
    let entry = &event.matches[0];
    TourneyVetoCase {
        name: name.to_string(),
        turn_team_id: entry
            .veto
            .as_ref()
            .and_then(|veto| veto.current_turn())
            .map(|turn| turn.team_id),
        may_veto: event.may_veto(entry),
        may_set_sides: event.may_set_veto_sides(entry),
        event: Box::new(event),
    }
}

fn running_veto(step_index: i32, team_a: Option<&str>, done: bool) -> MatchVeto {
    MatchVeto {
        remaining: vec!["map1".into(), "map2".into(), "map3".into(), "map4".into()],
        banned: Vec::new(),
        picks: Vec::new(),
        sequence: vec![
            pool_step(PoolAction::Ban, PoolSide::A),
            pool_step(PoolAction::Pick, PoolSide::B),
            pool_step(PoolAction::Pick, PoolSide::A),
        ],
        step_index,
        team_a: team_a.map(Into::into),
        team_b: team_a.map(|a| if a == "t1" { "t2".into() } else { "t1".into() }),
        done,
        decider: None,
    }
}

fn tourney_veto_cases() -> Vec<TourneyVetoCase> {
    vec![
        // The captain of the side that is due. The one case that says yes for
        // somebody who is not an organiser.
        veto_event(
            "the captain whose turn it is",
            false,
            Some("cap1"),
            Some(running_veto(0, Some("t1"), false)),
            MatchStatus::Ready,
        ),
        // The other captain, on the same run: it is not their step.
        veto_event(
            "the captain of the other side",
            false,
            Some("cap2"),
            Some(running_veto(0, Some("t1"), false)),
            MatchStatus::Ready,
        ),
        // An organiser may act for either side, which is how a run gets unstuck.
        veto_event(
            "an organiser, who may act for either side",
            true,
            None,
            Some(running_veto(0, Some("t1"), false)),
            MatchStatus::Ready,
        ),
        // Second step, so the other side is due and the first captain is not.
        veto_event(
            "one step in, where the other side is due",
            false,
            Some("cap1"),
            Some(running_veto(1, Some("t1"), false)),
            MatchStatus::Ready,
        ),
        // No sides yet: nobody is due, and the organiser is offered the choice.
        veto_event(
            "a run with no sides chosen yet",
            true,
            None,
            Some(running_veto(0, None, false)),
            MatchStatus::Ready,
        ),
        // Finished: nothing is due, and the sides are settled.
        veto_event(
            "a finished run",
            true,
            None,
            Some(running_veto(3, Some("t1"), true)),
            MatchStatus::Ready,
        ),
        // Walked off the end without being marked done, which the service can
        // leave behind after an undo. Nobody is due.
        veto_event(
            "a run walked past its last step",
            true,
            None,
            Some(running_veto(9, Some("t1"), false)),
            MatchStatus::Ready,
        ),
        // A played match keeps its run on screen but closes it.
        veto_event(
            "a match that already has a result",
            true,
            None,
            Some(running_veto(0, Some("t1"), false)),
            MatchStatus::Done,
        ),
        // No run at all, which is every match of an event without vetoes.
        veto_event("a match with no run", true, None, None, MatchStatus::Ready),
    ]
}

fn ffa_lobby(id: &str, index: i32, entrants: &[&str], is_final: bool) -> TourneyMatch {
    TourneyMatch {
        index,
        is_final,
        entrants: entrants.iter().map(|id| (*id).to_string()).collect(),
        ..tourney_match(id, BracketSide::FreeForAll, None, None)
    }
}

fn ffa_case(
    name: &str,
    mode: FfaMode,
    advance: i32,
    lobbies: Vec<TourneyMatch>,
    report: FfaReport,
) -> TourneyFfaCase {
    let event = Tourney {
        id: "ffa".into(),
        status: TourneyStatus::Running,
        competition: Competition::FreeForAll,
        team_size: 1,
        ffa: Some(FfaConfig {
            per_match: 3,
            advance,
            mode,
            rounds: 3,
            cut_to: 0,
            final_size: 0,
        }),
        teams: (1..=6)
            .map(|index| TourneyTeam {
                seed: index,
                ..tourney_team(&format!("t{index}"), &[])
            })
            .collect(),
        matches: lobbies,
        viewer: TourneyViewer {
            logged_in: true,
            organiser: true,
            ..TourneyViewer::default()
        },
        ..Tourney::default()
    };
    let entry = &event.matches[0];
    TourneyFfaCase {
        name: name.to_string(),
        winners_needed: event.ffa_winners_needed(entry),
        scored: event.ffa_is_scored(entry),
        may_report: event.may_report_ffa(entry),
        submittable: report.is_submittable(
            entry,
            event.ffa_is_scored(entry),
            event.ffa_winners_needed(entry),
        ),
        report,
        event: Box::new(event),
    }
}

fn ffa_points(pairs: &[(&str, i32)]) -> FfaReport {
    FfaReport {
        match_id: "f1".into(),
        winners: Vec::new(),
        points: pairs
            .iter()
            .map(|(id, points)| TeamPoints {
                team_id: (*id).to_string(),
                points: *points,
            })
            .collect(),
    }
}

fn ffa_winners(ids: &[&str]) -> FfaReport {
    FfaReport {
        match_id: "f1".into(),
        winners: ids.iter().map(|id| (*id).to_string()).collect(),
        points: Vec::new(),
    }
}

fn tourney_ffa_cases() -> Vec<TourneyFfaCase> {
    let two_lobbies = || {
        vec![
            ffa_lobby("f1", 0, &["t1", "t2", "t3"], false),
            ffa_lobby("f2", 1, &["t4", "t5", "t6"], false),
        ]
    };
    let one_lobby = || vec![ffa_lobby("f1", 0, &["t1", "t2", "t3"], false)];

    vec![
        ffa_case(
            "a scored lobby with a number for everyone",
            FfaMode::Points,
            1,
            two_lobbies(),
            ffa_points(&[("t1", 5), ("t2", 3), ("t3", 0)]),
        ),
        ffa_case(
            "a scored lobby missing an entrant",
            FfaMode::Points,
            1,
            two_lobbies(),
            ffa_points(&[("t1", 5), ("t2", 3)]),
        ),
        ffa_case(
            "a score above the range the service accepts",
            FfaMode::Points,
            1,
            two_lobbies(),
            ffa_points(&[("t1", 5), ("t2", 3), ("t3", 1_001)]),
        ),
        ffa_case(
            "an elimination lobby advancing exactly one",
            FfaMode::Elimination,
            1,
            two_lobbies(),
            ffa_winners(&["t1"]),
        ),
        ffa_case(
            "an elimination lobby advancing one too many",
            FfaMode::Elimination,
            1,
            two_lobbies(),
            ffa_winners(&["t1", "t2"]),
        ),
        ffa_case(
            "a winner who is not in the lobby",
            FfaMode::Elimination,
            1,
            two_lobbies(),
            ffa_winners(&["t9"]),
        ),
        ffa_case(
            "the same winner named twice",
            FfaMode::Elimination,
            2,
            two_lobbies(),
            ffa_winners(&["t1", "t1"]),
        ),
        ffa_case(
            "advance set higher than the lobby can give",
            FfaMode::Elimination,
            9,
            two_lobbies(),
            ffa_winners(&["t1", "t2"]),
        ),
        ffa_case(
            "the last lobby of a round, which is the final",
            FfaMode::Elimination,
            2,
            one_lobby(),
            ffa_winners(&["t1"]),
        ),
        // The last lobby of a round is a final for the *winner count*, but not
        // for scoring: the service keys that off `isFinal` alone, so a points
        // event still wants points here. The two rules read alike and are not.
        ffa_case(
            "the last lobby of a points round, which is still scored",
            FfaMode::Points,
            1,
            one_lobby(),
            ffa_winners(&["t1"]),
        ),
        ffa_case(
            "a points event's flagged final, decided by a winner",
            FfaMode::Points,
            1,
            vec![ffa_lobby("f1", 0, &["t1", "t2", "t3"], true)],
            ffa_winners(&["t1"]),
        ),
    ]
}

fn draft_case(
    name: &str,
    status: TourneyStatus,
    organiser: bool,
    my_player: Option<&str>,
    current: i32,
    last_pick: Option<(&str, &str, i32)>,
) -> TourneyDraftCase {
    let event = Tourney {
        id: "draft".into(),
        status,
        formation: Formation::Draft,
        team_size: 2,
        players: vec![
            tourney_player("cap1", Some(1_500), Some("d1"), false),
            tourney_player("cap2", Some(1_500), Some("d2"), false),
            tourney_player("free1", Some(1_400), None, false),
            tourney_player("free2", Some(1_300), None, false),
            // Waiting on the organiser, so not in the pool: the service refuses
            // a pick naming them.
            tourney_player("pending", Some(1_200), None, true),
        ],
        teams: vec![
            TourneyTeam {
                captain_id: Some("cap1".into()),
                ..tourney_team("d1", &["cap1"])
            },
            TourneyTeam {
                captain_id: Some("cap2".into()),
                ..tourney_team("d2", &["cap2"])
            },
        ],
        draft: Some(Draft {
            order: vec!["d1".into(), "d2".into()],
            current,
            last_pick: last_pick.map(|(player_id, team_id, at_index)| DraftPick {
                player_id: player_id.into(),
                team_id: team_id.into(),
                at_index,
            }),
        }),
        viewer: TourneyViewer {
            logged_in: true,
            organiser,
            signed_up_player_id: my_player.map(Into::into),
            ..TourneyViewer::default()
        },
        ..Tourney::default()
    };
    TourneyDraftCase {
        name: name.to_string(),
        turn_team_id: event.draft_turn().map(str::to_string),
        may_pick: event.may_pick(),
        may_undo: event.may_undo_pick(),
        undrafted_ids: event
            .undrafted()
            .into_iter()
            .map(|player| player.id.clone())
            .collect(),
        event: Box::new(event),
    }
}

fn tourney_draft_cases() -> Vec<TourneyDraftCase> {
    vec![
        draft_case(
            "the captain on the clock",
            TourneyStatus::Draft,
            false,
            Some("cap1"),
            0,
            None,
        ),
        draft_case(
            "the other captain, waiting",
            TourneyStatus::Draft,
            false,
            Some("cap2"),
            0,
            None,
        ),
        draft_case(
            "an organiser, who may pick for either",
            TourneyStatus::Draft,
            true,
            None,
            0,
            None,
        ),
        draft_case(
            "a captain undoing their own most recent pick",
            TourneyStatus::Draft,
            false,
            Some("cap1"),
            1,
            Some(("free1", "d1", 0)),
        ),
        draft_case(
            "a captain trying to undo after the next pick landed",
            TourneyStatus::Draft,
            false,
            Some("cap1"),
            2,
            Some(("free1", "d1", 0)),
        ),
        draft_case(
            "an organiser undoing a pick nobody else could",
            TourneyStatus::Draft,
            true,
            None,
            2,
            Some(("free1", "d1", 0)),
        ),
        draft_case(
            "a captain on the clock, who still may not undo the other side's pick",
            TourneyStatus::Draft,
            false,
            Some("cap2"),
            1,
            Some(("free1", "d1", 0)),
        ),
        draft_case(
            "the order walked out, so nobody is on the clock",
            TourneyStatus::Draft,
            true,
            None,
            2,
            None,
        ),
        draft_case(
            "a finished draft, where undoing is still allowed",
            TourneyStatus::Drafted,
            true,
            None,
            2,
            Some(("free2", "d2", 1)),
        ),
        draft_case(
            "signups, before the draft has started",
            TourneyStatus::Signup,
            true,
            None,
            0,
            None,
        ),
    ]
}

fn tourney_open_event_cases() -> Vec<TourneyOpenEventCase> {
    [
        (Some("e1"), Some("e1")),
        // The window between selecting a row and its detail arriving: the
        // previous event's bracket must not appear under the new name.
        (Some("e1"), Some("e2")),
        (Some("e1"), None),
        (None, Some("e1")),
        (None, None),
    ]
    .into_iter()
    .map(|(detail_id, selected_id)| {
        let state = TourneyState {
            detail: detail_id.map(|id| Tourney {
                id: id.into(),
                ..Tourney::default()
            }),
            selected_id: selected_id.map(Into::into),
            ..TourneyState::default()
        };
        TourneyOpenEventCase {
            detail_id: detail_id.map(Into::into),
            selected_id: selected_id.map(Into::into),
            open_id: state.open_event().map(|event| event.id.clone()),
        }
    })
    .collect()
}

fn tourney_busy_match_cases() -> Vec<TourneyBusyMatchCase> {
    [
        None,
        Some(TourneyAction::AnsweringReport {
            match_id: "m2".into(),
        }),
        // A player's own submission narrows to its match as well.
        Some(TourneyAction::SubmittingReport {
            match_id: "m6".into(),
        }),
        // Both narrow to one match too: a captain taking a veto step, or an
        // organiser scoring a lobby, must not freeze the rest of the draw.
        Some(TourneyAction::Vetoing {
            match_id: "m4".into(),
        }),
        Some(TourneyAction::ReportingFfa {
            match_id: "m5".into(),
        }),
        // Event-wide, despite belonging to the draft: a pick changes the teams,
        // and there is no one match to attach it to.
        Some(TourneyAction::Drafting),
        Some(TourneyAction::DecidingReport {
            match_id: "m3".into(),
        }),
        // Event-wide writes: the whole pane disables, no single match does.
        Some(TourneyAction::SigningUp),
        Some(TourneyAction::Reseeding),
        // Names an id, but not a match's: must not be mistaken for one.
        Some(TourneyAction::PostingChat {
            room_id: "m1".into(),
        }),
        Some(TourneyAction::AnsweringTeam {
            team_id: "m1".into(),
        }),
    ]
    .into_iter()
    .map(|pending| {
        let state = TourneyState {
            pending,
            ..TourneyState::default()
        };
        TourneyBusyMatchCase {
            busy_match_id: state.busy_match_id().map(Into::into),
            pending: state.pending,
        }
    })
    .collect()
}

fn tourney_phase_legality_cases() -> Vec<TourneyPhaseLegalityCase> {
    let phases = [
        TourneyPhase::FormTeams,
        TourneyPhase::StartBracket,
        TourneyPhase::ReopenSignups,
    ];
    let statuses = [
        TourneyStatus::Draft,
        TourneyStatus::Signup,
        TourneyStatus::Drafted,
        TourneyStatus::Running,
        TourneyStatus::Finished,
        TourneyStatus::Unknown,
    ];
    phases
        .into_iter()
        .flat_map(|phase| {
            statuses
                .into_iter()
                .map(move |status| TourneyPhaseLegalityCase {
                    phase,
                    status,
                    legal: phase.is_legal_from(status),
                })
        })
        .collect()
}

fn tourney_draft_rejection_cases() -> Vec<TourneyDraftRejectionCase> {
    let named = |over: TourneyDraft| TourneyDraft {
        name: "Weekend Cup".into(),
        ..over
    };
    let cases: Vec<(&str, TourneyDraft)> = vec![
        ("a plain valid draft", named(TourneyDraft::new())),
        (
            "no name at all, which is the server's first complaint",
            TourneyDraft::new(),
        ),
        (
            "a name of nothing but spaces still counts as none",
            TourneyDraft {
                name: "   ".into(),
                ..TourneyDraft::new()
            },
        ),
        (
            "a team of seven",
            named(TourneyDraft {
                team_size: 7,
                ..TourneyDraft::new()
            }),
        ),
        (
            "a team of none",
            named(TourneyDraft {
                team_size: 0,
                ..TourneyDraft::new()
            }),
        ),
        (
            "a rating floor above the ceiling",
            named(TourneyDraft {
                rating: RatingGate {
                    min: Some(1_800),
                    max: Some(1_200),
                    ..RatingGate::default()
                },
                ..TourneyDraft::new()
            }),
        ),
        (
            "a gate on an unrated event, which could only refuse everybody",
            named(TourneyDraft {
                rating_kind: RatingKind::None,
                rating: RatingGate {
                    min: Some(1_200),
                    ..RatingGate::default()
                },
                ..TourneyDraft::new()
            }),
        ),
        (
            "an unrated event with no gate is fine",
            named(TourneyDraft {
                rating_kind: RatingKind::None,
                ..TourneyDraft::new()
            }),
        ),
        (
            "signups closing before they open",
            named(TourneyDraft {
                signup_opens_at: Some(1_800_000_000),
                signup_closes_at: Some(1_700_000_000),
                ..TourneyDraft::new()
            }),
        ),
        (
            "signups opening and closing at the same instant",
            named(TourneyDraft {
                signup_opens_at: Some(1_700_000_000),
                signup_closes_at: Some(1_700_000_000),
                ..TourneyDraft::new()
            }),
        ),
        (
            // Two problems at once: the order decides which is shown, and it has
            // to be the server's order.
            "a nameless draft that also has an inverted rating range",
            TourneyDraft {
                rating: RatingGate {
                    min: Some(1_800),
                    max: Some(1_200),
                    ..RatingGate::default()
                },
                ..TourneyDraft::new()
            },
        ),
    ];
    cases
        .into_iter()
        .map(|(name, draft)| TourneyDraftRejectionCase {
            name: name.to_string(),
            rejection: draft.rejection(),
            submittable: draft.is_submittable(),
            draft: Box::new(draft),
        })
        .collect()
}

fn tourney_report_cases() -> Vec<TourneyReportCase> {
    let entry =
        |best_of: i32, handicap: i32, score1: Option<i32>, score2: Option<i32>| TourneyMatch {
            id: "m1".into(),
            bracket: BracketSide::Winners,
            round: 1,
            index: 0,
            best_of,
            handicap,
            division: 0,
            team1: Some("t1".into()),
            team2: Some("t2".into()),
            score1,
            score2,
            status: MatchStatus::Ready,
            winner: None,
            loser: None,
            winner_to: None,
            loser_to: None,
            pending_report: None,
            veto: None,
            faction_veto: None,
            entrants: Vec::new(),
            winners: Vec::new(),
            points: Vec::new(),
            is_final: false,
            replay_ids: Vec::new(),
            draw_replay_ids: Vec::new(),
            forfeit: None,
        };
    let ids = |count: usize| -> Vec<String> {
        (0..count).map(|index| format!("replay-{index}")).collect()
    };

    let cases: Vec<(&str, TourneyMatch, i32, i32, Vec<String>)> = vec![
        (
            "a fresh Bo3 taken 2-0",
            entry(3, 0, None, None),
            2,
            0,
            ids(2),
        ),
        (
            "the same, one replay id short",
            entry(3, 0, None, None),
            2,
            0,
            ids(1),
        ),
        ("one id too many", entry(3, 0, None, None), 2, 0, ids(3)),
        (
            // Blank fields are not ids, which is what the trimming is for.
            "two boxes filled, one of them blank",
            entry(3, 0, None, None),
            2,
            0,
            vec!["replay-0".into(), "   ".into()],
        ),
        (
            "a Bo3 at 1-1 reported as 2-1: one new game",
            entry(3, 0, Some(1), Some(1)),
            2,
            1,
            ids(1),
        ),
        (
            "re-reporting a score already confirmed adds nothing",
            entry(3, 0, Some(2), Some(0)),
            2,
            0,
            Vec::new(),
        ),
        (
            "scoring past the series length",
            entry(3, 0, None, None),
            3,
            0,
            ids(3),
        ),
        (
            "both sides winning it",
            entry(3, 0, None, None),
            2,
            2,
            ids(4),
        ),
        ("a negative score", entry(3, 0, None, None), -1, 2, ids(1)),
        (
            // The handicap case: an absent score is not zero here, so a naive
            // twin counts one game too many.
            "a handicapped grand final starting the favourite at 1-0",
            entry(5, 1, None, None),
            3,
            1,
            ids(3),
        ),
        (
            "the same handicapped final, counted as if from 0-0",
            entry(5, 1, None, None),
            3,
            1,
            ids(4),
        ),
        (
            "an even Bo2, where one win is not yet enough",
            entry(2, 0, None, None),
            1,
            0,
            ids(1),
        ),
        (
            // An organiser's correction, and a player's refusal: the player
            // path only counts up.
            "a Bo3 at 1-1 set back to 1-0",
            entry(3, 0, Some(1), Some(1)),
            1,
            0,
            Vec::new(),
        ),
        (
            "an id with no digit in it, which the server drops",
            entry(3, 0, None, None),
            1,
            0,
            vec!["abc".into()],
        ),
    ];

    cases
        .into_iter()
        .map(|(name, held, score1, score2, replay_ids)| {
            let report = MatchReport {
                match_id: held.id.clone(),
                score1,
                score2,
                replay_ids: replay_ids.clone(),
                draw_replay_ids: Vec::new(),
                winner: None,
                forfeit: None,
            };
            TourneyReportCase {
                name: name.to_string(),
                entry: TourneyReportEntry {
                    best_of: held.best_of,
                    handicap: held.handicap,
                    score1: held.score1,
                    score2: held.score2,
                },
                new_games: report.new_games(&held),
                submittable: report.is_submittable(&held),
                player_submittable: report.is_player_submittable(&held),
                score1,
                score2,
                replay_ids,
            }
        })
        .collect()
}

fn tourney_map_match_fixture() -> TourneyMapMatchFixture {
    let vault = vec![
        TourneyMapVaultEntry {
            display_name: "Seton's Clutch".into(),
            folder_name: "scmp_009.v0002".into(),
        },
        TourneyMapVaultEntry {
            display_name: "Open Palms".into(),
            folder_name: "scmp_001".into(),
        },
        TourneyMapVaultEntry {
            display_name: "Adaptive Tabula".into(),
            folder_name: "adaptive_tabula.v0006".into(),
        },
    ];
    let typed = [
        // The four spellings the same map arrives as.
        "Seton's Clutch",
        "setons clutch",
        "SCMP_009",
        "scmp_009.v0001",
        // Punctuation and spacing are noise; a version on both sides comes off.
        "  adaptive-tabula  ",
        "Adaptive Tabula.v0009",
        // Nothing to compare on, and a map that was never uploaded.
        "",
        "!!!",
        "Some Private Map",
    ];
    let cases = typed
        .into_iter()
        .map(|name| {
            let tourney_map = TourneyMap {
                id: "m".into(),
                name: name.into(),
                image_url: String::new(),
                description: String::new(),
                published: true,
                spec: None,
                secret: false,
                masked: false,
            };
            TourneyMapMatchCase {
                typed: name.into(),
                key: map_key(name),
                resolved_display_name: match_vault_map(
                    &tourney_map,
                    &vault,
                    |entry| &entry.display_name,
                    |entry| &entry.folder_name,
                )
                .map(|entry| entry.display_name.clone()),
            }
        })
        .collect();
    TourneyMapMatchFixture { vault, cases }
}

fn helper_fixture() -> HelperFixture {
    let review_sets = vec![
        Vec::new(),
        vec![review(1, 5, "Ada"), review(2, 2, "Bob")],
        vec![
            review(3, 1, "Ada"),
            review(4, 2, "Bob"),
            review(5, 2, "Cid"),
        ],
        vec![review(6, -1, "Ada"), review(7, 9, "Bob")],
    ];
    let statuses = vec![
        UploadStatus::Idle,
        UploadStatus::Compressing {
            done_bytes: 3,
            total_bytes: 10,
        },
        UploadStatus::Uploading {
            sent_bytes: 5,
            total_bytes: 10,
        },
        UploadStatus::Finishing,
        UploadStatus::Succeeded,
        UploadStatus::Failed {
            reason: "rejected".into(),
        },
    ];
    let note_lookups = vec![
        (
            vec![
                PlayerNote {
                    player_id: 42,
                    login: "Aurora".into(),
                    note: "Reliable teammate".into(),
                },
                PlayerNote {
                    player_id: 7,
                    login: "Ada".into(),
                    note: "Map specialist".into(),
                },
            ],
            7,
        ),
        (Vec::new(), 99),
    ];
    let galactic_war_states = vec![
        // Nothing known at all.
        galactic_war_state(None, None, GalacticWarStatus::Idle, false),
        // Never installed, the gateway has answered: a first install.
        galactic_war_state(
            None,
            Some(("v2026.04.04.1", None)),
            GalacticWarStatus::Idle,
            false,
        ),
        // Current, and startable.
        galactic_war_state(
            Some("v2026.04.04.1"),
            Some(("v2026.03.01.1", Some("v2026.04.04.1"))),
            GalacticWarStatus::Idle,
            false,
        ),
        // The gateway's pointer moved.
        galactic_war_state(
            Some("v2026.03.01.1"),
            Some(("v2026.03.01.1", Some("v2026.04.04.1"))),
            GalacticWarStatus::Idle,
            false,
        ),
        // Below the minimum: startable only after an update.
        galactic_war_state(
            Some("v2026.03.01.1"),
            Some(("v2026.04.04.1", None)),
            GalacticWarStatus::Idle,
            true,
        ),
        // A scheme neither side can order: the pointer still moved.
        galactic_war_state(
            Some("build-41"),
            Some(("build-42", None)),
            GalacticWarStatus::Idle,
            false,
        ),
        // In flight, and already running: both refuse a launch.
        galactic_war_state(
            Some("v1"),
            Some(("v1", None)),
            GalacticWarStatus::Downloading {
                version: "v1".into(),
                downloaded_bytes: 1,
                total_bytes: 2,
            },
            false,
        ),
        galactic_war_state(
            Some("v1"),
            Some(("v1", None)),
            GalacticWarStatus::Running,
            false,
        ),
        // A failed run must not lock the panel.
        galactic_war_state(
            Some("v1"),
            Some(("v1", None)),
            GalacticWarStatus::Failed {
                reason: "network".into(),
            },
            false,
        ),
    ];

    HelperFixture {
        review_summaries: review_sets
            .into_iter()
            .map(|reviews| ReviewSummaryCase {
                expected: summarize(&reviews),
                reviews,
            })
            .collect(),
        upload_busy: statuses
            .into_iter()
            .map(|status| UploadBusyCase {
                expected: status.is_busy(),
                status,
            })
            .collect(),
        player_note_lookups: note_lookups
            .into_iter()
            .map(|(notes, player_id)| PlayerNoteLookupCase {
                expected: SocialPreferences {
                    player_notes: notes.clone(),
                    replay_notes: Vec::new(),
                }
                .note_for(player_id)
                .map_or_else(String::new, |entry| entry.note.clone()),
                notes,
                player_id,
            })
            .collect(),
        galactic_war_actions: galactic_war_states
            .into_iter()
            .map(|state| GalacticWarActionCase {
                install_target: state.install_target().unwrap_or_default().to_string(),
                update_available: state.update_available(),
                can_launch: state.can_launch(),
                state,
            })
            .collect(),
        tourney_rules: tourney_rule_cases(),
        tourney_open_events: tourney_open_event_cases(),
        tourney_phase_legality: tourney_phase_legality_cases(),
        tourney_busy_matches: tourney_busy_match_cases(),
        tourney_faction_configs: [
            (true, 1, 2),
            (true, 2, 3),
            (true, 1, 1),
            (true, 2, 2),
            (true, 1, 4),
            (true, 3, 3),
            (false, 1, 1),
            (false, 0, 2),
        ]
        .into_iter()
        .map(|(enabled, bans, picks)| {
            let config = FactionVetoConfig {
                enabled,
                bans,
                picks,
            };
            TourneyFactionConfigCase {
                config,
                submittable: config.is_submittable(),
            }
        })
        .collect(),
        tourney_swiss_cuts: [(0, 0), (3, 0), (0, 2), (3, 3), (4, 2), (1, 1), (-1, 3)]
            .into_iter()
            .map(|(wins, losses)| {
                let cuts = SwissCuts { wins, losses };
                TourneySwissCutCase {
                    cuts,
                    rounds: cuts.rounds(),
                }
            })
            .collect(),
        tourney_draft_rejections: tourney_draft_rejection_cases(),
        tourney_reports: tourney_report_cases(),
        tourney_map_matches: tourney_map_match_fixture(),
        tourney_standings: tourney_standings_cases(),
        tourney_profiles: tourney_profile_cases(),
        tourney_pool_drafts: tourney_pool_draft_cases(),
        tourney_vetoes: tourney_veto_cases(),
        tourney_ffa: tourney_ffa_cases(),
        tourney_drafts: tourney_draft_cases(),
        tourney_qualifiers: tourney_qualifier_cases(),
        tourney_lifecycles: tourney_lifecycle_cases(),
        tourney_rounds: tourney_round_cases(),
        tourney_chat_rooms: tourney_chat_room_cases(),
        tourney_bracket_configs: tourney_bracket_config_cases(),
        training_filters: training_filter_fixture(),
        training_form_problems: training_form_problem_fixture(),
        tourney_match_plans: [BracketKind::Single, BracketKind::Double, BracketKind::Swiss]
            .into_iter()
            .map(|kind| TourneyMatchPlanCase {
                kind,
                expected: MatchPlan::default_for(kind),
            })
            .collect(),
    }
}

/// A vault map with just enough shape to be identified by version id.
fn conformance_vault_map(map_id: i32, version_id: i32, folder_name: &str) -> VaultMap {
    VaultMap {
        map_id,
        version_id,
        display_name: folder_name.into(),
        author: Some("Rackover".into()),
        author_id: Some(4711),
        folder_name: folder_name.into(),
        version: "1".into(),
        description: String::new(),
        map_type: "skirmish".into(),
        max_players: 8,
        width: 1024,
        height: 1024,
        games_played: 0,
        version_games_played: 0,
        ranked: false,
        hidden: false,
        recommended: false,
        rating_tenths: 0,
        reviews: 0,
        created_at: "2026-01-01T00:00:00Z".into(),
        download_url: String::new(),
        thumbnail_url: String::new(),
        thumbnail_url_large: String::new(),
    }
}

fn case(name: &str, events: Vec<AppEvent>) -> Case {
    let mut state = AppState::default();
    let steps = events
        .into_iter()
        .map(|event| {
            reduce(&mut state, &event);
            let slice = event_slice(&event);
            let expected = serde_json::to_value(&state)
                .expect("state must serialise")
                .get(&slice)
                .unwrap_or_else(|| panic!("event slice `{slice}` is absent from AppState"))
                .clone();
            Step { event, expected }
        })
        .collect();
    Case {
        name: name.to_string(),
        steps,
    }
}

fn event_slice(event: &AppEvent) -> String {
    let event = serde_json::to_value(event).expect("event must serialise");
    let kind = event["kind"].as_str().expect("every event is tagged");
    let mut chars = kind.chars();
    match chars.next() {
        Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

fn player() -> Player {
    Player::new(7, "Ada")
}

fn chat_message(sender: &str, content: &str) -> ChatMessage {
    ChatMessage {
        id: format!("m-{sender}-{content}"),
        sender: sender.into(),
        content: content.into(),
        timestamp: "2026-01-01T00:00:00Z".into(),
        kind: ChatMessageKind::Message,
        msgid: format!("srv-{sender}-{content}"),
        reply_to: String::new(),
    }
}

fn local_replay(uid: i32) -> LocalReplay {
    LocalReplay {
        path: format!("/replays/{uid}.fafreplay"),
        file_name: format!("{uid}.fafreplay"),
        uid: Some(uid),
        map: "scmp_009".into(),
        mod_name: "faf".into(),
        title: "Downloaded replay".into(),
        recorder: "Host".into(),
        start_time: None,
        duration_seconds: Some(1_337),
        modified_time: 1,
        file_size_bytes: 100,
        num_players: 2,
        teams: Vec::new(),
        average_rating: None,
        sim_mods: Vec::new(),
        status: LocalReplayStatus::Complete,
        watchable: true,
        game_version: None,
    }
}

/// A vault listing for one game, as the single-id lookup returns it.
fn vault_replay(uid: i32) -> VaultReplay {
    VaultReplay {
        uid,
        title: "Looked-up game".into(),
        map: "scmp_009".into(),
        map_thumbnail_url: String::new(),
        mod_name: "faf".into(),
        start_time: "2026-01-01T00:00:00Z".into(),
        end_time: "2026-01-01T00:30:00Z".into(),
        replay_available: true,
        duration_seconds: None,
        game_duration_seconds: None,
        teams: Vec::new(),
        average_rating: None,
        quality: None,
        reviews_average: None,
        reviews_count: None,
        game_version: None,
        validity: "VALID".into(),
        victory_condition: "DEMORALIZATION".into(),
    }
}

/// One live broadcast, for the streams cases.
fn live_stream(id: &str) -> LiveStream {
    LiveStream {
        id: id.into(),
        platform: StreamPlatform::Twitch,
        channel: "faflive".into(),
        display_name: "FAFLive".into(),
        title: "Setons Summer Slam: Grand Final".into(),
        url: "https://www.twitch.tv/faflive".into(),
        viewers: Some(412),
        started_at: "2026-09-11T18:02:00Z".into(),
    }
}

fn cases() -> Vec<Case> {
    let mut cases = vec![
        // ── streams: FAF's own channels going live ──────────────────────
        case(
            "background catalogue progress, cancellation and retry",
            vec![
                MapsEvent::VaultLoading.into(),
                MapsEvent::VaultProgress { progress: maps::CatalogueProgress { pages: 2, total_pages: Some(5) } }.into(),
                MapsEvent::VaultCancelled.into(),
                MapsEvent::VaultLoading.into(),
                MapsEvent::VaultLoaded { maps: vec![] }.into(),
                ModsEvent::VaultLoading.into(),
                ModsEvent::VaultProgress { progress: maps::CatalogueProgress { pages: 1, total_pages: None } }.into(),
                ModsEvent::VaultCancelled.into(),
                ModsEvent::VaultLoading.into(),
                ModsEvent::VaultLoaded { mods: vec![] }.into(),
                TutorialsEvent::LaunchPreparing { tutorial_id: 7, detail: "Preparing".into() }.into(),
                TutorialsEvent::LaunchCancelled.into(),
            ],
        ),
        case(
            "a channel goes live, is announced once, and goes off air",
            vec![
                StreamsEvent::Checking.into(),
                StreamsEvent::Loaded {
                    streams: vec![live_stream("twitch:1")],
                }
                .into(),
                StreamsEvent::Announced {
                    stream_ids: vec!["twitch:1".into()],
                }
                .into(),
                // The same broadcast, seen again five minutes later. Still
                // announced, so nothing more is said about it.
                StreamsEvent::Loaded {
                    streams: vec![live_stream("twitch:1")],
                }
                .into(),
                StreamsEvent::Loaded { streams: vec![] }.into(),
            ],
        ),
        case(
            "a failed check keeps the stream it last saw",
            vec![
                StreamsEvent::Loaded {
                    streams: vec![live_stream("twitch:1")],
                }
                .into(),
                StreamsEvent::LoadFailed {
                    reason: "Twitch responded with 503 Service Unavailable".into(),
                }
                .into(),
            ],
        ),
        // ── player card: per-map record ──────────────────────────────────
        case(
            "map statistics load, and a second player's scan replaces the first",
            vec![
                PlayerCardEvent::MapStatsLoading { player_id: 7 }.into(),
                PlayerCardEvent::MapStatsLoaded {
                    stats: Box::new(PlayerMapStats {
                        total_games: 3,
                        ranked_games: 3,
                        wins: 2,
                        losses: 1,
                        undecided: 0,
                        unranked: 0,
                        unattributed: 0,
                        maps: vec![PlayerMapStat {
                            map: "Setons Clutch".into(),
                            generated: false,
                            games: 3,
                            wins: 2,
                            losses: 1,
                            draws: 0,
                            last_played: "2026-01-04T20:00:00Z".into(),
                        }],
                        truncated: false,
                        player_id: 7,
                        games: vec![PlayerGameResult {
                            game_id: 27798371,
                            played_at: "2026-01-04T20:00:00Z".into(),
                            queue: "global".into(),
                            map: "Setons Clutch".into(),
                            generated: false,
                            outcome: "loss".into(),
                            rating_change_hundredths: Some(-1114),
                        }],
                    }),
                }
                .into(),
                // Opening another profile must not leave the first one's maps
                // sitting under a different name.
                PlayerCardEvent::MapStatsLoading { player_id: 8 }.into(),
                PlayerCardEvent::MapStatsLoadFailed {
                    reason: "offline".into(),
                }
                .into(),
            ],
        ),
        // ── changelog ────────────────────────────────────────────────────
        case(
            "a patch note is selected before it arrives, and earlier ones are kept",
            vec![
                ChangelogEvent::Loading.into(),
                ChangelogEvent::Loaded {
                    releases: vec![faf_domain::protocol::changelog::ChangelogRelease {
                        id: "3837".into(),
                        kind: "Game Patch".into(),
                        date: "2026-08-14".into(),
                        year: "2026".into(),
                        source_url: "https://example.invalid/3837.md".into(),
                        web_url: "https://example.invalid/3837".into(),
                    }],
                }
                .into(),
                ChangelogEvent::EntryLoading { id: "3837".into() }.into(),
                ChangelogEvent::EntryLoaded {
                    entry: faf_domain::protocol::changelog::ChangelogEntry {
                        id: "3837".into(),
                        title: "3837 - Game Patch".into(),
                        blocks: vec![faf_domain::protocol::changelog::ChangelogBlock::Heading {
                            level: 1,
                            text: "Game version 3837".into(),
                        }],
                    },
                }
                .into(),
                ChangelogEvent::EntryLoadFailed {
                    reason: "offline".into(),
                }
                .into(),
            ],
        ),
        // ── events ───────────────────────────────────────────────────────
        case(
            "a calendar is loaded, filtered, and keeps what it has when a reload fails",
            vec![
                EventsEvent::Loading.into(),
                EventsEvent::Loaded {
                    catalogue: EventCatalogue {
                        events: vec![CalendarEvent {
                            id: "cgn-42".into(),
                            title: "Community Game Night 42".into(),
                            summary: "Setons, all welcome.".into(),
                            category: EventCategory::Cgn,
                            origin: EventOrigin::Community,
                            host: "Setons Clutch".into(),
                            starts_at: 1_773_514_800,
                            ends_at: 0,
                            all_day: false,
                            links: vec![EventLink {
                                label: "Discord".into(),
                                url: "https://example.invalid/discord".into(),
                            }],
                            recurrence: Some(Recurrence::Weekly { interval: 1 }),
                            recurs_until: 1_781_000_000,
                        }],
                        source: EventsSource::Remote,
                        submit_url: "https://example.invalid/submit".into(),
                    },
                }
                .into(),
                EventsEvent::ViewChanged {
                    view: CalendarView::Week,
                }
                .into(),
                EventsEvent::AnchorChanged {
                    day: "2026-03-14".into(),
                }
                .into(),
                EventsEvent::QueryChanged {
                    query: EventsQuery {
                        text: "cgn".into(),
                        category: Some(EventCategory::Cgn),
                        origin: None,
                        only_reminders: true,
                    },
                }
                .into(),
                EventsEvent::Selected {
                    occurrence_id: Some("catalogue:cgn-42@2026-03-14".into()),
                }
                .into(),
                // The reload fails: the calendar keeps its entries and says so.
                EventsEvent::Loading.into(),
                EventsEvent::LoadFailed {
                    reason: "offline".into(),
                }
                .into(),
            ],
        ),
        case(
            "a reminder is set, moved, and the duplicate a hand-edited file left is dropped",
            vec![
                SettingsEvent::EventsChanged {
                    preferences: Box::new(EventsPreferences {
                        week_start: WeekStart::Sunday,
                        reminders: vec![
                            EventReminder {
                                occurrence_id: "catalogue:cgn-42@2026-03-14".into(),
                                title: "Community Game Night 42".into(),
                                starts_at: 1_773_514_800,
                                lead_minutes: 60,
                                notified: false,
                            },
                            EventReminder {
                                occurrence_id: "catalogue:cgn-42@2026-03-14".into(),
                                title: "A second copy of the same occurrence".into(),
                                starts_at: 1_773_514_800,
                                lead_minutes: 10,
                                notified: true,
                            },
                        ],
                    }),
                }
                .into(),
            ],
        ),
        // ── session / auth / nav ─────────────────────────────────────────
        case(
            "session connects and reports its version",
            vec![
                SessionEvent::Connecting.into(),
                SessionEvent::BackendReady {
                    version: "1.2.3".into(),
                    offline_auth: false,
                }
                .into(),
            ],
        ),
        case(
            "an offline development build keeps its flavour across a reconnect",
            vec![
                SessionEvent::BackendReady {
                    version: "1.2.3".into(),
                    offline_auth: true,
                }
                .into(),
                SessionEvent::Disconnected.into(),
                SessionEvent::Connecting.into(),
            ],
        ),
        case(
            "auth logs in then out",
            vec![
                AuthEvent::LoginStarted.into(),
                AuthEvent::LoggedIn { player: player() }.into(),
                AuthEvent::LoggedOut.into(),
            ],
        ),
        case(
            "an offline session signs nobody in, and leaving it clears the mode",
            vec![
                AuthEvent::LoginStarted.into(),
                AuthEvent::LoginFailed {
                    message: "the server did not answer".into(),
                }
                .into(),
                AuthEvent::WentOffline.into(),
                AuthEvent::LoggedOut.into(),
            ],
        ),
        case(
            "auth failure keeps the error",
            vec![
                AuthEvent::LoginStarted.into(),
                AuthEvent::LoginFailed {
                    message: "bad state".into(),
                }
                .into(),
            ],
        ),
        case(
            "nav selects a tab",
            vec![
                NavEvent::TabSelected { tab: Tab::Play }.into(),
                NavEvent::TabSelected {
                    tab: Tab::Training,
                }
                .into(),
            ],
        ),
        case(
            "nav remembers each destination's section across tab changes",
            vec![
                NavEvent::TabSelected { tab: Tab::Maps }.into(),
                NavEvent::MapsSectionSelected {
                    section: MapsSection::Installed,
                }
                .into(),
                NavEvent::ModsSectionSelected {
                    section: ModsSection::Installed,
                }
                .into(),
                NavEvent::ReplaysSectionSelected {
                    section: ReplaysSection::Live,
                }
                .into(),
                NavEvent::SettingsSectionSelected {
                    section: SettingsSection::Connectivity,
                }
                .into(),
                NavEvent::TabSelected { tab: Tab::Chat }.into(),
            ],
        ),
        case(
            "install check flips both flags and reports where paths resolve",
            vec![InstallEvent::Checked {
                game_ready: true,
                replay_ready: false,
                game_pending: false,
                // The replay install the updater has yet to download: not
                // ready, and not missing either.
                replay_pending: true,
                resolved: ResolvedPaths {
                    vault_dir: "D:/faf/vault".into(),
                    maps_dir: "D:/faf/vault/maps".into(),
                    mods_dir: "D:/faf/vault/mods".into(),
                    replays_dir: "C:/ProgramData/FAForever/replays".into(),
                    game_prefs_path: "C:/Users/ada/AppData/Local/Gas Powered Games/Supreme Commander Forged Alliance/game.prefs".into(),
                    map_generator_dir: "C:/Users/ada/AppData/Roaming/faf/map_generator".into(),
                    java_path: "java".into(),
                    // Empty is the Windows answer, and these fixtures describe
                    // a Windows install.
                    wine_prefix: String::new(),
                },
            }
            .into()],
        ),
        // ── chat ─────────────────────────────────────────────────────────
        case(
            "chat joins, counts unread, and clears on select",
            vec![
                ChatEvent::Connected {
                    username: "Ada".into(),
                }
                .into(),
                ChatEvent::ChannelJoined {
                    channel: "#aeolus".into(),
                }
                .into(),
                ChatEvent::ChannelJoined {
                    channel: "#uef".into(),
                }
                .into(),
                ChatEvent::MessageReceived {
                    channel: "#uef".into(),
                    message: chat_message("Bob", "hello Ada"),
                }
                .into(),
                ChatEvent::ChannelSelected {
                    channel: "#uef".into(),
                }
                .into(),
            ],
        ),
        case(
            "selecting an unjoined conversation opens it before the join lands",
            vec![
                ChatEvent::Connected {
                    username: "Ada".into(),
                }
                .into(),
                ChatEvent::ChannelJoined {
                    channel: "#aeolus".into(),
                }
                .into(),
                // The order a right-click produces: only `JoinChannel` goes
                // through the chat port, so its confirmation arrives second.
                ChatEvent::ChannelSelected {
                    channel: "Stormlord".into(),
                }
                .into(),
                ChatEvent::ChannelJoined {
                    channel: "Stormlord".into(),
                }
                .into(),
            ],
        ),
        case(
            "chat restores message history when a new message recreates a conversation",
            vec![
                ChatEvent::ChannelJoined {
                    channel: "#uef".into(),
                }
                .into(),
                ChatEvent::MessageReceived {
                    channel: "#uef".into(),
                    message: chat_message("Bob", "remember this"),
                }
                .into(),
                ChatEvent::ChannelLeft {
                    channel: "#uef".into(),
                }
                .into(),
                ChatEvent::MessageReceived {
                    channel: "#uef".into(),
                    message: chat_message("Carol", "new line"),
                }
                .into(),
            ],
        ),
        case(
            "chat roster events for an unknown channel are dropped",
            vec![
                ChatEvent::ChannelJoined {
                    channel: "#uef".into(),
                }
                .into(),
                // The divergence this whole fixture exists for: these must not
                // create a channel.
                ChatEvent::UserLeft {
                    channel: "#gone".into(),
                    user: "Bob".into(),
                }
                .into(),
                ChatEvent::UserElevationChanged {
                    channel: "#gone".into(),
                    user: "Bob".into(),
                    elevation: "@".into(),
                }
                .into(),
            ],
        ),
        case(
            "chat rename updates every roster and our own name",
            vec![
                ChatEvent::Connected {
                    username: "Ada".into(),
                }
                .into(),
                ChatEvent::ChannelJoined {
                    channel: "#uef".into(),
                }
                .into(),
                ChatEvent::UserJoined {
                    channel: "#uef".into(),
                    user: ChatUser {
                        name: "Ada".into(),
                        elevation: String::new(),
                    },
                }
                .into(),
                ChatEvent::UserRenamed {
                    old_name: "Ada".into(),
                    new_name: "Zara".into(),
                }
                .into(),
                ChatEvent::Disconnected.into(),
            ],
        ),
        case(
            "chat normalizes the server's auto-join channel list",
            vec![
                // The lobby sends bare names, repeats in a different case, and
                // may send blanks. All three are handled in `normalize_channels`,
                // which the frontend hand-mirrors.
                ChatEvent::AutoJoinAnnounced {
                    channels: vec![
                        "aeolus".into(),
                        " german ".into(),
                        "#GERMAN".into(),
                        String::new(),
                        "#clan_qai".into(),
                    ],
                }
                .into(),
                // A later announcement replaces the list rather than adding to
                // it: the server sends a complete set each time.
                ChatEvent::AutoJoinAnnounced {
                    channels: vec!["aeolus".into()],
                }
                .into(),
            ],
        ),
        case(
            "chat applies connection metadata and quiet history without unread badges",
            vec![
                ChatEvent::Connecting.into(),
                ChatEvent::Connected {
                    username: "Ada".into(),
                }
                .into(),
                ChatEvent::ChannelJoined {
                    channel: "#uef".into(),
                }
                .into(),
                ChatEvent::TopicChanged {
                    channel: "#uef".into(),
                    topic: "Build orders".into(),
                }
                .into(),
                ChatEvent::UsersUpdated {
                    channel: "#uef".into(),
                    users: vec![ChatUser::new("Ada", "@"), ChatUser::new("Bob", "")],
                }
                .into(),
                ChatEvent::JoinsPartsToggled { enabled: true }.into(),
                ChatEvent::MessageReceivedQuietly {
                    channel: "#uef".into(),
                    message: chat_message("Bob", "restored history"),
                }
                .into(),
            ],
        ),
        case(
            "someone composes, reacts, and their message clears the indicator",
            vec![
                ChatEvent::Connected {
                    username: "Aurora".into(),
                }
                .into(),
                ChatEvent::ChannelJoined {
                    channel: "#aeolus".into(),
                }
                .into(),
                ChatEvent::TypingChanged {
                    channel: "#aeolus".into(),
                    nickname: "Bob".into(),
                    composing: true,
                    at_seconds: 1_000,
                }
                .into(),
                // A refresh extends the same person rather than listing them twice.
                ChatEvent::TypingChanged {
                    channel: "#aeolus".into(),
                    nickname: "Bob".into(),
                    composing: true,
                    at_seconds: 1_003,
                }
                .into(),
                // The message itself is the loudest possible "done".
                ChatEvent::MessageReceived {
                    channel: "#aeolus".into(),
                    message: chat_message("Bob", "hello"),
                }
                .into(),
                ChatEvent::ReactionReceived {
                    channel: "#aeolus".into(),
                    msgid: "srv-Bob-hello".into(),
                    emoji: "\u{1f44d}".into(),
                    sender: "Ada".into(),
                }
                .into(),
                // Same emoji, second person: one entry, two senders.
                ChatEvent::ReactionReceived {
                    channel: "#aeolus".into(),
                    msgid: "srv-Bob-hello".into(),
                    emoji: "\u{1f44d}".into(),
                    sender: "Cid".into(),
                }
                .into(),
                // A repeat from someone already counted is swallowed.
                ChatEvent::ReactionReceived {
                    channel: "#aeolus".into(),
                    msgid: "srv-Bob-hello".into(),
                    emoji: "\u{1f44d}".into(),
                    sender: "ada".into(),
                }
                .into(),
                // A reaction with no anchor is dropped entirely.
                ChatEvent::ReactionReceived {
                    channel: "#aeolus".into(),
                    msgid: String::new(),
                    emoji: "\u{1f525}".into(),
                    sender: "Ada".into(),
                }
                .into(),
                // Taking one back removes that person and nobody else.
                ChatEvent::ReactionRemoved {
                    channel: "#aeolus".into(),
                    msgid: "srv-Bob-hello".into(),
                    emoji: "\u{1f44d}".into(),
                    sender: "Cid".into(),
                }
                .into(),
                // The last holder leaving takes the whole entry with it,
                // rather than leaving an emoji showing zero.
                ChatEvent::ReactionRemoved {
                    channel: "#aeolus".into(),
                    msgid: "srv-Bob-hello".into(),
                    emoji: "\u{1f44d}".into(),
                    sender: "Ada".into(),
                }
                .into(),
                // Stopping removes the notice at once.
                ChatEvent::TypingChanged {
                    channel: "#aeolus".into(),
                    nickname: "Cid".into(),
                    composing: true,
                    at_seconds: 1_010,
                }
                .into(),
                ChatEvent::TypingChanged {
                    channel: "#aeolus".into(),
                    nickname: "Cid".into(),
                    composing: false,
                    at_seconds: 1_011,
                }
                .into(),
            ],
        ),
        // ── lobby ────────────────────────────────────────────────────────
        case(
            "lobby runs the join state machine",
            vec![
                LobbyEvent::Connecting.into(),
                LobbyEvent::Connected.into(),
                LobbyEvent::Joining {
                    id: 7,
                    prepared: false,
                }
                .into(),
                LobbyEvent::Joining {
                    id: 7,
                    prepared: true,
                }
                .into(),
                LobbyEvent::JoinCancelled.into(),
                LobbyEvent::Joining {
                    id: 7,
                    prepared: false,
                }
                .into(),
                LobbyEvent::Preparing {
                    phase: PreparationPhase::Verifying,
                    detail: "Updating faf".into(),
                    progress: Some(50),
                }
                .into(),
                LobbyEvent::InGame.into(),
                LobbyEvent::GameTerminated.into(),
                LobbyEvent::Disconnected.into(),
            ],
        ),
        case(
            "a matchmaker game that ends releases the queue",
            vec![
                LobbyEvent::Connected.into(),
                // The launch sequence the server drives: paired, then ordered
                // to start.
                LobbyEvent::MatchmakingUpdated {
                    state: MatchmakingState::MatchFound {
                        queue_name: "ladder1v1".into(),
                    },
                }
                .into(),
                LobbyEvent::MatchmakingUpdated {
                    state: MatchmakingState::Launching {
                        queue_name: "ladder1v1".into(),
                    },
                }
                .into(),
                LobbyEvent::InGame.into(),
                // The game exiting has to put the panel back, or the Start
                // button stays locked for the rest of the session.
                LobbyEvent::GameTerminated.into(),
                // A search in progress is not a launch and must survive one:
                // the queue can be rejoined while the last game's process is
                // still shutting down.
                LobbyEvent::MatchmakingUpdated {
                    state: MatchmakingState::Searching {
                        queue_names: vec!["ladder1v1".into()],
                    },
                }
                .into(),
                LobbyEvent::GameTerminated.into(),
            ],
        ),
        case(
            "a reconnect ends a search but not a launch or a running game",
            vec![
                LobbyEvent::Connected.into(),
                LobbyEvent::MatchmakingUpdated {
                    state: MatchmakingState::Preparing {
                        queue_names: vec!["ladder1v1".into()],
                    },
                }
                .into(),
                LobbyEvent::Connecting.into(),
                LobbyEvent::Connected.into(),
                LobbyEvent::MatchmakingUpdated {
                    state: MatchmakingState::Searching {
                        queue_names: vec!["ladder1v1".into(), "tmm2v2".into()],
                    },
                }
                .into(),
                // The server ended this search with the old connection.
                LobbyEvent::Connecting.into(),
                LobbyEvent::Connected.into(),
                LobbyEvent::MatchmakingUpdated {
                    state: MatchmakingState::Launching {
                        queue_name: "ladder1v1".into(),
                    },
                }
                .into(),
                LobbyEvent::InGame.into(),
                // The game outlives the socket; so does its launch.
                LobbyEvent::Connecting.into(),
                LobbyEvent::GameTerminated.into(),
            ],
        ),
        case(
            "a matchmaker launch that fails or is called off frees the panel",
            vec![
                LobbyEvent::Connected.into(),
                LobbyEvent::MatchmakingUpdated {
                    state: MatchmakingState::Launching {
                        queue_name: "ladder1v1".into(),
                    },
                }
                .into(),
                LobbyEvent::LaunchFailed {
                    reason: "ice adapter: no port".into(),
                }
                .into(),
                LobbyEvent::MatchmakingUpdated {
                    state: MatchmakingState::Launching {
                        queue_name: "ladder1v1".into(),
                    },
                }
                .into(),
                LobbyEvent::Preparing {
                    phase: PreparationPhase::Map,
                    detail: "Generating map".into(),
                    progress: None,
                }
                .into(),
                LobbyEvent::JoinCancelled.into(),
                // A cancel with nothing in preparation leaves a game alone.
                LobbyEvent::MatchmakingUpdated {
                    state: MatchmakingState::Launching {
                        queue_name: "ladder1v1".into(),
                    },
                }
                .into(),
                LobbyEvent::InGame.into(),
                LobbyEvent::JoinCancelled.into(),
                LobbyEvent::GameTerminated.into(),
            ],
        ),
        case(
            "a mod version conflict parks the join until it is answered",
            vec![
                LobbyEvent::Connected.into(),
                LobbyEvent::Joining {
                    id: 42,
                    prepared: false,
                }
                .into(),
                LobbyEvent::JoinNeedsModReplacement {
                    id: 42,
                    conflicts: vec![ModVersionConflict {
                        required_uid: "old-uid".into(),
                        required_name: "Total Mayhem".into(),
                        required_version: "11".into(),
                        folder_name: "Total Mayhem".into(),
                        installed_uid: "new-uid".into(),
                        installed_name: "Total Mayhem".into(),
                        installed_version: "12".into(),
                    }],
                }
                .into(),
                // Declining goes back to idle, the same way cancelling a join does.
                LobbyEvent::JoinCancelled.into(),
            ],
        ),
        case(
            "matchmaker queues merge partial pushes and keep a stable order",
            vec![
                LobbyEvent::MatchmakerQueuesUpdated {
                    queues: vec![
                        MatchmakerQueue {
                            queue_name: "tmm4v4".into(),
                            team_size: 4,
                            num_players: 1,
                            queue_pop_time_seconds: 90,
                            queue_pops_at: String::new(),
                            boundary_80s: Vec::new(),
                            boundary_75s: Vec::new(),
                        },
                        MatchmakerQueue {
                            queue_name: "ladder1v1".into(),
                            team_size: 1,
                            num_players: 7,
                            queue_pop_time_seconds: 30,
                            queue_pops_at: String::new(),
                            boundary_80s: vec![RatingRange {
                                min: 900,
                                max: 1_300,
                            }],
                            boundary_75s: vec![RatingRange {
                                min: 800,
                                max: 1_400,
                            }],
                        },
                    ],
                }
                .into(),
                // A push naming one queue must not erase the others.
                LobbyEvent::MatchmakerQueuesUpdated {
                    queues: vec![MatchmakerQueue {
                        queue_name: "tmm2v2".into(),
                        team_size: 2,
                        num_players: 3,
                        queue_pop_time_seconds: 60,
                        queue_pops_at: String::new(),
                        boundary_80s: Vec::new(),
                        boundary_75s: Vec::new(),
                    }],
                }
                .into(),
                LobbyEvent::MatchmakerQueuesUpdated {
                    // The windows travel with the queue: a later push
                    // replaces them rather than merging, because the set of
                    // searches is only ever a snapshot.
                    queues: vec![MatchmakerQueue {
                        queue_name: "ladder1v1".into(),
                        team_size: 1,
                        num_players: 12,
                        queue_pop_time_seconds: 20,
                        queue_pops_at: String::new(),
                        boundary_80s: vec![
                            RatingRange {
                                min: 900,
                                max: 1_300,
                            },
                            RatingRange {
                                min: 1_200,
                                max: 1_600,
                            },
                        ],
                        boundary_75s: vec![RatingRange {
                            min: 800,
                            max: 1_400,
                        }],
                    }],
                }
                .into(),
                LobbyEvent::Disconnected.into(),
            ],
        ),
        case(
            "lobby keeps the play mode across a disconnect",
            vec![
                LobbyEvent::PlayModeChanged {
                    mode: PlayMode::Matchmaking,
                }
                .into(),
                LobbyEvent::Connected.into(),
                LobbyEvent::Disconnected.into(),
            ],
        ),
        case(
            "lobby owns avatar loading selection and disconnect cleanup",
            vec![
                LobbyEvent::AvatarsLoading.into(),
                LobbyEvent::AvatarsLoaded {
                    avatars: vec![AvailableAvatar {
                        url: "https://content.test/avatar.png".into(),
                        tooltip: "Winner".into(),
                    }],
                }
                .into(),
                LobbyEvent::AvatarSelectionStarted.into(),
                LobbyEvent::AvatarSelectionSucceeded.into(),
                LobbyEvent::Disconnected.into(),
            ],
        ),
        // ── coop ─────────────────────────────────────────────────────────
        case(
            "coop opens the first mission and drops a stale board",
            vec![
                CoopEvent::CatalogLoading.into(),
                CoopEvent::CatalogLoaded {
                    scenarios: vec![CoopScenario {
                        id: 1,
                        name: "Ivory Sun".into(),
                        description: String::new(),
                        order: 1,
                        faction: CoopFaction::Uef,
                        category: CoopCategory::Scfa,
                    }],
                    missions: vec![CoopMission {
                        id: 7,
                        name: "Ivory Sun 1".into(),
                        description: String::new(),
                        version: 1,
                        download_url: String::new(),
                        thumbnail_url_small: String::new(),
                        thumbnail_url_large: String::new(),
                        map_folder_name: "scmp_coop_7".into(),
                        scenario_id: Some(1),
                        order: 1,
                    }],
                }
                .into(),
                // For a mission that is not selected: must be ignored.
                CoopEvent::LeaderboardLoaded {
                    mission_id: 99,
                    player_count: 0,
                    results: Vec::new(),
                }
                .into(),
                CoopEvent::PlayerCountChanged { player_count: 2 }.into(),
                CoopEvent::LeaderboardLoadFailed {
                    reason: "session expired".into(),
                    kind: RequestFailureKind::Unauthorized,
                }
                .into(),
            ],
        ),
        // ── clan ─────────────────────────────────────────────────────────
        case(
            "founding a clan retires its own banner when the reload lands",
            vec![
                ClanEvent::ActionStarted {
                    action: ClanAction::Creating,
                }
                .into(),
                ClanEvent::ActionSucceeded {
                    action: ClanAction::Creating,
                }
                .into(),
                ClanEvent::Loaded {
                    identity: ClanIdentity {
                        player_id: 7,
                        login: "Ada".into(),
                        clan_id: "42".into(),
                        clan_name: "Brotherhood".into(),
                        clan_tag: "BRO".into(),
                        is_leader: true,
                    },
                    clan: None,
                }
                .into(),
            ],
        ),
        case(
            "a refused clan write survives the reload that follows it",
            vec![
                ClanEvent::ActionStarted {
                    action: ClanAction::Creating,
                }
                .into(),
                ClanEvent::ActionFailed {
                    action: ClanAction::Creating,
                    reason: "the clan name Brotherhood is already taken".into(),
                    kind: RequestFailureKind::Rejected,
                }
                .into(),
                ClanEvent::Loaded {
                    identity: ClanIdentity::default(),
                    clan: None,
                }
                .into(),
            ],
        ),
        case(
            "an invitation clears the candidate list, and leaving clears the invitation",
            vec![
                ClanEvent::Loaded {
                    identity: ClanIdentity {
                        player_id: 7,
                        login: "Ada".into(),
                        clan_id: "42".into(),
                        clan_name: "Brotherhood".into(),
                        clan_tag: "BRO".into(),
                        is_leader: true,
                    },
                    clan: None,
                }
                .into(),
                ClanEvent::CandidatesLoaded {
                    candidates: vec![PlayerSummary {
                        id: 9,
                        login: "Recruit".into(),
                        avatar_url: String::new(),
                        country: "de".into(),
                        global_rating: Some(1200),
                        ladder_rating: None,
                    }],
                }
                .into(),
                ClanEvent::InvitationReady {
                    invitation: ClanInvitation {
                        token: "jwt".into(),
                        player_id: 9,
                        login: "Recruit".into(),
                    },
                }
                .into(),
                ClanEvent::Loaded {
                    identity: ClanIdentity::default(),
                    clan: None,
                }
                .into(),
            ],
        ),
        // ── reviews ──────────────────────────────────────────────────────
        case(
            "reviews summarise on load and again on save",
            vec![
                ReviewsEvent::Opened {
                    target: ReviewTarget {
                        kind: ReviewKind::Map,
                        id: 42,
                        name: "Seton's".into(),
                    },
                }
                .into(),
                ReviewsEvent::Loading.into(),
                ReviewsEvent::Loaded {
                    target: ReviewTarget {
                        kind: ReviewKind::Map,
                        id: 42,
                        name: "Seton's".into(),
                    },
                    reviews: vec![
                        Review {
                            id: 1,
                            score: 5,
                            text: "Great".into(),
                            player: "Bob".into(),
                            version: "3".into(),
                        },
                        Review {
                            id: 2,
                            score: 2,
                            text: "Meh".into(),
                            player: "Cid".into(),
                            version: "2".into(),
                        },
                    ],
                }
                .into(),
                ReviewsEvent::Saving.into(),
                ReviewsEvent::Saved {
                    reviews: vec![Review {
                        id: 1,
                        score: 5,
                        text: "Great".into(),
                        player: "Bob".into(),
                        version: "3".into(),
                    }],
                }
                .into(),
            ],
        ),
        // ── uploads ──────────────────────────────────────────────────────
        case(
            "uploads keep a running publish when the dialog closes",
            vec![
                UploadsEvent::Opened {
                    request: UploadRequest {
                        kind: UploadKind::Map,
                        folder_name: "my_map.v0001".into(),
                        display_name: "My Map".into(),
                        ranked: false,
                        source_path: None,
                        rename_to: String::new(),
                    },
                }
                .into(),
                // The picture arrives after the dialog is already open, and a
                // later `Opened` clears it again: both halves are worth
                // replaying through the twins.
                UploadsEvent::PreviewRead {
                    data_url: "data:image/png;base64,iVBORw0KGgo=".into(),
                }
                .into(),
                UploadsEvent::RankedChanged { ranked: true }.into(),
                UploadsEvent::Progressed {
                    status: UploadStatus::Compressing {
                        done_bytes: 3,
                        total_bytes: 10,
                    },
                }
                .into(),
                UploadsEvent::Closed.into(),
                UploadsEvent::Progressed {
                    status: UploadStatus::Succeeded,
                }
                .into(),
                UploadsEvent::Closed.into(),
            ],
        ),
        // ── client self-update ───────────────────────────────────────────
        case(
            "a client update is offered, downloaded and then dismissed",
            vec![
                ClientUpdateEvent::CheckStarted {
                    current_version: "0.2.0".into(),
                }
                .into(),
                // Every check records when it settled, whichever way it went:
                // two checks in a row leave the same status, and without this
                // "Check now" changes nothing the user can see.
                ClientUpdateEvent::CheckCompleted {
                    at: "2026-02-01T09:00:00Z".into(),
                }
                .into(),
                ClientUpdateEvent::Available {
                    release: ClientRelease {
                        version: "0.3.0".into(),
                        notes_url: "https://example.invalid/releases/0.3.0".into(),
                        download_url: "https://example.invalid/installer.exe".into(),
                        asset_name: "installer.exe".into(),
                        size_bytes: 4096,
                        pre_release: false,
                        published_at: "2026-02-01T00:00:00Z".into(),
                    },
                }
                .into(),
                ClientUpdateEvent::DownloadProgressed {
                    received_bytes: 2048,
                    total_bytes: 4096,
                }
                .into(),
                ClientUpdateEvent::Downloaded {
                    path: "/cache/updates/faf-client-0.3.0.exe".into(),
                }
                .into(),
                ClientUpdateEvent::Installing.into(),
                ClientUpdateEvent::Dismissed {
                    version: "0.3.0".into(),
                }
                .into(),
                // A later check finding nothing must clear the offer, not keep
                // a dismissed release lying around under an up-to-date status.
                ClientUpdateEvent::UpToDate.into(),
                ClientUpdateEvent::CheckCompleted {
                    at: "2026-02-01T09:05:00Z".into(),
                }
                .into(),
            ],
        ),
        // ── tournaments / tutorials ──────────────────────────────────────
        case(
            "the account's Discord handle is read, then changed",
            vec![
                TourneyEvent::DiscordLoaded {
                    discord: "olduser".into(),
                }
                .into(),
                TourneyEvent::ActionStarted {
                    action: TourneyAction::SavingProfile,
                }
                .into(),
                // The service answers with what it stored, not with what was
                // typed: it strips what it will not keep and cuts the rest.
                TourneyEvent::DiscordLoaded {
                    discord: "newuser".into(),
                }
                .into(),
            ],
        ),
        case(
            "the service's own address arrives with the list",
            vec![
                TourneyEvent::Loading.into(),
                // A trailing slash is the difference between an image url with
                // one slash in it and one with two, and both reducers have to
                // drop it the same way or the frontend's twin is not one.
                TourneyEvent::AssetBase {
                    base: "https://tournaments.example.invalid/".into(),
                }
                .into(),
                TourneyEvent::Loaded {
                    events: vec![tourney("e1")],
                }
                .into(),
            ],
        ),
        case(
            "a player enters a tournament and is refused",
            vec![
                TourneyEvent::Loading.into(),
                TourneyEvent::Loaded {
                    events: vec![tourney("e1"), tourney("e2")],
                }
                .into(),
                TourneyEvent::Selected {
                    tournament_id: "e2".into(),
                }
                .into(),
                TourneyEvent::DetailLoading.into(),
                TourneyEvent::DetailLoaded {
                    event: Box::new(tourney("e2")),
                }
                .into(),
                TourneyEvent::ActionStarted {
                    action: TourneyAction::SigningUp,
                }
                .into(),
                // The server's own sentence: it names the gate that was missed,
                // and it has to survive until it is dismissed.
                TourneyEvent::ActionFailed {
                    failure: TourneyActionFailure {
                        action: TourneyAction::SigningUp,
                        reason: "your rating (1420) is below this tournament’s minimum of 1500"
                            .into(),
                        kind: RequestFailureKind::Rejected,
                    },
                }
                .into(),
                TourneyEvent::ActionErrorDismissed.into(),
                // Entering on the second attempt. The profiles arrive after the
                // detail, from a different service: the tournament service owns
                // the entry, FAF owns the player.
                TourneyEvent::ActionStarted {
                    action: TourneyAction::SigningUp,
                }
                .into(),
                TourneyEvent::ActionSucceeded {
                    action: TourneyAction::SigningUp,
                    select: None,
                }
                .into(),
                TourneyEvent::EntrantProfilesLoaded {
                    profiles: vec![player_summary(101, "Nuggets", Some(1750))],
                }
                .into(),
                // Site-wide, and loaded once rather than per tournament.
                // Creating names the event to open afterwards, which is how
                // the organiser lands inside the one they just made.
                TourneyEvent::ActionStarted {
                    action: TourneyAction::Creating,
                }
                .into(),
                TourneyEvent::ActionSucceeded {
                    action: TourneyAction::Creating,
                    select: Some("e3".into()),
                }
                .into(),
                TourneyEvent::HostingLoaded {
                    hosting: HostingStatus {
                        logged_in: true,
                        allowed: true,
                        pending: false,
                    },
                }
                .into(),
                TourneyEvent::ArticlesLoaded {
                    articles: vec![Article {
                        id: "art33adc81d9f78".into(),
                        title: "Tournament rules".into(),
                        body: "Be on time, and post your replay ids.".into(),
                        parent_id: None,
                    }],
                }
                .into(),
                // A detail for the event nobody is looking at any more must not
                // land under the open one's heading.
                TourneyEvent::DetailLoaded {
                    event: Box::new(tourney("e1")),
                }
                .into(),
                // e2 was archived between refreshes: the selection falls back
                // and takes the bracket with it.
                TourneyEvent::Loaded {
                    events: vec![tourney("e1")],
                }
                .into(),
            ],
        ),
        case(
            "an organiser searches for an account while answers overtake each other",
            vec![
                TourneyEvent::AccountSearchStarted {
                    query: "nug".into(),
                }
                .into(),
                TourneyEvent::AccountSearchLoaded {
                    query: "nug".into(),
                    matches: vec![
                        player_summary(101, "Nuggets", Some(1750)),
                        player_summary(105, "Nugget", Some(980)),
                    ],
                }
                .into(),
                // A fourth letter: the older results are clickable, so they go
                // at once rather than when the new answer arrives.
                TourneyEvent::AccountSearchStarted {
                    query: "nugge".into(),
                }
                .into(),
                // The slower answer for the abandoned query must be dropped, not
                // shown under the newer one.
                TourneyEvent::AccountSearchLoaded {
                    query: "nug".into(),
                    matches: vec![player_summary(101, "Nuggets", Some(1750))],
                }
                .into(),
                // Trimming and case are how the query was sent, so this *is* the
                // current one and it lands.
                TourneyEvent::AccountSearchLoaded {
                    query: " NUGGE ".into(),
                    matches: vec![player_summary(101, "Nuggets", Some(1750))],
                }
                .into(),
                // A refusal names its reason: an empty list that meant "your
                // session expired" would send the organiser hunting for a typo.
                TourneyEvent::AccountSearchStarted {
                    query: "ada".into(),
                }
                .into(),
                TourneyEvent::AccountSearchFailed {
                    query: "ada".into(),
                    reason: "Sign in to FAF to look up players.".into(),
                    kind: RequestFailureKind::Unauthorized,
                }
                .into(),
                // Picking somebody, or leaving the field, drops the list.
                TourneyEvent::AccountSearchCleared.into(),
            ],
        ),
        case(
            "a player checks their rating, and an organiser opens two players' ratings",
            vec![
                // The verdict belongs to the open event.
                TourneyEvent::Selected {
                    tournament_id: "t1".into(),
                }
                .into(),
                // A refusal keeps its sentence: it says to sign in again.
                TourneyEvent::RatingChecking {
                    tournament_id: "t1".into(),
                }
                .into(),
                TourneyEvent::RatingCheckFailed {
                    tournament_id: "t1".into(),
                    reason: "Checking your rating needs your FAF login.".into(),
                    kind: RequestFailureKind::Rejected,
                }
                .into(),
                TourneyEvent::RatingChecking {
                    tournament_id: "t1".into(),
                }
                .into(),
                // An answer about an event that is no longer open is dropped,
                // on both sides, rather than drawn under the open one.
                TourneyEvent::RatingChecked {
                    tournament_id: "t0".into(),
                    check: RatingCheck {
                        rated: true,
                        eligible: Some(true),
                        ..RatingCheck::default()
                    },
                }
                .into(),
                TourneyEvent::RatingCheckFailed {
                    tournament_id: "t0".into(),
                    reason: "stale".into(),
                    kind: RequestFailureKind::Rejected,
                }
                .into(),
                TourneyEvent::RatingChecked {
                    tournament_id: "t1".into(),
                    check: RatingCheck {
                        rated: true,
                        rating: Some(1480),
                        rating_kind: RatingKind::Ladder1v1,
                        min: Some(1500),
                        eligible: Some(false),
                        message: "Your rating is below the minimum of 1500.".into(),
                        ..RatingCheck::default()
                    },
                }
                .into(),
                TourneyEvent::PlayerRatingsLoading {
                    tournament_id: "t1".into(),
                }
                .into(),
                // A ratings table is the open event's too: one asked in an
                // event left behind is dropped, answer or refusal.
                TourneyEvent::PlayerRatingsLoaded {
                    tournament_id: "t0".into(),
                    ratings: EntrantRatings {
                        player_id: "p9".into(),
                        name: "Stale".into(),
                        ..EntrantRatings::default()
                    },
                }
                .into(),
                TourneyEvent::PlayerRatingsLoaded {
                    tournament_id: "t1".into(),
                    ratings: EntrantRatings {
                        player_id: "p1".into(),
                        name: "Ada".into(),
                        counts: RatingKind::Global,
                        boards: vec![EntrantBoardRating {
                            board: RatingKind::Global,
                            rating: Some(1800),
                            games: Some(400),
                        }],
                        ..EntrantRatings::default()
                    },
                }
                .into(),
                // The next player's request clears the first player's table.
                TourneyEvent::PlayerRatingsLoading {
                    tournament_id: "t1".into(),
                }
                .into(),
                TourneyEvent::PlayerRatingsFailed {
                    tournament_id: "t0".into(),
                    reason: "stale".into(),
                    kind: RequestFailureKind::Rejected,
                }
                .into(),
                TourneyEvent::PlayerRatingsFailed {
                    tournament_id: "t1".into(),
                    reason: "FAF could not be reached just now.".into(),
                    kind: RequestFailureKind::Offline,
                }
                .into(),
            ],
        ),
        case(
            "the site: the account, the pending bar, the Hall of Fame and the console",
            vec![
                TourneyEvent::SiteLoaded {
                    document: SiteDocument::Account(TourneyAccount {
                        logged_in: true,
                        oauth: true,
                        faf_id: Some(7),
                        faf_name: "Nuggets".into(),
                        discord: "n#1".into(),
                        director: true,
                        allowed: true,
                        ..TourneyAccount::default()
                    }),
                }
                .into(),
                TourneyEvent::SiteLoaded {
                    document: SiteDocument::Pending(PendingSummary {
                        items: vec![PendingItem {
                            tournament_id: "e1".into(),
                            tournament_name: "Cup".into(),
                            kind: "requests".into(),
                            tab: "players".into(),
                            text: "3 signup requests await your review".into(),
                            count: Some(3),
                        }],
                        requests: Some(2),
                        new_requests: None,
                    }),
                }
                .into(),
                TourneyEvent::SiteLoading {
                    read: SiteRead::HallOfFame,
                }
                .into(),
                TourneyEvent::SiteLoaded {
                    document: SiteDocument::HallOfFame(HallOfFame {
                        players: vec![HallPlayer {
                            faf_id: 5,
                            name: "Ada".into(),
                            wins: 2,
                            entered: 4,
                        }],
                        teams: Vec::new(),
                    }),
                }
                .into(),
                TourneyEvent::SiteLoading {
                    read: SiteRead::Console,
                }
                .into(),
                TourneyEvent::SiteLoadFailed {
                    read: SiteRead::Console,
                    reason: "Site admin, director, or approved editor only".into(),
                    kind: RequestFailureKind::Rejected,
                }
                .into(),
                TourneyEvent::SiteLoaded {
                    document: SiteDocument::Console(Box::new(SiteAdminData {
                        role: ConsoleRole::Editor,
                        ..SiteAdminData::default()
                    })),
                }
                .into(),
                TourneyEvent::SiteLoaded {
                    document: SiteDocument::Access {
                        kind: AccessKind::Editor,
                        status: AccessStatus {
                            oauth: true,
                            logged_in: true,
                            allowed: false,
                            pending: true,
                        },
                    },
                }
                .into(),
                TourneyEvent::SiteLoaded {
                    document: SiteDocument::Access {
                        kind: AccessKind::Host,
                        status: AccessStatus {
                            oauth: true,
                            logged_in: true,
                            allowed: true,
                            pending: false,
                        },
                    },
                }
                .into(),
                TourneyEvent::ArticleImageUploaded {
                    url: "/article-images/a.png".into(),
                }
                .into(),
                TourneyEvent::DescImageUploaded {
                    request_id: 7,
                    url: "/desc-images/d.png".into(),
                }
                .into(),
                TourneyEvent::DescImageUploadFailed { request_id: 8 }.into(),
            ],
        ),
        case(
            "the create form reads the presets and fills from an earlier event twice",
            vec![
                TourneyEvent::PresetsLoaded {
                    presets: vec![TourneyPreset {
                        id: "lots".into(),
                        name: "LotS".into(),
                        blurb: "Invitational with longer deciders".into(),
                        notes: vec!["Seeds 13-16 come from the qualifier.".into()],
                        allowed: true,
                        apply: Some(Box::new(tourney("preset:lots"))),
                    }],
                }
                .into(),
                TourneyEvent::TemplateLoading.into(),
                TourneyEvent::TemplateLoaded {
                    event: Box::new(tourney("e2")),
                }
                .into(),
                // The next request drops the first template at once.
                TourneyEvent::TemplateLoading.into(),
                TourneyEvent::TemplateFailed {
                    reason: "Tournament not found".into(),
                    kind: RequestFailureKind::Rejected,
                }
                .into(),
            ],
        ),
        case(
            "an organiser looks for maps to import, and picks a source twice",
            vec![
                TourneyEvent::CopySourcesLoading.into(),
                TourneyEvent::CopySourcesFailed {
                    reason: "FAF could not be reached just now.".into(),
                    kind: RequestFailureKind::Offline,
                }
                .into(),
                TourneyEvent::CopySourcesLoading.into(),
                TourneyEvent::CopySourcesLoaded {
                    sources: vec![CopySource {
                        id: "e2".into(),
                        name: "Last season".into(),
                        map_count: 2,
                        pool_count: 1,
                        may_copy: true,
                    }],
                }
                .into(),
                TourneyEvent::CopySourceLoading.into(),
                TourneyEvent::CopySourceLoaded {
                    source: CopySourceMaps {
                        tournament_id: "e2".into(),
                        maps: Vec::new(),
                        pools: Vec::new(),
                    },
                }
                .into(),
                // The next source's request clears the first one's maps.
                TourneyEvent::CopySourceLoading.into(),
                TourneyEvent::CopySourceFailed {
                    reason: "Tournament not found".into(),
                    kind: RequestFailureKind::Rejected,
                }
                .into(),
            ],
        ),
        case(
            "an organiser checks names against FAF, fails once, then writes",
            vec![
                TourneyEvent::RenamesChecking.into(),
                // A refusal keeps its sentence: it is the one that says to sign
                // in again.
                TourneyEvent::RenamesCheckFailed {
                    reason: "Checking names needs your FAF login.".into(),
                    kind: RequestFailureKind::Rejected,
                }
                .into(),
                TourneyEvent::RenamesChecking.into(),
                TourneyEvent::RenamesChecked {
                    check: RenameCheck {
                        checked: 3,
                        changed: vec![Rename {
                            player_id: "p1".into(),
                            from: "Old".into(),
                            to: "New".into(),
                            team: Some("Team Old".into()),
                        }],
                        failed: 0,
                        manual: 1,
                    },
                }
                .into(),
                // Any write re-reads the event, so the list goes with it.
                TourneyEvent::ActionStarted {
                    action: TourneyAction::Administering,
                }
                .into(),
                TourneyEvent::ActionSucceeded {
                    action: TourneyAction::Administering,
                    select: None,
                }
                .into(),
            ],
        ),
        case(
            "a series is browsed, opened and then deleted underneath",
            vec![
                TourneyEvent::SeriesLoading.into(),
                TourneyEvent::SeriesLoaded {
                    series: vec![tourney_series("s1", "Weekend Ladder", 2, 1)],
                }
                .into(),
                TourneyEvent::SeriesOpened {
                    detail: Box::new(tourney_series_detail("s1", "Weekend Ladder")),
                }
                .into(),
                // A list that no longer contains the open series drops it: the
                // page would otherwise keep showing editions that nothing can
                // reload, under a heading nobody can reach.
                TourneyEvent::SeriesLoaded {
                    series: vec![tourney_series("s2", "Midweek Blitz", 1, 0)],
                }
                .into(),
                // And the other way: an open series that survives the reload
                // stays open.
                TourneyEvent::SeriesOpened {
                    detail: Box::new(tourney_series_detail("s2", "Midweek Blitz")),
                }
                .into(),
                TourneyEvent::SeriesLoaded {
                    series: vec![tourney_series("s2", "Midweek Blitz", 1, 0)],
                }
                .into(),
                TourneyEvent::SeriesClosed.into(),
                TourneyEvent::SeriesFailed {
                    reason: "no route to host".into(),
                    kind: RequestFailureKind::Offline,
                }
                .into(),
            ],
        ),
        case(
            "a tournament chat room is opened, read and left behind",
            vec![
                TourneyEvent::Loaded {
                    events: vec![tourney("e1"), tourney("e2")],
                }
                .into(),
                TourneyEvent::DetailLoaded {
                    event: Box::new(tourney("e1")),
                }
                .into(),
                TourneyEvent::ChatRoomsLoaded {
                    rooms: vec![tourney_room("global", 3), tourney_room("m1", 1)],
                }
                .into(),
                TourneyEvent::RoomOpened {
                    room_id: "global".into(),
                }
                .into(),
                TourneyEvent::ChatLoading.into(),
                // Reading is what clears the badge server-side, so it clears
                // here too rather than waiting for the next room list.
                TourneyEvent::ChatLoaded {
                    room_id: "global".into(),
                    posts: vec![ChatPost {
                        faf_id: Some(102),
                        id: "c1".into(),
                        author: "Ada".into(),
                        body: "gl hf".into(),
                        at: Some(1_700_000_100),
                        system: false,
                        reply_to: None,
                        everyone: false,
                    }],
                }
                .into(),
                // An answer for a room that is no longer open is dropped.
                TourneyEvent::ChatLoaded {
                    room_id: "m1".into(),
                    posts: vec![],
                }
                .into(),
                // A pinned room is a second slot: its answer lands there, and
                // the open room keeps its own posts.
                TourneyEvent::RoomPinned {
                    room_id: Some("m1".into()),
                }
                .into(),
                TourneyEvent::ChatLoaded {
                    room_id: "m1".into(),
                    posts: vec![ChatPost {
                        faf_id: Some(103),
                        id: "c2".into(),
                        author: "Bo".into(),
                        body: "ready?".into(),
                        at: Some(1_700_000_200),
                        system: false,
                        reply_to: None,
                        everyone: false,
                    }],
                }
                .into(),
                // A room list without it lets the pin go.
                TourneyEvent::ChatRoomsLoaded {
                    rooms: vec![tourney_room("global", 0)],
                }
                .into(),
                TourneyEvent::RoomPinned {
                    room_id: Some("global".into()),
                }
                .into(),
                TourneyEvent::RoomPinned { room_id: None }.into(),
                // Switching events takes the whole conversation with it.
                TourneyEvent::Selected {
                    tournament_id: "e2".into(),
                }
                .into(),
            ],
        ),
        case(
            "the tournament tab hands a match title to the host dialog",
            vec![
                // Crosses a tab boundary, which is why it lives in the slice
                // rather than in a component: the Play tab opens its dialog
                // when the title appears, and clears it on close so the dialog
                // does not reopen on the next visit.
                LobbyEvent::HostPrepared {
                    title: "Weekend Cup R2: Nuggets vs Ada".into(),
                }
                .into(),
                LobbyEvent::HostPrefillCleared.into(),
            ],
        ),
        case(
            "tutorials narrate a launch",
            vec![
                TutorialsEvent::Loading.into(),
                TutorialsEvent::Loaded {
                    categories: vec![TutorialCategory {
                        id: 1,
                        name: "Basics".into(),
                    }],
                    tutorials: vec![tutorial(7), tutorial(8)],
                }
                .into(),
                TutorialsEvent::Selected { tutorial_id: 8 }.into(),
                TutorialsEvent::LaunchPreparing {
                    tutorial_id: 8,
                    detail: "Updating tutorials".into(),
                }
                .into(),
                TutorialsEvent::Launched { tutorial_id: 8 }.into(),
            ],
        ),
        case(
            "the training library loads, is filtered, and recommends",
            vec![
                TrainingEvent::Loading.into(),
                TrainingEvent::Loaded {
                    resources: vec![training_resource("eco"), training_resource("setons")],
                    trainers: vec![Trainer {
                        id: "a-trainer".into(),
                        name: "A trainer".into(),
                        faf_id: Some(101),
                        role: "Personal trainer".into(),
                        focus: "1v1 up to 1800".into(),
                        topics: vec![TrainingTopic::Economy],
                        game_modes: vec!["1v1".into()],
                        rating_min: Some(1000),
                        rating_max: Some(1800),
                        languages: vec!["English".into()],
                        discord: "a-trainer".into(),
                        note: String::new(),
                        avatar_url: String::new(),
                        accepting: true,
                    }],
                    links: TrainingLinks {
                        replay_review_url: "https://forum.faforever.com/category/4/i-need-help"
                            .into(),
                        replay_review_category: Some(4),
                        ..TrainingLinks::default()
                    },
                    source: TrainingSource::Bundled,
                }
                .into(),
                TrainingEvent::QueryChanged {
                    query: Box::new(TrainingQuery {
                        text: "eco".into(),
                        level: Some(TrainingLevel::Beginner),
                        my_rating_only: true,
                        ..TrainingQuery::default()
                    }),
                }
                .into(),
                TrainingEvent::Selected {
                    resource_id: Some("setons".into()),
                }
                .into(),
                TrainingEvent::Recommended {
                    resource_ids: vec!["setons".into(), "eco".into()],
                    profile: Box::new(TrainingProfile {
                        player: "Ada".into(),
                        rating: Some(1150),
                        // Global and 4v4 differ, which is the ordinary case and
                        // the reason the profile carries both.
                        ratings: std::collections::BTreeMap::from([
                            ("global".to_string(), 1150),
                            ("4v4".to_string(), 1320),
                        ]),
                        game_modes: vec!["4v4".into()],
                        maps: vec!["Setons Clutch".into()],
                        factions: vec!["uef".into()],
                        games_seen: 12,
                    }),
                }
                .into(),
                // A reload that drops the open entry must close the detail
                // pane rather than keep showing something absent.
                TrainingEvent::Loaded {
                    resources: vec![training_resource("eco")],
                    trainers: Vec::new(),
                    links: TrainingLinks::default(),
                    source: TrainingSource::Remote,
                }
                .into(),
                TrainingEvent::LoadFailed {
                    reason: "offline".into(),
                }
                .into(),
            ],
        ),
        case(
            "a hosted guide is read, and a reply for a closed entry is dropped",
            vec![
                TrainingEvent::Loaded {
                    resources: vec![training_resource("eco"), training_resource("setons")],
                    trainers: Vec::new(),
                    links: TrainingLinks::default(),
                    source: TrainingSource::Remote,
                }
                .into(),
                TrainingEvent::Selected {
                    resource_id: Some("setons".into()),
                }
                .into(),
                TrainingEvent::GuideReading {
                    resource_id: "setons".into(),
                }
                .into(),
                TrainingEvent::GuideRead {
                    resource_id: "setons".into(),
                    markdown: "# Seton's\n\n- **ACU:** Landfac\n".into(),
                }
                .into(),
                // Moving to another entry drops the text with it: the title
                // above the pane now belongs to something else.
                TrainingEvent::Selected {
                    resource_id: Some("eco".into()),
                }
                .into(),
                // The slow reply for the entry just left. Dropped, rather than
                // rendered under the wrong heading.
                TrainingEvent::GuideRead {
                    resource_id: "setons".into(),
                    markdown: "too late".into(),
                }
                .into(),
                TrainingEvent::GuideReading {
                    resource_id: "eco".into(),
                }
                .into(),
                TrainingEvent::GuideFailed {
                    resource_id: "eco".into(),
                    reason: "could not reach the guide".into(),
                }
                .into(),
                // And a failure for an entry already left is dropped too, so a
                // reader is not shown an error about something they closed.
                TrainingEvent::Selected { resource_id: None }.into(),
                TrainingEvent::GuideFailed {
                    resource_id: "eco".into(),
                    reason: "still too late".into(),
                }
                .into(),
            ],
        ),
        case(
            "a recorded run arrives beside the prose, and separately from it",
            vec![
                TrainingEvent::Loaded {
                    resources: vec![training_resource("eco"), training_resource("setons")],
                    trainers: Vec::new(),
                    links: TrainingLinks::default(),
                    source: TrainingSource::Remote,
                }
                .into(),
                TrainingEvent::Selected {
                    resource_id: Some("setons".into()),
                }
                .into(),
                TrainingEvent::GuideReading {
                    resource_id: "setons".into(),
                }
                .into(),
                TrainingEvent::RecordingReading {
                    resource_id: "setons".into(),
                }
                .into(),
                // The prose can fail while the run succeeds. They are tracked
                // apart precisely so one missing does not read as the other
                // missing, and this is the case that pins it.
                TrainingEvent::GuideFailed {
                    resource_id: "setons".into(),
                    reason: "could not reach the guide".into(),
                }
                .into(),
                TrainingEvent::RecordingRead {
                    resource_id: "setons".into(),
                    envelope: "{\"schema\":\"faf-bo/1\"}".into(),
                }
                .into(),
                // A run for an entry that carries no prose claims the document
                // on its own rather than waiting for a guide that never comes.
                TrainingEvent::Selected {
                    resource_id: Some("eco".into()),
                }
                .into(),
                TrainingEvent::RecordingReading {
                    resource_id: "eco".into(),
                }
                .into(),
                TrainingEvent::RecordingFailed {
                    resource_id: "eco".into(),
                    reason: "that recording was unexpectedly large".into(),
                }
                .into(),
                // And a reply for an entry already left is dropped, the same
                // way a guide's is.
                TrainingEvent::Selected { resource_id: None }.into(),
                TrainingEvent::RecordingRead {
                    resource_id: "eco".into(),
                    envelope: "too late".into(),
                }
                .into(),
            ],
        ),
        case(
            "a replay review is prefilled, composed, and invalidated by an edit",
            vec![
                TrainingEvent::ReviewOpened {
                    draft: Box::new(review_draft()),
                }
                .into(),
                TrainingEvent::ReviewComposed {
                    post: Box::new(compose_review_request(
                        &review_draft(),
                        &TrainingLinks {
                            replay_review_category: Some(4),
                            ..TrainingLinks::default()
                        },
                    )),
                }
                .into(),
                // Editing after composing drops the post: otherwise the player
                // sends the version they just changed away from.
                TrainingEvent::ReviewChanged {
                    draft: Box::new(ReviewRequestDraft {
                        goal: "Why did my push stall?".into(),
                        ..review_draft()
                    }),
                }
                .into(),
                TrainingEvent::ReviewClosed.into(),
            ],
        ),
        case(
            "the catalogue queue signs in, accepts one and declines another",
            vec![
                GuidesEvent::Configured {
                    repo: GUIDES_REPO.into(),
                    configured: true,
                }
                .into(),
                GuidesEvent::SignInStarted {
                    login: Box::new(DeviceLogin {
                        user_code: "WDJB-MJHT".into(),
                        verification_uri: "https://github.com/login/device".into(),
                        expires_at: 1_800_000_900,
                    }),
                }
                .into(),
                GuidesEvent::SignedIn {
                    identity: Box::new(GuidesIdentity {
                        login: "a-trainer".into(),
                        avatar_url: String::new(),
                        can_commit: true,
                    }),
                }
                .into(),
                GuidesEvent::QueueLoading.into(),
                GuidesEvent::QueueLoaded {
                    submissions: vec![submission(11), submission(12)],
                }
                .into(),
                GuidesEvent::Accepting { number: 11 }.into(),
                // A settled verdict drops its row: the queue is what is still
                // open, and a decided submission left in it invites a second
                // verdict.
                GuidesEvent::Accepted { number: 11 }.into(),
                GuidesEvent::Rejecting { number: 12 }.into(),
                // A failed write keeps the row, so it can be tried again.
                GuidesEvent::WriteFailed {
                    number: 12,
                    reason: "Resource not accessible by personal access token".into(),
                }
                .into(),
                // The retry lands. Like an accept, it drops the row and is
                // remembered, so the reload that follows cannot resurrect it.
                GuidesEvent::Rejecting { number: 12 }.into(),
                GuidesEvent::Rejected { number: 12 }.into(),
                GuidesEvent::QueueLoading.into(),
                GuidesEvent::QueueLoaded {
                    submissions: vec![submission(11), submission(12)],
                }
                .into(),
                // A read that fails keeps whatever was already listed.
                GuidesEvent::QueueLoadFailed {
                    reason: "API rate limit exceeded".into(),
                }
                .into(),
                GuidesEvent::SignedOut.into(),
                // Signing in again, abandoned and then refused.
                GuidesEvent::SignInStarted {
                    login: Box::new(DeviceLogin {
                        user_code: "14E4-218A".into(),
                        verification_uri: "https://github.com/login/device".into(),
                        expires_at: 1_800_001_800,
                    }),
                }
                .into(),
                GuidesEvent::SignInCancelled.into(),
                GuidesEvent::SignInFailed {
                    reason: "the sign-in code expired before it was used".into(),
                }
                .into(),
            ],
        ),
        case(
            "a submission of our own reports where it landed",
            vec![
                GuidesEvent::Submitting.into(),
                GuidesEvent::Submitted {
                    url: "https://github.com/FAForeverRustClient/guides/issues/13".into(),
                }
                .into(),
                // Writing a second guide in one session: without this the
                // preview kept offering the first one's link and hid the send
                // button, because the state still said the work was done.
                GuidesEvent::SubmitReset.into(),
                GuidesEvent::SubmitFailed {
                    reason: "GitHub refused to open the submission: Not Found".into(),
                }
                .into(),
            ],
        ),
        case(
            "a contribution is drafted, composed, and closed",
            vec![
                TrainingEvent::ContributionOpened {
                    draft: Box::new(ContributionDraft::default()),
                }
                .into(),
                TrainingEvent::ContributionChanged {
                    draft: Box::new(contribution_draft()),
                }
                .into(),
                TrainingEvent::ContributionComposed {
                    post: Box::new(compose_contribution(
                        &contribution_draft(),
                        &TrainingLinks {
                            contribute_category: Some(4),
                            ..TrainingLinks::default()
                        },
                    )),
                }
                .into(),
                TrainingEvent::ContributionClosed.into(),
            ],
        ),
        // ── notifications / reporting / social ───────────────────────────
        case(
            "notifications are added and dismissed",
            vec![
                NotificationEvent::Added {
                    notification: ClientNotification {
                        id: "n1".into(),
                        kind: NotificationKind::Mention,
                        title: "Hello".into(),
                        body: "World".into(),
                        action: None,
                        created_at: "2026-01-01T00:00:00Z".into(),
                        read: false,
                        text: None,
                    },
                }
                .into(),
                NotificationEvent::Dismissed { id: "n1".into() }.into(),
            ],
        ),
        case(
            "social records relations and removes offline profiles",
            vec![
                SocialEvent::RelationsUpdated {
                    friends: vec!["Bob".into()],
                    foes: vec!["Cid".into()],
                }
                .into(),
                SocialEvent::PlayersSeen {
                    players: vec![PlayerProfile {
                        id: 2,
                        login: "Bob".into(),
                        ..Default::default()
                    }],
                }
                .into(),
                SocialEvent::PlayersRemoved {
                    logins: vec!["Bob".into()],
                }
                .into(),
            ],
        ),
        // ── maps / mods ──────────────────────────────────────────────────
        case(
            "maps load the vault and the installed list",
            vec![
                MapsEvent::VaultLoading.into(),
                MapsEvent::VaultLoadFailed {
                    reason: "503".into(),
                }
                .into(),
                // The browsed page is a separate slice from the catalogue
                // index, so both reducers have to keep them apart.
                MapsEvent::VaultSearching.into(),
                MapsEvent::VaultSearched {
                    maps: Vec::new(),
                    query: faf_domain::protocol::vault_query::MapVaultQuery {
                        search: "seton".into(),
                        page: 2,
                        ..Default::default()
                    },
                    total_pages: Some(4),
                    total_records: Some(131),
                }
                .into(),
                MapsEvent::VaultSearchFailed {
                    reason: "the filter was rejected".into(),
                }
                .into(),
                MapsEvent::InstalledLoading.into(),
                MapsEvent::InstalledLoaded {
                    maps: vec![InstalledMap {
                        folder_name: "scmp_009".into(),
                        display_name: "Open Palms".into(),
                        max_players: 6,
                        width: 512,
                        height: 512,
                        version: Some("1.0".into()),
                        description: None,
                        installed_at: None,
                    }],
                }
                .into(),
            ],
        ),
        case(
            "local map previews merge per size and remember a fruitless look",
            vec![
                // Written from two directions: a tile that ran out of remote
                // candidates asks for one size, a detail pane for the other.
                // A whole-value overwrite would lose whichever landed first.
                MapsEvent::LocalPreviewsLoaded {
                    previews: [(
                        "scca_coop_a01".to_string(),
                        LocalMapPreview {
                            small: Some("data:image/png;base64,c21hbGw=".into()),
                            large: None,
                        },
                    )]
                    .into_iter()
                    .collect(),
                }
                .into(),
                MapsEvent::LocalPreviewsLoaded {
                    previews: [
                        (
                            "scca_coop_a01".to_string(),
                            LocalMapPreview {
                                small: None,
                                large: Some("data:image/png;base64,bGFyZ2U=".into()),
                            },
                        ),
                        // An empty entry is the answer "looked, found nothing":
                        // without it the UI asks again on every render.
                        ("plain_map".to_string(), LocalMapPreview::default()),
                    ]
                    .into_iter()
                    .collect(),
                }
                .into(),
            ],
        ),
        case(
            "an author hides one of their own map versions, then is refused the way back",
            vec![
                MapsEvent::VaultSearched {
                    maps: vec![
                        conformance_vault_map(1, 11, "scmp_009.v0001"),
                        conformance_vault_map(2, 22, "open_palms.v0001"),
                    ],
                    query: faf_domain::protocol::vault_query::MapVaultQuery {
                        author_id: Some(4711),
                        include_hidden: true,
                        ..Default::default()
                    },
                    total_pages: Some(1),
                    total_records: Some(2),
                }
                .into(),
                MapsEvent::MapVisibilityChanging { version_id: 22 }.into(),
                // Only the named version changes, and it changes in place: a
                // refetch would move the page under the user.
                MapsEvent::MapVisibilityChanged {
                    version_id: 22,
                    hidden: true,
                }
                .into(),
                MapsEvent::MapVisibilityChanging { version_id: 22 }.into(),
                MapsEvent::MapVisibilityFailed {
                    reason: "only a map administrator can unhide a version".into(),
                }
                .into(),
                // The refusal belongs to the page it happened on, so the next
                // search clears it rather than leaving it above every later one.
                MapsEvent::VaultSearching.into(),
            ],
        ),
        case(
            "a withdrawn map looked up by folder joins the index once",
            vec![
                MapsEvent::VaultLoaded {
                    maps: vec![conformance_vault_map(1, 11, "scmp_009.v0001")],
                }
                .into(),
                // Twice, and with the catalogue's own record in the second
                // answer: neither may duplicate what the index already holds.
                MapsEvent::VaultFoldersResolved {
                    maps: vec![conformance_vault_map(3, 33, "withdrawn_map.v0002")],
                }
                .into(),
                MapsEvent::VaultFoldersResolved {
                    maps: vec![
                        conformance_vault_map(3, 33, "withdrawn_map.v0002"),
                        conformance_vault_map(1, 11, "scmp_009.v0001"),
                    ],
                }
                .into(),
            ],
        ),
        case(
            "the catalogue arriving after a folder lookup keeps the looked-up map",
            vec![
                MapsEvent::VaultLoading.into(),
                MapsEvent::VaultFoldersResolved {
                    maps: vec![conformance_vault_map(4, 44, "phenom spartiate v2")],
                }
                .into(),
                MapsEvent::VaultLoaded {
                    maps: vec![
                        conformance_vault_map(1, 11, "scmp_009.v0001"),
                        conformance_vault_map(4, 44, "phenom spartiate v2"),
                    ],
                }
                .into(),
                MapsEvent::VaultFoldersResolved {
                    maps: vec![conformance_vault_map(5, 55, "old_map")],
                }
                .into(),
                MapsEvent::VaultLoaded {
                    maps: vec![conformance_vault_map(1, 11, "scmp_009.v0001")],
                }
                .into(),
            ],
        ),
        case(
            "the profile search's account suggestions replace each other",
            vec![
                PlayerCardEvent::AccountsFound {
                    lookup: AccountLookup {
                        query: "yud".into(),
                        matches: vec![
                            AccountLookupMatch {
                                player_id: 3,
                                login: "Yudi_Real".into(),
                                former_name: None,
                            },
                            AccountLookupMatch {
                                player_id: 2,
                                login: "Later".into(),
                                former_name: Some("yudi".into()),
                            },
                        ],
                    },
                }
                .into(),
                PlayerCardEvent::AccountsFound {
                    lookup: AccountLookup {
                        query: "yudi_".into(),
                        matches: Vec::new(),
                    },
                }
                .into(),
            ],
        ),
        case(
            "mods load and report a failure",
            vec![
                ModsEvent::VaultSearching.into(),
                ModsEvent::VaultSearched {
                    mods: Vec::new(),
                    query: faf_domain::protocol::vault_query::ModVaultQuery {
                        mod_type: "ui".into(),
                        ..Default::default()
                    },
                    total_pages: Some(2),
                    total_records: Some(48),
                }
                .into(),
                ModsEvent::VaultSearchFailed {
                    reason: "the filter was rejected".into(),
                }
                .into(),
                ModsEvent::InstalledLoading.into(),
                ModsEvent::InstalledLoadFailed {
                    reason: "no mods folder".into(),
                }
                .into(),
            ],
        ),
        case(
            "mod download sizes keep only the answers that carried a number",
            vec![
                ModsEvent::DownloadSizesResolved {
                    sizes: vec![
                        faf_domain::state::ModDownloadSize {
                            uid: "AAA".into(),
                            bytes: Some(4_194_304),
                        },
                        // No length in the response. Absent from the state
                        // rather than stored as a zero, because "unknown" and
                        // "empty" are different answers and only one of them is
                        // worth printing next to a mod.
                        faf_domain::state::ModDownloadSize {
                            uid: "BBB".into(),
                            bytes: None,
                        },
                    ],
                }
                .into(),
                // A second answer for a uid already known replaces it, which is
                // what makes re-asking after a vault refresh harmless.
                ModsEvent::DownloadSizesResolved {
                    sizes: vec![faf_domain::state::ModDownloadSize {
                        uid: "AAA".into(),
                        bytes: Some(5_242_880),
                    }],
                }
                .into(),
            ],
        ),
        // ── map generator ────────────────────────────────────────────────
        case(
            "the map generator narrates a run and keeps its option lists",
            vec![
                MapGeneratorEvent::StatusChanged {
                    status: GeneratorStatus::ResolvingVersion,
                }
                .into(),
                MapGeneratorEvent::OptionListLoaded {
                    query: GeneratorOptionQuery::Styles,
                    values: vec!["LAND".into(), "BASIC".into()],
                }
                .into(),
                MapGeneratorEvent::StatusChanged {
                    status: GeneratorStatus::Generated {
                        maps: vec!["neroxis_map_generator_1.9.0_abc".into()],
                    },
                }
                .into(),
            ],
        ),
        case(
            "the map generator reports bad options, then a name it would produce",
            vec![
                // The dialog checks options as they are edited, so the issue
                // list has to be replaced wholesale rather than appended to:
                // a fixed problem must disappear.
                MapGeneratorEvent::ValidationChanged {
                    issues: vec![
                        faf_domain::state::ValidationIssue::SpawnsNotDivisibleByTeams {
                            spawn_count: 5,
                            num_teams: 2,
                        },
                    ],
                }
                .into(),
                MapGeneratorEvent::ValidationChanged { issues: vec![] }.into(),
                MapGeneratorEvent::NamePredicted {
                    map_name: "neroxis_map_generator_1.22.1_aaaaaaaaaayds_ayeaeaaj".into(),
                }
                .into(),
                // A cancellation is its own terminal status: not a failure.
                MapGeneratorEvent::StatusChanged {
                    status: GeneratorStatus::Cancelled,
                }
                .into(),
            ],
        ),
        case(
            "decoded map names accumulate while help text replaces itself",
            vec![
                MapGeneratorEvent::NamesDecoded {
                    decoded: std::collections::HashMap::from([(
                        "neroxis_map_generator_1.22.1_aaaaaaaaaayds_ayeaeaaj".to_string(),
                        faf_domain::protocol::map_generator_name::decode(
                            "neroxis_map_generator_1.22.1_aaaaaaaaaayds_ayeaeaaj",
                        )
                        .expect("a known-good name must decode"),
                    )]),
                }
                .into(),
                MapGeneratorEvent::HelpLoaded {
                    text: "Usage: generate [-hV]".into(),
                }
                .into(),
                // The preset library is replaced wholesale on every change, so
                // a deleted preset has to disappear rather than linger.
                MapGeneratorEvent::PresetsLoaded {
                    presets: vec![
                        faf_domain::state::GeneratorPreset {
                            name: "Team Ladder".into(),
                            saved_at: "2026-08-16T00:00:00+00:00".into(),
                            options: GeneratorOptions::default(),
                        },
                        faf_domain::state::GeneratorPreset {
                            name: "Blind 1v1".into(),
                            saved_at: "2026-08-15T00:00:00+00:00".into(),
                            options: GeneratorOptions::default(),
                        },
                    ],
                }
                .into(),
                MapGeneratorEvent::PresetsLoaded {
                    presets: vec![faf_domain::state::GeneratorPreset {
                        name: "Team Ladder".into(),
                        saved_at: "2026-08-16T00:00:00+00:00".into(),
                        options: GeneratorOptions::default(),
                    }],
                }
                .into(),
                // A save answers its own request, stored or not; the newest
                // answer is the one kept.
                MapGeneratorEvent::PresetSaveFinished {
                    request_id: 4,
                    saved: true,
                }
                .into(),
                MapGeneratorEvent::PresetSaveFinished {
                    request_id: 5,
                    saved: false,
                }
                .into(),
            ],
        ),
        case(
            "one replay analysis is held at a time",
            vec![
                ReplayEvent::AnalysisLoading { uid: 11 }.into(),
                ReplayEvent::AnalysisLoaded {
                    analysis: faf_domain::state::ReplayAnalysis {
                        uid: 11,
                        ticks: 3_000,
                        game_version: "Supreme Commander v1.50.3839".into(),
                        activity: vec![faf_domain::state::ReplayActivity {
                            source: 0,
                            command_ticks: vec![10, 20, 30],
                            last_tick: 30,
                        }],
                        ..Default::default()
                    },
                }
                .into(),
                // Another replay's panel opens: the orders of the last one
                // are megabytes that describe a game nobody is looking at.
                ReplayEvent::AnalysisLoading { uid: 12 }.into(),
                ReplayEvent::AnalysisFailed {
                    uid: 12,
                    reason: "the replay file could not be read".into(),
                }
                .into(),
            ],
        ),
        case(
            "a late replay analysis never replaces the newer request's answer",
            vec![
                // Two panels opened in turn, both reads running at once.
                ReplayEvent::AnalysisLoading { uid: 21 }.into(),
                ReplayEvent::AnalysisLoading { uid: 22 }.into(),
                // The older answer landing first does not take the slot.
                ReplayEvent::AnalysisLoaded {
                    analysis: faf_domain::state::ReplayAnalysis {
                        uid: 21,
                        ticks: 1_000,
                        ..Default::default()
                    },
                }
                .into(),
                ReplayEvent::AnalysisLoaded {
                    analysis: faf_domain::state::ReplayAnalysis {
                        uid: 22,
                        ticks: 2_000,
                        ..Default::default()
                    },
                }
                .into(),
                // Nor does it landing last, and its failure is not 22's.
                ReplayEvent::AnalysisLoaded {
                    analysis: faf_domain::state::ReplayAnalysis {
                        uid: 21,
                        ticks: 1_000,
                        ..Default::default()
                    },
                }
                .into(),
                ReplayEvent::AnalysisFailed {
                    uid: 21,
                    reason: "late".into(),
                }
                .into(),
                // The newest request's own failure carries its uid.
                ReplayEvent::AnalysisLoading { uid: 23 }.into(),
                ReplayEvent::AnalysisFailed {
                    uid: 23,
                    reason: "the replay body is truncated".into(),
                }
                .into(),
            ],
        ),
        // ── leaderboard ──────────────────────────────────────────────────
        case(
            "the other boards land only for the page that is on screen",
            vec![
                LeaderboardEvent::RatingsLoading {
                    query: faf_domain::state::RatingQuery {
                        leaderboard: "global".into(),
                        page: 1,
                        ..Default::default()
                    },
                }
                .into(),
                // The answer to the page being looked at.
                LeaderboardEvent::CrossRatingsLoaded {
                    query: faf_domain::state::RatingQuery {
                        leaderboard: "global".into(),
                        page: 1,
                        ..Default::default()
                    },
                    ratings: vec![faf_domain::state::PlayerRatings {
                        player_id: 11,
                        ratings: vec![faf_domain::state::BoardRating {
                            leaderboard: "ladder_1v1".into(),
                            rating: 1_842,
                            games_played: 310,
                        }],
                    }],
                }
                .into(),
                // A page turn, and then the previous page's late answer. It
                // describes players who are no longer listed, so it is
                // dropped rather than drawn beside the new ones.
                LeaderboardEvent::RatingsLoading {
                    query: faf_domain::state::RatingQuery {
                        leaderboard: "global".into(),
                        page: 2,
                        ..Default::default()
                    },
                }
                .into(),
                LeaderboardEvent::CrossRatingsLoaded {
                    query: faf_domain::state::RatingQuery {
                        leaderboard: "global".into(),
                        page: 1,
                        ..Default::default()
                    },
                    ratings: vec![faf_domain::state::PlayerRatings {
                        player_id: 11,
                        ratings: vec![faf_domain::state::BoardRating {
                            leaderboard: "ladder_1v1".into(),
                            rating: 1_842,
                            games_played: 310,
                        }],
                    }],
                }
                .into(),
            ],
        ),
        case(
            "changing league clears stale season data",
            vec![
                LeaderboardEvent::ModeChanged {
                    mode: LeaderboardMode::Leagues,
                }
                .into(),
                LeaderboardEvent::SeasonsLoading { league_id: 3 }.into(),
                LeaderboardEvent::SeasonsLoadFailed {
                    reason: "503".into(),
                }
                .into(),
                LeaderboardEvent::SeasonsLoading { league_id: 8 }.into(),
            ],
        ),
        // ── player card ──────────────────────────────────────────────────
        case(
            "the player card opens and closes",
            vec![
                PlayerCardEvent::Loading {
                    login: "Bob".into(),
                }
                .into(),
                PlayerCardEvent::LoadFailed {
                    reason: "no such player".into(),
                }
                .into(),
                PlayerCardEvent::Closed.into(),
            ],
        ),
        case(
            "the open own profile follows avatar selections",
            vec![
                PlayerCardEvent::Loaded {
                    profile: Box::new(PlayerCardProfile {
                        player_id: 7,
                        login: "Ada".into(),
                        country: String::new(),
                        registered_at: String::new(),
                        user_agent: String::new(),
                        avatars: vec![PlayerAvatar {
                            url: "old".into(),
                            tooltip: "Old".into(),
                            selected: true,
                            expires_at: None,
                        }],
                        names: Vec::new(),
                        clan: None,
                        ratings: Vec::new(),
                        league_placements: Vec::new(),
                        events: Vec::new(),
                        achievements: Vec::new(),
                        warnings: Vec::new(),
                        matched_former_name: None,
                    }),
                }
                .into(),
                PlayerCardEvent::AvatarSelected {
                    player_id: 7,
                    url: Some("new".into()),
                    tooltip: "New".into(),
                }
                .into(),
                PlayerCardEvent::AvatarSelected {
                    player_id: 7,
                    url: None,
                    tooltip: String::new(),
                }
                .into(),
            ],
        ),
        case(
            "the matchmaker profile loads independently of the investigation modal",
            vec![
                PlayerCardEvent::MatchmakerProfileLoading { player_id: 7 }.into(),
                PlayerCardEvent::MatchmakerProfileLoaded {
                    profile: Box::new(MatchmakerPlayerProfile {
                        player_id: 7,
                        login: "Ada".into(),
                        country: "de".into(),
                        clan_tag: "FAF".into(),
                        avatar_url: "avatar".into(),
                        avatar_tooltip: "Champion".into(),
                        games_played: 120,
                        ratings: Vec::new(),
                        league_placements: Vec::new(),
                        warnings: Vec::new(),
                    }),
                }
                .into(),
                PlayerCardEvent::MatchmakerProfileLoadFailed {
                    player_id: 7,
                    reason: "offline".into(),
                }
                .into(),
            ],
        ),
        case(
            "party placements merge and keep an unplaced member distinguishable",
            vec![
                PlayerCardEvent::PartyPlacementsLoaded {
                    placements: std::collections::BTreeMap::from([
                        (
                            7,
                            vec![PlayerLeaguePlacement {
                                technical_name: "ladder_1v1".into(),
                                season_number: 1,
                                division: "diamond".into(),
                                subdivision: "I".into(),
                                score: 5,
                                highest_score: 10,
                                games_played: 3,
                                image_url: "diamond.png".into(),
                            }],
                        ),
                        (8, Vec::new()),
                    ]),
                }
                .into(),
                PlayerCardEvent::PartyPlacementsLoaded {
                    placements: std::collections::BTreeMap::from([(
                        9,
                        vec![PlayerLeaguePlacement {
                            technical_name: "tmm_2v2".into(),
                            season_number: 1,
                            division: "bronze".into(),
                            subdivision: "II".into(),
                            score: 1,
                            highest_score: 4,
                            games_played: 2,
                            image_url: "bronze.png".into(),
                        }],
                    )]),
                }
                .into(),
            ],
        ),
        // ── replays ──────────────────────────────────────────────────────
        case(
            "the matchmaker tab's recent games load beside the vault, not into it",
            vec![
                ReplayEvent::RecentMatchmakerLoading.into(),
                ReplayEvent::RecentMatchmakerFailed {
                    reason: "not logged in".into(),
                }
                .into(),
                ReplayEvent::RecentMatchmakerLoading.into(),
                ReplayEvent::RecentMatchmakerLoaded {
                    replays: vec![vault_replay(61), vault_replay(60)],
                }
                .into(),
            ],
        ),
        case(
            "an old replay reports what it is preparing, and the report ends with it",
            vec![
                // Ignored: nothing is starting yet.
                ReplayEvent::Preparing {
                    step: ReplayPreparation {
                        detail: "too early".into(),
                        progress: None,
                    },
                }
                .into(),
                ReplayEvent::Connecting.into(),
                ReplayEvent::Preparing {
                    step: ReplayPreparation {
                        detail: "Downloading ForgedAlliance.exe".into(),
                        progress: Some(40),
                    },
                }
                .into(),
                ReplayEvent::Playing {
                    uid: Some(5),
                    warning: None,
                }
                .into(),
            ],
        ),
        case(
            "a replay connects, fails, and is dismissed",
            vec![
                ReplayEvent::Connecting.into(),
                ReplayEvent::Failed {
                    reason: "delayed by five minutes".into(),
                }
                .into(),
                ReplayEvent::Closed.into(),
                ReplayEvent::LiveTrackingScheduled {
                    tracking: LiveReplayTracking {
                        target: LiveReplayTarget {
                            uid: 7,
                            mod_name: "faf".into(),
                            map: "scmp_009".into(),
                        },
                        title: "Tracked replay".into(),
                        action: LiveReplayTrackingAction::Notify,
                        ready_at: 1_800_000_300,
                    },
                }
                .into(),
                ReplayEvent::LiveTrackingCleared.into(),
                // The page count the pager renders numbered pages from. The
                // TypeScript twin silently dropped it, and nothing noticed
                // because this event had no fixture case at all.
                ReplayEvent::VaultLoading.into(),
                ReplayEvent::VaultLoaded {
                    replays: Vec::new(),
                    query: Box::new(ReplayQuery {
                        page: 2,
                        ..Default::default()
                    }),
                    has_more: true,
                    total_pages: Some(9),
                    total_records: Some(412),
                }
                .into(),
                // The maps read out of the replay files themselves. An empty
                // one is an answer as much as a folder name is: the view stops
                // asking about that game either way, so both have to survive
                // the trip through the reducer.
                ReplayEvent::MapsResolved {
                    maps: vec![
                        ResolvedReplayMap {
                            uid: 42,
                            map: "SCCA_Coop_A03.v0023".into(),
                        },
                        ResolvedReplayMap {
                            uid: 43,
                            map: String::new(),
                        },
                    ],
                }
                .into(),
                ReplayEvent::VaultDownloadStarted { uid: 42 }.into(),
                ReplayEvent::VaultDownloaded {
                    uid: 42,
                    replay: local_replay(42),
                }
                .into(),
                ReplayEvent::VaultDownloadStarted { uid: 43 }.into(),
                ReplayEvent::VaultDownloadFailed {
                    uid: 43,
                    reason: "not uploaded yet".into(),
                }
                .into(),
                // Watching a vault replay downloads it as part of playback, so
                // the download ends at `Playing` or `Failed` and never at
                // `VaultDownloaded`. Both reducers have to drop the transient
                // indicator there. Only the TypeScript one did not, and nothing
                // caught it because this pairing was not in the fixture: the
                // status bar showed "Downloading <uid>" for the rest of the
                // session after every watched replay.
                ReplayEvent::VaultDownloadStarted { uid: 44 }.into(),
                ReplayEvent::Playing {
                    uid: Some(44),
                    warning: None,
                }
                .into(),
                ReplayEvent::VaultDownloadStarted { uid: 45 }.into(),
                ReplayEvent::Failed {
                    reason: "could not update game to version 3701".into(),
                }
                .into(),
                // The third way that download can end: the user calls the
                // launch off while it is still running. Same hole as the two
                // above, found the same way -- the overlay's Cancel stopped the
                // launch and left "Downloading <uid>" in the status bar, which
                // reads as a download that was not cancelled.
                ReplayEvent::VaultDownloadStarted { uid: 46 }.into(),
                ReplayEvent::Closed.into(),
            ],
        ),
        // The three answers a local replay's rating lookup can get. All three
        // land in the same map and none of them touches `vault`: a local
        // replay opened while the Online tab holds results must not replace
        // them.
        case(
            "the vault is asked about three local replays' game ids",
            vec![
                ReplayEvent::OnlineLookupStarted { uid: 51 }.into(),
                ReplayEvent::OnlineLookupFinished {
                    uid: 51,
                    replay: Some(Box::new(vault_replay(51))),
                }
                .into(),
                ReplayEvent::OnlineLookupStarted { uid: 52 }.into(),
                ReplayEvent::OnlineLookupFinished {
                    uid: 52,
                    replay: None,
                }
                .into(),
                ReplayEvent::OnlineLookupStarted { uid: 53 }.into(),
                ReplayEvent::OnlineLookupFailed {
                    uid: 53,
                    reason: "offline".into(),
                }
                .into(),
            ],
        ),
        // ── reporting ────────────────────────────────────────────────────
        case(
            "a report is opened, submitted, and closed",
            vec![
                ReportingEvent::Opened {
                    player_id: 7,
                    login: "Bob".into(),
                }
                .into(),
                ReportingEvent::Submitting.into(),
                ReportingEvent::Submitted.into(),
                ReportingEvent::Closed.into(),
            ],
        ),
        // ── galactic war ─────────────────────────────────────────────────
        case(
            "galactic war is checked, updated, started, and reports its season",
            vec![
                GalacticWarEvent::InstallationChanged {
                    version: Some("v2026.03.01.1".into()),
                }
                .into(),
                GalacticWarEvent::StatusChanged {
                    status: GalacticWarStatus::CheckingVersion,
                }
                .into(),
                GalacticWarEvent::VersionsLoaded {
                    versions: ClientVersions {
                        required_version: "v2026.04.04.1".into(),
                        latest_version: None,
                    },
                }
                .into(),
                // The installed build turns out to be below the minimum.
                GalacticWarEvent::MinimumCheckChanged {
                    below_minimum: true,
                }
                .into(),
                GalacticWarEvent::StatusChanged {
                    status: GalacticWarStatus::Downloading {
                        version: "v2026.04.04.1".into(),
                        downloaded_bytes: 12_000_000,
                        total_bytes: 46_340_472,
                    },
                }
                .into(),
                GalacticWarEvent::StatusChanged {
                    status: GalacticWarStatus::Installing {
                        version: "v2026.04.04.1".into(),
                    },
                }
                .into(),
                GalacticWarEvent::InstallationChanged {
                    version: Some("v2026.04.04.1".into()),
                }
                .into(),
                GalacticWarEvent::MinimumCheckChanged {
                    below_minimum: false,
                }
                .into(),
                GalacticWarEvent::StatusChanged {
                    status: GalacticWarStatus::Running,
                }
                .into(),
                GalacticWarEvent::StatisticsStatusChanged {
                    status: StatisticsStatus::Loading,
                }
                .into(),
                GalacticWarEvent::StatisticsLoaded {
                    statistics: GalacticWarStatistics {
                        alltime: GalacticWarAlltime { num_players: 82 },
                        season: GalacticWarSeason {
                            started_at: "2026-03-15 22:55:15".into(),
                            name: "Testing Season 4".into(),
                            num_players: 16,
                            num_online_players: 4,
                            num_battles: 28,
                            num_planets: 1000,
                            num_factions: 4,
                            ..Default::default()
                        },
                        factions: vec![GalacticWarFaction {
                            id: 0,
                            name: "UEF".into(),
                            long_name: "United Earth Federation".into(),
                            num_planets: 254,
                            ..Default::default()
                        }],
                    },
                }
                .into(),
                // A later failure keeps the season that was already read.
                GalacticWarEvent::StatisticsStatusChanged {
                    status: StatisticsStatus::Failed {
                        reason: "gateway unreachable".into(),
                    },
                }
                .into(),
            ],
        ),
        // ── settings ─────────────────────────────────────────────────────
        case(
            "settings apply each group",
            vec![
                SettingsEvent::ThemeChanged {
                    theme: Theme::ForgeLight,
                }
                .into(),
                SettingsEvent::GamePathChanged {
                    path: "C:/games/FA/bin/ForgedAlliance.exe".into(),
                }
                .into(),
                // Deliberately untrimmed: a path pasted out of a file manager
                // arrives this way, and resolving it literally finds nothing.
                SettingsEvent::PathsChanged {
                    preferences: PathPreferences {
                        vault_dir: "  D:/faf/vault  ".into(),
                        maps_dir: "D:/faf/vault/maps".into(),
                        mods_dir: String::new(),
                        replays_dir: " D:/faf/replays ".into(),
                        game_prefs_path: String::new(),
                        map_generator_dir: String::new(),
                        java_path: String::new(),
                        wine_prefix: " /home/player/.wine ".into(),
                    },
                }
                .into(),
                // Additive and case-insensitive: a second run that keeps
                // nothing must not release what the first one kept, and the
                // same map arriving twice must not be listed twice.
                SettingsEvent::KeptGeneratedMaps {
                    map_names: vec![
                        "  neroxis_map_generator_1.15.2_keepme  ".into(),
                        "NEROXIS_MAP_GENERATOR_1.15.2_KEEPME".into(),
                        "   ".into(),
                    ],
                }
                .into(),
                SettingsEvent::KeptGeneratedMaps {
                    map_names: vec!["neroxis_map_generator_1.15.2_alsokeep".into()],
                }
                .into(),
                // The opposite rule to the two above: one whole veto
                // selection, replaced rather than merged, because clearing
                // every veto has to be able to clear it. The second event is
                // the server capping the first, which is the only time it
                // sends one back.
                SettingsEvent::MatchmakerVetoesChanged {
                    vetoes: vec![
                        PlayerVeto {
                            matchmaker_queue_map_pool_id: 4,
                            map_pool_map_version_id: 91,
                            veto_tokens_applied: 2,
                        },
                        PlayerVeto {
                            matchmaker_queue_map_pool_id: 4,
                            map_pool_map_version_id: 92,
                            veto_tokens_applied: 1,
                        },
                    ],
                }
                .into(),
                SettingsEvent::MatchmakerVetoesChanged {
                    vetoes: vec![PlayerVeto {
                        matchmaker_queue_map_pool_id: 4,
                        map_pool_map_version_id: 91,
                        veto_tokens_applied: 1,
                    }],
                }
                .into(),
                SettingsEvent::MapPoolsSeen {
                    seen: vec![faf_domain::state::settings::MapPoolsSeen {
                        queue_name: "ladder1v1".into(),
                        assignments: vec![91, 92],
                    }],
                }
                .into(),
                // Replaced whole as well: the service works out the order and
                // the cap, the reducer only stores the result. The empty
                // string is a deliberate choice of no avatar.
                SettingsEvent::AvatarHistoryChanged {
                    history: vec![
                        "https://example.test/cup.png".into(),
                        "https://example.test/veteran.png".into(),
                        String::new(),
                    ],
                }
                .into(),
                SettingsEvent::SocialChanged {
                    preferences: SocialPreferences {
                        player_notes: vec![
                            PlayerNote {
                                player_id: 42,
                                login: "Old Aurora".into(),
                                note: "old".into(),
                            },
                            PlayerNote {
                                player_id: 7,
                                login: "Ada".into(),
                                note: "Map specialist".into(),
                            },
                            PlayerNote {
                                player_id: 42,
                                login: "Aurora".into(),
                                note: "Reliable teammate".into(),
                            },
                            PlayerNote {
                                player_id: 0,
                                login: "Invalid".into(),
                                note: "drop me".into(),
                            },
                        ],
                        // Both reducers tidy these the same way: tags trimmed,
                        // folded and deduplicated, the later note for a replay
                        // winning, and an empty one dropped.
                        replay_notes: vec![
                            ReplayNote {
                                replay_id: 9,
                                comment: " first ".into(),
                                tags: vec!["old".into()],
                            },
                            ReplayNote {
                                replay_id: 12,
                                comment: String::new(),
                                tags: vec![" Lots  finals ".into(), "LOTS FINALS".into(), " ".into()],
                            },
                            ReplayNote {
                                replay_id: 9,
                                comment: "  Comeback  ".into(),
                                tags: Vec::new(),
                            },
                            ReplayNote {
                                replay_id: 30,
                                comment: " ".into(),
                                tags: Vec::new(),
                            },
                        ],
                    },
                }
                .into(),
                SettingsEvent::DiscordChanged {
                    preferences: DiscordPreferences {
                        enabled: false,
                        disallow_joins: true,
                    },
                }
                .into(),
                SettingsEvent::DebugChanged {
                    preferences: DebugPreferences {
                        ice_adapter_debug_window: true,
                        ice_adapter_info_window: false,
                        ice_adapter_console_window: true,
                        map_generator_window: true,
                    },
                }
                .into(),
                SettingsEvent::BrowsingChanged {
                    preferences: Box::new(BrowsingPreferences {
                        custom_games_view: CustomGameView::List,
                        replays_view: CustomGameView::List,
                        live_replay_view: CustomGameView::Tiles,
                        custom_games_browser: CustomGameBrowserPreferences {
                            sort: CustomGameSort::Age,
                            sort_reversed: true,
                            hide_private: true,
                            hide_modded: true,
                            hide_unranked: false,
                            hide_foes: false,
                            apply_filters: true,
                            rules: vec![
                                CustomGameFilterRule {
                                    field: CustomGameFilterField::Host,
                                    constraint: CustomGameFilterConstraint::Contains,
                                    value: "  noisy  ".into(),
                                },
                                CustomGameFilterRule {
                                    field: CustomGameFilterField::Host,
                                    constraint: CustomGameFilterConstraint::Contains,
                                    value: "NOISY".into(),
                                },
                            ],
                            column_widths: vec![320, 180, 90, 110, 80],
                            column_order: Vec::new(),
                            detail_width: 360,
                            detail_hidden: false,
                        },
                        matchmaker_unselected_queues: vec![
                            "  ladder_1v1 ".into(),
                            "LADDER_1V1".into(),
                        ],
                        matchmaker_factions: vec!["cybran".into(), "unknown".into()],
                        matchmaker_chat_width: 420,
                        live_replay_filters: LiveReplayFilters {
                            search: "  tournament  ".into(),
                            game_type: " matchmaker ".into(),
                            featured_mod: " faf ".into(),
                            active_players: "04".into(),
                            max_players: "999".into(),
                            hide_modded: true,
                            hide_single_player: false,
                            hide_unranked: true,
                            friends_only: true,
                        },
                        host_game: HostGamePreferences {
                            title: " Friday game ".into(),
                            featured_mod: " faf ".into(),
                            visibility: "friends".into(),
                            map: " scmp_009 ".into(),
                            password_enabled: true,
                            password: " secret ".into(),
                            enforce_rating_range: true,
                            rating_min: Some(700),
                            rating_max: Some(1_700),
                        },
                        // Its own block, normalised the same way: one dialog's
                        // setup must never land in the other's.
                        host_coop: HostGamePreferences {
                            title: " Operation Ivy ".into(),
                            featured_mod: "coop".into(),
                            visibility: "public".into(),
                            map: " SCCA_Coop_A03.v0023 ".into(),
                            password_enabled: false,
                            password: String::new(),
                            enforce_rating_range: false,
                            rating_min: Some(800),
                            rating_max: Some(1_500),
                        },
                        favorite_maps: vec!["adaptive_tabula.v0006".into()],
                        favorite_mods: vec!["eco_graph".into()],
                        map_vault_preset: "recommended".into(),
                        map_vault_sort: "newest".into(),
                        mod_vault_sort: "rating".into(),
                        vault_page_size: 48,
                        replay_list_columns: vec![64, 240, 150],
                        replay_list_order: Vec::new(),
                        live_replay_columns: vec![120, 200],
                        live_replay_order: Vec::new(),
                        coop_board_columns: Vec::new(),
                        matchmaker_recent_columns: Vec::new(),
                        matchmaker_recent_order: Vec::new(),
                        matchmaker_invite_columns: Vec::new(),
                        matchmaker_invite_order: Vec::new(),
                        mod_vault_preset: "recommended".into(),
                        mod_presets: Vec::new(),
                        leaderboard_rating_columns: vec!["games".into(), "updated".into()],
                        leaderboard_include_former_names: false,
                        replay_vault_player: String::new(),
                        replay_chat_channel: String::new(),
                        replay_chat_transfers: Default::default(),
                        legacy_storage_migrated: true,
                        remembered_filters: std::collections::BTreeMap::from([(
                            "installedMods".into(),
                            r#"{"search":"ui"}"#.into(),
                        )]),
                    }),
                }
                .into(),
            ],
        ),
    ];
    cases.extend(transition_cases());
    cases
}

/// Cases that reach a variant from a state where its arm has something to
/// decide: a load that fails after one that succeeded, an answer for a request
/// that has since been replaced, a write whose field another arm reads.
///
/// These replaced entries in `UNCOVERED_EVENT_VARIANTS`. A single event from
/// the default state would have proved only that a field was assigned, which
/// is the one thing a twin rarely gets wrong.
fn transition_cases() -> Vec<Case> {
    vec![
        // ── lobby ────────────────────────────────────────────────────────
        case(
            "a join the server refused is forgotten on reconnect, a launch is not",
            vec![
                LobbyEvent::Connected.into(),
                LobbyEvent::Joining {
                    id: 7,
                    prepared: false,
                }
                .into(),
                LobbyEvent::JoinFailed {
                    id: 7,
                    reason: "wrong password".into(),
                }
                .into(),
                // A cancel has nothing to call off once the server has said no.
                LobbyEvent::JoinCancelled.into(),
                LobbyEvent::Connecting.into(),
                LobbyEvent::Connected.into(),
                LobbyEvent::Joining {
                    id: 8,
                    prepared: true,
                }
                .into(),
                LobbyEvent::Launching {
                    launch: game_launch(8),
                }
                .into(),
                // The launch order outlives the socket, like a running game.
                LobbyEvent::Connecting.into(),
                // And a cancel cannot take it back: it is past preparation.
                LobbyEvent::JoinCancelled.into(),
                LobbyEvent::Connected.into(),
                LobbyEvent::InGame.into(),
                LobbyEvent::GameTerminated.into(),
            ],
        ),
        case(
            "the open and live lists take a snapshot, then changes, and survive a reconnect",
            vec![
                LobbyEvent::Connected.into(),
                LobbyEvent::GamesUpdated {
                    games: vec![lobby_game(1, "Setons 4v4", 6), lobby_game(3, "Gap 1v1", 1)],
                }
                .into(),
                // An insert lands in id order, an update replaces in place, and
                // a removal of a lobby this client never saw is not an error.
                LobbyEvent::GamesChanged {
                    upserted: vec![lobby_game(2, "Dual Gap", 2), lobby_game(3, "Gap 1v1", 2)],
                    removed: vec![1, 99],
                }
                .into(),
                LobbyEvent::LiveGamesUpdated {
                    games: vec![lobby_game(5, "Running", 8)],
                }
                .into(),
                LobbyEvent::LiveGamesChanged {
                    upserted: vec![lobby_game(4, "Just launched", 4)],
                    removed: vec![5],
                }
                .into(),
                // A blip keeps both lists for the replacement socket to resend.
                LobbyEvent::Connecting.into(),
                // A snapshot replaces whatever the changes built.
                LobbyEvent::GamesUpdated {
                    games: vec![lobby_game(6, "Fresh", 1)],
                }
                .into(),
                LobbyEvent::Disconnected.into(),
            ],
        ),
        case(
            "a party and its vetoes outlive a reconnect but not a disconnect",
            vec![
                LobbyEvent::Connected.into(),
                LobbyEvent::PartyUpdated {
                    party: PartyState {
                        owner_id: Some(7),
                        members: vec![
                            PartyMember {
                                player_id: 7,
                                name: "Ada".into(),
                                factions: vec!["uef".into(), "cybran".into()],
                            },
                            PartyMember {
                                player_id: 9,
                                name: "Bob".into(),
                                factions: vec!["aeon".into()],
                            },
                        ],
                    },
                }
                .into(),
                LobbyEvent::VetoesUpdated {
                    vetoes: vec![PlayerVeto {
                        matchmaker_queue_map_pool_id: 4,
                        map_pool_map_version_id: 91,
                        veto_tokens_applied: 2,
                    }],
                }
                .into(),
                LobbyEvent::MatchmakingUpdated {
                    state: MatchmakingState::Searching {
                        queue_names: vec!["tmm2v2".into()],
                    },
                }
                .into(),
                // The search ends with the socket; the party is the server's
                // to resend, so it stays for the blip.
                LobbyEvent::Connecting.into(),
                LobbyEvent::Connected.into(),
                // A party of one is still a snapshot: replaced, not merged.
                LobbyEvent::PartyUpdated {
                    party: PartyState {
                        owner_id: Some(7),
                        members: vec![PartyMember {
                            player_id: 7,
                            name: "Ada".into(),
                            factions: vec!["uef".into()],
                        }],
                    },
                }
                .into(),
                LobbyEvent::VetoesUpdated { vetoes: vec![] }.into(),
                LobbyEvent::VetoesUpdated {
                    vetoes: vec![PlayerVeto {
                        matchmaker_queue_map_pool_id: 4,
                        map_pool_map_version_id: 92,
                        veto_tokens_applied: 1,
                    }],
                }
                .into(),
                LobbyEvent::Disconnected.into(),
            ],
        ),
        case(
            "an avatar list that failed and a refused selection both clear on the next load",
            vec![
                LobbyEvent::AvatarsLoading.into(),
                LobbyEvent::AvatarsLoadFailed {
                    reason: "not authenticated".into(),
                }
                .into(),
                LobbyEvent::AvatarSelectionStarted.into(),
                LobbyEvent::AvatarSelectionFailed {
                    reason: "avatar is not yours".into(),
                }
                .into(),
                // A fresh load resets the selection status too: the refusal
                // was about a list that has just been replaced.
                LobbyEvent::AvatarsLoading.into(),
                LobbyEvent::AvatarsLoaded {
                    avatars: vec![AvailableAvatar {
                        url: "https://content.test/cup.png".into(),
                        tooltip: "Cup winner".into(),
                    }],
                }
                .into(),
                LobbyEvent::AvatarSelectionStarted.into(),
                LobbyEvent::AvatarSelectionFailed {
                    reason: "timed out".into(),
                }
                .into(),
                // A failed reload keeps the list it already has.
                LobbyEvent::AvatarsLoadFailed {
                    reason: "offline".into(),
                }
                .into(),
                LobbyEvent::Disconnected.into(),
            ],
        ),
        // ── leaderboard ──────────────────────────────────────────────────
        case(
            "the board catalogue fails, then loads over the failure",
            vec![
                LeaderboardEvent::CatalogLoading.into(),
                LeaderboardEvent::CatalogLoadFailed {
                    reason: "503".into(),
                }
                .into(),
                LeaderboardEvent::CatalogLoading.into(),
                LeaderboardEvent::CatalogLoaded {
                    rating_leaderboards: vec![RatingLeaderboard {
                        id: 1,
                        technical_name: "global".into(),
                        description: String::new(),
                    }],
                    leagues: vec![League {
                        id: 3,
                        technical_name: "1v1_league".into(),
                        description: String::new(),
                    }],
                }
                .into(),
                // A failed refresh keeps the boards it already has.
                LeaderboardEvent::CatalogLoadFailed {
                    reason: "offline".into(),
                }
                .into(),
            ],
        ),
        case(
            "a page that fails to load keeps the last page and takes the new query",
            vec![
                LeaderboardEvent::RatingsLoading {
                    query: RatingQuery::default(),
                }
                .into(),
                LeaderboardEvent::RatingsLoaded {
                    query: RatingQuery::default(),
                    page: RatingPage {
                        entries: vec![leaderboard_entry(11, 1), leaderboard_entry(12, 2)],
                        page: 1,
                        page_size: 100,
                        total_pages: 40,
                        total_results: Some(3_987),
                    },
                }
                .into(),
                LeaderboardEvent::CrossRatingsLoaded {
                    query: RatingQuery::default(),
                    ratings: vec![PlayerRatings {
                        player_id: 11,
                        ratings: vec![BoardRating {
                            leaderboard: "ladder_1v1".into(),
                            rating: 1_900,
                            games_played: 400,
                        }],
                    }],
                }
                .into(),
                // Paging on clears the other boards; the failure that follows
                // leaves the first page drawn under the second page's query,
                // which is what the pager reads to offer a retry.
                LeaderboardEvent::RatingsLoading {
                    query: RatingQuery {
                        page: 2,
                        ..RatingQuery::default()
                    },
                }
                .into(),
                LeaderboardEvent::RatingsLoadFailed {
                    reason: "timed out".into(),
                }
                .into(),
                LeaderboardEvent::RatingsLoaded {
                    query: RatingQuery {
                        page: 2,
                        ..RatingQuery::default()
                    },
                    page: RatingPage {
                        entries: vec![leaderboard_entry(13, 101)],
                        page: 2,
                        page_size: 100,
                        total_pages: 40,
                        total_results: Some(3_987),
                    },
                }
                .into(),
            ],
        ),
        case(
            "a league's seasons load, one season is opened, and a late answer moves the selection",
            vec![
                LeaderboardEvent::ModeChanged {
                    mode: LeaderboardMode::Leagues,
                }
                .into(),
                LeaderboardEvent::SeasonsLoading { league_id: 3 }.into(),
                LeaderboardEvent::SeasonsLoaded {
                    league_id: 3,
                    seasons: vec![league_season(31, 3, 11), league_season(32, 3, 12)],
                }
                .into(),
                LeaderboardEvent::SeasonLoading { season_id: 32 }.into(),
                LeaderboardEvent::SeasonLoaded {
                    season_id: 32,
                    leaderboard: SeasonLeaderboard {
                        entries: vec![leaderboard_entry(11, 1)],
                        tiers: vec![LeaderboardTier {
                            division: "gold".into(),
                            subdivision: "I".into(),
                            division_order: 31,
                            highest_score: 10,
                            image_url: None,
                            medium_image_url: None,
                        }],
                    },
                }
                .into(),
                // The next season is asked for and fails: the last one's table
                // stays rather than an empty page.
                LeaderboardEvent::SeasonLoading { season_id: 31 }.into(),
                LeaderboardEvent::SeasonLoadFailed {
                    reason: "503".into(),
                }
                .into(),
                // An answer is not checked against the request: the season it
                // names becomes the selection. Pinned so both twins agree.
                LeaderboardEvent::SeasonLoading { season_id: 31 }.into(),
                LeaderboardEvent::SeasonLoaded {
                    season_id: 32,
                    leaderboard: SeasonLeaderboard {
                        entries: vec![],
                        tiers: vec![],
                    },
                }
                .into(),
                // Changing league drops all of it.
                LeaderboardEvent::SeasonsLoading { league_id: 4 }.into(),
            ],
        ),
        // ── maps ─────────────────────────────────────────────────────────
        case(
            "a map install fails, then lands and forgets the preview markers",
            vec![
                MapsEvent::InstalledLoading.into(),
                MapsEvent::InstalledLoaded {
                    maps: vec![installed_map("scmp_009")],
                }
                .into(),
                MapsEvent::LocalPreviewsLoaded {
                    previews: [
                        (
                            "scmp_009".to_string(),
                            LocalMapPreview {
                                small: Some("data:image/png;base64,c21hbGw=".into()),
                                large: None,
                            },
                        ),
                        // "Looked, found nothing", for a folder that is about
                        // to get art.
                        ("setons_clutch".to_string(), LocalMapPreview::default()),
                    ]
                    .into_iter()
                    .collect(),
                }
                .into(),
                MapsEvent::Installing {
                    folder_name: "setons_clutch.v0003".into(),
                }
                .into(),
                MapsEvent::InstallFailed {
                    reason: "the download was cut off".into(),
                }
                .into(),
                MapsEvent::Installing {
                    folder_name: "setons_clutch.v0003".into(),
                }
                .into(),
                MapsEvent::Installed {
                    installed: vec![installed_map("scmp_009"), installed_map("setons_clutch.v0003")],
                }
                .into(),
                MapsEvent::LocalPreviewsLoaded {
                    previews: [(
                        "setons_clutch".to_string(),
                        LocalMapPreview {
                            small: Some("data:image/png;base64,c2V0b25z".into()),
                            large: None,
                        },
                    )]
                    .into_iter()
                    .collect(),
                }
                .into(),
                MapsEvent::UninstallFailed {
                    reason: "the folder is in use".into(),
                }
                .into(),
                MapsEvent::Uninstalled {
                    installed: vec![installed_map("setons_clutch.v0003")],
                }
                .into(),
                // A failed rescan keeps the folder list it has.
                MapsEvent::InstalledLoading.into(),
                MapsEvent::InstalledLoadFailed {
                    reason: "the maps folder is gone".into(),
                }
                .into(),
            ],
        ),
        case(
            "matchmaker pools accumulate per queue and survive a failed fetch",
            vec![
                MapsEvent::MatchmakerPoolsLoading.into(),
                MapsEvent::MatchmakerPoolsLoaded {
                    queue_name: "ladder1v1".into(),
                    pools: vec![matchmaker_pool(1, "Ladder 0-1300")],
                }
                .into(),
                MapsEvent::MatchmakerPoolsLoading.into(),
                MapsEvent::MatchmakerPoolsLoaded {
                    queue_name: "tmm2v2".into(),
                    pools: vec![matchmaker_pool(2, "2v2 all ratings")],
                }
                .into(),
                MapsEvent::MatchmakerPoolsLoading.into(),
                MapsEvent::MatchmakerPoolsLoadFailed {
                    reason: "503".into(),
                }
                .into(),
                // A queue's pools are replaced whole on the next answer.
                MapsEvent::MatchmakerPoolsLoaded {
                    queue_name: "ladder1v1".into(),
                    pools: vec![],
                }
                .into(),
            ],
        ),
        // ── mods ─────────────────────────────────────────────────────────
        case(
            "the mod catalogue loads, and a failed reload keeps it beside the browsed page",
            vec![
                ModsEvent::VaultLoading.into(),
                ModsEvent::VaultLoaded {
                    mods: vec![vault_mod(1, "AAA"), vault_mod(2, "BBB")],
                }
                .into(),
                ModsEvent::VaultSearched {
                    mods: vec![vault_mod(2, "BBB")],
                    query: faf_domain::protocol::vault_query::ModVaultQuery::default(),
                    total_pages: Some(1),
                    total_records: Some(1),
                }
                .into(),
                ModsEvent::VaultLoading.into(),
                ModsEvent::VaultLoadFailed {
                    reason: "503".into(),
                }
                .into(),
                ModsEvent::VaultLoaded {
                    mods: vec![vault_mod(1, "AAA")],
                }
                .into(),
            ],
        ),
        case(
            "a mod is installed after a failure, toggled after a failure, and removed",
            vec![
                ModsEvent::InstalledLoading.into(),
                ModsEvent::InstalledLoaded {
                    mods: vec![installed_mod("AAA", true)],
                }
                .into(),
                ModsEvent::Installing { uid: "BBB".into() }.into(),
                ModsEvent::InstallFailed {
                    reason: "the archive is corrupt".into(),
                }
                .into(),
                ModsEvent::Installing { uid: "BBB".into() }.into(),
                ModsEvent::Installed {
                    installed: vec![installed_mod("AAA", true), installed_mod("BBB", false)],
                }
                .into(),
                // Toggling has its own status: the install outcome stays.
                ModsEvent::Toggling { uid: "AAA".into() }.into(),
                ModsEvent::ToggleFailed {
                    reason: "game.prefs is read-only".into(),
                }
                .into(),
                ModsEvent::Toggling { uid: "AAA".into() }.into(),
                ModsEvent::Toggled {
                    installed: vec![installed_mod("AAA", false), installed_mod("BBB", false)],
                }
                .into(),
                ModsEvent::UninstallFailed {
                    reason: "the folder is in use".into(),
                }
                .into(),
                ModsEvent::Uninstalled {
                    installed: vec![installed_mod("BBB", false)],
                }
                .into(),
            ],
        ),
        // ── replays ──────────────────────────────────────────────────────
        case(
            "the local library loads, takes a download at the top, and loses a deleted file",
            vec![
                ReplayEvent::LocalLoading.into(),
                ReplayEvent::LocalLoaded {
                    replays: vec![local_replay(30), local_replay(20)],
                }
                .into(),
                // The same file again moves to the front rather than doubling.
                ReplayEvent::VaultDownloadStarted { uid: 20 }.into(),
                ReplayEvent::VaultDownloaded {
                    uid: 20,
                    replay: local_replay(20),
                }
                .into(),
                ReplayEvent::LocalDeleted {
                    path: local_replay(30).path,
                }
                .into(),
                ReplayEvent::LocalLoading.into(),
                ReplayEvent::LocalLoadFailed {
                    reason: "the replay folder is unreadable".into(),
                }
                .into(),
                // A delete that lands after a failed rescan still reports the
                // list it edited as current.
                ReplayEvent::LocalDeleted {
                    path: local_replay(20).path,
                }
                .into(),
            ],
        ),
        case(
            "replay details answer out of order and keep the newest request loading",
            vec![
                ReplayEvent::DetailsLoading { uid: 11 }.into(),
                ReplayEvent::DetailsLoading { uid: 12 }.into(),
                // The earlier answer is stored, but 12 is still the one asked.
                ReplayEvent::DetailsLoaded {
                    uid: 11,
                    details: ReplayDetails {
                        sim_seconds: 1_200,
                        game_version: Some(3_810),
                        ..ReplayDetails::default()
                    },
                }
                .into(),
                ReplayEvent::DetailsFailed {
                    uid: 12,
                    reason: "the replay body is truncated".into(),
                }
                .into(),
                // A failure for something no longer asked about keeps the
                // newer request's spinner, and is not shown as its failure.
                ReplayEvent::DetailsLoading { uid: 13 }.into(),
                ReplayEvent::DetailsFailed {
                    uid: 12,
                    reason: "late".into(),
                }
                .into(),
                ReplayEvent::DetailsLoaded {
                    uid: 13,
                    details: ReplayDetails::default(),
                }
                .into(),
                // 14 fails; 11 answering again afterwards keeps 14's failure.
                ReplayEvent::DetailsLoading { uid: 14 }.into(),
                ReplayEvent::DetailsFailed {
                    uid: 14,
                    reason: "not uploaded yet".into(),
                }
                .into(),
                ReplayEvent::DetailsLoaded {
                    uid: 11,
                    details: ReplayDetails::default(),
                }
                .into(),
            ],
        ),
        case(
            "a failed vault search keeps the page and query it last showed",
            vec![
                ReplayEvent::FeaturedModsLoaded {
                    mods: vec!["faf".into(), "coop".into()],
                }
                .into(),
                ReplayEvent::VaultLoading.into(),
                ReplayEvent::VaultLoaded {
                    replays: vec![vault_replay(70)],
                    query: Box::new(ReplayQuery {
                        page: 3,
                        ..Default::default()
                    }),
                    has_more: true,
                    total_pages: Some(5),
                    total_records: Some(240),
                }
                .into(),
                ReplayEvent::VaultLoading.into(),
                ReplayEvent::VaultLoadFailed {
                    reason: "the filter was rejected".into(),
                }
                .into(),
                ReplayEvent::FeaturedModsLoaded {
                    mods: vec!["faf".into()],
                }
                .into(),
            ],
        ),
        // ── player card ──────────────────────────────────────────────────
        case(
            "rating history pages merge by time, and another player starts from nothing",
            vec![
                PlayerCardEvent::Loading {
                    login: "Ada".into(),
                }
                .into(),
                PlayerCardEvent::HistoryLoading {
                    query: history_query(7),
                    append: false,
                }
                .into(),
                // Two points at one instant inside one page: the first stands.
                PlayerCardEvent::HistoryLoaded {
                    query: history_query(7),
                    page: RatingHistoryPage {
                        points: vec![
                            history_point("2026-01-02T00:00:00Z", 1_500.0),
                            history_point("2026-01-01T00:00:00Z", 1_450.0),
                            history_point("2026-01-02T00:00:00Z", 1_999.0),
                        ],
                        maximum: Some(history_point("2025-06-01T00:00:00Z", 1_800.0)),
                        page: 1,
                        total_pages: 2,
                    },
                    append: false,
                }
                .into(),
                PlayerCardEvent::HistoryLoading {
                    query: history_query(7),
                    append: true,
                }
                .into(),
                // An appended page overlapping the first: the point already
                // held is the one kept, and a page with no maximum keeps the
                // known one. A zero page count is read as one.
                PlayerCardEvent::HistoryLoaded {
                    query: history_query(7),
                    page: RatingHistoryPage {
                        points: vec![
                            history_point("2026-01-03T00:00:00Z", 1_520.0),
                            history_point("2026-01-02T00:00:00Z", 1_111.0),
                        ],
                        maximum: None,
                        page: 2,
                        total_pages: 0,
                    },
                    append: true,
                }
                .into(),
                PlayerCardEvent::HistoryLoading {
                    query: history_query(7),
                    append: true,
                }
                .into(),
                PlayerCardEvent::HistoryLoadFailed {
                    reason: "timed out".into(),
                }
                .into(),
                // Another player: the points and the peak both go.
                PlayerCardEvent::Loading {
                    login: "Bob".into(),
                }
                .into(),
                PlayerCardEvent::HistoryLoading {
                    query: history_query(9),
                    append: false,
                }
                .into(),
            ],
        ),
        // ── reporting ────────────────────────────────────────────────────
        case(
            "a refused report can be retried, and the history survives until the form closes",
            vec![
                ReportingEvent::Opened {
                    player_id: 7,
                    login: "Bob".into(),
                }
                .into(),
                ReportingEvent::Submitting.into(),
                ReportingEvent::Failed {
                    reason: "the game id does not exist".into(),
                }
                .into(),
                ReportingEvent::HistoryLoading.into(),
                ReportingEvent::HistoryLoaded {
                    reports: vec![ModerationReportSummary {
                        id: 8,
                        create_time: "2026-08-10T18:30:00Z".into(),
                        offenders: vec!["Bob".into()],
                        game_id: None,
                        description: "Abusive chat".into(),
                        moderator: String::new(),
                        moderator_notice: String::new(),
                        status: "OPEN".into(),
                    }],
                }
                .into(),
                // Reopening for someone else clears the failure, not the list.
                ReportingEvent::Opened {
                    player_id: 9,
                    login: "Cid".into(),
                }
                .into(),
                ReportingEvent::HistoryLoading.into(),
                ReportingEvent::HistoryFailed {
                    reason: "offline".into(),
                }
                .into(),
                ReportingEvent::Closed.into(),
            ],
        ),
        // ── reviews ──────────────────────────────────────────────────────
        case(
            "reviews for a subject left behind are dropped, and a failed save keeps the list",
            vec![
                ReviewsEvent::Opened {
                    target: review_target(42, "Seton's Clutch"),
                }
                .into(),
                ReviewsEvent::Loading.into(),
                // The same map reopened under another name from another view
                // is another request: the first answer no longer matches it.
                ReviewsEvent::Opened {
                    target: review_target(42, "Setons Clutch"),
                }
                .into(),
                ReviewsEvent::Loading.into(),
                ReviewsEvent::Loaded {
                    target: review_target(42, "Seton's Clutch"),
                    reviews: vec![review(1, 5, "Bob")],
                }
                .into(),
                ReviewsEvent::LoadFailed {
                    reason: "503".into(),
                }
                .into(),
                ReviewsEvent::Loading.into(),
                ReviewsEvent::Loaded {
                    target: review_target(42, "Setons Clutch"),
                    reviews: vec![review(1, 5, "Bob"), review(2, 2, "Cid")],
                }
                .into(),
                ReviewsEvent::Saving.into(),
                ReviewsEvent::SaveFailed {
                    reason: "you have not played this map".into(),
                }
                .into(),
                ReviewsEvent::Closed.into(),
                // An answer that lands after the panel closed has nowhere to go.
                ReviewsEvent::Loaded {
                    target: review_target(42, "Setons Clutch"),
                    reviews: vec![review(3, 4, "Dan")],
                }
                .into(),
            ],
        ),
        // ── notifications / social ──────────────────────────────────────
        case(
            "a notification is read, re-raised unread at the top, and everything is cleared",
            vec![
                NotificationEvent::Added {
                    notification: notification("n1", NotificationKind::Mention),
                }
                .into(),
                NotificationEvent::Added {
                    notification: notification("n2", NotificationKind::FriendOnline),
                }
                .into(),
                NotificationEvent::Read { id: "n1".into() }.into(),
                // Unknown ids change nothing.
                NotificationEvent::Read { id: "n9".into() }.into(),
                // The same id again replaces the read copy rather than adding
                // a second one.
                NotificationEvent::Added {
                    notification: notification("n1", NotificationKind::Mention),
                }
                .into(),
                NotificationEvent::Read { id: "n2".into() }.into(),
                NotificationEvent::Cleared.into(),
            ],
        ),
        case(
            "relations are set case-insensitively, offline players go by any spelling, and a disconnect clears both",
            vec![
                SocialEvent::RelationsUpdated {
                    friends: vec!["Bob".into(), "Ada".into()],
                    foes: vec!["Cid".into()],
                }
                .into(),
                SocialEvent::PlayersSeen {
                    players: vec![
                        PlayerProfile {
                            id: 2,
                            login: "Bob".into(),
                            ..Default::default()
                        },
                        PlayerProfile {
                            id: 1,
                            login: "Ada".into(),
                            ..Default::default()
                        },
                    ],
                }
                .into(),
                // Befriending a foe takes them off the foe list, whatever the
                // capitalisation the menu used.
                SocialEvent::RelationSet {
                    login: "cid".into(),
                    relation: Relation::Friend,
                    member: true,
                }
                .into(),
                SocialEvent::RelationSet {
                    login: "BOB".into(),
                    relation: Relation::Friend,
                    member: false,
                }
                .into(),
                SocialEvent::RelationSet {
                    login: "Ada".into(),
                    relation: Relation::Foe,
                    member: true,
                }
                .into(),
                // The server's offline notice need not match the login's case.
                SocialEvent::PlayersRemoved {
                    logins: vec!["ada".into()],
                }
                .into(),
                SocialEvent::Cleared.into(),
            ],
        ),
        case(
            "a login lookup keeps one answer per login, the newest last",
            vec![
                SocialEvent::LoginLookedUp {
                    login: "Ada".into(),
                    id: Some(1),
                }
                .into(),
                SocialEvent::LoginLookedUp {
                    login: "qai".into(),
                    id: None,
                }
                .into(),
                // Another spelling of a login already answered replaces it.
                SocialEvent::LoginLookedUp {
                    login: "ADA".into(),
                    id: Some(1),
                }
                .into(),
            ],
        ),
        // ── settings ─────────────────────────────────────────────────────
        case(
            "appearance values from outside their range are clamped on the way in",
            vec![SettingsEvent::AppearanceChanged {
                preferences: AppearancePreferences {
                    density: UiDensity::Compact,
                    reduce_motion: true,
                    ui_scale: 400,
                    game_tile_columns: 9,
                    sidebar_width: 20,
                    hover_panels: false,
                    hover_open_delay_ms: 9_000,
                    hover_close_delay_ms: 2_500,
                    replay_flags: true,
                    background_image: String::new(),
                    background_dim: 60,
                },
            }
            .into()],
        ),
        case(
            "every preference group is replaced whole, and a reload replaces them all",
            vec![
                SettingsEvent::GeneralChanged {
                    preferences: GeneralPreferences {
                        start_page: Tab::Play,
                        auto_login: false,
                        remember_typed_entries: true,
                        filter_memory: FilterMemory::Restart,
                    },
                }
                .into(),
                SettingsEvent::NotificationsChanged {
                    preferences: NotificationPreferences {
                        enabled: false,
                        ..NotificationPreferences::default()
                    },
                }
                .into(),
                SettingsEvent::ChatChanged {
                    preferences: Box::new(ChatPreferences {
                        font_size: 15,
                        show_timestamps: false,
                        ..ChatPreferences::default()
                    }),
                }
                .into(),
                SettingsEvent::GameChanged {
                    preferences: GamePreferences {
                        launch_wrapper: "gamemoderun".into(),
                        cache_size_alert_gb: None,
                        ..GamePreferences::default()
                    },
                }
                .into(),
                SettingsEvent::ConnectivityChanged {
                    preferences: ConnectivityPreferences {
                        adapter: IceAdapter::Go,
                        ..ConnectivityPreferences::default()
                    },
                }
                .into(),
                SettingsEvent::UpdatesChanged {
                    preferences: UpdatePreferences {
                        automatic: false,
                        ..UpdatePreferences::default()
                    },
                }
                .into(),
                SettingsEvent::MapGeneratorChanged {
                    preferences: Box::new(GeneratorOptions {
                        version: Some("1.22.1".into()),
                        spawn_count: Some(6),
                        ..GeneratorOptions::default()
                    }),
                }
                .into(),
                SettingsEvent::ReplayGamePathChanged {
                    path: "D:/FA-replay/bin/ForgedAlliance.exe".into(),
                }
                .into(),
                SettingsEvent::CacheInfoUpdated {
                    info: GameCacheInfo {
                        total_size_bytes: 1_500_000_000.0,
                        total_files: 3,
                        versions: vec![CachedGameVersion {
                            name: "faf".into(),
                            version: 3_810,
                            file_count: 3,
                            size_bytes: 1_500_000_000.0,
                            url: None,
                        }],
                    },
                }
                .into(),
                SettingsEvent::KeptGeneratedMaps {
                    map_names: vec!["neroxis_map_generator_1.22.1_keep".into()],
                }
                .into(),
                // The file read back from disk is the whole truth: nothing the
                // session changed survives it, normalised or otherwise.
                SettingsEvent::Loaded {
                    settings: Box::new(SettingsState {
                        theme: Theme::JavaClient,
                        game_path: "C:/games/FA/bin/ForgedAlliance.exe".into(),
                        ..SettingsState::default()
                    }),
                }
                .into(),
            ],
        ),
        // ── coop ─────────────────────────────────────────────────────────
        case(
            "a stale coop board is dropped, and a failed refresh keeps the catalogue",
            vec![
                CoopEvent::CatalogLoading.into(),
                CoopEvent::CatalogLoaded {
                    scenarios: vec![],
                    missions: vec![coop_mission(7, 1), coop_mission(8, 2)],
                }
                .into(),
                CoopEvent::MissionSelected { mission_id: 8 }.into(),
                CoopEvent::LeaderboardLoading.into(),
                // The board for the mission that was open before.
                CoopEvent::LeaderboardLoaded {
                    mission_id: 7,
                    player_count: 0,
                    results: vec![coop_result(1, &["Ada"])],
                }
                .into(),
                CoopEvent::LeaderboardLoaded {
                    mission_id: 8,
                    player_count: 0,
                    results: vec![coop_result(2, &["Bob", "Cid"])],
                }
                .into(),
                CoopEvent::CatalogLoading.into(),
                CoopEvent::CatalogLoadFailed {
                    reason: "503".into(),
                    kind: RequestFailureKind::Offline,
                }
                .into(),
                // The open mission left the catalogue: the first one opens.
                CoopEvent::CatalogLoaded {
                    scenarios: vec![],
                    missions: vec![coop_mission(7, 1)],
                }
                .into(),
                CoopEvent::MissionSelected { mission_id: 7 }.into(),
            ],
        ),
        // ── client self-update ───────────────────────────────────────────
        case(
            "a failed download keeps the offer, and a failed re-check does too",
            vec![
                ClientUpdateEvent::CheckStarted {
                    current_version: "0.2.0".into(),
                }
                .into(),
                ClientUpdateEvent::Available {
                    release: client_release("0.3.0"),
                }
                .into(),
                ClientUpdateEvent::DownloadProgressed {
                    received_bytes: 1_024,
                    total_bytes: 4_096,
                }
                .into(),
                ClientUpdateEvent::Failed {
                    reason: "the connection was reset".into(),
                }
                .into(),
                ClientUpdateEvent::CheckStarted {
                    current_version: "0.2.0".into(),
                }
                .into(),
                ClientUpdateEvent::Failed {
                    reason: "GitHub is unreachable".into(),
                }
                .into(),
                ClientUpdateEvent::CheckCompleted {
                    at: "2026-02-01T09:00:00Z".into(),
                }
                .into(),
            ],
        ),
        // ── map generator ────────────────────────────────────────────────
        case(
            "generator versions resolve, the options follow a chosen one, and previews are capped",
            vec![
                MapGeneratorEvent::VersionsLoaded {
                    versions: vec!["1.22.1".into(), "1.21.0".into()],
                }
                .into(),
                MapGeneratorEvent::VersionResolved {
                    version: "1.21.0".into(),
                }
                .into(),
                // The resolved version stays the latest one known; the list
                // only fills it in when nothing has.
                MapGeneratorEvent::VersionsLoaded {
                    versions: vec!["1.23.0".into(), "1.22.1".into()],
                }
                .into(),
                MapGeneratorEvent::OptionsChanged {
                    options: GeneratorOptions {
                        spawn_count: Some(4),
                        ..GeneratorOptions::default()
                    },
                }
                .into(),
                MapGeneratorEvent::OptionsChanged {
                    options: GeneratorOptions {
                        version: Some("1.22.1".into()),
                        spawn_count: Some(6),
                        ..GeneratorOptions::default()
                    },
                }
                .into(),
                MapGeneratorEvent::PreviewsLoaded {
                    previews: generator_previews("first", 40),
                }
                .into(),
                // Still within the cap together: kept alongside.
                MapGeneratorEvent::PreviewsLoaded {
                    previews: generator_previews("second", 20),
                }
                .into(),
                // Over it: the older ones go together, the new batch whole.
                MapGeneratorEvent::PreviewsLoaded {
                    previews: generator_previews("third", 10),
                }
                .into(),
            ],
        ),
        // ── tournaments / tutorials ──────────────────────────────────────
        case(
            "a tournament list, detail and chat fail independently and keep what they had",
            vec![
                TourneyEvent::Loading.into(),
                TourneyEvent::Loaded {
                    events: vec![tourney("e1"), tourney("e2")],
                }
                .into(),
                TourneyEvent::Selected {
                    tournament_id: "e2".into(),
                }
                .into(),
                TourneyEvent::DetailLoading.into(),
                // The detail for the event that was open before is dropped.
                TourneyEvent::DetailLoaded {
                    event: Box::new(tourney("e1")),
                }
                .into(),
                TourneyEvent::DetailLoadFailed {
                    reason: "503".into(),
                    kind: RequestFailureKind::Offline,
                }
                .into(),
                TourneyEvent::ChatRoomsLoaded {
                    rooms: vec![tourney_room("global", 2)],
                }
                .into(),
                TourneyEvent::RoomOpened {
                    room_id: "global".into(),
                }
                .into(),
                TourneyEvent::ChatLoading.into(),
                TourneyEvent::ChatFailed {
                    reason: "session expired".into(),
                    kind: RequestFailureKind::Unauthorized,
                }
                .into(),
                TourneyEvent::Loading.into(),
                TourneyEvent::LoadFailed {
                    reason: "offline".into(),
                    kind: RequestFailureKind::Offline,
                }
                .into(),
                // The open event left the list: its failures go with it.
                TourneyEvent::Loaded {
                    events: vec![tourney("e1")],
                }
                .into(),
            ],
        ),
        case(
            "a tutorial launch fails, and a refresh without the open lesson moves the selection",
            vec![
                TutorialsEvent::Loaded {
                    categories: vec![],
                    tutorials: vec![tutorial(7), tutorial(8)],
                }
                .into(),
                TutorialsEvent::Selected { tutorial_id: 8 }.into(),
                TutorialsEvent::LaunchPreparing {
                    tutorial_id: 8,
                    detail: "Updating tutorials".into(),
                }
                .into(),
                TutorialsEvent::LaunchFailed {
                    tutorial_id: 8,
                    reason: "the map could not be staged".into(),
                }
                .into(),
                TutorialsEvent::Loading.into(),
                TutorialsEvent::LoadFailed {
                    reason: "503".into(),
                }
                .into(),
                TutorialsEvent::Loaded {
                    categories: vec![],
                    tutorials: vec![tutorial(7)],
                }
                .into(),
            ],
        ),
        // ── clan / changelog ─────────────────────────────────────────────
        case(
            "a withdrawn invitation leaves the candidate list closed, and a failed reload keeps the clan",
            vec![
                ClanEvent::Loading.into(),
                ClanEvent::Loaded {
                    identity: ClanIdentity {
                        player_id: 7,
                        login: "Ada".into(),
                        clan_id: "42".into(),
                        clan_name: "Brotherhood".into(),
                        clan_tag: "BRO".into(),
                        is_leader: true,
                    },
                    clan: None,
                }
                .into(),
                ClanEvent::CandidatesLoaded {
                    candidates: vec![player_summary(9, "Recruit", Some(1_200))],
                }
                .into(),
                ClanEvent::InvitationReady {
                    invitation: ClanInvitation {
                        token: "jwt".into(),
                        player_id: 9,
                        login: "Recruit".into(),
                    },
                }
                .into(),
                ClanEvent::InvitationCleared.into(),
                ClanEvent::Loading.into(),
                ClanEvent::LoadFailed {
                    reason: "offline".into(),
                }
                .into(),
            ],
        ),
        case(
            "a failed refresh of the patch notes keeps the releases already listed",
            vec![
                ChangelogEvent::Loading.into(),
                ChangelogEvent::Loaded {
                    releases: vec![faf_domain::protocol::changelog::ChangelogRelease {
                        id: "3837".into(),
                        kind: "Game Patch".into(),
                        date: "2026-08-14".into(),
                        year: "2026".into(),
                        source_url: "https://example.invalid/3837.md".into(),
                        web_url: "https://example.invalid/3837".into(),
                    }],
                }
                .into(),
                ChangelogEvent::EntryLoading { id: "3837".into() }.into(),
                ChangelogEvent::Loading.into(),
                ChangelogEvent::LoadFailed {
                    reason: "GitHub answered 502".into(),
                }
                .into(),
            ],
        ),
        // ── auth ─────────────────────────────────────────────────────────
        case(
            "a test session keeps its mode through a failure and gives it up on logout",
            vec![
                AuthEvent::TestLoggedIn { player: player() }.into(),
                AuthEvent::LoginStarted.into(),
                AuthEvent::LoginFailed {
                    message: "the test server refused the token".into(),
                }
                .into(),
                AuthEvent::TestLoggedIn { player: player() }.into(),
                AuthEvent::LoggedOut.into(),
            ],
        ),
    ]
}

fn lobby_game(id: i32, title: &str, players: i32) -> Game {
    Game {
        id,
        title: title.into(),
        host: "Ada".into(),
        players,
        max_players: 8,
        map: "scmp_009".into(),
        mod_name: "faf".into(),
        average_rating: 1_200,
        rating_type: "global".into(),
        password_protected: false,
        visibility: "public".into(),
        game_type: "custom".into(),
        launched_at: None,
        hosted_at: None,
        rating_min: None,
        rating_max: None,
        enforce_rating_range: false,
        teams: std::collections::BTreeMap::from([("1".to_string(), vec!["Ada".to_string()])]),
        sim_mods: std::collections::BTreeMap::new(),
    }
}

fn game_launch(uid: i32) -> GameLaunch {
    GameLaunch {
        uid,
        mod_name: "faf".into(),
        name: "Friday game".into(),
        mapname: "scmp_009".into(),
        game_type: "custom".into(),
        rating_type: "global".into(),
        expected_players: None,
        team: None,
        faction: None,
        map_position: None,
        game_options: std::collections::BTreeMap::new(),
        args: vec!["/numgames".into(), "12".into()],
    }
}

fn leaderboard_entry(player_id: i32, rank: i32) -> LeaderboardEntry {
    LeaderboardEntry {
        player_id,
        rank,
        player_name: format!("Player{player_id}"),
        avatar_url: None,
        avatar_tooltip: None,
        score: None,
        rating: Some(1_500),
        mean: Some(1_620.0),
        deviation: Some(40.0),
        games_played: 120,
        won_games: Some(70),
        update_time: Some("2026-01-01T00:00:00Z".into()),
        division: None,
        subdivision: None,
        division_order: None,
        highest_score: None,
        division_image_url: None,
        division_medium_image_url: None,
        returning_player: None,
    }
}

fn league_season(id: i32, league_id: i32, season_number: i32) -> LeagueSeason {
    LeagueSeason {
        id,
        league_id,
        leaderboard_id: 2,
        season_number,
        start_date: "2026-01-01T00:00:00Z".into(),
        end_date: "2026-04-01T00:00:00Z".into(),
        placement_games: 10,
        placement_games_returning_player: 3,
        active: season_number == 12,
    }
}

fn installed_map(folder_name: &str) -> InstalledMap {
    InstalledMap {
        folder_name: folder_name.into(),
        display_name: folder_name.into(),
        max_players: 8,
        width: 512,
        height: 512,
        version: None,
        description: None,
        installed_at: None,
    }
}

fn matchmaker_pool(id: i32, name: &str) -> MatchmakerMapPool {
    MatchmakerMapPool {
        id,
        name: name.into(),
        min_rating: None,
        max_rating: Some(1_300),
        veto_tokens_per_player: 1,
        max_tokens_per_map: 1,
        minimum_maps_after_veto: 1,
        maps: vec![MatchmakerPoolMap {
            assignment_id: id * 10,
            display_name: "Gap of Rohan".into(),
            folder_name: "gap_of_rohan.v0002".into(),
            max_players: 2,
            width: 512,
            height: 512,
            thumbnail_url: String::new(),
        }],
    }
}

fn vault_mod(mod_id: i32, uid: &str) -> VaultMod {
    VaultMod {
        mod_id,
        version_id: mod_id * 10,
        display_name: format!("Mod {uid}"),
        author: "Someone".into(),
        uploader: "Someone".into(),
        uploader_id: Some(4711),
        uid: uid.into(),
        version: "3".into(),
        description: String::new(),
        filename: format!("{uid}.zip"),
        mod_type: ModType::Ui,
        ranked: false,
        recommended: false,
        rating_tenths: 0,
        reviews: 0,
        created_at: "2026-01-01T00:00:00Z".into(),
        updated_at: "2026-01-01T00:00:00Z".into(),
        download_url: String::new(),
        thumbnail_url: String::new(),
    }
}

fn installed_mod(uid: &str, enabled: bool) -> InstalledMod {
    InstalledMod {
        folder_name: format!("mod_{uid}"),
        uid: uid.into(),
        display_name: format!("Mod {uid}"),
        version: "3".into(),
        author: "Someone".into(),
        description: String::new(),
        mod_type: ModType::Sim,
        enabled,
    }
}

fn history_query(player_id: i32) -> RatingHistoryQuery {
    RatingHistoryQuery {
        player_id,
        leaderboard_id: 1,
        leaderboard: "global".into(),
        period: RatingHistoryPeriod::All,
        page: 1,
        page_size: 1_000,
    }
}

fn history_point(timestamp: &str, rating: f64) -> RatingHistoryPoint {
    RatingHistoryPoint {
        timestamp: timestamp.into(),
        rating,
        mean: rating + 300.0,
        deviation: 100.0,
    }
}

fn review_target(id: i32, name: &str) -> ReviewTarget {
    ReviewTarget {
        kind: ReviewKind::Map,
        id,
        name: name.into(),
    }
}

fn notification(id: &str, kind: NotificationKind) -> ClientNotification {
    ClientNotification {
        id: id.into(),
        kind,
        title: format!("Notification {id}"),
        body: String::new(),
        action: None,
        created_at: "2026-01-01T00:00:00Z".into(),
        read: false,
        text: None,
    }
}

fn coop_mission(id: i32, order: i32) -> CoopMission {
    CoopMission {
        id,
        name: format!("Ivory Sun {order}"),
        description: String::new(),
        version: 1,
        download_url: String::new(),
        thumbnail_url_small: String::new(),
        thumbnail_url_large: String::new(),
        map_folder_name: format!("scmp_coop_{id}"),
        scenario_id: Some(1),
        order,
    }
}

fn coop_result(id: i32, players: &[&str]) -> CoopResult {
    CoopResult {
        id,
        ranking: 1,
        secondary_objectives: true,
        duration_seconds: 1_800,
        player_count: players.len() as i32,
        players: players.iter().map(|login| login.to_string()).collect(),
        replay_id: None,
        played_at: None,
    }
}

fn client_release(version: &str) -> ClientRelease {
    ClientRelease {
        version: version.into(),
        notes_url: format!("https://example.invalid/releases/{version}"),
        download_url: "https://example.invalid/installer.exe".into(),
        asset_name: "installer.exe".into(),
        size_bytes: 4_096,
        pre_release: false,
        published_at: "2026-02-01T00:00:00Z".into(),
    }
}

/// `count` generated-map previews named after `batch`, for the preview cap.
fn generator_previews(batch: &str, count: usize) -> std::collections::HashMap<String, String> {
    (0..count)
        .map(|index| {
            (
                format!("neroxis_map_generator_1.22.1_{batch}_{index}"),
                format!("data:image/png;base64,{batch}{index}"),
            )
        })
        .collect()
}

fn tourney(id: &str) -> Tourney {
    Tourney {
        id: id.into(),
        name: format!("Event {id}"),
        status: TourneyStatus::Signup,
        player_count: 8,
        team_count: 8,
        created_at: Some(1_700_000_000),
        ..Tourney::default()
    }
}

fn tourney_room(id: &str, unread: i32) -> ChatRoom {
    ChatRoom {
        id: id.into(),
        name: format!("Room {id}"),
        unread,
        ..ChatRoom::default()
    }
}

/// A room whose match has been played, which is what the completed group is
/// built from.
fn tourney_room_done(id: &str, mentioned: bool) -> ChatRoom {
    ChatRoom {
        done: true,
        mentioned,
        ..tourney_room(id, 0)
    }
}

fn player_summary(id: i32, login: &str, rating: Option<i32>) -> PlayerSummary {
    PlayerSummary {
        id,
        login: login.into(),
        avatar_url: String::new(),
        country: "de".into(),
        global_rating: rating,
        ladder_rating: None,
    }
}

/// The catalogue every filter case runs against.
///
/// Kept small and deliberately awkward: an entry with no tags at all, one with
/// only a level, one with numbers that contradict its level, and one about a
/// specific map. Those are the four shapes the filter's edge cases live in.
fn training_catalogue() -> Vec<TrainingResource> {
    let base = TrainingResource {
        readable: false,
        recording_url: String::new(),
        image_url: String::new(),
        id: String::new(),
        title: String::new(),
        summary: String::new(),
        kind: TrainingKind::Guide,
        level: None,
        url: String::new(),
        tutorial_id: None,
        author: String::new(),
        rating_min: None,
        rating_max: None,
        game_modes: vec![],
        topics: vec![],
        maps: vec![],
        factions: vec![],
        duration_minutes: None,
        related: vec![],
        approved_by: String::new(),
        updated_at: String::new(),
    };
    vec![
        TrainingResource {
            image_url: String::new(),
            id: "untagged".into(),
            title: "How the game works".into(),
            ..base.clone()
        },
        TrainingResource {
            id: "eco-beginner".into(),
            title: "Economy fundamentals".into(),
            summary: "Mass and energy for a first game.".into(),
            kind: TrainingKind::Video,
            level: Some(TrainingLevel::Beginner),
            topics: vec![TrainingTopic::Economy],
            game_modes: vec!["1v1".into()],
            ..base.clone()
        },
        TrainingResource {
            id: "setons-4v4".into(),
            title: "Seton's Clutch opening".into(),
            kind: TrainingKind::BuildOrder,
            level: Some(TrainingLevel::Beginner),
            rating_min: Some(1400),
            topics: vec![TrainingTopic::BuildOrder, TrainingTopic::Economy],
            game_modes: vec!["4v4".into()],
            maps: vec!["Setons Clutch".into()],
            ..base.clone()
        },
        TrainingResource {
            id: "advanced-micro".into(),
            title: "Micro at the top".into(),
            author: "A trainer".into(),
            level: Some(TrainingLevel::Advanced),
            topics: vec![TrainingTopic::Micro],
            ..base
        },
    ]
}

fn training_form_problem_fixture() -> TrainingFormProblemFixture {
    let review = |name: &str, draft: ReviewRequestDraft| TrainingReviewProblemCase {
        name: name.to_string(),
        problem: review_problem(&draft),
        draft: Box::new(draft),
    };
    let contribution = |name: &str, draft: ContributionDraft| TrainingContributionProblemCase {
        name: name.to_string(),
        problem: contribution_problem(&draft),
        draft: Box::new(draft),
    };
    let asked = ReviewRequestDraft {
        goal: "Where did I lose the eco lead?".into(),
        ..ReviewRequestDraft::default()
    };
    let titled = ContributionDraft {
        title: "T1 tank micro".into(),
        ..ContributionDraft::default()
    };

    TrainingFormProblemFixture {
        reviews: vec![
            review(
                "an empty request has no replay",
                ReviewRequestDraft::default(),
            ),
            review(
                "a replay with no question",
                ReviewRequestDraft {
                    replay_id: Some(27_456_965),
                    ..ReviewRequestDraft::default()
                },
            ),
            review(
                "an id and a question is enough",
                ReviewRequestDraft {
                    replay_id: Some(27_456_965),
                    ..asked.clone()
                },
            ),
            review(
                "a pasted link is enough",
                ReviewRequestDraft {
                    replay_link: "https://replay.faforever.com/27456965".into(),
                    ..asked.clone()
                },
            ),
            review(
                "a local file that was never uploaded is enough",
                ReviewRequestDraft {
                    replay_file: "2026-09-04 setons.fafreplay".into(),
                    ..asked.clone()
                },
            ),
            review(
                "a question of only whitespace is no question",
                ReviewRequestDraft {
                    replay_id: Some(1),
                    goal: "   ".into(),
                    ..ReviewRequestDraft::default()
                },
            ),
        ],
        contributions: vec![
            contribution("nothing at all", ContributionDraft::default()),
            contribution("a title and nothing else", titled.clone()),
            contribution(
                "a title and a link",
                ContributionDraft {
                    url: "https://www.youtube.com/watch?v=abc".into(),
                    ..titled.clone()
                },
            ),
            contribution(
                "a title and a body",
                ContributionDraft {
                    body: "Split before they are shot.".into(),
                    ..titled.clone()
                },
            ),
            contribution(
                "a link that is not a link",
                ContributionDraft {
                    url: "youtube.com/watch".into(),
                    ..titled.clone()
                },
            ),
            contribution(
                "a link that is not HTTPS",
                ContributionDraft {
                    url: "http://example.invalid/guide".into(),
                    ..titled.clone()
                },
            ),
            contribution(
                "a scheme and nothing after it",
                ContributionDraft {
                    url: "https://".into(),
                    ..titled.clone()
                },
            ),
            contribution(
                "a host with a space in it",
                ContributionDraft {
                    url: "https://a b.com".into(),
                    ..titled
                },
            ),
        ],
    }
}

fn training_filter_fixture() -> TrainingFilterFixture {
    TrainingFilterFixture {
        catalogue: training_catalogue(),
        cases: training_filter_cases(),
    }
}

fn training_filter_cases() -> Vec<TrainingFilterCase> {
    let catalogue = training_catalogue();
    let case = |name: &str,
                query: TrainingQuery,
                my_rating: Option<i32>,
                ratings: std::collections::BTreeMap<String, i32>| {
        let profile = TrainingProfile {
            rating: my_rating,
            ratings: ratings.clone(),
            ..TrainingProfile::default()
        };
        TrainingFilterCase {
            name: name.to_string(),
            expected_ids: filter_resources(&catalogue, &query, &profile)
                .into_iter()
                .map(|resource| resource.id.clone())
                .collect(),
            query: Box::new(query),
            my_rating,
            ratings,
        }
    };

    vec![
        case(
            "no filter lists everything",
            TrainingQuery::default(),
            Some(1000),
            Default::default(),
        ),
        case(
            "free text searches prose and tags alike",
            TrainingQuery {
                text: "setons".into(),
                ..TrainingQuery::default()
            },
            None,
            Default::default(),
        ),
        case(
            "free text matches the author",
            TrainingQuery {
                text: "trainer".into(),
                ..TrainingQuery::default()
            },
            None,
            Default::default(),
        ),
        case(
            "a level filter is exact and excludes the untagged",
            TrainingQuery {
                level: Some(TrainingLevel::Beginner),
                ..TrainingQuery::default()
            },
            None,
            Default::default(),
        ),
        case(
            "a topic filter",
            TrainingQuery {
                topic: Some(TrainingTopic::Economy),
                ..TrainingQuery::default()
            },
            None,
            Default::default(),
        ),
        case(
            "a kind filter",
            TrainingQuery {
                kind: Some(TrainingKind::Video),
                ..TrainingQuery::default()
            },
            None,
            Default::default(),
        ),
        case(
            "a mode filter keeps entries that claim no mode",
            TrainingQuery {
                game_mode: "4v4".into(),
                ..TrainingQuery::default()
            },
            None,
            Default::default(),
        ),
        case(
            "a map filter keeps entries that claim no map",
            TrainingQuery {
                map: "setons".into(),
                ..TrainingQuery::default()
            },
            None,
            Default::default(),
        ),
        case(
            "my rating hides a band that excludes me, level bands included",
            TrainingQuery {
                my_rating_only: true,
                ..TrainingQuery::default()
            },
            Some(800),
            Default::default(),
        ),
        case(
            "my rating with no rating known hides nothing",
            TrainingQuery {
                my_rating_only: true,
                ..TrainingQuery::default()
            },
            None,
            Default::default(),
        ),
        case(
            "filters combine",
            TrainingQuery {
                topic: Some(TrainingTopic::Economy),
                level: Some(TrainingLevel::Beginner),
                my_rating_only: true,
                ..TrainingQuery::default()
            },
            Some(900),
            Default::default(),
        ),
        case(
            "a ladder guide is judged by the ladder rating, not the global one",
            TrainingQuery {
                my_rating_only: true,
                ..TrainingQuery::default()
            },
            Some(1800),
            std::collections::BTreeMap::from([("1v1".to_string(), 1200)]),
        ),
    ]
}

fn submission(number: i32) -> GuideSubmission {
    GuideSubmission {
        number,
        title: format!("Training submission: Guide {number}"),
        summary: "A short pitch for it.".into(),
        entry: Some(training_resource(&format!("guide-{number}"))),
        author: "someone".into(),
        author_avatar_url: String::new(),
        created_at: "2026-09-04T18:00:00Z".into(),
        url: format!("https://github.com/FAForeverRustClient/guides/issues/{number}"),
        guide: None,
        pull: None,
    }
}

fn training_resource(id: &str) -> TrainingResource {
    TrainingResource {
        readable: false,
        recording_url: String::new(),
        id: id.into(),
        image_url: String::new(),
        title: format!("Resource {id}"),
        summary: "A short description.".into(),
        kind: TrainingKind::Guide,
        level: Some(TrainingLevel::Beginner),
        url: "https://wiki.faforever.com".into(),
        tutorial_id: None,
        author: "A trainer".into(),
        rating_min: Some(800),
        rating_max: Some(1200),
        game_modes: vec!["4v4".into()],
        topics: vec![TrainingTopic::Economy],
        maps: vec!["Setons Clutch".into()],
        factions: vec![],
        duration_minutes: Some(12),
        related: vec![],
        approved_by: String::new(),
        updated_at: String::new(),
    }
}

fn review_draft() -> ReviewRequestDraft {
    ReviewRequestDraft {
        replay_id: Some(27_456_965),
        replay_link: "https://replay.faforever.com/27456965".into(),
        replay_file: "27456965.fafreplay".into(),
        player: "Ada".into(),
        rating: "1150".into(),
        game_mode: "4v4".into(),
        map: "Setons Clutch".into(),
        faction: "UEF".into(),
        played_at: "2026-09-04T18:00:00Z".into(),
        goal: "Where did I lose the eco lead?".into(),
        struggle: String::new(),
    }
}

fn contribution_draft() -> ContributionDraft {
    ContributionDraft {
        title: "T1 tank micro".into(),
        summary: "Split before they are shot.".into(),
        kind: TrainingKind::Guide,
        level: Some(TrainingLevel::Beginner),
        url: String::new(),
        body: "Split before they are shot.".into(),
        topics: vec![TrainingTopic::Micro],
        game_modes: vec!["1v1".into()],
        maps: vec![],
        factions: vec![],
        rating_min: "800".into(),
        rating_max: "1200".into(),
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

/// Writes the fixture. A test rather than a binary so it cannot go stale
/// without someone noticing: `cargo test` regenerates it, and the frontend
/// test then fails if the twin has not kept up.
#[test]
fn writes_the_frontend_conformance_fixture() {
    let fixture = Fixture {
        initial: AppState::default(),
        cases: cases(),
        helpers: helper_fixture(),
    };
    // Through `Value` on the way out, whose objects keep their keys sorted.
    // Several slices hold `HashMap`s (generated map previews, replay details,
    // resolved maps), which serialise in an order that changes with every
    // process, so the file differed on every run and CI's drift check failed
    // on a fixture nobody had changed. The frontend compares with `toEqual`,
    // which does not care about key order.
    let fixture = serde_json::to_value(&fixture).expect("the fixture must serialise");
    let json = serde_json::to_string_pretty(&fixture).expect("the fixture must serialise");

    let target =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ui/src/store/__fixtures__");
    std::fs::create_dir_all(&target).expect("could not create the fixture directory");
    std::fs::write(target.join("reducer-conformance.json"), format!("{json}\n"))
        .expect("could not write the fixture");
}

/// Every case must actually exercise something: an empty sequence would pass
/// the frontend test while proving nothing.
#[test]
fn every_case_has_steps() {
    for case in cases() {
        assert!(!case.steps.is_empty(), "`{}` has no steps", case.name);
    }
}

/// Every state slice must appear in at least one case.
///
/// Without this, adding a slice adds an untested hand-written TS twin and the
/// harness stays silent about it: which is precisely the gap the harness was
/// built to close. `AppState`'s own field names are the list, so a new slice
/// enrols itself: `AppEvent`'s `kind` tag is the same name in PascalCase.
#[test]
fn every_state_slice_is_covered_by_a_case() {
    let state = serde_json::to_value(AppState::default()).expect("state must serialise");
    let slices: Vec<String> = state
        .as_object()
        .expect("state is an object")
        .keys()
        .cloned()
        .collect();

    let mut covered = std::collections::BTreeSet::new();
    for case in cases() {
        for step in &case.steps {
            let event = serde_json::to_value(&step.event).expect("event must serialise");
            let kind = event["kind"].as_str().expect("every event is tagged");
            let mut chars = kind.chars();
            let camel = match chars.next() {
                Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
                None => String::new(),
            };
            covered.insert(camel);
        }
    }

    let missing: Vec<&String> = slices
        .iter()
        .filter(|slice| !covered.contains(*slice))
        .collect();
    assert!(
        missing.is_empty(),
        "these state slices have no conformance case: {missing:?}"
    );
}

/// Event variants not yet represented by a scenario. This is an explicit debt
/// baseline, not a blanket exemption: a new event fails the test until it gets
/// a conformance case or is deliberately added here. When a case is added, its
/// entry must be removed so the baseline can only shrink intentionally.
///
/// Empty: every declared variant is replayed by at least one case. An entry
/// added here should be the exception, with a comment saying why a case that
/// reaches the variant from a meaningful state cannot be written yet.
const UNCOVERED_EVENT_VARIANTS: &[&str] = &[];

const EVENT_ENUM_SOURCES: &[(&str, &str, &str)] = &[
    ("Auth", "AuthEvent", include_str!("../src/state/auth.rs")),
    (
        "Changelog",
        "ChangelogEvent",
        include_str!("../src/state/changelog.rs"),
    ),
    ("Chat", "ChatEvent", include_str!("../src/state/chat.rs")),
    ("Clan", "ClanEvent", include_str!("../src/state/clan.rs")),
    (
        "ClientUpdate",
        "ClientUpdateEvent",
        include_str!("../src/state/client_update.rs"),
    ),
    ("Coop", "CoopEvent", include_str!("../src/state/coop.rs")),
    (
        "Events",
        "EventsEvent",
        include_str!("../src/state/events.rs"),
    ),
    (
        "GalacticWar",
        "GalacticWarEvent",
        include_str!("../src/state/galactic_war.rs"),
    ),
    (
        "Guides",
        "GuidesEvent",
        include_str!("../src/state/guides.rs"),
    ),
    (
        "Install",
        "InstallEvent",
        include_str!("../src/state/install.rs"),
    ),
    (
        "Leaderboard",
        "LeaderboardEvent",
        include_str!("../src/state/leaderboard.rs"),
    ),
    ("Lobby", "LobbyEvent", include_str!("../src/state/lobby.rs")),
    (
        "MapGenerator",
        "MapGeneratorEvent",
        include_str!("../src/state/map_generator.rs"),
    ),
    ("Maps", "MapsEvent", include_str!("../src/state/maps.rs")),
    ("Mods", "ModsEvent", include_str!("../src/state/mods.rs")),
    ("Nav", "NavEvent", include_str!("../src/state/nav.rs")),
    (
        "Notifications",
        "NotificationEvent",
        include_str!("../src/state/notifications.rs"),
    ),
    (
        "PlayerCard",
        "PlayerCardEvent",
        include_str!("../src/state/player_card.rs"),
    ),
    (
        "Replays",
        "ReplayEvent",
        include_str!("../src/state/replays.rs"),
    ),
    (
        "Reporting",
        "ReportingEvent",
        include_str!("../src/state/reporting.rs"),
    ),
    (
        "Reviews",
        "ReviewsEvent",
        include_str!("../src/state/reviews.rs"),
    ),
    (
        "Session",
        "SessionEvent",
        include_str!("../src/state/session.rs"),
    ),
    (
        "Settings",
        "SettingsEvent",
        include_str!("../src/state/settings.rs"),
    ),
    (
        "Social",
        "SocialEvent",
        include_str!("../src/state/social.rs"),
    ),
    (
        "Streams",
        "StreamsEvent",
        include_str!("../src/state/streams.rs"),
    ),
    (
        "Tourney",
        "TourneyEvent",
        // The tourney slice is a directory; its events are declared in one file.
        include_str!("../src/state/tourney/messages.rs"),
    ),
    (
        "Training",
        "TrainingEvent",
        include_str!("../src/state/training.rs"),
    ),
    (
        "Tutorials",
        "TutorialsEvent",
        include_str!("../src/state/tutorials.rs"),
    ),
    (
        "Uploads",
        "UploadsEvent",
        include_str!("../src/state/uploads.rs"),
    ),
];

#[test]
fn every_event_variant_is_covered_or_explicitly_baselined() {
    use std::collections::BTreeSet;

    let covered: BTreeSet<String> = cases()
        .into_iter()
        .flat_map(|case| case.steps)
        .map(|step| {
            let event = serde_json::to_value(step.event).expect("event must serialise");
            format!(
                "{}:{}",
                event["kind"].as_str().expect("event kind is a string"),
                event["event"]["type"]
                    .as_str()
                    .expect("nested event type is a string")
            )
        })
        .collect();
    let declared: BTreeSet<String> = EVENT_ENUM_SOURCES
        .iter()
        .flat_map(|(kind, enum_name, source)| declared_variants(kind, enum_name, source))
        .collect();
    let actual_missing: BTreeSet<String> = declared.difference(&covered).cloned().collect();
    let baseline: BTreeSet<String> = UNCOVERED_EVENT_VARIANTS
        .iter()
        .map(|variant| (*variant).to_string())
        .collect();

    assert_eq!(
        actual_missing, baseline,
        "conformance coverage changed; add a case for new variants and remove newly covered variants from UNCOVERED_EVENT_VARIANTS"
    );
}

fn declared_variants(kind: &str, enum_name: &str, source: &str) -> Vec<String> {
    let declaration = format!("pub enum {enum_name} {{");
    let body = source
        .split_once(&declaration)
        .unwrap_or_else(|| panic!("could not find `{declaration}`"))
        .1;
    let mut depth = 1_i32;
    let mut variants = Vec::new();
    for line in body.lines() {
        if depth == 1 && line.starts_with("    ") && !line.starts_with("        ") {
            let identifier: String = line
                .trim_start()
                .chars()
                .take_while(|character| character.is_ascii_alphanumeric() || *character == '_')
                .collect();
            if identifier.starts_with(|character: char| character.is_ascii_uppercase()) {
                let mut characters = identifier.chars();
                let camel = characters
                    .next()
                    .map(|first| first.to_ascii_lowercase().to_string() + characters.as_str())
                    .unwrap_or_default();
                variants.push(format!("{kind}:{camel}"));
            }
        }
        depth += line.matches('{').count() as i32;
        depth -= line.matches('}').count() as i32;
        if depth == 0 {
            break;
        }
    }
    variants
}

/// The initial state must round-trip through JSON unchanged. Step expectations
/// are JSON values by design because each one is only a single heterogeneous
/// state slice.
#[test]
fn the_recorded_states_round_trip_through_json() {
    let state = AppState::default();
    let json = serde_json::to_string(&state).expect("state must serialise");
    let back: AppState = serde_json::from_str(&json).expect("state must deserialise");
    assert_eq!(back, state);
}
