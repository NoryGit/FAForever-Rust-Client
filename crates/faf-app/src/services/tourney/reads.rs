//! Tournament reads: the list and its order, an event's detail and its
//! entrants' FAF profiles, templates, copy sources and presets, the account
//! search and the series. Each replaceable read runs under its own generation,
//! so only the newest answer lands.

use faf_domain::state::{CopySourceMaps, TourneyEvent};

use crate::runtime::{EventSink, ServiceCtx};

pub(super) async fn load(ctx: &ServiceCtx, out: &EventSink) {
    out.emit(TourneyEvent::Loading);
    // Sent with every load rather than once at startup: it costs nothing, and
    // the alternative is a tab whose images work only if the list was loaded
    // through the one path that happened to announce it.
    out.emit(TourneyEvent::AssetBase {
        base: ctx.ports.tourney_read.asset_base(),
    });
    match ctx.ports.tourney_read.list().await {
        Ok(mut events) => {
            // Sorted here rather than in the view, because ordering is part of
            // the state every consumer shares.
            sort_events(&mut events, crate::services::now_seconds());
            out.emit(TourneyEvent::Loaded { events });
        }
        Err(error) => out.emit(TourneyEvent::LoadFailed {
            reason: error.to_string(),
            kind: error.kind(),
        }),
    }
}

/// The list, in the order every consumer reads it.
///
/// What a player can still act on comes first, and within one status the
/// soonest event does: a signup that closes tomorrow is worth more than one
/// three months out, which is the whole of what "closest upcoming first" asks
/// for. Once a date is behind us it stops being a countdown and becomes an
/// archive entry, so the past runs the other way, most recent first.
///
/// `now` is passed in rather than read here so the order is a pure function of
/// its inputs and can be asserted without a clock.
fn sort_events(events: &mut [faf_domain::state::Tourney], now: u32) {
    events.sort_by(|left, right| {
        rank(left.status)
            .cmp(&rank(right.status))
            .then_with(|| timing(left.event_date, now).cmp(&timing(right.event_date, now)))
            .then_with(|| right.created_at.cmp(&left.created_at))
    });
}

/// Where an event sits relative to now, as a sort key.
///
/// The derived `Ord` is the ordering: variants in declaration order, so
/// everything still ahead outranks everything behind it, and an event with no
/// date at all sorts last rather than being read as "happening imminently".
/// [`Reverse`] on the past is what turns an archive the right way round.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Timing {
    Upcoming(u32),
    Past(std::cmp::Reverse<u32>),
    Undated,
}

fn timing(event_date: Option<u32>, now: u32) -> Timing {
    match event_date {
        Some(at) if at >= now => Timing::Upcoming(at),
        Some(at) => Timing::Past(std::cmp::Reverse(at)),
        None => Timing::Undated,
    }
}

/// Sort order for the list: what a player can still do something about first.
fn rank(status: faf_domain::state::TourneyStatus) -> u8 {
    use faf_domain::state::TourneyStatus::*;
    match status {
        Signup => 0,
        Running => 1,
        Drafted => 2,
        Draft => 3,
        Finished => 4,
        Unknown => 5,
    }
}

pub(super) async fn load_detail(tournament_id: &str, ctx: &ServiceCtx, out: &EventSink) {
    let generation = ctx.tourney.detail_generation.begin();
    out.emit(TourneyEvent::DetailLoading);

    let loaded = ctx.ports.tourney_read.detail(tournament_id).await;
    if !ctx.tourney.detail_generation.is_current(generation) {
        // A newer selection is already in flight; emitting now would overwrite
        // its state with an older event's bracket.
        return;
    }
    match loaded {
        Ok(event) => {
            let accounts: Vec<i32> = event
                .players
                .iter()
                .filter_map(|player| player.faf_id)
                .collect();
            out.emit(TourneyEvent::DetailLoaded {
                event: Box::new(event),
            });
            load_entrant_profiles(&accounts, generation, ctx, out).await;
        }
        Err(error) => out.emit(TourneyEvent::DetailLoadFailed {
            reason: error.to_string(),
            kind: error.kind(),
        }),
    }
}

/// Re-read the open event without announcing it: `TourneyRead::RefreshDetail`.
pub(super) async fn refresh_detail(tournament_id: String, ctx: &ServiceCtx, out: &EventSink) {
    let generation = ctx.tourney.detail_generation.begin();
    match ctx.ports.tourney_read.detail(&tournament_id).await {
        Ok(event) if ctx.tourney.detail_generation.is_current(generation) => {
            out.emit(TourneyEvent::DetailLoaded {
                event: Box::new(event),
            });
        }
        Ok(_) => {}
        Err(error) => tracing::debug!(%error, "a silent tournament refresh failed"),
    }
}

/// Fetch the FAF accounts behind the entrants that carry one.
///
/// A second request after the detail rather than part of it, because the two
/// come from different services: the tournament service owns the entry, FAF
/// owns the player. A failure here is silent on purpose: the bracket is
/// complete without avatars, and an error banner over a working tournament
/// because a decoration did not load would be noise.
async fn load_entrant_profiles(
    accounts: &[i32],
    generation: u64,
    ctx: &ServiceCtx,
    out: &EventSink,
) {
    if accounts.is_empty() {
        out.emit(TourneyEvent::EntrantProfilesLoaded { profiles: vec![] });
        return;
    }
    match ctx.ports.player_card.players_by_id(accounts).await {
        Ok(profiles) => {
            if ctx.tourney.detail_generation.is_current(generation) {
                out.emit(TourneyEvent::EntrantProfilesLoaded { profiles });
            }
        }
        Err(error) => tracing::warn!(%error, "could not load the entrants' FAF profiles"),
    }
}

/// Clear the news badge: `TourneyRead::MarkNewsRead`.
pub(super) async fn mark_news_read(tournament_id: String, ctx: &ServiceCtx, out: &EventSink) {
    // Deliberately not a `write`: nothing on screen changes except a
    // badge, and announcing it would blank the pane and reload the list
    // for an act the reader did not ask for. A failure is logged rather
    // than shown, for the same reason: the badge staying is not worth an
    // error banner over the announcements it belongs to.
    if let Err(error) = ctx.ports.tourney_read.mark_news_read(&tournament_id).await {
        tracing::warn!(%error, "could not mark the tournament news as read");
        return;
    }
    // The silent re-read, so the badge clears without the pane going through
    // "loading": announced, it blanked the event and drew it again, and the
    // whole page jumped the moment the announcements were opened.
    refresh_detail(tournament_id, ctx, out).await;
}

/// The events this account may import maps from: `TourneyRead::LoadCopySources`.
pub(super) async fn load_copy_sources(ctx: &ServiceCtx, out: &EventSink) {
    out.emit(TourneyEvent::CopySourcesLoading);
    match ctx.ports.tourney_read.copy_sources().await {
        Ok(sources) => out.emit(TourneyEvent::CopySourcesLoaded { sources }),
        Err(error) => out.emit(TourneyEvent::CopySourcesFailed {
            reason: error.to_string(),
            kind: error.kind(),
        }),
    }
}

/// The create form's presets: `TourneyRead::LoadPresets`.
pub(super) async fn load_presets(ctx: &ServiceCtx, out: &EventSink) {
    match ctx.ports.tourney_read.presets().await {
        Ok(presets) => out.emit(TourneyEvent::PresetsLoaded { presets }),
        // Silent, like the rules pages: without them the form is the
        // form, only without the shortcut.
        Err(error) => tracing::warn!(%error, "could not load the tournament presets"),
    }
}

/// Fill the create form from an event: `TourneyRead::LoadTemplate`.
pub(super) async fn load_template(tournament_id: String, ctx: &ServiceCtx, out: &EventSink) {
    let generation = ctx.tourney.template_generation.begin();
    out.emit(TourneyEvent::TemplateLoading);
    let loaded = ctx.ports.tourney_read.detail(&tournament_id).await;
    if !ctx.tourney.template_generation.is_current(generation) {
        return;
    }
    match loaded {
        Ok(event) => out.emit(TourneyEvent::TemplateLoaded {
            event: Box::new(event),
        }),
        Err(error) => out.emit(TourneyEvent::TemplateFailed {
            reason: error.to_string(),
            kind: error.kind(),
        }),
    }
}

/// One copy source's maps and pools: `TourneyRead::LoadCopySource`.
pub(super) async fn load_copy_source(tournament_id: String, ctx: &ServiceCtx, out: &EventSink) {
    out.emit(TourneyEvent::CopySourceLoading);
    match ctx.ports.tourney_read.detail(&tournament_id).await {
        Ok(event) => out.emit(TourneyEvent::CopySourceLoaded {
            source: CopySourceMaps {
                tournament_id,
                maps: event.map_db,
                pools: event.map_pools,
            },
        }),
        Err(error) => out.emit(TourneyEvent::CopySourceFailed {
            reason: error.to_string(),
            kind: error.kind(),
        }),
    }
}

/// The shortest query worth asking the API about.
///
/// One letter matches a large share of the player base, and the list it returns
/// is useless to pick from while costing a full request per keystroke.
const MIN_ACCOUNT_QUERY: usize = 2;

/// FAF accounts whose name starts with what the organiser typed.
///
/// Deliberately the *same* lookup the player card's picker uses
/// (`PlayerCardPort::search_players`), not a tournament-specific one: an entrant
/// is a FAF account, and the client already knows how to find and show one. The
/// tournament service has no player search of its own worth using: it matches
/// names exactly and answers "no such player", which is the refusal this
/// removes.
pub(super) async fn search_accounts(query: &str, ctx: &ServiceCtx, out: &EventSink) {
    let trimmed = query.trim();
    if trimmed.chars().count() < MIN_ACCOUNT_QUERY {
        // Bump the generation too, so an answer for a longer query typed a
        // moment ago cannot land on the now-cleared field.
        ctx.tourney.account_search_generation.begin();
        out.emit(TourneyEvent::AccountSearchCleared);
        return;
    }
    let generation = ctx.tourney.account_search_generation.begin();
    out.emit(TourneyEvent::AccountSearchStarted {
        query: trimmed.to_string(),
    });

    let found = ctx
        .ports
        .player_card
        .search_players(trimmed, ACCOUNT_SEARCH_LIMIT)
        .await;
    if !ctx.tourney.account_search_generation.is_current(generation) {
        return;
    }
    match found {
        Ok(matches) => out.emit(TourneyEvent::AccountSearchLoaded {
            query: trimmed.to_string(),
            matches,
        }),
        // Said out loud rather than swallowed: unlike the avatars, this one is
        // the answer to something the organiser just did, and an empty list that
        // means "your session expired" would send them hunting for a typo.
        Err(error) => out.emit(TourneyEvent::AccountSearchFailed {
            query: trimmed.to_string(),
            reason: error.to_string(),
            kind: error.kind(),
        }),
    }
}

/// Enough rows to recognise the right person among similar names, few enough to
/// scan without scrolling.
const ACCOUNT_SEARCH_LIMIT: i32 = 8;

/// Empty the account search: `TourneyRead::ClearAccountSearch`.
pub(super) fn clear_account_search(ctx: &ServiceCtx, out: &EventSink) {
    // Bump the generation as well as clearing: a request already in
    // flight must not repopulate the list after the organiser picked
    // somebody and the field closed.
    ctx.tourney.account_search_generation.begin();
    out.emit(TourneyEvent::AccountSearchCleared);
}

pub(super) async fn load_series(ctx: &ServiceCtx, out: &EventSink) {
    out.emit(TourneyEvent::SeriesLoading);
    match ctx.ports.tourney_read.series().await {
        Ok(series) => out.emit(TourneyEvent::SeriesLoaded { series }),
        Err(error) => out.emit(TourneyEvent::SeriesFailed {
            reason: error.to_string(),
            kind: error.kind(),
        }),
    }
}

pub(super) async fn open_series(series_id: &str, ctx: &ServiceCtx, out: &EventSink) {
    match ctx.ports.tourney_read.series_detail(series_id).await {
        Ok(detail) => out.emit(TourneyEvent::SeriesOpened {
            detail: Box::new(detail),
        }),
        // Reported through the list's own status rather than swallowed: the
        // pane it would have filled stays empty otherwise, with nothing saying
        // why.
        Err(error) => out.emit(TourneyEvent::SeriesFailed {
            reason: error.to_string(),
            kind: error.kind(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faf_domain::state::{Tourney, TourneyStatus};

    #[test]
    fn the_list_puts_what_a_player_can_still_join_first() {
        let mut order = [
            TourneyStatus::Finished,
            TourneyStatus::Draft,
            TourneyStatus::Signup,
            TourneyStatus::Running,
            TourneyStatus::Drafted,
        ];
        order.sort_by_key(|status| rank(*status));
        assert_eq!(
            order,
            [
                TourneyStatus::Signup,
                TourneyStatus::Running,
                TourneyStatus::Drafted,
                TourneyStatus::Draft,
                TourneyStatus::Finished,
            ]
        );
    }

    #[test]
    fn the_soonest_event_in_a_group_is_the_one_at_the_top() {
        // The complaint this answers: a signup three months out sat above one
        // closing tomorrow, because the list was ordered newest-first.
        const DAY: u32 = 86_400;
        let now = 100 * DAY;
        let at = |id: &str, event_date: Option<u32>| Tourney {
            id: id.into(),
            status: TourneyStatus::Signup,
            event_date,
            ..Tourney::default()
        };
        let mut events = vec![
            at("far", Some(now + 90 * DAY)),
            at("undated", None),
            at("stale", Some(now - 5 * DAY)),
            at("soon", Some(now + DAY)),
            at("older", Some(now - 60 * DAY)),
        ];
        sort_events(&mut events, now);
        let order: Vec<&str> = events.iter().map(|event| event.id.as_str()).collect();
        // Ahead of us, soonest first; then the past, most recent first; then
        // the one that never said when it happens.
        assert_eq!(order, ["soon", "far", "stale", "older", "undated"]);
    }

    #[test]
    fn what_a_player_can_join_still_outranks_what_is_happening_sooner() {
        // Timing is the tie-break inside a status, not a replacement for it: a
        // running event tonight does not push tomorrow's open signup down.
        const DAY: u32 = 86_400;
        let now = 100 * DAY;
        let mut events = vec![
            Tourney {
                id: "running-tonight".into(),
                status: TourneyStatus::Running,
                event_date: Some(now),
                ..Tourney::default()
            },
            Tourney {
                id: "signup-tomorrow".into(),
                status: TourneyStatus::Signup,
                event_date: Some(now + DAY),
                ..Tourney::default()
            },
        ];
        sort_events(&mut events, now);
        let order: Vec<&str> = events.iter().map(|event| event.id.as_str()).collect();
        assert_eq!(order, ["signup-tomorrow", "running-tonight"]);
    }
}
