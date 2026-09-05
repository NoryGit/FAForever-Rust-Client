//! The multi-player vault search, against the real FAF API.
//!
//! Searching for "games A and B both played" cannot be one request: the API
//! derives a filter's join alias from the property path, so two clauses on
//! `playerStats.player.login` share a join and match nothing. The client asks
//! per player and intersects the ids (`ReplayClient::search_shared_games`).
//!
//! The pieces are unit-tested where they are pure (`replay_query`), and the two
//! API constructs the workaround relies on are already carried by shipped code
//! elsewhere in this client: the `fields[game]` sparse fieldset by
//! `infra::player_card`, `id=in=(…)` by `infra::leaderboard`. What no offline
//! test can check is the whole loop against the live database: that the scans
//! page to the end, that the intersection is the games the pair really shared,
//! and that the page fetch renders them. Hence this test.
//!
//! It is `#[ignore]`d: `/data/*` answers 401 without a token, and the token is
//! personal. To run it, take an access token from a logged-in client and:
//!
//! ```sh
//! FAF_API_TOKEN=<token> cargo test -p faf-app --test shared_games_search -- --ignored --nocapture
//! ```
//!
//! The pair of players is discovered from the data rather than hard-coded: the
//! test takes a recent game, picks two of its participants, and asserts that
//! searching for both finds at least that game and that every row really does
//! contain both of them.

use std::sync::Arc;

use faf_app::infra::{FakeGame, FakeMapGenerator, ReplayClient, TokenStore};
use faf_app::ports::ReplayPort;
use faf_domain::protocol::replay_query::ReplayQuery;
use faf_domain::state::VaultReplay;

fn client(token: String) -> ReplayClient {
    let tokens = TokenStore::new();
    tokens.set(token);
    ReplayClient::faf(tokens, Arc::new(FakeGame), Arc::new(FakeMapGenerator))
}

fn logins(replay: &VaultReplay) -> Vec<String> {
    replay
        .teams
        .iter()
        .flat_map(|team| &team.players)
        .map(|player| player.name.clone())
        .collect()
}

#[tokio::test]
#[ignore = "hits the real FAF API; needs FAF_API_TOKEN"]
async fn a_two_player_search_returns_only_games_they_shared() {
    let token = std::env::var("FAF_API_TOKEN").expect("set FAF_API_TOKEN to run this test");
    let client = client(token);

    // A recent, finished game with a full roster to take the pair from.
    let seed = client
        .search_vault(ReplayQuery {
            only_ranked: true,
            page_size: 20,
            ..ReplayQuery::default()
        })
        .await
        .expect("the plain newest-first feed must load");
    let seed_game = seed
        .replays
        .iter()
        .find(|replay| logins(replay).len() >= 2)
        .expect("a recent game with at least two players");
    let roster = logins(seed_game);
    let (first, second) = (&roster[0], &roster[1]);
    println!("seed game {} : {first} + {second}", seed_game.uid);

    let result = client
        .search_vault(ReplayQuery {
            player: format!("{first}, {second}"),
            exact_player: true,
            // Keeps the id scans short; the seed game is inside this window.
            after: "2020-01-01".into(),
            page_size: 50,
            ..ReplayQuery::default()
        })
        .await
        .expect("the shared-games search must not error");

    assert!(
        result.replays.iter().any(|r| r.uid == seed_game.uid),
        "the game the pair was taken from must be in their shared games"
    );
    for replay in &result.replays {
        let names = logins(replay);
        assert!(
            names.iter().any(|n| n.eq_ignore_ascii_case(first))
                && names.iter().any(|n| n.eq_ignore_ascii_case(second)),
            "replay {} lists {names:?}, which is missing one of the two",
            replay.uid
        );
    }
    // The count is the whole intersection, not the page.
    let total = result.total_records.expect("an exact total");
    assert!(
        total >= result.replays.len() as i32,
        "total {total} cannot be smaller than the page it summarises"
    );
    println!("{total} shared games, page 1 has {}", result.replays.len());
}

#[tokio::test]
#[ignore = "hits the real FAF API; needs FAF_API_TOKEN"]
async fn two_players_who_never_met_come_back_empty_rather_than_failing() {
    let token = std::env::var("FAF_API_TOKEN").expect("set FAF_API_TOKEN to run this test");
    let client = client(token);

    // Two logins that cannot both exist, so the scans are trivially short and
    // the intersection is empty: the path must still answer cleanly.
    let result = client
        .search_vault(ReplayQuery {
            player: "zzz_no_such_player_a, zzz_no_such_player_b".into(),
            exact_player: true,
            ..ReplayQuery::default()
        })
        .await
        .expect("an empty intersection is not an error");

    assert!(result.replays.is_empty());
    assert_eq!(result.total_records, Some(0));
}
