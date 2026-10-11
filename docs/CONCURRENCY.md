# Concurrency contract

Which commands may overlap, what happens to the one that arrives while another
runs, and what calling work off guarantees. The table in
`crates/faf-app/src/runtime/command_policy.rs` is the source of truth; this page
says the same thing in prose and names the test that pins each row. Test paths
are under `crates/faf-app/tests/`.

## Lanes

Every command waits in one of two queues, each with its own concurrency limit.

- **Ordinary**: everything not listed below. 64 commands run at once, 64 more
  wait; once both are full, `dispatch` waits and `try_dispatch` reports it.
- **Priority**: releases and navigation. Checked first, with its own slots, so
  it never waits behind ordinary work.
  `Nav::*`, `Lobby::{CancelJoin, DeclineModReplacement, TerminateGame,
  Disconnect}`, `Lobby::Matchmake { start: false }`, `Chat::Disconnect`,
  `Replays::{CancelWatch, CancelLiveTracking}`, `MapGenerator::Cancel`,
  `Guides::CancelSignIn`, `Auth::{CancelLogin, Logout}`.
  Pinned by `command_contracts::a_priority_command_is_not_held_up_by_a_saturated_ordinary_lane`.

Because a release can overtake its start, a start that leaves the ordinary queue
after a release of the same pair was dispatched is dropped instead of run
(`runtime::ReleaseOrder`; unit test `a_start_overtaken_by_its_release_is_dropped`
in `runtime/mod.rs`).

## Keys and service guards

"Dropped" means the command completes at once without doing anything.
"Queued" means it waits its turn and then runs. Commands without an entry here
run concurrently with everything.

| Key / guard | Commands | May overlap | Same kind while one runs | Cancellation | Pinned by |
|---|---|---|---|---|---|
| `MapVault` (single-flight) | `Maps::LoadVault` | everything else | dropped; a loaded catalogue is also skipped, a failed one is retried | `Maps::CancelVaultLoad` drops all pending page requests; progress reports completed pages; retry is explicit | `command_contracts::a_second_single_flight_command_*`, `map_vault_load.rs` |
| `ModVault` (single-flight) | `Mods::{LoadVault, ReloadVault}` | everything else | dropped | `Mods::CancelVaultLoad` drops all pending page and review requests; progress reports completed pages | `command_contracts::a_second_single_flight_command_*`, `failure_paths::a_failed_mod_vault_load_*`, `mods_reload.rs` |
| `Changelog` (single-flight) | `Changelog::Load` | `Changelog::Select` | dropped (the running load selects the newest patch itself) | none | `command_contracts::a_second_single_flight_command_*`, `changelog.rs` |
| `MapGenerator` (single-flight) | `MapGenerator::{Generate, GenerateNamed, CleanUp}` | options, presets, previews | dropped | `MapGenerator::Cancel`: during the preflight the run is never started; during a run the port stops it; ends `Cancelled`, no notification, nothing recorded, key free again | `command_contracts::a_second_single_flight_command_*`, `failure_paths::cancelling_*` |
| `Upload` (single-flight) | `Uploads::Start` | `Open`, `Close`, `SetRanked` | dropped | none: closing the dialog hides a running publish, it does not stop it | `command_contracts::a_second_single_flight_command_*`, `uploads.rs` |
| `ClientUpdate` (single-flight) | `ClientUpdate::{Check, Download, Install}` | `Dismiss` | dropped (also for the startup and six-hourly checks, which go through `run_command`) | none | `command_contracts::a_second_single_flight_command_*`, `client_update.rs` |
| `GalacticWar` (single-flight) | `GalacticWar::{Install, Play}` | `Refresh*` | dropped | `GalacticWar::CancelInstall` stops download or extraction before commit; a completed install stays recorded but Play never auto-launches after cancellation | `command_contracts::a_second_single_flight_command_*` |
| `GuidesSignIn` (single-flight) | `Guides::SignIn` | everything else | dropped | `Guides::CancelSignIn` stops the polling through the port | `command_contracts::a_second_single_flight_command_*` |
| `TutorialLaunch` (single-flight) | `Tutorials::Launch` | `Load`, `Select` | dropped | `Tutorials::CancelLaunch` cancels updater preparation and prevents launch | `command_contracts::a_second_single_flight_command_*`, `tutorials.rs` |
| `MapFiles` (serial) | `Maps::{InstallMap, UninstallMap}` | everything else | queued, dispatch order | none | `command_contracts::serial_commands_*` |
| `ModFiles` (serial) | `Mods::{InstallMod, UpdateMod, UninstallMod, ToggleMod, SetActiveMods}` | everything else | queued, dispatch order | none | `command_contracts::serial_commands_*` |
| `GuidesVerdict` (serial) | `Guides::{Accept, Reject}` | everything else | queued, dispatch order | none | `command_contracts::serial_commands_*` |
| `ClanWrite` (serial) | `Clan::{Create, Edit, AcceptInvitation, Remove, Leave, HandOver, Disband}` | `Load`, `SearchCandidates`, `Invite` | queued, dispatch order | none | `command_contracts::serial_commands_*` |
| `TourneyWrite` (serial) | every `TourneyWrite` | every `TourneyRead` | queued, dispatch order | none | `command_contracts::serial_commands_*`, `tourney_write_order.rs` |
| `PartyPlacements` (serial) | `PlayerCard::LoadPartyPlacements` | everything else | queued, dispatch order; the next one asks only for ids still unknown | none | `command_contracts::serial_commands_*`, `failure_paths::a_*placement*` |
| `LobbyConnection` (service guard) | `Lobby::Connect`, the watchdog's reconnect | everything | dropped while the socket is open or connecting | `Lobby::Disconnect` closes it and disarms the watchdog; any end of the socket (drop or disconnect) calls off a join or search being prepared, which is then never sent on the next connection | `command_contracts::the_lobby_socket_*`, `lobby.rs`, `lobby_operations.rs`, `reconnects_and_retries.rs` |
| `ChatConnection` (service guard) | `Chat::Connect`, the watchdog's reconnect | everything | dropped while the socket is open or connecting | `Chat::Disconnect` closes it and disarms the watchdog; after a drop the channels are joined again and the scrollback is kept | `command_contracts::the_chat_socket_*`, `reconnects_and_retries::chat_comes_back_*` |
| `LobbyJoin` (service guard) | `Lobby::Join` | everything | dropped from the click until the server answers | `CancelJoin`, `Disconnect` or a drop free the slot; a join called off before its request is never sent, one called off after is never launched, and neither is resent after a reconnect | `lobby_operations.rs`, `launch_preparation.rs`, `reconnects_and_retries.rs` |
| `Login` (service guard) | `Auth::{Login, Restore, Logout}` | everything | queued behind the lock | `Logout`, `CancelLogin`, `LogoutTest`, `PlayOffline` cancel a login in progress instead of waiting for it; a `Restore` cannot be cancelled, so `Logout` waits for it, and its answer does not land. A login or restore only stages its session in the port; the service commits it (token current, refresh token saved, renewal started) under the cancellation lock and only while the attempt is current and not called off, and discards it otherwise, so a called-off or failed sign-in leaves nothing live | `command_contracts::calling_a_sign_in_off_*`, `command_contracts::a_restore_answering_*`, `auth.rs`, `auth_session.rs`, `infra::oauth` unit tests |
| `Settings` (service guard) | every `Settings` command | everything; settings commands overlap each other except for the two steps below | the merge (read, change, emit) is under one lock, so patches never revert each other; the write is under another, so documents reach the store one at a time in order | none; nothing is written before the file was read, and a failed write is carried by the next | `command_contracts::settings_writes_*`, `settings_patches.rs`, `settings_startup.rs`, `reconnects_and_retries::a_failed_settings_write_*` |

Two more rules hold for every key:

- A service that starts another service's command goes through
  `runtime::run_command`, so the policy applies to it too.
- A serial command whose turn is dropped while it waits keeps the next one
  behind the one before it (unit test
  `a_turn_dropped_while_waiting_keeps_the_next_one_behind_the_one_before`).

## Stale responses (`LatestRequest`)

Admission decides what may run. It does not decide which answer lands: reads
that a newer read replaces run concurrently, and request order is not response
order. Those reads take a generation from a `LatestRequest` before they ask, and
drop their answer if a newer request (or a close, a clear, or a newer selection)
has taken one since.

The rule: **only the newest answer lands, and a refusal is an answer.** A stale
failure must no more set an error status or raise a notification than a stale
success may replace the data.

| Read | Generation | Pinned by |
|---|---|---|
| map and mod vault search | `maps.search_generation`, `mods.search_generation` | `stale_responses.rs` |
| report target by name | `reporting.generation` | `stale_responses.rs`, `reporting.rs` |
| clan invite candidates | `clan.candidate_generation` | `stale_responses.rs` |
| clan identity and roster, the reload after a write included | `clan.load_generation` | `stale_reads.rs` |
| changelog entry | `changelog.entry_generation` | `changelog.rs` |
| leaderboard ratings, seasons, season board | `leaderboard.{ratings, seasons, season}_generation` | `stale_reads.rs` |
| co-op catalogue and board | `coop.{catalog, leaderboard}_generation` | `stale_reads.rs` |
| guides queue | `guides.queue_generation` | `stale_reads.rs` |
| player card profile, history, matchmaker, map stats | `player_card.*_generation` | `stale_reads.rs` |
| reviews | `reviews.generation` | `stale_reads.rs` |
| replay vault search, local replays | `replays.{vault, local}_generation` | `stale_replay_and_tourney_reads.rs` |
| tournament detail, chat room, account search | `tourney.{detail, chat, account_search}_generation` | `stale_replay_and_tourney_reads.rs` |
| tournament rating check, player ratings, template | `tourney.{rating_check, player_ratings, template}_generation` | `tourney_eligibility.rs` |
| sign-in, restore, logout | `auth.generation` | `auth.rs`, `command_contracts.rs` |

Known gaps:

- `leaderboard.catalog_generation` is not reachable through commands: `LoadCatalog`
  returns early while the catalogue is loading or loaded.
- `TourneyRead::RefreshChat`, `PinRoom` and `LoadChat` take no generation. A
  slow poll of the open room sent before a post can land after the post's own
  re-read and hide the new post until the next poll.
- The lobby's `match_generation` (the found-match watchdog) is unit-tested in
  `services/lobby/matchmaking.rs` only; an integration test would need paused
  time.

## Interrupted work on disk

Real-client startup sweeps only private `.faf-install-<16 hex digits>`
directories in the configured maps, mods, generator output and Galactic War
folders, `.faf-download-<16 hex digits>` files in the client temp directory,
`upload-{map,mod}-<pid>.zip` files in the cache, and versioned
`MapGenerator_*.partial` downloads in the configured generator cache. Links
and unrelated names are retained. Replacement recovery restores the old mod if a hard kill landed
between moving it aside and publishing the staged replacement; invalid recovery
records are retained rather than deleting the only remaining copy.

Generated maps are written into private staging under a per-generator lease.
Dropping a join or replay's progress receiver cancels that run, not another run.
Only completed maps are moved into the selected output directory. The stdout
and stderr readers decode lines lossily so Windows console code-page bytes do
not close progress reporting.
