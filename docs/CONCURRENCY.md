# Concurrency contract

Which commands may overlap, what happens to the one that arrives while another
runs, and what calling work off guarantees. The table in
`crates/faf-app/src/runtime/command_policy.rs` is the source of truth; this page
says the same thing in prose and names the test that pins each row. Test paths
are under `crates/faf-app/tests/`.

## Lanes

Every command waits in a lane listed in the generated tables below, each with its own concurrency limit.

- **Ordinary**: everything not listed below. 64 commands run at once, 64 more
  wait; once both are full, `dispatch` waits and `try_dispatch` reports it.
- **Priority**: releases and navigation. Checked first, with its own slots, so it never waits behind ordinary work. The complete command list is generated below. Pinned by `command_contracts::a_priority_command_is_not_held_up_by_a_saturated_ordinary_lane`.

Because a release can overtake its start, a start that leaves the ordinary queue after a release of the same pair was dispatched is dropped instead of run (`runtime::ReleaseOrder`; unit test `a_start_overtaken_by_its_release_is_dropped` in `runtime/mod.rs`). Which half of which pair a command is belongs to its entry in the policy table (`command_policy::Pair`), so the table's exhaustive match covers it. Every priority command is either the release of a pair or names why it is none (`command_policy::Unpaired`): navigation, which calls nothing off; `Lobby::TerminateGame`, which ends whichever game runs and belongs to no one start; and `Replays::CancelReads`, which the runtime's replay read slots order against the reads of the same replay and panel. There is no priority entry without one of the two, and `command_policy::every_pair_has_a_start_and_a_release_under_the_same_key` checks that each pair has both halves.

Item-specific background pairs use the same item key for start and release: map folders are case-normalized, mod installs and updates share their uid, and replay downloads use their uid. Other background pairs have one item each. The generated pair table lists all starts and releases, including commands that share admission but have different cancellation. The six newer background pairs are pinned by `runtime/mod.rs` tests named `a_*_overtaken_by_its_cancel_is_dropped`.

## Background work and its cancel

What runs in the background registers with a `runtime::Cancellable` under its
key before its status reaches the screen, so the status bar's Cancel (or the
dialog's) can never arrive before there is anything to stop. Each run holds its
own ticket: one that settles late forgets only itself. The cancel reaches the
work one of two ways, named per row below: the service drops the port's future
(`tokio::select!` against the ticket), or the port is handed the token, or a
flag, and decides itself where stopping is still safe. Calling work off raises no failure notification. Its final state follows the outcome in the table; a stopped Linux runtime setup retains a retryable status rather than returning to idle.

| Work | Cancel | How it stops | Left on disk | Pinned by |
|---|---|---|---|---|
| Map install (`Maps::InstallMap`) | `Maps::CancelInstall { folder_name }`, any letter case | the port is handed the token and its answer is the outcome. While the folder is waited for and the archive downloads, that work is dropped and the temporary file goes with its handle; while the archive unpacks on the blocking pool, `vault_install::CallOff` asks the worker not to rename it into place, and the port waits for its answer. A cancel that comes after the rename (or between that look and the rename itself) is too late: the listing after it is not raced, and the install ends `Installed` | nothing, or the map installed and listed; the staging folder is removed, and the command (and the map folder's lease) is held until the worker returns | `background_cancellation::{cancelling_a_map_install_*, a_map_install_called_off_too_late_*}`, `infra::game_updater::maps::a_vault_install_called_off_*`, `infra::vault_install::an_install_called_off_before_its_rename_*` |
| Map uninstall (`Maps::UninstallMap`) | none: a half-deleted map is worse than either | holds the map folder's lease (`lease_map_folder`) and deletes on the blocking pool, so a replay or live-game staging of the same map waits for it | the folder gone or whole | `infra::game_updater::maps::an_uninstall_waits_for_a_staging_*` |
| Mod install (`Mods::InstallMod`) | `Mods::CancelInstall { uid }` | as the map install: the port is handed the token, drops the download, asks `CallOff` before the rename and waits for the worker's answer | nothing, or the mod installed and listed | `background_cancellation::{cancelling_a_mod_install_*, a_mod_install_called_off_too_late_*}` |
| Mod update (`Mods::UpdateMod`) | `Mods::CancelInstall { uid }` | the port is handed the token and looks at it during the download and once more before the old version is removed; past that it removes and unpacks on a task of its own and ignores the call-off, because stopping in between would leave neither version | the installed version untouched, or the update finished | `background_cancellation::cancelling_a_mod_update_*` |
| Library download (`Replays::DownloadVault`) | `Replays::CancelDownload { uid }` | future dropped; a write already on the blocking pool asks `CallOff` before it publishes the file | nothing under the replay's name; the temporary file is deleted | `background_cancellation::cancelling_a_library_download_*`, `infra::replay::vault::a_called_off_download_publishes_nothing` |
| Replay panel reads (`Replays::{LoadDetails, LoadAnalysis}`) | `Replays::CancelReads { uid, local_path, request_id }`, sent when the detail panel closes with a read running | futures dropped (a vault download stops where it is, the command-stream walk never starts, or stops at its next record); the analysis is called off and `ReadsCancelled` emitted under the analysis slot's lock, which a new read of the same replay also begins under, so the call-off never clears the loading line of the read that replaced it | the replay in the cache only if its download had finished | `background_cancellation::closing_a_replay_panel_*`, `ReplayDetailPanel.flows.dom.test.tsx` |
| Publish (`Uploads::Start`) | `Uploads::Cancel`, only while `UploadStatus::is_cancellable` (packing, or bytes still going out) | the port's flag (`UploadsPort::cancel_publish`): packing checks cancellation per file chunk and waits for worker cleanup; the request body stops on the next chunk; an atomic transition arbitrates Stop against a map's final archive chunk or a mod's confirmation; after commitment the flag is ignored and the run ends as it would have; the stream ends `Idle` when it stopped | nothing: the temporary archive is deleted with its handle, and by the packing worker itself when the packing was called off or failed | `background_cancellation::{cancelling_a_publish_*, a_publish_whose_bytes_are_all_sent_*}`, `infra::uploads::{a_publish_called_off_*, upload_cancellation_*}` |
| Client update download (`ClientUpdate::Download`) | `ClientUpdate::CancelDownload` | the port is handed the token; the adapter stops at its next await (a stalled connection included) and takes its error path, and the service reads the stream to its end, so `ClientUpdate` stays held until the worker has deleted its file: the next download writes the same `.partial` file | the `.partial` installer is deleted; the offer stays (`Available`), or the installer is `Ready` when it was complete before the cancel reached it | `background_cancellation::{cancelling_the_client_update_download_*, a_cancelled_update_download_is_over_*}` |
| Replay launch (`Replays::{WatchVault, WatchLive, OpenFile}`) | `Replays::CancelWatch`, from the starting dialog or the status bar | future dropped (see the runtime replay read-slot notes and the replay rows in `failure_paths`) | a vault replay's download is the launch's first measured step now, not the library download's status | `failure_paths::cancelling_a_replay_*` |
| Map generation | `MapGenerator::Cancel`, from its dialog or the status bar | the port's flag (see the `MapGenerator` row); the generator is killed, and once it has exited the folders this run made are removed (the requested map and the ones it announced, never one that was there before it started). A run that fails is cleared the same way | nothing of the run; a generator that does not exit within five seconds keeps its folder | `failure_paths::cancelling_*`, `infra::map_generator::{a_run_called_off_takes_*, a_failed_run_takes_*}` |
| Join, host and launch preparation (`Lobby::{Join, Host}`, a launch order) | `Lobby::CancelJoin`, from its dialog or the status bar; a newer join, host or launch order supersedes it; `Lobby::Disconnect` and a dropped socket call it off | the operation's token (`LobbyOperations::called_off`) is handed to the updater (`GameUpdaterPort::prepare_cancellable`) and to the simulation mods (`ModsPort::ensure_game_mods_cancellable`). The updater stops between two files of its checksum pass and of its downloads, drops a download still in flight, and stops before the map's download; a file whose write began is finished. The launcher lets `PREPARATION` go the moment the token is raised, and a preparation still waiting for it gives up at once. The mods stop between two mods and during a download; an approved replacement once begun is finished | every file already updated stays updated and whole, and the next run's checksum pass carries on from there; a stopped run writes no executable stamp, `fa_path.lua` or cache entry, and turns no mods on | `updater_cancellation::a_join_called_off_mid_update_*`, `infra::game_updater::update::the_file_loop_stops_*`, `runtime::policies::an_operations_token_is_raised_*`, `lobby_operations.rs` |
| Search preparation (`Lobby::StartSearch`) | `Lobby::Matchmake { start: false }` while `Preparing`; the end of the connection | the search's own token (`LobbyContext::search_preparation`) reaches the featured-mod update and the pool maps (`GameUpdaterPort::ensure_maps`, which stops between two maps); `PREPARATION` is let go at once. A party member's update for its leader's search stops when the search goes back to idle | as for a join; a pool map is staged whole or not at all | `updater_cancellation::a_search_stopped_mid_update_*`, `reconnects_and_retries::a_search_from_before_a_drop_*` |

| Lesson launch (`Training::LaunchLesson`) | `Training::CancelLessonLaunch` | its own ticket cancels updater preparation, drops the lesson-install future, and is checked before launch; it does not cancel a tutorial's ticket | completed updater files and a lesson whose install already committed can remain | `runtime::a_lesson_launch_overtaken_by_its_cancel_is_dropped`; launch ordering in `training_lessons.rs`; running lesson cancellation is not integration-tested |
| Tutorial launch (`Tutorials::Launch`) | `Tutorials::CancelLaunch` | its ticket reaches updater preparation and is checked before launch | completed updater files can remain; the game is not launched after cancellation | `tutorials::cancelling_tutorial_preparation_never_launches_the_game`, `runtime::a_tutorial_launch_overtaken_by_its_cancel_is_dropped` |
| Galactic War install or install-before-play (`GalacticWar::{Install, Play}`) | `GalacticWar::CancelInstall` | the port receives the token; the service drains the final outcome and suppresses Play's launch after cancellation, including an install that just committed | a completed installation stays recorded; temporary work follows the port's cleanup | `galactic_war_cancellation::cancelling_galactic_war_never_auto_launches_even_if_install_just_committed`, `runtime::a_galactic_war_install_or_play_overtaken_by_its_cancel_is_dropped` |
| Linux runtime setup (`LinuxRuntime::Prepare`) | `LinuxRuntime::Cancel` | the port receives the token; the service drains its final outcome before releasing the key, and keeps a stopped setup available for retry without a failure notification | completed components remain recorded for resume; games use the runtime only after the complete-install check succeeds | `runtime::a_linux_runtime_setup_overtaken_by_its_cancel_is_dropped` covers queued starts; running cancellation is not integration-tested |

Not stopped yet, each with its reason:

- A generated map behind a join is made by the generator, which `CancelJoin`
  does not reach; the join's updater is never started for it.
- The vault catalogue crawls (`MapVault`, `ModVault`) report their pages and can be called off (`Maps::CancelVaultLoad`, `Mods::CancelVaultLoad`), but no view offers it: the status bar leaves the catalogues out (issue 603). They are read in the background on every start for the names, sizes and pictures other views look up, nobody waits on them, and a crawl stopped for one view would only start again for the next.
- The replay launch's generated-map rebuild runs on the generator's own detached task, which `CancelWatch` does not reach.

## Admission and guards

The generated tables below enumerate both lanes, every admission kind, all keys, service guards and start/release pairs. `Concurrent` imposes no per-key admission restriction. `SingleFlight` drops a duplicate while its key runs. `Serial` queues commands with the same key in dispatch order. `Latest` cancels the preceding future with the same key. `ServiceGuarded` leaves admission concurrent here because a service owns a narrower or longer-lived guard. Services still arbitrate stale replies and commits after a future has been dropped.

<!-- BEGIN GENERATED COMMAND POLICY -->

Generated from `command_policy.rs`. Regenerate with `cargo test -p faf-app --lib regenerate_concurrency_tables -- --ignored`. Counts refer to exhaustive match patterns; `Tourney::Write` covers every tournament write.

### Lanes

| Lane | Policy patterns |
|---|---|
| `Ordinary` | 237 |
| `Priority` | 33 |

### Priority commands

- `Auth::CancelLogin`, `Auth::Logout`
- `Chat::Disconnect`
- `ClientUpdate::CancelDownload`
- `GalacticWar::CancelInstall`
- `Guides::CancelSignIn`
- `LinuxRuntime::Cancel`
- `Lobby::CancelJoin`, `Lobby::DeclineModReplacement`, `Lobby::Disconnect`, `Lobby::Matchmake { start: false }`, `Lobby::TerminateGame`
- `MapGenerator::Cancel`
- `Maps::CancelInstall`, `Maps::CancelVaultLoad`
- `Mods::CancelInstall`, `Mods::CancelVaultLoad`
- `Nav::LinkShown`, `Nav::OpenLink`, `Nav::SelectChangelogClientRelease`, `Nav::SelectChangelogSection`, `Nav::SelectMapsSection`, `Nav::SelectModsSection`, `Nav::SelectReplaysSection`, `Nav::SelectSettingsSection`, `Nav::Select`
- `Replays::CancelDownload`, `Replays::CancelLiveTracking`, `Replays::CancelReads`, `Replays::CancelWatch`
- `Training::CancelLessonLaunch`
- `Tutorials::CancelLaunch`
- `Uploads::Cancel`

### Admission kinds

| Admission | Policy patterns |
|---|---|
| `Concurrent` | 187 |
| `SingleFlight` | 22 |
| `Serial` | 20 |
| `Latest` | 5 |
| `ServiceGuarded` | 36 |

### Keys

| Key | Admission | Commands |
|---|---|---|
| `Contacts` | `SingleFlight` | `Social::ResolveContacts` |
| `ContactWrite` | `Serial` | `Social::SetRelation` |
| `ReplaySearch` | `Latest` | `Replays::SearchVault` |
| `MapSearch` | `Latest` | `Maps::SearchVault` |
| `ModSearch` | `Latest` | `Mods::SearchVault` |
| `RatingSearch` | `Latest` | `Leaderboard::LoadRatings` |
| `AccountLookup` | `Latest` | `PlayerCard::LookUpAccounts` |
| `EventQuery` | `Serial` | `Events::SetQuery` |
| `MapVault` | `SingleFlight` | `Maps::LoadVault` |
| `ModVault` | `SingleFlight` | `Mods::LoadVault` |
| `ModVaultCheck` | `SingleFlight` | `Mods::ReloadVault` |
| `Changelog` | `SingleFlight` | `Changelog::Load` |
| `MapFiles` | `Serial` | `Maps::InstallMap`, `Maps::UninstallMap` |
| `ModFiles` | `Serial` | `Mods::InstallMod`, `Mods::SetActiveMods`, `Mods::ToggleMod`, `Mods::UninstallMod`, `Mods::UpdateMod` |
| `MapGenerator` | `SingleFlight` | `MapGenerator::CleanUp`, `MapGenerator::GenerateNamed`, `MapGenerator::Generate` |
| `Upload` | `SingleFlight` | `Uploads::Start` |
| `ClientUpdate` | `SingleFlight` | `ClientUpdate::Check`, `ClientUpdate::Download`, `ClientUpdate::Install` |
| `ClientReleaseNotes` | `SingleFlight` | `ClientUpdate::LoadNotes` |
| `GalacticWar` | `SingleFlight` | `GalacticWar::Install`, `GalacticWar::Play` |
| `LinuxRuntime` | `SingleFlight` | `LinuxRuntime::Prepare` |
| `GuidesSignIn` | `SingleFlight` | `Guides::SignIn` |
| `GuidesVerdict` | `Serial` | `Guides::Accept`, `Guides::Reject` |
| `TutorialLaunch` | `SingleFlight` | `Training::LaunchLesson`, `Tutorials::Launch` |
| `TrainingLoad` | `SingleFlight` | `Training::Load` |
| `ClanWrite` | `Serial` | `Clan::AcceptInvitation`, `Clan::Create`, `Clan::Disband`, `Clan::Edit`, `Clan::HandOver`, `Clan::Leave`, `Clan::Remove` |
| `TourneyWrite` | `Serial` | `Tourney::Write` |
| `PartyPlacements` | `Serial` | `PlayerCard::LoadPartyPlacements` |
| `ConnectivityCheck` | `SingleFlight` | `Connectivity::RunCheck` |
| `RelayStatus` | `SingleFlight` | `Connectivity::RefreshRelayStatus` |

### Service guards

| Guard | Commands |
|---|---|
| `LobbyConnection` | `Lobby::Connect` |
| `ChatConnection` | `Chat::Connect` |
| `LobbyJoin` | `Lobby::Join` |
| `Login` | `Auth::Login`, `Auth::Logout`, `Auth::Restore` |
| `Settings` | `Settings::CheckInstalls`, `Settings::ClearGameCache`, `Settings::DeleteModPreset`, `Settings::DeleteSearchPreset`, `Settings::Load`, `Settings::PatchAppearance`, `Settings::PatchBrowsing`, `Settings::PatchChat`, `Settings::PatchConnectivity`, `Settings::PatchDebug`, `Settings::PatchDiscord`, `Settings::PatchEvents`, `Settings::PatchGame`, `Settings::PatchGeneral`, `Settings::PatchNotifications`, `Settings::PatchPaths`, `Settings::PatchUpdates`, `Settings::RefreshGameCache`, `Settings::RemoveNotificationSound`, `Settings::RenameReplayTag`, `Settings::SaveModPreset`, `Settings::SaveSearchPreset`, `Settings::SetGamePath`, `Settings::SetListMember`, `Settings::SetMapGenerator`, `Settings::SetPlayerNameColor`, `Settings::SetPlayerNote`, `Settings::SetReplayGamePath`, `Settings::SetReplayNote`, `Settings::SetTheme` |

### Start/release pairs

`Auth::LogoutTest` releases its pair in the ordinary lane; the other releases below use priority. Payload-dependent pair keys remain defined by the policy expressions and the cancellation prose above.

| Pair | Starts | Releases |
|---|---|---|
| `Join` | `Lobby::Host`, `Lobby::Join` | `Lobby::CancelJoin`, `Lobby::DeclineModReplacement` |
| `Lobby` | `Lobby::Connect` | `Lobby::Disconnect` |
| `Matchmake` | `Lobby::Matchmake { start: true }` | `Lobby::Matchmake { start: false }` |
| `Chat` | `Chat::Connect` | `Chat::Disconnect` |
| `Auth` | `Auth::LoginTest`, `Auth::Login`, `Auth::Restore` | `Auth::CancelLogin`, `Auth::LogoutTest`, `Auth::Logout` |
| `GuidesSignIn` | `Guides::SignIn` | `Guides::CancelSignIn` |
| `MapGenerator` | `MapGenerator::GenerateNamed`, `MapGenerator::Generate` | `MapGenerator::Cancel` |
| `ReplayWatch` | `Replays::OpenFile`, `Replays::WatchLive`, `Replays::WatchVault` | `Replays::CancelWatch` |
| `LiveTracking` | `Replays::TrackLive` | `Replays::CancelLiveTracking` |
| `ReplayDownload` | `Replays::DownloadVault` | `Replays::CancelDownload` |
| `MapInstall` | `Maps::InstallMap` | `Maps::CancelInstall` |
| `ModInstall` | `Mods::InstallMod`, `Mods::UpdateMod` | `Mods::CancelInstall` |
| `Upload` | `Uploads::Start` | `Uploads::Cancel` |
| `ClientDownload` | `ClientUpdate::Download` | `ClientUpdate::CancelDownload` |
| `MapVault` | `Maps::LoadVault` | `Maps::CancelVaultLoad` |
| `ModVault` | `Mods::LoadVault`, `Mods::ReloadVault` | `Mods::CancelVaultLoad` |
| `LessonLaunch` | `Training::LaunchLesson` | `Training::CancelLessonLaunch` |
| `TutorialLaunch` | `Tutorials::Launch` | `Tutorials::CancelLaunch` |
| `GalacticWarInstall` | `GalacticWar::Install`, `GalacticWar::Play` | `GalacticWar::CancelInstall` |
| `LinuxRuntime` | `LinuxRuntime::Prepare` | `LinuxRuntime::Cancel` |

<!-- END GENERATED COMMAND POLICY -->

## Runtime and service guarantees

These notes explain cancellation and test coverage beyond the generated policy mapping.

`MapVault` (single-flight): dropped; a loaded catalogue is also skipped, a failed one is retried. Cancellation: `Maps::CancelVaultLoad` drops all pending page requests; progress reports completed pages; retry is explicit. Tests: `command_contracts::a_second_single_flight_command_*`, `map_vault_load.rs`.

`ModVault` (single-flight): dropped. Cancellation: `Mods::CancelVaultLoad` drops all pending page and review requests; progress reports completed pages. Tests: `command_contracts::a_second_single_flight_command_*`, `failure_paths::a_failed_mod_vault_load_*`, `mods_reload.rs`.

`Changelog` (single-flight): dropped (the running load selects the newest patch itself). Cancellation: none. Tests: `command_contracts::a_second_single_flight_command_*`, `changelog.rs`.

`MapGenerator` (single-flight): dropped. Cancellation: `MapGenerator::Cancel`: during the preflight the run is never started; during a run the port stops it and, once the generator has exited, discards the run's private staging directory; ends `Cancelled`, no notification, nothing recorded, key free again. Tests: `command_contracts::a_second_single_flight_command_*`, `failure_paths::cancelling_*`, `infra::map_generator::a_run_called_off_takes_*`.

`Upload` (single-flight): dropped. Cancellation: `Uploads::Cancel` while bytes are still going out (see "Background work and its cancel"); closing the dialog only hides it. Tests: `command_contracts::a_second_single_flight_command_*`, `uploads.rs`, `background_cancellation.rs`.

`ClientUpdate` (single-flight): dropped (also for the startup and six-hourly checks, which go through `run_command`). Cancellation: `ClientUpdate::CancelDownload` stops a download (see "Background work and its cancel"); a check and an install cannot be stopped. Tests: `command_contracts::a_second_single_flight_command_*`, `client_update.rs`, `background_cancellation.rs`.

`ClientReleaseNotes` (single-flight): dropped (also the startup load for a pending What's new popup, which goes through `run_command`). Cancellation: none; the port answers from the release list the last check read, and asks GitHub only when nothing has been read yet. Tests: `command_contracts::a_second_single_flight_command_*`, `client_update.rs`.

`GalacticWar` (single-flight): dropped. Cancellation: `CancelInstall` stops download or extraction before commit; an installed result stays recorded, but Play does not auto-launch after cancellation. Tests: `command_contracts::a_second_single_flight_command_*`.

`GuidesSignIn` (single-flight): dropped. Cancellation: `Guides::CancelSignIn` stops the polling through the port. Tests: `command_contracts::a_second_single_flight_command_*`.

`TutorialLaunch` (single-flight): dropped. Cancellation: `CancelLaunch` cancels updater preparation and prevents launch. Tests: `command_contracts::a_second_single_flight_command_*`, `tutorials.rs`.

`ConnectivityCheck` (single-flight): dropped (its lines would interleave with the running check's). Cancellation: none; every probe is bounded by a few seconds. Tests: `command_contracts::a_second_single_flight_command_*`, `connectivity_check.rs`.

`RelayStatus` (single-flight): dropped (the settings page asks again on its next tick). Cancellation: none; the adapter's `status` call times out after 3 seconds. Tests: `command_contracts::a_second_single_flight_command_*`, `connectivity_check.rs`.

`MapFiles` (serial): queued, dispatch order. Cancellation: `Maps::CancelInstall` stops the running install (see "Background work and its cancel"); the key is free when it returns. Tests: `command_contracts::serial_commands_*`, `background_cancellation::cancelling_a_map_install_*`.

map folder lease (infra guard): waits for the writer holding that map's folder; an install or staging then finds the map in place and takes it as installed (no second download, no "already installed"), an uninstall removes what it finds. Cancellation: dropping the caller does not stop an extraction or a removal already on the blocking pool; the worker keeps the lease until it returns, and a called-off install's worker removes its staging instead of renaming it. Tests: `infra::game_updater::maps` unit tests (`a_vault_install_waits_for_a_replay_staging_*`, `a_replay_staging_waits_for_a_vault_install_*`, `a_map_folder_that_appears_during_staging_*`, `an_uninstall_waits_for_a_staging_*`, `a_vault_install_called_off_while_unpacking_*`).

Replay analysis (runtime read slots, not a `ServiceGuard`): for another replay: calls the running read off and starts; for the replay already being read by the same owner: dropped, the running read answers it. Cancellation: the running read's port future is dropped: a vault download stops, and the command-stream walk never starts if the file is still being read, and an already-running walk checks its cancellation on every record; a read called off emits nothing. `Replays::CancelReads` calls it off for its own replay and request owner when the panel closes, and emits `ReadsCancelled`. Tests: `stale_replay_and_tourney_reads::{a_superseded_analysis_*, asking_again_for_the_analysis_*}`, `background_cancellation::closing_a_replay_panel_*`, `infra::replay::reader` unit tests.

`ModFiles` (serial): queued, dispatch order. Cancellation: `Mods::CancelInstall` stops a running install, and an update still downloading (see "Background work and its cancel"). Tests: `command_contracts::serial_commands_*`, `background_cancellation::cancelling_a_mod_*`.

`GuidesVerdict` (serial): queued, dispatch order. Cancellation: none. Tests: `command_contracts::serial_commands_*`.

`ClanWrite` (serial): queued, dispatch order. Cancellation: none. Tests: `command_contracts::serial_commands_*`.

`TourneyWrite` (serial): queued, dispatch order. Cancellation: none. Tests: `command_contracts::serial_commands_*`, `tourney_write_order.rs`.

`PartyPlacements` (serial): queued, dispatch order; the next one asks only for ids still unknown. Cancellation: none. Tests: `command_contracts::serial_commands_*`, `failure_paths::a_*placement*`.

`LobbyConnection` (service guard): dropped while the socket is open or connecting. Cancellation: `Lobby::Disconnect` closes it and disarms the watchdog; any end of the socket (drop or disconnect) calls off a join or search being prepared, which is then never sent on the next connection. Tests: `command_contracts::the_lobby_socket_*`, `lobby.rs`, `lobby_operations.rs`, `reconnects_and_retries.rs`.

`ChatConnection` (service guard): dropped while the socket is open or connecting. Cancellation: `Chat::Disconnect` closes it and disarms the watchdog; after a drop the channels are joined again and the scrollback is kept. Tests: `command_contracts::the_chat_socket_*`, `reconnects_and_retries::chat_comes_back_*`.

`LobbyJoin` (service guard): dropped from the click until the server answers. Cancellation: `CancelJoin`, `Disconnect` or a drop free the slot; a join called off before its request is never sent, one called off after is never launched, and neither is resent after a reconnect. The updater behind it is stopped too, through the operation's token (see "Background work and its cancel"). Tests: `lobby_operations.rs`, `launch_preparation.rs`, `reconnects_and_retries.rs`, `updater_cancellation.rs`.

`Login` (service guard): queued behind the lock. Cancellation: `Logout`, `CancelLogin`, `LogoutTest`, `PlayOffline` cancel a login in progress instead of waiting for it; a `Restore` cannot be cancelled, so `Logout` waits for it, and its answer does not land. A login or restore only stages its session in the port; the service commits it (token current, refresh token saved, renewal started) under the cancellation lock and only while the attempt is current and not called off, and discards it otherwise, so a called-off or failed sign-in leaves nothing live. Tests: `command_contracts::calling_a_sign_in_off_*`, `command_contracts::a_restore_answering_*`, `auth.rs`, `auth_session.rs`, `infra::oauth` unit tests.

`Settings` (service guard): the merge (read, change, emit) is under one lock, so patches never revert each other; the write is under another, so documents reach the store one at a time in order. Cancellation: none; nothing is written before the file was read, and a failed write is carried by the next. Tests: `command_contracts::settings_writes_*`, `settings_patches.rs`, `settings_startup.rs`, `reconnects_and_retries::a_failed_settings_write_*`.

A manual mod update check uses `ModVaultCheck`, independently from `ModVault`, and waits for the background load inside the service's `catalogue_writer` before making its own request. The two commands share a cancel pair but not an admission key (`mods_reload.rs`). Lesson and tutorial launches share `TutorialLaunch` admission but have independent start/release pairs.

A service that starts another service's command goes through `runtime::run_command`, so the policy applies there too. A serial command whose turn is dropped while it waits keeps the next one behind the one before it (unit test `a_turn_dropped_while_waiting_keeps_the_next_one_behind_the_one_before`).

## Stale responses (`LatestRequest`)

Admission controls which command futures overlap. `Latest` also drops the preceding future on its key, so it participates in supersession. This does not replace service-side response and commit ordering: other reads remain concurrent, detached or blocking work can outlive a dropped future, and a close or newer selection can invalidate a result without admitting another command on that key. Those paths use a `LatestRequest` generation before asking and discard both a stale success and a stale failure.

The rule: **only the newest answer lands, and a refusal is an answer.** A stale
failure must no more set an error status or raise a notification than a stale
success may replace the data.

| Read | Generation | Pinned by |
|---|---|---|
| map and mod vault search | `maps.search_generation`, `mods.search_generation` | `stale_responses.rs` |
| report target by name | `reporting.generation` | `stale_responses.rs`, `reporting.rs` |
| report game log excerpt (also taken by open, close and untick) | `reporting.log_generation` | `reporting.rs` |
| clan invite candidates | `clan.candidate_generation` | `stale_responses.rs` |
| clan identity and roster, the reload after a write included | `clan.load_generation` | `stale_reads.rs` |
| changelog entry | `changelog.entry_generation` | `changelog.rs` |
| leaderboard ratings, seasons, season board | `leaderboard.{ratings, seasons, season}_generation` | `stale_reads.rs` |
| co-op catalogue and board | `coop.{catalog, leaderboard}_generation` | `stale_reads.rs` |
| guides queue | `guides.queue_generation` | `stale_reads.rs` |
| player card profile, history, matchmaker, map stats | `player_card.*_generation` | `stale_reads.rs` |
| reviews | `reviews.generation` | `stale_reads.rs` |
| replay vault search, local replays | `replays.{vault, local}_generation` | `stale_replay_and_tourney_reads.rs` |
| tournament detail, account search | `tourney.{detail, account_search}_generation` | `stale_replay_and_tourney_reads.rs` |
| tournament chat: opening a room (its loading state and its refusal) | `tourney.chat_generation`, and `tourney.chat_answers` below | `stale_replay_and_tourney_reads.rs` |
| tournament rating check, player ratings, template | `tourney.{rating_check, player_ratings, template}_generation` | `tourney_eligibility.rs` |
| sign-in, restore, logout | `auth.generation` | `auth.rs`, `command_contracts.rs` |

### Overlapping reads of one thing (`NewestAnswer`)

A generation is wrong for reads that overlap on purpose and are all equally
wanted: under one, a room polled every five seconds by a server that answers
in six would never update. Those reads take a ticket from a `NewestAnswer`
immediately before they ask, and an answer lands unless an answer from a later
ticket about the same key has already landed. Answers only move forward, and
none is dropped merely because another read started. A refusal that is shown
counts as an answer; a silent one (a failed poll) does not.

A write's re-read takes its ticket after the write has returned, so it outranks
every read sent before the write, and one of those answering late cannot hide
what was written.

| Reads | Key | Pinned by |
|---|---|---|
| tournament chat room: `OpenRoom`, `RefreshChat`, `PinRoom`, the re-read after `PostChat` and `DeleteChatPost` | `tourney.chat_answers`, per room id (what `ChatLoaded` names) | `stale_replay_and_tourney_reads.rs` (`a_poll_sent_before_a_post_never_hides_the_post`, `a_pinned_rooms_first_read_*`, `an_open_rooms_late_refusal_*`) |
| tournament chat room list: `LoadChat`, the list half of `RefreshChat`, the re-read after `PostChat` | `tourney.chat_answers`, one key for the list | `stale_replay_and_tourney_reads.rs` (`a_room_list_read_before_a_post_*`) |

The open room's read also stays under `tourney.chat_generation`: its loading
state and its refusal belong to the open room's pane, and opening another room
makes them worthless.

Known gaps:

- `leaderboard.catalog_generation` is not reachable through commands: `LoadCatalog`
  returns early while the catalogue is loading or loaded.
- A chat answer names its room but not its event, and room ids such as `global`
  repeat across events. A poll of one event's room still out when another event
  is opened is ordered against that event's room of the same name: it is
  dropped once that room's own read has landed, but can show for a moment if it
  arrives first.
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
