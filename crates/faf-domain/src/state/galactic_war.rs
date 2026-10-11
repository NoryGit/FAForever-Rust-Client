//! Galactic War slice: launching a game mode that ships its own client.
//!
//! Galactic War is not played inside this client. It has its own application,
//! published as a per-platform archive on FAF's download server, and it does
//! its own login. So this slice owns exactly three jobs, and deliberately no
//! others: know which build is installed, install or update it, and start it.
//! No lobby, no connectivity adapter, no game launch arguments.
//!
//! ## Status is what is *happening*, not what is *true*
//!
//! [`GalacticWarStatus`] carries only the transient machine: a download, an
//! extraction, a running process. Whether an update is available, whether the
//! installed build is too old to connect, whether it can be launched at all:
//! those are **derived** from the installed version and the gateway's version
//! document, by the methods on [`GalacticWarState`]. Storing them as status
//! variants would let the two disagree, and the disagreement would decide what
//! a button does.
//!
//! ## Two comparisons, on purpose
//!
//! The gateway publishes a *minimum* version, and will publish a *latest* one
//! (see [`ClientVersions`]). The two questions that follow need different
//! machinery:
//!
//! * **Should we install something?** Pure string inequality against the
//!   install target: the server's pointer moved, so follow it. This needs no
//!   version ordering, so it survives any future numbering scheme unharmed.
//! * **Is what we have too old to connect?** That is genuinely an ordering
//!   question, answered by [`is_below_minimum`] using [`compare_versions`]. It
//!   is guarded by [`is_release_version`] on both sides, because
//!   `compare_versions` reports `Equal` for anything it cannot parse: a scheme
//!   change would otherwise silently mean "new enough" forever. Unparseable
//!   means we make **no** claim and show no warning, while the pointer
//!   comparison above keeps updating regardless. The ordering-dependent path
//!   is therefore confined to a warning, never to the update itself.
//!
//! The answer to the second question is *stored*, in `below_minimum`, rather
//! than recomputed by whoever asks. Everything else the UI needs is a string
//! comparison or an enum check it can reproduce in a line, but version
//! ordering is thirty lines of parsing that the frontend mirror would have to
//! reimplement and keep in step. Storing the one bit keeps that logic in
//! exactly one language. The service recomputes it whenever either input
//! changes, which are both events it emits itself.

use serde::{Deserialize, Serialize};
use specta::Type;

use crate::state::client_update::{compare_versions, is_release_version};

pub use crate::protocol::galactic_war::{
    ClientVersions, GalacticWarAlltime, GalacticWarFaction, GalacticWarSeason,
    GalacticWarStatistics,
};

/// What the client is currently *doing* about the Galactic War installation.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum GalacticWarStatus {
    /// Nothing in flight. Says nothing about whether anything is installed.
    #[default]
    Idle,
    /// Asking the gateway which version to have.
    CheckingVersion,
    /// Fetching the archive. Byte counts are `u32` for the same reason as
    /// everywhere else on this boundary: specta rejects 64-bit integers, and
    /// the archive is under 50 MB.
    #[serde(rename_all = "camelCase")]
    Downloading {
        version: String,
        downloaded_bytes: u32,
        /// Zero when the server sent no `Content-Length`.
        total_bytes: u32,
    },
    /// Unpacking. A separate stage because the archive expands to well over a
    /// hundred megabytes, which is long enough that a silent gap reads as a
    /// hang.
    Installing {
        version: String,
    },
    /// The process has been asked to start but has not been observed running.
    Launching,
    /// The Galactic War client is running. Re-entry is refused while it is.
    Running,
    Failed {
        reason: String,
    },
}

impl GalacticWarStatus {
    /// Whether an operation is in flight: the UI refuses re-entry on this.
    pub fn is_busy(&self) -> bool {
        matches!(
            self,
            GalacticWarStatus::CheckingVersion
                | GalacticWarStatus::Downloading { .. }
                | GalacticWarStatus::Installing { .. }
                | GalacticWarStatus::Launching
        )
    }
}

/// Where the season statistics stand.
///
/// A separate status from [`GalacticWarStatus`] on purpose. The statistics are
/// decoration; installing and launching is the point of the tab. A gateway
/// that is unreachable, or that has moved its schema past what we decode, must
/// never take the Play button with it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
pub enum StatisticsStatus {
    #[default]
    Idle,
    Loading,
    Loaded,
    Failed {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct GalacticWarState {
    pub status: GalacticWarStatus,
    /// The version recorded by the last successful install, or `None` when
    /// nothing is installed.
    pub installed_version: Option<String>,
    /// The gateway's version document, once read.
    pub versions: Option<ClientVersions>,
    /// Whether the installed build is below the gateway's stated minimum.
    ///
    /// Computed by [`is_below_minimum`] and published by the service; see the
    /// module doc for why this is stored rather than derived on demand.
    /// `false` whenever the question cannot be answered, which is the honest
    /// default: no claim rather than a wrong one.
    pub below_minimum: bool,
    /// The last statistics that decoded. Kept across a later failure so a
    /// flaky gateway blanks the panel for no longer than it has to.
    pub statistics: Option<GalacticWarStatistics>,
    pub statistics_status: StatisticsStatus,
}

/// Whether `installed` is older than the gateway's `required` minimum.
///
/// `false` whenever either side is not a version this client can order. See
/// the module doc: an unrecognised scheme means no claim, never a warning
/// nobody can act on.
pub fn is_below_minimum(installed: &str, required: &str) -> bool {
    is_release_version(installed)
        && is_release_version(required)
        && compare_versions(installed, required).is_lt()
}

impl GalacticWarState {
    /// The version that *should* be installed, per the gateway.
    pub fn install_target(&self) -> Option<&str> {
        self.versions
            .as_ref()
            .and_then(ClientVersions::install_target)
    }

    pub fn is_installed(&self) -> bool {
        self.installed_version.is_some()
    }

    /// Whether the installed build differs from the one the gateway points at.
    ///
    /// Inequality, not ordering: see the module doc. `false` while nothing is
    /// installed, because that is a first install, a different affordance.
    pub fn update_available(&self) -> bool {
        match (self.installed_version.as_deref(), self.install_target()) {
            (Some(installed), Some(target)) => installed != target,
            _ => false,
        }
    }

    /// Recompute [`Self::below_minimum`] from what is currently known.
    ///
    /// The service calls this after either input changes and publishes the
    /// result; nothing reads it directly.
    pub fn recheck_minimum(&self) -> bool {
        match (self.installed_version.as_deref(), &self.versions) {
            (Some(installed), Some(versions)) => {
                is_below_minimum(installed, &versions.required_version)
            }
            _ => false,
        }
    }

    pub fn is_busy(&self) -> bool {
        self.status.is_busy()
    }

    /// Whether starting the game right now would work.
    ///
    /// A build known to be below the minimum is refused: the launch would
    /// reach the login and fail there, which tells the user less than the
    /// update prompt does.
    pub fn can_launch(&self) -> bool {
        self.is_installed()
            && !self.is_busy()
            && self.status != GalacticWarStatus::Running
            && !self.below_minimum
    }

    /// Whether the gateway answered the last time we asked.
    ///
    /// This is what the `/status` endpoint would have reported. Reading it off
    /// the statistics call instead keeps the tab to one request.
    pub fn online(&self) -> bool {
        self.statistics_status == StatisticsStatus::Loaded
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(
    tag = "type",
    content = "payload",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum GalacticWarEvent {
    StatusChanged {
        status: GalacticWarStatus,
    },
    /// What is on disk changed: a finished install, or a discovery run at
    /// startup finding nothing.
    InstallationChanged {
        version: Option<String>,
    },
    VersionsLoaded {
        versions: ClientVersions,
    },
    /// The result of [`GalacticWarState::recheck_minimum`], published after
    /// either of its inputs changed.
    MinimumCheckChanged {
        below_minimum: bool,
    },
    /// Statistics moved to loading or failed. The loaded case is
    /// [`GalacticWarEvent::StatisticsLoaded`], which carries the data.
    StatisticsStatusChanged {
        status: StatisticsStatus,
    },
    StatisticsLoaded {
        statistics: GalacticWarStatistics,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(
    tag = "type",
    content = "payload",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum GalacticWarCommand {
    CancelInstall,
    /// Re-read what is installed, what the gateway advertises, and the season
    /// statistics. Cheap enough to run on entering the tab.
    Refresh,
    /// Re-read the season statistics and nothing else.
    ///
    /// The headline of those statistics -- how many people are in Galactic War
    /// right now -- is printed on the Play tab's mode strip, where it used to
    /// stand at whatever it was when the tab was last entered. "It says x
    /// players online, I open it and there is nobody" is what a number that
    /// never moves looks like. The install half is deliberately not repeated:
    /// a poll must not put the Play button back into "checking" every minute.
    RefreshStatistics,
    /// Install the current target without starting anything.
    Install,
    /// Get the user into the game: install or update first if needed, then
    /// launch. The tab's single primary action.
    Play,
}

pub fn reduce(state: &mut GalacticWarState, event: &GalacticWarEvent) {
    match event {
        GalacticWarEvent::StatusChanged { status } => state.status = status.clone(),
        GalacticWarEvent::InstallationChanged { version } => {
            state.installed_version = version.clone()
        }
        GalacticWarEvent::VersionsLoaded { versions } => state.versions = Some(versions.clone()),
        GalacticWarEvent::MinimumCheckChanged { below_minimum } => {
            state.below_minimum = *below_minimum
        }
        GalacticWarEvent::StatisticsStatusChanged { status } => {
            state.statistics_status = status.clone()
        }
        GalacticWarEvent::StatisticsLoaded { statistics } => {
            state.statistics = Some(statistics.clone());
            state.statistics_status = StatisticsStatus::Loaded;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn versions(required: &str, latest: Option<&str>) -> ClientVersions {
        ClientVersions {
            required_version: required.into(),
            latest_version: latest.map(Into::into),
        }
    }

    /// A state as the service would leave it: the minimum check already run,
    /// so `can_launch` reads what it would read in the running client.
    fn installed(version: &str, required: &str, latest: Option<&str>) -> GalacticWarState {
        let mut state = GalacticWarState {
            installed_version: Some(version.into()),
            versions: Some(versions(required, latest)),
            ..Default::default()
        };
        state.below_minimum = state.recheck_minimum();
        state
    }

    #[test]
    fn defaults_to_idle_with_nothing_known() {
        let s = GalacticWarState::default();
        assert_eq!(s.status, GalacticWarStatus::Idle);
        assert!(!s.is_installed());
        assert!(!s.is_busy());
        assert!(!s.update_available());
        assert!(!s.below_minimum);
        assert!(!s.recheck_minimum());
        assert!(!s.can_launch());
        assert!(!s.online());
        assert_eq!(s.install_target(), None);
    }

    #[test]
    fn every_in_flight_status_reads_as_busy() {
        for status in [
            GalacticWarStatus::CheckingVersion,
            GalacticWarStatus::Downloading {
                version: "v1".into(),
                downloaded_bytes: 1,
                total_bytes: 2,
            },
            GalacticWarStatus::Installing {
                version: "v1".into(),
            },
            GalacticWarStatus::Launching,
        ] {
            assert!(status.is_busy(), "{status:?} should be busy");
        }
        for status in [
            GalacticWarStatus::Idle,
            GalacticWarStatus::Running,
            GalacticWarStatus::Failed { reason: "x".into() },
        ] {
            assert!(!status.is_busy(), "{status:?} should not be busy");
        }
    }

    #[test]
    fn the_install_target_is_latest_when_the_gateway_sends_it() {
        let s = installed("v1", "v1", Some("v3"));
        assert_eq!(s.install_target(), Some("v3"));
        assert!(s.update_available());
    }

    #[test]
    fn the_install_target_falls_back_to_the_minimum() {
        let s = installed("v1", "v2", None);
        assert_eq!(s.install_target(), Some("v2"));
        assert!(s.update_available());
    }

    #[test]
    fn matching_the_target_is_not_an_update() {
        let s = installed("v2026.04.04.1", "v2026.03.01.1", Some("v2026.04.04.1"));
        assert!(!s.update_available());
        assert!(s.can_launch());
    }

    #[test]
    fn nothing_installed_is_not_an_update() {
        let s = GalacticWarState {
            versions: Some(versions("v1", None)),
            ..Default::default()
        };
        assert!(!s.update_available());
        assert!(!s.can_launch());
    }

    #[test]
    fn an_unrecognisable_scheme_still_triggers_the_update() {
        // Neither side orders, so `too_old` makes no claim, but the pointer
        // moved and that is all the update needs.
        let s = installed("build-41", "build-42", None);
        assert!(s.update_available());
        assert!(!s.below_minimum);
        assert!(s.can_launch());
    }

    #[test]
    fn a_build_below_the_minimum_is_too_old_and_cannot_launch() {
        let s = installed("v2026.03.01.1", "v2026.04.04.1", None);
        assert!(s.below_minimum);
        assert!(!s.can_launch());
    }

    #[test]
    fn a_build_above_the_minimum_is_not_too_old() {
        let s = installed("v2026.05.01.1", "v2026.04.04.1", Some("v2026.05.01.1"));
        assert!(!s.below_minimum);
        assert!(!s.update_available());
        assert!(s.can_launch());
    }

    #[test]
    fn an_unorderable_version_makes_no_age_claim_on_either_side() {
        assert!(!is_below_minimum("build-41", "v2026.04.04.1"));
        assert!(!is_below_minimum("v2026.03.01.1", "nightly"));
        assert!(!installed("build-41", "v2026.04.04.1", None).below_minimum);
        assert!(!installed("v2026.03.01.1", "nightly", None).below_minimum);
    }

    #[test]
    fn the_minimum_check_is_published_rather_than_recomputed_by_readers() {
        let mut s = installed("v2026.03.01.1", "v2026.04.04.1", None);
        // The state says so only because something emitted it. An install
        // that has not been re-checked yet still reads as launchable, which
        // is why the service emits the check after every change.
        s.below_minimum = false;
        assert!(s.can_launch());
        assert!(s.recheck_minimum(), "the check itself still says otherwise");

        reduce(
            &mut s,
            &GalacticWarEvent::MinimumCheckChanged {
                below_minimum: true,
            },
        );
        assert!(!s.can_launch());
    }

    #[test]
    fn a_busy_or_running_client_cannot_be_launched_again() {
        for status in [
            GalacticWarStatus::Running,
            GalacticWarStatus::Launching,
            GalacticWarStatus::Installing {
                version: "v1".into(),
            },
        ] {
            let s = GalacticWarState {
                status,
                ..installed("v1", "v1", None)
            };
            assert!(!s.can_launch(), "{:?} should block a launch", s.status);
        }
    }

    #[test]
    fn a_failed_run_does_not_block_the_next_one() {
        let s = GalacticWarState {
            status: GalacticWarStatus::Failed {
                reason: "network".into(),
            },
            ..installed("v1", "v1", None)
        };
        assert!(s.can_launch());
    }

    #[test]
    fn a_finished_install_records_its_version() {
        let mut s = GalacticWarState::default();
        reduce(
            &mut s,
            &GalacticWarEvent::InstallationChanged {
                version: Some("v2026.04.04.1".into()),
            },
        );
        assert_eq!(s.installed_version.as_deref(), Some("v2026.04.04.1"));
        assert!(s.is_installed());
    }

    #[test]
    fn a_discovery_run_finding_nothing_clears_the_installation() {
        let mut s = installed("v1", "v1", None);
        reduce(
            &mut s,
            &GalacticWarEvent::InstallationChanged { version: None },
        );
        assert!(!s.is_installed());
    }

    #[test]
    fn loading_statistics_marks_the_gateway_online() {
        let mut s = GalacticWarState::default();
        reduce(
            &mut s,
            &GalacticWarEvent::StatisticsStatusChanged {
                status: StatisticsStatus::Loading,
            },
        );
        assert!(!s.online());

        let statistics = GalacticWarStatistics {
            season: GalacticWarSeason {
                name: "Testing Season 4".into(),
                num_online_players: 4,
                ..Default::default()
            },
            ..Default::default()
        };
        reduce(
            &mut s,
            &GalacticWarEvent::StatisticsLoaded {
                statistics: statistics.clone(),
            },
        );
        assert_eq!(s.statistics_status, StatisticsStatus::Loaded);
        assert!(s.online());
        assert_eq!(s.statistics, Some(statistics));
    }

    #[test]
    fn a_statistics_failure_keeps_the_last_good_data_and_the_play_button() {
        let mut s = installed("v1", "v1", None);
        reduce(
            &mut s,
            &GalacticWarEvent::StatisticsLoaded {
                statistics: GalacticWarStatistics::default(),
            },
        );
        reduce(
            &mut s,
            &GalacticWarEvent::StatisticsStatusChanged {
                status: StatisticsStatus::Failed {
                    reason: "gateway unreachable".into(),
                },
            },
        );
        assert!(s.statistics.is_some(), "the last good data is kept");
        assert!(!s.online());
        // The whole point of the separate status: statistics failing does not
        // touch the install machine.
        assert_eq!(s.status, GalacticWarStatus::Idle);
        assert!(s.can_launch());
    }

    #[test]
    fn download_progress_is_recorded_verbatim() {
        let mut s = GalacticWarState::default();
        let status = GalacticWarStatus::Downloading {
            version: "v2026.04.04.1".into(),
            downloaded_bytes: 12_000_000,
            total_bytes: 46_340_472,
        };
        reduce(
            &mut s,
            &GalacticWarEvent::StatusChanged {
                status: status.clone(),
            },
        );
        assert_eq!(s.status, status);
        assert!(s.is_busy());
    }
}
