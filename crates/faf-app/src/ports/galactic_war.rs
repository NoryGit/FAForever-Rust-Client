//! Galactic War boundary: the gateway API, the installation on disk, and the
//! external client process.
//!
//! One port rather than three, because all three concern the same external
//! thing and an implementation needs to see all of them at once: the version
//! the gateway advertises decides what to download, the download decides what
//! is on disk, and what is on disk decides what can be started.
//!
//! Installing is a *streaming* boundary like
//! [`MapGeneratorPort`](crate::ports::MapGeneratorPort), because it has two
//! slow stages the UI must be able to name: a ~40 MB download and an expansion
//! to well over a hundred megabytes.

use async_trait::async_trait;
use faf_domain::state::{ClientVersions, GalacticWarStatistics};
use tokio::sync::mpsc;

/// One step of an install run.
///
/// Deliberately not [`DownloadProgress`](crate::ports::DownloadProgress),
/// which the client's own updater uses: that one ends at a downloaded file,
/// while this continues into an extraction long enough that reporting it as
/// part of the download would look like a stall at 100%.
///
/// Always ends in [`InstallProgress::Finished`]. A receiver that sees the
/// channel close without one knows the producer died rather than succeeded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstallProgress {
    Downloading {
        received_bytes: u32,
        /// Zero when the server sent no `Content-Length`.
        total_bytes: u32,
    },
    /// Unpacking into a staging directory.
    Extracting,
    /// The version now installed, or why it could not be.
    Finished(Result<String, String>),
}

#[async_trait]
pub trait GalacticWarPort: Send + Sync {
    /// Season statistics from the gateway.
    ///
    /// Its own result rather than folded into any other call: statistics are
    /// decoration and must never be able to fail the install path with them.
    async fn statistics(&self) -> Result<GalacticWarStatistics, String>;

    /// What the gateway says should be installed, and what it will still
    /// accept.
    async fn versions(&self) -> Result<ClientVersions, String>;

    /// The version recorded by the last successful install, or `None` when
    /// nothing usable is on disk.
    ///
    /// Synchronous and cheap: it reads a small local manifest, and both the
    /// startup refresh and every launch attempt consult it. An installation
    /// whose executable has since been deleted must read as `None` here, not
    /// as a version that fails at launch.
    fn installed_version(&self) -> Option<String>;

    /// Download and install `version`, replacing whatever is installed.
    ///
    /// Implementations install into a directory named after the version and
    /// switch the manifest afterwards, rather than writing over the existing
    /// files: on Windows the running executable is locked, so an in-place
    /// update while the user has Galactic War open fails halfway with an IO
    /// error that means nothing to them.
    async fn install(&self, version: String) -> mpsc::Receiver<InstallProgress>;

    async fn install_cancellable(
        &self,
        version: String,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> mpsc::Receiver<InstallProgress> {
        self.install(version).await
    }

    /// Start the installed client. Returns once the process has been spawned,
    /// not once it has exited.
    ///
    /// No arguments and no credentials: Galactic War does its own login. This
    /// client hands it nothing, which is what keeps launching it free of any
    /// question about passing user data to a service outside FAF.
    async fn launch(&self) -> Result<(), String>;

    /// Whether the client this port started is still running.
    ///
    /// Answerable precisely because *we* spawned it: it drives the refusal to
    /// launch a second copy, and the return to idle once the user quits.
    fn is_running(&self) -> bool;

    /// Resolve when the running client exits.
    ///
    /// The default never resolves, which is the honest answer for a fake that
    /// never starts a process: no exit will ever happen.
    async fn wait_for_exit(&self) {
        std::future::pending::<()>().await
    }
}
