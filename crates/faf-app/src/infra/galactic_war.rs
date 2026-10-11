//! Galactic War: the gateway API, the installation on disk, and the process.
//!
//! ## What is trusted, and what is not
//!
//! This downloads an archive and then executes what comes out of it, so the
//! trust chain is worth stating precisely. It is the same length as
//! [`super::client_update`]'s and no longer:
//!
//! - **TLS to the download host.** Enforced by refusing any download URL that
//!   is not `https://`, unless the configured base is itself plain HTTP, which
//!   only a deliberate local test setup does.
//! - **The configured download root.** The URL is *built* from the version the
//!   gateway reports, never taken from a response body, and the version must
//!   pass [`is_safe_version`] before it reaches a path.
//! - **A `.sha256` beside the archive, when one exists.** The publisher does
//!   not ship one today. Requesting it costs one round trip and makes the
//!   check live the day the file appears, with no client release. Be clear
//!   about what it buys: it catches a truncated or corrupted download, and it
//!   is *not* authenticity, because whoever could serve a hostile archive
//!   could serve a matching hash or a 404 just as easily.
//! - **Nothing else.** There is no signature check, for the same reason as the
//!   client's own updater: nobody publishes a signature to verify against.
//!
//! ## Why installs are versioned directories
//!
//! Each version installs into its own directory and a small manifest is
//! switched afterwards, rather than writing over the files in place. On
//! Windows a running executable is locked, so an in-place update while the
//! user has Galactic War open fails halfway and leaves a broken installation.
//! A directory per version also makes the install atomic from the client's
//! point of view: the manifest either points at a complete install or at the
//! old one.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use faf_domain::protocol::galactic_war::{
    download_url, is_safe_version, parse_client_versions, parse_statistics, version_path,
    CONTENT_PACK_NAME, EXECUTABLE_NAME,
};
use faf_domain::state::{ClientVersions, GalacticWarStatistics};
use futures_util::StreamExt as _;
use sha2::{Digest as _, Sha256};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, Notify};

use crate::infra::{data_dir, env_or, http::shared_http_client, vault_install};
use crate::ports::{GalacticWarPort, InstallProgress};

/// Generous for a ~45 MB archive, bounded enough that a broken or hostile
/// server cannot exhaust memory.
const MAX_ARCHIVE_BYTES: u64 = 256 * 1024 * 1024;
/// The API answers in milliseconds when it answers at all. A tab that waits a
/// minute for statistics has already failed at its job.
const API_TIMEOUT: Duration = Duration::from_secs(15);
/// Names the file recording which version is installed.
const MANIFEST_NAME: &str = "installed.json";
/// How much has to arrive before another progress event is worth sending.
const PROGRESS_STEP_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct GalacticWarConfig {
    /// Root of the gateway's HTTP API.
    ///
    /// Configurable rather than a constant because where this is hosted is an
    /// operations decision that is still open: FAF may put it behind its own
    /// domain. Moving it must not be a code change.
    pub api_base: String,
    /// Root of the release download server. A download URL is built under
    /// this and refused if it does not stay there.
    pub download_base: String,
    /// Where versions are installed.
    pub install_dir: PathBuf,
}

impl GalacticWarConfig {
    pub fn faf() -> Result<Self, String> {
        Ok(Self {
            api_base: env_or("FAF_GW_API_BASE", "https://galactic-war-test.spidarna.com"),
            // The archive this names is unpacked and executed.
            download_base: crate::infra::dev_env_or(
                "FAF_GW_DOWNLOAD_BASE",
                "https://downloads.faforever.com",
            ),
            install_dir: data_dir()?.join("galactic-war"),
        })
    }
}

pub struct GalacticWarGateway {
    config: GalacticWarConfig,
    http: reqwest::Client,
    child: Arc<Mutex<Option<Child>>>,
    /// Woken when the tracked client exits; see [`Self::watch_for_exit`].
    exited: Arc<Notify>,
}

impl GalacticWarGateway {
    pub fn new(config: GalacticWarConfig) -> Self {
        Self {
            config,
            http: shared_http_client(),
            child: Arc::new(Mutex::new(None)),
            exited: Arc::new(Notify::new()),
        }
    }

    pub fn faf() -> Result<Self, String> {
        Ok(Self::new(GalacticWarConfig::faf()?))
    }

    fn api_url(&self, path: &str) -> String {
        format!("{}/{path}", self.config.api_base.trim_end_matches('/'))
    }

    async fn get_text(&self, url: &str, subject: &str) -> Result<String, String> {
        let response = self
            .http
            .get(url)
            .timeout(API_TIMEOUT)
            .send()
            .await
            .map_err(|error| format!("could not reach the Galactic War {subject}: {error}"))?;
        if !response.status().is_success() {
            // The spec promises `/statistics` always answers 200. Promises
            // about current behaviour are not guarantees, and a proxy in front
            // of it makes none at all.
            return Err(format!(
                "the Galactic War {subject} answered {}",
                response.status()
            ));
        }
        response
            .text()
            .await
            .map_err(|error| format!("could not read the Galactic War {subject}: {error}"))
    }

    fn manifest_path(&self) -> PathBuf {
        self.config.install_dir.join(MANIFEST_NAME)
    }

    fn version_dir(&self, version: &str) -> PathBuf {
        self.config.install_dir.join(version)
    }

    /// The executable of the recorded version, if the manifest names one and
    /// the file is still there.
    fn installed_executable(&self) -> Option<(String, PathBuf)> {
        let manifest = std::fs::read_to_string(self.manifest_path()).ok()?;
        let value: serde_json::Value = serde_json::from_str(&manifest).ok()?;
        let version = value.get("version")?.as_str()?.to_string();
        // A manifest is a local file, but it is also the only input that
        // becomes a path here, and a stale one can outlive an interrupted
        // uninstall. Re-check the shape rather than trusting what was written.
        if !is_safe_version(&version) {
            return None;
        }
        let executable = self.version_dir(&version).join(EXECUTABLE_NAME);
        executable.is_file().then_some((version, executable))
    }

    fn write_manifest(&self, version: &str) -> Result<(), String> {
        let manifest = serde_json::json!({ "version": version });
        std::fs::create_dir_all(&self.config.install_dir).map_err(|error| {
            format!(
                "could not create {}: {error}",
                self.config.install_dir.display()
            )
        })?;
        std::fs::write(self.manifest_path(), manifest.to_string())
            .map_err(|error| format!("could not record the installed version: {error}"))
    }

    /// Delete every installed version except `keep`.
    ///
    /// Best effort: a directory that will not go (the previous build is still
    /// running, most likely) is left for the next install to clear. Failing
    /// the install over it would be worse than a wasted 130 MB.
    fn remove_other_versions(&self, keep: &str) {
        let Ok(entries) = std::fs::read_dir(&self.config.install_dir) else {
            return;
        };
        for entry in entries.flatten() {
            if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                continue;
            }
            if entry.file_name() == keep {
                continue;
            }
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }

    /// Reject a download URL that does not stay under the configured root.
    ///
    /// The trailing slash is load-bearing: comparing against a bare
    /// `https://downloads.faforever.com` would also accept
    /// `https://downloads.faforever.com.example.invalid/`, which is a
    /// different host that merely starts with the same characters.
    ///
    /// `..` is refused outright: a URL parser would normalise it away, past
    /// the prefix this check just verified.
    fn is_trusted(&self, url: &str) -> bool {
        let base = format!("{}/", self.config.download_base.trim_end_matches('/'));
        let secure = url.starts_with("https://") || base.starts_with("http://");
        secure && url.starts_with(&base) && !url.contains("..")
    }

    /// Fetch the `.sha256` published beside the archive, if there is one.
    ///
    /// A missing file is `Ok(None)`, not an error: none is published today.
    async fn expected_digest(&self, url: &str) -> Option<String> {
        let response = self
            .http
            .get(format!("{url}.sha256"))
            .timeout(API_TIMEOUT)
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let body = response.text().await.ok()?;
        // `sha256sum` writes "<hex>  <filename>"; take the first field.
        let digest = body.split_whitespace().next()?.to_ascii_lowercase();
        (digest.len() == 64 && digest.chars().all(|c| c.is_ascii_hexdigit())).then_some(digest)
    }

    /// Download the archive, reporting progress, without trusting
    /// `Content-Length` or buffering without bound.
    async fn download_archive(
        &self,
        url: &str,
        progress: &mpsc::Sender<InstallProgress>,
    ) -> Result<Vec<u8>, String> {
        let response = self.http.get(url).send().await.map_err(|error| {
            format!(
                "could not download the Galactic War client: {}",
                crate::infra::http::describe_transport_error(&error)
            )
        })?;
        if !response.status().is_success() {
            return Err(format!(
                "the download server answered {}",
                response.status()
            ));
        }
        let total = response.content_length().unwrap_or(0);
        if total > MAX_ARCHIVE_BYTES {
            return Err("the Galactic War archive is larger than the allowed size".into());
        }

        let mut body = Vec::with_capacity(total.min(MAX_ARCHIVE_BYTES) as usize);
        let mut received = 0_u64;
        let mut reported = 0_u64;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| {
                format!(
                    "could not read the Galactic War archive: {}",
                    crate::infra::http::describe_transport_error(&error)
                )
            })?;
            received = received
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| "the Galactic War archive is too large".to_string())?;
            if received > MAX_ARCHIVE_BYTES {
                return Err("the Galactic War archive is larger than the allowed size".into());
            }
            body.extend_from_slice(&chunk);

            // One event per megabyte rather than per chunk. Every event is a
            // full state reduce plus a broadcast to the frontend, and the
            // button shows whole megabytes anyway: a 45 MB archive becomes
            // about 45 updates instead of several thousand.
            if received - reported >= PROGRESS_STEP_BYTES {
                reported = received;
                let _ = progress
                    .send(InstallProgress::Downloading {
                        received_bytes: clamp_u32(received),
                        total_bytes: clamp_u32(total),
                    })
                    .await;
            }
        }
        Ok(body)
    }

    async fn install_version(
        &self,
        version: String,
        progress: mpsc::Sender<InstallProgress>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<String, String> {
        if self.is_running() {
            return Err("close Galactic War before updating it".into());
        }
        let url = download_url(&self.config.download_base, &version)?;
        if !self.is_trusted(&url) {
            return Err("refusing a download outside the configured FAF download server".into());
        }

        let (expected, archive) = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err("installation cancelled".into()),
            result = async {
                let expected = self.expected_digest(&url).await;
                let archive = self.download_archive(&url, &progress).await?;
                Ok::<_, String>((expected, archive))
            } => result?,
        };
        if let Some(expected) = expected {
            let actual = format!("{:x}", Sha256::digest(&archive));
            if actual != expected {
                return Err("the downloaded Galactic War archive is damaged".into());
            }
        }

        let _ = progress.send(InstallProgress::Extracting).await;
        let target = self.version_dir(&version);
        let worker_cancel = cancel.clone();
        // Off the async runtime. Removing a directory tree and unpacking an
        // archive of up to 256 MiB are seconds of synchronous filesystem work,
        // and doing them on a Tokio worker parks that worker: the lobby's
        // frames queue up behind an install the user is watching a progress
        // bar for.
        tokio::task::spawn_blocking(move || {
            // A rerun after an interrupted install would otherwise refuse
            // forever.
            if worker_cancel.is_cancelled() {
                return Err("installation cancelled".into());
            }
            if target.exists() {
                if finish_install(&target).is_ok() {
                    return Ok(());
                }
                std::fs::remove_dir_all(&target).map_err(|error| {
                    format!("could not remove an incomplete installation: {error}")
                })?;
            }
            vault_install::install_flat_archive_cancellable(
                &archive,
                &target,
                "the Galactic War archive",
                finish_install,
                &worker_cancel,
            )
        })
        .await
        .map_err(|error| format!("the Galactic War install task failed: {error}"))??;

        self.write_manifest(&version)?;
        self.remove_other_versions(&version);
        Ok(version)
    }

    /// Poll the tracked client until it exits, then wake [`Self::wait_for_exit`].
    ///
    /// Polling rather than `Child::wait()`, which needs `&mut Child` while the
    /// handle has to stay in the shared slot for [`Self::is_running`] to see
    /// it. Same approach as [`super::game::GameProcess`], for the same reason.
    fn watch_for_exit(&self) {
        let child = self.child.clone();
        let exited = self.exited.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let finished = {
                    let mut guard = child.lock().unwrap();
                    match guard.as_mut() {
                        None => true,
                        Some(process) => match process.try_wait() {
                            Ok(Some(_status)) => {
                                *guard = None;
                                true
                            }
                            Ok(None) => false,
                            // An unusable handle: treat it as gone rather than
                            // spinning on it forever.
                            Err(_) => {
                                *guard = None;
                                true
                            }
                        },
                    }
                };
                if finished {
                    exited.notify_waiters();
                    return;
                }
            }
        });
    }
}

/// Make a staged install usable, and refuse it if it is not.
///
/// Two jobs, both mandatory:
///
/// * The Godot binary finds its content pack by its own base name, so an
///   archive missing either file produces a client that starts into nothing.
///   Checking here means the staged directory is discarded rather than
///   renamed into place.
/// * The published archives carry no Unix permission bits at all (their
///   external attributes are plain MS-DOS flags), and the extractor does not
///   set any. Without this the Linux build is delivered non-executable and
///   simply refuses to start.
fn finish_install(staged: &Path) -> Result<(), String> {
    let executable = staged.join(EXECUTABLE_NAME);
    if !executable.is_file() {
        return Err(format!(
            "the Galactic War archive does not contain {EXECUTABLE_NAME}"
        ));
    }
    if !staged.join(CONTENT_PACK_NAME).is_file() {
        return Err(format!(
            "the Galactic War archive does not contain {CONTENT_PACK_NAME}"
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).map_err(
            |error| format!("could not make the Galactic War client executable: {error}"),
        )?;
    }

    Ok(())
}

/// Byte counts cross to the frontend as `u32` (specta rejects 64-bit
/// integers). Saturating is right here: a count beyond four gigabytes only
/// ever means a progress bar reads slightly wrong, and the size bound above is
/// the thing actually protecting memory.
fn clamp_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[async_trait]
impl GalacticWarPort for GalacticWarGateway {
    async fn statistics(&self) -> Result<GalacticWarStatistics, String> {
        let body = self
            .get_text(&self.api_url("statistics"), "statistics")
            .await?;
        parse_statistics(&body)
    }

    async fn versions(&self) -> Result<ClientVersions, String> {
        let body = self
            .get_text(&self.api_url(&version_path()), "version service")
            .await?;
        parse_client_versions(&body)
    }

    fn installed_version(&self) -> Option<String> {
        self.installed_executable().map(|(version, _)| version)
    }

    async fn install(&self, version: String) -> mpsc::Receiver<InstallProgress> {
        self.install_cancellable(version, tokio_util::sync::CancellationToken::new())
            .await
    }

    async fn install_cancellable(
        &self,
        version: String,
        cancel: tokio_util::sync::CancellationToken,
    ) -> mpsc::Receiver<InstallProgress> {
        let (tx, rx) = mpsc::channel(32);
        // The run happens in a task so the receiver reaches the caller first.
        // Doing the work before returning `rx` deadlocks the moment the
        // channel fills: nothing is draining it yet, so the send that fills
        // the last slot waits for a consumer that cannot exist until this
        // function returns. Same shape as `client_update::download`.
        let worker = Self {
            config: self.config.clone(),
            http: self.http.clone(),
            child: self.child.clone(),
            exited: self.exited.clone(),
        };
        tokio::spawn(async move {
            let outcome = worker.install_version(version, tx.clone(), cancel).await;
            let _ = tx.send(InstallProgress::Finished(outcome)).await;
        });
        rx
    }

    async fn launch(&self) -> Result<(), String> {
        if self.is_running() {
            return Err("Galactic War is already running".into());
        }
        let Some((_version, executable)) = self.installed_executable() else {
            return Err("Galactic War is not installed".into());
        };

        let mut command = Command::new(&executable);
        // Godot resolves its content pack relative to the executable, so the
        // working directory is the install, not wherever this client started.
        if let Some(directory) = executable.parent() {
            command.current_dir(directory);
        }
        // No arguments and no credentials: Galactic War logs in by itself.
        let child = command
            .spawn()
            .map_err(|error| format!("could not start Galactic War: {error}"))?;

        *self.child.lock().unwrap() = Some(child);
        self.watch_for_exit();
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.child.lock().unwrap().is_some()
    }

    async fn wait_for_exit(&self) {
        self.exited.notified().await
    }
}

/// Reports that Galactic War is unavailable, without asking anyone.
///
/// Used offline and in tests. The statistics call fails rather than returning
/// an empty document: a panel of zeroes claims the season is empty, which is a
/// statement about the world, while a failure says only that we do not know.
#[derive(Debug, Clone, Default)]
pub struct FakeGalacticWar;

#[async_trait]
impl GalacticWarPort for FakeGalacticWar {
    async fn statistics(&self) -> Result<GalacticWarStatistics, String> {
        Err("this build does not reach the Galactic War gateway".into())
    }

    async fn versions(&self) -> Result<ClientVersions, String> {
        Err("this build does not reach the Galactic War gateway".into())
    }

    fn installed_version(&self) -> Option<String> {
        None
    }

    async fn install(&self, _version: String) -> mpsc::Receiver<InstallProgress> {
        let (tx, rx) = mpsc::channel(1);
        let _ = tx
            .send(InstallProgress::Finished(Err(
                "this build does not install Galactic War".into(),
            )))
            .await;
        rx
    }

    async fn launch(&self) -> Result<(), String> {
        Err("this build does not launch Galactic War".into())
    }

    fn is_running(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gateway(download_base: &str) -> GalacticWarGateway {
        GalacticWarGateway::new(GalacticWarConfig {
            api_base: "https://gw.example".into(),
            download_base: download_base.into(),
            install_dir: std::env::temp_dir().join(format!("faf-gw-{}", rand::random::<u64>())),
        })
    }

    #[test]
    fn api_urls_survive_a_trailing_slash_on_the_base() {
        let gateway = GalacticWarGateway::new(GalacticWarConfig {
            api_base: "https://gw.example/".into(),
            download_base: "https://downloads.faforever.com".into(),
            install_dir: PathBuf::from("."),
        });
        assert_eq!(
            gateway.api_url("statistics"),
            "https://gw.example/statistics"
        );
        assert_eq!(
            gateway.api_url(&version_path()),
            "https://gw.example/client/faf-gw-client/version"
        );
    }

    #[test]
    fn only_downloads_under_the_configured_root_are_trusted() {
        let gateway = gateway("https://downloads.faforever.com");
        assert!(gateway.is_trusted("https://downloads.faforever.com/faf-gw-client/v1/a.zip"));

        for hostile in [
            // Plain HTTP against an HTTPS root.
            "http://downloads.faforever.com/faf-gw-client/v1/a.zip",
            "https://evil.invalid/faf-gw-client/v1/a.zip",
            // A prefix match that leaves the host.
            "https://downloads.faforever.com.evil.invalid/a.zip",
            "https://downloads.faforever.com/faf-gw-client/../../etc/passwd",
        ] {
            assert!(!gateway.is_trusted(hostile), "{hostile} must be refused");
        }
    }

    #[test]
    fn a_deliberate_local_http_root_still_works() {
        // An explicit HTTP base is a test setup, not an accident: the scheme
        // check follows the configuration rather than overriding it.
        let gateway = gateway("http://localhost:8080");
        assert!(gateway.is_trusted("http://localhost:8080/faf-gw-client/v1/a.zip"));
        assert!(!gateway.is_trusted("http://elsewhere.invalid/a.zip"));
    }

    #[test]
    fn an_incomplete_archive_never_becomes_an_installation() {
        let staged = std::env::temp_dir().join(format!("faf-gw-staged-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&staged).unwrap();

        // Neither file.
        assert!(finish_install(&staged).is_err());
        // The binary without its content pack: Godot would start into nothing.
        std::fs::write(staged.join(EXECUTABLE_NAME), b"binary").unwrap();
        assert!(finish_install(&staged).is_err());
        // Both.
        std::fs::write(staged.join(CONTENT_PACK_NAME), b"content").unwrap();
        assert!(finish_install(&staged).is_ok());

        std::fs::remove_dir_all(staged).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_linux_install_is_made_executable() {
        use std::os::unix::fs::PermissionsExt as _;
        let staged = std::env::temp_dir().join(format!("faf-gw-mode-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&staged).unwrap();
        std::fs::write(staged.join(EXECUTABLE_NAME), b"binary").unwrap();
        std::fs::write(staged.join(CONTENT_PACK_NAME), b"content").unwrap();
        // What the published archive actually delivers: no executable bit,
        // because it carries no Unix permissions at all.
        std::fs::set_permissions(
            staged.join(EXECUTABLE_NAME),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();

        finish_install(&staged).unwrap();

        let mode = std::fs::metadata(staged.join(EXECUTABLE_NAME))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o111, 0o111, "the client must be executable");
        std::fs::remove_dir_all(staged).unwrap();
    }

    #[test]
    fn a_manifest_pointing_at_a_deleted_install_reads_as_not_installed() {
        let gateway = gateway("https://downloads.faforever.com");
        std::fs::create_dir_all(&gateway.config.install_dir).unwrap();
        gateway.write_manifest("v2026.04.04.1").unwrap();

        // The manifest is there but the files are not.
        assert_eq!(gateway.installed_version(), None);

        let directory = gateway.version_dir("v2026.04.04.1");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(EXECUTABLE_NAME), b"binary").unwrap();
        assert_eq!(
            gateway.installed_version().as_deref(),
            Some("v2026.04.04.1")
        );

        std::fs::remove_dir_all(&gateway.config.install_dir).unwrap();
    }

    #[test]
    fn a_manifest_naming_an_unusable_version_is_ignored() {
        let gateway = gateway("https://downloads.faforever.com");
        std::fs::create_dir_all(&gateway.config.install_dir).unwrap();
        std::fs::write(
            gateway.manifest_path(),
            br#"{"version":"../../somewhere-else"}"#,
        )
        .unwrap();

        assert_eq!(gateway.installed_version(), None);

        std::fs::remove_dir_all(&gateway.config.install_dir).unwrap();
    }

    #[tokio::test]
    async fn an_install_reports_through_the_channel_instead_of_blocking_on_it() {
        let gateway = gateway("https://downloads.faforever.com");

        // The regression this pins: the run used to happen *before* the
        // receiver was handed back, so the first full channel buffer
        // deadlocked against a consumer that could not exist yet. The download
        // stalled at zero and nothing ever settled it.
        let mut progress = gateway.install("../../evil".into()).await;

        let step = progress.recv().await;
        assert!(
            matches!(step, Some(InstallProgress::Finished(Err(_)))),
            "an install must always settle, got {step:?}"
        );
    }

    #[test]
    fn installing_clears_the_version_it_replaces() {
        let gateway = gateway("https://downloads.faforever.com");
        let old = gateway.version_dir("v1");
        std::fs::create_dir_all(&old).unwrap();
        let new = gateway.version_dir("v2");
        std::fs::create_dir_all(&new).unwrap();

        gateway.remove_other_versions("v2");

        assert!(!old.exists());
        assert!(new.exists());
        std::fs::remove_dir_all(&gateway.config.install_dir).unwrap();
    }
}
