//! Real Neroxis map generator: downloads the JAR from GitHub and runs it.
//!
//! FAF matchmaker pools contain *generated* maps. The server does not ship
//! them; it names them (`neroxis_map_generator_<version>_<seed>`) and every
//! client reproduces the identical terrain locally by running the matching
//! generator release. Without this, matching into a ladder game on a generated
//! map leaves you unable to start.
//!
//! ## What runs where
//! * Name grammar, version policy and the command line live in
//!   [`faf_domain::protocol::map_generator`]: pure and exhaustively tested.
//! * This module does the IO: resolve a release from the GitHub API, download
//!   the JAR, spawn `java -jar`, and scrape the generated map names off stdout.
//!
//! ## Two flavours, from the two reference clients
//! * `generate_named` reproduces a specific map. The version comes from the
//!   name, so an old lobby works even if this client has never seen that
//!   release. This is the whole of the Python client's `mapGenerator/`.
//! * `generate` builds a fresh map from options using the newest supported
//!   release: the Java client's `GenerateMapController` flow.
//!
//! ## Why stdout scraping
//! With `--num-to-generate` the client cannot predict the seeds, so it cannot
//! predict the folder names. Both reference clients read the names back out of
//! the generator's own output, and so does this.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use faf_domain::protocol::map_generator::{self, GeneratorVersion, VersionPolicy};
use faf_domain::state::{GeneratorOptionQuery, GeneratorOptions, GeneratorPreset, GeneratorStatus};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

use crate::infra::vault_install::MAX_DOWNLOAD_BYTES;
use crate::ports::{GeneratorUpdate, MapGeneratorPort};

/// How long an option query (`--styles` etc.) may take. Much shorter than a
/// generation run: it only prints a list. The Java client uses six seconds.
///
/// Generous against that, because the *first* query on a machine runs against
/// a 24 MB JAR that was downloaded seconds ago: on a spinning disk, with a
/// virus scanner reading every entry, the JVM start alone can outlast a tight
/// limit, and the cost is paid six times over before the dialog is usable.
const OPTION_QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a fetched release list stays fresh.
///
/// GitHub allows sixty unauthenticated requests an hour per address, and the
/// generate dialog spends four of them every time it opens or changes release:
/// two to list the versions, two to resolve the newest. A few minutes of
/// clicking through releases exhausts the budget, and then the version picker
/// has nothing to offer and no way to say why. The list barely changes in a
/// month, so ten minutes of reuse costs nothing.
const RELEASE_LIST_TTL: Duration = Duration::from_secs(600);

/// How long `--parse` may take. It resolves options and prints JSON without
/// generating anything, so this is a JVM startup and little else.
const PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(20);

/// Cap on the generator log, checked before each run.
///
/// The generator is chatty and a user may generate hundreds of maps, so an
/// unbounded log would quietly grow without limit. Rotating a single file at a
/// megabyte keeps the last session or two, which is what a bug report needs.
const MAX_LOG_BYTES: u64 = 1024 * 1024;

/// Releases per GitHub page. The default of 30 covers barely a fifth of the
/// generator's ~130 releases, which is why the Python client asks for 100 and
/// follows the `Link` header; without that the version picker silently stops
/// somewhere in the 1.8 range.
const RELEASES_PER_PAGE: u32 = 100;

/// What the command builder picks from when the user selected several styles,
/// symmetries or a density range but pinned no numeric seed.
///
/// The current time in milliseconds, which is what the builder used to read
/// for itself. It is read here instead because the domain crate is kept free
/// of clocks, so the same options always build the same command line there.
fn fallback_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// How many pages to walk before giving up. Bounded so a malformed `Link`
/// header cannot turn version resolution into an infinite request loop.
const MAX_RELEASE_PAGES: u32 = 10;

/// A "stop the current run" signal shared between the command handler and the
/// task driving the JVM.
///
/// A flag plus a wake-up rather than a `watch` channel, because reading a
/// `watch` hands back a guard, and a guard alive across an `await` would make
/// the whole run future non-`Send` and so unspawnable.
#[derive(Debug, Default)]
struct CancelSignal {
    raised: std::sync::atomic::AtomicBool,
    changed: tokio::sync::Notify,
}

impl CancelSignal {
    fn raise(&self) {
        self.raised.store(true, std::sync::atomic::Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    #[cfg(test)]
    fn clear(&self) {
        self.raised
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    fn is_raised(&self) -> bool {
        self.raised.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Resolve once the signal is raised, now or later.
    ///
    /// The listener is registered *before* the flag is read, so a raise landing
    /// between the two is still observed: the other order would drop the
    /// notification and leave the run going.
    async fn raised(&self) {
        loop {
            let listener = self.changed.notified();
            if self.is_raised() {
                return;
            }
            listener.await;
        }
    }
}

/// What ended a generation run.
enum RunOutcome {
    Generated(Vec<String>),
    /// The user asked to stop. Not an error, and reported as its own status.
    Cancelled,
    Failed(String),
}

struct AbortTaskOnDrop<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for AbortTaskOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn next_output_line<R: tokio::io::AsyncRead + Unpin>(
    lines: &mut tokio::io::Split<BufReader<R>>,
) -> std::io::Result<Option<String>> {
    Ok(lines.next_segment().await?.map(|bytes| {
        String::from_utf8_lossy(&bytes)
            .trim_end_matches('\r')
            .to_string()
    }))
}

async fn cancelled(signal: Option<&CancelSignal>) {
    match signal {
        Some(signal) => signal.raised().await,
        None => std::future::pending().await,
    }
}

#[derive(Debug, Clone)]
pub struct MapGeneratorConfig {
    /// GitHub releases API for the generator repository.
    pub releases_url: String,
    /// `{version}`-templated download URL for the release asset.
    pub download_url_format: String,
    /// `java` executable. Shared convention with the ICE adapter's
    /// `FAF_JAVA_PATH`, since both need a JVM. `None` follows Settings.
    pub java_path: Option<String>,
    /// Where downloaded generator JARs are cached. Also holds the generator
    /// log, the option cache and the preview drop folder. `None` follows
    /// Settings.
    pub generator_dir: Option<PathBuf>,
    /// Where generated maps are written: FA's user maps folder. `None`
    /// follows Settings.
    pub maps_dir: Option<PathBuf>,
    /// Which generator major versions this client will drive. Configurable for
    /// the same reason the Java client reads it from `application.yml`: FAF can
    /// move the window without a client release.
    pub version_policy: VersionPolicy,
}

impl MapGeneratorConfig {
    pub fn faf() -> Self {
        Self {
            // Both name a server whose bytes are handed to a JVM.
            releases_url: crate::infra::dev_env_or(
                "FAF_MAP_GENERATOR_RELEASES_URL",
                "https://api.github.com/repos/FAForever/Neroxis-Map-Generator/releases",
            ),
            download_url_format: crate::infra::dev_env_or(
                "FAF_MAP_GENERATOR_DOWNLOAD_URL",
                "https://github.com/FAForever/Neroxis-Map-Generator/releases/download/{version}/NeroxisGen_{version}.jar",
            ),
            // Not resolved here. The generator is built while the client
            // starts, before Settings has been read, so a folder taken now is
            // the default one: a player with their vault somewhere else had
            // every map generated into a folder the game never mounts, and the
            // "is it there?" check read the same wrong folder, so the map was
            // never found and never made again. Resolved at every use, all
            // three follow Settings the way the rest of the client does.
            java_path: None,
            generator_dir: None,
            maps_dir: None,
            version_policy: version_policy_from_env(),
        }
    }

    pub(crate) fn java_path(&self) -> String {
        self.java_path
            .clone()
            .unwrap_or_else(super::java_runtime::preferred_java_path)
    }

    pub(crate) fn generator_dir(&self) -> PathBuf {
        self.generator_dir.clone().unwrap_or_else(generator_dir)
    }

    pub(crate) fn maps_dir(&self) -> PathBuf {
        self.maps_dir.clone().unwrap_or_else(user_maps_dir)
    }
}

/// The supported generator-version window, overridable per deployment.
///
/// A malformed override falls back to the default rather than failing to
/// start: a typo in an env var should not make map generation impossible.
fn version_policy_from_env() -> VersionPolicy {
    let default = VersionPolicy::default();
    let read = |key: &str, fallback: u32| {
        std::env::var(key)
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(fallback)
    };
    let min_major = read("FAF_MAP_GENERATOR_MIN_MAJOR", default.min_major);
    let max_major = read("FAF_MAP_GENERATOR_MAX_MAJOR", default.max_major);
    // An inverted window would reject every version; treat it as unset.
    if min_major > max_major {
        tracing::warn!(
            min_major,
            max_major,
            "ignoring inverted map-generator version window"
        );
        return default;
    }
    VersionPolicy {
        min_major,
        max_major,
    }
}

/// Cache directory for generator JARs. Kept out of the maps folder so a
/// "delete generated maps" sweep never removes the generators themselves.
pub(crate) fn generator_dir() -> PathBuf {
    if let Some(dir) = crate::infra::paths::map_generator_dir() {
        return dir;
    }
    if let Ok(dir) = std::env::var("FAF_MAP_GENERATOR_DIR") {
        if !dir.is_empty() {
            return PathBuf::from(dir);
        }
    }
    crate::infra::project_dirs()
        .map(|dirs| dirs.data_dir().join("map_generator"))
        .unwrap_or_else(|| PathBuf::from("map_generator"))
}

/// FA's user maps folder: where the generator writes and where FA looks.
///
/// Deliberately the *same* resolver the maps port scans with, honouring the
/// same `FAF_MAPS_DIR` override. A second implementation here would let a
/// custom install point the generator at a folder the installed-map list never
/// reads, so a generated map would silently never appear.
fn user_maps_dir() -> PathBuf {
    crate::infra::maps::maps_dir()
}

/// The release list as it is held between fetches: when it arrived, and the
/// tags themselves.
type RememberedReleases = Arc<tokio::sync::Mutex<Option<(std::time::Instant, Vec<String>)>>>;

pub struct NeroxisMapGenerator {
    config: MapGeneratorConfig,
    http: reqwest::Client,
    /// Raised by [`MapGeneratorPort::cancel`]. Shared with the task running the
    /// JVM, which is why it is behind an `Arc`: the run happens on a spawned
    /// task that outlives the call which started it.
    cancel: Arc<CancelSignal>,
    active_runs: Arc<std::sync::Mutex<Vec<std::sync::Weak<CancelSignal>>>>,
    generation: Arc<tokio::sync::Mutex<()>>,
    /// Serialises JAR installs.
    ///
    /// Every command is dispatched on its own task, so two of them can want
    /// the same release at the same moment: the dialog loading its option
    /// lists while the preview button starts a preflight is the everyday case,
    /// and selecting a release the client has never run makes it certain.
    /// Both downloaded to the same temporary file and both then renamed it, so
    /// the loser reported "could not install the generator: the system cannot
    /// find the file specified", having also fetched the same twenty-four
    /// megabytes twice.
    installing: Arc<tokio::sync::Mutex<()>>,
    /// The last release list and when it arrived. Guarded rather than atomic
    /// because the lock is held across the fetch itself: two commands asking
    /// at once should cost one round trip, not two.
    releases: RememberedReleases,
    /// Whether a run may keep the JVM's console window. Off unless the user
    /// asked for it in Settings; shared with the run task, which is why it is
    /// behind an `Arc` like the cancel signal.
    show_window: Arc<AtomicBool>,
}

impl NeroxisMapGenerator {
    pub fn new(config: MapGeneratorConfig) -> Self {
        Self {
            config,
            // Carries the User-Agent GitHub requires, and refuses a redirect
            // that leaves HTTPS: these bytes are handed to a JVM, and the
            // decision not to sign them rests on the connection staying HTTPS.
            http: super::http::https_only_download_client(),
            cancel: Arc::new(CancelSignal::default()),
            active_runs: Arc::new(std::sync::Mutex::new(Vec::new())),
            generation: Arc::new(tokio::sync::Mutex::new(())),
            installing: Arc::new(tokio::sync::Mutex::new(())),
            releases: Arc::new(tokio::sync::Mutex::new(None)),
            show_window: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn faf() -> Self {
        Self::new(MapGeneratorConfig::faf())
    }

    /// Whether a cancellation is already pending.
    ///
    /// Deliberately not `async`: reading a `watch` yields a guard, and a guard
    /// living across an `await` would make the whole run future non-`Send`.
    /// Keeping the read inside a synchronous call confines it.
    fn is_cancelled(&self) -> bool {
        self.cancel.is_raised()
    }

    /// A handle onto the same run, for the task that actually drives the JVM.
    fn for_run(&self) -> Self {
        Self {
            config: self.config.clone(),
            http: self.http.clone(),
            cancel: Arc::clone(&self.cancel),
            active_runs: Arc::clone(&self.active_runs),
            generation: Arc::clone(&self.generation),
            installing: Arc::clone(&self.installing),
            releases: Arc::clone(&self.releases),
            show_window: Arc::clone(&self.show_window),
        }
    }

    fn jar_path(&self, version: GeneratorVersion) -> PathBuf {
        self.config.generator_dir().join(version.cached_jar_name())
    }

    /// Where the generator's own output is kept.
    ///
    /// Both reference clients keep one (Python's `map_generator.log`, Java's
    /// `faf-map-generator` logger) and for the same reason: when a run fails,
    /// the generator's explanation is on stdout, and a single scraped error
    /// line is rarely enough to say why.
    fn log_path(&self) -> PathBuf {
        self.config.generator_dir().join("map_generator.log")
    }

    /// Append a line to the generator log, rotating once it grows too large.
    ///
    /// Best-effort throughout: failing to write a diagnostic must never fail
    /// the generation it is diagnosing.
    async fn log_line(&self, line: &str) {
        use tokio::io::AsyncWriteExt as _;
        let path = self.log_path();
        if tokio::fs::metadata(&path)
            .await
            .is_ok_and(|meta| meta.len() > MAX_LOG_BYTES)
        {
            let _ = tokio::fs::rename(&path, path.with_extension("log.1")).await;
        }
        let _ = tokio::fs::create_dir_all(self.config.generator_dir()).await;
        if let Ok(mut file) = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
        {
            let _ = file.write_all(line.as_bytes()).await;
            let _ = file.write_all(b"\n").await;
        }
    }

    /// Download the generator JAR if it isn't cached, reporting progress.
    ///
    /// Writes to a temporary file and renames on completion, so an interrupted
    /// download can never leave a truncated JAR that later "exists" and fails
    /// to run.
    async fn ensure_jar(
        &self,
        version: GeneratorVersion,
        progress: &mpsc::Sender<GeneratorUpdate>,
    ) -> Result<PathBuf, String> {
        self.ensure_jar_inner(version, progress, None).await
    }

    async fn ensure_jar_inner(
        &self,
        version: GeneratorVersion,
        progress: &mpsc::Sender<GeneratorUpdate>,
        cancel: Option<&CancelSignal>,
    ) -> Result<PathBuf, String> {
        let target = self.jar_path(version);
        if target.is_file() {
            return Ok(target);
        }
        let _installing = tokio::select! {
            biased;
            () = cancelled(cancel) => return Err("map generation cancelled".into()),
            lease = self.installing.lock() => lease,
        };
        // Whoever held the lock may have been fetching exactly this release.
        if target.is_file() {
            return Ok(target);
        }

        let url = self
            .config
            .download_url_format
            .replace("{version}", &version.to_string());
        // `try_send`, not `send`: a progress frame must never be able to block
        // the download it is reporting on. With an awaited send, a caller that
        // holds a receiver without draining it (an option query, a preflight,
        // a `--help`) wedges the whole transfer once the channel fills, a few
        // kilobytes in, with no error and no timeout. Dropping a frame when
        // the consumer is behind costs nothing: the next one carries the same
        // running total.
        let _ = progress.try_send(GeneratorUpdate::Status(GeneratorStatus::Downloading {
            version: version.to_string(),
            downloaded_bytes: 0,
            total_bytes: None,
        }));

        let response = tokio::select! {
            biased;
            () = cancelled(cancel) => return Err("map generation cancelled".into()),
            response = self.http.get(&url).send() => response,
        }
        .map_err(|e| {
            format!(
                "could not reach the map generator download: {}",
                crate::infra::http::describe_transport_error(&e)
            )
        })?;
        if !response.status().is_success() {
            return Err(format!(
                "map generator {version} download returned {}",
                response.status()
            ));
        }
        if response
            .content_length()
            .is_some_and(|size| size > MAX_DOWNLOAD_BYTES)
        {
            return Err("map generator download is larger than the allowed size".into());
        }
        // Clamped to `u32` for the IPC boundary; see `GeneratorStatus::Downloading`.
        let total_bytes = response
            .content_length()
            .map(|n| u32::try_from(n).unwrap_or(u32::MAX));

        tokio::fs::create_dir_all(self.config.generator_dir())
            .await
            .map_err(|e| format!("could not create the generator directory: {e}"))?;
        let temp = target.with_extension("partial");
        if tokio::fs::try_exists(&temp).await.unwrap_or(false) {
            tokio::fs::remove_file(&temp)
                .await
                .map_err(|e| format!("could not clear the partial generator: {e}"))?;
        }
        let mut file = tokio::fs::File::create(&temp)
            .await
            .map_err(|e| format!("could not write the generator: {e}"))?;

        let mut downloaded = 0u64;
        let mut stream = response.bytes_stream();
        use futures_util::StreamExt as _;
        use tokio::io::AsyncWriteExt as _;
        let write = async {
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| {
                    format!(
                        "map generator download failed: {}",
                        crate::infra::http::describe_transport_error(&e)
                    )
                })?;
                downloaded = downloaded
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| "map generator download is too large".to_string())?;
                if downloaded > MAX_DOWNLOAD_BYTES {
                    return Err("map generator download is larger than the allowed size".into());
                }
                file.write_all(&chunk)
                    .await
                    .map_err(|e| format!("could not write the generator: {e}"))?;
                let _ = progress.try_send(GeneratorUpdate::Status(GeneratorStatus::Downloading {
                    version: version.to_string(),
                    downloaded_bytes: u32::try_from(downloaded).unwrap_or(u32::MAX),
                    total_bytes,
                }));
            }
            file.flush()
                .await
                .map_err(|e| format!("could not finish writing the generator: {e}"))
        };
        let write_result: Result<(), String> = tokio::select! {
            biased;
            () = cancelled(cancel) => Err("map generation cancelled".into()),
            result = write => result,
        };
        drop(file);
        if let Err(error) = write_result.and_then(|()| {
            if cancel.is_some_and(|signal| signal.is_raised()) {
                Err("map generation cancelled".into())
            } else {
                Ok(())
            }
        }) {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(error);
        }

        if let Err(error) = tokio::fs::rename(&temp, &target).await {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(format!("could not install the generator: {error}"));
        }
        Ok(target)
    }

    /// Give up on a run that has exceeded its deadline, and return the error.
    ///
    /// Kills *and reaps*: `start_kill` only signals, so without the follow-up
    /// wait the JVM would linger as a zombie for the rest of the session: and
    /// a user retrying after a timeout would accumulate one per attempt.
    async fn abandon(
        &self,
        child: &mut tokio::process::Child,
        untimed: bool,
        limit_seconds: u64,
    ) -> String {
        debug_assert!(!untimed, "an untimed run should never reach the deadline");
        let _ = child.start_kill();
        // Bounded: if the JVM ignores the signal, don't trade one hang for
        // another. The OS cleans up on client exit either way.
        let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
        format!("map generation timed out after {limit_seconds}s")
    }

    /// Run the generator and collect the map names it reports.
    async fn run_generator(
        &self,
        version: GeneratorVersion,
        jar: &Path,
        args: Vec<String>,
        progress: &mpsc::Sender<GeneratorUpdate>,
    ) -> RunOutcome {
        if let Err(e) = tokio::fs::create_dir_all(self.config.maps_dir()).await {
            return RunOutcome::Failed(format!("could not create the maps directory: {e}"));
        }
        // The generator writes previews into this folder if it was asked to;
        // creating it here keeps the earlier refusal paths free of side effects.
        if args.iter().any(|arg| arg == "--preview-path") {
            let _ = tokio::fs::create_dir_all(self.preview_dir()).await;
        }

        // Opened before the first line is written, so the window has the whole
        // run in it. Bound to a name rather than dropped: it lives until this
        // function returns, on every path out of it, and closes then.
        let mut window = self
            .show_window
            .load(Ordering::Relaxed)
            .then(RunWindow::open)
            .flatten();

        let header = format!("--- run {version} {}", shell_quote_all(&args));
        if let Some(window) = window.as_mut() {
            window.write_line(&header).await;
        }
        self.log_line(&header).await;

        let mut command = Command::new(self.config.java_path());
        command
            .arg("-jar")
            .arg(jar)
            .args(&args)
            // The generator writes into its working directory.
            .current_dir(self.config.maps_dir())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // Always hidden: what the switch opens is the log window above, which
        // is the only one that can show anything.
        crate::infra::hide_console(&mut command);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                return RunOutcome::Failed(format!(
                "could not start the map generator ({}): {e}. Java is required to generate maps.",
                self.config.java_path()
            ))
            }
        };

        let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
            return RunOutcome::Failed("the map generator produced no output streams".into());
        };

        let mut names: Vec<String> = Vec::new();
        let mut lines = BufReader::new(stdout).split(b'\n');

        // Drain stderr concurrently. The generator prints usage help there on a
        // bad option combination, and a full pipe would deadlock the child.
        let mut stderr_task = AbortTaskOnDrop(tokio::spawn(async move {
            let mut collected = String::new();
            let mut lines = BufReader::new(stderr).split(b'\n');
            while let Ok(Some(line)) = next_output_line(&mut lines).await {
                if collected.len() < 8000 {
                    collected.push_str(&line);
                    collected.push('\n');
                }
            }
            collected
        }));

        // A `--visualize` run opens a viewer window and stays alive on purpose,
        // so it is exempt from the timeout: the Java client's `GenerateMapTask`
        // makes the same exception. Everything else gets three minutes per
        // map it was asked for: see `generation_timeout_seconds`.
        let untimed = map_generator::runs_without_timeout(&args);
        let limit_seconds = map_generator::generation_timeout_seconds(&args);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(limit_seconds);
        // `None` disables every deadline below without duplicating the loop.
        let remaining = || -> Option<Duration> {
            (!untimed).then(|| deadline.saturating_duration_since(tokio::time::Instant::now()))
        };

        loop {
            let wait_for = remaining();
            if wait_for.is_some_and(|d| d.is_zero()) {
                return RunOutcome::Failed(self.abandon(&mut child, untimed, limit_seconds).await);
            }
            let read_line = async {
                match wait_for {
                    Some(limit) => tokio::time::timeout(limit, next_output_line(&mut lines)).await,
                    None => Ok(next_output_line(&mut lines).await),
                }
            };
            // A generation run is the one long operation in this client that a
            // user might reasonably change their mind about, so stopping it has
            // to be possible mid-flight rather than only between stages.
            let next = tokio::select! {
                _ = self.cancel.raised() => {
                    let _ = child.start_kill();
                    let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
                    self.log_line("--- cancelled by the user").await;
                    return RunOutcome::Cancelled;
                }
                result = read_line => result,
            };
            match next {
                Err(_) => {
                    return RunOutcome::Failed(
                        self.abandon(&mut child, untimed, limit_seconds).await,
                    );
                }
                Ok(Ok(Some(line))) => {
                    if let Some(window) = window.as_mut() {
                        window.write_line(&line).await;
                    }
                    self.log_line(&line).await;
                    for name in map_generator::scrape_map_names(&line) {
                        if !names.contains(&name) {
                            names.push(name);
                        }
                    }
                    // The generator's own output is the only progress signal.
                    let _ = progress
                        .send(GeneratorUpdate::Status(GeneratorStatus::Generating {
                            version: version.to_string(),
                            detail: line.chars().take(90).collect(),
                        }))
                        .await;
                }
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    return RunOutcome::Failed(format!("could not read generator output: {e}"))
                }
            }
        }

        // Stdout closing is not the same as exiting: a generator that closes
        // its pipes but never terminates would hang here forever without the
        // same deadline the read loop uses.
        let waited = tokio::select! {
            () = self.cancel.raised() => {
                let _ = child.start_kill();
                let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
                return RunOutcome::Cancelled;
            }
            status = async {
                match remaining() {
                    Some(limit) => tokio::time::timeout(limit, child.wait()).await.ok(),
                    None => Some(child.wait().await),
                }
            } => status,
        };
        let status = match waited {
            Some(Ok(status)) => status,
            Some(Err(error)) => {
                return RunOutcome::Failed(format!("map generator did not exit cleanly: {error}"))
            }
            None => {
                return RunOutcome::Failed(self.abandon(&mut child, untimed, limit_seconds).await)
            }
        };
        let errors = tokio::select! {
            () = self.cancel.raised() => return RunOutcome::Cancelled,
            errors = &mut stderr_task.0 => errors.unwrap_or_default(),
        };
        for line in errors.lines() {
            self.log_line(line).await;
        }

        if !status.success() {
            // The generator's first stderr line is its actual complaint
            // ("Spawn Count `5` not a multiple of Num Teams `2`"); everything
            // after it is the usage dump, which would bury it.
            let detail = errors.lines().next().unwrap_or("no detail").to_string();
            return RunOutcome::Failed(format!("the map generator failed: {detail}"));
        }
        if names.is_empty() {
            return RunOutcome::Failed(
                "the map generator produced no map: the option combination may be invalid".into(),
            );
        }

        // Trust the folder, not the log: a name in the output that didn't
        // result in a directory means the run half-failed.
        for name in &names {
            if !self.config.maps_dir().join(name).is_dir() {
                return RunOutcome::Failed(format!(
                    "the generator reported {name} but wrote no folder"
                ));
            }
        }
        RunOutcome::Generated(names)
    }

    /// Run the JAR for a short, non-generating query and return its stdout.
    ///
    /// Shared by `--parse`, `--help` and the option lists: all three start a
    /// JVM, print something and exit, and all three need the same treatment of
    /// a non-zero exit (the message is on stderr, and it is the useful part).
    async fn run_query(
        &self,
        jar: &Path,
        args: &[&str],
        limit: Duration,
    ) -> Result<String, String> {
        let mut command = Command::new(self.config.java_path());
        command
            .arg("-jar")
            .arg(jar)
            .args(args)
            // None of these queries writes anything, but a release that
            // does not know the flag it was given answers by generating a
            // map into its working directory. Beside the JAR that is at
            // least findable; in the client's own working directory it is
            // litter nobody would connect to the map generator.
            .current_dir(self.config.generator_dir())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // Never windowed: these run while the dialog is being filled in, and
        // are not the run anybody asked to watch.
        crate::infra::hide_console(&mut command);
        let output = tokio::time::timeout(limit, command.output())
            .await
            .map_err(|_| "the map generator did not answer in time".to_string())?
            .map_err(|e| {
                format!(
                "could not start the map generator ({}): {e}. Java is required to generate maps.",
                self.config.java_path()
            )
            })?;

        if output.status.success() {
            return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        // A JVM older than the release needs is reported as a linkage error
        // several lines in, under a generic "a JNI error has occurred" banner
        // that says nothing about the cause. Naming it is the difference
        // between a fixable report and a mystery.
        if stderr.contains("UnsupportedClassVersionError") {
            return Err(format!(
                "the Java runtime at {} is too old for this map generator release: install a newer Java, or point FAF_JAVA_PATH at one",
                self.config.java_path()
            ));
        }
        // picocli colours its errors; the escape sequences are noise in a
        // message that is about to be shown in a web view.
        let detail = stderr
            .lines()
            .map(strip_ansi)
            .find(|line| !line.trim().is_empty())
            .unwrap_or_else(|| "the map generator rejected these options".into());
        Err(detail)
    }

    /// Every supported release, following GitHub's pagination.
    ///
    /// A single unpaginated request returns GitHub's default of 30, which for
    /// this repository stops around 1.8.4 and leaves a hundred releases
    /// invisible to the version picker. Paging stops at the first short page,
    /// so the common case is still one request.
    ///
    /// A failure *after* the first page is swallowed rather than propagated:
    /// having the newest hundred releases and losing the tail beats showing
    /// nothing because page three timed out.
    async fn fetch_releases(&self) -> Result<Vec<GitHubRelease>, String> {
        let mut remembered = self.releases.lock().await;
        if let Some((fetched_at, tags)) = remembered.as_ref() {
            if fetched_at.elapsed() < RELEASE_LIST_TTL {
                return Ok(releases_from_tags(tags));
            }
        }

        let mut all: Vec<GitHubRelease> = Vec::new();
        for page in 1..=MAX_RELEASE_PAGES {
            let separator = if self.config.releases_url.contains('?') {
                '&'
            } else {
                '?'
            };
            let url = format!(
                "{}{separator}per_page={RELEASES_PER_PAGE}&page={page}",
                self.config.releases_url
            );
            let result = async {
                let response = self
                    .http
                    .get(&url)
                    .header(reqwest::header::ACCEPT, "application/vnd.github.v3+json")
                    .send()
                    .await
                    .map_err(|e| {
                        format!(
                            "could not reach the map generator releases: {}",
                            crate::infra::http::describe_transport_error(&e)
                        )
                    })?;
                if !response.status().is_success() {
                    return Err(format!(
                        "map generator releases returned {}",
                        response.status()
                    ));
                }
                response
                    .json::<Vec<GitHubRelease>>()
                    .await
                    .map_err(|e| format!("could not read the map generator releases: {e}"))
            }
            .await;

            match result {
                Ok(page_items) => {
                    let short_page = (page_items.len() as u32) < RELEASES_PER_PAGE;
                    all.extend(page_items);
                    if short_page {
                        break;
                    }
                }
                // The first page failing leaves nothing to offer *now*, but
                // the list of releases barely moves, and a rate limit or a
                // dropped connection is no reason to empty the version picker.
                Err(error) if all.is_empty() => {
                    return match self.stored_release_tags().await {
                        Some(tags) => {
                            tracing::warn!(%error, "serving the stored map generator release list");
                            Ok(releases_from_tags(&tags))
                        }
                        None => Err(error),
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, page, "stopping map generator release paging early");
                    break;
                }
            }
        }

        if !all.is_empty() {
            let tags: Vec<String> = all.iter().map(|r| r.tag_name.clone()).collect();
            self.store_release_tags(&tags).await;
            *remembered = Some((std::time::Instant::now(), tags));
        }
        Ok(all)
    }

    fn releases_cache_path(&self) -> PathBuf {
        self.config.generator_dir().join("releases_cache.json")
    }

    /// The last release list that reached us, if one ever did.
    async fn stored_release_tags(&self) -> Option<Vec<String>> {
        let raw = tokio::fs::read_to_string(self.releases_cache_path())
            .await
            .ok()?;
        let tags: Vec<String> = serde_json::from_str(&raw).ok()?;
        (!tags.is_empty()).then_some(tags)
    }

    async fn store_release_tags(&self, tags: &[String]) {
        let Ok(serialized) = serde_json::to_string(tags) else {
            return;
        };
        let _ = tokio::fs::create_dir_all(self.config.generator_dir()).await;
        let _ = tokio::fs::write(self.releases_cache_path(), serialized).await;
    }

    /// The newest release whose major version this client supports.
    async fn resolve_latest(&self) -> Result<GeneratorVersion, String> {
        let releases = self.fetch_releases().await?;
        releases
            .iter()
            .filter_map(|release| GeneratorVersion::parse(release.tag_name.trim_start_matches('v')))
            .filter(|version| self.config.version_policy.allows_major(version.major))
            .max()
            .ok_or_else(|| {
                format!(
                    "no map generator release between major {} and {}: this client may be out of date",
                    self.config.version_policy.min_major, self.config.version_policy.max_major
                )
            })
    }

    async fn resolve_version(&self, explicit: Option<&str>) -> Result<GeneratorVersion, String> {
        if let Some(v_str) = explicit {
            let parsed = GeneratorVersion::parse(v_str.trim_start_matches('v'))
                .ok_or_else(|| format!("invalid generator version: {v_str}"))?;
            if !self.config.version_policy.allows_major(parsed.major) {
                return Err(format!(
                    "generator version {v_str} is outside the supported range"
                ));
            }
            return Ok(parsed);
        }
        self.resolve_latest().await
    }

    /// Every supported release, newest first.
    async fn available_versions(&self) -> Result<Vec<String>, String> {
        let releases = self.fetch_releases().await?;
        let mut versions: Vec<GeneratorVersion> = releases
            .into_iter()
            .filter_map(|r| GeneratorVersion::parse(r.tag_name.trim_start_matches('v')))
            .filter(|v| self.config.version_policy.allows_major(v.major))
            .collect();
        versions.sort();
        versions.dedup();
        versions.reverse();
        Ok(versions.into_iter().map(|v| v.to_string()).collect())
    }

    async fn read_map_preview(&self, map_name: &str) -> Option<String> {
        use base64::Engine as _;
        let normalized = map_name.to_lowercase();
        let folder = self.config.maps_dir().join(map_name);
        let folder_lower = self.config.maps_dir().join(&normalized);
        // The `--preview-path` drop folder is checked first: when the run used
        // it, the file is there under a known name and nothing below is needed.
        let previews = self.preview_dir();
        let candidates = [
            previews.join(format!("{normalized}.png")),
            previews.join(format!("{map_name}.png")),
            previews.join(format!("{normalized}_preview.png")),
            folder.join(format!("{map_name}_preview.png")),
            folder.join(format!("{map_name}.png")),
            folder.join(format!("{normalized}_preview.png")),
            folder.join(format!("{normalized}.png")),
            folder.join("preview.png"),
            folder_lower.join(format!("{normalized}_preview.png")),
            folder_lower.join(format!("{normalized}.png")),
            folder_lower.join(format!("{map_name}_preview.png")),
            folder_lower.join("preview.png"),
        ];
        for path in candidates {
            if let Ok(bytes) = tokio::fs::read(&path).await {
                if !bytes.is_empty() {
                    return Some(format!(
                        "data:image/png;base64,{}",
                        base64::engine::general_purpose::STANDARD.encode(&bytes)
                    ));
                }
            }
        }

        let target_folder = if folder.is_dir() {
            folder
        } else if folder_lower.is_dir() {
            folder_lower
        } else {
            return None;
        };

        if let Ok(mut entries) = tokio::fs::read_dir(&target_folder).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_lowercase();
                if name.ends_with(".png") || name.ends_with(".jpg") || name.ends_with(".jpeg") {
                    if let Ok(bytes) = tokio::fs::read(entry.path()).await {
                        if !bytes.is_empty() {
                            let mime = if name.ends_with(".png") {
                                "image/png"
                            } else {
                                "image/jpeg"
                            };
                            return Some(format!(
                                "data:{mime};base64,{}",
                                base64::engine::general_purpose::STANDARD.encode(&bytes)
                            ));
                        }
                    }
                }
            }
        }

        None
    }

    /// Prepare and run, funnelling every failure into one `Failed` status.
    async fn run(
        &self,
        map_name: Option<String>,
        options: GeneratorOptions,
        tx: mpsc::Sender<GeneratorUpdate>,
    ) {
        let closed = tx.clone();
        let cancel = self.cancel.clone();
        let watcher = tokio::spawn(async move {
            closed.closed().await;
            cancel.raise();
        });
        let result = self.run_staged(map_name, options, &tx).await;
        watcher.abort();
        let status = match result {
            RunOutcome::Generated(maps) => GeneratorStatus::Generated { maps },
            RunOutcome::Cancelled => GeneratorStatus::Cancelled,
            RunOutcome::Failed(reason) => GeneratorStatus::Failed { reason },
        };
        let _ = tx.send(GeneratorUpdate::Status(status)).await;
    }

    async fn run_staged(
        &self,
        map_name: Option<String>,
        mut options: GeneratorOptions,
        tx: &mpsc::Sender<GeneratorUpdate>,
    ) -> RunOutcome {
        let _lease = tokio::select! {
            () = self.cancel.raised() => return RunOutcome::Cancelled,
            lease = self.generation.lock() => lease,
        };
        let destination = if map_name.is_none() && !options.output_path.is_empty() {
            let selected = PathBuf::from(&options.output_path);
            if selected.is_absolute() {
                selected
            } else {
                self.config.maps_dir().join(selected)
            }
        } else {
            self.config.maps_dir()
        };
        let staging = destination.join(format!(".faf-install-{:016x}", rand::random::<u64>()));
        if let Err(error) = tokio::fs::create_dir_all(&staging).await {
            return RunOutcome::Failed(format!("could not stage generated maps: {error}"));
        }
        let mut worker = self.for_run();
        worker.config.maps_dir = Some(staging.clone());
        // The chosen output directory is the commit destination. The JVM
        // writes only into staging, relative to its private working directory.
        options.output_path = ".".into();
        let mut result = worker.run_inner(map_name, options, tx).await;
        if self.is_cancelled() {
            result = RunOutcome::Cancelled;
        } else if let RunOutcome::Generated(maps) = &result {
            let mut failure = None;
            for name in maps {
                let target = destination.join(name);
                if target.exists() {
                    continue;
                }
                if let Err(error) = tokio::fs::rename(staging.join(name), target).await {
                    failure = Some(format!("could not install a generated map: {error}"));
                    break;
                }
            }
            if let Some(reason) = failure {
                result = RunOutcome::Failed(reason);
            }
        }
        let _ = tokio::fs::remove_dir_all(staging).await;
        result
    }

    fn generation_worker(&self) -> Self {
        let mut worker = self.for_run();
        worker.cancel = Arc::new(CancelSignal::default());
        let mut active = self.active_runs.lock().unwrap();
        active.retain(|signal| signal.strong_count() > 0);
        active.push(Arc::downgrade(&worker.cancel));
        worker
    }

    async fn run_inner(
        &self,
        map_name: Option<String>,
        options: GeneratorOptions,
        tx: &mpsc::Sender<GeneratorUpdate>,
    ) -> RunOutcome {
        macro_rules! fail {
            ($result:expr) => {
                match $result {
                    Ok(value) => value,
                    Err(reason) => return RunOutcome::Failed(reason.to_string()),
                }
            };
        }

        // Reproducing a map takes its version from the name; a fresh map uses
        // either the explicitly selected version or resolves the newest supported release.
        let version = match &map_name {
            Some(name) => match map_generator::parse_generated_map_name(name) {
                Some(parsed) => parsed.version,
                None => return RunOutcome::Failed(format!("{name} is not a generated map")),
            },
            None => {
                if let Some(explicit) = &options.version {
                    fail!(tokio::select! {
                        biased;
                        () = self.cancel.raised() => return RunOutcome::Cancelled,
                        result = self.resolve_version(Some(explicit)) => result,
                    })
                } else {
                    let _ = tx
                        .send(GeneratorUpdate::Status(GeneratorStatus::ResolvingVersion))
                        .await;
                    fail!(tokio::select! {
                        biased;
                        () = self.cancel.raised() => return RunOutcome::Cancelled,
                        result = self.resolve_latest() => result,
                    })
                }
            }
        };

        // Ask for previews in a folder of our own unless the user named one.
        // Costs nothing on releases that predate the flag: unmatched arguments
        // are ignored rather than fatal, and the folder scan still works.
        //
        // Only the path is decided here. Creating it waits until the run is
        // actually about to start, so a refused version leaves no directories
        // behind for a generator that was never usable.
        let mut options = options;
        if options.preview_path.is_empty() && options.command_line_args.is_empty() {
            options.preview_path = self.preview_dir().to_string_lossy().into_owned();
        }

        // A batch whose selections leave something to choose becomes one run
        // per map, each with its own pick (#414); otherwise this is the single
        // run it always was. Reproducing by name never has a choice to make.
        let batch = match map_name {
            Some(_) => vec![options],
            None => map_generator::plan_batch(&options, rand::random::<u64>),
        };
        // Every command line is built before anything is downloaded, so an
        // unusable option is refused without side effects, as it always was.
        let mut runs = Vec::with_capacity(batch.len());
        for options in &batch {
            runs.push(fail!(map_generator::build_arguments(
                version,
                map_name.as_deref(),
                options,
                self.config.version_policy,
                fallback_seed(),
            )));
        }
        let jar = fail!(self.ensure_jar_inner(version, tx, Some(&self.cancel)).await);
        // A cancellation arriving during the download should stop us here
        // rather than starting a JVM nobody is waiting for.
        if self.is_cancelled() {
            return RunOutcome::Cancelled;
        }
        if runs.len() == 1 {
            let args = runs.pop().unwrap_or_default();
            return self.run_generator(version, &jar, args, tx).await;
        }

        // Sequential on purpose: each run is a CPU-bound JVM, and running
        // them side by side would only make every one of them slower.
        let mut maps = Vec::new();
        for args in runs {
            match self.run_generator(version, &jar, args, tx).await {
                RunOutcome::Generated(names) => maps.extend(names),
                RunOutcome::Cancelled => return RunOutcome::Cancelled,
                RunOutcome::Failed(reason) if maps.is_empty() => return RunOutcome::Failed(reason),
                // Report how far the batch got. Staging is discarded when
                // the batch fails, so no incomplete batch is installed.
                RunOutcome::Failed(reason) => {
                    return RunOutcome::Failed(format!(
                        "{reason} (after {} of {} maps were generated)",
                        maps.len(),
                        batch.len()
                    ))
                }
            }
        }
        RunOutcome::Generated(maps)
    }

    /// Resolve options through the generator's own `--parse`, returning the map
    /// name it would produce.
    ///
    /// The authoritative counterpart to the pure rule checks in
    /// `faf_domain::protocol::map_generator::validate_options`: it applies the
    /// rules of the release actually installed rather than our copy of them,
    /// so it stays correct when the generator changes them. Costs one JVM
    /// start and produces no map.
    async fn preflight_inner(&self, options: &GeneratorOptions) -> Result<String, String> {
        let version = self.resolve_version(options.version.as_deref()).await?;
        // No `--parse` before 1.22.0, and an older release does not refuse the
        // flag: it ignores it and generates a map. An empty name means "this
        // release cannot answer that", which is not a fault in the options and
        // must not be reported as one: the run that follows is still fine.
        if !version.supports_parse() {
            return Ok(String::new());
        }
        let (tx, _rx) = mpsc::channel(8);
        let jar = self.ensure_jar(version, &tx).await?;

        let mut args = map_generator::build_arguments(
            version,
            None,
            options,
            self.config.version_policy,
            fallback_seed(),
        )
        .map_err(|e| e.to_string())?;
        // `--parse` prints and exits, so a viewer window would never open and
        // the debug dump would never be written: both only confuse the output.
        args.retain(|arg| arg != "--visualize" && arg != "--debug");
        args.push("--parse".to_string());

        let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
        let stdout = self.run_query(&jar, &borrowed, PREFLIGHT_TIMEOUT).await?;
        parse_map_name_from_json(&stdout)
            .ok_or_else(|| "the map generator did not report a map name".to_string())
    }

    /// Read cached option lists for a version, if they are on disk.
    ///
    /// Option lists cannot change within a release, so re-running six JVMs
    /// every time the dialog opens is pure waste. The Python client caches the
    /// same thing in `mapgen_options.json`; the Java client re-runs them.
    async fn cached_options(&self, version: &str) -> Option<HashMap<String, Vec<String>>> {
        let raw = tokio::fs::read_to_string(self.options_cache_path())
            .await
            .ok()?;
        let mut all: HashMap<String, HashMap<String, Vec<String>>> =
            serde_json::from_str(&raw).ok()?;
        all.remove(version)
    }

    async fn store_options(&self, version: &str, query: GeneratorOptionQuery, values: &[String]) {
        let path = self.options_cache_path();
        let mut all: HashMap<String, HashMap<String, Vec<String>>> =
            match tokio::fs::read_to_string(&path).await {
                Ok(raw) => serde_json::from_str(&raw).unwrap_or_default(),
                Err(_) => HashMap::new(),
            };
        all.entry(version.to_string())
            .or_default()
            .insert(query.flag().to_string(), values.to_vec());
        if let Ok(serialized) = serde_json::to_string_pretty(&all) {
            let _ = tokio::fs::create_dir_all(self.config.generator_dir()).await;
            let _ = tokio::fs::write(&path, serialized).await;
        }
    }

    fn options_cache_path(&self) -> PathBuf {
        self.config.generator_dir().join("options_cache.json")
    }

    /// Where the generator is asked to drop preview images.
    ///
    /// Having a folder that contains *only* previews, named after their maps,
    /// removes the guesswork from [`Self::read_map_preview`], which otherwise
    /// tries nine filename spellings because the map folder's own preview has
    /// been seen under several. Kept beside the JARs rather than in the maps
    /// directory so a "delete generated maps" sweep does not take it out.
    fn preview_dir(&self) -> PathBuf {
        self.config.generator_dir().join("previews")
    }

    /// Where saved option sets live, one JSON file each.
    ///
    /// A folder of plain files rather than a list inside the client's
    /// settings, so a preset can be copied, backed up or sent to someone else
    /// without exporting anything.
    fn presets_dir(&self) -> PathBuf {
        self.config.generator_dir().join("presets")
    }

    /// Resolve a preset name to its file, refusing anything that could point
    /// outside the presets folder.
    fn preset_path(&self, name: &str) -> Result<PathBuf, String> {
        let file = faf_domain::state::preset_file_name(name)
            .ok_or_else(|| "that preset name cannot be used".to_string())?;
        Ok(self.presets_dir().join(file))
    }
}

/// Pull `"mapName":"…"` out of the generator's `--parse` JSON.
///
/// Deliberately not a full deserialisation: the surrounding `parameters`
/// object gains fields between releases, and this client already has a pure
/// decoder that expands the name into all of them. Taking only the name keeps
/// the shape of the JSON from becoming a compatibility surface.
/// A console window showing one generation run, line by line.
///
/// Not the JVM's own console. The generator writes everything to stdout and the
/// client reads that stdout: the map names and the progress the dialog shows
/// both come out of it, so handing the stream to a console would take both
/// away, and leaving it piped makes the JVM's window an empty black box (a
/// console shows what its process writes to it, and a redirected process writes
/// nowhere near it). So the client opens a console of its own and writes the
/// lines into it as it reads them. `findstr` with a pattern every line matches
/// is the shortest thing in a stock Windows that copies its input to its
/// output a line at a time.
struct RunWindow {
    /// Killed on drop, which closes the window when the run ends, the way the
    /// generator's own console used to close with the process.
    _child: tokio::process::Child,
    input: tokio::process::ChildStdin,
}

impl RunWindow {
    #[cfg(windows)]
    fn open() -> Option<Self> {
        let mut command = Command::new("cmd.exe");
        command
            .arg("/c")
            .arg("findstr")
            .arg("/r")
            .arg(".*")
            .stdin(Stdio::piped())
            .kill_on_drop(true);
        crate::infra::console_window(&mut command, true);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                tracing::warn!(%error, "could not open the map generator window");
                return None;
            }
        };
        let input = child.stdin.take()?;
        Some(Self {
            _child: child,
            input,
        })
    }

    /// Windows has the console; everywhere else a run has no window to open.
    #[cfg(not(windows))]
    fn open() -> Option<Self> {
        None
    }

    /// Best effort throughout: a user who closes the window mid-run must not
    /// take the generation down with it.
    async fn write_line(&mut self, line: &str) {
        use tokio::io::AsyncWriteExt as _;
        let _ = self.input.write_all(line.as_bytes()).await;
        let _ = self.input.write_all(b"\r\n").await;
        let _ = self.input.flush().await;
    }
}

fn parse_map_name_from_json(stdout: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(stdout.trim()).ok()?;
    let name = value.get("mapName")?.as_str()?;
    map_generator::is_generated_map(name).then(|| name.to_string())
}

/// Drop ANSI escape sequences. picocli colours its error output, and those
/// codes would be rendered literally in the dialog.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            out.push(ch);
            continue;
        }
        // A CSI sequence is ESC '[' then parameter and intermediate bytes and
        // finally one byte in the @-~ range. The '[' falls in that range
        // itself, so it has to be consumed before the search for the terminator
        // begins, or every sequence "ends" immediately and leaves its digits
        // behind.
        if chars.peek() == Some(&'[') {
            chars.next();
            for next in chars.by_ref() {
                if ('@'..='~').contains(&next) {
                    break;
                }
            }
        } else {
            // A two-character escape: drop the one character that follows.
            chars.next();
        }
    }
    out
}

/// [`strip_ansi`] over a whole multi-line block.
fn strip_ansi_block(text: &str) -> String {
    text.lines().map(strip_ansi).collect::<Vec<_>>().join("\n")
}

/// Render an argument list for the log, quoting anything with spaces so a
/// logged command line can be pasted back into a shell.
fn shell_quote_all(args: &[String]) -> String {
    args.iter()
        .map(|arg| {
            if arg.contains(char::is_whitespace) {
                format!("\"{arg}\"")
            } else {
                arg.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
}

/// Rebuild release records from remembered tags. Only the tag is ever read out
/// of a release, so a cached list is as good as a fetched one.
fn releases_from_tags(tags: &[String]) -> Vec<GitHubRelease> {
    tags.iter()
        .map(|tag_name| GitHubRelease {
            tag_name: tag_name.clone(),
        })
        .collect()
}

#[async_trait]
impl MapGeneratorPort for NeroxisMapGenerator {
    fn set_show_window(&self, show: bool) {
        self.show_window.store(show, Ordering::Relaxed);
    }

    async fn generate_named(&self, map_name: String) -> mpsc::Receiver<GeneratorUpdate> {
        let (tx, rx) = mpsc::channel(32);
        // A stale cancellation must not stop the run that follows it.
        let runner = self.generation_worker();
        tokio::spawn(async move {
            runner
                .run(Some(map_name), GeneratorOptions::default(), tx)
                .await;
        });
        rx
    }

    async fn generate(&self, options: GeneratorOptions) -> mpsc::Receiver<GeneratorUpdate> {
        let (tx, rx) = mpsc::channel(32);
        let runner = self.generation_worker();
        tokio::spawn(async move {
            runner.run(None, options, tx).await;
        });
        rx
    }

    async fn query_options(
        &self,
        query: GeneratorOptionQuery,
        version: Option<String>,
        progress: Option<mpsc::Sender<GeneratorUpdate>>,
    ) -> Result<Vec<String>, String> {
        let ver = self.resolve_version(version.as_deref()).await?;
        // A release that does not know this flag has no list to give, and
        // asking anyway is not free: the pre-picocli generators treat an
        // unknown flag as "generate a map", so six queries against an old
        // release would write six random maps. An empty list is the honest
        // answer, and it also clears whatever the previously selected release
        // had put in the picker.
        if !query.supported_by(ver) {
            return Ok(Vec::new());
        }
        let version_key = ver.to_string();
        // An option list is fixed within a release, so a cache hit spares a
        // whole JVM start. Six of them open the dialog.
        if let Some(cached) = self.cached_options(&version_key).await {
            if let Some(values) = cached.get(query.flag()) {
                if !values.is_empty() {
                    return Ok(values.clone());
                }
            }
        }

        // The first query on a machine pays for the JAR: 24 MB, which the
        // caller can forward as download progress rather than leaving the
        // dialog looking hung.
        let (fallback, _drain) = mpsc::channel(8);
        let tx = progress.unwrap_or(fallback);
        let jar = self.ensure_jar(ver, &tx).await?;
        let stdout = self
            .run_query(&jar, &[query.flag()], OPTION_QUERY_TIMEOUT)
            .await?;

        let values = map_generator::parse_option_list(&stdout);
        if !values.is_empty() {
            self.store_options(&version_key, query, &values).await;
        }
        Ok(values)
    }

    async fn preflight(&self, options: GeneratorOptions) -> Result<String, String> {
        self.preflight_inner(&options).await
    }

    async fn help(&self, version: Option<String>) -> Result<String, String> {
        let ver = self.resolve_version(version.as_deref()).await?;
        let (tx, _rx) = mpsc::channel(8);
        let jar = self.ensure_jar(ver, &tx).await?;
        // `--help` exits non-zero on some picocli versions, so the stderr path
        // of `run_query` is a legitimate success here too.
        match self
            .run_query(&jar, &["--help"], OPTION_QUERY_TIMEOUT)
            .await
        {
            Ok(text) => Ok(strip_ansi_block(&text)),
            Err(text) => Ok(strip_ansi_block(&text)),
        }
    }

    fn cancel(&self) {
        self.cancel.raise();
        for signal in self
            .active_runs
            .lock()
            .unwrap()
            .iter()
            .filter_map(|run| run.upgrade())
        {
            signal.raise();
        }
    }

    async fn save_preset(&self, name: &str, options: &GeneratorOptions) -> Result<(), String> {
        let path = self.preset_path(name)?;
        let preset = GeneratorPreset {
            // The typed name is kept verbatim; only the file name is derived,
            // so "Team Ladder" stays capitalised in the list.
            name: name.trim().to_string(),
            saved_at: chrono::Utc::now().to_rfc3339(),
            options: options.clone(),
        };
        let json = serde_json::to_string_pretty(&preset)
            .map_err(|e| format!("could not encode the preset: {e}"))?;
        tokio::fs::create_dir_all(self.presets_dir())
            .await
            .map_err(|e| format!("could not create the presets folder: {e}"))?;
        tokio::fs::write(&path, json)
            .await
            .map_err(|e| format!("could not save the preset: {e}"))
    }

    async fn list_presets(&self) -> Vec<GeneratorPreset> {
        let mut presets = Vec::new();
        let Ok(mut entries) = tokio::fs::read_dir(self.presets_dir()).await else {
            // No folder yet simply means no presets, not a failure.
            return presets;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            // A preset that will not parse is skipped, not fatal: one
            // hand-edited file must not hide the rest of the library.
            match tokio::fs::read_to_string(&path).await {
                Ok(raw) => match serde_json::from_str::<GeneratorPreset>(&raw) {
                    Ok(preset) => presets.push(preset),
                    Err(error) => {
                        tracing::warn!(%error, ?path, "skipping an unreadable generator preset")
                    }
                },
                Err(error) => tracing::warn!(%error, ?path, "could not read a generator preset"),
            }
        }
        // Newest first: the one you just saved is the one you are looking for.
        presets.sort_by(|a, b| b.saved_at.cmp(&a.saved_at));
        presets
    }

    async fn delete_preset(&self, name: &str) -> Result<(), String> {
        let path = self.preset_path(name)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            // Already gone is the state the caller wanted.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("could not delete the preset: {error}")),
        }
    }

    async fn latest_version(&self) -> Result<String, String> {
        self.resolve_latest().await.map(|v| v.to_string())
    }

    async fn available_versions(&self) -> Result<Vec<String>, String> {
        NeroxisMapGenerator::available_versions(self).await
    }

    fn is_installed(&self, map_name: &str) -> bool {
        !map_name.is_empty() && self.config.maps_dir().join(map_name).is_dir()
    }

    async fn clean_up(&self, protected_maps: &[String]) -> Result<usize, String> {
        let protected: std::collections::HashSet<String> = protected_maps
            .iter()
            .map(|name| name.trim().to_ascii_lowercase())
            .collect();
        let mut removed = 0;
        let mut entries = match tokio::fs::read_dir(self.config.maps_dir()).await {
            Ok(entries) => entries,
            // No maps folder means nothing to clean, not a failure.
            Err(_) => return Ok(0),
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if !map_generator::is_generated_map(&name) {
                continue;
            }
            if protected.contains(&name.to_ascii_lowercase()) {
                continue;
            }
            if entry.path().is_dir() && tokio::fs::remove_dir_all(entry.path()).await.is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    async fn map_previews(
        &self,
        map_names: &[String],
    ) -> std::collections::HashMap<String, String> {
        let mut map = std::collections::HashMap::new();
        for name in map_names {
            if let Some(data_url) = self.read_map_preview(name).await {
                map.insert(name.clone(), data_url.clone());
                map.insert(name.to_lowercase(), data_url);
            }
        }
        map
    }
}

/// Inert generator: used offline and in tests.
#[derive(Debug, Clone, Default)]
pub struct FakeMapGenerator;

#[async_trait]
impl MapGeneratorPort for FakeMapGenerator {
    async fn generate_named(&self, map_name: String) -> mpsc::Receiver<GeneratorUpdate> {
        let (tx, rx) = mpsc::channel(4);
        // Report success so the offline path exercises the happy flow.
        let _ = tx
            .send(GeneratorUpdate::Status(GeneratorStatus::Generated {
                maps: vec![map_name],
            }))
            .await;
        rx
    }

    async fn generate(&self, _options: GeneratorOptions) -> mpsc::Receiver<GeneratorUpdate> {
        let (tx, rx) = mpsc::channel(4);
        let _ = tx
            .send(GeneratorUpdate::Status(GeneratorStatus::Generated {
                maps: vec!["neroxis_map_generator_1.7.7_offline".into()],
            }))
            .await;
        rx
    }

    async fn query_options(
        &self,
        query: GeneratorOptionQuery,
        _version: Option<String>,
        _progress: Option<mpsc::Sender<GeneratorUpdate>>,
    ) -> Result<Vec<String>, String> {
        // Representative values so the host dialog's pickers aren't empty offline.
        Ok(match query {
            GeneratorOptionQuery::Symmetries => vec!["POINT2".into(), "POINT4".into(), "XZ".into()],
            GeneratorOptionQuery::Styles => {
                vec!["BIG_ISLANDS".into(), "LAND".into(), "LOW_MEX".into()]
            }
            GeneratorOptionQuery::TerrainStyles => vec!["BASIC".into(), "MOUNTAIN_RANGE".into()],
            GeneratorOptionQuery::TextureStyles => vec!["BRIMSTONE".into(), "DESERT".into()],
            GeneratorOptionQuery::ResourceStyles => vec!["BASIC".into(), "LOW_MEX".into()],
            GeneratorOptionQuery::PropStyles => vec!["BASIC".into(), "ROCK_FIELD".into()],
        })
    }

    async fn preflight(&self, options: GeneratorOptions) -> Result<String, String> {
        // Offline, the pure rules are all we have; they are also the ones a
        // user is most likely to trip over, so the dialog still teaches.
        match map_generator::validate_options(&options)
            .into_iter()
            .find(|issue| issue.is_fatal())
        {
            Some(issue) => Err(issue.to_string()),
            None => Ok("neroxis_map_generator_1.7.7_offline".into()),
        }
    }

    async fn help(&self, _version: Option<String>) -> Result<String, String> {
        Ok("Map generator help is unavailable offline.".into())
    }

    fn cancel(&self) {}

    async fn save_preset(&self, _name: &str, _options: &GeneratorOptions) -> Result<(), String> {
        Ok(())
    }

    async fn list_presets(&self) -> Vec<GeneratorPreset> {
        Vec::new()
    }

    async fn delete_preset(&self, _name: &str) -> Result<(), String> {
        Ok(())
    }

    async fn latest_version(&self) -> Result<String, String> {
        Ok("1.7.7".into())
    }

    async fn available_versions(&self) -> Result<Vec<String>, String> {
        Ok(vec!["1.7.7".into(), "1.6.0".into(), "1.5.0".into()])
    }

    fn is_installed(&self, _map_name: &str) -> bool {
        false
    }

    async fn clean_up(&self, _protected_maps: &[String]) -> Result<usize, String> {
        Ok(0)
    }

    async fn map_previews(
        &self,
        _map_names: &[String],
    ) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn non_utf8_console_output_does_not_hide_the_next_map_name() {
        let data = b"caf\xe9\r\nneroxis_map_generator_1.21.0_ualhhyfgnqw4u_cagaeaakbyaaaqd2\n";
        let mut lines = BufReader::new(&data[..]).split(b'\n');
        assert_eq!(
            next_output_line(&mut lines).await.unwrap().unwrap(),
            "caf\u{fffd}"
        );
        let name = next_output_line(&mut lines).await.unwrap().unwrap();
        assert!(!map_generator::scrape_map_names(&name).is_empty());
        assert!(next_output_line(&mut lines).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dropping_generation_receiver_cancels_only_that_run() {
        let root = temp_dir("receiver-cancel");
        let generator = NeroxisMapGenerator::new(config(&root));
        let first = generator.generation_worker();
        let second = generator.generation_worker();
        let lock = generator.generation.lock().await;
        let (tx, rx) = mpsc::channel(4);
        let worker = tokio::spawn(async move {
            first.run(None, GeneratorOptions::default(), tx).await;
            assert!(first.is_cancelled());
        });
        drop(rx);
        tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap();
        assert!(!second.is_cancelled());
        drop(lock);
        assert!(!root.join("maps").exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn cancelling_a_generator_download_removes_its_partial_jar() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let root = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 2048];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\npartial jar")
                .await
                .unwrap();
            stream.flush().await.unwrap();
            std::future::pending::<()>().await;
        });
        let mut configuration = config(root.path());
        configuration.download_url_format = format!("http://{address}/{{version}}.jar");
        let mut generator = NeroxisMapGenerator::new(configuration).generation_worker();
        generator.http = reqwest::Client::new();
        let version = GeneratorVersion::parse("1.21.0").unwrap();
        let target = generator.jar_path(version);
        let partial = target.with_extension("partial");
        let signal = generator.cancel.clone();
        let (tx, _rx) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            generator
                .ensure_jar_inner(version, &tx, Some(&generator.cancel))
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if tokio::fs::metadata(&partial)
                    .await
                    .is_ok_and(|metadata| metadata.len() > 0)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        signal.raise();
        assert!(tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert!(!partial.exists());
        assert!(!target.exists());
        server.abort();
        let _ = server.await;
    }

    fn config(dir: &Path) -> MapGeneratorConfig {
        MapGeneratorConfig {
            releases_url: "http://localhost/releases".into(),
            download_url_format: "http://localhost/{version}.jar".into(),
            java_path: Some("java".into()),
            generator_dir: Some(dir.join("generators")),
            maps_dir: Some(dir.join("maps")),
            version_policy: VersionPolicy::default(),
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("faf-mapgen-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn jar_paths_are_version_stamped_so_releases_coexist() {
        let dir = temp_dir("jar-paths");
        let generator = NeroxisMapGenerator::new(config(&dir));
        let old = generator.jar_path(GeneratorVersion::parse("1.7.7").unwrap());
        let new = generator.jar_path(GeneratorVersion::parse("1.8.0").unwrap());
        assert_ne!(old, new);
        assert!(old.ends_with("MapGenerator_1.7.7.jar"));
    }

    #[test]
    fn is_installed_only_matches_an_existing_folder() {
        let dir = temp_dir("installed");
        let generator = NeroxisMapGenerator::new(config(&dir));
        let name = "neroxis_map_generator_1.7.7_abc";
        assert!(!generator.is_installed(name));
        std::fs::create_dir_all(dir.join("maps").join(name)).unwrap();
        assert!(generator.is_installed(name));
        assert!(!generator.is_installed(""));
    }

    #[tokio::test]
    async fn clean_up_removes_generated_maps_and_spares_the_rest() {
        let dir = temp_dir("cleanup");
        let maps = dir.join("maps");
        for name in [
            "neroxis_map_generator_1.7.7_aaa",
            "neroxis_map_generator_1.7.7_bbb",
            "scmp_009",
            "adaptive_gadostb.v0002",
        ] {
            std::fs::create_dir_all(maps.join(name)).unwrap();
        }
        let generator = NeroxisMapGenerator::new(config(&dir));
        assert_eq!(
            generator
                .clean_up(&["Neroxis_Map_Generator_1.7.7_AAA".into()])
                .await
                .unwrap(),
            1
        );
        assert!(maps.join("neroxis_map_generator_1.7.7_aaa").exists());
        assert!(!maps.join("neroxis_map_generator_1.7.7_bbb").exists());
        assert!(maps.join("scmp_009").exists());
        assert!(maps.join("adaptive_gadostb.v0002").exists());
    }

    #[tokio::test]
    async fn clean_up_is_fine_without_a_maps_folder() {
        let dir = temp_dir("cleanup-missing");
        let generator = NeroxisMapGenerator::new(config(&dir));
        assert_eq!(generator.clean_up(&[]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn a_version_outside_the_window_is_refused_before_downloading_anything() {
        // The download URL points at a dead host, so reaching it would fail
        // with a network error instead: the version message proves the policy
        // check ran first.
        let dir = temp_dir("unsupported-version");
        let generator = NeroxisMapGenerator::new(MapGeneratorConfig {
            version_policy: VersionPolicy {
                min_major: 1,
                max_major: 1,
            },
            ..config(&dir)
        });
        let mut rx = generator
            .generate_named("neroxis_map_generator_0.9.0_abc".into())
            .await;
        let GeneratorUpdate::Status(status) = rx.recv().await.unwrap();
        let GeneratorStatus::Failed { reason } = status else {
            panic!("expected a failure, got {status:?}");
        };
        assert!(reason.contains("older"), "{reason}");
        assert!(
            !dir.join("generators").exists(),
            "nothing should have been downloaded"
        );
    }

    #[tokio::test]
    async fn a_too_new_version_is_refused_with_update_advice() {
        let dir = temp_dir("too-new-version");
        let generator = NeroxisMapGenerator::new(config(&dir));
        let mut rx = generator
            .generate_named("neroxis_map_generator_9.0.0_abc".into())
            .await;
        let GeneratorUpdate::Status(status) = rx.recv().await.unwrap();
        let GeneratorStatus::Failed { reason } = status else {
            panic!("expected a failure, got {status:?}");
        };
        assert!(reason.contains("update the client"), "{reason}");
    }

    #[test]
    fn an_inverted_version_window_falls_back_to_the_default() {
        // Guards against a typo'd deployment override silently rejecting every
        // generator version.
        let inverted = VersionPolicy {
            min_major: 5,
            max_major: 1,
        };
        assert!(!inverted.allows_major(1));
        assert!(!inverted.allows_major(5));
        // `version_policy_from_env` refuses to build one; see its fallback.
        assert!(VersionPolicy::default().allows_major(1));
    }

    #[test]
    fn the_predicted_map_name_is_read_out_of_the_parse_output() {
        // Verbatim output of `NeroxisGen_1.22.1.jar --parse --map-size 10km
        // --spawn-count 6 --num-teams 2 --style MOUNTAIN_RANGE
        // --terrain-symmetry POINT2 --seed 12345`.
        let stdout = r#"{"parameters":{"seed":12345,"spawnCount":6,"mapSize":512,"numTeams":2,"mode":{"terrainSymmetry":"POINT2","mapStyle":"MOUNTAIN_RANGE"}},"mapName":"neroxis_map_generator_1.22.1_aaaaaaaaaayds_ayeaeaaj"}"#;
        assert_eq!(
            parse_map_name_from_json(stdout).as_deref(),
            Some("neroxis_map_generator_1.22.1_aaaaaaaaaayds_ayeaeaaj")
        );
    }

    #[test]
    fn parse_output_that_is_not_a_map_name_is_refused() {
        // A future release could add fields, but a name that is not a
        // generated map means we misread the output entirely.
        assert!(parse_map_name_from_json(r#"{"mapName":"scmp_009"}"#).is_none());
        assert!(parse_map_name_from_json(r#"{"parameters":{}}"#).is_none());
        assert!(parse_map_name_from_json("not json at all").is_none());
        assert!(parse_map_name_from_json("").is_none());
    }

    #[test]
    fn picocli_colour_codes_are_stripped_from_error_messages() {
        // The real refusal, escape codes and all, for spawn 5 with 2 teams.
        let raw = "\u{1b}[31m\u{1b}[1mSpawn Count `5` not a multiple of Num Teams `2`\u{1b}[21m\u{1b}[39m\u{1b}[0m";
        assert_eq!(
            strip_ansi(raw),
            "Spawn Count `5` not a multiple of Num Teams `2`"
        );
        assert_eq!(strip_ansi("plain text"), "plain text");
    }

    #[test]
    fn a_logged_command_line_can_be_pasted_back_into_a_shell() {
        assert_eq!(
            shell_quote_all(&[
                "--out-path".into(),
                r"C:\Users\Max Mustermann\maps".into(),
                "--debug".into(),
            ]),
            r#"--out-path "C:\Users\Max Mustermann\maps" --debug"#
        );
    }

    #[tokio::test]
    async fn a_raised_cancellation_is_observed_even_if_it_arrives_first() {
        // The listener-before-check ordering exists for exactly this case: a
        // cancellation that lands before anyone starts waiting must not be lost.
        let signal = CancelSignal::default();
        signal.raise();
        tokio::time::timeout(Duration::from_millis(500), signal.raised())
            .await
            .expect("an already-raised signal must resolve immediately");
        assert!(signal.is_raised());
        signal.clear();
        assert!(!signal.is_raised());
    }

    #[tokio::test]
    async fn a_cancellation_raised_later_still_wakes_the_waiter() {
        let signal = Arc::new(CancelSignal::default());
        let waiter = Arc::clone(&signal);
        let task = tokio::spawn(async move { waiter.raised().await });
        // Give the waiter time to register before raising.
        tokio::time::sleep(Duration::from_millis(50)).await;
        signal.raise();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("the waiter should have been woken")
            .expect("the waiting task should not panic");
    }

    #[tokio::test]
    async fn option_lists_are_cached_per_generator_version() {
        // Six JVM starts per dialog open is the cost this avoids; the lists
        // cannot change within a release, so the cache is always valid.
        let dir = temp_dir("options-cache");
        let generator = NeroxisMapGenerator::new(config(&dir));
        assert!(generator.cached_options("1.22.1").await.is_none());

        generator
            .store_options(
                "1.22.1",
                GeneratorOptionQuery::Styles,
                &["BASIC".to_string(), "VALLEY".to_string()],
            )
            .await;
        let cached = generator.cached_options("1.22.1").await.unwrap();
        assert_eq!(
            cached.get("--styles").map(Vec::as_slice),
            Some(["BASIC".to_string(), "VALLEY".to_string()].as_slice())
        );
        // A different release must not read another's answers.
        assert!(generator.cached_options("1.21.2").await.is_none());
    }

    #[tokio::test]
    async fn storing_a_second_list_keeps_the_first() {
        let dir = temp_dir("options-cache-merge");
        let generator = NeroxisMapGenerator::new(config(&dir));
        generator
            .store_options("1.22.1", GeneratorOptionQuery::Styles, &["BASIC".into()])
            .await;
        generator
            .store_options(
                "1.22.1",
                GeneratorOptionQuery::Symmetries,
                &["POINT2".into()],
            )
            .await;
        let cached = generator.cached_options("1.22.1").await.unwrap();
        assert!(cached.contains_key("--styles"));
        assert!(cached.contains_key("--symmetries"));
    }

    #[tokio::test]
    async fn presets_round_trip_through_the_folder() {
        let dir = temp_dir("presets");
        let generator = NeroxisMapGenerator::new(config(&dir));
        assert!(generator.list_presets().await.is_empty());

        let options = GeneratorOptions {
            spawn_count: Some(8),
            num_teams: Some(4),
            ..Default::default()
        };
        generator
            .save_preset("Team Ladder", &options)
            .await
            .unwrap();

        let presets = generator.list_presets().await;
        assert_eq!(presets.len(), 1);
        // The typed name survives verbatim; only the file name is derived.
        assert_eq!(presets[0].name, "Team Ladder");
        assert_eq!(presets[0].options.spawn_count, Some(8));
        assert!(!presets[0].saved_at.is_empty());
        assert!(dir.join("generators/presets/team-ladder.json").is_file());
    }

    #[tokio::test]
    async fn saving_the_same_name_replaces_rather_than_duplicates() {
        let dir = temp_dir("presets-replace");
        let generator = NeroxisMapGenerator::new(config(&dir));
        let first = GeneratorOptions {
            spawn_count: Some(2),
            ..Default::default()
        };
        let second = GeneratorOptions {
            spawn_count: Some(16),
            ..Default::default()
        };
        generator.save_preset("Ladder", &first).await.unwrap();
        // Case and spacing differ, but it is the same preset to a reader.
        generator.save_preset("  ladder ", &second).await.unwrap();

        let presets = generator.list_presets().await;
        assert_eq!(presets.len(), 1, "{presets:?}");
        assert_eq!(presets[0].options.spawn_count, Some(16));
    }

    #[tokio::test]
    async fn a_preset_name_cannot_escape_the_presets_folder() {
        // The name reaches the file system, so this is the boundary that stops
        // a crafted preset name writing anywhere it likes.
        let dir = temp_dir("presets-escape");
        let generator = NeroxisMapGenerator::new(config(&dir));
        for name in ["../evil", "..", ".hidden", "a/b"] {
            assert!(
                generator
                    .save_preset(name, &GeneratorOptions::default())
                    .await
                    .is_err(),
                "{name:?} should be refused"
            );
        }
        assert!(generator.list_presets().await.is_empty());
    }

    #[tokio::test]
    async fn an_unreadable_preset_does_not_hide_the_rest() {
        let dir = temp_dir("presets-corrupt");
        let generator = NeroxisMapGenerator::new(config(&dir));
        generator
            .save_preset("Good", &GeneratorOptions::default())
            .await
            .unwrap();
        tokio::fs::write(dir.join("generators/presets/broken.json"), "{ not json")
            .await
            .unwrap();

        let presets = generator.list_presets().await;
        assert_eq!(presets.len(), 1);
        assert_eq!(presets[0].name, "Good");
    }

    #[tokio::test]
    async fn deleting_a_preset_that_is_already_gone_is_not_an_error() {
        // The caller wanted it absent, and it is.
        let dir = temp_dir("presets-delete");
        let generator = NeroxisMapGenerator::new(config(&dir));
        assert!(generator.delete_preset("never existed").await.is_ok());

        generator
            .save_preset("Gone", &GeneratorOptions::default())
            .await
            .unwrap();
        assert!(generator.delete_preset("Gone").await.is_ok());
        assert!(generator.list_presets().await.is_empty());
    }

    #[tokio::test]
    async fn the_generator_log_rotates_instead_of_growing_without_limit() {
        let dir = temp_dir("generator-log");
        let generator = NeroxisMapGenerator::new(config(&dir));
        generator.log_line("first line").await;
        let path = generator.log_path();
        assert!(tokio::fs::read_to_string(&path)
            .await
            .unwrap()
            .contains("first line"));

        // Push it past the cap, then write again: the old file is kept aside
        // rather than deleted, since it holds the run that just failed.
        tokio::fs::write(&path, vec![b'x'; (MAX_LOG_BYTES + 1) as usize])
            .await
            .unwrap();
        generator.log_line("after rotation").await;
        let current = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(current.trim(), "after rotation");
        assert!(path.with_extension("log.1").exists());
    }

    #[tokio::test]
    async fn generating_a_non_generated_name_fails_before_touching_the_network() {
        let dir = temp_dir("bad-name");
        let generator = NeroxisMapGenerator::new(config(&dir));
        let mut rx = generator.generate_named("scmp_009".into()).await;
        let GeneratorUpdate::Status(status) = rx.recv().await.unwrap();
        assert!(
            matches!(status, GeneratorStatus::Failed { reason } if reason.contains("not a generated map")),
        );
    }
}
