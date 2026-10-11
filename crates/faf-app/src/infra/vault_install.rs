//! Bounded, transactional installation of downloaded archives.
//!
//! Vault URLs cross the IPC boundary and zip metadata is remote input. Keep
//! the trust checks, body bound, path validation, expansion bound, and staging
//! rename in one place so maps and mods cannot drift apart.
//!
//! Two layouts are installed from here, sharing one safety envelope:
//!
//! * [`install_archive`] for the vault, where the archive's single top-level
//!   folder *is* the installed identity (a map or mod folder name).
//! * [`install_flat_archive`] for a publisher who ships loose files and lets
//!   the caller name the directory: the Galactic War client is exported this
//!   way, two files at the archive root.
//!
//! The per-entry checks live in [`check_entry`] so a second layout cannot be
//! added with a weaker envelope than the first.

use std::ffi::OsString;
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};

use futures_util::StreamExt as _;

/// Generous enough for large content packages, bounded enough that a broken
/// or hostile server cannot consume all process memory.
pub const MAX_DOWNLOAD_BYTES: u64 = 512 * 1024 * 1024;
/// Zip bombs are constrained by both advertised expanded bytes and entries.
const MAX_EXPANDED_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_ARCHIVE_ENTRIES: usize = 100_000;

/// Require a vault download to stay on the configured content origin and in
/// the owning category (`maps` or `mods`). The configured origin may be HTTP
/// for an explicit local test setup; production defaults to HTTPS.
pub fn validate_url(raw: &str, content_base: &str, category: &str) -> Result<(), String> {
    let (url, base) = same_origin_urls(raw, content_base)?;

    let base_path = base.path().trim_end_matches('/');
    let category_prefix = format!("{base_path}/{category}/");
    if !url.path().starts_with(&category_prefix) {
        return Err(format!(
            "refusing a vault download outside the {category} content path"
        ));
    }
    Ok(())
}

/// Require an ordinary HTTP(S) URL on the configured origin. Useful for
/// generated endpoints such as replay downloads that have no category path.
pub fn validate_origin_url(raw: &str, configured_base: &str) -> Result<(), String> {
    same_origin_urls(raw, configured_base).map(|_| ())
}

fn same_origin_urls(raw: &str, configured_base: &str) -> Result<(url::Url, url::Url), String> {
    let url = url::Url::parse(raw).map_err(|_| "vault download URL is invalid".to_string())?;
    let base = url::Url::parse(configured_base)
        .map_err(|_| "configured FAF content URL is invalid".to_string())?;
    let ordinary_origin = |value: &url::Url| {
        !value.cannot_be_a_base()
            && value.username().is_empty()
            && value.password().is_none()
            && matches!(value.scheme(), "http" | "https")
    };
    if !ordinary_origin(&url) || !ordinary_origin(&base) || url.origin() != base.origin() {
        return Err("refusing a vault download outside the configured FAF content origin".into());
    }
    Ok((url, base))
}

/// The refusal both readers give, naming the size and the limit: "larger than
/// the allowed download size" alone left a failed launch impossible to place
/// from the toast (#282).
fn too_large(subject: &str, size: u64, max_bytes: u64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    format!(
        "{subject} is {:.0} MiB, more than the {:.0} MiB allowed for one download",
        size as f64 / MIB,
        max_bytes as f64 / MIB
    )
}

/// Read a response without trusting `Content-Length` or buffering forever.
pub async fn bounded_body(
    response: reqwest::Response,
    subject: &str,
    max_bytes: u64,
) -> Result<Vec<u8>, String> {
    bounded_body_with_progress(response, subject, max_bytes, &|_, _| {}).await
}

/// [`bounded_body`], reporting as the bytes arrive.
///
/// `on_bytes` is called with the bytes received so far and the total the
/// response declared, if it declared one. It is called per chunk, so a caller
/// that puts anything on a channel needs its own throttle.
pub async fn bounded_body_with_progress(
    response: reqwest::Response,
    subject: &str,
    max_bytes: u64,
    on_bytes: &(dyn Fn(u64, Option<u64>) + Sync),
) -> Result<Vec<u8>, String> {
    let declared = response.content_length();
    if let Some(size) = declared.filter(|size| *size > max_bytes) {
        return Err(too_large(subject, size, max_bytes));
    }

    let mut body = Vec::new();
    let mut received = 0_u64;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            format!(
                "could not read {subject}: {}",
                crate::infra::http::describe_transport_error(&error)
            )
        })?;
        received = received
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| format!("{subject} is too large"))?;
        if received > max_bytes {
            return Err(too_large(subject, received, max_bytes));
        }
        body.extend_from_slice(&chunk);
        on_bytes(received, declared);
    }
    Ok(body)
}

/// A downloaded archive held on disk, deleted when it goes out of scope.
///
/// Not `tempfile::NamedTempFile`: the install path has to open the same file
/// twice by name, which that type deliberately makes awkward, and the only
/// thing wanted here is "a path that cleans itself up".
pub struct DownloadedArchive {
    path: PathBuf,
}

impl DownloadedArchive {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DownloadedArchive {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// [`bounded_body_with_progress`], writing to a temporary file instead of
/// growing a `Vec`.
///
/// Same bounds, same progress callback, same refusal of a response that
/// declares or delivers more than `max_bytes`. The difference is where the
/// bytes live: a 512 MiB map no longer needs 512 MiB of heap to be unpacked
/// from, and the file is removed when the returned handle is dropped, whether
/// the install succeeded or not.
pub async fn bounded_body_to_file(
    response: reqwest::Response,
    subject: &str,
    max_bytes: u64,
    on_bytes: &(dyn Fn(u64, Option<u64>) + Sync),
) -> Result<DownloadedArchive, String> {
    let declared = response.content_length();
    if let Some(size) = declared.filter(|size| *size > max_bytes) {
        return Err(too_large(subject, size, max_bytes));
    }

    let directory = std::env::temp_dir().join(crate::infra::APP_SLUG);
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(|error| format!("could not create a download folder: {error}"))?;
    let path = directory.join(format!(".faf-download-{:016x}", rand::random::<u64>()));
    // Constructed before the first write, so an error partway through still
    // takes the partial file with it.
    let archive = DownloadedArchive { path: path.clone() };

    let mut file = tokio::fs::File::create(&path)
        .await
        .map_err(|error| format!("could not open a download file: {error}"))?;
    let mut received = 0_u64;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            format!(
                "could not read {subject}: {}",
                crate::infra::http::describe_transport_error(&error)
            )
        })?;
        received = received
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| format!("{subject} is too large"))?;
        if received > max_bytes {
            return Err(too_large(subject, received, max_bytes));
        }
        tokio::io::AsyncWriteExt::write_all(&mut file, &chunk)
            .await
            .map_err(|error| format!("could not save {subject}: {error}"))?;
        on_bytes(received, declared);
    }
    tokio::io::AsyncWriteExt::flush(&mut file)
        .await
        .map_err(|error| format!("could not save {subject}: {error}"))?;
    drop(file);
    Ok(archive)
}

/// The single top-level folder a vault archive installs into, without
/// extracting anything.
///
/// Simulation-mod preparation needs this before it touches the disk: the folder
/// is the only thing that tells it whether a *different version* of the same
/// mod is already installed, and it must be able to stop and ask rather than
/// find out halfway through an extraction.
pub fn archive_root_name(bytes: &[u8]) -> Result<String, String> {
    let root = inspect_archive(std::io::Cursor::new(bytes), None)?;
    // A name we cannot represent is a name we cannot safely compare against
    // what is on disk either, so it is rejected rather than lossily converted.
    root.into_string()
        .map_err(|_| "vault archive's install folder is not valid UTF-8".to_string())
}

/// Validate and extract one top-level folder into a private staging directory,
/// validate its contents, then rename it into place. The destination is never
/// left half-installed and an existing folder is never overwritten.
pub fn install_archive<F>(
    bytes: &[u8],
    destination: &Path,
    expected_root: Option<&str>,
    validate_contents: F,
) -> Result<PathBuf, String>
where
    F: FnOnce(&Path) -> Result<(), String>,
{
    install_archive_from(
        || Ok(std::io::Cursor::new(bytes)),
        destination,
        expected_root,
        validate_contents,
    )
}

/// [`install_archive`], reading the archive off disk rather than out of a
/// `Vec`.
///
/// A map can be half a gigabyte, and holding all of it in the heap to hand a
/// slice to a zip reader that only ever seeks around it is a cost with nothing
/// to show for it. The download streams into a temporary file and this opens
/// that file twice: once to inspect the shape, once to extract. Every bound the
/// in-memory path enforces is enforced here, because it is the same code.
pub fn install_archive_from_file<F>(
    archive: &Path,
    destination: &Path,
    expected_root: Option<&str>,
    validate_contents: F,
) -> Result<PathBuf, String>
where
    F: FnOnce(&Path) -> Result<(), String>,
{
    install_archive_from(
        || {
            std::fs::File::open(archive)
                .map_err(|error| format!("could not read the downloaded archive: {error}"))
        },
        destination,
        expected_root,
        validate_contents,
    )
}

/// The shared body. `open` is called twice, once per pass over the archive.
fn install_archive_from<R, O, F>(
    open: O,
    destination: &Path,
    expected_root: Option<&str>,
    validate_contents: F,
) -> Result<PathBuf, String>
where
    R: std::io::Read + std::io::Seek,
    O: Fn() -> Result<R, String>,
    F: FnOnce(&Path) -> Result<(), String>,
{
    let root_name = inspect_archive(open()?, expected_root)?;
    std::fs::create_dir_all(destination)
        .map_err(|error| format!("could not create {}: {error}", destination.display()))?;

    let target = destination.join(&root_name);
    if target.exists() {
        return Err(format!("{} is already installed", target.display()));
    }

    let staging = unique_staging_path(destination);
    std::fs::create_dir(&staging)
        .map_err(|error| format!("could not create install staging folder: {error}"))?;
    let outcome = (|| {
        extract_archive(open()?, &staging, "vault archive")?;
        let staged_root = staging.join(&root_name);
        validate_contents(&staged_root)?;
        std::fs::rename(&staged_root, &target).map_err(|error| {
            format!("could not finish installing {}: {error}", target.display())
        })?;
        Ok(target.clone())
    })();
    let _ = std::fs::remove_dir_all(&staging);
    outcome
}

/// Install an archive that has no wrapping folder into a directory the caller
/// names.
///
/// Same envelope as [`install_archive`]: bounded expansion, no symbolic links,
/// no path escaping the destination, and an extraction into staging that is
/// renamed into place, so a failure leaves nothing half-installed. The
/// difference is only the layout: here the archive's entries land directly in
/// `target`, which must not already exist.
///
/// `validate_contents` is handed the staged directory before it is renamed, so
/// a caller can insist the files it expects are actually in there.
#[cfg(test)]
pub fn install_flat_archive<F>(
    bytes: &[u8],
    target: &Path,
    subject: &str,
    validate_contents: F,
) -> Result<(), String>
where
    F: FnOnce(&Path) -> Result<(), String>,
{
    install_flat_archive_from(
        || Ok(std::io::Cursor::new(bytes)),
        target,
        subject,
        validate_contents,
        None,
    )
}

pub(crate) fn install_flat_archive_cancellable<F>(
    bytes: &[u8],
    target: &Path,
    subject: &str,
    validate_contents: F,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<(), String>
where
    F: FnOnce(&Path) -> Result<(), String>,
{
    install_flat_archive_from(
        || Ok(std::io::Cursor::new(bytes)),
        target,
        subject,
        validate_contents,
        Some(cancel),
    )
}

fn install_flat_archive_from<R, O, F>(
    open: O,
    target: &Path,
    subject: &str,
    validate_contents: F,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> Result<(), String>
where
    R: std::io::Read + std::io::Seek,
    O: Fn() -> Result<R, String>,
    F: FnOnce(&Path) -> Result<(), String>,
{
    inspect_flat_archive(open()?, subject)?;

    let parent = target
        .parent()
        .ok_or_else(|| format!("{} is not a usable install location", target.display()))?;
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("could not create {}: {error}", parent.display()))?;
    if target.exists() {
        return Err(format!("{} is already installed", target.display()));
    }

    // Staged beside the target so the rename stays on one filesystem.
    let staging = unique_staging_path(parent);
    std::fs::create_dir(&staging)
        .map_err(|error| format!("could not create install staging folder: {error}"))?;
    let outcome = (|| {
        extract_archive_checked(open()?, &staging, subject, cancel)?;
        validate_contents(&staging)?;
        check_cancelled(cancel)?;
        std::fs::rename(&staging, target)
            .map_err(|error| format!("could not finish installing {}: {error}", target.display()))
    })();
    if outcome.is_err() {
        let _ = std::fs::remove_dir_all(&staging);
    }
    outcome
}

fn unique_staging_path(destination: &Path) -> PathBuf {
    loop {
        let candidate = destination.join(format!(".faf-install-{:016x}", rand::random::<u64>()));
        if !candidate.exists() {
            return candidate;
        }
    }
}

pub(crate) fn is_install_staging_name(name: &str) -> bool {
    name.strip_prefix(".faf-install-").is_some_and(|suffix| {
        suffix.len() == 16 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

/// Stage and validate a replacement before moving the installed directory.
/// The recovery record lets startup restore the old copy after a hard kill
/// between the two renames. Failed extraction never touches the old copy.
pub(crate) fn replace_archive<F>(
    bytes: &[u8],
    destination: &Path,
    previous: &Path,
    validate_contents: F,
) -> Result<PathBuf, String>
where
    F: FnOnce(&Path) -> Result<(), String>,
{
    let root = inspect_archive(std::io::Cursor::new(bytes), None)?;
    let target = destination.join(root);
    let relative = previous
        .strip_prefix(destination)
        .map_err(|_| "replacement is outside the install folder".to_string())?;
    let same_target = target == previous
        || (cfg!(windows)
            && target
                .to_string_lossy()
                .eq_ignore_ascii_case(&previous.to_string_lossy()));
    if !same_target && target.exists() {
        return Err("the replacement folder is already occupied".into());
    }
    let staging = unique_staging_path(destination);
    std::fs::create_dir_all(&staging)
        .map_err(|error| format!("could not create replacement staging folder: {error}"))?;
    let outcome = (|| {
        let incoming = staging.join("incoming");
        extract_archive(std::io::Cursor::new(bytes), &incoming, "mod archive")?;
        let staged = incoming.join(target.file_name().unwrap());
        validate_contents(&staged)?;
        let record = serde_json::json!({
            "previous": relative,
            "target": target.strip_prefix(destination).unwrap(),
        });
        std::fs::write(staging.join("restore.json"), record.to_string())
            .map_err(|error| format!("could not record replacement recovery: {error}"))?;
        let backup = staging.join("previous");
        let had_previous = previous.exists();
        if had_previous {
            std::fs::rename(previous, &backup)
                .map_err(|error| format!("could not move the installed mod aside: {error}"))?;
        }
        if let Err(error) = std::fs::rename(&staged, &target) {
            if had_previous {
                std::fs::rename(&backup, previous).map_err(|restore| {
                    format!("could not install the replacement: {error}; could not restore the old mod: {restore}")
                })?;
            }
            return Err(format!("could not install the replacement: {error}"));
        }
        Ok(target)
    })();
    // A failed rollback must retain the only remaining copy for startup recovery.
    if !staging.join("previous").exists() || outcome.is_ok() {
        let _ = std::fs::remove_dir_all(&staging);
    }
    outcome
}

/// Bounds every archive must respect before a single entry is read.
fn check_archive_shape(len: usize, subject: &str) -> Result<(), String> {
    if len == 0 {
        return Err(format!("{subject} is empty"));
    }
    if len > MAX_ARCHIVE_ENTRIES {
        return Err(format!("{subject} contains too many entries"));
    }
    Ok(())
}

/// The per-entry checks every layout shares: no symbolic links, a bounded
/// total expansion, and a path that cannot escape the destination.
///
/// Takes the entry's properties rather than the entry, so both callers can use
/// it without either naming the zip crate's borrow-bound reader type.
fn check_entry(
    unix_mode: Option<u32>,
    size: u64,
    enclosed_name: Option<PathBuf>,
    expanded: &mut u64,
    subject: &str,
) -> Result<PathBuf, String> {
    if unix_mode.is_some_and(|mode| mode & 0o170000 == 0o120000) {
        return Err(format!("{subject} contains a symbolic link"));
    }
    *expanded = expanded
        .checked_add(size)
        .ok_or_else(|| format!("{subject} expands beyond the allowed size"))?;
    if *expanded > MAX_EXPANDED_BYTES {
        return Err(format!("{subject} expands beyond the allowed size"));
    }
    enclosed_name.ok_or_else(|| format!("{subject} contains an unsafe path"))
}

/// Validate an archive whose entries sit at the top level, with no wrapping
/// folder. The caller owns the destination directory name.
fn inspect_flat_archive<R: std::io::Read + std::io::Seek>(
    reader: R,
    subject: &str,
) -> Result<(), String> {
    let mut archive = zip::ZipArchive::new(reader)
        .map_err(|error| format!("not a valid zip archive: {error}"))?;
    check_archive_shape(archive.len(), subject)?;

    let mut expanded = 0_u64;
    for index in 0..archive.len() {
        let entry = archive
            .by_index(index)
            .map_err(|error| format!("could not inspect archive entry: {error}"))?;
        check_entry(
            entry.unix_mode(),
            entry.size(),
            entry.enclosed_name(),
            &mut expanded,
            subject,
        )?;
    }
    Ok(())
}

fn inspect_archive<R: std::io::Read + std::io::Seek>(
    reader: R,
    expected_root: Option<&str>,
) -> Result<OsString, String> {
    const SUBJECT: &str = "vault archive";
    let mut archive = zip::ZipArchive::new(reader)
        .map_err(|error| format!("not a valid zip archive: {error}"))?;
    check_archive_shape(archive.len(), SUBJECT)?;

    let mut root: Option<OsString> = None;
    let mut expanded = 0_u64;
    for index in 0..archive.len() {
        let entry = archive
            .by_index(index)
            .map_err(|error| format!("could not inspect archive entry: {error}"))?;
        let relative = check_entry(
            entry.unix_mode(),
            entry.size(),
            entry.enclosed_name(),
            &mut expanded,
            SUBJECT,
        )?;
        let mut components = relative.components();
        let Some(Component::Normal(first)) = components.next() else {
            return Err("vault archive contains an unsafe path".into());
        };
        if components.next().is_none() && !entry.is_dir() {
            return Err("vault archive must contain one top-level folder".into());
        }
        match &root {
            Some(existing) if existing != first => {
                return Err("vault archive contains more than one top-level folder".into())
            }
            None => root = Some(first.to_os_string()),
            _ => {}
        }
    }

    let root = root.ok_or_else(|| "vault archive has no install folder".to_string())?;
    if let Some(expected) = expected_root {
        if !root.to_string_lossy().eq_ignore_ascii_case(expected) {
            return Err(format!(
                "vault archive installs {:?}, expected {expected:?}",
                root.to_string_lossy()
            ));
        }
    }
    Ok(root)
}

/// Unpack one entry, refusing an archive that decompresses to more than its
/// own header said it would.
///
/// [`check_entry`] bounds the *declared* sizes, and a header is written by
/// whoever built the archive. `zip`'s reader caps the compressed stream and
/// puts no ceiling at all on the decompressed output, so copying to EOF wrote
/// whatever the stream produced: a 65 kB archive declaring ten bytes an entry
/// really does write 64 MB an entry, and the vault is content anybody can
/// upload. Hosting a game on such a map would have every player who joined
/// download it and fill their disk.
///
/// Reading one byte past the declared size is what turns that number from a
/// claim into a limit: if the reader can still produce it, the header lied,
/// and nothing beyond the limit has been written yet.
///
/// `remaining` is the running budget across the whole archive, so a thousand
/// honestly-declared entries cannot add up past the ceiling either.
fn extract_entry(
    entry: &mut impl std::io::Read,
    declared: u64,
    output: &Path,
    remaining: &mut u64,
    subject: &str,
) -> Result<(), String> {
    use std::io::Read as _;

    let allowed = declared.min(*remaining);
    let mut file = std::fs::File::create(output)
        .map_err(|error| format!("could not create {}: {error}", output.display()))?;
    let written = std::io::copy(&mut entry.take(allowed.saturating_add(1)), &mut file)
        .map_err(|error| format!("could not write {}: {error}", output.display()))?;
    if written > allowed {
        return Err(format!(
            "{subject} contains an entry bigger than the size it declares"
        ));
    }
    *remaining -= written;
    file.flush()
        .map_err(|error| format!("could not finish {}: {error}", output.display()))?;
    Ok(())
}

fn extract_archive<R: std::io::Read + std::io::Seek>(
    reader: R,
    destination: &Path,
    subject: &str,
) -> Result<(), String> {
    extract_archive_checked(reader, destination, subject, None)
}

fn check_cancelled(cancel: Option<&tokio_util::sync::CancellationToken>) -> Result<(), String> {
    if cancel.is_some_and(|token| token.is_cancelled()) {
        Err("installation cancelled".into())
    } else {
        Ok(())
    }
}

fn extract_archive_checked<R: std::io::Read + std::io::Seek>(
    reader: R,
    destination: &Path,
    subject: &str,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> Result<(), String> {
    let mut archive = zip::ZipArchive::new(reader)
        .map_err(|error| format!("not a valid zip archive: {error}"))?;
    let mut remaining = MAX_EXPANDED_BYTES;
    for index in 0..archive.len() {
        check_cancelled(cancel)?;
        let mut entry = archive
            .by_index(index)
            .map_err(|error| format!("could not read archive entry: {error}"))?;
        let relative = entry
            .enclosed_name()
            .ok_or_else(|| format!("{subject} contains an unsafe path"))?;
        let output = destination.join(relative);
        if entry.is_dir() {
            std::fs::create_dir_all(&output)
                .map_err(|error| format!("could not create {}: {error}", output.display()))?;
            continue;
        }
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("could not create {}: {error}", parent.display()))?;
        }
        let declared = entry.size();
        extract_entry(&mut entry, declared, &output, &mut remaining, subject)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// An archive whose headers understate an entry, byte for byte the shape
    /// the review demonstrated: the declared size is patched in both the local
    /// header and the central directory, while the compressed stream still
    /// holds the real payload.
    ///
    /// `zip` caps the compressed side and not the decompressed side, so before
    /// the bound in [`extract_entry`] this wrote the whole payload and
    /// `check_entry` booked it as ten bytes.
    fn zip_that_lies_about_its_size(name: &str, payload: &[u8], declared: u32) -> Vec<u8> {
        let mut bytes = std::io::Cursor::new(Vec::new());
        {
            let mut writer = zip::ZipWriter::new(&mut bytes);
            writer
                .start_file(name, zip::write::SimpleFileOptions::default())
                .unwrap();
            writer.write_all(payload).unwrap();
            writer.finish().unwrap();
        }
        let mut bytes = bytes.into_inner();

        // Uncompressed size sits at +22 in a local file header and at +24 in a
        // central directory record.
        for (signature, offset) in [(b"PK\x03\x04", 22_usize), (b"PK\x01\x02", 24_usize)] {
            let at = bytes
                .windows(4)
                .position(|window| window == signature.as_slice())
                .expect("the writer emits both records");
            bytes[at + offset..at + offset + 4].copy_from_slice(&declared.to_le_bytes());
        }
        bytes
    }

    #[test]
    fn an_archive_that_understates_an_entry_is_refused_rather_than_written() {
        let payload = vec![0_u8; 4 * 1024 * 1024];
        let bytes = zip_that_lies_about_its_size("bomb.v0001/heightmap.raw", &payload, 10);

        let root = std::env::temp_dir().join(format!("faf-zip-bomb-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);

        let outcome = install_archive(&bytes, &root, None, |_| Ok(()));
        let error = outcome.expect_err("an archive that lies about its size must not install");
        assert!(
            error.contains("bigger than the size it declares"),
            "unexpected error: {error}"
        );

        // Staging is removed either way, so nothing of the payload survives.
        let leftovers: Vec<_> = std::fs::read_dir(&root)
            .map(|entries| entries.flatten().map(|entry| entry.path()).collect())
            .unwrap_or_default();
        assert!(leftovers.is_empty(), "left behind {leftovers:?}");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn nothing_past_the_declared_size_is_ever_written_to_disk() {
        // The refusal above proves the archive is rejected. This proves the
        // rejection happens *before* the payload lands: the old code wrote
        // every byte the decompressor produced and only then had anything to
        // compare, which on a real bomb is gigabytes onto the user's disk.
        let payload = vec![0_u8; 4 * 1024 * 1024];
        let output = std::env::temp_dir().join(format!("faf-zip-bound-{}", std::process::id()));
        let _ = std::fs::remove_file(&output);

        let mut remaining = MAX_EXPANDED_BYTES;
        let error = extract_entry(
            &mut payload.as_slice(),
            10,
            &output,
            &mut remaining,
            "test archive",
        )
        .expect_err("a reader that outruns its declared size is refused");
        assert!(
            error.contains("bigger than the size it declares"),
            "{error}"
        );

        let written = std::fs::metadata(&output)
            .map(|meta| meta.len())
            .unwrap_or(0);
        assert!(written <= 11, "wrote {written} bytes for a 10 byte entry");

        let _ = std::fs::remove_file(&output);
    }

    #[test]
    fn the_budget_is_shared_across_every_entry() {
        // A thousand honestly-declared entries must not add up past the
        // ceiling either, which is what the running total is for.
        let output = std::env::temp_dir().join(format!("faf-zip-budget-{}", std::process::id()));
        let _ = std::fs::remove_file(&output);

        let mut remaining = 4_u64;
        extract_entry(
            &mut b"abcd".as_slice(),
            4,
            &output,
            &mut remaining,
            "test archive",
        )
        .expect("the first entry fits exactly");
        assert_eq!(remaining, 0, "the budget is spent by what was written");

        let error = extract_entry(
            &mut b"e".as_slice(),
            1,
            &output,
            &mut remaining,
            "test archive",
        )
        .expect_err("nothing fits once the budget is gone");
        assert!(
            error.contains("bigger than the size it declares"),
            "{error}"
        );

        let _ = std::fs::remove_file(&output);
    }

    #[test]
    fn an_honest_archive_still_installs() {
        // The other half of the bound: a truthful header must not be refused
        // by the extra byte the check reads.
        let root = std::env::temp_dir().join(format!("faf-zip-honest-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);

        let bytes = zip(&[("honest.v0001/scenario.lua", b"-- a map")]);
        let installed = install_archive(&bytes, &root, Some("honest.v0001"), |_| Ok(()))
            .expect("an ordinary archive installs");
        assert_eq!(
            std::fs::read(installed.join("scenario.lua")).unwrap(),
            b"-- a map"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    fn zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut bytes = std::io::Cursor::new(Vec::new());
        {
            let mut writer = zip::ZipWriter::new(&mut bytes);
            for (name, contents) in entries {
                writer
                    .start_file(*name, zip::write::SimpleFileOptions::default())
                    .unwrap();
                writer.write_all(contents).unwrap();
            }
            writer.finish().unwrap();
        }
        bytes.into_inner()
    }

    #[test]
    fn the_install_folder_is_read_without_extracting_anything() {
        // What simulation-mod preparation compares against the mods directory
        // before it decides whether it is about to overwrite somebody's mod.
        let archive = zip(&[
            ("Total Mayhem/mod_info.lua", b"uid = \"old\""),
            ("Total Mayhem/hook/init.lua", b"-- hook"),
        ]);
        assert_eq!(archive_root_name(&archive).unwrap(), "Total Mayhem");

        // The same shapes `install_archive` refuses, refused here too, so a
        // conflict check can never be the thing that accepts a bad archive.
        let loose = zip(&[("mod_info.lua", b"uid = \"old\"")]);
        assert!(archive_root_name(&loose).is_err());
        let two_roots = zip(&[("A/mod_info.lua", b"a"), ("B/mod_info.lua", b"b")]);
        assert!(archive_root_name(&two_roots).is_err());
    }

    #[test]
    fn content_downloads_stay_on_the_configured_category() {
        assert!(validate_url(
            "https://content.faforever.com/maps/test.zip",
            "https://content.faforever.com",
            "maps"
        )
        .is_ok());
        for hostile in [
            "http://content.faforever.com/maps/test.zip",
            "https://evil.invalid/maps/test.zip",
            "https://content.faforever.com/mods/test.zip",
            "file:///maps/test.zip",
        ] {
            assert!(
                validate_url(hostile, "https://content.faforever.com", "maps").is_err(),
                "{hostile} must be refused"
            );
        }
    }

    #[test]
    fn archive_requires_one_expected_root() {
        let bytes = zip(&[("wanted/file.txt", b"ok")]);
        assert_eq!(
            inspect_archive(Cursor::new(&bytes), Some("wanted")).unwrap(),
            OsString::from("wanted")
        );
        assert!(inspect_archive(Cursor::new(&bytes), Some("other")).is_err());

        let multiple = zip(&[("one/a", b"a"), ("two/b", b"b")]);
        assert!(inspect_archive(Cursor::new(&multiple), None).is_err());
    }

    fn temp_dir(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("faf-{tag}-test-{}", rand::random::<u64>()))
    }

    #[test]
    fn a_flat_archive_installs_into_the_directory_the_caller_names() {
        // The shape the Galactic War client ships in: loose files, no root.
        let bytes = zip(&[
            ("faf_galactic_war_client.exe", b"binary"),
            ("faf_galactic_war_client.pck", b"content"),
        ]);
        let root = temp_dir("flat");
        let target = root.join("v2026.04.04.1");

        install_flat_archive(&bytes, &target, "the Galactic War archive", |_| Ok(())).unwrap();

        assert!(target.join("faf_galactic_war_client.exe").is_file());
        assert!(target.join("faf_galactic_war_client.pck").is_file());
        // The staging directory is gone, not left beside the install.
        let entries = std::fs::read_dir(&root).unwrap().count();
        assert_eq!(entries, 1);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn the_vault_installer_still_refuses_what_the_flat_one_accepts() {
        let flat = zip(&[("faf_galactic_war_client.exe", b"binary")]);
        assert!(inspect_archive(Cursor::new(&flat), None).is_err());
        assert!(inspect_flat_archive(Cursor::new(&flat), "archive").is_ok());
    }

    #[test]
    fn a_flat_archive_cannot_escape_its_directory() {
        let escaping = zip(&[("../escaped.txt", b"nope")]);
        assert!(inspect_flat_archive(Cursor::new(&escaping), "archive").is_err());
    }

    #[test]
    fn a_flat_install_refuses_to_overwrite_an_existing_version() {
        let bytes = zip(&[("faf_galactic_war_client.exe", b"binary")]);
        let root = temp_dir("flat-existing");
        let target = root.join("v1");
        std::fs::create_dir_all(&target).unwrap();

        let result = install_flat_archive(&bytes, &target, "archive", |_| Ok(()));

        assert!(result.is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_flat_validation_leaves_nothing_behind() {
        let bytes = zip(&[("faf_galactic_war_client.exe", b"binary")]);
        let root = temp_dir("flat-invalid");
        let target = root.join("v1");

        let result = install_flat_archive(&bytes, &target, "archive", |_| {
            Err("the content pack is missing".into())
        });

        assert!(result.is_err());
        assert!(!target.exists());
        let entries = std::fs::read_dir(&root).unwrap().count();
        assert_eq!(entries, 0, "no staging folder is left behind");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_content_validation_leaves_no_installed_or_staging_folder() {
        let temp = std::env::temp_dir().join(format!("faf-vault-test-{}", rand::random::<u64>()));
        let bytes = zip(&[("mod/mod_info.lua", b"uid = 'wrong'")]);
        let result = install_archive(&bytes, &temp, None, |_| Err("wrong uid".into()));

        assert!(result.is_err());
        let entries = std::fs::read_dir(&temp)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(entries.is_empty());
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn a_replacement_failure_preserves_the_old_version_and_success_swaps_it() {
        let root = temp_dir("replace");
        let old = root.join("mod");
        std::fs::create_dir_all(&old).unwrap();
        std::fs::write(old.join("mod_info.lua"), "old").unwrap();
        let bytes = zip(&[("mod/mod_info.lua", b"new")]);
        assert!(replace_archive(&bytes, &root, &old, |_| Err("wrong uid".into())).is_err());
        assert_eq!(
            std::fs::read_to_string(old.join("mod_info.lua")).unwrap(),
            "old"
        );
        assert!(replace_archive(b"broken zip", &root, &old, |_| Ok(())).is_err());
        assert_eq!(
            std::fs::read_to_string(old.join("mod_info.lua")).unwrap(),
            "old"
        );
        replace_archive(&bytes, &root, &old, |_| Ok(())).unwrap();
        assert_eq!(
            std::fs::read_to_string(old.join("mod_info.lua")).unwrap(),
            "new"
        );
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        let differently_cased = zip(&[("MOD/mod_info.lua", b"newer")]);
        let updated = replace_archive(&differently_cased, &root, &old, |_| Ok(())).unwrap();
        assert_eq!(
            std::fs::read_to_string(updated.join("mod_info.lua")).unwrap(),
            "newer"
        );
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cancellation_before_flat_install_commit_discards_staging() {
        let root = temp_dir("flat-cancel");
        let target = root.join("version");
        let bytes = zip(&[("client.exe", b"binary")]);
        let cancel = tokio_util::sync::CancellationToken::new();
        let result = install_flat_archive_cancellable(
            &bytes,
            &target,
            "client",
            |_| {
                cancel.cancel();
                Ok(())
            },
            &cancel,
        );
        assert!(result.is_err());
        assert!(!target.exists());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
        std::fs::remove_dir_all(root).unwrap();
    }
}
