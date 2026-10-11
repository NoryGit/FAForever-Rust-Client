//! Startup recovery for this client's private staging artifacts.

use std::path::{Component, Path};

pub fn cleanup_interrupted_work(generator_output: &str) {
    for root in [super::maps::maps_dir(), super::mods::mods_dir()] {
        sweep(&root, Artifact::Install);
    }
    if !generator_output.is_empty() {
        let selected = std::path::PathBuf::from(generator_output);
        let root = if selected.is_absolute() {
            selected
        } else {
            super::maps::maps_dir().join(selected)
        };
        sweep(&root, Artifact::Install);
    }
    if let Ok(root) = super::data_dir() {
        sweep(&root.join("galactic-war"), Artifact::Install);
    }
    sweep(
        &std::env::temp_dir().join(super::APP_SLUG),
        Artifact::Download,
    );
    sweep(
        &super::map_generator::generator_dir(),
        Artifact::GeneratorDownload,
    );
    if let Ok(root) = super::cache_dir() {
        sweep(&root, Artifact::Upload);
    }
}

#[derive(Clone, Copy)]
enum Artifact {
    Install,
    Download,
    GeneratorDownload,
    Upload,
}

fn matches_artifact(name: &str, kind: Artifact) -> bool {
    match kind {
        Artifact::Install => super::vault_install::is_install_staging_name(name),
        Artifact::Download => name.strip_prefix(".faf-download-").is_some_and(|suffix| {
            suffix.len() == 16 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        }),
        Artifact::GeneratorDownload => name
            .strip_prefix("MapGenerator_")
            .and_then(|name| name.strip_suffix(".partial"))
            .and_then(faf_domain::protocol::map_generator::GeneratorVersion::parse)
            .is_some_and(|version| format!("MapGenerator_{version}.partial") == name),
        Artifact::Upload => name
            .strip_prefix("upload-")
            .and_then(|rest| {
                rest.strip_prefix("map-")
                    .or_else(|| rest.strip_prefix("mod-"))
            })
            .and_then(|rest| rest.strip_suffix(".zip"))
            .is_some_and(|pid| !pid.is_empty() && pid.bytes().all(|byte| byte.is_ascii_digit())),
    }
}

fn sweep(root: &Path, kind: Artifact) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_symlink() || !matches_artifact(&entry.file_name().to_string_lossy(), kind) {
            continue;
        }
        let path = entry.path();
        let outcome = match kind {
            Artifact::Install if file_type.is_dir() => recover_replacement(root, &path)
                .and_then(|()| std::fs::remove_dir_all(&path).map_err(|error| error.to_string())),
            Artifact::Download | Artifact::GeneratorDownload | Artifact::Upload
                if file_type.is_file() =>
            {
                std::fs::remove_file(&path).map_err(|error| error.to_string())
            }
            _ => continue,
        };
        if outcome.is_err() {
            tracing::warn!("an interrupted-work artifact could not be cleaned up; it was retained");
        }
    }
}

fn recovery_target(root: &Path, text: &str) -> Result<std::path::PathBuf, String> {
    let components: Vec<_> = Path::new(text).components().collect();
    if components.is_empty()
        || components.len() > 2
        || components
            .iter()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err("invalid replacement recovery path".into());
    }
    let mut target = root.to_path_buf();
    for component in components {
        target.push(component);
        if std::fs::symlink_metadata(&target)
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            return Err("replacement recovery path contains a link".into());
        }
    }
    Ok(target)
}

fn recover_replacement(root: &Path, staging: &Path) -> Result<(), String> {
    let backup = staging.join("previous");
    if !backup.exists() {
        return Ok(());
    }
    let record: serde_json::Value = serde_json::from_slice(
        &std::fs::read(staging.join("restore.json")).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let previous = recovery_target(
        root,
        record["previous"].as_str().ok_or("missing previous path")?,
    )?;
    let target = recovery_target(
        root,
        record["target"].as_str().ok_or("missing target path")?,
    )?;
    if !target.exists() {
        if previous.exists() {
            return Err("replacement recovery folder is occupied".into());
        }
        std::fs::rename(backup, previous).map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_recovers_interrupted_swap_and_spares_unrelated_files() {
        let root =
            std::env::temp_dir().join(format!("faf-recovery-test-{:016x}", rand::random::<u64>()));
        let stage = root.join(".faf-install-0123456789abcdef");
        std::fs::create_dir_all(stage.join("previous")).unwrap();
        std::fs::write(stage.join("previous/mod_info.lua"), "old version").unwrap();
        std::fs::write(
            stage.join("restore.json"),
            r#"{"previous":"mod","target":"mod"}"#,
        )
        .unwrap();
        std::fs::create_dir(root.join(".faf-install-user-folder")).unwrap();
        std::fs::write(root.join("keep.zip"), "keep").unwrap();
        sweep(&root, Artifact::Install);
        assert_eq!(
            std::fs::read_to_string(root.join("mod/mod_info.lua")).unwrap(),
            "old version"
        );
        assert!(!stage.exists());
        assert!(root.join(".faf-install-user-folder").exists());
        assert!(root.join("keep.zip").exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cleanup_removes_only_owned_downloads_and_uploads_and_retains_invalid_recovery() {
        let root = tempfile::tempdir().unwrap();
        for name in [
            ".faf-download-0123456789abcdef",
            "upload-map-123.zip",
            "upload-mod-123.zip",
            "upload-personal.zip",
            ".faf-download-keep",
            "MapGenerator_1.21.0.partial",
            "MapGenerator_personal.partial",
        ] {
            std::fs::write(root.path().join(name), "temporary").unwrap();
        }
        let stage = root.path().join(".faf-install-0123456789abcdef");
        std::fs::create_dir_all(stage.join("previous")).unwrap();
        std::fs::write(
            stage.join("restore.json"),
            r#"{"previous":"../outside","target":"mod"}"#,
        )
        .unwrap();
        sweep(root.path(), Artifact::Install);
        assert!(stage.join("previous").exists());
        sweep(root.path(), Artifact::Download);
        sweep(root.path(), Artifact::GeneratorDownload);
        sweep(root.path(), Artifact::Upload);
        assert!(!root.path().join(".faf-download-0123456789abcdef").exists());
        assert!(!root.path().join("upload-map-123.zip").exists());
        assert!(!root.path().join("upload-mod-123.zip").exists());
        assert!(root.path().join("upload-personal.zip").exists());
        assert!(root.path().join(".faf-download-keep").exists());
        assert!(!root.path().join("MapGenerator_1.21.0.partial").exists());
        assert!(root.path().join("MapGenerator_personal.partial").exists());
    }
}
