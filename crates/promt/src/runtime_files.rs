use crate::constants::files::BootstrapFileKind;
use crate::content;
use anyhow::{Context, Result, bail};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeIdentityFilesReport {
    pub created: Vec<PathBuf>,
    pub existing: Vec<PathBuf>,
}

struct RuntimeIdentitySeed {
    kind: BootstrapFileKind,
    content: &'static str,
}

fn runtime_identity_seeds() -> [RuntimeIdentitySeed; 2] {
    [
        RuntimeIdentitySeed {
            kind: BootstrapFileKind::Soul,
            content: content::SEED_SOUL_CORE_PROMPT,
        },
        RuntimeIdentitySeed {
            kind: BootstrapFileKind::Identity,
            content: content::SEED_IDENTITY_CORE_PROMPT,
        },
    ]
}

fn ensure_existing_file(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path).with_context(|| {
        format!(
            "failed to inspect runtime identity file `{}`",
            path.display()
        )
    })?;
    if !metadata.is_file() {
        bail!(
            "runtime identity path `{}` exists but is not a file",
            path.display()
        );
    }
    Ok(())
}

fn create_seed_file(path: &Path, content: &str) -> Result<bool> {
    let mut file = match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            ensure_existing_file(path)?;
            return Ok(false);
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to create runtime identity file `{}`",
                    path.display()
                )
            });
        }
    };

    if let Err(error) = file.write_all(content.as_bytes()) {
        let _ = fs::remove_file(path);
        return Err(error).with_context(|| {
            format!("failed to write runtime identity file `{}`", path.display())
        });
    }

    Ok(true)
}

pub fn ensure_runtime_identity_files(runtime_home: &Path) -> Result<RuntimeIdentityFilesReport> {
    fs::create_dir_all(runtime_home).with_context(|| {
        format!(
            "failed to create runtime home directory `{}`",
            runtime_home.display()
        )
    })?;

    let mut report = RuntimeIdentityFilesReport {
        created: Vec::new(),
        existing: Vec::new(),
    };

    for seed in runtime_identity_seeds() {
        let path = runtime_home.join(seed.kind.canonical_name());
        if path.exists() {
            ensure_existing_file(path.as_path())?;
            report.existing.push(path);
            continue;
        }

        if create_seed_file(path.as_path(), seed.content)? {
            report.created.push(path);
        } else {
            report.existing.push(path);
        }
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::ensure_runtime_identity_files;
    use crate::content;

    fn temp_runtime_home(name: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "pioneer_promt_runtime_files_{name}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp runtime home");
        root
    }

    #[test]
    fn creates_missing_files_with_seed_content() {
        let root = temp_runtime_home("creates_missing");

        let report = ensure_runtime_identity_files(root.as_path()).expect("ensure runtime files");

        assert_eq!(report.created.len(), 2);
        assert!(report.existing.is_empty());
        assert_eq!(
            std::fs::read_to_string(root.join("SOUL.md")).expect("read SOUL"),
            content::SEED_SOUL_CORE_PROMPT
        );
        assert_eq!(
            std::fs::read_to_string(root.join("IDENTITY.md")).expect("read IDENTITY"),
            content::SEED_IDENTITY_CORE_PROMPT
        );
    }

    #[test]
    fn preserves_existing_files() {
        let root = temp_runtime_home("preserves_existing");
        std::fs::write(root.join("SOUL.md"), "custom soul").expect("write SOUL");
        std::fs::write(root.join("IDENTITY.md"), "").expect("write empty IDENTITY");

        let report = ensure_runtime_identity_files(root.as_path()).expect("ensure runtime files");

        assert!(report.created.is_empty());
        assert_eq!(report.existing.len(), 2);
        assert_eq!(
            std::fs::read_to_string(root.join("SOUL.md")).expect("read SOUL"),
            "custom soul"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("IDENTITY.md")).expect("read IDENTITY"),
            ""
        );
    }

    #[test]
    fn errors_when_identity_path_is_directory() {
        let root = temp_runtime_home("directory_path");
        std::fs::create_dir(root.join("SOUL.md")).expect("create SOUL directory");

        let error = ensure_runtime_identity_files(root.as_path())
            .expect_err("directory identity path should error");

        assert!(
            error.to_string().contains("exists but is not a file"),
            "unexpected error: {error:#}"
        );
    }
}
