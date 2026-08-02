//! Owned cache paths for authenticated HTTP artifact downloads.

use std::path::Path;

use anyhow::{Result, bail};
use sha2::{Digest, Sha256};

use crate::platform::ClientPath;

const MAX_CACHE_PATH_COMPONENT_BYTES: usize = 100;
const PART_FILE_SUFFIX: &str = ".part";
const DIGEST_PREFIX_BYTES: usize = 16;
const MAX_EXTENSION_BYTES: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactHttpDownloadCachePaths {
    pub final_path: ClientPath,
    pub part_path: ClientPath,
}

pub fn build_artifact_http_download_cache_path(
    runtime_home: &Path,
    gateway_profile_id: &str,
    workspace_id: &str,
    artifact_id: &str,
    version_id: &str,
    display_name: &str,
) -> Result<ArtifactHttpDownloadCachePaths> {
    let safe_gateway_id = safe_identity_segment(gateway_profile_id, "gateway");
    let safe_workspace_id = safe_identity_segment(workspace_id, "workspace");
    let safe_artifact_id = safe_identity_segment(artifact_id, "artifact");
    let safe_version_id = safe_identity_segment(version_id, "version");
    let safe_display_name = safe_file_name(display_name, "artifact.bin");

    let directory = runtime_home
        .join("downloads")
        .join("gateways")
        .join(safe_gateway_id)
        .join("workspaces")
        .join(safe_workspace_id)
        .join("artifacts")
        .join(safe_artifact_id)
        .join(safe_version_id);
    let final_path = directory.join(safe_display_name.as_str());
    let part_path = directory.join(format!("{safe_display_name}{PART_FILE_SUFFIX}"));
    ensure_child_path(runtime_home, final_path.as_path())?;
    ensure_child_path(runtime_home, part_path.as_path())?;
    Ok(ArtifactHttpDownloadCachePaths {
        final_path: ClientPath::new(final_path),
        part_path: ClientPath::new(part_path),
    })
}

fn ensure_child_path(root: &Path, candidate: &Path) -> Result<()> {
    if !candidate.starts_with(root) {
        bail!("artifact HTTP download cache path escaped runtime home");
    }
    Ok(())
}

fn sanitized_segment(value: &str, fallback: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    let trimmed = sanitized
        .trim_matches([' ', '\t', '\n', '\r'])
        .trim_matches('.');
    if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        fallback.to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn safe_identity_segment(value: &str, fallback: &str) -> String {
    let readable = sanitized_segment(value, fallback);
    let readable_budget = MAX_CACHE_PATH_COMPONENT_BYTES - DIGEST_PREFIX_BYTES - 1;
    format!(
        "{}-{}",
        bounded_ascii_prefix(readable.as_str(), readable_budget),
        digest_prefix(value)
    )
}

fn safe_file_name(value: &str, fallback: &str) -> String {
    let readable = sanitized_segment(value, fallback);
    let path = Path::new(readable.as_str());
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("artifact");
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .map(|value| bounded_ascii_prefix(value, MAX_EXTENSION_BYTES));
    // The temporary path appends `.part`, so bound the final name tightly
    // enough that both final and partial path components stay within the same
    // portable limit.
    let final_name_budget = MAX_CACHE_PATH_COMPONENT_BYTES - PART_FILE_SUFFIX.len();
    let extension_bytes = extension.as_ref().map_or(0, |value| value.len() + 1);
    let stem_budget = final_name_budget - DIGEST_PREFIX_BYTES - 1 - extension_bytes;
    let stem = bounded_ascii_prefix(stem, stem_budget);
    let digest = digest_prefix(value);
    match extension {
        Some(extension) => format!("{stem}-{digest}.{extension}"),
        None => format!("{stem}-{digest}"),
    }
}

fn bounded_ascii_prefix(value: &str, max_bytes: usize) -> &str {
    &value[..value.len().min(max_bytes)]
}

fn digest_prefix(value: &str) -> String {
    let digest = hex::encode(Sha256::digest(value.as_bytes()));
    digest[..DIGEST_PREFIX_BYTES].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_download_cache_path_is_owned_and_sanitized() {
        let runtime_home = std::path::PathBuf::from("/owned/runtime");
        let paths = build_artifact_http_download_cache_path(
            runtime_home.as_path(),
            "../gateway",
            "workspace/one",
            "artifact/one",
            "version/one",
            "../report.txt",
        )
        .expect("safe cache paths");

        assert!(paths.final_path.as_path().starts_with(&runtime_home));
        assert!(paths.part_path.as_path().starts_with(&runtime_home));
        assert!(paths.part_path.as_path().to_string_lossy().ends_with(".part"));
        assert_eq!(paths.final_path.as_path().extension().and_then(|value| value.to_str()), Some("txt"));
    }

    #[test]
    fn sanitized_identity_collisions_and_long_names_have_distinct_bounded_paths() {
        let runtime_home = std::path::PathBuf::from("/owned/runtime");
        let first = build_artifact_http_download_cache_path(
            runtime_home.as_path(),
            "gateway/a",
            "workspace",
            "artifact",
            "version",
            &format!("{}.pdf", "report".repeat(300)),
        )
        .unwrap();
        let second = build_artifact_http_download_cache_path(
            runtime_home.as_path(),
            "gateway?a",
            "workspace",
            "artifact",
            "version",
            &format!("{}.pdf", "report".repeat(300)),
        )
        .unwrap();

        assert_ne!(first.final_path, second.final_path);
        for path in [&first.final_path, &first.part_path] {
            for component in path.as_path().components() {
                if let std::path::Component::Normal(value) = component {
                    assert!(value.to_string_lossy().len() <= MAX_CACHE_PATH_COMPONENT_BYTES);
                }
            }
        }
        assert_eq!(first.final_path.as_path().extension().and_then(|value| value.to_str()), Some("pdf"));
    }
}
