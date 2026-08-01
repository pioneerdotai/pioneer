//! Owned cache paths for authenticated HTTP artifact downloads.

use std::path::Path;

use anyhow::{Result, bail};

use crate::platform::ClientPath;

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
    let safe_gateway_id = safe_path_segment(gateway_profile_id, "gateway");
    let safe_workspace_id = safe_path_segment(workspace_id, "workspace");
    let safe_artifact_id = safe_path_segment(artifact_id, "artifact");
    let safe_version_id = safe_path_segment(version_id, "version");
    let safe_display_name = safe_path_segment(display_name, "artifact.bin");

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
    let part_path = directory.join(format!("{safe_display_name}.part"));
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

fn safe_path_segment(value: &str, fallback: &str) -> String {
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
    }
}
