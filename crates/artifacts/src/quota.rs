use pioneer_crud::CrudStore;
use serde::Serialize;

use crate::error::{ArtifactError, ArtifactResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactQuotaPolicy {
    pub max_file_bytes: u64,
    pub max_workspace_bytes: u64,
    pub max_files_per_workspace: u64,
    pub warn_at_percent: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactWorkspaceUsage {
    pub workspace_id: String,
    pub bytes: u64,
    pub files: u64,
    pub warning: Option<ArtifactQuotaWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactQuotaWarning {
    pub current_bytes: u64,
    pub limit_bytes: u64,
    pub percent_used: u8,
}

impl Default for ArtifactQuotaPolicy {
    fn default() -> Self {
        Self {
            max_file_bytes: 512 * 1024 * 1024,
            max_workspace_bytes: 10 * 1024 * 1024 * 1024,
            max_files_per_workspace: 100_000,
            warn_at_percent: 80,
        }
    }
}

impl ArtifactQuotaPolicy {
    pub fn disabled() -> Self {
        Self {
            max_file_bytes: u64::MAX,
            max_workspace_bytes: u64::MAX,
            max_files_per_workspace: u64::MAX,
            warn_at_percent: 100,
        }
    }

    pub fn check_file_size(&self, size_bytes: u64) -> ArtifactResult<()> {
        if size_bytes > self.max_file_bytes {
            return Err(ArtifactError::QuotaExceeded {
                message: format!(
                    "artifact file size {size_bytes} exceeds max_file_bytes={}",
                    self.max_file_bytes
                ),
                current_bytes: 0,
                limit_bytes: self.max_file_bytes,
            });
        }
        Ok(())
    }

    pub fn check_workspace_headroom(
        &self,
        usage: &ArtifactWorkspaceUsage,
        incoming_bytes: u64,
    ) -> ArtifactResult<()> {
        if usage.files.saturating_add(1) > self.max_files_per_workspace {
            return Err(ArtifactError::QuotaExceeded {
                message: format!(
                    "workspace `{}` exceeds max_files_per_workspace={}",
                    usage.workspace_id, self.max_files_per_workspace
                ),
                current_bytes: usage.bytes,
                limit_bytes: self.max_workspace_bytes,
            });
        }
        let projected = usage.bytes.saturating_add(incoming_bytes);
        if projected > self.max_workspace_bytes {
            return Err(ArtifactError::QuotaExceeded {
                message: format!(
                    "workspace `{}` storage would exceed max_workspace_bytes={}",
                    usage.workspace_id, self.max_workspace_bytes
                ),
                current_bytes: usage.bytes,
                limit_bytes: self.max_workspace_bytes,
            });
        }
        Ok(())
    }

    pub fn workspace_warning(
        &self,
        usage: &ArtifactWorkspaceUsage,
    ) -> Option<ArtifactQuotaWarning> {
        if self.max_workspace_bytes == 0 || self.max_workspace_bytes == u64::MAX {
            return None;
        }
        let percent = usage
            .bytes
            .saturating_mul(100)
            .saturating_div(self.max_workspace_bytes)
            .min(100) as u8;
        (percent >= self.warn_at_percent).then_some(ArtifactQuotaWarning {
            current_bytes: usage.bytes,
            limit_bytes: self.max_workspace_bytes,
            percent_used: percent,
        })
    }
}

pub async fn workspace_usage(
    store: &CrudStore,
    workspace_id: &str,
) -> ArtifactResult<ArtifactWorkspaceUsage> {
    let usage = store.artifact_workspace_usage(workspace_id).await?;
    Ok(ArtifactWorkspaceUsage {
        workspace_id: usage.workspace_id,
        bytes: usage.bytes,
        files: usage.files,
        warning: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_rejects_file_over_limit() {
        let policy = ArtifactQuotaPolicy {
            max_file_bytes: 4,
            ..ArtifactQuotaPolicy::default()
        };

        assert!(policy.check_file_size(5).is_err());
    }

    #[test]
    fn quota_rejects_workspace_over_limit() {
        let policy = ArtifactQuotaPolicy {
            max_workspace_bytes: 10,
            ..ArtifactQuotaPolicy::default()
        };
        let usage = ArtifactWorkspaceUsage {
            workspace_id: "ws".to_owned(),
            bytes: 8,
            files: 1,
            warning: None,
        };

        assert!(policy.check_workspace_headroom(&usage, 3).is_err());
    }

    #[test]
    fn quota_warns_at_configured_threshold() {
        let policy = ArtifactQuotaPolicy {
            max_workspace_bytes: 10,
            warn_at_percent: 80,
            ..ArtifactQuotaPolicy::default()
        };
        let usage = ArtifactWorkspaceUsage {
            workspace_id: "ws".to_owned(),
            bytes: 8,
            files: 1,
            warning: None,
        };

        let warning = policy.workspace_warning(&usage).expect("warning");
        assert_eq!(warning.percent_used, 80);
    }
}
