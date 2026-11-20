//! Backend execution identity and current App/workspace validation.
use super::super::*;
use anyhow::{Context, Result};
impl MessageProcessor {
    pub(crate) fn host_api(&self) -> &crate::host_api::HostApi {
        &self.host_api
    }
    // Durable admission is checked when a backend opens its connection.
    // Running messages use the issued execution authority and its cancellation
    // token. App lifecycle reconciliation and workspace invalidation revoke it;
    // neither input nor output traffic schedules database discovery.
    pub(crate) async fn validate_host_execution(
        &self,
        scope: &crate::host_api::ExecutionAuthority,
    ) -> Result<pioneer_crud::PackageRecord> {
        anyhow::ensure!(!scope.cancel.is_cancelled(), "host.revoked");
        let parent = self
            .crud_store
            .find_package_installation(&scope.app_id)
            .await?
            .context("host.revoked")?;
        anyhow::ensure!(
            parent.kind == pioneer_protocol::PackageKind::App
                && parent.enabled
                && parent.state == "installed"
                && parent.workspace_id == scope.workspace_id,
            "host.revoked"
        );
        anyhow::ensure!(
            pioneer_crud::resolve_workspace_authorization_scope(
                &self.crud_store.database_connection(),
                &scope.workspace_id
            )
            .await?
            .is_some_and(|workspace| workspace.is_active),
            "host.revoked"
        );
        if super::dev::is_dev(&parent) {
            anyhow::ensure!(
                scope.execution_id == parent.id
                    && parent.pending_json.is_none()
                    && !scope.cancel.is_cancelled(),
                "host.revoked"
            );
            return Ok(parent);
        }
        let status = self.app_runtime.status(&scope.app_id).await;
        anyhow::ensure!(
            status.execution_id.as_deref() == Some(scope.execution_id.as_str())
                && matches!(status.state.as_str(), "starting" | "ready"),
            "host.revoked"
        );
        Ok(parent)
    }
}

impl MessageProcessor {
    pub(crate) async fn app_render_source(
        &self,
        scope: &crate::host_api::ExecutionAuthority,
        path: String,
    ) -> Result<crate::browser_render::AssetSource> {
        let parent = self.validate_host_execution(scope).await?;
        let development = super::dev::is_dev(&parent);
        let (address, credential, upstream) = if development {
            let upstream = self.dev_upstream(&parent).await?;
            (upstream.address, upstream.credential, upstream.cancellation)
        } else {
            self.app_runtime
                .upstream(&scope.app_id)
                .await
                .context("host.revoked")?
        };
        anyhow::ensure!(
            !upstream.is_cancelled() && !scope.cancel.is_cancelled(),
            "host.revoked"
        );
        crate::browser_render::AssetSource::new(address, credential, path, development)
    }

    pub(crate) fn app_render_scratch(&self) -> std::path::PathBuf {
        self.artifact_runtime_home.join("app-render")
    }
}
