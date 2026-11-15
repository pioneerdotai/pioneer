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
    ) -> Result<()> {
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
        let status = self.app_runtime.status(&scope.app_id).await;
        anyhow::ensure!(
            status.execution_id.as_deref() == Some(scope.execution_id.as_str())
                && matches!(status.state.as_str(), "starting" | "ready"),
            "host.revoked"
        );
        Ok(())
    }
}
