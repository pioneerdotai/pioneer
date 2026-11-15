//! App UI admission. Opening a view observes an execution; it never starts one.
use super::super::*;
use crate::view_grants::{
    AppViewGrantScope, ViewGrantDisposition, ViewGrantScope, ViewGrantSubject,
};
use anyhow::{Context, Result};
impl MessageProcessor {
    pub(crate) async fn app_ui_endpoint(
        &self,
        context: &crate::request_context::AuthenticatedRequestContext,
        scope: &AppViewGrantScope,
    ) -> Result<(
        std::net::SocketAddr,
        pioneer_protocol::AuthSecretString,
        tokio_util::sync::CancellationToken,
    )> {
        self.admit_plugin_image(context, &scope.workspace_id)
            .await?;
        let parent = self
            .crud_store
            .find_package_installation(&scope.app_id)
            .await?
            .context("apps.unavailable")?;
        anyhow::ensure!(
            parent.kind == pioneer_protocol::PackageKind::App
                && parent.workspace_id == scope.workspace_id
                && parent.enabled
                && parent.state == "installed",
            "apps.unavailable"
        );
        let status = self.app_view_status(&scope.app_id).await;
        anyhow::ensure!(
            status.state == "ready"
                && status.execution_id.as_deref() == Some(scope.execution_id.as_str()),
            "apps.execution_unavailable"
        );
        // Dev grants control the native view lease only. They cannot be used
        // to serve a dev page or its resources through the release proxy.
        anyhow::ensure!(!super::dev::is_dev(&parent), "apps.dev_direct_view_only");
        self.app_runtime
            .upstream(&scope.app_id)
            .await
            .context("apps.execution_unavailable")
    }
    pub(super) async fn app_open_view(&self, context: &RequestContext, request: JsonRpcRequest) {
        let id = request.id;
        let result: Result<pioneer_protocol::AppViewBootstrap> = async {
            let params: pioneer_protocol::AppViewParams =
                serde_json::from_value(request.params.context("apps.invalid_request")?)?;
            let parent = self
                .crud_store
                .find_package_installation(&params.app_id)
                .await?
                .context("apps.not_found")?;
            anyhow::ensure!(
                parent.kind == pioneer_protocol::PackageKind::App
                    && parent.workspace_id == params.workspace_id
                    && parent.enabled
                    && parent.state == "installed",
                "apps.unavailable"
            );
            let manifest = pioneer_apps::AppManifest::parse(
                parent
                    .app_manifest_json
                    .as_deref()
                    .context("apps.invalid_manifest")?
                    .as_bytes(),
            )?;
            let ui = manifest.ui().context("apps.no_ui")?;
            let status = self.app_view_status(&params.app_id).await;
            anyhow::ensure!(status.state == "ready", "apps.not_ready");
            let execution_id = status.execution_id.context("apps.not_ready")?;
            let service = self.view_grant_service().context("apps.view_unavailable")?;
            let principal = context.principal();
            let development = super::dev::is_dev(&parent);
            let dev_url = if development {
                Some(super::dev::view_url(&parent)?)
            } else {
                None
            };
            let issued = service
                .mint(ViewGrantScope {
                    gateway_id: principal.gateway_id.clone(),
                    principal_id: principal.principal_id.clone(),
                    auth_session_id: principal.session_id.clone(),
                    disposition: ViewGrantDisposition::Inline,
                    subject: ViewGrantSubject::App(AppViewGrantScope {
                        workspace_id: parent.workspace_id,
                        app_id: parent.id,
                        execution_id: execution_id.clone(),
                    }),
                })
                .map_err(|_| anyhow::anyhow!("apps.view_unavailable"))?;
            let base = issued.secret.into_relative_url().replacen(
                "/storage/views/",
                "/storage/app-views/",
                1,
            );
            Ok(pioneer_protocol::AppViewBootstrap {
                development,
                dev_url,
                package_revision: parent.revision,
                relative_url: pioneer_protocol::AuthSecretString::new(format!("{base}{}", ui.path)),
                expires_at: issued.expires_at_unix,
                execution_id,
                title: manifest.presentation("", &Default::default()).name,
            })
        }
        .await;
        match result {
            Ok(value) => {
                if let Ok(response) =
                    pioneer_protocol::JsonRpcResponse::from_result(id.clone(), &value)
                {
                    let _ = self.send_json(context.connection_id(), &response).await;
                }
            }
            Err(error) if error.to_string() == "apps.view_denied" => {
                self.send_error(
                    context.connection_id(),
                    pioneer_protocol::JsonRpcErrorResponse::new(
                        Some(id.clone()),
                        pioneer_protocol::FORBIDDEN_CODE,
                        "apps.view_denied",
                    ),
                )
                .await
            }
            Err(_) => {
                self.send_error(
                    context.connection_id(),
                    super::super::packages::plugin_error(&id, "apps.view_unavailable"),
                )
                .await
            }
        }
    }
}

impl MessageProcessor {
    pub(super) async fn app_runtime_info(&self, context: &RequestContext, request: JsonRpcRequest) {
        let id = request.id;
        let result: Result<pioneer_protocol::AppRuntimeInfo> = async {
            let params: pioneer_protocol::AppViewParams =
                serde_json::from_value(request.params.context("apps.invalid_request")?)?;
            let parent = self
                .crud_store
                .find_package_installation(&params.app_id)
                .await?
                .filter(|p| {
                    p.workspace_id == params.workspace_id
                        && p.kind == pioneer_protocol::PackageKind::App
                })
                .context("apps.not_found")?;
            let status = self.app_view_status(&parent.id).await;
            let logs = if context.principal().kind == pioneer_protocol::PrincipalKind::Superuser {
                self.app_runtime.logs(&parent.id).await
            } else {
                Vec::new()
            };
            Ok(pioneer_protocol::AppRuntimeInfo {
                state: status.state,
                execution_id: status.execution_id,
                diagnostic: status.failure,
                logs,
            })
        }
        .await;
        match result {
            Ok(info) => {
                if let Ok(response) =
                    pioneer_protocol::JsonRpcResponse::from_result(id.clone(), &info)
                {
                    let _ = self.send_json(context.connection_id(), &response).await;
                }
            }
            Err(_) => {
                self.send_error(
                    context.connection_id(),
                    super::super::packages::plugin_error(&id, "apps.unavailable"),
                )
                .await
            }
        }
    }
}

impl MessageProcessor {
    pub(super) async fn app_view_lease(&self, context: &RequestContext, request: JsonRpcRequest) {
        let close = request.method == pioneer_protocol::constants::methods::APPS_CLOSE_VIEW;
        let id = request.id;
        let result: Result<pioneer_protocol::AppViewLeaseResponse> = async {
            let params: pioneer_protocol::AppViewLeaseParams =
                serde_json::from_value(request.params.context("apps.invalid_request")?)?;
            let presented = params
                .relative_url
                .expose_secret()
                .strip_prefix("/storage/app-views/")
                .and_then(|tail| tail.split('/').next())
                .filter(|s| s.len() == 43)
                .context("apps.invalid_view")?;
            let service = self.view_grant_service().context("apps.unavailable")?;
            let resolved_scope = service
                .app_scope(presented)
                .map_err(|_| anyhow::anyhow!("apps.invalid_view"))?;
            let principal = context.principal();
            let scope = &resolved_scope;
            anyhow::ensure!(
                scope.gateway_id == principal.gateway_id
                    && scope.principal_id == principal.principal_id
                    && scope.auth_session_id == principal.session_id,
                "apps.invalid_view"
            );
            let ViewGrantSubject::App(app) = &scope.subject else {
                anyhow::bail!("apps.invalid_view");
            };
            if close {
                service
                    .close_app(presented, scope)
                    .map_err(|_| anyhow::anyhow!("apps.invalid_view"))?;
                return Ok(pioneer_protocol::AppViewLeaseResponse { expires_at: 0 });
            }
            let check = pioneer_protocol::JsonRpcRequest {
                jsonrpc: "2.0".into(),
                id: id.clone(),
                method: pioneer_protocol::constants::methods::APPS_DETAILS.into(),
                params: Some(
                    serde_json::json!({"workspace_id":app.workspace_id,"app_id":app.app_id}),
                ),
            };
            self.authorize_normal_request(context, &check)
                .await
                .map_err(|_| anyhow::anyhow!("apps.invalid_view"))?;
            let parent = self
                .crud_store
                .find_package_installation(&app.app_id)
                .await?
                .context("apps.unavailable")?;
            anyhow::ensure!(
                parent.enabled
                    && parent.state == "installed"
                    && parent.workspace_id == app.workspace_id,
                "apps.unavailable"
            );
            let status = self.app_view_status(&app.app_id).await;
            anyhow::ensure!(
                status.execution_id.as_deref() == Some(app.execution_id.as_str())
                    && status.state == "ready",
                "apps.unavailable"
            );
            let expires_at = service
                .renew_app(presented, scope)
                .map_err(|_| anyhow::anyhow!("apps.invalid_view"))?;
            Ok(pioneer_protocol::AppViewLeaseResponse { expires_at })
        }
        .await;
        match result {
            Ok(value) => {
                if let Ok(response) =
                    pioneer_protocol::JsonRpcResponse::from_result(id.clone(), &value)
                {
                    let _ = self.send_json(context.connection_id(), &response).await;
                }
            }
            Err(_) => {
                self.send_error(
                    context.connection_id(),
                    super::super::packages::plugin_error(&id, "apps.view_unavailable"),
                )
                .await
            }
        }
    }
}
