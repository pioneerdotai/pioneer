use super::*;
use pioneer_protocol::{McpOAuthAction, McpOAuthParams, McpOAuthResponse};
impl MessageProcessor {
    pub(crate) async fn mcp_oauth(
        &self,
        context: &RequestContext,
        request_id: RequestId,
        params: McpOAuthParams,
    ) {
        let client = context.connection_id();
        let workspace = match self
            .validate_mcp_workspace(
                client,
                request_id.clone(),
                params.workspace_id,
                methods::MCP_OAUTH,
            )
            .await
        {
            Ok(w) => w,
            Err(e) => {
                self.send_error(client, e).await;
                return;
            }
        };
        let kind = params.scope_kind.as_str();
        let key = if kind == "user" {
            "default"
        } else {
            workspace.as_str()
        };
        let row = match self
            .crud_store
            .find_mcp_server_installation(kind, key, &params.name)
            .await
        {
            Ok(Some(r)) => r,
            _ => {
                self.send_error(
                    client,
                    mcp_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        MCP_ERROR_NOT_FOUND,
                        "MCP installation unavailable",
                        json!({}),
                    ),
                )
                .await;
                return;
            }
        };
        let Some(id) = &row.id else {
            return;
        };
        if id != &params.server_id {
            self.send_error(
                client,
                mcp_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    MCP_ERROR_NOT_FOUND,
                    "MCP installation was replaced",
                    json!({}),
                ),
            )
            .await;
            return;
        }
        let installation = match crate::mcp_service::installation_from_record(&row) {
            Ok(i) => i,
            Err(_) => {
                self.send_error(
                    client,
                    mcp_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        MCP_ERROR_INTERNAL,
                        "MCP configuration unavailable",
                        json!({}),
                    ),
                )
                .await;
                return;
            }
        };
        #[cfg(test)]
        let read_barrier = self
            .mcp_service
            .inner
            .oauth_rpc_read_barrier
            .lock()
            .unwrap()
            .clone();
        #[cfg(test)]
        if let Some((entered, release)) = read_barrier {
            entered.notify_one();
            release.notified().await;
        }
        let admission = self
            .mcp_service
            .installation_lifecycle_guard(kind, key, &params.name)
            .await;
        let current = self
            .crud_store
            .find_mcp_server_installation(kind, key, &params.name)
            .await;
        let admitted = match current {
            Ok(Some(current)) if current.id == row.id => {
                crate::mcp_service::installation_from_record(&current)
                    .ok()
                    .filter(|current| {
                        pioneer_mcp_oauth::McpOAuthService::same_configuration(
                            current,
                            &installation,
                        )
                    })
                    .map(|installation| (current, installation))
            }
            _ => None,
        };
        let Some((row, installation)) = admitted else {
            self.send_error(
                client,
                mcp_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    MCP_ERROR_NOT_FOUND,
                    "MCP installation was replaced",
                    json!({}),
                ),
            )
            .await;
            return;
        };
        let Some(id) = row.id.as_ref() else {
            return;
        };
        let service = self.mcp_service.oauth();
        // Short operations only use an already-bound operation. They never
        // restore/rebind credentials or wait on the owned exchange task.
        if matches!(
            &params.action,
            McpOAuthAction::Callback { .. } | McpOAuthAction::Cancel { .. }
        ) && !service.bound_to_installation(id, &installation).await
        {
            self.send_error(
                client,
                mcp_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    MCP_ERROR_INVALID_REQUEST,
                    "OAuth operation is no longer current",
                    json!({}),
                ),
            )
            .await;
            return;
        }
        let result = match params.action {
            McpOAuthAction::SignIn { redirect_uri } => {
                service
                    .sign_in_in_workspace(id, &installation, client, &redirect_uri, &workspace)
                    .await
            }
            McpOAuthAction::Disconnect => {
                let result = service
                    .disconnect_managed(id, &installation, client, &workspace)
                    .await;
                if result.is_ok() {
                    self.mcp_service.stop_oauth_connection(&row).await;
                }
                result
            }
            McpOAuthAction::Cancel { flow_id } => service.cancel(id, client, &flow_id).await,
            McpOAuthAction::Callback {
                flow_id,
                state,
                code,
                issuer,
                error,
            } => {
                service
                    .callback(
                        id,
                        client,
                        pioneer_mcp_oauth::OAuthCallback {
                            flow_id,
                            state: state.expose_secret().into(),
                            code: code.map(|s| s.expose_secret().into()),
                            issuer,
                            error,
                        },
                    )
                    .await
            }
        };
        drop(admission);
        match result {
            Ok(()) => {
                if let Ok(response) =
                    JsonRpcResponse::from_result(request_id, &McpOAuthResponse { accepted: true })
                {
                    let _ = self.send_json(client, &response).await;
                }
            }
            Err(error) => {
                self.send_error(
                    client,
                    mcp_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        MCP_ERROR_INVALID_REQUEST,
                        error.message,
                        json!({}),
                    ),
                )
                .await
            }
        }
    }
}
