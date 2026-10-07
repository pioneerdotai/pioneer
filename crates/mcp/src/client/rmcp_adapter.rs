use crate::catalog::McpCatalogSnapshot;
use crate::client::stdio::build_stdio_transport;
use crate::client::streamable_http::build_streamable_http_transport;
use crate::domain::McpServerInstallation;
use crate::redaction::redact_text;
use crate::runtime::{
    MaterializedTransport, McpRuntimeConnector, McpRuntimeError, McpRuntimeSession,
    McpSecretResolver, McpSessionEvent, McpToolCallResult, materialize_transport,
};
use async_trait::async_trait;
use rmcp::model::{
    CallToolRequest, CallToolRequestParams, CancelledNotificationParam, ClientRequest, ErrorCode,
    JsonObject, ServerResult,
};
use rmcp::service::{NotificationContext, PeerRequestOptions, RunningService, ServiceError};
use rmcp::{ClientHandler, RoleClient, ServiceExt};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const MCP_CANCELLATION_NOTIFICATION_GRACE: Duration = Duration::from_millis(250);

#[derive(Default)]
pub struct RmcpRuntimeConnector {
    oauth: Option<Arc<dyn crate::McpOAuthProvider>>,
}

impl RmcpRuntimeConnector {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_oauth(oauth: Arc<dyn crate::McpOAuthProvider>) -> Self {
        Self { oauth: Some(oauth) }
    }
}

#[async_trait]
impl McpRuntimeConnector for RmcpRuntimeConnector {
    async fn connect(
        &self,
        installation: McpServerInstallation,
        installation_id: String,
        resolver: Arc<dyn McpSecretResolver>,
        now_unix: i64,
    ) -> Result<Box<dyn McpRuntimeSession>, McpRuntimeError> {
        let materialized = materialize_transport(&installation.transport, resolver.as_ref())?;
        match materialized {
            MaterializedTransport::Stdio(transport) => {
                let secrets = transport.secrets.clone();
                let startup_timeout = Duration::from_millis(transport.startup_timeout_ms.max(1));
                let tool_timeout = Duration::from_millis(transport.tool_timeout_ms.max(1));
                let (transport, stderr_tail, child) =
                    build_stdio_transport(&transport).map_err(|error| {
                        McpRuntimeError::failed(redact_text(
                            format!("failed to spawn stdio MCP server: {error:#}").as_str(),
                            secrets.as_slice(),
                        ))
                    })?;
                let (event_tx, event_rx) = mpsc::unbounded_channel();
                let handler = RuntimeClientHandler { event_tx };
                let mut client =
                    match tokio::time::timeout(startup_timeout, handler.serve(transport)).await {
                        Ok(Ok(client)) => client,
                        result => {
                            let error = match result {
                                Err(_) => McpRuntimeError::failed("stdio MCP initialize timed out"),
                                Ok(Err(error)) => McpRuntimeError::failed(redact_text(
                                    format!("stdio MCP initialize failed: {error:#}").as_str(),
                                    secrets.as_slice(),
                                )),
                                Ok(Ok(_)) => unreachable!(),
                            };
                            if child.stop_and_wait().await.is_err() {
                                return Ok(Box::new(RmcpRuntimeSession::failed_startup(
                                    child,
                                    None,
                                    event_rx,
                                    installation_id,
                                    secrets,
                                    tool_timeout,
                                    error,
                                )));
                            }
                            return Err(error);
                        }
                    };
                let collected = match collect_catalog(
                    &client,
                    installation_id.clone(),
                    now_unix,
                    tool_timeout,
                    secrets.as_slice(),
                )
                .await
                {
                    Ok(catalog) => catalog,
                    Err(error) => {
                        let stderr = stderr_tail.snapshot().await;
                        let message = if stderr.trim().is_empty() {
                            error.message
                        } else {
                            format!("{}; stderr: {}", error.message, stderr)
                        };
                        let close_failed = client.close().await.is_err();
                        let child_failed = child.stop_and_wait().await.is_err();
                        let error = McpRuntimeError {
                            kind: error.kind,
                            state: error.state,
                            oauth_failure: error.oauth_failure,
                            message,
                        };
                        if close_failed || child_failed {
                            return Ok(Box::new(RmcpRuntimeSession::failed_startup(
                                child,
                                Some(client),
                                event_rx,
                                installation_id,
                                secrets,
                                tool_timeout,
                                error,
                            )));
                        }
                        return Err(error);
                    }
                };
                let CollectedCatalog {
                    catalog,
                    degraded_reason,
                } = collected;

                Ok(Box::new(RmcpRuntimeSession {
                    startup_failure: None,
                    shutdown_outcome: None,
                    child: Some(child),
                    client: Some(client),
                    event_rx,
                    catalog,
                    degraded_reason,
                    secrets,
                    tool_timeout,
                }))
            }
            MaterializedTransport::StreamableHttp(transport) => {
                let secrets = transport.secrets.clone();
                let startup_timeout = Duration::from_millis(transport.startup_timeout_ms.max(1));
                let tool_timeout = Duration::from_millis(transport.tool_timeout_ms.max(1));
                let authorized = match &self.oauth {
                    Some(owner) => owner.client(&installation_id, &installation).await?,
                    None => None,
                };
                let had_authorization = authorized.is_some();
                let http = crate::oauth::ManagedHttpClient {
                    // Portable configured headers cannot follow a redirect to
                    // another origin. The OAuth client already disables redirects.
                    plain: {
                        let builder = reqwest_0_13::Client::builder();
                        let builder = if installation.is_portable_plugin() {
                            builder.redirect(reqwest_0_13::redirect::Policy::none())
                        } else {
                            builder
                        };
                        builder.build()
                    }
                    .map_err(|_| McpRuntimeError::failed("HTTP client initialization failed"))?,
                    authorized,
                    owner: if installation.explicit_authorization_overrides_oauth() {
                        None
                    } else {
                        self.oauth.clone()
                    },
                    id: installation_id.clone(),
                    installation: installation.clone(),
                };
                let transport = build_streamable_http_transport(&transport, http)?;
                let (event_tx, event_rx) = mpsc::unbounded_channel();
                let handler = RuntimeClientHandler { event_tx };
                let mut client = tokio::time::timeout(startup_timeout, handler.serve(transport))
                    .await
                    .map_err(|_| {
                        McpRuntimeError::failed("Streamable HTTP MCP initialize timed out")
                    })?
                    .map_err(|error| {
                        classify_runtime_error(
                            "Streamable HTTP MCP initialize failed",
                            &error,
                            secrets.as_slice(),
                        )
                    })?;

                let collected = match collect_catalog(
                    &client,
                    installation_id.clone(),
                    now_unix,
                    tool_timeout,
                    secrets.as_slice(),
                )
                .await
                {
                    Ok(catalog) => catalog,
                    Err(error) => {
                        let _ = client.close_with_timeout(Duration::from_secs(3)).await;
                        return Err(error);
                    }
                };
                let CollectedCatalog {
                    catalog,
                    degraded_reason,
                } = collected;

                if let Some(owner) = &self.oauth {
                    owner
                        .connection_established(&installation_id, &installation, had_authorization)
                        .await;
                }
                Ok(Box::new(RmcpRuntimeSession {
                    startup_failure: None,
                    shutdown_outcome: None,
                    child: None,
                    client: Some(client),
                    event_rx,
                    catalog,
                    degraded_reason,
                    secrets,
                    tool_timeout,
                }))
            }
        }
    }
}

#[derive(Clone)]
struct RuntimeClientHandler {
    event_tx: mpsc::UnboundedSender<McpSessionEvent>,
}

impl ClientHandler for RuntimeClientHandler {
    fn on_resource_list_changed(
        &self,
        _context: NotificationContext<RoleClient>,
    ) -> impl Future<Output = ()> + rmcp::service::MaybeSendFuture + '_ {
        let event_tx = self.event_tx.clone();
        async move {
            let _ = event_tx.send(McpSessionEvent::CatalogChanged);
        }
    }

    fn on_tool_list_changed(
        &self,
        _context: NotificationContext<RoleClient>,
    ) -> impl Future<Output = ()> + rmcp::service::MaybeSendFuture + '_ {
        let event_tx = self.event_tx.clone();
        async move {
            let _ = event_tx.send(McpSessionEvent::CatalogChanged);
        }
    }

    fn on_prompt_list_changed(
        &self,
        _context: NotificationContext<RoleClient>,
    ) -> impl Future<Output = ()> + rmcp::service::MaybeSendFuture + '_ {
        let event_tx = self.event_tx.clone();
        async move {
            let _ = event_tx.send(McpSessionEvent::CatalogChanged);
        }
    }
}

struct RmcpRuntimeSession {
    child: Option<Arc<crate::client::stdio::StdioChildOwner>>,
    shutdown_outcome: Option<Result<(), McpRuntimeError>>,
    startup_failure: Option<McpRuntimeError>,
    client: Option<RunningService<RoleClient, RuntimeClientHandler>>,
    event_rx: mpsc::UnboundedReceiver<McpSessionEvent>,
    catalog: McpCatalogSnapshot,
    degraded_reason: Option<String>,
    secrets: Vec<String>,
    tool_timeout: Duration,
}

impl RmcpRuntimeSession {
    fn failed_startup(
        child: Arc<crate::client::stdio::StdioChildOwner>,
        client: Option<RunningService<RoleClient, RuntimeClientHandler>>,
        event_rx: mpsc::UnboundedReceiver<McpSessionEvent>,
        id: String,
        secrets: Vec<String>,
        tool_timeout: Duration,
        error: McpRuntimeError,
    ) -> Self {
        Self {
            child: Some(child),
            client,
            event_rx,
            secrets,
            tool_timeout,
            shutdown_outcome: None,
            startup_failure: Some(error),
            degraded_reason: None,
            catalog: McpCatalogSnapshot {
                server_installation_id: id,
                catalog_version: String::new(),
                server_info_json: "{}".into(),
                server_instructions_hash: None,
                tools_json: "[]".into(),
                resources_json: "[]".into(),
                resource_templates_json: "[]".into(),
                prompts_json: "[]".into(),
                generated_at_unix: 0,
            },
        }
    }
}

#[async_trait]
impl McpRuntimeSession for RmcpRuntimeSession {
    fn startup_failure(&self) -> Option<&McpRuntimeError> {
        self.startup_failure.as_ref()
    }
    fn initial_catalog(&self) -> &McpCatalogSnapshot {
        &self.catalog
    }

    fn degraded_reason(&self) -> Option<&str> {
        self.degraded_reason.as_deref()
    }

    async fn wait_for_event(&mut self) -> McpSessionEvent {
        let Some(client) = self.client.as_ref() else {
            return McpSessionEvent::Closed;
        };
        loop {
            if client.is_closed() || client.peer().is_transport_closed() {
                return McpSessionEvent::Closed;
            }
            tokio::select! {
                event = self.event_rx.recv() => return event.unwrap_or(McpSessionEvent::Closed),
                _ = tokio::time::sleep(Duration::from_secs(1)) => {},
            }
        }
    }

    async fn refresh_catalog(&mut self) -> Result<McpCatalogSnapshot, McpRuntimeError> {
        let Some(client) = self.client.as_ref() else {
            return Err(McpRuntimeError::failed("MCP session is closed"));
        };
        let collected = collect_catalog(
            client,
            self.catalog.server_installation_id.clone(),
            unix_timestamp_secs(),
            self.tool_timeout,
            self.secrets.as_slice(),
        )
        .await?;
        self.degraded_reason = collected.degraded_reason;
        self.catalog = collected.catalog.clone();
        Ok(collected.catalog)
    }

    async fn call_tool(
        &mut self,
        raw_tool_name: &str,
        arguments: serde_json::Value,
        budget: crate::McpInvocationBudget,
        timeout: Duration,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<McpToolCallResult, McpRuntimeError> {
        let Some(client) = self.client.as_ref() else {
            return Err(McpRuntimeError::failed("MCP session is closed"));
        };
        if cancellation.is_cancelled() {
            return Err(McpRuntimeError::cancelled(
                "MCP tools/call cancelled before transport dispatch",
            ));
        }
        crate::validate_mcp_arguments(&arguments, budget).map_err(|error| {
            McpRuntimeError::failed(format!(
                "MCP tools/call rejected before transport dispatch: {}",
                error.reason_code()
            ))
        })?;
        let arguments = match arguments {
            serde_json::Value::Object(map) => map,
            serde_json::Value::Null => JsonObject::new(),
            other => {
                return Err(McpRuntimeError::failed(format!(
                    "MCP tools/call arguments for `{raw_tool_name}` must be a JSON object, got {other}"
                )));
            }
        };

        let started = Instant::now();
        let effective_timeout = self.tool_timeout.min(timeout).max(Duration::from_millis(1));
        let peer = client.peer();
        let handle = peer
            .send_cancellable_request(
                ClientRequest::CallToolRequest(CallToolRequest::new(
                    CallToolRequestParams::new(raw_tool_name.to_owned()).with_arguments(arguments),
                )),
                PeerRequestOptions::no_options(),
            )
            .await
            .map_err(|error| {
                classify_runtime_error(
                    format!("MCP tools/call `{raw_tool_name}` dispatch failed").as_str(),
                    &error,
                    self.secrets.as_slice(),
                )
            })?;
        let request_id = handle.id.clone();
        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                let notification = peer.notify_cancelled(CancelledNotificationParam::new(
                        Some(request_id),
                        Some("Pioneer MCP invocation cancelled".to_owned()),
                    ));
                if !matches!(
                    tokio::time::timeout(MCP_CANCELLATION_NOTIFICATION_GRACE, notification).await,
                    Ok(Ok(()))
                ) {
                    client.cancellation_token().cancel();
                }
                return Err(McpRuntimeError::cancelled(
                    "MCP tools/call cancelled while awaiting the server",
                ));
            }
            _ = tokio::time::sleep(effective_timeout) => {
                let notification = peer.notify_cancelled(CancelledNotificationParam::new(
                    Some(request_id),
                    Some("Pioneer MCP invocation timed out".to_owned()),
                ));
                if !matches!(
                    tokio::time::timeout(MCP_CANCELLATION_NOTIFICATION_GRACE, notification).await,
                    Ok(Ok(()))
                ) {
                    client.cancellation_token().cancel();
                }
                return Err(McpRuntimeError::timed_out(format!(
                    "MCP tools/call `{raw_tool_name}` timed out"
                )));
            }
            response = handle.await_response() => response,
        }
        .map_err(|error| {
            classify_runtime_error(
                format!("MCP tools/call `{raw_tool_name}` failed").as_str(),
                &error,
                self.secrets.as_slice(),
            )
        })?;
        let result = match response {
            ServerResult::CallToolResult(result) => result,
            _ => {
                return Err(McpRuntimeError::failed(format!(
                    "MCP tools/call `{raw_tool_name}` returned an unexpected response"
                )));
            }
        };

        let content = serde_json::to_value(result.content).map_err(|error| {
            McpRuntimeError::failed(format!("failed to encode MCP tool content: {error}"))
        })?;
        let meta = result
            .meta
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| {
                McpRuntimeError::failed(format!("failed to encode MCP tool metadata: {error}"))
            })?;

        Ok(McpToolCallResult {
            content,
            structured_content: result.structured_content,
            is_error: result.is_error.unwrap_or(false),
            duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            meta,
        })
    }

    async fn shutdown(&mut self) {
        let _ = self.shutdown_result().await;
    }
    async fn shutdown_result(&mut self) -> Result<(), McpRuntimeError> {
        if let Some(result) = &self.shutdown_outcome {
            if result.is_err()
                && let Some(child) = &self.child
            {
                let _ = child.stop_and_wait().await;
            }
            return result.clone();
        }
        let mut outcome = Ok(());
        if let Some(client) = self.client.as_mut() {
            // Owned by the Gateway actor, which is not aborted by a stop waiter.
            // No nested timeout that discards rmcp's cleanup JoinHandle.
            if client.close().await.is_err() {
                outcome = Err(McpRuntimeError::failed("MCP service cleanup join failed"));
            }
        }
        if let Some(child) = &self.child {
            if child.stop_and_wait().await.is_err() {
                outcome = Err(McpRuntimeError::failed("MCP stdio process cleanup failed"));
            }
        }
        self.shutdown_outcome = Some(outcome.clone());
        outcome
    }
}

fn unix_timestamp_secs() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

struct CollectedCatalog {
    catalog: McpCatalogSnapshot,
    degraded_reason: Option<String>,
}

async fn collect_catalog(
    client: &RunningService<RoleClient, RuntimeClientHandler>,
    installation_id: String,
    generated_at_unix: i64,
    tool_timeout: Duration,
    secrets: &[String],
) -> Result<CollectedCatalog, McpRuntimeError> {
    let peer = client.peer();
    let mut optional_errors = Vec::new();
    let peer_info = peer.peer_info();
    let server_info = peer_info
        .as_deref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|error| McpRuntimeError::failed(format!("failed to encode server info: {error}")))?
        .unwrap_or_else(|| serde_json::json!({}));
    let instructions = peer_info
        .as_ref()
        .and_then(|info| info.instructions.as_deref());

    let tools = tokio::time::timeout(tool_timeout, peer.list_all_tools())
        .await
        .map_err(|_| McpRuntimeError::failed("MCP tools/list timed out"))?
        .map_err(|error| classify_runtime_error("MCP tools/list failed", &error, secrets))?;

    let supports_resources = peer_info
        .as_ref()
        .is_none_or(|info| info.capabilities.resources.is_some());
    let supports_prompts = peer_info
        .as_ref()
        .is_none_or(|info| info.capabilities.prompts.is_some());

    let resources = if supports_resources {
        match tokio::time::timeout(tool_timeout, peer.list_all_resources()).await {
            Ok(Ok(resources)) => resources,
            Ok(Err(error)) => {
                let classified =
                    classify_runtime_error("MCP catalog authorization failed", &error, secrets);
                if classified.state == crate::McpRuntimeState::AuthRequired
                    || matches!(
                        classified.kind,
                        crate::McpRuntimeErrorKind::CredentialStore
                            | crate::McpRuntimeErrorKind::TransientRefresh
                    )
                {
                    return Err(classified);
                }
                if let Some(message) = optional_catalog_error("resources/list", &error, secrets) {
                    optional_errors.push(message);
                }
                Vec::new()
            }
            Err(_) => {
                tracing::warn!("MCP resources/list timed out");
                optional_errors.push("resources/list timed out".to_owned());
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let resource_templates = if supports_resources {
        match tokio::time::timeout(tool_timeout, peer.list_all_resource_templates()).await {
            Ok(Ok(resource_templates)) => resource_templates,
            Ok(Err(error)) => {
                let classified =
                    classify_runtime_error("MCP catalog authorization failed", &error, secrets);
                if classified.state == crate::McpRuntimeState::AuthRequired
                    || matches!(
                        classified.kind,
                        crate::McpRuntimeErrorKind::CredentialStore
                            | crate::McpRuntimeErrorKind::TransientRefresh
                    )
                {
                    return Err(classified);
                }
                if let Some(message) =
                    optional_catalog_error("resources/templates/list", &error, secrets)
                {
                    optional_errors.push(message);
                }
                Vec::new()
            }
            Err(_) => {
                tracing::warn!("MCP resources/templates/list timed out");
                optional_errors.push("resources/templates/list timed out".to_owned());
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let prompts = if supports_prompts {
        match tokio::time::timeout(tool_timeout, peer.list_all_prompts()).await {
            Ok(Ok(prompts)) => prompts,
            Ok(Err(error)) => {
                let classified =
                    classify_runtime_error("MCP catalog authorization failed", &error, secrets);
                if classified.state == crate::McpRuntimeState::AuthRequired
                    || matches!(
                        classified.kind,
                        crate::McpRuntimeErrorKind::CredentialStore
                            | crate::McpRuntimeErrorKind::TransientRefresh
                    )
                {
                    return Err(classified);
                }
                if let Some(message) = optional_catalog_error("prompts/list", &error, secrets) {
                    optional_errors.push(message);
                }
                Vec::new()
            }
            Err(_) => {
                tracing::warn!("MCP prompts/list timed out");
                optional_errors.push("prompts/list timed out".to_owned());
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let catalog = McpCatalogSnapshot::from_json_values(
        installation_id,
        server_info,
        instructions,
        serde_json::to_value(tools)
            .map_err(|error| McpRuntimeError::failed(format!("failed to encode tools: {error}")))?,
        serde_json::to_value(resources).map_err(|error| {
            McpRuntimeError::failed(format!("failed to encode resources: {error}"))
        })?,
        serde_json::to_value(resource_templates).map_err(|error| {
            McpRuntimeError::failed(format!("failed to encode resource templates: {error}"))
        })?,
        serde_json::to_value(prompts).map_err(|error| {
            McpRuntimeError::failed(format!("failed to encode prompts: {error}"))
        })?,
        generated_at_unix,
    )
    .map_err(|error| McpRuntimeError::failed(format!("{error:#}")))?;

    Ok(CollectedCatalog {
        catalog,
        degraded_reason: (!optional_errors.is_empty()).then(|| optional_errors.join("; ")),
    })
}

fn optional_catalog_error(
    method: &'static str,
    error: &ServiceError,
    secrets: &[String],
) -> Option<String> {
    if is_method_not_found(error) {
        tracing::debug!(method, "MCP optional catalog method is not supported");
        return None;
    }

    let message = redact_text(format!("{error:#}").as_str(), secrets);
    tracing::warn!(method, error = %message, "MCP optional catalog method failed");
    Some(format!("{method} failed: {message}"))
}

fn is_method_not_found(error: &ServiceError) -> bool {
    matches!(
        error,
        ServiceError::McpError(data) if data.code == ErrorCode::METHOD_NOT_FOUND
    )
}

fn classify_runtime_error<E: std::error::Error + 'static>(
    context: &str,
    error: &E,
    secrets: &[String],
) -> McpRuntimeError {
    use rmcp::transport::{
        auth::AuthError,
        streamable_http_client::{AuthRequiredError, InsufficientScopeError},
    };
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(error) = source {
        if let Some(crate::oauth::ManagedHttpError::OAuth(runtime)) =
            error.downcast_ref::<crate::oauth::ManagedHttpError>()
        {
            return runtime.clone();
        }
        if let Some(crate::oauth::ManagedHttpError::Forbidden) =
            error.downcast_ref::<crate::oauth::ManagedHttpError>()
        {
            let mut error = McpRuntimeError::failed("MCP server denied access (403)");
            error.kind = crate::McpRuntimeErrorKind::Forbidden;
            return error;
        }
        if let Some(auth) = error.downcast_ref::<AuthError>() {
            return crate::oauth_runtime_error(auth);
        }
        if error.is::<AuthRequiredError>() {
            return McpRuntimeError::auth_required("OAuth sign-in required");
        }
        if error.is::<InsufficientScopeError>() {
            let mut error =
                McpRuntimeError::auth_required("OAuth consent required for additional permissions");
            error.kind = crate::McpRuntimeErrorKind::InsufficientScope;
            return error;
        }
        source = if let Some(http) = error
            .downcast_ref::<rmcp::transport::streamable_http_client::StreamableHttpError<
            crate::oauth::ManagedHttpError,
        >>() {
            match http {
                rmcp::transport::streamable_http_client::StreamableHttpError::Client(e) => Some(e),
                _ => error.source(),
            }
        } else if let Some(init) = error.downcast_ref::<rmcp::service::ClientInitializeError>() {
            match init {
                rmcp::service::ClientInitializeError::TransportError { error, .. } => {
                    Some(error.error.as_ref())
                }
                rmcp::service::ClientInitializeError::LegacyFallbackFailed { fallback, .. } => {
                    Some(fallback.as_ref())
                }
                _ => error.source(),
            }
        } else if let Some(ServiceError::TransportSend(transport)) =
            error.downcast_ref::<ServiceError>()
        {
            Some(transport.error.as_ref())
        } else {
            error.source()
        };
    }
    McpRuntimeError::failed(redact_text(&format!("{context}: {error}"), secrets))
}

#[cfg(test)]
fn extract_http_response_message(message: &str) -> Option<String> {
    let start = http_status_start(message)?;
    let tail = &message[start..];
    let end = ["\\n", "\n", "\\r", "\r", "\"", ",", ")"]
        .iter()
        .filter_map(|needle| tail.find(needle))
        .min()
        .unwrap_or(tail.len());
    let http = tail[..end].trim();
    (!http.is_empty()).then(|| http.to_owned())
}

#[cfg(test)]
fn http_status_start(message: &str) -> Option<usize> {
    message
        .match_indices("HTTP ")
        .find(|(index, _)| {
            message[*index + "HTTP ".len()..]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_digit())
        })
        .map(|(index, _)| index)
}

#[cfg(test)]
mod tests {
    #[test]
    fn oauth_failure_cause_survives_sdk_transport_error_classification() {
        let mut expected = super::McpRuntimeError::failed("OAuth provider temporarily unavailable");
        expected.kind = crate::McpRuntimeErrorKind::TransientRefresh;
        expected.oauth_failure = Some(crate::OAuthFailureCause {
            generation: "manager-A".into(),
            revision: 7,
        });
        let error = rmcp::transport::streamable_http_client::StreamableHttpError::<
            crate::oauth::ManagedHttpError,
        >::Client(crate::oauth::ManagedHttpError::OAuth(expected.clone()));
        assert_eq!(
            super::classify_runtime_error("MCP call failed", &error, &[]),
            expected
        );
    }

    use super::{extract_http_response_message, http_status_start};

    #[test]
    #[cfg(test)]
    fn http_status_start_ignores_transport_prefix() {
        let message = "Streamable HTTP MCP initialize failed: HTTP 403 Forbidden";
        let start = http_status_start(message).expect("HTTP status start");

        assert_eq!(&message[start..], "HTTP 403 Forbidden");
    }

    #[test]
    fn extracts_http_response_from_streamable_transport_debug() {
        let message = r#"Streamable HTTP MCP initialize failed: TransportError {
    error: DynamicTransportError {
        error: UnexpectedServerResponse(
            "HTTP 403 Forbidden: forbidden: access denied\n",
        ),
    },
}"#;

        assert_eq!(
            extract_http_response_message(message).as_deref(),
            Some("HTTP 403 Forbidden: forbidden: access denied")
        );
    }
}
