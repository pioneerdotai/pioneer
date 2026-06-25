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
use rmcp::model::{CallToolRequestParams, ErrorCode, JsonObject};
use rmcp::service::{NotificationContext, RunningService, ServiceError};
use rmcp::{ClientHandler, RoleClient, ServiceExt};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[derive(Debug, Default)]
pub struct RmcpRuntimeConnector;

impl RmcpRuntimeConnector {
    pub fn new() -> Self {
        Self
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
                let (transport, stderr_tail) =
                    build_stdio_transport(&transport).map_err(|error| {
                        McpRuntimeError::failed(redact_text(
                            format!("failed to spawn stdio MCP server: {error:#}").as_str(),
                            secrets.as_slice(),
                        ))
                    })?;
                let (event_tx, event_rx) = mpsc::unbounded_channel();
                let handler = RuntimeClientHandler { event_tx };
                let mut client = tokio::time::timeout(startup_timeout, handler.serve(transport))
                    .await
                    .map_err(|_| {
                        McpRuntimeError::failed("stdio MCP initialize timed out".to_owned())
                    })?
                    .map_err(|error| {
                        McpRuntimeError::failed(redact_text(
                            format!("stdio MCP initialize failed: {error:#}").as_str(),
                            secrets.as_slice(),
                        ))
                    })?;

                let collected = match collect_catalog(
                    &client,
                    installation_id,
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
                        let _ = client.close_with_timeout(Duration::from_secs(3)).await;
                        return Err(McpRuntimeError {
                            state: error.state,
                            message,
                        });
                    }
                };
                let CollectedCatalog {
                    catalog,
                    degraded_reason,
                } = collected;

                Ok(Box::new(RmcpRuntimeSession {
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
                let transport = build_streamable_http_transport(&transport)?;
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
                    installation_id,
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

                Ok(Box::new(RmcpRuntimeSession {
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
    client: Option<RunningService<RoleClient, RuntimeClientHandler>>,
    event_rx: mpsc::UnboundedReceiver<McpSessionEvent>,
    catalog: McpCatalogSnapshot,
    degraded_reason: Option<String>,
    secrets: Vec<String>,
    tool_timeout: Duration,
}

#[async_trait]
impl McpRuntimeSession for RmcpRuntimeSession {
    fn initial_catalog(&self) -> &McpCatalogSnapshot {
        &self.catalog
    }

    fn degraded_reason(&self) -> Option<&str> {
        self.degraded_reason.as_deref()
    }

    async fn wait_for_event(&mut self) -> McpSessionEvent {
        self.event_rx
            .recv()
            .await
            .unwrap_or(McpSessionEvent::Closed)
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
    ) -> Result<McpToolCallResult, McpRuntimeError> {
        let Some(client) = self.client.as_ref() else {
            return Err(McpRuntimeError::failed("MCP session is closed"));
        };
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
        let result = tokio::time::timeout(
            self.tool_timeout,
            client.peer().call_tool(
                CallToolRequestParams::new(raw_tool_name.to_owned()).with_arguments(arguments),
            ),
        )
        .await
        .map_err(|_| {
            McpRuntimeError::failed(format!("MCP tools/call `{raw_tool_name}` timed out"))
        })?
        .map_err(|error| {
            classify_runtime_error(
                format!("MCP tools/call `{raw_tool_name}` failed").as_str(),
                &error,
                self.secrets.as_slice(),
            )
        })?;

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
        if let Some(mut client) = self.client.take() {
            let _ = client.close_with_timeout(Duration::from_secs(3)).await;
        }
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

fn classify_runtime_error<E: std::fmt::Display + std::fmt::Debug>(
    context: &str,
    error: &E,
    secrets: &[String],
) -> McpRuntimeError {
    let raw = format!("{context}: {error:#?}");
    let redacted = redact_text(raw.as_str(), secrets);
    let lower = redacted.to_ascii_lowercase();
    let message = compact_transport_error_message(context, redacted.as_str()).unwrap_or(redacted);
    if lower.contains("auth required")
        || lower.contains("authrequired")
        || lower.contains("unauthorized")
        || lower.contains("forbidden")
        || lower.contains("http 401")
        || lower.contains("http 403")
        || lower.contains("status(401")
        || lower.contains("status(403")
        || lower.contains(" 401")
        || lower.contains(" 403")
    {
        McpRuntimeError::auth_required(message)
    } else {
        McpRuntimeError::failed(message)
    }
}

fn compact_transport_error_message(context: &str, message: &str) -> Option<String> {
    extract_http_response_message(message).map(|http| format!("{context}: {http}"))
}

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
    use super::{extract_http_response_message, http_status_start};

    #[test]
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
