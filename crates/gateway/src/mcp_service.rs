use anyhow::{Context, Result};
use pioneer_agent::{
    AgentMcpAvailability, AgentMcpMaterialization, AgentMcpMaterializationRequest,
    AgentMcpServerRef, AgentMcpToolProvider, AgentMcpToolRef,
};
use pioneer_crud::{
    CrudStore, McpAuditEventRecord, McpServerCatalogSnapshotRecord, McpServerInstallationRecord,
    TurnMcpBindingRecord,
};
use pioneer_mcp::{
    McpAuthConfig, McpCatalogSnapshot, McpRetryPolicy, McpRuntimeConnector, McpRuntimeError,
    McpRuntimeState as DomainRuntimeState, McpScopeKind as DomainScopeKind, McpSecretRef,
    McpSecretResolver, McpServerInstallation, McpServerRuntimeSnapshot,
    McpSourceKind as DomainSourceKind, McpToolCallResult, McpTransportConfig, RmcpRuntimeConnector,
};
use pioneer_protocol::{
    JsonRpcNotification, McpRuntimeState, McpRuntimeStatus, McpScopeKind,
    McpServerCatalogChangedNotification, McpServerStatus, McpServerStatusChangedNotification,
    McpServerStatusItem, TurnAcceptedCapability, TurnCapabilityAcceptedReason, TurnCapabilityKind,
    TurnCapabilityRejectedReason, TurnRejectedCapability, constants::events,
};
use serde::Serialize;
use serde_json::Value as JsonValue;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tracing::warn;

use crate::message::now_timestamp_secs;
use crate::secrets::GatewaySecrets;
use crate::session::SessionManager;

#[derive(Clone)]
pub(crate) struct McpService {
    inner: Arc<McpServiceInner>,
}

struct McpServiceInner {
    crud_store: Arc<CrudStore>,
    session_manager: Arc<SessionManager>,
    gateway_secrets: Arc<GatewaySecrets>,
    snapshot_version: Arc<AtomicU64>,
    connector: RwLock<Arc<dyn McpRuntimeConnector>>,
    tasks: Mutex<HashMap<String, McpServerTaskHandle>>,
    snapshots: Mutex<HashMap<String, McpServerRuntimeSnapshot>>,
    retry_policy: McpRetryPolicy,
}

struct McpServerTaskHandle {
    scope_kind: String,
    scope_key: String,
    fingerprint: String,
    call_tx: mpsc::Sender<McpServerCallCommand>,
    shutdown_tx: oneshot::Sender<DomainRuntimeState>,
    join: JoinHandle<()>,
}

struct McpServerCallCommand {
    request: pioneer_tools::McpToolCallRequest,
    response_tx: oneshot::Sender<Result<McpToolCallResult, McpRuntimeError>>,
}

struct GatewayMcpSecretResolver {
    gateway_secrets: Arc<GatewaySecrets>,
}

#[derive(Default)]
struct WorkspaceMcpToolState {
    available_mcp: Vec<String>,
    blocked_mcp: Vec<String>,
    descriptors: Vec<pioneer_tools::McpDynamicToolDescriptor>,
    diagnostics: Vec<String>,
    accepted_capabilities: Vec<TurnAcceptedCapability>,
    rejected_capabilities: Vec<TurnRejectedCapability>,
}

#[derive(Clone)]
struct McpToolSelection {
    priority: u8,
    selection_reason: &'static str,
    capability_id: Option<String>,
}

const MCP_SELECTION_IMPLICIT_POLICY: &str = "implicit_policy";
const MCP_SELECTION_EXPLICIT_CAPABILITY: &str = "explicit_composer_capability";

impl McpSecretResolver for GatewayMcpSecretResolver {
    fn resolve_mcp_secret(&self, ref_id: &str) -> Option<String> {
        match self.gateway_secrets.get_mcp_secret(ref_id) {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    ref_id,
                    error = %format!("{error:#}"),
                    "failed to resolve MCP secret"
                );
                None
            }
        }
    }
}

impl McpService {
    pub(crate) fn new(
        crud_store: Arc<CrudStore>,
        session_manager: Arc<SessionManager>,
        gateway_secrets: Arc<GatewaySecrets>,
        snapshot_version: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner: Arc::new(McpServiceInner {
                crud_store,
                session_manager,
                gateway_secrets,
                snapshot_version,
                connector: RwLock::new(Arc::new(RmcpRuntimeConnector::new())),
                tasks: Mutex::new(HashMap::new()),
                snapshots: Mutex::new(HashMap::new()),
                retry_policy: McpRetryPolicy::default(),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_connector_for_tests(&self, connector: Arc<dyn McpRuntimeConnector>) {
        *self
            .inner
            .connector
            .write()
            .expect("MCP connector lock poisoned") = connector;
    }

    pub(crate) async fn reload_workspace(&self, workspace_id: &str) -> Result<()> {
        let rows = self
            .inner
            .crud_store
            .list_mcp_server_installations("workspace", workspace_id)
            .await
            .context("failed to load MCP workspace installations for reload")?;

        let mut desired = HashSet::new();
        for row in rows {
            let installation_id = row
                .id
                .clone()
                .context("MCP installation row is missing id")?;

            if !row.enabled {
                self.stop_task(&installation_id, DomainRuntimeState::Disabled)
                    .await;
                self.publish_status(
                    &row,
                    DomainRuntimeState::Disabled,
                    Some("server is disabled".to_owned()),
                    None,
                    0,
                    None,
                    None,
                )
                .await;
                continue;
            }

            desired.insert(installation_id.clone());
            let should_start = {
                let mut tasks = self.inner.tasks.lock().await;
                if tasks
                    .get(&installation_id)
                    .is_some_and(|handle| handle.join.is_finished())
                {
                    tasks.remove(&installation_id);
                }
                match tasks.get(&installation_id) {
                    Some(handle) if handle.fingerprint == row.fingerprint => false,
                    Some(_) => true,
                    None => true,
                }
            };

            if should_start {
                if self.task_exists(&installation_id).await {
                    self.publish_status(
                        &row,
                        DomainRuntimeState::Restarting,
                        Some("server configuration changed; restarting".to_owned()),
                        None,
                        0,
                        None,
                        None,
                    )
                    .await;
                    self.stop_task(&installation_id, DomainRuntimeState::Stopped)
                        .await;
                }
                self.start_task(row).await?;
            }
        }

        let obsolete = {
            let tasks = self.inner.tasks.lock().await;
            tasks
                .iter()
                .filter(|(id, handle)| {
                    handle.scope_kind == "workspace"
                        && handle.scope_key == workspace_id
                        && !desired.contains(*id)
                })
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>()
        };
        for installation_id in obsolete {
            self.stop_task(&installation_id, DomainRuntimeState::Stopped)
                .await;
        }

        Ok(())
    }

    pub(crate) async fn restart_server(
        &self,
        scope_kind: &str,
        scope_key: &str,
        name: &str,
    ) -> Result<Option<McpServerInstallationRecord>> {
        let row = self
            .inner
            .crud_store
            .find_mcp_server_installation(scope_kind, scope_key, name)
            .await
            .context("failed to query MCP installation for restart")?;
        let Some(row) = row else {
            return Ok(None);
        };
        if let Some(installation_id) = row.id.as_deref() {
            self.publish_status(
                &row,
                DomainRuntimeState::Restarting,
                Some("manual restart requested".to_owned()),
                None,
                0,
                None,
                None,
            )
            .await;
            self.stop_task(installation_id, DomainRuntimeState::Stopped)
                .await;
        }
        if row.enabled {
            self.start_task(row.clone()).await?;
        } else {
            self.publish_status(
                &row,
                DomainRuntimeState::Disabled,
                Some("server is disabled".to_owned()),
                None,
                0,
                None,
                None,
            )
            .await;
        }
        Ok(Some(row))
    }

    pub(crate) async fn runtime_snapshot(
        &self,
        scope_kind: &str,
        scope_key: &str,
    ) -> HashMap<String, McpServerRuntimeSnapshot> {
        self.inner
            .snapshots
            .lock()
            .await
            .iter()
            .filter(|(_, snapshot)| {
                snapshot.scope_kind.as_str() == scope_kind && snapshot.scope_key == scope_key
            })
            .map(|(id, snapshot)| (id.clone(), snapshot.clone()))
            .collect()
    }

    async fn workspace_mcp_tool_state(
        &self,
        workspace_id: &str,
        include_implicit_tools: bool,
        explicit_servers: &[AgentMcpServerRef],
        explicit_tools: &[AgentMcpToolRef],
    ) -> Result<WorkspaceMcpToolState> {
        self.reload_workspace(workspace_id).await?;
        let rows = self
            .inner
            .crud_store
            .list_mcp_server_installations("workspace", workspace_id)
            .await
            .context("failed to load MCP workspace installations for tool materialization")?;
        let runtime = self.runtime_snapshot("workspace", workspace_id).await;
        let snapshot_version = self.inner.snapshot_version.load(Ordering::SeqCst);
        let mut state = WorkspaceMcpToolState::default();
        let mut seen_callable_names = HashSet::new();
        let mut explicit_servers_by_name = HashMap::<String, Vec<&AgentMcpServerRef>>::new();
        let mut explicit_tools_by_server = HashMap::<String, Vec<&AgentMcpToolRef>>::new();
        let mut explicit_tools_by_key = HashMap::<(String, String), Vec<&AgentMcpToolRef>>::new();
        let mut matched_server_capability_ids = HashSet::<String>::new();
        let mut matched_tool_capability_ids = HashSet::<String>::new();

        for reference in explicit_servers {
            if reference.capability_id.trim().is_empty() || reference.name.trim().is_empty() {
                state
                    .rejected_capabilities
                    .push(reject_mcp_server_capability(
                        reference,
                        TurnCapabilityRejectedReason::InvalidInput,
                        "MCP server capability is missing an id or server name.",
                    ));
                continue;
            }
            if reference.scope_kind != McpScopeKind::Workspace {
                state
                    .rejected_capabilities
                    .push(reject_mcp_server_capability(
                        reference,
                        TurnCapabilityRejectedReason::ProviderUnsupported,
                        "Only workspace MCP servers can be attached to a turn.",
                    ));
                continue;
            }
            explicit_servers_by_name
                .entry(mcp_ref_key(reference.name.as_str()))
                .or_default()
                .push(reference);
        }

        for reference in explicit_tools {
            if reference.capability_id.trim().is_empty()
                || reference.server_name.trim().is_empty()
                || reference.raw_tool_name.trim().is_empty()
            {
                state.rejected_capabilities.push(reject_mcp_tool_capability(
                    reference,
                    TurnCapabilityRejectedReason::InvalidInput,
                    "MCP tool capability is missing an id, server name, or tool name.",
                ));
                continue;
            }
            if reference.scope_kind != McpScopeKind::Workspace {
                state.rejected_capabilities.push(reject_mcp_tool_capability(
                    reference,
                    TurnCapabilityRejectedReason::ProviderUnsupported,
                    "Only workspace MCP tools can be attached to a turn.",
                ));
                continue;
            }
            let server_key = mcp_ref_key(reference.server_name.as_str());
            let tool_key = mcp_ref_key(reference.raw_tool_name.as_str());
            explicit_tools_by_server
                .entry(server_key.clone())
                .or_default()
                .push(reference);
            explicit_tools_by_key
                .entry((server_key, tool_key))
                .or_default()
                .push(reference);
        }

        for row in rows {
            let server_key = mcp_ref_key(row.name.as_str());
            let server_refs = explicit_servers_by_name
                .get(server_key.as_str())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let tool_refs_for_server = explicit_tools_by_server
                .get(server_key.as_str())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let installation_id = match row.id.clone() {
                Some(id) => id,
                None => {
                    state
                        .diagnostics
                        .push(format!("MCP server `{}` has no installation id", row.name));
                    reject_mcp_refs_for_server(
                        &mut state,
                        server_refs,
                        tool_refs_for_server,
                        &mut matched_server_capability_ids,
                        &mut matched_tool_capability_ids,
                        TurnCapabilityRejectedReason::Unavailable,
                        format!("MCP server `{}` is unavailable.", row.name).as_str(),
                    );
                    continue;
                }
            };

            if !row.enabled {
                state.blocked_mcp.push(row.name.clone());
                reject_mcp_refs_for_server(
                    &mut state,
                    server_refs,
                    tool_refs_for_server,
                    &mut matched_server_capability_ids,
                    &mut matched_tool_capability_ids,
                    TurnCapabilityRejectedReason::DisabledByPolicy,
                    format!("MCP server `{}` is disabled by workspace policy.", row.name).as_str(),
                );
                continue;
            }

            let Some(snapshot) = runtime.get(installation_id.as_str()) else {
                state.blocked_mcp.push(row.name.clone());
                state
                    .diagnostics
                    .push(format!("MCP server `{}` is not started", row.name));
                reject_mcp_refs_for_server(
                    &mut state,
                    server_refs,
                    tool_refs_for_server,
                    &mut matched_server_capability_ids,
                    &mut matched_tool_capability_ids,
                    TurnCapabilityRejectedReason::Unavailable,
                    format!("MCP server `{}` is not started.", row.name).as_str(),
                );
                continue;
            };

            if !snapshot.state.live() {
                state.blocked_mcp.push(row.name.clone());
                state.diagnostics.push(format!(
                    "MCP server `{}` is not live ({:?})",
                    row.name, snapshot.state
                ));
                reject_mcp_refs_for_server(
                    &mut state,
                    server_refs,
                    tool_refs_for_server,
                    &mut matched_server_capability_ids,
                    &mut matched_tool_capability_ids,
                    TurnCapabilityRejectedReason::Unavailable,
                    format!("MCP server `{}` is not live.", row.name).as_str(),
                );
                continue;
            }

            let catalog = self
                .inner
                .crud_store
                .find_mcp_server_catalog_snapshot(installation_id.as_str())
                .await
                .context("failed to load MCP server catalog snapshot")?;
            let Some(catalog) = catalog else {
                state.blocked_mcp.push(row.name.clone());
                state
                    .diagnostics
                    .push(format!("MCP server `{}` has no catalog snapshot", row.name));
                reject_mcp_refs_for_server(
                    &mut state,
                    server_refs,
                    tool_refs_for_server,
                    &mut matched_server_capability_ids,
                    &mut matched_tool_capability_ids,
                    TurnCapabilityRejectedReason::CatalogMissing,
                    format!("MCP server `{}` has no tool catalog snapshot.", row.name).as_str(),
                );
                continue;
            };

            state.available_mcp.push(row.name.clone());
            for reference in server_refs {
                matched_server_capability_ids.insert(reference.capability_id.clone());
                state
                    .accepted_capabilities
                    .push(accept_mcp_server_capability(reference));
            }
            let tools = parse_catalog_tools(catalog.tools_json.as_str());
            for tool in tools {
                state
                    .available_mcp
                    .push(format!("{}/{}", row.name, tool.raw_tool_name));
                let tool_refs = explicit_tools_by_key
                    .get(&(server_key.clone(), mcp_ref_key(tool.raw_tool_name.as_str())))
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                for reference in tool_refs {
                    matched_tool_capability_ids.insert(reference.capability_id.clone());
                    state
                        .accepted_capabilities
                        .push(accept_mcp_tool_capability(reference));
                }
                let mut selection = None;
                if include_implicit_tools && row.allow_implicit_invocation {
                    select_mcp_tool(
                        &mut selection,
                        McpToolSelection {
                            priority: 1,
                            selection_reason: MCP_SELECTION_IMPLICIT_POLICY,
                            capability_id: None,
                        },
                    );
                }
                if let Some(reference) = server_refs.first() {
                    select_mcp_tool(
                        &mut selection,
                        McpToolSelection {
                            priority: 2,
                            selection_reason: MCP_SELECTION_EXPLICIT_CAPABILITY,
                            capability_id: Some(reference.capability_id.clone()),
                        },
                    );
                }
                if let Some(reference) = tool_refs.first() {
                    select_mcp_tool(
                        &mut selection,
                        McpToolSelection {
                            priority: 3,
                            selection_reason: MCP_SELECTION_EXPLICIT_CAPABILITY,
                            capability_id: Some(reference.capability_id.clone()),
                        },
                    );
                }
                if let Some(selection) = selection {
                    let callable_name = mcp_callable_name(
                        row.name.as_str(),
                        tool.raw_tool_name.as_str(),
                        &mut seen_callable_names,
                    );
                    state
                        .descriptors
                        .push(pioneer_tools::McpDynamicToolDescriptor {
                            callable_name,
                            workspace_id: workspace_id.to_owned(),
                            server_id: installation_id.clone(),
                            server_name: row.name.clone(),
                            raw_tool_name: tool.raw_tool_name,
                            catalog_version: catalog.catalog_version.clone(),
                            fingerprint: row.fingerprint.clone(),
                            snapshot_version,
                            description: tool.description,
                            parameters: tool.parameters,
                            annotations: tool.annotations,
                            timeout_ms: tool.timeout_ms,
                            selection_reason: selection.selection_reason.to_owned(),
                            capability_id: selection.capability_id,
                        });
                }
            }
            for reference in tool_refs_for_server {
                if matched_tool_capability_ids.contains(reference.capability_id.as_str()) {
                    continue;
                }
                matched_tool_capability_ids.insert(reference.capability_id.clone());
                state.rejected_capabilities.push(reject_mcp_tool_capability(
                    reference,
                    TurnCapabilityRejectedReason::ToolMissing,
                    format!(
                        "MCP server `{}` does not expose tool `{}`.",
                        row.name, reference.raw_tool_name
                    )
                    .as_str(),
                ));
            }
        }

        for reference in explicit_servers {
            if !eligible_workspace_mcp_server_ref(reference)
                || matched_server_capability_ids.contains(reference.capability_id.as_str())
            {
                continue;
            }
            state
                .rejected_capabilities
                .push(reject_mcp_server_capability(
                    reference,
                    TurnCapabilityRejectedReason::NotFound,
                    format!(
                        "MCP server `{}` is not installed in this workspace.",
                        reference.name
                    )
                    .as_str(),
                ));
        }

        for reference in explicit_tools {
            if !eligible_workspace_mcp_tool_ref(reference)
                || matched_tool_capability_ids.contains(reference.capability_id.as_str())
            {
                continue;
            }
            state.rejected_capabilities.push(reject_mcp_tool_capability(
                reference,
                TurnCapabilityRejectedReason::NotFound,
                format!(
                    "MCP server `{}` is not installed in this workspace.",
                    reference.server_name
                )
                .as_str(),
            ));
        }

        state.available_mcp.sort();
        state.available_mcp.dedup();
        state.blocked_mcp.sort();
        state.blocked_mcp.dedup();
        state.descriptors.sort_by(|left, right| {
            left.callable_name
                .cmp(&right.callable_name)
                .then_with(|| left.server_name.cmp(&right.server_name))
                .then_with(|| left.raw_tool_name.cmp(&right.raw_tool_name))
        });

        Ok(state)
    }

    async fn task_exists(&self, installation_id: &str) -> bool {
        self.inner.tasks.lock().await.contains_key(installation_id)
    }

    async fn call_tool(
        &self,
        request: pioneer_tools::McpToolCallRequest,
    ) -> Result<pioneer_tools::McpToolCallOutput, pioneer_tools::ToolError> {
        let row = self
            .inner
            .crud_store
            .list_mcp_server_installations("workspace", request.workspace_id.as_str())
            .await
            .map_err(|error| {
                pioneer_tools::ToolError::Internal(format!(
                    "failed to load MCP installations before call: {error:#}"
                ))
            })?
            .into_iter()
            .find(|row| row.id.as_deref() == Some(request.server_id.as_str()))
            .ok_or_else(|| {
                pioneer_tools::ToolError::NotFound(format!(
                    "MCP server `{}` is not installed",
                    request.server_name
                ))
            })?;

        if !row.enabled {
            self.audit_tool_call(
                &row,
                &request,
                "tool_call_blocked",
                "blocked",
                Some("server_disabled"),
                json!({}),
            )
            .await;
            return Err(pioneer_tools::ToolError::Rejected(format!(
                "MCP server `{}` is disabled",
                row.name
            )));
        }

        let snapshot = self
            .inner
            .snapshots
            .lock()
            .await
            .get(request.server_id.as_str())
            .cloned();
        if !snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.state.live())
        {
            self.audit_tool_call(
                &row,
                &request,
                "tool_call_blocked",
                "blocked",
                Some("server_not_live"),
                json!({ "runtime": snapshot.map(|snapshot| format!("{:?}", snapshot.state)) }),
            )
            .await;
            return Err(pioneer_tools::ToolError::NotFound(format!(
                "MCP server `{}` is not live",
                row.name
            )));
        }
        let runtime_state_at_call = snapshot
            .as_ref()
            .map(|snapshot| domain_runtime_state_code(snapshot.state))
            .unwrap_or("unknown")
            .to_owned();

        let catalog = self
            .inner
            .crud_store
            .find_mcp_server_catalog_snapshot(request.server_id.as_str())
            .await
            .map_err(|error| {
                pioneer_tools::ToolError::Internal(format!(
                    "failed to load MCP catalog before call: {error:#}"
                ))
            })?
            .ok_or_else(|| {
                pioneer_tools::ToolError::NotFound(format!(
                    "MCP server `{}` has no catalog snapshot",
                    row.name
                ))
            })?;
        let catalog_drift = catalog.catalog_version != request.catalog_version;
        let tool_exists = parse_catalog_tools(catalog.tools_json.as_str())
            .into_iter()
            .any(|tool| tool.raw_tool_name == request.raw_tool_name);
        if !tool_exists {
            self.audit_tool_call(
                &row,
                &request,
                "tool_call_blocked",
                "blocked",
                Some("tool_missing"),
                json!({
                    "catalog_drift": catalog_drift,
                    "request_catalog_version": request.catalog_version.as_str(),
                    "current_catalog_version": catalog.catalog_version.as_str(),
                }),
            )
            .await;
            return Err(pioneer_tools::ToolError::NotFound(format!(
                "MCP tool `{}` is not present in server `{}` catalog",
                request.raw_tool_name, row.name
            )));
        }

        let call_tx = self
            .inner
            .tasks
            .lock()
            .await
            .get(request.server_id.as_str())
            .map(|handle| handle.call_tx.clone())
            .ok_or_else(|| {
                pioneer_tools::ToolError::NotFound(format!(
                    "MCP server `{}` task is not running",
                    row.name
                ))
            })?;

        self.audit_tool_call(
            &row,
            &request,
            "tool_call_started",
            "allowed",
            None,
            json!({
                "arguments_keys": json_object_keys(&request.arguments),
                "catalog_drift": catalog_drift,
                "request_catalog_version": request.catalog_version.as_str(),
                "current_catalog_version": catalog.catalog_version.as_str(),
            }),
        )
        .await;

        let (response_tx, response_rx) = oneshot::channel();
        call_tx
            .send(McpServerCallCommand {
                request: request.clone(),
                response_tx,
            })
            .await
            .map_err(|_| {
                pioneer_tools::ToolError::ExecutionFailed(format!(
                    "MCP server `{}` task is unavailable",
                    row.name
                ))
            })?;

        let wait_timeout = Duration::from_millis(request.timeout_ms.saturating_add(1_000).max(1));
        let result = tokio::time::timeout(wait_timeout, response_rx)
            .await
            .map_err(|_| {
                pioneer_tools::ToolError::ExecutionFailed(format!(
                    "MCP tool `{}` timed out waiting for gateway runtime",
                    request.raw_tool_name
                ))
            })?
            .map_err(|_| {
                pioneer_tools::ToolError::ExecutionFailed(format!(
                    "MCP server `{}` task dropped the call response",
                    row.name
                ))
            })?;

        match result {
            Ok(output) => {
                self.audit_tool_call(
                    &row,
                    &request,
                    "tool_call_completed",
                    "allowed",
                    None,
                    json!({
                        "is_error": output.is_error,
                        "duration_ms": output.duration_ms,
                        "catalog_drift": catalog_drift,
                    }),
                )
                .await;
                Ok(pioneer_tools::McpToolCallOutput {
                    content: output.content,
                    structured_content: output.structured_content,
                    is_error: output.is_error,
                    duration_ms: output.duration_ms,
                    meta: Some(enrich_tool_output_meta(
                        output.meta,
                        runtime_state_at_call.as_str(),
                        catalog_drift,
                        catalog.catalog_version.as_str(),
                    )),
                })
            }
            Err(error) => {
                let message = error.message.clone();
                self.audit_tool_call(
                    &row,
                    &request,
                    "tool_call_failed",
                    "allowed",
                    Some("runtime_error"),
                    json!({
                        "state": format!("{:?}", error.state),
                        "message": message,
                    }),
                )
                .await;
                Err(pioneer_tools::ToolError::ExecutionFailed(format!(
                    "MCP tool `{}` failed: {}",
                    request.raw_tool_name, error.message
                )))
            }
        }
    }

    async fn start_task(&self, row: McpServerInstallationRecord) -> Result<()> {
        let installation = installation_from_record(&row)?;
        let installation_id = row
            .id
            .clone()
            .context("MCP installation row is missing id")?;
        let connector = self
            .inner
            .connector
            .read()
            .expect("MCP connector lock poisoned")
            .clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (call_tx, call_rx) = mpsc::channel(32);
        let service = self.clone();
        let fingerprint = row.fingerprint.clone();
        let scope_kind = row.scope_kind.clone();
        let scope_key = row.scope_key.clone();
        let task_installation_id = installation_id.clone();
        let join = tokio::spawn(async move {
            service
                .run_server_task(
                    row,
                    installation,
                    task_installation_id,
                    connector,
                    shutdown_rx,
                    call_rx,
                )
                .await;
        });

        self.inner.tasks.lock().await.insert(
            installation_id,
            McpServerTaskHandle {
                scope_kind,
                scope_key,
                fingerprint,
                call_tx,
                shutdown_tx,
                join,
            },
        );

        Ok(())
    }

    async fn stop_task(&self, installation_id: &str, final_state: DomainRuntimeState) {
        let handle = self.inner.tasks.lock().await.remove(installation_id);
        if let Some(handle) = handle {
            let _ = handle.shutdown_tx.send(final_state);
            if let Err(error) = handle.join.await {
                warn!(
                    installation_id,
                    error = %format!("{error:#}"),
                    "MCP server task join failed"
                );
            }
        }
    }

    async fn run_server_task(
        &self,
        row: McpServerInstallationRecord,
        installation: McpServerInstallation,
        installation_id: String,
        connector: Arc<dyn McpRuntimeConnector>,
        mut shutdown_rx: oneshot::Receiver<DomainRuntimeState>,
        mut call_rx: mpsc::Receiver<McpServerCallCommand>,
    ) {
        let resolver = Arc::new(GatewayMcpSecretResolver {
            gateway_secrets: self.inner.gateway_secrets.clone(),
        });
        let mut retry_attempt = 0_u32;

        loop {
            let now = now_timestamp_secs();
            self.audit(&row, "start", None, json!({"retry_attempt": retry_attempt}))
                .await;
            self.publish_status(
                &row,
                DomainRuntimeState::Starting,
                Some("starting MCP server".to_owned()),
                None,
                retry_attempt,
                None,
                None,
            )
            .await;

            let connect = connector
                .connect(
                    installation.clone(),
                    installation_id.clone(),
                    resolver.clone(),
                    now,
                )
                .await;

            let mut session = match connect {
                Ok(session) => session,
                Err(error) => {
                    let next_retry_at = if error.state == DomainRuntimeState::Failed {
                        Some(now + self.inner.retry_policy.delay_secs(retry_attempt) as i64)
                    } else {
                        None
                    };
                    self.audit(
                        &row,
                        if error.state == DomainRuntimeState::AuthRequired {
                            "auth_required"
                        } else {
                            "start_failed"
                        },
                        None,
                        json!({"reason": error.message, "retry_attempt": retry_attempt}),
                    )
                    .await;
                    self.publish_status(
                        &row,
                        error.state,
                        Some(error.message.clone()),
                        Some(error.message),
                        retry_attempt,
                        next_retry_at,
                        None,
                    )
                    .await;

                    if error.state == DomainRuntimeState::AuthRequired {
                        if let Ok(final_state) = shutdown_rx.await {
                            self.publish_shutdown_status(&row, final_state).await;
                        }
                        return;
                    }

                    let delay = self.inner.retry_policy.delay_secs(retry_attempt);
                    retry_attempt = retry_attempt.saturating_add(1);
                    tokio::select! {
                        final_state = &mut shutdown_rx => {
                            if let Ok(final_state) = final_state {
                                self.publish_shutdown_status(&row, final_state).await;
                            }
                            return;
                        }
                        _ = sleep(Duration::from_secs(delay)) => {}
                    }
                    continue;
                }
            };

            let catalog = session.initial_catalog().clone();
            self.persist_catalog_and_notify(&row, &catalog).await;
            let ready_state = if session.degraded_reason().is_some() {
                DomainRuntimeState::Degraded
            } else {
                DomainRuntimeState::Ready
            };
            let ready_reason = session
                .degraded_reason()
                .map(str::to_owned)
                .or_else(|| Some("MCP server is ready".to_owned()));
            self.audit(
                &row,
                "started",
                Some(catalog.catalog_version.as_str()),
                json!({}),
            )
            .await;
            self.publish_status(
                &row,
                ready_state,
                ready_reason,
                None,
                0,
                None,
                Some(catalog.catalog_version.clone()),
            )
            .await;

            loop {
                tokio::select! {
                    final_state = &mut shutdown_rx => {
                        self.publish_status(
                            &row,
                            DomainRuntimeState::Stopping,
                            Some("stopping MCP server".to_owned()),
                            None,
                            0,
                            None,
                            Some(catalog.catalog_version.clone()),
                        )
                        .await;
                        self.audit(&row, "stop", Some(catalog.catalog_version.as_str()), json!({})).await;
                        session.shutdown().await;
                        if let Ok(final_state) = final_state {
                            self.publish_shutdown_status(&row, final_state).await;
                        }
                        self.audit(&row, "stopped", Some(catalog.catalog_version.as_str()), json!({})).await;
                        return;
                    }
                    event = session.wait_for_event() => {
                        match event {
                            pioneer_mcp::McpSessionEvent::CatalogChanged => {
                                match session.refresh_catalog().await {
                                    Ok(catalog) => {
                                        let degraded_reason = session
                                            .degraded_reason()
                                            .map(ToOwned::to_owned);
                                        let (state, reason) = if let Some(reason) = degraded_reason {
                                            (DomainRuntimeState::Degraded, reason)
                                        } else {
                                            (
                                                DomainRuntimeState::Ready,
                                                "MCP catalog refreshed".to_owned(),
                                            )
                                        };
                                        self.persist_catalog_and_notify(&row, &catalog).await;
                                        self.publish_status(
                                            &row,
                                            state,
                                            Some(reason),
                                            None,
                                            0,
                                            None,
                                            Some(catalog.catalog_version.clone()),
                                        )
                                        .await;
                                    }
                                    Err(error) => {
                                        self.publish_status(
                                            &row,
                                            DomainRuntimeState::Degraded,
                                            Some(error.message.clone()),
                                            Some(error.message),
                                            0,
                                            None,
                                            None,
                                        )
                                        .await;
                                    }
                                }
                            }
                            pioneer_mcp::McpSessionEvent::Closed => {
                                let error = McpRuntimeError::failed("MCP session closed");
                                self.publish_status(
                                    &row,
                                    DomainRuntimeState::Failed,
                                    Some(error.message.clone()),
                                    Some(error.message),
                                    0,
                                    None,
                                    None,
                                )
                                .await;
                                return;
                            }
                        }
                    }
                    command = call_rx.recv() => {
                        let Some(command) = command else {
                            continue;
                        };
                        let raw_tool_name = command.request.raw_tool_name.clone();
                        let arguments = command.request.arguments.clone();
                        let result = session
                            .call_tool(raw_tool_name.as_str(), arguments)
                            .await;
                        if let Err(error) = &result {
                            self.publish_status(
                                &row,
                                DomainRuntimeState::Degraded,
                                Some(error.message.clone()),
                                Some(error.message.clone()),
                                0,
                                None,
                                Some(catalog.catalog_version.clone()),
                            )
                            .await;
                        }
                        let _ = command.response_tx.send(result);
                    }
                }
            }
        }
    }

    async fn publish_shutdown_status(
        &self,
        row: &McpServerInstallationRecord,
        final_state: DomainRuntimeState,
    ) {
        let reason = match final_state {
            DomainRuntimeState::Disabled => "server is disabled",
            DomainRuntimeState::Stopped => "MCP server stopped",
            _ => "MCP server stopped",
        };
        self.publish_status(
            row,
            final_state,
            Some(reason.to_owned()),
            None,
            0,
            None,
            None,
        )
        .await;
    }

    async fn publish_status(
        &self,
        row: &McpServerInstallationRecord,
        state: DomainRuntimeState,
        status_reason: Option<String>,
        last_error: Option<String>,
        retry_attempt: u32,
        next_retry_at_unix: Option<i64>,
        catalog_version: Option<String>,
    ) {
        let Some(installation_id) = row.id.clone() else {
            return;
        };
        let scope_kind = match DomainScopeKind::from_str(row.scope_kind.as_str()) {
            Ok(scope_kind) => scope_kind,
            Err(error) => {
                warn!(error, "failed to map MCP runtime scope");
                return;
            }
        };
        let snapshot = McpServerRuntimeSnapshot {
            installation_id: installation_id.clone(),
            name: row.name.clone(),
            scope_kind,
            scope_key: row.scope_key.clone(),
            fingerprint: row.fingerprint.clone(),
            state,
            live: state.live(),
            status_reason,
            last_seen_at_unix: state.live().then_some(now_timestamp_secs()),
            last_error,
            retry_attempt,
            next_retry_at_unix,
            catalog_version,
        };

        let changed = {
            let mut snapshots = self.inner.snapshots.lock().await;
            if snapshots
                .get(installation_id.as_str())
                .is_some_and(|existing| existing == &snapshot)
            {
                false
            } else {
                snapshots.insert(installation_id.clone(), snapshot.clone());
                true
            }
        };
        if !changed {
            return;
        }

        let snapshot_version = self.next_snapshot_version();
        let notification = McpServerStatusChangedNotification {
            workspace_id: row.scope_key.clone(),
            snapshot_version,
            server: McpServerStatusItem {
                id: installation_id,
                name: snapshot.name.clone(),
                scope_kind: protocol_scope_kind(snapshot.scope_kind.clone()),
                runtime: McpRuntimeStatus {
                    state: protocol_runtime_state(snapshot.state),
                    live: snapshot.live,
                    last_seen_at: snapshot.last_seen_at_unix,
                    last_error: snapshot.last_error.clone(),
                },
                status: McpServerStatus::from(protocol_runtime_state(snapshot.state)),
                status_reason: snapshot.status_reason.clone(),
            },
        };
        self.send_notification_to_workspace_connections(
            row.scope_key.as_str(),
            events::MCP_SERVER_STATUS_CHANGED,
            &notification,
        )
        .await;
    }

    async fn persist_catalog_and_notify(
        &self,
        row: &McpServerInstallationRecord,
        catalog: &McpCatalogSnapshot,
    ) {
        let existing = self
            .inner
            .crud_store
            .find_mcp_server_catalog_snapshot(catalog.server_installation_id.as_str())
            .await
            .ok()
            .flatten();
        let changed = existing
            .as_ref()
            .map(|existing| existing.catalog_version != catalog.catalog_version)
            .unwrap_or(true);

        let record = McpServerCatalogSnapshotRecord {
            server_installation_id: catalog.server_installation_id.clone(),
            catalog_version: catalog.catalog_version.clone(),
            server_info_json: catalog.server_info_json.clone(),
            server_instructions_hash: catalog.server_instructions_hash.clone(),
            tools_json: catalog.tools_json.clone(),
            resources_json: catalog.resources_json.clone(),
            resource_templates_json: catalog.resource_templates_json.clone(),
            prompts_json: catalog.prompts_json.clone(),
            generated_at_unix: catalog.generated_at_unix,
        };
        if let Err(error) = self
            .inner
            .crud_store
            .upsert_mcp_server_catalog_snapshot(&record, now_timestamp_secs())
            .await
        {
            warn!(
                server = row.name,
                error = %format!("{error:#}"),
                "failed to persist MCP catalog snapshot"
            );
            return;
        }
        self.audit(
            row,
            "catalog_refreshed",
            Some(catalog.catalog_version.as_str()),
            json!({
                "tools_count": catalog.tools_count(),
                "resources_count": catalog.resources_count(),
                "resource_templates_count": catalog.resource_templates_count(),
                "prompts_count": catalog.prompts_count(),
            }),
        )
        .await;

        if !changed {
            return;
        }

        let notification = McpServerCatalogChangedNotification {
            workspace_id: row.scope_key.clone(),
            snapshot_version: self.next_snapshot_version(),
            server_id: catalog.server_installation_id.clone(),
            name: row.name.clone(),
            catalog_version: catalog.catalog_version.clone(),
            tools_count: catalog.tools_count(),
            resources_count: catalog.resources_count(),
            resource_templates_count: catalog.resource_templates_count(),
            prompts_count: catalog.prompts_count(),
        };
        self.send_notification_to_workspace_connections(
            row.scope_key.as_str(),
            events::MCP_SERVER_CATALOG_CHANGED,
            &notification,
        )
        .await;
    }

    async fn audit(
        &self,
        row: &McpServerInstallationRecord,
        action: &str,
        catalog_version: Option<&str>,
        details: serde_json::Value,
    ) {
        let audit = McpAuditEventRecord {
            turn_id: None,
            server_installation_id: row.id.clone(),
            server_name: row.name.clone(),
            raw_tool_name: None,
            callable_name: None,
            catalog_version: catalog_version.map(str::to_owned),
            action: action.to_owned(),
            decision: "allowed".to_owned(),
            reason_code: None,
            details_json: serde_json::to_string(&details).unwrap_or_else(|_| "{}".to_owned()),
            created_at_unix: now_timestamp_secs(),
        };
        if let Err(error) = self
            .inner
            .crud_store
            .insert_mcp_audit_event_record(&audit)
            .await
        {
            warn!(
                server = row.name,
                action,
                error = %format!("{error:#}"),
                "failed to write MCP runtime audit event"
            );
        }
    }

    async fn audit_tool_call(
        &self,
        row: &McpServerInstallationRecord,
        request: &pioneer_tools::McpToolCallRequest,
        action: &str,
        decision: &str,
        reason_code: Option<&str>,
        details: serde_json::Value,
    ) {
        let audit = McpAuditEventRecord {
            turn_id: Some(request.turn_id.clone()),
            server_installation_id: row.id.clone(),
            server_name: row.name.clone(),
            raw_tool_name: Some(request.raw_tool_name.clone()),
            callable_name: Some(request.callable_name.clone()),
            catalog_version: Some(request.catalog_version.clone()),
            action: action.to_owned(),
            decision: decision.to_owned(),
            reason_code: reason_code.map(str::to_owned),
            details_json: serde_json::to_string(&details).unwrap_or_else(|_| "{}".to_owned()),
            created_at_unix: now_timestamp_secs(),
        };
        if let Err(error) = self
            .inner
            .crud_store
            .insert_mcp_audit_event_record(&audit)
            .await
        {
            warn!(
                server = row.name,
                action,
                error = %format!("{error:#}"),
                "failed to write MCP tool audit event"
            );
        }
    }

    fn next_snapshot_version(&self) -> u64 {
        self.inner
            .snapshot_version
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1)
    }

    async fn send_notification_to_workspace_connections<T: Serialize>(
        &self,
        workspace_id: &str,
        method: &str,
        payload: &T,
    ) {
        let connection_ids = self
            .inner
            .session_manager
            .connection_ids_for_workspace(workspace_id)
            .await;
        if connection_ids.is_empty() {
            return;
        }
        let notification = match JsonRpcNotification::from_params(method, payload) {
            Ok(notification) => notification,
            Err(error) => {
                warn!(method, error = %error, "failed to encode MCP notification");
                return;
            }
        };
        let serialized = match serde_json::to_string(&notification) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(method, error = %error, "failed to serialize MCP notification");
                return;
            }
        };
        for connection_id in connection_ids {
            if let Err(error) = self
                .inner
                .session_manager
                .send_text(connection_id, serialized.clone())
                .await
            {
                warn!(
                    connection_id,
                    method,
                    error = %format!("{error:#}"),
                    "failed to send MCP notification"
                );
            }
        }
    }
}

#[async_trait::async_trait]
impl AgentMcpToolProvider for McpService {
    async fn mcp_availability(&self, workspace_id: &str) -> Result<AgentMcpAvailability, String> {
        let state = self
            .workspace_mcp_tool_state(workspace_id, false, &[], &[])
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok(AgentMcpAvailability {
            available_mcp: state.available_mcp,
            blocked_mcp: state.blocked_mcp,
        })
    }

    async fn materialize_mcp_tools(
        &self,
        request: AgentMcpMaterializationRequest,
    ) -> Result<AgentMcpMaterialization, String> {
        let mut state = self
            .workspace_mcp_tool_state(
                request.workspace_id.as_str(),
                true,
                request.explicit_servers.as_slice(),
                request.explicit_tools.as_slice(),
            )
            .await
            .map_err(|error| format!("{error:#}"))?;
        let executor: Arc<dyn pioneer_tools::McpToolExecutor> = Arc::new(self.clone());
        let materialized =
            pioneer_tools::materialize_mcp_runtime_tools(state.descriptors.as_slice(), executor);
        for excluded in &materialized.excluded_tools {
            state.diagnostics.push(format!(
                "MCP tool `{}/{}` excluded as `{}`: {}",
                excluded.server_name,
                excluded.raw_tool_name,
                excluded.callable_name,
                excluded.reason
            ));
        }
        let mcp_bindings = materialized
            .bindings
            .iter()
            .map(|binding| pioneer_protocol::McpTurnBindingSummary {
                server_installation_id: binding.server_installation_id.clone(),
                server_name: binding.server_name.clone(),
                raw_tool_name: binding.raw_tool_name.clone(),
                callable_name: binding.callable_name.clone(),
                catalog_version: binding.catalog_version.clone(),
                fingerprint: binding.fingerprint.clone(),
                selection_reason: binding.selection_reason.clone(),
                capability_id: binding.capability_id.clone(),
            })
            .collect::<Vec<_>>();
        let binding_records = materialized
            .bindings
            .iter()
            .map(|binding| TurnMcpBindingRecord {
                server_installation_id: binding.server_installation_id.clone(),
                server_name: binding.server_name.clone(),
                raw_tool_name: binding.raw_tool_name.clone(),
                callable_name: binding.callable_name.clone(),
                catalog_version: binding.catalog_version.clone(),
                fingerprint: binding.fingerprint.clone(),
                selection_reason: binding.selection_reason.clone(),
                capability_id: binding.capability_id.clone(),
            })
            .collect::<Vec<_>>();
        self.inner
            .crud_store
            .replace_turn_mcp_bindings(
                request.turn_id.as_str(),
                binding_records.as_slice(),
                now_timestamp_secs(),
            )
            .await
            .map_err(|error| format!("failed to persist turn MCP bindings: {error:#}"))?;
        Ok(AgentMcpMaterialization {
            bundles: materialized.bundles,
            available_mcp: state.available_mcp,
            blocked_mcp: state.blocked_mcp,
            diagnostics: state.diagnostics,
            accepted_capabilities: state.accepted_capabilities,
            rejected_capabilities: state.rejected_capabilities,
            mcp_bindings,
        })
    }
}

#[async_trait::async_trait]
impl pioneer_tools::McpToolExecutor for McpService {
    async fn call_mcp_tool(
        &self,
        request: pioneer_tools::McpToolCallRequest,
        trace: pioneer_tools::ToolEventTrace,
    ) -> Result<pioneer_tools::McpToolCallOutput, pioneer_tools::ToolError> {
        trace.emit_stage(
            1,
            "mcp.call.started",
            None,
            Some(json!({
                "server_id": request.server_id.as_str(),
                "server_name": request.server_name.as_str(),
                "raw_tool_name": request.raw_tool_name.as_str(),
                "catalog_version": request.catalog_version.as_str(),
            })),
        );
        let result = self.call_tool(request).await;
        match &result {
            Ok(output) => trace.emit_stage(
                1,
                "mcp.call.completed",
                None,
                Some(json!({
                    "duration_ms": output.duration_ms,
                    "is_error": output.is_error,
                })),
            ),
            Err(error) => trace.emit_stage(1, "mcp.call.failed", Some(error.to_string()), None),
        }
        result
    }
}

fn installation_from_record(row: &McpServerInstallationRecord) -> Result<McpServerInstallation> {
    Ok(McpServerInstallation {
        scope_kind: DomainScopeKind::from_str(row.scope_kind.as_str())
            .map_err(anyhow::Error::msg)?,
        scope_key: row.scope_key.clone(),
        name: row.name.clone(),
        display_name: row.display_name.clone(),
        source_kind: DomainSourceKind::from_str(row.source_kind.as_str())
            .map_err(anyhow::Error::msg)?,
        source_ref: serde_json::from_str(row.source_ref.as_str())
            .context("failed to decode MCP source_ref")?,
        transport: serde_json::from_str::<McpTransportConfig>(row.transport_json.as_str())
            .context("failed to decode MCP transport")?,
        auth: serde_json::from_str::<McpAuthConfig>(row.auth_json.as_str())
            .context("failed to decode MCP auth config")?,
        secret_refs: serde_json::from_str::<Vec<McpSecretRef>>(row.secret_refs_json.as_str())
            .context("failed to decode MCP secret refs")?,
        enabled: row.enabled,
        allow_implicit_invocation: row.allow_implicit_invocation,
        required: row.required,
        fingerprint: row.fingerprint.clone(),
    })
}

fn protocol_scope_kind(scope_kind: DomainScopeKind) -> McpScopeKind {
    match scope_kind {
        DomainScopeKind::Workspace => McpScopeKind::Workspace,
        DomainScopeKind::User => McpScopeKind::User,
    }
}

fn protocol_runtime_state(state: DomainRuntimeState) -> McpRuntimeState {
    match state {
        DomainRuntimeState::NotStarted => McpRuntimeState::NotStarted,
        DomainRuntimeState::Disabled => McpRuntimeState::Disabled,
        DomainRuntimeState::Starting => McpRuntimeState::Starting,
        DomainRuntimeState::Ready => McpRuntimeState::Ready,
        DomainRuntimeState::Degraded => McpRuntimeState::Degraded,
        DomainRuntimeState::AuthRequired => McpRuntimeState::AuthRequired,
        DomainRuntimeState::Failed => McpRuntimeState::Failed,
        DomainRuntimeState::Stopping => McpRuntimeState::Stopping,
        DomainRuntimeState::Stopped => McpRuntimeState::Stopped,
        DomainRuntimeState::Restarting => McpRuntimeState::Restarting,
    }
}

fn mcp_ref_key(value: &str) -> String {
    value.trim().to_owned()
}

fn eligible_workspace_mcp_server_ref(reference: &AgentMcpServerRef) -> bool {
    reference.scope_kind == McpScopeKind::Workspace
        && !reference.capability_id.trim().is_empty()
        && !reference.name.trim().is_empty()
}

fn eligible_workspace_mcp_tool_ref(reference: &AgentMcpToolRef) -> bool {
    reference.scope_kind == McpScopeKind::Workspace
        && !reference.capability_id.trim().is_empty()
        && !reference.server_name.trim().is_empty()
        && !reference.raw_tool_name.trim().is_empty()
}

fn mcp_server_capability_kind(reference: &AgentMcpServerRef) -> TurnCapabilityKind {
    TurnCapabilityKind::McpServer {
        name: reference.name.clone(),
        scope_kind: reference.scope_kind,
    }
}

fn mcp_tool_capability_kind(reference: &AgentMcpToolRef) -> TurnCapabilityKind {
    TurnCapabilityKind::McpTool {
        server_name: reference.server_name.clone(),
        raw_tool_name: reference.raw_tool_name.clone(),
        scope_kind: reference.scope_kind,
    }
}

fn accept_mcp_server_capability(reference: &AgentMcpServerRef) -> TurnAcceptedCapability {
    TurnAcceptedCapability {
        id: reference.capability_id.clone(),
        label: reference.label.clone(),
        kind: mcp_server_capability_kind(reference),
        reason: TurnCapabilityAcceptedReason::ExplicitComposerCapability,
    }
}

fn accept_mcp_tool_capability(reference: &AgentMcpToolRef) -> TurnAcceptedCapability {
    TurnAcceptedCapability {
        id: reference.capability_id.clone(),
        label: reference.label.clone(),
        kind: mcp_tool_capability_kind(reference),
        reason: TurnCapabilityAcceptedReason::ExplicitComposerCapability,
    }
}

fn reject_mcp_server_capability(
    reference: &AgentMcpServerRef,
    reason: TurnCapabilityRejectedReason,
    message: &str,
) -> TurnRejectedCapability {
    TurnRejectedCapability {
        id: reference.capability_id.clone(),
        label: reference.label.clone(),
        kind: mcp_server_capability_kind(reference),
        reason,
        message: message.to_owned(),
    }
}

fn reject_mcp_tool_capability(
    reference: &AgentMcpToolRef,
    reason: TurnCapabilityRejectedReason,
    message: &str,
) -> TurnRejectedCapability {
    TurnRejectedCapability {
        id: reference.capability_id.clone(),
        label: reference.label.clone(),
        kind: mcp_tool_capability_kind(reference),
        reason,
        message: message.to_owned(),
    }
}

fn reject_mcp_refs_for_server(
    state: &mut WorkspaceMcpToolState,
    server_refs: &[&AgentMcpServerRef],
    tool_refs: &[&AgentMcpToolRef],
    matched_server_capability_ids: &mut HashSet<String>,
    matched_tool_capability_ids: &mut HashSet<String>,
    reason: TurnCapabilityRejectedReason,
    message: &str,
) {
    for reference in server_refs {
        matched_server_capability_ids.insert(reference.capability_id.clone());
        state
            .rejected_capabilities
            .push(reject_mcp_server_capability(reference, reason, message));
    }
    for reference in tool_refs {
        matched_tool_capability_ids.insert(reference.capability_id.clone());
        state
            .rejected_capabilities
            .push(reject_mcp_tool_capability(reference, reason, message));
    }
}

fn select_mcp_tool(current: &mut Option<McpToolSelection>, candidate: McpToolSelection) {
    if current
        .as_ref()
        .is_none_or(|selected| candidate.priority > selected.priority)
    {
        *current = Some(candidate);
    }
}

fn domain_runtime_state_code(state: DomainRuntimeState) -> &'static str {
    match state {
        DomainRuntimeState::NotStarted => "not_started",
        DomainRuntimeState::Disabled => "disabled",
        DomainRuntimeState::Starting => "starting",
        DomainRuntimeState::Ready => "ready",
        DomainRuntimeState::Degraded => "degraded",
        DomainRuntimeState::AuthRequired => "auth_required",
        DomainRuntimeState::Failed => "failed",
        DomainRuntimeState::Stopping => "stopping",
        DomainRuntimeState::Stopped => "stopped",
        DomainRuntimeState::Restarting => "restarting",
    }
}

fn enrich_tool_output_meta(
    meta: Option<JsonValue>,
    runtime_state: &str,
    catalog_drift: bool,
    current_catalog_version: &str,
) -> JsonValue {
    let mut object = match meta {
        Some(JsonValue::Object(object)) => object,
        Some(value) => {
            let mut object = serde_json::Map::new();
            object.insert("server_meta".to_owned(), value);
            object
        }
        None => serde_json::Map::new(),
    };
    object.insert(
        "pioneer".to_owned(),
        json!({
            "runtime_state": runtime_state,
            "runtimeState": runtime_state,
            "catalog_drift": catalog_drift,
            "catalogDrift": catalog_drift,
            "current_catalog_version": current_catalog_version,
            "currentCatalogVersion": current_catalog_version,
        }),
    );
    JsonValue::Object(object)
}

struct CatalogTool {
    raw_tool_name: String,
    description: String,
    parameters: JsonValue,
    annotations: pioneer_tools::McpDynamicToolAnnotations,
    timeout_ms: Option<u64>,
}

fn parse_catalog_tools(tools_json: &str) -> Vec<CatalogTool> {
    let tools = serde_json::from_str::<Vec<JsonValue>>(tools_json).unwrap_or_default();
    tools
        .into_iter()
        .filter_map(|tool| {
            let raw_tool_name = tool
                .get("name")
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())?
                .to_owned();
            let description = tool
                .get("description")
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("MCP runtime tool")
                .to_owned();
            let parameters = tool
                .get("inputSchema")
                .or_else(|| tool.get("input_schema"))
                .cloned()
                .filter(JsonValue::is_object)
                .unwrap_or_else(|| {
                    json!({
                        "type": "object",
                        "additionalProperties": true
                    })
                });
            let annotations = tool
                .get("annotations")
                .cloned()
                .and_then(|value| {
                    serde_json::from_value::<pioneer_tools::McpDynamicToolAnnotations>(value).ok()
                })
                .unwrap_or_default();
            let timeout_ms = tool
                .get("_meta")
                .and_then(|meta| meta.get("pioneer"))
                .and_then(|pioneer| pioneer.get("timeoutMs"))
                .and_then(JsonValue::as_u64);
            Some(CatalogTool {
                raw_tool_name,
                description,
                parameters,
                annotations,
                timeout_ms,
            })
        })
        .collect()
}

fn mcp_callable_name(server_name: &str, raw_tool_name: &str, seen: &mut HashSet<String>) -> String {
    let base = format!(
        "mcp_{}_{}",
        sanitize_tool_component(server_name),
        sanitize_tool_component(raw_tool_name)
    );
    let mut candidate = truncate_callable_name(base.as_str(), 64);
    if seen.insert(candidate.clone()) {
        return candidate;
    }

    let hash = short_hash_hex(&(server_name, raw_tool_name, seen.len()));
    let prefix_len = 64_usize.saturating_sub(hash.len()).saturating_sub(1);
    candidate = format!(
        "{}_{}",
        truncate_callable_name(base.as_str(), prefix_len.max(1)),
        hash
    );
    let mut counter = 2_u32;
    while !seen.insert(candidate.clone()) {
        let suffix = format!("{}_{}", hash, counter);
        let prefix_len = 64_usize.saturating_sub(suffix.len()).saturating_sub(1);
        candidate = format!(
            "{}_{}",
            truncate_callable_name(base.as_str(), prefix_len.max(1)),
            suffix
        );
        counter = counter.saturating_add(1);
    }
    candidate
}

fn sanitize_tool_component(value: &str) -> String {
    let mut output = String::new();
    let mut last_was_sep = false;
    for ch in value.trim().chars() {
        let next = if ch.is_ascii_alphanumeric() {
            last_was_sep = false;
            Some(ch.to_ascii_lowercase())
        } else if !last_was_sep {
            last_was_sep = true;
            Some('_')
        } else {
            None
        };
        if let Some(ch) = next {
            output.push(ch);
        }
    }
    let output = output.trim_matches('_').to_owned();
    if output.is_empty() {
        "tool".to_owned()
    } else {
        output
    }
}

fn truncate_callable_name(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    value.chars().take(max_chars).collect()
}

fn short_hash_hex<T: Hash>(value: &T) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:08x}", hasher.finish() as u32)
}

fn json_object_keys(value: &JsonValue) -> Vec<String> {
    let mut keys = value
        .as_object()
        .map(|map| map.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    keys.sort();
    keys
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::bootstrap;
    use crate::workspace::DEFAULT_WORKSPACE_ID;
    use migration::{Migrator, MigratorTrait};
    use pioneer_keystore::MemorySecretStore;
    use sea_orm::Database;
    use std::collections::BTreeMap;

    struct TestMcpRuntimeConnector {
        tools: Vec<&'static str>,
        fail_auth: bool,
    }

    #[async_trait::async_trait]
    impl McpRuntimeConnector for TestMcpRuntimeConnector {
        async fn connect(
            &self,
            _installation: McpServerInstallation,
            installation_id: String,
            _resolver: Arc<dyn McpSecretResolver>,
            now_unix: i64,
        ) -> Result<Box<dyn pioneer_mcp::McpRuntimeSession>, McpRuntimeError> {
            if self.fail_auth {
                return Err(McpRuntimeError::auth_required("missing test secret"));
            }

            let tools = self
                .tools
                .iter()
                .map(|name| json!({ "name": name }))
                .collect::<Vec<_>>();
            let catalog = McpCatalogSnapshot::from_json_values(
                installation_id,
                json!({"name":"test-mcp","version":"test"}),
                None,
                json!(tools),
                json!([]),
                json!([]),
                json!([]),
                now_unix,
            )
            .expect("test MCP catalog should build");
            Ok(Box::new(TestMcpRuntimeSession { catalog }))
        }
    }

    struct TestMcpRuntimeSession {
        catalog: McpCatalogSnapshot,
    }

    #[async_trait::async_trait]
    impl pioneer_mcp::McpRuntimeSession for TestMcpRuntimeSession {
        fn initial_catalog(&self) -> &McpCatalogSnapshot {
            &self.catalog
        }

        async fn wait_for_event(&mut self) -> pioneer_mcp::McpSessionEvent {
            std::future::pending::<pioneer_mcp::McpSessionEvent>().await
        }

        async fn refresh_catalog(&mut self) -> Result<McpCatalogSnapshot, McpRuntimeError> {
            Ok(self.catalog.clone())
        }

        async fn call_tool(
            &mut self,
            raw_tool_name: &str,
            arguments: JsonValue,
        ) -> Result<McpToolCallResult, McpRuntimeError> {
            Ok(McpToolCallResult {
                content: json!([{"type":"text","text":format!("called {raw_tool_name}")}]),
                structured_content: Some(json!({
                    "tool": raw_tool_name,
                    "arguments": arguments,
                })),
                is_error: false,
                duration_ms: 1,
                meta: None,
            })
        }

        async fn shutdown(&mut self) {}
    }

    async fn test_mcp_service() -> (McpService, Arc<CrudStore>, String) {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        bootstrap(&connection)
            .await
            .expect("bootstrap should create default workspace");
        let crud_store = Arc::new(CrudStore::new(connection));
        let service = McpService::new(
            crud_store.clone(),
            Arc::new(SessionManager::new()),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
            Arc::new(AtomicU64::new(1)),
        );
        (service, crud_store, DEFAULT_WORKSPACE_ID.to_owned())
    }

    async fn seed_mcp_installation(
        crud_store: &CrudStore,
        workspace_id: &str,
        name: &str,
        enabled: bool,
        allow_implicit_invocation: bool,
    ) -> String {
        let transport = McpTransportConfig::Stdio {
            command: "test-mcp".to_owned(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
            startup_timeout_ms: 5_000,
            tool_timeout_ms: 5_000,
        };
        let record = McpServerInstallationRecord {
            id: None,
            scope_kind: "workspace".to_owned(),
            scope_key: workspace_id.to_owned(),
            name: name.to_owned(),
            display_name: None,
            source_kind: "config".to_owned(),
            source_ref: json!({"kind":"test"}).to_string(),
            transport_kind: "stdio".to_owned(),
            transport_json: serde_json::to_string(&transport).expect("transport serializes"),
            auth_json: serde_json::to_string(&McpAuthConfig::default()).expect("auth serializes"),
            secret_refs_json: "[]".to_owned(),
            enabled,
            allow_implicit_invocation,
            required: false,
            fingerprint: format!("{name}-fingerprint"),
            updated_at_unix: 1_700_000_000,
        };
        crud_store
            .upsert_mcp_server_installation(&record, 1_700_000_000)
            .await
            .expect("test MCP installation should persist");
        crud_store
            .find_mcp_server_installation("workspace", workspace_id, name)
            .await
            .expect("test MCP installation lookup should succeed")
            .and_then(|row| row.id)
            .expect("test MCP installation should have id")
    }

    async fn wait_for_catalog(crud_store: &CrudStore, installation_id: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if crud_store
                .find_mcp_server_catalog_snapshot(installation_id)
                .await
                .expect("catalog lookup should succeed")
                .is_some()
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() <= deadline,
                "test MCP catalog should be persisted"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_runtime_state(
        service: &McpService,
        workspace_id: &str,
        installation_id: &str,
        expected: DomainRuntimeState,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let snapshot = service.runtime_snapshot("workspace", workspace_id).await;
            if snapshot
                .get(installation_id)
                .is_some_and(|snapshot| snapshot.state == expected)
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() <= deadline,
                "test MCP runtime should reach {expected:?}"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    fn server_ref(id: &str, name: &str) -> AgentMcpServerRef {
        AgentMcpServerRef {
            capability_id: id.to_owned(),
            label: Some(name.to_owned()),
            name: name.to_owned(),
            scope_kind: McpScopeKind::Workspace,
        }
    }

    fn tool_ref(id: &str, server_name: &str, raw_tool_name: &str) -> AgentMcpToolRef {
        AgentMcpToolRef {
            capability_id: id.to_owned(),
            label: Some(format!("{server_name}/{raw_tool_name}")),
            server_name: server_name.to_owned(),
            raw_tool_name: raw_tool_name.to_owned(),
            scope_kind: McpScopeKind::Workspace,
        }
    }

    fn materialized_tool_names(materialization: &AgentMcpMaterialization) -> Vec<String> {
        let mut names = materialization
            .bundles
            .iter()
            .flat_map(|bundle| bundle.specs.iter())
            .map(|configured| configured.spec.name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[test]
    fn mcp_callable_name_sanitizes_and_prefixes_identity() {
        let mut seen = HashSet::new();
        let name = mcp_callable_name("GitHub Enterprise", "search/issues.create", &mut seen);
        assert_eq!(name, "mcp_github_enterprise_search_issues_create");
    }

    #[test]
    fn mcp_callable_name_is_deterministic_for_same_inputs() {
        let mut left_seen = HashSet::new();
        let mut right_seen = HashSet::new();
        let left = mcp_callable_name("resend", "send", &mut left_seen);
        let right = mcp_callable_name("resend", "send", &mut right_seen);
        assert_eq!(left, right);
    }

    #[test]
    fn mcp_callable_name_adds_collision_suffix() {
        let mut seen = HashSet::new();
        let first = mcp_callable_name("server-a", "tool", &mut seen);
        let second = mcp_callable_name("server_a", "tool", &mut seen);
        assert_eq!(first, "mcp_server_a_tool");
        assert_ne!(first, second);
        assert!(second.starts_with("mcp_server_a_tool_"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_capability_validation_accepts_live_server_selection() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send", "domains"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, true).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_server_capability".to_owned(),
                explicit_servers: vec![server_ref("mcp-server:workspace:resend", "resend")],
                explicit_tools: Vec::new(),
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.rejected_capabilities.is_empty());
        assert_eq!(materialization.accepted_capabilities.len(), 1);
        assert_eq!(
            materialization.accepted_capabilities[0].reason,
            TurnCapabilityAcceptedReason::ExplicitComposerCapability
        );
        assert!(
            materialization
                .bundles
                .iter()
                .flat_map(|bundle| bundle.specs.iter())
                .any(|configured| configured.spec.name == "mcp_resend_send")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_capability_validation_accepts_live_tool_selection() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send", "domains"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, true).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_tool_capability".to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: vec![tool_ref("mcp-tool:workspace:resend:send", "resend", "send")],
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.rejected_capabilities.is_empty());
        assert_eq!(materialization.accepted_capabilities.len(), 1);
        assert_eq!(
            materialization.accepted_capabilities[0].id,
            "mcp-tool:workspace:resend:send"
        );
        assert!(
            materialization
                .bundles
                .iter()
                .flat_map(|bundle| bundle.specs.iter())
                .any(|configured| configured.spec.name == "mcp_resend_send")
        );
        let bindings = crud_store
            .list_turn_mcp_bindings("turn_mcp_tool_capability")
            .await
            .expect("turn MCP bindings should load");
        let send_binding = bindings
            .iter()
            .find(|binding| binding.raw_tool_name == "send")
            .expect("selected tool should persist a binding");
        assert_eq!(
            send_binding.selection_reason,
            MCP_SELECTION_EXPLICIT_CAPABILITY
        );
        assert_eq!(
            send_binding.capability_id.as_deref(),
            Some("mcp-tool:workspace:resend:send")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_capability_validation_rejects_unavailable_server() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send"],
            fail_auth: true,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, true).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_runtime_state(
            &service,
            workspace_id.as_str(),
            installation_id.as_str(),
            DomainRuntimeState::AuthRequired,
        )
        .await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_unavailable_capability".to_owned(),
                explicit_servers: vec![server_ref("mcp-server:workspace:resend", "resend")],
                explicit_tools: Vec::new(),
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.accepted_capabilities.is_empty());
        assert_eq!(materialization.rejected_capabilities.len(), 1);
        assert_eq!(
            materialization.rejected_capabilities[0].reason,
            TurnCapabilityRejectedReason::Unavailable
        );
        assert!(
            materialization.rejected_capabilities[0]
                .message
                .contains("not live")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_capability_validation_rejects_missing_tool() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, true).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_missing_tool_capability".to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: vec![tool_ref(
                    "mcp-tool:workspace:resend:missing",
                    "resend",
                    "missing",
                )],
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.accepted_capabilities.is_empty());
        assert_eq!(materialization.rejected_capabilities.len(), 1);
        assert_eq!(
            materialization.rejected_capabilities[0].reason,
            TurnCapabilityRejectedReason::ToolMissing
        );
        assert!(
            materialization.rejected_capabilities[0]
                .message
                .contains("does not expose tool")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_selection_rules_preserve_implicit_policy_without_explicit_refs() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send", "domains"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, true).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_implicit_policy".to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: Vec::new(),
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.accepted_capabilities.is_empty());
        assert!(materialization.rejected_capabilities.is_empty());
        assert_eq!(
            materialized_tool_names(&materialization),
            vec!["mcp_resend_domains", "mcp_resend_send"]
        );

        let bindings = crud_store
            .list_turn_mcp_bindings("turn_mcp_implicit_policy")
            .await
            .expect("turn MCP bindings should load");
        assert_eq!(bindings.len(), 2);
        for binding in bindings {
            assert_eq!(binding.selection_reason, MCP_SELECTION_IMPLICIT_POLICY);
            assert_eq!(binding.capability_id, None);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_selection_rules_explicit_tool_attaches_only_selected_tool_when_implicit_false() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send", "domains"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, false).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_explicit_tool_only".to_owned(),
                explicit_servers: Vec::new(),
                explicit_tools: vec![tool_ref("mcp-tool:workspace:resend:send", "resend", "send")],
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.rejected_capabilities.is_empty());
        assert_eq!(materialization.accepted_capabilities.len(), 1);
        assert_eq!(
            materialized_tool_names(&materialization),
            vec!["mcp_resend_send"]
        );

        let bindings = crud_store
            .list_turn_mcp_bindings("turn_mcp_explicit_tool_only")
            .await
            .expect("turn MCP bindings should load");
        assert_eq!(bindings.len(), 1);
        let binding = bindings
            .first()
            .expect("explicit tool should persist one MCP binding");
        assert_eq!(binding.raw_tool_name, "send");
        assert_eq!(binding.selection_reason, MCP_SELECTION_EXPLICIT_CAPABILITY);
        assert_eq!(
            binding.capability_id.as_deref(),
            Some("mcp-tool:workspace:resend:send")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_selection_rules_explicit_server_expands_all_tools_when_implicit_false() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send", "domains"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, false).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_explicit_server_all_tools".to_owned(),
                explicit_servers: vec![server_ref("mcp-server:workspace:resend", "resend")],
                explicit_tools: Vec::new(),
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.rejected_capabilities.is_empty());
        assert_eq!(materialization.accepted_capabilities.len(), 1);
        assert_eq!(
            materialized_tool_names(&materialization),
            vec!["mcp_resend_domains", "mcp_resend_send"]
        );

        let bindings = crud_store
            .list_turn_mcp_bindings("turn_mcp_explicit_server_all_tools")
            .await
            .expect("turn MCP bindings should load");
        assert_eq!(bindings.len(), 2);
        for binding in bindings {
            assert_eq!(binding.selection_reason, MCP_SELECTION_EXPLICIT_CAPABILITY);
            assert_eq!(
                binding.capability_id.as_deref(),
                Some("mcp-server:workspace:resend")
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_selection_rules_reject_disabled_server() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send"],
            fail_auth: false,
        }));
        seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", false, false).await;

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_disabled_server".to_owned(),
                explicit_servers: vec![server_ref("mcp-server:workspace:resend", "resend")],
                explicit_tools: Vec::new(),
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.accepted_capabilities.is_empty());
        assert_eq!(materialization.rejected_capabilities.len(), 1);
        assert_eq!(
            materialization.rejected_capabilities[0].reason,
            TurnCapabilityRejectedReason::DisabledByPolicy
        );
        assert!(
            materialization.rejected_capabilities[0]
                .message
                .contains("disabled by workspace policy")
        );
        assert!(materialization.bundles.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mcp_selection_rules_reject_missing_catalog() {
        let (service, crud_store, workspace_id) = test_mcp_service().await;
        service.set_connector_for_tests(Arc::new(TestMcpRuntimeConnector {
            tools: vec!["send"],
            fail_auth: false,
        }));
        let installation_id =
            seed_mcp_installation(&crud_store, workspace_id.as_str(), "resend", true, false).await;
        service
            .reload_workspace(workspace_id.as_str())
            .await
            .expect("MCP workspace should reload");
        wait_for_catalog(&crud_store, installation_id.as_str()).await;
        crud_store
            .delete_mcp_server_catalog_snapshot(installation_id.as_str())
            .await
            .expect("test MCP catalog snapshot should delete");

        let materialization = service
            .materialize_mcp_tools(AgentMcpMaterializationRequest {
                workspace_id,
                turn_id: "turn_mcp_missing_catalog".to_owned(),
                explicit_servers: vec![server_ref("mcp-server:workspace:resend", "resend")],
                explicit_tools: Vec::new(),
            })
            .await
            .expect("MCP materialization should succeed");

        assert!(materialization.accepted_capabilities.is_empty());
        assert_eq!(materialization.rejected_capabilities.len(), 1);
        assert_eq!(
            materialization.rejected_capabilities[0].reason,
            TurnCapabilityRejectedReason::CatalogMissing
        );
        assert!(
            materialization.rejected_capabilities[0]
                .message
                .contains("no tool catalog snapshot")
        );
        assert!(materialization.bundles.is_empty());
    }
}
