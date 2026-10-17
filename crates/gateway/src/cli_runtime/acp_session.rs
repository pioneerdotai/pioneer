//! ACP transport adapter for the existing CLI lifecycle and admitted turn path.
//! All external ids are scoped by this immutable process instance. A prompt is
//! staged at start_turn and sent only at the existing persisted activation fence.
use super::{
    acp_mcp::AcpMcpBridge,
    continuation::{CliMcpSessionLaunch, CliProviderContinuation, CliSessionLaunchSpec},
    manager::*,
    mcp::{limits::CliMcpRuntimeLimits, supervisor::CliMcpBridgeSupervisor},
    session_instance::CliSessionInstanceId,
};
use crate::{
    mcp_service::McpService,
    patch_history_observer::{SqlitePatchObserver, reserve_patch_history_capacity},
    turn_mcp::invoker::TurnMcpInvoker,
};
use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use pioneer_cli_agent_runtime::{
    acp::{
        self,
        reducer::{Context as ReductionContext, Reducer},
        schema,
        transport::{RequestId, TransportLimits},
    },
    event::*,
};
use pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig;
use pioneer_protocol::{AgentDurableEvent, TurnExecutionSecuritySnapshot};
use pioneer_tools::{
    OrchestratorPolicy, PermissionEvaluationContext, ToolCallSource, ToolEventBus, ToolInvocation,
    ToolOrchestrator, ToolPayload, handlers::ProcessTerminal,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct AcpRuntimeServices {
    pub(crate) store: Arc<pioneer_crud::CrudStore>,
    pub(crate) mcp: Arc<McpService>,
    pub(crate) execution_owner_id: Arc<str>,
}

struct AdmittedTurn {
    pioneer_id: String,
    pioneer_thread: String,
    owner_generation: u64,
    security: TurnExecutionSecuritySnapshot,
    native_session: String,
    native_turn: String,
    request: Option<RequestId>,
    staged: Option<(schema::PromptRequest, acp::ConfigurationVersion)>,
    reducer: Reducer,
    media_seen: HashMap<String, Value>,
    media_bytes: usize,
    cancellation: CancellationToken,
}
struct HumanWait {
    request: acp::CallbackRequest,
    response: oneshot::Sender<schema::Result<Value>>,
}
struct ScopedTerminal {
    turn_id: String,
    terminal: Arc<ProcessTerminal>,
    released: bool,
    monitor: Option<JoinHandle<()>>,
}
struct State {
    admitted: Option<AdmittedTurn>,
    session_reducers: HashMap<String, Reducer>,
    pending_compactions: Vec<schema::SessionNotification>,
    pending: HashMap<RequestId, HumanWait>,
    terminals: HashMap<String, ScopedTerminal>,
    session: Option<String>,
    retiring: bool,
    continuation_requested: bool,
    commands: Vec<schema::AvailableCommand>,
    session_info: Option<schema::SessionInfoUpdate>,
    usage: Option<schema::UsageUpdate>,
    notice: Option<schema::Notice>,
    completed_elicitations: Vec<String>,
    closed_prompts: std::collections::HashSet<RequestId>,
    additional_directories: Vec<String>,
}
struct SessionHost {
    instance: CliSessionInstanceId,
    weak: std::sync::Weak<SessionHost>,
    projection_order: AsyncMutex<()>,
    snapshot_order: Mutex<u64>,
    services: AcpRuntimeServices,
    state: Mutex<State>,
    events: mpsc::Sender<RuntimeEvent>,
    stopped: CancellationToken,
}

fn wire_error(error: impl std::fmt::Display) -> schema::Error {
    // Local error text may contain paths but never credentials or environment.
    schema::Error::new(-32603, error.to_string())
}
fn encode(value: impl serde::Serialize) -> schema::Result<Value> {
    serde_json::to_value(value).map_err(wire_error)
}

impl SessionHost {
    fn buffer_compaction(
        state: &mut State,
        update: schema::SessionNotification,
    ) -> schema::Result<()> {
        let size = state
            .pending_compactions
            .iter()
            .map(|n| serde_json::to_vec(n).map(|v| v.len()).unwrap_or(usize::MAX))
            .fold(0usize, usize::saturating_add);
        if state.pending_compactions.len() >= 256
            || serde_json::to_vec(&update).map_err(wire_error)?.len()
                > (1024usize * 1024).saturating_sub(size)
        {
            return Err(schema::Error::new(
                -32603,
                "ACP unanchored compaction budget exceeded",
            ));
        }
        state.pending_compactions.push(update);
        Ok(())
    }

    async fn flush_pending_compactions(&self, session: &str) -> schema::Result<()> {
        let _order = self.projection_order.lock().await;
        let entries = {
            let mut state = self.state.lock().expect("ACP host state");
            if state
                .admitted
                .as_ref()
                .is_some_and(|active| active.request.is_some())
            {
                return Ok(());
            }
            if state.admitted.is_none() && !state.session_reducers.contains_key(session) {
                return Ok(());
            }
            let (pending, other): (Vec<_>, Vec<_>) = std::mem::take(&mut state.pending_compactions)
                .into_iter()
                .partition(|n| n.session_id.to_string() == session);
            state.pending_compactions = other;
            let retained = state
                .session_reducers
                .iter()
                .filter(|(id, _)| id.as_str() != session)
                .fold((0usize, 0usize), |(e, b), (_, r)| {
                    (
                        e.saturating_add(r.entity_count()),
                        b.saturating_add(r.retained_bytes()),
                    )
                });
            let reducer = if let Some(active) = state.admitted.as_mut() {
                &mut active.reducer
            } else {
                state
                    .session_reducers
                    .get_mut(session)
                    .expect("retained reducer")
            };
            reducer.limit_retention(
                8192usize.saturating_sub(retained.0),
                (16usize * 1024 * 1024).saturating_sub(retained.1),
            )?;
            let mut entries: Vec<RuntimeCompactionProjection> = Vec::new();
            let mut indexes = HashMap::<String, usize>::new();
            for update in pending {
                let projection = reducer.update(update)?;
                // Replay has no active prompt/lease: no tool, model or parent
                // effects and no expired artifact ingestion. Keep final state
                // per entity and its first appearance order within one ingress.
                for snapshot in projection.snapshots {
                    let pioneer_runtime_events::ExecutionSnapshotEvent::ItemUpdated {
                        notification: updated,
                    } = snapshot;
                    let started = projection.durable.iter().find_map(|event| match event {
                        AgentDurableEvent::ItemStarted { notification }
                            if notification.item.item_id() == updated.item.item_id() =>
                        {
                            Some(notification.clone())
                        }
                        _ => None,
                    });
                    let id = updated.item.item_id().to_owned();
                    if let Some(index) = indexes.get(&id) {
                        entries[*index].updated = updated;
                    } else {
                        indexes.insert(id, entries.len());
                        entries.push(RuntimeCompactionProjection { started, updated });
                    }
                }
            }
            entries
        };
        if !entries.is_empty() {
            self.emit(RuntimeEvent::SessionCompaction { entries })
                .await?;
        }
        Ok(())
    }
    async fn terminal_projection(&self, id: &str) -> schema::Result<()> {
        let _order = self.projection_order.lock().await;
        let projection = {
            let mut state = self.state.lock().expect("ACP host state");
            let Some(terminal) = state.terminals.get(id) else {
                return Ok(());
            };
            let turn = terminal.turn_id.clone();
            let output = terminal.terminal.output().map_err(wire_error)?;
            let Some(active) = state
                .admitted
                .as_mut()
                .filter(|active| active.pioneer_id == turn && active.request.is_some())
            else {
                return Ok(());
            };
            (
                active.reducer.terminal_output(id, &output.output)?,
                active.native_session.clone(),
                active.native_turn.clone(),
            )
        };
        self.projection(projection.0, &projection.1, &projection.2)
            .await
    }

    async fn presentation_changed(&self) -> schema::Result<()> {
        self.emit(RuntimeEvent::SessionStateChanged(
            RuntimeSessionStateChanged {
                status: "acp_session_changed".into(),
                message: None,
                native: None,
            },
        ))
        .await
    }
    async fn emit(&self, event: RuntimeEvent) -> schema::Result<()> {
        self.events
            .send(event)
            .await
            .map_err(|_| schema::Error::internal_error())
    }
    async fn projection(
        &self,
        projection: acp::reducer::Projection,
        session: &str,
        turn: &str,
    ) -> schema::Result<()> {
        let mut projection = projection;
        for media in std::mem::take(&mut projection.media) {
            use sha2::{Digest, Sha256};
            let bytes = serde_json::to_vec(&media.content).map_err(wire_error)?;
            let identity = format!(
                "acp-output:{turn}:{}:{}",
                media.item_id,
                hex::encode(Sha256::digest(&bytes))
            );
            let captured = {
                let mut state = self.state.lock().expect("ACP host state");
                let active = state
                    .admitted
                    .as_mut()
                    .filter(|active| active.native_turn == turn)
                    .ok_or_else(schema::Error::invalid_request)?;
                if let Some(previous) = active.media_seen.get(&identity) {
                    Some((None, previous.clone()))
                } else if active.media_seen.len() >= 64
                    || bytes.len() > (16usize * 1024 * 1024).saturating_sub(active.media_bytes)
                {
                    Some((
                        None,
                        json!({"artifactError":"ACP output media budget exceeded"}),
                    ))
                } else {
                    active.media_bytes += bytes.len();
                    active.media_seen.insert(
                        identity.clone(),
                        json!({"artifactError":"ACP artifact unavailable"}),
                    );
                    Some((
                        Some((
                            active.pioneer_thread.clone(),
                            active.pioneer_id.clone(),
                            active.cancellation.child_token(),
                        )),
                        Value::Null,
                    ))
                }
            };
            let Some((scope, mut payload)) = captured else {
                continue;
            };
            if let Some((thread, pioneer_turn, cancel)) = scope {
                let _guard = cancel.clone().drop_guard();
                payload = match self.authority(session).await {
                    Ok(_) => match self
                        .services
                        .mcp
                        .ingest_acp_output(
                            &self.instance.key().workspace_id,
                            &thread,
                            &pioneer_turn,
                            &media.item_id,
                            identity.clone(),
                            media.content,
                            cancel,
                        )
                        .await
                    {
                        Ok(artifact) => json!({"artifact":artifact}),
                        Err(_) => {
                            json!({"artifactError":"ACP artifact unavailable: content, quota or current authority rejected"})
                        }
                    },
                    Err(_) => json!({"artifactError":"ACP output authority expired"}),
                };
                if let Some(active) = self
                    .state
                    .lock()
                    .expect("ACP host state")
                    .admitted
                    .as_mut()
                    .filter(|active| active.native_turn == turn)
                {
                    active.media_seen.insert(identity.clone(), payload.clone());
                }
            }
            if let Some(message) = payload.get("artifactError").and_then(Value::as_str) {
                let warning = pioneer_protocol::TurnItem::SystemEvent {
                    id: format!(
                        "acp-output-warning:{}",
                        hex::encode(Sha256::digest(identity.as_bytes()))
                    ),
                    level: pioneer_protocol::SystemEventLevel::Warning,
                    message: message.into(),
                    code: Some("acp_output_unavailable".into()),
                    details: None,
                };
                projection
                    .durable
                    .push(pioneer_protocol::AgentDurableEvent::ItemCompleted {
                        notification: pioneer_protocol::ItemCompletedNotification {
                            workspace_id: self.instance.key().workspace_id.clone(),
                            thread_id: self.instance.key().thread_id.clone(),
                            turn_id: turn.into(),
                            item: warning,
                        },
                    });
            }
            projection
                .progress
                .push(pioneer_protocol::AgentProgressEvent::ItemDelta {
                    notification: pioneer_protocol::ItemDeltaNotification {
                        workspace_id: self.instance.key().workspace_id.clone(),
                        thread_id: self.instance.key().thread_id.clone(),
                        turn_id: turn.into(),
                        item_id: media.item_id,
                        delta: String::new(),
                        stream: None,
                        payload: Some(payload),
                        markdown: None,
                        markdown_version: None,
                    },
                });
        }
        for event in projection.runtime_events(session, turn)? {
            self.emit(event).await?;
        }
        Ok(())
    }
    async fn authority(
        &self,
        session: &str,
    ) -> schema::Result<(String, TurnExecutionSecuritySnapshot)> {
        let (turn, thread, generation, snapshot) = {
            let state = self.state.lock().expect("ACP host state");
            let active = state
                .admitted
                .as_ref()
                .filter(|active| {
                    active.native_session == session
                        && active.request.is_some()
                        && !active.cancellation.is_cancelled()
                })
                .ok_or_else(schema::Error::invalid_request)?;
            (
                active.pioneer_id.clone(),
                active.pioneer_thread.clone(),
                active.owner_generation,
                active.security.clone(),
            )
        };
        let execution = self
            .services
            .store
            .get_turn_execution(&turn)
            .await
            .map_err(wire_error)?
            .ok_or_else(schema::Error::invalid_request)?;
        if execution.owner_id != self.services.execution_owner_id.as_ref()
            || execution.owner_generation != generation
            || execution.workspace_id != self.instance.key().workspace_id
            || execution.thread_id != thread
            || !matches!(
                execution.status,
                pioneer_crud::TurnExecutionStatus::Starting
                    | pioneer_crud::TurnExecutionStatus::Running
            )
            || execution.lease_until.timestamp() <= chrono::Utc::now().timestamp()
        {
            return Err(schema::Error::invalid_request());
        }
        // CLI dispatch remains Starting until its ordinary runtime event is
        // committed. Admission is proved by the owned execution AND the exact
        // persisted running binding, never by a peer session id alone.
        let binding = self
            .services
            .store
            .get_cli_runtime_turn_binding(&turn)
            .await
            .map_err(wire_error)?
            .ok_or_else(schema::Error::invalid_request)?;
        let native_turn = self
            .state
            .lock()
            .expect("ACP host state")
            .admitted
            .as_ref()
            .map(|active| active.native_turn.clone())
            .ok_or_else(schema::Error::invalid_request)?;
        if binding.runtime_kind != "acp"
            || binding.runtime_id != self.instance.key().runtime_id
            || binding.workspace_id != self.instance.key().workspace_id
            || binding.thread_id != thread
            || binding.continuation_thread_id != self.instance.key().thread_id
            || binding.native_thread_id != session
            || binding.native_turn_id.as_deref() != Some(native_turn.as_str())
            || binding.status != "running"
        {
            return Err(schema::Error::invalid_request());
        }
        self.services
            .mcp
            .acp_execution_guard(
                &binding.workspace_id,
                &binding.thread_id,
                &turn,
                &binding.runtime_id,
            )
            .await
            .map_err(wire_error)?;
        let current = self
            .services
            .store
            .get_turn_execution_security_snapshot(&turn)
            .await
            .map_err(wire_error)?
            .ok_or_else(schema::Error::invalid_request)?;
        // Consent-driven revisions must be observed; the immutable cap and
        // principal remain fenced by the prepared execution generation.
        if current.snapshot.authority_cap != snapshot.authority_cap
            || current.snapshot.backend != snapshot.backend
        {
            return Err(schema::Error::invalid_request());
        }
        Ok((turn, current.snapshot))
    }
    fn invocation(
        &self,
        _turn: &str,
        name: &str,
        payload: ToolPayload,
        security: TurnExecutionSecuritySnapshot,
        cancellation: CancellationToken,
    ) -> ToolInvocation {
        ToolInvocation {
            call_id: format!("acp-callback-{}", uuid::Uuid::new_v4()),
            tool_name: name.into(),
            source: ToolCallSource::Model,
            payload,
            workdir: PathBuf::from(&security.sandbox.cwd),
            environment: BTreeMap::new(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: Default::default(),
            permission_metadata: Default::default(),
            execution_security_snapshot: Some(security),
            apply_patch_preflight: None,
            cancellation,
        }
    }
    async fn authorize(
        &self,
        turn: &str,
        invocation: &mut ToolInvocation,
    ) -> schema::Result<pioneer_tools::ToolEventTrace> {
        let trace = ToolEventBus::default().start_trace(
            turn,
            invocation.call_id.clone(),
            invocation.tool_name.clone(),
        );
        let thread = self
            .state
            .lock()
            .expect("ACP host state")
            .admitted
            .as_ref()
            .filter(|active| active.pioneer_id == turn)
            .map(|active| active.pioneer_thread.clone())
            .ok_or_else(schema::Error::invalid_request)?;
        let context = PermissionEvaluationContext::for_turn(
            self.instance.key().workspace_id.clone(),
            thread.clone(),
            turn.to_owned(),
            invocation
                .execution_security_snapshot
                .as_ref()
                .expect("captured snapshot")
                .permission_profile
                .clone(),
        );
        ToolOrchestrator::with_approval_broker(
            OrchestratorPolicy::default(),
            self.services.mcp.permission_approval_broker().await,
        )
        .authorize_direct_invocation(invocation, &trace, &context)
        .await
        .map_err(wire_error)?;
        self.services
            .mcp
            .acp_execution_guard(
                &self.instance.key().workspace_id,
                &thread,
                turn,
                &self.instance.key().runtime_id,
            )
            .await
            .map_err(wire_error)?;
        Ok(trace)
    }
    async fn human(
        &self,
        id: RequestId,
        request: acp::CallbackRequest,
        cancelled: CancellationToken,
    ) -> schema::Result<Value> {
        let (method, kind, session, item) = match &request {
            acp::CallbackRequest::RequestPermissionRequest(request) => (
                "session/request_permission",
                "permission_approval",
                Some(request.session_id.to_string()),
                Some(request.tool_call.tool_call_id.to_string()),
            ),
            acp::CallbackRequest::CreateElicitationRequest(request) => (
                "elicitation/create",
                "user_input",
                match request.scope() {
                    schema::ElicitationScope::Session(scope) => Some(scope.session_id.to_string()),
                    _ => None,
                },
                None,
            ),
            _ => return Err(schema::Error::invalid_request()),
        };
        let params = match &request {
            acp::CallbackRequest::RequestPermissionRequest(request) => encode(request)?,
            acp::CallbackRequest::CreateElicitationRequest(request) => encode(request)?,
            _ => unreachable!(),
        };
        let (tx, rx) = oneshot::channel();
        let native_turn = {
            let mut state = self.state.lock().expect("ACP host state");
            if state.pending.len() >= 32 || state.pending.contains_key(&id) {
                return Err(schema::Error::invalid_request());
            }
            let active = state
                .admitted
                .as_ref()
                .filter(|active| active.request.is_some() && !active.cancellation.is_cancelled());
            if let Some(session) = session.as_deref() {
                let active_matches = active.is_some_and(|turn| turn.native_session == session);
                let idle_session = kind == "user_input"
                    && active.is_none()
                    && state.session.as_deref() == Some(session);
                if !active_matches && !idle_session {
                    return Err(schema::Error::invalid_request());
                }
            }
            let turn = active.map(|active| active.native_turn.clone());
            state.pending.insert(
                id.clone(),
                HumanWait {
                    request,
                    response: tx,
                },
            );
            turn
        };
        self.emit(RuntimeEvent::RequestOpened(RuntimeRequestOpened { native_request_id: request_key(&id), native_request_id_json: Some(encode(&id)?), request_kind: kind.into(), native_thread_id: session, native_turn_id: native_turn, native_item_id: item, payload_redacted: Some(json!({"nativeRequestIdJson":id,"title":if kind == "permission_approval" { "Agent permission" } else { "Agent input" },"message":params.get("message").or_else(|| params.pointer("/toolCall/title")),"acp":{"method":method,"params":params}})), native: None })).await?;
        let result = tokio::select! { biased; _ = cancelled.cancelled() => Err(schema::Error::new(-32800, "Request cancelled")), _ = self.stopped.cancelled() => Err(schema::Error::internal_error()), result = rx => result.unwrap_or_else(|_| Err(schema::Error::internal_error())) };
        self.state
            .lock()
            .expect("ACP host state")
            .pending
            .remove(&id);
        self.emit(RuntimeEvent::RequestResolved(RuntimeRequestResolved {
            native_request_id: request_key(&id),
            native: None,
        }))
        .await?;
        result
    }
    async fn clear_terminals(&self, turn: Option<&str>) {
        let terminals = {
            let mut state = self.state.lock().expect("ACP host state");
            let ids = state
                .terminals
                .iter()
                .filter(|(_, terminal)| turn.is_none_or(|turn| terminal.turn_id == turn))
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| state.terminals.remove(&id))
                .collect::<Vec<_>>()
        };
        for terminal in terminals {
            if let Some(monitor) = terminal.monitor {
                monitor.abort();
                let _ = monitor.await;
            }
            let _ = terminal.terminal.release().await;
        }
    }
}
fn patch_session_info(
    current: &mut Option<schema::SessionInfoUpdate>,
    patch: &schema::SessionInfoUpdate,
) {
    let current = current.get_or_insert_with(schema::SessionInfoUpdate::new);
    if !patch.title.is_undefined() {
        current.title = patch.title.clone();
    }
    if !patch.updated_at.is_undefined() {
        current.updated_at = patch.updated_at.clone();
    }
    if patch.meta.is_some() {
        current.meta = patch.meta.clone();
    }
}

fn request_key(id: &RequestId) -> String {
    match id {
        RequestId::String(value) => format!("s:{value}"),
        RequestId::Number(value) => format!("n:{value}"),
    }
}

#[async_trait]
impl acp::Host for SessionHost {
    fn capabilities(&self) -> schema::ClientCapabilities {
        // Builder through the pinned schema also rejects misspelled flags.
        serde_json::from_value(json!({"fs":{"readTextFile":true,"writeTextFile":true},"terminal":true,"elicitation":{"form":{},"url":{}},"session":{"close":{},"notices":{},"compaction":{},"configOptions":{"boolean":{}}},"auth":{"terminal":cfg!(unix)}})).expect("stable ACP client capabilities")
    }
    async fn request(
        &self,
        id: RequestId,
        request: acp::CallbackRequest,
        cancelled: CancellationToken,
    ) -> schema::Result<Value> {
        if matches!(
            request,
            acp::CallbackRequest::RequestPermissionRequest(_)
                | acp::CallbackRequest::CreateElicitationRequest(_)
        ) {
            return self.human(id, request, cancelled).await;
        }
        let active = {
            let state = self.state.lock().expect("ACP host state");
            state
                .admitted
                .as_ref()
                .filter(|active| active.request.is_some())
                .map(|active| active.cancellation.child_token())
                .ok_or_else(schema::Error::invalid_request)?
        };
        let _drop_guard = active.clone().drop_guard();
        tokio::select! {
            biased;
            _ = cancelled.cancelled() => Err(schema::Error::new(-32800, "Request cancelled")),
            _ = self.stopped.cancelled() => Err(schema::Error::internal_error()),
            _ = active.cancelled() => Err(schema::Error::new(-32800, "Prompt cancelled")),
            result = self.execute_callback(id, request, active.clone()) => result,
        }
    }
    async fn update(&self, update: schema::SessionNotification) -> schema::Result<()> {
        let _order = self.projection_order.lock().await;
        let presentation = matches!(
            &update.update,
            schema::SessionUpdate::AvailableCommandsUpdate(_)
                | schema::SessionUpdate::CurrentModeUpdate(_)
                | schema::SessionUpdate::ConfigOptionUpdate(_)
                | schema::SessionUpdate::SessionInfoUpdate(_)
                | schema::SessionUpdate::UsageUpdate(_)
                | schema::SessionUpdate::Notice(_)
        );
        if matches!(
            &update.update,
            schema::SessionUpdate::CompactionUpdate(_)
                | schema::SessionUpdate::CompactionSummaryChunk(_)
        ) {
            let projection = {
                let mut state = self.state.lock().expect("ACP host state");
                if state
                    .session
                    .as_deref()
                    .is_some_and(|id| id != update.session_id.to_string())
                {
                    return Ok(());
                }
                let session_id = update.session_id.to_string();
                // During load replay, buffer in the existing bounded pending
                // lane. Never wait once per entity behind the admission lock.
                if state.session.is_none()
                    || state
                        .pending_compactions
                        .iter()
                        .any(|n| n.session_id == update.session_id)
                {
                    Self::buffer_compaction(&mut state, update)?;
                    return Ok(());
                }
                let retained = state
                    .session_reducers
                    .iter()
                    .filter(|(id, _)| **id != session_id)
                    .fold((0usize, 0usize), |(entities, bytes), (_, reducer)| {
                        (
                            entities.saturating_add(reducer.entity_count()),
                            bytes.saturating_add(reducer.retained_bytes()),
                        )
                    });
                if let Some(active) = state.admitted.as_mut() {
                    Some(active.reducer.update(update)?)
                } else if let Some(reducer) = state.session_reducers.get_mut(&session_id) {
                    reducer.limit_retention(
                        8192usize.saturating_sub(retained.0),
                        (16usize * 1024 * 1024).saturating_sub(retained.1),
                    )?;
                    Some(reducer.update(update)?)
                } else {
                    Self::buffer_compaction(&mut state, update)?;
                    None
                }
            };
            if let Some(mut projection) = projection {
                let mut commentary = acp::reducer::Projection::default();
                let mut compaction_events = Vec::new();
                for event in std::mem::take(&mut projection.durable) {
                    if matches!(&event, AgentDurableEvent::ItemStarted { notification }
                        if matches!(&notification.item, pioneer_protocol::TurnItem::SystemEvent { code: Some(code), .. } if code == "agent_context_compaction"))
                    {
                        compaction_events.push(event);
                    } else {
                        commentary.durable.push(event);
                    }
                }
                projection.durable = compaction_events;
                let scope = self
                    .state
                    .lock()
                    .expect("ACP host state")
                    .admitted
                    .as_ref()
                    .filter(|active| active.request.is_some())
                    .map(|active| (active.native_session.clone(), active.native_turn.clone()));
                if let Some((session, turn)) = scope {
                    commentary.media = std::mem::take(&mut projection.media);
                    self.projection(commentary, &session, &turn).await?;
                }
                // Idle patches may update presentation, but never authorize new
                // artifact ingestion using an expired execution lease.
                for snapshot in projection.snapshots {
                    let pioneer_runtime_events::ExecutionSnapshotEvent::ItemUpdated {
                        notification: updated,
                    } = snapshot;
                    let started = projection.durable.iter().find_map(|event| match event {
                        AgentDurableEvent::ItemStarted { notification }
                            if notification.item.item_id() == updated.item.item_id() =>
                        {
                            Some(notification.clone())
                        }
                        _ => None,
                    });
                    self.emit(RuntimeEvent::SessionCompaction {
                        entries: vec![RuntimeCompactionProjection { started, updated }],
                    })
                    .await?;
                }
            }
            return Ok(());
        }
        let projection = {
            let mut state = self.state.lock().expect("ACP host state");
            // Config and presentation are valid during session creation/load as
            // well as prompt. Replay of conversation items has no new effects.
            if state
                .session
                .as_deref()
                .is_some_and(|id| id != update.session_id.to_string())
            {
                return Ok(());
            }
            match &update.update {
                schema::SessionUpdate::AvailableCommandsUpdate(value) => {
                    state.commands = value.available_commands.clone()
                }
                schema::SessionUpdate::SessionInfoUpdate(value) => {
                    patch_session_info(&mut state.session_info, value);
                }
                schema::SessionUpdate::UsageUpdate(value) => state.usage = Some(value.clone()),
                schema::SessionUpdate::Notice(value) => state.notice = Some(value.clone()),
                _ => {}
            }
            let terminal_outputs = state
                .terminals
                .iter()
                .filter_map(|(id, scoped)| {
                    scoped
                        .terminal
                        .output()
                        .ok()
                        .map(|output| (id.clone(), output.output))
                })
                .collect::<Vec<_>>();
            state
                .admitted
                .as_mut()
                .filter(|active| {
                    active.native_session == update.session_id.to_string()
                        && active.request.is_some()
                })
                .map(|active| {
                    Ok::<_, schema::Error>((
                        {
                            let mut terminal_updates = Vec::new();
                            for (id, output) in &terminal_outputs {
                                terminal_updates
                                    .extend(active.reducer.terminal_output(id, output)?.snapshots);
                            }
                            let mut projection = active.reducer.update(update.clone())?;
                            projection.snapshots.splice(0..0, terminal_updates);
                            projection
                        },
                        active.native_session.clone(),
                        active.native_turn.clone(),
                    ))
                })
                .transpose()?
        };
        if let Some((projection, session, turn)) = projection {
            self.projection(projection, &session, &turn).await?;
        }
        if presentation {
            self.presentation_changed().await?;
        }
        Ok(())
    }
    async fn elicitation_complete(
        &self,
        notification: schema::CompleteElicitationNotification,
    ) -> schema::Result<()> {
        {
            let mut state = self.state.lock().expect("ACP host state");
            let id = notification.elicitation_id.to_string();
            if !state.completed_elicitations.contains(&id) {
                if state.completed_elicitations.len() == 32 {
                    state.completed_elicitations.remove(0);
                }
                state.completed_elicitations.push(id);
            }
        }
        self.presentation_changed().await
    }
    async fn prompt_response(
        &self,
        session: schema::SessionId,
        id: RequestId,
        response: schema::Result<schema::PromptResponse>,
    ) -> schema::Result<()> {
        let _order = self.projection_order.lock().await;
        let active = {
            let mut state = self.state.lock().expect("ACP host state");
            if state.closed_prompts.remove(&id) {
                return Ok(());
            }
            if state.admitted.as_ref().is_none_or(|active| {
                active.native_session != session.to_string() || active.request.as_ref() != Some(&id)
            }) {
                return Err(schema::Error::invalid_request());
            }
            // An auxiliary output read error does not invalidate the peer's
            // known prompt result. Preserve readable output and report the
            // missing part as a canonical warning before terminal delivery.
            let mut output_failed = false;
            let outputs = state
                .terminals
                .iter()
                .filter_map(|(id, terminal)| match terminal.terminal.output() {
                    Ok(output) => Some((id.clone(), output.output)),
                    Err(_) => {
                        output_failed = true;
                        None
                    }
                })
                .collect::<Vec<_>>();
            let active = state.admitted.as_mut().expect("verified turn");
            let mut snapshots = Vec::new();
            for (terminal, output) in outputs {
                snapshots.extend(
                    active
                        .reducer
                        .terminal_output(&terminal, &output)?
                        .snapshots,
                );
            }
            (
                state.admitted.take().expect("verified turn"),
                snapshots,
                output_failed,
            )
        };
        let (mut active, terminal_snapshots, output_failed) = active;
        active.cancellation.cancel();
        let terminal = match response {
            Ok(response) => {
                let (mut projection, final_answer) = active.reducer.finish(&response)?;
                projection.snapshots.splice(0..0, terminal_snapshots);
                if output_failed {
                    projection.durable.push(AgentDurableEvent::ItemCompleted {
                        notification: pioneer_protocol::ItemCompletedNotification {
                            workspace_id: self.instance.key().workspace_id.clone(),
                            thread_id: active.pioneer_thread.clone(),
                            turn_id: active.pioneer_id.clone(),
                            item: pioneer_protocol::TurnItem::SystemEvent {
                                id: format!("acp-terminal-output-warning:{}", active.native_turn),
                                level: pioneer_protocol::SystemEventLevel::Warning,
                                message: "Some terminal output could not be collected".into(),
                                code: Some("acp_terminal_output_unavailable".into()),
                                details: None,
                            },
                        },
                    });
                }
                if let Some(notification) = final_answer {
                    projection
                        .durable
                        .push(AgentDurableEvent::ItemCompleted { notification });
                }
                self.projection(projection, &active.native_session, &active.native_turn)
                    .await?;
                if response.stop_reason == schema::StopReason::Cancelled {
                    RuntimeEvent::TurnInterrupted(RuntimeTurnInterrupted {
                        native_thread_id: Some(active.native_session.clone()),
                        native_turn_id: active.native_turn.clone(),
                        reason: "cancelled".into(),
                        native: None,
                    })
                } else {
                    RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                        native_thread_id: Some(active.native_session.clone()),
                        native_turn_id: active.native_turn.clone(),
                        status: "completed".into(),
                        native: None,
                    })
                }
            }
            Err(error) => RuntimeEvent::TurnFailed(RuntimeTurnFailed {
                native_thread_id: Some(active.native_session.clone()),
                native_turn_id: Some(active.native_turn),
                message: error.message,
                code: Some(error.code.to_string()),
                native: None,
            }),
        };
        self.clear_terminals(Some(&active.pioneer_id)).await;
        active.reducer.retire_prompt()?;
        self.state
            .lock()
            .expect("ACP host state")
            .session_reducers
            .insert(active.native_session.clone(), active.reducer);
        self.emit(terminal).await
    }
    async fn session_closed(&self, session: schema::SessionId) -> schema::Result<()> {
        let _order = self.projection_order.lock().await;
        let matches = self
            .state
            .lock()
            .expect("ACP host state")
            .session
            .as_deref()
            == Some(session.to_string().as_str());
        if matches {
            let active = {
                let mut state = self.state.lock().expect("ACP host state");
                state.session = None;
                state.commands.clear();
                state.session_info = None;
                state.usage = None;
                state.notice = None;
                state.completed_elicitations.clear();
                let active = state.admitted.take();
                if let Some(id) = active.as_ref().and_then(|active| active.request.clone()) {
                    if state.closed_prompts.len() >= 32 {
                        self.stopped.cancel();
                        return Err(schema::Error::internal_error());
                    }
                    state.closed_prompts.insert(id);
                }
                active
            };
            if let Some(mut active) = active {
                active.cancellation.cancel();
                let (projection, _) = active
                    .reducer
                    .finish(&schema::PromptResponse::new(schema::StopReason::Cancelled))?;
                self.projection(projection, &active.native_session, &active.native_turn)
                    .await?;
                self.clear_terminals(Some(&active.pioneer_id)).await;
                active.reducer.retire_prompt()?;
                self.state
                    .lock()
                    .expect("ACP host state")
                    .session_reducers
                    .insert(active.native_session.clone(), active.reducer);
                self.emit(RuntimeEvent::TurnInterrupted(RuntimeTurnInterrupted {
                    native_thread_id: Some(active.native_session),
                    native_turn_id: active.native_turn,
                    reason: "session_closed".into(),
                    native: None,
                }))
                .await?;
            } else {
                self.clear_terminals(None).await;
            }
        }
        Ok(())
    }
    async fn request_cancelled(&self, id: &RequestId) -> schema::Result<()> {
        self.state
            .lock()
            .expect("ACP host state")
            .pending
            .remove(id);
        self.emit(RuntimeEvent::RequestResolved(RuntimeRequestResolved {
            native_request_id: request_key(id),
            native: None,
        }))
        .await
    }
    async fn disconnected(&self) {
        self.stopped.cancel();
        let active = self.state.lock().expect("ACP host state").admitted.take();
        let pending = self
            .state
            .lock()
            .expect("ACP host state")
            .pending
            .drain()
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        for id in pending {
            let _ = self
                .emit(RuntimeEvent::RequestResolved(RuntimeRequestResolved {
                    native_request_id: request_key(&id),
                    native: None,
                }))
                .await;
        }
        self.clear_terminals(None).await;
        let _ = self.presentation_changed().await;
        if let Some(active) = active {
            active.cancellation.cancel();
            if active.request.is_none() {
                return;
            }
            let _ = self
                .emit(RuntimeEvent::Error(RuntimeErrorEvent {
                    native_thread_id: Some(active.native_session),
                    native_turn_id: Some(active.native_turn),
                    message: "ACP connection closed; prompt outcome is unknown".into(),
                    code: Some("acp_outcome_unknown".into()),
                    retryable: false,
                    native: None,
                }))
                .await;
        }
    }
}

impl SessionHost {
    async fn execute_callback(
        &self,
        id: RequestId,
        request: acp::CallbackRequest,
        cancelled: CancellationToken,
    ) -> schema::Result<Value> {
        if matches!(
            request,
            acp::CallbackRequest::RequestPermissionRequest(_)
                | acp::CallbackRequest::CreateElicitationRequest(_)
        ) {
            return self.human(id, request, cancelled).await;
        }
        let raw = encode(&request)?;
        let session = raw
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(schema::Error::invalid_params)?;
        let (turn, security) = self.authority(session).await?;
        // A dropped callback must cancel a mutation still waiting on consent or
        // blocking execution; dispatch itself is allowed to drop this future.
        let operation = cancelled.child_token();
        let _cancel_on_drop = operation.clone().drop_guard();
        match request {
            acp::CallbackRequest::ReadTextFileRequest(request) => {
                let mut invocation = self.invocation(
                    &turn,
                    "read_file",
                    ToolPayload::Function {
                        arguments: json!({"file_path":request.path}),
                    },
                    security.clone(),
                    operation.clone(),
                );
                self.authorize(&turn, &mut invocation).await?;
                let authorized_security = invocation
                    .execution_security_snapshot
                    .as_ref()
                    .ok_or_else(schema::Error::internal_error)?;
                let content = pioneer_tools::handlers::ReadFileHandler
                    .read_text(
                        authorized_security,
                        &request.path,
                        request.line,
                        request.limit,
                        operation,
                    )
                    .await
                    .map_err(wire_error)?;
                Ok(json!({"content":content}))
            }
            acp::CallbackRequest::WriteTextFileRequest(request) => {
                let mut invocation = self.invocation(
                    &turn,
                    "write_text_file",
                    ToolPayload::Custom {
                        input: request.content.clone(),
                    },
                    security,
                    operation,
                );
                pioneer_tools::prepare_full_text_write(
                    &mut invocation,
                    &request.path,
                    request.content,
                )
                .map_err(wire_error)?;
                let trace = self.authorize(&turn, &mut invocation).await?;
                let thread = self
                    .state
                    .lock()
                    .expect("ACP host state")
                    .admitted
                    .as_ref()
                    .filter(|active| active.pioneer_id == turn)
                    .map(|active| active.pioneer_thread.clone())
                    .ok_or_else(schema::Error::invalid_request)?;
                let identity = pioneer_tools::apply_patch::history::InvocationIdentity::new(
                    thread,
                    turn,
                    invocation.call_id.clone(),
                )
                .map_err(wire_error)?;
                let permit = reserve_patch_history_capacity().await.map_err(wire_error)?;
                let observer = SqlitePatchObserver::new(
                    self.services.store.database_connection(),
                    identity.clone(),
                    permit,
                );
                let output = pioneer_tools::handlers::ApplyPatchHandler.handle_with_source_and_observer(invocation, trace, pioneer_tools::apply_patch::file_mutation::PatchRequestSource::NativeFunction, &identity, &observer).await.map_err(wire_error)?;
                if !output.success() {
                    return Err(wire_error(output.raw_text()));
                }
                Ok(json!({}))
            }
            acp::CallbackRequest::CreateTerminalRequest(request) => {
                let cwd = request
                    .cwd
                    .unwrap_or_else(|| PathBuf::from(&security.sandbox.cwd));
                let command = std::iter::once(request.command.clone())
                    .chain(request.args.iter().cloned())
                    .collect::<Vec<_>>();
                let env = request
                    .env
                    .into_iter()
                    .map(|env| (env.name, env.value))
                    .collect::<BTreeMap<_, _>>();
                let mut invocation = self.invocation(
                    &turn,
                    "exec_command",
                    ToolPayload::Function {
                        arguments: json!({"command":command,"workdir":cwd,"tty":false}),
                    },
                    security.clone(),
                    operation.clone(),
                );
                invocation.environment = env.clone();
                self.authorize(&turn, &mut invocation).await?;
                let authorized_security = invocation
                    .execution_security_snapshot
                    .as_ref()
                    .ok_or_else(schema::Error::internal_error)?;
                if operation.is_cancelled() {
                    return Err(schema::Error::new(-32800, "Request cancelled"));
                }
                let mut state = self.state.lock().expect("ACP host state");
                if state.terminals.len() >= 32
                    || state.admitted.as_ref().is_none_or(|active| {
                        active.pioneer_id != turn || active.cancellation.is_cancelled()
                    })
                {
                    return Err(schema::Error::invalid_request());
                }
                let terminal = Arc::new(
                    ProcessTerminal::spawn(
                        request.command,
                        request.args,
                        &cwd,
                        env,
                        request.output_byte_limit,
                        authorized_security,
                    )
                    .map_err(wire_error)?,
                );
                let id = uuid::Uuid::new_v4().to_string();
                let mut changes = terminal.output_changes();
                let weak = self.weak.clone();
                let terminal_id = id.clone();
                let monitor_cancel = state
                    .admitted
                    .as_ref()
                    .expect("admitted terminal")
                    .cancellation
                    .child_token();
                let monitor = tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            biased;
                            _ = monitor_cancel.cancelled() => break,
                            changed = changes.changed() => if changed.is_err() { break; },
                        }
                        let Some(host) = weak.upgrade() else {
                            break;
                        };
                        if host.terminal_projection(&terminal_id).await.is_err() {
                            break;
                        }
                    }
                });
                state.terminals.insert(
                    id.clone(),
                    ScopedTerminal {
                        turn_id: turn,
                        terminal,
                        released: false,
                        monitor: Some(monitor),
                    },
                );
                Ok(json!({"terminalId":id}))
            }
            request => {
                let raw = encode(&request)?;
                let id = raw
                    .get("terminalId")
                    .and_then(Value::as_str)
                    .ok_or_else(schema::Error::invalid_params)?
                    .to_owned();
                let terminal = {
                    let state = self.state.lock().expect("ACP host state");
                    state
                        .terminals
                        .get(&id)
                        .filter(|terminal| terminal.turn_id == turn && !terminal.released)
                        .map(|terminal| terminal.terminal.clone())
                        .ok_or_else(schema::Error::invalid_params)?
                };
                let exit = |exit: pioneer_tools::handlers::ProcessTerminalExit| json!({"exitCode":exit.exit_code,"signal":exit.signal});
                match request {
                    acp::CallbackRequest::TerminalOutputRequest(_) => {
                        let output = terminal.output().map_err(wire_error)?;
                        Ok(
                            json!({"output":output.output,"truncated":output.truncated,"exitStatus":output.exit.map(exit)}),
                        )
                    }
                    acp::CallbackRequest::WaitForTerminalExitRequest(_) => {
                        Ok(exit(terminal.wait(&operation).await.map_err(wire_error)?))
                    }
                    acp::CallbackRequest::KillTerminalRequest(_) => {
                        terminal.kill(&operation).await.map_err(wire_error)?;
                        Ok(json!({}))
                    }
                    acp::CallbackRequest::ReleaseTerminalRequest(_) => {
                        let monitor = {
                            let mut state = self.state.lock().expect("ACP host state");
                            let scoped = state
                                .terminals
                                .get_mut(&id)
                                .ok_or_else(schema::Error::invalid_params)?;
                            scoped.released = true;
                            scoped.monitor.take()
                        };
                        if let Some(monitor) = monitor {
                            monitor.abort();
                            let _ = monitor.await;
                        }
                        terminal.release().await.map_err(wire_error)?;
                        self.terminal_projection(&id).await?;
                        Ok(json!({}))
                    }
                    _ => Err(schema::Error::method_not_found()),
                }
            }
        }
    }
}

pub(crate) struct AcpSession {
    connection: Arc<acp::Connection>,
    cwd: PathBuf,
    host: Arc<SessionHost>,
    bridge: Option<AcpMcpBridge>,
    events: Mutex<Option<mpsc::Receiver<RuntimeEvent>>>,
    prompt_worker: AsyncMutex<Option<JoinHandle<()>>>,
    pending_preparation: AsyncMutex<Option<(String, String, u64, TurnExecutionSecuritySnapshot)>>,
    auth_terminal: AsyncMutex<Option<(String, Arc<acp::auth_terminal::AuthTerminal>)>>,
    skill_tooling: Option<Arc<super::acp_skills::AcpSkillTooling>>,
    skill_runtime: AsyncMutex<Option<(String, pioneer_tools::ToolCallRuntime)>>,
}
impl AcpSession {
    pub(crate) async fn spawn(
        instance: CliSessionInstanceId,
        config: EffectiveGatewayCliAgentRuntimeInstanceConfig,
        spec: &CliSessionLaunchSpec,
        supervisor: Arc<CliMcpBridgeSupervisor>,
        limits: CliMcpRuntimeLimits,
        invoker: Arc<dyn TurnMcpInvoker>,
        services: AcpRuntimeServices,
    ) -> Result<Self> {
        ensure!(
            matches!(
                spec.continuation,
                CliProviderContinuation::AcpSession { .. }
            ),
            "ACP launch requires an ACP continuation"
        );
        let skill_tooling = match &spec.mcp {
            CliMcpSessionLaunch::Acp(projection) => projection.skill_tooling.clone(),
            _ => None,
        };
        let invoker: Arc<dyn TurnMcpInvoker> = match &skill_tooling {
            Some(tooling) => Arc::new(services.mcp.turn_mcp_invoker_with_runtime_view(Arc::new(
                super::acp_skills::AcpSkillRuntimeView {
                    tooling: tooling.clone(),
                    delegate: services.mcp.clone(),
                },
            ))),
            None => invoker,
        };
        let bridge = match &spec.mcp {
            CliMcpSessionLaunch::Acp(projection)
                if !projection.canonical_projection.tools.is_empty() =>
            {
                Some(
                    AcpMcpBridge::prepare(
                        supervisor.clone(),
                        instance.clone(),
                        projection.clone(),
                        limits,
                    )
                    .await?,
                )
            }
            CliMcpSessionLaunch::Acp(_)
            | CliMcpSessionLaunch::Disabled
            | CliMcpSessionLaunch::ManagementOnly => None,
            _ => bail!("ACP cannot consume a native MCP launch projection"),
        };
        let (tx, rx) = mpsc::channel(128);
        let host = Arc::new_cyclic(|weak| SessionHost {
            weak: weak.clone(),
            projection_order: AsyncMutex::new(()),
            snapshot_order: Mutex::new(0),
            instance: instance.clone(),
            services,
            state: Mutex::new(State {
                admitted: None,
                session_reducers: HashMap::new(),
                pending_compactions: Vec::new(),
                pending: HashMap::new(),
                terminals: HashMap::new(),
                session: None,
                retiring: false,
                continuation_requested: false,
                commands: Vec::new(),
                session_info: None,
                usage: None,
                notice: None,
                completed_elicitations: Vec::new(),
                closed_prompts: Default::default(),
                additional_directories: spec
                    .options
                    .additional_directories
                    .iter()
                    .map(|path| path.to_string_lossy().into_owned())
                    .collect(),
            }),
            events: tx,
            stopped: CancellationToken::new(),
        });
        let launch = acp::Launch {
            executable: config.binary_path.clone(),
            args: config
                .app_server_args
                .iter()
                .chain(spec.options.app_server_args.iter())
                .cloned()
                .collect(),
            cwd: spec
                .options
                .cwd
                .clone()
                .context("ACP requires workspace cwd")?,
            env: super::config::acp_launch_environment(&config, &spec.options.env)?,
            env_remove: Vec::new(),
            stderr_ring_lines: 128,
            process_generation: instance.generation(),
        };
        let connection = match acp::Connection::spawn(
            &launch,
            host.clone(),
            TransportLimits {
                frame_bytes: spec
                    .native_event_budget
                    .max_frame_bytes
                    .min(super::context::MAX_CLI_TURN_INPUT_FRAME_BYTES),
                ..TransportLimits::default()
            },
        ) {
            Ok(connection) => Arc::new(connection),
            Err(error) => {
                if let Some(bridge) = &bridge {
                    bridge.close().await;
                }
                return Err(error);
            }
        };
        let startup = async {
            if let Some(bridge) = &bridge {
                bridge
                    .serve(
                        connection
                            .process_id()
                            .await
                            .context("ACP process id unavailable")?,
                        invoker,
                        limits,
                    )
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = startup {
            let _ = connection.close().await;
            if let Some(bridge) = &bridge {
                bridge.close().await;
            }
            return Err(error);
        }
        Ok(Self {
            cwd: launch.cwd.clone(),
            connection,
            host,
            bridge,
            events: Mutex::new(Some(rx)),
            prompt_worker: AsyncMutex::new(None),
            pending_preparation: AsyncMutex::new(None),
            auth_terminal: AsyncMutex::new(None),
            skill_tooling,
            skill_runtime: AsyncMutex::new(None),
        })
    }
    fn servers(&self) -> Result<Vec<schema::McpServer>> {
        self.bridge
            .as_ref()
            .map(AcpMcpBridge::descriptor)
            .transpose()
            .map(|value| value.into_iter().collect())
    }
    fn begin_session(&self) {
        let mut state = self.host.state.lock().expect("ACP host state");
        state.session = None;
        state.continuation_requested = false;
        state.commands.clear();
        state.session_info = None;
        state.usage = None;
        state.notice = None;
        state.completed_elicitations.clear();
    }
    async fn restore_session_compactions(&self, session: &str) -> Result<()> {
        if self
            .host
            .state
            .lock()
            .expect("ACP host state")
            .session_reducers
            .contains_key(session)
        {
            return Ok(());
        }
        let key = self.host.instance.key();
        let items = self
            .host
            .services
            .store
            .list_acp_compaction_items(&key.workspace_id, &key.runtime_id, &key.thread_id, session)
            .await?;
        let Some(first) = items.first() else {
            return Ok(());
        };
        let mut reducer = Reducer::new(
            ReductionContext {
                workspace_id: key.workspace_id.clone(),
                thread_id: first.thread_id.clone(),
                turn_id: first.turn_id.clone(),
                session_id: session.to_owned().into(),
                session_scope: format!(
                    "{}:{}:{}:{}",
                    key.runtime_id,
                    self.host.instance.boot_id().as_str(),
                    self.host.instance.generation(),
                    session
                ),
            },
            8192,
            16 * 1024 * 1024,
        )
        .map_err(wire_error)?;
        for item in items {
            reducer.hydrate_compaction(item).map_err(wire_error)?;
        }
        let mut state = self.host.state.lock().expect("ACP host state");
        ensure!(
            state.admitted.is_none() && !state.retiring,
            "ACP session is stale or busy"
        );
        if state.session_reducers.contains_key(session) {
            return Ok(());
        }
        ensure!(
            state.session_reducers.len() < 32,
            "ACP retained session capacity exceeded"
        );
        let (entities, bytes) =
            state
                .session_reducers
                .values()
                .fold((0usize, 0usize), |(entities, bytes), reducer| {
                    (
                        entities.saturating_add(reducer.entity_count()),
                        bytes.saturating_add(reducer.retained_bytes()),
                    )
                });
        reducer
            .limit_retention(
                8192usize.saturating_sub(entities),
                (16usize * 1024 * 1024).saturating_sub(bytes),
            )
            .map_err(wire_error)?;
        state.session_reducers.insert(session.to_owned(), reducer);
        Ok(())
    }
    fn forget_new_session_compactions(&self, session: &str) {
        let mut state = self.host.state.lock().expect("ACP host state");
        state.session_reducers.remove(session);
    }
    fn directories(&self) -> Vec<PathBuf> {
        self.host
            .state
            .lock()
            .expect("ACP host state")
            .additional_directories
            .iter()
            .map(PathBuf::from)
            .collect()
    }
    async fn snapshot(
        &self,
        session: String,
        cwd: String,
        raw: Value,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        self.host.state.lock().expect("ACP host state").session = Some(session.clone());
        self.host
            .flush_pending_compactions(&session)
            .await
            .map_err(wire_error)?;
        self.host.presentation_changed().await.map_err(wire_error)?;
        Ok(CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id: session,
            cwd: Some(cwd),
            model: None,
            raw,
        })
    }
}

#[async_trait]
impl CLIAgentRuntimeSession for AcpSession {
    async fn close(&self) -> Result<()> {
        self.host.state.lock().expect("ACP host state").retiring = true;
        if let Some(tooling) = &self.skill_tooling {
            tooling.clear().await;
        }
        self.skill_runtime.lock().await.take();
        if let Some((_, terminal)) = self.auth_terminal.lock().await.take() {
            terminal.close().await?;
        }
        let result = self.connection.close().await;
        if let Some(bridge) = &self.bridge {
            bridge.close().await;
        }
        if let Some(worker) = self.prompt_worker.lock().await.take() {
            let _ = worker.await;
        }
        result
    }
    async fn prepare_for_replacement(&self) -> Result<()> {
        {
            let state = self.host.state.lock().expect("ACP host state");
            ensure!(
                state.admitted.is_none() && state.pending.is_empty(),
                "ACP process still owns execution or interaction; replacement refused"
            );
        }
        ensure!(
            self.auth_terminal.lock().await.is_none(),
            "close the ACP auth terminal before replacing the process"
        );
        self.host.state.lock().expect("ACP host state").retiring = true;
        Ok(())
    }
    fn is_closed(&self) -> bool {
        self.connection.is_closed() || self.host.state.lock().expect("ACP host state").retiring
    }
    fn acp_snapshot(&self) -> Result<pioneer_protocol::AcpSessionSnapshot> {
        let mut revision = self
            .host
            .snapshot_order
            .lock()
            .expect("ACP snapshot sequence");
        *revision = revision
            .checked_add(1)
            .context("ACP snapshot sequence exhausted")?;
        let (
            session,
            commands,
            additional_directories,
            session_info,
            usage,
            notice,
            completed_elicitations,
        ) = {
            let state = self.host.state.lock().expect("ACP host state");
            (
                state.session.clone(),
                state.commands.clone(),
                state.additional_directories.clone(),
                state.session_info.clone(),
                state.usage.clone(),
                state.notice.clone(),
                state.completed_elicitations.clone(),
            )
        };
        let closed = self.is_closed();
        let session = if closed { None } else { session };
        let config = session.as_ref().and_then(|id| {
            self.connection
                .configuration(&schema::SessionId::new(id.clone()))
        });
        Ok(pioneer_protocol::AcpSessionSnapshot {
            initial_descriptor_fingerprint: config
                .as_ref()
                .map(|c| c.initial_descriptor_fingerprint.clone())
                .unwrap_or_default(),
            configuration_replay: config
                .as_ref()
                .map(|c| c.configuration_replay.clone())
                .unwrap_or_default(),
            continuation_requested: self
                .host
                .state
                .lock()
                .expect("ACP host state")
                .continuation_requested,
            workspace_id: self.host.instance.key().workspace_id.clone(),
            thread_id: self.host.instance.key().thread_id.clone(),
            runtime_id: self.host.instance.key().runtime_id.clone(),
            generation: self.host.instance.generation(),
            connected: !closed,
            presentation_revision: *revision,
            capabilities: self
                .connection
                .negotiated_capabilities()
                .map_err(wire_error)?,
            auth_methods: self
                .connection
                .auth_methods()
                .map_err(wire_error)?
                .into_iter()
                .filter_map(|method| {
                    let kind = match &method {
                        schema::AuthMethod::Agent(_) => pioneer_protocol::AcpAuthMethodKind::Agent,
                        schema::AuthMethod::Terminal(_) => {
                            pioneer_protocol::AcpAuthMethodKind::Terminal
                        }
                        _ => return None,
                    };
                    Some(pioneer_protocol::AcpAuthMethod {
                        id: method.id().to_string(),
                        name: method.name().to_owned(),
                        description: method.description().map(str::to_owned),
                        kind,
                    })
                })
                .collect(),
            session_id: session,
            version: config
                .as_ref()
                .map(|config| pioneer_protocol::AcpConfigurationVersion {
                    connection_id: config.connection_id.to_string(),
                    revision: config.revision,
                }),
            has_config_options: config
                .as_ref()
                .is_some_and(|config| config.has_config_options),
            config_options: config
                .as_ref()
                .map(|config| config.options.clone())
                .unwrap_or_default(),
            modes: config.and_then(|config| config.modes),
            commands,
            session_info,
            usage,
            notice,
            completed_elicitations,
            additional_directories,
        })
    }
    async fn acp_initialize(
        &self,
        timeout: Duration,
    ) -> Result<pioneer_protocol::AcpSessionSnapshot> {
        tokio::time::timeout(timeout, self.connection.initialize())
            .await
            .context("ACP initialize timed out")?
            .map_err(wire_error)?;
        self.host.presentation_changed().await.map_err(wire_error)?;
        self.acp_snapshot()
    }
    async fn acp_apply_configuration(
        &self,
        session: &str,
        selected: &pioneer_protocol::AcpTurnConfiguration,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<()> {
        let retire_on_drop = self.connection.retirement_guard();
        let transfer = async {
            super::acp_input::apply_configuration(&self.connection, session, selected).await?;
            self.host.presentation_changed().await.map_err(wire_error)?;
            Ok(())
        };
        let result =
            super::acp_input::await_configuration_transfer(timeout, &cancellation, transfer).await;
        match result {
            Ok(()) => {
                retire_on_drop.disarm();
                Ok(())
            }
            Err(error) => {
                self.host.state.lock().expect("ACP host state").retiring = true;
                // Do not join a driver which may be waiting on ordered ingress
                // behind the caller's admission transition. Process termination
                // is owned and awaited; its existing driver drains after release.
                let cleanup = self.connection.retire().await;
                if let Some(bridge) = &self.bridge {
                    bridge.close().await;
                }
                cleanup.context("ACP configuration process retirement failed")?;
                Err(error)
            }
        }
    }
    async fn acp_control(
        &self,
        operation: pioneer_protocol::AcpSessionOperation,
        timeout: Duration,
    ) -> Result<Option<Value>> {
        use pioneer_protocol::AcpSessionOperation as Operation;
        if !matches!(&operation, Operation::TerminalAuthClose { .. })
            && self.connection.negotiated_capabilities().is_err()
        {
            tokio::time::timeout(timeout, self.connection.initialize())
                .await
                .context("ACP initialize timed out")?
                .map_err(wire_error)?;
        }
        ensure!(
            matches!(
                &operation,
                Operation::Snapshot | Operation::Close { .. } | Operation::TerminalAuthClose { .. }
            ) || self
                .host
                .state
                .lock()
                .expect("ACP host state")
                .admitted
                .is_none(),
            "ACP session controls are unavailable during a prompt"
        );
        if matches!(
            &operation,
            Operation::Prepare { .. }
                | Operation::Load { .. }
                | Operation::Resume { .. }
                | Operation::Logout
                | Operation::TerminalAuthStart { .. }
        ) {
            ensure!(
                self.host
                    .state
                    .lock()
                    .expect("ACP host state")
                    .pending
                    .is_empty(),
                "Resolve the current ACP interaction before replacing session state"
            );
        }
        if let Operation::SetOption { session_id, .. }
        | Operation::SetMode { session_id, .. }
        | Operation::Close { session_id } = &operation
        {
            ensure!(
                self.host
                    .state
                    .lock()
                    .expect("ACP host state")
                    .session
                    .as_deref()
                    == Some(session_id.as_str()),
                "ACP control targets a stale session"
            );
        }
        let version = |expected: pioneer_protocol::AcpConfigurationVersion| -> Result<acp::ConfigurationVersion> { Ok(acp::ConfigurationVersion { connection_id: uuid::Uuid::parse_str(&expected.connection_id)?, revision: expected.revision }) };
        let operation = async {
            let result = match operation {
                Operation::Prepare {
                    additional_directories,
                } => {
                    let open = {
                        let state = self.host.state.lock().expect("ACP host state");
                        if state.session.is_some() {
                            ensure!(
                                state.additional_directories == additional_directories,
                                "Close the prepared session before changing additional directories"
                            );
                        }
                        state.session.is_some()
                    };
                    if !open {
                        self.begin_session();
                        let response = self
                            .connection
                            .new_session(
                                schema::NewSessionRequest::new(self.cwd.clone())
                                    .additional_directories(
                                        additional_directories.iter().map(PathBuf::from).collect(),
                                    )
                                    .mcp_servers(self.servers()?),
                            )
                            .await
                            .map_err(wire_error)?;
                        self.forget_new_session_compactions(&response.session_id.to_string());
                        {
                            let mut state = self.host.state.lock().expect("ACP host state");
                            state.session = Some(response.session_id.to_string());
                            state.additional_directories = additional_directories;
                        }
                        Some(serde_json::to_value(response)?)
                    } else {
                        None
                    }
                }
                Operation::SetOption {
                    session_id,
                    expected,
                    config_id,
                    value,
                } => Some(serde_json::to_value(
                    self.connection
                        .set_config_option(
                            schema::SetSessionConfigOptionRequest::new(
                                session_id, config_id, value,
                            ),
                            version(expected)?,
                        )
                        .await
                        .map_err(wire_error)?,
                )?),
                Operation::SetMode {
                    session_id,
                    expected,
                    mode_id,
                } => Some(serde_json::to_value(
                    self.connection
                        .set_mode(
                            schema::SetSessionModeRequest::new(session_id, mode_id),
                            version(expected)?,
                        )
                        .await
                        .map_err(wire_error)?,
                )?),
                Operation::Authenticate { method_id } => Some(serde_json::to_value(
                    self.connection
                        .authenticate(schema::AuthMethodId::new(method_id))
                        .await
                        .map_err(wire_error)?,
                )?),
                Operation::Logout => Some(serde_json::to_value(
                    self.connection.logout().await.map_err(wire_error)?,
                )?),
                Operation::List { cursor, cwd } => Some(serde_json::to_value(
                    self.connection
                        .list_sessions(
                            schema::ListSessionsRequest::new()
                                .cursor(cursor)
                                .cwd(cwd.map(PathBuf::from)),
                        )
                        .await
                        .map_err(wire_error)?,
                )?),
                Operation::Load { session_id } => {
                    self.restore_session_compactions(&session_id).await?;
                    self.begin_session();
                    let response = self
                        .connection
                        .load_session(
                            schema::LoadSessionRequest::new(session_id.clone(), self.cwd.clone())
                                .additional_directories(self.directories())
                                .mcp_servers(self.servers()?),
                        )
                        .await
                        .map_err(wire_error)?;
                    {
                        let mut state = self.host.state.lock().expect("ACP host state");
                        state.session = Some(session_id);
                        state.continuation_requested = true;
                    }
                    Some(serde_json::to_value(response)?)
                }
                Operation::Resume { session_id } => {
                    self.restore_session_compactions(&session_id).await?;
                    self.begin_session();
                    let response = self
                        .connection
                        .resume_session(
                            schema::ResumeSessionRequest::new(session_id.clone(), self.cwd.clone())
                                .additional_directories(self.directories())
                                .mcp_servers(self.servers()?),
                        )
                        .await
                        .map_err(wire_error)?;
                    {
                        let mut state = self.host.state.lock().expect("ACP host state");
                        state.session = Some(session_id);
                        state.continuation_requested = true;
                    }
                    Some(serde_json::to_value(response)?)
                }
                Operation::Delete { session_id } => Some(serde_json::to_value(
                    self.connection
                        .delete_session(schema::DeleteSessionRequest::new(session_id))
                        .await
                        .map_err(wire_error)?,
                )?),
                Operation::Close { session_id } => Some(serde_json::to_value(
                    self.connection
                        .close_session(schema::CloseSessionRequest::new(session_id))
                        .await
                        .map_err(wire_error)?,
                )?),
                Operation::TerminalAuthStart {
                    expected_generation,
                    terminal_id,
                    method_id,
                    columns,
                    rows,
                } => {
                    ensure!(
                        expected_generation == self.host.instance.generation(),
                        "stale ACP auth process"
                    );
                    ensure!(
                        terminal_id.len() == 32
                            && terminal_id
                                .bytes()
                                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-'),
                        "invalid auth invocation id"
                    );
                    let mut terminal = self.auth_terminal.lock().await;
                    ensure!(terminal.is_none(), "ACP auth terminal already open");
                    let generation = self
                        .host
                        .instance
                        .generation()
                        .checked_add(1)
                        .context("ACP auth generation exhausted")?;
                    let launch = self
                        .connection
                        .terminal_auth_invocation(&schema::AuthMethodId::new(method_id), generation)
                        .map_err(wire_error)?;
                    let spawned = tokio::task::spawn_blocking(move || {
                        acp::auth_terminal::AuthTerminal::spawn(launch, columns, rows)
                    })
                    .await??;
                    let id = terminal_id;
                    let snapshot = spawned.snapshot();
                    *terminal = Some((id.clone(), Arc::new(spawned)));
                    Some(json!({"terminal_id":id,"snapshot":snapshot}))
                }
                Operation::TerminalAuthInput { terminal_id, data } => {
                    let terminal = self.auth_terminal.lock().await;
                    let (id, terminal) = terminal.as_ref().context("ACP auth terminal not open")?;
                    ensure!(id == &terminal_id, "stale ACP auth terminal");
                    let accepted_bytes = terminal.input(data.as_bytes())?;
                    Some(
                        json!({"terminal_id":id,"accepted_bytes":accepted_bytes,"snapshot":terminal.snapshot()}),
                    )
                }
                Operation::TerminalAuthOutput { terminal_id } => {
                    let terminal = self.auth_terminal.lock().await;
                    let (id, terminal) = terminal.as_ref().context("ACP auth terminal not open")?;
                    ensure!(id == &terminal_id, "stale ACP auth terminal");
                    Some(json!({"terminal_id":id,"snapshot":terminal.snapshot()}))
                }
                Operation::TerminalAuthClose {
                    terminal_id,
                    expected_generation,
                } => {
                    if expected_generation != self.host.instance.generation() {
                        return Ok(Some(json!({"already_retired":true})));
                    }
                    let mut terminal = self.auth_terminal.lock().await;
                    if let Some((id, owned)) =
                        terminal.as_ref().filter(|(id, _)| id == &terminal_id)
                    {
                        let _ = id;
                        owned.close().await?;
                        self.connection.close().await?;
                        terminal.take();
                        Some(json!({"reconnect_required":true}))
                    } else {
                        // An old lifecycle release cannot close a different
                        // invocation or retire its process generation.
                        Some(json!({"already_closed":true}))
                    }
                }
                Operation::Snapshot => None,
            };
            let session = self
                .host
                .state
                .lock()
                .expect("ACP host state")
                .session
                .clone();
            if let Some(session) = session {
                self.host
                    .flush_pending_compactions(&session)
                    .await
                    .map_err(wire_error)?;
            }
            Ok::<_, anyhow::Error>(result)
        };
        match tokio::time::timeout(timeout, operation).await {
            Ok(result) => result,
            Err(_) => {
                let _ = self.connection.close().await;
                bail!("ACP control timed out; process retired because its outcome is unknown")
            }
        }
    }
    async fn execute_acp_skill_tool(
        &self,
        validated: &crate::turn_mcp::invoker::ValidatedTurnMcpInvocation,
        cancellation: CancellationToken,
    ) -> Result<crate::turn_mcp::result::CanonicalMcpToolResult> {
        let tooling = self
            .skill_tooling
            .as_ref()
            .context("ACP session has no selected skill runtime")?;
        ensure!(
            validated.binding.server_installation_id == super::acp_skills::SKILL_SERVER_ID,
            "Not a skill binding"
        );
        let (session, active_cancel) = {
            let state = self.host.state.lock().expect("ACP host state");
            let active = state
                .admitted
                .as_ref()
                .filter(|active| {
                    active.pioneer_id == validated.invocation.turn_id
                        && active.pioneer_thread == validated.invocation.thread_id
                        && active.request.is_some()
                })
                .context("ACP skill invocation turn is not active")?;
            (
                active.native_session.clone(),
                active.cancellation.child_token(),
            )
        };
        self.host.authority(&session).await.map_err(wire_error)?;
        tooling.revalidate_packages().await?;
        tooling.recheck(&validated.binding.raw_tool_name)?;
        let bindings = self
            .host
            .services
            .store
            .find_turn_skill_bindings(&validated.invocation.turn_id)
            .await?;
        ensure!(
            tooling.skills.iter().all(|skill| bindings
                .iter()
                .any(|binding| binding.skill_id == skill.skill_id
                    && binding.fingerprint == skill.definition.identity.fingerprint
                    && binding.skill_version == skill.definition.identity.version_hint)),
            "Skill runtime is not bound to the immutable turn projection"
        );
        let runtime = self
            .skill_runtime
            .lock()
            .await
            .as_ref()
            .filter(|(turn, _)| turn == &validated.invocation.turn_id)
            .map(|(_, runtime)| runtime.clone())
            .context("ACP skill runtime was retired")?;
        let call = runtime
            .router()
            .build_model_tool_call(pioneer_tools::RawToolCall {
                call_id: validated.invocation.provider_call_id.clone(),
                tool_name: validated.binding.raw_tool_name.clone(),
                arguments: serde_json::to_string(&validated.invocation.arguments)?,
            })
            .await?;
        let operation = cancellation.child_token();
        let _on_drop = operation.clone().drop_guard();
        let started = std::time::Instant::now();
        let output = tokio::select! {
            biased;
            _ = active_cancel.cancelled() => anyhow::bail!("ACP prompt cancelled"),
            output = runtime.execute_tool_call_with_cancellation(call, operation) => output?,
        };
        Ok(crate::turn_mcp::result::CanonicalMcpToolResult {
            content: json!([{ "type":"text", "text":output.model_visible_text() }]),
            structured_content: None,
            is_error: !output.success(),
            duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            meta: None,
        })
    }

    fn acp_can_continue(&self, expected: Option<&str>) -> bool {
        !self.connection.is_closed()
            && (expected.is_some_and(|expected| {
                self.host
                    .state
                    .lock()
                    .expect("ACP host state")
                    .session
                    .as_deref()
                    == Some(expected)
            }) || self
                .connection
                .negotiated_capabilities()
                .is_ok_and(|caps| caps.load_session || caps.session_capabilities.resume.is_some()))
    }
    fn acp_connection(&self) -> Option<Arc<acp::Connection>> {
        Some(self.connection.clone())
    }
    fn take_event_receivers(&self) -> Option<CLIAgentRuntimeEventReceivers> {
        self.events
            .lock()
            .expect("ACP event receiver")
            .take()
            .map(|events| CLIAgentRuntimeEventReceivers {
                process_instance: self.host.instance.clone(),
                runtime_kind: "acp".into(),
                events,
            })
    }
    async fn start_thread(
        &self,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        tokio::time::timeout(timeout, self.connection.initialize())
            .await
            .context("ACP initialize timed out")?
            .map_err(wire_error)?;
        let roots = self
            .host
            .state
            .lock()
            .expect("ACP host state")
            .additional_directories
            .iter()
            .map(PathBuf::from)
            .collect();
        self.begin_session();
        let response = tokio::time::timeout(
            timeout,
            self.connection.new_session(
                schema::NewSessionRequest::new(PathBuf::from(&params.cwd))
                    .additional_directories(roots)
                    .mcp_servers(self.servers()?),
            ),
        )
        .await
        .context("ACP session/new timed out; process generation must be replaced")?
        .map_err(wire_error)?;
        let session = response.session_id.to_string();
        self.forget_new_session_compactions(&session);
        self.snapshot(session, params.cwd, serde_json::to_value(response)?)
            .await
    }
    async fn resume_thread(
        &self,
        id: &str,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        let already_open = self
            .host
            .state
            .lock()
            .expect("ACP host state")
            .session
            .as_deref()
            == Some(id);
        ensure!(
            !self.connection.is_closed(),
            "ACP process closed before continuation"
        );
        if already_open {
            return self
                .snapshot(id.into(), params.cwd, json!({"sessionId":id}))
                .await;
        }
        tokio::time::timeout(timeout, self.connection.initialize())
            .await
            .context("ACP initialize timed out")?
            .map_err(wire_error)?;
        let capabilities = self
            .connection
            .negotiated_capabilities()
            .map_err(wire_error)?;
        self.restore_session_compactions(id).await?;
        self.begin_session();
        let raw = if capabilities.session_capabilities.resume.is_some() {
            serde_json::to_value(
                tokio::time::timeout(
                    timeout,
                    self.connection.resume_session(
                        schema::ResumeSessionRequest::new(
                            id.to_owned(),
                            PathBuf::from(&params.cwd),
                        )
                        .additional_directories(self.directories())
                        .mcp_servers(self.servers()?),
                    ),
                )
                .await
                .context("ACP resume timed out")?
                .map_err(wire_error)?,
            )?
        } else if capabilities.load_session {
            serde_json::to_value(
                tokio::time::timeout(
                    timeout,
                    self.connection.load_session(
                        schema::LoadSessionRequest::new(id.to_owned(), PathBuf::from(&params.cwd))
                            .additional_directories(self.directories())
                            .mcp_servers(self.servers()?),
                    ),
                )
                .await
                .context("ACP load timed out")?
                .map_err(wire_error)?,
            )?
        } else {
            bail!(
                "ACP agent cannot resume this session; create a fresh session from accepted Pioneer context"
            );
        };
        self.snapshot(id.into(), params.cwd, raw).await
    }
    async fn prepare_mcp_turn(
        &self,
        thread: &str,
        turn: &str,
    ) -> Result<Option<CLIAgentRuntimeMcpTurnMetadata>> {
        ensure!(
            self.auth_terminal.lock().await.is_none(),
            "Close the ACP auth terminal and reconnect before submitting a prompt"
        );
        let execution = self
            .host
            .services
            .store
            .get_turn_execution(turn)
            .await?
            .context("ACP execution missing")?;
        ensure!(
            execution.owner_id == self.host.services.execution_owner_id.as_ref()
                && execution.workspace_id == self.host.instance.key().workspace_id
                && execution.thread_id == thread,
            "ACP execution owner mismatch"
        );
        let security = self
            .host
            .services
            .store
            .get_turn_execution_security_snapshot(turn)
            .await?
            .context("ACP security snapshot missing")?
            .snapshot;
        let mut prepared = self.pending_preparation.lock().await;
        ensure!(
            prepared.is_none()
                && self
                    .host
                    .state
                    .lock()
                    .expect("ACP host state")
                    .admitted
                    .is_none(),
            "ACP turn already active"
        );
        if let Some(tooling) = &self.skill_tooling {
            let mut slot = self.skill_runtime.lock().await;
            ensure!(
                slot.is_none(),
                "Previous ACP skill execution is still owned"
            );
            tooling.revalidate_packages().await?;
            let runtime = tooling
                .build_runtime(
                    &self.host.instance.key().workspace_id,
                    thread,
                    turn,
                    &security,
                    self.host.services.mcp.permission_approval_broker().await,
                    self.host
                        .services
                        .mcp
                        .acp_skill_extensions(&self.host.instance, thread, turn)
                        .await?,
                    self.host.services.mcp.clone(),
                )
                .await?;
            *slot = Some((turn.into(), runtime));
        }
        let metadata = if let Some(bridge) = &self.bridge {
            match bridge.prepare_turn(thread, turn).await {
                Ok(metadata) => Some(metadata),
                Err(error) => {
                    if let Some(tooling) = &self.skill_tooling {
                        tooling.clear().await;
                    }
                    self.skill_runtime.lock().await.take();
                    return Err(error);
                }
            }
        } else {
            None
        };
        *prepared = Some((
            turn.into(),
            thread.into(),
            execution.owner_generation,
            security,
        ));
        Ok(metadata)
    }
    async fn start_turn(
        &self,
        params: CLIAgentRuntimeTurnStartParams,
        _timeout: Duration,
    ) -> Result<CLIAgentRuntimeTurnStartSnapshot> {
        let (pioneer_id, pioneer_thread, owner_generation, security) = self
            .pending_preparation
            .lock()
            .await
            .take()
            .context("ACP turn not prepared")?;
        let session = schema::SessionId::new(params.native_thread_id.clone());
        let configuration = self
            .connection
            .configuration(&session)
            .context("ACP session descriptors unavailable")?;
        let mut content = vec![schema::ContentBlock::Text(schema::TextContent::new(
            params.instructions.text(),
        ))];
        content.extend(super::acp_input::prompt_content(params.input)?);
        let native_turn = format!("acp-prompt-{}", uuid::Uuid::new_v4());
        let context = ReductionContext {
            workspace_id: self.host.instance.key().workspace_id.clone(),
            thread_id: pioneer_thread.clone(),
            turn_id: pioneer_id.clone(),
            session_id: session.clone(),
            session_scope: format!(
                "{}:{}:{}:{}",
                self.host.instance.key().runtime_id,
                self.host.instance.boot_id().as_str(),
                self.host.instance.generation(),
                session
            ),
        };
        {
            let mut state = self.host.state.lock().expect("ACP host state");
            ensure!(
                state.admitted.is_none()
                    && state.session.as_deref() == Some(params.native_thread_id.as_str()),
                "ACP session is stale or busy"
            );
            ensure!(
                state
                    .session_reducers
                    .contains_key(&params.native_thread_id)
                    || state.session_reducers.len() < 32,
                "ACP retained session capacity exceeded"
            );
            let mut reducer = if let Some(mut reducer) =
                state.session_reducers.remove(&params.native_thread_id)
            {
                reducer.begin_prompt(context).map_err(wire_error)?;
                reducer
            } else {
                Reducer::new(context, 8192, 16 * 1024 * 1024).map_err(wire_error)?
            };
            let retained = state.session_reducers.values().fold(
                (0usize, 0usize),
                |(entities, bytes), reducer| {
                    (
                        entities.saturating_add(reducer.entity_count()),
                        bytes.saturating_add(reducer.retained_bytes()),
                    )
                },
            );
            reducer
                .limit_retention(
                    8192usize.saturating_sub(retained.0),
                    (16usize * 1024 * 1024).saturating_sub(retained.1),
                )
                .map_err(wire_error)?;
            ensure!(
                state.admitted.is_none()
                    && state.session.as_deref() == Some(params.native_thread_id.as_str()),
                "ACP session is stale or busy"
            );
            let prompt = schema::PromptRequest::new(session, content);
            self.connection
                .validate_prompt_frame(&prompt)
                .map_err(wire_error)?;
            state.admitted = Some(AdmittedTurn {
                pioneer_id,
                pioneer_thread,
                owner_generation,
                security,
                native_session: params.native_thread_id.clone(),
                native_turn: native_turn.clone(),
                request: None,
                staged: Some((prompt, configuration.version())),
                reducer,
                media_seen: HashMap::new(),
                media_bytes: 0,
                cancellation: CancellationToken::new(),
            });
        }
        self.host
            .flush_pending_compactions(&params.native_thread_id)
            .await
            .map_err(wire_error)?;
        Ok(CLIAgentRuntimeTurnStartSnapshot {
            native_thread_id: params.native_thread_id,
            native_turn_id: native_turn,
            raw: json!({"dispatch":"staged"}),
        })
    }
    async fn activate_mcp_turn(
        &self,
        turn: &str,
        native_thread: &str,
        native_turn: &str,
    ) -> Result<()> {
        let mut worker = self.prompt_worker.lock().await;
        if let Some(previous) = worker.take() {
            // Admission proves the previous turn no longer owns this session.
            // Explicit close may finish it before its prompt response arrives.
            // Retain transport correlation, but do not wait on that old caller.
            previous.abort();
            let _ = previous.await;
        }
        let permit = self
            .host
            .events
            .reserve()
            .await
            .context("ACP event consumer closed")?;
        let dispatch = || -> Result<_> {
            let mut state = self.host.state.lock().expect("ACP host state");
            let active = state.admitted.as_mut().context("ACP turn not staged")?;
            ensure!(
                active.pioneer_id == turn
                    && active.native_turn == native_turn
                    && active.native_session == native_thread
                    && !active.cancellation.is_cancelled(),
                "ACP activation binding mismatch"
            );
            let (request, version) = active
                .staged
                .take()
                .context("ACP prompt already dispatched")?;
            let pending = self
                .connection
                .prompt(request, version)
                .map_err(wire_error)?;
            active.request = Some(pending.id().clone());
            // Ordered dispatcher cannot project updates until this mutex is
            // released; the already-reserved ingress slot establishes start.
            permit.send(RuntimeEvent::TurnStarted(RuntimeTurnStarted {
                native_thread_id: Some(native_thread.into()),
                native_turn_id: native_turn.into(),
                native: None,
            }));
            Ok(pending)
        };
        let pending = if let Some(bridge) = &self.bridge {
            bridge
                .activate_and_dispatch(turn, native_thread, native_turn, dispatch)
                .await?
        } else {
            dispatch()?
        };
        *worker = Some(tokio::spawn(async move {
            let _ = pending.receive().await;
        }));
        Ok(())
    }
    async fn terminal_mcp_turn(&self, turn: &str) -> Result<()> {
        if let Some(bridge) = &self.bridge {
            bridge.terminal(turn).await?;
        }
        let active = {
            let mut state = self.host.state.lock().expect("ACP host state");
            if state
                .admitted
                .as_ref()
                .is_some_and(|active| active.pioneer_id == turn && active.request.is_none())
            {
                state.admitted.take()
            } else {
                None
            }
        };
        if let Some(active) = active {
            active.cancellation.cancel();
        }
        if let Some(tooling) = &self.skill_tooling {
            tooling.clear().await;
        }
        self.skill_runtime.lock().await.take();
        self.pending_preparation.lock().await.take();
        Ok(())
    }
    async fn respond_to_request(&self, id: Value, response: Value) -> Result<()> {
        let id: RequestId = serde_json::from_value(id)?;
        let mut state = self.host.state.lock().expect("ACP host state");
        let waiting = state
            .pending
            .get(&id)
            .context("ACP request is no longer pending in this generation")?;
        let response =
            acp::validate_callback_response(&waiting.request, response).map_err(wire_error)?;
        let waiting = state.pending.remove(&id).expect("validated pending");
        waiting
            .response
            .send(Ok(response))
            .map_err(|_| anyhow::anyhow!("ACP request cancelled"))
    }
    async fn fail_request(
        &self,
        id: Value,
        code: i64,
        message: String,
        _data: Option<Value>,
    ) -> Result<()> {
        let id: RequestId = serde_json::from_value(id)?;
        let waiting = self
            .host
            .state
            .lock()
            .expect("ACP host state")
            .pending
            .remove(&id)
            .context("ACP request no longer pending")?;
        waiting
            .response
            .send(Err(schema::Error::new(
                i32::try_from(code).unwrap_or(-32603),
                message,
            )))
            .map_err(|_| anyhow::anyhow!("ACP request cancelled"))
    }
    async fn interrupt_turn(&self, session: Option<&str>, turn: Option<&str>) -> Result<()> {
        let session = {
            let state = self.host.state.lock().expect("ACP host state");
            let active = state.admitted.as_ref().context("ACP turn not active")?;
            ensure!(
                session.is_none_or(|id| id == active.native_session)
                    && turn.is_none_or(|id| id == active.native_turn),
                "ACP cancel binding mismatch"
            );
            active.cancellation.cancel();
            schema::SessionId::new(active.native_session.clone())
        };
        self.connection.cancel(session).map_err(wire_error)?;
        self.host.clear_terminals(None).await;
        Ok(())
    }
    async fn probe_turn_liveness(
        &self,
        session: &str,
        turn: &str,
    ) -> Result<CLIAgentRuntimeTurnLivenessProbe> {
        let state = self.host.state.lock().expect("ACP host state");
        Ok(
            if !self.connection.is_closed()
                && state.admitted.as_ref().is_some_and(|active| {
                    active.native_session == session
                        && active.native_turn == turn
                        && active.request.is_some()
                })
            {
                CLIAgentRuntimeTurnLivenessProbe::ConfirmedActive
            } else {
                CLIAgentRuntimeTurnLivenessProbe::Unavailable
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn session_info_omission_retains_and_null_clears_independently() {
        let mut current = None;
        patch_session_info(
            &mut current,
            &serde_json::from_value(json!({"title":"kept","updatedAt":"2026-01-01T00:00:00Z"}))
                .unwrap(),
        );
        patch_session_info(
            &mut current,
            &serde_json::from_value(json!({"updatedAt":null})).unwrap(),
        );
        let value = serde_json::to_value(current.as_ref().unwrap()).unwrap();
        assert_eq!(value["title"], "kept");
        assert!(value["updatedAt"].is_null());
        patch_session_info(
            &mut current,
            &serde_json::from_value(json!({"title":null})).unwrap(),
        );
        assert!(serde_json::to_value(current.unwrap()).unwrap()["title"].is_null());
    }
}
