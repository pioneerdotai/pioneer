use crate::classifier::{DefaultErrorClassifier, ErrorClassifier};
use crate::context::{AnyToolResult, LocalShellPayload, ToolCallSource, ToolPayload};
use crate::error::ToolError;
use crate::events::{ToolEventBus, ToolEventTrace};
use crate::orchestrator::ToolOrchestrator;
use crate::output_policy::{DeltaOutputPolicy, ToolDisplayPayload};
use crate::output_projection::{ToolProjectionInput, project_tool_result};
use crate::permissions::PermissionEvaluationContext;
use crate::router::{ToolCall, ToolRouter};
use crate::spec::ExecutionClass;
use pioneer_protocol::TurnExecutionSecuritySnapshot;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

// A nested function proxy stays inside its caller's already-held barrier.
// The identity prevents borrowing admission from an unrelated runtime.
tokio::task_local! { static HELD_TOOL_BARRIER: Option<(usize, bool, String)>; }

// The scope exists only after router resolution of an exact configured CUA
// chain. The orchestrator must classify the current wrapper's Err consistently
// with the final runtime/model boundary, without replacing its permission origin.
pub(crate) fn is_active_computer_use_dispatch(tool_name: &str) -> bool {
    HELD_TOOL_BARRIER
        .try_with(|held| held.as_ref().is_some_and(|(_, _, name)| name == tool_name))
        .unwrap_or(false)
}

const DISPATCH_CANCEL_GRACE_MS: u64 = 1_000;

#[derive(Clone)]
pub struct ToolCallRuntime {
    router: Arc<ToolRouter>,
    orchestrator: Arc<ToolOrchestrator>,
    event_bus: ToolEventBus,
    turn_id: String,
    permission_context: PermissionEvaluationContext,
    execution_security_snapshot: Option<TurnExecutionSecuritySnapshot>,
    workdir: PathBuf,
    environment: BTreeMap<String, String>,
    global_lock: Arc<RwLock<()>>,
    session_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    last_patch_failed: Arc<AtomicBool>,
}

impl ToolCallRuntime {
    pub fn new(
        router: Arc<ToolRouter>,
        orchestrator: Arc<ToolOrchestrator>,
        event_bus: ToolEventBus,
        turn_id: impl Into<String>,
        permission_context: PermissionEvaluationContext,
        workdir: PathBuf,
    ) -> Self {
        let turn_id = turn_id.into();
        assert!(
            permission_context.workspace_id.is_some()
                && permission_context.thread_id.is_some()
                && permission_context.turn_id.as_deref() == Some(turn_id.as_str()),
            "tool runtime requires a resolved permission context for its turn"
        );
        Self {
            router,
            orchestrator,
            event_bus,
            turn_id,
            permission_context,
            execution_security_snapshot: None,
            workdir,
            environment: BTreeMap::new(),
            global_lock: Arc::new(RwLock::new(())),
            session_locks: Arc::new(Mutex::new(HashMap::new())),
            last_patch_failed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_environment(mut self, environment: BTreeMap<String, String>) -> Self {
        self.environment = environment;
        self
    }

    pub fn with_execution_security_snapshot(
        mut self,
        snapshot: Option<TurnExecutionSecuritySnapshot>,
    ) -> Self {
        self.execution_security_snapshot = snapshot;
        self
    }

    pub fn with_orchestrator(mut self, orchestrator: Arc<ToolOrchestrator>) -> Self {
        self.orchestrator = orchestrator;
        self
    }

    pub fn permission_context(&self) -> &PermissionEvaluationContext {
        &self.permission_context
    }

    pub fn router(&self) -> Arc<ToolRouter> {
        self.router.clone()
    }

    pub fn event_bus(&self) -> ToolEventBus {
        self.event_bus.clone()
    }

    pub async fn execute_tool_call(&self, call: ToolCall) -> Result<AnyToolResult, ToolError> {
        self.execute_tool_call_with_cancellation(call, CancellationToken::new())
            .await
    }

    pub async fn execute_tool_call_with_cancellation(
        &self,
        call: ToolCall,
        cancellation: CancellationToken,
    ) -> Result<AnyToolResult, ToolError> {
        self.execute_tool_call_internal(
            call,
            cancellation,
            ToolCallSource::Model,
            self.workdir.clone(),
            true,
        )
        .await
    }

    /// CLI owns final encoded-frame delivery; retain the typed lease until then.
    pub async fn execute_tool_call_with_deferred_delivery(
        &self,
        call: ToolCall,
        cancellation: CancellationToken,
    ) -> Result<AnyToolResult, ToolError> {
        self.execute_tool_call_internal(
            call,
            cancellation,
            ToolCallSource::Model,
            self.workdir.clone(),
            false,
        )
        .await
    }

    pub async fn execute_nested_tool_call_with_cancellation(
        &self,
        call: ToolCall,
        workdir: PathBuf,
        cancellation: CancellationToken,
    ) -> Result<AnyToolResult, ToolError> {
        self.execute_tool_call_internal(
            call,
            cancellation,
            ToolCallSource::NestedTool,
            workdir,
            false,
        )
        .await
    }

    async fn execute_tool_call_internal(
        &self,
        call: ToolCall,
        cancellation: CancellationToken,
        source: ToolCallSource,
        workdir: PathBuf,
        commit_delivery: bool,
    ) -> Result<AnyToolResult, ToolError> {
        if take_patch_mutation_fallback(self.last_patch_failed.as_ref(), call.tool_name.as_str()) {
            crate::apply_patch::patch_telemetry().record_shell_fallback();
            pioneer_observability::record_patch_mutation_fallback(
                "tool_runtime",
                self.permission_context.permission_profile.mode.as_str(),
                "shell_or_python",
            );
        }
        let trace = self.event_bus.trace_with_id(
            call.trace_id.clone(),
            self.turn_id.clone(),
            call.call_id.clone(),
            call.tool_name.clone(),
        );
        trace
            .emit_started(
                1,
                Some(payload_arguments_json_for_event(
                    call.tool_name.as_str(),
                    &call.payload,
                    call.permission_metadata
                        .dynamic_skill
                        .as_ref()
                        .is_some_and(|metadata| {
                            metadata.kind == crate::spec::DynamicSkillPermissionKind::FunctionProxy
                        }),
                )),
                None,
                call.turn_item_execution_class,
                call.output_policy.clone(),
            )
            .await?;

        let result = match call.execution_class {
            ExecutionClass::Shared => {
                trace.emit_stage(
                    1,
                    "runtime.lock_wait.started",
                    None,
                    Some(serde_json::json!({
                        "execution_class": "shared",
                    })),
                );
                let wait_started = std::time::Instant::now();
                let read_guard = if self.inherited_barrier(&call).is_some() {
                    None
                } else {
                    Some(tokio::select! {
                        _ = cancellation.cancelled() => {
                            let error = ToolError::cancelled("cancelled before shared lock acquisition");
                            trace.emit_stage(1, "runtime.lock_wait.failed", Some(error.to_string()), None);
                            Err(error)
                        }
                        guard = self.global_lock.read() => {
                            trace.emit_stage(
                                1,
                                "runtime.lock_wait.completed",
                                None,
                                Some(serde_json::json!({
                                    "wait_ms": wait_started.elapsed().as_millis() as u64,
                                    "execution_class": "shared",
                                })),
                            );
                            Ok(guard)
                        },
                    }?)
                };

                let output = self
                    .dispatch_call(call.clone(), &cancellation, &trace, source, &workdir)
                    .await;
                drop(read_guard);
                output
            }
            ExecutionClass::MutationScoped => {
                trace.emit_stage(
                    1,
                    "runtime.lock_wait.started",
                    None,
                    Some(serde_json::json!({
                        "execution_class": "mutation_scoped",
                    })),
                );
                let wait_started = std::time::Instant::now();
                // Mutation-scoped calls share the global barrier with ordinary
                // reads, but do not take the exclusive write guard.  The
                // patch engine's canonical target locks serialize only
                // overlapping paths and allow disjoint patches to proceed.
                let read_guard = if self.inherited_barrier(&call).is_some() {
                    None
                } else {
                    Some(tokio::select! {
                        _ = cancellation.cancelled() => {
                            let error = ToolError::cancelled("cancelled before mutation-scoped lock acquisition");
                            trace.emit_stage(1, "runtime.lock_wait.failed", Some(error.to_string()), None);
                            Err(error)
                        }
                        guard = self.global_lock.read() => {
                            trace.emit_stage(
                                1,
                                "runtime.lock_wait.completed",
                                None,
                                Some(serde_json::json!({
                                    "wait_ms": wait_started.elapsed().as_millis() as u64,
                                    "execution_class": "mutation_scoped",
                                })),
                            );
                            Ok(guard)
                        },
                    }?)
                };

                let output = self
                    .dispatch_call(call.clone(), &cancellation, &trace, source, &workdir)
                    .await;
                drop(read_guard);
                output
            }
            ExecutionClass::Exclusive => {
                if self.inherited_barrier(&call) == Some(false) {
                    Err(ToolError::Rejected(
                        "nested tool cannot upgrade a shared barrier to exclusive".to_owned(),
                    ))
                } else {
                    trace.emit_stage(
                        1,
                        "runtime.lock_wait.started",
                        None,
                        Some(serde_json::json!({
                            "execution_class": "exclusive",
                        })),
                    );
                    let wait_started = std::time::Instant::now();
                    let write_guard = if self.inherited_barrier(&call).is_some() {
                        None
                    } else {
                        Some(tokio::select! {
                            _ = cancellation.cancelled() => {
                                let error = ToolError::cancelled("cancelled before exclusive lock acquisition");
                                trace.emit_stage(1, "runtime.lock_wait.failed", Some(error.to_string()), None);
                                Err(error)
                            }
                            guard = self.global_lock.write() => {
                                trace.emit_stage(
                                    1,
                                    "runtime.lock_wait.completed",
                                    None,
                                    Some(serde_json::json!({
                                        "wait_ms": wait_started.elapsed().as_millis() as u64,
                                        "execution_class": "exclusive",
                                    })),
                                );
                                Ok(guard)
                            },
                        }?)
                    };

                    let output = self
                        .dispatch_call(call.clone(), &cancellation, &trace, source, &workdir)
                        .await;
                    drop(write_guard);
                    output
                }
            }
            ExecutionClass::SessionScoped => {
                trace.emit_stage(
                    1,
                    "runtime.lock_wait.started",
                    None,
                    Some(serde_json::json!({
                        "execution_class": "session_scoped",
                    })),
                );
                let wait_started = std::time::Instant::now();
                let read_guard = if self.inherited_barrier(&call).is_some() {
                    None
                } else {
                    Some(tokio::select! {
                        _ = cancellation.cancelled() => {
                            let error = ToolError::cancelled("cancelled before session lock acquisition");
                            trace.emit_stage(1, "runtime.lock_wait.failed", Some(error.to_string()), None);
                            Err(error)
                        }
                        guard = self.global_lock.read() => Ok(guard),
                    }?)
                };

                let key = call
                    .session_scope_key()
                    .unwrap_or_else(|| format!("call:{}", call.call_id));

                let session_lock = {
                    let mut map = tokio::select! {
                        _ = cancellation.cancelled() => {
                            Err(ToolError::cancelled("cancelled before session map acquisition"))
                        }
                        map = self.session_locks.lock() => Ok(map),
                    }?;
                    map.entry(key.clone())
                        .or_insert_with(|| Arc::new(Mutex::new(())))
                        .clone()
                };
                let session_guard = tokio::select! {
                    _ = cancellation.cancelled() => {
                        let error = ToolError::cancelled("cancelled before session-scoped execution");
                        trace.emit_stage(1, "runtime.lock_wait.failed", Some(error.to_string()), None);
                        Err(error)
                    }
                    guard = session_lock.lock() => Ok(guard),
                }?;
                trace.emit_stage(
                    1,
                    "runtime.lock_wait.completed",
                    None,
                    Some(serde_json::json!({
                        "wait_ms": wait_started.elapsed().as_millis() as u64,
                        "execution_class": "session_scoped",
                        "session_key": key,
                    })),
                );

                let output = self
                    .dispatch_call(call.clone(), &cancellation, &trace, source, &workdir)
                    .await;
                drop(session_guard);
                drop(read_guard);
                output
            }
        };

        match result {
            Ok(mut output) => {
                // Keep the real CUA receipt alive through projection and durable
                // publication. A failure/drop before commit revokes observation.
                let delivery = output.output.delivery_receipt();
                record_patch_result(
                    self.last_patch_failed.as_ref(),
                    call.tool_name.as_str(),
                    output.success(),
                );
                let raw_output_text = output.raw_output_text();
                let raw_output_json = output.raw_output_json();
                let arguments = payload_arguments_json(&call.payload);
                let projection_input = ToolProjectionInput {
                    call_id: call.call_id.as_str(),
                    tool_name: call.tool_name.as_str(),
                    arguments: &arguments,
                    raw_output_text: raw_output_text.as_str(),
                    raw_output_json: &raw_output_json,
                    success: output.success(),
                    outcome: &output.outcome,
                    output_policy: &call.output_policy,
                    output_projection: &call.output_projection,
                };
                let projection = if output.output.is_computer_use_output() {
                    crate::output_projection::project_computer_use(projection_input)
                } else {
                    project_tool_result(projection_input)
                };
                if let Some((stream, delta_text)) =
                    output_delta_from_projection(&projection.display, &call.output_policy.deltas)
                    && !matches!(call.tool_name.as_str(), "exec_command" | "write_stdin")
                {
                    trace
                        .emit_output_chunk_delta(1, stream, delta_text, false)
                        .await?;
                }
                output.set_projection(projection);
                trace.emit_stage(
                    1,
                    "runtime.completed",
                    None,
                    Some(serde_json::json!({
                        "success": output.success(),
                    })),
                );
                if let Some(projection) = output.projection() {
                    trace.emit_completed(1, projection).await?;
                }
                self.event_bus.finish_trace(trace.trace_id());
                if let Some(delivery) = delivery {
                    if cancellation.is_cancelled() {
                        return Err(ToolError::cancelled("tool result delivery cancelled"));
                    }
                    if commit_delivery {
                        delivery.commit();
                    }
                }
                Ok(output)
            }
            Err(error) => {
                record_patch_result(
                    self.last_patch_failed.as_ref(),
                    call.tool_name.as_str(),
                    false,
                );
                let outcome = if self.router.computer_use_leaf(&call.tool_name).is_some() {
                    self.router.classify_tool_error(&call.tool_name, &error)
                } else {
                    classify_call_error(&call, &error, source, &workdir)
                };
                trace.emit_stage(1, "runtime.failed", Some(error.to_string()), None);
                trace
                    .emit_failed_with_outcome(
                        1,
                        error.to_string().as_str(),
                        &outcome,
                        call.output_policy,
                    )
                    .await?;
                self.event_bus.finish_trace(trace.trace_id());
                Err(error)
            }
        }
    }

    fn inherited_barrier(&self, call: &ToolCall) -> Option<bool> {
        self.router.computer_use_leaf(&call.tool_name)?;
        let identity = Arc::as_ptr(&self.global_lock) as usize;
        HELD_TOOL_BARRIER
            .try_with(|held| {
                let (owner, exclusive, parent) = held.as_ref()?;
                let origin = call.permission_metadata.nested_dynamic_skills.last()?;
                let configured = self.router.configured_proxy_origin(parent)?;
                (*owner == identity
                    && origin == configured
                    && origin.target_tool.as_deref() == Some(call.tool_name.as_str()))
                .then_some(*exclusive)
            })
            .ok()
            .flatten()
    }

    async fn dispatch_call(
        &self,
        call: ToolCall,
        cancellation: &CancellationToken,
        trace: &ToolEventTrace,
        source: ToolCallSource,
        workdir: &PathBuf,
    ) -> Result<AnyToolResult, ToolError> {
        if self.execution_security_snapshot.is_none() {
            return Err(ToolError::Rejected(
                "missing turn execution security snapshot; refusing to execute tool without resolved sandbox policy".to_owned(),
            ));
        }
        let exclusive = self
            .inherited_barrier(&call)
            .unwrap_or(call.execution_class == ExecutionClass::Exclusive);
        let identity = Arc::as_ptr(&self.global_lock) as usize;
        let cua_path = self.router.computer_use_leaf(&call.tool_name).is_some();
        let parent_name = call.tool_name.clone();
        let dispatch = self.router.dispatch(
            self.orchestrator.as_ref(),
            call,
            source,
            workdir.clone(),
            self.environment.clone(),
            &self.permission_context,
            self.execution_security_snapshot.clone(),
            trace,
            cancellation.clone(),
        );
        let dispatch = async {
            if cua_path {
                HELD_TOOL_BARRIER
                    .scope(Some((identity, exclusive, parent_name)), dispatch)
                    .await
            } else {
                // Clear an ambient CUA scope when entering an unrelated runtime
                // or handler; matching aliases alone cannot inherit its origin.
                HELD_TOOL_BARRIER.scope(None, dispatch).await
            }
        };
        tokio::pin!(dispatch);

        tokio::select! {
            result = &mut dispatch => result,
            _ = cancellation.cancelled() => {
                if !cua_path {
                    return Err(ToolError::cancelled("tool dispatch cancelled"));
                }
                match timeout(Duration::from_millis(DISPATCH_CANCEL_GRACE_MS), &mut dispatch).await {
                    Ok(result) => result,
                    Err(_) => Err(ToolError::cancelled("tool dispatch cancelled")),
                }
            }
        }
    }
}

fn take_patch_mutation_fallback(last_patch_failed: &AtomicBool, tool_name: &str) -> bool {
    tool_name == "exec_command" && last_patch_failed.swap(false, Ordering::AcqRel)
}

fn record_patch_result(last_patch_failed: &AtomicBool, tool_name: &str, success: bool) {
    if tool_name == "apply_patch" {
        last_patch_failed.store(!success, Ordering::Release);
    }
}

fn classify_call_error(
    call: &ToolCall,
    error: &ToolError,
    source: ToolCallSource,
    workdir: &PathBuf,
) -> crate::context::ToolOutcome {
    let invocation = crate::context::ToolInvocation {
        call_id: call.call_id.clone(),
        tool_name: call.tool_name.clone(),
        source,
        payload: call.payload.clone(),
        workdir: workdir.clone(),
        environment: BTreeMap::new(),
        attempt_id: 1,
        idempotency_key: call.idempotency_key.clone(),
        recovery: call.recovery,
        permission_metadata: call.permission_metadata.clone(),
        execution_security_snapshot: None,
        apply_patch_preflight: None,
        cancellation: CancellationToken::new(),
    };
    DefaultErrorClassifier.classify_error(&invocation, error)
}

fn payload_arguments_json(payload: &ToolPayload) -> JsonValue {
    match payload {
        ToolPayload::Function { arguments } => arguments.clone(),
        ToolPayload::Mcp {
            server,
            tool,
            arguments,
            read_only_hint,
            destructive_hint,
            open_world_hint,
        } => serde_json::json!({
            "server": server,
            "tool": tool,
            "arguments": arguments,
            "read_only_hint": read_only_hint,
            "destructive_hint": destructive_hint,
            "open_world_hint": open_world_hint,
        }),
        ToolPayload::LocalShell(LocalShellPayload::ExecCommand(args)) => {
            serde_json::to_value(args).unwrap_or_else(|_| serde_json::json!({}))
        }
        ToolPayload::LocalShell(LocalShellPayload::WriteStdin(args)) => {
            serde_json::to_value(args).unwrap_or_else(|_| serde_json::json!({}))
        }
        ToolPayload::ToolSearch {
            query,
            limit,
            include_hidden,
        } => serde_json::json!({
            "query": query,
            "limit": limit,
            "include_hidden": include_hidden,
        }),
        ToolPayload::Custom { input } => serde_json::json!({ "input": input }),
    }
}

/// Durable ordinary tool events must never carry patch source text.  Keep a
/// bounded audit marker instead; the executor and model-facing result still
/// use the original in-memory payload for the actual operation and its
/// projection.
fn payload_arguments_json_for_event(
    tool_name: &str,
    payload: &ToolPayload,
    function_proxy: bool,
) -> JsonValue {
    let arguments = payload_arguments_json(payload);
    if tool_name != "apply_patch" && !crate::is_computer_use_tool(tool_name) && !function_proxy {
        return arguments;
    }
    let encoded = serde_json::to_vec(&arguments).unwrap_or_default();
    serde_json::json!({
        "redacted": true,
        "input_sha256": hex::encode(Sha256::digest(&encoded)),
        "input_bytes": encoded.len(),
    })
}

fn output_delta_from_projection(
    display: &ToolDisplayPayload,
    policy: &DeltaOutputPolicy,
) -> Option<(pioneer_protocol::ItemDeltaStream, String)> {
    let DeltaOutputPolicy::PersistAndDisplay {
        max_chunk_bytes,
        max_total_bytes,
    } = policy
    else {
        return None;
    };
    let ToolDisplayPayload::Shell {
        stdout,
        stderr,
        aggregated_output,
        ..
    } = display
    else {
        return None;
    };

    let (stream, text) = aggregated_output
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(|value| (pioneer_protocol::ItemDeltaStream::Stdout, value.to_owned()))
        .or_else(|| {
            if let Some(stdout) = stdout.as_deref().filter(|value| !value.is_empty()) {
                return Some((pioneer_protocol::ItemDeltaStream::Stdout, stdout.to_owned()));
            }
            if let Some(stderr) = stderr.as_deref().filter(|value| !value.is_empty()) {
                return Some((pioneer_protocol::ItemDeltaStream::Stderr, stderr.to_owned()));
            }
            None
        })?;

    let max_bytes = (*max_chunk_bytes).min(*max_total_bytes);
    Some((stream, truncate_utf8_bytes(text.as_str(), max_bytes)))
}

fn truncate_utf8_bytes(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut truncated = text[..end].to_owned();
    truncated.push_str("\n... [truncated by output policy]");
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{
        FunctionToolOutput, LocalShellPayload, ToolInvocation, ToolOutput, ToolPayload,
        WriteStdinArgs,
    };
    use crate::registry::{ToolHandler, ToolRegistryBuilder};
    use crate::spec::{ConfiguredToolSpec, PayloadKind, ToolSpec};
    use crate::{
        ToolEventKind, ToolOutputProjectionKind, ToolVisibilitySnapshot,
        dynamic_unknown_output_policy,
    };
    use async_trait::async_trait;
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, sleep};

    struct SleepHandler {
        sleep_ms: u64,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    struct FailingHandler;

    #[test]
    fn shell_fallback_is_attributed_once_to_the_preceding_failed_patch() {
        let state = AtomicBool::new(false);

        record_patch_result(&state, "apply_patch", false);
        assert!(!take_patch_mutation_fallback(&state, "read_file"));
        assert!(take_patch_mutation_fallback(&state, "exec_command"));
        assert!(!take_patch_mutation_fallback(&state, "exec_command"));

        record_patch_result(&state, "apply_patch", true);
        assert!(!take_patch_mutation_fallback(&state, "exec_command"));
    }

    impl SleepHandler {
        fn observe_max(max: &AtomicUsize, value: usize) {
            let mut current = max.load(Ordering::SeqCst);
            while value > current {
                match max.compare_exchange(current, value, Ordering::SeqCst, Ordering::SeqCst) {
                    Ok(_) => return,
                    Err(observed) => current = observed,
                }
            }
        }
    }

    #[async_trait]
    impl ToolHandler for SleepHandler {
        async fn handle(
            &self,
            _invocation: ToolInvocation,
            _trace: crate::events::ToolEventTrace,
        ) -> Result<Box<dyn ToolOutput>, ToolError> {
            let active_now = self.active.fetch_add(1, Ordering::SeqCst).saturating_add(1);
            Self::observe_max(self.max_active.as_ref(), active_now);

            sleep(Duration::from_millis(self.sleep_ms)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);

            Ok(Box::new(FunctionToolOutput::new("ok", true)))
        }
    }

    #[async_trait]
    impl ToolHandler for FailingHandler {
        async fn handle(
            &self,
            _invocation: ToolInvocation,
            _trace: crate::events::ToolEventTrace,
        ) -> Result<Box<dyn ToolOutput>, ToolError> {
            Err(ToolError::execution_failed("boom"))
        }
    }

    fn build_runtime(
        tool_name: &str,
        execution_class: ExecutionClass,
        handler: Arc<SleepHandler>,
    ) -> ToolCallRuntime {
        build_runtime_with_dyn_handler(tool_name, execution_class, handler)
    }

    fn build_runtime_with_dyn_handler(
        tool_name: &str,
        execution_class: ExecutionClass,
        handler: Arc<dyn ToolHandler>,
    ) -> ToolCallRuntime {
        let mut builder = ToolRegistryBuilder::new();
        builder.push_configured_spec(ConfiguredToolSpec::with_output_projection(
            ToolSpec::new(
                tool_name,
                "test tool",
                serde_json::json!({ "type": "object" }),
                PayloadKind::Function,
            ),
            execution_class,
            dynamic_unknown_output_policy(),
            ToolOutputProjectionKind::DynamicGeneric,
        ));
        builder.register_dyn_handler(tool_name, handler);

        runtime_from_registry(builder)
    }

    fn runtime_from_registry(builder: ToolRegistryBuilder) -> ToolCallRuntime {
        let (specs, registry) = builder.build();
        let visibility = ToolVisibilitySnapshot::new(
            specs
                .iter()
                .map(|configured| configured.spec.clone())
                .collect(),
        );
        let event_bus = ToolEventBus::default();
        let router = Arc::new(ToolRouter::new(
            specs,
            registry,
            visibility,
            event_bus.clone(),
            "turn_test",
        ));
        let orchestrator = Arc::new(ToolOrchestrator::default());

        ToolCallRuntime::new(
            router,
            orchestrator,
            event_bus,
            "turn_test",
            crate::PermissionEvaluationContext::for_turn(
                "workspace_test",
                "thread_test",
                "turn_test",
                pioneer_protocol::default_turn_permission_profile_snapshot(),
            ),
            PathBuf::from("."),
        )
        .with_execution_security_snapshot(Some(test_full_access_snapshot()))
    }

    fn test_full_access_snapshot() -> pioneer_protocol::TurnExecutionSecuritySnapshot {
        pioneer_protocol::TurnExecutionSecuritySnapshot::unrestricted_full_access(".", 1)
    }

    fn function_call(tool_name: &str, call_id: &str, class: ExecutionClass) -> ToolCall {
        ToolCall {
            call_id: call_id.to_owned(),
            tool_name: tool_name.to_owned(),
            payload: ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
            execution_class: class,
            turn_item_execution_class: pioneer_protocol::TurnItemExecutionClass::Standard,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            output_policy: crate::output_policy::ToolOutputPolicySnapshot::for_tool_name(tool_name),
            output_projection: crate::output_policy::ToolOutputProjectionKind::Builtin,
            idempotency_key: None,
            trace_id: format!("trace_{call_id}"),
            session_scope_key: None,
        }
    }

    fn session_call(tool_name: &str, call_id: &str, session_id: u64) -> ToolCall {
        ToolCall {
            call_id: call_id.to_owned(),
            tool_name: tool_name.to_owned(),
            payload: ToolPayload::LocalShell(LocalShellPayload::WriteStdin(WriteStdinArgs {
                session_id,
                chars: None,
                yield_time_ms: None,
                max_output_tokens: None,
            })),
            execution_class: ExecutionClass::SessionScoped,
            turn_item_execution_class: pioneer_protocol::TurnItemExecutionClass::Standard,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            output_policy: crate::output_policy::ToolOutputPolicySnapshot::for_tool_name(tool_name),
            output_projection: crate::output_policy::ToolOutputProjectionKind::Builtin,
            idempotency_key: Some(format!("{tool_name}:{call_id}")),
            trace_id: format!("trace_{call_id}"),
            session_scope_key: Some(format!("shell:{session_id}")),
        }
    }

    #[test]
    fn runtime_permission_context_tracks_explicit_turn_profile() {
        let runtime = build_runtime(
            "tool",
            ExecutionClass::Shared,
            Arc::new(SleepHandler {
                sleep_ms: 1,
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
            }),
        );

        assert_eq!(
            runtime.permission_context().turn_id.as_deref(),
            Some("turn_test")
        );
        assert_eq!(
            runtime.permission_context().permission_profile.mode,
            pioneer_protocol::TurnPermissionMode::FullAccess
        );
    }

    #[test]
    fn runtime_can_be_constructed_with_restricted_turn_snapshot() {
        let mut builder = ToolRegistryBuilder::new();
        builder.push_configured_spec(ConfiguredToolSpec::with_output_projection(
            ToolSpec::new(
                "tool",
                "test tool",
                serde_json::json!({ "type": "object" }),
                PayloadKind::Function,
            ),
            ExecutionClass::Shared,
            dynamic_unknown_output_policy(),
            ToolOutputProjectionKind::DynamicGeneric,
        ));
        builder.register_dyn_handler(
            "tool",
            Arc::new(SleepHandler {
                sleep_ms: 1,
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
            }),
        );
        let (specs, registry) = builder.build();
        let visibility = ToolVisibilitySnapshot::new(
            specs
                .iter()
                .map(|configured| configured.spec.clone())
                .collect(),
        );
        let event_bus = ToolEventBus::default();
        let router = Arc::new(ToolRouter::new(
            specs,
            registry,
            visibility,
            event_bus.clone(),
            "turn",
        ));
        let runtime = ToolCallRuntime::new(
            router,
            Arc::new(ToolOrchestrator::default()),
            event_bus,
            "turn",
            crate::PermissionEvaluationContext::for_turn(
                "workspace",
                "thread",
                "turn",
                pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                    pioneer_protocol::TurnPermissionMode::Supervised,
                    pioneer_protocol::TurnPermissionProfileSource::Composer,
                ),
            ),
            PathBuf::from("."),
        );

        assert_eq!(
            runtime.permission_context().workspace_id.as_deref(),
            Some("workspace")
        );
        assert_eq!(
            runtime.permission_context().permission_profile.mode,
            pioneer_protocol::TurnPermissionMode::Supervised
        );
    }

    #[tokio::test]
    async fn shared_calls_run_in_parallel() {
        let max_active = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(SleepHandler {
            sleep_ms: 120,
            active: Arc::new(AtomicUsize::new(0)),
            max_active: max_active.clone(),
        });
        let runtime = build_runtime("shared_tool", ExecutionClass::Shared, handler);

        let runtime_left = runtime.clone();
        let runtime_right = runtime.clone();
        let left = runtime_left.execute_tool_call(function_call(
            "shared_tool",
            "call_left",
            ExecutionClass::Shared,
        ));
        let right = runtime_right.execute_tool_call(function_call(
            "shared_tool",
            "call_right",
            ExecutionClass::Shared,
        ));

        let (left, right) = tokio::join!(left, right);
        assert!(left.is_ok());
        assert!(right.is_ok());
        assert!(
            max_active.load(Ordering::SeqCst) >= 2,
            "shared execution should overlap"
        );
    }

    #[tokio::test]
    async fn mutation_scoped_calls_reach_target_locking_in_parallel() {
        let max_active = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(SleepHandler {
            sleep_ms: 120,
            active: Arc::new(AtomicUsize::new(0)),
            max_active: max_active.clone(),
        });
        let runtime = build_runtime("mutation_tool", ExecutionClass::MutationScoped, handler);

        let runtime_left = runtime.clone();
        let runtime_right = runtime.clone();
        let left = runtime_left.execute_tool_call(function_call(
            "mutation_tool",
            "call_left",
            ExecutionClass::MutationScoped,
        ));
        let right = runtime_right.execute_tool_call(function_call(
            "mutation_tool",
            "call_right",
            ExecutionClass::MutationScoped,
        ));

        let (left, right) = tokio::join!(left, right);
        assert!(left.is_ok());
        assert!(right.is_ok());
        assert!(
            max_active.load(Ordering::SeqCst) >= 2,
            "runtime must not globally serialize mutation-scoped calls"
        );
    }

    #[tokio::test]
    async fn runtime_rejects_tool_execution_without_security_snapshot() {
        let active = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(SleepHandler {
            sleep_ms: 1,
            active: active.clone(),
            max_active: Arc::new(AtomicUsize::new(0)),
        });
        let runtime = build_runtime_with_dyn_handler("tool", ExecutionClass::Shared, handler)
            .with_execution_security_snapshot(None);

        let error = match runtime
            .execute_tool_call(function_call(
                "tool",
                "call_missing_security",
                ExecutionClass::Shared,
            ))
            .await
        {
            Ok(_) => panic!("missing security snapshot should reject tool execution"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("missing turn execution security snapshot"),
            "unexpected error: {error}"
        );
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn exclusive_calls_are_serialized() {
        let max_active = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(SleepHandler {
            sleep_ms: 120,
            active: Arc::new(AtomicUsize::new(0)),
            max_active: max_active.clone(),
        });
        let runtime = build_runtime("exclusive_tool", ExecutionClass::Exclusive, handler);

        let runtime_left = runtime.clone();
        let runtime_right = runtime.clone();
        let left = runtime_left.execute_tool_call(function_call(
            "exclusive_tool",
            "call_left",
            ExecutionClass::Exclusive,
        ));
        let right = runtime_right.execute_tool_call(function_call(
            "exclusive_tool",
            "call_right",
            ExecutionClass::Exclusive,
        ));

        let (left, right) = tokio::join!(left, right);
        assert!(left.is_ok());
        assert!(right.is_ok());
        assert_eq!(
            max_active.load(Ordering::SeqCst),
            1,
            "exclusive execution must be serialized"
        );
    }

    #[tokio::test]
    async fn session_scoped_calls_serialize_only_within_same_session() {
        let max_active_same = Arc::new(AtomicUsize::new(0));
        let same_handler = Arc::new(SleepHandler {
            sleep_ms: 120,
            active: Arc::new(AtomicUsize::new(0)),
            max_active: max_active_same.clone(),
        });
        let same_runtime =
            build_runtime("session_tool", ExecutionClass::SessionScoped, same_handler);

        let same_left_runtime = same_runtime.clone();
        let same_right_runtime = same_runtime.clone();
        let same_left =
            same_left_runtime.execute_tool_call(session_call("session_tool", "call_same_1", 10));
        let same_right =
            same_right_runtime.execute_tool_call(session_call("session_tool", "call_same_2", 10));
        let (same_left, same_right) = tokio::join!(same_left, same_right);
        assert!(same_left.is_ok());
        assert!(same_right.is_ok());
        assert_eq!(
            max_active_same.load(Ordering::SeqCst),
            1,
            "same session id must not run concurrently"
        );

        let max_active_diff = Arc::new(AtomicUsize::new(0));
        let diff_handler = Arc::new(SleepHandler {
            sleep_ms: 120,
            active: Arc::new(AtomicUsize::new(0)),
            max_active: max_active_diff.clone(),
        });
        let diff_runtime =
            build_runtime("session_tool", ExecutionClass::SessionScoped, diff_handler);

        let diff_left_runtime = diff_runtime.clone();
        let diff_right_runtime = diff_runtime.clone();
        let diff_left =
            diff_left_runtime.execute_tool_call(session_call("session_tool", "call_diff_1", 11));
        let diff_right =
            diff_right_runtime.execute_tool_call(session_call("session_tool", "call_diff_2", 12));
        let (diff_left, diff_right) = tokio::join!(diff_left, diff_right);
        assert!(diff_left.is_ok());
        assert!(diff_right.is_ok());
        assert!(
            max_active_diff.load(Ordering::SeqCst) >= 2,
            "different sessions should execute in parallel"
        );
    }

    #[tokio::test]
    async fn emits_events_for_successful_call() {
        let handler = Arc::new(SleepHandler {
            sleep_ms: 10,
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        });
        let runtime = build_runtime("event_tool", ExecutionClass::Shared, handler);
        let mut events = runtime.event_bus().subscribe();

        let result = runtime
            .execute_tool_call(function_call(
                "event_tool",
                "call_events",
                ExecutionClass::Shared,
            ))
            .await;
        assert!(result.is_ok());

        let mut seen_started = false;
        let mut seen_delta = false;
        let mut seen_completed = false;
        let mut completed_meta = None;
        let mut last_seq = 0u64;

        for _ in 0..16 {
            let event = events.recv().await.expect("event");
            assert!(
                event.observation.event_seq > last_seq,
                "event sequence should be monotonic within trace"
            );
            last_seq = event.observation.event_seq;

            match event.kind() {
                ToolEventKind::CallStarted => seen_started = true,
                ToolEventKind::Heartbeat => {}
                ToolEventKind::PermissionAudit => {}
                ToolEventKind::OutputDelta => seen_delta = true,
                ToolEventKind::CallCompleted => {
                    seen_completed = true;
                    completed_meta = Some(event.payload);
                    break;
                }
                ToolEventKind::CallFailed => {}
            }
        }

        assert!(seen_started, "expected call started lifecycle event");
        assert!(
            !seen_delta,
            "non-shell tools must not emit raw output delta events"
        );
        assert!(seen_completed, "expected call completed lifecycle event");
        let completed_payload = completed_meta.expect("completion payload should be present");
        let completed_json =
            serde_json::to_value(completed_payload).expect("completion payload should serialize");
        assert!(completed_json.get("llmView").is_some());
        assert!(completed_json.get("display").is_some());
        assert!(completed_json.get("storage").is_some());
        assert!(completed_json.get("output_json").is_none());
        assert!(completed_json.get("output_text").is_none());
    }

    #[tokio::test]
    async fn successful_call_does_not_publish_internal_pipeline_stages() {
        let handler = Arc::new(SleepHandler {
            sleep_ms: 10,
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        });
        let runtime = build_runtime("pipeline_tool", ExecutionClass::Shared, handler);
        let mut events = runtime.event_bus().subscribe();

        let tool_call = runtime
            .router()
            .build_tool_call(crate::RawToolCall {
                call_id: "call_pipeline".to_owned(),
                tool_name: "pipeline_tool".to_owned(),
                arguments: "{}".to_owned(),
            })
            .expect("tool call parse should succeed");

        let result = runtime.execute_tool_call(tool_call).await;
        assert!(result.is_ok());

        let mut application_stages = Vec::new();
        let mut last_seq = 0u64;
        for _ in 0..8 {
            let event = events.recv().await.expect("event must be emitted");
            if event.call_id != "call_pipeline" {
                continue;
            }
            assert!(
                event.observation.event_seq > last_seq,
                "event sequence must be monotonic for one trace"
            );
            last_seq = event.observation.event_seq;

            application_stages.push(event.observation.pipeline_stage.clone());
            if matches!(
                event.kind(),
                ToolEventKind::CallCompleted | ToolEventKind::CallFailed
            ) {
                break;
            }
        }

        assert_eq!(
            application_stages,
            vec![
                "runtime.call.started",
                "permission.audit",
                "runtime.call.completed"
            ],
            "ToolEventBus must carry product lifecycle and permission audit events only"
        );
    }

    #[tokio::test]
    async fn failed_call_emits_started_and_failed_without_internal_stages() {
        let runtime = build_runtime_with_dyn_handler(
            "failing_tool",
            ExecutionClass::Shared,
            Arc::new(FailingHandler),
        );
        let mut events = runtime.event_bus().subscribe();

        let result = runtime
            .execute_tool_call(function_call(
                "failing_tool",
                "call_failed",
                ExecutionClass::Shared,
            ))
            .await;
        assert!(result.is_err());

        let mut application_stages = Vec::new();
        for _ in 0..8 {
            let event = events.recv().await.expect("event must be emitted");
            if event.call_id != "call_failed" {
                continue;
            }
            application_stages.push(event.observation.pipeline_stage.clone());
            if matches!(event.kind(), ToolEventKind::CallFailed) {
                break;
            }
        }

        assert_eq!(
            application_stages,
            vec![
                "runtime.call.started",
                "permission.audit",
                "runtime.call.failed"
            ],
            "failed calls must terminalize through product lifecycle and permission audit events only"
        );
    }

    struct NestedBarrierHandler {
        runtime: std::sync::Mutex<Option<ToolCallRuntime>>,
        entered: tokio::sync::Notify,
        proceed: tokio::sync::Notify,
        leaves: AtomicUsize,
        nested_class: ExecutionClass,
        nested_polled: tokio::sync::Notify,
    }
    #[async_trait]
    impl ToolHandler for NestedBarrierHandler {
        async fn handle(
            &self,
            invocation: ToolInvocation,
            _trace: ToolEventTrace,
        ) -> Result<Box<dyn ToolOutput>, ToolError> {
            if invocation.source == ToolCallSource::NestedTool {
                self.leaves.fetch_add(1, Ordering::SeqCst);
                return Ok(Box::new(FunctionToolOutput::new("leaf", true)));
            }
            self.entered.notify_one();
            self.proceed.notified().await;
            let runtime = self.runtime.lock().unwrap().as_ref().unwrap().clone();
            let mut call = runtime.router.build_tool_call(crate::RawToolCall {
                tool_name: "cua_type_text".to_owned(),
                call_id: "nested".to_owned(),
                arguments: "{}".to_owned(),
            })?;
            call.execution_class = self.nested_class;
            if let Some(origin) = invocation.permission_metadata.dynamic_skill.clone() {
                call.permission_metadata.nested_dynamic_skills.push(origin);
            }
            let nested = runtime.execute_nested_tool_call_with_cancellation(
                call,
                PathBuf::from("."),
                invocation.cancellation,
            );
            tokio::pin!(nested);
            let nested = std::future::poll_fn(|cx| {
                let result = nested.as_mut().poll(cx);
                self.nested_polled.notify_one();
                result
            })
            .await?;
            Ok(nested.output)
        }
    }
    #[tokio::test]
    async fn nested_proxy_inherits_barrier_while_writer_waits() {
        for (class, nested_class, cua_proxy, permitted) in [
            (ExecutionClass::Shared, ExecutionClass::Shared, true, true),
            (
                ExecutionClass::Exclusive,
                ExecutionClass::Shared,
                true,
                true,
            ),
            (
                ExecutionClass::Shared,
                ExecutionClass::Exclusive,
                true,
                false,
            ),
            (ExecutionClass::Shared, ExecutionClass::Shared, false, false),
        ] {
            let handler = Arc::new(NestedBarrierHandler {
                runtime: std::sync::Mutex::new(None),
                entered: tokio::sync::Notify::new(),
                proceed: tokio::sync::Notify::new(),
                leaves: AtomicUsize::new(0),
                nested_class,
                nested_polled: tokio::sync::Notify::new(),
            });
            let mut builder = ToolRegistryBuilder::new();
            let mut outer_spec = ToolSpec::new(
                "nested_tool",
                "proxy",
                serde_json::json!({"type":"object"}),
                PayloadKind::Function,
            );
            if cua_proxy {
                outer_spec.permission_metadata.dynamic_skill =
                    Some(crate::DynamicSkillPermissionMetadata {
                        kind: crate::DynamicSkillPermissionKind::FunctionProxy,
                        skill_id: pioneer_protocol::SkillId::new("SSSSSSSSSSSSSSSSSSSSS").unwrap(),
                        skill_owner: None,
                        skill_slug: "barrier".into(),
                        skill_fingerprint: "v1".into(),
                        source_kind: "user".into(),
                        trust_level: "user".into(),
                        target_tool: Some("cua_type_text".into()),
                        configured_method: None,
                        configured_url: None,
                    });
            }
            for (spec, execution_class) in [
                (outer_spec, class),
                (
                    ToolSpec::new(
                        "cua_type_text",
                        "leaf",
                        serde_json::json!({"type":"object"}),
                        PayloadKind::Function,
                    ),
                    nested_class,
                ),
            ] {
                let name = spec.name.clone();
                builder.push_configured_spec(ConfiguredToolSpec::new(
                    spec,
                    execution_class,
                    dynamic_unknown_output_policy(),
                ));
                builder.register_dyn_handler(&name, handler.clone());
            }
            let runtime = runtime_from_registry(builder);
            let mut events = runtime.event_bus().subscribe();
            *handler.runtime.lock().unwrap() = Some(runtime.clone());
            let executing = runtime.clone();
            let cancellation = CancellationToken::new();
            let outer_cancel = cancellation.clone();
            let outer = tokio::spawn(async move {
                let call = executing
                    .router
                    .build_tool_call(crate::RawToolCall {
                        tool_name: "nested_tool".into(),
                        call_id: "outer".into(),
                        arguments: "{}".into(),
                    })
                    .unwrap();
                executing
                    .execute_tool_call_with_cancellation(call, outer_cancel)
                    .await
            });
            handler.entered.notified().await;
            let (queued_tx, queued_rx) = tokio::sync::oneshot::channel();
            let lock = runtime.global_lock.clone();
            let writer = tokio::spawn(async move {
                let pending = lock.write();
                tokio::pin!(pending);
                std::future::poll_fn(|cx| {
                    assert!(pending.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                let _ = queued_tx.send(());
                drop(pending.await);
            });
            queued_rx.await.unwrap();
            handler.proceed.notify_one();
            if !cua_proxy {
                // The unrelated edge must wait behind the queued writer rather
                // than borrow its caller's admission. Cancellation breaks that wait.
                handler.nested_polled.notified().await;
                cancellation.cancel();
            }
            let result = timeout(Duration::from_secs(1), outer)
                .await
                .expect("nested dispatch must not wait behind its own queued writer")
                .unwrap();
            assert_eq!(result.is_ok(), permitted);
            assert_eq!(
                handler.leaves.load(Ordering::SeqCst),
                usize::from(permitted)
            );
            writer.await.unwrap();
            let mut started = 0;
            let mut terminal = 0;
            while let Ok(event) = events.try_recv() {
                match event.kind() {
                    ToolEventKind::CallStarted => started += 1,
                    ToolEventKind::CallCompleted | ToolEventKind::CallFailed => terminal += 1,
                    _ => {}
                }
            }
            assert!(started >= 1);
            if cua_proxy {
                assert_eq!(started, terminal, "all started CUA calls must terminate");
            }
            *handler.runtime.lock().unwrap() = None;
        }
    }
}
