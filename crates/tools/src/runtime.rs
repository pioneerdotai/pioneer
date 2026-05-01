use crate::classifier::{DefaultErrorClassifier, ErrorClassifier};
use crate::context::{AnyToolResult, LocalShellPayload, ToolCallSource, ToolPayload};
use crate::error::ToolError;
use crate::events::{ToolEventBus, ToolEventTrace};
use crate::orchestrator::ToolOrchestrator;
use crate::output_policy::{DeltaOutputPolicy, ToolDisplayPayload};
use crate::output_projection::{ToolProjectionInput, project_tool_result};
use crate::router::{ToolCall, ToolRouter};
use crate::spec::ExecutionClass;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct ToolCallRuntime {
    router: Arc<ToolRouter>,
    orchestrator: Arc<ToolOrchestrator>,
    event_bus: ToolEventBus,
    turn_id: String,
    workdir: PathBuf,
    global_lock: Arc<RwLock<()>>,
    session_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl ToolCallRuntime {
    pub fn new(
        router: Arc<ToolRouter>,
        orchestrator: Arc<ToolOrchestrator>,
        event_bus: ToolEventBus,
        turn_id: impl Into<String>,
        workdir: PathBuf,
    ) -> Self {
        Self {
            router,
            orchestrator,
            event_bus,
            turn_id: turn_id.into(),
            workdir,
            global_lock: Arc::new(RwLock::new(())),
            session_locks: Arc::new(Mutex::new(HashMap::new())),
        }
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
        )
        .await
    }

    pub async fn execute_nested_tool_call(
        &self,
        call: ToolCall,
        workdir: PathBuf,
    ) -> Result<AnyToolResult, ToolError> {
        self.execute_nested_tool_call_with_cancellation(call, workdir, CancellationToken::new())
            .await
    }

    pub async fn execute_nested_tool_call_with_cancellation(
        &self,
        call: ToolCall,
        workdir: PathBuf,
        cancellation: CancellationToken,
    ) -> Result<AnyToolResult, ToolError> {
        self.execute_tool_call_internal(call, cancellation, ToolCallSource::NestedTool, workdir)
            .await
    }

    async fn execute_tool_call_internal(
        &self,
        call: ToolCall,
        cancellation: CancellationToken,
        source: ToolCallSource,
        workdir: PathBuf,
    ) -> Result<AnyToolResult, ToolError> {
        let trace = self.event_bus.trace_with_id(
            call.trace_id.clone(),
            self.turn_id.clone(),
            call.call_id.clone(),
            call.tool_name.clone(),
        );
        trace.emit_started(
            1,
            Some(payload_arguments_json(&call.payload)),
            None,
            call.output_policy.clone(),
        );

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
                let read_guard = tokio::select! {
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
                }?;

                let dispatch =
                    self.dispatch_call(call.clone(), &cancellation, &trace, source, &workdir);
                tokio::pin!(dispatch);
                let output = tokio::select! {
                    _ = cancellation.cancelled() => Err(ToolError::cancelled("cancelled during shared execution")),
                    result = &mut dispatch => result,
                };
                drop(read_guard);
                output
            }
            ExecutionClass::Exclusive => {
                trace.emit_stage(
                    1,
                    "runtime.lock_wait.started",
                    None,
                    Some(serde_json::json!({
                        "execution_class": "exclusive",
                    })),
                );
                let wait_started = std::time::Instant::now();
                let write_guard = tokio::select! {
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
                }?;

                let dispatch =
                    self.dispatch_call(call.clone(), &cancellation, &trace, source, &workdir);
                tokio::pin!(dispatch);
                let output = tokio::select! {
                    _ = cancellation.cancelled() => Err(ToolError::cancelled("cancelled during exclusive execution")),
                    result = &mut dispatch => result,
                };
                drop(write_guard);
                output
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
                let read_guard = tokio::select! {
                    _ = cancellation.cancelled() => {
                        let error = ToolError::cancelled("cancelled before session lock acquisition");
                        trace.emit_stage(1, "runtime.lock_wait.failed", Some(error.to_string()), None);
                        Err(error)
                    }
                    guard = self.global_lock.read() => Ok(guard),
                }?;

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

                let dispatch =
                    self.dispatch_call(call.clone(), &cancellation, &trace, source, &workdir);
                tokio::pin!(dispatch);
                let output = tokio::select! {
                    _ = cancellation.cancelled() => Err(ToolError::cancelled("cancelled during session-scoped execution")),
                    result = &mut dispatch => result,
                };
                drop(session_guard);
                drop(read_guard);
                output
            }
        };

        match result {
            Ok(mut output) => {
                let raw_output_text = output.raw_output_text();
                let raw_output_json = output.raw_output_json();
                let arguments = payload_arguments_json(&call.payload);
                let projection = project_tool_result(ToolProjectionInput {
                    call_id: call.call_id.as_str(),
                    tool_name: call.tool_name.as_str(),
                    arguments: &arguments,
                    raw_output_text: raw_output_text.as_str(),
                    raw_output_json: &raw_output_json,
                    success: output.success(),
                    outcome: &output.outcome,
                    output_policy: &call.output_policy,
                    output_projection: &call.output_projection,
                });
                if let Some((stream, delta_text)) =
                    output_delta_from_projection(&projection.display, &call.output_policy.deltas)
                {
                    trace.emit_output_chunk_delta(1, stream, delta_text, false);
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
                    trace.emit_completed(1, projection);
                }
                self.event_bus.finish_trace(trace.trace_id());
                Ok(output)
            }
            Err(error) => {
                let outcome = classify_call_error(&call, &error, source, &workdir);
                trace.emit_stage(1, "runtime.failed", Some(error.to_string()), None);
                trace.emit_failed_with_outcome(
                    1,
                    error.to_string().as_str(),
                    &outcome,
                    call.output_policy,
                );
                self.event_bus.finish_trace(trace.trace_id());
                Err(error)
            }
        }
    }

    async fn dispatch_call(
        &self,
        call: ToolCall,
        cancellation: &CancellationToken,
        trace: &ToolEventTrace,
        source: ToolCallSource,
        workdir: &PathBuf,
    ) -> Result<AnyToolResult, ToolError> {
        let dispatch = self.router.dispatch(
            self.orchestrator.as_ref(),
            call,
            source,
            workdir.clone(),
            trace,
        );
        tokio::pin!(dispatch);

        tokio::select! {
            _ = cancellation.cancelled() => {
                Err(ToolError::cancelled("tool dispatch cancelled"))
            }
            result = &mut dispatch => result,
        }
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
        attempt_id: 1,
        idempotency_key: call.idempotency_key.clone(),
        recovery: call.recovery,
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
        } => serde_json::json!({
            "server": server,
            "tool": tool,
            "arguments": arguments,
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, sleep};

    struct SleepHandler {
        sleep_ms: u64,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    struct FailingHandler;

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
            PathBuf::from("."),
        )
    }

    fn function_call(tool_name: &str, call_id: &str, class: ExecutionClass) -> ToolCall {
        ToolCall {
            call_id: call_id.to_owned(),
            tool_name: tool_name.to_owned(),
            payload: ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
            execution_class: class,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
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
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            output_policy: crate::output_policy::ToolOutputPolicySnapshot::for_tool_name(tool_name),
            output_projection: crate::output_policy::ToolOutputProjectionKind::Builtin,
            idempotency_key: Some(format!("{tool_name}:{call_id}")),
            trace_id: format!("trace_{call_id}"),
            session_scope_key: Some(format!("shell:{session_id}")),
        }
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
            vec!["runtime.call.started", "runtime.call.completed"],
            "ToolEventBus must carry product lifecycle events only"
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
            vec!["runtime.call.started", "runtime.call.failed"],
            "failed calls must terminalize through product lifecycle events only"
        );
    }
}
