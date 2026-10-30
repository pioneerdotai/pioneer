// Inside mcp_service's existing test module to reuse its runtime,
// authorization, catalog and file-backed SQLite fixtures. Never spawns a CLI.
use super::*;
mod cli_mcp_finalization {
    use super::*;
    use crate::cli_runtime::codex_mcp::build_codex_mcp_session_launch_projection;
    use crate::cli_runtime::manager::CLIAgentRuntimeSessionKey;
    use crate::cli_runtime::mcp::coordinator::CliMcpCoordinator;
    use crate::cli_runtime::mcp::facade::{
        CliMcpCallContract, CliMcpFacadeRequestContext, CliMcpNoopProgressSink,
        CliMcpProgressNotification, CliMcpProgressSendError, CliMcpProgressSink, CliMcpToolFacade,
    };
    use crate::cli_runtime::mcp::grants::{
        CliMcpConnectionId, CliMcpGrantScope, CliMcpManifestHash,
    };
    use crate::cli_runtime::mcp::limits::{CliMcpFacadeLimits, CliMcpFacadeProjectionLimits};
    use crate::cli_runtime::mcp::native_correlation::{
        CodexMcpCorrelationBinding, CorrelatingTurnMcpInvoker, NativeMcpCorrelationLedger,
    };
    use crate::cli_runtime::projector::{CLIRuntimeProjectorContext, project_cli_runtime_event};
    use crate::cli_runtime::session_instance::CliSessionInstanceId;
    use pioneer_cli_agent_runtime::event::{
        RuntimeEvent, RuntimeItemCompleted, RuntimeItemStarted,
    };
    use pioneer_cli_mcp_bridge::{AttachRequest, BridgeGeneration};
    use pioneer_protocol::{AgentDurableEvent, ToolCallStatus, ToolErrorClass, TurnItem};

    #[derive(Clone, Copy)]
    enum Outcome {
        Success,
        ToolError,
        InvocationError,
        Cancelled,
        LateCancel,
        Unknown,
    }

    struct Execution {
        outcome: Outcome,
        calls: AtomicUsize,
        entered: tokio::sync::Notify,
        release: tokio::sync::Semaphore,
    }

    #[derive(Default)]
    struct CompletedProgressGate(tokio::sync::Notify);

    struct ValidationRefusal;

    #[async_trait::async_trait]
    impl TurnMcpRuntimeView for ValidationRefusal {
        async fn current_tool_identity(
            &self,
            _: &str,
            _: &pioneer_crud::TurnMcpBindingRecord,
        ) -> Result<CurrentMcpToolIdentity, TurnMcpInvocationError> {
            Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::PermissionDenied,
                "mock validation refused execution",
            ))
        }
    }

    #[async_trait::async_trait]
    impl CliMcpProgressSink for CompletedProgressGate {
        async fn send_progress(
            &self,
            notification: CliMcpProgressNotification,
        ) -> Result<(), CliMcpProgressSendError> {
            if notification.progress == 1.0 {
                self.0.notify_one();
                std::future::pending().await
            } else {
                Ok(())
            }
        }
    }

    #[async_trait::async_trait]
    impl TurnMcpValidatedExecution for Execution {
        async fn execute(
            &self,
            _: ValidatedTurnMcpInvocation,
            cancellation: CancellationToken,
        ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            if matches!(self.outcome, Outcome::Cancelled) {
                cancellation.cancelled().await;
                return Err(TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::Cancelled,
                    "mock transport confirmed cancellation",
                ));
            }
            self.release.acquire().await.unwrap().forget();
            if matches!(self.outcome, Outcome::LateCancel) {
                cancellation.cancel();
            }
            if matches!(self.outcome, Outcome::InvocationError) {
                return Err(TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::ExecutionFailed,
                    "mock operation failed",
                ));
            }
            Ok(CanonicalMcpToolResult {
                content: json!([{"type":"text", "text":"private-result-canary"}]),
                structured_content: None,
                is_error: matches!(self.outcome, Outcome::ToolError | Outcome::Unknown),
                duration_ms: 3892,
                meta: matches!(self.outcome, Outcome::Unknown)
                    .then(|| json!({"pioneer":{"outcome":"unknown"}})),
            })
        }
    }

    struct Harness {
        fixture: TestTurnMcpInvokerFixture,
        facade: Arc<CliMcpToolFacade>,
        context: CliMcpFacadeRequestContext,
        items: Arc<NativeMcpCorrelationLedger>,
        launch: crate::cli_runtime::codex_mcp::CodexMcpSessionLaunchProjection,
        execution: Arc<Execution>,
    }

    impl Harness {
        async fn new(outcome: Outcome) -> Self {
            Self::with_progress(outcome, Arc::new(CliMcpNoopProgressSink)).await
        }

        async fn with_progress(outcome: Outcome, progress: Arc<dyn CliMcpProgressSink>) -> Self {
            Self::with_validation(outcome, progress, None).await
        }

        async fn with_validation(
            outcome: Outcome,
            progress: Arc<dyn CliMcpProgressSink>,
            runtime_view: Option<Arc<dyn TurnMcpRuntimeView>>,
        ) -> Self {
            let fixture = turn_mcp_invoker_fixture().await;
            let projection = fixture
                .service
                .resolve_mcp_turn_projection(&AgentMcpMaterializationRequest {
                    workspace_id: fixture.workspace_id.clone(),
                    turn_id: fixture.turn_id.clone(),
                    explicit_servers: Vec::new(),
                    explicit_tools: vec![tool_ref(
                        "mcp-tool:workspace:resend:send",
                        "resend",
                        "send",
                    )],
                })
                .await
                .unwrap();
            let launch =
                build_codex_mcp_session_launch_projection(projection, "a".repeat(64)).unwrap();
            let facade_projection = launch
                .facade_projection(CliMcpFacadeProjectionLimits::default())
                .unwrap();
            let now = chrono::Utc::now().fixed_offset();
            fixture
                .crud_store
                .prepare_cli_runtime_initial_turn_attempt(
                    NewCliRuntimeTurnBinding {
                        turn_id: fixture.turn_id.clone(),
                        thread_id: fixture.thread_id.clone(),
                        continuation_thread_id: fixture.thread_id.clone(),
                        workspace_id: fixture.workspace_id.clone(),
                        runtime_id: "runtime-codex".into(),
                        runtime_kind: "codex".into(),
                        native_thread_id: "native-thread".into(),
                        native_turn_id: None,
                        request_id: None,
                        status: "starting".into(),
                        model: None,
                        cwd: None,
                        sandbox_json: None,
                        approval_policy: None,
                        input_mapping_json: "{}".into(),
                        created_at: now,
                        updated_at: now,
                    },
                    "native-attempt".into(),
                    1,
                )
                .await
                .unwrap();
            fixture
                .crud_store
                .activate_cli_runtime_turn_attempt(
                    &fixture.turn_id,
                    "native-attempt",
                    "native-turn",
                    None,
                    now,
                )
                .await
                .unwrap();
            let bindings = fixture
                .crud_store
                .list_turn_mcp_bindings(&fixture.turn_id)
                .await
                .unwrap();
            fixture
                .crud_store
                .set_cli_runtime_turn_mcp_metadata(
                    &fixture.turn_id,
                    Some(CliRuntimeTurnMcpMetadata {
                        adapter_kind: "codex_synthetic_mcp".into(),
                        manifest_hash: fixture.manifest_hash.clone(),
                        projection_fingerprint: facade_projection.fingerprint().as_str().into(),
                        provider_contract_fingerprint: "a".repeat(64),
                        isolation_contract_fingerprint: "b".repeat(64),
                        session_generation: 1,
                        projection_activation_generation: bindings[0]
                            .projection_activation_generation,
                    }),
                )
                .await
                .unwrap();
            let coordinator = Arc::new(CliMcpCoordinator::default());
            let instance = CliSessionInstanceId::unmanaged_for_test(
                CLIAgentRuntimeSessionKey::new(
                    &fixture.workspace_id,
                    "runtime-codex",
                    &fixture.thread_id,
                )
                .unwrap(),
                1,
            )
            .unwrap();
            let scope = CliMcpGrantScope::new(
                instance,
                CliMcpManifestHash::new(&fixture.manifest_hash).unwrap(),
            );
            let issued = coordinator
                .issue_grant(scope.clone(), u64::MAX)
                .await
                .unwrap();
            let reservation = coordinator
                .stage_projection(&issued.grant_ref(), facade_projection.fingerprint().clone())
                .await
                .unwrap();
            let bound = coordinator
                .attach(
                    &AttachRequest {
                        session_id: issued.bridge_session_id.clone(),
                        generation: BridgeGeneration::new(1).unwrap(),
                        nonce: issued.nonce.clone(),
                    },
                    &scope,
                    CliMcpConnectionId::for_test(1),
                )
                .await
                .unwrap();
            let execution = Arc::new(Execution {
                outcome,
                calls: AtomicUsize::new(0),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Semaphore::new(0),
            });
            let gateway = GatewayTurnMcpInvoker::new(
                fixture.crud_store.clone(),
                runtime_view.unwrap_or_else(|| Arc::new(fixture.service.clone())),
                execution.clone(),
                fixture.service.inner.authorization_invalidation_hub.clone(),
                fixture.service.inner.execution_leases.clone(),
                fixture.service.inner.invocation_governor.clone(),
            );
            let items = Arc::new(NativeMcpCorrelationLedger::default());
            let invoker = CorrelatingTurnMcpInvoker {
                inner: Arc::new(gateway),
                native_items: items.clone(),
                opencode_binding: None,
                codex_binding: Some(CodexMcpCorrelationBinding {
                    coordinator: coordinator.clone(),
                    bound_grant: bound.clone(),
                }),
            };
            let facade = CliMcpToolFacade::try_new(
                coordinator.clone(),
                Arc::new(invoker),
                facade_projection,
                progress,
                CliMcpFacadeLimits::default(),
                CliMcpCallContract::Codex,
            )
            .unwrap();
            let mut context = CliMcpFacadeRequestContext {
                bound_grant: bound,
                projection_generation: reservation.generation,
                activation_generation: None,
            };
            facade.handle_message(&context, json!({"jsonrpc":"2.0","id":1,"method":"initialize",
                "params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"mock","version":"1"}}})).await.unwrap();
            facade
                .handle_message(
                    &context,
                    json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                )
                .await;
            facade
                .handle_message(
                    &context,
                    json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
                )
                .await
                .unwrap();
            let turn = coordinator
                .reserve_turn(
                    &context.bound_grant.grant_ref(),
                    context.projection_generation,
                    &fixture.thread_id,
                    &fixture.turn_id,
                )
                .await
                .unwrap();
            coordinator
                .activate_turn(
                    &context.bound_grant,
                    turn.activation_generation,
                    "native-thread",
                    "native-turn",
                )
                .await
                .unwrap();
            context.activation_generation = Some(turn.activation_generation);
            Self {
                fixture,
                facade,
                context,
                items,
                launch,
                execution,
            }
        }

        fn projector(&self) -> CLIRuntimeProjectorContext {
            CLIRuntimeProjectorContext {
                workspace_id: self.fixture.workspace_id.clone(),
                thread_id: self.fixture.thread_id.clone(),
                turn_id: self.fixture.turn_id.clone(),
                recovery: None,
                codex_retry_attempt_id: None,
            }
        }

        async fn start(&self, id: &str, persist: bool) -> RuntimeEvent {
            let mut event = RuntimeEvent::ItemStarted(RuntimeItemStarted {
                native_thread_id: Some("native-thread".into()),
                native_turn_id: "native-turn".into(),
                native_item_id: id.into(),
                item_kind: "mcpToolCall".into(),
                title: None,
                phase: Default::default(),
                metadata: Some(
                    json!({"server":"pioneer", "tool":self.fixture.callable_name, "arguments":{}}),
                ),
                native_item_redacted: None,
                native: None,
            });
            self.items
                .register(
                    self.launch
                        .enrich_native_event("runtime-codex", 1, &mut event)
                        .unwrap()
                        .unwrap(),
                )
                .unwrap();
            if persist {
                self.persist_start(&event).await;
            }
            event
        }

        async fn persist_start(&self, event: &RuntimeEvent) {
            let projected = project_cli_runtime_event(&self.projector(), event);
            let AgentDurableEvent::ItemStarted { notification } = &projected.durable[0] else {
                panic!("start");
            };
            self.fixture
                .crud_store
                .materialize_item_started(notification.clone(), chrono::Utc::now().timestamp())
                .await
                .unwrap();
        }

        fn request(&self, id: &str) -> JsonValue {
            json!({"jsonrpc":"2.0", "id":id, "method":"tools/call", "params":{
                "name":self.fixture.callable_name, "arguments":{}, "_meta":{
                    "callId":id,"threadId":"native-thread", "x-codex-turn-metadata":{
                        "thread_id":"native-thread","turn_id":"native-turn","codex_version":"0.164.0"}}}})
        }

        fn call(&self, request: JsonValue) -> tokio::task::JoinHandle<JsonValue> {
            let facade = self.facade.clone();
            let context = self.context.clone();
            tokio::spawn(async move { facade.handle_message(&context, request).await.unwrap() })
        }

        async fn cancel(&self, id: &str) {
            self.facade
                .handle_message(
                    &self.context,
                    json!({"jsonrpc":"2.0", "method":"notifications/cancelled",
                "params":{"requestId":id, "reason":"caller stopped waiting"}}),
                )
                .await;
        }

        async fn terminal(&self) {
            let (_, mut turn) = self
                .fixture
                .crud_store
                .get_turn(&self.fixture.thread_id, &self.fixture.turn_id)
                .await
                .unwrap()
                .unwrap();
            turn.status = TurnStatus::Completed;
            self.fixture
                .crud_store
                .materialize_turn_completed(
                    pioneer_protocol::TurnCompletedNotification {
                        workspace_id: self.fixture.workspace_id.clone(),
                        thread_id: self.fixture.thread_id.clone(),
                        turn,
                    },
                    chrono::Utc::now().timestamp(),
                )
                .await
                .unwrap();
        }

        async fn item(&self, id: &str) -> TurnItem {
            self.fixture
                .crud_store
                .get_turn_item(&self.fixture.turn_id, id)
                .await
                .unwrap()
                .unwrap()
        }

        async fn completions(&self) -> usize {
            self.fixture
                .crud_store
                .get_turn_item_events(&self.fixture.thread_id, &self.fixture.turn_id)
                .await
                .unwrap()
                .unwrap()
                .events
                .iter()
                .filter(|event| {
                    matches!(
                        event.payload,
                        pioneer_protocol::TurnItemEventPayload::ItemCompleted { .. }
                    )
                })
                .count()
        }
    }

    #[tokio::test]
    async fn caller_dropped_after_execution_is_unconfirmed_delivery_and_cannot_erase_success() {
        let progress = Arc::new(CompletedProgressGate::default());
        let h = Harness::with_progress(Outcome::Success, progress.clone()).await;
        h.start("item", true).await;
        let mut request = h.request("item");
        request["params"]["_meta"]["progressToken"] = json!("progress");
        let call = h.call(request.clone());
        h.execution.entered.notified().await;
        h.execution.release.add_permits(1);
        progress.0.notified().await; // Actual invoker/canonical commit precedes progress.
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        let response = h.facade.handle_message(&h.context, request).await.unwrap();
        assert_eq!(response["result"]["isError"], false);
        assert_eq!(response["result"]["_meta"]["pioneer"]["durationMs"], 3892);
        assert_eq!(
            response["result"]["content"],
            json!([{"type":"text", "text":"private-result-canary"}])
        );
        h.terminal().await;
        let TurnItem::DynamicToolCall {
            success,
            storage: pioneer_protocol::ToolStoragePayload::Metadata { metadata },
            ..
        } = h.item("item").await
        else {
            panic!("canonical result");
        };
        assert_eq!(success, Some(true));
        assert_eq!(
            metadata.get("status").and_then(|value| value.as_str()),
            Some("completed")
        );
        assert_eq!(
            metadata
                .get("responseDelivery")
                .and_then(|value| value.as_str()),
            Some("unconfirmed")
        );
        assert_eq!(h.completions().await, 1);
        assert_eq!(h.execution.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn caller_dropped_before_result_does_not_invent_execution_or_cancellation_ack() {
        let h = Harness::new(Outcome::Success).await;
        h.start("item", true).await;
        let request = h.request("item");
        let call = h.call(request.clone());
        h.execution.entered.notified().await;
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        let response = h.facade.handle_message(&h.context, request).await.unwrap();
        assert_eq!(response["error"]["data"]["kind"], "response_unconfirmed");
        assert_eq!(h.completions().await, 0);
        h.terminal().await;
        let TurnItem::DynamicToolCall {
            success,
            outcome: Some(outcome),
            ..
        } = h.item("item").await
        else {
            panic!("unconfirmed result");
        };
        assert_eq!(success, None);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::Unknown));
        assert_eq!(
            outcome.incomplete_reason.as_deref(),
            Some("cli_mcp_result_unconfirmed")
        );
        assert_eq!(h.execution.calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.completions().await, 0);
    }

    #[tokio::test]
    async fn verified_validation_refusal_is_canonical_before_native_completion_and_turn_close() {
        for stale_generation in [false, true] {
            let h = Harness::with_validation(
                Outcome::Success,
                Arc::new(CliMcpNoopProgressSink),
                Some(Arc::new(ValidationRefusal)),
            )
            .await;
            let RuntimeEvent::ItemStarted(started) = h.start("item", true).await else {
                panic!("start");
            };
            if stale_generation {
                let mut metadata = h
                    .fixture
                    .crud_store
                    .get_cli_runtime_turn_binding(&h.fixture.turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .mcp
                    .unwrap();
                metadata.session_generation += 1;
                h.fixture
                    .crud_store
                    .set_cli_runtime_turn_mcp_metadata(&h.fixture.turn_id, Some(metadata))
                    .await
                    .unwrap();
            }
            let response = h.call(h.request("item")).await.unwrap();
            if stale_generation {
                assert_eq!(
                    response["error"]["data"]["kind"],
                    "session_generation_stale"
                );
                assert_eq!(h.execution.calls.load(Ordering::SeqCst), 0);
                assert_eq!(h.completions().await, 0);
                h.terminal().await;
                assert!(matches!(
                    h.item("item").await,
                    TurnItem::DynamicToolCall {
                        success: None,
                        outcome: Some(pioneer_protocol::ToolOutcome {
                            error_class: Some(ToolErrorClass::Unknown),
                            ..
                        }),
                        ..
                    }
                ));
                continue;
            }
            assert_eq!(response["error"]["data"]["kind"], "permission_denied");
            assert_eq!(h.execution.calls.load(Ordering::SeqCst), 0);
            let before = h.item("item").await;
            let native = RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                native_thread_id: started.native_thread_id,
                native_turn_id: started.native_turn_id,
                native_item_id: started.native_item_id,
                item_kind: started.item_kind,
                text: None,
                summary: Vec::new(),
                content: Vec::new(),
                phase: Default::default(),
                metadata: started.metadata,
                native_item_redacted: None,
                native: None,
            });
            assert!(
                project_cli_runtime_event(&h.projector(), &native)
                    .durable
                    .is_empty()
            );
            h.terminal().await;
            assert_eq!(h.item("item").await, before);
            let TurnItem::DynamicToolCall {
                status,
                success,
                outcome: Some(outcome),
                storage: pioneer_protocol::ToolStoragePayload::Metadata { metadata },
                ..
            } = before
            else {
                panic!("canonical rejection");
            };
            assert_eq!(status, ToolCallStatus::Failed);
            assert_eq!(success, Some(false));
            assert_eq!(outcome.error_class, Some(ToolErrorClass::PermissionDenied));
            assert_eq!(
                outcome.incomplete_reason.as_deref(),
                Some("permission_denied")
            );
            assert!(!outcome.incomplete);
            assert_eq!(
                metadata.get("executionAdmission").and_then(|v| v.as_str()),
                Some("rejected")
            );
            assert_eq!(
                metadata.get("responseDelivery").and_then(|v| v.as_str()),
                Some("unconfirmed")
            );
            assert_eq!(h.completions().await, 1);
            assert!(
                h.fixture
                    .crud_store
                    .list_running_turn_item_attempts_for_turn(&h.fixture.turn_id)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn managed_result_without_native_completion_survives_terminal_and_projection_recovery() {
        let h = Harness::new(Outcome::Success).await;
        h.start("exec-22d09e63-3a0d-429b-8585-f87d9f770dfc", true)
            .await;
        let request = h.request("exec-22d09e63-3a0d-429b-8585-f87d9f770dfc");
        let call = h.call(request.clone());
        h.execution.entered.notified().await;
        h.execution.release.add_permits(1);
        assert!(call.await.unwrap().get("result").is_some());
        let before = h.item("exec-22d09e63-3a0d-429b-8585-f87d9f770dfc").await;
        assert!(matches!(
            &before,
            TurnItem::DynamicToolCall {
                status: ToolCallStatus::Completed,
                success: Some(true),
                ..
            }
        ));
        assert!(
            h.facade
                .handle_message(&h.context, request)
                .await
                .unwrap()
                .get("result")
                .is_some()
        );
        h.terminal().await;
        let restored = CrudStore::new(h.fixture.crud_store.database_connection());
        restored
            .replay_due_turn_event_projections(chrono::Utc::now().timestamp() + 600, 64)
            .await
            .unwrap();
        assert_eq!(
            restored
                .get_turn_item(&h.fixture.turn_id, before.item_id())
                .await
                .unwrap()
                .unwrap(),
            before
        );
        assert_eq!(h.execution.calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.completions().await, 1);
        assert!(
            !serde_json::to_string(&before)
                .unwrap()
                .contains("private-result-canary")
        );
    }

    #[tokio::test]
    async fn managed_error_cancellation_and_late_client_cancel_keep_distinct_outcomes() {
        for (outcome, terminal_first) in [
            Outcome::ToolError,
            Outcome::InvocationError,
            Outcome::Cancelled,
            Outcome::LateCancel,
            Outcome::Unknown,
        ]
        .into_iter()
        .flat_map(|outcome| [(outcome, false), (outcome, true)])
        {
            let h = Harness::new(outcome).await;
            h.start("item", true).await;
            let call = h.call(h.request("item"));
            h.execution.entered.notified().await;
            if terminal_first {
                h.terminal().await;
            }
            if matches!(outcome, Outcome::Cancelled) {
                h.cancel("item").await;
            } else {
                h.execution.release.add_permits(1);
            }
            let response = call.await.unwrap();
            if !terminal_first {
                h.terminal().await;
            }
            let TurnItem::DynamicToolCall {
                status,
                success,
                outcome: Some(saved),
                ..
            } = h.item("item").await
            else {
                panic!("outcome");
            };
            match outcome {
                Outcome::ToolError => {
                    assert_eq!(response["result"]["isError"], true);
                    assert_eq!(success, Some(false));
                    assert_eq!(saved.error_class, Some(ToolErrorClass::ExecutionFailed));
                    assert!(!saved.incomplete);
                }
                Outcome::Cancelled => {
                    assert_eq!(response["error"]["data"]["kind"], "cancelled");
                    assert_eq!(saved.error_class, Some(ToolErrorClass::Cancelled));
                    let mut items = h
                        .fixture
                        .crud_store
                        .list_turn_items_with_status_by_type_for_turns(
                            std::slice::from_ref(&h.fixture.turn_id),
                            "dynamic_tool_call",
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        items.remove(&h.fixture.turn_id).unwrap()[0].1.as_deref(),
                        Some("cancelled")
                    );
                    assert_eq!(
                        saved.incomplete_reason.as_deref(),
                        Some("mock transport confirmed cancellation")
                    );
                }
                Outcome::InvocationError => {
                    assert_eq!(response["error"]["data"]["kind"], "execution_failed");
                    assert_eq!(success, Some(false));
                    assert_eq!(saved.error_class, Some(ToolErrorClass::ExecutionFailed));
                }
                Outcome::LateCancel => {
                    assert!(response.get("result").is_some());
                    assert_eq!(status, ToolCallStatus::Completed);
                    assert_eq!(success, Some(true));
                    assert_eq!(saved.error_class, None);
                }
                Outcome::Unknown => {
                    assert_eq!(success, None);
                    assert_eq!(saved.error_class, Some(ToolErrorClass::Unknown));
                    assert!(saved.incomplete);
                }
                _ => unreachable!(),
            }
            assert_eq!(h.completions().await, 1);
        }
    }

    #[tokio::test]
    async fn deferred_result_fences_late_observations_but_replays_an_earlier_accepted_start() {
        for start_accepted_before_result in [false, true] {
            let h = Harness::new(Outcome::Success).await;
            // Correlation is registered independently of canonical projection.
            let start = h.start("item", false).await;
            let projected = project_cli_runtime_event(&h.projector(), &start);
            let AgentDurableEvent::ItemStarted {
                notification: started,
            } = &projected.durable[0]
            else {
                panic!("start");
            };
            let call = h.call(h.request("item"));
            h.execution.entered.notified().await;
            let (_, mut turn) = h
                .fixture
                .crud_store
                .get_turn(&h.fixture.thread_id, &h.fixture.turn_id)
                .await
                .unwrap()
                .unwrap();
            turn.status = TurnStatus::Completed;
            h.fixture
                .crud_store
                .accept_turn_event_before_projection_for_test(
                    pioneer_crud::CanonicalTurnEventPayload::TurnCompleted(
                        pioneer_protocol::TurnCompletedNotification {
                            workspace_id: h.fixture.workspace_id.clone(),
                            thread_id: h.fixture.thread_id.clone(),
                            turn,
                        },
                    ),
                    chrono::Utc::now().timestamp(),
                )
                .await
                .unwrap();
            if start_accepted_before_result {
                h.fixture
                    .crud_store
                    .accept_turn_event_before_projection_for_test(
                        pioneer_crud::CanonicalTurnEventPayload::ItemStarted(started.clone()),
                        chrono::Utc::now().timestamp(),
                    )
                    .await
                    .unwrap();
            }
            h.execution.release.add_permits(1);
            assert!(call.await.unwrap().get("result").is_some());
            assert_eq!(
                h.completions().await,
                1,
                "result is durable before replay or native completion"
            );
            assert!(
                h.fixture
                    .crud_store
                    .get_turn_item(&h.fixture.turn_id, "item")
                    .await
                    .unwrap()
                    .is_none(),
                "canonical completion is accepted but has not been projected"
            );
            if !start_accepted_before_result {
                h.persist_start(&start).await;
            }
            h.fixture
                .crud_store
                .materialize_item_updated(
                    pioneer_protocol::ItemUpdatedNotification {
                        workspace_id: started.workspace_id.clone(),
                        thread_id: started.thread_id.clone(),
                        turn_id: started.turn_id.clone(),
                        item: started.item.clone(),
                    },
                    chrono::Utc::now().timestamp(),
                )
                .await
                .unwrap();
            let restored = CrudStore::new(h.fixture.crud_store.database_connection());
            if start_accepted_before_result {
                // Advance through the predecessor and the earlier start only.
                // A later accepted completion must not suppress this original start.
                restored
                    .replay_due_turn_event_projections(chrono::Utc::now().timestamp() + 600, 2)
                    .await
                    .unwrap();
                assert!(matches!(
                    h.item("item").await,
                    TurnItem::DynamicToolCall {
                        status: ToolCallStatus::InProgress,
                        ..
                    }
                ));
                assert_eq!(
                    restored
                        .list_running_turn_item_attempts_for_turn(&h.fixture.turn_id)
                        .await
                        .unwrap()
                        .len(),
                    1
                );
            }
            restored
                .replay_due_turn_event_projections(chrono::Utc::now().timestamp() + 600, 64)
                .await
                .unwrap();
            assert!(matches!(
                restored
                    .get_turn_item(&h.fixture.turn_id, "item")
                    .await
                    .unwrap()
                    .unwrap(),
                TurnItem::DynamicToolCall {
                    status: ToolCallStatus::Completed,
                    success: Some(true),
                    ..
                }
            ));
            assert!(
                restored
                    .list_running_turn_item_attempts_for_turn(&h.fixture.turn_id)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(h.completions().await, 1);
            assert_eq!(h.execution.calls.load(Ordering::SeqCst), 1);
            let saved = h.item("item").await;
            h.persist_start(&start).await;
            assert_eq!(h.item("item").await, saved);
        }
    }

    #[tokio::test]
    async fn terminal_before_confirmed_result_keeps_gap_honest_then_commits_only_that_attempt() {
        let h = Harness::new(Outcome::Success).await;
        h.start("item", true).await;
        let call = h.call(h.request("item"));
        h.execution.entered.notified().await;
        h.terminal().await;
        let TurnItem::DynamicToolCall {
            success,
            outcome: Some(gap),
            ..
        } = h.item("item").await
        else {
            panic!("gap");
        };
        assert_eq!(success, None);
        assert_eq!(gap.error_class, Some(ToolErrorClass::Unknown));
        assert_eq!(
            gap.incomplete_reason.as_deref(),
            Some("cli_mcp_result_unconfirmed")
        );
        h.cancel("item").await; // A cancelled waiter cannot undo completed work.
        h.execution.release.add_permits(1);
        assert!(call.await.unwrap().get("result").is_some());
        assert!(matches!(
            h.item("item").await,
            TurnItem::DynamicToolCall {
                success: Some(true),
                status: ToolCallStatus::Completed,
                ..
            }
        ));
        assert_eq!(h.completions().await, 1);
        assert_eq!(h.execution.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn identical_calls_and_late_native_events_do_not_mix_or_resurrect_results() {
        let h = Harness::new(Outcome::Success).await;
        let start = h.start("A", false).await;
        h.start("B", true).await;
        let call = h.call(h.request("A"));
        h.execution.entered.notified().await;
        h.execution.release.add_permits(1);
        assert!(call.await.unwrap().get("result").is_some());
        let before = h.item("A").await;
        h.persist_start(&start).await; // Native start projection lost the race.
        let RuntimeEvent::ItemStarted(started) = start else {
            panic!("start");
        };
        let late = RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
            native_thread_id: started.native_thread_id,
            native_turn_id: started.native_turn_id,
            native_item_id: started.native_item_id,
            item_kind: started.item_kind,
            text: None,
            summary: Vec::new(),
            content: Vec::new(),
            phase: Default::default(),
            metadata: started.metadata,
            native_item_redacted: None,
            native: None,
        });
        assert!(
            project_cli_runtime_event(&h.projector(), &late)
                .durable
                .is_empty()
        );
        assert_eq!(h.item("A").await, before);
        assert!(matches!(
            h.item("B").await,
            TurnItem::DynamicToolCall {
                status: ToolCallStatus::InProgress,
                ..
            }
        ));
        h.terminal().await;
        assert_eq!(h.item("A").await, before);
        assert!(matches!(
            h.item("B").await,
            TurnItem::DynamicToolCall { success: None, .. }
        ));
        assert_eq!(h.completions().await, 1);
    }

    #[tokio::test]
    async fn generation_change_during_execution_cannot_complete_current_item() {
        let h = Harness::new(Outcome::Success).await;
        h.start("item", true).await;
        let call = h.call(h.request("item"));
        h.execution.entered.notified().await;
        let mut binding = h
            .fixture
            .crud_store
            .get_cli_runtime_turn_binding(&h.fixture.turn_id)
            .await
            .unwrap()
            .unwrap()
            .mcp
            .unwrap();
        binding.session_generation += 1;
        h.fixture
            .crud_store
            .set_cli_runtime_turn_mcp_metadata(&h.fixture.turn_id, Some(binding))
            .await
            .unwrap();
        h.execution.release.add_permits(1);
        assert!(call.await.unwrap().get("error").is_some());
        assert!(matches!(
            h.item("item").await,
            TurnItem::DynamicToolCall {
                status: ToolCallStatus::InProgress,
                ..
            }
        ));
        assert_eq!(h.completions().await, 0);
    }

    #[tokio::test]
    async fn foreign_native_thread_cannot_borrow_an_identical_registered_call() {
        let h = Harness::new(Outcome::Success).await;
        h.start("item", true).await;
        let mut request = h.request("item");
        request["params"]["_meta"]["threadId"] = json!("foreign");
        request["params"]["_meta"]["x-codex-turn-metadata"]["thread_id"] = json!("foreign");
        let response = h.facade.handle_message(&h.context, request).await.unwrap();
        assert_eq!(response["error"]["data"]["kind"], "scope_mismatch");
        assert_eq!(h.execution.calls.load(Ordering::SeqCst), 0);
        assert_eq!(h.completions().await, 0);
        h.terminal().await;
        let TurnItem::DynamicToolCall {
            success,
            outcome: Some(outcome),
            ..
        } = h.item("item").await
        else {
            panic!("gap");
        };
        assert_eq!(success, None);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::Unknown));
    }
}
