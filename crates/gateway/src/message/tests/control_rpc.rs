//! Unrun owner-boundary regressions. The stream supplies frames; the production
//! pump owns classification/admission and every domain request uses dispatch.
use super::*;
use crate::transport::{ConnectionAccessLease, ConnectionHandlers, process_with_access_renewals};
use futures_util::FutureExt;
use pioneer_protocol::{JsonRpcRequest, constants::methods};
use pioneer_provider::ProviderRegistry;
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};
use tokio::sync::watch;

const THREAD: &str = "thrControl00000000001";
const TURN: &str = "trnControl00000000001";
const TASK: &str = "tskControl00000000001";
const RUN: &str = "runControl00000000001";

struct ModelGate {
    entered: Notify,
    release: Notify,
}
#[async_trait::async_trait]
impl Provider for ModelGate {
    fn name(&self) -> &str {
        "openai"
    }
    async fn list_models(&self) -> anyhow::Result<Vec<pioneer_protocol::ProviderModelInfo>> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(vec![])
    }
    async fn chat(&self, _: ChatRequest) -> anyhow::Result<ChatResponse> {
        panic!("control fixture only requests model discovery")
    }
    async fn stream_chat(
        &self,
        _: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        panic!("control fixture never calls a model")
    }
}

struct Fixture {
    processor: Arc<MessageProcessor>,
    auth: Arc<crate::auth::GatewayAuthService>,
    connection: ConnectionId,
    responses: mpsc::Receiver<Message>,
    outbound: crate::session::SessionOutbound,
    workspace: String,
    gate: Arc<ModelGate>,
}
impl Fixture {
    async fn new() -> Self {
        let (mut processor, store, workspace) =
            setup_execution_window_terminal_turn(THREAD, TURN).await;
        processor
            .thread_manager
            .turn_finish(THREAD, TURN, TurnStatus::Completed, None)
            .await
            .unwrap();
        let (_, terminal) = processor
            .thread_manager
            .turn_get(THREAD, TURN)
            .await
            .unwrap();
        store
            .materialize_turn_completed(
                pioneer_protocol::TurnCompletedNotification {
                    workspace_id: workspace.clone(),
                    thread_id: THREAD.into(),
                    turn: terminal,
                },
                now_timestamp_secs(),
            )
            .await
            .unwrap();
        let identity = Arc::new(
            crate::identity::bootstrap_identity(store.database_connection())
                .await
                .unwrap()
                .snapshot,
        );
        let auth = Arc::new(
            crate::auth::GatewayAuthService::new(
                store.database_connection(),
                pioneer_config::GatewayAuthConfig::default(),
                identity,
                &crate::secrets::AuthKeyMaterial::from_test_bytes(vec![8; 64]),
                &crate::secrets::AuthKeyMaterial::from_test_bytes(vec![9; 64]),
            )
            .unwrap(),
        );
        let gate = Arc::new(ModelGate {
            entered: Notify::new(),
            release: Notify::new(),
        });
        processor.provider_registry =
            Arc::new(ProviderRegistry::with_provider("openai", gate.clone()));
        let (outbound, responses) = mpsc::channel(128);
        let connection = processor
            .session_manager
            .register_connection(outbound.clone(), authenticated_test_superuser())
            .await
            .unwrap();
        // Seed actual durable targets through the existing ORM/runtime handle.
        // No executor is launched and teardown must leave both statuses intact.
        pioneer_entity::task::Entity::insert(pioneer_entity::task::ActiveModel {
            id: Set(TASK.into()),
            workspace_id: Set(workspace.clone()),
            owner_kind: Set("thread".into()),
            owner_id: Set(Some(THREAD.into())),
            created_by_thread_id: Set(Some(THREAD.into())),
            created_by_turn_id: Set(Some(TURN.into())),
            executor_kind: Set("agent".into()),
            status: Set("running".into()),
            title: Set("Protected wait".into()),
            goal: Set("Protected payload".into()),
            ..Default::default()
        })
        .exec(&store.database_connection())
        .await
        .unwrap();
        pioneer_entity::task_run::Entity::insert(pioneer_entity::task_run::ActiveModel {
            id: Set(RUN.into()),
            task_id: Set(TASK.into()),
            run_group_id: Set(RUN.into()),
            attempt_number: Set(1),
            run_number: Set(1),
            status: Set("running".into()),
            executor_kind: Set("agent".into()),
            ..Default::default()
        })
        .exec(&store.database_connection())
        .await
        .unwrap();
        Self {
            processor: Arc::new(processor),
            auth,
            connection,
            responses,
            outbound: outbound.into(),
            workspace,
            gate,
        }
    }

    async fn ordinary(&self) -> crate::message::AbortOnDropMessageTask<()> {
        let context = self
            .processor
            .session_manager
            .connection_context(self.connection)
            .await
            .unwrap();
        let task = self.processor.clone().start_owned_request(
            context,
            rpc(
                "ordinary",
                methods::PROVIDER_MODELS_LIST,
                json!({"workspace_id":self.workspace,"provider":"openai"}),
            ),
        );
        self.gate.entered.notified().await;
        task
    }

    async fn assert_task_running(&self) {
        assert_eq!(
            self.processor
                .crud_store
                .get_task(TASK)
                .await
                .unwrap()
                .unwrap()
                .task
                .status,
            TaskStatus::Running
        );
        assert_eq!(
            self.processor
                .crud_store
                .get_task_run(RUN)
                .await
                .unwrap()
                .unwrap()
                .status,
            TaskRunStatus::Running
        );
    }
}

fn id(label: &str) -> String {
    generate_test_request_id("controlrpc", label)
}
fn rpc(label: &str, method: &str, params: JsonValue) -> String {
    json!({"jsonrpc":"2.0","id":id(label),"method":method,"params":params}).to_string()
}
fn incoming() -> (
    mpsc::UnboundedSender<Message>,
    std::pin::Pin<Box<impl futures_util::Stream<Item = Result<Message, axum::Error>>>>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    (
        tx,
        Box::pin(futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|message| (Ok(message), rx))
        })),
    )
}
fn dispatch(
    processor: Arc<MessageProcessor>,
) -> impl Fn(crate::request_context::ConnectionContext, String) -> MessageFuture<'static, ()> {
    move |context, payload| {
        let processor = processor.clone();
        message_future(async move {
            processor.process_request(&context, &payload).await;
        })
    }
}

#[tokio::test]
async fn blocked_ordinary_allows_exact_cancel_and_wait_without_reordering_binary() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut f = Fixture::new().await;
        let mut ordinary = f.ordinary().await;
        let (input, mut reader) = incoming();
        let mut context = f
            .processor
            .session_manager
            .connection_context(f.connection)
            .await
            .unwrap();
        let (renewals, _) = watch::channel(ConnectionAccessLease::new(
            context.principal().clone(),
            Duration::from_secs(60),
        ));
        let mut handlers = ConnectionHandlers::default();
        let mut deferred = VecDeque::new();
        let dispatch = dispatch(f.processor.clone());
        let pump = process_with_access_renewals(
            async {
                ordinary.join().await.unwrap();
            },
            &mut reader,
            &mut deferred,
            &f.auth,
            &f.processor.session_manager,
            &mut context,
            &renewals,
            &f.outbound,
            &mut handlers,
            &dispatch,
        );
        let driver = async {
            input
                .send(Message::Text(
                    rpc("wait", methods::TASK_WAIT, json!({"run_ids":[RUN]})).into(),
                ))
                .unwrap();
            // Invalid protocol/target cannot use classification as authorization.
            let malformed = rpc(
                "version",
                methods::TURN_CANCEL,
                json!({"thread_id":THREAD,"turn_id":TURN}),
            )
            .replace("2.0", "1.0");
            input.send(Message::Text(malformed.into())).unwrap();
            recv_error_by_id(&mut f.responses, &id("version")).await;
            input
                .send(Message::Text(
                    rpc(
                        "foreign",
                        methods::TURN_CANCEL,
                        json!({"thread_id":THREAD,"turn_id":"missing"}),
                    )
                    .into(),
                ))
                .unwrap();
            recv_error_by_id(&mut f.responses, &id("foreign")).await;
            input
                .send(Message::Text(
                    rpc(
                        "cancel",
                        methods::TURN_CANCEL,
                        json!({"thread_id":THREAD,"turn_id":TURN}),
                    )
                    .into(),
                ))
                .unwrap();
            let ack = recv_response_by_id(&mut f.responses, &id("cancel")).await;
            assert_eq!(ack.result["turn"]["id"], TURN);
            assert_eq!(ack.result["turn"]["status"], "Completed");
            let next = rpc("short", methods::WORKSPACE_LIST, json!({}));
            input.send(Message::Text(next.clone().into())).unwrap();
            input.send(Message::Binary(vec![4, 5].into())).unwrap();
            // Consume a final Cancel ACK as a barrier after the FIFO frames.
            input
                .send(Message::Text(
                    rpc(
                        "marker",
                        methods::TURN_CANCEL,
                        json!({"thread_id":THREAD,"turn_id":TURN}),
                    )
                    .into(),
                ))
                .unwrap();
            recv_response_by_id(&mut f.responses, &id("marker")).await;
            assert!(
                f.responses.try_recv().is_err(),
                "ordinary/wait must still be incomplete"
            );
            f.gate.release.notify_one();
            next
        };
        let (alive, next) = tokio::join!(pump, driver);
        assert!(alive);
        assert_eq!(
            deferred,
            VecDeque::from([
                Message::Text(next.clone().into()),
                Message::Binary(vec![4, 5].into())
            ])
        );
        // The local no-timeout waiter remains owned while another ordinary RPC
        // completes. Dropping/joining its owner does not cancel durable work.
        f.processor
            .clone()
            .process_owned_request(context, next)
            .await;
        let short = recv_response_by_id(&mut f.responses, &id("short")).await;
        assert!(short.result.get("workspaces").is_some());
        handlers.shutdown().await;
        f.assert_task_running().await;
    })
    .await
    .expect("Cancel and task/wait must bypass the ordinary barrier");
}

#[tokio::test]
async fn deferred_count_bytes_single_message_and_control_capacity_close_boundedly() {
    // Cases share the actual blocked domain handler and pump, not a copy of its
    // scheduler. The gate only holds a real dispatcher future before admission.
    for case in ["count", "bytes", "byte-control", "single", "cancel", "wait"] {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut f = Fixture::new().await;
            let mut ordinary = f.ordinary().await;
            let (input, mut reader) = incoming();
            match case {
                "count" => {
                    for _ in 0..64 {
                        input.send(Message::Binary(vec![1].into())).unwrap();
                    }
                    input
                        .send(Message::Text(
                            rpc(
                                "at-full",
                                methods::TURN_CANCEL,
                                json!({"thread_id":THREAD,"turn_id":TURN}),
                            )
                            .into(),
                        ))
                        .unwrap();
                }
                "byte-control" => {
                    input
                        .send(Message::Binary(vec![1; 4 * 1024 * 1024].into()))
                        .unwrap();
                    input
                        .send(Message::Text(
                            rpc(
                                "at-full",
                                methods::TURN_CANCEL,
                                json!({"thread_id":THREAD,"turn_id":TURN}),
                            )
                            .into(),
                        ))
                        .unwrap();
                }
                "bytes" => {
                    input
                        .send(Message::Binary(vec![1; 3 * 1024 * 1024].into()))
                        .unwrap();
                    input
                        .send(Message::Binary(vec![2; 2 * 1024 * 1024].into()))
                        .unwrap();
                }
                "single" => {
                    input
                        .send(Message::Binary(vec![1; 4 * 1024 * 1024 + 1].into()))
                        .unwrap();
                }
                "cancel" => {
                    for n in 0..2 {
                        input
                            .send(Message::Text(
                                rpc(
                                    &format!("cancel{n}"),
                                    methods::TURN_CANCEL,
                                    json!({"thread_id":THREAD,"turn_id":TURN}),
                                )
                                .into(),
                            ))
                            .unwrap();
                    }
                }
                "wait" => {
                    for n in 0..5 {
                        input
                            .send(Message::Text(
                                rpc(
                                    &format!("wait{n}"),
                                    methods::TASK_WAIT,
                                    json!({"run_ids":[RUN]}),
                                )
                                .into(),
                            ))
                            .unwrap();
                    }
                }
                _ => unreachable!(),
            }
            let mut context = f
                .processor
                .session_manager
                .connection_context(f.connection)
                .await
                .unwrap();
            let (renewals, _) = watch::channel(ConnectionAccessLease::new(
                context.principal().clone(),
                Duration::from_secs(60),
            ));
            let mut handlers = ConnectionHandlers::default();
            let mut deferred = VecDeque::new();
            let gate = Arc::new(Notify::new());
            if matches!(case, "count" | "byte-control") {
                gate.notify_one();
            }
            let processor = f.processor.clone();
            let dispatch = move |context: crate::request_context::ConnectionContext,
                                 payload: String| {
                let gate = gate.clone();
                let processor = processor.clone();
                async move {
                    gate.notified().await;
                    processor.process_request(&context, &payload).await;
                }
            };
            let pump = process_with_access_renewals(
                async {
                    ordinary.join().await.unwrap();
                },
                &mut reader,
                &mut deferred,
                &f.auth,
                &f.processor.session_manager,
                &mut context,
                &renewals,
                &f.outbound,
                &mut handlers,
                &dispatch,
            );
            let driver = async {
                if matches!(case, "count" | "byte-control") {
                    // At the exact bound, control is still visible. The NEXT
                    // ordinary frame has a terminal overload outcome.
                    let response = recv_response_by_id(&mut f.responses, &id("at-full")).await;
                    assert_eq!(response.result["turn"]["id"], TURN);
                    input.send(Message::Binary(vec![2].into())).unwrap();
                }
                f.responses.recv().await.unwrap()
            };
            let (alive, close) = tokio::join!(pump, driver);
            assert!(
                !alive,
                "{case}: overload must close without waiting for ordinary work"
            );
            let Message::Close(Some(close)) = close else {
                panic!("explicit overload close: {case}")
            };
            assert_eq!(close.code, if case == "single" { 1009 } else { 1013 });
            assert!(deferred.len() <= 64);
            let bytes: usize = deferred
                .iter()
                .map(|message| match message {
                    Message::Text(p) => p.len(),
                    Message::Binary(p) => p.len(),
                    _ => 0,
                })
                .sum();
            assert!(bytes <= 4 * 1024 * 1024);
            handlers.shutdown().await;
            ordinary.abort_and_join().await;
            f.assert_task_running().await;
        })
        .await
        .unwrap_or_else(|_| panic!("bounded overload failed: {case}"));
    }
}

#[derive(Clone, Copy, Debug)]
enum Loss {
    Target,
    SessionRevoke,
    Disconnect,
    Expiry,
    None,
    Renew,
}

#[tokio::test]
async fn wait_reauthorizes_original_task_and_run_targets_after_initial_proof() {
    for run in [false, true] {
        for loss in [
            Loss::Target,
            Loss::SessionRevoke,
            Loss::Disconnect,
            Loss::Expiry,
            Loss::None,
            Loss::Renew,
        ] {
            tokio::time::timeout(Duration::from_secs(10), async {
                let f = Fixture::new().await;
                materialize_test_member_collaborator(&f.processor.crud_store, &f.workspace, THREAD)
                    .await;
                let mut principal = (*authenticated_test_member_collaborator()).clone();
                if matches!(loss, Loss::Expiry | Loss::Renew) {
                    principal.access_expires_at_unix = crate::helpers::unix_timestamp_secs()
                        .unwrap()
                        + if matches!(loss, Loss::Expiry) { 1 } else { 60 };
                }
                let (tx, mut responses) = mpsc::channel(16);
                let connection = f
                    .processor
                    .session_manager
                    .register_connection(tx, Arc::new(principal))
                    .await
                    .unwrap();
                let context =
                    registered_request_context(&f.processor, connection, methods::TASK_WAIT).await;
                let params = TaskWaitParams {
                    task_ids: if run { vec![] } else { vec![TASK.into()] },
                    run_ids: if run { vec![RUN.into()] } else { vec![] },
                    timeout_ms: None,
                    return_completed: true,
                    return_pending: true,
                    ..Default::default()
                };
                let request: JsonRpcRequest = serde_json::from_str(&rpc(
                    "wait-final",
                    methods::TASK_WAIT,
                    serde_json::to_value(&params).unwrap(),
                ))
                .unwrap();
                let admission = f
                    .processor
                    .authorize_normal_request(&context, &request)
                    .await
                    .unwrap_or_else(|_| panic!("fixture initial target proof must pass"));
                let super::super::dispatch::RequestAdmission::TaskBatch(proofs) = admission else {
                    panic!("Member must have scoped target proofs")
                };
                let waiting = f
                    .processor
                    .task_wait(&context, Some(&proofs), request.id, params);
                tokio::pin!(waiting);
                // Start the genuine handler while its target is still running.
                // Its first Pending may be a database read, not the wait loop.
                assert!(waiting.as_mut().now_or_never().is_none());
                // Keep both sides live when they share the fixture's connection.
                let (_, replacement) = tokio::join!(waiting, async {
                    match loss {
                        Loss::Target => {
                            pioneer_entity::thread_membership::Entity::delete_many()
                                .filter(
                                    pioneer_entity::thread_membership::Column::PrincipalId
                                        .eq(TEST_COLLABORATOR_PRINCIPAL_ID),
                                )
                                .exec(&f.processor.crud_store.database_connection())
                                .await
                                .unwrap();
                            pioneer_entity::workspace_membership::Entity::delete_many()
                                .filter(
                                    pioneer_entity::workspace_membership::Column::PrincipalId
                                        .eq(TEST_COLLABORATOR_PRINCIPAL_ID),
                                )
                                .exec(&f.processor.crud_store.database_connection())
                                .await
                                .unwrap();
                        }
                        Loss::SessionRevoke => {
                            f.processor
                                .session_manager
                                .disconnect_session(
                                    &authenticated_test_member_collaborator().session_id,
                                    pioneer_protocol::AuthSessionTerminationReason::SessionRevoked,
                                )
                                .await;
                        }
                        Loss::Disconnect => {
                            f.processor
                                .session_manager
                                .unregister_connection(connection)
                                .await;
                        }
                        Loss::Expiry => {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                        Loss::Renew => {
                            let current = f
                                .processor
                                .session_manager
                                .connection_context(connection)
                                .await
                                .unwrap();
                            let mut renewed = current.principal().clone();
                            renewed.access_jti = "renewed-wait-access".into();
                            renewed.access_expires_at_unix += 600;
                            // Preserve the same session/role, while changing access.
                            f.processor
                                .session_manager
                                .renew_connection_principal(
                                    connection,
                                    current.principal(),
                                    Arc::new(renewed),
                                )
                                .await
                                .unwrap();
                        }
                        Loss::None => {}
                    }
                    let replacement = if matches!(loss, Loss::Disconnect) {
                        let (new_tx, new_rx) = mpsc::channel(4);
                        let new = f
                            .processor
                            .session_manager
                            .register_connection(new_tx, authenticated_test_member_collaborator())
                            .await
                            .unwrap();
                        assert_ne!(new, connection);
                        Some(new_rx)
                    } else {
                        None
                    };
                    // Complete the actual persisted target. Service rescan, not an
                    // injected authorized reply, releases the no-timeout wait.
                    pioneer_entity::task_run::Entity::update_many()
                        .col_expr(
                            pioneer_entity::task_run::Column::Status,
                            Expr::value("succeeded"),
                        )
                        .filter(pioneer_entity::task_run::Column::Id.eq(RUN))
                        .exec(&f.processor.crud_store.database_connection())
                        .await
                        .unwrap();
                    pioneer_entity::task::Entity::update_many()
                        .col_expr(
                            pioneer_entity::task::Column::Status,
                            Expr::value("completed"),
                        )
                        .filter(pioneer_entity::task::Column::Id.eq(TASK))
                        .exec(&f.processor.crud_store.database_connection())
                        .await
                        .unwrap();
                    replacement
                });
                let mut successful = false;
                while let Ok(message) = responses.try_recv() {
                    if let Message::Text(text) = message {
                        let value: JsonValue = serde_json::from_str(&text).unwrap();
                        if value["id"] == id("wait-final") && value.get("result").is_some() {
                            successful = true;
                            let response: pioneer_protocol::PublicTaskWaitResponse =
                                serde_json::from_value(value["result"].clone()).unwrap();
                            assert!(!response.timed_out);
                        }
                    }
                }
                assert_eq!(
                    successful,
                    matches!(loss, Loss::None | Loss::Renew),
                    "run={run}, loss={loss:?}"
                );
                // Re-registering the same principal/session must never receive
                // this old request's result on its new connection identity.
                if let Some(mut new_rx) = replacement {
                    assert!(new_rx.try_recv().is_err());
                }
            })
            .await
            .unwrap_or_else(|_| panic!("wait final authorization: run={run}, loss={loss:?}"));
        }
    }
}

#[tokio::test]
async fn task_wait_timeout_preserves_pending_result_and_durable_work() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut f = Fixture::new().await;
        f.processor
            .process_request_for_connection(
                f.connection,
                &rpc(
                    "timed",
                    methods::TASK_WAIT,
                    serde_json::to_value(TaskWaitParams {
                        run_ids: vec![RUN.into()],
                        timeout_ms: Some(1),
                        return_pending: true,
                        ..Default::default()
                    })
                    .unwrap(),
                ),
            )
            .await;
        let response = recv_response_by_id(&mut f.responses, &id("timed")).await;
        let response: pioneer_protocol::PublicTaskWaitResponse =
            serde_json::from_value(response.result).unwrap();
        assert!(response.timed_out);
        assert_eq!(response.pending.len(), 1);
        f.assert_task_running().await;
    })
    .await
    .expect("task wait timeout remains an observation timeout");
}

#[tokio::test]
async fn durable_session_revocation_during_wait_is_not_hidden_by_original_principal() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut f = Fixture::new().await;
        let processor = Arc::new((*f.processor).clone().with_auth_service(f.auth.clone()));
        let context =
            registered_request_context(&processor, f.connection, methods::TASK_WAIT).await;
        f.auth
            .validate_session_lease(context.principal())
            .await
            .expect("initial durable session is live");
        let params = TaskWaitParams {
            run_ids: vec![RUN.into()],
            timeout_ms: None,
            return_completed: true,
            ..Default::default()
        };
        let waiting = processor.task_wait(
            &context,
            None,
            pioneer_protocol::RequestId::new(id("durable-revoke")).unwrap(),
            params,
        );
        tokio::pin!(waiting);
        assert!(waiting.as_mut().now_or_never().is_none());
        // Keep polling the handler while mutating the shared single-connection
        // fixture: its first Pending may still hold a database connection.
        tokio::join!(waiting, async {
            // Persist revoke without the runtime disconnect hook: final authorization
            // must discover it through the existing durable lease validator itself.
            pioneer_entity::auth_session::Entity::update_many()
                .col_expr(
                    pioneer_entity::auth_session::Column::Status,
                    Expr::value("revoked"),
                )
                .col_expr(
                    pioneer_entity::auth_session::Column::RevokedAt,
                    Expr::current_timestamp(),
                )
                .col_expr(
                    pioneer_entity::auth_session::Column::RevokeReason,
                    Expr::value("self_revoke"),
                )
                .filter(
                    pioneer_entity::auth_session::Column::Id
                        .eq(context.principal().session_id.as_str()),
                )
                .exec(&processor.crud_store.database_connection())
                .await
                .unwrap();
            pioneer_entity::task_run::Entity::update_many()
                .col_expr(
                    pioneer_entity::task_run::Column::Status,
                    Expr::value("succeeded"),
                )
                .filter(pioneer_entity::task_run::Column::Id.eq(RUN))
                .exec(&processor.crud_store.database_connection())
                .await
                .unwrap();
            pioneer_entity::task::Entity::update_many()
                .col_expr(
                    pioneer_entity::task::Column::Status,
                    Expr::value("completed"),
                )
                .filter(pioneer_entity::task::Column::Id.eq(TASK))
                .exec(&processor.crud_store.database_connection())
                .await
                .unwrap();
        });
        let _ = recv_error_by_id(&mut f.responses, &id("durable-revoke")).await;
        assert!(
            f.responses.try_recv().is_err(),
            "no protected result after durable revoke"
        );
    })
    .await
    .expect("durable lease checked after wait");
}
