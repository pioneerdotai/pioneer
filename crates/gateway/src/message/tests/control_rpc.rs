//! Unrun owner-boundary regressions. The stream supplies frames; the production
//! pump owns classification/admission and every domain request uses dispatch.
use super::*;
use crate::transport::{
    ConnectionAccessLease, ConnectionHandlers, IngressMode, process_with_access_renewals,
};
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
        // Message mode intentionally discards Work on completion. Admit an
        // Agent turn so the visible item below survives terminal projection.
        let (mut processor, store, workspace) =
            setup_execution_window_terminal_turn_with_input_and_mode(
                THREAD,
                TURN,
                &[],
                Some(ThreadMode::Agent),
            )
            .await;
        store
            .materialize_item_completed(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: workspace.clone(),
                    thread_id: THREAD.into(),
                    turn_id: TURN.into(),
                    item: pioneer_protocol::TurnItem::Reasoning {
                        id: "control-read-work".into(),
                        summary: vec!["Read fixture work".into()],
                        content: Vec::new(),
                    },
                },
                now_timestamp_secs(),
            )
            .await
            .unwrap();
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
            IngressMode::Serial,
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
    for (mode, case) in [IngressMode::Serial, IngressMode::ReadGroup]
        .into_iter()
        .flat_map(|mode| {
            ["count", "bytes", "byte-control", "single", "cancel", "wait"]
                .into_iter()
                .map(move |case| (mode, case))
        })
    {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut f = Fixture::new().await;
            let mut ordinary = if mode == IngressMode::Serial {
                Some(f.ordinary().await)
            } else {
                None
            };
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
            let mut deferred = if mode == IngressMode::ReadGroup {
                VecDeque::from([get_thread("overflow-block")])
            } else {
                VecDeque::new()
            };
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
                    if serde_json::from_str::<JsonRpcRequest>(&payload)
                        .unwrap()
                        .id
                        .as_str()
                        == id("overflow-block")
                    {
                        std::future::pending::<()>().await;
                    }
                    gate.notified().await;
                    processor.process_request(&context, &payload).await;
                }
            };
            let pump = process_with_access_renewals(
                async {
                    if let Some(ordinary) = ordinary.as_mut() {
                        ordinary.join().await.unwrap();
                    } else {
                        std::future::pending::<()>().await;
                    }
                },
                mode,
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
            if let Some(ordinary) = ordinary.as_mut() {
                ordinary.abort_and_join().await;
            }
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

// These gates delay execution only. The production pump classifies/adopts the
// frames and the real dispatcher supplies every result/error and domain effect.
struct ReadDispatchGate {
    blocked: HashSet<String>,
    entered: mpsc::UnboundedSender<String>,
    dropped: mpsc::UnboundedSender<String>,
    release: tokio::sync::Semaphore,
}
struct ReadDispatchDrop {
    id: String,
    gate: Arc<ReadDispatchGate>,
}
impl Drop for ReadDispatchDrop {
    fn drop(&mut self) {
        let _ = self.gate.dropped.send(self.id.clone());
    }
}
fn gated_read_dispatch(
    processor: Arc<MessageProcessor>,
    gate: Arc<ReadDispatchGate>,
) -> impl Fn(crate::request_context::ConnectionContext, String) -> MessageFuture<'static, ()> {
    move |context, payload| {
        let processor = processor.clone();
        let gate = gate.clone();
        message_future(async move {
            let request: JsonRpcRequest = serde_json::from_str(&payload).unwrap();
            let request_id = request.id.as_str().to_owned();
            if gate.blocked.contains(&request_id) {
                let _drop = ReadDispatchDrop {
                    id: request_id.clone(),
                    gate: gate.clone(),
                };
                gate.entered.send(request_id).unwrap();
                gate.release.acquire().await.unwrap().forget();
                processor.process_request(&context, &payload).await;
            } else {
                processor.process_request(&context, &payload).await;
            }
        })
    }
}
fn read_gate(
    labels: &[&str],
) -> (
    Arc<ReadDispatchGate>,
    mpsc::UnboundedReceiver<String>,
    mpsc::UnboundedReceiver<String>,
) {
    let (entered, entries) = mpsc::unbounded_channel();
    let (dropped, drops) = mpsc::unbounded_channel();
    (
        Arc::new(ReadDispatchGate {
            blocked: labels.iter().map(|label| id(label)).collect(),
            entered,
            dropped,
            release: tokio::sync::Semaphore::new(0),
        }),
        entries,
        drops,
    )
}
fn get_thread(label: &str) -> Message {
    Message::Text(rpc(label, methods::THREAD_GET, json!({"thread_id":THREAD})).into())
}

#[tokio::test]
async fn independent_read_finishes_before_blocked_peer_and_serial_head_fences_later_reads() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut f = Fixture::new().await;
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
        let (gate, mut entries, mut drops) = read_gate(&["read-a"]);
        let dispatch = gated_read_dispatch(f.processor.clone(), gate.clone());
        let mut handlers = ConnectionHandlers::default();
        let mut deferred = VecDeque::from([get_thread("read-a")]);
        let (input, mut reader) = incoming();
        let update = Message::Text(
            rpc(
                "rename",
                methods::THREAD_UPDATE,
                json!({"workspace_id":f.workspace,"thread_id":THREAD,"name":"After barrier"}),
            )
            .into(),
        );
        let read_c = get_thread("read-c");
        let binary = Message::Binary(vec![4, 5].into());
        let malformed = Message::Text("{broken".into());
        let pump = process_with_access_renewals(
            std::future::pending(),
            IngressMode::ReadGroup,
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
            assert_eq!(entries.recv().await.unwrap(), id("read-a"));
            // Sustained reads and controls must both progress with A held.
            for round in 0..32 {
                let read = format!("read-b-{round:02}");
                let (method, params, field) = match round % 5 {
                    0 => (methods::THREAD_TREE, json!({"workspace_id":f.workspace}), "threads"),
                    1 => (methods::THREAD_GET, json!({"thread_id":THREAD}), "thread"),
                    2 => (methods::THREAD_TIMELINE_PAGE, json!({"threadId":THREAD,"anchor":{"kind":"newest"},"limit":5}), "blocks"),
                    3 => (methods::TURN_WORK_PAGE, json!({"threadId":THREAD,"turnId":TURN,"anchor":{"kind":"oldest"},"limit":5}), "items"),
                    _ => (methods::TURN_WORK_ITEMS_GET, json!({"threadId":THREAD,"turnId":TURN,"workItemIds":["missing"]}), "removedWorkItemIds"),
                };
                input.send(Message::Text(rpc(&read, method, params).into())).unwrap();
                let b = recv_response_by_id(&mut f.responses, &id(&read)).await;
                assert!(if field == "thread" { b.result[field].is_object() } else { b.result[field].is_array() }, "{method}: {b:?}");
                let cancel = format!("rc-{round:02}");
                input.send(Message::Text(rpc(&cancel, methods::TURN_CANCEL,
                    json!({"thread_id":THREAD,"turn_id":TURN})).into())).unwrap();
                recv_response_by_id(&mut f.responses, &id(&cancel)).await;
            }
            // This is an observable independent completion while A has no permit.
            assert!(drops.try_recv().is_err());
            input.send(update.clone()).unwrap();
            input.send(read_c.clone()).unwrap();
            input.send(binary.clone()).unwrap();
            input.send(malformed.clone()).unwrap();
            input
                .send(Message::Text(
                    rpc(
                        "read-marker",
                        methods::TURN_CANCEL,
                        json!({"thread_id":THREAD,"turn_id":TURN}),
                    )
                    .into(),
                ))
                .unwrap();
            recv_response_by_id(&mut f.responses, &id("read-marker")).await;
            assert!(
                f.responses.try_recv().is_err(),
                "no serial or later-read reply before A completes"
            );
            gate.release.add_permits(1);
            recv_response_by_id(&mut f.responses, &id("read-a")).await;
        };
        let (alive, ()) = tokio::join!(pump, driver);
        assert!(alive);
        assert_eq!(
            deferred,
            VecDeque::from([update, read_c, binary, malformed])
        );
        handlers.shutdown().await;
        assert_eq!(drops.recv().await.unwrap(), id("read-a"));
    })
    .await
    .expect("independent completion and conservative serial/Binary barrier");
}

#[tokio::test]
async fn full_read_slots_defer_and_reuse_capacity_without_spending_cancel_or_wait_slots() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut f = Fixture::new().await;
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
        let (gate, mut entries, mut drops) =
            read_gate(&["slot0", "slot1", "slot2", "slot3", "slot4"]);
        let dispatch = gated_read_dispatch(f.processor.clone(), gate.clone());
        let mut handlers = ConnectionHandlers::default();
        let mut deferred = VecDeque::from([get_thread("slot0")]);
        let (input, mut reader) = incoming();
        let pump = process_with_access_renewals(
            std::future::pending(),
            IngressMode::ReadGroup,
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
            for label in ["slot1", "slot2", "slot3"] {
                input.send(get_thread(label)).unwrap();
            }
            let mut started = HashSet::new();
            for _ in 0..4 {
                started.insert(entries.recv().await.unwrap());
            }
            assert_eq!(started.len(), 4);
            input.send(get_thread("slot4")).unwrap();
            input
                .send(Message::Text(
                    rpc("slot-wait", methods::TASK_WAIT, json!({"run_ids":[RUN]})).into(),
                ))
                .unwrap();
            input
                .send(Message::Text(
                    rpc(
                        "slot-cancel",
                        methods::TURN_CANCEL,
                        json!({"thread_id":THREAD,"turn_id":TURN}),
                    )
                    .into(),
                ))
                .unwrap();
            recv_response_by_id(&mut f.responses, &id("slot-cancel")).await;
            assert!(entries.try_recv().is_err(), "fifth read must stay queued");
            gate.release.add_permits(1);
            assert_eq!(
                entries.recv().await.unwrap(),
                id("slot4"),
                "completion must reclaim a read slot"
            );
            // Close while the other read children and no-timeout wait are blocked.
            input.send(Message::Close(None)).unwrap();
        };
        let (alive, ()) = tokio::join!(pump, driver);
        assert!(!alive);
        handlers.shutdown().await;
        let mut finished = HashSet::new();
        for _ in 0..5 {
            finished.insert(drops.recv().await.unwrap());
        }
        assert_eq!(
            finished.len(),
            5,
            "all started children joined before resources retire"
        );
        f.assert_task_running().await;
    })
    .await
    .expect("bounded read capacity, control reserve, slot reuse and disconnect cleanup");
}

#[tokio::test]
async fn read_classification_keeps_actual_dispatcher_validation_and_target_authorization() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut f = Fixture::new().await;
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
        let (gate, mut entries, _) = read_gate(&["validation-block"]);
        let dispatch = gated_read_dispatch(f.processor.clone(), gate.clone());
        let mut handlers = ConnectionHandlers::default();
        let mut deferred = VecDeque::from([get_thread("validation-block")]);
        let (input, mut reader) = incoming();
        let pump = process_with_access_renewals(
            std::future::pending(),
            IngressMode::ReadGroup,
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
            entries.recv().await.unwrap();
            for (label, payload) in [
                (
                    "read-version",
                    rpc(
                        "read-version",
                        methods::THREAD_GET,
                        json!({"thread_id":THREAD}),
                    )
                    .replace("2.0", "1.0"),
                ),
                (
                    "read-params",
                    rpc("read-params", methods::THREAD_GET, json!({})),
                ),
                (
                    "read-target",
                    rpc(
                        "read-target",
                        methods::THREAD_GET,
                        json!({"thread_id":"missing"}),
                    ),
                ),
            ] {
                input.send(Message::Text(payload.into())).unwrap();
                recv_error_by_id(&mut f.responses, &id(label)).await;
            }
            input.send(Message::Close(None)).unwrap();
        };
        let (alive, ()) = tokio::join!(pump, driver);
        assert!(!alive);
        handlers.shutdown().await;
    })
    .await
    .expect("allowlist cannot supply authority or bypass validation");
}

#[tokio::test]
async fn socket_serial_predecessors_finish_before_explicit_and_implicit_reads() {
    use axum::{Router, extract::ws::WebSocketUpgrade, routing::get};
    use futures_util::SinkExt;
    use tokio_tungstenite::{connect_async, tungstenite::Message as ClientMessage};
    tokio::time::timeout(Duration::from_secs(15), async {
        let f = Fixture::new().await;
        let workspace_b = f.processor.workspace_manager.create_workspace("read_barrier_b", Some("B")).await.unwrap().id;
        let (stop, stop_rx) = watch::channel(false);
        let (finished, completion) = tokio::sync::oneshot::channel();
        let finished = Arc::new(std::sync::Mutex::new(Some(finished)));
        let runtime_home = tempfile::tempdir().unwrap();
        let processor = Arc::new((*f.processor).clone().with_runtime_home_for_tests(runtime_home.path().to_path_buf()));
        let auth = f.auth.clone();
        let router = Router::new().route("/ws", get(move |upgrade: WebSocketUpgrade| {
            let processor = processor.clone();
            let auth = auth.clone();
            let stop_rx = stop_rx.clone();
            let finished = finished.clone();
            async move {
                upgrade.on_upgrade(move |socket| async move {
                    let mut principal = (*authenticated_test_superuser()).clone();
                    principal.access_expires_at_unix = crate::helpers::unix_timestamp_secs().unwrap() + 3600;
                    let result = crate::transport::run_normal_connection(socket,
                        toml::from_str(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/default.toml"))).unwrap(),
                        Arc::new(principal), auth, processor.clone(), processor.session_manager.clone(), stop_rx).await;
                    let _ = finished.lock().unwrap().take().unwrap().send(result);
                })
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut server = crate::message::spawn_message_task(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let (mut socket, _) = connect_async(format!("ws://{address}/ws")).await.unwrap();
        socket.send(ClientMessage::Text(rpc("socket-block", methods::PROVIDER_MODELS_LIST,
            json!({"workspace_id":f.workspace,"provider":"openai"})).into())).await.unwrap();
        f.gate.entered.notified().await;
        for frame in [
            rpc("socket-write", methods::THREAD_UPDATE, json!({"workspace_id":f.workspace,"thread_id":THREAD,"name":"Committed before read"})),
            rpc("socket-select", methods::WORKSPACE_SELECT, json!({"workspace_id":workspace_b,"make_current":false})),
            rpc("socket-implicit", methods::SETTINGS_GET, json!({})),
            rpc("socket-read", methods::THREAD_GET, json!({"thread_id":THREAD})),
            rpc("socket-page", methods::THREAD_TIMELINE_PAGE, json!({"threadId":THREAD,"anchor":{"kind":"newest"},"limit":10})),
            rpc("socket-marker", methods::TURN_CANCEL, json!({"thread_id":THREAD,"turn_id":TURN})),
        ] { socket.send(ClientMessage::Text(frame.into())).await.unwrap(); }
        // Read without filtering away early replies: a read bypassing a serial
        // predecessor would be a failure even if its final contents looked right.
        loop {
            let frame = socket.next().await.unwrap().unwrap();
            if let ClientMessage::Text(text) = frame {
                let value: JsonValue = serde_json::from_str(&text).unwrap();
                if value.get("id").is_some() {
                    assert_eq!(value["id"], id("socket-marker"), "ordinary FIFO must remain blocked");
                    assert!(value.get("error").is_none(), "{value}");
                    break;
                }
            }
        }
        f.gate.release.notify_one();
        // Keep a control producer active until ordinary/read completion, rather
        // than ending the input stream to let the FIFO drain. One outstanding
        // Cancel respects its distinct capacity even on a slow domain handler.
        let mut flood_round = 0;
        socket.send(ClientMessage::Text(rpc("flood000000", methods::TURN_CANCEL,
            json!({"thread_id":THREAD,"turn_id":TURN})).into())).await.unwrap();
        let mut replies = Vec::new();
        while replies.len() < 6 {
            if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                let value: JsonValue = serde_json::from_str(&text).unwrap();
                if let Some(request) = value.get("id").and_then(JsonValue::as_str) {
                    assert!(value.get("error").is_none(), "{value}");
                    if request == id(&format!("flood{flood_round:06}")) {
                        flood_round += 1;
                        socket.send(ClientMessage::Text(rpc(&format!("flood{flood_round:06}"), methods::TURN_CANCEL,
                            json!({"thread_id":THREAD,"turn_id":TURN})).into())).await.unwrap();
                        continue;
                    }
                    if request == id("socket-implicit") {
                        assert_eq!(value["result"]["settings"]["self_improvement_status"]["workspace_id"], workspace_b);
                    }
                    if request == id("socket-read") {
                        assert_eq!(value["result"]["thread"]["name"], "Committed before read");
                    }
                    replies.push(request.to_owned());
                }
            }
        }
        assert_eq!(&replies[..4], &[id("socket-block"), id("socket-write"), id("socket-select"), id("socket-implicit")]);
        assert_eq!(replies[4..].iter().cloned().collect::<HashSet<_>>(), HashSet::from([id("socket-read"), id("socket-page")]));
        let selected = f.processor.session_manager.connection_ids().await.into_iter().filter(|id| *id != f.connection).collect::<Vec<_>>();
        assert_eq!(selected.len(), 1);
        assert_eq!(f.processor.session_manager.connection_workspace_id(selected[0]).await.as_deref(), Some(workspace_b.as_str()), "late resource A page must preserve selected B");
        stop.send(true).unwrap();
        completion.await.unwrap().unwrap();
        assert_eq!(f.processor.session_manager.connection_ids().await, vec![f.connection]);
        drop(socket);
        server.abort_and_join().await;
    }).await.expect("actual connection FIFO, selection freshness and write/read barrier");
}
