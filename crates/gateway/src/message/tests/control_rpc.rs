//! Unrun owner-boundary regressions. The stream supplies frames; the production
//! pump owns classification/admission and every domain request uses dispatch.
use super::*;
use crate::request_context::RequestContext;
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
    exited: Notify,
}
struct ModelGateDrop<'a>(&'a Notify);
impl Drop for ModelGateDrop<'_> {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
#[async_trait::async_trait]
impl Provider for ModelGate {
    fn name(&self) -> &str {
        "openai"
    }
    async fn list_models(&self) -> anyhow::Result<Vec<pioneer_protocol::ProviderModelInfo>> {
        let _drop = ModelGateDrop(&self.exited);
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
    responses: crate::session::test_support::OutboundTestReceiver,
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
            exited: Notify::new(),
        });
        processor.provider_registry =
            Arc::new(ProviderRegistry::with_provider("openai", gate.clone()));
        let (outbound, responses) = crate::session::test_support::outbound_channel(128);
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
                    rpc("wait", methods::TASK_WAIT, json!({"runIds":[RUN]})).into(),
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
                                    json!({"runIds":[RUN]}),
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
                let (tx, mut responses) = crate::session::test_support::outbound_channel(16);
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
                        let (new_tx, new_rx) = crate::session::test_support::outbound_channel(4);
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
                    rpc("slot-wait", methods::TASK_WAIT, json!({"runIds":[RUN]})).into(),
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

/// The real normal owner and real WebSocket, with only scheduling/fault gates
/// injected at the writer boundary. Tests never perform connection cleanup.
struct OutboundSocket {
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    connection: ConnectionId,
    outbound: crate::session::SessionOutbound,
    start_writer: tokio::sync::oneshot::Sender<()>,
    completion: tokio::sync::oneshot::Receiver<anyhow::Result<()>>,
    stop: watch::Sender<bool>,
    server: crate::message::AbortOnDropMessageTask<()>,
    _home: tempfile::TempDir,
}
impl OutboundSocket {
    async fn new(
        f: &Fixture,
        stalled: bool,
    ) -> (
        Self,
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        use axum::{Router, extract::ws::WebSocketUpgrade, routing::get};
        let (stop, stop_rx) = watch::channel(false);
        let (finished, completion) = tokio::sync::oneshot::channel();
        let finished = Arc::new(std::sync::Mutex::new(Some(finished)));
        let (registered, registration) = tokio::sync::oneshot::channel();
        let (start_writer, writer_start) = tokio::sync::oneshot::channel();
        let (block, write_block) = tokio::sync::oneshot::channel();
        let (write_entered, entered) = tokio::sync::oneshot::channel();
        let hooks = Arc::new(std::sync::Mutex::new(Some(
            crate::transport::NormalConnectionTestHooks {
                registered,
                writer_start,
                write_block: stalled.then_some(write_block),
                write_entered: Some(write_entered),
            },
        )));
        let home = tempfile::tempdir().unwrap();
        let processor = Arc::new(
            (*f.processor)
                .clone()
                .with_runtime_home_for_tests(home.path().to_path_buf()),
        );
        let auth = f.auth.clone();
        let router = Router::new().route(
            "/ws",
            get(move |upgrade: WebSocketUpgrade| {
                let processor = processor.clone();
                let auth = auth.clone();
                let stop_rx = stop_rx.clone();
                let finished = finished.clone();
                let hooks = hooks.lock().unwrap().take().expect("one test connection");
                async move {
                    upgrade.on_upgrade(move |socket| async move {
                        let mut principal = (*authenticated_test_superuser()).clone();
                        principal.access_expires_at_unix =
                            crate::helpers::unix_timestamp_secs().unwrap() + 3600;
                        let mut config: pioneer_config::AppConfig = toml::from_str(include_str!(
                            concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/default.toml")
                        ))
                        .unwrap();
                        if stalled {
                            config.gateway.outbound.write_timeout_ms = 50;
                        }
                        let result = crate::transport::run_normal_connection_inner(
                            socket,
                            config,
                            Arc::new(principal),
                            auth,
                            processor.clone(),
                            processor.session_manager.clone(),
                            stop_rx,
                            Some(hooks),
                        )
                        .await;
                        let _ = finished.lock().unwrap().take().unwrap().send(result);
                    })
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = crate::message::spawn_message_task(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/ws"))
            .await
            .unwrap();
        let (connection, outbound) = registration.await.unwrap();
        (
            Self {
                socket,
                connection,
                outbound,
                start_writer,
                completion,
                stop,
                server,
                _home: home,
            },
            block,
            entered,
        )
    }
}

#[tokio::test]
async fn sealed_normal_writer_delivers_shutdown_and_revoke_fifo_before_close() {
    use tokio_tungstenite::tungstenite::Message as ClientMessage;
    tokio::time::timeout(Duration::from_secs(15), async {
        for revoke in [false, true] {
            let f = Fixture::new().await;
            let (mut wire, _block, _entered) = OutboundSocket::new(&f, false).await;
            if revoke {
                wire.outbound
                    .send(Message::Text("committed-revoke-response".into()))
                    .await
                    .unwrap();
                f.processor
                    .session_manager
                    .disconnect_session(
                        &authenticated_test_superuser().session_id,
                        pioneer_protocol::AuthSessionTerminationReason::SessionRevoked,
                    )
                    .await;
            } else {
                wire.stop.send(true).unwrap();
            }
            // No writer poll has occurred. Seal must not cancel the accepted FIFO.
            wire.outbound.closed().await;
            assert!(
                wire.outbound
                    .send(Message::Text("late".into()))
                    .await
                    .is_err()
            );
            wire.start_writer.send(()).unwrap();
            if revoke {
                let first = wire.socket.next().await.unwrap().unwrap();
                assert!(
                    matches!(first, ClientMessage::Text(t) if t == "committed-revoke-response")
                );
                let notification = wire
                    .socket
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_text()
                    .unwrap();
                let value: JsonValue = serde_json::from_str(&notification).unwrap();
                assert_eq!(value["method"], events::AUTH_SESSION_REVOKED);
            }
            let close = wire.socket.next().await.unwrap().unwrap();
            let ClientMessage::Close(Some(close)) = close else {
                panic!("terminal Close required: {close:?}");
            };
            assert_eq!(u16::from(close.code), if revoke { 4403 } else { 1012 });
            assert_eq!(
                close.reason.as_str(),
                if revoke {
                    "session_revoked"
                } else {
                    "server_shutdown"
                }
            );
            wire.completion.await.unwrap().unwrap();
            assert!(
                f.processor
                    .session_manager
                    .connection_principal(wire.connection)
                    .await
                    .is_err()
            );
            wire.server.abort_and_join().await;
        }
    })
    .await
    .expect("sealed writer must attempt terminal control within the existing grace");
}

#[tokio::test]
async fn normal_writer_deadline_joins_pending_handler_and_cleans_connection_resources() {
    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message as ClientMessage;
    tokio::time::timeout(Duration::from_secs(15), async {
        let f = Fixture::new().await;
        let (mut wire, block, entered) = OutboundSocket::new(&f, true).await;
        let principal = authenticated_test_superuser();
        let owner = AuthenticatedTransferOwner {
            principal_id: principal.principal_id.clone(),
            auth_session_id: principal.session_id.clone(),
            connection_id: wire.connection,
        };
        let upload = f
            .processor
            .artifact_uploads
            .start(
                &owner,
                ArtifactUploadStartParams {
                    workspace_id: f.workspace.clone(),
                    thread_id: None,
                    planned_turn_id: None,
                    client_attachment_id: "writer-cleanup".to_owned(),
                    file_name: "cleanup.txt".to_owned(),
                    mime_type: Some("text/plain".to_owned()),
                    size_bytes: 1,
                    sha256: hex::encode(Sha256::digest(b"x")),
                    source_kind: ArtifactUploadSourceKind::UserComposer,
                },
                now_timestamp_secs(),
            )
            .await
            .unwrap();
        assert!(
            upload.temp_path.exists(),
            "fixture must own a real upload file before failure"
        );
        wire.socket
            .send(ClientMessage::Text(
                rpc(
                    "writer-pending",
                    methods::PROVIDER_MODELS_LIST,
                    json!({"workspace_id":f.workspace,"provider":"openai"}),
                )
                .into(),
            ))
            .await
            .unwrap();
        f.gate.entered.notified().await;
        assert!(f.gate.exited.notified().now_or_never().is_none());
        wire.outbound
            .send(Message::Text("writer-trigger".into()))
            .await
            .unwrap();
        wire.start_writer.send(()).unwrap();
        entered.await.unwrap();
        // The first real writer send is pending inside the production timeout.
        // Neither the provider nor the write gate is released by this test.
        let result = wire.completion.await.unwrap();
        assert!(result.unwrap_err().to_string().contains("deadline"));
        assert!(
            block.is_closed(),
            "writer must have dropped the cancelled write future"
        );
        f.gate
            .exited
            .notified()
            .now_or_never()
            .expect("owner must abort AND join the pending handler before returning");
        assert!(
            f.processor
                .session_manager
                .connection_principal(wire.connection)
                .await
                .is_err()
        );
        assert!(
            wire.outbound
                .send(Message::Text("late".into()))
                .await
                .is_err()
        );
        let error = f
            .processor
            .artifact_uploads
            .append_chunk(
                &owner,
                &ArtifactUploadChunkHeader {
                    workspace_id: f.workspace.clone(),
                    upload_id: upload.upload_id,
                    offset: 0,
                    len: 1,
                    chunk_sha256: None,
                },
                b"x",
                now_timestamp_secs(),
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("artifact upload not found"),
            "MessageProcessor::connection_closed must remove the real upload: {error}"
        );
        assert!(
            !upload.temp_path.exists(),
            "connection cleanup must remove the upload file"
        );
        wire.server.abort_and_join().await;
    })
    .await
    .expect("writer deadline must terminate the normal owner and its pending children");
}

#[tokio::test]
async fn blocked_ordinary_accepts_eighty_voice_frames_without_using_deferred_fifo() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut f = Fixture::new().await;
        let mut ordinary = f.ordinary().await;
        let mut context = f
            .processor
            .session_manager
            .connection_context(f.connection)
            .await
            .unwrap();
        let session = ingress_recording(
            &f.processor,
            &f.workspace,
            &context,
            "voice_ingress_regression",
        );
        let (renewals, _) = watch::channel(ConnectionAccessLease::new(
            context.principal().clone(),
            Duration::from_secs(60),
        ));
        let mut handlers = ConnectionHandlers::default();
        handlers.enable_audio(
            f.processor.clone(),
            f.processor.session_manager.clone(),
            f.auth.clone(),
            f.connection,
            f.outbound.clone(),
        );
        let mut deferred = VecDeque::new();
        let (input, mut reader) = incoming();
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
            // No native recognizer is running. Transport must still accept exact PCM.
            for sequence in 0..80 {
                input
                    .send(ingress_frame(&session.session_id, sequence))
                    .unwrap();
            }
            for sequence in 0..80 {
                let Message::Text(text) = f.responses.recv().await.unwrap() else {
                    panic!("voice traffic closed the socket");
                };
                let value: JsonValue = serde_json::from_str(&text).unwrap();
                assert_eq!(
                    value["method"],
                    pioneer_protocol::constants::events::VOICE_CHUNK_ACK
                );
                assert_eq!(value["params"]["session_id"], session.session_id);
                assert_eq!(value["params"]["sequence"], sequence);
            }
            let audio = f
                .processor
                .voice_session_buffers
                .take_session_audio(&session.session_id)
                .unwrap();
            assert_eq!(audio.chunks.len(), 80);
            for (sequence, chunk) in audio.chunks.iter().enumerate() {
                assert_eq!(chunk.sequence, sequence as u64);
                assert_eq!(chunk.audio_payload, (sequence as i16).to_le_bytes());
                assert_eq!(chunk.captured_at_unix_ms, Some(sequence as u64));
            }
            f.gate.release.notify_one();
            let response = recv_response_by_id(&mut f.responses, &id("ordinary")).await;
            assert!(response.result.is_object() || response.result.is_array());
        };
        let (alive, ()) = tokio::join!(pump, driver);
        assert!(alive);
        assert!(
            deferred.is_empty(),
            "voice never consumes ordinary count/bytes"
        );
        handlers.shutdown().await;
        f.processor.connection_closed(f.connection).await;
    })
    .await
    .expect("audio and ordinary owner both make positive progress");
}

fn ingress_recording(
    processor: &MessageProcessor,
    workspace: &str,
    context: &crate::request_context::ConnectionContext,
    name: &str,
) -> crate::voice::session_store::GatewayVoiceSession {
    let request = RequestContext::new(
        context,
        None,
        crate::request_context::CanonicalMethod::binary("voice/chunk"),
    );
    let owner = AuthenticatedTransferOwner::from_request_context(&request);
    let format = pioneer_protocol::VoiceAudioFormat::pioneer_streaming_target();
    let session = processor
        .voice_sessions
        .create_authenticated_session(
            name,
            owner.clone(),
            pioneer_protocol::VoiceSessionStartContext {
                workspace_id: workspace.to_owned(),
                thread_id: THREAD.into(),
                turn_id: TURN.into(),
            },
            format.clone(),
        )
        .unwrap();
    processor
        .voice_sessions
        .mark_recording_authenticated(name, &owner)
        .unwrap();
    processor
        .voice_session_buffers
        .start_session(name, format)
        .unwrap();
    session
}
fn ingress_frame(session: &str, sequence: u64) -> Message {
    Message::Binary(
        pioneer_protocol::encode_voice_chunk_frame(
            pioneer_protocol::VoiceChunkFrameHeader {
                session_id: session.into(),
                sequence,
                sample_rate_hz: 16_000,
                channels: 1,
                encoding: pioneer_protocol::VoiceAudioEncoding::PcmS16Le,
                payload_len: 0,
                captured_at_unix_ms: Some(sequence),
                duration_ms: None,
            },
            &(sequence as i16).to_le_bytes(),
        )
        .unwrap()
        .into(),
    )
}

#[tokio::test]
async fn paused_audio_ingest_does_not_hold_independent_read_group_or_cancel() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut f = Fixture::new().await;
        let mut context = f
            .processor
            .session_manager
            .connection_context(f.connection)
            .await
            .unwrap();
        let session = ingress_recording(
            &f.processor,
            &f.workspace,
            &context,
            "voice_reverse_ingress",
        );
        assert!(
            f.processor
                .thread_manager
                .subscribe_connection(THREAD, f.connection)
                .await
        );
        let (renewals, _) = watch::channel(ConnectionAccessLease::new(
            context.principal().clone(),
            Duration::from_secs(60),
        ));
        let mut handlers = ConnectionHandlers::default();
        handlers.enable_audio(
            f.processor.clone(),
            f.processor.session_manager.clone(),
            f.auth.clone(),
            f.connection,
            f.outbound.clone(),
        );
        let (entered, ingest_entered) = tokio::sync::oneshot::channel();
        let (release, ingest_release) = tokio::sync::oneshot::channel();
        handlers.pause_audio_once(entered, ingest_release);
        let (gate, mut entries, _) = read_gate(&["held-ingress-read"]);
        let dispatch = gated_read_dispatch(f.processor.clone(), gate.clone());
        let mut deferred = VecDeque::from([get_thread("held-ingress-read")]);
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
            assert_eq!(entries.recv().await.unwrap(), id("held-ingress-read"));
            input.send(ingress_frame(&session.session_id, 0)).unwrap();
            ingest_entered.await.unwrap();
            input.send(get_thread("while-audio-paused")).unwrap();
            let response = recv_response_by_id(&mut f.responses, &id("while-audio-paused")).await;
            assert!(response.result.get("thread").is_some());
            input
                .send(Message::Text(
                    rpc(
                        "cancel-paused-audio",
                        methods::VOICE_SESSION_CANCEL,
                        json!({"session_id":session.session_id,"reason":"test"}),
                    )
                    .into(),
                ))
                .unwrap();
            let response = recv_response_by_id(&mut f.responses, &id("cancel-paused-audio")).await;
            assert_eq!(response.result["cancelled"], true);
            assert!(session.ingress.cancelled.is_cancelled());
            let replacement_context = f
                .processor
                .session_manager
                .connection_context(f.connection)
                .await
                .unwrap();
            let replacement = ingress_recording(
                &f.processor,
                &f.workspace,
                &replacement_context,
                "voice_after_ingress_cancel",
            );
            input.send(ingress_frame(&session.session_id, 1)).unwrap();
            input
                .send(ingress_frame(&replacement.session_id, 0))
                .unwrap();
            let _ = release.send(());
            loop {
                let Message::Text(text) = f.responses.recv().await.unwrap() else {
                    panic!("cancel disconnected unrelated work");
                };
                let value: JsonValue = serde_json::from_str(&text).unwrap();
                if value["method"] == pioneer_protocol::constants::events::VOICE_CHUNK_ACK {
                    assert_eq!(value["params"]["session_id"], replacement.session_id);
                    assert_eq!(value["params"]["sequence"], 0);
                    break;
                }
            }
            assert_eq!(
                f.processor
                    .voice_session_buffers
                    .buffered_bytes_for_test(&replacement.session_id)
                    .unwrap(),
                6 // one PCM i16 sample plus one normalized f32 sample
            );
            gate.release.add_permits(1);
        };
        let (alive, ()) = tokio::join!(pump, driver);
        assert!(alive);
        handlers.shutdown().await;
        f.processor.connection_closed(f.connection).await;
    })
    .await
    .expect("RPC and control progress while audio admission owns active work");
}

#[tokio::test]
async fn audio_capacity_failure_is_terminal_for_its_recording_and_preserves_read_group() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut f = Fixture::new().await;
        let mut context = f
            .processor
            .session_manager
            .connection_context(f.connection)
            .await
            .unwrap();
        let session = ingress_recording(
            &f.processor,
            &f.workspace,
            &context,
            "voice_capacity_ingress",
        );
        let (renewals, _) = watch::channel(ConnectionAccessLease::new(
            context.principal().clone(),
            Duration::from_secs(60),
        ));
        let mut handlers = ConnectionHandlers::default();
        handlers.enable_audio(
            f.processor.clone(),
            f.processor.session_manager.clone(),
            f.auth.clone(),
            f.connection,
            f.outbound.clone(),
        );
        let (entered, ingest_entered) = tokio::sync::oneshot::channel();
        let (_release, ingest_release) = tokio::sync::oneshot::channel();
        handlers.pause_audio_once(entered, ingest_release);
        let (gate, mut entries, _) = read_gate(&["held-overload-read"]);
        let dispatch = gated_read_dispatch(f.processor.clone(), gate.clone());
        let mut deferred = VecDeque::from([get_thread("held-overload-read")]);
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
            assert_eq!(entries.recv().await.unwrap(), id("held-overload-read"));
            input.send(ingress_frame(&session.session_id, 0)).unwrap();
            ingest_entered.await.unwrap();
            // Count includes the gated active item; the 129th cannot fit.
            for sequence in 1..129 {
                input
                    .send(ingress_frame(&session.session_id, sequence))
                    .unwrap();
            }
            loop {
                let Message::Text(text) = f.responses.recv().await.unwrap() else {
                    panic!("voice overload closed the shared socket");
                };
                let value: JsonValue = serde_json::from_str(&text).unwrap();
                if value["method"] == pioneer_protocol::constants::events::VOICE_SESSION_RESULT {
                    let result: pioneer_protocol::VoiceSessionResultNotification =
                        serde_json::from_value(value["params"].clone()).unwrap();
                    assert_eq!(result.session_id, session.session_id);
                    assert_eq!(
                        result.outcome,
                        pioneer_protocol::VoiceSessionOutcome::Failed
                    );
                    assert_eq!(
                        result.error.unwrap().public_error.unwrap().code,
                        pioneer_protocol::PublicErrorCode::ResourceExhausted
                    );
                    break;
                }
            }
            assert!(session.ingress.cancelled.is_cancelled());
            session.ingress.drain().await;
            assert!(session.ingress.admit().is_none());
            assert!(
                f.processor
                    .voice_session_buffers
                    .finish_processing_verified(
                        &session.session_id,
                        Some(pioneer_protocol::VoiceAudioIntegrity {
                            next_sequence: 129,
                            normalized_samples: 129
                        })
                    )
                    .is_err()
            );
            input
                .send(ingress_frame("foreign_voice_recording", 0))
                .unwrap();
            input
                .send(Message::Binary(
                    pioneer_protocol::VOICE_CHUNK_FRAME_MAGIC.to_vec().into(),
                ))
                .unwrap();
            input.send(get_thread("after-voice-overload")).unwrap();
            let response = recv_response_by_id(&mut f.responses, &id("after-voice-overload")).await;
            assert!(response.result.get("thread").is_some());
            gate.release.add_permits(1);
        };
        let (alive, ()) = tokio::join!(pump, driver);
        assert!(alive);
        assert!(deferred.is_empty());
        handlers.shutdown().await;
        f.processor.connection_closed(f.connection).await;
    })
    .await
    .expect("bounded voice overload never retires healthy ordinary transport");
}

#[tokio::test]
async fn audio_worker_retirement_releases_active_and_queued_tickets() {
    for terminal in ["shutdown", "drop", "panic"] {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut f = Fixture::new().await;
            let mut context = f
                .processor
                .session_manager
                .connection_context(f.connection)
                .await
                .unwrap();
            let session = ingress_recording(
                &f.processor,
                &f.workspace,
                &context,
                "voice_retired_ingress",
            );
            let (renewals, _) = watch::channel(ConnectionAccessLease::new(
                context.principal().clone(),
                Duration::from_secs(60),
            ));
            let mut handlers = ConnectionHandlers::default();
            handlers.enable_audio(
                f.processor.clone(),
                f.processor.session_manager.clone(),
                f.auth.clone(),
                f.connection,
                f.outbound.clone(),
            );
            let (entered, ingest_entered) = tokio::sync::oneshot::channel();
            let (release, ingest_release) = tokio::sync::oneshot::channel();
            handlers.pause_audio_once(entered, ingest_release);
            let mut deferred = VecDeque::new();
            let (input, mut reader) = incoming();
            let (finish, finished) = tokio::sync::oneshot::channel();
            let dispatch = dispatch(f.processor.clone());
            let pump = process_with_access_renewals(
                async {
                    let _ = finished.await;
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
                input.send(ingress_frame(&session.session_id, 0)).unwrap();
                ingest_entered.await.unwrap();
                input.send(ingress_frame(&session.session_id, 1)).unwrap();
                // Positive dispatch completion proves the pump served later input
                // while the worker was alive and parked at its active item.
                input
                    .send(Message::Text(
                        rpc(
                            "retirement-probe",
                            methods::TASK_WAIT,
                            json!({"runIds":[RUN],"timeoutMs":1}),
                        )
                        .into(),
                    ))
                    .unwrap();
                let response = recv_response_by_id(&mut f.responses, &id("retirement-probe")).await;
                assert_eq!(response.result["timedOut"], true);
                if terminal == "panic" {
                    drop(release); // the test-only gate injects a worker panic
                    None
                } else {
                    finish.send(()).unwrap();
                    Some(release)
                }
            };
            let (alive, kept_release) = tokio::join!(pump, driver);
            assert_eq!(alive, terminal != "panic");
            if terminal == "drop" {
                drop(handlers);
            } else {
                handlers.shutdown().await;
            }
            session.ingress.drain().await;
            drop(kept_release);
            assert_eq!(
                f.processor
                    .voice_session_buffers
                    .buffered_bytes_for_test(&session.session_id)
                    .unwrap(),
                0
            );
            f.processor.connection_closed(f.connection).await;
        })
        .await
        .expect("audio retirement must drop accepted payload ownership");
    }
}

#[tokio::test]
async fn finalize_transport_seal_includes_active_and_queued_audio_and_rejects_late_tail() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut f = Fixture::new().await;
        let mut context = f.processor.session_manager.connection_context(f.connection).await.unwrap();
        let session = ingress_recording(&f.processor, &f.workspace, &context, "voice_finalize_ingress");
        f.processor.voice_session_buffers.remove_session(&session.session_id).unwrap();
        let (_recognizer, completion) = tokio::sync::oneshot::channel();
        f.processor.voice_session_buffers.start_processing(session.session_id.clone(), session.audio_format.clone(),
            tokio_util::sync::CancellationToken::new(), completion).unwrap();
        let (renewals, _) = watch::channel(ConnectionAccessLease::new(context.principal().clone(), Duration::from_secs(60)));
        let mut handlers = ConnectionHandlers::default();
        handlers.enable_audio(f.processor.clone(), f.processor.session_manager.clone(), f.auth.clone(), f.connection, f.outbound.clone());
        let (entered, ingest_entered) = tokio::sync::oneshot::channel();
        let (release, ingest_release) = tokio::sync::oneshot::channel();
        handlers.pause_audio_once(entered, ingest_release);
        let (gate, mut entries, _) = read_gate(&["held-finalize-read"]);
        let dispatch = gated_read_dispatch(f.processor.clone(), gate.clone());
        let mut deferred = VecDeque::from([get_thread("held-finalize-read")]);
        let (input, mut reader) = incoming();
        let pump = process_with_access_renewals(std::future::pending(), IngressMode::ReadGroup,
            &mut reader, &mut deferred, &f.auth, &f.processor.session_manager,
            &mut context, &renewals, &f.outbound, &mut handlers, &dispatch);
        let driver = async {
            assert_eq!(entries.recv().await.unwrap(), id("held-finalize-read"));
            input.send(ingress_frame(&session.session_id, 0)).unwrap();
            ingest_entered.await.unwrap();
            input.send(ingress_frame(&session.session_id, 1)).unwrap();
            input.send(ingress_frame(&session.session_id, 2)).unwrap();
            // Seal is a receive boundary even while the domain request is deferred.
            input.send(Message::Text(rpc("fenced-finalize", methods::VOICE_SESSION_FINALIZE, json!({
                "session_id":session.session_id,"audio_integrity":{"next_sequence":3,"normalized_samples":3},
                "context":{"workspace_id":f.workspace,"thread_id":THREAD,"turn_id":TURN}
            })).into())).unwrap();
            input.send(ingress_frame(&session.session_id, 3)).unwrap();
            // Exact control has independent admission beyond the deferred Finalize.
            input.send(Message::Text(rpc("seal-probe", methods::TASK_WAIT,
                json!({"runIds":[RUN],"timeoutMs":1})).into())).unwrap();
            let response = recv_response_by_id(&mut f.responses, &id("seal-probe")).await;
            assert_eq!(response.result["timedOut"], true);
            assert!(session.ingress.admit().is_none());
            assert!(session.ingress.drain().now_or_never().is_none());
            release.send(()).unwrap();
            for sequence in 0..3 {
                let Message::Text(text) = f.responses.recv().await.unwrap() else { panic!("finalize seal retired socket"); };
                let value: JsonValue = serde_json::from_str(&text).unwrap();
                assert_eq!(value["method"], pioneer_protocol::constants::events::VOICE_CHUNK_ACK);
                assert_eq!(value["params"]["sequence"], sequence);
            }
            session.ingress.drain().await;
            // The same production integrity proof used by the domain finalizer
            // accepts every pre-seal sample and excludes the late fourth frame.
            let completion = f.processor.voice_session_buffers.finish_processing_verified(&session.session_id,
                Some(pioneer_protocol::VoiceAudioIntegrity { next_sequence:3, normalized_samples:3 })).unwrap();
            drop(completion);
            let audio = f.processor.voice_session_buffers.take_session_audio(&session.session_id).unwrap();
            assert_eq!(audio.chunks.iter().map(|c| c.sequence).collect::<Vec<_>>(), vec![0,1,2]);
            assert_eq!(audio.normalized_samples.len(), 3);
            gate.release.add_permits(1);
        };
        let (alive, ()) = tokio::join!(pump, driver);
        assert!(alive);
        assert_eq!(deferred.len(), 1, "only the domain Finalize retains its ordinary barrier");
        handlers.shutdown().await;
        f.processor.connection_closed(f.connection).await;
    }).await.expect("finalize admission fence covers the active item and queue transition");
}

async fn seed_unrelated_voice_acl(processor: &MessageProcessor, workspace: &str) -> String {
    materialize_test_member_collaborator(processor.crud_store.as_ref(), workspace, THREAD).await;
    let other = processor
        .workspace_manager
        .create_workspace("W00000000000000000009", None)
        .await
        .unwrap()
        .id;
    let now = chrono::Utc::now().fixed_offset();
    pioneer_entity::workspace_membership::Entity::insert(
        pioneer_entity::workspace_membership::ActiveModel {
            principal_id: Set(TEST_COLLABORATOR_PRINCIPAL_ID.into()),
            workspace_id: Set(other.clone()),
            granted_by_actor_kind: Set("system".into()),
            granted_by_actor_id: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
            inbox_after_sequence: Set(None),
        },
    )
    .exec(&processor.crud_store.database_connection())
    .await
    .unwrap();
    other
}
async fn revoke_unrelated_voice_acl(processor: &MessageProcessor, other: &str) {
    let deletion = pioneer_entity::workspace_membership::Entity::delete_many()
        .filter(
            pioneer_entity::workspace_membership::Column::PrincipalId
                .eq(TEST_COLLABORATOR_PRINCIPAL_ID),
        )
        .filter(pioneer_entity::workspace_membership::Column::WorkspaceId.eq(other))
        .exec(&processor.crud_store.database_connection())
        .await
        .unwrap();
    assert_eq!(deletion.rows_affected, 1);
    processor
        .publish_committed_workspace_membership_invalidation(
            authenticated_test_member_collaborator()
                .principal_id
                .clone(),
            other,
        )
        .await;
}

#[tokio::test]
async fn unrelated_committed_policy_change_reauthorizes_same_audio_without_loss() {
    let mut f = Fixture::new().await;
    let context = f
        .processor
        .session_manager
        .connection_context(f.connection)
        .await
        .unwrap();
    let session = ingress_recording(&f.processor, &f.workspace, &context, "voice_policy_recheck");
    let other = seed_unrelated_voice_acl(&f.processor, &f.workspace).await;
    let before = f.processor.current_authorization_revision().await.unwrap();
    let (entered, reached) = tokio::sync::oneshot::channel();
    let (release, resumed) = tokio::sync::oneshot::channel();
    *session.ingress.proof_pause.lock().unwrap() = Some((entered, resumed));
    let Message::Binary(frame) = ingress_frame(&session.session_id, 0) else {
        unreachable!()
    };
    let processing = f.processor.process_binary_frame(&context, &frame);
    let mutation = async {
        reached.await.unwrap();
        revoke_unrelated_voice_acl(&f.processor, &other).await;
        assert!(f.processor.current_authorization_revision().await.unwrap() > before);
        release.send(()).unwrap();
    };
    tokio::join!(processing, mutation);
    let Message::Text(ack) = f.responses.recv().await.unwrap() else {
        panic!("unexpected close");
    };
    let ack: JsonValue = serde_json::from_str(&ack).unwrap();
    assert_eq!(
        ack["method"],
        pioneer_protocol::constants::events::VOICE_CHUNK_ACK
    );
    assert_eq!(ack["params"]["sequence"], 0);
    let audio = f
        .processor
        .voice_session_buffers
        .take_session_audio(&session.session_id)
        .unwrap();
    assert_eq!(audio.chunks.len(), 1);
    assert_eq!(audio.chunks[0].audio_payload, vec![0, 0]);
    assert_eq!(audio.normalized_samples.len(), 1);
    f.processor.connection_closed(f.connection).await;
}

#[tokio::test]
async fn domain_finalize_drains_slow_tail_with_recording_and_buffered_recognizer() {
    for buffered in [false, true] {
        tokio::time::timeout(Duration::from_secs(20), async {
            let mut f = Fixture::new().await;
            let home = tempfile::tempdir().unwrap();
            let mut processor = (*f.processor).clone().with_runtime_home_for_tests(home.path().to_path_buf())
                .with_test_voice_transcript("complete transport transcript")
                .with_voice_input_supervisor(ready_gateway_voice_supervisor_for_model("complete transport transcript", "moonshine-base"));
            processor.provider_registry = test_provider();
            let native_gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
            let native_release = PendingVoiceRelease(native_gate.clone());
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let worker_calls = calls.clone();
            let native_entered = Arc::new(Notify::new());
            let native_started = native_entered.clone();
            let gate = native_gate.clone();
            processor.voice_test_transcriber = Some(Arc::new(move |_| {
                if worker_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 { native_started.notify_one(); }
                let (lock, wake) = &*gate;
                let mut ready = lock.lock().unwrap();
                while !*ready { ready = wake.wait(ready).unwrap(); }
                Ok("complete transport transcript".into())
            }));
            f.processor = Arc::new(processor);
            let settings_context = registered_request_context(&f.processor, f.connection, "settings/update").await;
            f.processor.update_gateway_settings(&settings_context, pioneer_protocol::GatewaySettingsUpdate {
                voice_input: Some(pioneer_protocol::GatewayVoiceInputSettingsUpdate {
                    enabled: Some(true), provider: Some(Some(pioneer_protocol::GatewayVoiceInputProvider::Local)),
                    model: Some(Some("moonshine-base".into())),
                    transcribe_while_recording: Some(!buffered), ..Default::default()
                }), ..Default::default()
            }).await.unwrap();
            assert!(f.processor.thread_manager.subscribe_connection(THREAD, f.connection).await);
            let started = start_test_voice_session(&f.processor, f.connection, &mut f.responses,
                &f.workspace, THREAD, "turn_transport_finalize_full", &id("full-start")).await;
            let mut context = f.processor.session_manager.connection_context(f.connection).await.unwrap();
            let recording = f.processor.voice_sessions.lookup_session(&started.session_id, f.connection).unwrap();
            let other = seed_unrelated_voice_acl(&f.processor, &f.workspace).await;
            let policy_before = f.processor.current_authorization_revision().await.unwrap();
            let (renewals, _) = watch::channel(ConnectionAccessLease::new(context.principal().clone(), Duration::from_secs(60)));
            let mut handlers = ConnectionHandlers::default();
            handlers.enable_audio(f.processor.clone(), f.processor.session_manager.clone(), f.auth.clone(), f.connection, f.outbound.clone());
            let mut deferred = VecDeque::new();
            let (input, mut reader) = incoming();
            let (finalize, finalization) = tokio::sync::oneshot::channel::<String>();
            let dispatch = dispatch(f.processor.clone());
            let processor = f.processor.clone();
            let finalize_context = context.clone();
            let processing = async move {
                let payload = finalization.await.unwrap();
                let mut request = processor.start_owned_request(finalize_context, payload);
                request.join().await.unwrap();
            };
            let pump = process_with_access_renewals(processing, IngressMode::Serial,
                &mut reader, &mut deferred, &f.auth, &f.processor.session_manager,
                &mut context, &renewals, &f.outbound, &mut handlers, &dispatch);
            let driver = async {
                for sequence in 0..30 {
                    input.send(Message::Binary(voice_test_frame(&started.session_id, sequence, 1600, 12000).into())).unwrap();
                    let ack = recv_notification_by_method(&mut f.responses, pioneer_protocol::constants::events::VOICE_CHUNK_ACK).await;
                    assert_eq!(ack.params.unwrap()["sequence"], sequence);
                }
                if buffered { assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0); }
                let (entered, at_proof) = tokio::sync::oneshot::channel();
                let (release, resumed) = tokio::sync::oneshot::channel();
                *recording.ingress.proof_pause.lock().unwrap() = Some((entered, resumed));
                input.send(Message::Binary(voice_test_frame(&started.session_id, 30, 1600, 12000).into())).unwrap();
                at_proof.await.unwrap();
                let (tail_entered, tail_proof) = tokio::sync::oneshot::channel();
                let (tail_release, tail_resume) = tokio::sync::oneshot::channel();
                *recording.ingress.proof_pause.lock().unwrap() = Some((tail_entered, tail_resume));
                input.send(Message::Binary(voice_test_frame(&started.session_id, 31, 7, 3000).into())).unwrap();
                revoke_unrelated_voice_acl(&f.processor, &other).await;
                assert!(f.processor.current_authorization_revision().await.unwrap() > policy_before);
                let payload = rpc("full-finalize", methods::VOICE_SESSION_FINALIZE, json!({
                    "session_id":started.session_id,"audio_integrity":{"next_sequence":32,"normalized_samples":49607},
                    "context":{"workspace_id":f.workspace,"thread_id":THREAD,"turn_id":"turn_transport_finalize_full"}
                }));
                // Receive-time seal through the production busy pump, followed
                // by actual owned domain dispatch of that same finalized input.
                input.send(Message::Text(payload.clone().into())).unwrap();
                input.send(Message::Binary(voice_test_frame(&started.session_id,32,7,16000).into())).unwrap();
                input.send(Message::Text(rpc("full-seal-probe", methods::TASK_WAIT,
                    json!({"runIds":[RUN],"timeoutMs":1})).into())).unwrap();
                let probe = recv_response_by_id(&mut f.responses,&id("full-seal-probe")).await;
                assert_eq!(probe.result["timedOut"],true);
                assert!(recording.ingress.admit().is_none());
                finalize.send(payload).unwrap();
                release.send(()).unwrap();
                tail_proof.await.unwrap();
                if !buffered { native_entered.notified().await; }
                tail_release.send(()).unwrap();
                let mut reply = None;
                let mut received = 30;
                while received < 32 {
                    let Message::Text(text) = f.responses.recv().await.unwrap() else { panic!("finalize disconnected"); };
                    let value: JsonValue = serde_json::from_str(&text).unwrap();
                    if value["method"] == pioneer_protocol::constants::events::VOICE_CHUNK_ACK {
                        assert_eq!(value["params"]["sequence"], received);
                        received += 1;
                    } else if value["id"] == id("full-finalize") { reply = Some(value); }
                }
                if buffered { native_entered.notified().await; }
                native_release.release();
                let mut terminal = None;
                while reply.is_none() || terminal.is_none() {
                    let Message::Text(text) = f.responses.recv().await.unwrap() else { panic!("finalize disconnected"); };
                    let value: JsonValue = serde_json::from_str(&text).unwrap();
                    if value["id"] == id("full-finalize") { reply = Some(value); }
                    else if value["method"] == pioneer_protocol::constants::events::VOICE_SESSION_RESULT {
                        terminal = Some(value["params"].clone());
                    }
                }
                let response: pioneer_protocol::VoiceSessionFinalizeResponse = serde_json::from_value(reply.unwrap()["result"].clone()).unwrap();
                assert_eq!(response.status,pioneer_protocol::VoiceStatus::Transcribing);
                let result: pioneer_protocol::VoiceSessionResultNotification = serde_json::from_value(terminal.unwrap()).unwrap();
                assert_eq!(result.session_id,started.session_id);
                assert_eq!(result.outcome,pioneer_protocol::VoiceSessionOutcome::TurnStarted);
                assert_eq!(result.turn_id.as_deref(),Some("turn_transport_finalize_full"));
                assert!(f.processor.crud_store.get_turn(THREAD,"turn_transport_finalize_full").await.unwrap().is_some());
                assert!(result.error.is_none());
                assert!(calls.load(std::sync::atomic::Ordering::SeqCst) > 0);
            };
            let (alive,()) = tokio::join!(pump,driver);
            assert!(alive);
            handlers.shutdown().await;
            f.processor.connection_closed(f.connection).await;
        }).await.expect("complete native/fake pipeline must include the admitted slow tail");
    }
}

#[tokio::test]
async fn real_ack_capacity_wait_preserves_rpc_and_controls_with_full_ordinary_data() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut f = Fixture::new().await;
        let mut context = f
            .processor
            .session_manager
            .connection_context(f.connection)
            .await
            .unwrap();
        let session = ingress_recording(&f.processor, &f.workspace, &context, "voice_ack_pressure");
        // The physical audio delivery owner remains undrained throughout the
        // positive RPC/control checks. Ordinary Data is independently full.
        for sequence in 0..128 {
            let Message::Binary(frame) = ingress_frame(&session.session_id, sequence) else {
                unreachable!()
            };
            f.processor.process_binary_frame(&context, &frame).await;
        }
        let mut ordinary_count = 0;
        loop {
            match f
                .outbound
                .try_send_data(Message::Text("ordinary-budget".into()))
            {
                Ok(()) => ordinary_count += 1,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
                Err(e) => panic!("healthy owner closed: {e}"),
            }
        }
        assert!(ordinary_count > 0);
        let (entered, reached) = tokio::sync::oneshot::channel();
        let (release, resumed) = tokio::sync::oneshot::channel();
        *f.processor
            .session_manager
            .replay_delivery_pause
            .lock()
            .unwrap() = Some(crate::session::test_support::DeliveryPause {
            connection: f.connection,
            method: pioneer_protocol::constants::events::VOICE_CHUNK_ACK,
            entered,
            release: resumed,
        });
        let (renewals, _) = watch::channel(ConnectionAccessLease::new(
            context.principal().clone(),
            Duration::from_secs(60),
        ));
        let mut handlers = ConnectionHandlers::default();
        handlers.enable_audio(
            f.processor.clone(),
            f.processor.session_manager.clone(),
            f.auth.clone(),
            f.connection,
            f.outbound.clone(),
        );
        let (read_gate, mut entries, _) = read_gate(&["ack-held-read"]);
        let dispatch = gated_read_dispatch(f.processor.clone(), read_gate.clone());
        let mut deferred = VecDeque::from([get_thread("ack-held-read")]);
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
            input.send(ingress_frame(&session.session_id, 128)).unwrap();
            reached.await.unwrap(); // production send_voice_text observed actual Full
            release.send(()).unwrap();
            input.send(get_thread("ack-rpc")).unwrap();
            input
                .send(Message::Text(
                    rpc(
                        "ack-control",
                        methods::TASK_WAIT,
                        json!({"runIds":[RUN],"timeoutMs":1}),
                    )
                    .into(),
                ))
                .unwrap();
            let mut rpc_done = false;
            let mut control_done = false;
            while !rpc_done || !control_done {
                let Message::Text(text) = f.responses.recv_ordinary().await.unwrap() else {
                    panic!("audio pressure closed socket");
                };
                if text == "ordinary-budget" {
                    continue;
                }
                let value: JsonValue = serde_json::from_str(&text).unwrap();
                if value["id"] == id("ack-rpc") {
                    assert!(value["result"]["thread"].is_object());
                    rpc_done = true;
                }
                if value["id"] == id("ack-control") {
                    assert_eq!(value["result"]["timedOut"], true);
                    control_done = true;
                }
            }
            assert!(
                f.processor
                    .session_manager
                    .connection_context(f.connection)
                    .await
                    .is_ok()
            );
            assert!(session.ingress.drain().now_or_never().is_none());
            // Only now permit audio delivery. Every ACK must remain individual.
            for sequence in 0..129 {
                let ack = recv_notification_by_method(
                    &mut f.responses,
                    pioneer_protocol::constants::events::VOICE_CHUNK_ACK,
                )
                .await;
                assert_eq!(ack.params.unwrap()["sequence"], sequence);
            }
            session.ingress.drain().await;
            let audio = f
                .processor
                .voice_session_buffers
                .take_session_audio(&session.session_id)
                .unwrap();
            assert_eq!(audio.chunks.len(), 129);
            read_gate.release.add_permits(1);
        };
        let (alive, ()) = tokio::join!(pump, driver);
        assert!(alive);
        handlers.shutdown().await;
        f.processor.connection_closed(f.connection).await;
    })
    .await
    .expect("audio ACK pressure must preserve positively completed ordinary work");
}

#[tokio::test]
async fn durable_revoke_before_publication_is_rechecked_before_audio_effect() {
    for publish in [false, true] {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut f = Fixture::new().await;
            let unrelated = f.connection;
            materialize_test_member_collaborator(
                f.processor.crud_store.as_ref(),
                &f.workspace,
                THREAD,
            )
            .await;
            // The real audio worker validates the durable device/session lease.
            // ACL membership alone is insufficient for this physical owner.
            let db = f.processor.crud_store.database_connection();
            let member = authenticated_test_member_collaborator();
            let device = pioneer_entity::device::Entity::find_by_id("D00000000000000000001")
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            let session = pioneer_entity::auth_session::Entity::find_by_id("S00000000000000000001")
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            use sea_orm::IntoActiveModel;
            let mut device = device.into_active_model().reset_all();
            device.id = Set(member.device_id.to_string());
            device.principal_id = Set(member.principal_id.to_string());
            device.insert(&db).await.unwrap();
            let mut session = session.into_active_model().reset_all();
            session.id = Set(member.session_id.to_string());
            session.principal_id = Set(member.principal_id.to_string());
            session.device_id = Set(member.device_id.to_string());
            session.token_family_id = Set("F0000000000000000000C".into());
            session.activation_token_hash = Set(vec![0xCC; 32]);
            session.activation_locator_hash = Set(vec![0xCD; 32]);
            session.insert(&db).await.unwrap();
            f.auth
                .validate_session_lease(member.as_ref())
                .await
                .unwrap();
            let (tx, rx) = crate::session::test_support::outbound_channel(128);
            f.connection = f
                .processor
                .session_manager
                .register_connection(tx.clone(), authenticated_test_member_collaborator())
                .await
                .unwrap();
            f.outbound = tx;
            f.responses = rx;
            f.processor
                .session_manager
                .set_connection_workspace(f.connection, Some(f.workspace.clone()))
                .await;
            let mut context = f
                .processor
                .session_manager
                .connection_context(f.connection)
                .await
                .unwrap();
            let principal = context.principal().clone();
            let session = ingress_recording(
                &f.processor,
                &f.workspace,
                &context,
                "voice_revoke_before_observe",
            );
            let before = f.processor.current_authorization_revision().await.unwrap();
            let (entered, reached) = tokio::sync::oneshot::channel();
            let (release, resumed) = tokio::sync::oneshot::channel();
            *session.ingress.proof_pause.lock().unwrap() = Some((entered, resumed));
            let (renewals, _) = watch::channel(ConnectionAccessLease::new(
                principal.clone(),
                Duration::from_secs(60),
            ));
            let mut handlers = ConnectionHandlers::default();
            handlers.enable_audio(
                f.processor.clone(),
                f.processor.session_manager.clone(),
                f.auth.clone(),
                f.connection,
                f.outbound.clone(),
            );
            let (input, mut reader) = incoming();
            let (finish, finished) = tokio::sync::oneshot::channel();
            let mut deferred = VecDeque::new();
            let dispatch = dispatch(f.processor.clone());
            let pump = process_with_access_renewals(
                async {
                    finished.await.unwrap();
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
                input.send(ingress_frame(&session.session_id, 0)).unwrap();
                reached.await.unwrap();
                input.send(ingress_frame(&session.session_id, 1)).unwrap();
                input
                    .send(Message::Text(
                        rpc(
                            "revoke-queue-probe",
                            methods::TASK_WAIT,
                            json!({"runIds":[RUN],"timeoutMs":1}),
                        )
                        .into(),
                    ))
                    .unwrap();
                loop {
                    let Message::Text(text) = f.responses.recv().await.unwrap() else {
                        panic!("revoke probe lost")
                    };
                    let value: JsonValue = serde_json::from_str(&text).unwrap();
                    if value["id"] == id("revoke-queue-probe") {
                        assert!(value.get("result").is_some() || value.get("error").is_some());
                        break;
                    }
                }
                let deletion = pioneer_entity::workspace_membership::Entity::delete_many()
                    .filter(
                        pioneer_entity::workspace_membership::Column::PrincipalId
                            .eq(principal.principal_id.as_str()),
                    )
                    .filter(
                        pioneer_entity::workspace_membership::Column::WorkspaceId.eq(&f.workspace),
                    )
                    .exec(&f.processor.crud_store.database_connection())
                    .await
                    .unwrap();
                assert_eq!(deletion.rows_affected, 1);
                assert_eq!(
                    f.processor.current_authorization_revision().await.unwrap(),
                    before
                );
                if publish {
                    f.processor
                        .publish_committed_workspace_membership_invalidation(
                            principal.principal_id.clone(),
                            f.workspace.clone(),
                        )
                        .await;
                    assert!(session.ingress.cancelled.is_cancelled());
                }
                let _ = release.send(());
                if !publish {
                    let result = recv_notification_by_method(
                        &mut f.responses,
                        pioneer_protocol::constants::events::VOICE_SESSION_RESULT,
                    )
                    .await;
                    let result: pioneer_protocol::VoiceSessionResultNotification =
                        serde_json::from_value(result.params.unwrap()).unwrap();
                    assert_eq!(result.session_id, session.session_id);
                    assert_eq!(
                        result.outcome,
                        pioneer_protocol::VoiceSessionOutcome::Failed
                    );
                }
                session.ingress.drain().await;
                assert!(
                    f.processor
                        .voice_session_buffers
                        .buffered_bytes_for_test(&session.session_id)
                        .is_err()
                );
                assert!(
                    f.processor
                        .voice_sessions
                        .lookup_session(&session.session_id, f.connection)
                        .is_err()
                );
                assert!(session.ingress.cancelled.is_cancelled());
                assert!(
                    f.processor
                        .session_manager
                        .connection_context(unrelated)
                        .await
                        .is_ok()
                );
                finish.send(()).unwrap();
            };
            let (alive, ()) = tokio::join!(pump, driver);
            assert!(alive);
            handlers.shutdown().await;
            f.processor.connection_closed(f.connection).await;
        })
        .await
        .expect("durable denial and published retirement must fence active and queued audio");
    }
}

#[tokio::test]
async fn audio_delivery_capacity_wait_revalidates_renewal_expiry_and_physical_owner() {
    for action in ["renewal", "expiry", "disconnect", "replacement"] {
        let mut f = Fixture::new().await;
        let context = f
            .processor
            .session_manager
            .connection_context(f.connection)
            .await
            .unwrap();
        for _ in 0..128 {
            f.outbound
                .try_send_audio(Message::Text("held-audio".into()), false)
                .unwrap();
        }
        let waiting =
            f.processor
                .session_manager
                .send_voice_text(f.connection, "late-audio".into(), false);
        tokio::pin!(waiting);
        assert!(waiting.as_mut().now_or_never().is_none());
        f.processor
            .process_request_for_connection(
                f.connection,
                &rpc(
                    "audio-wait-probe",
                    methods::THREAD_GET,
                    json!({"thread_id":THREAD}),
                ),
            )
            .await;
        let Message::Text(reply) = f.responses.recv_ordinary().await.unwrap() else {
            panic!("RPC lost")
        };
        let reply: JsonValue = serde_json::from_str(&reply).unwrap();
        assert_eq!(reply["id"], id("audio-wait-probe"));
        assert!(reply["result"]["thread"].is_object());
        if matches!(action, "renewal" | "expiry") {
            let mut next = context.principal().clone();
            next.access_jti = "renewed-voice-wait".into();
            next.access_expires_at_unix = if action == "expiry" {
                0
            } else {
                crate::helpers::unix_timestamp_secs().unwrap() + 3600
            };
            f.processor
                .session_manager
                .renew_connection_principal(f.connection, context.principal(), Arc::new(next))
                .await
                .unwrap();
        } else {
            f.processor
                .session_manager
                .unregister_connection(f.connection)
                .await;
        }
        if action == "renewal" {
            assert!(waiting.as_mut().now_or_never().is_none());
            assert!(
                matches!(f.responses.recv().await,Some(Message::Text(text)) if text=="held-audio")
            );
            waiting.await.unwrap();
            for _ in 1..128 {
                f.responses.recv().await.unwrap();
            }
            assert!(
                matches!(f.responses.recv().await,Some(Message::Text(text)) if text=="late-audio")
            );
        } else {
            assert!(waiting.await.is_err());
            if action == "replacement" {
                let (tx, mut rx) = crate::session::test_support::outbound_channel(8);
                let current = f
                    .processor
                    .session_manager
                    .register_connection(tx, authenticated_test_superuser())
                    .await
                    .unwrap();
                assert_ne!(current, f.connection);
                f.processor
                    .session_manager
                    .send_voice_text(current, "current-owner".into(), false)
                    .await
                    .unwrap();
                assert!(
                    matches!(rx.recv().await,Some(Message::Text(text)) if text=="current-owner")
                );
                assert!(rx.try_recv().is_err());
                f.processor
                    .session_manager
                    .unregister_connection(current)
                    .await;
            }
        }
        f.processor.connection_closed(f.connection).await;
    }
}

#[tokio::test]
async fn unpolled_audio_owner_drops_queued_payload_and_completion_ticket() {
    let f = Fixture::new().await;
    let mut context = f
        .processor
        .session_manager
        .connection_context(f.connection)
        .await
        .unwrap();
    let session = ingress_recording(&f.processor, &f.workspace, &context, "voice_unpolled_owner");
    let (renewals, _) = watch::channel(ConnectionAccessLease::new(
        context.principal().clone(),
        Duration::from_secs(60),
    ));
    let mut handlers = ConnectionHandlers::default();
    handlers.enable_audio(
        f.processor.clone(),
        f.processor.session_manager.clone(),
        f.auth.clone(),
        f.connection,
        f.outbound.clone(),
    );
    let mut reader = futures_util::stream::iter([Ok(ingress_frame(&session.session_id, 0))]);
    let mut deferred = VecDeque::new();
    let dispatch = dispatch(f.processor.clone());
    // Current-thread runtime: the ready stream and EOF complete in this poll;
    // the spawned worker has had no scheduling yield before shutdown below.
    assert!(
        !process_with_access_renewals(
            std::future::pending(),
            IngressMode::Serial,
            &mut reader,
            &mut deferred,
            &f.auth,
            &f.processor.session_manager,
            &mut context,
            &renewals,
            &f.outbound,
            &mut handlers,
            &dispatch
        )
        .await
    );
    assert!(session.ingress.drain().now_or_never().is_none());
    handlers.shutdown().await;
    session.ingress.drain().await;
    assert_eq!(
        f.processor
            .voice_session_buffers
            .buffered_bytes_for_test(&session.session_id)
            .unwrap(),
        0
    );
    f.processor.connection_closed(f.connection).await;
}

#[tokio::test]
async fn idle_socket_audio_demultiplex_keeps_normal_rpc_and_exact_pcm() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message as WireMessage;
    tokio::time::timeout(Duration::from_secs(15), async {
        let f = Fixture::new().await;
        let (mut wire, _block, _entered) = OutboundSocket::new(&f, false).await;
        let context = f
            .processor
            .session_manager
            .connection_context(wire.connection)
            .await
            .unwrap();
        let session = ingress_recording(&f.processor, &f.workspace, &context, "voice_idle_socket");
        wire.start_writer.send(()).unwrap();
        let Message::Binary(frame) = ingress_frame(&session.session_id, 0) else {
            unreachable!()
        };
        wire.socket.send(WireMessage::Binary(frame)).await.unwrap();
        loop {
            match wire.socket.next().await.unwrap().unwrap() {
                WireMessage::Text(text) => {
                    let value: JsonValue = serde_json::from_str(&text).unwrap();
                    if value["method"] == pioneer_protocol::constants::events::VOICE_CHUNK_ACK {
                        assert_eq!(value["params"]["session_id"], session.session_id);
                        assert_eq!(value["params"]["sequence"], 0);
                        break;
                    }
                }
                WireMessage::Close(_) => panic!("audio closed idle socket"),
                _ => {}
            }
        }
        wire.socket
            .send(WireMessage::Text(
                rpc(
                    "idle-audio-rpc",
                    methods::THREAD_GET,
                    json!({"thread_id":THREAD}),
                )
                .into(),
            ))
            .await
            .unwrap();
        loop {
            match wire.socket.next().await.unwrap().unwrap() {
                WireMessage::Text(text) => {
                    let value: JsonValue = serde_json::from_str(&text).unwrap();
                    if value["id"] == id("idle-audio-rpc") {
                        assert!(value["result"]["thread"].is_object());
                        break;
                    }
                }
                WireMessage::Close(_) => panic!("ordinary RPC lost after idle audio"),
                _ => {}
            }
        }
        let audio = f
            .processor
            .voice_session_buffers
            .take_session_audio(&session.session_id)
            .unwrap();
        assert_eq!(audio.chunks.len(), 1);
        assert_eq!(audio.chunks[0].audio_payload, vec![0, 0]);
        for (label, method, params) in [
            (
                "idle-turn-cancel",
                methods::TURN_CANCEL,
                json!({"thread_id":THREAD,"turn_id":TURN}),
            ),
            (
                "idle-voice-cancel",
                methods::VOICE_SESSION_CANCEL,
                json!({"session_id":session.session_id}),
            ),
        ] {
            wire.socket
                .send(WireMessage::Text(rpc(label, method, params).into()))
                .await
                .unwrap();
        }
        let mut completed = Vec::new();
        while completed.len() < 2 {
            match wire.socket.next().await.unwrap().unwrap() {
                WireMessage::Text(text) => {
                    let value: JsonValue = serde_json::from_str(&text).unwrap();
                    for label in ["idle-turn-cancel", "idle-voice-cancel"] {
                        if value["id"] == id(label) {
                            assert!(!completed.contains(&label));
                            assert!(value.get("result").is_some() || value.get("error").is_some());
                            if label == "idle-voice-cancel" {
                                assert_eq!(value["result"]["cancelled"], true);
                            }
                            completed.push(label);
                        }
                    }
                }
                WireMessage::Close(_) => panic!("independent Cancel closed idle socket"),
                _ => {}
            }
        }
        wire.stop.send(true).unwrap();
        while let Some(frame) = wire.socket.next().await {
            if matches!(frame.unwrap(), WireMessage::Close(_)) {
                break;
            }
        }
        wire.completion.await.unwrap().unwrap();
        wire.server.abort_and_join().await;
    })
    .await
    .expect("idle normal owner must positively service audio then RPC");
}

#[tokio::test]
async fn queued_and_active_audio_revalidate_current_lease_and_exact_socket() {
    for action in ["renewal", "expiry", "disconnect", "replacement"] {
        tokio::time::timeout(Duration::from_secs(10),async {
            let mut f=Fixture::new().await;
            let mut context=f.processor.session_manager.connection_context(f.connection).await.unwrap();
            let expected=context.principal().clone();
            let session=ingress_recording(&f.processor, &f.workspace,&context,"voice_queued_lease");
            let (entered,reached)=tokio::sync::oneshot::channel();
            let (release,resumed)=tokio::sync::oneshot::channel();
            *session.ingress.proof_pause.lock().unwrap()=Some((entered,resumed));
            let (renewals,_)=watch::channel(ConnectionAccessLease::new(expected.clone(),Duration::from_secs(60)));
            let mut handlers=ConnectionHandlers::default();
            handlers.enable_audio(f.processor.clone(),f.processor.session_manager.clone(),f.auth.clone(),f.connection,f.outbound.clone());
            let (input,mut reader)=incoming();
            let (finish,finished)=tokio::sync::oneshot::channel();
            let mut deferred=VecDeque::new();
            let dispatch=dispatch(f.processor.clone());
            let pump=process_with_access_renewals(async {finished.await.unwrap();},IngressMode::Serial,
                &mut reader,&mut deferred,&f.auth,&f.processor.session_manager,&mut context,&renewals,&f.outbound,&mut handlers,&dispatch);
            let driver=async {
                input.send(ingress_frame(&session.session_id,0)).unwrap();
                reached.await.unwrap();
                input.send(ingress_frame(&session.session_id,1)).unwrap();
                input.send(Message::Text(rpc("queued-lease-probe",methods::TASK_WAIT,json!({"runIds":[RUN],"timeoutMs":1})).into())).unwrap();
                let probe=recv_response_by_id(&mut f.responses,&id("queued-lease-probe")).await;
                assert_eq!(probe.result["timedOut"],true);
                let mut replacement=None;
                if matches!(action,"renewal"|"expiry") {
                    let mut next=expected.clone();
                    next.access_jti="queued-renewal".into();
                    next.access_expires_at_unix=if action=="expiry" {0} else {expected.access_expires_at_unix+3600};
                    f.processor.session_manager.renew_connection_principal(f.connection,&expected,Arc::new(next)).await.unwrap();
                } else {
                    f.processor.session_manager.unregister_connection(f.connection).await;
                    if action=="replacement" {
                        let (sender,receiver)=crate::session::test_support::outbound_channel(8);
                        let current=f.processor.session_manager.register_connection(sender,authenticated_test_superuser()).await.unwrap();
                        assert_ne!(current,f.connection);
                        replacement=Some((current,receiver));
                    }
                }
                release.send(()).unwrap();
                if action=="renewal" {
                    for sequence in 0..2 {
                        let ack=recv_notification_by_method(&mut f.responses,pioneer_protocol::constants::events::VOICE_CHUNK_ACK).await;
                        assert_eq!(ack.params.unwrap()["sequence"],sequence);
                    }
                }
                session.ingress.drain().await;
                let audio=f.processor.voice_session_buffers.take_session_audio(&session.session_id).unwrap();
                assert_eq!(audio.chunks.len(),if action=="renewal" {2} else {0});
                if let Some((current,mut receiver))=replacement {
                    assert!(receiver.try_recv().is_err(),"old work cannot migrate to a new socket");
                    f.processor.session_manager.send_voice_text(current,"replacement-live".into(),false).await.unwrap();
                    assert!(matches!(receiver.recv().await,Some(Message::Text(text)) if text=="replacement-live"));
                    f.processor.session_manager.unregister_connection(current).await;
                }
                finish.send(()).unwrap();
            };
            let (alive,())=tokio::join!(pump,driver);
            assert!(alive);
            handlers.shutdown().await;
            f.processor.connection_closed(f.connection).await;
        }).await.expect("active and queued audio must observe current physical authority");
    }
}

#[tokio::test]
async fn fail_cancel_restart_while_terminal_delivery_is_full_preserves_new_recording() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut f = Fixture::new().await;
        let mut context = f
            .processor
            .session_manager
            .connection_context(f.connection)
            .await
            .unwrap();
        let old = ingress_recording(
            &f.processor,
            &f.workspace,
            &context,
            "voice_blocked_terminal_old",
        );
        assert!(
            f.processor
                .thread_manager
                .subscribe_connection(THREAD, f.connection)
                .await
        );
        for _ in 0..128 {
            f.outbound
                .try_send_audio(Message::Text("held-ack".into()), false)
                .unwrap();
        }
        for _ in 0..2 {
            f.outbound
                .try_send_audio(Message::Text("held-terminal".into()), true)
                .unwrap();
        }
        let (terminal_entered, terminal_full) = tokio::sync::oneshot::channel();
        let (terminal_release, terminal_resume) = tokio::sync::oneshot::channel();
        *f.processor
            .session_manager
            .replay_delivery_pause
            .lock()
            .unwrap() = Some(crate::session::test_support::DeliveryPause {
            connection: f.connection,
            method: pioneer_protocol::constants::events::VOICE_SESSION_RESULT,
            entered: terminal_entered,
            release: terminal_resume,
        });
        let (renewals, _) = watch::channel(ConnectionAccessLease::new(
            context.principal().clone(),
            Duration::from_secs(60),
        ));
        let mut handlers = ConnectionHandlers::default();
        handlers.enable_audio(
            f.processor.clone(),
            f.processor.session_manager.clone(),
            f.auth.clone(),
            f.connection,
            f.outbound.clone(),
        );
        let (entered, reached) = tokio::sync::oneshot::channel();
        let (_release, resume) = tokio::sync::oneshot::channel();
        handlers.pause_audio_once(entered, resume);
        let (gate, mut entries, _) = read_gate(&["terminal-held-read"]);
        let dispatch = gated_read_dispatch(f.processor.clone(), gate.clone());
        let mut deferred = VecDeque::from([get_thread("terminal-held-read")]);
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
            input.send(ingress_frame(&old.session_id, 0)).unwrap();
            reached.await.unwrap();
            for sequence in 1..129 {
                input
                    .send(ingress_frame(&old.session_id, sequence))
                    .unwrap();
            }
            terminal_full.await.unwrap();
            assert!(old.ingress.cancelled.is_cancelled());
            old.ingress.drain().await;
            input
                .send(Message::Text(
                    rpc(
                        "terminal-cancel",
                        methods::VOICE_SESSION_CANCEL,
                        json!({"session_id":old.session_id}),
                    )
                    .into(),
                ))
                .unwrap();
            loop {
                let Message::Text(text) = f.responses.recv_ordinary().await.unwrap() else {
                    panic!("shared owner lost")
                };
                let value: JsonValue = serde_json::from_str(&text).unwrap();
                if value["id"] == id("terminal-cancel") {
                    assert_eq!(value["result"]["cancelled"], true);
                    break;
                }
            }
            let current = f
                .processor
                .session_manager
                .connection_context(f.connection)
                .await
                .unwrap();
            let new = ingress_recording(
                &f.processor,
                &f.workspace,
                &current,
                "voice_blocked_terminal_new",
            );
            input.send(ingress_frame(&old.session_id, 200)).unwrap();
            input.send(ingress_frame(&new.session_id, 0)).unwrap();
            terminal_release.send(()).unwrap();
            input.send(get_thread("terminal-rpc-probe")).unwrap();
            loop {
                let Message::Text(text) = f.responses.recv_ordinary().await.unwrap() else {
                    panic!("RPC lost")
                };
                let value: JsonValue = serde_json::from_str(&text).unwrap();
                if value["id"] == id("terminal-rpc-probe") {
                    assert!(value["result"]["thread"].is_object());
                    break;
                }
            }
            // Release actual audio capacity only after both canonical Cancel
            // and an unrelated RPC have completed while terminal send waited.
            loop {
                let Message::Text(text) = f.responses.recv().await.unwrap() else {
                    panic!("voice error closed socket")
                };
                if text.starts_with("held-") {
                    continue;
                }
                let value: JsonValue = serde_json::from_str(&text).unwrap();
                if value["method"] == pioneer_protocol::constants::events::VOICE_CHUNK_ACK {
                    assert_eq!(value["params"]["session_id"], new.session_id);
                    assert_eq!(value["params"]["sequence"], 0);
                    break;
                }
            }
            new.ingress.drain().await;
            let audio = f
                .processor
                .voice_session_buffers
                .take_session_audio(&new.session_id)
                .unwrap();
            assert_eq!(audio.chunks.len(), 1);
            assert_eq!(audio.chunks[0].audio_payload, vec![0, 0]);
            assert!(
                f.processor
                    .voice_session_buffers
                    .buffered_bytes_for_test(&old.session_id)
                    .is_err()
            );
            gate.release.add_permits(1);
        };
        let (alive, ()) = tokio::join!(pump, driver);
        assert!(alive);
        handlers.shutdown().await;
        f.processor.connection_closed(f.connection).await;
    })
    .await
    .expect("old terminal delivery must not own a replacement recording");
}

#[tokio::test]
async fn production_cancel_dispatch_keeps_other_class_admissible_after_reply() {
    for voice_first in [false, true] {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut f = Fixture::new().await;
            let mut context = f
                .processor
                .session_manager
                .connection_context(f.connection)
                .await
                .unwrap();
            let session = ingress_recording(
                &f.processor,
                &f.workspace,
                &context,
                "voice_cancel_class_dispatch",
            );
            assert!(
                f.processor
                    .thread_manager
                    .subscribe_connection(THREAD, f.connection)
                    .await
            );
            let first = if voice_first {
                methods::VOICE_SESSION_CANCEL
            } else {
                methods::TURN_CANCEL
            };
            let second = if voice_first {
                methods::TURN_CANCEL
            } else {
                methods::VOICE_SESSION_CANCEL
            };
            let held = Arc::new(Notify::new());
            let release = Arc::new(tokio::sync::Semaphore::new(0));
            let dispatch = {
                let processor = f.processor.clone();
                let held = held.clone();
                let release = release.clone();
                move |context: crate::request_context::ConnectionContext, payload: String| {
                    let processor = processor.clone();
                    let held = held.clone();
                    let release = release.clone();
                    message_future(async move {
                        let request: JsonRpcRequest = serde_json::from_str(&payload).unwrap();
                        processor.process_request(&context, &payload).await;
                        if request.method == first {
                            held.notify_one();
                            release.acquire().await.unwrap().forget();
                        }
                    })
                }
            };
            let mut handlers = ConnectionHandlers::default();
            let mut deferred = VecDeque::new();
            let (input, mut reader) = incoming();
            let (finish, finished) = tokio::sync::oneshot::channel();
            let (renewals, _) = watch::channel(ConnectionAccessLease::new(
                context.principal().clone(),
                Duration::from_secs(60),
            ));
            let pump = process_with_access_renewals(
                async {
                    finished.await.unwrap();
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
                for (label, method) in [("first-cancel", first), ("second-cancel", second)] {
                    let params = if method == methods::VOICE_SESSION_CANCEL {
                        json!({"session_id":session.session_id})
                    } else {
                        json!({"thread_id":THREAD,"turn_id":TURN})
                    };
                    input
                        .send(Message::Text(rpc(label, method, params).into()))
                        .unwrap();
                    loop {
                        let Message::Text(text) = f.responses.recv().await.unwrap() else {
                            panic!("one Cancel class closed shared transport")
                        };
                        let value: JsonValue = serde_json::from_str(&text).unwrap();
                        if value["id"] == id(label) {
                            assert!(value.get("result").is_some() || value.get("error").is_some());
                            if method == methods::VOICE_SESSION_CANCEL {
                                assert_eq!(value["result"]["cancelled"], true);
                            }
                            break;
                        }
                    }
                    if label == "first-cancel" {
                        held.notified().await;
                    }
                }
                assert!(
                    f.processor
                        .session_manager
                        .connection_context(f.connection)
                        .await
                        .is_ok()
                );
                release.add_permits(1);
                finish.send(()).unwrap();
            };
            let (alive, ()) = tokio::join!(pump, driver);
            assert!(alive);
            handlers.shutdown().await;
            f.processor.connection_closed(f.connection).await;
        })
        .await
        .expect("real Cancel dispatch must retain independent class admission");
    }
}
