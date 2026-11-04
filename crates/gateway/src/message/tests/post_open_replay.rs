//! The paused producer has passed resource authorization and serialization.
//! Oracles inspect real dispatcher completion, typed terminal frames and FIFO
//! contents; the seam supplies scheduling only, never the expected result.
use super::*;
use crate::session::test_support::{DeliveryPause, OutboundTestReceiver, outbound_channel};
use pioneer_protocol::{
    TurnPermissionApprovalResolution, TurnPermissionRequestResolvedNotification,
};

const THREAD: &str = "T00000000000000000761";
const TURN: &str = "R00000000000000000761";
const REQUEST: &str = "post-open-native-request";

struct Fixture {
    processor: Arc<MessageProcessor>,
    connection: ConnectionId,
    workspace: String,
    rx: OutboundTestReceiver,
    responder: tokio::sync::oneshot::Receiver<pioneer_tools::PermissionApprovalResolution>,
}
impl Fixture {
    async fn new(orphan: bool) -> Self {
        let (workspaces, store, workspace) = setup_workspace_manager().await;
        let sessions = Arc::new(SessionManager::new());
        let (outbound, mut rx) = outbound_channel(128);
        let connection = register_authenticated_test_connection(&sessions, outbound).await;
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("test-model", "openai")),
            test_provider(),
            sessions,
            workspaces,
            store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        ));
        materialize_artifact_api_thread(&store, &workspace, THREAD, TURN).await;
        let context = processor
            .session_manager
            .connection_context(connection)
            .await
            .unwrap();
        persist_test_execution_authorization_context_for_principal_with_profile(
            &processor,
            context.principal(),
            &workspace,
            THREAD,
            TURN,
            &pioneer_protocol::system_turn_permission_profile_snapshot(
                pioneer_protocol::TurnPermissionMode::Supervised,
            ),
        )
        .await;
        let (respond_to, responder) = tokio::sync::oneshot::channel();
        processor
            .open_native_permission_request(crate::permissions::GatewayPermissionApprovalRequest {
                request_id: REQUEST.into(),
                workspace_id: Some(workspace.clone()),
                thread_id: Some(THREAD.into()),
                turn_id: Some(TURN.into()),
                tool_name: "exec_command".into(),
                key: pioneer_tools::PermissionRequestKey {
                    profile_mode: pioneer_protocol::TurnPermissionMode::Supervised,
                    tool_name: "exec_command".into(),
                    action: pioneer_tools::PermissionActionKind::ShellCommand,
                    normalized_scope_hash: "post-open-scope".into(),
                    turn_id: TURN.into(),
                },
                reason: pioneer_tools::PermissionDecisionReason::PolicyRequiresApproval,
                summary: Some("bounded replay fixture".into()),
                details: vec![],
                respond_to,
            })
            .await;
        recv_notification_by_method(&mut rx, events::TURN_PERMISSION_REQUEST_OPENED).await;
        if orphan {
            processor
                .native_permission_pending_requests
                .lock()
                .await
                .remove(REQUEST);
        }
        Self {
            processor,
            connection,
            workspace,
            rx,
            responder,
        }
    }

    fn pause(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        *self
            .processor
            .session_manager
            .replay_delivery_pause
            .lock()
            .unwrap() = Some(DeliveryPause {
            connection: self.connection,
            method: events::TURN_PERMISSION_REQUEST_OPENED,
            entered,
            release: released,
        });
        (waiting, release)
    }

    async fn open(&self, label: &str) -> (String, crate::message::AbortOnDropMessageTask<()>) {
        let id = generate_test_request_id("postopen", label);
        let context = self
            .processor
            .session_manager
            .connection_context(self.connection)
            .await
            .unwrap();
        let task = self.processor.clone().start_owned_request(context,
            json!({"jsonrpc":"2.0", "id":id, "method":"thread/open", "params":{"thread_id":THREAD}}).to_string());
        (id, task)
    }
}

#[tokio::test]
async fn held_native_live_and_orphan_replay_release_dispatcher_and_cannot_reopen_terminal() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for orphan in [false, true] {
            let mut f = Fixture::new(orphan).await;
            let (entered, release) = f.pause();
            let (id, mut opening) = f.open("held").await;
            entered.await.unwrap();
            // This is the same owned completion that the socket pump waits on.
            // The old serial tail cannot complete while this OPENED is paused.
            opening.join().await.unwrap();
            recv_response_by_id(&mut f.rx, &id).await;
            recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
            let next = generate_test_request_id("postopen", "nextordinary");
            let context = f
                .processor
                .session_manager
                .connection_context(f.connection)
                .await
                .unwrap();
            let mut ordinary = f.processor.clone().start_owned_request(
                context,
                json!({"jsonrpc":"2.0", "id":next, "method":"workspace/select",
                    "params":{"workspace_id":f.workspace,"make_current":false}})
                .to_string(),
            );
            ordinary.join().await.unwrap();
            recv_response_by_id(&mut f.rx, &next).await;
            let resolve = generate_test_request_id("postopen", "resolve");
            f.processor.process_request_for_connection(f.connection, &json!({
                "jsonrpc":"2.0", "id":resolve, "method":"turn/permission/request/respond",
                "params":{"request_id":REQUEST,"workspace_id":f.workspace,"thread_id":THREAD,
                    "turn_id":TURN,"resolution":"allow_once"}
            }).to_string()).await;
            let (_, terminal) = recv_response_and_notification_by_id_method(
                &mut f.rx,
                &resolve,
                events::TURN_PERMISSION_REQUEST_RESOLVED,
            )
            .await;
            let terminal: TurnPermissionRequestResolvedNotification =
                serde_json::from_value(terminal.params.unwrap()).unwrap();
            assert_eq!(
                terminal.resolution,
                TurnPermissionApprovalResolution::AllowOnce
            );
            release.send(()).unwrap();
            f.processor
                .session_manager
                .drain_thread_replays(f.connection)
                .await;
            while let Ok(Message::Text(text)) = f.rx.try_recv() {
                let frame: JsonValue = serde_json::from_str(&text).unwrap();
                assert_ne!(
                    frame["method"],
                    events::TURN_PERMISSION_REQUEST_OPENED,
                    "captured pending must not reopen after actual typed terminal"
                );
                if frame["method"] == events::TURN_PERMISSION_REQUEST_RESOLVED {
                    let terminal: TurnPermissionRequestResolvedNotification =
                        serde_json::from_value(frame["params"].clone()).unwrap();
                    assert_eq!(
                        terminal.resolution,
                        TurnPermissionApprovalResolution::AllowOnce
                    );
                }
            }
            if !orphan {
                assert_eq!(
                    f.responder.await.unwrap(),
                    pioneer_tools::PermissionApprovalResolution::AllowOnce
                );
            }
            let stored = f
                .processor
                .crud_store
                .get_cli_runtime_pending_request(REQUEST)
                .await
                .unwrap()
                .unwrap();
            assert!(stored.response_json.unwrap().contains("allow_once"));
        }
    })
    .await
    .expect("post-open dispatcher watchdog");
}

#[tokio::test]
async fn same_socket_resubscribe_rejects_old_actual_enqueue_and_old_cleanup_preserves_new_owner() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut f = Fixture::new(false).await;
        let (entered, release) = f.pause();
        let (first, mut opening) = f.open("old").await;
        entered.await.unwrap();
        opening.join().await.unwrap();
        recv_response_by_id(&mut f.rx, &first).await;
        recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
        let old = f
            .processor
            .thread_manager
            .subscription_incarnation(f.connection, THREAD)
            .await
            .unwrap();
        f.processor
            .thread_manager
            .thread_unsubscribe(f.connection, THREAD)
            .await;
        let (second, mut reopening) = f.open("replacement").await;
        reopening.join().await.unwrap();
        recv_response_by_id(&mut f.rx, &second).await;
        recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
        recv_notification_by_method(&mut f.rx, events::TURN_PERMISSION_REQUEST_OPENED).await;
        // The old task is cancelled while suspended; its drop must not abort
        // the replacement's shared socket or unregister the current membership.
        let _ = release.send(());
        f.processor
            .session_manager
            .drain_thread_replays(f.connection)
            .await;
        let current = f
            .processor
            .thread_manager
            .subscription_incarnation(f.connection, THREAD)
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&old, &current));
        assert!(
            f.rx.try_recv().is_err(),
            "old OPENED must never reach the new membership"
        );
        let (third, mut repeated) = f.open("dedup").await;
        repeated.join().await.unwrap();
        recv_response_by_id(&mut f.rx, &third).await;
        recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
        f.processor
            .session_manager
            .drain_thread_replays(f.connection)
            .await;
        assert!(
            f.rx.try_recv().is_err(),
            "same membership does not replay pending twice"
        );
        assert!(Arc::ptr_eq(
            &current,
            &f.processor
                .thread_manager
                .subscription_incarnation(f.connection, THREAD)
                .await
                .unwrap()
        ));
    })
    .await
    .expect("same-socket replay watchdog");
}

#[tokio::test]
async fn failed_reply_handoff_releases_obligation_without_cancelling_durable_request() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut f = Fixture::new(false).await;
        let (entered, captured) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        *f.processor
            .session_manager
            .replay_delivery_pause
            .lock()
            .unwrap() = Some(DeliveryPause {
            connection: f.connection,
            method: pioneer_protocol::constants::methods::THREAD_OPEN,
            entered,
            release: released,
        });
        let (_, mut opening) = f.open("replyfailure").await;
        captured.await.unwrap();
        f.processor
            .session_manager
            .unregister_connection(f.connection)
            .await;
        f.processor
            .thread_manager
            .connection_closed_with_runtime_details(f.connection)
            .await;
        release.send(()).unwrap();
        opening.join().await.unwrap();
        assert!(
            f.rx.try_recv().is_err(),
            "no successful reply or ownerless tail after failed admission"
        );
        assert!(
            matches!(
                f.responder.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ),
            "delivery cancellation must not cancel the execution responder"
        );
        let stored = f
            .processor
            .crud_store
            .get_cli_runtime_pending_request(REQUEST)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.status,
            pioneer_crud::CliRuntimePendingRequestStatus::Pending
        );
        let (outbound, rx) = outbound_channel(128);
        f.connection =
            register_authenticated_test_connection(&f.processor.session_manager, outbound).await;
        f.rx = rx;
        let (retry, mut opening) = f.open("reconnect").await;
        opening.join().await.unwrap();
        recv_response_by_id(&mut f.rx, &retry).await;
        recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
        let opened =
            recv_notification_by_method(&mut f.rx, events::TURN_PERMISSION_REQUEST_OPENED).await;
        assert_eq!(opened.params.unwrap()["request"]["request_id"], REQUEST);
        f.processor
            .session_manager
            .drain_thread_replays(f.connection)
            .await;
    })
    .await
    .expect("failed reply/reconnect watchdog");
}
