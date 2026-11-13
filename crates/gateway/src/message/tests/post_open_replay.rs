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
    outbound: crate::session::SessionOutbound,
    producer_rx: Option<OutboundTestReceiver>,
    responder: tokio::sync::oneshot::Receiver<pioneer_tools::PermissionApprovalResolution>,
}
impl Fixture {
    async fn new(orphan: bool) -> Self {
        let (workspaces, store, workspace) = setup_workspace_manager().await;
        let sessions = Arc::new(SessionManager::new());
        let (outbound, mut rx) = outbound_channel(128);
        let connection = register_authenticated_test_connection(&sessions, outbound.clone()).await;
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
            outbound,
            producer_rx: None,
            responder,
        }
    }

    async fn use_member_observer(&mut self) {
        materialize_test_member_collaborator(&self.processor.crud_store, &self.workspace, THREAD)
            .await;
        let (outbound, rx) = outbound_channel(128);
        self.connection = self
            .processor
            .session_manager
            .register_connection(outbound.clone(), authenticated_test_member_collaborator())
            .await
            .unwrap();
        self.outbound = outbound;
        let previous = std::mem::replace(&mut self.rx, rx);
        if self.producer_rx.is_none() {
            self.producer_rx = Some(previous);
        }
    }

    fn pause(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        self.pause_method(events::TURN_PERMISSION_REQUEST_OPENED)
    }
    fn pause_method(
        &self,
        method: &'static str,
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
            method,
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
            let (correction_entered, correction_release) =
                f.pause_method(events::TURN_PERMISSION_REQUEST_RESOLVED);
            release.send(()).unwrap();
            correction_entered.await.unwrap();
            renew_same_subject(&f.processor.session_manager, f.connection).await;
            correction_release.send(()).unwrap();
            let corrected =
                recv_notification_by_method(&mut f.rx, events::TURN_PERMISSION_REQUEST_RESOLVED)
                    .await;
            let corrected: TurnPermissionRequestResolvedNotification =
                serde_json::from_value(corrected.params.unwrap()).unwrap();
            assert_eq!(
                corrected.resolution,
                TurnPermissionApprovalResolution::AllowOnce
            );
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
            register_authenticated_test_connection(&f.processor.session_manager, outbound.clone())
                .await;
        f.outbound = outbound;
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

pub(super) async fn renew_same_subject(sessions: &SessionManager, connection: ConnectionId) {
    let before = sessions.connection_principal(connection).await.unwrap();
    let mut renewed = (*before).clone();
    renewed.access_jti.push_str("-rotated");
    renewed.access_expires_at_unix -= 1;
    sessions
        .renew_connection_principal(connection, &before, Arc::new(renewed))
        .await
        .unwrap();
}

#[tokio::test]
async fn native_replay_and_live_terminal_survive_same_socket_renewal_without_lost_cards() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for orphan in [false, true] {
            let mut f = Fixture::new(orphan).await;
            let (entered, release) = f.pause();
            let (id, mut opening) = f.open("renewal").await;
            entered.await.unwrap();
            opening.join().await.unwrap();
            let response = recv_response_by_id(&mut f.rx, &id).await;
            let response: pioneer_protocol::ThreadStartResponse =
                serde_json::from_value(response.result).unwrap();
            recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
            renew_same_subject(&f.processor.session_manager, f.connection).await;
            release.send(()).unwrap();
            let opened =
                recv_notification_by_method(&mut f.rx, events::TURN_PERMISSION_REQUEST_OPENED)
                    .await;
            let opened: pioneer_protocol::TurnPermissionRequestOpenedNotification =
                serde_json::from_value(opened.params.unwrap()).unwrap();
            let client = pioneer_client::core::ClientCore::new();
            client.upsert_thread(response.thread);
            client.apply_thread_notification(
                pioneer_protocol::GatewayNotification::TurnPermissionRequestOpened(opened),
            );
            assert_eq!(
                client
                    .pending_requests_for_scope(Some(&f.workspace), Some(THREAD))
                    .len(),
                1
            );
            f.processor
                .session_manager
                .drain_thread_replays(f.connection)
                .await;
            let token = f
                .processor
                .thread_manager
                .subscription_incarnation(f.connection, THREAD)
                .await
                .unwrap();
            let (repeat, mut repeated) = f.open("renewaldedup").await;
            repeated.join().await.unwrap();
            recv_response_by_id(&mut f.rx, &repeat).await;
            recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
            f.processor
                .session_manager
                .drain_thread_replays(f.connection)
                .await;
            assert!(
                f.rx.try_recv().is_err(),
                "a delivered pending replay remains deduplicated"
            );
            assert!(Arc::ptr_eq(
                &token,
                &f.processor
                    .thread_manager
                    .subscription_incarnation(f.connection, THREAD)
                    .await
                    .unwrap()
            ));
            let (entered, release) = f.pause_method(events::TURN_PERMISSION_REQUEST_RESOLVED);
            let context = f
                .processor
                .session_manager
                .connection_context(f.connection)
                .await
                .unwrap();
            let id = generate_test_request_id("postopen", "renewedterminal");
            let mut resolving = f.processor.clone().start_owned_request(
                context,
                json!({
                    "jsonrpc":"2.0","id":id,"method":"turn/permission/request/respond",
                    "params":{"request_id":REQUEST,"resolution":"allow_once"}
                })
                .to_string(),
            );
            entered.await.unwrap();
            renew_same_subject(&f.processor.session_manager, f.connection).await;
            release.send(()).unwrap();
            resolving.join().await.unwrap();
            let (_, terminal) = recv_response_and_notification_by_id_method(
                &mut f.rx,
                &id,
                events::TURN_PERMISSION_REQUEST_RESOLVED,
            )
            .await;
            let terminal: TurnPermissionRequestResolvedNotification =
                serde_json::from_value(terminal.params.unwrap()).unwrap();
            assert_eq!(
                terminal.resolution,
                TurnPermissionApprovalResolution::AllowOnce
            );
            client.apply_thread_notification(
                pioneer_protocol::GatewayNotification::TurnPermissionRequestResolved(terminal),
            );
            assert!(
                client
                    .pending_requests_for_scope(Some(&f.workspace), Some(THREAD))
                    .is_empty()
            );
            // A normal rotation keeps the same socket usable, rather than
            // hiding a failed delivery behind forced reconnect on every renewal.
            f.outbound
                .try_send_data(Message::Text("same socket remains live".into()))
                .unwrap();
        }
    })
    .await
    .expect("native renewal watchdog");
}

#[tokio::test]
async fn renewal_to_denied_role_does_not_reuse_old_execution_authorization() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut f = Fixture::new(false).await;
        let (entered, release) = f.pause();
        let (id, mut opening) = f.open("deniedrenewal").await;
        entered.await.unwrap();
        opening.join().await.unwrap();
        recv_response_by_id(&mut f.rx, &id).await;
        recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
        let old = f
            .processor
            .session_manager
            .connection_principal(f.connection)
            .await
            .unwrap();
        let mut changed = (*old).clone();
        changed.kind = pioneer_protocol::PrincipalKind::User;
        changed.role_key = Some(pioneer_protocol::RoleKey::member());
        changed.access_jti.push_str("-role-change");
        f.processor
            .session_manager
            .renew_connection_principal(f.connection, &old, Arc::new(changed))
            .await
            .unwrap();
        release.send(()).unwrap();
        f.processor
            .session_manager
            .drain_thread_replays(f.connection)
            .await;
        // This principal has no Member workspace grant. Fresh authorization
        // must fail closed, and the lost observation must require recovery.
        f.outbound.writer_stopped().await;
        assert!(
            f.outbound
                .try_send_data(Message::Text("forbidden".into()))
                .is_err()
        );
        while let Ok(Message::Text(text)) = f.rx.try_recv() {
            let frame: JsonValue = serde_json::from_str(&text).unwrap();
            assert_ne!(frame["method"], events::TURN_PERMISSION_REQUEST_OPENED);
        }
        assert_eq!(
            f.processor
                .crud_store
                .get_cli_runtime_pending_request(REQUEST)
                .await
                .unwrap()
                .unwrap()
                .status,
            pioneer_crud::CliRuntimePendingRequestStatus::Pending
        );
    })
    .await
    .expect("denied renewal watchdog");
}

#[tokio::test]
async fn repeated_rotation_exhausts_bounded_delivery_and_forces_recoverable_socket_teardown() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut f = Fixture::new(false).await;
        let (entered, mut release) = f.pause();
        let (id, mut opening) = f.open("rotationexhaustion").await;
        entered.await.unwrap();
        opening.join().await.unwrap();
        recv_response_by_id(&mut f.rx, &id).await;
        recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
        for attempt in 0..3 {
            renew_same_subject(&f.processor.session_manager, f.connection).await;
            if attempt == 2 {
                release.send(()).unwrap();
                break;
            }
            let (entered, next_release) = f.pause();
            release.send(()).unwrap();
            entered.await.unwrap();
            release = next_release;
        }
        f.processor
            .session_manager
            .drain_thread_replays(f.connection)
            .await;
        f.outbound.writer_stopped().await;
        assert!(
            f.outbound
                .try_send_data(Message::Text("must reconnect".into()))
                .is_err()
        );
        while let Ok(Message::Text(text)) = f.rx.try_recv() {
            let frame: JsonValue = serde_json::from_str(&text).unwrap();
            assert_ne!(frame["method"], events::TURN_PERMISSION_REQUEST_OPENED);
        }
        assert_eq!(
            f.processor
                .crud_store
                .get_cli_runtime_pending_request(REQUEST)
                .await
                .unwrap()
                .unwrap()
                .status,
            pioneer_crud::CliRuntimePendingRequestStatus::Pending
        );
        assert!(matches!(
            f.responder.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
    })
    .await
    .expect("bounded rotation watchdog");
}

/// The fault is in the actual resource-authorization future, before recipient
/// selection/serialization/Session enqueue. It never supplies a delivery result.
pub(super) fn pause_auth_unavailable(
    processor: &MessageProcessor,
    connection: ConnectionId,
    method: &'static str,
    thread_id: &str,
    skip: usize,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *processor.execution_auth_fault.lock().unwrap() =
        Some(super::super::notifications::ExecutionAuthFault {
            connection,
            method,
            thread_id: thread_id.to_owned(),
            skip,
            entered,
            release: released,
        });
    (waiting, release)
}

// Each case creates its own real database. Keep a watchdog per scenario so
// setup for the other resource passes cannot consume its recovery budget.
async fn assert_native_authorization_unavailable_requires_recovery(orphan: bool, pass: usize) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut f = Fixture::new(orphan).await;
        f.use_member_observer().await;
        let (entered, release) = pause_auth_unavailable(
            &f.processor,
            f.connection,
            events::TURN_PERMISSION_REQUEST_OPENED,
            THREAD,
            pass,
        );
        let (id, mut opening) = f.open("authunavailable").await;
        entered.await.unwrap();
        opening.join().await.unwrap();
        recv_response_by_id(&mut f.rx, &id).await;
        recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
        release.send(()).unwrap();
        f.processor
            .session_manager
            .drain_thread_replays(f.connection)
            .await;
        f.outbound.writer_stopped().await;
        assert!(
            f.outbound
                .try_send_data(Message::Text("lost replay cannot stay live".into()))
                .is_err()
        );
        while let Ok(Message::Text(text)) = f.rx.try_recv() {
            let frame: JsonValue = serde_json::from_str(&text).unwrap();
            assert_ne!(frame["method"], events::TURN_PERMISSION_REQUEST_OPENED);
        }
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
        if !orphan {
            assert!(matches!(
                f.responder.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
        }
        // A completed replay flag cannot justify another successful reply
        // on the sealed socket. Recovery must establish a new incarnation.
        let (repeat, mut repeated) = f.open("authlostdedup").await;
        repeated.join().await.unwrap();
        while let Ok(Message::Text(text)) = f.rx.try_recv() {
            let frame: JsonValue = serde_json::from_str(&text).unwrap();
            assert!(!(frame["id"] == repeat && frame.get("result").is_some()));
        }
        let old = f
            .processor
            .thread_manager
            .subscription_incarnation(f.connection, THREAD)
            .await
            .unwrap();
        f.processor
            .session_manager
            .unregister_connection(f.connection)
            .await;
        f.processor
            .thread_manager
            .connection_closed(f.connection)
            .await;
        f.use_member_observer().await;
        let (id, mut reopening) = f.open("authrecovered").await;
        reopening.join().await.unwrap();
        recv_response_by_id(&mut f.rx, &id).await;
        recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
        let opened =
            recv_notification_by_method(&mut f.rx, events::TURN_PERMISSION_REQUEST_OPENED).await;
        assert_eq!(opened.params.unwrap()["request"]["request_id"], REQUEST);
        f.processor
            .session_manager
            .drain_thread_replays(f.connection)
            .await;
        assert!(!Arc::ptr_eq(
            &old,
            &f.processor
                .thread_manager
                .subscription_incarnation(f.connection, THREAD)
                .await
                .unwrap()
        ));
    })
    .await
    .expect("resource authorization replay watchdog");
}

#[tokio::test]
async fn native_authorization_unavailable_at_first_resource_pass_requires_recovery() {
    assert_native_authorization_unavailable_requires_recovery(false, 0).await;
}

#[tokio::test]
async fn native_authorization_unavailable_at_second_resource_pass_requires_recovery() {
    assert_native_authorization_unavailable_requires_recovery(false, 1).await;
}

#[tokio::test]
async fn native_authorization_unavailable_at_third_resource_pass_requires_recovery() {
    assert_native_authorization_unavailable_requires_recovery(false, 2).await;
}

#[tokio::test]
async fn orphan_native_authorization_unavailable_requires_recovery() {
    assert_native_authorization_unavailable_requires_recovery(true, 2).await;
}

#[tokio::test]
async fn native_live_typed_terminal_auth_unavailable_requires_recovery_instead_of_lost_card() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut f = Fixture::new(false).await;
        let responder_connection = f.connection;
        f.use_member_observer().await;
        let (id, mut opening) = f.open("terminalauth").await;
        opening.join().await.unwrap();
        recv_response_by_id(&mut f.rx, &id).await;
        recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
        let opened =
            recv_notification_by_method(&mut f.rx, events::TURN_PERMISSION_REQUEST_OPENED).await;
        assert_eq!(opened.params.unwrap()["request"]["request_id"], REQUEST);
        f.processor
            .session_manager
            .drain_thread_replays(f.connection)
            .await;
        let (entered, release) = pause_auth_unavailable(
            &f.processor,
            f.connection,
            events::TURN_PERMISSION_REQUEST_RESOLVED,
            THREAD,
            2,
        );
        let context = f
            .processor
            .session_manager
            .connection_context(responder_connection)
            .await
            .unwrap();
        let id = generate_test_request_id("postopen", "terminalauthresolve");
        let mut resolving = f.processor.clone().start_owned_request(
            context,
            json!({
                "jsonrpc":"2.0", "id":id, "method":"turn/permission/request/respond",
                "params":{"request_id":REQUEST,"resolution":"allow_once"}
            })
            .to_string(),
        );
        entered.await.unwrap();
        release.send(()).unwrap();
        resolving.join().await.unwrap();
        f.outbound.writer_stopped().await;
        assert!(
            f.outbound
                .try_send_data(Message::Text("terminal loss requires reconnect".into()))
                .is_err()
        );
        assert!(
            f.rx.try_recv().is_err(),
            "no falsely delivered terminal frame"
        );
        assert_eq!(
            (&mut f.responder).await.unwrap(),
            pioneer_tools::PermissionApprovalResolution::AllowOnce
        );
        let stored = f
            .processor
            .crud_store
            .get_cli_runtime_pending_request(REQUEST)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            stored.status,
            pioneer_crud::CliRuntimePendingRequestStatus::Pending
        );
        assert!(stored.response_json.unwrap().contains("allow_once"));
        // Delivery recovery does not rewrite the terminal into Expired or cancel
        // accepted execution; the authoritative reconnect set has no pending P.
        f.processor
            .session_manager
            .unregister_connection(f.connection)
            .await;
        f.processor
            .thread_manager
            .connection_closed(f.connection)
            .await;
        let (outbound, rx) = outbound_channel(128);
        f.connection = f
            .processor
            .session_manager
            .register_connection(outbound.clone(), authenticated_test_member_collaborator())
            .await
            .unwrap();
        f.rx = rx;
        f.outbound = outbound;
        let (id, mut reopening) = f.open("terminalauthreconnect").await;
        reopening.join().await.unwrap();
        recv_response_by_id(&mut f.rx, &id).await;
        recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
        f.processor
            .session_manager
            .drain_thread_replays(f.connection)
            .await;
        assert!(
            f.rx.try_recv().is_err(),
            "resolved P cannot reopen on recovery"
        );
    })
    .await
    .expect("typed terminal authorization watchdog");
}

#[tokio::test]
async fn old_authorization_error_cannot_seal_a_resubscribed_member_socket() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut f = Fixture::new(false).await;
        f.use_member_observer().await;
        let (id, mut opening) = f.open("oldauthscope").await;
        opening.join().await.unwrap();
        recv_response_by_id(&mut f.rx, &id).await;
        recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
        recv_notification_by_method(&mut f.rx, events::TURN_PERMISSION_REQUEST_OPENED).await;
        f.processor
            .session_manager
            .drain_thread_replays(f.connection)
            .await;
        let old = f
            .processor
            .thread_manager
            .subscription_incarnation(f.connection, THREAD)
            .await
            .unwrap();
        let (entered, release) = pause_auth_unavailable(
            &f.processor,
            f.connection,
            events::TURN_PERMISSION_REQUEST_OPENED,
            THREAD,
            0,
        );
        // Deliberately poll a captured producer even after retirement. The
        // ordinary supervised owner cancels it; this cut independently tests
        // the final recovery gate rather than relying on cancellation winning.
        let late =
            {
                let processor = f.processor.clone();
                let workspace = f.workspace.clone();
                let connection = f.connection;
                tokio::spawn(async move {
                    old.run(processor.replay_native_permission_requests_for_thread(
                        connection, &workspace, THREAD,
                    ))
                    .await;
                })
            };
        entered.await.unwrap();
        f.processor
            .thread_manager
            .thread_unsubscribe(f.connection, THREAD)
            .await;
        let (id, mut replacement) = f.open("newauthscope").await;
        replacement.join().await.unwrap();
        recv_response_by_id(&mut f.rx, &id).await;
        recv_notification_by_method(&mut f.rx, events::THREAD_STARTED).await;
        recv_notification_by_method(&mut f.rx, events::TURN_PERMISSION_REQUEST_OPENED).await;
        f.processor
            .session_manager
            .drain_thread_replays(f.connection)
            .await;
        release.send(()).unwrap();
        late.await.unwrap();
        assert!(
            f.rx.try_recv().is_err(),
            "retired error cannot enqueue an old frame"
        );
        f.outbound
            .try_send_data(Message::Text("replacement healthy".into()))
            .unwrap();
        assert!(
            matches!(f.rx.recv().await, Some(Message::Text(text)) if text == "replacement healthy")
        );
        assert_eq!(
            f.processor
                .crud_store
                .get_cli_runtime_pending_request(REQUEST)
                .await
                .unwrap()
                .unwrap()
                .status,
            pioneer_crud::CliRuntimePendingRequestStatus::Pending
        );
        assert!(matches!(
            f.responder.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
    })
    .await
    .expect("old authorization recovery watchdog");
}

#[tokio::test]
async fn opening_reply_ack_and_owned_replay_have_separate_live_boundaries() {
    const NAME: &str = "message::tests::post_open_replay::opening_reply_ack_and_owned_replay_have_separate_live_boundaries";
    const CHILD: &str = "PIONEER_OPENING_REPLAY_OBSERVER_CHILD";
    const DONE: &str = "opening-replay-observer-completed";
    if std::env::var(CHILD).ok().as_deref() != Some(NAME) {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", NAME, "--test-threads=1", "--nocapture"])
            .env(CHILD, NAME)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains(DONE),
            "exact child must execute its assertions"
        );
        return;
    }
    use pioneer_observability::{
        live::{JournalPayload, WorkState},
        opening::Phase,
        work::Outcome,
    };
    pioneer_observability::set_telemetry_enabled(true);
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut fixture = Fixture::new(false).await;
        let (entered, release) = fixture.pause();
        let (id, mut opening) = fixture.open("observed-boundaries").await;
        entered.await.unwrap();
        opening.join().await.unwrap();
        recv_response_by_id(&mut fixture.rx, &id).await;
        let snapshot = pioneer_observability::live::snapshot().unwrap();
        assert!(snapshot.active.iter().any(|work| work.opening_phase == Some(Phase::ReplayTail)), "owned replay is still live after dispatcher return and local reply handoff");
        for phase in [Phase::ReplyAdmission, Phase::Acknowledge] {
            assert!(!snapshot.active.iter().any(|work| work.opening_phase == Some(phase)));
            assert!(snapshot.journal.iter().any(|entry| matches!(&entry.payload, JournalPayload::Work(work) if work.opening_phase == Some(phase) && work.state == WorkState::Terminal(Outcome::Completed))));
        }
        release.send(()).unwrap();
        fixture.processor.session_manager.drain_thread_replays(fixture.connection).await;
        assert!(!pioneer_observability::live::snapshot().unwrap().active.iter().any(|work| work.opening_phase == Some(Phase::ReplayTail)));
    }).await.expect("observed replay owner watchdog");
    pioneer_observability::set_telemetry_enabled(false);
    println!("{DONE}");
}
