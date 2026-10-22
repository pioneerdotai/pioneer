//! Unrun regression: committed Gateway values cross a real socket and the
//! shared client's production ingress. Only auth/me is a synthetic fixture;
//! capability, tree, registration, read, and push traffic use the real Gateway.
use super::*;
use futures_util::SinkExt;
use pioneer_client::{
    core::{ClientCore, ClientScope},
    gateway::{endpoint::GatewayBaseUrl, timings::GatewayWsTimings, types::GatewayEndpointKind},
    transport::ws::{GatewayWsConnectSpec, GatewayWsSessionIdentity},
};
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio_tungstenite::tungstenite::Message as SocketMessage;

const THREAD: &str = "thrPushClient00000001";
const TURN: &str = "trnPushClient00000001";

// RAII also stops the real client workers if a bounded assertion unwinds.
struct ClientOwner(Arc<ClientCore>);
impl Drop for ClientOwner {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

async fn await_unread(core: &ClientCore, workspace: &str, expected: u64) {
    let mut changed = core.watch_publications();
    loop {
        if let Some(tree) = core.workspace_tree(workspace) {
            assert!(
                tree.error().is_none(),
                "tree recovery failed: {:?}",
                tree.error()
            );
            if tree.snapshot().threads_by_id.contains_key(THREAD)
                && tree
                    .snapshot()
                    .unread
                    .iter()
                    .find(|row| row.thread_id == THREAD)
                    .map(|row| row.unread_count)
                    .unwrap_or(0)
                    == expected
            {
                return;
            }
        }
        changed
            .changed()
            .await
            .expect("client publication owner alive");
    }
}

fn auth_fixture() -> pioneer_protocol::AuthMeResponse {
    use pioneer_protocol::*;
    let principal = authenticated_test_superuser();
    AuthMeResponse {
        gateway: AuthGatewaySnapshot {
            id: principal.gateway_id.clone(),
        },
        principal: AuthPrincipalSnapshot {
            id: principal.principal_id.clone(),
            kind: principal.kind,
            display_name: "Superuser".into(),
            nickname: "superuser".into(),
            avatar_revision: None,
        },
        device: AuthDeviceSnapshot {
            id: principal.device_id.clone(),
            installation_id: "gateway-test-superuser".into(),
            display_name: "Gateway Test Superuser".into(),
            client_kind: ClientKind::Desktop,
            status: DeviceStatus::Active,
        },
        session: AuthSessionSnapshot {
            id: principal.session_id.clone(),
            device_id: principal.device_id.clone(),
            token_family_id: TokenFamilyId::new("F00000000000000000001").unwrap(),
            status: AuthSessionStatus::Active,
            refresh_generation: 0,
            refresh_expires_at_unix: u64::MAX,
        },
        role_key: principal.role_key.clone(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_push_crosses_socket_into_shared_client_without_tree_reload() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (workspaces, store, workspace) = setup_workspace_manager().await;
        prepare_post_access_unread_fixture(&store, &workspace).await;
        let sessions = Arc::new(SessionManager::new());
        let (source_tx, mut source_rx) = mpsc::channel(128);
        let source = register_authenticated_test_connection(&sessions, source_tx).await;
        let processor = MessageProcessor::new(
            Arc::new(ThreadManager::new("o4-mini", "openai")), test_provider(),
            sessions.clone(), workspaces, store, test_gateway_secrets(),
            test_summary_config(), test_tool_loop_config(),
        );
        let start = generate_test_request_id("pushclient", "start");
        processor.process_request_for_connection(source, &json!({
            "jsonrpc":"2.0", "id":start, "method":"thread/start",
            "params":{"thread_id":THREAD,"workspace_id":workspace}
        }).to_string()).await;
        let _ = recv_response_by_id(&mut source_rx, &start).await;
        let _ = recv_notification_by_method(&mut source_rx, events::THREAD_STARTED).await;
        send_epic6_test_message(&processor, source, &mut source_rx, THREAD, TURN, "question").await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (outbound_tx, mut outbound_rx) = mpsc::channel(128);
        let observer = register_authenticated_test_connection(&sessions, outbound_tx.clone()).await;
        sessions.set_connection_workspace(observer, Some(workspace.clone())).await;
        let tree_requests = Arc::new(AtomicUsize::new(0));
        let (packets_tx, mut packets_rx) = mpsc::channel(16);
        let cancel = tokio_util::sync::CancellationToken::new();
        let socket = tokio::spawn({
            let processor = processor.clone();
            let sessions = sessions.clone();
            let tree_requests = tree_requests.clone();
            let cancel = cancel.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                // Authentication is seeded by the existing authenticated session
                // fixture. This adapter exercises framing, not HTTP auth admission.
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        frame = socket.next() => {
                            match frame {
                                Some(Ok(SocketMessage::Text(text))) => {
                                    let request: serde_json::Value = serde_json::from_str(&text).unwrap();
                                    let method = request["method"].as_str().unwrap();
                                    if method == "thread/tree" {
                                        tree_requests.fetch_add(1, Ordering::SeqCst);
                                    }
                                    if method == "auth/me" {
                                        outbound_tx.send(Message::Text(json!({
                                            "jsonrpc":"2.0", "id":request["id"], "result":auth_fixture()
                                        }).to_string().into())).await.unwrap();
                                    } else {
                                        processor.process_request_for_connection(observer, &text).await;
                                    }
                                }
                                Some(Ok(SocketMessage::Ping(bytes))) => {
                                    socket.send(SocketMessage::Pong(bytes)).await.unwrap();
                                }
                                Some(Ok(SocketMessage::Close(_))) | None => break,
                                Some(Err(error)) => panic!("client socket: {error}"),
                                _ => {}
                            }
                        }
                        frame = outbound_rx.recv() => {
                            let Some(Message::Text(text)) = frame else { break; };
                            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                            socket.send(SocketMessage::Text(text.to_string().into())).await.unwrap();
                            if value["method"] == events::WORKSPACE_PUSH_CHANGED {
                                let packet: pioneer_protocol::WorkspacePushNotification =
                                    serde_json::from_value(value["params"].clone()).unwrap();
                                packets_tx.send(packet).await.unwrap();
                            }
                        }
                    }
                }
                sessions.unregister_connection(observer).await;
            }
        });
        // Dropping the JoinSet aborts the adapter on panic/timeout as well.
        let mut socket_owner = tokio::task::JoinSet::new();
        let abort = socket.abort_handle();
        socket_owner.spawn(async move { socket.await.unwrap() });
        struct AbortOnDrop(tokio::task::AbortHandle);
        impl Drop for AbortOnDrop { fn drop(&mut self) { self.0.abort(); } }
        let _abort = AbortOnDrop(abort);

        let core = ClientOwner(ClientCore::shared());
        let initial_authorization = core.0.authorization_connection_generation();
        let mut connection_publications = core.0.watch_publications();
        let client = core.0.clone();
        let principal = authenticated_test_superuser();
        tokio::task::spawn_blocking(move || {
            client.transport_runtime().ws_command_sender().connect_and_wait(GatewayWsConnectSpec {
                endpoint_id: "loopback-push".into(), endpoint_name: "Loopback push".into(),
                endpoint_kind: GatewayEndpointKind::Local,
                gateway_base_url: GatewayBaseUrl::parse_presentation(&format!("http://{address}")).unwrap(),
                auth_token: Some(pioneer_protocol::AuthSecretString::new("synthetic-access")),
                session: Some(GatewayWsSessionIdentity {
                    server_gateway_id: principal.gateway_id.clone(), session_id: principal.session_id.clone(),
                    device_id: principal.device_id.clone(), access_expires_at_unix: u64::MAX,
                    refresh_leeway_seconds: 0,
                }),
                timings: GatewayWsTimings::from_millis(5_000, 60_000, 5_000, 1_000, 1_000, 0).unwrap(),
            }).unwrap();
        }).await.unwrap();
        // connect_and_wait installs transport access; the ingress dispatcher
        // separately owns the authorization epoch. Bootstrap after that fence.
        while core.0.authorization_connection_generation() == initial_authorization {
            connection_publications.changed().await.expect("connected authorization publication");
        }
        let client = core.0.clone();
        let bootstrap_workspace = workspace.clone();
        tokio::task::spawn_blocking(move || {
            client.refresh_identity_authorization(pioneer_protocol::AuthorizationCapabilitiesParams {
                workspace_id: Some(bootstrap_workspace.clone()), thread_id: None,
            }).unwrap();
            client.refresh_workspace_tree(&bootstrap_workspace).unwrap();
        }).await.unwrap();
        let subscription = core.0.subscribe(ClientScope::WorkspaceTree {
            workspace_id: Some(workspace.clone()),
        }, NonZeroUsize::new(32).unwrap());
        let snapshot = packets_rx.recv().await.expect("negotiated snapshot");
        assert!(snapshot.snapshot && !snapshot.resync_required);
        assert_eq!(snapshot.indicators.iter().find(|row| row.thread_id == THREAD).unwrap().unread_count, Some(0));
        await_unread(&core.0, &workspace, 0).await;
        let baseline_requests = tree_requests.load(Ordering::SeqCst);
        assert!(baseline_requests > 0, "real tree bootstrap reached Gateway");

        let answer = pioneer_protocol::TurnItem::AgentMessage {
            id: pioneer_protocol::task_delivery_result_item_id("push-client-completion"),
            text: "answer".into(), phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
            markdown: None, markdown_version: None,
        };
        processor.crud_store.materialize_item_completed(pioneer_protocol::ItemCompletedNotification {
            workspace_id: workspace.clone(), thread_id: THREAD.into(), turn_id: TURN.into(), item: answer.clone(),
        }, now_timestamp_secs()).await.unwrap();
        processor.notify_semantic_timeline_item_changed(&workspace, THREAD, TURN, &answer, Some("completed")).await;
        let completed = packets_rx.recv().await.expect("committed completion push");
        assert!(!completed.snapshot && !completed.resync_required);
        assert_eq!(completed.epoch, snapshot.epoch);
        assert_eq!(completed.revision, snapshot.revision + 1);
        await_unread(&core.0, &workspace, 1).await;
        assert_eq!(tree_requests.load(Ordering::SeqCst), baseline_requests);

        // Use the same live client's read RPC. No test code applies its ack to
        // the directory, so the next zero must arrive through production ingress.
        let client = core.0.clone();
        let ack = tokio::task::spawn_blocking(move || {
            client.transport_runtime().ws_command_sender().thread_read(pioneer_protocol::ThreadReadParams {
                thread_id: THREAD.into(), through_turn_id: TURN.into(), through_block: None,
            }).unwrap()
        }).await.unwrap();
        assert_eq!(ack.unread_count, 0);
        let read = packets_rx.recv().await.expect("committed read push");
        assert!(!read.snapshot && !read.resync_required);
        assert_eq!(read.epoch, completed.epoch);
        assert_eq!(read.revision, completed.revision + 1);
        await_unread(&core.0, &workspace, 0).await;
        assert_eq!(tree_requests.load(Ordering::SeqCst), baseline_requests);

        core.0.shutdown();
        drop(subscription);
        cancel.cancel();
        socket_owner.join_next().await.unwrap().unwrap();
        sessions.unregister_connection(source).await;
        assert_eq!(tree_requests.load(Ordering::SeqCst), baseline_requests);
    }).await.expect("bounded committed-push integration");
}
