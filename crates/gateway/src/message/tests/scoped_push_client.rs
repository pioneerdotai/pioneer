//! Unrun regression: committed Gateway values cross a real socket and the
//! shared client's production ingress. Auth issuance/auth-me are synthetic;
//! capability, tree, registration, read, and push traffic use the real Gateway.
use super::*;
use futures_util::SinkExt;
use pioneer_client::{
    core::{ClientCore, ClientScope},
    gateway::{
        endpoint::GatewayBaseUrl,
        session_envelope::GatewaySessionEnvelope,
        session_refresh::{GatewaySessionRefreshRequest, GatewaySessionStorage},
        timings::GatewayWsTimings,
        types::GatewayEndpoint,
        types::GatewayEndpointKind,
    },
};
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio_tungstenite::tungstenite::Message as SocketMessage;

const THREAD: &str = "thrPushClient00000001";
const TURN: &str = "trnPushClient00000001";
const SECOND_THREAD: &str = "thrPushClient00000002";
const SECOND_TURN: &str = "trnPushClient00000002";
const NEXT_TURN: &str = "trnPushClient00000003";
const SECOND_NEXT_TURN: &str = "trnPushClient00000004";
const RENEWED_TURN: &str = "trnPushClient00000005";

// RAII also stops the real client workers if a bounded assertion unwinds.
struct AbortOnDrop(tokio::task::AbortHandle);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

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

struct Storage(std::sync::Mutex<GatewaySessionEnvelope>);
impl GatewaySessionStorage for Storage {
    fn load(&self, _: &GatewayEndpoint) -> anyhow::Result<Option<GatewaySessionEnvelope>> {
        Ok(Some(self.0.lock().unwrap().clone()))
    }
    fn persist(&self, _: &GatewayEndpoint, value: &GatewaySessionEnvelope) -> anyhow::Result<()> {
        *self.0.lock().unwrap() = value.clone();
        Ok(())
    }
}
fn refresh_grant(now: u64, generation: u64) -> pioneer_protocol::AuthRefreshGrant {
    use pioneer_protocol::*;
    let auth = auth_fixture();
    AuthRefreshGrant {
        gateway: auth.gateway,
        principal: auth.principal,
        device: auth.device,
        session: AuthSessionSnapshot {
            refresh_generation: generation,
            refresh_expires_at_unix: now + 7200,
            ..auth.session
        },
        access_token: AuthSecretString::new(format!("synthetic-access-{generation}")),
        access_expires_at_unix: now + if generation <= 1 { 120 } else { 3600 },
        refresh_token: AuthSecretString::new(format!(
            "{}{}",
            REFRESH_CREDENTIAL_PREFIX,
            "a".repeat(REFRESH_CREDENTIAL_BODY_LEN)
        )),
        refresh_generation: generation,
        refresh_expires_at_unix: now + 7200,
        auth_protocol_version: DEVICE_SESSION_AUTH_PROTOCOL_VERSION,
        credential_storage_order: CredentialStorageOrder::PersistRefreshBeforeActivatingAccess,
    }
}
async fn serve_refresh(stream: tokio::net::TcpStream, now: u64, generation: &AtomicUsize) {
    // AuthExchangeClient currently uses a one-shot restricted WebSocket.
    let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
    let Some(Ok(SocketMessage::Text(text))) = socket.next().await else {
        panic!("refresh request")
    };
    let request: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        request["method"],
        pioneer_protocol::constants::methods::AUTH_REFRESH
    );
    let next = generation.fetch_add(1, Ordering::SeqCst) + 1;
    socket
        .send(SocketMessage::Text(
            json!({"jsonrpc":"2.0","id":request["id"],"result":refresh_grant(now,next as u64)})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    socket.send(SocketMessage::Close(None)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_push_crosses_socket_into_shared_client_without_tree_reload() {
    committed_push_client_fixture(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hidden_task_cli_start_keeps_combined_sidebar_scope_without_client_recovery() {
    committed_push_client_fixture(true).await;
}

// The real provider materialization/handler boundary retains the persisted
// Task action envelope and performs its own policy, replay and durable commit.
async fn invoke_hidden_agent_action(
    processor: &Arc<MessageProcessor>,
    context: pioneer_agent::TurnToolContext,
    call_id: &str,
    name: &str,
    arguments: serde_json::Value,
) -> pioneer_protocol::AgentToolSafeResult {
    let tools = crate::message::agent_action_tools::materialize(processor.clone(), context.clone())
        .await
        .unwrap();
    let handler = tools
        .bundles
        .into_iter()
        .flat_map(|bundle| bundle.handlers)
        .find(|(tool, _)| tool == name)
        .unwrap_or_else(|| panic!("permitted production tool missing: {name}"))
        .1;
    let trace = pioneer_tools::ToolEventBus::new(16).start_trace(context.turn_id, call_id, name);
    let output = handler
        .handle(
            pioneer_tools::ToolInvocation {
                call_id: call_id.into(),
                tool_name: name.into(),
                source: pioneer_tools::ToolCallSource::Model,
                payload: pioneer_tools::ToolPayload::Function { arguments },
                workdir: Default::default(),
                environment: Default::default(),
                attempt_id: 0,
                idempotency_key: None,
                recovery: Default::default(),
                permission_metadata: Default::default(),
                execution_security_snapshot: None,
                apply_patch_preflight: None,
                cancellation: tokio_util::sync::CancellationToken::new(),
            },
            trace,
        )
        .await
        .unwrap_or_else(|error| panic!("permitted {name} must commit: {error:?}"));
    assert!(output.success(), "permitted {name}: {}", output.raw_text());
    serde_json::from_value(output.raw_json()).unwrap()
}

async fn committed_push_client_fixture(hidden_cli_start: bool) {
    let stage = std::cell::Cell::new("workspace setup");
    tokio::time::timeout(Duration::from_secs(30), async {
        let (workspaces, store, workspace) = setup_workspace_manager().await;
        prepare_post_access_unread_fixture(&store, &workspace).await;
        let sessions = Arc::new(SessionManager::new());
        let (source_tx, mut source_wire_rx) = mpsc::channel(128);
        let (source_forward_tx, mut source_rx) = mpsc::channel(128);
        let source_packets = Arc::new(std::sync::Mutex::new(Vec::<pioneer_protocol::WorkspacePushNotification>::new()));
        let source_recording = source_packets.clone();
        let source_relay = tokio::spawn(async move {
            while let Some(frame) = source_wire_rx.recv().await {
                if let Message::Text(text) = &frame {
                    let wire:serde_json::Value=serde_json::from_str(text).unwrap();
                    if wire["method"] == events::WORKSPACE_PUSH_CHANGED {
                        let mut packets=source_recording.lock().unwrap();
                        assert!(packets.len()<128,"bounded source observation");
                        packets.push(serde_json::from_value(wire["params"].clone()).unwrap());
                    }
                }
                if source_forward_tx.send(frame).await.is_err() { break; }
            }
        });
        let _source_abort = AbortOnDrop(source_relay.abort_handle());
        let source = register_authenticated_test_connection(&sessions, source_tx).await;
        let cli = Arc::new(RecordingCliRuntimeSession::default());
        let cli_manager = test_cli_runtime_manager(cli.clone());
        let processor = MessageProcessor::new(
            Arc::new(ThreadManager::new("o4-mini", "openai")), test_provider(),
            sessions.clone(), workspaces, store.clone(), test_gateway_secrets(),
            test_summary_config(), test_tool_loop_config(),
        );
        let processor = Arc::new(if hidden_cli_start {
            with_enabled_test_cli_runtime_catalog(processor
                .with_cli_runtime_manager_for_tests(cli_manager.clone())
                .with_cli_mcp_readiness_override_for_tests(supported_test_cli_mcp_readiness(CLIAgentRuntimeKind::Codex)))
        } else { processor });
        if hidden_cli_start {
            processor.mark_cli_runtimes_ready_for_tests(&workspace).await.unwrap();
            sync_test_cli_runtime_identities(&processor).await.unwrap();
            processor.mark_cli_reasoning_model_ready_for_tests(&workspace, "codex", "gpt-5").await.unwrap();
            processor.agent_manager.set_context_controller(Some(Arc::new(
                crate::compaction::GatewayNativeContextController::new(Arc::downgrade(&processor))
            ))).await;
            processor.bind_task_bridge().await;
            // This fixture drives the existing durable fanout quantum directly,
            // so no background dispatcher races the observation barriers.
        }
        let start = generate_test_request_id("pushclient", "start");
        processor.process_request_for_connection(source, &json!({
            "jsonrpc":"2.0", "id":start, "method":"thread/start",
            "params":{"thread_id":THREAD,"workspace_id":workspace}
        }).to_string()).await;
        let _ = recv_response_by_id(&mut source_rx, &start).await;
        let _ = recv_notification_by_method(&mut source_rx, events::THREAD_STARTED).await;
        send_epic6_test_message(&processor, source, &mut source_rx, THREAD, TURN, "question").await;

        let start_second = generate_test_request_id("pushclient", "start-second");
        processor.process_request_for_connection(source, &json!({
            "jsonrpc":"2.0", "id":start_second, "method":"thread/start",
            "params":{"thread_id":SECOND_THREAD,"workspace_id":workspace}
        }).to_string()).await;
        let _ = recv_response_by_id(&mut source_rx, &start_second).await;
        let _ = recv_notification_by_method(&mut source_rx, events::THREAD_STARTED).await;
        send_epic6_test_message(&processor, source, &mut source_rx, SECOND_THREAD, SECOND_TURN, "second question").await;

        // Establish the real child, lineage and admitted turn before observing
        // activation. Metadata/root/lineage events may legitimately reset inbox.
        stage.set("prepare hidden task lineage");
        let hidden_task = if hidden_cli_start {
            cli.set_next_native_turn_id("hidden-sidebar-native-turn").await;
            processor.arm_completed_history_preparation_barrier("__task_cli_before_history__");
            let response = create_task_for_test(&processor, detached_cli_task_create_params(
                &workspace, THREAD, "hidden-sidebar-launch", "codex", CLIAgentRuntimeKind::Codex,
                "gpt-5", "hidden sidebar regression",
            )).await.unwrap();
            processor.wait_for_completed_history_preparation_barrier().await;
            let run = response.run.unwrap();
            let child = wait_for_child_lineage_for_run(store.clone(), &run.id).await;
            let materialized = store.get_thread_model(&child.child_thread_id).await.unwrap().unwrap();
            assert_eq!(materialized.sidebar_visibility, pioneer_protocol::ThreadSidebarVisibility::Hidden);
            assert_eq!(materialized.origin_kind, pioneer_protocol::ThreadOriginKind::TaskRun);
            assert_eq!(child.parent_thread_id, THREAD);
            let lineage = store.get_task_thread_lineage(&child.child_thread_id).await.unwrap().unwrap();
            assert_eq!(lineage.root_thread_id, THREAD);
            assert_eq!(lineage.origin_kind.as_deref(), Some("task_run"));
            assert!(store.get_task_execution_admission(&response.task.id).await.unwrap().is_some());
            // The negative control is sidebar eligibility, never ACL denial:
            // this exact superuser can read the durable internal child via root.
            use crate::authorization::{AuthorizationResolver, AuthorizationService, ProofResolution, ResourceAction};
            let principal = authenticated_test_superuser();
            let gate = AuthorizationService::new().authorize_action(principal.kind, principal.role_key.as_ref(), ResourceAction::ThreadRead);
            assert!(matches!(AuthorizationResolver::new((*store).clone())
                .authorize_internal_thread_via_root(&principal, &gate, ResourceAction::ThreadRead,
                    &child.child_thread_id, Some(&workspace)).await.unwrap(), ProofResolution::Authorized(_)));
            assert!(cli.turn_starts.lock().await.is_empty());
            let fanout = processor.scoped_for_background_reconciliation().task_event_fanout_quantum().await.unwrap();
            assert_eq!(fanout.errors, 0);
            Some((response.task.id, run.id, child))
        } else { None };

        use sea_orm::ConnectionTrait;
        let long_preview = "ж界🙂".repeat(100);
        processor.crud_store.database_connection().execute_unprepared(&format!(
            "UPDATE thread SET name='Named detailed thread',preview='{long_preview}' WHERE id='{THREAD}'"
        )).await.unwrap();
        let detailed = processor.crud_store.get_thread_model(THREAD).await.unwrap().unwrap();
        let now = now_timestamp_secs() as u64;
        let refresh_generation = Arc::new(AtomicUsize::new(0));
        let renewals = Arc::new(AtomicUsize::new(0));
        let domain_connections = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (outbound_tx, mut outbound_rx) = mpsc::channel(128);
        let observer = register_authenticated_test_connection(&sessions, outbound_tx.clone()).await;
        sessions.set_connection_workspace(observer, Some(workspace.clone())).await;
        let tree_requests = Arc::new(AtomicUsize::new(0));
        let prefix_requests = Arc::new(AtomicUsize::new(0));
        let push_requests = Arc::new(AtomicUsize::new(0));
        let (packets_tx, mut packets_rx) = mpsc::channel(16);
        let (cursor_tx, mut cursor_rx) = mpsc::channel(16);
        let cancel = tokio_util::sync::CancellationToken::new();
        let socket = tokio::spawn({
            let processor = processor.clone();
            let sessions = sessions.clone();
            let tree_requests = tree_requests.clone();
            let prefix_requests = prefix_requests.clone();
            let push_requests = push_requests.clone();
            let cancel = cancel.clone();
            let refresh_generation=refresh_generation.clone();
            let renewals=renewals.clone();
            let domain_connections=domain_connections.clone();
            async move {
                let (refresh,_) = listener.accept().await.unwrap();
                serve_refresh(refresh,now,&refresh_generation).await;
                let (stream, _) = listener.accept().await.unwrap();
                // Authentication is seeded by the existing authenticated session
                // fixture. This adapter exercises framing, not HTTP auth admission.
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                domain_connections.fetch_add(1,Ordering::SeqCst);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        accepted=listener.accept()=> {
                            let (stream,_) = accepted.unwrap();
                            serve_refresh(stream,now,&refresh_generation).await;
                        },
                        frame = socket.next() => {
                            match frame {
                                Some(Ok(SocketMessage::Text(text))) => {
                                    let request: serde_json::Value = serde_json::from_str(&text).unwrap();
                                    let method = request["method"].as_str().unwrap();
                                    if method == "thread/tree" {
                                        tree_requests.fetch_add(1, Ordering::SeqCst);
                                    }
                                    if method == pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_LIST {
                                        prefix_requests.fetch_add(1, Ordering::SeqCst);
                                    }
                                    if method == pioneer_protocol::constants::methods::WORKSPACE_PUSH {
                                        push_requests.fetch_add(1, Ordering::SeqCst);
                                    }
                                    if method == pioneer_protocol::constants::methods::AUTH_ACCESS_RENEW {
                                        let params:pioneer_protocol::AuthAccessRenewParams=serde_json::from_value(request["params"].clone()).unwrap();
                                        assert_eq!(params.refresh_generation,2);
                                        let expected=sessions.connection_principal(observer).await.unwrap();
                                        let mut next=(*expected).clone();
                                        next.access_expires_at_unix=refresh_grant(now,params.refresh_generation).access_expires_at_unix;
                                        next.access_jti="renewed-live-push-jti".into();
                                        sessions.renew_connection_principal(observer,&expected,Arc::new(next)).await.unwrap();
                                        renewals.fetch_add(1,Ordering::SeqCst);
                                        outbound_tx.send(Message::Text(json!({"jsonrpc":"2.0","id":request["id"],"result":pioneer_protocol::AuthAccessRenewResponse {access_expires_at_unix:now+3600}}).to_string().into())).await.unwrap();
                                    } else if method == "auth/me" {
                                        let grant=refresh_grant(now,refresh_generation.load(Ordering::SeqCst) as u64);
                                        let auth=pioneer_protocol::AuthMeResponse {gateway:grant.gateway,principal:grant.principal,device:grant.device,session:grant.session,role_key:None};
                                        outbound_tx.send(Message::Text(json!({
                                            "jsonrpc":"2.0", "id":request["id"], "result":auth
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
                            if hidden_cli_start {
                                assert_ne!(value["method"], events::THREAD_TREE_CHANGED,
                                    "Hidden producers must not fall back to legacy tree hints");
                            }
                            socket.send(SocketMessage::Text(text.to_string().into())).await.unwrap();
                            if value["method"] == events::THREAD_READ_CURSOR_CHANGED {
                                let cursor: pioneer_protocol::ThreadReadCursorChangedNotification =
                                    serde_json::from_value(value["params"].clone()).unwrap();
                                cursor_tx.send(cursor).await.unwrap();
                            }
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
        let _abort = AbortOnDrop(abort);

        stage.set("connect shared client");
        let core = ClientOwner(ClientCore::shared());
        let initial_authorization = core.0.authorization_connection_generation();
        let mut connection_publications = core.0.watch_publications();
        let grant=refresh_grant(now,0);
        let storage=Arc::new(Storage(std::sync::Mutex::new(GatewaySessionEnvelope {
            schema_version:2,gateway_id:grant.gateway.id.clone(),principal_id:grant.principal.id,
            device_id:grant.device.id,session_id:grant.session.id,token_family_id:grant.session.token_family_id,
            installation_id:"gateway-test-superuser".into(),refresh_generation:0,refresh_expires_at_unix:now+7200,
            refresh_token:grant.refresh_token,pending_refresh_request_id:None,
        })));
        let endpoint=GatewayEndpoint {id:"loopback-push".into(),name:"Loopback push".into(),
            gateway_base_url:GatewayBaseUrl::parse_presentation(&format!("http://{address}")).unwrap(),
            kind:GatewayEndpointKind::Local,session_ref:Some("loopback-push".into()),server_gateway_id:Some(grant.gateway.id),
            workspace_id:None,service_name:None};
        let client=core.0.clone();let session_storage=storage.clone();let session_endpoint=endpoint.clone();
        let connected=tokio::task::spawn_blocking(move || client.ensure_gateway_session(GatewaySessionRefreshRequest {
            endpoint:&session_endpoint,installation_id:"gateway-test-superuser",client_kind:pioneer_protocol::ClientKind::Desktop,
            now_unix:now,timeout:Duration::from_secs(5),ws_timings:GatewayWsTimings::from_millis(5000,60000,5000,1000,1000,0).unwrap(),retry_delays:&[],
        },session_storage.as_ref()).unwrap()).await.unwrap();
        // Public session ensure installs transport access; the ingress dispatcher
        // separately owns the authorization epoch. Bootstrap after that fence.
        stage.set("connected authorization fence");
        while core.0.authorization_connection_generation() == initial_authorization {
            connection_publications.changed().await.expect("connected authorization publication");
        }
        let client = core.0.clone();
        stage.set("bootstrap authorization and directory");
        let bootstrap_workspace = workspace.clone();
        tokio::task::spawn_blocking(move || {
            client.refresh_identity_authorization(pioneer_protocol::AuthorizationCapabilitiesParams {
                workspace_id: Some(bootstrap_workspace.clone()), thread_id: None,
            }).unwrap();
            client.refresh_workspace_tree(&bootstrap_workspace).unwrap();
        }).await.unwrap();
        core.0.upsert_thread(detailed.clone());
        let subscription = core.0.subscribe(ClientScope::WorkspaceTree {
            workspace_id: Some(workspace.clone()),
        }, NonZeroUsize::new(32).unwrap());
        let inbox_subscription = hidden_cli_start.then(|| core.0.subscribe(ClientScope::TaskInbox {
            workspace_id: workspace.clone(),
        }, NonZeroUsize::new(32).unwrap()));
        if hidden_cli_start {
            // Subscription retains delivery; the shell's visible demand owns
            // initial inbox loading before combined push can be negotiated.
            core.0.dispatch(pioneer_client::core::ClientIntent::SetScopeDemand {
                scope: ClientScope::TaskInbox { workspace_id: workspace.clone() },
                demand: pioneer_client::core::ClientDemand::Visible,
                generation: pioneer_client::core::ClientGeneration::new(1),
            });
        }
        stage.set("initial negotiated snapshot");
        let snapshot = loop {
            let packet = packets_rx.recv().await.expect("negotiated snapshot");
            assert!(!packet.resync_required);
            if !hidden_cli_start || (packet.snapshot && packet.inbox.is_some() && !packet.indicators.is_empty()) {
                break packet;
            }
        };
        assert!(snapshot.snapshot && !snapshot.resync_required);
        assert_eq!(snapshot.indicators.iter().find(|row| row.thread_id == THREAD).unwrap().unread_count, Some(0));
        await_unread(&core.0, &workspace, 0).await;
        let baseline_requests = tree_requests.load(Ordering::SeqCst);
        assert!(baseline_requests > 0, "real tree bootstrap reached Gateway");

        if let Some((task_id, run_id, child)) = hidden_task {
            stage.set("hidden CLI activation and root activity");
            assert!(!core.0.workspace_tree(&workspace).unwrap().snapshot().threads_by_id.contains_key(&child.child_thread_id));
            let baseline_prefix = prefix_requests.load(Ordering::SeqCst);
            assert!(baseline_prefix > 0, "visible inbox demand reached the Gateway bootstrap");
            let baseline_push = push_requests.load(Ordering::SeqCst);
            let source_epoch="hidden-source-combined";
            let register_source=generate_test_request_id("pushclient","source-combined");
            processor.process_request_for_connection(source,&json!({
                "jsonrpc":"2.0","id":register_source,"method":pioneer_protocol::constants::methods::WORKSPACE_PUSH,
                "params":pioneer_protocol::WorkspacePushParams {
                    workspace_id:workspace.clone(),epoch:source_epoch.into(),tree:true,inbox:true,
                    thread_ids:vec![THREAD.into(),SECOND_THREAD.into()],run_ids:vec![],inbox_cursor:None,
                }
            }).to_string()).await;
            let registered=recv_response_by_id(&mut source_rx,&register_source).await;
            assert!(serde_json::from_value::<pioneer_protocol::WorkspacePushResponse>(registered.result).unwrap().supported);
            let hold = processor.begin_push_mutation(observer, &workspace, true).unwrap();
            processor.release_completed_history_preparation_barrier();
            let starts = wait_for_cli_runtime_turn_starts(&cli, 1).await;
            assert_eq!(starts.len(), 1, "real Task CLI activation reached recording adapter");
            // Adapter start alone does not prove scheduler RunStarted committed.
            assert_eq!(wait_for_run_status(store.clone(), &run_id, TaskRunStatus::Running).await,
                TaskRunStatus::Running);
            // Drive the same committed fanout owner deterministically while the
            // existing mutation hold coalesces the observed activation window.
            let fanout = processor.scoped_for_background_reconciliation().task_event_fanout_quantum().await.unwrap();
            assert_eq!(fanout.errors, 0);
            assert!(fanout.emitted > 0);
            hold.finish(None, None);
            let started = packets_rx.recv().await.expect("root RunStarted push");
            assert_eq!(started.epoch, snapshot.epoch);
            assert!(!started.snapshot && !started.resync_required);
            assert!(started.thread_summaries.iter().all(|patch| patch.thread.id != child.child_thread_id));
            assert!(started.indicators.iter().all(|patch| patch.thread_id != child.child_thread_id));
            assert!(started.indicators.iter().any(|patch| patch.thread_id == THREAD && patch.has_running_tasks == Some(true)));
            assert!(started.indicators.iter().filter(|patch| patch.thread_id == THREAD).all(|patch| patch.unread_count.is_none_or(|count| count == 0)));
            // Observe shared-client activity before delivering terminal state;
            // outbound content alone cannot prove that ingress accepted it.
            let mut activity_applied = core.0.watch_publications();
            loop {
                let tree = core.0.workspace_tree(&workspace).unwrap();
                assert!(tree.error().is_none());
                if tree.snapshot().running_task_thread_ids.iter().any(|id| id == THREAD) {
                    assert!(!tree.snapshot().threads_by_id.contains_key(&child.child_thread_id));
                    break;
                }
                activity_applied.changed().await.expect("root running activity ingress");
            }
            assert_eq!(tree_requests.load(Ordering::SeqCst), baseline_requests);
            assert_eq!(prefix_requests.load(Ordering::SeqCst), baseline_prefix);
            assert_eq!(push_requests.load(Ordering::SeqCst), baseline_push);
            // Materialize the actual execution-bound production handlers. The
            // CLI task facade exports only task tools and launch options, so
            // these agent actions are exercised at their real handler boundary.
            let hold = processor.begin_push_mutation(observer, &workspace, true).unwrap();
            stage.set("hidden agent actions and peer cursor");
            let action_binding = processor.agent_action_binding(&child.child_turn_id).await.unwrap();
            assert!(action_binding.capabilities.contains(&pioneer_protocol::AgentToolCapability::MessageCreate));
            let target = action_binding.options.target_options.iter()
                .find(|option| option.label == "Current thread").unwrap().id.clone();
            let action_context=pioneer_agent::TurnToolContext {
                workspace_id:workspace.clone(),thread_id:child.child_thread_id.clone(),turn_id:child.child_turn_id.clone(),
            };
            let safe=invoke_hidden_agent_action(&processor,action_context.clone(),"hidden-sidebar-agent-message","thread_message_send",
                serde_json::to_value(pioneer_protocol::AgentSendMessageToolInput {
                    target_option_id:target.clone(),input:pioneer_protocol::AgentAuthoredInput::new(vec![pioneer_protocol::UserInput::Text {
                        text:"hidden agent message".into(),text_elements:vec![],
                    }]),
                }).unwrap(),
            ).await;
            assert_eq!(safe.status, pioneer_protocol::AgentToolResultStatus::Accepted);
            assert!(safe.receipt_id.is_some() && safe.outbox_id.is_some());
            let materialized = store.get_thread_model(&child.child_thread_id).await.unwrap().unwrap();
            assert_eq!(materialized.sidebar_visibility, pioneer_protocol::ThreadSidebarVisibility::Hidden);

            // A real timeline query supplies the exact canonical viewed anchor.
            // This also proves Hidden timeline access remains legal; using the
            // legacy principal-authored anchor would not exercise agent messages.
            stage.set("hidden timeline response");
            let timeline_id = generate_test_request_id("pushclient", "hidden-timeline");
            processor.process_request_for_connection(source, &json!({
                "jsonrpc":"2.0","id":timeline_id,"method":"thread/timeline/page",
                "params":{"threadId":child.child_thread_id,"anchor":{"kind":"newest"},"limit":12}
            }).to_string()).await;
            let timeline = recv_response_by_id(&mut source_rx, &timeline_id).await;
            let timeline: pioneer_protocol::ThreadTimelinePageResponse = serde_json::from_value(timeline.result).unwrap();
            let block = timeline.blocks.iter().find(|block| matches!(&block.kind,
                pioneer_protocol::TimelineBlockKind::UserMessage { text, .. } if text == "hidden agent message")).unwrap();
            let (_,message) = store.get_turn(&child.child_thread_id,block.turn_id.as_deref().unwrap()).await.unwrap().unwrap();
            assert_ne!(message.id,child.child_turn_id);
            assert_eq!(message.mode,pioneer_protocol::ThreadMode::Message);
            assert_eq!(message.status,pioneer_protocol::TurnStatus::Completed);
            assert!(matches!(message.author.as_ref().unwrap().actor,pioneer_protocol::PersistedActorRef::AgentExecution(_)));
            stage.set("hidden read response");
            let read_id = generate_test_request_id("pushclient", "hidden-read");
            processor.process_request_for_connection(source, &json!({
                "jsonrpc":"2.0","id":read_id,"method":"thread/read",
                "params":pioneer_protocol::ThreadReadParams {
                    thread_id:child.child_thread_id.clone(),through_turn_id:message.id.clone(),
                    through_block:Some(pioneer_protocol::ThreadReadPosition {block_id:block.block_id.clone(),sort_key:block.sort_key.clone()}),
                }
            }).to_string()).await;
            let read = recv_response_by_id(&mut source_rx, &read_id).await;
            let read: pioneer_protocol::ThreadReadResponse = serde_json::from_value(read.result).unwrap();
            assert_eq!(read.thread_id, child.child_thread_id);
            assert_eq!(read.cursor.through_turn_id, message.id);
            assert_eq!(read.cursor.sort_key, block.sort_key);
            // The same-principal peer receives cursor synchronization over the
            // real socket even though the sidebar intentionally lacks the child.
            stage.set("hidden peer cursor synchronization");
            let cursor = cursor_rx.recv().await.expect("peer Hidden cursor synchronization");
            assert_eq!(cursor.thread_id, child.child_thread_id);
            assert_eq!(cursor.cursor, read.cursor);

            // Exercise the separate concurrent StartAgent writer too. This is
            // an authorized durable admission; no synthetic dirty call and no
            // explicit outbox/native-executor dispatch supplies its behavior.
            assert!(action_binding.capabilities.contains(&pioneer_protocol::AgentToolCapability::ChildStart));
            assert!(action_binding.options.inherit_parent_identity_available);
            assert!(action_binding.options.inherit_parent_profile_available);
            stage.set("hidden agent start admission");
            let safe=invoke_hidden_agent_action(&processor,action_context,"hidden-sidebar-agent-start","threads_turn_start",
                serde_json::to_value(pioneer_protocol::AgentStartToolInput {
                    target_option_id:target,input:pioneer_protocol::AgentAuthoredInput::new(vec![pioneer_protocol::UserInput::Text {
                        text:"hidden concurrent agent".into(),text_elements:vec![],
                    }]),
                    launch:pioneer_protocol::AgentToolLaunchSelection {
                        identity:pioneer_protocol::AgentToolIdentityChoice::InheritParent,
                        profile:pioneer_protocol::AgentToolProfileChoice::InheritParent,
                        reasoning:None,permission_profile:None,skill_ids:vec![],mcp_server_ids:vec![],
                    },
                }).unwrap(),
            ).await;
            assert!(matches!(safe.status,pioneer_protocol::AgentToolResultStatus::Accepted | pioneer_protocol::AgentToolResultStatus::Queued));
            assert!(safe.receipt_id.is_some() && safe.outbox_id.is_some());
            let started_child=store.get_thread_model(&child.child_thread_id).await.unwrap().unwrap();
            assert_eq!(started_child.sidebar_visibility,pioneer_protocol::ThreadSidebarVisibility::Hidden);
            use sea_orm::EntityTrait;
            let outbox=pioneer_entity::agent_action_outbox::Entity::find_by_id(safe.outbox_id.unwrap())
                .one(&store.database_connection()).await.unwrap().expect("durable StartAgent outbox");
            let dispatch:serde_json::Value=serde_json::from_str(&outbox.payload_json).unwrap();
            assert_eq!(dispatch["dispatch"]["thread_id"],child.child_thread_id);
            let (_,admitted)=store.get_turn(&child.child_thread_id,dispatch["dispatch"]["turn_id"].as_str().unwrap())
                .await.unwrap().expect("durable concurrent Agent start");
            assert_eq!(admitted.mode,pioneer_protocol::ThreadMode::Agent);
            assert_ne!(admitted.id,message.id);
            assert_ne!(admitted.id,child.child_turn_id);
            assert!(matches!(admitted.author.as_ref().unwrap().actor,pioneer_protocol::PersistedActorRef::AgentExecution(_)));
            assert!(pioneer_crud::load_agent_turn_response(&store.database_connection(),&admitted.id).await.unwrap().is_some());
            hold.finish(None, None);

            // A second real root producer is an ordered ingress barrier after
            // start. Hidden-ID rejection would instead request tree/prefix and
            // replace the epoch before these addressed values can apply.
            let hold = processor.begin_push_mutation(observer, &workspace, true).unwrap();
            stage.set("hidden CLI completion");
            complete_recorded_cli_task_turn(&processor, &cli, &cli_manager, &workspace, "codex", THREAD,
                &starts[0].native_thread_id, "hidden-sidebar-native-turn",
                r#"<task_result>{"summary":"hidden execution completed"}</task_result>"#).await;
            assert_eq!(wait_for_task_status(store.clone(), &task_id, TaskStatus::Completed).await, TaskStatus::Completed);
            let fanout = processor.scoped_for_background_reconciliation().task_event_fanout_quantum().await.unwrap();
            assert_eq!(fanout.errors, 0);
            assert!(fanout.emitted > 0);
            hold.finish(None, None);
            stage.set("root terminal and inbox ingress");
            let mut saw_terminal = false;
            let mut saw_inbox = false;
            let mut applied = core.0.watch_publications();
            while !saw_terminal || !saw_inbox {
                let packet = packets_rx.recv().await.expect("root terminal / inbox push");
                assert_eq!(packet.epoch, snapshot.epoch);
                assert!(!packet.snapshot && !packet.resync_required);
                assert!(packet.thread_summaries.iter().all(|patch| patch.thread.id != child.child_thread_id));
                assert!(packet.indicators.iter().all(|patch| patch.thread_id != child.child_thread_id));
                saw_terminal |= packet.indicators.iter().any(|patch| patch.thread_id == THREAD && patch.has_running_tasks == Some(false));
                saw_inbox |= packet.inbox.as_ref().is_some_and(|patch| patch.upserts.iter().any(|entry| entry.run_id == run_id));
            }
            loop {
                // The batch count map omits zeroes; a missing root entry is
                // an authoritative zero, just as in the sidebar publisher.
                let expected_unread = store.unread_counts_for_threads(&authenticated_test_superuser().principal_id,
                    &[THREAD.to_owned()]).await.unwrap().get(THREAD).copied().unwrap_or(0);
                let tree = core.0.workspace_tree(&workspace).unwrap();
                let inbox = core.0.task_inbox(&workspace).unwrap();
                if inbox.items().iter().any(|entry| entry.notification().run_id == run_id)
                    && !tree.snapshot().running_task_thread_ids.iter().any(|id| id == THREAD)
                    && tree.snapshot().unread.iter().find(|entry| entry.thread_id == THREAD)
                        .is_some_and(|entry| entry.unread_count == expected_unread)
                {
                    assert!(!tree.snapshot().threads_by_id.contains_key(&child.child_thread_id));
                    assert_eq!(inbox.unread_count(), 1);
                    break;
                }
                applied.changed().await.expect("terminal combined ingress");
            }
            assert_eq!(tree_requests.load(Ordering::SeqCst), baseline_requests);
            assert_eq!(prefix_requests.load(Ordering::SeqCst), baseline_prefix);
            assert_eq!(push_requests.load(Ordering::SeqCst), baseline_push);
            // Source owns a combined scope too, so read's ready-unread ack
            // bypass is covered in addition to the peer dirty path. Drain to a
            // real subsequent terminal/inbox event, never to an empty poll.
            stage.set("source terminal and inbox barrier");
            let mut source_terminal=false;
            let mut source_inbox=false;
            while !source_terminal || !source_inbox {
                let wire=recv_notification_by_method(&mut source_rx,events::WORKSPACE_PUSH_CHANGED).await;
                let packet:pioneer_protocol::WorkspacePushNotification=serde_json::from_value(wire.params.unwrap()).unwrap();
                assert_eq!(packet.epoch,source_epoch);
                source_terminal |= !packet.snapshot && packet.indicators.iter().any(|entry|entry.thread_id==THREAD && entry.has_running_tasks==Some(false));
                source_inbox |= packet.inbox.as_ref().is_some_and(|patch|patch.upserts.iter().any(|entry|entry.run_id==run_id));
            }
            for packet in source_packets.lock().unwrap().iter() {
                assert!(!packet.resync_required);
                assert!(packet.thread_summaries.iter().all(|patch|patch.thread.id!=child.child_thread_id));
                assert!(packet.indicators.iter().all(|patch|patch.thread_id!=child.child_thread_id));
            }

            // The eligibility guard must not suppress an unknown Visible ID.
            // Seed canonical persisted metadata without a structural hint, then
            // use the ordinary committed Message producer. The accepted client
            // must recover membership from tree/list and negotiate a new epoch.
            stage.set("unknown visible recovery control");
            const UNSEEN: &str = "T0000000000000000000V";
            const UNSEEN_TURN: &str = "T0000000000000000000W";
            let mut visible = store.get_thread_model(SECOND_THREAD).await.unwrap().unwrap();
            visible.id = UNSEEN.into();
            visible.turns.clear();
            visible.preview.clear();
            visible.name = Some("Previously unobserved visible thread".into());
            assert_eq!(visible.sidebar_visibility, pioneer_protocol::ThreadSidebarVisibility::Visible);
            store.upsert_thread_model(&visible, pioneer_protocol::PersistedActorRef::Principal(
                authenticated_test_superuser().principal_id.clone(),
            )).await.unwrap();
            // Opening the persisted thread subscribes only the source session.
            // A system load alone cannot admit its subsequent Message RPC.
            let open_id = generate_test_request_id("pushclient", "unseen-open");
            processor.process_request_for_connection(source, &json!({
                "jsonrpc":"2.0","id":open_id,"method":"thread/start",
                "params":{"thread_id":UNSEEN,"workspace_id":workspace}
            }).to_string()).await;
            let opened = recv_response_by_id(&mut source_rx, &open_id).await;
            let opened: pioneer_protocol::ThreadStartResponse = serde_json::from_value(opened.result).unwrap();
            assert_eq!(opened.thread.id, UNSEEN);
            let _ = recv_notification_by_method(&mut source_rx, events::THREAD_STARTED).await;
            assert!(!core.0.workspace_tree(&workspace).unwrap().snapshot().threads_by_id.contains_key(UNSEEN));
            send_epic6_test_message(&processor, source, &mut source_rx, UNSEEN, UNSEEN_TURN,
                "visible membership recovery control").await;
            loop {
                let packet = packets_rx.recv().await.expect("unknown Visible producer delivery");
                assert!(!packet.resync_required);
                if packet.thread_summaries.iter().any(|patch| patch.thread.id == UNSEEN) {
                    assert_eq!(packet.epoch, snapshot.epoch);
                    assert!(packet.indicators.iter().any(|patch| patch.thread_id == UNSEEN
                        && patch.unread_count == Some(0)));
                    break;
                }
            }
            let recovered = loop {
                let packet = packets_rx.recv().await.expect("canonical Visible membership recovery");
                assert!(!packet.resync_required);
                if packet.snapshot && packet.epoch != snapshot.epoch
                    && packet.indicators.iter().any(|patch| patch.thread_id == UNSEEN) {
                    break packet;
                }
            };
            let mut visible_applied = core.0.watch_publications();
            while !core.0.workspace_tree(&workspace).unwrap().snapshot().threads_by_id.contains_key(UNSEEN) {
                visible_applied.changed().await.expect("recovered Visible directory ingress");
            }
            assert_ne!(recovered.epoch, snapshot.epoch);
            assert!(tree_requests.load(Ordering::SeqCst) > baseline_requests);
            assert!(prefix_requests.load(Ordering::SeqCst) > baseline_prefix);
            assert!(push_requests.load(Ordering::SeqCst) > baseline_push);
            // Retire the adapter before client shutdown drops its socket. A
            // disconnect during the scenario must still fail the adapter task.
            cancel.cancel();
            socket_owner.join_next().await.unwrap().unwrap();
            core.0.shutdown();
            drop(inbox_subscription);
            drop(subscription);
            sessions.unregister_connection(source).await;
            return;
        }

        stage.set("atomic metadata and indicator batch");
        while subscription.try_next().is_some() {}
        let before_batch = core.0.workspace_tree(&workspace).unwrap().revision();
        // The existing source mutation hold deterministically collects two real
        // committed producer updates into one worker pass. No DTO injection.
        let hold = processor.begin_push_mutation(observer, &workspace, true).unwrap();
        for (thread, turn) in [(THREAD, NEXT_TURN), (SECOND_THREAD, SECOND_NEXT_TURN)] {
            send_epic6_test_message(&processor, source, &mut source_rx, thread, turn, "next question").await;
            let answer = pioneer_protocol::TurnItem::AgentMessage {
                id: pioneer_protocol::task_delivery_result_item_id(turn),
                text: "answer".into(), phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                markdown: None, markdown_version: None,
            };
            processor.crud_store.materialize_item_completed(pioneer_protocol::ItemCompletedNotification {
                workspace_id: workspace.clone(), thread_id: thread.into(), turn_id: turn.into(), item: answer.clone(),
            }, now_timestamp_secs()).await.unwrap();
            processor.notify_semantic_timeline_item_changed(&workspace, thread, turn, &answer, Some("completed")).await;
        }
        hold.finish(None, None);
        let completed = packets_rx.recv().await.expect("committed completion push");
        assert!(!completed.snapshot && !completed.resync_required);
        assert_eq!(completed.epoch, snapshot.epoch);
        assert_eq!(completed.revision, snapshot.revision + 1);
        await_unread(&core.0, &workspace, 1).await;
        assert_eq!(completed.thread_summaries.len(), 2);
        let compact=completed.thread_summaries.iter().find(|patch|patch.thread.id==THREAD).unwrap();
        assert!(compact.preview_truncated);
        assert_eq!(compact.thread.preview.chars().count(),160);
        assert_eq!(core.0.thread_snapshot(THREAD).unwrap().coordinator().thread().unwrap().preview,long_preview);
        let tree = core.0.workspace_tree(&workspace).unwrap();
        assert_eq!(tree.revision(), before_batch + 1);
        assert_eq!(tree.snapshot().threads_by_id[THREAD].preview.chars().count(),160);
        let retained=core.0.thread_snapshot(THREAD).unwrap().coordinator();
        assert_eq!(retained.thread().unwrap().preview,long_preview);
        assert_eq!(retained.thread().unwrap().turns.last().unwrap().id,NEXT_TURN);
        assert_eq!(tree.snapshot().threads_by_id[THREAD].turns.last().unwrap().id, NEXT_TURN);
        assert_eq!(tree.snapshot().threads_by_id[SECOND_THREAD].turns.last().unwrap().id, SECOND_NEXT_TURN);
        let mut observed = 0;
        while let Some(event) = subscription.try_next() {
            let pioneer_client::core::ClientSubscriptionEvent::Publication { publication, .. } = event else {
                panic!("unexpected resnapshot during bounded batch");
            };
            let publication = publication.typed::<pioneer_client::workspaces::directory::ThreadTreePublication>().unwrap();
            let payload = publication.payload();
            let state = payload.snapshot();
            assert_eq!(state.threads_by_id[THREAD].turns.last().unwrap().id, NEXT_TURN);
            assert_eq!(state.threads_by_id[SECOND_THREAD].turns.last().unwrap().id, SECOND_NEXT_TURN);
            for id in [THREAD, SECOND_THREAD] {
                assert_eq!(state.unread.iter().find(|row| row.thread_id == id).unwrap().unread_count, 1);
            }
            observed += 1;
        }
        assert_eq!(observed, 1, "metadata and indicators publish atomically per accepted packet");
        assert_eq!(tree_requests.load(Ordering::SeqCst), baseline_requests);

        let client=core.0.clone();let session_storage=storage.clone();let session_endpoint=endpoint.clone();
        let physical=core.0.authorization_connection_generation();
        let permissions=core.0.authorization_permissions_epoch();

        stage.set("same-session renewal snapshot");
        let renewed=tokio::task::spawn_blocking(move || client.ensure_gateway_session(GatewaySessionRefreshRequest {
            endpoint:&session_endpoint,installation_id:"gateway-test-superuser",client_kind:pioneer_protocol::ClientKind::Desktop,
            now_unix:now+90,timeout:Duration::from_secs(5),ws_timings:GatewayWsTimings::from_millis(5000,60000,5000,1000,1000,0).unwrap(),retry_delays:&[],
        },session_storage.as_ref()).unwrap()).await.unwrap();
        assert_eq!(renewed.connection_id,connected.connection_id);
        assert_eq!(core.0.authorization_connection_generation(),physical);
        assert_eq!(core.0.authorization_permissions_epoch(),permissions);

        assert_eq!(renewals.load(Ordering::SeqCst),1);
        assert_eq!(domain_connections.load(Ordering::SeqCst),1);
        let renewed_snapshot=packets_rx.recv().await.expect("renewal re-registration snapshot");
        assert!(renewed_snapshot.snapshot && !renewed_snapshot.resync_required);
        assert_ne!(renewed_snapshot.epoch,completed.epoch);
        stage.set("post-renewal completion ingress");
        let renewal_hold=processor.begin_push_mutation(observer,&workspace,true).unwrap();
        send_epic6_test_message(&processor,source,&mut source_rx,THREAD,RENEWED_TURN,"renewed user question").await;
        let post=pioneer_protocol::TurnItem::AgentMessage {
            id:pioneer_protocol::task_delivery_result_item_id("post-renewal"),text:"post renewal answer".into(),
            phase:pioneer_protocol::AgentMessagePhase::FinalAnswer,markdown:None,markdown_version:None,
        };
        processor.crud_store.materialize_item_completed(pioneer_protocol::ItemCompletedNotification {
            workspace_id:workspace.clone(),thread_id:THREAD.into(),turn_id:RENEWED_TURN.into(),item:post.clone(),
        },now_timestamp_secs()).await.unwrap();
        processor.notify_semantic_timeline_item_changed(&workspace,THREAD,RENEWED_TURN,&post,Some("completed")).await;
        renewal_hold.finish(None,None);
        let completed=packets_rx.recv().await.expect("post renewal completion");
        assert!(!completed.snapshot && !completed.resync_required);
        assert_eq!(completed.epoch,renewed_snapshot.epoch);
        assert_eq!(completed.revision,renewed_snapshot.revision+1);
        // Renewal and sending another user message do not read the earlier
        // answer. Both completed answers remain unread until the exact read.
        assert_eq!(completed.indicators.iter().find(|row|row.thread_id==THREAD).unwrap().unread_count,Some(2));
        let mut applied=core.0.watch_publications();
        loop {
            if core.0.workspace_tree(&workspace).unwrap().snapshot().threads_by_id[THREAD]
                .turns.last().is_some_and(|turn| turn.id==RENEWED_TURN) {break;}
            applied.changed().await.expect("post renewal batch ingress");
        }
        await_unread(&core.0,&workspace,2).await;

        // Use the same live client's read RPC. No test code applies its ack to
        // the directory, so the next zero must arrive through production ingress.
        let client = core.0.clone();
        stage.set("exact viewed answer read");
        let ack = tokio::task::spawn_blocking(move || {
            let sender = client.transport_runtime().ws_command_sender();
            let timeline = sender.thread_timeline_page(pioneer_protocol::ThreadTimelinePageParams {
                thread_id: THREAD.into(), anchor: pioneer_protocol::TimelinePageAnchor::Newest, limit: Some(12),
            }).unwrap();
            let answer_id = pioneer_protocol::task_delivery_result_item_id("post-renewal");
            let block = timeline.blocks.iter().find(|block| matches!(&block.kind,
                pioneer_protocol::TimelineBlockKind::AssistantMessage { item_id, .. } if item_id == &answer_id))
                .expect("viewed post-renewal answer");
            sender.thread_read(pioneer_protocol::ThreadReadParams {
                thread_id: THREAD.into(), through_turn_id: RENEWED_TURN.into(),
                through_block: Some(pioneer_protocol::ThreadReadPosition {block_id:block.block_id.clone(),sort_key:block.sort_key.clone()}),
            }).unwrap()
        }).await.unwrap();
        assert_eq!(ack.unread_count, 0);
        let read = packets_rx.recv().await.expect("committed read push");
        assert!(!read.snapshot && !read.resync_required);
        assert_eq!(read.epoch, completed.epoch);
        assert_eq!(read.revision, completed.revision + 1);
        await_unread(&core.0, &workspace, 0).await;
        assert_eq!(tree_requests.load(Ordering::SeqCst), baseline_requests);

        cancel.cancel();
        socket_owner.join_next().await.unwrap().unwrap();
        core.0.shutdown();
        drop(subscription);
        sessions.unregister_connection(source).await;
        assert_eq!(tree_requests.load(Ordering::SeqCst), baseline_requests);
    }).await.unwrap_or_else(|_| panic!("bounded committed-push integration at {}", stage.get()));
}
