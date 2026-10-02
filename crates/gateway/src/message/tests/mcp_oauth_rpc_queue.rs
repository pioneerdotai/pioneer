//! Regression through the production, sequential WebSocket reader, not two
//! concurrent calls to McpOAuthService. Written for post-review execution only.
use super::*;
use axum::{
    Json, Router,
    extract::ws::WebSocketUpgrade,
    routing::{get, post},
};
use futures_util::SinkExt;
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message as ClientMessage};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_acceptance_frees_same_websocket_for_cancel_during_exchange() {
    oauth_queue_cancellation(false, None, None, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparing_is_delivered_and_details_does_not_block_cancel_on_same_websocket() {
    oauth_queue_cancellation(true, None, None, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authorized_event_cannot_restart_reinstalled_or_replaced_identity_after_notification_await()
{
    for state in [
        pioneer_mcp_oauth::OAuthState::Authorized,
        pioneer_mcp_oauth::OAuthState::AuthRequired,
        pioneer_mcp_oauth::OAuthState::Recovered,
    ] {
        for new_uuid in [true, false] {
            oauth_queue_cancellation(false, Some((new_uuid, state)), None, None).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_websocket_terminal_decision_after_durable_put_preserves_cancel_timeout_and_success() {
    use pioneer_mcp_oauth::OAuthState;
    for decision in [
        OAuthState::Cancelled,
        OAuthState::TimedOut,
        OAuthState::Authorized,
    ] {
        oauth_queue_cancellation(false, None, Some(decision), None).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn other_installation_blocking_read_cannot_delay_same_websocket_details_and_cancel() {
    oauth_queue_cancellation_impl(false, None, None, None, true, None).await;
}

async fn oauth_queue_cancellation(
    preparing: bool,
    replacement: Option<(bool, pioneer_mcp_oauth::OAuthState)>,
    commit: Option<pioneer_mcp_oauth::OAuthState>,
    stale_rpc: Option<(&'static str, bool)>,
) {
    oauth_queue_cancellation_impl(preparing, replacement, commit, stale_rpc, false, None).await;
}
async fn oauth_queue_cancellation_impl(
    preparing: bool,
    replacement: Option<(bool, pioneer_mcp_oauth::OAuthState)>,
    commit: Option<pioneer_mcp_oauth::OAuthState>,
    stale_rpc: Option<(&'static str, bool)>,
    blocked_bind: bool,
    cleanup_failure: Option<usize>,
) {
    let token_entered = Arc::new(tokio::sync::Notify::new());
    let token_release = Arc::new(tokio::sync::Notify::new());
    let oauth_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}", oauth_listener.local_addr().unwrap());
    let metadata = json!({"issuer":issuer,"authorization_endpoint":format!("{issuer}/authorize"),"token_endpoint":format!("{issuer}/token"),"registration_endpoint":format!("{issuer}/register"),"response_types_supported":["code"],"code_challenge_methods_supported":["S256"],"authorization_response_iss_parameter_supported":true});
    let entered = token_entered.clone();
    let release = token_release.clone();
    let registration_entered = token_entered.clone();
    let registration_release = token_release.clone();
    let oauth_router = Router::new()
        .route("/.well-known/oauth-authorization-server", get(move || { let metadata = metadata.clone(); async move { Json(metadata) } }))
        .route("/register", post(move |Json(body): Json<Value>| { let entered = registration_entered.clone(); let release = registration_release.clone(); async move {
            if preparing { entered.notify_one(); release.notified().await; }
            Json(json!({"client_id":"queue-client","redirect_uris":body["redirect_uris"]}))
        } }))
        .route("/token", post(move || { let entered = entered.clone(); let release = release.clone(); async move {
            entered.notify_one(); release.notified().await;
            Json(json!({"access_token":"queue-access-canary","refresh_token":"queue-refresh-canary","token_type":"Bearer","expires_in":3600}))
        }}));
    let oauth_server = tokio::spawn(async move {
        axum::serve(oauth_listener, oauth_router).await.unwrap();
    });

    let (workspace_manager, crud_store, workspace_id) = setup_workspace_manager().await;
    let sessions = Arc::new(SessionManager::new());
    let secret_store = Arc::new(MemorySecretStore::new());
    let other_read = Arc::new(OtherInstallationRead {
        inner: secret_store.clone(),
        armed: Default::default(),
        cleanup_failure: Default::default(),
        uncertain_promotion: Default::default(),
        marker_unreadable: Default::default(),
        promotion_readback_failed: Default::default(),
        target: Default::default(),
        entered: Default::default(),
        released: Default::default(),
        resumed: Default::default(),
    });
    let secrets = Arc::new(GatewaySecrets::new(other_read.clone()));
    let clock = Arc::new(WireCommitClock::new(secret_store));
    let mut processor = MessageProcessor::new(
        Arc::new(ThreadManager::new("o4-mini", "openai")),
        test_provider(),
        sessions.clone(),
        workspace_manager,
        crud_store.clone(),
        secrets.clone(),
        test_summary_config(),
        test_tool_loop_config(),
    );
    if commit.is_some() || replacement.is_some() || stale_rpc.is_some() {
        processor.mcp_service.shutdown().await;
        processor.mcp_service = Arc::new(crate::mcp_service::McpService::new_with_oauth_options(
            crud_store.clone(),
            sessions.clone(),
            secrets.clone(),
            processor.mcp_snapshot_version.clone(),
            processor.authorization_invalidation_hub.clone(),
            processor.execution_leases.clone(),
            pioneer_mcp_oauth::OAuthServiceOptions {
                clock: clock.clone(),
                poll_interval: if replacement.is_some() {
                    Duration::from_millis(10)
                } else {
                    Duration::from_secs(3600)
                },
                ..Default::default()
            },
        ));
    }
    let identity = Arc::new(
        crate::identity::bootstrap_identity(&crud_store.database_connection())
            .await
            .unwrap()
            .snapshot,
    );
    let auth = Arc::new(
        crate::auth::GatewayAuthService::new(
            crud_store.database_connection(),
            pioneer_config::GatewayAuthConfig::default(),
            identity,
            &crate::secrets::AuthKeyMaterial::from_test_bytes(vec![8; 64]),
            &crate::secrets::AuthKeyMaterial::from_test_bytes(vec![9; 64]),
        )
        .unwrap(),
    );
    let processor = Arc::new(processor.with_auth_service(auth.clone()));
    let effect_entered = Arc::new(tokio::sync::Notify::new());
    let effect_release = Arc::new(tokio::sync::Notify::new());
    let effect_completed = Arc::new(tokio::sync::Notify::new());
    if replacement.is_some() || stale_rpc.is_some() {
        seed_ready_fake_mcp_server(&processor, &crud_store, &workspace_id).await;
        if replacement.is_some() {
            *processor
                .mcp_service
                .inner
                .oauth_effect_barrier
                .lock()
                .unwrap() = Some((
                effect_entered.clone(),
                effect_release.clone(),
                effect_completed.clone(),
            ));
        }
    }
    let _release_on_exit = QueueFixtureRelease {
        clock: clock.clone(),
        notifications: vec![token_release.clone(), effect_release.clone()],
        other_read: other_read.clone(),
    };
    let (stop, stop_rx) = tokio::sync::watch::channel(false);
    let ingress_processor = processor.clone();
    let router = Router::new().route(
        "/ws",
        get(move |upgrade: WebSocketUpgrade| {
            let processor = ingress_processor.clone();
            let sessions = sessions.clone();
            let auth = auth.clone();
            let stop_rx = stop_rx.clone();
            async move {
                upgrade.on_upgrade(move |socket| async move {
                    let mut principal = (*authenticated_test_superuser()).clone();
                    principal.access_expires_at_unix = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                        + 3600;
                    crate::transport::run_normal_connection(
                        socket,
                        toml::from_str(include_str!(concat!(
                            env!("CARGO_MANIFEST_DIR"),
                            "/../../config/default.toml"
                        )))
                        .unwrap(),
                        Arc::new(principal),
                        auth,
                        processor,
                        sessions,
                        stop_rx,
                    )
                    .await
                    .unwrap();
                })
            }
        }),
    );
    let ws_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_address = ws_listener.local_addr().unwrap();
    let ws_server = tokio::spawn(async move {
        axum::serve(ws_listener, router).await.unwrap();
    });
    let (mut socket, _) = connect_async(format!("ws://{ws_address}/ws"))
        .await
        .unwrap();
    socket.send(ClientMessage::Text(json!({"jsonrpc":"2.0","id":"oauth-install________","method":"mcp/install","params":{"workspace_id":workspace_id,"enabled":replacement.is_some() || stale_rpc.is_some(),"config_json":json!({"mcpServers":{"queue":{"url":format!("{issuer}/mcp")}}}).to_string()}}).to_string().into())).await.unwrap();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let ClientMessage::Text(text) = frame {
            let value: Value = serde_json::from_str(&text).unwrap();
            if value["id"] == "oauth-install________" {
                assert!(value.get("error").is_none(), "install rejected");
                break;
            }
        }
    }
    let row = crud_store
        .find_mcp_server_installation("workspace", &workspace_id, "queue")
        .await
        .unwrap()
        .unwrap();
    let server_id = row.id.unwrap();
    *clock.id.lock().unwrap() = Some(server_id.clone());
    if replacement.is_some() || stale_rpc.is_some() {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if processor
                    .mcp_service
                    .runtime_snapshot("workspace", &workspace_id)
                    .await
                    .get(&server_id)
                    .is_some_and(|snapshot| snapshot.state == pioneer_mcp::McpRuntimeState::Ready)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("enabled queue must be Ready before the targeted stale-event barrier");
    } else {
        // Install responds before its runtime reload finishes. Observe the
        // disabled projection without assuming that the snapshot exists yet.
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if processor
                    .mcp_service
                    .runtime_snapshot("workspace", &workspace_id)
                    .await
                    .get(&server_id)
                    .is_some_and(|snapshot| {
                        snapshot.state == pioneer_mcp::McpRuntimeState::Disabled
                    })
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("disabled installation must finish runtime reconciliation");
        assert_eq!(
            processor
                .mcp_service
                .runtime_snapshot("workspace", &workspace_id)
                .await[&server_id]
                .state,
            pioneer_mcp::McpRuntimeState::Disabled
        );
        assert_eq!(
            processor
                .mcp_service
                .runtime_snapshot("workspace", &workspace_id)
                .await[&server_id]
                .runtime_generation,
            0
        );
    }

    let envelope = |id: &str, action: Value| {
        let id = pioneer_protocol::RequestId::new(id).expect("valid wire request ID");
        json!({"jsonrpc":"2.0","id":id,"method":"mcp/oauth","params":{"workspace_id":workspace_id,"server_id":server_id,"name":"queue","action":action}}).to_string()
    };
    socket.send(ClientMessage::Text(envelope("oauth-signin_________", json!({"kind":"sign_in","redirect_uri":"http://127.0.0.1:37643/oauth/mcp/callback"})).into())).await.unwrap();
    let mut sign_in_accepted = false;
    let mut browser = None;
    while !sign_in_accepted || browser.is_none() {
        let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let ClientMessage::Text(text) = frame {
            let value: Value = serde_json::from_str(&text).unwrap();
            if value["id"] == "oauth-signin_________" {
                assert_eq!(value["result"]["accepted"], true);
                sign_in_accepted = true;
            }
            if value["params"]["state"]
                == if preparing {
                    "preparing"
                } else {
                    "awaiting_callback"
                }
            {
                browser = Some(value["params"].clone());
            }
        }
    }
    let browser = browser.unwrap();
    let flow = browser["flow_id"].as_str().unwrap();
    if let Some((action, new_uuid)) = stale_rpc {
        stale_rpc_replacement(
            &processor,
            &crud_store,
            &workspace_id,
            &server_id,
            &issuer,
            ws_address,
            &mut socket,
            &browser,
            action,
            new_uuid,
            token_release.clone(),
            secrets.mcp_oauth_persistence(),
        )
        .await;
        processor.mcp_service.shutdown().await;
        let _ = stop.send(true);
        socket.close(None).await.unwrap();
        ws_server.abort();
        oauth_server.abort();
        return;
    }

    if cleanup_failure == Some(4) {
        // Socket B retains Denied. A then owns a different current consent.
        let url = url::Url::parse(browser["authorization_url"].as_str().unwrap()).unwrap();
        let state = url
            .query_pairs()
            .find(|(k, _)| k == "state")
            .unwrap()
            .1
            .into_owned();
        socket.send(ClientMessage::Text(envelope("deny-B_______________", json!({"kind":"callback","flow_id":flow,"state":state,"issuer":issuer,"error":"access_denied"})).into())).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if value["params"]["state"] == "denied" {
                        assert_eq!(value["params"]["flow_id"], flow);
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        let (mut socket_a, _) = connect_async(format!("ws://{ws_address}/ws"))
            .await
            .unwrap();
        socket_a.send(ClientMessage::Text(envelope("signin-A_____________", json!({"kind":"sign_in","redirect_uri":"http://127.0.0.1:37643/oauth/mcp/callback"})).into())).await.unwrap();
        let flow_a = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let ClientMessage::Text(text) = socket_a.next().await.unwrap().unwrap() {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if value["params"]["state"] == "awaiting_callback" {
                        break value["params"]["flow_id"].clone();
                    }
                }
            }
        })
        .await
        .unwrap();
        assert_ne!(flow_a, flow);
        *other_read.target.lock().unwrap() = Some(server_id.clone());
        other_read
            .cleanup_failure
            .store(1, std::sync::atomic::Ordering::SeqCst);
        socket
            .send(ClientMessage::Text(
                envelope("clear-B______________", json!({"kind":"disconnect"})).into(),
            ))
            .await
            .unwrap();
        let cleanup = tokio::time::timeout(Duration::from_secs(3), async {
            let mut error_seen = false;
            let mut event = None;
            while !error_seen || event.is_none() {
                if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if value["id"] == "clear-B______________" {
                        assert!(value.get("error").is_some());
                        error_seen = true;
                    }
                    if value["params"]["state"] == "cleanup_required" {
                        assert_ne!(value["params"]["flow_id"], flow);
                        assert_ne!(value["params"]["flow_id"], flow_a);
                        assert_eq!(value["params"]["authorization_url"], Value::Null);
                        event = Some(value["params"]["flow_id"].clone());
                    }
                }
            }
            event.unwrap()
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let ClientMessage::Text(text) = socket_a.next().await.unwrap().unwrap() {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    assert_ne!(
                        value["params"]["state"], "cleanup_required",
                        "B's presentation is addressed to B"
                    );
                    if value["params"]["state"] == "retired" && value["params"]["flow_id"] == flow_a
                    {
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        socket.send(ClientMessage::Text(json!({"jsonrpc":"2.0","id":"details-B____________","method":pioneer_protocol::constants::methods::MCP_SERVER_DETAILS,"params":{"workspace_id":workspace_id,"server_id":server_id}}).to_string().into())).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if value["id"] == "details-B____________" {
                        let management: pioneer_protocol::McpOAuthState = serde_json::from_value(
                            value["result"]["management"]["oauth_state"].clone(),
                        )
                        .unwrap();
                        assert_eq!(
                            pioneer_client::mcp::oauth::effective_oauth_management_state(
                                Some(pioneer_protocol::McpOAuthState::Denied),
                                Some(management),
                                false
                            ),
                            Some(pioneer_protocol::McpOAuthState::CleanupRequired)
                        );
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        other_read
            .cleanup_failure
            .store(0, std::sync::atomic::Ordering::SeqCst);
        socket
            .send(ClientMessage::Text(
                envelope("retry-B______________", json!({"kind":"disconnect"})).into(),
            ))
            .await
            .unwrap();
        let mut acknowledged = false;
        let mut retired = false;
        tokio::time::timeout(Duration::from_secs(3), async {
            while !acknowledged || !retired {
                if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if value["id"] == "retry-B______________" {
                        assert_eq!(value["result"]["accepted"], true);
                        acknowledged = true;
                    }
                    retired |= value["params"]["state"] == "retired"
                        && value["params"]["flow_id"] == cleanup;
                    assert_ne!(value["params"]["state"], "authorized");
                    assert_eq!(value["params"]["authorization_url"], Value::Null);
                }
            }
        })
        .await
        .unwrap();
        let persistence = pioneer_mcp_oauth::OAuthPersistence::new(other_read.clone(), None);
        assert!(persistence.read(&server_id).await.unwrap().is_none());
        processor.mcp_service.shutdown().await;
        let _ = stop.send(true);
        socket_a.close(None).await.unwrap();
        socket.close(None).await.unwrap();
        ws_server.abort();
        oauth_server.abort();
        return;
    }
    if preparing {
        tokio::time::timeout(Duration::from_secs(2), token_entered.notified())
            .await
            .unwrap();
        assert_eq!(browser["authorization_url"], Value::Null);
    } else {
        let url = url::Url::parse(browser["authorization_url"].as_str().unwrap()).unwrap();
        let state = url
            .query_pairs()
            .find(|(key, _)| key == "state")
            .unwrap()
            .1
            .into_owned();
        socket.send(ClientMessage::Text(envelope("oauth-callback_______", json!({"kind":"callback","flow_id":flow,"state":state,"code":"queue-code-canary","issuer":issuer})).into())).await.unwrap();
        // Acceptance must arrive while the actual endpoint is stalled. The old
        // synchronous handler cannot send this response until token_release fires.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if value["id"] == "oauth-callback_______" {
                        assert_eq!(value["result"]["accepted"], true);
                        break;
                    }
                }
            }
            token_entered.notified().await;
        })
        .await
        .expect("Callback must acknowledge before completing token exchange");
    }
    if let Some(mode) = cleanup_failure {
        use pioneer_keystore::{SecretId, SecretStore};
        *other_read.target.lock().unwrap() = Some(server_id.clone());
        other_read
            .uncertain_promotion
            .store(true, std::sync::atomic::Ordering::SeqCst);
        token_release.notify_one();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if value["params"]["state"] == "resolving" {
                        break;
                    }
                    assert_ne!(value["params"]["state"], "authorized");
                }
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(3),
            other_read.promotion_readback_failed.notified(),
        )
        .await
        .expect("actual promotion deletion and failed readback prerequisite");
        assert!(
            other_read
                .inner
                .exists(&SecretId::mcp_oauth(&server_id).unwrap())
                .unwrap()
        );
        assert!(
            other_read
                .inner
                .exists(&SecretId::mcp_oauth(&format!("{server_id}::promotion")).unwrap())
                .unwrap()
        );

        other_read
            .cleanup_failure
            .store(mode, std::sync::atomic::Ordering::SeqCst);
        socket
            .send(ClientMessage::Text(
                envelope("clear-failed_________", json!({"kind":"disconnect"})).into(),
            ))
            .await
            .unwrap();
        let mut retired = false;
        let mut failed = false;
        let mut cleanup = None;
        tokio::time::timeout(Duration::from_secs(3), async {
            while !retired || !failed || cleanup.is_none() {
                if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    retired |=
                        value["params"]["state"] == "retired" && value["params"]["flow_id"] == flow;
                    if value["id"] == "clear-failed_________" {
                        assert!(value.get("error").is_some());
                        failed = true;
                    }
                    if value["params"]["state"] == "cleanup_required" {
                        assert_ne!(value["params"]["flow_id"], flow);
                        assert_eq!(value["params"]["authorization_url"], Value::Null);
                        cleanup = Some(value["params"]["flow_id"].clone());
                    }
                    assert_ne!(value["params"]["state"], "authorized");
                }
            }
        })
        .await
        .unwrap();
        other_read
            .uncertain_promotion
            .store(false, std::sync::atomic::Ordering::SeqCst);
        socket.send(ClientMessage::Text(json!({"jsonrpc":"2.0","id":"cleanup-details______","method":pioneer_protocol::constants::methods::MCP_SERVER_DETAILS,"params":{"workspace_id":workspace_id,"server_id":server_id,"name":"queue"}}).to_string().into())).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if value["id"] == "cleanup-details______" {
                        assert_eq!(
                            value["result"]["management"]["oauth_state"],
                            "cleanup_required"
                        );
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            processor.mcp_service.oauth().state(&server_id).await,
            Some(pioneer_mcp_oauth::OAuthState::CleanupRequired)
        );
        let persistence = pioneer_mcp_oauth::OAuthPersistence::new(other_read.clone(), None);
        let unreadable = persistence.read(&server_id).await;
        assert!(
            unreadable.is_err() || matches!(unreadable, Ok(None)),
            "candidate is never usable while fence outcome is unknown"
        );
        if mode == 1 {
            assert!(
                other_read
                    .inner
                    .exists(&SecretId::mcp_oauth(&server_id).unwrap())
                    .unwrap()
            );
        }
        other_read
            .cleanup_failure
            .store(0, std::sync::atomic::Ordering::SeqCst);
        other_read
            .marker_unreadable
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let visible = persistence.read(&server_id).await.unwrap();
        assert!(
            visible
                .as_ref()
                .and_then(|record| record.credentials.as_ref())
                .is_none(),
            "restored storage cannot expose the retired candidate before retry Clear"
        );
        socket
            .send(ClientMessage::Text(
                envelope("clear-retry__________", json!({"kind":"disconnect"})).into(),
            ))
            .await
            .unwrap();
        let mut cleared = false;
        let mut cleanup_retired = false;
        tokio::time::timeout(Duration::from_secs(3), async {
            while !cleared || !cleanup_retired {
                if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if value["id"] == "clear-retry__________" {
                        assert_eq!(value["result"]["accepted"], true);
                        cleared = true;
                    }
                    cleanup_retired |= value["params"]["state"] == "retired"
                        && Some(&value["params"]["flow_id"]) == cleanup.as_ref();
                    assert_ne!(value["params"]["state"], "authorized");
                    assert_eq!(value["params"]["authorization_url"], Value::Null);
                }
            }
        })
        .await
        .unwrap();
        assert!(
            !other_read
                .inner
                .exists(&SecretId::mcp_oauth(&server_id).unwrap())
                .unwrap()
        );
        assert!(
            !other_read
                .inner
                .exists(&SecretId::mcp_oauth(&format!("{server_id}::promotion")).unwrap())
                .unwrap()
        );
        assert!(persistence.read(&server_id).await.unwrap().is_none());
        assert_ne!(
            processor.mcp_service.oauth().state(&server_id).await,
            Some(pioneer_mcp_oauth::OAuthState::CleanupRequired)
        );
        assert_eq!(
            processor
                .mcp_service
                .runtime_snapshot("workspace", &workspace_id)
                .await[&server_id]
                .runtime_generation,
            0,
            "disabled clear/retry must not start a runtime"
        );
        processor.mcp_service.shutdown().await;
        let _ = stop.send(true);
        socket.close(None).await.unwrap();
        ws_server.abort();
        oauth_server.abort();
        return;
    }
    let other_bind = if blocked_bind {
        let mut other = crud_store
            .find_mcp_server_installation("workspace", &workspace_id, "queue")
            .await
            .unwrap()
            .unwrap();
        other.id = None;
        other.name = "other".into();
        crud_store
            .upsert_mcp_server_installation(&other, crate::message::now_timestamp_secs())
            .await
            .unwrap();
        let other = crud_store
            .find_mcp_server_installation("workspace", &workspace_id, "other")
            .await
            .unwrap()
            .unwrap();
        let other_id = other.id.clone().unwrap();
        *other_read.target.lock().unwrap() = Some(other_id.clone());
        let installation = crate::mcp_service::installation_from_record(&other).unwrap();
        other_read
            .armed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let owner = processor.mcp_service.oauth().clone();
        let task = tokio::spawn(async move { owner.synchronize(&other_id, &installation).await });
        tokio::time::timeout(Duration::from_secs(3), other_read.entered.notified())
            .await
            .unwrap();
        Some(task)
    } else {
        None
    };
    // Production Details precedes Cancel in this connection's sequential reader.
    // Neither the observable projection nor event validation may wait for the actor.
    socket.send(ClientMessage::Text(json!({"jsonrpc":"2.0","id":"oauth-details________","method":pioneer_protocol::constants::methods::MCP_SERVER_DETAILS,"params":{"workspace_id":workspace_id,"server_id":server_id,"name":"queue"}}).to_string().into())).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                let value: Value = serde_json::from_str(&text).unwrap();
                if value["id"] == "oauth-details________" {
                    assert!(
                        value.get("error").is_none(),
                        "Details must succeed before provider release"
                    );
                    break;
                }
            }
        }
    })
    .await
    .expect("Details must not obstruct Cancel on the same WebSocket");
    if let Some(decision) = commit {
        use pioneer_mcp_oauth::OAuthState;
        let previous = serde_json::to_value(
            secrets
                .mcp_oauth_persistence()
                .read(&server_id)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        clock.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        token_release.notify_one();
        tokio::time::timeout(Duration::from_secs(3), clock.entered.notified())
            .await
            .unwrap();
        assert!(
            secrets
                .mcp_oauth_persistence()
                .read(&server_id)
                .await
                .unwrap()
                .unwrap()
                .pending_consent
                .is_some()
        );
        if decision == OAuthState::Cancelled {
            socket
                .send(ClientMessage::Text(
                    envelope(
                        "commit-cancel________",
                        json!({"kind":"cancel","flow_id":flow}),
                    )
                    .into(),
                ))
                .await
                .unwrap();
            await_oauth_wire_response(&mut socket, "commit-cancel________").await;
        } else if decision == OAuthState::TimedOut {
            *clock.now.lock().unwrap() += Duration::from_secs(4000);
        }
        clock.resume();
        let wire_state = match decision {
            OAuthState::Cancelled => "cancelled",
            OAuthState::TimedOut => "timed_out",
            _ => "authorized",
        };
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if value["params"]["state"] == wire_state {
                        break;
                    }
                    if decision != OAuthState::Authorized {
                        assert_ne!(value["params"]["state"], "authorized");
                    }
                }
            }
        })
        .await
        .unwrap();
        let persisted = serde_json::to_value(
            secrets
                .mcp_oauth_persistence()
                .read(&server_id)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        if decision != OAuthState::Authorized {
            assert_eq!(persisted, previous);
        } else {
            assert_ne!(persisted, previous);
            socket
                .send(ClientMessage::Text(
                    envelope(
                        "late-cancel__________",
                        json!({"kind":"cancel","flow_id":flow}),
                    )
                    .into(),
                ))
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                        let value: Value = serde_json::from_str(&text).unwrap();
                        if value["id"] == "late-cancel__________" {
                            assert!(value.get("error").is_some());
                            break;
                        }
                    }
                }
            })
            .await
            .unwrap();
        }
        let disabled = processor
            .mcp_service
            .runtime_snapshot("workspace", &workspace_id)
            .await[&server_id]
            .clone();
        assert_eq!(disabled.state, pioneer_mcp::McpRuntimeState::Disabled);
        assert_eq!(disabled.runtime_generation, 0);
        processor.mcp_service.shutdown().await;
        let _ = stop.send(true);
        socket.close(None).await.unwrap();
        ws_server.abort();
        oauth_server.abort();
        return;
    }
    if let Some((new_uuid, state)) = replacement {
        // Each real service event is paused in the production consumer after
        // notification/rights validation, before admission of its runtime effect.
        token_release.notify_one();
        if state != pioneer_mcp_oauth::OAuthState::Authorized {
            tokio::time::timeout(Duration::from_secs(3), effect_entered.notified())
                .await
                .unwrap();
            effect_release.notify_one();
            tokio::time::timeout(Duration::from_secs(3), effect_completed.notified())
                .await
                .unwrap();
            let installation = crate::mcp_service::installation_from_record(
                &crud_store
                    .find_mcp_server_installation("workspace", &workspace_id, "queue")
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if processor.mcp_service.oauth().state(&server_id).await
                        == Some(pioneer_mcp_oauth::OAuthState::Authorized)
                        && processor
                            .mcp_service
                            .runtime_snapshot("workspace", &workspace_id)
                            .await
                            .get(&server_id)
                            .is_some_and(|snapshot| {
                                snapshot.state == pioneer_mcp::McpRuntimeState::Ready
                            })
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            use pioneer_mcp::McpOAuthProvider;
            if state == pioneer_mcp_oauth::OAuthState::AuthRequired {
                processor
                    .mcp_service
                    .oauth()
                    .authorization_lost(&server_id)
                    .await;
            } else {
                let client = processor
                    .mcp_service
                    .oauth()
                    .client(&server_id, &installation)
                    .await
                    .unwrap()
                    .unwrap();
                processor
                    .mcp_service
                    .oauth()
                    .transient_failure_from_session(&server_id, &installation, Some(&client))
                    .await;
                tokio::time::timeout(Duration::from_secs(3), token_entered.notified())
                    .await
                    .unwrap();
                token_release.notify_one();
            }
        }
        tokio::time::timeout(Duration::from_secs(3), effect_entered.notified())
            .await
            .unwrap();
        if new_uuid {
            socket.send(ClientMessage::Text(json!({"jsonrpc":"2.0","id":"replace-uninstall____","method":"mcp/uninstall","params":{"workspace_id":workspace_id,"name":"queue"}}).to_string().into())).await.unwrap();
            await_oauth_wire_response(&mut socket, "replace-uninstall____").await;
        }
        let resource = if new_uuid {
            format!("{issuer}/mcp")
        } else {
            format!("{issuer}/other-resource")
        };
        socket.send(ClientMessage::Text(json!({"jsonrpc":"2.0","id":"replace-install______","method":"mcp/install","params":{"workspace_id":workspace_id,"config_json":json!({"mcpServers":{"queue":{"url":resource}}}).to_string()}}).to_string().into())).await.unwrap();
        await_oauth_wire_response(&mut socket, "replace-install______").await;
        let current = crud_store
            .find_mcp_server_installation("workspace", &workspace_id, "queue")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.id.as_deref() != Some(&server_id), new_uuid);
        let current_id = current.id.as_deref().unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if processor
                    .mcp_service
                    .runtime_snapshot("workspace", &workspace_id)
                    .await
                    .get(current_id)
                    .is_some_and(|snapshot| snapshot.state == pioneer_mcp::McpRuntimeState::Ready)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let before = processor
            .mcp_service
            .runtime_snapshot("workspace", &workspace_id)
            .await[current_id]
            .clone();
        effect_release.notify_one();
        tokio::time::timeout(Duration::from_secs(3), effect_completed.notified())
            .await
            .unwrap();
        let after = processor
            .mcp_service
            .runtime_snapshot("workspace", &workspace_id)
            .await[current_id]
            .clone();
        assert_eq!(after.runtime_generation, before.runtime_generation);
        assert_eq!(after.state, before.state);
        assert_eq!(after.status_reason, before.status_reason);
        processor.mcp_service.shutdown().await;
        let _ = stop.send(true);
        socket.close(None).await.unwrap();
        ws_server.abort();
        oauth_server.abort();
        return;
    }
    socket
        .send(ClientMessage::Text(
            envelope(
                "oauth-cancel_________",
                json!({"kind":"cancel","flow_id":flow}),
            )
            .into(),
        ))
        .await
        .unwrap();
    let callback_accepted = true;
    let mut cancel_accepted = false;
    let mut cancelled = false;
    tokio::time::timeout(Duration::from_secs(2), async {
        while !callback_accepted || !cancel_accepted || !cancelled {
            if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                let value: Value = serde_json::from_str(&text).unwrap();
                if value["id"] == "oauth-callback_______" {
                    assert_eq!(value["result"]["accepted"], true);
                    assert!(callback_accepted);
                }
                if value["id"] == "oauth-cancel_________" {
                    assert_eq!(value["result"]["accepted"], true);
                    cancel_accepted = true;
                }
                if value["params"]["state"] == "cancelled" {
                    cancelled = true;
                }
                assert_ne!(value["params"]["state"], "authorized");
            }
        }
    })
    .await
    .expect("Cancel must be processed while the token endpoint is stalled");
    if let Some(other_bind) = other_bind {
        // The controlled physical read is still blocked after both wire replies
        // and the terminal cancellation notification have been delivered.
        assert!(!other_bind.is_finished());
        other_read.resume();
        other_bind.await.unwrap().unwrap();
    }
    token_release.notify_one();
    processor.mcp_service.oauth().shutdown().await;
    let record = secrets
        .mcp_oauth_persistence()
        .read(&server_id)
        .await
        .unwrap();
    if preparing {
        assert!(record.is_none());
    } else {
        assert!(record.unwrap().credentials.is_none());
    }
    let _ = stop.send(true);
    socket.close(None).await.unwrap();
    ws_server.abort();
    oauth_server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauth_scoped_restart_keeps_catalog_and_audit_on_maintenance_writer() {
    for phase in ["live", "startup", "backoff", "auth_required", "recovery"] {
        oauth_scope_phase(phase).await;
    }
}

async fn oauth_scope_phase(phase: &'static str) {
    let observer = Arc::new(NativeSchedulingObserver::default());
    use sea_orm::ConnectOptions;
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("oauth-scope.sqlite").display()
    );
    let mut options = ConnectOptions::new(url.clone());
    options.max_connections(1).sqlx_logging(false);
    let bootstrap_connection = Database::connect(options).await.unwrap();
    let (_, _, workspace_id) =
        setup_workspace_manager_with_connection(bootstrap_connection.clone()).await;
    bootstrap_connection
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut writer_options = ConnectOptions::new(url.clone());
    writer_options.max_connections(1).sqlx_logging(false);
    let writer = Database::connect(writer_options).await.unwrap();
    let mut reader_options = ConnectOptions::new(url);
    reader_options
        .max_connections(2)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
    let reader = Database::connect(reader_options).await.unwrap();
    let database = pioneer_sqlite::SqliteDatabase::from_executor_with_read_observer(
        reader,
        pioneer_sqlite::SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let workspaces = Arc::new(WorkspaceManager::new(database.clone()));
    let store = Arc::new(CrudStore::new(database));
    ensure_test_superuser_execution_authority(&store).await;
    let sessions = Arc::new(SessionManager::new());
    let processor = MessageProcessor::new(
        Arc::new(ThreadManager::new("o4-mini", "openai")),
        test_provider(),
        sessions.clone(),
        workspaces,
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    );
    let identity = Arc::new(
        crate::identity::bootstrap_identity(&store.database_connection())
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
    let processor = processor.with_auth_service(auth);
    let (sender, mut recipient) = tokio::sync::mpsc::channel(1024);
    let mut principal = (*authenticated_test_superuser()).clone();
    principal.access_expires_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let connection = sessions
        .register_connection(sender, Arc::new(principal))
        .await
        .unwrap();
    seed_ready_fake_mcp_server(&processor, &store, &workspace_id).await;
    let row = store
        .find_mcp_server_installation("workspace", &workspace_id, "resend")
        .await
        .unwrap()
        .unwrap();
    let id = row.id.as_deref().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if processor
                .mcp_service
                .runtime_snapshot("workspace", &workspace_id)
                .await
                .get(id)
                .is_some_and(|snapshot| snapshot.state == pioneer_mcp::McpRuntimeState::Ready)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        processor
            .mcp_service
            .authorized_management_notification_recipients(vec![connection])
            .await,
        vec![connection]
    );
    // Ready is visible in the snapshot before its authenticated status fanout
    // finishes. Wait for the actual wire notification so the old Interactive
    // actor cannot contribute startup lease reads to the OAuth observation.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let axum::extract::ws::Message::Text(text) = recipient.recv().await.unwrap() {
                let value: Value = serde_json::from_str(&text).unwrap();
                if value["method"] == pioneer_protocol::constants::events::MCP_SERVER_STATUS_CHANGED
                    && value["params"]["server"]["id"] == id
                    && value["params"]["server"]["runtime"]["state"] == "ready"
                {
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    while recipient.try_recv().is_ok() {}
    let scoped_store = Arc::new(store.with_maintenance_access());
    let service = crate::mcp_oauth::maintenance_service(processor.mcp_service.inner.clone());
    assert_eq!(
        scoped_store.database_connection().read_class(),
        pioneer_sqlite::SqliteReadClass::Maintenance
    );
    assert_eq!(
        scoped_store.database_connection().write_class(),
        pioneer_sqlite::SqliteWriteClass::Maintenance
    );
    if phase == "recovery" {
        let change = Arc::new(tokio::sync::Notify::new());
        processor
            .mcp_service
            .set_connector_for_tests(Arc::new(ScopeRefreshConnector(change.clone())));
        processor
            .mcp_service
            .restart_server("workspace", &workspace_id, "resend")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if processor
                    .mcp_service
                    .runtime_snapshot("workspace", &workspace_id)
                    .await[id]
                    .state
                    == pioneer_mcp::McpRuntimeState::Ready
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        change.notify_one();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let frame = recipient.recv().await.unwrap();
                if let axum::extract::ws::Message::Text(text) = frame {
                    let notification: Value = serde_json::from_str(&text).unwrap();
                    if notification["method"]
                        == pioneer_protocol::constants::events::MCP_SERVER_STATUS_CHANGED
                        && notification["params"]["server"]["id"] == id
                        && notification["params"]["server"]["runtime"]["state"] == "degraded"
                    {
                        let snapshot = processor
                            .mcp_service
                            .runtime_snapshot("workspace", &workspace_id)
                            .await[id]
                            .clone();
                        assert_eq!(
                            snapshot.last_error.as_deref(),
                            Some("controlled_refresh_failure")
                        );
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
    }
    if phase != "live" && phase != "recovery" {
        let entered = Arc::new(tokio::sync::Notify::new());
        processor
            .mcp_service
            .set_connector_for_tests(Arc::new(ScopePhaseConnector {
                phase,
                entered: entered.clone(),
            }));
        processor
            .mcp_service
            .restart_server("workspace", &workspace_id, "resend")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        if phase != "startup" {
            let state = if phase == "backoff" {
                pioneer_mcp::McpRuntimeState::Failed
            } else {
                pioneer_mcp::McpRuntimeState::AuthRequired
            };
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if processor
                        .mcp_service
                        .runtime_snapshot("workspace", &workspace_id)
                        .await[id]
                        .state
                        == state
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .unwrap();
            // Receiving the failure notification proves its authenticated lease
            // reads finished before we begin observing the OAuth shutdown path.
            let wire_state = if phase == "backoff" {
                "failed"
            } else {
                "auth_required"
            };
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let frame = recipient.recv().await.unwrap();
                    if let axum::extract::ws::Message::Text(text) = frame {
                        let value: Value = serde_json::from_str(&text).unwrap();
                        if value["params"].to_string().contains(wire_state) {
                            break;
                        }
                    }
                }
            })
            .await
            .unwrap();
        }
        processor
            .mcp_service
            .set_connector_for_tests(Arc::new(FakeMcpRuntimeConnector));
    }
    observer.writes.lock().unwrap().clear();
    observer.reads.lock().unwrap().clear();
    if phase == "recovery" {
        let before = service.runtime_snapshot("workspace", &workspace_id).await[id].clone();
        service.oauth_recovered(id).await;
        let after = service.runtime_snapshot("workspace", &workspace_id).await[id].clone();
        assert_eq!(after.runtime_generation, before.runtime_generation);
        assert_eq!(after.state, pioneer_mcp::McpRuntimeState::Ready);
        assert!(after.last_error.is_none());
        assert!(
            !observer.reads.lock().unwrap().is_empty(),
            "real lease reads must occur during recovery status fanout"
        );
        processor
            .mcp_service
            .set_connector_for_tests(Arc::new(FakeMcpRuntimeConnector));
    }
    service
        .restart_server("workspace", &workspace_id, "resend")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if service
                .runtime_snapshot("workspace", &workspace_id)
                .await
                .get(id)
                .is_some_and(|snapshot| snapshot.state == pioneer_mcp::McpRuntimeState::Ready)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    // The physical reader pool is query_only. Catalog/audit persistence must
    // succeed through the writer, with the Maintenance class retained by the
    // cloned service handle inside the background actor.
    assert!(
        scoped_store
            .find_mcp_server_catalog_snapshot(id)
            .await
            .unwrap()
            .is_some()
    );
    let writes = observer.writes.lock().unwrap().clone();
    let acquired = writes
        .iter()
        .filter_map(|event| match event {
            pioneer_sqlite::SqliteWriteEvent::Acquired { class, .. } => Some(*class),
            _ => None,
        })
        .collect::<Vec<_>>();
    let reads = observer.reads.lock().unwrap().clone();
    let read_classes = reads
        .iter()
        .filter_map(|event| match event {
            pioneer_sqlite::SqliteReadEvent::OperationFinished { class, .. } => Some(*class),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(!read_classes.is_empty());
    assert!(
        read_classes
            .iter()
            .all(|class| *class == pioneer_sqlite::SqliteReadClass::Maintenance)
    );
    assert!(!acquired.is_empty());
    assert!(
        acquired
            .iter()
            .all(|class| *class == pioneer_sqlite::SqliteWriteClass::Maintenance)
    );
    // The foreground source handle was not reclassified by background cloning.
    observer.reads.lock().unwrap().clear();
    assert!(
        store
            .find_mcp_server_installation("workspace", &workspace_id, "resend")
            .await
            .unwrap()
            .is_some()
    );
    assert!(observer.reads.lock().unwrap().iter().any(|event| matches!(
        event,
        pioneer_sqlite::SqliteReadEvent::OperationFinished {
            class: pioneer_sqlite::SqliteReadClass::Interactive,
            ..
        }
    )));
    service.shutdown().await;
}

async fn await_oauth_wire_response(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    id: &str,
) {
    let id = pioneer_protocol::RequestId::new(id).expect("valid wire request ID");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let ClientMessage::Text(text) = socket.next().await.unwrap().unwrap() {
                let value: Value = serde_json::from_str(&text).unwrap();
                if value["id"] == id.as_str() {
                    assert!(value.get("error").is_none(), "production RPC rejected");
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
}

struct ScopePhaseConnector {
    phase: &'static str,
    entered: Arc<tokio::sync::Notify>,
}
#[async_trait::async_trait]
impl pioneer_mcp::McpRuntimeConnector for ScopePhaseConnector {
    async fn connect(
        &self,
        _: pioneer_mcp::McpServerInstallation,
        _: String,
        _: Arc<dyn pioneer_mcp::McpSecretResolver>,
        _: i64,
    ) -> Result<Box<dyn pioneer_mcp::McpRuntimeSession>, pioneer_mcp::McpRuntimeError> {
        self.entered.notify_one();
        if self.phase == "startup" {
            std::future::pending().await
        } else if self.phase == "backoff" {
            Err(pioneer_mcp::McpRuntimeError::failed(
                "controlled retry backoff",
            ))
        } else {
            Err(pioneer_mcp::McpRuntimeError::auth_required(
                "controlled authorization loss",
            ))
        }
    }
}

// Reads the actual completed store record: no barrier inside put or token HTTP.
struct WireCommitClock {
    store: Arc<MemorySecretStore>,
    id: std::sync::Mutex<Option<String>>,
    armed: std::sync::atomic::AtomicBool,
    now: std::sync::Mutex<std::time::SystemTime>,
    entered: tokio::sync::Notify,
    released: std::sync::Mutex<bool>,
    release: std::sync::Condvar,
}
impl WireCommitClock {
    fn new(store: Arc<MemorySecretStore>) -> Self {
        Self {
            store,
            id: std::sync::Mutex::new(None),
            armed: std::sync::atomic::AtomicBool::new(false),
            now: std::sync::Mutex::new(std::time::SystemTime::now()),
            entered: tokio::sync::Notify::new(),
            released: std::sync::Mutex::new(false),
            release: std::sync::Condvar::new(),
        }
    }
    fn resume(&self) {
        *self.released.lock().unwrap() = true;
        self.release.notify_all();
    }
}
impl pioneer_mcp_oauth::OAuthClock for WireCommitClock {
    fn now(&self) -> std::time::SystemTime {
        use pioneer_keystore::SecretStore;
        let stored = self.id.lock().unwrap().as_ref().is_some_and(|id| {
            self.store
                .get_string(&pioneer_keystore::SecretId::mcp_oauth(id).unwrap())
                .unwrap()
                .is_some_and(|record| {
                    serde_json::from_str::<Value>(&record).unwrap()["pending_consent"]["candidate"]
                        .is_object()
                })
        });
        if stored && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.entered.notify_one();
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.release.wait(released).unwrap();
            }
        }
        *self.now.lock().unwrap()
    }
}

struct ScopeRefreshConnector(Arc<tokio::sync::Notify>);
struct ScopeRefreshSession {
    inner: Box<dyn pioneer_mcp::McpRuntimeSession>,
    change: Arc<tokio::sync::Notify>,
}
#[async_trait::async_trait]
impl pioneer_mcp::McpRuntimeConnector for ScopeRefreshConnector {
    async fn connect(
        &self,
        installation: pioneer_mcp::McpServerInstallation,
        id: String,
        resolver: Arc<dyn pioneer_mcp::McpSecretResolver>,
        now: i64,
    ) -> Result<Box<dyn pioneer_mcp::McpRuntimeSession>, pioneer_mcp::McpRuntimeError> {
        let inner = pioneer_mcp::McpRuntimeConnector::connect(
            &FakeMcpRuntimeConnector,
            installation,
            id,
            resolver,
            now,
        )
        .await?;
        Ok(Box::new(ScopeRefreshSession {
            inner,
            change: self.0.clone(),
        }))
    }
}
#[async_trait::async_trait]
impl pioneer_mcp::McpRuntimeSession for ScopeRefreshSession {
    fn initial_catalog(&self) -> &pioneer_mcp::McpCatalogSnapshot {
        self.inner.initial_catalog()
    }
    fn degraded_reason(&self) -> Option<&str> {
        self.inner.degraded_reason()
    }
    async fn wait_for_event(&mut self) -> pioneer_mcp::McpSessionEvent {
        self.change.notified().await;
        pioneer_mcp::McpSessionEvent::CatalogChanged
    }
    async fn refresh_catalog(
        &mut self,
    ) -> Result<pioneer_mcp::McpCatalogSnapshot, pioneer_mcp::McpRuntimeError> {
        Err(pioneer_mcp::McpRuntimeError {
            oauth_failure: None,
            kind: pioneer_mcp::McpRuntimeErrorKind::TransientRefresh,
            state: pioneer_mcp::McpRuntimeState::Failed,
            message: "controlled_refresh_failure".into(),
        })
    }
    async fn call_tool(
        &mut self,
        name: &str,
        args: Value,
        budget: pioneer_mcp::McpInvocationBudget,
        timeout: Duration,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<pioneer_mcp::McpToolCallResult, pioneer_mcp::McpRuntimeError> {
        self.inner
            .call_tool(name, args, budget, timeout, cancellation)
            .await
    }
    async fn shutdown(&mut self) {
        self.inner.shutdown().await;
    }
}

struct QueueFixtureRelease {
    clock: Arc<WireCommitClock>,
    notifications: Vec<Arc<tokio::sync::Notify>>,
    other_read: Arc<OtherInstallationRead>,
}
impl Drop for QueueFixtureRelease {
    fn drop(&mut self) {
        self.clock.resume();
        self.other_read
            .uncertain_promotion
            .store(false, std::sync::atomic::Ordering::SeqCst);
        self.other_read
            .marker_unreadable
            .store(false, std::sync::atomic::Ordering::SeqCst);
        self.other_read
            .cleanup_failure
            .store(0, std::sync::atomic::Ordering::SeqCst);
        self.other_read.resume();
        for release in &self.notifications {
            release.notify_waiters();
            release.notify_one();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_connections_stale_oauth_rpc_cannot_rebind_or_clear_replacement() {
    for action in ["sign_in", "callback", "cancel", "disconnect"] {
        for new_uuid in [false, true] {
            oauth_queue_cancellation(false, None, None, Some((action, new_uuid))).await;
        }
    }
}
type OAuthTestSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
async fn stale_rpc_replacement(
    processor: &Arc<MessageProcessor>,
    store: &Arc<CrudStore>,
    workspace: &str,
    old_id: &str,
    issuer: &str,
    address: std::net::SocketAddr,
    old_socket: &mut OAuthTestSocket,
    old_flow: &Value,
    action: &str,
    new_uuid: bool,
    token_release: Arc<tokio::sync::Notify>,
    persistence: pioneer_mcp_oauth::OAuthPersistence,
) {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    struct Release(Arc<tokio::sync::Notify>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.notify_waiters();
            self.0.notify_one();
        }
    }
    let _cleanup = Release(release.clone());
    *processor
        .mcp_service
        .inner
        .oauth_rpc_read_barrier
        .lock()
        .unwrap() = Some((entered.clone(), release.clone()));
    let url = url::Url::parse(old_flow["authorization_url"].as_str().unwrap()).unwrap();
    let state = url
        .query_pairs()
        .find(|(k, _)| k == "state")
        .unwrap()
        .1
        .into_owned();
    let flow = old_flow["flow_id"].as_str().unwrap();
    let action = match action {
        "sign_in" => {
            json!({"kind":"sign_in","redirect_uri":"http://127.0.0.1:37643/oauth/mcp/callback"})
        }
        "callback" => {
            json!({"kind":"callback","flow_id":flow,"state":state,"code":"stale-code","issuer":issuer})
        }
        "cancel" => json!({"kind":"cancel","flow_id":flow}),
        _ => json!({"kind":"disconnect"}),
    };
    old_socket.send(ClientMessage::Text(json!({"jsonrpc":"2.0","id":"stale-rpc____________","method":"mcp/oauth","params":{"workspace_id":workspace,"server_id":old_id,"name":"queue","action":action}}).to_string().into())).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    processor
        .mcp_service
        .inner
        .oauth_rpc_read_barrier
        .lock()
        .unwrap()
        .take();
    let (mut current_socket, _) = connect_async(format!("ws://{address}/ws")).await.unwrap();
    if new_uuid {
        current_socket.send(ClientMessage::Text(json!({"jsonrpc":"2.0","id":"remove-old___________","method":"mcp/uninstall","params":{"workspace_id":workspace,"name":"queue"}}).to_string().into())).await.unwrap();
        await_oauth_wire_response(&mut current_socket, "remove-old___________").await;
    }
    let resource = if new_uuid {
        format!("{issuer}/mcp")
    } else {
        format!("{issuer}/replacement")
    };
    current_socket.send(ClientMessage::Text(json!({"jsonrpc":"2.0","id":"install-current______","method":"mcp/install","params":{"workspace_id":workspace,"enabled":true,"config_json":json!({"mcpServers":{"queue":{"url":resource}}}).to_string()}}).to_string().into())).await.unwrap();
    await_oauth_wire_response(&mut current_socket, "install-current______").await;
    let current = store
        .find_mcp_server_installation("workspace", workspace, "queue")
        .await
        .unwrap()
        .unwrap();
    let id = current.id.as_deref().unwrap();
    assert_eq!(id != old_id, new_uuid);
    let initial_generation = processor
        .mcp_service
        .runtime_snapshot("workspace", workspace)
        .await[id]
        .runtime_generation;
    current_socket.send(ClientMessage::Text(json!({"jsonrpc":"2.0","id":"current-signin_______","method":"mcp/oauth","params":{"workspace_id":workspace,"server_id":id,"name":"queue","action":{"kind":"sign_in","redirect_uri":"http://127.0.0.1:37643/oauth/mcp/callback"}}}).to_string().into())).await.unwrap();
    let fresh = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let ClientMessage::Text(text) = current_socket.next().await.unwrap().unwrap() {
                let message: Value = serde_json::from_str(&text).unwrap();
                if message["params"]["state"] == "awaiting_callback" {
                    break message["params"].clone();
                }
            }
        }
    })
    .await
    .unwrap();
    let url = url::Url::parse(fresh["authorization_url"].as_str().unwrap()).unwrap();
    let state = url
        .query_pairs()
        .find(|(k, _)| k == "state")
        .unwrap()
        .1
        .into_owned();
    current_socket.send(ClientMessage::Text(json!({"jsonrpc":"2.0","id":"current-callback_____","method":"mcp/oauth","params":{"workspace_id":workspace,"server_id":id,"name":"queue","action":{"kind":"callback","flow_id":fresh["flow_id"],"state":state,"issuer":issuer,"code":"current-code"}}}).to_string().into())).await.unwrap();
    await_oauth_wire_response(&mut current_socket, "current-callback_____").await;
    token_release.notify_one();
    // The fake MCP connector can be Ready before OAuth persistence completes.
    // Callback acceptance and a runtime generation are not durable sign-in.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let ClientMessage::Text(text) = current_socket.next().await.unwrap().unwrap() {
                let notification: Value = serde_json::from_str(&text).unwrap();
                if notification["method"] == pioneer_protocol::constants::events::MCP_OAUTH_CHANGED
                    && notification["params"]["flow_id"] == fresh["flow_id"]
                    && notification["params"]["server_id"] == id
                    && notification["params"]["state"] == "authorized"
                {
                    break;
                }
            }
        }
    })
    .await
    .expect("current consent must durably authorize before releasing the stale RPC");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = processor
                .mcp_service
                .runtime_snapshot("workspace", workspace)
                .await[id]
                .clone();
            if snapshot.state == pioneer_mcp::McpRuntimeState::Ready
                && snapshot.runtime_generation > initial_generation
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let before = processor
        .mcp_service
        .runtime_snapshot("workspace", workspace)
        .await[id]
        .clone();
    let record = persistence.read(id).await.unwrap().unwrap();
    assert!(record.credentials.is_some());
    assert!(record.pending_consent.is_none());
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let ClientMessage::Text(text) = old_socket.next().await.unwrap().unwrap() {
                let message: Value = serde_json::from_str(&text).unwrap();
                if message["id"] == "stale-rpc____________" {
                    assert!(message.get("error").is_some());
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_value(persistence.read(id).await.unwrap().unwrap()).unwrap(),
        serde_json::to_value(record).unwrap()
    );
    assert_eq!(
        processor.mcp_service.oauth().state(id).await,
        Some(pioneer_mcp_oauth::OAuthState::Authorized)
    );
    let after = processor
        .mcp_service
        .runtime_snapshot("workspace", workspace)
        .await[id]
        .clone();
    assert_eq!(after.runtime_generation, before.runtime_generation);
    assert_eq!(after.state, before.state);
    assert_eq!(after.status_reason, before.status_reason);
    if new_uuid {
        assert!(persistence.read(old_id).await.unwrap().is_none());
    }
    current_socket.close(None).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disabled_consent_commits_account_without_starting_runtime() {
    oauth_queue_cancellation(
        false,
        None,
        Some(pioneer_mcp_oauth::OAuthState::Authorized),
        None,
    )
    .await;
}

struct OtherInstallationRead {
    inner: Arc<MemorySecretStore>,
    armed: std::sync::atomic::AtomicBool,
    cleanup_failure: std::sync::atomic::AtomicUsize,
    uncertain_promotion: std::sync::atomic::AtomicBool,
    marker_unreadable: std::sync::atomic::AtomicBool,
    promotion_readback_failed: tokio::sync::Notify,
    target: std::sync::Mutex<Option<String>>,
    entered: tokio::sync::Notify,
    released: std::sync::Mutex<bool>,
    resumed: std::sync::Condvar,
}
impl OtherInstallationRead {
    fn resume(&self) {
        *self.released.lock().unwrap() = true;
        self.resumed.notify_all();
    }
}
impl pioneer_keystore::SecretStore for OtherInstallationRead {
    fn get_string(
        &self,
        id: &pioneer_keystore::SecretId,
    ) -> pioneer_keystore::Result<Option<String>> {
        if id.user().ends_with("::promotion")
            && self
                .marker_unreadable
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            // Notification proves execution of the physical deletion/readback
            // fault, not merely publication of the preceding Resolving enum.
            self.promotion_readback_failed.notify_one();
            return Err(pioneer_keystore::KeystoreError::ReadFailed(
                "injected uncertain marker".into(),
            ));
        }
        let target = self.target.lock().unwrap().as_deref() == Some(id.user());
        if target && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.entered.notify_one();
            let mut released = self.released.lock().unwrap();
            while !*released {
                let next = self
                    .resumed
                    .wait_timeout(released, Duration::from_secs(5))
                    .unwrap();
                released = next.0;
                assert!(!next.1.timed_out(), "physical read A was not released");
            }
        }
        self.inner.get_string(id)
    }
    fn put_string(
        &self,
        id: &pioneer_keystore::SecretId,
        value: &str,
        meta: pioneer_keystore::SecretMeta,
    ) -> pioneer_keystore::Result<()> {
        self.inner.put_string(id, value, meta)
    }
    fn delete(&self, id: &pioneer_keystore::SecretId) -> pioneer_keystore::Result<bool> {
        let target = self.target.lock().unwrap().clone();
        let account = target.as_deref() == Some(id.user());
        let fence = target.is_some_and(|target| id.user() == format!("{target}::promotion"));
        let mode = self
            .cleanup_failure
            .load(std::sync::atomic::Ordering::SeqCst);
        if (mode == 1 && account) || (mode == 2 && fence) {
            return Err(pioneer_keystore::KeystoreError::WriteFailed(
                "injected pre-delete failure".into(),
            ));
        }
        if fence
            && self
                .uncertain_promotion
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            // Keep the account AND fence physically present in Resolving. The
            // unavailable readback makes the exchange own reconciliation.
            self.marker_unreadable
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(pioneer_keystore::KeystoreError::WriteFailed(
                "injected uncertain promotion delete".into(),
            ));
        }
        let deleted = self.inner.delete(id)?;
        if mode == 3 && account {
            return Err(pioneer_keystore::KeystoreError::WriteFailed(
                "injected post-delete failure".into(),
            ));
        }
        Ok(deleted)
    }
    fn exists(&self, id: &pioneer_keystore::SecretId) -> pioneer_keystore::Result<bool> {
        self.inner.exists(id)
    }
    fn list(
        &self,
        filter: pioneer_keystore::SecretFilter,
    ) -> pioneer_keystore::Result<Vec<pioneer_keystore::SecretEntryMeta>> {
        self.inner.list(filter)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolving_clear_failure_preserves_details_and_retry_on_same_websocket() {
    for mode in [1, 2, 3] {
        oauth_queue_cancellation_impl(false, None, None, None, false, Some(mode)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_management_clients_denied_then_other_consent_clear_delivers_current_cleanup() {
    oauth_queue_cancellation_impl(false, None, None, None, false, Some(4)).await;
}
