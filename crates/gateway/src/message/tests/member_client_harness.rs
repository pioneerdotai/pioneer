use super::*;
use crate::tests::authorization::MEMBER_A_ID;
use pioneer_client::{
    authorization::apply_access_changed_to_client_state,
    gateway::session_lifecycle::{
        GatewaySessionMetadata, SessionLifecycle, SessionLifecycleEffect, SessionLifecycleEvent,
        SessionLifecycleState,
    },
    rpc::request::{
        JsonRpcAuthorizationFailure, JsonRpcResponseResult, decode_json_rpc_response_value,
    },
    state::client_state::ClientState,
    threads::coordinator::ThreadCoordinator,
};
use pioneer_protocol::{
    AccessChangedNotification, AuthSessionId, DeviceId, GatewayId, ThreadGetResponse,
    ThreadStartResponse, ThreadTimelinePageResponse, ThreadVisibility, TurnStartResponse,
    WorkspaceListResponse,
};

const MEMBER_DEVICE_ID: &str = "D0000000000000000000A";
const MEMBER_SESSION_ID: &str = "S0000000000000000000A";
const AFFECTED_THREAD_ID: &str = "T0000000000000000000A";
const UNRELATED_THREAD_ID: &str = "T0000000000000000000B";
const INTERNAL_ROOT_THREAD_ID: &str = "T0000000000000000000R";
const INTERNAL_CHILD_THREAD_ID: &str = "T0000000000000000000C";

fn decode_shared_client_response<T: serde::Serialize>(response: &T) -> JsonRpcResponseResult {
    let value = serde_json::to_value(response).expect("Gateway response should serialize");
    let (_, result) =
        decode_json_rpc_response_value(&value).expect("shared client should recognize response");
    result
}

fn connect_member_session() -> SessionLifecycle {
    let mut lifecycle = SessionLifecycle::default();
    let stored_metadata = GatewaySessionMetadata {
        gateway_id: GatewayId::new(crate::session::test_support::TEST_GATEWAY_ID)
            .expect("fixture Gateway id"),
        device_id: DeviceId::new(MEMBER_DEVICE_ID).expect("fixture device id"),
        session_id: AuthSessionId::new(MEMBER_SESSION_ID).expect("fixture session id"),
        refresh_generation: 0,
        refresh_expires_at_unix: 10_000,
    };

    let intent_id = match lifecycle.reduce(SessionLifecycleEvent::StoredSessionLoaded(
        stored_metadata.clone(),
    )) {
        SessionLifecycleEffect::BeginRefresh {
            session_id,
            intent_id,
        } => {
            assert_eq!(session_id, stored_metadata.session_id);
            intent_id
        }
        effect => panic!("stored Member session should begin refresh, got {effect:?}"),
    };

    let refreshed_metadata = GatewaySessionMetadata {
        refresh_generation: 1,
        ..stored_metadata
    };
    let connection_generation =
        match lifecycle.reduce(SessionLifecycleEvent::RefreshGrantReceived {
            intent_id,
            metadata: refreshed_metadata.clone(),
            access_expires_at_unix: 1_000,
        }) {
            SessionLifecycleEffect::PersistRefreshBeforeAccess {
                intent_id: persisted_intent,
                candidate_connection_generation,
            } => {
                assert_eq!(persisted_intent, intent_id);
                candidate_connection_generation
            }
            effect => panic!("Member refresh should be persisted before connect, got {effect:?}"),
        };

    assert_eq!(
        lifecycle.reduce(SessionLifecycleEvent::SecureStorageCommitted { intent_id }),
        SessionLifecycleEffect::ConnectWithEphemeralAccess {
            connection_generation
        }
    );
    assert_eq!(
        lifecycle.reduce(SessionLifecycleEvent::ConnectionEstablished {
            generation: connection_generation,
        }),
        SessionLifecycleEffect::SwitchConnection {
            active_connection_generation: connection_generation,
            close_connection_generation: None,
        }
    );
    assert!(matches!(
        lifecycle.state(),
        SessionLifecycleState::Active { metadata, .. }
            if metadata == &refreshed_metadata
    ));
    lifecycle
}

fn register_member_principal() -> Arc<crate::auth::AuthenticatedSessionPrincipal> {
    let mut member = (*authenticated_test_superuser()).clone();
    member.principal_id =
        pioneer_protocol::PrincipalId::new(MEMBER_A_ID).expect("fixture Member principal id");
    member.kind = pioneer_protocol::PrincipalKind::User;
    member.role_key = Some(RoleKey::member());
    member.device_id = DeviceId::new(MEMBER_DEVICE_ID).expect("fixture Member device id");
    member.session_id = AuthSessionId::new(MEMBER_SESSION_ID).expect("fixture Member session id");
    Arc::new(member)
}

async fn start_member_thread(
    processor: &MessageProcessor,
    connection_id: ConnectionId,
    rx: &mut mpsc::Receiver<Message>,
    workspace_id: &str,
    thread_id: &str,
    visibility: ThreadVisibility,
) -> pioneer_protocol::Thread {
    let request_id = generate_test_request_id("memberclient", thread_id);
    processor
        .process_request_for_connection(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "thread/start",
                "params": {
                    "thread_id": thread_id,
                    "workspace_id": workspace_id,
                    "visibility": visibility
                }
            })
            .to_string(),
        )
        .await;
    let response = recv_response_by_id(rx, request_id.as_str()).await;
    let result = decode_shared_client_response(&response)
        .expect("shared client should accept allowed Member thread response");
    let response: ThreadStartResponse =
        serde_json::from_value(result).expect("typed Member thread/start response");
    assert_eq!(response.thread.visibility, Some(visibility));
    response.thread
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hermetic_member_shared_client_covers_policy_and_access_loss_without_logout() {
    let session_lifecycle = connect_member_session();
    let (workspace_manager, crud_store, affected_workspace_id) = setup_workspace_manager().await;
    let unrelated_workspace_id = "W0000000000000000000B";
    let inaccessible_workspace_id = "W0000000000000000000C";
    workspace_manager
        .create_workspace(unrelated_workspace_id, Some("Unrelated Member workspace"))
        .await
        .expect("create unrelated Member workspace");
    workspace_manager
        .create_workspace(inaccessible_workspace_id, Some("Inaccessible workspace"))
        .await
        .expect("create inaccessible workspace");
    crud_store
        .database_connection()
        .execute_unprepared(&format!(
            "INSERT INTO gateway_principal(\
                id,gateway_id,kind,role_key,status,display_name,nickname,nickname_key,\
                created_at,updated_at,removed_at\
             ) VALUES(\
                '{MEMBER_A_ID}','{}','user','member','active',\
                'Member Client','member-client','member-client',\
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL\
             );\
             INSERT INTO workspace_membership(\
                principal_id,workspace_id,granted_by_actor_kind,granted_by_actor_id,\
                created_at,updated_at\
             ) VALUES\
                ('{MEMBER_A_ID}','{affected_workspace_id}','system',NULL,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                ('{MEMBER_A_ID}','{unrelated_workspace_id}','system',NULL,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);\
             INSERT INTO device(\
                id,gateway_id,principal_id,installation_id,display_name,client_kind,\
                platform,client_version,status,created_at,updated_at,last_seen_at,revoked_at\
             ) VALUES(\
                '{MEMBER_DEVICE_ID}','{}','{MEMBER_A_ID}','member-client-fixture',\
                'Member Client','mobile','test','1','active',CURRENT_TIMESTAMP,\
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL\
             );\
             INSERT INTO auth_session(\
                id,gateway_id,principal_id,device_id,token_family_id,created_by_session_id,\
                activation_token_hash,activation_locator_hash,activation_failed_attempts,\
                activation_expires_at,activated_at,status,refresh_generation,created_at,\
                updated_at,last_seen_at,last_refreshed_at,refresh_expires_at,revoked_at,\
                revoke_reason\
             ) VALUES(\
                '{MEMBER_SESSION_ID}','{}','{MEMBER_A_ID}','{MEMBER_DEVICE_ID}',\
                'F0000000000000000000A',NULL,randomblob(32),randomblob(32),0,\
                datetime('now','+10 minutes'),CURRENT_TIMESTAMP,'active',1,\
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,\
                datetime('now','+90 days'),NULL,NULL\
             );\
             INSERT INTO auth_refresh_credential(\
                id,session_id,token_family_id,generation,token_hash,issued_at,expires_at\
             ) VALUES(\
                'R0000000000000000000A','{MEMBER_SESSION_ID}',\
                'F0000000000000000000A',1,randomblob(32),CURRENT_TIMESTAMP,\
                datetime('now','+90 days')\
             );",
            crate::session::test_support::TEST_GATEWAY_ID,
            crate::session::test_support::TEST_GATEWAY_ID,
            crate::session::test_support::TEST_GATEWAY_ID,
        ))
        .await
        .expect("materialize internal Member client grants");

    let session_manager = Arc::new(SessionManager::new());
    let (member_tx, mut member_rx) = mpsc::channel(32);
    let member_principal = register_member_principal();
    let connection_id = session_manager
        .register_connection(member_tx, member_principal.clone())
        .await
        .expect("connect internal Member client");
    let processor = MessageProcessor::new(
        Arc::new(ThreadManager::new("o4-mini", "openai")),
        test_provider(),
        session_manager.clone(),
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_context_budget(),
        test_tool_loop_config(),
    );

    let list_id = generate_test_request_id("memberclient", "allowed");
    processor
        .process_request_for_connection(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": list_id,
                "method": "workspace/list",
                "params": {}
            })
            .to_string(),
        )
        .await;
    let list_response = recv_response_by_id(&mut member_rx, list_id.as_str()).await;
    let list_result = decode_shared_client_response(&list_response)
        .expect("shared client should accept allowed workspace/list");
    let list: WorkspaceListResponse =
        serde_json::from_value(list_result).expect("typed workspace/list response");
    let listed_ids = list
        .workspaces
        .iter()
        .map(|workspace| workspace.id.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(
        listed_ids,
        HashSet::from([affected_workspace_id.as_str(), unrelated_workspace_id])
    );
    assert!(!listed_ids.contains(inaccessible_workspace_id));

    let mut affected_thread = start_member_thread(
        &processor,
        connection_id,
        &mut member_rx,
        affected_workspace_id.as_str(),
        AFFECTED_THREAD_ID,
        ThreadVisibility::Private,
    )
    .await;
    affected_thread.name = Some("classified affected client payload".to_owned());
    let unrelated_thread = start_member_thread(
        &processor,
        connection_id,
        &mut member_rx,
        unrelated_workspace_id,
        UNRELATED_THREAD_ID,
        ThreadVisibility::Workspace,
    )
    .await;

    let workspace_turn_id = generate_test_request_id("memberclient", "workspace-turn");
    processor
        .process_request_for_connection(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": workspace_turn_id,
                "method": "turn/start",
                "params": {
                    "thread_id": UNRELATED_THREAD_ID,
                    "turn_id": "T0000000000000000000D",
                    "mode": "Agent",
                    "model_provider": "openai",
                    "model": "o4-mini",
                    "permission_profile": { "mode": "supervised" },
                    "input": [{ "type": "text", "text": "run the Member agent" }]
                }
            })
            .to_string(),
        )
        .await;
    let workspace_turn = recv_response_by_id(&mut member_rx, workspace_turn_id.as_str()).await;
    let workspace_turn: TurnStartResponse = serde_json::from_value(
        decode_shared_client_response(&workspace_turn)
            .expect("Member may materialize a workspace-visible draft and start an agent"),
    )
    .expect("typed Member workspace turn/start response");
    assert_eq!(workspace_turn.turn.id, "T0000000000000000000D");
    assert_eq!(
        workspace_turn.turn.permission_profile.mode,
        pioneer_protocol::TurnPermissionMode::Supervised
    );
    let terminal = recv_turn_terminal_notification(&mut member_rx).await;
    assert_eq!(
        terminal.method,
        events::TURN_COMPLETED,
        "Member agent turn must complete successfully, got params={:?}",
        terminal.params
    );

    for (suffix, requested_resolution) in [("allow", "allow_for_turn"), ("deny", "deny")] {
        let permission_request_id = format!("member-permission-{suffix}");
        let response_request_id = generate_test_request_id("memberpermission", suffix);
        let (respond_tx, respond_rx) = tokio::sync::oneshot::channel();
        processor
            .open_native_permission_request(crate::permissions::GatewayPermissionApprovalRequest {
                request_id: permission_request_id.clone(),
                workspace_id: Some(unrelated_workspace_id.to_owned()),
                thread_id: Some(UNRELATED_THREAD_ID.to_owned()),
                turn_id: Some(workspace_turn.turn.id.clone()),
                tool_name: "exec_command".to_owned(),
                key: pioneer_tools::PermissionRequestKey {
                    profile_mode: pioneer_protocol::TurnPermissionMode::Supervised,
                    tool_name: "exec_command".to_owned(),
                    action: pioneer_tools::PermissionActionKind::ShellCommand,
                    normalized_scope_hash: format!("member-permission-scope-{suffix}"),
                    turn_id: workspace_turn.turn.id.clone(),
                },
                reason: pioneer_tools::PermissionDecisionReason::PolicyRequiresApproval,
                summary: Some("run a Member-approved command".to_owned()),
                details: Vec::new(),
                respond_to: respond_tx,
            })
            .await;

        let opened =
            recv_notification_by_method(&mut member_rx, events::TURN_PERMISSION_REQUEST_OPENED)
                .await;
        assert_eq!(
            opened.params.as_ref().and_then(|params| params
                .get("request")
                .and_then(|request| request.get("request_id"))),
            Some(&serde_json::json!(permission_request_id))
        );

        processor
            .process_request_for_connection(
                connection_id,
                &json!({
                    "jsonrpc": "2.0",
                    "id": response_request_id,
                    "method": pioneer_protocol::constants::methods::TURN_PERMISSION_REQUEST_RESPOND,
                    "params": {
                        "request_id": permission_request_id,
                        "resolution": requested_resolution
                    }
                })
                .to_string(),
            )
            .await;
        let (response, _) = recv_response_and_notification_by_id_method(
            &mut member_rx,
            response_request_id.as_str(),
            events::TURN_PERMISSION_REQUEST_RESOLVED,
        )
        .await;
        assert_eq!(response.result["resolution"], requested_resolution);
        let broker_resolution = respond_rx
            .await
            .expect("Member permission decision must reach the running agent");
        match requested_resolution {
            "allow_for_turn" => assert_eq!(
                broker_resolution,
                pioneer_tools::PermissionApprovalResolution::AllowForTurn
            ),
            "deny" => assert!(matches!(
                broker_resolution,
                pioneer_tools::PermissionApprovalResolution::Deny { .. }
            )),
            _ => unreachable!(),
        }
    }

    crud_store
        .database_connection()
        .execute_unprepared(&format!(
            "INSERT INTO thread(\
                id,workspace_id,name,preview,mode,model,model_provider,status,\
                origin_kind,sidebar_visibility,access_class,created_at,updated_at,\
                created_by_actor_kind,created_by_actor_id\
             ) VALUES(\
                '{INTERNAL_ROOT_THREAD_ID}','{affected_workspace_id}','Child root','',\
                'chat','test','test','active','user','visible','private',\
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'principal','{MEMBER_A_ID}'\
             ),(\
                '{INTERNAL_CHILD_THREAD_ID}','{affected_workspace_id}','Child agent','',\
                'agent','test','test','active','task_run','hidden','internal',\
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'system',NULL\
             );\
             INSERT INTO thread_membership(\
                thread_id,principal_id,added_by_actor_kind,added_by_actor_id,\
                created_at,updated_at\
             ) VALUES(\
                '{INTERNAL_ROOT_THREAD_ID}','{MEMBER_A_ID}','system',NULL,\
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP\
             );\
             INSERT INTO thread_lineage(\
                child_thread_id,parent_thread_id,root_thread_id,depth,created_at,\
                origin_kind,created_by_thread_id,created_by_turn_id\
             ) VALUES(\
                '{INTERNAL_CHILD_THREAD_ID}','{INTERNAL_ROOT_THREAD_ID}',\
                '{INTERNAL_ROOT_THREAD_ID}',1,CURRENT_TIMESTAMP,'task_run',\
                '{INTERNAL_ROOT_THREAD_ID}',NULL\
             );"
        ))
        .await
        .expect("materialize Member-visible internal child lineage");

    let child_get_id = generate_test_request_id("memberclient", "childget");
    processor
        .process_request_for_connection(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": child_get_id,
                "method": "thread/get",
                "params": { "thread_id": INTERNAL_CHILD_THREAD_ID }
            })
            .to_string(),
        )
        .await;
    let child_get = recv_response_by_id(&mut member_rx, child_get_id.as_str()).await;
    let child_get: ThreadGetResponse = serde_json::from_value(
        decode_shared_client_response(&child_get).expect("Member may resolve an authorized child"),
    )
    .expect("typed internal child thread/get response");
    assert_eq!(child_get.thread.id, INTERNAL_CHILD_THREAD_ID);

    let child_start_id = generate_test_request_id("memberclient", "childstart");
    processor
        .process_request_for_connection(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": child_start_id,
                "method": "thread/start",
                "params": {
                    "thread_id": INTERNAL_CHILD_THREAD_ID,
                    "workspace_id": affected_workspace_id
                }
            })
            .to_string(),
        )
        .await;
    let child_start = recv_response_by_id(&mut member_rx, child_start_id.as_str()).await;
    let child_start: ThreadStartResponse = serde_json::from_value(
        decode_shared_client_response(&child_start)
            .expect("Member may subscribe to an authorized child"),
    )
    .expect("typed internal child thread/start response");
    assert_eq!(child_start.thread.id, INTERNAL_CHILD_THREAD_ID);

    let child_timeline_id = generate_test_request_id("memberclient", "childtimeline");
    processor
        .process_request_for_connection(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": child_timeline_id,
                "method": "thread/timeline/page",
                "params": {
                    "threadId": INTERNAL_CHILD_THREAD_ID,
                    "anchor": { "kind": "newest" },
                    "limit": 12
                }
            })
            .to_string(),
        )
        .await;
    let child_timeline = recv_response_by_id(&mut member_rx, child_timeline_id.as_str()).await;
    let child_timeline: ThreadTimelinePageResponse = serde_json::from_value(
        decode_shared_client_response(&child_timeline)
            .expect("Member may read an authorized child timeline"),
    )
    .expect("typed internal child timeline response");
    assert_eq!(child_timeline.thread_id, INTERNAL_CHILD_THREAD_ID);

    let forbidden_id = generate_test_request_id("memberclient", "forbidden");
    processor
        .process_request_for_connection(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": forbidden_id,
                "method": "workspace/create",
                "params": {
                    "workspace_id": "W0000000000000000000D",
                    "name": "must not be created"
                }
            })
            .to_string(),
        )
        .await;
    let forbidden = recv_error_by_id(&mut member_rx, forbidden_id.as_str()).await;
    let forbidden = decode_shared_client_response(&forbidden)
        .expect_err("shared client should retain forbidden response");
    assert_eq!(
        forbidden.authorization_failure(),
        Some(JsonRpcAuthorizationFailure::Forbidden)
    );
    assert!(matches!(
        session_lifecycle.state(),
        SessionLifecycleState::Active { .. }
    ));

    let inaccessible_id = generate_test_request_id("memberclient", "inaccessible");
    processor
        .process_request_for_connection(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": inaccessible_id,
                "method": "workspace/select",
                "params": {
                    "workspace_id": inaccessible_workspace_id,
                    "make_current": false
                }
            })
            .to_string(),
        )
        .await;
    let inaccessible = recv_error_by_id(&mut member_rx, inaccessible_id.as_str()).await;
    let inaccessible = decode_shared_client_response(&inaccessible)
        .expect_err("shared client should retain inaccessible response");
    assert_eq!(
        inaccessible.authorization_failure(),
        Some(JsonRpcAuthorizationFailure::InaccessibleResource)
    );
    assert!(matches!(
        session_lifecycle.state(),
        SessionLifecycleState::Active { .. }
    ));

    let mut client_state = ClientState::default();
    client_state.workspaces.workspaces = list.workspaces;
    client_state.workspaces.preferred_workspace_id = Some(affected_workspace_id.clone());
    client_state.threads.active_thread_id = Some(AFFECTED_THREAD_ID.to_owned());
    client_state.threads.coordinators.insert(
        AFFECTED_THREAD_ID.to_owned(),
        ThreadCoordinator::new(affected_thread),
    );
    client_state.threads.coordinators.insert(
        UNRELATED_THREAD_ID.to_owned(),
        ThreadCoordinator::new(unrelated_thread),
    );
    client_state.gateway.ws_connection_id = Some(41);
    client_state.gateway.bootstrap_complete = true;

    crud_store
        .database_connection()
        .execute_unprepared(&format!(
            "DELETE FROM workspace_membership \
             WHERE principal_id='{MEMBER_A_ID}' \
               AND workspace_id='{affected_workspace_id}'"
        ))
        .await
        .expect("commit affected Member workspace revoke");
    let signal = processor
        .publish_committed_workspace_membership_invalidation(
            member_principal.principal_id.clone(),
            affected_workspace_id.clone(),
        )
        .await;
    let notification = recv_notification_by_method(&mut member_rx, events::ACCESS_CHANGED).await;
    let access_changed: AccessChangedNotification = serde_json::from_value(
        notification
            .params
            .expect("access/changed should carry safe params"),
    )
    .expect("shared protocol should decode access/changed");
    assert_eq!(
        access_changed.authorization_revision,
        signal.authorization_revision
    );
    assert_eq!(access_changed.access_lost, Some(true));

    let plan = apply_access_changed_to_client_state(&mut client_state, &access_changed);
    assert!(plan.apply);
    assert_eq!(plan.invalidate_thread_ids, vec![AFFECTED_THREAD_ID]);
    assert_eq!(client_state.gateway.ws_connection_id, Some(41));
    assert!(client_state.gateway.bootstrap_complete);
    assert!(client_state.threads.active_thread_id.is_none());
    assert!(
        !client_state
            .threads
            .coordinators
            .contains_key(AFFECTED_THREAD_ID)
    );
    assert!(
        client_state
            .threads
            .coordinators
            .contains_key(UNRELATED_THREAD_ID)
    );
    assert!(
        client_state
            .workspaces
            .workspaces
            .iter()
            .all(|workspace| workspace.id != affected_workspace_id)
    );
    assert!(
        client_state
            .workspaces
            .workspaces
            .iter()
            .any(|workspace| workspace.id == unrelated_workspace_id)
    );
    assert!(
        client_state
            .threads
            .coordinators
            .values()
            .filter_map(ThreadCoordinator::thread)
            .all(|thread| {
                thread.id != AFFECTED_THREAD_ID
                    && thread.workspace_id != affected_workspace_id
                    && !thread
                        .name
                        .as_deref()
                        .is_some_and(|name| name.contains("classified affected client payload"))
            }),
        "no inaccessible protected thread payload may remain cached"
    );
    assert!(matches!(
        session_lifecycle.state(),
        SessionLifecycleState::Active { .. }
    ));
    assert!(
        session_manager
            .connection_principal(connection_id)
            .await
            .is_ok(),
        "workspace revoke must not log out an otherwise valid Member session"
    );

    let refreshed_list_id = generate_test_request_id("memberclient", "refresh");
    processor
        .process_request_for_connection(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": refreshed_list_id,
                "method": "workspace/list",
                "params": {}
            })
            .to_string(),
        )
        .await;
    let refreshed_response = recv_response_by_id(&mut member_rx, refreshed_list_id.as_str()).await;
    let refreshed_result = decode_shared_client_response(&refreshed_response)
        .expect("shared client should refresh server-filtered workspace list");
    let refreshed: WorkspaceListResponse =
        serde_json::from_value(refreshed_result).expect("typed refreshed workspace/list");
    assert_eq!(refreshed.workspaces.len(), 1);
    assert_eq!(refreshed.workspaces[0].id, unrelated_workspace_id);
}

async fn recv_turn_terminal_notification(rx: &mut mpsc::Receiver<Message>) -> JsonRpcNotification {
    for _ in 0..200 {
        let payload = recv_text_timeout_context(
            rx,
            Duration::from_secs(10),
            "Member agent terminal notification",
        )
        .await;
        let value: serde_json::Value =
            serde_json::from_str(&payload).expect("json-rpc payload should decode");
        let method = value.get("method").and_then(serde_json::Value::as_str);
        if matches!(method, Some(events::TURN_COMPLETED))
            || matches!(method, Some(events::TURN_FAILED))
            || matches!(method, Some(events::TURN_BLOCKED))
        {
            return serde_json::from_value(value)
                .expect("terminal turn notification should decode");
        }
    }

    panic!("timed out waiting for Member agent terminal notification");
}
