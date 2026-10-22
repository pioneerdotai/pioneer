//! Observable committed-completion/read delivery over the existing socket lane.
//! Written for execution only after external source approval.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scoped_push_delivers_committed_answer_and_read_without_tree_hint() {
    const THREAD: &str = "thrPushEvent000000001";
    const TURN: &str = "trnPushEvent000000001";
    let (source_tx, mut source_rx) = mpsc::channel(128);
    let (observer_tx, mut observer_rx) = mpsc::channel(128);
    let sessions = Arc::new(SessionManager::new());
    let source = register_authenticated_test_connection(&sessions, source_tx).await;
    let observer = register_authenticated_test_connection(&sessions, observer_tx).await;
    let (workspaces, store, workspace) = setup_workspace_manager().await;
    prepare_post_access_unread_fixture(&store, &workspace).await;
    let processor = MessageProcessor::new(
        Arc::new(ThreadManager::new("o4-mini", "openai")),
        test_provider(),
        sessions.clone(),
        workspaces,
        store,
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    );
    let start = generate_test_request_id("pushtest", "start");
    processor
        .process_request_for_connection(
            source,
            &json!({
                "jsonrpc":"2.0", "id":start, "method":"thread/start",
                "params":{"thread_id":THREAD,"workspace_id":workspace}
            })
            .to_string(),
        )
        .await;
    let _ = recv_response_by_id(&mut source_rx, &start).await;
    let _ = recv_notification_by_method(&mut source_rx, events::THREAD_STARTED).await;
    send_epic6_test_message(&processor, source, &mut source_rx, THREAD, TURN, "question").await;
    sessions
        .set_connection_workspace(observer, Some(workspace.clone()))
        .await;
    let register = generate_test_request_id("pushtest", "register");
    processor.process_request_for_connection(observer, &json!({
        "jsonrpc":"2.0", "id":register, "method":"workspace/push",
        "params":{"workspaceId":workspace,"epoch":"push-event-test","tree":true,"inbox":false,"threadIds":[THREAD],"runIds":[]}
    }).to_string()).await;
    let response = recv_response_by_id(&mut observer_rx, &register).await;
    assert_eq!(response.result["supported"], true);
    let initial =
        recv_notification_by_method(&mut observer_rx, events::WORKSPACE_PUSH_CHANGED).await;
    let initial: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(initial.params.unwrap()).unwrap();
    assert!(initial.snapshot && !initial.resync_required);
    assert_eq!(initial.revision, 1);
    assert_eq!(initial.indicators[0].unread_count, Some(0));

    let answer = pioneer_protocol::TurnItem::AgentMessage {
        id: pioneer_protocol::task_delivery_result_item_id("push-completion"),
        text: "answer".into(),
        phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
        markdown: None,
        markdown_version: None,
    };
    processor
        .crud_store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: workspace.clone(),
                thread_id: THREAD.into(),
                turn_id: TURN.into(),
                item: answer.clone(),
            },
            now_timestamp_secs(),
        )
        .await
        .unwrap();
    processor
        .notify_semantic_timeline_item_changed(&workspace, THREAD, TURN, &answer, Some("completed"))
        .await;
    // Read every frame instead of filtering by method: a legacy tree hint here
    // would cause the forbidden whole-directory refresh on a negotiated client.
    let frame = observer_rx
        .recv()
        .await
        .expect("committed completion frame");
    let Message::Text(text) = frame else {
        panic!("expected JSON notification");
    };
    let frame: pioneer_protocol::JsonRpcNotification = serde_json::from_str(&text).unwrap();
    let mapped = pioneer_protocol::GatewayNotification::from_jsonrpc(frame).unwrap();
    let pioneer_protocol::GatewayNotification::WorkspacePush(packet) = mapped else {
        panic!("expected addressed patch, not tree invalidation");
    };
    assert_eq!(packet.thread_summaries.len(), 1);
    assert_eq!(packet.thread_summaries[0].thread.preview, "question");
    assert_eq!(packet.thread_summaries[0].thread.id, THREAD);
    assert_eq!(packet.revision, 2);
    assert!(!packet.snapshot && !packet.resync_required);
    assert_eq!(packet.indicators.len(), 1);
    assert_eq!(packet.indicators[0].thread_id, THREAD);
    assert_eq!(packet.indicators[0].unread_count, Some(1));
    assert_eq!(packet.indicators[0].has_running_tasks, None);
    assert!(packet.inbox.is_none());

    // A turn-only read stops at the user message. Obtain the exact viewed
    // assistant position so this read also covers the late completion.
    let timeline = request_thread_timeline_page_for_test(
        &processor,
        observer,
        &mut observer_rx,
        THREAD,
        &generate_test_request_id("pushtest", "timeline"),
    )
    .await;
    let answer_id = pioneer_protocol::task_delivery_result_item_id("push-completion");
    let answer_block = timeline
        .blocks
        .iter()
        .find(|block| matches!(&block.kind,
            pioneer_protocol::TimelineBlockKind::AssistantMessage { item_id, .. } if item_id == &answer_id))
        .expect("completed answer is visible in the canonical timeline");
    let read = generate_test_request_id("pushtest", "read");
    processor
        .process_request_for_connection(
            observer,
            &json!({
                "jsonrpc":"2.0", "id":read, "method":"thread/read",
                "params":{"thread_id":THREAD,"through_turn_id":TURN,
                    "through_block":{"block_id":answer_block.block_id,"sort_key":answer_block.sort_key}}
            })
            .to_string(),
        )
        .await;
    let (ack, patch) = recv_response_and_notification_by_id_method(
        &mut observer_rx,
        &read,
        events::WORKSPACE_PUSH_CHANGED,
    )
    .await;
    let ack: pioneer_protocol::ThreadReadResponse = serde_json::from_value(ack.result).unwrap();
    let patch: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(patch.params.unwrap()).unwrap();
    assert_eq!(ack.unread_count, 0);
    assert_eq!(patch.revision, 3);
    assert_eq!(patch.indicators[0].unread_count, Some(ack.unread_count));
    assert!(!patch.snapshot && patch.inbox.is_none());
    // Fault/late-event case: an ineligible run ID from a workspace producer
    // must not be disclosed as a remove to a subscriber who never knew it.
    let register_inbox = generate_test_request_id("pushtest", "inbox");
    processor.process_request_for_connection(observer, &json!({
        "jsonrpc":"2.0", "id":register_inbox, "method":"workspace/push",
        "params":{"workspaceId":workspace,"epoch":"push-inbox-test","tree":false,"inbox":true,"threadIds":[],"runIds":[]}
    }).to_string()).await;
    let _ = recv_response_by_id(&mut observer_rx, &register_inbox).await;
    let initial =
        recv_notification_by_method(&mut observer_rx, events::WORKSPACE_PUSH_CHANGED).await;
    let initial: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(initial.params.unwrap()).unwrap();
    assert!(initial.snapshot && initial.inbox.as_ref().unwrap().replace);
    processor.dirty_push(
        &workspace,
        None,
        None,
        Some("R0000000000000000000X"),
        false,
        None,
    );
    let update =
        recv_notification_by_method(&mut observer_rx, events::WORKSPACE_PUSH_CHANGED).await;
    let update: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(update.params.unwrap()).unwrap();
    assert!(!update.snapshot && !update.resync_required);
    assert!(update.indicators.is_empty());
    let inbox = update.inbox.unwrap();
    assert_eq!(inbox.unread_count, 0);
    assert!(inbox.upserts.is_empty() && inbox.removes.is_empty());
    sessions.unregister_connection(observer).await;
}

/// A real restricted connection must never receive private metadata, including
/// dirty metadata captured before the first snapshot. Inspect every frame.
#[tokio::test]
async fn private_dirty_during_bootstrap_does_not_disclose_or_retire_member_scope() {
    use pioneer_protocol::{PrincipalId, PrincipalKind, RoleKey};
    use sea_orm::ConnectionTrait;
    const MEMBER: &str = "P0000000000000000000A";
    const PUBLIC: &str = "T0000000000000000000A";
    const PRIVATE: &str = "T0000000000000000000B";
    let (workspaces, store, workspace) = setup_workspace_manager().await;
    let sessions = Arc::new(SessionManager::new());
    let (source_tx, mut source_rx) = mpsc::channel(128);
    let source = register_authenticated_test_connection(&sessions, source_tx).await;
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
    store.database_connection().execute_unprepared(&format!(
        "INSERT INTO gateway_principal(id,gateway_id,kind,role_key,status,display_name,nickname,nickname_key,created_at,updated_at) VALUES('{MEMBER}','{}','user','member','active','Push member','push-member','push-member',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); INSERT INTO workspace_membership(principal_id,workspace_id,granted_by_actor_kind,created_at,updated_at) VALUES('{MEMBER}','{workspace}','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);",
        crate::session::test_support::TEST_GATEWAY_ID,
    )).await.unwrap();
    for (thread, visibility) in [(PUBLIC, "workspace"), (PRIVATE, "private")] {
        let id = generate_test_request_id("pushacl", thread);
        processor
            .process_request_for_connection(
                source,
                &json!({
                    "jsonrpc":"2.0", "id":id, "method":"thread/start",
                    "params":{"thread_id":thread,"workspace_id":workspace,"visibility":visibility}
                })
                .to_string(),
            )
            .await;
        let response = recv_response_by_id(&mut source_rx, &id).await;
        assert_eq!(response.result["thread"]["id"], thread);
        if thread == PUBLIC {
            // thread/start owns an ephemeral draft; the subscribed public ID
            // needs a real durable commit before the publisher can authorize it.
            send_epic6_test_message(
                &processor,
                source,
                &mut source_rx,
                PUBLIC,
                "T0000000000000000000F",
                "public bootstrap message",
            )
            .await;
        }
    }
    send_epic6_test_message(
        &processor,
        source,
        &mut source_rx,
        PRIVATE,
        "T0000000000000000000C",
        "private-canary-preview",
    )
    .await;
    let mut principal = (*authenticated_test_superuser()).clone();
    principal.principal_id = PrincipalId::new(MEMBER).unwrap();
    principal.kind = PrincipalKind::User;
    principal.role_key = Some(RoleKey::member());
    let (member_tx, mut member_rx) = mpsc::channel(128);
    let member = sessions
        .register_connection(member_tx, Arc::new(principal))
        .await
        .unwrap();
    sessions
        .set_connection_workspace(member, Some(workspace.clone()))
        .await;
    let register = generate_test_request_id("pushacl", "register");
    processor.process_request_for_connection(member, &json!({
        "jsonrpc":"2.0", "id":register, "method":"workspace/push",
        "params":{"workspaceId":workspace,"epoch":"private-bootstrap","tree":true,"inbox":false,"threadIds":[PUBLIC]}
    }).to_string()).await;
    // Hold the first calculation while the real producer resolves sidebar
    // eligibility. No snapshot may overtake the private bootstrap dirty key.
    let bootstrap_hold = processor
        .begin_push_mutation(member, &workspace, true)
        .unwrap();
    processor
        .notify_thread_unread_changed(&workspace, PRIVATE)
        .await;
    bootstrap_hold.finish(None, None);
    let mut accepted = None;
    while accepted.is_none() {
        let frame = tokio::time::timeout(Duration::from_secs(5), member_rx.recv())
            .await
            .expect("bounded member frame")
            .unwrap();
        let Message::Text(text) = frame else {
            panic!("expected JSON frame")
        };
        assert!(
            !text.contains(PRIVATE),
            "private identity in outbound frame"
        );
        assert!(!text.contains("private-canary-preview"));
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        if value["method"] == events::WORKSPACE_PUSH_CHANGED {
            let packet: pioneer_protocol::WorkspacePushNotification =
                serde_json::from_value(value["params"].clone()).unwrap();
            assert!(packet.snapshot && !packet.resync_required);
            assert!(packet.thread_summaries.is_empty());
            accepted = Some(packet);
        }
    }
    // The unrelated private mutation must neither retire the scope nor block
    // the following authorized event. Check every intervening frame as well.
    let hold = processor
        .begin_push_mutation(member, &workspace, true)
        .unwrap();
    processor.dirty_push(&workspace, Some(PRIVATE), None, None, false, None);
    send_epic6_test_message(
        &processor,
        source,
        &mut source_rx,
        PUBLIC,
        "T0000000000000000000D",
        "accessible committed message",
    )
    .await;
    hold.finish(None, None);
    loop {
        let Message::Text(text) = tokio::time::timeout(Duration::from_secs(5), member_rx.recv())
            .await
            .expect("bounded member frame")
            .unwrap()
        else {
            panic!("expected JSON")
        };
        assert!(!text.contains(PRIVATE));
        assert!(!text.contains("private-canary-preview"));
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_ne!(value["method"], events::THREAD_TREE_CHANGED);
        if value["method"] == events::WORKSPACE_PUSH_CHANGED {
            let packet: pioneer_protocol::WorkspacePushNotification =
                serde_json::from_value(value["params"].clone()).unwrap();
            assert!(!packet.resync_required && !packet.snapshot);
            assert_eq!(packet.revision, accepted.unwrap().revision + 1);
            assert_eq!(packet.thread_summaries.len(), 1);
            assert_eq!(packet.thread_summaries[0].thread.id, PUBLIC);
            break;
        }
    }
    // Targeted committed ACL invalidation must retire A without retiring B.
    let register_b = generate_test_request_id("pushacl", "register-b");
    sessions
        .set_connection_workspace(source, Some(workspace.clone()))
        .await;
    processor.process_request_for_connection(source, &json!({
        "jsonrpc":"2.0", "id":register_b, "method":"workspace/push",
        "params":{"workspaceId":workspace,"epoch":"unaffected-b","tree":true,"inbox":true,"threadIds":[PUBLIC]}
    }).to_string()).await;
    let _ = recv_response_by_id(&mut source_rx, &register_b).await;
    let initial_b =
        recv_notification_by_method(&mut source_rx, events::WORKSPACE_PUSH_CHANGED).await;
    let initial_b: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(initial_b.params.unwrap()).unwrap();
    store.database_connection().execute_unprepared(&format!(
        "DELETE FROM workspace_membership WHERE principal_id='{MEMBER}' AND workspace_id='{workspace}'"
    )).await.unwrap();
    let signal = processor
        .authorization_invalidation_hub
        .publish(
            crate::authorization::AccessChangeKind::WorkspaceMembership,
            Some(PrincipalId::new(MEMBER).unwrap()),
            workspace.clone(),
            None,
        )
        .await
        .unwrap();
    processor
        .apply_committed_authorization_invalidation(&signal)
        .await;
    let hold = processor
        .begin_push_mutation(source, &workspace, true)
        .unwrap();
    store.database_connection().execute_unprepared(&format!(
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,executor_kind,status,title,goal,priority,revision,created_at,updated_at) VALUES('K0000000000000000000B','{workspace}','workspace','{workspace}','{PUBLIC}','system','completed','Unaffected inbox','Goal',0,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,completed_at,created_at,updated_at) VALUES('R0000000000000000000B','K0000000000000000000B','R0000000000000000000B',1,1,'succeeded','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"
    )).await.unwrap();
    processor.dirty_push(
        &workspace,
        None,
        None,
        Some("R0000000000000000000B"),
        false,
        None,
    );
    send_epic6_test_message(
        &processor,
        source,
        &mut source_rx,
        PUBLIC,
        "T0000000000000000000E",
        "post revoke committed accessible message",
    )
    .await;
    hold.finish(None, None);
    let next_b = recv_notification_by_method(&mut source_rx, events::WORKSPACE_PUSH_CHANGED).await;
    let next_b: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(next_b.params.unwrap()).unwrap();
    assert!(!next_b.snapshot && !next_b.resync_required);
    assert_eq!(next_b.epoch, initial_b.epoch);
    assert_eq!(next_b.revision, initial_b.revision + 1);
    let inbox = next_b.inbox.expect("unaffected recipient keeps inbox push");
    assert!(!inbox.replace);
    assert_eq!(inbox.unread_count, 1);
    assert_eq!(inbox.upserts.len(), 1);
    assert_eq!(inbox.upserts[0].run_id, "R0000000000000000000B");
    while let Ok(frame) = member_rx.try_recv() {
        if let Message::Text(text) = frame {
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_ne!(value["method"], events::WORKSPACE_PUSH_CHANGED);
        }
    }
    sessions.unregister_connection(member).await;
    sessions.unregister_connection(source).await;
}

#[tokio::test]
async fn inbox_reset_keeps_delivered_coverage_and_point_ack_cannot_hide_reset() {
    use sea_orm::ConnectionTrait;
    let (workspaces, store, workspace) = setup_workspace_manager().await;
    let sessions = Arc::new(SessionManager::new());
    let (tx, mut rx) = mpsc::channel(128);
    let connection = register_authenticated_test_connection(&sessions, tx).await;
    sessions
        .set_connection_workspace(connection, Some(workspace.clone()))
        .await;
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
    store.database_connection().execute_unprepared(&format!(
        "INSERT INTO thread(id,workspace_id,name,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('T0000000000000000000I','{workspace}','Inbox root','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,executor_kind,status,title,goal,priority,revision,created_at,updated_at) VALUES('K0000000000000000000I','{workspace}','workspace','{workspace}','T0000000000000000000I','system','completed','Before','Goal',0,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);"
    )).await.unwrap();
    let register = generate_test_request_id("pushcoverage", "register");
    processor
        .process_request_for_connection(
            connection,
            &json!({
                "jsonrpc":"2.0", "id":register,"method":"workspace/push",
                "params":{"workspaceId":workspace,"epoch":"coverage","tree":true,"threadIds":["T0000000000000000000I"],"inbox":true}
            })
            .to_string(),
        )
        .await;
    let _ = recv_response_by_id(&mut rx, &register).await;
    let initial = recv_notification_by_method(&mut rx, events::WORKSPACE_PUSH_CHANGED).await;
    let initial: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(initial.params.unwrap()).unwrap();
    assert!(initial.inbox.unwrap().upserts.is_empty());
    let mut delivered = Vec::new();
    for index in 0..31 {
        let run = format!("R{index:020}");
        store.database_connection().execute_unprepared(&format!(
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,completed_at,created_at,updated_at) VALUES('{run}','K0000000000000000000I','{run}',1,{},'succeeded','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", index + 1
        )).await.unwrap();
        processor.dirty_push(&workspace, None, None, Some(&run), false, None);
        let update = recv_notification_by_method(&mut rx, events::WORKSPACE_PUSH_CHANGED).await;
        let update: pioneer_protocol::WorkspacePushNotification =
            serde_json::from_value(update.params.unwrap()).unwrap();
        assert!(!update.resync_required);
        let patch = update.inbox.unwrap();
        assert!(!patch.replace);
        assert_eq!(patch.upserts.len(), 1);
        delivered.extend(patch.upserts);
    }
    processor.dirty_push_inbox(&workspace, None, None);
    let reset = recv_notification_by_method(&mut rx, events::WORKSPACE_PUSH_CHANGED).await;
    let reset: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(reset.params.unwrap()).unwrap();
    let reset = reset.inbox.unwrap();
    assert!(reset.replace);
    assert_eq!(reset.upserts.len(), 31);
    assert_eq!(reset.unread_count, 31);
    assert_eq!(reset.next_cursor, None);

    // A pending broad metadata invalidation precedes the point operation.
    // The same production mutation hold/cache API used by the RPC handler
    // prevents the worker from taking it while the committed receipt is written.
    processor.dirty_push_inbox(&workspace, None, None);
    let mutation = processor
        .begin_push_mutation(connection, &workspace, false)
        .unwrap();
    let run = &delivered[0].run_id;
    let principal = authenticated_test_superuser().principal_id.clone();
    store.database_connection().execute_unprepared(&format!(
        "UPDATE task SET title='After' WHERE id='K0000000000000000000I'; INSERT INTO task_run_read(principal_id,run_id,read_at) VALUES('{}','{run}',CURRENT_TIMESTAMP)", principal.as_str()
    )).await.unwrap();
    mutation.finish(
        None,
        Some(pioneer_protocol::TaskInboxPatch {
            replace: false,
            upserts: vec![],
            removes: vec![run.clone()],
            unread_count: 30,
            next_cursor: None,
        }),
    );
    let update = recv_notification_by_method(&mut rx, events::WORKSPACE_PUSH_CHANGED).await;
    let update: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(update.params.unwrap()).unwrap();
    let update = update.inbox.unwrap();
    assert!(
        update.replace,
        "point ready value must not swallow broad obligation"
    );
    assert_eq!(update.upserts.len(), 30);
    assert_eq!(update.unread_count, 30);
    assert!(
        update
            .upserts
            .iter()
            .all(|entry| entry.title == "After" && entry.run_id != *run)
    );

    // A genuine authoritative replacement covers that same broad obligation.
    processor.dirty_push_inbox(&workspace, None, None);
    let mutation = processor
        .begin_push_mutation(connection, &workspace, false)
        .unwrap();
    // The earlier replacement already removed this identity from the scope.
    // A fresh authoritative replacement carries only the retained membership.
    let mut expected_replacement = update;
    expected_replacement.removes.clear();
    mutation.finish(None, Some(expected_replacement.clone()));
    let replacement = recv_notification_by_method(&mut rx, events::WORKSPACE_PUSH_CHANGED).await;
    let replacement: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(replacement.params.unwrap()).unwrap();
    assert_eq!(replacement.inbox, Some(expected_replacement));

    // Real storage terminal orchestration appends both RunCompleted and its
    // TaskCompleted companion; drive the durable fanout, not hand-written DTOs.
    const TASK: &str = "K0000000000000000000J";
    const RUN: &str = "R0000000000000000000J";
    let now = now_timestamp_secs();
    store.database_connection().execute_unprepared(&format!(
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,executor_kind,status,title,goal,priority,revision,created_at,updated_at) VALUES('{TASK}','{workspace}','workspace','{workspace}','T0000000000000000000I','system','running','Terminal','Goal',0,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,created_at,updated_at) VALUES('{RUN}','{TASK}','{RUN}',1,1,'running','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);"
    )).await.unwrap();
    let actor: pioneer_protocol::TaskActorContract = serde_json::from_value(json!({
        "taskId":TASK,"workspaceId":workspace,"creator":pioneer_protocol::PersistedActorRef::System,
        "reviewer":"runtime_auto","delivery":{"enabled":false,"disclosureGeneration":0},"revision":1
    }))
    .unwrap();
    store.upsert_task_actor_contract(&actor, now).await.unwrap();
    let occurrence: pioneer_protocol::TaskOccurrenceContract = serde_json::from_value(json!({
        "occurrenceId":RUN,"taskId":TASK,"runId":RUN,"occurrenceKey":RUN,
        "executionGeneration":1,"status":"running","retryAttempt":0,"actionIdempotencyKey":RUN
    }))
    .unwrap();
    store
        .upsert_task_occurrence_contract(&occurrence, now)
        .await
        .unwrap();
    let prepared = store
        .prepare_task_terminal_transition(pioneer_protocol::TaskEventPayload::RunCompleted {
            task_id: TASK.into(),
            run_id: RUN.into(),
            result: None,
            completed_at: now,
        })
        .await
        .unwrap();
    let terminal = store
        .commit_task_terminal_transition(prepared)
        .await
        .unwrap();
    assert!(terminal.events.iter().any(|event| matches!(
        event.payload,
        pioneer_protocol::TaskEventPayload::RunCompleted { .. }
    )));
    assert!(terminal.events.iter().any(|event| matches!(
        event.payload,
        pioneer_protocol::TaskEventPayload::TaskCompleted { .. }
    )));
    // Hold one existing scope while the durable dispatcher emits its complete
    // committed batch, making the coalesced publisher pass deterministic.
    let hold = processor
        .begin_push_mutation(connection, &workspace, false)
        .unwrap();
    let summary = processor
        .scoped_for_background_reconciliation()
        .task_event_fanout_quantum()
        .await
        .unwrap();
    assert_eq!(summary.errors, 0);
    assert!(summary.emitted >= 2);
    hold.finish(None, None);
    let terminal = recv_notification_by_method(&mut rx, events::WORKSPACE_PUSH_CHANGED).await;
    let terminal: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(terminal.params.unwrap()).unwrap();
    assert!(!terminal.resync_required);
    assert_eq!(terminal.indicators.len(), 1);
    assert_eq!(terminal.indicators[0].has_running_tasks, Some(false));
    let terminal = terminal.inbox.unwrap();
    assert!(
        !terminal.replace,
        "task lifecycle companion must not reload retained prefix"
    );
    assert_eq!(terminal.upserts.len(), 1);
    assert_eq!(terminal.upserts[0].run_id, RUN);
    assert_eq!(terminal.unread_count, 31);

    // Exercise a frontier established by the existing row/byte cap, rather
    // than by the immutable empty registration. The next reset must use it.
    let large_title = "z".repeat(600);
    store.database_connection().execute_unprepared(&format!(
        "UPDATE task SET title='{large_title}' WHERE id='K0000000000000000000I'; WITH RECURSIVE n(i) AS (SELECT 100 UNION ALL SELECT i+1 FROM n WHERE i<4195) INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,completed_at,created_at,updated_at) SELECT printf('R%020d',i),'K0000000000000000000I',printf('R%020d',i),1,i,'succeeded','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM n"
    )).await.unwrap();
    processor.dirty_push_inbox(&workspace, None, None);
    let capped = recv_notification_by_method(&mut rx, events::WORKSPACE_PUSH_CHANGED).await;
    let capped: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(capped.params.unwrap()).unwrap();
    assert!(!capped.resync_required);
    let capped = capped.inbox.unwrap();
    assert!(capped.replace && !capped.upserts.is_empty() && capped.upserts.len() < 4096);
    let frontier = capped
        .next_cursor
        .clone()
        .expect("byte-capped exact frontier");
    const NEW: &str = "R99999999999999999999";
    store.database_connection().execute_unprepared(&format!(
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,completed_at,created_at,updated_at) VALUES('{NEW}','K0000000000000000000I','{NEW}',1,99999,'succeeded','system',datetime('now','+1 second'),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"
    )).await.unwrap();
    processor.dirty_push(&workspace, None, None, Some(NEW), false, None);
    let added = recv_notification_by_method(&mut rx, events::WORKSPACE_PUSH_CHANGED).await;
    let added: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(added.params.unwrap()).unwrap();
    assert!(!added.inbox.unwrap().replace);
    processor.dirty_push_inbox(&workspace, None, None);
    let retained = recv_notification_by_method(&mut rx, events::WORKSPACE_PUSH_CHANGED).await;
    let retained: pioneer_protocol::WorkspacePushNotification =
        serde_json::from_value(retained.params.unwrap()).unwrap();
    let retained = retained.inbox.unwrap();
    assert_eq!(retained.upserts.len(), capped.upserts.len() + 1);
    assert_eq!(retained.next_cursor, Some(frontier));
    assert_eq!(retained.unread_count, capped.unread_count + 1);
    assert!(retained.upserts.iter().any(|entry| entry.run_id == NEW));
    sessions.unregister_connection(connection).await;
}
