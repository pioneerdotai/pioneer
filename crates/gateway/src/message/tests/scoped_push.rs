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
    let metadata = observer_rx.recv().await.expect("canonical metadata frame");
    let Message::Text(text) = metadata else {
        panic!("expected JSON metadata");
    };
    let notification: pioneer_protocol::JsonRpcNotification = serde_json::from_str(&text).unwrap();
    let pioneer_protocol::GatewayNotification::ThreadUpdated(metadata) =
        pioneer_protocol::GatewayNotification::from_jsonrpc(notification).unwrap()
    else {
        panic!("expected point metadata, not tree refresh");
    };
    assert_eq!(metadata.thread.preview, "question");
    assert_eq!(metadata.thread.id, THREAD);
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
    assert_eq!(packet.revision, 2);
    assert!(!packet.snapshot && !packet.resync_required);
    assert_eq!(packet.indicators.len(), 1);
    assert_eq!(packet.indicators[0].thread_id, THREAD);
    assert_eq!(packet.indicators[0].unread_count, Some(1));
    assert_eq!(packet.indicators[0].has_running_tasks, None);
    assert!(packet.inbox.is_none());

    let read = generate_test_request_id("pushtest", "read");
    processor
        .process_request_for_connection(
            observer,
            &json!({
                "jsonrpc":"2.0", "id":read, "method":"thread/read",
                "params":{"thread_id":THREAD,"through_turn_id":TURN}
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
