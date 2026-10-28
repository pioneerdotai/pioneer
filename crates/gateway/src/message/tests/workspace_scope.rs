//! Explicit resource reads must not become connection selection transitions.
use super::*;

async fn rpc(
    harness: &mut SemanticTimelineQueryHarness,
    method: &str,
    params: JsonValue,
) -> JsonValue {
    let id = generate_test_request_id("scope", method);
    harness
        .processor
        .process_request_for_connection(
            harness.connection_id,
            &json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params}).to_string(),
        )
        .await;
    serde_json::from_str(&recv_jsonrpc_payload_by_id(&mut harness.rx, &id).await).unwrap()
}

#[tokio::test]
async fn explicit_resource_reads_preserve_selection_including_empty_tombstone_and_errors() {
    let mut harness = setup_semantic_timeline_query_harness().await;
    let workspace_a = harness
        .processor
        .crud_store
        .get_turn_work_projection(&harness.sandbox_turn_id)
        .await
        .unwrap()
        .unwrap()
        .workspace_id;
    let workspace_b = harness
        .processor
        .workspace_manager
        .create_workspace("scope_selected_b", Some("Selected B"))
        .await
        .unwrap()
        .id;
    let empty_workspace = harness
        .processor
        .workspace_manager
        .create_workspace("scope_empty", Some("Empty resource"))
        .await
        .unwrap()
        .id;
    let catalog_before = harness
        .processor
        .workspace_manager
        .list_workspaces()
        .await
        .unwrap();
    // Selection is an explicit, authorized transition. Reads of A happen after B.
    for workspace in [&workspace_a, &workspace_b] {
        let selected = rpc(
            &mut harness,
            "workspace/select",
            json!({"workspace_id":workspace,"make_current":false}),
        )
        .await;
        assert_eq!(selected["result"]["workspace"]["id"], workspace.as_str());
    }
    let thread = harness.sandbox_thread_id.clone();
    let turn = harness.sandbox_turn_id.clone();
    let item = pioneer_crud::work_item_projection_id(&turn, "semantic_cmd_00");
    let cases = [
        (
            "thread/tree",
            json!({"workspace_id":workspace_a}),
            "threads",
        ),
        (
            "thread/timeline/page",
            json!({"threadId":thread,"anchor":{"kind":"newest"},"limit":10}),
            "blocks",
        ),
        (
            "turn/work/page",
            json!({"threadId":thread,"turnId":turn,"anchor":{"kind":"oldest"},"limit":5}),
            "items",
        ),
        (
            "turn/work/items/get",
            json!({"threadId":thread,"turnId":turn,"workItemIds":[item]}),
            "items",
        ),
        (
            "turn/work/items/get",
            json!({"threadId":thread,"turnId":turn,"workItemIds":["absent"]}),
            "removedWorkItemIds",
        ),
        (
            "thread/tree",
            json!({"workspace_id":empty_workspace}),
            "threads",
        ),
    ];
    for (index, (method, params, field)) in cases.into_iter().enumerate() {
        let response = rpc(&mut harness, method, params).await;
        assert!(response.get("error").is_none(), "{method}: {response}");
        let rows = response["result"][field].as_array().expect("resource rows");
        assert_eq!(rows.is_empty(), index == 5, "{method}: {response}");
        assert_preserved(&harness, &workspace_b, &catalog_before).await;
    }
    let newest = rpc(
        &mut harness,
        "turn/work/page",
        json!({"threadId":thread,"turnId":turn,"anchor":{"kind":"newest"},"limit":5}),
    )
    .await;
    let cursor = newest["result"]["page"]["afterCursor"].clone();
    assert!(cursor["value"].is_string(), "last work cursor");
    let empty_work = rpc(&mut harness, "turn/work/page",
        json!({"threadId":thread,"turnId":turn,"anchor":{"kind":"after","cursor":cursor},"limit":5})).await;
    assert!(empty_work.get("error").is_none(), "{empty_work}");
    assert!(empty_work["result"]["work"].is_object());
    assert_eq!(empty_work["result"]["items"], json!([]));
    assert_preserved(&harness, &workspace_b, &catalog_before).await;
    // Reuse the projection reset fixture: authoritative thread/turn still exist,
    // but the canonical timeline is empty and the work page is a tombstone.
    let transaction = harness
        .processor
        .crud_store
        .database_connection()
        .begin()
        .await
        .unwrap();
    clear_synthetic_semantic_projection(&transaction, &thread).await;
    transaction.commit().await.unwrap();
    let timeline = rpc(
        &mut harness,
        "thread/timeline/page",
        json!({"threadId":thread,"anchor":{"kind":"newest"},"limit":10}),
    )
    .await;
    assert_eq!(timeline["result"]["blocks"], json!([]));
    assert_preserved(&harness, &workspace_b, &catalog_before).await;
    let tombstone = rpc(
        &mut harness,
        "turn/work/page",
        json!({"threadId":thread,"turnId":turn,"anchor":{"kind":"newest"},"limit":10}),
    )
    .await;
    assert!(tombstone.get("error").is_none(), "{tombstone}");
    assert_eq!(tombstone["result"]["work"], JsonValue::Null);
    assert_eq!(tombstone["result"]["items"], json!([]));
    assert_preserved(&harness, &workspace_b, &catalog_before).await;
    for (method, params) in [
        ("thread/tree", json!({"workspace_id":"missing"})),
        (
            "thread/timeline/page",
            json!({"threadId":thread,"anchor":{"kind":"newest"},"limit":0}),
        ),
        (
            "turn/work/page",
            json!({"threadId":thread,"turnId":turn,"anchor":{"kind":"newest"},"limit":0}),
        ),
        (
            "turn/work/items/get",
            json!({"threadId":thread,"turnId":turn,"workItemIds":["absent"]}),
        ),
    ] {
        let response = rpc(&mut harness, method, params).await;
        assert!(response.get("error").is_some(), "{method}: {response}");
        assert_preserved(&harness, &workspace_b, &catalog_before).await;
    }
}

async fn assert_preserved(
    harness: &SemanticTimelineQueryHarness,
    workspace: &str,
    catalog: &[pioneer_protocol::Workspace],
) {
    assert_eq!(
        harness
            .processor
            .session_manager
            .connection_workspace_id(harness.connection_id)
            .await
            .as_deref(),
        Some(workspace)
    );
    assert_eq!(
        harness
            .processor
            .workspace_manager
            .list_workspaces()
            .await
            .unwrap(),
        catalog
    );
}

// Exercise the same typed workflows used by desktop and the thin FFI boundary
// against the real dispatcher, without giving the test transport selection logic.
#[tokio::test]
async fn shared_workspace_transitions_establish_connection_scope_without_global_current() {
    struct Requests(
        tokio::sync::mpsc::Sender<(String, pioneer_client::rpc::JsonRpcResponseSender)>,
    );
    impl pioneer_client::rpc::JsonRpcRequestTransport for Requests {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: pioneer_client::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            self.0
                .try_send((payload, response))
                .map_err(|error| error.to_string())
        }
    }
    let mut harness = setup_semantic_timeline_query_harness().await;
    let workspace_a = harness
        .processor
        .crud_store
        .get_turn_work_projection(&harness.sandbox_turn_id)
        .await
        .unwrap()
        .unwrap()
        .workspace_id;
    let workspace_b = harness
        .processor
        .workspace_manager
        .create_workspace("scope_shared_b", Some("Selected B"))
        .await
        .unwrap()
        .id;
    let catalog_before = harness
        .processor
        .workspace_manager
        .list_workspaces()
        .await
        .unwrap();
    let (sender, mut requests) = tokio::sync::mpsc::channel(4);
    let selected = workspace_b.clone();
    let workflow = tokio::task::spawn_blocking(move || {
        let transport = Requests(sender);
        let bootstrap = pioneer_client::workspaces::bootstrap::bootstrap_workspace_catalog(
            &transport,
            pioneer_client::workspaces::bootstrap::WorkspaceBootstrapRequest {
                persisted_workspace_id: Some(workspace_a.clone()),
            },
        )
        .unwrap();
        assert_eq!(bootstrap.selected.workspace_id, workspace_a);
        let switched = pioneer_client::workspaces::commands::switch_workspace(
            &transport,
            pioneer_client::workspaces::commands::WorkspaceSwitchRequest {
                workspace_id: selected.clone(),
                current_workspace_id: Some(workspace_a),
                workspaces: bootstrap.workspaces,
                action_in_progress: false,
            },
        )
        .unwrap();
        let pioneer_client::workspaces::commands::WorkspaceSwitchResult::Switched { reduction } =
            switched
        else {
            panic!("shared user transition must select B");
        };
        assert_eq!(reduction.selected.workspace_id, selected);
    });
    while let Some((payload, response)) = requests.recv().await {
        let request: JsonValue = serde_json::from_str(&payload).unwrap();
        harness
            .processor
            .process_request_for_connection(harness.connection_id, &payload)
            .await;
        let reply =
            recv_jsonrpc_payload_by_id(&mut harness.rx, request["id"].as_str().unwrap()).await;
        let reply: JsonRpcResponse =
            serde_json::from_str(&reply).expect("authorized workspace response");
        response.send(Ok(reply.result)).unwrap();
    }
    workflow.await.unwrap();
    assert_preserved(&harness, &workspace_b, &catalog_before).await;
}
