use pioneer_crud_execution::pending_requests::{
    CliRuntimePendingRequestListFilter, CliRuntimePendingRequestStatus,
    CliRuntimeRequestAuthorizationBinding, NewCliRuntimePendingRequest,
    ResolveCliRuntimePendingRequest, bind_pending_request_authorization, create_pending_request,
    ensure_pending_request_capacity, list_open_pending_requests_for_descendant_threads,
    list_pending_requests, open_pending_request, resolve_pending_request,
};

use sea_orm::{DatabaseConnection, EntityTrait, Set, TransactionTrait};

#[tokio::test]
async fn cli_runtime_pending_request_resolution_rejects_second_answer() {
    pioneer_postgres_test::with_database(async |fixture| {
        let writer = fixture.connection();
        let created_at = chrono::DateTime::from_timestamp_secs(1_700_020_000)
            .unwrap()
            .fixed_offset();
        let updated_at = chrono::DateTime::from_timestamp_secs(1_700_020_010)
            .unwrap()
            .fixed_offset();
        let payload_json = serde_json::to_string(&serde_json::json!({
            "command": "cargo test",
            "cwd": "/tmp/project"
        }))
        .expect("payload json should serialize");

        let pending = create_pending_request(
            writer,
            NewCliRuntimePendingRequest {
                request_id: "approval-request-a".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                workspace_id: "ws_cli_pending".to_owned(),
                thread_id: "thread_cli_pending".to_owned(),
                turn_id: Some("turn_cli_pending".to_owned()),
                native_thread_id: Some("codex-thread-pending".to_owned()),
                native_turn_id: Some("codex-turn-pending".to_owned()),
                native_item_id: Some("item-1".to_owned()),
                request_kind: "command_approval".to_owned(),
                payload_json: payload_json.clone(),
                created_at,
                updated_at: created_at,
            },
        )
        .await
        .expect("pending request create should succeed");
        assert_eq!(pending.status, CliRuntimePendingRequestStatus::Pending);

        let duplicate_pending = create_pending_request(
            writer,
            NewCliRuntimePendingRequest {
                request_id: "approval-request-a".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                workspace_id: "ws_cli_pending".to_owned(),
                thread_id: "thread_cli_pending".to_owned(),
                turn_id: Some("turn_cli_pending".to_owned()),
                native_thread_id: Some("codex-thread-pending".to_owned()),
                native_turn_id: Some("codex-turn-pending".to_owned()),
                native_item_id: Some("item-1".to_owned()),
                request_kind: "command_approval".to_owned(),
                payload_json: serde_json::to_string(&serde_json::json!({
                    "command": "cargo check",
                    "cwd": "/tmp/project"
                }))
                .expect("duplicate payload should serialize"),
                created_at,
                updated_at,
            },
        )
        .await
        .expect("duplicate pending request should update metadata");
        assert_eq!(duplicate_pending.created_at, created_at);
        assert_eq!(duplicate_pending.updated_at, updated_at);
        assert!(duplicate_pending.payload_json.contains("cargo check"));

        let response_json = r#"{ "approved":true, "scope":"once", "note":"\u0000" }"#.to_owned();
        let answered = resolve_pending_request(
            writer,
            ResolveCliRuntimePendingRequest {
                request_id: "approval-request-a".to_owned(),
                status: CliRuntimePendingRequestStatus::Answered,
                response_json: Some(response_json.clone()),
                updated_at,
                resolved_at: updated_at,
            },
        )
        .await
        .expect("pending request answer should succeed")
        .expect("pending request should exist");
        assert_eq!(answered.status, CliRuntimePendingRequestStatus::Answered);
        assert_eq!(
            answered.response_json.as_deref(),
            Some(response_json.as_str())
        );

        let second_answer = resolve_pending_request(
            writer,
            ResolveCliRuntimePendingRequest {
                request_id: "approval-request-a".to_owned(),
                status: CliRuntimePendingRequestStatus::Answered,
                response_json: Some(response_json),
                updated_at,
                resolved_at: updated_at,
            },
        )
        .await;
        assert!(
            second_answer.is_err(),
            "answered pending request must reject a second answer"
        );

        let answered_rows = list_pending_requests(
            writer,
            CliRuntimePendingRequestListFilter {
                workspace_id: Some("ws_cli_pending".to_owned()),
                runtime_id: Some("codex".to_owned()),
                status: Some(CliRuntimePendingRequestStatus::Answered),
                ..Default::default()
            },
        )
        .await
        .expect("answered request list should succeed");
        assert_eq!(answered_rows.len(), 1);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn authorized_pending_request_capacity_is_atomic_under_concurrency() {
    pioneer_postgres_test::with_database(async |fixture| {
        let writer = fixture.connection();
        let other = fixture.connect().await?;
        let created_at = chrono::DateTime::from_timestamp_secs(1_700_020_400)
            .unwrap()
            .fixed_offset();
        let request = |request_id: &str| NewCliRuntimePendingRequest {
            request_id: request_id.to_owned(),
            runtime_id: "codex".to_owned(),
            runtime_kind: "codex".to_owned(),
            workspace_id: "ws_cli_pending_capacity".to_owned(),
            thread_id: "thread_cli_pending_capacity".to_owned(),
            turn_id: Some("turn_cli_pending_capacity".to_owned()),
            native_thread_id: Some("codex-thread-capacity".to_owned()),
            native_turn_id: Some("codex-turn-capacity".to_owned()),
            native_item_id: Some(request_id.to_owned()),
            request_kind: "command_approval".to_owned(),
            payload_json: "{}".to_owned(),
            created_at,
            updated_at: created_at,
        };
        let authorization = CliRuntimeRequestAuthorizationBinding {
            initiating_principal_id: "P00000000000000000001".to_owned(),
            initiating_session_id: "S00000000000000000001".to_owned(),
            initiating_session_generation: 0,
            authorization_context_fingerprint: "f".repeat(64),
        };

        let open = async |db: &DatabaseConnection, request: NewCliRuntimePendingRequest| {
            let transaction = db.begin().await?;
            let result = async {
                ensure_pending_request_capacity(
                    &transaction,
                    &request.request_id,
                    request.turn_id.as_deref().unwrap(),
                    1,
                )
                .await?;
                let opened = open_pending_request(&transaction, request).await?;
                bind_pending_request_authorization(
                    &transaction,
                    &opened.request_id,
                    &authorization,
                )
                .await?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if result.is_err() {
                transaction.rollback().await?;
            } else {
                transaction.commit().await?;
            }
            result
        };
        let first = open(writer, request("capacity-request-a"));
        let second = open(&other, request("capacity-request-b"));
        let (first, second) = tokio::join!(first, second);
        assert_ne!(
            first.is_ok(),
            second.is_ok(),
            "exactly one concurrent request may reserve the final interaction slot"
        );
        let error = first.and(second).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("maximum 1 pending human interactions")
        );
        let open = list_pending_requests(
            writer,
            CliRuntimePendingRequestListFilter {
                turn_id: Some("turn_cli_pending_capacity".to_owned()),
                open_only: true,
                limit: Some(8),
                ..Default::default()
            },
        )
        .await
        .expect("bounded pending request list should succeed");
        assert_eq!(open.len(), 1);
        other.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn descendant_pending_requests_are_loaded_by_one_bounded_subtree_query() {
    pioneer_postgres_test::with_database(async |fixture| {
        let writer = fixture.connection();
        let created_at = chrono::DateTime::from_timestamp_secs(1_700_020_200)
            .unwrap()
            .fixed_offset();
        let lineage_rows = [
            ("child_a", "root_a", 1),
            ("grandchild_a", "child_a", 2),
            ("unrelated_child", "unrelated_root", 1),
        ]
        .into_iter()
        .map(|(child_thread_id, parent_thread_id, depth)| {
            pioneer_entity::thread_lineage::ActiveModel {
                child_thread_id: Set(child_thread_id.to_owned()),
                parent_thread_id: Set(parent_thread_id.to_owned()),
                root_thread_id: Set(if child_thread_id == "unrelated_child" {
                    "unrelated_root".to_owned()
                } else {
                    "root_a".to_owned()
                }),
                depth: Set(depth),
                created_at: Set(created_at),
                origin_kind: Set(None),
                created_by_thread_id: Set(None),
                created_by_turn_id: Set(None),
                ..Default::default()
            }
        })
        .collect::<Vec<_>>();
        pioneer_entity::thread_lineage::Entity::insert_many(lineage_rows)
            .exec(writer)
            .await
            .expect("lineage fixtures should insert");

        let request =
            |request_id: &str, workspace_id: &str, thread_id: &str| NewCliRuntimePendingRequest {
                request_id: request_id.to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: Some(format!("turn_{thread_id}")),
                native_thread_id: None,
                native_turn_id: None,
                native_item_id: None,
                request_kind: "command_approval".to_owned(),
                payload_json: "{}".to_owned(),
                created_at,
                updated_at: created_at,
            };
        for pending in [
            request("request_child", "ws_descendant_pending", "child_a"),
            request(
                "request_grandchild",
                "ws_descendant_pending",
                "grandchild_a",
            ),
            request(
                "request_unrelated",
                "ws_descendant_pending",
                "unrelated_child",
            ),
            request("request_other_workspace", "ws_other", "child_a"),
        ] {
            create_pending_request(writer, pending)
                .await
                .expect("pending request fixture should insert");
        }

        let root_requests = list_open_pending_requests_for_descendant_threads(
            writer,
            "ws_descendant_pending",
            "root_a",
        )
        .await
        .expect("root subtree query should succeed");
        assert_eq!(
            root_requests
                .iter()
                .map(|request| request.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["request_child", "request_grandchild"]
        );

        let child_requests = list_open_pending_requests_for_descendant_threads(
            writer,
            "ws_descendant_pending",
            "child_a",
        )
        .await
        .expect("nested subtree query should succeed");
        assert_eq!(
            child_requests
                .iter()
                .map(|request| request.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["request_grandchild"]
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cli_runtime_pending_request_cancel_and_expire_helpers_are_terminal() {
    use pioneer_crud_execution::native_events::serialize_cli_runtime_json;
    use pioneer_crud_execution::pending_requests::{
        cancel_pending_request, expire_pending_request,
    };

    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();
        let created_at = pioneer_crud_execution::time::unix_to_datetime(1_700_021_000);
        let updated_at = pioneer_crud_execution::time::unix_to_datetime(1_700_021_030);
        let request_payload = serialize_cli_runtime_json(&serde_json::json!({
            "kind": "user_input",
            "title": "Need input",
            "payload": { "prompt": "Continue?" }
        }))
        .expect("request payload should serialize");

        for request_id in ["approval-cancel-a", "approval-expire-a"] {
            open_pending_request(
                db,
                NewCliRuntimePendingRequest {
                    request_id: request_id.to_owned(),
                    runtime_id: "codex".to_owned(),
                    runtime_kind: "codex".to_owned(),
                    workspace_id: "ws_cli_pending_terminal".to_owned(),
                    thread_id: "thread_cli_pending_terminal".to_owned(),
                    turn_id: Some("turn_cli_pending_terminal".to_owned()),
                    native_thread_id: Some("codex-thread-terminal".to_owned()),
                    native_turn_id: Some("codex-turn-terminal".to_owned()),
                    native_item_id: Some("item-terminal".to_owned()),
                    request_kind: "user_input".to_owned(),
                    payload_json: request_payload.clone(),
                    created_at,
                    updated_at: created_at,
                },
            )
            .await
            .expect("pending request should open");
        }

        let cancelled = cancel_pending_request(
            db,
            "approval-cancel-a".to_owned(),
            Some(
                serialize_cli_runtime_json(&serde_json::json!({
                    "status": "cancelled"
                }))
                .expect("cancel payload should serialize"),
            ),
            updated_at,
        )
        .await
        .expect("cancel should succeed")
        .expect("cancelled request should exist");
        assert_eq!(cancelled.status, CliRuntimePendingRequestStatus::Cancelled);
        assert_eq!(cancelled.resolved_at, Some(updated_at));

        let second_cancel =
            cancel_pending_request(db, "approval-cancel-a".to_owned(), None, updated_at).await;
        assert!(
            second_cancel.is_err(),
            "cancelled request must reject a second terminal transition"
        );

        let expired = expire_pending_request(db, "approval-expire-a".to_owned(), None, updated_at)
            .await
            .expect("expire should succeed")
            .expect("expired request should exist");
        assert_eq!(expired.status, CliRuntimePendingRequestStatus::Expired);

        let missing = expire_pending_request(db, "approval-missing-a".to_owned(), None, updated_at)
            .await
            .expect("missing expire should not fail");
        assert!(missing.is_none());
        Ok(())
    })
    .await
    .unwrap();
}
