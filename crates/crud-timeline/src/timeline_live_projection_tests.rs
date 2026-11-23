use super::project_semantic_timeline_live_turn_event;
use pioneer_crud_conversations::{
    events::TurnEventPayload,
    turn_item::{prepare_turn_item_projection, upsert_prepared_turn_item},
};
use pioneer_crud_execution::{
    event_append::{PreparedTurnEvent, append_prepared_event},
    time::unix_to_datetime,
    turn_item_terminal::terminal_turn_item_status_from_payload,
};
use pioneer_postgres_pools::PostgresDatabase;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

async fn publish_item(
    db: &PostgresDatabase,
    payload: TurnEventPayload,
    at: i64,
) -> anyhow::Result<()> {
    let created_at = unix_to_datetime(at);
    let (item, status) = match &payload {
        TurnEventPayload::ItemStarted(event) => (&event.item, "in_progress"),
        TurnEventPayload::ItemCompleted(event) => (
            &event.item,
            terminal_turn_item_status_from_payload(&event.item),
        ),
        _ => unreachable!("fixture accepts only item lifecycle events"),
    };
    let item_projection = prepare_turn_item_projection(
        payload.turn_id(),
        item,
        Some(status),
        created_at,
        created_at,
    )?;
    let timeline_item = serde_json::to_value(item)?;
    let prepared = PreparedTurnEvent::prepare_for_storage(payload)?;
    let tx = db.begin().await?;
    let event = append_prepared_event(&tx, prepared, created_at).await?;
    upsert_prepared_turn_item(&tx, item_projection).await?;
    project_semantic_timeline_live_turn_event(&tx, &event, Some(timeline_item)).await?;
    tx.commit().await?;
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn compaction_timeline_persists_explicit_terminal_status_with_one_item() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = PostgresDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        fixture.connection().clone(),
    );
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    for terminal in ["completed", "failed", "cancelled"] {
        let id = format!("compaction:{terminal}");
        let item = |status: &str| pioneer_protocol::TurnItem::SystemEvent {
            id: id.clone(),
            level: pioneer_protocol::SystemEventLevel::Info,
            message: status.into(),
            code: Some("agent_context_compaction".into()),
            details: Some(serde_json::json!({"status":status})),
        };
        publish_item(
            &db,
            TurnEventPayload::ItemStarted(pioneer_protocol::ItemStartedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: item("started"),
            }),
            1,
        )
        .await
        .unwrap();
        let query = || {
            Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT status FROM turn_work_item_projection WHERE item_id=$1",
                vec![id.clone().into()],
            )
        };
        let row = db.query_one_raw(query()).await.unwrap().unwrap();
        assert_eq!(row.try_get::<String>("", "status").unwrap(), "running");
        for _ in 0..2 {
            publish_item(
                &db,
                TurnEventPayload::ItemCompleted(pioneer_protocol::ItemCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item: item(terminal),
                }),
                2,
            )
            .await
            .unwrap();
        }
        let rows = db.query_all_raw(query()).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].try_get::<String>("", "status").unwrap(), terminal);
    }
}

#[tokio::test]
async fn timeline_approval_action_filters_before_pagination_and_anchor_lookup() {
    use pioneer_crud_conversations::thread_timeline_projection::{
        BLOCK_KIND_APPROVAL, BLOCK_KIND_USER_MESSAGE, ProjectionPageAnchor,
        ThreadTimelineApprovalScope, ThreadTimelineBlockRecord,
        find_thread_timeline_block_by_sort_key, list_thread_timeline_blocks_page,
        upsert_thread_timeline_block,
    };
    use pioneer_crud_execution::cli_runtime_binding::{
        CliRuntimeRequestAuthorizationBinding, NewCliRuntimePendingRequest,
        bind_pending_request_authorization, open_pending_request,
    };

    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_timeline_approval";
        let thread_id = "T".repeat(21);
        let owner_principal_id = "P".repeat(21);
        let owner_session_id = "S".repeat(21);
        let other_principal_id = "Q".repeat(21);
        let other_session_id = "R".repeat(21);
        let db = PostgresDatabase::new(
            fixture.connect_read_only().await?,
            fixture.connection().clone(),
        );
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES ($1,'Test Workspace',TRUE,FALSE)",
            [workspace_id.into()],
        )).await?;
        let created_at = unix_to_datetime(1_700_020_500);

        for (request_id, principal_id, session_id) in [
            (
                "approval-request-other",
                other_principal_id.as_str(),
                other_session_id.as_str(),
            ),
            (
                "approval-request-owner",
                owner_principal_id.as_str(),
                owner_session_id.as_str(),
            ),
        ] {
            let tx = db.begin().await?;
            let opened = open_pending_request(
                    &tx,
                    NewCliRuntimePendingRequest {
                        request_id: request_id.to_owned(),
                        runtime_id: "codex".to_owned(),
                        runtime_kind: "codex".to_owned(),
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.clone(),
                        turn_id: None,
                        native_thread_id: None,
                        native_turn_id: None,
                        native_item_id: None,
                        request_kind: "command_approval".to_owned(),
                        payload_json: "{}".to_owned(),
                        created_at,
                        updated_at: created_at,
                    },
                ).await?;
            let request = bind_pending_request_authorization(
                    &tx, &opened.request_id,
                    &CliRuntimeRequestAuthorizationBinding {
                        initiating_principal_id: principal_id.to_owned(),
                        initiating_session_id: session_id.to_owned(),
                        initiating_session_generation: 0,
                        authorization_context_fingerprint: "a".repeat(64),
                    },
                ).await?;
            super::project_cli_runtime_pending_request(&tx, &request).await?;
            tx.commit().await?;
        }

        for (block_id, block_kind, source_key, sort_key) in [
            (
                "approval-block-other",
                BLOCK_KIND_APPROVAL,
                Some("approval-request-other"),
                "000-other",
            ),
            (
                "ordinary-shared-block",
                BLOCK_KIND_USER_MESSAGE,
                None,
                "001-shared",
            ),
            (
                "approval-block-owner",
                BLOCK_KIND_APPROVAL,
                Some("approval-request-owner"),
                "002-owner",
            ),
        ] {
            upsert_thread_timeline_block(
                &db,
                ThreadTimelineBlockRecord {
                    block_id: block_id.to_owned(),
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.clone(),
                    turn_id: None,
                    block_kind: block_kind.to_owned(),
                    sort_key: sort_key.to_owned(),
                    source_kind: source_key.map(|_| "cli_runtime_pending_request".to_owned()),
                    source_key: source_key.map(str::to_owned),
                    started_at: Some(created_at),
                    completed_at: None,
                    metadata_json: "{}".to_owned(),
                    created_at,
                    updated_at: created_at,
                },
            )
            .await
            .expect("timeline block should persist");
        }

        let observer_scope = ThreadTimelineApprovalScope {
            can_observe_agent_requests: true,
        };
        let observer_page = list_thread_timeline_blocks_page(
                &db,
                thread_id.as_str(),
                Some(&observer_scope),
                ProjectionPageAnchor::Start,
                2,
            )
            .await
            .expect("observer timeline page should load");
        assert_eq!(
            observer_page
                .into_iter()
                .map(|row| row.block_id)
                .collect::<Vec<_>>(),
            vec![
                "approval-block-other".to_owned(),
                "ordinary-shared-block".to_owned(),
            ],
            "a collaborator with AgentRequestObserve sees approvals from every initiator"
        );
        assert!(
            find_thread_timeline_block_by_sort_key(
                    &db,
                    thread_id.as_str(),
                    Some(&observer_scope),
                    "000-other",
                )
                .await
                .expect("collaborative approval anchor lookup should succeed")
                .is_some(),
            "an approval from another initiator remains part of the shared capsule"
        );

        let reader_scope = ThreadTimelineApprovalScope {
            can_observe_agent_requests: false,
        };
        let reader_page = list_thread_timeline_blocks_page(
                &db,
                thread_id.as_str(),
                Some(&reader_scope),
                ProjectionPageAnchor::Start,
                2,
            )
            .await
            .expect("narrow reader timeline page should load");
        assert_eq!(
            reader_page
                .into_iter()
                .map(|row| row.block_id)
                .collect::<Vec<_>>(),
            vec!["ordinary-shared-block".to_owned()],
            "approval rows must be removed in SQL before LIMIT is applied"
        );
        assert!(
            find_thread_timeline_block_by_sort_key(
                    &db,
                    thread_id.as_str(),
                    Some(&reader_scope),
                    "002-owner",
                )
                .await
                .expect("narrow reader anchor lookup should succeed")
                .is_none(),
            "a role without AgentRequestObserve cannot use an approval as a cursor anchor"
        );
        Ok(())
    })
    .await
    .unwrap();
}
