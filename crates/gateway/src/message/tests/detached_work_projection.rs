//! Detached Composer parents retain messages/task cards without owning Work.
use super::*;
use pioneer_client::timeline::semantic::{
    SemanticTimelineState, TopLevelPageMergeMode, apply_thread_timeline_blocks_changed,
    apply_thread_timeline_page, apply_turn_work_items_changed, apply_turn_work_items_get_response,
};

async fn detached_parent(case: &str) -> (tempfile::TempDir, LiveSemanticTimelineHarness) {
    let (directory, workspace_manager, crud_store, workspace_id) =
        setup_pooled_file_workspace_manager().await;
    // Match Gateway's serialized FK OFF writer and query-only reader pool.
    crud_store
        .database_connection()
        .execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    let mut harness = setup_live_semantic_timeline_harness_on_store(
        case,
        generate_test_request_id("detachedthread", case),
        (workspace_manager, crud_store, workspace_id),
    )
    .await;
    let store = &harness.processor.crud_store;
    let thread = store
        .get_thread_model(&harness.thread_id)
        .await
        .unwrap()
        .unwrap();
    let (_, mut turn) = store
        .get_turn(&harness.thread_id, &harness.turn_id)
        .await
        .unwrap()
        .unwrap();
    turn.id = format!("{}_parent", harness.turn_id);
    // Admit the immutable owner through the real canonical projection path;
    // never retrofit ownership on an ordinary Turn to manufacture absence.
    store
        .materialize_native_agent_turn_event(
            pioneer_crud::CanonicalTurnEventPayload::TurnStarted(
                pioneer_crud::CanonicalTurnStartedEventPayload {
                    thread: thread.clone(),
                    sandbox_mode: SandboxMode::FullAccess,
                    turn: turn.clone(),
                    input: vec![UserInput::Text {
                        text: "Parent message".into(),
                        text_elements: Vec::new(),
                    }],
                    actor: Some(pioneer_protocol::PersistedActorRef::Principal(
                        authenticated_test_superuser().principal_id.clone(),
                    )),
                    reasoning_effort: None,
                    work_owner: pioneer_crud::TurnWorkOwner::DetachedTask,
                },
            ),
            thread.updated_at,
            None,
        )
        .await
        .unwrap();
    harness.turn_id = turn.id;
    while harness.rx.try_recv().is_ok() {}
    (directory, harness)
}

async fn rpc(
    harness: &mut LiveSemanticTimelineHarness,
    method: &str,
    params: JsonValue,
) -> JsonValue {
    let id = generate_test_request_id("detached-work", method);
    harness
        .processor
        .process_request_for_connection(
            harness.connection_id,
            &json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params}).to_string(),
        )
        .await;
    serde_json::from_str(&recv_jsonrpc_payload_by_id(&mut harness.rx, &id).await).unwrap()
}

fn items_params(harness: &LiveSemanticTimelineHarness) -> JsonValue {
    json!({"threadId":harness.thread_id, "turnId":harness.turn_id,
        "workItemIds":["stale-work", "stale-work", "missing-work"]})
}

#[tokio::test]
async fn detached_parent_notifications_and_late_items_reads_preserve_authoritative_deletion() {
    let (_directory, mut harness) = detached_parent("owned_absence").await;
    let store = harness.processor.crud_store.clone();
    let now = super::super::now_timestamp_secs();
    let thread = store
        .get_thread_model(&harness.thread_id)
        .await
        .unwrap()
        .unwrap();
    let (_, mut occurrence) = store
        .get_turn(&harness.thread_id, &harness.turn_id)
        .await
        .unwrap()
        .unwrap();
    // The task card belongs to its occurrence Turn in the presentation thread;
    // the Composer parent remains a Conversation with DetachedTask ownership.
    occurrence.id = "detached-run".into();
    occurrence.turn_kind = TurnKind::TaskRun;
    occurrence.origin = TurnOrigin::DetachedTask;
    store
        .materialize_native_agent_turn_event(
            pioneer_crud::CanonicalTurnEventPayload::TurnStarted(
                pioneer_crud::CanonicalTurnStartedEventPayload {
                    thread: thread.clone(),
                    sandbox_mode: SandboxMode::FullAccess,
                    turn: occurrence.clone(),
                    input: Vec::new(),
                    actor: Some(pioneer_protocol::PersistedActorRef::Principal(
                        authenticated_test_superuser().principal_id.clone(),
                    )),
                    reasoning_effort: None,
                    work_owner: pioneer_crud::TurnWorkOwner::Turn,
                },
            ),
            thread.updated_at,
            None,
        )
        .await
        .unwrap();
    let items = [
        TurnItem::SystemEvent {
            id: "parent-diagnostic".into(),
            level: pioneer_protocol::SystemEventLevel::Error,
            message: "Wrapper diagnostic".into(),
            code: Some("task_delivery_failed".into()),
            details: None,
        },
        TurnItem::AgentMessage {
            id: "parent-final".into(),
            text: "Task created".into(),
            phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        },
        TurnItem::Task {
            item: TaskTurnItem {
                id: "detached-task-anchor".into(),
                task_id: "detached-task".into(),
                created_by_turn_id: Some(harness.turn_id.clone()),
                run_id: Some(occurrence.id.clone()),
                parent_task_id: None,
                root_task_id: None,
                title: "Detached execution".into(),
                status: TaskStatus::Running,
                attachment: TaskAttachmentMode::Detached,
                trigger_kind: TaskTriggerKind::Immediate,
                executor_kind: TaskExecutorKind::Agent,
                child_thread_id: Some("child-thread".into()),
                child_turn_id: Some("child-turn".into()),
                agent_role: None,
                depth: 0,
                max_depth: 3,
                next_fire_at: None,
                progress_preview: None,
                result_preview: None,
                error_preview: None,
                started_at: Some(now),
                created_at: now,
                updated_at: now,
            },
        },
    ];
    let work_id = pioneer_crud::work_block_id(&harness.turn_id);
    let mut tombstones = Vec::new();
    let mut changed_blocks = HashSet::new();
    for (offset, item) in items.iter().enumerate() {
        let item_turn_id = if matches!(item, TurnItem::Task { .. }) {
            &occurrence.id
        } else {
            &harness.turn_id
        };
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: harness.workspace_id.clone(),
                    thread_id: harness.thread_id.clone(),
                    turn_id: item_turn_id.clone(),
                    item: item.clone(),
                },
                now + offset as i64,
            )
            .await
            .unwrap();
        // Replay delivery after projection has deleted/excluded the summary.
        for _ in 0..2 {
            harness
                .processor
                .notify_semantic_timeline_item_changed(
                    &harness.workspace_id,
                    &harness.thread_id,
                    item_turn_id,
                    item,
                    Some("completed"),
                )
                .await;
            if offset == 0 {
                harness
                    .processor
                    .notify_semantic_timeline_work_item_id_changed(
                        &harness.workspace_id,
                        &harness.thread_id,
                        &harness.turn_id,
                        item.item_id(),
                    )
                    .await;
                harness
                    .processor
                    .notify_semantic_turn_work_state_changed(
                        &harness.workspace_id,
                        &harness.thread_id,
                        &harness.turn_id,
                    )
                    .await;
                harness
                    .processor
                    .notify_semantic_user_message_changed(
                        &harness.workspace_id,
                        &harness.thread_id,
                        &harness.turn_id,
                    )
                    .await;
            }
            while let Ok(packet) = harness.rx.try_recv() {
                let Message::Text(packet) = packet else {
                    panic!("expected JSON notification")
                };
                let packet: JsonValue = serde_json::from_str(&packet).unwrap();
                match packet["method"].as_str() {
                    Some(events::TURN_WORK_STATE_CHANGED) => {
                        panic!("detached parent advertised Work state")
                    }
                    Some(events::TURN_WORK_ITEMS_CHANGED) => {
                        let notification: pioneer_protocol::TurnWorkItemsChangedNotification =
                            serde_json::from_value(packet["params"].clone()).unwrap();
                        assert!(notification.changed_work_item_ids.is_empty());
                        assert!(!notification.removed_work_item_ids.is_empty());
                    }
                    Some(events::THREAD_TIMELINE_BLOCKS_CHANGED) => {
                        let notification: pioneer_protocol::ThreadTimelineBlocksChangedNotification =
                            serde_json::from_value(packet["params"].clone()).unwrap();
                        assert!(!notification.changed_block_ids.contains(&work_id));
                        changed_blocks.extend(notification.changed_block_ids.iter().cloned());
                        if notification
                            .turn_work_tombstones
                            .iter()
                            .any(|tombstone| tombstone.turn_id == harness.turn_id)
                        {
                            tombstones.push(notification);
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    // The owner is a conversation parent, still in progress, not a task-run.
    let (_, parent) = store
        .get_turn(&harness.thread_id, &harness.turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parent.turn_kind, TurnKind::Conversation);
    assert_eq!(parent.status, TurnStatus::InProgress);
    assert!(changed_blocks.contains(&pioneer_crud::user_block_id(&harness.turn_id)));
    assert!(changed_blocks.contains(&pioneer_crud::assistant_block_id(
        &harness.turn_id,
        "parent-final"
    )));
    assert!(
        changed_blocks.contains(&pioneer_crud::detached_task_run_block_id(
            &occurrence.id,
            "detached-task-anchor"
        ))
    );
    let latest = tombstones
        .last()
        .expect("removal must carry its revision")
        .clone();
    let stream = store
        .get_turn_event_projection_stream_state(&harness.turn_id)
        .await
        .unwrap()
        .unwrap();
    let tombstone = &latest.turn_work_tombstones[0];
    assert_eq!(
        tombstone.source_high_watermark,
        stream.projected_through_sequence
    );
    assert_eq!(
        tombstone.projection_updated_at_unix_micros,
        stream.updated_at_unix_micros
    );
    assert!(tombstone.source_high_watermark > 0);
    assert!(tombstones.windows(2).all(|pair| {
        let a = &pair[0].turn_work_tombstones[0];
        let b = &pair[1].turn_work_tombstones[0];
        (a.source_high_watermark, a.projection_updated_at_unix_micros)
            <= (b.source_high_watermark, b.projection_updated_at_unix_micros)
    }));

    let page = request_live_thread_timeline_page(&mut harness, "parent-preserved").await;
    assert!(
        page.blocks
            .iter()
            .any(|b| b.block_id == pioneer_crud::user_block_id(&harness.turn_id))
    );
    assert!(
        page.blocks
            .iter()
            .any(|b| b.block_id
                == pioneer_crud::assistant_block_id(&harness.turn_id, "parent-final"))
    );
    assert!(page.blocks.iter().any(|b| b.block_id
        == pioneer_crud::detached_task_run_block_id(&occurrence.id, "detached-task-anchor")));
    assert!(page.blocks.iter().all(|b| b.block_id != work_id));
    let mut client = SemanticTimelineState::default();
    apply_thread_timeline_page(&mut client, page, TopLevelPageMergeMode::Reset);
    apply_thread_timeline_blocks_changed(&mut client, latest.clone());
    let params = items_params(&harness);
    for _ in 0..2 {
        let response = rpc(&mut harness, "turn/work/items/get", params.clone()).await;
        assert!(response.get("error").is_none(), "{response}");
        let response: pioneer_protocol::TurnWorkItemsGetResponse =
            serde_json::from_value(response["result"].clone()).unwrap();
        assert!(response.items.is_empty());
        assert_eq!(
            response.removed_work_item_ids,
            ["missing-work", "stale-work"]
        );
        assert_eq!(
            response.source_high_watermark,
            stream.projected_through_sequence
        );
        assert_eq!(
            response.projection_updated_at_unix_micros,
            stream.updated_at_unix_micros
        );
        apply_turn_work_items_get_response(&mut client, response);
        // An old pre-fix invalidation is redelivered after the tombstone/late reply.
        assert!(!apply_turn_work_items_changed(
            &mut client,
            pioneer_protocol::TurnWorkItemsChangedNotification {
                workspace_id: harness.workspace_id.clone(),
                thread_id: harness.thread_id.clone(),
                turn_id: harness.turn_id.clone(),
                changed_work_item_ids: vec!["stale-work".into()],
                removed_work_item_ids: Vec::new(),
                before_cursor: None,
                after_cursor: None,
                reason: pioneer_protocol::TimelineChangeReason::LiveEvent,
            }
        ));
        apply_thread_timeline_blocks_changed(&mut client, latest.clone());
        let cached = client.thread(&harness.thread_id).unwrap();
        assert!(cached.top_level.block(&work_id).is_none());
        assert!(
            cached
                .work_range(&harness.turn_id)
                .is_none_or(|r| r.work.is_none() && r.items_by_id.is_empty())
        );
        assert!(
            store
                .get_turn_work_projection(&harness.turn_id)
                .await
                .unwrap()
                .is_none()
        );
    }
    let page_params = json!({"threadId":harness.thread_id, "turnId":harness.turn_id,
        "anchor":{"kind":"newest"}, "limit":10});
    let page = rpc(&mut harness, "turn/work/page", page_params).await;
    assert!(page.get("error").is_none(), "{page}");
    assert_eq!(page["result"]["work"], JsonValue::Null);
    assert_eq!(
        page["result"]["sourceHighWatermark"],
        stream.projected_through_sequence
    );
}

#[tokio::test]
async fn detached_work_absence_does_not_mask_unprepared_unhealthy_or_database_failures() {
    use pioneer_entity::turn_event_projection_stream_state as stream;
    for fault in [
        "quarantined",
        "unprepared",
        "missing",
        "wrong-thread",
        "unexpected-work",
    ] {
        let (_directory, mut harness) = detached_parent(fault).await;
        let db = harness.processor.crud_store.database_connection();
        if fault == "unexpected-work" {
            let mut projection = harness
                .processor
                .crud_store
                .get_turn_work_projection(harness.turn_id.strip_suffix("_parent").unwrap())
                .await
                .unwrap()
                .unwrap();
            projection.turn_id = harness.turn_id.clone();
            use sea_orm::ActiveModelTrait;
            let projection: pioneer_entity::turn_work_projection::ActiveModel = projection.into();
            projection.reset_all().insert(&db).await.unwrap();
        } else if fault == "missing" {
            stream::Entity::delete_by_id(&harness.turn_id)
                .exec(&db)
                .await
                .unwrap();
        } else {
            let column = match fault {
                "quarantined" => stream::Column::Status,
                "unprepared" => stream::Column::ProjectedThroughSequence,
                _ => stream::Column::ThreadId,
            };
            let value = match fault {
                "quarantined" => sea_orm::sea_query::Expr::value("quarantined"),
                "unprepared" => sea_orm::sea_query::Expr::value(0i64),
                _ => sea_orm::sea_query::Expr::value("another-thread"),
            };
            stream::Entity::update_many()
                .col_expr(column, value)
                .filter(stream::Column::TurnId.eq(&harness.turn_id))
                .exec(&db)
                .await
                .unwrap();
        }
        let params = items_params(&harness);
        let result = rpc(&mut harness, "turn/work/items/get", params).await;
        assert!(result.get("error").is_some(), "{fault}: {result}");
        assert!(result.get("result").is_none());
    }
    // A missing summary for an ordinary running owner is still an integrity error.
    let mut ordinary = setup_live_semantic_timeline_harness("ordinary_missing").await;
    pioneer_entity::turn_work_projection::Entity::delete_by_id(&ordinary.turn_id)
        .exec(&ordinary.processor.crud_store.database_connection())
        .await
        .unwrap();
    let params = items_params(&ordinary);
    let result = rpc(&mut ordinary, "turn/work/items/get", params).await;
    assert!(result.get("error").is_some(), "{result}");

    let (_directory, mut broken_db) = detached_parent("closed_db").await;
    broken_db
        .processor
        .crud_store
        .database_connection()
        .close()
        .await
        .unwrap();
    assert!(
        broken_db
            .processor
            .crud_store
            .get_detached_turn_work_tombstone(
                &broken_db.workspace_id,
                &broken_db.thread_id,
                &broken_db.turn_id,
            )
            .await
            .is_err()
    );
    let params = items_params(&broken_db);
    let result = rpc(&mut broken_db, "turn/work/items/get", params).await;
    assert!(result.get("error").is_some(), "{result}");
}

#[tokio::test]
async fn detached_work_tombstones_require_current_authorization_and_authoritative_scope() {
    let (_directory, mut harness) = detached_parent("scope").await;
    let store = harness.processor.crud_store.clone();
    for (workspace, thread, turn) in [
        (
            "foreign-workspace",
            harness.thread_id.as_str(),
            harness.turn_id.as_str(),
        ),
        (
            harness.workspace_id.as_str(),
            "foreign-thread",
            harness.turn_id.as_str(),
        ),
        (
            harness.workspace_id.as_str(),
            harness.thread_id.as_str(),
            "unknown-turn",
        ),
    ] {
        assert!(
            store
                .get_detached_turn_work_tombstone(workspace, thread, turn)
                .await
                .unwrap()
                .is_none()
        );
    }
    for (thread, turn) in [
        (harness.thread_id.clone(), "unknown-turn".to_owned()),
        ("foreign-thread".to_owned(), harness.turn_id.clone()),
    ] {
        let result = rpc(
            &mut harness,
            "turn/work/items/get",
            json!({
                "threadId":thread, "turnId":turn, "workItemIds":["stale-work"],
            }),
        )
        .await;
        assert!(result.get("error").is_some(), "{result}");
        assert!(result.get("result").is_none());
    }
    use sea_orm::ActiveModelTrait;
    let mut thread: pioneer_entity::thread::ActiveModel = store
        .get_thread_by_id(&harness.thread_id)
        .await
        .unwrap()
        .unwrap()
        .into();
    thread.access_class = Set("private".into());
    thread.update(&store.database_connection()).await.unwrap();
    materialize_test_member_collaborator(store.as_ref(), &harness.workspace_id, &harness.thread_id)
        .await;
    let (tx, mut rx) = mpsc::channel(32);
    let member = harness
        .processor
        .session_manager
        .register_connection(tx, authenticated_test_member_collaborator())
        .await
        .unwrap();
    // Demonstrate admission before removing the membership, then reuse the same
    // connection and exact scope after revocation.
    for admitted in [true, false] {
        if !admitted {
            pioneer_entity::thread_membership::Entity::delete_many()
                .filter(pioneer_entity::thread_membership::Column::ThreadId.eq(&harness.thread_id))
                .filter(
                    pioneer_entity::thread_membership::Column::PrincipalId
                        .eq(TEST_COLLABORATOR_PRINCIPAL_ID),
                )
                .exec(&store.database_connection())
                .await
                .unwrap();
        }
        let id =
            generate_test_request_id("detached-auth", if admitted { "before" } else { "revoked" });
        harness.processor.process_request_for_connection(member,
            &json!({"jsonrpc":"2.0", "id":id, "method":"turn/work/items/get", "params":items_params(&harness)}).to_string(),
        ).await;
        let result: JsonValue =
            serde_json::from_str(&recv_jsonrpc_payload_by_id(&mut rx, &id).await).unwrap();
        assert_eq!(result.get("error").is_none(), admitted, "{result}");
        if !admitted {
            assert!(result.get("result").is_none());
        }
    }
}
