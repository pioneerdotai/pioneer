use super::*;

#[tokio::test]
async fn completed_final_selector_uses_committed_intent_and_checks_ownership() {
    for final_id in ["a_final", "z_final"] {
        pioneer_postgres_test::with_database(async |fixture| {
            let (store, _, _) =
                postgres_store_with_started_turn(fixture, "ws_select", "thr_select", "turn_select")
                    .await;
            let notification = |id: &str, phase, text: &str| ItemCompletedNotification {
                workspace_id: "ws_select".into(),
                thread_id: "thr_select".into(),
                turn_id: "turn_select".into(),
                item: TurnItem::AgentMessage {
                    id: id.into(),
                    text: text.into(),
                    phase,
                    markdown: None,
                    markdown_version: None,
                },
            };
            let answer = notification(
                final_id,
                AgentMessagePhase::FinalAnswer,
                "accepted substantive answer",
            );
            store
                .materialize_item_completed(
                    notification(
                        "old_partial_final",
                        AgentMessagePhase::FinalAnswer,
                        "old attempt",
                    ),
                    1_700_000_100,
                )
                .await
                .unwrap();
            for (id, phase) in [
                ("0_commentary", AgentMessagePhase::Commentary),
                ("old_partial_final", AgentMessagePhase::Commentary),
                ("zz_commentary", AgentMessagePhase::Commentary),
            ] {
                store
                    .materialize_item_completed(
                        notification(id, phase, "not the answer"),
                        1_700_000_100,
                    )
                    .await
                    .unwrap();
            }
            store
                .prepare_turn_finalization(&answer, 1, None, 1_700_000_100)
                .await
                .unwrap();
            assert!(
                store
                    .completed_turn_final_answer("ws_select", "thr_select", "turn_select")
                    .await
                    .unwrap()
                    .is_none()
            );
            store
                .commit_prepared_turn_finalization("turn_select", 1_700_000_100)
                .await
                .unwrap();
            store
                .materialize_item_completed(
                    notification(
                        "late_commentary",
                        AgentMessagePhase::Commentary,
                        "not a final",
                    ),
                    1_700_000_100,
                )
                .await
                .unwrap();
            let restarted = CrudStore::new(store.database_connection()).with_maintenance_access();
            assert_eq!(
                restarted
                    .completed_turn_final_answer("ws_select", "thr_select", "turn_select")
                    .await
                    .unwrap(),
                Some(answer.item)
            );
            assert!(
                restarted
                    .completed_turn_final_answer("foreign", "thr_select", "turn_select")
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                restarted
                    .completed_turn_final_answer("ws_select", "foreign", "turn_select")
                    .await
                    .unwrap()
                    .is_none()
            );
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn prepared_native_finalization_survives_restart_and_commits_exactly_once() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "ws_native_finalization",
            "thr_native_finalization",
            "turn_native_finalize",
        )
        .await;
        let started = unix_to_datetime(1_700_000_010);
        store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: "ws_native_finalization".into(),
                    thread_id: "thr_native_finalization".into(),
                    turn_id: "turn_native_finalize".into(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Checkpointed,
                    exhaustion_reason: None,
                    agent_round_count: 2,
                    tool_call_count: 0,
                    provider_token_count: 7,
                    metadata_json: serde_json::json!({}),
                    started_at: started,
                },
                started,
                started,
            )
            .await
            .unwrap();
        let mut completed_item = safe_web_fetch_item("native_finalization_tool");
        if let TurnItem::WebFetch {
            status, success, ..
        } = &mut completed_item
        {
            *status = ToolCallStatus::Completed;
            *success = Some(true);
        }
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: "ws_native_finalization".into(),
                    thread_id: "thr_native_finalization".into(),
                    turn_id: "turn_native_finalize".into(),
                    item: completed_item,
                },
                1_700_000_050,
            )
            .await
            .unwrap();
        let final_notification = ItemCompletedNotification {
            workspace_id: "ws_native_finalization".to_owned(),
            thread_id: "thr_native_finalization".to_owned(),
            turn_id: "turn_native_finalize".to_owned(),
            item: TurnItem::AgentMessage {
                id: "final_native_message_1".to_owned(),
                text: "the one durable final answer".to_owned(),
                phase: AgentMessagePhase::FinalAnswer,
                markdown: None,
                markdown_version: None,
            },
        };

        let database = store.database_connection();
        let mut projection = database.work_signal("projection").subscribe();
        projection.begin_pass(&database).await.unwrap();
        assert_eq!(
            store
                .prepare_turn_finalization(&final_notification, 1, None, 1_700_000_100)
                .await
                .expect("finalization intent should prepare"),
            PrepareTurnFinalizationOutcome::Prepared
        );
        assert!(
            futures_util::FutureExt::now_or_never(projection.wait(None)).is_some(),
            "a prepared intent must wake recovery even without a new event append"
        );
        projection.begin_pass(&database).await.unwrap();

        assert_eq!(
            store
                .prepare_turn_finalization(&final_notification, 1, None, 1_700_000_100)
                .await
                .expect("exact intent replay should be idempotent"),
            PrepareTurnFinalizationOutcome::AlreadyPrepared
        );
        assert!(
            futures_util::FutureExt::now_or_never(projection.wait(None)).is_none(),
            "idempotent preparation must not keep recovery awake"
        );

        let mut rerendered = final_notification.clone();
        if let TurnItem::AgentMessage {
            markdown_version, ..
        } = &mut rerendered.item
        {
            *markdown_version = Some(99);
        }
        assert_eq!(
            store
                .prepare_turn_finalization(&rerendered, 1, None, 1_700_000_100)
                .await
                .expect("derived renderer metadata must not change provider identity"),
            PrepareTurnFinalizationOutcome::AlreadyPrepared
        );
        let before = store
            .get_turn("thr_native_finalization", "turn_native_finalize")
            .await
            .expect("turn lookup should succeed")
            .expect("turn should exist")
            .1;
        assert_eq!(before.status, TurnStatus::InProgress);
        assert!(
            pioneer_crud_conversations::turn_reads::find_turn_item(
                &store.connection,
                "turn_native_finalize",
                "final_native_message_1"
            )
            .await
            .expect("item lookup should succeed")
            .is_none(),
            "prepare alone must not publish a half-terminal final item"
        );

        // A fresh store models a process restart after the provider response
        // was acknowledged but before TurnCompleted was emitted.
        let restarted = CrudStore::new(store.database_connection());
        let reconciliation = restarted
            .reconcile_prepared_turn_finalizations(10, 1_700_000_101)
            .await
            .expect("startup reconciliation should finish the commit");
        assert_eq!(reconciliation.scanned, 1);
        assert_eq!(reconciliation.committed_count(), 1);
        let closed_window = restarted
            .latest_turn_execution_window("turn_native_finalize")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(closed_window.status, ExecutionWindowStatus::Completed);
        assert_eq!(
            closed_window.completed_at,
            Some(unix_to_datetime(1_700_000_101))
        );
        assert_eq!(closed_window.tool_call_count, 1);
        assert_eq!(closed_window.agent_round_count, 2);
        assert_eq!(closed_window.provider_token_count, 7);
        projection.begin_pass(&database).await.unwrap();
        let committed = match restarted
            .commit_prepared_turn_finalization("turn_native_finalize", 1_700_000_102)
            .await
            .expect("duplicate terminal acknowledgement should reconcile")
        {
            TurnFinalizationCommitOutcome::Committed(committed) => committed,
            outcome => panic!("unexpected duplicate finalization outcome: {outcome:?}"),
        };
        assert!(committed.already_committed);
        assert_eq!(
            restarted
                .latest_turn_execution_window("turn_native_finalize")
                .await
                .unwrap()
                .unwrap(),
            closed_window,
            "duplicate finalization must not rewrite a closed window or its statistics"
        );
        assert!(
            futures_util::FutureExt::now_or_never(projection.wait(None)).is_none(),
            "duplicate finalization ACK must not wake recovery"
        );

        assert_eq!(committed.final_item, final_notification);

        let (_, terminal) = restarted
            .get_turn("thr_native_finalization", "turn_native_finalize")
            .await
            .expect("turn lookup should succeed")
            .expect("turn should exist");
        assert_eq!(terminal.status, TurnStatus::Completed);
        let items = pioneer_entity::turn_item::Entity::find()
            .filter(pioneer_entity::turn_item::Column::TurnId.eq("turn_native_finalize".to_owned()))
            .filter(
                pioneer_entity::turn_item::Column::ItemId.eq("final_native_message_1".to_owned()),
            )
            .all(&restarted.connection)
            .await
            .expect("final items should list");
        assert_eq!(items.len(), 1);

        let mut conflicting = final_notification;
        if let TurnItem::AgentMessage { text, .. } = &mut conflicting.item {
            *text = "a conflicting second provider answer".to_owned();
        }
        assert!(
            restarted
                .prepare_turn_finalization(&conflicting, 1, None, 1_700_000_103)
                .await
                .is_err(),
            "a finalization generation cannot alias different provider output"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn live_and_background_finalization_share_one_commit_receipt() -> anyhow::Result<()> {
    use anyhow::Context as _;

    pioneer_postgres_test::with_database(async |fixture| {
        let at = 1_700_000_000;
        let store = postgres_store_with_workspace(fixture, "ws_progress").await;
        let turn_id = prepare_progress_finalization(&store, 0, at).await;

        let pool = fixture.connection();
        let fence = pool.begin().await?;
        pioneer_crud_conversations::turn_event::lock_stream(&fence, &turn_id).await?;
        let fence_pid = fence
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT pg_backend_pid() AS pid",
            ))
            .await?
            .context("finalization fence backend pid")?
            .try_get::<i32>("", "pid")?;

        let left = store.clone();
        let right = CrudStore::new(pioneer_postgres_pools::PostgresDatabase::new(
            fixture.connect_read_only().await?,
            fixture.connect().await?,
        ));
        let left_id = turn_id.clone();
        let right_id = turn_id.clone();
        let live = tokio::spawn(async move {
            left.commit_prepared_turn_finalization(&left_id, at + 1).await
        });
        let recovery = tokio::spawn(async move {
            right.commit_prepared_turn_finalization(&right_id, at + 2).await
        });
        let blocked = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let waiters = pool
                    .query_one_raw(Statement::from_sql_and_values(
                        DatabaseBackend::Postgres,
                        "SELECT count(*) AS waiters FROM pg_locks WHERE locktype='advisory' AND NOT granted AND $1 = ANY(pg_blocking_pids(pid))",
                        [fence_pid.into()],
                    ))
                    .await?
                    .context("finalization fence waiters")?
                    .try_get::<i64>("", "waiters")?;
                if waiters == 2 {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        fence.rollback().await?;
        let live = live.await??;
        let recovery = recovery.await??;
        blocked.context("both publishers must wait before reading the canonical stream")??;

        let (
            TurnFinalizationCommitOutcome::Committed(live),
            TurnFinalizationCommitOutcome::Committed(recovery),
        ) = (live, recovery)
        else {
            panic!("both publishers must observe the committed finalization");
        };
        assert_ne!(live.already_committed, recovery.already_committed);
        assert_eq!(live.final_item, recovery.final_item);
        assert_eq!(live.turn_completed, recovery.turn_completed);
        let events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id))
            .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
            .all(pool)
            .await?;
        assert_eq!(events.len(), 3, "start, final item, and terminal event only");
        assert_eq!(events[1].event_type, pioneer_protocol::constants::events::ITEM_COMPLETED);
        assert_eq!(events[2].event_type, pioneer_protocol::constants::events::TURN_COMPLETED);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn finalization_filters_more_than_a_page_of_blocked_prefixes_before_limit() {
    pioneer_postgres_test::with_database(async |fixture| {
    let at = 1_700_000_000;
    let store = postgres_store_with_workspace(fixture, "ws_progress").await;
    for index in 0..6 {
        let id = prepare_progress_finalization(&store, index, at).await;
        if index < 5 {
            mark_first_turn_projection_failed(&store, &id, at + 100).await;
        }
    }
    let database = store.database_connection();
    let mut consumers = [
        "gateway.control",
        "gateway.cli",
        "maintenance.zstd",
        "maintenance.receipts",
    ]
    .map(|domain| database.work_signal(domain).subscribe());
    for consumer in &mut consumers {
        consumer.begin_pass(&database).await.unwrap();
    }
    let result = store
        .reconcile_prepared_turn_finalizations(2, at + 10)
        .await
        .unwrap();
    assert_eq!(result.scanned, 1);
    assert_eq!(result.committed_count(), 1);
    for consumer in &mut consumers {
        assert!(
            futures_util::FutureExt::now_or_never(consumer.wait(None)).is_some(),
            "finalization creates projection/control dependencies after the original preparation wake"
        );
        consumer.begin_pass(&database).await.unwrap();
    }
    assert_eq!(
        store
            .reconcile_prepared_turn_finalizations(2, at + 11)
            .await
            .unwrap()
            .committed_count(),
        0
    );
    for consumer in &mut consumers {
        assert!(
            futures_util::FutureExt::now_or_never(consumer.wait(None)).is_none(),
            "retained blocked history must let independent consumers remain idle"
        );
    }
    assert_eq!(
        result.committed[0].turn_completed.turn.id,
        "turn_progress_5"
    );
    assert!(result.failures.is_empty());
    assert_eq!(
        store
            .list_prepared_turn_finalization_ids(10)
            .await
            .unwrap()
            .len(),
        5
    );
    // Restoration makes the retained obligations eligible on the next pass.
    store
        .replay_due_turn_event_projections(at + 200, 10)
        .await
        .unwrap();
    let next = store
        .reconcile_prepared_turn_finalizations(2, at + 201)
        .await
        .unwrap();
    assert_eq!(next.committed_count(), 2);
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn finalization_pages_past_poison_and_reports_storage_failure_with_success() {
    pioneer_postgres_test::with_database(async |fixture| {
        let at = 1_700_000_000;
        let store = postgres_store_with_workspace(fixture, "ws_progress").await;
        for index in 0..6 {
            let id = prepare_progress_finalization(&store, index, at).await;
            if index < 4 {
                pioneer_entity::turn_finalization::Entity::update_many()
                    .col_expr(
                        pioneer_entity::turn_finalization::Column::ItemJson,
                        sea_orm::sea_query::Expr::value("invalid JSON"),
                    )
                    .filter(pioneer_entity::turn_finalization::Column::TurnId.eq(id))
                    .exec(&store.connection)
                    .await
                    .unwrap();
            }
        }
        store
            .connection
            .execute_unprepared(
                "CREATE OR REPLACE FUNCTION reject_progress_finalization() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'finalization storage failure'; END $$; CREATE TRIGGER reject_progress_finalization BEFORE INSERT ON turn_event FOR EACH ROW WHEN (NEW.turn_id='turn_progress_4') EXECUTE FUNCTION reject_progress_finalization()",
            )
            .await
            .unwrap();
        for _ in 0..2 {
            let page = store
                .reconcile_prepared_turn_finalizations(2, at + 10)
                .await
                .unwrap();
            assert_eq!(page.scanned, 2);
            assert_eq!(page.failures.len(), 2);
            assert_eq!(page.committed_count(), 0);
        }
        let page = store
            .reconcile_prepared_turn_finalizations(2, at + 10)
            .await
            .unwrap();
        assert_eq!(page.scanned, 2);
        assert_eq!(page.committed_count(), 1);
        assert_eq!(page.committed[0].turn_completed.turn.id, "turn_progress_5");
        assert_eq!(page.failures.len(), 1);
        assert!(matches!(
            page.failures[0].downcast_ref::<crate::NativeFinalizationPhase>(),
            Some(crate::NativeFinalizationPhase::WriterApply)
        ));
        assert_eq!(
            store
                .list_prepared_turn_finalization_ids(10)
                .await
                .unwrap()
                .len(),
            5
        );
        // EOF wraps; retained poison is retried rather than ACKed or forgotten.
        let eof = store
            .reconcile_prepared_turn_finalizations(2, at + 11)
            .await
            .unwrap();
        assert_eq!(eof.scanned, 0);
        let retry = store
            .reconcile_prepared_turn_finalizations(2, at + 12)
            .await
            .unwrap();
        assert_eq!(retry.failures.len(), 2);
        store
            .connection
            .execute_unprepared("DROP TRIGGER reject_progress_finalization ON turn_event")
            .await
            .unwrap();
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn blocked_finalization_defers_payload_preparation_and_preserves_failed_intent() {
    pioneer_postgres_test::with_database(async |fixture| {
        let at = 1_700_000_000;
        let turn_id = "turn_defer_decode";
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "ws_defer_decode",
            "thr_defer_decode",
            turn_id,
        )
        .await;
        store
            .prepare_turn_finalization(
                &ItemCompletedNotification {
                    workspace_id: "ws_defer_decode".into(),
                    thread_id: "thr_defer_decode".into(),
                    turn_id: turn_id.into(),
                    item: TurnItem::AgentMessage {
                        id: "final_defer_decode".into(),
                        text: "accepted answer".into(),
                        phase: AgentMessagePhase::FinalAnswer,
                        markdown: None,
                        markdown_version: None,
                    },
                },
                1,
                None,
                at + 1,
            )
            .await
            .unwrap();
        mark_first_turn_projection_failed(&store, turn_id, at + 20).await;
        // A corrupt stored payload must be examined only when its causal prefix
        // is ready. Deferral must not run answer preparation or ACK the intent.
        pioneer_entity::turn_finalization::Entity::update_many()
            .col_expr(
                pioneer_entity::turn_finalization::Column::ItemJson,
                sea_orm::sea_query::Expr::value("invalid JSON"),
            )
            .filter(pioneer_entity::turn_finalization::Column::TurnId.eq(turn_id))
            .exec(&store.connection)
            .await
            .unwrap();
        assert_eq!(
            store
                .commit_prepared_turn_finalization(turn_id, at + 2)
                .await
                .unwrap(),
            TurnFinalizationCommitOutcome::DeferredByPredecessor
        );
        store
            .replay_due_turn_event_projections(at + 30, 10)
            .await
            .unwrap();
        assert!(
            store
                .commit_prepared_turn_finalization(turn_id, at + 31)
                .await
                .is_err()
        );
        assert_eq!(
            store.list_prepared_turn_finalization_ids(1).await.unwrap(),
            vec![turn_id]
        );
        let (_, target) = store
            .get_turn("thr_defer_decode", turn_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(target.status, TurnStatus::InProgress);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn prepared_native_finalization_defers_until_its_predecessor_is_projected() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_000_000;
        let turn_id = "turn_finalize_defer";
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "ws_finalize_defer",
            "thr_finalize_defer",
            turn_id,
        )
        .await;
        let final_notification = ItemCompletedNotification {
            workspace_id: "ws_finalize_defer".to_owned(),
            thread_id: "thr_finalize_defer".to_owned(),
            turn_id: turn_id.to_owned(),
            item: TurnItem::AgentMessage {
                id: "final_defer_message".to_owned(),
                text: "publish after predecessor".to_owned(),
                phase: AgentMessagePhase::FinalAnswer,
                markdown: None,
                markdown_version: None,
            },
        };
        store
            .prepare_turn_finalization(&final_notification, 1, None, timestamp + 1)
            .await
            .expect("finalization intent should prepare");
        mark_first_turn_projection_failed(&store, turn_id, timestamp + 20).await;

        assert_eq!(
            store
                .commit_prepared_turn_finalization(turn_id, timestamp + 2)
                .await
                .expect("causal backpressure should be a typed outcome"),
            TurnFinalizationCommitOutcome::DeferredByPredecessor
        );
        let deferred = store
            .reconcile_prepared_turn_finalizations(10, timestamp + 10)
            .await
            .expect("deferred finalization must not fail reconciliation");
        assert_eq!(deferred.scanned, 0);
        assert_eq!(deferred.deferred_by_predecessor, 0);
        assert_eq!(deferred.committed_count(), 0);
        assert!(
            pioneer_crud_conversations::turn_reads::find_turn_item(
                &store.connection,
                turn_id,
                final_notification.item.item_id(),
            )
            .await
            .expect("final item lookup should succeed")
            .is_none(),
            "a deferred finalization must not publish its final item"
        );
        let events_before_replay = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id.to_owned()))
            .count(&store.connection)
            .await
            .expect("turn events should count");
        assert_eq!(
            events_before_replay, 1,
            "a deferred atomic finalization must roll back both new events"
        );

        let replay = store
            .replay_due_turn_event_projections(timestamp + 30, 10)
            .await
            .expect("predecessor replay should succeed");
        assert_eq!(replay.projected, 1);
        let reconciled = store
            .reconcile_prepared_turn_finalizations(10, timestamp + 31)
            .await
            .expect("finalization should commit after its predecessor");
        assert_eq!(reconciled.scanned, 1);
        assert_eq!(reconciled.committed_count(), 1);
        assert_eq!(reconciled.deferred_by_predecessor, 0);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn deferred_prepared_finalization_does_not_block_a_later_ready_turn() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_finalize_fair";
        let blocked_thread_id = "thr_finalize_blocked";
        let blocked_turn_id = "turn_finalize_blocked";
        let ready_thread_id = "thr_finalize_ready";
        let ready_turn_id = "turn_finalize_ready";
        let store = postgres_store_with_workspace(fixture, workspace_id).await;

        for (thread_id, turn_id) in [
            (blocked_thread_id, blocked_turn_id),
            (ready_thread_id, ready_turn_id),
        ] {
            store
                .materialize_turn_start(
                    &sample_thread(workspace_id, thread_id, timestamp),
                    SandboxMode::FullAccess,
                    &sample_turn(turn_id),
                    &[],
                    PersistedActorRef::System,
                )
                .await
                .expect("turn start should persist");
        }

        for (offset, thread_id, turn_id, item_id) in [
            (1, blocked_thread_id, blocked_turn_id, "final_blocked"),
            (2, ready_thread_id, ready_turn_id, "final_ready"),
        ] {
            store
                .prepare_turn_finalization(
                    &ItemCompletedNotification {
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        item: TurnItem::AgentMessage {
                            id: item_id.to_owned(),
                            text: format!("answer for {turn_id}"),
                            phase: AgentMessagePhase::FinalAnswer,
                            markdown: None,
                            markdown_version: None,
                        },
                    },
                    1,
                    None,
                    timestamp + offset,
                )
                .await
                .expect("finalization intent should prepare");
        }
        mark_first_turn_projection_failed(&store, blocked_turn_id, timestamp + 20).await;

        assert_eq!(
            store
                .list_prepared_turn_finalization_ids(10)
                .await
                .expect("prepared finalizations should list"),
            vec![blocked_turn_id.to_owned(), ready_turn_id.to_owned()],
            "prepared finalizations must use stable oldest-first ordering"
        );
        let reconciliation = store
            .reconcile_prepared_turn_finalizations(10, timestamp + 3)
            .await
            .expect("one deferred Turn must not fail the bounded batch");
        assert_eq!(reconciliation.scanned, 1);
        assert_eq!(reconciliation.deferred_by_predecessor, 0);
        assert_eq!(reconciliation.committed_count(), 1);
        assert_eq!(
            reconciliation.committed[0].turn_completed.turn.id,
            ready_turn_id
        );

        let (_, blocked_turn) = store
            .get_turn(blocked_thread_id, blocked_turn_id)
            .await
            .expect("blocked Turn should load")
            .expect("blocked Turn should exist");
        let (_, ready_turn) = store
            .get_turn(ready_thread_id, ready_turn_id)
            .await
            .expect("ready Turn should load")
            .expect("ready Turn should exist");
        assert_eq!(blocked_turn.status, TurnStatus::InProgress);
        assert_eq!(ready_turn.status, TurnStatus::Completed);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn terminal_turn_supersedes_a_stale_prepared_finalization_during_reconciliation() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_000_000;
        let turn_id = "turn_finalize_stale";
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "ws_finalize_stale",
            "thr_finalize_stale",
            turn_id,
        )
        .await;
        store
            .prepare_turn_finalization(
                &ItemCompletedNotification {
                    workspace_id: "ws_finalize_stale".to_owned(),
                    thread_id: "thr_finalize_stale".to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::AgentMessage {
                        id: "final_stale_message".to_owned(),
                        text: "stale provider answer".to_owned(),
                        phase: AgentMessagePhase::FinalAnswer,
                        markdown: None,
                        markdown_version: None,
                    },
                },
                1,
                None,
                timestamp + 1,
            )
            .await
            .expect("finalization intent should prepare");
        pioneer_entity::turn::Entity::update_many()
            .col_expr(
                pioneer_entity::turn::Column::Status,
                Expr::value("interrupted"),
            )
            .filter(pioneer_entity::turn::Column::Id.eq(turn_id.to_owned()))
            .exec(&store.connection)
            .await
            .expect("terminal status should persist");

        assert_eq!(
            store
                .commit_prepared_turn_finalization(turn_id, timestamp + 2)
                .await
                .expect("terminal conflict should resolve without an error"),
            TurnFinalizationCommitOutcome::SupersededByTerminalStatus(TurnStatus::Interrupted)
        );
        assert!(
            pioneer_crud_conversations::turn_finalization::find_by_turn_id(
                &store.connection,
                turn_id
            )
            .await
            .expect("finalization lookup should succeed")
            .is_none(),
            "superseded finalization intent must be removed"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn later_failed_turn_supersedes_a_prepared_native_finalization() {
    let timestamp = 1_700_000_000;
    let workspace_id = "ws_native_finalization_cancelled";
    let thread_id = "thr_native_finalization_cancelled";
    let turn_id = "turn_native_finalization_cancelled";
    let store = test_store_with_workspace(workspace_id).await;
    let thread = sample_thread(workspace_id, thread_id, timestamp);
    let turn = sample_turn(turn_id);
    store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &turn,
            &[],
            PersistedActorRef::System,
        )
        .await
        .expect("turn start should persist");

    let final_notification = ItemCompletedNotification {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        item: TurnItem::AgentMessage {
            id: "final_native_finalization_cancelled".to_owned(),
            text: "provider output accepted before cancellation".to_owned(),
            phase: AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        },
    };
    assert_eq!(
        store
            .prepare_turn_finalization(&final_notification, 1, None, timestamp + 1)
            .await
            .expect("finalization intent should prepare"),
        PrepareTurnFinalizationOutcome::Prepared
    );

    let mut interrupted = turn;
    interrupted.status = TurnStatus::Interrupted;
    interrupted.error = Some("cancelled after finalization preparation".to_owned());
    store
        .materialize_turn_failed(
            TurnFailedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn: interrupted,
            },
            timestamp + 2,
        )
        .await
        .expect("later failure should win atomically");

    assert!(
        pioneer_crud_conversations::turn_finalization::find_by_turn_id(&store.connection, turn_id)
            .await
            .expect("finalization intent lookup should succeed")
            .is_none(),
        "a superseded prepared finalization must not poison restart reconciliation"
    );
    let reconciliation = store
        .reconcile_prepared_turn_finalizations(10, timestamp + 3)
        .await
        .expect("restart reconciliation should have no stale intent");
    assert_eq!(reconciliation.scanned, 0);
    assert_eq!(reconciliation.committed_count(), 0);
    let (_, terminal) = store
        .get_turn(thread_id, turn_id)
        .await
        .expect("turn lookup should succeed")
        .expect("turn should exist");
    assert_eq!(terminal.status, TurnStatus::Interrupted);
}

#[tokio::test]
async fn late_native_finalization_cannot_attach_to_an_unrelated_completed_turn() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, mut turn) = postgres_store_with_started_turn(
            fixture,
            "ws_native_finalization_late",
            "thr_native_finalization_late",
            "turn_native_finalization_late",
        )
        .await;
        turn.status = TurnStatus::Completed;
        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: "ws_native_finalization_late".to_owned(),
                    thread_id: "thr_native_finalization_late".to_owned(),
                    turn,
                },
                1_700_000_100,
            )
            .await
            .expect("legacy terminal turn should commit");

        let late = ItemCompletedNotification {
            workspace_id: "ws_native_finalization_late".to_owned(),
            thread_id: "thr_native_finalization_late".to_owned(),
            turn_id: "turn_native_finalization_late".to_owned(),
            item: TurnItem::AgentMessage {
                id: "final_native_late_1".to_owned(),
                text: "late provider response".to_owned(),
                phase: AgentMessagePhase::FinalAnswer,
                markdown: None,
                markdown_version: None,
            },
        };
        let error = store
            .prepare_turn_finalization(&late, 1, None, 1_700_000_101)
            .await
            .expect_err("late finalization must not claim another terminal outcome");
        assert!(
            format!("{error:#}").contains("has no existing finalization intent"),
            "unexpected late-finalization error: {error:#}"
        );
        assert!(
            pioneer_entity::turn_finalization::Entity::find_by_id(
                "turn_native_finalization_late".to_owned()
            )
            .one(&store.connection)
            .await
            .expect("finalization lookup should succeed")
            .is_none()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_finalization_rejects_a_stale_attached_task_revision() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "ws_native_task_fence",
            "thr_native_task_fence",
            "turn_native_task_fence",
        )
        .await;
        let timestamp = 1_700_000_100;
        let mut task = sample_task(timestamp);
        task.id = "task_native_task_fence".to_owned();
        task.workspace_id = "ws_native_task_fence".to_owned();
        task.created_by_thread_id = Some("thr_native_task_fence".to_owned());
        task.created_by_turn_id = Some("turn_native_task_fence".to_owned());
        task.status = TaskStatus::Completed;
        task.completed_at = Some(timestamp);
        task.lifecycle_policy = Some(TaskLifecyclePolicy {
            attachment: TaskAttachmentMode::Attached,
            on_parent_cancel: TaskParentTerminalAction::Cancel,
            on_parent_failure: TaskParentTerminalAction::Cancel,
            completion: TaskCompletionBehavior::CompleteOnTerminalRun,
        });
        store
            .append_task_event(
                TaskEventPayload::TaskCreated { task: task.clone() },
                timestamp,
            )
            .await
            .expect("attached terminal task should persist");

        let observed = store
            .task_finalization_revision("thr_native_task_fence", "turn_native_task_fence")
            .await
            .expect("task finalization revision should load");
        assert_eq!(observed.attached_task_count, 1);

        pioneer_entity::task::Entity::update_many()
            .filter(pioneer_entity::task::Column::Id.eq(task.id.clone()))
            .col_expr(
                pioneer_entity::task::Column::Revision,
                Expr::value(task.revision + 1),
            )
            .col_expr(
                pioneer_entity::task::Column::UpdatedAt,
                Expr::value(unix_to_datetime(timestamp + 1)),
            )
            .exec(&store.connection)
            .await
            .expect("task transition should persist");

        let notification = ItemCompletedNotification {
            workspace_id: "ws_native_task_fence".to_owned(),
            thread_id: "thr_native_task_fence".to_owned(),
            turn_id: "turn_native_task_fence".to_owned(),
            item: TurnItem::AgentMessage {
                id: "final_native_task_fence".to_owned(),
                text: "fenced answer".to_owned(),
                phase: AgentMessagePhase::FinalAnswer,
                markdown: None,
                markdown_version: None,
            },
        };
        let error = store
            .prepare_turn_finalization(
                &notification,
                1,
                Some(observed.token.as_str()),
                timestamp + 2,
            )
            .await
            .expect_err("stale task snapshot must fail closed");
        assert!(
            format!("{error:#}").contains("revision changed before commit"),
            "unexpected stale task fence error: {error:#}"
        );
        assert!(
            pioneer_crud_conversations::turn_finalization::find_by_turn_id(
                &store.connection,
                notification.turn_id.as_str()
            )
            .await
            .expect("finalization intent lookup should succeed")
            .is_none()
        );

        let current = store
            .task_finalization_revision("thr_native_task_fence", "turn_native_task_fence")
            .await
            .expect("current task revision should load");
        assert_eq!(
            store
                .prepare_turn_finalization(
                    &notification,
                    1,
                    Some(current.token.as_str()),
                    timestamp + 3,
                )
                .await
                .expect("current task snapshot should prepare"),
            PrepareTurnFinalizationOutcome::Prepared
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_finalization_transaction_rolls_back_and_reconciles_after_injected_crash() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, _) = postgres_store_with_started_turn(fixture,
            "ws_native_finalize_fault",
            "thr_native_finalize_fault",
            "turn_native_fault",
        )
        .await;
        let started = unix_to_datetime(1_700_000_010);
        store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: "ws_native_finalize_fault".into(),
                    thread_id: "thr_native_finalize_fault".into(),
                    turn_id: "turn_native_fault".into(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 0,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({}),
                    started_at: started,
                },
                started,
                started,
            )
            .await
            .unwrap();
        let notification = ItemCompletedNotification {
            workspace_id: "ws_native_finalize_fault".to_owned(),
            thread_id: "thr_native_finalize_fault".to_owned(),
            turn_id: "turn_native_fault".to_owned(),
            item: TurnItem::AgentMessage {
                id: "final_native_fault_1".to_owned(),
                text: "durable answer before injected crash".to_owned(),
                phase: AgentMessagePhase::FinalAnswer,
                markdown: None,
                markdown_version: None,
            },
        };
        store
            .prepare_turn_finalization(&notification, 1, None, 1_700_000_100)
            .await
            .expect("finalization intent should prepare");
        store
            .connection
            .execute_unprepared(
                "CREATE FUNCTION reject_native_terminal_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected terminal commit crash'; END $$; CREATE TRIGGER reject_native_terminal_commit BEFORE UPDATE OF status ON turn_execution_window FOR EACH ROW WHEN (NEW.turn_id = 'turn_native_fault' AND NEW.status = 'completed') EXECUTE FUNCTION reject_native_terminal_commit()",
            )
            .await
            .expect("fault trigger should install");
        let error = store
            .commit_prepared_turn_finalization("turn_native_fault", 1_700_000_101)
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<crate::NativeFinalizationPhase>(),
            Some(crate::NativeFinalizationPhase::WriterApply)
        ));

        assert!(
            pioneer_crud_conversations::turn_reads::find_turn_item(
                &store.connection,
                "turn_native_fault",
                "final_native_fault_1"
            )
            .await
            .expect("item lookup should succeed")
            .is_none(),
            "failed terminal commit must roll back the final item"
        );
        let (_, still_running) = store
            .get_turn("thr_native_finalize_fault", "turn_native_fault")
            .await
            .expect("turn lookup should succeed")
            .expect("turn should exist");
        assert_eq!(still_running.status, TurnStatus::InProgress);
        assert_eq!(
            store
                .latest_turn_execution_window("turn_native_fault")
                .await
                .unwrap()
                .unwrap()
                .status,
            ExecutionWindowStatus::Running,
            "failure of window closure must roll back terminal publication too"
        );
        store
            .connection
            .execute_unprepared("DROP TRIGGER reject_native_terminal_commit ON turn_execution_window")
            .await
            .expect("fault trigger should uninstall");
        let reconciliation = store
            .reconcile_prepared_turn_finalizations(10, 1_700_000_102)
            .await
            .expect("prepared intent should reconcile");
        assert_eq!(reconciliation.scanned, 1);
        assert_eq!(reconciliation.committed_count(), 1);
        assert_eq!(
            store
                .latest_turn_execution_window("turn_native_fault")
                .await
                .unwrap()
                .unwrap()
                .status,
            ExecutionWindowStatus::Completed
        );
        Ok(())
    })
    .await
    .unwrap();
}
