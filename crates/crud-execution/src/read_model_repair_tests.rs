use super::*;
use crate::test_fixture::safe_web_fetch_item;
use crate::time::unix_to_datetime;
use sea_orm::{DatabaseBackend, Set, Statement};

struct NoEpisodicRepair;

impl EpisodicRepair for NoEpisodicRepair {
    type Prepared = ();

    async fn prepare<'a>(
        &'a self,
        _: &'a pioneer_entity::turn_item::Model,
        _: &'a str,
    ) -> Result<Option<()>> {
        Ok(None)
    }

    async fn commit<'a>(
        &'a self,
        _: &'a DatabaseTransaction,
        _: &'a (),
        _: DateTimeWithTimeZone,
    ) -> Result<()> {
        anyhow::bail!("repair fixture has no episodic source")
    }

    fn committed(&self) {}
}

async fn scoped(fixture: &pioneer_postgres_test::TestDatabase) -> SqliteDatabase {
    SqliteDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        fixture.connection().clone(),
    )
    .maintenance()
}

async fn run_repair(
    store: &ExecutionReadModelRepair<'_, NoEpisodicRepair>,
) -> Result<RepairSummary> {
    store
        .repair_terminal_turn_item_payloads(&mut ReadModelRepairTimings::default())
        .await
}

async fn read_fixture_item(
    db: &SqliteDatabase,
    row: pioneer_entity::turn_item::Model,
) -> Result<TurnItem> {
    let mut rows = turn_payload::hydrate_items(db, vec![row]).await?;
    let row = rows.pop().context("missing hydrated item")?;
    turn_payload::decode_item(&row)
}

#[tokio::test]
async fn read_model_repair_resumes_incremental_pass_and_catches_up_same_startup() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();
        let db = scoped(fixture).await;
        let store = ExecutionReadModelRepair::new(&db, NoEpisodicRepair);
        let timestamp = unix_to_datetime(1_700_000_000);

        let insert_inconsistent = |id: &str, item_id: &str| {
            let id = id.to_owned();
            let item_id = item_id.to_owned();
            let timestamp = timestamp.clone();
            let connection = connection.clone();
            async move {
                let item = safe_web_fetch_item(item_id.as_str());
                pioneer_entity::turn_item::Entity::insert(pioneer_entity::turn_item::ActiveModel {
                    id: Set(id),
                    turn_id: Set("turn_incremental_repair".to_owned()),
                    item_id: Set(item_id),
                    item_type: Set("web_fetch".to_owned()),
                    status: Set(Some(TURN_ITEM_STATUS_COMPLETED.to_owned())),
                    payload: Set(Some(
                        serde_json::to_value(&item).expect("turn item should serialize"),
                    )),
                    active_attempt_number: Set(0),
                    active_attempt_status: Set(None),
                    active_attempt_id: Set(None),
                    last_heartbeat_at: Set(None),
                    lease_expires_at: Set(None),
                    created_at: Set(timestamp.clone()),
                    updated_at: Set(timestamp),
                    ..Default::default()
                })
                .exec(&connection)
                .await
                .expect("inconsistent turn_item should insert");
            }
        };

        insert_inconsistent("row_incremental_seed", "item_incremental_seed").await;
        run_repair(&store)
            .await
            .expect("initial full repair should succeed");

        insert_inconsistent("row_incremental_resume", "item_incremental_resume").await;
        let interrupted_high_watermark = read_model_repair::current_change_generation(&connection)
            .await
            .expect("change generation should load");
        read_model_repair::begin_incremental_pass(
            &connection,
            read_model_repair::TERMINAL_TURN_ITEM_PAYLOAD_REPAIR_KEY,
            READ_MODEL_REPAIR_ALGORITHM_VERSION,
            interrupted_high_watermark,
        )
        .await
        .expect("interrupted incremental pass should start");

        // This row is newer than the interrupted pass's high-watermark. The
        // same startup must finish the old pass and then take a fresh snapshot.
        insert_inconsistent("row_incremental_catchup", "item_incremental_catchup").await;
        let summary = run_repair(&store)
            .await
            .expect("resumed and catch-up repair should succeed");
        assert_eq!(summary.remaining, 0);
        assert!(summary.repaired >= 2);

        for id in ["row_incremental_resume", "row_incremental_catchup"] {
            let row = pioneer_entity::turn_item::Entity::find_by_id(id)
                .one(&connection)
                .await
                .expect("repaired turn_item should query")
                .expect("repaired turn_item should exist");
            let item: TurnItem = read_fixture_item(&db, row)
                .await
                .expect("payload should decode");
            assert_ne!(tool_call_status(&item), Some(ToolCallStatus::InProgress));
        }

        let checkpoint = read_model_repair::load_checkpoint(
            &connection,
            read_model_repair::TERMINAL_TURN_ITEM_PAYLOAD_REPAIR_KEY,
        )
        .await
        .expect("checkpoint should query")
        .expect("checkpoint should exist");
        assert_eq!(
            checkpoint.incremental_status,
            read_model_repair::STATUS_COMPLETED
        );
        let dirty_count = connection
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT COUNT(*) AS value FROM read_model_repair_dirty_turn_item".to_owned(),
            ))
            .await
            .expect("dirty count should query")
            .expect("dirty count should return a row")
            .try_get::<i64>("", "value")
            .expect("dirty count should decode");
        assert_eq!(dirty_count, 0);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn read_model_repair_version_bump_forces_a_new_full_scan() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();
        let db = scoped(fixture).await;
        let store = ExecutionReadModelRepair::new(&db, NoEpisodicRepair);
        run_repair(&store)
            .await
            .expect("empty initial full repair should succeed");

        let timestamp = unix_to_datetime(1_700_000_000);
        pioneer_entity::turn_item::Entity::insert(pioneer_entity::turn_item::ActiveModel {
            id: Set("row_repair_version_probe".to_owned()),
            turn_id: Set("turn_repair_version_probe".to_owned()),
            item_id: Set("item_repair_version_probe".to_owned()),
            item_type: Set("web_fetch".to_owned()),
            status: Set(Some(TURN_ITEM_STATUS_COMPLETED.to_owned())),
            payload: Set(Some(serde_json::json!([]))),
            active_attempt_number: Set(0),
            active_attempt_status: Set(None),
            active_attempt_id: Set(None),
            last_heartbeat_at: Set(None),
            lease_expires_at: Set(None),
            created_at: Set(timestamp.clone()),
            updated_at: Set(timestamp),
            ..Default::default()
        })
        .exec(&connection)
        .await
        .expect("version probe row should insert");

        // Simulate a historical row that predates the exact dirty journal.
        connection
            .execute_unprepared("DELETE FROM read_model_repair_dirty_turn_item")
            .await
            .expect("test should clear the exact-change journal");
        run_repair(&store)
            .await
            .expect("same-version startup should not repeat the historical full scan");

        connection
            .execute_unprepared("UPDATE read_model_repair_checkpoint SET algorithm_version = 0")
            .await
            .expect("test should age the repair algorithm version");
        let error = run_repair(&store)
            .await
            .expect_err("version bump must force a new historical full scan");
        assert!(format!("{error:#}").contains("failed to decode turn_item payload"));
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn read_model_repair_updates_compressed_turn_item_and_then_uses_dirty_journal() {
    use pioneer_crud_conversations::turn_item::{ItemWrite, upsert};
    use pioneer_crud_payloads::storage::PreparedPayload;
    use pioneer_entity::portable_payload_value;

    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = scoped(&fixture).await;
    db.execute_unprepared(
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('workspace_repair','Repair',TRUE,FALSE);
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class)
             VALUES ('thread_repair','workspace_repair','','agent','gpt-5.4','openai','active','user','workspace');
         INSERT INTO turn(id,thread_id,status,turn_kind,origin)
             VALUES ('turn_compressed_repair','thread_repair','completed','conversation','user');",
    ).await.unwrap();
    let store = ExecutionReadModelRepair::new(&db, NoEpisodicRepair);

    let timestamp = unix_to_datetime(1_700_000_000);
    let row_id = "row_compressed_repair";
    let item_id = "item_compressed_repair";
    let original = serde_json::to_string(&safe_web_fetch_item(item_id)).unwrap();
    for reason in ["full", "incremental"] {
        let body = PreparedPayload::new_turn_item(&original).unwrap();
        let transaction = db.begin().await.unwrap();
        upsert(
            &transaction,
            ItemWrite {
                id: row_id.to_owned(),
                turn_id: "turn_compressed_repair".to_owned(),
                item_id: item_id.to_owned(),
                item_type: "web_fetch".to_owned(),
                status: Some(TURN_ITEM_STATUS_COMPLETED.to_owned()),
                created_at: timestamp,
                updated_at: timestamp,
            },
            &body,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        let stored_body = portable_payload_value::Entity::find_by_id(body.id())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored_body.codec, Some(1));
        let summary = run_repair(&store)
            .await
            .unwrap_or_else(|error| panic!("{reason} portable payload repair: {error:#}"));
        assert_eq!(summary.remaining, 0, "{reason}");
        assert_eq!(summary.repaired, 1, "{reason}");
    }

    let repaired = pioneer_entity::turn_item::Entity::find_by_id(row_id)
        .one(&db)
        .await
        .expect("repaired row should query")
        .expect("repaired row should exist");
    let repaired_item = read_fixture_item(&db, repaired)
        .await
        .expect("payload should decode");
    assert_ne!(
        tool_call_status(&repaired_item),
        Some(ToolCallStatus::InProgress)
    );
}

#[tokio::test]
async fn read_model_repair_never_applies_payload_for_a_stale_terminal_status() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();
        let db = scoped(fixture).await;
        let store = ExecutionReadModelRepair::new(&db, NoEpisodicRepair);
        let timestamp = unix_to_datetime(1_700_000_000);
        let item = safe_web_fetch_item("item_repair_status_fence");
        pioneer_entity::turn_item::Entity::insert(pioneer_entity::turn_item::ActiveModel {
            id: Set("row_repair_status_fence".to_owned()),
            turn_id: Set("turn_repair_status_fence".to_owned()),
            item_id: Set("item_repair_status_fence".to_owned()),
            item_type: Set("web_fetch".to_owned()),
            status: Set(Some(TURN_ITEM_STATUS_COMPLETED.to_owned())),
            payload: Set(Some(
                serde_json::to_value(&item).expect("turn item should serialize"),
            )),
            active_attempt_number: Set(0),
            active_attempt_status: Set(None),
            active_attempt_id: Set(None),
            last_heartbeat_at: Set(None),
            lease_expires_at: Set(None),
            created_at: Set(timestamp.clone()),
            updated_at: Set(timestamp),
            ..Default::default()
        })
        .exec(&connection)
        .await
        .expect("status-fence seed should insert");
        let stale_row = pioneer_entity::turn_item::Entity::find_by_id("row_repair_status_fence")
            .one(&connection)
            .await
            .expect("status-fence seed should query")
            .expect("status-fence seed should exist");
        let stale_repair = repaired_terminal_turn_item_payload(&stale_row)
            .expect("stale payload should decode")
            .expect("stale payload should need repair");

        pioneer_entity::turn_item::Entity::update_many()
            .filter(pioneer_entity::turn_item::Column::Id.eq("row_repair_status_fence".to_owned()))
            .col_expr(
                pioneer_entity::turn_item::Column::Status,
                Expr::value(Some(TURN_ITEM_STATUS_FAILED)),
            )
            .exec(&connection)
            .await
            .expect("live writer should change terminal status");

        let transaction = connection
            .begin()
            .await
            .expect("repair transaction should begin");
        let applied = store
            .apply_one_terminal_turn_item_payload_repair(
                &transaction,
                &stale_row,
                &pioneer_crud_payloads::storage::PreparedPayload::new_turn_item(
                    stale_repair.as_str(),
                )
                .unwrap(),
                None,
            )
            .await
            .expect("stale conditional repair should execute safely");
        transaction
            .commit()
            .await
            .expect("repair transaction should commit");
        assert!(!applied);

        let current = pioneer_entity::turn_item::Entity::find_by_id("row_repair_status_fence")
            .one(&connection)
            .await
            .expect("status-fence row should query")
            .expect("status-fence row should exist");
        assert_eq!(current.status.as_deref(), Some(TURN_ITEM_STATUS_FAILED));
        assert_eq!(current.payload, stale_row.payload);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn read_model_invariant_verifier_detects_and_repairs_terminal_tool_payload() {
    pioneer_postgres_test::with_database(async |fixture| {
        use pioneer_crud_conversations::turn_item::{
            prepare_turn_item_projection, upsert_prepared_turn_item,
        };
        let db = scoped(fixture).await;
        let store = ExecutionReadModelRepair::new(&db, NoEpisodicRepair);
        let timestamp = 1_700_000_000;
        let turn_id = "turn_invariant_repair";
        let item_id = "item_invariant_repair";
        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('ws_invariant_repair','Repair');
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
             VALUES ('thr_invariant_repair','ws_invariant_repair','','agent','m','p','active');
             INSERT INTO turn(id,thread_id,status,turn_kind,origin)
             VALUES ('turn_invariant_repair','thr_invariant_repair','in_progress','conversation','user');",
        ).await?;
        let at = unix_to_datetime(timestamp + 1);
        let prepared = prepare_turn_item_projection(
            turn_id, &safe_web_fetch_item(item_id), Some("in_progress"), at, at,
        )?;
        let tx = db.begin().await?;
        upsert_prepared_turn_item(&tx, prepared).await?;
        tx.commit().await?;

        pioneer_entity::turn_item::Entity::update_many()
            .filter(pioneer_entity::turn_item::Column::TurnId.eq(turn_id.to_owned()))
            .filter(pioneer_entity::turn_item::Column::ItemId.eq(item_id.to_owned()))
            .col_expr(
                pioneer_entity::turn_item::Column::Status,
                sea_orm::sea_query::Expr::value(Some("completed")),
            )
            .exec(&db)
            .await
            .expect("status mutation should succeed");

        // Exceed one repair quantum so the test also proves keyset progress
        // and durable processing across batch boundaries.
        for index in 0..=READ_MODEL_REPAIR_BATCH_SIZE {
            let extra_item_id = format!("item_invariant_repair_{index:03}");
            let extra_item = safe_web_fetch_item(extra_item_id.as_str());
            let item_timestamp = unix_to_datetime(timestamp + 1);
            pioneer_entity::turn_item::Entity::insert(pioneer_entity::turn_item::ActiveModel {
                id: Set(format!("row_invariant_repair_{index:03}")),
                turn_id: Set(turn_id.to_owned()),
                item_id: Set(extra_item_id),
                item_type: Set("web_fetch".to_owned()),
                status: Set(Some(TURN_ITEM_STATUS_COMPLETED.to_owned())),
                payload: Set(Some(
                    serde_json::to_value(&extra_item).expect("extra item should serialize"),
                )),
                active_attempt_number: Set(0),
                active_attempt_status: Set(None),
                active_attempt_id: Set(None),
                last_heartbeat_at: Set(None),
                lease_expires_at: Set(None),
                created_at: Set(item_timestamp),
                updated_at: Set(item_timestamp),
                ..Default::default()
            })
            .exec(&db)
            .await
            .expect("extra terminal turn_item should insert");
        }

        let violations = list_execution_invariant_violations(&db)
            .await
            .expect("invariant list should succeed");
        assert!(violations.iter().any(|violation| {
            matches!(
                violation.kind,
                ReadModelInvariantKind::TerminalToolPayloadInProgress
            )
        }));

        let mut timings = ReadModelRepairTimings::default();
        let summary = store
            .repair_terminal_turn_item_payloads(&mut timings)
            .await
            .expect("repair should succeed");
        let stage_timings = timings.into_stage_timings();
        assert!(summary.detected > READ_MODEL_REPAIR_BATCH_SIZE as usize);
        assert_eq!(summary.remaining, 0);
        let full_scan_commits = stage_timings.iter()
            .find(|timing| {
                timing.stage == ReadModelRepairStage::TerminalPayloadFullScanBatchCommit
            })
            .expect("full-scan commit timing should be reported");
        assert!(full_scan_commits.operations >= 2);
        assert!(stage_timings.iter().any(|timing| {
            timing.stage == ReadModelRepairStage::TerminalPayloadIncrementalComplete
        }));

        assert!(
            list_execution_invariant_violations(&db)
                .await
                .expect("post-repair invariant list should succeed")
                .is_empty()
        );

        let row = pioneer_crud_conversations::turn_reads::find_turn_item(&db, turn_id, item_id)
            .await
            .expect("turn_item lookup should succeed")
            .expect("turn_item row should exist");
        let payload: TurnItem = read_fixture_item(&db, row)
            .await
            .expect("payload should decode");
        let TurnItem::WebFetch { status, .. } = payload else {
            panic!("expected web_fetch payload");
        };
        assert_ne!(status, ToolCallStatus::InProgress);
        Ok(())
    })
    .await
    .unwrap();
}
