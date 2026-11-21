use super::*;
use crate::native_terminal_effect_outbox::{
    self as outbox, ClaimedNativeTerminalEffectRecord, NativeTerminalEffectClaimOutcome,
};
use pioneer_entity::native_terminal_effect_outbox as effect;
use pioneer_postgres_test::{TestDatabase, with_database};
use pioneer_protocol::{TaskResultCandidateStatus, generate_id};
use pioneer_sqlite::{
    SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass, SqliteWriteEvent,
    SqliteWriteExecutor, SqliteWriteObserver,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, EntityTrait, IntoActiveModel, QueryFilter, QueryTrait, Set,
    sea_query::Expr,
};
use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicUsize, Ordering},
};
use std::time::Duration;

const DB_ID_LEN: usize = 21;
const NOW: i64 = 1_900_000_000;

#[derive(Default)]
struct Routes {
    reads: AtomicUsize,
    writes: AtomicUsize,
    queued: AtomicUsize,
}
impl SqliteReadObserver for Routes {
    fn observe(&self, event: SqliteReadEvent) {
        if matches!(
            event,
            SqliteReadEvent::OperationFinished {
                class: SqliteReadClass::Maintenance,
                ..
            }
        ) {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl SqliteWriteObserver for Routes {
    fn observe(&self, event: SqliteWriteEvent) {
        match event {
            SqliteWriteEvent::Acquired {
                class: SqliteWriteClass::Maintenance,
                ..
            } => {
                self.writes.fetch_add(1, Ordering::SeqCst);
            }
            SqliteWriteEvent::Enqueued {
                class: SqliteWriteClass::Maintenance,
                ..
            } => {
                self.queued.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}
struct Fixture<'a> {
    db: SqliteDatabase,
    routes: Arc<Routes>,
    database: &'a TestDatabase,
}
impl<'a> Fixture<'a> {
    async fn open(database: &'a TestDatabase) -> Self {
        database.connection().execute_unprepared(
            "UPDATE workspace SET is_current=false WHERE is_current;
             INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws-gates','Gates',true,true)"
        ).await.unwrap();
        let routes = Arc::new(Routes::default());
        let db = Self::open_connection(database, &routes).await;
        Self {
            db,
            routes,
            database,
        }
    }
    async fn open_connection(database: &TestDatabase, routes: &Arc<Routes>) -> SqliteDatabase {
        // Queue the scheduling cases behind a deliberately occupied writer slot.
        let writer = database.connect_with_limit(1).await.unwrap();
        let reader = database.connect_read_only().await.unwrap();
        let db = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, routes.clone()),
            routes.clone(),
        );
        db.validate_reader().await.unwrap();
        db
    }
    async fn restart(&mut self) {
        self.db.clone().close().await.unwrap();
        self.db = Self::open_connection(self.database, &self.routes).await;
    }
    async fn effect(
        &self,
        suffix: &str,
        gate: pioneer_protocol::NativeTerminalEffectGate,
        committed: bool,
    ) -> String {
        let thread_id = format!("thr_{suffix}");
        let turn_id = format!("turn_{suffix}");
        self.db.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "INSERT INTO thread (id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class)
            VALUES ($1,'ws-gates','','agent','test','test','active','user','workspace')",
            [thread_id.clone().into()],
        )).await.unwrap();
        self.db
            .execute_raw(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "INSERT INTO turn (id,thread_id,status,turn_kind,origin)
            VALUES ($1,$2,'in_progress','conversation','user')",
                [turn_id.clone().into(), thread_id.clone().into()],
            ))
            .await
            .unwrap();
        let effect_id = format!("{turn_id}:terminal-effect:post-turn");
        TerminalEffectStore::new(&self.db)
            .prepare_native_terminal_effects(
                pioneer_protocol::NativeTerminalEffectPreparation {
                    batch_id: format!("batch_{suffix}"),
                    workspace_id: "ws-gates".into(),
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    runtime_generation: 1,
                    effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
                        effect_id: effect_id.clone(),
                        effect_kind: pioneer_protocol::NativeTerminalEffectKind::PostTurnHook,
                        gate,
                        payload: pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
                            request: serde_json::json!({"phase":"turn.post_turn"}),
                            runtime_snapshot: serde_json::json!({"schema_version":1}),
                        },
                        max_attempts: 3,
                    }],
                },
                NOW - 1,
            )
            .await
            .unwrap();
        if committed {
            let prepared = outbox::prepare_activation_for_terminal(&self.db, &turn_id)
                .await
                .unwrap();
            let tx = self.db.begin().await.unwrap();
            pioneer_crud_conversations::turn_writes::update_turn_status(
                &tx,
                &thread_id,
                &turn_id,
                pioneer_protocol::TurnStatus::Completed,
                None,
                unix_to_datetime(NOW),
            )
            .await
            .unwrap();
            outbox::activate_prepared_for_terminal(&tx, prepared, unix_to_datetime(NOW))
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        effect_id
    }
    async fn row(&self, id: &str) -> effect::Model {
        effect::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .unwrap()
            .unwrap()
    }

    async fn claim(&self, now: i64) -> Vec<ClaimedNativeTerminalEffectRecord> {
        self.claim_outcome(now, 10, 8, &|| now)
            .await
            .unwrap()
            .records
    }

    async fn claim_outcome(
        &self,
        now: i64,
        lease: u64,
        limit: u64,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<NativeTerminalEffectClaimOutcome> {
        outbox::claim_records(&self.db, now, lease, limit, clock).await
    }
}

#[tokio::test]
async fn poison_gate_reservation_failure_is_deferred_and_healthy_rows_progress() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let poison = f
            .effect(
                "a_poison",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        let healthy = f
            .effect(
                "z_healthy",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        f.db.execute_unprepared("CREATE FUNCTION fail_probe_once() RETURNS trigger LANGUAGE plpgsql AS $fault$
               BEGIN RAISE EXCEPTION 'probe failure'; RETURN NEW; END $fault$;
               CREATE TRIGGER fail_probe_once BEFORE UPDATE OF gate_probe_token ON native_terminal_effect_outbox
               FOR EACH ROW WHEN (NEW.gate_probe_token IS NOT NULL AND NEW.effect_id LIKE 'turn_a_poison:%') EXECUTE FUNCTION fail_probe_once()").await.unwrap();
        assert!(f.claim(NOW).await.is_empty());
        let poison_row = f.row(&poison).await;
        assert_eq!(poison_row.gate_probe_at, NOW + 5);
        assert_eq!(poison_row.gate_probe_token, None);
        assert_eq!(poison_row.attempt_count, 0);
        assert_eq!(f.row(&healthy).await.gate_probe_attempts, 1);
        f.db
            .execute_unprepared("DROP TRIGGER fail_probe_once ON native_terminal_effect_outbox")
            .await
            .unwrap();
        let mut at = NOW + 5;
        for delay in [
            10, 20, 40, 80, 160, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300,
        ] {
            let row = f.row(&poison).await;
            let next = outbox::reserve_gate_probe(
                &f.db,
                &row,
                Some(generate_id(DB_ID_LEN)),
                &|| at,
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(next.gate_probe_at, at + delay);
            assert!(next.gate_probe_attempts <= 16);
            assert_eq!(next.attempt_count, 0);
            at += delay;
        }
        assert_eq!(f.row(&poison).await.gate_probe_attempts, 16);
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn unavailable_probe_bookkeeping_reports_storage_error_after_other_rows_progress() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let poison = f
            .effect(
                "a_storage",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        let healthy = f
            .effect(
                "z_storage",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        let ready = f
            .effect(
                "storage_ready",
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await;
        f.db.execute_unprepared("CREATE FUNCTION fail_probe_bookkeeping() RETURNS trigger LANGUAGE plpgsql AS $fault$
               BEGIN RAISE EXCEPTION 'storage failure'; RETURN NEW; END $fault$;
               CREATE TRIGGER fail_probe_bookkeeping BEFORE UPDATE OF gate_probe_at ON native_terminal_effect_outbox
               FOR EACH ROW WHEN (NEW.effect_id LIKE 'turn_a_storage:%') EXECUTE FUNCTION fail_probe_bookkeeping()").await.unwrap();
        let outcome = f.claim_outcome(NOW, 10, 8, &|| NOW)
            .await
            .unwrap();
        assert!(outcome.storage_failed);
        assert_eq!(outcome.records.len(), 1);
        assert_eq!(outcome.records[0].effect_id, ready);
        assert_eq!(f.row(&poison).await.gate_probe_attempts, 0);
        assert_eq!(f.row(&healthy).await.gate_probe_attempts, 1);
        assert_eq!(f.row(&ready).await.status, "running");
        assert_eq!(f.row(&ready).await.attempt_count, 1);
        assert!(
            TerminalEffectStore::new(&f.db).complete_native_terminal_effect(&ready, &outcome.records[0].claim_token, NOW + 1)
                .await
                .unwrap()
        );
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn exhausted_final_running_lease_and_three_claim_pages_share_eight_inputs() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        for status in ["ready", "retry_wait", "running"] {
            for i in 0..10 {
                let id = f
                    .effect(
                        &format!("mixed_{status}_{i:02}"),
                        pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                        true,
                    )
                    .await;
                let mut row = f.row(&id).await.into_active_model();
                row.status = Set(status.into());
                // Four exhausted rows in each due prefix must consume input slots.
                row.attempt_count = Set(if i < 4 { 3 } else { 1 });
                if status == "running" {
                    row.claim_token = Set(Some(format!("old_{i}")));
                    row.claim_expires_at = Set(Some(unix_to_datetime(NOW)));
                }
                row.update(&f.db).await.unwrap();
            }
        }
        assert!(f.claim(NOW).await.is_empty());
        let unresolved = effect::Entity::find()
            .filter(effect::Column::Status.eq("unresolved"))
            .all(&f.db)
            .await
            .unwrap();
        assert_eq!(unresolved.len(), 8);
        for status in ["ready", "retry_wait", "running"] {
            assert!(unresolved.iter().any(|r| r.effect_id.contains(status)));
        }
        assert!(
            unresolved
                .iter()
                .all(|r| r.last_error_code.as_deref() == Some("retry_exhausted")
                    && r.claim_token.is_none())
        );
        let records = f.claim(NOW + 2).await;
        assert!(records.len() <= 8);
        assert!(!records.is_empty());
        assert!(records.iter().all(|r| r.attempt_count <= r.max_attempts));
        for row in &unresolved {
            assert!(
                !TerminalEffectStore::new(&f.db)
                    .complete_native_terminal_effect(&row.effect_id, "old_0", NOW + 2)
                    .await
                    .unwrap()
            );
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn total_quantum_is_sixteen_inputs_with_malformed_payload_and_checkpoint_progress() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        for i in 0..10 {
            f.effect(
                &format!("probe_{i}"),
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        }
        for i in 0..10 {
            let id = f
                .effect(
                    &format!("execute_{i}"),
                    pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                    true,
                )
                .await;
            if i < 2 {
                let mut row = f.row(&id).await.into_active_model();
                if i == 0 {
                    row.payload_json = Set("malformed".into());
                } else {
                    row.handler_checkpoint_json = Set(Some("{}".into()));
                    row.handler_checkpoint_sha256 = Set(Some("0".repeat(64)));
                }
                row.update(&f.db).await.unwrap();
            }
        }
        let records = f.claim(NOW).await;
        assert!(!records.is_empty());
        assert!(records.len() <= 6);
        let rows = effect::Entity::find().all(&f.db).await.unwrap();
        assert_eq!(rows.iter().filter(|r| r.gate_probe_attempts > 0).count(), 8);
        let execution_inputs = rows.iter().filter(|r| r.attempt_count > 0).count();
        assert!(execution_inputs <= 8);
        assert!(execution_inputs + rows.iter().filter(|r| r.gate_probe_attempts > 0).count() <= 16);
        assert_eq!(rows.iter().filter(|r| r.status == "unresolved").count(), 2);
        assert!(f.routes.reads.load(Ordering::SeqCst) > 0);
        assert!(f.routes.writes.load(Ordering::SeqCst) > 0);
        let db = f.db.maintenance();
        assert_eq!(db.read_class(), SqliteReadClass::Maintenance);
        assert_eq!(db.write_class(), SqliteWriteClass::Maintenance);
        db.validate_reader().await.unwrap();
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn clock_is_read_after_writer_wait_and_cancellation_preserves_reservation() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let id = f
            .effect(
                "clock",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        let row = f.row(&id).await;
        let maintenance = f.db.maintenance();
        let tx = f.db.begin().await.unwrap();
        let now = Arc::new(AtomicI64::new(NOW));
        let queued_before = f.routes.queued.load(Ordering::SeqCst);
        let reservation = {
            let db = maintenance.clone();
            let row = row.clone();
            let now = now.clone();
            tokio::spawn(async move {
                outbox::reserve_gate_probe(&db, &row, Some("clock-token".into()), &|| {
                    now.load(Ordering::SeqCst)
                })
                .await
            })
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            while f.routes.queued.load(Ordering::SeqCst) == queued_before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        now.store(NOW + 100, Ordering::SeqCst);
        tx.commit().await.unwrap();
        let reserved = reservation.await.unwrap().unwrap().unwrap();
        assert_eq!(reserved.gate_probe_at, NOW + 105);
        // Cancellation after the durable reservation does not reset its deadline.
        let tx = f.db.begin().await.unwrap();
        let probe = {
            let db = maintenance.clone();
            let reserved = reserved.clone();
            tokio::spawn(async move {
                outbox::defer_gate_probe(&db, &reserved, true, &|| NOW + 200).await
            })
        };
        tokio::task::yield_now().await;
        probe.abort();
        assert!(probe.await.unwrap_err().is_cancelled());
        tx.rollback().await.unwrap();
        assert_eq!(f.row(&id).await.gate_probe_at, NOW + 105);
        let ready = f
            .effect(
                "lease_clock",
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await;
        let tx = f.db.begin().await.unwrap();
        let queued_before = f.routes.queued.load(Ordering::SeqCst);
        let claim = {
            let db = maintenance.clone();
            let now = now.clone();
            tokio::spawn(async move {
                outbox::claim_due(
                    &db,
                    unix_to_datetime(NOW),
                    90,
                    8,
                    || generate_id(DB_ID_LEN),
                    &|| now.load(Ordering::SeqCst),
                )
                .await
            })
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            while f.routes.queued.load(Ordering::SeqCst) == queued_before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        now.store(NOW + 200, Ordering::SeqCst);
        tx.commit().await.unwrap();
        let claims = claim.await.unwrap().unwrap().claimed;
        assert_eq!(claims.len(), 1);
        assert_eq!(
            f.row(&ready).await.claim_expires_at,
            Some(unix_to_datetime(NOW + 290))
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn planner_uses_four_covering_seeks_gate_partial_range_and_three_claim_ranges() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let db = f.db.maintenance();
        for status in ["accepted", "rejected", "superseded", "cancelled"] {
            let statement = outbox::candidate_metadata_seek("thread", "turn", status)
                .build(DatabaseBackend::Postgres);
            assert!(!statement.sql.contains("result_json"));
            assert!(!statement.sql.contains("diagnostics_json"));
            assert!(!statement.sql.contains("summary"));
            assert!(statement.sql.contains("LIMIT"));
            assert_plan(
                &db,
                statement,
                "idx_task_result_candidate_gate",
                &["thread_id", "turn_id", "status"],
            )
            .await;
        }
        let gate_statement = outbox::gate_due_page(NOW, 8).build(db.get_database_backend());
        for analyzed in [false, true] {
            if analyzed {
                db.execute_unprepared("ANALYZE").await.unwrap();
            }
            assert_plan(
                &db,
                gate_statement.clone(),
                "idx_native_terminal_effect_gate_due",
                &["gate_probe_at"],
            )
            .await;
        }
        for status in ["ready", "retry_wait", "running"] {
            let statement = outbox::claim_page(status, unix_to_datetime(NOW), 3)
                .build(DatabaseBackend::Postgres);
            assert!(!statement.sql.contains("attempt_count\" <"));
            let (index, range) = if status == "running" {
                ("idx_native_terminal_effect_expired", "claim_expires_at")
            } else {
                ("idx_native_terminal_effect_due", "next_run_at")
            };
            assert_plan(&db, statement, index, &["status", range]).await;
        }
        Ok(())
    })
    .await
    .unwrap();
}

async fn assert_plan(
    db: &SqliteDatabase,
    mut statement: sea_orm::Statement,
    index: &str,
    keys: &[&str],
) {
    let tx = db.begin().await.unwrap();
    // Tiny fixtures have no meaningful cost advantage from index scans.
    tx.execute_unprepared("SET LOCAL enable_seqscan=off")
        .await
        .unwrap();
    statement.sql = format!("EXPLAIN {}", statement.sql);
    let details = tx
        .query_all_raw(statement)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "QUERY PLAN").unwrap())
        .collect::<Vec<_>>();
    assert!(
        details
            .iter()
            .any(|line| line.contains("Index") && line.contains(index)),
        "{details:?}"
    );
    assert!(
        details
            .iter()
            .any(|line| line.contains("Index Cond:") && keys.iter().all(|key| line.contains(key))),
        "{details:?}"
    );
    assert!(
        !details
            .iter()
            .any(|line| line.contains("Sort") || line.contains("Seq Scan")),
        "{details:?}"
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn point_claim_failure_preserves_healthy_claims_before_and_after_poison() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let mut ids = Vec::new();
        for suffix in ["a_claim", "b_claim", "c_claim"] {
            ids.push(
                f.effect(
                    suffix,
                    pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                    true,
                )
                .await,
            );
        }
        f.db.execute_unprepared("CREATE FUNCTION fail_second_claim() RETURNS trigger LANGUAGE plpgsql AS $fault$
               BEGIN RAISE EXCEPTION 'claim failure'; RETURN NEW; END $fault$;
               CREATE TRIGGER fail_second_claim BEFORE UPDATE OF claim_token ON native_terminal_effect_outbox
               FOR EACH ROW WHEN (NEW.effect_id LIKE 'turn_b_claim:%' AND NEW.status = 'running') EXECUTE FUNCTION fail_second_claim()").await.unwrap();
        let outcome = f.claim_outcome(NOW, 10, 8, &|| NOW)
            .await
            .unwrap();
        assert!(outcome.storage_failed);
        assert_eq!(
            outcome
                .records
                .iter()
                .map(|record| &record.effect_id)
                .collect::<Vec<_>>(),
            vec![&ids[0], &ids[2]]
        );
        assert_eq!(f.row(&ids[0]).await.status, "running");
        assert_eq!(f.row(&ids[0]).await.attempt_count, 1);
        assert_eq!(f.row(&ids[1]).await.status, "ready");
        assert_eq!(f.row(&ids[1]).await.attempt_count, 0);
        assert_eq!(
            f.row(&ids[1]).await.next_run_at,
            Some(unix_to_datetime(NOW + 5))
        );
        // The fault is permanent; a later quantum can still confirm other rows.
        assert!(f.claim(NOW + 1).await.is_empty());
        let outcome = f.claim_outcome(NOW + 5, 10, 8, &|| NOW + 5)
            .await
            .unwrap();
        assert!(outcome.storage_failed);
        assert!(outcome.records.is_empty());
        assert_eq!(f.row(&ids[1]).await.attempt_count, 0);
        Ok(())
    }).await.unwrap();
}

#[test]
fn preparation_keeps_effect_count_and_payload_byte_bounds() {
    let spec = pioneer_protocol::NativeTerminalEffectSpec {
        effect_id: "bound-effect".into(),
        effect_kind: pioneer_protocol::NativeTerminalEffectKind::PostTurnHook,
        gate: pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
        payload: pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
            request: serde_json::json!({}),
            runtime_snapshot: serde_json::json!({}),
        },
        max_attempts: 3,
    };
    let mut preparation = pioneer_protocol::NativeTerminalEffectPreparation {
        batch_id: "bound-batch".into(),
        workspace_id: "ws-gates".into(),
        thread_id: "bound-thread".into(),
        turn_id: "bound-turn".into(),
        runtime_generation: 1,
        effects: vec![spec.clone(); outbox::MAX_EFFECTS_PER_TURN + 1],
    };
    assert!(outbox::prepare_input(preparation.clone()).is_err());
    preparation.effects = vec![spec];
    preparation.effects[0].payload = pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
        request: serde_json::json!({"large":"x".repeat(outbox::MAX_EFFECT_PAYLOAD_BYTES + 1)}),
        runtime_snapshot: serde_json::json!({}),
    };
    assert!(outbox::prepare_input(preparation).is_err());
}

#[tokio::test]
async fn failed_claim_commit_never_returns_dispatchable_effects() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let id = f
            .effect(
                "commit_failure",
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await;
        // Mutation/reload succeed; the deferred FK makes the commit fail. The
        // API must never return the records prepared before this failed commit.
        f.db.execute_unprepared("CREATE TABLE claim_commit_check (workspace_id TEXT REFERENCES workspace(id) DEFERRABLE INITIALLY DEFERRED)").await.unwrap();
        f.db.execute_unprepared("CREATE FUNCTION fail_claim_commit() RETURNS trigger LANGUAGE plpgsql AS $fault$
               BEGIN INSERT INTO claim_commit_check VALUES('missing-workspace'); RETURN NEW; END $fault$;
               CREATE TRIGGER fail_claim_commit AFTER UPDATE OF claim_token ON native_terminal_effect_outbox
               FOR EACH ROW WHEN (NEW.status = 'running') EXECUTE FUNCTION fail_claim_commit()").await.unwrap();
        let outcome = f.claim_outcome(NOW, 10, 8, &|| NOW)
            .await
            .unwrap();
        assert!(outcome.storage_failed);
        assert!(outcome.records.is_empty());
        assert_eq!(f.row(&id).await.status, "ready");
        assert_eq!(f.row(&id).await.attempt_count, 0);
        f.db
            .execute_unprepared("DROP TRIGGER fail_claim_commit ON native_terminal_effect_outbox")
            .await
            .unwrap();
        assert_eq!(f.claim(NOW + 5).await.len(), 1);
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn permanent_failed_reservations_back_off_across_restart_without_double_increment() {
    with_database(async |database| {
        let mut f = Fixture::open(database).await;
        let id = f
            .effect(
                "permanent_probe",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        f.db.execute_unprepared("CREATE FUNCTION fail_every_reservation() RETURNS trigger LANGUAGE plpgsql AS $fault$
               BEGIN RAISE EXCEPTION 'reservation failure'; RETURN NEW; END $fault$;
               CREATE TRIGGER fail_every_reservation BEFORE UPDATE OF gate_probe_token ON native_terminal_effect_outbox
               FOR EACH ROW WHEN (NEW.gate_probe_token IS NOT NULL) EXECUTE FUNCTION fail_every_reservation()").await.unwrap();
        let mut now = NOW;
        for attempt in 1..=18 {
            if attempt == 4 {
                f.restart().await;
            }
            let outcome = f.claim_outcome(now, 10, 8, &|| now)
                .await
                .unwrap();
            assert!(outcome.storage_failed);
            assert!(outcome.records.is_empty());
            let row = f.row(&id).await;
            let delay = [5, 10, 20, 40, 80, 160, 300][usize::min(attempt - 1, 6)];
            assert_eq!(
                row.gate_probe_attempts,
                i64::try_from(usize::min(attempt, 16)).unwrap()
            );
            assert_eq!(row.gate_probe_at, now + delay);
            assert_eq!(row.gate_probe_token, None);
            assert_eq!(row.attempt_count, 0);
            now += delay;
        }
        f.db
            .execute_unprepared("DROP TRIGGER fail_every_reservation ON native_terminal_effect_outbox")
            .await
            .unwrap();
        let before = f.row(&id).await;
        let reserved = outbox::reserve_gate_probe(
            &f.db,
            &before,
            Some("committed-probe".into()),
            &|| now,
        )
        .await
        .unwrap()
        .unwrap();
        outbox::defer_gate_probe(&f.db, &reserved, true, &|| now + 10)
            .await
            .unwrap();
        assert_eq!(
            f.row(&id).await.gate_probe_attempts,
            reserved.gate_probe_attempts
        );
        outbox::defer_gate_probe(&f.db, &before, false, &|| now + 1000)
            .await
            .unwrap();
        assert_eq!(f.row(&id).await.gate_probe_token, reserved.gate_probe_token);
        assert_eq!(f.row(&id).await.gate_probe_attempts, 16);
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn failed_prefix_claim_uses_one_input_and_does_not_hide_large_backlog() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let mut ids = Vec::new();
        for i in 0..12 {
            ids.push(
                f.effect(
                    &format!("prefix_{i:02}"),
                    pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                    true,
                )
                .await,
            );
        }
        f.db.execute_unprepared("CREATE FUNCTION fail_prefix() RETURNS trigger LANGUAGE plpgsql AS $fault$
               BEGIN RAISE EXCEPTION 'claim failure'; RETURN NEW; END $fault$;
               CREATE TRIGGER fail_prefix BEFORE UPDATE OF claim_token ON native_terminal_effect_outbox
               FOR EACH ROW WHEN (NEW.effect_id LIKE 'turn_prefix_00:%') EXECUTE FUNCTION fail_prefix()").await.unwrap();
        // Rotation puts ready last; the two empty pages donate all eight slots.
        let at = NOW + 4;
        let outcome = f.claim_outcome(at, 10, 8, &|| at)
            .await
            .unwrap();
        assert!(outcome.storage_failed);
        assert_eq!(outcome.records.len(), 7);
        assert_eq!(f.row(&ids[0]).await.attempt_count, 0);
        assert_eq!(
            f.row(&ids[0]).await.next_run_at,
            Some(unix_to_datetime(at + 5))
        );
        for id in &ids[8..] {
            assert_eq!(f.row(id).await.attempt_count, 0);
        }
        let second = f.claim_outcome(at, 10, 8, &|| at)
            .await
            .unwrap();
        assert!(!second.storage_failed);
        assert_eq!(second.records.len(), 4);
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn exhausted_transition_failure_and_unavailable_deferral_preserve_other_claims() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let poison = f
            .effect(
                "a_exhausted",
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await;
        let healthy = f
            .effect(
                "z_after_exhausted",
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await;
        let mut row = f.row(&poison).await.into_active_model();
        row.attempt_count = Set(3);
        row.update(&f.db).await.unwrap();
        f.db.execute_unprepared("CREATE FUNCTION fail_exhausted() RETURNS trigger LANGUAGE plpgsql AS $fault$
               BEGIN RAISE EXCEPTION 'exhausted failure'; RETURN NEW; END $fault$;
               CREATE TRIGGER fail_exhausted BEFORE UPDATE OF status ON native_terminal_effect_outbox
               FOR EACH ROW WHEN (NEW.effect_id LIKE 'turn_a_exhausted:%' AND NEW.status = 'unresolved') EXECUTE FUNCTION fail_exhausted()").await.unwrap();
        let outcome = f.claim_outcome(NOW, 10, 8, &|| NOW)
            .await
            .unwrap();
        assert!(outcome.storage_failed);
        assert_eq!(outcome.records.len(), 1);
        assert_eq!(outcome.records[0].effect_id, healthy);
        assert_eq!(f.row(&poison).await.status, "ready");
        assert_eq!(f.row(&poison).await.attempt_count, 3);
        assert_eq!(
            f.row(&poison).await.next_run_at,
            Some(unix_to_datetime(NOW + 5))
        );
        f.db.execute_unprepared("CREATE FUNCTION fail_execution_deferral() RETURNS trigger LANGUAGE plpgsql AS $fault$
               BEGIN RAISE EXCEPTION 'deferral failure'; RETURN NEW; END $fault$;
               CREATE TRIGGER fail_execution_deferral BEFORE UPDATE OF next_run_at ON native_terminal_effect_outbox
               FOR EACH ROW WHEN (NEW.effect_id LIKE 'turn_a_exhausted:%') EXECUTE FUNCTION fail_execution_deferral()").await.unwrap();
        let next = f
            .effect(
                "healthy_next",
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await;
        let outcome = f.claim_outcome(NOW + 5, 10, 8, &|| NOW + 5)
            .await
            .unwrap();
        assert!(outcome.storage_failed);
        assert_eq!(outcome.records.len(), 1);
        assert_eq!(outcome.records[0].effect_id, next);
        assert_eq!(f.row(&poison).await.attempt_count, 3);
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn execution_failure_deferral_is_fenced_against_claim_completion_and_new_owner() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let id = f
            .effect(
                "stale_execution",
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await;
        let before = f.row(&id).await;
        let first = f.claim(NOW).await.pop().unwrap();
        assert!(
            !outbox::defer_execution_claim(&f.db, &before, &|| NOW + 100)
                .await
                .unwrap()
        );
        let running = f.row(&id).await;
        let second = f.claim(NOW + 10).await.pop().unwrap();
        let current = f.row(&id).await;
        assert!(
            !outbox::defer_execution_claim(&f.db, &running, &|| NOW + 200)
                .await
                .unwrap()
        );
        assert_eq!(f.row(&id).await.claim_token, current.claim_token);
        assert_eq!(f.row(&id).await.claim_expires_at, current.claim_expires_at);
        assert!(
            !TerminalEffectStore::new(&f.db)
                .complete_native_terminal_effect(&id, &first.claim_token, NOW + 11)
                .await
                .unwrap()
        );
        assert!(
            TerminalEffectStore::new(&f.db)
                .complete_native_terminal_effect(&id, &second.claim_token, NOW + 11)
                .await
                .unwrap()
        );
        assert!(
            !outbox::defer_execution_claim(&f.db, &current, &|| NOW + 300)
                .await
                .unwrap()
        );
        assert_eq!(f.row(&id).await.status, "succeeded");
        assert_eq!(f.row(&id).await.claim_token, None);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn failed_commit_of_one_claim_keeps_independently_confirmed_neighbors() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let mut ids = Vec::new();
        for suffix in ["a_confirmed", "b_commit_error", "c_confirmed"] {
            ids.push(
                f.effect(
                    suffix,
                    pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                    true,
                )
                .await,
            );
        }
        f.db.execute_unprepared("CREATE TABLE point_commit_check (workspace_id TEXT REFERENCES workspace(id) DEFERRABLE INITIALLY DEFERRED)").await.unwrap();
        f.db.execute_unprepared("CREATE FUNCTION fail_point_commit() RETURNS trigger LANGUAGE plpgsql AS $fault$
               BEGIN INSERT INTO point_commit_check VALUES('missing-workspace'); RETURN NEW; END $fault$;
               CREATE TRIGGER fail_point_commit AFTER UPDATE OF claim_token ON native_terminal_effect_outbox
               FOR EACH ROW WHEN (NEW.effect_id LIKE 'turn_b_commit_error:%' AND NEW.status = 'running') EXECUTE FUNCTION fail_point_commit()").await.unwrap();
        let outcome = f.claim_outcome(NOW, 10, 8, &|| NOW)
            .await
            .unwrap();
        assert!(outcome.storage_failed);
        assert_eq!(
            outcome
                .records
                .iter()
                .map(|record| &record.effect_id)
                .collect::<Vec<_>>(),
            vec![&ids[0], &ids[2]]
        );
        assert_eq!(f.row(&ids[1]).await.status, "ready");
        assert_eq!(f.row(&ids[1]).await.attempt_count, 0);
        assert_eq!(
            f.row(&ids[1]).await.next_run_at,
            Some(unix_to_datetime(NOW + 5))
        );
        assert!(f.row(&ids[0]).await.claim_token.is_some());
        assert!(f.row(&ids[2]).await.claim_token.is_some());
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn expired_running_failure_deferral_reads_clock_after_writer_wait_and_preserves_owner() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let id = f
            .effect(
                "running_deferral",
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await;
        let claim = f.claim(NOW).await.pop().unwrap();
        let snapshot = f.row(&id).await;
        let tx = f.db.begin().await.unwrap();
        let queued_before = f.routes.queued.load(Ordering::SeqCst);
        let clock = Arc::new(AtomicI64::new(NOW + 10));
        let task = {
            let db = f.db.maintenance();
            let snapshot = snapshot.clone();
            let clock = clock.clone();
            tokio::spawn(async move {
                outbox::defer_execution_claim(&db, &snapshot, &|| clock.load(Ordering::SeqCst))
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            while f.routes.queued.load(Ordering::SeqCst) == queued_before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        clock.store(NOW + 100, Ordering::SeqCst);
        tx.commit().await.unwrap();
        assert!(task.await.unwrap().unwrap());
        let deferred = f.row(&id).await;
        assert_eq!(deferred.claim_expires_at, Some(unix_to_datetime(NOW + 105)));
        assert_eq!(
            deferred.claim_token.as_deref(),
            Some(claim.claim_token.as_str())
        );
        assert_eq!(deferred.attempt_count, 1);
        assert_eq!(deferred.updated_at, snapshot.updated_at);
        assert_eq!(deferred.next_run_at, snapshot.next_run_at);
        assert!(f.claim(NOW + 104).await.is_empty());
        // Deferring recovery does not revoke the handler's existing ownership.
        assert!(
            TerminalEffectStore::new(&f.db)
                .complete_native_terminal_effect(&id, &claim.claim_token, NOW + 104)
                .await
                .unwrap()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn lost_claim_acknowledgement_does_not_allow_snapshot_repair_or_redispatch() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let id = f
            .effect(
                "a_lost_ack",
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await;
        let snapshot = f.row(&id).await;
        // Model the durable side of an ambiguous commit: the row committed, but
        // its acknowledgement/record was lost before dispatch. No mock writer or
        // alternative production claim path is needed to exercise recovery CAS.
        let confirmed = outbox::claim_due(
            &f.db.maintenance(),
            unix_to_datetime(NOW),
            10,
            1,
            || "lost-ack-token".into(),
            &|| NOW,
        )
        .await
        .unwrap();
        assert_eq!(confirmed.claimed.len(), 1);
        drop(confirmed);
        let committed = f.row(&id).await;
        assert!(
            !outbox::defer_execution_claim(&f.db, &snapshot, &|| NOW + 100)
                .await
                .unwrap()
        );
        assert_eq!(f.row(&id).await.claim_token, committed.claim_token);
        assert_eq!(
            f.row(&id).await.claim_expires_at,
            committed.claim_expires_at
        );
        assert_eq!(f.row(&id).await.attempt_count, 1);
        let mut healthy = Vec::new();
        for suffix in ["b_after_lost_ack", "c_after_lost_ack"] {
            healthy.push(
                f.effect(
                    suffix,
                    pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                    true,
                )
                .await,
            );
        }
        let outcome = f.claim_outcome(NOW, 10, 8, &|| NOW).await.unwrap();
        assert!(!outcome.storage_failed);
        assert_eq!(
            outcome
                .records
                .iter()
                .map(|record| &record.effect_id)
                .collect::<Vec<_>>(),
            healthy.iter().collect::<Vec<_>>()
        );
        assert!(outcome.records.iter().all(|record| record.effect_id != id));
        assert_eq!(f.row(&id).await.claim_token, committed.claim_token);
        assert_eq!(f.row(&id).await.attempt_count, 1);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn failed_claim_discovery_page_keeps_its_input_quota_and_other_statuses_progress() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        for status in ["ready", "retry_wait", "running"] {
            for i in 0..8 {
                let id = f
                    .effect(
                        &format!("discovery_{status}_{i:02}"),
                        pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                        true,
                    )
                    .await;
                let mut row = f.row(&id).await.into_active_model();
                row.status = Set(status.into());
                if status == "running" {
                    row.attempt_count = Set(1);
                    row.claim_token = Set(Some(format!("discovery-owner-{i}")));
                    row.claim_expires_at = Set(Some(unix_to_datetime(NOW)));
                }
                row.update(&f.db).await.unwrap();
            }
        }
        // Fail the ready page through a private schema visible only to the
        // reader. Its full quota still counts against this attempt.
        use sea_orm::Iterable;
        use sea_orm::sea_query::Query;
        let db = database.connection();
        db.execute_unprepared(
            "CREATE SCHEMA terminal_fixture;
             CREATE FUNCTION terminal_fixture.read_timestamp(id text, stamp timestamptz)
             RETURNS timestamptz LANGUAGE plpgsql AS $$
             BEGIN
               IF starts_with(id, 'turn_discovery_ready_00:') THEN
                 RAISE EXCEPTION 'fixture could not read this ready page';
               END IF;
               RETURN stamp;
             END $$;",
        )
        .await
        .unwrap();
        let discovery = Query::select()
            .from(effect::Entity)
            .columns(
                effect::Column::iter()
                    .filter(|column| !matches!(column, effect::Column::UpdatedAt)),
            )
            .expr_as(
                Expr::cust("terminal_fixture.read_timestamp(effect_id, updated_at)"),
                "updated_at",
            )
            .to_owned();
        let sql = DatabaseBackend::Postgres.build(&discovery).sql;
        db.execute_unprepared(&format!(
            "CREATE VIEW terminal_fixture.native_terminal_effect_outbox AS {sql}"
        ))
        .await
        .unwrap();
        let reader = database
            .connect_with_options(|options| {
                options.map_sqlx_postgres_opts(|opts| {
                    opts.options([
                        ("search_path", "terminal_fixture,public"),
                        ("default_transaction_read_only", "on"),
                    ])
                });
            })
            .await
            .unwrap();
        let scoped = SqliteDatabase::new(reader, db.clone());
        let at = NOW + 2; // ready first, then retry_wait, then running
        let outcome = outbox::claim_records(&scoped, at, 10, 8, &|| at)
            .await
            .unwrap();
        assert!(outcome.storage_failed);
        assert_eq!(outcome.records.len(), 5);
        assert_eq!(
            outcome
                .records
                .iter()
                .filter(|record| record.effect_id.contains("retry_wait"))
                .count(),
            3
        );
        assert_eq!(
            outcome
                .records
                .iter()
                .filter(|record| record.effect_id.contains("running"))
                .count(),
            2
        );
        assert!(
            outcome
                .records
                .iter()
                .all(|record| !record.effect_id.contains("ready"))
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn malformed_terminal_effect_checkpoint_is_quarantined_before_handler_execution() {
    assert_terminal_checkpoint_quarantine(serde_json::json!({"schema_version": 1}), false).await;
}

fn checkpoint_test_hook_snapshot(hook_id: &str) -> serde_json::Value {
    let hook_id = pioneer_hooks::HookId::new(hook_id).unwrap();
    let subscription = pioneer_hooks::HookSubscription::new(
        pioneer_hooks::HookSubscriptionId::new("test.post_turn").unwrap(),
        hook_id.clone(),
        pioneer_hooks::HookPhase::TurnPostTurn,
    );
    let descriptor = pioneer_hooks::HookHandlerDescriptor {
        hook_id,
        hook_kind: pioneer_hooks::HookKind::new("memory").unwrap(),
        supported_phases: vec![pioneer_hooks::HookPhase::TurnPostTurn],
        version: 1,
        input_contract_version: 1,
        output_contract_version: 1,
        default_execution_policy: Default::default(),
        default_failure_policy: pioneer_hooks::HookFailurePolicy::BestEffort,
        capabilities: pioneer_hooks::HookCapabilities::new([pioneer_hooks::HookCapability::new(
            "idempotent_side_effect",
        )
        .unwrap()]),
    };
    serde_json::json!({"schema_version":1, "subscriptions":[subscription], "handlers":[descriptor]})
}

#[tokio::test]
async fn checkpoint_quarantine_is_generic_even_with_memory_subscription() {
    assert_terminal_checkpoint_quarantine(
        checkpoint_test_hook_snapshot("memory.post_turn_extractor"),
        false,
    )
    .await;
}

#[tokio::test]
async fn checkpoint_quarantine_does_not_attribute_other_hook_or_kind_to_memory() {
    assert_terminal_checkpoint_quarantine(
        checkpoint_test_hook_snapshot("test.other_post_turn"),
        false,
    )
    .await;
    assert_terminal_checkpoint_quarantine(serde_json::json!({}), true).await;
}

async fn assert_terminal_checkpoint_quarantine(runtime_snapshot: serde_json::Value, cleanup: bool) {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_005_250;
        let workspace_id = "ws_terminal_effect_bad_checkpoint";
        let thread_id = "thr_terminal_effect_bad_checkpoint";
        let turn_id = "turn_terminal_effect_bad_checkpoint";
        let db = SqliteDatabase::new(
            fixture.connect_read_only().await?,
            fixture.connection().clone(),
        );
        db.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('ws_terminal_effect_bad_checkpoint', 'Terminal effect');
             INSERT INTO thread (id,workspace_id,preview,mode,model,model_provider,status)
                 VALUES ('thr_terminal_effect_bad_checkpoint','ws_terminal_effect_bad_checkpoint',
                         '', 'agent', 'model', 'provider', 'active');
             INSERT INTO turn (id,thread_id,status)
                 VALUES ('turn_terminal_effect_bad_checkpoint','thr_terminal_effect_bad_checkpoint',
                         'in_progress')",
        ).await?;
        let store = TerminalEffectStore::new(&db);
        let effect_id = format!("{turn_id}:terminal-effect:post-turn");
        store
            .prepare_native_terminal_effects(
                pioneer_protocol::NativeTerminalEffectPreparation {
                    batch_id: format!("{turn_id}:batch:bad-checkpoint"),
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    runtime_generation: 1,
                    effects: vec![
                        pioneer_protocol::NativeTerminalEffectSpec {
                            effect_id: effect_id.clone(),
                            effect_kind: if cleanup {
                                pioneer_protocol::NativeTerminalEffectKind::AttachedTaskCleanup
                            } else {
                                pioneer_protocol::NativeTerminalEffectKind::PostTurnHook
                            },
                            gate: pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                            payload: if cleanup {
                                pioneer_protocol::NativeTerminalEffectPayload::AttachedTaskCleanup {
                                    reason: "parent turn completed".to_owned(),
                                    runtime_contract: "pioneer.test.attached-task-cleanup.v1"
                                        .to_owned(),
                                }
                            } else {
                                pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
                                    request: serde_json::json!({"phase": "turn.post_turn"}),
                                    runtime_snapshot,
                                }
                            },
                            max_attempts: 3,
                        },
                        pioneer_protocol::NativeTerminalEffectSpec {
                            effect_id: format!("{turn_id}:terminal-effect:healthy"),
                            effect_kind: if cleanup {
                                pioneer_protocol::NativeTerminalEffectKind::PostTurnHook
                            } else {
                                pioneer_protocol::NativeTerminalEffectKind::AttachedTaskCleanup
                            },
                            gate: pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                            payload: if cleanup {
                                pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
                                    request: serde_json::json!({}),
                                    runtime_snapshot: serde_json::json!({}),
                                }
                            } else {
                                pioneer_protocol::NativeTerminalEffectPayload::AttachedTaskCleanup {
                                    reason: "completed".to_owned(),
                                    runtime_contract: "pioneer.test.attached-task-cleanup.v1"
                                        .to_owned(),
                                }
                            },
                            max_attempts: 3,
                        },
                    ],
                },
                timestamp,
            )
            .await
            .expect("post-turn effect should prepare");
        let prepared = outbox::prepare_activation_for_terminal(&db, turn_id).await?;
        let at = unix_to_datetime(timestamp + 1);
        let tx = db.begin().await?;
        pioneer_crud_conversations::turn_writes::update_turn_status(
            &tx, thread_id, turn_id, pioneer_protocol::TurnStatus::Completed, None, at,
        ).await?;
        outbox::activate_prepared_for_terminal(&tx, prepared, at).await?;
        tx.commit().await?;

        pioneer_entity::native_terminal_effect_outbox::Entity::update_many()
            .col_expr(
                pioneer_entity::native_terminal_effect_outbox::Column::HandlerCheckpointJson,
                Expr::value(Some(r#"{"schema_version":1}"#.to_owned())),
            )
            .col_expr(
                pioneer_entity::native_terminal_effect_outbox::Column::HandlerCheckpointSha256,
                Expr::value(Some(if cleanup {
                    outbox::payload_sha256_hex(r#"{"schema_version":1}"#)
                } else {
                    "0".repeat(64)
                })),
            )
            .filter(
                pioneer_entity::native_terminal_effect_outbox::Column::EffectId
                    .eq(effect_id.clone()),
            )
            .exec(&db)
            .await
            .expect("fault injection should corrupt the durable checkpoint hash");

        let healthy = outbox::claim_records(&db, timestamp + 1, 10, 10, &|| timestamp + 1)
            .await
            .expect("corrupt checkpoint should be quarantined without poisoning its batch")
            .records;
        assert_eq!(
            healthy.len(),
            1,
            "only the healthy obligation may reach a handler"
        );
        assert_eq!(
            healthy[0].effect_id,
            format!("{turn_id}:terminal-effect:healthy")
        );
        assert!(
            store
                .complete_native_terminal_effect(
                    &healthy[0].effect_id,
                    &healthy[0].claim_token,
                    timestamp + 2
                )
                .await
                .unwrap()
        );
        let quarantined = outbox::load_status(&db, &effect_id)
            .await
            .expect("quarantined effect status should load")
            .expect("quarantined effect should remain inspectable");
        assert_eq!(quarantined.status, "unresolved");
        assert_eq!(
            quarantined.last_error_code.as_deref(),
            Some("invalid_persisted_effect")
        );
        let persisted =
            pioneer_entity::native_terminal_effect_outbox::Entity::find_by_id(effect_id)
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        assert!(persisted.next_run_at.is_none());
        assert_eq!(
            persisted.handler_checkpoint_json.as_deref(),
            Some(r#"{"schema_version":1}"#)
        );
        assert_eq!(
            store
                .requeue_retryable_unresolved_native_terminal_effects(timestamp + 7200, 10)
                .await
                .unwrap(),
            0
        );
        assert!(
            outbox::claim_records(&db, timestamp + 7200, 10, 10, &|| timestamp + 7200)
                .await
                .unwrap()
                .records
                .is_empty()
        );
        Ok(())
    })
    .await
    .unwrap();
}

fn health_cleanup_effect_preparation(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    batch_suffix: &str,
) -> pioneer_protocol::NativeTerminalEffectPreparation {
    pioneer_protocol::NativeTerminalEffectPreparation {
        batch_id: format!("{turn_id}:batch:{batch_suffix}"),
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        runtime_generation: 1,
        effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
            effect_id: format!("{turn_id}:terminal-effect:attached-task-cleanup"),
            effect_kind: pioneer_protocol::NativeTerminalEffectKind::AttachedTaskCleanup,
            gate: pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
            payload: pioneer_protocol::NativeTerminalEffectPayload::AttachedTaskCleanup {
                reason: "parent turn failed".to_owned(),
                runtime_contract: "pioneer.test.attached-task-cleanup.v1".to_owned(),
            },
            max_attempts: 3,
        }],
    }
}

#[tokio::test]
async fn native_lifecycle_health_probe_commits_and_reports_stale_turn_and_terminal_backlog() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_native_health";
        let thread_id = "thr_native_health";
        let turn_id = "turn_native_health";
        let db = SqliteDatabase::new(fixture.connect_read_only().await?, fixture.connection().clone());
        db.execute_unprepared(
            "INSERT INTO workspace (id,name) VALUES ('ws_native_health','Native health');
             INSERT INTO thread (id,workspace_id,preview,mode,model,model_provider,status)
                 VALUES ('thr_native_health','ws_native_health','','agent','gpt-5.4','openai','active')",
        ).await?;
        let created_at = unix_to_datetime(1_700_000_000);
        db.execute_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DbBackend::Postgres,
            "INSERT INTO turn (id,thread_id,status,created_at,updated_at) VALUES
                 ($1,$2,'in_progress',$3,$3),
                 ('turn_cli_health_control',$2,'in_progress',$3,$3)",
            [turn_id.into(), thread_id.into(), created_at.into()],
        ))
        .await?;
        let store = TerminalEffectStore::new(&db);
        crate::turn_execution::insert_immutable(
            &db,
            crate::turn_execution::NewTurnExecution {
                turn_id: turn_id.to_owned(),
                thread_id: thread_id.to_owned(),
                workspace_id: workspace_id.to_owned(),
                executor_kind: crate::turn_execution::TurnExecutorKind::NativeAgent,
                executor_key: Some("openai".to_owned()),
                status: crate::turn_execution::TurnExecutionStatus::Running,
                owner_id: "native-health-owner".to_owned(),
                lease_until: unix_to_datetime(1_700_000_100),
                created_at,
            },
        )
        .await
        .expect("native execution identity should persist");

        crate::turn_execution::insert_immutable(
            &db,
            crate::turn_execution::NewTurnExecution {
                turn_id: "turn_cli_health_control".into(),
                thread_id: thread_id.to_owned(),
                workspace_id: workspace_id.to_owned(),
                executor_kind: crate::turn_execution::TurnExecutorKind::CliRuntime,
                executor_key: Some("codex".to_owned()),
                status: crate::turn_execution::TurnExecutionStatus::Running,
                owner_id: "cli-health-owner".to_owned(),
                lease_until: unix_to_datetime(1_700_000_100),
                created_at,
            },
        )
        .await
        .expect("CLI execution identity should persist");
        store
            .prepare_native_terminal_effects(
                health_cleanup_effect_preparation(workspace_id, thread_id, turn_id, "health"),
                1_700_000_100,
            )
            .await
            .expect("terminal obligation should be visible to readiness");

        store
            .native_lifecycle_read_write_probe()
            .await
            .expect("read/write canary transaction should commit without domain mutation");
        let recovery_work = Arc::new(tokio::sync::Notify::new());
        let snapshot = crate::recovery_store::RecoveryStore::new(&db, &recovery_work)
            .native_lifecycle_durable_health_snapshot(1_700_000_601, 300)
            .await
            .expect("bounded lifecycle snapshot should load");

        assert_eq!(snapshot.active_turns, 1);
        assert_eq!(snapshot.stale_running_turns, 1);
        assert_eq!(snapshot.active_recovery_jobs, 0);
        assert_eq!(snapshot.terminal_effects.prepared, 1);
        assert_eq!(snapshot.oldest_terminal_effect_age_secs, Some(501));
        Ok(())
    })
    .await
    .unwrap();
}

fn candidate_status(status: TaskResultCandidateStatus) -> &'static str {
    match status {
        TaskResultCandidateStatus::Accepted => "accepted",
        TaskResultCandidateStatus::Rejected => "rejected",
        TaskResultCandidateStatus::Superseded => "superseded",
        TaskResultCandidateStatus::Cancelled => "cancelled",
        _ => panic!("the gate only inspects terminal candidates"),
    }
}

// Restore the persisted candidate directly. The gate only reads the exact
// turn and candidate metadata. Cross-domain Task publication has its own tests.
async fn insert_restored_candidate(
    db: &SqliteDatabase,
    thread_id: &str,
    turn_id: &str,
    suffix: &str,
    status: TaskResultCandidateStatus,
    at: i64,
    summary: Option<String>,
) -> String {
    use pioneer_entity::task_result_candidate as candidate;
    let id = format!("candidate_{suffix}");
    let timestamp = unix_to_datetime(at);
    candidate::Entity::insert(candidate::ActiveModel {
        id: Set(id.clone()),
        task_id: Set(format!("task_{suffix}")),
        run_id: Set(format!("run_{suffix}")),
        task_run_turn_id: Set(format!("trt_{suffix}")),
        thread_id: Set(thread_id.to_owned()),
        turn_id: Set(turn_id.to_owned()),
        round: Set(0),
        status: Set(candidate_status(status).to_owned()),
        result_json: Set(Some(
            serde_json::json!({"summary": "result", "artifacts": []}).to_string(),
        )),
        summary: Set(summary),
        diagnostics_json: Set(Some("[]".to_owned())),
        final_review_event_id: Set(Some(format!("review_{suffix}"))),
        created_at: Set(timestamp),
        updated_at: Set(timestamp),
        resolved_at: Set(Some(timestamp)),
        ..Default::default()
    })
    .exec_without_returning(db)
    .await
    .unwrap();
    id
}

#[tokio::test]
async fn latest_terminal_candidate_all_statuses_ties_and_large_payloads() {
    for status in [
        TaskResultCandidateStatus::Rejected,
        TaskResultCandidateStatus::Superseded,
        TaskResultCandidateStatus::Cancelled,
    ] {
        with_database(async |database| {
            let f = Fixture::open(database).await;
            let id = f
                .effect(
                    "latest",
                    pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                    true,
                )
                .await;
            insert_restored_candidate(
                &f.db,
                "thr_latest",
                "turn_latest",
                "a",
                TaskResultCandidateStatus::Accepted,
                NOW - 20,
                Some("x".repeat(256 * 1024)),
            )
            .await;
            insert_restored_candidate(
                &f.db,
                "thr_latest",
                "turn_latest",
                "z",
                status,
                NOW - 10,
                None,
            )
            .await;
            assert!(f.claim(NOW).await.is_empty());
            assert_eq!(f.row(&id).await.status, "discarded");
            assert_eq!(f.row(&id).await.accepted_candidate_id, None);
            Ok(())
        })
        .await
        .unwrap();
    }
    // Equal timestamps choose id DESC across statuses, then across accepted IDs.
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let id = f
            .effect(
                "tie",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        insert_restored_candidate(
            &f.db,
            "thr_tie",
            "turn_tie",
            "a",
            TaskResultCandidateStatus::Rejected,
            NOW,
            None,
        )
        .await;
        let accepted = insert_restored_candidate(
            &f.db,
            "thr_tie",
            "turn_tie",
            "z",
            TaskResultCandidateStatus::Accepted,
            NOW,
            None,
        )
        .await;
        assert_eq!(f.claim(NOW).await.len(), 1);
        assert_eq!(
            f.row(&id).await.accepted_candidate_id.as_deref(),
            Some(accepted.as_str())
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn terminal_metadata_ties_choose_greatest_id_within_each_status() {
    for status in [
        TaskResultCandidateStatus::Accepted,
        TaskResultCandidateStatus::Rejected,
        TaskResultCandidateStatus::Superseded,
        TaskResultCandidateStatus::Cancelled,
    ] {
        with_database(async |database| {
            let f = Fixture::open(database).await;
            let id = f
                .effect(
                    "same_status_tie",
                    pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                    true,
                )
                .await;
            for suffix in ["z", "a"] {
                insert_restored_candidate(
                    &f.db,
                    "thr_same_status_tie",
                    "turn_same_status_tie",
                    suffix,
                    status,
                    NOW,
                    None,
                )
                .await;
            }
            let metadata = outbox::candidate_metadata_seek(
                "thr_same_status_tie",
                "turn_same_status_tie",
                candidate_status(status),
            )
            .into_model::<outbox::CandidateGateMetadata>()
            .one(&f.db)
            .await
            .unwrap()
            .unwrap();
            assert_eq!(metadata.id, "candidate_z");
            let records = f.claim(NOW).await;
            if status == TaskResultCandidateStatus::Accepted {
                assert_eq!(records.len(), 1);
                assert_eq!(
                    f.row(&id).await.accepted_candidate_id.as_deref(),
                    Some("candidate_z")
                );
            } else {
                assert!(records.is_empty());
                assert_eq!(f.row(&id).await.status, "discarded");
            }
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn latest_gate_revalidation_rejects_newer_rejection_after_preparation() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let id = f
            .effect(
                "stale",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                false,
            )
            .await;
        insert_restored_candidate(
            &f.db,
            "thr_stale",
            "turn_stale",
            "a",
            TaskResultCandidateStatus::Accepted,
            NOW,
            None,
        )
        .await;
        let prepared = outbox::prepare_activation_for_terminal(&f.db, "turn_stale")
            .await
            .unwrap();
        insert_restored_candidate(
            &f.db,
            "thr_stale",
            "turn_stale",
            "z",
            TaskResultCandidateStatus::Rejected,
            NOW,
            None,
        )
        .await;
        let tx = f.db.begin().await.unwrap();
        assert!(
            outbox::activate_prepared_for_terminal(&tx, prepared, unix_to_datetime(NOW))
                .await
                .is_err()
        );
        tx.rollback().await.unwrap();
        assert_eq!(f.row(&id).await.status, "prepared");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn committed_probe_failure_deferral_does_not_increment_again() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let id = f
            .effect(
                "committed_failure",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        insert_restored_candidate(&f.db,
            "thr_committed_failure",
            "turn_committed_failure",
            "committed_failure",
            TaskResultCandidateStatus::Accepted,
            NOW, None).await;
        // Reservation commits successfully; only the subsequent gate resolution
        // fails. Conditional deferral must keep its already incremented counter.
        f.db.execute_unprepared("CREATE FUNCTION fail_probe_resolution() RETURNS trigger LANGUAGE plpgsql AS $fault$
               BEGIN RAISE EXCEPTION 'probe resolution failure'; RETURN NEW; END $fault$;
               CREATE TRIGGER fail_probe_resolution BEFORE UPDATE OF status ON native_terminal_effect_outbox
               FOR EACH ROW WHEN (OLD.status='waiting_acceptance' AND NEW.status='ready') EXECUTE FUNCTION fail_probe_resolution()").await.unwrap();
        let outcome = f.claim_outcome(NOW, 10, 8, &|| NOW)
            .await
            .unwrap();
        assert!(outcome.storage_failed);
        assert_eq!(f.row(&id).await.gate_probe_attempts, 1);
        assert_eq!(f.row(&id).await.gate_probe_at, NOW + 5);
        let outcome = f.claim_outcome(NOW + 5, 10, 8, &|| NOW + 5)
            .await
            .unwrap();
        assert!(outcome.storage_failed);
        assert_eq!(f.row(&id).await.gate_probe_attempts, 2);
        assert_eq!(f.row(&id).await.gate_probe_at, NOW + 15);
        assert_eq!(f.row(&id).await.attempt_count, 0);
        Ok(())
    }).await.unwrap();
}
