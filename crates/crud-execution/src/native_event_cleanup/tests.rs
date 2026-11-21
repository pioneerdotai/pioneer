use super::*;
use crate::native_events::{NewCliRuntimeNativeEvent, append_native_event};
use sea_orm::sea_query::PostgresQueryBuilder;
use sea_orm::{DatabaseBackend, Statement};
use std::sync::Arc;

const NOW: i64 = 1_900_000_000;
struct Fixture {
    db: SqliteDatabase,
    fixture: Arc<pioneer_postgres_test::TestDatabase>,
}

impl Fixture {
    async fn open() -> Result<Self> {
        let fixture = Self::connect(pioneer_postgres_test::scoped_database().await?).await?;
        fixture.sql("INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws-cleanup','Cleanup',TRUE,FALSE);
        INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,created_at,updated_at)
        VALUES('thread-cleanup','ws-cleanup','','agent','m','p','active','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await?;
        Ok(fixture)
    }

    async fn connect(fixture: Arc<pioneer_postgres_test::TestDatabase>) -> Result<Self> {
        let writer = fixture.connect_with_limit(1).await?;
        let reader = fixture.connect_read_only().await?;
        let db = SqliteDatabase::new(reader, writer).maintenance();
        db.validate_reader().await?;
        Ok(Self { db, fixture })
    }
    async fn restart(self) -> Result<Self> {
        let fixture = self.fixture.clone();
        self.db.clone().close().await?;
        Self::connect(fixture).await
    }
    async fn sql(&self, sql: &str) -> Result<()> {
        self.db.execute_unprepared(sql).await?;
        Ok(())
    }
    async fn scalar(&self, sql: &str) -> Result<i64> {
        Ok(self
            .db
            .query_one_raw(Statement::from_string(DatabaseBackend::Postgres, sql))
            .await?
            .context("missing scalar")?
            .try_get_by_index(0)?)
    }
    async fn turn(&self, id: &str, completed: bool) -> Result<()> {
        let status = if completed {
            "completed"
        } else {
            "in_progress"
        };
        self.db.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at)
            VALUES($1,'thread-cleanup',$2,'conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            [id.into(), status.into()],
        )).await?;
        if completed {
            crate::test_fixture::seed_healthy_projection(&self.db, "thread-cleanup", id).await?;
        }
        self.bind(id, "codex", if completed { "completed" } else { "running" })
            .await
    }

    async fn bind(&self, id: &str, runtime: &str, status: &str) -> Result<()> {
        let mapping = pioneer_crud_payloads::Frame::encode("{}")?;
        self.db.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "INSERT INTO turn_cli_runtime_binding(turn_id,thread_id,continuation_thread_id,workspace_id,runtime_id,runtime_kind,
             native_thread_id,native_turn_id,status,input_mapping_json,input_mapping_codec,input_mapping_bytes,input_mapping_chars,input_mapping_sha256,frozen_manifest_id,created_at,updated_at)
             VALUES($1,'thread-cleanup','thread-cleanup','ws-cleanup',$2,'codex',$3,$4,$5,$6,$7,2,2,$8,NULL,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
             ON CONFLICT(turn_id) DO UPDATE SET runtime_id=EXCLUDED.runtime_id,native_thread_id=EXCLUDED.native_thread_id,status=EXCLUDED.status",
            [id.into(),runtime.into(),format!("native-{runtime}").into(),format!("native-{id}").into(),
            status.into(),mapping.bytes.into(),(mapping.encoding as i16).into(),mapping.sha256.to_vec().into()],
        )).await?;
        Ok(())
    }

    async fn event(
        &self,
        id: &str,
        turn: Option<&str>,
        runtime: &str,
        method: &str,
        bytes: usize,
    ) -> Result<()> {
        append_native_event(
            &self.db,
            NewCliRuntimeNativeEvent {
                id: id.into(),
                runtime_id: runtime.into(),
                runtime_kind: "codex".into(),
                workspace_id: Some("ws-cleanup".into()),
                thread_id: Some("thread-cleanup".into()),
                turn_id: turn.map(Into::into),
                native_thread_id: Some(format!("native-{runtime}")),
                native_turn_id: turn.map(|t| format!("native-{t}")),
                native_method: method.into(),
                payload_redacted_json: "x".repeat(bytes),
                sequence: NOW * 1000,
                created_at: chrono::DateTime::from_timestamp(NOW, 0)
                    .unwrap()
                    .fixed_offset(),
            },
        )
        .await?;
        Ok(())
    }
    async fn prepared(&self, turn: &str) -> Result<Prepared> {
        let rows = candidates(&self.db, turn, None).await?;
        let runtime_id = rows.first().map(|r| r.runtime_id.clone());
        Ok(Prepared {
            job: Job {
                turn_id: turn.into(),
                regular_lane: Some("new".into()),
            },
            runtime_id,
            ids: bounded_ids(rows).0,
        })
    }
}

#[pioneer_postgres_test::postgres_test]
async fn writer_revalidates_all_readiness_conditions() -> Result<()> {
    for change in [
        "UPDATE turn SET status='in_progress'",
        "UPDATE turn SET status='blocked'",
        "UPDATE turn_cli_runtime_binding SET status='running'",
        "DELETE FROM turn_cli_runtime_binding",
        "INSERT INTO turn_cli_runtime_attempt(id,turn_id,attempt_index,runtime_id,runtime_kind,native_thread_id,status,created_at,updated_at) VALUES('attempt','done',1,'codex','codex','native','starting',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO recovery_job(id,turn_id,item_id,item_type,status,trigger,action,policy,provider_attempt_number,policy_snapshot,run_count,max_attempts,scheduled_at,next_run_at,created_at,updated_at,resolution_pending) VALUES('recovery','done','item','command_execution','succeeded','timeout','retry','{}',1,'{}',0,3,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,TRUE)",
        "UPDATE turn_event_projection_stream_state SET status='quarantined'",
        "DELETE FROM turn_event_projection_stream_state",
        "INSERT INTO turn_event_projection_state(event_id,thread_id,turn_id,sequence,status,attempt_count,next_run_at,projection_context_json,created_at,updated_at) VALUES('receipt','thread-cleanup','done',99,'projected',0,CURRENT_TIMESTAMP,'{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        let f = Fixture::open().await?;
        f.turn("done", true).await?;
        f.event("selected", Some("done"), "codex", "item/completed", 8)
            .await?;
        let p = f.prepared("done").await?;
        assert_eq!(p.ids.len(), 1);
        f.sql(change).await?;
        let mut m = NativeEventCleanupMetrics::default();
        apply(&f.db, &p, 1, &mut m).await?;
        assert_eq!(m.events_deleted, 0, "{change}");
        assert_eq!(
            f.scalar("SELECT COUNT(*) FROM cli_runtime_native_event")
                .await?,
            1
        );
    }
    Ok(())
}
#[pioneer_postgres_test::postgres_test]
async fn byte_budget_runtime_ownership_and_empty_preparation_races() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("done", true).await?;
    f.turn("other", true).await?;
    f.event("a", Some("done"), "codex", "item/completed", 100_000)
        .await?;
    f.event("b", Some("done"), "codex", "item/completed", 100_000)
        .await?;
    let p = f.prepared("done").await?;
    f.sql("UPDATE cli_runtime_native_event SET payload_redacted_json=repeat('x',204800)")
        .await?;
    let mut m = NativeEventCleanupMetrics::default();
    apply(&f.db, &p, 1, &mut m).await?;
    assert_eq!(m.events_deleted, 1);
    assert_eq!(m.deleted_bytes, 204800);
    let p = f.prepared("done").await?;
    f.sql("UPDATE cli_runtime_native_event SET turn_id='other'")
        .await?;
    let mut m = NativeEventCleanupMetrics::default();
    apply(&f.db, &p, 2, &mut m).await?;
    assert_eq!(m.events_deleted, 0);
    let p = f.prepared("other").await?;
    f.bind("other", "changed", "completed").await?;
    let mut m = NativeEventCleanupMetrics::default();
    apply(&f.db, &p, 3, &mut m).await?;
    assert_eq!(m.events_deleted, 0);
    f.event("late-seed", Some("done"), "codex", "item/completed", 8)
        .await?;
    f.sql(
        "UPDATE cli_runtime_native_event SET native_method='future/retained' WHERE id='late-seed'",
    )
    .await?;
    let p = f.prepared("done").await?;
    assert!(p.ids.is_empty());
    f.event("late", Some("done"), "codex", "item/completed", 8)
        .await?;
    let mut m = NativeEventCleanupMetrics::default();
    apply(&f.db, &p, 4, &mut m).await?;
    assert_eq!(m.events_deleted, 0);
    assert_eq!(run(&f.db, 5).await?.events_deleted, 1);
    Ok(())
}
#[pioneer_postgres_test::postgres_test]
async fn live_registration_survives_reopen_and_resumes_bounded_cleanup() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("retained", true).await?;
    for i in 0..130 {
        f.event(
            &format!("m-{i:03}"),
            Some("retained"),
            "codex",
            "item/completed",
            1,
        )
        .await?;
    }
    assert_eq!(f.prepared("retained").await?.ids.len(), PAGE_ROWS);
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM native_event_cleanup_job WHERE turn_id='retained'")
            .await?,
        1
    );
    let f = f.restart().await?;
    f.turn("live", true).await?;
    f.event("a-live", Some("live"), "codex", "item/completed", 1)
        .await?;
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM native_event_cleanup_job WHERE turn_id='live'")
            .await?,
        1
    );
    let mut total = 0;
    for now in 1..6 {
        total += run(&f.db, now).await?.events_deleted;
    }
    assert_eq!(total, 131);
    assert_eq!(run(&f.db, 10).await?.events_deleted, 0);
    Ok(())
}
#[pioneer_postgres_test::postgres_test]
async fn rollback_unblock_and_late_registration() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("done", true).await?;
    f.event("event", Some("done"), "codex", "item/completed", 8)
        .await?;
    f.sql("UPDATE turn_cli_runtime_binding SET status='running'")
        .await?;
    assert_eq!(run(&f.db, 1).await?.events_deleted, 0);
    let tx = f.db.begin().await?;
    tx.execute_unprepared("UPDATE turn_cli_runtime_binding SET status='completed'")
        .await?;
    tx.rollback().await?;
    assert_eq!(run(&f.db, 2).await?.jobs_examined, 0);
    f.sql("UPDATE turn_cli_runtime_binding SET status='completed'")
        .await?;
    assert_eq!(run(&f.db, 3).await?.events_deleted, 1);
    f.event("late", Some("done"), "codex", "item/completed", 8)
        .await?;
    assert_eq!(run(&f.db, 4).await?.events_deleted, 1);
    Ok(())
}
#[pioneer_postgres_test::postgres_test]
async fn failed_delete_rolls_back_defers_only_poison_and_survives_restart() -> Result<()> {
    let f = Fixture::open().await?;
    for t in ["a-poison", "b-ready"] {
        f.turn(t, true).await?;
        f.event(t, Some(t), "codex", "item/completed", 8).await?;
    }
    f.sql("CREATE FUNCTION fail_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$; CREATE TRIGGER fail_cleanup BEFORE DELETE ON cli_runtime_native_event FOR EACH ROW WHEN (OLD.id='a-poison') EXECUTE FUNCTION fail_cleanup()").await?;
    let m = run(&f.db, 100).await?;
    assert_eq!(m.errors_deferred, 1);
    assert_eq!(m.events_deleted, 0);
    assert_eq!(run(&f.db, 101).await?.events_deleted, 1);
    let f = f.restart().await?;
    assert_eq!(
        f.scalar("SELECT available_at FROM native_event_cleanup_job WHERE turn_id='a-poison'")
            .await?,
        100 + RETRY_DELAY_MICROS
    );
    assert_eq!(run(&f.db, 102).await?.jobs_examined, 0);
    f.sql("DROP TRIGGER fail_cleanup ON cli_runtime_native_event; DROP FUNCTION fail_cleanup()")
        .await?;
    assert_eq!(
        run(&f.db, 100 + RETRY_DELAY_MICROS).await?.events_deleted,
        1
    );
    Ok(())
}
#[pioneer_postgres_test::postgres_test]
async fn sustained_new_jobs_do_not_starve_served_turn_and_scheduler_noops() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("large", true).await?;
    for i in 0..512 {
        f.event(
            &format!("large-{i:03}"),
            Some("large"),
            "codex",
            "item/completed",
            1,
        )
        .await?;
    }
    assert_eq!(run(&f.db, 100).await?.events_deleted, 128);
    for i in 0..10 {
        let t = format!("new-{i:02}");
        f.turn(&t, true).await?;
        f.event(&t, Some(&t), "codex", "item/completed", 1).await?;
        run(&f.db, 101 + i).await?;
    }
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM cli_runtime_native_event WHERE turn_id='large'")
            .await?,
        128
    );
    for now in 200..205 {
        run(&f.db, now).await?;
    }
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM cli_runtime_native_event")
            .await?,
        0
    );
    f.turn("served", true).await?;
    f.event("served", Some("served"), "codex", "item/completed", 1)
        .await?;
    f.sql("UPDATE native_event_cleanup_job SET last_served_at=1; UPDATE native_event_cleanup_scheduler SET new_jobs_since_served=0").await?;
    let m = run(&f.db, 300).await?;
    assert_eq!(m.apply_writes, 3);
    assert_eq!(m.scheduler_rows_changed, 0);
    Ok(())
}
#[test]
fn empty_candidate_binding_shape_does_not_read_events() {
    let (sql, bindings) = queries::candidates("turn", Some(&[])).build(PostgresQueryBuilder);
    let bindings = bindings.0;
    assert_eq!(sql.matches('$').count(), 1);
    assert_eq!(bindings.len(), 1);
    assert!(!sql.contains("cli_runtime_native_event"));
    let (sql, bindings) =
        queries::candidates("turn", Some(&["a".into(), "b".into()])).build(PostgresQueryBuilder);
    assert_eq!(sql.matches('$').count(), 5);
    assert_eq!(
        bindings.0,
        vec![
            "turn".into(),
            "turn".into(),
            "a".into(),
            "b".into(),
            (PAGE_ROWS as u64).into()
        ]
    );
}
#[pioneer_postgres_test::postgres_test]
async fn actual_query_plans_use_addressed_indexes() -> Result<()> {
    let f = Fixture::open().await?;
    for (name, query, required) in [
        (
            "discovery",
            queries::discovery(1),
            vec![
                "native_event_cleanup_due_retry",
                "native_event_cleanup_new",
                "native_event_cleanup_served",
            ],
        ),
        (
            "prepare",
            queries::candidates("turn", None),
            vec!["native_event_cleanup_candidate"],
        ),
        (
            "apply_one",
            queries::candidates("turn", Some(&["a".into()])),
            vec![],
        ),
        (
            "apply_max",
            queries::candidates(
                "turn",
                Some(
                    &(0..PAGE_ROWS)
                        .map(|id| format!("event-{id}"))
                        .collect::<Vec<_>>(),
                ),
            ),
            vec![],
        ),
        (
            "remaining",
            queries::remaining("turn", Some("codex")),
            vec!["native_event_cleanup_candidate"],
        ),
    ] {
        let (sql, values) = query.build(PostgresQueryBuilder);
        let rows =
            f.db.query_all_raw(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                format!("EXPLAIN (COSTS OFF) {sql}"),
                values.0,
            ))
            .await?;
        let details = rows
            .iter()
            .map(|r| r.try_get::<String>("", "QUERY PLAN"))
            .collect::<Result<Vec<_>, _>>()?;
        for index in required {
            assert!(
                details.iter().any(|s| s.contains(index)),
                "{name}: {details:?}"
            );
        }
        if name.starts_with("apply") {
            // Revalidation is restricted to <= PAGE_ROWS prepared primary keys.
            // PostgreSQL may prefer direct PK probes over the candidate index here;
            // both are addressed. Discovery/prepare still require their partial indexes.
            assert!(
                details
                    .iter()
                    .any(|detail| detail.contains("native_event_cleanup_candidate")
                        || detail.contains("cli_runtime_native_event_pkey")),
                "{name}: writer must use candidate-index or prepared-PK probes: {details:?}"
            );
        }
        for detail in &details {
            if let Some(relation) = detail
                .trim_start()
                .trim_start_matches("->")
                .trim_start()
                .strip_prefix("Seq Scan on ")
                .and_then(|s| s.split_ascii_whitespace().next())
            {
                assert!(
                    ![
                        "event",
                        "a",
                        "s",
                        "r",
                        "p",
                        "receipt",
                        "cli_runtime_native_event"
                    ]
                    .contains(&relation),
                    "{name}: {details:?}"
                );
            }
        }
        if name == "prepare" || name.starts_with("apply") {
            assert!(
                sql.split_ascii_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .contains("AS MATERIALIZED"),
                "{sql}"
            );
            let boundary = details
                .iter()
                .position(|line| line.trim() == "CTE candidates")
                .expect("bounded candidates CTE");
            let candidate_plan = &details[boundary + 1..];
            let first = candidate_plan.first().expect("candidate plan");
            assert!(
                first
                    .trim()
                    .trim_start_matches("->")
                    .trim_start()
                    .starts_with("Limit"),
                "{name}: candidate selection must be limited before the outer scan: {details:?}"
            );
            assert!(
                !candidate_plan
                    .iter()
                    .take_while(|line| { line.starts_with("    ") })
                    .any(|line| line.contains("Sort")),
                "{name}: an addressed source must not sort before LIMIT: {details:?}"
            );
        } else {
            assert!(
                !details.iter().any(|line| line.contains("Sort")),
                "{name}: {details:?}"
            );
        }
    }
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn dependency_signals_wake_only_waiters_and_receipts_wait_for_last_blocker() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("sentinel", false).await?;
    f.event("sentinel", Some("sentinel"), "codex", "item/completed", 1)
        .await?;
    run(&f.db, 1).await?;
    let sentinel = f
        .scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='sentinel'")
        .await?;
    f.turn("done", true).await?;
    for (block, unblock) in [
        (
            "UPDATE turn SET status='in_progress' WHERE id='done'",
            "UPDATE turn SET status='completed' WHERE id='done'",
        ),
        (
            "UPDATE turn_cli_runtime_binding SET status='running' WHERE turn_id='done'",
            "UPDATE turn_cli_runtime_binding SET status='completed' WHERE turn_id='done'",
        ),
        (
            "UPDATE turn_event_projection_stream_state SET status='quarantined' WHERE turn_id='done'",
            "UPDATE turn_event_projection_stream_state SET status='healthy' WHERE turn_id='done'",
        ),
        (
            "UPDATE turn_event_projection_stream_state SET projected_through_sequence=0 WHERE turn_id='done'",
            "UPDATE turn_event_projection_stream_state SET projected_through_sequence=2 WHERE turn_id='done'",
        ),
        (
            "INSERT INTO turn_cli_runtime_attempt(id,turn_id,attempt_index,runtime_id,runtime_kind,native_thread_id,status,created_at,updated_at) VALUES('attempt','done',1,'codex','codex','native','running',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "UPDATE turn_cli_runtime_attempt SET status='completed' WHERE id='attempt'",
        ),
        (
            "INSERT INTO turn_cli_runtime_execution_segment(id,attempt_id,turn_id,segment_index,runtime_id,native_thread_id,native_turn_id,status,started_at,created_at,updated_at) VALUES('segment','attempt','done',1,'codex','native','native-segment','running',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "UPDATE turn_cli_runtime_execution_segment SET status='completed' WHERE id='segment'",
        ),
        (
            "INSERT INTO recovery_job(id,turn_id,item_id,item_type,status,trigger,action,policy,provider_attempt_number,policy_snapshot,run_count,max_attempts,scheduled_at,next_run_at,created_at,updated_at,resolution_pending) VALUES('recovery','done','item','command_execution','pending','timeout','retry','{}',1,'{}',0,3,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,TRUE)",
            "UPDATE recovery_job SET status='succeeded',resolution_pending=FALSE WHERE id='recovery'",
        ),
    ] {
        f.sql(block).await?;
        f.event("work", Some("done"), "codex", "item/completed", 1)
            .await?;
        assert_eq!(run(&f.db, 2).await?.events_deleted, 0, "{block}");
        assert_eq!(f.scalar("SELECT COUNT(*) FROM native_event_cleanup_job WHERE turn_id='done' AND state='waiting'").await?,1);
        f.sql(unblock).await?;
        assert_eq!(run(&f.db, 3).await?.events_deleted, 1, "{unblock}");
        assert_eq!(
            f.scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='sentinel'")
                .await?,
            sentinel
        );
    }
    f.event("work", Some("done"), "codex", "item/completed", 1)
        .await?;
    f.sql("INSERT INTO turn_event_projection_state(event_id,thread_id,turn_id,sequence,status,attempt_count,next_run_at,projection_context_json,created_at,updated_at) VALUES
        ('low','thread-cleanup','done',1,'projected',0,CURRENT_TIMESTAMP,'{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
        ('high1','thread-cleanup','done',100,'projected',0,CURRENT_TIMESTAMP,'{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
        ('high2','thread-cleanup','done',101,'projected',0,CURRENT_TIMESTAMP,'{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await?;
    assert_eq!(run(&f.db, 4).await?.events_deleted, 0);
    let rev = f
        .scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='done'")
        .await?;
    f.sql("DELETE FROM turn_event_projection_state WHERE event_id='low'; DELETE FROM turn_event_projection_state WHERE event_id='high1'; UPDATE turn_event_projection_state SET sequence=sequence WHERE event_id='high2'").await?;
    assert_eq!(
        f.scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='done'")
            .await?,
        rev
    );
    f.sql("UPDATE turn_event_projection_state SET sequence=2 WHERE event_id='high2'")
        .await?;
    assert_eq!(run(&f.db, 5).await?.events_deleted, 1);
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn moved_event_wakes_destination_and_queued_deletes_do_not_update_job() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("source", true).await?;
    f.turn("target", true).await?;
    f.event(
        "wrong-runtime",
        Some("target"),
        "wrong",
        "item/completed",
        8,
    )
    .await?;
    run(&f.db, 1).await?;
    f.event("moving", Some("source"), "codex", "item/completed", 8)
        .await?;
    f.sql("UPDATE cli_runtime_native_event SET turn_id='target' WHERE id='moving'")
        .await?;
    assert_eq!(f.scalar("SELECT COUNT(*) FROM native_event_cleanup_job WHERE turn_id='target' AND state='queued'").await?,1);
    let rev = f
        .scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='target'")
        .await?;
    f.sql("DELETE FROM cli_runtime_native_event WHERE id='moving'")
        .await?;
    assert_eq!(
        f.scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='target'")
            .await?,
        rev
    );
    run(&f.db, 2).await?;
    run(&f.db, 3).await?;
    let rev = f
        .scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='target'")
        .await?;
    f.event("retained", Some("target"), "codex", "future/event", 8)
        .await?;
    f.sql("DELETE FROM cli_runtime_native_event WHERE id='retained'")
        .await?;
    assert_eq!(
        f.scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='target'")
            .await?,
        rev
    );
    f.sql("DELETE FROM cli_runtime_native_event WHERE id='wrong-runtime'")
        .await?;
    assert_eq!(
        f.scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='target'")
            .await?,
        rev + 1
    );
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn failure_after_delete_rolls_back_events_and_scheduler_before_deferral() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("done", true).await?;
    f.event("event", Some("done"), "codex", "item/completed", 8)
        .await?;
    f.sql("CREATE FUNCTION fail_scheduler() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected post-delete failure'; END $$; CREATE TRIGGER fail_scheduler BEFORE UPDATE ON native_event_cleanup_scheduler FOR EACH ROW EXECUTE FUNCTION fail_scheduler()").await?;
    let m = run(&f.db, 100).await?;
    assert_eq!(m.errors_deferred, 1);
    assert_eq!(m.events_deleted, 0);
    assert_eq!(m.deleted_bytes, 0);
    assert_eq!(m.scheduler_rows_changed, 0);
    assert_eq!(
        m.queue_rows_changed, 1,
        "only the durable deferral changes the job"
    );
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM cli_runtime_native_event WHERE id='event'")
            .await?,
        1
    );
    assert_eq!(
        f.scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='done'")
            .await?,
        2
    );
    assert_eq!(
        f.scalar("SELECT new_jobs_since_served FROM native_event_cleanup_scheduler")
            .await?,
        0
    );
    f.sql("DROP TRIGGER fail_scheduler ON native_event_cleanup_scheduler; DROP FUNCTION fail_scheduler()").await?;
    assert_eq!(
        run(&f.db, 100 + RETRY_DELAY_MICROS).await?.events_deleted,
        1
    );
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn retry_deadline_ignores_dependency_waiters_and_preserves_subsecond_due_time() {
    let f = Fixture::open().await.unwrap();
    assert_eq!(deadline(&f.db).await.unwrap(), None);
    f.sql("INSERT INTO native_event_cleanup_job(turn_id,state,available_at,last_served_at,revision) VALUES('blocked','waiting',1,0,1),('retry','queued',1900000000000001,0,1)")
        .await.unwrap();
    assert_eq!(
        deadline(&f.db).await.unwrap(),
        Some(1_900_000_001),
        "round up a real retry; never repeatedly wake before microsecond eligibility"
    );
    f.sql("UPDATE native_event_cleanup_job SET state='waiting' WHERE turn_id='retry'")
        .await
        .unwrap();
    assert_eq!(deadline(&f.db).await.unwrap(), None);
}

#[pioneer_postgres_test::postgres_test]
async fn cleanup_rechecks_rows_updated_by_an_independent_writer() -> Result<()> {
    for commit_move in [false, true] {
        let f = Fixture::open().await?;
        f.turn("original", true).await?;
        f.turn("destination", true).await?;
        f.event("selected", Some("original"), "codex", "item/completed", 8)
            .await?;
        let prepared = f.prepared("original").await?;
        assert_eq!(prepared.ids, ["selected"]);

        let other_writer = f.fixture.connect_with_limit(1).await?;
        let move_tx = other_writer.begin().await?;
        let backend = move_tx
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT pg_backend_pid()",
            ))
            .await?
            .expect("independent writer backend")
            .try_get_by_index::<i32>(0)?;
        move_tx
            .execute_unprepared(
                "UPDATE cli_runtime_native_event SET turn_id='destination' WHERE id='selected'",
            )
            .await?;

        let worker = f.db.clone();
        let cleanup = tokio::spawn(async move {
            let mut metrics = NativeEventCleanupMetrics::default();
            apply(&worker, &prepared, 1, &mut metrics).await?;
            Ok::<_, anyhow::Error>(metrics)
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let waiting = f.db.query_one_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)))",
                    [backend.into()],
                )).await?.expect("lock observation should return one row").try_get_by_index::<bool>(0)?;
                if waiting {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }).await??;
        if commit_move {
            move_tx.commit().await?;
        } else {
            move_tx.rollback().await?;
        }
        let metrics = cleanup.await??;
        assert_eq!(metrics.events_deleted, u64::from(!commit_move));
        assert_eq!(metrics.deleted_bytes, if commit_move { 0 } else { 8 });
        assert_eq!(
            f.scalar("SELECT count(*) FROM cli_runtime_native_event WHERE id='selected' AND turn_id='destination'").await?,
            i64::from(commit_move)
        );
        other_writer.close().await?;
    }
    Ok(())
}
