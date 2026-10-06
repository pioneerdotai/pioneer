use super::*;
use migration::{Migrator, MigratorTrait};
use sea_orm::{Database, DatabaseBackend, DbErr};

async fn database() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    // These fixtures exercise outbox state only, as the existing ledger tests do.
    db.execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    db
}

async fn seed<C: ConnectionTrait>(
    db: &C,
    id: &str,
    status: &str,
    attempts: i64,
    created: DateTimeWithTimeZone,
    due: Option<DateTimeWithTimeZone>,
) {
    agent_action_outbox::ActiveModel {
        id: Set(id.to_owned()),
        action_id: Set(format!("action-{id}")),
        owner_execution_id: Set("fixture-owner".to_owned()),
        payload_json: Set(format!(r#"{{"kind":"send_message","original":"{id}"}}"#)),
        status: Set(status.to_owned()),
        attempts: Set(attempts),
        next_attempt_at: Set(due),
        delivered_at: Set(None),
        last_error: Set(None),
        created_at: Set(created),
    }
    .insert(db)
    .await
    .unwrap();
}

async fn load<C: ConnectionTrait>(db: &C, id: &str) -> agent_action_outbox::Model {
    agent_action_outbox::Entity::find_by_id(id)
        .one(db)
        .await
        .unwrap()
        .unwrap()
}

async fn ids<C: ConnectionTrait>(db: &C, now: DateTimeWithTimeZone) -> Vec<String> {
    discover_agent_action_outbox(db, now, 64)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect()
}

#[tokio::test]
async fn action_outbox_ranges_bound_mixed_work_and_share_final_cleanup_budget() {
    let db = database().await;
    let now = utc_now();
    for index in (0..40).rev() {
        seed(
            &db,
            &format!("immediate-{index:02}"),
            if index % 2 == 0 { "pending" } else { "failed" },
            0,
            now,
            None,
        )
        .await;
        seed(
            &db,
            &format!("timed-{index:02}"),
            "pending",
            if index < 20 { 8 } else { 1 },
            now - Duration::days(index + 1),
            Some(now - Duration::seconds(40 - index)),
        )
        .await;
    }
    for index in 0..48 {
        // Much older history must not be read ahead of the current work.
        seed(
            &db,
            &format!("failed-{index:02}"),
            "failed",
            8,
            now - Duration::days(100),
            if index % 2 == 0 { None } else { Some(now) },
        )
        .await;
        seed(
            &db,
            &format!("delivered-{index:02}"),
            "delivered",
            1,
            now - Duration::days(100),
            Some(now),
        )
        .await;
    }
    for limit in [2, 3, 63, 64] {
        let inputs = discover_agent_action_outbox(&db, now, limit).await.unwrap();
        assert!(inputs.len() <= limit as usize);
        assert!(
            inputs
                .iter()
                .filter(|row| row.id.starts_with("immediate-"))
                .count()
                <= 32
        );
        assert!(
            inputs
                .iter()
                .filter(|row| row.id.starts_with("timed-"))
                .count()
                <= 32
        );
    }
    let selected = ids(&db, now).await;
    assert_eq!(selected.len(), 64);
    assert_eq!(
        &selected[..32],
        &(0..32)
            .map(|i| format!("immediate-{i:02}"))
            .collect::<Vec<_>>()
    );
    // Due order wins over deliberately reversed created_at order.
    assert_eq!(
        &selected[32..],
        &(0..32).map(|i| format!("timed-{i:02}")).collect::<Vec<_>>()
    );
    let claimed = claim_agent_action_outbox_with_clock(&db, 64, &|| now)
        .await
        .unwrap()
        .rows;
    assert_eq!(claimed.len(), 44); // 32 immediate + 12 retries; 20 closures share the 64 inputs
    for index in 0..40 {
        let immediate = load(&db, &format!("immediate-{index:02}")).await;
        assert_eq!(immediate.attempts, i64::from(index < 32));
        let timed = load(&db, &format!("timed-{index:02}")).await;
        if index < 20 {
            assert_eq!(timed.status, "failed");
            assert_eq!(timed.next_attempt_at, None);
            assert_eq!(
                timed.last_error.as_deref(),
                Some("outbox delivery lease expired after the retry limit")
            );
        } else {
            assert_eq!(timed.attempts, if index < 32 { 2 } else { 1 });
        }
    }
    assert_eq!(load(&db, "failed-00").await.attempts, 8);
    assert_eq!(load(&db, "delivered-00").await.status, "delivered");
    // No full quantum refill when one range has fewer remaining inputs.
    assert_eq!(ids(&db, now).await.len(), 16);
    assert_eq!(
        claim_agent_action_outbox_with_clock(&db, 64, &|| now)
            .await
            .unwrap()
            .rows
            .len(),
        16
    );
    assert!(ids(&db, now).await.is_empty());
    assert!(claim_agent_action_outbox(&db, 65).await.is_err());
    assert!(
        claim_agent_action_outbox(&db, 0)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
}

#[tokio::test]
async fn action_outbox_plans_use_both_partial_ranges_without_history_sort() {
    let db = database().await;
    let now = utc_now();
    for (sql, values, index) in [
        (
            AGENT_ACTION_OUTBOX_IMMEDIATE_SQL,
            vec![32_u64.into()],
            "idx_agent_action_outbox_immediate",
        ),
        (
            AGENT_ACTION_OUTBOX_TIMED_SQL,
            vec![now.into(), 32_u64.into()],
            "idx_agent_action_outbox_timed",
        ),
    ] {
        // This EXPLAIN is test code only; it must be run after review approval.
        let plan = db
            .query_all_raw(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                format!("EXPLAIN QUERY PLAN {sql}"),
                values,
            ))
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String>("", "detail").unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            plan.len(),
            1,
            "one index range, without a second scan: {plan:?}"
        );
        let plan = &plan[0];
        assert!(plan.contains(index), "{plan}");
        assert!(!plan.contains("USE TEMP B-TREE"), "{plan}");
        assert!(
            plan.contains("USING INDEX") || plan.contains("USING COVERING INDEX"),
            "{plan}"
        );
        if index.ends_with("timed") {
            assert!(
                plan.contains("SEARCH") && plan.contains("next_attempt_at"),
                "{plan}"
            );
        }
    }
}

#[tokio::test]
async fn action_outbox_final_attempt_crash_closes_only_expired_supported_leases() {
    let db = database().await;
    let now = utc_now();
    seed(&db, "final", "pending", 7, now, None).await;
    seed(&db, "unsupported-null", "pending", 8, now, None).await;
    seed(
        &db,
        "future",
        "pending",
        8,
        now,
        Some(now + Duration::hours(1)),
    )
    .await;
    seed(&db, "exhausted", "failed", 8, now, Some(now)).await;
    let final_claim = claim_agent_action_outbox_with_clock(&db, 64, &|| now)
        .await
        .unwrap()
        .rows;
    assert_eq!(final_claim.len(), 1);
    assert_eq!(final_claim[0].attempts, 8);
    assert_eq!(
        final_claim[0].payload_json,
        load(&db, "final").await.payload_json
    );
    assert!(
        claim_agent_action_outbox_with_clock(&db, 64, &|| now + Duration::seconds(29))
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    // Dropping the claim models crash/cancellation before acknowledgement.
    drop(final_claim);
    assert!(
        claim_agent_action_outbox_with_clock(&db, 64, &|| now + Duration::seconds(30))
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    let final_row = load(&db, "final").await;
    assert_eq!(
        (
            final_row.status.as_str(),
            final_row.attempts,
            final_row.next_attempt_at
        ),
        ("failed", 8, None)
    );
    assert!(final_row.last_error.is_some());
    assert!(
        !mark_agent_action_outbox_delivered(&db, "final", 8, now)
            .await
            .unwrap()
    );
    assert!(
        !mark_agent_action_outbox_failed(&db, "final", 8, now)
            .await
            .unwrap()
    );
    assert_eq!(load(&db, "unsupported-null").await.status, "pending");
    assert_eq!(load(&db, "future").await.status, "pending");
    assert_eq!(load(&db, "exhausted").await.next_attempt_at, Some(now));
}

#[tokio::test]
async fn action_outbox_exact_lease_status_attempt_races_and_old_callbacks_are_fenced() {
    let db = database().await;
    let now = utc_now();
    seed(&db, "race", "pending", 8, now, Some(now)).await;
    let mut stale = load(&db, "race").await;
    let mut changed: agent_action_outbox::ActiveModel = stale.clone().into();
    changed.next_attempt_at = Set(Some(now - Duration::seconds(1)));
    changed.update(&db).await.unwrap();
    assert!(
        !transition_agent_action_outbox(&db, &mut stale.clone(), now)
            .await
            .unwrap()
    );
    assert_eq!(
        load(&db, "race").await.status,
        "pending",
        "same attempts but replaced lease must not close"
    );
    let mut changed: agent_action_outbox::ActiveModel = stale.clone().into();
    changed.next_attempt_at = Set(Some(now));
    changed.attempts = Set(1);
    changed.update(&db).await.unwrap();
    assert!(
        !transition_agent_action_outbox(&db, &mut stale, now)
            .await
            .unwrap()
    );
    let mut stale = load(&db, "race").await;
    let mut changed: agent_action_outbox::ActiveModel = stale.clone().into();
    changed.status = Set("failed".to_owned());
    changed.update(&db).await.unwrap();
    assert!(
        !transition_agent_action_outbox(&db, &mut stale, now)
            .await
            .unwrap()
    );
    let current = claim_agent_action_outbox_with_clock(&db, 64, &|| now)
        .await
        .unwrap()
        .rows
        .remove(0);
    assert_eq!(current.attempts, 2);
    assert!(
        !mark_agent_action_outbox_delivered(&db, "race", 1, now)
            .await
            .unwrap()
    );
    assert!(
        !mark_agent_action_outbox_failed(&db, "race", 1, now)
            .await
            .unwrap()
    );
    assert!(
        mark_agent_action_outbox_delivered(&db, "race", 2, now)
            .await
            .unwrap()
    );
    assert!(
        !mark_agent_action_outbox_delivered(&db, "race", 2, now)
            .await
            .unwrap()
    );
    assert!(ids(&db, now + Duration::hours(2)).await.is_empty());
}

#[tokio::test]
async fn action_outbox_deferral_returns_final_attempt_budget_for_both_wait_classes() {
    let db = database().await;
    let now = utc_now();
    for id in ["permit", "runtime"] {
        seed(&db, id, "failed", 7, now, None).await;
    }
    let claimed = claim_agent_action_outbox_with_clock(&db, 64, &|| now)
        .await
        .unwrap()
        .rows;
    assert_eq!(claimed.len(), 2);
    assert!(
        defer_agent_action_outbox_for_permit(&db, "permit", 8, now)
            .await
            .unwrap()
    );
    assert!(
        defer_agent_action_outbox_for_runtime(&db, "runtime", 8, now)
            .await
            .unwrap()
    );
    for id in ["permit", "runtime"] {
        let row = load(&db, id).await;
        assert_eq!((row.status.as_str(), row.attempts), ("failed", 7));
        assert!(
            !mark_agent_action_outbox_failed(&db, id, 8, now)
                .await
                .unwrap()
        );
        assert!(
            !mark_agent_action_outbox_delivered(&db, id, 8, now)
                .await
                .unwrap()
        );
    }
    assert!(ids(&db, now).await.is_empty());
    let again = claim_agent_action_outbox_with_clock(&db, 64, &|| now + Duration::seconds(30))
        .await
        .unwrap()
        .rows;
    assert_eq!(again.len(), 2);
    assert!(again.iter().all(|row| row.attempts == 8));
}

#[tokio::test]
async fn action_outbox_rollback_preserves_source_and_partial_index_membership() {
    let db = database().await;
    let now = utc_now();
    seed(&db, "immediate", "pending", 0, now, None).await;
    seed(&db, "final", "pending", 8, now, Some(now)).await;
    let original_ids = ids(&db, now).await;
    let transaction = db.begin().await.unwrap();
    let mut row = load(&transaction, "immediate").await;
    assert!(
        transition_agent_action_outbox(&transaction, &mut row, now)
            .await
            .unwrap()
    );
    assert_eq!(ids(&transaction, now).await, vec!["final"]);
    assert_eq!(
        ids(&transaction, now + Duration::seconds(30)).await,
        vec!["final", "immediate"]
    );
    assert!(
        mark_agent_action_outbox_delivered(&transaction, &row.id, row.attempts, now)
            .await
            .unwrap()
    );
    assert!(
        !transition_agent_action_outbox(&transaction, &mut load(&transaction, "final").await, now)
            .await
            .unwrap()
    );
    assert!(ids(&transaction, now + Duration::hours(1)).await.is_empty());
    transaction.rollback().await.unwrap();
    assert_eq!(ids(&db, now).await, original_ids);
    assert_eq!(load(&db, "immediate").await.attempts, 0);
    assert_eq!(load(&db, "final").await.status, "pending");
}

#[tokio::test]
async fn action_outbox_index_migration_and_marker_rollback_and_retry_are_atomic() {
    let db = database().await;
    let now = utc_now();
    seed(&db, "existing-immediate", "pending", 0, now, None).await;
    seed(&db, "existing-final", "pending", 8, now, Some(now)).await;
    const MIGRATION: &str = "m20261004_000007_agent_action_outbox_ranges";
    let migrations = Migrator::migrations();
    let target = migrations
        .iter()
        .position(|migration| migration.name() == MIGRATION)
        .unwrap();
    Migrator::down(&db, Some((migrations.len() - target) as u32))
        .await
        .unwrap();
    async fn index_count<C: ConnectionTrait>(db: &C) -> i64 {
        db.query_one_raw(Statement::from_string(DatabaseBackend::Sqlite,
            "SELECT count(*) AS count FROM sqlite_master WHERE type='index' AND name IN ('idx_agent_action_outbox_immediate','idx_agent_action_outbox_timed')"))
            .await.unwrap().unwrap().try_get("", "count").unwrap()
    }
    assert_eq!(index_count(&db).await, 0);
    let transaction = db.begin().await.unwrap();
    Migrator::up(&transaction, None).await.unwrap();
    assert_eq!(index_count(&transaction).await, 2);
    transaction.rollback().await.unwrap();
    assert_eq!(index_count(&db).await, 0);
    assert!(
        !Migrator::get_applied_migrations(&db)
            .await
            .unwrap()
            .iter()
            .any(|migration| migration.name() == MIGRATION)
    );
    let immediate_before = load(&db, "existing-immediate").await;
    let final_before = load(&db, "existing-final").await;
    // A table/index namespace collision fails the second CREATE INDEX, after
    // the first DDL succeeded. The migration's own transaction must roll back.
    db.execute_unprepared("CREATE TABLE idx_agent_action_outbox_timed(id INTEGER)")
        .await
        .unwrap();
    assert!(Migrator::up(&db, None).await.is_err());
    assert_eq!(index_count(&db).await, 0);
    assert!(
        !Migrator::get_applied_migrations(&db)
            .await
            .unwrap()
            .iter()
            .any(|migration| migration.name() == MIGRATION)
    );
    assert_eq!(load(&db, "existing-immediate").await, immediate_before);
    assert_eq!(load(&db, "existing-final").await, final_before);
    db.execute_unprepared("DROP TABLE idx_agent_action_outbox_timed")
        .await
        .unwrap();
    // Retry must see the reverted marker and rebuild both ranges from source.
    Migrator::up(&db, None).await.unwrap();
    assert!(
        Migrator::get_applied_migrations(&db)
            .await
            .unwrap()
            .iter()
            .any(|migration| migration.name() == MIGRATION)
    );
    assert_eq!(load(&db, "existing-immediate").await, immediate_before);
    assert_eq!(load(&db, "existing-final").await, final_before);
    assert_eq!(index_count(&db).await, 2);
    assert_eq!(
        ids(&db, now).await,
        vec!["existing-immediate", "existing-final"]
    );
    assert_eq!(load(&db, "existing-immediate").await.attempts, 0);
    assert_eq!(load(&db, "existing-final").await.status, "pending");
}

// Test-only faults at the two externally observable boundaries: another
// writer between discovery reads, or a lost reply after SQLite commits.
#[derive(Clone, Copy)]
enum OutboxFault {
    LostCommitReply,
    ReadB,
    PanicReadB,
    PanicTransitionB,
    PanicCommitB,
    MoveToTimed(DateTimeWithTimeZone),
}
struct OutboxFaultConnection<C>(C, OutboxFault, std::sync::atomic::AtomicBool);
impl<C> OutboxFaultConnection<C> {
    fn new(db: C, fault: OutboxFault) -> Self {
        Self(db, fault, std::sync::atomic::AtomicBool::new(false))
    }
}
fn targets_b(statement: &Statement) -> bool {
    statement.values.as_ref().is_some_and(|values| {
        values
            .0
            .iter()
            .any(|value| matches!(value, sea_orm::Value::String(Some(id)) if id.as_str() == "b"))
    })
}

#[sea_orm::prelude::async_trait::async_trait]
impl<C: ConnectionTrait> ConnectionTrait for OutboxFaultConnection<C> {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.0.get_database_backend()
    }
    async fn execute_raw(&self, s: Statement) -> std::result::Result<sea_orm::ExecResult, DbErr> {
        if targets_b(&s) {
            self.2.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        let targets_b = targets_b(&s);
        let result = self.0.execute_raw(s).await?;
        if targets_b && matches!(self.1, OutboxFault::PanicTransitionB) {
            panic!("injected point transition unwind after write");
        }
        Ok(result)
    }
    async fn execute_unprepared(&self, s: &str) -> std::result::Result<sea_orm::ExecResult, DbErr> {
        self.0.execute_unprepared(s).await
    }
    async fn query_one_raw(
        &self,
        s: Statement,
    ) -> std::result::Result<Option<sea_orm::QueryResult>, DbErr> {
        if matches!(self.1, OutboxFault::PanicReadB) && targets_b(&s) {
            panic!("injected point read unwind");
        }
        if matches!(self.1, OutboxFault::ReadB) && targets_b(&s) {
            return Err(DbErr::Custom(
                "injected point-read failure without snapshot".to_owned(),
            ));
        }
        self.0.query_one_raw(s).await
    }
    async fn query_all_raw(
        &self,
        s: Statement,
    ) -> std::result::Result<Vec<sea_orm::QueryResult>, DbErr> {
        let immediate = s.sql == AGENT_ACTION_OUTBOX_IMMEDIATE_SQL;
        let rows = self.0.query_all_raw(s).await?;
        if immediate
            && let OutboxFault::MoveToTimed(due) = self.1
            && let Some(row) = rows.first()
        {
            // This write happens after the reader has returned its narrow IDs.
            agent_action_outbox::Entity::update_many()
                .col_expr(
                    agent_action_outbox::Column::NextAttemptAt,
                    sea_orm::sea_query::Expr::value(Some(due)),
                )
                .filter(agent_action_outbox::Column::Id.eq(row.try_get::<String>("", "id")?))
                .exec(&self.0)
                .await?;
        }
        Ok(rows)
    }
    fn support_returning(&self) -> bool {
        self.0.support_returning()
    }
    fn is_mock_connection(&self) -> bool {
        self.0.is_mock_connection()
    }
}

#[sea_orm::prelude::async_trait::async_trait]
impl<C: TransactionSession + Send> TransactionSession for OutboxFaultConnection<C> {
    async fn commit(self) -> std::result::Result<(), DbErr> {
        self.0.commit().await?;
        if matches!(self.1, OutboxFault::PanicCommitB)
            && self.2.load(std::sync::atomic::Ordering::SeqCst)
        {
            panic!("injected unwind after durable commit");
        }
        if matches!(self.1, OutboxFault::LostCommitReply)
            && self.2.load(std::sync::atomic::Ordering::SeqCst)
        {
            Err(DbErr::Custom("injected lost commit reply".to_owned()))
        } else {
            Ok(())
        }
    }
    async fn rollback(self) -> std::result::Result<(), DbErr> {
        self.0.rollback().await
    }
}

#[sea_orm::prelude::async_trait::async_trait]
impl<C: ConnectionTrait + TransactionTrait> TransactionTrait for OutboxFaultConnection<C> {
    type Transaction = OutboxFaultConnection<C::Transaction>;
    async fn begin(&self) -> std::result::Result<Self::Transaction, DbErr> {
        Ok(OutboxFaultConnection::new(self.0.begin().await?, self.1))
    }
    async fn begin_with_config(
        &self,
        isolation: Option<sea_orm::IsolationLevel>,
        access: Option<sea_orm::AccessMode>,
    ) -> std::result::Result<Self::Transaction, DbErr> {
        Ok(OutboxFaultConnection::new(
            self.0.begin_with_config(isolation, access).await?,
            self.1,
        ))
    }
    async fn begin_with_options(
        &self,
        options: sea_orm::TransactionOptions,
    ) -> std::result::Result<Self::Transaction, DbErr> {
        Ok(OutboxFaultConnection::new(
            self.0.begin_with_options(options).await?,
            self.1,
        ))
    }
    async fn transaction<F, T, E>(
        &self,
        _: F,
    ) -> std::result::Result<T, sea_orm::TransactionError<E>>
    where
        F: for<'c> FnOnce(
                &'c Self::Transaction,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = std::result::Result<T, E>> + Send + 'c>,
            > + Send,
        T: Send,
        E: std::fmt::Display + std::fmt::Debug + Send,
    {
        unreachable!("claim only uses explicit begin/commit")
    }
    async fn transaction_with_config<F, T, E>(
        &self,
        _: F,
        _: Option<sea_orm::IsolationLevel>,
        _: Option<sea_orm::AccessMode>,
    ) -> std::result::Result<T, sea_orm::TransactionError<E>>
    where
        F: for<'c> FnOnce(
                &'c Self::Transaction,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = std::result::Result<T, E>> + Send + 'c>,
            > + Send,
        T: Send,
        E: std::fmt::Display + std::fmt::Debug + Send,
    {
        unreachable!("claim only uses explicit begin/commit")
    }
}

#[tokio::test]
async fn action_outbox_unknown_second_commit_preserves_known_claims_without_retry() {
    let db = database().await;
    let now = utc_now();
    seed(&db, "a", "pending", 7, now, None).await;
    seed(&db, "b", "pending", 0, now, None).await;
    seed(&db, "c", "pending", 0, now, None).await;
    let fault = OutboxFaultConnection::new(db.clone(), OutboxFault::LostCommitReply);
    let batch = claim_agent_action_outbox_with_clock(&fault, 64, &|| now)
        .await
        .unwrap();
    assert_eq!(batch.errors.len(), 1);
    assert!(
        batch.errors[0]
            .to_string()
            .contains("commit outcome unknown")
    );
    assert_eq!(
        batch
            .rows
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "c"]
    );
    assert_eq!(batch.rows[0].attempts, 8);
    assert_eq!(
        load(&db, "b").await.attempts,
        1,
        "unknown row was committed exactly once"
    );
    assert_eq!(
        load(&db, "b").await.next_attempt_at,
        Some(now + Duration::seconds(30))
    );
    assert!(
        ids(&db, now).await.is_empty(),
        "no refill or same-quantum rediscovery"
    );
}

#[tokio::test]
async fn action_outbox_point_read_error_keeps_partial_success_and_reports_missing_durability() {
    let db = database().await;
    let now = utc_now();
    for (id, attempts) in [("a", 7), ("b", 0), ("c", 0)] {
        seed(&db, id, "pending", attempts, now, None).await;
    }
    let fault = OutboxFaultConnection::new(db.clone(), OutboxFault::ReadB);
    let batch = claim_agent_action_outbox_with_clock(&fault, 64, &|| now)
        .await
        .unwrap();
    assert_eq!(
        batch
            .rows
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "c"]
    );
    assert_eq!(batch.errors.len(), 1);
    assert!(batch.errors[0].to_string().contains("no snapshot"));
    assert_eq!(load(&db, "b").await.attempts, 0);
    assert_eq!(load(&db, "b").await.next_attempt_at, None);
}

#[tokio::test]
async fn action_outbox_failed_transition_defers_original_snapshot_without_spending_attempt() {
    let db = database().await;
    let now = utc_now();
    for (id, attempts) in [("a", 7), ("b", 0), ("c", 0)] {
        seed(&db, id, "pending", attempts, now, None).await;
    }
    // Test-only storage fault: refuses execution attempt writes, permits delay.
    db.execute_unprepared("CREATE TRIGGER reject_b_claim BEFORE UPDATE OF attempts ON agent_action_outbox WHEN OLD.id='b' BEGIN SELECT RAISE(ABORT,'local claim failure'); END").await.unwrap();
    let batch = claim_agent_action_outbox_with_clock(&db, 64, &|| now)
        .await
        .unwrap();
    assert_eq!(
        batch
            .rows
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "c"]
    );
    assert_eq!(batch.rows[0].attempts, 8);
    assert_eq!(batch.errors.len(), 1);
    let delayed = load(&db, "b").await;
    assert_eq!((delayed.status.as_str(), delayed.attempts), ("pending", 0));
    assert_eq!(delayed.next_attempt_at, Some(now + Duration::seconds(30)));
    assert!(ids(&db, now).await.is_empty());
}

#[tokio::test]
async fn action_outbox_failed_or_stale_conditional_deferral_preserves_new_ownership() {
    let db = database().await;
    let now = utc_now();
    seed(&db, "b", "pending", 0, now, None).await;
    let stale_null = load(&db, "b").await;
    let batch = claim_agent_action_outbox_with_clock(&db, 2, &|| now)
        .await
        .unwrap();
    let claimed = batch.rows.into_iter().next().unwrap();
    assert!(
        !defer_unclaimed_agent_action_outbox(&db, &stale_null, &|| now)
            .await
            .unwrap()
    );
    assert_eq!(load(&db, "b").await, claimed);
    let mut stale_lease = claimed.clone();
    stale_lease.next_attempt_at = Some(now + Duration::seconds(29));
    assert!(
        !defer_unclaimed_agent_action_outbox(&db, &stale_lease, &|| now)
            .await
            .unwrap()
    );
    db.execute_unprepared("CREATE TRIGGER reject_b_delay BEFORE UPDATE ON agent_action_outbox WHEN OLD.id='b' BEGIN SELECT RAISE(ABORT,'bookkeeping unavailable'); END").await.unwrap();
    assert!(
        defer_unclaimed_agent_action_outbox(&db, &claimed, &|| now)
            .await
            .is_err()
    );
    assert_eq!(load(&db, "b").await, claimed);
    let failed = claim_agent_action_outbox_with_clock(&db, 2, &|| now + Duration::seconds(30))
        .await
        .unwrap();
    assert!(failed.rows.is_empty());
    assert_eq!(
        failed.errors.len(),
        2,
        "claim failure and failed durable deferral both reported"
    );
    assert!(
        failed.errors[1]
            .to_string()
            .contains("durability unavailable")
    );
    assert_eq!(
        load(&db, "b").await,
        claimed,
        "failed bookkeeping does not alter ownership"
    );
}

#[tokio::test]
async fn action_outbox_minimum_budget_rejects_one_and_serves_both_ranges_at_two() {
    let db = database().await;
    let now = utc_now();
    seed(&db, "immediate", "pending", 0, now, None).await;
    seed(&db, "timed", "failed", 0, now, Some(now)).await;
    let error = claim_agent_action_outbox_with_clock(&db, 1, &|| {
        panic!("invalid limit must reject before discovery")
    })
    .await
    .unwrap_err();
    let empty = claim_agent_action_outbox_with_clock(&db, 0, &|| panic!("no-op must not discover"))
        .await
        .unwrap();
    assert!(empty.rows.is_empty() && empty.errors.is_empty());
    assert!(error.to_string().contains("2..=64"));
    assert_eq!(load(&db, "immediate").await.attempts, 0);
    let batch = claim_agent_action_outbox_with_clock(&db, 2, &|| now)
        .await
        .unwrap();
    assert!(batch.errors.is_empty());
    assert_eq!(
        batch
            .rows
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["immediate", "timed"]
    );
}

#[derive(Default)]
struct RouteObserver {
    reads: std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadClass>>,
    writes: std::sync::Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>,
    enqueued: tokio::sync::Notify,
}
impl pioneer_sqlite::SqliteReadObserver for RouteObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        if let pioneer_sqlite::SqliteReadEvent::OperationFinished { class, .. } = event {
            self.reads.lock().unwrap().push(class);
        }
    }
}
impl pioneer_sqlite::SqliteWriteObserver for RouteObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        self.writes.lock().unwrap().push(event);
        if matches!(event, pioneer_sqlite::SqliteWriteEvent::Enqueued { .. }) {
            self.enqueued.notify_one();
        }
    }
}
impl RouteObserver {
    async fn wait_for_enqueue(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if self
                    .writes
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|event| matches!(event, pioneer_sqlite::SqliteWriteEvent::Enqueued { .. }))
                {
                    return;
                }
                self.enqueued.notified().await;
            }
        })
        .await
        .unwrap();
    }
}
struct DatabasePath(std::path::PathBuf);
impl Drop for DatabasePath {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

#[tokio::test]
async fn action_outbox_routes_cancellation_and_clock_after_writer_admission() {
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadClass, SqliteWriteClass, SqliteWriteEvent, SqliteWriteExecutor,
        sqlite_read_only_connection_url,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let path = DatabasePath(
        std::env::temp_dir().join(format!("pioneer-outbox-{}.sqlite", uuid::Uuid::new_v4())),
    );
    let mut options =
        sea_orm::ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.0.display()));
    options.max_connections(1);
    let writer = Database::connect(options).await.unwrap();
    Migrator::up(&writer, None).await.unwrap();
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    writer
        .execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    let now = utc_now();
    seed(&writer, "queued", "pending", 0, now, None).await;
    let mut options = sea_orm::ConnectOptions::new(sqlite_read_only_connection_url(&path.0));
    options.max_connections(2).map_sqlx_sqlite_opts(|options| {
        options
            .read_only(true)
            .create_if_missing(false)
            .pragma("query_only", "ON")
    });
    let reader = Database::connect(options).await.unwrap();
    let observer = Arc::new(RouteObserver::default());
    let db = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    db.validate_reader().await.unwrap();
    let store = crate::CrudStore::new(db.clone());
    let maintenance = store.with_maintenance_access().database_connection();
    assert_eq!(maintenance.read_class(), SqliteReadClass::Maintenance);
    assert_eq!(maintenance.write_class(), SqliteWriteClass::Maintenance);
    let held = db.begin().await.unwrap();
    observer.writes.lock().unwrap().clear();
    observer.reads.lock().unwrap().clear();
    let cancelled = tokio::spawn({
        let maintenance = maintenance.clone();
        async move { claim_agent_action_outbox(&maintenance, 64).await }
    });
    observer.wait_for_enqueue().await;
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    assert!(observer.writes.lock().unwrap().iter().any(|event| matches!(event,
        SqliteWriteEvent::Cancelled { class: SqliteWriteClass::Maintenance, queue, .. } if queue.maintenance == 0
    )));
    assert_eq!(
        load(&db, "queued").await.attempts,
        0,
        "discovery really uses the reader while writer is occupied"
    );
    observer.writes.lock().unwrap().clear();
    observer.reads.lock().unwrap().clear();
    let advanced = Arc::new(AtomicBool::new(false));
    let claim = tokio::spawn({
        let advanced = advanced.clone();
        let maintenance = maintenance.clone();
        async move {
            claim_agent_action_outbox_with_clock(&maintenance, 64, &|| {
                now + Duration::seconds(if advanced.load(Ordering::SeqCst) {
                    40
                } else {
                    0
                })
            })
            .await
        }
    });
    observer.wait_for_enqueue().await;
    advanced.store(true, Ordering::SeqCst);
    held.rollback().await.unwrap();
    let batch = claim.await.unwrap().unwrap();
    assert!(batch.errors.is_empty());
    let rows = batch.rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].next_attempt_at, Some(now + Duration::seconds(70)));
    assert!(!observer.reads.lock().unwrap().is_empty());
    assert!(
        observer
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|class| *class == SqliteReadClass::Maintenance)
    );
    assert_eq!(
        observer
            .writes
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                SqliteWriteEvent::Acquired { class, .. } => Some(*class),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![SqliteWriteClass::Maintenance]
    );
    // A deferred backlog costs exactly the two empty reader ranges, no writer.
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    assert!(
        claim_agent_action_outbox_with_clock(&maintenance, 64, &|| now + Duration::seconds(40))
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        *observer.reads.lock().unwrap(),
        vec![SqliteReadClass::Maintenance; 2]
    );
    assert!(observer.writes.lock().unwrap().is_empty());
    // The caller may now prepare/decode without retaining writer capacity.
    let foreground = tokio::time::timeout(std::time::Duration::from_secs(1), db.begin())
        .await
        .unwrap()
        .unwrap();
    foreground.rollback().await.unwrap();
    assert_eq!(load(&db, "queued").await.payload_json, rows[0].payload_json);
    assert!(db.reader_query_only_enabled().await.unwrap());
    // A separate failure delay also reads its clock after its own admission.
    seed(&maintenance, "unclaimed", "pending", 0, now, None).await;
    let snapshot = load(&maintenance, "unclaimed").await;
    let held = db.begin().await.unwrap();
    observer.writes.lock().unwrap().clear();
    advanced.store(false, Ordering::SeqCst);
    let delay = tokio::spawn({
        let maintenance = maintenance.clone();
        let advanced = advanced.clone();
        async move {
            defer_unclaimed_agent_action_outbox(&maintenance, &snapshot, &|| {
                now + Duration::seconds(if advanced.load(Ordering::SeqCst) {
                    100
                } else {
                    0
                })
            })
            .await
        }
    });
    observer.wait_for_enqueue().await;
    advanced.store(true, Ordering::SeqCst);
    held.rollback().await.unwrap();
    assert!(delay.await.unwrap().unwrap());
    let delayed = load(&maintenance, "unclaimed").await;
    assert_eq!(delayed.attempts, 0);
    assert_eq!(delayed.next_attempt_at, Some(now + Duration::seconds(130)));
    assert_eq!(
        observer
            .writes
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                SqliteWriteEvent::Acquired { class, .. } => Some(*class),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![SqliteWriteClass::Maintenance]
    );
    // Reuse the physical reader/single-writer fixture for point panics before
    // a snapshot, after a source write, and after a real commit acknowledgement
    // would have returned. No panic is promoted into a successful claim/repair.
    for fault in [
        OutboxFault::PanicReadB,
        OutboxFault::PanicTransitionB,
        OutboxFault::PanicCommitB,
    ] {
        for (id, attempts) in [("a", 7), ("b", 0), ("c", 7)] {
            seed(&maintenance, id, "pending", attempts, now, None).await;
        }
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        let connection = OutboxFaultConnection::new(maintenance.clone(), fault);
        let batch = claim_agent_action_outbox_with_clock(&connection, 64, &|| now)
            .await
            .unwrap();
        assert_eq!(batch.errors.len(), 1);
        assert_eq!(
            batch.errors[0].to_string(),
            "outbox_claim_panic_outcome_unknown"
        );
        assert_eq!(
            batch
                .rows
                .iter()
                .map(|row| (row.id.as_str(), row.attempts))
                .collect::<Vec<_>>(),
            vec![("a", 8), ("c", 8)]
        );
        let ambiguous = load(&maintenance, "b").await;
        assert_eq!(ambiguous.status, "pending");
        assert_eq!(
            ambiguous.attempts,
            i64::from(matches!(fault, OutboxFault::PanicCommitB))
        );
        assert_eq!(
            ambiguous.next_attempt_at,
            if matches!(fault, OutboxFault::PanicCommitB) {
                Some(now + Duration::seconds(30))
            } else {
                None
            }
        );
        assert_eq!(
            ambiguous.last_error, None,
            "no compensation or claimed durable deferral after unwind"
        );
        assert_eq!(
            observer
                .writes
                .lock()
                .unwrap()
                .iter()
                .filter_map(|event| match event {
                    SqliteWriteEvent::Acquired { class, .. } => Some(*class),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![SqliteWriteClass::Maintenance; 3],
            "three selected IDs, no retry/refill after panic"
        );
        assert!(
            observer
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|class| *class == SqliteReadClass::Maintenance)
        );
        let writer = tokio::time::timeout(std::time::Duration::from_secs(1), db.begin())
            .await
            .unwrap()
            .unwrap();
        writer.rollback().await.unwrap();
        agent_action_outbox::Entity::delete_many()
            .filter(agent_action_outbox::Column::Id.is_in(["a", "b", "c"]))
            .exec(&maintenance)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn action_outbox_concurrent_range_move_does_not_duplicate_an_input() {
    let db = database().await;
    let now = utc_now();
    seed(&db, "moving", "pending", 0, now, None).await;
    let concurrent_writer = OutboxFaultConnection::new(db.clone(), OutboxFault::MoveToTimed(now));
    let selected = discover_agent_action_outbox(&concurrent_writer, now, 64)
        .await
        .unwrap();
    assert_eq!(
        selected.into_iter().map(|row| row.id).collect::<Vec<_>>(),
        vec!["moving"]
    );
    assert_eq!(load(&db, "moving").await.next_attempt_at, Some(now));
}
