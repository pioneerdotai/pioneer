//! Primary adapter regressions for the occurrence producer cutover. NOT RUN.
use super::*;
use crate::CrudStore;
use migration::{Migrator, MigratorTrait};
use pioneer_entity::{
    task_occurrence_reconcile_pending as pending, task_occurrence_reconcile_sequence as sequence,
};
use pioneer_protocol::TaskRunStatus;
use sea_orm::sea_query::ExprTrait;
use sea_orm::{Database, TransactionTrait};

async fn store() -> CrudStore {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let database = pioneer_sqlite::SqliteDatabase::from_single_connection(db);
    pioneer_sqlite::apply_sqlite_pragmas(&database)
        .await
        .unwrap();
    let tx = database.begin().await.unwrap();
    Migrator::up(&*tx, None).await.unwrap();
    tx.commit().await.unwrap();
    CrudStore::new(database).with_maintenance_access()
}
async fn seed(store: &CrudStore) {
    let db = store.database_connection();
    let now = chrono::Utc::now().fixed_offset();
    // These rows exercise tracking only, never destructive cleanup.
    task_run::Entity::insert(task_run::ActiveModel {
        id: Set("occurrence-run".into()),
        task_id: Set("occurrence-task".into()),
        run_group_id: Set("occurrence-run".into()),
        attempt_number: Set(1),
        run_number: Set(1),
        status: Set("running".into()),
        executor_kind: Set("system".into()),
        created_at: Set(now),
        updated_at: Set(now),
        ..Default::default()
    })
    .exec_without_returning(&db)
    .await
    .unwrap();
}
#[tokio::test]
async fn cutover_preserves_rollback_and_invalidates_claim_without_resetting_retry() {
    let store = store().await;
    seed(&store).await;
    let db = store.database_connection();
    assert!(store.task_occurrence_tracking_is_typed().await.unwrap());
    let tx = db.begin().await.unwrap();
    super::super::task_run::update_run_status(
        &tx,
        "occurrence-run",
        TaskRunStatus::Waiting,
        chrono::Utc::now().fixed_offset(),
    )
    .await
    .unwrap();
    assert!(
        pending::Entity::find_by_id("occurrence-run")
            .one(&tx)
            .await
            .unwrap()
            .is_some()
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        task_run::Entity::find_by_id("occurrence-run")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    assert!(
        pending::Entity::find_by_id("occurrence-run")
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );

    let tx = db.begin().await.unwrap();
    super::super::task_run::update_run_status(
        &tx,
        "occurrence-run",
        TaskRunStatus::Waiting,
        chrono::Utc::now().fixed_offset(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let before = pending::Entity::find_by_id("occurrence-run")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    pending::Entity::update_many()
        .col_expr(pending::Column::ClaimToken, Expr::val("claim"))
        .col_expr(pending::Column::NextAttemptAt, Expr::val(9_000_000_000_i64))
        .col_expr(pending::Column::AttemptCount, Expr::val(4_i64))
        .filter(pending::Column::RunId.eq("occurrence-run"))
        .exec(&db)
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    super::super::task_run::update_run_status(
        &tx,
        "occurrence-run",
        TaskRunStatus::WaitingReview,
        chrono::Utc::now().fixed_offset(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let after = pending::Entity::find_by_id("occurrence-run")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(after.generation > before.generation);
    assert_eq!(
        (
            after.next_attempt_at,
            after.attempt_count,
            after.claim_token
        ),
        (9_000_000_000, 4, None)
    );
    let stale = pending::Entity::delete_many()
        .filter(pending::Column::RunId.eq("occurrence-run"))
        .filter(pending::Column::Generation.eq(before.generation))
        .exec(&db)
        .await
        .unwrap();
    assert_eq!(stale.rows_affected, 0);
}
#[tokio::test]
async fn unknown_trigger_generation_stays_in_compatibility_mode() {
    let store = store().await;
    let db = store.database_connection();
    // A foreign repair installed an unknown producer generation. Runtime
    // readiness must retain compatibility rather than trust typed producers.
    db.execute_unprepared("CREATE TRIGGER task_occurrence_reconcile_task_run_update AFTER UPDATE OF status ON task_run BEGIN SELECT 1; END").await.unwrap();
    assert!(!store.task_occurrence_tracking_is_typed().await.unwrap());
}

#[tokio::test]
async fn terminal_parent_mismatch_is_removed_when_turn_projection_catches_up() {
    let store = store().await;
    seed(&store).await;
    let db = store.database_connection();
    let now = chrono::Utc::now().fixed_offset();
    turn::Entity::insert(turn::ActiveModel {
        id: Set("occurrence-run".into()),
        thread_id: Set("parent-thread".into()),
        status: Set("in_progress".into()),
        turn_kind: Set("task_run".into()),
        origin: Set("system".into()),
        prompt_manifest_json: Set("{}".into()),
        created_at: Set(now),
        updated_at: Set(now),
        ..Default::default()
    })
    .exec_without_returning(&db)
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    super::super::task_run::update_run_status(&tx, "occurrence-run", TaskRunStatus::Succeeded, now)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    use pioneer_entity::task_run_occurrence_reconcile_pending as parents;
    assert!(
        parents::Entity::find_by_id("occurrence-run")
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    let tx = db.begin().await.unwrap();
    super::super::turn::update_turn_status(
        &tx,
        "parent-thread",
        "occurrence-run",
        pioneer_protocol::TurnStatus::Completed,
        None,
        now,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(
        parents::Entity::find_by_id("occurrence-run")
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn enqueue_failure_rolls_back_source_transition() {
    let store = store().await;
    seed(&store).await;
    let db = store.database_connection();
    sequence::Entity::update_many()
        .col_expr(sequence::Column::Generation, Expr::val(i64::MAX))
        .filter(sequence::Column::Singleton.eq(1))
        .exec(&db)
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    let result = super::super::task_run::update_run_status(
        &tx,
        "occurrence-run",
        TaskRunStatus::Waiting,
        chrono::Utc::now().fixed_offset(),
    )
    .await;
    assert!(
        result.is_err(),
        "exhausted generation must abort the atomic write set"
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        task_run::Entity::find_by_id("occurrence-run")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    assert!(
        pending::Entity::find_by_id("occurrence-run")
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );
}

#[derive(Clone, Copy)]
enum RacedSequence {
    Contract,
    Parent,
}
impl RacedSequence {
    fn table(self) -> &'static str {
        match self {
            Self::Contract => "\"task_occurrence_reconcile_sequence\"",
            Self::Parent => "\"task_run_occurrence_reconcile_sequence\"",
        }
    }
}

// Inject a stale observation, not a fabricated success/error: read the actual
// sequence row, change its stored generation, return the observed real row.
// The real conditional UPDATE must lose its fence. All storage I/O delegates
// to the scoped domain transaction, which must be dropped before retry.
struct SequenceReadRace<'a, C> {
    inner: &'a C,
    sequence: RacedSequence,
    fault_remaining: &'a std::sync::atomic::AtomicBool,
}
#[sea_orm::prelude::async_trait::async_trait]
impl<C: ConnectionTrait> ConnectionTrait for SequenceReadRace<'_, C> {
    fn get_database_backend(&self) -> DbBackend {
        self.inner.get_database_backend()
    }
    async fn execute_raw(
        &self,
        statement: Statement,
    ) -> std::result::Result<sea_orm::ExecResult, sea_orm::DbErr> {
        self.inner.execute_raw(statement).await
    }
    async fn execute_unprepared(
        &self,
        sql: &str,
    ) -> std::result::Result<sea_orm::ExecResult, sea_orm::DbErr> {
        self.inner.execute_unprepared(sql).await
    }
    async fn query_one_raw(
        &self,
        statement: Statement,
    ) -> std::result::Result<Option<sea_orm::QueryResult>, sea_orm::DbErr> {
        let sequence_read =
            statement.sql.starts_with("SELECT") && statement.sql.contains(self.sequence.table());
        let observed = self.inner.query_one_raw(statement).await?;
        if sequence_read
            && observed.is_some()
            && self
                .fault_remaining
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            match self.sequence {
                RacedSequence::Contract => {
                    sequence::Entity::update_many()
                        .col_expr(
                            sequence::Column::Generation,
                            Expr::col(sequence::Column::Generation).add(1),
                        )
                        .filter(sequence::Column::Singleton.eq(1))
                        .exec(self.inner)
                        .await?;
                }
                RacedSequence::Parent => {
                    use pioneer_entity::task_run_occurrence_reconcile_sequence as parent_seq;
                    parent_seq::Entity::update_many()
                        .col_expr(
                            parent_seq::Column::Generation,
                            Expr::col(parent_seq::Column::Generation).add(1),
                        )
                        .filter(parent_seq::Column::Singleton.eq(1))
                        .exec(self.inner)
                        .await?;
                }
            }
        }
        Ok(observed)
    }
    async fn query_all_raw(
        &self,
        statement: Statement,
    ) -> std::result::Result<Vec<sea_orm::QueryResult>, sea_orm::DbErr> {
        self.inner.query_all_raw(statement).await
    }
}

#[tokio::test]
async fn sequence_cas_race_retries_the_whole_source_transaction_from_clean_storage() {
    use pioneer_entity::task_run_occurrence_reconcile_sequence as parent_seq;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    for raced in [RacedSequence::Contract, RacedSequence::Parent] {
        let store = store().await;
        seed(&store).await;
        let db = store.database_connection();
        let now = chrono::Utc::now().fixed_offset();
        if matches!(raced, RacedSequence::Parent) {
            turn::Entity::insert(turn::ActiveModel {
                id: Set("occurrence-run".into()),
                thread_id: Set("parent-thread".into()),
                status: Set("in_progress".into()),
                turn_kind: Set("task_run".into()),
                origin: Set("system".into()),
                prompt_manifest_json: Set("{}".into()),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            })
            .exec_without_returning(&db)
            .await
            .unwrap();
        }
        let fault_remaining = AtomicBool::new(true);
        let attempts = AtomicUsize::new(0);
        store
            .run_serialized_write(|| async {
                attempts.fetch_add(1, Ordering::SeqCst);
                let tx = db.begin().await?;
                // Also on the second attempt: neither the source write nor sequence
                // increments/pending rows from the failed transaction may survive.
                assert_eq!(
                    task_run::Entity::find_by_id("occurrence-run")
                        .one(&tx)
                        .await?
                        .unwrap()
                        .status,
                    "running"
                );
                assert!(
                    pending::Entity::find_by_id("occurrence-run")
                        .one(&tx)
                        .await?
                        .is_none()
                );
                assert_eq!(
                    sequence::Entity::find_by_id(1)
                        .one(&tx)
                        .await?
                        .unwrap()
                        .generation,
                    0
                );
                assert_eq!(
                    parent_seq::Entity::find_by_id(1)
                        .one(&tx)
                        .await?
                        .unwrap()
                        .generation,
                    0
                );
                let connection = SequenceReadRace {
                    inner: &tx,
                    sequence: raced,
                    fault_remaining: &fault_remaining,
                };
                let status = if matches!(raced, RacedSequence::Parent) {
                    TaskRunStatus::Succeeded
                } else {
                    TaskRunStatus::Waiting
                };
                match super::super::task_run::update_run_status(
                    &connection,
                    "occurrence-run",
                    status,
                    now,
                )
                .await
                {
                    Err(error) => {
                        assert!(
                            error
                                .downcast_ref::<super::super::portable_payload::PreflightRace>()
                                .is_some()
                        );
                        // No explicit rollback: production's error path must drop
                        // this transaction/future before the retry closure runs.
                        Err(error)
                    }
                    Ok(outcome) => {
                        assert!(matches!(
                            outcome,
                            super::super::ProjectionWriteOutcome::Applied
                        ));
                        tx.commit().await?;
                        Ok(())
                    }
                }
            })
            .await
            .unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(!fault_remaining.load(Ordering::SeqCst));
        assert_eq!(
            sequence::Entity::find_by_id(1)
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .generation,
            1
        );
        assert_eq!(
            task_run::Entity::find_by_id("occurrence-run")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .status,
            if matches!(raced, RacedSequence::Parent) {
                "succeeded"
            } else {
                "waiting"
            }
        );
        if matches!(raced, RacedSequence::Parent) {
            assert_eq!(
                parent_seq::Entity::find_by_id(1)
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap()
                    .generation,
                1
            );
        }
    }
}

// The absent counterpart branch is deliberately conservative; a later typed
// birth discharges it when positive headers establish actual agreement.
#[tokio::test]
async fn missing_parent_retains_obligation_until_consistent_birth() {
    use pioneer_entity::task_run_occurrence_reconcile_pending as parents;
    let store = store().await;
    seed(&store).await;
    let db = store.database_connection();
    let now = chrono::Utc::now().fixed_offset();
    let tx = db.begin().await.unwrap();
    super::super::task_run::update_run_status(&tx, "occurrence-run", TaskRunStatus::Succeeded, now)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(
        parents::Entity::find_by_id("occurrence-run")
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    let tx = db.begin().await.unwrap();
    let parent = turn::ActiveModel {
        id: Set("occurrence-run".into()),
        thread_id: Set("parent-thread".into()),
        status: Set("completed".into()),
        turn_kind: Set("task_run".into()),
        origin: Set("system".into()),
        prompt_manifest_json: Set("{}".into()),
        created_at: Set(now),
        updated_at: Set(now),
        ..Default::default()
    };
    turn::Entity::insert(parent)
        .exec_without_returning(&tx)
        .await
        .unwrap();
    let parent = turn::Entity::find_by_id("occurrence-run")
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    turn_change(&tx, None, Some(&parent)).await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        parents::Entity::find_by_id("occurrence-run")
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn known_ineligible_births_do_not_fill_parent_queue_and_deletes_keep_generation() {
    use pioneer_entity::{
        task_run_occurrence_reconcile_pending as parents,
        task_run_occurrence_reconcile_sequence as parent_sequence,
    };
    let store = store().await;
    seed(&store).await;
    let db = store.database_connection();
    let now = chrono::Utc::now().fixed_offset();
    let before = parent_sequence::Entity::find_by_id(1)
        .one(&db)
        .await
        .unwrap()
        .unwrap()
        .generation;
    let tx = db.begin().await.unwrap();
    // A real source adapter publishes the contract owner's birth, while a
    // nonterminal Run cannot require a terminal parent projection repair.
    let template = task_run::Entity::find_by_id("occurrence-run")
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    let mut queued = crate::task_run_from_db_model(template).unwrap();
    queued.id = "ordinary-queued".into();
    queued.run_group_id = queued.id.clone();
    queued.run_number = 2;
    queued.status = TaskRunStatus::Queued;
    super::super::task_run::upsert_run(&tx, &queued)
        .await
        .unwrap();
    assert!(
        pending::Entity::find_by_id(&queued.id)
            .one(&tx)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        parents::Entity::find_by_id(&queued.id)
            .one(&tx)
            .await
            .unwrap()
            .is_none()
    );
    turn::Entity::insert(turn::ActiveModel {
        id: Set("ordinary-conversation".into()),
        thread_id: Set("parent-thread".into()),
        status: Set("in_progress".into()),
        turn_kind: Set("conversation".into()),
        origin: Set("system".into()),
        prompt_manifest_json: Set("{}".into()),
        created_at: Set(now),
        updated_at: Set(now),
        ..Default::default()
    })
    .exec_without_returning(&tx)
    .await
    .unwrap();
    let old = turn::Entity::find_by_id("ordinary-conversation")
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    turn_change(&tx, None, Some(&old)).await.unwrap();
    turn::Entity::update_many()
        .col_expr(turn::Column::Status, Expr::val("completed"))
        .filter(turn::Column::Id.eq(&old.id))
        .exec(&tx)
        .await
        .unwrap();
    let new = turn::Entity::find_by_id(&old.id)
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    turn_change(&tx, Some(&old), Some(&new)).await.unwrap();
    assert!(
        parents::Entity::find_by_id(&old.id)
            .one(&tx)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        parent_sequence::Entity::find_by_id(1)
            .one(&tx)
            .await
            .unwrap()
            .unwrap()
            .generation,
        before
    );
    tx.commit().await.unwrap();

    // An explicit source deletion has authoritative absence. It must remove
    // old work without allocating a conservative birth generation first.
    let tx = db.begin().await.unwrap();
    super::super::task_run::update_run_status(&tx, &queued.id, TaskRunStatus::Succeeded, now)
        .await
        .unwrap();
    let old = task_run::Entity::find_by_id(&queued.id)
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    assert!(
        parents::Entity::find_by_id(&queued.id)
            .one(&tx)
            .await
            .unwrap()
            .is_some()
    );
    let generation = parent_sequence::Entity::find_by_id(1)
        .one(&tx)
        .await
        .unwrap()
        .unwrap()
        .generation;
    task_run::Entity::delete_by_id(&queued.id)
        .exec(&tx)
        .await
        .unwrap();
    run_change(&tx, Some(&old), None).await.unwrap();
    assert!(
        parents::Entity::find_by_id(&queued.id)
            .one(&tx)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        parent_sequence::Entity::find_by_id(1)
            .one(&tx)
            .await
            .unwrap()
            .unwrap()
            .generation,
        generation
    );
    tx.commit().await.unwrap();
}
