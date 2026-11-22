//! Task and turn source adapters share an atomic occurrence publication.
use super::OccurrencePublicationRace;
use crate::{projection::ProjectionWriteOutcome, test_support::occurrence::seed_occurrence_pair};
use pioneer_entity::{
    task_occurrence_reconcile_pending as pending, task_occurrence_reconcile_sequence as sequence,
    task_run, turn,
};
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, PostgresDatabase,
    retry_with_backoff,
};
use pioneer_postgres_test::{TestDatabase, with_database};
use pioneer_protocol::TaskRunStatus;
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, Set, Statement,
    TransactionTrait,
};
use std::time::Duration;

async fn store(database: &TestDatabase) -> PostgresDatabase {
    let reader = database.connect_read_only().await.unwrap();
    PostgresDatabase::new(reader, database.connection().clone()).maintenance()
}
#[tokio::test]
async fn terminal_parent_mismatch_is_removed_when_turn_projection_catches_up() {
    with_database(async |database| {
        let db = store(database).await;
        seed_occurrence_pair(&db).await;
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
        crate::task_run::update_run_status(&tx, "occurrence-run", TaskRunStatus::Succeeded, now)
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
        crate::turn_sources::update_turn_status(
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
        Ok(())
    })
    .await
    .unwrap();
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
        with_database(async |database| {
            let db = store(database).await;
            seed_occurrence_pair(&db).await;
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
            retry_with_backoff(
                || async {
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
                    match crate::task_run::update_run_status(
                        &connection,
                        "occurrence-run",
                        status,
                        now,
                    )
                    .await
                    {
                        Err(error) => {
                            assert!(error.downcast_ref::<OccurrencePublicationRace>().is_some());
                            // No explicit rollback: production's error path must drop
                            // this transaction/future before the retry closure runs.
                            Err(error)
                        }
                        Ok(outcome) => {
                            assert!(matches!(outcome, ProjectionWriteOutcome::Applied));
                            tx.commit().await?;
                            Ok(())
                        }
                    }
                },
                crate::retry::is_retryable,
                DEFAULT_LOCK_RETRY_ATTEMPTS,
                Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
            )
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
            Ok(())
        })
        .await
        .unwrap();
    }
}
