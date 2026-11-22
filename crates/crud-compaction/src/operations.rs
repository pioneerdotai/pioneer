//! Compaction operation authority and bounded admission.

use anyhow::{Result, ensure};
use pioneer_compaction::{FORMAT_VERSION, OperationSnapshot};
use pioneer_entity::{compaction_context, compaction_execution_stop, compaction_operation, turn};
use pioneer_postgres_pools::PostgresDatabase;
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, OnConflict, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect, Statement, TransactionTrait,
};

use crate::SOURCE_PAGE_BYTES;
use crate::sweep_work::retry_conflict;

#[derive(Clone, Debug, FromQueryResult)]
pub struct OperationRecord {
    pub id: String,
    pub owner: String,
    pub status: String,
    pub snapshot: String,
    pub execution_turn: Option<String>,
    pub deadline_ms: i64,
    pub attempts: i64,
    pub transient_retries: i64,
    pub correction: i64,
    pub next_portion: i64,
    pub outcome: Option<String>,
}

impl From<compaction_operation::Model> for OperationRecord {
    fn from(row: compaction_operation::Model) -> Self {
        Self {
            id: row.id,
            owner: row.owner,
            status: row.status,
            snapshot: row.snapshot,
            execution_turn: row.execution_turn,
            deadline_ms: row.deadline_ms,
            attempts: row.attempts,
            transient_retries: row.transient_retries,
            correction: row.correction,
            next_portion: row.next_portion,
            outcome: row.outcome,
        }
    }
}

pub async fn compaction_admit(
    db: &PostgresDatabase,
    workspace: &str,
    thread: &str,
    snapshot: &OperationSnapshot,
) -> Result<OperationRecord> {
    compaction_admit_for_turn(db, workspace, thread, snapshot, None).await
}

pub async fn compaction_admit_for_turn(
    db: &PostgresDatabase,
    workspace: &str,
    thread: &str,
    snapshot: &OperationSnapshot,
    execution_turn: Option<&str>,
) -> Result<OperationRecord> {
    let snapshot_json = serde_json::to_string(snapshot)?;
    ensure!(
        snapshot_json.len() <= SOURCE_PAGE_BYTES,
        "operation metadata exceeds bounded admission"
    );
    let deadline = i64::try_from(snapshot.admission.deadline_ms)?;
    // Preparation reads no mutable DB state. Owner and format are revalidated inside the write boundary.
    retry_conflict(|| async {
            let txn = db.begin().await?;
            compaction_context::Entity::insert(compaction_context::ActiveModel {workspace_id: sea_orm::Set((workspace).to_owned()), thread_id: sea_orm::Set((thread).to_owned()), owner: sea_orm::Set((snapshot.owner.clone()).to_owned()), ..Default::default() }).on_conflict(OnConflict::columns([compaction_context::Column::Owner]).do_nothing().to_owned()).exec_without_returning(&txn).await?;
            let valid = compaction_context::Entity::find()
            .select_only()
            .column(compaction_context::Column::Owner)
            .filter(Expr::col(compaction_context::Column::Owner)
                .eq(Expr::Value(snapshot.owner.clone()
                        .into()))
                .and(Expr::col(compaction_context::Column::WorkspaceId)
                    .eq(Expr::Value(workspace.into())))
                .and(Expr::col(compaction_context::Column::ThreadId)
                    .eq(Expr::Value(thread.into())))
                .and(Expr::col(compaction_context::Column::FormatVersion)
                    .eq(Expr::val(1_i64))))
            .into_tuple::<String>()
            .one(&txn)
            .await?;
            ensure!(valid.is_some(), "compaction owner scope or format mismatch");
            if let Some(turn) = execution_turn {
                ensure!(compaction_context::Entity::find()
                    .select_only()
                    .join(JoinType::InnerJoin, compaction_context::Entity::belongs_to(turn::Entity)
                        .from(compaction_context::Column::ThreadId)
                        .to(turn::Column::ThreadId)
                        .into())
                    .expr(Expr::col((compaction_context::Entity, compaction_context::Column::Owner)))
                    .filter(Expr::col((compaction_context::Entity, compaction_context::Column::Owner))
                        .eq(Expr::Value(snapshot.owner.clone()
                                .into()))
                        .and(Expr::col((turn::Entity, turn::Column::Id))
                            .eq(Expr::Value(turn.into())))
                        .and(Expr::exists(Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(compaction_execution_stop::Entity, "stop")
                                .and_where(Expr::col(("stop", compaction_execution_stop::Column::Owner))
                                    .eq(Expr::col((compaction_context::Entity, compaction_context::Column::Owner)))
                                    .and(Expr::col(("stop", compaction_execution_stop::Column::TurnId))
                                        .eq(Expr::col((turn::Entity, turn::Column::Id)))))
                                .to_owned())
                            .not())
                        .and(Expr::col((turn::Entity, turn::Column::Status))
                            .is_in(["interrupted", "cancelled"])
                            .not()))
                    .into_tuple::<String>()
                    .one(&txn)
                    .await?.is_some(), "compaction execution was stopped or changed scope");
            }
            // Epochs were read before the source projection was resolved.
            // Revalidate their workspace and revision inside admission; parsing
            // the bounded metadata here is part of this atomic DB transition.
            // SeaORM's Entity projection cannot expand a bound JSON object with
            // json_each and compare each epoch in a single bounded probe.
            let changed = txn.query_one_raw(Statement::from_sql_and_values(DbBackend::Postgres, "SELECT wanted.key FROM json_each($1::text::json->'source_epochs') wanted WHERE NOT EXISTS (SELECT 1 FROM thread t LEFT JOIN compaction_projection_epoch e ON e.thread_id=t.id WHERE t.id=wanted.key AND t.workspace_id=$2 AND COALESCE(e.version,0)=wanted.value::text::bigint) LIMIT 1", [snapshot_json.clone().into(), workspace.into()])).await?;
            ensure!(changed.is_none(), "source scope or epoch changed before admission");
            compaction_operation::Entity::insert(compaction_operation::ActiveModel {id: sea_orm::Set((snapshot.id.clone())
                        .to_owned()), owner: sea_orm::Set((snapshot.owner.clone())
                        .to_owned()), fingerprint: sea_orm::Set((snapshot.plan.fingerprint.clone())
                        .to_owned()), status: sea_orm::Set(("running")
                        .to_owned()), snapshot: sea_orm::Set((snapshot_json.clone())
                        .to_owned()), deadline_ms: sea_orm::Set(deadline), expected_head: sea_orm::Set(snapshot.expected_checkpoint.clone()), execution_turn: sea_orm::Set(execution_turn.map(str::to_owned)), ..Default::default() })
            .on_conflict(OnConflict::columns([compaction_operation::Column::Owner, compaction_operation::Column::Fingerprint])
                .do_nothing()
                .to_owned())
            .exec_without_returning(&txn)
            .await?;
            txn.commit().await?;
            Ok(())
        }).await?;
    compaction_operation::Entity::find()
        .filter(compaction_operation::Column::Owner.eq(snapshot.owner.clone()))
        .filter(compaction_operation::Column::Fingerprint.eq(snapshot.plan.fingerprint.clone()))
        .into_model::<OperationRecord>()
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("admitted operation missing"))
}

pub async fn compaction_operation_for_plan<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    owner: &str,
    fingerprint: &str,
) -> Result<Option<OperationRecord>> {
    use sea_orm::ColumnTrait;
    Ok(compaction_operation::Entity::find()
        .inner_join(compaction_context::Entity)
        .filter(compaction_context::Column::WorkspaceId.eq(workspace))
        .filter(compaction_context::Column::ThreadId.eq(thread))
        .filter(compaction_operation::Column::Owner.eq(owner))
        .filter(compaction_operation::Column::Fingerprint.eq(fingerprint))
        .one(db)
        .await?
        .map(OperationRecord::from))
}

/// One bounded diagnostic lookup for progress on the current context head.
/// Historical failures cannot obstruct new plans after the head advances.
/// Never supplies a cursor to a different plan.
pub async fn compaction_invalid_completion_progress<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    owner: &str,
) -> Result<Option<OperationRecord>> {
    Ok(compaction_operation::Entity::find()
        .inner_join(compaction_context::Entity)
        .filter(compaction_context::Column::WorkspaceId.eq(workspace))
        .filter(compaction_context::Column::ThreadId.eq(thread))
        .filter(compaction_operation::Column::Owner.eq(owner))
        .filter(compaction_operation::Column::Status.eq("failed"))
        .filter(Expr::cust_with_exprs(
            "$1 IS NOT DISTINCT FROM $2",
            [
                Expr::col((
                    compaction_operation::Entity,
                    compaction_operation::Column::ExpectedHead,
                )),
                Expr::col((compaction_context::Entity, compaction_context::Column::Head)),
            ],
        ))
        .filter(Expr::cust(
            r#"EXISTS (SELECT 1 FROM compaction_runner_state s
            WHERE s.operation_id=compaction_operation.id
            AND (s.state->'phase'->'Failed'->>'kind')='InvalidCompletion'
            AND ((s.state->'cursor'->>'unit')::bigint>0
                OR (s.state->'cursor'->>'source')::bigint>0
                OR (s.state->'cursor'->>'character')::bigint>0))"#,
        ))
        .order_by_asc(compaction_operation::Column::Id)
        .limit(1)
        .one(db)
        .await?
        .map(OperationRecord::from))
}

pub async fn compaction_operation<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<OperationRecord>> {
    use pioneer_entity::compaction_operation as operation;
    use sea_orm::EntityTrait;
    Ok(operation::Entity::find_by_id(id)
        .one(db)
        .await?
        .map(OperationRecord::from))
}

pub async fn compaction_head<C: ConnectionTrait>(db: &C, owner: &str) -> Result<Option<String>> {
    let row = compaction_context::Entity::find_by_id(owner)
        .one(db)
        .await?;
    match row {
        None => Ok(None),
        Some(row) => {
            ensure!(
                row.format_version == i64::from(FORMAT_VERSION),
                "unsupported working-context version"
            );
            Ok(row.head)
        }
    }
}

/// This is the only admission of a provider attempt. Counters survive restart.
pub async fn compaction_claim_attempt<C: ConnectionTrait>(
    db: &C,
    id: &str,
    expected_attempts: i64,
    now_ms: i64,
    transient_retry: bool,
    correction: bool,
) -> Result<bool> {
    let result = compaction_operation::Entity::update_many()
        .col_expr(
            compaction_operation::Column::Attempts,
            Expr::col(compaction_operation::Column::Attempts).add(Expr::val(1_i64)),
        )
        .col_expr(
            compaction_operation::Column::TransientRetries,
            Expr::col(compaction_operation::Column::TransientRetries)
                .add(Expr::Value((transient_retry as i64).into())),
        )
        .col_expr(
            compaction_operation::Column::Correction,
            Expr::col(compaction_operation::Column::Correction)
                .add(Expr::Value((correction as i64).into())),
        )
        .filter(
            Expr::col(compaction_operation::Column::Id)
                .eq(Expr::Value(id.into()))
                .and(Expr::col(compaction_operation::Column::Status).eq(Expr::val("running")))
                .and(
                    Expr::col(compaction_operation::Column::Attempts)
                        .eq(Expr::Value(expected_attempts.into())),
                )
                .and(
                    Expr::col(compaction_operation::Column::DeadlineMs)
                        .gt(Expr::Value(now_ms.into())),
                )
                .and(
                    Expr::col(compaction_operation::Column::TransientRetries)
                        .add(Expr::Value((transient_retry as i64).into()))
                        .lte(Expr::val(2_i64)),
                )
                .and(
                    Expr::col(compaction_operation::Column::Correction)
                        .add(Expr::Value((correction as i64).into()))
                        .lte(Expr::val(1_i64)),
                ),
        )
        .exec(db)
        .await?;
    Ok(result.rows_affected == 1)
}

pub async fn compaction_finish<C: ConnectionTrait>(
    db: &C,
    id: &str,
    status: &str,
    outcome: &str,
) -> Result<bool> {
    ensure!(
        ["failed", "cancelled", "stale"].contains(&status),
        "invalid terminal transition"
    );
    ensure!(
        outcome.len() <= 128,
        "outcome must be a bounded classification"
    );
    let changed = compaction_operation::Entity::update_many()
        .col_expr(
            compaction_operation::Column::Status,
            Expr::Value(status.into()),
        )
        .col_expr(
            compaction_operation::Column::Outcome,
            Expr::Value(outcome.into()),
        )
        .filter(
            Expr::col(compaction_operation::Column::Id)
                .eq(Expr::Value(id.into()))
                .and(Expr::col(compaction_operation::Column::Status).eq(Expr::val("running"))),
        )
        .exec(db)
        .await?;
    Ok(changed.rows_affected > 0)
}
