//! Transaction-local replacements for the two scoped occurrence trigger groups.
//! Callers must pass the domain transaction. No commit, wake or lease change.
use anyhow::{Result, ensure};
use pioneer_entity::{
    agent_execution, task, task_occurrence_contract, task_run, task_run_execution, turn,
};
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

#[derive(Debug)]
pub struct OccurrencePublicationRace;

impl std::fmt::Display for OccurrencePublicationRace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Task occurrence publication changed during the transaction")
    }
}

impl std::error::Error for OccurrencePublicationRace {}

pub async fn next_generation<C: ConnectionTrait>(db: &C) -> Result<i64> {
    use pioneer_entity::task_occurrence_reconcile_sequence as sequence;
    let row = sequence::Entity::find_by_id(1)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("task occurrence sequence missing"))?;
    let generation = row
        .generation
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("task occurrence generation exhausted"))?;
    let updated = sequence::Entity::update_many()
        .col_expr(sequence::Column::Generation, Expr::val(generation))
        .filter(sequence::Column::Singleton.eq(1))
        .filter(sequence::Column::Generation.eq(row.generation))
        .exec(db)
        .await?;
    if updated.rows_affected != 1 {
        // Escape the owning transaction; its caller retries from a fresh snapshot.
        return Err(OccurrencePublicationRace.into());
    }
    Ok(generation)
}

async fn contract_ticket<C: ConnectionTrait>(db: &C, id: &str) -> Result<()> {
    use pioneer_entity::task_occurrence_reconcile_pending as p;
    let generation = next_generation(db).await?;
    p::Entity::insert(p::ActiveModel {
        run_id: Set(id.into()),
        generation: Set(generation),
        next_attempt_at: Set(chrono::Utc::now().timestamp()),
        attempt_count: Set(0),
        claim_token: Set(None),
    })
    .on_conflict(
        OnConflict::column(p::Column::RunId)
            .update_columns([p::Column::Generation, p::Column::ClaimToken])
            .to_owned(),
    )
    .exec_without_returning(db)
    .await?;
    Ok(())
}
async fn parent_ticket<C: ConnectionTrait>(db: &C, id: &str) -> Result<()> {
    use pioneer_entity::{
        task_run_occurrence_reconcile_pending as p, task_run_occurrence_reconcile_sequence as seq,
    };
    // Lock positive headers by PK, before evaluating the relationship. A
    // mismatch-filtered locking SELECT could exclude the row using an old RR
    // snapshot. MySQL current reads see the latest counterpart; PostgreSQL RR
    // rejects a changed visible header, unwinding the owning transaction.
    let run_status = task_run::Entity::find_by_id(id.to_owned())
        .select_only()
        .column(task_run::Column::Status)
        .lock_exclusive()
        .into_tuple::<String>()
        .one(db)
        .await?;
    let parent = turn::Entity::find_by_id(id.to_owned())
        .select_only()
        .columns([turn::Column::Status, turn::Column::TurnKind])
        .lock_exclusive()
        .into_tuple::<(String, String)>()
        .one(db)
        .await?;
    let mismatch = match (run_status.as_deref(), parent.as_ref()) {
        (Some(status), Some((parent_status, kind))) => {
            kind == "task_run"
                && match status {
                    "succeeded" => parent_status != "completed",
                    "failed" | "timed_out" => parent_status != "failed",
                    "blocked" => parent_status != "blocked",
                    "cancelled" => parent_status != "interrupted",
                    _ => false,
                }
        }
        // A terminal Run may have an invisible TaskRun Turn birth, or the
        // TaskRun Turn may have an invisible terminal Run birth under RR.
        // Positive nonterminal/non-TaskRun headers cannot be mismatches; their
        // own later eligible transition publishes while these locks exclude it.
        (Some(status), None) => matches!(
            status,
            "succeeded" | "failed" | "timed_out" | "blocked" | "cancelled"
        ),
        (None, Some((_, kind))) => kind == "task_run",
        (None, None) => false,
    };
    if !mismatch {
        p::Entity::delete_by_id(id.to_owned()).exec(db).await?;
        return Ok(());
    }
    let old = seq::Entity::find_by_id(1)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("task occurrence sequence missing"))?;
    let generation = old
        .generation
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("task occurrence generation exhausted"))?;
    ensure!(old.generation >= 0, "invalid task occurrence sequence");
    let changed = seq::Entity::update_many()
        .col_expr(seq::Column::Generation, Expr::val(generation))
        .filter(seq::Column::Singleton.eq(1))
        .filter(seq::Column::Generation.eq(old.generation))
        .exec(db)
        .await?;
    if changed.rows_affected != 1 {
        // Escape the owning transaction; its caller retries from a fresh snapshot.
        return Err(OccurrencePublicationRace.into());
    }
    p::Entity::insert(p::ActiveModel {
        run_id: Set(id.into()),
        generation: Set(generation),
        next_attempt_at: Set(chrono::Utc::now().timestamp()),
        attempt_count: Set(0),
        claim_token: Set(None),
    })
    .on_conflict(
        OnConflict::column(p::Column::RunId)
            .update_columns([p::Column::Generation, p::Column::ClaimToken])
            .to_owned(),
    )
    .exec_without_returning(db)
    .await?;
    Ok(())
}
async fn task_scope<C: ConnectionTrait>(db: &C, id: &str) -> Result<()> {
    use pioneer_entity::task_occurrence_reconcile_scope as s;
    let generation = next_generation(db).await?;
    let upper = task_run::Entity::find()
        .select_only()
        .column(task_run::Column::Id)
        .filter(task_run::Column::TaskId.eq(id))
        .order_by_desc(task_run::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?;
    s::Entity::insert(s::ActiveModel {
        task_id: Set(id.into()),
        generation: Set(generation),
        after_run_id: Set(None),
        upper_run_id: Set(upper),
    })
    .on_conflict(
        OnConflict::column(s::Column::TaskId)
            .update_columns([
                s::Column::Generation,
                s::Column::AfterRunId,
                s::Column::UpperRunId,
            ])
            .to_owned(),
    )
    .exec_without_returning(db)
    .await?;
    Ok(())
}

pub async fn task_change<C: ConnectionTrait>(
    db: &C,
    old: Option<&task::Model>,
    new: Option<&task::Model>,
) -> Result<()> {
    let facts = |r: &task::Model| {
        (
            r.id.clone(),
            r.workspace_id.clone(),
            r.executor_kind.clone(),
        )
    };
    if old.map(facts) == new.map(facts) {
        return Ok(());
    }
    if let Some(r) = old {
        if new.is_none_or(|n| n.id != r.id) {
            task_scope(db, &r.id).await?;
        }
    }
    if let Some(r) = new {
        task_scope(db, &r.id).await?;
    }
    Ok(())
}
pub async fn run_change<C: ConnectionTrait>(
    db: &C,
    old: Option<&task_run::Model>,
    new: Option<&task_run::Model>,
) -> Result<()> {
    let contract = |r: &task_run::Model| {
        (
            r.id.clone(),
            r.task_id.clone(),
            r.status.clone(),
            r.executor_kind.clone(),
            r.completed_at,
        )
    };
    let parent = |r: &task_run::Model| {
        (
            r.id.clone(),
            r.status.clone(),
            r.completed_at,
            r.error_json.clone(),
        )
    };
    let c = old.map(contract) != new.map(contract);
    let p = old.map(parent) != new.map(parent);
    if !c && !p {
        return Ok(());
    }
    if let Some(r) = old {
        if new.is_none_or(|n| n.id != r.id) {
            if c {
                contract_ticket(db, &r.id).await?;
            }
            if p {
                // The owning source removed this old PK; its absence is known,
                // not an invisible counterpart birth in an RR snapshot.
                pioneer_entity::task_run_occurrence_reconcile_pending::Entity::delete_by_id(
                    r.id.clone(),
                )
                .exec(db)
                .await?;
            }
        }
    }
    if let Some(r) = new {
        if c {
            contract_ticket(db, &r.id).await?;
        }
        if p {
            parent_ticket(db, &r.id).await?;
        }
    }
    Ok(())
}
pub async fn turn_change<C: ConnectionTrait>(
    db: &C,
    old: Option<&turn::Model>,
    new: Option<&turn::Model>,
) -> Result<()> {
    let facts = |r: &turn::Model| {
        (
            r.id.clone(),
            r.thread_id.clone(),
            r.status.clone(),
            r.turn_kind.clone(),
        )
    };
    if old.map(facts) == new.map(facts) {
        return Ok(());
    }
    if let Some(r) = old {
        if new.is_none_or(|n| n.id != r.id) {
            pioneer_entity::task_run_occurrence_reconcile_pending::Entity::delete_by_id(
                r.id.clone(),
            )
            .exec(db)
            .await?;
        }
    }
    if let Some(r) = new {
        parent_ticket(db, &r.id).await?;
    }
    Ok(())
}
pub async fn execution_change<C: ConnectionTrait>(
    db: &C,
    old: Option<&task_run_execution::Model>,
    new: Option<&task_run_execution::Model>,
) -> Result<()> {
    let facts = |r: &task_run_execution::Model| {
        (
            r.id.clone(),
            r.task_run_id.clone(),
            r.task_id.clone(),
            r.executor_kind.clone(),
            r.status.clone(),
            r.completed_at,
        )
    };
    if old.map(facts) == new.map(facts) {
        return Ok(());
    }
    if let Some(r) = old {
        if new.is_none_or(|n| n.task_run_id != r.task_run_id) {
            contract_ticket(db, &r.task_run_id).await?;
        }
    }
    if let Some(r) = new {
        contract_ticket(db, &r.task_run_id).await?;
    }
    Ok(())
}
pub async fn contract_change<C: ConnectionTrait>(
    db: &C,
    old: Option<&task_occurrence_contract::Model>,
    new: Option<&task_occurrence_contract::Model>,
) -> Result<()> {
    let facts = |r: &task_occurrence_contract::Model| {
        (
            r.occurrence_id.clone(),
            r.run_id.clone(),
            r.task_id.clone(),
            r.status.clone(),
            r.execution_generation,
            r.agent_execution_id.clone(),
            r.work_graph_root_execution_id.clone(),
            r.root_resource_scope_id.clone(),
        )
    };
    if old.map(facts) == new.map(facts) {
        return Ok(());
    }
    if let Some(r) = old {
        if new.is_none_or(|n| n.run_id != r.run_id) {
            contract_ticket(db, &r.run_id).await?;
        }
    }
    if let Some(r) = new {
        contract_ticket(db, &r.run_id).await?;
    }
    Ok(())
}
pub async fn agent_change<C: ConnectionTrait>(
    db: &C,
    old: Option<&agent_execution::Model>,
    new: Option<&agent_execution::Model>,
) -> Result<()> {
    let facts = |r: &agent_execution::Model| {
        (
            r.id.clone(),
            r.workspace_id.clone(),
            r.parent_task_id.clone(),
            r.execution_generation,
            r.status.clone(),
            r.finished_at,
            r.work_graph_root_execution_id.clone(),
        )
    };
    if old.map(facts) == new.map(facts) {
        return Ok(());
    }
    // Mapping births publish through contract_ticket in the same transaction.
    // Lock that existing publication sequence before consulting the mapping:
    // PostgreSQL RR must abort if a birth committed after our snapshot (even
    // when the new mapping is invisible); later births publish after this Tx.
    // This is a serialization barrier, not a generation/timestamp increment.
    pioneer_entity::task_occurrence_reconcile_sequence::Entity::find_by_id(1)
        .lock_exclusive()
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("task occurrence sequence missing"))?;
    for id in old.map(|r| &r.id).into_iter().chain(
        new.filter(|n| old.is_none_or(|o| o.id != n.id))
            .map(|r| &r.id),
    ) {
        if let Some(r) = task_run_execution::Entity::find_by_id(id.clone())
            .lock_exclusive()
            .one(db)
            .await?
        {
            contract_ticket(db, &r.task_run_id).await?;
        }
    }
    Ok(())
}
