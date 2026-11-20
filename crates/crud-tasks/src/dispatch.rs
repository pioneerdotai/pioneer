use anyhow::Result;
use pioneer_entity::{task, task_run};
use sea_orm::{
    ColumnTrait, Condition, DatabaseTransaction, EntityTrait, FromQueryResult, PaginatorTrait,
    QueryFilter, QuerySelect, entity::prelude::DateTimeWithTimeZone,
};

#[derive(FromQueryResult)]
pub struct DispatchTask {
    pub status: String,
    pub concurrency_policy_json: Option<serde_json::Value>,
}

/// Serialize dispatch for all runs of one task. This also orders admission
/// against task policy updates and the finish fence.
pub async fn lock_task_for_dispatch(
    db: &DatabaseTransaction,
    task_id: &str,
) -> Result<Option<DispatchTask>> {
    super::event_log::lock_task(db, task_id).await?;
    Ok(task::Entity::find_by_id(task_id)
        .select_only()
        .columns([task::Column::Status, task::Column::ConcurrencyPolicyJson])
        .lock_exclusive()
        .into_model::<DispatchTask>()
        .one(db)
        .await?)
}

/// The writer transaction owns this check and the subsequent Queued→Starting
/// claim. Queued siblings reserve order, rather than occupying active slots.
pub async fn has_dispatch_capacity(
    db: &DatabaseTransaction,
    run: &task_run::Model,
    max_parallel_runs: u32,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let occupied = task_run::Entity::find()
        .filter(task_run::Column::TaskId.eq(run.task_id.clone()))
        .filter(task_run::Column::Status.is_in([
            "starting",
            "running",
            "waiting",
            "waiting_review",
        ]))
        .count(db)
        .await?;
    let earlier_ready = task_run::Entity::find()
        .filter(task_run::Column::TaskId.eq(run.task_id.clone()))
        .filter(task_run::Column::Status.eq("queued"))
        .filter(task_run::Column::RunNumber.lt(run.run_number))
        .filter(
            Condition::any()
                .add(task_run::Column::ReadyAt.is_null())
                .add(task_run::Column::ReadyAt.lte(now)),
        )
        .count(db)
        .await?;
    Ok(occupied.saturating_add(earlier_ready) < u64::from(std::cmp::max(max_parallel_runs, 1)))
}
