use anyhow::Result;
use pioneer_crud_conversations::context_cleanup::{
    MODEL_HISTORY_CONTEXT_SOURCES, delete_shell_controls_for_recovery_jobs,
    find_existing_tool_result, shell_child_turns_for_recovery_jobs,
};
use pioneer_crud_conversations::turn_context;
use pioneer_crud_conversations::turn_context::{
    NewTurnLlmContextEntry, TurnLlmContextAppendOutcome,
};
use pioneer_crud_tasks::task_child_producers;
use pioneer_entity::turn_llm_context;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

pub async fn insert_turn_llm_context(
    db: &DatabaseTransaction,
    entry: NewTurnLlmContextEntry,
) -> Result<turn_llm_context::Model> {
    let child_turn = entry.turn_id.clone();
    turn_context::lock_context_stream(db, &child_turn).await?;
    let creates_capsule = task_child_producers::context_will_create(
        db,
        &child_turn,
        &entry.source,
        &MODEL_HISTORY_CONTEXT_SOURCES,
    )
    .await?;
    let row = turn_context::insert(db, entry).await?;
    if creates_capsule {
        task_child_producers::capsule_birth(db, &child_turn).await?;
    }
    Ok(row)
}

pub async fn append_turn_llm_context(
    db: &DatabaseTransaction,
    entry: NewTurnLlmContextEntry,
    delivery_key: &str,
) -> Result<TurnLlmContextAppendOutcome> {
    let child_turn = entry.turn_id.clone();
    turn_context::lock_context_stream(db, &child_turn).await?;
    let creates_capsule = task_child_producers::context_will_create(
        db,
        &child_turn,
        &entry.source,
        &MODEL_HISTORY_CONTEXT_SOURCES,
    )
    .await?;
    let outcome = turn_context::append(db, entry, delivery_key).await?;
    if creates_capsule && !outcome.already_committed {
        task_child_producers::capsule_birth(db, &child_turn).await?;
    }
    Ok(outcome)
}

/// Return an already committed canonical result instead of replacing its bytes.
/// The caller validates the terminal source revision in this same transaction.
pub(crate) async fn append_recovered_shell_result(
    db: &DatabaseTransaction,
    entry: NewTurnLlmContextEntry,
    delivery_key: &str,
) -> Result<TurnLlmContextAppendOutcome> {
    anyhow::ensure!(
        entry.source == "tool_result_v2" && entry.item_id.is_some(),
        "invalid recovered shell result"
    );
    turn_context::lock_context_stream(db, &entry.turn_id).await?;
    if let Some(existing) =
        find_existing_tool_result(db, &entry.turn_id, entry.item_id.as_deref()).await?
    {
        return Ok(TurnLlmContextAppendOutcome {
            entry: existing,
            already_committed: true,
        });
    }
    append_turn_llm_context(db, entry, delivery_key).await
}

/// Preserve task-child publication in the recovery settlement transaction.
pub(crate) async fn settle_shell_controls_for_recovery_jobs<C: ConnectionTrait>(
    db: &C,
    job_ids: &[String],
) -> Result<u64> {
    for turn_id in shell_child_turns_for_recovery_jobs(db, job_ids).await? {
        task_child_producers::capsule_birth(db, &turn_id).await?;
    }
    delete_shell_controls_for_recovery_jobs(db, job_ids).await
}
