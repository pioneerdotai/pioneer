//! Receipt-gated continuation of the existing terminal-child owner.
use anyhow::{Context, Result};
use sea_orm::{ConnectionTrait, Statement};

// Match the persisted launch topology, not just the current (editable) policy.
// TaskRun occurrences require a healthy terminal receipt. Immediate attached
// launches retain their creator's conversation Turn and have no such receipt:
// the closed child + terminal aggregate end their recovery obligation.
// Both modes require positive, consistent lineage; absence is never permission.
pub(crate) const READY: &str = r#"
    trt.status <> 'blocked' AND trt.task_id = r.task_id
    -- New control-only receipts keep a child capsule while its undelivered
    -- recovery is owned. All existing receipt/lineage/candidate gates remain.
    AND NOT EXISTS (
        SELECT 1 FROM turn_llm_context shell_control
        WHERE shell_control.turn_id = t.id AND shell_control.source = 'shell_control_v1'
            AND NOT EXISTS (
                SELECT 1 FROM turn_llm_context shell_canonical
                WHERE shell_canonical.turn_id = shell_control.turn_id
                    AND shell_canonical.item_id = shell_control.item_id
                    AND shell_canonical.source = 'tool_result_v2'
            )
            AND (
                EXISTS (SELECT 1 FROM recovery_job shell_recovery
                    WHERE shell_recovery.turn_id = t.id
                        AND (shell_recovery.status IN ('pending', 'active')
                            -- READY already requires a definitively closed Turn.
                            -- Ignore settled blocked audit jobs only with the
                            -- same positive execution proof as control deletion.
                            OR (shell_recovery.status = 'blocked' AND NOT EXISTS (
                                SELECT 1 FROM turn_execution shell_execution
                                WHERE shell_execution.turn_id = t.id
                                    AND shell_execution.status IN ('completed', 'failed', 'interrupted', 'blocked')
                                    AND shell_execution.completed_at IS NOT NULL
                            ))
                            OR shell_recovery.resolution_pending))
                OR EXISTS (SELECT 1 FROM recovery_terminalization_outbox shell_outbox
                    WHERE shell_outbox.turn_id = t.id AND shell_outbox.status NOT IN ('delivered', 'cancelled'))
            )
    )
    AND t.turn_kind = 'conversation'
    AND t.status IN ('completed', 'failed', 'interrupted')
    AND EXISTS (
        SELECT 1 FROM thread_lineage l
        JOIN task owner_task ON owner_task.id = r.task_id
        JOIN "turn" parent ON parent.id = l.created_by_turn_id
            AND parent.thread_id = COALESCE(l.created_by_thread_id, l.parent_thread_id)
        JOIN thread parent_thread ON parent_thread.id = parent.thread_id
            AND parent_thread.workspace_id = owner_task.workspace_id
        JOIN thread child_thread ON child_thread.id = t.thread_id
            AND child_thread.workspace_id = owner_task.workspace_id
        WHERE l.child_thread_id = t.thread_id AND l.origin_kind = 'task_run'
            AND LENGTH(TRIM(l.child_thread_id)) > 0
            AND LENGTH(TRIM(l.parent_thread_id)) > 0
            AND LENGTH(TRIM(l.root_thread_id)) > 0
            AND l.parent_thread_id <> l.child_thread_id
            AND l.root_thread_id <> l.child_thread_id
            AND (
                (parent.turn_kind = 'task_run' AND parent.id = r.id
                    AND parent.status IN ('completed', 'failed', 'interrupted')
                    AND EXISTS (
                        SELECT 1 FROM turn_event_projection_stream_state s
                        WHERE s.turn_id = parent.id AND s.thread_id = parent.thread_id
                            AND s.status = 'healthy' AND s.accepted_terminal_sequence IS NOT NULL
                            AND s.projected_through_sequence >= s.accepted_terminal_sequence
                    ))
                OR (parent.turn_kind = 'conversation'
                    AND parent.id = owner_task.created_by_turn_id
                    AND parent.thread_id = owner_task.created_by_thread_id
                    AND parent.thread_id = l.parent_thread_id
                    AND parent.id <> r.id
                    -- Trigger spec and lifecycle policy can both change after
                    -- launch. The occurrence key pins the original immediate
                    -- generation, including retries in the same run group.
                    AND EXISTS (
                        SELECT 1 FROM task_run origin_run
                        JOIN task_occurrence_contract contract ON contract.occurrence_id = origin_run.id
                        JOIN task_run contract_run ON contract_run.id = contract.run_id
                        WHERE origin_run.run_group_id = r.run_group_id AND origin_run.attempt_number = 1
                            AND origin_run.task_id = r.task_id AND contract.task_id = r.task_id
                            AND r.trigger_id IS NOT NULL AND origin_run.trigger_id = r.trigger_id
                            AND contract.trigger_id = r.trigger_id
                            AND SUBSTR(contract.occurrence_key, 1, 10) = 'immediate:'
                            AND SUBSTR(contract.occurrence_key, 11) = origin_run.id
                            AND contract_run.run_group_id = r.run_group_id
                            AND contract_run.task_id = r.task_id
                    )
                    AND NOT EXISTS (SELECT 1 FROM "turn" occurrence WHERE occurrence.id = r.id)
                    -- Accepted before execution and immutable across policy
                    -- edits. This is a positive launch/source identity proof,
                    -- not permission inferred from a missing parent receipt.
                    AND EXISTS (
                        SELECT 1 FROM task_run_conversation_snapshot accepted_scope
                        WHERE accepted_scope.run_id = r.id AND accepted_scope.task_id = r.task_id
                            AND accepted_scope.workspace_id = owner_task.workspace_id
                            AND accepted_scope.conversation_thread_id = parent.thread_id
                            -- Composer may pin another source Turn in the same
                            -- conversation; it need not equal the creator Turn.
                            AND EXISTS (
                                SELECT 1 FROM "turn" accepted_source
                                WHERE accepted_source.id = accepted_scope.source_turn_id
                                    AND accepted_source.thread_id = parent.thread_id
                            )
                    ))
            )
    )
"#;

/// None: the aggregate is still owned by ordinary child reconciliation.
/// Some(false): terminal, but required receipt/launch lineage is not proven.
pub(crate) async fn ready<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<bool>> {
    let row = db
        .query_one_raw(ready_statement(
            db.get_database_backend(),
            thread_id,
            turn_id,
        ))
        .await
        .context("failed to inspect terminal child cleanup receipt")?;
    row.map(|row| {
        // PostgreSQL CASE integer literals are int4; SQLite stores signed int64.
        if db.get_database_backend() == sea_orm::DbBackend::Postgres {
            row.try_get::<i32>("", "ready")
                .map(|n| n != 0)
                .map_err(Into::into)
        } else {
            row.try_get::<i64>("", "ready")
                .map(|n| n != 0)
                .map_err(Into::into)
        }
    })
    .transpose()
}

/// Preserve the receipt proof across backend identifier quoting rules.
pub(crate) fn render_ready(backend: sea_orm::DbBackend) -> String {
    if backend == sea_orm::DbBackend::MySql {
        READY.replace("\"turn\"", "`turn`")
    } else {
        READY.to_owned()
    }
}

/// The destructive boundary must protect predicates as well as existing rows.
/// In particular, absence of run-id occurrence is part of immediate topology.
/// PostgreSQL SSI protects that predicate; MySQL/MariaDB transactional engines
/// take serializable next-key locks. Serialization/deadlock failure retries the
/// WHOLE transaction. SQLite already has the physically serialized writer.
pub(crate) async fn begin_authoritative<C>(db: &C) -> Result<C::Transaction>
where
    C: ConnectionTrait + sea_orm::TransactionTrait,
{
    if db.get_database_backend() == sea_orm::DbBackend::Sqlite {
        Ok(db.begin().await?)
    } else {
        Ok(db
            .begin_with_config(Some(sea_orm::IsolationLevel::Serializable), None)
            .await?)
    }
}

/// Lock positive domain rows, not queue rows, before checking READY. Missing
/// rows confer no authority. The final proof is reread under the same serializable
/// transaction, including every negative predicate. No producer lock convention
/// is assumed: ordinary domain UPDATE/DELETE conflicts with these row locks.
/// Producers write domain rows before obligations; cleanup follows that order.
/// Different domain writers can use a different row order, so deadlocks are a
/// retryable transaction outcome, never proof of empty work or permission.
pub(crate) async fn lock_authority<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<()> {
    use pioneer_entity::{
        task, task_occurrence_contract as contract, task_run,
        task_run_conversation_snapshot as snapshot, task_run_turn as link, thread,
        thread_lineage as lineage, turn, turn_event_projection_stream_state as receipt,
    };
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
    if db.get_database_backend() == sea_orm::DbBackend::Sqlite {
        return Ok(());
    }
    // Locking reads fetch headers only, never Turn prompts, Task policies,
    // accepted history or capsule payloads under writer capacity.
    let child = turn::Entity::find_by_id(turn_id)
        .filter(turn::Column::ThreadId.eq(thread_id))
        .select_only()
        .column(turn::Column::ThreadId)
        .lock_exclusive()
        .into_tuple::<String>()
        .one(db)
        .await?;
    let Some(child_thread) = child else {
        return Ok(());
    };
    let Some(run_id) = link::Entity::find()
        .filter(link::Column::TurnId.eq(turn_id))
        .filter(link::Column::ThreadId.eq(thread_id))
        .select_only()
        .column(link::Column::RunId)
        .lock_exclusive()
        .into_tuple::<String>()
        .one(db)
        .await?
    else {
        return Ok(());
    };
    let Some((task_id, group)) = task_run::Entity::find_by_id(&run_id)
        .select_only()
        .columns([task_run::Column::TaskId, task_run::Column::RunGroupId])
        .lock_exclusive()
        .into_tuple::<(String, String)>()
        .one(db)
        .await?
    else {
        return Ok(());
    };
    let Some((creator_thread, creator_turn)) = task::Entity::find_by_id(&task_id)
        .select_only()
        .columns([
            task::Column::CreatedByThreadId,
            task::Column::CreatedByTurnId,
        ])
        .lock_exclusive()
        .into_tuple::<(Option<String>, Option<String>)>()
        .one(db)
        .await?
    else {
        return Ok(());
    };
    let lineage = lineage::Entity::find_by_id(thread_id)
        .select_only()
        .columns([
            lineage::Column::ParentThreadId,
            lineage::Column::CreatedByThreadId,
            lineage::Column::CreatedByTurnId,
        ])
        .lock_exclusive()
        .into_tuple::<(String, Option<String>, Option<String>)>()
        .one(db)
        .await?;
    let accepted = snapshot::Entity::find_by_id(&run_id)
        .select_only()
        .columns([
            snapshot::Column::ConversationThreadId,
            snapshot::Column::SourceTurnId,
        ])
        .lock_exclusive()
        .into_tuple::<(String, Option<String>)>()
        .one(db)
        .await?;
    let origin = task_run::Entity::find()
        .filter(task_run::Column::TaskId.eq(task_id))
        .filter(task_run::Column::RunGroupId.eq(group))
        .filter(task_run::Column::AttemptNumber.eq(1))
        .select_only()
        .column(task_run::Column::Id)
        .lock_exclusive()
        .into_tuple::<String>()
        .one(db)
        .await?;
    let mut thread_ids = vec![child_thread];
    let mut turn_ids = vec![run_id]; // Explicit absent-occurrence predicate.
    if let Some(id) = creator_thread {
        thread_ids.push(id);
    }
    if let Some(id) = creator_turn {
        turn_ids.push(id);
    }
    if let Some((parent, creator_thread, creator_turn)) = lineage {
        thread_ids.push(parent);
        if let Some(id) = creator_thread {
            thread_ids.push(id);
        }
        if let Some(id) = creator_turn {
            turn_ids.push(id);
        }
    }
    if let Some((thread, source)) = accepted {
        thread_ids.push(thread);
        if let Some(id) = source {
            turn_ids.push(id);
        }
    }
    thread_ids.sort();
    thread_ids.dedup();
    turn_ids.sort();
    turn_ids.dedup();
    for id in turn_ids {
        // Exact absent PK lookup participates in SERIALIZABLE proof as well.
        turn::Entity::find_by_id(&id)
            .select_only()
            .column(turn::Column::Id)
            .lock_exclusive()
            .into_tuple::<String>()
            .one(db)
            .await?;
        receipt::Entity::find_by_id(&id)
            .select_only()
            .column(receipt::Column::TurnId)
            .lock_exclusive()
            .into_tuple::<String>()
            .one(db)
            .await?;
    }
    thread::Entity::find()
        .filter(thread::Column::Id.is_in(thread_ids))
        .select_only()
        .column(thread::Column::Id)
        .order_by_asc(thread::Column::Id)
        .lock_exclusive()
        .into_tuple::<String>()
        .all(db)
        .await?;
    if let Some(origin) = origin {
        if let Some(contract_run) = contract::Entity::find_by_id(origin)
            .select_only()
            .column(contract::Column::RunId)
            .lock_exclusive()
            .into_tuple::<String>()
            .one(db)
            .await?
        {
            task_run::Entity::find_by_id(contract_run)
                .select_only()
                .column(task_run::Column::Id)
                .lock_exclusive()
                .into_tuple::<String>()
                .one(db)
                .await?;
        }
    }
    Ok(())
}

/// One bound statement renderer for the read and authoritative commit paths.
pub(crate) fn ready_statement(
    backend: sea_orm::DbBackend,
    thread_id: &str,
    turn_id: &str,
) -> Statement {
    let ready = render_ready(backend);
    let turn_table = if backend == sea_orm::DbBackend::MySql {
        "`turn`"
    } else {
        "\"turn\""
    };
    let (thread_bind, turn_bind) = if backend == sea_orm::DbBackend::Postgres {
        ("$1", "$2")
    } else {
        ("?", "?")
    };
    Statement::from_sql_and_values(
        backend,
        format!(
            "SELECT CASE WHEN {ready} THEN 1 ELSE 0 END AS ready FROM task_run_turn trt JOIN task_run r ON r.id = trt.run_id JOIN {turn_table} t ON t.id = trt.turn_id AND t.thread_id = trt.thread_id WHERE t.thread_id = {thread_bind} AND t.id = {turn_bind} AND r.status IN ('succeeded','failed','cancelled','timed_out')"
        ),
        [thread_id.into(), turn_id.into()],
    )
}
