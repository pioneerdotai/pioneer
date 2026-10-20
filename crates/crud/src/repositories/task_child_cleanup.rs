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
                            AND r.trigger_id IS NOT NULL AND origin_run.trigger_id IS r.trigger_id
                            AND contract.trigger_id IS r.trigger_id
                            AND contract.occurrence_key = 'immediate:' || origin_run.id
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
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                r#"SELECT CASE WHEN {READY} THEN 1 ELSE 0 END AS ready
            FROM task_run_turn trt JOIN task_run r ON r.id = trt.run_id
            JOIN "turn" t ON t.id = trt.turn_id AND t.thread_id = trt.thread_id
            WHERE t.thread_id = ? AND t.id = ?
              AND r.status IN ('succeeded', 'failed', 'cancelled', 'timed_out')"#
            ),
            [thread_id.into(), turn_id.into()],
        ))
        .await
        .context("failed to inspect terminal child cleanup receipt")?;
    row.map(|row| {
        row.try_get::<i64>("", "ready")
            .map(|n| n != 0)
            .map_err(Into::into)
    })
    .transpose()
}
