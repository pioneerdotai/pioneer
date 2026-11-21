//! Exact revision predicate shared by admission and frozen capture.
use sea_orm::{DbBackend, Statement};

const COMPACTION_SOURCES_CURRENT_SQL: &str = r#"
SELECT COUNT(*) AS matched
FROM json_array_elements($1::text::json) AS wanted(value)
WHERE
  EXISTS (
    SELECT 1
    FROM LATERAL (
      SELECT source_id, turn_id, revision, present
      FROM compaction_source_revision context_revision
      WHERE context_revision.source_id=(wanted.value->>'id')
      LIMIT 1
    ) current_context_revision
    JOIN LATERAL (
      SELECT id, turn_id FROM turn_llm_context context_source
      WHERE context_source.id=current_context_revision.source_id
      LIMIT 1
    ) current_context_source ON current_context_source.turn_id=current_context_revision.turn_id
    JOIN turn context_turn ON context_turn.id=current_context_revision.turn_id
    JOIN thread context_thread ON context_thread.id=context_turn.thread_id
    WHERE current_context_revision.present=1
      AND context_thread.workspace_id=$2
      AND context_turn.thread_id=$3
      AND 'context:'||current_context_revision.turn_id=(wanted.value->>'scope')
      AND 'revision:'||current_context_revision.revision=(wanted.value->>'version')
  )
  OR EXISTS (
    SELECT 1
    FROM LATERAL (
      SELECT source_id, turn_id, revision, present
      FROM compaction_item_revision item_revision
      WHERE item_revision.source_id=(wanted.value->>'id')
      LIMIT 1
    ) current_item_revision
    JOIN LATERAL (
      SELECT id, turn_id FROM turn_item item_source
      WHERE item_source.id=current_item_revision.source_id
      LIMIT 1
    ) current_item_source ON current_item_source.turn_id=current_item_revision.turn_id
    JOIN turn item_turn ON item_turn.id=current_item_revision.turn_id
    JOIN thread item_thread ON item_thread.id=item_turn.thread_id
    WHERE current_item_revision.present=1
      AND item_thread.workspace_id=$2
      AND item_turn.thread_id=$3
      AND 'item:'||current_item_revision.turn_id=(wanted.value->>'scope')
      AND 'item-revision:'||current_item_revision.revision=(wanted.value->>'version')
  )
  OR EXISTS (
    SELECT 1
    FROM LATERAL (
      SELECT source_id, turn_id, revision, present
      FROM compaction_event_revision event_revision
      WHERE event_revision.source_id=(wanted.value->>'id')
      LIMIT 1
    ) current_event_revision
    JOIN LATERAL (
      SELECT id, turn_id, event_type, sequence FROM turn_event event_source
      WHERE event_source.id=current_event_revision.source_id
      LIMIT 1
    ) current_event_source ON current_event_source.turn_id=current_event_revision.turn_id
    JOIN turn event_turn ON event_turn.id=current_event_revision.turn_id
    JOIN thread event_thread ON event_thread.id=event_turn.thread_id
    WHERE current_event_revision.present=1
      AND event_thread.workspace_id=$2
      AND event_turn.thread_id=$3
      AND 'event:'||current_event_revision.turn_id=(wanted.value->>'scope')
      AND 'event-revision:'||current_event_revision.revision=(wanted.value->>'version')
      AND (
        current_event_source.event_type NOT IN ('turn/started', 'turn/message/edited')
        OR NOT EXISTS (
          SELECT 1 FROM turn_event later_input
          WHERE later_input.turn_id=current_event_source.turn_id
            AND later_input.sequence>current_event_source.sequence
            AND later_input.event_type IN ('turn/message/edited', 'turn/message/deleted')
        )
      )
  )
  OR EXISTS (
    SELECT 1
    FROM LATERAL (
      SELECT source_id, turn_id, revision, present
      FROM compaction_input_revision input_revision
      WHERE input_revision.source_id=(wanted.value->>'id')
      LIMIT 1
    ) current_input_revision
    JOIN LATERAL (
      SELECT id, turn_id FROM turn_input input_source
      WHERE input_source.id=current_input_revision.source_id
      LIMIT 1
    ) current_input_source ON current_input_source.turn_id=current_input_revision.turn_id
    JOIN turn input_turn ON input_turn.id=current_input_revision.turn_id
    JOIN thread input_thread ON input_thread.id=input_turn.thread_id
    WHERE current_input_revision.present=1
      AND input_thread.workspace_id=$2
      AND input_turn.thread_id=$3
      AND 'input:'||current_input_revision.turn_id=(wanted.value->>'scope')
      AND 'input-revision:'||current_input_revision.revision=(wanted.value->>'version')
      AND input_turn.message_revision=0
      AND input_turn.message_deleted_at IS NULL
  )
  OR EXISTS (
    SELECT 1
    FROM LATERAL (
      SELECT run_id, workspace_id, conversation_thread_id, history_json_has_array_prefix
      FROM task_run_conversation_snapshot basis
      WHERE basis.run_id=(wanted.value->>'id')
      LIMIT 1
    ) current_basis
    JOIN thread basis_thread
      ON basis_thread.id=current_basis.conversation_thread_id
     AND basis_thread.workspace_id=current_basis.workspace_id
    LEFT JOIN compaction_task_basis_revision basis_revision
      ON basis_revision.run_id=current_basis.run_id
    WHERE current_basis.history_json_has_array_prefix
      AND current_basis.workspace_id=$2
      AND current_basis.conversation_thread_id=$3
      AND 'task-basis:'||current_basis.run_id=(wanted.value->>'scope')
      AND 'task-basis-revision:'||COALESCE(basis_revision.revision,1)=(wanted.value->>'version')
  )
  OR EXISTS (
    SELECT 1
    FROM LATERAL (
      SELECT id, owner, operation_id, status, format_version, identity_sha256
      FROM compaction_checkpoint published_checkpoint
      WHERE published_checkpoint.id=(wanted.value->>'id')
      LIMIT 1
    ) current_published_checkpoint
    JOIN compaction_context published_context ON published_context.owner=current_published_checkpoint.owner
    WHERE published_context.workspace_id=$2
      AND published_context.thread_id=$3
      AND 'checkpoint:'||current_published_checkpoint.owner=(wanted.value->>'scope')
      AND current_published_checkpoint.identity_sha256=(wanted.value->>'version')
      AND current_published_checkpoint.format_version=1
      AND (
        current_published_checkpoint.status='applied'
        OR (
          current_published_checkpoint.status='retained'
          AND EXISTS (
            SELECT 1
            FROM compaction_operation published_operation
            WHERE published_operation.id=current_published_checkpoint.operation_id
              AND published_operation.status='completed'
          )
        )
      )
  )
"#;

pub fn compaction_sources_current_statement(
    payload: String,
    workspace: &str,
    thread: &str,
) -> Statement {
    Statement::from_sql_and_values(
        DbBackend::Postgres,
        COMPACTION_SOURCES_CURRENT_SQL,
        [payload.into(), workspace.into(), thread.into()],
    )
}
