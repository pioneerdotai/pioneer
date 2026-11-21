//! Exact revision predicate shared by admission and frozen capture.
use anyhow::{Result, ensure};
use pioneer_compaction::SourceRef;
use pioneer_crud_conversations::context_cleanup::MODEL_HISTORY_CONTEXT_SOURCES;
use pioneer_entity::{
    compaction_checkpoint as checkpoint, compaction_context as context,
    compaction_operation as operation,
};
use sea_orm::sea_query::{BinOper, Expr, ExprTrait, JoinType, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, FromQueryResult, QueryFilter,
    QuerySelect, Select, Statement,
};

use crate::{SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS, live_sources};

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

// SeaORM cannot join a bound JSON array via json_array_elements and index each
// branch with a correlated LATERAL LIMIT in one Entity query.
pub(crate) fn compaction_sources_current_statement(
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

/// Projection metadata retained for the exact event revision that was last
/// decoded. This does not imply that the covered raw event is still present.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoricalEventProjection {
    pub reference: SourceRef,
    pub item_id: Option<String>,
    pub projection_kind: String,
}

#[derive(FromQueryResult)]
struct MatchedSourceCount {
    matched: i64,
}

/// Validate a bounded batch of directly consumed source identities. Canonical
/// rows and task bases are exact-current; a checkpoint is current when the
/// checkpoint object itself is published in the requested scope.
pub async fn compaction_sources_current<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    sources: &[SourceRef],
) -> Result<bool> {
    ensure!(
        sources.len() <= SOURCE_PAGE_ROWS as usize,
        "source validation batch exceeds row bound"
    );
    let payload = serde_json::to_string(sources)?;
    ensure!(
        payload.len() <= SOURCE_PAGE_BYTES,
        "source validation batch exceeds byte bound"
    );
    let row = MatchedSourceCount::find_by_statement(compaction_sources_current_statement(
        payload, workspace, thread,
    ))
    .one(db)
    .await?
    .ok_or_else(|| anyhow::anyhow!("source validation missing"))?;
    Ok(row.matched == sources.len() as i64)
}

/// Admit only cleanup-stable context identities to a newly published history.
/// The same scoped revision lookup used by source validation is required here;
/// a source name alone cannot authenticate a caller-supplied reference.
pub async fn compaction_model_context_sources_current<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    sources: &[SourceRef],
) -> Result<bool> {
    ensure!(
        sources.len() <= SOURCE_PAGE_ROWS as usize,
        "source validation batch exceeds row bound"
    );
    let payload = serde_json::to_string(sources)?;
    ensure!(
        payload.len() <= SOURCE_PAGE_BYTES,
        "source validation batch exceeds byte bound"
    );
    // SeaORM cannot expand a bound JSON array into a FROM relation for this
    // exact match count without one query per source.
    let row = MatchedSourceCount::find_by_statement(Statement::from_sql_and_values(DbBackend::Postgres,
        r#"SELECT COUNT(*) AS matched FROM json_array_elements($1::text::json) AS wanted(value) WHERE EXISTS (
            SELECT 1 FROM compaction_source_revision revision
            JOIN turn_llm_context context_source
              ON context_source.id=revision.source_id AND context_source.turn_id=revision.turn_id
            JOIN turn context_turn ON context_turn.id=revision.turn_id
            JOIN thread context_thread ON context_thread.id=context_turn.thread_id
            WHERE revision.present=1 AND context_thread.workspace_id=$2
              AND context_turn.thread_id=$3
              AND context_source.source IN ($4,$5,$6,$7,$8)
              AND 'context:'||revision.turn_id=(wanted.value->>'scope')
              AND revision.source_id=(wanted.value->>'id')
              AND 'revision:'||revision.revision=(wanted.value->>'version')
        )"#,
        [
            payload.into(),
            workspace.into(),
            thread.into(),
            MODEL_HISTORY_CONTEXT_SOURCES[0].into(),
            MODEL_HISTORY_CONTEXT_SOURCES[1].into(),
            MODEL_HISTORY_CONTEXT_SOURCES[2].into(),
            MODEL_HISTORY_CONTEXT_SOURCES[3].into(),
            MODEL_HISTORY_CONTEXT_SOURCES[4].into(),
        ],
    ))
    .one(db)
    .await?
    .ok_or_else(|| anyhow::anyhow!("context source validation missing"))?;
    Ok(row.matched == sources.len() as i64)
}

#[derive(FromQueryResult)]
struct HistoricalEventProjectionRow {
    source_id: String,
    turn_id: String,
    source_version: String,
    item_id: Option<String>,
    projection_kind: String,
}

/// Read only the durable relationship metadata for exact captured revisions.
/// `present` and the raw `turn_event` row are intentionally not consulted: a
/// published checkpoint remains authoritative after its covered payload is
/// edited or removed.
pub async fn compaction_historical_event_projections<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    sources: &[SourceRef],
) -> Result<Vec<HistoricalEventProjection>> {
    ensure!(
        sources.len() <= SOURCE_PAGE_ROWS as usize,
        "event projection metadata batch exceeds row bound"
    );
    ensure!(
        sources
            .iter()
            .all(|source| source.scope.starts_with("event:")),
        "event projection metadata received a non-event source"
    );
    let payload = serde_json::to_string(sources)?;
    ensure!(
        payload.len() <= SOURCE_PAGE_BYTES,
        "event projection metadata batch exceeds byte bound"
    );
    // SeaORM does not support a FROM relation from json_array_elements WITH
    // ORDINALITY for preserving the caller’s revision order.
    let rows = HistoricalEventProjectionRow::find_by_statement(Statement::from_sql_and_values(DbBackend::Postgres,
        "SELECT r.source_id,r.turn_id,'event-revision:'||r.projection_revision AS source_version,r.item_id,r.projection_kind \
         FROM json_array_elements($1::text::json) WITH ORDINALITY AS w(value, ordinal) \
         JOIN compaction_event_revision r \
           ON 'event:'||r.turn_id=(w.value->>'scope') \
          AND r.source_id=(w.value->>'id') \
          AND 'event-revision:'||r.projection_revision=(w.value->>'version') \
          AND r.projection_kind IS NOT NULL \
         JOIN turn t ON t.id=r.turn_id AND t.thread_id=$2 \
         JOIN thread h ON h.id=t.thread_id AND h.workspace_id=$3 \
         ORDER BY w.ordinal",
        [payload.into(), thread.into(), workspace.into()],
    ))
    .all(db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| HistoricalEventProjection {
            reference: SourceRef {
                scope: format!("event:{}", row.turn_id),
                id: row.source_id,
                version: row.source_version,
            },
            item_id: row.item_id,
            projection_kind: row.projection_kind,
        })
        .collect())
}

/// Resolve a directly consumed source's owning thread. Checkpoint ownership is
/// recorded on the published object; raw ownership remains an exact live-row
/// lookup. Historical checkpoint coverage uses its saved manifest ownership.
pub async fn compaction_reference_thread<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    source: &SourceRef,
) -> Result<Option<String>> {
    ensure!(
        source
            .scope
            .len()
            .saturating_add(source.id.len())
            .saturating_add(source.version.len())
            <= SOURCE_PAGE_BYTES,
        "source identity exceeds metadata quantum"
    );
    if source.scope.starts_with("checkpoint:") {
        return Ok(published_checkpoint_in_workspace(workspace, &source.id)
            .filter(checkpoint_scope_filter(&source.scope))
            .filter(checkpoint::Column::IdentitySha256.eq(&source.version))
            .column(context::Column::ThreadId)
            .into_tuple::<String>()
            .one(db)
            .await?);
    }
    // compaction_live_sources is a migration-owned SQL view without an Entity.
    Ok(live_sources::ThreadRow::find_by_statement(
        db.get_database_backend().build(
            &(sea_orm::sea_query::Query::select()
                .from(live_sources::Column::Table)
                .expr(Expr::col(live_sources::Column::ThreadId))
                .and_where(
                    Expr::col(live_sources::Column::WorkspaceId)
                        .eq(Expr::Value(workspace.into()))
                        .and(
                            Expr::col(live_sources::Column::SourceScope)
                                .eq(Expr::Value(source.scope.clone().into())),
                        )
                        .and(
                            Expr::col(live_sources::Column::SourceId)
                                .eq(Expr::Value(source.id.clone().into())),
                        )
                        .and(
                            Expr::col(live_sources::Column::SourceVersion)
                                .eq(Expr::Value(source.version.clone().into())),
                        ),
                )
                .limit(1)
                .to_owned()),
        ),
    )
    .one(db)
    .await?
    .map(|row| row.thread_id))
}

pub(crate) fn published_checkpoint_in_workspace(
    workspace: &str,
    id: &str,
) -> Select<checkpoint::Entity> {
    let completed = Query::select()
        .expr(Expr::val(1_i64))
        .from(operation::Entity)
        .and_where(
            Expr::col((operation::Entity, operation::Column::Id)).eq(Expr::col((
                checkpoint::Entity,
                checkpoint::Column::OperationId,
            ))),
        )
        .and_where(operation::Column::Status.eq("completed"))
        .to_owned();
    checkpoint::Entity::find_by_id(id)
        .select_only()
        .join(
            JoinType::InnerJoin,
            checkpoint::Entity::belongs_to(context::Entity)
                .from(checkpoint::Column::Owner)
                .to(context::Column::Owner)
                .into(),
        )
        .filter(context::Column::WorkspaceId.eq(workspace))
        .filter(checkpoint::Column::FormatVersion.eq(1_i64))
        .filter(
            checkpoint::Column::Status
                .eq("applied")
                .or(checkpoint::Column::Status
                    .eq("retained")
                    .and(Expr::exists(completed))),
        )
        .limit(1)
}

fn checkpoint_payload_query(
    workspace: &str,
    thread: &str,
    reference: &SourceRef,
) -> Select<checkpoint::Entity> {
    published_checkpoint_in_workspace(workspace, &reference.id)
        .filter(context::Column::ThreadId.eq(thread))
        .filter(checkpoint_scope_filter(&reference.scope))
        .filter(checkpoint::Column::IdentitySha256.eq(&reference.version))
        .column(checkpoint::Column::Summary)
}

pub async fn compaction_reference_checkpoint_payload<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    reference: &SourceRef,
) -> Result<Option<String>> {
    if !reference.scope.starts_with("checkpoint:") {
        return Ok(None);
    }
    Ok(checkpoint_payload_query(workspace, thread, reference)
        .into_tuple::<String>()
        .one(db)
        .await?)
}

pub(crate) fn checkpoint_scope_filter(scope: &str) -> Expr {
    // Compare the wire scope after looking up the exact checkpoint ID. A
    // plain owner equality can make PostgreSQL scan the owner's checkpoints.
    Expr::val("checkpoint:")
        .binary(
            BinOper::Custom("||"),
            Expr::col((checkpoint::Entity, checkpoint::Column::Owner)),
        )
        .eq(scope)
}

fn checkpoint_source_query(workspace: &str, thread: &str, id: &str) -> Select<checkpoint::Entity> {
    published_checkpoint_in_workspace(workspace, id)
        .filter(context::Column::ThreadId.eq(thread))
        .columns([
            checkpoint::Column::Owner,
            checkpoint::Column::Id,
            checkpoint::Column::IdentitySha256,
        ])
}

pub async fn compaction_checkpoint_source<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    checkpoint: &str,
) -> Result<Option<SourceRef>> {
    Ok(checkpoint_source_query(workspace, thread, checkpoint)
        .into_tuple::<(String, String, String)>()
        .one(db)
        .await?
        .map(|(owner, id, version)| SourceRef {
            scope: format!("checkpoint:{owner}"),
            id,
            version,
        }))
}

#[cfg(test)]
#[path = "source_revision_tests.rs"]
mod tests;
