//! Read-only SQL view projections. This view is not a generated SeaORM entity.
use sea_orm::sea_query::{ConditionType, IntoIden, IntoTableRef};
use sea_orm::{DeriveIden, FromQueryResult, RelationDef, RelationType};

#[derive(Clone, Copy, Debug, DeriveIden)]
pub(super) enum Column {
    #[sea_orm(iden = "compaction_live_sources")]
    Table,
    SourceScope,
    SourceId,
    SourceVersion,
    ThreadId,
    WorkspaceId,
}

#[derive(FromQueryResult)]
pub(super) struct SourceRow {
    pub source_scope: String,
    pub source_id: String,
    pub source_version: String,
}

#[derive(FromQueryResult)]
pub(super) struct ThreadRow {
    pub thread_id: String,
}

/// Point validation equivalent to the live-source view. A join against the
/// UNION ALL view can materialize every source before applying import keys.
/// Correlated EXISTS instead probes each canonical table by its primary key;
/// no payload is selected (including with transparent compression enabled).
/// Arguments are repository-owned SQL identifiers, never caller input.
pub(super) fn current_source_predicate(source: &str, workspace: &str) -> String {
    let mut alternatives = Vec::new();
    for (kind, revision, table, prefix) in [
        (
            "context",
            "compaction_source_revision",
            "turn_llm_context",
            "revision",
        ),
        (
            "item",
            "compaction_item_revision",
            "turn_item",
            "item-revision",
        ),
        (
            "event",
            "compaction_event_revision",
            "turn_event",
            "event-revision",
        ),
        (
            "input",
            "compaction_input_revision",
            "turn_input",
            "input-revision",
        ),
    ] {
        alternatives.push(format!(
            "EXISTS (SELECT 1 FROM {revision} r \
             JOIN {table} s ON s.id=r.source_id AND s.turn_id=r.turn_id \
             JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id \
             WHERE r.source_id={source}.source_id AND r.present=1 \
             AND {source}.source_scope='{kind}:'||r.turn_id \
             AND {source}.source_version='{prefix}:'||r.revision \
             AND t.thread_id={source}.source_thread AND th.workspace_id={workspace})"
        ));
    }
    alternatives.push(format!(
        "EXISTS (SELECT 1 FROM compaction_checkpoint p \
         JOIN compaction_context c ON c.owner=p.owner \
         LEFT JOIN compaction_projection_epoch e ON e.thread_id=c.thread_id \
         WHERE p.id={source}.source_id AND {source}.source_scope='checkpoint:'||p.owner \
         AND p.identity_sha256={source}.source_version \
         AND c.thread_id={source}.source_thread AND c.workspace_id={workspace} \
         AND (p.status='applied' OR (p.status='retained' AND EXISTS \
           (SELECT 1 FROM compaction_operation committed \
            WHERE committed.id=p.operation_id AND committed.status='completed'))) \
         AND p.projection_version=COALESCE(e.version,0))"
    ));
    alternatives.push(format!(
        "EXISTS (SELECT 1 FROM task_run_conversation_snapshot s \
         JOIN thread th ON th.id=s.conversation_thread_id AND th.workspace_id=s.workspace_id \
         LEFT JOIN compaction_task_basis_revision r ON r.run_id=s.run_id \
         WHERE s.run_id={source}.source_id AND {source}.source_scope='task-basis:'||s.run_id \
         AND {source}.source_version='task-basis-revision:'||COALESCE(r.revision,1) \
         AND s.conversation_thread_id={source}.source_thread AND s.workspace_id={workspace} \
         AND substr(ltrim(s.history_json),1,1)='[')"
    ));
    format!("({})", alternatives.join(" OR "))
}

pub(super) async fn references_current<C: sea_orm::ConnectionTrait>(
    db: &C,
    workspace: &str,
    references: &[(String, pioneer_compaction::SourceRef)],
) -> anyhow::Result<bool> {
    use super::compaction::{SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS, sqlite_specific_sql};
    anyhow::ensure!(
        references.len() as u64 <= SOURCE_PAGE_ROWS,
        "source validation row limit"
    );
    let bytes = references
        .iter()
        .map(|(thread, source)| {
            thread.len() + source.scope.len() + source.id.len() + source.version.len()
        })
        .sum::<usize>();
    anyhow::ensure!(bytes <= SOURCE_PAGE_BYTES, "source validation byte limit");
    if references.is_empty() {
        return Ok(true);
    }
    let mut values = Vec::<sea_orm::Value>::new();
    for (thread, source) in references {
        values.extend([
            thread.clone().into(),
            source.scope.clone().into(),
            source.id.clone().into(),
            source.version.clone().into(),
        ]);
    }
    values.push(workspace.into());
    let sql = format!(
        "WITH wanted(source_thread,source_scope,source_id,source_version) AS (VALUES {}), scope(workspace_id) AS (VALUES (?)) \
         SELECT COUNT(*) AS matched FROM wanted CROSS JOIN scope WHERE {}",
        vec!["(?,?,?,?)"; references.len()].join(","),
        current_source_predicate("wanted", "scope.workspace_id")
    );
    let row = db
        .query_one_raw(sqlite_specific_sql(&sql, values))
        .await?
        .ok_or_else(|| anyhow::anyhow!("source validation result missing"))?;
    Ok(row.try_get::<i64>("", "matched")? == i64::try_from(references.len())?)
}

/// Join an entity query to the view without declaring an entity or foreign key.
pub(super) fn join(
    table: impl IntoTableRef,
    column: impl IntoIden,
    view_column: Column,
) -> RelationDef {
    RelationDef {
        rel_type: RelationType::HasOne,
        from_tbl: table.into_table_ref(),
        to_tbl: Column::Table.into_table_ref(),
        from_col: sea_orm::Identity::Unary(column.into_iden()),
        to_col: sea_orm::Identity::Unary(view_column.into_iden()),
        is_owner: false,
        skip_fk: true,
        on_delete: None,
        on_update: None,
        on_condition: None,
        fk_name: None,
        condition_type: ConditionType::All,
    }
}
