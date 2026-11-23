use anyhow::{Context, Result};
use pioneer_compaction::SourceRef;
use pioneer_crud_payloads::storage::{self, CapturedPayload, Selection};
use pioneer_entity::{
    compaction_task_basis_revision as revision, task_run_conversation_snapshot as snapshot, thread,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, JoinType, QueryFilter, QuerySelect,
    sea_query::{Expr, ExprTrait, Func},
};

const PAGE_CHARACTERS: usize = 16_384;

pub struct TaskBasisPayload {
    pub body: CapturedPayload,
    pub next_character: Option<u64>,
}

/// Capture a Task basis value under the caller's read snapshot. A frozen
/// manifest is a reference of its own kind and has no portable basis body.
pub async fn capture<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread_id: &str,
    reference: &SourceRef,
    from_character: Option<u64>,
) -> Result<Option<TaskBasisPayload>> {
    use snapshot::Column as Col;
    if reference.scope != format!("task-basis:{}", reference.id) {
        return Ok(None);
    }
    let Some(expected) = reference
        .version
        .strip_prefix("task-basis-revision:")
        .and_then(|value| value.parse::<i64>().ok())
    else {
        return Ok(None);
    };
    if reference.version != format!("task-basis-revision:{expected}") {
        return Ok(None);
    }
    let Some(payload_id) = snapshot::Entity::find()
        .select_only()
        .column(Col::HistoryJsonPayloadId)
        .join(
            JoinType::InnerJoin,
            snapshot::Entity::belongs_to(thread::Entity)
                .from(Col::ConversationThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .join(
            JoinType::LeftJoin,
            snapshot::Entity::belongs_to(revision::Entity)
                .from(Col::RunId)
                .to(revision::Column::RunId)
                .into(),
        )
        .filter(Col::WorkspaceId.eq(workspace))
        .filter(thread::Column::WorkspaceId.eq(workspace))
        .filter(Col::ConversationThreadId.eq(thread_id))
        .filter(Col::RunId.eq(&reference.id))
        .filter(Col::HistoryJsonHasArrayPrefix.eq(true))
        .filter(
            Expr::from(Func::coalesce([
                Expr::col((revision::Entity, revision::Column::Revision)),
                Expr::val(1_i64),
            ]))
            .eq(expected),
        )
        .into_tuple::<String>()
        .one(db)
        .await?
    else {
        return Ok(None);
    };

    let (selection, next_character) = if let Some(start) = from_character {
        let metadata = storage::metadata(db, &payload_id)
            .await?
            .context("missing Task basis payload")?;
        let start = usize::try_from(start)?;
        let end = start
            .saturating_add(PAGE_CHARACTERS)
            .min(metadata.original_chars);
        (
            Selection::Characters(start..end),
            (end < metadata.original_chars).then_some(u64::try_from(end)?),
        )
    } else {
        (Selection::Full, None)
    };
    let body = storage::capture(db, &payload_id, selection)
        .await?
        .context("missing Task basis payload")?;
    Ok(Some(TaskBasisPayload {
        body,
        next_character,
    }))
}
