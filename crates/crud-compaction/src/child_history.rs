//! Bounded authority lookup for accepted child conversation ancestry.
use anyhow::{Result, ensure};
use pioneer_entity::{
    task, task_agent_spec, task_run, task_run_conversation_snapshot, task_run_thread_binding,
    task_run_turn, thread, thread_lineage, turn,
};
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

/// Existing durable branch relationship; no Task input payload is read.
#[derive(Clone, Debug)]
pub struct ChildHistoryParent {
    pub parent_thread: String,
    pub root_thread: String,
    pub started_at: sea_orm::prelude::DateTimeWithTimeZone,
    pub source_turn: Option<String>,
    pub task_source_turn: Option<String>,
    pub task_metadata_json: Option<String>,
    pub context_policy_json: Option<String>,
}

pub async fn compaction_child_history_parent<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    child: &str,
) -> Result<Option<ChildHistoryParent>> {
    let Some(lineage) = thread_lineage::Entity::find_by_id(child).one(db).await? else {
        return Ok(None);
    };
    ensure!(
        lineage.parent_thread_id != child && lineage.root_thread_id != child && lineage.depth >= 0,
        "invalid child history lineage"
    );
    for id in [child, lineage.parent_thread_id.as_str()] {
        ensure!(
            thread::Entity::find_by_id(id)
                .filter(thread::Column::WorkspaceId.eq(workspace))
                .select_only()
                .column(thread::Column::Id)
                .into_tuple::<String>()
                .one(db)
                .await?
                .is_some(),
            "child history left its workspace"
        );
    }
    let binding = task_run_thread_binding::Entity::find()
        .filter(task_run_thread_binding::Column::ThreadId.eq(child))
        .one(db)
        .await?;
    // agent_action is capsule lineage, not an accepted Task conversation.
    // Do not silently ignore conflicting Task evidence or corrupt Task children.
    if lineage.origin_kind.as_deref() == Some("agent_action") {
        ensure!(
            binding.is_none()
                && thread::Entity::find_by_id(child)
                    .filter(thread::Column::OriginKind.eq("system"))
                    .select_only()
                    .column(thread::Column::Id)
                    .into_tuple::<String>()
                    .one(db)
                    .await?
                    .is_some()
                && task_run_turn::Entity::find()
                    .filter(task_run_turn::Column::ThreadId.eq(child))
                    .select_only()
                    .column(task_run_turn::Column::Id)
                    .into_tuple::<String>()
                    .one(db)
                    .await?
                    .is_none(),
            "internal lineage conflicts with Task child identity"
        );
        return Ok(None);
    }
    let mut query = task_run_turn::Entity::find().filter(task_run_turn::Column::ThreadId.eq(child));
    if let Some(binding) = &binding {
        query = query
            .filter(task_run_turn::Column::RunId.eq(&binding.run_id))
            .filter(task_run_turn::Column::TaskId.eq(&binding.task_id));
    }
    let first = query
        .order_by_asc(task_run_turn::Column::Sequence)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("child history has no initial Task turn"))?;
    // Sequence orders turns within one run. It cannot order different runs;
    // reject a conflicting owner anywhere in this child's relationship rows.
    ensure!(
        task_run_turn::Entity::find()
            .filter(task_run_turn::Column::ThreadId.eq(child))
            .filter(
                sea_orm::Condition::any()
                    .add(task_run_turn::Column::RunId.ne(&first.run_id))
                    .add(task_run_turn::Column::TaskId.ne(&first.task_id))
            )
            .select_only()
            .column(task_run_turn::Column::Id)
            .into_tuple::<String>()
            .one(db)
            .await?
            .is_none(),
        "child history has ambiguous Task runs"
    );
    let started_at = turn::Entity::find_by_id(&first.turn_id)
        .filter(turn::Column::ThreadId.eq(child))
        .select_only()
        .column(turn::Column::CreatedAt)
        .into_tuple::<sea_orm::prelude::DateTimeWithTimeZone>()
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("initial child turn is unavailable"))?;
    let run_task = task_run::Entity::find_by_id(&first.run_id)
        .select_only()
        .column(task_run::Column::TaskId)
        .into_tuple::<String>()
        .one(db)
        .await?;
    ensure!(
        run_task.as_deref() == Some(first.task_id.as_str()),
        "child history run changed"
    );
    let (task_source_turn, task_metadata_json) = task::Entity::find_by_id(&first.task_id)
        .filter(task::Column::WorkspaceId.eq(workspace))
        .select_only()
        .column(task::Column::CreatedByTurnId)
        .column_as(
            Expr::col(task::Column::MetadataJson).cast_as("text"),
            "metadata_json",
        )
        .into_tuple::<(Option<String>, Option<String>)>()
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("child history Task is unavailable"))?;
    // Old rows may predate thread bindings. The exact run-turn + snapshot +
    // lineage still have to agree; a binding, when present, narrows this lookup.
    let snapshot = task_run_conversation_snapshot::Entity::find_by_id(&first.run_id)
        .filter(task_run_conversation_snapshot::Column::TaskId.eq(&first.task_id))
        .filter(task_run_conversation_snapshot::Column::WorkspaceId.eq(workspace))
        .filter(
            task_run_conversation_snapshot::Column::ConversationThreadId
                .eq(&lineage.parent_thread_id),
        )
        .select_only()
        .column(task_run_conversation_snapshot::Column::SourceTurnId)
        .into_tuple::<Option<String>>()
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("child history snapshot relationship is unavailable"))?;
    let context_policy_json = task_agent_spec::Entity::find()
        .filter(task_agent_spec::Column::TaskId.eq(&first.task_id))
        .filter(task_agent_spec::Column::RunId.eq(&first.run_id))
        .select_only()
        .column_as(
            Expr::col(task_agent_spec::Column::ContextPolicyJson).cast_as("text"),
            "context_policy_json",
        )
        .into_tuple::<Option<String>>()
        .one(db)
        .await?;
    let context_policy_json = match context_policy_json {
        Some(policy) => policy,
        None => task_agent_spec::Entity::find()
            .filter(task_agent_spec::Column::TaskId.eq(&first.task_id))
            .filter(task_agent_spec::Column::RunId.is_null())
            .select_only()
            .column_as(
                Expr::col(task_agent_spec::Column::ContextPolicyJson).cast_as("text"),
                "context_policy_json",
            )
            .into_tuple::<Option<String>>()
            .one(db)
            .await?
            .flatten(),
    };
    Ok(Some(ChildHistoryParent {
        parent_thread: lineage.parent_thread_id,
        root_thread: lineage.root_thread_id,
        started_at,
        source_turn: snapshot,
        task_source_turn,
        task_metadata_json,
        context_policy_json,
    }))
}
