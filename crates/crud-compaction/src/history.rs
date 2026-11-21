//! Metadata-only capture fences for history projection. The fence limits later
//! discovery; payload reads still require workspace/thread scope and revisions.
use crate::live_sources as compaction_live_sources;
use anyhow::{Result, ensure};
use pioneer_compaction::SourceRef;
use pioneer_crud_conversations::events::CanonicalTurnEventPayload;
use pioneer_crud_execution::canonical_id::canonical_agent_id;
use pioneer_entity::{
    compaction_event_revision, compaction_input_revision, compaction_source_revision, task,
    task_delivery, task_run, task_run_conversation_snapshot, task_run_turn, thread, thread_lineage,
    turn, turn_event, turn_input, turn_llm_context,
};
use pioneer_observability::portable_payload as observe;
use sea_orm::sea_query::{Alias, BinOper, Expr, ExprTrait, Func, JoinType, Order, Query};
use sea_orm::{ColumnTrait, DbBackend, EntityTrait, QueryFilter, QueryTrait};
use sea_orm::{ConnectionTrait, FromQueryResult};

pub use crate::history_fence::HistoryReadFence;
pub use crate::history_fence::compaction_history_read_fence;
pub use crate::history_turn_pages::HistoryTurnBoundary;
pub use crate::history_turn_pages::{
    compaction_history_selected_turn_page, compaction_history_turn_page,
};
pub use crate::history_turn_pages::{history_before, history_event_before, history_input_before};

#[derive(Clone, Debug, FromQueryResult)]
pub struct HistoryCausalBoundary {
    pub delegated_command: bool,
    pub task_transport: bool,
    pub delivered_outcome: bool,
}
/// The immutable basis accepted for one exact child execution. This is a
/// storage locator plus its existing descriptor, not a new history copy.
#[derive(Clone, Debug)]
pub struct AcceptedTaskBasis {
    pub parent_thread: String,
    pub run_id: String,
    pub history_json: String,
}

pub use crate::child_history::ChildHistoryParent;
pub use crate::child_history::compaction_child_history_parent;

/// Timestamp metadata only, including a source whose body was edited. Missing
/// historical leaves are handled by checkpoint admission evidence by the caller.
pub async fn compaction_history_source_created_at<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread_id: &str,
    source: &SourceRef,
) -> Result<Option<sea_orm::prelude::DateTimeWithTimeZone>> {
    use sea_orm::QuerySelect;
    let Some((kind, turn_id)) = source.scope.split_once(':') else {
        return Ok(None);
    };
    if kind == "task-basis" {
        return Ok(
            task_run_conversation_snapshot::Entity::find_by_id(&source.id)
                .filter(task_run_conversation_snapshot::Column::RunId.eq(turn_id))
                .filter(task_run_conversation_snapshot::Column::WorkspaceId.eq(workspace))
                .filter(task_run_conversation_snapshot::Column::ConversationThreadId.eq(thread_id))
                .select_only()
                .column(task_run_conversation_snapshot::Column::CreatedAt)
                .into_tuple::<sea_orm::prelude::DateTimeWithTimeZone>()
                .one(db)
                .await?,
        );
    }
    let Some((turn_created_at, message_revision)) = turn::Entity::find_by_id(turn_id)
        .filter(turn::Column::ThreadId.eq(thread_id))
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .filter(thread::Column::WorkspaceId.eq(workspace))
        .select_only()
        .columns([turn::Column::CreatedAt, turn::Column::MessageRevision])
        .into_tuple::<(sea_orm::prelude::DateTimeWithTimeZone, i64)>()
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    macro_rules! timestamp {
        ($entity:ident) => {
            $entity::Entity::find_by_id(&source.id)
                .filter($entity::Column::TurnId.eq(turn_id))
                .select_only()
                .column($entity::Column::CreatedAt)
                .into_tuple::<sea_orm::prelude::DateTimeWithTimeZone>()
                .one(db)
                .await?
        };
    }
    Ok(match kind {
        "input" => timestamp!(turn_input).map(|at| {
            if message_revision > 0 {
                turn_created_at
            } else {
                at
            }
        }),
        "event" => {
            let mutation = message_revision > 0
                && turn_event::Entity::find_by_id(&source.id)
                    .filter(turn_event::Column::TurnId.eq(turn_id))
                    .filter(turn_event::Column::EventType.is_in([
                        pioneer_protocol::constants::events::TURN_MESSAGE_EDITED,
                        pioneer_protocol::constants::events::TURN_MESSAGE_DELETED,
                    ]))
                    .select_only()
                    .column(turn_event::Column::Id)
                    .into_tuple::<String>()
                    .one(db)
                    .await?
                    .is_some();
            if mutation {
                Some(turn_created_at)
            } else {
                timestamp!(turn_event)
            }
        }
        "context" => timestamp!(turn_llm_context),
        "item" => {
            use pioneer_entity::{compaction_item_revision, turn_item};
            let revision = compaction_item_revision::Entity::find_by_id(&source.id)
                .filter(compaction_item_revision::Column::TurnId.eq(turn_id))
                .select_only()
                .column(compaction_item_revision::Column::Revision)
                .into_tuple::<i64>()
                .one(db)
                .await?;
            if revision.is_none_or(|revision| source.version != format!("item-revision:{revision}"))
            {
                return Ok(None);
            }
            let item = turn_item::Entity::find_by_id(&source.id)
                .filter(turn_item::Column::TurnId.eq(turn_id))
                .select_only()
                .columns([turn_item::Column::ItemId, turn_item::Column::UpdatedAt])
                .into_tuple::<(String, sea_orm::prelude::DateTimeWithTimeZone)>()
                .one(db)
                .await?;
            if let Some((item_id, updated_at)) = item {
                turn_llm_context::Entity::find()
                    .filter(turn_llm_context::Column::TurnId.eq(turn_id))
                    .filter(turn_llm_context::Column::ItemId.eq(item_id))
                    .filter(turn_llm_context::Column::Source.eq("tool_result_v2"))
                    .select_only()
                    .column(turn_llm_context::Column::CreatedAt)
                    .into_tuple::<sea_orm::prelude::DateTimeWithTimeZone>()
                    .one(db)
                    .await?
                    .or(Some(updated_at))
            } else {
                None
            }
        }
        _ => None,
    })
}

pub use crate::input_copy_aliases::TaskInputCopyAlias;
pub use crate::input_copy_aliases::compaction_task_input_copy_aliases;

pub async fn compaction_turn_is_completed<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> Result<bool> {
    Ok(turn::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .expr(Expr::val(1_i64))
        .filter(
            Expr::col((thread::Entity, thread::Column::WorkspaceId))
                .eq(Expr::Value(workspace.into()))
                .and(
                    Expr::col((turn::Entity, turn::Column::ThreadId))
                        .eq(Expr::Value(thread.into())),
                )
                .and(Expr::col((turn::Entity, turn::Column::Id)).eq(Expr::Value(turn.into())))
                .and(Expr::col((turn::Entity, turn::Column::Status)).eq(Expr::val("completed"))),
        )
        .into_tuple::<i64>()
        .one(db)
        .await?
        .is_some())
}

/// Metadata locator for an already accepted legacy array. Payload remains
/// in its original immutable TaskRun snapshot, read through source fragments.
pub async fn compaction_legacy_task_basis_source<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    parent: &str,
    run: &str,
) -> Result<Option<SourceRef>> {
    let row = compaction_live_sources::SourceRow::find_by_statement(
        db.get_database_backend().build(
            &(sea_orm::sea_query::Query::select()
                .from(compaction_live_sources::Column::Table)
                .expr(Expr::col(compaction_live_sources::Column::SourceScope))
                .expr(Expr::col(compaction_live_sources::Column::SourceId))
                .expr(Expr::col(compaction_live_sources::Column::SourceVersion))
                .and_where(
                    Expr::col(compaction_live_sources::Column::WorkspaceId)
                        .eq(Expr::Value(workspace.into()))
                        .and(
                            Expr::col(compaction_live_sources::Column::ThreadId)
                                .eq(Expr::Value(parent.into())),
                        )
                        .and(
                            Expr::col(compaction_live_sources::Column::SourceScope)
                                .eq(Expr::val("task-basis:")
                                    .binary(BinOper::Custom("||"), Expr::Value(run.into()))),
                        )
                        .and(
                            Expr::col(compaction_live_sources::Column::SourceId)
                                .eq(Expr::Value(run.into())),
                        ),
                )
                .to_owned()),
        ),
    )
    .one(db)
    .await?
    .map(|row| (row.source_scope, row.source_id, row.source_version));
    row.map(|(scope, id, version)| Ok(SourceRef { scope, id, version }))
        .transpose()
}

/// The parent basis admitted for this exact child execution. Attachment is
/// deliberately irrelevant: it controls lifecycle/hooks, not history scope.
/// Read only identity metadata, never the snapshot transcript or Task body.
pub async fn compaction_task_basis_thread<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> Result<Option<String>> {
    Ok(task_run_turn::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            task_run_turn::Entity::belongs_to(task_run_conversation_snapshot::Entity)
                .from(task_run_turn::Column::RunId)
                .to(task_run_conversation_snapshot::Column::RunId)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((
                            task_run_conversation_snapshot::Entity,
                            task_run_conversation_snapshot::Column::TaskId,
                        ))
                        .eq(Expr::col((
                            task_run_turn::Entity,
                            task_run_turn::Column::TaskId,
                        ))),
                    )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            task_run_turn::Entity::belongs_to(thread_lineage::Entity)
                .from(task_run_turn::Column::ThreadId)
                .to(thread_lineage::Column::ChildThreadId)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((
                            thread_lineage::Entity,
                            thread_lineage::Column::ParentThreadId,
                        ))
                        .eq(Expr::col((
                            task_run_conversation_snapshot::Entity,
                            task_run_conversation_snapshot::Column::ConversationThreadId,
                        ))),
                    )
                })
                .into(),
        )
        .join_as(
            JoinType::InnerJoin,
            task_run_turn::Entity::belongs_to(thread::Entity)
                .from(task_run_turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
            Alias::new("child"),
        )
        .join_as(
            JoinType::InnerJoin,
            task_run_conversation_snapshot::Entity::belongs_to(thread::Entity)
                .from(task_run_conversation_snapshot::Column::ConversationThreadId)
                .to(thread::Column::Id)
                .into(),
            Alias::new("parent"),
        )
        .expr(Expr::col((
            task_run_conversation_snapshot::Entity,
            task_run_conversation_snapshot::Column::ConversationThreadId,
        )))
        .filter(
            Expr::col((task_run_turn::Entity, task_run_turn::Column::ThreadId))
                .eq(Expr::Value(thread.into()))
                .and(
                    Expr::col((task_run_turn::Entity, task_run_turn::Column::TurnId))
                        .eq(Expr::Value(turn.into())),
                )
                .and(
                    Expr::col((
                        task_run_conversation_snapshot::Entity,
                        task_run_conversation_snapshot::Column::WorkspaceId,
                    ))
                    .eq(Expr::Value(workspace.into())),
                )
                .and(
                    Expr::col(("child", thread::Column::WorkspaceId)).eq(Expr::col((
                        task_run_conversation_snapshot::Entity,
                        task_run_conversation_snapshot::Column::WorkspaceId,
                    ))),
                )
                .and(
                    Expr::col(("parent", thread::Column::WorkspaceId)).eq(Expr::col((
                        task_run_conversation_snapshot::Entity,
                        task_run_conversation_snapshot::Column::WorkspaceId,
                    ))),
                ),
        )
        .into_tuple::<String>()
        .one(db)
        .await?)
}

/// For a destination without a creator execution, select the most recent
/// admitted basis whose actual input existed at the shared capture fence.
/// This reads relationship metadata, never a newer ancestor transcript.
pub async fn compaction_latest_task_basis_turn<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    fence: &HistoryReadFence,
) -> Result<Option<String>> {
    Ok(turn::Entity::find()
        .select_only()
        .join_as(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
            Alias::new("th"),
        )
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(task_run_turn::Entity)
                .from(turn::Column::ThreadId)
                .to(task_run_turn::Column::ThreadId)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((task_run_turn::Entity, task_run_turn::Column::TurnId))
                            .eq(Expr::col((turn::Entity, turn::Column::Id))),
                    )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            task_run_turn::Entity::belongs_to(task_run_conversation_snapshot::Entity)
                .from(task_run_turn::Column::RunId)
                .to(task_run_conversation_snapshot::Column::RunId)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((
                            task_run_conversation_snapshot::Entity,
                            task_run_conversation_snapshot::Column::TaskId,
                        ))
                        .eq(Expr::col((
                            task_run_turn::Entity,
                            task_run_turn::Column::TaskId,
                        ))),
                    )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread_lineage::Entity)
                .from(turn::Column::ThreadId)
                .to(thread_lineage::Column::ChildThreadId)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((
                            thread_lineage::Entity,
                            thread_lineage::Column::ParentThreadId,
                        ))
                        .eq(Expr::col((
                            task_run_conversation_snapshot::Entity,
                            task_run_conversation_snapshot::Column::ConversationThreadId,
                        ))),
                    )
                })
                .into(),
        )
        .join_as(
            JoinType::InnerJoin,
            task_run_conversation_snapshot::Entity::belongs_to(thread::Entity)
                .from(task_run_conversation_snapshot::Column::ConversationThreadId)
                .to(thread::Column::Id)
                .into(),
            Alias::new("parent"),
        )
        .expr(Expr::col((turn::Entity, turn::Column::Id)))
        .filter(
            Expr::col(("th", thread::Column::WorkspaceId))
                .eq(Expr::Value(workspace.into()))
                .and(
                    Expr::col((turn::Entity, turn::Column::ThreadId))
                        .eq(Expr::Value(thread.into())),
                )
                .and(
                    Expr::col((
                        task_run_conversation_snapshot::Entity,
                        task_run_conversation_snapshot::Column::WorkspaceId,
                    ))
                    .eq(Expr::col(("th", thread::Column::WorkspaceId))),
                )
                .and(
                    Expr::col(("parent", thread::Column::WorkspaceId))
                        .eq(Expr::col(("th", thread::Column::WorkspaceId))),
                )
                .and(Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(turn_input::Entity, "i")
                        .join_as(
                            JoinType::InnerJoin,
                            compaction_input_revision::Entity,
                            "r",
                            Expr::col(("r", compaction_input_revision::Column::SourceId))
                                .eq(Expr::col(("i", turn_input::Column::Id)))
                                .and(
                                    Expr::col(("r", compaction_input_revision::Column::TurnId))
                                        .eq(Expr::col(("i", turn_input::Column::TurnId))),
                                )
                                .and(
                                    Expr::col(("r", compaction_input_revision::Column::Present))
                                        .eq(Expr::val(1_i64)),
                                ),
                        )
                        .and_where(
                            Expr::col(("i", turn_input::Column::TurnId))
                                .eq(Expr::col((turn::Entity, turn::Column::Id)))
                                .and(
                                    Expr::col((
                                        "r",
                                        compaction_input_revision::Column::CaptureOrder,
                                    ))
                                    .lte(Expr::Value(fence.input_order.into())),
                                ),
                        )
                        .to_owned(),
                )),
        )
        .order_by(
            Expr::col((turn::Entity, turn::Column::CreatedAt)),
            Order::Desc,
        )
        .order_by(Expr::col((turn::Entity, turn::Column::Id)), Order::Desc)
        .limit(1)
        .into_tuple::<String>()
        .one(db)
        .await?)
}

/// Read the already accepted TaskRun basis in byte-bounded fragments.
/// Each fragment repeats the exact execution/lineage/workspace predicate;
/// deletion or reparenting cannot yield a partially authorized transcript.
/// Snapshot rows are immutable (insert-if-absent); decoding is outside DB
/// capacity. Inline history arrays retain their original bytes.
pub async fn compaction_task_basis_snapshot(
    db: &pioneer_sqlite::SqliteDatabase,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> Result<Option<AcceptedTaskBasis>> {
    observe::scope_result(
        observe::Family::TaskHistory,
        observe::Mode::Full,
        compaction_task_basis_snapshot_inner(db, workspace, thread, turn),
    )
    .await
}
async fn compaction_task_basis_snapshot_inner(
    db: &pioneer_sqlite::SqliteDatabase,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> Result<Option<AcceptedTaskBasis>> {
    let reader = db.begin_read().await?;
    let metadata_read = observe::stage(observe::Stage::SourceRead);
    let Some(row) = task_run_turn::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            task_run_turn::Entity::belongs_to(task_run_conversation_snapshot::Entity)
                .from(task_run_turn::Column::RunId)
                .to(task_run_conversation_snapshot::Column::RunId)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((
                            task_run_conversation_snapshot::Entity,
                            task_run_conversation_snapshot::Column::TaskId,
                        ))
                        .eq(Expr::col((
                            task_run_turn::Entity,
                            task_run_turn::Column::TaskId,
                        ))),
                    )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            task_run_turn::Entity::belongs_to(thread_lineage::Entity)
                .from(task_run_turn::Column::ThreadId)
                .to(thread_lineage::Column::ChildThreadId)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((
                            thread_lineage::Entity,
                            thread_lineage::Column::ParentThreadId,
                        ))
                        .eq(Expr::col((
                            task_run_conversation_snapshot::Entity,
                            task_run_conversation_snapshot::Column::ConversationThreadId,
                        ))),
                    )
                })
                .into(),
        )
        .join_as(
            JoinType::InnerJoin,
            task_run_turn::Entity::belongs_to(thread::Entity)
                .from(task_run_turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
            Alias::new("child"),
        )
        .join_as(
            JoinType::InnerJoin,
            task_run_conversation_snapshot::Entity::belongs_to(thread::Entity)
                .from(task_run_conversation_snapshot::Column::ConversationThreadId)
                .to(thread::Column::Id)
                .into(),
            Alias::new("parent"),
        )
        .expr(Expr::col((
            task_run_conversation_snapshot::Entity,
            task_run_conversation_snapshot::Column::RunId,
        )))
        .expr(Expr::col((
            task_run_conversation_snapshot::Entity,
            task_run_conversation_snapshot::Column::ConversationThreadId,
        )))
        .filter(
            Expr::col((task_run_turn::Entity, task_run_turn::Column::ThreadId))
                .eq(Expr::Value(thread.into()))
                .and(
                    Expr::col((task_run_turn::Entity, task_run_turn::Column::TurnId))
                        .eq(Expr::Value(turn.into())),
                )
                .and(
                    Expr::col((
                        task_run_conversation_snapshot::Entity,
                        task_run_conversation_snapshot::Column::WorkspaceId,
                    ))
                    .eq(Expr::Value(workspace.into())),
                )
                .and(
                    Expr::col(("child", thread::Column::WorkspaceId)).eq(Expr::col((
                        task_run_conversation_snapshot::Entity,
                        task_run_conversation_snapshot::Column::WorkspaceId,
                    ))),
                )
                .and(
                    Expr::col(("parent", thread::Column::WorkspaceId)).eq(Expr::col((
                        task_run_conversation_snapshot::Entity,
                        task_run_conversation_snapshot::Column::WorkspaceId,
                    ))),
                ),
        )
        .expr(Expr::col((
            task_run_conversation_snapshot::Entity,
            task_run_conversation_snapshot::Column::HistoryJsonPayloadId,
        )))
        .into_model::<BasisSnapshotMetadata>()
        .one(&reader)
        .await?
    else {
        return Ok(None);
    };
    let capture = pioneer_crud_tasks::conversation_snapshot::capture_history(
        &reader,
        &row.run_id,
        row.history_json_payload_id.as_deref(),
    )
    .await?;
    reader.commit().await?;
    drop(metadata_read);
    Ok(Some(AcceptedTaskBasis {
        parent_thread: row.conversation_thread_id,
        run_id: row.run_id,
        history_json: capture.into_text()?,
    }))
}

/// Refresh a stale metadata cache after decoding a scoped canonical source
/// outside database capacity. The revision CAS prevents a later event edit
/// from being labelled using the earlier typed payload.
pub async fn compaction_record_event_projection<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    source: &SourceRef,
    event: &CanonicalTurnEventPayload,
) -> Result<bool> {
    let turn = source
        .scope
        .strip_prefix("event:")
        .ok_or_else(|| anyhow::anyhow!("projection source is not an event"))?;
    ensure!(
        event.workspace_id() == workspace && event.thread_id() == thread && event.turn_id() == turn,
        "canonical projection scope mismatch"
    );
    let (item, kind) = event_projection_metadata(event);
    Ok(compaction_event_revision::Entity::update_many()
        .col_expr(
            compaction_event_revision::Column::ProjectionRevision,
            Expr::col(compaction_event_revision::Column::Revision),
        )
        .col_expr(
            compaction_event_revision::Column::ItemId,
            Expr::Value(item.into()),
        )
        .col_expr(
            compaction_event_revision::Column::ProjectionKind,
            Expr::Value(kind.into()),
        )
        .filter(
            Expr::col(compaction_event_revision::Column::SourceId)
                .eq(Expr::Value(source.id.clone().into()))
                .and(
                    Expr::col(compaction_event_revision::Column::TurnId)
                        .eq(Expr::Value(turn.into())),
                )
                .and(Expr::col(compaction_event_revision::Column::Present).eq(Expr::val(1_i64)))
                .and(
                    Expr::val("event-revision:")
                        .binary(
                            BinOper::Custom("||"),
                            Expr::col(compaction_event_revision::Column::Revision),
                        )
                        .eq(Expr::Value(source.version.clone().into())),
                )
                .and(Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(turn_event::Entity, "e")
                        .join_as(
                            JoinType::InnerJoin,
                            turn::Entity,
                            "t",
                            Expr::col(("t", turn::Column::Id))
                                .eq(Expr::col(("e", turn_event::Column::TurnId)))
                                .and(
                                    Expr::col(("t", turn::Column::ThreadId))
                                        .eq(Expr::col(("e", turn_event::Column::ThreadId))),
                                ),
                        )
                        .join_as(
                            JoinType::InnerJoin,
                            thread::Entity,
                            "th",
                            Expr::col(("th", thread::Column::Id))
                                .eq(Expr::col(("t", turn::Column::ThreadId))),
                        )
                        .and_where(
                            Expr::col(("e", turn_event::Column::Id))
                                .eq(Expr::col(("compaction_event_revision", "source_id")))
                                .and(
                                    Expr::col(("e", turn_event::Column::TurnId))
                                        .eq(Expr::col(("compaction_event_revision", "turn_id"))),
                                )
                                .and(
                                    Expr::col(("e", turn_event::Column::ThreadId))
                                        .eq(Expr::Value(thread.into())),
                                )
                                .and(
                                    Expr::col(("th", thread::Column::WorkspaceId))
                                        .eq(Expr::Value(workspace.into())),
                                ),
                        )
                        .to_owned(),
                )),
        )
        .exec(db)
        .await?
        .rows_affected
        == 1)
}

/// Exact command/outcome relationship for a canonical delivered result.
/// This is a point metadata query: neither matching text nor a delivered
/// status alone establishes an alias. Frozen manifests preserve this link.
pub async fn compaction_task_delivery_command(
    db: &impl ConnectionTrait,
    workspace: &str,
    thread: &str,
    source: &SourceRef,
) -> Result<Option<String>> {
    let Some(turn) = source.scope.strip_prefix("event:") else {
        return Ok(None);
    };
    let row = turn_event::Entity::find()
        .select_only()
        .join_as(
            JoinType::InnerJoin,
            turn_event::Entity::belongs_to(turn::Entity)
                .from(turn_event::Column::TurnId)
                .to(turn::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(Expr::col(("t", turn::Column::ThreadId)).eq(
                        Expr::col((turn_event::Entity, turn_event::Column::ThreadId)),
                    ))
                })
                .into(),
            Alias::new("t"),
        )
        .join(
            JoinType::InnerJoin,
            sea_orm::RelationDef::from(
                turn::Entity::belongs_to(thread::Entity)
                    .from(turn::Column::ThreadId)
                    .to(thread::Column::Id),
            )
            .from_alias(Alias::new("t")),
        )
        .join(
            JoinType::InnerJoin,
            turn_event::Entity::belongs_to(compaction_event_revision::Entity)
                .from(turn_event::Column::Id)
                .to(compaction_event_revision::Column::SourceId)
                .on_condition(|_, _| {
                    sea_orm::Condition::all()
                        .add(
                            Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::TurnId,
                            ))
                            .eq(Expr::col((turn_event::Entity, turn_event::Column::TurnId))),
                        )
                        .add(
                            Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::Present,
                            ))
                            .eq(Expr::val(1_i64)),
                        )
                        .add(
                            Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::ProjectionRevision,
                            ))
                            .eq(Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::Revision,
                            ))),
                        )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            turn_event::Entity::belongs_to(task_delivery::Entity)
                .from(turn_event::Column::TurnId)
                .to(task_delivery::Column::DeliveredTurnId)
                .on_condition(|_, _| {
                    sea_orm::Condition::all()
                        .add(
                            Expr::col((
                                task_delivery::Entity,
                                task_delivery::Column::TargetThreadId,
                            ))
                            .eq(Expr::col((
                                turn_event::Entity,
                                turn_event::Column::ThreadId,
                            ))),
                        )
                        .add(
                            Expr::col((task_delivery::Entity, task_delivery::Column::WorkspaceId))
                                .eq(Expr::col((thread::Entity, thread::Column::WorkspaceId))),
                        )
                        .add(
                            Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::ItemId,
                            ))
                            .eq(Expr::Value(
                                pioneer_protocol::task_delivery_result_item_id("").into(),
                            )
                            .binary(
                                BinOper::Custom("||"),
                                Expr::col((task_delivery::Entity, task_delivery::Column::Id)),
                            )),
                        )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            task_delivery::Entity::belongs_to(task::Entity)
                .from(task_delivery::Column::TaskId)
                .to(task::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all()
                        .add(
                            Expr::col((task::Entity, task::Column::WorkspaceId))
                                .eq(Expr::col((thread::Entity, thread::Column::WorkspaceId))),
                        )
                        .add(
                            Expr::col((task::Entity, task::Column::CreatedByThreadId)).eq(
                                Expr::col((turn_event::Entity, turn_event::Column::ThreadId)),
                            ),
                        )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            task_delivery::Entity::belongs_to(task_run::Entity)
                .from(task_delivery::Column::RunId)
                .to(task_run::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((task_run::Entity, task_run::Column::TaskId))
                            .eq(Expr::col((task::Entity, task::Column::Id))),
                    )
                })
                .into(),
        )
        .join_as(
            JoinType::InnerJoin,
            task::Entity::belongs_to(turn::Entity)
                .from(task::Column::CreatedByTurnId)
                .to(turn::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col(("command", turn::Column::ThreadId))
                            .eq(Expr::col((task::Entity, task::Column::CreatedByThreadId))),
                    )
                })
                .into(),
            Alias::new("command"),
        )
        .expr(Expr::col((task::Entity, task::Column::CreatedByTurnId)))
        .filter(
            Expr::col((thread::Entity, thread::Column::WorkspaceId))
                .eq(Expr::Value(workspace.into()))
                .and(
                    Expr::col((turn_event::Entity, turn_event::Column::ThreadId))
                        .eq(Expr::Value(thread.into())),
                )
                .and(
                    Expr::col((turn_event::Entity, turn_event::Column::TurnId))
                        .eq(Expr::Value(turn.into())),
                )
                .and(
                    Expr::col((turn_event::Entity, turn_event::Column::Id))
                        .eq(Expr::Value(source.id.clone().into())),
                )
                .and(
                    Expr::val("event-revision:")
                        .binary(
                            BinOper::Custom("||"),
                            Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::Revision,
                            )),
                        )
                        .eq(Expr::Value(source.version.clone().into())),
                )
                .and(
                    Expr::col((turn_event::Entity, turn_event::Column::EventType)).eq(Expr::Value(
                        pioneer_protocol::constants::events::ITEM_COMPLETED.into(),
                    )),
                )
                .and(
                    Expr::col((task_delivery::Entity, task_delivery::Column::Status))
                        .eq(Expr::val("delivered")),
                )
                .and(Expr::col((task::Entity, task::Column::CreatedByTurnId)).is_not_null()),
        )
        .into_tuple::<String>()
        .one(db)
        .await?;
    if let Some(row) = row {
        return Ok(Some(row));
    }
    // Failed, blocked and interrupted occurrence turns have no result item.
    // Their exact TaskRun identity and acknowledged event establish closure,
    // including cancellation which intentionally suppresses TaskDelivery.
    let row = turn_event::Entity::find()
        .select_only()
        .join_as(
            JoinType::InnerJoin,
            turn_event::Entity::belongs_to(turn::Entity)
                .from(turn_event::Column::TurnId)
                .to(turn::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(Expr::col(("t", turn::Column::ThreadId)).eq(
                        Expr::col((turn_event::Entity, turn_event::Column::ThreadId)),
                    ))
                })
                .into(),
            Alias::new("t"),
        )
        .join(
            JoinType::InnerJoin,
            sea_orm::RelationDef::from(
                turn::Entity::belongs_to(thread::Entity)
                    .from(turn::Column::ThreadId)
                    .to(thread::Column::Id),
            )
            .from_alias(Alias::new("t")),
        )
        .join(
            JoinType::InnerJoin,
            turn_event::Entity::belongs_to(compaction_event_revision::Entity)
                .from(turn_event::Column::Id)
                .to(compaction_event_revision::Column::SourceId)
                .on_condition(|_, _| {
                    sea_orm::Condition::all()
                        .add(
                            Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::TurnId,
                            ))
                            .eq(Expr::col((turn_event::Entity, turn_event::Column::TurnId))),
                        )
                        .add(
                            Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::Present,
                            ))
                            .eq(Expr::val(1_i64)),
                        )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            sea_orm::RelationDef::from(
                turn::Entity::belongs_to(task_run::Entity)
                    .from(turn::Column::Id)
                    .to(task_run::Column::Id),
            )
            .from_alias(Alias::new("t")),
        )
        .join(
            JoinType::InnerJoin,
            task_run::Entity::belongs_to(task::Entity)
                .from(task_run::Column::TaskId)
                .to(task::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all()
                        .add(
                            Expr::col((task::Entity, task::Column::WorkspaceId))
                                .eq(Expr::col((thread::Entity, thread::Column::WorkspaceId))),
                        )
                        .add(
                            Expr::col((task::Entity, task::Column::CreatedByThreadId))
                                .eq(Expr::col(("t", turn::Column::ThreadId))),
                        )
                })
                .into(),
        )
        .join_as(
            JoinType::InnerJoin,
            task::Entity::belongs_to(turn::Entity)
                .from(task::Column::CreatedByTurnId)
                .to(turn::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col(("command", turn::Column::ThreadId))
                            .eq(Expr::col(("t", turn::Column::ThreadId))),
                    )
                })
                .into(),
            Alias::new("command"),
        )
        .expr(Expr::col((task::Entity, task::Column::CreatedByTurnId)))
        .filter(
            Expr::col((thread::Entity, thread::Column::WorkspaceId))
                .eq(Expr::Value(workspace.into()))
                .and(
                    Expr::col((turn_event::Entity, turn_event::Column::ThreadId))
                        .eq(Expr::Value(thread.into())),
                )
                .and(
                    Expr::col((turn_event::Entity, turn_event::Column::TurnId))
                        .eq(Expr::Value(turn.into())),
                )
                .and(
                    Expr::col((turn_event::Entity, turn_event::Column::Id))
                        .eq(Expr::Value(source.id.clone().into())),
                )
                .and(
                    Expr::val("event-revision:")
                        .binary(
                            BinOper::Custom("||"),
                            Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::Revision,
                            )),
                        )
                        .eq(Expr::Value(source.version.clone().into())),
                )
                .and(Expr::col(("t", turn::Column::TurnKind)).eq(Expr::val("task_run")))
                .and(
                    Expr::col((turn_event::Entity, turn_event::Column::EventType)).is_in([
                        Expr::Value(pioneer_protocol::constants::events::TURN_FAILED.into()),
                        Expr::Value(pioneer_protocol::constants::events::TURN_BLOCKED.into()),
                    ]),
                ),
        )
        .into_tuple::<String>()
        .one(db)
        .await?;
    if let Some(row) = row {
        return Ok(Some(row));
    }
    compaction_failed_delivery_command(db, workspace, thread, None, Some(source), i64::MAX, None)
        .await
}

/// Generic failed deliveries use a deterministic Turn ID, checked after
/// releasing DB capacity. Page only delivery identities whose exact failure
/// event is visible; never infer an outcome from mutable TaskRun status.
pub async fn compaction_failed_delivery_command<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    command: Option<&str>,
    source: Option<&SourceRef>,
    event_fence: i64,
    before: Option<sea_orm::prelude::DateTimeWithTimeZone>,
) -> Result<Option<String>> {
    let mut after = String::new();
    loop {
        let rows = task_delivery::Entity::find()
            .select_only()
            .join(
                JoinType::InnerJoin,
                task_delivery::Entity::belongs_to(task::Entity)
                    .from(task_delivery::Column::TaskId)
                    .to(task::Column::Id)
                    .on_condition(|_, _| {
                        sea_orm::Condition::all()
                            .add(Expr::col((task::Entity, task::Column::WorkspaceId)).eq(
                                Expr::col((
                                    task_delivery::Entity,
                                    task_delivery::Column::WorkspaceId,
                                )),
                            ))
                            .add(
                                Expr::col((task::Entity, task::Column::CreatedByThreadId)).eq(
                                    Expr::col((
                                        task_delivery::Entity,
                                        task_delivery::Column::TargetThreadId,
                                    )),
                                ),
                            )
                    })
                    .into(),
            )
            .join(
                JoinType::InnerJoin,
                task_delivery::Entity::belongs_to(task_run::Entity)
                    .from(task_delivery::Column::RunId)
                    .to(task_run::Column::Id)
                    .on_condition(|_, _| {
                        sea_orm::Condition::all().add(
                            Expr::col((task_run::Entity, task_run::Column::TaskId))
                                .eq(Expr::col((task::Entity, task::Column::Id))),
                        )
                    })
                    .into(),
            )
            .join_as(
                JoinType::InnerJoin,
                task::Entity::belongs_to(turn::Entity)
                    .from(task::Column::CreatedByTurnId)
                    .to(turn::Column::Id)
                    .on_condition(|_, _| {
                        sea_orm::Condition::all().add(
                            Expr::col(("command", turn::Column::ThreadId)).eq(Expr::col((
                                task_delivery::Entity,
                                task_delivery::Column::TargetThreadId,
                            ))),
                        )
                    })
                    .into(),
                Alias::new("command"),
            )
            .join_as(
                JoinType::InnerJoin,
                task_delivery::Entity::belongs_to(turn::Entity)
                    .from(task_delivery::Column::DeliveredTurnId)
                    .to(turn::Column::Id)
                    .on_condition(|_, _| {
                        sea_orm::Condition::all().add(
                            Expr::col(("outcome", turn::Column::ThreadId)).eq(Expr::col((
                                task_delivery::Entity,
                                task_delivery::Column::TargetThreadId,
                            ))),
                        )
                    })
                    .into(),
                Alias::new("outcome"),
            )
            .join(
                JoinType::InnerJoin,
                sea_orm::RelationDef::from(
                    turn::Entity::belongs_to(thread::Entity)
                        .from(turn::Column::ThreadId)
                        .to(thread::Column::Id)
                        .on_condition(|_, _| {
                            sea_orm::Condition::all().add(
                                Expr::col((thread::Entity, thread::Column::WorkspaceId)).eq(
                                    Expr::col((
                                        task_delivery::Entity,
                                        task_delivery::Column::WorkspaceId,
                                    )),
                                ),
                            )
                        }),
                )
                .from_alias(Alias::new("outcome")),
            )
            .expr(Expr::col((
                task_delivery::Entity,
                task_delivery::Column::Id,
            )))
            .expr(Expr::col((
                task_delivery::Entity,
                task_delivery::Column::DeliveredTurnId,
            )))
            .expr(Expr::col((task::Entity, task::Column::CreatedByTurnId)))
            .filter(
                Expr::col((task_delivery::Entity, task_delivery::Column::WorkspaceId))
                    .eq(Expr::Value(workspace.into()))
                    .and(
                        Expr::col((task_delivery::Entity, task_delivery::Column::TargetThreadId))
                            .eq(Expr::Value(thread.into())),
                    )
                    .and(
                        Expr::col((task_delivery::Entity, task_delivery::Column::Status))
                            .eq(Expr::val("delivered")),
                    )
                    .and(
                        Expr::col((task_delivery::Entity, task_delivery::Column::Id))
                            .gt(Expr::Value(after.clone().into())),
                    )
                    .and(
                        Expr::val(command.is_none()).or(Expr::col((
                            task::Entity,
                            task::Column::CreatedByTurnId,
                        ))
                        .eq(Expr::Value(command.map(str::to_owned).into()))),
                    )
                    .and(Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as(turn_event::Entity, "e")
                            .join_as(
                                JoinType::InnerJoin,
                                compaction_event_revision::Entity,
                                "r",
                                Expr::col(("r", compaction_event_revision::Column::SourceId))
                                    .eq(Expr::col(("e", turn_event::Column::Id)))
                                    .and(
                                        Expr::col(("r", compaction_event_revision::Column::TurnId))
                                            .eq(Expr::col(("e", turn_event::Column::TurnId))),
                                    )
                                    .and(
                                        Expr::col((
                                            "r",
                                            compaction_event_revision::Column::Present,
                                        ))
                                        .eq(Expr::val(1_i64)),
                                    ),
                            )
                            .and_where(
                                Expr::col(("e", turn_event::Column::TurnId))
                                    .eq(Expr::col(("outcome", turn::Column::Id)))
                                    .and(
                                        Expr::col(("e", turn_event::Column::ThreadId))
                                            .eq(Expr::col(("outcome", turn::Column::ThreadId))),
                                    )
                                    .and(Expr::col(("e", turn_event::Column::EventType)).eq(
                                        Expr::Value(
                                            pioneer_protocol::constants::events::TURN_FAILED.into(),
                                        ),
                                    ))
                                    .and(
                                        Expr::col((
                                            "r",
                                            compaction_event_revision::Column::CaptureOrder,
                                        ))
                                        .lte(Expr::Value(event_fence.into())),
                                    ),
                            )
                            // The source is an immutable argument, not prepared database
                            // state. Emit its exact key directly: a nullable-parameter OR
                            // can hide the primary-key lookup and make every
                            // historical message scan unrelated event revisions.
                            .and_where_option(before.map(|before| {
                                history_before(
                                    Expr::col(("e", turn_event::Column::CreatedAt)),
                                    before,
                                )
                            }))
                            .and_where_option(source.map(|source| {
                                Expr::col(("e", turn_event::Column::Id))
                                    .eq(source.id.clone())
                                    .and(
                                        Expr::col(("e", turn_event::Column::TurnId)).eq(source
                                            .scope
                                            .strip_prefix("event:")
                                            .map(str::to_owned)),
                                    )
                                    .and(
                                        Expr::val("event-revision:")
                                            .binary(
                                                BinOper::Custom("||"),
                                                Expr::col((
                                                    "r",
                                                    compaction_event_revision::Column::Revision,
                                                )),
                                            )
                                            .eq(source.version.clone()),
                                    )
                            }))
                            .to_owned(),
                    )),
            )
            .order_by(
                Expr::col((task_delivery::Entity, task_delivery::Column::Id)),
                Order::Asc,
            )
            .limit(128)
            .into_tuple::<(String, String, String)>()
            .all(db)
            .await?;
        if rows.is_empty() {
            return Ok(None);
        }
        for (delivery, outcome, created_by_turn_id) in rows {
            if outcome == canonical_agent_id('T', &format!("task-delivery-turn\0{delivery}")) {
                return Ok(Some(created_by_turn_id));
            }
            after = delivery;
        }
    }
}

/// Bounded relationship metadata for a single selected turn. A Task status
/// alone does not close its command: the identified outcome event must
/// already exist below the same event fence. This grants no child-history
/// access and reads no Task result or event payload.
pub async fn compaction_history_causal_boundary(
    db: &impl ConnectionTrait,
    workspace: &str,
    thread: &str,
    turn: &str,
    fence: &HistoryReadFence,
) -> Result<HistoryCausalBoundary> {
    let mut boundary = turn::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .expr_as(
            Expr::exists(
                Query::select()
                    .expr(Expr::val(1_i64))
                    .from_as(task::Entity, "task")
                    .and_where_option(fence.before.map(|before| {
                        history_before(Expr::col(("task", task::Column::CreatedAt)), before)
                    }))
                    .and_where(
                        Expr::col(("task", task::Column::WorkspaceId))
                            .eq(Expr::col((thread::Entity, thread::Column::WorkspaceId)))
                            .and(
                                Expr::col(("task", task::Column::CreatedByThreadId))
                                    .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
                            )
                            .and(
                                Expr::col(("task", task::Column::CreatedByTurnId))
                                    .eq(Expr::col((turn::Entity, turn::Column::Id))),
                            ),
                    )
                    .to_owned(),
            ),
            "delegated_command",
        )
        .expr_as(
            Expr::exists(
                Query::select()
                    .expr(Expr::val(1_i64))
                    .from_as(task_run::Entity, "r")
                    .join_as(
                        JoinType::InnerJoin,
                        task::Entity,
                        "task",
                        Expr::col(("task", task::Column::Id))
                            .eq(Expr::col(("r", task_run::Column::TaskId))),
                    )
                    .and_where(
                        Expr::col(("r", task_run::Column::Id))
                            .eq(Expr::col((turn::Entity, turn::Column::Id)))
                            .and(
                                Expr::col(("task", task::Column::WorkspaceId))
                                    .eq(Expr::col((thread::Entity, thread::Column::WorkspaceId))),
                            ),
                    )
                    .to_owned(),
            )
            .or(Expr::exists(
                Query::select()
                    .expr(Expr::val(1_i64))
                    .from_as(task_delivery::Entity, "d")
                    .and_where_option(fence.before.map(|before| {
                        history_before(Expr::col(("d", task_delivery::Column::CreatedAt)), before)
                    }))
                    .and_where(
                        Expr::col(("d", task_delivery::Column::WorkspaceId))
                            .eq(Expr::col((thread::Entity, thread::Column::WorkspaceId)))
                            .and(
                                Expr::col(("d", task_delivery::Column::TargetThreadId))
                                    .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
                            )
                            .and(
                                Expr::col(("d", task_delivery::Column::DeliveredTurnId))
                                    .eq(Expr::col((turn::Entity, turn::Column::Id))),
                            ),
                    )
                    .to_owned(),
            )),
            "task_transport",
        )
        .expr_as(
            Expr::exists(
                Query::select()
                    .expr(Expr::val(1_i64))
                    .from_as(task::Entity, "task")
                    .and_where_option(fence.before.map(|before| {
                        history_before(Expr::col(("task", task::Column::CreatedAt)), before)
                    }))
                    .join_as(
                        JoinType::InnerJoin,
                        task_delivery::Entity,
                        "d",
                        Expr::col(("d", task_delivery::Column::TaskId))
                            .eq(Expr::col(("task", task::Column::Id))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        task_run::Entity,
                        "run",
                        Expr::col(("run", task_run::Column::Id))
                            .eq(Expr::col(("d", task_delivery::Column::RunId)))
                            .and(
                                Expr::col(("run", task_run::Column::TaskId))
                                    .eq(Expr::col(("task", task::Column::Id))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        turn_event::Entity,
                        "e",
                        Expr::col(("e", turn_event::Column::TurnId))
                            .eq(Expr::col(("d", task_delivery::Column::DeliveredTurnId)))
                            .and(
                                Expr::col(("e", turn_event::Column::ThreadId))
                                    .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        compaction_event_revision::Entity,
                        "v",
                        Expr::col(("v", compaction_event_revision::Column::SourceId))
                            .eq(Expr::col(("e", turn_event::Column::Id)))
                            .and(
                                Expr::col(("v", compaction_event_revision::Column::TurnId))
                                    .eq(Expr::col(("e", turn_event::Column::TurnId))),
                            )
                            .and(
                                Expr::col(("v", compaction_event_revision::Column::Present))
                                    .eq(Expr::val(1_i64)),
                            )
                            .and(
                                Expr::col((
                                    "v",
                                    compaction_event_revision::Column::ProjectionRevision,
                                ))
                                .eq(Expr::col((
                                    "v",
                                    compaction_event_revision::Column::Revision,
                                ))),
                            ),
                    )
                    .and_where(
                        Expr::col(("task", task::Column::WorkspaceId))
                            .eq(Expr::col((thread::Entity, thread::Column::WorkspaceId)))
                            .and(
                                Expr::col(("task", task::Column::CreatedByThreadId))
                                    .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
                            )
                            .and(
                                Expr::col(("task", task::Column::CreatedByTurnId))
                                    .eq(Expr::col((turn::Entity, turn::Column::Id))),
                            )
                            .and(
                                Expr::col(("d", task_delivery::Column::WorkspaceId))
                                    .eq(Expr::col((thread::Entity, thread::Column::WorkspaceId))),
                            )
                            .and(
                                Expr::col(("d", task_delivery::Column::TargetThreadId))
                                    .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
                            )
                            .and(
                                Expr::col(("d", task_delivery::Column::Status))
                                    .eq(Expr::val("delivered")),
                            )
                            .and(
                                Expr::col(("v", compaction_event_revision::Column::CaptureOrder))
                                    .lte(Expr::Value(fence.event_order.into())),
                            )
                            .and(match fence.before {
                                Some(before) => history_before(
                                    Expr::col(("e", turn_event::Column::CreatedAt)),
                                    before,
                                ),
                                None => Expr::val(true).into(),
                            })
                            .and(
                                Expr::col(("v", compaction_event_revision::Column::ItemId)).eq(
                                    Expr::Value(
                                        pioneer_protocol::task_delivery_result_item_id("").into(),
                                    )
                                    .binary(
                                        BinOper::Custom("||"),
                                        Expr::col(("d", task_delivery::Column::Id)),
                                    ),
                                ),
                            )
                            .and(
                                Expr::col(("e", turn_event::Column::EventType)).eq(Expr::Value(
                                    pioneer_protocol::constants::events::ITEM_COMPLETED.into(),
                                )),
                            ),
                    )
                    .to_owned(),
            )
            .or(Expr::exists(
                Query::select()
                    .expr(Expr::val(1_i64))
                    .from_as(task::Entity, "task")
                    .and_where_option(fence.before.map(|before| {
                        history_before(Expr::col(("task", task::Column::CreatedAt)), before)
                    }))
                    .join_as(
                        JoinType::InnerJoin,
                        task_run::Entity,
                        "run",
                        Expr::col(("run", task_run::Column::TaskId))
                            .eq(Expr::col(("task", task::Column::Id))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        turn::Entity,
                        "occurrence",
                        Expr::col(("occurrence", turn::Column::Id))
                            .eq(Expr::col(("run", task_run::Column::Id)))
                            .and(
                                Expr::col(("occurrence", turn::Column::ThreadId))
                                    .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
                            )
                            .and(
                                Expr::col(("occurrence", turn::Column::TurnKind))
                                    .eq(Expr::val("task_run")),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        turn_event::Entity,
                        "e",
                        Expr::col(("e", turn_event::Column::TurnId))
                            .eq(Expr::col(("occurrence", turn::Column::Id)))
                            .and(
                                Expr::col(("e", turn_event::Column::ThreadId))
                                    .eq(Expr::col(("occurrence", turn::Column::ThreadId))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        compaction_event_revision::Entity,
                        "v",
                        Expr::col(("v", compaction_event_revision::Column::SourceId))
                            .eq(Expr::col(("e", turn_event::Column::Id)))
                            .and(
                                Expr::col(("v", compaction_event_revision::Column::TurnId))
                                    .eq(Expr::col(("e", turn_event::Column::TurnId))),
                            )
                            .and(
                                Expr::col(("v", compaction_event_revision::Column::Present))
                                    .eq(Expr::val(1_i64)),
                            ),
                    )
                    .and_where(
                        Expr::col(("task", task::Column::WorkspaceId))
                            .eq(Expr::col((thread::Entity, thread::Column::WorkspaceId)))
                            .and(
                                Expr::col(("task", task::Column::CreatedByThreadId))
                                    .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
                            )
                            .and(
                                Expr::col(("task", task::Column::CreatedByTurnId))
                                    .eq(Expr::col((turn::Entity, turn::Column::Id))),
                            )
                            .and(
                                Expr::col(("v", compaction_event_revision::Column::CaptureOrder))
                                    .lte(Expr::Value(fence.event_order.into())),
                            )
                            .and(match fence.before {
                                Some(before) => history_before(
                                    Expr::col(("e", turn_event::Column::CreatedAt)),
                                    before,
                                ),
                                None => Expr::val(true).into(),
                            })
                            .and(Expr::col(("e", turn_event::Column::EventType)).is_in([
                                Expr::Value(
                                    pioneer_protocol::constants::events::TURN_FAILED.into(),
                                ),
                                Expr::Value(
                                    pioneer_protocol::constants::events::TURN_BLOCKED.into(),
                                ),
                            ])),
                    )
                    .to_owned(),
            )),
            "delivered_outcome",
        )
        .filter(
            Expr::col((thread::Entity, thread::Column::WorkspaceId))
                .eq(Expr::Value(workspace.into()))
                .and(
                    Expr::col((turn::Entity, turn::Column::ThreadId))
                        .eq(Expr::Value(thread.into())),
                )
                .and(Expr::col((turn::Entity, turn::Column::Id)).eq(Expr::Value(turn.into()))),
        )
        .into_model::<HistoryCausalBoundary>()
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("causal history scope unavailable"))?;
    if boundary.delegated_command && !boundary.delivered_outcome {
        boundary.delivered_outcome = compaction_failed_delivery_command(
            db,
            workspace,
            thread,
            Some(turn),
            None,
            fence.event_order,
            fence.before,
        )
        .await?
        .is_some();
    }
    Ok(boundary)
}

pub use pioneer_crud_conversations::model_history::event_projection_metadata;

use sea_orm::QuerySelect;

use sea_orm::QueryOrder;

#[derive(FromQueryResult)]
struct BasisSnapshotMetadata {
    run_id: String,
    conversation_thread_id: String,
    history_json_payload_id: Option<String>,
}
