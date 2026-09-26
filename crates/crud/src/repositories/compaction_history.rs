//! Metadata-only capture fences for history projection. The fence limits later
//! discovery; payload reads still require workspace/thread scope and revisions.
use super::compaction::*;
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_compaction::SourceRef;
use pioneer_entity::{
    compaction_event_revision, compaction_input_revision, compaction_source_revision,
    compaction_turn_creation, task, task_delivery, task_run, task_run_conversation_snapshot,
    task_run_turn, thread, thread_lineage, turn, turn_event, turn_input, turn_llm_context,
};
use sea_orm::sea_query::{Alias, BinOper, Expr, ExprTrait, Func, JoinType, Order, Query};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sea_orm::{ConnectionTrait, FromQueryResult};

#[derive(Clone, Debug, FromQueryResult)]
pub struct HistoryReadFence {
    pub turn_order: i64,
    pub turn_id: String,
    pub input_order: i64,
    pub event_order: i64,
    pub context_order: i64,
}
/// Tool items are resolved by exact source references, not enumerated by the
/// history assembler. A physical turn_item rowid boundary is neither used nor
/// available when transparent compression exposes the source through a view.
#[derive(Clone, Debug, FromQueryResult)]
pub struct HistoryTurnBoundary {
    pub creation_order: i64,
    /// Tie-breaker for turns predating the creation-order trigger. This is
    /// ordering metadata only, never proof of source coverage or a read fence.
    pub legacy_creation_order: i64,
    pub id: String,
    pub created_at: String,
    pub status: String,
    pub turn_kind: String,
    pub send_mode: Option<String>,
    pub input_high_water: i64,
    pub event_high_water: i64,
    pub context_high_water: i64,
}
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

pub(crate) async fn compaction_turn_is_completed<C: ConnectionTrait>(
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
pub(crate) async fn compaction_legacy_task_basis_source<C: ConnectionTrait>(
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
pub(crate) async fn compaction_task_basis_thread<C: ConnectionTrait>(
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
pub(crate) async fn compaction_latest_task_basis_turn<C: ConnectionTrait>(
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
/// capacity. Legacy arrays retain their original bytes until migration.
pub(crate) async fn compaction_task_basis_snapshot<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> Result<Option<AcceptedTaskBasis>> {
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
        .expr(Expr::col((
            task_run_conversation_snapshot::Entity,
            task_run_conversation_snapshot::Column::CreatedAt,
        )))
        .expr_as(
            Expr::expr(
                Func::cust(Alias::new("length")).args([Expr::col((
                    task_run_conversation_snapshot::Entity,
                    task_run_conversation_snapshot::Column::HistoryJson,
                ))
                .cast_as(Alias::new("BLOB"))]),
            ),
            "history_bytes",
        )
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
        .into_model::<BasisSnapshotMetadata>()
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let run_id: String = row.run_id;
    let parent_thread: String = row.conversation_thread_id;
    let created_at: String = row.created_at;
    let history_bytes = usize::try_from(row.history_bytes)?;
    let mut bytes = Vec::new();
    bytes.try_reserve(history_bytes)?;
    while bytes.len() < history_bytes {
        let count = SOURCE_PAGE_BYTES.min(history_bytes - bytes.len());
        let row = task_run_turn::Entity::find()
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
            .expr_as(
                Expr::expr(
                    Func::cust(Alias::new("substr")).args([
                        Expr::col((
                            task_run_conversation_snapshot::Entity,
                            task_run_conversation_snapshot::Column::HistoryJson,
                        ))
                        .cast_as(Alias::new("BLOB")),
                        Expr::Value(i64::try_from(bytes.len() + 1)?.into()),
                        Expr::Value(i64::try_from(count)?.into()),
                    ]),
                ),
                "fragment",
            )
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
                    )
                    .and(
                        Expr::col((
                            task_run_conversation_snapshot::Entity,
                            task_run_conversation_snapshot::Column::RunId,
                        ))
                        .eq(Expr::Value(run_id.clone().into())),
                    )
                    .and(
                        Expr::col((
                            task_run_conversation_snapshot::Entity,
                            task_run_conversation_snapshot::Column::ConversationThreadId,
                        ))
                        .eq(Expr::Value(parent_thread.clone().into())),
                    )
                    .and(
                        Expr::col((
                            task_run_conversation_snapshot::Entity,
                            task_run_conversation_snapshot::Column::CreatedAt,
                        ))
                        .eq(Expr::Value(created_at.clone().into())),
                    )
                    .and(
                        Expr::expr(
                            Func::cust(Alias::new("length")).args([Expr::col((
                                task_run_conversation_snapshot::Entity,
                                task_run_conversation_snapshot::Column::HistoryJson,
                            ))
                            .cast_as(Alias::new("BLOB"))]),
                        )
                        .eq(Expr::Value(i64::try_from(history_bytes)?.into())),
                    ),
            )
            .into_tuple::<Vec<u8>>()
            .one(db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("accepted Task basis changed during read"))?;
        let fragment = row;
        ensure!(
            fragment.len() == count,
            "accepted Task basis fragment is incomplete"
        );
        bytes.extend(fragment);
    }
    Ok(Some(AcceptedTaskBasis {
        parent_thread,
        run_id,
        history_json: String::from_utf8(bytes)?,
    }))
}

/// Refresh a stale metadata cache after decoding a scoped canonical source
/// outside database capacity. The revision CAS prevents a later event edit
/// from being labelled using the earlier typed payload.
pub(crate) async fn compaction_record_event_projection<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    source: &SourceRef,
    event: &crate::CanonicalTurnEventPayload,
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
pub(crate) async fn compaction_task_delivery_command(
    store: &CrudStore,
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
                .and(
                    Expr::col((task::Entity, task::Column::CreatedByTurnId))
                        .binary(BinOper::Is, Expr::val(Option::<String>::None))
                        .not(),
                ),
        )
        .into_tuple::<String>()
        .one(&store.connection)
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
        .one(&store.connection)
        .await?;
    if let Some(row) = row {
        return Ok(Some(row));
    }
    store
        .compaction_failed_delivery_command(workspace, thread, None, Some(source), i64::MAX)
        .await
}

/// Generic failed deliveries use a deterministic Turn ID, checked after
/// releasing DB capacity. Page only delivery identities whose exact failure
/// event is visible; never infer an outcome from mutable TaskRun status.
pub(crate) async fn compaction_failed_delivery_command<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    command: Option<&str>,
    source: Option<&SourceRef>,
    event_fence: i64,
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
                        Expr::Value(command.map(str::to_owned).into())
                            .binary(BinOper::Is, Expr::val(Option::<String>::None))
                            .or(Expr::col((task::Entity, task::Column::CreatedByTurnId))
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
                            // hides the primary-key lookup from SQLite and makes every
                            // historical message scan unrelated event revisions.
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
            if outcome == crate::canonical_agent_id('T', &format!("task-delivery-turn\0{delivery}"))
            {
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
pub(crate) async fn compaction_history_causal_boundary(
    store: &CrudStore,
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
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("causal history scope unavailable"))?;
    if boundary.delegated_command && !boundary.delivered_outcome {
        boundary.delivered_outcome = store
            .compaction_failed_delivery_command(
                workspace,
                thread,
                Some(turn),
                None,
                fence.event_order,
            )
            .await?
            .is_some();
    }
    Ok(boundary)
}

/// One read fixes the append boundary before enumerating any turns. Source
/// bounds use retained metadata with explicit insertion order; deleting or
/// vacuuming canonical rows cannot change that order. MAX uses indexes and
/// reads no payload. Every subsequent discovery checks its workspace scope.
pub(crate) async fn compaction_history_read_fence<C: ConnectionTrait>(
    db: &C,
) -> Result<HistoryReadFence> {
    // The aggregate base yields one row even for an empty database; all six
    // boundaries are still captured by one SQLite read snapshot.
    compaction_turn_creation::Entity::find()
        .select_only()
        .expr_as(
            Func::coalesce([
                Func::max(Expr::col(compaction_turn_creation::Column::Sequence)).into(),
                Expr::val(0_i64),
            ]),
            "turn_order",
        )
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(Func::cust(Alias::new("coalesce")).args([
                            Expr::expr(
                                Func::cust(Alias::new("max")).args([Expr::col(turn::Column::Id)]),
                            ),
                            Expr::val(""),
                        ])))
                        .from(turn::Entity)
                        .to_owned()
                        .into(),
                ),
            ),
            "turn_id",
        )
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(Func::cust(Alias::new("coalesce")).args([
                            Expr::expr(Func::cust(Alias::new("max")).args([Expr::col(
                                compaction_input_revision::Column::CaptureOrder,
                            )])),
                            Expr::val(0_i64),
                        ])))
                        .from(compaction_input_revision::Entity)
                        .to_owned()
                        .into(),
                ),
            ),
            "input_order",
        )
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(Func::cust(Alias::new("coalesce")).args([
                            Expr::expr(Func::cust(Alias::new("max")).args([Expr::col(
                                compaction_event_revision::Column::CaptureOrder,
                            )])),
                            Expr::val(0_i64),
                        ])))
                        .from(compaction_event_revision::Entity)
                        .to_owned()
                        .into(),
                ),
            ),
            "event_order",
        )
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(Func::cust(Alias::new("coalesce")).args([
                            Expr::expr(Func::cust(Alias::new("max")).args([Expr::col(
                                compaction_source_revision::Column::CaptureOrder,
                            )])),
                            Expr::val(0_i64),
                        ])))
                        .from(compaction_source_revision::Entity)
                        .to_owned()
                        .into(),
                ),
            ),
            "context_order",
        )
        .into_model::<HistoryReadFence>()
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("history read fence missing"))
}

/// Discover at most 128 metadata rows, including active turns. Eligibility
/// is determined from complete canonical rounds/events below the captured
/// fence, never by a later mutable terminal status alone.
pub(crate) async fn compaction_history_turn_page<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    after: &str,
    fence: &HistoryReadFence,
) -> Result<Vec<HistoryTurnBoundary>> {
    compaction_history_turn_page_inner(db, workspace, thread, after, fence, None).await
}

/// The accepted manifest already names exact turns. Restrict the metadata
/// query before its per-turn input/event/context boundary subqueries run.
pub(crate) async fn compaction_history_selected_turn_page<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    selected: &[String],
    fence: &HistoryReadFence,
) -> Result<Vec<HistoryTurnBoundary>> {
    anyhow::ensure!(
        selected.len() <= 64,
        "selected history turn page exceeds row bound"
    );
    anyhow::ensure!(
        selected.iter().map(String::len).sum::<usize>() <= SOURCE_PAGE_BYTES,
        "selected history turn page exceeds byte bound"
    );
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    compaction_history_turn_page_inner(db, workspace, thread, "", fence, Some(selected)).await
}

async fn compaction_history_turn_page_inner<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    after: &str,
    fence: &HistoryReadFence,
    selected: Option<&[String]>,
) -> Result<Vec<HistoryTurnBoundary>> {
    // Preserve the scoped-reader contract: an unavailable/foreign thread has
    // no rows. Only an accessible, incompletely prepared history is an error.
    if thread::Entity::find_by_id(thread)
        .filter(thread::Column::WorkspaceId.eq(workspace))
        .select_only()
        .column(thread::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_none()
    {
        return Ok(Vec::new());
    }
    use pioneer_entity::compaction_history_preparation as preparation;
    anyhow::ensure!(
        preparation::Entity::find_by_id(thread)
            .filter(preparation::Column::Ready.eq(1))
            .filter(preparation::Column::InputOrder.lte(fence.input_order))
            .filter(preparation::Column::EventOrder.lte(fence.event_order))
            .filter(preparation::Column::ContextOrder.lte(fence.context_order))
            .one(db)
            .await?
            .is_some(),
        "compaction history preparation is required before capturing a read fence"
    );
    let mut query = turn::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .expr(Expr::col((turn::Entity, turn::Column::Id)))
        .expr_as(
            Expr::expr(
                Func::cust(Alias::new("coalesce")).args([
                    Expr::SubQuery(
                        None,
                        Box::new(
                            Query::select()
                                .expr(Expr::col(compaction_turn_creation::Column::Sequence))
                                .from(compaction_turn_creation::Entity)
                                .and_where(
                                    Expr::col(compaction_turn_creation::Column::TurnId)
                                        .eq(Expr::col((turn::Entity, turn::Column::Id))),
                                )
                                .to_owned()
                                .into(),
                        ),
                    ),
                    Expr::val(0_i64),
                ]),
            ),
            "creation_order",
        )
        .expr_as(
            Expr::col((turn::Entity, Alias::new("rowid"))),
            "legacy_creation_order",
        )
        .expr(Expr::col((turn::Entity, turn::Column::CreatedAt)))
        .expr(Expr::col((turn::Entity, turn::Column::Status)))
        .expr(Expr::col((turn::Entity, turn::Column::TurnKind)))
        .expr(Expr::col((turn::Entity, turn::Column::SendMode)))
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(
                            Func::cust(Alias::new("coalesce")).args([
                                Expr::expr(
                                    Func::cust(Alias::new("max")).args([Expr::col((
                                        "s",
                                        turn_input::Column::InputIndex,
                                    ))
                                    .add(Expr::val(1_i64))]),
                                ),
                                Expr::val(0_i64),
                            ]),
                        ))
                        .from_as(turn_input::Entity, "s")
                        .join_as(
                            JoinType::InnerJoin,
                            compaction_input_revision::Entity,
                            "r",
                            Expr::col(("r", compaction_input_revision::Column::SourceId))
                                .eq(Expr::col(("s", turn_input::Column::Id)))
                                .and(
                                    Expr::col(("r", compaction_input_revision::Column::TurnId))
                                        .eq(Expr::col(("s", turn_input::Column::TurnId))),
                                )
                                .and(
                                    Expr::col(("r", compaction_input_revision::Column::Present))
                                        .eq(Expr::val(1_i64)),
                                ),
                        )
                        .and_where(
                            Expr::col(("s", turn_input::Column::TurnId))
                                .eq(Expr::col((turn::Entity, turn::Column::Id)))
                                .and(
                                    Expr::col((
                                        "r",
                                        compaction_input_revision::Column::CaptureOrder,
                                    ))
                                    .lte(Expr::Value(fence.input_order.into())),
                                ),
                        )
                        .to_owned()
                        .into(),
                ),
            ),
            "input_high_water",
        )
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(
                            Func::cust(Alias::new("coalesce")).args([
                                Expr::expr(
                                    Func::cust(Alias::new("max"))
                                        .args([Expr::col(("s", turn_event::Column::Sequence))]),
                                ),
                                Expr::val(0_i64),
                            ]),
                        ))
                        .from_as(turn_event::Entity, "s")
                        .join_as(
                            JoinType::InnerJoin,
                            compaction_event_revision::Entity,
                            "r",
                            Expr::col(("r", compaction_event_revision::Column::SourceId))
                                .eq(Expr::col(("s", turn_event::Column::Id)))
                                .and(
                                    Expr::col(("r", compaction_event_revision::Column::TurnId))
                                        .eq(Expr::col(("s", turn_event::Column::TurnId))),
                                )
                                .and(
                                    Expr::col(("r", compaction_event_revision::Column::Present))
                                        .eq(Expr::val(1_i64)),
                                ),
                        )
                        .and_where(
                            Expr::col(("s", turn_event::Column::TurnId))
                                .eq(Expr::col((turn::Entity, turn::Column::Id)))
                                .and(
                                    Expr::col((
                                        "r",
                                        compaction_event_revision::Column::CaptureOrder,
                                    ))
                                    .lte(Expr::Value(fence.event_order.into())),
                                ),
                        )
                        .to_owned()
                        .into(),
                ),
            ),
            "event_high_water",
        )
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(Func::cust(Alias::new("coalesce")).args(
                            [
                                Expr::expr(
                                    Func::cust(Alias::new("max")).args([Expr::col((
                                        "s",
                                        turn_llm_context::Column::Sequence,
                                    ))]),
                                ),
                                Expr::val(0_i64),
                            ],
                        )))
                        .from_as(turn_llm_context::Entity, "s")
                        .join_as(
                            JoinType::InnerJoin,
                            compaction_source_revision::Entity,
                            "r",
                            Expr::col(("r", compaction_source_revision::Column::SourceId))
                                .eq(Expr::col(("s", turn_llm_context::Column::Id)))
                                .and(
                                    Expr::col(("r", compaction_source_revision::Column::TurnId))
                                        .eq(Expr::col(("s", turn_llm_context::Column::TurnId))),
                                )
                                .and(
                                    Expr::col(("r", compaction_source_revision::Column::Present))
                                        .eq(Expr::val(1_i64)),
                                ),
                        )
                        .and_where(
                            Expr::col(("s", turn_llm_context::Column::TurnId))
                                .eq(Expr::col((turn::Entity, turn::Column::Id)))
                                .and(
                                    Expr::col((
                                        "r",
                                        compaction_source_revision::Column::CaptureOrder,
                                    ))
                                    .lte(Expr::Value(fence.context_order.into())),
                                ),
                        )
                        .to_owned()
                        .into(),
                ),
            ),
            "context_high_water",
        )
        .filter(
            Expr::col((thread::Entity, thread::Column::WorkspaceId))
                .eq(Expr::Value(workspace.into()))
                .and(Expr::col((thread::Entity, thread::Column::Id)).eq(Expr::Value(thread.into())))
                .and(Expr::col((turn::Entity, turn::Column::Id)).gt(Expr::Value(after.into())))
                .and(
                    Expr::col((turn::Entity, turn::Column::Id))
                        .lte(Expr::Value(fence.turn_id.clone().into())),
                ),
        );
    if let Some(selected) = selected {
        query = query.filter(turn::Column::Id.is_in(selected.iter().cloned()));
    }
    Ok(query
        .order_by(Expr::col((turn::Entity, turn::Column::Id)), Order::Asc)
        .limit(128)
        .into_model::<HistoryTurnBoundary>()
        .all(db)
        .await?)
}

/// Computed from the typed event before a writer is acquired. These identities
/// let projection skip technical copies without parsing their retained bodies.
pub fn event_projection_metadata(
    event: &crate::CanonicalTurnEventPayload,
) -> (Option<String>, &'static str) {
    use crate::CanonicalTurnEventPayload as Event;
    use pioneer_protocol::TurnItem;
    let (item, update) = match event {
        Event::ItemStarted(value) => {
            let kind = if matches!(&value.item, TurnItem::SystemEvent {code:Some(code), ..} if code=="agent_context_compaction")
            {
                "technical"
            } else {
                "start"
            };
            return (Some(value.item.item_id().into()), kind);
        }
        Event::ItemCompleted(value) => (&value.item, false),
        Event::ItemUpdated(value) => (&value.item, true),
        Event::TurnStarted(_) => return (None, "input"),
        Event::TurnMessageEdited(_) => return (None, "input_revision"),
        Event::TurnMessageDeleted(_) => return (None, "input_deleted"),
        _ => {
            let kind = if crate::canonical_event_model_projection(event).is_omitted() {
                "technical"
            } else {
                "status"
            };
            return (None, kind);
        }
    };
    let kind = match item {
        TurnItem::SystemEvent { code, .. }
            if code.as_deref() == Some("agent_context_compaction") =>
        {
            "technical"
        }
        // Input-copy identities are structural even when their visible body is
        // empty. Revisions/deletions use them to suppress stale text and
        // attachment references.
        TurnItem::UserMessage { .. } => "input_copy",
        _ if crate::canonical_event_model_projection(event).is_omitted() => "technical",
        _ if update => "update",
        TurnItem::Reasoning { .. } => "reasoning",
        TurnItem::AgentMessage { .. } => "assistant",
        TurnItem::SystemEvent { .. } => "observation",
        _ => "tool_observation",
    };
    (Some(item.item_id().into()), kind)
}

use sea_orm::QuerySelect;

use super::compaction_live_sources;

use sea_orm::QueryOrder;

#[derive(FromQueryResult)]
struct BasisSnapshotMetadata {
    run_id: String,
    conversation_thread_id: String,
    created_at: String,
    history_bytes: i64,
}
