//! Exact initial Task input aliases against the accepted parent snapshot.
use anyhow::Result;
use pioneer_entity::{
    compaction_input_revision, task, task_run, task_run_conversation_snapshot, task_run_turn,
    thread, thread_lineage, turn, turn_input,
};
use pioneer_protocol::{TASK_COMPOSER_WORK_VERSION, TaskMetadata, UserInput};
use sea_orm::sea_query::{Expr, ExprTrait, Func, JoinType, Query};
use sea_orm::{ConnectionTrait, FromQueryResult};

#[derive(Clone, Debug, FromQueryResult)]
pub struct TaskInputCopyAlias {
    pub original_thread_id: String,
    pub original_turn_id: String,
    pub original_id: String,
    pub original_revision: i64,
    pub copy_thread_id: String,
    pub copy_turn_id: String,
    pub copy_id: String,
    pub copy_revision: i64,
    original_input_index: i64,
    copy_input_index: i64,
    original_payload: String,
    copy_payload: String,
    original_message_revision: i64,
    copy_message_revision: i64,
    task_metadata_json: String,
}

/// Resolve only concrete initial-Task input copies among the supplied
/// canonical input IDs. The execution turn, run, Task creator, immutable
/// conversation snapshot, parent/child lineage, and immutable Composer launch
/// must all agree. The current source and parent rows must still be the
/// unedited inputs at the exact launch index. Ordinary synthetic Task prompts
/// carry no such copy proof and are never inferred from equal text. The result
/// is bounded to one row per supplied copy ID; an overfull relationship page
/// is rejected conservatively.
pub async fn compaction_task_input_copy_aliases<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    input_ids: &[String],
) -> Result<Vec<TaskInputCopyAlias>> {
    if input_ids.is_empty() {
        return Ok(Vec::new());
    }
    let original_match_count = Query::select()
        .expr(Func::count(Expr::col((
            "original_match",
            turn_input::Column::Id,
        ))))
        .from_as(turn_input::Entity, "original_match")
        .join_as(
            JoinType::InnerJoin,
            compaction_input_revision::Entity,
            "original_match_revision",
            Expr::col((
                "original_match_revision",
                compaction_input_revision::Column::SourceId,
            ))
            .eq(Expr::col(("original_match", turn_input::Column::Id)))
            .and(
                Expr::col((
                    "original_match_revision",
                    compaction_input_revision::Column::TurnId,
                ))
                .eq(Expr::col(("original_match", turn_input::Column::TurnId))),
            )
            .and(
                Expr::col((
                    "original_match_revision",
                    compaction_input_revision::Column::Present,
                ))
                .eq(Expr::val(1_i64)),
            ),
        )
        .and_where(
            Expr::col(("original_match", turn_input::Column::TurnId))
                .eq(Expr::col((
                    "snapshot",
                    task_run_conversation_snapshot::Column::SourceTurnId,
                )))
                .and(
                    Expr::col(("original_match", turn_input::Column::Payload))
                        .eq(Expr::col(("copy", turn_input::Column::Payload))),
                ),
        )
        .to_owned();
    let copy_match_count = Query::select()
        .expr(Func::count(Expr::col((
            "copy_match",
            turn_input::Column::Id,
        ))))
        .from_as(turn_input::Entity, "copy_match")
        .join_as(
            JoinType::InnerJoin,
            compaction_input_revision::Entity,
            "copy_match_revision",
            Expr::col((
                "copy_match_revision",
                compaction_input_revision::Column::SourceId,
            ))
            .eq(Expr::col(("copy_match", turn_input::Column::Id)))
            .and(
                Expr::col((
                    "copy_match_revision",
                    compaction_input_revision::Column::TurnId,
                ))
                .eq(Expr::col(("copy_match", turn_input::Column::TurnId))),
            )
            .and(
                Expr::col((
                    "copy_match_revision",
                    compaction_input_revision::Column::Present,
                ))
                .eq(Expr::val(1_i64)),
            ),
        )
        .and_where(
            Expr::col(("copy_match", turn_input::Column::TurnId))
                .eq(Expr::col(("copy", turn_input::Column::TurnId)))
                .and(
                    Expr::col(("copy_match", turn_input::Column::Payload))
                        .eq(Expr::col(("copy", turn_input::Column::Payload))),
                ),
        )
        .to_owned();
    let query = Query::select()
        .from_as(turn_input::Entity, "copy")
        .join_as(
            JoinType::InnerJoin,
            task_run_turn::Entity,
            "run_turn",
            Expr::col(("run_turn", task_run_turn::Column::TurnId))
                .eq(Expr::col(("copy", turn_input::Column::TurnId))),
        )
        .join_as(
            JoinType::InnerJoin,
            task_run::Entity,
            "run",
            Expr::col(("run", task_run::Column::Id))
                .eq(Expr::col(("run_turn", task_run_turn::Column::RunId)))
                .and(
                    Expr::col(("run", task_run::Column::TaskId))
                        .eq(Expr::col(("run_turn", task_run_turn::Column::TaskId))),
                ),
        )
        .join_as(
            JoinType::InnerJoin,
            task_run_conversation_snapshot::Entity,
            "snapshot",
            Expr::col(("snapshot", task_run_conversation_snapshot::Column::RunId))
                .eq(Expr::col(("run", task_run::Column::Id)))
                .and(
                    Expr::col(("snapshot", task_run_conversation_snapshot::Column::TaskId))
                        .eq(Expr::col(("run", task_run::Column::TaskId))),
                ),
        )
        .join_as(
            JoinType::InnerJoin,
            task::Entity,
            "task_copy",
            Expr::col(("task_copy", task::Column::Id))
                .eq(Expr::col(("run_turn", task_run_turn::Column::TaskId)))
                .and(
                    Expr::col(("task_copy", task::Column::WorkspaceId)).eq(Expr::col((
                        "snapshot",
                        task_run_conversation_snapshot::Column::WorkspaceId,
                    ))),
                )
                .and(
                    Expr::col(("task_copy", task::Column::CreatedByTurnId)).eq(Expr::col((
                        "snapshot",
                        task_run_conversation_snapshot::Column::SourceTurnId,
                    ))),
                )
                .and(
                    Expr::col(("task_copy", task::Column::CreatedByThreadId)).eq(Expr::col((
                        "snapshot",
                        task_run_conversation_snapshot::Column::ConversationThreadId,
                    ))),
                ),
        )
        .join_as(
            JoinType::InnerJoin,
            thread_lineage::Entity,
            "lineage",
            Expr::col(("lineage", thread_lineage::Column::ChildThreadId))
                .eq(Expr::col(("run_turn", task_run_turn::Column::ThreadId)))
                .and(
                    Expr::col(("lineage", thread_lineage::Column::ParentThreadId)).eq(Expr::col((
                        "snapshot",
                        task_run_conversation_snapshot::Column::ConversationThreadId,
                    ))),
                ),
        )
        .join_as(
            JoinType::InnerJoin,
            thread::Entity,
            "child_thread",
            Expr::col(("child_thread", thread::Column::Id))
                .eq(Expr::col(("run_turn", task_run_turn::Column::ThreadId))),
        )
        .join_as(
            JoinType::InnerJoin,
            thread::Entity,
            "parent_thread",
            Expr::col(("parent_thread", thread::Column::Id)).eq(Expr::col((
                "snapshot",
                task_run_conversation_snapshot::Column::ConversationThreadId,
            ))),
        )
        .join_as(
            JoinType::InnerJoin,
            turn::Entity,
            "child_turn",
            Expr::col(("child_turn", turn::Column::Id))
                .eq(Expr::col(("copy", turn_input::Column::TurnId)))
                .and(
                    Expr::col(("child_turn", turn::Column::ThreadId))
                        .eq(Expr::col(("run_turn", task_run_turn::Column::ThreadId))),
                ),
        )
        .join_as(
            JoinType::InnerJoin,
            turn::Entity,
            "parent_turn",
            Expr::col(("parent_turn", turn::Column::Id))
                .eq(Expr::col((
                    "snapshot",
                    task_run_conversation_snapshot::Column::SourceTurnId,
                )))
                .and(
                    Expr::col(("parent_turn", turn::Column::ThreadId)).eq(Expr::col((
                        "snapshot",
                        task_run_conversation_snapshot::Column::ConversationThreadId,
                    ))),
                ),
        )
        .join_as(
            JoinType::InnerJoin,
            turn_input::Entity,
            "original",
            Expr::col(("original", turn_input::Column::TurnId))
                .eq(Expr::col((
                    "snapshot",
                    task_run_conversation_snapshot::Column::SourceTurnId,
                )))
                .and(
                    Expr::col(("original", turn_input::Column::InputIndex))
                        .eq(Expr::col(("copy", turn_input::Column::InputIndex))),
                )
                .and(
                    Expr::col(("original", turn_input::Column::Payload))
                        .eq(Expr::col(("copy", turn_input::Column::Payload))),
                ),
        )
        .join_as(
            JoinType::InnerJoin,
            compaction_input_revision::Entity,
            "original_revision",
            Expr::col((
                "original_revision",
                compaction_input_revision::Column::SourceId,
            ))
            .eq(Expr::col(("original", turn_input::Column::Id)))
            .and(
                Expr::col((
                    "original_revision",
                    compaction_input_revision::Column::TurnId,
                ))
                .eq(Expr::col(("original", turn_input::Column::TurnId))),
            )
            .and(
                Expr::col((
                    "original_revision",
                    compaction_input_revision::Column::Present,
                ))
                .eq(Expr::val(1_i64)),
            ),
        )
        .join_as(
            JoinType::InnerJoin,
            compaction_input_revision::Entity,
            "copy_revision",
            Expr::col(("copy_revision", compaction_input_revision::Column::SourceId))
                .eq(Expr::col(("copy", turn_input::Column::Id)))
                .and(
                    Expr::col(("copy_revision", compaction_input_revision::Column::TurnId))
                        .eq(Expr::col(("copy", turn_input::Column::TurnId))),
                )
                .and(
                    Expr::col(("copy_revision", compaction_input_revision::Column::Present))
                        .eq(Expr::val(1_i64)),
                ),
        )
        .expr_as(
            Expr::col((
                "snapshot",
                task_run_conversation_snapshot::Column::ConversationThreadId,
            )),
            "original_thread_id",
        )
        .expr_as(
            Expr::col((
                "snapshot",
                task_run_conversation_snapshot::Column::SourceTurnId,
            )),
            "original_turn_id",
        )
        .expr_as(
            Expr::col(("original", turn_input::Column::Id)),
            "original_id",
        )
        .expr_as(
            Expr::col((
                "original_revision",
                compaction_input_revision::Column::Revision,
            )),
            "original_revision",
        )
        .expr_as(
            Expr::col(("run_turn", task_run_turn::Column::ThreadId)),
            "copy_thread_id",
        )
        .expr_as(
            Expr::col(("copy", turn_input::Column::TurnId)),
            "copy_turn_id",
        )
        .expr_as(Expr::col(("copy", turn_input::Column::Id)), "copy_id")
        .expr_as(
            Expr::col(("copy_revision", compaction_input_revision::Column::Revision)),
            "copy_revision",
        )
        .expr_as(
            Expr::col(("original", turn_input::Column::InputIndex)),
            "original_input_index",
        )
        .expr_as(
            Expr::col(("copy", turn_input::Column::InputIndex)),
            "copy_input_index",
        )
        .expr_as(
            Expr::col(("original", turn_input::Column::Payload)),
            "original_payload",
        )
        .expr_as(
            Expr::col(("copy", turn_input::Column::Payload)),
            "copy_payload",
        )
        .expr_as(
            Expr::col(("parent_turn", turn::Column::MessageRevision)),
            "original_message_revision",
        )
        .expr_as(
            Expr::col(("child_turn", turn::Column::MessageRevision)),
            "copy_message_revision",
        )
        .expr_as(
            Expr::col(("task_copy", task::Column::MetadataJson)).cast_as("text"),
            "task_metadata_json",
        )
        .and_where(
            Expr::col(("copy", turn_input::Column::Id))
                .is_in(input_ids.iter().cloned())
                .and(Expr::col(("run_turn", task_run_turn::Column::Kind)).eq(Expr::val("initial")))
                .and(
                    Expr::col((
                        "snapshot",
                        task_run_conversation_snapshot::Column::WorkspaceId,
                    ))
                    .eq(Expr::Value(workspace.into())),
                )
                .and(
                    Expr::col(("child_thread", thread::Column::WorkspaceId))
                        .eq(Expr::Value(workspace.into())),
                )
                .and(
                    Expr::col(("parent_thread", thread::Column::WorkspaceId))
                        .eq(Expr::Value(workspace.into())),
                )
                .and(Expr::col(("task_copy", task::Column::MetadataJson)).is_not_null())
                .and(Expr::SubQuery(None, Box::new(original_match_count.into())).eq(1_i64))
                .and(Expr::SubQuery(None, Box::new(copy_match_count.into())).eq(1_i64)),
        )
        .limit(u64::try_from(input_ids.len())?.saturating_add(1))
        .to_owned();
    let rows = TaskInputCopyAlias::find_by_statement(db.get_database_backend().build(&query))
        .all(db)
        .await?;
    if rows.len() > input_ids.len() {
        // Multiple durable Task/run relationships claimed at least one copy.
        // Do not turn a bounded prefix into false uniqueness.
        return Ok(Vec::new());
    }
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let metadata = serde_json::from_str::<TaskMetadata>(&row.task_metadata_json).ok()?;
            let composer = metadata.composer_work?;
            let index = usize::try_from(row.copy_input_index).ok()?;
            let original = serde_json::from_str::<UserInput>(&row.original_payload).ok()?;
            let copy = serde_json::from_str::<UserInput>(&row.copy_payload).ok()?;
            (composer.version == TASK_COMPOSER_WORK_VERSION
                && row.original_message_revision == 0
                && row.copy_message_revision == 0
                && row.original_input_index == row.copy_input_index
                && composer.launch.thread_id == row.original_thread_id
                && composer.launch.turn_id == row.original_turn_id
                && composer.launch.input.get(index) == Some(&original)
                && original == copy)
                .then_some(row)
        })
        .collect())
}
