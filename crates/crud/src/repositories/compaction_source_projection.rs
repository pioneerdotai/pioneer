//! Operation ownership is admitted from a ready reference manifest, never from
//! a caller's claim that an arbitrary foreign source is its own work.
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_compaction::frozen::FrozenHistoryRef;
use pioneer_entity::{
    compaction_context, compaction_frozen_history, compaction_operation,
    compaction_operation_projection, compaction_runner_plan, task, task_run,
    task_run_conversation_snapshot, task_run_turn, thread_lineage, turn,
};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, OnConflict, Query};
use sea_orm::{ConnectionTrait, TransactionTrait};

pub(crate) async fn compaction_bound_source_projection<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<Option<FrozenHistoryRef>> {
    use pioneer_entity::{
        compaction_frozen_history as history, compaction_operation_projection as projection,
    };
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect};
    let row = projection::Entity::find_by_id(operation)
        .inner_join(history::Entity)
        .select_only()
        .column_as(Expr::col((history::Entity, history::Column::Id)), "id")
        .column_as(
            Expr::col((history::Entity, history::Column::IdentitySha256)),
            "identity_sha256",
        )
        .column_as(
            Expr::col((history::Entity, history::Column::MessageCount)),
            "message_count",
        )
        .filter(history::Column::Ready.eq(1_i64))
        .filter(
            Expr::col((history::Entity, history::Column::IdentitySha256)).eq(Expr::col((
                projection::Entity,
                projection::Column::IdentitySha256,
            ))),
        )
        .filter(
            Expr::col((history::Entity, history::Column::ImportsSha256)).eq(Expr::col((
                projection::Entity,
                projection::Column::ImportsSha256,
            ))),
        )
        .filter(
            Expr::col((history::Entity, history::Column::ImportCount)).eq(Expr::col((
                projection::Entity,
                projection::Column::ImportCount,
            ))),
        )
        .filter(
            Expr::col((history::Entity, history::Column::NextImport)).eq(Expr::col((
                projection::Entity,
                projection::Column::ImportCount,
            ))),
        )
        .into_tuple::<(String, String, i64)>()
        .one(db)
        .await?;
    row.map(|(manifest_id, identity_sha256, messages)| {
        Ok(FrozenHistoryRef {
            format: 1,
            manifest_id,
            identity_sha256,
            messages: u64::try_from(messages)?,
        })
    })
    .transpose()
}

/// Serialization is outside writer capacity. The writer revalidates the
/// ready manifest and exact execution/TaskRun snapshot before storing its
/// identity. This immutable binding also survives operation recovery.
pub(crate) async fn compaction_bind_source_projection(
    store: &CrudStore,
    operation: &str,
    descriptor: &FrozenHistoryRef,
) -> Result<()> {
    ensure!(descriptor.format == 1, "unsupported source projection");
    let json = serde_json::to_string(descriptor)?;
    ensure!(
        json.len() <= super::compaction::SOURCE_PAGE_BYTES,
        "oversized source projection"
    );
    let count = i64::try_from(descriptor.messages)?;
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            // A parent manifest is eligible only through the exact accepted
            // TaskRun basis for this execution. Output candidate selection
            // remains a separate boundary; it excludes reviewer outputs.
            // Dependency validation and binding share the existing transaction.
            if let Some((
                operation_id,
                manifest_id,
                identity_sha256,
                imports_sha256,
                import_count,
            )) = compaction_operation::Entity::find()
                .select_only()
                .join(
                    JoinType::InnerJoin,
                    compaction_operation::Entity::belongs_to(compaction_context::Entity)
                        .from(compaction_operation::Column::Owner)
                        .to(compaction_context::Column::Owner)
                        .into(),
                )
                .join(
                    JoinType::InnerJoin,
                    compaction_operation::Entity::belongs_to(turn::Entity)
                        .from(compaction_operation::Column::ExecutionTurn)
                        .to(turn::Column::Id)
                        .into(),
                )
                .join(
                    JoinType::InnerJoin,
                    compaction_context::Entity::belongs_to(compaction_frozen_history::Entity)
                        .from(compaction_context::Column::WorkspaceId)
                        .to(compaction_frozen_history::Column::WorkspaceId)
                        .into(),
                )
                .filter(
                    Expr::col((turn::Entity, turn::Column::ThreadId)).eq(Expr::col((
                        compaction_context::Entity,
                        compaction_context::Column::ThreadId,
                    ))),
                )
                .expr(Expr::col((
                    compaction_operation::Entity,
                    compaction_operation::Column::Id,
                )))
                .expr(Expr::col((
                    compaction_frozen_history::Entity,
                    compaction_frozen_history::Column::Id,
                )))
                .expr(Expr::col((
                    compaction_frozen_history::Entity,
                    compaction_frozen_history::Column::IdentitySha256,
                )))
                .expr(Expr::col((
                    compaction_frozen_history::Entity,
                    compaction_frozen_history::Column::ImportsSha256,
                )))
                .expr(Expr::col((
                    compaction_frozen_history::Entity,
                    compaction_frozen_history::Column::ImportCount,
                )))
                .filter(
                    Expr::col((
                        compaction_operation::Entity,
                        compaction_operation::Column::Id,
                    ))
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::Status,
                        ))
                        .eq(Expr::val("running")),
                    )
                    .and(
                        Expr::col((turn::Entity, turn::Column::Status))
                            .is_in(["interrupted", "cancelled"])
                            .not(),
                    )
                    .and(
                        Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::Id,
                        ))
                        .eq(Expr::Value(descriptor.manifest_id.clone().into())),
                    )
                    .and(
                        Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::IdentitySha256,
                        ))
                        .eq(Expr::Value(descriptor.identity_sha256.clone().into())),
                    )
                    .and(
                        Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::MessageCount,
                        ))
                        .eq(Expr::Value(count.into())),
                    )
                    .and(
                        Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::NextOrdinal,
                        ))
                        .eq(Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::MessageCount,
                        ))),
                    )
                    .and(
                        Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::Ready,
                        ))
                        .eq(Expr::val(1_i64)),
                    )
                    .and(
                        Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::ImportCount,
                        ))
                        .eq(Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::NextImport,
                        ))),
                    )
                    .and(
                        Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::OwnerThread,
                        ))
                        .eq(Expr::col((
                            compaction_context::Entity,
                            compaction_context::Column::ThreadId,
                        )))
                        .or(Expr::exists(
                            Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(task_run_turn::Entity, "rt")
                                .join_as(
                                    JoinType::InnerJoin,
                                    task_run::Entity,
                                    "r",
                                    Expr::col(("r", task_run::Column::Id))
                                        .eq(Expr::col(("rt", task_run_turn::Column::RunId)))
                                        .and(
                                            Expr::col(("r", task_run::Column::TaskId)).eq(
                                                Expr::col(("rt", task_run_turn::Column::TaskId)),
                                            ),
                                        ),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    task::Entity,
                                    "t",
                                    Expr::col(("t", task::Column::Id))
                                        .eq(Expr::col(("r", task_run::Column::TaskId)))
                                        .and(Expr::col(("t", task::Column::WorkspaceId)).eq(
                                            Expr::col((
                                                compaction_context::Entity,
                                                compaction_context::Column::WorkspaceId,
                                            )),
                                        )),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    thread_lineage::Entity,
                                    "lineage",
                                    Expr::col(("lineage", thread_lineage::Column::ChildThreadId))
                                        .eq(Expr::col(("rt", task_run_turn::Column::ThreadId)))
                                        .and(Expr::col(("lineage", thread_lineage::Column::ParentThreadId)).eq(
                                            Expr::col((
                                                compaction_frozen_history::Entity,
                                                compaction_frozen_history::Column::OwnerThread,
                                            )),
                                        )),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    task_run_conversation_snapshot::Entity,
                                    "basis",
                                    Expr::col((
                                        "basis",
                                        task_run_conversation_snapshot::Column::RunId,
                                    ))
                                    .eq(Expr::col(("r", task_run::Column::Id)))
                                    .and(
                                        Expr::col((
                                            "basis",
                                            task_run_conversation_snapshot::Column::TaskId,
                                        ))
                                        .eq(Expr::col(("t", task::Column::Id))),
                                    )
                                    .and(
                                        Expr::col(("basis", task_run_conversation_snapshot::Column::WorkspaceId)).eq(Expr::col((
                                            compaction_context::Entity,
                                            compaction_context::Column::WorkspaceId,
                                        ))),
                                    ),
                                )
                                .and_where(
                                    Expr::col(("rt", task_run_turn::Column::TurnId))
                                        .eq(Expr::col((turn::Entity, turn::Column::Id)))
                                        .and(Expr::col(("rt", task_run_turn::Column::ThreadId)).eq(
                                            Expr::col((
                                                compaction_context::Entity,
                                                compaction_context::Column::ThreadId,
                                            )),
                                        ))
                                        .and(Expr::col(("basis", task_run_conversation_snapshot::Column::ConversationThreadId)).eq(
                                            Expr::col((
                                                compaction_frozen_history::Entity,
                                                compaction_frozen_history::Column::OwnerThread,
                                            )),
                                        ))
                                        .and(
                                            Expr::col(("basis", task_run_conversation_snapshot::Column::HistoryJson))
                                                .eq(Expr::Value(json.clone().into())),
                                        ),
                                )
                                .to_owned(),
                        )),
                    )
                    .and(
                        Expr::exists(
                            Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(compaction_runner_plan::Entity, "p")
                                .and_where(
                                    Expr::col(("p", compaction_runner_plan::Column::OperationId))
                                        .eq(Expr::col((
                                            compaction_operation::Entity,
                                            compaction_operation::Column::Id,
                                        )))
                                        .and(Expr::col(("p", compaction_runner_plan::Column::Ready)).eq(Expr::val(1_i64))),
                                )
                                .to_owned(),
                        )
                        .not(),
                    ),
                )
                .into_tuple::<(String, String, String, String, i64)>()
                .one(&tx)
                .await?
            {
                compaction_operation_projection::Entity::insert(
                    compaction_operation_projection::ActiveModel {
                        operation_id: sea_orm::Set(operation_id),
                        manifest_id: sea_orm::Set(manifest_id),
                        identity_sha256: sea_orm::Set(identity_sha256),
                        imports_sha256: sea_orm::Set(imports_sha256),
                        import_count: sea_orm::Set(import_count),
                    },
                )
                .on_conflict(
                    OnConflict::columns([compaction_operation_projection::Column::OperationId])
                        .do_nothing()
                        .to_owned(),
                )
                .exec_without_returning(&tx)
                .await?;
            }
            ensure!(
                compaction_operation_projection::Entity::find()
                    .select_only()
                    .join(
                        JoinType::InnerJoin,
                        compaction_operation_projection::Entity::belongs_to(
                            compaction_frozen_history::Entity
                        )
                        .from(compaction_operation_projection::Column::ManifestId)
                        .to(compaction_frozen_history::Column::Id)
                        .into()
                    )
                    .expr(Expr::col((
                        compaction_operation_projection::Entity,
                        compaction_operation_projection::Column::OperationId
                    )))
                    .filter(
                        Expr::col((
                            compaction_operation_projection::Entity,
                            compaction_operation_projection::Column::OperationId
                        ))
                        .eq(Expr::Value(operation.into()))
                        .and(
                            Expr::col((
                                compaction_operation_projection::Entity,
                                compaction_operation_projection::Column::ManifestId
                            ))
                            .eq(Expr::Value(descriptor.manifest_id.clone().into()))
                        )
                        .and(
                            Expr::col((
                                compaction_operation_projection::Entity,
                                compaction_operation_projection::Column::IdentitySha256
                            ))
                            .eq(Expr::Value(descriptor.identity_sha256.clone().into()))
                        )
                        .and(
                            Expr::col((
                                compaction_frozen_history::Entity,
                                compaction_frozen_history::Column::MessageCount
                            ))
                            .eq(Expr::Value(count.into()))
                        )
                        .and(
                            Expr::col((
                                compaction_frozen_history::Entity,
                                compaction_frozen_history::Column::Ready
                            ))
                            .eq(Expr::val(1_i64))
                        )
                        .and(
                            Expr::col((
                                compaction_frozen_history::Entity,
                                compaction_frozen_history::Column::IdentitySha256
                            ))
                            .eq(Expr::col((
                                compaction_operation_projection::Entity,
                                compaction_operation_projection::Column::IdentitySha256
                            )))
                        )
                        .and(
                            Expr::col((
                                compaction_frozen_history::Entity,
                                compaction_frozen_history::Column::ImportsSha256
                            ))
                            .eq(Expr::col((
                                compaction_operation_projection::Entity,
                                compaction_operation_projection::Column::ImportsSha256
                            )))
                        )
                        .and(
                            Expr::col((
                                compaction_frozen_history::Entity,
                                compaction_frozen_history::Column::ImportCount
                            ))
                            .eq(Expr::col((
                                compaction_operation_projection::Entity,
                                compaction_operation_projection::Column::ImportCount
                            )))
                        )
                        .and(
                            Expr::col((
                                compaction_frozen_history::Entity,
                                compaction_frozen_history::Column::NextImport
                            ))
                            .eq(Expr::col((
                                compaction_operation_projection::Entity,
                                compaction_operation_projection::Column::ImportCount
                            )))
                        )
                    )
                    .into_tuple::<String>()
                    .one(&tx)
                    .await?
                    .is_some(),
                "source projection is not accepted by this execution or binding changed"
            );
            tx.commit().await?;
            Ok(())
        })
        .await
}

use sea_orm::{EntityTrait, QueryFilter, QuerySelect};
