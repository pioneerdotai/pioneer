//! Transactional retention checks for frozen history.

use anyhow::{Result, ensure};
use pioneer_entity::{
    compaction_checkpoint as checkpoint, compaction_context as context,
    compaction_frozen_history as history, compaction_frozen_layout as layout,
    compaction_operation as operation, compaction_operation_projection as projection,
    compaction_runner_plan as plan, compaction_runner_state as runner,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func, JoinType, Query, SimpleExpr};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    QuerySelect,
};

// Failed admission can finish before activation creates a runner. Activation
// requires running; both failed-operation resume paths require a saved state.
// Supported writers cannot revive this failed operation without a saved state
// or replace its admitted projection. Recheck current facts in the expiry
// writer, without caching bindings or deriving a generation. CASE fails closed
// for missing/corrupt nullable joins. Closed portions still need the ordinary
// checkpoint proof/reachability checks; their count is not a lifecycle root.
pub fn pre_runner_failure(header: &history::Model) -> SimpleExpr {
    Expr::case(
        Condition::all()
            .add(Expr::col((operation::Entity, operation::Column::Status)).eq("failed"))
            .add(Expr::col((operation::Entity, operation::Column::Owner)).ne(""))
            .add(
                Expr::col((operation::Entity, operation::Column::Outcome))
                    .is_in(["invalid_source_manifest", "deadline"]),
            )
            .add(Expr::col((operation::Entity, operation::Column::Attempts)).eq(0))
            .add(Expr::col((operation::Entity, operation::Column::TransientRetries)).eq(0))
            .add(Expr::col((operation::Entity, operation::Column::Correction)).eq(0))
            .add(Expr::col((operation::Entity, operation::Column::NextPortion)).gte(0))
            .add(Expr::col((runner::Entity, runner::Column::OperationId)).is_null())
            .add(Expr::col((projection::Entity, projection::Column::TerminalGeneration)).is_null())
            .add(
                Expr::col((context::Entity, context::Column::WorkspaceId)).eq(&header.workspace_id),
            )
            .add(Expr::col((context::Entity, context::Column::FormatVersion)).eq(1))
            .add(Expr::col((context::Entity, context::Column::ThreadId)).ne(""))
            .add(Expr::col((projection::Entity, projection::Column::ManifestId)).eq(&header.id))
            .add(
                Expr::col((projection::Entity, projection::Column::IdentitySha256))
                    .eq(&header.identity_sha256),
            )
            .add(
                Expr::col((projection::Entity, projection::Column::ImportsSha256))
                    .eq(&header.imports_sha256),
            )
            .add(
                Expr::col((projection::Entity, projection::Column::ImportCount))
                    .eq(header.import_count),
            )
            .add(Expr::exists(
                Query::select()
                    .expr(Expr::val(1))
                    .from(plan::Entity)
                    .and_where(
                        Expr::col((plan::Entity, plan::Column::OperationId))
                            .eq(Expr::col((operation::Entity, operation::Column::Id))),
                    )
                    .and_where(Expr::col((plan::Entity, plan::Column::Ready)).eq(0))
                    .to_owned(),
            )),
        true,
    )
    .finally(false)
    .into()
}

pub fn unknown_predicate(alias: &str) -> String {
    format!(
        r#"
EXISTS(SELECT 1 FROM compaction_context x JOIN compaction_operation o ON o.owner=x.owner JOIN compaction_runner_plan plan ON plan.operation_id=o.id LEFT JOIN compaction_operation_projection b ON b.operation_id=o.id WHERE x.workspace_id={alias}.workspace_id AND b.operation_id IS NULL AND NOT EXISTS(SELECT 1 FROM compaction_checkpoint c WHERE c.operation_id=o.id AND c.proof_version=1))
OR EXISTS(SELECT 1 FROM compaction_context x JOIN compaction_checkpoint c ON c.owner=x.owner LEFT JOIN compaction_operation_projection b ON b.operation_id=c.operation_id WHERE x.workspace_id={alias}.workspace_id AND c.proof_version=0 AND b.operation_id IS NULL)
"#
    )
}
fn root_predicate(alias: &str) -> String {
    format!(
        r#"{}
OR EXISTS(SELECT 1 FROM task_run_conversation_snapshot s LEFT JOIN task t ON t.id=s.task_id WHERE s.frozen_manifest_id={alias}.id AND (t.id IS NULL OR t.status NOT IN ('completed','failed','cancelled')))
OR EXISTS(SELECT 1 FROM turn_runtime_snapshot s WHERE s.frozen_manifest_id={alias}.id)
OR EXISTS(SELECT 1 FROM thread_cli_runtime_binding s WHERE s.frozen_manifest_id={alias}.id)
OR EXISTS(SELECT 1 FROM turn_cli_runtime_binding s WHERE s.frozen_manifest_id={alias}.id)
OR EXISTS(SELECT 1 FROM compaction_task_output s WHERE s.manifest_id={alias}.id)
"#,
        unknown_predicate(alias)
    )
}

/// Lock before checking published roots. The caller commits the update and
/// publishes any resulting wakeups, or rolls back the whole expiry.
pub async fn expire(
    tx: &DatabaseTransaction,
    header: &history::Model,
    locally_held: impl FnOnce(&str) -> bool,
) -> Result<bool> {
    // Root publishers take a share lock on this same header. Read
    // holding references only after taking the conflicting lock.
    let h = history::Entity::find_by_id(&header.id)
        .lock_exclusive()
        .one(tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("expiry header missing"))?;
    super::frozen::logical_bounds(&h)?;
    ensure!(h.ready == 1 && h == *header, "expiry header changed");
    if locally_held(&h.id) {
        return Ok(false);
    }
    let guard = tx
        .query_one(
            &Query::select()
                .expr(Expr::val(1))
                .from_as(history::Entity, Alias::new("h"))
                .and_where(Expr::col((Alias::new("h"), history::Column::Id)).eq(&h.id))
                .and_where(Expr::cust(root_predicate("h")))
                .limit(1)
                .to_owned(),
        )
        .await?;
    if guard.is_some() {
        return Ok(false);
    }
    // Saved runners retain the derived generation guard. Absent-runner
    // failures instead recheck their current durable facts in this writer.
    // Activation requires running; state insertion invalidates the marker.
    let active = tx
        .query_one(
            &Query::select()
                .expr(Expr::val(1))
                .from(projection::Entity)
                .join(
                    JoinType::LeftJoin,
                    operation::Entity,
                    Expr::col((operation::Entity, operation::Column::Id)).eq(Expr::col((
                        projection::Entity,
                        projection::Column::OperationId,
                    ))),
                )
                .join(
                    JoinType::LeftJoin,
                    runner::Entity,
                    Expr::col((runner::Entity, runner::Column::OperationId))
                        .eq(Expr::col((operation::Entity, operation::Column::Id))),
                )
                .join(
                    JoinType::LeftJoin,
                    context::Entity,
                    Expr::col((context::Entity, context::Column::Owner))
                        .eq(Expr::col((operation::Entity, operation::Column::Owner))),
                )
                .and_where(
                    Expr::col((projection::Entity, projection::Column::ManifestId)).eq(&h.id),
                )
                .cond_where(
                    Condition::any()
                        .add(Expr::col((operation::Entity, operation::Column::Id)).is_null())
                        .add(
                            Expr::col((operation::Entity, operation::Column::Status)).is_not_in([
                                "completed",
                                "failed",
                                "cancelled",
                                "stale",
                            ]),
                        )
                        .add(Expr::col((runner::Entity, runner::Column::OperationId)).is_null())
                        .add(
                            Expr::col((projection::Entity, projection::Column::TerminalGeneration))
                                .is_null(),
                        )
                        .add(
                            Expr::col((projection::Entity, projection::Column::TerminalGeneration))
                                .ne(Expr::col((runner::Entity, runner::Column::Generation))),
                        )
                        .add(Expr::col((context::Entity, context::Column::WorkspaceId)).is_null())
                        .add(
                            Expr::col((context::Entity, context::Column::WorkspaceId))
                                .ne(&h.workspace_id),
                        ),
                )
                .and_where(pre_runner_failure(&h).eq(false))
                .limit(1)
                .to_owned(),
        )
        .await?;
    if active.is_some() {
        return Ok(false);
    }
    let unsealed_public = tx
        .query_one(
            &Query::select()
                .expr(Expr::val(1))
                .from(projection::Entity)
                .join(
                    JoinType::InnerJoin,
                    checkpoint::Entity,
                    Expr::col((checkpoint::Entity, checkpoint::Column::OperationId)).eq(Expr::col(
                        (projection::Entity, projection::Column::OperationId),
                    )),
                )
                .join(
                    JoinType::InnerJoin,
                    operation::Entity,
                    Expr::col((operation::Entity, operation::Column::Id)).eq(Expr::col((
                        projection::Entity,
                        projection::Column::OperationId,
                    ))),
                )
                .and_where(
                    Expr::col((projection::Entity, projection::Column::ManifestId)).eq(&h.id),
                )
                .and_where(Expr::col((checkpoint::Entity, checkpoint::Column::ProofVersion)).ne(1))
                .and_where(
                    Expr::col((checkpoint::Entity, checkpoint::Column::Status))
                        .eq("applied")
                        .or(Expr::col((checkpoint::Entity, checkpoint::Column::Status))
                            .eq("retained")
                            .and(
                                Expr::col((operation::Entity, operation::Column::Status))
                                    .eq("completed"),
                            )),
                )
                .limit(1)
                .to_owned(),
        )
        .await?;
    if unsealed_public.is_some() {
        return Ok(false);
    }
    let transitions = layout::Entity::find()
        .select_only()
        .column(layout::Column::ManifestId)
        .filter(
            Condition::any()
                .add(layout::Column::ManifestId.eq(&h.id))
                .add(layout::Column::Candidate.eq(&h.id)),
        )
        .filter(
            Condition::any()
                .add(layout::Column::Active.ne(1))
                .add(layout::Column::Pending.ne(0))
                .add(layout::Column::Failed.ne(0))
                .add(Expr::col(layout::Column::CopyNext).lt(Func::coalesce([
                    Expr::col(layout::Column::CopyTo),
                    Expr::val(0),
                ]))),
        )
        .into_tuple::<String>()
        .one(tx)
        .await?;
    if transitions.is_some() {
        return Ok(false);
    }
    let expired = history::Entity::update_many()
        .col_expr(history::Column::Expired, Expr::val(1))
        .filter(history::Column::Id.eq(&h.id))
        .filter(history::Column::Expired.eq(0))
        .filter(history::Column::Ready.eq(1))
        .filter(
            Expr::col(history::Column::NextOrdinal).eq(Expr::col(history::Column::MessageCount)),
        )
        .filter(Expr::col(history::Column::NextImport).eq(Expr::col(history::Column::ImportCount)))
        .exec(tx)
        .await?;
    Ok(expired.rows_affected > 0)
}
