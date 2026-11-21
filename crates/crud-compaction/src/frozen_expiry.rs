//! Transactional retention checks for frozen history.

use anyhow::{Result, ensure};
use pioneer_compaction::runner::{RunnerPhase, RunnerState};
use pioneer_entity::{
    compaction_checkpoint as checkpoint, compaction_context as context,
    compaction_frozen_history as history, compaction_frozen_layout as layout,
    compaction_operation as operation, compaction_operation_projection as projection,
    compaction_runner_plan as plan, compaction_runner_state as runner,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func, JoinType, Order, Query, SimpleExpr};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    QuerySelect, TransactionTrait,
};

use crate::frozen::{LayoutScan, header_bounds};
use crate::frozen_lifetime::{ReaderCounts, cleanup_blocked};
use crate::frozen_reachability::Reachability;
use crate::frozen_storage_reads::directly_held;
use crate::sweep_work::retry_conflict;

// Indexed roots only: JSON is authoritative at cutover writers and sentinel CAS,
// never parsed in expiry/sweep. A missing Task is unknown. Only a completed,
// failed or cancelled Task releases its inputs; blocked remains retaining.
// Releasing inputs do not retain even an inconsistent old snapshot scope;
// unresolved checkpoint ownership is a separate prerequisite. Retaining
// unknown Task inputs block both known workspace interpretations.
pub async fn publish_expiry(
    db: &SqliteDatabase,
    readers: &ReaderCounts,
    header: &history::Model,
) -> Result<bool> {
    let changes = [
        db.work_signal("maintenance.portable").changing(),
        db.work_signal("maintenance.html").changing(),
    ];
    let released = db.work_signal("maintenance.frozen.release");
    let result = retry_conflict(|| async {
        let tx = db.begin().await?;
        let changed = expire(&tx, header, |id| cleanup_blocked(readers, id, &released)).await?;
        tx.commit().await?;
        Ok(changed)
    })
    .await;
    if let Ok(changed) = &result {
        for change in changes {
            change.completed(*changed);
        }
    }
    result
}

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

/// Metadata-only geometry walk; no frozen payload is read. Complete headers
/// and published spans remain immutable through expiry. Incomplete mutations
/// fail closed against the saved bounds; final DELETE rechecks current ranges.
/// Artificial sweep pins would repeatedly prevent logical expiry.
pub struct LayoutCheck {
    scan: LayoutScan,
    done: bool,
}
impl LayoutCheck {
    pub fn from_scan(scan: LayoutScan) -> Self {
        Self { scan, done: false }
    }

    pub fn done(&self) -> bool {
        self.done
    }

    async fn new(db: &SqliteDatabase, id: &str, kind: i64) -> Result<Self> {
        let header = history::Entity::find_by_id(id)
            .one(db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("physical backing scope missing"))?;
        header_bounds(&header)?;
        let scan = LayoutScan::new(header, kind, kind + 1)?;
        Ok(Self { scan, done: false })
    }
    pub async fn step(&mut self, db: &SqliteDatabase) -> Result<()> {
        self.scan.step(db).await?;
        self.done = self.scan.done();
        Ok(())
    }
}
#[derive(Clone)]
struct TerminalBinding {
    id: String,
    owner: String,
    status: String,
    generation: Option<i64>,
}
pub fn terminal_runner(status: &str, generation: Option<i64>, state: Option<&str>) -> Result<bool> {
    if !matches!(status, "completed" | "failed" | "cancelled" | "stale") {
        return Ok(false);
    }
    let (Some(generation), Some(json)) = (generation, state) else {
        return Ok(false);
    };
    let state: RunnerState = serde_json::from_str(json)?;
    ensure!(
        i64::try_from(state.generation)? == generation,
        "terminal runner generation corrupt"
    );
    Ok(match status {
        "completed" => matches!(state.phase, RunnerPhase::Applied { .. }),
        "failed" | "cancelled" | "stale" => matches!(state.phase, RunnerPhase::Failed { .. }),
        _ => false,
    })
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpiryStep {
    More,
    Held,
    ProtectedByCheckpoint,
    Expired,
}

pub struct ExpiryPreparation {
    header: history::Model,
    roots_checked: bool,
    operations_after: String,
    operations_done: bool,
    checkpoint_after: String,
    reverse: Option<Reachability>,
    checkpoints_done: bool,
    layout_kind: i64,
    layout: Option<LayoutCheck>,
}
impl ExpiryPreparation {
    pub fn new(header: history::Model) -> Self {
        Self {
            header,
            roots_checked: false,
            operations_after: String::new(),
            operations_done: false,
            checkpoint_after: String::new(),
            reverse: None,
            checkpoints_done: false,
            layout_kind: 0,
            layout: None,
        }
    }
    pub fn restart(&mut self) {
        *self = Self::new(self.header.clone());
    }

    pub fn after_root_preflight(header: history::Model) -> Self {
        Self {
            roots_checked: true,
            ..Self::new(header)
        }
    }

    pub fn header(&self) -> &history::Model {
        &self.header
    }

    pub fn roots_checked(&self) -> bool {
        self.roots_checked
    }

    pub fn completed_layouts(&self) -> i64 {
        self.layout_kind
    }

    pub fn operations_checked(&self) -> bool {
        self.operations_done
    }

    pub fn checkpoints_checked(&self) -> bool {
        self.checkpoints_done
    }

    pub fn is_preparing_reverse_dependencies(&self) -> bool {
        self.reverse.is_some()
    }

    pub fn is_unprepared(&self) -> bool {
        self.layout_kind == 0
            && self.layout.is_none()
            && !self.operations_done
            && self.operations_after.is_empty()
            && !self.checkpoints_done
            && self.checkpoint_after.is_empty()
            && self.reverse.is_none()
    }

    pub async fn step(
        &mut self,
        db: &SqliteDatabase,
        readers: &ReaderCounts,
    ) -> Result<ExpiryStep> {
        if !self.roots_checked {
            // Advisory positive hold only, once per attempt. Release the reader
            // before any preparation; a negative result leaves every final
            // writer guard in place. Ordinary passes revisit released roots.
            let held = directly_held(db, &self.header.id).await?;
            self.roots_checked = true;
            if held {
                return Ok(ExpiryStep::Held);
            }
            return Ok(ExpiryStep::More);
        }
        if self.layout_kind < 2 {
            if let Some(layout) = self.layout.as_mut() {
                layout.step(db).await?;
                if layout.done {
                    self.layout = None;
                    self.layout_kind += 1;
                }
            } else {
                self.layout = Some(LayoutCheck::new(db, &self.header.id, self.layout_kind).await?);
            }
            return Ok(ExpiryStep::More);
        }
        if !self.operations_done {
            // One bounded saved state per quantum, decoded after reader release.
            let row = db.query_one(&Query::select().from(projection::Entity)
                .column((projection::Entity,projection::Column::OperationId))
                .columns([operation::Column::Owner,operation::Column::Status].map(|c| (operation::Entity,c)))
                .column((runner::Entity,runner::Column::Generation)).column((context::Entity,context::Column::WorkspaceId))
                .expr_as(pre_runner_failure(&self.header),Alias::new("pre_runner_failure"))
                .expr_as(Expr::cust("CASE WHEN octet_length(compaction_runner_state.state::text)<=262144 THEN compaction_runner_state.state::text END"),Alias::new("state"))
                .join(JoinType::LeftJoin,operation::Entity,
                    Expr::col((operation::Entity,operation::Column::Id)).eq(Expr::col((projection::Entity,projection::Column::OperationId))))
                .join(JoinType::LeftJoin,context::Entity,
                    Expr::col((context::Entity,context::Column::Owner)).eq(Expr::col((operation::Entity,operation::Column::Owner))))
                .join(JoinType::LeftJoin,runner::Entity,
                    Expr::col((runner::Entity,runner::Column::OperationId)).eq(Expr::col((operation::Entity,operation::Column::Id))))
                .and_where(Expr::col((projection::Entity,projection::Column::ManifestId)).eq(&self.header.id))
                .and_where(Expr::col((projection::Entity,projection::Column::OperationId)).gt(&self.operations_after))
                .order_by((projection::Entity,projection::Column::OperationId),Order::Asc).limit(1).to_owned()).await?;
            if let Some(row) = row {
                ensure!(
                    row.try_get::<String>("", "workspace_id")? == self.header.workspace_id,
                    "origin operation scope corrupt"
                );
                let id: String = row.try_get("", "operation_id")?;
                let owner: String = row.try_get("", "owner")?;
                let status: String = row.try_get("", "status")?;
                let generation: Option<i64> = row.try_get("", "generation")?;
                let state: Option<String> = row.try_get("", "state")?;
                if row.try_get::<bool>("", "pre_runner_failure")? {
                    // No synthetic state/terminal_generation. Checkpoints below
                    // still need their ordinary proof/reachability validation.
                    self.operations_after = id;
                    return Ok(ExpiryStep::More);
                }
                if !terminal_runner(&status, generation, state.as_deref())? {
                    return Ok(ExpiryStep::Held);
                }
                self.operations_after = id.clone();
                let binding = TerminalBinding {
                    id,
                    owner,
                    status,
                    generation,
                };
                retry_conflict(|| async {
                    let tx = db.begin().await?;
                    let current = Query::select()
                        .expr(Expr::val(1))
                        .from(operation::Entity)
                        .join(
                            JoinType::InnerJoin,
                            runner::Entity,
                            Expr::col((runner::Entity, runner::Column::OperationId))
                                .eq(Expr::col((operation::Entity, operation::Column::Id))),
                        )
                        .join(
                            JoinType::InnerJoin,
                            context::Entity,
                            Expr::col((context::Entity, context::Column::Owner))
                                .eq(Expr::col((operation::Entity, operation::Column::Owner))),
                        )
                        .and_where(
                            Expr::col((operation::Entity, operation::Column::Id)).eq(&binding.id),
                        )
                        .and_where(
                            Expr::col((operation::Entity, operation::Column::Owner))
                                .eq(&binding.owner),
                        )
                        .and_where(
                            Expr::col((operation::Entity, operation::Column::Status))
                                .eq(&binding.status),
                        )
                        .and_where(
                            Expr::col((runner::Entity, runner::Column::Generation))
                                .eq(binding.generation),
                        )
                        .and_where(
                            Expr::col((context::Entity, context::Column::WorkspaceId))
                                .eq(&self.header.workspace_id),
                        )
                        .to_owned();
                    projection::Entity::update_many()
                        .col_expr(
                            projection::Column::TerminalGeneration,
                            Expr::val(binding.generation),
                        )
                        .filter(projection::Column::OperationId.eq(&binding.id))
                        .filter(projection::Column::ManifestId.eq(&self.header.id))
                        .filter(Expr::exists(current))
                        .exec(&tx)
                        .await?;
                    tx.commit().await?;
                    Ok(())
                })
                .await?;
                return Ok(ExpiryStep::More);
            }
            self.operations_done = true;
            return Ok(ExpiryStep::More);
        }
        if let Some(reverse) = self.reverse.as_mut() {
            reverse.step(db).await?;
            if reverse.done() {
                if reverse.reachable() {
                    // A published legacy dependency is a hold, not poison.
                    // End this expiry attempt; the cyclic pass revisits it
                    // after the independent bounded proof preparation seals it.
                    return Ok(ExpiryStep::ProtectedByCheckpoint);
                }
                self.reverse = None;
            }
            return Ok(ExpiryStep::More);
        }
        if !self.checkpoints_done {
            let query = Query::select()
                .column((checkpoint::Entity, checkpoint::Column::Id))
                .from(checkpoint::Entity)
                .join(
                    JoinType::InnerJoin,
                    projection::Entity,
                    Expr::col((projection::Entity, projection::Column::OperationId)).eq(Expr::col(
                        (checkpoint::Entity, checkpoint::Column::OperationId),
                    )),
                )
                .and_where(
                    Expr::col((projection::Entity, projection::Column::ManifestId))
                        .eq(&self.header.id),
                )
                .and_where(
                    Expr::col((checkpoint::Entity, checkpoint::Column::Id))
                        .gt(&self.checkpoint_after),
                )
                .and_where(Expr::col((checkpoint::Entity, checkpoint::Column::ProofVersion)).ne(1))
                .order_by((checkpoint::Entity, checkpoint::Column::Id), Order::Asc)
                .limit(1)
                .to_owned();
            let row = db.query_one(&query).await?;
            if let Some(row) = row {
                let id: String = row.try_get("", "id")?;
                self.checkpoint_after = id.clone();
                self.reverse = Some(Reachability::new(&id, &self.header.workspace_id));
                return Ok(ExpiryStep::More);
            }
            self.checkpoints_done = true;
            return Ok(ExpiryStep::More);
        }
        let changed = publish_expiry(db, readers, &self.header).await?;
        Ok(if changed {
            ExpiryStep::Expired
        } else {
            ExpiryStep::Held
        })
    }
}
