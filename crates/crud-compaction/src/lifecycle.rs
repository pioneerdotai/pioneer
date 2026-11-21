//! Durable lifecycle admission and repair fences for one compaction operation.
use crate::lifecycle_pending as queue;
use crate::runner_state::{RunnerStateRecord, observation, update_runner_state};
use anyhow::{Result, ensure};
use pioneer_compaction::runner::RunnerState;
use pioneer_crud_conversations::events::CanonicalTurnEventPayload;
use pioneer_entity::{
    compaction_context, compaction_execution_stop, compaction_operation, thread, turn, turn_item,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, TransactionTrait,
};

// Preparation reads exact rows; the commit fences every value it uses,
// including serialized state/payload when runner generations repeat.
#[derive(Clone, Debug)]
struct LifecycleFacts {
    operation: Option<compaction_operation::Model>,
    context: Option<compaction_context::Model>,
    turn: Option<turn::Model>,
    thread: Option<thread::Model>,
    runner: Option<RunnerStateRecord>,
    item: Option<turn_item::Model>,
    stopped: bool,
}
impl LifecycleFacts {
    fn same_inputs(&self, fresh: &Self) -> bool {
        // Unrelated previews, timestamps and heartbeat metadata must not fence
        // deadline/Stop or publication while their actual inputs stay equal.
        self.operation.as_ref().map(|o| {
            (
                &o.id,
                &o.owner,
                &o.status,
                &o.outcome,
                o.deadline_ms,
                &o.execution_turn,
            )
        }) == fresh.operation.as_ref().map(|o| {
            (
                &o.id,
                &o.owner,
                &o.status,
                &o.outcome,
                o.deadline_ms,
                &o.execution_turn,
            )
        }) && self
            .context
            .as_ref()
            .map(|c| (&c.owner, &c.workspace_id, &c.thread_id))
            == fresh
                .context
                .as_ref()
                .map(|c| (&c.owner, &c.workspace_id, &c.thread_id))
            && self.turn.as_ref().map(|t| (&t.id, &t.thread_id, &t.status))
                == fresh
                    .turn
                    .as_ref()
                    .map(|t| (&t.id, &t.thread_id, &t.status))
            && self.thread.as_ref().map(|t| (&t.id, &t.workspace_id))
                == fresh.thread.as_ref().map(|t| (&t.id, &t.workspace_id))
            && self.runner == fresh.runner
            && self.item.as_ref().map(|i| {
                (
                    &i.id,
                    &i.turn_id,
                    &i.item_id,
                    &i.item_type,
                    &i.status,
                    &i.portable_identity,
                    i.portable_revision,
                )
            }) == fresh.item.as_ref().map(|i| {
                (
                    &i.id,
                    &i.turn_id,
                    &i.item_id,
                    &i.item_type,
                    &i.status,
                    &i.portable_identity,
                    i.portable_revision,
                )
            })
            && self.stopped == fresh.stopped
    }
    async fn read<C: ConnectionTrait>(db: &C, id: &str) -> Result<Self> {
        let operation = compaction_operation::Entity::find_by_id(id).one(db).await?;
        let context = if let Some(op) = &operation {
            compaction_context::Entity::find_by_id(&op.owner)
                .one(db)
                .await?
        } else {
            None
        };
        let turn = if let Some(id) = operation.as_ref().and_then(|op| op.execution_turn.as_ref()) {
            turn::Entity::find_by_id(id).one(db).await?
        } else {
            None
        };
        let thread = if let Some(turn) = &turn {
            thread::Entity::find_by_id(&turn.thread_id).one(db).await?
        } else {
            None
        };
        let stopped = if let Some(op) = &operation {
            if let Some(turn) = &op.execution_turn {
                compaction_execution_stop::Entity::find()
                    .filter(compaction_execution_stop::Column::Owner.eq(&op.owner))
                    .filter(compaction_execution_stop::Column::TurnId.eq(turn))
                    .one(db)
                    .await?
                    .is_some()
            } else {
                false
            }
        } else {
            false
        };
        let runner = observation(db, id).await?;
        let item = if let Some(turn) = &turn {
            turn_item::Entity::find()
                .filter(turn_item::Column::TurnId.eq(&turn.id))
                .filter(turn_item::Column::ItemId.eq(format!("compaction:{id}")))
                .one(db)
                .await?
        } else {
            None
        };
        Ok(Self {
            operation,
            context,
            turn,
            thread,
            runner,
            item,
            stopped,
        })
    }
    fn binding(&self) -> bool {
        matches!((&self.context, &self.turn), (Some(c), Some(t)) if c.thread_id == t.thread_id)
    }
    fn publication_binding(&self) -> bool {
        self.binding()
            && matches!((&self.context, &self.thread), (Some(c), Some(t)) if c.workspace_id == t.workspace_id)
    }
    fn cancelled(&self) -> bool {
        self.stopped
            || self
                .turn
                .as_ref()
                .is_some_and(|turn| matches!(turn.status.as_str(), "interrupted" | "cancelled"))
    }
    fn terminal_item(&self) -> Result<bool> {
        let Some(item) = &self.item else {
            return Ok(false);
        };
        // This preserves the previous details.status predicate, without SQL
        // JSON or a historical anti-join. Malformed JSON is durable retry work.
        let Some(payload) = item.payload.as_ref() else {
            anyhow::bail!("compaction lifecycle item payload was not loaded");
        };
        Ok(matches!(
            payload["details"]["status"].as_str(),
            Some("completed" | "failed" | "cancelled")
        ))
    }
}

/// The worker holds no DB resource while acquiring its coordinator lease or
/// building the existing lifecycle item. Only this bounded preparation crosses
/// that gap; commit compares every used input under the serialized writer.
#[derive(Clone, Debug)]
pub struct PreparedCompactionLifecycle {
    claim: queue::CompactionLifecycleClaim,
    facts: LifecycleFacts,
    finish: Option<(&'static str, &'static str)>,
    encoded_runner: Option<(i64, String)>,
    state: Option<RunnerState>,
    needs_publication: bool,
    keep_deadline: Option<i64>,
}
impl PreparedCompactionLifecycle {
    pub fn state(&self) -> Option<&RunnerState> {
        self.state.as_ref()
    }
    pub fn needs_publication(&self) -> bool {
        self.needs_publication
    }
    pub fn operation_id(&self) -> &str {
        self.claim.operation_id()
    }
    pub fn scope(&self) -> Option<(&str, &str, &str)> {
        match (&self.facts.context, &self.facts.turn) {
            (Some(c), Some(t)) if self.facts.binding() => {
                Some((&c.workspace_id, &c.thread_id, &t.id))
            }
            _ => None,
        }
    }
}

pub async fn prepare_lifecycle(
    db: &SqliteDatabase,
    claim: &queue::CompactionLifecycleClaim,
    now_ms: i64,
) -> Result<Option<PreparedCompactionLifecycle>> {
    if !queue::owns(db, claim).await? {
        return Ok(None);
    }
    let mut facts = LifecycleFacts::read(db, claim.operation_id()).await?;
    if let Some(item) = facts.item.take() {
        facts.item = pioneer_crud_conversations::turn_payload::hydrate_items(db, vec![item])
            .await?
            .pop();
    }
    let mut prepared = PreparedCompactionLifecycle {
        claim: claim.clone(),
        facts,
        finish: None,
        encoded_runner: None,
        state: None,
        needs_publication: false,
        keep_deadline: None,
    };
    let Some(op) = &prepared.facts.operation else {
        return Ok(Some(prepared));
    };
    if op.status == "running" {
        // The old scanner required a matching context/Turn even for timeout.
        // Keep an unrepairable running obligation with the claimed backoff;
        // never invent a missing binding or remove the deadline obligation.
        ensure!(
            prepared.facts.binding(),
            "running compaction binding missing"
        );
        if !prepared.facts.cancelled() && op.deadline_ms > now_ms {
            prepared.keep_deadline = Some(op.deadline_ms);
            return Ok(Some(prepared));
        }
        prepared.finish = Some(if prepared.facts.cancelled() {
            ("cancelled", "cancelled")
        } else {
            ("failed", "deadline")
        });
        // Finish is the existing independent control-plane transition. A
        // poison runner/publication must never prevent deadline or Stop from
        // fencing the operation. The terminal debt runs in a later quantum.
        return Ok(Some(prepared));
    } else if !prepared.facts.binding()
        || prepared.facts.runner.is_none()
        || prepared.facts.terminal_item()?
    {
        // Previous terminal predicate is false. Late source inserts and item
        // replay/deletion re-enqueue via physical triggers.
        return Ok(Some(prepared));
    }
    if let Some(runner) = &prepared.facts.runner {
        let state = runner.decode()?;
        let terminal = crate::runner_repository::reconciled_terminal_runner(
            &state,
            &op.status,
            op.outcome.as_deref(),
        )?;
        if terminal != state {
            let encoded = serde_json::to_string(&terminal)?;
            ensure!(
                encoded.len() <= crate::SOURCE_PAGE_BYTES,
                "runner state exceeds quantum"
            );
            prepared.encoded_runner = Some((i64::try_from(terminal.generation)?, encoded));
        }
        prepared.state = Some(terminal);
        ensure!(
            prepared.facts.publication_binding(),
            "compaction publication binding missing"
        );
        prepared.needs_publication = true;
    }
    Ok(Some(prepared))
}

fn validate_item(operation: &str, event: &CanonicalTurnEventPayload) -> Result<bool> {
    let (item, terminal) = match event {
        CanonicalTurnEventPayload::ItemStarted(n) => (&n.item, false),
        CanonicalTurnEventPayload::ItemCompleted(n) => (&n.item, true),
        _ => anyhow::bail!("compaction may only publish its lifecycle item"),
    };
    ensure!(
        matches!(item, pioneer_protocol::TurnItem::SystemEvent {id,code:Some(code),..}
        if id == &format!("compaction:{operation}") && code == "agent_context_compaction"),
        "compaction lifecycle item identity mismatch"
    );
    ensure!(
        serde_json::to_vec(event)?.len() <= crate::SOURCE_PAGE_BYTES,
        "compaction lifecycle exceeds its metadata quantum"
    );
    Ok(terminal)
}
fn event_terminal_item(event: &CanonicalTurnEventPayload) -> bool {
    matches!(event, CanonicalTurnEventPayload::ItemCompleted(n)
        if matches!(&n.item, pioneer_protocol::TurnItem::SystemEvent {details:Some(d),..}
            if matches!(d["status"].as_str(),Some("completed" | "failed" | "cancelled"))))
}

impl PreparedCompactionLifecycle {
    pub fn validate_publication(&self, event: &CanonicalTurnEventPayload) -> Result<()> {
        ensure!(
            validate_item(self.operation_id(), event)? && event_terminal_item(event),
            "pending repair requires terminal lifecycle item"
        );
        let (workspace, thread, turn) = self
            .scope()
            .ok_or_else(|| anyhow::anyhow!("compaction binding missing"))?;
        ensure!(
            event.workspace_id() == workspace
                && event.thread_id() == thread
                && event.turn_id() == turn,
            "compaction event scope changed"
        );

        Ok(())
    }

    /// Retain the claim, binding, runner generation and source payload fences.
    pub async fn repair_in_connection(&mut self, tx: &DatabaseTransaction) -> Result<Option<bool>> {
        let id = self.claim.operation_id();
        if !queue::owns(tx, &self.claim).await?
            || !self.facts.same_inputs(&LifecycleFacts::read(tx, id).await?)
        {
            return Ok(None);
        }
        let mut changed = false;
        if let Some((status, outcome)) = self.finish {
            changed |= crate::operations::compaction_finish(tx, id, status, outcome).await?;
        }
        if let Some((generation, state)) = self.encoded_runner.take() {
            let updated = update_runner_state(generation, state)
                .filter(pioneer_entity::compaction_runner_state::Column::OperationId.eq(id))
                .exec(tx)
                .await?;
            changed |= updated.rows_affected > 0;
        }

        Ok(Some(changed))
    }

    /// Call after the optional canonical publication in the same transaction.
    pub async fn acknowledge(&self, tx: &DatabaseTransaction) -> Result<()> {
        queue::acknowledge(
            tx,
            self.operation_id(),
            if self.finish.is_some() {
                Some(0)
            } else {
                self.keep_deadline
            },
        )
        .await?;
        Ok(())
    }
}

pub struct LifecycleAdmission {
    operation: String,
    facts: LifecycleFacts,
    acknowledge: bool,
}

impl LifecycleAdmission {
    pub async fn prepare(
        db: &SqliteDatabase,
        operation: &str,
        generation: u64,
        event: &CanonicalTurnEventPayload,
    ) -> Result<Self> {
        let terminal = validate_item(operation, event)?;
        let acknowledge = terminal && event_terminal_item(event);
        let facts = LifecycleFacts::read(db, operation).await?;
        let op = facts
            .operation
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("compaction operation missing"))?;
        let runner = facts
            .runner
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("compaction runner missing"))?;
        let context = facts
            .context
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("compaction context missing"))?;
        let turn = facts
            .turn
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("compaction Turn missing"))?;
        ensure!(
            facts.publication_binding()
                && runner.generation == i64::try_from(generation)?
                && event.workspace_id() == context.workspace_id
                && event.thread_id() == context.thread_id
                && event.turn_id() == turn.id
                && if terminal {
                    op.status != "running"
                } else {
                    op.status == "running" && !facts.cancelled()
                },
            "compaction lifecycle authority changed"
        );

        Ok(Self {
            operation: operation.to_owned(),
            facts,
            acknowledge,
        })
    }

    pub async fn revalidate(&self, tx: &DatabaseTransaction) -> Result<()> {
        ensure!(
            self.facts
                .same_inputs(&LifecycleFacts::read(tx, &self.operation).await?),
            "compaction lifecycle inputs changed"
        );
        Ok(())
    }

    pub async fn acknowledge(&self, tx: &DatabaseTransaction) -> Result<()> {
        if self.acknowledge {
            queue::acknowledge(tx, &self.operation, None).await?;
        }
        Ok(())
    }
}

impl crate::store::CompactionStore {
    pub async fn compaction_prepare_lifecycle(
        &self,
        claim: &queue::CompactionLifecycleClaim,
        now_ms: i64,
    ) -> Result<Option<PreparedCompactionLifecycle>> {
        prepare_lifecycle(&self.with_maintenance_access().connection, claim, now_ms).await
    }

    pub async fn repair_lifecycle_without_publication(
        &self,
        mut prepared: PreparedCompactionLifecycle,
    ) -> Result<(bool, bool)> {
        ensure!(
            !prepared.needs_publication(),
            "lifecycle publication preparation mismatch"
        );
        // Do not retry this claim after an ambiguous commit.
        let tx = self.connection.begin().await?;
        let Some(changed) = prepared.repair_in_connection(&tx).await? else {
            tx.rollback().await?;
            return Ok((false, false));
        };
        prepared.acknowledge(&tx).await?;
        tx.commit().await?;
        Ok((true, changed))
    }
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
