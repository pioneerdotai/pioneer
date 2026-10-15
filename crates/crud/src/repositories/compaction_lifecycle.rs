//! A service operation can publish its own lifecycle after a completed Turn.
//! It cannot publish provider/tool events or inherit a retired execution lease.
use super::compaction_lifecycle_pending as queue;
use crate::{CanonicalTurnEventPayload, CrudStore};
use anyhow::{Result, ensure};
use pioneer_compaction::runner::RunnerState;
use pioneer_entity::{
    compaction_context, compaction_execution_stop, compaction_history_check, compaction_operation,
    compaction_runner_state, thread, turn, turn_event, turn_item,
};
use sea_orm::TransactionTrait;
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, OnConflict, Query};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect};

/// Constant-size control-plane fence. The owning runtime awaits this write
/// before cancelling service work. It survives worker loss without changing
/// the completed user Turn or granting a new compaction attempt.
pub(crate) async fn compaction_stop_execution(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    owner: &str,
    turn: &str,
) -> Result<()> {
    let store = store.with_maintenance_reads_and_critical_writes();
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            // Validate and insert under the same existing writer transaction;
            // a concurrent scope change cannot enter between these operations.
            let scoped_turn = turn::Entity::find_by_id(turn)
                .select_only()
                .column(turn::Column::Id)
                .join(
                    JoinType::InnerJoin,
                    turn::Entity::belongs_to(thread::Entity)
                        .from(turn::Column::ThreadId)
                        .to(thread::Column::Id)
                        .into(),
                )
                .filter(thread::Column::Id.eq(thread))
                .filter(thread::Column::WorkspaceId.eq(workspace))
                .into_tuple::<String>()
                .one(&tx)
                .await?;
            ensure!(scoped_turn.is_some(), "compaction Stop scope mismatch");
            compaction_context::Entity::insert(compaction_context::ActiveModel {
                workspace_id: sea_orm::Set(workspace.to_owned()),
                thread_id: sea_orm::Set(thread.to_owned()),
                owner: sea_orm::Set(owner.to_owned()),
                ..Default::default()
            })
            .on_conflict(
                OnConflict::columns([compaction_context::Column::Owner])
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&tx)
            .await?;
            let scoped_owner = compaction_context::Entity::find_by_id(owner)
                .select_only()
                .column(compaction_context::Column::Owner)
                .filter(compaction_context::Column::WorkspaceId.eq(workspace))
                .filter(compaction_context::Column::ThreadId.eq(thread))
                .into_tuple::<String>()
                .one(&tx)
                .await?;
            ensure!(scoped_owner.is_some(), "compaction Stop scope mismatch");
            compaction_execution_stop::Entity::insert(compaction_execution_stop::ActiveModel {
                owner: sea_orm::Set(owner.to_owned()),
                turn_id: sea_orm::Set(turn.to_owned()),
            })
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .exec_without_returning(&tx)
            .await?;
            compaction_history_check::Entity::update_many()
                .col_expr(
                    compaction_history_check::Column::State,
                    Expr::val("finished"),
                )
                .col_expr(
                    compaction_history_check::Column::Outcome,
                    Expr::val("cancelled"),
                )
                .filter(
                    Expr::col(compaction_history_check::Column::TurnId)
                        .eq(Expr::Value(turn.into()))
                        .and(Expr::exists(
                            Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(compaction_execution_stop::Entity, "stop")
                                .and_where(
                                    Expr::col(("stop", compaction_execution_stop::Column::Owner))
                                        .eq(Expr::Value(owner.into()))
                                        .and(
                                            Expr::col((
                                                "stop",
                                                compaction_execution_stop::Column::TurnId,
                                            ))
                                            .eq(Expr::col(("compaction_history_check", "turn_id"))),
                                        ),
                                )
                                .to_owned(),
                        )),
                )
                .exec(&tx)
                .await?;
            tx.commit().await?;
            Ok(())
        })
        .await
}

// Preparation reads exact rows; the commit fences every value it uses,
// including serialized state/payload when runner generations repeat.
#[derive(Clone, Debug)]
struct LifecycleFacts {
    operation: Option<compaction_operation::Model>,
    context: Option<compaction_context::Model>,
    turn: Option<turn::Model>,
    thread: Option<thread::Model>,
    runner: Option<compaction_runner_state::Model>,
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
                    &i.payload,
                )
            }) == fresh.item.as_ref().map(|i| {
                (
                    &i.id,
                    &i.turn_id,
                    &i.item_id,
                    &i.item_type,
                    &i.status,
                    &i.payload,
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
                compaction_execution_stop::Entity::find_by_id((op.owner.clone(), turn.clone()))
                    .one(db)
                    .await?
                    .is_some()
            } else {
                false
            }
        } else {
            false
        };
        let runner = compaction_runner_state::Entity::find_by_id(id)
            .one(db)
            .await?;
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
        let payload: serde_json::Value = serde_json::from_str(&item.payload)?;
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

pub(crate) async fn prepare_lifecycle(
    store: &CrudStore,
    claim: &queue::CompactionLifecycleClaim,
    now_ms: i64,
) -> Result<Option<PreparedCompactionLifecycle>> {
    if !queue::owns(&store.connection, claim).await? {
        return Ok(None);
    }
    let facts = LifecycleFacts::read(&store.connection, claim.operation_id()).await?;
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
        let state: RunnerState = serde_json::from_str(&runner.state)?;
        ensure!(
            i64::try_from(state.generation)? == runner.generation,
            "runner generation mismatch"
        );
        let terminal = super::compaction_runner::reconciled_terminal_runner(
            &state,
            &op.status,
            op.outcome.as_deref(),
        )?;
        if terminal != state {
            let encoded = serde_json::to_string(&terminal)?;
            ensure!(
                encoded.len() <= super::compaction::SOURCE_PAGE_BYTES,
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
        serde_json::to_vec(event)?.len() <= super::compaction::SOURCE_PAGE_BYTES,
        "compaction lifecycle exceeds its metadata quantum"
    );
    Ok(terminal)
}
fn event_terminal_item(event: &CanonicalTurnEventPayload) -> bool {
    matches!(event, CanonicalTurnEventPayload::ItemCompleted(n)
        if matches!(&n.item, pioneer_protocol::TurnItem::SystemEvent {details:Some(d),..}
            if matches!(d["status"].as_str(),Some("completed" | "failed" | "cancelled"))))
}

// The item must use the domain event's time, not the repair/claim time.
// Read the exact idempotency key and prepare JSON/projection outside capacity;
// the writer compares the full immutable event snapshot before applying it.
struct PreparedLifecycleEvent {
    projected: crate::PreparedProjectedTurnEvent,
    existing: Option<turn_event::Model>,
    idempotency_key: String,
    created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
}
impl PreparedLifecycleEvent {
    async fn read_existing<C: ConnectionTrait>(
        db: &C,
        turn_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<turn_event::Model>> {
        Ok(turn_event::Entity::find()
            .filter(turn_event::Column::TurnId.eq(turn_id))
            .filter(turn_event::Column::IdempotencyKey.eq(idempotency_key))
            .one(db)
            .await?)
    }

    async fn prepare(
        store: &CrudStore,
        event: CanonicalTurnEventPayload,
        created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<Self> {
        let idempotency_key = event.idempotency_key()?;
        let existing =
            Self::read_existing(&store.connection, event.turn_id(), &idempotency_key).await?;
        let created_at = existing.as_ref().map_or(created_at, |e| e.created_at);
        let projected = crate::prepare_projected_turn_event_for_permanent_storage(
            &store.connection,
            event,
            created_at,
        )
        .await?;
        Ok(Self {
            projected,
            existing,
            idempotency_key,
            created_at,
        })
    }

    async fn apply(
        self,
        store: &CrudStore,
        tx: &sea_orm::DatabaseTransaction,
        expires: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<()> {
        ensure!(
            self.existing
                == Self::read_existing(
                    tx,
                    self.projected.event.payload().turn_id(),
                    &self.idempotency_key
                )
                .await?,
            "compaction lifecycle event changed after preparation"
        );
        store
            .append_and_project_compaction_event_in_transaction(
                tx,
                self.projected,
                self.created_at,
                expires,
            )
            .await
    }
}

pub(crate) async fn repair_lifecycle(
    store: &CrudStore,
    prepared: PreparedCompactionLifecycle,
    event: Option<CanonicalTurnEventPayload>,
    timestamp_secs: i64,
) -> Result<bool> {
    let operation_id = prepared.operation_id().to_owned();
    let id = operation_id.as_str();
    ensure!(
        event.is_some() == prepared.needs_publication,
        "lifecycle publication preparation mismatch"
    );
    let created_at = crate::unix_to_datetime(timestamp_secs);
    let expires = crate::unix_to_datetime(
        timestamp_secs.saturating_add(crate::TURN_EVENT_PROJECTION_LEASE_SECS),
    );
    let projected = if let Some(event) = event {
        ensure!(
            validate_item(id, &event)? && event_terminal_item(&event),
            "pending repair requires terminal lifecycle item"
        );
        let (workspace, thread, turn) = prepared
            .scope()
            .ok_or_else(|| anyhow::anyhow!("compaction binding missing"))?;
        ensure!(
            event.workspace_id() == workspace
                && event.thread_id() == thread
                && event.turn_id() == turn,
            "compaction event scope changed"
        );
        Some(PreparedLifecycleEvent::prepare(store, event, created_at).await?)
    } else {
        None
    };
    // No operation-wide retry. A lost commit result must not issue a second
    // claim or repeat discovery within this quantum.
    let tx = store.connection.begin().await?;
    if !queue::owns(&tx, &prepared.claim).await?
        || !prepared
            .facts
            .same_inputs(&LifecycleFacts::read(&tx, id).await?)
    {
        tx.rollback().await?;
        return Ok(false);
    }
    if let Some((status, outcome)) = prepared.finish {
        super::compaction::compaction_finish(&tx, id, status, outcome).await?;
    }
    if let Some((generation, state)) = prepared.encoded_runner {
        compaction_runner_state::Entity::update_many()
            .col_expr(
                compaction_runner_state::Column::Generation,
                Expr::val(generation),
            )
            .col_expr(compaction_runner_state::Column::State, Expr::val(state))
            .filter(compaction_runner_state::Column::OperationId.eq(id))
            .exec(&tx)
            .await?;
    }
    if let Some(projected) = projected {
        projected.apply(store, &tx, expires).await?;
    }
    // All own triggers have now run. ACK the post-repair generation while the
    // same writer still owns the transaction; an external refresh cannot enter.
    queue::acknowledge(
        &tx,
        id,
        if prepared.finish.is_some() {
            Some(0)
        } else {
            prepared.keep_deadline
        },
    )
    .await?;
    tx.commit().await?;
    Ok(true)
}

pub(crate) async fn compaction_materialize_lifecycle(
    store: &CrudStore,
    operation: &str,
    generation: u64,
    event: CanonicalTurnEventPayload,
    timestamp_secs: i64,
) -> Result<()> {
    let terminal = validate_item(operation, &event)?;
    let acknowledge = terminal && event_terminal_item(&event);
    let facts = LifecycleFacts::read(&store.connection, operation).await?;
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
    let created_at = crate::unix_to_datetime(timestamp_secs);
    let expires = crate::unix_to_datetime(
        timestamp_secs.saturating_add(crate::TURN_EVENT_PROJECTION_LEASE_SECS),
    );
    let projected = PreparedLifecycleEvent::prepare(store, event, created_at).await?;
    let tx = store.connection.begin().await?;
    ensure!(
        facts.same_inputs(&LifecycleFacts::read(&tx, operation).await?),
        "compaction lifecycle inputs changed"
    );
    projected.apply(store, &tx, expires).await?;
    if acknowledge {
        queue::acknowledge(&tx, operation, None).await?;
    }
    tx.commit().await?;
    Ok(())
}
