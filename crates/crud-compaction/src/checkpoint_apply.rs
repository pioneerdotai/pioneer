//! Bounded canonical checkpoint publication and its transaction fence.

use anyhow::{Result, ensure};
use pioneer_compaction::{Checkpoint, SourceRef};
use pioneer_entity::{
    compaction_checkpoint, compaction_context, compaction_event_revision,
    compaction_execution_stop, compaction_input_revision, compaction_item_revision,
    compaction_operation, compaction_operation_projection, compaction_source_revision, thread,
    turn, turn_event, turn_input, turn_item, turn_llm_context,
};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, DbBackend, EntityTrait, QueryFilter,
    QuerySelect, Statement, TransactionTrait,
};
use sha2::{Digest, Sha256};

use crate::SOURCE_PAGE_BYTES;
use crate::checkpoint_topology::checkpoint_coverage;
use crate::runner_writes::{CanonicalSource, checkpoint_identity};
use crate::source_revision::published_checkpoint;

#[derive(Clone, Debug)]
pub struct SourceAssertion {
    pub revision: Option<i64>,
    pub kind: CanonicalSource,
    pub turn_id: String,
    pub id: String,
    pub payload: String,
}
impl SourceAssertion {
    pub fn reference(&self) -> SourceRef {
        SourceRef {
            scope: format!("{}:{}", self.kind.prefix(), self.turn_id),
            id: self.id.clone(),
            version: self
                .revision
                .map(|v| match self.kind {
                    CanonicalSource::Input => format!("input-revision:{v}"),
                    CanonicalSource::ToolItem => format!("item-revision:{v}"),
                    CanonicalSource::Event => format!("event-revision:{v}"),
                    _ => format!("revision:{v}"),
                })
                .unwrap_or_else(|| hex::encode(Sha256::digest(self.payload.as_bytes()))),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    Applied,
    AlreadyApplied,
    Stale,
    Cancelled,
}

pub async fn compaction_checkpoint<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<Checkpoint>> {
    let Some(row) = compaction_checkpoint::Entity::find_by_id(id)
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let coverage = checkpoint_coverage(db, id).await?;
    // Decode only after the bounded database reads have released their capacity.
    Ok(Some(Checkpoint {
        id: row.id,
        operation_id: row.operation_id,
        owner: row.owner,
        previous: row.previous,
        summary: row.summary,
        selection: serde_json::from_value(row.selection)?,
        coverage,
        projection_version: row.projection_version as u64,
        format_version: row.format_version as u32,
    }))
}

pub async fn checkpoint_has_projection(
    db: &impl ConnectionTrait,
    operation_id: &str,
) -> Result<bool> {
    Ok(
        compaction_operation_projection::Entity::find_by_id(operation_id)
            .select_only()
            .column(compaction_operation_projection::Column::OperationId)
            .into_tuple::<String>()
            .one(db)
            .await?
            .is_some(),
    )
}

pub struct PreparedCheckpointApplication<'a> {
    checkpoint: &'a Checkpoint,
    expected_head: Option<&'a str>,
    assertions: &'a [SourceAssertion],
    has_projection: bool,
    identity: String,
}

pub fn prepare_checkpoint_application<'a>(
    checkpoint: &'a Checkpoint,
    expected_head: Option<&'a str>,
    assertions: &'a [SourceAssertion],
    has_projection: bool,
) -> Result<PreparedCheckpointApplication<'a>> {
    ensure!(
        assertions.len() <= pioneer_compaction::REPLAY_ALIAS_LIMIT
            && assertions.iter().map(|s| s.payload.len()).sum::<usize>() <= SOURCE_PAGE_BYTES,
        "source revalidation exceeds quantum"
    );
    let mut references: Vec<_> = assertions.iter().map(SourceAssertion::reference).collect();
    let mut coverage = checkpoint.coverage.clone();
    references.sort();
    coverage.sort();
    ensure!(
        references == coverage,
        "candidate coverage differs from validated sources"
    );
    Ok(PreparedCheckpointApplication {
        checkpoint,
        expected_head,
        assertions,
        has_projection,
        identity: checkpoint_identity(checkpoint)?,
    })
}

/// The caller commits only Applied. All other outcomes leave the checkpoint,
/// its operation and the current context unchanged.
impl PreparedCheckpointApplication<'_> {
    pub async fn apply(&self, txn: &DatabaseTransaction) -> Result<CommitOutcome> {
        let Self {
            checkpoint,
            expected_head,
            assertions,
            has_projection,
            identity,
        } = self;
        let expected_head = *expected_head;
        let assertions = *assertions;
        let has_projection = *has_projection;
        let exact = compaction_checkpoint::Entity::find()
            .select_only()
            .column(compaction_checkpoint::Column::Id)
            .filter(
                Expr::col(compaction_checkpoint::Column::Id)
                    .eq(Expr::Value(checkpoint.id.clone().into()))
                    .and(
                        Expr::col(compaction_checkpoint::Column::IdentitySha256)
                            .eq(Expr::Value(identity.clone().into())),
                    ),
            )
            .into_tuple::<String>()
            .one(txn)
            .await?;
        ensure!(exact.is_some(), "candidate identity mismatch");
        ensure!(
            compaction_checkpoint::Entity::find_by_id(&checkpoint.id)
                .select_only()
                .column(compaction_checkpoint::Column::Id)
                .filter(compaction_checkpoint::Column::ProofVersion.eq(1))
                .into_tuple::<String>()
                .one(txn)
                .await?
                .is_some(),
            "candidate proofs not ready"
        );
        let op = compaction_operation::Entity::find()
            .select_only()
            .column(compaction_operation::Column::Status)
            .filter(
                Expr::col(compaction_operation::Column::Id)
                    .eq(Expr::Value(checkpoint.operation_id.clone().into()))
                    .and(
                        Expr::col(compaction_operation::Column::Owner)
                            .eq(Expr::Value(checkpoint.owner.clone().into())),
                    )
                    .and(Expr::cust_with_exprs(
                        "$1 IS NOT DISTINCT FROM $2",
                        [
                            Expr::col(compaction_operation::Column::ExpectedHead),
                            Expr::val(expected_head),
                        ],
                    )),
            )
            .into_tuple::<String>()
            .one(txn)
            .await?;
        let status: String = op.ok_or_else(|| anyhow::anyhow!("operation missing"))?;
        if status == "completed" {
            return Ok(CommitOutcome::AlreadyApplied);
        }
        if status != "running" {
            return Ok(CommitOutcome::Cancelled);
        }
        let projection_still_matches =
            checkpoint_has_projection(txn, &checkpoint.operation_id).await? == has_projection;
        if !projection_still_matches {
            return Ok(CommitOutcome::Stale);
        }
        let stopped = compaction_operation::Entity::find()
            .select_only()
            .join(
                JoinType::InnerJoin,
                compaction_operation::Entity::belongs_to(compaction_context::Entity)
                    .from(compaction_operation::Column::Owner)
                    .to(compaction_context::Column::Owner)
                    .into(),
            )
            .expr(Expr::col((
                compaction_operation::Entity,
                compaction_operation::Column::Id,
            )))
            .filter(
                Expr::col((
                    compaction_operation::Entity,
                    compaction_operation::Column::Id,
                ))
                .eq(Expr::Value(checkpoint.operation_id.clone().into()))
                .and(
                    Expr::col((
                        compaction_operation::Entity,
                        compaction_operation::Column::ExecutionTurn,
                    ))
                    .is_not_null(),
                )
                .and(Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_execution_stop::Entity, "stop")
                        .and_where(
                            Expr::col(("stop", compaction_execution_stop::Column::Owner))
                                .eq(Expr::col((
                                    compaction_context::Entity,
                                    compaction_context::Column::Owner,
                                )))
                                .and(
                                    Expr::col(("stop", compaction_execution_stop::Column::TurnId))
                                        .eq(Expr::col((
                                            compaction_operation::Entity,
                                            compaction_operation::Column::ExecutionTurn,
                                        ))),
                                ),
                        )
                        .to_owned(),
                )),
            )
            .into_tuple::<String>()
            .one(txn)
            .await?;
        if stopped.is_some() {
            return Ok(CommitOutcome::Cancelled);
        }
        if let Some(previous) = &checkpoint.previous {
            let previous_available = published_checkpoint(previous)
                .filter(compaction_checkpoint::Column::Owner.eq(&checkpoint.owner))
                .column(compaction_checkpoint::Column::Id)
                .into_tuple::<String>()
                .one(txn)
                .await?;
            if previous_available.is_none() {
                return Ok(CommitOutcome::Stale);
            }
        }
        // The legacy writer has no per-reference manifest. For an initial
        // raw-only checkpoint its captured epochs still fence unasserted
        // accepted context. Once a previous published checkpoint is the
        // basis, that checkpoint is atomic: old covered mutations may
        // advance a coarse thread epoch, while every newly covered raw
        // source below is still checked by its exact assertion.
        if checkpoint.previous.is_none() {
            // SeaORM cannot expand the operation's JSON object into
            // entries and correlate each one with its projection epoch.
            let dependency_changed = txn.query_one_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT o.id FROM compaction_operation o WHERE o.id=$1 AND EXISTS (
                    SELECT 1 FROM json_each(o.snapshot::json->'source_epochs') AS wanted
                    WHERE COALESCE((SELECT version FROM compaction_projection_epoch WHERE thread_id=wanted.key),0)<>wanted.value::text::bigint
                       OR NOT EXISTS (SELECT 1 FROM thread t JOIN compaction_context c ON c.workspace_id=t.workspace_id WHERE t.id=wanted.key AND c.owner=o.owner)
                 )",
                [checkpoint.operation_id.clone().into()],
            )).await?;
            if dependency_changed.is_some() {
                return Ok(CommitOutcome::Stale);
            }
        }
        for assertion in assertions {
            let valid = match assertion.kind {
                CanonicalSource::Input => {
                    source_assertion_matches::<
                        _,
                        turn_input::Entity,
                        compaction_input_revision::Entity,
                    >(
                        txn,
                        &checkpoint.owner,
                        assertion,
                        (
                            turn_input::Column::Id,
                            turn_input::Column::TurnId,
                            turn_input::Column::Payload,
                        ),
                        (
                            compaction_input_revision::Column::SourceId,
                            compaction_input_revision::Column::TurnId,
                            compaction_input_revision::Column::Revision,
                            compaction_input_revision::Column::Present,
                        ),
                    )
                    .await?
                }
                CanonicalSource::Event => {
                    source_assertion_matches::<
                        _,
                        turn_event::Entity,
                        compaction_event_revision::Entity,
                    >(
                        txn,
                        &checkpoint.owner,
                        assertion,
                        (
                            turn_event::Column::Id,
                            turn_event::Column::TurnId,
                            turn_event::Column::Payload,
                        ),
                        (
                            compaction_event_revision::Column::SourceId,
                            compaction_event_revision::Column::TurnId,
                            compaction_event_revision::Column::Revision,
                            compaction_event_revision::Column::Present,
                        ),
                    )
                    .await?
                }
                CanonicalSource::ProviderContext => {
                    source_assertion_matches::<
                        _,
                        turn_llm_context::Entity,
                        compaction_source_revision::Entity,
                    >(
                        txn,
                        &checkpoint.owner,
                        assertion,
                        (
                            turn_llm_context::Column::Id,
                            turn_llm_context::Column::TurnId,
                            turn_llm_context::Column::Payload,
                        ),
                        (
                            compaction_source_revision::Column::SourceId,
                            compaction_source_revision::Column::TurnId,
                            compaction_source_revision::Column::Revision,
                            compaction_source_revision::Column::Present,
                        ),
                    )
                    .await?
                }
                CanonicalSource::ToolItem => source_assertion_matches::<
                    _,
                    turn_item::Entity,
                    compaction_item_revision::Entity,
                >(
                    txn,
                    &checkpoint.owner,
                    assertion,
                    (
                        turn_item::Column::Id,
                        turn_item::Column::TurnId,
                        turn_item::Column::Payload,
                    ),
                    (
                        compaction_item_revision::Column::SourceId,
                        compaction_item_revision::Column::TurnId,
                        compaction_item_revision::Column::Revision,
                        compaction_item_revision::Column::Present,
                    ),
                )
                .await?,
            };
            if !valid {
                return Ok(CommitOutcome::Stale);
            }
        }
        let changed = compaction_context::Entity::update_many()
            .col_expr(
                compaction_context::Column::Head,
                Expr::Value(checkpoint.id.clone().into()),
            )
            .filter(
                Expr::col(compaction_context::Column::Owner)
                    .eq(Expr::Value(checkpoint.owner.clone().into()))
                    .and(Expr::cust_with_exprs(
                        "$1 IS NOT DISTINCT FROM $2",
                        [
                            Expr::col(compaction_context::Column::Head),
                            Expr::val(expected_head),
                        ],
                    ))
                    .and(Expr::col(compaction_context::Column::FormatVersion).eq(Expr::val(1_i64)))
                    .and(Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as(compaction_checkpoint::Entity, "p")
                            .and_where(
                                Expr::col(("p", compaction_checkpoint::Column::Id))
                                    .eq(Expr::Value(checkpoint.id.clone().into()))
                                    .and(
                                        Expr::col((
                                            "p",
                                            compaction_checkpoint::Column::OperationId,
                                        ))
                                        .eq(Expr::Value(checkpoint.operation_id.clone().into())),
                                    )
                                    .and(
                                        Expr::col(("p", compaction_checkpoint::Column::Owner))
                                            .eq(Expr::col(("compaction_context", "owner"))),
                                    )
                                    .and(
                                        Expr::col(("p", compaction_checkpoint::Column::Status))
                                            .eq(Expr::val("candidate")),
                                    ),
                            )
                            .to_owned(),
                    )),
            )
            .exec(txn)
            .await?;
        if changed.rows_affected != 1 {
            return Ok(CommitOutcome::Stale);
        }
        compaction_checkpoint::Entity::update_many()
            .col_expr(compaction_checkpoint::Column::Status, Expr::val("applied"))
            .filter(
                Expr::col(compaction_checkpoint::Column::Id)
                    .eq(Expr::Value(checkpoint.id.clone().into())),
            )
            .exec(txn)
            .await?;
        compaction_operation::Entity::update_many()
            .col_expr(compaction_operation::Column::Status, Expr::val("completed"))
            .col_expr(compaction_operation::Column::Outcome, Expr::val("applied"))
            .filter(
                Expr::col(compaction_operation::Column::Id)
                    .eq(Expr::Value(checkpoint.operation_id.clone().into())),
            )
            .exec(txn)
            .await?;
        Ok(CommitOutcome::Applied)
    }
}

// This bounded point check executes inside the existing commit transaction.
// All source bytes and checkpoint digests were prepared before writer capacity.
async fn source_assertion_matches<C: ConnectionTrait, E: EntityTrait, R: EntityTrait>(
    db: &C,
    owner: &str,
    assertion: &SourceAssertion,
    source_columns: (E::Column, E::Column, E::Column),
    revision_columns: (R::Column, R::Column, R::Column, R::Column),
) -> Result<bool> {
    let (id, turn_id, payload) = source_columns;
    let (revision_id, revision_turn, revision, present) = revision_columns;
    let mut query = E::find()
        .select_only()
        .column(id)
        .join(
            JoinType::InnerJoin,
            E::belongs_to(turn::Entity)
                .from(turn_id)
                .to(turn::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            thread::Entity::belongs_to(compaction_context::Entity)
                .from((thread::Column::Id, thread::Column::WorkspaceId))
                .to((
                    compaction_context::Column::ThreadId,
                    compaction_context::Column::WorkspaceId,
                ))
                .into(),
        )
        .filter(compaction_context::Column::Owner.eq(owner))
        .filter(turn_id.eq(assertion.turn_id.clone()))
        .filter(id.eq(assertion.id.clone()));
    if let Some(expected) = assertion.revision {
        query = query
            .join(
                JoinType::InnerJoin,
                E::belongs_to(R::default())
                    .from((id, turn_id))
                    .to((revision_id, revision_turn))
                    .into(),
            )
            .filter(revision.eq(expected))
            .filter(present.eq(1_i64));
    } else {
        query = query.filter(payload.eq(assertion.payload.clone()));
    }
    Ok(query.into_tuple::<String>().one(db).await?.is_some())
}

pub(crate) async fn compaction_apply(
    store: &crate::store::CompactionStore,
    checkpoint: &Checkpoint,
    expected_head: Option<&str>,
    assertions: &[SourceAssertion],
) -> Result<CommitOutcome> {
    // The legacy assertion writer has no frozen projection or manifest. Its
    // exact source assertions are the publication proof. If an operation did
    // bind a frozen projection, require its graph to be readable as the runner
    // publication path does; otherwise the legacy CAS remains unchanged.
    let has_projection =
        checkpoint_has_projection(&store.connection, &checkpoint.operation_id).await?;
    crate::checkpoint_proofs::prepare_graph(store, &checkpoint.id).await?;
    let application =
        prepare_checkpoint_application(checkpoint, expected_head, assertions, has_projection)?;
    store
        .retry_write_on_conflict(|| async {
            let txn = store.connection.begin().await?;
            let outcome = application.apply(&txn).await?;
            if outcome == CommitOutcome::Applied {
                txn.commit().await?;
            } else {
                txn.rollback().await?;
            }
            Ok(outcome)
        })
        .await
}
