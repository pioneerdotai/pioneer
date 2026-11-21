//! Exact checkpoint observations and publication of prepared evidence.
use anyhow::{Result, ensure};
use pioneer_entity::{
    compaction_checkpoint as checkpoint, compaction_context as context,
    compaction_frozen_history as history, compaction_operation as operation,
    compaction_operation_projection as projection, compaction_runner_plan as plan,
    compaction_runner_state as runner,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, JoinType, LockType, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, FromQueryResult, QueryFilter,
    QuerySelect, QueryTrait,
};
use std::collections::BTreeSet;

use crate::proof_graph::CheckpointEdgesRow;

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult)]
pub struct ProofControl {
    pub owner: String,
    pub status: String,
    pub generation: Option<i64>,
}
pub async fn read_proof_control<C: ConnectionTrait>(db: &C, id: &str) -> Result<ProofControl> {
    let query = Query::select()
        .from(operation::Entity)
        .columns(
            [operation::Column::Owner, operation::Column::Status]
                .map(|column| (operation::Entity, column)),
        )
        .column((runner::Entity, runner::Column::Generation))
        .join(
            JoinType::LeftJoin,
            runner::Entity,
            Expr::col((runner::Entity, runner::Column::OperationId))
                .eq(Expr::col((operation::Entity, operation::Column::Id))),
        )
        .and_where(Expr::col((operation::Entity, operation::Column::Id)).eq(id))
        .limit(1)
        .to_owned();
    let row = db
        .query_one(&query)
        .await?
        .ok_or_else(|| anyhow::anyhow!("proof operation missing"))?;
    Ok(ProofControl::from_query_result(&row, "")?)
}

pub async fn checkpoint_row<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<CheckpointEdgesRow>> {
    let query = Query::select()
        .from(checkpoint::Entity)
        .columns(
            [
                checkpoint::Column::Id,
                checkpoint::Column::ProofVersion,
                checkpoint::Column::Status,
                checkpoint::Column::Owner,
                checkpoint::Column::IdentitySha256,
                checkpoint::Column::Previous,
                checkpoint::Column::FormatVersion,
                checkpoint::Column::OperationId,
            ]
            .map(|column| (checkpoint::Entity, column)),
        )
        .expr_as(
            Expr::col((checkpoint::Entity, checkpoint::Column::Portion)).lt(Expr::col((
                operation::Entity,
                operation::Column::NextPortion,
            ))),
            Alias::new("coverage_closed"),
        )
        .columns(
            [context::Column::WorkspaceId, context::Column::ThreadId]
                .map(|column| (context::Entity, column)),
        )
        .column((projection::Entity, projection::Column::ManifestId))
        .join(
            JoinType::InnerJoin,
            operation::Entity,
            Expr::col((operation::Entity, operation::Column::Id))
                .eq(Expr::col((
                    checkpoint::Entity,
                    checkpoint::Column::OperationId,
                )))
                .and(
                    Expr::col((operation::Entity, operation::Column::Owner))
                        .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
                ),
        )
        .join(
            JoinType::InnerJoin,
            context::Entity,
            Expr::col((context::Entity, context::Column::Owner))
                .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
        )
        .join(
            JoinType::LeftJoin,
            projection::Entity,
            Expr::col((projection::Entity, projection::Column::OperationId)).eq(Expr::col((
                checkpoint::Entity,
                checkpoint::Column::OperationId,
            ))),
        )
        .and_where(Expr::col((checkpoint::Entity, checkpoint::Column::Id)).eq(id))
        .limit(1)
        .to_owned();
    db.query_one(&query)
        .await?
        .map(|row| CheckpointEdgesRow::from_query_result(&row, ""))
        .transpose()
        .map_err(Into::into)
}

/// Only close and seal may advance this reader observation. Status remains
/// exact: publication/Stop is a control transition, not an identity refresh.
/// The closed portion guards the full coverage and selected ownership set.
pub async fn refresh_checkpoint_row<C: ConnectionTrait>(
    db: &C,
    observed: &CheckpointEdgesRow,
) -> Result<CheckpointEdgesRow> {
    let current = checkpoint_row(db, &observed.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint identity disappeared during read"))?;
    ensure!(
        matches!(observed.proof_version, 0 | 1)
            && matches!(current.proof_version, 0 | 1)
            && current.proof_version >= observed.proof_version
            && (!observed.coverage_closed || current.coverage_closed)
            && (current.proof_version == 0 || current.coverage_closed),
        "unsupported or regressed checkpoint readiness"
    );
    let mut identity = current.clone();
    identity.proof_version = observed.proof_version;
    identity.coverage_closed = observed.coverage_closed;
    ensure!(
        identity == *observed,
        "checkpoint identity changed between topology pages"
    );
    Ok(current)
}

/// Seal the exact control/identity and dependencies verified during preflight.
/// The caller supplies the already-scanned dependencies and the origin proof.
pub async fn seal_checkpoint(
    tx: &DatabaseTransaction,
    row: &CheckpointEdgesRow,
    expected_control: &ProofControl,
    header: Option<&history::Model>,
    foreign: &BTreeSet<(&str, &str, &str)>,
) -> Result<()> {
    // Use the trigger's lock order. The operation lock fences plan
    // and projection changes and the origin lock fences expiry.
    ensure!(
        checkpoint::Entity::find_by_id(&row.id)
            .select_only()
            .column(checkpoint::Column::Id)
            .lock(LockType::NoKeyUpdate)
            .into_tuple::<String>()
            .one(tx)
            .await?
            .is_some(),
        "proof candidate disappeared"
    );
    ensure!(
        context::Entity::find_by_id(&row.owner)
            .select_only()
            .column(context::Column::Owner)
            .lock_shared()
            .into_tuple::<String>()
            .one(tx)
            .await?
            .is_some(),
        "proof context disappeared"
    );
    ensure!(
        operation::Entity::find_by_id(&row.operation_id)
            .select_only()
            .column(operation::Column::Id)
            .lock(LockType::NoKeyUpdate)
            .into_tuple::<String>()
            .one(tx)
            .await?
            .is_some(),
        "proof operation disappeared"
    );
    if let Some(header) = header {
        ensure!(
            history::Entity::find_by_id(&header.id)
                .select_only()
                .column(history::Column::Id)
                .lock_shared()
                .into_tuple::<String>()
                .one(tx)
                .await?
                .is_some(),
            "proof origin disappeared"
        );
    }
    ensure!(
        read_proof_control(tx, &row.operation_id).await? == *expected_control,
        "proof operation changed during preparation"
    );
    let mut current = checkpoint_row(tx, &row.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("proof candidate disappeared"))?;
    ensure!(
        matches!(current.proof_version, 0 | 1),
        "unsupported proof version at seal"
    );
    current.proof_version = row.proof_version;
    ensure!(current == *row, "proof identity/status/binding changed");
    ensure!(
        current.coverage_closed,
        "proof portion lost its immutable boundary"
    );
    if let Some(previous) = &row.previous {
        ensure!(
            checkpoint::Entity::find_by_id(previous)
                .select_only()
                .column(checkpoint::Column::Id)
                .filter(checkpoint::Column::Owner.eq(&row.owner))
                .filter(checkpoint::Column::ProofVersion.eq(1))
                .into_tuple::<String>()
                .one(tx)
                .await?
                .is_some(),
            "previous proof changed before seal"
        );
    }
    if !foreign.is_empty() {
        let dependencies = checkpoint::Entity::find()
            .select_only()
            .columns([
                checkpoint::Column::Id,
                checkpoint::Column::Owner,
                checkpoint::Column::IdentitySha256,
            ])
            .filter(checkpoint::Column::Id.is_in(foreign.iter().map(|entry| entry.0)))
            .filter(checkpoint::Column::ProofVersion.eq(1))
            .into_tuple::<(String, String, String)>()
            .all(tx)
            .await?;
        let dependencies = dependencies
            .iter()
            .map(|(id, owner, identity)| (id.as_str(), owner.as_str(), identity.as_str()))
            .collect::<BTreeSet<_>>();
        ensure!(
            dependencies == *foreign,
            "foreign proof changed before seal"
        );
    }
    if let Some(header) = header {
        let binding = projection::Entity::find()
            .select_only()
            .column(projection::Column::OperationId)
            .filter(projection::Column::OperationId.eq(&row.operation_id))
            .filter(projection::Column::ManifestId.eq(&header.id))
            .filter(projection::Column::IdentitySha256.eq(&header.identity_sha256))
            .filter(projection::Column::ImportsSha256.eq(&header.imports_sha256))
            .filter(projection::Column::ImportCount.eq(header.import_count))
            .into_query();
        let exact = history::Entity::find_by_id(&header.id)
            .select_only()
            .column(history::Column::Id)
            .filter(history::Column::WorkspaceId.eq(&header.workspace_id))
            .filter(history::Column::OwnerThread.eq(&header.owner_thread))
            .filter(history::Column::IdentitySha256.eq(&header.identity_sha256))
            .filter(history::Column::MessageCount.eq(header.message_count))
            .filter(history::Column::ImportsSha256.eq(&header.imports_sha256))
            .filter(history::Column::ImportCount.eq(header.import_count))
            .filter(history::Column::Ready.eq(1))
            .filter(history::Column::Expired.eq(0))
            .filter(history::Column::NextOrdinal.eq(header.message_count))
            .filter(history::Column::NextImport.eq(header.import_count))
            .filter(Expr::exists(binding))
            .into_tuple::<String>()
            .one(tx)
            .await?;
        ensure!(
            exact.is_some(),
            "proof origin changed or became unavailable"
        );
        ensure!(
            plan::Entity::find_by_id(&row.operation_id)
                .select_only()
                .column(plan::Column::OperationId)
                .filter(plan::Column::Ready.eq(1))
                .into_tuple::<String>()
                .one(tx)
                .await?
                .is_some(),
            "proof origin plan lost readiness"
        );
    }
    // Coverage/ownership/binding/identity are immutable behind the
    // closed portion. Staging writers add only exact selected values;
    // cleanup requires expired origin + no reader/active operation.
    // Every checked dependency has a permanent incoming edge from
    // this frozen node, which forbids demotion during publication.
    checkpoint::Entity::update_many()
        .col_expr(checkpoint::Column::ProofVersion, Expr::val(1))
        .filter(checkpoint::Column::Id.eq(&row.id))
        .filter(checkpoint::Column::ProofVersion.eq(0))
        .exec(tx)
        .await?;
    Ok(())
}
