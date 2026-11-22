//! Immutable candidate persistence and source ownership captured at admission.
use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use pioneer_compaction::{
    Checkpoint, FORMAT_VERSION, REPLAY_ALIAS_LIMIT as CHECKPOINT_SOURCE_LIMIT, SourceRef,
};
use pioneer_entity::{
    compaction_checkpoint, compaction_context as context, compaction_coverage,
    compaction_manifest as manifest, compaction_operation,
    compaction_operation_projection as projection, compaction_runner_plan as plan,
    compaction_runner_state as runner,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func, JoinType, OnConflict};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    RelationTrait, TransactionTrait,
};

use crate::{
    SOURCE_PAGE_BYTES, proof_admission::RawAdmissionRead, runner_writes::checkpoint_identity,
    sweep_work::retry_conflict,
};

/// Saves complete intermediate results without publishing a working pointer.
pub async fn compaction_save_candidate(
    db: &pioneer_postgres_pools::PostgresDatabase,
    checkpoint: &Checkpoint,
    portion: i64,
) -> Result<()> {
    ensure!(
        checkpoint.format_version == FORMAT_VERSION && !checkpoint.summary.trim().is_empty(),
        "invalid candidate"
    );
    ensure!(
        checkpoint.summary.len() <= SOURCE_PAGE_BYTES
            && checkpoint.coverage.len() <= CHECKPOINT_SOURCE_LIMIT,
        "candidate exceeds storage quantum"
    );
    let selection = serde_json::to_value(&checkpoint.selection)?;
    let identity = checkpoint_identity(checkpoint)?;
    let projection = i64::try_from(checkpoint.projection_version)?;
    let raw_ownership = prepare_raw_ownership(db, checkpoint).await?;
    let coverage: Vec<_> = checkpoint
        .coverage
        .iter()
        .map(|source| compaction_coverage::ActiveModel {
            checkpoint_id: sea_orm::Set(checkpoint.id.clone()),
            source_scope: sea_orm::Set(source.scope.clone()),
            source_id: sea_orm::Set(source.id.clone()),
            source_version: sea_orm::Set(source.version.clone()),
        })
        .collect();
    // The payload and coverage are prepared outside capacity. The running operation is revalidated below.
    retry_conflict(|| async {
        let txn = db.begin().await?;
        let running = compaction_operation::Entity::find()
            .select_only()
            .expr(Expr::col(compaction_operation::Column::Id))
            .filter(
                Expr::col(compaction_operation::Column::Id)
                    .eq(Expr::Value(checkpoint.operation_id.clone().into()))
                    .and(
                        Expr::col(compaction_operation::Column::Owner)
                            .eq(Expr::Value(checkpoint.owner.clone().into())),
                    )
                    .and(Expr::col(compaction_operation::Column::Status).eq(Expr::val("running"))),
            )
            .into_tuple::<String>()
            .one(&txn)
            .await?;
        ensure!(running.is_some(), "operation is not running");
        if let Some(rows) = &raw_ownership {
            write_raw_ownership(&txn, checkpoint, rows).await?;
        }
        compaction_checkpoint::Entity::insert(compaction_checkpoint::ActiveModel {
            proof_version: sea_orm::Set(0),
            id: sea_orm::Set((checkpoint.id.clone()).to_owned()),
            operation_id: sea_orm::Set((checkpoint.operation_id.clone()).to_owned()),
            owner: sea_orm::Set((checkpoint.owner.clone()).to_owned()),
            previous: sea_orm::Set(checkpoint.previous.clone()),
            portion: sea_orm::Set(portion),
            summary: sea_orm::Set((checkpoint.summary.clone()).to_owned()),
            selection: sea_orm::Set(selection.clone()),
            projection_version: sea_orm::Set(projection),
            format_version: sea_orm::Set(i64::from(FORMAT_VERSION)),
            identity_sha256: sea_orm::Set((identity.clone()).to_owned()),
            status: sea_orm::Set(("candidate").to_owned()),
        })
        .on_conflict(
            OnConflict::columns([
                compaction_checkpoint::Column::OperationId,
                compaction_checkpoint::Column::Portion,
            ])
            .do_nothing()
            .to_owned(),
        )
        .exec_without_returning(&txn)
        .await?;
        // Idempotency is exact, never silently accept a different result for the same portion.
        let exact = compaction_checkpoint::Entity::find()
            .select_only()
            .expr(Expr::col(compaction_checkpoint::Column::Id))
            .filter(
                Expr::col(compaction_checkpoint::Column::Id)
                    .eq(Expr::Value(checkpoint.id.clone().into()))
                    .and(
                        Expr::col(compaction_checkpoint::Column::OperationId)
                            .eq(Expr::Value(checkpoint.operation_id.clone().into())),
                    )
                    .and(
                        Expr::col(compaction_checkpoint::Column::Portion)
                            .eq(Expr::Value(portion.into())),
                    )
                    .and(
                        Expr::col(compaction_checkpoint::Column::IdentitySha256)
                            .eq(Expr::Value(identity.clone().into())),
                    ),
            )
            .into_tuple::<String>()
            .one(&txn)
            .await?;
        ensure!(exact.is_some(), "candidate idempotency conflict");
        for model in &coverage {
            compaction_coverage::Entity::insert(model.clone())
                .on_conflict(OnConflict::new().do_nothing().to_owned())
                .exec_without_returning(&txn)
                .await?;
        }
        compaction_operation::Entity::update_many()
            .col_expr(
                compaction_operation::Column::NextPortion,
                Expr::expr(Func::cust(Alias::new("greatest")).args([
                    Expr::col(compaction_operation::Column::NextPortion),
                    Expr::Value((portion + 1).into()),
                ])),
            )
            .filter(
                Expr::col(compaction_operation::Column::Id)
                    .eq(Expr::Value(checkpoint.operation_id.clone().into())),
            )
            .exec(&txn)
            .await?;
        txn.commit().await?;
        Ok(())
    })
    .await
}

async fn prepare_raw_ownership(
    db: &pioneer_postgres_pools::PostgresDatabase,
    checkpoint: &pioneer_compaction::Checkpoint,
) -> Result<Option<Vec<(i64, SourceRef, String)>>> {
    if crate::source_projection::projection_identity(db, &checkpoint.operation_id)
        .await?
        .is_some()
    {
        return Ok(None);
    }
    let mut admission = RawAdmissionRead::new(db, &checkpoint.operation_id).await?;
    while !admission.is_complete() {
        admission.step(db).await?;
    }
    let owner = admission.owner().to_owned();
    let original = admission.finish()?;
    ensure!(
        owner == checkpoint.owner
            && original.id == checkpoint.operation_id
            && original.owner == checkpoint.owner
            && !original.plan.compact.is_empty()
            && !original.plan.coverage.is_empty(),
        "raw candidate has no original assertion admission"
    );
    let workspace = context::Entity::find_by_id(&checkpoint.owner)
        .select_only()
        .column(context::Column::WorkspaceId)
        .into_tuple::<String>()
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("raw context missing"))?;
    // Coverage proves membership, not the order of an already prepared
    // manifest. Its positions are used only for unclaimed legacy raw assertions.
    let mut bootstrap_ordinals = BTreeMap::new();
    for (ordinal, source) in original.plan.coverage.iter().enumerate() {
        bootstrap_ordinals.entry(source).or_insert(ordinal);
    }
    let mut rows = Vec::new();
    for source in &checkpoint.coverage {
        let ordinal = *bootstrap_ordinals
            .get(source)
            .ok_or_else(|| anyhow::anyhow!("raw candidate exceeds original admission"))?;
        let thread = crate::source_revision::compaction_reference_thread(db, &workspace, source)
            .await?
            .ok_or_else(|| anyhow::anyhow!("raw candidate assertion ownership unavailable"))?;
        rows.push((i64::try_from(ordinal)?, source.clone(), thread));
    }
    Ok(Some(rows))
}
async fn write_raw_ownership<C: ConnectionTrait>(
    db: &C,
    checkpoint: &pioneer_compaction::Checkpoint,
    rows: &[(i64, SourceRef, String)],
) -> Result<()> {
    ensure!(
        projection::Entity::find_by_id(&checkpoint.operation_id)
            .select_only()
            .column(projection::Column::OperationId)
            .into_tuple::<String>()
            .one(db)
            .await?
            .is_none(),
        "raw admission unexpectedly acquired a projection"
    );
    let workspace = context::Entity::find_by_id(&checkpoint.owner)
        .select_only()
        .column(context::Column::WorkspaceId)
        .into_tuple::<String>()
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("raw context lost"))?;
    let mut bootstrap_allowed = None;
    for (bootstrap_ordinal, source, thread) in rows {
        ensure!(
            crate::source_revision::compaction_reference_thread(db, &workspace, source)
                .await?
                .as_ref()
                == Some(thread),
            "raw assertion ownership changed"
        );
        let selected_source = Condition::all()
            .add(manifest::Column::OperationId.eq(&checkpoint.operation_id))
            .add(manifest::Column::ReferenceOnly.eq(0))
            .add(manifest::Column::SourceScope.eq(&source.scope))
            .add(manifest::Column::SourceId.eq(&source.id))
            .add(manifest::Column::SourceVersion.eq(&source.version));
        // The existing source index supports this exact selected identity.
        // Return only scalar metadata (including an owner equality), never
        // source payloads. Two rows suffice to reject ambiguous ownership.
        let owners = manifest::Entity::find()
            .select_only()
            .columns([manifest::Column::Ordinal, manifest::Column::UnitOrdinal])
            .expr(Expr::col(manifest::Column::SourceThread).eq(thread.clone()))
            .filter(selected_source.clone())
            .limit(2)
            .into_tuple::<(i64, i64, bool)>()
            .all(db)
            .await?;
        if !owners.is_empty() {
            ensure!(
                owners.len() == 1 && owners[0].0 >= 0 && owners[0].1 >= 0 && owners[0].2,
                "raw historical ownership conflict"
            );
            // Ordinal/unit belong to this row, not to coverage order. An exact
            // retry must not INSERT or rewrite the historical manifest.
            continue;
        }
        let can_bootstrap = match bootstrap_allowed {
            Some(allowed) => allowed,
            None => {
                // Prepared/incomplete runners require their existing manifest.
                // A genuine raw operation can assert another originally admitted
                // source in a later portion; check that source's history below.
                let allowed = plan::Entity::find_by_id(&checkpoint.operation_id)
                    .select_only()
                    .expr(Expr::val(1_i64))
                    .into_tuple::<i64>()
                    .one(db)
                    .await?
                    .is_none()
                    && runner::Entity::find_by_id(&checkpoint.operation_id)
                        .select_only()
                        .expr(Expr::val(1_i64))
                        .into_tuple::<i64>()
                        .one(db)
                        .await?
                        .is_none()
                    // Both boundary seeks use the operation/role index. The
                    // PostgreSQL bigint column rejects fractional roles.
                    && manifest::Entity::find()
                        .select_only()
                        .expr(Expr::col(manifest::Column::ReferenceOnly).is_in([0, 1]))
                        .filter(manifest::Column::OperationId.eq(&checkpoint.operation_id))
                        .order_by_asc(manifest::Column::ReferenceOnly)
                        .limit(1)
                        .into_tuple::<bool>()
                        .one(db)
                        .await?
                        .is_none_or(|valid| valid)
                    && manifest::Entity::find()
                        .select_only()
                        .expr(Expr::col(manifest::Column::ReferenceOnly).is_in([0, 1]))
                        .filter(manifest::Column::OperationId.eq(&checkpoint.operation_id))
                        .order_by_desc(manifest::Column::ReferenceOnly)
                        .limit(1)
                        .into_tuple::<bool>()
                        .one(db)
                        .await?
                        .is_none_or(|valid| valid);
                bootstrap_allowed = Some(allowed);
                allowed
            }
        };
        ensure!(
            can_bootstrap,
            "raw candidate missing expected manifest ownership"
        );
        // Do not treat a different version/role as a fresh assertion. Both
        // supported roles use the existing source index, without payload reads.
        // Historical claims are checked independently of version: lost or
        // corrupt ownership cannot be reconstructed under an old checkpoint.
        ensure!(
            manifest::Entity::find()
                .select_only()
                .expr(Expr::val(1_i64))
                .filter(manifest::Column::OperationId.eq(&checkpoint.operation_id))
                .filter(manifest::Column::ReferenceOnly.is_in([0, 1]))
                .filter(manifest::Column::SourceScope.eq(&source.scope))
                .filter(manifest::Column::SourceId.eq(&source.id))
                .limit(1)
                .into_tuple::<i64>()
                .one(db)
                .await?
                .is_none()
                && compaction_coverage::Entity::find()
                    .select_only()
                    .expr(Expr::val(1_i64))
                    .join(
                        JoinType::InnerJoin,
                        compaction_coverage::Relation::CompactionCheckpoint.def(),
                    )
                    .filter(compaction_checkpoint::Column::OperationId.eq(&checkpoint.operation_id))
                    .filter(compaction_coverage::Column::SourceScope.eq(&source.scope))
                    .filter(compaction_coverage::Column::SourceId.eq(&source.id))
                    .limit(1)
                    .into_tuple::<i64>()
                    .one(db)
                    .await?
                    .is_none(),
            "raw candidate missing expected manifest ownership"
        );
        let exact = selected_source
            .add(manifest::Column::Ordinal.eq(*bootstrap_ordinal))
            .add(manifest::Column::UnitOrdinal.eq(*bootstrap_ordinal))
            .add(manifest::Column::SourceThread.eq(thread));
        manifest::Entity::insert(manifest::ActiveModel {
            operation_id: sea_orm::Set(checkpoint.operation_id.clone()),
            ordinal: sea_orm::Set(*bootstrap_ordinal),
            unit_ordinal: sea_orm::Set(*bootstrap_ordinal),
            reference_only: sea_orm::Set(0),
            source_thread: sea_orm::Set(thread.clone()),
            source_scope: sea_orm::Set(source.scope.clone()),
            source_id: sea_orm::Set(source.id.clone()),
            source_version: sea_orm::Set(source.version.clone()),
            ..Default::default()
        })
        .on_conflict(OnConflict::new().do_nothing().to_owned())
        .exec_without_returning(db)
        .await?;
        ensure!(
            manifest::Entity::find()
                .select_only()
                .column(manifest::Column::OperationId)
                .filter(exact)
                .into_tuple::<String>()
                .one(db)
                .await?
                .is_some(),
            "raw historical ownership conflict"
        );
    }
    Ok(())
}
