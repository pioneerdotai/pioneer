//! Frozen-history manifests are populated in bounded restart-safe quanta. An
//! incomplete manifest cannot be referenced by a started execution snapshot.
use super::compaction::*;
use super::compaction_frozen_views::message as compaction_frozen_message;
use crate::{CrudStore, FrozenUseGuard};
use anyhow::{Result, ensure};
use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenMessageRef};
use pioneer_entity::{compaction_frozen_history, compaction_frozen_message_data, thread};
use sea_orm::sea_query::{Expr, ExprTrait, OnConflict, Query};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect};
use sea_orm::{ConnectionTrait, TransactionTrait};

pub(crate) async fn compaction_begin_frozen_history_with_imports<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    owner_thread: &str,
    descriptor: &FrozenHistoryRef,
    imports: u64,
    imports_sha256: &str,
) -> Result<()> {
    ensure!(
        descriptor.format == 1
            && !descriptor.manifest_id.is_empty()
            && descriptor.identity_sha256.len() == 64
            && descriptor
                .identity_sha256
                .bytes()
                .all(|c| c.is_ascii_hexdigit()),
        "invalid frozen history descriptor"
    );
    ensure!(
        imports_sha256.len() == 64 && imports_sha256.bytes().all(|c| c.is_ascii_hexdigit()),
        "invalid import digest"
    );
    let messages = i64::try_from(descriptor.messages)?;
    db.execute(
        &Query::insert()
            .into_table(compaction_frozen_history::Entity)
            .columns([
                compaction_frozen_history::Column::Id,
                compaction_frozen_history::Column::WorkspaceId,
                compaction_frozen_history::Column::OwnerThread,
                compaction_frozen_history::Column::IdentitySha256,
                compaction_frozen_history::Column::MessageCount,
                compaction_frozen_history::Column::ImportCount,
                compaction_frozen_history::Column::ImportsSha256,
            ])
            .select_from(
                thread::Entity::find()
                    .select_only()
                    .expr(Expr::Value(descriptor.manifest_id.clone().into()))
                    .expr(Expr::Value(workspace.into()))
                    .expr(Expr::Value(owner_thread.into()))
                    .expr(Expr::Value(descriptor.identity_sha256.clone().into()))
                    .expr(Expr::Value(messages.into()))
                    .expr(Expr::Value(i64::try_from(imports)?.into()))
                    .expr(Expr::Value(imports_sha256.into()))
                    .filter(
                        thread::Column::Id
                            .eq(owner_thread)
                            .and(thread::Column::WorkspaceId.eq(workspace)),
                    )
                    .into_query(),
            )?
            .on_conflict(OnConflict::columns(["id"]).do_nothing().to_owned())
            .to_owned(),
    )
    .await?;
    let row = compaction_frozen_history::Entity::find_by_id(descriptor.manifest_id.clone())
        .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
        .filter(compaction_frozen_history::Column::OwnerThread.eq(owner_thread))
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("frozen history scope is unavailable"))?;
    ensure!(
        row.identity_sha256 == descriptor.identity_sha256
            && row.message_count == messages
            && row.import_count == i64::try_from(imports)?
            && row.imports_sha256 == imports_sha256,
        "frozen history identity collision"
    );
    Ok(())
}

/// The prepared JSON contains typed references and hashes only. Before DB
/// admission it is validated and bounded; the transaction revalidates the
/// manifest owner/readiness and exact existing bytes on an idempotent retry.
pub(crate) async fn compaction_append_frozen_history(
    store: &CrudStore,
    workspace: &str,
    owner_thread: &str,
    manifest: &str,
    start: u64,
    messages: &[FrozenMessageRef],
    guard: &FrozenUseGuard,
) -> Result<()> {
    ensure!(
        guard.workspace() == workspace
            && guard.owner() == owner_thread
            && guard.header().id == manifest,
        "frozen append use scope mismatch"
    );
    guard.validate_in(&store.connection, false).await?;
    ensure!(
        messages.len() as u64 <= SOURCE_PAGE_ROWS,
        "frozen history batch row limit"
    );
    let mut batch = Vec::new();
    let mut total = 0;
    for (index, message) in messages.iter().enumerate() {
        message.validate()?;
        let json = serde_json::to_string(message)?;
        total += json.len();
        ensure!(
            total <= SOURCE_PAGE_BYTES,
            "frozen history batch byte limit"
        );
        batch.push((
            i64::try_from(
                start
                    .checked_add(index as u64)
                    .ok_or_else(|| anyhow::anyhow!("frozen ordinal overflow"))?,
            )?,
            json,
        ));
    }
    let tx = store.connection.begin().await?;
    guard.validate_in(&tx, false).await?;
    let row = compaction_frozen_history::Entity::find_by_id(manifest)
        .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
        .filter(compaction_frozen_history::Column::OwnerThread.eq(owner_thread))
        .one(&tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("frozen history owner is unavailable"))?;
    let ready = row.ready != 0;
    let count = row.message_count;
    let next = row.next_ordinal;
    let start = i64::try_from(start)?;
    let end = start
        .checked_add(i64::try_from(batch.len())?)
        .ok_or_else(|| anyhow::anyhow!("frozen ordinal overflow"))?;
    ensure!(
        start <= next && end <= count && (start == next || end <= next),
        "frozen history append is not sequential or an exact retry"
    );
    for (ordinal, json) in &batch {
        ensure!(
            *ordinal < count,
            "frozen history ordinal exceeds declared count"
        );
        if !ready && *ordinal >= next {
            let source =
                super::compaction_frozen_storage::append_source(&tx, manifest, 0, *ordinal).await?;
            compaction_frozen_message_data::Entity::insert(
                compaction_frozen_message_data::ActiveModel {
                    manifest_id: sea_orm::Set(source),
                    ordinal: sea_orm::Set(*ordinal),
                    reference_json: sea_orm::Set((json.clone()).to_owned()),
                    bytes: sea_orm::Set(json.len() as i64),
                },
            )
            .on_conflict(
                OnConflict::columns([
                    compaction_frozen_message_data::Column::ManifestId,
                    compaction_frozen_message_data::Column::Ordinal,
                ])
                .do_nothing()
                .to_owned(),
            )
            .exec_without_returning(&tx)
            .await?;
        }
        let matches =
            compaction_frozen_message::Entity::find_by_id((manifest.to_owned(), *ordinal))
                .select_only()
                .column(compaction_frozen_message::Column::Ordinal)
                .filter(compaction_frozen_message::Column::ReferenceJson.eq(json.clone()))
                .into_tuple::<i64>()
                .one(&tx)
                .await?
                .is_some();
        ensure!(matches, "frozen history retry changed an immutable entry");
    }
    if start == next && !ready {
        compaction_frozen_history::Entity::update_many()
            .col_expr(
                compaction_frozen_history::Column::NextOrdinal,
                Expr::Value(end.into()),
            )
            .filter(
                Expr::col(compaction_frozen_history::Column::Id)
                    .eq(Expr::Value(manifest.into()))
                    .and(
                        Expr::col(compaction_frozen_history::Column::NextOrdinal)
                            .eq(Expr::Value(next.into())),
                    )
                    .and(Expr::col(compaction_frozen_history::Column::Ready).eq(Expr::val(0_i64))),
            )
            .exec(&tx)
            .await?;
    }
    guard.validate_in(&tx, false).await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn compaction_frozen_history_owner<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    descriptor: &FrozenHistoryRef,
) -> Result<Option<String>> {
    use pioneer_entity::compaction_frozen_history as history;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    ensure!(descriptor.format == 1, "unsupported frozen history format");
    let row = history::Entity::find_by_id(descriptor.manifest_id.clone())
        .filter(history::Column::WorkspaceId.eq(workspace))
        .filter(history::Column::Ready.eq(1_i64))
        .filter(history::Column::Availability.eq("resident"))
        .filter(history::Column::IdentitySha256.eq(descriptor.identity_sha256.clone()))
        .filter(history::Column::MessageCount.eq(i64::try_from(descriptor.messages)?))
        .one(db)
        .await?;
    Ok(row.map(|row| row.owner_thread))
}

use sea_orm::QueryTrait;
