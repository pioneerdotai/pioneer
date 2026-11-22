//! Bounded verification of frozen preview dependencies and import membership.
use crate::store::CompactionStore;
use anyhow::{Result, ensure};
use pioneer_crud_artifacts::html_preview_io;
use pioneer_crud_artifacts::html_preview_retention_queries::{self as queries, col};
use pioneer_entity::{
    compaction_frozen_history as history, html_preview_frozen_index as frozen_index,
    html_preview_frozen_result as frozen_result, html_preview_frozen_turn as frozen_turn,
};
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, is_retryable_database_conflict,
    retry_with_backoff,
};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, OnConflict, Order, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
    Set, TransactionTrait,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, VecDeque},
    future::Future,
    time::Duration,
};

pub async fn frontier<C: ConnectionTrait>(db: &C) -> Result<String> {
    Ok(history::Entity::find()
        .select_only()
        .column(history::Column::Id)
        .order_by_desc(history::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?
        .unwrap_or_default())
}

async fn retry_index_write<T, F, Fut>(operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    retry_with_backoff(
        operation,
        |error| is_retryable_database_conflict(error) || html_preview_io::is_conflict(error),
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

/// One immutable manifest page per call. Existing logical views include shared
/// backing ranges. Completed headers cannot change identity; expiry only drops
/// a hold. The short commit rechecks the header and progress before publication.
pub async fn index_quantum(
    store: &CompactionStore,
    io_wake: &tokio::sync::Notify,
    after: &mut String,
    imports: &mut Option<PreviewImportIndex>,
    upper: &str,
) -> Result<bool> {
    if let Some(state) = imports.as_ref() {
        let manifest = state.header.manifest.clone();
        if index_import_quantum(store, imports).await? {
            *after = manifest;
        }
        return Ok(true);
    }
    let previous_after = after.clone();
    let mut query = Query::select()
        .columns([
            ("h", history::Column::Id),
            ("h", history::Column::WorkspaceId),
            ("h", history::Column::OwnerThread),
            ("h", history::Column::IdentitySha256),
            ("h", history::Column::MessageCount),
            ("h", history::Column::ImportCount),
            ("h", history::Column::ImportsSha256),
        ])
        .expr_as(col("i", frozen_index::Column::Unknown), "unknown")
        .expr_as(col("i", frozen_index::Column::NextOrdinal), "next_ordinal")
        .limit(1)
        .to_owned();
    // The manifest triggers create the marker with the header. Discover only
    // active work using the predicate of the partial index.
    query
        .from_as(frozen_index::Entity, "i")
        .join_as(
            JoinType::InnerJoin,
            history::Entity,
            "h",
            col("h", history::Column::Id).eq(col("i", frozen_index::Column::ManifestId)),
        )
        .and_where(col("i", frozen_index::Column::Expired).eq(Expr::Constant(0.into())))
        .and_where(col("i", frozen_index::Column::Ready).eq(Expr::Constant(1.into())))
        .and_where(
            col("i", frozen_index::Column::Complete)
                .ne(Expr::Constant(1.into()))
                .or(col("i", frozen_index::Column::Unknown).eq(Expr::Constant(1.into()))),
        )
        .and_where(col("i", frozen_index::Column::ManifestId).gt(after.as_str()))
        .and_where(col("i", frozen_index::Column::ManifestId).lte(upper))
        .order_by(("i", frozen_index::Column::ManifestId), Order::Asc);
    let row = store.connection.query_one(&query).await?;
    let Some(row) = row else {
        after.clear();
        return Ok(false);
    };
    *after = row.try_get("", "id")?;
    let id: String = row.try_get("", "id")?;
    let workspace: String = row.try_get("", "workspace_id")?;
    let owner: String = row.try_get("", "owner_thread")?;
    let count: i64 = row.try_get("", "message_count")?;
    let identity: String = row.try_get("", "identity_sha256")?;
    let next: i64 = row.try_get("", "next_ordinal")?;
    let import_count: i64 = row.try_get("", "import_count")?;
    if row.try_get::<i64>("", "unknown")? == 1 {
        if import_count > 0 {
            // Repeat the entire walk so a previously uncertain page cannot
            // disappear from the proof. Incomplete markers retain media while
            // the bounded recheck is in progress.
            let restarted = retry_index_write(|| async {
                let changed = frozen_index::Entity::update_many()
                    .col_expr(frozen_index::Column::NextOrdinal, Expr::val(0))
                    .col_expr(frozen_index::Column::Complete, Expr::val(0))
                    .col_expr(frozen_index::Column::Unknown, Expr::val(0))
                    .filter(frozen_index::Column::ManifestId.eq(&id))
                    .filter(frozen_index::Column::WorkspaceId.eq(&workspace))
                    .filter(frozen_index::Column::IdentitySha256.eq(&identity))
                    .filter(frozen_index::Column::MessageCount.eq(count))
                    .filter(frozen_index::Column::NextOrdinal.eq(next))
                    .filter(frozen_index::Column::Ready.eq(1_i64))
                    .filter(frozen_index::Column::Expired.eq(0_i64))
                    .filter(frozen_index::Column::Unknown.eq(1))
                    .exec(&store.connection)
                    .await?;
                Ok(changed.rows_affected == 1)
            })
            .await?;
            if restarted {
                *after = previous_after;
            }
        }
        return Ok(true);
    }
    if next == count && import_count > 0 {
        *imports = Some(PreviewImportIndex {
            header: PreviewImportHeader {
                manifest: id.clone(),
                workspace,
                owner,
                identity,
                messages: count,
                count: u64::try_from(import_count)?,
                expected: row.try_get("", "imports_sha256")?,
            },
            next: 0,
            digest: Sha256::new(),
            page: VecDeque::new(),
            pending: None,
        });
        // Keep discovery before this manifest until verification or its guarded
        // terminal write returns. An error/cancellation must replay from zero.
        *after = previous_after;
        if index_import_quantum(store, imports).await? {
            *after = id;
        }
        return Ok(true);
    }
    let page = crate::frozen_capture::compaction_frozen_history_page(
        &store.connection,
        &workspace,
        &owner,
        &id,
        u64::try_from(next)?,
    )
    .await;
    let mut turns = BTreeSet::new();
    let mut results = Vec::new();
    let mut unknown = false;
    let end = match page {
        Ok(page) => {
            for (offset, reference) in page.iter().enumerate() {
                // Legacy opaque Task input may contain media. Keep it until its
                // existing owner expires it, rather than infer an absent image.
                if reference
                    .sources
                    .iter()
                    .any(|s| s.scope.starts_with("task-basis:"))
                {
                    unknown = true;
                }
                if reference.tool_name.as_deref() != Some("html_preview") {
                    continue;
                }
                let mut found = false;
                for source in reference
                    .sources
                    .iter()
                    .chain(reference.replay_source.iter())
                {
                    if let Some(turn) = source
                        .scope
                        .strip_prefix("context:")
                        .or_else(|| source.scope.strip_prefix("item:"))
                    {
                        turns.insert(turn.to_owned());
                        found = true;
                    }
                }
                if !found {
                    unknown = true;
                }
                let source = reference
                    .replay_source
                    .as_ref()
                    .or_else(|| (reference.sources.len() == 1).then(|| &reference.sources[0]));
                if let Some(source) = source.filter(|s| s.scope.starts_with("context:")) {
                    results.push((
                        next + i64::try_from(offset)?,
                        serde_json::to_string(reference)?,
                        source.clone(),
                    ));
                } else {
                    // A legacy result without exact canonical replay cannot be
                    // authenticated as a diagnostic. Preserve its media.
                    unknown = true;
                }
            }
            next + i64::try_from(page.len())?
        }
        Err(error) => {
            if error.downcast_ref::<sea_orm::DbErr>().is_some() {
                return Err(error);
            }
            unknown = true;
            next
        }
    };
    if turns.len() > 64 {
        unknown = true;
        turns.clear();
    }
    if unknown {
        tracing::warn!("HTML preview retention preserved an unknown frozen media scope");
    }
    let io_locator = html_preview_io::HtmlPreviewIoLocator::new(&id);
    let change = store.connection.work_signal("maintenance.html").changing();
    let changed = retry_index_write(|| async {
        let tx = store.connection.begin().await?;
        let header = history::Entity::find_by_id(&id)
            .filter(history::Column::WorkspaceId.eq(&workspace))
            .filter(history::Column::OwnerThread.eq(&owner))
            .filter(history::Column::Ready.eq(1_i64))
            .filter(history::Column::Expired.eq(0_i64))
            .filter(history::Column::MessageCount.eq(count))
            .filter(
                Expr::col(history::Column::NextOrdinal)
                    .eq(Expr::col(history::Column::MessageCount)),
            )
            .filter(history::Column::IdentitySha256.eq(&identity))
            .one(&tx)
            .await?;
        if header.is_none() {
            tx.rollback().await?;
            return Ok(false);
        }
        let changed = frozen_index::Entity::update_many()
            .col_expr(frozen_index::Column::NextOrdinal, Expr::val(end))
            .col_expr(
                frozen_index::Column::Complete,
                Expr::val(i64::from(end == count && import_count == 0)),
            )
            .col_expr(frozen_index::Column::Unknown, Expr::val(i64::from(unknown)))
            .filter(frozen_index::Column::ManifestId.eq(&id))
            .filter(frozen_index::Column::NextOrdinal.eq(next))
            .filter(frozen_index::Column::Complete.eq(0))
            .filter(frozen_index::Column::Unknown.eq(0))
            .exec(&tx)
            .await?
            .rows_affected;
        if changed == 1 {
            for turn in &turns {
                frozen_turn::Entity::insert(frozen_turn::ActiveModel {
                    manifest_id: Set(id.clone()),
                    turn_id: Set(turn.clone()),
                })
                .on_conflict(
                    OnConflict::columns([
                        frozen_turn::Column::ManifestId,
                        frozen_turn::Column::TurnId,
                    ])
                    .do_nothing()
                    .to_owned(),
                )
                .exec_without_returning(&tx)
                .await?;
            }
            for (ordinal, reference, source) in &results {
                let turn = source
                    .scope
                    .strip_prefix("context:")
                    .expect("prepared context source");
                let inserted = frozen_result::Entity::insert(frozen_result::ActiveModel {
                    manifest_id: Set(id.clone()),
                    ordinal: Set(*ordinal),
                    reference_json: Set(reference.clone()),
                    turn_id: Set(turn.to_owned()),
                    source_id: Set(source.id.clone()),
                    source_version: Set(source.version.clone()),
                    archive_wire_sha256: Set(None),
                })
                .on_conflict(
                    OnConflict::columns([
                        frozen_result::Column::ManifestId,
                        frozen_result::Column::Ordinal,
                    ])
                    .do_nothing()
                    .to_owned(),
                )
                .exec_without_returning(&tx)
                .await?;
                if inserted == 1 {
                    html_preview_io::request_prepared(
                        &tx,
                        html_preview_io::HtmlPreviewIoFamily::Witness,
                        &io_locator,
                        *ordinal,
                        &workspace,
                    )
                    .await?;
                }
            }
        }
        tx.commit().await?;
        Ok(changed == 1)
    })
    .await?;
    change.completed(changed && !unknown && end == count && import_count == 0);
    if changed && !results.is_empty() {
        io_wake.notify_one();
        store.connection.work_signal("maintenance.html").notify();
    }
    if changed && !unknown && (end < count || import_count > 0) {
        *after = previous_after;
    }
    Ok(true)
}

// Existing progress owns one streaming digest, never a second graph/cache.
// A restart resumes message indexing and replays import verification from zero;
// only a complete matching digest publishes a safe marker.
// Ready identity/counts are protected by frozen_complete_identity. Expiry,
// source availability and the derived marker are still mutable. Publication
// rechecks the header and expected marker (ready=1, expired=0, next=messages,
// complete=unknown=0); this snapshot only owns bounded preparation.
struct PreviewImportHeader {
    manifest: String,
    workspace: String,
    owner: String,
    identity: String,
    messages: i64,
    count: u64,
    expected: String,
}
pub struct PreviewImportIndex {
    header: PreviewImportHeader,
    next: u64,
    digest: Sha256,
    page: VecDeque<crate::frozen_import::FrozenImportRecord>,
    pending: Option<(
        crate::frozen_import::FrozenImportRecord,
        crate::checkpoint_proofs::HistoricalMember,
    )>,
}
async fn index_import_quantum(
    store: &CompactionStore,
    progress: &mut Option<PreviewImportIndex>,
) -> Result<bool> {
    // HistoricalMember can mutate before a fallible await. Keeping this state
    // local also drops it on cancellation, not just on a returned DB error.
    let mut state = progress.take().expect("initialized import progress");
    let PreviewImportHeader {
        manifest: id,
        workspace,
        owner,
        identity,
        messages,
        count,
        expected,
    } = &state.header;
    let verified = async {
        // Reuse the checkpoint proof's bounded historical membership walker;
        // a checkpoint summary cannot authorize arbitrary original raw leaves.
        let proof = if let Some((_, member)) = state.pending.as_mut() {
            if !member.done() {
                member.step(store).await?;
                return Ok(());
            }
            ensure!(
                member.found(),
                "preview dependency import is outside checkpoint coverage"
            );
            state.pending.take().expect("completed member").0
        } else {
            if state.page.is_empty() {
                state.page = crate::frozen_import::compaction_frozen_import_page(
                    &store.connection,
                    workspace,
                    owner,
                    id,
                    state.next,
                )
                .await?
                .into();
                ensure!(
                    state.page.len() as u64 <= *count - state.next,
                    "preview dependency import count mismatch"
                );
            }
            let proof = state
                .page
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("preview dependency import page is missing"))?;
            let target = crate::checkpoint_proofs::target_reference(
                &store.connection,
                id,
                proof.message_ordinal,
            )
            .await?;
            let checkpoint = match target.sources.as_slice() {
                [s] if s.scope.starts_with("checkpoint:") => Some(s),
                _ => None,
            };
            ensure!(
                !target.inherited
                    && target.complete
                    && !target.protected_input
                    && target
                        .context_thread
                        .as_deref()
                        .unwrap_or(&target.source_thread)
                        == owner.as_str()
                    && (proof.matches_render_target(&target) || checkpoint.is_some()),
                "preview dependency import target is unknown"
            );
            // Imports convey authority for an existing message, never extra
            // media. Logical message/replay references carry its dependencies.
            crate::checkpoint_proofs::original_binding(store, workspace, &proof, true).await?;
            if let Some(checkpoint) = checkpoint {
                let member = crate::checkpoint_proofs::HistoricalMember::new(
                    store,
                    workspace,
                    &target.source_thread,
                    checkpoint,
                    &proof.source_thread,
                    proof.render_source(),
                )
                .await?;
                state.pending = Some((proof, member));
                return Ok(());
            }
            proof
        };
        let bytes = serde_json::to_vec(&proof)?;
        state.digest.update((bytes.len() as u64).to_le_bytes());
        state.digest.update(bytes);
        state.next += 1;
        ensure!(
            state.next <= *count,
            "preview dependency import count mismatch"
        );
        if state.next == *count {
            ensure!(
                hex::encode(state.digest.clone().finalize()) == *expected,
                "preview dependency import digest mismatch"
            );
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let unknown = match verified {
        Ok(()) => false,
        Err(error) if error.downcast_ref::<sea_orm::DbErr>().is_some() => {
            // A failed graph read may have advanced its local walker. Restart
            // the existing stream verification; never seal a partial closure.
            return Err(error);
        }
        Err(error) => {
            tracing::warn!(reason=%error,
                "HTML preview retention preserved an invalid import proof or opaque media dependency"
            );
            true
        }
    };
    let done = state.next == *count || unknown;
    if !done {
        *progress = Some(state);
        return Ok(false);
    }
    let header = history::Entity::find()
        .select_only()
        .column(history::Column::Id)
        .filter(history::Column::Id.eq(id))
        .filter(history::Column::WorkspaceId.eq(workspace))
        .filter(history::Column::OwnerThread.eq(owner))
        .filter(history::Column::Ready.eq(1_i64))
        .filter(history::Column::Expired.eq(0_i64))
        .filter(history::Column::IdentitySha256.eq(identity))
        .filter(history::Column::MessageCount.eq(*messages))
        .filter(
            Expr::col(history::Column::NextOrdinal).eq(Expr::col(history::Column::MessageCount)),
        )
        .filter(history::Column::ImportCount.eq(i64::try_from(*count)?))
        .filter(Expr::col(history::Column::NextImport).eq(Expr::col(history::Column::ImportCount)))
        .filter(history::Column::ImportsSha256.eq(expected))
        .into_query();
    let change = store.connection.work_signal("maintenance.html").changing();
    let changed = retry_index_write(|| async {
        let changed = frozen_index::Entity::update_many()
            .col_expr(
                frozen_index::Column::Complete,
                Expr::val(i64::from(done && !unknown)),
            )
            .col_expr(frozen_index::Column::Unknown, Expr::val(i64::from(unknown)))
            .filter(frozen_index::Column::ManifestId.eq(id))
            .filter(frozen_index::Column::Complete.eq(0))
            .filter(frozen_index::Column::Unknown.eq(0))
            .filter(frozen_index::Column::NextOrdinal.eq(*messages))
            .filter(frozen_index::Column::MessageCount.eq(*messages))
            .filter(frozen_index::Column::IdentitySha256.eq(identity))
            .filter(frozen_index::Column::WorkspaceId.eq(workspace))
            .filter(frozen_index::Column::Ready.eq(1_i64))
            .filter(frozen_index::Column::Expired.eq(0_i64))
            .filter(Expr::exists(header.clone()))
            .exec(&store.connection)
            .await?
            .rows_affected;
        Ok(changed == 1)
    })
    .await?;
    change.completed(changed && !unknown);
    // A zero-row guard rejection is terminal for this pass too. The marker
    // remains retaining and a later pass/restart can prepare fresh progress.
    Ok(true)
}

/// Existing ownership is authoritative. No age or terminal status alone is a
/// release proof. Other active workspace work is a conservative short hold:
/// its in-memory provider request need not yet have a durable frozen root.
pub async fn unowned<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    root_execution: &str,
    readers: &crate::frozen_lifetime::ReaderCounts,
    released: &pioneer_postgres_pools::WorkSignal,
) -> Result<bool> {
    let row = db
        .query_one(&queries::unowned(workspace, thread, turn, root_execution))
        .await?;
    if row.is_none() {
        return Ok(false);
    }
    // Check active reader holds before retiring the preview. The set of held
    // roots is bounded independently of permanent archive roots.
    let held_ids: Vec<String> = {
        let mut readers = readers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if readers.len() > 64 {
            // Falling below the bound necessarily releases one of these roots.
            // Register while holding the release lock; do not retain all roots.
            for held in readers.values_mut().take(65) {
                held.block_cleanup(released);
            }
            return Ok(false);
        }
        readers.keys().cloned().collect()
    };
    for manifest in held_ids {
        let held = Query::select()
            .expr(Expr::val(1))
            .from_as(frozen_turn::Entity, "p")
            .join_as(
                JoinType::InnerJoin,
                history::Entity,
                "h",
                col("h", history::Column::Id).eq(col("p", frozen_turn::Column::ManifestId)),
            )
            .and_where(col("p", frozen_turn::Column::ManifestId).eq(&manifest))
            .and_where(col("p", frozen_turn::Column::TurnId).eq(turn))
            .and_where(col("h", history::Column::WorkspaceId).eq(workspace))
            .and_where(col("h", history::Column::Expired).eq(0))
            .limit(1)
            .to_owned();
        if db.query_one(&held).await?.is_some()
            && crate::frozen_lifetime::cleanup_blocked(readers, &manifest, released)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
#[path = "preview_frozen_index_tests.rs"]
mod tests;
