//! Temporary first-party HTML media. Frozen inputs keep their exact source;
//! only unowned canonical preview results may become compact diagnostics.
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_entity::{
    artifact, artifact_binding, artifact_blob, artifact_external_ref, artifact_projection,
    artifact_version, compaction_frozen_history as history, compaction_source_revision,
    html_preview_frozen_index as frozen_index, html_preview_frozen_result as frozen_result,
    html_preview_frozen_turn as frozen_turn, html_preview_index_backfill as backfill,
    thread as thread_entity, turn as turn_entity, turn_input, turn_llm_context,
};
use sea_orm::sea_query::{BinOper, Expr, ExprTrait, Func, JoinType, OnConflict, Order, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
    Set, TransactionTrait,
};
#[path = "html_preview_retention_queries.rs"]
pub(super) mod queries;
use queries::{bytes, col, json as json_field, revision};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, VecDeque};

// Used by every user list, before identity pagination. A separate real binding
// adopts the artifact; an ordinary screenshot from another tool never matches.
pub(super) fn temporary_preview_predicate(alias: &'static str) -> Expr {
    queries::temporary_preview(alias)
}

/// One immutable manifest page per call. Existing logical views include shared
/// backing ranges. Completed headers cannot change identity; expiry only drops
/// a hold. The short commit rechecks the header and progress before publication.
async fn index_quantum(
    store: &CrudStore,
    after: &mut String,
    imports: &mut Option<PreviewImportIndex>,
) -> Result<bool> {
    if let Some(state) = imports.as_ref() {
        let manifest = state.header.manifest.clone();
        if index_import_quantum(store, imports).await? {
            *after = manifest;
        }
        return Ok(true);
    }
    // Missing state never proves that legacy headers have markers. Pending
    // import continuation above must not rediscover work or reread this gate.
    let backfill_complete = backfill::Entity::find_by_id(1_i64)
        .one(&store.connection)
        .await?
        .is_some_and(|state| state.complete == 1);
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
            ("h", history::Column::Ready),
            ("h", history::Column::Expired),
        ])
        .expr_as(col("i", frozen_index::Column::ManifestId), "marker_id")
        .expr_as(
            Func::coalesce([col("i", frozen_index::Column::NextOrdinal), Expr::val(0)]),
            "next_ordinal",
        )
        .expr_as(
            Func::coalesce([col("i", frozen_index::Column::Complete), Expr::val(0)]),
            "complete",
        )
        .expr_as(
            Func::coalesce([col("i", frozen_index::Column::Unknown), Expr::val(0)]),
            "unknown",
        )
        .limit(1)
        .to_owned();
    if backfill_complete {
        // Keep literal predicate terms identical to html_preview_frozen_work.
        // Bound parameters or COALESCE would obscure partial-index eligibility.
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
            .order_by(("i", frozen_index::Column::ManifestId), Order::Asc);
    } else {
        query
            .from_as(history::Entity, "h")
            .join_as(
                JoinType::LeftJoin,
                frozen_index::Entity,
                "i",
                col("i", frozen_index::Column::ManifestId).eq(col("h", history::Column::Id)),
            )
            .and_where(col("h", history::Column::Id).gt(after.as_str()))
            .order_by(("h", history::Column::Id), Order::Asc);
    }
    let row = store.connection.query_one(&query).await?;
    let Some(row) = row else {
        // Every old header was visited. Header triggers cover every insertion,
        // update, key change and deletion concurrent with this keyset traversal.
        if !backfill_complete {
            store
                .run_serialized_write(|| async {
                    backfill::Entity::update_many()
                        .col_expr(backfill::Column::Complete, Expr::val(1))
                        .filter(backfill::Column::Singleton.eq(1))
                        .filter(backfill::Column::Complete.eq(0))
                        .exec(&store.connection)
                        .await?;
                    Ok(())
                })
                .await?;
        }
        after.clear();
        return Ok(false);
    };
    let discovered_id: String = row.try_get("", "id")?;
    if row.try_get::<Option<String>>("", "marker_id")?.is_none() {
        store
            .run_serialized_write(|| async {
                store
                    .connection
                    .execute(
                        &Query::insert()
                            .into_table(frozen_index::Entity)
                            .columns([
                                frozen_index::Column::ManifestId,
                                frozen_index::Column::WorkspaceId,
                                frozen_index::Column::IdentitySha256,
                                frozen_index::Column::MessageCount,
                                frozen_index::Column::Ready,
                                frozen_index::Column::Expired,
                            ])
                            .select_from(
                                history::Entity::find()
                                    .select_only()
                                    .columns([
                                        history::Column::Id,
                                        history::Column::WorkspaceId,
                                        history::Column::IdentitySha256,
                                        history::Column::MessageCount,
                                        history::Column::Ready,
                                        history::Column::Expired,
                                    ])
                                    .filter(history::Column::Id.eq(&discovered_id))
                                    .into_query(),
                            )?
                            .on_conflict(
                                OnConflict::column(frozen_index::Column::ManifestId)
                                    .do_nothing()
                                    .to_owned(),
                            )
                            .to_owned(),
                    )
                    .await?;
                Ok(())
            })
            .await?;
    }
    *after = row.try_get("", "id")?;
    // Older derived markers treated every import stream as opaque. Revisit
    // these metadata-only markers; they remain unsafe until fully verified.
    if row.try_get::<i64>("", "expired")? == 0
        && row.try_get::<i64>("", "ready")? == 1
        && row.try_get::<i64>("", "unknown")? == 1
        && row.try_get::<i64>("", "import_count")? > 0
    {
        store
            .run_serialized_write(|| async {
                frozen_index::Entity::update_many()
                    .col_expr(frozen_index::Column::NextOrdinal, Expr::val(0))
                    .col_expr(frozen_index::Column::Complete, Expr::val(0))
                    .col_expr(frozen_index::Column::Unknown, Expr::val(0))
                    .filter(frozen_index::Column::ManifestId.eq(&discovered_id))
                    .filter(frozen_index::Column::Unknown.eq(1))
                    .filter(frozen_index::Column::Expired.eq(0))
                    .exec(&store.connection)
                    .await?;
                Ok(())
            })
            .await?;
        *imports = None;
        *after = previous_after;
        return Ok(true);
    }
    if row.try_get::<i64>("", "expired")? == 1
        || row.try_get::<i64>("", "ready")? != 1
        || row.try_get::<i64>("", "complete")? == 1
        || row.try_get::<i64>("", "unknown")? == 1
    {
        return Ok(true);
    }
    let id: String = row.try_get("", "id")?;
    let workspace: String = row.try_get("", "workspace_id")?;
    let owner: String = row.try_get("", "owner_thread")?;
    let count: i64 = row.try_get("", "message_count")?;
    let identity: String = row.try_get("", "identity_sha256")?;
    let next: i64 = row.try_get("", "next_ordinal")?;
    if next == count && row.try_get::<i64>("", "import_count")? > 0 {
        *imports = Some(PreviewImportIndex {
            header: PreviewImportHeader {
                manifest: id.clone(),
                workspace,
                owner,
                identity,
                messages: count,
                count: u64::try_from(row.try_get::<i64>("", "import_count")?)?,
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
    let page = super::compaction_frozen::compaction_frozen_history_page(
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
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            let header = history::Entity::find_by_id(&id)
                .filter(history::Column::WorkspaceId.eq(&workspace))
                .filter(history::Column::OwnerThread.eq(&owner))
                .filter(history::Column::Ready.eq(1))
                .filter(history::Column::Expired.eq(0))
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
                return Ok(());
            }
            let changed = frozen_index::Entity::update_many()
                .col_expr(frozen_index::Column::NextOrdinal, Expr::val(end))
                .col_expr(
                    frozen_index::Column::Complete,
                    Expr::val(i64::from(
                        end == count && row.try_get::<i64>("", "import_count")? == 0,
                    )),
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
                    frozen_result::Entity::insert(frozen_result::ActiveModel {
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
                }
            }
            tx.commit().await?;
            Ok(())
        })
        .await?;
    if !unknown && (end < count || row.try_get::<i64>("", "import_count")? > 0) {
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
struct PreviewImportIndex {
    header: PreviewImportHeader,
    next: u64,
    digest: Sha256,
    page: VecDeque<super::compaction::FrozenImportRecord>,
    pending: Option<(
        super::compaction::FrozenImportRecord,
        super::checkpoint_proofs::HistoricalMember,
    )>,
}
async fn index_import_quantum(
    store: &CrudStore,
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
                state.page = super::compaction_frozen_import::compaction_frozen_import_page(
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
            let target =
                super::checkpoint_proofs::target_reference(store, id, proof.message_ordinal)
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
            super::checkpoint_proofs::original_binding(store, workspace, &proof, true).await?;
            if let Some(checkpoint) = checkpoint {
                let member = super::checkpoint_proofs::HistoricalMember::new(
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
        .filter(history::Column::Ready.eq(1))
        .filter(history::Column::Expired.eq(0))
        .filter(history::Column::IdentitySha256.eq(identity))
        .filter(history::Column::MessageCount.eq(*messages))
        .filter(
            Expr::col(history::Column::NextOrdinal).eq(Expr::col(history::Column::MessageCount)),
        )
        .filter(history::Column::ImportCount.eq(i64::try_from(*count)?))
        .filter(Expr::col(history::Column::NextImport).eq(Expr::col(history::Column::ImportCount)))
        .filter(history::Column::ImportsSha256.eq(expected))
        .into_query();
    store
        .run_serialized_write(|| async {
            frozen_index::Entity::update_many()
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
                .filter(frozen_index::Column::Ready.eq(1))
                .filter(frozen_index::Column::Expired.eq(0))
                .filter(Expr::exists(header.clone()))
                .exec(&store.connection)
                .await?;
            Ok(())
        })
        .await?;
    // A zero-row guard rejection is terminal for this pass too. The marker
    // remains retaining and a later pass/restart can prepare fresh progress.
    Ok(true)
}

/// Existing ownership is authoritative. No age or terminal status alone is a
/// release proof. Other active workspace work is a conservative short hold:
/// its in-memory provider request need not yet have a durable frozen root.
async fn unowned<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    root_execution: &str,
    readers: &crate::frozen_lifetime::ReaderCounts,
) -> Result<bool> {
    let row = db
        .query_one(&queries::unowned(workspace, thread, turn, root_execution))
        .await?;
    if row.is_none() {
        return Ok(false);
    }
    // Acquisition uses the same serialized writer as retirement. Inspect only
    // currently held roots, not the unbounded set of permanent archive roots.
    let held_ids: Vec<String> = readers
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .keys()
        .take(65)
        .cloned()
        .collect();
    if held_ids.len() > 64 {
        return Ok(false);
    }
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
            .and_where(col("p", frozen_turn::Column::ManifestId).eq(manifest))
            .and_where(col("p", frozen_turn::Column::TurnId).eq(turn))
            .and_where(col("h", history::Column::WorkspaceId).eq(workspace))
            .and_where(col("h", history::Column::Expired).eq(0))
            .limit(1)
            .to_owned();
        if db.query_one(&held).await?.is_some() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Compact only the exact canonical preview result. Pairing, name, call id,
/// diagnostics and ordered result remain; no source/image bytes are copied.
pub fn archive_result(payload: &str) -> Result<String> {
    let mut view: Value = serde_json::from_str(payload)?;
    ensure!(
        view["kind"] == "json" && view["truncated"] == false,
        "incomplete canonical preview result"
    );
    let message = &mut view["value"];
    ensure!(
        message["role"] == "tool"
            && message["name"] == "html_preview"
            && message["tool_call_id"]
                .as_str()
                .is_some_and(|s| !s.is_empty()),
        "preview result identity mismatch"
    );
    let mut diagnostics: Value = serde_json::from_str(
        message["content"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("preview result content is not JSON"))?,
    )?;
    let object = diagnostics
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("preview diagnostics are not an object"))?;
    object.remove("llm_context");
    object.insert(
        "previewRetention".into(),
        json!("released; temporary images are no longer retained"),
    );
    message["content"] = serde_json::to_string(&diagnostics)?.into();
    message.as_object_mut().unwrap().remove("content_parts");
    Ok(serde_json::to_string(&view)?)
}

#[derive(Default)]
pub struct HtmlPreviewRetentionProgress {
    after: String,
    index_after: String,
    imports: Option<PreviewImportIndex>,
    input_after: String,
    phase: usize,
    idle: [bool; 3],
}
impl HtmlPreviewRetentionProgress {
    /// Returns false at discovery EOF. Errors do not pin the cursor to poison
    /// data; the existing maintenance owner revisits every pass/restart.
    pub async fn quantum(&mut self, store: &CrudStore) -> Result<bool> {
        while self.idle[self.phase] {
            self.phase = (self.phase + 1) % 3;
        }
        let path = self.phase;
        self.phase = (self.phase + 1) % 3;
        let more = match path {
            0 => index_quantum(store, &mut self.index_after, &mut self.imports).await?,
            1 => input_adoption_quantum(store, &mut self.input_after).await?,
            _ => self.artifact_quantum(store).await?,
        };
        self.idle[path] = !more;
        if self.idle.iter().all(|idle| *idle) {
            self.idle = [false; 3];
            Ok(false)
        } else {
            Ok(true)
        }
    }
    async fn artifact_quantum(&mut self, store: &CrudStore) -> Result<bool> {
        let query = artifact::Entity::find()
            .select_only()
            .columns([artifact::Column::Id, artifact::Column::CurrentVersionId])
            .column_as(bytes(Expr::col(artifact::Column::MetadataJson)), "bytes")
            .filter(artifact::Column::Id.gt(self.after.as_str()))
            .filter(Expr::col(artifact::Column::CreatedByKind).eq(Expr::Constant("tool".into())))
            .filter(
                Expr::col(artifact::Column::CreatedByActorId)
                    .eq(Expr::Constant("html_preview".into())),
            )
            .order_by_asc(artifact::Column::Id)
            .limit(1)
            .into_query();
        let row = store.connection.query_one(&query).await?;
        let Some(row) = row else {
            self.after.clear();
            return Ok(false);
        };
        self.after = row.try_get("", "id")?;
        if row
            .try_get::<Option<String>>("", "current_version_id")?
            .is_none()
        {
            return Ok(true);
        }
        if row.try_get::<i64>("", "bytes")? > 1048576 {
            tracing::warn!("HTML preview retained oversized provenance metadata");
            return Ok(true);
        }
        let artifact = artifact::Entity::find_by_id(&self.after)
            .filter(bytes(Expr::col(artifact::Column::MetadataJson)).lte(1048576))
            .one(&store.connection)
            .await?;
        let Some(artifact) = artifact else {
            return Ok(true);
        };
        if let Err(error) = release(store, artifact).await {
            // Do not log payloads, file paths or database identifiers.
            tracing::warn!("HTML preview retention quantum preserved an uncertain candidate");
            return Err(error);
        }
        Ok(true)
    }
}

async fn release(store: &CrudStore, candidate: artifact::Model) -> Result<()> {
    let metadata: Value = serde_json::from_str(&candidate.metadata_json)?;
    let origin = &metadata["htmlTool"];
    ensure!(
        origin["version"] == 1 && origin["publication"] == false,
        "unknown preview provenance"
    );
    let turn = origin["turnId"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("missing preview turn"))?;
    let call = origin["callId"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("missing preview call"))?;
    let thread = candidate
        .primary_thread_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("missing preview thread"))?;
    let root_execution = crate::canonical_agent_id('E', &format!("root-agent-turn\0{turn}"));
    if !unowned(
        &store.connection,
        &candidate.workspace_id,
        thread,
        turn,
        &root_execution,
        &store.frozen_readers,
    )
    .await?
    {
        return Ok(());
    }
    let artifacts = artifact::Entity::find()
        .filter(artifact::Column::WorkspaceId.eq(&candidate.workspace_id))
        .filter(artifact::Column::PrimaryThreadId.eq(thread))
        // An adopted sibling can revisit this call after the other files retire.
        // Skip only completed retirements; unknown missing-version rows still
        // fail the provenance checks below and preserve their remaining owners.
        .filter(
            artifact::Column::CurrentVersionId
                .is_null()
                .and(artifact::Column::Status.eq("deleted"))
                .and(
                    json_field(
                        Expr::col(artifact::Column::MetadataJson),
                        "$.htmlTool.retention",
                    )
                    .binary(BinOper::Is, Expr::val("released")),
                )
                .not(),
        )
        .filter(temporary_preview_predicate("artifact"))
        .filter(
            json_field(
                Expr::col(artifact::Column::MetadataJson),
                "$.htmlTool.turnId",
            )
            .eq(turn),
        )
        .filter(
            json_field(
                Expr::col(artifact::Column::MetadataJson),
                "$.htmlTool.callId",
            )
            .eq(call),
        )
        .limit(5)
        .all(&store.connection)
        .await?;
    ensure!(artifacts.len() <= 4, "ambiguous preview artifact group");
    if artifacts.is_empty() {
        return Ok(());
    }
    let mut versions = Vec::new();
    let mut bindings = Vec::new();
    let mut indices = BTreeSet::new();
    for a in &artifacts {
        let meta: Value = serde_json::from_str(&a.metadata_json)?;
        let index = meta["htmlTool"]["index"].as_str().unwrap_or("");
        ensure!(
            matches!(index, "input" | "360" | "768" | "1024") && indices.insert(index.to_owned()),
            "invalid preview index"
        );
        let v = artifact_version::Entity::find()
            .filter(artifact_version::Column::ArtifactId.eq(&a.id))
            .limit(2)
            .all(&store.connection)
            .await?;
        ensure!(
            v.len() == 1
                && a.current_version_id.as_deref() == Some(v[0].id.as_str())
                && v[0].workspace_id == a.workspace_id
                && v[0].created_by_turn_id.as_deref() == Some(turn)
                && v[0].created_by_tool_call_id.as_deref() == Some(call),
            "preview version provenance mismatch"
        );
        let b = artifact_binding::Entity::find()
            .filter(artifact_binding::Column::ArtifactId.eq(&a.id))
            .limit(2)
            .all(&store.connection)
            .await?;
        ensure!(
            b.len() == 1 && b[0].artifact_version_id.as_deref() == Some(v[0].id.as_str()),
            "preview binding provenance mismatch"
        );
        ensure!(
            b[0].workspace_id == a.workspace_id
                && b[0].thread_id.as_deref() == Some(thread)
                && b[0].turn_id.as_deref() == Some(turn)
                && b[0].tool_call_id.as_deref() == Some(call)
                && b[0].binding_kind == "tool_output"
                && b[0].direction == "output"
                && b[0].role.as_deref() == Some("tool")
                && b[0].turn_item_id.is_none(),
            "unknown preview binding owner"
        );
        let version_metadata: Value = serde_json::from_str(&v[0].metadata_json)?;
        ensure!(
            version_metadata["htmlTool"] == meta["htmlTool"],
            "preview version metadata mismatch"
        );
        versions.push(v[0].clone());
        bindings.push(b[0].clone());
    }
    // Canonical call identity is also the existing durable tool item identity.
    // No v2 row is valid for a partial/failed capture; duplicate rows are unknown.
    let query = turn_llm_context::Entity::find()
        .select_only()
        .column(turn_llm_context::Column::Id)
        .column_as(bytes(Expr::col(turn_llm_context::Column::Payload)), "bytes")
        .filter(turn_llm_context::Column::TurnId.eq(turn))
        .filter(turn_llm_context::Column::ItemId.eq(call))
        .filter(turn_llm_context::Column::Source.eq("tool_result_v2"))
        .filter(turn_llm_context::Column::ToolName.eq("html_preview"))
        .limit(2)
        .into_query();
    let context_ids = store.connection.query_all(&query).await?;
    ensure!(
        context_ids.len() <= 1,
        "duplicate canonical preview results"
    );
    let mut contexts = Vec::new();
    for row in context_ids {
        let size: i64 = row.try_get("", "bytes")?;
        ensure!(
            (0..=524288).contains(&size),
            "unknown canonical preview result size"
        );
        let id: String = row.try_get("", "id")?;
        let context = turn_llm_context::Entity::find_by_id(id)
            .filter(bytes(Expr::col(turn_llm_context::Column::Payload)).lte(524288))
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("preview result changed during preparation"))?;
        contexts.push(context);
    }
    if contexts.is_empty() {
        tracing::warn!(
            "HTML preview has no canonical readable-copy provenance; unattributed legacy cache files are preserved"
        );
    }
    let archived = contexts
        .iter()
        .map(|c| archive_result(&c.payload))
        .collect::<Result<Vec<_>>>()?;
    let mut metadata_updates = Vec::new();
    for a in &artifacts {
        let mut m: Value = serde_json::from_str(&a.metadata_json)?;
        m["htmlTool"]["retention"] = json!("released");
        let mut copies = BTreeSet::new();
        for c in &contexts {
            let view: Value = serde_json::from_str(&c.payload)?;
            if let Some(parts) = view["value"]["content_parts"].as_array() {
                for part in parts {
                    let attachment = &part["image"];
                    if attachment["artifact"]["artifact_id"] == a.id {
                        ensure!(
                            attachment["artifact"]["artifact_version_id"].as_str()
                                == a.current_version_id.as_deref(),
                            "preview copy version mismatch"
                        );
                        if let Some(path) = attachment["source"]["path"].as_str() {
                            copies.insert(path.to_owned());
                        }
                    }
                }
            }
        }
        ensure!(
            copies.len() <= 3 && copies.iter().all(|p| p.len() <= 4096),
            "preview copy metadata bound"
        );
        m["htmlTool"]["releasedCopies"] = json!(copies);
        metadata_updates.push(serde_json::to_string(&m)?);
    }
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            if !unowned(
                &tx,
                &candidate.workspace_id,
                thread,
                turn,
                &root_execution,
                &store.frozen_readers,
            )
            .await?
            {
                tx.rollback().await?;
                return Ok(());
            }
            // Recheck mutable adoption/version/identity assumptions inside commit.
            for ((a, v), b) in artifacts.iter().zip(&versions).zip(&bindings) {
                ensure!(
                    artifact::Entity::find_by_id(&a.id).one(&tx).await?.as_ref() == Some(a),
                    "preview artifact changed"
                );
                ensure!(
                    artifact_version::Entity::find()
                        .filter(artifact_version::Column::ArtifactId.eq(&a.id))
                        .limit(2)
                        .all(&tx)
                        .await?
                        == vec![v.clone()],
                    "preview version changed"
                );
                ensure!(
                    artifact_binding::Entity::find()
                        .filter(artifact_binding::Column::ArtifactId.eq(&a.id))
                        .limit(2)
                        .all(&tx)
                        .await?
                        == vec![b.clone()],
                    "preview adopted concurrently"
                );
            }
            ensure!(
                turn_llm_context::Entity::find()
                    .filter(turn_llm_context::Column::TurnId.eq(turn))
                    .filter(turn_llm_context::Column::ItemId.eq(call))
                    .filter(turn_llm_context::Column::Source.eq("tool_result_v2"))
                    .filter(turn_llm_context::Column::ToolName.eq("html_preview"))
                    .limit(2)
                    .all(&tx)
                    .await?
                    == contexts,
                "preview context publication changed"
            );
            for (c, payload) in contexts.iter().zip(&archived) {
                ensure!(
                    turn_llm_context::Entity::find_by_id(&c.id)
                        .one(&tx)
                        .await?
                        .as_ref()
                        == Some(c),
                    "preview result changed"
                );
                turn_llm_context::Entity::update_many()
                    .col_expr(
                        turn_llm_context::Column::Payload,
                        Expr::val(payload.clone()),
                    )
                    .filter(turn_llm_context::Column::Id.eq(&c.id))
                    .exec(&tx)
                    .await?;
            }
            for ((a, v), metadata) in artifacts.iter().zip(&versions).zip(&metadata_updates) {
                // Attribute these exact original/projection dependencies before
                // dropping references, including legacy previews without staging metadata.
                let projections = artifact_projection::Entity::find()
                    .select_only()
                    .column(artifact_projection::Column::BlobId)
                    .filter(artifact_projection::Column::ArtifactId.eq(&a.id))
                    .filter(artifact_projection::Column::WorkspaceId.eq(&a.workspace_id))
                    .into_query();
                artifact_blob::Entity::update_many()
                    .col_expr(
                        artifact_blob::Column::MetadataJson,
                        queries::mark_temporary_blob(),
                    )
                    .filter(
                        artifact_blob::Column::Id
                            .eq(&v.blob_id)
                            .or(artifact_blob::Column::Id.in_subquery(projections)),
                    )
                    .exec(&tx)
                    .await?;
                // FK enforcement is disabled in production. Explicitly detach all
                // byte owners in this transaction; never rely on ON DELETE CASCADE.
                artifact_external_ref::Entity::delete_many()
                    .filter(artifact_external_ref::Column::ArtifactId.eq(&a.id))
                    .filter(artifact_external_ref::Column::WorkspaceId.eq(&a.workspace_id))
                    .exec(&tx)
                    .await?;
                artifact_projection::Entity::delete_many()
                    .filter(artifact_projection::Column::ArtifactId.eq(&a.id))
                    .filter(artifact_projection::Column::WorkspaceId.eq(&a.workspace_id))
                    .exec(&tx)
                    .await?;
                artifact_binding::Entity::update_many()
                    .col_expr(
                        artifact_binding::Column::ArtifactVersionId,
                        Expr::val(None::<String>),
                    )
                    .filter(artifact_binding::Column::ArtifactId.eq(&a.id))
                    .filter(artifact_binding::Column::WorkspaceId.eq(&a.workspace_id))
                    .exec(&tx)
                    .await?;
                artifact::Entity::update_many()
                    .col_expr(
                        artifact::Column::CurrentVersionId,
                        Expr::val(None::<String>),
                    )
                    .col_expr(artifact::Column::Status, Expr::val("deleted"))
                    .col_expr(artifact::Column::MetadataJson, Expr::val(metadata.clone()))
                    .filter(artifact::Column::Id.eq(&a.id))
                    .filter(artifact::Column::WorkspaceId.eq(&a.workspace_id))
                    .exec(&tx)
                    .await?;
                artifact_version::Entity::delete_many()
                    .filter(artifact_version::Column::Id.eq(&v.id))
                    .filter(artifact_version::Column::ArtifactId.eq(&a.id))
                    .filter(artifact_version::Column::WorkspaceId.eq(&a.workspace_id))
                    .exec(&tx)
                    .await?;
            }
            tx.commit().await?;
            Ok(())
        })
        .await
}

impl CrudStore {
    /// Rechecked under the external storage mutation guard by every production
    /// unlink path. Check all owners of the physical key, including projections.
    pub async fn artifact_storage_key_is_unreferenced(
        &self,
        workspace: &str,
        key: &str,
    ) -> Result<bool> {
        let owners = artifact_blob::Entity::find()
            .select_only()
            .column(artifact_blob::Column::Id)
            .filter(artifact_blob::Column::WorkspaceId.eq(workspace))
            .filter(artifact_blob::Column::StorageKey.eq(key))
            .filter(queries::blob_has_owners("artifact_blob"))
            .limit(1)
            .into_query();
        Ok(self.connection.query_one(&owners).await?.is_none())
    }

    /// Bounded keyset discovery. Advance after a failed filesystem deletion too;
    /// a fresh pass/restart revisits its still-durable orphan row.
    pub async fn html_preview_gc_blob_page(
        &self,
        after: &str,
        cutoff_ms: i64,
    ) -> Result<(
        String,
        Vec<(String, super::artifact::ArtifactGcBlobCandidateRecord)>,
    )> {
        // Page the source rows first, even if every blob still has owners.
        // This bounds work independently of the number of GC candidates.
        let cutoff = chrono::DateTime::from_timestamp_millis(cutoff_ms)
            .ok_or_else(|| anyhow::anyhow!("invalid artifact GC cutoff"))?
            .fixed_offset();
        let orphan = col("b", artifact_blob::Column::CreatedAt)
            .lte(cutoff)
            .and(Func::coalesce([
                json_field(
                    col("b", artifact_blob::Column::MetadataJson),
                    "$.htmlToolTemporary",
                )
                .eq(1),
                Expr::val(0),
            ]))
            .and(queries::blob_has_owners("b").not());
        let query = Query::select()
            .from_as(artifact_blob::Entity, "b")
            .columns([
                ("b", artifact_blob::Column::WorkspaceId),
                ("b", artifact_blob::Column::Id),
                ("b", artifact_blob::Column::StorageKey),
                ("b", artifact_blob::Column::SizeBytes),
            ])
            .expr_as(orphan, "orphan")
            .and_where(col("b", artifact_blob::Column::Id).gt(after))
            .order_by(("b", artifact_blob::Column::Id), Order::Asc)
            .limit(16)
            .to_owned();
        let rows = self.connection.query_all(&query).await?;
        let mut next = String::new();
        let mut candidates = Vec::new();
        for row in rows {
            next = row.try_get("", "id")?;
            if row.try_get::<i64>("", "orphan")? != 0 {
                candidates.push((
                    row.try_get("", "workspace_id")?,
                    super::artifact::ArtifactGcBlobCandidateRecord {
                        blob_id: next.clone(),
                        storage_key: row.try_get("", "storage_key")?,
                        size_bytes: std::cmp::max(row.try_get::<i64>("", "size_bytes")?, 0) as u64,
                    },
                ));
            }
        }
        Ok((next, candidates))
    }
    pub async fn html_preview_released_copies_page(
        &self,
        after: &str,
    ) -> Result<(String, Option<(String, String)>)> {
        let query = artifact::Entity::find()
            .select_only()
            .columns([
                artifact::Column::Id,
                artifact::Column::WorkspaceId,
                artifact::Column::CurrentVersionId,
            ])
            .column_as(bytes(Expr::col(artifact::Column::MetadataJson)), "bytes")
            .column_as(
                Expr::from(Func::cust(sea_orm::sea_query::Alias::new("substr")).args([
                    Expr::col(artifact::Column::MetadataJson),
                    Expr::val(1),
                    Expr::val(1048576),
                ])),
                "metadata_json",
            )
            .filter(artifact::Column::Id.gt(after))
            .filter(Expr::col(artifact::Column::CreatedByKind).eq(Expr::Constant("tool".into())))
            .filter(
                Expr::col(artifact::Column::CreatedByActorId)
                    .eq(Expr::Constant("html_preview".into())),
            )
            .order_by_asc(artifact::Column::Id)
            .limit(1)
            .into_query();
        let row = self.connection.query_one(&query).await?;
        let Some(row) = row else {
            return Ok((String::new(), None));
        };
        let id: String = row.try_get("", "id")?;
        if row
            .try_get::<Option<String>>("", "current_version_id")?
            .is_some()
        {
            return Ok((id, None));
        }
        if row.try_get::<i64>("", "bytes")? > 1048576 {
            tracing::warn!("HTML preview retained oversized copy metadata");
            return Ok((id, None));
        }
        let metadata: String = row.try_get("", "metadata_json")?;
        let parsed: Value = match serde_json::from_str(&metadata) {
            Ok(value) => value,
            Err(_) => return Ok((id, None)),
        };
        if parsed["htmlTool"]["retention"] != "released"
            || parsed["htmlTool"]["releasedCopies"]
                .as_array()
                .is_none_or(|paths| paths.is_empty())
        {
            return Ok((id, None));
        }
        Ok((id, Some((row.try_get("", "workspace_id")?, metadata))))
    }

    pub async fn finish_html_preview_copy_cleanup(
        &self,
        id: &str,
        previous: &str,
        metadata: &str,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            artifact::Entity::update_many()
                .col_expr(artifact::Column::MetadataJson, Expr::val(metadata))
                .filter(artifact::Column::Id.eq(id))
                .filter(artifact::Column::CurrentVersionId.is_null())
                .filter(artifact::Column::MetadataJson.eq(previous))
                .exec(&self.connection)
                .await?;
            Ok(())
        })
        .await
    }
}

/// Late durable delivery/retry must observe the same archived representation
/// after a release commit; it cannot republish deleted image dependencies.
pub(crate) async fn preview_result_was_released<C: ConnectionTrait>(
    db: &C,
    turn: &str,
    call: &str,
) -> Result<bool> {
    let query = artifact::Entity::find()
        .select_only()
        .column(artifact::Column::Id)
        .filter(Expr::col(artifact::Column::CreatedByKind).eq(Expr::Constant("tool".into())))
        .filter(
            Expr::col(artifact::Column::CreatedByActorId).eq(Expr::Constant("html_preview".into())),
        )
        .filter(artifact::Column::CurrentVersionId.is_null())
        .filter(
            json_field(
                Expr::col(artifact::Column::MetadataJson),
                "$.htmlTool.turnId",
            )
            .eq(turn),
        )
        .filter(
            json_field(
                Expr::col(artifact::Column::MetadataJson),
                "$.htmlTool.callId",
            )
            .eq(call),
        )
        .filter(
            json_field(
                Expr::col(artifact::Column::MetadataJson),
                "$.htmlTool.retention",
            )
            .eq("released"),
        )
        .limit(1)
        .into_query();
    Ok(db.query_one(&query).await?.is_some())
}

/// A supported canonical UserInput is a real adoption even when Message edits
/// do not create a UserMessage item. Prepare IDs outside the projection commit;
/// ordinary artifacts keep their existing binding behavior.
#[derive(Clone, Debug)]
pub(crate) struct PreparedPreviewInputAdoption {
    turn: String,
    artifact: String,
    version: Option<String>,
    binding: String,
    index: i64,
    at: chrono::DateTime<chrono::FixedOffset>,
}
impl PreparedPreviewInputAdoption {
    pub(crate) fn new(
        turn: &str,
        artifact: &str,
        version: Option<&str>,
        index: i64,
        at: chrono::DateTime<chrono::FixedOffset>,
    ) -> Self {
        Self {
            turn: turn.into(),
            artifact: artifact.into(),
            version: version.map(str::to_owned),
            binding: pioneer_protocol::generate_id(21),
            index,
            at,
        }
    }
}

pub(crate) async fn retain_preview_input<C: ConnectionTrait>(
    db: &C,
    input: &PreparedPreviewInputAdoption,
) -> Result<()> {
    let query = Query::select()
        .from_as(artifact::Entity, "a")
        .columns([
            ("a", artifact::Column::WorkspaceId),
            ("a", artifact::Column::CurrentVersionId),
        ])
        .column(("t", turn_entity::Column::ThreadId))
        .expr_as(
            col("th", thread_entity::Column::WorkspaceId),
            "target_workspace",
        )
        .join_as(
            JoinType::InnerJoin,
            turn_entity::Entity,
            "t",
            col("t", turn_entity::Column::Id).eq(&input.turn),
        )
        .join_as(
            JoinType::InnerJoin,
            thread_entity::Entity,
            "th",
            col("th", thread_entity::Column::Id).eq(col("t", turn_entity::Column::ThreadId)),
        )
        .and_where(col("a", artifact::Column::Id).eq(&input.artifact))
        .and_where(col("a", artifact::Column::CreatedByKind).eq("tool"))
        .and_where(col("a", artifact::Column::CreatedByActorId).eq("html_preview"))
        .limit(1)
        .to_owned();
    let row = db.query_one(&query).await?;
    let Some(row) = row else {
        return Ok(());
    };
    let workspace: String = row.try_get("", "workspace_id")?;
    ensure!(
        row.try_get::<String>("", "target_workspace")? == workspace,
        "preview input crosses workspace"
    );
    let thread: String = row.try_get("", "thread_id")?;
    let version = input
        .version
        .clone()
        .or(row.try_get::<Option<String>>("", "current_version_id")?)
        .ok_or_else(|| anyhow::anyhow!("released preview input is unavailable"))?;
    let available = artifact_version::Entity::find_by_id(&version)
        .select_only()
        .column(artifact_version::Column::Id)
        .filter(artifact_version::Column::ArtifactId.eq(&input.artifact))
        .filter(artifact_version::Column::WorkspaceId.eq(&workspace))
        .into_query();
    let available = db.query_one(&available).await?;
    ensure!(
        available.is_some(),
        "released preview input version is unavailable"
    );
    // An immutable real UserInput binding is the existing adoption path. The
    // current input projection remains authoritative for Message rendering.
    let existing = artifact_binding::Entity::find()
        .select_only()
        .column(artifact_binding::Column::Id)
        .filter(artifact_binding::Column::ArtifactId.eq(&input.artifact))
        .filter(artifact_binding::Column::ArtifactVersionId.eq(&version))
        .filter(artifact_binding::Column::WorkspaceId.eq(&workspace))
        .filter(artifact_binding::Column::ThreadId.eq(&thread))
        .filter(artifact_binding::Column::TurnId.eq(&input.turn))
        .filter(artifact_binding::Column::BindingKind.eq("user_input"))
        .filter(artifact_binding::Column::Direction.eq("input"))
        .into_query();
    let values = Query::select()
        .exprs([
            Expr::val(input.binding.clone()),
            Expr::val(input.artifact.clone()),
            Expr::val(version),
            Expr::val(workspace),
            Expr::val(thread),
            Expr::val(input.turn.clone()),
            Expr::val("user_input"),
            Expr::val("input"),
            Expr::val(input.index),
            Expr::val("user"),
            Expr::val(input.at),
            Expr::val("{}"),
        ])
        .and_where(Expr::exists(existing).not())
        .to_owned();
    db.execute(
        &Query::insert()
            .into_table(artifact_binding::Entity)
            .columns([
                artifact_binding::Column::Id,
                artifact_binding::Column::ArtifactId,
                artifact_binding::Column::ArtifactVersionId,
                artifact_binding::Column::WorkspaceId,
                artifact_binding::Column::ThreadId,
                artifact_binding::Column::TurnId,
                artifact_binding::Column::BindingKind,
                artifact_binding::Column::Direction,
                artifact_binding::Column::ItemIndex,
                artifact_binding::Column::Role,
                artifact_binding::Column::CreatedAt,
                artifact_binding::Column::MetadataJson,
            ])
            .select_from(values)?
            .to_owned(),
    )
    .await?;
    Ok(())
}

async fn input_adoption_quantum(store: &CrudStore, after: &mut String) -> Result<bool> {
    // Supported input projection writers retain previews in their atomic write
    // set, including inputs inserted behind this legacy cursor after completion.
    // Do not cache the gate: a legacy-data migration may need to reopen it.
    if backfill::Entity::find_by_id(1_i64)
        .one(&store.connection)
        .await?
        .is_some_and(|state| state.inputs_complete == 1)
    {
        after.clear();
        return Ok(false);
    }
    // Enumerate source IDs first: no request/commit scans all input payloads.
    let payload_bytes: Expr = Expr::case(
        Expr::col(turn_input::Column::InputType).eq("artifact"),
        bytes(Expr::col(turn_input::Column::Payload)),
    )
    .finally(0)
    .into();
    let query = turn_input::Entity::find()
        .select_only()
        .columns([
            turn_input::Column::Id,
            turn_input::Column::TurnId,
            turn_input::Column::InputType,
            turn_input::Column::InputIndex,
            turn_input::Column::CreatedAt,
        ])
        .column_as(payload_bytes, "bytes")
        .filter(turn_input::Column::Id.gt(after.as_str()))
        .order_by_asc(turn_input::Column::Id)
        .limit(1)
        .into_query();
    let row = store.connection.query_one(&query).await?;
    let Some(row) = row else {
        store
            .run_serialized_write(|| async {
                backfill::Entity::update_many()
                    .col_expr(backfill::Column::InputsComplete, Expr::val(1))
                    .filter(backfill::Column::Singleton.eq(1))
                    .filter(backfill::Column::InputsComplete.eq(0))
                    .exec(&store.connection)
                    .await?;
                Ok(())
            })
            .await?;
        after.clear();
        return Ok(false);
    };
    *after = row.try_get("", "id")?;
    if row.try_get::<String>("", "input_type")? != "artifact" {
        return Ok(true);
    }
    if row.try_get::<i64>("", "bytes")? > 8192 {
        tracing::warn!("HTML preview adoption backfill preserved oversized canonical input");
        return Ok(true);
    }
    let query = turn_input::Entity::find_by_id(after.as_str())
        .select_only()
        .column(turn_input::Column::Payload)
        .filter(bytes(Expr::col(turn_input::Column::Payload)).lte(8192))
        .into_query();
    let current = store.connection.query_one(&query).await?;
    let Some(current) = current else {
        return Ok(true);
    };
    let payload: String = current.try_get("", "payload")?;
    let pioneer_protocol::UserInput::Artifact {
        artifact_id,
        version_id,
    } = serde_json::from_str(&payload)?
    else {
        tracing::warn!("HTML preview adoption backfill preserved unknown canonical input");
        return Ok(true);
    };
    let turn: String = row.try_get("", "turn_id")?;
    let adoption = PreparedPreviewInputAdoption::new(
        &turn,
        &artifact_id,
        version_id.as_deref(),
        row.try_get("", "input_index")?,
        row.try_get("", "created_at")?,
    );
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            let current = turn_input::Entity::find_by_id(after.as_str())
                .filter(turn_input::Column::TurnId.eq(&turn))
                .filter(turn_input::Column::InputType.eq("artifact"))
                .filter(turn_input::Column::Payload.eq(&payload))
                .one(&tx)
                .await?;
            if current.is_some() {
                retain_preview_input(&tx, &adoption).await?;
            }
            tx.commit().await?;
            Ok(())
        })
        .await?;
    Ok(true)
}

/// A bounded, derived verification receipt, not a second history payload.
/// Gateway verifies the original frozen rendering before preparing its hash.
#[derive(Clone)]
pub struct HtmlPreviewArchiveWitness {
    pub manifest: String,
    pub ordinal: i64,
    pub workspace: String,
    pub owner_thread: String,
    pub reference: pioneer_compaction::frozen::FrozenMessageRef,
    pub payload: String,
}
impl CrudStore {
    pub async fn html_preview_archive_witness_page(
        &self,
        after: &(String, i64),
    ) -> Result<(Option<(String, i64)>, Option<HtmlPreviewArchiveWitness>)> {
        // Page source rows, even if expired/already verified; never search the
        // whole candidate pool inside one background quantum.
        let query = Query::select()
            .from_as(frozen_result::Entity, "r")
            .columns([
                ("r", frozen_result::Column::ManifestId),
                ("r", frozen_result::Column::Ordinal),
                ("r", frozen_result::Column::ReferenceJson),
                ("r", frozen_result::Column::SourceId),
                ("r", frozen_result::Column::SourceVersion),
                ("r", frozen_result::Column::TurnId),
                ("r", frozen_result::Column::ArchiveWireSha256),
            ])
            .columns([
                ("h", history::Column::WorkspaceId),
                ("h", history::Column::OwnerThread),
                ("h", history::Column::Expired),
            ])
            .join_as(
                JoinType::LeftJoin,
                history::Entity,
                "h",
                col("h", history::Column::Id).eq(col("r", frozen_result::Column::ManifestId)),
            )
            .and_where(
                Expr::tuple([
                    col("r", frozen_result::Column::ManifestId),
                    col("r", frozen_result::Column::Ordinal),
                ])
                .gt(Expr::tuple([
                    Expr::val(after.0.clone()),
                    Expr::val(after.1),
                ])),
            )
            .order_by(("r", frozen_result::Column::ManifestId), Order::Asc)
            .order_by(("r", frozen_result::Column::Ordinal), Order::Asc)
            .limit(1)
            .to_owned();
        let row = self.connection.query_one(&query).await?;
        let Some(r) = row else {
            return Ok((None, None));
        };
        let manifest: String = r.try_get("", "manifest_id")?;
        let ordinal: i64 = r.try_get("", "ordinal")?;
        let next = Some((manifest.clone(), ordinal));
        if r.try_get::<Option<i64>>("", "expired")? != Some(0)
            || r.try_get::<Option<String>>("", "archive_wire_sha256")?
                .is_some()
        {
            return Ok((next, None));
        }
        let reference = match serde_json::from_str(&r.try_get::<String>("", "reference_json")?) {
            Ok(reference) => reference,
            Err(_) => {
                tracing::warn!("HTML preview archive preserved an invalid reference");
                return Ok((next, None));
            }
        };
        let workspace: String = r.try_get("", "workspace_id")?;
        let query = queries::canonical_preview()
            .clear_selects()
            .column(("c", turn_llm_context::Column::Payload))
            .and_where(
                col("c", turn_llm_context::Column::Id).eq(r.try_get::<String>("", "source_id")?),
            )
            .and_where(
                col("c", turn_llm_context::Column::TurnId).eq(r.try_get::<String>("", "turn_id")?),
            )
            .and_where(
                revision(col("v", compaction_source_revision::Column::Revision))
                    .eq(r.try_get::<String>("", "source_version")?),
            )
            .and_where(col("th", thread_entity::Column::WorkspaceId).eq(&workspace))
            .and_where(bytes(col("c", turn_llm_context::Column::Payload)).lte(524288))
            .to_owned();
        let body = self.connection.query_one(&query).await?;
        if body.is_none() {
            tracing::warn!("HTML preview archive preserved an unavailable canonical revision");
        }
        let witness = body
            .map(|body| -> Result<HtmlPreviewArchiveWitness> {
                Ok(HtmlPreviewArchiveWitness {
                    manifest,
                    ordinal,
                    workspace,
                    owner_thread: r.try_get("", "owner_thread")?,
                    reference,
                    payload: body.try_get("", "payload")?,
                })
            })
            .transpose()?;
        Ok((next, witness))
    }

    pub async fn record_html_preview_archive_witness(
        &self,
        witness: &HtmlPreviewArchiveWitness,
        archive_hash: &str,
    ) -> Result<()> {
        ensure!(
            archive_hash.len() == 64 && archive_hash.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid archival hash"
        );
        let reference = serde_json::to_string(&witness.reference)?;
        let current = queries::canonical_preview()
            .join_as(
                JoinType::InnerJoin,
                history::Entity,
                "h",
                col("h", history::Column::Id).eq(Expr::col((
                    frozen_result::Entity,
                    frozen_result::Column::ManifestId,
                ))),
            )
            .and_where(col("c", turn_llm_context::Column::Id).eq(Expr::col((
                frozen_result::Entity,
                frozen_result::Column::SourceId,
            ))))
            .and_where(col("c", turn_llm_context::Column::TurnId).eq(Expr::col((
                frozen_result::Entity,
                frozen_result::Column::TurnId,
            ))))
            .and_where(col("h", history::Column::WorkspaceId).eq(&witness.workspace))
            .and_where(col("h", history::Column::OwnerThread).eq(&witness.owner_thread))
            .and_where(col("h", history::Column::Ready).eq(1))
            .and_where(col("h", history::Column::Expired).eq(0))
            .and_where(
                col("th", thread_entity::Column::WorkspaceId)
                    .eq(col("h", history::Column::WorkspaceId)),
            )
            .and_where(col("t", turn_entity::Column::ThreadId).eq(&witness.reference.source_thread))
            .and_where(col("c", turn_llm_context::Column::Payload).eq(&witness.payload))
            .and_where(
                revision(col("v", compaction_source_revision::Column::Revision)).eq(Expr::col((
                    frozen_result::Entity,
                    frozen_result::Column::SourceVersion,
                ))),
            )
            .to_owned();
        self.run_serialized_write(|| async {
            frozen_result::Entity::update_many()
                .col_expr(
                    frozen_result::Column::ArchiveWireSha256,
                    Expr::val(archive_hash),
                )
                .filter(frozen_result::Column::ManifestId.eq(&witness.manifest))
                .filter(frozen_result::Column::Ordinal.eq(witness.ordinal))
                .filter(frozen_result::Column::ReferenceJson.eq(&reference))
                .filter(frozen_result::Column::ArchiveWireSha256.is_null())
                .filter(Expr::exists(current.clone()))
                .exec(&self.connection)
                .await?;
            Ok(())
        })
        .await
    }

    /// Only an exact, previously authenticated frozen preview can use this
    /// projection. Retirement's single UPDATE advances the existing source
    /// trigger exactly once. Require that adjacent revision, a release marker
    /// and the verified diagnostic hash; a later edit invalidates the receipt.
    /// An already archived diagnostic can also have an indexing receipt, but
    /// its unchanged wire hash cannot authorize another revision transition.
    pub async fn html_preview_archived_frozen_result(
        &self,
        workspace: &str,
        reference: &pioneer_compaction::frozen::FrozenMessageRef,
    ) -> Result<Option<(String, String, pioneer_compaction::SourceRef)>> {
        if reference.tool_name.as_deref() != Some("html_preview") {
            return Ok(None);
        }
        let serialized = serde_json::to_string(reference)?;
        let Some(source) = reference
            .replay_source
            .as_ref()
            .or_else(|| reference.sources.first())
        else {
            return Ok(None);
        };
        let query = queries::canonical_preview()
            .clear_selects()
            .column(("c", turn_llm_context::Column::Payload))
            .column(("r", frozen_result::Column::ArchiveWireSha256))
            .expr_as(
                revision(col("v", compaction_source_revision::Column::Revision)),
                "retired_version",
            )
            .join_as(
                JoinType::InnerJoin,
                frozen_result::Entity,
                "r",
                col("r", frozen_result::Column::SourceId)
                    .eq(col("c", turn_llm_context::Column::Id))
                    .and(
                        col("r", frozen_result::Column::TurnId)
                            .eq(col("c", turn_llm_context::Column::TurnId)),
                    ),
            )
            .join_as(
                JoinType::InnerJoin,
                history::Entity,
                "h",
                col("h", history::Column::Id).eq(col("r", frozen_result::Column::ManifestId)),
            )
            .and_where(col("r", frozen_result::Column::SourceId).eq(&source.id))
            .and_where(col("r", frozen_result::Column::SourceVersion).eq(&source.version))
            .and_where(col("r", frozen_result::Column::ReferenceJson).eq(serialized))
            .and_where(col("h", history::Column::WorkspaceId).eq(workspace))
            .and_where(col("th", thread_entity::Column::WorkspaceId).eq(workspace))
            .and_where(col("t", turn_entity::Column::ThreadId).eq(&reference.source_thread))
            .and_where(col("r", frozen_result::Column::SourceVersion).eq(revision(
                col("v", compaction_source_revision::Column::Revision).sub(1),
            )))
            .and_where(
                col("r", frozen_result::Column::ArchiveWireSha256).ne(json_field(
                    col("r", frozen_result::Column::ReferenceJson),
                    "$.wire_sha256",
                )),
            )
            .and_where(Expr::exists(queries::released_marker(workspace)))
            .and_where(bytes(col("c", turn_llm_context::Column::Payload)).lte(524288))
            .limit(1)
            .to_owned();
        let row = self.connection.query_one(&query).await?;
        row.map(|r| {
            Ok((
                r.try_get("", "payload")?,
                r.try_get("", "archive_wire_sha256")?,
                pioneer_compaction::SourceRef {
                    scope: source.scope.clone(),
                    id: source.id.clone(),
                    version: r.try_get("", "retired_version")?,
                },
            ))
        })
        .transpose()
    }
}

#[cfg(test)]
#[path = "html_preview_retention_tests.rs"]
mod tests;
