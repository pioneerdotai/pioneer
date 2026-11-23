//! Persistent receipts for archived HTML previews and the storage callbacks they authorize.
use crate::html_preview_io::{HtmlPreviewIoClaim, HtmlPreviewIoFamily, HtmlPreviewIoSource};
use crate::html_preview_retention_queries as queries;
use anyhow::{Result, ensure};
use pioneer_entity::{
    artifact, compaction_frozen_history as history, compaction_source_revision,
    html_preview_frozen_result as frozen_result, thread as thread_entity, turn as turn_entity,
    turn_llm_context,
};
use queries::{bytes, col, json as json_field, revision};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, TransactionTrait,
};

pub struct HtmlPreviewReceipts<'a> {
    connection: &'a pioneer_postgres_pools::PostgresDatabase,
    io_wake: &'a tokio::sync::Notify,
}

impl<'a> HtmlPreviewReceipts<'a> {
    pub fn new(
        connection: &'a pioneer_postgres_pools::PostgresDatabase,
        io_wake: &'a tokio::sync::Notify,
    ) -> Self {
        Self {
            connection,
            io_wake,
        }
    }

    fn notify_html_preview_io_work(&self) {
        self.io_wake.notify_one();
        self.connection.work_signal("maintenance.html").notify();
    }

    async fn retry_write_on_conflict<T, F, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        pioneer_postgres_pools::retry_with_backoff(
            || operation(),
            |error| {
                pioneer_postgres_pools::is_retryable_database_conflict(error)
                    || crate::html_preview_io::is_conflict(error)
            },
            pioneer_postgres_pools::DEFAULT_LOCK_RETRY_ATTEMPTS,
            std::time::Duration::from_millis(
                pioneer_postgres_pools::DEFAULT_LOCK_RETRY_BASE_DELAY_MS,
            ),
        )
        .await
    }
}

impl HtmlPreviewReceipts<'_> {
    /// Rechecked under the external storage mutation guard by every production
    /// unlink path. Check all owners of the physical key, including projections.
    pub async fn artifact_storage_key_is_unreferenced(
        &self,
        workspace: &str,
        key: &str,
    ) -> Result<bool> {
        crate::html_preview_discovery::artifact_storage_key_is_unreferenced(
            self.connection,
            workspace,
            key,
        )
        .await
    }

    /// Bounded keyset discovery. Advance after a failed filesystem deletion too;
    /// a fresh pass/restart revisits its still-durable orphan row.
    pub async fn html_preview_gc_blob_page(
        &self,
        after: &str,
        cutoff_ms: i64,
    ) -> Result<(
        String,
        Vec<(String, crate::artifact::ArtifactGcBlobCandidateRecord)>,
    )> {
        crate::html_preview_discovery::html_preview_gc_blob_page(self.connection, after, cutoff_ms)
            .await
    }
    pub async fn html_preview_gc_blob_page_through(
        &self,
        after: &str,
        cutoff_ms: i64,
        upper: &str,
    ) -> Result<(
        String,
        Vec<(String, crate::artifact::ArtifactGcBlobCandidateRecord)>,
    )> {
        crate::html_preview_discovery::html_preview_gc_blob_page_through(
            self.connection,
            after,
            cutoff_ms,
            upper,
        )
        .await
    }
    #[cfg(any(test, feature = "test-support"))]
    pub async fn html_preview_released_copies_page(
        &self,
        after: &str,
    ) -> Result<(String, Option<(String, String)>)> {
        crate::html_preview_discovery::html_preview_released_copies_page(self.connection, after)
            .await
    }
    #[cfg(any(test, feature = "test-support"))]
    pub async fn html_preview_released_copies_page_through(
        &self,
        after: &str,
        upper: &str,
    ) -> Result<(String, Option<(String, String)>)> {
        crate::html_preview_discovery::html_preview_released_copies_page_through(
            self.connection,
            after,
            upper,
        )
        .await
    }

    pub async fn finish_html_preview_copy_cleanup(
        &self,
        id: &str,
        previous: &str,
        metadata: &str,
    ) -> Result<()> {
        self.finish_html_preview_copy_cleanup_commit(id, previous, metadata, None)
            .await
            .and_then(|changed| {
                ensure!(changed, "HTML IO source CAS refused");
                Ok(())
            })
    }
    pub async fn finish_html_preview_copy_cleanup_claimed(
        &self,
        claim: &HtmlPreviewIoClaim,
        previous: &str,
        metadata: &str,
    ) -> Result<bool> {
        let id = claim.source_id();
        ensure!(claim.row.family == "copies", "copy claim identity mismatch");
        self.finish_html_preview_copy_cleanup_commit(id, previous, metadata, Some(claim))
            .await
    }
    async fn finish_html_preview_copy_cleanup_commit(
        &self,
        id: &str,
        previous: &str,
        metadata: &str,
        claim: Option<&HtmlPreviewIoClaim>,
    ) -> Result<bool> {
        let io_locator = crate::html_preview_io::HtmlPreviewIoLocator::new(id);
        self.retry_write_on_conflict(|| async {
            let tx = self.connection.begin().await?;
            let callback = if claim.is_none() {
                crate::html_preview_io::callback_ticket(&tx, "copies", &io_locator, 0).await?
            } else {
                None
            };
            if let Some(claim) = claim {
                ensure!(
                    pioneer_entity::html_preview_io_work::Entity::find()
                        .filter(crate::html_preview_io::fence(&claim.row))
                        .one(&tx)
                        .await?
                        .is_some(),
                    "copy claim fence lost"
                );
            }
            // SQL text equality can be case-insensitive. Lock the current
            // source (FOR UPDATE on PostgreSQL/MySQL), then compare exact bytes
            // in Rust before publishing prepared cleanup and ACK. A locking
            // read returns the committed row after competing writers settle.
            let current = artifact::Entity::find_by_id(id)
                .select_only()
                .columns([
                    artifact::Column::WorkspaceId,
                    artifact::Column::CurrentVersionId,
                    artifact::Column::MetadataJson,
                ])
                .filter(queries::bytes(Expr::col(artifact::Column::MetadataJson)).lte(1048576))
                .lock_exclusive()
                .into_tuple::<(String, Option<String>, String)>()
                .one(&tx)
                .await?;
            let Some((workspace, current_version, current_metadata)) = current else {
                tx.rollback().await?;
                return Ok(false);
            };
            if current_metadata != previous
                || current_version.is_some()
                || claim.is_some_and(|claim| workspace != claim.row.workspace_id)
            {
                tx.rollback().await?;
                return Ok(false);
            }
            let mut update = artifact::Entity::update_many()
                .col_expr(artifact::Column::MetadataJson, Expr::val(metadata))
                .filter(artifact::Column::Id.eq(id))
                .filter(artifact::Column::CurrentVersionId.is_null())
                .filter(artifact::Column::MetadataJson.eq(previous));
            if let Some(claim) = claim {
                update = update.filter(artifact::Column::WorkspaceId.eq(&claim.row.workspace_id));
            }
            if update.exec(&tx).await?.rows_affected != 1 {
                tx.rollback().await?;
                return Ok(false);
            }
            if let Some(claim) = claim {
                ensure!(
                    crate::html_preview_io::ack(&tx, claim).await?,
                    "copy ACK fence lost"
                );
            }
            if let Some(row) = &callback {
                crate::html_preview_io::callback_ack(&tx, row).await?;
            }
            tx.commit().await?;
            Ok(true)
        })
        .await
    }
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
impl HtmlPreviewReceipts<'_> {
    #[cfg(any(test, feature = "test-support"))]
    pub async fn html_preview_archive_witness_page(
        &self,
        after: &(String, i64),
    ) -> Result<(Option<(String, i64)>, Option<HtmlPreviewArchiveWitness>)> {
        let upper = self.html_preview_witness_frontier().await?;
        self.html_preview_archive_witness_page_through(after, &upper)
            .await
    }
    #[cfg(any(test, feature = "test-support"))]
    pub async fn html_preview_archive_witness_page_through(
        &self,
        after: &(String, i64),
        upper: &(String, i64),
    ) -> Result<(Option<(String, i64)>, Option<HtmlPreviewArchiveWitness>)> {
        use sea_orm::sea_query::{Order, Query};

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
            .and_where(
                Expr::tuple([
                    col("r", frozen_result::Column::ManifestId),
                    col("r", frozen_result::Column::Ordinal),
                ])
                .lte(Expr::tuple([
                    Expr::val(upper.0.clone()),
                    Expr::val(upper.1),
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

    #[cfg(any(test, feature = "test-support"))]
    pub async fn record_html_preview_archive_witness(
        &self,
        witness: &HtmlPreviewArchiveWitness,
        archive_hash: &str,
    ) -> Result<()> {
        self.record_html_preview_archive_witness_commit(witness, archive_hash, None)
            .await
            .and_then(|changed| {
                ensure!(changed, "HTML IO source CAS refused");
                Ok(())
            })
    }
    pub async fn record_html_preview_archive_witness_claimed(
        &self,
        claim: &HtmlPreviewIoClaim,
        witness: &HtmlPreviewArchiveWitness,
        archive_hash: &str,
    ) -> Result<bool> {
        ensure!(
            claim.row.family == "witness"
                && claim.source_id() == witness.manifest
                && claim.row.ordinal == witness.ordinal
                && claim.row.workspace_id == witness.workspace,
            "witness claim identity mismatch"
        );
        self.record_html_preview_archive_witness_commit(witness, archive_hash, Some(claim))
            .await
    }
    async fn record_html_preview_archive_witness_commit(
        &self,
        witness: &HtmlPreviewArchiveWitness,
        archive_hash: &str,
        claim: Option<&HtmlPreviewIoClaim>,
    ) -> Result<bool> {
        ensure!(
            archive_hash.len() == 64 && archive_hash.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid archival hash"
        );
        let change = self.connection.work_signal("maintenance.html").changing();
        let reference = serde_json::to_string(&witness.reference)?;
        let io_locator = crate::html_preview_io::HtmlPreviewIoLocator::new(&witness.manifest);
        let Some((source_version, source_id, source_turn)) =
            crate::html_preview::find_frozen_result(witness.manifest.clone(), witness.ordinal)
                .select_only()
                .column(frozen_result::Column::SourceVersion)
                .column(frozen_result::Column::SourceId)
                .column(frozen_result::Column::TurnId)
                .into_tuple::<(String, String, String)>()
                .one(self.connection)
                .await?
        else {
            change.completed(false);
            return Ok(false);
        };
        let Some(source_revision) = source_version
            .strip_prefix("revision:")
            .and_then(|v| v.parse::<i64>().ok())
        else {
            change.completed(false);
            return Ok(false);
        };
        if source_version != format!("revision:{source_revision}") {
            change.completed(false);
            return Ok(false);
        }
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
            .and_where(col("v", compaction_source_revision::Column::Revision).eq(source_revision))
            .to_owned();
        let result = self
            .retry_write_on_conflict(|| async {
                let tx = self.connection.begin().await?;
                let callback = if claim.is_none() {
                    crate::html_preview_io::callback_ticket(
                        &tx,
                        "witness",
                        &io_locator,
                        witness.ordinal,
                    )
                    .await?
                } else {
                    None
                };
                if let Some(claim) = claim {
                    ensure!(
                        pioneer_entity::html_preview_io_work::Entity::find()
                            .filter(crate::html_preview_io::fence(&claim.row))
                            .one(&tx)
                            .await?
                            .is_some(),
                        "witness claim fence lost"
                    );
                }
                let changed = frozen_result::Entity::update_many()
                    .col_expr(
                        frozen_result::Column::ArchiveWireSha256,
                        Expr::val(archive_hash),
                    )
                    .filter(frozen_result::Column::ManifestId.eq(&witness.manifest))
                    .filter(frozen_result::Column::Ordinal.eq(witness.ordinal))
                    .filter(frozen_result::Column::ReferenceJson.eq(&reference))
                    .filter(frozen_result::Column::SourceVersion.eq(&source_version))
                    .filter(frozen_result::Column::SourceId.eq(&source_id))
                    .filter(frozen_result::Column::TurnId.eq(&source_turn))
                    .filter(frozen_result::Column::ArchiveWireSha256.is_null())
                    .filter(Expr::exists(current.clone()))
                    .exec(&tx)
                    .await?
                    .rows_affected;
                // Zero affected rows alone never establish successful publication.
                // A concurrent publisher may already have stored this exact receipt;
                // reread under current domain authority before acknowledging it.
                if changed != 1 {
                    let stored = crate::html_preview::find_frozen_result(
                        witness.manifest.clone(),
                        witness.ordinal,
                    )
                    .select_only()
                    .column(frozen_result::Column::ArchiveWireSha256)
                    .filter(frozen_result::Column::ReferenceJson.eq(&reference))
                    .filter(frozen_result::Column::SourceVersion.eq(&source_version))
                    .filter(frozen_result::Column::SourceId.eq(&source_id))
                    .filter(frozen_result::Column::TurnId.eq(&source_turn))
                    .filter(Expr::exists(current.clone()))
                    .lock_exclusive()
                    .into_tuple::<Option<String>>()
                    .one(&tx)
                    .await?;
                    if !stored.is_some_and(|value| value.as_deref() == Some(archive_hash)) {
                        tx.rollback().await?;
                        return Ok((false, false));
                    }
                }
                if let Some(claim) = claim {
                    ensure!(
                        crate::html_preview_io::ack(&tx, claim).await?,
                        "witness ACK fence lost"
                    );
                }
                if let Some(row) = &callback {
                    crate::html_preview_io::callback_ack(&tx, row).await?;
                }
                tx.commit().await?;
                Ok((true, changed == 1))
            })
            .await;
        // A new witness can unblock previews already passed by retirement.
        // Repeated verification/ACK alone must not invalidate its own pass.
        if let Ok((_, published)) = &result {
            change.completed(*published);
        }
        result.map(|(accepted, _)| accepted)
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
        html_preview_archived_frozen_result(self.connection, workspace, reference).await
    }
}

impl HtmlPreviewReceipts<'_> {
    pub async fn load_html_preview_io_witness(
        &self,
        claim: &HtmlPreviewIoClaim,
    ) -> Result<HtmlPreviewIoSource<HtmlPreviewArchiveWitness>> {
        use HtmlPreviewIoSource as Source;
        ensure!(claim.row.family == "witness", "invalid witness family");
        if !claim.has_valid_point_locator() {
            return Ok(Source::Quarantine);
        }
        let reference_bytes = crate::html_preview::find_frozen_result(
            claim.source_id().to_owned(),
            claim.row.ordinal,
        )
        .select_only()
        .column_as(
            queries::bytes(Expr::col(frozen_result::Column::ReferenceJson)),
            "bytes",
        )
        .into_tuple::<i64>()
        .one(self.connection)
        .await?;
        if reference_bytes.is_some_and(|n| n > 524288) {
            return Ok(Source::Quarantine);
        }
        let Some(row) = crate::html_preview::find_frozen_result(
            claim.source_id().to_owned(),
            claim.row.ordinal,
        )
        .filter(queries::bytes(Expr::col(frozen_result::Column::ReferenceJson)).lte(524288))
        .one(self.connection)
        .await?
        else {
            return Ok(if reference_bytes.is_none() {
                Source::Obsolete
            } else {
                Source::Retry
            });
        };
        let Some(header) = history::Entity::find_by_id(&row.manifest_id)
            .one(self.connection)
            .await?
        else {
            return Ok(Source::Retry);
        };
        if header.workspace_id != claim.row.workspace_id {
            if claim.row.workspace_id.is_empty() {
                // Historical orphan was deliberately timed, not permanently
                // parked. Restoration can now bind its previously unknown owner.
                let locator = crate::html_preview_io::HtmlPreviewIoLocator::new(claim.source_id());
                self.retry_write_on_conflict(|| async {
                    let tx = self.connection.begin().await?;
                    ensure!(
                        history::Entity::find_by_id(claim.source_id())
                            .select_only()
                            .column(history::Column::WorkspaceId)
                            .filter(history::Column::WorkspaceId.eq(&header.workspace_id))
                            .lock_exclusive()
                            .into_tuple::<String>()
                            .one(&tx)
                            .await?
                            .is_some(),
                        "restored witness owner changed"
                    );
                    crate::html_preview_io::request_prepared(
                        &tx,
                        HtmlPreviewIoFamily::Witness,
                        &locator,
                        claim.row.ordinal,
                        &header.workspace_id,
                    )
                    .await?;
                    tx.commit().await?;
                    Ok(())
                })
                .await?;
                self.notify_html_preview_io_work();
                return Ok(Source::Retry);
            }
            return Ok(Source::Quarantine);
        }
        if header.expired != 0 || row.archive_wire_sha256.is_some() {
            return Ok(Source::Obsolete);
        }
        let reference: pioneer_compaction::frozen::FrozenMessageRef =
            match serde_json::from_str(&row.reference_json) {
                Ok(reference) => reference,
                Err(_) => return Ok(Source::Quarantine),
            };
        if reference.validate().is_err() {
            return Ok(Source::Quarantine);
        }
        if header.ready != 1 {
            return Ok(Source::Retry);
        }
        let Some(expected_revision) = row
            .source_version
            .strip_prefix("revision:")
            .and_then(|v| v.parse::<i64>().ok())
        else {
            return Ok(Source::Quarantine);
        };
        if row.source_version != format!("revision:{expected_revision}") {
            return Ok(Source::Quarantine);
        }
        if let Some(revision) = compaction_source_revision::Entity::find_by_id(&row.source_id)
            .one(self.connection)
            .await?
        {
            if revision.revision != expected_revision || revision.turn_id != row.turn_id {
                return Ok(Source::Quarantine);
            }
        }

        let canonical = queries::canonical_preview()
            .and_where(col("c", turn_llm_context::Column::Id).eq(&row.source_id))
            .and_where(col("c", turn_llm_context::Column::TurnId).eq(&row.turn_id))
            .and_where(col("v", compaction_source_revision::Column::Revision).eq(expected_revision))
            .and_where(col("th", thread_entity::Column::WorkspaceId).eq(&header.workspace_id))
            .to_owned();
        let payload = turn_llm_context::Entity::find_by_id(&row.source_id)
            .select_only()
            .column(turn_llm_context::Column::Payload)
            .filter(Expr::exists(canonical))
            .filter(queries::bytes(Expr::col(turn_llm_context::Column::Payload)).lte(524288))
            .into_tuple::<String>()
            .one(self.connection)
            .await?;
        if let Some(payload) = payload {
            return Ok(Source::Ready(HtmlPreviewArchiveWitness {
                manifest: row.manifest_id.clone(),
                ordinal: row.ordinal,
                workspace: header.workspace_id.clone(),
                owner_thread: header.owner_thread.clone(),
                reference,
                payload,
            }));
        }
        // Absence in the canonical join is not evidence of invalid authority.
        // Restore can publish revision, body, turn and thread in separate steps.
        let Some((turn_id, kind, tool)) = turn_llm_context::Entity::find_by_id(&row.source_id)
            .select_only()
            .columns([
                turn_llm_context::Column::TurnId,
                turn_llm_context::Column::Source,
                turn_llm_context::Column::ToolName,
            ])
            .into_tuple::<(String, String, Option<String>)>()
            .one(self.connection)
            .await?
        else {
            return Ok(Source::Retry);
        };
        if turn_id != row.turn_id
            || kind != "tool_result_v2"
            || tool.as_deref() != Some("html_preview")
        {
            return Ok(Source::Quarantine);
        }
        let Some(thread_id) = turn_entity::Entity::find_by_id(&turn_id)
            .select_only()
            .column(turn_entity::Column::ThreadId)
            .into_tuple::<String>()
            .one(self.connection)
            .await?
        else {
            return Ok(Source::Retry);
        };
        let Some(workspace) = thread_entity::Entity::find_by_id(&thread_id)
            .select_only()
            .column(thread_entity::Column::WorkspaceId)
            .into_tuple::<String>()
            .one(self.connection)
            .await?
        else {
            return Ok(Source::Retry);
        };
        if workspace != header.workspace_id || thread_id != reference.source_thread {
            return Ok(Source::Quarantine);
        }
        let length = turn_llm_context::Entity::find_by_id(&row.source_id)
            .select_only()
            .column_as(
                queries::bytes(Expr::col(turn_llm_context::Column::Payload)),
                "bytes",
            )
            .into_tuple::<i64>()
            .one(self.connection)
            .await?;
        Ok(if length.is_some_and(|n| n > 524288) {
            Source::Quarantine
        } else {
            Source::Retry
        })
    }

    pub async fn load_html_preview_io_copies(
        &self,
        claim: &HtmlPreviewIoClaim,
    ) -> Result<HtmlPreviewIoSource<(String, String)>> {
        crate::html_preview_discovery::load_html_preview_io_copies(self.connection, claim).await
    }
}

impl HtmlPreviewReceipts<'_> {
    /// Future GC eligibility for currently unowned temporary blobs. Source
    /// changes can invalidate ownership and must wake maintenance separately.
    pub async fn html_preview_gc_deadline(
        &self,
        grace_ms: i64,
        pass_cutoff_ms: i64,
    ) -> Result<Option<i64>> {
        crate::html_preview_discovery::html_preview_gc_deadline(
            self.connection,
            grace_ms,
            pass_cutoff_ms,
        )
        .await
    }
}

impl HtmlPreviewReceipts<'_> {
    /// Capture once per keyset pass. Later source commits remain invalidations
    /// for the following pass, including changes behind its current cursor.
    pub async fn html_preview_artifact_frontier(&self) -> Result<String> {
        crate::html_preview_discovery::html_preview_artifact_frontier(self.connection).await
    }
    pub async fn html_preview_blob_frontier(&self) -> Result<String> {
        crate::html_preview_discovery::html_preview_blob_frontier(self.connection).await
    }
    #[cfg(any(test, feature = "test-support"))]
    pub async fn html_preview_witness_frontier(&self) -> Result<(String, i64)> {
        crate::html_preview_discovery::html_preview_witness_frontier(self.connection).await
    }
}

#[cfg(test)]
#[path = "html_preview_receipts_tests.rs"]
mod tests;

pub async fn html_preview_archived_frozen_result(
    db: &pioneer_postgres_pools::PostgresDatabase,
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
                &["wire_sha256"],
            )),
        )
        .and_where(Expr::exists(queries::released_marker(workspace)))
        .and_where(bytes(col("c", turn_llm_context::Column::Payload)).lte(524288))
        .limit(1)
        .to_owned();
    let row = db.query_one(&query).await?;
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
