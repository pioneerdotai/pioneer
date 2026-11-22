//! Bounded discovery and transactional retirement of temporary HTML media.
use crate::html_preview::archive_result;
use crate::html_preview_retention_queries::{
    self as queries, bytes, col, json as json_field, revision,
};
use crate::visibility::temporary_preview_predicate;
use anyhow::{Result, ensure};
use pioneer_entity::{
    artifact, artifact_binding, artifact_blob, artifact_external_ref, artifact_projection,
    artifact_version, compaction_source_revision, thread as thread_entity, turn as turn_entity,
    turn_input, turn_llm_context,
};
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, PostgresDatabase, WorkSignal,
    is_retryable_database_conflict, retry_with_backoff,
};
use sea_orm::sea_query::{BinOper, Expr, ExprTrait, JoinType, Order, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
    TransactionTrait,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, VecDeque},
    future::Future,
    pin::Pin,
};

/// The owner of frozen reader holds must recheck them with the durable retention
/// predicates both during preparation and with the retirement transaction.
pub trait PreviewRetirementGuard: Sync {
    fn unowned<'a, C: ConnectionTrait + 'a>(
        &'a self,
        db: &'a C,
        workspace: &'a str,
        thread: &'a str,
        turn: &'a str,
        release: &'a WorkSignal,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>>;
}

#[derive(Default)]
pub struct PreviewArtifactScan {
    after: String,
    pending: VecDeque<PreviewArtifactHeader>,
}

pub enum PreviewCandidate {
    Exhausted,
    Skipped,
    Ready(artifact::Model),
}

// Compact advisory discovery only. Metadata stays behind the individual byte
// guard and release retains its authoritative writer proof.
#[derive(sea_orm::FromQueryResult)]
struct PreviewArtifactHeader {
    id: String,
    current_version_id: Option<String>,
    bytes: i64,
}

impl PreviewArtifactScan {
    pub async fn quantum(
        &mut self,
        db: &PostgresDatabase,
        upper: &str,
    ) -> Result<PreviewCandidate> {
        if self.pending.is_empty() {
            self.pending = artifact::Entity::find()
                .select_only()
                .columns([artifact::Column::Id, artifact::Column::CurrentVersionId])
                .column_as(bytes(Expr::col(artifact::Column::MetadataJson)), "bytes")
                .filter(artifact::Column::Id.gt(self.after.as_str()))
                .filter(artifact::Column::Id.lte(upper))
                .filter(
                    Expr::col(artifact::Column::CreatedByKind).eq(Expr::Constant("tool".into())),
                )
                .filter(
                    Expr::col(artifact::Column::CreatedByActorId)
                        .eq(Expr::Constant("html_preview".into())),
                )
                .order_by_asc(artifact::Column::Id)
                .limit(16)
                .into_model::<PreviewArtifactHeader>()
                .all(db)
                .await?
                .into();
        }
        let Some(row) = self.pending.pop_front() else {
            self.after.clear();
            return Ok(PreviewCandidate::Exhausted);
        };
        self.after = row.id;
        if row.current_version_id.is_none() {
            return Ok(PreviewCandidate::Skipped);
        }
        if row.bytes > 1048576 {
            tracing::warn!("HTML preview retained oversized provenance metadata");
            return Ok(PreviewCandidate::Skipped);
        }
        let artifact = artifact::Entity::find_by_id(&self.after)
            .filter(bytes(Expr::col(artifact::Column::MetadataJson)).lte(1048576))
            .one(db)
            .await?;
        let Some(artifact) = artifact else {
            return Ok(PreviewCandidate::Skipped);
        };
        Ok(PreviewCandidate::Ready(artifact))
    }
}

pub async fn retire_preview(
    db: &PostgresDatabase,
    io_wake: &tokio::sync::Notify,
    guard: &impl PreviewRetirementGuard,
    candidate: artifact::Model,
) -> Result<()> {
    let released = db.work_signal("maintenance.html.release");
    let metadata: Value = serde_json::from_str(&candidate.metadata_json)?;
    let origin = &metadata["htmlTool"];
    ensure!(
        matches!(origin["version"].as_u64(), Some(1 | 2)) && origin["publication"] == false,
        "unknown preview provenance"
    );
    // Version one is the persisted single-theme, three-width layout. Version
    // two names each frame by appearance and width; formats cannot be mixed.
    let preview_format = origin["version"]
        .as_u64()
        .expect("validated preview version");
    let six_frames = preview_format == 2;
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
    if !guard
        .unowned(db, &candidate.workspace_id, thread, turn, &released)
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
                        &["htmlTool", "retention"],
                    )
                    .binary(
                        BinOper::Custom("IS NOT DISTINCT FROM"),
                        Expr::val("released"),
                    ),
                )
                .not(),
        )
        .filter(temporary_preview_predicate("artifact"))
        .filter(
            json_field(
                Expr::col(artifact::Column::MetadataJson),
                &["htmlTool", "turnId"],
            )
            .eq(turn),
        )
        .filter(
            json_field(
                Expr::col(artifact::Column::MetadataJson),
                &["htmlTool", "callId"],
            )
            .eq(call),
        )
        // Source plus six frames, with one sentinel to reject ambiguity.
        .order_by_asc(artifact::Column::Id)
        .limit(8)
        .all(db)
        .await?;
    ensure!(
        artifacts.len() <= if six_frames { 7 } else { 4 },
        "ambiguous preview artifact group"
    );
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
            meta["htmlTool"]["version"].as_u64() == Some(preview_format),
            "mixed preview capture formats"
        );
        let valid_index = if six_frames {
            matches!(
                index,
                "input"
                    | "light-360"
                    | "light-768"
                    | "light-1024"
                    | "dark-360"
                    | "dark-768"
                    | "dark-1024"
            )
        } else {
            matches!(index, "input" | "360" | "768" | "1024")
        };
        ensure!(
            valid_index && indices.insert(index.to_owned()),
            "invalid preview index"
        );
        let v = artifact_version::Entity::find()
            .filter(artifact_version::Column::ArtifactId.eq(&a.id))
            .limit(2)
            .all(db)
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
            .all(db)
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
    let context_ids = db.query_all(&query).await?;
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
            .one(db)
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
    let mut copy_obligations = BTreeSet::new();
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
        if !copies.is_empty() {
            copy_obligations.insert(a.id.clone());
        }
        m["htmlTool"]["releasedCopies"] = json!(copies);
        metadata_updates.push(serde_json::to_string(&m)?);
    }
    let copy_locators = copy_obligations
        .iter()
        .map(|id| {
            (
                id.clone(),
                crate::html_preview_io::HtmlPreviewIoLocator::new(id),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    // Retirement releases physical blob references, including when no readable
    // copy ticket exists. Reopen GC that already passed this source's key.
    let payload_change = pioneer_crud_signals::terminal_cleanup::Change::payload(db);
    let changes = ["maintenance.html"].map(|domain| db.work_signal(domain).changing());
    let changed = retry_with_backoff(
        || async {
            let tx = db.begin().await?;
            if !guard
                .unowned(&tx, &candidate.workspace_id, thread, turn, &released)
                .await?
            {
                tx.rollback().await?;
                return Ok(false);
            }
            // Recheck mutable adoption/version/identity assumptions inside commit.
            for ((a, v), b) in artifacts.iter().zip(&versions).zip(&bindings) {
                ensure!(
                    artifact::Entity::find_by_id(&a.id)
                        .lock_exclusive()
                        .one(&tx)
                        .await?
                        .as_ref()
                        == Some(a),
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
                // Clear the live references and publish the release marker in the
                // same transaction before removing the version.
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
                if copy_obligations.contains(&a.id) {
                    crate::html_preview_io::request_prepared(
                        &tx,
                        crate::html_preview_io::HtmlPreviewIoFamily::Copies,
                        copy_locators.get(&a.id).expect("prepared copy locator"),
                        0,
                        &a.workspace_id,
                    )
                    .await?;
                }
                artifact_version::Entity::delete_many()
                    .filter(artifact_version::Column::Id.eq(&v.id))
                    .filter(artifact_version::Column::ArtifactId.eq(&a.id))
                    .filter(artifact_version::Column::WorkspaceId.eq(&a.workspace_id))
                    .exec(&tx)
                    .await?;
            }
            tx.commit().await?;
            Ok(true)
        },
        |error| is_retryable_database_conflict(error) || crate::html_preview_io::is_conflict(error),
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        std::time::Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await?;
    payload_change
        .complete(db, changed, &[turn.to_owned()])
        .await;
    for change in changes {
        change.completed(changed);
    }
    if changed && !copy_obligations.is_empty() {
        io_wake.notify_one();
        db.work_signal("maintenance.html").notify();
    }
    Ok(())
}
