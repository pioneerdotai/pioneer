//! Bounded discovery of preview artifacts and unowned blobs.
use anyhow::{Result, ensure};
use pioneer_entity::{artifact, artifact_blob, html_preview_frozen_result as frozen_result};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::sea_query::{Expr, ExprTrait, Func, Order, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
};
use serde_json::Value;

use crate::artifact::ArtifactGcBlobCandidateRecord;
use crate::html_preview_io::{HtmlPreviewIoClaim, HtmlPreviewIoSource};
use crate::html_preview_retention_queries as queries;
use queries::{bytes, col, json as json_field};

/// Used under the external storage mutation guard before unlinking a key.
pub async fn artifact_storage_key_is_unreferenced(
    db: &SqliteDatabase,
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
    Ok(db.query_one(&owners).await?.is_none())
}

pub async fn html_preview_gc_blob_page(
    db: &SqliteDatabase,
    after: &str,
    cutoff_ms: i64,
) -> Result<(String, Vec<(String, ArtifactGcBlobCandidateRecord)>)> {
    let upper = html_preview_blob_frontier(db).await?;
    html_preview_gc_blob_page_through(db, after, cutoff_ms, &upper).await
}

pub async fn html_preview_gc_blob_page_through(
    db: &SqliteDatabase,
    after: &str,
    cutoff_ms: i64,
    upper: &str,
) -> Result<(String, Vec<(String, ArtifactGcBlobCandidateRecord)>)> {
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
                &["htmlToolTemporary"],
            )
            .is_in(["1", "true"]),
            Expr::val(false),
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
        .and_where(col("b", artifact_blob::Column::Id).lte(upper))
        .order_by(("b", artifact_blob::Column::Id), Order::Asc)
        .limit(16)
        .to_owned();
    let rows = db.query_all(&query).await?;
    let mut next = String::new();
    let mut candidates = Vec::new();
    for row in rows {
        next = row.try_get("", "id")?;
        if row.try_get::<bool>("", "orphan")? {
            candidates.push((
                row.try_get("", "workspace_id")?,
                ArtifactGcBlobCandidateRecord {
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
    db: &SqliteDatabase,
    after: &str,
) -> Result<(String, Option<(String, String)>)> {
    let upper = html_preview_artifact_frontier(db).await?;
    html_preview_released_copies_page_through(db, after, &upper).await
}

pub async fn html_preview_released_copies_page_through(
    db: &SqliteDatabase,
    after: &str,
    upper: &str,
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
        .filter(artifact::Column::Id.lte(upper))
        .filter(Expr::col(artifact::Column::CreatedByKind).eq(Expr::Constant("tool".into())))
        .filter(
            Expr::col(artifact::Column::CreatedByActorId).eq(Expr::Constant("html_preview".into())),
        )
        .order_by_asc(artifact::Column::Id)
        .limit(1)
        .into_query();
    let row = db.query_one(&query).await?;
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

pub async fn load_html_preview_io_copies(
    db: &SqliteDatabase,
    claim: &HtmlPreviewIoClaim,
) -> Result<HtmlPreviewIoSource<(String, String)>> {
    use HtmlPreviewIoSource as Source;
    ensure!(claim.row.family == "copies", "invalid copies family");
    if !claim.has_valid_point_locator() {
        return Ok(Source::Quarantine);
    }
    // Header and length are compact; oversized metadata is never materialized.
    let header = artifact::Entity::find_by_id(claim.source_id())
        .select_only()
        .column(artifact::Column::WorkspaceId)
        .column(artifact::Column::CurrentVersionId)
        .column_as(
            queries::bytes(Expr::col(artifact::Column::MetadataJson)),
            "bytes",
        )
        .into_tuple::<(String, Option<String>, i64)>()
        .one(db)
        .await?;
    let Some((workspace, current, length)) = header else {
        return Ok(Source::Obsolete);
    };
    if workspace != claim.row.workspace_id {
        return Ok(Source::Quarantine);
    }
    if current.is_some() {
        return Ok(Source::Obsolete);
    }
    if length > 1048576 {
        return Ok(Source::Quarantine);
    }
    let metadata = artifact::Entity::find_by_id(claim.source_id())
        .select_only()
        .column(artifact::Column::MetadataJson)
        .filter(artifact::Column::WorkspaceId.eq(&workspace))
        .filter(artifact::Column::CurrentVersionId.is_null())
        .filter(artifact::Column::CreatedByKind.eq("tool"))
        .filter(artifact::Column::CreatedByActorId.eq("html_preview"))
        .filter(queries::bytes(Expr::col(artifact::Column::MetadataJson)).lte(1048576))
        .into_tuple::<String>()
        .one(db)
        .await?;
    let Some(metadata) = metadata else {
        return Ok(Source::Retry);
    };
    let parsed: Value = match serde_json::from_str(&metadata) {
        Ok(value) => value,
        Err(_) => return Ok(Source::Quarantine),
    };
    if parsed["htmlTool"]["retention"] != "released"
        || parsed["htmlTool"]["releasedCopies"]
            .as_array()
            .is_none_or(|a| a.is_empty())
    {
        return Ok(Source::Obsolete);
    }
    Ok(Source::Ready((workspace, metadata)))
}

pub async fn html_preview_gc_deadline(
    db: &SqliteDatabase,
    grace_ms: i64,
    pass_cutoff_ms: i64,
) -> Result<Option<i64>> {
    let cutoff = chrono::DateTime::from_timestamp_millis(pass_cutoff_ms)
        .ok_or_else(|| anyhow::anyhow!("invalid artifact GC cutoff"))?
        .fixed_offset();
    let created = artifact_blob::Entity::find()
        .select_only()
        .column(artifact_blob::Column::CreatedAt)
        .filter(artifact_blob::Column::CreatedAt.gt(cutoff))
        .filter(
            json_field(
                Expr::col(artifact_blob::Column::MetadataJson),
                &["htmlToolTemporary"],
            )
            .is_in(["1", "true"]),
        )
        .filter(queries::blob_has_owners("artifact_blob").not())
        .order_by_asc(artifact_blob::Column::CreatedAt)
        .into_tuple::<chrono::DateTime<chrono::FixedOffset>>()
        .one(db)
        .await?;
    Ok(created.map(|at| {
        let ms = at.timestamp_millis().saturating_add(grace_ms);
        ms.div_euclid(1000) + i64::from(ms.rem_euclid(1000) != 0)
    }))
}

pub async fn html_preview_artifact_frontier(db: &SqliteDatabase) -> Result<String> {
    Ok(artifact::Entity::find()
        .select_only()
        .column(artifact::Column::Id)
        .order_by_desc(artifact::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?
        .unwrap_or_default())
}

pub async fn html_preview_blob_frontier(db: &SqliteDatabase) -> Result<String> {
    Ok(artifact_blob::Entity::find()
        .select_only()
        .column(artifact_blob::Column::Id)
        .order_by_desc(artifact_blob::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?
        .unwrap_or_default())
}

pub async fn html_preview_witness_frontier(db: &SqliteDatabase) -> Result<(String, i64)> {
    Ok(frozen_result::Entity::find()
        .select_only()
        .columns([
            frozen_result::Column::ManifestId,
            frozen_result::Column::Ordinal,
        ])
        .order_by_desc(frozen_result::Column::ManifestId)
        .order_by_desc(frozen_result::Column::Ordinal)
        .into_tuple::<(String, i64)>()
        .one(db)
        .await?
        .unwrap_or_default())
}
