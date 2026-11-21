//! Authorized checkpoint projection reads with exact PostgreSQL JSON decoding.
use anyhow::Result;
use pioneer_entity::compaction_checkpoint;
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{ConnectionTrait, EntityTrait, QueryFilter, QuerySelect};

#[derive(Clone, Debug)]
pub struct CheckpointBody {
    pub id: String,
    pub operation_id: String,
    pub owner: String,
    pub previous: Option<String>,
    pub summary: String,
    pub identity_sha256: String,
    pub selection: pioneer_compaction::ModelSelection,
    pub projection_version: u64,
    pub format_version: u32,
}

#[derive(Clone, Debug)]
pub struct CheckpointMetadata {
    pub id: String,
    pub operation_id: String,
    pub owner: String,
    pub previous: Option<String>,
    pub identity_sha256: String,
    pub selection: pioneer_compaction::ModelSelection,
    pub projection_version: u64,
    pub format_version: u32,
}

/// Load projection data without re-reading coverage. Callers must first
/// authorize and validate the exact checkpoint SourceRef through metadata.
pub async fn compaction_checkpoint_body<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<CheckpointBody>> {
    let Some((
        id,
        operation_id,
        owner,
        previous,
        summary,
        identity_sha256,
        selection,
        projection_version,
        format_version,
    )) = compaction_checkpoint::Entity::find_by_id(id)
        .select_only()
        .column(compaction_checkpoint::Column::Id)
        .column(compaction_checkpoint::Column::OperationId)
        .column(compaction_checkpoint::Column::Owner)
        .column(compaction_checkpoint::Column::Previous)
        .column(compaction_checkpoint::Column::Summary)
        .column(compaction_checkpoint::Column::IdentitySha256)
        .column_as(
            Expr::col(compaction_checkpoint::Column::Selection).cast_as("text"),
            "selection",
        )
        .column(compaction_checkpoint::Column::ProjectionVersion)
        .column(compaction_checkpoint::Column::FormatVersion)
        .into_tuple::<(
            String,
            String,
            String,
            Option<String>,
            String,
            String,
            String,
            i64,
            i64,
        )>()
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    // Decode only after the bounded database read has released its capacity.
    Ok(Some(CheckpointBody {
        id,
        operation_id,
        owner,
        previous,
        summary,
        identity_sha256,
        selection: serde_json::from_str(&selection)?,
        projection_version: u64::try_from(projection_version)?,
        format_version: u32::try_from(format_version)?,
    }))
}

/// Projection metadata without summary payload. Selection is decoded here to
/// preserve the existing fail-closed validation without retaining body text.
/// Callers must first authorize the exact checkpoint SourceRef through edges.
pub async fn compaction_checkpoint_metadata<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<CheckpointMetadata>> {
    let Some((
        id,
        operation_id,
        owner,
        previous,
        identity_sha256,
        selection,
        projection_version,
        format_version,
    )) = compaction_checkpoint::Entity::find_by_id(id)
        .filter(Expr::cust("octet_length(selection::text) <= 262144"))
        .select_only()
        .column(compaction_checkpoint::Column::Id)
        .column(compaction_checkpoint::Column::OperationId)
        .column(compaction_checkpoint::Column::Owner)
        .column(compaction_checkpoint::Column::Previous)
        .column(compaction_checkpoint::Column::IdentitySha256)
        .column_as(
            Expr::col(compaction_checkpoint::Column::Selection).cast_as("text"),
            "selection",
        )
        .column(compaction_checkpoint::Column::ProjectionVersion)
        .column(compaction_checkpoint::Column::FormatVersion)
        .into_tuple::<(
            String,
            String,
            String,
            Option<String>,
            String,
            String,
            i64,
            i64,
        )>()
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(CheckpointMetadata {
        id,
        operation_id,
        owner,
        previous,
        identity_sha256,
        selection: serde_json::from_str(&selection)?,
        projection_version: u64::try_from(projection_version)?,
        format_version: u32::try_from(format_version)?,
    }))
}
