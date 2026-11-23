//! Immutable import proofs and exact archived-preview provenance.
use anyhow::{Result, ensure};
use pioneer_compaction::{SourceRef, frozen::FrozenMessageRef};
use sea_orm::ConnectionTrait;
use sea_orm::sea_query::{Expr, ExprTrait};

pub const EMPTY_FROZEN_IMPORT_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenImportRecord {
    pub message_ordinal: u64,
    pub source_thread: String,
    pub source: SourceRef,
    pub delivery_id: String,
    pub candidate_id: String,
    pub output_manifest: String,
    pub output_ordinal: u64,
    pub acknowledgement: SourceRef,
    /// The grant above remains the immutable output source. Only a verified
    /// temporary preview may render a diagnostic at this distinct revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_archive: Option<PreviewArchiveProjection>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviewArchiveProjection {
    pub source: SourceRef,
    pub wire_sha256: String,
}
impl FrozenImportRecord {
    pub fn render_source(&self) -> &SourceRef {
        self.preview_archive
            .as_ref()
            .map_or(&self.source, |p| &p.source)
    }
    pub fn matches_render_target(&self, target: &FrozenMessageRef) -> bool {
        target.source_thread == self.source_thread
            && target.sources.contains(self.render_source())
            && self.preview_archive.as_ref().is_none_or(|p| {
                target.tool_name.as_deref() == Some("html_preview")
                    && target.sources.as_slice() == std::slice::from_ref(&p.source)
                    && target.replay_source.as_ref().is_none_or(|s| s == &p.source)
                    && target.wire_sha256 == p.wire_sha256
            })
    }
}

/// Only the existing archive receipt can translate an immutable grant to a
/// body revision. Prepared SQL revalidates that receipt inside publication;
/// no parsing or hashing is done while holding writer capacity.
pub async fn prepare_preview_archive_guards(
    db: &impl ConnectionTrait,
    workspace: &str,
    record: &FrozenImportRecord,
) -> Result<Option<(sea_orm::Statement, sea_orm::Statement)>> {
    let Some(projection) = &record.preview_archive else {
        return Ok(None);
    };
    use pioneer_crud_artifacts::html_preview_retention_queries as queries;
    use pioneer_entity::{
        compaction_source_revision as revision, html_preview_frozen_result as receipt, thread,
        turn, turn_llm_context as context,
    };
    use queries::{FrozenMessage, bytes, col};
    let base = queries::archive_receipt(
        workspace,
        &record.source_thread,
        &record.output_manifest,
        i64::try_from(record.output_ordinal)?,
    );
    let query = base
        .clone()
        .clear_selects()
        .columns([
            ("r", receipt::Column::ReferenceJson),
            ("r", receipt::Column::ArchiveWireSha256),
        ])
        .expr_as(
            col("m", FrozenMessage::ReferenceJson),
            "output_reference_json",
        )
        .and_where(col("r", receipt::Column::SourceId).eq(&record.source.id))
        .and_where(col("r", receipt::Column::SourceVersion).eq(&record.source.version))
        .and_where(bytes(col("r", receipt::Column::ReferenceJson)).lte(262144))
        .and_where(col("m", FrozenMessage::Bytes).between(0, 262144))
        .and_where(bytes(col("m", FrozenMessage::ReferenceJson)).eq(col("m", FrozenMessage::Bytes)))
        .to_owned();
    let row = db
        .query_one(&query)
        .await?
        .ok_or_else(|| anyhow::anyhow!("archival preview grant receipt is unavailable"))?;
    let json: String = row.try_get("", "reference_json")?;
    let original: FrozenMessageRef = serde_json::from_str(&json)?;
    original.validate()?;
    let output_reference_json: String = row.try_get("", "output_reference_json")?;
    let output_reference: FrozenMessageRef = serde_json::from_str(&output_reference_json)?;
    ensure!(
        original == output_reference,
        "archival preview receipt/output mismatch"
    );
    let revision = record
        .source
        .version
        .strip_prefix("revision:")
        .and_then(|s| s.parse::<u64>().ok())
        .and_then(|n| n.checked_add(1))
        .ok_or_else(|| anyhow::anyhow!("invalid preview grant revision"))?;
    // Indexing an already archived diagnostic also yields a receipt. Its
    // unchanged wire body proves no retirement and cannot grant a new revision.
    ensure!(
        original.tool_name.as_deref() == Some("html_preview")
            && original.source_thread == record.source_thread
            && original.sources.as_slice() == std::slice::from_ref(&record.source)
            && record.source.scope.starts_with("context:")
            && projection.source.scope == record.source.scope
            && projection.source.id == record.source.id
            && projection.source.version == format!("revision:{revision}")
            && projection.wire_sha256 != original.wire_sha256
            && projection.wire_sha256 == row.try_get::<String>("", "archive_wire_sha256")?,
        "archival preview grant does not match its exact diagnostic"
    );
    let historical = base
        .clone()
        .and_where(col("r", receipt::Column::ReferenceJson).eq(json))
        .and_where(col("r", receipt::Column::ArchiveWireSha256).eq(&projection.wire_sha256))
        .and_where(col("m", FrozenMessage::ReferenceJson).eq(output_reference_json))
        .to_owned();
    let canonical = queries::canonical_preview()
        .and_where(col("c", context::Column::Id).eq(col("r", receipt::Column::SourceId)))
        .and_where(col("c", context::Column::TurnId).eq(col("r", receipt::Column::TurnId)))
        .and_where(
            queries::revision(col("v", revision::Column::Revision)).eq(&projection.source.version),
        )
        .and_where(col("th", thread::Column::WorkspaceId).eq(workspace))
        .and_where(col("t", turn::Column::ThreadId).eq(&record.source_thread))
        .and_where(Expr::exists(queries::released_marker(workspace)))
        .to_owned();
    let current = historical
        .clone()
        .and_where(Expr::exists(canonical))
        .to_owned();
    let historical = db.get_database_backend().build(&historical);
    let current = db.get_database_backend().build(&current);
    Ok(Some((current, historical)))
}
impl FrozenImportRecord {
    /// Historical proof validation does not grant access or accept a fresh body.
    /// Direct import preparation/publication separately requires current guard.
    pub async fn validate_preview_archive(
        &self,
        db: &impl ConnectionTrait,
        workspace: &str,
    ) -> Result<()> {
        let _ = prepare_preview_archive_guards(db, workspace, self).await?;
        Ok(())
    }
}

use crate::SOURCE_PAGE_BYTES;
use crate::frozen_views::import as compaction_frozen_import;
use pioneer_entity::compaction_frozen_history;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

pub async fn compaction_frozen_import_page<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    owner: &str,
    manifest: &str,
    start: u64,
) -> Result<Vec<FrozenImportRecord>> {
    let mut complete_count = None;
    if let Some(h) = compaction_frozen_history::Entity::find_by_id(manifest)
        .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
        .filter(compaction_frozen_history::Column::OwnerThread.eq(owner))
        .one(db)
        .await?
    {
        crate::frozen::logical_bounds(&h)?;
        if let Some(layout) = crate::frozen::find_layout(manifest.to_owned(), 1_i64)
            .one(db)
            .await?
        {
            ensure!(layout.failed == 0, "invalid frozen layout state");
        }
        if h.ready == 1 {
            complete_count = Some(u64::try_from(h.import_count)?);
        }
    }
    let scoped = compaction_frozen_import::Entity::find()
        .inner_join(compaction_frozen_history::Entity)
        .filter(compaction_frozen_history::Column::Id.eq(manifest))
        .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
        .filter(compaction_frozen_history::Column::OwnerThread.eq(owner))
        .filter(compaction_frozen_history::Column::Ready.eq(1_i64))
        .filter(compaction_frozen_import::Column::Ordinal.gte(i64::try_from(start)?));
    let rows = scoped
        .clone()
        .select_only()
        .column(compaction_frozen_import::Column::Ordinal)
        .column(compaction_frozen_import::Column::Bytes)
        .order_by_asc(compaction_frozen_import::Column::Ordinal)
        .limit(128)
        .into_tuple::<(i64, i64)>()
        .all(db)
        .await?;
    let mut end = start;
    let mut bytes = 0;
    for (ordinal, size) in rows {
        let size = usize::try_from(size)?;
        ensure!(size <= SOURCE_PAGE_BYTES, "invalid import metadata size");
        if bytes + size > SOURCE_PAGE_BYTES {
            break;
        }
        ensure!(u64::try_from(ordinal)? == end, "import ordinal gap");
        bytes += size;
        end += 1;
    }
    ensure!(
        complete_count.is_none_or(|count| start >= count || end > start),
        "import page missing before declared count"
    );
    let rows = scoped
        .select_only()
        .column(compaction_frozen_import::Column::ProofJson)
        .filter(compaction_frozen_import::Column::Ordinal.lt(i64::try_from(end)?))
        .filter(Expr::cust("octet_length(compaction_frozen_import.proof_json)=compaction_frozen_import.bytes AND compaction_frozen_import.bytes BETWEEN 0 AND 262144"))
        .order_by_asc(compaction_frozen_import::Column::Ordinal)
        .into_tuple::<String>()
        .all(db)
        .await?;
    ensure!(
        rows.len() == usize::try_from(end - start)?,
        "import bounded readback incomplete"
    );
    rows.into_iter()
        .map(|json| Ok(serde_json::from_str(&json)?))
        .collect()
}
