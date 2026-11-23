//! Exact witnesses for bounded physical retirement of immutable ranges.
use anyhow::{Result, ensure};
use pioneer_entity::{compaction_frozen_history as history, compaction_frozen_layout as layout};
use sea_orm::{DbBackend, Statement};

use crate::frozen_expiry::unknown_predicate;

pub const SWEEP_ROWS: u64 = 256;
pub const SWEEP_BYTES: i64 = 1024 * 1024;

#[derive(Default)]
pub struct PhysicalDeleteResult {
    pub rows: u64,
    pub bytes: u64,
    pub refused: bool,
}
impl PhysicalDeleteResult {
    pub fn refused() -> Self {
        Self {
            refused: true,
            ..Self::default()
        }
    }
    pub fn from_rows(rows: &[sea_orm::QueryResult]) -> Result<Self> {
        let mut bytes = 0;
        for row in rows {
            bytes += row.try_get::<i64>("", "bytes")? as u64;
        }
        Ok(Self {
            rows: rows.len() as u64,
            bytes,
            refused: false,
        })
    }
}
// A checked geometry token, local to this visit, not a global/durable revision.
// Active ready spans are immutable. Incomplete appends atomically change the
// header counters; activation changes the layout. Compare both before DELETE.
pub struct CheckedGeometry {
    pub h: history::Model,
    pub l: Option<layout::Model>,
}
pub fn healthy(alias: &str) -> String {
    format!(
        "{alias}.ready IN (0,1) AND {alias}.expired IN (0,1) AND {alias}.message_count>=0 AND {alias}.import_count>=0 AND {alias}.next_ordinal BETWEEN 0 AND {alias}.message_count AND {alias}.next_import BETWEEN 0 AND {alias}.import_count AND ({alias}.ready=0 OR ({alias}.next_ordinal={alias}.message_count AND {alias}.next_import={alias}.import_count))"
    )
}
pub fn physical_table(kind: i64) -> (&'static str, &'static str) {
    if kind == 0 {
        ("compaction_frozen_message_data", "reference_json")
    } else {
        ("compaction_frozen_import_data", "proof_json")
    }
}

const GEOMETRY_HEADER_FIELDS: [&str; 12] = [
    "id",
    "workspace_id",
    "owner_thread",
    "identity_sha256",
    "imports_sha256",
    "message_count",
    "import_count",
    "next_ordinal",
    "next_import",
    "ready",
    "expired",
    "storage_registered",
];
const GEOMETRY_LAYOUT_FIELDS: [&str; 10] = [
    "manifest_id",
    "kind",
    "active",
    "pending",
    "candidate",
    "compared",
    "copy_to",
    "copy_next",
    "cleanup_to",
    "failed",
];

// Prepare the bounded row projections before reserving a writer. The final
// guard uses PostgreSQL's typed json_to_recordset to bind all witnesses as one
// parameter even when independently prepared pages contain many references.
pub fn geometry_values<'a>(
    geometries: impl IntoIterator<Item = &'a CheckedGeometry>,
) -> Result<String> {
    let rows = geometries
        .into_iter()
        .map(|g| {
            let h = &g.h;
            let l = g.l.as_ref();
            serde_json::json!({
                "h_id": h.id,
                "h_workspace_id": h.workspace_id,
                "h_owner_thread": h.owner_thread,
                "h_identity_sha256": h.identity_sha256,
                "h_imports_sha256": h.imports_sha256,
                "h_message_count": h.message_count,
                "h_import_count": h.import_count,
                "h_next_ordinal": h.next_ordinal,
                "h_next_import": h.next_import,
                "h_ready": h.ready,
                "h_expired": h.expired,
                "h_storage_registered": h.storage_registered,
                "l_manifest_id": l.map(|l| &l.manifest_id),
                "l_kind": l.map(|l| l.kind),
                "l_active": l.map(|l| l.active),
                "l_pending": l.map(|l| l.pending),
                "l_candidate": l.and_then(|l| l.candidate.as_deref()),
                "l_compared": l.map(|l| l.compared),
                "l_copy_to": l.and_then(|l| l.copy_to),
                "l_copy_next": l.map(|l| l.copy_next),
                "l_cleanup_to": l.map(|l| l.cleanup_to),
                "l_failed": l.map(|l| l.failed),
            })
        })
        .collect::<Vec<_>>();
    Ok(serde_json::to_string(&rows)?)
}

// One common guard for the whole selected free interval, never a predicate
// repeated for each physical row. Missing FK parents explicitly fail closed.
pub fn sweep_guard(manifest: &str, kind: i64, prepared: &str, first: i64, last: i64) -> Statement {
    sweep_validation(manifest, kind, prepared, Some((first, last)))
}

// Held completion needs the same current geometry and unknown-scope checks,
// without the interval predicates which intentionally reject retained rows.
pub fn sweep_validation(
    manifest: &str,
    kind: i64,
    prepared: &str,
    interval: Option<(i64, i64)>,
) -> Statement {
    let (first, last) = interval.unwrap_or((0, 0));
    let deleting = if interval.is_some() { "1" } else { "0" };
    let h_fields = GEOMETRY_HEADER_FIELDS;
    let l_fields = GEOMETRY_LAYOUT_FIELDS;
    let same_h = h_fields
        .iter()
        .map(|f| format!("h.{f} IS NOT DISTINCT FROM e.h_{f}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    // Cleanup cursor movement cannot change published geometry. Validate its
    // current bounds without invalidating a continuing visit on every advance.
    let same_l = l_fields
        .iter()
        .map(|f| format!("l.{f} IS NOT DISTINCT FROM e.l_{f}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let health = healthy("h");
    let unknown = unknown_predicate("p");
    let hk = if kind == 0 {
        "message_count"
    } else {
        "import_count"
    };
    let nk = if kind == 0 {
        "next_ordinal"
    } else {
        "next_import"
    };
    Statement::from_sql_and_values(
        DbBackend::Postgres,
        format!(
            r#"
WITH checked AS MATERIALIZED (
 SELECT * FROM json_to_recordset($5::json) AS geometry(
  h_id text, h_workspace_id text, h_owner_thread text,
  h_identity_sha256 text, h_imports_sha256 text,
  h_message_count bigint, h_import_count bigint, h_next_ordinal bigint, h_next_import bigint,
  h_ready bigint, h_expired bigint, h_storage_registered bigint,
  l_manifest_id text, l_kind bigint, l_active bigint, l_pending bigint, l_candidate text,
  l_compared bigint, l_copy_to bigint, l_copy_next bigint, l_cleanup_to bigint, l_failed bigint
 )
)
SELECT 1 WHERE
NOT EXISTS(SELECT 1 FROM checked e LEFT JOIN compaction_frozen_history h ON h.id=e.h_id LEFT JOIN compaction_frozen_layout l ON l.manifest_id=e.h_id AND l.kind=$2 WHERE h.id IS NULL OR NOT ({same_h}) OR (l.manifest_id IS NOT NULL AND (l.cleanup_next<0 OR l.cleanup_next>l.cleanup_to)) OR NOT ((e.l_manifest_id IS NULL AND l.manifest_id IS NULL) OR (e.l_manifest_id IS NOT NULL AND l.manifest_id IS NOT NULL AND {same_l})))
AND EXISTS(SELECT 1 FROM compaction_frozen_history p WHERE p.id=$1)
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history p WHERE p.id=$1 AND ({unknown}))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=$1 AND (NOT ({health}) OR ({deleting}=1 AND h.expired=0 AND $3<CASE h.ready WHEN 1 THEN h.{hk} ELSE h.{nk} END AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=h.id AND l.kind=$2 AND l.active=1))))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_span s LEFT JOIN checked e ON e.h_id=s.manifest_id LEFT JOIN compaction_frozen_history h ON h.id=s.manifest_id LEFT JOIN compaction_frozen_layout l ON l.manifest_id=s.manifest_id AND l.kind=s.kind WHERE s.source_manifest=$1 AND s.kind=$2 AND (e.h_id IS NULL OR h.id IS NULL OR NOT ({health}) OR l.manifest_id IS NULL OR l.failed<>0 OR l.active<>1 OR l.copy_next<COALESCE(l.copy_to,0) OR (h.storage_registered<>0 AND l.manifest_id IS NULL) OR ({deleting}=1 AND h.expired=0 AND s.start<=$4 AND $3<s."end" AND $3<CASE h.ready WHEN 1 THEN h.{hk} ELSE h.{nk} END)))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.kind=$2 AND (l.manifest_id=$1 OR l.candidate=$1) AND (l.failed<>0 OR (({deleting}=1 OR l.manifest_id<>$1) AND l.active<>1) OR l.copy_next<COALESCE(l.copy_to,0)))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=$1 AND h.storage_registered<>0 AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=h.id AND l.kind=$2))
"#
        ),
        [
            manifest.into(),
            kind.into(),
            first.into(),
            last.into(),
            prepared.into(),
        ],
    )
}

// The installed builders do not expose DELETE joined to a typed VALUES
// relation with RETURNING. Match the bounded page's byte counts under lock.
pub fn physical_delete(manifest: &str, kind: i64, keys: &[(i64, i64)]) -> Result<Statement> {
    ensure!(
        matches!(kind, 0 | 1) && !keys.is_empty() && keys.len() <= SWEEP_ROWS as usize,
        "invalid physical batch"
    );
    let mut bytes = 0;
    let mut values: Vec<sea_orm::Value> = vec![manifest.into()];
    let mut tuples = Vec::with_capacity(keys.len());
    for &(ordinal, size) in keys {
        ensure!(
            ordinal >= 0 && (0..=SWEEP_BYTES).contains(&size),
            "invalid physical key"
        );
        bytes += size;
        tuples.push(format!(
            "(${}::bigint,${}::bigint)",
            values.len() + 1,
            values.len() + 2
        ));
        values.extend([ordinal.into(), size.into()]);
    }
    ensure!(bytes <= SWEEP_BYTES, "physical batch byte limit exceeded");
    let (data, payload) = physical_table(kind);
    let import_key = if kind == 1 {
        "AND d.message_ordinal>=0"
    } else {
        ""
    };
    Ok(Statement::from_sql_and_values(
        DbBackend::Postgres,
        format!(
            "WITH selected(ordinal,bytes) AS (VALUES {}) DELETE FROM {data} AS d WHERE d.manifest_id=$1 AND d.ordinal IN (SELECT ordinal FROM selected) AND EXISTS(SELECT 1 FROM selected k WHERE k.ordinal=d.ordinal AND k.bytes=d.bytes) AND octet_length(d.{payload})=d.bytes {import_key} RETURNING bytes",
            tuples.join(",")
        ),
        values,
    ))
}

pub fn duplicate_cleanup_statements(
    h: &history::Model,
    l: &layout::Model,
    keys: &[(i64, i64)],
) -> Result<Option<(Statement, Statement)>> {
    if keys.is_empty() {
        return Ok(None);
    }
    let prepared = geometry_values(&[CheckedGeometry {
        h: h.clone(),
        l: Some(l.clone()),
    }])?;
    Ok(Some((
        sweep_guard(
            &h.id,
            l.kind,
            &prepared,
            keys[0].0,
            keys.last().expect("keys").0,
        ),
        physical_delete(&h.id, l.kind, keys)?,
    )))
}
