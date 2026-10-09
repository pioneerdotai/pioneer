//! Checkpoint metadata proofs preserve the existing extractor, never mint OWN.
use super::compaction::{
    self, HistoricalEventInputEvidenceRow as Evidence, HistoricalReplayAliasRow as Alias,
    HistoricalSourceRef,
};
use crate::{CrudStore, FrozenUseGuard};
use anyhow::Result;
#[derive(Debug)]
struct ProofIntegrityError(&'static str);
impl std::fmt::Display for ProofIntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ProofIntegrityError {}
macro_rules! ensure {
    ($condition:expr,$message:literal $(,)?) => {
        if !$condition {
            return Err(ProofIntegrityError($message).into());
        }
    };
}

use pioneer_compaction::{CoverageDomain, OperationSnapshot, SourceRef};
use pioneer_entity::{
    compaction_checkpoint_event_input_proof as evidence, compaction_checkpoint_proof as header,
    compaction_checkpoint_replay_proof as replay,
};
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ActiveModelTrait, DbBackend, FromQueryResult, IntoActiveModel, QueryOrder, QuerySelect, Set,
    Statement, TransactionTrait, entity::prelude::*,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

fn text(hash: &mut Sha256, value: &str) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value.as_bytes());
}
fn optional(hash: &mut Sha256, value: Option<&str>) {
    hash.update([u8::from(value.is_some())]);
    if let Some(value) = value {
        text(hash, value);
    }
}
fn begin(tag: &[u8], count: usize) -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(tag);
    hash.update((count as u64).to_be_bytes());
    hash
}
pub(super) fn coverage_digest(rows: &[HistoricalSourceRef]) -> String {
    let mut sorted = rows.iter().collect::<Vec<_>>();
    sorted.sort_by(|a, b| {
        (
            &a.source.scope,
            &a.source.id,
            &a.source.version,
            &a.source_thread,
        )
            .cmp(&(
                &b.source.scope,
                &b.source.id,
                &b.source.version,
                &b.source_thread,
            ))
    });
    let mut hash = begin(b"p73/coverage/v1", rows.len());
    for row in sorted {
        for value in [
            &row.source_thread,
            &row.source.scope,
            &row.source.id,
            &row.source.version,
        ] {
            text(&mut hash, value);
        }
    }
    hex::encode(hash.finalize())
}
pub(super) fn aliases_digest(rows: &[Alias]) -> String {
    let mut hash = begin(b"p73/aliases/v1", rows.len());
    for row in rows {
        // Original HistoricalReplayAliasRow field order, including thread before scope.
        for value in [
            &row.covered_thread,
            &row.replay_thread,
            &row.covered_scope,
            &row.covered_id,
            &row.covered_version,
            &row.replay_scope,
            &row.replay_id,
            &row.replay_version,
        ] {
            text(&mut hash, value);
        }
        optional(&mut hash, row.tool_item_id.as_deref());
    }
    hex::encode(hash.finalize())
}
pub(super) fn evidence_digest(rows: &[Evidence]) -> String {
    let mut hash = begin(b"p73/evidence/v1", rows.len());
    for row in rows {
        for value in [
            &row.source_thread,
            &row.source_scope,
            &row.source_id,
            &row.source_version,
            &row.role,
        ] {
            text(&mut hash, value);
        }
    }
    hex::encode(hash.finalize())
}
fn alias_bytes(row: &Alias) -> usize {
    row.covered_thread.len()
        + row.replay_thread.len()
        + row.covered_scope.len()
        + row.covered_id.len()
        + row.covered_version.len()
        + row.replay_scope.len()
        + row.replay_id.len()
        + row.replay_version.len()
        + row.tool_item_id.as_ref().map_or(0, String::len)
}
fn evidence_bytes(row: &Evidence) -> usize {
    row.source_thread.len()
        + row.source_scope.len()
        + row.source_id.len()
        + row.source_version.len()
        + row.role.len()
}
fn alias_model(id: &str, ordinal: i64, row: &Alias) -> replay::ActiveModel {
    replay::ActiveModel {
        checkpoint_id: Set(id.into()),
        ordinal: Set(ordinal),
        covered_thread: Set(row.covered_thread.clone()),
        covered_scope: Set(row.covered_scope.clone()),
        covered_id: Set(row.covered_id.clone()),
        covered_version: Set(row.covered_version.clone()),
        replay_thread: Set(row.replay_thread.clone()),
        replay_scope: Set(row.replay_scope.clone()),
        replay_id: Set(row.replay_id.clone()),
        replay_version: Set(row.replay_version.clone()),
        tool_item_id: Set(row.tool_item_id.clone()),
        ..Default::default()
    }
}
fn evidence_model(id: &str, ordinal: i64, row: &Evidence) -> evidence::ActiveModel {
    evidence::ActiveModel {
        checkpoint_id: Set(id.into()),
        ordinal: Set(ordinal),
        source_thread: Set(row.source_thread.clone()),
        source_scope: Set(row.source_scope.clone()),
        source_id: Set(row.source_id.clone()),
        source_version: Set(row.source_version.clone()),
        role: Set(row.role.clone()),
        ..Default::default()
    }
}
fn alias_row(row: replay::Model) -> Alias {
    Alias {
        covered_thread: row.covered_thread,
        replay_thread: row.replay_thread,
        covered_scope: row.covered_scope,
        covered_id: row.covered_id,
        covered_version: row.covered_version,
        replay_scope: row.replay_scope,
        replay_id: row.replay_id,
        replay_version: row.replay_version,
        tool_item_id: row.tool_item_id,
    }
}
fn evidence_row(row: evidence::Model) -> Evidence {
    Evidence {
        source_thread: row.source_thread,
        source_scope: row.source_scope,
        source_id: row.source_id,
        source_version: row.source_version,
        role: row.role,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult)]
struct Binding {
    checkpoint_id: String,
    operation_id: String,
    owner: String,
    workspace_id: String,
    thread_id: String,
    checkpoint_identity_sha256: String,
    previous: Option<String>,
    projection_version: i64,
    format_version: i64,
    snapshot: String,
    manifest_id: String,
    identity_sha256: String,
    imports_sha256: String,
    import_count: i64,
}
async fn binding<C: ConnectionTrait>(db: &C, id: &str) -> Result<Binding> {
    Binding::find_by_statement(Statement::from_sql_and_values(DbBackend::Sqlite,
        "SELECT p.id AS checkpoint_id,p.operation_id,p.owner,c.workspace_id,c.thread_id,p.identity_sha256 AS checkpoint_identity_sha256,p.previous,p.projection_version,p.format_version,o.snapshot,x.manifest_id,x.identity_sha256,x.imports_sha256,x.import_count \
         FROM compaction_checkpoint p JOIN compaction_operation o ON o.id=p.operation_id AND o.owner=p.owner \
         JOIN compaction_context c ON c.owner=o.owner JOIN thread t ON t.id=c.thread_id AND t.workspace_id=c.workspace_id \
         JOIN workspace w ON w.id=c.workspace_id JOIN compaction_operation_projection x ON x.operation_id=o.id \
         WHERE p.id=? AND x.storage_state='bound' AND o.frozen_publication_contract<>'assertion_compat' AND (o.status='completed' OR NOT EXISTS(SELECT 1 FROM compaction_runner_plan r WHERE r.operation_id=o.id AND r.ready<>1))",[id.into()])).one(db).await?.ok_or_else(||anyhow::anyhow!("checkpoint proof has no real bound origin/domain"))
}
struct Plan {
    binding: Binding,
    model: header::ActiveModel,
    coverage: Vec<HistoricalSourceRef>,
    coverage_checks: Vec<Statement>,
    aliases: Vec<Alias>,
    evidence: Vec<Evidence>,
}
impl Plan {
    async fn validate<C: ConnectionTrait>(&self, db: &C, guard: &FrozenUseGuard) -> Result<()> {
        guard.validate_in(db, true).await?;
        ensure!(
            binding(db, &self.binding.checkpoint_id).await? == self.binding,
            "checkpoint proof original binding changed"
        );
        for statement in &self.coverage_checks {
            ensure!(
                db.query_one_raw(statement.clone()).await?.is_some(),
                "checkpoint proof coverage or historical ownership changed"
            );
        }
        Ok(())
    }
    fn matches(&self, row: &header::Model) -> bool {
        use header::Column::*;
        let actual = row.clone().into_active_model();
        [
            CheckpointId,
            OperationId,
            Owner,
            WorkspaceId,
            ThreadId,
            CheckpointIdentitySha256,
            Previous,
            ProjectionVersion,
            FormatVersion,
            CoverageDomain,
            OriginManifestId,
            OriginMessageCount,
            OriginIdentitySha256,
            OriginImportCount,
            OriginImportsSha256,
            CoverageCount,
            CoverageSha256,
            AliasCount,
            AliasesSha256,
            EvidenceCount,
            EvidenceSha256,
            ProofFormat,
        ]
        .into_iter()
        .all(|column| actual.get(column).into_value() == self.model.get(column).into_value())
    }
}

async fn plan(store: &CrudStore, b: Binding, guard: &FrozenUseGuard) -> Result<Plan> {
    let snapshot: OperationSnapshot = serde_json::from_str(&b.snapshot)?;
    ensure!(
        snapshot.owner == b.owner,
        "checkpoint proof snapshot owner changed"
    );
    super::compaction_frozen_verify::verify(store, guard).await?;
    let edges = compaction::compaction_checkpoint_edges(store, &b.checkpoint_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint proof coverage unavailable"))?;
    ensure!(
        edges.owner == b.owner
            && edges.workspace_id == b.workspace_id
            && edges.thread_id == b.thread_id
            && edges.identity_sha256 == b.checkpoint_identity_sha256
            && edges.previous == b.previous,
        "checkpoint proof coverage binding changed"
    );
    let ownership: BTreeMap<SourceRef, BTreeSet<String>> = edges
        .coverage
        .iter()
        .map(|row| {
            (
                row.source.clone(),
                BTreeSet::from([row.source_thread.clone()]),
            )
        })
        .collect();
    // Both preparation and the legacy reader call this one exact algorithm.
    let (aliases, evidence) =
        compaction::extract_checkpoint_projection_metadata(store, guard, &ownership).await?;
    let h = guard.header();
    let model = header::ActiveModel {
        checkpoint_id: Set(b.checkpoint_id.clone()),
        operation_id: Set(b.operation_id.clone()),
        owner: Set(b.owner.clone()),
        workspace_id: Set(b.workspace_id.clone()),
        thread_id: Set(b.thread_id.clone()),
        checkpoint_identity_sha256: Set(b.checkpoint_identity_sha256.clone()),
        previous: Set(b.previous.clone()),
        projection_version: Set(b.projection_version),
        format_version: Set(b.format_version),
        coverage_domain: Set(match snapshot.plan.coverage_domain {
            CoverageDomain::OwnContribution => "own_contribution",
            CoverageDomain::WorkingContext => "working_context",
        }
        .into()),
        origin_manifest_id: Set(h.id.clone()),
        origin_message_count: Set(h.message_count),
        origin_identity_sha256: Set(h.identity_sha256.clone()),
        origin_import_count: Set(h.import_count),
        origin_imports_sha256: Set(h.imports_sha256.clone()),
        coverage_count: Set(i64::try_from(edges.coverage.len())?),
        coverage_sha256: Set(coverage_digest(&edges.coverage)),
        alias_count: Set(i64::try_from(aliases.len())?),
        aliases_sha256: Set(aliases_digest(&aliases)),
        next_alias: Set(0),
        evidence_count: Set(i64::try_from(evidence.len())?),
        evidence_sha256: Set(evidence_digest(&evidence)),
        next_evidence: Set(0),
        proof_format: Set(1),
        state: Set("pending".into()),
        ..Default::default()
    };
    // Prepared outside capacity; only bounded scalar existence results enter the
    // final writer. No fresh raw leaf validation or hash is needed.
    let mut coverage_checks = vec![Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "SELECT 1 WHERE (SELECT COUNT(*) FROM compaction_coverage WHERE checkpoint_id=?)=?",
        [
            b.checkpoint_id.clone().into(),
            i64::try_from(edges.coverage.len())?.into(),
        ],
    )];
    for row in &edges.coverage {
        coverage_checks.push(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT 1 WHERE EXISTS(SELECT 1 FROM compaction_coverage WHERE checkpoint_id=?1 AND source_scope=?3 AND source_id=?4 AND source_version=?5) \
             AND EXISTS(SELECT 1 FROM compaction_manifest WHERE operation_id=?2 AND reference_only=0 AND source_scope=?3 AND source_id=?4 AND source_version=?5 AND source_thread=?6) \
             AND NOT EXISTS(SELECT 1 FROM compaction_manifest WHERE operation_id=?2 AND reference_only=0 AND source_scope=?3 AND source_id=?4 AND source_version=?5 AND source_thread<>?6)",
            [b.checkpoint_id.clone().into(),b.operation_id.clone().into(),row.source.scope.clone().into(),row.source.id.clone().into(),row.source.version.clone().into(),row.source_thread.clone().into()]));
    }
    Ok(Plan {
        coverage_checks,
        binding: b,
        model,
        coverage: edges.coverage,
        aliases,
        evidence,
    })
}
async fn stage(store: &CrudStore, plan: &Plan, guard: &FrozenUseGuard) -> Result<()> {
    let id = &plan.binding.checkpoint_id;
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            plan.validate(&tx, guard).await?;
            header::Entity::insert(plan.model.clone())
                .on_conflict(
                    OnConflict::column(header::Column::CheckpointId)
                        .do_nothing()
                        .to_owned(),
                )
                .exec_without_returning(&tx)
                .await?;
            let row = header::Entity::find_by_id(id)
                .one(&tx)
                .await?
                .ok_or_else(|| ProofIntegrityError("proof header unavailable"))?;
            ensure!(
                plan.matches(&row) && row.state != "quarantined",
                "checkpoint proof immutable header conflict"
            );
            tx.commit().await?;
            Ok(())
        })
        .await?;
    for kind in [0, 1] {
        let count = if kind == 0 {
            plan.aliases.len()
        } else {
            plan.evidence.len()
        };
        let mut start = 0usize;
        while start < count {
            let mut end = start;
            let mut bytes = 0usize;
            while end < count && end - start < 128 {
                let size = id.len()
                    + if kind == 0 {
                        alias_bytes(&plan.aliases[end])
                    } else {
                        evidence_bytes(&plan.evidence[end])
                    };
                ensure!(
                    size <= compaction::SOURCE_PAGE_BYTES,
                    "proof item exceeds original metadata quantum"
                );
                if bytes + size > compaction::SOURCE_PAGE_BYTES {
                    break;
                }
                bytes += size;
                end += 1;
            }
            store
                .run_serialized_write(|| async {
                    let tx = store.connection.begin().await?;
                    plan.validate(&tx, guard).await?;
                    let current = header::Entity::find_by_id(id)
                        .one(&tx)
                        .await?
                        .ok_or_else(|| ProofIntegrityError("proof header unavailable"))?;
                    ensure!(
                        plan.matches(&current) && current.state != "quarantined",
                        "proof staging binding changed"
                    );
                    let next = if kind == 0 {
                        current.next_alias
                    } else {
                        current.next_evidence
                    };
                    ensure!(
                        (start as i64 == next || end as i64 <= next)
                            && (current.state == "pending" || end as i64 <= next),
                        "proof append not sequential or exact retry"
                    );
                    for ordinal in start..end {
                        if kind == 0 {
                            if ordinal as i64 >= next && current.state == "pending" {
                                replay::Entity::insert(alias_model(
                                    id,
                                    ordinal as i64,
                                    &plan.aliases[ordinal],
                                ))
                                .on_conflict(
                                    OnConflict::columns([
                                        replay::Column::CheckpointId,
                                        replay::Column::Ordinal,
                                    ])
                                    .do_nothing()
                                    .to_owned(),
                                )
                                .exec_without_returning(&tx)
                                .await?;
                            }
                            let row = replay::Entity::find_by_id((id.clone(), ordinal as i64))
                                .one(&tx)
                                .await?
                                .ok_or_else(|| ProofIntegrityError("proof alias unavailable"))?;
                            ensure!(
                                alias_row(row) == plan.aliases[ordinal],
                                "proof alias immutable conflict"
                            );
                        } else {
                            if ordinal as i64 >= next && current.state == "pending" {
                                evidence::Entity::insert(evidence_model(
                                    id,
                                    ordinal as i64,
                                    &plan.evidence[ordinal],
                                ))
                                .on_conflict(
                                    OnConflict::columns([
                                        evidence::Column::CheckpointId,
                                        evidence::Column::Ordinal,
                                    ])
                                    .do_nothing()
                                    .to_owned(),
                                )
                                .exec_without_returning(&tx)
                                .await?;
                            }
                            let row = evidence::Entity::find_by_id((id.clone(), ordinal as i64))
                                .one(&tx)
                                .await?
                                .ok_or_else(|| ProofIntegrityError("proof evidence unavailable"))?;
                            ensure!(
                                evidence_row(row) == plan.evidence[ordinal],
                                "proof evidence immutable conflict"
                            );
                        }
                    }
                    if start as i64 == next && current.state == "pending" {
                        let column = if kind == 0 {
                            header::Column::NextAlias
                        } else {
                            header::Column::NextEvidence
                        };
                        let result = header::Entity::update_many()
                            .col_expr(column, sea_orm::sea_query::Expr::val(end as i64))
                            .filter(header::Column::CheckpointId.eq(id))
                            .filter(column.eq(next))
                            .filter(header::Column::State.eq("pending"))
                            .exec(&tx)
                            .await?;
                        ensure!(result.rows_affected == 1, "proof staging cursor changed");
                    }
                    tx.commit().await?;
                    Ok(())
                })
                .await?;
            start = end;
        }
    }
    Ok(())
}

// Size discovery uses bounded PK pages and UTF-8 byte lengths before fetching item text.
pub(super) async fn read_items(
    store: &CrudStore,
    id: &str,
    counts: (i64, i64),
) -> Result<(Vec<Alias>, Vec<Evidence>)> {
    let mut aliases = Vec::new();
    let mut evidence_rows = Vec::new();
    for (kind, count) in [(0, counts.0), (1, counts.1)] {
        ensure!(count >= 0, "invalid proof count");
        let mut start = 0i64;
        let (table, columns) = if kind == 0 {
            (
                "compaction_checkpoint_replay_proof",
                vec![
                    "covered_thread",
                    "covered_scope",
                    "covered_id",
                    "covered_version",
                    "replay_thread",
                    "replay_scope",
                    "replay_id",
                    "replay_version",
                    "tool_item_id",
                ],
            )
        } else {
            (
                "compaction_checkpoint_event_input_proof",
                vec![
                    "source_thread",
                    "source_scope",
                    "source_id",
                    "source_version",
                    "role",
                ],
            )
        };
        let size = columns
            .iter()
            .map(|column| format!("COALESCE(length(CAST({column} AS BLOB)),0)"))
            .collect::<Vec<_>>()
            .join("+");
        while start < count {
            let sizes=store.connection.query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite,format!("SELECT ordinal,length(CAST(checkpoint_id AS BLOB))+{size} AS bytes FROM {table} WHERE checkpoint_id=? AND ordinal>=? ORDER BY ordinal LIMIT 128"),[id.into(),start.into()])).await?;
            let mut end = start;
            let mut bytes = 0i64;
            for row in sizes {
                let ordinal = row.try_get::<i64>("", "ordinal")?;
                let size = row.try_get::<i64>("", "bytes")?;
                ensure!(
                    size >= 0 && size <= compaction::SOURCE_PAGE_BYTES as i64,
                    "invalid proof item size"
                );
                if bytes + size > compaction::SOURCE_PAGE_BYTES as i64 {
                    break;
                }
                ensure!(
                    ordinal == end && ordinal < count,
                    "proof item ordinal gap or unexpected tail"
                );
                bytes += size;
                end += 1;
            }
            ensure!(end > start, "proof item sequence incomplete");
            if kind == 0 {
                let rows = replay::Entity::find()
                    .filter(replay::Column::CheckpointId.eq(id))
                    .filter(replay::Column::Ordinal.gte(start))
                    .filter(replay::Column::Ordinal.lt(end))
                    .order_by_asc(replay::Column::Ordinal)
                    .all(&store.connection)
                    .await?;
                ensure!(
                    rows.len() == usize::try_from(end - start)?
                        && rows
                            .iter()
                            .enumerate()
                            .all(|(n, row)| row.ordinal == start + n as i64),
                    "proof alias page incomplete"
                );
                aliases.extend(rows.into_iter().map(alias_row));
            } else {
                let rows = evidence::Entity::find()
                    .filter(evidence::Column::CheckpointId.eq(id))
                    .filter(evidence::Column::Ordinal.gte(start))
                    .filter(evidence::Column::Ordinal.lt(end))
                    .order_by_asc(evidence::Column::Ordinal)
                    .all(&store.connection)
                    .await?;
                ensure!(
                    rows.len() == usize::try_from(end - start)?
                        && rows
                            .iter()
                            .enumerate()
                            .all(|(n, row)| row.ordinal == start + n as i64),
                    "proof evidence page incomplete"
                );
                evidence_rows.extend(rows.into_iter().map(evidence_row));
            }
            start = end;
        }
        let extra=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,format!("SELECT ordinal FROM {table} WHERE checkpoint_id=? AND (ordinal<0 OR ordinal>=?) LIMIT 1"),[id.into(),count.into()])).await?;
        ensure!(extra.is_none(), "proof contains unexpected ordinal");
    }
    Ok((aliases, evidence_rows))
}

/// Metadata-only historical cutover. A promised complete proof never falls back
/// to an origin body; the projection is an immutable receipt after detach.
pub(super) async fn read_effective(
    store: &CrudStore,
    id: &str,
    ownership: &BTreeMap<SourceRef, BTreeSet<String>>,
) -> Result<Option<(Vec<Alias>, Vec<Evidence>)>> {
    use pioneer_entity::{
        compaction_checkpoint as cp, compaction_context as ctx, compaction_operation as op,
        compaction_operation_projection as origin,
    };
    let checkpoint = cp::Entity::find_by_id(id)
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("historical checkpoint unavailable"))?;
    let operation = op::Entity::find_by_id(&checkpoint.operation_id)
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("historical operation unavailable"))?;
    let projection = origin::Entity::find_by_id(&operation.id)
        .one(&store.connection)
        .await?;
    if operation.frozen_publication_contract == "assertion_compat" {
        ensure!(
            projection
                .as_ref()
                .is_none_or(|p| p.storage_state == "bound"),
            "compat historical projection detached"
        );
        return Ok(None);
    }
    if operation.frozen_proof_state != "complete" || !store.frozen_history_protocol_enabled() {
        ensure!(
            projection
                .as_ref()
                .is_none_or(|p| p.storage_state == "bound"),
            "historical proof-only origin has no effective complete proof"
        );
        ensure!(
            operation.frozen_publication_contract != "native_frozen" || projection.is_some(),
            "native historical origin unavailable"
        );
        return Ok(None);
    }
    ensure!(
        operation.status == "completed"
            && operation.outcome.as_deref() == Some("applied")
            && operation.frozen_inventory_state == "known"
            && operation.frozen_checkpoint_count.is_some()
            && operation.frozen_checkpoint_count == operation.frozen_prepared_checkpoint_count,
        "historical complete seal invalid"
    );
    let projection = projection
        .ok_or_else(|| anyhow::anyhow!("historical immutable origin receipt unavailable"))?;
    let context = ctx::Entity::find_by_id(&checkpoint.owner)
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("historical context unavailable"))?;
    let proof = header::Entity::find_by_id(id)
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("promised historical proof unavailable"))?;
    let snapshot: OperationSnapshot = serde_json::from_str(&operation.snapshot)?;
    let domain = match snapshot.plan.coverage_domain {
        CoverageDomain::OwnContribution => "own_contribution",
        CoverageDomain::WorkingContext => "working_context",
    };
    ensure!(
        operation.owner == checkpoint.owner
            && snapshot.owner == checkpoint.owner
            && context.format_version == checkpoint.format_version
            && proof.operation_id == operation.id
            && proof.owner == checkpoint.owner
            && proof.workspace_id == context.workspace_id
            && proof.thread_id == context.thread_id
            && proof.checkpoint_identity_sha256 == checkpoint.identity_sha256
            && proof.previous == checkpoint.previous
            && proof.projection_version == checkpoint.projection_version
            && proof.format_version == checkpoint.format_version
            && proof.coverage_domain == domain
            && proof.proof_format == 1
            && proof.state == "prepared"
            && checkpoint.frozen_accounting_state == "prepared"
            && proof.next_alias == proof.alias_count
            && proof.next_evidence == proof.evidence_count,
        "historical proof binding/format incomplete"
    );
    ensure!(
        proof.origin_manifest_id == projection.manifest_id
            && proof.origin_identity_sha256 == projection.identity_sha256
            && proof.origin_import_count == projection.import_count
            && proof.origin_imports_sha256 == projection.imports_sha256,
        "historical origin receipt mismatch"
    );
    ensure!(store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "SELECT h.id FROM compaction_frozen_history h JOIN thread t ON t.id=h.owner_thread AND t.workspace_id=h.workspace_id JOIN workspace w ON w.id=h.workspace_id WHERE h.id=? AND h.workspace_id=? AND h.identity_sha256=? AND h.message_count=? AND h.imports_sha256=? AND h.import_count=?",
        [proof.origin_manifest_id.clone().into(),context.workspace_id.clone().into(),proof.origin_identity_sha256.clone().into(),proof.origin_message_count.into(),proof.origin_imports_sha256.clone().into(),proof.origin_import_count.into()])).await?.is_some(),"historical immutable origin tuple mismatch");
    let coverage = ownership
        .iter()
        .map(|(source, threads)| {
            ensure!(
                threads.len() == 1,
                "historical proof coverage ownership ambiguous"
            );
            Ok(HistoricalSourceRef {
                source_thread: threads.first().expect("one owner").clone(),
                source: source.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        proof.coverage_count == i64::try_from(coverage.len())?
            && proof.coverage_sha256 == coverage_digest(&coverage),
        "historical proof coverage checksum mismatch"
    );
    let (aliases, evidence) =
        read_items(store, id, (proof.alias_count, proof.evidence_count)).await?;
    ensure!(
        proof.aliases_sha256 == aliases_digest(&aliases)
            && proof.evidence_sha256 == evidence_digest(&evidence),
        "historical proof metadata checksum mismatch"
    );
    // Immutable final results use exactly the extractor's stable sort. Digest
    // checks precede this assertion, never repair a damaged stored ordering.
    let mut ordered_aliases = aliases.clone();
    ordered_aliases.sort_by(|a, b| {
        (&a.replay_scope, &a.replay_id, &a.replay_thread).cmp(&(
            &b.replay_scope,
            &b.replay_id,
            &b.replay_thread,
        ))
    });
    let mut ordered_evidence = evidence.clone();
    ordered_evidence.sort_by(|a, b| {
        (
            &a.source_scope,
            &a.source_id,
            &a.source_thread,
            &a.source_version,
            &a.role,
        )
            .cmp(&(
                &b.source_scope,
                &b.source_id,
                &b.source_thread,
                &b.source_version,
                &b.role,
            ))
    });
    ensure!(
        aliases == ordered_aliases && evidence == ordered_evidence,
        "historical proof order mismatch"
    );
    let domain_exists=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT t.id FROM thread t JOIN workspace w ON w.id=t.workspace_id WHERE t.id=? AND t.workspace_id=?",[context.thread_id.clone().into(),context.workspace_id.clone().into()])).await?.is_some();
    ensure!(domain_exists, "historical proof domain unavailable");
    ensure!(
        header::Entity::find_by_id(id)
            .one(&store.connection)
            .await?
            .as_ref()
            == Some(&proof)
            && op::Entity::find_by_id(&operation.id)
                .one(&store.connection)
                .await?
                .as_ref()
                == Some(&operation)
            && cp::Entity::find_by_id(id)
                .one(&store.connection)
                .await?
                .as_ref()
                == Some(&checkpoint),
        "historical proof changed during read"
    );
    Ok(Some((aliases, evidence)))
}

pub(crate) async fn prepare(store: &CrudStore, id: &str) -> Result<()> {
    let b = binding(&store.connection, id).await?;
    let descriptor = store
        .compaction_bound_source_projection(&b.operation_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("proof origin unavailable"))?;
    let guard = store
        .compaction_pin_checkpoint_projection(id, &b.operation_id, &b.workspace_id, &descriptor)
        .await?;
    let result=async{
        let plan=plan(store,b.clone(),&guard).await?;
        stage(store,&plan,&guard).await?;
        let staged=header::Entity::find_by_id(id).one(&store.connection).await?.ok_or_else(||ProofIntegrityError("proof header unavailable"))?;
        ensure!(plan.matches(&staged) && staged.next_alias==staged.alias_count && staged.next_evidence==staged.evidence_count,"proof staging incomplete");
        let (aliases,evidence)=read_items(store,id,(staged.alias_count,staged.evidence_count)).await?;
        ensure!(aliases==plan.aliases && evidence==plan.evidence && aliases_digest(&aliases)==staged.aliases_sha256 && evidence_digest(&evidence)==staged.evidence_sha256 && coverage_digest(&plan.coverage)==staged.coverage_sha256,"staged proof digest conflict");
        store.run_serialized_write(||async{
            let tx=store.connection.begin().await?;plan.validate(&tx,&guard).await?;
            let current=header::Entity::find_by_id(id).one(&tx).await?.ok_or_else(||ProofIntegrityError("proof header unavailable"))?;
            ensure!(plan.matches(&current) && current.next_alias==current.alias_count && current.next_evidence==current.evidence_count && current.state!="quarantined","proof prepare binding changed");
            tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"UPDATE compaction_checkpoint_proof SET state='prepared' WHERE checkpoint_id=? AND state='pending' AND next_alias=alias_count AND next_evidence=evidence_count",[id.into()])).await?;
            let promoted=tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "UPDATE compaction_checkpoint SET frozen_accounting_state='prepared' WHERE id=? AND frozen_accounting_state='counted' AND EXISTS(SELECT 1 FROM compaction_operation o WHERE o.id=compaction_checkpoint.operation_id AND o.frozen_accounting_mode IN ('live_known','completed_inventory'))",[id.into()])).await?.rows_affected();
            if promoted==1 { tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"UPDATE compaction_operation SET frozen_prepared_checkpoint_count=frozen_prepared_checkpoint_count+1 WHERE id=? AND frozen_accounting_mode IN ('live_known','completed_inventory')",[b.operation_id.clone().into()])).await?; }
            tx.commit().await?;Ok(())
        }).await
    }.await;
    if let Err(error) = &result {
        if error.is::<ProofIntegrityError>()
            || error.is::<super::compaction_frozen_verify::FrozenIntegrityError>()
            || error.is::<serde_json::Error>()
        {
            // Only proven integrity failures poison the object. SQL/lock failure,
            // cancellation and an unavailable use/domain leave restartable work.
            let quarantine = store.run_serialized_write(||async {
                let tx=store.connection.begin().await?;
                guard.validate_in(&tx,true).await?;
                ensure!(binding(&tx,id).await?==b,"quarantine binding changed");
                tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"UPDATE compaction_checkpoint_proof SET state='quarantined' WHERE checkpoint_id=?",[id.into()])).await?;
                tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"UPDATE compaction_operation SET frozen_proof_state='quarantined' WHERE id=? AND frozen_publication_contract<>'assertion_compat'",[b.operation_id.clone().into()])).await?;
                tx.commit().await?;Ok(())
            }).await;
            // Preserve the initiating error; failed quarantine is conservative.
            if quarantine.is_err() {
                tracing::warn!(
                    phase = "proof",
                    outcome = "quarantine_deferred",
                    "Frozen proof quarantine retained as debt"
                );
            }
        }
    }
    guard.complete(result).await
}

/// Invoked only for the actual insert winner in the original candidate TX.
pub(super) async fn account_insert<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    checkpoint: &str,
    inserted: u64,
) -> Result<()> {
    if inserted == 0 {
        return Ok(());
    }
    let counted=db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"UPDATE compaction_checkpoint SET frozen_accounting_state='counted' WHERE id=? AND operation_id=? AND frozen_accounting_state='uncounted' AND EXISTS(SELECT 1 FROM compaction_operation o WHERE o.id=? AND o.frozen_accounting_mode='live_known' AND o.status='running')",[checkpoint.into(),operation.into(),operation.into()])).await?.rows_affected();
    if counted == 1 {
        let changed=db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"UPDATE compaction_operation SET frozen_checkpoint_count=frozen_checkpoint_count+1,frozen_proof_state='pending' WHERE id=? AND frozen_accounting_mode='live_known' AND status='running'",[operation.into()])).await?.rows_affected();
        ensure!(changed == 1, "checkpoint accounting operation changed");
    }
    Ok(())
}

/// Captured dependencies for the original publication transaction. No global fence.
pub(super) struct Publication {
    operation: String,
    contract: String,
    snapshot: String,
    execution_turn: Option<String>,
    workspace: String,
    thread: String,
    projection: Option<pioneer_entity::compaction_operation_projection::Model>,
    checkpoint: String,
    identity: String,
    total: Option<i64>,
    origin: Option<FrozenUseGuard>,
}
impl Publication {
    pub(super) async fn complete<T>(self, result: Result<T>) -> Result<T> {
        match self.origin {
            Some(guard) => guard.complete(result).await,
            None => result,
        }
    }
    pub(super) async fn validate<C: ConnectionTrait>(&self, db: &C) -> Result<bool> {
        let op = pioneer_entity::compaction_operation::Entity::find_by_id(&self.operation)
            .one(db)
            .await?;
        let Some(op) = op else { return Ok(false) };
        if op.frozen_publication_contract != self.contract
            || op.snapshot != self.snapshot
            || op.execution_turn != self.execution_turn
        {
            return Ok(false);
        }
        let context=db.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT c.owner FROM compaction_context c JOIN thread t ON t.id=c.thread_id AND t.workspace_id=c.workspace_id JOIN workspace w ON w.id=c.workspace_id WHERE c.owner=? AND c.workspace_id=? AND c.thread_id=?",
            [op.owner.clone().into(),self.workspace.clone().into(),self.thread.clone().into()])).await?;
        if context.is_none() {
            return Ok(false);
        }
        let checkpoint = db.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT id FROM compaction_checkpoint WHERE id=? AND operation_id=? AND owner=? AND identity_sha256=?",
            [self.checkpoint.clone().into(),self.operation.clone().into(),op.owner.clone().into(),self.identity.clone().into()])).await?;
        if checkpoint.is_none() {
            return Ok(false);
        }
        let projection =
            pioneer_entity::compaction_operation_projection::Entity::find_by_id(&self.operation)
                .one(db)
                .await?;
        if projection != self.projection {
            return Ok(false);
        }
        if self.contract == "native_frozen" {
            let Some(origin) = &self.origin else {
                return Ok(false);
            };
            origin.validate_in(db, true).await?;
        }
        if op.frozen_accounting_mode == "live_known" {
            if op.frozen_inventory_state != "known"
                || !matches!(op.frozen_proof_state.as_str(), "pending" | "prepared")
                || op.frozen_checkpoint_count.is_none()
            {
                return Ok(false);
            }
            if op.frozen_proof_state != "prepared"
                || op.frozen_checkpoint_count != self.total
                || op.frozen_prepared_checkpoint_count != self.total
            {
                return Err(compaction::FrozenProofReadinessInvalidated.into());
            }
            let row=db.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "SELECT p.id FROM compaction_checkpoint p JOIN compaction_checkpoint_proof h ON h.checkpoint_id=p.id AND h.operation_id=p.operation_id \
                 WHERE p.id=? AND p.operation_id=? AND p.identity_sha256=? AND p.frozen_accounting_state='prepared' AND h.state='prepared' \
                 AND h.checkpoint_identity_sha256=p.identity_sha256 AND h.previous IS p.previous AND h.projection_version=p.projection_version AND h.format_version=p.format_version",
                [self.checkpoint.clone().into(),self.operation.clone().into(),self.identity.clone().into()])).await?;
            if row.is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }
    pub(super) async fn seal_complete<C: ConnectionTrait>(&self, db: &C) -> Result<()> {
        if self.total.is_some() {
            let result=db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "UPDATE compaction_operation SET frozen_proof_state='complete' WHERE id=? AND status='completed' AND outcome='applied' AND frozen_accounting_mode='live_known' AND frozen_inventory_state='known' AND frozen_proof_state='prepared' AND frozen_checkpoint_count=? AND frozen_prepared_checkpoint_count=?",
                [self.operation.clone().into(),self.total.into(),self.total.into()])).await?;
            ensure!(
                result.rows_affected() == 1,
                "publication complete proof seal changed"
            );
        }
        Ok(())
    }
}

pub(super) async fn prepare_publication(
    store: &CrudStore,
    operation: &str,
    checkpoint: &str,
) -> Result<Publication> {
    use pioneer_entity::{
        compaction_checkpoint as cp, compaction_context as ctx, compaction_operation as op,
        compaction_operation_projection as projection,
    };
    let operation_row = op::Entity::find_by_id(operation)
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("publication operation missing"))?;
    let context = ctx::Entity::find_by_id(&operation_row.owner)
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("publication context missing"))?;
    let cp_identity = cp::Entity::find_by_id(checkpoint)
        .select_only()
        .column(cp::Column::IdentitySha256)
        .into_tuple::<String>()
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("publication checkpoint missing"))?;
    let bound = projection::Entity::find_by_id(operation)
        .one(&store.connection)
        .await?;
    let completed = operation_row.status == "completed";
    let origin = if !completed && operation_row.frozen_publication_contract == "native_frozen" {
        Some(
            store
                .compaction_pin_operation_projection(operation)
                .await?
                .ok_or_else(|| anyhow::anyhow!("native frozen publication has no bound origin"))?,
        )
    } else {
        None
    };
    let result=async{
        let total=if !completed && operation_row.frozen_accounting_mode=="live_known" {
            ensure!(store.frozen_history_protocol_enabled(),"exclusive frozen history permit missing");
            ensure!(operation_row.frozen_inventory_state=="known" && operation_row.frozen_checkpoint_count.is_some(),"whole operation proof inventory unknown");
            let mut after:Option<String>=None;let mut discovered=0i64;
            loop {
                let ids=checkpoint_ids_page(store,operation,after.as_deref()).await?;
                if ids.is_empty(){break;}
                for id in &ids{prepare(store,id).await?;discovered+=1;}
                after=ids.last().cloned();
            }
            if Some(discovered)!=operation_row.frozen_checkpoint_count { return Err(compaction::FrozenProofReadinessInvalidated.into()); }
            super::compaction_runner::validate_publication_checkpoint_graph(store,checkpoint).await?;
            let expected=operation_row.frozen_checkpoint_count;
            store.run_serialized_write(||async{
                let tx=store.connection.begin().await?;
                if let Some(origin)=&origin{origin.validate_in(&tx,true).await?;}
                let current_bound=projection::Entity::find_by_id(operation).one(&tx).await?;
                ensure!(current_bound==bound,"whole proof readiness origin binding changed");
                let current_context=ctx::Entity::find_by_id(&operation_row.owner).one(&tx).await?;
                ensure!(current_context.as_ref().is_some_and(|current|current.workspace_id==context.workspace_id && current.thread_id==context.thread_id),"whole proof readiness context changed");
                let changed=tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "UPDATE compaction_operation SET frozen_proof_state='prepared' WHERE id=? AND status='running' AND snapshot=? AND frozen_publication_contract='native_frozen' AND frozen_accounting_mode='live_known' AND frozen_inventory_state='known' AND frozen_checkpoint_count=? AND frozen_prepared_checkpoint_count=? AND frozen_proof_state IN ('pending','prepared') \
                     AND EXISTS(SELECT 1 FROM compaction_checkpoint p WHERE p.id=? AND p.operation_id=compaction_operation.id AND p.identity_sha256=? AND p.frozen_accounting_state='prepared')",
                    [operation.into(),operation_row.snapshot.clone().into(),expected.into(),expected.into(),checkpoint.into(),cp_identity.clone().into()])).await?.rows_affected();
                if changed!=1 {
                    let current=op::Entity::find_by_id(operation).one(&tx).await?;
                    if current.as_ref().is_some_and(|current|current.status=="running" && current.snapshot==operation_row.snapshot && current.frozen_accounting_mode=="live_known" && current.frozen_inventory_state=="known" && current.frozen_checkpoint_count!=expected) { return Err(compaction::FrozenProofReadinessInvalidated.into()); }
                    return Err(ProofIntegrityError("whole operation proof readiness changed").into());
                }
                tx.commit().await?;Ok(())
            }).await?;
            expected
        }else{None};
        Ok(total)
    }.await;
    match result {
        Ok(total) => Ok(Publication {
            operation: operation.into(),
            contract: operation_row.frozen_publication_contract,
            snapshot: operation_row.snapshot,
            execution_turn: operation_row.execution_turn,
            workspace: context.workspace_id,
            thread: context.thread_id,
            projection: bound,
            checkpoint: checkpoint.into(),
            identity: cp_identity,
            total,
            origin,
        }),
        Err(error) => match origin {
            Some(origin) => origin.complete(Err(error)).await,
            None => Err(error),
        },
    }
}

/// Keyset discovery returns only numeric row identity/size. Long admitted TEXT
/// keys are joined outside capacity from bounded UTF-8 byte fragments.
pub(super) async fn checkpoint_ids_page(
    store: &CrudStore,
    operation: &str,
    after: Option<&str>,
) -> Result<Vec<String>> {
    let discovery = match after {
        None => Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT rowid AS rid,length(CAST(id AS BLOB)) AS bytes FROM compaction_checkpoint WHERE operation_id=? ORDER BY id LIMIT 128",
            [operation.into()],
        ),
        Some(after) => Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT rowid AS rid,length(CAST(id AS BLOB)) AS bytes FROM compaction_checkpoint WHERE operation_id=? AND id>? ORDER BY id LIMIT 128",
            [operation.into(), after.into()],
        ),
    };
    let sizes = store.connection.query_all_raw(discovery).await?;
    let mut ids = Vec::new();
    let mut page_bytes = 0usize;
    for row in sizes {
        let rid: i64 = row.try_get("", "rid")?;
        let size = usize::try_from(row.try_get::<i64>("", "bytes")?)?;
        if !ids.is_empty() && page_bytes.saturating_add(size) > compaction::SOURCE_PAGE_BYTES {
            break;
        }
        let mut bytes = Vec::new();
        let mut offset = 0usize;
        while offset < size {
            let length = (size - offset).min(compaction::SOURCE_PAGE_BYTES);
            let mut values: Vec<sea_orm::Value> = vec![
                (i64::try_from(offset)? + 1).into(),
                i64::try_from(length)?.into(),
                rid.into(),
                operation.into(),
                i64::try_from(size)?.into(),
            ];
            let lower = if let Some(after) = after {
                values.push(after.into());
                " AND id>?"
            } else {
                ""
            };
            let fragment=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                format!("SELECT substr(CAST(id AS BLOB),?,?) AS fragment FROM compaction_checkpoint WHERE rowid=? AND operation_id=? AND length(CAST(id AS BLOB))=?{lower}"),values)).await?.ok_or_else(||anyhow::anyhow!("checkpoint discovery changed"))?.try_get::<Vec<u8>>("","fragment")?;
            ensure!(
                fragment.len() == length,
                "checkpoint key fragment incomplete"
            );
            bytes.extend(fragment);
            offset += length;
        }
        let id = String::from_utf8(bytes)?;
        ensure!(
            store
                .connection
                .query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "SELECT 1 FROM compaction_checkpoint WHERE rowid=? AND operation_id=? AND id=?",
                    [rid.into(), operation.into(), id.clone().into()]
                ))
                .await?
                .is_some(),
            "checkpoint key changed"
        );
        page_bytes = page_bytes.saturating_add(size);
        ids.push(id);
    }
    Ok(ids)
}

#[cfg(test)]
mod framing_tests {
    use super::*;
    #[test]
    fn auxiliary_hashes_keep_domains_nullable_tags_and_utf8_byte_lengths() {
        // Literal format vectors distinguish zero-count domain tags without
        // deriving their expected framing from the helpers under test.
        assert_eq!(
            aliases_digest(&[]),
            hex::encode(Sha256::digest(b"p73/aliases/v1\0\0\0\0\0\0\0\0"))
        );
        assert_eq!(
            evidence_digest(&[]),
            hex::encode(Sha256::digest(b"p73/evidence/v1\0\0\0\0\0\0\0\0"))
        );
        assert_ne!(aliases_digest(&[]), coverage_digest(&[]));
        let mut h = Sha256::new();
        text(&mut h, "α");
        assert_eq!(
            hex::encode(h.finalize()),
            hex::encode(Sha256::digest(b"\0\0\0\0\0\0\0\x02\xce\xb1"))
        );
        let mut absent = Sha256::new();
        optional(&mut absent, None);
        let mut empty = Sha256::new();
        optional(&mut empty, Some(""));
        assert_eq!(
            hex::encode(absent.finalize()),
            hex::encode(Sha256::digest([0]))
        );
        assert_eq!(
            hex::encode(empty.finalize()),
            hex::encode(Sha256::digest([1, 0, 0, 0, 0, 0, 0, 0, 0]))
        );
    }
}
