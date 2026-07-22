use crate::message::MessageProcessor;
use anyhow::{Context, Result, bail};
use migration::stable_skill_id::{
    LEGACY_RELATION_TABLES, LEGACY_SKILL_AUDIT_EVENT_TABLE, LEGACY_SKILL_DEPENDENCY_SNAPSHOT_TABLE,
    LEGACY_SKILL_INSTALLATION_TABLE, LEGACY_SKILL_WORKSPACE_POLICY_TABLE,
    LEGACY_TURN_SKILL_BINDING_TABLE,
};
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_COMPLETE, ProjectionMetaRecord, find_projection_meta,
    upsert_projection_meta,
};
use pioneer_protocol::SkillId;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ConnectionTrait, DatabaseConnection, Statement, TransactionTrait};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{info, warn};

const BATCH_SIZE: u64 = 32;
const JSON_BATCH_SIZE: u64 = 8;
const BATCH_PAUSE: Duration = Duration::from_millis(25);
const BUSY_PAUSE: Duration = Duration::from_millis(100);
const STABLE_SKILL_ID_BACKFILL_KEY: &str = "stable_skill_id_backfill";
const STABLE_SKILL_ID_BACKFILL_VERSION: i64 = 1;
const BUNDLED_MANIFEST_BYTES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../resources/skills/bundled-system-skills.toml"
));

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StableSkillIdBackfillSummary {
    pub(crate) installations: u64,
    pub(crate) policies: u64,
    pub(crate) history_rows: u64,
    pub(crate) runtime_snapshot_fields: u64,
    pub(crate) turn_item_payloads: u64,
    pub(crate) turn_event_payloads: u64,
    pub(crate) turn_attempt_payloads: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BatchOutcome {
    processed: u64,
    complete: bool,
}

impl BatchOutcome {
    const fn progressed(processed: u64) -> Self {
        Self {
            processed,
            complete: false,
        }
    }

    const fn complete() -> Self {
        Self {
            processed: 0,
            complete: true,
        }
    }

    const fn completed(processed: u64) -> Self {
        Self {
            processed,
            complete: true,
        }
    }
}

pub(super) async fn run(crud_store: &CrudStore, message_processor: &MessageProcessor) {
    let db = crud_store.database_connection();
    match backfill_is_current(&db).await {
        Ok(true) => return,
        Ok(false) => {}
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "failed to read Stable SkillId backfill status"
            );
            return;
        }
    }

    let started_at = now_datetime();
    info!("stable SkillId background backfill started");
    match backfill_all(crud_store).await {
        Ok(summary) => {
            info!(
                installations = summary.installations,
                policies = summary.policies,
                history_rows = summary.history_rows,
                runtime_snapshot_fields = summary.runtime_snapshot_fields,
                turn_item_payloads = summary.turn_item_payloads,
                turn_event_payloads = summary.turn_event_payloads,
                turn_attempt_payloads = summary.turn_attempt_payloads,
                "stable SkillId database backfill completed"
            );
            let storage_summary = message_processor.run_skill_storage_startup_pass().await;
            if storage_summary.failed != 0 {
                warn!(
                    failed = storage_summary.failed,
                    "stable SkillId migration remains incomplete after filesystem failures"
                );
                return;
            }
            if let Err(error) = mark_backfill_complete(&db, started_at).await {
                warn!(
                    error = %format!("{error:#}"),
                    "failed to mark Stable SkillId migration complete"
                );
                return;
            }
            info!("stable SkillId migration completed");
        }
        Err(error) => warn!(
            error = %format!("{error:#}"),
            "stable SkillId background backfill stopped; remaining database and filesystem work will retry on the next Gateway start"
        ),
    }
}

async fn backfill_is_current(db: &DatabaseConnection) -> Result<bool> {
    let Some(meta) = find_projection_meta(db, STABLE_SKILL_ID_BACKFILL_KEY).await? else {
        return Ok(false);
    };

    Ok(meta.projection_version == STABLE_SKILL_ID_BACKFILL_VERSION
        && meta.status == PROJECTION_META_STATUS_COMPLETE)
}

async fn mark_backfill_complete(
    db: &DatabaseConnection,
    started_at: DateTimeWithTimeZone,
) -> Result<()> {
    let completed_at = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: STABLE_SKILL_ID_BACKFILL_KEY.to_owned(),
            projection_version: STABLE_SKILL_ID_BACKFILL_VERSION,
            status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(started_at),
            backfilled_at: Some(completed_at),
            created_at: completed_at,
            updated_at: completed_at,
        },
    )
    .await
    .context("failed to persist Stable SkillId backfill status")
}

fn now_datetime() -> DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}

pub(crate) async fn backfill_all(crud_store: &CrudStore) -> Result<StableSkillIdBackfillSummary> {
    let mut summary = StableSkillIdBackfillSummary::default();
    let db = crud_store.database_connection();
    if !legacy_relation_backfill_pending(&db).await? {
        return Ok(summary);
    }

    summary.installations = backfill_installations(crud_store).await?;

    let resolver = Arc::new(Mutex::new(IdentityResolver::load(&db).await?));

    // The legacy policy table is the implicit JSON-phase marker. It is part of the schema
    // migration on every upgraded database, remains present if JSON work is interrupted, and is
    // dropped immediately after JSON and policy rows finish. No cursor or progress row is needed.
    if table_exists(&db, LEGACY_SKILL_WORKSPACE_POLICY_TABLE).await? {
        for surface in [
            JsonSurface::RuntimeSnapshot,
            JsonSurface::TurnItem,
            JsonSurface::TurnEvent,
            JsonSurface::TurnAttempt,
        ] {
            let migrated = backfill_json(crud_store, resolver.clone(), surface).await?;
            match surface {
                JsonSurface::RuntimeSnapshot => summary.runtime_snapshot_fields = migrated,
                JsonSurface::TurnItem => summary.turn_item_payloads = migrated,
                JsonSurface::TurnEvent => summary.turn_event_payloads = migrated,
                JsonSurface::TurnAttempt => summary.turn_attempt_payloads = migrated,
            }
        }
        summary.policies = backfill_policies(crud_store, resolver.clone()).await?;
    }

    for surface in [
        HistorySurface::Binding,
        HistorySurface::Audit,
        HistorySurface::Dependency,
    ] {
        summary.history_rows = summary
            .history_rows
            .saturating_add(backfill_history(crud_store, resolver.clone(), surface).await?);
    }

    Ok(summary)
}

async fn backfill_installations(crud_store: &CrudStore) -> Result<u64> {
    let mut migrated = 0_u64;
    loop {
        let db = crud_store.database_connection();
        let outcome = crud_store
            .try_run_low_priority_write(|| {
                let db = db.clone();
                async move { migrate_installation_batch(&db).await }
            })
            .await?;
        let Some(outcome) = outcome else {
            tokio::time::sleep(BUSY_PAUSE).await;
            continue;
        };
        migrated = migrated.saturating_add(outcome.processed);
        if outcome.complete {
            return Ok(migrated);
        }
        tokio::time::sleep(BATCH_PAUSE).await;
    }
}

async fn backfill_policies(
    crud_store: &CrudStore,
    resolver: Arc<Mutex<IdentityResolver>>,
) -> Result<u64> {
    let mut migrated = 0_u64;
    loop {
        let db = crud_store.database_connection();
        let outcome = crud_store
            .try_run_low_priority_write(|| {
                let db = db.clone();
                let resolver = resolver.clone();
                async move {
                    let resolver = resolver.lock().await;
                    migrate_policy_batch(&db, &resolver).await
                }
            })
            .await?;
        let Some(outcome) = outcome else {
            tokio::time::sleep(BUSY_PAUSE).await;
            continue;
        };
        migrated = migrated.saturating_add(outcome.processed);
        if outcome.complete {
            return Ok(migrated);
        }
        tokio::time::sleep(BATCH_PAUSE).await;
    }
}

async fn backfill_history(
    crud_store: &CrudStore,
    resolver: Arc<Mutex<IdentityResolver>>,
    surface: HistorySurface,
) -> Result<u64> {
    let mut migrated = 0_u64;
    loop {
        let db = crud_store.database_connection();
        let outcome = crud_store
            .try_run_low_priority_write(|| {
                let db = db.clone();
                let resolver = resolver.clone();
                async move {
                    let mut resolver = resolver.lock().await;
                    migrate_history_batch(&db, &mut resolver, surface).await
                }
            })
            .await?;
        let Some(outcome) = outcome else {
            tokio::time::sleep(BUSY_PAUSE).await;
            continue;
        };
        migrated = migrated.saturating_add(outcome.processed);
        if outcome.complete {
            return Ok(migrated);
        }
        tokio::time::sleep(BATCH_PAUSE).await;
    }
}

async fn backfill_json(
    crud_store: &CrudStore,
    resolver: Arc<Mutex<IdentityResolver>>,
    surface: JsonSurface,
) -> Result<u64> {
    let mut migrated = 0_u64;
    loop {
        let db = crud_store.database_connection();
        // Candidate discovery can scan compressed views. Keep that read outside the shared
        // write coordinator; only the bounded update transaction below may occupy it.
        let batch = Arc::new(load_json_batch(&db, surface).await?);
        if batch.is_empty() {
            return Ok(migrated);
        }

        let processed = loop {
            let outcome = crud_store
                .try_run_low_priority_write(|| {
                    let batch = batch.clone();
                    let db = db.clone();
                    let resolver = resolver.clone();
                    async move {
                        let mut resolver = resolver.lock().await;
                        migrate_json_batch(&db, &mut resolver, batch.as_ref()).await
                    }
                })
                .await?;
            if let Some(processed) = outcome {
                break processed;
            }
            tokio::time::sleep(BUSY_PAUSE).await;
        };
        migrated = migrated.saturating_add(processed);
        tokio::time::sleep(BATCH_PAUSE).await;
    }
}

async fn migrate_installation_batch(db: &DatabaseConnection) -> Result<BatchOutcome> {
    let transaction = db.begin().await?;
    if !table_exists(&transaction, LEGACY_SKILL_INSTALLATION_TABLE).await? {
        transaction.commit().await?;
        return Ok(BatchOutcome::complete());
    }

    let rows = transaction
        .query_all_raw(Statement::from_string(
            transaction.get_database_backend(),
            format!(
                "SELECT id FROM {LEGACY_SKILL_INSTALLATION_TABLE} ORDER BY id LIMIT {BATCH_SIZE}"
            ),
        ))
        .await?;
    if rows.is_empty() {
        drop_table(&transaction, LEGACY_SKILL_INSTALLATION_TABLE).await?;
        transaction.commit().await?;
        return Ok(BatchOutcome::complete());
    }

    for row in &rows {
        let id: String = row.try_get("", "id")?;
        validate_skill_id(id.as_str(), "legacy skill_installation.id")?;
        let inserted = transaction
            .execute_raw(Statement::from_sql_and_values(
                transaction.get_database_backend(),
                format!(
                    r#"
                    INSERT INTO skill_installation (
                        id, owner, slug, version, source_kind, scope_key, source_ref,
                        install_path, trust_level, fingerprint, created_at, updated_at
                    )
                    SELECT
                        id,
                        CASE WHEN instr(slug, '/') > 1
                             THEN substr(slug, 1, instr(slug, '/') - 1)
                             ELSE NULL END,
                        CASE WHEN instr(slug, '/') > 0
                             THEN substr(slug, instr(slug, '/') + 1)
                             ELSE slug END,
                        version, source_kind, scope_key, source_ref, install_path,
                        trust_level, fingerprint, created_at, updated_at
                    FROM {LEGACY_SKILL_INSTALLATION_TABLE}
                    WHERE id = ?
                    ON CONFLICT(id) DO NOTHING
                    "#
                ),
                [id.clone().into()],
            ))
            .await?;
        match inserted.rows_affected() {
            1 => {}
            0 => {
                if !row_exists(&transaction, "skill_installation", id.as_str()).await? {
                    bail!(
                        "stable SkillId backfill insert for `{LEGACY_SKILL_INSTALLATION_TABLE}` row `{id}` did not create a final installation"
                    );
                }
            }
            rows_affected => bail!(
                "stable SkillId backfill insert for `{LEGACY_SKILL_INSTALLATION_TABLE}` row `{id}` affected {rows_affected} rows"
            ),
        }
        delete_legacy_row(&transaction, LEGACY_SKILL_INSTALLATION_TABLE, id.as_str()).await?;
    }

    let processed = rows.len() as u64;
    let complete = !table_has_rows(&transaction, LEGACY_SKILL_INSTALLATION_TABLE).await?;
    if complete {
        drop_table(&transaction, LEGACY_SKILL_INSTALLATION_TABLE).await?;
    }
    transaction.commit().await?;
    Ok(if complete {
        BatchOutcome::completed(processed)
    } else {
        BatchOutcome::progressed(processed)
    })
}

async fn migrate_policy_batch(
    db: &DatabaseConnection,
    resolver: &IdentityResolver,
) -> Result<BatchOutcome> {
    let transaction = db.begin().await?;
    if !table_exists(&transaction, LEGACY_SKILL_WORKSPACE_POLICY_TABLE).await? {
        transaction.commit().await?;
        return Ok(BatchOutcome::complete());
    }

    let rows = transaction
        .query_all_raw(Statement::from_string(
            transaction.get_database_backend(),
            format!(
                r#"
                SELECT id, workspace_id, skill_slug, source_kind
                FROM {LEGACY_SKILL_WORKSPACE_POLICY_TABLE}
                ORDER BY updated_at DESC, id DESC
                LIMIT {BATCH_SIZE}
                "#
            ),
        ))
        .await?;
    if rows.is_empty() {
        drop_table(&transaction, LEGACY_SKILL_WORKSPACE_POLICY_TABLE).await?;
        transaction.commit().await?;
        return Ok(BatchOutcome::complete());
    }

    for row in &rows {
        let id: String = row.try_get("", "id")?;
        let workspace_id: String = row.try_get("", "workspace_id")?;
        let legacy_locator: String = row.try_get("", "skill_slug")?;
        let source_kind: String = row.try_get("", "source_kind")?;
        if let Some(skill_id) = resolver.resolve_policy_identity(
            workspace_id.as_str(),
            legacy_locator.as_str(),
            source_kind.as_str(),
        ) {
            transaction
                .execute_raw(Statement::from_sql_and_values(
                    transaction.get_database_backend(),
                    format!(
                        r#"
                        INSERT INTO skill_workspace_policy (
                            id, workspace_id, skill_id, enabled,
                            allow_implicit_invocation, created_at, updated_at
                        )
                        SELECT id, workspace_id, ?, enabled,
                               allow_implicit_invocation, created_at, updated_at
                        FROM {LEGACY_SKILL_WORKSPACE_POLICY_TABLE}
                        WHERE id = ?
                        ON CONFLICT(workspace_id, skill_id) DO NOTHING
                        "#
                    ),
                    [skill_id.to_string().into(), id.clone().into()],
                ))
                .await?;
        }
        delete_legacy_row(
            &transaction,
            LEGACY_SKILL_WORKSPACE_POLICY_TABLE,
            id.as_str(),
        )
        .await?;
    }

    let processed = rows.len() as u64;
    let complete = !table_has_rows(&transaction, LEGACY_SKILL_WORKSPACE_POLICY_TABLE).await?;
    if complete {
        drop_table(&transaction, LEGACY_SKILL_WORKSPACE_POLICY_TABLE).await?;
    }
    transaction.commit().await?;
    Ok(if complete {
        BatchOutcome::completed(processed)
    } else {
        BatchOutcome::progressed(processed)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistorySurface {
    Binding,
    Audit,
    Dependency,
}

impl HistorySurface {
    const fn legacy_table(self) -> &'static str {
        match self {
            Self::Binding => LEGACY_TURN_SKILL_BINDING_TABLE,
            Self::Audit => LEGACY_SKILL_AUDIT_EVENT_TABLE,
            Self::Dependency => LEGACY_SKILL_DEPENDENCY_SNAPSHOT_TABLE,
        }
    }

    const fn target_table(self) -> &'static str {
        match self {
            Self::Binding => "turn_skill_binding",
            Self::Audit => "skill_audit_event",
            Self::Dependency => "skill_dependency_snapshot",
        }
    }
}

async fn migrate_history_batch(
    db: &DatabaseConnection,
    resolver: &mut IdentityResolver,
    surface: HistorySurface,
) -> Result<BatchOutcome> {
    let transaction = db.begin().await?;
    let legacy_table = surface.legacy_table();
    if !table_exists(&transaction, legacy_table).await? {
        transaction.commit().await?;
        return Ok(BatchOutcome::complete());
    }

    let rows = transaction
        .query_all_raw(Statement::from_string(
            transaction.get_database_backend(),
            format!(
                r#"
                SELECT legacy.id, legacy.turn_id, legacy.skill_slug, legacy.source_kind,
                       w.id AS workspace_id
                FROM {legacy_table} legacy
                LEFT JOIN turn t ON t.id = legacy.turn_id
                LEFT JOIN thread th ON th.id = t.thread_id
                LEFT JOIN workspace w ON w.id = th.workspace_id
                ORDER BY legacy.id
                LIMIT {BATCH_SIZE}
                "#
            ),
        ))
        .await?;
    if rows.is_empty() {
        drop_table(&transaction, legacy_table).await?;
        transaction.commit().await?;
        return Ok(BatchOutcome::complete());
    }

    for row in &rows {
        let id: String = row.try_get("", "id")?;
        let turn_id: Option<String> = row.try_get("", "turn_id")?;
        let legacy_locator: String = row.try_get("", "skill_slug")?;
        let source_kind: String = row.try_get("", "source_kind")?;
        let workspace_id: Option<String> = row.try_get("", "workspace_id")?;
        let skill_id = resolver.resolve_history_identity(
            turn_id.as_deref(),
            workspace_id.as_deref(),
            legacy_locator.as_str(),
            source_kind.as_str(),
        );
        let (owner, slug) = split_legacy_locator(legacy_locator.as_str());
        copy_history_row(
            &transaction,
            surface,
            id.as_str(),
            &skill_id,
            owner.as_deref(),
            slug.as_str(),
        )
        .await?;
        delete_legacy_row(&transaction, legacy_table, id.as_str()).await?;
    }

    let processed = rows.len() as u64;
    let complete = !table_has_rows(&transaction, legacy_table).await?;
    if complete {
        drop_table(&transaction, legacy_table).await?;
    }
    transaction.commit().await?;
    Ok(if complete {
        BatchOutcome::completed(processed)
    } else {
        BatchOutcome::progressed(processed)
    })
}

async fn copy_history_row(
    db: &impl ConnectionTrait,
    surface: HistorySurface,
    row_id: &str,
    skill_id: &SkillId,
    owner: Option<&str>,
    slug: &str,
) -> Result<()> {
    let source = surface.legacy_table();
    let target = surface.target_table();
    let sql = match surface {
        HistorySurface::Binding => format!(
            r#"
            INSERT INTO {target} (
                id, turn_id, skill_id, skill_owner, skill_slug, skill_version,
                fingerprint, source_kind, resolved_reason, created_at
            )
            SELECT id, turn_id, ?, ?, ?, skill_version, fingerprint, source_kind,
                   resolved_reason, created_at
            FROM {source} WHERE id = ?
            "#
        ),
        HistorySurface::Audit => format!(
            r#"
            INSERT INTO {target} (
                id, turn_id, skill_id, skill_owner, skill_slug, source_kind,
                action, decision, reason_code, details_json, created_at
            )
            SELECT id, turn_id, ?, ?, ?, source_kind, action, decision,
                   reason_code, details_json, created_at
            FROM {source} WHERE id = ?
            "#
        ),
        HistorySurface::Dependency => format!(
            r#"
            INSERT INTO {target} (
                id, turn_id, skill_id, skill_owner, skill_slug, source_kind,
                diagnostics_json, created_at
            )
            SELECT id, turn_id, ?, ?, ?, source_kind, diagnostics_json, created_at
            FROM {source} WHERE id = ?
            "#
        ),
    };
    let inserted = db
        .execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            vec![
                skill_id.to_string().into(),
                owner.map(str::to_owned).into(),
                slug.to_owned().into(),
                row_id.to_owned().into(),
            ],
        ))
        .await?;
    require_one_row(inserted.rows_affected(), source, row_id, "insert")
}

async fn legacy_relation_backfill_pending(db: &impl ConnectionTrait) -> Result<bool> {
    for (_, legacy_table) in LEGACY_RELATION_TABLES {
        if table_exists(db, legacy_table).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn table_exists(db: &impl ConnectionTrait, table: &str) -> Result<bool> {
    Ok(db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            [table.to_owned().into()],
        ))
        .await?
        .is_some())
}

async fn table_has_rows(db: &impl ConnectionTrait, table: &str) -> Result<bool> {
    Ok(db
        .query_one_raw(Statement::from_string(
            db.get_database_backend(),
            format!("SELECT 1 FROM {table} LIMIT 1"),
        ))
        .await?
        .is_some())
}

async fn row_exists(db: &impl ConnectionTrait, table: &str, row_id: &str) -> Result<bool> {
    Ok(db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!("SELECT 1 FROM {table} WHERE id = ?"),
            [row_id.to_owned().into()],
        ))
        .await?
        .is_some())
}

async fn drop_table(db: &impl ConnectionTrait, table: &str) -> Result<()> {
    db.execute_unprepared(format!("DROP TABLE {table}").as_str())
        .await?;
    Ok(())
}

async fn delete_legacy_row(db: &impl ConnectionTrait, table: &str, row_id: &str) -> Result<()> {
    let deleted = db
        .execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!("DELETE FROM {table} WHERE id = ?"),
            [row_id.to_owned().into()],
        ))
        .await?;
    require_one_row(deleted.rows_affected(), table, row_id, "delete")
}

fn require_one_row(rows_affected: u64, table: &str, row_id: &str, action: &str) -> Result<()> {
    if rows_affected != 1 {
        bail!(
            "stable SkillId backfill {action} for `{table}` row `{row_id}` affected {rows_affected} rows"
        );
    }
    Ok(())
}

fn validate_skill_id(value: &str, field: &str) -> Result<SkillId> {
    SkillId::new(value.to_owned()).with_context(|| format!("invalid {field} `{value}`"))
}

fn split_legacy_locator(locator: &str) -> (Option<String>, String) {
    let Some((owner, slug)) = locator.split_once('/') else {
        return (None, locator.to_owned());
    };
    let owner = (!owner.is_empty()).then(|| owner.to_owned());
    (owner, slug.to_owned())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundledManifest {
    version: u32,
    skills: Vec<BundledManifestEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundledManifestEntry {
    skill_id: String,
    owner: String,
    slug: String,
    resource_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SkillCandidate {
    skill_id: SkillId,
    legacy_locator: String,
    source_kind: String,
    scope_key: Option<String>,
    bundled: bool,
}

impl SkillCandidate {
    fn visible_in_workspace(&self, workspace_id: &str) -> bool {
        if self.bundled || self.source_kind == "system" {
            return true;
        }
        self.scope_key.as_deref() == Some(workspace_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum HistoricalContext {
    Workspace(String),
    Turn(String),
    Global,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HistoricalGroup {
    context: HistoricalContext,
    legacy_locator: String,
    source_kind: String,
}

#[derive(Debug)]
struct IdentityResolver {
    candidates: Vec<SkillCandidate>,
    historical_ids: HashMap<HistoricalGroup, SkillId>,
}

impl IdentityResolver {
    async fn load(db: &DatabaseConnection) -> Result<Self> {
        let mut candidates = load_installation_candidates(db).await?;
        candidates.extend(load_bundled_candidates()?);
        ensure_unique_candidate_ids(candidates.as_slice())?;
        let mut resolver = Self {
            candidates,
            historical_ids: HashMap::new(),
        };
        resolver.load_persisted_historical_ids(db).await?;
        Ok(resolver)
    }

    fn resolve_policy_identity(
        &self,
        workspace_id: &str,
        legacy_locator: &str,
        source_kind: &str,
    ) -> Option<SkillId> {
        exact_candidate(self.candidates.iter().filter(|candidate| {
            candidate.source_kind == source_kind
                && candidate.legacy_locator == legacy_locator
                && candidate.visible_in_workspace(workspace_id)
        }))
    }

    fn resolve_workspace_history_identity(
        &mut self,
        workspace_id: &str,
        legacy_locator: &str,
        source_kind: &str,
    ) -> SkillId {
        let context = HistoricalContext::Workspace(workspace_id.to_owned());
        let group = HistoricalGroup {
            context: context.clone(),
            legacy_locator: legacy_locator.to_owned(),
            source_kind: source_kind.to_owned(),
        };
        if let Some(skill_id) = self.historical_ids.get(&group) {
            return skill_id.clone();
        }

        let current = self.resolve_policy_identity(workspace_id, legacy_locator, source_kind);
        if let Some(skill_id) = current {
            return skill_id;
        }
        self.resolve_historical_group(context, legacy_locator, source_kind)
    }

    fn resolve_history_identity(
        &mut self,
        turn_id: Option<&str>,
        workspace_id: Option<&str>,
        legacy_locator: &str,
        source_kind: &str,
    ) -> SkillId {
        let context = match (workspace_id, turn_id) {
            (Some(workspace_id), _) => HistoricalContext::Workspace(workspace_id.to_owned()),
            (None, Some(turn_id)) => HistoricalContext::Turn(turn_id.to_owned()),
            (None, None) => HistoricalContext::Global,
        };
        let group = HistoricalGroup {
            context: context.clone(),
            legacy_locator: legacy_locator.to_owned(),
            source_kind: source_kind.to_owned(),
        };
        if let Some(skill_id) = self.historical_ids.get(&group) {
            return skill_id.clone();
        }

        let current = if let Some(workspace_id) = workspace_id {
            exact_candidate(self.candidates.iter().filter(|candidate| {
                candidate.source_kind == source_kind
                    && candidate.legacy_locator == legacy_locator
                    && candidate.visible_in_workspace(workspace_id)
            }))
        } else if turn_id.is_none() {
            exact_candidate(self.candidates.iter().filter(|candidate| {
                candidate.source_kind == source_kind && candidate.legacy_locator == legacy_locator
            }))
        } else {
            None
        };
        if let Some(skill_id) = current {
            return skill_id;
        }
        self.resolve_historical_group(context, legacy_locator, source_kind)
    }

    fn resolve_historical_group(
        &mut self,
        context: HistoricalContext,
        legacy_locator: &str,
        source_kind: &str,
    ) -> SkillId {
        let group = HistoricalGroup {
            context,
            legacy_locator: legacy_locator.to_owned(),
            source_kind: source_kind.to_owned(),
        };
        if let Some(skill_id) = self.historical_ids.get(&group) {
            return skill_id.clone();
        }

        let skill_id = self.deterministic_historical_skill_id(&group);
        self.historical_ids.insert(group, skill_id.clone());
        skill_id
    }

    fn deterministic_historical_skill_id(&self, group: &HistoricalGroup) -> SkillId {
        for collision_nonce in 0_u64.. {
            let mut hasher = Sha256::new();
            hasher.update(b"pioneer:stable-skill-id:historical:v1\0");
            match &group.context {
                HistoricalContext::Workspace(workspace_id) => {
                    hasher.update(b"workspace\0");
                    hash_length_prefixed(&mut hasher, workspace_id);
                }
                HistoricalContext::Turn(turn_id) => {
                    hasher.update(b"turn\0");
                    hash_length_prefixed(&mut hasher, turn_id);
                }
                HistoricalContext::Global => hasher.update(b"global\0"),
            }
            hash_length_prefixed(&mut hasher, group.legacy_locator.as_str());
            hash_length_prefixed(&mut hasher, group.source_kind.as_str());
            hasher.update(collision_nonce.to_be_bytes());
            let digest = hasher.finalize();
            let skill_id = skill_id_from_digest(digest.as_ref());
            let collides_with_candidate = self
                .candidates
                .iter()
                .any(|candidate| candidate.skill_id == skill_id);
            let collides_with_history = self
                .historical_ids
                .values()
                .any(|existing| existing == &skill_id);
            if !collides_with_candidate && !collides_with_history {
                return skill_id;
            }
        }
        unreachable!("u64 collision nonce space cannot be exhausted")
    }

    async fn load_persisted_historical_ids(&mut self, db: &DatabaseConnection) -> Result<()> {
        for surface in [
            HistorySurface::Binding,
            HistorySurface::Audit,
            HistorySurface::Dependency,
        ] {
            let rows = db
                .query_all_raw(Statement::from_string(
                    db.get_database_backend(),
                    format!(
                        r#"
                        SELECT h.turn_id, h.skill_id, h.skill_owner, h.skill_slug,
                               h.source_kind, w.id AS workspace_id
                        FROM {} h
                        LEFT JOIN turn t ON t.id = h.turn_id
                        LEFT JOIN thread th ON th.id = t.thread_id
                        LEFT JOIN workspace w ON w.id = th.workspace_id
                        "#,
                        surface.target_table()
                    ),
                ))
                .await?;
            for row in rows {
                let turn_id: Option<String> = row.try_get("", "turn_id")?;
                let skill_id_raw: String = row.try_get("", "skill_id")?;
                let owner: Option<String> = row.try_get("", "skill_owner")?;
                let slug: String = row.try_get("", "skill_slug")?;
                let source_kind: String = row.try_get("", "source_kind")?;
                let workspace_id: Option<String> = row.try_get("", "workspace_id")?;
                let skill_id =
                    validate_skill_id(skill_id_raw.as_str(), "persisted historical skill_id")?;
                let legacy_locator = owner.map(|owner| format!("{owner}/{slug}")).unwrap_or(slug);

                let current = match workspace_id.as_deref() {
                    Some(workspace_id) => self.resolve_policy_identity(
                        workspace_id,
                        legacy_locator.as_str(),
                        source_kind.as_str(),
                    ),
                    None if turn_id.is_none() => {
                        exact_candidate(self.candidates.iter().filter(|candidate| {
                            candidate.source_kind == source_kind
                                && candidate.legacy_locator == legacy_locator
                        }))
                    }
                    None => None,
                };
                if current.as_ref() == Some(&skill_id) {
                    continue;
                }

                let context = match (workspace_id.as_deref(), turn_id.as_deref()) {
                    (Some(workspace_id), _) => {
                        HistoricalContext::Workspace(workspace_id.to_owned())
                    }
                    (None, Some(turn_id)) => HistoricalContext::Turn(turn_id.to_owned()),
                    (None, None) => HistoricalContext::Global,
                };
                let group = HistoricalGroup {
                    context,
                    legacy_locator,
                    source_kind,
                };
                match self.historical_ids.get(&group) {
                    Some(existing) if existing != &skill_id => bail!(
                        "conflicting persisted SkillIds `{existing}` and `{skill_id}` for one legacy historical group"
                    ),
                    Some(_) => {}
                    None => {
                        self.historical_ids.insert(group, skill_id);
                    }
                }
            }
        }
        Ok(())
    }
}

fn hash_length_prefixed(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn skill_id_from_digest(digest: &[u8]) -> SkillId {
    const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    let mut value_bytes = [0_u8; 16];
    value_bytes[1..].copy_from_slice(&digest[..15]);
    let mut value = u128::from_be_bytes(value_bytes);
    let mut encoded = [b'0'; 21];
    for byte in encoded.iter_mut().rev() {
        *byte = ALPHABET[(value % ALPHABET.len() as u128) as usize];
        value /= ALPHABET.len() as u128;
    }
    debug_assert_eq!(value, 0);

    let encoded = String::from_utf8(encoded.to_vec())
        .expect("the historical SkillId alphabet must remain ASCII");
    SkillId::new(encoded).expect("the historical digest encoder must produce a valid SkillId")
}

async fn load_installation_candidates(db: &DatabaseConnection) -> Result<Vec<SkillCandidate>> {
    db.query_all_raw(Statement::from_string(
        db.get_database_backend(),
        "SELECT id, owner, slug, source_kind, scope_key FROM skill_installation ORDER BY id"
            .to_owned(),
    ))
    .await?
    .into_iter()
    .map(|row| {
        let skill_id_raw: String = row.try_get("", "id")?;
        let skill_id = validate_skill_id(skill_id_raw.as_str(), "skill_installation.id")?;
        let owner: Option<String> = row.try_get("", "owner")?;
        let slug: String = row.try_get("", "slug")?;
        let legacy_locator = owner
            .as_ref()
            .map(|owner| format!("{owner}/{slug}"))
            .unwrap_or_else(|| slug.clone());
        Ok(SkillCandidate {
            skill_id,
            legacy_locator,
            source_kind: row.try_get("", "source_kind")?,
            scope_key: Some(row.try_get("", "scope_key")?),
            bundled: false,
        })
    })
    .collect()
}

fn load_bundled_candidates() -> Result<Vec<SkillCandidate>> {
    let manifest: BundledManifest =
        toml::from_str(BUNDLED_MANIFEST_BYTES).context("invalid bundled skills manifest")?;
    if manifest.version != 1 {
        bail!(
            "unsupported bundled skills manifest version {}",
            manifest.version
        );
    }

    let mut resource_paths = HashSet::new();
    let mut candidates = Vec::with_capacity(manifest.skills.len());
    for entry in manifest.skills {
        let skill_id = validate_skill_id(entry.skill_id.as_str(), "bundled skill_id")?;
        if entry.owner.is_empty() || entry.slug.is_empty() {
            bail!("bundled owner and slug must be non-empty");
        }
        if entry.resource_path.starts_with('/')
            || entry.resource_path.split('/').any(|part| part == "..")
            || !resource_paths.insert(entry.resource_path.clone())
        {
            bail!(
                "invalid or duplicate bundled resource path `{}`",
                entry.resource_path
            );
        }
        candidates.push(SkillCandidate {
            skill_id,
            legacy_locator: format!("{}/{}", entry.owner, entry.slug),
            source_kind: "system".to_owned(),
            scope_key: None,
            bundled: true,
        });
    }
    Ok(candidates)
}

fn ensure_unique_candidate_ids(candidates: &[SkillCandidate]) -> Result<()> {
    let mut ids = HashSet::new();
    for candidate in candidates {
        if !ids.insert(candidate.skill_id.clone()) {
            bail!(
                "duplicate active or bundled SkillId `{}`",
                candidate.skill_id
            );
        }
    }
    Ok(())
}

fn exact_candidate<'a>(
    mut candidates: impl Iterator<Item = &'a SkillCandidate>,
) -> Option<SkillId> {
    let candidate = candidates.next()?;
    candidates
        .next()
        .is_none()
        .then(|| candidate.skill_id.clone())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonSurface {
    RuntimeSnapshot,
    TurnItem,
    TurnEvent,
    TurnAttempt,
}

#[derive(Debug, Clone)]
struct RuntimeSnapshotJsonRow {
    turn_id: String,
    workspace_id: String,
    policies_json: String,
    capabilities_json: String,
    migrate_policies: bool,
    migrate_capabilities: bool,
}

#[derive(Debug, Clone)]
struct PayloadJsonRow {
    row_id: String,
    turn_id: String,
    workspace_id: Option<String>,
    payload: String,
}

#[derive(Debug, Clone)]
enum JsonBatch {
    RuntimeSnapshots(Vec<RuntimeSnapshotJsonRow>),
    Payloads {
        surface: JsonSurface,
        rows: Vec<PayloadJsonRow>,
    },
}

impl JsonBatch {
    fn is_empty(&self) -> bool {
        match self {
            Self::RuntimeSnapshots(rows) => rows.is_empty(),
            Self::Payloads { rows, .. } => rows.is_empty(),
        }
    }
}

async fn load_json_batch(db: &DatabaseConnection, surface: JsonSurface) -> Result<JsonBatch> {
    match surface {
        JsonSurface::RuntimeSnapshot => Ok(JsonBatch::RuntimeSnapshots(
            load_runtime_snapshot_batch(db).await?,
        )),
        JsonSurface::TurnItem | JsonSurface::TurnEvent | JsonSurface::TurnAttempt => {
            Ok(JsonBatch::Payloads {
                surface,
                rows: load_payload_batch(db, surface).await?,
            })
        }
    }
}

async fn migrate_json_batch(
    db: &DatabaseConnection,
    resolver: &mut IdentityResolver,
    batch: &JsonBatch,
) -> Result<u64> {
    match batch {
        JsonBatch::RuntimeSnapshots(rows) => {
            migrate_runtime_snapshot_batch(db, resolver, rows).await
        }
        JsonBatch::Payloads { surface, rows } => {
            migrate_payload_batch(db, resolver, *surface, rows).await
        }
    }
}

#[derive(Debug, Deserialize)]
struct LegacyWorkspaceSkillPolicy {
    skill_slug: String,
    source_kind: String,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    allow_implicit_invocation: Option<bool>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct StoredWorkspaceSkillPolicy {
    skill_id: SkillId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    allow_implicit_invocation: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum WorkspaceSkillPolicySnapshot {
    Legacy(LegacyWorkspaceSkillPolicy),
    Stable(StoredWorkspaceSkillPolicy),
}

#[derive(Debug, Deserialize)]
struct LegacyTurnCapability {
    id: String,
    kind: JsonValue,
    #[serde(default)]
    label: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
enum LegacySkillCapabilityKind {
    Skill {
        slug: String,
        #[serde(rename = "sourceKind")]
        source_kind: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LegacySkillAttachmentCapability {
    #[serde(rename = "id")]
    _id: String,
    label: String,
    slug: String,
    source_kind: String,
}

#[derive(Debug, Deserialize)]
struct LegacyItemEventPayload {
    #[serde(rename = "workspace_id")]
    _workspace_id: String,
    #[serde(rename = "thread_id")]
    _thread_id: String,
    #[serde(rename = "turn_id")]
    _turn_id: String,
    item: JsonValue,
}

async fn load_runtime_snapshot_batch(
    db: &DatabaseConnection,
) -> Result<Vec<RuntimeSnapshotJsonRow>> {
    let rows = db
        .query_all_raw(Statement::from_string(
            db.get_database_backend(),
            format!(
                r#"
                WITH candidates AS (
                    SELECT
                        turn_id,
                        workspace_id,
                        workspace_skill_policies_json,
                        capabilities_json,
                        CASE
                            WHEN instr(workspace_skill_policies_json, '"skill_slug"') = 0 THEN 0
                            WHEN json_valid(workspace_skill_policies_json) = 0 THEN 1
                            WHEN EXISTS (
                                SELECT 1
                                FROM json_each(workspace_skill_policies_json) entry
                                WHERE json_type(entry.value, '$.skill_slug') = 'text'
                                  AND json_type(entry.value, '$.source_kind') = 'text'
                            ) THEN 1
                            ELSE 0
                        END AS migrate_policies,
                        CASE
                            WHEN instr(capabilities_json, '"skill"') = 0
                              OR instr(capabilities_json, '"slug"') = 0
                              OR instr(capabilities_json, '"sourceKind"') = 0 THEN 0
                            WHEN json_valid(capabilities_json) = 0 THEN 1
                            WHEN EXISTS (
                                SELECT 1
                                FROM json_each(capabilities_json) capability
                                WHERE json_extract(capability.value, '$.kind.type') = 'skill'
                                  AND json_type(capability.value, '$.kind.skillId') IS NULL
                                  AND json_type(capability.value, '$.kind.slug') = 'text'
                                  AND json_type(capability.value, '$.kind.sourceKind') = 'text'
                            ) THEN 1
                            ELSE 0
                        END AS migrate_capabilities
                    FROM turn_runtime_snapshot
                )
                SELECT turn_id, workspace_id, workspace_skill_policies_json, capabilities_json,
                       migrate_policies, migrate_capabilities
                FROM candidates
                WHERE migrate_policies = 1 OR migrate_capabilities = 1
                ORDER BY turn_id
                LIMIT {JSON_BATCH_SIZE}
                "#
            ),
        ))
        .await?;

    rows.into_iter()
        .map(|row| {
            Ok(RuntimeSnapshotJsonRow {
                turn_id: row.try_get("", "turn_id")?,
                workspace_id: row.try_get("", "workspace_id")?,
                policies_json: row.try_get("", "workspace_skill_policies_json")?,
                capabilities_json: row.try_get("", "capabilities_json")?,
                migrate_policies: row.try_get::<i64>("", "migrate_policies")? != 0,
                migrate_capabilities: row.try_get::<i64>("", "migrate_capabilities")? != 0,
            })
        })
        .collect()
}

async fn migrate_runtime_snapshot_batch(
    db: &DatabaseConnection,
    resolver: &mut IdentityResolver,
    rows: &[RuntimeSnapshotJsonRow],
) -> Result<u64> {
    let transaction = db.begin().await?;
    let mut updated_fields = 0_u64;
    for row in rows {
        if row.migrate_policies {
            let migrated = migrate_workspace_policy_snapshot(
                row.policies_json.as_str(),
                row.workspace_id.as_str(),
                resolver,
            )
            .with_context(|| {
                json_field_context(
                    "turn_runtime_snapshot",
                    row.turn_id.as_str(),
                    "workspace_skill_policies_json",
                )
            })?;
            if update_json_field_if_unchanged(
                &transaction,
                "turn_runtime_snapshot",
                "turn_id",
                row.turn_id.as_str(),
                "workspace_skill_policies_json",
                row.policies_json.as_str(),
                migrated,
            )
            .await?
            {
                updated_fields = updated_fields.saturating_add(1);
            }
        }

        if row.migrate_capabilities {
            let migrated = migrate_capability_snapshot(
                row.capabilities_json.as_str(),
                row.workspace_id.as_str(),
                resolver,
            )
            .with_context(|| {
                json_field_context(
                    "turn_runtime_snapshot",
                    row.turn_id.as_str(),
                    "capabilities_json",
                )
            })?;
            if update_json_field_if_unchanged(
                &transaction,
                "turn_runtime_snapshot",
                "turn_id",
                row.turn_id.as_str(),
                "capabilities_json",
                row.capabilities_json.as_str(),
                migrated,
            )
            .await?
            {
                updated_fields = updated_fields.saturating_add(1);
            }
        }
    }

    transaction.commit().await?;
    Ok(updated_fields)
}

fn migrate_workspace_policy_snapshot(
    raw: &str,
    workspace_id: &str,
    resolver: &mut IdentityResolver,
) -> Result<String, serde_json::Error> {
    let policies: Vec<WorkspaceSkillPolicySnapshot> = serde_json::from_str(raw)?;
    let final_policies = policies
        .into_iter()
        .map(|policy| match policy {
            WorkspaceSkillPolicySnapshot::Legacy(policy) => StoredWorkspaceSkillPolicy {
                skill_id: resolver.resolve_workspace_history_identity(
                    workspace_id,
                    policy.skill_slug.as_str(),
                    policy.source_kind.as_str(),
                ),
                enabled: policy.enabled,
                allow_implicit_invocation: policy.allow_implicit_invocation,
            },
            WorkspaceSkillPolicySnapshot::Stable(policy) => policy,
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&final_policies)
}

fn migrate_capability_snapshot(
    raw: &str,
    workspace_id: &str,
    resolver: &mut IdentityResolver,
) -> Result<String, serde_json::Error> {
    let legacy: Vec<LegacyTurnCapability> = serde_json::from_str(raw)?;
    let final_capabilities = legacy
        .into_iter()
        .map(|capability| {
            let kind = if capability.kind.get("type").and_then(JsonValue::as_str) == Some("skill")
                && capability.kind.get("skillId").is_none()
            {
                let LegacySkillCapabilityKind::Skill { slug, source_kind } =
                    serde_json::from_value(capability.kind)?;
                let skill_id = resolver.resolve_workspace_history_identity(
                    workspace_id,
                    slug.as_str(),
                    source_kind.as_str(),
                );
                pioneer_protocol::TurnCapabilityKind::Skill {
                    skill_id,
                    pack_id: None,
                }
            } else {
                serde_json::from_value(capability.kind)?
            };
            let id = match &kind {
                pioneer_protocol::TurnCapabilityKind::Skill { skill_id, .. } => {
                    pioneer_protocol::skill_capability_key(skill_id)
                }
                _ => capability.id,
            };
            Ok(pioneer_protocol::TurnCapability {
                id,
                kind,
                label: capability.label,
            })
        })
        .collect::<Result<Vec<_>, serde_json::Error>>()?;
    serde_json::to_string(&final_capabilities)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PayloadKind {
    TurnItem,
    TurnEvent,
}

impl JsonSurface {
    const fn payload_config(
        self,
    ) -> Option<(&'static str, &'static str, &'static str, PayloadKind)> {
        match self {
            Self::RuntimeSnapshot => None,
            Self::TurnItem => Some((
                "turn_item",
                "source.item_type = 'user_message'",
                "$.attachments",
                PayloadKind::TurnItem,
            )),
            Self::TurnEvent => Some((
                "turn_event",
                "source.event_type IN ('item/started', 'item/completed', 'item/updated')",
                "$.payload.item.attachments",
                PayloadKind::TurnEvent,
            )),
            Self::TurnAttempt => Some((
                "turn_item_attempt",
                "source.item_type = 'user_message'",
                "$.attachments",
                PayloadKind::TurnItem,
            )),
        }
    }
}

async fn load_payload_batch(
    db: &DatabaseConnection,
    surface: JsonSurface,
) -> Result<Vec<PayloadJsonRow>> {
    let (table, type_predicate, attachments_path, _) = surface
        .payload_config()
        .expect("runtime snapshots do not use the payload loader");
    let rows = db
        .query_all_raw(Statement::from_string(
            db.get_database_backend(),
            format!(
                r#"
                WITH candidates AS (
                    SELECT
                        source.id,
                        source.turn_id,
                        source.payload,
                        w.id AS workspace_id,
                        CASE
                            WHEN instr(source.payload, '"skill"') = 0
                              OR instr(source.payload, '"slug"') = 0
                              OR instr(source.payload, '"sourceKind"') = 0 THEN 0
                            WHEN json_valid(source.payload) = 0 THEN 1
                            WHEN EXISTS (
                                SELECT 1
                                FROM json_each(source.payload, '{attachments_path}') attachment
                                WHERE json_extract(attachment.value, '$.type') = 'skill'
                                  AND json_type(attachment.value, '$.capability.skillId') IS NULL
                                  AND json_type(attachment.value, '$.capability.slug') = 'text'
                                  AND json_type(attachment.value, '$.capability.sourceKind') = 'text'
                            ) THEN 1
                            ELSE 0
                        END AS migrate_payload
                    FROM {table} source
                    LEFT JOIN turn t ON t.id = source.turn_id
                    LEFT JOIN thread th ON th.id = t.thread_id
                    LEFT JOIN workspace w ON w.id = th.workspace_id
                    WHERE {type_predicate}
                )
                SELECT id, turn_id, payload, workspace_id
                FROM candidates
                WHERE migrate_payload = 1
                ORDER BY id
                LIMIT {JSON_BATCH_SIZE}
                "#
            ),
        ))
        .await?;

    rows.into_iter()
        .map(|row| {
            Ok(PayloadJsonRow {
                row_id: row.try_get("", "id")?,
                turn_id: row.try_get("", "turn_id")?,
                workspace_id: row.try_get("", "workspace_id")?,
                payload: row.try_get("", "payload")?,
            })
        })
        .collect()
}

async fn migrate_payload_batch(
    db: &DatabaseConnection,
    resolver: &mut IdentityResolver,
    surface: JsonSurface,
    rows: &[PayloadJsonRow],
) -> Result<u64> {
    let (table, _, _, kind) = surface
        .payload_config()
        .expect("runtime snapshots do not use the payload migrator");
    let transaction = db.begin().await?;
    let mut updated = 0_u64;
    for row in rows {
        let migrated = match kind {
            PayloadKind::TurnItem => {
                let value: JsonValue = serde_json::from_str(row.payload.as_str())
                    .with_context(|| json_field_context(table, row.row_id.as_str(), "payload"))?;
                let (value, converted) = migrate_turn_item_value(
                    value,
                    row.turn_id.as_str(),
                    row.workspace_id.as_deref(),
                    resolver,
                )
                .with_context(|| json_field_context(table, row.row_id.as_str(), "payload"))?;
                if converted == 0 {
                    bail!(
                        "{} did not contain the legacy skill attachment selected for backfill",
                        json_field_context(table, row.row_id.as_str(), "payload")
                    );
                }
                serde_json::to_string(&value)
                    .with_context(|| json_field_context(table, row.row_id.as_str(), "payload"))?
            }
            PayloadKind::TurnEvent => migrate_turn_event_payload(
                row.payload.as_str(),
                row.turn_id.as_str(),
                row.workspace_id.as_deref(),
                resolver,
            )
            .with_context(|| json_field_context(table, row.row_id.as_str(), "payload"))?,
        };
        if update_json_field_if_unchanged(
            &transaction,
            table,
            "id",
            row.row_id.as_str(),
            "payload",
            row.payload.as_str(),
            migrated,
        )
        .await?
        {
            updated = updated.saturating_add(1);
        }
    }

    transaction.commit().await?;
    Ok(updated)
}

fn migrate_turn_event_payload(
    raw: &str,
    persisted_turn_id: &str,
    workspace_id: Option<&str>,
    resolver: &mut IdentityResolver,
) -> Result<String, serde_json::Error> {
    let mut value: JsonValue = serde_json::from_str(raw)?;
    let kind = value
        .get("kind")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    if !matches!(kind, "item_started" | "item_completed" | "item_updated") {
        return Err(json_shape_error(
            "event selected for skill backfill has a non-item event kind",
        ));
    }
    let legacy: LegacyItemEventPayload =
        serde_json::from_value(value.get("payload").cloned().unwrap_or(JsonValue::Null))?;
    let (migrated_item, converted) =
        migrate_turn_item_value(legacy.item, persisted_turn_id, workspace_id, resolver)?;
    if converted == 0 {
        return Err(json_shape_error(
            "event did not contain the selected legacy skill attachment",
        ));
    }
    let payload = value
        .get_mut("payload")
        .and_then(JsonValue::as_object_mut)
        .ok_or_else(|| json_shape_error("event payload is not an object"))?;
    payload.insert("item".to_owned(), migrated_item);
    serde_json::to_string(&value)
}

fn migrate_turn_item_value(
    mut value: JsonValue,
    turn_id: &str,
    workspace_id: Option<&str>,
    resolver: &mut IdentityResolver,
) -> Result<(JsonValue, usize), serde_json::Error> {
    let mut converted = 0_usize;
    if value.get("type").and_then(JsonValue::as_str) == Some("userMessage") {
        if let Some(attachments) = value.get_mut("attachments") {
            let attachments = attachments
                .as_array_mut()
                .ok_or_else(|| json_shape_error("attachments is not an array"))?;
            for attachment in attachments {
                if attachment.get("type").and_then(JsonValue::as_str) != Some("skill")
                    || attachment
                        .get("capability")
                        .and_then(|capability| capability.get("skillId"))
                        .is_some()
                {
                    continue;
                }
                let legacy: LegacySkillAttachmentCapability = serde_json::from_value(
                    attachment
                        .get("capability")
                        .cloned()
                        .unwrap_or(JsonValue::Null),
                )?;
                let skill_id = resolver.resolve_history_identity(
                    Some(turn_id),
                    workspace_id,
                    legacy.slug.as_str(),
                    legacy.source_kind.as_str(),
                );
                let (owner, slug) = split_legacy_locator(legacy.slug.as_str());
                let final_summary = pioneer_protocol::TurnSkillCapabilitySummary {
                    skill_id,
                    label: legacy.label,
                    owner,
                    slug,
                    source_kind: legacy.source_kind,
                    pack: None,
                };
                let object = attachment
                    .as_object_mut()
                    .ok_or_else(|| json_shape_error("attachment is not an object"))?;
                object.insert(
                    "capability".to_owned(),
                    serde_json::to_value(final_summary)?,
                );
                converted = converted.saturating_add(1);
            }
        }
    }

    let typed: pioneer_protocol::TurnItem = serde_json::from_value(value)?;
    Ok((serde_json::to_value(typed)?, converted))
}

async fn update_json_field_if_unchanged(
    db: &impl ConnectionTrait,
    table: &str,
    id_column: &str,
    row_id: &str,
    field: &str,
    expected: &str,
    value: String,
) -> Result<bool> {
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!("UPDATE {table} SET {field} = ? WHERE {id_column} = ? AND {field} = ?"),
            [
                value.clone().into(),
                row_id.to_owned().into(),
                expected.to_owned().into(),
            ],
        ))
        .await?;
    match result.rows_affected() {
        1 => return Ok(true),
        0 => {}
        rows_affected => bail!(
            "stable SkillId JSON update for `{table}` row `{row_id}` affected {rows_affected} rows"
        ),
    }

    // sqlite-zstd writes through an INSTEAD OF trigger and reports zero rows.
    // Verify the public view, keeping compression transparent to this worker.
    let persisted = db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!("SELECT {field} AS migrated_value FROM {table} WHERE {id_column} = ?"),
            [row_id.to_owned().into()],
        ))
        .await?;
    let Some(persisted) = persisted else {
        return Ok(false);
    };
    let persisted: String = persisted.try_get("", "migrated_value")?;
    if persisted == value {
        return Ok(true);
    }
    if persisted == expected {
        bail!("stable SkillId JSON update for `{table}` row `{row_id}` was not persisted");
    }
    Ok(false)
}

fn json_field_context(table: &str, row_id: &str, field: &str) -> String {
    format!("failed to backfill {table} row `{row_id}` field `{field}`")
}

fn json_shape_error(message: &str) -> serde_json::Error {
    <serde_json::Error as serde::de::Error>::custom(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use sea_orm::{Database, DatabaseConnection};

    const WORKSPACE_ID: &str = "WWWWWWWWWWWWWWWWWWWWW";
    const THREAD_ID: &str = "TTTTTTTTTTTTTTTTTTTTT";
    const TURN_ID: &str = "UUUUUUUUUUUUUUUUUUUUU";
    const INSTALLATION_ID: &str = "AAAAAAAAAAAAAAAAAAAAA";
    const UNCHANGED_EVENT: &str =
        r#"{ "kind" : "turn_started", "payload" : { "marker" : "unchanged" } }"#;

    fn candidate(
        id: char,
        legacy_locator: &str,
        source_kind: &str,
        scope_key: Option<&str>,
        bundled: bool,
    ) -> SkillCandidate {
        SkillCandidate {
            skill_id: SkillId::new(id.to_string().repeat(21)).expect("valid fixture SkillId"),
            legacy_locator: legacy_locator.to_owned(),
            source_kind: source_kind.to_owned(),
            scope_key: scope_key.map(str::to_owned),
            bundled,
        }
    }

    async fn apply_pre_stable_migrations(db: &DatabaseConnection) {
        let migration_count = Migrator::migrations()
            .iter()
            .position(|migration| migration.name() == "m20260720_000002_stable_skill_id")
            .expect("Stable SkillId migration should be registered");
        Migrator::up(db, Some(migration_count as u32))
            .await
            .expect("pre-Stable-SkillId migrations should apply");
    }

    async fn seed_graph(db: &DatabaseConnection) {
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name)
                VALUES ('WWWWWWWWWWWWWWWWWWWWW', 'Fixture');
            INSERT INTO thread
                (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('TTTTTTTTTTTTTTTTTTTTT', 'WWWWWWWWWWWWWWWWWWWWW',
                        '', 'agent', 'model', 'provider', 'active');
            INSERT INTO turn (id, thread_id, status)
                VALUES ('UUUUUUUUUUUUUUUUUUUUU', 'TTTTTTTTTTTTTTTTTTTTT', 'active');
            "#,
        )
        .await
        .expect("workspace graph should insert");
    }

    async fn seed_legacy_relations(db: &DatabaseConnection) {
        db.execute_unprepared(
            r#"
            INSERT INTO skill_installation
                (id, slug, version, source_kind, scope_key, source_ref, install_path,
                 trust_level, fingerprint)
                VALUES ('AAAAAAAAAAAAAAAAAAAAA', 'owner/current', '1.0', 'user',
                        'WWWWWWWWWWWWWWWWWWWWW', 'source-a', '/legacy/a',
                        'community', 'fp-a');
            INSERT INTO skill_workspace_policy
                (id, workspace_id, skill_slug, source_kind, enabled,
                 allow_implicit_invocation)
                VALUES ('PPPPPPPPPPPPPPPPPPPPP', 'WWWWWWWWWWWWWWWWWWWWW',
                        'owner/current', 'user', 1, 0);
            INSERT INTO turn_skill_binding
                (id, turn_id, skill_slug, skill_version, fingerprint, source_kind,
                 resolved_reason)
                VALUES ('BBBBBBBBBBBBBBBBBBBBB', 'UUUUUUUUUUUUUUUUUUUUU',
                        'owner/current', '1.0', 'fp-binding', 'user', 'explicit');
            INSERT INTO skill_audit_event
                (id, turn_id, skill_slug, source_kind, action, decision, details_json)
                VALUES ('CCCCCCCCCCCCCCCCCCCCC', 'UUUUUUUUUUUUUUUUUUUUU',
                        'old/deleted', 'registry', 'invoke', 'allow', '{}');
            INSERT INTO skill_dependency_snapshot
                (id, turn_id, skill_slug, source_kind, diagnostics_json)
                VALUES ('DDDDDDDDDDDDDDDDDDDDD', 'UUUUUUUUUUUUUUUUUUUUU',
                        'old/deleted', 'registry', '[]');
            "#,
        )
        .await
        .expect("legacy skill relations should insert");
    }

    fn legacy_item() -> &'static str {
        r#"{ "type" : "userMessage", "id" : "message-1", "text" : "hello", "attachments" : [{ "type" : "skill", "capability" : { "id" : "skill:user:owner/current", "label" : "Current", "slug" : "owner/current", "sourceKind" : "user" } }] }"#
    }

    async fn seed_legacy_json(db: &DatabaseConnection) {
        let legacy_event = format!(
            r#"{{"kind":"item_started","payload":{{"workspace_id":"{WORKSPACE_ID}","thread_id":"{THREAD_ID}","turn_id":"{TURN_ID}","item":{}}}}}"#,
            legacy_item()
        );
        let policies = r#"[{ "skill_slug" : "owner/current", "source_kind" : "user", "enabled" : true, "allow_implicit_invocation" : false }]"#;
        let capabilities = r#"[{ "id" : "skill:user:owner/current", "label" : "Current", "kind" : { "type" : "skill", "slug" : "owner/current", "sourceKind" : "user" } }]"#;

        db.execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            r#"
            INSERT INTO turn_runtime_snapshot (
                turn_id, thread_id, workspace_id, mode_json, model, provider_name,
                hook_runtime_context_json, workspace_skill_policies_json, input_json,
                capabilities_json, resolved_artifacts_json, runtime_environment_json,
                history_json
            ) VALUES (?, ?, ?, '{}', 'model', 'provider', '{}', ?, '[]', ?, '[]', '{}', '[]')
            "#,
            vec![
                TURN_ID.to_owned().into(),
                THREAD_ID.to_owned().into(),
                WORKSPACE_ID.to_owned().into(),
                policies.to_owned().into(),
                capabilities.to_owned().into(),
            ],
        ))
        .await
        .expect("legacy runtime snapshot should insert");
        db.execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "INSERT INTO turn_item (id, turn_id, item_id, item_type, payload) VALUES (?, ?, 'message-1', 'user_message', ?)",
            [
                "IIIIIIIIIIIIIIIIIIIII".to_owned().into(),
                TURN_ID.to_owned().into(),
                legacy_item().to_owned().into(),
            ],
        ))
        .await
        .expect("legacy turn item should insert");
        db.execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "INSERT INTO turn_item_attempt (id, turn_id, item_id, item_type, attempt_number, status, payload) VALUES (?, ?, 'message-1', 'user_message', 1, 'running', ?)",
            [
                "JJJJJJJJJJJJJJJJJJJJJ".to_owned().into(),
                TURN_ID.to_owned().into(),
                legacy_item().to_owned().into(),
            ],
        ))
        .await
        .expect("legacy turn attempt should insert");
        for (id, sequence, event_type, payload) in [
            (
                "EEEEEEEEEEEEEEEEEEEEE",
                1_i64,
                "item/started",
                legacy_event.as_str(),
            ),
            (
                "FFFFFFFFFFFFFFFFFFFFF",
                2_i64,
                "turn_started",
                UNCHANGED_EVENT,
            ),
        ] {
            db.execute_raw(Statement::from_sql_and_values(
                db.get_database_backend(),
                "INSERT INTO turn_event (id, thread_id, turn_id, sequence, event_type, payload) VALUES (?, ?, ?, ?, ?, ?)",
                vec![
                    id.to_owned().into(),
                    THREAD_ID.to_owned().into(),
                    TURN_ID.to_owned().into(),
                    sequence.into(),
                    event_type.to_owned().into(),
                    payload.to_owned().into(),
                ],
            ))
            .await
            .expect("turn event should insert");
        }
    }

    async fn row_count(db: &DatabaseConnection, table: &str) -> i64 {
        db.query_one_raw(Statement::from_string(
            db.get_database_backend(),
            format!("SELECT COUNT(*) AS count FROM {table}"),
        ))
        .await
        .expect("row count should query")
        .expect("row count should exist")
        .try_get("", "count")
        .expect("row count should decode")
    }

    async fn table_is_present(db: &DatabaseConnection, table: &str) -> bool {
        table_exists(db, table)
            .await
            .expect("sqlite schema should query")
    }

    async fn string_field(db: &DatabaseConnection, table: &str, id: &str, field: &str) -> String {
        db.query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!("SELECT {field} AS value FROM {table} WHERE id = ?"),
            [id.to_owned().into()],
        ))
        .await
        .expect("field should query")
        .expect("row should exist")
        .try_get("", "value")
        .expect("field should decode")
    }

    #[test]
    fn historical_id_is_reproducible_without_persisted_progress_state() {
        let mut first = IdentityResolver {
            candidates: Vec::new(),
            historical_ids: HashMap::new(),
        };
        let first_id =
            first.resolve_workspace_history_identity(WORKSPACE_ID, "removed/example", "registry");

        let mut restarted = IdentityResolver {
            candidates: Vec::new(),
            historical_ids: HashMap::new(),
        };
        let restarted_id = restarted.resolve_workspace_history_identity(
            WORKSPACE_ID,
            "removed/example",
            "registry",
        );

        assert_eq!(first_id, restarted_id);
        assert_eq!(first_id.as_str().len(), 21);
        assert!(
            first_id
                .as_str()
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric())
        );
    }

    #[test]
    fn resolver_requires_one_exact_visible_candidate_and_never_guesses_ambiguity() {
        let workspace_a = candidate('A', "owner/current", "user", Some("workspace-a"), false);
        let workspace_b = candidate('B', "owner/current", "user", Some("workspace-b"), false);
        let bundled = candidate('C', "pioneer/browser", "system", None, true);
        let resolver = IdentityResolver {
            candidates: vec![workspace_a.clone(), workspace_b, bundled.clone()],
            historical_ids: HashMap::new(),
        };
        assert_eq!(
            resolver.resolve_policy_identity("workspace-a", "owner/current", "user"),
            Some(workspace_a.skill_id.clone())
        );
        assert_eq!(
            resolver.resolve_policy_identity("any-workspace", "pioneer/browser", "system"),
            Some(bundled.skill_id)
        );

        let duplicate = candidate('D', "owner/current", "user", Some("workspace-a"), false);
        let mut ambiguous = IdentityResolver {
            candidates: vec![workspace_a, duplicate],
            historical_ids: HashMap::new(),
        };
        assert_eq!(
            ambiguous.resolve_policy_identity("workspace-a", "owner/current", "user"),
            None
        );
        let historical =
            ambiguous.resolve_workspace_history_identity("workspace-a", "owner/current", "user");
        assert_eq!(
            historical,
            ambiguous.resolve_workspace_history_identity("workspace-a", "owner/current", "user")
        );
        assert!(
            !ambiguous
                .candidates
                .iter()
                .any(|candidate| candidate.skill_id == historical)
        );
        assert_eq!(
            split_legacy_locator("owner/nested/leaf"),
            (Some("owner".to_owned()), "nested/leaf".to_owned())
        );
    }

    #[tokio::test]
    async fn invalid_installation_id_rolls_back_its_complete_batch() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        apply_pre_stable_migrations(&db).await;
        for (id, slug) in [
            ("AAAAAAAAAAAAAAAAAAAAA", "owner/valid"),
            ("invalid-skill-id", "owner/invalid"),
        ] {
            db.execute_raw(Statement::from_sql_and_values(
                db.get_database_backend(),
                r#"
                INSERT INTO skill_installation (
                    id, slug, source_kind, scope_key, source_ref, install_path,
                    trust_level, fingerprint
                ) VALUES (?, ?, 'user', 'workspace', ?, ?, 'community', ?)
                "#,
                vec![
                    id.to_owned().into(),
                    slug.to_owned().into(),
                    format!("source-{id}").into(),
                    format!("/legacy/{slug}").into(),
                    format!("fingerprint-{id}").into(),
                ],
            ))
            .await
            .expect("legacy installation should insert");
        }
        Migrator::up(&db, None)
            .await
            .expect("schema-only Stable SkillId migration should apply");

        migrate_installation_batch(&db)
            .await
            .expect_err("invalid legacy ID should reject the complete batch");
        assert_eq!(row_count(&db, "skill_installation").await, 0);
        assert_eq!(row_count(&db, LEGACY_SKILL_INSTALLATION_TABLE).await, 2);
    }

    #[tokio::test]
    async fn foreground_installation_with_the_same_id_wins_over_its_legacy_row() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        apply_pre_stable_migrations(&db).await;
        db.execute_unprepared(
            r#"
            INSERT INTO skill_installation (
                id, slug, source_kind, scope_key, source_ref, install_path,
                trust_level, fingerprint
            ) VALUES (
                'AAAAAAAAAAAAAAAAAAAAA', 'owner/legacy', 'user', 'workspace',
                'legacy-source', '/legacy/path', 'community', 'legacy-fingerprint'
            );
            "#,
        )
        .await
        .expect("legacy installation should insert");
        Migrator::up(&db, None)
            .await
            .expect("schema-only Stable SkillId migration should apply");
        db.execute_unprepared(
            r#"
            INSERT INTO skill_installation (
                id, owner, slug, source_kind, scope_key, source_ref, install_path,
                trust_level, fingerprint
            ) VALUES (
                'AAAAAAAAAAAAAAAAAAAAA', 'owner', 'foreground', 'user', 'workspace',
                'foreground-source', '/new/path', 'community', 'foreground-fingerprint'
            );
            "#,
        )
        .await
        .expect("foreground installation should insert");

        assert_eq!(
            migrate_installation_batch(&db)
                .await
                .expect("legacy row should yield to the foreground installation"),
            BatchOutcome::completed(1)
        );
        assert!(!table_is_present(&db, LEGACY_SKILL_INSTALLATION_TABLE).await);
        assert_eq!(
            string_field(&db, "skill_installation", INSTALLATION_ID, "install_path").await,
            "/new/path"
        );
    }

    #[tokio::test]
    async fn backfill_is_progressive_and_completes_all_approved_surfaces() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        apply_pre_stable_migrations(&db).await;
        seed_graph(&db).await;
        seed_legacy_relations(&db).await;
        seed_legacy_json(&db).await;
        Migrator::up(&db, None)
            .await
            .expect("schema-only Stable SkillId migration should apply");

        assert_eq!(row_count(&db, "skill_installation").await, 0);
        let first_batch = migrate_installation_batch(&db)
            .await
            .expect("first installation batch should migrate");
        assert_eq!(first_batch, BatchOutcome::completed(1));
        assert_eq!(row_count(&db, "skill_installation").await, 1);
        assert!(!table_is_present(&db, LEGACY_SKILL_INSTALLATION_TABLE).await);

        let summary = backfill_all(&CrudStore::new(db.clone()))
            .await
            .expect("remaining Stable SkillId backfill should complete");
        assert_eq!(summary.installations, 0);
        assert_eq!(summary.policies, 1);
        assert_eq!(summary.history_rows, 3);
        assert_eq!(summary.runtime_snapshot_fields, 2);
        assert_eq!(summary.turn_item_payloads, 1);
        assert_eq!(summary.turn_event_payloads, 1);
        assert_eq!(summary.turn_attempt_payloads, 1);

        for table in [
            LEGACY_SKILL_INSTALLATION_TABLE,
            LEGACY_SKILL_WORKSPACE_POLICY_TABLE,
            LEGACY_TURN_SKILL_BINDING_TABLE,
            LEGACY_SKILL_AUDIT_EVENT_TABLE,
            LEGACY_SKILL_DEPENDENCY_SNAPSHOT_TABLE,
        ] {
            assert!(
                !table_is_present(&db, table).await,
                "{table} should be dropped"
            );
        }

        let installation = db
            .query_one_raw(Statement::from_string(
                db.get_database_backend(),
                "SELECT id, owner, slug, install_path FROM skill_installation".to_owned(),
            ))
            .await
            .expect("installation should query")
            .expect("installation should exist");
        assert_eq!(
            installation.try_get::<String>("", "id").unwrap(),
            INSTALLATION_ID
        );
        assert_eq!(
            installation.try_get::<String>("", "owner").unwrap(),
            "owner"
        );
        assert_eq!(
            installation.try_get::<String>("", "slug").unwrap(),
            "current"
        );
        assert_eq!(
            installation.try_get::<String>("", "install_path").unwrap(),
            "/legacy/a"
        );
        assert_eq!(
            string_field(
                &db,
                "skill_workspace_policy",
                "PPPPPPPPPPPPPPPPPPPPP",
                "skill_id"
            )
            .await,
            INSTALLATION_ID
        );
        assert_eq!(
            string_field(
                &db,
                "turn_skill_binding",
                "BBBBBBBBBBBBBBBBBBBBB",
                "skill_id"
            )
            .await,
            INSTALLATION_ID
        );
        let audit_id = string_field(
            &db,
            "skill_audit_event",
            "CCCCCCCCCCCCCCCCCCCCC",
            "skill_id",
        )
        .await;
        assert_eq!(
            string_field(
                &db,
                "skill_dependency_snapshot",
                "DDDDDDDDDDDDDDDDDDDDD",
                "skill_id"
            )
            .await,
            audit_id
        );
        validate_skill_id(audit_id.as_str(), "historical fixture skill_id")
            .expect("historical ID should be valid");

        for (table, id) in [
            ("turn_item", "IIIIIIIIIIIIIIIIIIIII"),
            ("turn_event", "EEEEEEEEEEEEEEEEEEEEE"),
            ("turn_item_attempt", "JJJJJJJJJJJJJJJJJJJJJ"),
        ] {
            let payload = string_field(&db, table, id, "payload").await;
            assert!(
                payload.contains("\"skillId\":\"AAAAAAAAAAAAAAAAAAAAA\""),
                "{table} should contain the exact stable ID"
            );
            assert!(!payload.contains("\"id\":\"skill:user:owner/current\""));
        }
        assert_eq!(
            string_field(&db, "turn_event", "FFFFFFFFFFFFFFFFFFFFF", "payload").await,
            UNCHANGED_EVENT
        );
        assert_eq!(
            backfill_all(&CrudStore::new(db.clone()))
                .await
                .expect("completed backfill should be a cheap no-op"),
            StableSkillIdBackfillSummary::default()
        );
    }

    #[tokio::test]
    async fn restart_reuses_historical_id_from_an_already_migrated_surface() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        apply_pre_stable_migrations(&db).await;
        seed_graph(&db).await;
        seed_legacy_relations(&db).await;
        Migrator::up(&db, None)
            .await
            .expect("schema-only Stable SkillId migration should apply");

        let store = CrudStore::new(db.clone());
        assert_eq!(backfill_installations(&store).await.unwrap(), 1);

        let mut first_process = IdentityResolver::load(&db)
            .await
            .expect("first process resolver should load");
        assert_eq!(
            migrate_history_batch(&db, &mut first_process, HistorySurface::Audit)
                .await
                .expect("audit history should migrate"),
            BatchOutcome::completed(1)
        );
        let audit_id = string_field(
            &db,
            "skill_audit_event",
            "CCCCCCCCCCCCCCCCCCCCC",
            "skill_id",
        )
        .await;

        let mut restarted = IdentityResolver::load(&db)
            .await
            .expect("restarted resolver should recover migrated historical identities");
        assert_eq!(
            migrate_history_batch(&db, &mut restarted, HistorySurface::Dependency)
                .await
                .expect("dependency history should migrate after restart"),
            BatchOutcome::completed(1)
        );
        assert_eq!(
            string_field(
                &db,
                "skill_dependency_snapshot",
                "DDDDDDDDDDDDDDDDDDDDD",
                "skill_id"
            )
            .await,
            audit_id
        );
    }

    #[tokio::test]
    async fn restart_resumes_from_remaining_legacy_rows_without_cursor_state() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        apply_pre_stable_migrations(&db).await;
        for index in 0..40 {
            let id = format!("{index:021}");
            db.execute_raw(Statement::from_sql_and_values(
                db.get_database_backend(),
                r#"
                INSERT INTO skill_installation (
                    id, slug, source_kind, scope_key, source_ref, install_path,
                    trust_level, fingerprint
                ) VALUES (?, ?, 'user', 'workspace', ?, ?, 'community', ?)
                "#,
                vec![
                    id.clone().into(),
                    format!("owner/skill-{index}").into(),
                    format!("source-{index}").into(),
                    format!("/legacy/{index}").into(),
                    format!("fingerprint-{index}").into(),
                ],
            ))
            .await
            .expect("legacy installation should insert");
        }
        Migrator::up(&db, None)
            .await
            .expect("schema-only Stable SkillId migration should apply");

        assert_eq!(
            migrate_installation_batch(&db)
                .await
                .expect("first process should migrate one batch"),
            BatchOutcome::progressed(BATCH_SIZE)
        );
        assert_eq!(
            row_count(&db, "skill_installation").await,
            BATCH_SIZE as i64
        );
        assert_eq!(
            row_count(&db, LEGACY_SKILL_INSTALLATION_TABLE).await,
            40 - BATCH_SIZE as i64
        );

        let restarted_store = CrudStore::new(db.clone());
        let summary = backfill_all(&restarted_store)
            .await
            .expect("restarted worker should finish remaining rows");
        assert_eq!(summary.installations, 40 - BATCH_SIZE);
        assert_eq!(row_count(&db, "skill_installation").await, 40);
        assert!(!table_is_present(&db, LEGACY_SKILL_INSTALLATION_TABLE).await);

        let state_tables = db
            .query_one_raw(Statement::from_string(
                db.get_database_backend(),
                "SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'table' AND (name LIKE '%stable_skill_id%cursor%' OR name LIKE '%stable_skill_id%progress%' OR name LIKE '%stable_skill_id%state%')".to_owned(),
            ))
            .await
            .expect("schema should query")
            .expect("schema count should exist")
            .try_get::<i64>("", "count")
            .expect("schema count should decode");
        assert_eq!(state_tables, 0);
    }

    #[tokio::test]
    async fn foreground_skill_write_completes_between_background_batches() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        apply_pre_stable_migrations(&db).await;
        for index in 0..40 {
            let id = format!("{index:021}");
            db.execute_raw(Statement::from_sql_and_values(
                db.get_database_backend(),
                r#"
                INSERT INTO skill_installation (
                    id, slug, source_kind, scope_key, source_ref, install_path,
                    trust_level, fingerprint
                ) VALUES (?, ?, 'user', 'workspace', ?, ?, 'community', ?)
                "#,
                vec![
                    id.clone().into(),
                    format!("owner/skill-{index}").into(),
                    format!("source-{index}").into(),
                    format!("/legacy/{index}").into(),
                    format!("fingerprint-{index}").into(),
                ],
            ))
            .await
            .expect("legacy installation should insert");
        }
        Migrator::up(&db, None)
            .await
            .expect("schema-only Stable SkillId migration should apply");

        let store = Arc::new(CrudStore::new(db.clone()));
        let background = tokio::spawn({
            let store = store.clone();
            async move { backfill_installations(store.as_ref()).await }
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if row_count(&db, LEGACY_SKILL_INSTALLATION_TABLE).await < 40 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first background batch should commit");

        let foreground = pioneer_crud::SkillInstallationRecord {
            skill_id: SkillId::new("ZZZZZZZZZZZZZZZZZZZZZ").unwrap(),
            owner: Some("owner".to_owned()),
            slug: "foreground".to_owned(),
            version: None,
            source_kind: "user".to_owned(),
            scope_key: "workspace".to_owned(),
            source_ref: "foreground-source".to_owned(),
            install_path: "/new/foreground".to_owned(),
            trust_level: "community".to_owned(),
            fingerprint: "foreground-fingerprint".to_owned(),
            updated_at_unix: 0,
            pack_id: None,
            pack_member_key: None,
        };
        tokio::time::timeout(
            Duration::from_secs(5),
            store.insert_skill_installation(&foreground, 1_700_000_000),
        )
        .await
        .expect("foreground write must not wait for the complete background migration")
        .expect("foreground skill installation should insert");

        assert_eq!(
            background
                .await
                .expect("background task should join")
                .expect("background installation migration should complete"),
            40
        );
        assert_eq!(row_count(&db, "skill_installation").await, 41);
    }

    #[tokio::test]
    async fn malformed_identity_payload_stops_only_background_work_and_retries_from_row_state() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        apply_pre_stable_migrations(&db).await;
        seed_graph(&db).await;
        seed_legacy_relations(&db).await;
        let malformed = r#"{ "type": "userMessage", "attachments": [{ "type": "skill", "capability": { "slug": "owner/current", "sourceKind": "user" }"#;
        db.execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "INSERT INTO turn_item (id, turn_id, item_id, item_type, payload) VALUES (?, ?, 'message-malformed', 'user_message', ?)",
            [
                "MMMMMMMMMMMMMMMMMMMMM".to_owned().into(),
                TURN_ID.to_owned().into(),
                malformed.to_owned().into(),
            ],
        ))
        .await
        .expect("malformed legacy identity payload should insert as text");
        Migrator::up(&db, None)
            .await
            .expect("schema-only Stable SkillId migration should apply");

        let store = CrudStore::new(db.clone());
        let error = backfill_all(&store)
            .await
            .expect_err("malformed identity payload should stop this background pass");
        assert!(
            error
                .to_string()
                .contains("turn_item row `MMMMMMMMMMMMMMMMMMMMM`")
        );
        assert_eq!(
            string_field(&db, "turn_item", "MMMMMMMMMMMMMMMMMMMMM", "payload").await,
            malformed
        );

        db.execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "UPDATE turn_item SET payload = ? WHERE id = ?",
            [
                legacy_item().to_owned().into(),
                "MMMMMMMMMMMMMMMMMMMMM".to_owned().into(),
            ],
        ))
        .await
        .expect("fixture payload should repair");
        backfill_all(&store)
            .await
            .expect("restarted background pass should finish from remaining row state");
        assert!(
            string_field(&db, "turn_item", "MMMMMMMMMMMMMMMMMMMMM", "payload")
                .await
                .contains("\"skillId\":\"AAAAAAAAAAAAAAAAAAAAA\"")
        );
    }

    #[tokio::test]
    async fn payload_batch_does_not_overwrite_a_concurrent_runtime_change() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        apply_pre_stable_migrations(&db).await;
        seed_graph(&db).await;
        seed_legacy_relations(&db).await;
        db.execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "INSERT INTO turn_item (id, turn_id, item_id, item_type, payload) VALUES (?, ?, 'message-race', 'user_message', ?)",
            [
                "RRRRRRRRRRRRRRRRRRRRR".to_owned().into(),
                TURN_ID.to_owned().into(),
                legacy_item().to_owned().into(),
            ],
        ))
        .await
        .expect("legacy identity payload should insert");
        Migrator::up(&db, None)
            .await
            .expect("schema-only Stable SkillId migration should apply");
        let store = CrudStore::new(db.clone());
        backfill_installations(&store)
            .await
            .expect("installation backfill should complete");

        let selected = load_payload_batch(&db, JsonSurface::TurnItem)
            .await
            .expect("legacy payload candidate should load");
        assert_eq!(selected.len(), 1);

        let concurrent_value =
            r#"{"type":"userMessage","id":"message-race","text":"edited","attachments":[]}"#;
        db.execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "UPDATE turn_item SET payload = ? WHERE id = ?",
            [
                concurrent_value.to_owned().into(),
                "RRRRRRRRRRRRRRRRRRRRR".to_owned().into(),
            ],
        ))
        .await
        .expect("concurrent runtime update should persist");

        let mut resolver = IdentityResolver::load(&db)
            .await
            .expect("identity resolver should load");
        assert_eq!(
            migrate_payload_batch(&db, &mut resolver, JsonSurface::TurnItem, &selected)
                .await
                .expect("stale batch should complete without overwriting"),
            0
        );
        assert_eq!(
            string_field(&db, "turn_item", "RRRRRRRRRRRRRRRRRRRRR", "payload").await,
            concurrent_value
        );
    }

    async fn enable_and_compress_payload_views(db: &DatabaseConnection) {
        for table in ["turn_item", "turn_event"] {
            let config = serde_json::json!({
                "table": table,
                "column": "payload",
                "compression_level": 3,
                "dict_chooser": "'[nodict]'",
            });
            db.query_one_raw(Statement::from_sql_and_values(
                db.get_database_backend(),
                "SELECT zstd_enable_transparent(?) AS value",
                [config.to_string().into()],
            ))
            .await
            .unwrap_or_else(|error| panic!("{table}.payload compression should enable: {error}"));
        }
        db.query_one_raw(Statement::from_string(
            db.get_database_backend(),
            "SELECT zstd_incremental_maintenance(NULL, 1) AS value".to_owned(),
        ))
        .await
        .expect("identity payload rows should compress");
    }

    async fn payload_dict_id(db: &DatabaseConnection, table: &str, row_id: &str) -> Option<i64> {
        db.query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!("SELECT _payload_dict AS dict_id FROM _{table}_zstd WHERE id = ?"),
            [row_id.to_owned().into()],
        ))
        .await
        .expect("backing row should query")
        .expect("backing row should exist")
        .try_get("", "dict_id")
        .expect("dictionary ID should decode")
    }

    #[tokio::test]
    async fn compressed_payloads_migrate_through_public_views_and_recompress() {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd extension should register");
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        apply_pre_stable_migrations(&db).await;
        seed_graph(&db).await;
        seed_legacy_relations(&db).await;
        seed_legacy_json(&db).await;
        enable_and_compress_payload_views(&db).await;
        assert!(
            payload_dict_id(&db, "turn_item", "IIIIIIIIIIIIIIIIIIIII")
                .await
                .is_some()
        );
        assert!(
            payload_dict_id(&db, "turn_event", "EEEEEEEEEEEEEEEEEEEEE")
                .await
                .is_some()
        );

        Migrator::up(&db, None)
            .await
            .expect("schema-only Stable SkillId migration should apply");
        backfill_all(&CrudStore::new(db.clone()))
            .await
            .expect("compressed Stable SkillId payloads should migrate");

        for (table, row_id) in [
            ("turn_item", "IIIIIIIIIIIIIIIIIIIII"),
            ("turn_event", "EEEEEEEEEEEEEEEEEEEEE"),
        ] {
            let migrated = string_field(&db, table, row_id, "payload").await;
            assert!(migrated.contains("\"skillId\":\"AAAAAAAAAAAAAAAAAAAAA\""));
            assert_eq!(payload_dict_id(&db, table, row_id).await, None);
        }

        db.query_one_raw(Statement::from_string(
            db.get_database_backend(),
            "SELECT zstd_incremental_maintenance(NULL, 1) AS value".to_owned(),
        ))
        .await
        .expect("migrated payload rows should recompress");
        assert!(
            payload_dict_id(&db, "turn_item", "IIIIIIIIIIIIIIIIIIIII")
                .await
                .is_some()
        );
        assert!(
            payload_dict_id(&db, "turn_event", "EEEEEEEEEEEEEEEEEEEEE")
                .await
                .is_some()
        );
    }
}
