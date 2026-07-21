use pioneer_protocol::SkillId;
use sea_orm_migration::{
    prelude::*,
    schema::{boolean, string, text, timestamp_with_time_zone},
    sea_orm::{ConnectionTrait, Statement},
};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};

const BUNDLED_MANIFEST_BYTES: &str =
    include_str!("../../../resources/skills/bundled-system-skills.toml");

const NEXT_INSTALLATION_TABLE: &str = "_stable_skill_installation";
const NEXT_POLICY_TABLE: &str = "_stable_skill_workspace_policy";
const POLICY_MAP_TABLE: &str = "_stable_skill_policy_map";
const NEXT_BINDING_TABLE: &str = "_stable_turn_skill_binding";
const NEXT_AUDIT_TABLE: &str = "_stable_skill_audit_event";
const NEXT_DEPENDENCY_TABLE: &str = "_stable_skill_dependency_snapshot";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        tracing::info!(migration = "stable_skill_id", "database migration started");
        let result = run_stable_skill_id_migration(manager).await;
        match &result {
            Ok(counts) => tracing::info!(
                migration = "stable_skill_id",
                active_installations = counts.active_installations,
                bundled_candidates = counts.bundled_candidates,
                policies = counts.policies,
                history_rows = counts.history_rows,
                runtime_snapshot_fields = counts.runtime_snapshot_fields,
                turn_item_payloads = counts.turn_item_payloads,
                turn_event_payloads = counts.turn_event_payloads,
                turn_attempt_payloads = counts.turn_attempt_payloads,
                "database migration completed"
            ),
            Err(error) => tracing::error!(
                migration = "stable_skill_id",
                error = %error,
                "database migration failed; transaction rollback requested"
            ),
        }
        result.map(|_| ())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "stable SkillId migration is intentionally irreversible".to_owned(),
        ))
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn run_stable_skill_id_migration(
    manager: &SchemaManager<'_>,
) -> Result<MigrationCounts, DbErr> {
    let mut context = migrate_installations_and_policies(manager).await?;
    migrate_history_surfaces(manager, &mut context).await?;
    migrate_typed_json_surfaces(manager, &mut context).await?;
    finish_core_table_swap(manager).await?;
    Ok(context.counts)
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
    owner: Option<String>,
    slug: String,
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

#[derive(Debug)]
struct MigrationContext {
    candidates: Vec<SkillCandidate>,
    turn_workspaces: HashMap<String, String>,
    historical_ids: HashMap<HistoricalGroup, SkillId>,
    counts: MigrationCounts,
}

#[derive(Debug, Default)]
struct MigrationCounts {
    active_installations: u64,
    bundled_candidates: u64,
    policies: u64,
    history_rows: u64,
    runtime_snapshot_fields: u64,
    turn_item_payloads: u64,
    turn_event_payloads: u64,
    turn_attempt_payloads: u64,
}

impl MigrationContext {
    fn resolve_workspace_history_identity(
        &mut self,
        workspace_id: &str,
        legacy_locator: &str,
        source_kind: &str,
    ) -> SkillId {
        let current = exact_candidate(self.candidates.iter().filter(|candidate| {
            candidate.source_kind == source_kind
                && candidate.legacy_locator == legacy_locator
                && candidate.visible_in_workspace(workspace_id)
        }));
        if let Some(skill_id) = current {
            return skill_id;
        }
        self.resolve_historical_group(
            HistoricalContext::Workspace(workspace_id.to_owned()),
            legacy_locator,
            source_kind,
        )
    }

    fn resolve_history_identity(
        &mut self,
        turn_id: Option<&str>,
        legacy_locator: &str,
        source_kind: &str,
    ) -> SkillId {
        let workspace_id = turn_id.and_then(|turn_id| self.turn_workspaces.get(turn_id));
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

        let context = match (workspace_id, turn_id) {
            (Some(workspace_id), _) => HistoricalContext::Workspace(workspace_id.clone()),
            (None, Some(turn_id)) => HistoricalContext::Turn(turn_id.to_owned()),
            (None, None) => HistoricalContext::Global,
        };
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

        let skill_id = self.fresh_historical_skill_id();
        self.historical_ids.insert(group, skill_id.clone());
        skill_id
    }

    fn fresh_historical_skill_id(&self) -> SkillId {
        loop {
            let skill_id = SkillId::new(pioneer_protocol::generate_id(21))
                .expect("the shared 21-character ID generator must create a valid SkillId");
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistorySurface {
    Binding,
    Audit,
    Dependency,
}

#[derive(Debug)]
struct LegacyHistoryIdentity {
    surface: HistorySurface,
    row_id: String,
    turn_id: Option<String>,
    skill_slug: String,
    source_kind: String,
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

#[derive(Debug, serde::Serialize)]
struct StoredWorkspaceSkillPolicy {
    skill_id: SkillId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    allow_implicit_invocation: Option<bool>,
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

#[derive(Debug)]
struct LegacyInstallationIdentity {
    id: String,
    slug: String,
    source_kind: String,
    scope_key: String,
}

#[derive(Debug)]
struct LegacyPolicyIdentity {
    id: String,
    workspace_id: String,
    skill_slug: String,
    source_kind: String,
}

async fn migrate_installations_and_policies(
    manager: &SchemaManager<'_>,
) -> Result<MigrationContext, DbErr> {
    let db = manager.get_connection();
    let installations = load_legacy_installations(db).await?;
    let mut candidates = Vec::with_capacity(installations.len() + 4);

    for installation in &installations {
        let skill_id = validate_skill_id(installation.id.as_str(), "skill_installation.id")?;
        let (owner, slug) = split_legacy_locator(installation.slug.as_str());
        candidates.push(SkillCandidate {
            skill_id,
            owner,
            slug,
            legacy_locator: installation.slug.clone(),
            source_kind: installation.source_kind.clone(),
            scope_key: Some(installation.scope_key.clone()),
            bundled: false,
        });
    }

    let active_installations = installations.len() as u64;
    let bundled_candidates = load_bundled_candidates()?;
    let bundled_candidate_count = bundled_candidates.len() as u64;
    candidates.extend(bundled_candidates);
    ensure_unique_candidate_ids(candidates.as_slice())?;

    create_core_shadow_tables(manager).await?;
    let copied_installations = copy_installations(db).await?;
    let policies = copy_mapped_policies(db, candidates.as_slice()).await?;
    if copied_installations != active_installations {
        return Err(DbErr::Migration(format!(
            "installation copy count changed: expected {active_installations}, copied {copied_installations}"
        )));
    }

    Ok(MigrationContext {
        candidates,
        turn_workspaces: HashMap::new(),
        historical_ids: HashMap::new(),
        counts: MigrationCounts {
            active_installations,
            bundled_candidates: bundled_candidate_count,
            policies,
            ..MigrationCounts::default()
        },
    })
}

async fn load_legacy_installations(
    db: &impl ConnectionTrait,
) -> Result<Vec<LegacyInstallationIdentity>, DbErr> {
    let statement = Statement::from_string(
        db.get_database_backend(),
        "SELECT id, slug, source_kind, scope_key FROM skill_installation ORDER BY id".to_owned(),
    );
    db.query_all_raw(statement)
        .await?
        .into_iter()
        .map(|row| {
            Ok(LegacyInstallationIdentity {
                id: row.try_get("", "id")?,
                slug: row.try_get("", "slug")?,
                source_kind: row.try_get("", "source_kind")?,
                scope_key: row.try_get("", "scope_key")?,
            })
        })
        .collect()
}

async fn load_legacy_policies(
    db: &impl ConnectionTrait,
) -> Result<Vec<LegacyPolicyIdentity>, DbErr> {
    let statement = Statement::from_string(
        db.get_database_backend(),
        "SELECT id, workspace_id, skill_slug, source_kind FROM skill_workspace_policy ORDER BY id"
            .to_owned(),
    );
    db.query_all_raw(statement)
        .await?
        .into_iter()
        .map(|row| {
            Ok(LegacyPolicyIdentity {
                id: row.try_get("", "id")?,
                workspace_id: row.try_get("", "workspace_id")?,
                skill_slug: row.try_get("", "skill_slug")?,
                source_kind: row.try_get("", "source_kind")?,
            })
        })
        .collect()
}

fn validate_skill_id(value: &str, field: &str) -> Result<SkillId, DbErr> {
    SkillId::new(value.to_owned())
        .map_err(|error| DbErr::Migration(format!("invalid {field} `{value}`: {error}")))
}

fn split_legacy_locator(locator: &str) -> (Option<String>, String) {
    let Some((owner, slug)) = locator.split_once('/') else {
        return (None, locator.to_owned());
    };
    let owner = (!owner.is_empty()).then(|| owner.to_owned());
    (owner, slug.to_owned())
}

fn load_bundled_candidates() -> Result<Vec<SkillCandidate>, DbErr> {
    let manifest: BundledManifest = toml::from_str(BUNDLED_MANIFEST_BYTES)
        .map_err(|error| DbErr::Migration(format!("invalid bundled skills manifest: {error}")))?;
    if manifest.version != 1 {
        return Err(DbErr::Migration(format!(
            "unsupported bundled skills manifest version {}",
            manifest.version
        )));
    }

    let mut resource_paths = HashSet::new();
    let mut candidates = Vec::with_capacity(manifest.skills.len());
    for entry in manifest.skills {
        let skill_id = validate_skill_id(entry.skill_id.as_str(), "bundled skill_id")?;
        if entry.owner.is_empty() || entry.slug.is_empty() {
            return Err(DbErr::Migration(
                "bundled owner and slug must be non-empty".to_owned(),
            ));
        }
        if entry.resource_path.starts_with('/')
            || entry.resource_path.split('/').any(|part| part == "..")
            || !resource_paths.insert(entry.resource_path.clone())
        {
            return Err(DbErr::Migration(format!(
                "invalid or duplicate bundled resource path `{}`",
                entry.resource_path
            )));
        }
        candidates.push(SkillCandidate {
            skill_id,
            owner: Some(entry.owner.clone()),
            slug: entry.slug.clone(),
            legacy_locator: format!("{}/{}", entry.owner, entry.slug),
            source_kind: "system".to_owned(),
            scope_key: None,
            bundled: true,
        });
    }
    Ok(candidates)
}

fn ensure_unique_candidate_ids(candidates: &[SkillCandidate]) -> Result<(), DbErr> {
    let mut ids = HashSet::new();
    for candidate in candidates {
        if !ids.insert(candidate.skill_id.clone()) {
            return Err(DbErr::Migration(format!(
                "duplicate active or bundled SkillId `{}`",
                candidate.skill_id
            )));
        }
    }
    Ok(())
}

fn exact_policy_candidate<'a>(
    candidates: &'a [SkillCandidate],
    policy: &LegacyPolicyIdentity,
) -> Option<&'a SkillCandidate> {
    let mut matches = candidates.iter().filter(|candidate| {
        candidate.source_kind == policy.source_kind
            && candidate.legacy_locator == policy.skill_slug
            && candidate.visible_in_workspace(policy.workspace_id.as_str())
    });
    let candidate = matches.next()?;
    matches.next().is_none().then_some(candidate)
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

async fn create_core_shadow_tables(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(NEXT_INSTALLATION_TABLE))
                .col(string("id").string_len(21).primary_key())
                .col(string("owner").string_len(255).null())
                .col(string("slug").string_len(255))
                .col(string("version").string_len(64).null())
                .col(string("source_kind").string_len(32))
                .col(string("scope_key").string_len(255))
                .col(text("source_ref"))
                .col(text("install_path"))
                .col(string("trust_level").string_len(32))
                .col(string("fingerprint").string_len(128))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table(Alias::new(NEXT_POLICY_TABLE))
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("skill_id").string_len(21))
                .col(boolean("enabled").null())
                .col(boolean("allow_implicit_invocation").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager.get_connection().execute_unprepared(
        format!(
            "CREATE TEMP TABLE {POLICY_MAP_TABLE} (old_policy_id TEXT PRIMARY KEY, skill_id TEXT NOT NULL)"
        )
        .as_str(),
    )
    .await?;
    Ok(())
}

async fn copy_installations(db: &impl ConnectionTrait) -> Result<u64, DbErr> {
    let result = db
        .execute_unprepared(
            format!(
                r#"
            INSERT INTO {NEXT_INSTALLATION_TABLE} (
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
            FROM skill_installation
            "#
            )
            .as_str(),
        )
        .await?;
    Ok(result.rows_affected())
}

async fn copy_mapped_policies(
    db: &impl ConnectionTrait,
    candidates: &[SkillCandidate],
) -> Result<u64, DbErr> {
    for policy in load_legacy_policies(db).await? {
        let Some(candidate) = exact_policy_candidate(candidates, &policy) else {
            continue;
        };
        db.execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!("INSERT INTO {POLICY_MAP_TABLE} (old_policy_id, skill_id) VALUES (?, ?)"),
            [policy.id.into(), candidate.skill_id.to_string().into()],
        ))
        .await?;
    }

    let result = db
        .execute_unprepared(
            format!(
                r#"
            INSERT INTO {NEXT_POLICY_TABLE} (
                id, workspace_id, skill_id, enabled, allow_implicit_invocation,
                created_at, updated_at
            )
            SELECT id, workspace_id, skill_id, enabled, allow_implicit_invocation,
                   created_at, updated_at
            FROM (
                SELECT p.id, p.workspace_id, m.skill_id, p.enabled,
                       p.allow_implicit_invocation, p.created_at, p.updated_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY p.workspace_id, m.skill_id
                           ORDER BY p.updated_at DESC, p.id DESC
                       ) AS duplicate_rank
                FROM skill_workspace_policy p
                JOIN {POLICY_MAP_TABLE} m ON m.old_policy_id = p.id
            ) ranked
            WHERE duplicate_rank = 1
            "#
            )
            .as_str(),
        )
        .await?;
    Ok(result.rows_affected())
}

async fn migrate_history_surfaces(
    manager: &SchemaManager<'_>,
    context: &mut MigrationContext,
) -> Result<(), DbErr> {
    let db = manager.get_connection();
    context.turn_workspaces = load_turn_workspaces(db).await?;
    create_history_shadow_tables(manager).await?;

    for identity in load_legacy_history_identities(db).await? {
        let skill_id = context.resolve_history_identity(
            identity.turn_id.as_deref(),
            identity.skill_slug.as_str(),
            identity.source_kind.as_str(),
        );
        copy_history_row(db, &identity, &skill_id).await?;
        context.counts.history_rows += 1;
    }
    Ok(())
}

async fn load_turn_workspaces(db: &impl ConnectionTrait) -> Result<HashMap<String, String>, DbErr> {
    let rows = db
        .query_all_raw(Statement::from_string(
            db.get_database_backend(),
            r#"
            SELECT t.id AS turn_id, w.id AS workspace_id
            FROM turn t
            JOIN thread th ON th.id = t.thread_id
            JOIN workspace w ON w.id = th.workspace_id
            "#
            .to_owned(),
        ))
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("", "turn_id")?,
                row.try_get("", "workspace_id")?,
            ))
        })
        .collect()
}

async fn load_legacy_history_identities(
    db: &impl ConnectionTrait,
) -> Result<Vec<LegacyHistoryIdentity>, DbErr> {
    let mut identities = Vec::new();
    for (surface, table) in [
        (HistorySurface::Binding, "turn_skill_binding"),
        (HistorySurface::Audit, "skill_audit_event"),
        (HistorySurface::Dependency, "skill_dependency_snapshot"),
    ] {
        let rows = db
            .query_all_raw(Statement::from_string(
                db.get_database_backend(),
                format!("SELECT id, turn_id, skill_slug, source_kind FROM {table} ORDER BY id"),
            ))
            .await?;
        for row in rows {
            identities.push(LegacyHistoryIdentity {
                surface,
                row_id: row.try_get("", "id")?,
                turn_id: row.try_get("", "turn_id")?,
                skill_slug: row.try_get("", "skill_slug")?,
                source_kind: row.try_get("", "source_kind")?,
            });
        }
    }
    Ok(identities)
}

async fn create_history_shadow_tables(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(NEXT_BINDING_TABLE))
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21))
                .col(string("skill_id").string_len(21))
                .col(string("skill_owner").string_len(255).null())
                .col(string("skill_slug").string_len(255))
                .col(string("skill_version").string_len(64).null())
                .col(string("fingerprint").string_len(128))
                .col(string("source_kind").string_len(32))
                .col(string("resolved_reason").string_len(32))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table(Alias::new(NEXT_AUDIT_TABLE))
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21).null())
                .col(string("skill_id").string_len(21))
                .col(string("skill_owner").string_len(255).null())
                .col(string("skill_slug").string_len(255))
                .col(string("source_kind").string_len(32))
                .col(string("action").string_len(64))
                .col(string("decision").string_len(32))
                .col(string("reason_code").string_len(128).null())
                .col(text("details_json").default("{}"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table(Alias::new(NEXT_DEPENDENCY_TABLE))
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21).null())
                .col(string("skill_id").string_len(21))
                .col(string("skill_owner").string_len(255).null())
                .col(string("skill_slug").string_len(255))
                .col(string("source_kind").string_len(32))
                .col(text("diagnostics_json").default("[]"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

async fn copy_history_row(
    db: &impl ConnectionTrait,
    identity: &LegacyHistoryIdentity,
    skill_id: &SkillId,
) -> Result<(), DbErr> {
    let (owner, slug) = split_legacy_locator(identity.skill_slug.as_str());
    let sql = match identity.surface {
        HistorySurface::Binding => format!(
            r#"
            INSERT INTO {NEXT_BINDING_TABLE} (
                id, turn_id, skill_id, skill_owner, skill_slug, skill_version,
                fingerprint, source_kind, resolved_reason, created_at
            )
            SELECT id, turn_id, ?, ?, ?, skill_version, fingerprint, source_kind,
                   resolved_reason, created_at
            FROM turn_skill_binding WHERE id = ?
            "#
        ),
        HistorySurface::Audit => format!(
            r#"
            INSERT INTO {NEXT_AUDIT_TABLE} (
                id, turn_id, skill_id, skill_owner, skill_slug, source_kind,
                action, decision, reason_code, details_json, created_at
            )
            SELECT id, turn_id, ?, ?, ?, source_kind, action, decision,
                   reason_code, details_json, created_at
            FROM skill_audit_event WHERE id = ?
            "#
        ),
        HistorySurface::Dependency => format!(
            r#"
            INSERT INTO {NEXT_DEPENDENCY_TABLE} (
                id, turn_id, skill_id, skill_owner, skill_slug, source_kind,
                diagnostics_json, created_at
            )
            SELECT id, turn_id, ?, ?, ?, source_kind, diagnostics_json, created_at
            FROM skill_dependency_snapshot WHERE id = ?
            "#
        ),
    };
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            vec![
                skill_id.to_string().into(),
                owner.into(),
                slug.into(),
                identity.row_id.clone().into(),
            ],
        ))
        .await?;
    if result.rows_affected() != 1 {
        return Err(DbErr::Migration(format!(
            "history row `{}` disappeared while migrating {:?}",
            identity.row_id, identity.surface
        )));
    }
    Ok(())
}

async fn migrate_typed_json_surfaces(
    manager: &SchemaManager<'_>,
    context: &mut MigrationContext,
) -> Result<(), DbErr> {
    let db = manager.get_connection();
    migrate_runtime_snapshot_json(db, context).await?;
    migrate_turn_item_json_rows(db, context).await?;
    migrate_turn_event_json_rows(db, context).await?;
    migrate_turn_attempt_json_rows(db, context).await?;
    Ok(())
}

fn json_migration_error(
    table: &str,
    row_id: &str,
    field: &str,
    error: impl std::fmt::Display,
) -> DbErr {
    DbErr::Migration(format!(
        "failed to migrate {table} row `{row_id}` field `{field}`: {error}"
    ))
}

fn json_shape_error(message: &str) -> serde_json::Error {
    <serde_json::Error as serde::de::Error>::custom(message)
}

async fn migrate_runtime_snapshot_json(
    db: &impl ConnectionTrait,
    context: &mut MigrationContext,
) -> Result<(), DbErr> {
    let rows = db
        .query_all_raw(Statement::from_string(
            db.get_database_backend(),
            "SELECT turn_id, workspace_id, workspace_skill_policies_json, capabilities_json FROM turn_runtime_snapshot ORDER BY turn_id".to_owned(),
        ))
        .await?;
    for row in rows {
        let turn_id: String = row.try_get("", "turn_id")?;
        let workspace_id: String = row.try_get("", "workspace_id")?;
        let policies_json: String = row.try_get("", "workspace_skill_policies_json")?;
        let capabilities_json: String = row.try_get("", "capabilities_json")?;
        let policies = migrate_workspace_policy_snapshot(
            policies_json.as_str(),
            workspace_id.as_str(),
            context,
        )
        .map_err(|error| {
            json_migration_error(
                "turn_runtime_snapshot",
                turn_id.as_str(),
                "workspace_skill_policies_json",
                error,
            )
        })?;
        let capabilities =
            migrate_capability_snapshot(capabilities_json.as_str(), workspace_id.as_str(), context)
                .map_err(|error| {
                    json_migration_error(
                        "turn_runtime_snapshot",
                        turn_id.as_str(),
                        "capabilities_json",
                        error,
                    )
                })?;
        update_json_field(
            db,
            "turn_runtime_snapshot",
            "turn_id",
            turn_id.as_str(),
            "workspace_skill_policies_json",
            policies,
        )
        .await?;
        update_json_field(
            db,
            "turn_runtime_snapshot",
            "turn_id",
            turn_id.as_str(),
            "capabilities_json",
            capabilities,
        )
        .await?;
        context.counts.runtime_snapshot_fields += 2;
    }
    Ok(())
}

fn migrate_workspace_policy_snapshot(
    raw: &str,
    workspace_id: &str,
    context: &mut MigrationContext,
) -> Result<String, serde_json::Error> {
    let legacy: Vec<LegacyWorkspaceSkillPolicy> = serde_json::from_str(raw)?;
    let final_policies = legacy
        .into_iter()
        .map(|policy| {
            let skill_id = context.resolve_workspace_history_identity(
                workspace_id,
                policy.skill_slug.as_str(),
                policy.source_kind.as_str(),
            );
            StoredWorkspaceSkillPolicy {
                skill_id,
                enabled: policy.enabled,
                allow_implicit_invocation: policy.allow_implicit_invocation,
            }
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&final_policies)
}

fn migrate_capability_snapshot(
    raw: &str,
    workspace_id: &str,
    context: &mut MigrationContext,
) -> Result<String, serde_json::Error> {
    let legacy: Vec<LegacyTurnCapability> = serde_json::from_str(raw)?;
    let final_capabilities = legacy
        .into_iter()
        .map(|capability| {
            let kind = if capability.kind.get("type").and_then(JsonValue::as_str) == Some("skill") {
                let LegacySkillCapabilityKind::Skill { slug, source_kind } =
                    serde_json::from_value(capability.kind)?;
                let skill_id = context.resolve_workspace_history_identity(
                    workspace_id,
                    slug.as_str(),
                    source_kind.as_str(),
                );
                pioneer_protocol::TurnCapabilityKind::Skill { skill_id }
            } else {
                serde_json::from_value(capability.kind)?
            };
            let id = match &kind {
                pioneer_protocol::TurnCapabilityKind::Skill { skill_id } => {
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

async fn migrate_turn_item_json_rows(
    db: &impl ConnectionTrait,
    context: &mut MigrationContext,
) -> Result<(), DbErr> {
    let rows = load_payload_rows(db, "turn_item").await?;
    for (row_id, turn_id, payload) in rows {
        let value = parse_json_value("turn_item", row_id.as_str(), "payload", payload.as_str())?;
        let migrated =
            migrate_turn_item_value(value, turn_id.as_str(), context).map_err(|error| {
                json_migration_error("turn_item", row_id.as_str(), "payload", error)
            })?;
        let encoded = serde_json::to_string(&migrated).map_err(|error| {
            json_migration_error("turn_item", row_id.as_str(), "payload", error)
        })?;
        update_json_field(db, "turn_item", "id", row_id.as_str(), "payload", encoded).await?;
        context.counts.turn_item_payloads += 1;
    }
    Ok(())
}

async fn migrate_turn_event_json_rows(
    db: &impl ConnectionTrait,
    context: &mut MigrationContext,
) -> Result<(), DbErr> {
    let rows = load_payload_rows(db, "turn_event").await?;
    for (row_id, turn_id, payload) in rows {
        let migrated = migrate_turn_event_payload(payload.as_str(), turn_id.as_str(), context)
            .map_err(|error| {
                json_migration_error("turn_event", row_id.as_str(), "payload", error)
            })?;
        if let Some(migrated) = migrated {
            update_json_field(db, "turn_event", "id", row_id.as_str(), "payload", migrated).await?;
            context.counts.turn_event_payloads += 1;
        }
    }
    Ok(())
}

async fn migrate_turn_attempt_json_rows(
    db: &impl ConnectionTrait,
    context: &mut MigrationContext,
) -> Result<(), DbErr> {
    let rows = load_payload_rows(db, "turn_item_attempt").await?;
    for (row_id, turn_id, payload) in rows {
        let parsed = parse_json_value(
            "turn_item_attempt",
            row_id.as_str(),
            "payload",
            payload.as_str(),
        )?;
        if parsed.as_object().is_some_and(serde_json::Map::is_empty) {
            continue;
        }
        let migrated =
            migrate_turn_item_value(parsed, turn_id.as_str(), context).map_err(|error| {
                json_migration_error("turn_item_attempt", row_id.as_str(), "payload", error)
            })?;
        let encoded = serde_json::to_string(&migrated).map_err(|error| {
            json_migration_error("turn_item_attempt", row_id.as_str(), "payload", error)
        })?;
        update_json_field(
            db,
            "turn_item_attempt",
            "id",
            row_id.as_str(),
            "payload",
            encoded,
        )
        .await?;
        context.counts.turn_attempt_payloads += 1;
    }
    Ok(())
}

async fn load_payload_rows(
    db: &impl ConnectionTrait,
    table: &str,
) -> Result<Vec<(String, String, String)>, DbErr> {
    db.query_all_raw(Statement::from_string(
        db.get_database_backend(),
        format!("SELECT id, turn_id, payload FROM {table} ORDER BY id"),
    ))
    .await?
    .into_iter()
    .map(|row| {
        Ok((
            row.try_get("", "id")?,
            row.try_get("", "turn_id")?,
            row.try_get("", "payload")?,
        ))
    })
    .collect()
}

fn parse_json_value(table: &str, row_id: &str, field: &str, raw: &str) -> Result<JsonValue, DbErr> {
    serde_json::from_str(raw).map_err(|error| json_migration_error(table, row_id, field, error))
}

fn migrate_turn_event_payload(
    raw: &str,
    persisted_turn_id: &str,
    context: &mut MigrationContext,
) -> Result<Option<String>, serde_json::Error> {
    let mut value: JsonValue = serde_json::from_str(raw)?;
    let kind = value
        .get("kind")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    if !matches!(kind, "item_started" | "item_completed" | "item_updated") {
        return Ok(None);
    }

    let legacy: LegacyItemEventPayload =
        serde_json::from_value(value.get("payload").cloned().unwrap_or(JsonValue::Null))?;
    let migrated_item = migrate_turn_item_value(legacy.item, persisted_turn_id, context)?;
    let payload = value
        .get_mut("payload")
        .and_then(JsonValue::as_object_mut)
        .ok_or_else(|| json_shape_error("event payload is not an object"))?;
    payload.insert("item".to_owned(), migrated_item);
    serde_json::to_string(&value).map(Some)
}

fn migrate_turn_item_value(
    mut value: JsonValue,
    turn_id: &str,
    context: &mut MigrationContext,
) -> Result<JsonValue, serde_json::Error> {
    if value.get("type").and_then(JsonValue::as_str) == Some("userMessage") {
        if let Some(attachments) = value.get_mut("attachments") {
            let attachments = attachments
                .as_array_mut()
                .ok_or_else(|| json_shape_error("attachments is not an array"))?;
            for attachment in attachments {
                if attachment.get("type").and_then(JsonValue::as_str) != Some("skill") {
                    continue;
                }
                let legacy: LegacySkillAttachmentCapability = serde_json::from_value(
                    attachment
                        .get("capability")
                        .cloned()
                        .unwrap_or(JsonValue::Null),
                )?;
                let skill_id = context.resolve_history_identity(
                    Some(turn_id),
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
                };
                let object = attachment
                    .as_object_mut()
                    .ok_or_else(|| json_shape_error("attachment is not an object"))?;
                object.insert(
                    "capability".to_owned(),
                    serde_json::to_value(final_summary)?,
                );
            }
        }
    }

    let typed: pioneer_protocol::TurnItem = serde_json::from_value(value)?;
    serde_json::to_value(typed)
}

async fn update_json_field(
    db: &impl ConnectionTrait,
    table: &str,
    id_column: &str,
    row_id: &str,
    field: &str,
    value: String,
) -> Result<(), DbErr> {
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!("UPDATE {table} SET {field} = ? WHERE {id_column} = ?"),
            [value.clone().into(), row_id.to_owned().into()],
        ))
        .await?;

    match result.rows_affected() {
        1 => return Ok(()),
        0 => {}
        rows_affected => {
            return Err(DbErr::Migration(format!(
                "typed JSON update for row `{row_id}` unexpectedly affected {rows_affected} rows in {table}.{field}"
            )));
        }
    }

    // sqlite-zstd exposes compressed tables as views backed by INSTEAD OF
    // triggers. SQLite reports zero affected rows for a successful update on
    // such a view, so verify the persisted value instead of treating that
    // count as proof that the row disappeared.
    let persisted = db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!("SELECT {field} AS migrated_value FROM {table} WHERE {id_column} = ?"),
            [row_id.to_owned().into()],
        ))
        .await?;
    let Some(persisted) = persisted else {
        return Err(DbErr::Migration(format!(
            "typed JSON row `{row_id}` disappeared while updating {table}.{field}"
        )));
    };
    let persisted: String = persisted.try_get("", "migrated_value")?;
    if persisted != value {
        return Err(DbErr::Migration(format!(
            "typed JSON update for row `{row_id}` was not persisted in {table}.{field}"
        )));
    }
    Ok(())
}

async fn finish_core_table_swap(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let db = manager.get_connection();
    for (old_table, next_table) in [
        ("turn_skill_binding", NEXT_BINDING_TABLE),
        ("skill_audit_event", NEXT_AUDIT_TABLE),
        ("skill_dependency_snapshot", NEXT_DEPENDENCY_TABLE),
    ] {
        db.execute_unprepared(format!("DROP TABLE {old_table}").as_str())
            .await?;
        db.execute_unprepared(format!("ALTER TABLE {next_table} RENAME TO {old_table}").as_str())
            .await?;
    }
    db.execute_unprepared(format!("DROP TABLE {POLICY_MAP_TABLE}").as_str())
        .await?;
    db.execute_unprepared("DROP TABLE skill_workspace_policy")
        .await?;
    db.execute_unprepared(
        format!("ALTER TABLE {NEXT_POLICY_TABLE} RENAME TO skill_workspace_policy").as_str(),
    )
    .await?;
    db.execute_unprepared("DROP TABLE skill_installation")
        .await?;
    db.execute_unprepared(
        format!("ALTER TABLE {NEXT_INSTALLATION_TABLE} RENAME TO skill_installation").as_str(),
    )
    .await?;

    for sql in [
        "CREATE INDEX idx_turn_skill_binding_turn_id ON turn_skill_binding(turn_id)",
        "CREATE UNIQUE INDEX uq_turn_skill_binding_turn_id_skill_id ON turn_skill_binding(turn_id, skill_id)",
        "CREATE INDEX idx_skill_audit_event_skill_id_created_at ON skill_audit_event(skill_id, created_at)",
        "CREATE INDEX idx_skill_audit_event_turn_id ON skill_audit_event(turn_id)",
        "CREATE INDEX idx_skill_dependency_snapshot_skill_id_created_at ON skill_dependency_snapshot(skill_id, created_at)",
        "CREATE INDEX idx_skill_dependency_snapshot_turn_id ON skill_dependency_snapshot(turn_id)",
        "CREATE INDEX idx_skill_installation_scope_source ON skill_installation(scope_key, source_kind)",
        "CREATE INDEX idx_skill_installation_source_ref ON skill_installation(source_ref)",
        "CREATE INDEX idx_skill_workspace_policy_workspace ON skill_workspace_policy(workspace_id)",
        "CREATE UNIQUE INDEX uq_skill_workspace_policy_workspace_skill_id ON skill_workspace_policy(workspace_id, skill_id)",
    ] {
        db.execute_unprepared(sql).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Migrator, MigratorTrait};
    use sea_orm_migration::sea_orm::{Database, DatabaseConnection, TransactionTrait};

    const FIXTURE_WORKSPACE_ID: &str = "WWWWWWWWWWWWWWWWWWWWW";
    const FIXTURE_THREAD_ID: &str = "TTTTTTTTTTTTTTTTTTTTT";
    const FIXTURE_TURN_ID: &str = "UUUUUUUUUUUUUUUUUUUUU";

    async fn apply_pre_stable_migrations(db: &DatabaseConnection) {
        let migration_count = Migrator::migrations().len();
        Migrator::up(db, Some((migration_count - 1) as u32))
            .await
            .expect("pre-Stable-SkillId migrations should apply");
    }

    async fn enable_and_compress_identity_payload_views(db: &DatabaseConnection) {
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
        .unwrap_or_else(|error| panic!("{table} backing row should query: {error}"))
        .unwrap_or_else(|| panic!("{table} backing row `{row_id}` should exist"))
        .try_get("", "dict_id")
        .unwrap_or_else(|error| panic!("{table} dictionary id should decode: {error}"))
    }

    async fn seed_minimal_turn_and_installation(db: &impl ConnectionTrait) {
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
            INSERT INTO skill_installation
                (id, slug, version, source_kind, scope_key, source_ref, install_path,
                 trust_level, fingerprint)
                VALUES ('AAAAAAAAAAAAAAAAAAAAA', 'owner/current', '1.0', 'user',
                        'WWWWWWWWWWWWWWWWWWWWW', 'source-a', '/legacy/a',
                        'community', 'fp-a');
            "#,
        )
        .await
        .expect("minimal turn and installation should insert");
    }

    const UNCHANGED_RUNTIME_INPUT: &str =
        r#"[ { "type" : "text", "text" : "runtime input stays byte-identical" } ]"#;
    const UNCHANGED_TURN_INPUT: &str = r#"{ "path" : "turn input stays byte-identical" }"#;
    const UNCHANGED_TURN_STARTED_EVENT: &str = r#"{ "kind" : "turn_started", "payload" : { "input" : [ { "type" : "text", "text" : "unchanged" } ], "marker" : true } }"#;

    async fn seed_identity_json_surfaces(db: &impl ConnectionTrait) {
        let legacy_item = r#"{"type":"userMessage","id":"message-1","text":"hello","attachments":[{"type":"skill","capability":{"id":"skill:user:owner/current","label":"Current","slug":"owner/current","sourceKind":"user"}}]}"#;
        let legacy_event = format!(
            r#"{{"kind":"item_started","payload":{{"workspace_id":"{FIXTURE_WORKSPACE_ID}","thread_id":"{FIXTURE_THREAD_ID}","turn_id":"{FIXTURE_TURN_ID}","item":{legacy_item}}}}}"#
        );
        let policies = r#"[{"skill_slug":"owner/current","source_kind":"user","enabled":true,"allow_implicit_invocation":false}]"#;
        let capabilities = r#"[{"id":"skill:user:owner/current","label":"Current","kind":{"type":"skill","slug":"owner/current","sourceKind":"user"}}]"#;

        db.execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            r#"
            INSERT INTO turn_runtime_snapshot (
                turn_id, thread_id, workspace_id, mode_json, model, provider_name,
                hook_runtime_context_json, workspace_skill_policies_json, input_json,
                capabilities_json, resolved_artifacts_json, runtime_environment_json,
                history_json
            ) VALUES (?, ?, ?, '{}', 'model', 'provider', '{}', ?, ?, ?, '[]', '{}', '[]')
            "#,
            vec![
                FIXTURE_TURN_ID.to_owned().into(),
                FIXTURE_THREAD_ID.to_owned().into(),
                FIXTURE_WORKSPACE_ID.to_owned().into(),
                policies.to_owned().into(),
                UNCHANGED_RUNTIME_INPUT.to_owned().into(),
                capabilities.to_owned().into(),
            ],
        ))
        .await
        .expect("runtime snapshot should insert");
        db.execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "INSERT INTO turn_input (id, turn_id, input_index, input_type, payload) VALUES (?, ?, 0, 'text', ?)",
            [
                "IIIIIIIIIIIIIIIIIIIII".to_owned().into(),
                FIXTURE_TURN_ID.to_owned().into(),
                UNCHANGED_TURN_INPUT.to_owned().into(),
            ],
        ))
        .await
        .expect("turn input should insert");
        db.execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "INSERT INTO turn_item (id, turn_id, item_id, item_type, payload) VALUES (?, ?, 'message-1', 'user_message', ?)",
            [
                "JJJJJJJJJJJJJJJJJJJJJ".to_owned().into(),
                FIXTURE_TURN_ID.to_owned().into(),
                legacy_item.to_owned().into(),
            ],
        ))
        .await
        .expect("turn item should insert");
        for (id, attempt_number, payload) in [
            ("KKKKKKKKKKKKKKKKKKKKK", 1_i32, legacy_item),
            ("LLLLLLLLLLLLLLLLLLLLL", 2_i32, "{}"),
        ] {
            db.execute_raw(Statement::from_sql_and_values(
                db.get_database_backend(),
                "INSERT INTO turn_item_attempt (id, turn_id, item_id, item_type, attempt_number, status, payload) VALUES (?, ?, 'message-1', 'user_message', ?, 'running', ?)",
                vec![
                    id.to_owned().into(),
                    FIXTURE_TURN_ID.to_owned().into(),
                    attempt_number.into(),
                    payload.to_owned().into(),
                ],
            ))
            .await
            .expect("turn item attempt should insert");
        }
        for (id, sequence, event_type, payload) in [
            (
                "MMMMMMMMMMMMMMMMMMMMM",
                1_i64,
                "item_started",
                legacy_event.as_str(),
            ),
            (
                "NNNNNNNNNNNNNNNNNNNNN",
                2_i64,
                "turn_started",
                UNCHANGED_TURN_STARTED_EVENT,
            ),
        ] {
            db.execute_raw(Statement::from_sql_and_values(
                db.get_database_backend(),
                "INSERT INTO turn_event (id, thread_id, turn_id, sequence, event_type, payload) VALUES (?, ?, ?, ?, ?, ?)",
                vec![
                    id.to_owned().into(),
                    FIXTURE_THREAD_ID.to_owned().into(),
                    FIXTURE_TURN_ID.to_owned().into(),
                    sequence.into(),
                    event_type.to_owned().into(),
                    payload.to_owned().into(),
                ],
            ))
            .await
            .expect("turn event should insert");
        }
    }

    async fn create_legacy_core_tables(db: &impl ConnectionTrait) {
        db.execute_unprepared(
            r#"
            CREATE TABLE skill_installation (
                id TEXT PRIMARY KEY,
                slug TEXT NOT NULL,
                version TEXT NULL,
                source_kind TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                install_path TEXT NOT NULL,
                trust_level TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX uq_skill_installation_slug_source_scope
                ON skill_installation(slug, source_kind, scope_key);
            CREATE TABLE skill_workspace_policy (
                id TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                skill_slug TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                enabled INTEGER NULL,
                allow_implicit_invocation INTEGER NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            "#,
        )
        .await
        .expect("legacy core tables should be created");
    }

    async fn create_turn_and_legacy_history_tables(db: &impl ConnectionTrait) {
        db.execute_unprepared(
            r#"
            CREATE TABLE workspace (id TEXT PRIMARY KEY);
            CREATE TABLE thread (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL);
            CREATE TABLE turn (id TEXT PRIMARY KEY, thread_id TEXT NOT NULL);
            CREATE TABLE turn_skill_binding (
                id TEXT PRIMARY KEY,
                turn_id TEXT NOT NULL,
                skill_slug TEXT NOT NULL,
                skill_version TEXT NULL,
                fingerprint TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                resolved_reason TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX uq_turn_skill_binding_turn_id_skill_source
                ON turn_skill_binding(turn_id, skill_slug, source_kind);
            CREATE TABLE skill_audit_event (
                id TEXT PRIMARY KEY,
                turn_id TEXT NULL,
                skill_slug TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                action TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason_code TEXT NULL,
                details_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE skill_dependency_snapshot (
                id TEXT PRIMARY KEY,
                turn_id TEXT NULL,
                skill_slug TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                diagnostics_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            "#,
        )
        .await
        .expect("legacy history tables should be created");
    }

    fn candidate(
        id: char,
        locator: &str,
        source_kind: &str,
        scope_key: Option<&str>,
        bundled: bool,
    ) -> SkillCandidate {
        let (owner, slug) = split_legacy_locator(locator);
        SkillCandidate {
            skill_id: SkillId::new(id.to_string().repeat(21)).expect("valid skill id"),
            owner,
            slug,
            legacy_locator: locator.to_owned(),
            source_kind: source_kind.to_owned(),
            scope_key: scope_key.map(str::to_owned),
            bundled,
        }
    }

    fn policy(workspace_id: &str, locator: &str, source_kind: &str) -> LegacyPolicyIdentity {
        LegacyPolicyIdentity {
            id: "policy-id".to_owned(),
            workspace_id: workspace_id.to_owned(),
            skill_slug: locator.to_owned(),
            source_kind: source_kind.to_owned(),
        }
    }

    #[test]
    fn legacy_locator_splits_only_on_first_slash() {
        assert_eq!(
            split_legacy_locator("aradotso/humanizer"),
            (Some("aradotso".to_owned()), "humanizer".to_owned())
        );
        assert_eq!(
            split_legacy_locator("humanizer"),
            (None, "humanizer".to_owned())
        );
        assert_eq!(
            split_legacy_locator("owner/nested/leaf"),
            (Some("owner".to_owned()), "nested/leaf".to_owned())
        );
    }

    #[test]
    fn bundled_manifest_is_the_single_valid_fixed_id_source() {
        let candidates = load_bundled_candidates().expect("bundled manifest should parse");
        assert_eq!(candidates.len(), 4);
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.slug.as_str())
                .collect::<Vec<_>>(),
            vec!["browser", "memory", "subagents", "tasks"]
        );
        assert!(candidates.iter().all(|candidate| candidate.bundled));
        ensure_unique_candidate_ids(candidates.as_slice()).expect("bundled IDs must be unique");
    }

    #[test]
    fn policy_mapping_requires_one_exact_visible_candidate() {
        let bundled = candidate('A', "pioneer/browser", "system", None, true);
        assert_eq!(
            exact_policy_candidate(
                std::slice::from_ref(&bundled),
                &policy("workspace-a", "pioneer/browser", "system")
            ),
            Some(&bundled)
        );

        let workspace_a = candidate('B', "pioneer/duplicate", "user", Some("workspace-a"), false);
        let workspace_b = candidate('C', "pioneer/duplicate", "user", Some("workspace-b"), false);
        let candidates = vec![workspace_a.clone(), workspace_b];
        assert_eq!(
            exact_policy_candidate(
                candidates.as_slice(),
                &policy("workspace-a", "pioneer/duplicate", "user")
            ),
            Some(&workspace_a)
        );

        let ambiguous = vec![workspace_a.clone(), workspace_a];
        assert!(
            exact_policy_candidate(
                ambiguous.as_slice(),
                &policy("workspace-a", "pioneer/duplicate", "user")
            )
            .is_none()
        );
        assert!(
            exact_policy_candidate(
                candidates.as_slice(),
                &policy("workspace-a", "missing", "user")
            )
            .is_none()
        );
    }

    #[test]
    fn invalid_existing_id_is_not_replaced() {
        assert!(validate_skill_id("too-short", "skill_installation.id").is_err());
    }

    #[test]
    fn history_resolution_reuses_exact_ids_and_groups_ambiguous_rows() {
        let workspace_active = candidate('A', "owner/current", "user", Some("workspace-a"), false);
        let bundled = candidate('B', "pioneer/browser", "system", None, true);
        let ambiguous_one = candidate('C', "owner/ambiguous", "user", Some("workspace-a"), false);
        let ambiguous_two = candidate('D', "owner/ambiguous", "user", Some("workspace-a"), false);
        let mut context = MigrationContext {
            candidates: vec![
                workspace_active.clone(),
                bundled.clone(),
                ambiguous_one,
                ambiguous_two,
            ],
            turn_workspaces: HashMap::from([
                ("turn-a".to_owned(), "workspace-a".to_owned()),
                ("turn-b".to_owned(), "workspace-b".to_owned()),
            ]),
            historical_ids: HashMap::new(),
            counts: MigrationCounts::default(),
        };

        assert_eq!(
            context.resolve_history_identity(Some("turn-a"), "owner/current", "user"),
            workspace_active.skill_id
        );
        assert_eq!(
            context.resolve_history_identity(Some("turn-a"), "pioneer/browser", "system"),
            bundled.skill_id
        );

        let ambiguous = context.resolve_history_identity(Some("turn-a"), "owner/ambiguous", "user");
        assert_eq!(
            ambiguous,
            context.resolve_history_identity(Some("turn-a"), "owner/ambiguous", "user")
        );
        assert_ne!(
            ambiguous,
            context.resolve_history_identity(Some("turn-b"), "owner/ambiguous", "user")
        );

        let deleted = context.resolve_history_identity(Some("turn-a"), "old/deleted", "registry");
        assert_eq!(
            deleted,
            context.resolve_history_identity(Some("turn-a"), "old/deleted", "registry")
        );
        assert_ne!(
            deleted,
            context.resolve_history_identity(None, "old/deleted", "registry")
        );
    }

    #[test]
    fn typed_json_conversion_uses_exact_id_and_preserves_presentation() {
        let active = candidate(
            'A',
            "owner/current",
            "user",
            Some(FIXTURE_WORKSPACE_ID),
            false,
        );
        let mut context = MigrationContext {
            candidates: vec![active],
            turn_workspaces: HashMap::from([(
                FIXTURE_TURN_ID.to_owned(),
                FIXTURE_WORKSPACE_ID.to_owned(),
            )]),
            historical_ids: HashMap::new(),
            counts: MigrationCounts::default(),
        };

        let policies = migrate_workspace_policy_snapshot(
            r#"[{"skill_slug":"owner/current","source_kind":"user","enabled":true}]"#,
            FIXTURE_WORKSPACE_ID,
            &mut context,
        )
        .expect("legacy policy snapshot should migrate");
        let policies: JsonValue = serde_json::from_str(policies.as_str()).unwrap();
        assert_eq!(policies[0]["skill_id"], "AAAAAAAAAAAAAAAAAAAAA");
        assert!(policies[0].get("owner").is_none());
        assert!(policies[0].get("slug").is_none());
        assert!(policies[0].get("source_kind").is_none());

        let capabilities = migrate_capability_snapshot(
            r#"[{"id":"skill:user:owner/current","label":"Current","kind":{"type":"skill","slug":"owner/current","sourceKind":"user"}}]"#,
            FIXTURE_WORKSPACE_ID,
            &mut context,
        )
        .expect("legacy capability snapshot should migrate");
        let capabilities: JsonValue = serde_json::from_str(capabilities.as_str()).unwrap();
        assert_eq!(capabilities[0]["id"], "skill:AAAAAAAAAAAAAAAAAAAAA");
        assert_eq!(capabilities[0]["kind"]["skillId"], "AAAAAAAAAAAAAAAAAAAAA");
        assert!(capabilities[0]["kind"].get("slug").is_none());

        let item: JsonValue = serde_json::from_str(
            r#"{"type":"userMessage","id":"message-1","text":"hello","attachments":[{"type":"skill","capability":{"id":"skill:user:owner/current","label":"Current","slug":"owner/current","sourceKind":"user"}}]}"#,
        )
        .unwrap();
        let item = migrate_turn_item_value(item, FIXTURE_TURN_ID, &mut context)
            .expect("legacy turn item should migrate");
        assert_eq!(
            item["attachments"][0]["capability"]["skillId"],
            "AAAAAAAAAAAAAAAAAAAAA"
        );
        assert_eq!(item["attachments"][0]["capability"]["owner"], "owner");
        assert_eq!(item["attachments"][0]["capability"]["slug"], "current");
        assert!(item["attachments"][0]["capability"].get("id").is_none());

        assert!(
            migrate_turn_event_payload(
                r#"{"kind":"turn_started","payload":{"input":[]}}"#,
                FIXTURE_TURN_ID,
                &mut context,
            )
            .expect("non-item event should remain untouched")
            .is_none()
        );
    }

    #[tokio::test]
    async fn core_fixture_preserves_ids_paths_splits_locators_and_maps_only_exact_policies() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        create_legacy_core_tables(&db).await;
        db.execute_unprepared(
            r#"
            INSERT INTO skill_installation VALUES
              ('AAAAAAAAAAAAAAAAAAAAA', 'pioneer/duplicate', '1.0', 'user', 'workspace-a',
               'source-a', '/legacy/a', 'community', 'fp-a', '2026-01-01', '2026-01-01'),
              ('BBBBBBBBBBBBBBBBBBBBB', 'pioneer/duplicate', '1.0', 'user', 'workspace-b',
               'source-b', '/legacy/b', 'community', 'fp-b', '2026-01-01', '2026-01-01'),
              ('CCCCCCCCCCCCCCCCCCCCC', 'plain', NULL, 'registry', 'workspace-a',
               'source-c', '/legacy/c', 'community', 'fp-c', '2026-01-01', '2026-01-01'),
              ('DDDDDDDDDDDDDDDDDDDDD', 'pioneer/browser', '1.0', 'system', 'global',
               'source-d', '/legacy/d', 'internal', 'fp-d', '2026-01-01', '2026-01-01');
            INSERT INTO skill_workspace_policy VALUES
              ('PPPPPPPPPPPPPPPPPPPPP', 'workspace-a', 'pioneer/duplicate', 'user', 1, 0,
               '2026-01-01', '2026-01-02'),
              ('QQQQQQQQQQQQQQQQQQQQQ', 'workspace-a', 'missing', 'user', 1, 1,
               '2026-01-01', '2026-01-02'),
              ('RRRRRRRRRRRRRRRRRRRRR', 'workspace-a', 'pioneer/browser', 'system', 1, 1,
               '2026-01-01', '2026-01-02'),
              ('SSSSSSSSSSSSSSSSSSSSS', 'workspace-a', 'pioneer/tasks', 'system', 1, 1,
               '2026-01-01', '2026-01-02');
            "#,
        )
        .await
        .expect("legacy fixture should insert");

        let transaction = db.begin().await.expect("transaction should begin");
        let manager = SchemaManager::new(&transaction);
        let context = migrate_installations_and_policies(&manager)
            .await
            .expect("core migration should succeed");

        let rows = transaction
            .query_all_raw(Statement::from_string(
                transaction.get_database_backend(),
                format!(
                    "SELECT id, owner, slug, install_path FROM {NEXT_INSTALLATION_TABLE} ORDER BY id"
                ),
            ))
            .await
            .expect("migrated installations should query");
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows[0].try_get::<String>("", "id").unwrap(),
            "AAAAAAAAAAAAAAAAAAAAA"
        );
        assert_eq!(rows[0].try_get::<String>("", "owner").unwrap(), "pioneer");
        assert_eq!(rows[0].try_get::<String>("", "slug").unwrap(), "duplicate");
        assert_eq!(
            rows[0].try_get::<String>("", "install_path").unwrap(),
            "/legacy/a"
        );
        assert_eq!(rows[1].try_get::<String>("", "slug").unwrap(), "duplicate");
        assert_eq!(
            rows[2].try_get::<Option<String>>("", "owner").unwrap(),
            None
        );
        assert_eq!(rows[2].try_get::<String>("", "slug").unwrap(), "plain");

        let policies = transaction
            .query_all_raw(Statement::from_string(
                transaction.get_database_backend(),
                format!("SELECT id, skill_id FROM {NEXT_POLICY_TABLE} ORDER BY id"),
            ))
            .await
            .expect("migrated policies should query");
        assert_eq!(
            policies.len(),
            2,
            "orphan and ambiguous policies are dropped"
        );
        assert_eq!(
            policies[0].try_get::<String>("", "skill_id").unwrap(),
            "AAAAAAAAAAAAAAAAAAAAA"
        );
        let bundled_tasks_id = context
            .candidates
            .iter()
            .find(|candidate| candidate.bundled && candidate.slug == "tasks")
            .expect("tasks candidate")
            .skill_id
            .to_string();
        assert_eq!(
            policies[1].try_get::<String>("", "skill_id").unwrap(),
            bundled_tasks_id
        );
        transaction
            .rollback()
            .await
            .expect("fixture should rollback");
    }

    #[tokio::test]
    async fn invalid_installation_id_aborts_before_shadow_schema_creation() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        create_legacy_core_tables(&db).await;
        db.execute_unprepared(
            "INSERT INTO skill_installation VALUES \
             ('invalid', 'plain', NULL, 'user', 'workspace-a', 'source', '/legacy', \
              'community', 'fp', '2026-01-01', '2026-01-01')",
        )
        .await
        .expect("invalid legacy fixture should insert");

        let transaction = db.begin().await.expect("transaction should begin");
        let manager = SchemaManager::new(&transaction);
        assert!(migrate_installations_and_policies(&manager).await.is_err());
        let shadow = transaction
            .query_one_raw(Statement::from_string(
                transaction.get_database_backend(),
                format!(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '{NEXT_INSTALLATION_TABLE}'"
                ),
            ))
            .await
            .expect("schema query should succeed");
        assert!(shadow.is_none());
        transaction
            .rollback()
            .await
            .expect("fixture should rollback");
    }

    #[tokio::test]
    async fn history_fixture_preserves_snapshots_and_shares_historical_ids_across_tables() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        create_legacy_core_tables(&db).await;
        create_turn_and_legacy_history_tables(&db).await;
        db.execute_unprepared(
            r#"
            INSERT INTO workspace VALUES ('WWWWWWWWWWWWWWWWWWWWW');
            INSERT INTO thread VALUES ('TTTTTTTTTTTTTTTTTTTTT', 'WWWWWWWWWWWWWWWWWWWWW');
            INSERT INTO turn VALUES ('UUUUUUUUUUUUUUUUUUUUU', 'TTTTTTTTTTTTTTTTTTTTT');
            INSERT INTO skill_installation VALUES
              ('AAAAAAAAAAAAAAAAAAAAA', 'owner/current', '1.0', 'user',
               'WWWWWWWWWWWWWWWWWWWWW', 'source-a', '/legacy/a', 'community',
               'fp-a', '2026-01-01', '2026-01-02');
            INSERT INTO turn_skill_binding VALUES
              ('BBBBBBBBBBBBBBBBBBBBB', 'UUUUUUUUUUUUUUUUUUUUU', 'owner/current',
               '1.0', 'fp-binding', 'user', 'explicit', '2026-02-01'),
              ('CCCCCCCCCCCCCCCCCCCCC', 'UUUUUUUUUUUUUUUUUUUUU', 'pioneer/browser',
               NULL, 'fp-browser', 'system', 'implicit', '2026-02-02');
            INSERT INTO skill_audit_event VALUES
              ('DDDDDDDDDDDDDDDDDDDDD', 'UUUUUUUUUUUUUUUUUUUUU', 'old/deleted',
               'registry', 'invoke', 'allow', 'reason-a', '{"detail":true}', '2026-03-01'),
              ('EEEEEEEEEEEEEEEEEEEEE', 'UUUUUUUUUUUUUUUUUUUUU', 'old/deleted',
               'registry', 'finish', 'allow', NULL, '{}', '2026-03-02');
            INSERT INTO skill_dependency_snapshot VALUES
              ('FFFFFFFFFFFFFFFFFFFFF', 'UUUUUUUUUUUUUUUUUUUUU', 'old/deleted',
               'registry', '[{"missing":"tool"}]', '2026-04-01');
            "#,
        )
        .await
        .expect("legacy history fixture should insert");

        let transaction = db.begin().await.expect("transaction should begin");
        let manager = SchemaManager::new(&transaction);
        let mut context = migrate_installations_and_policies(&manager)
            .await
            .expect("core migration should succeed");
        migrate_history_surfaces(&manager, &mut context)
            .await
            .expect("history migration should succeed");

        let bindings = transaction
            .query_all_raw(Statement::from_string(
                transaction.get_database_backend(),
                format!(
                    "SELECT skill_id, skill_owner, skill_slug, fingerprint FROM {NEXT_BINDING_TABLE} ORDER BY id"
                ),
            ))
            .await
            .expect("binding rows should query");
        assert_eq!(bindings.len(), 2);
        assert_eq!(
            bindings[0].try_get::<String>("", "skill_id").unwrap(),
            "AAAAAAAAAAAAAAAAAAAAA"
        );
        assert_eq!(
            bindings[0].try_get::<String>("", "skill_owner").unwrap(),
            "owner"
        );
        assert_eq!(
            bindings[0].try_get::<String>("", "skill_slug").unwrap(),
            "current"
        );
        assert_eq!(
            bindings[0].try_get::<String>("", "fingerprint").unwrap(),
            "fp-binding"
        );
        let bundled_browser_id = context
            .candidates
            .iter()
            .find(|candidate| candidate.bundled && candidate.slug == "browser")
            .expect("browser candidate")
            .skill_id
            .to_string();
        assert_eq!(
            bindings[1].try_get::<String>("", "skill_id").unwrap(),
            bundled_browser_id
        );

        let audits = transaction
            .query_all_raw(Statement::from_string(
                transaction.get_database_backend(),
                format!(
                    "SELECT skill_id, skill_owner, skill_slug, details_json FROM {NEXT_AUDIT_TABLE} ORDER BY id"
                ),
            ))
            .await
            .expect("audit rows should query");
        assert_eq!(audits.len(), 2);
        let historical_id = audits[0].try_get::<String>("", "skill_id").unwrap();
        validate_skill_id(historical_id.as_str(), "fixture history skill_id")
            .expect("history ID should be valid");
        assert_eq!(
            audits[1].try_get::<String>("", "skill_id").unwrap(),
            historical_id
        );
        assert_eq!(
            audits[0].try_get::<String>("", "skill_owner").unwrap(),
            "old"
        );
        assert_eq!(
            audits[0].try_get::<String>("", "skill_slug").unwrap(),
            "deleted"
        );
        assert_eq!(
            audits[0].try_get::<String>("", "details_json").unwrap(),
            "{\"detail\":true}"
        );

        let dependency = transaction
            .query_one_raw(Statement::from_string(
                transaction.get_database_backend(),
                format!("SELECT skill_id, diagnostics_json FROM {NEXT_DEPENDENCY_TABLE}"),
            ))
            .await
            .expect("dependency row should query")
            .expect("dependency row should exist");
        assert_eq!(
            dependency.try_get::<String>("", "skill_id").unwrap(),
            historical_id
        );
        assert_eq!(
            dependency
                .try_get::<String>("", "diagnostics_json")
                .unwrap(),
            "[{\"missing\":\"tool\"}]"
        );

        let installation_count = transaction
            .query_one_raw(Statement::from_string(
                transaction.get_database_backend(),
                format!("SELECT COUNT(*) AS count FROM {NEXT_INSTALLATION_TABLE}"),
            ))
            .await
            .expect("installation count should query")
            .expect("installation count row");
        assert_eq!(
            installation_count.try_get::<i64>("", "count").unwrap(),
            1,
            "historical-only identities must not create active installations"
        );
        transaction
            .rollback()
            .await
            .expect("fixture should rollback");
    }

    #[tokio::test]
    async fn registered_migration_converts_all_typed_json_and_preserves_unowned_input_bytes() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        apply_pre_stable_migrations(&db).await;
        seed_minimal_turn_and_installation(&db).await;
        seed_identity_json_surfaces(&db).await;

        Migrator::up(&db, None)
            .await
            .expect("registered Stable SkillId migration should apply");

        let snapshot = db
            .query_one_raw(Statement::from_string(
                db.get_database_backend(),
                format!(
                    "SELECT workspace_skill_policies_json, capabilities_json, input_json FROM turn_runtime_snapshot WHERE turn_id = '{FIXTURE_TURN_ID}'"
                ),
            ))
            .await
            .expect("snapshot should query")
            .expect("snapshot should exist");
        let policies: JsonValue = serde_json::from_str(
            snapshot
                .try_get::<String>("", "workspace_skill_policies_json")
                .unwrap()
                .as_str(),
        )
        .unwrap();
        assert_eq!(policies[0]["skill_id"], "AAAAAAAAAAAAAAAAAAAAA");
        assert_eq!(policies[0]["enabled"], true);
        assert_eq!(policies[0]["allow_implicit_invocation"], false);
        assert!(policies[0].get("owner").is_none());
        let capabilities: JsonValue = serde_json::from_str(
            snapshot
                .try_get::<String>("", "capabilities_json")
                .unwrap()
                .as_str(),
        )
        .unwrap();
        assert_eq!(capabilities[0]["kind"]["skillId"], "AAAAAAAAAAAAAAAAAAAAA");
        assert_eq!(
            snapshot.try_get::<String>("", "input_json").unwrap(),
            UNCHANGED_RUNTIME_INPUT
        );

        for (table, id) in [
            ("turn_item", "JJJJJJJJJJJJJJJJJJJJJ"),
            ("turn_item_attempt", "KKKKKKKKKKKKKKKKKKKKK"),
        ] {
            let row = db
                .query_one_raw(Statement::from_string(
                    db.get_database_backend(),
                    format!("SELECT payload FROM {table} WHERE id = '{id}'"),
                ))
                .await
                .expect("migrated item payload should query")
                .expect("migrated item payload should exist");
            let payload: JsonValue =
                serde_json::from_str(row.try_get::<String>("", "payload").unwrap().as_str())
                    .unwrap();
            assert_eq!(
                payload["attachments"][0]["capability"]["skillId"],
                "AAAAAAAAAAAAAAAAAAAAA"
            );
        }
        let cleared_attempt = db
            .query_one_raw(Statement::from_string(
                db.get_database_backend(),
                "SELECT payload FROM turn_item_attempt WHERE id = 'LLLLLLLLLLLLLLLLLLLLL'"
                    .to_owned(),
            ))
            .await
            .expect("cleared attempt should query")
            .expect("cleared attempt should exist");
        assert_eq!(
            cleared_attempt.try_get::<String>("", "payload").unwrap(),
            "{}"
        );

        let item_event = db
            .query_one_raw(Statement::from_string(
                db.get_database_backend(),
                "SELECT payload FROM turn_event WHERE id = 'MMMMMMMMMMMMMMMMMMMMM'".to_owned(),
            ))
            .await
            .expect("item event should query")
            .expect("item event should exist");
        let item_event: JsonValue = serde_json::from_str(
            item_event
                .try_get::<String>("", "payload")
                .unwrap()
                .as_str(),
        )
        .unwrap();
        assert_eq!(
            item_event["payload"]["item"]["attachments"][0]["capability"]["skillId"],
            "AAAAAAAAAAAAAAAAAAAAA"
        );
        let turn_started = db
            .query_one_raw(Statement::from_string(
                db.get_database_backend(),
                "SELECT payload FROM turn_event WHERE id = 'NNNNNNNNNNNNNNNNNNNNN'".to_owned(),
            ))
            .await
            .expect("turn-started event should query")
            .expect("turn-started event should exist");
        assert_eq!(
            turn_started.try_get::<String>("", "payload").unwrap(),
            UNCHANGED_TURN_STARTED_EVENT
        );
        let turn_input = db
            .query_one_raw(Statement::from_string(
                db.get_database_backend(),
                "SELECT payload FROM turn_input WHERE id = 'IIIIIIIIIIIIIIIIIIIII'".to_owned(),
            ))
            .await
            .expect("turn input should query")
            .expect("turn input should exist");
        assert_eq!(
            turn_input.try_get::<String>("", "payload").unwrap(),
            UNCHANGED_TURN_INPUT
        );
    }

    #[tokio::test]
    async fn registered_migration_updates_identity_json_through_compressed_views() {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        apply_pre_stable_migrations(&db).await;
        seed_minimal_turn_and_installation(&db).await;
        seed_identity_json_surfaces(&db).await;
        enable_and_compress_identity_payload_views(&db).await;

        for (table, row_id) in [
            ("turn_item", "JJJJJJJJJJJJJJJJJJJJJ"),
            ("turn_event", "MMMMMMMMMMMMMMMMMMMMM"),
        ] {
            assert!(
                payload_dict_id(&db, table, row_id).await.is_some(),
                "fixture row should be compressed before migration"
            );
        }

        Migrator::up(&db, None)
            .await
            .expect("Stable SkillId migration should update compressed views");

        let mut migrated_payloads = HashMap::new();
        for (table, row_id) in [
            ("turn_item", "JJJJJJJJJJJJJJJJJJJJJ"),
            ("turn_event", "MMMMMMMMMMMMMMMMMMMMM"),
        ] {
            let row = db
                .query_one_raw(Statement::from_sql_and_values(
                    db.get_database_backend(),
                    format!("SELECT payload FROM {table} WHERE id = ?"),
                    [row_id.to_owned().into()],
                ))
                .await
                .unwrap_or_else(|error| panic!("migrated {table} payload should query: {error}"))
                .unwrap_or_else(|| panic!("migrated {table} row should exist"));
            let payload: String = row
                .try_get("", "payload")
                .unwrap_or_else(|error| panic!("migrated {table} payload should decode: {error}"));
            assert!(
                payload.contains("\"skillId\":\"AAAAAAAAAAAAAAAAAAAAA\""),
                "migrated {table} payload should contain the stable SkillId"
            );
            assert_eq!(
                payload_dict_id(&db, table, row_id).await,
                None,
                "view update should leave the migrated payload pending recompression"
            );
            migrated_payloads.insert((table, row_id), payload);
        }

        db.query_one_raw(Statement::from_string(
            db.get_database_backend(),
            "SELECT zstd_incremental_maintenance(NULL, 1) AS value".to_owned(),
        ))
        .await
        .expect("migrated identity payload rows should recompress");

        for ((table, row_id), expected_payload) in migrated_payloads {
            assert!(
                payload_dict_id(&db, table, row_id).await.is_some(),
                "migrated payload should be compressed again"
            );
            let row = db
                .query_one_raw(Statement::from_sql_and_values(
                    db.get_database_backend(),
                    format!("SELECT payload FROM {table} WHERE id = ?"),
                    [row_id.to_owned().into()],
                ))
                .await
                .unwrap_or_else(|error| {
                    panic!("recompressed {table} payload should query: {error}")
                })
                .unwrap_or_else(|| panic!("recompressed {table} row should exist"));
            assert_eq!(
                row.try_get::<String>("", "payload").unwrap(),
                expected_payload,
                "recompression must preserve the migrated payload"
            );
        }
    }

    #[tokio::test]
    async fn malformed_identity_json_rolls_back_and_registered_migration_is_retryable() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        apply_pre_stable_migrations(&db).await;
        seed_minimal_turn_and_installation(&db).await;
        seed_identity_json_surfaces(&db).await;
        db.execute_unprepared("UPDATE turn_runtime_snapshot SET capabilities_json = '{malformed'")
            .await
            .expect("fixture should corrupt the identity-bearing field");

        let error = Migrator::up(&db, None)
            .await
            .expect_err("malformed identity JSON must abort migration");
        let diagnostic = error.to_string();
        assert!(diagnostic.contains("turn_runtime_snapshot"));
        assert!(diagnostic.contains(FIXTURE_TURN_ID));
        assert!(diagnostic.contains("capabilities_json"));

        let installation_columns = db
            .query_all_raw(Statement::from_string(
                db.get_database_backend(),
                "PRAGMA table_info('skill_installation')".to_owned(),
            ))
            .await
            .expect("installation schema should query");
        let installation_columns = installation_columns
            .into_iter()
            .map(|row| row.try_get::<String>("", "name").unwrap())
            .collect::<Vec<_>>();
        assert!(installation_columns.contains(&"slug".to_owned()));
        assert!(!installation_columns.contains(&"owner".to_owned()));
        let shadow = db
            .query_one_raw(Statement::from_string(
                db.get_database_backend(),
                format!(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '{NEXT_INSTALLATION_TABLE}'"
                ),
            ))
            .await
            .expect("shadow schema should query");
        assert!(shadow.is_none());
        let migration_record = db
            .query_one_raw(Statement::from_string(
                db.get_database_backend(),
                "SELECT 1 FROM seaql_migrations WHERE version = 'm20260720_000002_stable_skill_id'"
                    .to_owned(),
            ))
            .await
            .expect("migration tracking should query");
        assert!(migration_record.is_none());

        db.execute_unprepared("UPDATE turn_runtime_snapshot SET capabilities_json = '[]'")
            .await
            .expect("fixture should repair malformed JSON");
        Migrator::up(&db, None)
            .await
            .expect("ordinary Migrator retry should succeed");
        let final_columns = db
            .query_all_raw(Statement::from_string(
                db.get_database_backend(),
                "PRAGMA table_info('skill_installation')".to_owned(),
            ))
            .await
            .expect("final installation schema should query")
            .into_iter()
            .map(|row| row.try_get::<String>("", "name").unwrap())
            .collect::<Vec<_>>();
        assert!(final_columns.contains(&"owner".to_owned()));
    }

    async fn stable_schema_signature(db: &DatabaseConnection) -> Vec<(String, String, String)> {
        let tables = [
            "skill_installation",
            "skill_workspace_policy",
            "turn_skill_binding",
            "skill_audit_event",
            "skill_dependency_snapshot",
        ];
        let mut signature = Vec::new();
        for table in tables {
            for row in db
                .query_all_raw(Statement::from_string(
                    db.get_database_backend(),
                    format!("PRAGMA table_info('{table}')"),
                ))
                .await
                .expect("table signature should query")
            {
                signature.push((
                    table.to_owned(),
                    row.try_get::<String>("", "name").unwrap(),
                    format!(
                        "{}:{}:{}",
                        row.try_get::<String>("", "type").unwrap(),
                        row.try_get::<i64>("", "notnull").unwrap(),
                        row.try_get::<i64>("", "pk").unwrap()
                    ),
                ));
            }
        }
        let index_rows = db
            .query_all_raw(Statement::from_string(
                db.get_database_backend(),
                "SELECT tbl_name, name, COALESCE(sql, '') AS sql FROM sqlite_master \
                 WHERE type = 'index' AND tbl_name IN (\
                    'skill_installation', 'skill_workspace_policy', 'turn_skill_binding', \
                    'skill_audit_event', 'skill_dependency_snapshot'\
                 ) ORDER BY tbl_name, name"
                    .to_owned(),
            ))
            .await
            .expect("index signature should query");
        for row in index_rows {
            signature.push((
                row.try_get("", "tbl_name").unwrap(),
                row.try_get("", "name").unwrap(),
                row.try_get("", "sql").unwrap(),
            ));
        }
        signature
    }

    #[tokio::test]
    async fn fresh_and_staged_upgrade_databases_have_the_same_final_skill_schema() {
        let fresh = Database::connect("sqlite::memory:")
            .await
            .expect("fresh sqlite database should open");
        Migrator::up(&fresh, None)
            .await
            .expect("fresh database should migrate");

        let upgraded = Database::connect("sqlite::memory:")
            .await
            .expect("upgrade sqlite database should open");
        apply_pre_stable_migrations(&upgraded).await;
        Migrator::up(&upgraded, None)
            .await
            .expect("staged database should migrate");

        assert_eq!(
            stable_schema_signature(&fresh).await,
            stable_schema_signature(&upgraded).await
        );
    }
}
