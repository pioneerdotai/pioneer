use anyhow::{Context, Result};
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_BACKFILLING, PROJECTION_META_STATUS_COMPLETE,
    PROJECTION_META_STATUS_FAILED, ProjectionMetaRecord, find_projection_meta,
    upsert_projection_meta,
};
use pioneer_protocol::{
    TaskAgentSecurityCap, TurnExecutionSecuritySnapshot, TurnFilesystemSandboxEntry,
    TurnFilesystemSandboxKind, TurnFilesystemSandboxPath, TurnNetworkPolicySnapshot,
    TurnPermissionMode, TurnPermissionProfileSnapshot, TurnProcessPolicySnapshot, TurnSandboxMode,
    TurnSecurityRuleProvenance, TurnSecuritySnapshotSource,
};
use sea_orm::{
    ConnectionTrait, DbBackend, FromQueryResult, Statement, TransactionTrait,
    entity::prelude::DateTimeWithTimeZone,
};
use serde_json::Value as JsonValue;
use std::path::Path;
use tracing::{info, warn};

const TURN_PERMISSION_PROFILE_BACKFILL_KEY: &str = "turn_permission_profile_payload_backfill";
const TURN_PERMISSION_PROFILE_BACKFILL_VERSION: i64 = 4;
const TURN_PERMISSION_PROFILE_BACKFILL_BATCH_SIZE: u64 = 32;
const DEFAULT_TURN_PERMISSION_PROFILE_MODE: &str = "full_access";
const DEFAULT_TURN_PERMISSION_PROFILE_SOURCE: &str = "defaulted";
const DEFAULT_TURN_PERMISSION_PROFILE_SNAPSHOT_JSON: &str = r#"{"mode":"full_access","source":"defaulted","effective_policy":{"default_behavior":"allow","file_read":"allow","file_write":"allow","shell_command":"allow","network":"allow","mcp_read":"allow","mcp_write_or_unknown":"allow","dynamic_skill_tool":"allow","computer_use":"allow","task_subagent":"allow","memory_write":"allow","agent_action":"allow"}}"#;

#[derive(Debug, FromQueryResult)]
struct LegacyTurnEventPayload {
    id: String,
    payload: String,
}

#[derive(Debug, FromQueryResult)]
struct LegacyFullAccessTurnSecuritySnapshot {
    id: String,
    permission_profile_snapshot_json: String,
    created_at: DateTimeWithTimeZone,
}

#[derive(Debug, FromQueryResult)]
struct SyntheticWorkspaceTurnSecuritySnapshot {
    id: String,
    execution_security_snapshot_json: String,
}

#[derive(Debug, FromQueryResult)]
struct SyntheticWorkspaceTaskSecurityCap {
    id: String,
    workspace_id: String,
    security_cap_json: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct TurnPermissionProfileBackfillSummary {
    pub(crate) skipped: bool,
    pub(crate) batches: u64,
    pub(crate) turn_events_updated: u64,
    pub(crate) turns_updated: u64,
    pub(crate) task_agent_caps_updated: u64,
    pub(crate) security_snapshots_updated: u64,
    pub(crate) security_snapshots_repaired: u64,
    pub(crate) cli_runtime_thread_bindings_removed: u64,
}

pub(super) async fn run(crud_store: &CrudStore, runtime_home: &Path) -> Result<()> {
    match backfill_once(crud_store, runtime_home).await {
        Ok(summary) if summary.skipped => {}
        Ok(summary) => {
            info!(
                batches = summary.batches,
                turn_events_updated = summary.turn_events_updated,
                turns_updated = summary.turns_updated,
                task_agent_caps_updated = summary.task_agent_caps_updated,
                security_snapshots_updated = summary.security_snapshots_updated,
                security_snapshots_repaired = summary.security_snapshots_repaired,
                cli_runtime_thread_bindings_removed = summary.cli_runtime_thread_bindings_removed,
                "turn permission profile and security snapshot backfill completed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "turn permission profile  and security snapshot backfill failed at startup"
            );
            return Err(error);
        }
    }
    Ok(())
}

pub(crate) async fn backfill_once(
    crud_store: &CrudStore,
    runtime_home: &Path,
) -> Result<TurnPermissionProfileBackfillSummary> {
    // This routine owns a complete startup-maintenance operation, including
    // its projection marker writes. Pin the class at this boundary instead of
    // relying on every caller (including repair and test entry points) to
    // remember to pre-scope the store.
    let crud_store = crud_store.with_maintenance_access();
    let db = crud_store.database_connection();
    if backfill_is_current(&db).await? {
        return Ok(TurnPermissionProfileBackfillSummary {
            skipped: true,
            ..Default::default()
        });
    }

    let started_at = now_datetime();
    upsert_projection_meta(
        &db,
        ProjectionMetaRecord {
            projection_key: TURN_PERMISSION_PROFILE_BACKFILL_KEY.to_owned(),
            projection_version: TURN_PERMISSION_PROFILE_BACKFILL_VERSION,
            status: PROJECTION_META_STATUS_BACKFILLING.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(started_at),
            backfilled_at: None,
            created_at: started_at,
            updated_at: started_at,
        },
    )
    .await?;

    let result = backfill_all_batches(&crud_store, runtime_home).await;
    match result {
        Ok(summary) => {
            mark_backfill_complete(&db, &summary).await?;
            Ok(summary)
        }
        Err(error) => {
            mark_backfill_failed(&db, &error).await?;
            Err(error)
        }
    }
}

async fn backfill_all_batches(
    crud_store: &CrudStore,
    runtime_home: &Path,
) -> Result<TurnPermissionProfileBackfillSummary> {
    let mut summary = TurnPermissionProfileBackfillSummary::default();
    let cwd = std::env::current_dir()
        .context("failed to resolve gateway cwd for legacy turn security snapshots")?;
    let cwd = cwd.to_string_lossy().into_owned();

    loop {
        let turn_events_updated =
            run_low_priority_batch(crud_store, PermissionBackfillBatch::TurnEvents)
                .await
                .context("failed to backfill turn_event permission profiles")?;
        let turns_updated = run_low_priority_batch(crud_store, PermissionBackfillBatch::Turns)
            .await
            .context("failed to backfill turn permission profile columns")?;
        let task_agent_caps_backfilled =
            run_low_priority_batch(crud_store, PermissionBackfillBatch::TaskAgentCaps)
                .await
                .context("failed to backfill task agent permission and security caps")?;
        let task_agent_caps_repaired = run_low_priority_batch(
            crud_store,
            PermissionBackfillBatch::RepairSyntheticTaskCaps {
                runtime_home: runtime_home.to_path_buf(),
                cwd: cwd.clone(),
            },
        )
        .await
        .context("failed to repair synthetic workspace paths in task security caps")?;
        let task_agent_caps_updated =
            task_agent_caps_backfilled.saturating_add(task_agent_caps_repaired);
        let security_snapshots_updated = run_low_priority_batch(
            crud_store,
            PermissionBackfillBatch::FullAccessSecuritySnapshots { cwd: cwd.clone() },
        )
        .await
        .context("failed to backfill full access turn security snapshots")?;
        let synthetic_workspace_security_snapshots_repaired = run_low_priority_batch(
            crud_store,
            PermissionBackfillBatch::RepairSyntheticSecuritySnapshots {
                runtime_home: runtime_home.to_path_buf(),
                cwd: cwd.clone(),
            },
        )
        .await
        .context("failed to repair synthetic workspace paths in turn security snapshots")?;
        let regressed_security_snapshots_repaired = run_low_priority_batch(
            crud_store,
            PermissionBackfillBatch::RepairRegressedSecuritySnapshots,
        )
        .await
        .context("failed to repair regressed turn security snapshots")?;
        let security_snapshots_repaired = synthetic_workspace_security_snapshots_repaired
            .saturating_add(regressed_security_snapshots_repaired);
        let cli_runtime_thread_bindings_removed = run_low_priority_batch(
            crud_store,
            PermissionBackfillBatch::RemoveSyntheticCliBindings {
                runtime_home: runtime_home.to_path_buf(),
            },
        )
        .await
        .context("failed to remove synthetic workspace CLI thread bindings")?;

        if turn_events_updated == 0
            && turns_updated == 0
            && task_agent_caps_updated == 0
            && security_snapshots_updated == 0
            && security_snapshots_repaired == 0
            && cli_runtime_thread_bindings_removed == 0
        {
            break;
        }

        summary.batches = summary.batches.saturating_add(1);
        summary.turn_events_updated = summary
            .turn_events_updated
            .saturating_add(turn_events_updated);
        summary.turns_updated = summary.turns_updated.saturating_add(turns_updated);
        summary.task_agent_caps_updated = summary
            .task_agent_caps_updated
            .saturating_add(task_agent_caps_updated);
        summary.security_snapshots_updated = summary
            .security_snapshots_updated
            .saturating_add(security_snapshots_updated);
        summary.security_snapshots_repaired = summary
            .security_snapshots_repaired
            .saturating_add(security_snapshots_repaired);
        summary.cli_runtime_thread_bindings_removed = summary
            .cli_runtime_thread_bindings_removed
            .saturating_add(cli_runtime_thread_bindings_removed);

        super::maintenance_checkpoint().await?;
    }

    Ok(summary)
}

#[derive(Clone)]
enum PermissionBackfillBatch {
    TurnEvents,
    Turns,
    TaskAgentCaps,
    RepairSyntheticTaskCaps {
        runtime_home: std::path::PathBuf,
        cwd: String,
    },
    FullAccessSecuritySnapshots {
        cwd: String,
    },
    RepairSyntheticSecuritySnapshots {
        runtime_home: std::path::PathBuf,
        cwd: String,
    },
    RepairRegressedSecuritySnapshots,
    RemoveSyntheticCliBindings {
        runtime_home: std::path::PathBuf,
    },
}

impl PermissionBackfillBatch {
    async fn run(&self, database: &pioneer_sqlite::SqliteDatabase) -> Result<u64> {
        match self {
            Self::TurnEvents => backfill_turn_event_batch(database).await,
            Self::Turns => backfill_turn_batch(database).await,
            Self::TaskAgentCaps => backfill_task_agent_cap_batch(database).await,
            Self::RepairSyntheticTaskCaps { runtime_home, cwd } => {
                repair_synthetic_workspace_task_security_cap_batch(database, runtime_home, cwd)
                    .await
            }
            Self::FullAccessSecuritySnapshots { cwd } => {
                backfill_full_access_turn_security_snapshot_batch(database, cwd).await
            }
            Self::RepairSyntheticSecuritySnapshots { runtime_home, cwd } => {
                repair_synthetic_workspace_security_snapshot_batch(database, runtime_home, cwd)
                    .await
            }
            Self::RepairRegressedSecuritySnapshots => {
                repair_regressed_turn_security_snapshot_batch(database).await
            }
            Self::RemoveSyntheticCliBindings { runtime_home } => {
                remove_synthetic_workspace_cli_thread_binding_batch(database, runtime_home).await
            }
        }
    }
}

async fn run_low_priority_batch(
    crud_store: &CrudStore,
    operation: PermissionBackfillBatch,
) -> Result<u64> {
    let database = crud_store.database_connection();
    let result = crud_store
        .run_background_database_quantum(|| {
            let database = database.clone();
            let operation = operation.clone();
            async move {
                // Candidate reads and all JSON/path preparation happen before
                // the batch function begins its short writer transaction.
                operation.run(&database).await
            }
        })
        .await?;
    // The transaction is committed and its pool connection has been
    // released. Pause at every durable batch boundary.
    super::maintenance_checkpoint().await?;
    Ok(result)
}

async fn execute_prepared_backfill_statement(
    database: &pioneer_sqlite::SqliteDatabase,
    statement: Option<Statement>,
    label: &'static str,
) -> Result<u64> {
    let Some(statement) = statement else {
        return Ok(0);
    };
    let transaction = database
        .begin()
        .await
        .with_context(|| format!("failed to begin {label} transaction"))?;
    match transaction.execute_raw(statement).await {
        Ok(result) => {
            let rows_affected = result.rows_affected();
            transaction
                .commit()
                .await
                .with_context(|| format!("failed to commit {label} transaction"))?;
            Ok(rows_affected)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error).with_context(|| format!("failed to execute {label} statement"))
        }
    }
}

async fn repair_regressed_turn_security_snapshot_batch(
    db: &pioneer_sqlite::SqliteDatabase,
) -> Result<u64> {
    let candidates = SyntheticWorkspaceTurnSecuritySnapshot::find_by_statement(
        Statement::from_string(
            db.get_database_backend(),
            format!(
                "SELECT id, execution_security_snapshot_json \
                 FROM turn \
                 WHERE execution_security_snapshot_json IS NOT NULL \
                   AND (\
                        (\
                            json_extract(execution_security_snapshot_json, '$.source') = 'composer_selection' \
                            AND \
                            (json_type(execution_security_snapshot_json, '$.parent_cap') IS NULL \
                             OR json_type(execution_security_snapshot_json, '$.parent_cap') = 'null') \
                            AND json_extract(execution_security_snapshot_json, '$.sandbox.mode') != 'unrestricted' \
                            AND (\
                                json_extract(execution_security_snapshot_json, '$.authority_cap.filesystem.kind') != 'unrestricted' \
                                OR json_extract(execution_security_snapshot_json, '$.authority_cap.network.mode') != 'enabled'\
                            )\
                        ) \
                        OR (\
                            json_extract(execution_security_snapshot_json, '$.process.environment.inherit') = 0 \
                            AND COALESCE(json_array_length(execution_security_snapshot_json, '$.process.environment.allowed_vars'), 0) = 0 \
                            AND COALESCE(json_array_length(execution_security_snapshot_json, '$.process.environment.denied_patterns'), 0) = 0\
                        ) \
                        OR (\
                            json_extract(execution_security_snapshot_json, '$.authority_cap.process.environment.inherit') = 0 \
                            AND COALESCE(json_array_length(execution_security_snapshot_json, '$.authority_cap.process.environment.allowed_vars'), 0) = 0 \
                            AND COALESCE(json_array_length(execution_security_snapshot_json, '$.authority_cap.process.environment.denied_patterns'), 0) = 0\
                        )\
                   ) \
                 ORDER BY created_at, id \
                 LIMIT {TURN_PERMISSION_PROFILE_BACKFILL_BATCH_SIZE}",
            ),
        ),
    )
    .all(db)
    .await
    .context("failed to list turn snapshots with regressed security policies")?;

    let mut updates = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let mut snapshot: TurnExecutionSecuritySnapshot =
            serde_json::from_str(candidate.execution_security_snapshot_json.as_str())
                .with_context(|| {
                    format!(
                        "failed to decode regressed security snapshot for turn `{}`",
                        candidate.id
                    )
                })?;
        if !repair_regressed_turn_security_snapshot(&mut snapshot) {
            continue;
        }

        let snapshot_version = i64::from(snapshot.version);
        let snapshot_json = serde_json::to_string(&snapshot).with_context(|| {
            format!(
                "failed to serialize repaired security snapshot for turn `{}`",
                candidate.id
            )
        })?;
        updates.push((candidate.id, snapshot_version, snapshot_json));
    }
    let statement = turn_security_snapshot_update_statement(updates)?;
    execute_prepared_backfill_statement(db, statement, "regressed security snapshot repair").await
}

fn repair_regressed_turn_security_snapshot(snapshot: &mut TurnExecutionSecuritySnapshot) -> bool {
    let mut changed = false;

    // Root turns are interactive security principals. Their initial sandbox
    // remains restricted, while this immutable cap describes the maximum an
    // exact human consent may grant. Child and reviewer snapshots carry a
    // parent cap and must retain that durable inherited maximum.
    // The regressed constructors emitted only ComposerSelection snapshots.
    // Other provenance classes can encode inherited or recovery authority
    // even when old records lack parent metadata, so never infer a wider cap
    // for them during startup repair.
    let provenance_allows_root_repair =
        snapshot.source == TurnSecuritySnapshotSource::ComposerSelection;
    if provenance_allows_root_repair
        && snapshot.parent_cap.is_none()
        && snapshot.sandbox.mode != TurnSandboxMode::Unrestricted
    {
        if snapshot.authority_cap.filesystem.kind
            != pioneer_protocol::TurnFilesystemSandboxKind::Unrestricted
        {
            snapshot.authority_cap.filesystem =
                pioneer_protocol::TurnFilesystemSandboxPolicy::unrestricted();
            changed = true;
        }
        if snapshot.authority_cap.network.mode != pioneer_protocol::TurnNetworkMode::Enabled {
            snapshot.authority_cap.network = TurnNetworkPolicySnapshot::enabled();
            changed = true;
        }
    }

    let restricted_environment = TurnProcessPolicySnapshot::restricted().environment;
    for environment in [
        &mut snapshot.process.environment,
        &mut snapshot.authority_cap.process.environment,
    ] {
        if !environment.inherit
            && environment.allowed_vars.is_empty()
            && environment.denied_patterns.is_empty()
        {
            *environment = restricted_environment.clone();
            changed = true;
        }
    }

    changed
}

async fn backfill_task_agent_cap_batch(db: &pioneer_sqlite::SqliteDatabase) -> Result<u64> {
    let permission_cap =
        pioneer_protocol::task_permission_cap_for_mode(TurnPermissionMode::FullAccess);
    let security_cap = TaskAgentSecurityCap {
        max_permission_profile: permission_cap.clone(),
        max_filesystem_kind: Some(TurnFilesystemSandboxKind::Unrestricted),
        max_filesystem_entries: Vec::new(),
        max_network_policy: TurnNetworkPolicySnapshot::enabled(),
        max_sandbox_mode: TurnSandboxMode::Unrestricted,
        max_process_policy: TurnProcessPolicySnapshot::unrestricted(),
    };
    let permission_cap_json = serde_json::to_string(&permission_cap)
        .context("failed to serialize legacy full access task permission cap")?;
    let security_cap_json = serde_json::to_string(&security_cap)
        .context("failed to serialize legacy full access task security cap")?;
    let statement = Statement::from_sql_and_values(
        db.get_database_backend(),
        format!(
            "UPDATE task_agent_spec \
                 SET permission_cap_json = COALESCE(permission_cap_json, ?), \
                     security_cap_json = COALESCE(security_cap_json, ?) \
                 WHERE id IN (\
                    SELECT id FROM (\
                        SELECT id \
                        FROM task_agent_spec \
                        WHERE permission_cap_json IS NULL \
                           OR security_cap_json IS NULL \
                        ORDER BY created_at, id \
                        LIMIT {TURN_PERMISSION_PROFILE_BACKFILL_BATCH_SIZE}\
                    )\
                 )",
        ),
        vec![permission_cap_json.into(), security_cap_json.into()],
    );
    execute_prepared_backfill_statement(db, Some(statement), "legacy task agent cap backfill").await
}

async fn repair_synthetic_workspace_security_snapshot_batch(
    db: &pioneer_sqlite::SqliteDatabase,
    runtime_home: &Path,
    cwd: &str,
) -> Result<u64> {
    let legacy_workspace_segment = format!(
        "{}workspaces{}",
        std::path::MAIN_SEPARATOR,
        std::path::MAIN_SEPARATOR
    );
    let regressed_workspace_segment = format!(
        "{}workspace_filesystems{}",
        std::path::MAIN_SEPARATOR,
        std::path::MAIN_SEPARATOR
    );
    let agent_segment = format!("{}agent", std::path::MAIN_SEPARATOR);
    let candidates =
        SyntheticWorkspaceTurnSecuritySnapshot::find_by_statement(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                "SELECT t.id, t.execution_security_snapshot_json \
                 FROM turn AS t \
                 JOIN thread AS th ON th.id = t.thread_id \
                 WHERE t.execution_security_snapshot_json IS NOT NULL \
                   AND (\
                        json_extract(t.execution_security_snapshot_json, '$.sandbox.cwd') = ? || ? || th.workspace_id \
                        OR json_extract(t.execution_security_snapshot_json, '$.sandbox.cwd') = ? || ? || th.workspace_id || ?\
                   ) \
                 ORDER BY t.created_at, t.id \
                 LIMIT {TURN_PERMISSION_PROFILE_BACKFILL_BATCH_SIZE}",
            ),
            vec![
                runtime_home.to_string_lossy().into_owned().into(),
                legacy_workspace_segment.into(),
                runtime_home.to_string_lossy().into_owned().into(),
                regressed_workspace_segment.into(),
                agent_segment.into(),
            ],
        ))
        .all(db)
        .await
        .context("failed to list turn snapshots with synthetic workspace cwd")?;

    let mut updates = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let mut snapshot: TurnExecutionSecuritySnapshot =
            serde_json::from_str(candidate.execution_security_snapshot_json.as_str())
                .with_context(|| {
                    format!(
                        "failed to decode security snapshot for turn `{}`",
                        candidate.id
                    )
                })?;
        let synthetic_cwd = snapshot.sandbox.cwd.clone();
        snapshot.sandbox.cwd = cwd.to_owned();
        for entry in &mut snapshot.sandbox.filesystem.entries {
            repair_synthetic_workspace_entry(entry, synthetic_cwd.as_str(), cwd);
        }
        for entry in &mut snapshot.authority_cap.filesystem.entries {
            repair_synthetic_workspace_entry(entry, synthetic_cwd.as_str(), cwd);
        }
        if let Some(parent_cap) = snapshot.parent_cap.as_mut() {
            for entry in &mut parent_cap.max_filesystem_entries {
                repair_synthetic_workspace_entry(entry, synthetic_cwd.as_str(), cwd);
            }
        }
        repair_regressed_turn_security_snapshot(&mut snapshot);
        let snapshot_version = i64::from(snapshot.version);
        let snapshot_json = serde_json::to_string(&snapshot).with_context(|| {
            format!(
                "failed to serialize repaired security snapshot for turn `{}`",
                candidate.id
            )
        })?;

        updates.push((candidate.id, snapshot_version, snapshot_json));
    }
    let statement = turn_security_snapshot_update_statement(updates)?;
    execute_prepared_backfill_statement(db, statement, "synthetic turn security snapshot repair")
        .await
}

async fn repair_synthetic_workspace_task_security_cap_batch(
    db: &pioneer_sqlite::SqliteDatabase,
    runtime_home: &Path,
    cwd: &str,
) -> Result<u64> {
    let legacy_workspace_segment = format!(
        "{}workspaces{}",
        std::path::MAIN_SEPARATOR,
        std::path::MAIN_SEPARATOR
    );
    let regressed_workspace_segment = format!(
        "{}workspace_filesystems{}",
        std::path::MAIN_SEPARATOR,
        std::path::MAIN_SEPARATOR
    );
    let agent_segment = format!("{}agent", std::path::MAIN_SEPARATOR);
    let candidates =
        SyntheticWorkspaceTaskSecurityCap::find_by_statement(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                "SELECT spec.id, task.workspace_id, spec.security_cap_json \
                 FROM task_agent_spec AS spec \
                 JOIN task ON task.id = spec.task_id \
                 WHERE spec.security_cap_json IS NOT NULL \
                   AND EXISTS (\
                        SELECT 1 \
                        FROM json_each(spec.security_cap_json, '$.maxFilesystemEntries') AS entry \
                        WHERE json_extract(entry.value, '$.resolved_path') = ? || ? || task.workspace_id \
                           OR json_extract(entry.value, '$.resolved_path') = ? || ? || task.workspace_id || ?\
                   ) \
                 ORDER BY spec.created_at, spec.id \
                 LIMIT {TURN_PERMISSION_PROFILE_BACKFILL_BATCH_SIZE}",
            ),
            vec![
                runtime_home.to_string_lossy().into_owned().into(),
                legacy_workspace_segment.into(),
                runtime_home.to_string_lossy().into_owned().into(),
                regressed_workspace_segment.into(),
                agent_segment.into(),
            ],
        ))
        .all(db)
        .await
        .context("failed to list task security caps with synthetic workspace paths")?;

    let mut updates = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let mut cap: TaskAgentSecurityCap =
            serde_json::from_str(candidate.security_cap_json.as_str()).with_context(|| {
                format!(
                    "failed to decode task security cap for agent spec `{}`",
                    candidate.id
                )
            })?;
        let legacy_cwd = runtime_home
            .join("workspaces")
            .join(candidate.workspace_id.as_str())
            .to_string_lossy()
            .into_owned();
        let regressed_cwd = runtime_home
            .join("workspace_filesystems")
            .join(candidate.workspace_id.as_str())
            .join("agent")
            .to_string_lossy()
            .into_owned();
        for entry in &mut cap.max_filesystem_entries {
            repair_synthetic_workspace_entry(entry, legacy_cwd.as_str(), cwd);
            repair_synthetic_workspace_entry(entry, regressed_cwd.as_str(), cwd);
        }
        let security_cap_json = serde_json::to_string(&cap).with_context(|| {
            format!(
                "failed to serialize repaired task security cap for agent spec `{}`",
                candidate.id
            )
        })?;
        updates.push((candidate.id, security_cap_json));
    }
    let statement = json_column_update_statement(
        db.get_database_backend(),
        "task_agent_spec",
        "security_cap_json",
        updates,
    )?;
    execute_prepared_backfill_statement(db, statement, "synthetic task security cap repair").await
}

async fn remove_synthetic_workspace_cli_thread_binding_batch(
    db: &pioneer_sqlite::SqliteDatabase,
    runtime_home: &Path,
) -> Result<u64> {
    let legacy_workspace_segment = format!(
        "{}workspaces{}",
        std::path::MAIN_SEPARATOR,
        std::path::MAIN_SEPARATOR
    );
    let regressed_workspace_segment = format!(
        "{}workspace_filesystems{}",
        std::path::MAIN_SEPARATOR,
        std::path::MAIN_SEPARATOR
    );
    let agent_segment = format!("{}agent", std::path::MAIN_SEPARATOR);
    let statement = Statement::from_sql_and_values(
        db.get_database_backend(),
        format!(
            "DELETE FROM thread_cli_runtime_binding \
                 WHERE thread_id IN (\
                    SELECT thread_id FROM (\
                        SELECT thread_id \
                        FROM thread_cli_runtime_binding \
                        WHERE native_cwd = ? || ? || workspace_id \
                           OR native_cwd = ? || ? || workspace_id || ? \
                        ORDER BY created_at, thread_id \
                        LIMIT {TURN_PERMISSION_PROFILE_BACKFILL_BATCH_SIZE}\
                    )\
                 )",
        ),
        vec![
            runtime_home.to_string_lossy().into_owned().into(),
            legacy_workspace_segment.into(),
            runtime_home.to_string_lossy().into_owned().into(),
            regressed_workspace_segment.into(),
            agent_segment.into(),
        ],
    );
    execute_prepared_backfill_statement(db, Some(statement), "synthetic CLI binding removal").await
}

fn repair_synthetic_workspace_entry(
    entry: &mut TurnFilesystemSandboxEntry,
    synthetic_cwd: &str,
    cwd: &str,
) {
    if entry.resolved_path.as_deref() != Some(synthetic_cwd) {
        return;
    }
    entry.resolved_path = Some(cwd.to_owned());
    if entry.path == TurnFilesystemSandboxPath::WorkspaceRoot {
        entry.path = TurnFilesystemSandboxPath::CurrentWorkingDirectory;
        entry.provenance = TurnSecurityRuleProvenance::Runtime;
    }
}

async fn backfill_full_access_turn_security_snapshot_batch(
    db: &pioneer_sqlite::SqliteDatabase,
    cwd: &str,
) -> Result<u64> {
    let candidates =
        LegacyFullAccessTurnSecuritySnapshot::find_by_statement(Statement::from_string(
            db.get_database_backend(),
            format!(
                "SELECT id, permission_profile_snapshot_json, created_at \
                 FROM turn \
                 WHERE execution_security_snapshot_json IS NULL \
                   AND permission_profile_mode = 'full_access' \
                   AND permission_profile_snapshot_json IS NOT NULL \
                 ORDER BY created_at, id \
                 LIMIT {TURN_PERMISSION_PROFILE_BACKFILL_BATCH_SIZE}",
            ),
        ))
        .all(db)
        .await
        .context("failed to list legacy full access turns without security snapshots")?;

    let mut updates = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let permission_profile: TurnPermissionProfileSnapshot =
            serde_json::from_str(candidate.permission_profile_snapshot_json.as_str())
                .with_context(|| {
                    format!(
                        "failed to decode permission profile for legacy turn `{}`",
                        candidate.id
                    )
                })?;
        let mut snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            cwd,
            candidate.created_at.timestamp_millis(),
        );
        snapshot.source = TurnSecuritySnapshotSource::BackfilledLegacy;
        snapshot.permission_profile = permission_profile;
        let snapshot_version = i64::from(snapshot.version);
        let snapshot_json = serde_json::to_string(&snapshot).with_context(|| {
            format!(
                "failed to serialize security snapshot for legacy turn `{}`",
                candidate.id
            )
        })?;

        updates.push((candidate.id, snapshot_version, snapshot_json));
    }
    let statement = turn_security_snapshot_update_statement(updates)?;
    execute_prepared_backfill_statement(db, statement, "legacy full-access snapshot backfill").await
}

async fn backfill_turn_event_batch(db: &pioneer_sqlite::SqliteDatabase) -> Result<u64> {
    let candidates = LegacyTurnEventPayload::find_by_statement(Statement::from_string(
        db.get_database_backend(),
        format!(
            "SELECT id, payload \
             FROM turn_event \
             WHERE (\
                    json_type(payload, '$.payload.turn') = 'object' \
                    AND (\
                        json_type(payload, '$.payload.turn.permission_profile') IS NULL \
                        OR json_type(payload, '$.payload.turn.permission_profile') = 'null'\
                    )\
                 ) \
                OR EXISTS (\
                    SELECT 1 \
                    FROM json_each(payload, '$.payload.thread.turns') \
                    WHERE json_type(value) = 'object' \
                      AND (\
                          json_type(value, '$.permission_profile') IS NULL \
                          OR json_type(value, '$.permission_profile') = 'null'\
                      )\
               ) \
                OR (\
                    json_extract(payload, '$.kind') = 'turn_tool_loop_budget_exceeded' \
                    AND json_extract(payload, '$.payload.action') = 'fail_turn'\
               ) \
             ORDER BY created_at, id \
             LIMIT {TURN_PERMISSION_PROFILE_BACKFILL_BATCH_SIZE}",
        ),
    ))
    .all(db)
    .await
    .context("failed to list legacy turn_event permission profile payloads")?;

    let default_profile = default_permission_profile_json()?;
    let mut updates = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let mut payload: JsonValue = serde_json::from_str(candidate.payload.as_str())
            .with_context(|| format!("failed to decode turn_event `{}` payload", candidate.id))?;
        if !patch_turn_event_payload(&mut payload, &default_profile) {
            continue;
        }

        updates.push((
            candidate.id,
            serde_json::to_string(&payload)
                .context("failed to serialize patched turn_event payload")?,
        ));
    }
    let statement =
        json_column_update_statement(db.get_database_backend(), "turn_event", "payload", updates)?;
    execute_prepared_backfill_statement(db, statement, "legacy turn-event payload backfill").await
}

fn turn_security_snapshot_update_statement(
    updates: Vec<(String, i64, String)>,
) -> Result<Option<Statement>> {
    if updates.is_empty() {
        return Ok(None);
    }
    let mut version_case = String::from("CASE id ");
    let mut json_case = String::from("CASE id ");
    let mut values: Vec<sea_orm::Value> = Vec::with_capacity(updates.len() * 5);
    for (id, version, _) in &updates {
        version_case.push_str("WHEN ? THEN ? ");
        values.push(id.clone().into());
        values.push((*version).into());
    }
    version_case.push_str("ELSE execution_security_snapshot_version END");
    for (id, _, json) in &updates {
        json_case.push_str("WHEN ? THEN ? ");
        values.push(id.clone().into());
        values.push(json.clone().into());
    }
    json_case.push_str("ELSE execution_security_snapshot_json END");
    let placeholders = std::iter::repeat_n("?", updates.len())
        .collect::<Vec<_>>()
        .join(",");
    values.extend(updates.into_iter().map(|(id, _, _)| id.into()));
    Ok(Some(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        format!(
            "UPDATE turn SET execution_security_snapshot_version = {version_case}, \
                 execution_security_snapshot_json = {json_case} WHERE id IN ({placeholders})"
        ),
        values,
    )))
}

fn json_column_update_statement(
    backend: DbBackend,
    table: &'static str,
    column: &'static str,
    updates: Vec<(String, String)>,
) -> Result<Option<Statement>> {
    if updates.is_empty() {
        return Ok(None);
    }
    if !matches!(
        (table, column),
        ("task_agent_spec", "security_cap_json") | ("turn_event", "payload")
    ) {
        anyhow::bail!("unsupported startup JSON backfill target {table}.{column}");
    }
    let mut update_case = String::from("CASE id ");
    let mut values: Vec<sea_orm::Value> = Vec::with_capacity(updates.len() * 3);
    for (id, value) in &updates {
        update_case.push_str("WHEN ? THEN ? ");
        values.push(id.clone().into());
        values.push(value.clone().into());
    }
    update_case.push_str(&format!("ELSE {column} END"));
    let placeholders = std::iter::repeat_n("?", updates.len())
        .collect::<Vec<_>>()
        .join(",");
    values.extend(updates.into_iter().map(|(id, _)| id.into()));
    Ok(Some(Statement::from_sql_and_values(
        backend,
        format!("UPDATE {table} SET {column} = {update_case} WHERE id IN ({placeholders})"),
        values,
    )))
}

async fn backfill_turn_batch(db: &pioneer_sqlite::SqliteDatabase) -> Result<u64> {
    let sql = format!(
        "UPDATE turn \
         SET permission_profile_mode = COALESCE(\
                permission_profile_mode, \
                '{DEFAULT_TURN_PERMISSION_PROFILE_MODE}'\
             ), \
             permission_profile_source = COALESCE(\
                permission_profile_source, \
                '{DEFAULT_TURN_PERMISSION_PROFILE_SOURCE}'\
             ), \
             permission_profile_snapshot_json = COALESCE(\
                permission_profile_snapshot_json, \
                '{DEFAULT_TURN_PERMISSION_PROFILE_SNAPSHOT_JSON}'\
             ) \
         WHERE id IN (\
            SELECT id FROM (\
                SELECT id \
                FROM turn \
                WHERE permission_profile_mode IS NULL \
                   OR permission_profile_source IS NULL \
                   OR permission_profile_snapshot_json IS NULL \
                ORDER BY created_at, id \
                LIMIT {TURN_PERMISSION_PROFILE_BACKFILL_BATCH_SIZE}\
            )\
         )",
    );
    execute_prepared_backfill_statement(
        db,
        Some(Statement::from_string(db.get_database_backend(), sql)),
        "legacy turn permission profile backfill",
    )
    .await
}

async fn backfill_is_current<C: ConnectionTrait>(db: &C) -> Result<bool> {
    let Some(meta) = find_projection_meta(db, TURN_PERMISSION_PROFILE_BACKFILL_KEY).await? else {
        return Ok(false);
    };
    Ok(
        meta.projection_version == TURN_PERMISSION_PROFILE_BACKFILL_VERSION
            && meta.status == PROJECTION_META_STATUS_COMPLETE,
    )
}

async fn mark_backfill_complete<C: ConnectionTrait>(
    db: &C,
    summary: &TurnPermissionProfileBackfillSummary,
) -> Result<()> {
    let now = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: TURN_PERMISSION_PROFILE_BACKFILL_KEY.to_owned(),
            projection_version: TURN_PERMISSION_PROFILE_BACKFILL_VERSION,
            status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: 0,
            source_turn_count: summary
                .turns_updated
                .max(summary.security_snapshots_updated)
                .max(summary.security_snapshots_repaired) as i64,
            source_turn_item_count: 0,
            source_turn_event_count: summary.turn_events_updated as i64,
            last_error: None,
            backfill_started_at: Some(now),
            backfilled_at: Some(now),
            created_at: now,
            updated_at: now,
        },
    )
    .await
}

async fn mark_backfill_failed<C: ConnectionTrait>(db: &C, error: &anyhow::Error) -> Result<()> {
    let now = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: TURN_PERMISSION_PROFILE_BACKFILL_KEY.to_owned(),
            projection_version: TURN_PERMISSION_PROFILE_BACKFILL_VERSION,
            status: PROJECTION_META_STATUS_FAILED.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: Some(format!("{error:#}")),
            backfill_started_at: None,
            backfilled_at: None,
            created_at: now,
            updated_at: now,
        },
    )
    .await
}

fn now_datetime() -> DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}

fn default_permission_profile_json() -> Result<JsonValue> {
    serde_json::from_str(DEFAULT_TURN_PERMISSION_PROFILE_SNAPSHOT_JSON)
        .context("failed to parse default permission profile snapshot")
}

fn patch_turn_event_payload(payload: &mut JsonValue, default_profile: &JsonValue) -> bool {
    let mut changed = false;

    if let Some(turn) = payload
        .get_mut("payload")
        .and_then(|value| value.get_mut("turn"))
        .and_then(JsonValue::as_object_mut)
    {
        if !has_non_null_field(&*turn, "permission_profile") {
            turn.insert("permission_profile".to_owned(), default_profile.clone());
            changed = true;
        }
    }

    if let Some(turns) = payload
        .get_mut("payload")
        .and_then(|value| value.get_mut("thread"))
        .and_then(|value| value.get_mut("turns"))
        .and_then(JsonValue::as_array_mut)
    {
        for turn in turns {
            let Some(turn) = turn.as_object_mut() else {
                continue;
            };
            if has_non_null_field(&*turn, "permission_profile") {
                continue;
            }
            turn.insert("permission_profile".to_owned(), default_profile.clone());
            changed = true;
        }
    }

    if payload.get("kind").and_then(JsonValue::as_str) == Some("turn_tool_loop_budget_exceeded") {
        if let Some(event_payload) = payload
            .get_mut("payload")
            .and_then(JsonValue::as_object_mut)
        {
            if event_payload.get("action").and_then(JsonValue::as_str) == Some("fail_turn") {
                event_payload.insert(
                    "action".to_owned(),
                    JsonValue::String("continue_in_next_window".to_owned()),
                );
                changed = true;
            }
        }
    }

    changed
}

fn has_non_null_field(object: &serde_json::Map<String, JsonValue>, field: &str) -> bool {
    object.get(field).is_some_and(|value| !value.is_null())
}

#[cfg(test)]
mod tests {
    use super::{
        TURN_PERMISSION_PROFILE_BACKFILL_KEY, TURN_PERMISSION_PROFILE_BACKFILL_VERSION,
        backfill_once, now_datetime, repair_regressed_turn_security_snapshot,
    };
    use anyhow::Context;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{
        CrudStore, NewCliRuntimeThreadBinding, PROJECTION_META_STATUS_COMPLETE,
        ProjectionMetaRecord, find_projection_meta, upsert_projection_meta,
    };
    use pioneer_entity::{task, task_agent_spec, thread, turn, turn_event, workspace};
    use pioneer_protocol::{
        TurnExecutionSecuritySnapshot, TurnFilesystemAccess, TurnFilesystemSandboxEntry,
        TurnFilesystemSandboxKind, TurnFilesystemSandboxPath, TurnNetworkMode, TurnPermissionMode,
        TurnPermissionProfileSnapshot, TurnPermissionProfileSource, TurnSandboxMode,
        TurnSecuritySnapshotSource,
    };
    use sea_orm::{Database, EntityTrait, Set};
    use sea_orm::{FromQueryResult, Statement};
    use serde_json::Value as JsonValue;

    #[test]
    fn regressed_snapshot_repair_never_widens_non_composer_authority_without_parent_metadata() {
        for source in [
            TurnSecuritySnapshotSource::GatewayDefault,
            TurnSecuritySnapshotSource::TaskInherited,
            TurnSecuritySnapshotSource::ReviewerInherited,
            TurnSecuritySnapshotSource::RevisionInherited,
            TurnSecuritySnapshotSource::RuntimeRecovery,
            TurnSecuritySnapshotSource::BackfilledLegacy,
        ] {
            let mut snapshot = TurnExecutionSecuritySnapshot::read_only(
                TurnPermissionProfileSnapshot::from_mode(
                    TurnPermissionMode::Supervised,
                    TurnPermissionProfileSource::Composer,
                ),
                "/tmp/inherited-workspace",
                vec![TurnFilesystemSandboxEntry::workspace_root(
                    TurnFilesystemAccess::Read,
                    "/tmp/inherited-workspace",
                )],
                1,
            );
            snapshot.source = source;
            snapshot.authority_cap.filesystem = snapshot.sandbox.filesystem.clone();
            snapshot.authority_cap.network = snapshot.network.clone();
            snapshot.parent_cap = None;
            let expected_filesystem = snapshot.authority_cap.filesystem.clone();
            let expected_network = snapshot.authority_cap.network.clone();

            repair_regressed_turn_security_snapshot(&mut snapshot);

            assert_eq!(
                snapshot.authority_cap.filesystem, expected_filesystem,
                "source {source:?} must remain fail-closed"
            );
            assert_eq!(
                snapshot.authority_cap.network, expected_network,
                "source {source:?} must remain fail-closed"
            );
        }
    }

    #[derive(Debug, FromQueryResult)]
    struct CandidateCount {
        count: i64,
    }

    async fn count_missing_turn_event_permission_profiles(
        db: &sea_orm::DatabaseConnection,
    ) -> anyhow::Result<i64> {
        let row = CandidateCount::find_by_statement(Statement::from_string(
            db.get_database_backend(),
            "SELECT COUNT(*) AS count \
             FROM turn_event \
             WHERE (\
                    json_type(payload, '$.payload.turn') = 'object' \
                    AND (\
                        json_type(payload, '$.payload.turn.permission_profile') IS NULL \
                        OR json_type(payload, '$.payload.turn.permission_profile') = 'null'\
                    )\
                 ) \
                OR EXISTS (\
                    SELECT 1 \
                    FROM json_each(payload, '$.payload.thread.turns') \
                    WHERE json_type(value) = 'object' \
                      AND (\
                          json_type(value, '$.permission_profile') IS NULL \
                          OR json_type(value, '$.permission_profile') = 'null'\
                      )\
               ) \
                OR (\
                    json_extract(payload, '$.kind') = 'turn_tool_loop_budget_exceeded' \
                    AND json_extract(payload, '$.payload.action') = 'fail_turn'\
               )",
        ))
        .one(db)
        .await
        .context("failed to count missing turn_event permission profiles")?;
        Ok(row.map_or(0, |row| row.count))
    }

    async fn count_missing_turn_permission_profile_columns(
        db: &sea_orm::DatabaseConnection,
    ) -> anyhow::Result<i64> {
        let row = CandidateCount::find_by_statement(Statement::from_string(
            db.get_database_backend(),
            "SELECT COUNT(*) AS count \
             FROM turn \
             WHERE permission_profile_mode IS NULL \
                OR permission_profile_source IS NULL \
                OR permission_profile_snapshot_json IS NULL",
        ))
        .one(db)
        .await
        .context("failed to count missing turn permission profile columns")?;
        Ok(row.map_or(0, |row| row.count))
    }

    async fn count_missing_full_access_turn_security_snapshots(
        db: &sea_orm::DatabaseConnection,
    ) -> anyhow::Result<i64> {
        let row = CandidateCount::find_by_statement(Statement::from_string(
            db.get_database_backend(),
            "SELECT COUNT(*) AS count \
             FROM turn \
             WHERE permission_profile_mode = 'full_access' \
               AND execution_security_snapshot_json IS NULL",
        ))
        .one(db)
        .await
        .context("failed to count missing full access turn security snapshots")?;
        Ok(row.map_or(0, |row| row.count))
    }

    #[tokio::test]
    async fn startup_backfill_patches_legacy_permission_profiles_without_resetting_refill_marker() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection.clone());
        let runtime_home = tempfile::tempdir().expect("runtime home should create");
        let now = now_datetime();
        let turn_id = "turn_legacy_permission_profile";
        let synthetic_turn_id = "turn_synthetic_workspace_snapshot";
        let task_id = "task_legacy_agent_caps";
        let thread_id = "thread_legacy_permission_profile";
        let workspace_id = "workspace_legacy_permission_profile";
        let refill_key = "thread_episodic_workspace_capsule_refill";

        workspace::Entity::insert(workspace::ActiveModel {
            id: Set(workspace_id.to_owned()),
            name: Set("Legacy workspace".to_owned()),
            is_active: Set(true),
            is_current: Set(true),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .exec(&connection)
        .await
        .expect("workspace should insert");
        thread::Entity::insert(thread::ActiveModel {
            id: Set(thread_id.to_owned()),
            workspace_id: Set(workspace_id.to_owned()),
            name: Set(Some("Legacy thread".to_owned())),
            preview: Set(String::new()),
            preview_author_json: Set(None),
            mode: Set("agent".to_owned()),
            model: Set("o4-mini".to_owned()),
            model_provider: Set("openai".to_owned()),
            status: Set("idle".to_owned()),
            origin_kind: Set("user".to_owned()),
            sidebar_visibility: Set("visible".to_owned()),
            access_class: Set("private".to_owned()),
            agent_nickname: Set(None),
            agent_role: Set(None),
            created_by_actor_id: Set(None),
            created_by_actor_kind: Set(None),
            summary: Set(None),
            summary_turn_count: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .exec(&connection)
        .await
        .expect("thread should insert");

        let synthetic_cwd = runtime_home
            .path()
            .join("workspace_filesystems")
            .join(workspace_id)
            .join("agent")
            .to_string_lossy()
            .into_owned();
        store
            .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                thread_id: thread_id.to_owned(),
                workspace_id: workspace_id.to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "native-thread-synthetic-cwd".to_owned(),
                native_session_id: None,
                native_root_thread_id: None,
                native_cwd: Some(synthetic_cwd.clone()),
                native_model: Some("o4-mini".to_owned()),
                resume_cursor_json: "{}".to_owned(),
                status: "active".to_owned(),
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("synthetic CLI thread binding should insert");

        turn::Entity::insert(turn::ActiveModel {
            id: Set(turn_id.to_owned()),
            thread_id: Set(thread_id.to_owned()),
            initiated_by_actor_id: Set(None),
            initiated_by_actor_kind: Set(None),
            status: Set("completed".to_owned()),
            error: Set(None),
            prompt_manifest_json: Set("{}".to_owned()),
            prompt_compiler_version: Set(None),
            prompt_profile: Set(None),
            prompt_fingerprint_stable: Set(None),
            prompt_fingerprint_dynamic: Set(None),
            prompt_fingerprint_full: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            reasoning_effort: Set(None),
            work_owner: Set("turn".to_owned()),
            permission_profile_mode: Set(None),
            permission_profile_source: Set(None),
            permission_profile_snapshot_json: Set(None),
            execution_security_snapshot_version: Set(None),
            execution_security_snapshot_json: Set(None),
            execution_authorization_context_json: Set(None),
            send_mode: Set(None),
            author_display_name_snapshot: Set(None),
            author_nickname_snapshot: Set(None),
            author_avatar_revision_snapshot: Set(None),
            author_agent_snapshot_json: Set(None),
            reply_to_turn_id: Set(None),
            mentions_json: Set("[]".to_owned()),
            message_revision: Set(0),
            message_deleted_at: Set(None),
            message_deleted_by_actor_id: Set(None),
            message_deleted_by_actor_kind: Set(None),
        })
        .exec(&connection)
        .await
        .expect("legacy turn should insert");

        task::Entity::insert(task::ActiveModel {
            id: Set(task_id.to_owned()),
            workspace_id: Set(workspace_id.to_owned()),
            owner_kind: Set("thread".to_owned()),
            owner_id: Set(Some(thread_id.to_owned())),
            created_by_thread_id: Set(Some(thread_id.to_owned())),
            created_by_turn_id: Set(Some(turn_id.to_owned())),
            root_task_id: Set(None),
            parent_task_id: Set(None),
            executor_kind: Set("agent".to_owned()),
            status: Set("scheduled".to_owned()),
            title: Set("Legacy task caps".to_owned()),
            goal: Set("Verify legacy task cap backfill".to_owned()),
            priority: Set(0),
            lifecycle_policy_json: Set(None),
            delivery_policy_json: Set(None),
            retry_policy_json: Set(None),
            timeout_policy_json: Set(None),
            concurrency_policy_json: Set(None),
            metadata_json: Set(None),
            result_json: Set(None),
            error_json: Set(None),
            revision: Set(1),
            created_at: Set(now),
            updated_at: Set(now),
            completed_at: Set(None),
        })
        .exec(&connection)
        .await
        .expect("legacy task should insert");
        task_agent_spec::Entity::insert(task_agent_spec::ActiveModel {
            id: Set("agent_spec_legacy_caps".to_owned()),
            task_id: Set(task_id.to_owned()),
            run_id: Set(None),
            agent_role: Set(None),
            agent_nickname: Set(None),
            model: Set(Some("o4-mini".to_owned())),
            model_provider: Set(Some("openai".to_owned())),
            prompt_json: Set(serde_json::json!({
                "goal": "Verify legacy task cap backfill",
                "instructions": []
            })
            .to_string()),
            context_policy_json: Set(None),
            tool_policy_json: Set(None),
            result_contract_json: Set(None),
            review_policy_json: Set(None),
            depth: Set(0),
            max_depth: Set(4),
            created_at: Set(now),
            updated_at: Set(now),
            permission_cap_json: Set(None),
            security_cap_json: Set(None),
        })
        .exec(&connection)
        .await
        .expect("legacy task agent spec should insert");

        let synthetic_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::AutoAcceptEdits,
            TurnPermissionProfileSource::Composer,
        );
        let mut synthetic_snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            synthetic_profile.clone(),
            synthetic_cwd.clone(),
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                synthetic_cwd.clone(),
            )],
            now.timestamp_millis(),
        );
        // Reproduce the authority/environment regression shipped before
        // backfill version 4. The initial sandbox remains the same; only the
        // maximum consent cap and process runtime contract were corrupted.
        synthetic_snapshot.authority_cap.filesystem = synthetic_snapshot.sandbox.filesystem.clone();
        synthetic_snapshot.authority_cap.network = synthetic_snapshot.network.clone();
        synthetic_snapshot.process.environment.allowed_vars.clear();
        synthetic_snapshot
            .process
            .environment
            .denied_patterns
            .clear();
        synthetic_snapshot.authority_cap.process.environment =
            synthetic_snapshot.process.environment.clone();
        let synthetic_task_security_cap =
            crate::turn_security::task_security_cap_from_snapshot(&synthetic_snapshot);
        let mut synthetic_task_security_cap_json =
            serde_json::to_value(&synthetic_task_security_cap)
                .expect("synthetic security cap should serialize");
        synthetic_task_security_cap_json
            .as_object_mut()
            .expect("synthetic security cap should be an object")
            .remove("maxFilesystemKind");
        task_agent_spec::Entity::insert(task_agent_spec::ActiveModel {
            id: Set("agent_spec_synthetic_workspace_caps".to_owned()),
            task_id: Set(task_id.to_owned()),
            run_id: Set(None),
            agent_role: Set(None),
            agent_nickname: Set(None),
            model: Set(Some("o4-mini".to_owned())),
            model_provider: Set(Some("openai".to_owned())),
            prompt_json: Set(serde_json::json!({
                "goal": "Verify synthetic task cap repair",
                "instructions": []
            })
            .to_string()),
            context_policy_json: Set(None),
            tool_policy_json: Set(None),
            result_contract_json: Set(None),
            review_policy_json: Set(None),
            depth: Set(0),
            max_depth: Set(4),
            created_at: Set(now),
            updated_at: Set(now),
            permission_cap_json: Set(Some(
                serde_json::to_string(&synthetic_task_security_cap.max_permission_profile)
                    .expect("synthetic permission cap should serialize"),
            )),
            security_cap_json: Set(Some(
                serde_json::to_string(&synthetic_task_security_cap_json)
                    .expect("synthetic security cap should serialize"),
            )),
        })
        .exec(&connection)
        .await
        .expect("task agent spec with synthetic workspace cap should insert");
        turn::Entity::insert(turn::ActiveModel {
            id: Set(synthetic_turn_id.to_owned()),
            thread_id: Set(thread_id.to_owned()),
            initiated_by_actor_id: Set(None),
            initiated_by_actor_kind: Set(None),
            status: Set("completed".to_owned()),
            error: Set(None),
            prompt_manifest_json: Set("{}".to_owned()),
            prompt_compiler_version: Set(None),
            prompt_profile: Set(None),
            prompt_fingerprint_stable: Set(None),
            prompt_fingerprint_dynamic: Set(None),
            prompt_fingerprint_full: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            reasoning_effort: Set(None),
            work_owner: Set("turn".to_owned()),
            permission_profile_mode: Set(Some("auto_accept_edits".to_owned())),
            permission_profile_source: Set(Some("composer".to_owned())),
            permission_profile_snapshot_json: Set(Some(
                serde_json::to_string(&synthetic_profile)
                    .expect("synthetic permission profile should serialize"),
            )),
            execution_security_snapshot_version: Set(Some(1)),
            execution_security_snapshot_json: Set(Some(
                serde_json::to_string(&synthetic_snapshot)
                    .expect("synthetic security snapshot should serialize"),
            )),
            execution_authorization_context_json: Set(None),
            send_mode: Set(None),
            author_display_name_snapshot: Set(None),
            author_nickname_snapshot: Set(None),
            author_avatar_revision_snapshot: Set(None),
            author_agent_snapshot_json: Set(None),
            reply_to_turn_id: Set(None),
            mentions_json: Set("[]".to_owned()),
            message_revision: Set(0),
            message_deleted_at: Set(None),
            message_deleted_by_actor_id: Set(None),
            message_deleted_by_actor_kind: Set(None),
        })
        .exec(&connection)
        .await
        .expect("turn with synthetic workspace snapshot should insert");

        turn_event::Entity::insert(turn_event::ActiveModel {
            id: Set("evt_legacy_permission_profile".to_owned()),
            thread_id: Set("thread_legacy_permission_profile".to_owned()),
            turn_id: Set(turn_id.to_owned()),
            sequence: Set(1),
            event_type: Set("turn/completed".to_owned()),
            payload: Set(serde_json::json!({
                "kind": "turn_started",
                "payload": {
                    "thread": {
                        "id": "thread_legacy_permission_profile",
                        "turns": [
                            {
                                "id": turn_id,
                                "status": "Completed"
                            },
                            {
                                "id": "turn_already_profiled",
                                "status": "Completed",
                                "permission_profile": {
                                    "mode": "supervised",
                                    "source": "composer",
                                    "effective_policy": {
                                        "default_behavior": "ask",
                                        "file_read": "allow",
                                        "file_write": "ask",
                                        "shell_command": "ask",
                                        "network": "ask",
                                        "mcp_read": "allow",
                                        "mcp_write_or_unknown": "ask",
                                        "dynamic_skill_tool": "ask",
                                        "computer_use": "ask",
                                        "task_subagent": "ask"
                                    }
                                }
                            }
                        ]
                    },
                    "sandbox_mode": "FullAccess",
                    "turn": {
                        "id": turn_id,
                        "status": "Completed",
                        "permission_profile": null
                    },
                    "input": []
                }
            })
            .to_string()),
            idempotency_key: Set(None),
            created_at: Set(now),
        })
        .exec(&connection)
        .await
        .expect("legacy turn_event should insert");

        turn_event::Entity::insert(turn_event::ActiveModel {
            id: Set("evt_legacy_tool_loop_action".to_owned()),
            thread_id: Set("thread_legacy_permission_profile".to_owned()),
            turn_id: Set(turn_id.to_owned()),
            sequence: Set(2),
            event_type: Set("turn/tool_loop/budget_exceeded".to_owned()),
            payload: Set(serde_json::json!({
                "kind": "turn_tool_loop_budget_exceeded",
                "payload": {
                    "workspace_id": "workspace_legacy_permission_profile",
                    "thread_id": "thread_legacy_permission_profile",
                    "turn_id": turn_id,
                    "limit_kind": "provider_returned_tools_after_tools_disabled",
                    "limit": 0,
                    "observed": 2,
                    "action": "fail_turn",
                    "reason": "provider_returned_tools_after_tools_disabled"
                }
            })
            .to_string()),
            idempotency_key: Set(None),
            created_at: Set(now),
        })
        .exec(&connection)
        .await
        .expect("legacy tool loop event should insert");

        upsert_projection_meta(
            &connection,
            ProjectionMetaRecord {
                projection_key: refill_key.to_owned(),
                projection_version: 1,
                status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
                source_thread_count: 1,
                source_turn_count: 1,
                source_turn_item_count: 1,
                source_turn_event_count: 1,
                last_error: None,
                backfill_started_at: Some(now),
                backfilled_at: Some(now),
                created_at: now,
                updated_at: now,
            },
        )
        .await
        .expect("refill marker should insert");
        upsert_projection_meta(
            &connection,
            ProjectionMetaRecord {
                projection_key: TURN_PERMISSION_PROFILE_BACKFILL_KEY.to_owned(),
                projection_version: 1,
                status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
                source_thread_count: 0,
                source_turn_count: 1,
                source_turn_item_count: 0,
                source_turn_event_count: 2,
                last_error: None,
                backfill_started_at: Some(now),
                backfilled_at: Some(now),
                created_at: now,
                updated_at: now,
            },
        )
        .await
        .expect("version one backfill marker should insert");
        let summary = backfill_once(&store, runtime_home.path())
            .await
            .expect("backfill should run");
        assert!(!summary.skipped);
        assert_eq!(summary.turn_events_updated, 2);
        assert_eq!(summary.turns_updated, 1);
        assert_eq!(summary.task_agent_caps_updated, 2);
        assert_eq!(summary.security_snapshots_updated, 1);
        assert_eq!(summary.security_snapshots_repaired, 1);
        assert_eq!(summary.cli_runtime_thread_bindings_removed, 1);

        assert_eq!(
            count_missing_turn_event_permission_profiles(&connection)
                .await
                .expect("must count event candidates"),
            0
        );
        assert_eq!(
            count_missing_turn_permission_profile_columns(&connection)
                .await
                .expect("must count turn candidates"),
            0
        );
        assert_eq!(
            count_missing_full_access_turn_security_snapshots(&connection)
                .await
                .expect("must count security snapshot candidates"),
            0
        );

        let task = store
            .get_task(task_id)
            .await
            .expect("legacy task should load")
            .expect("legacy task should exist");
        let agent_spec = task
            .agent_specs
            .iter()
            .find(|spec| spec.id == "agent_spec_legacy_caps")
            .expect("legacy task agent spec should load");
        let permission_cap = agent_spec
            .permission_cap
            .as_ref()
            .expect("permission cap should be backfilled");
        let security_cap = agent_spec
            .security_cap
            .as_ref()
            .expect("security cap should be backfilled");
        assert_eq!(permission_cap.mode, TurnPermissionMode::FullAccess);
        assert_eq!(
            security_cap.max_permission_profile.mode,
            TurnPermissionMode::FullAccess
        );
        assert_eq!(security_cap.max_sandbox_mode, TurnSandboxMode::Unrestricted);

        let repaired_agent_spec = task
            .agent_specs
            .iter()
            .find(|spec| spec.id == "agent_spec_synthetic_workspace_caps")
            .expect("repaired task agent spec should load");
        let repaired_cap = repaired_agent_spec
            .security_cap
            .as_ref()
            .expect("repaired security cap should remain present");
        assert_eq!(
            repaired_cap.max_filesystem_kind, None,
            "the legacy missing kind remains fail-closed and is inferred from bounded roots"
        );
        let repaired_cap_entry = repaired_cap
            .max_filesystem_entries
            .first()
            .expect("repaired security cap should keep its cwd entry");
        assert_eq!(
            repaired_cap_entry.path,
            TurnFilesystemSandboxPath::CurrentWorkingDirectory
        );
        let expected_cwd = std::env::current_dir()
            .expect("test cwd should resolve")
            .to_string_lossy()
            .into_owned();
        assert_eq!(
            repaired_cap_entry.resolved_path.as_deref(),
            Some(expected_cwd.as_str())
        );

        assert!(
            store
                .get_cli_runtime_thread_binding(thread_id)
                .await
                .expect("CLI thread binding should query")
                .is_none(),
            "synthetic CLI thread binding should be removed"
        );

        let event = turn_event::Entity::find_by_id("evt_legacy_permission_profile".to_owned())
            .one(&connection)
            .await
            .expect("event should load")
            .expect("event should exist");
        let payload: JsonValue =
            serde_json::from_str(event.payload.as_str()).expect("payload should decode");
        assert_eq!(
            payload["payload"]["turn"]["permission_profile"]["mode"],
            "full_access"
        );
        assert_eq!(
            payload["payload"]["turn"]["permission_profile"]["source"],
            "defaulted"
        );
        assert_eq!(
            payload["payload"]["thread"]["turns"][0]["permission_profile"]["mode"],
            "full_access"
        );
        assert_eq!(
            payload["payload"]["thread"]["turns"][0]["permission_profile"]["source"],
            "defaulted"
        );
        assert_eq!(
            payload["payload"]["thread"]["turns"][1]["permission_profile"]["mode"],
            "supervised"
        );
        assert_eq!(
            payload["payload"]["thread"]["turns"][1]["permission_profile"]["source"],
            "composer"
        );

        let event = turn_event::Entity::find_by_id("evt_legacy_tool_loop_action".to_owned())
            .one(&connection)
            .await
            .expect("tool loop event should load")
            .expect("tool loop event should exist");
        let payload: JsonValue =
            serde_json::from_str(event.payload.as_str()).expect("payload should decode");
        assert_eq!(payload["payload"]["action"], "continue_in_next_window");

        let turn = turn::Entity::find_by_id(turn_id.to_owned())
            .one(&connection)
            .await
            .expect("turn should load")
            .expect("turn should exist");
        assert_eq!(turn.permission_profile_mode.as_deref(), Some("full_access"));
        assert_eq!(turn.permission_profile_source.as_deref(), Some("defaulted"));
        assert!(turn.permission_profile_snapshot_json.is_some());
        assert_eq!(turn.execution_security_snapshot_version, Some(1));
        let snapshot: TurnExecutionSecuritySnapshot = serde_json::from_str(
            turn.execution_security_snapshot_json
                .as_deref()
                .expect("security snapshot should be backfilled"),
        )
        .expect("security snapshot should decode");
        assert_eq!(
            snapshot.source,
            TurnSecuritySnapshotSource::BackfilledLegacy
        );
        assert_eq!(
            snapshot.permission_profile.mode,
            TurnPermissionMode::FullAccess
        );
        assert_eq!(
            snapshot.sandbox.filesystem.kind,
            TurnFilesystemSandboxKind::Unrestricted
        );
        assert_eq!(
            snapshot.sandbox.cwd,
            std::env::current_dir()
                .expect("test cwd should resolve")
                .to_string_lossy()
                .into_owned()
        );

        let synthetic_turn = turn::Entity::find_by_id(synthetic_turn_id.to_owned())
            .one(&connection)
            .await
            .expect("synthetic turn should load")
            .expect("synthetic turn should exist");
        let synthetic_snapshot: TurnExecutionSecuritySnapshot = serde_json::from_str(
            synthetic_turn
                .execution_security_snapshot_json
                .as_deref()
                .expect("repaired snapshot should remain present"),
        )
        .expect("repaired snapshot should decode");
        assert_eq!(
            synthetic_snapshot.sandbox.cwd,
            std::env::current_dir()
                .expect("test cwd should resolve")
                .to_string_lossy()
                .into_owned()
        );
        let repaired_entry = synthetic_snapshot
            .sandbox
            .filesystem
            .entries
            .first()
            .expect("repaired restricted snapshot should keep its cwd entry");
        assert_eq!(
            repaired_entry.path,
            TurnFilesystemSandboxPath::CurrentWorkingDirectory
        );
        assert_eq!(
            repaired_entry.resolved_path.as_deref(),
            Some(synthetic_snapshot.sandbox.cwd.as_str())
        );
        assert_eq!(
            synthetic_snapshot.authority_cap.filesystem.kind,
            TurnFilesystemSandboxKind::Unrestricted
        );
        assert_eq!(
            synthetic_snapshot.authority_cap.network.mode,
            TurnNetworkMode::Enabled
        );
        assert!(
            synthetic_snapshot
                .process
                .environment
                .allowed_vars
                .iter()
                .any(|name| name == "PATH")
        );
        assert!(
            synthetic_snapshot
                .authority_cap
                .process
                .environment
                .denied_patterns
                .iter()
                .any(|pattern| pattern.contains("TOKEN"))
        );

        let refill_meta = find_projection_meta(&connection, refill_key)
            .await
            .expect("refill meta should query")
            .expect("refill marker should remain");
        assert_eq!(refill_meta.status, PROJECTION_META_STATUS_COMPLETE);

        let own_meta = find_projection_meta(&connection, TURN_PERMISSION_PROFILE_BACKFILL_KEY)
            .await
            .expect("own meta should query")
            .expect("own marker should exist");
        assert_eq!(own_meta.status, PROJECTION_META_STATUS_COMPLETE);
        assert_eq!(
            own_meta.projection_version,
            TURN_PERMISSION_PROFILE_BACKFILL_VERSION
        );

        let skipped = backfill_once(&store, runtime_home.path())
            .await
            .expect("second backfill should skip");
        assert!(skipped.skipped);
    }
}
