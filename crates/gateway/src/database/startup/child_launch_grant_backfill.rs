use anyhow::{Context, Result};
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_BACKFILLING, PROJECTION_META_STATUS_COMPLETE,
    PROJECTION_META_STATUS_FAILED, ProjectionMetaRecord, find_projection_meta,
    upgrade_agent_execution_grant_model_to_current, upgrade_task_actor_contract_model_to_current,
    upsert_projection_meta,
};
use pioneer_entity::{agent_execution_grant, task_actor_contract};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, TransactionTrait, entity::prelude::DateTimeWithTimeZone, sea_query::Expr,
};
use tracing::{info, warn};

const CHILD_LAUNCH_GRANT_BACKFILL_KEY: &str = "child_launch_grant_contract_backfill";
const CHILD_LAUNCH_GRANT_BACKFILL_BATCH_SIZE: u64 = 64;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ChildLaunchGrantBackfillSummary {
    pub(crate) skipped: bool,
    pub(crate) batches: u64,
    pub(crate) task_contracts_scanned: u64,
    pub(crate) execution_grants_scanned: u64,
    pub(crate) task_contracts_upgraded: u64,
    pub(crate) execution_grants_upgraded: u64,
    pub(crate) task_contracts_rejected: u64,
    pub(crate) execution_grants_rejected: u64,
}

pub(super) async fn run(crud_store: &CrudStore) -> Result<()> {
    match backfill_once(crud_store).await {
        Ok(summary) if summary.skipped => {}
        Ok(summary) => {
            info!(
                batches = summary.batches,
                task_contracts_scanned = summary.task_contracts_scanned,
                execution_grants_scanned = summary.execution_grants_scanned,
                task_contracts_upgraded = summary.task_contracts_upgraded,
                execution_grants_upgraded = summary.execution_grants_upgraded,
                task_contracts_rejected = summary.task_contracts_rejected,
                execution_grants_rejected = summary.execution_grants_rejected,
                "child launch grant background backfill completed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "child launch grant background backfill failed"
            );
            return Err(error);
        }
    }
    Ok(())
}

pub(crate) async fn backfill_once(
    crud_store: &CrudStore,
) -> Result<ChildLaunchGrantBackfillSummary> {
    let crud_store = crud_store.with_maintenance_access();
    let database = crud_store.database_connection();
    if backfill_is_current(&database).await? {
        return Ok(ChildLaunchGrantBackfillSummary {
            skipped: true,
            ..Default::default()
        });
    }

    let started_at = now_datetime();
    upsert_projection_meta(
        &database,
        projection_meta_record(
            PROJECTION_META_STATUS_BACKFILLING,
            None,
            started_at.clone(),
            None,
            &ChildLaunchGrantBackfillSummary::default(),
        ),
    )
    .await?;

    let result = backfill_all_batches(&crud_store).await;
    match result {
        Ok(summary) => {
            upsert_projection_meta(
                &database,
                projection_meta_record(
                    PROJECTION_META_STATUS_COMPLETE,
                    None,
                    started_at,
                    Some(now_datetime()),
                    &summary,
                ),
            )
            .await?;
            Ok(summary)
        }
        Err(error) => {
            upsert_projection_meta(
                &database,
                projection_meta_record(
                    PROJECTION_META_STATUS_FAILED,
                    Some(format!("{error:#}")),
                    started_at,
                    None,
                    &ChildLaunchGrantBackfillSummary::default(),
                ),
            )
            .await?;
            Err(error)
        }
    }
}

async fn backfill_all_batches(crud_store: &CrudStore) -> Result<ChildLaunchGrantBackfillSummary> {
    let mut summary = ChildLaunchGrantBackfillSummary::default();
    for target in [
        BackfillTarget::TaskActorContracts,
        BackfillTarget::ExecutionGrants,
    ] {
        let mut after_id = None;
        loop {
            let batch = run_low_priority_batch(crud_store, target, after_id.as_deref()).await?;
            if batch.scanned == 0 {
                break;
            }
            summary.batches = summary.batches.saturating_add(1);
            match target {
                BackfillTarget::TaskActorContracts => {
                    summary.task_contracts_scanned =
                        summary.task_contracts_scanned.saturating_add(batch.scanned);
                    summary.task_contracts_upgraded = summary
                        .task_contracts_upgraded
                        .saturating_add(batch.upgraded);
                    summary.task_contracts_rejected = summary
                        .task_contracts_rejected
                        .saturating_add(batch.rejected);
                }
                BackfillTarget::ExecutionGrants => {
                    summary.execution_grants_scanned = summary
                        .execution_grants_scanned
                        .saturating_add(batch.scanned);
                    summary.execution_grants_upgraded = summary
                        .execution_grants_upgraded
                        .saturating_add(batch.upgraded);
                    summary.execution_grants_rejected = summary
                        .execution_grants_rejected
                        .saturating_add(batch.rejected);
                }
            }
            after_id = batch.last_id;
        }
    }
    Ok(summary)
}

#[derive(Clone, Copy)]
enum BackfillTarget {
    TaskActorContracts,
    ExecutionGrants,
}

#[derive(Debug, Clone, Default)]
struct BackfillBatchResult {
    last_id: Option<String>,
    scanned: u64,
    upgraded: u64,
    rejected: u64,
}

#[derive(Debug, Clone)]
struct PreparedTaskActorContractUpdate {
    task_id: String,
    stored_grant_json: String,
    migrated_grant_json: String,
}

#[derive(Debug, Clone)]
struct PreparedExecutionGrantUpdate {
    grant_id: String,
    stored_grant_json: String,
    stored_grant_fingerprint: String,
    migrated_grant_json: String,
    migrated_grant_fingerprint: String,
}

#[derive(Debug, Clone)]
enum PreparedBackfillBatch {
    TaskActorContracts {
        summary: BackfillBatchResult,
        updates: Vec<PreparedTaskActorContractUpdate>,
    },
    ExecutionGrants {
        summary: BackfillBatchResult,
        updates: Vec<PreparedExecutionGrantUpdate>,
    },
}

impl PreparedBackfillBatch {
    fn summary(&self) -> &BackfillBatchResult {
        match self {
            Self::TaskActorContracts { summary, .. } | Self::ExecutionGrants { summary, .. } => {
                summary
            }
        }
    }

    fn has_updates(&self) -> bool {
        match self {
            Self::TaskActorContracts { updates, .. } => !updates.is_empty(),
            Self::ExecutionGrants { updates, .. } => !updates.is_empty(),
        }
    }
}

async fn run_low_priority_batch(
    crud_store: &CrudStore,
    target: BackfillTarget,
    after_id: Option<&str>,
) -> Result<BackfillBatchResult> {
    let database = crud_store.database_connection();
    let after_id = after_id.map(str::to_owned);
    let prepared = crud_store
        .run_background_database_quantum(|| {
            let database = database.clone();
            let after_id = after_id.clone();
            async move {
                match target {
                    BackfillTarget::TaskActorContracts => {
                        prepare_task_actor_contract_batch(&database, after_id.as_deref()).await
                    }
                    BackfillTarget::ExecutionGrants => {
                        prepare_execution_grant_batch(&database, after_id.as_deref()).await
                    }
                }
            }
        })
        .await?;
    let result = if prepared.has_updates() {
        crud_store
            .run_background_database_quantum(|| {
                let database = database.clone();
                let prepared = prepared.clone();
                async move {
                    let transaction = database
                        .begin()
                        .await
                        .context("failed to begin child launch grant backfill batch")?;
                    let result = apply_prepared_backfill_batch(&transaction, &prepared).await;
                    match result {
                        Ok(summary) => {
                            transaction
                                .commit()
                                .await
                                .context("failed to commit child launch grant backfill batch")?;
                            Ok(summary)
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            Err(error)
                        }
                    }
                }
            })
            .await?
    } else {
        prepared.summary().clone()
    };
    super::maintenance_checkpoint().await?;
    Ok(result)
}

async fn prepare_task_actor_contract_batch<C: ConnectionTrait>(
    database: &C,
    after_id: Option<&str>,
) -> Result<PreparedBackfillBatch> {
    let mut query = task_actor_contract::Entity::find()
        .filter(task_actor_contract::Column::DerivedChildLaunchGrantJson.is_not_null())
        .order_by_asc(task_actor_contract::Column::TaskId)
        .limit(CHILD_LAUNCH_GRANT_BACKFILL_BATCH_SIZE);
    if let Some(after_id) = after_id {
        query = query.filter(task_actor_contract::Column::TaskId.gt(after_id.to_owned()));
    }
    let rows = query
        .all(database)
        .await
        .context("failed to scan Task child launch grants")?;
    let mut summary = BackfillBatchResult::default();
    let mut updates = Vec::new();
    for row in rows {
        summary.scanned = summary.scanned.saturating_add(1);
        summary.last_id = Some(row.task_id.clone());
        let migrated = match upgrade_task_actor_contract_model_to_current(row.clone()) {
            Ok(migrated) => migrated,
            Err(error) => {
                summary.rejected = summary.rejected.saturating_add(1);
                warn!(
                    task_id = %row.task_id,
                    error = %format!("{error:#}"),
                    "Task child launch grant rejected during background backfill"
                );
                continue;
            }
        };
        if migrated.derived_child_launch_grant_json == row.derived_child_launch_grant_json {
            continue;
        }
        let Some(stored_grant_json) = row.derived_child_launch_grant_json else {
            continue;
        };
        let Some(migrated_grant_json) = migrated.derived_child_launch_grant_json else {
            summary.rejected = summary.rejected.saturating_add(1);
            warn!(
                task_id = %row.task_id,
                "Task child launch grant migration unexpectedly removed the stored grant"
            );
            continue;
        };
        updates.push(PreparedTaskActorContractUpdate {
            task_id: row.task_id,
            stored_grant_json,
            migrated_grant_json,
        });
    }
    Ok(PreparedBackfillBatch::TaskActorContracts { summary, updates })
}

async fn prepare_execution_grant_batch<C: ConnectionTrait>(
    database: &C,
    after_id: Option<&str>,
) -> Result<PreparedBackfillBatch> {
    let mut query = agent_execution_grant::Entity::find()
        .order_by_asc(agent_execution_grant::Column::Id)
        .limit(CHILD_LAUNCH_GRANT_BACKFILL_BATCH_SIZE);
    if let Some(after_id) = after_id {
        query = query.filter(agent_execution_grant::Column::Id.gt(after_id.to_owned()));
    }
    let rows = query
        .all(database)
        .await
        .context("failed to scan AgentExecution child launch grants")?;
    let mut summary = BackfillBatchResult::default();
    let mut updates = Vec::new();
    for row in rows {
        summary.scanned = summary.scanned.saturating_add(1);
        summary.last_id = Some(row.id.clone());
        let migrated = match upgrade_agent_execution_grant_model_to_current(row.clone()) {
            Ok(migrated) => migrated,
            Err(error) => {
                summary.rejected = summary.rejected.saturating_add(1);
                warn!(
                    grant_id = %row.id,
                    execution_id = %row.execution_id,
                    error = %format!("{error:#}"),
                    "AgentExecution child launch grant rejected during background backfill"
                );
                continue;
            }
        };
        if migrated.grant_json == row.grant_json
            && migrated.grant_fingerprint == row.grant_fingerprint
        {
            continue;
        }
        updates.push(PreparedExecutionGrantUpdate {
            grant_id: row.id,
            stored_grant_json: row.grant_json,
            stored_grant_fingerprint: row.grant_fingerprint,
            migrated_grant_json: migrated.grant_json,
            migrated_grant_fingerprint: migrated.grant_fingerprint,
        });
    }
    Ok(PreparedBackfillBatch::ExecutionGrants { summary, updates })
}

async fn apply_prepared_backfill_batch(
    database: &DatabaseTransaction,
    prepared: &PreparedBackfillBatch,
) -> Result<BackfillBatchResult> {
    let mut summary = prepared.summary().clone();
    match prepared {
        PreparedBackfillBatch::TaskActorContracts { updates, .. } => {
            for update in updates {
                let affected = task_actor_contract::Entity::update_many()
                    .col_expr(
                        task_actor_contract::Column::DerivedChildLaunchGrantJson,
                        Expr::value(update.migrated_grant_json.clone()),
                    )
                    .filter(task_actor_contract::Column::TaskId.eq(update.task_id.clone()))
                    .filter(
                        task_actor_contract::Column::DerivedChildLaunchGrantJson
                            .eq(update.stored_grant_json.clone()),
                    )
                    .exec(database)
                    .await
                    .context("failed to persist migrated Task child launch grant")?
                    .rows_affected;
                summary.upgraded = summary.upgraded.saturating_add(affected);
            }
        }
        PreparedBackfillBatch::ExecutionGrants { updates, .. } => {
            for update in updates {
                let affected = agent_execution_grant::Entity::update_many()
                    .col_expr(
                        agent_execution_grant::Column::GrantJson,
                        Expr::value(update.migrated_grant_json.clone()),
                    )
                    .col_expr(
                        agent_execution_grant::Column::GrantFingerprint,
                        Expr::value(update.migrated_grant_fingerprint.clone()),
                    )
                    .filter(agent_execution_grant::Column::Id.eq(update.grant_id.clone()))
                    .filter(
                        agent_execution_grant::Column::GrantJson
                            .eq(update.stored_grant_json.clone()),
                    )
                    .filter(
                        agent_execution_grant::Column::GrantFingerprint
                            .eq(update.stored_grant_fingerprint.clone()),
                    )
                    .exec(database)
                    .await
                    .context("failed to persist migrated AgentExecution child launch grant")?
                    .rows_affected;
                summary.upgraded = summary.upgraded.saturating_add(affected);
            }
        }
    }
    Ok(summary)
}

async fn backfill_is_current<C: ConnectionTrait>(database: &C) -> Result<bool> {
    let Some(meta) = find_projection_meta(database, CHILD_LAUNCH_GRANT_BACKFILL_KEY).await? else {
        return Ok(false);
    };
    // An older binary in a rolling deployment must never replace a marker
    // written by a newer contract migrator. It cannot improve newer rows and
    // would otherwise downgrade only the bookkeeping version.
    Ok(meta.projection_version > current_contract_version()
        || (meta.projection_version == current_contract_version()
            && meta.status == PROJECTION_META_STATUS_COMPLETE))
}

fn projection_meta_record(
    status: &str,
    last_error: Option<String>,
    started_at: DateTimeWithTimeZone,
    backfilled_at: Option<DateTimeWithTimeZone>,
    summary: &ChildLaunchGrantBackfillSummary,
) -> ProjectionMetaRecord {
    let now = now_datetime();
    ProjectionMetaRecord {
        projection_key: CHILD_LAUNCH_GRANT_BACKFILL_KEY.to_owned(),
        projection_version: current_contract_version(),
        status: status.to_owned(),
        source_thread_count: saturating_i64(summary.task_contracts_scanned),
        source_turn_count: saturating_i64(summary.execution_grants_scanned),
        source_turn_item_count: saturating_i64(
            summary
                .task_contracts_upgraded
                .saturating_add(summary.execution_grants_upgraded),
        ),
        source_turn_event_count: saturating_i64(
            summary
                .task_contracts_rejected
                .saturating_add(summary.execution_grants_rejected),
        ),
        last_error,
        backfill_started_at: Some(started_at),
        backfilled_at,
        created_at: now.clone(),
        updated_at: now,
    }
}

fn current_contract_version() -> i64 {
    i64::from(pioneer_protocol::ChildAgentLaunchGrantSet::VERSION)
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn now_datetime() -> DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_entity::{agent_execution, agent_identity, workspace};
    use sea_orm::{ActiveModelTrait, Database, EntityTrait, IntoActiveModel, Set};

    fn version_one_child_launch_grant() -> serde_json::Value {
        serde_json::json!({
            "version": 1,
            "identities": [{
                "id": "A00000000000000000001",
                "source_kind": "native_agent",
                "display_name": "Pioneer",
                "nickname": "pioneer",
                "source_revision": 3,
                "source_fingerprint": "generation-a"
            }],
            "allowInheritParentIdentity": false,
            "allowServerDerivedEphemeral": false,
            "profiles": [{
                "id": "P00000000000000000003",
                "compatibleAgentIdentityIds": ["A00000000000000000001"],
                "backend": "api_provider",
                "providerId": "provider-opaque",
                "modelId": "model-opaque",
                "providerDisplayName": "Provider",
                "modelDisplayName": "Model",
                "allowedReasoning": [{ "effort": "medium" }],
                "allowedPermissionProfiles": ["full_access"],
                "catalogGeneration": 4,
                "policyGeneration": 5,
                "fingerprint": "profile-fingerprint"
            }],
            "allowInheritParentProfile": false,
            "skillIds": [],
            "mcpServerIds": [],
            "maxPermissionProfile": {
                "mode": "supervised",
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
            },
            "maxReasoning": { "allowed": [{ "effort": "medium" }] },
            "fingerprint": "fc747ed91c91bd27b7ed9378961fedb2c57de5eba831ae5bfd5e3745e29e0af3"
        })
    }

    fn task_launch_json(child: &serde_json::Value) -> String {
        serde_json::json!({
            "kind": "resolved_task_launch",
            "identity": child["identities"][0].clone(),
            "profile": child["profiles"][0].clone(),
            "role_key": "member",
            "agent_policy_generation": 1,
            "allowed_actions": ["create_task"],
            "agent_authorization_fingerprint": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "child_launch_grant": child
        })
        .to_string()
    }

    async fn insert_task_actor_contract(
        database: &sea_orm::DatabaseConnection,
        task_id: &str,
        grant_json: &str,
    ) {
        let now = now_datetime();
        let identity_selection = pioneer_protocol::AgentIdentitySelection::Exact {
            agent_identity_id: pioneer_protocol::AgentIdentityId::new(
                "A00000000000000000001".to_owned(),
            )
            .expect("identity id should be valid"),
        };
        let launch = pioneer_protocol::AgentLaunchSelection {
            agent: identity_selection.clone(),
            execution: pioneer_protocol::AgentExecutionSelection {
                profile: pioneer_protocol::AgentExecutionProfileSelection::Exact {
                    profile_id: pioneer_protocol::AgentExecutionProfileId::new(
                        "P00000000000000000003".to_owned(),
                    )
                    .expect("profile id should be valid"),
                },
                reasoning: None,
                permission_profile: None,
                skill_ids: Vec::new(),
                mcp_server_ids: Vec::new(),
            },
        };
        let delivery = pioneer_protocol::TaskDeliveryActorContract {
            enabled: false,
            destination_thread_id: None,
            destination_user_id: None,
            destination_webhook_url_fingerprint: None,
            route_id: None,
            return_route_id: None,
            author_snapshot: None,
            route_receipt_json: None,
            disclosure_generation: 1,
            route_expires_at_millis: None,
        };
        task_actor_contract::Entity::insert(task_actor_contract::ActiveModel {
            task_id: Set(task_id.to_owned()),
            workspace_id: Set("W00000000000000000001".to_owned()),
            creator_json: Set(
                serde_json::to_string(&pioneer_protocol::PersistedActorRef::System)
                    .expect("creator should serialize"),
            ),
            creator_snapshot_json: Set(None),
            reviewer_json: Set(
                serde_json::to_string(&pioneer_protocol::TaskReviewerIntent::RuntimeAuto)
                    .expect("reviewer should serialize"),
            ),
            delivery_json: Set(
                serde_json::to_string(&delivery).expect("delivery should serialize"),
            ),
            launch_selection_json: Set(Some(
                serde_json::to_string(&launch).expect("launch should serialize"),
            )),
            requested_identity_json: Set(Some(
                serde_json::to_string(&identity_selection)
                    .expect("identity selection should serialize"),
            )),
            resolved_identity_id: Set(Some("A00000000000000000001".to_owned())),
            resolved_profile_id: Set(Some("P00000000000000000003".to_owned())),
            source_config_fingerprint: Set(Some("generation-a".to_owned())),
            derived_child_launch_grant_json: Set(Some(grant_json.to_owned())),
            execution_destination_thread_id: Set(None),
            execution_route_id: Set(None),
            execution_route_receipt_json: Set(None),
            execution_route_expires_at_millis: Set(None),
            creator_work_graph_root_execution_id: Set(None),
            work_graph_root_execution_id: Set(None),
            root_resource_scope_id: Set(None),
            accounting_attribution_json: Set(None),
            controller_principal_id: Set(None),
            revision: Set(1),
            created_at: Set(now.clone()),
            updated_at: Set(now),
        })
        .exec(database)
        .await
        .expect("Task actor contract should insert");
    }

    async fn insert_execution_grant(
        database: &sea_orm::DatabaseConnection,
        child: &serde_json::Value,
    ) {
        let now = now_datetime();
        workspace::Entity::insert(workspace::ActiveModel {
            id: Set("W00000000000000000001".to_owned()),
            name: Set("Workspace".to_owned()),
            is_active: Set(true),
            is_current: Set(true),
            created_at: Set(now.clone()),
            updated_at: Set(now.clone()),
        })
        .exec(database)
        .await
        .expect("workspace should insert");
        agent_identity::Entity::insert(agent_identity::ActiveModel {
            id: Set("A00000000000000000001".to_owned()),
            workspace_id: Set("W00000000000000000001".to_owned()),
            source_kind: Set("native_agent".to_owned()),
            source_id: Set("pioneer".to_owned()),
            source_revision: Set(3),
            source_fingerprint: Set("generation-a".to_owned()),
            status: Set("active".to_owned()),
            created_at: Set(now.clone()),
            updated_at: Set(now.clone()),
            retired_at: Set(None),
        })
        .exec(database)
        .await
        .expect("identity should insert");
        agent_execution::Entity::insert(agent_execution::ActiveModel {
            id: Set("E00000000000000000001".to_owned()),
            workspace_id: Set("W00000000000000000001".to_owned()),
            agent_identity_id: Set("A00000000000000000001".to_owned()),
            identity_source_revision: Set(3),
            identity_source_fingerprint: Set("generation-a".to_owned()),
            parent_execution_id: Set(None),
            parent_task_id: Set(None),
            parent_thread_id: Set(None),
            home_root_thread_id: Set("H00000000000000000001".to_owned()),
            work_graph_root_execution_id: Set("E00000000000000000001".to_owned()),
            requested_identity_selection_json: Set("{}".to_owned()),
            requested_profile_selection_json: Set("{}".to_owned()),
            resolved_profile_id: Set(Some("P00000000000000000003".to_owned())),
            resolved_profile_fingerprint: Set(Some("profile-fingerprint".to_owned())),
            presentation_snapshot_id: Set(None),
            authorization_context_fingerprint: Set("authorization".to_owned()),
            execution_generation: Set(1),
            status: Set("running".to_owned()),
            created_at: Set(now.clone()),
            updated_at: Set(now.clone()),
            finished_at: Set(None),
        })
        .exec(database)
        .await
        .expect("execution should insert");
        let grant_json = serde_json::json!({
            "kind": "test",
            "identity": child["identities"][0].clone(),
            "profile": child["profiles"][0].clone(),
            "child_launch_grant": child
        })
        .to_string();
        let grant_fingerprint = pioneer_crud::agent_execution_grant_fingerprint(&grant_json)
            .expect("execution grant fingerprint");
        agent_execution_grant::Entity::insert(agent_execution_grant::ActiveModel {
            id: Set("G00000000000000000001".to_owned()),
            execution_id: Set("E00000000000000000001".to_owned()),
            parent_execution_id: Set(None),
            child_identity_id: Set("A00000000000000000001".to_owned()),
            grant_fingerprint: Set(grant_fingerprint),
            grant_json: Set(grant_json),
            created_at: Set(now),
        })
        .exec(database)
        .await
        .expect("execution grant should insert");
    }

    #[tokio::test]
    async fn background_backfill_upgrades_both_containers_without_poison_row_head_of_line() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should connect");
        Migrator::up(&database, None)
            .await
            .expect("schema should migrate");
        let child = version_one_child_launch_grant();
        let task_launch = task_launch_json(&child);
        insert_task_actor_contract(&database, "T00000000000000000001", task_launch.as_str()).await;
        insert_task_actor_contract(&database, "T00000000000000000002", "{}").await;
        insert_task_actor_contract(&database, "T00000000000000000003", task_launch.as_str()).await;
        let mismatched = task_actor_contract::Entity::find_by_id("T00000000000000000003")
            .one(&database)
            .await
            .expect("mismatched Task query should succeed")
            .expect("mismatched Task should exist");
        let mut mismatched = mismatched.into_active_model();
        mismatched.resolved_identity_id = Set(Some("A99999999999999999999".to_owned()));
        mismatched
            .update(&database)
            .await
            .expect("test should corrupt outer Task actor facts");
        insert_execution_grant(&database, &child).await;
        let valid_execution_grant =
            agent_execution_grant::Entity::find_by_id("G00000000000000000001")
                .one(&database)
                .await
                .expect("valid execution grant query should succeed")
                .expect("valid execution grant should exist");
        let invalid_execution_fingerprint = "0".repeat(64);
        agent_execution_grant::Entity::insert(agent_execution_grant::ActiveModel {
            id: Set("G00000000000000000002".to_owned()),
            execution_id: Set(valid_execution_grant.execution_id.clone()),
            parent_execution_id: Set(valid_execution_grant.parent_execution_id.clone()),
            child_identity_id: Set(valid_execution_grant.child_identity_id.clone()),
            grant_fingerprint: Set(invalid_execution_fingerprint.clone()),
            grant_json: Set(valid_execution_grant.grant_json.clone()),
            created_at: Set(valid_execution_grant.created_at),
        })
        .exec(&database)
        .await
        .expect("invalid execution grant fixture should insert");

        let summary = backfill_once(&CrudStore::new(database.clone()))
            .await
            .expect("per-record rejection must not fail the background stage");
        assert_eq!(summary.task_contracts_scanned, 3);
        assert_eq!(summary.task_contracts_upgraded, 1);
        assert_eq!(summary.task_contracts_rejected, 2);
        assert_eq!(summary.execution_grants_scanned, 2);
        assert_eq!(summary.execution_grants_upgraded, 1);
        assert_eq!(summary.execution_grants_rejected, 1);

        let task = task_actor_contract::Entity::find_by_id("T00000000000000000001")
            .one(&database)
            .await
            .expect("Task query should succeed")
            .expect("Task actor contract should remain");
        let task_json: serde_json::Value = serde_json::from_str(
            task.derived_child_launch_grant_json
                .as_deref()
                .expect("Task child grant should remain"),
        )
        .expect("Task child grant should decode");
        assert_eq!(
            task_json["child_launch_grant"]["version"],
            serde_json::json!(2)
        );
        let malformed_task = task_actor_contract::Entity::find_by_id("T00000000000000000002")
            .one(&database)
            .await
            .expect("malformed Task query should succeed")
            .expect("malformed Task should remain");
        assert_eq!(
            malformed_task.derived_child_launch_grant_json.as_deref(),
            Some("{}"),
            "a rejected Task contract must not be partially rewritten"
        );
        let mismatched_task = task_actor_contract::Entity::find_by_id("T00000000000000000003")
            .one(&database)
            .await
            .expect("mismatched Task query should succeed")
            .expect("mismatched Task should remain");
        assert_eq!(
            mismatched_task.resolved_identity_id.as_deref(),
            Some("A99999999999999999999")
        );
        let mismatched_task_json: serde_json::Value = serde_json::from_str(
            mismatched_task
                .derived_child_launch_grant_json
                .as_deref()
                .expect("mismatched Task child grant should remain"),
        )
        .expect("mismatched Task child grant should decode");
        assert_eq!(
            mismatched_task_json["child_launch_grant"]["version"],
            serde_json::json!(1),
            "a rejected outer Task contract must retain its authenticated V1 child grant"
        );

        let grant = agent_execution_grant::Entity::find_by_id("G00000000000000000001")
            .one(&database)
            .await
            .expect("grant query should succeed")
            .expect("grant should remain");
        let grant_json: serde_json::Value =
            serde_json::from_str(&grant.grant_json).expect("grant should decode");
        assert_eq!(
            grant_json["child_launch_grant"]["version"],
            serde_json::json!(2)
        );
        assert_eq!(
            grant.grant_fingerprint,
            pioneer_crud::agent_execution_grant_fingerprint(&grant.grant_json)
                .expect("outer fingerprint should be migrated atomically")
        );
        let invalid_grant = agent_execution_grant::Entity::find_by_id("G00000000000000000002")
            .one(&database)
            .await
            .expect("invalid grant query should succeed")
            .expect("invalid grant should remain");
        assert_eq!(
            invalid_grant.grant_fingerprint,
            invalid_execution_fingerprint
        );
        let invalid_grant_json: serde_json::Value =
            serde_json::from_str(&invalid_grant.grant_json).expect("invalid grant should decode");
        assert_eq!(
            invalid_grant_json["child_launch_grant"]["version"],
            serde_json::json!(1),
            "a rejected execution grant must not be partially rewritten"
        );

        let second = backfill_once(&CrudStore::new(database))
            .await
            .expect("completed marker should be readable");
        assert!(second.skipped);
    }
}
