use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, anyhow, bail};
use pioneer_protocol::{
    AuthSessionId, DeviceId, GatewayId, PrincipalId, RoleKey, SkillId, Task, TaskExecutorKind,
    TaskOccurrenceContract, TaskOccurrenceStatus, TaskOwnerKind, TaskRun, TaskRunStatus,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use tracing::info;

use crate::message::MessageProcessor;

/// Completes the one-time Agent-domain data conversion before the Gateway
/// listener starts. The final runtime assumes these rows exist and contains
/// no alternate path for an older Task representation.
pub(crate) async fn apply(processor: &MessageProcessor) -> Result<()> {
    normalize_execution_authorization_contexts(processor).await?;
    let task_ids = tasks_requiring_conversion(processor).await?;
    let mut converted_tasks = 0_usize;
    let mut converted_occurrences = 0_usize;
    let mut converted_deliveries = 0_usize;

    for task_id in task_ids {
        let response = processor
            .crud_store
            .get_task(task_id.as_str())
            .await?
            .with_context(|| format!("Task `{task_id}` disappeared during database upgrade"))?;

        let actor_contract = match processor
            .crud_store
            .get_task_actor_contract(task_id.as_str())
            .await?
        {
            Some(contract) => contract,
            None => {
                let contract = build_actor_contract(processor, &response).await?;
                processor
                    .crud_store
                    .upsert_task_actor_contract(&contract, response.task.updated_at)
                    .await?;
                converted_tasks = converted_tasks.saturating_add(1);
                contract
            }
        };

        converted_occurrences = converted_occurrences.saturating_add(
            convert_occurrences(processor, &response.task, &response.runs, &actor_contract).await?,
        );
        converted_deliveries = converted_deliveries
            .saturating_add(convert_deliveries(processor, &response.task, &actor_contract).await?);
    }

    converted_deliveries =
        converted_deliveries.saturating_add(convert_remaining_deliveries(processor).await?);

    verify_conversion(processor).await?;
    info!(
        converted_tasks,
        converted_occurrences, converted_deliveries, "Agent domain data upgrade is complete"
    );
    Ok(())
}

async fn normalize_execution_authorization_contexts(processor: &MessageProcessor) -> Result<()> {
    let database = processor.crud_store.database_connection();
    for (table, column) in [
        ("task_execution_admission", "authorization_context_json"),
        ("turn", "execution_authorization_context_json"),
    ] {
        let current = format!("{table}.{column}");
        let sql = format!(
            "UPDATE {table} SET {column} = json_set(\
                {current}, \
                '$.human_interaction_budget', json_object(\
                    'max_pending_requests_per_execution', \
                    COALESCE(json_extract({current}, '$.human_interaction_budget.max_pending_requests_per_execution'), 8)\
                ), \
                '$.mcp_invocation_limits', json_object(\
                    'profile_version', 5, \
                    'max_arguments_bytes', \
                    COALESCE(json_extract({current}, '$.mcp_invocation_limits.max_arguments_bytes'), 131072), \
                    'max_queue_wait_ms', \
                    COALESCE(\
                        json_extract({current}, '$.mcp_invocation_limits.max_queue_wait_ms'), \
                        json_extract({current}, '$.mcp_invocation_limits.max_timeout_ms'), \
                        120000\
                    ), \
                    'max_concurrent_calls', \
                    COALESCE(json_extract({current}, '$.mcp_invocation_limits.max_concurrent_calls'), 8), \
                    'max_queued_calls', \
                    COALESCE(json_extract({current}, '$.mcp_invocation_limits.max_queued_calls'), 16)\
                ), \
                '$.native_event_budget', json_object(\
                    'profile_version', 2, \
                    'max_frame_bytes', \
                    COALESCE(json_extract({current}, '$.native_event_budget.max_frame_bytes'), 1048576), \
                    'max_recovery_frame_bytes', MAX(\
                        COALESCE(\
                            json_extract({current}, '$.native_event_budget.max_recovery_frame_bytes'), \
                            67108864\
                        ), \
                        COALESCE(json_extract({current}, '$.native_event_budget.max_frame_bytes'), 1048576)\
                    )\
                )\
             ) \
             WHERE {column} IS NOT NULL AND (\
                 json_type({current}, '$.human_interaction_budget') IS NULL OR \
                 json_type({current}, '$.human_interaction_budget.max_questions_per_request') IS NOT NULL OR \
                 json_type({current}, '$.human_interaction_budget.max_pending_requests_per_execution') IS NULL OR \
                 COALESCE(json_extract({current}, '$.mcp_invocation_limits.profile_version'), 0) <> 5 OR \
                 json_type({current}, '$.mcp_invocation_limits.max_arguments_depth') IS NOT NULL OR \
                 json_type({current}, '$.mcp_invocation_limits.max_queue_wait_ms') IS NULL OR \
                 COALESCE(json_extract({current}, '$.native_event_budget.profile_version'), 0) <> 2 OR \
                 json_type({current}, '$.native_event_budget.max_json_depth') IS NOT NULL OR \
                 json_type({current}, '$.native_event_budget.max_recovery_frame_bytes') IS NULL\
             )"
        );
        database
            .execute_raw(Statement::from_string(DatabaseBackend::Sqlite, sql))
            .await
            .with_context(|| {
                format!("failed to normalize final execution authorization data in `{table}`")
            })?;
    }
    Ok(())
}

#[derive(Debug)]
struct DeliveryConversionRow {
    delivery_id: String,
    task_id: String,
    run_id: String,
    idempotency_key: String,
    status: String,
    reviewer_json: Option<String>,
    updated_at: i64,
}

async fn convert_deliveries(
    processor: &MessageProcessor,
    task: &Task,
    actor_contract: &pioneer_protocol::TaskActorContract,
) -> Result<usize> {
    let rows = deliveries_requiring_conversion(processor, Some(task.id.as_str())).await?;
    convert_delivery_rows(processor, rows, |task_id| {
        (task_id == task.id).then_some(actor_contract)
    })
    .await
}

async fn convert_remaining_deliveries(processor: &MessageProcessor) -> Result<usize> {
    let rows = deliveries_requiring_conversion(processor, None).await?;
    let mut contracts = HashMap::new();
    for row in &rows {
        if !contracts.contains_key(row.task_id.as_str()) {
            let contract = processor
                .crud_store
                .get_task_actor_contract(row.task_id.as_str())
                .await?
                .with_context(|| {
                    format!(
                        "Task delivery `{}` has no converted actor contract",
                        row.delivery_id
                    )
                })?;
            contracts.insert(row.task_id.clone(), contract);
        }
    }
    convert_delivery_rows(processor, rows, |task_id| contracts.get(task_id)).await
}

async fn deliveries_requiring_conversion(
    processor: &MessageProcessor,
    task_id: Option<&str>,
) -> Result<Vec<DeliveryConversionRow>> {
    let database = processor.crud_store.database_connection();
    let task_filter = task_id.map_or_else(String::new, |_| " AND delivery.task_id = ?".to_owned());
    let statement = if let Some(task_id) = task_id {
        Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            format!(
                "SELECT delivery.id AS delivery_id, delivery.task_id, delivery.run_id, \
                        delivery.delivery_key AS idempotency_key, delivery.status, \
                        CASE WHEN delivery.error_snapshot_json IS NULL THEN (\
                            SELECT review.reviewer_ref_json \
                            FROM task_result_candidate candidate \
                            JOIN task_result_review_event review \
                              ON review.id = candidate.final_review_event_id \
                            WHERE candidate.run_id = delivery.run_id \
                              AND candidate.status = 'accepted' \
                            ORDER BY candidate.created_at DESC, candidate.id DESC \
                            LIMIT 1\
                        ) ELSE NULL END AS reviewer_json, \
                        CAST(strftime('%s', delivery.updated_at) AS INTEGER) AS updated_at \
                 FROM task_delivery delivery \
                 WHERE NOT EXISTS (\
                     SELECT 1 FROM task_delivery_authority authority \
                     WHERE authority.delivery_id = delivery.id\
                 ){task_filter} \
                 ORDER BY delivery.created_at, delivery.id"
            ),
            [task_id.into()],
        )
    } else {
        Statement::from_string(
            DatabaseBackend::Sqlite,
            format!(
                "SELECT delivery.id AS delivery_id, delivery.task_id, delivery.run_id, \
                        delivery.delivery_key AS idempotency_key, delivery.status, \
                        CASE WHEN delivery.error_snapshot_json IS NULL THEN (\
                            SELECT review.reviewer_ref_json \
                            FROM task_result_candidate candidate \
                            JOIN task_result_review_event review \
                              ON review.id = candidate.final_review_event_id \
                            WHERE candidate.run_id = delivery.run_id \
                              AND candidate.status = 'accepted' \
                            ORDER BY candidate.created_at DESC, candidate.id DESC \
                            LIMIT 1\
                        ) ELSE NULL END AS reviewer_json, \
                        CAST(strftime('%s', delivery.updated_at) AS INTEGER) AS updated_at \
                 FROM task_delivery delivery \
                 WHERE NOT EXISTS (\
                     SELECT 1 FROM task_delivery_authority authority \
                     WHERE authority.delivery_id = delivery.id\
                 ){task_filter} \
                 ORDER BY delivery.created_at, delivery.id"
            ),
        )
    };
    database
        .query_all_raw(statement)
        .await
        .context("failed to list Task deliveries requiring Agent-domain conversion")?
        .into_iter()
        .map(|row| {
            Ok(DeliveryConversionRow {
                delivery_id: row.try_get("", "delivery_id")?,
                task_id: row.try_get("", "task_id")?,
                run_id: row.try_get("", "run_id")?,
                idempotency_key: row.try_get("", "idempotency_key")?,
                status: row.try_get("", "status")?,
                reviewer_json: row.try_get("", "reviewer_json")?,
                updated_at: row.try_get("", "updated_at")?,
            })
        })
        .collect()
}

async fn convert_delivery_rows<'a>(
    processor: &MessageProcessor,
    rows: Vec<DeliveryConversionRow>,
    mut contract_for: impl FnMut(&str) -> Option<&'a pioneer_protocol::TaskActorContract>,
) -> Result<usize> {
    // Before the Agent-domain schema, Task delivery Turns were created by the
    // internal delivery projector and were therefore System-authored. Preserve
    // that exact fact for existing rows; only new Agent-domain deliveries use
    // their occurrence AgentExecution as the author.
    let author_json = serde_json::to_string(&pioneer_protocol::PersistedActorRef::System)?;
    let converted = rows.len();
    for row in rows {
        let contract = contract_for(row.task_id.as_str()).with_context(|| {
            format!(
                "Task delivery `{}` has no converted actor contract",
                row.delivery_id
            )
        })?;
        contract.delivery.validate().map_err(|error| {
            anyhow!(
                "Task delivery `{}` actor contract is invalid: {error:?}",
                row.delivery_id
            )
        })?;
        if !contract.delivery.enabled {
            bail!(
                "Task delivery `{}` exists for a Task with delivery disabled",
                row.delivery_id
            );
        }
        processor
            .crud_store
            .upsert_task_delivery_authority(
                row.delivery_id.as_str(),
                row.task_id.as_str(),
                row.run_id.as_str(),
                author_json.as_str(),
                row.reviewer_json.as_deref(),
                contract.delivery.route_id.as_deref(),
                contract.delivery.route_receipt_json.as_deref(),
                contract.delivery.disclosure_generation,
                row.idempotency_key.as_str(),
                row.status.as_str(),
                row.updated_at,
            )
            .await?;
    }
    Ok(converted)
}

async fn tasks_requiring_conversion(processor: &MessageProcessor) -> Result<Vec<String>> {
    let database = processor.crud_store.database_connection();
    let rows = database
        .query_all_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT task.id AS task_id FROM task \
             WHERE NOT EXISTS (\
                 SELECT 1 FROM task_actor_contract contract \
                 WHERE contract.task_id = task.id\
             ) OR EXISTS (\
                 SELECT 1 FROM task_run run \
                 WHERE run.task_id = task.id \
                   AND NOT EXISTS (\
                       SELECT 1 FROM task_run retry \
                       WHERE retry.retry_of_run_id = run.id\
                   ) \
                   AND NOT EXISTS (\
                       SELECT 1 FROM task_occurrence_contract occurrence \
                       WHERE occurrence.run_id = run.id\
                   )\
             ) \
             ORDER BY task.id"
                .to_owned(),
        ))
        .await
        .context("failed to list Tasks requiring Agent-domain conversion")?;
    rows.into_iter()
        .map(|row| row.try_get("", "task_id").map_err(Into::into))
        .collect()
}

async fn build_actor_contract(
    processor: &MessageProcessor,
    response: &pioneer_protocol::TaskGetResponse,
) -> Result<pioneer_protocol::TaskActorContract> {
    let task = &response.task;
    let task_agent_spec = response
        .agent_specs
        .iter()
        .find(|spec| spec.run_id.is_none())
        .or_else(|| response.agent_specs.first());
    let persisted_admission = if task.executor_kind == TaskExecutorKind::Agent {
        processor
            .crud_store
            .get_task_execution_admission(task.id.as_str())
            .await?
    } else {
        None
    };
    let exact_creator_id = exact_task_creator(processor, task).await?;
    let creator_id = match persisted_admission.as_ref() {
        Some(admission) => {
            if let Some(exact_creator_id) = exact_creator_id.as_deref()
                && exact_creator_id != admission.initiating_principal_id
            {
                bail!(
                    "Agent Task `{}` creator `{exact_creator_id}` differs from its persisted admission principal `{}`",
                    task.id,
                    admission.initiating_principal_id
                );
            }
            Some(admission.initiating_principal_id.clone())
        }
        None => exact_creator_id,
    };
    let mut context = pioneer_tasks::TaskCreateContext {
        actor_id: creator_id.clone(),
        ..Default::default()
    };

    if task.executor_kind == TaskExecutorKind::Agent {
        let agent_spec = task_agent_spec.with_context(|| {
            format!(
                "Agent Task `{}` has no durable agent specification",
                task.id
            )
        })?;
        let (admitted, execution_admission) = match persisted_admission {
            Some(admission) => {
                let admitted =
                    crate::authorization::ExecutionAuthorizationContext::load_for_task_admission(
                        processor.crud_store.as_ref(),
                        &admission,
                    )
                    .await?;
                let (execution_resources, task_resources) = admitted.admitted_resource_budgets()?;
                let seed = pioneer_tasks::TaskExecutionAdmissionSeed {
                    workspace_id: admission.workspace_id,
                    root_thread_id: admission.root_thread_id,
                    initiating_principal_id: admission.initiating_principal_id,
                    authorization_context_json: admission.authorization_context_json,
                    role_key: admitted.role_key().to_owned(),
                    policy_fingerprint: admitted.policy_fingerprint().to_owned(),
                    execution_resources,
                    task_resources,
                };
                (admitted, Some(seed))
            }
            None => {
                let creator_id = creator_id.as_deref().with_context(|| {
                    format!(
                        "historical Agent Task `{}` has no exact principal creator",
                        task.id
                    )
                })?;
                let principal = data_upgrade_principal(processor, creator_id).await?;
                let seed = processor
                    .task_execution_admission_seed_for_existing_task(
                        &principal,
                        task.created_by_thread_id.as_deref(),
                        response,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "failed to authorize historical Agent Task `{}` during conversion",
                            task.id
                        )
                    })?;
                let admitted =
                    crate::authorization::ExecutionAuthorizationContext::from_persisted_json(
                        seed.authorization_context_json.as_str(),
                    )?;
                // The temporary admission is used only to evaluate the current,
                // final authorization policy. Historical terminal Tasks did not
                // persist an execution admission, so no invented session
                // provenance is written to the database.
                (admitted, None)
            }
        };
        let root_thread = processor
            .crud_store
            .get_thread_by_id(admitted.root_thread_id())
            .await?
            .with_context(|| {
                format!(
                    "Agent Task `{}` execution root `{}` is missing",
                    task.id,
                    admitted.root_thread_id()
                )
            })?;
        let source_provider = agent_spec
            .model_provider
            .as_deref()
            .unwrap_or(root_thread.model_provider.as_str());
        let source_model = agent_spec
            .model
            .as_deref()
            .unwrap_or(root_thread.model.as_str());
        let requested_backend = task
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.composer_work.as_ref())
            .and_then(|work| work.launch.execution_backend.as_ref());
        let (canonical_launch, resolved_launch) =
            crate::message::agent_action_tools::resolve_workspace_task_launch(
                processor,
                task.workspace_id.as_str(),
                source_provider,
                source_model,
                None,
                requested_backend,
                task.id.as_str(),
            )
            .await?;
        let (identity, profile) = resolved_launch
            .context("Agent Task conversion did not resolve an identity and execution profile")?;
        let skill_ids = admitted
            .granted_skill_ids()
            .iter()
            .map(|id| SkillId::new(id.clone()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| anyhow!("Agent Task admission contains an invalid Skill id"))?;
        let child_launch_grant =
            crate::message::agent_action_tools::current_workspace_child_launch_ceiling(
                processor,
                task.workspace_id.as_str(),
                &identity,
                &profile,
                source_provider,
                source_model,
                true,
                skill_ids,
                admitted.granted_mcp_server_capability_ids().to_vec(),
                admitted.permission_profile_cap().clone(),
            )
            .await?;
        let authorization_grant = crate::authorization::derive_task_agent_authorization_grant_seed(
            identity.id.clone(),
            admitted.root_thread_id(),
            "thread_agent",
            processor.current_authorization_revision().await?.max(1),
            child_launch_grant,
        )
        .map_err(|error| anyhow!("failed to freeze Agent Task authorization: {error:?}"))?;

        context.launch_selection = Some(canonical_launch);
        context.resolved_launch_identity = Some(identity);
        context.resolved_launch_profile = Some(profile);
        context.agent_authorization_grant = Some(authorization_grant);
        context.execution_admission = execution_admission;
    }

    pioneer_tasks::build_task_actor_contract(task, task_agent_spec, &context, task.created_at)
        .with_context(|| format!("failed to convert Task `{}` actor contract", task.id))
}

async fn data_upgrade_principal(
    processor: &MessageProcessor,
    principal_id: &str,
) -> Result<crate::auth::AuthenticatedSessionPrincipal> {
    let row = processor
        .crud_store
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT principal.gateway_id, principal.kind AS principal_kind, principal.role_key, \
                    session.device_id, session.id AS session_id \
             FROM gateway_principal principal \
             JOIN auth_session session ON session.principal_id = principal.id \
             JOIN device ON device.id = session.device_id \
             WHERE principal.id = ? \
               AND principal.status = 'active' \
               AND session.status = 'active' \
               AND device.status = 'active' \
             ORDER BY session.last_seen_at DESC, session.id DESC \
             LIMIT 1",
            [principal_id.into()],
        ))
        .await?
        .with_context(|| {
            format!(
                "historical Agent Task principal `{principal_id}` has no active local session for one-time authorization conversion"
            )
        })?;
    let gateway_id: String = row.try_get("", "gateway_id")?;
    let principal_kind: String = row.try_get("", "principal_kind")?;
    let role_key: Option<String> = row.try_get("", "role_key")?;
    let device_id: String = row.try_get("", "device_id")?;
    let session_id: String = row.try_get("", "session_id")?;
    Ok(crate::auth::AuthenticatedSessionPrincipal {
        gateway_id: GatewayId::new(gateway_id)?,
        principal_id: PrincipalId::new(principal_id.to_owned())?,
        kind: pioneer_crud::principal_kind_from_db(principal_kind.as_str())?,
        role_key: role_key.map(RoleKey::new).transpose()?,
        device_id: DeviceId::new(device_id)?,
        session_id: AuthSessionId::new(session_id)?,
        access_jti: "agent-domain-data-upgrade".to_owned(),
        access_expires_at_unix: u64::MAX,
    })
}

async fn exact_task_creator(processor: &MessageProcessor, task: &Task) -> Result<Option<String>> {
    if let (Some(thread_id), Some(turn_id)) = (
        task.created_by_thread_id.as_deref(),
        task.created_by_turn_id.as_deref(),
    ) && let Some((_, turn)) = processor.crud_store.get_turn(thread_id, turn_id).await?
        && let Some(author) = turn.author
    {
        return match author.actor {
            pioneer_protocol::PersistedActorRef::Principal(principal_id) => {
                Ok(Some(principal_id.to_string()))
            }
            pioneer_protocol::PersistedActorRef::System => Ok(None),
            pioneer_protocol::PersistedActorRef::AgentExecution(_) => bail!(
                "Task `{}` has Agent authorship but no immutable actor contract",
                task.id
            ),
        };
    }
    if task.owner_kind == TaskOwnerKind::User {
        return Ok(task.owner_id.clone());
    }
    Ok(None)
}

async fn convert_occurrences(
    processor: &MessageProcessor,
    task: &Task,
    runs: &[TaskRun],
    actor_contract: &pioneer_protocol::TaskActorContract,
) -> Result<usize> {
    let by_id = runs
        .iter()
        .map(|run| (run.id.as_str(), run))
        .collect::<HashMap<_, _>>();
    let retried = runs
        .iter()
        .filter_map(|run| run.retry_of_run_id.as_deref())
        .collect::<HashSet<_>>();
    let mut heads = runs
        .iter()
        .filter(|run| !retried.contains(run.id.as_str()))
        .collect::<Vec<_>>();
    heads.sort_by_key(|run| (run.run_number, run.created_at, run.id.as_str()));

    let mut converted = 0_usize;
    for (index, run) in heads.into_iter().enumerate() {
        let root = retry_root(run, &by_id)?;
        let execution_generation = u64::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(1))
            .context("Task occurrence generation overflow")?;
        let occurrence = TaskOccurrenceContract {
            occurrence_id: root.id.clone(),
            task_id: task.id.clone(),
            run_id: run.id.clone(),
            trigger_id: root.trigger_id.clone(),
            occurrence_key: root
                .trigger_id
                .as_deref()
                .map(|trigger_id| format!("{trigger_id}:{}", root.run_number))
                .unwrap_or_else(|| format!("immediate:{}", root.id)),
            execution_generation,
            agent_execution_id: None,
            work_graph_root_execution_id: None,
            root_resource_scope_id: None,
            status: occurrence_status(run.status),
            queue_position: None,
            retry_attempt: run.attempt_number,
            action_idempotency_key: format!("task:{}:{}", task.id, root.id),
            route_id: actor_contract.execution_route_id.clone(),
            result_return_route_id: actor_contract.delivery.route_id.clone(),
            terminal_reason: None,
        };
        occurrence
            .validate()
            .map_err(|error| anyhow!("Task occurrence conversion is invalid: {error:?}"))?;
        let existing = processor
            .crud_store
            .get_task_occurrence_contract(root.id.as_str())
            .await?;
        if existing.as_ref() != Some(&occurrence) {
            processor
                .crud_store
                .upsert_task_occurrence_contract(&occurrence, run.updated_at)
                .await?;
            converted = converted.saturating_add(1);
        }
    }
    Ok(converted)
}

fn retry_root<'a>(run: &'a TaskRun, by_id: &HashMap<&str, &'a TaskRun>) -> Result<&'a TaskRun> {
    let mut current = run;
    let mut seen = HashSet::new();
    while let Some(previous_id) = current.retry_of_run_id.as_deref() {
        if !seen.insert(current.id.as_str()) {
            bail!("Task run retry chain contains a cycle at `{}`", current.id);
        }
        current = by_id.get(previous_id).copied().with_context(|| {
            format!(
                "Task run `{}` references missing retry source `{previous_id}`",
                current.id
            )
        })?;
    }
    Ok(current)
}

const fn occurrence_status(status: TaskRunStatus) -> TaskOccurrenceStatus {
    match status {
        TaskRunStatus::Queued | TaskRunStatus::Starting => TaskOccurrenceStatus::Queued,
        TaskRunStatus::Running | TaskRunStatus::Waiting => TaskOccurrenceStatus::Recovering,
        TaskRunStatus::WaitingReview => TaskOccurrenceStatus::WaitingReview,
        TaskRunStatus::Succeeded => TaskOccurrenceStatus::Delivered,
        TaskRunStatus::Failed | TaskRunStatus::Blocked | TaskRunStatus::TimedOut => {
            TaskOccurrenceStatus::Failed
        }
        TaskRunStatus::Cancelled => TaskOccurrenceStatus::Cancelled,
    }
}

async fn verify_conversion(processor: &MessageProcessor) -> Result<()> {
    let database = processor.crud_store.database_connection();
    for (label, sql) in [
        (
            "Tasks without actor contracts",
            "SELECT COUNT(*) AS row_count FROM task task_row \
             WHERE NOT EXISTS (SELECT 1 FROM task_actor_contract contract WHERE contract.task_id = task_row.id)",
        ),
        (
            "Agent Tasks without exact frozen launches",
            "SELECT COUNT(*) AS row_count FROM task task_row \
             JOIN task_actor_contract contract ON contract.task_id = task_row.id \
             WHERE task_row.executor_kind = 'agent' AND (\
                 contract.launch_selection_json IS NULL OR \
                 contract.requested_identity_json IS NULL OR \
                 contract.resolved_identity_id IS NULL OR \
                 contract.resolved_profile_id IS NULL OR \
                 contract.source_config_fingerprint IS NULL OR \
                 contract.derived_child_launch_grant_json IS NULL\
             )",
        ),
        (
            "Task retry heads without occurrence contracts",
            "SELECT COUNT(*) AS row_count FROM task_run run \
             WHERE NOT EXISTS (SELECT 1 FROM task_run retry WHERE retry.retry_of_run_id = run.id) \
               AND NOT EXISTS (SELECT 1 FROM task_occurrence_contract occurrence WHERE occurrence.run_id = run.id)",
        ),
        (
            "Task turns without execution ownership",
            "SELECT COUNT(*) AS row_count FROM task_run_turn task_turn \
             JOIN turn materialized_turn \
               ON materialized_turn.id = task_turn.turn_id \
              AND materialized_turn.thread_id = task_turn.thread_id \
             WHERE NOT EXISTS (SELECT 1 FROM turn_execution execution WHERE execution.turn_id = task_turn.turn_id)",
        ),
        (
            "review events without exact reviewers",
            "SELECT COUNT(*) AS row_count FROM task_result_review_event WHERE reviewer_ref_json IS NULL",
        ),
        (
            "Task deliveries without final authority",
            "SELECT COUNT(*) AS row_count FROM task_delivery delivery \
             WHERE NOT EXISTS (\
                 SELECT 1 FROM task_delivery_authority authority \
                 WHERE authority.delivery_id = delivery.id\
             )",
        ),
        (
            "successful Task deliveries without exact reviewers",
            "SELECT COUNT(*) AS row_count FROM task_delivery delivery \
             JOIN task_delivery_authority authority ON authority.delivery_id = delivery.id \
             WHERE delivery.error_snapshot_json IS NULL AND authority.reviewer_json IS NULL",
        ),
    ] {
        let count = database
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                sql.to_owned(),
            ))
            .await?
            .map(|row| row.try_get::<i64>("", "row_count"))
            .transpose()?
            .unwrap_or_default();
        if count != 0 {
            bail!("Agent domain upgrade left {count} {label}");
        }
    }
    Ok(())
}
