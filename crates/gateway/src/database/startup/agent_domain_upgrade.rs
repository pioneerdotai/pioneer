use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, anyhow, bail};
use pioneer_protocol::{
    SkillId, Task, TaskExecutorKind, TaskOccurrenceContract, TaskOccurrenceStatus, TaskOwnerKind,
    TaskRun, TaskRunStatus,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use tracing::info;

use crate::message::MessageProcessor;

/// Completes the one-time Agent-domain data conversion before the Gateway
/// listener starts. The final runtime assumes these rows exist and contains
/// no alternate path for an older Task representation.
pub(crate) async fn apply(processor: &MessageProcessor) -> Result<()> {
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
    let mut context = pioneer_tasks::TaskCreateContext {
        actor_id: exact_task_creator(processor, task).await?,
        ..Default::default()
    };

    if task.executor_kind == TaskExecutorKind::Agent {
        let agent_spec = task_agent_spec.with_context(|| {
            format!(
                "Agent Task `{}` has no durable agent specification",
                task.id
            )
        })?;
        let admission = processor
            .crud_store
            .get_task_execution_admission(task.id.as_str())
            .await?
            .with_context(|| {
                format!(
                    "Agent Task `{}` has no execution admission to convert",
                    task.id
                )
            })?;
        let admitted =
            crate::authorization::ExecutionAuthorizationContext::load_for_task_admission(
                processor.crud_store.as_ref(),
                &admission,
            )
            .await?;
        let root_thread = processor
            .crud_store
            .get_thread_by_id(admission.root_thread_id.as_str())
            .await?
            .with_context(|| {
                format!(
                    "Agent Task `{}` execution root `{}` is missing",
                    task.id, admission.root_thread_id
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
        let (execution_resources, task_resources) = admitted.admitted_resource_budgets()?;

        context.actor_id = Some(admission.initiating_principal_id.clone());
        context.launch_selection = Some(canonical_launch);
        context.resolved_launch_identity = Some(identity);
        context.resolved_launch_profile = Some(profile);
        context.agent_authorization_grant = Some(authorization_grant);
        context.execution_admission = Some(pioneer_tasks::TaskExecutionAdmissionSeed {
            workspace_id: admission.workspace_id,
            root_thread_id: admission.root_thread_id,
            initiating_principal_id: admission.initiating_principal_id,
            authorization_context_json: admission.authorization_context_json,
            role_key: admitted.role_key().to_owned(),
            policy_fingerprint: admitted.policy_fingerprint().to_owned(),
            execution_resources,
            task_resources,
        });
    }

    pioneer_tasks::build_task_actor_contract(task, task_agent_spec, &context, task.created_at)
        .with_context(|| format!("failed to convert Task `{}` actor contract", task.id))
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
