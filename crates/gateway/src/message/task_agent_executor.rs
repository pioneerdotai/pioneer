use super::agent_runtime::TurnFailureRecoveryKind;
use super::*;
use crate::authorization::AgentExecutionPersistenceFacts;
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use pioneer_agent::{AgentTurnHookRuntimeContext, ExecutionCheckpointContext};
use pioneer_crud::{
    AgentExecutionGrantInput, AgentExecutionGraphCommitInput, AgentExecutionInput,
    AgentIdentityInput, AgentResourceStateInput, PresentationSnapshotInput, canonical_agent_id,
    utc_now,
};
use pioneer_promt::{TaskRevisionPromptInput, TaskRunPromptCompiler, TaskRunPromptInput};
use pioneer_protocol::{
    AgentExecutionBackend, AgentExecutionId, AgentExecutionProfileBackend, AgentIdentitySourceKind,
    CLIAgentRuntimeKind, ExecutionCheckpointPayload, ItemCompletedNotification,
    ItemStartedNotification, PermissionBehavior, TASK_COMPOSER_WORK_VERSION, Task,
    TaskAgentContext, TaskAgentContextMode, TaskAgentInput, TaskAgentResultContract,
    TaskAgentResultFormat, TaskAgentReviewPolicy, TaskAgentSpec, TaskAgentToolPolicy,
    TaskAgentWriteMode, TaskAttachmentMode, TaskError, TaskErrorClass, TaskExecutorKind,
    TaskGetResponse, TaskResult, TaskResultCandidate, TaskResultCandidateStatus,
    TaskResultReviewDecision, TaskResultReviewEvent, TaskResultReviewEventKind,
    TaskResultReviewerKind, TaskResultReviewerSpec, TaskReviseResponse, TaskRun, TaskRunExecution,
    TaskRunExecutionStatus, TaskRunStatus, TaskRunThreadBinding, TaskRunThreadBindingKind,
    TaskRunTurn, TaskRunTurnKind, TaskRunTurnStatus, TaskThreadLineage, TaskTrigger,
    TaskTriggerKind, TaskValue, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
    ToolPermissionPolicySnapshot, Turn, TurnBlockedNotification, TurnCompletedNotification,
    TurnExecutionSecuritySnapshot, TurnFailedNotification, TurnKind, TurnOrigin,
    TurnPermissionProfileSnapshot, TurnPermissionProfileSource, TurnStartParams,
    TurnStartedNotification, TurnStatus, UserInput,
};
use pioneer_tasks::{
    CreateTaskResultReviewerContextParams, RecordTaskResultReviewEventParams,
    TASK_EXECUTION_LEASE_SECONDS, TaskExecutionContext, TaskExecutionHandle, TaskExecutor,
    TaskExecutorRecoveryOutcome, TaskExecutorStartOutcome, TaskResultReviewActor,
    WriteLockDecision, stable_review_thread_id, stable_review_turn_id,
    task_result_reviewer_spec_key,
};
use std::collections::BTreeMap;
use std::sync::{RwLock as StdRwLock, Weak};
use tokio::time::{Duration, sleep};

const TASK_EXECUTION_HEARTBEAT_SECONDS: u64 = 30;

fn task_agent_liveness_timeouts(task: &Task) -> (i64, i64) {
    let defaults = pioneer_config::GatewayProviderStreamItemTimeoutConfig::default();
    let configured = task.timeout_policy.as_ref();
    let idle = configured
        .and_then(|policy| policy.heartbeat_timeout_seconds)
        .filter(|seconds| *seconds > 0)
        .unwrap_or(defaults.idle_secs as i64);
    let hard = configured
        .and_then(|policy| policy.run_timeout_seconds)
        .filter(|seconds| *seconds > 0)
        .unwrap_or(defaults.hard_secs as i64);
    (idle, hard.max(idle))
}

fn task_occurrence_turn_author(
    actor_contract: &pioneer_protocol::TaskActorContract,
    execution_id: &str,
) -> Result<pioneer_protocol::TurnAuthorSnapshot> {
    actor_contract
        .validate()
        .map_err(|error| anyhow!("Task actor contract is invalid: {error:?}"))?;
    let grant_json = actor_contract
        .derived_child_launch_grant_json
        .as_deref()
        .context("Agent Task has no immutable resolved launch grant")?;
    let pioneer_protocol::TaskDerivedChildLaunchGrant::ResolvedTaskLaunch { identity, .. } =
        serde_json::from_str(grant_json).context("Task resolved launch grant is invalid")?;
    if actor_contract.resolved_identity_id.as_deref() != Some(identity.id.as_str())
        || actor_contract.source_config_fingerprint.as_deref()
            != Some(identity.source_fingerprint.as_str())
    {
        bail!("Task occurrence identity differs from its immutable actor contract");
    }
    let agent_execution_id = AgentExecutionId::new(execution_id.to_owned())
        .map_err(|error| anyhow!("invalid task execution id `{execution_id}`: {error:?}"))?;
    Ok(pioneer_protocol::AgentPresentationSnapshot {
        agent_identity_id: identity.id,
        agent_execution_id,
        identity_source_kind: identity.source_kind,
        identity_source_revision: identity.source_revision,
        display_name: identity.display_name,
        nickname: identity.nickname,
        avatar_revision: identity.avatar_revision,
        role_label: identity.role_label,
    }
    .to_turn_author_snapshot())
}

fn task_occurrence_execution_lineage(
    execution_id: &str,
    occurrence_root_execution_id: Option<&str>,
    occurrence_execution_id: Option<&str>,
    task_root_execution_id: Option<&str>,
    task_creator_execution_id: Option<&str>,
) -> Result<(String, Option<String>)> {
    let root_execution_id = occurrence_root_execution_id
        .or(task_root_execution_id)
        .unwrap_or(execution_id)
        .to_owned();
    if root_execution_id == execution_id {
        return Ok((root_execution_id, None));
    }
    let parent_execution_id = occurrence_execution_id
        .filter(|candidate| *candidate != execution_id)
        .or(task_creator_execution_id)
        .filter(|candidate| *candidate != execution_id)
        .map(str::to_owned)
        .context("inherited Task work graph has no exact parent execution actor")?;
    Ok((root_execution_id, Some(parent_execution_id)))
}

async fn exact_agent_execution_author(
    processor: &Arc<MessageProcessor>,
    execution_id: &str,
) -> Result<pioneer_protocol::TurnAuthorSnapshot> {
    let database = processor.crud_store.database_connection();
    let execution = pioneer_crud::load_agent_execution(&database, execution_id)
        .await?
        .with_context(|| format!("AgentExecution `{execution_id}` is missing"))?;
    let snapshot_id = execution
        .presentation_snapshot_id
        .as_deref()
        .context("AgentExecution has no immutable presentation snapshot")?;
    let snapshot = pioneer_crud::load_agent_presentation_snapshot(&database, snapshot_id)
        .await?
        .context("AgentExecution presentation snapshot is missing")?;
    let identity =
        pioneer_crud::load_agent_identity(&database, execution.agent_identity_id.as_str())
            .await?
            .context("AgentExecution identity is missing")?;
    let presentation =
        pioneer_crud::agent_presentation_snapshot_from_rows(&identity, &execution, &snapshot)?;
    Ok(presentation.to_turn_author_snapshot())
}

async fn agent_turn_response_input(
    processor: &Arc<MessageProcessor>,
    turn_id: &str,
    execution_id: &str,
) -> Result<pioneer_crud::AgentTurnResponseInput> {
    let database = processor.crud_store.database_connection();
    let execution = pioneer_crud::load_agent_execution(&database, execution_id)
        .await?
        .with_context(|| format!("responding AgentExecution `{execution_id}` is missing"))?;
    let presentation_snapshot_id = execution
        .presentation_snapshot_id
        .context("responding AgentExecution has no presentation snapshot")?;
    Ok(pioneer_crud::AgentTurnResponseInput {
        turn_id: turn_id.to_owned(),
        execution_id: execution_id.to_owned(),
        presentation_snapshot_id,
        now: utc_now().into(),
    })
}

async fn task_actor_turn_author(
    processor: &Arc<MessageProcessor>,
    actor_contract: &pioneer_protocol::TaskActorContract,
) -> Result<pioneer_protocol::TurnAuthorSnapshot> {
    match &actor_contract.creator {
        pioneer_protocol::PersistedActorRef::AgentExecution(execution_id) => {
            let snapshot = actor_contract
                .creator_presentation_snapshot
                .as_ref()
                .context("agent Task creator has no immutable presentation snapshot")?;
            if &snapshot.agent_execution_id != execution_id {
                bail!("Task creator snapshot differs from its exact AgentExecution");
            }
            Ok(snapshot.to_turn_author_snapshot())
        }
        pioneer_protocol::PersistedActorRef::Principal(_) => {
            super::message_turn::resolve_turn_author_snapshot(
                processor.crud_store.as_ref(),
                &actor_contract.creator,
            )
            .await?
            .context("Task creator principal has no presentation snapshot")
        }
        _ => bail!("Agent Task creator must be a Principal or exact AgentExecution"),
    }
}

async fn revision_turn_author(
    processor: &Arc<MessageProcessor>,
    task_run_turn: &TaskRunTurn,
) -> Result<pioneer_protocol::TurnAuthorSnapshot> {
    let review_event_id = task_run_turn
        .requested_by_review_event_id
        .as_deref()
        .context("revision Turn has no requesting review event")?;
    let review_event = processor
        .crud_store
        .get_task_result_review_event(review_event_id)
        .await?
        .with_context(|| format!("revision review event `{review_event_id}` is missing"))?;
    match review_event.reviewer {
        pioneer_protocol::TaskResultReviewerRef::AgentExecution(execution_id) => {
            exact_agent_execution_author(processor, execution_id.as_str()).await
        }
        pioneer_protocol::TaskResultReviewerRef::Principal(principal_id) => {
            super::message_turn::resolve_turn_author_snapshot(
                processor.crud_store.as_ref(),
                &pioneer_protocol::PersistedActorRef::Principal(principal_id),
            )
            .await?
            .context("revision reviewer principal has no presentation snapshot")
        }
        pioneer_protocol::TaskResultReviewerRef::RuntimePolicy => {
            Ok(super::message_turn::system_turn_author_snapshot())
        }
    }
}

fn task_child_launch_grant(
    actor_contract: &pioneer_protocol::TaskActorContract,
) -> Result<pioneer_protocol::ChildAgentLaunchGrantSet> {
    let grant_json = actor_contract
        .derived_child_launch_grant_json
        .as_deref()
        .context("Agent Task has no immutable child launch ceiling")?;
    let pioneer_protocol::TaskDerivedChildLaunchGrant::ResolvedTaskLaunch {
        child_launch_grant,
        ..
    } = serde_json::from_str(grant_json).context("Task resolved child launch grant is invalid")?;
    child_launch_grant
        .validate()
        .map_err(|error| anyhow!("Task child launch ceiling failed validation: {error:?}"))?;
    Ok(child_launch_grant)
}

async fn exact_task_agent_source_id(
    processor: &MessageProcessor,
    workspace_id: &str,
    task_id: &str,
    facts: &AgentExecutionPersistenceFacts,
) -> Result<String> {
    let expected_source_kind = match facts.identity.source_kind {
        AgentIdentitySourceKind::NativeAgent => pioneer_crud::SOURCE_NATIVE_AGENT,
        AgentIdentitySourceKind::CliRuntimeInstance => pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE,
        AgentIdentitySourceKind::Ephemeral => {
            return Ok(format!("task-agent:{task_id}:{}", facts.identity.id));
        }
    };
    let identity = pioneer_crud::load_agent_identity(
        &processor.crud_store.database_connection(),
        facts.identity.id.as_str(),
    )
    .await?
    .context("Task Agent identity source is missing")?;
    if identity.workspace_id != workspace_id
        || identity.source_kind != expected_source_kind
        || identity.status != "active"
        || identity.retired_at.is_some()
        || identity.source_revision != i64::try_from(facts.identity_source_revision).unwrap_or(-1)
        || identity.source_fingerprint != facts.identity_source_fingerprint
    {
        bail!("Task Agent identity source differs from its immutable execution facts");
    }
    if let AgentExecutionProfileBackend::CliRuntime {
        runtime_instance_id,
    } = &facts.profile.backend
        && runtime_instance_id != &identity.source_id
    {
        bail!("Task CLI identity and execution profile use different runtime instances");
    }
    Ok(identity.source_id)
}

async fn exact_task_agent_presentation_snapshot_id(
    processor: &MessageProcessor,
    facts: &AgentExecutionPersistenceFacts,
    ephemeral_snapshot_seed: &str,
) -> Result<String> {
    if facts.identity.source_kind == AgentIdentitySourceKind::Ephemeral {
        return Ok(canonical_agent_id('S', ephemeral_snapshot_seed));
    }

    let source_revision = i64::try_from(facts.identity_source_revision)
        .context("Task Agent identity source revision exceeds database range")?;
    let snapshot = pioneer_crud::load_current_agent_presentation_snapshot(
        &processor.crud_store.database_connection(),
        facts.identity.id.as_str(),
        source_revision,
        facts.identity_source_fingerprint.as_str(),
    )
    .await?
    .context("Task Agent identity has no authoritative presentation snapshot")?;
    if snapshot.display_name != facts.identity.display_name
        || snapshot.nickname != facts.identity.nickname
        || snapshot.avatar_revision != facts.identity.avatar_revision
        || snapshot.role_label != facts.identity.role_label
    {
        bail!("Task Agent presentation differs from its immutable identity snapshot");
    }
    Ok(snapshot.id)
}

/// Persist the server-derived agent domain graph before a Task runtime starts.
/// The task execution row remains the visible actor; the deterministic root
/// scope is only the graph/resource owner and never a human/session identity.
async fn persist_task_agent_execution_graph(
    processor: &Arc<MessageProcessor>,
    task_response: &TaskGetResponse,
    run: &TaskRun,
    agent_spec: &TaskAgentSpec,
    parent: &TaskParentRuntimeContext,
    facts: &AgentExecutionPersistenceFacts,
    authorization_context_fingerprint: &str,
) -> Result<pioneer_crud::AgentExecutionGraphCommitResult> {
    let task = &task_response.task;
    let now = utc_now();
    let (idle_timeout_secs, hard_timeout_secs) = task_agent_liveness_timeouts(task);
    let source_kind = match facts.identity.source_kind {
        AgentIdentitySourceKind::NativeAgent => pioneer_crud::SOURCE_NATIVE_AGENT,
        AgentIdentitySourceKind::CliRuntimeInstance => pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE,
        AgentIdentitySourceKind::Ephemeral => pioneer_crud::SOURCE_EPHEMERAL,
    };
    // Registered Native/CLI identities must reuse their exact durable source
    // key. Only an execution-local ephemeral identity receives a derived key.
    let source_id = exact_task_agent_source_id(
        processor,
        task.workspace_id.as_str(),
        task.id.as_str(),
        facts,
    )
    .await?;
    let source_revision = i64::try_from(facts.identity_source_revision)
        .context("task agent identity source revision exceeds database range")?;
    let execution_id = facts.execution_id.as_str().to_owned();
    let snapshot_id = exact_task_agent_presentation_snapshot_id(
        processor,
        facts,
        &format!(
            "task-agent-snapshot\0{}\0{}",
            facts.identity.id, execution_id
        ),
    )
    .await?;
    let actor_contract = processor
        .crud_store
        .get_task_actor_contract(task.id.as_str())
        .await?
        .context("agent Task is missing its durable actor contract")?;
    let child_launch_grant = task_child_launch_grant(&actor_contract)?;
    let launch = actor_contract
        .launch
        .as_ref()
        .context("Agent Task has no immutable launch selection")?;
    let requested_identity_json = serde_json::to_string(&launch.agent)
        .context("failed to serialize requested Task agent identity selection")?;
    let requested_profile_json = serde_json::to_string(&launch.execution)
        .context("failed to serialize requested Task execution selection")?;
    let occurrence_contract = processor
        .crud_store
        .get_task_occurrence_contract_by_run(run.id.as_str())
        .await?
        .context("agent Task run is missing its durable occurrence contract")?;
    if occurrence_contract.route_id != actor_contract.execution_route_id
        || occurrence_contract.result_return_route_id != actor_contract.delivery.route_id
    {
        bail!("Task occurrence route facts differ from its immutable actor contract");
    }
    let execution_generation = i64::try_from(occurrence_contract.execution_generation)
        .context("task occurrence execution generation exceeds database range")?;
    let attempt_generation = i64::from(occurrence_contract.retry_attempt).saturating_add(1);
    let creator_execution_id = match &actor_contract.creator {
        pioneer_protocol::PersistedActorRef::AgentExecution(execution_id) => {
            Some(execution_id.as_str().to_owned())
        }
        _ => None,
    };
    let (root_execution_id, parent_execution_id) = task_occurrence_execution_lineage(
        execution_id.as_str(),
        occurrence_contract.work_graph_root_execution_id.as_deref(),
        occurrence_contract.agent_execution_id.as_deref(),
        actor_contract.work_graph_root_execution_id.as_deref(),
        creator_execution_id.as_deref(),
    )?;
    if facts.root_execution_id.as_str() != root_execution_id.as_str() {
        bail!("Task action binding and occurrence resolve different work-graph roots");
    }
    let root_home_thread_id = facts.home_root_thread_id.clone();
    let branch_key = if let Some(parent_execution_id) = parent_execution_id.as_deref()
        && parent_execution_id != root_execution_id
    {
        let parent_resource = pioneer_crud::load_agent_execution_resource_state(
            &processor.crud_store.database_connection(),
            parent_execution_id,
        )
        .await?
        .context("nested Task parent has no durable branch resource state")?;
        parent_resource.branch_key
    } else {
        format!("task:{}:{}", task.id, run.id)
    };
    let grant_id = canonical_agent_id('G', &format!("grant\0{execution_id}"));
    let child_resource_state_id = canonical_agent_id(
        'R',
        &format!("resource\0{execution_id}\0{attempt_generation}"),
    );
    let grant_json = serde_json::json!({
        "kind": "task_child",
        "parent_execution_id": parent_execution_id,
        "execution_id": execution_id,
        "identity_id": facts.identity.id,
        "profile_id": facts.profile.id,
        "identity": facts.identity.clone(),
        "profile": facts.profile.clone(),
        "launch": actor_contract.launch.clone(),
        "execution_route_id": actor_contract.execution_route_id.clone(),
        "result_return_route_id": actor_contract.delivery.route_id.clone(),
        "depth": agent_spec.depth,
        "max_depth": agent_spec.max_depth,
        "root_thread_id": facts.home_root_thread_id,
        "role_key": facts.agent_authorization_role_key,
        "agent_policy_generation": facts.agent_authorization_policy_generation,
        "allowed_actions": facts.agent_authorization_allowed_actions,
        "agent_authorization_fingerprint": facts.agent_authorization_fingerprint,
        "child_launch_grant": child_launch_grant,
    })
    .to_string();
    let grant_fingerprint = pioneer_crud::agent_execution_grant_fingerprint(&grant_json)?;

    let result = match processor
        .crud_store
        .commit_agent_execution_graph(AgentExecutionGraphCommitInput {
            identity: AgentIdentityInput {
                id: facts.identity.id.as_str().to_owned(),
                workspace_id: task.workspace_id.clone(),
                source_kind: source_kind.to_owned(),
                source_id,
                source_revision,
                source_fingerprint: facts.identity_source_fingerprint.clone(),
                now: now.clone().into(),
            },
            presentation: PresentationSnapshotInput {
                id: snapshot_id.clone(),
                agent_identity_id: facts.identity.id.as_str().to_owned(),
                source_revision,
                source_fingerprint: facts.identity_source_fingerprint.clone(),
                display_name: facts.identity.display_name.clone(),
                nickname: facts.identity.nickname.clone(),
                avatar_revision: facts.identity.avatar_revision.clone(),
                role_label: facts.identity.role_label.clone(),
                now: now.clone().into(),
            },
            root_execution_id: root_execution_id.clone(),
            root_execution: (root_execution_id == execution_id).then(|| AgentExecutionInput {
                id: root_execution_id.clone(),
                workspace_id: task.workspace_id.clone(),
                agent_identity_id: facts.identity.id.as_str().to_owned(),
                identity_source_revision: source_revision,
                identity_source_fingerprint: facts.identity_source_fingerprint.clone(),
                parent_execution_id: None,
                parent_task_id: Some(task.id.clone()),
                parent_thread_id: Some(parent.parent_thread_id.clone()),
                home_root_thread_id: root_home_thread_id.clone(),
                work_graph_root_execution_id: root_execution_id.clone(),
                requested_identity_selection_json: requested_identity_json.clone(),
                requested_profile_selection_json: requested_profile_json.clone(),
                resolved_profile_id: Some(facts.profile.id.as_str().to_owned()),
                resolved_profile_fingerprint: Some(facts.profile.fingerprint.clone()),
                presentation_snapshot_id: Some(snapshot_id.clone()),
                authorization_context_fingerprint: authorization_context_fingerprint.to_owned(),
                execution_generation,
                status: "created".to_owned(),
                now: now.clone().into(),
            }),
            child_execution: AgentExecutionInput {
                id: execution_id.clone(),
                workspace_id: task.workspace_id.clone(),
                agent_identity_id: facts.identity.id.as_str().to_owned(),
                identity_source_revision: source_revision,
                identity_source_fingerprint: facts.identity_source_fingerprint.clone(),
                parent_execution_id: parent_execution_id.clone(),
                parent_task_id: Some(task.id.clone()),
                parent_thread_id: Some(parent.parent_thread_id.clone()),
                home_root_thread_id: root_home_thread_id,
                work_graph_root_execution_id: root_execution_id.clone(),
                requested_identity_selection_json: requested_identity_json,
                requested_profile_selection_json: requested_profile_json,
                resolved_profile_id: Some(facts.profile.id.as_str().to_owned()),
                resolved_profile_fingerprint: Some(facts.profile.fingerprint.clone()),
                presentation_snapshot_id: Some(snapshot_id.clone()),
                authorization_context_fingerprint: authorization_context_fingerprint.to_owned(),
                execution_generation,
                status: "created".to_owned(),
                now: now.clone().into(),
            },
            root_resource_state: None,
            child_resource_state: AgentResourceStateInput {
                id: child_resource_state_id,
                execution_id: execution_id.clone(),
                attempt_generation,
                branch_key,
                fair_order: 1,
                now: now.clone().into(),
            },
            grant: AgentExecutionGrantInput {
                id: grant_id,
                execution_id: execution_id.clone(),
                parent_execution_id: parent_execution_id.clone(),
                child_identity_id: facts.identity.id.as_str().to_owned(),
                grant_fingerprint,
                grant_json,
                now: now.clone().into(),
            },
            response: None,
            root_routes: Vec::new(),
            max_concurrency: crate::authorization::AgentWorkResourcePolicy::default()
                .max_concurrency as i32,
            max_queue_depth: crate::authorization::AgentWorkResourcePolicy::default()
                .max_queue_depth as i32,
            max_depth: i32::from(
                crate::authorization::AgentWorkResourcePolicy::default().max_depth,
            ),
            max_fan_out: crate::authorization::AgentWorkResourcePolicy::default().max_fan_out
                as i32,
            max_total_nodes: crate::authorization::AgentWorkResourcePolicy::default()
                .max_total_nodes as i32,
            idle_timeout_secs,
            hard_timeout_secs,
            child_permit_id: canonical_agent_id(
                'P',
                &format!("permit\0{root_execution_id}\0{execution_id}\0{attempt_generation}"),
            ),
            child_queue_id: canonical_agent_id(
                'Q',
                &format!("queue\0{root_execution_id}\0{execution_id}\0{attempt_generation}"),
            ),
            // Task-level creator, launch selection and creating graph facts
            // were frozen atomically with Task creation. Occurrence-specific
            // execution/grant facts belong to the execution graph rows above
            // and must never rewrite that immutable actor contract.
            task_actor_contract: None,
            task_occurrence_contract: Some(occurrence_contract),
            contract_now: now_timestamp_secs(),
        })
        .await
    {
        Ok(result) => result,
        Err(error) => {
            let root_was_cancelled = pioneer_crud::load_agent_execution(
                &processor.crud_store.database_connection(),
                root_execution_id.as_str(),
            )
            .await?
            .is_some_and(|execution| execution.status == "cancelled");
            if !root_was_cancelled {
                return Err(error).context("failed to persist agent domain task execution graph");
            }
            // Parent cancellation may fence the graph after Task creation but
            // before this child admission transaction. This is an intentional
            // terminal race, not an executor-start failure. Leave the run
            // non-failed so the parent cancellation path can project the exact
            // Task/Run cancellation through TaskService.
            pioneer_crud::AgentExecutionGraphCommitResult {
                root_execution_id: root_execution_id.clone(),
                execution_id: execution_id.clone(),
                queued: true,
                queue_position: None,
            }
        }
    };
    processor
        .notify_agent_work_graph_state_changed(result.root_execution_id.as_str())
        .await;
    Ok(result)
}

/// Reviewer turns are independent AgentExecutions in the existing Task work
/// graph. They must acquire their own permit and grant, but must not overwrite
/// the occurrence's candidate execution actor in `task_occurrence_contract`.
async fn persist_task_reviewer_execution_graph(
    processor: &Arc<MessageProcessor>,
    task_response: &TaskGetResponse,
    run: &TaskRun,
    reviewer_key: &str,
    parent: &TaskParentRuntimeContext,
    facts: &AgentExecutionPersistenceFacts,
    authorization_context_fingerprint: &str,
) -> Result<pioneer_crud::AgentExecutionGraphCommitResult> {
    let task = &task_response.task;
    let occurrence = processor
        .crud_store
        .get_task_occurrence_contract_by_run(run.id.as_str())
        .await?
        .context("reviewer run is missing its durable occurrence contract")?;
    let parent_execution_id = occurrence
        .agent_execution_id
        .clone()
        .context("reviewer run has no exact candidate execution")?;
    let root_execution_id = occurrence
        .work_graph_root_execution_id
        .clone()
        .context("reviewer run has no durable work-graph root")?;
    if occurrence.root_resource_scope_id.as_deref() != Some(root_execution_id.as_str()) {
        bail!("reviewer run has inconsistent root resource scope");
    }
    let now = utc_now();
    let (idle_timeout_secs, hard_timeout_secs) = task_agent_liveness_timeouts(task);
    let source_kind = match facts.identity.source_kind {
        AgentIdentitySourceKind::NativeAgent => pioneer_crud::SOURCE_NATIVE_AGENT,
        AgentIdentitySourceKind::CliRuntimeInstance => pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE,
        AgentIdentitySourceKind::Ephemeral => pioneer_crud::SOURCE_EPHEMERAL,
    };
    let source_id = exact_task_agent_source_id(
        processor,
        task.workspace_id.as_str(),
        task.id.as_str(),
        facts,
    )
    .await?;
    let source_revision = i64::try_from(facts.identity_source_revision)
        .context("reviewer identity source revision exceeds database range")?;
    let execution_generation = i64::try_from(occurrence.execution_generation)
        .context("reviewer execution generation exceeds database range")?;
    let attempt_generation = i64::from(occurrence.retry_attempt).saturating_add(1);
    let execution_id = facts.execution_id.as_str().to_owned();
    let snapshot_id = exact_task_agent_presentation_snapshot_id(
        processor,
        facts,
        &format!(
            "task-reviewer-snapshot\0{}\0{}",
            facts.identity.id, execution_id
        ),
    )
    .await?;
    let actor_contract = processor
        .crud_store
        .get_task_actor_contract(task.id.as_str())
        .await?
        .context("reviewer Task is missing its durable actor contract")?;
    let child_launch_grant = task_child_launch_grant(&actor_contract)?;
    let grant_json = serde_json::json!({
        "kind": "task_reviewer",
        "parent_execution_id": parent_execution_id.clone(),
        "execution_id": execution_id.clone(),
        "root_execution_id": root_execution_id.clone(),
        "reviewer_key": reviewer_key,
        "identity": facts.identity.clone(),
        "profile": facts.profile.clone(),
        "role_key": facts.agent_authorization_role_key,
        "agent_policy_generation": facts.agent_authorization_policy_generation,
        "allowed_actions": facts.agent_authorization_allowed_actions,
        "agent_authorization_fingerprint": facts.agent_authorization_fingerprint,
        "child_launch_grant": child_launch_grant,
    })
    .to_string();
    let grant_fingerprint = pioneer_crud::agent_execution_grant_fingerprint(&grant_json)?;
    let requested_identity = pioneer_protocol::AgentIdentitySelection::Exact {
        agent_identity_id: facts.identity.id.clone(),
    };
    let requested_execution = pioneer_protocol::AgentExecutionSelection {
        profile: pioneer_protocol::AgentExecutionProfileSelection::Exact {
            profile_id: facts.profile.id.clone(),
        },
        reasoning: None,
        permission_profile: None,
        skill_ids: Vec::new(),
        mcp_server_ids: Vec::new(),
    };
    let policy = crate::authorization::AgentWorkResourcePolicy::default();
    let result = processor
        .crud_store
        .commit_agent_execution_graph(AgentExecutionGraphCommitInput {
            identity: AgentIdentityInput {
                id: facts.identity.id.as_str().to_owned(),
                workspace_id: task.workspace_id.clone(),
                source_kind: source_kind.to_owned(),
                source_id,
                source_revision,
                source_fingerprint: facts.identity_source_fingerprint.clone(),
                now: now.clone().into(),
            },
            presentation: PresentationSnapshotInput {
                id: snapshot_id.clone(),
                agent_identity_id: facts.identity.id.as_str().to_owned(),
                source_revision,
                source_fingerprint: facts.identity_source_fingerprint.clone(),
                display_name: facts.identity.display_name.clone(),
                nickname: facts.identity.nickname.clone(),
                avatar_revision: facts.identity.avatar_revision.clone(),
                role_label: facts.identity.role_label.clone(),
                now: now.clone().into(),
            },
            root_execution_id: root_execution_id.clone(),
            root_execution: None,
            child_execution: AgentExecutionInput {
                id: execution_id.clone(),
                workspace_id: task.workspace_id.clone(),
                agent_identity_id: facts.identity.id.as_str().to_owned(),
                identity_source_revision: source_revision,
                identity_source_fingerprint: facts.identity_source_fingerprint.clone(),
                parent_execution_id: Some(parent_execution_id.clone()),
                parent_task_id: Some(task.id.clone()),
                parent_thread_id: Some(parent.parent_thread_id.clone()),
                home_root_thread_id: facts.home_root_thread_id.clone(),
                work_graph_root_execution_id: root_execution_id.clone(),
                requested_identity_selection_json: serde_json::to_string(&requested_identity)
                    .context("failed to encode reviewer identity selection")?,
                requested_profile_selection_json: serde_json::to_string(&requested_execution)
                    .context("failed to encode reviewer execution selection")?,
                resolved_profile_id: Some(facts.profile.id.as_str().to_owned()),
                resolved_profile_fingerprint: Some(facts.profile.fingerprint.clone()),
                presentation_snapshot_id: Some(snapshot_id.clone()),
                authorization_context_fingerprint: authorization_context_fingerprint.to_owned(),
                execution_generation,
                status: "created".to_owned(),
                now: now.clone().into(),
            },
            root_resource_state: None,
            child_resource_state: AgentResourceStateInput {
                id: canonical_agent_id(
                    'R',
                    &format!("reviewer-resource\0{execution_id}\0{attempt_generation}"),
                ),
                execution_id: execution_id.clone(),
                attempt_generation,
                branch_key: format!("task-reviewer:{}:{reviewer_key}", task.id),
                fair_order: 1,
                now: now.clone().into(),
            },
            grant: AgentExecutionGrantInput {
                id: canonical_agent_id('G', &format!("reviewer-grant\0{execution_id}")),
                execution_id: execution_id.clone(),
                parent_execution_id: Some(parent_execution_id),
                child_identity_id: facts.identity.id.as_str().to_owned(),
                grant_fingerprint,
                grant_json,
                now: now.clone().into(),
            },
            response: None,
            root_routes: Vec::new(),
            max_concurrency: i32::try_from(policy.max_concurrency).unwrap_or(i32::MAX),
            max_queue_depth: i32::try_from(policy.max_queue_depth).unwrap_or(i32::MAX),
            max_depth: i32::from(policy.max_depth),
            max_fan_out: i32::from(policy.max_fan_out),
            max_total_nodes: i32::try_from(policy.max_total_nodes).unwrap_or(i32::MAX),
            idle_timeout_secs,
            hard_timeout_secs,
            child_permit_id: canonical_agent_id(
                'P',
                &format!("reviewer-permit\0{execution_id}\0{attempt_generation}"),
            ),
            child_queue_id: canonical_agent_id(
                'Q',
                &format!("reviewer-queue\0{execution_id}\0{attempt_generation}"),
            ),
            task_actor_contract: None,
            task_occurrence_contract: None,
            contract_now: now_timestamp_secs(),
        })
        .await
        .context("failed to persist reviewer execution graph")?;
    processor
        .notify_agent_work_graph_state_changed(result.root_execution_id.as_str())
        .await;
    Ok(result)
}

async fn bind_existing_task_agent_graph(
    processor: &Arc<MessageProcessor>,
    run_id: &str,
    execution_id: &str,
    adapter: &mut crate::authorization::BoundAgentActionAdapter,
) -> Result<()> {
    let occurrence = processor
        .crud_store
        .get_task_occurrence_contract_by_run(run_id)
        .await?
        .context("task continuation is missing its durable occurrence contract")?;
    if occurrence.agent_execution_id.as_deref() != Some(execution_id) {
        bail!("task continuation execution differs from the persisted occurrence actor");
    }
    let root_execution_id = occurrence
        .work_graph_root_execution_id
        .as_deref()
        .context("task continuation occurrence has no persisted work-graph root")?;
    if occurrence.root_resource_scope_id.as_deref() != Some(root_execution_id) {
        bail!("task continuation occurrence has inconsistent root resource scope");
    }
    adapter
        .bind_persisted_work_graph_root(root_execution_id)
        .map_err(|error| anyhow!("failed to bind persisted task work graph: {error:?}"))
}

pub(super) type TaskAgentActionBinding = (
    crate::authorization::BoundAgentActionAdapter,
    pioneer_protocol::AgentToolOptionsProjection,
    std::collections::BTreeSet<pioneer_protocol::AgentToolCapability>,
);

/// Restore the exact identity/profile snapshot stored with an admitted Task
/// execution, or materialize the exact launch frozen in the Task actor
/// contract. Current Task, workspace, and runtime defaults are never used to
/// widen or rewrite that admitted launch.
#[allow(clippy::too_many_arguments)]
pub(super) async fn materialize_task_agent_action_binding_for_execution(
    processor: &MessageProcessor,
    task: &Task,
    agent_spec: &TaskAgentSpec,
    execution_id: &str,
    run_id: &str,
    root_capsule_id: &str,
    policy_generation: u64,
    allow_terminal_execution: bool,
) -> Result<TaskAgentActionBinding> {
    let database = processor.crud_store.database_connection();
    let actor_contract = processor
        .crud_store
        .get_task_actor_contract(task.id.as_str())
        .await?
        .context("agent Task is missing its durable actor contract")?;
    let effective_home_root_thread_id =
        if let Some(route_id) = actor_contract.execution_route_id.as_deref() {
            let route = pioneer_crud::load_agent_delegation_route(&database, route_id)
                .await?
                .context("Task execution route is unavailable")?;
            pioneer_crud::agent_delegation_route_projection(&route)?.destination_capsule_id
        } else {
            root_capsule_id.to_owned()
        };
    if let Some(execution) = pioneer_crud::load_agent_execution(&database, execution_id).await? {
        if execution.workspace_id != task.workspace_id
            || execution.parent_task_id.as_deref() != Some(task.id.as_str())
            || (!allow_terminal_execution
                && (execution.finished_at.is_some()
                    || matches!(
                        execution.status.as_str(),
                        "completed" | "failed" | "cancelled"
                    )))
        {
            bail!("persisted Task execution binding is stale or belongs to another Task");
        }
        if execution.home_root_thread_id != effective_home_root_thread_id {
            bail!("persisted Task execution left its admitted destination capsule");
        }
        let identity =
            pioneer_crud::load_agent_identity(&database, execution.agent_identity_id.as_str())
                .await?
                .context("persisted Task execution identity is missing")?;
        if identity.workspace_id != task.workspace_id || identity.status != "active" {
            bail!("persisted Task execution identity is unavailable");
        }
        super::agent_action_tools::current_agent_identity_source_fence(
            processor,
            execution.id.as_str(),
        )
        .await?;
        let snapshot_id = execution
            .presentation_snapshot_id
            .as_deref()
            .context("persisted Task execution has no presentation snapshot")?;
        let snapshot = pioneer_crud::load_agent_presentation_snapshot(&database, snapshot_id)
            .await?
            .context("persisted Task execution presentation snapshot is missing")?;
        if snapshot.agent_identity_id != identity.id
            || snapshot.source_revision != execution.identity_source_revision
            || snapshot.source_fingerprint != execution.identity_source_fingerprint
        {
            bail!("persisted Task presentation snapshot does not match its identity revision");
        }
        let source_kind = match identity.source_kind.as_str() {
            pioneer_crud::SOURCE_NATIVE_AGENT => AgentIdentitySourceKind::NativeAgent,
            pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE => {
                AgentIdentitySourceKind::CliRuntimeInstance
            }
            pioneer_crud::SOURCE_EPHEMERAL => AgentIdentitySourceKind::Ephemeral,
            _ => bail!("persisted Task identity has an unsupported source kind"),
        };
        let source_revision = u64::try_from(execution.identity_source_revision)
            .context("persisted Task identity revision is invalid")?;
        let identity_projection = pioneer_protocol::AgentIdentityProjection::new(
            pioneer_protocol::AgentIdentityId::new(identity.id.clone())
                .map_err(|error| anyhow!("persisted Task identity id is invalid: {error:?}"))?,
            source_kind,
            snapshot.display_name,
            snapshot.nickname,
            snapshot.avatar_revision,
            snapshot.role_label,
            source_revision,
            execution.identity_source_fingerprint.clone(),
        )
        .map_err(|error| anyhow!("persisted Task identity projection is invalid: {error:?}"))?;
        let execution_grant =
            pioneer_crud::load_agent_execution_grant(&database, execution.id.as_str())
                .await?
                .context("persisted Task execution grant is missing")?;
        let grant: serde_json::Value = serde_json::from_str(execution_grant.grant_json.as_str())
            .context("persisted Task execution grant is invalid")?;
        let grant_kind = grant.get("kind").and_then(serde_json::Value::as_str);
        let profile: pioneer_protocol::AgentExecutionProfileProjection = serde_json::from_value(
            grant
                .get("profile")
                .cloned()
                .context("persisted Task execution grant has no resolved profile")?,
        )
        .context("persisted Task resolved execution profile is invalid")?;
        if execution.resolved_profile_id.as_deref() != Some(profile.id.as_str())
            || execution.resolved_profile_fingerprint.as_deref()
                != Some(profile.fingerprint.as_str())
        {
            bail!("persisted Task execution profile differs from its resolved snapshot");
        }
        if grant_kind != Some("task_reviewer")
            && let Some(selection) = actor_contract.launch.as_ref()
        {
            if execution.requested_identity_selection_json
                != serde_json::to_string(&selection.agent)?
                || execution.requested_profile_selection_json
                    != serde_json::to_string(&selection.execution)?
            {
                bail!(
                    "persisted Task execution requested selection differs from its actor contract"
                );
            }
        }
        let execution_id = AgentExecutionId::new(execution.id.clone())
            .map_err(|error| anyhow!("persisted Task execution id is invalid: {error:?}"))?;
        let root_execution_id =
            AgentExecutionId::new(execution.work_graph_root_execution_id.clone())
                .map_err(|error| anyhow!("persisted Task graph root is invalid: {error:?}"))?;
        let execution_generation = u64::try_from(execution.execution_generation)
            .context("persisted Task execution generation is invalid")?;
        let resource_state =
            pioneer_crud::load_agent_execution_resource_state(&database, execution.id.as_str())
                .await?
                .context("persisted Task execution resource attempt is missing")?;
        let attempt_generation = u64::try_from(resource_state.attempt_generation)
            .context("persisted Task execution attempt generation is invalid")?;
        let depth =
            u16::try_from(agent_spec.depth).context("persisted Task execution depth is invalid")?;
        let role_key = grant
            .get("role_key")
            .and_then(serde_json::Value::as_str)
            .context("persisted Task execution grant has no subject role")?;
        let persisted_policy_generation = grant
            .get("agent_policy_generation")
            .and_then(serde_json::Value::as_u64)
            .context("persisted Task execution grant has no policy generation")?;
        let agent_authorization_fingerprint = grant
            .get("agent_authorization_fingerprint")
            .and_then(serde_json::Value::as_str)
            .context("persisted Task execution grant has no authorization fingerprint")?;
        let allowed_action_names: Vec<String> = serde_json::from_value(
            grant
                .get("allowed_actions")
                .cloned()
                .context("persisted Task execution grant has no action ceiling")?,
        )
        .context("persisted Task execution action ceiling is invalid")?;
        return crate::authorization::materialize_persisted_task_agent_action_binding(
            execution_id,
            effective_home_root_thread_id.as_str(),
            root_execution_id,
            identity_projection,
            profile,
            execution_generation,
            attempt_generation,
            depth,
            &format!("task:{}", task.id),
            role_key,
            persisted_policy_generation,
            policy_generation,
            agent_authorization_fingerprint,
            allowed_action_names.as_slice(),
        )
        .map_err(|error| anyhow!("failed to restore exact Task agent binding: {error:?}"));
    }

    if let Some(grant_json) = actor_contract.derived_child_launch_grant_json.as_deref() {
        let pioneer_protocol::TaskDerivedChildLaunchGrant::ResolvedTaskLaunch {
            identity,
            profile,
            role_key,
            agent_policy_generation: persisted_policy_generation,
            allowed_actions: allowed_action_names,
            agent_authorization_fingerprint: authorization_fingerprint,
            child_launch_grant: _,
        } = serde_json::from_str(grant_json).context("Task resolved launch grant is invalid")?;
        let identity_row = pioneer_crud::load_agent_identity(&database, identity.id.as_str())
            .await?
            .context("Task resolved launch identity is no longer available")?;
        if identity_row.workspace_id != task.workspace_id
            || identity_row.status != "active"
            || identity_row.source_revision != i64::try_from(identity.source_revision).unwrap_or(-1)
            || identity_row.source_fingerprint != identity.source_fingerprint
        {
            bail!("Task resolved launch identity changed before occurrence admission");
        }
        let occurrence = processor
            .crud_store
            .get_task_occurrence_contract_by_run(run_id)
            .await?
            .context("Task run is missing its occurrence contract")?;
        let execution_generation = occurrence.execution_generation;
        let depth = u16::try_from(agent_spec.depth).context("Task execution depth is invalid")?;
        let execution_id = AgentExecutionId::new(execution_id.to_owned())
            .map_err(|error| anyhow!("Task execution id is invalid: {error:?}"))?;
        let task_creator_execution_id = match &actor_contract.creator {
            pioneer_protocol::PersistedActorRef::AgentExecution(execution_id) => {
                Some(execution_id.as_str())
            }
            _ => None,
        };
        let (work_graph_root_execution_id, _) = task_occurrence_execution_lineage(
            execution_id.as_str(),
            occurrence.work_graph_root_execution_id.as_deref(),
            occurrence.agent_execution_id.as_deref(),
            actor_contract.work_graph_root_execution_id.as_deref(),
            task_creator_execution_id,
        )?;
        let work_graph_root_execution_id = AgentExecutionId::new(work_graph_root_execution_id)
            .map_err(|error| anyhow!("Task work graph root is invalid: {error:?}"))?;
        return crate::authorization::materialize_persisted_selected_task_agent_action_binding(
            execution_id,
            effective_home_root_thread_id.as_str(),
            work_graph_root_execution_id,
            agent_spec.id.as_str(),
            identity,
            profile,
            execution_generation,
            u64::from(occurrence.retry_attempt).saturating_add(1),
            depth,
            role_key.as_str(),
            persisted_policy_generation,
            policy_generation,
            authorization_fingerprint.as_str(),
            allowed_action_names.as_slice(),
        )
        .map_err(|error| anyhow!("failed to bind resolved Task launch: {error:?}"));
    }
    bail!("Task launch selection was not resolved at create/schedule commit")
}

#[derive(Debug, Clone)]
pub(crate) enum TaskChildResumeOutcome {
    Resumed { recovery_job_id: String },
    NotFound,
    MissingRuntimeSnapshot { recovery_job_id: String },
    Conflict { reason: String },
}

async fn close_admitted_task_turn_on_error<T>(
    processor: &Arc<MessageProcessor>,
    thread_id: &str,
    turn_id: &str,
    result: Result<T>,
) -> Result<T> {
    let Err(error) = result else {
        return result;
    };
    let reason = "task_turn_admission_failed".to_owned();
    if !processor
        .mark_turn_blocked(thread_id.to_owned(), turn_id.to_owned(), reason.clone())
        .await
    {
        warn!(
            thread_id,
            turn_id,
            failure_class = "task_turn_admission_close_failed",
            "failed to durably close task turn after admission failure"
        );
    }
    Err(error)
}

async fn report_or_block_task_turn_failure(
    processor: &Arc<MessageProcessor>,
    thread_id: String,
    turn_id: String,
    kind: TurnFailureRecoveryKind,
    _reason_detail: String,
) {
    let reason = "task_runtime_failure".to_owned();
    if !processor
        .report_turn_failure(thread_id.clone(), turn_id.clone(), kind, reason.clone())
        .await
        && !processor
            .mark_turn_blocked(thread_id.clone(), turn_id.clone(), reason.clone())
            .await
    {
        warn!(
            thread_id,
            turn_id,
            failure_class = "task_runtime_failure_persistence_failed",
            "failed to report or durably block task turn after runtime failure"
        );
    }
}

async fn commit_task_turn_execution_running(
    processor: &Arc<MessageProcessor>,
    thread_id: &str,
    turn_id: &str,
) -> Result<()> {
    if processor
        .crud_store
        .get_turn_execution(turn_id)
        .await?
        .is_none()
    {
        bail!("Task Turn `{turn_id}` has no durable execution-owner record");
    }
    let now = now_timestamp_secs();
    if processor
        .crud_store
        .mark_turn_execution_running_owned(
            turn_id,
            processor.turn_execution_owner_id.as_ref(),
            now,
            now.saturating_add(super::TURN_EXECUTION_OWNER_LEASE_SECONDS),
        )
        .await?
    {
        return Ok(());
    }
    let reason = "task Turn execution ownership changed before native dispatch completed";
    let _ = processor
        .agent_manager
        .cancel_turn(thread_id, turn_id, reason)
        .await;
    bail!(reason)
}

async fn claim_task_turn_execution_for_recovery(
    processor: &Arc<MessageProcessor>,
    turn_id: &str,
) -> Result<()> {
    let execution = processor
        .crud_store
        .get_turn_execution(turn_id)
        .await?
        .with_context(|| format!("Task Turn `{turn_id}` has no durable execution-owner record"))?;
    if execution.owner_id == processor.turn_execution_owner_id.as_ref() {
        return Ok(());
    }

    let now = now_timestamp_secs();
    if processor
        .crud_store
        .claim_expired_turn_execution(
            &execution,
            processor.turn_execution_owner_id.as_ref(),
            now,
            now.saturating_add(super::TURN_EXECUTION_OWNER_LEASE_SECONDS),
        )
        .await?
        .is_some()
    {
        return Ok(());
    }

    bail!("task Turn execution is still owned by another live Gateway")
}

async fn verify_durable_task_child_admission(
    processor: &Arc<MessageProcessor>,
    child_runtime: &TaskRunChildRuntime,
    execution: &TaskRunExecution,
) -> Result<()> {
    let task_run_turn = &child_runtime.task_run_turn;
    let task = processor
        .crud_store
        .get_task(task_run_turn.task_id.as_str())
        .await?
        .ok_or_else(|| {
            anyhow!(
                "task `{}` is missing after child admission",
                task_run_turn.task_id
            )
        })?;
    if task.task.status.is_terminal() {
        bail!(
            "task `{}` became terminal before child `{}` activation",
            task_run_turn.task_id,
            task_run_turn.turn_id
        );
    }

    let run = processor
        .crud_store
        .get_task_run(task_run_turn.run_id.as_str())
        .await?
        .ok_or_else(|| {
            anyhow!(
                "task run `{}` is missing after child admission",
                task_run_turn.run_id
            )
        })?;
    if run.task_id != task_run_turn.task_id || run.status.is_terminal() {
        bail!(
            "task run `{}` no longer owns an active child admission",
            task_run_turn.run_id
        );
    }

    let persisted_turn = processor
        .crud_store
        .get_task_run_turn_by_turn(
            task_run_turn.thread_id.as_str(),
            task_run_turn.turn_id.as_str(),
        )
        .await?
        .ok_or_else(|| {
            anyhow!(
                "task run turn `{}` is missing after child admission",
                task_run_turn.id
            )
        })?;
    if persisted_turn.id != task_run_turn.id
        || persisted_turn.task_id != task_run_turn.task_id
        || persisted_turn.run_id != task_run_turn.run_id
        || persisted_turn.status != TaskRunTurnStatus::InProgress
    {
        bail!(
            "task run turn `{}` changed before child activation",
            task_run_turn.id
        );
    }

    let persisted_execution = processor
        .crud_store
        .load_execution_for_run(task_run_turn.run_id.as_str())
        .await?
        .ok_or_else(|| {
            anyhow!(
                "task execution `{}` is missing after child admission",
                execution.id
            )
        })?;
    if persisted_execution.id != execution.id || persisted_execution.status.is_terminal() {
        bail!(
            "task execution `{}` changed before child activation",
            execution.id
        );
    }

    if processor
        .crud_store
        .get_turn(
            task_run_turn.thread_id.as_str(),
            task_run_turn.turn_id.as_str(),
        )
        .await?
        .is_none()
    {
        bail!(
            "child Turn `{}` is missing after durable admission",
            task_run_turn.turn_id
        );
    }

    Ok(())
}

#[derive(Default)]
pub(crate) struct TaskAgentExecutor {
    processor: StdRwLock<Option<Weak<MessageProcessor>>>,
}

impl TaskAgentExecutor {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn bind(&self, processor: Weak<MessageProcessor>) {
        if let Ok(mut guard) = self.processor.write() {
            *guard = Some(processor);
        }
    }

    fn processor(&self) -> Result<Arc<MessageProcessor>> {
        let weak = self
            .processor
            .read()
            .map_err(|_| anyhow!("task agent executor lock is poisoned"))?
            .clone()
            .ok_or_else(|| anyhow!("task agent executor is not bound"))?;
        weak.upgrade()
            .ok_or_else(|| anyhow!("message processor is no longer available"))
    }

    pub(super) fn processor_weak(&self) -> Result<Weak<MessageProcessor>> {
        self.processor
            .read()
            .map_err(|_| anyhow!("task agent executor lock is poisoned"))?
            .clone()
            .ok_or_else(|| anyhow!("task agent executor is not bound"))
    }

    fn task_reconciliation_processor(
        &self,
        origin: TaskChildReconciliationOrigin,
    ) -> Result<Option<Arc<MessageProcessor>>> {
        match self.processor() {
            Ok(processor) => Ok(Some(match origin {
                TaskChildReconciliationOrigin::Live => {
                    processor.with_database_class(SqliteWriteClass::Critical)
                }
                TaskChildReconciliationOrigin::DurableBackground => {
                    processor.for_background_reconciliation()
                }
            })),
            Err(error) if error.to_string() == "task agent executor is not bound" => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn root_collaboration_still_has_task_authority(
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
    ) -> Result<bool> {
        let task = &task_response.task;
        let admission = processor
            .crud_store
            .get_task_execution_admission(task.id.as_str())
            .await?
            .with_context(|| {
                format!(
                    "agent Task `{}` has no durable execution admission",
                    task.id
                )
            })?;
        let context = crate::authorization::ExecutionAuthorizationContext::load_for_task_admission(
            processor.crud_store.as_ref(),
            &admission,
        )
        .await?;
        if admission.workspace_id != task.workspace_id
            || admission.workspace_id != context.workspace_id()
            || admission.root_thread_id != context.root_thread_id()
            || admission.initiating_principal_id != context.initiating_principal_id().as_str()
        {
            return Ok(false);
        }
        let revision = processor.current_authorization_revision().await?;
        Ok(processor
            .execution_leases
            .revalidate_context(
                processor.crud_store.as_ref(),
                &context,
                crate::authorization::ResourceAction::TaskCreate,
                revision,
            )
            .await
            .is_ok())
    }

    async fn start_or_recover_run(
        &self,
        context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        let processor = self
            .processor()?
            .with_database_class(SqliteWriteClass::Critical);
        let handle = handle.with_critical_writes();
        let Some(task_response) = processor.crud_store.get_task(run.task_id.as_str()).await? else {
            bail!("task `{}` not found", run.task_id);
        };
        if !Self::root_collaboration_still_has_task_authority(&processor, &task_response).await? {
            return Ok(TaskExecutorStartOutcome::Rejected);
        }
        if task_response.task.status.is_terminal() || run.status.is_terminal() {
            return Ok(TaskExecutorStartOutcome::Queued);
        }
        let agent_spec = select_agent_spec(&task_response, run.id.as_str())
            .ok_or_else(|| anyhow!("agent task `{}` has no agent spec", run.task_id))?;
        if run.status == TaskRunStatus::WaitingReview {
            return self
                .recover_waiting_review_run(&processor, &task_response, &run, &agent_spec)
                .await;
        }
        // A disabled native runtime must not consume a new task execution
        // reservation.  Keep the run queued so enabling the same configured
        // runtime can resume it without rewriting its history.  The session
        // factory repeats this check at process start to cover a config change
        // racing this preflight.
        if !cli_runtime_backend_enabled(&processor, &task_response.task)? {
            return Ok(TaskExecutorStartOutcome::Queued);
        }
        let Some(execution) = self
            .load_or_reserve_execution(&processor, &context, &run)
            .await?
        else {
            return Ok(TaskExecutorStartOutcome::Queued);
        };

        if let Some(child_runtime) = load_child_runtime_for_run(&processor, run.id.as_str()).await?
        {
            return self
                .recover_existing_child_turn(
                    &processor,
                    &task_response,
                    &run,
                    &agent_spec,
                    &execution,
                    child_runtime,
                    handle,
                )
                .await;
        }

        let parent = resolve_parent_context(&processor, &task_response.task).await?;
        match self
            .acquire_write_locks(&processor, &task_response.task, &run, handle.clone())
            .await?
        {
            TaskExecutorStartOutcome::Started => {}
            outcome => return Ok(outcome),
        }
        // The occurrence turn is the durable security parent for this run, so it
        // carries the Task's maximum cap. The composer selection is applied to
        // the actual hidden child below and may safely narrow that cap.
        let occurrence_permission_profile =
            effective_task_child_permission_profile(&agent_spec, None)?;
        let parent = ensure_task_run_occurrence_context(
            &processor,
            &task_response,
            &run,
            &execution,
            &agent_spec,
            parent,
            &occurrence_permission_profile,
        )
        .await?;
        self.start_new_child_turn(
            &processor,
            &context,
            &task_response,
            &run,
            &agent_spec,
            &parent,
            execution,
            handle,
        )
        .await
    }

    async fn load_or_reserve_execution(
        &self,
        processor: &Arc<MessageProcessor>,
        context: &TaskExecutionContext,
        run: &TaskRun,
    ) -> Result<Option<TaskRunExecution>> {
        let now = now_timestamp_secs();
        let reserved = processor
            .crud_store
            .reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::Agent, now)
            .await
            .context("failed to reserve task run execution")?;
        if let Some(context_execution_id) = context.execution_id.as_deref()
            && context_execution_id != reserved.id
        {
            bail!(
                "task run `{}` context execution `{}` does not match reserved execution `{}`",
                run.id,
                context_execution_id,
                reserved.id
            );
        }
        let lease_until = now.saturating_add(TASK_EXECUTION_LEASE_SECONDS);
        let claimed = processor
            .crud_store
            .claim_execution_at(
                reserved.id.as_str(),
                context.worker_id.as_str(),
                now,
                lease_until,
            )
            .await
            .context("failed to claim task run execution")?;
        Ok(claimed)
    }

    async fn recover_waiting_review_run(
        &self,
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        run: &TaskRun,
        agent_spec: &TaskAgentSpec,
    ) -> Result<TaskExecutorStartOutcome> {
        let Some(review_policy) = agent_spec
            .review_policy
            .as_ref()
            .filter(|policy| policy.is_enabled())
        else {
            return Ok(TaskExecutorStartOutcome::Queued);
        };
        let mut candidates = processor
            .crud_store
            .list_task_result_candidates(run.id.as_str())
            .await?;
        candidates.retain(|candidate| {
            matches!(
                candidate.status,
                TaskResultCandidateStatus::PendingReview
                    | TaskResultCandidateStatus::ExtractionFailed
            )
        });
        candidates.sort_by(|left, right| {
            right
                .round
                .cmp(&left.round)
                .then_with(|| right.created_at.cmp(&left.created_at))
        });
        if let Some(candidate) = candidates.first() {
            self.start_reviewer_turns_for_candidate(
                processor,
                task_response,
                agent_spec,
                review_policy,
                candidate,
            )
            .await?;
        }
        Ok(TaskExecutorStartOutcome::Started)
    }

    async fn start_new_child_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        context: &TaskExecutionContext,
        task_response: &TaskGetResponse,
        run: &TaskRun,
        agent_spec: &TaskAgentSpec,
        parent: &TaskParentRuntimeContext,
        execution: TaskRunExecution,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        let task = &task_response.task;
        let now = now_timestamp_secs();
        let task_run_turn = initial_task_run_turn_from_execution(task, run, &execution, now);
        let binding =
            task_run_primary_binding_from_turn(task, run, &execution, &task_run_turn, now);
        let child_runtime = TaskRunChildRuntime {
            lineage: lineage_from_task_run_turn(task, run, agent_spec, parent, &task_run_turn, now),
            task_run_turn,
        };
        let child_thread_id = child_runtime.task_run_turn.thread_id.clone();
        let child_turn_id = child_runtime.task_run_turn.turn_id.clone();
        let mut composer_launch =
            rebound_composer_work_launch(task, child_thread_id.as_str(), child_turn_id.as_str())?;
        if let Some(launch) = composer_launch.as_ref() {
            validate_composer_launch_backend(launch)?;
        }
        // Resolve the immutable Task launch before creating any hidden
        // conversation state. The selected profile, not Task metadata or the
        // current parent thread, is authoritative for backend/provider/model.
        let action_policy_generation = processor.current_authorization_revision().await?;
        let (mut action_adapter, action_options, action_capabilities) =
            materialize_task_agent_action_binding_for_execution(
                processor,
                &task_response.task,
                agent_spec,
                execution.id.as_str(),
                run.id.as_str(),
                parent.root_thread_id.as_str(),
                action_policy_generation,
                false,
            )
            .await
            .context("failed to bind child agent action service")?;
        let action_facts = action_adapter.persistence_facts();
        let launch_selection = processor
            .crud_store
            .get_task_actor_contract(task.id.as_str())
            .await?
            .context("agent Task is missing its durable actor contract")?
            .launch;
        let effective_model = EffectiveAgentModel {
            model: action_facts.profile.model_id.clone(),
            model_provider: action_facts.profile.provider_id.clone(),
        };
        let (cli_runtime_backend, selected_execution_backend) = match &action_facts.profile.backend
        {
            AgentExecutionProfileBackend::ApiProvider => (
                None,
                Some(AgentExecutionBackend::ApiProvider {
                    provider: action_facts.profile.provider_id.clone(),
                }),
            ),
            AgentExecutionProfileBackend::CliRuntime {
                runtime_instance_id,
            } => {
                let runtime = processor
                    .load_cli_runtime_instances()?
                    .into_iter()
                    .find(|runtime| runtime.id == *runtime_instance_id && runtime.enabled)
                    .context("selected Task CLI runtime is unavailable")?;
                let runtime_kind = match runtime.kind {
                    pioneer_config::GatewayCliAgentRuntimeKindConfig::Codex => {
                        CLIAgentRuntimeKind::Codex
                    }
                    pioneer_config::GatewayCliAgentRuntimeKindConfig::Claude => {
                        CLIAgentRuntimeKind::Claude
                    }
                };
                (
                    Some((runtime_instance_id.clone(), runtime_kind)),
                    Some(AgentExecutionBackend::CLIAgentRuntime {
                        runtime_id: runtime_instance_id.clone(),
                        runtime_kind,
                    }),
                )
            }
            AgentExecutionProfileBackend::AcpAgentRuntime { runtime_id } => (
                None,
                Some(AgentExecutionBackend::ACPAgentRuntime {
                    runtime_id: runtime_id.clone(),
                }),
            ),
        };
        if let Some(launch) = composer_launch.as_mut() {
            if launch
                .execution_backend
                .as_ref()
                .is_some_and(|backend| Some(backend) != selected_execution_backend.as_ref())
            {
                bail!(
                    "Composer Task backend {:?} differs from its resolved launch profile {:?}",
                    launch.execution_backend,
                    selected_execution_backend
                );
            }
            launch.execution_backend = selected_execution_backend.clone();
            launch.model = Some(effective_model.model.clone());
            launch.model_provider = Some(effective_model.model_provider.clone());
        }
        let normalized_composer_capabilities = if cli_runtime_backend.is_none()
            && let Some(launch) = composer_launch.as_mut()
        {
            let normalized = processor
                .normalize_turn_skill_capabilities(
                    context.workspace_id.as_str(),
                    launch.capabilities.as_slice(),
                )
                .await
                .map_err(|message| anyhow!(message))
                .context("failed to normalize composer work capabilities")?;
            launch.capabilities = normalized.execution.clone();
            Some(normalized)
        } else {
            None
        };
        let normalized_selected_capabilities = if let Some(selection) = launch_selection.as_ref() {
            let requested =
                super::agent_action_tools::launch_selection_capabilities(&selection.execution)
                    .context("persisted Task launch capabilities are invalid")?;
            Some(
                processor
                    .normalize_turn_skill_capabilities(
                        context.workspace_id.as_str(),
                        requested.as_slice(),
                    )
                    .await
                    .map_err(|message| anyhow!(message))
                    .context("persisted Task launch capabilities are unavailable")?,
            )
        } else {
            None
        };
        if let (Some(composer), Some(selection), Some(launch)) = (
            normalized_composer_capabilities.as_ref(),
            launch_selection.as_ref(),
            composer_launch.as_ref(),
        ) {
            let mut validation_launch = launch.clone();
            validation_launch.capabilities = composer.execution.clone();
            validation_launch.agent_launch = Some(selection.clone());
            super::turn_handlers::validate_root_agent_launch_capabilities(&validation_launch)
                .map_err(|_| {
                    anyhow!("Composer Task capabilities differ from its persisted launch selection")
                })?;
        }
        let effective_model_provider = effective_model.model_provider.clone();
        let child_mode = composer_launch
            .as_ref()
            .and_then(|launch| launch.mode)
            .unwrap_or(ThreadMode::Agent);
        // The actor launch selection describes the selected Agent/profile. Its
        // per-turn permission is optional: ordinary Composer work persists that
        // selection on `composer_work.launch` instead. Do not turn an omitted
        // actor field into FullAccess, because that would manufacture a false
        // conflict with Supervised or AutoAcceptEdits Composer launches.
        let selected_permission_profile = launch_selection
            .as_ref()
            .and_then(|selection| selection.execution.permission_profile.as_ref())
            .map(|selection| pioneer_protocol::resolve_turn_permission_profile(Some(selection)));
        let composer_permission_profile =
            composer_launch_permission_profile(composer_launch.as_ref());
        if let (Some(selected), Some(composer)) = (
            selected_permission_profile.as_ref(),
            composer_permission_profile.as_ref(),
        ) && selected.mode != composer.mode
        {
            bail!("Composer Task permission profile differs from its persisted launch selection");
        }
        let launch_permission_profile = selected_permission_profile.or(composer_permission_profile);
        let child_permission_profile = effective_task_child_permission_profile(
            agent_spec,
            launch_permission_profile.as_ref(),
        )?;
        let selected_reasoning = launch_selection
            .as_ref()
            .and_then(|selection| selection.execution.reasoning.clone());
        let composer_reasoning_effort = composer_launch_reasoning_effort(composer_launch.as_ref());
        if let (Some(selected), Some(composer)) = (
            selected_reasoning.as_ref(),
            composer_reasoning_effort.as_ref(),
        ) && selected.effort.trim() != composer
        {
            bail!("Composer Task reasoning differs from its persisted launch selection");
        }
        let reasoning_effort = selected_reasoning
            .as_ref()
            .map(|reasoning| reasoning.effort.trim())
            .filter(|effort| !effort.is_empty())
            .map(str::to_owned)
            .or(composer_reasoning_effort);
        if let Some(launch) = composer_launch.as_mut()
            && let Some(selection) = launch_selection.as_ref()
        {
            if let Some(reasoning) = selection.execution.reasoning.clone() {
                launch.reasoning = Some(reasoning);
            }
            if let Some(permission_profile) = selection.execution.permission_profile.clone() {
                launch.permission_profile = Some(permission_profile);
            }
        }
        let sandbox_mode = match composer_launch
            .as_ref()
            .and_then(|launch| launch.sandbox_policy.as_ref())
            .map(|policy| policy.mode)
        {
            Some(mode) => mode,
            None => processor
                .crud_store
                .get_thread_sandbox_mode(parent.parent_thread_id.as_str())
                .await?
                .with_context(|| {
                    format!(
                        "agent Task `{}` parent thread `{}` has no persisted sandbox policy",
                        task.id, parent.parent_thread_id
                    )
                })?,
        };
        let thread_params = pioneer_protocol::ThreadStartParams {
            thread_id: child_thread_id.clone(),
            workspace_id: context.workspace_id.clone(),
            name: thread_name_from_task(task),
            model: Some(effective_model.model.clone()),
            model_provider: Some(effective_model_provider.clone()),
            sandbox: Some(sandbox_mode),
            mode: Some(child_mode),
            origin_kind: Some(ThreadOriginKind::TaskRun),
            sidebar_visibility: Some(ThreadSidebarVisibility::Hidden),
            visibility: None,
            agent_nickname: agent_spec.agent_nickname.clone(),
            agent_role: agent_spec.agent_role.clone(),
        };
        let thread_outcome = processor
            .thread_manager
            .system_thread_start_seeded(context.workspace_id.clone(), thread_params, None, None)
            .await
            .context("failed to create hidden task thread")?;
        let frozen_conversation_scope = if task_attachment(task) == TaskAttachmentMode::Detached {
            Some(
                load_task_execution_conversation_scope(
                    processor,
                    task,
                    run,
                    parent,
                    child_runtime.task_run_turn.kind,
                    child_thread_id.as_str(),
                    child_turn_id.as_str(),
                    thread_outcome.started_notification.thread.model.as_str(),
                    thread_outcome
                        .started_notification
                        .thread
                        .model_provider
                        .as_str(),
                )
                .await?,
            )
        } else {
            None
        };
        let frozen_parent_history = frozen_conversation_scope
            .as_ref()
            .map(|(_, history)| history.as_slice());

        let child_input = if let Some(launch) = composer_launch.as_ref() {
            launch.input.clone()
        } else {
            let prompt = materialize_child_task_prompt(
                processor,
                task_response,
                run,
                agent_spec,
                parent,
                None,
                &child_permission_profile,
                frozen_parent_history,
            )
            .await?;
            materialize_child_task_input(prompt, agent_spec)
        };
        let selected_capabilities = normalized_selected_capabilities
            .as_ref()
            .map(|normalized| normalized.execution.clone())
            .unwrap_or_default();
        let turn_params = composer_launch.unwrap_or_else(|| TurnStartParams {
            agent_delegation_routes: Vec::new(),
            thread_id: child_thread_id.clone(),
            turn_id: child_turn_id.clone(),
            input: child_input.clone(),
            capabilities: selected_capabilities,
            model: Some(effective_model.model.clone()),
            model_provider: Some(effective_model_provider.clone()),
            sandbox_policy: None,
            mode: Some(child_mode),
            agent_launch: None,
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: selected_execution_backend,
            reasoning: selected_reasoning,
            permission_profile: launch_selection
                .as_ref()
                .and_then(|selection| selection.execution.permission_profile.clone()),
            cli_runtime_options: None,
        });
        let child_execution_backend = turn_params.execution_backend.clone();
        let child_security_snapshot = match cli_runtime_backend.as_ref() {
            Some((runtime_id, runtime_kind)) => resolve_task_child_cli_execution_security_snapshot(
                processor,
                context.workspace_id.as_str(),
                parent,
                agent_spec,
                child_permission_profile.clone(),
                runtime_id.as_str(),
                *runtime_kind,
                child_thread_id.as_str(),
                child_turn_id.as_str(),
            )
            .await
            .context("failed to resolve hidden task CLI runtime execution security")?,
            None => resolve_task_child_execution_security_snapshot(
                processor,
                context.workspace_id.as_str(),
                parent,
                agent_spec,
                child_permission_profile.clone(),
                effective_model_provider.as_str(),
                child_thread_id.as_str(),
                child_turn_id.as_str(),
            )
            .await
            .context("failed to resolve hidden task execution security")?,
        };
        let (child_authorization, mut agent_skill_overlay) =
            resolve_task_child_execution_authorization_context(
                processor,
                task,
                parent,
                effective_model_provider.as_str(),
                effective_model.model.as_str(),
                child_execution_backend.as_ref(),
                turn_params.capabilities.as_slice(),
                &child_security_snapshot.permission_profile,
                child_turn_id.as_str(),
            )
            .await
            .context("failed to resolve hidden task execution authorization")?;
        let child_authorization_context = child_authorization.context;
        let child_authorization_revalidation = child_authorization.revalidation;
        let child_authorization_fingerprint = child_authorization_context
            .authorization_fingerprint()
            .context("failed to fingerprint hidden task execution authorization")?;
        let actor_contract = processor
            .crud_store
            .get_task_actor_contract(task.id.as_str())
            .await?
            .context("agent Task is missing its durable actor contract")?;
        let input_author = task_actor_turn_author(processor, &actor_contract).await?;
        let non_cli_action_author = input_author.clone();
        let graph = persist_task_agent_execution_graph(
            processor,
            task_response,
            run,
            agent_spec,
            parent,
            &action_facts,
            child_authorization_fingerprint.as_str(),
        )
        .await?;
        if graph.queued
            && pioneer_crud::load_agent_execution(
                &processor.crud_store.database_connection(),
                graph.root_execution_id.as_str(),
            )
            .await?
            .is_some_and(|execution| execution.status == "cancelled")
        {
            self.cancel_run(
                context.clone(),
                run.id.as_str(),
                "parent Agent work graph cancelled",
                handle,
            )
            .await?;
            return Ok(TaskExecutorStartOutcome::Started);
        }
        action_adapter
            .bind_persisted_work_graph_root(graph.root_execution_id.as_str())
            .map_err(|error| anyhow!("failed to bind persisted task work graph: {error:?}"))?;
        if graph.queued {
            return Ok(TaskExecutorStartOutcome::Queued);
        }
        let turn_response =
            agent_turn_response_input(processor, child_turn_id.as_str(), execution.id.as_str())
                .await?;
        if let Some((runtime_id, runtime_kind)) = cli_runtime_backend {
            let action_author = input_author;
            return message_future(async move {
                let conversation_history = frozen_conversation_scope
                    .as_ref()
                    .map(|(_, history)| history.clone())
                    .unwrap_or_default();
                // Child-scoped authorization is revalidated while CLI MCP and skill
                // projections are committed. Persist the durable lineage first so
                // those checks can prove that the hidden thread belongs to the
                // parent's authorization root.
                handle
                    .link_child_thread_with_runtime(
                        child_runtime.lineage.clone(),
                        binding,
                        child_runtime.task_run_turn.clone(),
                        now,
                    )
                    .await
                    .context("failed to link hidden task CLI runtime turn")?;
                // The shared CLI preparation future is deliberately large. Run it
                // from a fresh Tokio task so Task scheduler dispatch frames do not
                // consume the native runtime worker's stack before preparation
                // begins.
                let prepare_processor = processor.clone();
                let continuation_thread_id = parent.parent_thread_id.clone();
                let task_run_id = run.id.clone();
                let execution_id = execution.id.clone();
                let prepared = message_fresh_task(async move {
                    prepare_processor
                        .prepare_task_cli_runtime_turn(
                            TurnStartParams {
                                input: child_input,
                                model: Some(effective_model.model),
                                model_provider: Some(effective_model_provider),
                                mode: Some(child_mode),
                                ..turn_params
                            },
                            runtime_id,
                            runtime_kind,
                            child_permission_profile,
                            child_security_snapshot,
                            child_authorization_context,
                            child_authorization_revalidation,
                            continuation_thread_id.clone(),
                            continuation_thread_id,
                            task_run_id,
                            execution_id,
                            conversation_history,
                            action_author,
                            turn_response,
                        )
                        .await
                })
                .await
                .map_err(|error| anyhow!("task CLI runtime preparation task failed: {error}"))?;
                let prepared = match prepared {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        if processor
                            .crud_store
                            .get_task_run(run.id.as_str())
                            .await?
                            .is_some_and(|current| current.status.is_terminal())
                        {
                            return Ok(TaskExecutorStartOutcome::Started);
                        }
                        record_task_run_turn_failure(
                            &handle,
                            &child_runtime.task_run_turn,
                            TaskRunTurnStatus::Failed,
                            Some(task_error(
                                "task_cli_runtime_prepare_failed",
                                format!(
                                    "failed to prepare hidden task CLI runtime turn: {error:#}"
                                ),
                                TaskErrorClass::Internal,
                                Some(run.id.clone()),
                            )),
                            now_timestamp_secs(),
                        )
                        .await
                        .context("failed to record hidden task CLI runtime preparation failure")?;
                        return Err(error)
                            .context("failed to prepare hidden task CLI runtime turn");
                    }
                };
                if processor
                    .crud_store
                    .get_task_run(run.id.as_str())
                    .await?
                    .is_none_or(|current| current.status.is_terminal())
                {
                    processor
                        .abort_prepared_task_cli_runtime_turn(
                            prepared,
                            "task run became terminal before its CLI runtime turn was activated"
                                .to_owned(),
                        )
                        .await;
                    record_task_run_turn_failure(
                        &handle,
                        &child_runtime.task_run_turn,
                        TaskRunTurnStatus::Blocked,
                        Some(task_error(
                            "task_cli_runtime_start_superseded",
                            "task run became terminal before its CLI runtime turn was activated",
                            TaskErrorClass::Cancelled,
                            Some(run.id.clone()),
                        )),
                        now_timestamp_secs(),
                    )
                    .await
                    .context("failed to close superseded hidden task CLI runtime turn")?;
                    return Ok(TaskExecutorStartOutcome::Started);
                }

                if let Err(error) =
                    verify_durable_task_child_admission(processor, &child_runtime, &execution).await
                {
                    let abort_reason = "task_cli_runtime_child_admission_invalid".to_owned();
                    processor
                        .abort_prepared_task_cli_runtime_turn(prepared, abort_reason)
                        .await;
                    return Err(error).context("failed to verify task CLI runtime child admission");
                }

                let started_at = now_timestamp_secs();
                if let Err(error) = handle.mark_started(started_at).await {
                    let abort_reason = "task_cli_runtime_run_start_failed".to_owned();
                    processor
                        .abort_prepared_task_cli_runtime_turn(prepared, abort_reason)
                        .await;
                    return Err(error).context("failed to mark task CLI runtime run started");
                }
                if let Err(error) = processor
                    .crud_store
                    .mark_execution_running(
                        execution.id.as_str(),
                        started_at,
                        Some(started_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
                    )
                    .await
                {
                    let abort_reason = "task_cli_runtime_execution_start_failed".to_owned();
                    processor
                        .abort_prepared_task_cli_runtime_turn(prepared, abort_reason)
                        .await;
                    return Err(error).context("failed to mark task CLI runtime execution running");
                }

                // Activation publishes the canonical user-message lifecycle before
                // it starts the native runtime. Keep that projection on the same
                // fresh-task boundary as preparation instead of rebuilding the
                // scheduler -> executor -> SeaORM poll chain here.
                let activation_processor = processor.clone();
                let activation_result = message_fresh_task(async move {
                    activation_processor
                        .activate_prepared_task_cli_runtime_turn(prepared)
                        .await
                })
                .await;
                if matches!(activation_result, Ok(Ok(()))) {
                    let action_binding = processor
                        .prepare_agent_action_binding(
                            child_turn_id.clone(),
                            crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                                action_adapter,
                                action_options,
                                action_capabilities,
                            ),
                        )
                        .await?;
                    processor
                        .register_agent_action_binding(child_turn_id.clone(), action_binding)
                        .await;
                    spawn_execution_heartbeat(
                        processor,
                        execution.id,
                        child_thread_id,
                        child_turn_id,
                        run.id.clone(),
                        task_agent_liveness_timeouts(task),
                    );
                }
                return Ok(TaskExecutorStartOutcome::Started);
            })
            .await;
        }
        let materialize_actor = non_cli_action_author.actor.clone();
        // LLDB shows that polling this workflow underneath start_run,
        // start_or_recover_run and start_new_child_turn exhausts the Tokio
        // worker's native stack before the first post-materialization query.
        // Own every borrowed input and schedule the complete workflow as one
        // task so its poll stack starts at the runtime boundary.
        let workflow_processor = Arc::clone(processor);
        let workflow_context = (*context).clone();
        let workflow_task = (*task).clone();
        let workflow_run = (*run).clone();
        let workflow_parent = (*parent).clone();
        message_fresh_task(async move {
            let processor = &workflow_processor;
            let context = &workflow_context;
            let task = &workflow_task;
            let run = &workflow_run;
            let parent = &workflow_parent;
            let turn_outcome = processor
                .thread_manager
                .agent_turn_start_with_permission_profile(
                    TurnStartParams {
                        input: child_input,
                        model: Some(effective_model.model.clone()),
                        model_provider: Some(effective_model.model_provider.clone()),
                        mode: Some(child_mode),
                        ..turn_params
                    },
                    child_permission_profile,
                    non_cli_action_author,
                )
                .await
                .context("failed to create hidden task turn")?;

        if let Err(error) = processor
            .validate_turn_artifact_user_inputs(
                context.workspace_id.as_str(),
                parent.root_thread_id.as_str(),
                turn_outcome.materialization.input.as_slice(),
            )
            .await
        {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            return Err(error).context("failed to validate hidden task artifact input");
        }

        let turn_permission_profile = match processor
            .materialized_turn_permission_profile(&turn_outcome.materialization.turn)
        {
            Ok(permission_profile) => permission_profile,
            Err(error) => {
                processor
                    .thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                return Err(error).context("failed to resolve hidden task permission profile");
            }
        };
        let profile_selected_audit = processor.turn_profile_selected_audit_event_for_turn(
            context.workspace_id.as_str(),
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            turn_permission_profile.clone(),
        );
        let child_authority_json = child_authorization_context
            .to_persisted_json()
            .context("failed to encode hidden task authority envelope")?;
        let child_turn_admission = match child_authorization_context
            .durable_turn_admission_after_revalidation(
                child_thread_id.as_str(),
                child_turn_id.as_str(),
                child_execution_backend.as_ref(),
                &child_authorization_revalidation,
            )
        {
            Ok(admission) => admission,
            Err(error) => {
                processor
                    .thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                return Err(error).context("failed to reserve hidden task execution quota");
            }
        };
        // Turn-start projection is one atomic CRUD operation, but its SeaORM
        // future is intentionally large. Poll it from a fresh task so the
        // scheduler -> TaskExecutor -> child-admission frames are not stacked
        // underneath the projector. Abort-on-drop preserves cancellation and
        // lets the transaction roll back if its caller disappears.
        let materialize_store = processor.crud_store.clone();
        let materialize_thread = turn_outcome.materialization.thread.clone();
        let materialize_sandbox_mode = turn_outcome.materialization.sandbox_mode;
        let materialize_turn = turn_outcome.materialization.turn.clone();
        let materialize_input = turn_outcome.materialization.input.clone();
        let materialize_reasoning_effort = reasoning_effort.clone();
        let materialize_execution = super::turn_handlers::new_turn_execution(
            processor.turn_execution_owner_id.as_ref(),
            child_execution_backend.as_ref(),
            &turn_outcome.materialization,
        )?;
        let materialize_response = turn_response;
        let materialize_security_snapshot = child_security_snapshot.clone();
        let materialize_security_audits = processor.turn_security_audit_events_for_turn(
            context.workspace_id.as_str(),
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            &child_security_snapshot,
        );
        let materialize_result = message_fresh_task(async move {
            materialize_store
                .materialize_authorized_turn_start_with_reasoning_effort_and_permission_audit(
                    &materialize_thread,
                    materialize_sandbox_mode,
                    &materialize_turn,
                    &materialize_input,
                    materialize_reasoning_effort.as_deref(),
                    materialize_actor,
                    profile_selected_audit,
                    child_authority_json.as_str(),
                    None,
                    Some(child_turn_admission),
                    Some(materialize_execution),
                    &materialize_security_snapshot,
                    materialize_security_audits,
                    None,
                    Some(materialize_response),
                )
                .await
        })
        .await
        .map_err(|error| anyhow!("hidden task turn projection task failed: {error}"))?;
        if let Err(error) = materialize_result {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            return Err(error).context("failed to persist hidden task turn");
        }
        // The execution lease revalidator proves the child against durable
        // lineage. Turn, authority, security snapshot and response are already
        // one atomic write; link the runtime before registering its live lease.
        close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            handle
                .link_child_thread_with_runtime(
                    child_runtime.lineage.clone(),
                    binding,
                    child_runtime.task_run_turn.clone(),
                    now,
                )
                .await
                .context("failed to link hidden task runtime"),
        )
        .await?;
        close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            register_resolved_task_child_execution_lease(processor, child_turn_id.as_str())
            .await
            .context("failed to register hidden task execution lease"),
        )
        .await?;
        close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            verify_durable_task_child_admission(processor, &child_runtime, &execution)
                .await
                .context("failed to verify hidden task child admission"),
        )
        .await?;

        processor.ensure_hook_runtime_with_run_store().await;
        close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            processor
                .agent_manager
                .ensure_thread(child_thread_id.as_str(), context.workspace_id.as_str())
                .await
                .map_err(|error| anyhow!("failed to prepare child agent runtime: {error}")),
        )
        .await?;
        close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            processor
                .ensure_agent_listener_task(child_thread_id.as_str())
                .await,
        )
        .await?;

        let action_binding = processor
            .prepare_agent_action_binding(
                child_turn_id.clone(),
                crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                    action_adapter,
                    action_options,
                    action_capabilities,
                ),
            )
            .await?;
        processor
            .register_agent_action_binding(child_turn_id.clone(), action_binding)
            .await;

        let started_at = now_timestamp_secs();
        close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            handle
                .mark_started(started_at)
                .await
                .context("failed to mark hidden task run started"),
        )
        .await?;
        close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            processor
                .crud_store
                .mark_execution_running(
                    execution.id.as_str(),
                    started_at,
                    Some(started_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
                )
                .await
                .context("failed to mark task run execution running"),
        )
        .await?;
        let workspace_skill_policies = close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            load_workspace_skill_policies(processor, task.workspace_id.as_str())
                .await
                .context("failed to load hidden task workspace skill policies"),
        )
        .await?;
        let skill_catalog = close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            processor
                .validate_turn_skill_capabilities(
                    task.workspace_id.as_str(),
                    turn_outcome.materialization.capabilities.as_slice(),
                )
                .await
                .map_err(|message| anyhow!(message))
                .context("failed to validate hidden task skill capabilities"),
        )
        .await?;
        if let Some(normalized) = normalized_composer_capabilities.as_ref() {
            let capability_attachments = close_admitted_task_turn_on_error(
                processor,
                child_thread_id.as_str(),
                child_turn_id.as_str(),
                super::agent_runtime::user_message_attachments_from_capabilities_and_catalog(
                    normalized.presentation.as_slice(),
                    &skill_catalog,
                    &normalized.pack_names,
                )
                .context("failed to snapshot composer work capability presentation"),
            )
            .await?;
            close_admitted_task_turn_on_error(
                processor,
                child_thread_id.as_str(),
                child_turn_id.as_str(),
                processor
                    .emit_user_message_item_lifecycle(
                        task.workspace_id.as_str(),
                        child_thread_id.as_str(),
                        child_turn_id.as_str(),
                        turn_outcome.materialization.input.as_slice(),
                        capability_attachments.as_slice(),
                    )
                    .await
                    .context("failed to persist hidden task user message lifecycle"),
            )
            .await?;
        }
        let resolved_artifacts = close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            processor
                .resolve_provider_artifact_inputs(
                    task.workspace_id.as_str(),
                    turn_outcome.materialization.input.as_slice(),
                )
                .await
                .context("failed to resolve hidden task artifact input for provider"),
        )
        .await?;
        let runtime_environment = close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            processor
                .create_artifact_output_environment(
                    task.workspace_id.as_str(),
                    child_thread_id.as_str(),
                    child_turn_id.as_str(),
                )
                .await
                .context("failed to prepare hidden task artifact output directory"),
        )
        .await?
        .into_iter()
        .collect();
        let (hook_runtime_context, history) = close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            load_task_execution_conversation_scope(
                processor,
                task,
                run,
                parent,
                child_runtime.task_run_turn.kind,
                child_thread_id.as_str(),
                child_turn_id.as_str(),
                thread_outcome.started_notification.thread.model.as_str(),
                thread_outcome
                    .started_notification
                    .thread
                    .model_provider
                    .as_str(),
            )
            .await,
        )
        .await?;
        if let Err(error) = processor
            .persist_turn_runtime_snapshot_with_optional_agent_overlay(
                child_thread_id.as_str(),
                task.workspace_id.as_str(),
                child_turn_id.as_str(),
                child_mode,
                &hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                reasoning_effort.as_deref(),
                &workspace_skill_policies,
                turn_outcome.materialization.input.as_slice(),
                turn_outcome.materialization.capabilities.as_slice(),
                resolved_artifacts.as_slice(),
                &runtime_environment,
                history.as_slice(),
                &mut agent_skill_overlay,
            )
            .await
        {
            report_or_block_task_turn_failure(
                processor,
                child_thread_id,
                child_turn_id,
                TurnFailureRecoveryKind::TaskDispatch,
                format!("failed to persist child task turn runtime snapshot: {error:#}"),
            )
            .await;
            return Ok(TaskExecutorStartOutcome::Started);
        }
        let runtime_permission_profile = turn_permission_profile;
        if let Err(error) = processor
            .agent_manager
            .start_turn_with_hook_context_reasoning_permission_profile_security_snapshot_and_agent_skill_overlay(
                child_thread_id.as_str(),
                child_turn_id.as_str(),
                child_mode,
                hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                workspace_skill_policies,
                skill_catalog,
                agent_skill_overlay,
                turn_outcome.materialization.input,
                turn_outcome.materialization.capabilities,
                resolved_artifacts,
                runtime_environment,
                history,
                reasoning_effort.as_deref(),
                runtime_permission_profile,
                child_security_snapshot,
            )
            .await
        {
            report_or_block_task_turn_failure(
                processor,
                child_thread_id,
                child_turn_id,
                TurnFailureRecoveryKind::TaskDispatch,
                format!("failed to dispatch child task turn: {error}"),
            )
            .await;
            return Ok(TaskExecutorStartOutcome::Started);
        }
        if let Err(error) =
            commit_task_turn_execution_running(processor, child_thread_id.as_str(), child_turn_id.as_str()).await
        {
            report_or_block_task_turn_failure(
                processor,
                child_thread_id,
                child_turn_id,
                TurnFailureRecoveryKind::TaskDispatch,
                format!("failed to commit child task Turn execution ownership: {error:#}"),
            )
            .await;
            return Ok(TaskExecutorStartOutcome::Started);
        }
        spawn_execution_heartbeat(
            processor,
            execution.id,
            child_thread_id,
            child_turn_id,
            run.id.clone(),
            task_agent_liveness_timeouts(task),
        );

            Ok(TaskExecutorStartOutcome::Started)
        })
        .await
        .map_err(|error| anyhow!("hidden task turn workflow task failed: {error}"))?
    }

    pub(crate) async fn dispatch_revision_turn(
        self: &Arc<Self>,
        response: TaskReviseResponse,
    ) -> Result<TaskReviseResponse> {
        let processor = self.processor()?;
        let task_response =
            message_future(processor.crud_store.get_task(response.task.id.as_str()))
                .await?
                .ok_or_else(|| anyhow!("task `{}` not found", response.task.id))?;
        let run = task_response
            .runs
            .iter()
            .find(|run| run.id == response.run.id)
            .cloned()
            .ok_or_else(|| anyhow!("task run `{}` not found", response.run.id))?;
        let agent_spec = select_agent_spec(&task_response, run.id.as_str())
            .ok_or_else(|| anyhow!("agent task `{}` has no agent spec", task_response.task.id))?;
        let execution =
            match message_future(processor.crud_store.load_execution_for_run(run.id.as_str()))
                .await?
            {
                Some(execution) => execution,
                None => {
                    message_future(processor.crud_store.reserve_execution_for_run(
                        run.id.as_str(),
                        TaskExecutorKind::Agent,
                        now_timestamp_secs(),
                    ))
                    .await?
                }
            };
        let child_runtime = message_future(load_child_runtime_from_task_run_turn(
            &processor,
            response.task_run_turn.clone(),
        ))
        .await?;
        let handle = TaskExecutionHandle::new(
            processor.crud_store.clone(),
            processor.task_runtime.event_bus(),
            run.task_id.clone(),
            run.id.clone(),
        );
        let authority_processor = Arc::clone(&processor);
        let authority_task_response = task_response.clone();
        let root_still_has_authority = message_fresh_task(async move {
            Self::root_collaboration_still_has_task_authority(
                &authority_processor,
                &authority_task_response,
            )
            .await
        })
        .await
        .context("task revision authority revalidation task did not finish")??;
        if !root_still_has_authority {
            self.block_revision_dispatch_turn(
                &processor,
                child_runtime,
                handle,
                task_error(
                    "task_root_access_revoked",
                    "task continuation was blocked after root-thread access was revoked".to_owned(),
                    TaskErrorClass::Policy,
                    Some(run.id.clone()),
                ),
            )
            .await?;
            return message_future(task_revise_response_from_store(&processor, response)).await;
        }
        let lock_processor = Arc::clone(&processor);
        let lock_task = task_response.task.clone();
        let lock_run = run.clone();
        let lock_handle = handle.clone();
        let lock_outcome = message_fresh_task(async move {
            Self::acquire_write_locks_owned(lock_processor, lock_task, lock_run, lock_handle).await
        })
        .await
        .context("task revision write-lock phase did not finish")??;
        match lock_outcome {
            TaskExecutorStartOutcome::Started => {}
            TaskExecutorStartOutcome::Queued | TaskExecutorStartOutcome::Rejected => {
                return message_future(task_revise_response_from_store(&processor, response)).await;
            }
        }
        let revision_executor = Arc::clone(self);
        let revision_processor = Arc::clone(&processor);
        message_fresh_task(async move {
            revision_executor
                .dispatch_existing_revision_turn(
                    &revision_processor,
                    &task_response,
                    &run,
                    &agent_spec,
                    &execution,
                    child_runtime,
                    handle,
                )
                .await
        })
        .await
        .context("existing task revision turn task did not finish")??;
        message_future(task_revise_response_from_store(&processor, response)).await
    }

    async fn dispatch_existing_revision_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        run: &TaskRun,
        agent_spec: &TaskAgentSpec,
        execution: &TaskRunExecution,
        child_runtime: TaskRunChildRuntime,
        handle: TaskExecutionHandle,
    ) -> Result<()> {
        let task = &task_response.task;
        let child_thread_id = child_runtime.task_run_turn.thread_id.clone();
        let child_turn_id = child_runtime.task_run_turn.turn_id.clone();
        if let Some((_, turn)) = processor
            .crud_store
            .get_turn(child_thread_id.as_str(), child_turn_id.as_str())
            .await?
        {
            match turn.status {
                TurnStatus::Completed => {
                    self.complete_child_turn(processor, child_runtime, handle)
                        .await?;
                }
                TurnStatus::Failed | TurnStatus::Interrupted => {
                    let error_message = turn
                        .error
                        .unwrap_or_else(|| "revision child turn failed".to_owned());
                    let target_status =
                        task_run_turn_terminal_status_from_child_turn_status(turn.status)
                            .unwrap_or(TaskRunTurnStatus::Failed);
                    self.fail_child_turn(
                        child_runtime,
                        error_message.as_str(),
                        target_status,
                        handle,
                    )
                    .await?;
                }
                TurnStatus::Blocked => {
                    let error_message = turn
                        .error
                        .unwrap_or_else(|| "revision child turn blocked".to_owned());
                    self.block_child_turn(child_runtime, error_message.as_str(), handle)
                        .await?;
                }
                TurnStatus::InProgress => {
                    revalidate_existing_task_child_execution_authorization(
                        processor,
                        task,
                        child_thread_id.as_str(),
                        child_turn_id.as_str(),
                    )
                    .await
                    .context("revision task continuation authorization is no longer active")?;
                    let started_at = now_timestamp_secs();
                    processor
                        .crud_store
                        .mark_execution_running(
                            execution.id.as_str(),
                            started_at,
                            Some(started_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
                        )
                        .await
                        .context("failed to mark revision task run execution running")?;
                    processor
                        .ensure_agent_listener_task(child_thread_id.as_str())
                        .await?;
                    spawn_execution_heartbeat(
                        processor,
                        execution.id.clone(),
                        child_runtime.task_run_turn.thread_id.clone(),
                        child_runtime.task_run_turn.turn_id.clone(),
                        run.id.clone(),
                        task_agent_liveness_timeouts(task),
                    );
                }
            }
            return Ok(());
        }

        let Some(seed_thread) = processor
            .crud_store
            .get_thread_model(child_thread_id.as_str())
            .await?
        else {
            self.block_revision_dispatch_turn(
                processor,
                child_runtime,
                handle,
                task_error(
                    "child_thread_missing",
                    "revision child task thread is missing".to_owned(),
                    TaskErrorClass::Internal,
                    Some(run.id.clone()),
                ),
            )
            .await?;
            return Ok(());
        };
        let seed_sandbox_mode = processor
            .crud_store
            .get_thread_sandbox_mode(child_thread_id.as_str())
            .await?;
        let parent = resolve_parent_context(processor, task).await?;
        let action_policy_generation = processor.current_authorization_revision().await?;
        let (mut action_adapter, action_options, action_capabilities) =
            materialize_task_agent_action_binding_for_execution(
                processor,
                task,
                agent_spec,
                execution.id.as_str(),
                run.id.as_str(),
                parent.root_thread_id.as_str(),
                action_policy_generation,
                false,
            )
            .await
            .context("failed to bind revision agent action service")?;
        bind_existing_task_agent_graph(
            processor,
            run.id.as_str(),
            execution.id.as_str(),
            &mut action_adapter,
        )
        .await?;
        let action_facts = action_adapter.persistence_facts();
        let actor_contract = processor
            .crud_store
            .get_task_actor_contract(task.id.as_str())
            .await?
            .context("revision Task has no durable actor contract")?;
        let turn_settings = resolved_task_execution_turn_settings(
            processor,
            task,
            agent_spec,
            &action_facts,
            actor_contract.launch.as_ref(),
        )
        .await?;
        let effective_model = turn_settings.model.clone();
        let action_author = revision_turn_author(processor, &child_runtime.task_run_turn).await?;
        let action_actor = action_author.actor.clone();
        let turn_response =
            agent_turn_response_input(processor, child_turn_id.as_str(), execution.id.as_str())
                .await?;
        let thread_outcome = processor
            .thread_manager
            .system_thread_start_seeded(
                task.workspace_id.clone(),
                pioneer_protocol::ThreadStartParams {
                    thread_id: child_runtime.task_run_turn.thread_id.clone(),
                    workspace_id: task.workspace_id.clone(),
                    name: thread_name_from_task(task),
                    model: Some(effective_model.model.clone()),
                    model_provider: Some(effective_model.model_provider.clone()),
                    sandbox: seed_sandbox_mode,
                    mode: Some(ThreadMode::Agent),
                    origin_kind: Some(ThreadOriginKind::TaskRun),
                    sidebar_visibility: Some(ThreadSidebarVisibility::Hidden),
                    visibility: None,
                    agent_nickname: agent_spec.agent_nickname.clone(),
                    agent_role: agent_spec.agent_role.clone(),
                },
                Some(seed_thread),
                seed_sandbox_mode,
            )
            .await
            .context("failed to restore revision task thread")?;
        let child_permission_profile = turn_settings.permission_profile.clone();
        let frozen_conversation_scope = if task_attachment(task) == TaskAttachmentMode::Detached {
            Some(
                load_task_execution_conversation_scope(
                    processor,
                    task,
                    run,
                    &parent,
                    child_runtime.task_run_turn.kind,
                    child_runtime.task_run_turn.thread_id.as_str(),
                    child_runtime.task_run_turn.turn_id.as_str(),
                    thread_outcome.started_notification.thread.model.as_str(),
                    thread_outcome
                        .started_notification
                        .thread
                        .model_provider
                        .as_str(),
                )
                .await?,
            )
        } else {
            None
        };
        let input = materialize_child_task_input(
            materialize_child_task_prompt(
                processor,
                task_response,
                run,
                agent_spec,
                &parent,
                Some(&child_runtime.task_run_turn),
                &child_permission_profile,
                frozen_conversation_scope
                    .as_ref()
                    .map(|(_, history)| history.as_slice()),
            )
            .await?,
            agent_spec,
        );
        if let Some((runtime_id, runtime_kind)) = turn_settings.cli_runtime.clone() {
            let child_security_snapshot = resolve_task_child_cli_execution_security_snapshot(
                processor,
                task.workspace_id.as_str(),
                &parent,
                agent_spec,
                child_permission_profile.clone(),
                runtime_id.as_str(),
                runtime_kind,
                child_thread_id.as_str(),
                child_turn_id.as_str(),
            )
            .await
            .context("failed to resolve revision CLI execution security")?;
            let (child_authorization, _) = resolve_task_child_execution_authorization_context(
                processor,
                task,
                &parent,
                effective_model.model_provider.as_str(),
                effective_model.model.as_str(),
                Some(&turn_settings.execution_backend),
                turn_settings.capabilities.as_slice(),
                &child_security_snapshot.permission_profile,
                child_turn_id.as_str(),
            )
            .await
            .context("failed to resolve revision CLI execution authorization")?;
            let child_authorization_context = child_authorization.context;
            let child_authorization_revalidation = child_authorization.revalidation;
            let prepared = processor
                .prepare_task_cli_runtime_turn(
                    TurnStartParams {
                        agent_delegation_routes: Vec::new(),
                        thread_id: child_thread_id.clone(),
                        turn_id: child_turn_id.clone(),
                        input,
                        capabilities: turn_settings.capabilities.clone(),
                        model: Some(effective_model.model.clone()),
                        model_provider: Some(effective_model.model_provider.clone()),
                        sandbox_policy: None,
                        mode: Some(ThreadMode::Agent),
                        agent_launch: None,
                        reply_to_turn_id: None,
                        mentioned_principal_ids: Vec::new(),
                        execution_backend: Some(turn_settings.execution_backend.clone()),
                        reasoning: turn_settings.reasoning.clone(),
                        permission_profile: turn_settings.permission_selection.clone(),
                        cli_runtime_options: None,
                    },
                    runtime_id,
                    runtime_kind,
                    child_permission_profile,
                    child_security_snapshot,
                    child_authorization_context,
                    child_authorization_revalidation,
                    parent.parent_thread_id.clone(),
                    parent.parent_thread_id.clone(),
                    run.id.clone(),
                    execution.id.clone(),
                    frozen_conversation_scope
                        .as_ref()
                        .map(|(_, history)| history.clone())
                        .unwrap_or_default(),
                    action_author,
                    turn_response.clone(),
                )
                .await
                .context("failed to prepare revision CLI runtime turn")?;
            processor
                .activate_prepared_task_cli_runtime_turn(prepared)
                .await
                .context("failed to activate revision CLI runtime turn")?;
            let action_binding = processor
                .prepare_agent_action_binding(
                    child_turn_id.clone(),
                    crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                        action_adapter,
                        action_options,
                        action_capabilities,
                    ),
                )
                .await?;
            processor
                .register_agent_action_binding(child_turn_id.clone(), action_binding)
                .await;
            let started_at = now_timestamp_secs();
            processor
                .crud_store
                .mark_execution_running(
                    execution.id.as_str(),
                    started_at,
                    Some(started_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
                )
                .await
                .context("failed to mark revision CLI execution running")?;
            spawn_execution_heartbeat(
                processor,
                execution.id.clone(),
                child_runtime.task_run_turn.thread_id,
                child_runtime.task_run_turn.turn_id,
                run.id.clone(),
                task_agent_liveness_timeouts(task),
            );
            return Ok(());
        }
        if !matches!(
            turn_settings.execution_backend,
            AgentExecutionBackend::ApiProvider { .. }
        ) {
            bail!("revision Task backend has no installed runtime adapter");
        }
        let turn_outcome = match processor
            .thread_manager
            .agent_turn_start_with_permission_profile(
                TurnStartParams {
                    agent_delegation_routes: Vec::new(),
                    thread_id: child_runtime.task_run_turn.thread_id.clone(),
                    turn_id: child_runtime.task_run_turn.turn_id.clone(),
                    input,
                    capabilities: turn_settings.capabilities.clone(),
                    model: Some(effective_model.model),
                    model_provider: Some(effective_model.model_provider),
                    sandbox_policy: None,
                    mode: Some(ThreadMode::Agent),
                    agent_launch: None,
                    reply_to_turn_id: None,
                    mentioned_principal_ids: Vec::new(),
                    execution_backend: Some(turn_settings.execution_backend.clone()),
                    reasoning: turn_settings.reasoning.clone(),
                    permission_profile: turn_settings.permission_selection.clone(),
                    cli_runtime_options: None,
                },
                child_permission_profile,
                action_author,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) if format!("{error:#}").contains("already has a running turn") => {
                let started_at = now_timestamp_secs();
                processor
                    .crud_store
                    .mark_execution_running(
                        execution.id.as_str(),
                        started_at,
                        Some(started_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
                    )
                    .await
                    .context("failed to mark revision task run execution running")?;
                processor
                    .ensure_agent_listener_task(child_thread_id.as_str())
                    .await?;
                spawn_execution_heartbeat(
                    processor,
                    execution.id.clone(),
                    child_runtime.task_run_turn.thread_id,
                    child_runtime.task_run_turn.turn_id,
                    run.id.clone(),
                    task_agent_liveness_timeouts(task),
                );
                return Ok(());
            }
            Err(error) => {
                self.block_revision_dispatch_turn(
                    processor,
                    child_runtime,
                    handle,
                    task_error(
                        "revision_turn_start_failed",
                        format!("failed to create revision task turn: {error:#}"),
                        TaskErrorClass::Internal,
                        Some(run.id.clone()),
                    ),
                )
                .await?;
                return Ok(());
            }
        };
        if let Err(error) = processor
            .validate_turn_artifact_user_inputs(
                task.workspace_id.as_str(),
                parent.root_thread_id.as_str(),
                turn_outcome.materialization.input.as_slice(),
            )
            .await
        {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            self.block_revision_dispatch_turn(
                processor,
                child_runtime,
                handle,
                task_error(
                    "revision_artifact_input_invalid",
                    format!("failed to validate revision task artifact input: {error:#}"),
                    TaskErrorClass::Validation,
                    Some(run.id.clone()),
                ),
            )
            .await?;
            return Ok(());
        }
        let turn_permission_profile = match processor
            .materialized_turn_permission_profile(&turn_outcome.materialization.turn)
        {
            Ok(permission_profile) => permission_profile,
            Err(error) => {
                processor
                    .thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                self.block_revision_dispatch_turn(
                    processor,
                    child_runtime,
                    handle,
                    task_error(
                        "revision_permission_profile_missing",
                        format!("failed to resolve revision task permission profile: {error:#}"),
                        TaskErrorClass::Internal,
                        Some(run.id.clone()),
                    ),
                )
                .await?;
                return Ok(());
            }
        };
        let child_security_snapshot = match resolve_task_child_execution_security_snapshot(
            processor,
            task.workspace_id.as_str(),
            &parent,
            agent_spec,
            turn_permission_profile.clone(),
            thread_outcome
                .started_notification
                .thread
                .model_provider
                .as_str(),
            child_thread_id.as_str(),
            child_turn_id.as_str(),
        )
        .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                processor
                    .thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                self.block_revision_dispatch_turn(
                    processor,
                    child_runtime,
                    handle,
                    task_error(
                        "revision_execution_security_unavailable",
                        format!("failed to resolve revision task execution security: {error:#}"),
                        TaskErrorClass::Internal,
                        Some(run.id.clone()),
                    ),
                )
                .await?;
                return Ok(());
            }
        };
        let (child_authorization, mut agent_skill_overlay) =
            match resolve_task_child_execution_authorization_context(
                processor,
                task,
                &parent,
                turn_outcome.materialization.thread.model_provider.as_str(),
                turn_outcome.materialization.thread.model.as_str(),
                Some(&turn_settings.execution_backend),
                turn_outcome.materialization.capabilities.as_slice(),
                &child_security_snapshot.permission_profile,
                child_turn_id.as_str(),
            )
            .await
            {
                Ok(resolved) => resolved,
                Err(error) => {
                    processor
                        .thread_manager
                        .rollback_turn_start(turn_outcome.rollback_context)
                        .await;
                    self.block_revision_dispatch_turn(
                        processor,
                        child_runtime,
                        handle,
                        task_error(
                            "revision_execution_authorization_unavailable",
                            format!(
                                "failed to resolve revision task execution authorization: {error:#}"
                            ),
                            TaskErrorClass::Policy,
                            Some(run.id.clone()),
                        ),
                    )
                    .await?;
                    return Ok(());
                }
            };
        let child_authorization_context = child_authorization.context;
        let child_authorization_revalidation = child_authorization.revalidation;
        let profile_selected_audit = processor.turn_profile_selected_audit_event_for_turn(
            task.workspace_id.as_str(),
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            turn_permission_profile.clone(),
        );
        let child_authority_json = match child_authorization_context.to_persisted_json() {
            Ok(encoded) => encoded,
            Err(error) => {
                processor
                    .thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                self.block_revision_dispatch_turn(
                    processor,
                    child_runtime,
                    handle,
                    task_error(
                        "revision_authority_envelope_invalid",
                        format!("failed to encode revision task authority: {error:#}"),
                        TaskErrorClass::Internal,
                        Some(run.id.clone()),
                    ),
                )
                .await?;
                return Ok(());
            }
        };
        let child_turn_admission = match child_authorization_context
            .durable_turn_admission_after_revalidation(
                child_thread_id.as_str(),
                child_turn_id.as_str(),
                Some(&turn_settings.execution_backend),
                &child_authorization_revalidation,
            ) {
            Ok(admission) => admission,
            Err(error) => {
                processor
                    .thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                self.block_revision_dispatch_turn(
                    processor,
                    child_runtime,
                    handle,
                    task_error(
                        "revision_execution_quota_unavailable",
                        format!("failed to reserve revision task execution quota: {error:#}"),
                        TaskErrorClass::Policy,
                        Some(run.id.clone()),
                    ),
                )
                .await?;
                return Ok(());
            }
        };
        let materialization_store = processor.crud_store.clone();
        let materialization_thread = turn_outcome.materialization.thread.clone();
        let materialization_sandbox_mode = turn_outcome.materialization.sandbox_mode;
        let materialization_turn = turn_outcome.materialization.turn.clone();
        let materialization_input = turn_outcome.materialization.input.clone();
        let materialization_reasoning_effort = turn_settings
            .reasoning
            .as_ref()
            .map(|reasoning| reasoning.effort.clone());
        let materialization_execution = super::turn_handlers::new_turn_execution(
            processor.turn_execution_owner_id.as_ref(),
            Some(&turn_settings.execution_backend),
            &turn_outcome.materialization,
        )?;
        let materialization_security_snapshot = child_security_snapshot.clone();
        let materialization_security_audit_events = processor.turn_security_audit_events_for_turn(
            task.workspace_id.as_str(),
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            &child_security_snapshot,
        );
        let materialization_result = message_fresh_task(async move {
            materialization_store
                .materialize_authorized_turn_start_with_reasoning_effort_and_permission_audit(
                    &materialization_thread,
                    materialization_sandbox_mode,
                    &materialization_turn,
                    &materialization_input,
                    materialization_reasoning_effort.as_deref(),
                    action_actor,
                    profile_selected_audit,
                    child_authority_json.as_str(),
                    None,
                    Some(child_turn_admission),
                    Some(materialization_execution),
                    &materialization_security_snapshot,
                    materialization_security_audit_events,
                    None,
                    Some(turn_response),
                )
                .await
        })
        .await
        .context("task revision turn materialization task did not finish")?;
        if let Err(error) = materialization_result {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            self.block_revision_dispatch_turn(
                processor,
                child_runtime,
                handle,
                task_error(
                    "revision_turn_persist_failed",
                    format!("failed to persist revision task turn: {error:#}"),
                    TaskErrorClass::Internal,
                    Some(run.id.clone()),
                ),
            )
            .await?;
            return Ok(());
        }
        if let Err(error) =
            register_resolved_task_child_execution_lease(processor, child_turn_id.as_str()).await
        {
            warn!(
                thread_id = child_thread_id,
                turn_id = child_turn_id,
                error = %format!("{error:#}"),
                failure_class = "revision_execution_security_persist_failed",
                "failed to persist revision task execution security"
            );
            let reason = "revision_execution_security_persist_failed".to_owned();
            if !processor
                .mark_turn_blocked(
                    child_thread_id.clone(),
                    child_turn_id.clone(),
                    reason.clone(),
                )
                .await
            {
                warn!(
                    thread_id = child_thread_id,
                    turn_id = child_turn_id,
                    failure_class = "revision_execution_security_close_failed",
                    "failed to durably close revision task turn after admission failure"
                );
            }
            self.block_revision_dispatch_turn(
                processor,
                child_runtime,
                handle,
                task_error(
                    "revision_execution_security_persist_failed",
                    reason,
                    TaskErrorClass::Internal,
                    Some(run.id.clone()),
                ),
            )
            .await?;
            return Ok(());
        }
        processor.ensure_hook_runtime_with_run_store().await;
        close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            processor
                .agent_manager
                .ensure_thread(child_thread_id.as_str(), task.workspace_id.as_str())
                .await
                .map_err(|error| {
                    anyhow!("failed to prepare revision child agent runtime: {error}")
                }),
        )
        .await?;
        close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            processor
                .ensure_agent_listener_task(child_thread_id.as_str())
                .await,
        )
        .await?;
        let action_binding = processor
            .prepare_agent_action_binding(
                child_turn_id.to_owned(),
                crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                    action_adapter,
                    action_options,
                    action_capabilities,
                ),
            )
            .await?;
        processor
            .register_agent_action_binding(child_turn_id.to_owned(), action_binding)
            .await;

        let started_at = now_timestamp_secs();
        close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            processor
                .crud_store
                .mark_execution_running(
                    execution.id.as_str(),
                    started_at,
                    Some(started_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
                )
                .await
                .context("failed to mark revision task run execution running"),
        )
        .await?;
        let workspace_skill_policies = close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            load_workspace_skill_policies(processor, task.workspace_id.as_str())
                .await
                .context("failed to load revision task workspace skill policies"),
        )
        .await?;
        let skill_catalog = close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            processor
                .validate_turn_skill_capabilities(
                    task.workspace_id.as_str(),
                    turn_outcome.materialization.capabilities.as_slice(),
                )
                .await
                .map_err(|message| anyhow!(message))
                .context("failed to validate revision task skill capabilities"),
        )
        .await?;
        let resolved_artifacts = close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            processor
                .resolve_provider_artifact_inputs(
                    task.workspace_id.as_str(),
                    turn_outcome.materialization.input.as_slice(),
                )
                .await
                .context("failed to resolve revision task artifact input for provider"),
        )
        .await?;
        let runtime_environment = close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            processor
                .create_artifact_output_environment(
                    task.workspace_id.as_str(),
                    child_thread_id.as_str(),
                    child_turn_id.as_str(),
                )
                .await
                .context("failed to prepare revision task artifact output directory"),
        )
        .await?
        .into_iter()
        .collect();
        let execution_checkpoint_context = close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            load_execution_checkpoint_context_for_turn(processor, child_turn_id.as_str()).await,
        )
        .await?;
        let (hook_runtime_context, history) = close_admitted_task_turn_on_error(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            load_task_execution_conversation_scope(
                processor,
                task,
                run,
                &parent,
                child_runtime.task_run_turn.kind,
                child_thread_id.as_str(),
                child_turn_id.as_str(),
                thread_outcome.started_notification.thread.model.as_str(),
                thread_outcome
                    .started_notification
                    .thread
                    .model_provider
                    .as_str(),
            )
            .await,
        )
        .await?;
        if let Err(error) = processor
            .persist_turn_runtime_snapshot_with_optional_agent_overlay(
                child_thread_id.as_str(),
                task.workspace_id.as_str(),
                child_turn_id.as_str(),
                ThreadMode::Agent,
                &hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                None,
                &workspace_skill_policies,
                turn_outcome.materialization.input.as_slice(),
                turn_outcome.materialization.capabilities.as_slice(),
                resolved_artifacts.as_slice(),
                &runtime_environment,
                history.as_slice(),
                &mut agent_skill_overlay,
            )
            .await
        {
            report_or_block_task_turn_failure(
                processor,
                child_thread_id.clone(),
                child_turn_id.clone(),
                TurnFailureRecoveryKind::TaskDispatch,
                format!("failed to persist revision task turn runtime snapshot: {error:#}"),
            )
            .await;
            self.block_revision_dispatch_turn(
                processor,
                child_runtime,
                handle,
                task_error(
                    "revision_dispatch_snapshot_failed",
                    format!("failed to persist revision task turn runtime snapshot: {error:#}"),
                    TaskErrorClass::Internal,
                    Some(run.id.clone()),
                ),
            )
            .await?;
            return Ok(());
        }
        let runtime_permission_profile = turn_permission_profile;
        if let Err(error) = processor
            .agent_manager
            .start_turn_with_hook_context_and_execution_checkpoint_permission_profile_security_snapshot_and_agent_skill_overlay(
                child_thread_id.as_str(),
                child_turn_id.as_str(),
                ThreadMode::Agent,
                hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                workspace_skill_policies,
                skill_catalog,
                agent_skill_overlay,
                turn_outcome.materialization.input,
                turn_outcome.materialization.capabilities,
                resolved_artifacts,
                runtime_environment,
                history,
                execution_checkpoint_context,
                runtime_permission_profile,
                child_security_snapshot,
            )
            .await
        {
            report_or_block_task_turn_failure(
                processor,
                child_thread_id.clone(),
                child_turn_id.clone(),
                TurnFailureRecoveryKind::TaskDispatch,
                format!("failed to dispatch revision task turn: {error}"),
            )
            .await;
            self.block_revision_dispatch_turn(
                processor,
                child_runtime,
                handle,
                task_error(
                    "revision_dispatch_failed",
                    format!("failed to dispatch revision task turn: {error:#}"),
                    TaskErrorClass::Internal,
                    Some(run.id.clone()),
                ),
            )
            .await?;
            return Ok(());
        }
        if let Err(error) = commit_task_turn_execution_running(
            processor,
            child_thread_id.as_str(),
            child_turn_id.as_str(),
        )
        .await
        {
            report_or_block_task_turn_failure(
                processor,
                child_thread_id.clone(),
                child_turn_id.clone(),
                TurnFailureRecoveryKind::TaskDispatch,
                format!("failed to commit revision task Turn execution ownership: {error:#}"),
            )
            .await;
            self.block_revision_dispatch_turn(
                processor,
                child_runtime,
                handle,
                task_error(
                    "revision_dispatch_ownership_failed",
                    format!("failed to commit revision task Turn execution ownership: {error:#}"),
                    TaskErrorClass::Internal,
                    Some(run.id.clone()),
                ),
            )
            .await?;
            return Ok(());
        }
        spawn_execution_heartbeat(
            processor,
            execution.id.clone(),
            child_runtime.task_run_turn.thread_id,
            child_runtime.task_run_turn.turn_id,
            run.id.clone(),
            task_agent_liveness_timeouts(task),
        );
        Ok(())
    }

    async fn recover_existing_child_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        run: &TaskRun,
        agent_spec: &TaskAgentSpec,
        execution: &TaskRunExecution,
        child_runtime: TaskRunChildRuntime,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        let Some((_, turn)) = processor
            .crud_store
            .get_turn(
                child_runtime.task_run_turn.thread_id.as_str(),
                child_runtime.task_run_turn.turn_id.as_str(),
            )
            .await?
        else {
            let now = now_timestamp_secs();
            handle
                .fail_run(
                    Some(task_error(
                        "child_turn_missing",
                        "child task turn is missing during recovery".to_owned(),
                        TaskErrorClass::Internal,
                        Some(run.id.clone()),
                    )),
                    now,
                )
                .await?;
            return Ok(TaskExecutorStartOutcome::Started);
        };

        match turn.status {
            TurnStatus::Completed => {
                self.complete_child_turn(processor, child_runtime, handle)
                    .await?;
                Ok(TaskExecutorStartOutcome::Started)
            }
            TurnStatus::Failed | TurnStatus::Interrupted => {
                let error_message = turn.error.unwrap_or_else(|| "child turn failed".to_owned());
                let target_status =
                    task_run_turn_terminal_status_from_child_turn_status(turn.status)
                        .unwrap_or(TaskRunTurnStatus::Failed);
                self.fail_child_turn(child_runtime, error_message.as_str(), target_status, handle)
                    .await?;
                Ok(TaskExecutorStartOutcome::Started)
            }
            TurnStatus::Blocked => {
                let error_message = turn
                    .error
                    .unwrap_or_else(|| "child turn blocked".to_owned());
                self.block_child_turn(child_runtime, error_message.as_str(), handle)
                    .await?;
                Ok(TaskExecutorStartOutcome::Started)
            }
            TurnStatus::InProgress => {
                claim_task_turn_execution_for_recovery(
                    processor,
                    child_runtime.task_run_turn.turn_id.as_str(),
                )
                .await
                .context("failed to claim restored task Turn execution ownership")?;
                let child_authorization_context =
                    revalidate_existing_task_child_execution_authorization(
                        processor,
                        &task_response.task,
                        child_runtime.task_run_turn.thread_id.as_str(),
                        child_runtime.task_run_turn.turn_id.as_str(),
                    )
                    .await
                    .context("task child continuation authorization is no longer active")?;
                let child_authorization_fingerprint = child_authorization_context
                    .authorization_fingerprint()
                    .context("failed to fingerprint restored task child authorization")?;
                let child_turn_id = child_runtime.task_run_turn.turn_id.as_str();
                if let Some(binding) = processor
                    .crud_store
                    .get_cli_runtime_turn_binding(child_turn_id)
                    .await?
                {
                    let parent = resolve_parent_context(processor, &task_response.task).await?;
                    if binding.thread_id != child_runtime.task_run_turn.thread_id
                        || binding.continuation_thread_id != parent.parent_thread_id
                    {
                        let message =
                            "native CLI runtime binding ownership is invalid during task recovery";
                        report_or_block_task_turn_failure(
                            processor,
                            child_runtime.task_run_turn.thread_id.clone(),
                            child_runtime.task_run_turn.turn_id.clone(),
                            TurnFailureRecoveryKind::TaskDispatch,
                            message.to_owned(),
                        )
                        .await;
                        self.fail_child_turn(
                            child_runtime,
                            message,
                            TaskRunTurnStatus::Failed,
                            handle,
                        )
                        .await?;
                        return Ok(TaskExecutorStartOutcome::Started);
                    }
                    let runtime_kind = match binding.runtime_kind.as_str() {
                        "codex" => CLIAgentRuntimeKind::Codex,
                        "claude" => CLIAgentRuntimeKind::Claude,
                        _ => {
                            return Err(anyhow!(
                                "unknown CLI runtime kind `{}` during task recovery",
                                binding.runtime_kind
                            ));
                        }
                    };
                    let action_policy_generation =
                        processor.current_authorization_revision().await?;
                    let (mut action_adapter, action_options, action_capabilities) =
                        materialize_task_agent_action_binding_for_execution(
                            processor,
                            &task_response.task,
                            agent_spec,
                            execution.id.as_str(),
                            run.id.as_str(),
                            parent.root_thread_id.as_str(),
                            action_policy_generation,
                            false,
                        )
                        .await
                        .context("failed to restore child agent action binding")?;
                    let action_facts = action_adapter.persistence_facts();
                    let actor_contract = processor
                        .crud_store
                        .get_task_actor_contract(task_response.task.id.as_str())
                        .await?
                        .context("recovered CLI Task has no durable actor contract")?;
                    let turn_settings = resolved_task_execution_turn_settings(
                        processor,
                        &task_response.task,
                        agent_spec,
                        &action_facts,
                        actor_contract.launch.as_ref(),
                    )
                    .await?;
                    if turn_settings.cli_runtime.as_ref()
                        != Some(&(binding.runtime_id.clone(), runtime_kind))
                    {
                        bail!("persisted Task CLI binding differs from its pinned profile");
                    }
                    let graph = persist_task_agent_execution_graph(
                        processor,
                        task_response,
                        run,
                        agent_spec,
                        &parent,
                        &action_facts,
                        child_authorization_fingerprint.as_str(),
                    )
                    .await
                    .context("failed to restore agent domain task execution graph")?;
                    action_adapter
                        .bind_persisted_work_graph_root(graph.root_execution_id.as_str())
                        .map_err(|error| {
                            anyhow!("failed to restore persisted task work graph: {error:?}")
                        })?;
                    if graph.queued {
                        return Ok(TaskExecutorStartOutcome::Queued);
                    }
                    let action_binding = processor
                        .prepare_agent_action_binding(
                            child_runtime.task_run_turn.turn_id.clone(),
                            crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                                action_adapter,
                                action_options,
                                action_capabilities,
                            ),
                        )
                        .await?;
                    processor
                        .register_agent_action_binding(
                            child_runtime.task_run_turn.turn_id.clone(),
                            action_binding,
                        )
                        .await;
                    spawn_execution_heartbeat(
                        processor,
                        execution.id.clone(),
                        child_runtime.task_run_turn.thread_id,
                        child_runtime.task_run_turn.turn_id,
                        run.id.clone(),
                        task_agent_liveness_timeouts(&task_response.task),
                    );
                    return Ok(TaskExecutorStartOutcome::Started);
                }
                let parent = resolve_parent_context(processor, &task_response.task).await?;
                let action_policy_generation = processor.current_authorization_revision().await?;
                let (mut action_adapter, action_options, action_capabilities) =
                    materialize_task_agent_action_binding_for_execution(
                        processor,
                        &task_response.task,
                        agent_spec,
                        execution.id.as_str(),
                        run.id.as_str(),
                        parent.root_thread_id.as_str(),
                        action_policy_generation,
                        false,
                    )
                    .await
                    .context("failed to restore child agent action binding")?;
                let action_facts = action_adapter.persistence_facts();
                let graph = persist_task_agent_execution_graph(
                    processor,
                    task_response,
                    run,
                    agent_spec,
                    &parent,
                    &action_facts,
                    child_authorization_fingerprint.as_str(),
                )
                .await
                .context("failed to restore agent domain task execution graph")?;
                action_adapter
                    .bind_persisted_work_graph_root(graph.root_execution_id.as_str())
                    .map_err(|error| {
                        anyhow!("failed to restore persisted task work graph: {error:?}")
                    })?;
                if graph.queued {
                    return Ok(TaskExecutorStartOutcome::Queued);
                }
                let action_binding = processor
                    .prepare_agent_action_binding(
                        child_runtime.task_run_turn.turn_id.clone(),
                        crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                            action_adapter,
                            action_options,
                            action_capabilities,
                        ),
                    )
                    .await?;
                processor
                    .register_agent_action_binding(
                        child_runtime.task_run_turn.turn_id.clone(),
                        action_binding,
                    )
                    .await;
                self.restart_in_progress_child_turn(
                    processor,
                    task_response,
                    run,
                    agent_spec,
                    execution,
                    &child_runtime,
                    None,
                    handle,
                )
                .await
            }
        }
    }

    async fn restart_in_progress_child_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        run: &TaskRun,
        agent_spec: &TaskAgentSpec,
        execution: &TaskRunExecution,
        child_runtime: &TaskRunChildRuntime,
        launch_permission_profile: Option<&TurnPermissionProfileSnapshot>,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        let task = &task_response.task;
        let child_thread_id = child_runtime.task_run_turn.thread_id.as_str();
        let child_turn_id = child_runtime.task_run_turn.turn_id.as_str();
        match self
            .acquire_write_locks(processor, task, run, handle.clone())
            .await?
        {
            TaskExecutorStartOutcome::Started => {}
            outcome => return Ok(outcome),
        }
        let Some(seed_thread) = processor
            .crud_store
            .get_thread_model(child_thread_id)
            .await?
        else {
            let now = now_timestamp_secs();
            handle
                .fail_run(
                    Some(task_error(
                        "child_thread_missing",
                        "child task thread is missing during recovery".to_owned(),
                        TaskErrorClass::Internal,
                        Some(run.id.clone()),
                    )),
                    now,
                )
                .await?;
            return Ok(TaskExecutorStartOutcome::Started);
        };
        let seed_sandbox_mode = processor
            .crud_store
            .get_thread_sandbox_mode(child_thread_id)
            .await?;
        let parent = resolve_parent_context(processor, task).await?;
        let action_policy_generation = processor.current_authorization_revision().await?;
        let (mut action_adapter, action_options, action_capabilities) =
            materialize_task_agent_action_binding_for_execution(
                processor,
                task,
                agent_spec,
                execution.id.as_str(),
                run.id.as_str(),
                parent.root_thread_id.as_str(),
                action_policy_generation,
                false,
            )
            .await
            .context("failed to bind recovery agent action service")?;
        bind_existing_task_agent_graph(
            processor,
            run.id.as_str(),
            execution.id.as_str(),
            &mut action_adapter,
        )
        .await?;
        let action_facts = action_adapter.persistence_facts();
        let actor_contract = processor
            .crud_store
            .get_task_actor_contract(task.id.as_str())
            .await?
            .context("recovered Task has no durable actor contract")?;
        let turn_settings = resolved_task_execution_turn_settings(
            processor,
            task,
            agent_spec,
            &action_facts,
            actor_contract.launch.as_ref(),
        )
        .await?;
        if !matches!(
            turn_settings.execution_backend,
            AgentExecutionBackend::ApiProvider { .. }
        ) {
            bail!("non-API Task execution entered API recovery");
        }
        let effective_model = turn_settings.model.clone();
        processor
            .thread_manager
            .system_thread_restore_persisted(seed_thread, seed_sandbox_mode)
            .await
            .context("failed to restore hidden task thread")?;
        let restored_thread = processor
            .thread_manager
            .thread_get(child_thread_id)
            .await
            .context("restored hidden task thread is not loaded")?;
        if let Some(launch_permission_profile) = launch_permission_profile
            && launch_permission_profile.mode != turn_settings.permission_profile.mode
        {
            bail!("recovered Task permission profile differs from its pinned launch");
        }
        let persisted_input = processor
            .crud_store
            .get_turn_inputs(child_turn_id)
            .await
            .context("failed to load restored task turn input")?;
        let turn_start_params = TurnStartParams {
            agent_delegation_routes: Vec::new(),
            thread_id: child_runtime.task_run_turn.thread_id.clone(),
            turn_id: child_runtime.task_run_turn.turn_id.clone(),
            input: persisted_input.clone(),
            capabilities: turn_settings.capabilities.clone(),
            model: Some(effective_model.model),
            model_provider: Some(effective_model.model_provider),
            sandbox_policy: None,
            mode: Some(ThreadMode::Agent),
            agent_launch: None,
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: Some(turn_settings.execution_backend.clone()),
            reasoning: turn_settings.reasoning.clone(),
            permission_profile: turn_settings.permission_selection.clone(),
            cli_runtime_options: None,
        };
        let turn_outcome = processor
            .thread_manager
            .rehydrate_committed_agent_turn(&turn_start_params, persisted_input)
            .await
            .context("failed to rehydrate hidden task turn")?;

        processor
            .validate_turn_artifact_user_inputs(
                task.workspace_id.as_str(),
                parent.root_thread_id.as_str(),
                turn_outcome.materialization.input.as_slice(),
            )
            .await
            .context("failed to validate restored task artifact input")?;
        close_admitted_task_turn_on_error(
            processor,
            child_thread_id,
            child_turn_id,
            register_resolved_task_child_execution_lease(processor, child_turn_id)
                .await
                .context("failed to register restored task execution lease"),
        )
        .await?;

        processor.ensure_hook_runtime_with_run_store().await;
        processor
            .agent_manager
            .ensure_thread(child_thread_id, task.workspace_id.as_str())
            .await
            .map_err(|error| anyhow!("failed to restore child agent runtime: {error}"))?;
        processor
            .ensure_agent_listener_task(child_thread_id)
            .await?;
        let action_binding = processor
            .prepare_agent_action_binding(
                child_turn_id.to_owned(),
                crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                    action_adapter,
                    action_options,
                    action_capabilities,
                ),
            )
            .await?;
        processor
            .register_agent_action_binding(child_turn_id.to_owned(), action_binding)
            .await;

        if run.status != TaskRunStatus::Running
            || execution.status != TaskRunExecutionStatus::Running
        {
            let started_at = now_timestamp_secs();
            handle.mark_started(started_at).await?;
            processor
                .crud_store
                .mark_execution_running(
                    execution.id.as_str(),
                    started_at,
                    Some(started_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
                )
                .await
                .context("failed to mark restored task run execution running")?;
        }
        let workspace_skill_policies =
            load_workspace_skill_policies(processor, task.workspace_id.as_str())
                .await
                .context("failed to load restored task workspace skill policies")?;
        let skill_catalog = processor
            .validate_turn_skill_capabilities(
                task.workspace_id.as_str(),
                turn_outcome.materialization.capabilities.as_slice(),
            )
            .await
            .map_err(|message| anyhow!(message))
            .context("failed to validate restored task skill capabilities")?;
        let resolved_artifacts = processor
            .resolve_provider_artifact_inputs(
                task.workspace_id.as_str(),
                turn_outcome.materialization.input.as_slice(),
            )
            .await
            .context("failed to resolve restored task artifact input for provider")?;
        let runtime_environment = processor
            .create_artifact_output_environment(
                task.workspace_id.as_str(),
                child_thread_id,
                child_turn_id,
            )
            .await
            .context("failed to prepare restored task artifact output directory")?
            .into_iter()
            .collect();
        let execution_checkpoint_context =
            load_execution_checkpoint_context_for_turn(processor, child_turn_id).await?;
        let runtime_snapshot = processor
            .crud_store
            .get_turn_runtime_snapshot(child_turn_id)
            .await
            .context("failed to load restored task turn runtime snapshot")?
            .context("restored task turn is missing its authoritative runtime snapshot")?;
        if runtime_snapshot.thread_id != child_thread_id
            || runtime_snapshot.workspace_id != task.workspace_id
            || runtime_snapshot.model != restored_thread.model
            || runtime_snapshot.provider_name != restored_thread.model_provider
        {
            bail!("restored task turn runtime identity does not match its authoritative snapshot");
        }
        let (hook_runtime_context, history) =
            crate::turn_runtime_snapshot::restored_conversation_scope_from_snapshot(
                &runtime_snapshot,
            )
            .context("failed to restore frozen Task conversation scope")?;
        let agent_skill_overlay =
            crate::turn_runtime_snapshot::restore_agent_skill_overlay_from_snapshot(
                processor.crud_store.as_ref(),
                task.workspace_id.as_str(),
                &runtime_snapshot,
            )
            .await
            .context("failed to restore exact pinned Agent skills for task turn")?;
        let runtime_permission_profile = processor
            .materialized_turn_permission_profile(&turn_outcome.materialization.turn)
            .context("failed to resolve restored task permission profile")?;
        let runtime_security_snapshot =
            load_required_task_child_execution_security_snapshot(processor, child_turn_id).await?;
        processor
            .agent_manager
            .start_turn_with_hook_context_and_execution_checkpoint_permission_profile_security_snapshot_and_agent_skill_overlay(
                child_thread_id,
                child_turn_id,
                ThreadMode::Agent,
                hook_runtime_context,
                &restored_thread.model,
                &restored_thread.model_provider,
                workspace_skill_policies,
                skill_catalog,
                agent_skill_overlay,
                turn_outcome.materialization.input,
                turn_outcome.materialization.capabilities,
                resolved_artifacts,
                runtime_environment,
                history,
                execution_checkpoint_context,
                runtime_permission_profile,
                runtime_security_snapshot,
            )
            .await
            .map_err(|error| anyhow!("failed to redispatch child task turn: {error}"))?;
        commit_task_turn_execution_running(processor, child_thread_id, child_turn_id)
            .await
            .context("failed to commit redispatched task Turn execution ownership")?;
        spawn_execution_heartbeat(
            processor,
            execution.id.clone(),
            child_runtime.task_run_turn.thread_id.clone(),
            child_runtime.task_run_turn.turn_id.clone(),
            run.id.clone(),
            task_agent_liveness_timeouts(task),
        );

        Ok(TaskExecutorStartOutcome::Started)
    }

    async fn acquire_write_locks(
        &self,
        processor: &Arc<MessageProcessor>,
        task: &Task,
        run: &TaskRun,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        let processor = Arc::clone(processor);
        let task = task.clone();
        let run = run.clone();
        message_fresh_task(Self::acquire_write_locks_owned(
            processor, task, run, handle,
        ))
        .await
        .context("task write-lock acquisition task did not finish")?
    }

    async fn acquire_write_locks_owned(
        processor: Arc<MessageProcessor>,
        task: Task,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        match processor
            .critical_task_service()
            .acquire_write_locks_for_run(run.id.as_str(), now_timestamp_secs())
            .await?
        {
            WriteLockDecision::NoLocksRequired | WriteLockDecision::Acquired(_) => {
                Ok(TaskExecutorStartOutcome::Started)
            }
            WriteLockDecision::Queued => Ok(TaskExecutorStartOutcome::Queued),
            WriteLockDecision::Rejected => {
                handle
                    .fail_run(
                        Some(task_error(
                            "write_lock_conflict",
                            "write scope conflicts with another active task run".to_owned(),
                            TaskErrorClass::Policy,
                            Some(run.id.clone()),
                        )),
                        now_timestamp_secs(),
                    )
                    .await?;
                let _ = task;
                Ok(TaskExecutorStartOutcome::Started)
            }
        }
    }

    pub(super) async fn reconcile_child_turn_completed(
        &self,
        thread_id: &str,
        turn_id: &str,
        origin: TaskChildReconciliationOrigin,
    ) -> Result<bool> {
        let Some(processor) = self.task_reconciliation_processor(origin)? else {
            // Processors used without the task runtime have no task aggregate
            // to reconcile; ordinary native terminal cleanup remains safe to
            // complete.
            return Ok(true);
        };
        let Some(child_runtime) =
            load_child_runtime_for_turn(&processor, thread_id, turn_id).await?
        else {
            // No task lineage means this is an ordinary native Turn.  Its
            // terminal cleanup is safe to proceed even though there is no
            // task aggregate to reconcile.
            return Ok(true);
        };
        // A late completion from the pre-resume actor must not reopen or
        // complete a task-run turn that has already been fenced terminally.
        // The aggregate resume transaction is the only path that puts this
        // durable turn back into `InProgress`.
        if child_runtime.task_run_turn.status == TaskRunTurnStatus::Blocked {
            // A late completion from the pre-resume actor must not erase the
            // blocked transcript/runtime capsule that an explicit resume
            // still needs.
            return Ok(false);
        }
        if !matches!(
            child_runtime.task_run_turn.status,
            TaskRunTurnStatus::InProgress | TaskRunTurnStatus::CandidateCreated
        ) {
            return Ok(true);
        }
        let Some(task_response) = processor
            .crud_store
            .get_task(child_runtime.task_run_turn.task_id.as_str())
            .await?
        else {
            bail!(
                "task `{}` disappeared while reconciling completed child turn",
                child_runtime.task_run_turn.task_id
            );
        };
        if task_response.task.status.is_terminal() {
            return Ok(true);
        }
        let handle = TaskExecutionHandle::new(
            processor.crud_store.clone(),
            processor.task_runtime.event_bus(),
            child_runtime.task_run_turn.task_id.clone(),
            child_runtime.task_run_turn.run_id.clone(),
        );
        self.complete_child_turn(&processor, child_runtime, handle)
            .await?;
        Ok(true)
    }

    /// Reopen and dispatch a blocked task child through the native
    /// `TaskExecutor` path.  Generic Turn recovery must not be allowed to
    /// start an actor while TaskRun/execution/locks remain terminal.
    pub(super) async fn resume_blocked_child_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        recovery_job_id: Option<&str>,
        now_unix: i64,
    ) -> Result<Option<TaskChildResumeOutcome>> {
        let processor = self.processor()?;
        let Some(task_run_turn) = processor
            .crud_store
            .get_task_run_turn_by_turn(thread_id, turn_id)
            .await?
        else {
            return Ok(None);
        };

        let outcome = processor
            .crud_store
            .resume_task_owned_turn(
                thread_id,
                turn_id,
                recovery_job_id,
                now_unix,
                processor.turn_execution_owner_id.as_ref(),
                now_unix.saturating_add(super::TURN_EXECUTION_OWNER_LEASE_SECONDS),
            )
            .await?;
        let Some(outcome) = outcome else {
            return Ok(Some(TaskChildResumeOutcome::Conflict {
                reason: "task-owned child lineage disappeared while resuming".to_owned(),
            }));
        };
        let outcome = match outcome {
            pioneer_crud::TaskOwnedTurnResumeOutcome::NotFound => TaskChildResumeOutcome::NotFound,
            pioneer_crud::TaskOwnedTurnResumeOutcome::MissingRuntimeSnapshot {
                recovery_job_id,
            } => TaskChildResumeOutcome::MissingRuntimeSnapshot { recovery_job_id },
            pioneer_crud::TaskOwnedTurnResumeOutcome::Conflict { reason } => {
                TaskChildResumeOutcome::Conflict { reason }
            }
            pioneer_crud::TaskOwnedTurnResumeOutcome::Resumed {
                recovery_job,
                task,
                run,
                execution,
                ..
            } => {
                let handle = TaskExecutionHandle::new(
                    processor.crud_store.clone(),
                    processor.task_runtime.event_bus(),
                    task.id.clone(),
                    run.id.clone(),
                );
                // Reacquire scopes before dispatch.  The CRUD transaction
                // already restored rows belonging to this run; this call also
                // covers tasks whose lock rows had been removed entirely.
                // Resume must not use the ordinary start helper here: that
                // helper intentionally terminalizes a rejected fresh run,
                // while this aggregate has already been reopened and must
                // remain resumably Blocked on a lock conflict.
                match processor
                    .task_runtime
                    .service()
                    .acquire_write_locks_for_run(run.id.as_str(), now_unix)
                    .await?
                {
                    WriteLockDecision::NoLocksRequired | WriteLockDecision::Acquired(_) => {}
                    WriteLockDecision::Queued | WriteLockDecision::Rejected => {
                        let reason =
                            "task child resume could not reacquire its write locks".to_owned();
                        let error = task_error(
                            "task_child_resume_lock_conflict",
                            reason.clone(),
                            TaskErrorClass::Policy,
                            Some(run.id.clone()),
                        );
                        let blocked_at = now_timestamp_secs();
                        handle
                            .record_task_run_turn_blocked(
                                blocked_task_run_turn(&task_run_turn, blocked_at),
                                Some(error.clone()),
                                blocked_at,
                            )
                            .await?;
                        handle.block_run(Some(error), now_timestamp_secs()).await?;
                        let _ = processor
                            .mark_turn_blocked(
                                thread_id.to_owned(),
                                turn_id.to_owned(),
                                reason.clone(),
                            )
                            .await;
                        processor
                            .crud_store
                            .mark_recovery_job_terminal(
                                recovery_job.id.as_str(),
                                pioneer_protocol::RecoveryJobStatus::Blocked,
                                Some(reason.clone()),
                                blocked_at,
                            )
                            .await
                            .context(
                                "failed to return task-owned recovery job to blocked after lock conflict",
                            )?;
                        return Ok(Some(TaskChildResumeOutcome::Conflict { reason }));
                    }
                }

                let execution_id = execution.as_ref().map(|execution| execution.id.clone());
                let start_result = self
                    .start_or_recover_run(
                        TaskExecutionContext {
                            workspace_id: task.workspace_id.clone(),
                            task_id: task.id.clone(),
                            execution_id,
                            worker_id: format!("task-resume-{}-{now_unix}", run.id),
                        },
                        run.clone(),
                        handle.clone(),
                    )
                    .await;
                match start_result {
                    Ok(TaskExecutorStartOutcome::Started)
                    | Ok(TaskExecutorStartOutcome::Queued) => {
                        // The atomic aggregate transition consumed the old
                        // recovery job before dispatch. `Started` owns the
                        // replacement locally; `Queued` means another fenced
                        // TaskRuntime owner already owns it. In neither case
                        // may the generic recovery worker dispatch it again.
                        TaskChildResumeOutcome::Resumed {
                            recovery_job_id: recovery_job.id,
                        }
                    }
                    Ok(TaskExecutorStartOutcome::Rejected) | Err(_) => {
                        let reason =
                            "task child resume failed while restarting its executor".to_owned();
                        let error = task_error(
                            "task_child_resume_dispatch_failed",
                            reason.clone(),
                            TaskErrorClass::Internal,
                            Some(run.id.clone()),
                        );
                        let blocked_at = now_timestamp_secs();
                        handle
                            .record_task_run_turn_blocked(
                                blocked_task_run_turn(&task_run_turn, blocked_at),
                                Some(error.clone()),
                                blocked_at,
                            )
                            .await?;
                        let _ = handle.block_run(Some(error), now_timestamp_secs()).await;
                        let _ = processor
                            .mark_turn_blocked(
                                thread_id.to_owned(),
                                turn_id.to_owned(),
                                reason.clone(),
                            )
                            .await;
                        processor
                            .crud_store
                            .mark_recovery_job_terminal(
                                recovery_job.id.as_str(),
                                pioneer_protocol::RecoveryJobStatus::Blocked,
                                Some(reason.clone()),
                                blocked_at,
                            )
                            .await
                            .context(
                                "failed to return task-owned recovery job to blocked after dispatch failure",
                            )?;
                        TaskChildResumeOutcome::Conflict { reason }
                    }
                }
            }
        };

        // Keep the compiler from treating the initial lineage read as an
        // accidental unused query: it is the ownership discriminator that
        // selects this path instead of generic recovery.
        let _ = task_run_turn;
        Ok(Some(outcome))
    }

    pub(super) async fn reconcile_child_turn_failed(
        &self,
        thread_id: &str,
        turn_id: &str,
        error_message: &str,
        origin: TaskChildReconciliationOrigin,
    ) -> Result<bool> {
        let Some(processor) = self.task_reconciliation_processor(origin)? else {
            return Ok(true);
        };
        let Some(child_runtime) =
            load_child_runtime_for_turn(&processor, thread_id, turn_id).await?
        else {
            return Ok(true);
        };
        if child_runtime.task_run_turn.status == TaskRunTurnStatus::Blocked {
            return Ok(false);
        }
        if !matches!(
            child_runtime.task_run_turn.status,
            TaskRunTurnStatus::InProgress | TaskRunTurnStatus::CandidateCreated
        ) {
            return Ok(true);
        }
        let Some(task_response) = processor
            .crud_store
            .get_task(child_runtime.task_run_turn.task_id.as_str())
            .await?
        else {
            bail!(
                "task `{}` disappeared while reconciling failed child turn",
                child_runtime.task_run_turn.task_id
            );
        };
        if task_response.task.status.is_terminal() {
            return Ok(true);
        }
        let handle = TaskExecutionHandle::new(
            processor.crud_store.clone(),
            processor.task_runtime.event_bus(),
            child_runtime.task_run_turn.task_id.clone(),
            child_runtime.task_run_turn.run_id.clone(),
        );
        if child_runtime.task_run_turn.kind == TaskRunTurnKind::Review {
            let failed_at = now_timestamp_secs();
            record_task_run_turn_failure(
                &handle,
                &child_runtime.task_run_turn,
                TaskRunTurnStatus::Failed,
                Some(task_error(
                    "reviewer_turn_failed",
                    error_message.to_owned(),
                    TaskErrorClass::Unknown,
                    Some(child_runtime.task_run_turn.run_id.clone()),
                )),
                failed_at,
            )
            .await?;
            return Ok(true);
        }
        let Some((_, turn)) = processor.crud_store.get_turn(thread_id, turn_id).await? else {
            self.fail_child_turn(
                child_runtime,
                error_message,
                TaskRunTurnStatus::Failed,
                handle,
            )
            .await?;
            return Ok(true);
        };
        if turn.status == TurnStatus::Blocked {
            self.block_child_turn(child_runtime, error_message, handle)
                .await?;
            return Ok(true);
        }
        let target_status = task_run_turn_terminal_status_from_child_turn_status(turn.status)
            .unwrap_or(TaskRunTurnStatus::Failed);
        self.fail_child_turn(child_runtime, error_message, target_status, handle)
            .await?;
        Ok(true)
    }

    pub(super) async fn reconcile_child_turn_cancelled(
        &self,
        thread_id: &str,
        turn_id: &str,
        reason: &str,
        origin: TaskChildReconciliationOrigin,
    ) -> Result<bool> {
        let Some(processor) = self.task_reconciliation_processor(origin)? else {
            return Ok(true);
        };
        let Some(child_runtime) =
            load_child_runtime_for_turn(&processor, thread_id, turn_id).await?
        else {
            return Ok(true);
        };
        if child_runtime.task_run_turn.status == TaskRunTurnStatus::Blocked {
            return Ok(false);
        }
        if !matches!(
            child_runtime.task_run_turn.status,
            TaskRunTurnStatus::InProgress | TaskRunTurnStatus::CandidateCreated
        ) {
            return Ok(true);
        }
        let Some(task_response) = processor
            .crud_store
            .get_task(child_runtime.task_run_turn.task_id.as_str())
            .await?
        else {
            bail!(
                "task `{}` disappeared while reconciling cancelled child turn",
                child_runtime.task_run_turn.task_id
            );
        };
        if task_response.task.status.is_terminal() {
            return Ok(true);
        }
        let handle = TaskExecutionHandle::new(
            processor.crud_store.clone(),
            processor.task_runtime.event_bus(),
            child_runtime.task_run_turn.task_id.clone(),
            child_runtime.task_run_turn.run_id.clone(),
        );
        self.cancel_child_turn(child_runtime, reason, handle)
            .await?;
        Ok(true)
    }

    pub(super) async fn reconcile_child_turn_blocked(
        &self,
        thread_id: &str,
        turn_id: &str,
        reason: &str,
        origin: TaskChildReconciliationOrigin,
    ) -> Result<bool> {
        let Some(processor) = self.task_reconciliation_processor(origin)? else {
            return Ok(true);
        };
        let Some(child_runtime) =
            load_child_runtime_for_turn(&processor, thread_id, turn_id).await?
        else {
            return Ok(true);
        };
        if child_runtime.task_run_turn.status == TaskRunTurnStatus::Blocked {
            return Ok(false);
        }
        if !matches!(
            child_runtime.task_run_turn.status,
            TaskRunTurnStatus::InProgress | TaskRunTurnStatus::CandidateCreated
        ) {
            return Ok(true);
        }
        let Some(task_response) = processor
            .crud_store
            .get_task(child_runtime.task_run_turn.task_id.as_str())
            .await?
        else {
            bail!(
                "task `{}` disappeared while reconciling blocked child turn",
                child_runtime.task_run_turn.task_id
            );
        };
        if task_response.task.status.is_terminal() {
            return Ok(true);
        }
        let handle = TaskExecutionHandle::new(
            processor.crud_store.clone(),
            processor.task_runtime.event_bus(),
            child_runtime.task_run_turn.task_id.clone(),
            child_runtime.task_run_turn.run_id.clone(),
        );
        self.block_child_turn(child_runtime, reason, handle).await?;
        Ok(true)
    }

    async fn complete_child_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        child_runtime: TaskRunChildRuntime,
        handle: TaskExecutionHandle,
    ) -> Result<()> {
        if child_runtime.task_run_turn.kind == TaskRunTurnKind::Review {
            self.complete_reviewer_turn(processor, child_runtime, handle)
                .await?;
            return Ok(());
        }
        let task_response = processor
            .crud_store
            .get_task(child_runtime.task_run_turn.task_id.as_str())
            .await?
            .ok_or_else(|| anyhow!("task `{}` not found", child_runtime.task_run_turn.task_id))?;
        let agent_spec =
            select_agent_spec(&task_response, child_runtime.task_run_turn.run_id.as_str())
                .ok_or_else(|| {
                    anyhow!("agent task `{}` has no agent spec", task_response.task.id)
                })?;
        let review_policy = agent_spec
            .review_policy
            .clone()
            .filter(|policy| policy.is_enabled());
        if let Some(candidate) = processor
            .crud_store
            .get_accepted_task_result_candidate(child_runtime.task_run_turn.run_id.as_str())
            .await?
            && candidate.task_run_turn_id == child_runtime.task_run_turn.id
            && let Some(result) = candidate.result
        {
            handle
                .complete_run(
                    Some(result),
                    candidate.resolved_at.unwrap_or_else(now_timestamp_secs),
                )
                .await?;
            mark_task_run_occurrence_turn_completed(processor, &child_runtime.lineage).await?;
            return Ok(());
        }
        if let Some(candidate) = processor
            .crud_store
            .get_task_result_candidate_by_turn(child_runtime.task_run_turn.id.as_str())
            .await?
            && matches!(
                candidate.status,
                TaskResultCandidateStatus::PendingReview
                    | TaskResultCandidateStatus::ExtractionFailed
                    | TaskResultCandidateStatus::Rejected
                    | TaskResultCandidateStatus::Superseded
                    | TaskResultCandidateStatus::Cancelled
            )
        {
            return Ok(());
        }

        let artifact_mode = if review_policy.is_some() {
            TaskAgentResultArtifactMode::ResultCandidate {
                candidate_id: task_result_candidate_id(
                    child_runtime.task_run_turn.run_id.as_str(),
                    child_runtime.task_run_turn.turn_id.as_str(),
                ),
            }
        } else {
            TaskAgentResultArtifactMode::FinalResult
        };
        match TaskAgentResultExtractor::extract_with_artifact_mode(
            processor,
            &child_runtime.task_run_turn,
            &child_runtime.lineage,
            artifact_mode,
        )
        .await?
        {
            Ok(result) if review_policy.is_some() => {
                let review_policy = review_policy.as_ref().expect("review policy checked");
                let completed_at = now_timestamp_secs();
                let completed_turn =
                    candidate_created_task_run_turn(&child_runtime.task_run_turn, completed_at);
                let candidate = match invalid_structured_result_error(
                    &result,
                    &agent_spec,
                    child_runtime.task_run_turn.run_id.as_str(),
                ) {
                    Some(error) if revision_possible(review_policy, &completed_turn) => {
                        extraction_failed_result_candidate(&completed_turn, error, completed_at)
                    }
                    Some(error) => {
                        record_task_run_turn_failure(
                            &handle,
                            &child_runtime.task_run_turn,
                            TaskRunTurnStatus::Failed,
                            Some(error.clone()),
                            completed_at,
                        )
                        .await?;
                        handle.fail_run(Some(error), completed_at).await?;
                        mark_task_run_occurrence_turn_failed(
                            processor,
                            &child_runtime.lineage,
                            "child task result extraction failed",
                        )
                        .await?;
                        return Ok(());
                    }
                    None => pending_review_result_candidate(&completed_turn, result, completed_at),
                };
                let candidate_for_review = candidate.clone();
                handle
                    .record_pending_review_result_candidate(completed_turn, candidate, completed_at)
                    .await?;
                self.start_reviewer_turns_for_candidate(
                    processor,
                    &task_response,
                    &agent_spec,
                    review_policy,
                    &candidate_for_review,
                )
                .await?;
            }
            Ok(result) => {
                let completed_at = now_timestamp_secs();
                let completed_turn =
                    candidate_created_task_run_turn(&child_runtime.task_run_turn, completed_at);
                let candidate =
                    accepted_result_candidate(&completed_turn, result.clone(), completed_at);
                let review_event = runtime_auto_accept_review_event(&candidate, completed_at);
                handle
                    .record_auto_accepted_result_candidate(
                        completed_turn,
                        candidate,
                        review_event,
                        completed_at,
                    )
                    .await?;
                handle.complete_run(Some(result), completed_at).await?;
                mark_task_run_occurrence_turn_completed(processor, &child_runtime.lineage).await?;
            }
            Err(error)
                if review_policy.as_ref().is_some_and(|policy| {
                    revision_possible(policy, &child_runtime.task_run_turn)
                }) =>
            {
                let completed_at = now_timestamp_secs();
                let completed_turn =
                    candidate_created_task_run_turn(&child_runtime.task_run_turn, completed_at);
                let candidate =
                    extraction_failed_result_candidate(&completed_turn, error, completed_at);
                let candidate_for_review = candidate.clone();
                handle
                    .record_pending_review_result_candidate(completed_turn, candidate, completed_at)
                    .await?;
                let review_policy = review_policy.as_ref().expect("review policy checked");
                self.start_reviewer_turns_for_candidate(
                    processor,
                    &task_response,
                    &agent_spec,
                    review_policy,
                    &candidate_for_review,
                )
                .await?;
            }
            Err(error) => {
                let failed_at = now_timestamp_secs();
                record_task_run_turn_failure(
                    &handle,
                    &child_runtime.task_run_turn,
                    TaskRunTurnStatus::Failed,
                    Some(error.clone()),
                    failed_at,
                )
                .await?;
                handle.fail_run(Some(error), failed_at).await?;
                mark_task_run_occurrence_turn_failed(
                    processor,
                    &child_runtime.lineage,
                    "child task result extraction failed",
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn start_reviewer_turns_for_candidate(
        &self,
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        agent_spec: &TaskAgentSpec,
        review_policy: &TaskAgentReviewPolicy,
        candidate: &TaskResultCandidate,
    ) -> Result<()> {
        if review_policy.reviewers.is_empty() {
            return Ok(());
        }
        for (index, reviewer_spec) in review_policy.reviewers.iter().enumerate() {
            if reviewer_spec.reviewer_kind != TaskResultReviewerKind::ReviewAgent {
                continue;
            }
            let reviewer_key = task_result_reviewer_spec_key(index, reviewer_spec);
            let reviewer_thread_id =
                stable_review_thread_id(candidate.id.as_str(), reviewer_key.as_str());
            let reviewer_turn_id =
                stable_review_turn_id(candidate.id.as_str(), reviewer_key.as_str());
            let reviewer_context = processor
                .critical_task_service()
                .create_task_result_reviewer_context(CreateTaskResultReviewerContextParams {
                    candidate_id: candidate.id.clone(),
                    reviewer_index: index,
                    reviewer_spec: reviewer_spec.clone(),
                    reviewer_thread_id,
                    reviewer_turn_id,
                    created_at: Some(now_timestamp_secs()),
                })
                .await?;
            self.dispatch_or_recover_reviewer_turn(
                processor,
                task_response,
                agent_spec,
                review_policy,
                candidate,
                index,
                reviewer_spec,
                reviewer_context.task_run_turn,
            )
            .await?;
        }
        Ok(())
    }

    async fn dispatch_or_recover_reviewer_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        agent_spec: &TaskAgentSpec,
        review_policy: &TaskAgentReviewPolicy,
        candidate: &TaskResultCandidate,
        reviewer_index: usize,
        reviewer_spec: &TaskResultReviewerSpec,
        task_run_turn: TaskRunTurn,
    ) -> Result<()> {
        let review_event_exists = self
            .review_event_exists_for_turn(candidate.id.as_str(), task_run_turn.turn_id.as_str())
            .await?;
        if review_event_exists {
            let reviewer_execution_id = task_run_turn
                .execution_id
                .clone()
                .unwrap_or_else(|| task_run_turn.turn_id.clone());
            let handle = TaskExecutionHandle::new(
                processor.crud_store.clone(),
                processor.task_runtime.event_bus(),
                task_run_turn.task_id.clone(),
                task_run_turn.run_id.clone(),
            );
            self.mark_reviewer_turn_recorded(handle, task_run_turn)
                .await?;
            processor
                .finalize_agent_execution_and_notify(reviewer_execution_id.as_str(), "completed")
                .await?;
            return Ok(());
        }
        let existing_turn = processor
            .crud_store
            .get_turn(
                task_run_turn.thread_id.as_str(),
                task_run_turn.turn_id.as_str(),
            )
            .await?
            .map(|(_, turn)| turn);

        let task = &task_response.task;
        let run = processor
            .crud_store
            .get_task_run(task_run_turn.run_id.as_str())
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "reviewer Task run `{}` disappeared before dispatch",
                    task_run_turn.run_id
                )
            })?;
        let parent = resolve_parent_context(processor, task).await?;
        let reviewer_execution_id = task_run_turn
            .execution_id
            .clone()
            .unwrap_or_else(|| task_run_turn.turn_id.clone());
        let reviewer_key = task_result_reviewer_spec_key(reviewer_index, reviewer_spec);
        let mut reviewer_agent_spec = agent_spec.clone();
        reviewer_agent_spec.id = format!("{}:reviewer:{reviewer_key}", agent_spec.id);
        reviewer_agent_spec.agent_role = reviewer_spec
            .agent_role
            .clone()
            .or_else(|| agent_spec.agent_role.clone());
        reviewer_agent_spec.agent_nickname = reviewer_spec.agent_nickname.clone();
        let action_policy_generation = processor.current_authorization_revision().await?;
        let database = processor.crud_store.database_connection();
        let (mut action_adapter, action_options, action_capabilities) =
            if pioneer_crud::load_agent_execution(&database, reviewer_execution_id.as_str())
                .await?
                .is_some()
            {
                materialize_task_agent_action_binding_for_execution(
                    processor,
                    task,
                    &reviewer_agent_spec,
                    reviewer_execution_id.as_str(),
                    run.id.as_str(),
                    parent.root_thread_id.as_str(),
                    action_policy_generation,
                    existing_turn.is_some(),
                )
                .await
                .context("failed to restore exact reviewer agent action service")?
            } else {
                let occurrence = processor
                    .crud_store
                    .get_task_occurrence_contract_by_run(run.id.as_str())
                    .await?
                    .context("reviewer run is missing its durable occurrence contract")?;
                let candidate_execution_id = occurrence
                    .agent_execution_id
                    .as_deref()
                    .context("reviewer run has no exact candidate execution")?;
                let (candidate_adapter, candidate_options, candidate_capabilities) =
                    materialize_task_agent_action_binding_for_execution(
                        processor,
                        task,
                        agent_spec,
                        candidate_execution_id,
                        run.id.as_str(),
                        parent.root_thread_id.as_str(),
                        action_policy_generation,
                        false,
                    )
                    .await
                    .context("failed to restore candidate launch catalog for reviewer")?;
                let mut catalog_binding =
                    crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                        candidate_adapter,
                        candidate_options,
                        candidate_capabilities,
                    );
                catalog_binding
                    .refresh_start_options_catalog(processor, candidate.turn_id.as_str())
                    .await
                    .context("failed to project reviewer launch catalog")?;
                let nickname = reviewer_spec
                    .agent_nickname
                    .as_deref()
                    .context("review agent requires an exact configured nickname")?;
                let (identity, profile) = catalog_binding
                    .adapter
                    .lock()
                    .await
                    .resolve_named_task_reviewer_launch(nickname)
                    .map_err(|error| anyhow!("failed to resolve exact reviewer: {error:?}"))?;
                let execution_id = AgentExecutionId::new(reviewer_execution_id.clone())
                    .map_err(|error| anyhow!("reviewer execution id is invalid: {error:?}"))?;
                let reviewer_root_execution_id = occurrence
                    .work_graph_root_execution_id
                    .as_deref()
                    .context("reviewer occurrence has no exact work-graph root")?;
                let reviewer_root_execution_id = AgentExecutionId::new(
                    reviewer_root_execution_id.to_owned(),
                )
                .map_err(|error| anyhow!("reviewer work-graph root is invalid: {error:?}"))?;
                crate::authorization::materialize_selected_task_agent_action_binding(
                    execution_id,
                    parent.home_root_thread_id.as_str(),
                    reviewer_root_execution_id,
                    reviewer_agent_spec.id.as_str(),
                    identity,
                    profile,
                    occurrence.execution_generation,
                    u64::from(occurrence.retry_attempt).saturating_add(1),
                    u16::try_from(reviewer_agent_spec.depth)
                        .context("reviewer depth is invalid")?,
                    "agent_reviewer",
                    action_policy_generation,
                )
                .map_err(|error| anyhow!("failed to bind exact reviewer agent: {error:?}"))?
            };
        let reviewer_facts = action_adapter.persistence_facts();
        let turn_settings = resolved_task_execution_turn_settings(
            processor,
            task,
            &reviewer_agent_spec,
            &reviewer_facts,
            None,
        )
        .await?;
        let effective_model = turn_settings.model.clone();
        if let Some(turn) = existing_turn {
            let action_binding = processor
                .prepare_agent_action_binding(
                    task_run_turn.turn_id.clone(),
                    crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                        action_adapter,
                        action_options,
                        action_capabilities,
                    ),
                )
                .await?;
            processor
                .register_agent_action_binding(task_run_turn.turn_id.clone(), action_binding)
                .await;
            match turn.status {
                TurnStatus::Completed => {
                    let handle = TaskExecutionHandle::new(
                        processor.crud_store.clone(),
                        processor.task_runtime.event_bus(),
                        task_run_turn.task_id.clone(),
                        task_run_turn.run_id.clone(),
                    );
                    let lineage =
                        load_required_task_thread_lineage(processor, &task_run_turn).await?;
                    self.complete_reviewer_turn(
                        processor,
                        TaskRunChildRuntime {
                            lineage,
                            task_run_turn,
                        },
                        handle,
                    )
                    .await?;
                }
                TurnStatus::InProgress => {
                    revalidate_existing_task_child_execution_authorization(
                        processor,
                        task,
                        task_run_turn.thread_id.as_str(),
                        task_run_turn.turn_id.as_str(),
                    )
                    .await
                    .context("reviewer task continuation authorization is no longer active")?;
                    let cli_binding = processor
                        .crud_store
                        .get_cli_runtime_turn_binding(task_run_turn.turn_id.as_str())
                        .await?;
                    match (turn_settings.cli_runtime.as_ref(), cli_binding.as_ref()) {
                        (Some((runtime_id, runtime_kind)), Some(binding))
                            if binding.runtime_id == *runtime_id
                                && binding.runtime_kind
                                    == match runtime_kind {
                                        CLIAgentRuntimeKind::Codex => "codex",
                                        CLIAgentRuntimeKind::Claude => "claude",
                                    } => {}
                        (None, None) => {}
                        _ => bail!(
                            "reviewer runtime binding differs from its pinned execution profile"
                        ),
                    }
                }
                TurnStatus::Failed | TurnStatus::Interrupted => {
                    let handle = TaskExecutionHandle::new(
                        processor.crud_store.clone(),
                        processor.task_runtime.event_bus(),
                        task_run_turn.task_id.clone(),
                        task_run_turn.run_id.clone(),
                    );
                    let error_message = turn
                        .error
                        .unwrap_or_else(|| "reviewer child turn failed".to_owned());
                    let target_status =
                        task_run_turn_terminal_status_from_child_turn_status(turn.status)
                            .unwrap_or(TaskRunTurnStatus::Failed);
                    let terminal_status = if turn.status == TurnStatus::Interrupted {
                        "cancelled"
                    } else {
                        "failed"
                    };
                    let failed_at = now_timestamp_secs();
                    record_task_run_turn_failure(
                        &handle,
                        &task_run_turn,
                        target_status,
                        Some(task_error(
                            "reviewer_turn_failed",
                            error_message,
                            TaskErrorClass::Unknown,
                            Some(task_run_turn.run_id.clone()),
                        )),
                        failed_at,
                    )
                    .await?;
                    processor
                        .finalize_agent_execution_and_notify(
                            reviewer_execution_id.as_str(),
                            terminal_status,
                        )
                        .await?;
                }
                TurnStatus::Blocked => {
                    let handle = TaskExecutionHandle::new(
                        processor.crud_store.clone(),
                        processor.task_runtime.event_bus(),
                        task_run_turn.task_id.clone(),
                        task_run_turn.run_id.clone(),
                    );
                    let error_message = turn
                        .error
                        .unwrap_or_else(|| "reviewer child turn blocked".to_owned());
                    let lineage =
                        load_required_task_thread_lineage(processor, &task_run_turn).await?;
                    self.block_child_turn(
                        TaskRunChildRuntime {
                            lineage,
                            task_run_turn,
                        },
                        error_message.as_str(),
                        handle,
                    )
                    .await?;
                    processor
                        .finalize_agent_execution_and_notify(
                            reviewer_execution_id.as_str(),
                            "failed",
                        )
                        .await?;
                }
            }
            return Ok(());
        }
        let reviewer_permission_profile = turn_settings.permission_profile.clone();
        let reviewer_security_snapshot =
            if let Some((runtime_id, runtime_kind)) = turn_settings.cli_runtime.as_ref() {
                resolve_task_child_cli_execution_security_snapshot(
                    processor,
                    task.workspace_id.as_str(),
                    &parent,
                    &reviewer_agent_spec,
                    reviewer_permission_profile.clone(),
                    runtime_id.as_str(),
                    *runtime_kind,
                    task_run_turn.thread_id.as_str(),
                    task_run_turn.turn_id.as_str(),
                )
                .await
                .context("failed to resolve reviewer CLI execution security")?
            } else {
                resolve_task_child_execution_security_snapshot(
                    processor,
                    task.workspace_id.as_str(),
                    &parent,
                    &reviewer_agent_spec,
                    reviewer_permission_profile.clone(),
                    effective_model.model_provider.as_str(),
                    task_run_turn.thread_id.as_str(),
                    task_run_turn.turn_id.as_str(),
                )
                .await
                .context("failed to resolve reviewer execution security")?
            };
        let (reviewer_authorization, mut agent_skill_overlay) =
            resolve_task_child_execution_authorization_context(
                processor,
                task,
                &parent,
                effective_model.model_provider.as_str(),
                effective_model.model.as_str(),
                Some(&turn_settings.execution_backend),
                turn_settings.capabilities.as_slice(),
                &reviewer_security_snapshot.permission_profile,
                task_run_turn.turn_id.as_str(),
            )
            .await
            .context("failed to resolve reviewer execution authorization")?;
        let reviewer_authorization_context = reviewer_authorization.context;
        let reviewer_authorization_revalidation = reviewer_authorization.revalidation;
        let reviewer_authorization_fingerprint = reviewer_authorization_context
            .authorization_fingerprint()
            .context("failed to fingerprint reviewer execution authorization")?;
        let graph = persist_task_reviewer_execution_graph(
            processor,
            task_response,
            &run,
            reviewer_key.as_str(),
            &parent,
            &reviewer_facts,
            reviewer_authorization_fingerprint.as_str(),
        )
        .await?;
        action_adapter
            .bind_persisted_work_graph_root(graph.root_execution_id.as_str())
            .map_err(|error| anyhow!("failed to bind reviewer work graph: {error:?}"))?;
        if graph.queued {
            return Ok(());
        }
        let actor_contract = processor
            .crud_store
            .get_task_actor_contract(task.id.as_str())
            .await?
            .context("reviewer Task has no durable actor contract")?;
        let action_author = task_actor_turn_author(processor, &actor_contract).await?;
        let action_actor = action_author.actor.clone();
        let turn_response = agent_turn_response_input(
            processor,
            task_run_turn.turn_id.as_str(),
            reviewer_execution_id.as_str(),
        )
        .await?;
        let parent_sandbox_mode = processor
            .crud_store
            .get_thread_sandbox_mode(parent.parent_thread_id.as_str())
            .await?
            .with_context(|| {
                format!(
                    "reviewer Task `{}` parent thread `{}` has no persisted sandbox policy",
                    task.id, parent.parent_thread_id
                )
            })?;
        let thread_params = pioneer_protocol::ThreadStartParams {
            thread_id: task_run_turn.thread_id.clone(),
            workspace_id: task.workspace_id.clone(),
            name: Some(reviewer_thread_name(task, reviewer_spec)),
            model: Some(effective_model.model.clone()),
            model_provider: Some(effective_model.model_provider.clone()),
            sandbox: Some(parent_sandbox_mode),
            mode: Some(ThreadMode::Agent),
            origin_kind: Some(ThreadOriginKind::TaskRun),
            sidebar_visibility: Some(ThreadSidebarVisibility::Hidden),
            visibility: None,
            agent_nickname: reviewer_spec.agent_nickname.clone(),
            agent_role: reviewer_spec.agent_role.clone(),
        };
        let thread_outcome = processor
            .thread_manager
            .system_thread_start_seeded(task.workspace_id.clone(), thread_params, None, None)
            .await
            .context("failed to create hidden reviewer thread")?;
        let prompt = materialize_reviewer_prompt(
            task_response,
            agent_spec,
            review_policy,
            candidate,
            reviewer_spec,
            reviewer_key.as_str(),
        );
        let input = vec![UserInput::Text {
            text: prompt,
            text_elements: Vec::new(),
        }];
        if let Some((runtime_id, runtime_kind)) = turn_settings.cli_runtime.clone() {
            let (_, conversation_history) = load_task_execution_conversation_scope(
                processor,
                task,
                &run,
                &parent,
                task_run_turn.kind,
                task_run_turn.thread_id.as_str(),
                task_run_turn.turn_id.as_str(),
                thread_outcome.started_notification.thread.model.as_str(),
                thread_outcome
                    .started_notification
                    .thread
                    .model_provider
                    .as_str(),
            )
            .await?;
            let prepared = processor
                .prepare_task_cli_runtime_turn(
                    TurnStartParams {
                        agent_delegation_routes: Vec::new(),
                        thread_id: task_run_turn.thread_id.clone(),
                        turn_id: task_run_turn.turn_id.clone(),
                        input,
                        capabilities: turn_settings.capabilities.clone(),
                        model: Some(effective_model.model.clone()),
                        model_provider: Some(effective_model.model_provider.clone()),
                        sandbox_policy: None,
                        mode: Some(ThreadMode::Agent),
                        agent_launch: None,
                        reply_to_turn_id: None,
                        mentioned_principal_ids: Vec::new(),
                        execution_backend: Some(turn_settings.execution_backend.clone()),
                        reasoning: turn_settings.reasoning.clone(),
                        permission_profile: turn_settings.permission_selection.clone(),
                        cli_runtime_options: None,
                    },
                    runtime_id,
                    runtime_kind,
                    reviewer_permission_profile,
                    reviewer_security_snapshot,
                    reviewer_authorization_context,
                    reviewer_authorization_revalidation,
                    parent.parent_thread_id.clone(),
                    parent.parent_thread_id.clone(),
                    run.id.clone(),
                    reviewer_execution_id.clone(),
                    conversation_history,
                    action_author,
                    turn_response.clone(),
                )
                .await
                .context("failed to prepare reviewer CLI runtime turn")?;
            processor
                .activate_prepared_task_cli_runtime_turn(prepared)
                .await
                .context("failed to activate reviewer CLI runtime turn")?;
            let action_binding = processor
                .prepare_agent_action_binding(
                    task_run_turn.turn_id.clone(),
                    crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                        action_adapter,
                        action_options,
                        action_capabilities,
                    ),
                )
                .await?;
            processor
                .register_agent_action_binding(task_run_turn.turn_id.clone(), action_binding)
                .await;
            return Ok(());
        }
        if !matches!(
            turn_settings.execution_backend,
            AgentExecutionBackend::ApiProvider { .. }
        ) {
            bail!("reviewer Task backend has no installed runtime adapter");
        }
        let turn_outcome = processor
            .thread_manager
            .agent_turn_start_with_permission_profile(
                TurnStartParams {
                    agent_delegation_routes: Vec::new(),
                    thread_id: task_run_turn.thread_id.clone(),
                    turn_id: task_run_turn.turn_id.clone(),
                    input,
                    capabilities: turn_settings.capabilities.clone(),
                    model: Some(effective_model.model),
                    model_provider: Some(effective_model.model_provider),
                    sandbox_policy: None,
                    mode: Some(ThreadMode::Agent),
                    agent_launch: None,
                    reply_to_turn_id: None,
                    mentioned_principal_ids: Vec::new(),
                    execution_backend: Some(turn_settings.execution_backend.clone()),
                    reasoning: turn_settings.reasoning.clone(),
                    permission_profile: turn_settings.permission_selection.clone(),
                    cli_runtime_options: None,
                },
                reviewer_permission_profile,
                action_author,
            )
            .await
            .context("failed to create hidden reviewer turn")?;

        if let Err(error) = processor
            .validate_turn_artifact_user_inputs(
                task.workspace_id.as_str(),
                parent.root_thread_id.as_str(),
                turn_outcome.materialization.input.as_slice(),
            )
            .await
        {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            return Err(error).context("failed to validate hidden reviewer input");
        }
        let turn_permission_profile = match processor
            .materialized_turn_permission_profile(&turn_outcome.materialization.turn)
        {
            Ok(permission_profile) => permission_profile,
            Err(error) => {
                processor
                    .thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                return Err(error).context("failed to resolve reviewer task permission profile");
            }
        };
        if turn_permission_profile != reviewer_security_snapshot.permission_profile {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            bail!("materialized reviewer permission profile differs from its execution admission");
        }
        let child_security_snapshot = reviewer_security_snapshot;
        let child_authorization_context = reviewer_authorization_context;
        let child_authorization_revalidation = reviewer_authorization_revalidation;
        let profile_selected_audit = processor.turn_profile_selected_audit_event_for_turn(
            task.workspace_id.as_str(),
            task_run_turn.thread_id.as_str(),
            task_run_turn.turn_id.as_str(),
            turn_permission_profile.clone(),
        );
        let child_authority_json = child_authorization_context
            .to_persisted_json()
            .context("failed to encode hidden reviewer authority envelope")?;
        let child_turn_admission = match child_authorization_context
            .durable_turn_admission_after_revalidation(
                task_run_turn.thread_id.as_str(),
                task_run_turn.turn_id.as_str(),
                Some(&turn_settings.execution_backend),
                &child_authorization_revalidation,
            ) {
            Ok(admission) => admission,
            Err(error) => {
                processor
                    .thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                return Err(error).context("failed to reserve hidden reviewer execution quota");
            }
        };
        if let Err(error) = processor
            .crud_store
            .materialize_authorized_turn_start_with_reasoning_effort_and_permission_audit(
                &turn_outcome.materialization.thread,
                turn_outcome.materialization.sandbox_mode,
                &turn_outcome.materialization.turn,
                &turn_outcome.materialization.input,
                turn_settings
                    .reasoning
                    .as_ref()
                    .map(|reasoning| reasoning.effort.as_str()),
                action_actor,
                profile_selected_audit,
                child_authority_json.as_str(),
                None,
                Some(child_turn_admission),
                Some(super::turn_handlers::new_turn_execution(
                    processor.turn_execution_owner_id.as_ref(),
                    Some(&turn_settings.execution_backend),
                    &turn_outcome.materialization,
                )?),
                &child_security_snapshot,
                processor.turn_security_audit_events_for_turn(
                    task.workspace_id.as_str(),
                    task_run_turn.thread_id.as_str(),
                    task_run_turn.turn_id.as_str(),
                    &child_security_snapshot,
                ),
                None,
                Some(turn_response),
            )
            .await
        {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            return Err(error).context("failed to persist hidden reviewer turn");
        }
        if let Err(error) =
            register_resolved_task_child_execution_lease(processor, task_run_turn.turn_id.as_str())
                .await
        {
            let reason = "reviewer_execution_security_persist_failed".to_owned();
            if !processor
                .mark_turn_blocked(
                    task_run_turn.thread_id.clone(),
                    task_run_turn.turn_id.clone(),
                    reason,
                )
                .await
            {
                warn!(
                    thread_id = task_run_turn.thread_id,
                    turn_id = task_run_turn.turn_id,
                    failure_class = "reviewer_execution_security_close_failed",
                    "failed to durably close reviewer turn after admission failure"
                );
            }
            return Err(error).context("failed to register hidden reviewer execution lease");
        }

        processor.ensure_hook_runtime_with_run_store().await;
        close_admitted_task_turn_on_error(
            processor,
            task_run_turn.thread_id.as_str(),
            task_run_turn.turn_id.as_str(),
            processor
                .agent_manager
                .ensure_thread(task_run_turn.thread_id.as_str(), task.workspace_id.as_str())
                .await
                .map_err(|error| anyhow!("failed to prepare reviewer agent runtime: {error}")),
        )
        .await?;
        close_admitted_task_turn_on_error(
            processor,
            task_run_turn.thread_id.as_str(),
            task_run_turn.turn_id.as_str(),
            processor
                .ensure_agent_listener_task(task_run_turn.thread_id.as_str())
                .await,
        )
        .await?;
        let action_binding = processor
            .prepare_agent_action_binding(
                task_run_turn.turn_id.clone(),
                crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                    action_adapter,
                    action_options,
                    action_capabilities,
                ),
            )
            .await?;
        processor
            .register_agent_action_binding(task_run_turn.turn_id.clone(), action_binding)
            .await;
        let workspace_skill_policies = close_admitted_task_turn_on_error(
            processor,
            task_run_turn.thread_id.as_str(),
            task_run_turn.turn_id.as_str(),
            load_workspace_skill_policies(processor, task.workspace_id.as_str())
                .await
                .context("failed to load reviewer task workspace skill policies"),
        )
        .await?;
        let skill_catalog = close_admitted_task_turn_on_error(
            processor,
            task_run_turn.thread_id.as_str(),
            task_run_turn.turn_id.as_str(),
            processor
                .validate_turn_skill_capabilities(
                    task.workspace_id.as_str(),
                    turn_outcome.materialization.capabilities.as_slice(),
                )
                .await
                .map_err(|message| anyhow!(message))
                .context("failed to validate reviewer task skill capabilities"),
        )
        .await?;
        let resolved_artifacts = close_admitted_task_turn_on_error(
            processor,
            task_run_turn.thread_id.as_str(),
            task_run_turn.turn_id.as_str(),
            processor
                .resolve_provider_artifact_inputs(
                    task.workspace_id.as_str(),
                    turn_outcome.materialization.input.as_slice(),
                )
                .await
                .context("failed to resolve reviewer artifact input for provider"),
        )
        .await?;
        let runtime_environment = close_admitted_task_turn_on_error(
            processor,
            task_run_turn.thread_id.as_str(),
            task_run_turn.turn_id.as_str(),
            processor
                .create_artifact_output_environment(
                    task.workspace_id.as_str(),
                    task_run_turn.thread_id.as_str(),
                    task_run_turn.turn_id.as_str(),
                )
                .await
                .context("failed to prepare reviewer artifact output directory"),
        )
        .await?
        .into_iter()
        .collect();
        let (hook_runtime_context, history) = close_admitted_task_turn_on_error(
            processor,
            task_run_turn.thread_id.as_str(),
            task_run_turn.turn_id.as_str(),
            load_task_execution_conversation_scope(
                processor,
                task,
                &run,
                &parent,
                task_run_turn.kind,
                task_run_turn.thread_id.as_str(),
                task_run_turn.turn_id.as_str(),
                thread_outcome.started_notification.thread.model.as_str(),
                thread_outcome
                    .started_notification
                    .thread
                    .model_provider
                    .as_str(),
            )
            .await,
        )
        .await?;
        if let Err(error) = processor
            .persist_turn_runtime_snapshot_with_optional_agent_overlay(
                task_run_turn.thread_id.as_str(),
                task.workspace_id.as_str(),
                task_run_turn.turn_id.as_str(),
                ThreadMode::Agent,
                &hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                None,
                &workspace_skill_policies,
                turn_outcome.materialization.input.as_slice(),
                turn_outcome.materialization.capabilities.as_slice(),
                resolved_artifacts.as_slice(),
                &runtime_environment,
                history.as_slice(),
                &mut agent_skill_overlay,
            )
            .await
        {
            report_or_block_task_turn_failure(
                processor,
                task_run_turn.thread_id,
                task_run_turn.turn_id,
                TurnFailureRecoveryKind::TaskDispatch,
                format!("failed to persist reviewer task turn runtime snapshot: {error:#}"),
            )
            .await;
            return Ok(());
        }
        let runtime_permission_profile = turn_permission_profile;
        if let Err(error) = processor
            .agent_manager
            .start_turn_with_hook_context_permission_profile_security_snapshot_and_agent_skill_overlay(
                task_run_turn.thread_id.as_str(),
                task_run_turn.turn_id.as_str(),
                ThreadMode::Agent,
                hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                workspace_skill_policies,
                skill_catalog,
                agent_skill_overlay,
                turn_outcome.materialization.input,
                turn_outcome.materialization.capabilities,
                resolved_artifacts,
                runtime_environment,
                history,
                runtime_permission_profile,
                child_security_snapshot,
            )
            .await
        {
            report_or_block_task_turn_failure(
                processor,
                task_run_turn.thread_id,
                task_run_turn.turn_id,
                TurnFailureRecoveryKind::TaskDispatch,
                format!("failed to dispatch reviewer task turn: {error}"),
            )
            .await;
            return Ok(());
        }
        if let Err(error) = commit_task_turn_execution_running(
            processor,
            task_run_turn.thread_id.as_str(),
            task_run_turn.turn_id.as_str(),
        )
        .await
        {
            report_or_block_task_turn_failure(
                processor,
                task_run_turn.thread_id,
                task_run_turn.turn_id,
                TurnFailureRecoveryKind::TaskDispatch,
                format!("failed to commit reviewer task Turn execution ownership: {error:#}"),
            )
            .await;
        }
        Ok(())
    }

    async fn complete_reviewer_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        reviewer_runtime: TaskRunChildRuntime,
        handle: TaskExecutionHandle,
    ) -> Result<()> {
        let reviewer_execution_id = reviewer_runtime
            .task_run_turn
            .execution_id
            .clone()
            .unwrap_or_else(|| reviewer_runtime.task_run_turn.turn_id.clone());
        let Some(candidate_id) = reviewer_runtime.task_run_turn.reviews_candidate_id.clone() else {
            return Ok(());
        };
        if self
            .review_event_exists_for_turn(
                candidate_id.as_str(),
                reviewer_runtime.task_run_turn.turn_id.as_str(),
            )
            .await?
        {
            self.mark_reviewer_turn_recorded(handle, reviewer_runtime.task_run_turn)
                .await?;
            processor
                .finalize_agent_execution_and_notify(reviewer_execution_id.as_str(), "completed")
                .await?;
            return Ok(());
        }
        let Some(candidate) = processor
            .crud_store
            .get_task_result_candidate(candidate_id.as_str())
            .await?
        else {
            return Ok(());
        };
        let task_response = processor
            .crud_store
            .get_task(candidate.task_id.as_str())
            .await?
            .ok_or_else(|| anyhow!("task `{}` not found", candidate.task_id))?;
        let agent_spec = select_agent_spec(&task_response, candidate.run_id.as_str())
            .ok_or_else(|| anyhow!("agent task `{}` has no agent spec", task_response.task.id))?;
        let reviewer_key = reviewer_key_for_turn(
            agent_spec.review_policy.as_ref(),
            &candidate,
            &reviewer_runtime.task_run_turn,
        );
        let advisory =
            extract_reviewer_advisory(processor, reviewer_runtime.task_run_turn.turn_id.as_str())
                .await?;
        let action_decision = match advisory.decision {
            TaskResultReviewDecision::Accept => pioneer_protocol::AgentReviewDecision::Accept,
            TaskResultReviewDecision::Reject => pioneer_protocol::AgentReviewDecision::Reject,
            TaskResultReviewDecision::RequestChanges => {
                pioneer_protocol::AgentReviewDecision::RequestChanges
            }
            TaskResultReviewDecision::Abstain | TaskResultReviewDecision::Cancel => {
                pioneer_protocol::AgentReviewDecision::Abstain
            }
        };
        let binding = processor
            .agent_action_binding(reviewer_runtime.task_run_turn.turn_id.as_str())
            .await
            .context("reviewer Turn has no execution-bound canonical action service")?;
        let source_fence = super::agent_action_tools::current_agent_identity_source_fence(
            processor,
            reviewer_execution_id.as_str(),
        )
        .await?;
        let mut adapter = binding.adapter.lock().await;
        let action_id = pioneer_protocol::AgentActionId::new(canonical_agent_id(
            'A',
            &format!(
                "task-review-action\0{}\0{}",
                candidate.id, reviewer_runtime.task_run_turn.turn_id
            ),
        ))
        .map_err(|error| anyhow!("reviewer action id is invalid: {error:?}"))?;
        let action_intent = pioneer_protocol::AgentActionIntent::ReviewTaskResult {
            action_id,
            execution_id: AgentExecutionId::new(reviewer_execution_id.clone())
                .map_err(|error| anyhow!("reviewer execution id is invalid: {error:?}"))?,
            task_id: candidate.task_id.clone(),
            decision: action_decision,
            idempotency_key: format!(
                "task-review:{}:{}",
                candidate.id, reviewer_runtime.task_run_turn.turn_id
            ),
        };
        let prepared = adapter
            .prepare(&action_intent)
            .map_err(|error| anyhow!("reviewer action was denied: {error:?}"))?;
        let policy_generation = adapter.current_policy_generation();
        let mut action_plan = adapter
            .prepare_commit(
                &prepared,
                Some(
                    serde_json::json!({
                        "candidateId": candidate.id,
                        "reviewerTurnId": reviewer_runtime.task_run_turn.turn_id,
                    })
                    .to_string(),
                ),
                adapter.policy_fingerprint(),
                policy_generation,
            )
            .map_err(|error| anyhow!("reviewer action commit was denied: {error:?}"))?;
        super::agent_action_tools::apply_current_identity_source_fence(
            &mut action_plan,
            &source_fence,
        );
        drop(adapter);
        processor
            .critical_task_service()
            .record_task_result_review_event_with_agent_action(
                RecordTaskResultReviewEventParams {
                    candidate_id: candidate.id,
                    review_event_id: Some(format!(
                        "trre_{}",
                        reviewer_runtime.task_run_turn.turn_id
                    )),
                    actor: TaskResultReviewActor {
                        reviewer_kind: TaskResultReviewerKind::ReviewAgent,
                        reviewer: pioneer_protocol::TaskResultReviewerRef::AgentExecution(
                            AgentExecutionId::new(reviewer_execution_id.clone()).map_err(
                                |error| anyhow!("reviewer execution id is invalid: {error:?}"),
                            )?,
                        ),
                        reviewer_thread_id: Some(reviewer_runtime.task_run_turn.thread_id.clone()),
                        reviewer_turn_id: Some(reviewer_runtime.task_run_turn.turn_id.clone()),
                        reviewer_user_id: None,
                        reviewer_agent_spec_id: reviewer_key,
                    },
                    event_kind: TaskResultReviewEventKind::Advisory,
                    decision: advisory.decision,
                    feedback_text: advisory.feedback_text,
                    feedback: advisory.feedback,
                    confidence: advisory.confidence,
                    supersedes_review_event_id: None,
                    next_task_run_turn_id: None,
                    created_at: Some(now_timestamp_secs()),
                },
                Some(action_plan.input),
            )
            .await?;
        self.mark_reviewer_turn_recorded(handle, reviewer_runtime.task_run_turn)
            .await?;
        processor
            .finalize_agent_execution_and_notify(reviewer_execution_id.as_str(), "completed")
            .await?;
        Ok(())
    }

    async fn review_event_exists_for_turn(
        &self,
        candidate_id: &str,
        turn_id: &str,
    ) -> Result<bool> {
        let processor = self.processor()?;
        Ok(processor
            .crud_store
            .list_task_result_review_events(candidate_id)
            .await?
            .iter()
            .any(|event| event.reviewer_turn_id.as_deref() == Some(turn_id)))
    }

    async fn mark_reviewer_turn_recorded(
        &self,
        handle: TaskExecutionHandle,
        mut task_run_turn: TaskRunTurn,
    ) -> Result<()> {
        if task_run_turn.status == TaskRunTurnStatus::ReviewRecorded {
            return Ok(());
        }
        let completed_at = now_timestamp_secs();
        task_run_turn.status = TaskRunTurnStatus::ReviewRecorded;
        task_run_turn.completed_at = Some(completed_at);
        handle
            .record_task_run_turn_completed(task_run_turn, completed_at)
            .await?;
        Ok(())
    }

    async fn fail_child_turn(
        &self,
        child_runtime: TaskRunChildRuntime,
        error_message: &str,
        target_status: TaskRunTurnStatus,
        handle: TaskExecutionHandle,
    ) -> Result<()> {
        let processor = self
            .processor()?
            .with_database_class(SqliteWriteClass::Critical);
        let handle = handle.with_critical_writes();
        let failed_at = now_timestamp_secs();
        let error = task_error(
            "child_turn_failed",
            error_message.to_owned(),
            TaskErrorClass::Unknown,
            Some(child_runtime.task_run_turn.run_id.clone()),
        );
        record_task_run_turn_failure(
            &handle,
            &child_runtime.task_run_turn,
            target_status,
            Some(error.clone()),
            failed_at,
        )
        .await?;
        handle.fail_run(Some(error), failed_at).await?;
        mark_task_run_occurrence_turn_failed(
            &processor,
            &child_runtime.lineage,
            "child_turn_failed",
        )
        .await?;
        Ok(())
    }

    async fn cancel_child_turn(
        &self,
        child_runtime: TaskRunChildRuntime,
        reason: &str,
        handle: TaskExecutionHandle,
    ) -> Result<()> {
        let processor = self
            .processor()?
            .with_database_class(SqliteWriteClass::Critical);
        let handle = handle.with_critical_writes();
        let cancelled_at = now_timestamp_secs();
        let error = task_error(
            "child_turn_cancelled",
            reason.to_owned(),
            TaskErrorClass::Cancelled,
            Some(child_runtime.task_run_turn.run_id.clone()),
        );
        record_task_run_turn_failure(
            &handle,
            &child_runtime.task_run_turn,
            TaskRunTurnStatus::Cancelled,
            Some(error),
            cancelled_at,
        )
        .await?;
        handle
            .cancel_run(Some(reason.to_owned()), cancelled_at)
            .await?;
        mark_task_run_occurrence_turn_terminal(
            &processor,
            &child_runtime.lineage,
            TurnStatus::Interrupted,
            Some("child_turn_cancelled".to_owned()),
            cancelled_at,
        )
        .await?;
        Ok(())
    }

    async fn block_child_turn(
        &self,
        child_runtime: TaskRunChildRuntime,
        reason: &str,
        handle: TaskExecutionHandle,
    ) -> Result<()> {
        let processor = self
            .processor()?
            .with_database_class(SqliteWriteClass::Critical);
        let handle = handle.with_critical_writes();
        let blocked_at = now_timestamp_secs();
        let error = task_error(
            "child_turn_blocked",
            reason.to_owned(),
            TaskErrorClass::Policy,
            Some(child_runtime.task_run_turn.run_id.clone()),
        );
        handle
            .record_task_run_turn_blocked(
                blocked_task_run_turn(&child_runtime.task_run_turn, blocked_at),
                Some(error.clone()),
                blocked_at,
            )
            .await?;
        handle.block_run(Some(error), blocked_at).await?;
        mark_task_run_occurrence_turn_blocked(
            &processor,
            &child_runtime.lineage,
            "child_turn_blocked",
        )
        .await?;
        Ok(())
    }

    async fn block_revision_dispatch_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        child_runtime: TaskRunChildRuntime,
        handle: TaskExecutionHandle,
        error: TaskError,
    ) -> Result<()> {
        let processor = Arc::clone(processor);
        message_fresh_task(async move {
            Self::block_revision_dispatch_turn_owned(processor, child_runtime, handle, error).await
        })
        .await
        .context("task revision block transition task did not finish")?
    }

    async fn block_revision_dispatch_turn_owned(
        processor: Arc<MessageProcessor>,
        child_runtime: TaskRunChildRuntime,
        handle: TaskExecutionHandle,
        mut error: TaskError,
    ) -> Result<()> {
        let blocked_at = now_timestamp_secs();
        error.details = Some(revision_dispatch_error_details(
            &child_runtime.task_run_turn,
        ));
        let message = error.message.clone();
        handle
            .record_task_run_turn_blocked(
                blocked_task_run_turn(&child_runtime.task_run_turn, blocked_at),
                Some(error.clone()),
                blocked_at,
            )
            .await?;
        handle.block_run(Some(error), blocked_at).await?;
        mark_task_run_occurrence_turn_blocked(&processor, &child_runtime.lineage, message.as_str())
            .await?;
        Ok(())
    }
}

#[async_trait]
impl TaskExecutor for TaskAgentExecutor {
    fn kind(&self) -> TaskExecutorKind {
        TaskExecutorKind::Agent
    }

    async fn start_run(
        &self,
        context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> pioneer_tasks::TaskRuntimeResult<TaskExecutorStartOutcome> {
        self.start_or_recover_run(context, run, handle).await
    }

    async fn cancel_run(
        &self,
        _context: TaskExecutionContext,
        run_id: &str,
        reason: &str,
        handle: TaskExecutionHandle,
    ) -> pioneer_tasks::TaskRuntimeResult<()> {
        let processor = self
            .processor()?
            .with_database_class(SqliteWriteClass::Critical);
        let handle = handle.with_critical_writes();
        let task_run_turns = processor.crud_store.list_task_run_turns(run_id).await?;
        let cancelled_at = now_timestamp_secs();
        // Task cancellation owns the run terminal state. Commit it before
        // interrupting the child so the generic turn-interruption projector
        // cannot race in and reinterpret an intentional cancellation as a
        // failed Task.
        handle
            .cancel_run(Some(reason.to_owned()), cancelled_at)
            .await?;
        for task_run_turn in task_run_turns {
            let error = task_error(
                "task_run_cancelled",
                reason.to_owned(),
                TaskErrorClass::Cancelled,
                Some(run_id.to_owned()),
            );
            record_task_run_turn_failure(
                &handle,
                &task_run_turn,
                TaskRunTurnStatus::Cancelled,
                Some(error),
                cancelled_at,
            )
            .await?;
            let child_runtime = match load_child_runtime_from_task_run_turn(
                &processor,
                task_run_turn.clone(),
            )
            .await
            {
                Ok(child_runtime) => child_runtime,
                Err(error) => {
                    warn!(
                        run_id,
                        turn_id = task_run_turn.turn_id,
                        error = %format!("{error:#}"),
                        failure_class = "task_cancel_partial_runtime_cleanup_skipped",
                        "Task run Turn was cancelled before its child runtime lineage completed"
                    );
                    continue;
                }
            };
            let cancelled_cli_runtime = processor
                .cancel_task_cli_runtime_turn(
                    child_runtime.task_run_turn.thread_id.as_str(),
                    child_runtime.task_run_turn.turn_id.as_str(),
                    reason,
                )
                .await;
            match cancelled_cli_runtime {
                Ok(true) => {}
                Ok(false) => {
                    let _ = processor
                        .agent_manager
                        .cancel_turn(
                            child_runtime.task_run_turn.thread_id.as_str(),
                            child_runtime.task_run_turn.turn_id.as_str(),
                            reason,
                        )
                        .await;
                }
                Err(_error) => {
                    warn!(
                        run_id,
                        turn_id = child_runtime.task_run_turn.turn_id.as_str(),
                        failure_class = "task_cli_runtime_cancel_failed",
                        "failed to cancel native task CLI runtime turn"
                    );
                }
            }
            if child_runtime.task_run_turn.kind != TaskRunTurnKind::Review {
                mark_task_run_occurrence_turn_terminal(
                    &processor,
                    &child_runtime.lineage,
                    TurnStatus::Interrupted,
                    Some(reason.to_owned()),
                    cancelled_at,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn recover_run(
        &self,
        context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> pioneer_tasks::TaskRuntimeResult<TaskExecutorRecoveryOutcome> {
        match self.start_or_recover_run(context, run, handle).await? {
            TaskExecutorStartOutcome::Started => Ok(TaskExecutorRecoveryOutcome::Recovered),
            TaskExecutorStartOutcome::Queued => Ok(TaskExecutorRecoveryOutcome::AlreadyRunning),
            TaskExecutorStartOutcome::Rejected => Ok(TaskExecutorRecoveryOutcome::LeftUnchanged),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct TaskParentRuntimeContext {
    pub(super) parent_thread_id: String,
    pub(super) parent_turn_id: Option<String>,
    /// Collaboration capsule that owns the occurrence conversation and local
    /// control. This differs from `root_thread_id` only for an explicitly
    /// routed Task; the latter remains the immutable source admission root.
    pub(super) home_root_thread_id: String,
    pub(super) root_thread_id: String,
}

#[derive(Debug, Clone)]
struct EffectiveAgentModel {
    model: String,
    model_provider: String,
}

fn composer_work_launch(task: &Task) -> Result<Option<&TurnStartParams>> {
    let Some(composer_work) = task
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.composer_work.as_ref())
    else {
        return Ok(None);
    };
    if composer_work.version != TASK_COMPOSER_WORK_VERSION {
        bail!(
            "unsupported composer work payload version {}; expected {}",
            composer_work.version,
            TASK_COMPOSER_WORK_VERSION
        );
    }
    Ok(Some(&composer_work.launch))
}

fn task_cli_runtime_backend(task: &Task) -> Result<Option<(String, CLIAgentRuntimeKind)>> {
    Ok(
        composer_work_launch(task)?.and_then(|launch| match launch.execution_backend.as_ref() {
            Some(AgentExecutionBackend::CLIAgentRuntime {
                runtime_id,
                runtime_kind,
            }) => Some((runtime_id.clone(), *runtime_kind)),
            None
            | Some(AgentExecutionBackend::ApiProvider { .. })
            | Some(AgentExecutionBackend::ACPAgentRuntime { .. }) => None,
        }),
    )
}

fn cli_runtime_backend_enabled(processor: &MessageProcessor, task: &Task) -> Result<bool> {
    let Some((runtime_id, _)) = task_cli_runtime_backend(task)? else {
        return Ok(true);
    };
    let instances = crate::cli_runtime::config::load_effective_cli_runtime_instances(
        processor.artifact_runtime_home.as_path(),
    )
    .with_context(|| format!("failed to load CLI runtime `{runtime_id}` for task admission"))?;
    let instance = instances
        .into_iter()
        .find(|instance| instance.id == runtime_id)
        .ok_or_else(|| anyhow!("unknown CLI runtime `{runtime_id}` for task admission"))?;
    Ok(instance.enabled)
}

fn rebound_composer_work_launch(
    task: &Task,
    child_thread_id: &str,
    child_turn_id: &str,
) -> Result<Option<TurnStartParams>> {
    let Some(composer_work) = task
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.composer_work.as_ref())
    else {
        return Ok(None);
    };
    if composer_work.version != TASK_COMPOSER_WORK_VERSION {
        bail!(
            "unsupported composer work payload version {}; expected {}",
            composer_work.version,
            TASK_COMPOSER_WORK_VERSION
        );
    }
    Ok(Some(
        composer_work.rebound_launch(child_thread_id, child_turn_id),
    ))
}

fn validate_composer_launch_backend(launch: &TurnStartParams) -> Result<()> {
    match launch.execution_backend.as_ref() {
        None
        | Some(AgentExecutionBackend::ApiProvider { .. })
        | Some(AgentExecutionBackend::CLIAgentRuntime { .. }) => Ok(()),
        Some(AgentExecutionBackend::ACPAgentRuntime { runtime_id }) => {
            bail!("composer work selected unsupported ACP runtime `{runtime_id}`")
        }
    }
}

fn composer_launch_permission_profile(
    launch: Option<&TurnStartParams>,
) -> Option<TurnPermissionProfileSnapshot> {
    launch.map(|launch| {
        pioneer_protocol::resolve_turn_permission_profile(launch.permission_profile.as_ref())
    })
}

fn composer_launch_reasoning_effort(launch: Option<&TurnStartParams>) -> Option<String> {
    launch
        .and_then(|launch| launch.reasoning.as_ref())
        .map(|reasoning| reasoning.effort.trim())
        .filter(|effort| !effort.is_empty())
        .map(str::to_owned)
}

fn effective_task_child_model(
    task: &Task,
    agent_spec: &TaskAgentSpec,
) -> Result<EffectiveAgentModel> {
    let launch = composer_work_launch(task)?;
    let model = launch
        .and_then(|launch| launch.model.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            agent_spec
                .model
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .ok_or_else(|| anyhow!("task agent spec `{}` is missing `model`", agent_spec.id))?;
    let model_provider = launch
        .and_then(|launch| launch.model_provider.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            agent_spec
                .model_provider
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .ok_or_else(|| {
            anyhow!(
                "task agent spec `{}` is missing `model_provider`",
                agent_spec.id
            )
        })?;

    Ok(EffectiveAgentModel {
        model: model.to_owned(),
        model_provider: model_provider.to_owned(),
    })
}

async fn resolve_parent_context(
    processor: &Arc<MessageProcessor>,
    task: &Task,
) -> Result<TaskParentRuntimeContext> {
    let admission = processor
        .crud_store
        .get_task_execution_admission(task.id.as_str())
        .await?
        .with_context(|| {
            format!(
                "agent Task `{}` has no durable execution admission",
                task.id
            )
        })?;
    if admission.workspace_id != task.workspace_id || admission.root_thread_id.trim().is_empty() {
        bail!(
            "agent Task `{}` has an invalid durable execution boundary",
            task.id
        );
    }
    let root_thread_id = admission.root_thread_id;
    let actor_contract = processor
        .crud_store
        .get_task_actor_contract(task.id.as_str())
        .await?
        .with_context(|| format!("agent Task `{}` has no durable actor contract", task.id))?;
    let mut parent_thread_id = task
        .created_by_thread_id
        .clone()
        .unwrap_or_else(|| root_thread_id.clone());
    let mut home_root_thread_id = root_thread_id.clone();

    if let Some(destination_thread_id) = actor_contract.execution_destination_thread_id.as_deref() {
        parent_thread_id = destination_thread_id.to_owned();
        if let Some(route_id) = actor_contract.execution_route_id.as_deref() {
            let database = processor.crud_store.database_connection();
            let route = pioneer_crud::load_agent_delegation_route(&database, route_id)
                .await?
                .with_context(|| format!("Task execution route `{route_id}` is unavailable"))?;
            let projection = pioneer_crud::agent_delegation_route_projection(&route)?;
            let now_millis = pioneer_crud::utc_now().timestamp_millis();
            projection
                .validate(Some(now_millis))
                .map_err(|error| anyhow!("Task execution route is invalid: {error:?}"))?;
            let route_action = actor_contract
                .execution_route_receipt_json
                .as_deref()
                .and_then(|receipt| serde_json::from_str::<serde_json::Value>(receipt).ok())
                .and_then(|receipt| {
                    receipt
                        .get("action")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .and_then(|action| match action.as_str() {
                    "create_task" => Some(pioneer_protocol::AgentActionKind::CreateTask),
                    "schedule_task" => Some(pioneer_protocol::AgentActionKind::ScheduleTask),
                    _ => None,
                })
                .context("Task execution route receipt has an invalid action")?;
            if !projection.status.is_live()
                || projection.destination_thread_id != destination_thread_id
                || projection.source_workspace_id != task.workspace_id
                || projection.destination_workspace_id != task.workspace_id
                || actor_contract.execution_route_expires_at_millis != projection.expires_at
                || actor_contract.execution_route_receipt_json.as_deref()
                    != Some(
                        crate::authorization::safe_route_receipt(
                            &crate::authorization::AgentRouteFacts::from_projection(&projection)
                                .map_err(|message| anyhow!(message))?,
                            route_action,
                        )
                        .as_str(),
                    )
                || !projection.allowed_actions.contains(&match route_action {
                    pioneer_protocol::AgentActionKind::CreateTask => {
                        pioneer_protocol::AgentRouteAction::CreateTask
                    }
                    pioneer_protocol::AgentActionKind::ScheduleTask => {
                        pioneer_protocol::AgentRouteAction::ScheduleTask
                    }
                    _ => unreachable!("route receipt was normalized above"),
                })
            {
                bail!("Task execution route changed after admission");
            }
            match projection.kind {
                pioneer_protocol::AgentRouteKind::ExecutionBound => {
                    if actor_contract.creator
                        != pioneer_protocol::PersistedActorRef::AgentExecution(
                            projection.source_execution_id.clone(),
                        )
                    {
                        bail!("Task execution route is bound to a different creator execution");
                    }
                }
                pioneer_protocol::AgentRouteKind::IdentityBound => {
                    if actor_contract
                        .creator_presentation_snapshot
                        .as_ref()
                        .map(|snapshot| &snapshot.agent_identity_id)
                        != Some(&projection.source_agent_identity_id)
                    {
                        bail!("Task execution route is bound to a different creator identity");
                    }
                }
            }
            let current_generation = processor.current_authorization_revision().await?.max(1);
            if projection.source_policy_generation != current_generation
                || projection.destination_policy_generation != current_generation
            {
                bail!("Task execution route policy generation is stale");
            }
            home_root_thread_id = projection.destination_capsule_id;
        } else if destination_thread_id != root_thread_id {
            bail!("Task destination outside its source capsule requires a durable route");
        }
    }

    let destination_thread = processor
        .crud_store
        .get_thread_model(parent_thread_id.as_str())
        .await?
        .with_context(|| format!("Task destination thread `{parent_thread_id}` is unavailable"))?;
    if destination_thread.workspace_id != task.workspace_id {
        bail!("Task destination thread left its admitted workspace");
    }

    if actor_contract.execution_route_id.is_none() && parent_thread_id != root_thread_id {
        let lineage = processor
            .crud_store
            .get_task_thread_lineage(parent_thread_id.as_str())
            .await?
            .with_context(|| {
                format!(
                    "agent Task `{}` parent thread `{}` has no durable collaboration lineage",
                    task.id, parent_thread_id
                )
            })?;
        if lineage.child_thread_id != parent_thread_id || lineage.root_thread_id != root_thread_id {
            bail!(
                "agent Task `{}` parent thread differs from its admitted collaboration root",
                task.id
            );
        }
    }

    Ok(TaskParentRuntimeContext {
        parent_thread_id,
        parent_turn_id: task.created_by_turn_id.clone(),
        home_root_thread_id,
        root_thread_id,
    })
}

fn task_attachment(task: &Task) -> TaskAttachmentMode {
    task.lifecycle_policy
        .as_ref()
        .map(|policy| policy.attachment)
        .unwrap_or(TaskAttachmentMode::Detached)
}

fn task_hook_runtime_context(
    task: &Task,
    parent: &TaskParentRuntimeContext,
    task_run_turn_kind: TaskRunTurnKind,
) -> AgentTurnHookRuntimeContext {
    if task_attachment(task) == TaskAttachmentMode::Detached {
        if task_run_turn_kind == TaskRunTurnKind::Review {
            AgentTurnHookRuntimeContext::task_in_conversation(
                task.id.clone(),
                parent.parent_thread_id.clone(),
            )
        } else {
            AgentTurnHookRuntimeContext::accepted_result_candidate_in_conversation(
                task.id.clone(),
                parent.parent_thread_id.clone(),
            )
        }
    } else {
        AgentTurnHookRuntimeContext::task(task.id.clone())
    }
}

#[allow(clippy::too_many_arguments)]
async fn load_task_execution_conversation_scope(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    run: &TaskRun,
    parent: &TaskParentRuntimeContext,
    task_run_turn_kind: TaskRunTurnKind,
    execution_thread_id: &str,
    execution_turn_id: &str,
    fallback_model: &str,
    fallback_model_provider: &str,
) -> Result<(
    AgentTurnHookRuntimeContext,
    Vec<pioneer_provider::ChatMessage>,
)> {
    let expected_hook_context = task_hook_runtime_context(task, parent, task_run_turn_kind);
    if let Some(snapshot) = processor
        .crud_store
        .get_turn_runtime_snapshot(execution_turn_id)
        .await?
    {
        if snapshot.workspace_id != task.workspace_id || snapshot.thread_id != execution_thread_id {
            bail!("task child runtime snapshot identity mismatch for turn `{execution_turn_id}`");
        }
        return crate::turn_runtime_snapshot::restored_conversation_scope_from_snapshot(&snapshot)
            .context("failed to restore frozen Task conversation scope");
    }
    if task_attachment(task) != TaskAttachmentMode::Detached {
        return Ok((expected_hook_context, Vec::new()));
    }
    let source_turn_id = task
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.composer_work.as_ref())
        .map(|composer_work| composer_work.launch.turn_id.as_str())
        .or(task.created_by_turn_id.as_deref());
    if let Some(snapshot) = processor
        .crud_store
        .get_task_run_conversation_snapshot(run.id.as_str())
        .await?
    {
        let history =
            restore_task_run_conversation_snapshot(&snapshot, task, parent, source_turn_id)?;
        return Ok((expected_hook_context, history));
    }
    if let Some(retry_of_run_id) = run.retry_of_run_id.as_deref()
        && let Some(snapshot) = processor
            .crud_store
            .get_task_run_conversation_snapshot(retry_of_run_id)
            .await?
    {
        let history =
            restore_task_run_conversation_snapshot(&snapshot, task, parent, source_turn_id)?;
        let persisted = processor
            .crud_store
            .insert_task_run_conversation_snapshot_if_absent(
                pioneer_crud::NewTaskRunConversationSnapshot {
                    run_id: run.id.clone(),
                    task_id: task.id.clone(),
                    workspace_id: task.workspace_id.clone(),
                    conversation_thread_id: parent.parent_thread_id.clone(),
                    source_turn_id: source_turn_id.map(str::to_owned),
                    history_json: serde_json::to_string(&history)
                        .context("failed to serialize inherited Task conversation snapshot")?,
                    created_at: chrono::Utc::now().fixed_offset(),
                },
            )
            .await?;
        let history =
            restore_task_run_conversation_snapshot(&persisted, task, parent, source_turn_id)?;
        return Ok((expected_hook_context, history));
    }
    let history = processor
        .load_conversation_history_for_workspace_in_execution_excluding_turn(
            task.workspace_id.as_str(),
            parent.parent_thread_id.as_str(),
            execution_thread_id,
            execution_turn_id,
            source_turn_id,
            Some(fallback_model),
            Some(fallback_model_provider),
        )
        .await;
    let persisted = processor
        .crud_store
        .insert_task_run_conversation_snapshot_if_absent(
            pioneer_crud::NewTaskRunConversationSnapshot {
                run_id: run.id.clone(),
                task_id: task.id.clone(),
                workspace_id: task.workspace_id.clone(),
                conversation_thread_id: parent.parent_thread_id.clone(),
                source_turn_id: source_turn_id.map(str::to_owned),
                history_json: serde_json::to_string(&history)
                    .context("failed to serialize Task conversation snapshot")?,
                created_at: chrono::Utc::now().fixed_offset(),
            },
        )
        .await?;
    let history = restore_task_run_conversation_snapshot(&persisted, task, parent, source_turn_id)?;
    Ok((expected_hook_context, history))
}

fn restore_task_run_conversation_snapshot(
    snapshot: &pioneer_crud::TaskRunConversationSnapshotRecord,
    task: &Task,
    parent: &TaskParentRuntimeContext,
    source_turn_id: Option<&str>,
) -> Result<Vec<pioneer_provider::ChatMessage>> {
    if snapshot.task_id != task.id
        || snapshot.workspace_id != task.workspace_id
        || snapshot.conversation_thread_id != parent.parent_thread_id
        || snapshot.source_turn_id.as_deref() != source_turn_id
    {
        bail!(
            "Task run `{}` conversation snapshot identity does not match its execution context",
            snapshot.run_id
        );
    }
    serde_json::from_str(snapshot.history_json.as_str())
        .context("failed to restore frozen Task conversation history")
}

async fn ensure_task_run_occurrence_context(
    processor: &Arc<MessageProcessor>,
    task_response: &TaskGetResponse,
    run: &TaskRun,
    execution: &TaskRunExecution,
    agent_spec: &TaskAgentSpec,
    mut parent: TaskParentRuntimeContext,
    permission_profile: &TurnPermissionProfileSnapshot,
) -> Result<TaskParentRuntimeContext> {
    let Some(origin) = task_run_occurrence_origin(task_response, run) else {
        return Ok(parent);
    };
    let effective_model = effective_task_child_model(&task_response.task, agent_spec)?;
    let occurrence_security_snapshot = resolve_task_child_execution_security_snapshot(
        processor,
        task_response.task.workspace_id.as_str(),
        &parent,
        agent_spec,
        permission_profile.clone(),
        effective_model.model_provider.as_str(),
        parent.parent_thread_id.as_str(),
        run.id.as_str(),
    )
    .await
    .with_context(|| {
        format!(
            "failed to resolve task run occurrence execution security for run `{}`",
            run.id
        )
    })?;
    let occurrence_authorization = resolve_task_parent_execution_authorization_context(
        processor,
        &task_response.task,
        &parent,
    )
    .await
    .with_context(|| {
        format!(
            "failed to resolve task run occurrence authorization for run `{}`",
            run.id
        )
    })?;
    ensure_task_run_occurrence_turn(
        processor,
        &task_response.task,
        parent.parent_thread_id.as_str(),
        run,
        execution,
        origin,
        permission_profile,
        &occurrence_security_snapshot,
        &occurrence_authorization.context,
    )
    .await?;
    ensure_task_run_occurrence_anchor(
        processor,
        task_response,
        parent.parent_thread_id.as_str(),
        run.id.as_str(),
    )
    .await?;
    parent.parent_turn_id = Some(run.id.clone());
    Ok(parent)
}

fn task_run_occurrence_origin(
    task_response: &TaskGetResponse,
    run: &TaskRun,
) -> Option<TurnOrigin> {
    let trigger_kind = run
        .trigger_id
        .as_deref()
        .and_then(|trigger_id| find_task_run_trigger(task_response, trigger_id))
        .map(TaskTrigger::kind);
    let attachment = task_response
        .task
        .lifecycle_policy
        .as_ref()
        .map(|policy| policy.attachment)
        .unwrap_or(TaskAttachmentMode::Detached);
    let immediate_attached_to_live_parent = trigger_kind == Some(TaskTriggerKind::Immediate)
        && attachment == TaskAttachmentMode::Attached
        && task_response.task.created_by_turn_id.is_some();
    if immediate_attached_to_live_parent {
        return None;
    }
    if matches!(
        trigger_kind,
        Some(TaskTriggerKind::ScheduledAt | TaskTriggerKind::Interval | TaskTriggerKind::Cron)
    ) {
        return Some(TurnOrigin::ScheduledTask);
    }
    if attachment == TaskAttachmentMode::Detached {
        return Some(TurnOrigin::DetachedTask);
    }
    Some(TurnOrigin::AttachedTask)
}

async fn ensure_task_run_occurrence_turn(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    parent_thread_id: &str,
    run: &TaskRun,
    execution: &TaskRunExecution,
    origin: TurnOrigin,
    permission_profile: &TurnPermissionProfileSnapshot,
    execution_security_snapshot: &TurnExecutionSecuritySnapshot,
    authorization_context: &crate::authorization::ExecutionAuthorizationContext,
) -> Result<()> {
    if processor
        .crud_store
        .get_turn(parent_thread_id, run.id.as_str())
        .await?
        .is_some()
    {
        processor
            .crud_store
            .get_turn_execution_security_snapshot(run.id.as_str())
            .await?
            .with_context(|| {
                format!(
                    "existing task run occurrence `{}` has no durable execution security snapshot",
                    run.id
                )
            })?;
        processor
            .load_turn_execution_authorization_context(run.id.as_str())
            .await
            .with_context(|| {
                format!(
                    "existing task run occurrence `{}` has no valid durable authority envelope",
                    run.id
                )
            })?;
        return Ok(());
    }

    let Some(mut parent_thread) = processor
        .crud_store
        .get_thread_model(parent_thread_id)
        .await?
    else {
        bail!(
            "cannot create task run occurrence turn for task `{}` without parent thread `{}`",
            task.id,
            parent_thread_id
        );
    };
    let now = now_timestamp_secs();
    parent_thread.updated_at = now;
    parent_thread.turns.clear();
    let actor_contract = processor
        .crud_store
        .get_task_actor_contract(task.id.as_str())
        .await?
        .context("agent Task is missing its durable actor contract")?;
    let occurrence_author = task_occurrence_turn_author(&actor_contract, execution.id.as_str())?;
    let occurrence_actor = occurrence_author.actor.clone();
    let occurrence_turn = Turn {
        id: run.id.clone(),
        status: TurnStatus::InProgress,
        turn_kind: TurnKind::TaskRun,
        origin,
        mode: Default::default(),
        author: Some(occurrence_author),
        reply_to_turn_id: None,
        mentions: Vec::new(),
        message_revision: 0,
        message_deleted: false,
        error: None,
        prompt_manifest: None,
        permission_profile: permission_profile.clone(),
    };
    let sandbox_mode = processor
        .crud_store
        .get_thread_sandbox_mode(parent_thread_id)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "cannot create task run occurrence turn for task `{}` because parent thread `{}` is missing its persisted sandbox policy",
                task.id,
                parent_thread_id
            )
        })?;
    let profile_selected_audit = processor.turn_profile_selected_audit_event_for_turn(
        task.workspace_id.as_str(),
        parent_thread_id,
        run.id.as_str(),
        permission_profile.clone(),
    );
    let occurrence_authority_json = authorization_context
        .to_persisted_json()
        .context("failed to encode task run occurrence authority envelope")?;
    processor
        .crud_store
        .materialize_authorized_turn_start_with_reasoning_effort_and_permission_audit(
            &parent_thread,
            sandbox_mode,
            &occurrence_turn,
            &[],
            None,
            occurrence_actor,
            profile_selected_audit,
            occurrence_authority_json.as_str(),
            None,
            None,
            None,
            execution_security_snapshot,
            processor.turn_security_audit_events_for_turn(
                task.workspace_id.as_str(),
                parent_thread_id,
                run.id.as_str(),
                execution_security_snapshot,
            ),
            None,
            None,
        )
        .await
        .with_context(|| {
            format!(
                "failed to persist task run occurrence turn and permission audit `{}` for task `{}`",
                run.id, task.id
            )
        })?;
    if let Err(error) =
        register_resolved_task_child_execution_lease(processor, run.id.as_str()).await
    {
        let reason = "task_occurrence_execution_security_persist_failed".to_owned();
        if !processor
            .mark_turn_blocked(parent_thread_id.to_owned(), run.id.clone(), reason.clone())
            .await
        {
            warn!(
                thread_id = parent_thread_id,
                turn_id = run.id,
                failure_class = "task_occurrence_execution_security_close_failed",
                "failed to durably close task run occurrence after admission failure"
            );
        }
        return Err(error).context(reason);
    }
    processor
        .send_notification_to_thread_subscribers(
            parent_thread_id,
            events::TURN_STARTED,
            &TurnStartedNotification {
                workspace_id: task.workspace_id.clone(),
                thread_id: parent_thread_id.to_owned(),
                turn: occurrence_turn,
            },
        )
        .await;
    Ok(())
}

async fn ensure_task_run_occurrence_anchor(
    processor: &Arc<MessageProcessor>,
    task_response: &TaskGetResponse,
    parent_thread_id: &str,
    occurrence_turn_id: &str,
) -> Result<()> {
    let item = crate::task_tools::task_turn_item_from_response_for_run(
        processor,
        task_response,
        occurrence_turn_id,
        crate::task_tools::task_run_anchor_id(occurrence_turn_id),
    )
    .await?;
    if processor
        .crud_store
        .get_turn_item(occurrence_turn_id, item.id.as_str())
        .await?
        .is_some()
    {
        return Ok(());
    }

    let now = now_timestamp_secs();
    let item = pioneer_protocol::TurnItem::Task { item };
    let started = ItemStartedNotification {
        workspace_id: task_response.task.workspace_id.clone(),
        thread_id: parent_thread_id.to_owned(),
        turn_id: occurrence_turn_id.to_owned(),
        item: item.clone(),
    };
    processor
        .crud_store
        .materialize_item_started(started.clone(), now)
        .await
        .with_context(|| {
            format!("failed to persist task run occurrence anchor for turn `{occurrence_turn_id}`")
        })?;
    processor
        .send_notification_to_thread_subscribers(parent_thread_id, events::ITEM_STARTED, &started)
        .await;
    processor
        .notify_semantic_timeline_item_changed(
            started.workspace_id.as_str(),
            started.thread_id.as_str(),
            started.turn_id.as_str(),
            &started.item,
            Some("in_progress"),
        )
        .await;

    let completed = ItemCompletedNotification {
        workspace_id: task_response.task.workspace_id.clone(),
        thread_id: parent_thread_id.to_owned(),
        turn_id: occurrence_turn_id.to_owned(),
        item,
    };
    processor
        .crud_store
        .materialize_item_completed(completed.clone(), now)
        .await
        .with_context(|| {
            format!("failed to complete task run occurrence anchor for turn `{occurrence_turn_id}`")
        })?;
    processor
        .send_notification_to_thread_subscribers(
            parent_thread_id,
            events::ITEM_COMPLETED,
            &completed,
        )
        .await;
    processor
        .notify_semantic_timeline_item_changed(
            completed.workspace_id.as_str(),
            completed.thread_id.as_str(),
            completed.turn_id.as_str(),
            &completed.item,
            None,
        )
        .await;
    Ok(())
}

async fn mark_task_run_occurrence_turn_completed(
    processor: &Arc<MessageProcessor>,
    lineage: &TaskThreadLineage,
) -> Result<()> {
    mark_task_run_occurrence_turn_terminal(
        processor,
        lineage,
        TurnStatus::Completed,
        None,
        now_timestamp_secs(),
    )
    .await
}

async fn mark_task_run_occurrence_turn_failed(
    processor: &Arc<MessageProcessor>,
    lineage: &TaskThreadLineage,
    error_message: &str,
) -> Result<()> {
    mark_task_run_occurrence_turn_terminal(
        processor,
        lineage,
        TurnStatus::Failed,
        Some(error_message.to_owned()),
        now_timestamp_secs(),
    )
    .await
}

async fn mark_task_run_occurrence_turn_blocked(
    processor: &Arc<MessageProcessor>,
    lineage: &TaskThreadLineage,
    reason: &str,
) -> Result<()> {
    mark_task_run_occurrence_turn_terminal(
        processor,
        lineage,
        TurnStatus::Blocked,
        Some(reason.to_owned()),
        now_timestamp_secs(),
    )
    .await
}

async fn mark_task_run_occurrence_turn_terminal(
    processor: &Arc<MessageProcessor>,
    lineage: &TaskThreadLineage,
    status: TurnStatus,
    error: Option<String>,
    completed_at: i64,
) -> Result<()> {
    let Some(parent_turn_id) = lineage.created_by_turn_id.as_deref() else {
        return Ok(());
    };
    let parent_thread_id = lineage
        .created_by_thread_id
        .as_deref()
        .unwrap_or(lineage.parent_thread_id.as_str());
    let Some((workspace_id, mut turn)) = processor
        .crud_store
        .get_turn(parent_thread_id, parent_turn_id)
        .await?
    else {
        return Ok(());
    };
    if turn.turn_kind != TurnKind::TaskRun || turn.status != TurnStatus::InProgress {
        return Ok(());
    }
    turn.status = status;
    turn.error = error;
    match status {
        TurnStatus::Completed => {
            let notification = TurnCompletedNotification {
                workspace_id,
                thread_id: parent_thread_id.to_owned(),
                turn,
            };
            processor
                .crud_store
                .materialize_turn_completed(notification.clone(), completed_at)
                .await?;
            processor
                .send_notification_to_thread_subscribers(
                    parent_thread_id,
                    events::TURN_COMPLETED,
                    &notification,
                )
                .await;
            processor
                .notify_semantic_timeline_turn_state_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn.id.as_str(),
                )
                .await;
        }
        TurnStatus::Failed | TurnStatus::Interrupted => {
            let notification = TurnFailedNotification {
                workspace_id,
                thread_id: parent_thread_id.to_owned(),
                turn,
            };
            processor
                .crud_store
                .materialize_turn_failed(notification.clone(), completed_at)
                .await?;
            processor
                .send_notification_to_thread_subscribers(
                    parent_thread_id,
                    events::TURN_FAILED,
                    &notification,
                )
                .await;
            processor
                .notify_semantic_timeline_turn_state_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn.id.as_str(),
                )
                .await;
        }
        TurnStatus::Blocked => {
            let notification = TurnBlockedNotification {
                workspace_id,
                thread_id: parent_thread_id.to_owned(),
                turn,
                resume: None,
            };
            processor
                .crud_store
                .materialize_turn_blocked(notification.clone(), completed_at)
                .await?;
            processor
                .send_notification_to_thread_subscribers(
                    parent_thread_id,
                    events::TURN_BLOCKED,
                    &notification,
                )
                .await;
            processor
                .notify_semantic_timeline_turn_state_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn.id.as_str(),
                )
                .await;
        }
        TurnStatus::InProgress => {}
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct TaskRunChildRuntime {
    lineage: TaskThreadLineage,
    task_run_turn: TaskRunTurn,
}

async fn load_child_runtime_for_run(
    processor: &Arc<MessageProcessor>,
    run_id: &str,
) -> Result<Option<TaskRunChildRuntime>> {
    let Some(task_run_turn) = processor
        .crud_store
        .list_task_run_turns(run_id)
        .await?
        .into_iter()
        .rev()
        .find(|turn| turn.kind != TaskRunTurnKind::Review)
    else {
        return Ok(None);
    };
    load_child_runtime_from_task_run_turn(processor, task_run_turn)
        .await
        .map(Some)
}

async fn load_child_runtime_for_turn(
    processor: &Arc<MessageProcessor>,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<TaskRunChildRuntime>> {
    if let Some(task_run_turn) = processor
        .crud_store
        .get_task_run_turn_by_turn(thread_id, turn_id)
        .await?
    {
        return load_child_runtime_from_task_run_turn(processor, task_run_turn)
            .await
            .map(Some);
    }
    Ok(None)
}

async fn load_child_runtime_from_task_run_turn(
    processor: &Arc<MessageProcessor>,
    task_run_turn: TaskRunTurn,
) -> Result<TaskRunChildRuntime> {
    if task_run_turn.kind != TaskRunTurnKind::Review
        && let Some(binding) = processor
            .crud_store
            .get_task_run_primary_thread_binding(task_run_turn.run_id.as_str())
            .await?
        && binding.thread_id != task_run_turn.thread_id
    {
        bail!(
            "primary task run thread binding `{}` points to `{}`, but task run turn `{}` points to `{}`",
            binding.id,
            binding.thread_id,
            task_run_turn.id,
            task_run_turn.thread_id
        );
    }

    let lineage = load_required_task_thread_lineage(processor, &task_run_turn).await?;
    Ok(TaskRunChildRuntime {
        lineage,
        task_run_turn,
    })
}

async fn load_required_task_thread_lineage(
    processor: &Arc<MessageProcessor>,
    task_run_turn: &TaskRunTurn,
) -> Result<TaskThreadLineage> {
    let lineage = processor
        .crud_store
        .get_task_thread_lineage(task_run_turn.thread_id.as_str())
        .await?
        .with_context(|| {
            format!(
                "task run turn `{}` has no durable collaboration lineage for child thread `{}`",
                task_run_turn.id, task_run_turn.thread_id
            )
        })?;
    validate_task_thread_lineage(task_run_turn, lineage)
}

fn validate_task_thread_lineage(
    task_run_turn: &TaskRunTurn,
    lineage: TaskThreadLineage,
) -> Result<TaskThreadLineage> {
    if lineage.child_thread_id != task_run_turn.thread_id
        || lineage.child_thread_id.trim().is_empty()
        || lineage.parent_thread_id.trim().is_empty()
        || lineage.root_thread_id.trim().is_empty()
        || lineage.root_thread_id == lineage.child_thread_id
    {
        bail!(
            "task run turn `{}` has an invalid durable collaboration lineage",
            task_run_turn.id
        );
    }
    Ok(lineage)
}

async fn load_execution_checkpoint_context_for_turn(
    processor: &Arc<MessageProcessor>,
    turn_id: &str,
) -> Result<Option<ExecutionCheckpointContext>> {
    let Some(checkpoint) = processor
        .crud_store
        .latest_turn_execution_checkpoint_for_turn(turn_id)
        .await?
    else {
        return Ok(None);
    };
    let Some(window) = processor
        .crud_store
        .get_turn_execution_window(checkpoint.window_id.as_str())
        .await?
    else {
        return Ok(None);
    };
    if window.turn_id != turn_id {
        warn!(
            turn_id,
            checkpoint_id = %checkpoint.id,
            window_turn_id = %window.turn_id,
            "skipping execution checkpoint whose window belongs to another turn"
        );
        return Ok(None);
    }
    let payload =
        match serde_json::from_value::<ExecutionCheckpointPayload>(checkpoint.payload_json.clone())
        {
            Ok(payload) => payload,
            Err(_error) => {
                warn!(
                    turn_id,
                    checkpoint_id = %checkpoint.id,
                    failure_class = "task_recovery_checkpoint_invalid",
                    "skipping invalid execution checkpoint payload during task child recovery"
                );
                return Ok(None);
            }
        };
    // `ExecutionCheckpointContext::window_id` is the provider/runtime window
    // identity used by continuation events, not the database row primary key
    // stored in `checkpoint.window_id`.  The transition validator compares it
    // with the immutable runtimeWindowId written by the Started event.
    let runtime_window_id = window
        .metadata_json
        .get("runtimeWindowId")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            payload
                .window
                .window_id
                .as_ref()
                .filter(|id| id.as_str() != window.id.as_str())
                .cloned()
        })
        .unwrap_or_else(|| format!("{turn_id}:window:{}", window.window_index));
    Ok(Some(ExecutionCheckpointContext {
        window_id: runtime_window_id,
        window_index: window.window_index,
        checkpoint_id: checkpoint.id,
        checkpoint_kind: task_execution_checkpoint_kind_label(checkpoint.checkpoint_kind),
        payload,
        usage: crate::turn_runtime_snapshot::execution_window_usage_snapshot(
            processor.crud_store.as_ref(),
            turn_id,
        )
        .await?,
    }))
}

fn task_execution_checkpoint_kind_label(kind: pioneer_crud::TurnExecutionCheckpointKind) -> String {
    match kind {
        pioneer_crud::TurnExecutionCheckpointKind::WindowExhausted => "window_exhausted",
        pioneer_crud::TurnExecutionCheckpointKind::TurnBlocked => "turn_blocked",
        pioneer_crud::TurnExecutionCheckpointKind::StartupRecovery => "startup_recovery",
    }
    .to_owned()
}

fn task_run_primary_binding_from_turn(
    task: &Task,
    run: &TaskRun,
    execution: &TaskRunExecution,
    task_run_turn: &TaskRunTurn,
    created_at: i64,
) -> TaskRunThreadBinding {
    TaskRunThreadBinding {
        id: primary_task_run_thread_binding_id(run.id.as_str()),
        task_id: task.id.clone(),
        run_id: run.id.clone(),
        execution_id: Some(execution.id.clone()),
        thread_id: task_run_turn.thread_id.clone(),
        binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
        created_at,
    }
}

fn initial_task_run_turn_from_execution(
    task: &Task,
    run: &TaskRun,
    execution: &TaskRunExecution,
    created_at: i64,
) -> TaskRunTurn {
    let child_thread_id = pioneer_protocol::generate_id(21);
    let child_turn_id = pioneer_protocol::generate_id(21);
    TaskRunTurn {
        id: task_run_turn_id_for_turn(child_turn_id.as_str()),
        task_id: task.id.clone(),
        run_id: run.id.clone(),
        execution_id: Some(execution.id.clone()),
        thread_id: child_thread_id,
        turn_id: child_turn_id,
        kind: TaskRunTurnKind::Initial,
        round: 0,
        sequence: 0,
        status: TaskRunTurnStatus::InProgress,
        reviews_candidate_id: None,
        requested_by_candidate_id: None,
        requested_by_review_event_id: None,
        created_at,
        started_at: Some(created_at),
        completed_at: None,
    }
}

fn lineage_from_task_run_turn(
    _task: &Task,
    _run: &TaskRun,
    agent_spec: &TaskAgentSpec,
    parent: &TaskParentRuntimeContext,
    task_run_turn: &TaskRunTurn,
    created_at: i64,
) -> TaskThreadLineage {
    TaskThreadLineage {
        child_thread_id: task_run_turn.thread_id.clone(),
        parent_thread_id: parent.parent_thread_id.clone(),
        root_thread_id: parent.home_root_thread_id.clone(),
        depth: agent_spec.depth,
        origin_kind: Some("task_run".to_owned()),
        created_by_thread_id: Some(parent.parent_thread_id.clone()),
        created_by_turn_id: parent.parent_turn_id.clone(),
        created_at,
    }
}

fn primary_task_run_thread_binding_id(run_id: &str) -> String {
    format!("trb_primary_{run_id}")
}

fn task_run_turn_id_for_turn(turn_id: &str) -> String {
    format!("trt_{turn_id}")
}

fn candidate_created_task_run_turn(task_run_turn: &TaskRunTurn, completed_at: i64) -> TaskRunTurn {
    let mut completed = task_run_turn.clone();
    completed.status = TaskRunTurnStatus::CandidateCreated;
    completed.completed_at = Some(completed_at);
    completed
}

fn failed_task_run_turn(
    task_run_turn: &TaskRunTurn,
    status: TaskRunTurnStatus,
    completed_at: i64,
) -> TaskRunTurn {
    let mut failed = task_run_turn.clone();
    failed.status = status;
    failed.completed_at = Some(completed_at);
    failed
}

fn blocked_task_run_turn(task_run_turn: &TaskRunTurn, completed_at: i64) -> TaskRunTurn {
    let mut blocked = task_run_turn.clone();
    blocked.status = TaskRunTurnStatus::Blocked;
    blocked.completed_at = Some(completed_at);
    blocked
}

fn revision_dispatch_error_details(task_run_turn: &TaskRunTurn) -> TaskValue {
    TaskValue::Object(BTreeMap::from([
        (
            "taskRunTurnId".to_owned(),
            TaskValue::String(task_run_turn.id.clone()),
        ),
        (
            "threadId".to_owned(),
            TaskValue::String(task_run_turn.thread_id.clone()),
        ),
        (
            "turnId".to_owned(),
            TaskValue::String(task_run_turn.turn_id.clone()),
        ),
        (
            "previousCandidateId".to_owned(),
            task_run_turn
                .requested_by_candidate_id
                .clone()
                .map(TaskValue::String)
                .unwrap_or(TaskValue::Null),
        ),
        (
            "requestedByReviewEventId".to_owned(),
            task_run_turn
                .requested_by_review_event_id
                .clone()
                .map(TaskValue::String)
                .unwrap_or(TaskValue::Null),
        ),
    ]))
}

async fn record_task_run_turn_failure(
    handle: &TaskExecutionHandle,
    task_run_turn: &TaskRunTurn,
    status: TaskRunTurnStatus,
    error: Option<TaskError>,
    completed_at: i64,
) -> Result<()> {
    handle
        .record_task_run_turn_failed(
            failed_task_run_turn(task_run_turn, status, completed_at),
            error,
            completed_at,
        )
        .await?;
    Ok(())
}

fn accepted_result_candidate(
    task_run_turn: &TaskRunTurn,
    result: TaskResult,
    accepted_at: i64,
) -> TaskResultCandidate {
    let review_event_id = runtime_auto_accept_review_event_id(
        task_run_turn.run_id.as_str(),
        task_run_turn.turn_id.as_str(),
    );
    TaskResultCandidate {
        id: task_result_candidate_id(
            task_run_turn.run_id.as_str(),
            task_run_turn.turn_id.as_str(),
        ),
        task_id: task_run_turn.task_id.clone(),
        run_id: task_run_turn.run_id.clone(),
        task_run_turn_id: task_run_turn.id.clone(),
        thread_id: task_run_turn.thread_id.clone(),
        turn_id: task_run_turn.turn_id.clone(),
        round: task_run_turn.round,
        status: TaskResultCandidateStatus::Accepted,
        summary: result.summary.clone(),
        result: Some(result),
        extraction_error: None,
        diagnostics: Vec::new(),
        final_review_event_id: Some(review_event_id),
        created_at: accepted_at,
        updated_at: accepted_at,
        resolved_at: Some(accepted_at),
    }
}

fn pending_review_result_candidate(
    task_run_turn: &TaskRunTurn,
    result: TaskResult,
    created_at: i64,
) -> TaskResultCandidate {
    TaskResultCandidate {
        id: task_result_candidate_id(
            task_run_turn.run_id.as_str(),
            task_run_turn.turn_id.as_str(),
        ),
        task_id: task_run_turn.task_id.clone(),
        run_id: task_run_turn.run_id.clone(),
        task_run_turn_id: task_run_turn.id.clone(),
        thread_id: task_run_turn.thread_id.clone(),
        turn_id: task_run_turn.turn_id.clone(),
        round: task_run_turn.round,
        status: TaskResultCandidateStatus::PendingReview,
        summary: result.summary.clone(),
        result: Some(result),
        extraction_error: None,
        diagnostics: Vec::new(),
        final_review_event_id: None,
        created_at,
        updated_at: created_at,
        resolved_at: None,
    }
}

fn extraction_failed_result_candidate(
    task_run_turn: &TaskRunTurn,
    error: TaskError,
    created_at: i64,
) -> TaskResultCandidate {
    let diagnostics = error
        .details
        .as_ref()
        .and_then(extraction_diagnostics_from_error_details)
        .unwrap_or_default();
    TaskResultCandidate {
        id: task_result_candidate_id(
            task_run_turn.run_id.as_str(),
            task_run_turn.turn_id.as_str(),
        ),
        task_id: task_run_turn.task_id.clone(),
        run_id: task_run_turn.run_id.clone(),
        task_run_turn_id: task_run_turn.id.clone(),
        thread_id: task_run_turn.thread_id.clone(),
        turn_id: task_run_turn.turn_id.clone(),
        round: task_run_turn.round,
        status: TaskResultCandidateStatus::ExtractionFailed,
        summary: Some(error.message.clone()),
        result: None,
        extraction_error: Some(error),
        diagnostics,
        final_review_event_id: None,
        created_at,
        updated_at: created_at,
        resolved_at: None,
    }
}

fn revision_possible(review_policy: &TaskAgentReviewPolicy, task_run_turn: &TaskRunTurn) -> bool {
    task_run_turn.round < review_policy.max_revision_rounds
}

fn invalid_structured_result_error(
    result: &TaskResult,
    agent_spec: &TaskAgentSpec,
    run_id: &str,
) -> Option<TaskError> {
    agent_spec.result_contract.as_ref()?;
    let TaskValue::Object(data) = result.data.as_ref()? else {
        return None;
    };
    let fallback_used = matches!(data.get("fallbackUsed"), Some(TaskValue::Bool(true)));
    let schema_invalid = matches!(data.get("schemaValid"), Some(TaskValue::Bool(false)));
    if !fallback_used && !schema_invalid {
        return None;
    }
    let diagnostics = data
        .get("diagnostics")
        .and_then(task_value_string_list)
        .unwrap_or_default();
    let message = if diagnostics.is_empty() {
        "child task result did not satisfy the result contract".to_owned()
    } else {
        format!(
            "child task result did not satisfy the result contract: {}",
            diagnostics.join("; ")
        )
    };
    Some(TaskError {
        code: "task_agent_result_extraction_failed".to_owned(),
        message,
        class: TaskErrorClass::Validation,
        details: Some(TaskValue::Object(BTreeMap::from([
            ("schemaValid".to_owned(), TaskValue::Bool(!schema_invalid)),
            ("fallbackUsed".to_owned(), TaskValue::Bool(fallback_used)),
            (
                "diagnostics".to_owned(),
                TaskValue::List(diagnostics.into_iter().map(TaskValue::String).collect()),
            ),
        ]))),
        failed_run_id: Some(run_id.to_owned()),
    })
}

fn task_value_string_list(value: &TaskValue) -> Option<Vec<String>> {
    let TaskValue::List(items) = value else {
        return None;
    };
    Some(
        items
            .iter()
            .filter_map(|item| match item {
                TaskValue::String(value) => Some(value.clone()),
                _ => None,
            })
            .collect(),
    )
}

fn extraction_diagnostics_from_error_details(details: &TaskValue) -> Option<Vec<String>> {
    let TaskValue::Object(object) = details else {
        return None;
    };
    object.get("diagnostics").and_then(task_value_string_list)
}

fn runtime_auto_accept_review_event(
    candidate: &TaskResultCandidate,
    accepted_at: i64,
) -> TaskResultReviewEvent {
    TaskResultReviewEvent {
        id: candidate.final_review_event_id.clone().unwrap_or_else(|| {
            runtime_auto_accept_review_event_id(
                candidate.run_id.as_str(),
                candidate.turn_id.as_str(),
            )
        }),
        candidate_id: candidate.id.clone(),
        task_id: candidate.task_id.clone(),
        run_id: candidate.run_id.clone(),
        task_run_turn_id: candidate.task_run_turn_id.clone(),
        reviewer_kind: TaskResultReviewerKind::RuntimeAuto,
        reviewer: pioneer_protocol::TaskResultReviewerRef::RuntimePolicy,
        reviewer_thread_id: None,
        reviewer_turn_id: None,
        reviewer_user_id: None,
        reviewer_agent_spec_id: None,
        event_kind: TaskResultReviewEventKind::SystemAuto,
        decision: TaskResultReviewDecision::Accept,
        feedback_text: None,
        feedback: None,
        confidence: None,
        supersedes_review_event_id: None,
        next_task_run_turn_id: None,
        created_at: accepted_at,
    }
}

fn task_result_candidate_id(run_id: &str, turn_id: &str) -> String {
    format!("trc_{run_id}_{turn_id}")
}

fn runtime_auto_accept_review_event_id(run_id: &str, turn_id: &str) -> String {
    format!("trre_auto_{run_id}_{turn_id}")
}

fn task_run_turn_terminal_status_from_child_turn_status(
    status: TurnStatus,
) -> Option<TaskRunTurnStatus> {
    match status {
        TurnStatus::Failed => Some(TaskRunTurnStatus::Failed),
        TurnStatus::Interrupted => Some(TaskRunTurnStatus::Interrupted),
        TurnStatus::Blocked => Some(TaskRunTurnStatus::Blocked),
        TurnStatus::Completed | TurnStatus::InProgress => None,
    }
}

fn spawn_execution_heartbeat(
    processor: &Arc<MessageProcessor>,
    execution_id: String,
    child_thread_id: String,
    child_turn_id: String,
    run_id: String,
    liveness_timeouts: (i64, i64),
) {
    let processor = Arc::downgrade(processor);
    tokio::spawn(async move {
        let Some(owner) = processor.upgrade() else {
            return;
        };
        let owner = owner.with_database_class(SqliteWriteClass::Critical);
        let Ok(Some(resource_state)) = pioneer_crud::load_agent_execution_resource_state(
            &owner.crud_store.database_connection(),
            execution_id.as_str(),
        )
        .await
        else {
            return;
        };
        let Ok(attempt_generation) = u64::try_from(resource_state.attempt_generation) else {
            return;
        };
        drop(owner);
        let typed_execution_id = AgentExecutionId::new(execution_id.clone()).ok();
        let liveness_started_at = chrono::Utc::now().fixed_offset();
        let mut liveness = typed_execution_id.clone().and_then(|execution_id| {
            crate::authorization::ExecutionLivenessAdapter::new(
                execution_id,
                attempt_generation,
                Some(liveness_started_at + chrono::Duration::seconds(liveness_timeouts.0.max(1))),
                Some(
                    liveness_started_at
                        + chrono::Duration::seconds(liveness_timeouts.1.max(liveness_timeouts.0)),
                ),
                u64::try_from(TASK_EXECUTION_LEASE_SECONDS).unwrap_or_default(),
            )
            .ok()
        });
        loop {
            sleep(Duration::from_secs(TASK_EXECUTION_HEARTBEAT_SECONDS)).await;
            let Some(processor) = processor.upgrade() else {
                break;
            };
            let processor = processor.with_database_class(SqliteWriteClass::Critical);
            let Ok(Some(execution)) = processor.crud_store.load_execution_for_run(&run_id).await
            else {
                break;
            };
            if execution.id != execution_id || execution.status.is_terminal() {
                break;
            }
            let Ok(turn) = processor
                .crud_store
                .get_turn(child_thread_id.as_str(), child_turn_id.as_str())
                .await
            else {
                break;
            };
            let Some((_, turn)) = turn else {
                break;
            };
            if turn.status != TurnStatus::InProgress {
                break;
            }
            if let (Some(liveness), Some(typed_execution_id)) =
                (liveness.as_mut(), typed_execution_id.as_ref())
            {
                let observation = liveness.observe(
                    typed_execution_id,
                    attempt_generation,
                    crate::authorization::ExecutionObservation::Heartbeat,
                    chrono::Utc::now().fixed_offset(),
                );
                if matches!(
                    observation,
                    crate::authorization::ExecutionLivenessDecision::StaleAttempt
                ) {
                    break;
                }
            }
            let now = now_timestamp_secs();
            let idle_lease_secs = TASK_EXECUTION_LEASE_SECONDS.max(liveness_timeouts.0.max(1));
            let heartbeat = processor
                .crud_store
                .heartbeat_execution_for_agent_attempt(
                    execution_id.as_str(),
                    resource_state.attempt_generation,
                    now,
                    Some(now.saturating_add(idle_lease_secs)),
                )
                .await;
            if !matches!(heartbeat, Ok(Some(_))) {
                break;
            }
        }
    });
}

struct ResolvedTaskExecutionTurnSettings {
    model: EffectiveAgentModel,
    execution_backend: AgentExecutionBackend,
    cli_runtime: Option<(String, CLIAgentRuntimeKind)>,
    capabilities: Vec<pioneer_protocol::TurnCapability>,
    reasoning: Option<pioneer_protocol::TurnReasoningSelection>,
    permission_selection: Option<pioneer_protocol::TurnPermissionProfileSelection>,
    permission_profile: TurnPermissionProfileSnapshot,
}

async fn resolved_task_execution_turn_settings(
    processor: &MessageProcessor,
    task: &Task,
    agent_spec: &TaskAgentSpec,
    facts: &AgentExecutionPersistenceFacts,
    launch: Option<&pioneer_protocol::AgentLaunchSelection>,
) -> Result<ResolvedTaskExecutionTurnSettings> {
    let (execution_backend, cli_runtime) = match &facts.profile.backend {
        AgentExecutionProfileBackend::ApiProvider => (
            AgentExecutionBackend::ApiProvider {
                provider: facts.profile.provider_id.clone(),
            },
            None,
        ),
        AgentExecutionProfileBackend::CliRuntime {
            runtime_instance_id,
        } => {
            let runtime = processor
                .load_cli_runtime_instances()?
                .into_iter()
                .find(|runtime| runtime.id == *runtime_instance_id && runtime.enabled)
                .context("pinned Task CLI runtime is unavailable")?;
            let runtime_kind = match runtime.kind {
                pioneer_config::GatewayCliAgentRuntimeKindConfig::Codex => {
                    CLIAgentRuntimeKind::Codex
                }
                pioneer_config::GatewayCliAgentRuntimeKindConfig::Claude => {
                    CLIAgentRuntimeKind::Claude
                }
            };
            (
                AgentExecutionBackend::CLIAgentRuntime {
                    runtime_id: runtime_instance_id.clone(),
                    runtime_kind,
                },
                Some((runtime_instance_id.clone(), runtime_kind)),
            )
        }
        AgentExecutionProfileBackend::AcpAgentRuntime { runtime_id } => (
            AgentExecutionBackend::ACPAgentRuntime {
                runtime_id: runtime_id.clone(),
            },
            None,
        ),
    };
    let capabilities = match launch {
        Some(launch) => {
            let requested =
                super::agent_action_tools::launch_selection_capabilities(&launch.execution)
                    .context("pinned Task launch capabilities are invalid")?;
            processor
                .normalize_turn_skill_capabilities(task.workspace_id.as_str(), requested.as_slice())
                .await
                .map_err(|message| anyhow!(message))
                .context("pinned Task launch capabilities are unavailable")?
                .execution
        }
        None => Vec::new(),
    };
    let permission_selection =
        launch.and_then(|launch| launch.execution.permission_profile.clone());
    let launch_permission_profile = permission_selection
        .as_ref()
        .map(|selection| pioneer_protocol::resolve_turn_permission_profile(Some(selection)));
    let permission_profile =
        effective_task_child_permission_profile(agent_spec, launch_permission_profile.as_ref())?;
    Ok(ResolvedTaskExecutionTurnSettings {
        model: EffectiveAgentModel {
            model: facts.profile.model_id.clone(),
            model_provider: facts.profile.provider_id.clone(),
        },
        execution_backend,
        cli_runtime,
        capabilities,
        reasoning: launch.and_then(|launch| launch.execution.reasoning.clone()),
        permission_selection,
        permission_profile,
    })
}

fn effective_task_child_permission_profile(
    agent_spec: &TaskAgentSpec,
    launch_profile: Option<&TurnPermissionProfileSnapshot>,
) -> Result<TurnPermissionProfileSnapshot> {
    let cap = agent_spec.permission_cap.as_ref().ok_or_else(|| {
        anyhow!(
            "task agent spec `{}` is missing permission_cap",
            agent_spec.id
        )
    })?;
    let cap_profile = pioneer_protocol::task_permission_cap_snapshot(cap);
    let launcher = launch_profile
        .cloned()
        .unwrap_or_else(pioneer_protocol::default_turn_permission_profile_snapshot);
    let mut profile = pioneer_protocol::intersect_turn_permission_profiles(
        &cap_profile,
        &launcher,
        TurnPermissionProfileSource::TaskPermissionCap,
    );
    if let Some(tool_policy) = agent_spec.tool_policy.as_ref() {
        apply_task_tool_policy_to_permission_profile(&mut profile, tool_policy);
    }
    Ok(profile)
}

async fn resolve_task_child_execution_security_snapshot(
    processor: &Arc<MessageProcessor>,
    workspace_id: &str,
    parent: &TaskParentRuntimeContext,
    agent_spec: &TaskAgentSpec,
    child_permission_profile: TurnPermissionProfileSnapshot,
    effective_model_provider: &str,
    child_thread_id: &str,
    child_turn_id: &str,
) -> Result<TurnExecutionSecuritySnapshot> {
    let security_cap = agent_spec.security_cap.as_ref().ok_or_else(|| {
        anyhow!(
            "task agent spec `{}` is missing security_cap",
            agent_spec.id
        )
    })?;
    let parent_turn_id = parent.parent_turn_id.as_deref().ok_or_else(|| {
        anyhow!(
            "task agent spec `{}` cannot start child turn without parent turn security snapshot",
            agent_spec.id
        )
    })?;
    let parent_snapshot = processor
        .crud_store
        .get_turn_execution_security_snapshot(parent_turn_id)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "parent turn `{}` is missing execution security snapshot for task agent spec `{}`",
                parent_turn_id,
                agent_spec.id
            )
        })?
        .snapshot;

    let mut snapshot = crate::turn_security::resolve_task_child_execution_security(
        workspace_id,
        parent_turn_id,
        &parent_snapshot,
        security_cap,
        child_permission_profile,
        effective_model_provider.to_owned(),
        child_thread_id.to_owned(),
        child_turn_id.to_owned(),
        now_timestamp_secs().saturating_mul(1000),
    )?;
    processor.add_native_turn_runtime_sandbox_roots(
        &mut snapshot,
        workspace_id,
        child_thread_id,
        child_turn_id,
    )?;
    Ok(snapshot)
}

async fn resolve_task_child_cli_execution_security_snapshot(
    processor: &Arc<MessageProcessor>,
    workspace_id: &str,
    parent: &TaskParentRuntimeContext,
    agent_spec: &TaskAgentSpec,
    child_permission_profile: TurnPermissionProfileSnapshot,
    runtime_id: &str,
    runtime_kind: CLIAgentRuntimeKind,
    child_thread_id: &str,
    child_turn_id: &str,
) -> Result<TurnExecutionSecuritySnapshot> {
    let security_cap = agent_spec.security_cap.as_ref().ok_or_else(|| {
        anyhow!(
            "task agent spec `{}` is missing security_cap",
            agent_spec.id
        )
    })?;
    let parent_turn_id = parent.parent_turn_id.as_deref().ok_or_else(|| {
        anyhow!(
            "task agent spec `{}` cannot start child turn without parent turn security snapshot",
            agent_spec.id
        )
    })?;
    let parent_snapshot = processor
        .crud_store
        .get_turn_execution_security_snapshot(parent_turn_id)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "parent turn `{}` is missing execution security snapshot for task agent spec `{}`",
                parent_turn_id,
                agent_spec.id
            )
        })?
        .snapshot;
    let execution_backend = match runtime_kind {
        CLIAgentRuntimeKind::Codex => {
            crate::turn_security::TurnSecurityResolverExecutionBackend::CodexCli {
                runtime_id: runtime_id.to_owned(),
            }
        }
        CLIAgentRuntimeKind::Claude => {
            crate::turn_security::TurnSecurityResolverExecutionBackend::ClaudeCli {
                runtime_id: runtime_id.to_owned(),
            }
        }
    };
    crate::turn_security::resolve_task_child_execution_security_for_backend(
        workspace_id,
        parent_turn_id,
        &parent_snapshot,
        security_cap,
        child_permission_profile,
        execution_backend,
        child_thread_id.to_owned(),
        child_turn_id.to_owned(),
        now_timestamp_secs().saturating_mul(1000),
    )
}

async fn register_resolved_task_child_execution_lease(
    processor: &Arc<MessageProcessor>,
    child_turn_id: &str,
) -> Result<()> {
    processor
        .register_execution_lease(child_turn_id)
        .await
        .context("failed to register task child execution lease")?;
    Ok(())
}

struct RevalidatedTaskExecutionAuthorizationContext {
    context: crate::authorization::ExecutionAuthorizationContext,
    revalidation: crate::authorization::RevalidatedExecutionAuthorization,
}

async fn resolve_task_parent_execution_authorization_context(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    parent: &TaskParentRuntimeContext,
) -> Result<RevalidatedTaskExecutionAuthorizationContext> {
    let admission = processor
        .crud_store
        .get_task_execution_admission(task.id.as_str())
        .await
        .context("failed to load durable Task execution admission")?
        .with_context(|| {
            format!(
                "agent Task `{}` has no durable execution admission",
                task.id
            )
        })?;
    let context = crate::authorization::ExecutionAuthorizationContext::load_for_task_admission(
        processor.crud_store.as_ref(),
        &admission,
    )
    .await
    .context("durable Task execution admission is invalid")?;
    if admission.workspace_id != task.workspace_id
        || admission.workspace_id != context.workspace_id()
        || admission.root_thread_id != parent.root_thread_id
        || admission.root_thread_id != context.root_thread_id()
        || admission.initiating_principal_id != context.initiating_principal_id().as_str()
    {
        bail!("Task execution admission differs from its immutable task boundary");
    }
    let revision = processor
        .authorization_invalidation_hub
        .current_revision()
        .await
        .context("failed to load current authorization generation")?;
    let revalidation = processor
        .execution_leases
        .revalidate_context(
            processor.crud_store.as_ref(),
            &context,
            crate::authorization::ResourceAction::TaskCreate,
            revision,
        )
        .await
        .context("Task execution admission is no longer authorized")?;
    context
        .verify_current_provider_authority(processor.provider_registry().as_ref())
        .context("Task provider authority changed after admission")?;
    Ok(RevalidatedTaskExecutionAuthorizationContext {
        context,
        revalidation,
    })
}

async fn resolve_task_child_execution_authorization_context(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    parent: &TaskParentRuntimeContext,
    provider: &str,
    model: &str,
    execution_backend: Option<&AgentExecutionBackend>,
    capabilities: &[pioneer_protocol::TurnCapability],
    permission_profile: &TurnPermissionProfileSnapshot,
    turn_id: &str,
) -> Result<(
    RevalidatedTaskExecutionAuthorizationContext,
    Vec<pioneer_skills::AgentSkillRuntimeEntry>,
)> {
    let provider_authority_fingerprint = match execution_backend {
        Some(AgentExecutionBackend::CLIAgentRuntime { .. })
        | Some(AgentExecutionBackend::ACPAgentRuntime { .. }) => None,
        _ => Some(
            processor
                .provider_registry()
                .authority_fingerprint_for_workspace(task.workspace_id.as_str(), provider)?
                .as_str()
                .to_owned(),
        ),
    };
    let parent_authorization =
        resolve_task_parent_execution_authorization_context(processor, task, parent).await?;
    let agent_skill_overlay = if !matches!(
        execution_backend,
        Some(AgentExecutionBackend::CLIAgentRuntime { .. })
            | Some(AgentExecutionBackend::ACPAgentRuntime { .. })
    ) && processor
        .native_api_provider_supports_agent_skill_overlay(task.workspace_id.as_str(), provider)
    {
        load_task_agent_skill_overlay(processor, &parent_authorization.context, turn_id).await?
    } else {
        Vec::new()
    };
    let grant_capabilities = crate::authorization::execution_grant_capabilities_with_agent_skills(
        capabilities,
        agent_skill_overlay
            .iter()
            .map(|entry| entry.skill_id.clone()),
    );
    let child_authorization = parent_authorization
        .context
        .derive_continuation_with_grant_capabilities(
            provider,
            model,
            execution_backend,
            capabilities,
            grant_capabilities.as_slice(),
            permission_profile,
            provider_authority_fingerprint.as_deref(),
        )?;
    Ok((
        RevalidatedTaskExecutionAuthorizationContext {
            context: child_authorization,
            revalidation: parent_authorization.revalidation,
        },
        agent_skill_overlay,
    ))
}

async fn revalidate_existing_task_child_execution_authorization(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    child_thread_id: &str,
    child_turn_id: &str,
) -> Result<crate::authorization::ExecutionAuthorizationContext> {
    let context = processor
        .load_turn_execution_authorization_context(child_turn_id)
        .await
        .context("failed to load task child execution authorization")?;
    processor
        .execution_leases
        .revalidate_for_turn(
            processor.crud_store.as_ref(),
            &context,
            task.workspace_id.as_str(),
            child_thread_id,
            child_turn_id,
            crate::authorization::ResourceAction::TaskCreate,
            processor
                .current_authorization_revision()
                .await
                .context("task continuation policy generation is unavailable")?,
        )
        .await
        .context("task child collaboration authority no longer permits continuation")?;
    Ok(context)
}

async fn load_required_task_child_execution_security_snapshot(
    processor: &Arc<MessageProcessor>,
    child_turn_id: &str,
) -> Result<TurnExecutionSecuritySnapshot> {
    processor
        .crud_store
        .get_turn_execution_security_snapshot(child_turn_id)
        .await?
        .map(|record| record.snapshot)
        .ok_or_else(|| missing_task_child_execution_security_snapshot_error(child_turn_id))
}

fn missing_task_child_execution_security_snapshot_error(child_turn_id: &str) -> anyhow::Error {
    anyhow!(
        "child turn `{}` is missing persisted execution security snapshot during recovery",
        child_turn_id
    )
}

fn apply_task_tool_policy_to_permission_profile(
    profile: &mut TurnPermissionProfileSnapshot,
    tool_policy: &TaskAgentToolPolicy,
) {
    let task_policy = task_tool_policy_permission_snapshot(tool_policy);
    profile.effective_policy = pioneer_protocol::intersect_tool_permission_policies(
        &profile.effective_policy,
        &task_policy,
    );
}

fn task_tool_policy_permission_snapshot(
    tool_policy: &TaskAgentToolPolicy,
) -> ToolPermissionPolicySnapshot {
    let mut policy = ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow);
    match tool_policy.write_mode {
        TaskAgentWriteMode::ReadOnly => {
            policy.file_write = PermissionBehavior::Deny;
        }
        TaskAgentWriteMode::WorkspaceWrite | TaskAgentWriteMode::ScopedWrite => {}
        TaskAgentWriteMode::FullAccess => {}
    }
    if !tool_policy.network_access {
        policy.network = PermissionBehavior::Deny;
    }
    policy.allowed_tools = normalized_task_policy_values(&tool_policy.allowed_tools);
    policy.allowed_tools_restricted = !policy.allowed_tools.is_empty();
    policy.denied_tools = normalized_task_policy_values(&tool_policy.denied_tools);
    policy.allowed_paths = normalized_task_policy_values(&tool_policy.allowed_paths);
    policy.allowed_paths_restricted = !policy.allowed_paths.is_empty();
    policy
}

fn normalized_task_policy_values(values: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !normalized
            .iter()
            .any(|existing: &String| existing == trimmed)
        {
            normalized.push(trimmed.to_owned());
        }
    }
    normalized
}

pub(super) fn select_agent_spec(response: &TaskGetResponse, run_id: &str) -> Option<TaskAgentSpec> {
    response
        .agent_specs
        .iter()
        .rev()
        .find(|spec| spec.run_id.as_deref() == Some(run_id))
        .or_else(|| {
            response
                .agent_specs
                .iter()
                .rev()
                .find(|spec| spec.run_id.is_none())
        })
        .cloned()
}

#[derive(Debug, Clone)]
struct RevisionPromptContext {
    task_run_turn: TaskRunTurn,
    previous_candidate: TaskResultCandidate,
    review_event: TaskResultReviewEvent,
    additional_instructions: Vec<String>,
}

async fn load_revision_prompt_context(
    processor: &Arc<MessageProcessor>,
    task_run_turn: &TaskRunTurn,
) -> Result<RevisionPromptContext> {
    let candidate_id = task_run_turn
        .requested_by_candidate_id
        .as_deref()
        .ok_or_else(|| {
            anyhow!(
                "revision task run turn `{}` has no requested_by_candidate_id",
                task_run_turn.id
            )
        })?;
    let previous_candidate = processor
        .crud_store
        .get_task_result_candidate(candidate_id)
        .await?
        .ok_or_else(|| anyhow!("revision candidate `{candidate_id}` not found"))?;
    let review_event = match task_run_turn.requested_by_review_event_id.as_deref() {
        Some(review_event_id) => processor
            .crud_store
            .get_task_result_review_event(review_event_id)
            .await?
            .ok_or_else(|| anyhow!("revision review event `{review_event_id}` not found"))?,
        None => processor
            .crud_store
            .list_task_result_review_events(candidate_id)
            .await?
            .into_iter()
            .find(|event| event.next_task_run_turn_id.as_deref() == Some(task_run_turn.id.as_str()))
            .ok_or_else(|| {
                anyhow!(
                    "revision task run turn `{}` has no matching review event",
                    task_run_turn.id
                )
            })?,
    };
    Ok(RevisionPromptContext {
        task_run_turn: task_run_turn.clone(),
        additional_instructions: revision_additional_instructions_from_feedback(
            review_event.feedback.as_ref(),
        ),
        previous_candidate,
        review_event,
    })
}

fn revision_additional_instructions_from_feedback(feedback: Option<&TaskValue>) -> Vec<String> {
    let Some(TaskValue::Object(object)) = feedback else {
        return Vec::new();
    };
    let Some(TaskValue::List(values)) = object.get("additionalInstructions") else {
        return Vec::new();
    };
    values
        .iter()
        .filter_map(|value| match value {
            TaskValue::String(value) => Some(value.clone()),
            _ => None,
        })
        .collect()
}

async fn task_revise_response_from_store(
    processor: &Arc<MessageProcessor>,
    response: TaskReviseResponse,
) -> Result<TaskReviseResponse> {
    let task_response = processor
        .crud_store
        .get_task(response.task.id.as_str())
        .await?
        .ok_or_else(|| anyhow!("task `{}` not found", response.task.id))?;
    let run = task_response
        .runs
        .iter()
        .find(|run| run.id == response.run.id)
        .cloned()
        .ok_or_else(|| anyhow!("task run `{}` not found", response.run.id))?;
    let candidate = processor
        .crud_store
        .get_task_result_candidate(response.candidate.id.as_str())
        .await?
        .unwrap_or(response.candidate);
    let review_event = processor
        .crud_store
        .get_task_result_review_event(response.review_event.id.as_str())
        .await?
        .unwrap_or(response.review_event);
    let task_run_turn = processor
        .crud_store
        .get_task_run_turn(response.task_run_turn.id.as_str())
        .await?
        .unwrap_or(response.task_run_turn);
    Ok(TaskReviseResponse {
        status: task_response.task.status,
        task: task_response.task,
        run,
        candidate,
        review_event,
        child_thread_id: task_run_turn.thread_id.clone(),
        child_turn_id: task_run_turn.turn_id.clone(),
        round: task_run_turn.round,
        task_run_turn,
        requested: response.requested,
        already_requested: response.already_requested,
        feedback: response.feedback,
        additional_instructions: response.additional_instructions,
    })
}

async fn materialize_child_task_prompt(
    processor: &Arc<MessageProcessor>,
    task_response: &TaskGetResponse,
    run: &TaskRun,
    agent_spec: &TaskAgentSpec,
    parent: &TaskParentRuntimeContext,
    task_run_turn: Option<&TaskRunTurn>,
    effective_permission_profile: &TurnPermissionProfileSnapshot,
    frozen_parent_history: Option<&[pioneer_provider::ChatMessage]>,
) -> Result<String> {
    let parent_context = render_context_policy(
        processor,
        task_response.task.workspace_id.as_str(),
        agent_spec,
        parent,
        frozen_parent_history,
    )
    .await?;
    let trigger = run
        .trigger_id
        .as_deref()
        .and_then(|trigger_id| find_task_run_trigger(task_response, trigger_id));
    let revision_context = match task_run_turn {
        Some(task_run_turn) if task_run_turn.kind == TaskRunTurnKind::Revision => {
            Some(load_revision_prompt_context(processor, task_run_turn).await?)
        }
        _ => None,
    };
    let revision = revision_context
        .as_ref()
        .map(|context| TaskRevisionPromptInput {
            task_run_turn: &context.task_run_turn,
            previous_candidate: &context.previous_candidate,
            review_event: &context.review_event,
            additional_instructions: &context.additional_instructions,
        });
    Ok(TaskRunPromptCompiler::new().compile(TaskRunPromptInput {
        task: &task_response.task,
        run,
        trigger,
        agent_spec,
        now: now_timestamp_secs(),
        parent_context: parent_context.as_deref(),
        output_instructions: agent_spec.prompt.output_instructions.as_deref(),
        effective_permission_profile,
        revision,
    }))
}

fn materialize_child_task_input(prompt: String, agent_spec: &TaskAgentSpec) -> Vec<UserInput> {
    let mut input = vec![UserInput::Text {
        text: prompt,
        text_elements: Vec::new(),
    }];
    input.extend(task_artifacts::task_agent_artifact_user_inputs(agent_spec));
    input
}

fn materialize_reviewer_prompt(
    task_response: &TaskGetResponse,
    agent_spec: &TaskAgentSpec,
    review_policy: &TaskAgentReviewPolicy,
    candidate: &TaskResultCandidate,
    reviewer_spec: &TaskResultReviewerSpec,
    reviewer_key: &str,
) -> String {
    let result_json = candidate
        .result
        .as_ref()
        .and_then(|result| serde_json::to_string_pretty(result).ok())
        .unwrap_or_else(|| "null".to_owned());
    let extraction_error_json = candidate
        .extraction_error
        .as_ref()
        .and_then(|error| serde_json::to_string_pretty(error).ok())
        .unwrap_or_else(|| "null".to_owned());
    let reviewer_role = reviewer_spec
        .agent_role
        .as_deref()
        .or(reviewer_spec.agent_nickname.as_deref())
        .unwrap_or("reviewer");
    format!(
        r#"You are reviewing a child agent result for a task.

Task title:
{title}

Task goal:
{goal}

Original child-agent instructions:
{instructions}

Review policy:
- strategy: {strategy:?}
- max revision rounds: {max_revision_rounds}
- reviewer key: {reviewer_key}
- reviewer role: {reviewer_role}
- required reviewer: {required}

Candidate:
- id: {candidate_id}
- round: {round}
- status: {status:?}
- summary: {summary}

Candidate result JSON:
{result_json}

Candidate extraction error JSON:
{extraction_error_json}

Return only one JSON object:
{{
  "decision": "accept" | "request_changes" | "reject" | "abstain",
  "feedback": "short actionable feedback",
  "confidence": 0.0
}}
"#,
        title = task_response.task.title,
        goal = task_response.task.goal,
        instructions = agent_spec.prompt.instructions.join("\n"),
        strategy = review_policy.resolution_strategy,
        max_revision_rounds = review_policy.max_revision_rounds,
        required = reviewer_spec.required,
        candidate_id = candidate.id,
        round = candidate.round,
        status = candidate.status,
        summary = candidate.summary.as_deref().unwrap_or(""),
    )
}

#[derive(Debug, Clone, PartialEq)]
struct ReviewerAdvisory {
    decision: TaskResultReviewDecision,
    feedback_text: Option<String>,
    feedback: Option<TaskValue>,
    confidence: Option<f64>,
}

async fn extract_reviewer_advisory(
    processor: &Arc<MessageProcessor>,
    turn_id: &str,
) -> Result<ReviewerAdvisory> {
    let messages = processor
        .crud_store
        .list_completed_agent_messages(turn_id)
        .await?;
    let final_text = messages.into_iter().rev().find_map(|item| match item {
        TurnItem::AgentMessage { text, .. } => Some(text),
        _ => None,
    });
    Ok(match final_text {
        Some(text) => parse_reviewer_advisory_text(text.as_str()),
        None => ReviewerAdvisory {
            decision: TaskResultReviewDecision::Abstain,
            feedback_text: Some("reviewer turn completed without a final agent message".to_owned()),
            feedback: None,
            confidence: None,
        },
    })
}

fn parse_reviewer_advisory_text(raw: &str) -> ReviewerAdvisory {
    let parsed = serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .or_else(|| extract_json_object(raw).and_then(|json| serde_json::from_str(&json).ok()));
    let Some(value) = parsed else {
        return ReviewerAdvisory {
            decision: fallback_review_decision(raw),
            feedback_text: Some(raw.trim().to_owned()).filter(|text| !text.is_empty()),
            feedback: None,
            confidence: None,
        };
    };
    let decision = value
        .get("decision")
        .and_then(|value| value.as_str())
        .and_then(parse_review_decision)
        .unwrap_or(TaskResultReviewDecision::Abstain);
    let feedback_text = value
        .get("feedback")
        .or_else(|| value.get("feedbackText"))
        .or_else(|| value.get("reason"))
        .and_then(|value| value.as_str())
        .map(str::to_owned);
    let confidence = value.get("confidence").and_then(|value| value.as_f64());
    ReviewerAdvisory {
        decision,
        feedback_text,
        feedback: Some(task_value_from_json(value)),
        confidence,
    }
}

fn extract_json_object(raw: &str) -> Option<String> {
    let start = raw.find('{')?;
    let end = raw.rfind('}')?;
    (end >= start).then(|| raw[start..=end].to_owned())
}

fn parse_review_decision(value: &str) -> Option<TaskResultReviewDecision> {
    match value.trim().to_ascii_lowercase().as_str() {
        "accept" | "accepted" | "approve" | "approved" => Some(TaskResultReviewDecision::Accept),
        "request_changes" | "request changes" | "revise" | "needs_changes" => {
            Some(TaskResultReviewDecision::RequestChanges)
        }
        "reject" | "rejected" => Some(TaskResultReviewDecision::Reject),
        "cancel" | "cancelled" | "canceled" => Some(TaskResultReviewDecision::Cancel),
        "abstain" | "unknown" => Some(TaskResultReviewDecision::Abstain),
        _ => None,
    }
}

fn fallback_review_decision(raw: &str) -> TaskResultReviewDecision {
    let lower = raw.to_ascii_lowercase();
    if lower.contains("request changes")
        || lower.contains("needs changes")
        || lower.contains("revise")
    {
        TaskResultReviewDecision::RequestChanges
    } else if lower.contains("reject") {
        TaskResultReviewDecision::Reject
    } else if lower.contains("accept") || lower.contains("approve") {
        TaskResultReviewDecision::Accept
    } else {
        TaskResultReviewDecision::Abstain
    }
}

fn reviewer_key_for_turn(
    review_policy: Option<&TaskAgentReviewPolicy>,
    candidate: &TaskResultCandidate,
    task_run_turn: &TaskRunTurn,
) -> Option<String> {
    let review_policy = review_policy?;
    review_policy
        .reviewers
        .iter()
        .enumerate()
        .map(|(index, spec)| task_result_reviewer_spec_key(index, spec))
        .find(|key| {
            stable_review_thread_id(candidate.id.as_str(), key.as_str()) == task_run_turn.thread_id
        })
}

fn reviewer_thread_name(task: &Task, reviewer_spec: &TaskResultReviewerSpec) -> String {
    let reviewer = reviewer_spec
        .agent_nickname
        .as_deref()
        .or(reviewer_spec.agent_role.as_deref())
        .unwrap_or("Reviewer");
    format!("{reviewer}: {}", task.title)
}

fn find_task_run_trigger<'a>(
    task_response: &'a TaskGetResponse,
    trigger_id: &str,
) -> Option<&'a TaskTrigger> {
    task_response
        .triggers
        .iter()
        .find(|trigger| trigger.id == trigger_id)
}

fn thread_name_from_task(task: &Task) -> Option<String> {
    let trimmed = task.title.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

async fn render_context_policy(
    processor: &Arc<MessageProcessor>,
    workspace_id: &str,
    agent_spec: &TaskAgentSpec,
    parent: &TaskParentRuntimeContext,
    frozen_parent_history: Option<&[pioneer_provider::ChatMessage]>,
) -> Result<Option<String>> {
    let Some(policy) = agent_spec.context_policy.as_ref() else {
        let rendered = match frozen_parent_history {
            Some(history) => render_frozen_parent_history(history, Some(6), true),
            None => render_parent_history(processor, parent, Some(6), true).await?,
        };
        return Ok(rendered.map(frame_background_context));
    };

    let mut sections = Vec::new();
    if let Some(rendered) = match policy.mode {
        TaskAgentContextMode::Empty => Ok(None),
        TaskAgentContextMode::Custom => Ok(policy
            .custom_context
            .as_ref()
            .and_then(render_agent_context)
            .map(|value| format!("Context:\n{value}"))),
        TaskAgentContextMode::SummaryOnly => match frozen_parent_history {
            Some(history) => Ok(render_frozen_parent_summary(
                history,
                policy.include_parent_summary,
            )),
            None => render_parent_summary(processor, parent, policy.include_parent_summary).await,
        },
        TaskAgentContextMode::LastNTurns => {
            let max_turns = policy.max_turns.map(|value| value as usize).or(Some(6));
            match frozen_parent_history {
                Some(history) => Ok(render_frozen_parent_history(
                    history,
                    max_turns,
                    policy.include_parent_summary,
                )),
                None => {
                    render_parent_history(
                        processor,
                        parent,
                        max_turns,
                        policy.include_parent_summary,
                    )
                    .await
                }
            }
        }
        TaskAgentContextMode::InheritParent => {
            let max_turns = policy.max_turns.map(|value| value as usize).or(Some(12));
            match frozen_parent_history {
                Some(history) => Ok(render_frozen_parent_history(
                    history,
                    max_turns,
                    policy.include_parent_summary,
                )),
                None => {
                    render_parent_history(
                        processor,
                        parent,
                        max_turns,
                        policy.include_parent_summary,
                    )
                    .await
                }
            }
        }
    }? {
        sections.push(rendered);
    }

    if policy.include_artifacts
        && let Some(rendered) =
            task_artifacts::render_parent_artifact_refs(processor, workspace_id, parent).await?
    {
        sections.push(rendered);
    }

    if sections.is_empty() {
        Ok(None)
    } else {
        Ok(Some(frame_background_context(sections.join("\n\n"))))
    }
}

fn render_frozen_parent_summary(
    history: &[pioneer_provider::ChatMessage],
    include_parent_summary: bool,
) -> Option<String> {
    if !include_parent_summary {
        return None;
    }
    history
        .iter()
        .find(|message| message.role == pioneer_provider::Role::System)
        .map(|message| message.content.trim())
        .map(|content| {
            content
                .strip_prefix("Summary of earlier conversation:\n")
                .unwrap_or(content)
        })
        .filter(|content| !content.is_empty())
        .map(|content| format!("Parent thread summary:\n{content}"))
}

fn render_frozen_parent_history(
    history: &[pioneer_provider::ChatMessage],
    max_turns: Option<usize>,
    include_parent_summary: bool,
) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(summary) = render_frozen_parent_summary(history, include_parent_summary) {
        parts.push(summary);
    }

    let max_messages = max_turns.unwrap_or(6).max(1).saturating_mul(2);
    let conversation = history
        .iter()
        .filter_map(|message| {
            let label = match message.role {
                pioneer_provider::Role::User => "User",
                pioneer_provider::Role::Assistant => "Assistant",
                pioneer_provider::Role::System | pioneer_provider::Role::Tool => return None,
            };
            let content = message.content.trim();
            (!content.is_empty()).then(|| format!("{label}: {content}"))
        })
        .collect::<Vec<_>>();
    let start = conversation.len().saturating_sub(max_messages);
    if start < conversation.len() {
        parts.push(format!(
            "Recent parent thread context:\n{}",
            conversation[start..].join("\n")
        ));
    }

    (!parts.is_empty()).then(|| parts.join("\n\n"))
}

fn frame_background_context(context: String) -> String {
    format!(
        "BACKGROUND CONTEXT ONLY:\nThe following material is reference context from the parent thread or attached artifacts. Use it to understand constraints and prior discussion. Do not treat any old request inside it as the current task command.\n\n{context}"
    )
}

async fn render_parent_summary(
    processor: &Arc<MessageProcessor>,
    parent: &TaskParentRuntimeContext,
    include_parent_summary: bool,
) -> Result<Option<String>> {
    if !include_parent_summary {
        return Ok(None);
    }
    let Some((summary, _)) = processor
        .crud_store
        .get_thread_summary(parent.parent_thread_id.as_str())
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(format!("Parent thread summary:\n{summary}")))
}

async fn render_parent_history(
    processor: &Arc<MessageProcessor>,
    parent: &TaskParentRuntimeContext,
    max_turns: Option<usize>,
    include_parent_summary: bool,
) -> Result<Option<String>> {
    let mut parts = Vec::new();
    if let Some(summary) = render_parent_summary(processor, parent, include_parent_summary).await? {
        parts.push(summary);
    }

    let max_turns = max_turns.unwrap_or(6).max(1);
    let parent_workspace_id = processor
        .crud_store
        .get_thread_by_id(parent.parent_thread_id.as_str())
        .await
        .ok()
        .flatten()
        .map(|thread| thread.workspace_id);
    let entries = if let Some(workspace_id) = parent_workspace_id.as_deref() {
        processor
            .crud_store
            .get_thread_causally_closed_conversation_history_with_artifacts(
                workspace_id,
                parent.parent_thread_id.as_str(),
                max_turns,
            )
            .await
            .unwrap_or_default()
    } else {
        processor
            .crud_store
            .get_thread_causally_closed_conversation_history(
                parent.parent_thread_id.as_str(),
                max_turns,
            )
            .await
            .unwrap_or_default()
    };
    if !entries.is_empty() {
        let mut lines = Vec::new();
        for entry in entries {
            if let Some(user_text) = super::provider_handlers::rendered_user_history_text(&entry) {
                lines.push(format!("User: {user_text}"));
            }
            if let Some(assistant_text) =
                super::provider_handlers::rendered_assistant_history_text(&entry)
            {
                lines.push(format!("Assistant: {assistant_text}"));
            }
        }
        if !lines.is_empty() {
            parts.push(format!(
                "Recent parent thread context:\n{}",
                lines.join("\n")
            ));
        }
    }

    if parts.is_empty() {
        Ok(None)
    } else {
        Ok(Some(parts.join("\n\n")))
    }
}

fn render_agent_input(input: &TaskAgentInput) -> Option<String> {
    let mut lines = Vec::new();
    if let Some(text) = input.text.as_deref()
        && !text.trim().is_empty()
    {
        lines.push(text.to_owned());
    }
    for variable in &input.variables {
        let value = serde_json::to_string(&variable.value)
            .unwrap_or_else(|_| "<unserializable>".to_owned());
        lines.push(format!("Variable {}: {}", variable.name, value));
    }
    for attachment in &input.attachments {
        lines.push(format!(
            "Attachment {:?}: {}",
            attachment.kind,
            attachment
                .name
                .as_deref()
                .or(attachment.path.as_deref())
                .or(attachment.url.as_deref())
                .or(attachment.artifact_id.as_deref())
                .unwrap_or("unnamed")
        ));
    }
    for reference in &input.references {
        lines.push(format!(
            "Reference {:?}: {}{}",
            reference.kind,
            reference.id,
            reference
                .label
                .as_ref()
                .map(|label| format!(" ({label})"))
                .unwrap_or_default()
        ));
    }
    (!lines.is_empty()).then(|| lines.join("\n"))
}

fn render_agent_context(context: &TaskAgentContext) -> Option<String> {
    let input = TaskAgentInput {
        text: context.text.clone(),
        variables: context.variables.clone(),
        attachments: context.attachments.clone(),
        references: context.references.clone(),
    };
    render_agent_input(&input)
}

struct TaskAgentResultExtractor;

type TaskAgentResultExtraction = std::result::Result<TaskResult, TaskError>;

enum TaskAgentResultArtifactMode {
    FinalResult,
    ResultCandidate { candidate_id: String },
}

struct StructuredResultCandidate {
    value: TaskValue,
}

impl TaskAgentResultExtractor {
    async fn extract_with_artifact_mode(
        processor: &Arc<MessageProcessor>,
        task_run_turn: &TaskRunTurn,
        lineage: &TaskThreadLineage,
        artifact_mode: TaskAgentResultArtifactMode,
    ) -> Result<TaskAgentResultExtraction> {
        let task_response = match processor
            .crud_store
            .get_task(task_run_turn.task_id.as_str())
            .await?
        {
            Some(response) => response,
            None => {
                return Ok(Err(task_error(
                    "task_missing",
                    format!(
                        "task `{}` was not found for result extraction",
                        task_run_turn.task_id
                    ),
                    TaskErrorClass::Internal,
                    Some(task_run_turn.run_id.clone()),
                )));
            }
        };
        let contract = select_agent_spec(&task_response, task_run_turn.run_id.as_str())
            .and_then(|spec| spec.result_contract);

        let messages = processor
            .crud_store
            .list_completed_agent_messages(task_run_turn.turn_id.as_str())
            .await?;
        let final_message = messages.into_iter().rev().find_map(|item| match item {
            TurnItem::AgentMessage { id, text, .. } => Some((id, text)),
            _ => None,
        });
        let Some((source_item_id, raw_text)) = final_message else {
            return Ok(Err(task_error(
                "task_agent_result_missing",
                "child task turn completed without a final agent message".to_owned(),
                TaskErrorClass::Validation,
                Some(task_run_turn.run_id.clone()),
            )));
        };

        match Self::normalize_final_message(
            raw_text,
            source_item_id,
            task_run_turn,
            contract.as_ref(),
        ) {
            Ok(result) => {
                Self::normalize_result_artifacts(
                    processor,
                    &task_response,
                    task_run_turn,
                    lineage,
                    result,
                    artifact_mode,
                )
                .await
            }
            Err(error) => Ok(Err(error)),
        }
    }

    async fn normalize_result_artifacts(
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        task_run_turn: &TaskRunTurn,
        lineage: &TaskThreadLineage,
        result: TaskResult,
        artifact_mode: TaskAgentResultArtifactMode,
    ) -> Result<TaskAgentResultExtraction> {
        let authorization = task_artifacts::authorize_task_result_artifacts(
            processor,
            &task_response.task,
            task_run_turn,
            &result,
        )
        .await?;
        match artifact_mode {
            TaskAgentResultArtifactMode::FinalResult => {
                task_artifacts::normalize_task_result_artifacts(
                    processor,
                    &task_response.task,
                    task_run_turn,
                    lineage,
                    &authorization,
                    result,
                )
                .await
            }
            TaskAgentResultArtifactMode::ResultCandidate { candidate_id } => {
                task_artifacts::normalize_task_result_candidate_artifacts(
                    processor,
                    &task_response.task,
                    task_run_turn,
                    lineage,
                    candidate_id.as_str(),
                    &authorization,
                    result,
                )
                .await
            }
        }
    }

    fn normalize_final_message(
        raw_text: String,
        source_item_id: String,
        task_run_turn: &TaskRunTurn,
        contract: Option<&TaskAgentResultContract>,
    ) -> TaskAgentResultExtraction {
        let mut diagnostics = Vec::new();
        if let Some(candidate) =
            extract_structured_result_candidate(raw_text.as_str(), contract, &mut diagnostics)
        {
            let schema_errors = validate_structured_candidate(&candidate.value, contract);
            if schema_errors.is_empty() {
                return Ok(task_result_from_structured_candidate(
                    candidate,
                    raw_text.as_str(),
                    task_run_turn,
                ));
            }
            diagnostics.extend(schema_errors);
        }

        Ok(fallback_text_task_result(
            raw_text.as_str(),
            source_item_id,
            task_run_turn,
            diagnostics,
        ))
    }
}

fn task_result_from_structured_candidate(
    candidate: StructuredResultCandidate,
    raw_text: &str,
    task_run_turn: &TaskRunTurn,
) -> TaskResult {
    let (summary, data, artifacts) = match candidate.value {
        TaskValue::Object(mut object) => {
            let artifacts = object
                .remove("artifacts")
                .and_then(|value| match value {
                    TaskValue::List(values) => Some(task_artifacts::parse_task_artifacts(&values)),
                    _ => None,
                })
                .unwrap_or_default();
            let summary = object
                .remove("summary")
                .and_then(|value| task_value_into_string(value))
                .or_else(|| first_meaningful_line(raw_text));
            let data = object
                .remove("data")
                .or_else(|| (!object.is_empty()).then(|| TaskValue::Object(object)));
            (summary, data, artifacts)
        }
        TaskValue::String(value) => (
            Some(value.clone()),
            Some(TaskValue::String(value)),
            Vec::new(),
        ),
        value => (first_meaningful_line(raw_text), Some(value), Vec::new()),
    };

    let mut result = TaskResult {
        summary,
        data,
        artifacts,
        completed_by_run_id: Some(task_run_turn.run_id.clone()),
    };
    if let Some(TaskValue::Object(object)) = &result.data
        && let Some(TaskValue::List(artifact_values)) = object.get("artifacts")
    {
        result.artifacts = task_artifacts::parse_task_artifacts(artifact_values);
    }
    result
}

fn fallback_text_task_result(
    raw_text: &str,
    source_item_id: String,
    task_run_turn: &TaskRunTurn,
    diagnostics: Vec<String>,
) -> TaskResult {
    let plain_text = strip_structured_result_blocks(raw_text);
    let fallback_text = if plain_text.trim().is_empty() {
        raw_text.to_owned()
    } else {
        plain_text
    };
    let data = TaskValue::Object(BTreeMap::from([
        (
            "rawText".to_owned(),
            TaskValue::String(fallback_text.clone()),
        ),
        ("schemaValid".to_owned(), TaskValue::Bool(false)),
        ("fallbackUsed".to_owned(), TaskValue::Bool(true)),
        (
            "diagnostics".to_owned(),
            TaskValue::List(diagnostics.into_iter().map(TaskValue::String).collect()),
        ),
        (
            "sourceThreadId".to_owned(),
            TaskValue::String(task_run_turn.thread_id.clone()),
        ),
        (
            "sourceTurnId".to_owned(),
            TaskValue::String(task_run_turn.turn_id.clone()),
        ),
        ("sourceItemId".to_owned(), TaskValue::String(source_item_id)),
    ]));
    TaskResult {
        summary: first_meaningful_line(fallback_text.as_str()),
        data: Some(data),
        artifacts: Vec::new(),
        completed_by_run_id: Some(task_run_turn.run_id.clone()),
    }
}

fn extract_structured_result_candidate(
    raw_text: &str,
    contract: Option<&TaskAgentResultContract>,
    diagnostics: &mut Vec<String>,
) -> Option<StructuredResultCandidate> {
    for (source, snippet) in collect_structured_result_snippets(raw_text, contract) {
        match serde_json::from_str::<serde_json::Value>(snippet.trim()) {
            Ok(value) => {
                return Some(StructuredResultCandidate {
                    value: task_value_from_json(value),
                });
            }
            Err(error) => diagnostics.push(format!("{source} parse failed: {error}")),
        }
    }
    None
}

fn collect_structured_result_snippets(
    raw_text: &str,
    contract: Option<&TaskAgentResultContract>,
) -> Vec<(String, String)> {
    let mut snippets = Vec::new();
    collect_tagged_snippets(
        raw_text,
        "<task_result>",
        "</task_result>",
        "task_result tag",
        &mut snippets,
    );
    collect_tagged_snippets(
        raw_text,
        "<task-result>",
        "</task-result>",
        "task-result tag",
        &mut snippets,
    );

    let (task_fences, json_fences) = collect_fenced_result_snippets(raw_text, contract);
    snippets.extend(task_fences);
    snippets.extend(json_fences);

    let trimmed = raw_text.trim();
    if matches!(
        contract.map(|contract| contract.format),
        Some(TaskAgentResultFormat::Json)
    ) && (trimmed.starts_with('{') || trimmed.starts_with('['))
    {
        snippets.push(("whole json message".to_owned(), trimmed.to_owned()));
    }
    snippets
}

fn collect_tagged_snippets(
    raw_text: &str,
    open_tag: &str,
    close_tag: &str,
    source: &str,
    snippets: &mut Vec<(String, String)>,
) {
    let lower = raw_text.to_ascii_lowercase();
    let mut cursor = 0;
    while let Some(open_offset) = lower[cursor..].find(open_tag) {
        let start = cursor + open_offset + open_tag.len();
        let Some(close_offset) = lower[start..].find(close_tag) else {
            break;
        };
        let end = start + close_offset;
        snippets.push((source.to_owned(), raw_text[start..end].to_owned()));
        cursor = end + close_tag.len();
    }
}

fn collect_fenced_result_snippets(
    raw_text: &str,
    contract: Option<&TaskAgentResultContract>,
) -> (Vec<(String, String)>, Vec<(String, String)>) {
    let mut task_fences = Vec::new();
    let mut json_fences = Vec::new();
    let mut active_info: Option<String> = None;
    let mut body = Vec::new();

    for line in raw_text.lines() {
        let trimmed = line.trim_start();
        if let Some(info) = trimmed.strip_prefix("```") {
            if let Some(open_info) = active_info.take() {
                let snippet = body.join("\n");
                if is_task_result_fence(open_info.as_str()) {
                    task_fences.push((format!("fenced {open_info} block"), snippet));
                } else if is_json_result_fence(open_info.as_str(), contract) {
                    json_fences.push((format!("fenced {open_info} block"), snippet));
                }
                body.clear();
            } else {
                active_info = Some(info.trim().to_ascii_lowercase());
            }
            continue;
        }
        if active_info.is_some() {
            body.push(line);
        }
    }

    (task_fences, json_fences)
}

fn is_task_result_fence(info: &str) -> bool {
    info.contains("task-result")
        || info.contains("task_result")
        || info
            .split_whitespace()
            .collect::<Vec<_>>()
            .windows(2)
            .any(|window| window[0] == "task" && window[1] == "result")
}

fn is_json_result_fence(info: &str, contract: Option<&TaskAgentResultContract>) -> bool {
    matches!(
        contract.map(|contract| contract.format),
        Some(TaskAgentResultFormat::Json)
    ) && info.split_whitespace().next() == Some("json")
}

fn validate_structured_candidate(
    value: &TaskValue,
    contract: Option<&TaskAgentResultContract>,
) -> Vec<String> {
    let Some(contract) = contract else {
        return Vec::new();
    };
    let mut errors = Vec::new();
    if matches!(contract.format, TaskAgentResultFormat::Json)
        && !matches!(value, TaskValue::Object(_) | TaskValue::List(_))
    {
        errors.push("result contract expects JSON object or array".to_owned());
    }
    if let Some(schema) = contract.schema.as_ref() {
        errors.extend(validate_task_value_schema(
            structured_contract_validation_target(value),
            &schema.schema,
            "$",
        ));
    }
    errors
}

fn structured_contract_validation_target(value: &TaskValue) -> &TaskValue {
    if let TaskValue::Object(object) = value
        && let Some(data) = object.get("data")
    {
        return data;
    }
    value
}

fn validate_task_value_schema(value: &TaskValue, schema: &TaskValue, path: &str) -> Vec<String> {
    let Some(schema_object) = task_value_object(schema) else {
        return Vec::new();
    };
    let mut errors = Vec::new();

    if let Some(type_schema) = schema_object.get("type")
        && !schema_type_matches(value, type_schema)
    {
        errors.push(format!(
            "{path} does not match schema type {}",
            schema_type_label(type_schema)
        ));
        return errors;
    }

    if let Some(enum_schema) = schema_object.get("enum")
        && let TaskValue::List(values) = enum_schema
        && !values.iter().any(|allowed| allowed == value)
    {
        errors.push(format!("{path} is not one of the allowed enum values"));
    }

    if let TaskValue::Object(object) = value {
        if let Some(TaskValue::List(required)) = schema_object.get("required") {
            for name in required.iter().filter_map(task_value_str) {
                if !object.contains_key(name) {
                    errors.push(format!("{path}.{name} is required"));
                }
            }
        }
        if let Some(TaskValue::Object(properties)) = schema_object.get("properties") {
            for (name, property_schema) in properties {
                if let Some(child) = object.get(name) {
                    errors.extend(validate_task_value_schema(
                        child,
                        property_schema,
                        format!("{path}.{name}").as_str(),
                    ));
                }
            }
        }
    }

    if let TaskValue::List(items) = value
        && let Some(item_schema) = schema_object.get("items")
    {
        for (index, item) in items.iter().enumerate() {
            errors.extend(validate_task_value_schema(
                item,
                item_schema,
                format!("{path}[{index}]").as_str(),
            ));
        }
    }

    errors
}

fn schema_type_matches(value: &TaskValue, type_schema: &TaskValue) -> bool {
    match type_schema {
        TaskValue::String(type_name) => task_value_matches_type(value, type_name.as_str()),
        TaskValue::List(type_names) => type_names
            .iter()
            .filter_map(task_value_str)
            .any(|type_name| task_value_matches_type(value, type_name)),
        _ => true,
    }
}

fn task_value_matches_type(value: &TaskValue, type_name: &str) -> bool {
    match type_name {
        "null" => matches!(value, TaskValue::Null),
        "boolean" => matches!(value, TaskValue::Bool(_)),
        "integer" => matches!(value, TaskValue::Integer(_)),
        "number" => matches!(value, TaskValue::Integer(_) | TaskValue::Number(_)),
        "string" => matches!(value, TaskValue::String(_)),
        "array" => matches!(value, TaskValue::List(_)),
        "object" => matches!(value, TaskValue::Object(_)),
        _ => true,
    }
}

fn schema_type_label(type_schema: &TaskValue) -> String {
    match type_schema {
        TaskValue::String(value) => value.clone(),
        TaskValue::List(values) => values
            .iter()
            .filter_map(task_value_str)
            .collect::<Vec<_>>()
            .join("|"),
        _ => "unknown".to_owned(),
    }
}

fn strip_structured_result_blocks(raw_text: &str) -> String {
    let mut stripped = strip_tagged_blocks(raw_text, "<task_result>", "</task_result>");
    stripped = strip_tagged_blocks(stripped.as_str(), "<task-result>", "</task-result>");
    strip_fenced_result_blocks(stripped.as_str())
}

fn strip_tagged_blocks(raw_text: &str, open_tag: &str, close_tag: &str) -> String {
    let lower = raw_text.to_ascii_lowercase();
    let mut ranges = Vec::new();
    let mut cursor = 0;
    while let Some(open_offset) = lower[cursor..].find(open_tag) {
        let start = cursor + open_offset;
        let content_start = start + open_tag.len();
        let Some(close_offset) = lower[content_start..].find(close_tag) else {
            break;
        };
        let end = content_start + close_offset + close_tag.len();
        ranges.push((start, end));
        cursor = end;
    }
    remove_ranges(raw_text, ranges)
}

fn strip_fenced_result_blocks(raw_text: &str) -> String {
    let mut output = Vec::new();
    let mut active_info: Option<String> = None;
    let mut active_lines = Vec::new();
    for line in raw_text.lines() {
        let trimmed = line.trim_start();
        if let Some(info) = trimmed.strip_prefix("```") {
            if let Some(open_info) = active_info.take() {
                if !is_task_result_fence(open_info.as_str())
                    && !open_info
                        .split_whitespace()
                        .next()
                        .is_some_and(|value| value == "json")
                {
                    output.append(&mut active_lines);
                    output.push(line.to_owned());
                }
                active_lines.clear();
            } else {
                let open_info = info.trim().to_ascii_lowercase();
                if !is_task_result_fence(open_info.as_str())
                    && !open_info
                        .split_whitespace()
                        .next()
                        .is_some_and(|value| value == "json")
                {
                    active_lines.push(line.to_owned());
                }
                active_info = Some(open_info);
            }
            continue;
        }
        if active_info.is_some() {
            active_lines.push(line.to_owned());
        } else {
            output.push(line.to_owned());
        }
    }
    if !active_lines.is_empty() {
        output.append(&mut active_lines);
    }
    output.join("\n").trim().to_owned()
}

fn remove_ranges(raw_text: &str, ranges: Vec<(usize, usize)>) -> String {
    if ranges.is_empty() {
        return raw_text.to_owned();
    }
    let mut output = String::new();
    let mut cursor = 0;
    for (start, end) in ranges {
        output.push_str(&raw_text[cursor..start]);
        cursor = end;
    }
    output.push_str(&raw_text[cursor..]);
    output.trim().to_owned()
}

fn task_value_from_json(value: serde_json::Value) -> TaskValue {
    match value {
        serde_json::Value::Null => TaskValue::Null,
        serde_json::Value::Bool(value) => TaskValue::Bool(value),
        serde_json::Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                TaskValue::Integer(integer)
            } else {
                TaskValue::Number(value.as_f64().unwrap_or_default())
            }
        }
        serde_json::Value::String(value) => TaskValue::String(value),
        serde_json::Value::Array(values) => {
            TaskValue::List(values.into_iter().map(task_value_from_json).collect())
        }
        serde_json::Value::Object(values) => TaskValue::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, task_value_from_json(value)))
                .collect(),
        ),
    }
}

fn task_value_object(value: &TaskValue) -> Option<&BTreeMap<String, TaskValue>> {
    match value {
        TaskValue::Object(value) => Some(value),
        _ => None,
    }
}

fn task_value_str(value: &TaskValue) -> Option<&str> {
    match value {
        TaskValue::String(value) => Some(value.as_str()),
        _ => None,
    }
}

fn task_value_into_string(value: TaskValue) -> Option<String> {
    match value {
        TaskValue::String(value) => Some(value),
        _ => None,
    }
}

async fn load_workspace_skill_policies(
    processor: &Arc<MessageProcessor>,
    workspace_id: &str,
) -> Result<
    std::collections::HashMap<pioneer_skills::SkillPolicyKey, pioneer_agent::WorkspaceSkillPolicy>,
> {
    processor
        .crud_store
        .list_workspace_skill_policies(workspace_id)
        .await
        .with_context(|| {
            format!(
                "authoritative workspace skill policy projection is unavailable for \
                 `{workspace_id}`"
            )
        })
        .map(|records| {
            records
                .into_iter()
                .map(|record| {
                    (
                        pioneer_skills::SkillPolicyKey::new(record.skill_id),
                        pioneer_agent::WorkspaceSkillPolicy {
                            enabled: record.enabled,
                            allow_implicit_invocation: record.allow_implicit_invocation,
                        },
                    )
                })
                .collect()
        })
}

async fn load_task_agent_skill_overlay(
    processor: &Arc<MessageProcessor>,
    authorization: &crate::authorization::ExecutionAuthorizationContext,
    turn_id: &str,
) -> Result<Vec<pioneer_skills::AgentSkillRuntimeEntry>> {
    match authorization.runtime_principal_policy()? {
        crate::authorization::RuntimePrincipalPolicy::Absolute => Ok(processor
            .load_agent_skill_overlay_for_new_native_turn(authorization.workspace_id(), turn_id)
            .await),
        crate::authorization::RuntimePrincipalPolicy::ScopedCollaboration => {
            processor
                .load_agent_skill_overlay_for_scoped_turn(
                    authorization.initiating_principal_id(),
                    authorization.workspace_id(),
                    turn_id,
                )
                .await
        }
    }
}

fn task_error(
    code: impl Into<String>,
    _message: impl Into<String>,
    class: TaskErrorClass,
    failed_run_id: Option<String>,
) -> TaskError {
    let code = code.into();
    TaskError {
        message: code.clone(),
        code,
        class,
        details: None,
        failed_run_id,
    }
}

fn first_meaningful_line(value: &str) -> Option<String> {
    value
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_agent::{
        SkillsDependenciesLoopConfig, SkillsLoopConfig, SkillsRuntimeLoopConfig,
        SkillsSecurityLoopConfig, SkillsValidationLoopConfig,
    };
    use pioneer_artifacts::{
        ArtifactBindingTarget, ArtifactListFilter, IngestArtifactBytesRequest,
    };
    use pioneer_config::GatewayWebToolsConfig;
    use pioneer_keystore::MemorySecretStore;
    use pioneer_memory::hooks::{
        MemoryActiveRecallConfig, MemoryActiveRecallMode, MemoryLoopConfig,
    };
    use pioneer_protocol::{
        ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
        ArtifactRole, ArtifactSummary, TaskAgentInputAttachmentKind, TaskAgentInputReferenceKind,
        TaskArtifact,
    };
    use pioneer_provider::providers::EchoProvider;
    use pioneer_tools::{
        ComputerUseToolsConfig, ToolLoopBudgetConfig, ToolRetryBudgetConfig, WebToolsConfig,
    };
    use sea_orm::{ConnectionTrait, Database};

    fn exact_task_execution_actor(
        execution_id: &str,
    ) -> Result<pioneer_protocol::PersistedActorRef> {
        let execution_id = AgentExecutionId::new(execution_id.to_owned()).map_err(|error| {
            anyhow!("invalid task execution actor id `{execution_id}`: {error:?}")
        })?;
        Ok(pioneer_protocol::PersistedActorRef::AgentExecution(
            execution_id,
        ))
    }

    fn test_task_run_turn() -> TaskRunTurn {
        TaskRunTurn {
            id: "task_run_turn".to_owned(),
            task_id: "task".to_owned(),
            run_id: "run".to_owned(),
            execution_id: Some("execution".to_owned()),
            thread_id: "child_thread".to_owned(),
            turn_id: "child_turn".to_owned(),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::CandidateCreated,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: 1,
            started_at: Some(1),
            completed_at: Some(2),
        }
    }

    #[test]
    fn task_child_runtime_requires_exact_non_self_rooted_collaboration_lineage() {
        let task_run_turn = test_task_run_turn();
        let valid = TaskThreadLineage {
            child_thread_id: task_run_turn.thread_id.clone(),
            parent_thread_id: "parent_thread".to_owned(),
            root_thread_id: "root_thread".to_owned(),
            depth: 1,
            origin_kind: Some("task_run".to_owned()),
            created_by_thread_id: Some("parent_thread".to_owned()),
            created_by_turn_id: Some("parent_turn".to_owned()),
            created_at: 1,
        };
        assert_eq!(
            validate_task_thread_lineage(&task_run_turn, valid.clone())
                .expect("valid task child lineage"),
            valid
        );

        let mut wrong_child = valid.clone();
        wrong_child.child_thread_id = "other_child".to_owned();
        assert!(validate_task_thread_lineage(&task_run_turn, wrong_child).is_err());

        let mut self_rooted = valid;
        self_rooted.root_thread_id = task_run_turn.thread_id.clone();
        assert!(validate_task_thread_lineage(&task_run_turn, self_rooted).is_err());
    }

    #[test]
    fn executable_task_actor_is_always_an_agent_execution() {
        let actor = exact_task_execution_actor("E12345678901234567890")
            .expect("valid execution id should become an agent actor");
        assert_eq!(
            actor,
            pioneer_protocol::PersistedActorRef::AgentExecution(
                pioneer_protocol::AgentExecutionId::new("E12345678901234567890").unwrap()
            )
        );
        assert!(exact_task_execution_actor("system").is_err());
    }

    #[test]
    fn task_occurrence_lineage_materializes_and_then_reuses_its_exact_root() {
        assert_eq!(
            task_occurrence_execution_lineage("new-root", None, None, None, None).unwrap(),
            ("new-root".to_owned(), None)
        );
        assert_eq!(
            task_occurrence_execution_lineage(
                "first-child",
                None,
                None,
                Some("creator-root"),
                Some("creator-root"),
            )
            .unwrap(),
            ("creator-root".to_owned(), Some("creator-root".to_owned()))
        );
        assert_eq!(
            task_occurrence_execution_lineage(
                "retry-execution",
                Some("occurrence-root"),
                Some("previous-execution"),
                None,
                None,
            )
            .unwrap(),
            (
                "occurrence-root".to_owned(),
                Some("previous-execution".to_owned())
            )
        );
        assert!(
            task_occurrence_execution_lineage(
                "orphan-child",
                Some("occurrence-root"),
                None,
                None,
                None,
            )
            .is_err()
        );
    }

    fn permission_test_agent_spec(
        permission_cap: Option<pioneer_protocol::TurnPermissionProfileCap>,
        tool_policy: Option<TaskAgentToolPolicy>,
    ) -> TaskAgentSpec {
        TaskAgentSpec {
            id: "agent_spec_permission".to_owned(),
            task_id: "task_permission".to_owned(),
            run_id: None,
            agent_role: None,
            agent_nickname: None,
            model: Some("test-model".to_owned()),
            model_provider: Some("openai".to_owned()),
            prompt: pioneer_protocol::TaskAgentPrompt {
                goal: "Do the task".to_owned(),
                instructions: Vec::new(),
                input: None,
                output_instructions: None,
            },
            context_policy: None,
            tool_policy,
            permission_cap,
            security_cap: None,
            result_contract: None,
            review_policy: None,
            depth: 0,
            max_depth: 3,
            created_at: 1,
            updated_at: 1,
        }
    }

    #[test]
    fn task_child_permission_profile_rejects_missing_cap() {
        let agent_spec = permission_test_agent_spec(None, None);
        let error = effective_task_child_permission_profile(&agent_spec, None)
            .expect_err("missing permission cap should fail");

        assert!(format!("{error:#}").contains("missing permission_cap"));
    }

    #[test]
    fn recovery_security_missing_child_snapshot_error_is_explicit() {
        let error = missing_task_child_execution_security_snapshot_error("child_turn_missing");
        let message = format!("{error:#}");

        assert!(message.contains("missing persisted execution security snapshot"));
        assert!(!message.contains("FullAccess"));
        assert!(!message.contains("full_access"));
    }

    #[test]
    fn task_child_permission_profile_inherits_parent_cap_modes() {
        for mode in [
            pioneer_protocol::TurnPermissionMode::FullAccess,
            pioneer_protocol::TurnPermissionMode::AutoAcceptEdits,
            pioneer_protocol::TurnPermissionMode::Supervised,
        ] {
            let agent_spec = permission_test_agent_spec(
                Some(pioneer_protocol::task_permission_cap_from_snapshot(
                    &pioneer_protocol::inherited_turn_permission_profile_snapshot(mode),
                )),
                None,
            );
            let profile = effective_task_child_permission_profile(&agent_spec, None);
            let profile = profile.expect("permission cap should produce a profile");

            assert_eq!(profile.mode, mode);
            assert_eq!(
                profile.source,
                TurnPermissionProfileSource::TaskPermissionCap
            );
        }
    }

    #[test]
    fn task_child_permission_profile_uses_most_restrictive_cap_and_launch_mode() {
        let agent_spec = permission_test_agent_spec(
            Some(pioneer_protocol::task_permission_cap_for_mode(
                pioneer_protocol::TurnPermissionMode::AutoAcceptEdits,
            )),
            None,
        );
        let launch_profile = pioneer_protocol::inherited_turn_permission_profile_snapshot(
            pioneer_protocol::TurnPermissionMode::Supervised,
        );
        let profile = effective_task_child_permission_profile(&agent_spec, Some(&launch_profile))
            .expect("permission cap should produce a profile");

        assert_eq!(
            profile.mode,
            pioneer_protocol::TurnPermissionMode::Supervised
        );
        assert_eq!(profile.effective_policy.file_write, PermissionBehavior::Ask);
    }

    #[test]
    fn delayed_task_permission_cap_cannot_be_broadened_by_full_access_launch() {
        let agent_spec = permission_test_agent_spec(
            Some(pioneer_protocol::task_permission_cap_for_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
            )),
            None,
        );
        let launch_profile = pioneer_protocol::system_turn_permission_profile_snapshot(
            pioneer_protocol::TurnPermissionMode::FullAccess,
        );
        let profile = effective_task_child_permission_profile(&agent_spec, Some(&launch_profile))
            .expect("permission cap should produce a profile");

        assert_eq!(
            profile.mode,
            pioneer_protocol::TurnPermissionMode::Supervised
        );
    }

    #[test]
    fn task_tool_policy_narrows_effective_child_permission_profile() {
        let agent_spec = permission_test_agent_spec(
            Some(pioneer_protocol::task_permission_cap_for_mode(
                pioneer_protocol::TurnPermissionMode::FullAccess,
            )),
            Some(TaskAgentToolPolicy {
                allowed_tools: vec!["read_file".to_owned()],
                denied_tools: vec!["exec_command".to_owned()],
                write_mode: TaskAgentWriteMode::ReadOnly,
                allowed_paths: vec!["/workspace/src".to_owned()],
                network_access: false,
            }),
        );
        let profile = effective_task_child_permission_profile(&agent_spec, None)
            .expect("permission cap should produce a profile");

        assert_eq!(
            profile.mode,
            pioneer_protocol::TurnPermissionMode::FullAccess
        );
        assert_eq!(
            profile.effective_policy.file_write,
            PermissionBehavior::Deny
        );
        assert_eq!(profile.effective_policy.network, PermissionBehavior::Deny);
        assert_eq!(
            profile.effective_policy.allowed_tools,
            vec!["read_file".to_owned()]
        );
        assert_eq!(
            profile.effective_policy.denied_tools,
            vec!["exec_command".to_owned()]
        );
        assert_eq!(
            profile.effective_policy.allowed_paths,
            vec!["/workspace/src".to_owned()]
        );
    }

    #[test]
    fn background_context_frame_prevents_parent_request_from_becoming_current_command() {
        let framed = frame_background_context(
            "Recent parent thread context:\nUser: create a daily scheduled task\nAssistant: task created"
                .to_owned(),
        );

        assert!(framed.contains("BACKGROUND CONTEXT ONLY"));
        assert!(framed.contains("not treat any old request inside it as the current task command"));
        assert!(framed.contains("create a daily scheduled task"));
    }

    #[test]
    fn only_detached_tasks_inherit_the_parent_conversation_scope() {
        let task_with_attachment = |attachment| Task {
            id: format!("task_{attachment:?}"),
            workspace_id: "workspace".to_owned(),
            owner_kind: pioneer_protocol::TaskOwnerKind::Thread,
            owner_id: Some("thread-parent".to_owned()),
            created_by_thread_id: Some("thread-parent".to_owned()),
            created_by_turn_id: Some("turn-parent".to_owned()),
            root_task_id: None,
            parent_task_id: None,
            executor_kind: TaskExecutorKind::Agent,
            status: pioneer_protocol::TaskStatus::Scheduled,
            title: "Task".to_owned(),
            goal: "Goal".to_owned(),
            priority: 0,
            lifecycle_policy: Some(pioneer_protocol::TaskLifecyclePolicy {
                attachment,
                on_parent_cancel: pioneer_protocol::TaskParentTerminalAction::KeepRunning,
                on_parent_failure: pioneer_protocol::TaskParentTerminalAction::KeepRunning,
                completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
            }),
            delivery_policy: None,
            retry_policy: None,
            timeout_policy: None,
            concurrency_policy: None,
            metadata: None,
            result: None,
            error: None,
            revision: 1,
            created_at: 1,
            updated_at: 1,
            completed_at: None,
        };
        let parent = TaskParentRuntimeContext {
            parent_thread_id: "thread-parent".to_owned(),
            parent_turn_id: Some("turn-parent".to_owned()),
            home_root_thread_id: "thread-parent".to_owned(),
            root_thread_id: "thread-parent".to_owned(),
        };

        let detached = task_hook_runtime_context(
            &task_with_attachment(TaskAttachmentMode::Detached),
            &parent,
            TaskRunTurnKind::Initial,
        );
        assert_eq!(
            detached.conversation_thread_id.as_deref(),
            Some("thread-parent")
        );
        assert_eq!(
            detached.post_turn_dispatch_mode,
            pioneer_agent::AgentTurnPostTurnDispatchMode::AwaitTaskResultAcceptance
        );

        let attached = task_hook_runtime_context(
            &task_with_attachment(TaskAttachmentMode::Attached),
            &parent,
            TaskRunTurnKind::Initial,
        );
        assert_eq!(attached.conversation_thread_id, None);
        assert_eq!(
            attached.post_turn_dispatch_mode,
            pioneer_agent::AgentTurnPostTurnDispatchMode::Immediate
        );

        let reviewer = task_hook_runtime_context(
            &task_with_attachment(TaskAttachmentMode::Detached),
            &parent,
            TaskRunTurnKind::Review,
        );
        assert_eq!(
            reviewer.post_turn_dispatch_mode,
            pioneer_agent::AgentTurnPostTurnDispatchMode::Immediate
        );
    }

    #[tokio::test]
    async fn missing_execution_checkpoint_context_falls_back_to_none() {
        let (processor, _task, task_run_turn, _lineage) =
            task_artifact_harness("missing_checkpoint_context").await;
        let context =
            load_execution_checkpoint_context_for_turn(&processor, task_run_turn.turn_id.as_str())
                .await
                .expect("missing checkpoint should not fail");
        assert!(context.is_none());
    }

    #[tokio::test]
    async fn child_runtime_without_persisted_lineage_fails_closed() {
        let (processor, _task, task_run_turn, _lineage) =
            task_artifact_harness("missing_child_lineage").await;

        let error = load_child_runtime_from_task_run_turn(&processor, task_run_turn)
            .await
            .err()
            .expect("missing durable child lineage must be rejected");

        assert!(
            format!("{error:#}").contains("no durable collaboration lineage"),
            "failure must identify the missing collaboration authority"
        );
    }

    #[tokio::test]
    async fn agent_task_without_persisted_execution_admission_fails_closed() {
        let (processor, task, _task_run_turn, _lineage) =
            task_artifact_harness("missing_task_admission").await;

        let error = resolve_parent_context(&processor, &task)
            .await
            .err()
            .expect("missing durable Task admission must be rejected");

        assert!(
            format!("{error:#}").contains("no durable execution admission"),
            "failure must identify the missing execution authority"
        );
    }

    #[tokio::test]
    async fn unavailable_workspace_skill_policy_projection_fails_closed() {
        let (processor, task, _task_run_turn, _lineage) =
            task_artifact_harness("skill_policy_unavailable").await;
        processor
            .crud_store
            .database_connection()
            .execute_unprepared("DROP TABLE skill_workspace_policy")
            .await
            .expect("remove isolated policy table");

        let error = load_workspace_skill_policies(&processor, task.workspace_id.as_str())
            .await
            .expect_err("missing policy authority must not fall back to global defaults");

        assert!(
            format!("{error:#}").contains("authoritative workspace skill policy projection"),
            "failure must identify the unavailable authorization projection"
        );
    }

    async fn task_artifact_harness(
        name: &str,
    ) -> (Arc<MessageProcessor>, Task, TaskRunTurn, TaskThreadLineage) {
        let connection = Database::connect("sqlite::memory:").await.expect("sqlite");
        Migrator::up(&connection, None).await.expect("migrate");
        crate::bootstrap::bootstrap(&connection)
            .await
            .expect("bootstrap");
        let workspace_manager = Arc::new(WorkspaceManager::new(connection.clone()));
        let workspace_id = workspace_manager
            .list_workspaces()
            .await
            .expect("workspaces")
            .into_iter()
            .find(|workspace| workspace.is_current)
            .expect("current workspace")
            .id;
        let crud_store = Arc::new(CrudStore::new(connection));
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("o4-mini", "openai")),
            Arc::new(ProviderRegistry::with_provider(
                "openai",
                Arc::new(EchoProvider::new()),
            )),
            Arc::new(SessionManager::new()),
            workspace_manager,
            crud_store,
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
            summary::SummaryConfig {
                summary_model: Some("test-model".to_owned()),
                summary_model_provider: Some("echo".to_owned()),
                title_model: Some("test-model".to_owned()),
                title_model_provider: Some("echo".to_owned()),
            },
            ContextBudget {
                max_context_tokens: 128_000,
                response_reserve_tokens: 16_000,
            },
            test_tool_loop_config_for_task_artifacts(),
        ));
        let task = Task {
            id: format!("task_{name}"),
            workspace_id: workspace_id.clone(),
            owner_kind: pioneer_protocol::TaskOwnerKind::Thread,
            owner_id: Some(format!("thread_{name}")),
            created_by_thread_id: Some(format!("thread_{name}")),
            created_by_turn_id: Some(format!("turn_{name}")),
            root_task_id: None,
            parent_task_id: None,
            executor_kind: TaskExecutorKind::Agent,
            status: pioneer_protocol::TaskStatus::Running,
            title: "Task".to_owned(),
            goal: "Goal".to_owned(),
            priority: 0,
            lifecycle_policy: None,
            delivery_policy: None,
            retry_policy: None,
            timeout_policy: None,
            concurrency_policy: None,
            metadata: None,
            result: None,
            error: None,
            revision: 1,
            created_at: 1,
            updated_at: 1,
            completed_at: None,
        };
        let task_run_turn = TaskRunTurn {
            id: format!("task_run_turn_{name}"),
            task_id: task.id.clone(),
            run_id: format!("run_{name}"),
            execution_id: Some(format!("execution_{name}")),
            thread_id: format!("child_{name}"),
            turn_id: format!("child_turn_{name}"),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::CandidateCreated,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: 1,
            started_at: Some(1),
            completed_at: Some(2),
        };
        let lineage = TaskThreadLineage {
            child_thread_id: format!("child_{name}"),
            parent_thread_id: format!("thread_{name}"),
            root_thread_id: format!("thread_{name}"),
            depth: 1,
            origin_kind: Some("task_run".to_owned()),
            created_by_thread_id: Some(format!("thread_{name}")),
            created_by_turn_id: Some(format!("turn_{name}")),
            created_at: 1,
        };
        (processor, task, task_run_turn, lineage)
    }

    fn test_tool_loop_config_for_task_artifacts() -> ToolLoopConfig {
        let web = GatewayWebToolsConfig::default();
        ToolLoopConfig {
            provider: pioneer_provider::ProviderTimeoutPolicy::default(),
            preflight: pioneer_agent::PreflightLoopConfig::default(),
            web: WebToolsConfig {
                default_timeout_ms: web.default_timeout_ms,
                hard_max_timeout_ms: web.hard_max_timeout_ms,
                default_fetch_max_bytes: web.default_fetch_max_bytes,
                hard_fetch_max_bytes: web.hard_fetch_max_bytes,
                default_download_max_bytes: web.default_download_max_bytes,
                hard_download_max_bytes: web.hard_download_max_bytes,
                default_max_results: web.default_max_results,
                hard_max_results: web.hard_max_results,
                default_snippet_chars: web.default_snippet_chars,
                hard_max_snippet_chars: web.hard_max_snippet_chars,
                default_link_count: web.default_link_count,
                hard_link_count: web.hard_link_count,
                default_render_max_chars: web.default_render_max_chars,
                ddg_html_search_url: web.ddg_html_search_url,
                ddg_instant_api_url: web.ddg_instant_api_url,
                default_user_agent: web.default_user_agent,
            },
            computer_use: ComputerUseToolsConfig {
                runtime_home_dir: std::env::temp_dir().join("pioneer-task-artifact-tests"),
                artifacts_subdir: "tools/computer_use".to_owned(),
                ..ComputerUseToolsConfig::default()
            },
            skills: SkillsLoopConfig {
                enabled: true,
                max_skills_per_source: 256,
                max_skill_file_bytes: 1024 * 1024,
                prompt_max_chars: 24_000,
                allow_implicit_invocation: false,
                system_roots: Vec::new(),
                user_roots: Vec::new(),
                registry_roots: Vec::new(),
                system_import_roots: Vec::new(),
                user_import_roots: Vec::new(),
                registry_import_roots: Vec::new(),
                validation: SkillsValidationLoopConfig {
                    strict_agentskills: true,
                    accept_openclaw_profile: true,
                },
                security: SkillsSecurityLoopConfig {
                    allow_untrusted_install: false,
                    min_trust_for_shell_tools: pioneer_skills::SkillTrustLevel::Verified,
                    min_trust_for_http_tools: pioneer_skills::SkillTrustLevel::Community,
                    min_trust_for_function_proxy_tools: pioneer_skills::SkillTrustLevel::Community,
                    max_install_archive_bytes: 10 * 1024 * 1024,
                    max_install_archive_compressed_bytes: 10 * 1024 * 1024,
                    max_install_archive_uncompressed_bytes: 50 * 1024 * 1024,
                    max_install_archive_entries: 2048,
                    max_install_file_bytes: 1024 * 1024,
                    upload_ttl_secs: 3600,
                    upload_recommended_chunk_size_bytes: 256 * 1024,
                    upload_max_chunk_size_bytes: 1024 * 1024,
                },
                dependencies: SkillsDependenciesLoopConfig {
                    preflight_on_resolve: true,
                    runtime_recheck_on_tool_call: true,
                },
                runtime: SkillsRuntimeLoopConfig {
                    enable_dynamic_tools: true,
                    enable_read_skill: true,
                    max_dynamic_tools_per_skill: 64,
                    read_skill_max_chars: 72_000,
                    compact_mode_threshold: 6,
                    allow_shell_tools: true,
                    allow_http_tools: true,
                    allow_function_proxy_tools: true,
                },
            },
            memory: MemoryLoopConfig {
                active_recall: MemoryActiveRecallConfig {
                    mode: MemoryActiveRecallMode::DeterministicOnly,
                    ..MemoryActiveRecallConfig::default()
                },
                ..MemoryLoopConfig::default()
            },
            budget: ToolLoopBudgetConfig::default(),
            execution_windows: pioneer_tools::ExecutionWindowsConfig::default(),
            retry: ToolRetryBudgetConfig::default(),
        }
        .normalized()
    }

    async fn ingest_task_test_artifact(
        processor: &MessageProcessor,
        workspace_id: &str,
        thread_id: Option<String>,
        display_name: &str,
    ) -> ArtifactSummary {
        processor
            .artifact_service
            .ingest_bytes(IngestArtifactBytesRequest {
                workspace_id: workspace_id.to_owned(),
                primary_thread_id: thread_id.clone(),
                bytes: b"task artifact".to_vec(),
                display_name: display_name.to_owned(),
                kind: ArtifactKind::Text,
                mime_type: Some("text/plain".to_owned()),
                created_by_kind: ArtifactCreatedByKind::User,
                created_by_actor_id: None,
                binding: thread_id.map(|thread_id| ArtifactBindingTarget {
                    thread_id: Some(thread_id),
                    turn_id: None,
                    message_id: None,
                    turn_item_id: None,
                    tool_call_id: None,
                    task_id: None,
                    task_run_id: None,
                    binding_kind: ArtifactBindingKind::ManualAttach,
                    direction: ArtifactBindingDirection::Context,
                    role: Some(ArtifactRole::User),
                    item_index: None,
                }),
                metadata: Default::default(),
            })
            .await
            .expect("ingest artifact")
    }

    fn json_answer_contract() -> TaskAgentResultContract {
        TaskAgentResultContract {
            format: TaskAgentResultFormat::Json,
            required: true,
            schema: Some(pioneer_protocol::TaskSchema {
                name: Some("answer".to_owned()),
                description: None,
                schema: TaskValue::Object(BTreeMap::from([
                    ("type".to_owned(), TaskValue::String("object".to_owned())),
                    (
                        "required".to_owned(),
                        TaskValue::List(vec![TaskValue::String("answer".to_owned())]),
                    ),
                    (
                        "properties".to_owned(),
                        TaskValue::Object(BTreeMap::from([(
                            "answer".to_owned(),
                            TaskValue::Object(BTreeMap::from([(
                                "type".to_owned(),
                                TaskValue::String("string".to_owned()),
                            )])),
                        )])),
                    ),
                ])),
            }),
        }
    }

    #[test]
    fn result_extractor_prefers_valid_structured_task_result_block() {
        let raw_text = r#"Human readable fallback.
```task-result
{"summary":"Structured summary","data":{"answer":"42"}}
```"#;

        let result = TaskAgentResultExtractor::normalize_final_message(
            raw_text.to_owned(),
            "item".to_owned(),
            &test_task_run_turn(),
            Some(&json_answer_contract()),
        )
        .expect("structured result should be valid");

        assert_eq!(result.summary.as_deref(), Some("Structured summary"));
        let TaskValue::Object(data) = result.data.expect("data should be present") else {
            panic!("structured data should remain an object");
        };
        assert_eq!(
            data.get("answer"),
            Some(&TaskValue::String("42".to_owned()))
        );
        assert_eq!(result.completed_by_run_id.as_deref(), Some("run"));
    }

    #[test]
    fn task_artifact_input_attachments_are_materialized_as_user_inputs() {
        let agent_spec = TaskAgentSpec {
            id: "spec".to_owned(),
            task_id: "task".to_owned(),
            run_id: None,
            agent_role: None,
            agent_nickname: None,
            model: Some("model".to_owned()),
            model_provider: Some("provider".to_owned()),
            prompt: pioneer_protocol::TaskAgentPrompt {
                goal: "Use artifacts".to_owned(),
                instructions: Vec::new(),
                input: Some(TaskAgentInput {
                    text: None,
                    variables: Vec::new(),
                    attachments: vec![pioneer_protocol::TaskAgentInputAttachment {
                        kind: TaskAgentInputAttachmentKind::Artifact,
                        name: None,
                        path: None,
                        url: None,
                        artifact_id: Some("art_input".to_owned()),
                        version_id: Some("version_input".to_owned()),
                        mime_type: None,
                    }],
                    references: vec![pioneer_protocol::TaskAgentInputReference {
                        kind: TaskAgentInputReferenceKind::Artifact,
                        id: "art_ref".to_owned(),
                        label: None,
                        version_id: Some("version_ref".to_owned()),
                    }],
                }),
                output_instructions: None,
            },
            context_policy: None,
            tool_policy: None,
            permission_cap: Some(pioneer_protocol::task_permission_cap_from_snapshot(
                &pioneer_protocol::default_turn_permission_profile_snapshot(),
            )),
            security_cap: None,
            result_contract: None,
            review_policy: None,
            depth: 0,
            max_depth: 1,
            created_at: 1,
            updated_at: 1,
        };

        let inputs = materialize_child_task_input("prompt".to_owned(), &agent_spec);

        assert!(matches!(inputs[0], UserInput::Text { .. }));
        assert!(inputs.iter().any(|input| matches!(
            input,
            UserInput::Artifact { artifact_id, .. } if artifact_id == "art_input"
        )));
        assert!(inputs.iter().any(|input| matches!(
            input,
            UserInput::Artifact { artifact_id, .. } if artifact_id == "art_ref"
        )));
    }

    #[test]
    fn task_artifact_parser_preserves_version_id() {
        let values = vec![TaskValue::Object(BTreeMap::from([
            (
                "artifactId".to_owned(),
                TaskValue::String("artifact".to_owned()),
            ),
            (
                "versionId".to_owned(),
                TaskValue::String("version".to_owned()),
            ),
        ]))];

        let artifacts = task_artifacts::parse_task_artifacts(&values);

        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].artifact_id.as_deref(), Some("artifact"));
        assert_eq!(artifacts[0].version_id.as_deref(), Some("version"));
    }

    #[test]
    fn reviewer_advisory_parser_reads_json_decision_feedback_and_confidence() {
        let advisory = parse_reviewer_advisory_text(
            r#"{"decision":"request_changes","feedback":"tighten the summary","confidence":0.8}"#,
        );

        assert_eq!(advisory.decision, TaskResultReviewDecision::RequestChanges);
        assert_eq!(
            advisory.feedback_text.as_deref(),
            Some("tighten the summary")
        );
        assert_eq!(advisory.confidence, Some(0.8));
        assert!(matches!(advisory.feedback, Some(TaskValue::Object(_))));
    }

    #[test]
    fn reviewer_advisory_parser_falls_back_to_text_decision() {
        let advisory = parse_reviewer_advisory_text("I would accept this result.");

        assert_eq!(advisory.decision, TaskResultReviewDecision::Accept);
        assert_eq!(
            advisory.feedback_text.as_deref(),
            Some("I would accept this result.")
        );
    }

    #[tokio::test]
    async fn task_artifact_existing_id_gets_task_result_binding() {
        let (processor, task, task_run_turn, lineage) = task_artifact_harness("existing").await;
        let source = ingest_task_test_artifact(
            &processor,
            task.workspace_id.as_str(),
            task.created_by_thread_id.clone(),
            "source.txt",
        )
        .await;
        let result = TaskResult {
            summary: Some("done".to_owned()),
            data: None,
            artifacts: vec![TaskArtifact {
                artifact_id: Some(source.artifact.artifact_id.clone()),
                version_id: source.artifact.version_id.clone(),
                path: None,
                url: None,
                mime_type: None,
                metadata: None,
            }],
            completed_by_run_id: Some(task_run_turn.run_id.clone()),
        };

        let normalized = task_artifacts::normalize_task_result_artifacts(
            &processor,
            &task,
            &task_run_turn,
            &lineage,
            &task_artifacts::TaskResultArtifactAuthorization::structural_test_proof(),
            result,
        )
        .await
        .expect("normalize")
        .expect("valid result");

        assert_eq!(
            normalized.artifacts[0].artifact_id.as_deref(),
            Some(source.artifact.artifact_id.as_str())
        );
        let summary = processor
            .artifact_service
            .get_artifact(
                task.workspace_id.as_str(),
                source.artifact.artifact_id.as_str(),
                None,
            )
            .await
            .expect("artifact");
        assert!(summary.bindings.iter().any(|binding| {
            binding.binding_kind == ArtifactBindingKind::TaskResult
                && binding.task_id.as_deref() == Some(task.id.as_str())
                && binding.task_run_id.as_deref() == Some(task_run_turn.run_id.as_str())
                && binding.thread_id.as_deref() == task.created_by_thread_id.as_deref()
        }));
    }

    #[tokio::test]
    async fn review_candidate_artifact_existing_id_gets_task_result_candidate_binding() {
        let (processor, task, task_run_turn, lineage) =
            task_artifact_harness("candidate_existing").await;
        let source = ingest_task_test_artifact(
            &processor,
            task.workspace_id.as_str(),
            task.created_by_thread_id.clone(),
            "candidate-source.txt",
        )
        .await;
        let candidate_id = "candidate_artifact_binding";
        let result = TaskResult {
            summary: Some("candidate".to_owned()),
            data: None,
            artifacts: vec![TaskArtifact {
                artifact_id: Some(source.artifact.artifact_id.clone()),
                version_id: source.artifact.version_id.clone(),
                path: None,
                url: None,
                mime_type: None,
                metadata: None,
            }],
            completed_by_run_id: Some(task_run_turn.run_id.clone()),
        };

        let normalized = task_artifacts::normalize_task_result_candidate_artifacts(
            &processor,
            &task,
            &task_run_turn,
            &lineage,
            candidate_id,
            &task_artifacts::TaskResultArtifactAuthorization::structural_test_proof(),
            result,
        )
        .await
        .expect("normalize")
        .expect("valid result");

        assert_eq!(
            normalized.artifacts[0].artifact_id.as_deref(),
            Some(source.artifact.artifact_id.as_str())
        );
        let summary = processor
            .artifact_service
            .get_artifact(
                task.workspace_id.as_str(),
                source.artifact.artifact_id.as_str(),
                None,
            )
            .await
            .expect("artifact");
        assert!(summary.bindings.iter().any(|binding| {
            binding.binding_kind == ArtifactBindingKind::TaskResultCandidate
                && binding.task_id.as_deref() == Some(task.id.as_str())
                && binding.task_run_id.as_deref() == Some(task_run_turn.run_id.as_str())
                && binding.thread_id.as_deref() == task.created_by_thread_id.as_deref()
                && binding.item_index == Some(0)
        }));
        assert!(!summary.bindings.iter().any(|binding| {
            binding.binding_kind == ArtifactBindingKind::TaskResult
                && binding.task_id.as_deref() == Some(task.id.as_str())
                && binding.task_run_id.as_deref() == Some(task_run_turn.run_id.as_str())
        }));
        let page = processor
            .artifact_service
            .list_artifacts(
                task.workspace_id.as_str(),
                ArtifactListFilter {
                    task_id: Some(task.id.clone()),
                    task_run_id: Some(task_run_turn.run_id.clone()),
                    ..ArtifactListFilter::default()
                },
            )
            .await
            .expect("list task artifacts");
        assert_eq!(page.items.len(), 1);
        assert!(
            page.items[0]
                .bindings
                .iter()
                .any(|binding| binding.binding_kind == ArtifactBindingKind::TaskResultCandidate)
        );
    }

    #[tokio::test]
    async fn task_artifact_path_is_rejected_without_an_execution_owned_handle() {
        let (processor, task, task_run_turn, lineage) = task_artifact_harness("path").await;
        let output_dir = std::env::current_dir()
            .expect("cwd")
            .join("target")
            .join("task-artifact-tests")
            .join(task_run_turn.run_id.as_str());
        tokio::fs::create_dir_all(output_dir.as_path())
            .await
            .expect("mkdir");
        let output_path = output_dir.join("result.txt");
        tokio::fs::write(output_path.as_path(), b"path artifact")
            .await
            .expect("write");
        let result = TaskResult {
            summary: Some("done".to_owned()),
            data: None,
            artifacts: vec![TaskArtifact {
                artifact_id: None,
                version_id: None,
                path: Some(output_path.display().to_string()),
                url: None,
                mime_type: Some("text/plain".to_owned()),
                metadata: None,
            }],
            completed_by_run_id: Some(task_run_turn.run_id.clone()),
        };

        let error = task_artifacts::normalize_task_result_artifacts(
            &processor,
            &task,
            &task_run_turn,
            &lineage,
            &task_artifacts::TaskResultArtifactAuthorization::structural_test_proof(),
            result,
        )
        .await
        .expect("normalization should return a task error")
        .expect_err("raw paths must not become task artifacts");

        assert_eq!(error.code, "task_artifact_invalid");
        assert!(
            error
                .message
                .contains("raw task result paths and URLs are not accepted")
        );
        let page = processor
            .artifact_service
            .list_artifacts(
                task.workspace_id.as_str(),
                ArtifactListFilter {
                    task_id: Some(task.id.clone()),
                    task_run_id: Some(task_run_turn.run_id.clone()),
                    ..ArtifactListFilter::default()
                },
            )
            .await
            .expect("list task artifacts");
        assert!(page.items.is_empty());
        let _ = tokio::fs::remove_dir_all(output_dir.as_path()).await;
    }

    #[tokio::test]
    async fn task_artifact_rejects_cross_workspace_id() {
        let (processor, task, task_run_turn, lineage) = task_artifact_harness("foreign").await;
        let other_workspace = processor
            .workspace_manager
            .create_workspace("task_artifact_other", Some("Task Artifact Other"))
            .await
            .expect("other workspace");
        let source =
            ingest_task_test_artifact(&processor, other_workspace.id.as_str(), None, "foreign.txt")
                .await;
        let result = TaskResult {
            summary: None,
            data: None,
            artifacts: vec![TaskArtifact {
                artifact_id: Some(source.artifact.artifact_id),
                version_id: source.artifact.version_id,
                path: None,
                url: None,
                mime_type: None,
                metadata: None,
            }],
            completed_by_run_id: Some(task_run_turn.run_id.clone()),
        };

        let error = task_artifacts::normalize_task_result_artifacts(
            &processor,
            &task,
            &task_run_turn,
            &lineage,
            &task_artifacts::TaskResultArtifactAuthorization::structural_test_proof(),
            result,
        )
        .await
        .expect("normalization should return task error")
        .expect_err("foreign artifact should fail result");

        assert_eq!(error.code, "task_artifact_invalid");
    }

    #[tokio::test]
    async fn task_artifact_rejects_unbound_workspace_artifact() {
        let (processor, task, task_run_turn, lineage) = task_artifact_harness("unbound").await;
        let source =
            ingest_task_test_artifact(&processor, task.workspace_id.as_str(), None, "unbound.txt")
                .await;
        let result = TaskResult {
            summary: None,
            data: None,
            artifacts: vec![TaskArtifact {
                artifact_id: Some(source.artifact.artifact_id),
                version_id: source.artifact.version_id,
                path: None,
                url: None,
                mime_type: None,
                metadata: None,
            }],
            completed_by_run_id: Some(task_run_turn.run_id.clone()),
        };

        let error = task_artifacts::normalize_task_result_artifacts(
            &processor,
            &task,
            &task_run_turn,
            &lineage,
            &task_artifacts::TaskResultArtifactAuthorization::structural_test_proof(),
            result,
        )
        .await
        .expect("normalization should return task error")
        .expect_err("an unbound artifact must not be shared into a collaboration root");

        assert_eq!(error.code, "task_artifact_invalid");
    }

    #[tokio::test]
    async fn include_artifacts_context_renders_refs_without_paths() {
        let (processor, task, _task_run_turn, lineage) = task_artifact_harness("context").await;
        let source = ingest_task_test_artifact(
            &processor,
            task.workspace_id.as_str(),
            task.created_by_thread_id.clone(),
            "context.txt",
        )
        .await;
        let rendered = task_artifacts::render_parent_artifact_refs(
            &processor,
            task.workspace_id.as_str(),
            &TaskParentRuntimeContext {
                parent_thread_id: lineage.parent_thread_id,
                parent_turn_id: lineage.created_by_turn_id,
                home_root_thread_id: lineage.root_thread_id.clone(),
                root_thread_id: lineage.root_thread_id,
            },
        )
        .await
        .expect("render")
        .expect("artifact context");

        assert!(rendered.contains(source.artifact.artifact_id.as_str()));
        assert!(rendered.contains("context.txt"));
        assert!(!rendered.contains("source_path"));
    }

    #[test]
    fn result_extractor_falls_back_when_structured_block_violates_schema() {
        let raw_text = r#"Plain fallback result.
```task-result
{"summary":"Bad structured summary","data":{"answer":42}}
```"#;

        let result = TaskAgentResultExtractor::normalize_final_message(
            raw_text.to_owned(),
            "item".to_owned(),
            &test_task_run_turn(),
            Some(&json_answer_contract()),
        )
        .expect("invalid structured result should fallback to text");

        assert_eq!(result.summary.as_deref(), Some("Plain fallback result."));
        let TaskValue::Object(data) = result.data.expect("data should be present") else {
            panic!("fallback data should be an object");
        };
        assert_eq!(data.get("fallbackUsed"), Some(&TaskValue::Bool(true)));
        let diagnostics = data.get("diagnostics").expect("diagnostics should exist");
        let TaskValue::List(diagnostics) = diagnostics else {
            panic!("diagnostics should be a list");
        };
        assert!(!diagnostics.is_empty());
    }
}
