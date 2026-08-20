//! Task actor/occurrence contract materialization.

use anyhow::{Result, anyhow, bail};
use pioneer_protocol::{
    PersistedActorRef, Task, TaskActorContract, TaskAgentReviewMode, TaskAgentSpec,
    TaskDeliveryActorContract, TaskDeliveryMode, TaskOccurrenceContract, TaskOccurrenceStatus,
    TaskReviewerIntent,
};
use sha2::{Digest, Sha256};

use crate::policy::TaskCreateContext;

/// Build the immutable task-level actor contract at create time. The input
/// actor is supplied by the already-authenticated Gateway/task boundary; this
/// function never consults the latest message or current session.
pub fn build_task_actor_contract(
    task: &Task,
    agent_spec: Option<&TaskAgentSpec>,
    context: &TaskCreateContext,
    now: i64,
) -> Result<TaskActorContract> {
    let creator = persisted_actor_from_context(
        context.actor_id.as_deref(),
        context.creator_presentation_snapshot.as_ref(),
    )?;
    if task.executor_kind == pioneer_protocol::TaskExecutorKind::Agent
        && context.execution_admission.is_some()
        && creator == PersistedActorRef::System
    {
        bail!("agent Task admission requires an explicit persisted creator actor");
    }
    let reviewer = match agent_spec.and_then(|spec| spec.review_policy.as_ref()) {
        Some(policy)
            if matches!(
                policy.mode,
                TaskAgentReviewMode::ParentAgent | TaskAgentReviewMode::ParentAgentWithReviewers
            ) =>
        {
            TaskReviewerIntent::Parent
        }
        Some(policy) if policy.mode == TaskAgentReviewMode::UserApproval => {
            TaskReviewerIntent::Human {
                principal_id: context
                    .execution_admission
                    .as_ref()
                    .map(|seed| seed.initiating_principal_id.clone())
                    .or_else(|| context.actor_id.clone())
                    .ok_or_else(|| anyhow!("user review requires an exact principal actor"))?,
            }
        }
        _ => TaskReviewerIntent::RuntimeAuto,
    };
    let delivery_policy = task.delivery_policy.as_ref();
    let enabled = delivery_policy.is_some_and(|policy| policy.mode != TaskDeliveryMode::None);
    let delivery = TaskDeliveryActorContract {
        enabled,
        destination_thread_id: delivery_policy.and_then(|policy| policy.thread_id.clone()),
        destination_user_id: delivery_policy
            .filter(|policy| policy.mode == TaskDeliveryMode::UserNotification)
            .and_then(|_| {
                context
                    .execution_admission
                    .as_ref()
                    .map(|admission| admission.initiating_principal_id.clone())
                    .or_else(|| {
                        (task.owner_kind == pioneer_protocol::TaskOwnerKind::User)
                            .then(|| task.owner_id.clone())
                            .flatten()
                    })
            }),
        destination_webhook_url_fingerprint: delivery_policy
            .filter(|policy| policy.mode == TaskDeliveryMode::Webhook)
            .and_then(|policy| policy.webhook_url.as_deref())
            .map(delivery_target_fingerprint),
        route_id: context.delivery_route_id.clone(),
        return_route_id: None,
        // Result authorship belongs to the occurrence AgentExecution and is
        // materialized only after that execution exists. The Task creator is
        // authority/lineage, never a fallback delivery author.
        author_snapshot: None,
        route_receipt_json: context.delivery_route_receipt_json.clone(),
        disclosure_generation: 1,
        route_expires_at_millis: context.delivery_route_expires_at_millis,
    };
    let resolved_launch_present = context.resolved_launch_identity.is_some();
    if resolved_launch_present != context.resolved_launch_profile.is_some()
        || resolved_launch_present != context.agent_authorization_grant.is_some()
    {
        bail!("Task launch resolution must contain identity, profile, and authorization grant");
    }
    if let (Some(identity), Some(profile)) = (
        context.resolved_launch_identity.as_ref(),
        context.resolved_launch_profile.as_ref(),
    ) {
        if identity.source_kind != pioneer_protocol::AgentIdentitySourceKind::Ephemeral
            && !profile
                .compatible_agent_identity_ids
                .iter()
                .any(|candidate| candidate == &identity.id)
        {
            bail!("resolved Task launch profile is incompatible with its exact identity");
        }
    }
    if let Some(grant) = context.agent_authorization_grant.as_ref() {
        if grant.role_key.trim().is_empty()
            || grant.allowed_actions.is_empty()
            || grant.fingerprint.len() != 64
            || !grant
                .fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            bail!("resolved Task launch authorization grant is malformed");
        }
        let mut unique_actions = grant.allowed_actions.clone();
        unique_actions.sort();
        unique_actions.dedup();
        if unique_actions != grant.allowed_actions {
            bail!("resolved Task launch authorization actions are not canonical");
        }
    }
    let derived_child_launch_grant_json = context
        .resolved_launch_identity
        .as_ref()
        .zip(context.resolved_launch_profile.as_ref())
        .zip(context.agent_authorization_grant.as_ref())
        .map(|((identity, profile), authorization)| {
            serde_json::to_string(
                &pioneer_protocol::TaskDerivedChildLaunchGrant::ResolvedTaskLaunch {
                    identity: identity.clone(),
                    profile: profile.clone(),
                    role_key: authorization.role_key.clone(),
                    agent_policy_generation: authorization.policy_generation,
                    allowed_actions: authorization.allowed_actions.clone(),
                    agent_authorization_fingerprint: authorization.fingerprint.clone(),
                    child_launch_grant: authorization.child_launch_grant.clone(),
                },
            )
        })
        .transpose()?;
    let contract = TaskActorContract {
        task_id: task.id.clone(),
        workspace_id: task.workspace_id.clone(),
        creator: creator.clone(),
        creator_presentation_snapshot: context.creator_presentation_snapshot.clone(),
        reviewer,
        execution_destination_thread_id: context.execution_destination_thread_id.clone(),
        execution_route_id: context.execution_route_id.clone(),
        execution_route_receipt_json: context.execution_route_receipt_json.clone(),
        execution_route_expires_at_millis: context.execution_route_expires_at_millis,
        delivery,
        launch: context.launch_selection.clone(),
        requested_identity_json: context
            .launch_selection
            .as_ref()
            .map(|selection| serde_json::to_string(&selection.agent))
            .transpose()?,
        resolved_identity_id: context
            .resolved_launch_identity
            .as_ref()
            .map(|identity| identity.id.as_str().to_owned()),
        resolved_profile_id: context
            .resolved_launch_profile
            .as_ref()
            .map(|profile| profile.id.as_str().to_owned()),
        source_config_fingerprint: context
            .resolved_launch_identity
            .as_ref()
            .map(|identity| identity.source_fingerprint.clone()),
        derived_child_launch_grant_json,
        creator_work_graph_root_execution_id: context.creator_work_graph_root_execution_id.clone(),
        work_graph_root_execution_id: context.work_graph_root_execution_id.clone(),
        root_resource_scope_id: context.work_graph_root_execution_id.clone(),
        accounting_attribution: Some(creator.clone()),
        controller_principal_id: context
            .execution_admission
            .as_ref()
            .map(|seed| seed.initiating_principal_id.clone()),
        revision: 1,
    };
    if let PersistedActorRef::AgentExecution(execution_id) = &contract.creator {
        let snapshot = contract
            .creator_presentation_snapshot
            .as_ref()
            .ok_or_else(|| {
                anyhow!("agent Task creator requires an immutable presentation snapshot")
            })?;
        if &snapshot.agent_execution_id != execution_id {
            bail!("task creator presentation snapshot does not match exact execution actor");
        }
    }
    contract
        .validate()
        .map_err(|error| anyhow!("task actor contract validation failed: {error:?}"))?;
    let _ = now;
    Ok(contract)
}

pub(crate) fn delivery_target_fingerprint(target: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(target.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn build_occurrence_contract(
    task: &Task,
    actor_contract: &TaskActorContract,
    run_id: &str,
    trigger_id: Option<&str>,
    occurrence_key: impl Into<String>,
    execution_generation: u64,
    now: i64,
) -> Result<TaskOccurrenceContract> {
    let contract = TaskOccurrenceContract {
        occurrence_id: run_id.to_owned(),
        task_id: task.id.clone(),
        run_id: run_id.to_owned(),
        trigger_id: trigger_id.map(str::to_owned),
        occurrence_key: occurrence_key.into(),
        execution_generation,
        agent_execution_id: None,
        work_graph_root_execution_id: None,
        root_resource_scope_id: None,
        status: TaskOccurrenceStatus::Queued,
        queue_position: None,
        retry_attempt: 0,
        action_idempotency_key: format!("task:{}:{}", task.id, run_id),
        route_id: actor_contract.execution_route_id.clone(),
        result_return_route_id: actor_contract.delivery.route_id.clone(),
        terminal_reason: None,
    };
    contract
        .validate()
        .map_err(|error| anyhow!("task occurrence contract validation failed: {error:?}"))?;
    let _ = now;
    Ok(contract)
}

fn persisted_actor_from_context(
    actor_id: Option<&str>,
    creator_snapshot: Option<&pioneer_protocol::AgentPresentationSnapshot>,
) -> Result<PersistedActorRef> {
    match (actor_id, creator_snapshot) {
        (None, None) => Ok(PersistedActorRef::System),
        (None, Some(_)) => bail!("agent Task creator snapshot requires an exact actor id"),
        (Some(actor_id), Some(_)) => pioneer_protocol::AgentExecutionId::new(actor_id.to_owned())
            .map(PersistedActorRef::AgentExecution)
            .map_err(|_| anyhow!("task actor boundary received an invalid AgentExecution id")),
        (Some(actor_id), None) => pioneer_protocol::PrincipalId::new(actor_id.to_owned())
            .map(PersistedActorRef::Principal)
            .map_err(|_| anyhow!("task actor boundary received an invalid Principal id")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        TaskDeliveryFormat, TaskDeliveryPolicy, TaskExecutorKind, TaskLifecyclePolicy,
        TaskOwnerKind, TaskStatus,
    };

    fn task() -> Task {
        Task {
            id: "TASK123456789012345".to_owned(),
            workspace_id: "WORK123456789012345".to_owned(),
            owner_kind: TaskOwnerKind::System,
            owner_id: None,
            created_by_thread_id: None,
            created_by_turn_id: None,
            root_task_id: None,
            parent_task_id: None,
            executor_kind: TaskExecutorKind::System,
            status: TaskStatus::Draft,
            title: "task".to_owned(),
            goal: "goal".to_owned(),
            priority: 0,
            lifecycle_policy: Some(TaskLifecyclePolicy {
                attachment: pioneer_protocol::TaskAttachmentMode::Detached,
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
        }
    }

    #[test]
    fn system_task_has_explicit_system_creator() {
        let contract =
            build_task_actor_contract(&task(), None, &TaskCreateContext::default(), 1).unwrap();
        assert_eq!(contract.creator, PersistedActorRef::System);
        assert!(!contract.delivery.enabled);
    }

    #[test]
    fn creator_snapshot_disambiguates_agent_execution_from_principal_id() {
        let execution_id = pioneer_protocol::AgentExecutionId::new("E12345678901234567890")
            .expect("valid execution id");
        let snapshot = pioneer_protocol::AgentPresentationSnapshot {
            agent_identity_id: pioneer_protocol::AgentIdentityId::new("A12345678901234567890")
                .expect("valid identity id"),
            agent_execution_id: execution_id.clone(),
            identity_source_kind: pioneer_protocol::AgentIdentitySourceKind::NativeAgent,
            identity_source_revision: 1,
            display_name: "Agent".to_owned(),
            nickname: "agent".to_owned(),
            avatar_revision: None,
            role_label: None,
        };

        assert_eq!(
            persisted_actor_from_context(Some(execution_id.as_str()), Some(&snapshot)).unwrap(),
            PersistedActorRef::AgentExecution(execution_id)
        );
        assert!(matches!(
            persisted_actor_from_context(Some("P12345678901234567890"), None).unwrap(),
            PersistedActorRef::Principal(_)
        ));
    }

    #[test]
    fn resolved_launch_facts_are_an_atomic_contract() {
        let context = TaskCreateContext {
            agent_authorization_grant: Some(crate::TaskAgentAuthorizationGrantSeed {
                role_key: "thread_agent".to_owned(),
                policy_generation: 1,
                allowed_actions: vec!["thread.read".to_owned()],
                fingerprint: "a".repeat(64),
                child_launch_grant: pioneer_protocol::ChildAgentLaunchGrantSet::new(
                    vec![
                        pioneer_protocol::AgentIdentityProjection::new(
                            pioneer_protocol::AgentIdentityId::new("A12345678901234567890")
                                .unwrap(),
                            pioneer_protocol::AgentIdentitySourceKind::NativeAgent,
                            "Agent",
                            "agent",
                            None,
                            None,
                            1,
                            "source",
                        )
                        .unwrap(),
                    ],
                    Vec::new(),
                )
                .unwrap(),
            }),
            ..Default::default()
        };
        assert!(build_task_actor_contract(&task(), None, &context, 1).is_err());
    }

    #[test]
    fn webhook_delivery_pins_only_a_safe_destination_fingerprint() {
        let mut task = task();
        task.delivery_policy = Some(TaskDeliveryPolicy {
            mode: TaskDeliveryMode::Webhook,
            thread_target: None,
            thread_id: None,
            webhook_url: Some("https://hooks.example.test/result".to_owned()),
            include_result: true,
            format: TaskDeliveryFormat::Summary,
        });
        let contract =
            build_task_actor_contract(&task, None, &TaskCreateContext::default(), 1).unwrap();
        assert_eq!(
            contract.delivery.destination_webhook_url_fingerprint,
            Some(delivery_target_fingerprint(
                "https://hooks.example.test/result"
            ))
        );
        assert!(contract.delivery.destination_thread_id.is_none());
        assert!(contract.delivery.destination_user_id.is_none());
    }
}
