//! Canonical AgentActionService boundary.
//!
//! Adapters only construct protocol intents.  This service normalizes opaque
//! selections, evaluates the already-resolved security envelope, performs
//! separate root-scope resource admission, and exposes a transaction handoff
//! for the CRUD commit/outbox API.

use pioneer_crud::{AgentActionInput, AgentCommitInput, canonical_agent_id, utc_now};
use pioneer_protocol::{
    AgentActionId, AgentActionIntent, AgentActionKind, AgentExecutionId, NormalizedAgentAction,
};
use sha2::{Digest, Sha256};

use super::{
    AgentRouteFacts, AgentSecurityEnvelope, AgentWorkResourcePolicy, AuthorizationService,
    ResourceAction, RootExecutionBinding, RouteAuthorizationRequest, authorize_route,
    safe_route_receipt,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AgentActionServiceError {
    MalformedIntent(String),
    PayloadBoundary,
    NotAuthorized(&'static str),
    TargetNotAuthorized(&'static str),
    ResourceBoundary,
    Commit(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreparedAgentAction {
    pub(crate) normalized: NormalizedAgentAction,
    pub(crate) required_action: ResourceAction,
    pub(crate) resource: Option<super::RunningPermit>,
    pub(crate) request_fingerprint: String,
    pub(crate) actor_identity_id: pioneer_protocol::AgentIdentityId,
    pub(crate) root_execution_id: AgentExecutionId,
    pub(crate) source_capsule_id: String,
    pub(crate) subject_role_key: String,
    pub(crate) policy_generation: u64,
    pub(crate) decision_policy_fingerprint: String,
    pub(crate) execution_grant_fingerprint: String,
    pub(crate) execution_grant_policy_generation: u64,
    pub(crate) execution_generation: u64,
    pub(crate) attempt_generation: u64,
    pub(crate) identity_source_revision: u64,
    pub(crate) identity_source_fingerprint: String,
    pub(crate) route: Option<AgentRouteFacts>,
    pub(crate) routed_disclosure_class: Option<&'static str>,
    /// Server-allocated descendant identity. Model input can neither provide
    /// nor influence this value. Scheduled Tasks allocate their occurrence
    /// execution only when that occurrence is admitted.
    pub(crate) spawned_execution_id: Option<AgentExecutionId>,
    pub(crate) idle_timeout_secs: i64,
    pub(crate) hard_timeout_secs: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AgentActionCommitProjection {
    pub(crate) action_id: AgentActionId,
    pub(crate) execution_id: AgentExecutionId,
    pub(crate) kind: AgentActionKind,
    pub(crate) queued: bool,
    pub(crate) receipt_id: String,
    pub(crate) outbox_id: String,
}

#[derive(Clone, Debug)]
pub(crate) struct AgentActionCommitPlan {
    pub(crate) input: AgentCommitInput,
    pub(crate) projection: AgentActionCommitProjection,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CanonicalAgentActionService {
    pub(crate) authorization: AuthorizationService,
}

impl CanonicalAgentActionService {
    pub(crate) fn normalize(
        &self,
        intent: &AgentActionIntent,
    ) -> Result<NormalizedAgentAction, AgentActionServiceError> {
        intent.normalize().map_err(|error| match error {
            pioneer_protocol::AgentActionNormalizationError::PayloadLimitExceeded => {
                AgentActionServiceError::PayloadBoundary
            }
            error => AgentActionServiceError::MalformedIntent(format!("{error:?}")),
        })
    }

    fn action_for(intent: &AgentActionIntent, kind: AgentActionKind) -> ResourceAction {
        match kind {
            AgentActionKind::SendMessage => ResourceAction::MessageCreate,
            AgentActionKind::CreateThread => match intent {
                AgentActionIntent::CreateThread { option, .. } => match option.audience {
                    pioneer_protocol::AgentThreadAudienceTemplate::HomeCapsule => {
                        ResourceAction::ThreadCreatePrivate
                    }
                    pioneer_protocol::AgentThreadAudienceTemplate::RootDelegation => {
                        ResourceAction::ThreadCreateWorkspace
                    }
                },
                _ => unreachable!("normalized create-thread kind comes from a thread intent"),
            },
            AgentActionKind::StartAgent => ResourceAction::ChildStart,
            AgentActionKind::CreateTask => ResourceAction::TaskCreate,
            AgentActionKind::ScheduleTask => ResourceAction::TaskScheduleManage,
            AgentActionKind::ReviewTaskResult => ResourceAction::TaskReview,
            AgentActionKind::ControlTask => match intent {
                AgentActionIntent::ControlTask {
                    control: pioneer_protocol::AgentTaskControl::Cancel,
                    ..
                } => ResourceAction::TaskCancel,
                AgentActionIntent::ControlTask {
                    control: pioneer_protocol::AgentTaskControl::Detach,
                    ..
                } => ResourceAction::TaskDetach,
                AgentActionIntent::ControlTask {
                    control: pioneer_protocol::AgentTaskControl::Resume,
                    ..
                } => ResourceAction::TaskScheduleManage,
                _ => unreachable!("normalized control kind comes from a control intent"),
            },
            AgentActionKind::DeliverResult => ResourceAction::MessageCreate,
        }
    }

    pub(crate) fn prepare(
        &mut self,
        intent: &AgentActionIntent,
        envelope: &AgentSecurityEnvelope,
        root: &RootExecutionBinding,
        route: Option<&AgentRouteFacts>,
        same_capsule_thread_ids: &[String],
        policy: &AgentWorkResourcePolicy,
        branch_key: impl Into<String>,
        attempt_generation: u64,
        current_policy_generation: u64,
        depth: u16,
    ) -> Result<PreparedAgentAction, AgentActionServiceError> {
        if root.execution_generation == 0 || attempt_generation == 0 {
            return Err(AgentActionServiceError::NotAuthorized(
                "execution attempt binding is invalid",
            ));
        }
        let normalized = self.normalize(intent)?;
        if normalized.execution_id != root.execution_id {
            return Err(AgentActionServiceError::NotAuthorized(
                "action execution does not match pinned execution",
            ));
        }
        if envelope.fingerprint != root.envelope.fingerprint {
            return Err(AgentActionServiceError::NotAuthorized(
                "authorization envelope is stale",
            ));
        }
        if current_policy_generation != envelope.policy_generation {
            return Err(AgentActionServiceError::NotAuthorized(
                "authorization policy generation is stale",
            ));
        }
        let required_action = Self::action_for(intent, normalized.kind);
        self.require_source_actions(
            envelope,
            std::iter::once(required_action)
                .chain(additional_source_actions(normalized.kind).iter().copied()),
        )?;
        if let Some(launch) = normalized.launch.as_ref() {
            let inherited_profile = matches!(
                launch.execution.profile,
                pioneer_protocol::AgentExecutionProfileSelection::InheritParent
            )
            .then_some(&root.profile.id);
            launch.validate(inherited_profile).map_err(|_| {
                AgentActionServiceError::MalformedIntent(
                    "agent launch selection requires an exact bound profile".to_owned(),
                )
            })?;
            if !envelope.allows(ResourceAction::ChildStart) {
                return Err(AgentActionServiceError::NotAuthorized(
                    "agent role cannot start child execution",
                ));
            }
        }
        if let Some(target) = &normalized.target {
            if let Some(route_facts) = route {
                if route_facts.kind == pioneer_protocol::AgentRouteKind::ExecutionBound
                    && route_facts.source_execution_id != root.execution_id.as_str()
                {
                    return Err(AgentActionServiceError::TargetNotAuthorized(
                        "route is outside the bound execution capsule",
                    ));
                }
                if matches!(
                    target,
                    pioneer_protocol::AgentStartTarget::RoutedThread { .. }
                ) {
                    let requested_identity =
                        normalized
                            .launch
                            .as_ref()
                            .and_then(|launch| {
                                match &launch.agent {
                            pioneer_protocol::AgentIdentitySelection::Exact {
                                agent_identity_id,
                            } => Some(agent_identity_id),
                            pioneer_protocol::AgentIdentitySelection::InheritParent => {
                                Some(&root.identity.id)
                            }
                            pioneer_protocol::AgentIdentitySelection::DefaultPioneer
                            | pioneer_protocol::AgentIdentitySelection::ServerDerivedEphemeral {
                                ..
                            } => None,
                        }
                            });
                    let requested_profile = normalized.launch.as_ref().and_then(|launch| {
                        match &launch.execution.profile {
                            pioneer_protocol::AgentExecutionProfileSelection::Exact {
                                profile_id,
                            } => Some(profile_id),
                            pioneer_protocol::AgentExecutionProfileSelection::InheritParent => {
                                Some(&root.profile.id)
                            }
                        }
                    });
                    let routed_disclosure_class =
                        routed_disclosure_class(intent, route_facts.disclosure);
                    authorize_route(&RouteAuthorizationRequest {
                        route: route_facts,
                        source: &root.envelope,
                        source_execution_id: &root.execution_id,
                        action: normalized.kind,
                        target,
                        source_export_allowed: self.authorization.agent_action_allowed(
                            &envelope.role_key,
                            ResourceAction::AgentSourceExport,
                        ) && envelope
                            .allows(ResourceAction::AgentSourceExport),
                        destination_action_allowed: destination_route_actions(normalized.kind)
                            .iter()
                            .all(|action| {
                                self.authorization
                                    .agent_action_allowed(&envelope.role_key, *action)
                                    && envelope.allows(*action)
                            }),
                        disclosure_allowed: routed_disclosure_class.is_some(),
                        source_policy_generation: root.envelope.policy_generation,
                        destination_policy_generation: current_policy_generation,
                        requested_identity,
                        requested_profile,
                        now_millis: Some(utc_now().timestamp_millis()),
                    })
                    .map_err(|reason| {
                        AgentActionServiceError::TargetNotAuthorized(reason.public_message())
                    })?;
                }
                if !route_facts.permits_target(target) {
                    return Err(AgentActionServiceError::TargetNotAuthorized(
                        "target is outside durable route",
                    ));
                }
            } else {
                match target {
                    pioneer_protocol::AgentStartTarget::RoutedThread { .. } => {
                        return Err(AgentActionServiceError::TargetNotAuthorized(
                            "target has no active durable route",
                        ));
                    }
                    pioneer_protocol::AgentStartTarget::SameCapsuleThread { thread_id }
                        if thread_id != &root.home_root_thread_id
                            && !same_capsule_thread_ids
                                .iter()
                                .any(|candidate| candidate == thread_id) =>
                    {
                        return Err(AgentActionServiceError::TargetNotAuthorized(
                            "same-capsule target is outside the pinned collaboration root",
                        ));
                    }
                    pioneer_protocol::AgentStartTarget::CurrentThread
                    | pioneer_protocol::AgentStartTarget::SameCapsuleThread { .. } => {}
                }
            }
        }
        let idle_timeout_secs = i64::try_from(policy.idle_timeout_secs)
            .map_err(|_| AgentActionServiceError::ResourceBoundary)?;
        let hard_timeout_secs = i64::try_from(policy.hard_timeout_secs)
            .map_err(|_| AgentActionServiceError::ResourceBoundary)?;
        if idle_timeout_secs < 1 || hard_timeout_secs < idle_timeout_secs {
            return Err(AgentActionServiceError::ResourceBoundary);
        }

        let inherited_branch_key = branch_key.into();
        // A Task action creates a durable aggregate; its concrete occurrence
        // execution is admitted by the scheduler with the persisted Task
        // graph. Reserving a synthetic execution here would double-account
        // the same work and leave an orphan permit. Direct agent_start, by
        // contrast, materializes its child execution in this action.
        let spawned_execution_id = (normalized.kind == AgentActionKind::StartAgent)
            .then(|| execution_id_for_action(&normalized.action_id));
        let resource = if normalized.kind == AgentActionKind::StartAgent {
            if depth == 0 || depth > policy.max_depth {
                return Err(AgentActionServiceError::ResourceBoundary);
            }
            // This is only a deterministic write-set descriptor. Durable
            // `commit_agent_execution_graph` owns fan-out, capacity, permit
            // and queue admission inside its transaction. Keeping a second
            // mutable coordinator here could reject a valid action after a
            // restart or after a durable permit was released.
            let child_branch_key = if root.execution_id == root.work_graph_root_execution_id {
                format!(
                    "branch:{}",
                    spawned_execution_id
                        .as_ref()
                        .expect("resource-consuming actions allocate a child execution")
                )
            } else {
                inherited_branch_key
            };
            Some(super::RunningPermit {
                permit_id: 1,
                root_execution_id: root.work_graph_root_execution_id.clone(),
                execution_id: spawned_execution_id
                    .clone()
                    .expect("resource-consuming actions allocate a child execution"),
                // A newly materialized child owns its own first attempt. The
                // parent's current attempt only fences the authoring action.
                attempt_generation: 1,
                branch_key: child_branch_key,
            })
        } else {
            None
        };
        let request_fingerprint = request_fingerprint(&normalized);
        let routed_disclosure_class = route
            .filter(|route| !route.same_capsule)
            .and_then(|route| routed_disclosure_class(intent, route.disclosure));
        Ok(PreparedAgentAction {
            normalized,
            required_action,
            resource,
            request_fingerprint,
            actor_identity_id: envelope.identity_id.clone(),
            root_execution_id: root.work_graph_root_execution_id.clone(),
            source_capsule_id: envelope.root_capsule_id.clone(),
            subject_role_key: envelope.role_key.clone(),
            policy_generation: envelope.policy_generation,
            decision_policy_fingerprint: envelope.fingerprint.clone(),
            execution_grant_fingerprint: envelope.fingerprint.clone(),
            execution_grant_policy_generation: envelope.policy_generation,
            execution_generation: root.execution_generation,
            attempt_generation,
            identity_source_revision: root.identity_source_revision,
            identity_source_fingerprint: root.identity_source_fingerprint.clone(),
            route: route.cloned(),
            routed_disclosure_class,
            spawned_execution_id,
            idle_timeout_secs,
            hard_timeout_secs,
        })
    }

    /// Require extra source-side authority derived from server-resolved facts
    /// that are intentionally absent from the model intent (for example a
    /// nested Task parent or an immediate detached lifecycle).
    pub(crate) fn require_source_actions(
        &self,
        envelope: &AgentSecurityEnvelope,
        actions: impl IntoIterator<Item = ResourceAction>,
    ) -> Result<(), AgentActionServiceError> {
        if actions.into_iter().any(|action| {
            !self
                .authorization
                .agent_action_allowed(&envelope.role_key, action)
                || !envelope.allows(action)
        }) {
            return Err(AgentActionServiceError::NotAuthorized(
                "agent role does not allow action",
            ));
        }
        Ok(())
    }

    pub(crate) fn prepare_commit(
        &self,
        prepared: &PreparedAgentAction,
        response_json: Option<String>,
        policy_fingerprint: &str,
        current_policy_generation: u64,
        current_destination_policy_generation: u64,
    ) -> Result<AgentActionCommitPlan, AgentActionServiceError> {
        if current_policy_generation != prepared.policy_generation {
            return Err(AgentActionServiceError::NotAuthorized(
                "authorization policy changed before commit",
            ));
        }
        if policy_fingerprint != prepared.decision_policy_fingerprint {
            return Err(AgentActionServiceError::Commit(
                "commit policy fingerprint differs from the prepared decision".to_owned(),
            ));
        }
        if let Some(route) = prepared.route.as_ref() {
            let target = prepared.normalized.target.as_ref().ok_or(
                AgentActionServiceError::TargetNotAuthorized("routed action has no target"),
            )?;
            if !route.generations_match(
                prepared.policy_generation,
                current_destination_policy_generation,
            ) || !route.permits_action(prepared.normalized.kind)
                || !route.permits_target(target)
            {
                return Err(AgentActionServiceError::TargetNotAuthorized(
                    "route changed before commit",
                ));
            }
        }
        let now = utc_now();
        // These IDs are derived from the immutable action identity.  A
        // response-loss retry must address the same receipt/outbox/resource
        // rows rather than manufacture a second set of rows.
        let receipt_id = canonical_agent_id(
            'R',
            &format!(
                "agent-action-receipt\0{}\0{}",
                prepared.normalized.kind.safe_name(),
                prepared.normalized.action_id
            ),
        );
        let outbox_id = canonical_agent_id(
            'O',
            &format!(
                "agent-action-outbox\0{}\0{}",
                prepared.normalized.kind.safe_name(),
                prepared.normalized.action_id
            ),
        );
        let action_id = prepared.normalized.action_id.as_str().to_owned();
        let execution_id = prepared.normalized.execution_id.as_str().to_owned();
        let request_fingerprint = prepared.request_fingerprint.clone();
        let destination_scope_id = match prepared.normalized.target.as_ref() {
            Some(pioneer_protocol::AgentStartTarget::SameCapsuleThread { .. }) => {
                Some(prepared.source_capsule_id.clone())
            }
            Some(pioneer_protocol::AgentStartTarget::RoutedThread { .. }) => prepared
                .route
                .as_ref()
                .map(|route| route.destination_capsule_id.clone()),
            Some(pioneer_protocol::AgentStartTarget::CurrentThread) | None => None,
        };
        let source_policy_generation = i64::try_from(prepared.policy_generation).map_err(|_| {
            AgentActionServiceError::Commit(
                "source policy generation exceeds database range".to_owned(),
            )
        })?;
        let destination_policy_generation = prepared
            .route
            .as_ref()
            .map(|_| {
                i64::try_from(current_destination_policy_generation).map_err(|_| {
                    AgentActionServiceError::Commit(
                        "destination policy generation exceeds database range".to_owned(),
                    )
                })
            })
            .transpose()?;
        let route_generation = prepared
            .route
            .as_ref()
            .map(|route| {
                i64::try_from(route.generation).map_err(|_| {
                    AgentActionServiceError::Commit(
                        "route generation exceeds database range".to_owned(),
                    )
                })
            })
            .transpose()?;
        let disclosure_class = if prepared.route.is_some() {
            prepared.routed_disclosure_class.ok_or_else(|| {
                AgentActionServiceError::Commit(
                    "routed action has no exact disclosure class".to_owned(),
                )
            })?
        } else if destination_scope_id.is_some() {
            "same_capsule"
        } else {
            "source_capsule"
        };
        let resource = match prepared.resource.as_ref() {
            Some(permit) => {
                let resource_key = format!(
                    "agent-action-resource\0{}\0{}\0{}",
                    prepared.normalized.action_id, permit.execution_id, permit.attempt_generation
                );
                Some(pioneer_crud::AgentResourceCommitInput {
                    root_execution_id: permit.root_execution_id.as_str().to_owned(),
                    execution_id: permit.execution_id.as_str().to_owned(),
                    attempt_generation: permit.attempt_generation as i64,
                    branch_key: permit.branch_key.clone(),
                    fair_order: stable_resource_order(&resource_key),
                    resource_state_id: canonical_agent_id('S', &format!("{resource_key}\0state")),
                    permit_id: Some(canonical_agent_id('P', &format!("{resource_key}\0permit"))),
                    queue_id: None,
                    enqueue_sequence: None,
                    idle_timeout_secs: Some(prepared.idle_timeout_secs),
                    hard_timeout_secs: Some(prepared.hard_timeout_secs),
                })
            }
            None => None,
        };
        let mut outbox_payload = serde_json::json!({
            "action_id": action_id,
            "execution_id": execution_id,
            "normalized": prepared.normalized,
            "kind": prepared.normalized.kind.safe_name(),
            "actor_identity_id": prepared.actor_identity_id,
            "root_execution_id": prepared.root_execution_id,
            "spawned_execution_id": prepared.spawned_execution_id,
        });
        if let Some(route) = prepared.route.as_ref()
            && let Some(payload) = outbox_payload.as_object_mut()
        {
            payload.insert("route_id".to_owned(), serde_json::json!(route.route_id));
            payload.insert(
                "destination_thread_id".to_owned(),
                serde_json::json!(route.destination_thread_id),
            );
        }
        let input = AgentCommitInput {
            mutation_kind: prepared.normalized.kind.safe_name().to_owned(),
            idempotency_key: prepared.normalized.idempotency_key.clone(),
            request_fingerprint: request_fingerprint.clone(),
            actor_identity_id: prepared.actor_identity_id.as_str().to_owned(),
            action: AgentActionInput {
                id: action_id.clone(),
                execution_id: execution_id.clone(),
                action_kind: prepared.normalized.kind.safe_name().to_owned(),
                idempotency_key: prepared.normalized.idempotency_key.clone(),
                request_fingerprint,
                now,
            },
            receipt_id: receipt_id.clone(),
            outbox_id: outbox_id.clone(),
            action_response_json: response_json.clone(),
            receipt_response_json: response_json,
            route_receipt_json: prepared
                .route
                .as_ref()
                .map(|route| safe_route_receipt(route, prepared.normalized.kind)),
            outbox_payload_json: outbox_payload.to_string(),
            policy_fingerprint: policy_fingerprint.to_owned(),
            execution_grant_fingerprint: prepared.execution_grant_fingerprint.clone(),
            execution_grant_policy_generation: i64::try_from(
                prepared.execution_grant_policy_generation,
            )
            .map_err(|_| {
                AgentActionServiceError::Commit(
                    "execution grant policy generation exceeds database range".to_owned(),
                )
            })?,
            source_scope_id: prepared.source_capsule_id.clone(),
            destination_scope_id,
            subject_role_key: prepared.subject_role_key.clone(),
            authorized_resource_action: prepared.required_action.safe_name().to_owned(),
            source_policy_generation,
            destination_policy_generation,
            route_generation,
            disclosure_class: disclosure_class.to_owned(),
            expected_execution_generation: i64::try_from(prepared.execution_generation).map_err(
                |_| {
                    AgentActionServiceError::Commit(
                        "execution generation exceeds database range".to_owned(),
                    )
                },
            )?,
            expected_current_identity_source_revision: i64::try_from(
                prepared.identity_source_revision,
            )
            .map_err(|_| {
                AgentActionServiceError::Commit(
                    "identity source revision exceeds database range".to_owned(),
                )
            })?,
            expected_current_identity_source_fingerprint: prepared
                .identity_source_fingerprint
                .clone(),
            expected_attempt_generation: i64::try_from(prepared.attempt_generation).map_err(
                |_| {
                    AgentActionServiceError::Commit(
                        "execution attempt generation exceeds database range".to_owned(),
                    )
                },
            )?,
            expected_policy_generation: i64::try_from(prepared.policy_generation).map_err(
                |_| {
                    AgentActionServiceError::Commit(
                        "policy generation exceeds database range".to_owned(),
                    )
                },
            )?,
            requires_cross_capsule_route: prepared
                .route
                .as_ref()
                .is_some_and(|route| !route.same_capsule),
            resource,
        };
        Ok(AgentActionCommitPlan {
            projection: AgentActionCommitProjection {
                action_id: prepared.normalized.action_id.clone(),
                execution_id: prepared.normalized.execution_id.clone(),
                kind: prepared.normalized.kind,
                queued: false,
                receipt_id,
                outbox_id,
            },
            input,
        })
    }
}

fn destination_route_actions(kind: AgentActionKind) -> &'static [ResourceAction] {
    match kind {
        AgentActionKind::SendMessage | AgentActionKind::DeliverResult => {
            &[ResourceAction::MessageCreate]
        }
        AgentActionKind::StartAgent => &[ResourceAction::AgentTurnStart],
        AgentActionKind::CreateTask => &[ResourceAction::TaskCreate],
        AgentActionKind::ScheduleTask => &[
            ResourceAction::TaskCreate,
            ResourceAction::TaskScheduleManage,
        ],
        AgentActionKind::ReviewTaskResult => &[ResourceAction::TaskReview],
        AgentActionKind::CreateThread | AgentActionKind::ControlTask => &[],
    }
}

fn routed_disclosure_class(
    intent: &AgentActionIntent,
    disclosure: pioneer_protocol::AgentRouteDisclosurePolicy,
) -> Option<&'static str> {
    match intent {
        AgentActionIntent::SendMessage { input, .. }
        | AgentActionIntent::StartAgent {
            start: pioneer_protocol::StartAgentIntent { input, .. },
            ..
        } if disclosure.permits_authored_input(input) => {
            let has_text = input
                .as_slice()
                .iter()
                .any(|item| matches!(item, pioneer_protocol::UserInput::Text { .. }));
            let has_artifacts = input
                .as_slice()
                .iter()
                .any(|item| matches!(item, pioneer_protocol::UserInput::Artifact { .. }));
            Some(match (has_text, has_artifacts) {
                (true, true) => "routed_text_artifacts",
                (true, false) => "routed_text",
                (false, true) => "routed_artifacts",
                (false, false) => "routed_empty",
            })
        }
        // Task payload/context is held behind an opaque server template. The
        // Task adapter performs the exact field-by-field check before commit;
        // the canonical route decision still requires the dedicated class.
        AgentActionIntent::CreateTask { .. } | AgentActionIntent::ScheduleTask { .. } => {
            disclosure.user_input.then_some("routed_task_input")
        }
        AgentActionIntent::DeliverResult { .. } => match disclosure.result_return {
            pioneer_protocol::AgentResultReturnPolicy::None => None,
            pioneer_protocol::AgentResultReturnPolicy::SummaryOnly => Some("routed_result_summary"),
            pioneer_protocol::AgentResultReturnPolicy::FullResult => Some("routed_result_full"),
        },
        AgentActionIntent::CreateThread { .. }
        | AgentActionIntent::ReviewTaskResult { .. }
        | AgentActionIntent::ControlTask { .. } => {
            disclosure.allows_anything().then_some("routed_control")
        }
        _ => None,
    }
}

/// Source-side delegation and destination-domain authority are independent
/// facts even inside one collaboration capsule. The receipt keeps the primary
/// action, while this intersection prevents a narrowed immutable execution
/// grant from creating work with only half of the required authority.
fn additional_source_actions(kind: AgentActionKind) -> &'static [ResourceAction] {
    match kind {
        AgentActionKind::StartAgent => &[ResourceAction::AgentTurnStart],
        AgentActionKind::CreateTask => &[],
        AgentActionKind::ScheduleTask => &[ResourceAction::TaskCreate],
        AgentActionKind::SendMessage
        | AgentActionKind::CreateThread
        | AgentActionKind::ReviewTaskResult
        | AgentActionKind::ControlTask
        | AgentActionKind::DeliverResult => &[],
    }
}

pub(super) fn request_fingerprint(normalized: &NormalizedAgentAction) -> String {
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:agent-action:v1\0");
    digest.update(serde_json::to_vec(normalized).expect("normalized agent action must serialize"));
    hex::encode(digest.finalize())
}

fn stable_resource_order(value: &str) -> i64 {
    let digest = Sha256::digest(value.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    let order = i64::from_be_bytes(bytes) & i64::MAX;
    order.max(1)
}

fn execution_id_for_action(action_id: &AgentActionId) -> AgentExecutionId {
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:action-execution:v1\0");
    digest.update(action_id.as_str().as_bytes());
    AgentExecutionId::new(format!("E{}", &hex::encode(digest.finalize())[..20]))
        .expect("hashed action execution id is valid")
}

pub(crate) trait AgentActionKindName {
    fn safe_name(self) -> &'static str;
}

impl AgentActionKindName for AgentActionKind {
    fn safe_name(self) -> &'static str {
        match self {
            AgentActionKind::SendMessage => "send_message",
            AgentActionKind::CreateThread => "create_thread",
            AgentActionKind::StartAgent => "start_agent",
            AgentActionKind::CreateTask => "create_task",
            AgentActionKind::ScheduleTask => "schedule_task",
            AgentActionKind::ReviewTaskResult => "review_task_result",
            AgentActionKind::ControlTask => "control_task",
            AgentActionKind::DeliverResult => "deliver_result",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        AgentActionId, AgentAuthoredInput, AgentIdentityId, AgentStartTarget,
        AgentThreadAudienceTemplate, AgentThreadCreationOption,
    };

    #[test]
    fn malformed_intent_is_rejected_before_resource_admission() {
        let (action_id, execution_id) = (
            AgentActionId::new("X12345678901234567890").unwrap(),
            AgentExecutionId::new("E12345678901234567890").unwrap(),
        );
        let intent = AgentActionIntent::CreateThread {
            action_id,
            execution_id,
            option: AgentThreadCreationOption {
                option_id: String::new(),
                audience: AgentThreadAudienceTemplate::HomeCapsule,
            },
            idempotency_key: "key".to_owned(),
        };
        let service = CanonicalAgentActionService::default();
        assert!(matches!(
            service.normalize(&intent),
            Err(AgentActionServiceError::MalformedIntent(_))
        ));
        let _ = AgentAuthoredInput::default();
        let _ = AgentIdentityId::new("A12345678901234567890").unwrap();
        let _ = AgentStartTarget::CurrentThread;
    }

    #[test]
    fn source_action_intersections_cover_child_start_and_scheduling() {
        assert_eq!(
            additional_source_actions(AgentActionKind::StartAgent),
            &[ResourceAction::AgentTurnStart]
        );
        assert_eq!(additional_source_actions(AgentActionKind::CreateTask), &[]);
        assert_eq!(
            additional_source_actions(AgentActionKind::ScheduleTask),
            &[ResourceAction::TaskCreate]
        );
    }

    #[test]
    fn payload_limit_remains_typed_at_the_canonical_service_boundary() {
        let intent = AgentActionIntent::SendMessage {
            action_id: AgentActionId::new("X12345678901234567890").unwrap(),
            execution_id: AgentExecutionId::new("E12345678901234567890").unwrap(),
            target: AgentStartTarget::CurrentThread,
            input: AgentAuthoredInput::from(vec![pioneer_protocol::UserInput::Text {
                text: "x".repeat(pioneer_protocol::TURN_EXECUTION_INPUT_MAX_BYTES + 1),
                text_elements: Vec::new(),
            }]),
            idempotency_key: "oversized".to_owned(),
        };
        assert_eq!(
            CanonicalAgentActionService::default().normalize(&intent),
            Err(AgentActionServiceError::PayloadBoundary)
        );
    }
}
