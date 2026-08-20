use pioneer_protocol::{
    TaskAgentReviewMode, TaskAgentReviewPolicy, TaskResultCandidate, TaskResultCandidateStatus,
    TaskResultReviewDecision, TaskResultReviewEvent, TaskResultReviewEventKind,
    TaskResultReviewResolutionStrategy, TaskResultReviewerKind, TaskResultReviewerRef,
    TaskResultReviewerSpec, TaskValue,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub struct TaskResultReviewActor {
    pub reviewer_kind: TaskResultReviewerKind,
    pub reviewer: TaskResultReviewerRef,
    pub reviewer_thread_id: Option<String>,
    pub reviewer_turn_id: Option<String>,
    pub reviewer_user_id: Option<String>,
    pub reviewer_agent_spec_id: Option<String>,
}

impl TaskResultReviewActor {
    pub fn runtime_auto() -> Self {
        Self {
            reviewer_kind: TaskResultReviewerKind::RuntimeAuto,
            reviewer: TaskResultReviewerRef::RuntimePolicy,
            reviewer_thread_id: None,
            reviewer_turn_id: None,
            reviewer_user_id: None,
            reviewer_agent_spec_id: None,
        }
    }

    pub fn system() -> Self {
        Self {
            reviewer_kind: TaskResultReviewerKind::System,
            reviewer: TaskResultReviewerRef::RuntimePolicy,
            reviewer_thread_id: None,
            reviewer_turn_id: None,
            reviewer_user_id: None,
            reviewer_agent_spec_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordTaskResultReviewEventParams {
    pub candidate_id: String,
    pub review_event_id: Option<String>,
    pub actor: TaskResultReviewActor,
    pub event_kind: TaskResultReviewEventKind,
    pub decision: TaskResultReviewDecision,
    pub feedback_text: Option<String>,
    pub feedback: Option<TaskValue>,
    pub confidence: Option<f64>,
    pub supersedes_review_event_id: Option<String>,
    pub next_task_run_turn_id: Option<String>,
    pub created_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordUserTaskResultReviewEventParams {
    pub candidate_id: String,
    pub review_event_id: Option<String>,
    pub decision: TaskResultReviewDecision,
    pub feedback_text: Option<String>,
    pub feedback: Option<TaskValue>,
    pub confidence: Option<f64>,
    pub next_task_run_turn_id: Option<String>,
    pub created_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTaskResultReviewerContextParams {
    pub candidate_id: String,
    pub reviewer_index: usize,
    pub reviewer_spec: TaskResultReviewerSpec,
    pub reviewer_thread_id: String,
    pub reviewer_turn_id: String,
    pub created_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskResultReviewerContext {
    pub lineage: pioneer_protocol::TaskThreadLineage,
    pub binding: pioneer_protocol::TaskRunThreadBinding,
    pub task_run_turn: pioneer_protocol::TaskRunTurn,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskResultReviewRecordResponse {
    pub review_event: TaskResultReviewEvent,
    pub candidate: TaskResultCandidate,
    pub resolution: Option<TaskResultReviewCandidateResolution>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskResultReviewFinalActor {
    ParentAgent,
    User,
    RuntimeAuto,
    System,
    ReviewAgent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskResultReviewBlockReason {
    MissingRequiredReviewers(Vec<String>),
    QuorumNotReached,
    ParentFinalNotAllowed,
    UserFinalNotAllowed,
    ReviewAgentFinalNotAllowed,
    CandidateAlreadyResolved,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskResultReviewResolutionState {
    pub strategy: TaskResultReviewResolutionStrategy,
    pub parent_final_allowed: bool,
    pub user_final_allowed: bool,
    pub advisory_can_resolve: bool,
    pub required_reviewers_satisfied: bool,
    pub quorum_satisfied: bool,
    pub recommended_advisory_decision: Option<TaskResultReviewDecision>,
    pub blocking_reasons: Vec<TaskResultReviewBlockReason>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskResultReviewCandidateResolution {
    pub status: TaskResultCandidateStatus,
    pub review_event_id: String,
    pub resolved_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdvisoryClass {
    Accept,
    RequestChanges,
    Abstain,
}

pub fn task_result_reviewer_spec_key(index: usize, spec: &TaskResultReviewerSpec) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("{index}:").as_bytes());
    hasher.update(format!("{:?}:", spec.reviewer_kind).as_bytes());
    hasher.update(spec.agent_nickname.as_deref().unwrap_or("").as_bytes());
    hasher.update(b":");
    hasher.update(spec.agent_role.as_deref().unwrap_or("").as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("reviewer_{index}_{}", &digest[..16])
}

pub fn stable_review_thread_id(candidate_id: &str, reviewer_key: &str) -> String {
    stable_short_id("rvt", &[candidate_id, reviewer_key])
}

pub fn stable_review_turn_id(candidate_id: &str, reviewer_key: &str) -> String {
    stable_short_id("rvu", &[candidate_id, reviewer_key])
}

pub fn stable_review_event_id(
    candidate: &TaskResultCandidate,
    actor: &TaskResultReviewActor,
    event_kind: TaskResultReviewEventKind,
    decision: TaskResultReviewDecision,
) -> String {
    if let Some(turn_id) = actor.reviewer_turn_id.as_deref() {
        return format!(
            "trre_{}",
            stable_hash(&[candidate.id.as_str(), turn_id], 24)
        );
    }
    if let Some(user_id) = actor.reviewer_user_id.as_deref() {
        return format!(
            "trre_{}",
            stable_hash(
                &[
                    candidate.id.as_str(),
                    "user",
                    user_id,
                    review_event_kind_label(event_kind),
                    review_decision_label(decision),
                ],
                24,
            )
        );
    }
    format!(
        "trre_{}",
        stable_hash(
            &[
                candidate.id.as_str(),
                review_actor_label(actor.reviewer_kind),
                review_event_kind_label(event_kind),
                review_decision_label(decision),
            ],
            24,
        )
    )
}

pub fn build_task_result_review_event(
    candidate: &TaskResultCandidate,
    params: RecordTaskResultReviewEventParams,
    created_at: i64,
) -> TaskResultReviewEvent {
    let id = params.review_event_id.unwrap_or_else(|| {
        stable_review_event_id(candidate, &params.actor, params.event_kind, params.decision)
    });
    TaskResultReviewEvent {
        id,
        candidate_id: candidate.id.clone(),
        task_id: candidate.task_id.clone(),
        run_id: candidate.run_id.clone(),
        task_run_turn_id: candidate.task_run_turn_id.clone(),
        reviewer_kind: params.actor.reviewer_kind,
        reviewer: params.actor.reviewer,
        reviewer_thread_id: params.actor.reviewer_thread_id,
        reviewer_turn_id: params.actor.reviewer_turn_id,
        reviewer_user_id: params.actor.reviewer_user_id,
        reviewer_agent_spec_id: params.actor.reviewer_agent_spec_id,
        event_kind: params.event_kind,
        decision: params.decision,
        feedback_text: params.feedback_text,
        feedback: params.feedback,
        confidence: params.confidence,
        supersedes_review_event_id: params.supersedes_review_event_id,
        next_task_run_turn_id: params.next_task_run_turn_id,
        created_at,
    }
}

pub fn evaluate_task_result_review_resolution(
    policy: Option<&TaskAgentReviewPolicy>,
    events: &[TaskResultReviewEvent],
) -> TaskResultReviewResolutionState {
    let strategy = policy
        .map(|policy| policy.resolution_strategy)
        .unwrap_or(TaskResultReviewResolutionStrategy::ParentFinal);
    let Some(policy) = policy else {
        return TaskResultReviewResolutionState {
            strategy,
            parent_final_allowed: true,
            user_final_allowed: false,
            advisory_can_resolve: false,
            required_reviewers_satisfied: true,
            quorum_satisfied: true,
            recommended_advisory_decision: None,
            blocking_reasons: Vec::new(),
        };
    };

    let latest = latest_advisory_by_reviewer(policy, events);
    let missing_required = missing_required_reviewers(policy, &latest);
    let required_reviewers_satisfied = missing_required.is_empty();
    let quorum = quorum_state(policy, &latest);
    let required_request_changes = required_reviewer_request_changes(policy, &latest);

    let mut state = TaskResultReviewResolutionState {
        strategy,
        parent_final_allowed: false,
        user_final_allowed: false,
        advisory_can_resolve: false,
        required_reviewers_satisfied,
        quorum_satisfied: quorum.0,
        recommended_advisory_decision: quorum.1,
        blocking_reasons: Vec::new(),
    };

    match strategy {
        TaskResultReviewResolutionStrategy::ParentFinal => {
            state.parent_final_allowed = true;
        }
        TaskResultReviewResolutionStrategy::UserFinal => {
            state.user_final_allowed = true;
        }
        TaskResultReviewResolutionStrategy::RequireAllRequiredReviewersThenParent => {
            state.parent_final_allowed = required_reviewers_satisfied;
        }
        TaskResultReviewResolutionStrategy::QuorumThenParent => {
            state.parent_final_allowed = state.quorum_satisfied;
        }
        TaskResultReviewResolutionStrategy::AnyRequiredReviewerCanRequestChanges => {
            state.parent_final_allowed = required_reviewers_satisfied || required_request_changes;
            state.advisory_can_resolve = required_request_changes;
            if required_request_changes {
                state.recommended_advisory_decision =
                    Some(TaskResultReviewDecision::RequestChanges);
            }
        }
    }

    if !required_reviewers_satisfied {
        state
            .blocking_reasons
            .push(TaskResultReviewBlockReason::MissingRequiredReviewers(
                missing_required,
            ));
    }
    if matches!(
        strategy,
        TaskResultReviewResolutionStrategy::QuorumThenParent
    ) && !state.quorum_satisfied
    {
        state
            .blocking_reasons
            .push(TaskResultReviewBlockReason::QuorumNotReached);
    }
    if matches!(policy.mode, TaskAgentReviewMode::UserApproval) {
        state.user_final_allowed = true;
    }

    state
}

pub fn review_event_candidate_resolution(
    candidate: &TaskResultCandidate,
    event: &TaskResultReviewEvent,
    policy: Option<&TaskAgentReviewPolicy>,
    prior_events: &[TaskResultReviewEvent],
    resolved_at: i64,
) -> Option<TaskResultReviewCandidateResolution> {
    let mut events = prior_events.to_vec();
    if !events.iter().any(|existing| existing.id == event.id) {
        events.push(event.clone());
    }
    let resolution_state = evaluate_task_result_review_resolution(policy, &events);
    if !review_event_can_resolve(event, &resolution_state) {
        return None;
    }
    let status = match event.decision {
        TaskResultReviewDecision::Accept => TaskResultCandidateStatus::Accepted,
        TaskResultReviewDecision::RequestChanges | TaskResultReviewDecision::Reject => {
            TaskResultCandidateStatus::Rejected
        }
        TaskResultReviewDecision::Cancel => TaskResultCandidateStatus::Cancelled,
        TaskResultReviewDecision::Abstain => return None,
    };
    Some(TaskResultReviewCandidateResolution {
        status,
        review_event_id: event.id.clone(),
        resolved_at: resolved_at.max(candidate.created_at),
    })
}

pub fn final_actor_for_reviewer_kind(
    reviewer_kind: TaskResultReviewerKind,
) -> TaskResultReviewFinalActor {
    match reviewer_kind {
        TaskResultReviewerKind::ParentAgent => TaskResultReviewFinalActor::ParentAgent,
        TaskResultReviewerKind::User => TaskResultReviewFinalActor::User,
        TaskResultReviewerKind::RuntimeAuto => TaskResultReviewFinalActor::RuntimeAuto,
        TaskResultReviewerKind::System => TaskResultReviewFinalActor::System,
        TaskResultReviewerKind::ReviewAgent => TaskResultReviewFinalActor::ReviewAgent,
    }
}

pub fn final_actor_allowed(
    actor: TaskResultReviewFinalActor,
    state: &TaskResultReviewResolutionState,
) -> bool {
    match actor {
        TaskResultReviewFinalActor::ParentAgent => state.parent_final_allowed,
        TaskResultReviewFinalActor::User => state.user_final_allowed,
        TaskResultReviewFinalActor::RuntimeAuto | TaskResultReviewFinalActor::System => true,
        TaskResultReviewFinalActor::ReviewAgent => state.advisory_can_resolve,
    }
}

fn review_event_can_resolve(
    event: &TaskResultReviewEvent,
    state: &TaskResultReviewResolutionState,
) -> bool {
    match event.event_kind {
        TaskResultReviewEventKind::Advisory => {
            state.advisory_can_resolve
                && matches!(
                    event.decision,
                    TaskResultReviewDecision::RequestChanges | TaskResultReviewDecision::Reject
                )
        }
        TaskResultReviewEventKind::Decision | TaskResultReviewEventKind::Override => {
            final_actor_allowed(final_actor_for_reviewer_kind(event.reviewer_kind), state)
        }
        TaskResultReviewEventKind::SystemAuto => true,
    }
}

fn latest_advisory_by_reviewer<'a>(
    policy: &TaskAgentReviewPolicy,
    events: &'a [TaskResultReviewEvent],
) -> BTreeMap<String, &'a TaskResultReviewEvent> {
    let reviewer_keys: BTreeSet<String> = policy
        .reviewers
        .iter()
        .enumerate()
        .map(|(index, spec)| task_result_reviewer_spec_key(index, spec))
        .collect();
    let mut latest = BTreeMap::new();
    for event in events {
        if event.event_kind != TaskResultReviewEventKind::Advisory {
            continue;
        }
        let Some(key) = reviewer_event_key(policy, event) else {
            continue;
        };
        if reviewer_keys.contains(&key) {
            latest.insert(key, event);
        }
    }
    latest
}

fn reviewer_event_key(
    policy: &TaskAgentReviewPolicy,
    event: &TaskResultReviewEvent,
) -> Option<String> {
    if let Some(key) = event.reviewer_agent_spec_id.as_deref() {
        return Some(key.to_owned());
    }
    policy
        .reviewers
        .iter()
        .enumerate()
        .find(|(_, spec)| spec.reviewer_kind == event.reviewer_kind)
        .map(|(index, spec)| task_result_reviewer_spec_key(index, spec))
}

fn missing_required_reviewers(
    policy: &TaskAgentReviewPolicy,
    latest: &BTreeMap<String, &TaskResultReviewEvent>,
) -> Vec<String> {
    policy
        .reviewers
        .iter()
        .enumerate()
        .filter(|(_, spec)| spec.required)
        .map(|(index, spec)| task_result_reviewer_spec_key(index, spec))
        .filter(|key| !latest.contains_key(key))
        .collect()
}

fn required_reviewer_request_changes(
    policy: &TaskAgentReviewPolicy,
    latest: &BTreeMap<String, &TaskResultReviewEvent>,
) -> bool {
    policy
        .reviewers
        .iter()
        .enumerate()
        .filter(|(_, spec)| spec.required)
        .any(|(index, spec)| {
            latest
                .get(&task_result_reviewer_spec_key(index, spec))
                .is_some_and(|event| {
                    matches!(
                        event.decision,
                        TaskResultReviewDecision::RequestChanges | TaskResultReviewDecision::Reject
                    )
                })
        })
}

fn quorum_state(
    policy: &TaskAgentReviewPolicy,
    latest: &BTreeMap<String, &TaskResultReviewEvent>,
) -> (bool, Option<TaskResultReviewDecision>) {
    if policy.reviewers.is_empty() {
        return (true, None);
    }
    let total_weight: f64 = policy
        .reviewers
        .iter()
        .map(|spec| spec.weight.unwrap_or(1.0).max(0.0))
        .sum::<f64>()
        .max(1.0);
    let mut accept = 0.0;
    let mut request_changes = 0.0;

    for (index, spec) in policy.reviewers.iter().enumerate() {
        let key = task_result_reviewer_spec_key(index, spec);
        let Some(event) = latest.get(&key) else {
            continue;
        };
        let weight = spec.weight.unwrap_or(1.0).max(0.0);
        match advisory_class(event.decision) {
            AdvisoryClass::Accept => accept += weight,
            AdvisoryClass::RequestChanges => request_changes += weight,
            AdvisoryClass::Abstain => {}
        }
    }

    let threshold = total_weight / 2.0;
    if accept > threshold {
        return (true, Some(TaskResultReviewDecision::Accept));
    }
    if request_changes > threshold {
        return (true, Some(TaskResultReviewDecision::RequestChanges));
    }
    (false, None)
}

fn advisory_class(decision: TaskResultReviewDecision) -> AdvisoryClass {
    match decision {
        TaskResultReviewDecision::Accept => AdvisoryClass::Accept,
        TaskResultReviewDecision::RequestChanges
        | TaskResultReviewDecision::Reject
        | TaskResultReviewDecision::Cancel => AdvisoryClass::RequestChanges,
        TaskResultReviewDecision::Abstain => AdvisoryClass::Abstain,
    }
}

fn stable_short_id(prefix: &str, parts: &[&str]) -> String {
    let suffix = stable_hash(parts, 18);
    format!("{prefix}{suffix}")
}

fn stable_hash(parts: &[&str], len: usize) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    let digest = hex::encode(hasher.finalize());
    digest[..len.min(digest.len())].to_owned()
}

fn review_actor_label(kind: TaskResultReviewerKind) -> &'static str {
    match kind {
        TaskResultReviewerKind::RuntimeAuto => "runtime_auto",
        TaskResultReviewerKind::ParentAgent => "parent_agent",
        TaskResultReviewerKind::ReviewAgent => "review_agent",
        TaskResultReviewerKind::User => "user",
        TaskResultReviewerKind::System => "system",
    }
}

fn review_event_kind_label(kind: TaskResultReviewEventKind) -> &'static str {
    match kind {
        TaskResultReviewEventKind::Advisory => "advisory",
        TaskResultReviewEventKind::Decision => "decision",
        TaskResultReviewEventKind::Override => "override",
        TaskResultReviewEventKind::SystemAuto => "system_auto",
    }
}

fn review_decision_label(decision: TaskResultReviewDecision) -> &'static str {
    match decision {
        TaskResultReviewDecision::Accept => "accept",
        TaskResultReviewDecision::RequestChanges => "request_changes",
        TaskResultReviewDecision::Reject => "reject",
        TaskResultReviewDecision::Abstain => "abstain",
        TaskResultReviewDecision::Cancel => "cancel",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reviewer(required: bool, weight: Option<f64>) -> TaskResultReviewerSpec {
        TaskResultReviewerSpec {
            reviewer_kind: TaskResultReviewerKind::ReviewAgent,
            agent_nickname: Some(format!("reviewer_{required}_{weight:?}")),
            agent_role: None,
            required,
            weight,
        }
    }

    fn policy(
        strategy: TaskResultReviewResolutionStrategy,
        reviewers: Vec<TaskResultReviewerSpec>,
    ) -> TaskAgentReviewPolicy {
        TaskAgentReviewPolicy {
            mode: TaskAgentReviewMode::ParentAgentWithReviewers,
            max_revision_rounds: 2,
            require_explicit_acceptance: true,
            reviewers,
            resolution_strategy: strategy,
        }
    }

    fn advisory(
        policy: &TaskAgentReviewPolicy,
        index: usize,
        decision: TaskResultReviewDecision,
    ) -> TaskResultReviewEvent {
        TaskResultReviewEvent {
            id: format!("event_{index}_{decision:?}"),
            candidate_id: "candidate".to_owned(),
            task_id: "task".to_owned(),
            run_id: "run".to_owned(),
            task_run_turn_id: "turn".to_owned(),
            reviewer_kind: TaskResultReviewerKind::ReviewAgent,
            reviewer: TaskResultReviewerRef::AgentExecution(
                pioneer_protocol::AgentExecutionId::new("R".repeat(21))
                    .expect("reviewer execution id"),
            ),
            reviewer_thread_id: Some(format!("thread_{index}")),
            reviewer_turn_id: Some(format!("turn_{index}")),
            reviewer_user_id: None,
            reviewer_agent_spec_id: Some(task_result_reviewer_spec_key(
                index,
                &policy.reviewers[index],
            )),
            event_kind: TaskResultReviewEventKind::Advisory,
            decision,
            feedback_text: None,
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: i64::try_from(index).unwrap_or(0),
        }
    }

    #[test]
    fn parent_final_allows_parent_without_advisory_reviews() {
        let policy = policy(
            TaskResultReviewResolutionStrategy::ParentFinal,
            vec![reviewer(true, None)],
        );
        let state = evaluate_task_result_review_resolution(Some(&policy), &[]);
        assert!(state.parent_final_allowed);
        assert!(!state.user_final_allowed);
        assert!(!state.advisory_can_resolve);
    }

    #[test]
    fn user_final_allows_user_without_parent_final() {
        let policy = policy(
            TaskResultReviewResolutionStrategy::UserFinal,
            vec![reviewer(true, None)],
        );
        let state = evaluate_task_result_review_resolution(Some(&policy), &[]);
        assert!(state.user_final_allowed);
        assert!(!state.parent_final_allowed);
    }

    #[test]
    fn require_all_required_reviewers_blocks_until_required_advisory_exists() {
        let policy = policy(
            TaskResultReviewResolutionStrategy::RequireAllRequiredReviewersThenParent,
            vec![reviewer(true, None), reviewer(false, None)],
        );
        let blocked = evaluate_task_result_review_resolution(Some(&policy), &[]);
        assert!(!blocked.parent_final_allowed);
        assert!(!blocked.required_reviewers_satisfied);

        let allowed = evaluate_task_result_review_resolution(
            Some(&policy),
            &[advisory(&policy, 0, TaskResultReviewDecision::Accept)],
        );
        assert!(allowed.parent_final_allowed);
        assert!(allowed.required_reviewers_satisfied);
    }

    #[test]
    fn quorum_then_parent_handles_conflicting_advisory_events() {
        let policy = policy(
            TaskResultReviewResolutionStrategy::QuorumThenParent,
            vec![
                reviewer(false, Some(2.0)),
                reviewer(false, Some(1.0)),
                reviewer(false, Some(1.0)),
            ],
        );
        let split = evaluate_task_result_review_resolution(
            Some(&policy),
            &[
                advisory(&policy, 0, TaskResultReviewDecision::Accept),
                advisory(&policy, 1, TaskResultReviewDecision::RequestChanges),
            ],
        );
        assert!(!split.parent_final_allowed);
        assert_eq!(split.recommended_advisory_decision, None);

        let quorum = evaluate_task_result_review_resolution(
            Some(&policy),
            &[
                advisory(&policy, 0, TaskResultReviewDecision::Accept),
                advisory(&policy, 1, TaskResultReviewDecision::Accept),
            ],
        );
        assert!(quorum.parent_final_allowed);
        assert_eq!(
            quorum.recommended_advisory_decision,
            Some(TaskResultReviewDecision::Accept)
        );
    }

    #[test]
    fn required_reviewer_can_request_changes_by_advisory() {
        let policy = policy(
            TaskResultReviewResolutionStrategy::AnyRequiredReviewerCanRequestChanges,
            vec![reviewer(true, None)],
        );
        let state = evaluate_task_result_review_resolution(
            Some(&policy),
            &[advisory(
                &policy,
                0,
                TaskResultReviewDecision::RequestChanges,
            )],
        );
        assert!(state.advisory_can_resolve);
        assert!(state.parent_final_allowed);
        assert_eq!(
            state.recommended_advisory_decision,
            Some(TaskResultReviewDecision::RequestChanges)
        );
    }
}
