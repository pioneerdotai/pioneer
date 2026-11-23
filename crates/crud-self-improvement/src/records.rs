use pioneer_protocol::SkillId;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfImprovementWorkspaceStateRecord {
    pub workspace_id: String,
    pub activation_epoch: i64,
    pub cursor_source_id: i64,
    pub effective_enabled_at_unix: Option<i64>,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSelfImprovementRun {
    pub workspace_id: String,
    pub activation_epoch: i64,
    pub scheduled_date_utc: String,
    pub source_lower_exclusive: i64,
    pub source_upper_inclusive: i64,
    pub learner_provider: String,
    pub learner_model: String,
    pub learner_reasoning_effort: Option<String>,
    pub reviewer_provider: String,
    pub reviewer_model: String,
    pub reviewer_reasoning_effort: Option<String>,
    pub pipeline_contract_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfImprovementRunRecord {
    pub id: String,
    pub workspace_id: String,
    pub activation_epoch: i64,
    pub scheduled_date_utc: String,
    pub source_lower_exclusive: i64,
    pub source_upper_inclusive: i64,
    pub status: String,
    pub claim_token: Option<String>,
    pub claimed_by: Option<String>,
    pub lease_expires_at_unix: Option<i64>,
    pub attempt_count: i64,
    pub next_attempt_at_unix: Option<i64>,
    pub learner_provider: String,
    pub learner_model: String,
    pub learner_reasoning_effort: Option<String>,
    pub reviewer_provider: String,
    pub reviewer_model: String,
    pub reviewer_reasoning_effort: Option<String>,
    pub pipeline_contract_version: String,
    pub analysis_cursor_json: Option<String>,
    pub analysis_digest_json: Option<String>,
    pub outcome: Option<String>,
    pub applied_action: Option<String>,
    pub skill_id: Option<String>,
    pub previous_version_id: Option<String>,
    pub resulting_version_id: Option<String>,
    pub result_summary: Option<String>,
    pub last_error: Option<String>,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfImprovementRunFence {
    pub run_id: String,
    pub workspace_id: String,
    pub activation_epoch: i64,
    pub source_lower_exclusive: i64,
    pub source_upper_inclusive: i64,
    pub claim_token: String,
    pub claimed_by: String,
    pub learner_provider: String,
    pub learner_model: String,
    pub learner_reasoning_effort: Option<String>,
    pub reviewer_provider: String,
    pub reviewer_model: String,
    pub reviewer_reasoning_effort: Option<String>,
    pub pipeline_contract_version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfImprovementRunMutationResult {
    Applied,
    LostAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfImprovementFinalizationAuthority {
    pub effective_enabled: bool,
    pub learner_provider: String,
    pub learner_model: String,
    pub learner_reasoning_effort: Option<String>,
    pub reviewer_provider: String,
    pub reviewer_model: String,
    pub reviewer_reasoning_effort: Option<String>,
    pub pipeline_contract_version: String,
}
impl SelfImprovementRunRecord {
    pub fn fence(&self) -> Option<SelfImprovementRunFence> {
        Some(SelfImprovementRunFence {
            run_id: self.id.clone(),
            workspace_id: self.workspace_id.clone(),
            activation_epoch: self.activation_epoch,
            source_lower_exclusive: self.source_lower_exclusive,
            source_upper_inclusive: self.source_upper_inclusive,
            claim_token: self.claim_token.clone()?,
            claimed_by: self.claimed_by.clone()?,
            learner_provider: self.learner_provider.clone(),
            learner_model: self.learner_model.clone(),
            learner_reasoning_effort: self.learner_reasoning_effort.clone(),
            reviewer_provider: self.reviewer_provider.clone(),
            reviewer_model: self.reviewer_model.clone(),
            reviewer_reasoning_effort: self.reviewer_reasoning_effort.clone(),
            pipeline_contract_version: self.pipeline_contract_version.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSkillEvidenceTime {
    /// Oldest of the newest supporting message dates, one per selected observation.
    pub confirmed_at_unix: i64,
    /// Newest actual supporting message, never the time of learning/version creation.
    pub latest_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedAgentSkillCreate {
    pub evidence_time: Option<AgentSkillEvidenceTime>,
    pub skill_id: SkillId,
    pub version_id: String,
    pub slug: String,
    pub candidate_key: String,
    pub display_name: String,
    pub skill_markdown: String,
    pub instruction_body: String,
    pub when_to_use: String,
    pub when_not_to_use: String,
    pub fingerprint: String,
    pub source_turn_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedAgentSkillUpdate {
    pub evidence_time: Option<AgentSkillEvidenceTime>,
    pub skill_id: SkillId,
    pub expected_active_version_id: String,
    pub version_id: String,
    pub version_number: i64,
    pub slug: String,
    pub candidate_key: String,
    pub display_name: String,
    pub skill_markdown: String,
    pub instruction_body: String,
    pub when_to_use: String,
    pub when_not_to_use: String,
    pub fingerprint: String,
    pub source_turn_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedAgentSkillRollback {
    pub evidence_time: Option<AgentSkillEvidenceTime>,
    pub skill_id: SkillId,
    pub expected_active_version_id: String,
    pub target_parent_version_id: String,
    pub candidate_key: String,
    pub source_turn_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfImprovementNoChangeReason {
    NoCandidate,
    ReviewerRejected,
    HostValidationRejected,
    ModelContractRejected,
}

impl SelfImprovementNoChangeReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoCandidate => "no_candidate",
            Self::ReviewerRejected => "reviewer_rejected",
            Self::HostValidationRejected => "host_validation_rejected",
            Self::ModelContractRejected => "model_contract_rejected",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelfImprovementFinalOutcome {
    AcceptedCreate(AcceptedAgentSkillCreate),
    AcceptedUpdate(AcceptedAgentSkillUpdate),
    AcceptedRollback(AcceptedAgentSkillRollback),
    NoChange {
        reason: SelfImprovementNoChangeReason,
        reason_codes: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeSelfImprovementRunInput {
    pub fence: SelfImprovementRunFence,
    pub authority: SelfImprovementFinalizationAuthority,
    pub outcome: SelfImprovementFinalOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfImprovementFinalizationConflict {
    SkillIdentity,
    VersionIdentity,
    Slug,
    Fingerprint,
    Candidate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalizeSelfImprovementRunResult {
    Applied {
        skill_id: SkillId,
        version_id: String,
    },
    NoChange {
        reason: SelfImprovementNoChangeReason,
    },
    AlreadyFinalized,
    Stale,
    Conflict(SelfImprovementFinalizationConflict),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfImprovementSourceTurnRecord {
    pub id: i64,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub parent_turn_created_at_unix: i64,
    pub task_delivery_id: Option<String>,
    pub terminal_event_id: String,
    pub terminal_at_unix: i64,
    pub created_at_unix: i64,
}
