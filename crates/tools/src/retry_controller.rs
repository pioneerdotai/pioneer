use crate::context::{ToolErrorClass, ToolOutcome, ToolOutcomeStatus};
use crate::output_policy::ToolRecoveryView;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolRetryBudgetConfig {
    pub max_recoverable_retry_rounds_per_episode: u32,
    pub max_same_tool_error_retries_per_episode: u32,
    pub max_retries_per_tool_name_per_episode: u32,
}

impl Default for ToolRetryBudgetConfig {
    fn default() -> Self {
        Self {
            max_recoverable_retry_rounds_per_episode: 32,
            max_same_tool_error_retries_per_episode: 3,
            max_retries_per_tool_name_per_episode: 16,
        }
    }
}

impl ToolRetryBudgetConfig {
    pub fn normalized(&self) -> Self {
        Self {
            max_recoverable_retry_rounds_per_episode: self
                .max_recoverable_retry_rounds_per_episode
                .max(1),
            max_same_tool_error_retries_per_episode: self
                .max_same_tool_error_retries_per_episode
                .max(1),
            max_retries_per_tool_name_per_episode: self
                .max_retries_per_tool_name_per_episode
                .max(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolRetryClassBudget {
    pub error_class: ToolErrorClass,
    pub max_retries: u32,
}

pub fn default_tool_retry_class_budgets() -> Vec<ToolRetryClassBudget> {
    vec![
        ToolRetryClassBudget {
            error_class: ToolErrorClass::InvalidArguments,
            max_retries: 3,
        },
        ToolRetryClassBudget {
            error_class: ToolErrorClass::NotFound,
            max_retries: 2,
        },
        ToolRetryClassBudget {
            error_class: ToolErrorClass::ToolNotVisible,
            max_retries: 1,
        },
        ToolRetryClassBudget {
            error_class: ToolErrorClass::PermissionDenied,
            max_retries: 1,
        },
        ToolRetryClassBudget {
            error_class: ToolErrorClass::CommandNotFound,
            max_retries: 2,
        },
        ToolRetryClassBudget {
            error_class: ToolErrorClass::Timeout,
            max_retries: 2,
        },
        ToolRetryClassBudget {
            error_class: ToolErrorClass::Cancelled,
            max_retries: 1,
        },
        ToolRetryClassBudget {
            error_class: ToolErrorClass::ExecutionFailed,
            max_retries: 2,
        },
        ToolRetryClassBudget {
            error_class: ToolErrorClass::NeedsNarrowing,
            max_retries: 0,
        },
        ToolRetryClassBudget {
            error_class: ToolErrorClass::Internal,
            max_retries: 0,
        },
        ToolRetryClassBudget {
            error_class: ToolErrorClass::OutputTruncated,
            max_retries: 3,
        },
        ToolRetryClassBudget {
            error_class: ToolErrorClass::Unknown,
            max_retries: 1,
        },
    ]
}

#[derive(Debug, Clone)]
pub struct ToolRetryObservation {
    pub item_id: String,
    pub item_type: String,
    pub attempt_number: u32,
    pub tool_name: String,
    pub arguments: String,
    pub success: bool,
    pub outcome: ToolOutcome,
    pub recovery_view: Option<ToolRecoveryView>,
}

impl ToolRetryObservation {
    pub fn from_tool_outcome(
        item_id: impl Into<String>,
        item_type: impl Into<String>,
        attempt_number: u32,
        tool_name: impl Into<String>,
        arguments: impl Into<String>,
        success: bool,
        outcome: ToolOutcome,
    ) -> Self {
        Self {
            item_id: item_id.into(),
            item_type: item_type.into(),
            attempt_number,
            tool_name: tool_name.into(),
            arguments: arguments.into(),
            success,
            outcome,
            recovery_view: None,
        }
    }

    pub fn with_recovery_view(mut self, recovery_view: Option<ToolRecoveryView>) -> Self {
        self.recovery_view = recovery_view;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ToolFailureSignature {
    pub tool_name: String,
    pub arguments_fingerprint: String,
    pub error_class: ToolErrorClass,
}

impl ToolFailureSignature {
    pub fn from_arguments(
        tool_name: impl Into<String>,
        arguments: &str,
        error_class: ToolErrorClass,
    ) -> Self {
        Self {
            tool_name: tool_name.into(),
            arguments_fingerprint: normalized_arguments_fingerprint(arguments),
            error_class,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolRetryPromptEntry {
    pub item_id: String,
    pub item_type: String,
    pub attempt_number: u32,
    pub tool_name: String,
    pub error_class: ToolErrorClass,
    pub retry_hint: String,
    pub failure_signature_fingerprint: String,
    pub episode_retry_used: u32,
    pub episode_retry_limit: u32,
    pub class_retry_used: u32,
    pub class_retry_limit: u32,
    pub tool_retry_used: u32,
    pub tool_retry_limit: u32,
    pub signature_retry_used: u32,
    pub signature_retry_limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRetryPrompt {
    Retry {
        entries: Vec<ToolRetryPromptEntry>,
    },
    Exhausted {
        entries: Vec<ToolRetryPromptEntry>,
        reason: ToolRetryExhaustionReason,
    },
}

impl ToolRetryPrompt {
    pub fn fact_lines(&self) -> Vec<String> {
        match self {
            Self::Retry { entries } => entries
                .iter()
                .take(4)
                .map(ToolRetryPromptEntry::fact_line)
                .collect(),
            Self::Exhausted { entries, reason } => {
                let mut lines = Vec::with_capacity(entries.len().min(4).saturating_add(1));
                lines.push(format!("exhaustion_reason={}", reason.fact_value()));
                lines.extend(entries.iter().take(4).map(ToolRetryPromptEntry::fact_line));
                lines
            }
        }
    }
}

impl ToolRetryPromptEntry {
    pub fn fact_line(&self) -> String {
        format!(
            "{}: class={:?} episode_retry_budget={}/{} class_retry_budget={}/{} tool_retry_budget={}/{} signature_retry_budget={}/{} hint={}",
            self.tool_name,
            self.error_class,
            self.episode_retry_used,
            self.episode_retry_limit,
            self.class_retry_used,
            self.class_retry_limit,
            self.tool_retry_used,
            self.tool_retry_limit,
            self.signature_retry_used,
            self.signature_retry_limit,
            self.retry_hint
        )
    }

    pub fn budget_usages(&self) -> Vec<ToolRetryBudgetUsage> {
        vec![
            ToolRetryBudgetUsage {
                kind: ToolRetryBudgetKind::Episode,
                used: self.episode_retry_used,
                limit: self.episode_retry_limit,
            },
            ToolRetryBudgetUsage {
                kind: ToolRetryBudgetKind::ErrorClass,
                used: self.class_retry_used,
                limit: self.class_retry_limit,
            },
            ToolRetryBudgetUsage {
                kind: ToolRetryBudgetKind::ToolName,
                used: self.tool_retry_used,
                limit: self.tool_retry_limit,
            },
            ToolRetryBudgetUsage {
                kind: ToolRetryBudgetKind::FailureSignature,
                used: self.signature_retry_used,
                limit: self.signature_retry_limit,
            },
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ToolRetryBudgetKind {
    Episode,
    ErrorClass,
    ToolName,
    FailureSignature,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolRetryBudgetUsage {
    pub kind: ToolRetryBudgetKind,
    pub used: u32,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRetryExhaustionReason {
    TotalRetryRounds {
        used: u32,
        limit: u32,
    },
    ErrorClass {
        error_class: ToolErrorClass,
        used: u32,
        limit: u32,
    },
    ToolName {
        tool_name: String,
        used: u32,
        limit: u32,
    },
    FailureSignature {
        signature: ToolFailureSignature,
        used: u32,
        limit: u32,
    },
}

impl ToolRetryExhaustionReason {
    pub fn kind(&self) -> ToolRetryBudgetKind {
        match self {
            Self::TotalRetryRounds { .. } => ToolRetryBudgetKind::Episode,
            Self::ErrorClass { .. } => ToolRetryBudgetKind::ErrorClass,
            Self::ToolName { .. } => ToolRetryBudgetKind::ToolName,
            Self::FailureSignature { .. } => ToolRetryBudgetKind::FailureSignature,
        }
    }

    pub fn fact_value(&self) -> String {
        match self {
            Self::TotalRetryRounds { used, limit } => {
                format!("total_retry_rounds {used}/{limit}")
            }
            Self::ErrorClass {
                error_class,
                used,
                limit,
            } => {
                format!("class={error_class:?} {used}/{limit}")
            }
            Self::ToolName {
                tool_name,
                used,
                limit,
            } => {
                format!("tool={tool_name} {used}/{limit}")
            }
            Self::FailureSignature {
                signature,
                used,
                limit,
            } => {
                format!("signature={} {used}/{limit}", signature.fact_value())
            }
        }
    }
}

impl ToolFailureSignature {
    pub fn fact_value(&self) -> String {
        format!(
            "{}:{}:{:?}",
            self.tool_name, self.arguments_fingerprint, self.error_class
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolRetryResolution {
    Succeeded,
    NonRetryable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolRetryResolvedEntry {
    pub item_id: String,
    pub item_type: String,
    pub attempt_number: u32,
    pub tool_name: String,
    pub resolution: ToolRetryResolution,
    pub budgets: Vec<ToolRetryBudgetUsage>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRetryEventDraft {
    Scheduled {
        entries: Vec<ToolRetryPromptEntry>,
    },
    Resolved {
        episode: ToolRetryEpisodeState,
        entries: Vec<ToolRetryResolvedEntry>,
    },
    Exhausted {
        entries: Vec<ToolRetryPromptEntry>,
        reason: ToolRetryExhaustionReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRetryDecision {
    None {
        drafts: Vec<ToolRetryEventDraft>,
    },
    Retry {
        prompt: ToolRetryPrompt,
        drafts: Vec<ToolRetryEventDraft>,
    },
    Exhausted {
        prompt: ToolRetryPrompt,
        reason: ToolRetryExhaustionReason,
        drafts: Vec<ToolRetryEventDraft>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolRetryBudgetSnapshot {
    pub active_episode: Option<ToolRetryEpisodeState>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolRetryEpisodeState {
    pub total_retry_rounds: u32,
    pub by_class: HashMap<ToolErrorClass, u32>,
    pub by_tool_name: HashMap<String, u32>,
    pub by_failure_signature: HashMap<ToolFailureSignature, u32>,
}

#[derive(Debug)]
pub struct ToolRetryController {
    budget: ToolRetryBudgetConfig,
    class_limits: HashMap<ToolErrorClass, u32>,
    active_episode: Option<ToolRetryEpisodeState>,
}

impl ToolRetryController {
    pub fn new(budget: ToolRetryBudgetConfig) -> Self {
        Self::with_class_budgets(budget, default_tool_retry_class_budgets())
    }

    pub fn with_class_budgets(
        budget: ToolRetryBudgetConfig,
        class_budgets: impl IntoIterator<Item = ToolRetryClassBudget>,
    ) -> Self {
        let mut class_limits = HashMap::new();
        for class_budget in class_budgets {
            class_limits.insert(class_budget.error_class, class_budget.max_retries);
        }

        Self {
            budget: budget.normalized(),
            class_limits,
            active_episode: None,
        }
    }

    pub fn state_snapshot(&self) -> ToolRetryBudgetSnapshot {
        ToolRetryBudgetSnapshot {
            active_episode: self.active_episode.clone(),
        }
    }

    pub fn decide(&mut self, observations: &[ToolRetryObservation]) -> ToolRetryDecision {
        let mut candidates = observations
            .iter()
            .filter_map(RetryCandidate::from_observation)
            .collect::<Vec<_>>();

        if candidates.is_empty() {
            let drafts = self
                .active_episode
                .take()
                .map(|episode| {
                    let entries =
                        self.resolved_entries(observations, &episode, "retry_episode_resolved");
                    ToolRetryEventDraft::Resolved { episode, entries }
                })
                .into_iter()
                .collect::<Vec<_>>();
            return ToolRetryDecision::None { drafts };
        }

        candidates.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
        if self.active_episode.is_none() {
            self.active_episode = Some(ToolRetryEpisodeState::default());
        }

        let episode = self
            .active_episode
            .as_ref()
            .expect("retry episode must exist before evaluating candidates");

        let entries = candidates
            .iter()
            .map(|candidate| self.prompt_entry(candidate, &episode))
            .collect::<Vec<_>>();

        if episode.total_retry_rounds >= self.budget.max_recoverable_retry_rounds_per_episode {
            let reason = ToolRetryExhaustionReason::TotalRetryRounds {
                used: episode.total_retry_rounds,
                limit: self.budget.max_recoverable_retry_rounds_per_episode,
            };
            return self.exhausted_and_close(entries, reason);
        }

        let mut projected_by_class = episode.by_class.clone();
        let mut projected_by_tool_name = episode.by_tool_name.clone();
        let mut projected_by_failure_signature = episode.by_failure_signature.clone();

        for candidate in &candidates {
            let class_limit = self.class_limit(candidate.error_class);
            let class_next = projected_by_class
                .get(&candidate.error_class)
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
            if class_next > class_limit {
                let reason = ToolRetryExhaustionReason::ErrorClass {
                    error_class: candidate.error_class,
                    used: class_next.saturating_sub(1),
                    limit: class_limit,
                };
                return self.exhausted_and_close(entries, reason);
            }
            projected_by_class.insert(candidate.error_class, class_next);

            let tool_next = projected_by_tool_name
                .get(candidate.tool_name.as_str())
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
            if tool_next > self.budget.max_retries_per_tool_name_per_episode {
                let reason = ToolRetryExhaustionReason::ToolName {
                    tool_name: candidate.tool_name.clone(),
                    used: tool_next.saturating_sub(1),
                    limit: self.budget.max_retries_per_tool_name_per_episode,
                };
                return self.exhausted_and_close(entries, reason);
            }
            projected_by_tool_name.insert(candidate.tool_name.clone(), tool_next);

            let signature_next = projected_by_failure_signature
                .get(&candidate.signature)
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
            if signature_next > self.budget.max_same_tool_error_retries_per_episode {
                let reason = ToolRetryExhaustionReason::FailureSignature {
                    signature: candidate.signature.clone(),
                    used: signature_next.saturating_sub(1),
                    limit: self.budget.max_same_tool_error_retries_per_episode,
                };
                return self.exhausted_and_close(entries, reason);
            }
            projected_by_failure_signature.insert(candidate.signature.clone(), signature_next);
        }

        let episode = {
            let episode = self
                .active_episode
                .as_mut()
                .expect("retry episode must exist before consuming budget");
            episode.total_retry_rounds = episode.total_retry_rounds.saturating_add(1);
            for candidate in &candidates {
                increment_class(&mut episode.by_class, candidate.error_class);
                increment_string(&mut episode.by_tool_name, candidate.tool_name.as_str());
                increment_signature(&mut episode.by_failure_signature, &candidate.signature);
            }
            episode.clone()
        };

        let entries = candidates
            .iter()
            .map(|candidate| self.prompt_entry(candidate, &episode))
            .collect::<Vec<_>>();
        let prompt = ToolRetryPrompt::Retry {
            entries: entries.clone(),
        };
        ToolRetryDecision::Retry {
            prompt,
            drafts: vec![ToolRetryEventDraft::Scheduled { entries }],
        }
    }

    fn exhausted_and_close(
        &mut self,
        entries: Vec<ToolRetryPromptEntry>,
        reason: ToolRetryExhaustionReason,
    ) -> ToolRetryDecision {
        let prompt = ToolRetryPrompt::Exhausted {
            entries: entries.clone(),
            reason: reason.clone(),
        };
        let decision = ToolRetryDecision::Exhausted {
            prompt,
            reason: reason.clone(),
            drafts: vec![ToolRetryEventDraft::Exhausted { entries, reason }],
        };
        self.active_episode = None;
        decision
    }

    fn prompt_entry(
        &self,
        candidate: &RetryCandidate,
        episode: &ToolRetryEpisodeState,
    ) -> ToolRetryPromptEntry {
        ToolRetryPromptEntry {
            item_id: candidate.item_id.clone(),
            item_type: candidate.item_type.clone(),
            attempt_number: candidate.attempt_number,
            tool_name: candidate.tool_name.clone(),
            error_class: candidate.error_class,
            retry_hint: candidate.retry_hint.clone(),
            failure_signature_fingerprint: candidate.signature.arguments_fingerprint.clone(),
            episode_retry_used: episode.total_retry_rounds,
            episode_retry_limit: self.budget.max_recoverable_retry_rounds_per_episode,
            class_retry_used: self.class_used(episode, candidate.error_class),
            class_retry_limit: self.class_limit(candidate.error_class),
            tool_retry_used: self.tool_used(episode, candidate.tool_name.as_str()),
            tool_retry_limit: self.budget.max_retries_per_tool_name_per_episode,
            signature_retry_used: self.signature_used(episode, &candidate.signature),
            signature_retry_limit: self.budget.max_same_tool_error_retries_per_episode,
        }
    }

    fn resolved_entries(
        &self,
        observations: &[ToolRetryObservation],
        episode: &ToolRetryEpisodeState,
        reason: &str,
    ) -> Vec<ToolRetryResolvedEntry> {
        let resolution = if observations.iter().any(|observation| observation.success) {
            ToolRetryResolution::Succeeded
        } else {
            ToolRetryResolution::NonRetryable
        };

        let mut entries = observations
            .iter()
            .filter(|observation| match resolution {
                ToolRetryResolution::Succeeded => observation.success,
                ToolRetryResolution::NonRetryable => !observation.success,
            })
            .map(|observation| {
                let tool_used = self.tool_used(episode, observation.tool_name.as_str());
                ToolRetryResolvedEntry {
                    item_id: observation.item_id.clone(),
                    item_type: observation.item_type.clone(),
                    attempt_number: observation.attempt_number,
                    tool_name: observation.tool_name.clone(),
                    resolution,
                    budgets: vec![
                        ToolRetryBudgetUsage {
                            kind: ToolRetryBudgetKind::Episode,
                            used: episode.total_retry_rounds,
                            limit: self.budget.max_recoverable_retry_rounds_per_episode,
                        },
                        ToolRetryBudgetUsage {
                            kind: ToolRetryBudgetKind::ToolName,
                            used: tool_used,
                            limit: self.budget.max_retries_per_tool_name_per_episode,
                        },
                    ],
                    reason: reason.to_owned(),
                }
            })
            .collect::<Vec<_>>();

        entries.sort_by(|left, right| {
            (
                left.tool_name.as_str(),
                left.item_id.as_str(),
                left.attempt_number,
            )
                .cmp(&(
                    right.tool_name.as_str(),
                    right.item_id.as_str(),
                    right.attempt_number,
                ))
        });
        entries
    }

    fn class_limit(&self, error_class: ToolErrorClass) -> u32 {
        self.class_limits.get(&error_class).copied().unwrap_or(1)
    }

    fn class_used(&self, episode: &ToolRetryEpisodeState, error_class: ToolErrorClass) -> u32 {
        episode.by_class.get(&error_class).copied().unwrap_or(0)
    }

    fn tool_used(&self, episode: &ToolRetryEpisodeState, tool_name: &str) -> u32 {
        episode.by_tool_name.get(tool_name).copied().unwrap_or(0)
    }

    fn signature_used(
        &self,
        episode: &ToolRetryEpisodeState,
        signature: &ToolFailureSignature,
    ) -> u32 {
        episode
            .by_failure_signature
            .get(signature)
            .copied()
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
struct RetryCandidate {
    item_id: String,
    item_type: String,
    attempt_number: u32,
    tool_name: String,
    error_class: ToolErrorClass,
    retry_hint: String,
    signature: ToolFailureSignature,
}

impl RetryCandidate {
    fn from_observation(observation: &ToolRetryObservation) -> Option<Self> {
        if observation.success
            || !observation.outcome.should_retry
            || !matches!(
                observation.outcome.status,
                ToolOutcomeStatus::RecoverableError | ToolOutcomeStatus::PartialSuccess
            )
        {
            return None;
        }

        let error_class = observation
            .outcome
            .error_class
            .unwrap_or(ToolErrorClass::Unknown);
        let signature = ToolFailureSignature::from_arguments(
            observation.tool_name.clone(),
            observation.arguments.as_str(),
            error_class,
        );
        let retry_hint = observation
            .recovery_view
            .as_ref()
            .and_then(|recovery| recovery.retry_hint.clone())
            .or_else(|| observation.outcome.retry_hint.clone())
            .unwrap_or_else(|| "retry with corrected arguments".to_owned());

        Some(Self {
            item_id: observation.item_id.clone(),
            item_type: observation.item_type.clone(),
            attempt_number: observation.attempt_number,
            tool_name: observation.tool_name.clone(),
            error_class,
            retry_hint,
            signature,
        })
    }

    fn sort_key(&self) -> (String, String, &'static str, String, u32) {
        (
            self.tool_name.clone(),
            self.signature.arguments_fingerprint.clone(),
            error_class_code(self.error_class),
            self.item_id.clone(),
            self.attempt_number,
        )
    }
}

fn increment_class(map: &mut HashMap<ToolErrorClass, u32>, key: ToolErrorClass) {
    let next = map.get(&key).copied().unwrap_or(0).saturating_add(1);
    map.insert(key, next);
}

fn increment_string(map: &mut HashMap<String, u32>, key: &str) {
    let next = map.get(key).copied().unwrap_or(0).saturating_add(1);
    map.insert(key.to_owned(), next);
}

fn increment_signature(map: &mut HashMap<ToolFailureSignature, u32>, key: &ToolFailureSignature) {
    let next = map.get(key).copied().unwrap_or(0).saturating_add(1);
    map.insert(key.clone(), next);
}

fn normalized_arguments_fingerprint(arguments: &str) -> String {
    match serde_json::from_str::<JsonValue>(arguments) {
        Ok(value) => canonical_json_fingerprint(&value),
        Err(_) => arguments.trim().to_owned(),
    }
}

fn canonical_json_fingerprint(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_owned(),
        JsonValue::Bool(value) => value.to_string(),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::String(value) => {
            serde_json::to_string(value).unwrap_or_else(|_| format!("{value:?}"))
        }
        JsonValue::Array(items) => {
            let rendered = items
                .iter()
                .map(canonical_json_fingerprint)
                .collect::<Vec<_>>();
            format!("[{}]", rendered.join(","))
        }
        JsonValue::Object(object) => {
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by(|(left_key, _), (right_key, _)| left_key.cmp(right_key));
            let rendered = entries
                .into_iter()
                .map(|(key, value)| {
                    let key = serde_json::to_string(key).unwrap_or_else(|_| format!("{key:?}"));
                    format!("{key}:{}", canonical_json_fingerprint(value))
                })
                .collect::<Vec<_>>();
            format!("{{{}}}", rendered.join(","))
        }
    }
}

fn error_class_code(error_class: ToolErrorClass) -> &'static str {
    match error_class {
        ToolErrorClass::InvalidArguments => "invalid_arguments",
        ToolErrorClass::NotFound => "not_found",
        ToolErrorClass::ToolNotVisible => "tool_not_visible",
        ToolErrorClass::PermissionDenied => "permission_denied",
        ToolErrorClass::CommandNotFound => "command_not_found",
        ToolErrorClass::Timeout => "timeout",
        ToolErrorClass::Cancelled => "cancelled",
        ToolErrorClass::ExecutionFailed => "execution_failed",
        ToolErrorClass::NeedsNarrowing => "needs_narrowing",
        ToolErrorClass::Internal => "internal",
        ToolErrorClass::OutputTruncated => "output_truncated",
        ToolErrorClass::Unknown => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(tool_name: &str, arguments: &str, outcome: ToolOutcome) -> ToolRetryObservation {
        ToolRetryObservation::from_tool_outcome(
            format!("item_{tool_name}"),
            "command_execution",
            1,
            tool_name,
            arguments,
            false,
            outcome,
        )
    }

    fn recoverable(class: ToolErrorClass) -> ToolOutcome {
        ToolOutcome::recoverable(class, "fix and retry", false, None)
    }

    fn retry_decision(controller: &mut ToolRetryController, class: ToolErrorClass) {
        let decision = controller.decide(&[observation("exec_command", "{}", recoverable(class))]);
        assert!(matches!(decision, ToolRetryDecision::Retry { .. }));
    }

    fn active_episode(controller: &ToolRetryController) -> ToolRetryEpisodeState {
        controller
            .state_snapshot()
            .active_episode
            .expect("retry episode should be active")
    }

    #[test]
    fn no_retry_for_success() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig::default());
        let decision = controller.decide(&[ToolRetryObservation::from_tool_outcome(
            "item_read_file",
            "command_execution",
            1,
            "read_file",
            "{}",
            true,
            ToolOutcome::ok(),
        )]);

        assert!(matches!(decision, ToolRetryDecision::None { ref drafts } if drafts.is_empty()));
        assert!(controller.state_snapshot().active_episode.is_none());
    }

    #[test]
    fn retry_for_recoverable_result_consumes_budget_once() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig::default());
        let decision = controller.decide(&[observation(
            "exec_command",
            r#"{"command":["false"]}"#,
            recoverable(ToolErrorClass::ExecutionFailed),
        )]);

        let ToolRetryDecision::Retry { prompt, drafts } = decision else {
            panic!("expected retry decision");
        };
        assert_eq!(drafts.len(), 1);
        let fact_lines = prompt.fact_lines();
        assert_eq!(fact_lines.len(), 1);
        assert!(fact_lines[0].contains("exec_command"));
        assert!(fact_lines[0].contains("class=ExecutionFailed"));
        assert!(fact_lines[0].contains("episode_retry_budget=1/32"));
        assert!(fact_lines[0].contains("fix and retry"));

        let ToolRetryPrompt::Retry { entries } = prompt else {
            panic!("expected retry prompt");
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].item_id, "item_exec_command");
        assert_eq!(entries[0].item_type, "command_execution");
        assert_eq!(entries[0].attempt_number, 1);
        assert_eq!(entries[0].tool_name, "exec_command");
        assert_eq!(entries[0].error_class, ToolErrorClass::ExecutionFailed);
        assert_eq!(
            entries[0].failure_signature_fingerprint,
            r#"{"command":["false"]}"#
        );
        assert_eq!(entries[0].episode_retry_used, 1);
        assert_eq!(entries[0].class_retry_used, 1);
        assert_eq!(entries[0].tool_retry_used, 1);
        assert_eq!(entries[0].signature_retry_used, 1);
        assert_eq!(entries[0].retry_hint, "fix and retry");

        let snapshot = active_episode(&controller);
        assert_eq!(snapshot.total_retry_rounds, 1);
        assert_eq!(
            snapshot
                .by_class
                .get(&ToolErrorClass::ExecutionFailed)
                .copied(),
            Some(1)
        );
        assert_eq!(snapshot.by_tool_name.get("exec_command").copied(), Some(1));
        assert_eq!(snapshot.by_failure_signature.len(), 1);
    }

    #[test]
    fn needs_narrowing_is_not_retried_automatically() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig::default());
        let decision = controller.decide(&[observation(
            "grep_files",
            r#"{"pattern":"TaskService"}"#,
            recoverable(ToolErrorClass::NeedsNarrowing),
        )]);

        assert!(matches!(
            decision,
            ToolRetryDecision::Exhausted {
                reason: ToolRetryExhaustionReason::ErrorClass {
                    error_class: ToolErrorClass::NeedsNarrowing,
                    limit: 0,
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn retry_prompt_uses_recovery_view_hint_when_available() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig::default());
        let observation = observation(
            "exec_command",
            r#"{"command":["false"]}"#,
            recoverable(ToolErrorClass::ExecutionFailed),
        )
        .with_recovery_view(Some(ToolRecoveryView {
            error_class: Some("ExecutionFailed".to_owned()),
            retry_hint: Some("recovery view hint".to_owned()),
            incomplete_reason: None,
            diagnostic_summary: Some("from recovery view".to_owned()),
            diagnostic_excerpt: Some("bounded excerpt".to_owned()),
            output_fingerprint: Some("sha256:output".to_owned()),
            content_fingerprint: Some("sha256:content".to_owned()),
            was_truncated: false,
            continuation: None,
        }));

        let decision = controller.decide(&[observation]);
        let ToolRetryDecision::Retry { prompt, .. } = decision else {
            panic!("expected retry decision");
        };
        let ToolRetryPrompt::Retry { entries } = prompt else {
            panic!("expected retry prompt");
        };
        assert_eq!(entries[0].retry_hint, "recovery view hint");
    }

    #[test]
    fn retry_prompt_for_write_file_read_required_keeps_changed_sequence_hint() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig::default());
        let outcome = ToolOutcome::recoverable(
            ToolErrorClass::InvalidArguments,
            "write_file needs current file state before overwrite. Call read_file for the complete file, then retry write_file with updated content.",
            false,
            None,
        );

        let decision = controller.decide(&[observation(
            "write_file",
            r#"{"path":"file.txt","content":"new"}"#,
            outcome,
        )]);

        let ToolRetryDecision::Retry { prompt, .. } = decision else {
            panic!("expected retry decision");
        };
        let ToolRetryPrompt::Retry { entries } = prompt else {
            panic!("expected retry prompt");
        };
        assert_eq!(entries[0].tool_name, "write_file");
        assert_eq!(entries[0].error_class, ToolErrorClass::InvalidArguments);
        assert!(entries[0].retry_hint.contains("read_file"));
        assert!(entries[0].retry_hint.contains("retry write_file"));
        assert_eq!(entries[0].signature_retry_used, 1);
    }

    #[test]
    fn retry_prompt_for_write_file_precondition_failed_keeps_fresh_read_hint() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig::default());
        let outcome = ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "The target changed before write_file could overwrite it. Call read_file again for the complete current file, then retry write_file with updated content.",
            false,
            None,
        );

        let decision = controller.decide(&[observation(
            "write_file",
            r#"{"path":"file.txt","content":"new"}"#,
            outcome,
        )]);

        let ToolRetryDecision::Retry { prompt, .. } = decision else {
            panic!("expected retry decision");
        };
        let ToolRetryPrompt::Retry { entries } = prompt else {
            panic!("expected retry prompt");
        };
        assert_eq!(entries[0].tool_name, "write_file");
        assert_eq!(entries[0].error_class, ToolErrorClass::ExecutionFailed);
        assert!(entries[0].retry_hint.contains("read_file again"));
        assert!(entries[0].retry_hint.contains("retry write_file"));
    }

    #[test]
    fn retry_prompt_for_edit_file_read_required_keeps_changed_sequence_hint() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig::default());
        let outcome = ToolOutcome::recoverable(
            ToolErrorClass::InvalidArguments,
            "edit_file needs current file state before editing. Call read_file for the complete file, then retry edit_file with exact old_string text copied without line-number prefixes.",
            false,
            None,
        );

        let decision = controller.decide(&[observation(
            "edit_file",
            r#"{"path":"file.txt","old_string":"old","new_string":"new"}"#,
            outcome,
        )]);

        let ToolRetryDecision::Retry { prompt, .. } = decision else {
            panic!("expected retry decision");
        };
        let ToolRetryPrompt::Retry { entries } = prompt else {
            panic!("expected retry prompt");
        };
        assert_eq!(entries[0].tool_name, "edit_file");
        assert_eq!(entries[0].error_class, ToolErrorClass::InvalidArguments);
        assert!(entries[0].retry_hint.contains("read_file"));
        assert!(entries[0].retry_hint.contains("retry edit_file"));
        assert!(
            entries[0]
                .retry_hint
                .contains("without line-number prefixes")
        );
    }

    #[test]
    fn retry_prompt_for_edit_file_precondition_failed_keeps_fresh_read_hint() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig::default());
        let outcome = ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "The target changed before edit_file could modify it. Call read_file again for the complete current file, then retry edit_file with updated exact old_string text.",
            false,
            None,
        );

        let decision = controller.decide(&[observation(
            "edit_file",
            r#"{"path":"file.txt","old_string":"old","new_string":"new"}"#,
            outcome,
        )]);

        let ToolRetryDecision::Retry { prompt, .. } = decision else {
            panic!("expected retry decision");
        };
        let ToolRetryPrompt::Retry { entries } = prompt else {
            panic!("expected retry prompt");
        };
        assert_eq!(entries[0].tool_name, "edit_file");
        assert_eq!(entries[0].error_class, ToolErrorClass::ExecutionFailed);
        assert!(entries[0].retry_hint.contains("read_file again"));
        assert!(entries[0].retry_hint.contains("retry edit_file"));
    }

    #[test]
    fn retry_prompt_for_edit_file_match_failures_requires_corrected_arguments() {
        for (status_name, hint) in [
            (
                "not_found",
                "edit_file could not find old_string. Call read_file again, copy the exact current file text without line-number prefixes, and retry with a corrected old_string.",
            ),
            (
                "ambiguous_match",
                "edit_file old_string matched multiple locations. Retry with more surrounding context for a unique match, or set replace_all=true only if every exact occurrence should change.",
            ),
        ] {
            let mut controller = ToolRetryController::new(ToolRetryBudgetConfig::default());
            let outcome =
                ToolOutcome::recoverable(ToolErrorClass::InvalidArguments, hint, false, None);

            let decision = controller.decide(&[observation(
                "edit_file",
                r#"{"path":"file.txt","old_string":"old","new_string":"new"}"#,
                outcome,
            )]);

            let ToolRetryDecision::Retry { prompt, .. } = decision else {
                panic!("expected retry decision for {status_name}");
            };
            let ToolRetryPrompt::Retry { entries } = prompt else {
                panic!("expected retry prompt for {status_name}");
            };
            assert_eq!(entries[0].tool_name, "edit_file");
            assert_eq!(entries[0].error_class, ToolErrorClass::InvalidArguments);
            assert!(
                entries[0].retry_hint.contains("corrected old_string")
                    || entries[0].retry_hint.contains("more surrounding context")
            );
            assert!(!entries[0].retry_hint.contains("retry the same arguments"));
        }
    }

    #[test]
    fn no_retry_for_fatal_result() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig::default());
        let decision = controller.decide(&[observation(
            "exec_command",
            "{}",
            ToolOutcome::fatal(
                ToolErrorClass::Internal,
                Some("internal failure".to_owned()),
            ),
        )]);

        assert!(matches!(decision, ToolRetryDecision::None { ref drafts } if drafts.is_empty()));
        assert!(controller.state_snapshot().active_episode.is_none());
    }

    #[test]
    fn class_budget_exhaustion_returns_exhausted() {
        let mut controller = ToolRetryController::with_class_budgets(
            ToolRetryBudgetConfig::default(),
            [ToolRetryClassBudget {
                error_class: ToolErrorClass::ExecutionFailed,
                max_retries: 1,
            }],
        );

        retry_decision(&mut controller, ToolErrorClass::ExecutionFailed);
        let decision = controller.decide(&[observation(
            "exec_command",
            r#"{"command":["false"]}"#,
            recoverable(ToolErrorClass::ExecutionFailed),
        )]);

        assert!(matches!(
            decision,
            ToolRetryDecision::Exhausted {
                reason: ToolRetryExhaustionReason::ErrorClass {
                    error_class: ToolErrorClass::ExecutionFailed,
                    used: 1,
                    limit: 1
                },
                ..
            }
        ));
        assert!(controller.state_snapshot().active_episode.is_none());
    }

    #[test]
    fn episode_retry_budget_exhaustion_returns_exhausted() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig {
            max_recoverable_retry_rounds_per_episode: 1,
            max_same_tool_error_retries_per_episode: 8,
            max_retries_per_tool_name_per_episode: 8,
        });

        retry_decision(&mut controller, ToolErrorClass::ExecutionFailed);
        let decision = controller.decide(&[observation(
            "read_file",
            r#"{"path":"missing"}"#,
            recoverable(ToolErrorClass::NotFound),
        )]);

        assert!(matches!(
            decision,
            ToolRetryDecision::Exhausted {
                reason: ToolRetryExhaustionReason::TotalRetryRounds { used: 1, limit: 1 },
                ..
            }
        ));
        assert!(controller.state_snapshot().active_episode.is_none());
    }

    #[test]
    fn per_tool_budget_exhaustion_returns_exhausted() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig {
            max_recoverable_retry_rounds_per_episode: 8,
            max_same_tool_error_retries_per_episode: 8,
            max_retries_per_tool_name_per_episode: 1,
        });

        retry_decision(&mut controller, ToolErrorClass::ExecutionFailed);
        let decision = controller.decide(&[observation(
            "exec_command",
            r#"{"command":["pwd"]}"#,
            recoverable(ToolErrorClass::NotFound),
        )]);

        assert!(matches!(
            decision,
            ToolRetryDecision::Exhausted {
                reason: ToolRetryExhaustionReason::ToolName {
                    ref tool_name,
                    used: 1,
                    limit: 1
                },
                ..
            } if tool_name == "exec_command"
        ));
        assert!(controller.state_snapshot().active_episode.is_none());
    }

    #[test]
    fn same_failure_signature_exhaustion_uses_normalized_json_arguments() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig {
            max_recoverable_retry_rounds_per_episode: 8,
            max_same_tool_error_retries_per_episode: 1,
            max_retries_per_tool_name_per_episode: 8,
        });

        let first = controller.decide(&[observation(
            "exec_command",
            r#"{"b":2,"a":1}"#,
            recoverable(ToolErrorClass::ExecutionFailed),
        )]);
        assert!(matches!(first, ToolRetryDecision::Retry { .. }));

        let decision = controller.decide(&[observation(
            "exec_command",
            r#"{"a":1,"b":2}"#,
            recoverable(ToolErrorClass::ExecutionFailed),
        )]);

        assert!(matches!(
            decision,
            ToolRetryDecision::Exhausted {
                reason: ToolRetryExhaustionReason::FailureSignature {
                    ref signature,
                    used: 1,
                    limit: 1
                },
                ..
            } if signature.arguments_fingerprint == r#"{"a":1,"b":2}"#
        ));
        assert!(controller.state_snapshot().active_episode.is_none());
    }

    #[test]
    fn retry_episode_resets_after_success() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig {
            max_recoverable_retry_rounds_per_episode: 1,
            max_same_tool_error_retries_per_episode: 1,
            max_retries_per_tool_name_per_episode: 1,
        });

        let first = controller.decide(&[observation(
            "web_fetch",
            r#"{"url":"https://example.com"}"#,
            recoverable(ToolErrorClass::ExecutionFailed),
        )]);
        assert!(matches!(first, ToolRetryDecision::Retry { .. }));
        assert_eq!(active_episode(&controller).total_retry_rounds, 1);

        let success = controller.decide(&[ToolRetryObservation::from_tool_outcome(
            "item_web_fetch_success",
            "web_fetch",
            1,
            "web_fetch",
            r#"{"url":"https://example.com"}"#,
            true,
            ToolOutcome::ok(),
        )]);
        assert!(matches!(
            success,
            ToolRetryDecision::None {
                drafts: ref succeeded
            } if matches!(succeeded.as_slice(), [ToolRetryEventDraft::Resolved { entries, .. }] if entries.len() == 1 && entries[0].resolution == ToolRetryResolution::Succeeded)
        ));
        assert!(controller.state_snapshot().active_episode.is_none());

        let next_failure = controller.decide(&[observation(
            "web_fetch",
            r#"{"url":"https://example.com"}"#,
            recoverable(ToolErrorClass::ExecutionFailed),
        )]);
        assert!(matches!(next_failure, ToolRetryDecision::Retry { .. }));
        assert_eq!(active_episode(&controller).total_retry_rounds, 1);
    }

    #[test]
    fn retry_episode_resets_after_non_retryable_resolution() {
        let mut controller = ToolRetryController::new(ToolRetryBudgetConfig {
            max_recoverable_retry_rounds_per_episode: 1,
            max_same_tool_error_retries_per_episode: 1,
            max_retries_per_tool_name_per_episode: 1,
        });

        let first = controller.decide(&[observation(
            "web_fetch",
            r#"{"url":"https://example.com"}"#,
            recoverable(ToolErrorClass::ExecutionFailed),
        )]);
        assert!(matches!(first, ToolRetryDecision::Retry { .. }));

        let fatal = controller.decide(&[observation(
            "web_fetch",
            r#"{"url":"https://example.com"}"#,
            ToolOutcome::fatal(
                ToolErrorClass::PermissionDenied,
                Some("blocked by policy".to_owned()),
            ),
        )]);
        assert!(matches!(
            fatal,
            ToolRetryDecision::None {
                drafts: ref succeeded
            } if matches!(succeeded.as_slice(), [ToolRetryEventDraft::Resolved { entries, .. }] if entries.len() == 1 && entries[0].resolution == ToolRetryResolution::NonRetryable)
        ));
        assert!(controller.state_snapshot().active_episode.is_none());

        let next_failure = controller.decide(&[observation(
            "web_fetch",
            r#"{"url":"https://example.com"}"#,
            recoverable(ToolErrorClass::ExecutionFailed),
        )]);
        assert!(matches!(next_failure, ToolRetryDecision::Retry { .. }));
        assert_eq!(active_episode(&controller).total_retry_rounds, 1);
    }

    #[test]
    fn long_turn_same_tool_episode_resets_after_late_success() {
        let mut controller = ToolRetryController::with_class_budgets(
            ToolRetryBudgetConfig {
                max_recoverable_retry_rounds_per_episode: 16,
                max_same_tool_error_retries_per_episode: 16,
                max_retries_per_tool_name_per_episode: 16,
            },
            [ToolRetryClassBudget {
                error_class: ToolErrorClass::ExecutionFailed,
                max_retries: 16,
            }],
        );
        let args = r#"{"url":"https://example.com/article"}"#;

        for attempt in 1..=15 {
            let decision = controller.decide(&[ToolRetryObservation::from_tool_outcome(
                format!("item_web_fetch_{attempt}"),
                "web_fetch",
                attempt,
                "web_fetch",
                args,
                false,
                recoverable(ToolErrorClass::ExecutionFailed),
            )]);
            assert!(
                matches!(decision, ToolRetryDecision::Retry { .. }),
                "attempt {attempt} should still be inside the active retry episode"
            );
            let episode = active_episode(&controller);
            assert_eq!(episode.total_retry_rounds, attempt);
            assert_eq!(
                episode.by_tool_name.get("web_fetch").copied(),
                Some(attempt)
            );
            assert_eq!(episode.by_failure_signature.len(), 1);
        }

        let success = controller.decide(&[ToolRetryObservation::from_tool_outcome(
            "item_web_fetch_success",
            "web_fetch",
            16,
            "web_fetch",
            args,
            true,
            ToolOutcome::ok(),
        )]);
        assert!(matches!(
            success,
            ToolRetryDecision::None {
                drafts: ref resolved
            } if matches!(resolved.as_slice(), [ToolRetryEventDraft::Resolved { entries, .. }]
                if entries.len() == 1
                    && entries[0].resolution == ToolRetryResolution::Succeeded
                    && entries[0].budgets.iter().any(|budget| budget.kind == ToolRetryBudgetKind::Episode && budget.used == 15 && budget.limit == 16))
        ));
        assert!(
            controller.state_snapshot().active_episode.is_none(),
            "successful tool output must close the retry episode"
        );

        let later_same_tool_failure =
            controller.decide(&[ToolRetryObservation::from_tool_outcome(
                "item_web_fetch_later_failure",
                "web_fetch",
                17,
                "web_fetch",
                args,
                false,
                recoverable(ToolErrorClass::ExecutionFailed),
            )]);
        let ToolRetryDecision::Retry { prompt, .. } = later_same_tool_failure else {
            panic!("later same-tool failure must start a fresh retry episode");
        };
        let ToolRetryPrompt::Retry { entries } = prompt else {
            panic!("expected retry prompt for fresh episode");
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].episode_retry_used, 1);
        assert_eq!(entries[0].class_retry_used, 1);
        assert_eq!(entries[0].tool_retry_used, 1);
        assert_eq!(entries[0].signature_retry_used, 1);
        let fresh_episode = active_episode(&controller);
        assert_eq!(fresh_episode.total_retry_rounds, 1);
        assert_eq!(
            fresh_episode.by_class.get(&ToolErrorClass::ExecutionFailed),
            Some(&1)
        );
        assert_eq!(fresh_episode.by_tool_name.get("web_fetch"), Some(&1));
        assert_eq!(fresh_episode.by_failure_signature.len(), 1);
    }
}
