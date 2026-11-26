use crate::TerminalTaskObservation;
use pioneer_protocol::TaskStatus;
use pioneer_provider::{ChatMessage, Role};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

/// Identity of the terminal outcome actually delivered, never just a task ID.
/// Task revision changes on resume, including when the same run is reopened.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ObservedTaskOutcome {
    TaskRevision {
        task_id: String,
        revision: i64,
    },
    AcceptedCandidate {
        task_id: String,
        run_id: String,
        candidate_id: String,
    },
}

impl TerminalTaskObservation {
    pub fn outcome(&self) -> ObservedTaskOutcome {
        ObservedTaskOutcome::TaskRevision {
            task_id: self.task_id.clone(),
            revision: self.task_revision,
        }
    }

    pub fn was_observed(&self, observed: &BTreeSet<ObservedTaskOutcome>) -> bool {
        observed.contains(&self.outcome())
            || (self.status == "completed"
                && self
                    .run_id
                    .as_ref()
                    .zip(self.accepted_candidate_id.as_ref())
                    .is_some_and(|(run_id, candidate_id)| {
                        observed.contains(&ObservedTaskOutcome::AcceptedCandidate {
                            task_id: self.task_id.clone(),
                            run_id: run_id.clone(),
                            candidate_id: candidate_id.clone(),
                        })
                    }))
    }
}

/// Interpret only the public result of an identified, successful Pioneer task
/// tool. Callers own invocation identity, delivery and lifecycle validation.
pub fn observed_terminal_task_outcomes_from_payload(
    tool: &str,
    value: &Value,
) -> BTreeSet<ObservedTaskOutcome> {
    fn terminal_task(value: &Value) -> Option<ObservedTaskOutcome> {
        let status: TaskStatus = serde_json::from_value(value.get("status")?.clone()).ok()?;
        if !status.is_terminal() {
            return None;
        }
        Some(ObservedTaskOutcome::TaskRevision {
            task_id: value.get("id")?.as_str()?.to_owned(),
            revision: value.get("revision")?.as_i64()?,
        })
    }
    match tool {
        "task_wait" => ["completed", "failed", "blocked", "cancelled"]
            .into_iter()
            .filter_map(|bucket| value.get(bucket)?.as_array())
            .flatten()
            .filter_map(|item| terminal_task(item.get("task")?))
            .collect(),
        "task_get" | "task_cancel" | "task_finish" => value
            .get("task")
            .and_then(terminal_task)
            .into_iter()
            .collect(),
        "task_accept"
            if value.get("taskTerminal").and_then(Value::as_bool) == Some(true)
                && value.get("status").and_then(Value::as_str) == Some("completed") =>
        {
            // Accept has no public task revision. Its immutable accepted candidate
            // is checked against the current completed run by the snapshot owner.
            // A blocked accept response cannot leave a dormant candidate key
            // that would match a later completion after same-run resume.
            let candidate = || {
                Some(ObservedTaskOutcome::AcceptedCandidate {
                    task_id: value.get("taskId")?.as_str()?.to_owned(),
                    run_id: value.get("runId")?.as_str()?.to_owned(),
                    candidate_id: value.get("candidateId")?.as_str()?.to_owned(),
                })
            };
            candidate().into_iter().collect()
        }
        _ => BTreeSet::new(),
    }
}

pub fn reports_terminal_outcome(tool: &str) -> bool {
    matches!(
        tool,
        "task_wait" | "task_get" | "task_cancel" | "task_finish" | "task_accept"
    )
}

/// Interpret the final provider representation. A bounded non-JSON excerpt
/// provides no complete structured evidence for suppressing a notice.
pub fn observed_terminal_task_outcomes_from_message(
    message: &ChatMessage,
) -> BTreeSet<ObservedTaskOutcome> {
    if message.role == Role::Tool
        && let Some(name) = message
            .name
            .as_deref()
            .filter(|name| reports_terminal_outcome(name))
        && let Ok(payload) = serde_json::from_str(&message.content)
    {
        return observed_terminal_task_outcomes_from_payload(name, &payload);
    }
    BTreeSet::new()
}

const OBSERVATIONS: &str = "observed_task_outcomes_v1";

#[derive(Serialize, Deserialize)]
struct SavedObservations {
    had_context: bool,
    outcomes: BTreeSet<ObservedTaskOutcome>,
}

pub fn save_observations(message: &mut ChatMessage, observed: &BTreeSet<ObservedTaskOutcome>) {
    remove_observations(message);
    let had_context = message.application_context.is_some();
    if let Some(metadata) = message
        .application_context
        .get_or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
    {
        // An explicit empty set proves that this result was already assessed.
        metadata.insert(
            OBSERVATIONS.into(),
            serde_json::json!(SavedObservations {
                had_context,
                outcomes: observed.clone(),
            }),
        );
    }
}

pub fn saved_observations(message: &ChatMessage) -> BTreeSet<ObservedTaskOutcome> {
    message
        .application_context
        .as_ref()
        .and_then(|metadata| metadata.get(OBSERVATIONS))
        .and_then(|value| serde_json::from_value::<SavedObservations>(value.clone()).ok())
        .map(|saved| saved.outcomes)
        .unwrap_or_default()
}

/// Outcome evidence is local bookkeeping. Preserve the original envelope,
/// including an originally empty context, when building the provider request.
pub(crate) fn remove_observations(message: &mut ChatMessage) {
    if let Some(metadata) = message
        .application_context
        .as_mut()
        .and_then(Value::as_object_mut)
        && let Some(saved) = metadata.remove(OBSERVATIONS)
        && metadata.is_empty()
        && saved.get("had_context").and_then(Value::as_bool) != Some(true)
    {
        message.application_context = None;
    }
}
