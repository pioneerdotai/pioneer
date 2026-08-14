use std::collections::BTreeSet;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use pioneer_protocol::{
    CLIRuntimePendingRequest, CLIRuntimeRequestKind, CLIRuntimeRequestResolution,
    TurnPermissionApprovalRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use zeroize::{Zeroize, Zeroizing};

pub(crate) const HUMAN_INTERACTION_RESPONSE_TIMEOUT: Duration = Duration::from_secs(15 * 60);
pub(crate) const HUMAN_INTERACTION_RESPONSE_TIMEOUT_MS: i64 = 15 * 60 * 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HumanInteractionBudget {
    pub(crate) max_pending_requests_per_execution: usize,
    pub(crate) max_questions_per_request: usize,
    pub(crate) max_options_per_question: usize,
    pub(crate) max_field_bytes: usize,
    pub(crate) max_aggregate_bytes: usize,
    pub(crate) max_response_bytes: usize,
    pub(crate) max_json_depth: usize,
    pub(crate) max_json_nodes: usize,
}

impl Default for HumanInteractionBudget {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl HumanInteractionBudget {
    pub(crate) const DEFAULT: Self = Self {
        max_pending_requests_per_execution: 8,
        max_questions_per_request: 16,
        max_options_per_question: 32,
        max_field_bytes: 4 * 1024,
        max_aggregate_bytes: 64 * 1024,
        max_response_bytes: 64 * 1024,
        max_json_depth: 16,
        max_json_nodes: 2_048,
    };
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ValidatedHumanInteraction {
    secret_question_ids: BTreeSet<String>,
}

pub(crate) struct EphemeralCliResolution(CLIRuntimeRequestResolution);

impl EphemeralCliResolution {
    pub(crate) fn new(resolution: CLIRuntimeRequestResolution) -> Self {
        Self(resolution)
    }
}

impl Deref for EphemeralCliResolution {
    type Target = CLIRuntimeRequestResolution;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for EphemeralCliResolution {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for EphemeralCliResolution {
    fn drop(&mut self) {
        HumanInteractionService::new().zeroize_resolution(&mut self.0);
    }
}

impl ValidatedHumanInteraction {
    pub(crate) fn contains_secret_answer(&self, resolution: &CLIRuntimeRequestResolution) -> bool {
        !self.secret_question_ids.is_empty()
            && matches!(resolution, CLIRuntimeRequestResolution::Answered { .. })
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct HumanInteractionService {
    budget: HumanInteractionBudget,
}

impl HumanInteractionService {
    pub(crate) const fn new() -> Self {
        Self {
            budget: HumanInteractionBudget::DEFAULT,
        }
    }

    pub(crate) const fn from_budget(budget: HumanInteractionBudget) -> Self {
        Self { budget }
    }

    pub(crate) const fn budget(self) -> HumanInteractionBudget {
        self.budget
    }

    #[cfg(test)]
    pub(crate) fn validate_pending_count(self, current_pending: usize) -> Result<()> {
        if current_pending >= self.budget.max_pending_requests_per_execution {
            bail!(
                "execution already has the maximum {} pending human interactions",
                self.budget.max_pending_requests_per_execution
            );
        }
        Ok(())
    }

    pub(crate) fn validate_native_request(
        self,
        request: &TurnPermissionApprovalRequest,
    ) -> Result<()> {
        validate_field("native request id", request.request_id.as_str(), 256)?;
        validate_field("native tool name", request.tool_name.as_str(), 256)?;
        validate_field(
            "native action",
            request.action.as_str(),
            self.budget.max_field_bytes,
        )?;
        validate_field(
            "native scope hash",
            request.scope_hash.as_str(),
            self.budget.max_field_bytes,
        )?;
        if request.details.len() > 64 {
            bail!("native permission request contains too many detail rows");
        }
        if let Some(summary) = request.summary.as_deref() {
            validate_field(
                "native request summary",
                summary,
                self.budget.max_field_bytes,
            )?;
        }
        for detail in &request.details {
            validate_field("native detail label", detail.label.as_str(), 256)?;
            validate_field(
                "native detail value",
                detail.value.as_str(),
                self.budget.max_field_bytes,
            )?;
        }
        let encoded = serde_json::to_vec(request)
            .context("failed to encode native human interaction for budget validation")?;
        if encoded.len() > self.budget.max_aggregate_bytes {
            bail!("native permission request exceeds the aggregate byte budget");
        }
        Ok(())
    }

    pub(crate) fn validate_cli_request(
        self,
        request: &CLIRuntimePendingRequest,
    ) -> Result<ValidatedHumanInteraction> {
        if let Some(title) = request.title.as_deref() {
            validate_field("CLI request title", title, 512)?;
        }
        if let Some(message) = request.message.as_deref() {
            validate_field("CLI request message", message, self.budget.max_field_bytes)?;
        }
        let encoded = serde_json::to_vec(request)
            .context("failed to encode CLI human interaction for budget validation")?;
        if encoded.len() > self.budget.max_aggregate_bytes {
            bail!("CLI human interaction exceeds the aggregate byte budget");
        }

        let mut nodes = 0usize;
        if let Some(payload) = request.payload.as_ref() {
            validate_json_shape(payload, 0, &mut nodes, self.budget)?;
        }

        if request.kind != CLIRuntimeRequestKind::UserInput {
            return Ok(ValidatedHumanInteraction::default());
        }
        let questions = request
            .payload
            .as_ref()
            .and_then(|payload| payload.get("questions"))
            .and_then(JsonValue::as_array)
            .context("CLI user-input request has no questions")?;
        if questions.is_empty() || questions.len() > self.budget.max_questions_per_request {
            bail!(
                "CLI user-input request must contain between 1 and {} questions",
                self.budget.max_questions_per_request
            );
        }

        let mut ids = BTreeSet::new();
        let mut secret_question_ids = BTreeSet::new();
        for question in questions {
            let id = question
                .get("id")
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .context("CLI user-input question has no id")?;
            validate_field("CLI question id", id, 128)?;
            if !ids.insert(id.to_owned()) {
                bail!("CLI user-input request contains duplicate question id `{id}`");
            }
            let prompt = question
                .get("question")
                .and_then(JsonValue::as_str)
                .context("CLI user-input question has no prompt")?;
            validate_field("CLI question prompt", prompt, self.budget.max_field_bytes)?;
            if let Some(header) = question.get("header").and_then(JsonValue::as_str) {
                validate_field("CLI question header", header, 256)?;
            }
            let options = question
                .get("options")
                .and_then(JsonValue::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default();
            if options.len() > self.budget.max_options_per_question {
                bail!("CLI user-input question contains too many options");
            }
            for option in options {
                let label = option
                    .get("label")
                    .and_then(JsonValue::as_str)
                    .context("CLI user-input option has no label")?;
                validate_field("CLI option label", label, 512)?;
                if let Some(description) = option.get("description").and_then(JsonValue::as_str) {
                    validate_field(
                        "CLI option description",
                        description,
                        self.budget.max_field_bytes,
                    )?;
                }
            }
            if question
                .get("isSecret")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false)
            {
                secret_question_ids.insert(id.to_owned());
            }
        }
        Ok(ValidatedHumanInteraction {
            secret_question_ids,
        })
    }

    pub(crate) fn validate_client_resolution(
        self,
        request: &CLIRuntimePendingRequest,
        resolution: &CLIRuntimeRequestResolution,
    ) -> Result<ValidatedHumanInteraction> {
        let validated = self.validate_cli_request(request)?;
        let encoded = Zeroizing::new(
            serde_json::to_vec(resolution)
                .context("failed to encode human interaction response for budget validation")?,
        );
        if encoded.len() > self.budget.max_response_bytes {
            bail!("human interaction response exceeds the response byte budget");
        }
        match request.kind {
            CLIRuntimeRequestKind::CommandApproval
            | CLIRuntimeRequestKind::FileChangeApproval
            | CLIRuntimeRequestKind::Other => match resolution {
                CLIRuntimeRequestResolution::Approved | CLIRuntimeRequestResolution::Cancelled => {}
                CLIRuntimeRequestResolution::Denied { reason } => {
                    if let Some(reason) = reason.as_deref() {
                        validate_field("denial reason", reason, self.budget.max_field_bytes)?;
                    }
                }
                CLIRuntimeRequestResolution::Answered { .. }
                | CLIRuntimeRequestResolution::Expired
                | CLIRuntimeRequestResolution::Error { .. } => {
                    bail!("approval requests accept only canonical approve, deny, or cancel")
                }
            },
            CLIRuntimeRequestKind::UserInput => match resolution {
                CLIRuntimeRequestResolution::Answered {
                    response: Some(response),
                } => {
                    let mut response_nodes = 0usize;
                    validate_json_shape(response, 0, &mut response_nodes, self.budget)?;
                    let answer_count = response
                        .get("answers")
                        .and_then(JsonValue::as_object)
                        .map(|answers| answers.len())
                        .unwrap_or(1);
                    if answer_count > self.budget.max_questions_per_request {
                        bail!("CLI user-input response contains too many answers");
                    }
                }
                CLIRuntimeRequestResolution::Cancelled => {}
                _ => bail!("user-input requests must be answered or cancelled"),
            },
        }
        Ok(validated)
    }

    pub(crate) fn durable_resolution(
        self,
        resolution: &CLIRuntimeRequestResolution,
        contains_secret: bool,
    ) -> CLIRuntimeRequestResolution {
        if contains_secret {
            CLIRuntimeRequestResolution::Answered { response: None }
        } else {
            resolution.clone()
        }
    }

    pub(crate) fn zeroize_resolution(self, resolution: &mut CLIRuntimeRequestResolution) {
        match resolution {
            CLIRuntimeRequestResolution::Denied { reason } => {
                if let Some(reason) = reason {
                    reason.zeroize();
                }
            }
            CLIRuntimeRequestResolution::Answered { response } => {
                if let Some(response) = response {
                    zeroize_json(response);
                }
            }
            CLIRuntimeRequestResolution::Error { message } => message.zeroize(),
            CLIRuntimeRequestResolution::Approved
            | CLIRuntimeRequestResolution::Cancelled
            | CLIRuntimeRequestResolution::Expired => {}
        }
    }

    pub(crate) fn zeroize_json_value(self, value: &mut JsonValue) {
        zeroize_json(value);
    }
}

fn validate_field(label: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.as_bytes().len() > max_bytes {
        bail!("{label} exceeds the {max_bytes}-byte limit");
    }
    Ok(())
}

fn validate_json_shape(
    value: &JsonValue,
    depth: usize,
    nodes: &mut usize,
    budget: HumanInteractionBudget,
) -> Result<()> {
    if depth > budget.max_json_depth {
        bail!("human interaction JSON exceeds the depth budget");
    }
    *nodes = nodes.saturating_add(1);
    if *nodes > budget.max_json_nodes {
        bail!("human interaction JSON exceeds the node budget");
    }
    match value {
        JsonValue::String(value) => {
            validate_field("human interaction string", value, budget.max_field_bytes)?;
        }
        JsonValue::Array(values) => {
            if values.len() > budget.max_json_nodes {
                bail!("human interaction array exceeds the cardinality budget");
            }
            for value in values {
                validate_json_shape(value, depth + 1, nodes, budget)?;
            }
        }
        JsonValue::Object(values) => {
            if values.len() > budget.max_json_nodes {
                bail!("human interaction object exceeds the cardinality budget");
            }
            for (key, value) in values {
                validate_field("human interaction key", key, 256)?;
                validate_json_shape(value, depth + 1, nodes, budget)?;
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => {}
    }
    Ok(())
}

fn zeroize_json(value: &mut JsonValue) {
    match value {
        JsonValue::String(value) => value.zeroize(),
        JsonValue::Array(values) => values.iter_mut().for_each(zeroize_json),
        JsonValue::Object(values) => values.values_mut().for_each(zeroize_json),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn user_input_request(questions: JsonValue) -> CLIRuntimePendingRequest {
        CLIRuntimePendingRequest {
            kind: CLIRuntimeRequestKind::UserInput,
            title: Some("Need input".to_owned()),
            message: None,
            native_request_id: Some("native-request".to_owned()),
            payload: Some(json!({ "questions": questions })),
        }
    }

    #[test]
    fn human_interaction_budget_rejects_cardinality_fields_and_pending_overflow() {
        let budget = HumanInteractionBudget {
            max_pending_requests_per_execution: 2,
            max_questions_per_request: 1,
            max_options_per_question: 1,
            max_field_bytes: 16,
            max_aggregate_bytes: 4 * 1024,
            max_response_bytes: 4 * 1024,
            max_json_depth: 8,
            max_json_nodes: 64,
        };
        let service = HumanInteractionService::from_budget(budget);

        assert!(service.validate_pending_count(1).is_ok());
        assert!(service.validate_pending_count(2).is_err());

        let too_many_questions = user_input_request(json!([
            { "id": "one", "question": "First?" },
            { "id": "two", "question": "Second?" }
        ]));
        assert!(service.validate_cli_request(&too_many_questions).is_err());

        let too_many_options = user_input_request(json!([{
            "id": "one",
            "question": "Choose",
            "options": [{ "label": "A" }, { "label": "B" }]
        }]));
        assert!(service.validate_cli_request(&too_many_options).is_err());

        let oversized_field = user_input_request(json!([{
            "id": "one",
            "question": "this prompt is intentionally longer than sixteen bytes"
        }]));
        assert!(service.validate_cli_request(&oversized_field).is_err());
    }

    #[test]
    fn secret_human_interaction_response_is_bounded_ephemeral_and_not_durable() {
        let service = HumanInteractionService::new();
        let request = user_input_request(json!([{
            "id": "token",
            "question": "Token?",
            "isSecret": true
        }]));
        let mut resolution = CLIRuntimeRequestResolution::Answered {
            response: Some(json!({ "answers": { "token": "super-secret" } })),
        };
        let validated = service
            .validate_client_resolution(&request, &resolution)
            .expect("bounded canonical secret answer must validate");
        assert!(validated.contains_secret_answer(&resolution));
        assert_eq!(
            service.durable_resolution(&resolution, true),
            CLIRuntimeRequestResolution::Answered { response: None }
        );

        service.zeroize_resolution(&mut resolution);
        let CLIRuntimeRequestResolution::Answered {
            response: Some(response),
        } = resolution
        else {
            panic!("zeroization must preserve the typed ephemeral envelope");
        };
        assert_eq!(response["answers"]["token"], "");
    }

    #[test]
    fn client_resolution_must_match_request_kind_and_response_budget() {
        let service = HumanInteractionService::from_budget(HumanInteractionBudget {
            max_response_bytes: 96,
            ..HumanInteractionBudget::DEFAULT
        });
        let approval = CLIRuntimePendingRequest {
            kind: CLIRuntimeRequestKind::CommandApproval,
            title: None,
            message: None,
            native_request_id: Some("approval".to_owned()),
            payload: None,
        };
        assert!(
            service
                .validate_client_resolution(
                    &approval,
                    &CLIRuntimeRequestResolution::Answered {
                        response: Some(json!({ "answers": {} })),
                    },
                )
                .is_err()
        );

        let request = user_input_request(json!([{
            "id": "answer",
            "question": "Value?"
        }]));
        let oversized = CLIRuntimeRequestResolution::Answered {
            response: Some(json!({ "answers": { "answer": "x".repeat(256) } })),
        };
        assert!(
            service
                .validate_client_resolution(&request, &oversized)
                .is_err()
        );
    }
}
