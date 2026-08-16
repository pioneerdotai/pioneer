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
use zeroize::Zeroize;

const MAX_HUMAN_INTERACTION_QUESTIONS: usize = 16;
const MAX_HUMAN_INTERACTION_OPTIONS_PER_QUESTION: usize = 32;
const MAX_NATIVE_PERMISSION_DETAIL_ROWS: usize = 64;
pub(crate) const HUMAN_INTERACTION_RESPONSE_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);
pub(crate) const HUMAN_INTERACTION_RESPONSE_TIMEOUT_MS: i64 = 6 * 60 * 60 * 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HumanInteractionBudget {
    pub(crate) max_pending_requests_per_execution: usize,
}

impl Default for HumanInteractionBudget {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl HumanInteractionBudget {
    pub(crate) const DEFAULT: Self = Self {
        max_pending_requests_per_execution: 8,
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
        if request.details.len() > MAX_NATIVE_PERMISSION_DETAIL_ROWS {
            bail!("native permission request contains too many detail rows");
        }
        Ok(())
    }

    pub(crate) fn validate_cli_request(
        self,
        request: &CLIRuntimePendingRequest,
    ) -> Result<ValidatedHumanInteraction> {
        if request.kind != CLIRuntimeRequestKind::UserInput {
            return Ok(ValidatedHumanInteraction::default());
        }
        let questions = request
            .payload
            .as_ref()
            .and_then(|payload| payload.get("questions"))
            .and_then(JsonValue::as_array)
            .context("CLI user-input request has no questions")?;
        if questions.is_empty() || questions.len() > MAX_HUMAN_INTERACTION_QUESTIONS {
            bail!(
                "CLI user-input request must contain between 1 and {} questions",
                MAX_HUMAN_INTERACTION_QUESTIONS
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
            if !ids.insert(id.to_owned()) {
                bail!("CLI user-input request contains duplicate question id `{id}`");
            }
            question
                .get("question")
                .and_then(JsonValue::as_str)
                .context("CLI user-input question has no prompt")?;
            let options = question
                .get("options")
                .and_then(JsonValue::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default();
            if options.len() > MAX_HUMAN_INTERACTION_OPTIONS_PER_QUESTION {
                bail!("CLI user-input question contains too many options");
            }
            for option in options {
                option
                    .get("label")
                    .and_then(JsonValue::as_str)
                    .context("CLI user-input option has no label")?;
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
        match request.kind {
            CLIRuntimeRequestKind::CommandApproval
            | CLIRuntimeRequestKind::FileChangeApproval
            | CLIRuntimeRequestKind::Other => match resolution {
                CLIRuntimeRequestResolution::Approved | CLIRuntimeRequestResolution::Cancelled => {}
                CLIRuntimeRequestResolution::Denied { .. } => {}
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
                    let answer_count = response
                        .get("answers")
                        .and_then(JsonValue::as_object)
                        .map(|answers| answers.len())
                        .unwrap_or(1);
                    if answer_count > MAX_HUMAN_INTERACTION_QUESTIONS {
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
    fn human_interaction_enforces_product_cardinality_and_pending_overflow() {
        let budget = HumanInteractionBudget {
            max_pending_requests_per_execution: 2,
        };
        let service = HumanInteractionService::from_budget(budget);

        assert!(service.validate_pending_count(1).is_ok());
        assert!(service.validate_pending_count(2).is_err());

        let too_many_questions = user_input_request(JsonValue::Array(
            (0..=MAX_HUMAN_INTERACTION_QUESTIONS)
                .map(|index| json!({ "id": index.to_string(), "question": "Question?" }))
                .collect(),
        ));
        assert!(service.validate_cli_request(&too_many_questions).is_err());

        let too_many_options = user_input_request(json!([{
            "id": "one",
            "question": "Choose",
            "options": (0..=MAX_HUMAN_INTERACTION_OPTIONS_PER_QUESTION)
                .map(|index| json!({ "label": index.to_string() }))
                .collect::<Vec<_>>()
        }]));
        assert!(service.validate_cli_request(&too_many_options).is_err());
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
    fn client_resolution_must_match_request_kind() {
        let service = HumanInteractionService::new();
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
    }
}
