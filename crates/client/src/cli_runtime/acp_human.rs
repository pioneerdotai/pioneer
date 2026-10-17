//! Shared presentation and typed answers for standard ACP human requests.
//! Shells keep editable strings; opaque permission IDs and form conversion
//! remain here, and the Gateway repeats validation against the captured origin.
use super::approvals::*;
use anyhow::{Context, Result, bail, ensure};
use pioneer_protocol::{CLIRuntimePendingRequest, CLIRuntimeRequestResolution, acp};
use serde_json::{Value, json};

fn envelope(request: &CLIRuntimePendingRequest) -> Option<&Value> {
    request.payload.as_ref()?.get("acp")
}
pub fn is_acp(request: &CLIRuntimePendingRequest) -> bool {
    envelope(request).is_some()
}
fn pending(request: &PendingRequest) -> Option<&CLIRuntimePendingRequest> {
    match &request.payload {
        PendingRequestPayload::CLIRuntime { request } => Some(request),
        _ => None,
    }
}
fn action(
    kind: PendingRequestActionKind,
    label: Option<String>,
    response: Value,
) -> PendingRequestAvailableAction {
    PendingRequestAvailableAction {
        kind,
        label,
        resolution: Some(PendingRequestResolution::Answered {
            response: Some(response),
        }),
    }
}
pub fn present(request: &PendingRequest) -> Option<PendingRequestPresentation> {
    let envelope = envelope(pending(request)?)?;
    let params = envelope.get("params")?;
    let mut presentation = PendingRequestPresentation {
        origin_label: pending_request_origin_label(&request.origin).into(),
        kind_label: pending_request_kind_label(request.kind).into(),
        title: request
            .title
            .clone()
            .unwrap_or_else(|| pending_request_kind_label(request.kind).into()),
        message: request.message.clone(),
        details: vec![],
        user_input_questions: vec![],
        actions: vec![],
        external_url: None,
    };
    match envelope.get("method")?.as_str()? {
        "session/request_permission" => {
            let permission: acp::RequestPermissionRequest =
                serde_json::from_value(params.clone()).ok()?;
            presentation.title = permission
                .tool_call
                .fields
                .title
                .clone()
                .unwrap_or(presentation.title);
            for option in permission.options {
                let kind = match option.kind {
                    acp::PermissionOptionKind::AllowOnce => PendingRequestActionKind::Allow,
                    acp::PermissionOptionKind::AllowAlways => {
                        PendingRequestActionKind::AllowForSession
                    }
                    _ => PendingRequestActionKind::Deny,
                };
                presentation.actions.push(action(
                    kind,
                    Some(option.name),
                    json!({"outcome":{"outcome":"selected","optionId":option.option_id}}),
                ));
            }
        }
        "elicitation/create" => {
            let elicitation: acp::CreateElicitationRequest =
                serde_json::from_value(params.clone()).ok()?;
            pioneer_protocol::acp_validation::validate_request(&elicitation).ok()?;
            presentation.message = Some(elicitation.message);
            presentation.actions.push(action(
                PendingRequestActionKind::Deny,
                None,
                json!({"action":"decline"}),
            ));
            match elicitation.mode {
                acp::ElicitationMode::Url(url) => {
                    presentation.external_url = Some(url.url);
                    presentation.actions.push(action(
                        PendingRequestActionKind::Allow,
                        None,
                        json!({"action":"accept"}),
                    ));
                }
                acp::ElicitationMode::Form(form) => {
                    let schema = serde_json::to_value(form.requested_schema).ok()?;
                    for (id, field) in schema.get("properties")?.as_object()? {
                        let mut options = vec![];
                        let choices = if field["type"] == "array" {
                            &field["items"]
                        } else {
                            field
                        };
                        if let Some(values) = choices.get("enum").and_then(Value::as_array) {
                            for (index, value) in values.iter().enumerate() {
                                let value = value.as_str()?.to_owned();
                                options.push(PendingRequestUserInputOption {
                                    label: choices
                                        .get("enumNames")
                                        .and_then(Value::as_array)
                                        .and_then(|names| names.get(index))
                                        .and_then(Value::as_str)
                                        .unwrap_or(&value)
                                        .to_owned(),
                                    value: Some(value),
                                    description: None,
                                });
                            }
                        } else if let Some(values) = choices
                            .get("oneOf")
                            .or_else(|| choices.get("anyOf"))
                            .and_then(Value::as_array)
                        {
                            for value in values {
                                let id = value.get("const")?.as_str()?.to_owned();
                                options.push(PendingRequestUserInputOption {
                                    label: value
                                        .get("title")
                                        .and_then(Value::as_str)
                                        .unwrap_or(&id)
                                        .into(),
                                    value: Some(id),
                                    description: value
                                        .get("description")
                                        .and_then(Value::as_str)
                                        .map(str::to_owned),
                                });
                            }
                        }
                        if field["type"] == "boolean" {
                            for value in [false, true] {
                                options.push(PendingRequestUserInputOption {
                                    label: value.to_string(),
                                    value: Some(value.to_string()),
                                    description: None,
                                });
                            }
                        }
                        let required = schema["required"]
                            .as_array()
                            .is_some_and(|fields| fields.iter().any(|v| v == id));
                        let mut value_schema = field.clone();
                        value_schema
                            .as_object_mut()?
                            .insert("required".into(), Value::Bool(required));
                        presentation
                            .user_input_questions
                            .push(PendingRequestUserInputQuestion {
                                id: id.clone(),
                                header: field
                                    .get("title")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned),
                                question: field
                                    .get("description")
                                    .and_then(Value::as_str)
                                    .unwrap_or(id)
                                    .into(),
                                options,
                                // Stable ACP forms have no password format.
                                // Credential authorization uses explicit URL/terminal flows.
                                is_secret: false,
                                value_schema: Some(value_schema),
                            });
                    }
                    if presentation.user_input_questions.is_empty() {
                        presentation.actions.push(action(
                            PendingRequestActionKind::Allow,
                            None,
                            json!({"action":"accept","content":{}}),
                        ));
                    } else {
                        presentation.actions.push(PendingRequestAvailableAction {
                            kind: PendingRequestActionKind::Answer,
                            label: None,
                            resolution: None,
                        });
                    }
                }
                _ => return None,
            }
        }
        _ => return None,
    }
    presentation.actions.insert(
        0,
        PendingRequestAvailableAction {
            kind: PendingRequestActionKind::CancelTurn,
            label: None,
            resolution: Some(PendingRequestResolution::Cancel),
        },
    );
    Some(presentation)
}

/// Native callers preserve their existing string answers. ACP fields become
/// schema-directed JSON values before any transport or durable response exists.
pub fn answered_resolution(
    request: &PendingRequest,
    answers: impl IntoIterator<Item = (String, String)>,
) -> Result<PendingRequestResolution> {
    let answers = answers
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
    let Some(cli) = pending(request).filter(|request| is_acp(request)) else {
        return Ok(pending_request_answered_resolution(answers));
    };
    let presentation = present(request).context("ACP request cannot be presented")?;
    ensure!(
        presentation.external_url.is_none(),
        "URL consent requires an explicit action"
    );
    let mut content = serde_json::Map::new();
    for question in presentation.user_input_questions {
        let schema = question.value_schema.context("form field has no schema")?;
        let default = schema.get("default").map(|value| match value {
            Value::String(text) => text.clone(),
            value => value.to_string(),
        });
        let text = answers
            .get(&question.id)
            .map(String::as_str)
            .or(default.as_deref())
            .unwrap_or("");
        if text.is_empty() && schema["required"] != true {
            continue;
        }
        let value = match schema["type"].as_str() {
            Some("string") => Value::String(text.into()),
            Some("number" | "integer" | "boolean" | "array") => serde_json::from_str(text)
                .with_context(|| {
                    format!(
                        "Invalid value for {}",
                        question.header.as_deref().unwrap_or(&question.id)
                    )
                })?,
            _ => bail!("Unsupported form field"),
        };
        content.insert(question.id, value);
    }
    let resolution = PendingRequestResolution::Answered {
        response: Some(json!({"action":"accept","content":content})),
    };
    validate_resolution(cli, resolution.clone())?;
    Ok(resolution)
}

pub fn validate_resolution(
    request: &CLIRuntimePendingRequest,
    resolution: PendingRequestResolution,
) -> Result<CLIRuntimeRequestResolution> {
    let response = match resolution {
        PendingRequestResolution::Cancel => return Ok(CLIRuntimeRequestResolution::Cancelled),
        PendingRequestResolution::Expired => return Ok(CLIRuntimeRequestResolution::Expired),
        PendingRequestResolution::Answered {
            response: Some(response),
        } => response,
        _ => bail!("Choose a specific offered response"),
    };
    let envelope = envelope(request).context("Missing ACP request")?;
    let params = envelope.get("params").context("Missing ACP params")?;
    match envelope["method"].as_str() {
        Some("session/request_permission") => {
            let permission: acp::RequestPermissionRequest = serde_json::from_value(params.clone())?;
            let answer: acp::RequestPermissionResponse = serde_json::from_value(response.clone())?;
            match answer.outcome {
                acp::RequestPermissionOutcome::Selected(selected) => {
                    ensure!(
                        permission
                            .options
                            .iter()
                            .any(|option| option.option_id == selected.option_id),
                        "Permission option is no longer offered"
                    );
                }
                acp::RequestPermissionOutcome::Cancelled => {}
                _ => bail!("Unsupported permission outcome"),
            }
        }
        Some("elicitation/create") => {
            let request = serde_json::from_value(params.clone())?;
            let answer = serde_json::from_value(response.clone())?;
            pioneer_protocol::acp_validation::validate_response(&request, &answer)
                .map_err(|error| anyhow::anyhow!(error.message))?;
        }
        _ => bail!("Unknown ACP human request"),
    }
    Ok(CLIRuntimeRequestResolution::Answered {
        response: Some(response),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{CLIRuntimeRequestKind, CLIRuntimeRequestOpenedNotification};

    fn pending_request(method: &str, params: Value, permission: bool) -> PendingRequest {
        PendingRequest::from_cli_runtime_opened_notification(CLIRuntimeRequestOpenedNotification {
            workspace_id: "workspace".into(),
            runtime_id: "configured-instance".into(),
            request_id: "request".into(),
            thread_id: Some("thread".into()),
            turn_id: Some("turn".into()),
            item_id: None,
            visible_thread_ids: vec![],
            request: CLIRuntimePendingRequest {
                kind: if permission {
                    CLIRuntimeRequestKind::PermissionApproval
                } else {
                    CLIRuntimeRequestKind::UserInput
                },
                title: None,
                message: None,
                native_request_id: Some("opaque".into()),
                payload: Some(json!({"acp":{"method":method,"params":params}})),
            },
        })
    }

    #[test]
    fn two_same_kind_permission_choices_preserve_their_distinct_opaque_ids() {
        let request = pending_request(
            "session/request_permission",
            json!({"sessionId":"s","toolCall":{"toolCallId":"not-an-mcp-id","title":"apply_patch"},"options":[
                {"optionId":"scope/a","name":"Selected files","kind":"allow_once"},
                {"optionId":"scope/b","name":"Whole change","kind":"allow_once"}
            ]}),
            true,
        );
        let presentation = present(&request).unwrap();
        assert_eq!(presentation.actions.len(), 3);
        for (action, expected) in presentation.actions[1..].iter().zip(["scope/a", "scope/b"]) {
            let plan = plan_pending_request_response(&request, action.resolution.clone().unwrap())
                .unwrap();
            let PendingRequestResponseAction::CLIRuntime { params, .. } = plan else {
                panic!("wrong origin")
            };
            let CLIRuntimeRequestResolution::Answered {
                response: Some(response),
            } = params.resolution
            else {
                panic!("choice was guessed")
            };
            assert_eq!(
                response.pointer("/outcome/optionId"),
                Some(&json!(expected))
            );
        }
        assert!(plan_pending_request_response(&request, PendingRequestResolution::Allow).is_err());
    }

    #[test]
    fn form_values_remain_typed_and_invalid_required_or_enum_values_are_not_sent() {
        let request = pending_request(
            "elicitation/create",
            json!({"sessionId":"s","mode":"form","message":"Provide settings","requestedSchema":{
                "type":"object","required":["enabled","count","choices"],"properties":{
                    "enabled":{"type":"boolean"},"count":{"type":"integer","minimum":1},
                    "choices":{"type":"array","items":{"type":"string","enum":["opaque/a","opaque/b"]}},
                    "contact":{"type":"string","format":"email"}
                }
            }}),
            false,
        );
        assert!(
            present(&request)
                .unwrap()
                .user_input_questions
                .iter()
                .any(|field| field.id == "contact" && !field.is_secret)
        );
        let answer = answered_resolution(
            &request,
            [
                ("enabled".into(), "false".into()),
                ("count".into(), "2".into()),
                ("choices".into(), "[\"opaque/b\"]".into()),
            ],
        )
        .unwrap();
        let PendingRequestResolution::Answered {
            response: Some(response),
        } = answer
        else {
            panic!("missing typed answer")
        };
        assert_eq!(
            response["content"],
            json!({"enabled":false,"count":2,"choices":["opaque/b"]})
        );
        assert!(answered_resolution(&request, [("enabled".into(), "true".into())]).is_err());
    }
    #[test]
    fn explicit_empty_required_string_is_not_replaced_by_its_default() {
        let request = pending_request(
            "elicitation/create",
            json!({
                "sessionId":"s","mode":"form","message":"Review value", "requestedSchema":{
                    "type":"object","required":["text"],"properties":{
                        "text":{"type":"string","default":"old"}
                    }
                }
            }),
            false,
        );
        let response = answered_resolution(&request, [("text".into(), String::new())]).unwrap();
        let PendingRequestResolution::Answered {
            response: Some(value),
        } = response
        else {
            panic!("missing answer")
        };
        assert_eq!(value["content"]["text"], "");
    }
}
