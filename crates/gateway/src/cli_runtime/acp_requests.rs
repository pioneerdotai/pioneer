//! Standard ACP human requests in the existing pending-request FSM.

use anyhow::{Context, Result, bail};
use pioneer_cli_agent_runtime::acp::{self, CallbackRequest, schema};
use pioneer_protocol::{
    CLIRuntimePendingRequest, CLIRuntimeRequestKind, CLIRuntimeRequestResolution,
};
use serde_json::{Value, json};

pub(crate) fn request(payload: &Value) -> Result<CallbackRequest> {
    let envelope = payload
        .get("acp")
        .context("ACP pending request has no protocol envelope")?;
    let method = envelope
        .get("method")
        .and_then(Value::as_str)
        .context("ACP pending request has no method")?;
    let params = envelope
        .get("params")
        .cloned()
        .context("ACP pending request has no params")?;
    acp::parse_human_request(method, params).map_err(|error| anyhow::anyhow!(error.message))
}

pub(crate) fn validate_pending(pending: &CLIRuntimePendingRequest) -> Result<bool> {
    let typed = request(
        pending
            .payload
            .as_ref()
            .context("ACP pending request has no payload")?,
    )?;
    anyhow::ensure!(
        matches!(
            (&typed, pending.kind),
            (
                CallbackRequest::RequestPermissionRequest(_),
                CLIRuntimeRequestKind::PermissionApproval
            ) | (
                CallbackRequest::CreateElicitationRequest(_),
                CLIRuntimeRequestKind::UserInput
            )
        ),
        "ACP pending request kind does not match its standard method"
    );
    Ok(
        matches!(typed, CallbackRequest::CreateElicitationRequest(ref request)
        if matches!(request.mode, schema::ElicitationMode::Form(_))),
    )
}

/// An Answered response carries the selected opaque optionId or exact typed
/// form content. Native approve/session decisions cannot express that choice.
pub(crate) fn response(
    payload: &Value,
    resolution: &CLIRuntimeRequestResolution,
) -> Result<(Value, bool)> {
    let request = request(payload)?;
    let response = match resolution {
        CLIRuntimeRequestResolution::Answered {
            response: Some(response),
        } => response.clone(),
        CLIRuntimeRequestResolution::Cancelled
        | CLIRuntimeRequestResolution::Expired
        | CLIRuntimeRequestResolution::Error { .. } => match &request {
            CallbackRequest::RequestPermissionRequest(_) => {
                json!({"outcome":{"outcome":"cancelled"}})
            }
            CallbackRequest::CreateElicitationRequest(_) => json!({"action":"cancel"}),
            _ => bail!("not an ACP human request"),
        },
        CLIRuntimeRequestResolution::Denied { .. } => match &request {
            CallbackRequest::RequestPermissionRequest(permission) => {
                let mut choices = permission
                    .options
                    .iter()
                    .filter(|option| option.kind == schema::PermissionOptionKind::RejectOnce);
                let first = choices
                    .next()
                    .context("select a specific rejection option for this ACP request")?;
                anyhow::ensure!(
                    choices.next().is_none(),
                    "select a specific rejection option for this ACP request"
                );
                json!({"outcome":{"outcome":"selected", "optionId":first.option_id}})
            }
            CallbackRequest::CreateElicitationRequest(_) => json!({"action":"decline"}),
            _ => bail!("not an ACP human request"),
        },
        _ => bail!("ACP requests require a specific offered option or typed response"),
    };
    let response = acp::validate_callback_response(&request, response)
        .map_err(|error| anyhow::anyhow!(error.message))?;
    // Cancellation of a permission request is tied to prompt cancellation.
    // URL consent/decline never asserts that the URL flow has completed.
    let interrupt = matches!(request, CallbackRequest::RequestPermissionRequest(_))
        && response.pointer("/outcome/outcome").and_then(Value::as_str) == Some("cancelled");
    Ok((response, interrupt))
}
