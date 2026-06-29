use pioneer_client::cli_runtime::approvals::{
    PendingRequest, PendingRequestPresentation, PendingRequestResolution,
    PendingRequestResponseAction, plan_pending_request_response, present_pending_request,
};
use pioneer_protocol::{CLIRuntimeRequestRespondParams, TurnPermissionRequestRespondParams};
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClientPendingRequestResponsePlanRequest {
    pub request: PendingRequest,
    pub resolution: PendingRequestResolution,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClientPendingRequestPresentationRequest {
    pub request: PendingRequest,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ClientPendingRequestPresentationResult {
    pub presentation: PendingRequestPresentation,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ClientPendingRequestResponsePlanResult {
    pub action: ClientPendingRequestResponseAction,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "target", rename_all = "snake_case")]
pub enum ClientPendingRequestResponseAction {
    #[serde(rename = "cli_runtime")]
    CLIRuntime {
        method: String,
        params: CLIRuntimeRequestRespondParams,
    },
    NativePermissionGate {
        method: String,
        params: TurnPermissionRequestRespondParams,
    },
}

pub fn plan_pending_request_response_for_bridge(
    request: ClientPendingRequestResponsePlanRequest,
) -> Result<ClientPendingRequestResponsePlanResult, String> {
    let action = plan_pending_request_response(&request.request, request.resolution)
        .map_err(|error| format!("invalid pending request response plan: {error:?}"))?;

    Ok(ClientPendingRequestResponsePlanResult {
        action: action.into(),
    })
}

pub fn pending_request_presentation_for_bridge(
    request: ClientPendingRequestPresentationRequest,
) -> Result<ClientPendingRequestPresentationResult, String> {
    Ok(ClientPendingRequestPresentationResult {
        presentation: present_pending_request(&request.request),
    })
}

impl From<PendingRequestResponseAction> for ClientPendingRequestResponseAction {
    fn from(action: PendingRequestResponseAction) -> Self {
        match action {
            PendingRequestResponseAction::CLIRuntime { method, params } => {
                Self::CLIRuntime { method, params }
            }
            PendingRequestResponseAction::NativePermissionGate { method, params } => {
                Self::NativePermissionGate { method, params }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::cli_runtime::approvals::PendingRequest;
    use pioneer_protocol::{TurnPermissionApprovalRequest, TurnPermissionApprovalResolution};

    fn native_pending_request() -> PendingRequest {
        PendingRequest::from_native_permission_request(TurnPermissionApprovalRequest {
            request_id: "req_native".to_owned(),
            workspace_id: "ws".to_owned(),
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            tool_name: "exec_command".to_owned(),
            action: pioneer_protocol::TurnPermissionActionKind::ShellCommand,
            scope_hash: "scope".to_owned(),
            reason: pioneer_protocol::TurnPermissionDecisionReason::PolicyRequiresApproval,
            summary: None,
            details: Vec::new(),
        })
    }

    #[test]
    fn pending_request_response_plan_bridge_uses_client_planner() {
        let result =
            plan_pending_request_response_for_bridge(ClientPendingRequestResponsePlanRequest {
                request: native_pending_request(),
                resolution: PendingRequestResolution::AllowForTurn,
            })
            .expect("bridge should plan response");

        let ClientPendingRequestResponseAction::NativePermissionGate { method, params } =
            result.action
        else {
            panic!("expected native permission action");
        };
        assert_eq!(
            method,
            pioneer_protocol::constants::methods::TURN_PERMISSION_REQUEST_RESPOND
        );
        assert_eq!(params.request_id, "req_native");
        assert_eq!(
            params.resolution,
            TurnPermissionApprovalResolution::AllowForTurn
        );
    }

    #[test]
    fn pending_request_presentation_bridge_uses_client_renderer() {
        let result =
            pending_request_presentation_for_bridge(ClientPendingRequestPresentationRequest {
                request: native_pending_request(),
            })
            .expect("bridge should present request");

        assert_eq!(result.presentation.origin_label, "Native agent request");
        assert!(result.presentation.actions.iter().any(|action| {
            action.kind
                == pioneer_client::cli_runtime::approvals::PendingRequestActionKind::AllowForTurn
                && action.resolution == Some(PendingRequestResolution::AllowForTurn)
        }));
    }
}
