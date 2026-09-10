use pioneer_client::cli_runtime::approvals::{
    PendingRequest, PendingRequestPresentation, present_pending_request,
};
use serde::{Deserialize, Serialize};

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

pub fn pending_request_presentation_for_bridge(
    request: ClientPendingRequestPresentationRequest,
) -> Result<ClientPendingRequestPresentationResult, String> {
    Ok(ClientPendingRequestPresentationResult {
        presentation: present_pending_request(&request.request),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::cli_runtime::approvals::{PendingRequest, PendingRequestResolution};
    use pioneer_protocol::TurnPermissionApprovalRequest;

    fn native_pending_request() -> PendingRequest {
        PendingRequest::from_native_permission_request(TurnPermissionApprovalRequest {
            request_id: "req_native".to_owned(),
            workspace_id: "ws".to_owned(),
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            visible_thread_ids: Vec::new(),
            tool_name: "exec_command".to_owned(),
            action: pioneer_protocol::TurnPermissionActionKind::ShellCommand,
            scope_hash: "scope".to_owned(),
            reason: pioneer_protocol::TurnPermissionDecisionReason::PolicyRequiresApproval,
            summary: None,
            details: Vec::new(),
        })
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
