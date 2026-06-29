//! Client-side pending request state for CLI runtime approvals.

use pioneer_protocol::{
    CLIRuntimePendingRequest, CLIRuntimeRequestKind, CLIRuntimeRequestOpenedNotification,
    CLIRuntimeRequestResolution, CLIRuntimeRequestResolvedNotification,
    CLIRuntimeRequestRespondParams, TurnPermissionApprovalRequest,
    TurnPermissionApprovalResolution, TurnPermissionRequestOpenedNotification,
    TurnPermissionRequestResolvedNotification, TurnPermissionRequestRespondParams,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingRequestKind {
    CommandApproval,
    FileChangeApproval,
    UserInput,
    Other,
}

impl From<CLIRuntimeRequestKind> for PendingRequestKind {
    fn from(kind: CLIRuntimeRequestKind) -> Self {
        match kind {
            CLIRuntimeRequestKind::CommandApproval => Self::CommandApproval,
            CLIRuntimeRequestKind::FileChangeApproval => Self::FileChangeApproval,
            CLIRuntimeRequestKind::UserInput => Self::UserInput,
            CLIRuntimeRequestKind::Other => Self::Other,
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "origin", rename_all = "snake_case")]
pub enum PendingRequestOrigin {
    #[serde(rename = "cli_runtime")]
    CLIRuntime {
        runtime_id: String,
    },
    NativePermissionGate,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum PendingRequestPayload {
    #[serde(rename = "cli_runtime")]
    CLIRuntime { request: CLIRuntimePendingRequest },
    NativePermissionGate {
        request: TurnPermissionApprovalRequest,
    },
    Other {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<serde_json::Value>,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PendingRequest {
    pub workspace_id: String,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    pub origin: PendingRequestOrigin,
    pub kind: PendingRequestKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_request_id: Option<String>,
    pub payload: PendingRequestPayload,
}

impl PendingRequest {
    pub fn from_cli_runtime_opened_notification(
        notification: CLIRuntimeRequestOpenedNotification,
    ) -> Self {
        CLIRuntimePendingRequestEntry::from_opened_notification(notification).into_pending_request()
    }

    pub fn from_native_permission_request(request: TurnPermissionApprovalRequest) -> Self {
        Self {
            workspace_id: request.workspace_id.clone(),
            request_id: request.request_id.clone(),
            thread_id: Some(request.thread_id.clone()),
            turn_id: Some(request.turn_id.clone()),
            item_id: None,
            origin: PendingRequestOrigin::NativePermissionGate,
            kind: PendingRequestKind::Other,
            title: Some(request.tool_name.clone()),
            message: request
                .summary
                .clone()
                .or_else(|| Some(request.reason.as_str().to_owned())),
            native_request_id: Some(request.request_id.clone()),
            payload: PendingRequestPayload::NativePermissionGate { request },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PendingRequestsReduction {
    Opened(PendingRequest),
    Resolved {
        request_id: String,
    },
    TerminalTurn {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
    },
    ThreadClosed {
        workspace_id: String,
        thread_id: String,
    },
    ClearWorkspace {
        workspace_id: String,
    },
    ClearAll,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "resolution", rename_all = "snake_case")]
pub enum PendingRequestResolution {
    Allow,
    AllowForTurn,
    AllowForSession,
    Deny {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    Cancel,
    Answered {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        response: Option<serde_json::Value>,
    },
    Expired,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PendingRequestResponseAction {
    CLIRuntime {
        method: String,
        params: CLIRuntimeRequestRespondParams,
    },
    NativePermissionGate {
        method: String,
        params: TurnPermissionRequestRespondParams,
    },
}

impl PendingRequestResponseAction {
    pub fn method(&self) -> &str {
        match self {
            Self::CLIRuntime { method, .. } | Self::NativePermissionGate { method, .. } => method,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingRequestResponsePlanError {
    OriginPayloadMismatch,
    UnsupportedResolutionForOrigin,
}

pub fn plan_pending_request_response(
    request: &PendingRequest,
    resolution: PendingRequestResolution,
) -> Result<PendingRequestResponseAction, PendingRequestResponsePlanError> {
    match (&request.origin, &request.payload) {
        (
            PendingRequestOrigin::CLIRuntime { runtime_id },
            PendingRequestPayload::CLIRuntime { .. },
        ) => Ok(PendingRequestResponseAction::CLIRuntime {
            method: pioneer_protocol::constants::methods::CLI_RUNTIME_REQUEST_RESPOND.to_owned(),
            params: CLIRuntimeRequestRespondParams {
                workspace_id: request.workspace_id.clone(),
                runtime_id: runtime_id.clone(),
                request_id: request.request_id.clone(),
                resolution: cli_runtime_resolution_from_pending_resolution(resolution),
            },
        }),
        (
            PendingRequestOrigin::NativePermissionGate,
            PendingRequestPayload::NativePermissionGate { .. },
        ) => {
            let resolution = turn_permission_resolution_from_pending_resolution(resolution)?;
            Ok(PendingRequestResponseAction::NativePermissionGate {
                method: pioneer_protocol::constants::methods::TURN_PERMISSION_REQUEST_RESPOND
                    .to_owned(),
                params: TurnPermissionRequestRespondParams {
                    request_id: request.request_id.clone(),
                    resolution,
                },
            })
        }
        _ => Err(PendingRequestResponsePlanError::OriginPayloadMismatch),
    }
}

fn cli_runtime_resolution_from_pending_resolution(
    resolution: PendingRequestResolution,
) -> CLIRuntimeRequestResolution {
    match resolution {
        PendingRequestResolution::Allow => CLIRuntimeRequestResolution::Approved,
        PendingRequestResolution::AllowForTurn => CLIRuntimeRequestResolution::Answered {
            response: Some(serde_json::json!({ "decision": "allow_for_turn" })),
        },
        PendingRequestResolution::AllowForSession => CLIRuntimeRequestResolution::Answered {
            response: Some(serde_json::json!({ "decision": "allow_for_session" })),
        },
        PendingRequestResolution::Deny { reason } => CLIRuntimeRequestResolution::Denied { reason },
        PendingRequestResolution::Cancel => CLIRuntimeRequestResolution::Cancelled,
        PendingRequestResolution::Answered { response } => {
            CLIRuntimeRequestResolution::Answered { response }
        }
        PendingRequestResolution::Expired => CLIRuntimeRequestResolution::Expired,
    }
}

fn turn_permission_resolution_from_pending_resolution(
    resolution: PendingRequestResolution,
) -> Result<TurnPermissionApprovalResolution, PendingRequestResponsePlanError> {
    match resolution {
        PendingRequestResolution::Allow => Ok(TurnPermissionApprovalResolution::AllowOnce),
        PendingRequestResolution::AllowForTurn => {
            Ok(TurnPermissionApprovalResolution::AllowForTurn)
        }
        PendingRequestResolution::AllowForSession => {
            Err(PendingRequestResponsePlanError::UnsupportedResolutionForOrigin)
        }
        PendingRequestResolution::Deny { .. } => Ok(TurnPermissionApprovalResolution::Deny),
        PendingRequestResolution::Cancel => Ok(TurnPermissionApprovalResolution::Cancelled),
        PendingRequestResolution::Expired => Ok(TurnPermissionApprovalResolution::Expired),
        PendingRequestResolution::Answered { .. } => {
            Err(PendingRequestResponsePlanError::UnsupportedResolutionForOrigin)
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PendingRequestState {
    requests: Vec<PendingRequest>,
}

impl PendingRequestState {
    pub fn requests(&self) -> &[PendingRequest] {
        self.requests.as_slice()
    }

    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    pub fn pending_for_thread(&self, thread_id: Option<&str>) -> Vec<PendingRequest> {
        self.pending_for_scope(None, thread_id)
    }

    pub fn pending_for_scope(
        &self,
        workspace_id: Option<&str>,
        thread_id: Option<&str>,
    ) -> Vec<PendingRequest> {
        self.requests
            .iter()
            .filter(|request| pending_request_matches_scope(request, workspace_id, thread_id))
            .cloned()
            .collect()
    }

    pub fn cli_runtime_pending_for_thread(
        &self,
        thread_id: Option<&str>,
    ) -> Vec<CLIRuntimePendingRequestEntry> {
        self.cli_runtime_pending_for_scope(None, thread_id)
    }

    pub fn cli_runtime_pending_for_scope(
        &self,
        workspace_id: Option<&str>,
        thread_id: Option<&str>,
    ) -> Vec<CLIRuntimePendingRequestEntry> {
        self.requests
            .iter()
            .filter(|request| pending_request_matches_scope(request, workspace_id, thread_id))
            .filter_map(CLIRuntimePendingRequestEntry::from_pending_request)
            .collect()
    }

    pub fn request(&self, request_id: &str) -> Option<&PendingRequest> {
        self.requests
            .iter()
            .find(|request| request.request_id == request_id)
    }

    pub fn apply<R>(&mut self, reduction: R) -> bool
    where
        R: Into<PendingRequestsReduction>,
    {
        match reduction.into() {
            PendingRequestsReduction::Opened(request) => {
                if let Some(existing) = self
                    .requests
                    .iter_mut()
                    .find(|existing| existing.request_id == request.request_id)
                {
                    let changed = existing != &request;
                    *existing = request;
                    return changed;
                }

                self.requests.push(request);
                true
            }
            PendingRequestsReduction::Resolved { request_id } => {
                remove_matching(&mut self.requests, |request| {
                    request.request_id == request_id
                })
            }
            PendingRequestsReduction::TerminalTurn {
                workspace_id,
                thread_id,
                turn_id,
            } => remove_matching(&mut self.requests, |request| {
                request.workspace_id == workspace_id
                    && request.thread_id.as_deref() == Some(thread_id.as_str())
                    && request.turn_id.as_deref() == Some(turn_id.as_str())
            }),
            PendingRequestsReduction::ThreadClosed {
                workspace_id,
                thread_id,
            } => remove_matching(&mut self.requests, |request| {
                request.workspace_id == workspace_id
                    && request.thread_id.as_deref() == Some(thread_id.as_str())
            }),
            PendingRequestsReduction::ClearWorkspace { workspace_id } => {
                remove_matching(&mut self.requests, |request| {
                    request.workspace_id == workspace_id
                })
            }
            PendingRequestsReduction::ClearAll => {
                if self.requests.is_empty() {
                    false
                } else {
                    self.requests.clear();
                    true
                }
            }
        }
    }
}

fn pending_request_matches_scope(
    request: &PendingRequest,
    workspace_id: Option<&str>,
    thread_id: Option<&str>,
) -> bool {
    workspace_id.map_or(true, |workspace_id| request.workspace_id == workspace_id)
        && match thread_id {
            Some(thread_id) => request.thread_id.as_deref() == Some(thread_id),
            None => request.thread_id.is_none(),
        }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CLIRuntimePendingRequestEntry {
    pub workspace_id: String,
    pub runtime_id: String,
    pub request_id: String,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub request: CLIRuntimePendingRequest,
}

impl CLIRuntimePendingRequestEntry {
    pub fn from_opened_notification(notification: CLIRuntimeRequestOpenedNotification) -> Self {
        Self {
            workspace_id: notification.workspace_id,
            runtime_id: notification.runtime_id,
            request_id: notification.request_id,
            thread_id: notification.thread_id,
            turn_id: notification.turn_id,
            item_id: notification.item_id,
            request: notification.request,
        }
    }

    pub fn from_pending_request(request: &PendingRequest) -> Option<Self> {
        let PendingRequestOrigin::CLIRuntime { runtime_id } = &request.origin else {
            return None;
        };
        let PendingRequestPayload::CLIRuntime {
            request: cli_request,
        } = &request.payload
        else {
            return None;
        };

        Some(Self {
            workspace_id: request.workspace_id.clone(),
            runtime_id: runtime_id.clone(),
            request_id: request.request_id.clone(),
            thread_id: request.thread_id.clone(),
            turn_id: request.turn_id.clone(),
            item_id: request.item_id.clone(),
            request: cli_request.clone(),
        })
    }

    pub fn to_pending_request(&self) -> PendingRequest {
        PendingRequest {
            workspace_id: self.workspace_id.clone(),
            request_id: self.request_id.clone(),
            thread_id: self.thread_id.clone(),
            turn_id: self.turn_id.clone(),
            item_id: self.item_id.clone(),
            origin: PendingRequestOrigin::CLIRuntime {
                runtime_id: self.runtime_id.clone(),
            },
            kind: self.request.kind.into(),
            title: self.request.title.clone(),
            message: self.request.message.clone(),
            native_request_id: self.request.native_request_id.clone(),
            payload: PendingRequestPayload::CLIRuntime {
                request: self.request.clone(),
            },
        }
    }

    pub fn into_pending_request(self) -> PendingRequest {
        PendingRequest {
            workspace_id: self.workspace_id,
            request_id: self.request_id,
            thread_id: self.thread_id,
            turn_id: self.turn_id,
            item_id: self.item_id,
            origin: PendingRequestOrigin::CLIRuntime {
                runtime_id: self.runtime_id,
            },
            kind: self.request.kind.into(),
            title: self.request.title.clone(),
            message: self.request.message.clone(),
            native_request_id: self.request.native_request_id.clone(),
            payload: PendingRequestPayload::CLIRuntime {
                request: self.request,
            },
        }
    }
}

pub type CLIRuntimePendingRequestsReduction = PendingRequestsReduction;
pub type CLIRuntimePendingRequestState = PendingRequestState;

pub fn reduce_cli_runtime_request_opened_notification(
    notification: CLIRuntimeRequestOpenedNotification,
) -> PendingRequestsReduction {
    PendingRequestsReduction::Opened(PendingRequest::from_cli_runtime_opened_notification(
        notification,
    ))
}

pub fn reduce_cli_runtime_request_resolved_notification(
    notification: CLIRuntimeRequestResolvedNotification,
) -> PendingRequestsReduction {
    PendingRequestsReduction::Resolved {
        request_id: notification.request_id,
    }
}

pub fn reduce_native_permission_request_opened_notification(
    notification: TurnPermissionRequestOpenedNotification,
) -> PendingRequestsReduction {
    PendingRequestsReduction::Opened(PendingRequest::from_native_permission_request(
        notification.request,
    ))
}

pub fn reduce_native_permission_request_resolved_notification(
    notification: TurnPermissionRequestResolvedNotification,
) -> PendingRequestsReduction {
    PendingRequestsReduction::Resolved {
        request_id: notification.request_id,
    }
}

pub fn reduce_cli_runtime_terminal_turn_cleanup(
    workspace_id: String,
    thread_id: String,
    turn_id: String,
) -> PendingRequestsReduction {
    reduce_pending_request_terminal_turn_cleanup(workspace_id, thread_id, turn_id)
}

pub fn reduce_pending_request_terminal_turn_cleanup(
    workspace_id: String,
    thread_id: String,
    turn_id: String,
) -> PendingRequestsReduction {
    PendingRequestsReduction::TerminalTurn {
        workspace_id,
        thread_id,
        turn_id,
    }
}

pub fn reduce_cli_runtime_thread_closed_cleanup(
    workspace_id: String,
    thread_id: String,
) -> PendingRequestsReduction {
    reduce_pending_request_thread_closed_cleanup(workspace_id, thread_id)
}

pub fn reduce_pending_request_thread_closed_cleanup(
    workspace_id: String,
    thread_id: String,
) -> PendingRequestsReduction {
    PendingRequestsReduction::ThreadClosed {
        workspace_id,
        thread_id,
    }
}

fn remove_matching<T>(requests: &mut Vec<T>, mut matches: impl FnMut(&T) -> bool) -> bool {
    let initial_len = requests.len();
    requests.retain(|request| !matches(request));
    requests.len() != initial_len
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{CLIRuntimeRequestKind, CLIRuntimeRequestResolution};

    fn request_opened(
        request_id: &str,
        workspace_id: &str,
        runtime_id: &str,
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        command: &str,
    ) -> CLIRuntimeRequestOpenedNotification {
        CLIRuntimeRequestOpenedNotification {
            workspace_id: workspace_id.to_owned(),
            runtime_id: runtime_id.to_owned(),
            request_id: request_id.to_owned(),
            thread_id: thread_id.map(str::to_owned),
            turn_id: turn_id.map(str::to_owned),
            item_id: None,
            request: CLIRuntimePendingRequest {
                kind: CLIRuntimeRequestKind::CommandApproval,
                title: Some(format!("Run {command}")),
                message: Some(command.to_owned()),
                native_request_id: Some(format!("native_{request_id}")),
                payload: None,
            },
        }
    }

    fn file_change_request_opened(
        request_id: &str,
        workspace_id: &str,
        runtime_id: &str,
        thread_id: Option<&str>,
        turn_id: Option<&str>,
    ) -> CLIRuntimeRequestOpenedNotification {
        CLIRuntimeRequestOpenedNotification {
            workspace_id: workspace_id.to_owned(),
            runtime_id: runtime_id.to_owned(),
            request_id: request_id.to_owned(),
            thread_id: thread_id.map(str::to_owned),
            turn_id: turn_id.map(str::to_owned),
            item_id: Some("item_file_change".to_owned()),
            request: CLIRuntimePendingRequest {
                kind: CLIRuntimeRequestKind::FileChangeApproval,
                title: Some("Apply file changes".to_owned()),
                message: Some("Review file edits".to_owned()),
                native_request_id: Some(format!("native_{request_id}")),
                payload: Some(serde_json::json!({
                    "changedFiles": ["src/main.rs"],
                    "diffPreview": { "text": "-old\n+new" }
                })),
            },
        }
    }

    fn native_permission_request(request_id: &str) -> TurnPermissionApprovalRequest {
        TurnPermissionApprovalRequest {
            request_id: request_id.to_owned(),
            workspace_id: "ws".to_owned(),
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            tool_name: "exec_command".to_owned(),
            action: pioneer_protocol::TurnPermissionActionKind::ShellCommand,
            scope_hash: "scope".to_owned(),
            reason: pioneer_protocol::TurnPermissionDecisionReason::PolicyRequiresApproval,
            summary: Some("Approve command".to_owned()),
            details: Vec::new(),
        }
    }

    #[test]
    fn cli_runtime_pending_request_entry_projects_to_shared_model() {
        let entry = CLIRuntimePendingRequestEntry::from_opened_notification(request_opened(
            "req",
            "ws",
            "codex",
            Some("thread"),
            Some("turn"),
            "pwd",
        ));

        let pending = entry.to_pending_request();

        assert_eq!(pending.workspace_id, "ws");
        assert_eq!(pending.request_id, "req");
        assert_eq!(pending.thread_id.as_deref(), Some("thread"));
        assert_eq!(pending.turn_id.as_deref(), Some("turn"));
        assert_eq!(
            pending.origin,
            PendingRequestOrigin::CLIRuntime {
                runtime_id: "codex".to_owned()
            }
        );
        assert_eq!(pending.kind, PendingRequestKind::CommandApproval);
        assert_eq!(pending.title.as_deref(), Some("Run pwd"));
        assert_eq!(pending.message.as_deref(), Some("pwd"));
        assert_eq!(pending.native_request_id.as_deref(), Some("native_req"));
        assert!(matches!(
            pending.payload,
            PendingRequestPayload::CLIRuntime { .. }
        ));
    }

    #[test]
    fn cli_runtime_opened_notification_projects_to_shared_model() {
        let pending = PendingRequest::from_cli_runtime_opened_notification(request_opened(
            "req",
            "ws",
            "claude",
            Some("thread"),
            Some("turn"),
            "ls",
        ));

        assert_eq!(
            pending.origin,
            PendingRequestOrigin::CLIRuntime {
                runtime_id: "claude".to_owned()
            }
        );
        assert_eq!(pending.kind, PendingRequestKind::CommandApproval);
        assert!(matches!(
            pending.payload,
            PendingRequestPayload::CLIRuntime { .. }
        ));
    }

    #[test]
    fn cli_runtime_file_change_request_projects_to_shared_model() {
        let pending = PendingRequest::from_cli_runtime_opened_notification(
            file_change_request_opened("req_file", "ws", "codex", Some("thread"), Some("turn")),
        );

        assert_eq!(pending.workspace_id, "ws");
        assert_eq!(pending.request_id, "req_file");
        assert_eq!(pending.item_id.as_deref(), Some("item_file_change"));
        assert_eq!(
            pending.origin,
            PendingRequestOrigin::CLIRuntime {
                runtime_id: "codex".to_owned()
            }
        );
        assert_eq!(pending.kind, PendingRequestKind::FileChangeApproval);
        assert_eq!(pending.title.as_deref(), Some("Apply file changes"));
        assert_eq!(
            pending.native_request_id.as_deref(),
            Some("native_req_file")
        );
        let PendingRequestPayload::CLIRuntime { request } = pending.payload else {
            panic!("expected CLI runtime payload");
        };
        assert_eq!(request.kind, CLIRuntimeRequestKind::FileChangeApproval);
        assert_eq!(
            request
                .payload
                .as_ref()
                .and_then(|payload| payload.get("changedFiles"))
                .and_then(serde_json::Value::as_array)
                .map(Vec::len),
            Some(1)
        );
    }

    #[test]
    fn native_permission_request_projects_to_shared_model_placeholder() {
        let pending =
            PendingRequest::from_native_permission_request(native_permission_request("req_native"));

        assert_eq!(pending.workspace_id, "ws");
        assert_eq!(pending.request_id, "req_native");
        assert_eq!(pending.thread_id.as_deref(), Some("thread"));
        assert_eq!(pending.turn_id.as_deref(), Some("turn"));
        assert_eq!(pending.origin, PendingRequestOrigin::NativePermissionGate);
        assert_eq!(pending.kind, PendingRequestKind::Other);
        assert_eq!(pending.title.as_deref(), Some("exec_command"));
        assert_eq!(pending.message.as_deref(), Some("Approve command"));
        assert_eq!(pending.native_request_id.as_deref(), Some("req_native"));
        assert!(matches!(
            pending.payload,
            PendingRequestPayload::NativePermissionGate { .. }
        ));
    }

    #[test]
    fn shared_pending_request_serializes_roundtrip() {
        let pending =
            PendingRequest::from_native_permission_request(native_permission_request("req_native"));

        let encoded = serde_json::to_string(&pending).expect("serialize pending request");
        let decoded: PendingRequest =
            serde_json::from_str(&encoded).expect("deserialize pending request");

        assert_eq!(decoded, pending);
    }

    #[test]
    fn response_planner_routes_cli_runtime_request_to_cli_rpc_params() {
        let pending = PendingRequest::from_cli_runtime_opened_notification(request_opened(
            "req",
            "ws",
            "codex",
            Some("thread"),
            Some("turn"),
            "pwd",
        ));

        let action =
            plan_pending_request_response(&pending, PendingRequestResolution::AllowForSession)
                .expect("plan CLI response");

        let PendingRequestResponseAction::CLIRuntime { method, params } = action else {
            panic!("expected CLI runtime action");
        };
        assert_eq!(
            method,
            pioneer_protocol::constants::methods::CLI_RUNTIME_REQUEST_RESPOND
        );
        assert_eq!(params.workspace_id, "ws");
        assert_eq!(params.runtime_id, "codex");
        assert_eq!(params.request_id, "req");
        assert_eq!(
            params.resolution,
            CLIRuntimeRequestResolution::Answered {
                response: Some(serde_json::json!({ "decision": "allow_for_session" }))
            }
        );
    }

    #[test]
    fn response_planner_routes_native_permission_request_to_native_rpc_params() {
        let pending =
            PendingRequest::from_native_permission_request(native_permission_request("req_native"));

        let action =
            plan_pending_request_response(&pending, PendingRequestResolution::AllowForTurn)
                .expect("plan native response");

        let PendingRequestResponseAction::NativePermissionGate { method, params } = action else {
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
    fn response_planner_rejects_session_resolution_for_native_permission_request() {
        let pending =
            PendingRequest::from_native_permission_request(native_permission_request("req_native"));

        let error =
            plan_pending_request_response(&pending, PendingRequestResolution::AllowForSession)
                .expect_err("native permission approvals are scoped to the turn");

        assert_eq!(
            error,
            PendingRequestResponsePlanError::UnsupportedResolutionForOrigin
        );
    }

    #[test]
    fn response_planner_rejects_answered_resolution_for_native_permission_request() {
        let pending =
            PendingRequest::from_native_permission_request(native_permission_request("req_native"));

        let error = plan_pending_request_response(
            &pending,
            PendingRequestResolution::Answered {
                response: Some(serde_json::json!({ "text": "hello" })),
            },
        )
        .expect_err("native permission requests are not user-input answers");

        assert_eq!(
            error,
            PendingRequestResponsePlanError::UnsupportedResolutionForOrigin
        );
    }

    #[test]
    fn cli_runtime_approval_state_tracks_concurrent_requests() {
        let mut state = CLIRuntimePendingRequestState::default();

        assert!(state.apply(reduce_cli_runtime_request_opened_notification(
            request_opened(
                "req_a",
                "ws",
                "codex",
                Some("thread"),
                Some("turn_a"),
                "pwd"
            )
        )));
        assert!(state.apply(reduce_cli_runtime_request_opened_notification(
            request_opened("req_b", "ws", "codex", Some("thread"), Some("turn_b"), "ls")
        )));

        let requests = state.pending_for_thread(Some("thread"));
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].request_id, "req_a");
        assert_eq!(requests[1].request_id, "req_b");

        let cli_requests = state.cli_runtime_pending_for_thread(Some("thread"));
        assert_eq!(cli_requests.len(), 2);
        assert_eq!(cli_requests[0].runtime_id, "codex");
        assert_eq!(cli_requests[1].runtime_id, "codex");
    }

    #[test]
    fn cli_runtime_approval_state_ignores_stale_resolution() {
        let mut state = CLIRuntimePendingRequestState::default();

        assert!(
            !state.apply(reduce_cli_runtime_request_resolved_notification(
                CLIRuntimeRequestResolvedNotification {
                    workspace_id: "ws".to_owned(),
                    runtime_id: "codex".to_owned(),
                    request_id: "missing".to_owned(),
                    thread_id: Some("thread".to_owned()),
                    turn_id: Some("turn".to_owned()),
                    item_id: None,
                    resolution: CLIRuntimeRequestResolution::Cancelled,
                },
            ),)
        );
        assert!(state.is_empty());
    }

    #[test]
    fn cli_runtime_approval_state_removes_resolved_request_only() {
        let mut state = CLIRuntimePendingRequestState::default();
        state.apply(reduce_cli_runtime_request_opened_notification(
            request_opened("req_a", "ws", "codex", Some("thread"), Some("turn"), "pwd"),
        ));
        state.apply(reduce_cli_runtime_request_opened_notification(
            request_opened("req_b", "ws", "codex", Some("thread"), Some("turn"), "ls"),
        ));

        assert!(
            state.apply(reduce_cli_runtime_request_resolved_notification(
                CLIRuntimeRequestResolvedNotification {
                    workspace_id: "ws".to_owned(),
                    runtime_id: "codex".to_owned(),
                    request_id: "req_a".to_owned(),
                    thread_id: Some("thread".to_owned()),
                    turn_id: Some("turn".to_owned()),
                    item_id: None,
                    resolution: CLIRuntimeRequestResolution::Approved,
                },
            ),)
        );

        assert_eq!(state.requests().len(), 1);
        assert_eq!(state.requests()[0].request_id, "req_b");
    }

    #[test]
    fn pending_request_state_uses_same_reducer_for_native_permission_requests() {
        let mut state = PendingRequestState::default();

        assert!(
            state.apply(reduce_native_permission_request_opened_notification(
                TurnPermissionRequestOpenedNotification {
                    request: native_permission_request("req_native"),
                }
            ))
        );
        assert_eq!(state.requests().len(), 1);
        assert_eq!(
            state.requests()[0].origin,
            PendingRequestOrigin::NativePermissionGate
        );
        assert!(
            state
                .cli_runtime_pending_for_thread(Some("thread"))
                .is_empty()
        );

        assert!(
            state.apply(reduce_native_permission_request_resolved_notification(
                TurnPermissionRequestResolvedNotification {
                    request_id: "req_native".to_owned(),
                    workspace_id: "ws".to_owned(),
                    thread_id: "thread".to_owned(),
                    turn_id: "turn".to_owned(),
                    resolution: pioneer_protocol::TurnPermissionApprovalResolution::Deny,
                }
            ))
        );
        assert!(state.is_empty());
    }

    #[test]
    fn cli_runtime_approval_state_cleans_up_terminal_turn_requests() {
        let mut state = CLIRuntimePendingRequestState::default();
        state.apply(reduce_cli_runtime_request_opened_notification(
            request_opened(
                "req_a",
                "ws",
                "codex",
                Some("thread"),
                Some("turn_a"),
                "pwd",
            ),
        ));
        state.apply(reduce_cli_runtime_request_opened_notification(
            request_opened("req_b", "ws", "codex", Some("thread"), Some("turn_b"), "ls"),
        ));

        assert!(state.apply(reduce_cli_runtime_terminal_turn_cleanup(
            "ws".to_owned(),
            "thread".to_owned(),
            "turn_a".to_owned(),
        )));

        assert_eq!(state.requests().len(), 1);
        assert_eq!(state.requests()[0].request_id, "req_b");
    }
}
