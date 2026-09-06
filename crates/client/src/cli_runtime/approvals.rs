//! Client-side pending request state for CLI runtime approvals.

use pioneer_protocol::{
    CLIRuntimePendingRequest, CLIRuntimeRequestKind, CLIRuntimeRequestOpenedNotification,
    CLIRuntimeRequestResolution, CLIRuntimeRequestResolvedNotification,
    CLIRuntimeRequestRespondParams, TurnPermissionApprovalRequest,
    TurnPermissionApprovalResolution, TurnPermissionRequestOpenedNotification,
    TurnPermissionRequestResolvedNotification, TurnPermissionRequestRespondParams,
};
use serde_json::{Map as JsonMap, Value as JsonValue};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingRequestKind {
    CommandApproval,
    FileChangeApproval,
    PermissionApproval,
    UserInput,
    Other,
}

impl From<CLIRuntimeRequestKind> for PendingRequestKind {
    fn from(kind: CLIRuntimeRequestKind) -> Self {
        match kind {
            CLIRuntimeRequestKind::CommandApproval => Self::CommandApproval,
            CLIRuntimeRequestKind::FileChangeApproval => Self::FileChangeApproval,
            CLIRuntimeRequestKind::PermissionApproval => Self::PermissionApproval,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visible_thread_ids: Vec<String>,
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
            visible_thread_ids: request.visible_thread_ids.clone(),
            origin: PendingRequestOrigin::NativePermissionGate,
            kind: PendingRequestKind::PermissionApproval,
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
    ResolvedInWorkspace {
        workspace_id: String,
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

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingRequestActionKind {
    CancelTurn,
    Deny,
    Allow,
    AllowForTurn,
    AllowForSession,
    Answer,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PendingRequestAvailableAction {
    pub kind: PendingRequestActionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<PendingRequestResolution>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingRequestDetailStyle {
    Field,
    Diff,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PendingRequestDetailRow {
    pub label: String,
    pub value: String,
    pub monospace: bool,
    pub style: PendingRequestDetailStyle,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PendingRequestUserInputQuestion {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    pub question: String,
    pub options: Vec<PendingRequestUserInputOption>,
    pub is_secret: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PendingRequestUserInputOption {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PendingRequestPresentation {
    pub origin_label: String,
    pub kind_label: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub details: Vec<PendingRequestDetailRow>,
    pub user_input_questions: Vec<PendingRequestUserInputQuestion>,
    pub actions: Vec<PendingRequestAvailableAction>,
}

pub fn present_pending_request(request: &PendingRequest) -> PendingRequestPresentation {
    PendingRequestPresentation {
        origin_label: pending_request_origin_label(&request.origin).to_owned(),
        kind_label: pending_request_kind_label(request.kind).to_owned(),
        title: request
            .title
            .clone()
            .unwrap_or_else(|| pending_request_kind_label(request.kind).to_owned()),
        message: request.message.clone(),
        details: pending_request_detail_rows(request),
        user_input_questions: pending_request_user_input_questions(request),
        actions: pending_request_available_actions(request),
    }
}

pub fn pending_request_available_actions(
    request: &PendingRequest,
) -> Vec<PendingRequestAvailableAction> {
    use PendingRequestActionKind as Kind;

    let kinds = match (&request.origin, request.kind) {
        (PendingRequestOrigin::NativePermissionGate, _) => &[
            Kind::CancelTurn,
            Kind::Deny,
            Kind::AllowForTurn,
            Kind::Allow,
        ][..],
        (PendingRequestOrigin::CLIRuntime { .. }, PendingRequestKind::CommandApproval)
        | (PendingRequestOrigin::CLIRuntime { .. }, PendingRequestKind::FileChangeApproval)
            if cli_runtime_request_supports_session_approval(request) =>
        {
            &[
                Kind::CancelTurn,
                Kind::Deny,
                Kind::AllowForSession,
                Kind::Allow,
            ][..]
        }
        (PendingRequestOrigin::CLIRuntime { .. }, PendingRequestKind::CommandApproval)
        | (PendingRequestOrigin::CLIRuntime { .. }, PendingRequestKind::FileChangeApproval)
        | (PendingRequestOrigin::CLIRuntime { .. }, PendingRequestKind::PermissionApproval) => {
            &[Kind::CancelTurn, Kind::Deny, Kind::Allow][..]
        }
        (PendingRequestOrigin::CLIRuntime { .. }, PendingRequestKind::UserInput) => {
            &[Kind::CancelTurn, Kind::Answer][..]
        }
        (PendingRequestOrigin::CLIRuntime { .. }, PendingRequestKind::Other) => {
            &[Kind::CancelTurn, Kind::Allow][..]
        }
    };

    kinds
        .iter()
        .copied()
        .map(|kind| PendingRequestAvailableAction {
            kind,
            resolution: pending_request_action_resolution(request, kind),
        })
        .collect()
}

pub fn pending_request_action_resolution(
    request: &PendingRequest,
    kind: PendingRequestActionKind,
) -> Option<PendingRequestResolution> {
    match kind {
        PendingRequestActionKind::CancelTurn => Some(PendingRequestResolution::Cancel),
        PendingRequestActionKind::Deny => Some(PendingRequestResolution::Deny { reason: None }),
        PendingRequestActionKind::Allow => Some(PendingRequestResolution::Allow),
        PendingRequestActionKind::AllowForTurn => match (&request.origin, request.kind) {
            (PendingRequestOrigin::NativePermissionGate, _) => {
                Some(PendingRequestResolution::AllowForTurn)
            }
            (
                PendingRequestOrigin::CLIRuntime { .. },
                PendingRequestKind::CommandApproval
                | PendingRequestKind::FileChangeApproval
                | PendingRequestKind::PermissionApproval,
            ) => None,
            (PendingRequestOrigin::CLIRuntime { .. }, _) => None,
        },
        PendingRequestActionKind::AllowForSession => match (&request.origin, request.kind) {
            (
                PendingRequestOrigin::CLIRuntime { .. },
                PendingRequestKind::CommandApproval | PendingRequestKind::FileChangeApproval,
            ) if cli_runtime_request_supports_session_approval(request) => {
                Some(PendingRequestResolution::AllowForSession)
            }
            _ => None,
        },
        PendingRequestActionKind::Answer => None,
    }
}

fn cli_runtime_request_supports_session_approval(request: &PendingRequest) -> bool {
    let PendingRequestPayload::CLIRuntime { request } = &request.payload else {
        return false;
    };
    request
        .payload
        .as_ref()
        .and_then(|payload| payload.get("supportsSessionApproval"))
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
}

pub fn pending_request_answered_resolution(
    answers: impl IntoIterator<Item = (String, String)>,
) -> PendingRequestResolution {
    let mut answer_map = JsonMap::new();
    for (id, answer) in answers {
        answer_map.insert(id, JsonValue::String(answer));
    }

    PendingRequestResolution::Answered {
        response: Some(JsonValue::Object(
            [("answers".to_owned(), JsonValue::Object(answer_map))]
                .into_iter()
                .collect(),
        )),
    }
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
            PendingRequestPayload::CLIRuntime {
                request: cli_request,
            },
        ) => {
            let resolution =
                cli_runtime_resolution_from_pending_resolution(cli_request, resolution)?;
            Ok(PendingRequestResponseAction::CLIRuntime {
                method: pioneer_protocol::constants::methods::CLI_RUNTIME_REQUEST_RESPOND
                    .to_owned(),
                params: CLIRuntimeRequestRespondParams {
                    workspace_id: request.workspace_id.clone(),
                    runtime_id: runtime_id.clone(),
                    request_id: request.request_id.clone(),
                    resolution,
                },
            })
        }
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
    request: &CLIRuntimePendingRequest,
    resolution: PendingRequestResolution,
) -> Result<CLIRuntimeRequestResolution, PendingRequestResponsePlanError> {
    match request.kind {
        CLIRuntimeRequestKind::CommandApproval
        | CLIRuntimeRequestKind::FileChangeApproval
        | CLIRuntimeRequestKind::PermissionApproval => match resolution {
            PendingRequestResolution::Allow => Ok(CLIRuntimeRequestResolution::Approved),
            PendingRequestResolution::AllowForSession
                if request
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.get("supportsSessionApproval"))
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false) =>
            {
                Ok(CLIRuntimeRequestResolution::ApprovedForSession)
            }
            PendingRequestResolution::Deny { reason } => {
                Ok(CLIRuntimeRequestResolution::Denied { reason })
            }
            PendingRequestResolution::Cancel => Ok(CLIRuntimeRequestResolution::Cancelled),
            PendingRequestResolution::Expired => Ok(CLIRuntimeRequestResolution::Expired),
            PendingRequestResolution::AllowForTurn
            | PendingRequestResolution::AllowForSession
            | PendingRequestResolution::Answered { .. } => {
                Err(PendingRequestResponsePlanError::UnsupportedResolutionForOrigin)
            }
        },
        CLIRuntimeRequestKind::UserInput => match resolution {
            PendingRequestResolution::Answered { response } => {
                Ok(CLIRuntimeRequestResolution::Answered { response })
            }
            PendingRequestResolution::Cancel => Ok(CLIRuntimeRequestResolution::Cancelled),
            PendingRequestResolution::Expired => Ok(CLIRuntimeRequestResolution::Expired),
            PendingRequestResolution::Allow
            | PendingRequestResolution::AllowForTurn
            | PendingRequestResolution::AllowForSession
            | PendingRequestResolution::Deny { .. } => {
                Err(PendingRequestResponsePlanError::UnsupportedResolutionForOrigin)
            }
        },
        CLIRuntimeRequestKind::Other => match resolution {
            PendingRequestResolution::Allow => Ok(CLIRuntimeRequestResolution::Approved),
            PendingRequestResolution::Deny { reason } => {
                Ok(CLIRuntimeRequestResolution::Denied { reason })
            }
            PendingRequestResolution::Cancel => Ok(CLIRuntimeRequestResolution::Cancelled),
            PendingRequestResolution::Expired => Ok(CLIRuntimeRequestResolution::Expired),
            PendingRequestResolution::AllowForTurn
            | PendingRequestResolution::AllowForSession
            | PendingRequestResolution::Answered { .. } => {
                Err(PendingRequestResponsePlanError::UnsupportedResolutionForOrigin)
            }
        },
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
pub struct PendingRequestRegistry {
    requests: Vec<PendingRequest>,
    by_id: std::collections::HashMap<String, usize>,
}

/// Compatibility name for pure request reductions; runtime ownership belongs to ClientCore.
pub type PendingRequestState = PendingRequestRegistry;

impl PendingRequestRegistry {
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
        self.by_id
            .get(request_id)
            .map(|index| &self.requests[*index])
    }

    pub fn apply<R>(&mut self, reduction: R) -> bool
    where
        R: Into<PendingRequestsReduction>,
    {
        let changed = match reduction.into() {
            PendingRequestsReduction::Opened(request) => {
                if let Some(index) = self.by_id.get(&request.request_id).copied() {
                    let existing = &mut self.requests[index];
                    if existing.workspace_id != request.workspace_id {
                        return false;
                    }
                    let changed = existing != &request;
                    *existing = request;
                    return changed;
                }

                self.by_id
                    .insert(request.request_id.clone(), self.requests.len());
                self.requests.push(request);
                true
            }
            PendingRequestsReduction::Resolved { request_id } => {
                remove_matching(&mut self.requests, |request| {
                    request.request_id == request_id
                })
            }
            PendingRequestsReduction::ResolvedInWorkspace {
                workspace_id,
                request_id,
            } => remove_matching(&mut self.requests, |request| {
                request.workspace_id == workspace_id && request.request_id == request_id
            }),
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
        };
        if changed {
            self.by_id = self
                .requests
                .iter()
                .enumerate()
                .map(|(index, request)| (request.request_id.clone(), index))
                .collect();
        }
        changed
    }
}

fn pending_request_matches_scope(
    request: &PendingRequest,
    workspace_id: Option<&str>,
    thread_id: Option<&str>,
) -> bool {
    workspace_id.map_or(true, |workspace_id| request.workspace_id == workspace_id)
        && match thread_id {
            Some(thread_id) => {
                request.thread_id.as_deref() == Some(thread_id)
                    || pending_request_visible_thread_ids(request)
                        .iter()
                        .any(|visible_thread_id| visible_thread_id == thread_id)
            }
            None => request.thread_id.is_none(),
        }
}

fn pending_request_visible_thread_ids(request: &PendingRequest) -> &[String] {
    request.visible_thread_ids.as_slice()
}

#[derive(Clone, Debug, PartialEq)]
pub struct CLIRuntimePendingRequestEntry {
    pub workspace_id: String,
    pub runtime_id: String,
    pub request_id: String,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub visible_thread_ids: Vec<String>,
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
            visible_thread_ids: notification.visible_thread_ids,
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
            visible_thread_ids: request.visible_thread_ids.clone(),
            request: cli_request.clone(),
        })
    }

    pub fn to_pending_request(&self) -> PendingRequest {
        if let Some(request) = self.native_permission_request() {
            return PendingRequest::from_native_permission_request(request);
        }

        PendingRequest {
            workspace_id: self.workspace_id.clone(),
            request_id: self.request_id.clone(),
            thread_id: self.thread_id.clone(),
            turn_id: self.turn_id.clone(),
            item_id: self.item_id.clone(),
            visible_thread_ids: self.visible_thread_ids.clone(),
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
        if let Some(request) = self.native_permission_request() {
            return PendingRequest::from_native_permission_request(request);
        }

        PendingRequest {
            workspace_id: self.workspace_id,
            request_id: self.request_id,
            thread_id: self.thread_id,
            turn_id: self.turn_id,
            item_id: self.item_id,
            visible_thread_ids: self.visible_thread_ids,
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

    fn native_permission_request(&self) -> Option<TurnPermissionApprovalRequest> {
        if self.runtime_id != pioneer_protocol::constants::runtime_ids::NATIVE_PERMISSION
            || self.request.kind != CLIRuntimeRequestKind::Other
            || self.request.native_request_id.as_deref() != Some(self.request_id.as_str())
        {
            return None;
        }

        let request =
            serde_json::from_value::<TurnPermissionApprovalRequest>(self.request.payload.clone()?)
                .ok()?;
        (request.request_id == self.request_id && request.workspace_id == self.workspace_id)
            .then_some(request)
    }
}

pub fn pending_request_detail_rows(request: &PendingRequest) -> Vec<PendingRequestDetailRow> {
    if let Some(entry) = CLIRuntimePendingRequestEntry::from_pending_request(request) {
        return match entry.request.kind {
            CLIRuntimeRequestKind::CommandApproval => {
                cli_runtime_command_detail_rows(entry.request.payload.as_ref())
            }
            CLIRuntimeRequestKind::FileChangeApproval => {
                cli_runtime_file_change_detail_rows(entry.request.payload.as_ref())
            }
            CLIRuntimeRequestKind::PermissionApproval => {
                cli_runtime_permission_detail_rows(entry.request.payload.as_ref())
            }
            CLIRuntimeRequestKind::UserInput | CLIRuntimeRequestKind::Other => Vec::new(),
        };
    }

    match &request.payload {
        PendingRequestPayload::NativePermissionGate { request } => {
            native_permission_detail_rows(request)
        }
        PendingRequestPayload::CLIRuntime { .. } | PendingRequestPayload::Other { .. } => {
            Vec::new()
        }
    }
}

pub fn pending_request_user_input_questions(
    request: &PendingRequest,
) -> Vec<PendingRequestUserInputQuestion> {
    let Some(entry) = CLIRuntimePendingRequestEntry::from_pending_request(request) else {
        return Vec::new();
    };
    if entry.request.kind != CLIRuntimeRequestKind::UserInput {
        return Vec::new();
    }

    cli_runtime_user_input_questions(entry.request.payload.as_ref())
}

pub fn pending_request_origin_label(origin: &PendingRequestOrigin) -> &'static str {
    match origin {
        PendingRequestOrigin::CLIRuntime { .. } => "CLI provider request",
        PendingRequestOrigin::NativePermissionGate => "Native agent request",
    }
}

pub fn pending_request_kind_label(kind: PendingRequestKind) -> &'static str {
    match kind {
        PendingRequestKind::CommandApproval => "Command approval",
        PendingRequestKind::FileChangeApproval => "File change approval",
        PendingRequestKind::PermissionApproval => "Permission approval",
        PendingRequestKind::UserInput => "Input requested",
        PendingRequestKind::Other => "Pending request",
    }
}

fn cli_runtime_command_detail_rows(payload: Option<&JsonValue>) -> Vec<PendingRequestDetailRow> {
    let command = payload
        .and_then(|payload| string_field(payload, &["command"]))
        .or_else(|| payload.and_then(command_from_argv));
    let cwd = payload.and_then(|payload| string_field(payload, &["cwd"]));
    let reason = payload.and_then(|payload| string_field(payload, &["reason"]));

    let mut rows = Vec::new();
    if let Some(command) = command {
        rows.push(detail_row("Command", command, true));
    }
    if let Some(cwd) = cwd {
        rows.push(detail_row("Directory", cwd, true));
    }
    if let Some(reason) = reason {
        rows.push(detail_row("Reason", reason, false));
    }
    rows
}

fn cli_runtime_file_change_detail_rows(
    payload: Option<&JsonValue>,
) -> Vec<PendingRequestDetailRow> {
    let mut rows = Vec::new();

    if let Some(grant_root) = payload.and_then(|payload| string_field(payload, &["grantRoot"])) {
        rows.push(detail_row("Root", grant_root, true));
    }

    if let Some(files) = payload.and_then(|payload| string_array_field(payload, "changedFiles"))
        && !files.is_empty()
    {
        rows.push(detail_row("Files", files.join("\n"), true));
    }

    if let Some(reason) = payload.and_then(|payload| string_field(payload, &["reason"])) {
        rows.push(detail_row("Reason", reason, false));
    }

    if let Some(diff_preview) = payload.and_then(diff_preview_text) {
        rows.push(PendingRequestDetailRow {
            label: "Diff".to_owned(),
            value: diff_preview,
            monospace: true,
            style: PendingRequestDetailStyle::Diff,
        });
    }

    rows
}

fn cli_runtime_permission_detail_rows(payload: Option<&JsonValue>) -> Vec<PendingRequestDetailRow> {
    let mut rows = Vec::new();
    if let Some(cwd) = payload.and_then(|payload| string_field(payload, &["cwd"])) {
        rows.push(detail_row("Directory", cwd, true));
    }
    if let Some(permissions) = payload.and_then(|payload| payload.get("permissions")) {
        rows.push(detail_row(
            "Requested permissions",
            serde_json::to_string_pretty(permissions).unwrap_or_else(|_| permissions.to_string()),
            true,
        ));
    }
    if let Some(reason) = payload.and_then(|payload| string_field(payload, &["reason"])) {
        rows.push(detail_row("Reason", reason, false));
    }
    rows
}

fn native_permission_detail_rows(
    request: &TurnPermissionApprovalRequest,
) -> Vec<PendingRequestDetailRow> {
    let mut rows = vec![
        detail_row("Tool", request.tool_name.clone(), true),
        detail_row("Action", request.action.as_str().to_owned(), true),
    ];
    rows.extend(
        request
            .details
            .iter()
            .map(|detail| PendingRequestDetailRow {
                label: detail.label.clone(),
                value: detail.value.clone(),
                monospace: detail.monospace,
                style: PendingRequestDetailStyle::Field,
            }),
    );
    rows.push(detail_row("Scope", request.scope_hash.clone(), true));
    rows.push(detail_row(
        "Reason",
        request.reason.as_str().to_owned(),
        false,
    ));
    if let Some(summary) = request.summary.clone() {
        rows.push(detail_row("Summary", summary, false));
    }
    rows
}

fn detail_row(label: impl Into<String>, value: String, monospace: bool) -> PendingRequestDetailRow {
    PendingRequestDetailRow {
        label: label.into(),
        value,
        monospace,
        style: PendingRequestDetailStyle::Field,
    }
}

fn string_field(payload: &JsonValue, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| payload.get(*key).and_then(JsonValue::as_str))
        .map(str::to_owned)
        .filter(|value| !value.trim().is_empty())
}

fn command_from_argv(payload: &JsonValue) -> Option<String> {
    payload
        .get("argv")
        .and_then(JsonValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(JsonValue::as_str)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.trim().is_empty())
}

fn string_array_field(payload: &JsonValue, key: &str) -> Option<Vec<String>> {
    payload.get(key).and_then(JsonValue::as_array).map(|items| {
        items
            .iter()
            .filter_map(JsonValue::as_str)
            .map(str::to_owned)
            .filter(|value| !value.trim().is_empty())
            .collect::<Vec<_>>()
    })
}

fn diff_preview_text(payload: &JsonValue) -> Option<String> {
    payload
        .get("diffPreview")
        .and_then(|preview| preview.get("text"))
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .filter(|value| !value.trim().is_empty())
}

fn cli_runtime_user_input_questions(
    payload: Option<&JsonValue>,
) -> Vec<PendingRequestUserInputQuestion> {
    let Some(payload) = payload else {
        return Vec::new();
    };
    let Some(questions) = payload.get("questions").and_then(JsonValue::as_array) else {
        return Vec::new();
    };

    questions
        .iter()
        .enumerate()
        .map(|(ix, value)| {
            let id = value
                .get("id")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
                .filter(|id| !id.trim().is_empty())
                .unwrap_or_else(|| format!("question_{}", ix + 1));
            let header = value
                .get("header")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
                .filter(|header| !header.trim().is_empty());
            let question = value
                .get("question")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
                .filter(|question| !question.trim().is_empty())
                .or_else(|| header.clone())
                .unwrap_or_else(|| id.clone());
            let options = value
                .get("options")
                .and_then(JsonValue::as_array)
                .into_iter()
                .flatten()
                .filter_map(|option| {
                    let label = option
                        .get("label")
                        .and_then(JsonValue::as_str)
                        .map(str::to_owned)
                        .filter(|label| !label.trim().is_empty())?;
                    let description = option
                        .get("description")
                        .and_then(JsonValue::as_str)
                        .map(str::to_owned)
                        .filter(|description| !description.trim().is_empty());
                    Some(PendingRequestUserInputOption { label, description })
                })
                .collect();
            let is_secret = value
                .get("isSecret")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false);

            PendingRequestUserInputQuestion {
                id,
                header,
                question,
                options,
                is_secret,
            }
        })
        .collect()
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
    PendingRequestsReduction::ResolvedInWorkspace {
        workspace_id: notification.workspace_id,
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
    PendingRequestsReduction::ResolvedInWorkspace {
        workspace_id: notification.workspace_id,
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
            visible_thread_ids: Vec::new(),
            request: CLIRuntimePendingRequest {
                kind: CLIRuntimeRequestKind::CommandApproval,
                title: Some(format!("Run {command}")),
                message: Some(command.to_owned()),
                native_request_id: Some(format!("native_{request_id}")),
                payload: None,
            },
        }
    }

    fn session_approvable_request_opened(
        request_id: &str,
        workspace_id: &str,
        runtime_id: &str,
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        command: &str,
    ) -> CLIRuntimeRequestOpenedNotification {
        let mut opened = request_opened(
            request_id,
            workspace_id,
            runtime_id,
            thread_id,
            turn_id,
            command,
        );
        opened.request.payload = Some(serde_json::json!({
            "supportsSessionApproval": true,
            "command": command,
        }));
        opened
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
            visible_thread_ids: Vec::new(),
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

    fn user_input_request_opened(
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
            item_id: Some("item_input".to_owned()),
            visible_thread_ids: Vec::new(),
            request: CLIRuntimePendingRequest {
                kind: CLIRuntimeRequestKind::UserInput,
                title: Some("Input requested".to_owned()),
                message: Some("Pick a target".to_owned()),
                native_request_id: Some(format!("native_{request_id}")),
                payload: Some(serde_json::json!({
                    "questions": [
                        {
                            "id": "target",
                            "header": "Target",
                            "question": "Pick a target",
                            "options": [
                                {
                                    "label": "A",
                                    "description": "Use target A"
                                }
                            ],
                            "isSecret": true
                        }
                    ]
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
            visible_thread_ids: Vec::new(),
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
        assert_eq!(pending.kind, PendingRequestKind::PermissionApproval);
        assert_eq!(pending.title.as_deref(), Some("exec_command"));
        assert_eq!(pending.message.as_deref(), Some("Approve command"));
        assert_eq!(pending.native_request_id.as_deref(), Some("req_native"));
        assert!(matches!(
            pending.payload,
            PendingRequestPayload::NativePermissionGate { .. }
        ));
    }

    #[test]
    fn durable_native_permission_envelope_uses_native_response_route() {
        let mut native = native_permission_request("req_native");
        native.thread_id = "child_thread".to_owned();
        native.visible_thread_ids = vec!["parent_thread".to_owned()];
        let pending = CLIRuntimePendingRequestEntry {
            workspace_id: "ws".to_owned(),
            runtime_id: pioneer_protocol::constants::runtime_ids::NATIVE_PERMISSION.to_owned(),
            request_id: "req_native".to_owned(),
            thread_id: Some("child_thread".to_owned()),
            turn_id: Some("turn".to_owned()),
            item_id: None,
            visible_thread_ids: vec!["parent_thread".to_owned()],
            request: CLIRuntimePendingRequest {
                kind: CLIRuntimeRequestKind::Other,
                title: Some("Tool permission requested".to_owned()),
                message: Some("generic durable envelope".to_owned()),
                native_request_id: Some("req_native".to_owned()),
                payload: Some(serde_json::to_value(native).expect("serialize native request")),
            },
        }
        .into_pending_request();

        assert_eq!(pending.origin, PendingRequestOrigin::NativePermissionGate);
        assert_eq!(pending.thread_id.as_deref(), Some("child_thread"));
        assert_eq!(
            plan_pending_request_response(&pending, PendingRequestResolution::Allow)
                .expect("plan native response")
                .method(),
            pioneer_protocol::constants::methods::TURN_PERMISSION_REQUEST_RESPOND
        );
    }

    #[test]
    fn native_permission_request_is_visible_in_every_ancestor_thread_scope() {
        let mut request = native_permission_request("req_native");
        request.thread_id = "grandchild_thread".to_owned();
        request.visible_thread_ids = vec!["child_thread".to_owned(), "root_thread".to_owned()];
        let pending = PendingRequest::from_native_permission_request(request);
        let mut state = PendingRequestState::default();

        state.apply(PendingRequestsReduction::Opened(pending.clone()));

        assert_eq!(
            state.pending_for_scope(Some("ws"), Some("grandchild_thread")),
            vec![pending.clone()]
        );
        assert_eq!(
            state.pending_for_scope(Some("ws"), Some("child_thread")),
            vec![pending.clone()]
        );
        assert_eq!(
            state.pending_for_scope(Some("ws"), Some("root_thread")),
            vec![pending]
        );
        assert!(
            state
                .pending_for_scope(Some("ws"), Some("other_thread"))
                .is_empty()
        );

        state.apply(PendingRequestsReduction::Resolved {
            request_id: "req_native".to_owned(),
        });

        assert!(
            state
                .pending_for_scope(Some("ws"), Some("root_thread"))
                .is_empty()
        );
        assert!(
            state
                .pending_for_scope(Some("ws"), Some("child_thread"))
                .is_empty()
        );
    }

    #[test]
    fn cli_runtime_request_is_visible_in_every_projected_ancestor_thread_scope() {
        let mut opened = request_opened(
            "req_cli",
            "ws",
            "codex",
            Some("grandchild_thread"),
            Some("turn"),
            "cargo check",
        );
        opened.visible_thread_ids = vec!["child_thread".to_owned(), "root_thread".to_owned()];
        let pending = PendingRequest::from_cli_runtime_opened_notification(opened);
        let mut state = PendingRequestState::default();

        state.apply(PendingRequestsReduction::Opened(pending.clone()));

        assert_eq!(
            state.pending_for_scope(Some("ws"), Some("grandchild_thread")),
            vec![pending.clone()]
        );
        assert_eq!(
            state.pending_for_scope(Some("ws"), Some("child_thread")),
            vec![pending.clone()]
        );
        assert_eq!(
            state.pending_for_scope(Some("ws"), Some("root_thread")),
            vec![pending]
        );
        assert!(
            state
                .pending_for_scope(Some("ws"), Some("other_thread"))
                .is_empty()
        );
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

        let action = plan_pending_request_response(&pending, PendingRequestResolution::Allow)
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
        assert_eq!(params.resolution, CLIRuntimeRequestResolution::Approved);
    }

    #[test]
    fn response_planner_rejects_cli_runtime_session_scope() {
        let pending = PendingRequest::from_cli_runtime_opened_notification(request_opened(
            "req",
            "ws",
            "codex",
            Some("thread"),
            Some("turn"),
            "pwd",
        ));

        let error =
            plan_pending_request_response(&pending, PendingRequestResolution::AllowForSession)
                .expect_err("CLI session scope exceeds the canonical approval contract");
        assert_eq!(
            error,
            PendingRequestResponsePlanError::UnsupportedResolutionForOrigin
        );
    }

    #[test]
    fn response_planner_routes_eligible_codex_session_scope_as_typed_resolution() {
        let pending = PendingRequest::from_cli_runtime_opened_notification(
            session_approvable_request_opened(
                "req",
                "ws",
                "codex",
                Some("thread"),
                Some("turn"),
                "pwd",
            ),
        );

        let action =
            plan_pending_request_response(&pending, PendingRequestResolution::AllowForSession)
                .expect("eligible Codex approval should expose session scope");
        let PendingRequestResponseAction::CLIRuntime { params, .. } = action else {
            panic!("expected CLI runtime action");
        };
        assert_eq!(
            params.resolution,
            CLIRuntimeRequestResolution::ApprovedForSession
        );
    }

    #[test]
    fn response_planner_routes_cli_runtime_denial_to_cli_rpc_params() {
        let pending = PendingRequest::from_cli_runtime_opened_notification(request_opened(
            "req",
            "ws",
            "claude",
            Some("thread"),
            Some("turn"),
            "cargo check",
        ));

        let action = plan_pending_request_response(
            &pending,
            PendingRequestResolution::Deny {
                reason: Some("unsafe command".to_owned()),
            },
        )
        .expect("plan CLI denial response");

        let PendingRequestResponseAction::CLIRuntime { method, params } = action else {
            panic!("expected CLI runtime action");
        };
        assert_eq!(
            method,
            pioneer_protocol::constants::methods::CLI_RUNTIME_REQUEST_RESPOND
        );
        assert_eq!(params.workspace_id, "ws");
        assert_eq!(params.runtime_id, "claude");
        assert_eq!(params.request_id, "req");
        assert_eq!(
            params.resolution,
            CLIRuntimeRequestResolution::Denied {
                reason: Some("unsafe command".to_owned())
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
    fn response_planner_rejects_turn_resolution_for_cli_runtime_request() {
        let pending = PendingRequest::from_cli_runtime_opened_notification(request_opened(
            "req",
            "ws",
            "codex",
            Some("thread"),
            Some("turn"),
            "pwd",
        ));

        let error = plan_pending_request_response(&pending, PendingRequestResolution::AllowForTurn)
            .expect_err("CLI runtime approvals do not use native turn scope");

        assert_eq!(
            error,
            PendingRequestResponsePlanError::UnsupportedResolutionForOrigin
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
    fn pending_request_actions_are_scoped_by_origin_and_kind() {
        let cli_command = PendingRequest::from_cli_runtime_opened_notification(request_opened(
            "req",
            "ws",
            "codex",
            Some("thread"),
            Some("turn"),
            "pwd",
        ));
        let native =
            PendingRequest::from_native_permission_request(native_permission_request("req_native"));
        let cli_session_command = PendingRequest::from_cli_runtime_opened_notification(
            session_approvable_request_opened(
                "req_session",
                "ws",
                "codex",
                Some("thread"),
                Some("turn"),
                "pwd",
            ),
        );
        let user_input = PendingRequest::from_cli_runtime_opened_notification(
            user_input_request_opened("req_input", "ws", "codex", Some("thread"), Some("turn")),
        );

        assert_eq!(
            pending_request_available_actions(&cli_command)
                .into_iter()
                .map(|action| action.kind)
                .collect::<Vec<_>>(),
            vec![
                PendingRequestActionKind::CancelTurn,
                PendingRequestActionKind::Deny,
                PendingRequestActionKind::Allow,
            ]
        );
        assert_eq!(
            pending_request_available_actions(&cli_session_command)
                .into_iter()
                .map(|action| action.kind)
                .collect::<Vec<_>>(),
            vec![
                PendingRequestActionKind::CancelTurn,
                PendingRequestActionKind::Deny,
                PendingRequestActionKind::AllowForSession,
                PendingRequestActionKind::Allow,
            ]
        );
        assert_eq!(
            pending_request_available_actions(&native)
                .into_iter()
                .map(|action| action.kind)
                .collect::<Vec<_>>(),
            vec![
                PendingRequestActionKind::CancelTurn,
                PendingRequestActionKind::Deny,
                PendingRequestActionKind::AllowForTurn,
                PendingRequestActionKind::Allow,
            ]
        );
        assert_eq!(
            pending_request_available_actions(&user_input)
                .into_iter()
                .map(|action| action.kind)
                .collect::<Vec<_>>(),
            vec![
                PendingRequestActionKind::CancelTurn,
                PendingRequestActionKind::Answer,
            ]
        );
    }

    #[test]
    fn allow_for_turn_action_is_exposed_only_for_native_permission_gate() {
        let cli_command = PendingRequest::from_cli_runtime_opened_notification(request_opened(
            "req",
            "ws",
            "codex",
            Some("thread"),
            Some("turn"),
            "pwd",
        ));
        let native =
            PendingRequest::from_native_permission_request(native_permission_request("req_native"));

        let cli_allow_for_turn = pending_request_available_actions(&cli_command)
            .into_iter()
            .find(|action| action.kind == PendingRequestActionKind::AllowForTurn);
        let native_allow_for_turn = pending_request_available_actions(&native)
            .into_iter()
            .find(|action| action.kind == PendingRequestActionKind::AllowForTurn)
            .expect("native approvals expose a scoped allow action");

        assert!(cli_allow_for_turn.is_none());
        assert_eq!(
            native_allow_for_turn.resolution,
            Some(PendingRequestResolution::AllowForTurn)
        );
    }

    #[test]
    fn pending_request_presentation_projects_details_and_questions() {
        let file_change = PendingRequest::from_cli_runtime_opened_notification(
            file_change_request_opened("req_file", "ws", "codex", Some("thread"), Some("turn")),
        );
        let file_presentation = present_pending_request(&file_change);

        assert_eq!(file_presentation.origin_label, "CLI provider request");
        assert_eq!(file_presentation.kind_label, "File change approval");
        assert!(
            file_presentation
                .details
                .iter()
                .any(|row| { row.label == "Files" && row.value == "src/main.rs" && row.monospace })
        );
        assert!(
            file_presentation
                .details
                .iter()
                .any(|row| { row.label == "Diff" && row.style == PendingRequestDetailStyle::Diff })
        );

        let user_input = PendingRequest::from_cli_runtime_opened_notification(
            user_input_request_opened("req_input", "ws", "codex", Some("thread"), Some("turn")),
        );
        let input_presentation = present_pending_request(&user_input);

        assert_eq!(input_presentation.user_input_questions.len(), 1);
        assert_eq!(input_presentation.user_input_questions[0].id, "target");
        assert_eq!(
            input_presentation.user_input_questions[0].options[0]
                .description
                .as_deref(),
            Some("Use target A")
        );
        assert!(input_presentation.user_input_questions[0].is_secret);
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
                    visible_thread_ids: Vec::new(),
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
                    visible_thread_ids: Vec::new(),
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
