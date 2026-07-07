use async_trait::async_trait;
use pioneer_protocol::{TurnPermissionApprovalRequestDetail, generate_id};
use pioneer_tools::{
    PermissionApprovalBroker, PermissionApprovalResolution, PermissionDecisionReason,
    PermissionEvaluationContext, PermissionIntent, PermissionRequestKey, ToolInvocation,
};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub struct GatewayPermissionApprovalRequest {
    pub request_id: String,
    pub workspace_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub tool_name: String,
    pub key: PermissionRequestKey,
    pub reason: PermissionDecisionReason,
    pub summary: Option<String>,
    pub details: Vec<TurnPermissionApprovalRequestDetail>,
    pub respond_to: oneshot::Sender<PermissionApprovalResolution>,
}

#[derive(Debug, Clone)]
pub struct GatewayPermissionApprovalBroker {
    request_tx: mpsc::UnboundedSender<GatewayPermissionApprovalEvent>,
}

#[derive(Debug)]
pub enum GatewayPermissionApprovalEvent {
    Open(GatewayPermissionApprovalRequest),
    Cancelled { request_id: String },
}

impl GatewayPermissionApprovalBroker {
    pub fn channel() -> (
        Self,
        mpsc::UnboundedReceiver<GatewayPermissionApprovalEvent>,
    ) {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        (Self { request_tx }, request_rx)
    }
}

#[async_trait]
impl PermissionApprovalBroker for GatewayPermissionApprovalBroker {
    async fn request_approval(
        &self,
        context: &PermissionEvaluationContext,
        invocation: &ToolInvocation,
        intent: &PermissionIntent,
        key: &PermissionRequestKey,
        reason: PermissionDecisionReason,
    ) -> PermissionApprovalResolution {
        let request_id = generate_id(21);
        let (respond_tx, respond_rx) = oneshot::channel();
        let request = GatewayPermissionApprovalRequest {
            request_id: request_id.clone(),
            workspace_id: context.workspace_id.clone(),
            thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
            tool_name: invocation.tool_name.clone(),
            key: key.clone(),
            reason,
            summary: intent.summary.clone(),
            details: permission_request_details(intent),
            respond_to: respond_tx,
        };

        if self
            .request_tx
            .send(GatewayPermissionApprovalEvent::Open(request))
            .is_err()
        {
            return PermissionApprovalResolution::Deny {
                message: "permission approval broker is not connected".to_owned(),
            };
        }

        tokio::select! {
            _ = invocation.cancellation.cancelled() => {
                let _ = self.request_tx.send(GatewayPermissionApprovalEvent::Cancelled {
                    request_id,
                });
                PermissionApprovalResolution::Cancelled
            }
            resolution = respond_rx => resolution.unwrap_or(PermissionApprovalResolution::Expired),
        }
    }
}

fn permission_request_details(
    intent: &PermissionIntent,
) -> Vec<TurnPermissionApprovalRequestDetail> {
    const DETAIL_KEYS: &[(&str, &str, bool)] = &[
        ("operation", "Operation", false),
        ("command", "Command", true),
        ("argv", "Arguments", true),
        ("cwd", "Directory", true),
        ("env_keys", "Environment keys", true),
        ("timeout_ms", "Timeout (ms)", false),
        ("tty", "TTY", false),
        ("path", "Path", true),
        ("changed_paths", "Changed paths", true),
        ("grant_access", "Grant access", false),
        ("grant_root", "Grant root", true),
        ("grant_roots", "Grant roots", true),
        ("network_mode", "Network mode", false),
        ("network_host", "Network host", true),
        ("network_hosts", "Network hosts", true),
        ("operations", "Patch operations", false),
        ("method", "Method", false),
        ("domain", "Domain", true),
        ("url_origin", "URL origin", true),
        ("url_path", "URL path", true),
        ("destination_hint", "Destination", false),
        ("destination", "Download destination", true),
        ("server", "MCP server", true),
        ("tool", "MCP tool", true),
        ("mcp_safety", "MCP safety", false),
        ("dynamic_skill_kind", "Dynamic skill kind", false),
        ("skill_slug", "Skill", true),
        ("source_kind", "Skill source", false),
        ("trust_level", "Skill trust", false),
        ("target_tool", "Target tool", true),
        ("action", "Action", false),
        ("session_id", "Session", false),
        ("stdin_chars_present", "Stdin chars present", false),
        ("stdin_bytes", "Stdin bytes", false),
    ];

    let mut details = Vec::new();
    for (key, label, monospace) in DETAIL_KEYS {
        push_permission_request_detail(
            &mut details,
            label,
            intent.scope.entries.get(*key),
            *monospace,
        );
    }
    for index in 0..20 {
        let key = format!("path.{index}");
        push_permission_request_detail(
            &mut details,
            "Changed path",
            intent.scope.entries.get(key.as_str()),
            true,
        );
    }
    details
}

fn push_permission_request_detail(
    details: &mut Vec<TurnPermissionApprovalRequestDetail>,
    label: &str,
    value: Option<&String>,
    monospace: bool,
) {
    let Some(value) = value
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    details.push(TurnPermissionApprovalRequestDetail {
        label: label.to_owned(),
        value: value.to_owned(),
        monospace,
    });
}
