use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ThreadCallParams {
    pub workspace_id: String,
    pub thread_id: String,
    pub action: ThreadCallAction,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ThreadCallAction {
    Status {
        call_id: Option<String>,
    },
    History {
        before_call_id: Option<String>,
    },
    Start {
        request_key: String,
    },
    Change {
        call_id: String,
        revision: i64,
        transition: ThreadCallTransition,
    },
    KeepHeld {
        call_id: String,
    },
    Connect {
        call_id: String,
    },
    Restore {
        call_id: String,
        media_identity: String,
    },
    Disconnect {
        call_id: String,
        media_identity: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resume_key: Option<String>,
    },
    Resume {
        call_id: String,
        media_identity: String,
        resume_key: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ThreadCallTransition {
    Join,
    Decline,
    Leave,
    Hold,
    Resume,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ThreadCallPhase {
    Ringing,
    Active,
    Ended,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ThreadCallEndReason {
    Left,
    Declined,
    Missed,
    GatewayRestarted,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ThreadCallParticipant {
    pub principal_id: String,
    pub media_identity: String,
    pub held: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ThreadCallSnapshot {
    pub workspace_id: String,
    pub thread_id: String,
    pub call_id: String,
    pub revision: i64,
    pub created_at_unix: i64,
    pub phase: ThreadCallPhase,
    pub initiator_id: String,
    pub invitee_id: Option<String>,
    pub ring_until_unix: Option<i64>,
    pub end_reason: Option<ThreadCallEndReason>,
    pub participants: Vec<ThreadCallParticipant>,
}

#[derive(Deserialize, Serialize, JsonSchema)]
pub struct ThreadCallCredentials {
    pub url: String,
    pub token: String,
    pub expires_at_unix: i64,
}

#[derive(Deserialize, Serialize, JsonSchema)]
pub struct ThreadCallHistory {
    pub calls: Vec<ThreadCallSnapshot>,
    pub next_before_call_id: Option<String>,
}

#[derive(Deserialize, Serialize, JsonSchema)]
pub struct ThreadCallResponse {
    pub call: Option<ThreadCallSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials: Option<ThreadCallCredentials>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<ThreadCallHistory>,
}
