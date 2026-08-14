use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const PUBLIC_ERROR_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PublicErrorCode {
    InvalidInput,
    PolicyDenied,
    NotFound,
    Conflict,
    ResourceExhausted,
    Unavailable,
    Timeout,
    Internal,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PublicErrorStage {
    Discovery,
    Admission,
    Preparation,
    Execution,
    Persistence,
    Delivery,
    Observation,
}

/// Stable, bounded failure presentation shared by RPC, voice and task
/// execution surfaces. Raw source chains are never part of this type.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PublicError {
    pub version: u8,
    pub code: PublicErrorCode,
    pub stage: PublicErrorStage,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    pub correlation_id: String,
}
