//! Server-side, content-free execution statistics. Sequence frontiers freeze
//! immutable recorded revisions, not the state of a running external service.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionObservationView {
    Summary,
    Timeline,
    Contexts,
    Attempt,
    Export,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionObservabilityParams {
    pub thread_id: String,
    pub view: ExecutionObservationView,
    pub turn_id: Option<String>,
    pub task_id: Option<String>,
    pub execution_id: Option<String>,
    pub operation_id: Option<String>,
    pub parent_operation_id: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub purpose: Option<String>,
    /// Half-open source start wall-time range; untimed facts are excluded and counted separately.
    pub from_wall_ms: Option<i64>,
    pub before_wall_ms: Option<i64>,
    pub include_descendants: Option<bool>,
    pub include_revisions: Option<bool>,
    pub after: Option<i64>,
    pub through: Option<i64>,
    pub limit: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionObservationRecord {
    pub lane: String,
    pub causal_links: Vec<String>,
    pub sequence: i64,
    pub operation_id: String,
    pub revision: i64,
    pub source_revision: i64,
    pub resolution_origin: String,
    pub turn_id: Option<String>,
    pub root_thread_id: Option<String>,
    pub task_id: Option<String>,
    pub task_run_id: Option<String>,
    pub task_execution_id: Option<String>,
    pub execution_id: Option<String>,
    pub owner_kind: String,
    pub owner_id: String,
    pub parent_operation_id: Option<String>,
    pub kind: String,
    pub purpose: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub status: String,
    pub recorded_at_ms: i64,
    pub tariff_hash: Option<String>,
    pub facts: Value,
    pub timing: Value,
    pub window_id: Option<String>,
    pub tariff_evidence: Option<Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionObservabilityResponse {
    pub workspace_id: String,
    pub thread_id: String,
    pub through: i64,
    pub next_after: Option<i64>,
    pub records: Vec<ExecutionObservationRecord>,
    pub summary: Option<Value>,
    pub owners: Vec<Value>,
    pub coverage: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionReconcileParams {
    pub thread_id: String,
    pub operation_id: String,
    pub expected_sequence: i64,
}

/// Workspace-owned work only. Thread data requires its separate ThreadRead proof.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceExecutionObservabilityParams {
    pub workspace_id: String,
    pub view: ExecutionObservationView,
    pub operation_id: Option<String>,
    pub parent_operation_id: Option<String>,
    pub after: Option<i64>,
    pub through: Option<i64>,
    pub limit: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceExecutionObservabilityResponse {
    pub workspace_id: String,
    pub through: i64,
    pub next_after: Option<i64>,
    pub records: Vec<ExecutionObservationRecord>,
    pub summary: Option<Value>,
    pub coverage: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceExecutionReconcileParams {
    pub workspace_id: String,
    pub operation_id: String,
    pub expected_sequence: i64,
}
