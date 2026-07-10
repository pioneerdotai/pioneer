/// Runtime-authoritative state used by reconciliation. This contract is shared
/// by in-process and external runtimes so timeout policy never has to infer a
/// turn's lifecycle from an individual item's lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionTurnStatus {
    InProgress,
    Completed,
    Failed,
    Blocked,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionTurnObservation {
    pub status: ExecutionTurnStatus,
    pub message: Option<String>,
}
