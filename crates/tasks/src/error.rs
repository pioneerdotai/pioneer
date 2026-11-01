pub type TaskRuntimeError = anyhow::Error;
pub type TaskRuntimeResult<T> = anyhow::Result<T>;

/// Safe domain classification; diagnostic context may contain private details
/// and must not be forwarded to model-facing tools.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOperationError {
    StateConflict,
    AccessDenied,
}

impl std::fmt::Display for TaskOperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::StateConflict => "task_state_conflict",
            Self::AccessDenied => "task_access_denied",
        })
    }
}

impl std::error::Error for TaskOperationError {}

/// Independent startup work and owned workers started, but known row-local
/// discovery failures remain. Diagnostics are private and must not reach tools.
#[derive(Debug)]
pub struct PartialTaskStartup(pub(crate) Vec<anyhow::Error>);
impl std::fmt::Display for PartialTaskStartup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "partial Task startup: {:?}", self.0)
    }
}
impl std::error::Error for PartialTaskStartup {}
