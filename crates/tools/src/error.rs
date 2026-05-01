use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub enum ToolError {
    InvalidArguments(String),
    NotFound(String),
    Rejected(String),
    Cancelled(String),
    ExecutionFailed(String),
    Internal(String),
}

impl ToolError {
    pub fn invalid_arguments(message: impl Into<String>) -> Self {
        Self::InvalidArguments(message.into())
    }

    pub fn execution_failed(message: impl Into<String>) -> Self {
        Self::ExecutionFailed(message.into())
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::Cancelled(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

impl Display for ToolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidArguments(message) => write!(f, "invalid arguments: {message}"),
            Self::NotFound(message) => write!(f, "tool not found: {message}"),
            Self::Rejected(message) => write!(f, "tool call rejected: {message}"),
            Self::Cancelled(message) => write!(f, "tool call cancelled: {message}"),
            Self::ExecutionFailed(message) => write!(f, "tool execution failed: {message}"),
            Self::Internal(message) => write!(f, "tool internal error: {message}"),
        }
    }
}

impl std::error::Error for ToolError {}
