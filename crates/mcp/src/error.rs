use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpValidationDiagnostic {
    pub code: String,
    pub level: McpDiagnosticLevel,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_path: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpDiagnosticLevel {
    Error,
    Warning,
}

impl McpValidationDiagnostic {
    pub fn error(
        code: impl Into<String>,
        message: impl Into<String>,
        field_path: impl Into<Option<String>>,
    ) -> Self {
        Self {
            code: code.into(),
            level: McpDiagnosticLevel::Error,
            message: message.into(),
            field_path: field_path.into(),
        }
    }

    pub fn warning(
        code: impl Into<String>,
        message: impl Into<String>,
        field_path: impl Into<Option<String>>,
    ) -> Self {
        Self {
            code: code.into(),
            level: McpDiagnosticLevel::Warning,
            message: message.into(),
            field_path: field_path.into(),
        }
    }

    pub fn is_error(&self) -> bool {
        self.level == McpDiagnosticLevel::Error
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpConfigDocumentError {
    pub diagnostic: McpValidationDiagnostic,
}

impl McpConfigDocumentError {
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        field_path: impl Into<Option<String>>,
    ) -> Self {
        Self {
            diagnostic: McpValidationDiagnostic::error(code, message, field_path),
        }
    }
}

impl fmt::Display for McpConfigDocumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {}",
            self.diagnostic.code, self.diagnostic.message
        )
    }
}

impl Error for McpConfigDocumentError {}
