use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Boundary {
    Package,
    Skills,
    Mcp,
    Skill,
    Server,
    Extension,
    HostEffect,
}

/// Messages are fixed host text. Neither external errors nor config values are
/// interpolated into diagnostics; pointers identify only package-relative data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: String,
    pub boundary: Boundary,
    pub pointer: String,
    pub message: String,
    pub retryable: bool,
}
impl Diagnostic {
    pub fn new(code: &str, boundary: Boundary, pointer: &str, message: &str) -> Self {
        Self {
            code: code.into(),
            boundary,
            pointer: pointer.chars().take(512).collect(),
            message: message.into(),
            retryable: false,
        }
    }
}

pub(crate) fn pointer_key(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}
