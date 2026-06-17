use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexInputMappingRequest {
    pub inputs: Vec<CodexInputSource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexInputSource {
    Text { text: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodexTurnInputMapping {
    pub input: Vec<CodexTurnInputItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<CodexInputMappingDiagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CodexTurnInputItem {
    Text { text: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodexInputMappingDiagnostic {
    pub level: CodexInputMappingDiagnosticLevel,
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_index: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CodexInputMappingDiagnosticLevel {
    Info,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexInputMappingError {
    diagnostics: Vec<CodexInputMappingDiagnostic>,
}

impl CodexInputMappingError {
    pub fn diagnostics(&self) -> &[CodexInputMappingDiagnostic] {
        self.diagnostics.as_slice()
    }
}

impl fmt::Display for CodexInputMappingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let messages = self
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.level == CodexInputMappingDiagnosticLevel::Error)
            .map(|diagnostic| diagnostic.message.as_str())
            .collect::<Vec<_>>();
        if messages.is_empty() {
            formatter.write_str("Codex input mapping failed")
        } else {
            write!(
                formatter,
                "Codex input mapping failed: {}",
                messages.join("; ")
            )
        }
    }
}

impl Error for CodexInputMappingError {}

pub fn map_codex_turn_input(
    request: CodexInputMappingRequest,
) -> Result<CodexTurnInputMapping, CodexInputMappingError> {
    let mut input = Vec::with_capacity(request.inputs.len());

    for item in request.inputs {
        match item {
            CodexInputSource::Text { text } => {
                input.push(CodexTurnInputItem::Text { text });
            }
        }
    }

    Ok(CodexTurnInputMapping {
        input,
        diagnostics: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_text_input() {
        let mapping = map_codex_turn_input(CodexInputMappingRequest {
            inputs: vec![CodexInputSource::Text {
                text: "hello".to_owned(),
            }],
        })
        .expect("text input should map");

        assert_eq!(
            mapping.input,
            vec![CodexTurnInputItem::Text {
                text: "hello".to_owned(),
            }]
        );
        assert!(mapping.diagnostics.is_empty());
    }
}
