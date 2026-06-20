use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexInputMappingRequest {
    pub inputs: Vec<CodexInputSource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexInputSource {
    Text {
        text: String,
    },
    ImageUrl {
        url: String,
    },
    LocalImage {
        path: String,
    },
    FileReference {
        location: CodexFileReferenceLocation,
        name: Option<String>,
        mime_type: Option<String>,
        size_bytes: Option<u64>,
        sha256: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexFileReferenceLocation {
    Path(String),
    Url(String),
    Reference(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodexTurnInputMapping {
    pub input: Vec<CodexTurnInputItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<CodexInputMappingDiagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum CodexTurnInputItem {
    Text { text: String },
    Image { url: String },
    LocalImage { path: String },
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
    let mut diagnostics = Vec::new();

    for (input_index, item) in request.inputs.into_iter().enumerate() {
        match item {
            CodexInputSource::Text { text } => {
                input.push(CodexTurnInputItem::Text { text });
            }
            CodexInputSource::ImageUrl { url } => {
                input.push(CodexTurnInputItem::Image { url });
                diagnostics.push(CodexInputMappingDiagnostic {
                    level: CodexInputMappingDiagnosticLevel::Info,
                    code: "codex_input.image_url_mapped".to_owned(),
                    message: "Mapped image URL input to native Codex image input.".to_owned(),
                    input_index: Some(input_index),
                });
            }
            CodexInputSource::LocalImage { path } => {
                input.push(CodexTurnInputItem::LocalImage { path });
                diagnostics.push(CodexInputMappingDiagnostic {
                    level: CodexInputMappingDiagnosticLevel::Info,
                    code: "codex_input.local_image_mapped".to_owned(),
                    message: "Mapped local image input to native Codex localImage input."
                        .to_owned(),
                    input_index: Some(input_index),
                });
            }
            CodexInputSource::FileReference {
                location,
                name,
                mime_type,
                size_bytes,
                sha256,
            } => {
                input.push(CodexTurnInputItem::Text {
                    text: render_file_reference_text(location, name, mime_type, size_bytes, sha256),
                });
                diagnostics.push(CodexInputMappingDiagnostic {
                    level: CodexInputMappingDiagnosticLevel::Info,
                    code: "codex_input.file_reference_mapped".to_owned(),
                    message: "Mapped file attachment to a Codex-readable file reference."
                        .to_owned(),
                    input_index: Some(input_index),
                });
            }
        }
    }

    Ok(CodexTurnInputMapping { input, diagnostics })
}

fn render_file_reference_text(
    location: CodexFileReferenceLocation,
    name: Option<String>,
    mime_type: Option<String>,
    size_bytes: Option<u64>,
    sha256: Option<String>,
) -> String {
    let mut lines = vec![
        "Attached file available to Codex.".to_owned(),
        "Use the file path or URL below when the user's request requires inspecting the attachment."
            .to_owned(),
        String::new(),
    ];

    if let Some(name) = name.as_ref().and_then(|value| normalized(value)) {
        lines.push(format!("Name: {name}"));
    }
    if let Some(mime_type) = mime_type.as_ref().and_then(|value| normalized(value)) {
        lines.push(format!("MIME type: {mime_type}"));
    }
    if let Some(size_bytes) = size_bytes {
        lines.push(format!("Size: {size_bytes} bytes"));
    }
    if let Some(sha256) = sha256.as_ref().and_then(|value| normalized(value)) {
        lines.push(format!("SHA-256: {sha256}"));
    }

    match location {
        CodexFileReferenceLocation::Path(path) => lines.push(format!("Path: {path}")),
        CodexFileReferenceLocation::Url(url) => lines.push(format!("URL: {url}")),
        CodexFileReferenceLocation::Reference(reference) => {
            lines.push(format!("Reference: {reference}"))
        }
    }

    lines.join("\n")
}

fn normalized(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
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

    #[test]
    fn maps_image_inputs_to_native_codex_items() {
        let mapping = map_codex_turn_input(CodexInputMappingRequest {
            inputs: vec![
                CodexInputSource::ImageUrl {
                    url: "https://example.test/image.png".to_owned(),
                },
                CodexInputSource::LocalImage {
                    path: "/tmp/image.png".to_owned(),
                },
            ],
        })
        .expect("image input should map");

        assert_eq!(
            mapping.input,
            vec![
                CodexTurnInputItem::Image {
                    url: "https://example.test/image.png".to_owned(),
                },
                CodexTurnInputItem::LocalImage {
                    path: "/tmp/image.png".to_owned(),
                },
            ]
        );
        assert_eq!(mapping.diagnostics.len(), 2);
    }

    #[test]
    fn maps_file_reference_to_text_input() {
        let mapping = map_codex_turn_input(CodexInputMappingRequest {
            inputs: vec![CodexInputSource::FileReference {
                location: CodexFileReferenceLocation::Path("/tmp/report.pdf".to_owned()),
                name: Some("report.pdf".to_owned()),
                mime_type: Some("application/pdf".to_owned()),
                size_bytes: Some(42),
                sha256: Some("abc123".to_owned()),
            }],
        })
        .expect("file reference should map");

        let CodexTurnInputItem::Text { text } = &mapping.input[0] else {
            panic!("file reference should become text");
        };
        assert!(text.contains("Attached file available to Codex."));
        assert!(text.contains("Name: report.pdf"));
        assert!(text.contains("MIME type: application/pdf"));
        assert!(text.contains("Path: /tmp/report.pdf"));
        assert!(
            mapping
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "codex_input.file_reference_mapped")
        );
    }

    #[test]
    fn serializes_local_image_as_app_server_camel_case() {
        let value = serde_json::to_value(CodexTurnInputItem::LocalImage {
            path: "/tmp/image.png".to_owned(),
        })
        .expect("serialize local image");

        assert_eq!(
            value,
            serde_json::json!({
                "type": "localImage",
                "path": "/tmp/image.png"
            })
        );
    }
}
