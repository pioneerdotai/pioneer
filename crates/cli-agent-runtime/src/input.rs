use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CLIRuntimeInputMappingRequest {
    pub inputs: Vec<CLIRuntimeInputSource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CLIRuntimeInputSource {
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
        location: CLIRuntimeFileReferenceLocation,
        name: Option<String>,
        mime_type: Option<String>,
        size_bytes: Option<u64>,
        sha256: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CLIRuntimeFileReferenceLocation {
    Path(String),
    Url(String),
    Reference(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CLIRuntimeTurnInputMapping {
    pub input: Vec<CLIRuntimeTurnInputItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<CLIRuntimeInputMappingDiagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum CLIRuntimeTurnInputItem {
    Text { text: String },
    Image { url: String },
    LocalImage { path: String },
    Skill { name: String, path: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CLIRuntimeInputMappingDiagnostic {
    pub level: CLIRuntimeInputMappingDiagnosticLevel,
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_index: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CLIRuntimeInputMappingDiagnosticLevel {
    Info,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CLIRuntimeInputMappingError {
    diagnostics: Vec<CLIRuntimeInputMappingDiagnostic>,
}

impl CLIRuntimeInputMappingError {
    pub fn diagnostics(&self) -> &[CLIRuntimeInputMappingDiagnostic] {
        self.diagnostics.as_slice()
    }
}

impl fmt::Display for CLIRuntimeInputMappingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let messages = self
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.level == CLIRuntimeInputMappingDiagnosticLevel::Error)
            .map(|diagnostic| diagnostic.message.as_str())
            .collect::<Vec<_>>();
        if messages.is_empty() {
            formatter.write_str("CLI runtime input mapping failed")
        } else {
            write!(
                formatter,
                "CLI runtime input mapping failed: {}",
                messages.join("; ")
            )
        }
    }
}

impl Error for CLIRuntimeInputMappingError {}

pub fn map_cli_runtime_turn_input(
    request: CLIRuntimeInputMappingRequest,
) -> Result<CLIRuntimeTurnInputMapping, CLIRuntimeInputMappingError> {
    map_cli_runtime_turn_input_for_runtime(request, "CLI runtime")
}

pub fn map_cli_runtime_turn_input_for_runtime(
    request: CLIRuntimeInputMappingRequest,
    runtime_label: &str,
) -> Result<CLIRuntimeTurnInputMapping, CLIRuntimeInputMappingError> {
    let mut input = Vec::with_capacity(request.inputs.len());
    let mut diagnostics = Vec::new();
    let runtime_label = normalized(runtime_label).unwrap_or("CLI runtime");

    for (input_index, item) in request.inputs.into_iter().enumerate() {
        match item {
            CLIRuntimeInputSource::Text { text } => {
                input.push(CLIRuntimeTurnInputItem::Text { text });
            }
            CLIRuntimeInputSource::ImageUrl { url } => {
                input.push(CLIRuntimeTurnInputItem::Image { url });
                diagnostics.push(CLIRuntimeInputMappingDiagnostic {
                    level: CLIRuntimeInputMappingDiagnosticLevel::Info,
                    code: "cli_runtime_input.image_url_mapped".to_owned(),
                    message: "Mapped image URL input to native CLI image input.".to_owned(),
                    input_index: Some(input_index),
                });
            }
            CLIRuntimeInputSource::LocalImage { path } => {
                input.push(CLIRuntimeTurnInputItem::LocalImage { path });
                diagnostics.push(CLIRuntimeInputMappingDiagnostic {
                    level: CLIRuntimeInputMappingDiagnosticLevel::Info,
                    code: "cli_runtime_input.local_image_mapped".to_owned(),
                    message: "Mapped local image input to native CLI localImage input.".to_owned(),
                    input_index: Some(input_index),
                });
            }
            CLIRuntimeInputSource::FileReference {
                location,
                name,
                mime_type,
                size_bytes,
                sha256,
            } => {
                input.push(CLIRuntimeTurnInputItem::Text {
                    text: render_file_reference_text(
                        &runtime_label,
                        location,
                        name,
                        mime_type,
                        size_bytes,
                        sha256,
                    ),
                });
                diagnostics.push(CLIRuntimeInputMappingDiagnostic {
                    level: CLIRuntimeInputMappingDiagnosticLevel::Info,
                    code: "cli_runtime_input.file_reference_mapped".to_owned(),
                    message: "Mapped file attachment to a CLI runtime-readable file reference."
                        .to_owned(),
                    input_index: Some(input_index),
                });
            }
        }
    }

    Ok(CLIRuntimeTurnInputMapping { input, diagnostics })
}

fn render_file_reference_text(
    runtime_label: &str,
    location: CLIRuntimeFileReferenceLocation,
    name: Option<String>,
    mime_type: Option<String>,
    size_bytes: Option<u64>,
    sha256: Option<String>,
) -> String {
    let mut lines = vec![
        format!("Attached file available to {runtime_label}."),
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
        CLIRuntimeFileReferenceLocation::Path(path) => lines.push(format!("Path: {path}")),
        CLIRuntimeFileReferenceLocation::Url(url) => lines.push(format!("URL: {url}")),
        CLIRuntimeFileReferenceLocation::Reference(reference) => {
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
        let mapping = map_cli_runtime_turn_input(CLIRuntimeInputMappingRequest {
            inputs: vec![CLIRuntimeInputSource::Text {
                text: "hello".to_owned(),
            }],
        })
        .expect("text input should map");

        assert_eq!(
            mapping.input,
            vec![CLIRuntimeTurnInputItem::Text {
                text: "hello".to_owned(),
            }]
        );
        assert!(mapping.diagnostics.is_empty());
    }

    #[test]
    fn maps_image_inputs_to_native_cli_items() {
        let mapping = map_cli_runtime_turn_input(CLIRuntimeInputMappingRequest {
            inputs: vec![
                CLIRuntimeInputSource::ImageUrl {
                    url: "https://example.test/image.png".to_owned(),
                },
                CLIRuntimeInputSource::LocalImage {
                    path: "/tmp/image.png".to_owned(),
                },
            ],
        })
        .expect("image input should map");

        assert_eq!(
            mapping.input,
            vec![
                CLIRuntimeTurnInputItem::Image {
                    url: "https://example.test/image.png".to_owned(),
                },
                CLIRuntimeTurnInputItem::LocalImage {
                    path: "/tmp/image.png".to_owned(),
                },
            ]
        );
        assert_eq!(mapping.diagnostics.len(), 2);
    }

    #[test]
    fn maps_file_reference_to_text_input() {
        let mapping = map_cli_runtime_turn_input(CLIRuntimeInputMappingRequest {
            inputs: vec![CLIRuntimeInputSource::FileReference {
                location: CLIRuntimeFileReferenceLocation::Path("/tmp/report.pdf".to_owned()),
                name: Some("report.pdf".to_owned()),
                mime_type: Some("application/pdf".to_owned()),
                size_bytes: Some(42),
                sha256: Some("abc123".to_owned()),
            }],
        })
        .expect("file reference should map");

        let CLIRuntimeTurnInputItem::Text { text } = &mapping.input[0] else {
            panic!("file reference should become text");
        };
        assert!(text.contains("Attached file available to CLI runtime."));
        assert!(text.contains("Name: report.pdf"));
        assert!(text.contains("MIME type: application/pdf"));
        assert!(text.contains("Path: /tmp/report.pdf"));
        assert!(
            mapping
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cli_runtime_input.file_reference_mapped")
        );
    }

    #[test]
    fn serializes_local_image_as_app_server_camel_case() {
        let value = serde_json::to_value(CLIRuntimeTurnInputItem::LocalImage {
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

    #[test]
    fn serializes_skill_as_app_server_structured_input() {
        let value = serde_json::to_value(CLIRuntimeTurnInputItem::Skill {
            name: "pdf".to_owned(),
            path: "/absolute/pdf/SKILL.md".to_owned(),
        })
        .expect("serialize skill input");

        assert_eq!(
            value,
            serde_json::json!({
                "type": "skill",
                "name": "pdf",
                "path": "/absolute/pdf/SKILL.md"
            })
        );
    }
}
