use pioneer_agent::ResolvedArtifactInput;
use pioneer_cli_agent_runtime::codex_input::{
    CodexFileReferenceLocation, CodexInputMappingRequest, CodexInputSource, CodexTurnInputMapping,
    map_codex_turn_input,
};
use pioneer_protocol::UserInput;
use pioneer_provider::{AttachmentDataSource, InputContentType};

pub(crate) fn map_codex_turn_input_from_pioneer(
    input: &[UserInput],
    resolved_artifacts: &[ResolvedArtifactInput],
) -> Result<CodexTurnInputMapping, pioneer_cli_agent_runtime::codex_input::CodexInputMappingError> {
    map_codex_turn_input(CodexInputMappingRequest {
        inputs: input
            .iter()
            .map(|input| codex_input_source_from_pioneer(input, resolved_artifacts))
            .collect(),
    })
}

fn codex_input_source_from_pioneer(
    input: &UserInput,
    resolved_artifacts: &[ResolvedArtifactInput],
) -> CodexInputSource {
    match input {
        UserInput::Text { text, .. } => CodexInputSource::Text { text: text.clone() },
        UserInput::Image { url } => CodexInputSource::ImageUrl { url: url.clone() },
        UserInput::LocalImage { path } => CodexInputSource::LocalImage { path: path.clone() },
        UserInput::File { url } | UserInput::Audio { url } | UserInput::Video { url } => {
            CodexInputSource::FileReference {
                location: CodexFileReferenceLocation::Url(url.clone()),
                name: None,
                mime_type: None,
                size_bytes: None,
                sha256: None,
            }
        }
        UserInput::LocalFile { path }
        | UserInput::LocalAudio { path }
        | UserInput::LocalVideo { path } => CodexInputSource::FileReference {
            location: CodexFileReferenceLocation::Path(path.clone()),
            name: file_name_from_path(path),
            mime_type: None,
            size_bytes: None,
            sha256: None,
        },
        UserInput::Artifact {
            artifact_id,
            version_id,
        } => resolved_artifact_input_source(artifact_id, version_id.as_deref(), resolved_artifacts)
            .unwrap_or_else(|| CodexInputSource::FileReference {
                location: CodexFileReferenceLocation::Reference(format!(
                    "artifact://{}{}",
                    artifact_id,
                    version_id
                        .as_ref()
                        .map(|version_id| format!("#{version_id}"))
                        .unwrap_or_default()
                )),
                name: None,
                mime_type: None,
                size_bytes: None,
                sha256: None,
            }),
        UserInput::Mention { .. } => {
            unreachable!("CLI runtime mentions must be rejected before input mapping")
        }
    }
}

fn resolved_artifact_input_source(
    artifact_id: &str,
    version_id: Option<&str>,
    resolved_artifacts: &[ResolvedArtifactInput],
) -> Option<CodexInputSource> {
    let resolved = resolved_artifacts.iter().find(|candidate| {
        candidate.artifact_id == artifact_id
            && version_id
                .is_none_or(|version_id| candidate.version_id.as_deref() == Some(version_id))
    })?;
    let name = resolved.attachment.name.clone();
    let mime_type = Some(resolved.attachment.mime_type.clone());
    let size_bytes = resolved.attachment.size_bytes;
    let sha256 = resolved.attachment.sha256.clone();

    match (resolved.content_type, &resolved.attachment.source) {
        (InputContentType::Image, AttachmentDataSource::Path { path }) => {
            Some(CodexInputSource::LocalImage { path: path.clone() })
        }
        (InputContentType::Image, AttachmentDataSource::Url { url }) => {
            Some(CodexInputSource::ImageUrl { url: url.clone() })
        }
        (_, AttachmentDataSource::Path { path }) => Some(CodexInputSource::FileReference {
            location: CodexFileReferenceLocation::Path(path.clone()),
            name,
            mime_type,
            size_bytes,
            sha256,
        }),
        (_, AttachmentDataSource::Url { url }) => Some(CodexInputSource::FileReference {
            location: CodexFileReferenceLocation::Url(url.clone()),
            name,
            mime_type,
            size_bytes,
            sha256,
        }),
        (_, AttachmentDataSource::Reference { reference }) => {
            Some(CodexInputSource::FileReference {
                location: CodexFileReferenceLocation::Reference(reference.clone()),
                name,
                mime_type,
                size_bytes,
                sha256,
            })
        }
        (_, AttachmentDataSource::Bytes { .. }) => Some(CodexInputSource::FileReference {
            location: CodexFileReferenceLocation::Reference(format!("artifact://{artifact_id}")),
            name,
            mime_type,
            size_bytes,
            sha256,
        }),
    }
}

fn file_name_from_path(path: &str) -> Option<String> {
    std::path::Path::new(path)
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_provider::MessageAttachment;

    fn resolved_artifact(
        artifact_id: &str,
        version_id: Option<&str>,
        content_type: InputContentType,
        path: &str,
    ) -> ResolvedArtifactInput {
        let mut attachment = MessageAttachment::from_path(path, "application/octet-stream");
        attachment.name = Some("attachment.bin".to_owned());
        attachment.size_bytes = Some(10);
        attachment.sha256 = Some("a".repeat(64));

        ResolvedArtifactInput {
            artifact_id: artifact_id.to_owned(),
            version_id: version_id.map(str::to_owned),
            content_type,
            attachment,
        }
    }

    #[test]
    fn codex_text_input_maps_from_pioneer_user_input() {
        let mapping = map_codex_turn_input_from_pioneer(
            &[UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }],
            &[],
        )
        .expect("text input should map");

        assert!(mapping.diagnostics.is_empty());
        assert_eq!(
            mapping.input,
            vec![
                pioneer_cli_agent_runtime::codex_input::CodexTurnInputItem::Text {
                    text: "hello".to_owned(),
                }
            ]
        );
    }

    #[test]
    fn codex_local_image_maps_from_pioneer_user_input() {
        let mapping = map_codex_turn_input_from_pioneer(
            &[UserInput::LocalImage {
                path: "/tmp/screenshot.png".to_owned(),
            }],
            &[],
        )
        .expect("image input should map");

        assert_eq!(
            mapping.input,
            vec![
                pioneer_cli_agent_runtime::codex_input::CodexTurnInputItem::LocalImage {
                    path: "/tmp/screenshot.png".to_owned(),
                }
            ]
        );
    }

    #[test]
    fn codex_local_file_maps_from_pioneer_user_input_as_text_reference() {
        let mapping = map_codex_turn_input_from_pioneer(
            &[UserInput::LocalFile {
                path: "/tmp/report.pdf".to_owned(),
            }],
            &[],
        )
        .expect("file input should map");

        let pioneer_cli_agent_runtime::codex_input::CodexTurnInputItem::Text { text } =
            &mapping.input[0]
        else {
            panic!("file should map as text reference");
        };
        assert!(text.contains("Attached file available to Codex."));
        assert!(text.contains("Name: report.pdf"));
        assert!(text.contains("Path: /tmp/report.pdf"));
    }

    #[test]
    fn codex_image_artifact_maps_to_materialized_local_image() {
        let mapping = map_codex_turn_input_from_pioneer(
            &[UserInput::Artifact {
                artifact_id: "art_img".to_owned(),
                version_id: None,
            }],
            &[resolved_artifact(
                "art_img",
                Some("ver_img"),
                InputContentType::Image,
                "/tmp/materialized/screenshot.png",
            )],
        )
        .expect("image artifact should map");

        assert_eq!(
            mapping.input,
            vec![
                pioneer_cli_agent_runtime::codex_input::CodexTurnInputItem::LocalImage {
                    path: "/tmp/materialized/screenshot.png".to_owned(),
                }
            ]
        );
    }

    #[test]
    fn codex_file_artifact_maps_to_materialized_file_reference() {
        let mapping = map_codex_turn_input_from_pioneer(
            &[UserInput::Artifact {
                artifact_id: "art_file".to_owned(),
                version_id: Some("ver_file".to_owned()),
            }],
            &[resolved_artifact(
                "art_file",
                Some("ver_file"),
                InputContentType::File,
                "/tmp/materialized/report.pdf",
            )],
        )
        .expect("file artifact should map");

        let pioneer_cli_agent_runtime::codex_input::CodexTurnInputItem::Text { text } =
            &mapping.input[0]
        else {
            panic!("file artifact should map as text reference");
        };
        assert!(text.contains("Attached file available to Codex."));
        assert!(text.contains("Name: attachment.bin"));
        assert!(text.contains("Path: /tmp/materialized/report.pdf"));
    }
}
