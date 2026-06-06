//! Turn request preparation helpers.

use crate::{
    artifacts::upload::{ArtifactUploadFileRequest, ArtifactUploadTransport, upload_artifact_file},
    composer::attachments::{
        ComposerAttachment, ComposerAttachmentKind, ComposerAttachmentUploadState,
    },
    composer::capabilities::{
        ComposerCapability, turn_capabilities_from_composer_capabilities,
        user_message_attachments_from_composer_capabilities,
    },
    gateway::types::GatewayEndpointKind,
    platform::{ClientFileSystem, ClientPath},
    turns::start as turn_start,
};
use anyhow::{Context as _, Result, anyhow};
use pioneer_protocol::{
    ArtifactCapabilitiesParams, ArtifactCapabilitiesResponse, ArtifactRef, TurnCapability,
    UserInput, UserMessageAttachment,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PreparedComposerAttachment {
    pub attachment: ComposerAttachment,
    pub artifact: Option<ArtifactRef>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PrepareComposerTurnRequest {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub endpoint_kind: Option<GatewayEndpointKind>,
    pub text: String,
    pub attachments: Vec<ComposerAttachment>,
    pub capabilities: Vec<ComposerCapability>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PreparedComposerTurn {
    pub input: Vec<UserInput>,
    pub capabilities: Vec<TurnCapability>,
    pub user_text: String,
    pub user_message_text: String,
    pub user_attachments: Vec<UserMessageAttachment>,
    pub attachments: Vec<PreparedComposerAttachment>,
}

pub trait ComposerTurnPrepareTransport: ArtifactUploadTransport {
    fn artifact_capabilities(
        &self,
        params: ArtifactCapabilitiesParams,
    ) -> Result<ArtifactCapabilitiesResponse>;
}

pub fn prepare_composer_turn<TTransport, TFileSystem>(
    transport: &TTransport,
    file_system: &TFileSystem,
    request: PrepareComposerTurnRequest,
) -> Result<PreparedComposerTurn>
where
    TTransport: ComposerTurnPrepareTransport,
    TFileSystem: ClientFileSystem,
{
    if request.workspace_id.trim().is_empty() {
        return Err(anyhow!(
            "workspace_id is required before uploading attachments"
        ));
    }

    let artifact_capabilities = artifact_capabilities_params_for_attachments(
        request.workspace_id.as_str(),
        !request.attachments.is_empty(),
    )
    .and_then(|params| transport.artifact_capabilities(params).ok());
    let upload_required = local_attachments_require_artifact_upload(
        request.endpoint_kind,
        artifact_capabilities.as_ref(),
    );
    let mut prepared_attachments = Vec::with_capacity(request.attachments.len());

    for (index, attachment) in request.attachments.into_iter().enumerate() {
        let artifact = if let Some(artifact) =
            crate::composer::attachments::uploaded_artifact_from_attachment(&attachment)
        {
            Some(artifact)
        } else if upload_required {
            let client_attachment_id = format!("{}_attachment_{index}", request.turn_id);
            Some(
                upload_artifact_file(
                    transport,
                    file_system,
                    ArtifactUploadFileRequest {
                        workspace_id: request.workspace_id.clone(),
                        thread_id: Some(request.thread_id.clone()),
                        planned_turn_id: Some(request.turn_id.clone()),
                        client_attachment_id,
                        path: ClientPath::new(attachment.path.clone()),
                        mime_type: None,
                    },
                )
                .with_context(|| {
                    format!(
                        "failed to upload `{}` before starting turn",
                        attachment.file_name
                    )
                })?,
            )
        } else {
            None
        };

        prepared_attachments.push(PreparedComposerAttachment {
            attachment,
            artifact,
        });
    }

    Ok(build_prepared_composer_turn(
        request.text,
        prepared_attachments,
        request.capabilities,
    ))
}

pub fn build_prepared_composer_turn(
    text: String,
    prepared_attachments: Vec<PreparedComposerAttachment>,
    capabilities: Vec<ComposerCapability>,
) -> PreparedComposerTurn {
    let text_preparation = turn_start::prepare_text_turn(text.as_str());
    let input = turn_input_from_prepared_composer(text.as_str(), prepared_attachments.as_slice());
    let turn_capabilities = turn_capabilities_from_composer_capabilities(capabilities.as_slice());
    let user_text = user_message_preview_from_prepared_composer(
        text_preparation.user_text.as_str(),
        prepared_attachments.as_slice(),
    );
    let user_message_text = text_preparation.user_message_text;
    let mut user_attachments =
        user_message_attachments_from_prepared_composer(prepared_attachments.as_slice());
    user_attachments.extend(user_message_attachments_from_composer_capabilities(
        capabilities.as_slice(),
    ));

    PreparedComposerTurn {
        input,
        capabilities: turn_capabilities,
        user_text,
        user_message_text,
        user_attachments,
        attachments: prepared_attachments,
    }
}

pub fn artifact_capabilities_params_for_attachments(
    workspace_id: &str,
    has_attachments: bool,
) -> Option<ArtifactCapabilitiesParams> {
    has_attachments.then(|| ArtifactCapabilitiesParams {
        workspace_id: workspace_id.to_owned(),
    })
}

pub fn composer_has_sendable_content(
    text: &str,
    has_attachments: bool,
    has_capabilities: bool,
) -> bool {
    !text.trim().is_empty() || has_attachments || has_capabilities
}

pub fn local_attachments_require_artifact_upload(
    endpoint_kind: Option<GatewayEndpointKind>,
    capabilities: Option<&ArtifactCapabilitiesResponse>,
) -> bool {
    if capabilities.is_some_and(|capabilities| capabilities.upload.required_for_local_paths) {
        return true;
    }

    !matches!(endpoint_kind, Some(GatewayEndpointKind::Local)) || capabilities.is_none()
}

pub fn mark_pending_composer_attachments_uploading(attachments: &mut [ComposerAttachment]) -> bool {
    let mut changed = false;
    for attachment in attachments {
        if !matches!(
            attachment.upload_state,
            ComposerAttachmentUploadState::Uploaded { .. }
        ) {
            attachment.upload_state = ComposerAttachmentUploadState::Uploading;
            changed = true;
        }
    }
    changed
}

pub fn mark_uploading_composer_attachments_failed(
    attachments: &mut [ComposerAttachment],
    error: impl AsRef<str>,
) -> bool {
    let error = error.as_ref();
    let mut changed = false;
    for attachment in attachments {
        if matches!(
            attachment.upload_state,
            ComposerAttachmentUploadState::Uploading
        ) {
            attachment.upload_state = ComposerAttachmentUploadState::Failed {
                error: error.to_owned(),
            };
            changed = true;
        }
    }
    changed
}

pub fn apply_uploaded_composer_attachment_artifacts(
    attachments: &mut [ComposerAttachment],
    artifacts: impl IntoIterator<Item = Option<ArtifactRef>>,
) -> bool {
    let mut changed = false;
    for (attachment, artifact) in attachments.iter_mut().zip(artifacts) {
        if let Some(artifact) = artifact {
            attachment.upload_state = ComposerAttachmentUploadState::Uploaded { artifact };
            changed = true;
        }
    }
    changed
}

pub fn turn_input_from_prepared_composer(
    text: &str,
    attachments: &[PreparedComposerAttachment],
) -> Vec<UserInput> {
    let mut input = turn_start::text_user_input(text)
        .into_iter()
        .collect::<Vec<_>>();

    for prepared in attachments {
        if let Some(artifact) = prepared.artifact.as_ref() {
            input.push(UserInput::Artifact {
                artifact_id: artifact.artifact_id.clone(),
                version_id: artifact.version_id.clone(),
            });
        } else {
            input.push(local_user_input_from_attachment(&prepared.attachment));
        }
    }

    input
}

pub fn turn_input_from_composer_attachments(
    text: &str,
    attachments: &[ComposerAttachment],
) -> Vec<UserInput> {
    let mut input = turn_start::text_user_input(text)
        .into_iter()
        .collect::<Vec<_>>();

    for attachment in attachments {
        input.push(local_user_input_from_attachment(attachment));
    }

    input
}

pub fn local_user_input_from_attachment(attachment: &ComposerAttachment) -> UserInput {
    match attachment.kind {
        ComposerAttachmentKind::Image => UserInput::LocalImage {
            path: attachment.path.clone(),
        },
        ComposerAttachmentKind::Audio => UserInput::LocalAudio {
            path: attachment.path.clone(),
        },
        ComposerAttachmentKind::Video => UserInput::LocalVideo {
            path: attachment.path.clone(),
        },
        ComposerAttachmentKind::File => UserInput::LocalFile {
            path: attachment.path.clone(),
        },
    }
}

pub fn user_message_attachments_from_prepared_composer(
    attachments: &[PreparedComposerAttachment],
) -> Vec<UserMessageAttachment> {
    attachments
        .iter()
        .map(user_message_attachment_from_prepared_attachment)
        .collect()
}

pub fn user_message_attachment_from_prepared_attachment(
    prepared: &PreparedComposerAttachment,
) -> UserMessageAttachment {
    if let Some(artifact) = prepared.artifact.as_ref() {
        UserMessageAttachment::Artifact {
            artifact: artifact.clone(),
        }
    } else {
        local_user_message_attachment_from_attachment(&prepared.attachment)
    }
}

pub fn local_user_message_attachment_from_attachment(
    attachment: &ComposerAttachment,
) -> UserMessageAttachment {
    match attachment.kind {
        ComposerAttachmentKind::Image => UserMessageAttachment::LocalImage {
            path: attachment.path.clone(),
        },
        ComposerAttachmentKind::Audio => UserMessageAttachment::LocalAudio {
            path: attachment.path.clone(),
        },
        ComposerAttachmentKind::Video => UserMessageAttachment::LocalVideo {
            path: attachment.path.clone(),
        },
        ComposerAttachmentKind::File => UserMessageAttachment::LocalFile {
            path: attachment.path.clone(),
        },
    }
}

pub fn user_message_preview_from_prepared_composer(
    text: &str,
    attachments: &[PreparedComposerAttachment],
) -> String {
    let mut body_lines = Vec::new();
    let trimmed = text.trim();
    if !trimmed.is_empty() {
        body_lines.push(trimmed.to_owned());
    }

    let attachment_names = attachments
        .iter()
        .map(|prepared| {
            let display_name = prepared
                .artifact
                .as_ref()
                .map(|artifact| artifact.display_name.as_str())
                .filter(|display_name| !display_name.trim().is_empty())
                .unwrap_or(prepared.attachment.file_name.as_str());
            if display_name.trim().is_empty() {
                attachment_name_from_reference(prepared.attachment.path.as_str())
            } else {
                display_name.to_owned()
            }
        })
        .filter(|name| !name.trim().is_empty())
        .collect::<Vec<_>>();

    if !attachment_names.is_empty() {
        body_lines.push(attachment_names.join(", "));
    }

    body_lines.join("\n")
}

pub fn attachment_name_from_reference(reference: &str) -> String {
    if reference.contains("://") || reference.starts_with("data:") {
        let without_query = reference
            .split_once('?')
            .map_or(reference, |(value, _)| value);
        let without_fragment = without_query
            .split_once('#')
            .map_or(without_query, |(value, _)| value);
        let candidate = without_fragment
            .rsplit('/')
            .next()
            .unwrap_or(without_fragment);
        if candidate.is_empty() {
            reference.to_owned()
        } else {
            candidate.to_owned()
        }
    } else {
        std::path::Path::new(reference)
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::to_owned)
            .unwrap_or_else(|| reference.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composer::attachments::{ComposerAttachmentKind, ComposerAttachmentUploadState};
    use crate::platform::{ClientFileMetadata, ClientFileSystem, ClientPath};
    use crate::{ClientError, ClientResult};
    use pioneer_protocol::{
        ArtifactCapabilitiesResponse, ArtifactDownloadCapabilities, ArtifactKind, ArtifactStatus,
        ArtifactUploadAbortParams, ArtifactUploadAbortResponse, ArtifactUploadCapabilities,
        ArtifactUploadChunkAckNotification, ArtifactUploadFinishParams,
        ArtifactUploadFinishResponse, ArtifactUploadStartParams, ArtifactUploadStartResponse,
    };
    use std::sync::Mutex;

    fn attachment(path: &str, upload_state: ComposerAttachmentUploadState) -> ComposerAttachment {
        ComposerAttachment {
            path: path.to_owned(),
            file_name: path.to_owned(),
            kind: ComposerAttachmentKind::File,
            upload_state,
        }
    }

    fn artifact_ref(id: &str) -> ArtifactRef {
        ArtifactRef {
            artifact_id: id.to_owned(),
            version_id: Some(format!("{id}_v1")),
            display_name: format!("{id}.txt"),
            kind: ArtifactKind::File,
            mime_type: Some("text/plain".to_owned()),
            size_bytes: Some(5),
            sha256: Some("a".repeat(64)),
            status: ArtifactStatus::Ready,
            preview: None,
        }
    }

    fn capabilities(required_for_local_paths: bool) -> ArtifactCapabilitiesResponse {
        ArtifactCapabilitiesResponse {
            upload: ArtifactUploadCapabilities {
                required_for_local_paths,
                recommended_chunk_size_bytes: 1024,
                max_chunk_size_bytes: 1024,
                max_file_size_bytes: 1024 * 1024,
                max_files_per_turn: 16,
            },
            download: ArtifactDownloadCapabilities {
                recommended_chunk_size_bytes: 1024,
                max_chunk_size_bytes: 1024,
                max_concurrent_downloads: 2,
            },
        }
    }

    #[test]
    fn artifact_capabilities_params_are_only_needed_when_attachments_exist() {
        assert!(artifact_capabilities_params_for_attachments("ws_1", false).is_none());

        let params = artifact_capabilities_params_for_attachments("ws_1", true)
            .expect("capability query params");

        assert_eq!(params.workspace_id, "ws_1");
    }

    #[test]
    fn composer_content_gate_allows_text_attachment_or_capability() {
        assert!(composer_has_sendable_content("", false, true));
        assert!(composer_has_sendable_content("   ", true, false));
        assert!(composer_has_sendable_content("hello", false, false));
        assert!(!composer_has_sendable_content("   ", false, false));
    }

    #[test]
    fn remote_gateway_requires_artifact_upload_even_when_capability_is_permissive() {
        let caps = capabilities(false);

        assert!(local_attachments_require_artifact_upload(
            Some(GatewayEndpointKind::Remote),
            Some(&caps)
        ));
        assert!(local_attachments_require_artifact_upload(None, Some(&caps)));
        assert!(!local_attachments_require_artifact_upload(
            Some(GatewayEndpointKind::Local),
            Some(&caps)
        ));
    }

    #[test]
    fn capability_required_for_local_paths_forces_upload() {
        let caps = capabilities(true);

        assert!(local_attachments_require_artifact_upload(
            Some(GatewayEndpointKind::Local),
            Some(&caps)
        ));
    }

    #[test]
    fn missing_capability_response_falls_back_to_upload() {
        assert!(local_attachments_require_artifact_upload(
            Some(GatewayEndpointKind::Local),
            None
        ));
    }

    #[test]
    fn mark_pending_uploading_skips_already_uploaded_attachments() {
        let uploaded = artifact_ref("artifact_1");
        let mut attachments = vec![
            attachment("/tmp/local.txt", ComposerAttachmentUploadState::Local),
            attachment(
                "/tmp/uploaded.txt",
                ComposerAttachmentUploadState::Uploaded {
                    artifact: uploaded.clone(),
                },
            ),
            attachment(
                "/tmp/failed.txt",
                ComposerAttachmentUploadState::Failed {
                    error: "old".to_owned(),
                },
            ),
        ];

        assert!(mark_pending_composer_attachments_uploading(
            &mut attachments
        ));

        assert_eq!(
            attachments[0].upload_state,
            ComposerAttachmentUploadState::Uploading
        );
        assert_eq!(
            attachments[1].upload_state,
            ComposerAttachmentUploadState::Uploaded { artifact: uploaded }
        );
        assert_eq!(
            attachments[2].upload_state,
            ComposerAttachmentUploadState::Uploading
        );
    }

    #[test]
    fn mark_uploading_failed_updates_only_uploading_attachments() {
        let uploaded = artifact_ref("artifact_1");
        let mut attachments = vec![
            attachment("/tmp/local.txt", ComposerAttachmentUploadState::Local),
            attachment(
                "/tmp/uploading.txt",
                ComposerAttachmentUploadState::Uploading,
            ),
            attachment(
                "/tmp/uploaded.txt",
                ComposerAttachmentUploadState::Uploaded {
                    artifact: uploaded.clone(),
                },
            ),
        ];

        assert!(mark_uploading_composer_attachments_failed(
            &mut attachments,
            "boom"
        ));

        assert_eq!(
            attachments[0].upload_state,
            ComposerAttachmentUploadState::Local
        );
        assert_eq!(
            attachments[1].upload_state,
            ComposerAttachmentUploadState::Failed {
                error: "boom".to_owned()
            }
        );
        assert_eq!(
            attachments[2].upload_state,
            ComposerAttachmentUploadState::Uploaded { artifact: uploaded }
        );
    }

    #[test]
    fn apply_uploaded_artifacts_updates_matching_attachment_positions() {
        let artifact = artifact_ref("artifact_1");
        let mut attachments = vec![
            attachment("/tmp/a.txt", ComposerAttachmentUploadState::Uploading),
            attachment("/tmp/b.txt", ComposerAttachmentUploadState::Uploading),
        ];

        assert!(apply_uploaded_composer_attachment_artifacts(
            &mut attachments,
            vec![None, Some(artifact.clone())]
        ));

        assert_eq!(
            attachments[0].upload_state,
            ComposerAttachmentUploadState::Uploading
        );
        assert_eq!(
            attachments[1].upload_state,
            ComposerAttachmentUploadState::Uploaded { artifact }
        );
    }

    #[test]
    fn turn_input_maps_uploaded_attachments_to_artifact_input() {
        let prepared = vec![
            PreparedComposerAttachment {
                attachment: attachment("/tmp/a.txt", ComposerAttachmentUploadState::Uploading),
                artifact: Some(artifact_ref("art_a")),
            },
            PreparedComposerAttachment {
                attachment: attachment("/tmp/b.txt", ComposerAttachmentUploadState::Uploading),
                artifact: Some(artifact_ref("art_b")),
            },
        ];

        let input = turn_input_from_prepared_composer("hello", prepared.as_slice());

        assert_eq!(input.len(), 3);
        assert!(matches!(
            input[1],
            UserInput::Artifact {
                ref artifact_id,
                ref version_id,
            } if artifact_id == "art_a" && version_id.as_deref() == Some("art_a_v1")
        ));
        assert!(matches!(
            input[2],
            UserInput::Artifact {
                ref artifact_id,
                ref version_id,
            } if artifact_id == "art_b" && version_id.as_deref() == Some("art_b_v1")
        ));
    }

    #[test]
    fn turn_input_keeps_local_input_when_upload_is_not_required() {
        let prepared = vec![PreparedComposerAttachment {
            attachment: attachment("/tmp/a.txt", ComposerAttachmentUploadState::Local),
            artifact: None,
        }];

        let input = turn_input_from_prepared_composer("hello", prepared.as_slice());

        assert!(matches!(input[1], UserInput::LocalFile { .. }));
    }

    #[test]
    fn user_message_attachments_project_artifacts_and_local_files() {
        let artifact = artifact_ref("art_a");
        let prepared = vec![
            PreparedComposerAttachment {
                attachment: attachment("/tmp/a.txt", ComposerAttachmentUploadState::Local),
                artifact: Some(artifact.clone()),
            },
            PreparedComposerAttachment {
                attachment: attachment("/tmp/b.txt", ComposerAttachmentUploadState::Local),
                artifact: None,
            },
        ];

        let attachments = user_message_attachments_from_prepared_composer(prepared.as_slice());

        assert!(matches!(
            attachments[0],
            UserMessageAttachment::Artifact {
                artifact: ref actual
            }
                if actual.artifact_id == artifact.artifact_id
        ));
        assert!(matches!(
            attachments[1],
            UserMessageAttachment::LocalFile { ref path } if path == "/tmp/b.txt"
        ));
    }

    #[test]
    fn user_message_preview_uses_uploaded_display_names() {
        let prepared = vec![PreparedComposerAttachment {
            attachment: ComposerAttachment {
                path: "/tmp/a.txt".to_owned(),
                file_name: "local-a.txt".to_owned(),
                kind: ComposerAttachmentKind::File,
                upload_state: ComposerAttachmentUploadState::Local,
            },
            artifact: Some(ArtifactRef {
                display_name: "remote-a.txt".to_owned(),
                ..artifact_ref("art_a")
            }),
        }];

        let preview = user_message_preview_from_prepared_composer("hello", prepared.as_slice());

        assert_eq!(preview, "hello\nremote-a.txt");
    }

    #[test]
    fn prepare_composer_turn_uploads_local_attachment_when_required() {
        let transport = FakeTurnPrepareTransport::new(capabilities(true));
        let file_system = FakeTurnPrepareFileSystem {
            bytes: b"hello".to_vec(),
        };

        let prepared = prepare_composer_turn(
            &transport,
            &file_system,
            PrepareComposerTurnRequest {
                workspace_id: "ws_1".to_owned(),
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                endpoint_kind: Some(GatewayEndpointKind::Local),
                text: "hello".to_owned(),
                attachments: vec![attachment(
                    "/tmp/report.txt",
                    ComposerAttachmentUploadState::Uploading,
                )],
                capabilities: Vec::new(),
            },
        )
        .expect("prepare composer turn");

        assert_eq!(prepared.input.len(), 2);
        assert!(matches!(
            prepared.input[1],
            UserInput::Artifact {
                ref artifact_id,
                ref version_id,
            } if artifact_id == "artifact_uploaded" && version_id.as_deref() == Some("version_uploaded")
        ));
        assert!(matches!(
            prepared.user_attachments[0],
            UserMessageAttachment::Artifact {
                artifact: ref actual,
            } if actual.artifact_id == "artifact_uploaded"
        ));
        assert_eq!(
            transport.started()[0].client_attachment_id,
            "turn_1_attachment_0"
        );
        assert_eq!(transport.chunks(), vec![(0, b"hello".to_vec())]);
    }

    struct FakeTurnPrepareFileSystem {
        bytes: Vec<u8>,
    }

    impl ClientFileSystem for FakeTurnPrepareFileSystem {
        fn read_file(&self, _path: &ClientPath) -> ClientResult<Vec<u8>> {
            Ok(self.bytes.clone())
        }

        fn metadata(&self, _path: &ClientPath) -> ClientResult<ClientFileMetadata> {
            Ok(ClientFileMetadata {
                len: self.bytes.len() as u64,
                modified: None,
                is_file: true,
                is_dir: false,
            })
        }

        fn write_cache_file(&self, _key: &str, _bytes: &[u8]) -> ClientResult<ClientPath> {
            Err(ClientError::platform("cache writes are not supported"))
        }
    }

    struct FakeTurnPrepareTransport {
        capabilities: ArtifactCapabilitiesResponse,
        started: Mutex<Vec<ArtifactUploadStartParams>>,
        chunks: Mutex<Vec<(u64, Vec<u8>)>>,
        finished: Mutex<Vec<ArtifactUploadFinishParams>>,
    }

    impl FakeTurnPrepareTransport {
        fn new(capabilities: ArtifactCapabilitiesResponse) -> Self {
            Self {
                capabilities,
                started: Mutex::new(Vec::new()),
                chunks: Mutex::new(Vec::new()),
                finished: Mutex::new(Vec::new()),
            }
        }

        fn started(&self) -> Vec<ArtifactUploadStartParams> {
            self.started.lock().expect("started").clone()
        }

        fn chunks(&self) -> Vec<(u64, Vec<u8>)> {
            self.chunks.lock().expect("chunks").clone()
        }
    }

    impl ComposerTurnPrepareTransport for FakeTurnPrepareTransport {
        fn artifact_capabilities(
            &self,
            _params: ArtifactCapabilitiesParams,
        ) -> Result<ArtifactCapabilitiesResponse> {
            Ok(self.capabilities.clone())
        }
    }

    impl ArtifactUploadTransport for FakeTurnPrepareTransport {
        fn artifact_upload_start(
            &self,
            params: ArtifactUploadStartParams,
        ) -> Result<ArtifactUploadStartResponse> {
            self.started.lock().expect("started").push(params);
            Ok(ArtifactUploadStartResponse {
                upload_id: "upload_1".to_owned(),
                recommended_chunk_size_bytes: 16,
                max_chunk_size_bytes: 16,
                max_size_bytes: 1024,
                expires_at_unix: 1_700_000_000,
            })
        }

        fn send_artifact_upload_chunk(
            &self,
            _workspace_id: String,
            upload_id: String,
            offset: u64,
            chunk: Vec<u8>,
        ) -> Result<ArtifactUploadChunkAckNotification> {
            assert_eq!(upload_id, "upload_1");
            let len = chunk.len() as u64;
            self.chunks.lock().expect("chunks").push((offset, chunk));
            Ok(ArtifactUploadChunkAckNotification {
                workspace_id: "ws_1".to_owned(),
                upload_id,
                offset,
                len,
                received_bytes: len,
                next_offset: offset + len,
            })
        }

        fn artifact_upload_finish(
            &self,
            params: ArtifactUploadFinishParams,
        ) -> Result<ArtifactUploadFinishResponse> {
            self.finished.lock().expect("finished").push(params.clone());
            Ok(ArtifactUploadFinishResponse {
                upload_id: params.upload_id,
                artifact: ArtifactRef {
                    artifact_id: "artifact_uploaded".to_owned(),
                    version_id: Some("version_uploaded".to_owned()),
                    display_name: "report.txt".to_owned(),
                    kind: ArtifactKind::File,
                    mime_type: Some("text/plain".to_owned()),
                    size_bytes: Some(5),
                    sha256: Some("a".repeat(64)),
                    status: ArtifactStatus::Ready,
                    preview: None,
                },
            })
        }

        fn artifact_upload_abort(
            &self,
            _params: ArtifactUploadAbortParams,
        ) -> Result<ArtifactUploadAbortResponse> {
            unreachable!("successful fake upload should not abort")
        }
    }
}
