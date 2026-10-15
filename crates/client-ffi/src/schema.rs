//! FFI boundary schema export.
//!
//! These schemas describe bridge-owned request/result/event DTOs. Shared domain
//! schemas are exported by `pioneer-client`.

use schemars::{Schema, schema_for};
use std::{fs, path::Path};

pub struct SchemaDocument {
    pub file_name: &'static str,
    pub schema: Schema,
}

macro_rules! schema_doc {
    ($file_name:literal, $ty:ty) => {
        SchemaDocument {
            file_name: $file_name,
            schema: schema_for!($ty),
        }
    };
}

pub fn client_ffi_schema_documents() -> Vec<SchemaDocument> {
    let mut documents = vec![
        schema_doc!(
            "client_scope_lease_request_dto.json",
            crate::client_binding::ClientScopeLeaseRequestDto
        ),
        schema_doc!(
            "administration_activation_request.json",
            crate::administration_activation::AdministrationActivationRequest
        ),
        schema_doc!(
            "timeline_snapshot.json",
            pioneer_client::timeline::presentation::TimelineSnapshot
        ),
        schema_doc!(
            "client_gateway_session_validation_request.json",
            crate::auth::ClientGatewaySessionValidationRequest
        ),
        schema_doc!(
            "client_gateway_session_validation_result.json",
            crate::auth::ClientGatewaySessionValidationResult
        ),
        schema_doc!(
            "client_publication_wait_request_dto.json",
            crate::client_binding::ClientPublicationWaitRequestDto
        ),
        schema_doc!(
            "client_process_change_batch_dto.json",
            crate::client_binding::ClientProcessChangeBatchDto
        ),
        schema_doc!(
            "client_intent_dispatch_dto.json",
            crate::client_binding::ClientIntentDispatchDto
        ),
        schema_doc!(
            "client_scoped_snapshot_request_dto.json",
            crate::client_binding::ClientScopedSnapshotRequestDto
        ),
        schema_doc!(
            "client_scoped_snapshot_dto.json",
            crate::client_binding::ClientScopedSnapshotDto
        ),
        schema_doc!(
            "client_change_batch_request_dto.json",
            crate::client_binding::ClientChangeBatchRequestDto
        ),
        schema_doc!(
            "client_change_batch_dto.json",
            crate::client_binding::ClientChangeBatchDto
        ),
        schema_doc!(
            "client_effect_completion_dto.json",
            crate::client_binding::ClientEffectCompletionDto
        ),
        schema_doc!(
            "client_effect_cancellation_dto.json",
            crate::client_binding::ClientEffectCancellationDto
        ),
        schema_doc!(
            "client_sequence_gap_resnapshot_dto.json",
            crate::client_binding::ClientSequenceGapResnapshotDto
        ),
        schema_doc!(
            "client_transition_dto.json",
            crate::client_binding::ClientTransitionDto
        ),
        schema_doc!(
            "client_artifact_target_request.json",
            crate::artifacts::ClientArtifactTargetRequest
        ),
        schema_doc!(
            "client_artifact_view_open_result.json",
            crate::artifacts::ClientArtifactViewOpenResult
        ),
        schema_doc!(
            "client_thread_file_view_open_request.json",
            crate::thread_files::ClientThreadFileViewOpenRequest
        ),
        schema_doc!(
            "client_thread_file_view_open_result.json",
            crate::thread_files::ClientThreadFileViewOpenResult
        ),
        schema_doc!(
            "client_artifact_download_request.json",
            crate::artifacts::ClientArtifactDownloadRequest
        ),
        schema_doc!(
            "client_artifact_download_result.json",
            crate::artifacts::ClientArtifactDownloadResult
        ),
        schema_doc!(
            "client_member_avatar_cache_request.json",
            crate::avatars::ClientMemberAvatarCacheRequest
        ),
        schema_doc!(
            "client_member_avatar_cache_result.json",
            crate::avatars::ClientMemberAvatarCacheResult
        ),
        schema_doc!(
            "client_agent_avatar_cache_request.json",
            crate::avatars::ClientAgentAvatarCacheRequest
        ),
        schema_doc!(
            "client_agent_avatar_cache_result.json",
            crate::avatars::ClientAgentAvatarCacheResult
        ),
        schema_doc!(
            "client_diagnostic_event.json",
            crate::diagnostics::ClientDiagnosticEvent
        ),
        schema_doc!(
            "client_thread_create_visibility_request.json",
            crate::presentation::ClientThreadCreateVisibilityRequest
        ),
        schema_doc!(
            "client_member_presentation_request.json",
            crate::presentation::ClientMemberPresentationRequest
        ),
        schema_doc!(
            "client_current_principal_presentation_request.json",
            crate::presentation::ClientCurrentPrincipalPresentationRequest
        ),
        schema_doc!(
            "client_artifact_presentation_policy_request.json",
            crate::presentation::ClientArtifactPresentationPolicyRequest
        ),
        schema_doc!(
            "client_device_activation_presentation_request.json",
            crate::auth::ClientDeviceActivationPresentationRequest
        ),
        schema_doc!(
            "client_device_activation_presentation_result.json",
            crate::auth::ClientDeviceActivationPresentationResult
        ),
        schema_doc!(
            "client_device_activation_parse_request.json",
            crate::auth::ClientDeviceActivationParseRequest
        ),
        schema_doc!(
            "client_device_activation_parse_result.json",
            crate::auth::ClientDeviceActivationParseResult
        ),
        schema_doc!(
            "client_invitation_presentation_request.json",
            crate::invitation::ClientInvitationPresentationRequest
        ),
        schema_doc!(
            "client_invitation_presentation_result.json",
            crate::invitation::ClientInvitationPresentationResult
        ),
        schema_doc!(
            "client_active_thread_clear_result.json",
            crate::active_thread::ClientActiveThreadClearResult
        ),
        schema_doc!(
            "client_active_thread_open_request.json",
            crate::active_thread::ClientActiveThreadOpenRequest
        ),
        schema_doc!(
            "client_active_thread_open_by_id_request.json",
            crate::active_thread::ClientActiveThreadOpenByIdRequest
        ),
        schema_doc!(
            "client_ensure_workspace_draft_request.json",
            crate::active_thread::ClientEnsureWorkspaceDraftRequest
        ),
        schema_doc!(
            "client_active_thread_send_text_request.json",
            crate::active_thread::ClientActiveThreadSendTextRequest
        ),
        schema_doc!(
            "client_active_thread_send_text_result.json",
            crate::active_thread::ClientActiveThreadSendTextResult
        ),
        schema_doc!(
            "client_prepare_voice_composer_snapshot_request.json",
            crate::active_thread::ClientPrepareVoiceComposerSnapshotRequest
        ),
        schema_doc!(
            "client_active_thread_snapshot.json",
            crate::active_thread::ClientActiveThreadSnapshot
        ),
        schema_doc!(
            "client_turn_security_summary.json",
            pioneer_client::security::ClientTurnSecuritySummary
        ),
        schema_doc!(
            "client_security_diagnostic_row.json",
            pioneer_client::security::ClientSecurityDiagnosticRow
        ),
        schema_doc!(
            "client_composer_attachment_from_path_request.json",
            crate::composer::ClientComposerAttachmentFromPathRequest
        ),
        schema_doc!(
            "client_composer_capability_target_request.json",
            crate::composer::ClientComposerCapabilityTargetRequest
        ),
        schema_doc!(
            "client_composer_capability_menu_visibility_request.json",
            crate::composer::ClientComposerCapabilityMenuVisibilityRequest
        ),
        schema_doc!(
            "client_composer_submission_plan_request.json",
            crate::composer::ClientComposerSubmissionPlanRequest
        ),
        schema_doc!(
            "client_composer_skill_rows_for_target_request.json",
            crate::composer::ClientComposerSkillRowsForTargetRequest
        ),
        schema_doc!(
            "client_composer_filter_mcp_rows_request.json",
            crate::composer::ClientComposerFilterMcpRowsRequest
        ),
        schema_doc!(
            "client_composer_filter_mcp_rows_result.json",
            crate::composer::ClientComposerFilterMcpRowsResult
        ),
        schema_doc!(
            "client_composer_skill_pack_picker_request.json",
            crate::skills::ClientComposerSkillPackPickerRequest
        ),
        schema_doc!(
            "client_composer_skill_chips_request.json",
            crate::skills::ClientComposerSkillChipsRequest
        ),
        schema_doc!(
            "auth_session_grant.json",
            pioneer_protocol::AuthSessionGrant
        ),
        schema_doc!(
            "auth_refresh_grant.json",
            pioneer_protocol::AuthRefreshGrant
        ),
        schema_doc!("auth_me_response.json", pioneer_protocol::AuthMeResponse),
        schema_doc!(
            "auth_profile_update_params.json",
            pioneer_protocol::AuthProfileUpdateParams
        ),
        schema_doc!(
            "auth_profile_update_response.json",
            pioneer_protocol::AuthProfileUpdateResponse
        ),
        schema_doc!(
            "session_list_row_presentation.json",
            pioneer_client::authorization::SessionListRowPresentation
        ),
        schema_doc!(
            "auth_session_list_response.json",
            pioneer_protocol::AuthSessionListResponse
        ),
        schema_doc!(
            "auth_session_revoke_params.json",
            pioneer_protocol::AuthSessionRevokeParams
        ),
        schema_doc!(
            "auth_session_revoke_response.json",
            pioneer_protocol::AuthSessionRevokeResponse
        ),
        schema_doc!(
            "auth_logout_response.json",
            pioneer_protocol::AuthLogoutResponse
        ),
        schema_doc!(
            "auth_device_create_response.json",
            pioneer_protocol::AuthDeviceCreateResponse
        ),
        schema_doc!(
            "access_changed_plan.json",
            pioneer_client::authorization::AccessChangedPlan
        ),
        schema_doc!(
            "client_transport_reserve_request_dto.json",
            crate::client_binding::ClientTransportReserveRequestDto
        ),
        schema_doc!(
            "client_transport_lease_request_dto.json",
            crate::client_binding::ClientTransportLeaseRequestDto
        ),
        schema_doc!(
            "gateway_session_connection_result.json",
            pioneer_client::gateway::session_connection::GatewaySessionConnectionResult
        ),
        schema_doc!(
            "gateway_settings_get_response.json",
            pioneer_protocol::GatewaySettingsGetResponse
        ),
        schema_doc!(
            "gateway_settings_update_response.json",
            pioneer_protocol::GatewaySettingsUpdateResponse
        ),
        schema_doc!(
            "client_pending_request_presentation_request.json",
            crate::pending_requests::ClientPendingRequestPresentationRequest
        ),
        schema_doc!(
            "client_pending_request_presentation_result.json",
            crate::pending_requests::ClientPendingRequestPresentationResult
        ),
        schema_doc!(
            "pending_request.json",
            pioneer_client::cli_runtime::approvals::PendingRequest
        ),
        schema_doc!(
            "pending_request_kind.json",
            pioneer_client::cli_runtime::approvals::PendingRequestKind
        ),
        schema_doc!(
            "pending_request_action_kind.json",
            pioneer_client::cli_runtime::approvals::PendingRequestActionKind
        ),
        schema_doc!(
            "pending_request_available_action.json",
            pioneer_client::cli_runtime::approvals::PendingRequestAvailableAction
        ),
        schema_doc!(
            "pending_request_detail_row.json",
            pioneer_client::cli_runtime::approvals::PendingRequestDetailRow
        ),
        schema_doc!(
            "pending_request_detail_style.json",
            pioneer_client::cli_runtime::approvals::PendingRequestDetailStyle
        ),
        schema_doc!(
            "pending_request_origin.json",
            pioneer_client::cli_runtime::approvals::PendingRequestOrigin
        ),
        schema_doc!(
            "pending_request_payload.json",
            pioneer_client::cli_runtime::approvals::PendingRequestPayload
        ),
        schema_doc!(
            "pending_request_resolution.json",
            pioneer_client::cli_runtime::approvals::PendingRequestResolution
        ),
        schema_doc!(
            "pending_request_presentation.json",
            pioneer_client::cli_runtime::approvals::PendingRequestPresentation
        ),
        schema_doc!(
            "pending_request_user_input_option.json",
            pioneer_client::cli_runtime::approvals::PendingRequestUserInputOption
        ),
        schema_doc!(
            "pending_request_user_input_question.json",
            pioneer_client::cli_runtime::approvals::PendingRequestUserInputQuestion
        ),
        schema_doc!(
            "turn_permission_approval_request.json",
            pioneer_protocol::TurnPermissionApprovalRequest
        ),
        schema_doc!(
            "turn_permission_approval_resolution.json",
            pioneer_protocol::TurnPermissionApprovalResolution
        ),
        schema_doc!(
            "turn_permission_request_opened_notification.json",
            pioneer_protocol::TurnPermissionRequestOpenedNotification
        ),
        schema_doc!(
            "turn_permission_request_resolved_notification.json",
            pioneer_protocol::TurnPermissionRequestResolvedNotification
        ),
        schema_doc!(
            "turn_permission_request_respond_params.json",
            pioneer_protocol::TurnPermissionRequestRespondParams
        ),
        schema_doc!(
            "turn_permission_request_respond_response.json",
            pioneer_protocol::TurnPermissionRequestRespondResponse
        ),
        schema_doc!(
            "voice_audio_encoding.json",
            pioneer_protocol::VoiceAudioEncoding
        ),
        schema_doc!(
            "voice_audio_format.json",
            pioneer_protocol::VoiceAudioFormat
        ),
        schema_doc!(
            "voice_chunk_ack_notification.json",
            pioneer_protocol::VoiceChunkAckNotification
        ),
        schema_doc!("voice_error.json", pioneer_protocol::VoiceError),
        schema_doc!("voice_error_kind.json", pioneer_protocol::VoiceErrorKind),
        schema_doc!(
            "voice_session_cancel_params.json",
            pioneer_protocol::VoiceSessionCancelParams
        ),
        schema_doc!(
            "voice_session_cancel_response.json",
            pioneer_protocol::VoiceSessionCancelResponse
        ),
        schema_doc!(
            "voice_session_finalize_params.json",
            pioneer_protocol::VoiceSessionFinalizeParams
        ),
        schema_doc!(
            "voice_session_finalize_response.json",
            pioneer_protocol::VoiceSessionFinalizeResponse
        ),
        schema_doc!(
            "voice_session_outcome.json",
            pioneer_protocol::VoiceSessionOutcome
        ),
        schema_doc!(
            "voice_session_result_notification.json",
            pioneer_protocol::VoiceSessionResultNotification
        ),
        schema_doc!(
            "voice_session_result_reduction.json",
            pioneer_client::voice::VoiceSessionResultReduction
        ),
        schema_doc!(
            "voice_session_start_params.json",
            pioneer_protocol::VoiceSessionStartParams
        ),
        schema_doc!(
            "voice_session_start_response.json",
            pioneer_protocol::VoiceSessionStartResponse
        ),
        schema_doc!("voice_status.json", pioneer_protocol::VoiceStatus),
        schema_doc!(
            "voice_status_params.json",
            pioneer_protocol::VoiceStatusParams
        ),
        schema_doc!(
            "voice_status_response.json",
            pioneer_protocol::VoiceStatusResponse
        ),
        schema_doc!(
            "voice_turn_context.json",
            pioneer_protocol::VoiceTurnContext
        ),
        schema_doc!(
            "thread_agents_doc_archive_params.json",
            pioneer_protocol::ThreadAgentsDocArchiveParams
        ),
        schema_doc!(
            "thread_agents_doc_archive_response.json",
            pioneer_protocol::ThreadAgentsDocArchiveResponse
        ),
        schema_doc!(
            "thread_agents_doc_get_params.json",
            pioneer_protocol::ThreadAgentsDocGetParams
        ),
        schema_doc!(
            "thread_agents_doc_get_response.json",
            pioneer_protocol::ThreadAgentsDocGetResponse
        ),
        schema_doc!(
            "thread_agents_doc_payload.json",
            pioneer_protocol::ThreadAgentsDocPayload
        ),
        schema_doc!(
            "thread_agents_doc_resolved_payload.json",
            pioneer_protocol::ThreadAgentsDocResolvedPayload
        ),
        schema_doc!(
            "thread_agents_doc_save_params.json",
            pioneer_protocol::ThreadAgentsDocSaveParams
        ),
        schema_doc!(
            "thread_agents_doc_save_reason.json",
            pioneer_protocol::ThreadAgentsDocSaveReason
        ),
        schema_doc!(
            "thread_agents_doc_save_response.json",
            pioneer_protocol::ThreadAgentsDocSaveResponse
        ),
        schema_doc!(
            "thread_agents_doc_status.json",
            pioneer_protocol::ThreadAgentsDocStatus
        ),
        schema_doc!(
            "load_gateway_registry_request.json",
            crate::gateway::LoadGatewayRegistryRequest
        ),
        schema_doc!(
            "load_gateway_registry_result.json",
            crate::gateway::LoadGatewayRegistryResult
        ),
        schema_doc!(
            "thread_tree_level.json",
            crate::threads::ClientThreadTreeLevel
        ),
        schema_doc!(
            "thread_tree_level_request.json",
            crate::threads::ThreadTreeLevelRequest
        ),
        schema_doc!(
            "thread_tree_query_data.json",
            crate::threads::ClientThreadTreeQueryData
        ),
        schema_doc!(
            "thread_tree_refresh_request.json",
            crate::threads::ThreadTreeRefreshRequest
        ),
        schema_doc!(
            "thread_tree_snapshot.json",
            crate::threads::ClientThreadTreeSnapshot
        ),
        schema_doc!(
            "thread_timeline_page_params.json",
            pioneer_protocol::ThreadTimelinePageParams
        ),
        schema_doc!(
            "thread_timeline_page_response.json",
            pioneer_protocol::ThreadTimelinePageResponse
        ),
        schema_doc!(
            "thread_read_params.json",
            pioneer_protocol::ThreadReadParams
        ),
        schema_doc!(
            "thread_read_response.json",
            pioneer_protocol::ThreadReadResponse
        ),
        schema_doc!("timeline_block.json", pioneer_protocol::TimelineBlock),
        schema_doc!(
            "timeline_block_kind.json",
            pioneer_protocol::TimelineBlockKind
        ),
        schema_doc!("timeline_cursor.json", pioneer_protocol::TimelineCursor),
        schema_doc!(
            "timeline_page_anchor.json",
            pioneer_protocol::TimelinePageAnchor
        ),
        schema_doc!(
            "timeline_page_info.json",
            pioneer_protocol::TimelinePageInfo
        ),
        schema_doc!("turn_work_block.json", pioneer_protocol::TurnWorkBlock),
        schema_doc!(
            "turn_message_revisions_page_response.json",
            pioneer_protocol::TurnMessageRevisionsPageResponse
        ),
        schema_doc!("turn_work_item.json", pioneer_protocol::TurnWorkItem),
        schema_doc!(
            "turn_work_item_status.json",
            pioneer_protocol::TurnWorkItemStatus
        ),
        schema_doc!(
            "turn_work_page_params.json",
            pioneer_protocol::TurnWorkPageParams
        ),
        schema_doc!(
            "turn_work_page_response.json",
            pioneer_protocol::TurnWorkPageResponse
        ),
        schema_doc!(
            "turn_work_items_get_params.json",
            pioneer_protocol::TurnWorkItemsGetParams
        ),
        schema_doc!(
            "turn_work_items_get_response.json",
            pioneer_protocol::TurnWorkItemsGetResponse
        ),
        schema_doc!(
            "turn_work_presentation.json",
            pioneer_protocol::TurnWorkPresentation
        ),
        schema_doc!("turn_work_state.json", pioneer_protocol::TurnWorkState),
        schema_doc!(
            "workspace_create_request.json",
            crate::workspaces::WorkspaceCreateRequest
        ),
        schema_doc!(
            "workspace_create_result.json",
            crate::workspaces::WorkspaceCreateResult
        ),
        schema_doc!(
            "workspace_rename_request.json",
            crate::workspaces::WorkspaceRenameRequest
        ),
        schema_doc!(
            "workspace_rename_result.json",
            crate::workspaces::WorkspaceRenameResult
        ),
        schema_doc!(
            "workspace_switch_request.json",
            crate::workspaces::WorkspaceSwitchRequest
        ),
        schema_doc!(
            "workspace_switch_result.json",
            crate::workspaces::WorkspaceSwitchResult
        ),
    ];

    documents.sort_by(|left, right| left.file_name.cmp(right.file_name));
    documents
}

pub fn write_client_ffi_schemas(
    output_directory: impl AsRef<Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_directory = output_directory.as_ref();
    fs::create_dir_all(output_directory)?;

    for document in client_ffi_schema_documents() {
        let schema_json = serde_json::to_string_pretty(&document.schema)?;
        let path = output_directory.join(document.file_name);
        fs::write(path, schema_json)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scoped_boundary_schemas_are_complete_and_serializable() {
        let documents = client_ffi_schema_documents();
        for name in [
            "client_intent_dispatch_dto.json",
            "client_scope_lease_request_dto.json",
            "client_scoped_snapshot_request_dto.json",
            "client_scoped_snapshot_dto.json",
            "client_change_batch_request_dto.json",
            "client_change_batch_dto.json",
            "client_publication_wait_request_dto.json",
            "client_process_change_batch_dto.json",
            "client_effect_completion_dto.json",
            "client_effect_cancellation_dto.json",
            "client_sequence_gap_resnapshot_dto.json",
            "client_transition_dto.json",
            "client_gateway_session_validation_request.json",
            "client_gateway_session_validation_result.json",
            "client_composer_skill_chips_request.json",
            "client_composer_skill_pack_picker_request.json",
        ] {
            let schema = &documents
                .iter()
                .find(|document| document.file_name == name)
                .unwrap_or_else(|| panic!("missing {name}"))
                .schema;
            assert!(schema.as_value().is_object());
            serde_json::to_string(schema).unwrap();
        }
        let mut names = std::collections::HashSet::new();
        for document in &documents {
            assert!(
                names.insert(document.file_name),
                "duplicate schema {}",
                document.file_name
            );
        }
    }
    #[test]
    fn retired_raw_ingress_and_shell_reducer_contracts_are_absent() {
        let documents = client_ffi_schema_documents();
        for name in [
            "client_event.json",
            "client_active_thread_event_request.json",
            "client_access_change_plan_request_dto.json",
            "client_gateway_session_lifecycle_request.json",
            "client_gateway_session_ensure_request.json",
            "client_gateway_session_control_request.json",
            "client_composer_domain_transition_request.json",
            "client_composer_draft_lifecycle_transition_request.json",
            "client_authorization_projection_accept_request.json",
            "client_invitation_commit_request.json",
            "client_auth_refresh_request.json",
            "client_gateway_session_replace_access_request.json",
        ] {
            assert!(
                !documents.iter().any(|document| document.file_name == name),
                "retired schema {name}"
            );
        }
    }
    #[test]
    fn publications_are_secret_free_and_thread_output_contains_no_semantic_state() {
        let documents = client_ffi_schema_documents();
        for name in [
            "client_process_change_batch_dto.json",
            "client_active_thread_snapshot.json",
            "timeline_snapshot.json",
        ] {
            let mut value = documents
                .iter()
                .find(|document| document.file_name == name)
                .unwrap()
                .schema
                .as_value()
                .clone();
            // The process batch carries addressed native SecureStore effects.
            // Credentials are permitted only in that existing storage envelope.
            if name == "client_process_change_batch_dto.json" {
                let definitions = value.get_mut("$defs").unwrap().as_object_mut().unwrap();
                let storage = definitions
                    .remove("GatewaySessionEnvelope")
                    .expect("typed storage effect envelope");
                assert!(storage["properties"]["refresh_token"].is_object());
            }
            let schema = serde_json::to_string(&value).unwrap();
            for forbidden in [
                "access_token",
                "refresh_token",
                "authorization_header",
                "activation_code",
                "authorization_proof",
            ] {
                assert!(!schema.contains(forbidden), "{name} exposes {forbidden}");
            }
            if name == "client_active_thread_snapshot.json" {
                assert!(value["properties"].get("semantic").is_none());
                assert!(value["properties"].get("timeline_rows").is_none());
            }
        }
    }
}
