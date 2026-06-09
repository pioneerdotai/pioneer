//! Client-owned schema export boundary.
//!
//! This module defines which client-owned DTOs are allowed to become schema
//! documents. Public DTOs in this manifest derive `JsonSchema` and are exported
//! as deterministic schema files for shell integration.

pub use crate::contracts::export::{
    ClientContractDomain, ClientContractKind, ClientContractStability, ClientContractType,
    ClientSchemaInternalExclusion, client_contract_types, client_schema_internal_exclusions,
};

#[cfg(any(feature = "schema", test))]
use schemars::{Schema, schema_for};
#[cfg(any(feature = "schema", test))]
use std::{fs, path::Path};

#[cfg(any(feature = "schema", test))]
pub struct SchemaDocument {
    pub file_name: &'static str,
    pub schema: Schema,
}

#[cfg(any(feature = "schema", test))]
macro_rules! schema_doc {
    ($file_name:literal, $ty:ty) => {
        SchemaDocument {
            file_name: $file_name,
            schema: schema_for!($ty),
        }
    };
}

pub fn public_client_schema_contracts() -> Vec<ClientContractType> {
    client_contract_types()
}

pub fn internal_client_schema_exclusions() -> Vec<ClientSchemaInternalExclusion> {
    client_schema_internal_exclusions()
}

#[cfg(any(feature = "schema", test))]
pub fn client_schema_documents() -> Vec<SchemaDocument> {
    let mut documents = vec![
        schema_doc!(
            "active_thread_phase_snapshot.json",
            crate::state::snapshot::ActiveThreadPhaseSnapshot
        ),
        schema_doc!(
            "active_thread_snapshot.json",
            crate::state::snapshot::ActiveThreadSnapshot
        ),
        schema_doc!(
            "active_thread_status_snapshot.json",
            crate::state::snapshot::ActiveThreadStatusSnapshot
        ),
        schema_doc!(
            "artifact_action_status.json",
            crate::artifacts::actions::ArtifactActionStatus
        ),
        schema_doc!(
            "artifact_download_request_plan_error.json",
            crate::artifacts::actions::ArtifactDownloadRequestPlanError
        ),
        schema_doc!(
            "artifact_download_request.json",
            crate::artifacts::download::ArtifactDownloadRequest
        ),
        schema_doc!(
            "artifact_download_result.json",
            crate::artifacts::download::ArtifactDownloadResult
        ),
        schema_doc!(
            "artifact_file_action_block_reason.json",
            crate::artifacts::actions::ArtifactFileActionBlockReason
        ),
        schema_doc!(
            "artifact_binding_target_kind.json",
            crate::artifacts::presentation::ArtifactBindingTargetKind
        ),
        schema_doc!(
            "artifact_binding_target_part.json",
            crate::artifacts::presentation::ArtifactBindingTargetPart
        ),
        schema_doc!(
            "artifact_upload_file_request.json",
            crate::artifacts::upload::ArtifactUploadFileRequest
        ),
        schema_doc!(
            "artifact_version_key.json",
            crate::artifacts::actions::ArtifactVersionKey
        ),
        schema_doc!(
            "capability_rejection_row.json",
            crate::timeline::labels::CapabilityRejectionRow
        ),
        schema_doc!("client_command.json", crate::contracts::ClientCommand),
        schema_doc!(
            "client_effect.json",
            crate::notifications::effects::ClientEffect
        ),
        schema_doc!(
            "client_error_event.json",
            crate::contracts::ClientErrorEvent
        ),
        schema_doc!("client_event.json", crate::contracts::ClientEvent),
        schema_doc!(
            "client_gateway_connect_request.json",
            crate::contracts::ClientGatewayConnectRequest
        ),
        schema_doc!(
            "client_gateway_connect_result.json",
            crate::contracts::ClientGatewayConnectResult
        ),
        schema_doc!(
            "client_gateway_ws_timings.json",
            crate::contracts::ClientGatewayWsTimings
        ),
        schema_doc!(
            "client_snapshot.json",
            crate::state::snapshot::ClientSnapshot
        ),
        schema_doc!(
            "composer_attachment.json",
            crate::composer::attachments::ComposerAttachment
        ),
        schema_doc!(
            "composer_attachment_kind.json",
            crate::composer::attachments::ComposerAttachmentKind
        ),
        schema_doc!(
            "composer_attachment_upload_state.json",
            crate::composer::attachments::ComposerAttachmentUploadState
        ),
        schema_doc!(
            "composer_capability.json",
            crate::composer::capabilities::ComposerCapability
        ),
        schema_doc!(
            "composer_capability_kind.json",
            crate::composer::capabilities::ComposerCapabilityKind
        ),
        schema_doc!(
            "composer_draft.json",
            crate::composer::draft::ComposerDraft<
                crate::composer::attachments::ComposerAttachment,
                crate::composer::capabilities::ComposerCapability,
            >
        ),
        schema_doc!(
            "composer_model_selection.json",
            crate::composer::model_selection::ComposerModelSelection
        ),
        schema_doc!(
            "composer_model_selection_candidate.json",
            crate::composer::model_selection::ComposerModelSelectionCandidate
        ),
        schema_doc!(
            "gateway_connection_state.json",
            crate::state::client_state::GatewayConnectionState
        ),
        schema_doc!(
            "gateway_endpoint.json",
            crate::gateway::types::GatewayEndpoint
        ),
        schema_doc!(
            "gateway_endpoint_kind.json",
            crate::gateway::types::GatewayEndpointKind
        ),
        schema_doc!(
            "gateway_registry.json",
            crate::gateway::types::GatewayRegistry
        ),
        schema_doc!(
            "gateway_auth_token_write.json",
            crate::gateway::setup::GatewayAuthTokenWrite
        ),
        schema_doc!(
            "remote_gateway_validation_request.json",
            crate::gateway::setup::RemoteGatewayValidationRequest
        ),
        schema_doc!(
            "remote_gateway_validation.json",
            crate::gateway::setup::RemoteGatewayValidation
        ),
        schema_doc!(
            "plan_add_remote_gateway_request.json",
            crate::gateway::setup::PlanAddRemoteGatewayRequest
        ),
        schema_doc!(
            "add_remote_gateway_plan.json",
            crate::gateway::setup::AddRemoteGatewayPlan
        ),
        schema_doc!(
            "add_and_activate_remote_gateway_registry_plan.json",
            crate::gateway::setup::AddAndActivateRemoteGatewayRegistryPlan
        ),
        schema_doc!(
            "plan_activate_gateway_request.json",
            crate::gateway::setup::PlanActivateGatewayRequest
        ),
        schema_doc!(
            "activate_gateway_registry_plan.json",
            crate::gateway::setup::ActivateGatewayRegistryPlan
        ),
        schema_doc!(
            "gateway_auth_token_update.json",
            crate::gateway::setup::GatewayAuthTokenUpdate
        ),
        schema_doc!(
            "plan_update_remote_gateway_request.json",
            crate::gateway::setup::PlanUpdateRemoteGatewayRequest
        ),
        schema_doc!(
            "update_remote_gateway_registry_plan.json",
            crate::gateway::setup::UpdateRemoteGatewayRegistryPlan
        ),
        schema_doc!(
            "plan_delete_remote_gateway_request.json",
            crate::gateway::setup::PlanDeleteRemoteGatewayRequest
        ),
        schema_doc!(
            "delete_remote_gateway_registry_plan.json",
            crate::gateway::setup::DeleteRemoteGatewayRegistryPlan
        ),
        schema_doc!(
            "gateway_settings_state.json",
            crate::settings::gateway::GatewaySettingsState
        ),
        schema_doc!(
            "gateway_settings_action_scope.json",
            crate::settings::gateway::GatewaySettingsActionScope
        ),
        schema_doc!(
            "gateway_settings_refresh_plan.json",
            crate::settings::gateway::GatewaySettingsRefreshPlan
        ),
        schema_doc!(
            "gateway_settings_refresh_unavailable.json",
            crate::settings::gateway::GatewaySettingsRefreshUnavailable
        ),
        schema_doc!(
            "gateway_settings_update_plan.json",
            crate::settings::gateway::GatewaySettingsUpdatePlan
        ),
        schema_doc!(
            "gateway_status_endpoint.json",
            crate::state::reducers::GatewayStatusEndpoint
        ),
        schema_doc!(
            "gateway_status_level.json",
            crate::state::client_state::GatewayStatusLevel
        ),
        schema_doc!(
            "gateway_status_message.json",
            crate::state::reducers::GatewayStatusMessage
        ),
        schema_doc!(
            "gateway_status_projection.json",
            crate::state::reducers::GatewayStatusProjection
        ),
        schema_doc!(
            "gateway_status_text_update.json",
            crate::state::reducers::GatewayStatusTextUpdate
        ),
        schema_doc!(
            "gateway_ws_event.json",
            crate::transport::ws::GatewayWsEvent
        ),
        schema_doc!(
            "mcp_audit_action.json",
            crate::mcp::presentation::McpAuditAction
        ),
        schema_doc!(
            "mcp_audit_decision.json",
            crate::mcp::presentation::McpAuditDecision
        ),
        schema_doc!(
            "mcp_audit_details_summary.json",
            crate::mcp::presentation::McpAuditDetailsSummary
        ),
        schema_doc!("mcp_audit_row.json", crate::mcp::presentation::McpAuditRow),
        schema_doc!(
            "mcp_capability_count.json",
            crate::mcp::presentation::McpCapabilityCount
        ),
        schema_doc!(
            "mcp_capability_kind.json",
            crate::mcp::presentation::McpCapabilityKind
        ),
        schema_doc!(
            "mcp_capability_selection_toggle.json",
            crate::composer::capabilities::McpCapabilitySelectionToggle
        ),
        schema_doc!(
            "mcp_capability_unavailable_reason.json",
            crate::composer::capabilities::McpCapabilityUnavailableReason
        ),
        schema_doc!(
            "mcp_config_validation_error.json",
            crate::mcp::actions::McpConfigValidationError
        ),
        schema_doc!(
            "mcp_detail_meta_kind.json",
            crate::mcp::presentation::McpDetailMetaKind
        ),
        schema_doc!(
            "mcp_detail_meta_row.json",
            crate::mcp::presentation::McpDetailMetaRow
        ),
        schema_doc!(
            "mcp_detail_value.json",
            crate::mcp::presentation::McpDetailValue
        ),
        schema_doc!(
            "mcp_install_field_error.json",
            crate::mcp::actions::McpInstallFieldError
        ),
        schema_doc!(
            "mcp_install_field_issue.json",
            crate::mcp::actions::McpInstallFieldIssue
        ),
        schema_doc!(
            "mcp_json_value_preview.json",
            crate::mcp::presentation::McpJsonValuePreview
        ),
        schema_doc!("mcp_list_state.json", crate::mcp::list::McpListState),
        schema_doc!(
            "mcp_presentation_tone.json",
            crate::mcp::presentation::McpPresentationTone
        ),
        schema_doc!(
            "mcp_scope_label.json",
            crate::mcp::presentation::McpScopeLabel
        ),
        schema_doc!(
            "mcp_source_label.json",
            crate::mcp::presentation::McpSourceLabel
        ),
        schema_doc!(
            "mcp_status_label.json",
            crate::mcp::presentation::McpStatusLabel
        ),
        schema_doc!(
            "mcp_timeline_metadata.json",
            crate::timeline::labels::McpTimelineMetadata
        ),
        schema_doc!(
            "mcp_transport_presentation.json",
            crate::mcp::presentation::McpTransportPresentation
        ),
        schema_doc!(
            "memory_model_setting.json",
            crate::settings::memory::MemoryModelSetting
        ),
        schema_doc!(
            "memory_setting_toggle.json",
            crate::settings::memory::MemorySettingToggle
        ),
        schema_doc!(
            "model_provider_selection_update.json",
            crate::composer::model_selection::ModelProviderSelectionUpdate
        ),
        schema_doc!(
            "model_selector_selection.json",
            crate::composer::model_selection::ModelSelectorSelection
        ),
        schema_doc!(
            "parsed_user_attachment.json",
            crate::timeline::labels::ParsedUserAttachment
        ),
        schema_doc!(
            "parsed_user_attachment_kind.json",
            crate::timeline::labels::ParsedUserAttachmentKind
        ),
        schema_doc!(
            "prepared_composer_attachment.json",
            crate::composer::turn_prepare::PreparedComposerAttachment
        ),
        schema_doc!(
            "prepare_composer_turn_request.json",
            crate::composer::turn_prepare::PrepareComposerTurnRequest
        ),
        schema_doc!(
            "prepared_composer_turn.json",
            crate::composer::turn_prepare::PreparedComposerTurn
        ),
        schema_doc!(
            "provider_api_key_action_unavailable.json",
            crate::providers::actions::ProviderApiKeyActionUnavailable
        ),
        schema_doc!(
            "provider_delete_api_key_action_request.json",
            crate::providers::actions::ProviderDeleteApiKeyActionRequest
        ),
        schema_doc!(
            "provider_delete_api_key_plan.json",
            crate::providers::actions::ProviderDeleteApiKeyPlan
        ),
        schema_doc!(
            "provider_filter.json",
            crate::providers::selectors::ProviderFilter
        ),
        schema_doc!(
            "provider_list_refresh_plan.json",
            crate::providers::list::ProviderListRefreshPlan
        ),
        schema_doc!(
            "provider_list_refresh_request.json",
            crate::providers::list::ProviderListRefreshRequest
        ),
        schema_doc!(
            "provider_list_refresh_unavailable.json",
            crate::providers::list::ProviderListRefreshUnavailable
        ),
        schema_doc!(
            "provider_set_api_key_action_request.json",
            crate::providers::actions::ProviderSetApiKeyActionRequest
        ),
        schema_doc!(
            "provider_set_api_key_plan.json",
            crate::providers::actions::ProviderSetApiKeyPlan
        ),
        schema_doc!(
            "reconciled_skills_snapshot.json",
            crate::skills::catalog::ReconciledSkillsSnapshot
        ),
        schema_doc!(
            "selectable_mcp_capability.json",
            crate::composer::capabilities::SelectableMcpCapability
        ),
        schema_doc!(
            "selectable_skill_capability.json",
            crate::composer::capabilities::SelectableSkillCapability
        ),
        schema_doc!(
            "skill_audit_action.json",
            crate::skills::presentation::SkillAuditAction
        ),
        schema_doc!(
            "skill_audit_decision.json",
            crate::skills::presentation::SkillAuditDecision
        ),
        schema_doc!(
            "skill_audit_details_summary.json",
            crate::skills::presentation::SkillAuditDetailsSummary
        ),
        schema_doc!(
            "skill_audit_row.json",
            crate::skills::presentation::SkillAuditRow
        ),
        schema_doc!(
            "skill_capability_unavailable_reason.json",
            crate::composer::capabilities::SkillCapabilityUnavailableReason
        ),
        schema_doc!(
            "skill_catalog_state.json",
            crate::skills::catalog::SkillCatalogState
        ),
        schema_doc!(
            "skill_dependency_card.json",
            crate::skills::presentation::SkillDependencyCard
        ),
        schema_doc!(
            "skill_dependency_kind.json",
            crate::skills::presentation::SkillDependencyKind
        ),
        schema_doc!(
            "skill_dependency_status.json",
            crate::skills::presentation::SkillDependencyStatus
        ),
        schema_doc!(
            "skill_detail_diagnostics.json",
            crate::skills::presentation::SkillDetailDiagnostics
        ),
        schema_doc!(
            "skill_diagnostics_table_cell.json",
            crate::skills::presentation::SkillDiagnosticsTableCell
        ),
        schema_doc!(
            "skill_diagnostics_table_row.json",
            crate::skills::presentation::SkillDiagnosticsTableRow
        ),
        schema_doc!(
            "skill_diagnostics_tone.json",
            crate::skills::presentation::SkillDiagnosticsTone
        ),
        schema_doc!(
            "skill_json_value_preview.json",
            crate::skills::presentation::SkillJsonValuePreview
        ),
        schema_doc!(
            "skill_security_card.json",
            crate::skills::presentation::SkillSecurityCard
        ),
        schema_doc!(
            "skill_security_severity.json",
            crate::skills::presentation::SkillSecuritySeverity
        ),
        schema_doc!(
            "skill_slug_presentation.json",
            crate::skills::presentation::SkillSlugPresentation
        ),
        schema_doc!(
            "skill_source_kind.json",
            crate::skills::presentation::SkillSourceKind
        ),
        schema_doc!(
            "skill_status.json",
            crate::skills::presentation::SkillStatus
        ),
        schema_doc!(
            "skill_summary_presentation.json",
            crate::skills::presentation::SkillSummaryPresentation
        ),
        schema_doc!(
            "skill_trust_gate_card.json",
            crate::skills::presentation::SkillTrustGateCard
        ),
        schema_doc!(
            "skill_trust_gate_decision.json",
            crate::skills::presentation::SkillTrustGateDecision
        ),
        schema_doc!(
            "skill_trust_gate_tool_kind.json",
            crate::skills::presentation::SkillTrustGateToolKind
        ),
        schema_doc!(
            "skill_trust_level.json",
            crate::skills::presentation::SkillTrustLevel
        ),
        schema_doc!(
            "skills_catalog_snapshot.json",
            crate::skills::catalog::SkillsCatalogSnapshot
        ),
        schema_doc!(
            "skills_catalog_split.json",
            crate::skills::catalog::SkillsCatalogSplit
        ),
        schema_doc!(
            "system_event_detail_row.json",
            crate::timeline::labels::SystemEventDetailRow
        ),
        schema_doc!(
            "system_event_presentation.json",
            crate::timeline::labels::SystemEventPresentation
        ),
        schema_doc!(
            "task_review_action.json",
            crate::tasks::review::TaskReviewAction
        ),
        schema_doc!(
            "task_review_action_state.json",
            crate::tasks::review::TaskReviewActionState
        ),
        schema_doc!(
            "task_review_plan_error.json",
            crate::tasks::review::TaskReviewPlanError
        ),
        schema_doc!(
            "task_wait_review_display.json",
            crate::timeline::labels::TaskWaitReviewDisplay
        ),
        schema_doc!(
            "task_wait_review_display_item.json",
            crate::timeline::labels::TaskWaitReviewDisplayItem
        ),
        schema_doc!(
            "thread_artifact_cache_entry.json",
            crate::artifacts::state::ThreadArtifactCacheEntry
        ),
        schema_doc!(
            "thread_artifact_filter.json",
            crate::artifacts::state::ThreadArtifactFilter
        ),
        schema_doc!(
            "thread_list_snapshot.json",
            crate::state::snapshot::ThreadListSnapshot
        ),
        schema_doc!(
            "timeline_coalesced_tools_kind.json",
            crate::timeline::rows::TimelineCoalescedToolsKind
        ),
        schema_doc!(
            "timeline_coalesced_tools_row.json",
            crate::timeline::rows::TimelineCoalescedToolsRow
        ),
        schema_doc!(
            "timeline_final_status.json",
            crate::timeline::labels::TimelineFinalStatus
        ),
        schema_doc!(
            "timeline_final_status_kind.json",
            crate::timeline::labels::TimelineFinalStatusKind
        ),
        schema_doc!("timeline_row.json", crate::timeline::rows::TimelineRow),
        schema_doc!(
            "timeline_row_kind.json",
            crate::timeline::rows::TimelineRowKind
        ),
        schema_doc!(
            "turn_work_group_row.json",
            crate::timeline::rows::TurnWorkGroupRow
        ),
        schema_doc!(
            "workspace_snapshot.json",
            crate::state::snapshot::WorkspaceSnapshot
        ),
    ];

    documents.sort_by(|left, right| left.file_name.cmp(right.file_name));
    documents
}

#[cfg(any(feature = "schema", test))]
pub fn write_client_schemas(
    output_directory: impl AsRef<Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_directory = output_directory.as_ref();
    fs::create_dir_all(output_directory)?;

    for document in client_schema_documents() {
        let schema_json = serde_json::to_string_pretty(&document.schema)?;
        let path = output_directory.join(document.file_name);
        fs::write(path, schema_json)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as JsonValue;
    use std::fs;

    #[test]
    fn schema_documents_match_public_contract_boundary() {
        let contract_files = client_contract_types()
            .into_iter()
            .map(|contract| contract.file_name)
            .collect::<Vec<_>>();
        let schema_files = client_schema_documents()
            .into_iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();

        assert_eq!(schema_files, contract_files);
    }

    #[test]
    fn schema_documents_are_sorted_unique_and_serializable() {
        let documents = client_schema_documents();
        let file_names = documents
            .iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();
        let mut sorted = file_names.clone();
        sorted.sort_unstable();
        sorted.dedup();

        assert_eq!(file_names, sorted);

        for document in documents {
            let schema_json =
                serde_json::to_string_pretty(&document.schema).expect("schema serializes");
            let schema_value: JsonValue =
                serde_json::from_str(&schema_json).expect("schema JSON parses");

            assert!(
                schema_value.is_object(),
                "{} should serialize to a JSON object",
                document.file_name
            );
        }
    }

    #[test]
    fn schema_export_is_reproducible_across_directories() {
        let left = tempfile::tempdir().expect("left schema tempdir");
        let right = tempfile::tempdir().expect("right schema tempdir");

        write_client_schemas(left.path()).expect("left schema export");
        write_client_schemas(right.path()).expect("right schema export");

        let file_names = client_schema_documents()
            .into_iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();

        for file_name in file_names {
            let left_bytes = fs::read(left.path().join(file_name)).expect("left schema file");
            let right_bytes = fs::read(right.path().join(file_name)).expect("right schema file");

            assert_eq!(
                left_bytes, right_bytes,
                "{file_name} changed between exports"
            );
        }
    }
}
