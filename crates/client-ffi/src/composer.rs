use pioneer_client::composer::turn_prepare::{
    mark_pending_composer_attachments_uploading, mark_uploading_composer_attachments_failed,
};
use pioneer_client::{
    composer::{
        attachments::{
            ComposerAttachment, ComposerAttachmentKind, composer_attachment_from_path,
            composer_attachment_has_path, remove_composer_attachment_at,
        },
        capabilities::{
            ComposerCapability, ComposerCapabilityMenuVisibility, ComposerCapabilityTarget,
            ComposerSubmissionPlan, SelectableMcpCapability, SelectableSkillCapability,
            add_composer_capability, composer_capability_menu_visibility,
            composer_capability_target_for_provider, filter_search_mcp_tool_capability_rows,
            filter_selectable_mcp_capability_rows, filter_selectable_skill_capabilities_for_target,
            filter_selectable_skill_capability_rows, mcp_row_to_composer_capability,
            plan_composer_submission, reduce_composer_mcp_server_picker_rows_response,
            reduce_composer_mcp_tool_picker_rows_response,
            reduce_composer_skill_picker_rows_response, remove_composer_capability_at,
            replace_selected_mcp_composer_capabilities, selected_mcp_server_ids,
            toggle_mcp_capability_selection, toggle_selected_capability_key,
        },
        draft::{
            ComposerDraftLifecycleAction, ComposerDraftLifecycleState,
            ComposerDraftLifecycleTransition, reduce_composer_draft_lifecycle,
        },
        state_machine::{
            ComposerDomainAction, ComposerDomainState, ComposerDomainTransition,
            reduce_composer_domain_state,
        },
    },
    mcp::{details as mcp_details, list as mcp_list},
    skills::catalog as skill_catalog,
    transport::ws::GatewayWsCommandSender,
};
use pioneer_protocol::RuntimeSummary;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, path::Path};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerAttachmentFromPathRequest {
    pub path: String,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub kind: Option<ComposerAttachmentKind>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerAttachmentsUpdateRequest {
    pub attachments: Vec<ComposerAttachment>,
    pub action: ClientComposerAttachmentsUpdateAction,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub enum ClientComposerAttachmentsUpdateAction {
    Add { attachment: ComposerAttachment },
    RemoveAt { index: usize },
    MarkPendingUploading,
    MarkUploadingFailed { error: String },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerSkillPickerRowsRequest {
    pub workspace_id: String,
    #[serde(default)]
    pub query: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerMcpPickerRowsRequest {
    pub workspace_id: String,
    #[serde(default)]
    pub query: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientComposerMcpPickerRowsResult {
    pub server_rows: Vec<SelectableMcpCapability>,
    pub tool_rows: Vec<SelectableMcpCapability>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerCapabilitiesUpdateRequest {
    pub capabilities: Vec<ComposerCapability>,
    pub action: ClientComposerCapabilitiesUpdateAction,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerCapabilityTargetRequest {
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub runtimes: Vec<RuntimeSummary>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerCapabilityMenuVisibilityRequest {
    pub target: ComposerCapabilityTarget,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerSubmissionPlanRequest {
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub has_attachments: bool,
    #[serde(default)]
    pub capabilities: Vec<ComposerCapability>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerDomainTransitionRequest {
    pub state: ComposerDomainState,
    pub action: ComposerDomainAction,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerDraftLifecycleTransitionRequest {
    pub state: ComposerDraftLifecycleState,
    pub action: ComposerDraftLifecycleAction,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerSkillRowsForTargetRequest {
    #[serde(default)]
    pub rows: Vec<SelectableSkillCapability>,
    pub target: ComposerCapabilityTarget,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub enum ClientComposerCapabilitiesUpdateAction {
    Add { capability: ComposerCapability },
    Remove { id: String },
    RemoveAt { index: usize },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerSkillCapabilityFromRowRequest {
    pub row: SelectableSkillCapability,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerMcpCapabilityFromRowRequest {
    pub row: SelectableMcpCapability,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerMcpToggleRequest {
    pub capabilities: Vec<ComposerCapability>,
    pub selected_keys: Vec<String>,
    pub server_rows: Vec<SelectableMcpCapability>,
    pub tool_rows: Vec<SelectableMcpCapability>,
    pub row: SelectableMcpCapability,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientComposerMcpToggleResult {
    pub capabilities: Vec<ComposerCapability>,
    pub selected_keys: Vec<String>,
    pub collapse_active_server: bool,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerSkillToggleRequest {
    pub selected_keys: Vec<String>,
    pub key: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientComposerSkillToggleResult {
    pub selected_keys: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerFilterSkillRowsRequest {
    pub rows: Vec<SelectableSkillCapability>,
    #[serde(default)]
    pub query: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerFilterMcpRowsRequest {
    pub server_rows: Vec<SelectableMcpCapability>,
    pub tool_rows: Vec<SelectableMcpCapability>,
    pub selected_keys: Vec<String>,
    #[serde(default)]
    pub active_server_id: Option<String>,
    #[serde(default)]
    pub query: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientComposerFilterMcpRowsResult {
    pub server_rows: Vec<SelectableMcpCapability>,
    pub tool_rows: Vec<SelectableMcpCapability>,
    pub has_query: bool,
}

pub fn composer_attachment_from_path_request(
    request: ClientComposerAttachmentFromPathRequest,
) -> anyhow::Result<ComposerAttachment> {
    let path = normalize_client_file_reference(request.path.as_str())?;
    let mut attachment = composer_attachment_from_path(Path::new(path.as_str()))
        .ok_or_else(|| anyhow::anyhow!("attachment path is required"))?;
    if let Some(file_name) = request.file_name.and_then(non_empty_string) {
        attachment.file_name = file_name;
    }
    if let Some(kind) = request.kind {
        attachment.kind = kind;
    }
    Ok(attachment)
}

pub fn update_composer_attachments(
    mut request: ClientComposerAttachmentsUpdateRequest,
) -> Vec<ComposerAttachment> {
    match request.action {
        ClientComposerAttachmentsUpdateAction::Add { attachment } => {
            if !composer_attachment_has_path(
                request.attachments.as_slice(),
                attachment.path.as_str(),
            ) {
                request.attachments.push(attachment);
            }
        }
        ClientComposerAttachmentsUpdateAction::RemoveAt { index } => {
            remove_composer_attachment_at(&mut request.attachments, index);
        }
        ClientComposerAttachmentsUpdateAction::MarkPendingUploading => {
            mark_pending_composer_attachments_uploading(&mut request.attachments);
        }
        ClientComposerAttachmentsUpdateAction::MarkUploadingFailed { error } => {
            mark_uploading_composer_attachments_failed(&mut request.attachments, error);
        }
    }
    request.attachments
}

pub fn composer_skill_picker_rows(
    ws_sender: &GatewayWsCommandSender,
    request: ClientComposerSkillPickerRowsRequest,
) -> anyhow::Result<Vec<SelectableSkillCapability>> {
    let response = ws_sender.skills_list(skill_catalog::skill_list_params(request.workspace_id))?;
    Ok(reduce_composer_skill_picker_rows_response(response, request.query.as_str()).rows)
}

pub fn composer_mcp_picker_rows(
    ws_sender: &GatewayWsCommandSender,
    request: ClientComposerMcpPickerRowsRequest,
) -> anyhow::Result<ClientComposerMcpPickerRowsResult> {
    let workspace_id = request.workspace_id;
    let response = ws_sender.mcp_list(mcp_list::mcp_list_params(workspace_id.clone()))?;
    let server_reduction =
        reduce_composer_mcp_server_picker_rows_response(response, request.query.as_str());
    let mut tool_rows = Vec::new();

    for server_id in &server_reduction.prefetch_server_ids {
        if let Ok(details) = ws_sender.mcp_server_details(mcp_details::mcp_server_details_params(
            workspace_id.clone(),
            server_id.clone(),
        )) {
            tool_rows.extend(
                reduce_composer_mcp_tool_picker_rows_response(details, request.query.as_str()).rows,
            );
        }
    }

    Ok(ClientComposerMcpPickerRowsResult {
        server_rows: server_reduction.rows,
        tool_rows,
    })
}

pub fn update_composer_capabilities(
    mut request: ClientComposerCapabilitiesUpdateRequest,
) -> Vec<ComposerCapability> {
    match request.action {
        ClientComposerCapabilitiesUpdateAction::Add { capability } => {
            add_composer_capability(&mut request.capabilities, capability);
        }
        ClientComposerCapabilitiesUpdateAction::Remove { id } => {
            if let Some(index) = request
                .capabilities
                .iter()
                .position(|capability| capability.id == id)
            {
                remove_composer_capability_at(&mut request.capabilities, index);
            }
        }
        ClientComposerCapabilitiesUpdateAction::RemoveAt { index } => {
            remove_composer_capability_at(&mut request.capabilities, index);
        }
    }
    request.capabilities
}

pub fn composer_capability_target(
    request: ClientComposerCapabilityTargetRequest,
) -> ComposerCapabilityTarget {
    composer_capability_target_for_provider(
        request.provider.as_deref(),
        request.runtimes.as_slice(),
    )
}

pub fn composer_capability_menu(
    request: ClientComposerCapabilityMenuVisibilityRequest,
) -> ComposerCapabilityMenuVisibility {
    composer_capability_menu_visibility(request.target)
}

pub fn composer_submission_plan(
    request: ClientComposerSubmissionPlanRequest,
) -> ComposerSubmissionPlan {
    plan_composer_submission(
        request.provider.as_deref(),
        request.text.as_str(),
        request.has_attachments,
        request.capabilities.as_slice(),
    )
}

pub fn composer_domain_transition(
    request: ClientComposerDomainTransitionRequest,
) -> ComposerDomainTransition {
    reduce_composer_domain_state(&request.state, request.action)
}

pub fn composer_draft_lifecycle_transition(
    request: ClientComposerDraftLifecycleTransitionRequest,
) -> ComposerDraftLifecycleTransition {
    reduce_composer_draft_lifecycle(&request.state, request.action)
}

pub fn composer_skill_rows_for_target(
    request: ClientComposerSkillRowsForTargetRequest,
) -> Vec<SelectableSkillCapability> {
    filter_selectable_skill_capabilities_for_target(request.rows.as_slice(), request.target)
}

pub fn skill_capability_from_row(
    request: ClientComposerSkillCapabilityFromRowRequest,
) -> ComposerCapability {
    ComposerCapability {
        id: pioneer_protocol::skill_capability_key(&request.row.skill_id),
        label: request.row.label,
        kind: pioneer_client::composer::capabilities::ComposerCapabilityKind::Skill {
            skill_id: request.row.skill_id,
            owner: request.row.owner,
            slug: request.row.slug,
            source_kind: request.row.source_kind,
        },
    }
}

pub fn mcp_capability_from_row(
    request: ClientComposerMcpCapabilityFromRowRequest,
) -> ComposerCapability {
    mcp_row_to_composer_capability(request.row)
}

pub fn toggle_skill_picker_selection(
    request: ClientComposerSkillToggleRequest,
) -> ClientComposerSkillToggleResult {
    let mut selected = request.selected_keys.into_iter().collect::<HashSet<_>>();
    toggle_selected_capability_key(&mut selected, request.key.as_str());
    ClientComposerSkillToggleResult {
        selected_keys: sorted_keys(selected),
    }
}

pub fn toggle_mcp_picker_selection(
    request: ClientComposerMcpToggleRequest,
) -> ClientComposerMcpToggleResult {
    let mut selected = request.selected_keys.into_iter().collect::<HashSet<_>>();
    let update = toggle_mcp_capability_selection(
        &mut selected,
        request.server_rows.as_slice(),
        request.tool_rows.as_slice(),
        &request.row,
    );

    let capabilities = replace_selected_mcp_composer_capabilities(
        request.capabilities.as_slice(),
        request.server_rows.as_slice(),
        request.tool_rows.as_slice(),
        &selected,
    );

    ClientComposerMcpToggleResult {
        capabilities,
        selected_keys: sorted_keys(selected),
        collapse_active_server: update.collapse_active_server,
    }
}

pub fn filter_skill_picker_rows(
    request: ClientComposerFilterSkillRowsRequest,
) -> Vec<SelectableSkillCapability> {
    filter_selectable_skill_capability_rows(request.rows.as_slice(), request.query.as_str())
}

pub fn filter_mcp_picker_rows(
    request: ClientComposerFilterMcpRowsRequest,
) -> ClientComposerFilterMcpRowsResult {
    let selected = request.selected_keys.into_iter().collect::<HashSet<_>>();
    let selected_server_ids = selected_mcp_server_ids(request.server_rows.as_slice(), &selected);
    let query = request.query;
    let has_query = !query.trim().is_empty();
    let server_rows =
        filter_selectable_mcp_capability_rows(request.server_rows.as_slice(), query.as_str());
    let tool_rows = if has_query {
        filter_search_mcp_tool_capability_rows(
            request.tool_rows.as_slice(),
            &selected_server_ids,
            query.as_str(),
        )
    } else if request
        .active_server_id
        .as_deref()
        .is_some_and(|server_id| selected_server_ids.contains(server_id))
    {
        Vec::new()
    } else {
        pioneer_client::composer::capabilities::filter_active_mcp_tool_capability_rows(
            request.tool_rows.as_slice(),
            request.active_server_id.as_deref(),
            query.as_str(),
        )
    };

    ClientComposerFilterMcpRowsResult {
        server_rows,
        tool_rows,
        has_query,
    }
}

pub fn normalize_client_file_reference(value: &str) -> anyhow::Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(anyhow::anyhow!("attachment path is required"));
    }

    if let Ok(url) = url::Url::parse(value)
        && url.scheme() == "file"
    {
        return url
            .to_file_path()
            .map(|path| path.to_string_lossy().to_string())
            .map_err(|_| anyhow::anyhow!("invalid file URL for attachment"));
    }

    Ok(value.to_owned())
}

fn non_empty_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn sorted_keys(keys: HashSet<String>) -> Vec<String> {
    let mut keys = keys.into_iter().collect::<Vec<_>>();
    keys.sort();
    keys
}
