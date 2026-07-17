//! Shell-neutral composer domain state and transitions.
//!
//! This module deliberately excludes hot UI state such as text selection,
//! cursor/IME composition, focus, keyboard, sheets, gestures, and microphone
//! capture. Desktop and mobile keep those concerns locally while applying the
//! same deterministic domain transitions here.

use super::{
    attachments::{
        ComposerAttachment, add_composer_attachment_from_artifact, composer_attachment_has_path,
        remove_composer_attachment_at,
    },
    capabilities::{
        ComposerCapability, ComposerCapabilityPolicy, ComposerCapabilityTarget,
        add_composer_capability, remove_composer_capability_at,
    },
    model_selection::{
        ComposerModelSelection, ComposerModelSelectionState, default_composer_turn_mode,
    },
    permissions::default_composer_permission_mode,
    turn_prepare::{
        apply_uploaded_composer_attachment_artifacts, mark_pending_composer_attachments_uploading,
        mark_uploading_composer_attachments_failed,
    },
};
use crate::providers::list::runtime_id_from_cli_runtime_provider_key;
use pioneer_protocol::{ArtifactRef, ThreadMode, TurnPermissionMode};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerDomainState {
    #[serde(default)]
    pub attachments: Vec<ComposerAttachment>,
    #[serde(default)]
    pub capabilities: Vec<ComposerCapability>,
    #[serde(default = "default_composer_turn_mode")]
    pub selected_mode: ThreadMode,
    #[serde(default)]
    pub mode_manually_selected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_provider: Option<String>,
    pub capability_target: ComposerCapabilityTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_reasoning_effort: Option<String>,
    #[serde(default = "default_composer_permission_mode")]
    pub selected_permission_mode: TurnPermissionMode,
    #[serde(default)]
    pub model_manually_selected: bool,
}

impl Default for ComposerDomainState {
    fn default() -> Self {
        Self {
            attachments: Vec::new(),
            capabilities: Vec::new(),
            selected_mode: default_composer_turn_mode(),
            mode_manually_selected: false,
            selected_provider: None,
            capability_target: ComposerCapabilityTarget::native(),
            selected_model: None,
            selected_reasoning_effort: None,
            selected_permission_mode: default_composer_permission_mode(),
            model_manually_selected: false,
        }
    }
}

impl ComposerDomainState {
    pub fn model_selection_state(&self) -> ComposerModelSelectionState {
        ComposerModelSelectionState::new_with_reasoning_effort(
            self.selected_provider.clone(),
            self.selected_model.clone(),
            self.selected_reasoning_effort.clone(),
            self.model_manually_selected,
        )
    }

    fn apply_model_selection_state(&mut self, state: ComposerModelSelectionState) {
        let (provider, model, reasoning_effort, manually_selected) =
            state.into_parts_with_reasoning_effort();
        self.selected_provider = provider;
        self.selected_model = model;
        self.selected_reasoning_effort = reasoning_effort;
        self.model_manually_selected = manually_selected;
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ComposerDomainAction {
    SetAttachments {
        attachments: Vec<ComposerAttachment>,
    },
    AddAttachment {
        attachment: ComposerAttachment,
    },
    AddArtifactAttachment {
        artifact: ArtifactRef,
    },
    RemoveAttachmentAt {
        index: usize,
    },
    MarkAttachmentsUploading,
    MarkAttachmentsFailed {
        error: String,
    },
    ApplyUploadedAttachments {
        artifacts: Vec<Option<ArtifactRef>>,
    },
    SetCapabilities {
        capabilities: Vec<ComposerCapability>,
    },
    AddCapability {
        capability: ComposerCapability,
    },
    RemoveCapability {
        id: String,
    },
    RemoveCapabilityAt {
        index: usize,
    },
    SetModeFromUser {
        mode: ThreadMode,
    },
    SetPermissionMode {
        mode: TurnPermissionMode,
    },
    SetModelSelectionFromUser {
        #[serde(default)]
        provider: Option<String>,
        #[serde(default)]
        model: Option<String>,
        #[serde(default)]
        capability_target: Option<ComposerCapabilityTarget>,
    },
    SetReasoningEffortFromUser {
        #[serde(default)]
        effort: Option<String>,
    },
    SyncResolvedModelSelection {
        #[serde(default)]
        selection: Option<ComposerModelSelection>,
        #[serde(default)]
        capability_target: Option<ComposerCapabilityTarget>,
    },
    ResetModelSelection {
        #[serde(default)]
        selection: Option<ComposerModelSelection>,
        #[serde(default)]
        capability_target: Option<ComposerCapabilityTarget>,
    },
    SyncCapabilityTarget {
        #[serde(default)]
        provider: Option<String>,
        target: ComposerCapabilityTarget,
    },
    ClearReasoningEffort,
    ClearPayload,
    Reset {
        defaults: ComposerDomainState,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerDomainTransition {
    pub state: ComposerDomainState,
    pub changed: bool,
    pub payload_changed: bool,
    pub model_selection_changed: bool,
}

pub fn reduce_composer_domain_state(
    state: &ComposerDomainState,
    action: ComposerDomainAction,
) -> ComposerDomainTransition {
    let mut next = state.clone();

    match action {
        ComposerDomainAction::SetAttachments { attachments } => {
            next.attachments = attachments;
        }
        ComposerDomainAction::AddAttachment { attachment } => {
            if !composer_attachment_has_path(&next.attachments, attachment.path.as_str()) {
                next.attachments.push(attachment);
            }
        }
        ComposerDomainAction::AddArtifactAttachment { artifact } => {
            add_composer_attachment_from_artifact(&mut next.attachments, artifact);
        }
        ComposerDomainAction::RemoveAttachmentAt { index } => {
            remove_composer_attachment_at(&mut next.attachments, index);
        }
        ComposerDomainAction::MarkAttachmentsUploading => {
            mark_pending_composer_attachments_uploading(&mut next.attachments);
        }
        ComposerDomainAction::MarkAttachmentsFailed { error } => {
            mark_uploading_composer_attachments_failed(&mut next.attachments, error);
        }
        ComposerDomainAction::ApplyUploadedAttachments { artifacts } => {
            apply_uploaded_composer_attachment_artifacts(&mut next.attachments, artifacts);
        }
        ComposerDomainAction::SetCapabilities { capabilities } => {
            next.capabilities = capabilities;
        }
        ComposerDomainAction::AddCapability { capability } => {
            add_composer_capability(&mut next.capabilities, capability);
        }
        ComposerDomainAction::RemoveCapability { id } => {
            if let Some(index) = next
                .capabilities
                .iter()
                .position(|capability| capability.id == id)
            {
                remove_composer_capability_at(&mut next.capabilities, index);
            }
        }
        ComposerDomainAction::RemoveCapabilityAt { index } => {
            remove_composer_capability_at(&mut next.capabilities, index);
        }
        ComposerDomainAction::SetModeFromUser { mode } => {
            next.selected_mode = mode;
            next.mode_manually_selected = true;
        }
        ComposerDomainAction::SetPermissionMode { mode } => {
            next.selected_permission_mode = mode;
        }
        ComposerDomainAction::SetModelSelectionFromUser {
            provider,
            model,
            capability_target,
        } => {
            next.capability_target =
                capability_target_for_selection(state, provider.as_deref(), capability_target);
            let mut selection = next.model_selection_state();
            selection.set_model_selection_from_user(provider, model);
            next.apply_model_selection_state(selection);
        }
        ComposerDomainAction::SetReasoningEffortFromUser { effort } => {
            next.selected_reasoning_effort = normalize_optional_text(effort);
            next.model_manually_selected = true;
        }
        ComposerDomainAction::SyncResolvedModelSelection {
            selection,
            capability_target,
        } => {
            if next.model_manually_selected {
                let resolved_provider = selection
                    .as_ref()
                    .map(|selection| selection.provider.as_str());
                if next.selected_provider.as_deref() == resolved_provider
                    && let Some(target) = capability_target
                {
                    next.capability_target = target;
                }
            } else {
                let resolved_provider = selection
                    .as_ref()
                    .map(|selection| selection.provider.as_str());
                next.capability_target =
                    capability_target_for_selection(state, resolved_provider, capability_target);
                let mut model_state = next.model_selection_state();
                model_state.sync_resolved_selection(selection);
                next.apply_model_selection_state(model_state);
            }
        }
        ComposerDomainAction::ResetModelSelection {
            selection,
            capability_target,
        } => {
            let resolved_provider = selection
                .as_ref()
                .map(|selection| selection.provider.as_str());
            next.capability_target =
                capability_target_for_selection(state, resolved_provider, capability_target);
            let mut model_state = next.model_selection_state();
            model_state.reset_to_resolved_selection(selection);
            next.apply_model_selection_state(model_state);
        }
        ComposerDomainAction::SyncCapabilityTarget { provider, target } => {
            if next.selected_provider == provider {
                next.capability_target = target;
            }
        }
        ComposerDomainAction::ClearReasoningEffort => {
            next.selected_reasoning_effort = None;
        }
        ComposerDomainAction::ClearPayload => {
            next.attachments.clear();
            next.capabilities.clear();
        }
        ComposerDomainAction::Reset { defaults } => {
            next = defaults;
        }
    }

    let payload_changed =
        next.attachments != state.attachments || next.capabilities != state.capabilities;
    let model_selection_changed = next.selected_provider != state.selected_provider
        || next.selected_model != state.selected_model
        || next.selected_reasoning_effort != state.selected_reasoning_effort
        || next.model_manually_selected != state.model_manually_selected
        || next.capability_target != state.capability_target;
    let changed = next != *state;

    ComposerDomainTransition {
        state: next,
        changed,
        payload_changed,
        model_selection_changed,
    }
}

fn capability_target_for_selection(
    current: &ComposerDomainState,
    provider: Option<&str>,
    requested: Option<ComposerCapabilityTarget>,
) -> ComposerCapabilityTarget {
    if let Some(requested) = requested {
        return requested;
    }
    if current.selected_provider.as_deref() == provider {
        return current.capability_target;
    }
    if provider
        .and_then(runtime_id_from_cli_runtime_provider_key)
        .is_some()
    {
        ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::unsupported_cli())
    } else {
        ComposerCapabilityTarget::native()
    }
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composer::capabilities::ComposerCapabilityKind;
    use pioneer_protocol::McpScopeKind;

    fn mcp_capability() -> ComposerCapability {
        ComposerCapability {
            id: "mcp:workspace:mail".to_owned(),
            label: "mail".to_owned(),
            kind: ComposerCapabilityKind::McpServer {
                name: "mail".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        }
    }

    #[test]
    fn explicit_capabilities_survive_model_and_readiness_transitions() {
        let initial = ComposerDomainState {
            capabilities: vec![mcp_capability()],
            ..ComposerDomainState::default()
        };
        let selected = reduce_composer_domain_state(
            &initial,
            ComposerDomainAction::SetModelSelectionFromUser {
                provider: Some("cli_runtime:codex".to_owned()),
                model: Some("gpt-5".to_owned()),
                capability_target: Some(ComposerCapabilityTarget::cli(
                    ComposerCapabilityPolicy::cli(true, true),
                )),
            },
        )
        .state;
        let refreshed = reduce_composer_domain_state(
            &selected,
            ComposerDomainAction::SyncResolvedModelSelection {
                selection: Some(ComposerModelSelection {
                    provider: "cli_runtime:codex".to_owned(),
                    model: "ignored".to_owned(),
                    selected_reasoning_effort: None,
                }),
                capability_target: Some(ComposerCapabilityTarget::cli(
                    ComposerCapabilityPolicy::unsupported_cli(),
                )),
            },
        )
        .state;

        assert_eq!(refreshed.selected_model.as_deref(), Some("gpt-5"));
        assert_eq!(refreshed.capabilities, initial.capabilities);
        assert!(!refreshed.capability_target.policy().supports_mcp_tools);
    }

    #[test]
    fn user_reasoning_effort_is_a_manual_model_selection() {
        let initial = ComposerDomainState {
            selected_provider: Some("openai".to_owned()),
            selected_model: Some("gpt-5".to_owned()),
            ..ComposerDomainState::default()
        };

        let next = reduce_composer_domain_state(
            &initial,
            ComposerDomainAction::SetReasoningEffortFromUser {
                effort: Some(" high ".to_owned()),
            },
        )
        .state;

        assert_eq!(next.selected_reasoning_effort.as_deref(), Some("high"));
        assert!(next.model_manually_selected);
    }

    #[test]
    fn user_model_change_clears_effort_and_clear_payload_preserves_selection() {
        let initial = ComposerDomainState {
            attachments: vec![ComposerAttachment {
                path: "/tmp/file.txt".to_owned(),
                file_name: "file.txt".to_owned(),
                kind: super::super::attachments::ComposerAttachmentKind::File,
                upload_state: super::super::attachments::ComposerAttachmentUploadState::Local,
            }],
            capabilities: vec![mcp_capability()],
            selected_provider: Some("openai".to_owned()),
            selected_model: Some("gpt-5".to_owned()),
            selected_reasoning_effort: Some("high".to_owned()),
            ..ComposerDomainState::default()
        };
        let changed = reduce_composer_domain_state(
            &initial,
            ComposerDomainAction::SetModelSelectionFromUser {
                provider: Some("openai".to_owned()),
                model: Some("gpt-5.1".to_owned()),
                capability_target: None,
            },
        )
        .state;
        assert_eq!(changed.selected_reasoning_effort, None);

        let cleared = reduce_composer_domain_state(&changed, ComposerDomainAction::ClearPayload);
        assert!(cleared.state.attachments.is_empty());
        assert!(cleared.state.capabilities.is_empty());
        assert_eq!(cleared.state.selected_model.as_deref(), Some("gpt-5.1"));
        assert_eq!(
            cleared.state.selected_permission_mode,
            default_composer_permission_mode()
        );
    }
}
