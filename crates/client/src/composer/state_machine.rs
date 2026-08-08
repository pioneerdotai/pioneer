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
    skill_selection::{
        ComposerSkillPickerProjection, ComposerSkillSelection, normalize_composer_skill_selections,
        reduce_composer_skill_selection_toggle,
    },
    turn_prepare::{
        apply_uploaded_composer_attachment_artifacts, mark_pending_composer_attachments_uploading,
        mark_uploading_composer_attachments_failed,
    },
};
use crate::providers::list::runtime_id_from_cli_runtime_provider_key;
use pioneer_protocol::{
    ArtifactRef, MemberSummary, PrincipalId, PrincipalStatus, ThreadMode, TurnPermissionMode,
};

const COMPOSER_REPLY_PREVIEW_MAX_CHARS: usize = 160;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerReplyTarget {
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerMentionCandidate {
    pub principal_id: PrincipalId,
    pub display_name: String,
    pub nickname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_revision: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerMentionSelection {
    pub principal_id: PrincipalId,
    pub display_name: String,
    pub nickname: String,
    pub text_token: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerDomainState {
    #[serde(default)]
    pub attachments: Vec<ComposerAttachment>,
    #[serde(default)]
    pub capabilities: Vec<ComposerCapability>,
    #[serde(default)]
    pub skill_selections: Vec<ComposerSkillSelection>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_target: Option<ComposerReplyTarget>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub selected_mentions: Vec<ComposerMentionSelection>,
}

impl Default for ComposerDomainState {
    fn default() -> Self {
        Self {
            attachments: Vec::new(),
            capabilities: Vec::new(),
            skill_selections: Vec::new(),
            selected_mode: default_composer_turn_mode(),
            mode_manually_selected: false,
            selected_provider: None,
            capability_target: ComposerCapabilityTarget::native(),
            selected_model: None,
            selected_reasoning_effort: None,
            selected_permission_mode: default_composer_permission_mode(),
            model_manually_selected: false,
            reply_target: None,
            selected_mentions: Vec::new(),
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
    SetSkillSelections {
        selections: Vec<ComposerSkillSelection>,
    },
    ToggleSkillSelection {
        picker: ComposerSkillPickerProjection,
        selection: ComposerSkillSelection,
    },
    SetModeFromUser {
        mode: ThreadMode,
    },
    SetReplyTarget {
        target: ComposerReplyTarget,
    },
    ClearReplyTarget,
    SelectMention {
        candidate: ComposerMentionCandidate,
    },
    RemoveMention {
        principal_id: PrincipalId,
    },
    ReconcileMentionsWithText {
        text: String,
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
    SendSucceeded,
    SendFailed,
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
    pub execution_capabilities_removed: bool,
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
        ComposerDomainAction::SetSkillSelections { selections } => {
            next.skill_selections = normalize_composer_skill_selections(selections);
        }
        ComposerDomainAction::ToggleSkillSelection { picker, selection } => {
            next.skill_selections = reduce_composer_skill_selection_toggle(
                next.skill_selections.as_slice(),
                &picker,
                selection,
            )
            .selections;
        }
        ComposerDomainAction::SetModeFromUser { mode } => {
            next.selected_mode = mode;
            next.mode_manually_selected = true;
            if mode == ThreadMode::Message {
                clear_execution_only_state(&mut next);
            }
        }
        ComposerDomainAction::SetReplyTarget { target } => {
            let target = normalize_reply_target(target);
            if target.is_some() && next.selected_mode != ThreadMode::Message {
                next.selected_mode = ThreadMode::Message;
                next.mode_manually_selected = true;
                clear_execution_only_state(&mut next);
            }
            next.reply_target = target;
        }
        ComposerDomainAction::ClearReplyTarget => {
            next.reply_target = None;
        }
        ComposerDomainAction::SelectMention { candidate } => {
            if !next
                .selected_mentions
                .iter()
                .any(|selection| selection.principal_id == candidate.principal_id)
            {
                if let Some(selection) = mention_selection_from_candidate(candidate) {
                    next.selected_mentions.push(selection);
                }
            }
        }
        ComposerDomainAction::RemoveMention { principal_id } => {
            next.selected_mentions
                .retain(|selection| selection.principal_id != principal_id);
        }
        ComposerDomainAction::ReconcileMentionsWithText { text } => {
            next.selected_mentions
                .retain(|selection| text.contains(selection.text_token.as_str()));
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
            next.skill_selections.clear();
            next.reply_target = None;
            next.selected_mentions.clear();
        }
        ComposerDomainAction::SendSucceeded => {
            next.attachments.clear();
            next.selected_mode = default_composer_turn_mode();
            next.mode_manually_selected = false;
            next.reply_target = None;
            next.selected_mentions.clear();
            clear_execution_only_state(&mut next);
        }
        ComposerDomainAction::SendFailed => {
            // A failed pre-commit send is intentionally a no-op. The user can
            // retry the exact explicit mode and collaboration payload.
        }
        ComposerDomainAction::Reset { defaults } => {
            next = defaults;
        }
    }

    if next.selected_mode == ThreadMode::Message {
        clear_execution_only_state(&mut next);
    }

    let payload_changed = next.attachments != state.attachments
        || next.capabilities != state.capabilities
        || next.skill_selections != state.skill_selections
        || next.reply_target != state.reply_target
        || next.selected_mentions != state.selected_mentions;
    let model_selection_changed = next.selected_provider != state.selected_provider
        || next.selected_model != state.selected_model
        || next.selected_reasoning_effort != state.selected_reasoning_effort
        || next.model_manually_selected != state.model_manually_selected
        || next.capability_target != state.capability_target;
    let changed = next != *state;
    let execution_capabilities_removed = next.selected_mode == ThreadMode::Message
        && (next.capabilities.len() < state.capabilities.len()
            || next.skill_selections.len() < state.skill_selections.len()
            || (state.selected_provider.is_some() && next.selected_provider.is_none())
            || (state.selected_model.is_some() && next.selected_model.is_none())
            || (state.selected_reasoning_effort.is_some()
                && next.selected_reasoning_effort.is_none())
            || state.selected_permission_mode != next.selected_permission_mode);

    ComposerDomainTransition {
        state: next,
        changed,
        payload_changed,
        model_selection_changed,
        execution_capabilities_removed,
    }
}

pub fn composer_mention_candidates(
    members: impl IntoIterator<Item = MemberSummary>,
) -> Vec<ComposerMentionCandidate> {
    let mut candidates = Vec::new();
    for member in members {
        if member.status != PrincipalStatus::Active
            || candidates
                .iter()
                .any(|candidate: &ComposerMentionCandidate| {
                    candidate.principal_id == member.principal_id
                })
        {
            continue;
        }
        candidates.push(ComposerMentionCandidate {
            principal_id: member.principal_id,
            display_name: member.display_name,
            nickname: member.nickname,
            avatar_revision: member.avatar_revision,
        });
    }
    candidates
}

pub fn composer_reply_target_from_visible_message(
    presentation: &crate::timeline::rows::UserMessagePresentation,
    visible_text: &str,
) -> Option<ComposerReplyTarget> {
    let turn_id = non_empty_text(presentation.turn_id.as_str())?;
    let author_display_name = presentation
        .author
        .as_ref()
        .and_then(|author| non_empty_text(author.display_name.as_str()));
    let preview = (!presentation.deleted)
        .then(|| bounded_text(visible_text, COMPOSER_REPLY_PREVIEW_MAX_CHARS))
        .flatten();
    Some(ComposerReplyTarget {
        turn_id,
        author_display_name,
        preview,
    })
}

pub fn bound_composer_mentioned_principal_ids(
    state: &ComposerDomainState,
    text: &str,
) -> Vec<PrincipalId> {
    let mut ids = Vec::new();
    for selection in &state.selected_mentions {
        if text.contains(selection.text_token.as_str()) && !ids.contains(&selection.principal_id) {
            ids.push(selection.principal_id.clone());
        }
    }
    ids
}

fn mention_selection_from_candidate(
    candidate: ComposerMentionCandidate,
) -> Option<ComposerMentionSelection> {
    let nickname = non_empty_text(candidate.nickname.as_str())?;
    Some(ComposerMentionSelection {
        principal_id: candidate.principal_id,
        display_name: candidate.display_name.trim().to_owned(),
        text_token: format!("@{nickname}"),
        nickname,
    })
}

fn normalize_reply_target(target: ComposerReplyTarget) -> Option<ComposerReplyTarget> {
    Some(ComposerReplyTarget {
        turn_id: non_empty_text(target.turn_id.as_str())?,
        author_display_name: target
            .author_display_name
            .as_deref()
            .and_then(non_empty_text),
        preview: target
            .preview
            .as_deref()
            .and_then(|value| bounded_text(value, COMPOSER_REPLY_PREVIEW_MAX_CHARS)),
    })
}

fn clear_execution_only_state(state: &mut ComposerDomainState) {
    state.capabilities.clear();
    state.skill_selections.clear();
    state.selected_provider = None;
    state.selected_model = None;
    state.selected_reasoning_effort = None;
    state.selected_permission_mode = default_composer_permission_mode();
    state.model_manually_selected = false;
    state.capability_target = ComposerCapabilityTarget::native();
}

fn non_empty_text(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn bounded_text(value: &str, max_chars: usize) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.chars().take(max_chars).collect())
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
    use pioneer_protocol::{
        McpScopeKind, PersistedActorRef, PrincipalKind, SkillId, SkillPackId, TurnAuthorSnapshot,
    };

    fn mcp_capability() -> ComposerCapability {
        ComposerCapability {
            id: "mcp-server:workspace:mail".to_owned(),
            label: "mail".to_owned(),
            kind: ComposerCapabilityKind::McpServer {
                name: "mail".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        }
    }

    fn skill_capability() -> ComposerCapability {
        let skill_id = SkillId::new("R".repeat(21)).expect("valid skill id");
        ComposerCapability {
            id: pioneer_protocol::skill_capability_key(&skill_id),
            label: "owner/reviewer".to_owned(),
            kind: ComposerCapabilityKind::Skill {
                skill_id,
                owner: Some("owner".to_owned()),
                slug: "reviewer".to_owned(),
                source_kind: "user".to_owned(),
            },
        }
    }

    #[test]
    fn draft_round_trip_preserves_exact_skill_id_and_label_snapshot() {
        let pack_id = SkillPackId::new("P".repeat(21)).expect("pack id");
        let state = ComposerDomainState {
            capabilities: vec![skill_capability()],
            skill_selections: vec![ComposerSkillSelection::SkillPack { pack_id }],
            selected_mode: ThreadMode::Agent,
            ..ComposerDomainState::default()
        };

        let encoded = serde_json::to_value(&state).expect("encode draft state");
        let decoded =
            serde_json::from_value::<ComposerDomainState>(encoded).expect("decode draft state");

        assert_eq!(decoded.capabilities, state.capabilities);
        assert_eq!(decoded.capabilities[0].label, "owner/reviewer");
        assert!(matches!(
            decoded.capabilities[0].kind,
            ComposerCapabilityKind::Skill { ref skill_id, .. }
                if skill_id.as_str() == "RRRRRRRRRRRRRRRRRRRRR"
        ));
        assert_eq!(decoded.skill_selections, state.skill_selections);
    }

    #[test]
    fn set_skill_selections_normalizes_full_and_partial_pack_intent() {
        let pack_id = SkillPackId::new("P".repeat(21)).expect("pack id");
        let child = ComposerSkillSelection::Skill {
            skill_id: SkillId::new("C".repeat(21)).expect("skill id"),
            pack_id: Some(pack_id.clone()),
        };
        let standalone = ComposerSkillSelection::Skill {
            skill_id: SkillId::new("S".repeat(21)).expect("skill id"),
            pack_id: None,
        };
        let full = ComposerSkillSelection::SkillPack { pack_id };
        let state = ComposerDomainState {
            selected_mode: ThreadMode::Agent,
            ..ComposerDomainState::default()
        };

        let transition = reduce_composer_domain_state(
            &state,
            ComposerDomainAction::SetSkillSelections {
                selections: vec![child, standalone.clone(), full.clone()],
            },
        );

        assert_eq!(transition.state.skill_selections, vec![standalone, full]);
        assert!(transition.payload_changed);
    }

    #[test]
    fn old_domain_state_payload_defaults_skill_selections() {
        let value = serde_json::to_value(ComposerDomainState::default()).expect("state value");
        let mut object = value.as_object().expect("state object").clone();
        object.remove("skill_selections");
        object.remove("reply_target");
        object.remove("selected_mentions");

        let decoded: ComposerDomainState =
            serde_json::from_value(serde_json::Value::Object(object)).expect("old state payload");

        assert!(decoded.skill_selections.is_empty());
        assert!(decoded.reply_target.is_none());
        assert!(decoded.selected_mentions.is_empty());
    }

    #[test]
    fn explicit_capabilities_survive_model_and_readiness_transitions() {
        let initial = ComposerDomainState {
            capabilities: vec![mcp_capability()],
            selected_mode: ThreadMode::Agent,
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
            selected_mode: ThreadMode::Agent,
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
            skill_selections: vec![ComposerSkillSelection::SkillPack {
                pack_id: SkillPackId::new("P".repeat(21)).expect("pack id"),
            }],
            selected_provider: Some("openai".to_owned()),
            selected_model: Some("gpt-5".to_owned()),
            selected_reasoning_effort: Some("high".to_owned()),
            selected_mode: ThreadMode::Agent,
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
        assert!(cleared.state.skill_selections.is_empty());
        assert_eq!(cleared.state.selected_model.as_deref(), Some("gpt-5.1"));
        assert_eq!(
            cleared.state.selected_permission_mode,
            default_composer_permission_mode()
        );
    }

    #[test]
    fn switching_to_message_preserves_files_and_reports_removed_execution_capabilities() {
        let attachment = ComposerAttachment {
            path: "/tmp/file.txt".to_owned(),
            file_name: "file.txt".to_owned(),
            kind: super::super::attachments::ComposerAttachmentKind::File,
            upload_state: super::super::attachments::ComposerAttachmentUploadState::Local,
        };
        let initial = ComposerDomainState {
            attachments: vec![attachment.clone()],
            capabilities: vec![mcp_capability()],
            selected_provider: Some("openai".to_owned()),
            selected_model: Some("gpt-5".to_owned()),
            selected_reasoning_effort: Some("high".to_owned()),
            selected_permission_mode: TurnPermissionMode::Supervised,
            model_manually_selected: true,
            selected_mode: ThreadMode::Agent,
            ..ComposerDomainState::default()
        };

        let transition = reduce_composer_domain_state(
            &initial,
            ComposerDomainAction::SetModeFromUser {
                mode: ThreadMode::Message,
            },
        );

        assert_eq!(transition.state.selected_mode, ThreadMode::Message);
        assert_eq!(transition.state.attachments, vec![attachment]);
        assert!(transition.state.capabilities.is_empty());
        assert!(transition.state.selected_provider.is_none());
        assert!(transition.state.selected_model.is_none());
        assert!(transition.state.selected_reasoning_effort.is_none());
        assert_eq!(
            transition.state.selected_permission_mode,
            default_composer_permission_mode()
        );
        assert!(transition.execution_capabilities_removed);
    }

    #[test]
    fn successful_send_clears_payload_and_resets_mode_but_failure_needs_no_reduction() {
        let initial = ComposerDomainState {
            attachments: vec![ComposerAttachment {
                path: "/tmp/file.txt".to_owned(),
                file_name: "file.txt".to_owned(),
                kind: super::super::attachments::ComposerAttachmentKind::File,
                upload_state: super::super::attachments::ComposerAttachmentUploadState::Local,
            }],
            selected_mode: ThreadMode::Chat,
            mode_manually_selected: true,
            reply_target: Some(ComposerReplyTarget {
                turn_id: "turn-parent".to_owned(),
                author_display_name: Some("Alice".to_owned()),
                preview: Some("Earlier".to_owned()),
            }),
            selected_mentions: vec![ComposerMentionSelection {
                principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").expect("principal id"),
                display_name: "Alice".to_owned(),
                nickname: "alice".to_owned(),
                text_token: "@alice".to_owned(),
            }],
            ..ComposerDomainState::default()
        };

        let failed = reduce_composer_domain_state(&initial, ComposerDomainAction::SendFailed);
        assert_eq!(failed.state, initial);
        assert!(!failed.changed);

        let transition =
            reduce_composer_domain_state(&initial, ComposerDomainAction::SendSucceeded);

        assert_eq!(transition.state.selected_mode, ThreadMode::Message);
        assert!(!transition.state.mode_manually_selected);
        assert!(transition.state.attachments.is_empty());
        assert!(transition.state.reply_target.is_none());
        assert!(transition.state.selected_mentions.is_empty());
        assert_eq!(initial.selected_mode, ThreadMode::Chat);
    }

    #[test]
    fn reply_target_comes_from_visible_turn_and_clear_is_deterministic() {
        let presentation = crate::timeline::rows::UserMessagePresentation {
            workspace_id: "workspace-a".to_owned(),
            thread_id: "thread-a".to_owned(),
            block_id: "block-a".to_owned(),
            turn_id: "turn-parent".to_owned(),
            item_id: "item-a".to_owned(),
            mode: ThreadMode::Message,
            author: Some(TurnAuthorSnapshot {
                actor: PersistedActorRef::Principal(
                    PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").expect("principal id"),
                ),
                display_name: "Alice".to_owned(),
                nickname: "alice".to_owned(),
                avatar_revision: None,
            }),
            reply: None,
            reply_state: None,
            mentions: Vec::new(),
            attachments: Vec::new(),
            revision: 0,
            edited: false,
            deleted: false,
        };
        let target = composer_reply_target_from_visible_message(
            &presentation,
            &"x".repeat(COMPOSER_REPLY_PREVIEW_MAX_CHARS + 20),
        )
        .expect("visible reply target");
        assert_eq!(target.turn_id, "turn-parent");
        assert_eq!(target.author_display_name.as_deref(), Some("Alice"));
        assert_eq!(
            target.preview.as_ref().map(|value| value.chars().count()),
            Some(160)
        );

        let initial = ComposerDomainState {
            selected_mode: ThreadMode::Agent,
            capabilities: vec![mcp_capability()],
            selected_provider: Some("openai".to_owned()),
            selected_model: Some("gpt-5".to_owned()),
            ..ComposerDomainState::default()
        };
        let selected =
            reduce_composer_domain_state(&initial, ComposerDomainAction::SetReplyTarget { target });
        assert!(selected.state.reply_target.is_some());
        assert_eq!(selected.state.selected_mode, ThreadMode::Message);
        assert!(selected.state.mode_manually_selected);
        assert!(selected.state.capabilities.is_empty());
        assert!(selected.state.selected_provider.is_none());
        assert!(selected.state.selected_model.is_none());
        assert!(selected.execution_capabilities_removed);
        let cleared =
            reduce_composer_domain_state(&selected.state, ComposerDomainAction::ClearReplyTarget);
        assert!(cleared.state.reply_target.is_none());
    }

    #[test]
    fn mention_candidates_are_scoped_active_and_selected_ids_follow_exact_tokens() {
        let active = MemberSummary {
            principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").expect("principal id"),
            kind: PrincipalKind::User,
            display_name: "Alice".to_owned(),
            nickname: "alice".to_owned(),
            role_key: None,
            status: PrincipalStatus::Active,
            avatar_revision: Some("a".repeat(64)),
        };
        let suspended = MemberSummary {
            principal_id: PrincipalId::new("PBBBBBBBBBBBBBBBBBBBB").expect("principal id"),
            status: PrincipalStatus::Suspended,
            display_name: "Bob".to_owned(),
            nickname: "bob".to_owned(),
            avatar_revision: None,
            kind: PrincipalKind::User,
            role_key: None,
        };
        let candidates = composer_mention_candidates([active.clone(), active.clone(), suspended]);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].principal_id, active.principal_id);

        let selected = reduce_composer_domain_state(
            &ComposerDomainState::default(),
            ComposerDomainAction::SelectMention {
                candidate: candidates[0].clone(),
            },
        );
        let duplicate = reduce_composer_domain_state(
            &selected.state,
            ComposerDomainAction::SelectMention {
                candidate: candidates[0].clone(),
            },
        );
        assert_eq!(duplicate.state.selected_mentions.len(), 1);
        assert_eq!(
            bound_composer_mentioned_principal_ids(&duplicate.state, "hello @alice"),
            vec![active.principal_id]
        );

        let reconciled = reduce_composer_domain_state(
            &duplicate.state,
            ComposerDomainAction::ReconcileMentionsWithText {
                text: "hello".to_owned(),
            },
        );
        assert!(reconciled.state.selected_mentions.is_empty());
    }

    #[test]
    fn unknown_composer_mode_fails_closed_at_the_schema_boundary() {
        let mut value = serde_json::to_value(ComposerDomainState::default()).expect("state value");
        value["selected_mode"] = serde_json::Value::String("future_mode".to_owned());
        assert!(serde_json::from_value::<ComposerDomainState>(value).is_err());
    }
}
