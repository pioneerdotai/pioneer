//! Composer draft state.

use crate::composer::{
    permissions::default_composer_permission_mode, state_machine::ComposerDomainState,
};
use pioneer_protocol::TurnPermissionMode;
use std::collections::{BTreeMap, HashMap};

/// Complete, shell-neutral draft payload used by desktop and mobile.
///
/// Hot editor state (cursor, IME composition, focus, keyboard, sheets) is not
/// part of this value. A shell snapshots its text only at lifecycle boundaries
/// such as switching threads.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerDomainDraft {
    #[serde(default)]
    pub text: String,
    pub domain: ComposerDomainState,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerDraftLifecycleState {
    #[serde(default)]
    pub drafts: BTreeMap<String, ComposerDomainDraft>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ComposerDraftLifecycleAction {
    SwitchThread {
        #[serde(default)]
        current_thread_id: Option<String>,
        #[serde(default)]
        current_draft: Option<ComposerDomainDraft>,
        target_thread_id: String,
        fallback: ComposerDomainDraft,
    },
    RememberThread {
        thread_id: String,
        draft: ComposerDomainDraft,
    },
    ClearThread {
        thread_id: String,
    },
    ClearAll,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerDraftLifecycleTransition {
    pub state: ComposerDraftLifecycleState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restored_draft: Option<ComposerDomainDraft>,
    pub changed: bool,
}

/// Build the fallback for a thread that has no saved Composer draft.
///
/// Model, mode, and permission preferences may follow the active shell, but
/// user payload is thread-owned and must never leak into another thread.
pub fn composer_thread_switch_fallback(mut domain: ComposerDomainState) -> ComposerDomainDraft {
    domain.attachments.clear();
    domain.capabilities.clear();
    domain.skill_selections.clear();
    domain.reply_target = None;
    domain.selected_mentions.clear();
    ComposerDomainDraft {
        text: String::new(),
        domain,
    }
}

pub fn reduce_composer_draft_lifecycle(
    state: &ComposerDraftLifecycleState,
    action: ComposerDraftLifecycleAction,
) -> ComposerDraftLifecycleTransition {
    let mut next = state.clone();
    let restored_draft = match action {
        ComposerDraftLifecycleAction::SwitchThread {
            current_thread_id,
            current_draft,
            target_thread_id,
            fallback,
        } => {
            if let (Some(thread_id), Some(draft)) = (current_thread_id, current_draft) {
                next.drafts.insert(thread_id, draft);
            }
            let draft = next
                .drafts
                .entry(target_thread_id)
                .or_insert(fallback)
                .clone();
            Some(draft)
        }
        ComposerDraftLifecycleAction::RememberThread { thread_id, draft } => {
            next.drafts.insert(thread_id, draft);
            None
        }
        ComposerDraftLifecycleAction::ClearThread { thread_id } => {
            next.drafts.remove(thread_id.as_str());
            None
        }
        ComposerDraftLifecycleAction::ClearAll => {
            next.drafts.clear();
            None
        }
    };
    let changed = next != *state;

    ComposerDraftLifecycleTransition {
        state: next,
        restored_draft,
        changed,
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerDraft<Attachment, Capability> {
    pub text: String,
    pub attachments: Vec<Attachment>,
    pub capabilities: Vec<Capability>,
    #[serde(default = "default_composer_permission_mode")]
    pub permission_mode: TurnPermissionMode,
}

impl<Attachment, Capability> ComposerDraft<Attachment, Capability> {
    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
            && self.attachments.is_empty()
            && self.capabilities.is_empty()
            && self.permission_mode == default_composer_permission_mode()
    }
}

pub fn normalize_composer_draft_text(value: &str) -> String {
    value.trim_end().to_owned()
}

pub fn remember_thread_composer_draft<Attachment, Capability>(
    thread_id: &str,
    text: String,
    attachments: Vec<Attachment>,
    capabilities: Vec<Capability>,
    permission_mode: TurnPermissionMode,
    thread_drafts: &mut HashMap<String, String>,
    thread_draft_attachments: &mut HashMap<String, Vec<Attachment>>,
    thread_draft_capabilities: &mut HashMap<String, Vec<Capability>>,
    thread_draft_permission_modes: &mut HashMap<String, TurnPermissionMode>,
) -> bool {
    if text.is_empty()
        && attachments.is_empty()
        && capabilities.is_empty()
        && permission_mode == default_composer_permission_mode()
    {
        clear_thread_composer_draft(
            thread_id,
            thread_drafts,
            thread_draft_attachments,
            thread_draft_capabilities,
            thread_draft_permission_modes,
        );
        return false;
    }

    thread_drafts.insert(thread_id.to_owned(), text);
    thread_draft_attachments.insert(thread_id.to_owned(), attachments);
    thread_draft_capabilities.insert(thread_id.to_owned(), capabilities);
    thread_draft_permission_modes.insert(thread_id.to_owned(), permission_mode);
    true
}

pub fn clear_thread_composer_draft<Attachment, Capability>(
    thread_id: &str,
    thread_drafts: &mut HashMap<String, String>,
    thread_draft_attachments: &mut HashMap<String, Vec<Attachment>>,
    thread_draft_capabilities: &mut HashMap<String, Vec<Capability>>,
    thread_draft_permission_modes: &mut HashMap<String, TurnPermissionMode>,
) {
    thread_drafts.remove(thread_id);
    thread_draft_attachments.remove(thread_id);
    thread_draft_capabilities.remove(thread_id);
    thread_draft_permission_modes.remove(thread_id);
}

pub fn restore_thread_composer_draft<Attachment: Clone, Capability: Clone>(
    thread_id: &str,
    thread_drafts: &HashMap<String, String>,
    thread_draft_attachments: &HashMap<String, Vec<Attachment>>,
    thread_draft_capabilities: &HashMap<String, Vec<Capability>>,
    thread_draft_permission_modes: &HashMap<String, TurnPermissionMode>,
) -> ComposerDraft<Attachment, Capability> {
    ComposerDraft {
        text: thread_drafts.get(thread_id).cloned().unwrap_or_default(),
        attachments: thread_draft_attachments
            .get(thread_id)
            .cloned()
            .unwrap_or_default(),
        capabilities: thread_draft_capabilities
            .get(thread_id)
            .cloned()
            .unwrap_or_default(),
        permission_mode: thread_draft_permission_modes
            .get(thread_id)
            .copied()
            .unwrap_or_else(default_composer_permission_mode),
    }
}

pub fn clear_all_composer_drafts<Attachment, Capability>(
    thread_drafts: &mut HashMap<String, String>,
    thread_draft_attachments: &mut HashMap<String, Vec<Attachment>>,
    thread_draft_capabilities: &mut HashMap<String, Vec<Capability>>,
    thread_draft_permission_modes: &mut HashMap<String, TurnPermissionMode>,
) {
    thread_drafts.clear();
    thread_draft_attachments.clear();
    thread_draft_capabilities.clear();
    thread_draft_permission_modes.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_draft_text_trims_only_trailing_whitespace() {
        assert_eq!(normalize_composer_draft_text("  hello \n"), "  hello");
    }

    #[test]
    fn remember_thread_draft_inserts_or_clears_empty_state() {
        let mut texts = HashMap::new();
        let mut attachments = HashMap::new();
        let mut capabilities = HashMap::new();
        let mut permission_modes = HashMap::new();

        assert!(remember_thread_composer_draft(
            "thread_a",
            "draft".to_owned(),
            vec!["attachment".to_owned()],
            Vec::<String>::new(),
            TurnPermissionMode::Supervised,
            &mut texts,
            &mut attachments,
            &mut capabilities,
            &mut permission_modes,
        ));
        assert_eq!(texts.get("thread_a").map(String::as_str), Some("draft"));
        assert_eq!(
            permission_modes.get("thread_a").copied(),
            Some(TurnPermissionMode::Supervised)
        );

        assert!(!remember_thread_composer_draft(
            "thread_a",
            String::new(),
            Vec::<String>::new(),
            Vec::<String>::new(),
            default_composer_permission_mode(),
            &mut texts,
            &mut attachments,
            &mut capabilities,
            &mut permission_modes,
        ));
        assert!(!texts.contains_key("thread_a"));
        assert!(!attachments.contains_key("thread_a"));
        assert!(!capabilities.contains_key("thread_a"));
        assert!(!permission_modes.contains_key("thread_a"));
    }

    #[test]
    fn remember_thread_draft_keeps_non_default_permission_mode() {
        let mut texts = HashMap::new();
        let mut attachments = HashMap::new();
        let mut capabilities = HashMap::new();
        let mut permission_modes = HashMap::new();

        assert!(remember_thread_composer_draft(
            "thread_a",
            String::new(),
            Vec::<String>::new(),
            Vec::<String>::new(),
            TurnPermissionMode::AutoAcceptEdits,
            &mut texts,
            &mut attachments,
            &mut capabilities,
            &mut permission_modes,
        ));

        let restored = restore_thread_composer_draft(
            "thread_a",
            &texts,
            &attachments,
            &capabilities,
            &permission_modes,
        );
        assert_eq!(
            restored.permission_mode,
            TurnPermissionMode::AutoAcceptEdits
        );
        assert!(!restored.is_empty());
    }

    #[test]
    fn restore_thread_draft_returns_default_for_missing_thread() {
        let mut texts = HashMap::new();
        let mut attachments = HashMap::new();
        let capabilities = HashMap::new();
        let permission_modes = HashMap::new();

        texts.insert("thread_a".to_owned(), "draft".to_owned());
        attachments.insert("thread_a".to_owned(), vec!["a.txt".to_owned()]);

        let restored = restore_thread_composer_draft(
            "thread_a",
            &texts,
            &attachments,
            &capabilities,
            &permission_modes,
        );
        assert_eq!(restored.text, "draft");
        assert_eq!(restored.attachments, vec!["a.txt".to_owned()]);
        assert!(restored.capabilities.is_empty());
        assert_eq!(restored.permission_mode, TurnPermissionMode::FullAccess);

        let missing: ComposerDraft<String, String> = restore_thread_composer_draft(
            "missing",
            &texts,
            &attachments,
            &capabilities,
            &permission_modes,
        );
        assert!(missing.is_empty());
        assert_eq!(missing.permission_mode, TurnPermissionMode::FullAccess);
    }

    #[test]
    fn lifecycle_switch_remembers_current_and_restores_target_domain() {
        let mut current = ComposerDomainDraft {
            text: "first".to_owned(),
            domain: ComposerDomainState::default(),
        };
        current.domain.model_manually_selected = true;
        current.domain.selected_model = Some("gpt-5".to_owned());
        let mut fallback = ComposerDomainDraft {
            text: "second".to_owned(),
            domain: ComposerDomainState::default(),
        };
        fallback.domain.selected_permission_mode = TurnPermissionMode::Supervised;

        let switched = reduce_composer_draft_lifecycle(
            &ComposerDraftLifecycleState::default(),
            ComposerDraftLifecycleAction::SwitchThread {
                current_thread_id: Some("thread-a".to_owned()),
                current_draft: Some(current.clone()),
                target_thread_id: "thread-b".to_owned(),
                fallback: fallback.clone(),
            },
        );

        assert_eq!(switched.state.drafts.get("thread-a"), Some(&current));
        assert_eq!(switched.state.drafts.get("thread-b"), Some(&fallback));
        assert_eq!(switched.restored_draft, Some(fallback));
    }

    #[test]
    fn thread_switch_fallback_preserves_preferences_without_leaking_payload() {
        let mut domain = ComposerDomainState::default();
        domain
            .attachments
            .push(crate::composer::attachments::ComposerAttachment {
                file_name: "private.txt".to_owned(),
                path: "/tmp/private.txt".to_owned(),
                kind: crate::composer::attachments::ComposerAttachmentKind::File,
                upload_state: crate::composer::attachments::ComposerAttachmentUploadState::Local,
            });
        let capability_skill_id = pioneer_protocol::SkillId::new("S".repeat(21)).expect("skill id");
        domain
            .capabilities
            .push(crate::composer::capabilities::ComposerCapability {
                id: pioneer_protocol::skill_capability_key(&capability_skill_id),
                kind: crate::composer::capabilities::ComposerCapabilityKind::Skill {
                    skill_id: capability_skill_id,
                    owner: Some("tests".to_owned()),
                    slug: "skill".to_owned(),
                    source_kind: "user".to_owned(),
                },
                label: "Skill".to_owned(),
            });
        domain.skill_selections.push(
            crate::composer::skill_selection::ComposerSkillSelection::Skill {
                skill_id: pioneer_protocol::SkillId::new("T".repeat(21)).expect("skill id"),
                pack_id: None,
            },
        );
        domain.selected_model = Some("gpt-5".to_owned());
        domain.selected_permission_mode = TurnPermissionMode::Supervised;

        let fallback = composer_thread_switch_fallback(domain);

        assert!(fallback.text.is_empty());
        assert!(fallback.domain.attachments.is_empty());
        assert!(fallback.domain.capabilities.is_empty());
        assert!(fallback.domain.skill_selections.is_empty());
        assert_eq!(fallback.domain.selected_model.as_deref(), Some("gpt-5"));
        assert_eq!(
            fallback.domain.selected_permission_mode,
            TurnPermissionMode::Supervised
        );
    }

    #[test]
    fn lifecycle_clear_thread_does_not_touch_other_drafts() {
        let draft = ComposerDomainDraft {
            text: "draft".to_owned(),
            domain: ComposerDomainState::default(),
        };
        let state = ComposerDraftLifecycleState {
            drafts: BTreeMap::from([
                ("thread-a".to_owned(), draft.clone()),
                ("thread-b".to_owned(), draft),
            ]),
        };

        let cleared = reduce_composer_draft_lifecycle(
            &state,
            ComposerDraftLifecycleAction::ClearThread {
                thread_id: "thread-a".to_owned(),
            },
        );

        assert!(!cleared.state.drafts.contains_key("thread-a"));
        assert!(cleared.state.drafts.contains_key("thread-b"));
    }
}
