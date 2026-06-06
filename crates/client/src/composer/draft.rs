//! Composer draft state.

use std::collections::HashMap;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerDraft<Attachment, Capability> {
    pub text: String,
    pub attachments: Vec<Attachment>,
    pub capabilities: Vec<Capability>,
}

impl<Attachment, Capability> ComposerDraft<Attachment, Capability> {
    pub fn is_empty(&self) -> bool {
        self.text.is_empty() && self.attachments.is_empty() && self.capabilities.is_empty()
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
    thread_drafts: &mut HashMap<String, String>,
    thread_draft_attachments: &mut HashMap<String, Vec<Attachment>>,
    thread_draft_capabilities: &mut HashMap<String, Vec<Capability>>,
) -> bool {
    if text.is_empty() && attachments.is_empty() && capabilities.is_empty() {
        clear_thread_composer_draft(
            thread_id,
            thread_drafts,
            thread_draft_attachments,
            thread_draft_capabilities,
        );
        return false;
    }

    thread_drafts.insert(thread_id.to_owned(), text);
    thread_draft_attachments.insert(thread_id.to_owned(), attachments);
    thread_draft_capabilities.insert(thread_id.to_owned(), capabilities);
    true
}

pub fn clear_thread_composer_draft<Attachment, Capability>(
    thread_id: &str,
    thread_drafts: &mut HashMap<String, String>,
    thread_draft_attachments: &mut HashMap<String, Vec<Attachment>>,
    thread_draft_capabilities: &mut HashMap<String, Vec<Capability>>,
) {
    thread_drafts.remove(thread_id);
    thread_draft_attachments.remove(thread_id);
    thread_draft_capabilities.remove(thread_id);
}

pub fn restore_thread_composer_draft<Attachment: Clone, Capability: Clone>(
    thread_id: &str,
    thread_drafts: &HashMap<String, String>,
    thread_draft_attachments: &HashMap<String, Vec<Attachment>>,
    thread_draft_capabilities: &HashMap<String, Vec<Capability>>,
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
    }
}

pub fn clear_all_composer_drafts<Attachment, Capability>(
    thread_drafts: &mut HashMap<String, String>,
    thread_draft_attachments: &mut HashMap<String, Vec<Attachment>>,
    thread_draft_capabilities: &mut HashMap<String, Vec<Capability>>,
) {
    thread_drafts.clear();
    thread_draft_attachments.clear();
    thread_draft_capabilities.clear();
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

        assert!(remember_thread_composer_draft(
            "thread_a",
            "draft".to_owned(),
            vec!["attachment".to_owned()],
            Vec::<String>::new(),
            &mut texts,
            &mut attachments,
            &mut capabilities,
        ));
        assert_eq!(texts.get("thread_a").map(String::as_str), Some("draft"));

        assert!(!remember_thread_composer_draft(
            "thread_a",
            String::new(),
            Vec::<String>::new(),
            Vec::<String>::new(),
            &mut texts,
            &mut attachments,
            &mut capabilities,
        ));
        assert!(!texts.contains_key("thread_a"));
        assert!(!attachments.contains_key("thread_a"));
        assert!(!capabilities.contains_key("thread_a"));
    }

    #[test]
    fn restore_thread_draft_returns_default_for_missing_thread() {
        let mut texts = HashMap::new();
        let mut attachments = HashMap::new();
        let capabilities = HashMap::new();

        texts.insert("thread_a".to_owned(), "draft".to_owned());
        attachments.insert("thread_a".to_owned(), vec!["a.txt".to_owned()]);

        let restored =
            restore_thread_composer_draft("thread_a", &texts, &attachments, &capabilities);
        assert_eq!(restored.text, "draft");
        assert_eq!(restored.attachments, vec!["a.txt".to_owned()]);
        assert!(restored.capabilities.is_empty());

        let missing: ComposerDraft<String, String> =
            restore_thread_composer_draft("missing", &texts, &attachments, &capabilities);
        assert!(missing.is_empty());
    }
}
