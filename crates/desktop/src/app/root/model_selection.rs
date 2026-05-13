use super::*;
use pioneer_protocol::Thread;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ComposerModelSelection {
    pub(super) provider: String,
    pub(super) model: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ComposerModelSelectionCandidate {
    pub(super) thread_id: String,
    pub(super) workspace_id: String,
    pub(super) updated_at: i64,
    pub(super) has_turns: bool,
    pub(super) selection: Option<ComposerModelSelection>,
}

impl PioneerDesktop {
    pub(in crate::app) fn set_composer_model_selection_from_user(
        &mut self,
        provider: Option<String>,
        model: Option<String>,
    ) {
        self.composer_selected_provider = provider;
        self.composer_selected_model = model;
        self.composer_model_selection_manually_selected = true;
    }

    pub(in crate::app) fn sync_composer_model_selection_for_active_thread(&mut self) {
        if self.composer_model_selection_manually_selected {
            return;
        }

        self.apply_composer_model_selection(self.resolve_composer_model_selection());
    }

    pub(in crate::app) fn reset_composer_model_selection_for_active_thread(&mut self) {
        self.composer_model_selection_manually_selected = false;
        self.apply_composer_model_selection(self.resolve_composer_model_selection());
    }

    pub(in crate::app) fn has_complete_composer_model_selection(&self) -> bool {
        self.composer_selected_provider
            .as_deref()
            .is_some_and(|provider| !provider.trim().is_empty())
            && self
                .composer_selected_model
                .as_deref()
                .is_some_and(|model| !model.trim().is_empty())
    }

    fn apply_composer_model_selection(&mut self, selection: Option<ComposerModelSelection>) {
        match selection {
            Some(selection) => {
                self.composer_selected_provider = Some(selection.provider);
                self.composer_selected_model = Some(selection.model);
            }
            None => {
                self.composer_selected_provider = None;
                self.composer_selected_model = None;
            }
        }
    }

    fn resolve_composer_model_selection(&self) -> Option<ComposerModelSelection> {
        let active_thread_id = self.current_active_thread_id()?;
        let active_workspace_id = self.thread_workspace_id(active_thread_id);

        resolve_composer_model_selection(
            Some(active_thread_id),
            active_workspace_id,
            self.model_selection_candidates(),
        )
    }

    fn model_selection_candidates(&self) -> Vec<ComposerModelSelectionCandidate> {
        self.thread_coordinators
            .iter()
            .filter_map(|(thread_id, coordinator)| {
                let thread = coordinator.thread()?;
                Some(ComposerModelSelectionCandidate {
                    thread_id: thread_id.clone(),
                    workspace_id: coordinator.workspace_id.clone(),
                    updated_at: coordinator.updated_at(),
                    has_turns: self.thread_has_known_turns(thread_id.as_str(), thread),
                    selection: ComposerModelSelection::from_thread(thread),
                })
            })
            .collect()
    }

    fn thread_has_known_turns(&self, thread_id: &str, thread: &Thread) -> bool {
        !thread.turns.is_empty()
            || self
                .thread_conversation(thread_id)
                .is_some_and(|conversation| !conversation.projection().turns.is_empty())
    }
}

impl ComposerModelSelection {
    fn from_thread(thread: &Thread) -> Option<Self> {
        let provider = thread.model_provider.trim();
        let model = thread.model.trim();

        if provider.is_empty() || model.is_empty() {
            return None;
        }

        Some(Self {
            provider: provider.to_owned(),
            model: model.to_owned(),
        })
    }
}

pub(super) fn resolve_composer_model_selection(
    active_thread_id: Option<&str>,
    active_workspace_id: Option<&str>,
    candidates: Vec<ComposerModelSelectionCandidate>,
) -> Option<ComposerModelSelection> {
    let active_thread_id = active_thread_id?;

    if let Some(active) = candidates
        .iter()
        .find(|candidate| candidate.thread_id == active_thread_id)
    {
        if active.has_turns {
            return active.selection.clone();
        }
    }

    let workspace_id = active_workspace_id.map(str::to_owned).or_else(|| {
        candidates
            .iter()
            .find(|candidate| candidate.thread_id == active_thread_id)
            .map(|candidate| candidate.workspace_id.clone())
    })?;

    candidates
        .into_iter()
        .filter(|candidate| candidate.workspace_id == workspace_id)
        .filter(|candidate| candidate.thread_id != active_thread_id)
        .filter(|candidate| candidate.has_turns)
        .filter_map(|candidate| {
            let selection = candidate.selection.clone()?;
            Some((candidate, selection))
        })
        .max_by(|(lhs, _), (rhs, _)| {
            lhs.updated_at
                .cmp(&rhs.updated_at)
                .then_with(|| lhs.thread_id.cmp(&rhs.thread_id))
        })
        .map(|(_, selection)| selection)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selection(provider: &str, model: &str) -> Option<ComposerModelSelection> {
        Some(ComposerModelSelection {
            provider: provider.to_owned(),
            model: model.to_owned(),
        })
    }

    fn candidate(
        thread_id: &str,
        workspace_id: &str,
        updated_at: i64,
        has_turns: bool,
        provider: &str,
        model: &str,
    ) -> ComposerModelSelectionCandidate {
        ComposerModelSelectionCandidate {
            thread_id: thread_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            updated_at,
            has_turns,
            selection: selection(provider, model),
        }
    }

    #[::core::prelude::v1::test]
    fn active_thread_with_turns_wins_over_newer_workspace_turn() {
        let resolved = resolve_composer_model_selection(
            Some("thread_a"),
            Some("ws"),
            vec![
                candidate("thread_a", "ws", 10, true, "openai", "gpt-5.4"),
                candidate("thread_b", "ws", 20, true, "anthropic", "claude-sonnet-4.5"),
            ],
        );

        assert_eq!(resolved, selection("openai", "gpt-5.4"));
    }

    #[::core::prelude::v1::test]
    fn empty_active_thread_uses_latest_workspace_turn() {
        let resolved = resolve_composer_model_selection(
            Some("thread_empty"),
            Some("ws"),
            vec![
                candidate("thread_empty", "ws", 30, false, "openai", "default"),
                candidate("thread_old", "ws", 10, true, "openai", "gpt-5.4"),
                candidate(
                    "thread_new",
                    "ws",
                    20,
                    true,
                    "openrouter",
                    "anthropic/claude",
                ),
            ],
        );

        assert_eq!(resolved, selection("openrouter", "anthropic/claude"));
    }

    #[::core::prelude::v1::test]
    fn empty_workspace_keeps_model_selection_blank() {
        let resolved = resolve_composer_model_selection(
            Some("thread_empty"),
            Some("ws"),
            vec![candidate(
                "thread_empty",
                "ws",
                30,
                false,
                "openai",
                "default",
            )],
        );

        assert_eq!(resolved, None);
    }

    #[::core::prelude::v1::test]
    fn fallback_ignores_other_workspaces() {
        let resolved = resolve_composer_model_selection(
            Some("thread_empty"),
            Some("ws_a"),
            vec![
                candidate("thread_empty", "ws_a", 30, false, "openai", "default"),
                candidate("thread_b", "ws_b", 40, true, "anthropic", "claude"),
                candidate("thread_a", "ws_a", 20, true, "openai", "gpt-5.4"),
            ],
        );

        assert_eq!(resolved, selection("openai", "gpt-5.4"));
    }
}
