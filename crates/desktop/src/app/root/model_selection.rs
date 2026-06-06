use super::*;
use pioneer_client::composer::model_selection::{
    self as composer_model_selection, ComposerModelSelection, ComposerModelSelectionCandidate,
    ComposerModelSelectionState,
};
use pioneer_protocol::Thread;

impl PioneerDesktop {
    pub(in crate::app) fn set_composer_model_selection_from_user(
        &mut self,
        provider: Option<String>,
        model: Option<String>,
    ) {
        let mut state = self.composer_model_selection_state();
        state.set_from_user(provider, model);
        self.apply_composer_model_selection_state(state);
    }

    pub(in crate::app) fn sync_composer_model_selection_for_active_thread(&mut self) {
        let selection = self.resolve_composer_model_selection();
        let mut state = self.composer_model_selection_state();
        state.sync_resolved_selection(selection);
        self.apply_composer_model_selection_state(state);
    }

    pub(in crate::app) fn reset_composer_model_selection_for_active_thread(&mut self) {
        let selection = self.resolve_composer_model_selection();
        let mut state = self.composer_model_selection_state();
        state.reset_to_resolved_selection(selection);
        self.apply_composer_model_selection_state(state);
    }

    pub(in crate::app) fn has_complete_composer_model_selection(&self) -> bool {
        self.composer_model_selection_state()
            .has_complete_selection()
    }

    fn composer_model_selection_state(&self) -> ComposerModelSelectionState {
        ComposerModelSelectionState::new(
            self.composer_selected_provider.clone(),
            self.composer_selected_model.clone(),
            self.composer_model_selection_manually_selected,
        )
    }

    fn apply_composer_model_selection_state(&mut self, state: ComposerModelSelectionState) {
        let (provider, model, manually_selected) = state.into_parts();
        self.composer_selected_provider = provider;
        self.composer_selected_model = model;
        self.composer_model_selection_manually_selected = manually_selected;
    }

    fn resolve_composer_model_selection(&self) -> Option<ComposerModelSelection> {
        let active_thread_id = self.current_active_thread_id()?;
        let active_workspace_id = self
            .active_workspace_id()
            .or_else(|| self.thread_workspace_id(active_thread_id));

        composer_model_selection::resolve_composer_model_selection(
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
