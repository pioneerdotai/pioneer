use super::*;
use pioneer_client::composer::state_machine::{
    ComposerDomainAction, ComposerDomainState, ComposerDomainTransition,
    reduce_composer_domain_state,
};

impl PioneerDesktop {
    pub(in crate::app) fn composer_domain_state(&self) -> ComposerDomainState {
        ComposerDomainState {
            attachments: self.composer_attachments.clone(),
            capabilities: self.composer_capabilities.clone(),
            selected_mode: self.composer_turn_mode,
            mode_manually_selected: self.composer_mode_manually_selected,
            selected_provider: self.composer_selected_provider.clone(),
            capability_target: self.composer_capability_target,
            selected_model: self.composer_selected_model.clone(),
            selected_reasoning_effort: self.composer_selected_reasoning_effort.clone(),
            selected_permission_mode: self.composer_permission_mode,
            model_manually_selected: self.composer_model_selection_manually_selected,
        }
    }

    pub(in crate::app) fn apply_composer_domain_state(&mut self, state: ComposerDomainState) {
        self.composer_attachments = state.attachments;
        self.composer_capabilities = state.capabilities;
        self.composer_turn_mode = state.selected_mode;
        self.composer_mode_manually_selected = state.mode_manually_selected;
        self.composer_selected_provider = state.selected_provider;
        self.composer_capability_target = state.capability_target;
        self.composer_selected_model = state.selected_model;
        self.composer_selected_reasoning_effort = state.selected_reasoning_effort;
        self.composer_permission_mode = state.selected_permission_mode;
        self.composer_model_selection_manually_selected = state.model_manually_selected;
    }

    pub(in crate::app) fn reduce_composer_domain(
        &mut self,
        action: ComposerDomainAction,
    ) -> ComposerDomainTransition {
        let transition = reduce_composer_domain_state(&self.composer_domain_state(), action);
        self.apply_composer_domain_state(transition.state.clone());
        transition
    }
}
