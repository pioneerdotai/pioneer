use super::*;
use pioneer_client::composer::model_selection::{
    ComposerModelSelection, ComposerModelSelectionState,
};
use pioneer_client::providers::list as provider_list;
use pioneer_client::providers::presentation::{
    ProviderModelDisplayKey, ProviderModelDisplayState, provider_model_display_key,
    provider_model_display_models_params, resolve_provider_model_display_from_response,
};
use pioneer_client::state::selectors as client_selectors;

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
        self.clear_composer_reasoning_effort();
    }

    pub(in crate::app) fn has_complete_composer_model_selection(&self) -> bool {
        self.composer_model_selection_state()
            .has_complete_selection()
    }

    pub(in crate::app) fn set_composer_reasoning_effort_from_user(
        &mut self,
        effort: Option<String>,
    ) {
        let mut state = self.composer_model_selection_state();
        state.set_reasoning_effort_from_user(effort);
        self.apply_composer_model_selection_state(state);
    }

    pub(in crate::app) fn clear_composer_reasoning_effort(&mut self) {
        self.composer_selected_reasoning_effort = None;
    }

    pub(in crate::app) fn composer_model_display_state(
        &mut self,
        cx: &mut Context<Self>,
    ) -> ProviderModelDisplayState {
        let Some(key) = self.composer_model_display_key() else {
            return if self.composer_model_selection_pending() {
                ProviderModelDisplayState::Loading
            } else {
                ProviderModelDisplayState::Missing
            };
        };

        if let Some(label) = self.composer_model_display_cache.get(&key) {
            return label
                .clone()
                .map(ProviderModelDisplayState::Label)
                .unwrap_or(ProviderModelDisplayState::Missing);
        }

        if self.composer_model_display_loading_key.as_ref() != Some(&key) {
            self.composer_model_display_loading_key = Some(key.clone());
            self.spawn_composer_model_display_resolve(key, cx);
        }

        ProviderModelDisplayState::Loading
    }

    fn composer_model_selection_pending(&self) -> bool {
        if self.composer_model_selection_manually_selected {
            return false;
        }

        if self.gateway.connection_state.is_transitioning()
            || self.workspaces_loading
            || self.thread_list_loading
            || self.thread_start_requested
            || self.thread_start.in_progress
            || self.thread_start.pending_thread_id.is_some()
        {
            return true;
        }

        self.current_active_thread_id()
            .is_some_and(|thread_id| self.is_thread_timeline_loading(thread_id))
    }

    fn composer_model_selection_state(&self) -> ComposerModelSelectionState {
        ComposerModelSelectionState::new_with_reasoning_effort(
            self.composer_selected_provider.clone(),
            self.composer_selected_model.clone(),
            self.composer_selected_reasoning_effort.clone(),
            self.composer_model_selection_manually_selected,
        )
    }

    fn apply_composer_model_selection_state(&mut self, state: ComposerModelSelectionState) {
        let (provider, model, reasoning_effort, manually_selected) =
            state.into_parts_with_reasoning_effort();
        self.composer_selected_provider = provider;
        self.composer_selected_model = model;
        self.composer_selected_reasoning_effort = reasoning_effort;
        self.composer_model_selection_manually_selected = manually_selected;
        if self.composer_selected_provider_is_cli_runtime() {
            self.composer_capabilities.clear();
            self.composer_upload_in_progress = false;
            self.composer_upload_error = None;
        }
    }

    fn composer_model_display_key(&self) -> Option<ProviderModelDisplayKey> {
        let workspace_id = self.model_selector_workspace_id();
        provider_model_display_key(
            Some(workspace_id.as_str()),
            self.composer_selected_provider.as_deref(),
            self.composer_selected_model.as_deref(),
        )
    }

    fn spawn_composer_model_display_resolve(
        &self,
        key: ProviderModelDisplayKey,
        cx: &mut Context<Self>,
    ) {
        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = if let Some(runtime_id) =
                    provider_list::runtime_id_from_cli_runtime_provider_key(key.provider.as_str())
                {
                    let provider_key = key.provider.clone();
                    let params = provider_list::cli_runtime_list_models_params(
                        key.workspace_id.clone(),
                        runtime_id.to_owned(),
                    );
                    cx.background_spawn(async move {
                        ws_sender.cli_runtime_list_models(params).map(|response| {
                            provider_list::provider_models_response_from_cli_runtime_models_response(
                                provider_key,
                                response,
                            )
                        })
                    })
                    .await
                } else {
                    let params = provider_model_display_models_params(&key);
                    cx.background_spawn(async move { ws_sender.provider_list_models(params) })
                        .await
                };
                let _ = this.update(&mut cx, |view, cx| {
                    if view.composer_model_display_loading_key.as_ref() != Some(&key) {
                        return;
                    }

                    let label = result.ok().and_then(|response| {
                        resolve_provider_model_display_from_response(&key, &response).label
                    });

                    view.composer_model_display_cache.insert(key.clone(), label);
                    view.composer_model_display_loading_key = None;
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn resolve_composer_model_selection(&self) -> Option<ComposerModelSelection> {
        let active_thread_id = self.current_active_thread_id()?;
        let active_workspace_id = self
            .active_workspace_id()
            .or_else(|| self.thread_workspace_id(active_thread_id));

        client_selectors::resolve_composer_model_selection_from(
            Some(active_thread_id),
            active_workspace_id,
            &self.thread_coordinators,
        )
    }
}
