//! Model, provider, and composer mode selection helpers.

use pioneer_protocol::{Thread, ThreadMode};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerModelSelection {
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_reasoning_effort: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerModelSelectionCandidate {
    pub thread_id: String,
    pub workspace_id: String,
    pub updated_at: i64,
    pub has_turns: bool,
    pub selection: Option<ComposerModelSelection>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerModelSelectionState {
    pub selected_provider: Option<String>,
    pub selected_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_reasoning_effort: Option<String>,
    pub manually_selected: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ModelSelectorSelection {
    pub provider: Option<String>,
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_reasoning_effort: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ModelProviderSelectionUpdate {
    pub selected_provider: Option<String>,
    pub selected_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_reasoning_effort: Option<String>,
    pub clear_models: bool,
    pub loading_models: bool,
}

impl ComposerModelSelection {
    pub fn from_thread(thread: &Thread) -> Option<Self> {
        composer_model_selection_from_parts_with_reasoning_effort(
            Some(thread.model_provider.as_str()),
            Some(thread.model.as_str()),
            thread.reasoning_effort.as_deref(),
        )
    }

    pub fn into_parts(self) -> (Option<String>, Option<String>) {
        (Some(self.provider), Some(self.model))
    }

    pub fn into_parts_with_reasoning_effort(
        self,
    ) -> (Option<String>, Option<String>, Option<String>) {
        (
            Some(self.provider),
            Some(self.model),
            self.selected_reasoning_effort,
        )
    }
}

impl ComposerModelSelectionState {
    pub fn new(
        selected_provider: Option<String>,
        selected_model: Option<String>,
        manually_selected: bool,
    ) -> Self {
        Self::new_with_reasoning_effort(selected_provider, selected_model, None, manually_selected)
    }

    pub fn new_with_reasoning_effort(
        selected_provider: Option<String>,
        selected_model: Option<String>,
        selected_reasoning_effort: Option<String>,
        manually_selected: bool,
    ) -> Self {
        Self {
            selected_provider,
            selected_model,
            selected_reasoning_effort,
            manually_selected,
        }
    }

    pub fn set_from_user(
        &mut self,
        selected_provider: Option<String>,
        selected_model: Option<String>,
    ) -> bool {
        self.set_model_selection_from_user(selected_provider, selected_model)
    }

    pub fn set_model_selection_from_user(
        &mut self,
        selected_provider: Option<String>,
        selected_model: Option<String>,
    ) -> bool {
        let selection_changed =
            self.selected_provider != selected_provider || self.selected_model != selected_model;
        let changed = self.selected_provider != selected_provider
            || self.selected_model != selected_model
            || !self.manually_selected;
        self.selected_provider = selected_provider;
        self.selected_model = selected_model;
        if selection_changed {
            self.selected_reasoning_effort = None;
        }
        self.manually_selected = true;
        changed
    }

    pub fn select_provider_from_user(&mut self, selected_provider: Option<String>) -> bool {
        self.set_model_selection_from_user(selected_provider, None)
    }

    pub fn select_model_from_user(&mut self, selected_model: Option<String>) -> bool {
        self.set_model_selection_from_user(self.selected_provider.clone(), selected_model)
    }

    pub fn set_reasoning_effort_from_user(&mut self, effort: Option<String>) -> bool {
        if self.selected_reasoning_effort == effort {
            return false;
        }

        self.selected_reasoning_effort = effort;
        true
    }

    pub fn sync_resolved_selection(&mut self, selection: Option<ComposerModelSelection>) -> bool {
        if self.manually_selected {
            return false;
        }

        self.apply_resolved_selection(selection)
    }

    pub fn reset_to_resolved_selection(
        &mut self,
        selection: Option<ComposerModelSelection>,
    ) -> bool {
        let changed = self.manually_selected;
        self.manually_selected = false;
        self.apply_resolved_selection(selection) || changed
    }

    pub fn apply_resolved_selection(&mut self, selection: Option<ComposerModelSelection>) -> bool {
        let (selected_provider, selected_model, selected_reasoning_effort) = selection
            .map(ComposerModelSelection::into_parts_with_reasoning_effort)
            .unwrap_or((None, None, None));
        let changed = self.selected_provider != selected_provider
            || self.selected_model != selected_model
            || self.selected_reasoning_effort != selected_reasoning_effort;
        self.selected_provider = selected_provider;
        self.selected_model = selected_model;
        self.selected_reasoning_effort = selected_reasoning_effort;
        changed
    }

    pub fn has_complete_selection(&self) -> bool {
        has_complete_composer_model_selection(
            self.selected_provider.as_deref(),
            self.selected_model.as_deref(),
        )
    }

    pub fn into_parts(self) -> (Option<String>, Option<String>, bool) {
        (
            self.selected_provider,
            self.selected_model,
            self.manually_selected,
        )
    }

    pub fn into_parts_with_reasoning_effort(
        self,
    ) -> (Option<String>, Option<String>, Option<String>, bool) {
        (
            self.selected_provider,
            self.selected_model,
            self.selected_reasoning_effort,
            self.manually_selected,
        )
    }
}

pub fn composer_model_selection_from_parts(
    provider: Option<&str>,
    model: Option<&str>,
) -> Option<ComposerModelSelection> {
    composer_model_selection_from_parts_with_reasoning_effort(provider, model, None)
}

pub fn composer_model_selection_from_parts_with_reasoning_effort(
    provider: Option<&str>,
    model: Option<&str>,
    selected_reasoning_effort: Option<&str>,
) -> Option<ComposerModelSelection> {
    let provider = provider?.trim();
    let model = model?.trim();
    let selected_reasoning_effort = selected_reasoning_effort
        .map(str::trim)
        .filter(|effort| !effort.is_empty())
        .map(str::to_owned);

    if provider.is_empty() || model.is_empty() {
        return None;
    }

    Some(ComposerModelSelection {
        provider: provider.to_owned(),
        model: model.to_owned(),
        selected_reasoning_effort,
    })
}

pub fn has_complete_composer_model_selection(provider: Option<&str>, model: Option<&str>) -> bool {
    composer_model_selection_from_parts(provider, model).is_some()
}

pub fn resolve_composer_model_selection(
    active_thread_id: Option<&str>,
    active_workspace_id: Option<&str>,
    candidates: Vec<ComposerModelSelectionCandidate>,
) -> Option<ComposerModelSelection> {
    let active_candidate = active_thread_id.and_then(|active_thread_id| {
        candidates
            .iter()
            .find(|candidate| candidate.thread_id == active_thread_id)
    });
    let workspace_id = active_workspace_id.map(str::to_owned).or_else(|| {
        let active_thread_id = active_thread_id?;
        candidates
            .iter()
            .find(|candidate| candidate.thread_id == active_thread_id)
            .map(|candidate| candidate.workspace_id.clone())
    })?;

    if let Some(active) = active_candidate {
        if active.workspace_id == workspace_id && active.has_turns {
            return active.selection.clone();
        }
    }

    candidates
        .into_iter()
        .filter(|candidate| candidate.workspace_id == workspace_id)
        .filter(|candidate| Some(candidate.thread_id.as_str()) != active_thread_id)
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

pub fn default_composer_turn_mode() -> ThreadMode {
    ThreadMode::Message
}

pub fn composer_turn_mode_options() -> [ThreadMode; 3] {
    [ThreadMode::Message, ThreadMode::Chat, ThreadMode::Agent]
}

pub fn set_composer_turn_mode(current: &mut ThreadMode, mode: ThreadMode) -> bool {
    if *current == mode {
        return false;
    }

    *current = mode;
    true
}

pub fn select_model_provider(provider_name: String) -> ModelProviderSelectionUpdate {
    ModelProviderSelectionUpdate {
        selected_provider: Some(provider_name),
        selected_model: None,
        selected_reasoning_effort: None,
        clear_models: true,
        loading_models: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus};

    fn selection(provider: &str, model: &str) -> Option<ComposerModelSelection> {
        selection_with_effort(provider, model, None)
    }

    fn selection_with_effort(
        provider: &str,
        model: &str,
        selected_reasoning_effort: Option<&str>,
    ) -> Option<ComposerModelSelection> {
        Some(ComposerModelSelection {
            provider: provider.to_owned(),
            model: model.to_owned(),
            selected_reasoning_effort: selected_reasoning_effort.map(str::to_owned),
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
        candidate_with_effort(
            thread_id,
            workspace_id,
            updated_at,
            has_turns,
            provider,
            model,
            None,
        )
    }

    fn candidate_with_effort(
        thread_id: &str,
        workspace_id: &str,
        updated_at: i64,
        has_turns: bool,
        provider: &str,
        model: &str,
        selected_reasoning_effort: Option<&str>,
    ) -> ComposerModelSelectionCandidate {
        ComposerModelSelectionCandidate {
            thread_id: thread_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            updated_at,
            has_turns,
            selection: selection_with_effort(provider, model, selected_reasoning_effort),
        }
    }

    fn thread_with_effort(
        provider: &str,
        model: &str,
        selected_reasoning_effort: Option<&str>,
    ) -> Thread {
        Thread {
            workspace_id: "ws".to_owned(),
            id: "thread".to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: model.to_owned(),
            model_provider: provider.to_owned(),
            reasoning_effort: selected_reasoning_effort.map(str::to_owned),
            created_at: 1,
            updated_at: 1,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        }
    }

    #[test]
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

    #[test]
    fn active_thread_with_turns_restores_reasoning_effort() {
        let resolved = resolve_composer_model_selection(
            Some("thread_a"),
            Some("ws"),
            vec![
                candidate_with_effort(
                    "thread_a",
                    "ws",
                    10,
                    true,
                    "openai",
                    "gpt-5.4",
                    Some("high"),
                ),
                candidate("thread_b", "ws", 20, true, "anthropic", "claude-sonnet-4.5"),
            ],
        );

        assert_eq!(
            resolved,
            selection_with_effort("openai", "gpt-5.4", Some("high"))
        );
    }

    #[test]
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

    #[test]
    fn empty_active_thread_restores_latest_workspace_turn_reasoning_effort() {
        let resolved = resolve_composer_model_selection(
            Some("thread_empty"),
            Some("ws"),
            vec![
                candidate("thread_empty", "ws", 30, false, "openai", "default"),
                candidate_with_effort(
                    "thread_old",
                    "ws",
                    10,
                    true,
                    "openai",
                    "gpt-5.4",
                    Some("low"),
                ),
                candidate_with_effort(
                    "thread_new",
                    "ws",
                    20,
                    true,
                    "openrouter",
                    "anthropic/claude",
                    Some("high"),
                ),
            ],
        );

        assert_eq!(
            resolved,
            selection_with_effort("openrouter", "anthropic/claude", Some("high"))
        );
    }

    #[test]
    fn workspace_without_active_thread_uses_latest_workspace_turn() {
        let resolved = resolve_composer_model_selection(
            None,
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
                candidate("thread_other", "other", 40, true, "anthropic", "claude"),
            ],
        );

        assert_eq!(resolved, selection("openrouter", "anthropic/claude"));
    }

    #[test]
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

    #[test]
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

    #[test]
    fn active_thread_from_other_workspace_does_not_win_when_workspace_is_explicit() {
        let resolved = resolve_composer_model_selection(
            Some("thread_old_active"),
            Some("ws_b"),
            vec![
                candidate("thread_old_active", "ws_a", 30, true, "openai", "gpt-5.4"),
                candidate(
                    "thread_b",
                    "ws_b",
                    20,
                    true,
                    "openrouter",
                    "anthropic/claude",
                ),
            ],
        );

        assert_eq!(resolved, selection("openrouter", "anthropic/claude"));
    }

    #[test]
    fn model_selection_from_parts_trims_and_rejects_missing_values() {
        assert_eq!(
            composer_model_selection_from_parts(Some(" openai "), Some(" gpt-5.4 ")),
            selection("openai", "gpt-5.4")
        );
        assert!(composer_model_selection_from_parts(Some("openai"), Some(" ")).is_none());
        assert!(!has_complete_composer_model_selection(
            Some(" "),
            Some("gpt-5.4")
        ));
    }

    #[test]
    fn model_selection_from_thread_uses_thread_provider_model_and_reasoning_effort() {
        assert_eq!(
            ComposerModelSelection::from_thread(&thread_with_effort(
                " openai ",
                " gpt-5.4 ",
                Some(" high ")
            )),
            selection_with_effort("openai", "gpt-5.4", Some("high"))
        );
    }

    #[test]
    fn composer_model_selection_state_tracks_manual_override_and_reset() {
        let mut state = ComposerModelSelectionState::default();

        assert!(state.set_from_user(Some("openai".to_owned()), Some("gpt-5.4".to_owned()),));
        assert!(state.manually_selected);
        assert!(state.has_complete_selection());

        assert!(!state.sync_resolved_selection(selection("anthropic", "claude")));
        assert_eq!(state.selected_provider.as_deref(), Some("openai"));
        assert_eq!(state.selected_model.as_deref(), Some("gpt-5.4"));

        assert!(state.reset_to_resolved_selection(selection_with_effort(
            "anthropic",
            "claude",
            Some("high")
        )));
        assert!(!state.manually_selected);
        assert_eq!(state.selected_provider.as_deref(), Some("anthropic"));
        assert_eq!(state.selected_model.as_deref(), Some("claude"));
        assert_eq!(state.selected_reasoning_effort.as_deref(), Some("high"));

        assert!(state.sync_resolved_selection(None));
        assert!(!state.has_complete_selection());
    }

    #[test]
    fn provider_change_clears_model_and_reasoning_effort() {
        let mut state = ComposerModelSelectionState::new_with_reasoning_effort(
            Some("openai".to_owned()),
            Some("gpt-5.4".to_owned()),
            Some("high".to_owned()),
            true,
        );

        assert!(state.select_provider_from_user(Some("anthropic".to_owned())));

        assert_eq!(state.selected_provider.as_deref(), Some("anthropic"));
        assert!(state.selected_model.is_none());
        assert!(state.selected_reasoning_effort.is_none());
        assert!(state.manually_selected);
    }

    #[test]
    fn model_change_clears_reasoning_effort() {
        let mut state = ComposerModelSelectionState::new_with_reasoning_effort(
            Some("openai".to_owned()),
            Some("gpt-5.4".to_owned()),
            Some("high".to_owned()),
            true,
        );

        assert!(state.select_model_from_user(Some("gpt-5.5".to_owned())));

        assert_eq!(state.selected_provider.as_deref(), Some("openai"));
        assert_eq!(state.selected_model.as_deref(), Some("gpt-5.5"));
        assert!(state.selected_reasoning_effort.is_none());
    }

    #[test]
    fn unchanged_provider_and_model_preserve_reasoning_effort() {
        let mut state = ComposerModelSelectionState::new_with_reasoning_effort(
            Some("openai".to_owned()),
            Some("gpt-5.4".to_owned()),
            Some("high".to_owned()),
            true,
        );

        assert!(
            !state.set_model_selection_from_user(
                Some("openai".to_owned()),
                Some("gpt-5.4".to_owned())
            )
        );

        assert_eq!(state.selected_reasoning_effort.as_deref(), Some("high"));
    }

    #[test]
    fn resolved_selection_sync_restores_and_clears_reasoning_effort() {
        let mut state = ComposerModelSelectionState::new_with_reasoning_effort(
            Some("openai".to_owned()),
            Some("gpt-5.4".to_owned()),
            None,
            false,
        );

        assert!(state.sync_resolved_selection(selection_with_effort(
            "openai",
            "gpt-5.4",
            Some("high")
        )));
        assert_eq!(state.selected_reasoning_effort.as_deref(), Some("high"));

        assert!(!state.sync_resolved_selection(selection_with_effort(
            "openai",
            "gpt-5.4",
            Some("high")
        )));

        assert!(state.sync_resolved_selection(selection("openai", "gpt-5.4")));
        assert!(state.selected_reasoning_effort.is_none());

        assert!(state.sync_resolved_selection(selection_with_effort(
            "openai",
            "gpt-5.5",
            Some("max")
        )));
        assert_eq!(state.selected_provider.as_deref(), Some("openai"));
        assert_eq!(state.selected_model.as_deref(), Some("gpt-5.5"));
        assert_eq!(state.selected_reasoning_effort.as_deref(), Some("max"));
    }

    #[test]
    fn reset_to_resolved_selection_restores_reasoning_effort_when_model_selection_changes() {
        let mut state = ComposerModelSelectionState::new_with_reasoning_effort(
            Some("openai".to_owned()),
            Some("gpt-5.4".to_owned()),
            Some("high".to_owned()),
            true,
        );

        assert!(state.reset_to_resolved_selection(selection_with_effort(
            "anthropic",
            "claude",
            Some("max")
        )));

        assert!(!state.manually_selected);
        assert_eq!(state.selected_provider.as_deref(), Some("anthropic"));
        assert_eq!(state.selected_model.as_deref(), Some("claude"));
        assert_eq!(state.selected_reasoning_effort.as_deref(), Some("max"));
    }

    #[test]
    fn set_composer_turn_mode_reports_changes() {
        let mut mode = default_composer_turn_mode();

        assert!(!set_composer_turn_mode(&mut mode, ThreadMode::Message));
        assert!(set_composer_turn_mode(&mut mode, ThreadMode::Agent));
        assert_eq!(mode, ThreadMode::Agent);
        assert_eq!(
            composer_turn_mode_options(),
            [ThreadMode::Message, ThreadMode::Chat, ThreadMode::Agent]
        );
    }

    #[test]
    fn selecting_provider_resets_model_and_requests_model_loading() {
        let update = select_model_provider("openai".to_owned());

        assert_eq!(update.selected_provider.as_deref(), Some("openai"));
        assert_eq!(update.selected_model, None);
        assert_eq!(update.selected_reasoning_effort, None);
        assert!(update.clear_models);
        assert!(update.loading_models);
    }
}
