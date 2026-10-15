//! Self-improvement settings presentation and update plans.

use pioneer_protocol::{
    GatewaySelfImprovementModelSelection, GatewaySelfImprovementSettings, GatewaySettingsSnapshot,
    GatewaySettingsUpdate,
};

use crate::composer::model_selection::ModelSelectorSelection;
use crate::settings::gateway::GatewaySettingsUpdatePlan;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SelfImprovementModelSetting {
    Default,
    Reviewer,
}

pub fn current_self_improvement_settings(
    settings: Option<&GatewaySettingsSnapshot>,
) -> Option<GatewaySelfImprovementSettings> {
    settings.map(|settings| settings.self_improvement.clone())
}

pub fn configuration_is_incomplete(settings: &GatewaySelfImprovementSettings) -> bool {
    settings.enabled && settings.default_model.is_none()
}

/// Never show a cached operational status from a different workspace.
pub fn status_for_workspace<'a>(
    snapshot: Option<&'a GatewaySettingsSnapshot>,
    workspace_id: Option<&str>,
) -> Option<&'a pioneer_protocol::GatewaySelfImprovementStatus> {
    let workspace_id = workspace_id?;
    snapshot?
        .self_improvement_status
        .as_ref()
        .filter(|status| status.workspace_id == workspace_id)
}

pub fn model_selection_from_selector(
    selection: ModelSelectorSelection,
) -> Option<GatewaySelfImprovementModelSelection> {
    let provider = selection
        .provider
        .map(|provider| provider.trim().to_owned())
        .filter(|provider| !provider.is_empty())?;
    let model = selection
        .model
        .map(|model| model.trim().to_owned())
        .filter(|model| !model.is_empty())?;
    GatewaySelfImprovementModelSelection {
        provider,
        model,
        reasoning_effort: selection.selected_reasoning_effort,
    }
    .normalized()
    .ok()
}

pub fn model_selection_display_label(
    selection: Option<&GatewaySelfImprovementModelSelection>,
    empty_label: impl Into<String>,
) -> String {
    selection
        .map(|selection| format!("{}/{}", selection.provider, selection.model))
        .unwrap_or_else(|| empty_label.into())
}

pub fn enabled_update_plan(
    current: Option<&GatewaySettingsSnapshot>,
    enabled: bool,
) -> Option<GatewaySettingsUpdatePlan> {
    let mut snapshot = current.cloned()?;
    snapshot.self_improvement.enabled = enabled;
    let update_settings = snapshot.self_improvement.clone();
    Some(plan(snapshot, update_settings))
}

pub fn model_update_plan(
    current: Option<&GatewaySettingsSnapshot>,
    setting: SelfImprovementModelSetting,
    selection: Option<GatewaySelfImprovementModelSelection>,
) -> Option<GatewaySettingsUpdatePlan> {
    let mut snapshot = current.cloned()?;
    match setting {
        SelfImprovementModelSetting::Default => {
            if selection.is_some() {
                snapshot.self_improvement.enabled = true;
            }
            snapshot.self_improvement.default_model = selection;
        }
        SelfImprovementModelSetting::Reviewer => {
            snapshot.self_improvement.reviewer_model = selection;
        }
    }
    let update_settings = snapshot.self_improvement.clone();
    Some(plan(snapshot, update_settings))
}

fn plan(
    snapshot: GatewaySettingsSnapshot,
    self_improvement: GatewaySelfImprovementSettings,
) -> GatewaySettingsUpdatePlan {
    GatewaySettingsUpdatePlan {
        snapshot,
        update: GatewaySettingsUpdate {
            general: None,
            memory: None,
            self_improvement: Some(self_improvement),
            thread_episodic: None,
            cli_runtimes: None,
            remote_access: None,
            voice_input: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        GatewayGeneralSettings, GatewayMemorySettings, GatewaySettingsSnapshot,
    };

    #[test]
    fn status_is_read_only_and_never_crosses_workspace_boundaries() {
        let mut current = snapshot();
        current.self_improvement_status = Some(pioneer_protocol::GatewaySelfImprovementStatus {
            workspace_id: "a".into(),
            phase: pioneer_protocol::SelfImprovementPhase::Waiting,
            reason: pioneer_protocol::SelfImprovementStatusReason::NoNewSources,
            observed_at_unix: 10,
            last_run_at_unix: None,
            last_result: None,
            next_scheduled_at_unix: Some(86400),
            next_retry_at_unix: None,
            progress: None,
        });
        assert!(status_for_workspace(Some(&current), Some("a")).is_some());
        assert!(status_for_workspace(Some(&current), Some("b")).is_none());
        assert!(status_for_workspace(Some(&current), None).is_none());
        let update = enabled_update_plan(Some(&current), true).unwrap();
        let serialized = serde_json::to_value(update.update).unwrap();
        assert!(serialized.get("self_improvement_status").is_none());
        let mut old_wire = serde_json::to_value(&current).unwrap();
        old_wire
            .as_object_mut()
            .unwrap()
            .remove("self_improvement_status");
        let old: GatewaySettingsSnapshot = serde_json::from_value(old_wire).unwrap();
        assert!(old.self_improvement_status.is_none());
    }
    fn selection(provider: &str, model: &str) -> GatewaySelfImprovementModelSelection {
        GatewaySelfImprovementModelSelection {
            provider: provider.to_owned(),
            model: model.to_owned(),
            reasoning_effort: None,
        }
    }

    fn snapshot() -> GatewaySettingsSnapshot {
        GatewaySettingsSnapshot {
            self_improvement_status: None,
            general: GatewayGeneralSettings::default(),
            memory: GatewayMemorySettings::default(),
            self_improvement: GatewaySelfImprovementSettings {
                enabled: false,
                default_model: Some(selection("openai", "learner")),
                reviewer_model: Some(selection("anthropic", "reviewer")),
            },
            thread_episodic: Default::default(),
            cli_runtimes: Default::default(),
            remote_access: Default::default(),
            voice_input: Default::default(),
        }
    }

    #[test]
    fn disclosure_toggle_preserves_both_hidden_selections() {
        let current = snapshot();
        let disabled = enabled_update_plan(Some(&current), false).expect("disable plan");
        assert!(!disabled.snapshot.self_improvement.enabled);
        assert_eq!(
            disabled.snapshot.self_improvement.default_model,
            current.self_improvement.default_model
        );
        assert_eq!(
            disabled.snapshot.self_improvement.reviewer_model,
            current.self_improvement.reviewer_model
        );
        assert_eq!(
            disabled
                .update
                .self_improvement
                .expect("section-only update"),
            disabled.snapshot.self_improvement
        );
    }

    #[test]
    fn default_and_reviewer_updates_are_section_scoped() {
        let current = snapshot();
        let updated = model_update_plan(
            Some(&current),
            SelfImprovementModelSetting::Default,
            Some(selection("openrouter", "new-learner")),
        )
        .expect("default update plan");
        assert_eq!(
            updated
                .snapshot
                .self_improvement
                .default_model
                .as_ref()
                .map(|selection| selection.model.as_str()),
            Some("new-learner")
        );
        assert!(
            updated.snapshot.self_improvement.enabled,
            "choosing the required model completes the enable flow"
        );
        assert_eq!(
            updated.snapshot.self_improvement.reviewer_model,
            current.self_improvement.reviewer_model
        );
        assert!(updated.update.general.is_none());
        assert!(updated.update.memory.is_none());

        let inherited = model_update_plan(
            Some(&updated.snapshot),
            SelfImprovementModelSetting::Reviewer,
            None,
        )
        .expect("reviewer inheritance plan");
        assert!(inherited.snapshot.self_improvement.reviewer_model.is_none());
        assert!(inherited.snapshot.self_improvement.default_model.is_some());
    }

    #[test]
    fn incomplete_is_a_presentation_state_not_client_effective_state() {
        let mut settings = GatewaySelfImprovementSettings {
            enabled: true,
            default_model: None,
            reviewer_model: None,
        };
        assert!(configuration_is_incomplete(&settings));
        settings.enabled = false;
        assert!(!configuration_is_incomplete(&settings));
    }

    #[test]
    fn selector_mapping_requires_and_normalizes_both_parts() {
        assert_eq!(
            model_selection_from_selector(ModelSelectorSelection {
                provider: Some(" openai ".to_owned()),
                model: Some(" gpt-5.4 ".to_owned()),
                selected_reasoning_effort: None,
            }),
            Some(selection("openai", "gpt-5.4"))
        );
        assert!(
            model_selection_from_selector(ModelSelectorSelection {
                provider: Some("openai".to_owned()),
                model: Some(" ".to_owned()),
                selected_reasoning_effort: None,
            })
            .is_none()
        );
    }
    #[test]
    fn selector_reasoning_round_trips_and_provider_default_remains_distinct() {
        for effort in [None, Some("high"), Some("none")] {
            let selected = model_selection_from_selector(ModelSelectorSelection {
                provider: Some("openrouter".to_owned()),
                model: Some("model".to_owned()),
                selected_reasoning_effort: effort.map(str::to_owned),
            })
            .unwrap();
            assert_eq!(selected.reasoning_effort.as_deref(), effort);
            let plan = model_update_plan(
                Some(&snapshot()),
                SelfImprovementModelSetting::Default,
                Some(selected.clone()),
            )
            .unwrap();
            assert_eq!(
                plan.snapshot.self_improvement.default_model,
                Some(selected.clone())
            );
            assert_eq!(
                plan.update.self_improvement.unwrap().default_model,
                Some(selected)
            );
        }
    }
}
