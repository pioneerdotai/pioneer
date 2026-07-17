//! Shared Voice Input settings plans and runtime presentation reduction.

use pioneer_protocol::{
    GatewaySettingsUpdate, GatewayVoiceInputProvider, GatewayVoiceInputRuntimePhase,
    GatewayVoiceInputSettings, GatewayVoiceInputSettingsUpdate,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VoiceInputSettingsAction {
    Enable,
    Disable,
    Select {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
    },
    Retry,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct VoiceInputSettingsPlanRequest {
    pub current: GatewayVoiceInputSettings,
    pub action: VoiceInputSettingsAction,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VoiceInputSettingsPlanRejection {
    InvalidProvider,
    MissingModel,
    RetryUnavailable,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VoiceInputSettingsPlan {
    Update {
        update: GatewaySettingsUpdate,
    },
    NeedsSelection,
    Noop,
    Rejected {
        reason: VoiceInputSettingsPlanRejection,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VoiceInputRuntimePresentation {
    Disabled,
    NeedsSelection,
    Preparing,
    Ready,
    Failed,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct VoiceInputStatusReduction {
    pub phase: GatewayVoiceInputRuntimePhase,
    pub presentation: VoiceInputRuntimePresentation,
    pub desired_enabled: bool,
    pub effective_enabled: bool,
    pub model_selected: bool,
    pub non_terminal: bool,
    pub show_progress: bool,
    pub retry_available: bool,
}

pub fn voice_input_settings_plan(request: VoiceInputSettingsPlanRequest) -> VoiceInputSettingsPlan {
    match request.action {
        VoiceInputSettingsAction::Enable => voice_input_enable_plan(&request.current),
        VoiceInputSettingsAction::Disable => voice_input_disable_plan(&request.current),
        VoiceInputSettingsAction::Select { provider, model } => {
            voice_input_model_selection_plan(&request.current, provider.as_deref(), model)
        }
        VoiceInputSettingsAction::Retry => voice_input_retry_plan(&request.current),
    }
}

pub fn voice_input_enable_plan(current: &GatewayVoiceInputSettings) -> VoiceInputSettingsPlan {
    if current.enabled {
        return VoiceInputSettingsPlan::Noop;
    }
    if !has_valid_local_selection(current) {
        return VoiceInputSettingsPlan::NeedsSelection;
    }

    VoiceInputSettingsPlan::Update {
        update: voice_input_update(GatewayVoiceInputSettingsUpdate {
            enabled: Some(true),
            ..GatewayVoiceInputSettingsUpdate::default()
        }),
    }
}

pub fn voice_input_disable_plan(current: &GatewayVoiceInputSettings) -> VoiceInputSettingsPlan {
    if !current.enabled {
        return VoiceInputSettingsPlan::Noop;
    }

    VoiceInputSettingsPlan::Update {
        update: voice_input_update(GatewayVoiceInputSettingsUpdate {
            enabled: Some(false),
            ..GatewayVoiceInputSettingsUpdate::default()
        }),
    }
}

pub fn voice_input_model_selection_plan(
    current: &GatewayVoiceInputSettings,
    provider: Option<&str>,
    model: Option<String>,
) -> VoiceInputSettingsPlan {
    if provider.is_none_or(|provider| provider.trim() != "local") {
        return VoiceInputSettingsPlan::Rejected {
            reason: VoiceInputSettingsPlanRejection::InvalidProvider,
        };
    }
    let Some(model) = model
        .map(|model| model.trim().to_owned())
        .filter(|model| !model.is_empty())
    else {
        return VoiceInputSettingsPlan::Rejected {
            reason: VoiceInputSettingsPlanRejection::MissingModel,
        };
    };
    if current.enabled
        && current.provider == Some(GatewayVoiceInputProvider::Local)
        && current.model.as_deref() == Some(model.as_str())
    {
        return VoiceInputSettingsPlan::Noop;
    }

    VoiceInputSettingsPlan::Update {
        update: voice_input_update(GatewayVoiceInputSettingsUpdate {
            enabled: Some(true),
            provider: Some(Some(GatewayVoiceInputProvider::Local)),
            model: Some(Some(model)),
            retry_install: false,
        }),
    }
}

pub fn voice_input_retry_plan(current: &GatewayVoiceInputSettings) -> VoiceInputSettingsPlan {
    if !has_valid_local_selection(current)
        || !current.enabled
        || current.runtime.phase != GatewayVoiceInputRuntimePhase::Failed
    {
        return VoiceInputSettingsPlan::Rejected {
            reason: VoiceInputSettingsPlanRejection::RetryUnavailable,
        };
    }

    VoiceInputSettingsPlan::Update {
        update: voice_input_update(GatewayVoiceInputSettingsUpdate {
            retry_install: true,
            ..GatewayVoiceInputSettingsUpdate::default()
        }),
    }
}

pub fn voice_input_status_reduction(
    current: &GatewayVoiceInputSettings,
) -> VoiceInputStatusReduction {
    let phase = current.runtime.phase;
    let presentation = match phase {
        GatewayVoiceInputRuntimePhase::Disabled => VoiceInputRuntimePresentation::Disabled,
        GatewayVoiceInputRuntimePhase::ModelNotSelected => {
            VoiceInputRuntimePresentation::NeedsSelection
        }
        GatewayVoiceInputRuntimePhase::Missing
        | GatewayVoiceInputRuntimePhase::Downloading
        | GatewayVoiceInputRuntimePhase::Installing
        | GatewayVoiceInputRuntimePhase::Loading => VoiceInputRuntimePresentation::Preparing,
        GatewayVoiceInputRuntimePhase::Ready => VoiceInputRuntimePresentation::Ready,
        GatewayVoiceInputRuntimePhase::Failed => VoiceInputRuntimePresentation::Failed,
    };
    let non_terminal = matches!(
        phase,
        GatewayVoiceInputRuntimePhase::Downloading
            | GatewayVoiceInputRuntimePhase::Installing
            | GatewayVoiceInputRuntimePhase::Loading
    );

    VoiceInputStatusReduction {
        phase,
        presentation,
        desired_enabled: current.enabled,
        effective_enabled: current.runtime.effective_enabled,
        model_selected: has_valid_local_selection(current),
        non_terminal,
        show_progress: phase == GatewayVoiceInputRuntimePhase::Downloading,
        retry_available: current.enabled
            && has_valid_local_selection(current)
            && phase == GatewayVoiceInputRuntimePhase::Failed,
    }
}

fn has_valid_local_selection(current: &GatewayVoiceInputSettings) -> bool {
    current.provider == Some(GatewayVoiceInputProvider::Local)
        && current
            .model
            .as_deref()
            .is_some_and(|model| !model.trim().is_empty())
}

fn voice_input_update(update: GatewayVoiceInputSettingsUpdate) -> GatewaySettingsUpdate {
    GatewaySettingsUpdate {
        voice_input: Some(update),
        ..GatewaySettingsUpdate::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(
        enabled: bool,
        selected: bool,
        phase: GatewayVoiceInputRuntimePhase,
    ) -> GatewayVoiceInputSettings {
        GatewayVoiceInputSettings {
            enabled,
            provider: selected.then_some(GatewayVoiceInputProvider::Local),
            model: selected.then(|| "parakeet-tdt-0.6b-v3".to_owned()),
            runtime: pioneer_protocol::GatewayVoiceInputRuntimeSnapshot {
                phase,
                effective_enabled: phase == GatewayVoiceInputRuntimePhase::Ready,
                ..Default::default()
            },
        }
    }

    #[test]
    fn voice_input_settings_enable_disable_and_selection_plans_are_exact() {
        assert!(matches!(
            voice_input_enable_plan(&settings(
                false,
                false,
                GatewayVoiceInputRuntimePhase::Disabled
            )),
            VoiceInputSettingsPlan::NeedsSelection
        ));

        let VoiceInputSettingsPlan::Update { update } = voice_input_enable_plan(&settings(
            false,
            true,
            GatewayVoiceInputRuntimePhase::Disabled,
        )) else {
            panic!("remembered model must enable")
        };
        let update = update.voice_input.expect("Voice Input update");
        assert_eq!(update.enabled, Some(true));
        assert_eq!(update.provider, None);
        assert_eq!(update.model, None);

        let VoiceInputSettingsPlan::Update { update } = voice_input_model_selection_plan(
            &settings(false, false, GatewayVoiceInputRuntimePhase::Disabled),
            Some("local"),
            Some(" moonshine-base ".to_owned()),
        ) else {
            panic!("valid selection must update")
        };
        let update = update.voice_input.expect("Voice Input update");
        assert_eq!(update.enabled, Some(true));
        assert_eq!(
            update.provider,
            Some(Some(GatewayVoiceInputProvider::Local))
        );
        assert_eq!(
            update.model.as_ref().and_then(|model| model.as_deref()),
            Some("moonshine-base")
        );

        let VoiceInputSettingsPlan::Update { update } =
            voice_input_disable_plan(&settings(true, true, GatewayVoiceInputRuntimePhase::Ready))
        else {
            panic!("enabled Voice Input must disable")
        };
        let update = update.voice_input.expect("Voice Input update");
        assert_eq!(update.enabled, Some(false));
        assert_eq!(update.provider, None);
        assert_eq!(update.model, None);
    }

    #[test]
    fn voice_input_settings_invalid_selection_never_enables() {
        let current = settings(false, false, GatewayVoiceInputRuntimePhase::Disabled);
        assert!(matches!(
            voice_input_model_selection_plan(&current, Some("openai"), Some("model".to_owned())),
            VoiceInputSettingsPlan::Rejected {
                reason: VoiceInputSettingsPlanRejection::InvalidProvider
            }
        ));
        assert!(matches!(
            voice_input_model_selection_plan(&current, Some("local"), Some(" ".to_owned())),
            VoiceInputSettingsPlan::Rejected {
                reason: VoiceInputSettingsPlanRejection::MissingModel
            }
        ));
    }

    #[test]
    fn voice_input_settings_retry_is_command_only() {
        let VoiceInputSettingsPlan::Update { update } =
            voice_input_retry_plan(&settings(true, true, GatewayVoiceInputRuntimePhase::Failed))
        else {
            panic!("failed selected model must retry")
        };
        let update = update.voice_input.expect("Voice Input update");
        assert!(update.retry_install);
        assert_eq!(update.enabled, None);
        assert_eq!(update.provider, None);
        assert_eq!(update.model, None);
        assert!(matches!(
            voice_input_retry_plan(&settings(true, true, GatewayVoiceInputRuntimePhase::Ready)),
            VoiceInputSettingsPlan::Rejected {
                reason: VoiceInputSettingsPlanRejection::RetryUnavailable
            }
        ));
    }

    #[test]
    fn voice_input_settings_status_reduction_maps_every_runtime_phase() {
        let cases = [
            (
                GatewayVoiceInputRuntimePhase::Disabled,
                VoiceInputRuntimePresentation::Disabled,
                false,
                false,
            ),
            (
                GatewayVoiceInputRuntimePhase::ModelNotSelected,
                VoiceInputRuntimePresentation::NeedsSelection,
                false,
                false,
            ),
            (
                GatewayVoiceInputRuntimePhase::Missing,
                VoiceInputRuntimePresentation::Preparing,
                false,
                false,
            ),
            (
                GatewayVoiceInputRuntimePhase::Downloading,
                VoiceInputRuntimePresentation::Preparing,
                true,
                true,
            ),
            (
                GatewayVoiceInputRuntimePhase::Installing,
                VoiceInputRuntimePresentation::Preparing,
                true,
                false,
            ),
            (
                GatewayVoiceInputRuntimePhase::Loading,
                VoiceInputRuntimePresentation::Preparing,
                true,
                false,
            ),
            (
                GatewayVoiceInputRuntimePhase::Ready,
                VoiceInputRuntimePresentation::Ready,
                false,
                false,
            ),
            (
                GatewayVoiceInputRuntimePhase::Failed,
                VoiceInputRuntimePresentation::Failed,
                false,
                false,
            ),
        ];
        for (phase, presentation, non_terminal, show_progress) in cases {
            let reduction = voice_input_status_reduction(&settings(true, true, phase));
            assert_eq!(reduction.phase, phase);
            assert_eq!(reduction.presentation, presentation);
            assert_eq!(reduction.non_terminal, non_terminal);
            assert_eq!(reduction.show_progress, show_progress);
            assert_eq!(
                reduction.retry_available,
                phase == GatewayVoiceInputRuntimePhase::Failed
            );
        }
    }

    #[test]
    fn voice_input_settings_request_and_results_are_serializable() {
        let request = VoiceInputSettingsPlanRequest {
            current: settings(false, false, GatewayVoiceInputRuntimePhase::Disabled),
            action: VoiceInputSettingsAction::Select {
                provider: Some("local".to_owned()),
                model: Some("moonshine-base".to_owned()),
            },
        };
        let encoded = serde_json::to_string(&request).expect("request serializes");
        let decoded: VoiceInputSettingsPlanRequest =
            serde_json::from_str(encoded.as_str()).expect("request deserializes");
        let result = voice_input_settings_plan(decoded);
        let encoded = serde_json::to_string(&result).expect("result serializes");
        let _: VoiceInputSettingsPlan =
            serde_json::from_str(encoded.as_str()).expect("result deserializes");
    }
}
