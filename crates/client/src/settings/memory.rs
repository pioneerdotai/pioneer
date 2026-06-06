//! Memory settings state.

use pioneer_protocol::{
    GatewayGeneralSettings, GatewayMemoryModelSelection, GatewayMemorySettings,
    GatewaySettingsSnapshot, GatewaySettingsUpdate,
};

use crate::composer::model_selection::ModelSelectorSelection;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum MemorySettingToggle {
    Enabled,
    ActiveRecall,
    ProactiveWrites,
    BackgroundExtraction,
    DebugTrace,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum MemoryModelSetting {
    PostTurnExtractor,
}

pub fn current_gateway_memory_settings(
    settings: Option<&GatewaySettingsSnapshot>,
) -> Option<GatewayMemorySettings> {
    settings.map(|settings| settings.memory.clone())
}

pub fn apply_memory_setting(
    memory: &mut GatewayMemorySettings,
    toggle: MemorySettingToggle,
    enabled: bool,
) {
    match toggle {
        MemorySettingToggle::Enabled => memory.enabled = enabled,
        MemorySettingToggle::ActiveRecall => memory.active_recall_enabled = enabled,
        MemorySettingToggle::ProactiveWrites => memory.proactive_writes_enabled = enabled,
        MemorySettingToggle::BackgroundExtraction => memory.background_extraction_enabled = enabled,
        MemorySettingToggle::DebugTrace => memory.debug_trace_enabled = enabled,
    }
}

pub fn memory_settings_with_toggle(
    mut memory: GatewayMemorySettings,
    toggle: MemorySettingToggle,
    enabled: bool,
) -> GatewayMemorySettings {
    apply_memory_setting(&mut memory, toggle, enabled);
    memory
}

pub fn apply_memory_model_setting(
    memory: &mut GatewayMemorySettings,
    setting: MemoryModelSetting,
    model_selection: GatewayMemoryModelSelection,
) {
    match setting {
        MemoryModelSetting::PostTurnExtractor => {
            memory.proactive_writes_model = model_selection;
        }
    }
}

pub fn memory_settings_with_model_selection(
    mut memory: GatewayMemorySettings,
    setting: MemoryModelSetting,
    model_selection: GatewayMemoryModelSelection,
) -> GatewayMemorySettings {
    apply_memory_model_setting(&mut memory, setting, model_selection);
    memory
}

pub fn gateway_memory_model_selection_from_model_selector(
    selection: ModelSelectorSelection,
) -> GatewayMemoryModelSelection {
    match (selection.provider, selection.model) {
        (Some(provider), Some(model))
            if !provider.trim().is_empty() && !model.trim().is_empty() =>
        {
            GatewayMemoryModelSelection::custom(provider, model)
        }
        _ => GatewayMemoryModelSelection::thread(),
    }
}

pub fn gateway_settings_snapshot_with_memory(
    current: Option<&GatewaySettingsSnapshot>,
    memory: GatewayMemorySettings,
) -> GatewaySettingsSnapshot {
    let mut snapshot = current.cloned().unwrap_or_else(|| GatewaySettingsSnapshot {
        general: GatewayGeneralSettings::default(),
        memory: GatewayMemorySettings::default(),
    });
    snapshot.memory = memory;
    snapshot
}

pub fn gateway_settings_update_for_memory(memory: GatewayMemorySettings) -> GatewaySettingsUpdate {
    GatewaySettingsUpdate {
        general: None,
        memory: Some(memory),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> GatewaySettingsSnapshot {
        GatewaySettingsSnapshot {
            general: GatewayGeneralSettings {
                keepawake: true,
                ..Default::default()
            },
            memory: GatewayMemorySettings::default(),
        }
    }

    #[test]
    fn memory_toggles_update_only_the_requested_field() {
        let base = GatewayMemorySettings::default();
        let updated =
            memory_settings_with_toggle(base.clone(), MemorySettingToggle::ActiveRecall, false);
        assert!(!updated.active_recall_enabled);
        assert_eq!(updated.enabled, base.enabled);
        assert_eq!(
            updated.proactive_writes_enabled,
            base.proactive_writes_enabled
        );
        assert_eq!(
            updated.background_extraction_enabled,
            base.background_extraction_enabled
        );

        let updated =
            memory_settings_with_toggle(base.clone(), MemorySettingToggle::DebugTrace, true);
        assert!(updated.debug_trace_enabled);
        assert_eq!(updated.enabled, base.enabled);
    }

    #[test]
    fn memory_model_setting_updates_proactive_writes_model_only() {
        let base = GatewayMemorySettings::default();
        let custom = GatewayMemoryModelSelection::custom("openai", "gpt-5.4");
        let updated = memory_settings_with_model_selection(
            base.clone(),
            MemoryModelSetting::PostTurnExtractor,
            custom.clone(),
        );

        assert_eq!(updated.proactive_writes_model, custom);
        assert_eq!(updated.enabled, base.enabled);
        assert_eq!(updated.active_recall_enabled, base.active_recall_enabled);
    }

    #[test]
    fn model_selector_selection_maps_to_gateway_memory_model_selection() {
        assert_eq!(
            gateway_memory_model_selection_from_model_selector(ModelSelectorSelection {
                provider: Some("openai".to_owned()),
                model: Some("gpt-5.4".to_owned()),
            }),
            GatewayMemoryModelSelection::custom("openai", "gpt-5.4")
        );

        assert_eq!(
            gateway_memory_model_selection_from_model_selector(ModelSelectorSelection {
                provider: Some("openai".to_owned()),
                model: Some(" ".to_owned()),
            }),
            GatewayMemoryModelSelection::thread()
        );
    }

    #[test]
    fn memory_update_preserves_general_settings_and_builds_memory_only_update() {
        let current = snapshot();
        let mut memory = current.memory.clone();
        memory.enabled = false;

        let next = gateway_settings_snapshot_with_memory(Some(&current), memory.clone());
        assert_eq!(next.general.keepawake, current.general.keepawake);
        assert_eq!(next.memory, memory);

        let update = gateway_settings_update_for_memory(memory.clone());
        assert!(update.general.is_none());
        assert_eq!(update.memory, Some(memory));
    }

    #[test]
    fn current_memory_settings_none_when_snapshot_missing() {
        assert!(current_gateway_memory_settings(None).is_none());
        assert!(current_gateway_memory_settings(Some(&snapshot())).is_some());
    }
}
