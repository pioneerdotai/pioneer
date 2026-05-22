use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct GatewaySettingsGetParams {}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GatewaySettingsGetResponse {
    pub settings: GatewaySettingsSnapshot,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct GatewaySettingsUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub general: Option<GatewayGeneralSettingsUpdate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<GatewayMemorySettings>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayGeneralSettingsUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keepawake: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preflight_model: Option<GatewayMemoryModelSelection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GatewaySettingsUpdateParams {
    pub update: GatewaySettingsUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GatewaySettingsUpdateResponse {
    pub settings: GatewaySettingsSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewaySettingsSnapshot {
    #[serde(default)]
    pub general: GatewayGeneralSettings,
    pub memory: GatewayMemorySettings,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayGeneralSettings {
    #[serde(default)]
    pub keepawake: bool,
    #[serde(default)]
    pub preflight_model: GatewayMemoryModelSelection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayMemorySettings {
    pub enabled: bool,
    pub deterministic_recall_enabled: bool,
    pub active_recall_enabled: bool,
    pub tools_enabled: bool,
    pub proactive_writes_enabled: bool,
    pub background_extraction_enabled: bool,
    #[serde(default)]
    pub proactive_writes_model: GatewayMemoryModelSelection,
    pub debug_trace_enabled: bool,
    pub strict_diagnostics_enabled: bool,
}

impl Default for GatewayMemorySettings {
    fn default() -> Self {
        Self {
            enabled: true,
            deterministic_recall_enabled: true,
            active_recall_enabled: true,
            tools_enabled: true,
            proactive_writes_enabled: true,
            background_extraction_enabled: true,
            proactive_writes_model: GatewayMemoryModelSelection::thread(),
            debug_trace_enabled: false,
            strict_diagnostics_enabled: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GatewayMemoryModelSelectionSource {
    Thread,
    Custom,
}

impl Default for GatewayMemoryModelSelectionSource {
    fn default() -> Self {
        Self::Thread
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayMemoryModelSelection {
    #[serde(default)]
    pub source: GatewayMemoryModelSelectionSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

impl Default for GatewayMemoryModelSelection {
    fn default() -> Self {
        Self::thread()
    }
}

impl GatewayMemoryModelSelection {
    pub fn thread() -> Self {
        Self {
            source: GatewayMemoryModelSelectionSource::Thread,
            model_provider: None,
            model: None,
        }
    }

    pub fn custom(model_provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            source: GatewayMemoryModelSelectionSource::Custom,
            model_provider: Some(model_provider.into()),
            model: Some(model.into()),
        }
    }

    pub fn is_thread_model(&self) -> bool {
        self.source == GatewayMemoryModelSelectionSource::Thread
    }

    pub fn model_provider_override(&self) -> Option<String> {
        if self.is_thread_model() {
            return None;
        }
        let model_provider =
            normalized_optional_model_selection_text(self.model_provider.as_deref(), 80);
        let model = normalized_optional_model_selection_text(self.model.as_deref(), 160);
        model_provider.filter(|_| model.is_some())
    }

    pub fn model_override(&self) -> Option<String> {
        if self.is_thread_model() {
            return None;
        }
        let model_provider =
            normalized_optional_model_selection_text(self.model_provider.as_deref(), 80);
        let model = normalized_optional_model_selection_text(self.model.as_deref(), 160);
        model.filter(|_| model_provider.is_some())
    }
}

fn normalized_optional_model_selection_text(value: Option<&str>, max_len: usize) -> Option<String> {
    let normalized = value?.trim();
    if normalized.is_empty() {
        return None;
    }

    Some(normalized.chars().take(max_len).collect())
}

#[cfg(test)]
mod tests {
    use super::{
        GatewayGeneralSettings, GatewayGeneralSettingsUpdate, GatewayMemoryModelSelection,
    };

    #[test]
    fn settings_general_defaults_to_thread_preflight_model() {
        let settings = GatewayGeneralSettings::default();

        assert!(!settings.keepawake);
        assert!(settings.preflight_model.is_thread_model());
    }

    #[test]
    fn settings_general_update_roundtrips_preflight_model() {
        let update = GatewayGeneralSettingsUpdate {
            keepawake: Some(true),
            preflight_model: Some(GatewayMemoryModelSelection::custom(
                "planner-provider",
                "planner-model",
            )),
        };

        let serialized = serde_json::to_string(&update).expect("settings update should serialize");
        assert!(serialized.contains("preflight_model"));
        assert!(serialized.contains("planner-provider"));
        assert!(serialized.contains("planner-model"));

        let roundtrip: GatewayGeneralSettingsUpdate =
            serde_json::from_str(serialized.as_str()).expect("settings update should deserialize");
        assert_eq!(roundtrip, update);
    }
}
