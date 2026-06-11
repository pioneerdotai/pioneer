//! UI-neutral provider presentation rows.

use pioneer_protocol::{
    ProviderListModelsParams, ProviderListModelsResponse, ProviderModelInfo, ProviderSummary,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct ProviderModelDisplayKey {
    pub workspace_id: String,
    pub provider: String,
    pub model: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProviderModelDisplayResolution {
    pub label: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderModelDisplayState {
    Loading,
    Label(String),
    Missing,
}

pub fn provider_model_display_key(
    workspace_id: Option<&str>,
    provider: Option<&str>,
    model: Option<&str>,
) -> Option<ProviderModelDisplayKey> {
    let workspace_id = workspace_id?.trim();
    let provider = provider?.trim();
    let model = model?.trim();

    if workspace_id.is_empty() || provider.is_empty() || model.is_empty() {
        return None;
    }

    Some(ProviderModelDisplayKey {
        workspace_id: workspace_id.to_owned(),
        provider: provider.to_owned(),
        model: model.to_owned(),
    })
}

pub fn provider_model_display_models_params(
    key: &ProviderModelDisplayKey,
) -> ProviderListModelsParams {
    ProviderListModelsParams {
        workspace_id: key.workspace_id.clone(),
        provider: key.provider.clone(),
    }
}

pub fn filter_model_selector_providers(
    providers: &[ProviderSummary],
    query: &str,
) -> Vec<ProviderSummary> {
    let query = normalize_selector_query(query);
    providers
        .iter()
        .filter(|provider| {
            query.is_empty() || provider.name.to_lowercase().contains(query.as_str())
        })
        .cloned()
        .collect()
}

pub fn filter_model_selector_models(
    models: &[ProviderModelInfo],
    query: &str,
) -> Vec<ProviderModelInfo> {
    let query = normalize_selector_query(query);
    models
        .iter()
        .filter(|model| {
            if query.is_empty() {
                return true;
            }

            let name_match = model
                .name
                .as_deref()
                .map(|name| name.to_lowercase().contains(query.as_str()))
                .unwrap_or(false);
            let id_match = model.id.to_lowercase().contains(query.as_str());
            name_match || id_match
        })
        .cloned()
        .collect()
}

pub fn model_selector_model_display_name(model: &ProviderModelInfo) -> String {
    model.name.clone().unwrap_or_else(|| model.id.clone())
}

pub fn model_selector_model_has_name(model: &ProviderModelInfo) -> bool {
    model.name.is_some()
}

pub fn resolve_provider_model_display_name(
    models: &[ProviderModelInfo],
    selected_model: &str,
) -> Option<String> {
    models
        .iter()
        .find(|model| model.id.as_str() == selected_model)
        .map(model_selector_model_display_name)
}

pub fn resolve_provider_model_display_from_response(
    key: &ProviderModelDisplayKey,
    response: &ProviderListModelsResponse,
) -> ProviderModelDisplayResolution {
    let label = (response.provider == key.provider)
        .then(|| {
            resolve_provider_model_display_name(response.models.as_slice(), key.model.as_str())
        })
        .flatten();

    ProviderModelDisplayResolution { label }
}

pub fn model_selector_selected_model_display_state(
    selected_model: Option<&str>,
    models: &[ProviderModelInfo],
    loading_models: bool,
) -> ProviderModelDisplayState {
    let Some(selected_model) = selected_model else {
        return ProviderModelDisplayState::Missing;
    };

    if loading_models {
        return ProviderModelDisplayState::Loading;
    }

    resolve_provider_model_display_name(models, selected_model)
        .map(ProviderModelDisplayState::Label)
        .unwrap_or(ProviderModelDisplayState::Missing)
}

fn normalize_selector_query(query: &str) -> String {
    query.trim().to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{ProviderModelCapabilities, ProviderModelLimits, ProviderModelPricing};

    fn provider(name: &str) -> ProviderSummary {
        ProviderSummary {
            name: name.to_owned(),
        }
    }

    fn model(id: &str, name: Option<&str>) -> ProviderModelInfo {
        ProviderModelInfo {
            id: id.to_owned(),
            name: name.map(str::to_owned),
            description: None,
            created: None,
            provider: "openai".to_owned(),
            owned_by: None,
            limits: ProviderModelLimits::default(),
            capabilities: ProviderModelCapabilities::default(),
            pricing: Some(ProviderModelPricing::default()),
            active: Some(true),
            family: None,
            lifecycle_status: None,
        }
    }

    #[test]
    fn model_selector_filters_providers_by_case_insensitive_name() {
        let rows =
            filter_model_selector_providers(&[provider("OpenAI"), provider("Anthropic")], " OPEN ");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "OpenAI");
    }

    #[test]
    fn model_selector_filters_models_by_name_or_id() {
        let rows = filter_model_selector_models(
            &[
                model("gpt-5.4", Some("GPT 5.4")),
                model("anthropic/claude", Some("Claude Sonnet")),
            ],
            " CLAUDE ",
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "anthropic/claude");

        let rows = filter_model_selector_models(&[model("o4-mini", None)], "mini");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "o4-mini");
    }

    #[test]
    fn model_selector_model_display_name_uses_name_then_id() {
        let named = model("gpt-5.4", Some("GPT 5.4"));
        assert_eq!(model_selector_model_display_name(&named), "GPT 5.4");
        assert!(model_selector_model_has_name(&named));

        let unnamed = model("o4-mini", None);
        assert_eq!(model_selector_model_display_name(&unnamed), "o4-mini");
        assert!(!model_selector_model_has_name(&unnamed));
    }

    #[test]
    fn provider_model_display_key_trims_and_rejects_incomplete_selection() {
        let key = provider_model_display_key(Some(" ws "), Some(" openai "), Some(" gpt-5 "))
            .expect("valid key");

        assert_eq!(
            key,
            ProviderModelDisplayKey {
                workspace_id: "ws".to_owned(),
                provider: "openai".to_owned(),
                model: "gpt-5".to_owned(),
            }
        );
        assert!(provider_model_display_key(Some("ws"), Some("openai"), Some("")).is_none());
        assert!(provider_model_display_key(Some("ws"), None, Some("gpt-5")).is_none());
    }

    #[test]
    fn provider_model_display_resolution_matches_provider_and_model() {
        let key = ProviderModelDisplayKey {
            workspace_id: "ws".to_owned(),
            provider: "openai".to_owned(),
            model: "gpt-5".to_owned(),
        };
        let response = pioneer_protocol::ProviderListModelsResponse {
            provider: "openai".to_owned(),
            models: vec![model("gpt-5", Some("GPT 5"))],
        };

        assert_eq!(
            resolve_provider_model_display_from_response(&key, &response),
            ProviderModelDisplayResolution {
                label: Some("GPT 5".to_owned()),
            }
        );

        let response = pioneer_protocol::ProviderListModelsResponse {
            provider: "anthropic".to_owned(),
            models: vec![model("gpt-5", Some("GPT 5"))],
        };
        assert_eq!(
            resolve_provider_model_display_from_response(&key, &response),
            ProviderModelDisplayResolution { label: None }
        );
    }

    #[test]
    fn model_selector_selected_model_display_state_tracks_loading_label_and_missing() {
        assert_eq!(
            model_selector_selected_model_display_state(Some("gpt-5"), &[], true),
            ProviderModelDisplayState::Loading
        );
        assert_eq!(
            model_selector_selected_model_display_state(
                Some("gpt-5"),
                &[model("gpt-5", Some("GPT 5"))],
                false,
            ),
            ProviderModelDisplayState::Label("GPT 5".to_owned())
        );
        assert_eq!(
            model_selector_selected_model_display_state(
                Some("missing"),
                &[model("gpt-5", Some("GPT 5"))],
                false,
            ),
            ProviderModelDisplayState::Missing
        );
        assert_eq!(
            model_selector_selected_model_display_state(None, &[model("gpt-5", None)], false),
            ProviderModelDisplayState::Missing
        );
    }
}
