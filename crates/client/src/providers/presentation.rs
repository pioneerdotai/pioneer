//! UI-neutral provider presentation rows.

use pioneer_protocol::{ProviderModelInfo, ProviderSummary};

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
}
