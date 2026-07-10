//! UI-neutral provider presentation rows.

use pioneer_protocol::{
    ProviderListModelsParams, ProviderListModelsResponse, ProviderModelInfo, ProviderSummary,
    ReasoningCapabilitySource, reasoning_effort_comparison_key,
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

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReasoningEffortRow {
    pub effort: String,
    pub label: String,
    pub selected: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReasoningEffortRowsRequest {
    pub model: ProviderModelInfo,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_effort: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReasoningEffortRowsResponse {
    pub rows: Vec<ReasoningEffortRow>,
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
            let description_match = model
                .description
                .as_deref()
                .map(|description| description.to_lowercase().contains(query.as_str()))
                .unwrap_or(false);
            name_match || id_match || description_match
        })
        .cloned()
        .collect()
}

pub fn model_selector_model_display_name(model: &ProviderModelInfo) -> String {
    model
        .name
        .as_deref()
        .and_then(non_empty_trimmed)
        .map(str::to_owned)
        .unwrap_or_else(|| model.id.clone())
}

pub fn model_selector_model_secondary_text(model: &ProviderModelInfo) -> Option<String> {
    if let Some(description) = model.description.as_deref().and_then(non_empty_trimmed) {
        return Some(description.to_owned());
    }

    let id = non_empty_trimmed(model.id.as_str())?;
    let display_name = model_selector_model_display_name(model);
    (!id.eq_ignore_ascii_case(display_name.trim())).then(|| id.to_owned())
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

pub fn normalize_reasoning_effort(value: &str) -> Option<String> {
    pioneer_protocol::normalize_metadata_reasoning_effort(value)
}

pub fn reasoning_effort_display_label(value: &str) -> String {
    match normalize_reasoning_effort(value).as_deref() {
        Some("none") => "None".to_owned(),
        Some("minimal") => "Minimal".to_owned(),
        Some("low") => "Low".to_owned(),
        Some("medium") => "Medium".to_owned(),
        Some("high") => "High".to_owned(),
        Some("xhigh") => "Extra High".to_owned(),
        Some("max") => "Max".to_owned(),
        Some(_) => title_case_effort_label(value),
        None => String::new(),
    }
}

fn ordered_known_reasoning_effort_options(options: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for option in options {
        let Some(effort) =
            pioneer_protocol::ReasoningEffort::canonical_value(option.as_str()).map(str::to_owned)
        else {
            continue;
        };
        if !normalized.contains(&effort) {
            normalized.push(effort);
        }
    }

    if known_efforts_are_in_order(normalized.as_slice()) {
        return normalized;
    }

    normalized.sort_by(|lhs, rhs| {
        match (
            reasoning_effort_known_rank(lhs),
            reasoning_effort_known_rank(rhs),
        ) {
            (Some(lhs_rank), Some(rhs_rank)) => lhs_rank.cmp(&rhs_rank),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => lhs.cmp(rhs),
        }
    });
    normalized
}

fn metadata_defined_reasoning_effort_options(options: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    let mut keys = Vec::new();
    for option in options {
        let Some(effort) = normalize_reasoning_effort(option) else {
            continue;
        };
        let Some(key) = reasoning_effort_comparison_key(effort.as_str()) else {
            continue;
        };
        if keys.contains(&key) {
            continue;
        }
        keys.push(key);
        normalized.push(effort);
    }
    normalized
}

pub fn reasoning_effort_rows_for_model(
    model: &ProviderModelInfo,
    selected_effort: Option<&str>,
) -> Vec<ReasoningEffortRow> {
    let Some(reasoning) = model.capabilities.reasoning.as_ref() else {
        return Vec::new();
    };
    if reasoning.supported != Some(true) || reasoning.effort_options.is_empty() {
        return Vec::new();
    }

    let metadata_defined = reasoning.source == Some(ReasoningCapabilitySource::CliMetadata);
    let selected_effort = selected_effort.and_then(reasoning_effort_comparison_key);
    let options = if metadata_defined {
        metadata_defined_reasoning_effort_options(reasoning.effort_options.as_slice())
    } else {
        ordered_known_reasoning_effort_options(reasoning.effort_options.as_slice())
    };
    options
        .into_iter()
        .filter(|effort| {
            reasoning.mandatory != Some(true)
                || reasoning_effort_comparison_key(effort.as_str()).as_deref() != Some("none")
        })
        .map(|effort| {
            let effort_key = reasoning_effort_comparison_key(effort.as_str());
            ReasoningEffortRow {
                label: reasoning_effort_display_label(effort.as_str()),
                selected: selected_effort.as_ref() == effort_key.as_ref(),
                effort,
            }
        })
        .collect()
}

pub fn reasoning_effort_rows_from_request(
    request: ReasoningEffortRowsRequest,
) -> ReasoningEffortRowsResponse {
    ReasoningEffortRowsResponse {
        rows: reasoning_effort_rows_for_model(&request.model, request.selected_effort.as_deref()),
    }
}

fn normalize_selector_query(query: &str) -> String {
    query.trim().to_lowercase()
}

fn non_empty_trimmed(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

fn reasoning_effort_known_rank(value: &str) -> Option<u8> {
    match value {
        "none" => Some(0),
        "minimal" => Some(1),
        "low" => Some(2),
        "medium" => Some(3),
        "high" => Some(4),
        "xhigh" => Some(5),
        "max" => Some(6),
        _ => None,
    }
}

fn known_efforts_are_in_order(options: &[String]) -> bool {
    let mut previous_rank = None;
    for option in options {
        let Some(rank) = reasoning_effort_known_rank(option.as_str()) else {
            continue;
        };
        if previous_rank.is_some_and(|previous| rank < previous) {
            return false;
        }
        previous_rank = Some(rank);
    }
    true
}

fn title_case_effort_label(value: &str) -> String {
    value
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            let Some(first) = chars.next() else {
                return String::new();
            };
            let mut label = first.to_uppercase().collect::<String>();
            label.push_str(chars.as_str().to_ascii_lowercase().as_str());
            label
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        ProviderModelCapabilities, ProviderModelLimits, ProviderModelPricing,
        ProviderModelReasoningCapabilities,
    };

    fn provider(name: &str) -> ProviderSummary {
        ProviderSummary {
            name: name.to_owned(),
            capabilities: Default::default(),
            api_key_configured: true,
            proxy_url: None,
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

    fn reasoning_model(options: Vec<&str>, supported: Option<bool>) -> ProviderModelInfo {
        let mut model = model("gpt-5", Some("GPT 5"));
        model.capabilities.reasoning = Some(ProviderModelReasoningCapabilities {
            supported,
            effort_options: options.into_iter().map(str::to_owned).collect(),
            default_effort: None,
            mandatory: None,
            supports_token_budget: None,
            source: None,
        });
        model
    }

    fn mandatory_reasoning_model(options: Vec<&str>) -> ProviderModelInfo {
        let mut model = reasoning_model(options, Some(true));
        if let Some(reasoning) = model.capabilities.reasoning.as_mut() {
            reasoning.mandatory = Some(true);
        }
        model
    }

    fn cli_reasoning_model(options: Vec<&str>) -> ProviderModelInfo {
        let mut model = reasoning_model(options, Some(true));
        model.provider = "cli_runtime:codex".to_owned();
        if let Some(reasoning) = model.capabilities.reasoning.as_mut() {
            reasoning.source = Some(ReasoningCapabilitySource::CliMetadata);
        }
        model
    }

    #[test]
    fn model_selector_filters_providers_by_case_insensitive_name() {
        let rows =
            filter_model_selector_providers(&[provider("OpenAI"), provider("Anthropic")], " OPEN ");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "OpenAI");
    }

    #[test]
    fn model_selector_filters_models_by_name_id_or_description() {
        let mut described = model("claude-opus", Some("Opus"));
        described.description = Some("Opus 4.8 with 1M context".to_owned());
        let rows = filter_model_selector_models(
            &[
                model("gpt-5.4", Some("GPT 5.4")),
                model("anthropic/claude", Some("Claude Sonnet")),
                described,
            ],
            " CLAUDE ",
        );

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, "anthropic/claude");

        let rows = filter_model_selector_models(&[model("o4-mini", None)], "mini");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "o4-mini");

        let mut described = model("opus", Some("Opus"));
        described.description = Some("Opus 4.8 with 1M context".to_owned());
        let rows = filter_model_selector_models(&[described], "1m context");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "opus");
    }

    #[test]
    fn model_selector_model_text_uses_description_then_distinct_id() {
        let mut named = model("gpt-5.4", Some("GPT 5.4"));
        named.description = Some("Flagship model".to_owned());
        assert_eq!(model_selector_model_display_name(&named), "GPT 5.4");
        assert_eq!(
            model_selector_model_secondary_text(&named),
            Some("Flagship model".to_owned())
        );

        let named_without_description = model("o4-mini", Some("O4 Mini"));
        assert_eq!(
            model_selector_model_secondary_text(&named_without_description),
            Some("o4-mini".to_owned())
        );

        let unnamed = model("o4-mini", None);
        assert_eq!(model_selector_model_display_name(&unnamed), "o4-mini");
        assert_eq!(model_selector_model_secondary_text(&unnamed), None);
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

    #[test]
    fn reasoning_effort_normalization_and_labels_cover_known_aliases() {
        assert_eq!(
            normalize_reasoning_effort(" Extra High "),
            Some("xhigh".to_owned())
        );
        assert_eq!(
            normalize_reasoning_effort("x-high"),
            Some("xhigh".to_owned())
        );
        assert_eq!(
            normalize_reasoning_effort("MAXIMUM"),
            Some("max".to_owned())
        );
        assert_eq!(normalize_reasoning_effort(" "), None);
        assert_eq!(reasoning_effort_display_label("xhigh"), "Extra High");
        assert_eq!(reasoning_effort_display_label("max"), "Max");
        assert_eq!(reasoning_effort_display_label("turbo-high"), "Turbo High");
    }

    #[test]
    fn reasoning_effort_order_preserves_ordered_provider_values() {
        assert_eq!(
            ordered_known_reasoning_effort_options(&[
                "low".to_owned(),
                "medium".to_owned(),
                "high".to_owned()
            ]),
            vec!["low".to_owned(), "medium".to_owned(), "high".to_owned()]
        );
    }

    #[test]
    fn reasoning_effort_order_sorts_unordered_known_values() {
        assert_eq!(
            ordered_known_reasoning_effort_options(&[
                "high".to_owned(),
                "low".to_owned(),
                "x-high".to_owned(),
                "low".to_owned()
            ]),
            vec!["low".to_owned(), "high".to_owned(), "xhigh".to_owned()]
        );
    }

    #[test]
    fn reasoning_effort_order_drops_unknown_provider_values() {
        assert_eq!(
            ordered_known_reasoning_effort_options(&[
                "low".to_owned(),
                "turbo-high".to_owned(),
                "maximum".to_owned(),
            ]),
            vec!["low".to_owned(), "max".to_owned()]
        );
    }

    #[test]
    fn reasoning_effort_rows_require_supported_model_and_options() {
        assert!(reasoning_effort_rows_for_model(&model("gpt-5", None), None).is_empty());
        assert!(
            reasoning_effort_rows_for_model(&reasoning_model(vec!["low"], Some(false)), None)
                .is_empty()
        );
        assert!(
            reasoning_effort_rows_for_model(&reasoning_model(Vec::new(), Some(true)), None)
                .is_empty()
        );
    }

    #[test]
    fn reasoning_effort_rows_mark_selected_effort() {
        let rows = reasoning_effort_rows_for_model(
            &reasoning_model(vec!["low", "high", "x-high"], Some(true)),
            Some("extra high"),
        );

        assert_eq!(
            rows,
            vec![
                ReasoningEffortRow {
                    effort: "low".to_owned(),
                    label: "Low".to_owned(),
                    selected: false,
                },
                ReasoningEffortRow {
                    effort: "high".to_owned(),
                    label: "High".to_owned(),
                    selected: false,
                },
                ReasoningEffortRow {
                    effort: "xhigh".to_owned(),
                    label: "Extra High".to_owned(),
                    selected: true,
                },
            ]
        );
    }

    #[test]
    fn reasoning_effort_rows_preserve_cli_runtime_metadata_values_and_order() {
        let rows = reasoning_effort_rows_for_model(
            &cli_reasoning_model(vec!["low", "high", "xhigh", "max", "ultra"]),
            Some("Ultra"),
        );

        assert_eq!(
            rows,
            vec![
                ReasoningEffortRow {
                    effort: "low".to_owned(),
                    label: "Low".to_owned(),
                    selected: false,
                },
                ReasoningEffortRow {
                    effort: "high".to_owned(),
                    label: "High".to_owned(),
                    selected: false,
                },
                ReasoningEffortRow {
                    effort: "xhigh".to_owned(),
                    label: "Extra High".to_owned(),
                    selected: false,
                },
                ReasoningEffortRow {
                    effort: "max".to_owned(),
                    label: "Max".to_owned(),
                    selected: false,
                },
                ReasoningEffortRow {
                    effort: "ultra".to_owned(),
                    label: "Ultra".to_owned(),
                    selected: true,
                },
            ]
        );
    }

    #[test]
    fn reasoning_effort_rows_hide_none_for_mandatory_reasoning_models() {
        let rows =
            reasoning_effort_rows_for_model(&mandatory_reasoning_model(vec!["none", "low"]), None);

        assert_eq!(
            rows,
            vec![ReasoningEffortRow {
                effort: "low".to_owned(),
                label: "Low".to_owned(),
                selected: false,
            }]
        );
    }
}
