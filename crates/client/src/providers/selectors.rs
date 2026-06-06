//! Provider selectors.

use std::collections::HashSet;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ProviderFilter {
    All,
    Connected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderAliasEntry<'a> {
    pub id: &'a str,
    pub aliases: &'a [&'a str],
}

pub fn provider_filter_tree_index(filter: ProviderFilter) -> usize {
    match filter {
        ProviderFilter::All => 0,
        ProviderFilter::Connected => 1,
    }
}

pub fn provider_filter_from_node_id(
    value: &str,
    all_node_id: &str,
    connected_node_id: &str,
) -> Option<ProviderFilter> {
    if value == all_node_id {
        return Some(ProviderFilter::All);
    }

    if value == connected_node_id {
        return Some(ProviderFilter::Connected);
    }

    None
}

pub fn provider_filter_includes_provider(
    filter: ProviderFilter,
    provider_id: &str,
    configured_provider_names: &HashSet<String>,
) -> bool {
    match filter {
        ProviderFilter::All => true,
        ProviderFilter::Connected => configured_provider_names.contains(provider_id),
    }
}

pub fn provider_filter_empty_connected_state(
    filter: ProviderFilter,
    visible_provider_count: usize,
) -> bool {
    filter == ProviderFilter::Connected && visible_provider_count == 0
}

pub fn normalize_provider_name(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace('_', "-")
        .replace(' ', "")
}

pub fn canonical_provider_id<'a>(
    raw: &str,
    providers: impl IntoIterator<Item = ProviderAliasEntry<'a>>,
) -> String {
    let normalized = normalize_provider_name(raw);
    let normalized_match_key = provider_name_match_key(raw);

    for provider in providers {
        if provider_name_match_key(provider.id) == normalized_match_key {
            return provider.id.to_owned();
        }

        if provider
            .aliases
            .iter()
            .any(|alias| provider_name_match_key(alias) == normalized_match_key)
        {
            return provider.id.to_owned();
        }
    }

    normalized
}

fn provider_name_match_key(value: &str) -> String {
    normalize_provider_name(value).replace('-', "")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries<'a>() -> Vec<ProviderAliasEntry<'a>> {
        vec![
            ProviderAliasEntry {
                id: "openai",
                aliases: &[],
            },
            ProviderAliasEntry {
                id: "bedrock",
                aliases: &["aws-bedrock"],
            },
            ProviderAliasEntry {
                id: "lmstudio",
                aliases: &["lm-studio"],
            },
        ]
    }

    #[test]
    fn provider_filter_maps_to_tree_index_and_node_id() {
        assert_eq!(provider_filter_tree_index(ProviderFilter::All), 0);
        assert_eq!(provider_filter_tree_index(ProviderFilter::Connected), 1);
        assert_eq!(
            provider_filter_from_node_id("providers:all", "providers:all", "providers:connected"),
            Some(ProviderFilter::All)
        );
        assert_eq!(
            provider_filter_from_node_id(
                "providers:connected",
                "providers:all",
                "providers:connected"
            ),
            Some(ProviderFilter::Connected)
        );
        assert_eq!(
            provider_filter_from_node_id(
                "providers:unknown",
                "providers:all",
                "providers:connected"
            ),
            None
        );
    }

    #[test]
    fn provider_filter_connected_requires_configured_provider() {
        let configured = HashSet::from(["openai".to_owned()]);

        assert!(provider_filter_includes_provider(
            ProviderFilter::All,
            "anthropic",
            &configured
        ));
        assert!(provider_filter_includes_provider(
            ProviderFilter::Connected,
            "openai",
            &configured
        ));
        assert!(!provider_filter_includes_provider(
            ProviderFilter::Connected,
            "anthropic",
            &configured
        ));
        assert!(provider_filter_empty_connected_state(
            ProviderFilter::Connected,
            0
        ));
    }

    #[test]
    fn canonical_provider_id_normalizes_aliases() {
        assert_eq!(normalize_provider_name(" AWS_Bedrock "), "aws-bedrock");
        assert_eq!(canonical_provider_id("aws bedrock", entries()), "bedrock");
        assert_eq!(canonical_provider_id("lm_studio", entries()), "lmstudio");
        assert_eq!(
            canonical_provider_id("Custom Provider", entries()),
            "customprovider"
        );
        assert_eq!(
            canonical_provider_id("Custom-Provider", entries()),
            "custom-provider"
        );
    }
}
