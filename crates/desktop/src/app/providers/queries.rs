use super::catalog::{PROVIDER_CATALOG, ProviderCatalogEntry};
use crate::app::root::PioneerDesktop;

impl PioneerDesktop {
    pub(super) fn canonical_provider_id(raw: &str) -> String {
        let normalized = Self::normalize_provider_name(raw);

        for provider in PROVIDER_CATALOG {
            if Self::normalize_provider_name(provider.id) == normalized {
                return provider.id.to_owned();
            }

            if provider
                .aliases
                .iter()
                .any(|alias| Self::normalize_provider_name(alias) == normalized)
            {
                return provider.id.to_owned();
            }
        }

        normalized
    }

    pub(super) fn provider_catalog_entry(
        provider_id: &str,
    ) -> Option<&'static ProviderCatalogEntry> {
        let canonical = Self::canonical_provider_id(provider_id);
        PROVIDER_CATALOG
            .iter()
            .find(|provider| provider.id == canonical)
    }

    fn normalize_provider_name(value: &str) -> String {
        value
            .trim()
            .to_ascii_lowercase()
            .replace('_', "-")
            .replace(' ', "")
    }
}
