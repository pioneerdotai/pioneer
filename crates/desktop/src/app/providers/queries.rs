use super::catalog::{ProviderCatalogEntry, provider_catalog_entries};
use crate::app::root::PioneerDesktop;
use pioneer_client::providers::catalog as client_provider_catalog;

impl PioneerDesktop {
    pub(super) fn canonical_provider_id(raw: &str) -> String {
        client_provider_catalog::canonical_provider_id(raw)
    }

    pub(super) fn provider_catalog_entry(provider_id: &str) -> Option<ProviderCatalogEntry> {
        let canonical = Self::canonical_provider_id(provider_id);
        provider_catalog_entries().find(|provider| provider.id == canonical)
    }
}
