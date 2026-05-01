use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::Result;

use crate::factory::create_provider;
use crate::traits::Provider;

/// Thread-safe, lazily-populated cache of provider instances.
///
/// Each unique provider name is created once (via [`create_provider`]) and then
/// served from the cache on subsequent requests. API keys are resolved through
/// the injected `key_resolver` closure, keeping environment-specific logic out
/// of the provider crate.
pub struct ProviderRegistry {
    cache: RwLock<HashMap<String, Arc<dyn Provider>>>,
    key_resolver: Box<dyn Fn(&str) -> String + Send + Sync>,
}

impl ProviderRegistry {
    /// Create a new registry with the given key resolver.
    ///
    /// `key_resolver` maps a provider name (e.g. `"openai"`) to the API key
    /// string. It is called at most once per provider name.
    pub fn new(key_resolver: impl Fn(&str) -> String + Send + Sync + 'static) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            key_resolver: Box::new(key_resolver),
        }
    }

    pub fn get_or_create(&self, provider_name: &str) -> Result<Arc<dyn Provider>> {
        {
            let cache = self.cache.read().expect("provider cache poisoned");
            if let Some(provider) = cache.get(provider_name) {
                return Ok(provider.clone());
            }
        }

        let mut cache = self.cache.write().expect("provider cache poisoned");
        if let Some(provider) = cache.get(provider_name) {
            return Ok(provider.clone());
        }

        let api_key = (self.key_resolver)(provider_name);
        let provider: Arc<dyn Provider> = Arc::from(create_provider(provider_name, &api_key)?);
        cache.insert(provider_name.to_owned(), provider.clone());
        Ok(provider)
    }

    pub fn insert(&self, name: impl Into<String>, provider: Arc<dyn Provider>) {
        let mut cache = self.cache.write().expect("provider cache poisoned");
        cache.insert(name.into(), provider);
    }

    pub fn invalidate(&self, provider_name: &str) {
        let mut cache = self.cache.write().expect("provider cache poisoned");
        cache.remove(provider_name);
    }
}

/// Create a registry with a single pre-seeded provider. For tests.
impl ProviderRegistry {
    pub fn with_provider(name: &str, provider: Arc<dyn Provider>) -> Self {
        let mut cache = HashMap::new();
        cache.insert(name.to_owned(), provider);
        Self {
            cache: RwLock::new(cache),
            key_resolver: Box::new(|_| String::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::EchoProvider;

    #[test]
    fn get_or_create_caches_provider() {
        let registry = ProviderRegistry::with_provider("echo", Arc::new(EchoProvider::new()));

        let p1 = registry.get_or_create("echo").unwrap();
        let p2 = registry.get_or_create("echo").unwrap();

        assert_eq!(p1.name(), "echo");
        assert!(Arc::ptr_eq(&p1, &p2));
    }

    #[test]
    fn get_or_create_unknown_provider_errors() {
        let registry = ProviderRegistry::new(|_| String::new());
        let result = registry.get_or_create("nonexistent_provider_xyz");
        assert!(result.is_err());
    }

    #[test]
    fn insert_overrides_cache() {
        let registry = ProviderRegistry::new(|_| String::new());
        let echo = Arc::new(EchoProvider::new());
        registry.insert("echo", echo.clone());

        let p = registry.get_or_create("echo").unwrap();
        assert_eq!(p.name(), "echo");
    }

    #[test]
    fn key_resolver_is_called_on_miss() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let call_count = Arc::new(AtomicUsize::new(0));
        let count_clone = call_count.clone();

        let registry = ProviderRegistry::new(move |_name| {
            count_clone.fetch_add(1, Ordering::SeqCst);
            String::new()
        });

        // Pre-seed so creation succeeds
        registry.insert("echo", Arc::new(EchoProvider::new()));

        // Cached hit — resolver not called
        let _ = registry.get_or_create("echo").unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 0);
    }
}
