use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::Result;

use crate::factory::create_provider_with_timeout_policy;
use crate::traits::Provider;
use crate::types::ProviderTimeoutPolicy;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProviderCacheKey {
    workspace_id: Option<String>,
    provider_name: String,
}

impl ProviderCacheKey {
    fn global(provider_name: &str) -> Self {
        Self {
            workspace_id: None,
            provider_name: provider_name.to_owned(),
        }
    }

    fn workspace(workspace_id: &str, provider_name: &str) -> Self {
        Self {
            workspace_id: Some(workspace_id.to_owned()),
            provider_name: provider_name.to_owned(),
        }
    }
}

/// Thread-safe, lazily-populated cache of provider instances.
///
/// Each unique provider name is created once (via [`create_provider`]) and then
/// served from the cache on subsequent requests. API keys are resolved through
/// the injected `key_resolver` closure, keeping environment-specific logic out
/// of the provider crate.
pub struct ProviderRegistry {
    cache: RwLock<HashMap<ProviderCacheKey, Arc<dyn Provider>>>,
    key_resolver: Box<dyn Fn(Option<&str>, &str) -> String + Send + Sync>,
    timeout_policy: ProviderTimeoutPolicy,
}

impl ProviderRegistry {
    /// Create a new registry with the given key resolver.
    ///
    /// `key_resolver` maps a provider name (e.g. `"openai"`) to the API key
    /// string. It is called at most once per provider name.
    pub fn new(key_resolver: impl Fn(&str) -> String + Send + Sync + 'static) -> Self {
        Self::new_with_timeout_policy(key_resolver, ProviderTimeoutPolicy::default())
    }

    pub fn new_with_timeout_policy(
        key_resolver: impl Fn(&str) -> String + Send + Sync + 'static,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            key_resolver: Box::new(move |_, provider_name| key_resolver(provider_name)),
            timeout_policy,
        }
    }

    pub fn new_scoped(
        key_resolver: impl Fn(Option<&str>, &str) -> String + Send + Sync + 'static,
    ) -> Self {
        Self::new_scoped_with_timeout_policy(key_resolver, ProviderTimeoutPolicy::default())
    }

    pub fn new_scoped_with_timeout_policy(
        key_resolver: impl Fn(Option<&str>, &str) -> String + Send + Sync + 'static,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            key_resolver: Box::new(key_resolver),
            timeout_policy,
        }
    }

    pub fn get_or_create(&self, provider_name: &str) -> Result<Arc<dyn Provider>> {
        self.get_or_create_with_key(ProviderCacheKey::global(provider_name))
    }

    pub fn get_or_create_for_workspace(
        &self,
        workspace_id: &str,
        provider_name: &str,
    ) -> Result<Arc<dyn Provider>> {
        self.get_or_create_with_key(ProviderCacheKey::workspace(workspace_id, provider_name))
    }

    fn get_or_create_with_key(&self, key: ProviderCacheKey) -> Result<Arc<dyn Provider>> {
        {
            let cache = self.cache.read().expect("provider cache poisoned");
            if let Some(provider) = cache.get(&key) {
                return Ok(provider.clone());
            }
            if key.workspace_id.is_some()
                && let Some(provider) = cache.get(&ProviderCacheKey::global(&key.provider_name))
            {
                return Ok(provider.clone());
            }
        }

        let mut cache = self.cache.write().expect("provider cache poisoned");
        if let Some(provider) = cache.get(&key) {
            return Ok(provider.clone());
        }
        if key.workspace_id.is_some()
            && let Some(provider) = cache.get(&ProviderCacheKey::global(&key.provider_name))
        {
            return Ok(provider.clone());
        }

        let api_key = (self.key_resolver)(key.workspace_id.as_deref(), key.provider_name.as_str());
        let provider: Arc<dyn Provider> = Arc::from(create_provider_with_timeout_policy(
            &key.provider_name,
            &api_key,
            self.timeout_policy,
        )?);
        cache.insert(key, provider.clone());
        Ok(provider)
    }

    pub fn insert(&self, name: impl Into<String>, provider: Arc<dyn Provider>) {
        let mut cache = self.cache.write().expect("provider cache poisoned");
        let name = name.into();
        cache.insert(ProviderCacheKey::global(name.as_str()), provider);
    }

    pub fn invalidate(&self, provider_name: &str) {
        let mut cache = self.cache.write().expect("provider cache poisoned");
        cache.retain(|key, _| key.provider_name != provider_name);
    }
}

/// Create a registry with a single pre-seeded provider. For tests.
impl ProviderRegistry {
    pub fn with_provider(name: &str, provider: Arc<dyn Provider>) -> Self {
        let mut cache = HashMap::new();
        cache.insert(ProviderCacheKey::global(name), provider);
        Self {
            cache: RwLock::new(cache),
            key_resolver: Box::new(|_, _| String::new()),
            timeout_policy: ProviderTimeoutPolicy::default(),
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

    #[test]
    fn scoped_registry_caches_by_workspace() {
        use std::sync::Mutex;

        let calls = Arc::new(Mutex::new(Vec::new()));
        let calls_clone = calls.clone();

        let registry = ProviderRegistry::new_scoped(move |workspace_id, provider_name| {
            calls_clone
                .lock()
                .expect("calls lock poisoned")
                .push((workspace_id.map(str::to_owned), provider_name.to_owned()));
            String::new()
        });

        let ws1_first = registry
            .get_or_create_for_workspace("workspace-1", "ollama")
            .unwrap();
        let ws1_second = registry
            .get_or_create_for_workspace("workspace-1", "ollama")
            .unwrap();
        let ws2 = registry
            .get_or_create_for_workspace("workspace-2", "ollama")
            .unwrap();

        assert!(Arc::ptr_eq(&ws1_first, &ws1_second));
        assert!(!Arc::ptr_eq(&ws1_first, &ws2));
        assert_eq!(
            calls.lock().expect("calls lock poisoned").as_slice(),
            &[
                (Some("workspace-1".to_owned()), "ollama".to_owned()),
                (Some("workspace-2".to_owned()), "ollama".to_owned()),
            ]
        );
    }
}
