use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use sha2::{Digest, Sha256};

use crate::factory::create_provider_with_timeout_policy_and_proxy_and_authority;
use crate::traits::Provider;
use crate::types::{
    ChatRequest, ChatResponse, EmbeddingRequest, EmbeddingResponse, ProviderCapabilities,
    ProviderFailureClassification, ProviderTimeoutPolicy, StreamChunk,
};
use pioneer_protocol::ProviderModelInfo;

const PROVIDER_AUTHORITY_FINGERPRINT_VERSION: &str = "pioneer-provider-authority-v1";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProviderAuthorityFingerprint(String);

impl ProviderAuthorityFingerprint {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProviderCacheKey {
    workspace_id: Option<String>,
    provider_name: String,
    authority_fingerprint: ProviderAuthorityFingerprint,
}

impl ProviderCacheKey {
    fn new(
        workspace_id: Option<&str>,
        provider_name: &str,
        authority_fingerprint: ProviderAuthorityFingerprint,
    ) -> Self {
        Self {
            workspace_id: workspace_id.map(str::to_owned),
            provider_name: provider_name.to_owned(),
            authority_fingerprint,
        }
    }
}

struct AuthorityBoundProvider {
    inner: Arc<dyn Provider>,
    authority_fingerprint: ProviderAuthorityFingerprint,
}

#[async_trait]
impl Provider for AuthorityBoundProvider {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn authority_fingerprint(&self) -> Option<&str> {
        Some(self.authority_fingerprint.as_str())
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.inner.capabilities()
    }

    fn classify_failure(&self, error: &anyhow::Error) -> Option<ProviderFailureClassification> {
        self.inner.classify_failure(error)
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        crate::attachments::runtime::with_async_authority_scope(
            self.authority_fingerprint.as_str().to_owned(),
            self.inner.chat(request),
        )
        .await
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        crate::attachments::runtime::with_async_authority_scope(
            self.authority_fingerprint.as_str().to_owned(),
            self.inner.stream_chat(request),
        )
        .await
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        self.inner.list_models().await
    }

    async fn list_embedding_models(&self) -> Result<Vec<ProviderModelInfo>> {
        self.inner.list_embedding_models().await
    }

    async fn list_transcription_models(&self) -> Result<Vec<ProviderModelInfo>> {
        self.inner.list_transcription_models().await
    }

    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse> {
        self.inner.embed(request).await
    }

    async fn warmup(&self) -> Result<()> {
        self.inner.warmup().await
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
    /// Explicitly injected providers are a test/integration seam. Unlike the
    /// production factory, an injected provider name is intentionally valid
    /// in every workspace; each lookup still receives a scope-specific
    /// authority wrapper and cache key.
    injected: RwLock<HashMap<String, Arc<dyn Provider>>>,
    key_resolver: Box<dyn Fn(Option<&str>, &str) -> String + Send + Sync>,
    proxy_resolver: Box<dyn Fn(Option<&str>, &str) -> Option<String> + Send + Sync>,
    timeout_policy: ProviderTimeoutPolicy,
}

impl ProviderRegistry {
    /// Create a new registry with the given key resolver.
    ///
    /// `key_resolver` maps a provider name (e.g. `"openai"`) to the API key
    /// string. It is resolved before every lookup so credential rotation
    /// changes the authority fingerprint and cannot reuse a stale instance.
    pub fn new(key_resolver: impl Fn(&str) -> String + Send + Sync + 'static) -> Self {
        Self::new_with_timeout_policy(key_resolver, ProviderTimeoutPolicy::default())
    }

    pub fn new_with_timeout_policy(
        key_resolver: impl Fn(&str) -> String + Send + Sync + 'static,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            injected: RwLock::new(HashMap::new()),
            key_resolver: Box::new(move |_, provider_name| key_resolver(provider_name)),
            proxy_resolver: Box::new(|_, _| None),
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
        Self::new_scoped_with_timeout_policy_and_proxy(key_resolver, |_, _| None, timeout_policy)
    }

    pub fn new_scoped_with_timeout_policy_and_proxy(
        key_resolver: impl Fn(Option<&str>, &str) -> String + Send + Sync + 'static,
        proxy_resolver: impl Fn(Option<&str>, &str) -> Option<String> + Send + Sync + 'static,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            injected: RwLock::new(HashMap::new()),
            key_resolver: Box::new(key_resolver),
            proxy_resolver: Box::new(proxy_resolver),
            timeout_policy,
        }
    }

    pub fn get_or_create(&self, provider_name: &str) -> Result<Arc<dyn Provider>> {
        self.get_or_create_for_scope(None, provider_name)
    }

    pub fn get_or_create_for_workspace(
        &self,
        workspace_id: &str,
        provider_name: &str,
    ) -> Result<Arc<dyn Provider>> {
        self.get_or_create_for_scope(Some(workspace_id), provider_name)
    }

    pub fn authority_fingerprint_for_workspace(
        &self,
        workspace_id: &str,
        provider_name: &str,
    ) -> ProviderAuthorityFingerprint {
        self.resolve_authority(Some(workspace_id), provider_name).2
    }

    fn get_or_create_for_scope(
        &self,
        workspace_id: Option<&str>,
        provider_name: &str,
    ) -> Result<Arc<dyn Provider>> {
        // Resolve the complete effective authority before consulting the
        // cache. A workspace lookup can therefore never fall back to an
        // instance created for a different credential/account scope.
        let (api_key, proxy_url, authority_fingerprint) =
            self.resolve_authority(workspace_id, provider_name);
        let key = ProviderCacheKey::new(workspace_id, provider_name, authority_fingerprint.clone());
        {
            // A poisoned cache must not panic the native agent actor. The
            // registry only stores independently owned provider Arcs, so the
            // map remains structurally usable after recovering the guard and
            // the caller can continue through the normal provider error path.
            let cache = self
                .cache
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(provider) = cache.get(&key) {
                return Ok(provider.clone());
            }
        }

        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(provider) = cache.get(&key) {
            return Ok(provider.clone());
        }

        let provider: Arc<dyn Provider> = if let Some(provider) = self
            .injected
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key.provider_name)
            .cloned()
        {
            provider
        } else {
            Arc::from(create_provider_with_timeout_policy_and_proxy_and_authority(
                &key.provider_name,
                &api_key,
                self.timeout_policy,
                proxy_url.as_deref(),
                authority_fingerprint.as_str(),
            )?)
        };
        let provider: Arc<dyn Provider> = Arc::new(AuthorityBoundProvider {
            inner: provider,
            authority_fingerprint,
        });
        cache.insert(key, provider.clone());
        Ok(provider)
    }

    fn resolve_authority(
        &self,
        workspace_id: Option<&str>,
        provider_name: &str,
    ) -> (String, Option<String>, ProviderAuthorityFingerprint) {
        let api_key = (self.key_resolver)(workspace_id, provider_name);
        let proxy_url = (self.proxy_resolver)(workspace_id, provider_name);
        let mut digest = Sha256::new();
        digest.update(PROVIDER_AUTHORITY_FINGERPRINT_VERSION.as_bytes());
        digest.update([0]);
        digest.update(workspace_id.unwrap_or("<global>").as_bytes());
        digest.update([0]);
        digest.update(provider_name.trim().to_ascii_lowercase().as_bytes());
        digest.update([0]);
        digest.update(api_key.as_bytes());
        digest.update([0]);
        digest.update(proxy_url.as_deref().unwrap_or("<direct>").as_bytes());
        (
            api_key,
            proxy_url,
            ProviderAuthorityFingerprint(hex::encode(digest.finalize())),
        )
    }

    pub fn insert(&self, name: impl Into<String>, provider: Arc<dyn Provider>) {
        let name = name.into();
        let (_, _, authority_fingerprint) = self.resolve_authority(None, name.as_str());
        self.injected
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.clone(), provider.clone());
        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let provider: Arc<dyn Provider> = Arc::new(AuthorityBoundProvider {
            inner: provider,
            authority_fingerprint: authority_fingerprint.clone(),
        });
        cache.insert(
            ProviderCacheKey::new(None, name.as_str(), authority_fingerprint),
            provider,
        );
    }

    pub fn invalidate(&self, provider_name: &str) {
        self.injected
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(provider_name);
        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.retain(|key, _| key.provider_name != provider_name);
    }
}

/// Create a registry with a single pre-seeded provider. For tests.
impl ProviderRegistry {
    pub fn with_provider(name: &str, provider: Arc<dyn Provider>) -> Self {
        let registry = Self {
            cache: RwLock::new(HashMap::new()),
            injected: RwLock::new(HashMap::new()),
            key_resolver: Box::new(|_, _| String::new()),
            proxy_resolver: Box::new(|_, _| None),
            timeout_policy: ProviderTimeoutPolicy::default(),
        };
        registry.insert(name, provider);
        registry
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
    fn poisoned_cache_is_recovered_without_panicking() {
        let registry = ProviderRegistry::with_provider("echo", Arc::new(EchoProvider::new()));
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = registry.cache.write().expect("cache should be writable");
            panic!("test cache poison");
        }));
        assert!(poisoned.is_err());

        let provider = registry
            .get_or_create("echo")
            .expect("poisoned cache should remain readable");
        assert_eq!(provider.name(), "echo");
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
    fn key_resolver_is_called_before_every_authority_lookup() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let call_count = Arc::new(AtomicUsize::new(0));
        let count_clone = call_count.clone();

        let registry = ProviderRegistry::new(move |_name| {
            count_clone.fetch_add(1, Ordering::SeqCst);
            String::new()
        });

        // Pre-seed so creation succeeds
        registry.insert("echo", Arc::new(EchoProvider::new()));

        // Insert and lookup both resolve authority so credential rotation can
        // never be hidden by an older cache hit.
        let _ = registry.get_or_create("echo").unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
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
                (Some("workspace-1".to_owned()), "ollama".to_owned()),
                (Some("workspace-2".to_owned()), "ollama".to_owned()),
            ]
        );
    }

    #[test]
    fn scoped_registry_cache_key_includes_workspace_proxy() {
        use std::sync::Mutex;

        let proxy_url = Arc::new(Mutex::new("http://127.0.0.1:8080".to_owned()));
        let proxy_url_clone = proxy_url.clone();
        let registry = ProviderRegistry::new_scoped_with_timeout_policy_and_proxy(
            |_, _| String::new(),
            move |workspace_id, _| {
                workspace_id.map(|_| proxy_url_clone.lock().expect("proxy lock").clone())
            },
            ProviderTimeoutPolicy::default(),
        );

        let first = registry
            .get_or_create_for_workspace("workspace-1", "ollama")
            .unwrap();
        let second = registry
            .get_or_create_for_workspace("workspace-1", "ollama")
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));

        *proxy_url.lock().expect("proxy lock") = "http://127.0.0.1:8081".to_owned();
        let after_proxy_change = registry
            .get_or_create_for_workspace("workspace-1", "ollama")
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &after_proxy_change));
    }

    #[test]
    fn workspace_lookup_never_reuses_global_credential_authority() {
        let registry = ProviderRegistry::new_scoped(|workspace_id, _| match workspace_id {
            None => "global-account-key".to_owned(),
            Some("workspace-a") => "workspace-a-account-key".to_owned(),
            Some(other) => format!("{other}-account-key"),
        });
        registry.insert("echo", Arc::new(EchoProvider::new()));

        // Exercise both lookup orders.  A global-first cache hit must not
        // become the workspace authority, and a workspace-first lookup must
        // not poison the global scope.
        let global = registry.get_or_create("echo").expect("global provider");
        let workspace = registry
            .get_or_create_for_workspace("workspace-a", "echo")
            .expect("workspace provider");
        assert!(!Arc::ptr_eq(&global, &workspace));
        assert_ne!(
            global.authority_fingerprint(),
            workspace.authority_fingerprint(),
            "credential/account authority must be part of the cache identity"
        );

        let workspace_again = registry
            .get_or_create_for_workspace("workspace-a", "echo")
            .expect("workspace provider cache hit");
        let global_again = registry.get_or_create("echo").expect("global cache hit");
        assert!(Arc::ptr_eq(&workspace, &workspace_again));
        assert!(Arc::ptr_eq(&global, &global_again));
    }
}
