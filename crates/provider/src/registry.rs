use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use sha2::{Digest, Sha256};

use crate::factory::create_provider_with_timeout_policy_and_proxy_and_authority;
use crate::traits::{Provider, ProviderWarmupOutcome};
use crate::types::{
    ChatRequest, ChatResponse, EmbeddingRequest, EmbeddingResponse, ProviderCapabilities,
    ProviderFailureClassification, ProviderTimeoutPolicy, StreamChunk,
};
use pioneer_protocol::ProviderModelInfo;

const PROVIDER_AUTHORITY_FINGERPRINT_VERSION: &str = "pioneer-provider-authority-v1";
const DEFAULT_PROVIDER_CACHE_MAX_ENTRIES: usize = 256;
const DEFAULT_INJECTED_PROVIDER_MAX_ENTRIES: usize = 64;
const DEFAULT_PROVIDER_CACHE_IDLE_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_PROVIDER_CACHE_ENTRIES: usize = 4_096;
const MAX_INJECTED_PROVIDER_ENTRIES: usize = 1_024;
const MAX_PROVIDER_CACHE_IDLE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const AUTHORITY_SCOPE_LOCK_COUNT: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderRegistryLimits {
    pub max_cached_instances: usize,
    pub max_injected_providers: usize,
    pub idle_ttl: Duration,
}

impl Default for ProviderRegistryLimits {
    fn default() -> Self {
        Self {
            max_cached_instances: DEFAULT_PROVIDER_CACHE_MAX_ENTRIES,
            max_injected_providers: DEFAULT_INJECTED_PROVIDER_MAX_ENTRIES,
            idle_ttl: DEFAULT_PROVIDER_CACHE_IDLE_TTL,
        }
    }
}

impl ProviderRegistryLimits {
    fn normalized(self) -> Self {
        Self {
            max_cached_instances: self
                .max_cached_instances
                .clamp(1, MAX_PROVIDER_CACHE_ENTRIES),
            max_injected_providers: self
                .max_injected_providers
                .clamp(1, MAX_INJECTED_PROVIDER_ENTRIES),
            idle_ttl: self
                .idle_ttl
                .clamp(Duration::from_millis(1), MAX_PROVIDER_CACHE_IDLE_TTL),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderRegistryStats {
    pub cached_instances: usize,
    pub injected_providers: usize,
    pub max_cached_instances: usize,
    pub max_injected_providers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderAuthorityRevoked;

impl Display for ProviderAuthorityRevoked {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("provider authority has been revoked")
    }
}

impl Error for ProviderAuthorityRevoked {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderRegistryCapacityExceeded {
    pub max_cached_instances: usize,
}

impl Display for ProviderRegistryCapacityExceeded {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "provider authority registry reached its {} active-instance limit",
            self.max_cached_instances
        )
    }
}

impl Error for ProviderRegistryCapacityExceeded {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderRegistryDefinitionCapacityExceeded {
    pub max_injected_providers: usize,
}

impl Display for ProviderRegistryDefinitionCapacityExceeded {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "provider registry reached its {} injected-definition limit",
            self.max_injected_providers
        )
    }
}

impl Error for ProviderRegistryDefinitionCapacityExceeded {}

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
    revoked: Arc<AtomicBool>,
}

impl AuthorityBoundProvider {
    fn ensure_not_revoked(&self) -> Result<()> {
        if self.revoked.load(Ordering::Acquire) {
            return Err(ProviderAuthorityRevoked.into());
        }
        Ok(())
    }
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

    fn native_file_tool_capability(
        &self,
        model: &str,
    ) -> crate::file_tools::NativeFileToolCapability {
        self.inner.native_file_tool_capability(model)
    }

    fn classify_failure(&self, error: &anyhow::Error) -> Option<ProviderFailureClassification> {
        self.inner.classify_failure(error)
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        self.ensure_not_revoked()?;
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
        self.ensure_not_revoked()?;
        crate::attachments::runtime::with_async_authority_scope(
            self.authority_fingerprint.as_str().to_owned(),
            self.inner.stream_chat(request),
        )
        .await
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        self.ensure_not_revoked()?;
        self.inner.list_models().await
    }

    async fn list_embedding_models(&self) -> Result<Vec<ProviderModelInfo>> {
        self.ensure_not_revoked()?;
        self.inner.list_embedding_models().await
    }

    async fn list_transcription_models(&self) -> Result<Vec<ProviderModelInfo>> {
        self.ensure_not_revoked()?;
        self.inner.list_transcription_models().await
    }

    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse> {
        self.ensure_not_revoked()?;
        self.inner.embed(request).await
    }

    async fn warmup(&self) -> Result<ProviderWarmupOutcome> {
        self.ensure_not_revoked()?;
        self.inner.warmup().await
    }
}

struct ProviderCacheEntry {
    provider: Arc<dyn Provider>,
    revoked: Arc<AtomicBool>,
    last_access: Instant,
    access_sequence: u64,
}

#[derive(Default)]
struct ProviderCacheState {
    entries: HashMap<ProviderCacheKey, ProviderCacheEntry>,
    next_access_sequence: u64,
}

impl ProviderCacheState {
    fn next_sequence(&mut self) -> u64 {
        self.next_access_sequence = self.next_access_sequence.saturating_add(1);
        self.next_access_sequence
    }

    fn prune_expired(&mut self, now: Instant, idle_ttl: Duration) {
        self.entries.retain(|_, entry| {
            let expired = now
                .checked_duration_since(entry.last_access)
                .is_some_and(|idle| idle >= idle_ttl);
            // The cache's Arc is also the revocation index for every issued
            // wrapper. Removing an externally owned entry would make a later
            // credential/config invalidation unable to fence that authority.
            // Treat external Arc ownership as an explicit active lease.
            !expired || Arc::strong_count(&entry.provider) > 1
        });
    }

    fn make_room_for_insert(&mut self, limit: usize) -> bool {
        while self.entries.len() >= limit {
            let Some(key) = self
                .entries
                .iter()
                .filter(|(_, entry)| Arc::strong_count(&entry.provider) == 1)
                .min_by_key(|(_, entry)| entry.access_sequence)
                .map(|(key, _)| key.clone())
            else {
                return false;
            };
            self.entries.remove(&key);
        }
        true
    }
}

/// Thread-safe, lazily-populated cache of provider instances.
///
/// Each unique provider name is created once (via [`create_provider`]) and then
/// served from the cache on subsequent requests. API keys are resolved through
/// the injected `key_resolver` closure, keeping environment-specific logic out
/// of the provider crate.
pub struct ProviderRegistry {
    cache: RwLock<ProviderCacheState>,
    /// Explicitly injected providers are a test/integration seam. Unlike the
    /// production factory, an injected provider name is intentionally valid
    /// in every workspace; each lookup still receives a scope-specific
    /// authority wrapper and cache key.
    injected: RwLock<HashMap<String, Arc<dyn Provider>>>,
    /// Bounded, secret-free striped ownership gates. A lookup and an
    /// invalidation for the same workspace/provider scope serialize on the
    /// same stripe, so a resolver admitted before revocation may finish but
    /// cannot republish stale authority after revocation returns. Unrelated
    /// tenants neither retry nor fail when another scope mutates.
    authority_scope_locks: [Mutex<()>; AUTHORITY_SCOPE_LOCK_COUNT],
    key_resolver: Box<dyn Fn(Option<&str>, &str) -> Result<String> + Send + Sync>,
    proxy_resolver: Box<dyn Fn(Option<&str>, &str) -> Result<Option<String>> + Send + Sync>,
    timeout_policy: ProviderTimeoutPolicy,
    limits: ProviderRegistryLimits,
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
        Self::new_scoped_with_timeout_policy_proxy_and_limits(
            move |_, provider_name| key_resolver(provider_name),
            |_, _| None,
            timeout_policy,
            ProviderRegistryLimits::default(),
        )
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
        Self::new_scoped_with_timeout_policy_proxy_and_limits(
            key_resolver,
            proxy_resolver,
            timeout_policy,
            ProviderRegistryLimits::default(),
        )
    }

    pub fn new_scoped_with_timeout_policy_proxy_and_limits(
        key_resolver: impl Fn(Option<&str>, &str) -> String + Send + Sync + 'static,
        proxy_resolver: impl Fn(Option<&str>, &str) -> Option<String> + Send + Sync + 'static,
        timeout_policy: ProviderTimeoutPolicy,
        limits: ProviderRegistryLimits,
    ) -> Self {
        Self::new_scoped_fallible_with_timeout_policy_proxy_and_limits(
            move |workspace_id, provider_name| Ok(key_resolver(workspace_id, provider_name)),
            move |workspace_id, provider_name| Ok(proxy_resolver(workspace_id, provider_name)),
            timeout_policy,
            limits,
        )
    }

    /// Production constructor for authority sources whose reads can fail.
    /// Resolver failures are never converted into an empty credential or a
    /// direct-network fallback, because either would silently change the
    /// effective authority boundary.
    pub fn new_scoped_fallible_with_timeout_policy_and_proxy(
        key_resolver: impl Fn(Option<&str>, &str) -> Result<String> + Send + Sync + 'static,
        proxy_resolver: impl Fn(Option<&str>, &str) -> Result<Option<String>> + Send + Sync + 'static,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self::new_scoped_fallible_with_timeout_policy_proxy_and_limits(
            key_resolver,
            proxy_resolver,
            timeout_policy,
            ProviderRegistryLimits::default(),
        )
    }

    pub fn new_scoped_fallible_with_timeout_policy_proxy_and_limits(
        key_resolver: impl Fn(Option<&str>, &str) -> Result<String> + Send + Sync + 'static,
        proxy_resolver: impl Fn(Option<&str>, &str) -> Result<Option<String>> + Send + Sync + 'static,
        timeout_policy: ProviderTimeoutPolicy,
        limits: ProviderRegistryLimits,
    ) -> Self {
        Self {
            cache: RwLock::new(ProviderCacheState::default()),
            injected: RwLock::new(HashMap::new()),
            authority_scope_locks: std::array::from_fn(|_| Mutex::new(())),
            key_resolver: Box::new(key_resolver),
            proxy_resolver: Box::new(proxy_resolver),
            timeout_policy,
            limits: limits.normalized(),
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
    ) -> Result<ProviderAuthorityFingerprint> {
        let provider_name = normalize_provider_name(provider_name);
        let _scope = self.lock_authority_scope(Some(workspace_id), provider_name.as_str());
        let fingerprint = self
            .resolve_authority(Some(workspace_id), provider_name.as_str())?
            .2;
        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::revoke_matching(&mut cache, |existing| {
            existing.workspace_id.as_deref() == Some(workspace_id)
                && existing.provider_name == provider_name
                && existing.authority_fingerprint != fingerprint
        });
        Ok(fingerprint)
    }

    /// Build and immediately discard a candidate adapter before its
    /// credential/configuration is published. This catches unsupported
    /// providers, malformed proxy configuration, and local client-construction
    /// failures without revoking the currently authoritative instance.
    pub fn validate_candidate_workspace_authority(
        &self,
        workspace_id: &str,
        provider_name: &str,
        api_key: Option<&str>,
        proxy_url: Option<&str>,
    ) -> Result<()> {
        let provider_name = normalize_provider_name(provider_name);
        if self
            .injected
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(provider_name.as_str())
        {
            return Ok(());
        }
        let authority_fingerprint = Self::authority_fingerprint(
            Some(workspace_id),
            provider_name.as_str(),
            api_key.unwrap_or_default(),
            proxy_url,
        );
        create_provider_with_timeout_policy_and_proxy_and_authority(
            provider_name.as_str(),
            api_key.unwrap_or_default(),
            self.timeout_policy,
            proxy_url,
            authority_fingerprint.as_str(),
        )?;
        Ok(())
    }

    fn get_or_create_for_scope(
        &self,
        workspace_id: Option<&str>,
        provider_name: &str,
    ) -> Result<Arc<dyn Provider>> {
        let provider_name = normalize_provider_name(provider_name);
        // The same bounded stripe is also a per-scope singleflight gate. It
        // prevents duplicate provider construction on concurrent cache
        // misses without retaining one mutex per tenant or credential.
        let _scope = self.lock_authority_scope(workspace_id, provider_name.as_str());
        // Resolve the complete effective authority without holding cache or
        // injected-provider locks. The scope gate is deliberately retained so
        // the matching invalidation cannot return before this authority is
        // either published or discarded.
        let (api_key, proxy_url, authority_fingerprint) =
            self.resolve_authority(workspace_id, provider_name.as_str())?;
        let key = ProviderCacheKey::new(
            workspace_id,
            provider_name.as_str(),
            authority_fingerprint.clone(),
        );
        {
            let mut cache = self
                .cache
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let now = Instant::now();
            cache.prune_expired(now, self.limits.idle_ttl);
            let cached_provider = cache.entries.get(&key).map(|entry| entry.provider.clone());
            if let Some(provider) = cached_provider {
                let access_sequence = cache.next_sequence();
                if let Some(entry) = cache.entries.get_mut(&key) {
                    entry.last_access = now;
                    entry.access_sequence = access_sequence;
                }
                return Ok(provider);
            }
            // Resolving a different effective key/proxy is itself proof of
            // authority rotation. Fence every older wrapper in this exact
            // tenant/provider scope before attempting construction of the new
            // adapter; otherwise a failed warm replacement would leave stale
            // credentials usable by existing holders.
            Self::revoke_matching(&mut cache, |existing| {
                existing.workspace_id == key.workspace_id
                    && existing.provider_name == key.provider_name
                    && existing.authority_fingerprint != key.authority_fingerprint
            });
        }

        let injected = self
            .injected
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(provider_name.as_str())
            .cloned();
        let provider: Arc<dyn Provider> = match injected {
            Some(provider) => provider,
            None => Arc::from(create_provider_with_timeout_policy_and_proxy_and_authority(
                provider_name.as_str(),
                &api_key,
                self.timeout_policy,
                proxy_url.as_deref(),
                authority_fingerprint.as_str(),
            )?),
        };

        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        cache.prune_expired(now, self.limits.idle_ttl);
        let access_sequence = cache.next_sequence();
        let revoked = Arc::new(AtomicBool::new(false));
        let provider: Arc<dyn Provider> = Arc::new(AuthorityBoundProvider {
            inner: provider,
            authority_fingerprint,
            revoked: revoked.clone(),
        });
        if !cache.make_room_for_insert(self.limits.max_cached_instances) {
            return Err(ProviderRegistryCapacityExceeded {
                max_cached_instances: self.limits.max_cached_instances,
            }
            .into());
        }
        cache.entries.insert(
            key,
            ProviderCacheEntry {
                provider: provider.clone(),
                revoked,
                last_access: now,
                access_sequence,
            },
        );
        Ok(provider)
    }

    fn lock_authority_scope(
        &self,
        workspace_id: Option<&str>,
        provider_name: &str,
    ) -> MutexGuard<'_, ()> {
        let index = Self::authority_scope_lock_index(workspace_id, provider_name);
        self.authority_scope_locks[index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn authority_scope_lock_index(workspace_id: Option<&str>, provider_name: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        workspace_id.unwrap_or("<global>").hash(&mut hasher);
        provider_name.hash(&mut hasher);
        (hasher.finish() % AUTHORITY_SCOPE_LOCK_COUNT as u64) as usize
    }

    fn lock_all_authority_scopes(&self) -> Vec<MutexGuard<'_, ()>> {
        self.authority_scope_locks
            .iter()
            .map(|lock| {
                lock.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
            })
            .collect()
    }

    fn resolve_authority(
        &self,
        workspace_id: Option<&str>,
        provider_name: &str,
    ) -> Result<(String, Option<String>, ProviderAuthorityFingerprint)> {
        let api_key = (self.key_resolver)(workspace_id, provider_name)?;
        let proxy_url = (self.proxy_resolver)(workspace_id, provider_name)?;
        let authority_fingerprint = Self::authority_fingerprint(
            workspace_id,
            provider_name,
            api_key.as_str(),
            proxy_url.as_deref(),
        );
        Ok((api_key, proxy_url, authority_fingerprint))
    }

    fn authority_fingerprint(
        workspace_id: Option<&str>,
        provider_name: &str,
        api_key: &str,
        proxy_url: Option<&str>,
    ) -> ProviderAuthorityFingerprint {
        let mut digest = Sha256::new();
        digest.update(PROVIDER_AUTHORITY_FINGERPRINT_VERSION.as_bytes());
        digest.update([0]);
        digest.update(workspace_id.unwrap_or("<global>").as_bytes());
        digest.update([0]);
        digest.update(provider_name.trim().to_ascii_lowercase().as_bytes());
        digest.update([0]);
        digest.update(api_key.as_bytes());
        digest.update([0]);
        digest.update(proxy_url.unwrap_or("<direct>").as_bytes());
        ProviderAuthorityFingerprint(hex::encode(digest.finalize()))
    }

    pub fn insert(&self, name: impl Into<String>, provider: Arc<dyn Provider>) -> Result<()> {
        let name = normalize_provider_name(name.into().as_str());
        let _scopes = self.lock_all_authority_scopes();
        let (_, _, authority_fingerprint) = self.resolve_authority(None, name.as_str())?;
        let mut injected = self
            .injected
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !injected.contains_key(name.as_str())
            && injected.len() >= self.limits.max_injected_providers
        {
            return Err(ProviderRegistryDefinitionCapacityExceeded {
                max_injected_providers: self.limits.max_injected_providers,
            }
            .into());
        }
        injected.insert(name.clone(), provider.clone());
        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::revoke_matching(&mut cache, |key| key.provider_name == name);
        let now = Instant::now();
        let access_sequence = cache.next_sequence();
        let revoked = Arc::new(AtomicBool::new(false));
        let provider: Arc<dyn Provider> = Arc::new(AuthorityBoundProvider {
            inner: provider,
            authority_fingerprint: authority_fingerprint.clone(),
            revoked: revoked.clone(),
        });
        cache.prune_expired(now, self.limits.idle_ttl);
        if cache.make_room_for_insert(self.limits.max_cached_instances) {
            cache.entries.insert(
                ProviderCacheKey::new(None, name.as_str(), authority_fingerprint),
                ProviderCacheEntry {
                    provider,
                    revoked,
                    last_access: now,
                    access_sequence,
                },
            );
        }
        Ok(())
    }

    fn revoke_matching(
        cache: &mut ProviderCacheState,
        predicate: impl Fn(&ProviderCacheKey) -> bool,
    ) -> usize {
        let keys = cache
            .entries
            .keys()
            .filter(|key| predicate(key))
            .cloned()
            .collect::<Vec<_>>();
        let mut revoked_count = 0;
        for key in keys {
            if let Some(entry) = cache.entries.remove(&key) {
                entry.revoked.store(true, Ordering::Release);
                revoked_count += 1;
            }
        }
        revoked_count
    }

    /// Revoke exactly one workspace/provider scope. Other tenants using the
    /// same adapter remain cached and usable.
    pub fn invalidate_workspace_provider(&self, workspace_id: &str, provider_name: &str) -> usize {
        let provider_name = normalize_provider_name(provider_name);
        let _scope = self.lock_authority_scope(Some(workspace_id), provider_name.as_str());
        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let revoked = Self::revoke_matching(&mut cache, |key| {
            key.workspace_id.as_deref() == Some(workspace_id) && key.provider_name == provider_name
        });
        revoked
    }

    pub fn invalidate_global_provider(&self, provider_name: &str) -> usize {
        let provider_name = normalize_provider_name(provider_name);
        let _scope = self.lock_authority_scope(None, provider_name.as_str());
        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let revoked = Self::revoke_matching(&mut cache, |key| {
            key.workspace_id.is_none() && key.provider_name == provider_name
        });
        revoked
    }

    /// Administrative all-scope revocation. Normal credential/config updates
    /// must use `invalidate_workspace_provider` instead.
    pub fn invalidate_all_provider_authorities(&self, provider_name: &str) -> usize {
        let provider_name = normalize_provider_name(provider_name);
        let _scopes = self.lock_all_authority_scopes();
        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let revoked = Self::revoke_matching(&mut cache, |key| key.provider_name == provider_name);
        revoked
    }

    /// Backward-compatible global-scope API. Unlike the former implementation
    /// it does not silently invalidate unrelated workspaces.
    pub fn invalidate(&self, provider_name: &str) {
        self.invalidate_global_provider(provider_name);
    }

    pub fn remove_injected_provider(&self, provider_name: &str) -> Option<Arc<dyn Provider>> {
        let provider_name = normalize_provider_name(provider_name);
        let _scopes = self.lock_all_authority_scopes();
        let mut injected = self
            .injected
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let removed = injected.remove(provider_name.as_str());
        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::revoke_matching(&mut cache, |key| key.provider_name == provider_name);
        removed
    }

    pub fn prune_idle(&self) -> usize {
        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = cache.entries.len();
        cache.prune_expired(Instant::now(), self.limits.idle_ttl);
        before.saturating_sub(cache.entries.len())
    }

    pub fn stats(&self) -> ProviderRegistryStats {
        self.prune_idle();
        // Do not retain either read guard across construction of the result.
        // Mutating paths intentionally acquire `injected` before `cache`;
        // keeping the cache guard alive while taking `injected` here would
        // invert that order and permit a readiness/registry deadlock.
        let cached_instances = {
            self.cache
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .len()
        };
        let injected_providers = {
            self.injected
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
        };
        ProviderRegistryStats {
            cached_instances,
            injected_providers,
            max_cached_instances: self.limits.max_cached_instances,
            max_injected_providers: self.limits.max_injected_providers,
        }
    }
}

fn normalize_provider_name(provider_name: &str) -> String {
    provider_name.trim().to_ascii_lowercase()
}

/// Create a registry with a single pre-seeded provider. For tests.
impl ProviderRegistry {
    pub fn with_provider(name: &str, provider: Arc<dyn Provider>) -> Self {
        Self::with_provider_and_limits(name, provider, ProviderRegistryLimits::default())
    }

    pub fn with_provider_and_limits(
        name: &str,
        provider: Arc<dyn Provider>,
        limits: ProviderRegistryLimits,
    ) -> Self {
        let registry = Self::new_scoped_with_timeout_policy_proxy_and_limits(
            |_, _| String::new(),
            |_, _| None,
            ProviderTimeoutPolicy::default(),
            limits,
        );
        registry
            .insert(name, provider)
            .expect("single injected provider must fit the configured registry bound");
        registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::EchoProvider;
    use crate::{ProviderTermination, TokenUsage};
    use futures_util::stream;

    fn chat_request() -> ChatRequest {
        ChatRequest {
            model: "test-model".to_owned(),
            messages: vec![crate::ChatMessage::user("hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        }
    }

    #[test]
    fn registry_configuration_is_hard_bounded() {
        let limits = ProviderRegistryLimits {
            max_cached_instances: usize::MAX,
            max_injected_providers: usize::MAX,
            idle_ttl: Duration::MAX,
        }
        .normalized();

        assert_eq!(limits.max_cached_instances, MAX_PROVIDER_CACHE_ENTRIES);
        assert_eq!(limits.max_injected_providers, MAX_INJECTED_PROVIDER_ENTRIES);
        assert_eq!(limits.idle_ttl, MAX_PROVIDER_CACHE_IDLE_TTL);
    }

    struct BlockingProvider {
        entered: Arc<tokio::sync::Semaphore>,
        release: Arc<tokio::sync::Semaphore>,
    }

    #[async_trait]
    impl Provider for BlockingProvider {
        fn name(&self) -> &str {
            "blocking"
        }

        async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse> {
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .expect("release semaphore should remain open")
                .forget();
            Ok(ChatResponse {
                text: "done".to_owned(),
                reasoning_content: None,
                tool_calls: Vec::new(),
                usage: Some(TokenUsage::default()),
                provider_replay_state: None,
                termination: ProviderTermination::Complete,
            })
        }

        async fn stream_chat(
            &self,
            _request: ChatRequest,
        ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
            Ok(Box::pin(stream::empty()))
        }
    }

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
        registry
            .insert("echo", echo.clone())
            .expect("test provider should fit registry bounds");

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
        registry
            .insert("echo", Arc::new(EchoProvider::new()))
            .expect("test provider should fit registry bounds");

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

    #[tokio::test]
    async fn scoped_registry_rotation_revokes_the_previous_workspace_authority() {
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
        registry
            .insert("echo", Arc::new(EchoProvider::new()))
            .expect("test provider should fit registry bounds");

        let first = registry
            .get_or_create_for_workspace("workspace-1", "echo")
            .unwrap();
        let second = registry
            .get_or_create_for_workspace("workspace-1", "echo")
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));

        *proxy_url.lock().expect("proxy lock") = "http://127.0.0.1:8081".to_owned();
        let after_proxy_change = registry
            .get_or_create_for_workspace("workspace-1", "echo")
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &after_proxy_change));
        let revoked = first
            .chat(chat_request())
            .await
            .expect_err("the previous authority must be fenced after observed rotation");
        assert!(revoked.downcast_ref::<ProviderAuthorityRevoked>().is_some());
        after_proxy_change
            .chat(chat_request())
            .await
            .expect("the replacement authority should remain usable");
    }

    #[tokio::test]
    async fn authority_source_failure_never_falls_back_or_revokes_last_known_authority() {
        let fail_reads = Arc::new(AtomicBool::new(false));
        let fail_key_reads = fail_reads.clone();
        let registry = ProviderRegistry::new_scoped_fallible_with_timeout_policy_and_proxy(
            move |workspace_id, _| {
                if workspace_id.is_some() && fail_key_reads.load(Ordering::SeqCst) {
                    anyhow::bail!("injected credential store outage");
                }
                Ok("workspace-authority".to_owned())
            },
            |_, _| Ok(Some("http://127.0.0.1:8080".to_owned())),
            ProviderTimeoutPolicy::default(),
        );
        registry
            .insert("echo", Arc::new(EchoProvider::new()))
            .expect("test provider should fit registry bounds");
        let issued = registry
            .get_or_create_for_workspace("workspace-a", "echo")
            .expect("initial authority should resolve");

        fail_reads.store(true, Ordering::SeqCst);
        assert!(
            registry
                .get_or_create_for_workspace("workspace-a", "echo")
                .is_err(),
            "a credential-store error must not become an empty credential or direct route"
        );
        assert!(
            registry
                .authority_fingerprint_for_workspace("workspace-a", "echo")
                .is_err(),
            "authority verification must surface the same typed source failure"
        );
        issued
            .chat(chat_request())
            .await
            .expect("an unresolved read failure must not falsely revoke the last known authority");
    }

    #[test]
    fn workspace_lookup_never_reuses_global_credential_authority() {
        let registry = ProviderRegistry::new_scoped(|workspace_id, _| match workspace_id {
            None => "global-account-key".to_owned(),
            Some("workspace-a") => "workspace-a-account-key".to_owned(),
            Some(other) => format!("{other}-account-key"),
        });
        registry
            .insert("echo", Arc::new(EchoProvider::new()))
            .expect("test provider should fit registry bounds");

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

    #[tokio::test]
    async fn scoped_invalidation_revokes_only_target_workspace_after_in_flight_call() {
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let registry = Arc::new(ProviderRegistry::with_provider(
            "blocking",
            Arc::new(BlockingProvider {
                entered: entered.clone(),
                release: release.clone(),
            }),
        ));
        let workspace_a = registry
            .get_or_create_for_workspace("workspace-a", "blocking")
            .expect("workspace A provider");
        let workspace_b = registry
            .get_or_create_for_workspace("workspace-b", "blocking")
            .expect("workspace B provider");

        let in_flight = tokio::spawn({
            let provider = workspace_a.clone();
            async move { provider.chat(chat_request()).await }
        });
        entered
            .acquire()
            .await
            .expect("in-flight call should enter provider")
            .forget();

        assert_eq!(
            registry.invalidate_workspace_provider("workspace-a", "blocking"),
            1
        );
        release.add_permits(1);
        assert!(
            in_flight.await.expect("in-flight task should join").is_ok(),
            "a call admitted before revocation remains owned by that Turn"
        );

        let revoked = workspace_a
            .chat(chat_request())
            .await
            .expect_err("future calls through an explicitly revoked Arc must fail");
        assert!(revoked.downcast_ref::<ProviderAuthorityRevoked>().is_some());
        assert_eq!(revoked.to_string(), "provider authority has been revoked");
        assert!(
            !revoked
                .to_string()
                .contains(workspace_a.authority_fingerprint().expect("fingerprint")),
            "stable secret-derived authority identity must not cross the error boundary"
        );

        release.add_permits(1);
        assert!(workspace_b.chat(chat_request()).await.is_ok());
        let workspace_b_again = registry
            .get_or_create_for_workspace("workspace-b", "blocking")
            .expect("unrelated workspace remains cached");
        assert!(Arc::ptr_eq(&workspace_b, &workspace_b_again));

        let workspace_a_recreated = registry
            .get_or_create_for_workspace("workspace-a", "blocking")
            .expect("revoked workspace can create a fresh generation");
        assert!(!Arc::ptr_eq(&workspace_a, &workspace_a_recreated));
    }

    #[tokio::test]
    async fn concurrent_resolution_cannot_reinsert_authority_after_scoped_invalidation() {
        use std::sync::Mutex;
        use std::sync::atomic::AtomicBool;
        use std::sync::mpsc::sync_channel;

        let credential = Arc::new(Mutex::new("credential-before".to_owned()));
        let block_next_workspace_resolution = Arc::new(AtomicBool::new(false));
        let (entered_tx, entered_rx) = sync_channel(1);
        let (release_tx, release_rx) = sync_channel(1);
        let release_rx = Arc::new(Mutex::new(release_rx));
        let registry = Arc::new(ProviderRegistry::new_scoped({
            let credential = credential.clone();
            let block_next_workspace_resolution = block_next_workspace_resolution.clone();
            let release_rx = release_rx.clone();
            move |workspace_id, provider_name| {
                assert_eq!(provider_name, "echo", "provider names are canonical");
                let resolved = credential.lock().expect("credential lock").clone();
                if workspace_id == Some("workspace-a")
                    && block_next_workspace_resolution.swap(false, Ordering::SeqCst)
                {
                    entered_tx.send(()).expect("signal blocked resolver");
                    release_rx
                        .lock()
                        .expect("release receiver lock")
                        .recv()
                        .expect("release blocked resolver");
                }
                resolved
            }
        }));
        registry
            .insert(" EcHo ", Arc::new(EchoProvider::new()))
            .expect("test provider should fit registry bounds");
        let old_fingerprint = registry
            .authority_fingerprint_for_workspace("workspace-a", "echo")
            .expect("old authority fingerprint");

        block_next_workspace_resolution.store(true, Ordering::SeqCst);
        let lookup = std::thread::spawn({
            let registry = registry.clone();
            move || {
                registry
                    .get_or_create_for_workspace("workspace-a", " ECHO ")
                    .expect("lookup admitted before invalidation may finish")
            }
        });
        entered_rx
            .recv()
            .expect("first authority resolution should block");
        *credential.lock().expect("credential lock") = "credential-after".to_owned();
        let invalidation = std::thread::spawn({
            let registry = registry.clone();
            move || registry.invalidate_workspace_provider("workspace-a", "eChO")
        });
        release_tx.send(()).expect("release authority resolver");

        let admitted = lookup.join().expect("lookup thread should join");
        assert_eq!(
            invalidation
                .join()
                .expect("invalidation thread should join"),
            1,
            "invalidation waits for and revokes authority published by an admitted resolver"
        );
        let revoked = admitted
            .chat(chat_request())
            .await
            .expect_err("the admitted stale Arc must be fenced once invalidation returns");
        assert!(revoked.downcast_ref::<ProviderAuthorityRevoked>().is_some());

        let resolved = registry
            .get_or_create_for_workspace("workspace-a", "echo")
            .expect("next lookup must bind the new authority");
        let current_fingerprint = registry
            .authority_fingerprint_for_workspace("workspace-a", "echo")
            .expect("current authority fingerprint");
        assert_eq!(
            resolved.authority_fingerprint(),
            Some(current_fingerprint.as_str())
        );
        assert_ne!(current_fingerprint, old_fingerprint);
        {
            let cache = registry
                .cache
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(cache.entries.keys().all(|key| key.provider_name == "echo"));
            assert!(
                cache
                    .entries
                    .keys()
                    .all(|key| key.authority_fingerprint != old_fingerprint)
            );
        }
        assert_eq!(
            registry.invalidate_workspace_provider("workspace-a", " ECHO "),
            1,
            "case and surrounding whitespace cannot split invalidation identity"
        );
    }

    #[test]
    fn unrelated_scope_lookup_does_not_fail_during_authority_churn() {
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::mpsc::sync_channel;

        let workspace_a = "workspace-a";
        let workspace_b = (0..1_000)
            .map(|index| format!("workspace-b-{index}"))
            .find(|candidate| {
                ProviderRegistry::authority_scope_lock_index(Some(candidate.as_str()), "echo")
                    != ProviderRegistry::authority_scope_lock_index(Some(workspace_a), "echo")
            })
            .expect("a distinct bounded authority stripe should exist");
        let block_workspace_a = Arc::new(AtomicBool::new(false));
        let (entered_tx, entered_rx) = sync_channel(1);
        let (release_tx, release_rx) = sync_channel(1);
        let release_rx = Arc::new(Mutex::new(release_rx));
        let registry = Arc::new(ProviderRegistry::new_scoped({
            let block_workspace_a = block_workspace_a.clone();
            let release_rx = release_rx.clone();
            move |workspace_id, _| {
                if workspace_id == Some(workspace_a)
                    && block_workspace_a.swap(false, Ordering::SeqCst)
                {
                    entered_tx.send(()).expect("signal blocked resolver");
                    release_rx
                        .lock()
                        .expect("release receiver lock")
                        .recv()
                        .expect("release blocked resolver");
                }
                format!("credential-for-{}", workspace_id.unwrap_or("global"))
            }
        }));
        registry
            .insert("echo", Arc::new(EchoProvider::new()))
            .expect("test provider should fit registry bounds");

        block_workspace_a.store(true, Ordering::SeqCst);
        let blocked_lookup = std::thread::spawn({
            let registry = registry.clone();
            move || {
                registry
                    .get_or_create_for_workspace(workspace_a, "echo")
                    .expect("blocked scope lookup")
            }
        });
        entered_rx
            .recv()
            .expect("workspace A resolver should block");

        registry
            .get_or_create_for_workspace(workspace_b.as_str(), "echo")
            .expect("unrelated tenant lookup must not retry or fail");

        release_tx.send(()).expect("release workspace A resolver");
        blocked_lookup
            .join()
            .expect("workspace A lookup thread should join");
    }

    #[tokio::test]
    async fn cache_lru_and_ttl_preserve_active_revocation_leases_within_the_bound() {
        let registry = ProviderRegistry::with_provider_and_limits(
            "echo",
            Arc::new(EchoProvider::new()),
            ProviderRegistryLimits {
                max_cached_instances: 2,
                max_injected_providers: 4,
                idle_ttl: Duration::from_secs(60),
            },
        );
        let workspace_a = registry
            .get_or_create_for_workspace("workspace-a", "echo")
            .expect("workspace A provider");
        registry
            .get_or_create_for_workspace("workspace-b", "echo")
            .expect("workspace B provider");
        registry
            .get_or_create_for_workspace("workspace-c", "echo")
            .expect("workspace C provider");
        assert_eq!(registry.stats().cached_instances, 2);

        // The active Turn lease remains tracked inside the bounded registry so
        // a later credential invalidation can still revoke it.
        assert!(workspace_a.chat(chat_request()).await.is_ok());
        let workspace_a_again = registry
            .get_or_create_for_workspace("workspace-a", "echo")
            .expect("active workspace provider remains tracked");
        assert!(Arc::ptr_eq(&workspace_a, &workspace_a_again));
        assert_eq!(registry.stats().cached_instances, 2);

        {
            let mut cache = registry
                .cache
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for entry in cache.entries.values_mut() {
                entry.last_access = Instant::now() - Duration::from_secs(120);
            }
        }
        assert_eq!(registry.prune_idle(), 1);
        assert_eq!(registry.stats().cached_instances, 1);
        assert_eq!(
            registry.invalidate_workspace_provider("workspace-a", "echo"),
            1
        );
        let revoked = workspace_a_again
            .chat(chat_request())
            .await
            .expect_err("an active lease must remain revocable after cache churn");
        assert!(revoked.downcast_ref::<ProviderAuthorityRevoked>().is_some());
        assert_eq!(registry.stats().cached_instances, 0);
    }

    #[test]
    fn cache_rejects_a_new_authority_when_every_bounded_entry_is_actively_leased() {
        let registry = ProviderRegistry::with_provider_and_limits(
            "echo",
            Arc::new(EchoProvider::new()),
            ProviderRegistryLimits {
                max_cached_instances: 1,
                max_injected_providers: 4,
                idle_ttl: Duration::from_secs(60),
            },
        );
        let workspace_a = registry
            .get_or_create_for_workspace("workspace-a", "echo")
            .expect("workspace A provider");

        let error = registry
            .get_or_create_for_workspace("workspace-b", "echo")
            .err()
            .expect("a live revocation lease cannot be silently evicted");
        assert_eq!(
            error
                .downcast_ref::<ProviderRegistryCapacityExceeded>()
                .copied(),
            Some(ProviderRegistryCapacityExceeded {
                max_cached_instances: 1,
            })
        );
        assert!(workspace_a.authority_fingerprint().is_some());
        assert_eq!(registry.stats().cached_instances, 1);
    }

    #[test]
    fn injected_provider_definitions_are_bounded_and_replacements_remain_allowed() {
        let registry = ProviderRegistry::new_scoped_with_timeout_policy_proxy_and_limits(
            |_, _| String::new(),
            |_, _| None,
            ProviderTimeoutPolicy::default(),
            ProviderRegistryLimits {
                max_cached_instances: 4,
                max_injected_providers: 1,
                idle_ttl: Duration::from_secs(60),
            },
        );
        registry
            .insert("echo", Arc::new(EchoProvider::new()))
            .expect("first injected definition should fit");
        registry
            .insert("echo", Arc::new(EchoProvider::new()))
            .expect("replacing the same definition must not consume capacity");

        let error = registry
            .insert("second", Arc::new(EchoProvider::new()))
            .expect_err("a second definition must be rejected at the configured bound");
        assert_eq!(
            error
                .downcast_ref::<ProviderRegistryDefinitionCapacityExceeded>()
                .copied(),
            Some(ProviderRegistryDefinitionCapacityExceeded {
                max_injected_providers: 1,
            })
        );
        assert_eq!(registry.stats().injected_providers, 1);
    }

    #[test]
    fn cache_identity_never_retains_raw_credentials_or_proxy_urls() {
        const SECRET: &str = "sk-private-cache-secret";
        const PROXY: &str = "http://proxy-user:proxy-password@127.0.0.1:8080";
        let registry = ProviderRegistry::new_scoped_with_timeout_policy_and_proxy(
            |_, _| SECRET.to_owned(),
            |_, _| Some(PROXY.to_owned()),
            ProviderTimeoutPolicy::default(),
        );
        registry
            .insert("echo", Arc::new(EchoProvider::new()))
            .expect("test provider should fit registry bounds");
        registry
            .get_or_create_for_workspace("workspace-a", "echo")
            .expect("workspace provider");

        let cache_debug = {
            let cache = registry
                .cache
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            format!("{:?}", cache.entries.keys().collect::<Vec<_>>())
        };
        assert!(!cache_debug.contains(SECRET));
        assert!(!cache_debug.contains("proxy-password"));
        assert!(!cache_debug.contains(PROXY));
    }
}
