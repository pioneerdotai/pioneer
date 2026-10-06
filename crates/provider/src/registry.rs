use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use futures_util::{StreamExt, stream::BoxStream};
use sha2::{Digest, Sha256};

use crate::factory::create_provider_with_timeout_policy_and_proxy_and_base_url_and_authority;
use crate::failure::{
    classify_http_error_body_too_large, classify_provider_failure_class, extract_provider_code,
    extract_retry_after_ms,
};
use crate::traits::{Provider, ProviderWarmupOutcome};
use crate::types::{
    ChatRequest, ChatResponse, EmbeddingRequest, EmbeddingResponse, ProviderCapabilities,
    ProviderFailureClassification, ProviderTimeoutPolicy, StreamChunk,
};
use pioneer_protocol::{ProviderFailureClass, ProviderFailureStage, ProviderModelInfo};

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

#[derive(Clone, Copy)]
enum ProviderOrigin {
    Factory,
    Injected,
}

impl ProviderOrigin {
    fn use_public_catalog(self, provider: &str, base_url: Option<&str>) -> bool {
        // Resolver URLs do not attest the endpoint of an injected inner.
        matches!(self, Self::Factory)
            && base_url.is_none_or(|endpoint| {
                crate::provider_definition(provider)
                    .and_then(|definition| definition.default_base_url)
                    .is_some_and(|stock| {
                        stock.trim_end_matches('/') == endpoint.trim_end_matches('/')
                    })
            })
    }
}

struct AuthorityBoundProvider {
    inner: Arc<dyn Provider>,
    authority_fingerprint: ProviderAuthorityFingerprint,
    revoked: Arc<AtomicBool>,
    redact_endpoint_errors: bool,
    discovery_tools: RwLock<BTreeMap<String, bool>>,
    discovery_reasoning: RwLock<BTreeMap<String, crate::generation::NativeReasoning>>,
    use_public_catalog: bool,
}

/// The request's endpoint can contain a secret path. Never retain a raw
/// adapter/transport error in an error returned from that provider instance.
#[derive(Debug)]
struct RedactedEndpointError {
    message: &'static str,
    classification: ProviderFailureClassification,
    incomplete: Option<crate::failure::ProviderStreamIncomplete>,
    native: Option<crate::failure::AnthropicStreamError>,
}

impl Display for RedactedEndpointError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.classification.http_status {
            Some(status) => write!(f, "{} (HTTP {status})", self.message),
            None => f.write_str(self.message),
        }
    }
}

impl Error for RedactedEndpointError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.native
            .as_ref()
            .map(|cause| cause as &(dyn Error + 'static))
            .or_else(|| {
                self.incomplete
                    .as_ref()
                    .map(|cause| cause as &(dyn Error + 'static))
            })
    }
}

fn endpoint_error_status(error: &anyhow::Error) -> Option<u16> {
    if let Some(status) = error.downcast_ref::<crate::types::ProviderHttpErrorBodyTooLarge>() {
        return Some(status.status);
    }
    for cause in error.chain() {
        if let Some(request) = cause.downcast_ref::<reqwest::Error>()
            && let Some(status) = request.status()
        {
            return Some(status.as_u16());
        }
    }
    // The supported adapters format non-success responses as `API error
    // (429 Too Many Requests): ...`. Parse only that fixed prefix, never an
    // arbitrary three-digit sequence in a provider-controlled body or URL.
    let message = format!("{error:#}");
    let rest = message.split_once("API error (")?.1;
    let status = rest.get(..3)?.parse::<u16>().ok()?;
    (100..600).contains(&status).then_some(status)
}

fn redacted_endpoint_error(
    inner: &dyn Provider,
    error: anyhow::Error,
    stage: ProviderFailureStage,
) -> anyhow::Error {
    if error.is::<RedactedEndpointError>() {
        return error;
    }
    // Adapters can supply structured status even when their error does not
    // contain a reqwest source or the usual `API error (...)` prefix.
    let adapter_classification = inner.classify_failure(&error);
    let status = adapter_classification
        .as_ref()
        .and_then(|classification| classification.http_status)
        .or_else(|| endpoint_error_status(&error));
    let is_network = error.chain().any(|cause| cause.is::<reqwest::Error>())
        || adapter_classification
            .as_ref()
            .is_some_and(|value| value.is_network_error);
    let raw_message = format!("{error:#}");
    let lower = raw_message.to_ascii_lowercase();
    let provider_code = extract_provider_code(&raw_message);
    let mut class =
        classify_provider_failure_class(&lower, stage, status, provider_code.as_deref());
    if let Some(oversized) = error.downcast_ref::<crate::types::ProviderHttpErrorBodyTooLarge>() {
        class = classify_http_error_body_too_large(oversized.status).class;
    } else if error.is::<crate::types::ProviderResponseTooLarge>() {
        class = ProviderFailureClass::ProviderRejected;
    } else if class == ProviderFailureClass::Unknown && is_network {
        class = ProviderFailureClass::NetworkTransient;
    }
    let mut classification =
        adapter_classification.unwrap_or_else(|| ProviderFailureClassification::new(class));
    // Preserve the old custom-endpoint reqwest fallback even when the adapter
    // has already discarded the unsafe source and retained only transport facts.
    if classification.class == ProviderFailureClass::Unknown && classification.is_network_error {
        classification.class = ProviderFailureClass::NetworkTransient;
    }
    classification.http_status = classification.http_status.or(status);
    // `retry-after` is useful for recovery. Retain only the numeric interval.
    classification.retry_after_ms = classification
        .retry_after_ms
        .or_else(|| extract_retry_after_ms(&lower));
    // Provider-supplied codes may themselves contain the endpoint path.
    classification.provider_code =
        crate::failure::anthropic_stream_error(&error).and_then(|native| {
            (native != crate::failure::AnthropicStreamError::Unknown)
                .then(|| native.code().to_owned())
        });
    let message = if is_network {
        "provider network request failed"
    } else if status.is_some() {
        "provider HTTP request failed"
    } else {
        "provider request failed"
    };
    RedactedEndpointError {
        message,
        classification,
        incomplete: crate::failure::provider_stream_incomplete(&error),
        native: crate::failure::anthropic_stream_error(&error),
    }
    .into()
}

impl AuthorityBoundProvider {
    fn model_tool_calling_with_catalog(
        &self,
        model: &str,
        catalog: Option<&crate::catalog::ModelCatalog>,
    ) -> bool {
        let discovery = self
            .discovery_tools
            .read()
            .expect("discovery tools lock")
            .get(model)
            .copied();
        let source = self
            .use_public_catalog
            .then(|| catalog.and_then(|c| c.tool_support(self.name(), model)))
            .flatten();
        self.capabilities().tool_calling
            && crate::catalog::merge_tool_support(discovery, source) != Some(false)
    }

    fn enrich_discovery(
        &self,
        catalog: &crate::catalog::ModelCatalog,
        models: &mut [ProviderModelInfo],
    ) {
        // Keep raw discovery capability in this authority's existing instance;
        // enrichment must never export it to another credential/endpoint scope.
        *self.discovery_tools.write().expect("discovery tools lock") = models
            .iter()
            .filter_map(|m| m.capabilities.tool_calling.map(|v| (m.id.clone(), v)))
            .collect();
        *self
            .discovery_reasoning
            .write()
            .expect("discovery reasoning lock") = models
            .iter()
            .filter_map(|m| {
                m.capabilities
                    .reasoning
                    .as_ref()
                    .filter(|r| !r.native.is_empty())
                    .map(|r| (m.id.clone(), r.native.clone()))
            })
            .collect();
        catalog.enrich_for_tool_scope(self.inner.name(), models, self.use_public_catalog);
    }

    fn discovery_reasoning_snapshot(&self) -> BTreeMap<String, crate::generation::NativeReasoning> {
        self.discovery_reasoning
            .read()
            .expect("discovery reasoning lock")
            .clone()
    }

    fn discovery_tool_snapshot(&self) -> BTreeMap<String, bool> {
        self.discovery_tools
            .read()
            .expect("discovery tools lock")
            .clone()
    }

    fn ensure_not_revoked(&self) -> Result<()> {
        if self.revoked.load(Ordering::Acquire) {
            return Err(ProviderAuthorityRevoked.into());
        }
        // Check before catalog loading, budgeting and endpoint redaction so
        // retirement remains a local, non-secret diagnostic in every operation.
        if let Some(reason) = crate::definition::provider_definition(self.inner.name())
            .and_then(|definition| definition.retirement_reason())
        {
            anyhow::bail!(reason);
        }
        Ok(())
    }

    fn public_result<T>(&self, result: Result<T>) -> Result<T> {
        if self.redact_endpoint_errors {
            result.map_err(|error| {
                redacted_endpoint_error(self.inner.as_ref(), error, ProviderFailureStage::Connect)
            })
        } else {
            result
        }
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

    fn model_tool_calling(&self, model: &str) -> bool {
        let catalog = crate::catalog::model_catalog().ok();
        self.model_tool_calling_with_catalog(model, catalog.as_deref())
    }

    fn native_file_tool_capability(
        &self,
        model: &str,
    ) -> crate::file_tools::NativeFileToolCapability {
        self.inner.native_file_tool_capability(model)
    }

    fn classify_failure(&self, error: &anyhow::Error) -> Option<ProviderFailureClassification> {
        if let Some(redacted) = error.downcast_ref::<RedactedEndpointError>() {
            return Some(redacted.classification.clone());
        }
        self.inner.classify_failure(error)
    }

    async fn prepare_input_budget(
        &self,
        request: ChatRequest,
    ) -> Result<crate::attachments::PreparedInputBudget> {
        self.ensure_not_revoked()?;
        self.public_result(
            crate::attachments::runtime::with_async_authority_scope(
                self.authority_fingerprint.as_str().to_owned(),
                crate::generation::with_native_reasoning(
                    self.name(),
                    self.use_public_catalog,
                    self.discovery_reasoning_snapshot(),
                    self.inner.prepare_input_budget(request),
                ),
            )
            .await,
        )
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        self.ensure_not_revoked()?;
        self.public_result(
            crate::attachments::runtime::with_async_authority_scope(
                self.authority_fingerprint.as_str().to_owned(),
                crate::tools::policy::with_discovery_tools(
                    self.name(),
                    self.use_public_catalog,
                    self.discovery_tool_snapshot(),
                    crate::generation::with_native_reasoning(
                        self.name(),
                        self.use_public_catalog,
                        self.discovery_reasoning_snapshot(),
                        self.inner.chat(request),
                    ),
                ),
            )
            .await,
        )
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        Ok(self.stream_chat_with_diagnostics(request).await?.stream)
    }

    async fn stream_chat_with_diagnostics(
        &self,
        request: ChatRequest,
    ) -> Result<crate::ProviderStream> {
        self.ensure_not_revoked()?;
        let mut response = self.public_result(
            crate::attachments::runtime::with_async_authority_scope(
                self.authority_fingerprint.as_str().to_owned(),
                crate::tools::policy::with_discovery_tools(
                    self.name(),
                    self.use_public_catalog,
                    self.discovery_tool_snapshot(),
                    crate::generation::with_native_reasoning(
                        self.name(),
                        self.use_public_catalog,
                        self.discovery_reasoning_snapshot(),
                        self.inner.stream_chat_with_diagnostics(request),
                    ),
                ),
            )
            .await,
        )?;
        if self.redact_endpoint_errors {
            let inner = self.inner.clone();
            response.stream = Box::pin(response.stream.map(move |result| {
                result.map_err(|error| {
                    redacted_endpoint_error(inner.as_ref(), error, ProviderFailureStage::MidStream)
                })
            }));
        }
        Ok(response)
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        self.ensure_not_revoked()?;
        let catalog = crate::catalog::model_catalog()?;
        let mut models = self.public_result(self.inner.list_models().await)?;
        self.enrich_discovery(&catalog, &mut models);
        Ok(models)
    }

    async fn list_embedding_models(&self) -> Result<Vec<ProviderModelInfo>> {
        self.ensure_not_revoked()?;
        self.public_result(self.inner.list_embedding_models().await)
    }

    async fn list_transcription_models(&self) -> Result<Vec<ProviderModelInfo>> {
        self.ensure_not_revoked()?;
        self.public_result(self.inner.list_transcription_models().await)
    }

    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse> {
        self.ensure_not_revoked()?;
        self.public_result(self.inner.embed(request).await)
    }

    async fn warmup(&self) -> Result<ProviderWarmupOutcome> {
        self.ensure_not_revoked()?;
        self.public_result(self.inner.warmup().await)
    }
}

struct ProviderCacheEntry {
    // Keep the authority wrapper type; coercion at the public API boundary
    // preserves the same Arc lease/revocation semantics.
    provider: Arc<AuthorityBoundProvider>,
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
    base_url_resolver: Box<dyn Fn(Option<&str>, &str) -> Result<Option<String>> + Send + Sync>,
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

    pub fn new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
        key_resolver: impl Fn(Option<&str>, &str) -> Result<String> + Send + Sync + 'static,
        proxy_resolver: impl Fn(Option<&str>, &str) -> Result<Option<String>> + Send + Sync + 'static,
        base_url_resolver: impl Fn(Option<&str>, &str) -> Result<Option<String>> + Send + Sync + 'static,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self::new_scoped_fallible_with_timeout_policy_proxy_base_url_and_limits(
            key_resolver,
            proxy_resolver,
            base_url_resolver,
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
        Self::new_scoped_fallible_with_timeout_policy_proxy_base_url_and_limits(
            key_resolver,
            proxy_resolver,
            |_, _| Ok(None),
            timeout_policy,
            limits,
        )
    }

    pub fn new_scoped_fallible_with_timeout_policy_proxy_base_url_and_limits(
        key_resolver: impl Fn(Option<&str>, &str) -> Result<String> + Send + Sync + 'static,
        proxy_resolver: impl Fn(Option<&str>, &str) -> Result<Option<String>> + Send + Sync + 'static,
        base_url_resolver: impl Fn(Option<&str>, &str) -> Result<Option<String>> + Send + Sync + 'static,
        timeout_policy: ProviderTimeoutPolicy,
        limits: ProviderRegistryLimits,
    ) -> Self {
        Self {
            cache: RwLock::new(ProviderCacheState::default()),
            injected: RwLock::new(HashMap::new()),
            authority_scope_locks: std::array::from_fn(|_| Mutex::new(())),
            key_resolver: Box::new(key_resolver),
            proxy_resolver: Box::new(proxy_resolver),
            base_url_resolver: Box::new(base_url_resolver),
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
            .3;
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
        self.validate_candidate_workspace_authority_with_base_url(
            workspace_id,
            provider_name,
            api_key,
            proxy_url,
            None,
        )
    }

    pub fn validate_candidate_workspace_authority_with_base_url(
        &self,
        workspace_id: &str,
        provider_name: &str,
        api_key: Option<&str>,
        proxy_url: Option<&str>,
        base_url: Option<&str>,
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
            base_url,
        );
        create_provider_with_timeout_policy_and_proxy_and_base_url_and_authority(
            provider_name.as_str(),
            api_key.unwrap_or_default(),
            self.timeout_policy,
            proxy_url,
            base_url,
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
        let (api_key, proxy_url, base_url, authority_fingerprint) =
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
        let (provider, origin): (Arc<dyn Provider>, _) = match injected {
            Some(provider) => (provider, ProviderOrigin::Injected),
            None => (
                Arc::from(
                    create_provider_with_timeout_policy_and_proxy_and_base_url_and_authority(
                        provider_name.as_str(),
                        &api_key,
                        self.timeout_policy,
                        proxy_url.as_deref(),
                        base_url.as_deref(),
                        authority_fingerprint.as_str(),
                    )?,
                ),
                ProviderOrigin::Factory,
            ),
        };

        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        cache.prune_expired(now, self.limits.idle_ttl);
        let access_sequence = cache.next_sequence();
        let revoked = Arc::new(AtomicBool::new(false));
        let use_public_catalog = origin.use_public_catalog(provider.name(), base_url.as_deref());
        let provider = Arc::new(AuthorityBoundProvider {
            inner: provider,
            authority_fingerprint,
            revoked: revoked.clone(),
            redact_endpoint_errors: base_url.is_some(),
            use_public_catalog,
            discovery_tools: RwLock::new(BTreeMap::new()),
            discovery_reasoning: RwLock::new(BTreeMap::new()),
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
    ) -> Result<(
        String,
        Option<String>,
        Option<String>,
        ProviderAuthorityFingerprint,
    )> {
        let api_key = (self.key_resolver)(workspace_id, provider_name)?;
        let proxy_url = (self.proxy_resolver)(workspace_id, provider_name)?;
        let base_url = (self.base_url_resolver)(workspace_id, provider_name)?;
        let authority_fingerprint = Self::authority_fingerprint(
            workspace_id,
            provider_name,
            api_key.as_str(),
            proxy_url.as_deref(),
            base_url.as_deref(),
        );
        Ok((api_key, proxy_url, base_url, authority_fingerprint))
    }

    fn authority_fingerprint(
        workspace_id: Option<&str>,
        provider_name: &str,
        api_key: &str,
        proxy_url: Option<&str>,
        base_url: Option<&str>,
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
        digest.update([0]);
        digest.update(base_url.unwrap_or("<default>").as_bytes());
        crate::factory::hash_connection_environment(&mut digest, provider_name);
        ProviderAuthorityFingerprint(hex::encode(digest.finalize()))
    }

    pub fn insert(&self, name: impl Into<String>, provider: Arc<dyn Provider>) -> Result<()> {
        let name = normalize_provider_name(name.into().as_str());
        let _scopes = self.lock_all_authority_scopes();
        let (_, _, _, authority_fingerprint) = self.resolve_authority(None, name.as_str())?;
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
        let use_public_catalog = ProviderOrigin::Injected.use_public_catalog(provider.name(), None);
        let provider = Arc::new(AuthorityBoundProvider {
            inner: provider,
            authority_fingerprint: authority_fingerprint.clone(),
            revoked: revoked.clone(),
            redact_endpoint_errors: false,
            use_public_catalog,
            discovery_tools: RwLock::new(BTreeMap::new()),
            discovery_reasoning: RwLock::new(BTreeMap::new()),
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
    crate::definition::provider_definition(provider_name)
        .map(|definition| definition.name.to_owned())
        .unwrap_or_else(|| provider_name.trim().to_ascii_lowercase())
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

    #[tokio::test]
    async fn discovery_requires_loaded_catalog_but_direct_chat_does_not() {
        let registry = ProviderRegistry::with_provider("echo", Arc::new(EchoProvider::new()));
        let provider = registry.get_or_create("echo").unwrap();
        let error = provider.list_models().await.unwrap_err();
        assert!(error.to_string().contains("Model catalog is not loaded"));
        assert!(provider.chat(chat_request()).await.is_ok());
    }

    struct RouterReasoningFixture {
        discovery: String,
        catalog: Arc<crate::catalog::ModelCatalog>,
        bodies: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    }
    #[async_trait]
    impl Provider for RouterReasoningFixture {
        fn name(&self) -> &str {
            "openrouter"
        }
        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities::default()
        }
        async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
            Ok(crate::providers::openrouter::models_from_native_discovery_fixture(&self.discovery))
        }
        async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
            let body = crate::providers::openrouter::render_chat_request_mode_for_test(
                &self.catalog,
                &request,
                false,
            )
            .await?;
            self.bodies.lock().unwrap().push(body);
            Ok(ChatResponse {
                text: String::new(),
                usage: None,
                termination: ProviderTermination::Complete,
                reasoning_content: None,
                tool_calls: vec![],
                provider_replay_state: None,
            })
        }
        async fn stream_chat(
            &self,
            request: ChatRequest,
        ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
            let body = crate::providers::openrouter::render_chat_request_mode_for_test(
                &self.catalog,
                &request,
                true,
            )
            .await?;
            self.bodies.lock().unwrap().push(body);
            Ok(Box::pin(stream::empty()))
        }
    }

    #[tokio::test]
    async fn router_native_fields_survive_catalog_conflicts_and_actual_authority_requests() {
        use crate::{ReasoningConfig, ReasoningEffort};
        let id = "openai/gpt-5.4";
        // Each case has a distinct real authority instance/cache. These facts
        // enter through the production parser, never with_native_reasoning.
        for (metadata, expected, supported, mandatory, default, budget) in [
            (
                serde_json::json!({"supported_efforts":["low","high"], "default_effort":"high", "default_enabled":true,"mandatory":true,"supports_max_tokens":false}),
                vec!["low", "high"],
                Some(true),
                Some(true),
                Some("high"),
                Some(false),
            ),
            (
                serde_json::json!({"supported_efforts":["none","low","high"], "mandatory":false}),
                vec!["none", "low", "high"],
                Some(true),
                Some(false),
                None,
                None,
            ),
            (
                serde_json::json!({"supported_efforts":[], "default_enabled":false}),
                vec![],
                Some(true),
                Some(false),
                None,
                None,
            ),
            // An explicit aggregate negative remains independent of a positive enum.
            (
                serde_json::json!({"supported":false,"supported_efforts":["low","high"],"mandatory":true}),
                vec![],
                Some(false),
                Some(true),
                None,
                None,
            ),
            // Documented omission within the object denies effort selection,
            // but does not turn budget support or a default into effort support.
            (
                serde_json::json!({"default_effort":"high","supports_max_tokens":true}),
                vec![],
                Some(true),
                Some(false),
                Some("high"),
                Some(true),
            ),
            (
                serde_json::json!({"supported_efforts":null,"default_effort":"none","default_enabled":false,"mandatory":false}),
                vec!["none", "minimal", "low", "medium", "high", "xhigh", "max"],
                Some(true),
                Some(false),
                Some("none"),
                None,
            ),
            // Absent or malformed enum carries no confirmed per-model enum;
            // matching catalog fallback remains visible as mixed/unknown source.
            (
                serde_json::Value::Null,
                vec!["none", "minimal", "low", "medium", "high", "xhigh", "max"],
                Some(true),
                Some(false),
                None,
                None,
            ),
            (
                serde_json::json!({"supported_efforts":[null],"mandatory":null,"supports_max_tokens":null}),
                vec!["none", "minimal", "low", "medium", "high", "xhigh", "max"],
                Some(true),
                Some(false),
                None,
                None,
            ),
        ] {
            let mut catalog_metadata = serde_json::json!({"thinkingLevelMap":{"off":"none","medium":"high","xhigh":"xhigh","max":"max","minimal":"minimal","low":"low","high":"high"}});
            if metadata["mandatory"] == true && metadata.get("supported").is_none() {
                catalog_metadata["reasoning"] = serde_json::json!(false);
                catalog_metadata["compat"] = serde_json::json!({"supportsReasoningEffort":false});
            }
            let catalog = Arc::new(crate::generation::test_catalog_model(
                "openrouter",
                id,
                id,
                catalog_metadata,
            ));
            let bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
            let wrapper = AuthorityBoundProvider {
                inner: Arc::new(RouterReasoningFixture {
                    discovery: serde_json::json!({"data":[{"id":id,"reasoning":metadata}]})
                        .to_string(),
                    catalog: catalog.clone(),
                    bodies: bodies.clone(),
                }),
                authority_fingerprint: ProviderAuthorityFingerprint(
                    "router-reasoning-fixture".into(),
                ),
                revoked: Arc::new(AtomicBool::new(false)),
                redact_endpoint_errors: false,
                use_public_catalog: true,
                discovery_tools: RwLock::new(BTreeMap::new()),
                discovery_reasoning: RwLock::new(BTreeMap::new()),
            };
            let mut models = wrapper.inner.list_models().await.unwrap();
            let raw_native = models[0]
                .capabilities
                .reasoning
                .as_ref()
                .map(|r| r.native.clone())
                .unwrap_or_default();
            wrapper.enrich_discovery(&catalog, &mut models);
            let r = models[0].capabilities.reasoning.as_ref().unwrap();
            assert_eq!(r.native, raw_native);
            for (field, fact) in [
                ("mandatory", "mandatory"),
                ("default_enabled", "default_enabled"),
                ("supports_max_tokens", "supports_token_budget"),
                ("supported", "reasoning.supported"),
            ] {
                assert_eq!(
                    raw_native.get(fact).copied(),
                    metadata.get(field).map(serde_json::Value::as_bool)
                );
            }

            assert_eq!(
                models[0].capabilities.thinking,
                Some(!(metadata["mandatory"] == true && metadata.get("supported").is_none()))
            );
            assert_eq!(r.effort_options, expected);
            assert_eq!(r.supported, supported);
            assert_eq!(r.mandatory, mandatory);
            assert_eq!(r.default_effort.as_deref(), default);
            assert_eq!(r.supports_token_budget, budget);
            let mut request = crate::generation::test_request(id);
            for setting in [
                None,
                Some(ReasoningConfig::Disabled),
                Some(ReasoningConfig::Effort(ReasoningEffort::None)),
                Some(ReasoningConfig::Effort(ReasoningEffort::Minimal)),
                Some(ReasoningConfig::Effort(ReasoningEffort::Low)),
                Some(ReasoningConfig::Effort(ReasoningEffort::Medium)),
                Some(ReasoningConfig::Effort(ReasoningEffort::High)),
                Some(ReasoningConfig::Effort(ReasoningEffort::XHigh)),
                Some(ReasoningConfig::Effort(ReasoningEffort::Max)),
            ] {
                request.reasoning = setting;
                let effort = setting.map(|s| match s {
                    ReasoningConfig::Disabled => "none",
                    ReasoningConfig::Effort(e) => e.as_str(),
                });
                let allowed = effort.is_none_or(|e| expected.contains(&e));
                for stream in [false, true] {
                    let result = if stream {
                        wrapper.stream_chat(request.clone()).await.map(|_| ())
                    } else {
                        wrapper.chat(request.clone()).await.map(|_| ())
                    };
                    if allowed {
                        result.unwrap();
                        let body = bodies.lock().unwrap().last().unwrap().clone();
                        assert_eq!(body["stream"], stream);
                        assert_eq!(body["max_tokens"], 1024);
                        if let Some(effort) = effort {
                            let expected_wire = if effort == "medium"
                                && raw_native.get("effort.medium") != Some(&Some(true))
                            {
                                "high"
                            } else {
                                effort
                            };
                            assert_eq!(body["reasoning"]["effort"], expected_wire);
                        } else {
                            assert!(body.get("reasoning").is_none());
                        }
                    } else {
                        let error = result.unwrap_err().to_string();
                        assert!(error.contains("native OpenRouter"), "{error}");
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn router_budget_support_is_independent_of_effort_selection_and_catalog() {
        use crate::{ReasoningConfig, ReasoningEffort};
        let id = "openai/gpt-5.4";
        for (enum_case, enum_value) in [
            ("empty", serde_json::json!([])),
            ("omitted", serde_json::Value::Null),
            ("gateway", serde_json::Value::Null),
            ("malformed", serde_json::json!([null])),
        ] {
            for budget in [
                None,
                Some(serde_json::Value::Null),
                Some(serde_json::json!(false)),
                Some(serde_json::json!(true)),
            ] {
                for catalog_support in [false, true] {
                    for aggregate_negative in [false, true] {
                        let mut metadata = serde_json::json!({"default_enabled":true,"default_effort":"high","mandatory":true});
                        if enum_case != "omitted" {
                            metadata["supported_efforts"] = enum_value.clone();
                        }
                        if let Some(budget) = &budget {
                            metadata["supports_max_tokens"] = budget.clone();
                        }
                        if aggregate_negative {
                            metadata["supported"] = serde_json::json!(false);
                        }
                        let catalog = Arc::new(crate::generation::test_catalog_model(
                            "openrouter",
                            id,
                            id,
                            serde_json::json!({"reasoning":catalog_support,"thinkingLevelMap":{"off":"none","low":"low","high":"high"}}),
                        ));
                        let bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
                        let authority = AuthorityBoundProvider {
                            inner: Arc::new(RouterReasoningFixture {
                                discovery:
                                    serde_json::json!({"data":[{"id":id,"reasoning":metadata}]})
                                        .to_string(),
                                catalog: catalog.clone(),
                                bodies: bodies.clone(),
                            }),
                            authority_fingerprint: ProviderAuthorityFingerprint(
                                "router-budget-fixture".into(),
                            ),
                            revoked: Arc::new(AtomicBool::new(false)),
                            redact_endpoint_errors: false,
                            use_public_catalog: true,
                            discovery_tools: RwLock::new(BTreeMap::new()),
                            discovery_reasoning: RwLock::new(BTreeMap::new()),
                        };
                        let mut models = authority.inner.list_models().await.unwrap();
                        let raw = models[0].capabilities.reasoning.as_ref().unwrap();
                        let positive_budget =
                            budget.as_ref().and_then(serde_json::Value::as_bool) == Some(true);
                        let native_support = if aggregate_negative {
                            Some(false)
                        } else if positive_budget || enum_case == "gateway" {
                            Some(true)
                        } else {
                            None
                        };
                        // Empty/omitted enums and default-on/mandatory by themselves
                        // are not an aggregate negative (or an invented positive).
                        assert_eq!(raw.supported, native_support);
                        assert_eq!(
                            raw.supports_token_budget,
                            budget.as_ref().and_then(serde_json::Value::as_bool)
                        );
                        let native = raw.native.clone();
                        authority.enrich_discovery(&catalog, &mut models);
                        let effective = models[0].capabilities.reasoning.as_ref().unwrap();
                        assert_eq!(effective.native, native);
                        assert_eq!(
                            effective.supported,
                            native_support.or(Some(catalog_support))
                        );
                        assert_eq!(models[0].capabilities.thinking, Some(catalog_support));
                        assert_eq!(
                            effective.supports_token_budget,
                            budget.as_ref().and_then(serde_json::Value::as_bool)
                        );
                        assert_eq!(effective.mandatory, Some(true));
                        assert_eq!(effective.default_effort.as_deref(), Some("high"));
                        assert_eq!(native.get("default_enabled"), Some(&Some(true)));
                        assert_eq!(
                            native.get("supports_token_budget").copied(),
                            budget.as_ref().map(serde_json::Value::as_bool)
                        );
                        let effort_allowed = !aggregate_negative
                            && (enum_case == "gateway"
                                || enum_case == "malformed" && catalog_support);
                        assert_eq!(
                            effective.effort_options.iter().any(|e| e == "high"),
                            effort_allowed
                        );
                        if matches!(enum_case, "empty" | "omitted") || aggregate_negative {
                            assert!(effective.effort_options.is_empty());
                        }
                        for setting in [
                            None,
                            Some(ReasoningConfig::Disabled),
                            Some(ReasoningConfig::Effort(ReasoningEffort::None)),
                            Some(ReasoningConfig::Effort(ReasoningEffort::High)),
                        ] {
                            let mut request = crate::generation::test_request(id);
                            request.reasoning = setting;
                            for stream in [false, true] {
                                let result = if stream {
                                    authority.stream_chat(request.clone()).await.map(|_| ())
                                } else {
                                    authority.chat(request.clone()).await.map(|_| ())
                                };
                                if setting.is_none()
                                    || setting
                                        == Some(ReasoningConfig::Effort(ReasoningEffort::High))
                                        && effort_allowed
                                {
                                    result.unwrap();
                                    let body = bodies.lock().unwrap().last().unwrap().clone();
                                    assert_eq!(body["stream"], stream);
                                    assert_eq!(body["max_tokens"], 1024);
                                    if setting.is_none() {
                                        assert!(body.get("reasoning").is_none());
                                    } else {
                                        assert_eq!(body["reasoning"]["effort"], "high");
                                    }
                                } else {
                                    let error = result.unwrap_err().to_string();
                                    assert!(
                                        error.contains("native OpenRouter")
                                            || error.contains(
                                                "selected catalog model does not support reasoning"
                                            ),
                                        "{error}"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    struct ToolDiscoveryProvider;
    #[async_trait]
    impl Provider for ToolDiscoveryProvider {
        fn name(&self) -> &str {
            "openai"
        }
        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities {
                tool_calling: true,
                ..Default::default()
            }
        }
        async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
            Ok(vec![crate::catalog::tool_tests::discovered(
                "g03-positive",
                Some(false),
            )])
        }
        async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
            crate::tools::policy::prepare_request(self.name(), request)?;
            anyhow::bail!("fixture reached model boundary")
        }
        async fn stream_chat(
            &self,
            request: ChatRequest,
        ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
            crate::tools::policy::prepare_request(self.name(), request)?;
            anyhow::bail!("fixture reached model boundary")
        }
    }

    #[tokio::test]
    async fn discovered_false_reaches_agent_consumer_and_both_authority_request_paths() {
        let catalog = crate::catalog::tool_tests::generated_catalog();
        let wrapper = |authority: &str| AuthorityBoundProvider {
            inner: Arc::new(ToolDiscoveryProvider),
            authority_fingerprint: ProviderAuthorityFingerprint(authority.into()),
            revoked: Arc::new(AtomicBool::new(false)),
            redact_endpoint_errors: false,
            use_public_catalog: false,
            discovery_tools: RwLock::new(BTreeMap::new()),
            discovery_reasoning: RwLock::new(BTreeMap::new()),
        };
        let mut public = wrapper("public-authority");
        public.use_public_catalog = true;
        // Same method as the actual agent's Provider::model_tool_calling,
        // with an isolated source-generated snapshot rather than global state.
        assert!(!public.model_tool_calling_with_catalog("g03-negative", Some(&catalog)));
        assert!(public.model_tool_calling_with_catalog("g03-positive", Some(&catalog)));
        assert!(public.model_tool_calling_with_catalog("unknown", Some(&catalog)));
        let a = wrapper("authority-a");
        let b = wrapper("authority-b");
        let mut models = a.inner.list_models().await.unwrap();
        a.enrich_discovery(&catalog, &mut models);
        assert_eq!(models[0].capabilities.tool_calling, Some(false));
        assert!(!a.model_tool_calling("g03-positive"));
        assert!(b.model_tool_calling("g03-positive"));
        let mut request = crate::tools::policy::test_request();
        request.model = "g03-positive".into();
        assert!(
            a.chat(request.clone())
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("does not support tool definitions")
        );
        assert!(
            a.stream_chat(request.clone())
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("does not support tool definitions")
        );
        assert!(
            b.chat(request.clone())
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("fixture reached model boundary")
        );
        request.tool_choice = Some(crate::ToolChoice::None);
        assert!(
            a.chat(request)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("does not support tool definitions")
        );
        // A fresh discovery listing atomically replaces previous known values.
        a.enrich_discovery(
            &catalog,
            &mut [crate::catalog::tool_tests::discovered(
                "g03-positive",
                Some(true),
            )],
        );
        assert!(a.model_tool_calling("g03-positive"));
    }

    // Native response data, not hand-built ProviderModelInfo capability bools.
    const NATIVE_TOOL_MODELS: &str = r#"{"data":[
        {"id":"g03-positive","supported_parameters":[]},
        {"id":"negative-no-source","supported_parameters":["temperature"]},
        {"id":"native-positive","supported_parameters":["tools"]},
        {"id":"g03-negative","supported_parameters":["tools"]},
        {"id":"missing"}, {"id":"null","supported_parameters":null},
        {"id":"malformed","supported_parameters":["tools",42]}
    ]}"#;

    struct NativeModelsProvider {
        name: &'static str,
        catalog: Arc<crate::catalog::ModelCatalog>,
    }
    #[async_trait]
    impl Provider for NativeModelsProvider {
        fn name(&self) -> &str {
            self.name
        }
        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities {
                tool_calling: true,
                ..Default::default()
            }
        }
        async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
            Ok(
                crate::providers::openrouter::models_from_native_discovery_fixture(
                    NATIVE_TOOL_MODELS,
                ),
            )
        }
        async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
            crate::tools::policy::prepare_request_with_catalog(
                self.name(),
                request,
                Some(&self.catalog),
            )?;
            Ok(ChatResponse {
                text: "local boundary".into(),
                usage: None,
                reasoning_content: None,
                tool_calls: vec![],
                provider_replay_state: None,
                termination: ProviderTermination::Complete,
            })
        }
        async fn stream_chat(
            &self,
            request: ChatRequest,
        ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
            self.chat(request).await?;
            Ok(Box::pin(stream::empty()))
        }
    }

    fn scoped_registry(endpoint: Option<&str>) -> ProviderRegistry {
        let endpoint = endpoint.map(str::to_owned);
        ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
            |_, _| Ok(String::new()),
            |_, _| Ok(None),
            move |_, _| Ok(endpoint.clone()),
            ProviderTimeoutPolicy::default(),
        )
    }

    // Inspect the actual cached wrapper, without extra production metadata or
    // changing its flag. Cache stores the same single Arc used by public leases.
    fn cached_wrapper(
        registry: &ProviderRegistry,
        workspace: Option<&str>,
        name: &str,
    ) -> Arc<AuthorityBoundProvider> {
        let name = normalize_provider_name(name);
        registry
            .cache
            .read()
            .unwrap()
            .entries
            .iter()
            .find(|(key, _)| key.workspace_id.as_deref() == workspace && key.provider_name == name)
            .unwrap()
            .1
            .provider
            .clone()
    }

    async fn wrapper_support(
        wrapper: &AuthorityBoundProvider,
        model: &str,
        catalog: &crate::catalog::ModelCatalog,
    ) -> Option<bool> {
        crate::tools::policy::with_discovery_tools(
            wrapper.name(),
            wrapper.use_public_catalog,
            wrapper.discovery_tool_snapshot(),
            async {
                crate::tools::policy::tool_support_with_catalog(
                    wrapper.name(),
                    model,
                    Some(catalog),
                )
            },
        )
        .await
    }

    #[tokio::test]
    async fn native_openrouter_discovery_reaches_scoped_consumers_and_both_preflights() {
        let catalog = Arc::new(crate::catalog::tool_tests::generated_catalog());
        for endpoint in [
            None,
            Some("https://openrouter.ai/api/v1"),
            Some("https://private.invalid/api/v1"),
        ] {
            let registry = scoped_registry(endpoint);
            // Factory path determines real stock/override scope; no HTTP.
            let actual = registry.get_or_create("openrouter").unwrap();
            let wrapper = cached_wrapper(&registry, None, "openrouter");
            assert_eq!(
                wrapper.use_public_catalog,
                endpoint != Some("https://private.invalid/api/v1")
            );
            let mut models = crate::providers::openrouter::models_from_native_discovery_fixture(
                NATIVE_TOOL_MODELS,
            );
            wrapper.enrich_discovery(&catalog, &mut models);
            for id in ["g03-positive", "negative-no-source"] {
                assert_eq!(wrapper_support(&wrapper, id, &catalog).await, Some(false));
                assert!(!actual.model_tool_calling(id));
                assert!(!wrapper.model_tool_calling_with_catalog(id, Some(&catalog)));
                for choice in [None, Some(crate::ToolChoice::None)] {
                    let mut request = crate::tools::policy::test_request();
                    request.model = id.into();
                    request.tool_choice = choice;
                    // Actual OpenRouter adapter entrypoints reject before HTTP,
                    // including disabled new calls with definitions supplied.
                    assert!(actual.chat(request.clone()).await.is_err());
                    assert!(actual.stream_chat(request.clone()).await.is_err());
                    assert!(actual.stream_chat_with_diagnostics(request).await.is_err());
                }
            }
            for id in [
                "native-positive",
                "missing",
                "null",
                "malformed",
                "custom-unknown",
            ] {
                assert!(wrapper.model_tool_calling_with_catalog(id, Some(&catalog)));
            }
            // A public source negative is still a veto against discovery true;
            // a foreign endpoint never inherits that same source restriction.
            assert_eq!(
                wrapper_support(&wrapper, "g03-negative", &catalog).await,
                Some(!wrapper.use_public_catalog)
            );
            let other = registry
                .get_or_create_for_workspace("other", "openrouter")
                .unwrap();
            assert!(other.model_tool_calling("negative-no-source"));

            // Native-normalized fixture adapter permits the allowed-path cases
            // without HTTP/preparation/runtime; registry constructors stay real.
            let isolated = scoped_registry(endpoint);
            isolated
                .insert(
                    "openrouter",
                    Arc::new(NativeModelsProvider {
                        name: "openrouter",
                        catalog: catalog.clone(),
                    }),
                )
                .unwrap();
            let provider = isolated
                .get_or_create_for_workspace("a", "openrouter")
                .unwrap();
            let fixture = cached_wrapper(&isolated, Some("a"), "openrouter");
            let mut models = fixture.inner.list_models().await.unwrap();
            fixture.enrich_discovery(&catalog, &mut models);
            for id in [
                "g03-positive",
                "negative-no-source",
                "native-positive",
                "missing",
                "null",
                "malformed",
                "custom-unknown",
            ] {
                for disabled in [false, true] {
                    let mut request = crate::tools::policy::test_request();
                    request.model = id.into();
                    if disabled {
                        request.tool_choice = Some(crate::ToolChoice::None);
                    }
                    let allowed = !matches!(id, "g03-positive" | "negative-no-source");
                    assert_eq!(provider.chat(request.clone()).await.is_ok(), allowed);
                    assert_eq!(provider.stream_chat(request.clone()).await.is_ok(), allowed);
                    request.tools = None;
                    request.tool_choice = Some(crate::ToolChoice::None);
                    assert!(provider.chat(request.clone()).await.is_ok());
                    assert!(provider.stream_chat(request).await.is_ok());
                }
            }
        }
    }

    fn catalog_with_restricted_parallel() -> Arc<crate::catalog::ModelCatalog> {
        let mut generated = crate::catalog::generator::generate(
            &crate::catalog::tool_tests::source_snapshot(),
            false,
        )
        .unwrap();
        // Isolated protocol metadata, not a global catalog publication. Source
        // bools still come from generation of the real source fixtures.
        generated
            .models
            .get_mut("openai")
            .unwrap()
            .get_mut("g03-positive")
            .unwrap()["compat"]["supportsParallelToolCalls"] = serde_json::json!(false);
        Arc::new(
            crate::catalog::ModelCatalog::parse_with_capabilities(
                &serde_json::to_string(&generated.models).unwrap(),
                &serde_json::to_string(&generated.provenance).unwrap(),
                generated.tool_capabilities,
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn injected_origin_stays_isolated_on_insert_workspace_and_global_reconstruction() {
        let catalog = catalog_with_restricted_parallel();
        for endpoint in [
            None,
            Some("https://api.openai.com/v1"),
            Some("https://private.invalid/v1"),
        ] {
            let registry = scoped_registry(endpoint);
            registry
                .insert(
                    " OPENAI ",
                    Arc::new(NativeModelsProvider {
                        name: "openai",
                        catalog: catalog.clone(),
                    }),
                )
                .unwrap();
            for path in 0..3 {
                let (workspace, provider) = match path {
                    0 => (None, registry.get_or_create("openai").unwrap()),
                    1 => (
                        Some("workspace-a"),
                        registry
                            .get_or_create_for_workspace("workspace-a", "openai")
                            .unwrap(),
                    ),
                    _ => {
                        registry.invalidate_global_provider("openai");
                        (None, registry.get_or_create("openai").unwrap())
                    }
                };
                let wrapper = cached_wrapper(&registry, workspace, "openai");
                assert!(
                    !wrapper.use_public_catalog,
                    "path {path}, endpoint {endpoint:?}"
                );
                // Public negative, positive and compat controls all stay out.
                for id in ["g03-positive", "g03-negative"] {
                    assert_eq!(wrapper_support(&wrapper, id, &catalog).await, None);
                    assert!(wrapper.model_tool_calling_with_catalog(id, Some(&catalog)));
                    let mut request = crate::tools::policy::test_request();
                    request.model = id.into();
                    request.parallel_tool_calls = Some(false);
                    assert!(provider.chat(request.clone()).await.is_ok());
                    assert!(provider.stream_chat(request).await.is_ok());
                }
                let mut models = wrapper.inner.list_models().await.unwrap();
                wrapper.enrich_discovery(&catalog, &mut models);
                assert_eq!(
                    wrapper_support(&wrapper, "g03-positive", &catalog).await,
                    Some(false)
                );
                assert!(!provider.model_tool_calling("g03-positive"));
                let mut request = crate::tools::policy::test_request();
                request.model = "g03-positive".into();
                assert!(provider.chat(request.clone()).await.is_err());
                assert!(provider.stream_chat(request).await.is_err());
                // Native positive does not inherit public negative.
                assert_eq!(
                    wrapper_support(&wrapper, "g03-negative", &catalog).await,
                    Some(true)
                );
                assert!(provider.model_tool_calling("g03-negative"));
                let mut positive = crate::tools::policy::test_request();
                positive.model = "g03-negative".into();
                positive.parallel_tool_calls = Some(false);
                assert!(provider.chat(positive.clone()).await.is_ok());
                assert!(provider.stream_chat(positive).await.is_ok());
                let other = registry
                    .get_or_create_for_workspace(&format!("other-{path}"), "openai")
                    .unwrap();
                let other_wrapper =
                    cached_wrapper(&registry, Some(&format!("other-{path}")), "openai");
                assert_eq!(
                    wrapper_support(&other_wrapper, "g03-positive", &catalog).await,
                    None
                );
                assert!(other.model_tool_calling("g03-positive"));
            }
        }
    }

    #[tokio::test]
    async fn factory_origin_retains_only_stock_source_and_control_scope_after_reconstruction() {
        let catalog = catalog_with_restricted_parallel();
        for endpoint in [
            None,
            Some("https://api.openai.com/v1/"),
            Some("https://private.invalid/v1"),
        ] {
            let registry = scoped_registry(endpoint);
            let public = endpoint != Some("https://private.invalid/v1");
            for path in 0..3 {
                let workspace = (path == 1).then_some("workspace-a");
                if path == 2 {
                    registry.invalidate_global_provider("openai");
                }
                let _lease = match workspace {
                    Some(ws) => registry.get_or_create_for_workspace(ws, "openai").unwrap(),
                    None => registry.get_or_create("openai").unwrap(),
                };
                let wrapper = cached_wrapper(&registry, workspace, "openai");
                assert_eq!(wrapper.use_public_catalog, public);
                assert_eq!(
                    wrapper_support(&wrapper, "g03-negative", &catalog).await,
                    public.then_some(false)
                );
                assert_eq!(
                    wrapper_support(&wrapper, "g03-positive", &catalog).await,
                    public.then_some(true)
                );
                assert_eq!(
                    wrapper.model_tool_calling_with_catalog("g03-negative", Some(&catalog)),
                    !public
                );
                let mut request = crate::tools::policy::test_request();
                request.model = "g03-positive".into();
                request.parallel_tool_calls = Some(false);
                let result = crate::tools::policy::with_discovery_tools(
                    wrapper.name(),
                    wrapper.use_public_catalog,
                    wrapper.discovery_tool_snapshot(),
                    async {
                        crate::tools::policy::prepare_request_with_catalog(
                            wrapper.name(),
                            request,
                            Some(&catalog),
                        )
                    },
                )
                .await;
                assert_eq!(result.is_err(), public); // explicit public compat restriction
            }
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
    fn glm_profiles_and_aliases_keep_native_file_tools_after_factory_and_registry() {
        let registry = ProviderRegistry::new(|_| "dummy-key".to_owned());
        for alias in [
            "glm",
            "zhipu",
            "bigmodel",
            "glm-cn",
            "zhipu-cn",
            "zai",
            "glm-global",
            "zhipu-global",
            "z.ai",
            "z-ai",
            "glm-coding",
            "glm-coding-cn",
            "zhipu-coding",
            "zai-coding-cn",
            "zai-coding",
            "glm-coding-global",
            "zai-coding-plan",
        ] {
            let canonical = crate::definition::provider_definition(alias).unwrap().name;
            let direct = crate::factory::create_provider(alias, "dummy-key").unwrap();
            let scoped = registry
                .get_or_create_for_workspace("fixture", alias)
                .unwrap();
            for provider in [direct.as_ref(), scoped.as_ref()] {
                assert_eq!(provider.name(), canonical, "{alias}");
                assert!(provider.capabilities().tool_calling, "{alias}");
                let capability = provider.native_file_tool_capability("glm-5.2");
                assert_eq!(capability.provider, canonical, "{alias}");
                assert_eq!(capability.model, "glm-5.2");
                assert_eq!(
                    capability.patch_shape,
                    crate::NativePatchWireShape::JsonFunction
                );
                assert!(capability.read_file && capability.apply_patch, "{alias}");
                assert!(capability.is_supported());
                let schema = crate::apply_patch_tool_schema(capability.patch_shape);
                assert_eq!(schema["type"], "object");
                assert_eq!(schema["required"], serde_json::json!(["patch"]));
                assert_eq!(schema["properties"]["patch"]["type"], "string");
                assert_eq!(schema["additionalProperties"], false);
                for missing in ["", " ", "unknown", "unsupported"] {
                    let unavailable = provider.native_file_tool_capability(missing);
                    assert_eq!(unavailable.provider, canonical);
                    assert_eq!(
                        unavailable.patch_shape,
                        crate::NativePatchWireShape::Unavailable
                    );
                    assert!(!unavailable.read_file && !unavailable.apply_patch);
                }
            }
            assert!(scoped.authority_fingerprint().is_some());
        }
        assert!(crate::factory::create_provider("unknown-glm-profile", "dummy-key").is_err());
        let unknown = crate::select_native_file_tool_capability("unknown-glm-profile", "glm-5.2");
        assert!(!unknown.read_file && !unknown.apply_patch);
    }

    #[test]
    fn glm_global_and_coding_profiles_resolve_separate_credential_authorities() {
        let names = Arc::new(Mutex::new(Vec::new()));
        let captured = names.clone();
        let registry = ProviderRegistry::new_scoped(move |_, provider| {
            captured.lock().unwrap().push(provider.to_owned());
            format!("dummy-{provider}-key")
        });
        let cn = registry
            .get_or_create_for_workspace("fixture", "bigmodel")
            .unwrap();
        let global = registry
            .get_or_create_for_workspace("fixture", "glm-global")
            .unwrap();
        let cn_coding = registry
            .get_or_create_for_workspace("fixture", "zai-coding-cn")
            .unwrap();
        let global_coding = registry
            .get_or_create_for_workspace("fixture", "zai-coding-plan")
            .unwrap();
        assert_eq!(
            *names.lock().unwrap(),
            ["glm", "zai", "glm-coding", "zai-coding"]
        );
        for (provider, name) in [
            (cn, "glm"),
            (global, "zai"),
            (cn_coding, "glm-coding"),
            (global_coding, "zai-coding"),
        ] {
            assert_eq!(provider.name(), name);
        }
    }

    #[test]
    fn get_or_create_unknown_provider_errors() {
        let registry = ProviderRegistry::new(|_| String::new());
        let result = registry.get_or_create("nonexistent_provider_xyz");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn retired_profiles_never_discover_warmup_or_chat_even_with_override() {
        let registry = ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
            |_, _| Ok("dummy-key".to_owned()),
            |_, _| Ok(None),
            |_, _| Ok(Some("https://example.test/retired/v1".to_owned())),
            ProviderTimeoutPolicy::default(),
        );
        for alias in ["yi", "01ai", "lingyiwanwu", "hyperbolic"] {
            let provider = registry
                .get_or_create_for_workspace("fixture-workspace", alias)
                .unwrap();
            assert!(!provider.capabilities().streaming);
            for error in [
                provider.list_models().await.unwrap_err(),
                provider.warmup().await.unwrap_err(),
                provider.chat(chat_request()).await.unwrap_err(),
            ] {
                assert!(error.to_string().contains("retired"));
                assert!(!error.to_string().contains("example.test"));
                assert!(!error.to_string().contains("dummy-key"));
            }
            let error = match provider.stream_chat(chat_request()).await {
                Err(error) => error,
                Ok(_) => panic!("retired stream must fail"),
            };
            assert!(error.to_string().contains("retired"));
        }
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
        let mut media_request = chat_request();
        media_request.messages[0]
            .content_parts
            .push(crate::MessageContentPart::image(
                crate::MessageAttachment::from_path("/missing-revoked-media.png", "image/png"),
            ));
        let budget_error = workspace_a
            .prepare_input_budget(media_request)
            .await
            .err()
            .expect("revocation must precede materialization and filesystem access");
        assert!(
            budget_error
                .downcast_ref::<ProviderAuthorityRevoked>()
                .is_some()
        );

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

    #[tokio::test]
    async fn cache_identity_incorporates_custom_base_url() {
        for name in ["openai", "ollama"] {
            let endpoint = Arc::new(Mutex::new(None::<String>));
            let registry =
                ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
                    |_, _| Ok("sk-key".to_owned()),
                    |_, _| Ok(None),
                    {
                        let endpoint = endpoint.clone();
                        move |workspace, _| {
                            Ok(if workspace == Some("ws_a") {
                                endpoint.lock().expect("endpoint lock").clone()
                            } else {
                                None
                            })
                        }
                    },
                    ProviderTimeoutPolicy::default(),
                );
            let old_a = registry.get_or_create_for_workspace("ws_a", name).unwrap();
            let b = registry.get_or_create_for_workspace("ws_b", name).unwrap();
            let fp_default = registry
                .authority_fingerprint_for_workspace("ws_a", name)
                .expect("default fingerprint");
            *endpoint.lock().expect("endpoint lock") = Some("https://custom.api.com/v1".to_owned());
            let fp_custom = registry
                .authority_fingerprint_for_workspace("ws_a", name)
                .expect("custom fingerprint");
            assert_ne!(fp_default, fp_custom);
            let revoked = old_a
                .chat(chat_request())
                .await
                .expect_err("old authority revoked");
            assert!(revoked.downcast_ref::<ProviderAuthorityRevoked>().is_some());
            assert!(Arc::ptr_eq(
                &b,
                &registry.get_or_create_for_workspace("ws_b", name).unwrap()
            ));
            let new_a = registry.get_or_create_for_workspace("ws_a", name).unwrap();
            assert_eq!(new_a.authority_fingerprint(), Some(fp_custom.as_str()));
            *endpoint.lock().expect("endpoint lock") = None;
            let fp_reset = registry
                .authority_fingerprint_for_workspace("ws_a", name)
                .expect("reset fingerprint");
            assert_eq!(
                fp_reset, fp_default,
                "{name} reset must select default authority"
            );
            let reset_a = registry.get_or_create_for_workspace("ws_a", name).unwrap();
            assert_eq!(reset_a.authority_fingerprint(), Some(fp_default.as_str()));
            let revoked = new_a
                .chat(chat_request())
                .await
                .expect_err("updated authority revoked");
            assert!(revoked.downcast_ref::<ProviderAuthorityRevoked>().is_some());
            assert!(Arc::ptr_eq(
                &b,
                &registry.get_or_create_for_workspace("ws_b", name).unwrap()
            ));
            assert_eq!(registry.invalidate_workspace_provider("ws_a", name), 1);
            assert!(Arc::ptr_eq(
                &b,
                &registry.get_or_create_for_workspace("ws_b", name).unwrap()
            ));
        }
    }

    #[tokio::test]
    async fn workspace_base_urls_route_real_requests_to_their_own_servers() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        async fn server(body: &'static str) -> (String, tokio::task::JoinHandle<String>) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/gateway/v1/", listener.local_addr().unwrap());
            let task = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = vec![0u8; 8192];
                let count = stream.read(&mut bytes).await.unwrap();
                let line = String::from_utf8_lossy(&bytes[..count])
                    .lines()
                    .next()
                    .unwrap()
                    .to_owned();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                line
            });
            (url, task)
        }

        for (name, body, expected_path) in [
            (
                "vllm",
                r#"{"choices":[{"message":{"content":"fixture reply"}}]}"#,
                "POST /gateway/v1/chat/completions HTTP/1.1",
            ),
            (
                "ollama",
                r#"{"message":{"content":"fixture reply"},"done":true}"#,
                "POST /gateway/v1/api/chat HTTP/1.1",
            ),
        ] {
            let (url_a, server_a) = server(body).await;
            let (url_b, server_b) = server(body).await;
            let registry =
                ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
                    |_, _| Ok(String::new()),
                    |_, _| Ok(None),
                    move |workspace, _| {
                        Ok(Some(if workspace == Some("ws_a") {
                            url_a.clone()
                        } else {
                            url_b.clone()
                        }))
                    },
                    ProviderTimeoutPolicy::default(),
                );
            let a = registry.get_or_create_for_workspace("ws_a", name).unwrap();
            let b = registry.get_or_create_for_workspace("ws_b", name).unwrap();
            assert_eq!(a.chat(chat_request()).await.unwrap().text, "fixture reply");
            assert_eq!(b.chat(chat_request()).await.unwrap().text, "fixture reply");
            assert_eq!(server_a.await.unwrap(), expected_path);
            assert_eq!(server_b.await.unwrap(), expected_path);
        }
    }

    #[tokio::test]
    async fn old_in_flight_override_error_stays_redacted_after_reset() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let secret = "private-path-token";
        let url = format!("http://{}/{secret}/v1", listener.local_addr().unwrap());
        let current = Arc::new(Mutex::new(Some(url.clone())));
        let registry = Arc::new(
            ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
                |_, _| Ok("key".into()),
                |_, _| Ok(None),
                {
                    let current = current.clone();
                    move |_, _| Ok(current.lock().unwrap().clone())
                },
                ProviderTimeoutPolicy::default(),
            ),
        );
        let provider = registry.get_or_create_for_workspace("a", "openai").unwrap();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let response_url = url.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 8192];
            stream.read(&mut request).await.unwrap();
            accepted_tx.send(()).unwrap();
            release_rx.await.unwrap();
            let body = format!("{{\"error\":\"rate limit at {response_url}; retry-after: 3\"}}");
            stream.write_all(format!("HTTP/1.1 429 Too Many Requests\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        });
        let request_provider = provider.clone();
        let request = tokio::spawn(async move { request_provider.chat(chat_request()).await });
        accepted_rx.await.unwrap();
        *current.lock().unwrap() = None;
        registry.invalidate_workspace_provider("a", "openai");
        release_tx.send(()).unwrap();
        let error = request.await.unwrap().unwrap_err();
        server.await.unwrap();
        assert!(!format!("{error:#?}").contains(secret));
        assert!(!format!("{error:#}").contains(secret));
        let classification = provider.classify_failure(&error).unwrap();
        assert_eq!(classification.class, ProviderFailureClass::RateLimit);
        assert_eq!(classification.http_status, Some(429));
        assert_eq!(classification.retry_after_ms, Some(3000));
    }

    #[tokio::test]
    async fn openrouter_completion_marker_survives_endpoint_redaction() {
        use crate::failure::{ProviderStreamIncomplete, provider_stream_incomplete};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        for done in [false, true] {
            for with_json in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!(
                    "http://{}/private-marker-path/v1",
                    listener.local_addr().unwrap()
                );
                let body = format!(
                    "{}{}",
                    if with_json {
                        "data: {\"id\":\"gen-completion_body\",\"choices\":[{\"delta\":{\"content\":\"{\\\"facts\\\":[]}\"},\"finish_reason\":null}]}\n\n"
                    } else {
                        ""
                    },
                    if done { "data: [DONE]\n\n" } else { "" }
                );
                let server = tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    let mut buf = [0u8; 4096];
                    loop {
                        let n = socket.read(&mut buf).await.unwrap();
                        assert!(n > 0);
                        request.extend_from_slice(&buf[..n]);
                        if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&request[..end]);
                            let length: usize = headers
                                .lines()
                                .find_map(|line| {
                                    let (key, value) = line.split_once(':')?;
                                    key.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse().unwrap())
                                })
                                .unwrap_or(0);
                            if request.len() >= end + 4 + length {
                                break;
                            }
                        }
                    }
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nX-Generation-Id: gen-completion_header\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                    socket.shutdown().await.unwrap();
                });
                let registry =
                    ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
                        |_, _| Ok("key".into()),
                        |_, _| Ok(None),
                        move |_, _| Ok(Some(url.clone())),
                        ProviderTimeoutPolicy::default(),
                    );
                let provider = registry
                    .get_or_create_for_workspace("a", "openrouter")
                    .unwrap();
                let mut stream = provider.stream_chat(chat_request()).await.unwrap();
                let mut text = String::new();
                let error = loop {
                    match stream.next().await {
                        Some(Ok(chunk)) => {
                            assert!(!chunk.is_final);
                            text.push_str(&chunk.delta);
                        }
                        Some(Err(error)) => break error,
                        None => panic!("adapter must report incomplete completion"),
                    }
                };
                server.await.unwrap();
                assert_eq!(text, if with_json { "{\"facts\":[]}" } else { "" });
                assert_eq!(
                    provider_stream_incomplete(&error),
                    Some(if done {
                        ProviderStreamIncomplete::DoneWithoutFinishReason
                    } else {
                        ProviderStreamIncomplete::EofWithoutTerminalMarker
                    })
                );
                let classification = provider.classify_failure(&error).unwrap();
                assert_eq!(classification.class, ProviderFailureClass::StreamStall);
                assert_eq!(classification.http_status, None);
                assert_eq!(classification.error_reason, None);
                assert_eq!(
                    classification.request_id.map(String::from).as_deref(),
                    Some(if with_json {
                        "gen-completion_body"
                    } else {
                        "gen-completion_header"
                    })
                );
                assert!(!format!("{error:#?}").contains("private-marker-path"));
            }
        }
    }

    #[test]
    fn endpoint_redaction_does_not_invent_incomplete_completion_for_neighbor_errors() {
        use crate::failure::provider_stream_incomplete;
        let provider = EchoProvider::new();
        for (message, class) in [
            ("connection reset", ProviderFailureClass::NetworkTransient),
            (
                "API error (429 Too Many Requests): rate limit",
                ProviderFailureClass::RateLimit,
            ),
            (
                "API error (403 Forbidden): permission denied",
                ProviderFailureClass::AuthOrPermission,
            ),
            (
                "API error (400 Bad Request): invalid request",
                ProviderFailureClass::ProviderRejected,
            ),
            ("stream stall", ProviderFailureClass::StreamStall),
            (
                "malformed OpenRouter SSE frame",
                ProviderFailureClass::StreamStall,
            ),
            (
                "provider sent payload after finish_reason",
                ProviderFailureClass::StreamStall,
            ),
            // Identical text alone is deliberately insufficient proof.
            (
                "provider stream ended before a terminal marker",
                ProviderFailureClass::StreamStall,
            ),
        ] {
            let error = redacted_endpoint_error(
                &provider,
                anyhow::anyhow!(message),
                ProviderFailureStage::MidStream,
            );
            assert!(provider_stream_incomplete(&error).is_none());
            assert_eq!(
                error
                    .downcast_ref::<RedactedEndpointError>()
                    .unwrap()
                    .classification
                    .class,
                class
            );
        }
    }

    #[tokio::test]
    async fn stream_response_read_error_does_not_expose_override_path() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/stream-private-token/v1",
            listener.local_addr().unwrap()
        );
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 8192];
            stream.read(&mut request).await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 128\r\nConnection: close\r\n\r\ndata: {",
                )
                .await
                .unwrap();
        });
        let registry = ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
            |_, _| Ok("key".into()),
            |_, _| Ok(None),
            move |_, _| Ok(Some(url.clone())),
            ProviderTimeoutPolicy::default(),
        );
        let provider = registry.get_or_create_for_workspace("a", "openai").unwrap();
        let mut stream = provider.stream_chat(chat_request()).await.unwrap();
        let error = loop {
            match stream.next().await {
                Some(Err(error)) => break error,
                Some(Ok(_)) => continue,
                None => panic!("truncated response should fail"),
            }
        };
        server.await.unwrap();
        assert!(!format!("{error:#?}").contains("stream-private-token"));
        assert!(provider.classify_failure(&error).is_some());
    }

    #[tokio::test]
    async fn gemini_override_errors_hide_endpoint_and_key_for_chat_and_stream() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/gemini-private-path/v1beta",
            listener.local_addr().unwrap()
        );
        let echoed_url = url.clone();
        let server = tokio::spawn(async move {
            for (status, detail) in [
                ("429 Too Many Requests", "rate limit"),
                ("400 Bad Request", "this model does not support streaming"),
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 8192];
                socket.read(&mut request).await.unwrap();
                let body = format!("{detail} at {echoed_url}?key=gemini-private-key");
                socket.write_all(format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ).as_bytes()).await.unwrap();
            }
        });
        let registry = ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
            |_, _| Ok("gemini-private-key".into()),
            |_, _| Ok(None),
            move |_, _| Ok(Some(url.clone())),
            ProviderTimeoutPolicy::default(),
        );
        let provider = registry.get_or_create_for_workspace("a", "gemini").unwrap();
        for (streaming, expected_class, expected_status) in [
            (false, ProviderFailureClass::RateLimit, 429),
            (true, ProviderFailureClass::UnsupportedStreaming, 400),
        ] {
            let error = if streaming {
                provider
                    .stream_chat(chat_request())
                    .await
                    .err()
                    .expect("stream request fails")
            } else {
                provider.chat(chat_request()).await.unwrap_err()
            };
            let public = format!("{error:#} {error:#?}");
            assert!(!public.contains("gemini-private-path"));
            assert!(!public.contains("gemini-private-key"));
            let classification = provider.classify_failure(&error).unwrap();
            assert_eq!(classification.class, expected_class);
            assert_eq!(classification.http_status, Some(expected_status));
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn auxiliary_embedding_error_keeps_class_without_exposing_endpoint() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let secret = "embedding-private-token";
        let url = format!("http://{}/{secret}/v1", listener.local_addr().unwrap());
        let echoed_url = url.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 8192];
            socket.read(&mut request).await.unwrap();
            let body = serde_json::json!({
                "error": {"message": format!("maximum context length exceeded at {echoed_url}")}
            })
            .to_string();
            socket.write_all(format!(
                "HTTP/1.1 400 Bad Request\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            ).as_bytes()).await.unwrap();
        });
        let registry = ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
            |_, _| Ok("key".into()),
            |_, _| Ok(None),
            move |_, _| Ok(Some(url.clone())),
            ProviderTimeoutPolicy::default(),
        );
        let provider = registry.get_or_create_for_workspace("a", "openai").unwrap();
        let error = provider
            .embed(EmbeddingRequest::new("fixture", vec!["hello".into()]))
            .await
            .unwrap_err();
        server.await.unwrap();
        assert!(!format!("{error:#?}").contains(secret));
        let classification = provider.classify_failure(&error).unwrap();
        assert_eq!(classification.class, ProviderFailureClass::ContextTooLarge);
        assert_eq!(classification.http_status, Some(400));
    }

    #[tokio::test]
    async fn openai_upload_errors_are_safe_before_observability_and_keep_http_classification() {
        use base64::Engine;
        use std::io::Write;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        struct TraceWriter(Arc<Mutex<Vec<u8>>>);
        impl Write for TraceWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        for (status, echo_url, class, attempts, retry_after_ms) in [
            (
                None,
                false,
                ProviderFailureClass::NetworkTransient,
                0usize,
                None,
            ),
            (
                Some(400),
                true,
                ProviderFailureClass::ProviderRejected,
                1,
                None,
            ),
            (
                Some(429),
                false,
                ProviderFailureClass::RateLimit,
                3,
                Some(3000),
            ),
            (Some(503), false, ProviderFailureClass::Provider5xx, 3, None),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let secret = "openai-upload-private-token";
            let url = format!("http://{}/{secret}/v1", listener.local_addr().unwrap());
            let echoed_url = url.clone();
            let assertion_url = url.clone();
            let server = match status {
                Some(status) => Some(tokio::spawn(async move {
                    let mut paths = Vec::new();
                    for _ in 0..attempts {
                        let (mut socket, _) = listener.accept().await.unwrap();
                        let mut request = Vec::new();
                        let header_end = loop {
                            if let Some(index) =
                                request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                            {
                                break index + 4;
                            }
                            let mut chunk = [0u8; 8192];
                            let count = socket.read(&mut chunk).await.unwrap();
                            assert!(count > 0, "upload request headers ended early");
                            request.extend_from_slice(&chunk[..count]);
                        };
                        let headers = std::str::from_utf8(&request[..header_end]).unwrap();
                        paths.push(headers.lines().next().unwrap().to_owned());
                        let content_length = headers
                            .lines()
                            .find_map(|line| {
                                line.split_once(':')
                                    .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                                    .map(|(_, value)| value)
                            })
                            .unwrap()
                            .trim()
                            .parse::<usize>()
                            .unwrap();
                        while request.len() < header_end + content_length {
                            let mut chunk = [0u8; 8192];
                            let count = socket.read(&mut chunk).await.unwrap();
                            assert!(count > 0, "upload request body ended early");
                            request.extend_from_slice(&chunk[..count]);
                        }
                        let body = if echo_url {
                            format!("bad request at {echoed_url}/files")
                        } else {
                            String::new()
                        };
                        let retry_after = if status == 429 {
                            "Retry-After: 3\r\n"
                        } else {
                            ""
                        };
                        socket.write_all(format!(
                            "HTTP/1.1 {status} Failure\r\n{retry_after}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        ).as_bytes()).await.unwrap();
                    }
                    paths
                })),
                None => {
                    drop(listener);
                    None
                }
            };
            let registry =
                ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
                    |_, _| Ok("key".into()),
                    |_, _| Ok(None),
                    move |_, _| Ok(Some(url.clone())),
                    ProviderTimeoutPolicy::default(),
                );
            let provider = registry
                .get_or_create_for_workspace("upload-workspace", "openai")
                .unwrap();
            let mut request = chat_request();
            // The normal planner uploads files at or above its default threshold.
            let bytes = vec![b'x'; 512 * 1024];
            request.messages = vec![crate::ChatMessage::user_parts(vec![
                crate::MessageContentPart::file(crate::MessageAttachment {
                    mime_type: "application/octet-stream".into(),
                    name: Some("payload.bin".into()),
                    size_bytes: Some(bytes.len() as u64),
                    sha256: None,
                    source: crate::AttachmentDataSource::Bytes {
                        base64_data: base64::engine::general_purpose::STANDARD.encode(bytes),
                    },
                    artifact: None,
                }),
            ])];
            let captured = Arc::new(Mutex::new(Vec::new()));
            let output = captured.clone();
            let subscriber = tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::DEBUG)
                .with_writer(move || TraceWriter(output.clone()))
                .finish();
            let _guard = tracing::subscriber::set_default(subscriber);
            let error = provider.chat(request).await.unwrap_err();
            let logs = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
            assert!(logs.contains("attachment.upload.fail"), "{logs}");
            assert!(logs.contains("upload_file"), "{logs}");
            assert!(logs.contains("ATTACHMENT_OPERATION_FAILED"), "{logs}");
            assert!(logs.contains(&format!("{:?}", class)), "{logs}");
            assert_eq!(
                logs.matches("attachment.upload.retry").count(),
                if status.is_some() {
                    attempts.saturating_sub(1)
                } else {
                    2
                }
            );
            assert!(!logs.contains(secret), "{logs}");
            assert!(!logs.contains(&assertion_url));
            for representation in [
                format!("{error}"),
                format!("{error:#}"),
                format!("{error:?}"),
                format!("{error:#?}"),
            ] {
                assert!(!representation.contains(secret), "{representation}");
            }
            assert!(
                !error
                    .chain()
                    .any(|cause| format!("{cause:?}").contains(secret))
            );
            let classification = provider.classify_failure(&error).unwrap();
            assert_eq!(classification.class, class);
            assert_eq!(classification.http_status, status);
            assert_eq!(classification.retry_after_ms, retry_after_ms);
            if let Some(server) = server {
                let paths = tokio::time::timeout(Duration::from_secs(10), server)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    paths,
                    vec![format!("POST /{secret}/v1/files HTTP/1.1"); attempts]
                );
                assert!(
                    logs.contains(&format!("HTTP {}", status.unwrap())),
                    "{logs}"
                );
            }
        }
    }
}

#[cfg(test)]
mod stream_error_redaction_tests {
    use super::*;
    #[test]
    fn native_cause_survives_endpoint_redaction_without_untrusted_text() {
        let error = anyhow::Error::from(crate::failure::AnthropicStreamError::Overloaded)
            .context("credential=secret payload");
        let redacted = redacted_endpoint_error(
            &crate::providers::EchoProvider::new(),
            error,
            ProviderFailureStage::MidStream,
        );
        assert_eq!(
            crate::failure::anthropic_stream_error(&redacted),
            Some(crate::failure::AnthropicStreamError::Overloaded)
        );
        let classification = &redacted
            .downcast_ref::<RedactedEndpointError>()
            .unwrap()
            .classification;
        assert_eq!(classification.class, ProviderFailureClass::Provider5xx);
        assert_eq!(
            classification.provider_code.as_deref(),
            Some("overloaded_error")
        );
        assert!(!format!("{redacted:#?} {redacted:#}").contains("secret"));
    }
}
