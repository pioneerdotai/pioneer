//! Immutable selected-model facts for one accepted native Turn. Authorization
//! is revalidated by the delegated provider; these facts are never permissions.
use crate::{
    ChatRequest, ChatResponse, EmbeddingRequest, EmbeddingResponse, ModelToolSupport,
    NativeFileToolCapability, Provider, ProviderCapabilities, ProviderStream, StreamChunk,
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[path = "model_contract_bounds.rs"]
mod bounds;
pub(crate) use bounds::validate as validate_json_shape;

use std::{collections::BTreeMap, future::Future, sync::Arc};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedProviderModelContract {
    schema_version: u32,
    provider: String,
    model: String,
    authority_fingerprint: Option<String>,
    adapter_api: String,
    adapter_api_version: Option<String>,
    adapter_route_identity: Option<String>,
    public_catalog: bool,
    adapter_capabilities: ProviderCapabilities,
    adapter_model_tools: ModelToolSupport,
    discovery_tools: Option<bool>,
    catalog_tools: Option<bool>,
    effective_tools: ModelToolSupport,
    native_file_tools: NativeFileToolCapability,
    catalog_json: String,
    discovery_json: Option<String>,
    native_reasoning: crate::generation::NativeReasoning,
    revision: String,
}

impl std::fmt::Debug for AcceptedProviderModelContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceptedProviderModelContract")
            .field("schema_version", &self.schema_version)
            .field("revision", &self.revision)
            .field("tool_support", &self.effective_tools)
            .finish_non_exhaustive()
    }
}

pub(crate) fn bounded_json(value: &(impl Serialize + ?Sized)) -> Result<String> {
    bounded_json_with_limit(value, pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES)
}
pub(crate) fn bounded_json_with_limit(
    value: &(impl Serialize + ?Sized),
    limit: usize,
) -> Result<String> {
    struct Writer(Vec<u8>, usize);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.1.saturating_sub(self.0.len()) {
                return Err(std::io::Error::other(
                    "accepted model contract exceeds byte limit",
                ));
            }
            let next_len = self.0.len() + bytes.len();
            if next_len > self.0.capacity() {
                let capacity = next_len
                    .max(self.0.capacity().saturating_mul(2))
                    .min(self.1);
                self.0.reserve_exact(capacity - self.0.len());
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(
        Vec::new(),
        limit.min(pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES),
    );
    serde_json::to_writer(&mut writer, value).map_err(|_| {
        anyhow::anyhow!("accepted model contract cannot be encoded within byte limit")
    })?;
    Ok(String::from_utf8(writer.0)?)
}

fn same_provider(first: &str, second: &str) -> bool {
    match (
        crate::provider_definition(first),
        crate::provider_definition(second),
    ) {
        (Some(a), Some(b)) => a.name == b.name,
        (None, None) => first.eq_ignore_ascii_case(second),
        _ => false,
    }
}

impl AcceptedProviderModelContract {
    pub(crate) fn capture(
        provider: &(impl Provider + ?Sized),
        model: &str,
        public_catalog: bool,
        discovery: Option<&pioneer_protocol::ProviderModelInfo>,
        discovery_tools: Option<bool>,
        native_reasoning: crate::generation::NativeReasoning,
        adapter_model_tools: ModelToolSupport,
        limit: usize,
    ) -> Result<Self> {
        let catalog = crate::catalog::model_catalog().ok();
        Self::capture_with_catalog(
            provider,
            model,
            public_catalog,
            discovery,
            discovery_tools,
            native_reasoning,
            adapter_model_tools,
            catalog.as_deref(),
            limit,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn capture_with_catalog(
        provider: &(impl Provider + ?Sized),
        model: &str,
        public_catalog: bool,
        discovery: Option<&pioneer_protocol::ProviderModelInfo>,
        discovery_tools: Option<bool>,
        native_reasoning: crate::generation::NativeReasoning,
        adapter_model_tools: ModelToolSupport,
        catalog: Option<&crate::catalog::ModelCatalog>,
        limit: usize,
    ) -> Result<Self> {
        ensure!(
            provider
                .name()
                .len()
                .saturating_add(model.len())
                .saturating_add(provider.authority_fingerprint().map_or(0, str::len))
                <= limit.min(pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES),
            "accepted model identity exceeds byte limit"
        );
        // Select borrowed facts and encode before cloning potentially large
        // catalog/discovery metadata. No network refresh occurs here.
        ensure!(
            native_reasoning.len() <= 64,
            "accepted native reasoning cardinality exceeds profile limit"
        );
        let catalog_json =
            crate::catalog::selected_model_json(catalog, provider.name(), model, limit)?;
        validate_json_shape(&catalog_json)?;
        let catalog_tools = catalog.and_then(|c| c.tool_support(provider.name(), model));
        let adapter_capabilities = provider.capabilities();
        let ceiling = adapter_capabilities.tool_calling;
        let effective_tools = if ceiling {
            ModelToolSupport::from_optional(crate::catalog::merge_tool_support(
                adapter_model_tools.as_optional(),
                crate::catalog::merge_tool_support(
                    discovery_tools,
                    public_catalog.then_some(catalog_tools).flatten(),
                ),
            ))
        } else {
            ModelToolSupport::Unsupported
        };
        let mut contract = Self {
            schema_version: 1,
            provider: provider.name().into(),
            model: model.into(),
            authority_fingerprint: provider.authority_fingerprint().map(str::to_owned),
            adapter_api: provider.usage_api().to_owned(),
            adapter_api_version: provider.usage_api_version(),
            adapter_route_identity: provider.usage_route(),
            public_catalog,
            adapter_capabilities,
            adapter_model_tools,
            discovery_tools,
            catalog_tools,
            effective_tools,
            native_file_tools: provider.native_file_tool_capability(model),
            discovery_json: discovery
                .map(|value| {
                    bounded_json_with_limit(value, limit.saturating_sub(catalog_json.len()))
                })
                .transpose()?,
            catalog_json,
            native_reasoning,
            revision: String::new(),
        };
        contract.revision = hex::encode(Sha256::digest(
            bounded_json_with_limit(&contract, limit)?.as_bytes(),
        ));
        bounded_json_with_limit(&contract, limit)?;
        contract.validate_identity(provider.name(), model, provider.authority_fingerprint())?;
        Ok(contract)
    }

    pub fn require_selection(&self, provider: &str, model: &str) -> Result<()> {
        self.validate_identity(provider, model, self.authority_fingerprint.as_deref())
    }
    pub fn revision(&self) -> &str {
        &self.revision
    }
    pub fn tool_support(&self) -> ModelToolSupport {
        self.effective_tools
    }
    pub fn validate_identity(
        &self,
        provider: &str,
        model: &str,
        fingerprint: Option<&str>,
    ) -> Result<()> {
        ensure!(
            self.schema_version == 1,
            "unsupported accepted model contract version"
        );
        ensure!(
            same_provider(&self.provider, provider)
                && self.model == model
                && self.authority_fingerprint.as_deref() == fingerprint,
            "accepted model contract identity differs from current provider authority"
        );
        ensure!(
            self.native_file_tools.schema_version == crate::NATIVE_FILE_TOOL_SCHEMA_VERSION
                && same_provider(&self.native_file_tools.provider, provider)
                && self.native_file_tools.model == model,
            "accepted native file wire contract is unavailable"
        );
        ensure!(
            self.native_reasoning.len() <= 64,
            "accepted native reasoning cardinality exceeds profile limit"
        );
        let expected = if self.adapter_capabilities.tool_calling {
            ModelToolSupport::from_optional(crate::catalog::merge_tool_support(
                self.adapter_model_tools.as_optional(),
                crate::catalog::merge_tool_support(
                    self.discovery_tools,
                    self.public_catalog.then_some(self.catalog_tools).flatten(),
                ),
            ))
        } else {
            ModelToolSupport::Unsupported
        };
        ensure!(
            self.effective_tools == expected,
            "accepted tool capability evidence is inconsistent"
        );
        bounded_json(self)?;
        let mut unsigned = self.clone();
        unsigned.revision.clear();
        ensure!(
            self.revision == hex::encode(Sha256::digest(bounded_json(&unsigned)?.as_bytes())),
            "accepted model capability revision differs from its facts"
        );
        Ok(())
    }
    pub fn from_json(encoded: &str) -> Result<Self> {
        ensure!(
            encoded.len() <= pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES,
            "accepted model contract exceeds byte limit"
        );
        validate_json_shape(encoded)?;
        let result: Self = serde_json::from_str(encoded)
            .map_err(|_| anyhow::anyhow!("accepted model contract is invalid"))?;
        result.validate_identity(
            &result.provider,
            &result.model,
            result.authority_fingerprint.as_deref(),
        )?;
        // Rebuild the existing owner now, outside DB access, to validate all
        // exact selected facts before a continuation can be dispatched.
        RuntimeContract::new(result.clone())?;
        Ok(result)
    }
    pub fn to_json(&self) -> Result<String> {
        bounded_json(self)
    }
    pub fn to_json_with_limit(&self, limit: usize) -> Result<String> {
        bounded_json_with_limit(self, limit)
    }
    pub fn bind(self, provider: Arc<dyn Provider>) -> Result<Arc<dyn Provider>> {
        self.validate_identity(
            provider.name(),
            &self.model,
            provider.authority_fingerprint(),
        )?;
        ensure!(
            self.adapter_api == provider.usage_api()
                && self.adapter_api_version == provider.usage_api_version()
                && self.adapter_route_identity == provider.usage_route(),
            "accepted provider API wire identity is unavailable in current adapter"
        );
        Ok(Arc::new(FrozenModelProvider {
            inner: provider,
            contract: Arc::new(RuntimeContract::new(self)?),
        }))
    }
}

pub(crate) struct RuntimeContract {
    pub(crate) accepted: AcceptedProviderModelContract,
    pub(crate) catalog: Arc<crate::catalog::ModelCatalog>,
    pub(crate) discovery: Option<pioneer_protocol::ProviderModelInfo>,
}
impl RuntimeContract {
    fn new(accepted: AcceptedProviderModelContract) -> Result<Self> {
        let catalog = Arc::new(crate::catalog::restore_selected_model(
            &accepted.catalog_json,
            &accepted.provider,
            &accepted.model,
        )?);
        if let Some(encoded) = accepted.discovery_json.as_deref() {
            validate_json_shape(encoded)?;
        }
        let mut discovery: Option<pioneer_protocol::ProviderModelInfo> = accepted
            .discovery_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|_| anyhow::anyhow!("accepted scoped discovery is invalid"))?;
        if let Some(model) = &mut discovery {
            ensure!(
                model.capabilities.reasoning.is_some() || accepted.native_reasoning.is_empty(),
                "accepted native reasoning owner is missing"
            );
            if let Some(reasoning) = &mut model.capabilities.reasoning {
                reasoning.native = accepted.native_reasoning.clone();
            }
        }
        ensure!(
            discovery.as_ref().is_none_or(|m| m.id == accepted.model),
            "accepted discovery model identity differs"
        );
        ensure!(
            catalog.tool_support(&accepted.provider, &accepted.model) == accepted.catalog_tools,
            "accepted catalog tool evidence differs"
        );
        Ok(Self {
            accepted,
            catalog,
            discovery,
        })
    }
    async fn scope<T>(self: &Arc<Self>, future: impl Future<Output = T>) -> T {
        CURRENT.scope(self.clone(), future).await
    }
    pub(crate) fn require_provider_authority(
        &self,
        provider: &str,
        fingerprint: Option<&str>,
    ) -> Result<()> {
        ensure!(
            same_provider(provider, &self.accepted.provider)
                && fingerprint == self.accepted.authority_fingerprint.as_deref(),
            "accepted model facts belong to another provider authority"
        );
        Ok(())
    }
    pub(crate) fn public_catalog(&self) -> bool {
        self.accepted.public_catalog
    }
    pub(crate) fn tools(&self) -> BTreeMap<String, bool> {
        self.accepted
            .discovery_tools
            .map(|v| BTreeMap::from([(self.accepted.model.clone(), v)]))
            .unwrap_or_default()
    }
    pub(crate) fn reasoning(&self) -> BTreeMap<String, crate::generation::NativeReasoning> {
        BTreeMap::from([(
            self.accepted.model.clone(),
            self.accepted.native_reasoning.clone(),
        )])
    }
}
tokio::task_local! { static CURRENT: Arc<RuntimeContract>; }
pub(crate) fn current() -> Option<Arc<RuntimeContract>> {
    CURRENT.try_with(Clone::clone).ok()
}

struct FrozenModelProvider {
    inner: Arc<dyn Provider>,
    contract: Arc<RuntimeContract>,
}
impl FrozenModelProvider {
    fn require_request(&self, request: &ChatRequest) -> Result<()> {
        ensure!(
            request.model == self.contract.accepted.model,
            "request differs from accepted model"
        );
        self.contract
            .require_provider_authority(self.inner.name(), self.inner.authority_fingerprint())
    }
}
#[async_trait]
impl Provider for FrozenModelProvider {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn usage_api(&self) -> &'static str {
        self.inner.usage_api()
    }
    fn usage_api_version(&self) -> Option<String> {
        self.inner.usage_api_version()
    }
    fn usage_route(&self) -> Option<String> {
        self.inner.usage_route()
    }
    fn authority_fingerprint(&self) -> Option<&str> {
        self.inner.authority_fingerprint()
    }
    fn resolved_azure_connection(&self) -> Option<crate::AzureResolvedConnection> {
        self.inner.resolved_azure_connection()
    }
    fn capabilities(&self) -> ProviderCapabilities {
        self.contract.accepted.adapter_capabilities.clone()
    }
    fn model_tool_support(&self, model: &str) -> ModelToolSupport {
        if model == self.contract.accepted.model {
            self.contract.accepted.effective_tools
        } else {
            ModelToolSupport::Unknown
        }
    }
    fn native_file_tool_capability(&self, model: &str) -> NativeFileToolCapability {
        if model == self.contract.accepted.model {
            self.contract.accepted.native_file_tools.clone()
        } else {
            NativeFileToolCapability::unavailable(
                self.name(),
                model,
                "model differs from accepted contract",
            )
        }
    }
    fn model_catalog_for_request(&self, model: &str) -> Result<Arc<crate::catalog::ModelCatalog>> {
        ensure!(
            model == self.contract.accepted.model,
            "model differs from accepted contract"
        );
        Ok(self.contract.catalog.clone())
    }
    fn capture_model_contract_with_limit(
        &self,
        model: &str,
        limit: usize,
    ) -> Result<AcceptedProviderModelContract> {
        ensure!(
            model == self.contract.accepted.model,
            "model differs from accepted contract"
        );
        self.contract.accepted.to_json_with_limit(limit)?;
        Ok(self.contract.accepted.clone())
    }
    fn classify_failure(
        &self,
        error: &anyhow::Error,
    ) -> Option<crate::ProviderFailureClassification> {
        self.inner.classify_failure(error)
    }
    async fn prepare_input_budget(
        &self,
        request: ChatRequest,
    ) -> Result<crate::attachments::PreparedInputBudget> {
        self.require_request(&request)?;
        self.contract
            .scope(self.inner.prepare_input_budget(request))
            .await
    }
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        self.require_request(&request)?;
        self.contract.scope(self.inner.chat(request)).await
    }
    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        Ok(self.stream_chat_with_diagnostics(request).await?.stream)
    }
    async fn stream_chat_with_diagnostics(&self, request: ChatRequest) -> Result<ProviderStream> {
        self.require_request(&request)?;
        self.contract
            .scope(self.inner.stream_chat_with_diagnostics(request))
            .await
    }
    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse> {
        self.inner.embed(request).await
    }
    async fn warmup(&self) -> Result<crate::ProviderWarmupOutcome> {
        self.inner.warmup().await
    }
    async fn list_models(&self) -> Result<Vec<pioneer_protocol::ProviderModelInfo>> {
        self.inner.list_models().await
    }
    async fn list_embedding_models(&self) -> Result<Vec<pioneer_protocol::ProviderModelInfo>> {
        self.inner.list_embedding_models().await
    }
    async fn list_transcription_models(&self) -> Result<Vec<pioneer_protocol::ProviderModelInfo>> {
        self.inner.list_transcription_models().await
    }
}

#[cfg(test)]
#[path = "model_contract_tests.rs"]
mod tests;
