//! Operation-scoped observation for calls outside the ordinary agent round.
use anyhow::Result;
use async_trait::async_trait;
use futures_util::{StreamExt, stream::BoxStream};
use pioneer_crud::{CrudStore, ProviderUsageObservation};
use pioneer_protocol::ProviderModelInfo;
use pioneer_provider::{
    ChatRequest, ChatResponse, EmbeddingRequest, EmbeddingResponse, Provider, ProviderCapabilities,
    ProviderFailureClassification, ProviderWarmupOutcome, StreamChunk, TokenUsage,
};
use std::sync::Arc;

pub(crate) fn observe(
    inner: Arc<dyn Provider>,
    store: &CrudStore,
    workspace: &str,
    operation: &'static str,
    owner: &str,
) -> Arc<dyn Provider> {
    Arc::new(JournalProvider {
        inner,
        store: store.clone(),
        workspace: workspace.into(),
        operation,
        owner: owner.into(),
    })
}

struct JournalProvider {
    inner: Arc<dyn Provider>,
    store: CrudStore,
    workspace: String,
    operation: &'static str,
    owner: String,
}

#[derive(Clone)]
pub(crate) struct Call {
    store: CrudStore,
    record: ProviderUsageObservation,
}
impl Call {
    pub(crate) async fn start(
        store: &CrudStore,
        workspace: &str,
        operation: &str,
        owner: &str,
        usage: &TokenUsage,
    ) -> Result<Self> {
        let now = chrono::Utc::now().timestamp();
        let call = Self {
            store: store.clone(),
            record: ProviderUsageObservation {
                id: pioneer_protocol::generate_id(21),
                workspace_id: workspace.into(),
                operation_kind: operation.into(),
                owner_id: owner.into(),
                status: "started".into(),
                usage_json: "{}".into(),
                started_at: now,
                updated_at: now,
            },
        };
        call.record("started", usage).await?;
        Ok(call)
    }
    pub(crate) async fn record(&self, status: &str, usage: &TokenUsage) -> Result<()> {
        let mut record = self.record.clone();
        record.status = status.into();
        record.usage_json = serde_json::to_string(usage)?;
        record.updated_at = chrono::Utc::now().timestamp();
        self.store.record_provider_usage(record).await
    }
}

impl JournalProvider {
    async fn start(&self, model: &str) -> Result<(Call, TokenUsage)> {
        let usage = TokenUsage {
            provider: Some(self.name().into()),
            model: Some(model.into()),
            api: Some(self.usage_api().into()),
            api_version: self.usage_api_version(),
            route: self.usage_route(),
            ..Default::default()
        };
        let call = Call::start(
            &self.store,
            &self.workspace,
            self.operation,
            &self.owner,
            &usage,
        )
        .await?;
        Ok((call, usage))
    }
}

#[async_trait]
impl Provider for JournalProvider {
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
    fn capabilities(&self) -> ProviderCapabilities {
        self.inner.capabilities()
    }
    fn model_tool_calling(&self, model: &str) -> bool {
        self.inner.model_tool_calling(model)
    }
    fn native_file_tool_capability(
        &self,
        model: &str,
    ) -> pioneer_provider::file_tools::NativeFileToolCapability {
        self.inner.native_file_tool_capability(model)
    }
    fn classify_failure(&self, error: &anyhow::Error) -> Option<ProviderFailureClassification> {
        self.inner.classify_failure(error)
    }
    async fn prepare_input_budget(
        &self,
        request: ChatRequest,
    ) -> Result<pioneer_provider::attachments::PreparedInputBudget> {
        self.inner.prepare_input_budget(request).await
    }
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        let (call, mut usage) = self.start(&request.model).await?;
        let response = self.inner.chat(request).await;
        if let Ok(response) = &response {
            if let Some(snapshot) = &response.usage {
                usage.update(snapshot);
            }
        } else if let Err(error) = &response {
            if let Some(snapshot) = pioneer_provider::usage::error_usage(error) {
                usage.update(snapshot);
            }
        }
        call.record(
            if response.is_ok() {
                "completed"
            } else {
                "failed"
            },
            &usage,
        )
        .await?;
        response
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
    ) -> Result<pioneer_provider::ProviderStream> {
        let (call, mut usage) = self.start(&request.model).await?;
        let response = match self.inner.stream_chat_with_diagnostics(request).await {
            Ok(response) => response,
            Err(error) => {
                if let Some(snapshot) = pioneer_provider::usage::error_usage(&error) {
                    usage.update(snapshot);
                }
                call.record("failed", &usage).await?;
                return Err(error);
            }
        };
        let diagnostics = response.diagnostics;
        let stream = Box::pin(futures_util::stream::unfold(
            (response.stream, call, usage, None::<bool>),
            |(mut stream, call, mut usage, mut terminal)| async move {
                match stream.next().await {
                    Some(result) => {
                        let status = match &result {
                            Ok(chunk) => {
                                if let Some(snapshot) = &chunk.usage {
                                    usage.update(snapshot);
                                }
                                if chunk.is_final {
                                    terminal = terminal.or(Some(true));
                                }
                                // Usage (or a protocol error) may follow a
                                // final chunk. Commit success after draining
                                // the native stream, keeping partial evidence
                                // durable without reopening a terminal row.
                                match terminal {
                                    Some(false) => "failed",
                                    _ => "started",
                                }
                            }
                            Err(error) => {
                                if let Some(snapshot) = pioneer_provider::usage::error_usage(error)
                                {
                                    usage.update(snapshot);
                                }
                                terminal = Some(false);
                                "failed"
                            }
                        };
                        // Keep partial usage before handing it to a cancellable consumer.
                        if !matches!(&result, Ok(chunk) if chunk.usage.is_none() && !chunk.is_final)
                        {
                            if let Err(error) = call.record(status, &usage).await {
                                return Some((Err(error), (stream, call, usage, Some(false))));
                            }
                        }
                        Some((result, (stream, call, usage, terminal)))
                    }
                    None => {
                        if terminal != Some(false) {
                            let status = if terminal == Some(true) {
                                "completed"
                            } else {
                                "failed"
                            };
                            if let Err(error) = call.record(status, &usage).await {
                                return Some((Err(error), (stream, call, usage, Some(false))));
                            }
                        }
                        None
                    }
                }
            },
        ));
        Ok(pioneer_provider::ProviderStream {
            stream,
            diagnostics,
        })
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
    async fn warmup(&self) -> Result<ProviderWarmupOutcome> {
        self.inner.warmup().await
    }
}
