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
        inherited_parent: pioneer_provider::usage::current_causal_parent(),
    })
}

struct JournalProvider {
    inner: Arc<dyn Provider>,
    store: CrudStore,
    workspace: String,
    operation: &'static str,
    owner: String,
    // Preserve runtime causality across the existing synchronous embedding bridge.
    inherited_parent: Option<String>,
}

#[derive(Clone)]
pub(crate) struct Call {
    store: CrudStore,
    record: ProviderUsageObservation,
    connection: Option<String>,
    manifest: Option<serde_json::Value>,
    causal_parent: Option<String>,
    bound_thread: bool,
    prepared_mono: std::time::Instant,
    prepared_at_ms: i64,
    prepared_clock_ns: Option<u64>,
    revision: Arc<std::sync::atomic::AtomicI64>,
    phase_persistence_failures: Arc<std::sync::atomic::AtomicU64>,
}
impl Call {
    pub(crate) async fn start(
        store: &CrudStore,
        workspace: &str,
        operation: &str,
        owner: &str,
        usage: &TokenUsage,
    ) -> Result<Self> {
        Self::start_with_connection(store, workspace, operation, owner, usage, None, None).await
    }
    async fn start_with_connection(
        store: &CrudStore,
        workspace: &str,
        operation: &str,
        owner: &str,
        usage: &TokenUsage,
        connection: Option<&str>,
        manifest: Option<serde_json::Value>,
    ) -> Result<Self> {
        let now = chrono::Utc::now().timestamp();
        let bound_thread = store
            .get_thread_by_id(owner)
            .await?
            .is_some_and(|thread| thread.workspace_id == workspace);
        let call = Self {
            bound_thread,
            store: store.clone(),
            manifest,
            causal_parent: pioneer_provider::usage::current_causal_parent(),
            prepared_mono: std::time::Instant::now(),
            prepared_at_ms: chrono::Utc::now().timestamp_millis(),
            prepared_clock_ns: pioneer_observability::work::clock_ms().checked_mul(1_000_000),
            revision: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            phase_persistence_failures: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            connection: connection
                .filter(|value| !value.is_empty() && value.len() <= 256)
                .map(str::to_owned),
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
    async fn phase(&self, mut fact: serde_json::Value) -> Result<()> {
        if let (Some(fact), Some(point)) = (
            fact.as_object_mut(),
            pioneer_provider::usage::observation_point().as_object(),
        ) {
            for (key, value) in point {
                fact.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
        fact["parent_operation_id"] =
            serde_json::json!(format!("provider:journal:{}", self.record.id));
        let result = self
            .store
            .record_provider_phase(&self.record.workspace_id, &self.record.owner_id, fact)
            .await;
        if result.is_err() {
            self.phase_persistence_failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }
    pub(crate) async fn record(&self, status: &str, usage: &TokenUsage) -> Result<()> {
        let mut record = self.record.clone();
        record.status = status.into();
        let mut value = serde_json::to_value(usage)?;
        // Reapply runtime-owned metadata on every cumulative/terminal update.
        // Provider JSON cannot supply owner, timing or context evidence.
        if let Some(connection) = &self.connection {
            value["_pioneer_journal"] =
                serde_json::json!({"schema_version":1,"connection_id":connection});
        }
        let revision = self
            .revision
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        value["_pioneer_observation"] = serde_json::json!({
            "schema_version":1,"connection_id":self.connection,
            "source_observation":pioneer_provider::usage::observation_point(),
            "revision":revision,"live_only":status=="started" && revision>0,
            "phase_persistence_failures":self.phase_persistence_failures.load(std::sync::atomic::Ordering::Relaxed),
            "observation_id":self.record.id,"context_manifest":self.manifest,"causal_parent":self.causal_parent,"bound_thread":self.bound_thread,
            "timing":{"process_run_id":pioneer_observability::identity::run_identity().run_id,
                "prepared_at_unix_seconds":self.record.started_at,"prepared_at_unix_ms":self.prepared_at_ms,
                "prepared_mono_ns":self.prepared_clock_ns,"clock_domain":"observability_work_clock_v1","clock_resolution_ns":1_000_000,
                "observation_finished_mono_ns":(status!="started").then(||pioneer_observability::work::clock_ms().checked_mul(1_000_000)).flatten(),
                "elapsed_ns":(status!="started").then(||self.prepared_mono.elapsed().as_nanos().min(i64::MAX as u128) as i64),
                "observation_finished_at":(status != "started").then(||chrono::Utc::now()),
                "elapsed_ms":(status != "started").then(|| self.prepared_mono.elapsed().as_millis().min(u64::MAX as u128) as u64),
                "basis":"observation_lifetime_monotonic",
                "dispatch_at":null,"provider_body_finished_at":null,
                "coverage":"preparation_and_observation_only"}
        });
        record.usage_json = serde_json::to_string(&value)?;
        record.updated_at = chrono::Utc::now().timestamp();
        self.store.record_provider_usage(record).await?;
        if status != "started" {
            // Numeric terminal commit has already succeeded. This separate
            // boundary is diagnostic and cannot change the provider outcome.
            let _ = self
                .phase(serde_json::json!({"phase":"settlement_completed",
                "numeric_revision":revision,"timing_basis":"after_durable_numeric_terminal_ack"}))
                .await;
        }
        Ok(())
    }
}

impl JournalProvider {
    async fn start(
        &self,
        request: &ChatRequest,
    ) -> Result<(Call, TokenUsage, pioneer_provider::usage::UsageContext)> {
        let context =
            pioneer_provider::usage::UsageContext::prepare_request(self.inner.as_ref(), request)?;
        let usage = context.initial_usage();
        let start = Call::start_with_connection(
            &self.store,
            &self.workspace,
            self.operation,
            &self.owner,
            &usage,
            self.authority_fingerprint(),
            Some(pioneer_provider::usage::request_manifest(request)?),
        );
        let call = match pioneer_provider::usage::current_causal_parent()
            .or_else(|| self.inherited_parent.clone())
        {
            Some(parent) => pioneer_provider::usage::scope_causal_parent(parent, start).await?,
            None => start.await?,
        };
        Ok((call, usage, context))
    }
}

#[async_trait]
impl Provider for JournalProvider {
    fn embedding_usage_profile(&self) -> pioneer_provider::ProviderUsageProfile {
        self.inner.embedding_usage_profile()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
    fn request_usage_profile(
        &self,
        request: &ChatRequest,
    ) -> anyhow::Result<pioneer_provider::ProviderUsageProfile> {
        self.inner.request_usage_profile(request)
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
    async fn prepare_model(&self, model: &str) -> Result<()> {
        self.inner.prepare_model(model).await
    }
    async fn model_context_tiers(
        &self,
        model: &str,
    ) -> Result<Option<pioneer_protocol::ProviderContextTiers>> {
        self.inner.model_context_tiers(model).await
    }
    async fn model_service_modes(
        &self,
        model: &str,
    ) -> Result<Option<pioneer_protocol::SourceServiceModes>> {
        self.inner.model_service_modes(model).await
    }
    fn standard_service_mode(&self, model: &str) -> pioneer_protocol::ServiceModeParameters {
        self.inner.standard_service_mode(model)
    }
    async fn model_limits(&self, model: &str) -> Result<pioneer_provider::catalog::CatalogLimits> {
        self.inner.model_limits(model).await
    }
    fn model_catalog_entry(&self, model: &str) -> Option<pioneer_provider::catalog::CatalogModel> {
        self.inner.model_catalog_entry(model)
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
        let (call, mut usage, context) = self.start(&request).await?;
        let response = context
            .scope_with_observations(
                async {
                    if self.operation == "summary" {
                        pioneer_provider::prompt_cache::scope(
                            &self.workspace,
                            &self.owner,
                            "compaction",
                            self.inner.chat(request),
                        )
                        .await
                    } else {
                        self.inner.chat(request).await
                    }
                },
                |fact| call.phase(fact),
            )
            .await?;
        if let Ok(response) = &response {
            if let Some(snapshot) = &response.usage {
                usage.update(snapshot);
            }
        } else if let Err(error) = &response {
            if let Some(snapshot) = pioneer_provider::usage::error_usage(error) {
                usage.update(snapshot);
            }
        }
        context.finalize_usage(&mut usage);
        if call
            .record(
                if response.is_ok() {
                    "completed"
                } else {
                    "failed"
                },
                &usage,
            )
            .await
            .is_err()
        {
            // Preserve the provider outcome. Re-running a successful paid call
            // cannot repair its journal commit; the durable start stays unknown.
            tracing::warn!("provider usage settlement commit unconfirmed");
            return response.map(|mut response| {
                if let Some(usage) = &mut response.usage {
                    if let Some(accounting) = &mut usage.accounting {
                        accounting["settlement_persistence"] = "unconfirmed".into();
                    }
                }
                response
            });
        }
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
        let (call, mut usage, context) = self.start(&request).await?;
        let response = match context
            .scope_with_observations(self.inner.stream_chat_with_diagnostics(request), |fact| {
                call.phase(fact)
            })
            .await?
        {
            Ok(response) => response,
            Err(error) => {
                if let Some(snapshot) = pioneer_provider::usage::error_usage(&error) {
                    usage.update(snapshot);
                }
                context.finalize_usage(&mut usage);
                if call.record("failed", &usage).await.is_err() {
                    tracing::warn!("provider usage failure commit unconfirmed");
                }
                return Err(error);
            }
        };
        let diagnostics = response.diagnostics;
        let message_delivery = response.message_delivery;
        let stream = Box::pin(futures_util::stream::unfold(
            (
                response.stream,
                call,
                usage,
                None::<bool>,
                context,
                false,
                false,
            ),
            |(
                mut stream,
                call,
                mut usage,
                mut terminal,
                context,
                mut first_chunk,
                mut first_semantic,
            )| async move {
                let next = context
                    .observe_transport(stream.next(), |fact| call.phase(fact))
                    .await;
                let next = match next {
                    Ok(next) => next,
                    Err(_) => {
                        // Only post-send diagnostics are possible while draining.
                        // Preserve existing numeric evidence and report the gap.
                        if let Some(accounting) = &mut usage.accounting {
                            accounting["settlement_persistence"] = "unconfirmed".into();
                        }
                        None
                    }
                };
                match next {
                    Some(result) => {
                        let mut phase_facts = Vec::new();
                        if let Ok(chunk) = &result {
                            let semantic = if !chunk.delta.is_empty() {
                                Some("text")
                            } else if chunk
                                .reasoning_delta
                                .as_ref()
                                .is_some_and(|s| !s.is_empty())
                            {
                                Some("reasoning")
                            } else if chunk
                                .tool_calls
                                .iter()
                                .any(|call| !call.arguments.is_empty())
                            {
                                Some("tool_arguments")
                            } else if !chunk.tool_calls.is_empty() {
                                Some("tool_declaration")
                            } else {
                                None
                            };
                            for (enabled, phase, kind) in [
                                (!first_chunk, "first_normalized_chunk", None),
                                (
                                    !first_semantic && semantic.is_some(),
                                    "first_semantic_content",
                                    semantic,
                                ),
                            ] {
                                if enabled {
                                    let mut fact = pioneer_provider::usage::observation_point();
                                    fact["phase"] = phase.into();
                                    fact["semantic_kind"] = serde_json::json!(kind);
                                    fact["elapsed_ms"] = (call
                                        .prepared_mono
                                        .elapsed()
                                        .as_millis()
                                        .min(u64::MAX as u128)
                                        as u64)
                                        .into();
                                    fact["timing_basis"] = "since_preparation_not_prefill".into();
                                    phase_facts.push(fact);
                                }
                            }
                            first_chunk = true;
                            first_semantic |= semantic.is_some();
                        }
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
                        context.finalize_usage(&mut usage);
                        // Keep partial usage before handing it to a cancellable consumer.
                        if !matches!(&result, Ok(chunk) if chunk.usage.is_none() && !chunk.is_final)
                        {
                            if call.record(status, &usage).await.is_err() {
                                tracing::warn!("provider cumulative usage commit unconfirmed");
                                if let Some(accounting) = &mut usage.accounting {
                                    accounting["settlement_persistence"] = "unconfirmed".into();
                                }
                                let mut result = result;
                                if let Ok(chunk) = &mut result {
                                    chunk.usage = Some(usage.clone());
                                }
                                return Some((
                                    result,
                                    (
                                        stream,
                                        call,
                                        usage,
                                        terminal,
                                        context,
                                        first_chunk,
                                        first_semantic,
                                    ),
                                ));
                            }
                        }
                        // Capture the point while polling, commit numeric usage
                        // first, then acknowledge optional diagnostic boundaries.
                        for fact in phase_facts {
                            let _ = call.phase(fact).await;
                        }
                        Some((
                            result,
                            (
                                stream,
                                call,
                                usage,
                                terminal,
                                context,
                                first_chunk,
                                first_semantic,
                            ),
                        ))
                    }
                    None => {
                        context.finalize_usage(&mut usage);
                        if terminal != Some(false) {
                            let status = if terminal == Some(true) {
                                "completed"
                            } else {
                                "failed"
                            };
                            if call.record(status, &usage).await.is_err() {
                                tracing::warn!("provider terminal usage commit unconfirmed");
                                if let Some(accounting) = &mut usage.accounting {
                                    accounting["settlement_persistence"] = "unconfirmed".into();
                                }
                                return Some((
                                    Ok(StreamChunk::usage(usage.clone())),
                                    (
                                        stream,
                                        call,
                                        usage,
                                        Some(false),
                                        context,
                                        first_chunk,
                                        first_semantic,
                                    ),
                                ));
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
            message_delivery,
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
        let context = pioneer_provider::usage::UsageContext::prepare_operation(
            self.inner.as_ref(),
            &request.model,
            "embeddings",
        );
        let mut usage = context.initial_usage();
        let manifest = pioneer_provider::usage::embedding_request_manifest(&request)?;
        let start = Call::start_with_connection(
            &self.store,
            &self.workspace,
            self.operation,
            &self.owner,
            &usage,
            self.authority_fingerprint(),
            Some(manifest),
        );
        let call = match pioneer_provider::usage::current_causal_parent()
            .or_else(|| self.inherited_parent.clone())
        {
            Some(parent) => pioneer_provider::usage::scope_causal_parent(parent, start).await?,
            None => start.await?,
        };
        let response = context
            .scope_with_observations(self.inner.embed(request), |fact| call.phase(fact))
            .await?;
        match &response {
            Ok(response) => {
                if let Some(observed) = &response.usage {
                    usage.update(observed);
                }
            }
            Err(error) => {
                if let Some(observed) = pioneer_provider::usage::error_usage(error) {
                    usage.update(observed);
                }
            }
        }
        context.finalize_usage(&mut usage);
        let unconfirmed = call
            .record(
                if response.is_ok() {
                    "completed"
                } else {
                    "failed"
                },
                &usage,
            )
            .await
            .is_err();
        if unconfirmed {
            tracing::warn!("embedding usage settlement commit unconfirmed");
        }
        let response = response.map(|mut response| {
            if unconfirmed {
                if let Some(accounting) = &mut usage.accounting {
                    accounting["settlement_persistence"] = "unconfirmed".into();
                }
            }
            response.usage = Some(usage);
            response
        });
        response
    }
    async fn warmup(&self) -> Result<ProviderWarmupOutcome> {
        self.inner.warmup().await
    }
}
