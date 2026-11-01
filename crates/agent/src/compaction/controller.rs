//! Native execution boundary. Gateway owns persistence, settings and service
//! transports; the agent retains the actual request receipt and measured input.
use anyhow::Result;
use async_trait::async_trait;
use pioneer_compaction::{InputEstimate, RequestIdentity, SourceRef};
use pioneer_provider::{ChatRequest, Provider, TokenUsage};
use pioneer_runtime_events::ExecutionEventHub;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct NativeContext {
    pub overflow_recovery: bool,
    pub recovery_deadline_ms: Option<u64>,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub conversation_thread_id: Option<String>,
    pub provider_instance: String,
    pub provider: Arc<dyn Provider>,
    pub events: Arc<ExecutionEventHub>,
    pub cancellation: CancellationToken,
}
#[derive(Clone)]
pub struct NativeInputReceipt {
    pub identity: RequestIdentity,
    /// Exact serialized-message identities including source versions. This is
    /// metadata only; the provider prompt is never copied into a usage record.
    pub messages: Vec<SourceRef>,
}
impl NativeInputReceipt {
    /// Bind calibration to the entire request shape, the provider's authority,
    /// and the exact message prefix. Hashes contain no transcript or credentials.
    pub fn for_request(
        request: &ChatRequest,
        provider_instance: &str,
        api_format: &str,
        projection_version: u64,
        checkpoint: Option<String>,
    ) -> Result<Self> {
        let digest = |value: &serde_json::Value| -> Result<String> {
            Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
        };
        let instructions_hash = digest(&serde_json::json!({
            "system_sections": request.compiled_prompt.as_ref().map(|v|v.system_sections()),
            // This is request-level provider context, omitted from per-message
            // append estimates. Replacing it invalidates an otherwise exact
            // message-prefix anchor rather than silently charging only the tail.
            "native_prefix_context": request.messages.iter().rev()
                .filter_map(|message| message.provider_replay_state.as_ref())
                .find_map(|state| state.payload.get("prefix_context")),
        }))?;
        let tools_hash = digest(&serde_json::json!({
            "tools": request.tools, "tool_choice": request.tool_choice,
            "parallel_tool_calls": request.parallel_tool_calls,
            "reasoning": request.reasoning.map(|v|format!("{v:?}")),
            "output_cap": request.max_tokens, "temperature": request.temperature,
        }))?;
        let messages = request
            .messages
            .iter()
            .enumerate()
            .map(|(index, message)| {
                let mut hash = Sha256::new();
                hash.update(serde_json::to_vec(message)?);
                if let Some(origin) = &message.provenance {
                    hash.update(serde_json::to_vec(origin)?);
                }
                Ok(SourceRef {
                    scope: "native-request-message".into(),
                    id: index.to_string(),
                    version: hex::encode(hash.finalize()),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            identity: RequestIdentity {
                provider_instance: provider_instance.into(),
                model: request.model.clone(),
                api_format: api_format.into(),
                instructions_hash,
                tools_hash,
                projection_version,
                checkpoint,
            },
            messages,
        })
    }

    /// Stable identity for admission, including retained runtime messages that
    /// have no durable source locator. Store only a digest, never their text.
    pub fn target_fingerprint(&self) -> Result<String> {
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
            &self.identity,
            &self.messages,
        ))?)))
    }

    pub fn calibrated_input(
        &self,
        local_complete: u64,
        local_messages: &[u64],
        measured: Option<&NativeUsageMeasurement>,
    ) -> Result<u64> {
        Ok(self
            .calibrated_estimate(local_complete, local_messages, measured)?
            .total())
    }

    /// The receipt covers normalized provider input only. Its generated answer
    /// and subsequent tool results are unmeasured additions to this exact prefix.
    pub fn calibrated_estimate(
        &self,
        local_complete: u64,
        local_messages: &[u64],
        measured: Option<&NativeUsageMeasurement>,
    ) -> Result<InputEstimate> {
        anyhow::ensure!(
            local_messages.len() == self.messages.len(),
            "missing message estimates"
        );
        let Some(measured) = measured.filter(|m| {
            self.identity == m.receipt.identity && self.messages.starts_with(&m.receipt.messages)
        }) else {
            return Ok(InputEstimate::local(local_complete));
        };
        let appended = local_messages[measured.receipt.messages.len()..]
            .iter()
            .fold(0_u64, |sum, value| sum.saturating_add(*value));
        Ok(InputEstimate {
            measured: measured.input_tokens,
            estimated: appended,
        })
    }
}

#[derive(Clone)]
pub struct NativeUsageMeasurement {
    pub receipt: NativeInputReceipt,
    pub input_tokens: u64,
}
pub struct NativePreparedRequest {
    pub request: ChatRequest,
    pub receipt: NativeInputReceipt,
    /// Compact budgeting metadata captured while the provider request is
    /// already materialized. It contains no messages or request payload.
    pub history_check: NativeHistoryCheckMetadata,
}
#[derive(Clone, Debug)]
pub struct NativeHistoryCheckMetadata {
    pub target_output_cap: u32,
    pub fixed_input_tokens: u64,
}

#[async_trait]
pub trait NativeContextController: Send + Sync {
    /// Persist the service Stop fence before its owned future is cancelled.
    async fn stop(&self, context: &NativeContext) -> Result<()>;
    async fn prepare(
        &self,
        context: &NativeContext,
        request: ChatRequest,
        measurement: Option<NativeUsageMeasurement>,
        recovery: bool,
    ) -> Result<NativePreparedRequest>;
}

pub struct NativeContextSession {
    pub context: NativeContext,
    controller: Arc<dyn NativeContextController>,
    measurement: Mutex<Option<NativeUsageMeasurement>>,
    recovery_pending: std::sync::atomic::AtomicBool,
    recovery_active: std::sync::atomic::AtomicBool,
    recovery_deadline: Option<tokio::time::Instant>,
}
impl NativeContextSession {
    pub fn new(context: NativeContext, controller: Arc<dyn NativeContextController>) -> Self {
        let recovery = context.overflow_recovery;
        let recovery_deadline = context.recovery_deadline_ms.map(|deadline| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u64::MAX as u128) as u64;
            tokio::time::Instant::now()
                + std::time::Duration::from_millis(deadline.saturating_sub(now))
        });
        Self {
            context,
            controller,
            measurement: Mutex::new(None),
            recovery_pending: std::sync::atomic::AtomicBool::new(recovery),
            recovery_active: std::sync::atomic::AtomicBool::new(recovery),
            recovery_deadline,
        }
    }
    pub async fn prepare(
        &self,
        request: ChatRequest,
        recovery: bool,
    ) -> Result<NativePreparedRequest> {
        let _startup_context = pioneer_observability::turn_startup::current_stage(
            pioneer_observability::turn_startup::Stage::ContextPrepare,
        );
        let pending = self
            .recovery_pending
            .swap(false, std::sync::atomic::Ordering::SeqCst);
        let recovery = recovery || pending;
        if recovery {
            anyhow::ensure!(
                self.recovery_deadline.is_some(),
                "recovery has no durable operation deadline"
            );
        }
        let measurement = self
            .measurement
            .lock()
            .map_err(|_| anyhow::anyhow!("native usage state unavailable"))?
            .clone();
        let prepare = self
            .controller
            .prepare(&self.context, request, measurement, recovery);
        let work = async {
            if recovery {
                let deadline = self.recovery_deadline.expect("validated recovery deadline");
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "context recovery deadline exceeded before preparation"
                );
                tokio::time::timeout_at(deadline, prepare)
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!("context recovery deadline exceeded during preparation")
                    })?
            } else {
                prepare.await
            }
        };
        let prepared = tokio::select! { biased;
            _ = self.context.cancellation.cancelled() => anyhow::bail!("native context preparation cancelled"),
            result = work => result,
        }?;
        Ok(prepared)
    }
    pub fn provider_deadline(&self) -> Option<tokio::time::Instant> {
        self.recovery_active
            .load(std::sync::atomic::Ordering::SeqCst)
            .then_some(self.recovery_deadline)
            .flatten()
    }
    pub fn record_usage(
        &self,
        receipt: NativeInputReceipt,
        usage: Option<&TokenUsage>,
    ) -> Result<()> {
        self.recovery_active
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let measurement = usage
            .and_then(|value| value.input_tokens)
            .map(|input_tokens| NativeUsageMeasurement {
                receipt,
                input_tokens,
            });
        *self
            .measurement
            .lock()
            .map_err(|_| anyhow::anyhow!("native usage state unavailable"))? = measurement;
        Ok(())
    }
    pub async fn stop(&self) -> Result<()> {
        let result = self.controller.stop(&self.context).await;
        self.context.cancellation.cancel();
        result
    }
}

impl std::fmt::Debug for NativeContextSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeContextSession")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_provider::{ChatMessage, ReasoningConfig, ReasoningEffort, ToolDefinition};
    fn request() -> ChatRequest {
        ChatRequest {
            model: "m".into(),
            messages: vec![ChatMessage::user("a")],
            temperature: None,
            max_tokens: Some(100),
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        }
    }
    fn receipt(request: &ChatRequest) -> NativeInputReceipt {
        NativeInputReceipt::for_request(request, "instance", "api/authority", 0, None).unwrap()
    }
    struct UnusedBoundary;
    #[async_trait]
    impl NativeContextController for UnusedBoundary {
        async fn stop(&self, _: &NativeContext) -> Result<()> {
            unreachable!("usage bookkeeping does not stop")
        }
        async fn prepare(
            &self,
            _: &NativeContext,
            _: ChatRequest,
            _: Option<NativeUsageMeasurement>,
            _: bool,
        ) -> Result<NativePreparedRequest> {
            unreachable!("usage bookkeeping does not prepare")
        }
    }
    #[async_trait]
    impl Provider for UnusedBoundary {
        fn name(&self) -> &str {
            "fixture"
        }
        async fn chat(&self, _: ChatRequest) -> Result<pioneer_provider::ChatResponse> {
            unreachable!("usage bookkeeping does not call provider")
        }
        async fn stream_chat(
            &self,
            _: ChatRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, Result<pioneer_provider::StreamChunk>>>
        {
            unreachable!("usage bookkeeping does not stream")
        }
    }
    #[test]
    fn recorded_normalized_usage_does_not_add_cache_output_or_reasoning_and_unknown_clears_anchor()
    {
        let context = NativeContext {
            overflow_recovery: false,
            recovery_deadline_ms: None,
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            conversation_thread_id: None,
            provider_instance: "instance".into(),
            provider: Arc::new(UnusedBoundary),
            events: Arc::new(ExecutionEventHub::new()),
            cancellation: CancellationToken::new(),
        };
        let session = NativeContextSession::new(context, Arc::new(UnusedBoundary));
        let receipt = receipt(&request());
        let mut usage = TokenUsage {
            input_tokens: Some(200),
            output_tokens: Some(100),
            cache_read_input_tokens: Some(150),
            cache_write_input_tokens: Some(20),
            reasoning_tokens: Some(80),
            reported_total_tokens: Some(999),
            ..Default::default()
        };
        session.record_usage(receipt.clone(), Some(&usage)).unwrap();
        {
            let measured = session.measurement.lock().unwrap();
            assert_eq!(
                receipt
                    .calibrated_estimate(900, &[20], measured.as_ref())
                    .unwrap(),
                InputEstimate {
                    measured: 200,
                    estimated: 0
                }
            );
        }
        usage.input_tokens = None;
        session.record_usage(receipt.clone(), Some(&usage)).unwrap();
        let measured = session.measurement.lock().unwrap();
        assert!(measured.is_none());
        assert_eq!(
            receipt
                .calibrated_estimate(900, &[20], measured.as_ref())
                .unwrap(),
            InputEstimate::local(900)
        );
    }

    #[test]
    fn changing_native_prefix_context_invalidates_input_anchor() {
        let mut request = request();
        request.messages[0].provider_replay_state =
            Some(pioneer_provider::ProviderReplayState::for_model(
                "fixture",
                "m",
                serde_json::json!({"prefix_context":{"prefix":"old"}}),
            ));
        let measured = NativeUsageMeasurement {
            receipt: receipt(&request),
            input_tokens: 200,
        };
        let mut appended = ChatMessage::assistant("new answer");
        appended.provider_replay_state = Some(pioneer_provider::ProviderReplayState::for_model(
            "fixture",
            "m",
            serde_json::json!({"prefix_context":{"prefix":"replacement"}}),
        ));
        request.messages.push(appended);
        assert_eq!(
            receipt(&request)
                .calibrated_estimate(900, &[20, 30], Some(&measured))
                .unwrap(),
            InputEstimate::local(900)
        );
    }

    #[test]
    fn usage_anchor_covers_input_only_and_missing_or_shortened_prefix_is_local() {
        let mut request = request();
        let measured = NativeUsageMeasurement {
            receipt: receipt(&request),
            input_tokens: 200,
        };
        request
            .messages
            .push(ChatMessage::assistant("generated answer"));
        request
            .messages
            .push(ChatMessage::tool_result("call", "tool", "new result"));
        let current = receipt(&request);
        let estimate = current
            .calibrated_estimate(900, &[20, 30, 40], Some(&measured))
            .unwrap();
        assert_eq!(
            estimate,
            InputEstimate {
                measured: 200,
                estimated: 70
            }
        );
        assert_eq!(estimate.padded(), 281);
        assert_eq!(
            current
                .calibrated_estimate(900, &[20, 30, 40], None)
                .unwrap(),
            InputEstimate::local(900)
        );
        let shortened = NativeUsageMeasurement {
            receipt: current,
            input_tokens: 400,
        };
        request.messages.pop();
        assert_eq!(
            receipt(&request)
                .calibrated_estimate(80, &[20, 30], Some(&shortened))
                .unwrap(),
            InputEstimate::local(80)
        );
    }

    #[test]
    fn native_target_identity_tracks_unattributed_runtime_messages() {
        let mut request = request();
        request
            .messages
            .insert(0, ChatMessage::system("runtime instructions"));
        let original = receipt(&request);
        assert_eq!(
            original.target_fingerprint().unwrap(),
            receipt(&request).target_fingerprint().unwrap()
        );
        for index in 0..request.messages.len() {
            let mut changed = request.clone();
            changed.messages[index].content.push_str(" changed");
            let changed = receipt(&changed);
            // Message changes are not part of the calibration shape, but must
            // invalidate a previous failed admission for the complete target.
            assert_eq!(original.identity, changed.identity);
            assert_ne!(
                original.target_fingerprint().unwrap(),
                changed.target_fingerprint().unwrap()
            );
        }
    }

    #[test]
    fn native_usage_prefix_requires_exact_request_shape_and_versions() {
        let mut request = request();
        let measurement = NativeUsageMeasurement {
            receipt: receipt(&request),
            input_tokens: 200,
        };
        request.messages.push(ChatMessage::assistant("answer"));
        request.messages.push(ChatMessage::user("next"));
        assert_eq!(
            receipt(&request)
                .calibrated_input(100, &[20, 30, 40], Some(&measurement))
                .unwrap(),
            270
        );
        assert_eq!(
            receipt(&request)
                .calibrated_input(400, &[20, 30, 40], Some(&measurement))
                .unwrap(),
            270
        );
        for change in 0..4 {
            let mut changed = request.clone();
            match change {
                0 => changed.messages[0] = ChatMessage::user("edited"),
                1 => changed.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High)),
                2 => changed.max_tokens = Some(101),
                _ => {
                    changed.tools = Some(vec![ToolDefinition {
                        name: "tool".into(),
                        description: "tool".into(),
                        parameters: serde_json::json!({"type":"object"}),
                    }])
                }
            }
            assert_eq!(
                receipt(&changed)
                    .calibrated_input(100, &[20, 30, 40], Some(&measurement))
                    .unwrap(),
                100
            );
        }
        let mut with_origin = request.clone();
        with_origin.messages[0].provenance = Some(super::super::history::pending_origin(
            "ws",
            "thread",
            "turn",
            "input",
            super::super::history::PendingOriginKind::Input,
            "input",
        ));
        assert_eq!(
            receipt(&with_origin)
                .calibrated_input(100, &[20, 30, 40], Some(&measurement))
                .unwrap(),
            100
        );
        let changed = NativeInputReceipt::for_request(
            &request,
            "instance",
            "api/authority",
            1,
            Some("cp".into()),
        )
        .unwrap();
        assert_eq!(
            changed
                .calibrated_input(100, &[20, 30, 40], Some(&measurement))
                .unwrap(),
            100
        );
        assert!(
            changed
                .calibrated_input(100, &[], Some(&measurement))
                .is_err()
        );
    }
}
