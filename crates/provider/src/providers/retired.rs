//! Tombstones retain configuration identities without contacting retired APIs.
use crate::{
    ChatRequest, ChatResponse, Provider, ProviderCapabilities, ProviderWarmupOutcome, StreamChunk,
};
use anyhow::Result;
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use pioneer_protocol::ProviderModelInfo;

pub(crate) struct RetiredProvider {
    name: &'static str,
    reason: &'static str,
}

impl RetiredProvider {
    pub(crate) fn new(name: &'static str, reason: &'static str) -> Self {
        Self { name, reason }
    }
}

#[async_trait]
impl Provider for RetiredProvider {
    fn name(&self) -> &str {
        self.name
    }

    fn capabilities(&self) -> ProviderCapabilities {
        let mut capabilities = ProviderCapabilities::default();
        capabilities.input_types.text = false;
        capabilities
    }

    async fn prepare_input_budget(
        &self,
        _request: ChatRequest,
    ) -> Result<crate::attachments::PreparedInputBudget> {
        anyhow::bail!(self.reason)
    }

    async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse> {
        anyhow::bail!(self.reason)
    }

    async fn stream_chat(
        &self,
        _request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        anyhow::bail!(self.reason)
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        anyhow::bail!(self.reason)
    }

    async fn warmup(&self) -> Result<ProviderWarmupOutcome> {
        anyhow::bail!(self.reason)
    }
}
