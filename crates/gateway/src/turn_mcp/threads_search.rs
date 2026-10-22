//! Execution-bound observation adapter shared by native and frozen CLI catalogs.
use crate::message::MessageProcessor;
use async_trait::async_trait;
use pioneer_agent::TurnToolContext;
use pioneer_tools::threads_search::{
    THREADS_SEARCH_TOOL_NAME, ThreadsSearchParams, threads_search_tool_spec,
};
use pioneer_tools::{
    FunctionToolOutput, ToolError, ToolEventTrace, ToolExtensionBundle, ToolHandler,
    ToolInvocation, ToolOutput, ToolPayload,
};
use std::sync::Arc;

pub(crate) async fn materialize(
    processor: Arc<MessageProcessor>,
    context: TurnToolContext,
) -> Option<ToolExtensionBundle> {
    processor.thread_search_authority(&context).await.ok()?;
    Some(ToolExtensionBundle {
        specs: vec![threads_search_tool_spec()],
        handlers: vec![(
            THREADS_SEARCH_TOOL_NAME.to_owned(),
            Arc::new(ThreadsSearchHandler { processor, context }),
        )],
    })
}

struct ThreadsSearchHandler {
    processor: Arc<MessageProcessor>,
    context: TurnToolContext,
}

#[async_trait]
impl ToolHandler for ThreadsSearchHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _: ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let ToolPayload::Function { arguments } = invocation.payload else {
            return Err(ToolError::invalid_arguments(
                "threads_search requires function arguments",
            ));
        };
        let params: ThreadsSearchParams = serde_json::from_value(arguments)
            .map_err(|_| ToolError::invalid_arguments("invalid threads_search arguments"))?;
        params.validate()?;
        // The callback repeats the exact materialization checks at dispatch.
        let result = self
            .processor
            .search_threads_for_tool(&self.context, params)
            .await?;
        Ok(Box::new(FunctionToolOutput::with_payload(
            "thread search results",
            true,
            result,
        )))
    }
}
