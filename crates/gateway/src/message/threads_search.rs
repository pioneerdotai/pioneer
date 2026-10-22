//! The model supplies only a query and pagination. All authority is server-owned.
use super::MessageProcessor;
use crate::authorization::RevalidatedExecutionAuthorization;
use crate::thread_episodic::EpisodicSearchAuthority;
use pioneer_agent::TurnToolContext;
use pioneer_tools::{ToolError, threads_search::ThreadsSearchParams};

impl MessageProcessor {
    pub(crate) async fn search_threads_for_tool(
        &self,
        context: &TurnToolContext,
        params: ThreadsSearchParams,
        _current: &RevalidatedExecutionAuthorization,
    ) -> Result<serde_json::Value, ToolError> {
        let authority = EpisodicSearchAuthority::resolve_for_turn(
            &self.crud_store,
            &context.workspace_id,
            &context.turn_id,
            None,
        )
        .await
        .map_err(|_| {
            ToolError::execution_failed("thread search is unavailable or access denied")
        })?;
        let page = self
            .thread_episodic_recall_service
            .search_threads(
                authority,
                &params.query,
                params.limit as usize,
                params.cursor.as_deref(),
            )
            .await
            .map_err(|error| {
                // Storage/provider details and denied candidate identifiers never
                // cross the tool boundary. Cursor errors remain actionable.
                if error.to_string().contains("cursor")
                    || error.to_string().contains("start a new search")
                {
                    ToolError::invalid_arguments(
                        "invalid or stale search cursor; start a new search",
                    )
                } else {
                    ToolError::execution_failed("thread search is temporarily unavailable")
                }
            })?;
        serde_json::to_value(page)
            .map_err(|_| ToolError::execution_failed("thread search is temporarily unavailable"))
    }
}
