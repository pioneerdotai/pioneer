//! The model supplies only a query and pagination. All authority is server-owned.
use super::MessageProcessor;
use crate::authorization::ResourceAction;
use crate::thread_episodic::EpisodicSearchAuthority;
use pioneer_agent::TurnToolContext;
use pioneer_tools::{ToolError, threads_search::ThreadsSearchParams};

impl MessageProcessor {
    /// Materialization and dispatch use the same fresh Turn/user/Agent intersection.
    pub(crate) async fn thread_search_authority(
        &self,
        context: &TurnToolContext,
    ) -> anyhow::Result<EpisodicSearchAuthority> {
        let current = self
            .revalidate_tool_execution_authorization(
                &context.workspace_id,
                &context.thread_id,
                &context.turn_id,
                None,
                ResourceAction::ThreadRead,
            )
            .await?;
        let principal = current
            .initiating_principal(self.crud_store.as_ref())
            .await?;
        let binding = self
            .agent_action_binding(&context.turn_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("episodic Agent authority unavailable"))?;
        let facts = binding.adapter.lock().await.persistence_facts();
        EpisodicSearchAuthority::resolve_for_turn(
            &self.crud_store,
            &context.workspace_id,
            &context.turn_id,
            Some(principal.principal_id.as_str()),
        )
        .await?
        .restrict_to_agent_binding(&facts)
    }

    pub(crate) async fn search_threads_for_tool(
        &self,
        context: &TurnToolContext,
        params: ThreadsSearchParams,
    ) -> Result<serde_json::Value, ToolError> {
        let authority = self.thread_search_authority(context).await.map_err(|_| {
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
