use crate::CrudStore;
use pioneer_crud_execution::tool_output;
pub use pioneer_crud_execution::tool_output::ToolOutputChunk;

impl CrudStore {
    pub async fn record_tool_output(
        &self,
        id: &str,
        n: &pioneer_protocol::ItemDeltaNotification,
    ) -> anyhow::Result<()> {
        tool_output::record_output(&self.connection, id, n).await
    }

    pub async fn tool_output_page(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
        after: i64,
    ) -> anyhow::Result<Vec<(i64, ToolOutputChunk)>> {
        self.run_scoped_database_quantum(|| async {
            tool_output::page(&self.connection, workspace, thread, turn, item, after).await
        })
        .await
    }
}
