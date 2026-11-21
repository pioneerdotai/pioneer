use crate::CrudStore;
use anyhow::Result;
use pioneer_compaction::SourceRef;
use sea_orm::TransactionTrait;

impl CrudStore {
    pub async fn shell_control_receipt(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> Result<Option<String>> {
        pioneer_crud_conversations::context_cleanup::shell_control_receipt(
            &self.connection,
            workspace,
            thread,
            turn,
            item,
        )
        .await
    }

    /// Prepare the model payload before admission. Its terminal source revision
    /// and absence of a canonical result are revalidated atomically with append.
    pub async fn commit_recovered_shell_result(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
        terminal_source: &SourceRef,
        entry: crate::NewTurnLlmContextEntry,
    ) -> Result<crate::TurnLlmContextEntry> {
        use sea_orm::TransactionTrait;
        anyhow::ensure!(
            entry.turn_id == turn && entry.item_id.as_deref() == Some(item),
            "recovery result scope mismatch"
        );
        let delivery_key = format!("shell-recovery-v1:{item}");
        let changes = [
            self.connection.work_signal("tasks.lifecycle").changing(),
            self.connection.work_signal("maintenance.frozen").changing(),
            self.connection.work_signal("maintenance.html").changing(),
        ];
        let outcome = self
            .run_scoped_database_quantum(|| {
                let entry = entry.clone();
                let delivery_key = delivery_key.clone();
                async move {
                    let transaction = self.connection.begin().await?;
                    anyhow::ensure!(
                        pioneer_crud_compaction::tool_sources::compaction_tool_item_reference(
                            &transaction,
                            workspace,
                            thread,
                            turn,
                            item
                        )
                        .await?
                        .as_ref()
                            == Some(terminal_source),
                        "terminal shell source changed before recovery commit"
                    );
                    let outcome =
                        crate::store::conversations::context_writes::append_recovered_shell_result(
                            &transaction,
                            entry,
                            &delivery_key,
                        )
                        .await?;
                    transaction.commit().await?;
                    Ok(outcome)
                }
            })
            .await;
        if let Ok(outcome) = &outcome {
            for change in changes {
                change.completed(!outcome.already_committed);
            }
        }
        outcome.map(|outcome| outcome.entry)
    }
}
