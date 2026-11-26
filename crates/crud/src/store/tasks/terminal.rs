//! Task terminal transitions use the configured frozen output binder.
use crate::CrudStore;
use anyhow::Result;
pub use pioneer_crud_tasks::terminal::{
    PreparedTaskTerminalTransition, TaskTerminalCommitOutcome, TaskTerminalCommitStatus,
    TaskTerminalConflict,
};
use pioneer_protocol::{TaskEventPayload, TaskRunTurn};

impl CrudStore {
    pub async fn prepare_task_terminal_with_revision(
        &self,
        prepared: PreparedTaskTerminalTransition,
        revision: TaskRunTurn,
    ) -> Result<PreparedTaskTerminalTransition> {
        self.task_event_store()
            .prepare_task_terminal_with_revision(prepared, revision)
            .await
    }

    pub async fn prepare_task_terminal_with_extraction_failure(
        &self,
        prepared: PreparedTaskTerminalTransition,
        turn: TaskRunTurn,
    ) -> Result<PreparedTaskTerminalTransition> {
        self.task_event_store()
            .prepare_task_terminal_with_extraction_failure(prepared, turn)
            .await
    }

    pub async fn prepare_task_terminal_transition(
        &self,
        terminal: TaskEventPayload,
    ) -> Result<PreparedTaskTerminalTransition> {
        self.task_event_store()
            .prepare_task_terminal_transition(terminal)
            .await
    }

    pub async fn commit_task_terminal_transition(
        &self,
        prepared: PreparedTaskTerminalTransition,
    ) -> Result<TaskTerminalCommitOutcome> {
        self.task_event_store()
            .commit_task_terminal_transition(prepared)
            .await
    }
}
