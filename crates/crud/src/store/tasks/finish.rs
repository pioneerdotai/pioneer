//! A manual Task finish shares its commit with the associated agent action.
use crate::store::execution::agent_transactions::commit_agent_action;
use crate::{AgentCommitInput, AppendedTaskEvent, CrudStore, GatewayControlWake};
use anyhow::Result;
use pioneer_crud_execution::agent_action_commit::agent_action_commit_exists;
use pioneer_crud_tasks::event_records::materialize_task_events;
use pioneer_crud_tasks::finish::FinishPreparation;
use sea_orm::TransactionTrait;

impl CrudStore {
    pub async fn request_task_finish(
        &self,
        task_id: &str,
        reason: Option<String>,
        at: i64,
        action: Option<crate::AgentCommitInput>,
    ) -> Result<(bool, Vec<AppendedTaskEvent>)> {
        let Some(action) = action else {
            return self
                .task_event_store()
                .request_task_finish(task_id, reason, at)
                .await;
        };
        let change = pioneer_crud_tasks::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            [task_id.to_owned()],
        );
        let result = async {
            self.retry_write_on_conflict(|| {
                self.request_task_finish_once(task_id, reason.clone(), at, &action)
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(pioneer_crud_tasks::work_change::inserted_task_events(
                &value.1,
            ));
        }
        result
    }

    async fn request_task_finish_once(
        &self,
        task_id: &str,
        reason: Option<String>,
        at: i64,
        action: &AgentCommitInput,
    ) -> Result<(bool, Vec<AppendedTaskEvent>)> {
        let tasks = self.task_event_store();
        for _ in 0..16 {
            let prepared = match tasks.prepare_finish(task_id, reason.clone(), at).await? {
                FinishPreparation::Existing { already_terminal } => {
                    let events = self
                        .append_task_events_with_agent_action(Vec::new(), at, action.clone())
                        .await?;
                    return Ok((already_terminal, events));
                }
                FinishPreparation::Apply(prepared) => prepared,
            };
            let control_change = self.control_work_change(
                "gateway.control",
                &[GatewayControlWake::Resources, GatewayControlWake::Actions],
            );
            let tx = self.connection.begin().await?;
            let result = async {
                let Some((closed, events)) = tasks
                    .apply_finish_in_connection(&tx, task_id, *prepared, at)
                    .await?
                else {
                    return Ok(None);
                };
                let changed = !agent_action_commit_exists(&tx, action).await?;
                commit_agent_action(&tx, action).await?;
                Ok(Some((closed, events, changed)))
            }
            .await;
            match result {
                Ok(Some((closed, events, changed))) => {
                    tx.commit().await?;
                    control_change.completed(changed);
                    return Ok((closed, materialize_task_events(events)));
                }
                Ok(None) => {
                    tx.rollback().await?;
                    control_change.completed(false);
                }
                Err(error) => {
                    let _ = tx.rollback().await;
                    return Err(error);
                }
            }
        }
        anyhow::bail!("Task changed repeatedly during finish preparation")
    }

    pub async fn list_task_finish_queue_candidates(&self, limit: u64) -> Result<Vec<String>> {
        self.task_event_store()
            .list_task_finish_queue_candidates(limit)
            .await
    }

    pub async fn discard_finished_task_queue(
        &self,
        task_id: &str,
        at: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        self.task_event_store()
            .discard_finished_task_queue(task_id, at)
            .await
    }
}
