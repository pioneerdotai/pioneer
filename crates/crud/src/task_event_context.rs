//! Point context for Task notifications and anchors, without a Task aggregate.
use crate::{CrudStore, repositories::task_trigger, task_trigger_from_db_model};
use anyhow::{Context, Result};
use pioneer_protocol::{Task, TaskEventPayload, TaskRun, TaskTrigger};
use sea_orm::{ColumnTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder, QuerySelect};

#[derive(Clone, Debug, FromQueryResult)]
pub struct TaskAnchorAgent {
    pub agent_role: Option<String>,
    pub depth: i64,
    pub max_depth: i64,
}
#[derive(Clone, Debug)]
pub struct TaskAnchorContext {
    pub run: Option<TaskRun>,
    pub trigger: Option<TaskTrigger>,
    pub agent: Option<TaskAnchorAgent>,
}
#[derive(Clone, Debug)]
pub struct TaskEventContext {
    pub task: Task,
    pub run: Option<TaskRun>,
    pub run_trigger: Option<TaskTrigger>,
    pub scheduled_trigger: Option<TaskTrigger>,
}
impl TaskEventContext {
    pub fn run_uses_creation_anchor(&self, run_id: &str) -> bool {
        self.task.created_by_turn_id.is_some()
            && self.run.as_ref().is_some_and(|run| run.id == run_id)
            && attached_immediate(&self.task, self.run_trigger.as_ref())
    }
}
fn attached_immediate(task: &Task, trigger: Option<&TaskTrigger>) -> bool {
    task.lifecycle_policy
        .as_ref()
        .is_some_and(|p| p.attachment == pioneer_protocol::TaskAttachmentMode::Attached)
        && trigger.is_some_and(|t| t.kind() == pioneer_protocol::TaskTriggerKind::Immediate)
}
impl CrudStore {
    pub async fn task_run_has_pending_thread_delivery(
        &self,
        workspace_id: &str,
        task_id: &str,
        run_id: &str,
        occurrence_thread_id: Option<&str>,
    ) -> Result<bool> {
        let targets = crate::repositories::task_delivery::pending_thread_targets(
            &self.connection,
            workspace_id,
            task_id,
            run_id,
        )
        .await?;
        for (mode, target) in targets {
            if crate::convention::task_delivery_mode_from_db(&mode)
                .context("invalid delivery mode")?
                == pioneer_protocol::TaskDeliveryMode::Thread
                && target.as_deref() == occurrence_thread_id
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
    async fn fanout_trigger(&self, id: Option<&str>) -> Result<Option<TaskTrigger>> {
        match id {
            Some(id) => task_trigger::find_trigger_by_id(&self.connection, id)
                .await?
                .map(task_trigger_from_db_model)
                .transpose(),
            None => Ok(None),
        }
    }
    async fn anchor_agent(
        &self,
        task_id: &str,
        run_id: Option<&str>,
    ) -> Result<Option<TaskAnchorAgent>> {
        use pioneer_entity::task_agent_spec as spec;
        let query = || {
            spec::Entity::find().select_only().columns([
                spec::Column::AgentRole,
                spec::Column::Depth,
                spec::Column::MaxDepth,
            ])
        };
        if let Some(run_id) = run_id {
            let exact = query()
                .filter(spec::Column::RunId.eq(run_id))
                .filter(spec::Column::TaskId.eq(task_id))
                .order_by_desc(spec::Column::CreatedAt)
                .limit(1)
                .into_model::<TaskAnchorAgent>()
                .one(&self.connection)
                .await?;
            if exact.is_some() {
                return Ok(exact);
            }
        }
        Ok(query()
            .filter(spec::Column::TaskId.eq(task_id))
            .order_by_desc(spec::Column::CreatedAt)
            .limit(1)
            .into_model::<TaskAnchorAgent>()
            .one(&self.connection)
            .await?)
    }
    pub async fn get_task_event_context(
        &self,
        payload: &TaskEventPayload,
    ) -> Result<Option<TaskEventContext>> {
        let Some(task) = self.get_task_record(payload.task_id()).await? else {
            return Ok(None);
        };
        let run = match payload.run_id() {
            Some(id) => self
                .get_task_run(id)
                .await?
                .filter(|r| r.task_id == task.id),
            None => None,
        };
        let run_trigger = self
            .fanout_trigger(run.as_ref().and_then(|r| r.trigger_id.as_deref()))
            .await?
            .filter(|t| t.task_id == task.id);
        let scheduled_trigger = match payload {
            TaskEventPayload::TaskScheduled { trigger_id, .. } => self
                .fanout_trigger(Some(trigger_id))
                .await?
                .filter(|t| t.task_id == task.id),
            _ => None,
        };
        Ok(Some(TaskEventContext {
            task,
            run,
            run_trigger,
            scheduled_trigger,
        }))
    }
    // Metadata-only latest seek. An unrelated latest Run's result is never
    // loaded merely to decide whether an event refreshes the creation anchor.
    async fn latest_creation_anchor_run(
        &self,
        task: &Task,
    ) -> Result<Option<(String, TaskTrigger)>> {
        if !task
            .lifecycle_policy
            .as_ref()
            .is_some_and(|p| p.attachment == pioneer_protocol::TaskAttachmentMode::Attached)
        {
            return Ok(None);
        }
        use pioneer_entity::task_run as run;
        let latest = run::Entity::find()
            .select_only()
            .columns([run::Column::Id, run::Column::TriggerId])
            .filter(run::Column::TaskId.eq(task.id.clone()))
            .order_by_desc(run::Column::RunNumber)
            .limit(1)
            .into_tuple::<(String, Option<String>)>()
            .one(&self.connection)
            .await?;
        let Some((id, trigger_id)) = latest else {
            return Ok(None);
        };
        Ok(self
            .fanout_trigger(trigger_id.as_deref())
            .await?
            .filter(|t| {
                t.task_id == task.id && t.kind() == pioneer_protocol::TaskTriggerKind::Immediate
            })
            .map(|trigger| (id, trigger)))
    }
    pub async fn latest_task_run_uses_creation_anchor(&self, task: &Task) -> Result<bool> {
        Ok(self.latest_creation_anchor_run(task).await?.is_some())
    }
    pub async fn get_task_creation_anchor_context(&self, task: &Task) -> Result<TaskAnchorContext> {
        let (run, trigger) = match self.latest_creation_anchor_run(task).await? {
            Some((id, trigger)) => (self.get_task_run(&id).await?, Some(trigger)),
            None => (None, None),
        };
        self.get_task_run_anchor_context(task, run.as_ref(), trigger.as_ref())
            .await
    }
    pub async fn get_task_run_anchor_context(
        &self,
        task: &Task,
        run: Option<&TaskRun>,
        trigger: Option<&TaskTrigger>,
    ) -> Result<TaskAnchorContext> {
        let trigger = match trigger {
            Some(t) => Some(t.clone()),
            None => {
                use pioneer_entity::task_trigger as trigger;
                trigger::Entity::find()
                    .filter(trigger::Column::TaskId.eq(task.id.clone()))
                    .order_by_desc(trigger::Column::CreatedAt)
                    .limit(1)
                    .one(&self.connection)
                    .await?
                    .map(task_trigger_from_db_model)
                    .transpose()?
            }
        };
        let agent = self
            .anchor_agent(&task.id, run.map(|r| r.id.as_str()))
            .await?;
        Ok(TaskAnchorContext {
            run: run.cloned(),
            trigger,
            agent,
        })
    }
}
