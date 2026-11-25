//! Task lists, bounded hierarchy and scheduler agenda.

use anyhow::{Context, Result};
use pioneer_entity::{task as task_entity, task_trigger as trigger_entity};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::{
    Task, TaskAgendaItem, TaskAgendaParams, TaskAgendaResponse, TaskAgentSpec, TaskDependency,
    TaskListParams, TaskRun, TaskStatus, TaskTree, TaskTrigger, TaskTriggerKind, TaskTriggerSpec,
    TaskWriteLock,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, JoinType, NullOrdering, Order};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
};
use std::collections::HashMap;

use crate::delivery::{self as task_delivery, task_delivery_from_db_model};
use crate::status::{task_owner_kind_to_db, task_status_to_db, task_trigger_kind_to_db};
use crate::task::{
    self as task_repository, TaskRootAccessFilter, apply_root_access_filter, task_from_db_model,
};
use crate::task_agent_spec::{self, task_agent_spec_from_db_model};
use crate::task_dependency::{self, task_dependency_from_db_model};
use crate::task_run::{self, task_run_from_db_model};
use crate::task_trigger::{self, task_trigger_from_db_model};
use crate::values::unix_to_datetime;
use crate::write_lock::{self as task_write_lock, task_write_lock_from_db_model};

fn trigger_timezone(spec: &TaskTriggerSpec) -> Option<String> {
    match spec {
        TaskTriggerSpec::ScheduledAt { timezone, .. } => timezone.clone(),
        TaskTriggerSpec::Cron { timezone, .. } => Some(timezone.clone()),
        _ => None,
    }
}

fn bounded_preview(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let preview = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

fn build_task_tree(
    task: Task,
    children_by_parent: &mut HashMap<String, Vec<Task>>,
    triggers_by_task: &mut HashMap<String, Vec<TaskTrigger>>,
    runs_by_task: &mut HashMap<String, Vec<TaskRun>>,
    specs_by_task: &mut HashMap<String, Vec<TaskAgentSpec>>,
    dependencies_by_task: &mut HashMap<String, Vec<TaskDependency>>,
    write_locks_by_task: &mut HashMap<String, Vec<TaskWriteLock>>,
) -> TaskTree {
    let task_id = task.id.clone();
    let children = children_by_parent
        .remove(task_id.as_str())
        .unwrap_or_default()
        .into_iter()
        .map(|child| {
            build_task_tree(
                child,
                children_by_parent,
                triggers_by_task,
                runs_by_task,
                specs_by_task,
                dependencies_by_task,
                write_locks_by_task,
            )
        })
        .collect();

    TaskTree {
        task,
        triggers: triggers_by_task
            .remove(task_id.as_str())
            .unwrap_or_default(),
        runs: runs_by_task.remove(task_id.as_str()).unwrap_or_default(),
        agent_specs: specs_by_task.remove(task_id.as_str()).unwrap_or_default(),
        dependencies: dependencies_by_task
            .remove(task_id.as_str())
            .unwrap_or_default(),
        write_locks: write_locks_by_task
            .remove(task_id.as_str())
            .unwrap_or_default(),
        children,
    }
}

pub async fn list_tasks_scoped(
    db: &PostgresDatabase,
    params: TaskListParams,
    access: Option<&TaskRootAccessFilter>,
) -> Result<Vec<Task>> {
    let limit = params.limit.map(u64::from);
    let offset = params.cursor;
    let rows = if let Some(parent_task_id) = params.parent_task_id.as_deref() {
        task_repository::list_tasks_by_parent_scoped(db, parent_task_id, access, limit, offset)
            .await?
    } else if let Some(root_task_id) = params.root_task_id.as_deref() {
        task_repository::list_tasks_by_root_scoped(db, root_task_id, access, limit, offset).await?
    } else if let Some(owner_kind) = params.owner_kind {
        let owner_kind = task_owner_kind_to_db(owner_kind);
        task_repository::list_tasks_by_owner_scoped(
            db,
            params.workspace_id.as_str(),
            owner_kind,
            params.owner_id.as_deref(),
            limit,
            offset,
            access,
        )
        .await?
    } else {
        let status = params.status.map(task_status_to_db);
        task_repository::list_tasks_by_workspace_status_scoped(
            db,
            params.workspace_id.as_str(),
            status,
            limit,
            offset,
            access,
        )
        .await?
    };

    rows.into_iter().map(task_from_db_model).collect()
}
pub async fn get_task_tree_bounded(
    db: &PostgresDatabase,
    task_id: &str,
    max_nodes: usize,
) -> Result<Option<TaskTree>> {
    if max_nodes == 0 {
        anyhow::bail!("task tree node limit must be positive");
    }
    let Some(root_model) = task_repository::find_task_by_id(db, task_id).await? else {
        return Ok(None);
    };

    let mut task_models = vec![root_model.clone()];
    let maximum_children = max_nodes - 1;
    let query_limit = (max_nodes != usize::MAX)
        .then(|| u64::try_from(maximum_children.saturating_add(1)).unwrap_or(u64::MAX));
    let mut child_models = task_repository::list_tasks_by_root(db, task_id, query_limit).await?;
    if child_models.len().saturating_add(1) > max_nodes {
        anyhow::bail!("task tree exceeds server node budget of {max_nodes}");
    }
    task_models.append(&mut child_models);

    let task_ids = task_models
        .iter()
        .map(|model| model.id.clone())
        .collect::<Vec<_>>();

    let mut triggers_by_task: HashMap<String, Vec<TaskTrigger>> = HashMap::new();
    let mut runs_by_task: HashMap<String, Vec<TaskRun>> = HashMap::new();
    let mut specs_by_task: HashMap<String, Vec<TaskAgentSpec>> = HashMap::new();
    let mut dependencies_by_task: HashMap<String, Vec<TaskDependency>> = HashMap::new();
    let mut write_locks_by_task: HashMap<String, Vec<TaskWriteLock>> = HashMap::new();

    for task_id in &task_ids {
        triggers_by_task.insert(
            task_id.clone(),
            task_trigger::list_triggers_by_task(db, task_id)
                .await?
                .into_iter()
                .map(task_trigger_from_db_model)
                .collect::<Result<Vec<_>>>()?,
        );
        runs_by_task.insert(
            task_id.clone(),
            task_run::list_runs_by_task(db, task_id)
                .await?
                .into_iter()
                .map(task_run_from_db_model)
                .collect::<Result<Vec<_>>>()?,
        );
        specs_by_task.insert(
            task_id.clone(),
            task_agent_spec::list_agent_specs_by_task(db, task_id)
                .await?
                .into_iter()
                .map(task_agent_spec_from_db_model)
                .collect::<Result<Vec<_>>>()?,
        );
        dependencies_by_task.insert(
            task_id.clone(),
            task_dependency::list_dependencies_for_task(db, task_id)
                .await?
                .into_iter()
                .map(task_dependency_from_db_model)
                .collect::<Result<Vec<_>>>()?,
        );
        write_locks_by_task.insert(
            task_id.clone(),
            task_write_lock::list_locks_by_task(db, task_id)
                .await?
                .into_iter()
                .map(task_write_lock_from_db_model)
                .collect::<Result<Vec<_>>>()?,
        );
    }

    let mut children_by_parent: HashMap<String, Vec<Task>> = HashMap::new();
    for model in task_models {
        let task = task_from_db_model(model)?;
        if let Some(parent_task_id) = task.parent_task_id.clone() {
            children_by_parent
                .entry(parent_task_id)
                .or_default()
                .push(task);
        }
    }

    let root = task_from_db_model(root_model)?;
    Ok(Some(build_task_tree(
        root,
        &mut children_by_parent,
        &mut triggers_by_task,
        &mut runs_by_task,
        &mut specs_by_task,
        &mut dependencies_by_task,
        &mut write_locks_by_task,
    )))
}
pub async fn list_task_agenda_scoped(
    db: &PostgresDatabase,
    params: TaskAgendaParams,
    access: Option<&TaskRootAccessFilter>,
) -> Result<TaskAgendaResponse> {
    let limit = std::cmp::max(params.limit.unwrap_or(100), 1);
    let reader = db.begin_read().await?;
    let ids = agenda_page(&reader, &params, access, u64::from(limit)).await?;
    let mut items = Vec::with_capacity(ids.len());
    for (task_id, trigger_id) in ids {
        let task = task_repository::find_task_by_id(&reader, &task_id)
            .await?
            .context("task disappeared from the agenda snapshot")?;
        let task = task_from_db_model(task)?;
        let trigger = trigger_entity::Entity::find_by_id(trigger_id)
            .one(&reader)
            .await?
            .context("trigger disappeared from the agenda snapshot")?;
        let trigger = task_trigger_from_db_model(trigger)?;
        let latest_run = task_run::find_latest_numbered_run(&reader, task.id.as_str())
            .await?
            .map(task_run_from_db_model)
            .transpose()?;
        let latest_delivery = task_delivery::list_deliveries_for_task(&reader, task.id.as_str())
            .await?
            .into_iter()
            .map(task_delivery_from_db_model)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .last();
        let result = latest_run
            .as_ref()
            .and_then(|run| run.result.as_ref())
            .or(task.result.as_ref());
        let error = latest_run
            .as_ref()
            .and_then(|run| run.error.as_ref())
            .or(task.error.as_ref());
        items.push(TaskAgendaItem {
            goal_preview: Some(bounded_preview(task.goal.as_str(), 240)),
            trigger_kind: Some(trigger.kind()),
            trigger_status: Some(trigger.status),
            next_fire_at: trigger.next_fire_at,
            last_fire_at: trigger.last_fire_at,
            timezone: trigger_timezone(&trigger.spec),
            recurring: matches!(
                trigger.kind(),
                TaskTriggerKind::Interval | TaskTriggerKind::Cron
            ),
            delivery_mode: task
                .delivery_policy
                .as_ref()
                .map(|policy| policy.mode)
                .unwrap_or(pioneer_protocol::TaskDeliveryMode::None),
            result_preview: result.and_then(|result| result.summary.clone()),
            error_preview: error.map(|error| {
                error
                    .recovery_public_message()
                    .unwrap_or_else(|| bounded_preview(error.message.as_str(), 240))
            }),
            task,
            trigger: Some(trigger),
            latest_run,
            latest_delivery,
        });
    }
    reader.commit().await?;
    Ok(TaskAgendaResponse { items })
}

// Use PostgreSQL LATERAL so the selected trigger and task filters precede
// LIMIT. The work done by the hydration loop remains bounded by the page.
async fn agenda_page<C: ConnectionTrait>(
    db: &C,
    params: &TaskAgendaParams,
    access: Option<&TaskRootAccessFilter>,
    limit: u64,
) -> Result<Vec<(String, String)>> {
    use task_entity::Column as TaskField;
    use trigger_entity::Column as TriggerField;

    let latest = trigger_entity::Entity::find()
        .select_only()
        .columns([TriggerField::Id, TriggerField::NextFireAt])
        .filter(
            Expr::col((trigger_entity::Entity, TriggerField::TaskId))
                .eq(Expr::col((task_entity::Entity, TaskField::Id))),
        )
        .order_by_desc(TriggerField::CreatedAt)
        .order_by_desc(TriggerField::Id)
        .limit(1);
    let mut trigger = latest;
    if !params.include_paused {
        trigger = trigger.filter(TriggerField::Status.ne("paused"));
    }
    if !params.trigger_kinds.is_empty() {
        trigger = trigger.filter(
            TriggerField::Kind.is_in(
                params
                    .trigger_kinds
                    .iter()
                    .copied()
                    .map(task_trigger_kind_to_db),
            ),
        );
    }
    if let Some(from) = params.from {
        trigger = trigger.filter(
            TriggerField::NextFireAt
                .is_null()
                .or(TriggerField::NextFireAt.gte(unix_to_datetime(from))),
        );
    }
    if let Some(to) = params.to {
        trigger = trigger.filter(
            TriggerField::NextFireAt
                .is_null()
                .or(TriggerField::NextFireAt.lte(unix_to_datetime(to))),
        );
    }
    let latest = Alias::new("latest_trigger");
    let mut tasks = apply_root_access_filter(
        task_entity::Entity::find()
            .select_only()
            .column(TaskField::Id)
            .column_as(Expr::col((latest.clone(), TriggerField::Id)), "trigger_id")
            .filter(TaskField::WorkspaceId.eq(&params.workspace_id)),
        access,
    );
    if let Some(owner) = params.owner_kind {
        tasks = tasks.filter(TaskField::OwnerKind.eq(task_owner_kind_to_db(owner)));
        if let Some(id) = &params.owner_id {
            tasks = tasks.filter(TaskField::OwnerId.eq(id));
        }
    }
    if !params.include_completed {
        tasks = tasks.filter(
            crate::projection::effective_task_status_expr().is_not_in(
                [
                    TaskStatus::Completed,
                    TaskStatus::Failed,
                    TaskStatus::Blocked,
                    TaskStatus::Cancelled,
                ]
                .map(task_status_to_db),
            ),
        );
    }
    if !params.statuses.is_empty() {
        tasks = tasks.filter(
            crate::projection::effective_task_status_expr()
                .is_in(params.statuses.iter().copied().map(task_status_to_db)),
        );
    }
    QueryTrait::query(&mut tasks)
        .join_lateral(
            JoinType::InnerJoin,
            trigger.into_query(),
            latest.clone(),
            Expr::val(true),
        )
        .order_by_expr_with_nulls(
            Expr::col((latest, TriggerField::NextFireAt)),
            Order::Asc,
            NullOrdering::Last,
        )
        .order_by((task_entity::Entity, TaskField::CreatedAt), Order::Asc)
        .order_by((task_entity::Entity, TaskField::Id), Order::Asc);
    tasks
        .limit(limit)
        .into_tuple::<(String, String)>()
        .all(db)
        .await
        .context("read task agenda")
}
