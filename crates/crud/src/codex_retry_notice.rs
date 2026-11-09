//! Narrow, read-only presentation links for Codex retry notices.
use crate::CrudStore;
use anyhow::{Result, bail};
use pioneer_entity::{turn_cli_runtime_attempt as attempt, turn_item};
use pioneer_protocol::{
    CodexRetryNotice, SystemEventLevel, TaskAttachmentMode, TaskTurnItem, TurnItem, TurnKind,
};
use sea_orm::{ColumnTrait, Condition, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use std::collections::{HashMap, HashSet};

const NOTICE_READ_BATCH_SIZE: u64 = 64;

/// Shared by canonical projection and exact historical lookup.
pub fn codex_retry_notice_item_id(turn_id: &str, attempt_id: &str) -> String {
    crate::canonical_agent_id(
        'I',
        &format!("codex-request-retried:{turn_id}:{attempt_id}"),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexRetryNoticeTarget {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub task: TaskTurnItem,
}

/// Advisory exact item identities captured for one page. This is not an access
/// proof: the reader below still validates the source's complete Task linkage.
#[derive(Debug, Clone)]
pub struct CodexRetryNoticeRead {
    source_turn_id: String,
    item_ids: Vec<String>,
}

impl CrudStore {
    /// Bounded metadata-only discovery before child authorization or Task graph
    /// reconstruction. No payload/decompression, CLI mapping, or actor contract
    /// is read for a card with no canonical notice. Per-Turn high-water marks
    /// make this finite even while other attempts are being created.
    pub async fn codex_retry_notice_reads(
        &self,
        source_turn_ids: &[String],
    ) -> Result<HashMap<String, CodexRetryNoticeRead>> {
        let ids = source_turn_ids
            .iter()
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut reads = HashMap::new();
        for chunk in ids.chunks(NOTICE_READ_BATCH_SIZE as usize) {
            let frontiers = attempt::Entity::find()
                .select_only()
                .column(attempt::Column::TurnId)
                .column_as(attempt::Column::AttemptIndex.max(), "through_index")
                .filter(attempt::Column::TurnId.is_in(chunk.iter().cloned()))
                .filter(attempt::Column::RuntimeKind.eq("codex"))
                .group_by(attempt::Column::TurnId)
                .into_tuple::<(String, i64)>()
                .all(&self.connection)
                .await?;
            let mut after = None::<(String, i64)>;
            loop {
                if frontiers.is_empty() {
                    break;
                }
                let mut bounds = Condition::any();
                for (turn, through) in &frontiers {
                    if after.as_ref().is_some_and(|(id, _)| turn < id) {
                        continue;
                    }
                    let lower = after
                        .as_ref()
                        .filter(|(id, _)| id == turn)
                        .map_or(0, |(_, index)| *index);
                    bounds = bounds.add(
                        Condition::all()
                            .add(attempt::Column::TurnId.eq(turn.clone()))
                            .add(attempt::Column::AttemptIndex.gt(lower))
                            .add(attempt::Column::AttemptIndex.lte(*through)),
                    );
                }
                let attempts = attempt::Entity::find()
                    .select_only()
                    .columns([
                        attempt::Column::TurnId,
                        attempt::Column::Id,
                        attempt::Column::AttemptIndex,
                    ])
                    .filter(attempt::Column::RuntimeKind.eq("codex"))
                    .filter(bounds)
                    .order_by_asc(attempt::Column::TurnId)
                    .order_by_asc(attempt::Column::AttemptIndex)
                    .limit(NOTICE_READ_BATCH_SIZE)
                    .into_tuple::<(String, String, i64)>()
                    .all(&self.connection)
                    .await?;
                let Some((last_turn, _, last_index)) = attempts.last() else {
                    break;
                };
                after = Some((last_turn.clone(), *last_index));
                let mut exact = Condition::any();
                for (turn, id, _) in &attempts {
                    exact = exact.add(
                        Condition::all()
                            .add(turn_item::Column::TurnId.eq(turn.clone()))
                            .add(
                                turn_item::Column::ItemId.eq(codex_retry_notice_item_id(turn, id)),
                            ),
                    );
                }
                // Deliberately do not filter item_type here. A malformed exact
                // canonical item must still reach the existing validation error.
                let items = turn_item::Entity::find()
                    .select_only()
                    .columns([turn_item::Column::TurnId, turn_item::Column::ItemId])
                    .filter(exact)
                    .into_tuple::<(String, String)>()
                    .all(&self.connection)
                    .await?;
                for (turn, id) in items {
                    reads
                        .entry(turn.clone())
                        .or_insert_with(|| CodexRetryNoticeRead {
                            source_turn_id: turn,
                            item_ids: Vec::new(),
                        })
                        .item_ids
                        .push(id);
                }
                if attempts.len() < NOTICE_READ_BATCH_SIZE as usize {
                    break;
                }
            }
        }
        Ok(reads)
    }

    /// Discover only an exact primary execution -> run -> occurrence card link.
    /// Each point read releases database capacity before the next operation;
    /// callers reauthorize the source child before disclosing its notice body.
    pub async fn codex_retry_notice_target(
        &self,
        source_thread_id: &str,
        source_turn_id: &str,
    ) -> Result<Option<CodexRetryNoticeTarget>> {
        let Some(child) = self.get_thread_by_id(source_thread_id).await? else {
            return Ok(None);
        };
        if child.access_class != "internal" || child.sidebar_visibility != "hidden" {
            return Ok(None);
        }
        let Some(binding) = self
            .get_cli_runtime_turn_binding_control(source_turn_id)
            .await?
        else {
            return Ok(None);
        };
        if binding.thread_id != source_thread_id
            || binding.workspace_id != child.workspace_id
            || binding.runtime_kind != "codex"
        {
            return Ok(None);
        }
        let Some(turn) = self
            .get_task_run_turn_by_turn(source_thread_id, source_turn_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(execution_id) = turn.execution_id.as_deref() else {
            return Ok(None);
        };
        let Some(execution) = crate::repositories::task_run_execution::find_execution_by_id(
            &self.connection,
            execution_id,
        )
        .await?
        else {
            return Ok(None);
        };
        if execution.task_id != turn.task_id || execution.task_run_id != turn.run_id {
            return Ok(None);
        }
        let Some(primary) = self
            .get_task_run_primary_thread_binding(&turn.run_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(lineage) = self.get_task_thread_lineage(source_thread_id).await? else {
            return Ok(None);
        };
        let Some(run) = self.get_task_run(&turn.run_id).await? else {
            return Ok(None);
        };
        let Some(task) = self.get_task_record(&turn.task_id).await? else {
            return Ok(None);
        };
        let Some(occurrence) = self
            .get_task_occurrence_contract_by_run(&turn.run_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(parent_thread_id) = lineage.created_by_thread_id.as_deref() else {
            return Ok(None);
        };
        if primary.thread_id != source_thread_id
            || primary.task_id != turn.task_id
            || primary.execution_id.is_none()
            || primary.execution_id != turn.execution_id
            || run.task_id != turn.task_id
            || task.workspace_id != child.workspace_id
            || occurrence.task_id != turn.task_id
            || occurrence.run_id != turn.run_id
            || occurrence
                .delivery_plan
                .as_ref()
                .and_then(|plan| plan.presentation_thread_id.as_deref())
                != Some(parent_thread_id)
            || lineage.child_thread_id != source_thread_id
            || lineage.depth <= 0
            || lineage.origin_kind.as_deref() != Some("task_run")
            || lineage.created_by_turn_id.as_deref() != Some(turn.run_id.as_str())
        {
            return Ok(None);
        }
        let Some((workspace, parent_turn)) = self.get_turn(parent_thread_id, &turn.run_id).await?
        else {
            return Ok(None);
        };
        let Some(root) = self.get_thread_by_id(&lineage.root_thread_id).await? else {
            return Ok(None);
        };
        if workspace != child.workspace_id
            || root.workspace_id != workspace
            || root.access_class == "internal"
            || parent_turn.turn_kind != TurnKind::TaskRun
        {
            return Ok(None);
        }
        let anchor = self
            .list_turn_items_by_type(&turn.run_id, "task")
            .await?
            .into_iter()
            .find_map(|item| match item {
                TurnItem::Task { item }
                    if item.task_id == turn.task_id
                        && item.run_id.as_deref() == Some(turn.run_id.as_str())
                        && item.attachment == TaskAttachmentMode::Detached
                        && item.child_thread_id.as_deref() == Some(source_thread_id)
                        && item.child_turn_id.as_deref() == Some(source_turn_id) =>
                {
                    Some(item)
                }
                _ => None,
            });
        Ok(anchor.map(|task| CodexRetryNoticeTarget {
            workspace_id: workspace,
            thread_id: parent_thread_id.to_owned(),
            turn_id: turn.run_id,
            task,
        }))
    }

    pub async fn codex_retry_notices_for_task(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        task: &TaskTurnItem,
    ) -> Result<Vec<CodexRetryNotice>> {
        let (Some(_), Some(source_turn)) = (
            task.child_thread_id.as_deref(),
            task.child_turn_id.as_deref(),
        ) else {
            return Ok(Vec::new());
        };
        let mut reads = self
            .codex_retry_notice_reads(&[source_turn.to_owned()])
            .await?;
        let Some(read) = reads.remove(source_turn) else {
            return Ok(Vec::new());
        };
        self.codex_retry_notices_for_task_read(workspace_id, thread_id, turn_id, task, &read)
            .await
    }

    /// Called after child-observation authorization by presentation readers.
    /// Discovery is advisory only; revalidate all source/run/workspace links
    /// before loading any selected notice body. Concurrent appends are presented
    /// on the next invalidated page; deletion simply removes a selected item.
    pub async fn codex_retry_notices_for_task_read(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        task: &TaskTurnItem,
        read: &CodexRetryNoticeRead,
    ) -> Result<Vec<CodexRetryNotice>> {
        let targets = [CodexRetryNoticeTarget {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            task: task.clone(),
        }];
        let reads = HashMap::from([(read.source_turn_id.clone(), read.clone())]);
        Ok(self
            .codex_retry_notices_for_tasks(&targets, &reads)
            .await?
            .pop()
            .unwrap_or_default())
    }

    /// Page-local validation and reads. Every relation is loaded once per
    /// bounded group, using metadata projections rather than Task/actor payloads.
    /// Authorization remains the caller's responsibility, before this read.
    pub async fn codex_retry_notices_for_tasks(
        &self,
        targets: &[CodexRetryNoticeTarget],
        reads: &HashMap<String, CodexRetryNoticeRead>,
    ) -> Result<Vec<Vec<CodexRetryNotice>>> {
        use pioneer_entity::{
            task, task_occurrence_contract, task_run, task_run_execution, task_run_thread_binding,
            task_run_turn, thread, thread_lineage, turn, turn_cli_runtime_binding,
        };
        let mut result = Vec::with_capacity(targets.len());
        for targets in targets.chunks(NOTICE_READ_BATCH_SIZE as usize) {
            let sources = targets
                .iter()
                .filter_map(|t| t.task.child_turn_id.clone())
                .collect::<HashSet<_>>();
            // Each helper call releases its reader before validation/JSON work.
            macro_rules! facts {
                ($entity:ident, $filter:ident, $ids:expr, [$key:ident $(,$column:ident)*], $tuple:ty) => {{
                    $entity::Entity::find().select_only()
                        .columns([$entity::Column::$key $(,$entity::Column::$column)*])
                        .filter($entity::Column::$filter.is_in($ids))
                        .into_tuple::<$tuple>().all(&self.connection).await?
                }};
            }
            let bindings = facts!(
                turn_cli_runtime_binding,
                TurnId,
                sources.iter().cloned(),
                [TurnId, ThreadId, WorkspaceId, RuntimeKind],
                (String, String, String, String)
            )
            .into_iter()
            .map(|(id, t, w, k)| (id, (t, w, k)))
            .collect::<HashMap<_, _>>();
            let turns = facts!(
                task_run_turn,
                TurnId,
                sources.iter().cloned(),
                [TurnId, ThreadId, TaskId, RunId, ExecutionId],
                (String, String, String, String, Option<String>)
            )
            .into_iter()
            .map(|(id, t, task, run, e)| (id, (t, task, run, e)))
            .collect::<HashMap<_, _>>();
            let children = turns.values().map(|v| v.0.clone()).collect::<HashSet<_>>();
            let lineages = thread_lineage::Entity::find()
                .filter(thread_lineage::Column::ChildThreadId.is_in(children.iter().cloned()))
                .all(&self.connection)
                .await?
                .into_iter()
                .map(|l| (l.child_thread_id.clone(), l))
                .collect::<HashMap<_, _>>();
            let thread_ids = children
                .iter()
                .cloned()
                .chain(
                    lineages
                        .values()
                        .flat_map(|l| {
                            [
                                Some(l.root_thread_id.clone()),
                                l.created_by_thread_id.clone(),
                            ]
                        })
                        .flatten(),
                )
                .collect::<HashSet<_>>();
            let threads = facts!(
                thread,
                Id,
                thread_ids,
                [Id, WorkspaceId, AccessClass, SidebarVisibility],
                (String, String, String, String)
            )
            .into_iter()
            .map(|(id, w, a, v)| (id, (w, a, v)))
            .collect::<HashMap<_, _>>();
            let execution_ids = turns
                .values()
                .filter_map(|v| v.3.clone())
                .collect::<HashSet<_>>();
            let executions = facts!(
                task_run_execution,
                Id,
                execution_ids,
                [Id, TaskId, TaskRunId],
                (String, String, String)
            )
            .into_iter()
            .map(|(id, t, r)| (id, (t, r)))
            .collect::<HashMap<_, _>>();
            let run_ids = turns.values().map(|v| v.2.clone()).collect::<HashSet<_>>();
            let primary = task_run_thread_binding::Entity::find()
                .filter(task_run_thread_binding::Column::RunId.is_in(run_ids.iter().cloned()))
                .filter(task_run_thread_binding::Column::BindingKind.eq("primary_executor"))
                .all(&self.connection)
                .await?
                .into_iter()
                .map(|b| (b.run_id.clone(), b))
                .collect::<HashMap<_, _>>();
            let runs = facts!(
                task_run,
                Id,
                run_ids.iter().cloned(),
                [Id, TaskId],
                (String, String)
            )
            .into_iter()
            .collect::<HashMap<_, _>>();
            let task_ids = turns.values().map(|v| v.1.clone()).collect::<HashSet<_>>();
            let tasks = facts!(task, Id, task_ids, [Id, WorkspaceId], (String, String))
                .into_iter()
                .collect::<HashMap<_, _>>();
            let occurrence_rows = task_occurrence_contract::Entity::find()
                .filter(task_occurrence_contract::Column::RunId.is_in(run_ids.iter().cloned()))
                .all(&self.connection)
                .await?;
            let mut occurrences = HashMap::new();
            for row in occurrence_rows {
                let occurrence =
                    crate::repositories::task_actor_contract::task_occurrence_contract_from_model(
                        row,
                    )?;
                occurrences.insert(occurrence.run_id.clone(), occurrence);
            }
            let parent_turns = facts!(
                turn,
                Id,
                run_ids,
                [Id, ThreadId, TurnKind],
                (String, String, String)
            )
            .into_iter()
            .map(|(id, t, k)| (id, (t, k)))
            .collect::<HashMap<_, _>>();
            let mut exact = Condition::any();
            for target in targets {
                exact = exact.add(
                    Condition::all()
                        .add(turn_item::Column::TurnId.eq(target.turn_id.clone()))
                        .add(turn_item::Column::ItemId.eq(target.task.id.clone())),
                );
            }
            let anchors = turn_item::Entity::find()
                .filter(exact)
                .all(&self.connection)
                .await?
                .into_iter()
                .map(|row| ((row.turn_id.clone(), row.item_id.clone()), row))
                .collect::<HashMap<_, _>>();
            let mut eligible = Vec::with_capacity(targets.len());
            for target in targets {
                let valid = (|| -> Result<bool> {
                    let card = &target.task;
                    let (Some(child), Some(source)) = (&card.child_thread_id, &card.child_turn_id)
                    else {
                        return Ok(false);
                    };
                    if !reads
                        .get(source)
                        .is_some_and(|read| read.source_turn_id == *source)
                    {
                        return Ok(false);
                    }
                    let (
                        Some((bt, bw, bk)),
                        Some((tt, task_id, run_id, execution_id)),
                        Some(l),
                        Some(ct),
                    ) = (
                        bindings.get(source),
                        turns.get(source),
                        lineages.get(child),
                        threads.get(child),
                    )
                    else {
                        return Ok(false);
                    };
                    let (
                        Some(e),
                        Some(p),
                        Some(run_task),
                        Some(task_workspace),
                        Some(occurrence),
                        Some((pt, pk)),
                    ) = (
                        execution_id.as_ref().and_then(|id| executions.get(id)),
                        primary.get(run_id),
                        runs.get(run_id),
                        tasks.get(task_id),
                        occurrences.get(run_id),
                        parent_turns.get(run_id),
                    )
                    else {
                        return Ok(false);
                    };
                    let Some(parent) = l.created_by_thread_id.as_ref() else {
                        return Ok(false);
                    };
                    let (Some(root), Some(parent_thread)) =
                        (threads.get(&l.root_thread_id), threads.get(parent))
                    else {
                        return Ok(false);
                    };
                    if ct.1 != "internal"
                        || ct.2 != "hidden"
                        || bt != child
                        || tt != child
                        || bw != &ct.0
                        || bk != "codex"
                        || e != &(task_id.clone(), run_id.clone())
                        || p.thread_id != *child
                        || p.task_id != *task_id
                        || p.execution_id.is_none()
                        || &p.execution_id != execution_id
                        || run_task != task_id
                        || task_workspace != &ct.0
                        || occurrence.task_id != *task_id
                        || l.depth <= 0
                        || l.origin_kind.as_deref() != Some("task_run")
                        || l.created_by_turn_id.as_deref() != Some(run_id.as_str())
                        || pt != parent
                        || pk != "task_run"
                        || parent_thread.0 != ct.0
                        || root.0 != ct.0
                        || root.1 == "internal"
                        || target.workspace_id != ct.0
                        || target.thread_id != *parent
                        || target.turn_id != *run_id
                    {
                        return Ok(false);
                    }
                    if occurrence
                        .delivery_plan
                        .as_ref()
                        .and_then(|p| p.presentation_thread_id.as_ref())
                        != Some(parent)
                    {
                        return Ok(false);
                    }
                    let Some(anchor) = anchors.get(&(target.turn_id.clone(), card.id.clone()))
                    else {
                        return Ok(false);
                    };
                    if anchor.item_type != "task" {
                        return Ok(false);
                    }
                    let TurnItem::Task { item } =
                        serde_json::from_str::<TurnItem>(&anchor.payload)?
                    else {
                        return Ok(false);
                    };
                    Ok(item.id == card.id
                        && item.task_id == *task_id
                        && item.task_id == card.task_id
                        && item.run_id.as_ref() == Some(run_id)
                        && item.run_id == card.run_id
                        && item.attachment == TaskAttachmentMode::Detached
                        && item.attachment == card.attachment
                        && item.child_thread_id == card.child_thread_id
                        && item.child_turn_id == card.child_turn_id)
                })()?;
                eligible.push(valid);
            }
            let mut selected = HashSet::new();
            for (target, valid) in targets.iter().zip(&eligible) {
                if *valid {
                    let source = target
                        .task
                        .child_turn_id
                        .as_ref()
                        .expect("validated child Turn");
                    for id in &reads[source].item_ids {
                        selected.insert((source.clone(), id.clone()));
                    }
                }
            }
            let selected = selected.into_iter().collect::<Vec<_>>();
            let mut notices = HashMap::<String, Vec<CodexRetryNotice>>::new();
            for batch in selected.chunks(NOTICE_READ_BATCH_SIZE as usize) {
                let mut exact = Condition::any();
                for (turn, id) in batch {
                    exact = exact.add(
                        Condition::all()
                            .add(turn_item::Column::TurnId.eq(turn.clone()))
                            .add(turn_item::Column::ItemId.eq(id.clone())),
                    );
                }
                let rows = turn_item::Entity::find()
                    .filter(exact)
                    .all(&self.connection)
                    .await?;
                for row in rows {
                    let TurnItem::SystemEvent {
                        id,
                        level: SystemEventLevel::Warning,
                        code: Some(code),
                        message,
                        details: None,
                    } = serde_json::from_str::<TurnItem>(&row.payload)?
                    else {
                        bail!("invalid canonical Codex retry notice")
                    };
                    if code != "codex_request_retried"
                        || id != row.item_id
                        || row.item_type != "system_event"
                    {
                        bail!("invalid canonical Codex retry notice")
                    }
                    notices
                        .entry(row.turn_id.clone())
                        .or_default()
                        .push(CodexRetryNotice {
                            source_thread_id: turns[&row.turn_id].0.clone(),
                            source_turn_id: row.turn_id,
                            item_id: id,
                            message,
                            occurred_at_unix_ms: row.created_at.timestamp_millis(),
                        });
                }
            }
            for notices in notices.values_mut() {
                notices.sort_by(|a, b| {
                    (a.occurred_at_unix_ms, &a.item_id).cmp(&(b.occurred_at_unix_ms, &b.item_id))
                });
            }
            for (target, valid) in targets.iter().zip(eligible) {
                result.push(if valid {
                    notices
                        .get(
                            target
                                .task
                                .child_turn_id
                                .as_ref()
                                .expect("validated child Turn"),
                        )
                        .cloned()
                        .unwrap_or_default()
                } else {
                    Vec::new()
                });
            }
        }
        Ok(result)
    }
}
