use anyhow::{Context, Result, bail};
use pioneer_entity::{
    self_improvement_source_turn, task, task_delivery, task_result_candidate, task_run,
    task_run_conversation_snapshot, task_run_thread_binding, task_run_turn, thread, thread_lineage,
    turn,
};
use pioneer_protocol::{
    ItemCompletedNotification, TASK_COMPOSER_WORK_VERSION, TaskDeliveryMode, TaskMetadata,
    TaskResult, TaskResultCandidateStatus, TaskRunStatus, TaskRunThreadBindingKind,
    TaskRunTurnKind, TaskStatus, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
    TurnCompletedNotification, TurnItem, TurnKind, TurnOrigin, TurnStatus,
    task_delivery_id_from_result_item_id,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, JoinType, QueryFilter, QueryOrder, QuerySelect, Set,
};

use super::membership::{
    PersistedThreadAccessClass, persisted_thread_access_class_from_db,
    persisted_thread_access_class_to_db,
};
use crate::SelfImprovementSourceTurnRecord;
use crate::convention::{
    is_terminal_task_run_status_db, is_terminal_task_status_db, task_delivery_mode_to_db,
    task_delivery_thread_target_to_db, task_result_candidate_status_to_db, task_run_status_to_db,
    task_run_thread_binding_kind_to_db, task_run_turn_kind_from_db, task_status_to_db,
    thread_origin_kind_from_db, thread_origin_kind_to_db, thread_sidebar_visibility_from_db,
    thread_sidebar_visibility_to_db, turn_kind_from_db, turn_kind_to_db, turn_origin_from_db,
    turn_origin_to_db, turn_status_from_db, turn_status_to_db,
};
use crate::util::unix_to_datetime;

#[derive(Debug)]
pub(crate) struct VerifiedCollaborativeExchange {
    pub delivery_id: String,
    pub delivery_turn_id: String,
    pub task_id: String,
    pub run_id: String,
    pub accepted_task_run_turn_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedSelfImprovementSourceTurn {
    row: self_improvement_source_turn::ActiveModel,
    workspace_id: String,
    thread_id: String,
    turn_id: String,
    task_delivery_id: Option<String>,
    terminal_event_id: String,
    parent_turn_created_at_unix: i64,
}

pub(crate) async fn prepare_completed_source_turn<C: ConnectionTrait>(
    db: &C,
    terminal_event_id: &str,
    terminal_at: DateTimeWithTimeZone,
    notification: &TurnCompletedNotification,
) -> Result<Option<PreparedSelfImprovementSourceTurn>> {
    let thread = thread::Entity::find_by_id(notification.thread_id.clone())
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to load thread `{}` for self-improvement source projection",
                notification.thread_id
            )
        })?
        .with_context(|| {
            format!(
                "self-improvement source projection cannot find thread `{}`",
                notification.thread_id
            )
        })?;

    if thread.workspace_id != notification.workspace_id {
        bail!(
            "self-improvement source projection workspace mismatch for thread `{}`: expected `{}`, got `{}`",
            notification.thread_id,
            thread.workspace_id,
            notification.workspace_id
        );
    }
    if !eligible_source_thread(&thread) {
        return Ok(None);
    }

    let eligible = matches!(
        thread.origin_kind.as_str(),
        value
            if value == thread_origin_kind_to_db(ThreadOriginKind::DirectMessage)
                || value == thread_origin_kind_to_db(ThreadOriginKind::User)
    ) && notification.turn.turn_kind == TurnKind::Conversation
        && notification.turn.origin == TurnOrigin::User
        && notification.turn.mode != ThreadMode::Message
        && notification.turn.status == TurnStatus::Completed;
    if !eligible {
        return Ok(None);
    }

    let row = self_improvement_source_turn::ActiveModel {
        id: sea_orm::ActiveValue::NotSet,
        workspace_id: Set(thread.workspace_id.clone()),
        thread_id: Set(notification.thread_id.clone()),
        turn_id: Set(notification.turn.id.clone()),
        task_delivery_id: Set(None),
        terminal_event_id: Set(terminal_event_id.to_owned()),
        terminal_at: Set(terminal_at),
        created_at: Set(terminal_at),
    };

    let parent_turn_created_at_unix = turn::Entity::find_by_id(notification.turn.id.clone())
        .one(db)
        .await
        .context("failed to load foreground source parent turn")?
        .context("foreground source parent turn is missing after completion")?
        .created_at
        .timestamp();

    Ok(Some(PreparedSelfImprovementSourceTurn {
        row,
        workspace_id: thread.workspace_id,
        thread_id: notification.thread_id.clone(),
        turn_id: notification.turn.id.clone(),
        task_delivery_id: None,
        terminal_event_id: terminal_event_id.to_owned(),
        parent_turn_created_at_unix,
    }))
}

pub(crate) async fn prepare_completed_collaborative_source_exchange<C: ConnectionTrait>(
    db: &C,
    terminal_event_id: &str,
    terminal_at: DateTimeWithTimeZone,
    notification: &ItemCompletedNotification,
) -> Result<Option<PreparedSelfImprovementSourceTurn>> {
    let TurnItem::AgentMessage { id: item_id, .. } = &notification.item else {
        return Ok(None);
    };
    let Some(task_delivery_id) = task_delivery_id_from_result_item_id(item_id.as_str()) else {
        return Ok(None);
    };

    let Some(parent_thread) = thread::Entity::find_by_id(notification.thread_id.clone())
        .one(db)
        .await
        .context("failed to load collaborative source parent thread")?
    else {
        return Ok(None);
    };
    if parent_thread.workspace_id != notification.workspace_id {
        bail!(
            "collaborative self-improvement source workspace mismatch for thread `{}`",
            notification.thread_id
        );
    }
    if !eligible_source_thread(&parent_thread) {
        return Ok(None);
    }
    if parent_thread.origin_kind != thread_origin_kind_to_db(ThreadOriginKind::Collaborative) {
        return Ok(None);
    }

    let Some(parent_turn_id) = collaborative_parent_turn_id_for_delivery(
        db,
        notification.workspace_id.as_str(),
        notification.thread_id.as_str(),
        task_delivery_id,
    )
    .await?
    else {
        return Ok(None);
    };
    let Some(parent_turn) = turn::Entity::find_by_id(parent_turn_id.clone())
        .one(db)
        .await
        .context("failed to load collaborative source parent turn")?
    else {
        return Ok(None);
    };
    if parent_turn.thread_id != notification.thread_id
        || parent_turn.status != turn_status_to_db(TurnStatus::Completed)
        || parent_turn.turn_kind != turn_kind_to_db(TurnKind::Conversation)
        || parent_turn.origin != turn_origin_to_db(TurnOrigin::User)
    {
        return Ok(None);
    }

    if verify_collaborative_exchange(
        db,
        notification.workspace_id.as_str(),
        notification.thread_id.as_str(),
        parent_turn_id.as_str(),
        task_delivery_id,
        notification.turn_id.as_str(),
        true,
    )
    .await?
    .is_none()
    {
        return Ok(None);
    }

    let row = self_improvement_source_turn::ActiveModel {
        id: sea_orm::ActiveValue::NotSet,
        workspace_id: Set(notification.workspace_id.clone()),
        thread_id: Set(notification.thread_id.clone()),
        turn_id: Set(parent_turn_id.clone()),
        task_delivery_id: Set(Some(task_delivery_id.to_owned())),
        terminal_event_id: Set(terminal_event_id.to_owned()),
        terminal_at: Set(terminal_at),
        created_at: Set(terminal_at),
    };

    Ok(Some(PreparedSelfImprovementSourceTurn {
        row,
        workspace_id: notification.workspace_id.clone(),
        thread_id: notification.thread_id.clone(),
        turn_id: parent_turn_id,
        task_delivery_id: Some(task_delivery_id.to_owned()),
        terminal_event_id: terminal_event_id.to_owned(),
        parent_turn_created_at_unix: parent_turn.created_at.timestamp(),
    }))
}

pub(crate) async fn apply_prepared_source_turn<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedSelfImprovementSourceTurn,
) -> Result<SelfImprovementSourceTurnRecord> {
    self_improvement_source_turn::Entity::insert(prepared.row)
        .on_conflict(OnConflict::new().do_nothing().to_owned())
        .exec_without_returning(db)
        .await
        .context("failed to insert prepared self-improvement source turn")?;

    let by_turn = self_improvement_source_turn::Entity::find()
        .filter(self_improvement_source_turn::Column::TurnId.eq(prepared.turn_id.clone()))
        .one(db)
        .await
        .context("failed to verify prepared self-improvement source turn identity")?
        .context("prepared self-improvement source turn is missing after idempotent insert")?;
    if by_turn.terminal_event_id != prepared.terminal_event_id
        || by_turn.workspace_id != prepared.workspace_id
        || by_turn.thread_id != prepared.thread_id
        || by_turn.task_delivery_id != prepared.task_delivery_id
    {
        bail!(
            "self-improvement source identity conflict for turn `{}`",
            prepared.turn_id
        );
    }
    Ok(record_from_model(
        by_turn,
        prepared.parent_turn_created_at_unix,
    ))
}

async fn collaborative_parent_turn_id_for_delivery<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    parent_thread_id: &str,
    delivery_id: &str,
) -> Result<Option<String>> {
    let Some(delivery) = task_delivery::Entity::find_by_id(delivery_id.to_owned())
        .one(db)
        .await
        .context("failed to resolve collaborative source delivery")?
    else {
        return Ok(None);
    };
    if delivery.workspace_id != workspace_id
        || delivery.mode != task_delivery_mode_to_db(TaskDeliveryMode::Thread)
        || delivery.thread_target.as_deref()
            != Some(task_delivery_thread_target_to_db(
                pioneer_protocol::TaskDeliveryThreadTarget::OriginThread,
            ))
        || delivery.target_thread_id.as_deref() != Some(parent_thread_id)
    {
        return Ok(None);
    }

    let Some(task) = task::Entity::find_by_id(delivery.task_id)
        .one(db)
        .await
        .context("failed to resolve collaborative source task")?
    else {
        return Ok(None);
    };
    let metadata = task
        .metadata_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<TaskMetadata>(json).ok());
    let Some(composer_work) = metadata
        .as_ref()
        .and_then(|metadata| metadata.composer_work.as_ref())
    else {
        return Ok(None);
    };
    if task.workspace_id != workspace_id
        || task.created_by_thread_id.as_deref() != Some(parent_thread_id)
        || task.created_by_turn_id.as_deref() != Some(composer_work.launch.turn_id.as_str())
        || !metadata
            .as_ref()
            .is_some_and(|metadata| metadata.labels.iter().any(|label| label == "composer"))
        || composer_work.version != TASK_COMPOSER_WORK_VERSION
        || composer_work.launch.thread_id != parent_thread_id
        || composer_work.launch.turn_id.trim().is_empty()
    {
        return Ok(None);
    }
    Ok(Some(composer_work.launch.turn_id.clone()))
}

pub(crate) async fn verify_collaborative_exchange<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    parent_thread_id: &str,
    parent_turn_id: &str,
    delivery_id: &str,
    delivery_turn_id: &str,
    expected_success: bool,
) -> Result<Option<VerifiedCollaborativeExchange>> {
    let Some(delivery) = task_delivery::Entity::find_by_id(delivery_id.to_owned())
        .one(db)
        .await
        .context("failed to load collaborative task delivery")?
    else {
        return Ok(None);
    };
    let payload_matches = if expected_success {
        delivery.result_snapshot_json.is_some() && delivery.error_snapshot_json.is_none()
    } else {
        delivery.error_snapshot_json.is_some() && delivery.result_snapshot_json.is_none()
    };
    if !matches!(delivery.status.as_str(), "delivering" | "delivered")
        || delivery.mode != task_delivery_mode_to_db(TaskDeliveryMode::Thread)
        || delivery.thread_target.as_deref()
            != Some(task_delivery_thread_target_to_db(
                pioneer_protocol::TaskDeliveryThreadTarget::OriginThread,
            ))
        || delivery.workspace_id != workspace_id
        || delivery.target_thread_id.as_deref() != Some(parent_thread_id)
        || delivery
            .delivered_turn_id
            .as_deref()
            .is_some_and(|turn_id| turn_id != delivery_turn_id)
        || !payload_matches
    {
        return Ok(None);
    }

    let Some(task) = task::Entity::find_by_id(delivery.task_id.clone())
        .one(db)
        .await
        .context("failed to load collaborative task")?
    else {
        return Ok(None);
    };
    let metadata = task
        .metadata_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<TaskMetadata>(json).ok());
    let Some(composer_work) = metadata
        .as_ref()
        .and_then(|metadata| metadata.composer_work.as_ref())
    else {
        return Ok(None);
    };
    if task.workspace_id != workspace_id
        || task.created_by_thread_id.as_deref() != Some(parent_thread_id)
        || task.created_by_turn_id.as_deref() != Some(parent_turn_id)
        || !metadata
            .as_ref()
            .is_some_and(|metadata| metadata.labels.iter().any(|label| label == "composer"))
        || composer_work.version != TASK_COMPOSER_WORK_VERSION
        || composer_work.launch.thread_id != parent_thread_id
        || composer_work.launch.turn_id != parent_turn_id
        || !is_terminal_task_status_db(task.status.as_str())
        || expected_success != (task.status == task_status_to_db(TaskStatus::Completed))
        || (expected_success && task.result_json.is_none())
    {
        return Ok(None);
    }

    let Some(run) = task_run::Entity::find_by_id(delivery.run_id.clone())
        .one(db)
        .await
        .context("failed to load collaborative task run")?
    else {
        return Ok(None);
    };
    if run.task_id != task.id
        || !is_terminal_task_run_status_db(run.status.as_str())
        || expected_success != (run.status == task_run_status_to_db(TaskRunStatus::Succeeded))
        || (expected_success && run.result_json.is_none())
    {
        return Ok(None);
    }
    if delivery_turn_id != parent_turn_id && delivery_turn_id != run.id {
        return Ok(None);
    }
    let Some(delivery_turn) = turn::Entity::find_by_id(delivery_turn_id.to_owned())
        .one(db)
        .await
        .context("failed to load collaborative origin delivery turn")?
    else {
        return Ok(None);
    };
    if delivery_turn.thread_id != parent_thread_id
        || (delivery_turn_id == run.id
            && (delivery_turn.turn_kind != turn_kind_to_db(TurnKind::TaskRun)
                || delivery_turn.origin != turn_origin_to_db(TurnOrigin::DetachedTask)))
    {
        return Ok(None);
    }
    let successful_result = if expected_success {
        let Some(delivery_result) = decode_task_result(delivery.result_snapshot_json.as_deref())
        else {
            return Ok(None);
        };
        let Some(task_result) = decode_task_result(task.result_json.as_deref()) else {
            return Ok(None);
        };
        let Some(run_result) = decode_task_result(run.result_json.as_deref()) else {
            return Ok(None);
        };
        if delivery_result != task_result || delivery_result != run_result {
            return Ok(None);
        }
        Some(delivery_result)
    } else {
        None
    };

    let Some(primary_binding) = task_run_thread_binding::Entity::find()
        .filter(task_run_thread_binding::Column::RunId.eq(run.id.clone()))
        .filter(task_run_thread_binding::Column::BindingKind.eq(
            task_run_thread_binding_kind_to_db(TaskRunThreadBindingKind::PrimaryExecutor),
        ))
        .one(db)
        .await
        .context("failed to load collaborative primary child binding")?
    else {
        return Ok(None);
    };
    if primary_binding.task_id != task.id
        || !collaborative_child_lineage_matches(
            db,
            primary_binding.thread_id.as_str(),
            parent_thread_id,
            parent_turn_id,
            run.id.as_str(),
            delivery_turn_id,
        )
        .await?
    {
        return Ok(None);
    }
    let Some(child_thread) = thread::Entity::find_by_id(primary_binding.thread_id.clone())
        .one(db)
        .await
        .context("failed to load collaborative child execution thread")?
    else {
        return Ok(None);
    };
    if child_thread.workspace_id != workspace_id
        || child_thread.origin_kind != thread_origin_kind_to_db(ThreadOriginKind::TaskRun)
        || child_thread.sidebar_visibility
            != thread_sidebar_visibility_to_db(ThreadSidebarVisibility::Hidden)
    {
        return Ok(None);
    }

    let Some(snapshot) = task_run_conversation_snapshot::Entity::find_by_id(run.id.clone())
        .one(db)
        .await
        .context("failed to load collaborative conversation snapshot")?
    else {
        return Ok(None);
    };
    if snapshot.task_id != task.id
        || snapshot.workspace_id != workspace_id
        || snapshot.conversation_thread_id != parent_thread_id
        || snapshot.source_turn_id.as_deref() != Some(parent_turn_id)
    {
        return Ok(None);
    }

    let accepted_task_run_turn_id = if expected_success {
        let Some(candidate) =
            task_result_candidate::Entity::find()
                .filter(task_result_candidate::Column::RunId.eq(run.id.clone()))
                .filter(task_result_candidate::Column::Status.eq(
                    task_result_candidate_status_to_db(TaskResultCandidateStatus::Accepted),
                ))
                .one(db)
                .await
                .context("failed to load collaborative accepted result candidate")?
        else {
            return Ok(None);
        };
        let Some(candidate_result) = decode_task_result(candidate.result_json.as_deref()) else {
            return Ok(None);
        };
        if candidate.task_id != task.id || successful_result.as_ref() != Some(&candidate_result) {
            return Ok(None);
        }
        let Some(child_run_turn) =
            task_run_turn::Entity::find_by_id(candidate.task_run_turn_id.clone())
                .one(db)
                .await
                .context("failed to load collaborative accepted child turn")?
        else {
            return Ok(None);
        };
        if child_run_turn.task_id != task.id
            || child_run_turn.run_id != run.id
            || child_run_turn.thread_id != primary_binding.thread_id
            || child_run_turn.thread_id != candidate.thread_id
            || child_run_turn.turn_id != candidate.turn_id
            || child_run_turn.completed_at.is_none()
            || !matches!(
                task_run_turn_kind_from_db(child_run_turn.kind.as_str())?,
                TaskRunTurnKind::Initial | TaskRunTurnKind::Revision
            )
        {
            return Ok(None);
        }
        let Some(child_turn) = turn::Entity::find_by_id(child_run_turn.turn_id.clone())
            .one(db)
            .await
            .context("failed to load collaborative child execution turn")?
        else {
            return Ok(None);
        };
        if child_turn.thread_id != child_run_turn.thread_id
            || child_turn.turn_kind != turn_kind_to_db(TurnKind::Conversation)
            || child_turn.origin != turn_origin_to_db(TurnOrigin::User)
            || child_turn.status != turn_status_to_db(TurnStatus::Completed)
            || !collaborative_child_lineage_matches(
                db,
                child_run_turn.thread_id.as_str(),
                parent_thread_id,
                parent_turn_id,
                run.id.as_str(),
                delivery_turn_id,
            )
            .await?
        {
            return Ok(None);
        }
        Some(child_run_turn.id)
    } else {
        None
    };

    Ok(Some(VerifiedCollaborativeExchange {
        delivery_id: delivery.id,
        delivery_turn_id: delivery_turn_id.to_owned(),
        task_id: task.id,
        run_id: run.id,
        accepted_task_run_turn_id,
    }))
}

fn decode_task_result(json: Option<&str>) -> Option<TaskResult> {
    json.and_then(|json| serde_json::from_str(json).ok())
}

pub(crate) async fn collaborative_child_lineage_matches<C: ConnectionTrait>(
    db: &C,
    child_thread_id: &str,
    parent_thread_id: &str,
    parent_turn_id: &str,
    run_id: &str,
    delivery_turn_id: &str,
) -> Result<bool> {
    let Some(lineage) = thread_lineage::Entity::find_by_id(child_thread_id.to_owned())
        .one(db)
        .await
        .context("failed to load collaborative source child lineage")?
    else {
        return Ok(false);
    };
    if lineage.parent_thread_id != parent_thread_id
        || lineage.origin_kind.as_deref() != Some("task_run")
        || lineage.created_by_thread_id.as_deref() != Some(parent_thread_id)
    {
        return Ok(false);
    }
    if lineage.created_by_turn_id.as_deref() != Some(delivery_turn_id) {
        return Ok(false);
    }
    match lineage.created_by_turn_id.as_deref() {
        Some(turn_id) if turn_id == parent_turn_id => Ok(true),
        Some(turn_id) if turn_id == run_id => {
            let Some(occurrence) = turn::Entity::find_by_id(run_id.to_owned())
                .one(db)
                .await
                .context("failed to load collaborative detached occurrence turn")?
            else {
                return Ok(false);
            };
            Ok(occurrence.thread_id == parent_thread_id
                && occurrence.turn_kind == turn_kind_to_db(TurnKind::TaskRun)
                && occurrence.origin == turn_origin_to_db(TurnOrigin::DetachedTask))
        }
        _ => Ok(false),
    }
}

pub async fn source_head<C: ConnectionTrait>(db: &C, workspace_id: &str) -> Result<i64> {
    Ok(self_improvement_source_turn::Entity::find()
        .filter(self_improvement_source_turn::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .order_by_desc(self_improvement_source_turn::Column::Id)
        .one(db)
        .await
        .with_context(|| {
            format!("failed to read self-improvement source head for workspace `{workspace_id}`")
        })?
        .map(|row| row.id)
        .unwrap_or(0))
}

pub async fn contains_source_id<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    source_id: i64,
) -> Result<bool> {
    if source_id <= 0 {
        return Ok(false);
    }
    Ok(self_improvement_source_turn::Entity::find_by_id(source_id)
        .filter(self_improvement_source_turn::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to verify self-improvement source `{source_id}` for workspace \
                     `{workspace_id}`"
            )
        })?
        .is_some())
}

pub async fn list_after_cursor<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    cursor_source_id: i64,
    effective_enabled_at_unix: i64,
    limit: u64,
) -> Result<Vec<SelfImprovementSourceTurnRecord>> {
    if cursor_source_id < 0 {
        bail!("self-improvement source cursor must be non-negative");
    }
    if limit == 0 {
        return Ok(Vec::new());
    }

    let rows = self_improvement_source_turn::Entity::find()
        .join(
            JoinType::InnerJoin,
            self_improvement_source_turn::Entity::belongs_to(thread::Entity)
                .from(self_improvement_source_turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .filter(self_improvement_source_turn::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread::Column::AccessClass.is_in(source_access_classes()))
        .filter(
            thread::Column::SidebarVisibility.eq(thread_sidebar_visibility_to_db(
                ThreadSidebarVisibility::Visible,
            )),
        )
        .filter(thread::Column::OriginKind.is_in(source_origins()))
        .filter(self_improvement_source_turn::Column::Id.gt(cursor_source_id))
        .filter(
            self_improvement_source_turn::Column::TerminalAt
                .gte(unix_to_datetime(effective_enabled_at_unix)),
        )
        .order_by_asc(self_improvement_source_turn::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list self-improvement source turns for workspace `{workspace_id}`")
        })?;
    records_from_models(db, workspace_id, rows).await
}

pub async fn list_frozen_range<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    source_lower_exclusive: i64,
    source_upper_inclusive: i64,
    effective_enabled_at_unix: i64,
) -> Result<Vec<SelfImprovementSourceTurnRecord>> {
    if workspace_id.trim().is_empty() {
        bail!("self-improvement frozen source workspace_id must not be empty");
    }
    if source_lower_exclusive < 0 || source_upper_inclusive <= source_lower_exclusive {
        bail!("self-improvement frozen source range bounds are invalid");
    }

    let rows = self_improvement_source_turn::Entity::find()
        .join(
            JoinType::InnerJoin,
            self_improvement_source_turn::Entity::belongs_to(thread::Entity)
                .from(self_improvement_source_turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .filter(self_improvement_source_turn::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread::Column::AccessClass.is_in(source_access_classes()))
        .filter(
            thread::Column::SidebarVisibility.eq(thread_sidebar_visibility_to_db(
                ThreadSidebarVisibility::Visible,
            )),
        )
        .filter(thread::Column::OriginKind.is_in(source_origins()))
        .filter(self_improvement_source_turn::Column::Id.gt(source_lower_exclusive))
        .filter(self_improvement_source_turn::Column::Id.lte(source_upper_inclusive))
        .filter(
            self_improvement_source_turn::Column::TerminalAt
                .gte(unix_to_datetime(effective_enabled_at_unix)),
        )
        .order_by_asc(self_improvement_source_turn::Column::Id)
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to load frozen self-improvement source range for workspace \
                 `{workspace_id}`"
            )
        })?;
    records_from_models(db, workspace_id, rows).await
}

fn record_from_model(
    model: self_improvement_source_turn::Model,
    parent_turn_created_at_unix: i64,
) -> SelfImprovementSourceTurnRecord {
    SelfImprovementSourceTurnRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        parent_turn_created_at_unix,
        task_delivery_id: model.task_delivery_id,
        terminal_event_id: model.terminal_event_id,
        terminal_at_unix: model.terminal_at.timestamp(),
        created_at_unix: model.created_at.timestamp(),
    }
}

async fn records_from_models<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    models: Vec<self_improvement_source_turn::Model>,
) -> Result<Vec<SelfImprovementSourceTurnRecord>> {
    if models.is_empty() {
        return Ok(Vec::new());
    }
    let turn_ids = models
        .iter()
        .map(|model| model.turn_id.clone())
        .collect::<Vec<_>>();
    let parent_turns = turn::Entity::find()
        .filter(turn::Column::Id.is_in(turn_ids))
        .all(db)
        .await
        .with_context(|| {
            format!("failed to load source parent turns for workspace `{workspace_id}`")
        })?;
    let parent_turns = parent_turns
        .into_iter()
        .map(|parent_turn| (parent_turn.id.clone(), parent_turn))
        .collect::<std::collections::HashMap<_, _>>();
    let thread_ids = models
        .iter()
        .map(|model| model.thread_id.clone())
        .collect::<std::collections::HashSet<_>>();
    let source_threads = thread::Entity::find()
        .filter(thread::Column::Id.is_in(thread_ids))
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to revalidate self-improvement source threads for workspace \
                 `{workspace_id}`"
            )
        })?
        .into_iter()
        .map(|model| (model.id.clone(), model))
        .collect::<std::collections::HashMap<_, _>>();

    models
        .into_iter()
        .map(|model| {
            let source_thread =
                source_threads
                    .get(model.thread_id.as_str())
                    .with_context(|| {
                        format!(
                            "self-improvement source `{}` thread `{}` is missing",
                            model.id, model.thread_id
                        )
                    })?;
            if source_thread.workspace_id != workspace_id || !eligible_source_thread(source_thread)
            {
                bail!(
                    "self-improvement source `{}` is no longer eligible",
                    model.id
                );
            }
            let parent_turn = parent_turns.get(model.turn_id.as_str()).with_context(|| {
                format!(
                    "self-improvement source `{}` parent turn `{}` is missing",
                    model.id, model.turn_id
                )
            })?;
            if parent_turn.thread_id != model.thread_id {
                bail!(
                    "self-improvement source `{}` parent thread identity mismatch",
                    model.id
                );
            }
            if turn_status_from_db(parent_turn.status.as_str()) != Some(TurnStatus::Completed)
                || turn_kind_from_db(parent_turn.turn_kind.as_str()) != Some(TurnKind::Conversation)
                || turn_origin_from_db(parent_turn.origin.as_str()) != Some(TurnOrigin::User)
            {
                bail!(
                    "self-improvement source `{}` parent turn is not terminal eligible history",
                    model.id
                );
            }
            Ok(record_from_model(model, parent_turn.created_at.timestamp()))
        })
        .collect()
}

pub(super) fn source_origins() -> Vec<String> {
    [
        ThreadOriginKind::Collaborative,
        ThreadOriginKind::DirectMessage,
        ThreadOriginKind::User,
    ]
    .into_iter()
    .map(|origin| thread_origin_kind_to_db(origin).to_owned())
    .collect()
}

pub(super) fn source_access_classes() -> Vec<&'static str> {
    [
        PersistedThreadAccessClass::Private,
        PersistedThreadAccessClass::Workspace,
    ]
    .into_iter()
    .map(persisted_thread_access_class_to_db)
    .collect()
}

// Self-improvement is shared within the workspace, including experience from
// private conversations. Internal execution threads contribute through their
// verified parent exchange, never as duplicate independent source anchors.
// DirectMessage currently represents an agent conversation; a future human-only
// DM must be distinguished explicitly rather than inferred from private access.
pub(super) fn eligible_source_thread(model: &thread::Model) -> bool {
    matches!(
        persisted_thread_access_class_from_db(model.access_class.as_str()).ok(),
        Some(PersistedThreadAccessClass::Private | PersistedThreadAccessClass::Workspace)
    ) && thread_sidebar_visibility_from_db(model.sidebar_visibility.as_str())
        == Some(ThreadSidebarVisibility::Visible)
        && thread_origin_kind_from_db(model.origin_kind.as_str()).is_some_and(|origin| {
            matches!(
                origin,
                ThreadOriginKind::Collaborative
                    | ThreadOriginKind::DirectMessage
                    | ThreadOriginKind::User
            )
        })
}
