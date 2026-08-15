use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result, bail};
use pioneer_entity::{
    self_improvement_source_turn, task, task_delivery, task_run_turn, thread, turn, turn_event,
};
use pioneer_protocol::{
    SystemEventLevel, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, TurnItem, TurnKind,
    TurnOrigin, TurnStatus, UserInput, task_delivery_id_from_result_item_id,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder};

use super::identity::actor_ref_from_db;
use super::membership::{PersistedThreadAccessClass, persisted_thread_access_class_from_db};
use super::self_improvement_source_turn::{
    VerifiedCollaborativeExchange, collaborative_child_lineage_matches,
    verify_collaborative_exchange,
};
use crate::convention::{
    task_run_turn_kind_from_db, thread_origin_kind_from_db, thread_sidebar_visibility_from_db,
    turn_kind_from_db, turn_origin_from_db, turn_status_from_db,
};
use crate::{
    CanonicalTurnEventPayload, CanonicalTurnEventRecord, SelfImprovementFrozenSourceRange,
    SelfImprovementSourceTurnRecord, SelfImprovementThreadTerminalBoundary,
};

pub async fn list_for_frozen_range<C: ConnectionTrait>(
    db: &C,
    frozen_range: &SelfImprovementFrozenSourceRange,
) -> Result<Vec<CanonicalTurnEventRecord>> {
    frozen_range.validate()?;
    let workspace_id = frozen_range.workspace_id.as_str();

    let mut seen_source_ids = HashSet::new();
    let mut sources_by_thread = BTreeMap::<String, Vec<SelfImprovementSourceTurnRecord>>::new();
    for source in frozen_range.anchors.iter().cloned() {
        if source.id <= 0 || !seen_source_ids.insert(source.id) {
            bail!("canonical history selected source IDs must be positive and unique");
        }
        if source.workspace_id != workspace_id {
            bail!(
                "canonical history source `{}` belongs to workspace `{}`, not `{workspace_id}`",
                source.id,
                source.workspace_id
            );
        }
        verify_selected_source(db, &source).await?;
        sources_by_thread
            .entry(source.thread_id.clone())
            .or_default()
            .push(source);
    }

    let boundaries_by_thread = frozen_range
        .thread_terminal_boundaries
        .iter()
        .map(|boundary| (boundary.thread_id.as_str(), boundary))
        .collect::<HashMap<_, _>>();
    let mut result = Vec::new();

    for (thread_id, selected_thread_sources) in sources_by_thread {
        let visible_thread = thread::Entity::find_by_id(thread_id.clone())
            .one(db)
            .await
            .with_context(|| format!("failed to load canonical history thread `{thread_id}`"))?
            .with_context(|| format!("canonical history thread `{thread_id}` is missing"))?;
        if visible_thread.workspace_id != workspace_id {
            bail!(
                "canonical history thread `{thread_id}` belongs to workspace `{}`, not \
                 `{workspace_id}`",
                visible_thread.workspace_id
            );
        }
        if persisted_thread_access_class_from_db(visible_thread.access_class.as_str()).ok()
            != Some(PersistedThreadAccessClass::Workspace)
            || thread_sidebar_visibility_from_db(visible_thread.sidebar_visibility.as_str())
                != Some(ThreadSidebarVisibility::Visible)
        {
            bail!("canonical history thread `{thread_id}` is no longer workspace-visible");
        }
        let origin =
            thread_origin_kind_from_db(visible_thread.origin_kind.as_str()).with_context(|| {
                format!(
                    "canonical history thread `{thread_id}` has unknown origin `{}`",
                    visible_thread.origin_kind
                )
            })?;
        if !matches!(
            origin,
            ThreadOriginKind::Collaborative
                | ThreadOriginKind::DirectMessage
                | ThreadOriginKind::User
        ) {
            bail!("canonical history anchor thread `{thread_id}` is not user-visible");
        }

        let boundary = boundaries_by_thread
            .get(thread_id.as_str())
            .with_context(|| {
                format!("canonical history thread `{thread_id}` has no frozen terminal boundary")
            })?;
        let boundary_event = verify_thread_boundary(db, boundary, &selected_thread_sources).await?;
        let selected_turn_ids = selected_thread_sources
            .iter()
            .map(|source| source.turn_id.as_str())
            .collect::<HashSet<_>>();

        let source_rows = self_improvement_source_turn::Entity::find()
            .filter(self_improvement_source_turn::Column::WorkspaceId.eq(workspace_id.to_owned()))
            .filter(self_improvement_source_turn::Column::ThreadId.eq(thread_id.clone()))
            .all(db)
            .await
            .with_context(|| {
                format!("failed to load source identities for canonical thread `{thread_id}`")
            })?;
        let sources_by_turn = source_rows
            .into_iter()
            .map(|source| (source.turn_id.clone(), source))
            .collect::<HashMap<_, _>>();

        let parent_turns = turn::Entity::find()
            .filter(turn::Column::ThreadId.eq(thread_id.clone()))
            .order_by_asc(turn::Column::CreatedAt)
            .order_by_asc(turn::Column::Id)
            .all(db)
            .await
            .with_context(|| format!("failed to load canonical turns for thread `{thread_id}`"))?;
        let mut included_selected_turns = HashSet::new();

        for parent_turn in parent_turns {
            if turn_kind_from_db(parent_turn.turn_kind.as_str()) != Some(TurnKind::Conversation)
                || turn_origin_from_db(parent_turn.origin.as_str()) != Some(TurnOrigin::User)
            {
                continue;
            }
            let bundle = match origin {
                ThreadOriginKind::DirectMessage | ThreadOriginKind::User => {
                    foreground_exchange_bundle(
                        db,
                        workspace_id,
                        &parent_turn,
                        sources_by_turn.get(parent_turn.id.as_str()),
                        frozen_range.source_upper_inclusive,
                        boundary,
                        &boundary_event,
                    )
                    .await?
                }
                ThreadOriginKind::Collaborative => {
                    collaborative_exchange_bundle(
                        db,
                        workspace_id,
                        thread_id.as_str(),
                        &parent_turn,
                        sources_by_turn.get(parent_turn.id.as_str()),
                        frozen_range.source_upper_inclusive,
                        boundary,
                        &boundary_event,
                    )
                    .await?
                }
                ThreadOriginKind::TaskRun | ThreadOriginKind::System => None,
            };
            let Some(bundle) = bundle else {
                continue;
            };
            if selected_turn_ids.contains(parent_turn.id.as_str()) {
                included_selected_turns.insert(parent_turn.id.clone());
            }
            result.extend(bundle);
        }

        if included_selected_turns.len() != selected_turn_ids.len() {
            bail!(
                "canonical history boundary did not include every selected source exchange in \
                 thread `{thread_id}`"
            );
        }
    }

    Ok(result)
}

async fn foreground_exchange_bundle<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    parent_turn: &turn::Model,
    source: Option<&self_improvement_source_turn::Model>,
    source_upper_inclusive: i64,
    logical_boundary: &SelfImprovementThreadTerminalBoundary,
    terminal_boundary_event: &CanonicalTurnEventRecord,
) -> Result<Option<Vec<CanonicalTurnEventRecord>>> {
    let Some(status) = turn_status_from_db(parent_turn.status.as_str()) else {
        bail!(
            "canonical foreground turn `{}` has unknown status `{}`",
            parent_turn.id,
            parent_turn.status
        );
    };
    if !is_terminal_status(status) {
        return Ok(None);
    }
    if source.is_some_and(|source| source.id > source_upper_inclusive) {
        return Ok(None);
    }
    if !parent_turn_at_or_before_boundary(parent_turn, logical_boundary) {
        return Ok(None);
    }

    let events = load_decoded_turn_events(
        db,
        workspace_id,
        parent_turn.thread_id.as_str(),
        parent_turn.id.as_str(),
    )
    .await?;
    let terminal = if let Some(source) = source {
        events
            .iter()
            .find(|event| event.event_id == source.terminal_event_id)
            .with_context(|| {
                format!(
                    "canonical foreground source `{}` has no terminal event",
                    source.id
                )
            })?
    } else {
        events
            .iter()
            .rev()
            .find(|event| terminal_status(&event.payload) == Some(status))
            .with_context(|| {
                format!(
                    "canonical foreground turn `{}` has no matching terminal event",
                    parent_turn.id
                )
            })?
    };
    if terminal_status(&terminal.payload) != Some(status) {
        bail!(
            "canonical foreground turn `{}` terminal event does not match its status",
            parent_turn.id
        );
    }
    if source.is_none() && terminal.created_at >= terminal_boundary_event.created_at {
        return Ok(None);
    }
    let terminal_sequence = terminal.sequence;
    Ok(Some(
        events
            .into_iter()
            .filter(|event| event.sequence <= terminal_sequence)
            .collect(),
    ))
}

async fn collaborative_exchange_bundle<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    parent_thread_id: &str,
    parent_turn: &turn::Model,
    source: Option<&self_improvement_source_turn::Model>,
    source_upper_inclusive: i64,
    logical_boundary: &SelfImprovementThreadTerminalBoundary,
    terminal_boundary_event: &CanonicalTurnEventRecord,
) -> Result<Option<Vec<CanonicalTurnEventRecord>>> {
    if parent_turn.send_mode.as_deref() == Some("message") {
        return foreground_exchange_bundle(
            db,
            workspace_id,
            parent_turn,
            source,
            source_upper_inclusive,
            logical_boundary,
            terminal_boundary_event,
        )
        .await;
    }
    if turn_status_from_db(parent_turn.status.as_str()) != Some(TurnStatus::Completed)
        || source.is_some_and(|source| source.id > source_upper_inclusive)
        || !parent_turn_at_or_before_boundary(parent_turn, logical_boundary)
    {
        return Ok(None);
    }
    let parent_events =
        load_decoded_turn_events(db, workspace_id, parent_thread_id, parent_turn.id.as_str())
            .await?;
    let admission_terminal = parent_events
        .iter()
        .find(|event| {
            matches!(
                &event.payload,
                CanonicalTurnEventPayload::TurnCompleted(notification)
                    if notification.turn.status == TurnStatus::Completed
            )
        })
        .with_context(|| {
            format!(
                "collaborative parent turn `{}` has no admission completion",
                parent_turn.id
            )
        })?;

    let terminal_delivery = if let Some(source) = source {
        let delivery_id = source.task_delivery_id.as_deref().with_context(|| {
            format!(
                "collaborative source `{}` has no task delivery identity",
                source.id
            )
        })?;
        let event = load_decoded_event(db, workspace_id, source.terminal_event_id.as_str()).await?;
        if event.thread_id != parent_thread_id {
            bail!(
                "collaborative source `{}` terminal delivery thread mismatch",
                source.id
            );
        }
        let event_delivery_id = completed_delivery_identity(&event.payload)
            .context("collaborative source terminal is not a task delivery completion")?;
        if event_delivery_id != delivery_id {
            bail!(
                "collaborative source `{}` terminal delivery identity mismatch",
                source.id
            );
        }
        let verified = verify_collaborative_exchange(
            db,
            workspace_id,
            parent_thread_id,
            parent_turn.id.as_str(),
            delivery_id,
            event.turn_id.as_str(),
            true,
        )
        .await?
        .with_context(|| {
            format!(
                "collaborative source `{}` no longer matches durable task lineage",
                source.id
            )
        })?;
        (event, verified)
    } else {
        let mut terminal = None;
        let delivery_terminals = load_collaborative_delivery_terminals(
            db,
            workspace_id,
            parent_thread_id,
            parent_turn.id.as_str(),
        )
        .await?;
        for event in delivery_terminals.into_iter().rev() {
            let Some(delivery_id) = completed_delivery_identity(&event.payload) else {
                continue;
            };
            let expected_success = matches!(
                &event.payload,
                CanonicalTurnEventPayload::ItemCompleted(notification)
                    if matches!(&notification.item, TurnItem::AgentMessage { .. })
            );
            let expected_failure = matches!(
                &event.payload,
                CanonicalTurnEventPayload::ItemCompleted(notification)
                    if matches!(
                        &notification.item,
                        TurnItem::SystemEvent {
                            level: SystemEventLevel::Error,
                            ..
                        }
                    )
            );
            if !expected_success && !expected_failure {
                continue;
            }
            let Some(verified) = verify_collaborative_exchange(
                db,
                workspace_id,
                parent_thread_id,
                parent_turn.id.as_str(),
                delivery_id,
                event.turn_id.as_str(),
                expected_success,
            )
            .await?
            else {
                continue;
            };
            terminal = Some((event, verified));
            break;
        }
        let Some(terminal) = terminal else {
            return Ok(None);
        };
        terminal
    };
    if source.is_none() && terminal_delivery.0.created_at >= terminal_boundary_event.created_at {
        return Ok(None);
    }

    let delivery_item_id =
        pioneer_protocol::task_delivery_result_item_id(terminal_delivery.1.delivery_id.as_str());
    let mut bundle = parent_events
        .iter()
        .filter(|event| event.sequence <= admission_terminal.sequence)
        .cloned()
        .collect::<Vec<_>>();
    let child_records = load_collaborative_child_events(
        db,
        workspace_id,
        parent_thread_id,
        parent_turn.id.as_str(),
        &terminal_delivery.1,
        &terminal_delivery.0.created_at,
    )
    .await?;
    bundle.extend(child_records);
    let delivery_events = load_decoded_turn_events(
        db,
        workspace_id,
        parent_thread_id,
        terminal_delivery.1.delivery_turn_id.as_str(),
    )
    .await?;
    bundle.extend(delivery_events.into_iter().filter(|event| {
        event.sequence <= terminal_delivery.0.sequence
            && payload_item_id(&event.payload) == Some(delivery_item_id.as_str())
    }));
    if bundle
        .last()
        .is_none_or(|event| event.event_id != terminal_delivery.0.event_id)
    {
        bail!(
            "collaborative exchange `{}` did not end at its exact delivery boundary",
            parent_turn.id
        );
    }
    Ok(Some(bundle))
}

async fn load_collaborative_delivery_terminals<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    parent_thread_id: &str,
    parent_turn_id: &str,
) -> Result<Vec<CanonicalTurnEventRecord>> {
    let tasks = task::Entity::find()
        .filter(task::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(task::Column::CreatedByThreadId.eq(parent_thread_id.to_owned()))
        .filter(task::Column::CreatedByTurnId.eq(parent_turn_id.to_owned()))
        .all(db)
        .await
        .context("failed to load canonical collaborative parent tasks")?;
    if tasks.is_empty() {
        return Ok(Vec::new());
    }
    let task_ids = tasks.into_iter().map(|task| task.id).collect::<Vec<_>>();
    let deliveries = task_delivery::Entity::find()
        .filter(task_delivery::Column::TaskId.is_in(task_ids))
        .filter(task_delivery::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(task_delivery::Column::TargetThreadId.eq(parent_thread_id.to_owned()))
        .filter(task_delivery::Column::DeliveredTurnId.is_not_null())
        .all(db)
        .await
        .context("failed to load canonical collaborative parent deliveries")?;

    let mut terminals = Vec::new();
    for delivery in deliveries {
        let Some(delivery_turn_id) = delivery.delivered_turn_id.as_deref() else {
            continue;
        };
        let expected_item_id = pioneer_protocol::task_delivery_result_item_id(delivery.id.as_str());
        let events =
            load_decoded_turn_events(db, workspace_id, parent_thread_id, delivery_turn_id).await?;
        terminals.extend(events.into_iter().filter(|event| {
            completed_delivery_identity(&event.payload) == Some(delivery.id.as_str())
                && payload_item_id(&event.payload) == Some(expected_item_id.as_str())
        }));
    }
    terminals.sort_by(|left, right| {
        (left.created_at, left.sequence, left.event_id.as_str()).cmp(&(
            right.created_at,
            right.sequence,
            right.event_id.as_str(),
        ))
    });
    Ok(terminals)
}

async fn load_collaborative_child_events<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    parent_thread_id: &str,
    parent_turn_id: &str,
    exchange: &VerifiedCollaborativeExchange,
    owner_delivery_completed_at: &DateTimeWithTimeZone,
) -> Result<Vec<CanonicalTurnEventRecord>> {
    let run_turns = task_run_turn::Entity::find()
        .filter(task_run_turn::Column::RunId.eq(exchange.run_id.clone()))
        .order_by_asc(task_run_turn::Column::Sequence)
        .order_by_asc(task_run_turn::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to load canonical collaborative child turns")?;
    let mut result = Vec::new();
    let mut included_accepted_turn = exchange.accepted_task_run_turn_id.is_none();
    let accepted_sequence = exchange
        .accepted_task_run_turn_id
        .as_deref()
        .and_then(|accepted_id| {
            run_turns
                .iter()
                .find(|run_turn| run_turn.id == accepted_id)
                .map(|run_turn| run_turn.sequence)
        });

    for run_turn in run_turns {
        let kind = task_run_turn_kind_from_db(run_turn.kind.as_str())?;
        if !matches!(
            kind,
            pioneer_protocol::TaskRunTurnKind::Initial
                | pioneer_protocol::TaskRunTurnKind::Revision
        ) {
            continue;
        }
        if run_turn.task_id != exchange.task_id
            || run_turn.completed_at.is_none()
            || !collaborative_child_lineage_matches(
                db,
                run_turn.thread_id.as_str(),
                parent_thread_id,
                parent_turn_id,
                exchange.run_id.as_str(),
                exchange.delivery_turn_id.as_str(),
            )
            .await?
        {
            bail!(
                "canonical collaborative child `{}` does not match its source lineage",
                run_turn.id
            );
        }
        if run_turn
            .completed_at
            .as_ref()
            .is_none_or(|completed_at| completed_at > owner_delivery_completed_at)
        {
            bail!(
                "canonical collaborative child `{}` did not complete before its origin delivery",
                run_turn.id
            );
        }
        if accepted_sequence.is_some_and(|sequence| run_turn.sequence > sequence) {
            bail!(
                "canonical collaborative child `{}` occurs after the accepted execution turn",
                run_turn.id
            );
        }
        let child_thread = thread::Entity::find_by_id(run_turn.thread_id.clone())
            .one(db)
            .await
            .context("failed to load canonical collaborative execution thread")?
            .with_context(|| {
                format!(
                    "canonical collaborative execution thread `{}` is missing",
                    run_turn.thread_id
                )
            })?;
        let child_thread_origin = thread_origin_kind_from_db(child_thread.origin_kind.as_str())
            .with_context(|| {
                format!(
                    "canonical collaborative execution thread `{}` has unknown origin `{}`",
                    child_thread.id, child_thread.origin_kind
                )
            })?;
        let child_thread_visibility =
            thread_sidebar_visibility_from_db(child_thread.sidebar_visibility.as_str())
                .with_context(|| {
                    format!(
                        "canonical collaborative execution thread `{}` has unknown sidebar \
                         visibility `{}`",
                        child_thread.id, child_thread.sidebar_visibility
                    )
                })?;
        if child_thread.workspace_id != workspace_id
            || child_thread_origin != ThreadOriginKind::TaskRun
            || child_thread_visibility != ThreadSidebarVisibility::Hidden
        {
            bail!(
                "canonical collaborative execution thread `{}` is not a hidden TaskRun thread in \
                 workspace `{workspace_id}`",
                child_thread.id
            );
        }
        let child_turn = turn::Entity::find_by_id(run_turn.turn_id.clone())
            .one(db)
            .await
            .context("failed to load canonical collaborative execution turn")?
            .with_context(|| {
                format!(
                    "canonical collaborative execution turn `{}` is missing",
                    run_turn.turn_id
                )
            })?;
        let child_kind = turn_kind_from_db(child_turn.turn_kind.as_str()).with_context(|| {
            format!(
                "canonical collaborative child turn `{}` has unknown kind `{}`",
                child_turn.id, child_turn.turn_kind
            )
        })?;
        let child_origin = turn_origin_from_db(child_turn.origin.as_str()).with_context(|| {
            format!(
                "canonical collaborative child turn `{}` has unknown origin `{}`",
                child_turn.id, child_turn.origin
            )
        })?;
        let child_status = turn_status_from_db(child_turn.status.as_str()).with_context(|| {
            format!(
                "canonical collaborative child turn `{}` has unknown status `{}`",
                child_turn.id, child_turn.status
            )
        })?;
        if child_turn.thread_id != run_turn.thread_id
            || child_kind != TurnKind::Conversation
            || child_origin != TurnOrigin::User
            || !is_terminal_status(child_status)
        {
            bail!(
                "canonical collaborative child turn `{}` is not a terminal Conversation/User \
                 execution turn",
                child_turn.id
            );
        }
        let child_events = load_decoded_turn_events(
            db,
            workspace_id,
            run_turn.thread_id.as_str(),
            run_turn.turn_id.as_str(),
        )
        .await?;
        let terminal = child_events
            .iter()
            .rev()
            .find(|event| terminal_status(&event.payload) == Some(child_status))
            .with_context(|| {
                format!(
                    "canonical collaborative child turn `{}` has no terminal event",
                    child_turn.id
                )
            })?;
        if &terminal.created_at > owner_delivery_completed_at {
            bail!(
                "canonical collaborative child turn `{}` terminated after its origin delivery",
                child_turn.id
            );
        }
        let terminal_sequence = terminal.sequence;
        result.extend(
            child_events
                .into_iter()
                .filter(|event| event.sequence <= terminal_sequence),
        );
        included_accepted_turn |= exchange
            .accepted_task_run_turn_id
            .as_deref()
            .is_some_and(|id| id == run_turn.id);
    }
    if !included_accepted_turn {
        bail!(
            "canonical collaborative run `{}` omitted its accepted child",
            exchange.run_id
        );
    }
    Ok(result)
}

async fn verify_selected_source<C: ConnectionTrait>(
    db: &C,
    selected: &SelfImprovementSourceTurnRecord,
) -> Result<()> {
    let persisted = self_improvement_source_turn::Entity::find_by_id(selected.id)
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to verify canonical history source `{}`",
                selected.id
            )
        })?
        .with_context(|| {
            format!(
                "canonical history selected source `{}` is missing",
                selected.id
            )
        })?;
    if persisted.workspace_id != selected.workspace_id
        || persisted.thread_id != selected.thread_id
        || persisted.turn_id != selected.turn_id
        || persisted.task_delivery_id != selected.task_delivery_id
        || persisted.terminal_event_id != selected.terminal_event_id
        || persisted.terminal_at.timestamp() != selected.terminal_at_unix
    {
        bail!(
            "canonical history selected source `{}` does not match its persisted identity",
            selected.id
        );
    }
    let parent_turn = turn::Entity::find_by_id(selected.turn_id.clone())
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to verify canonical source `{}` parent turn",
                selected.id
            )
        })?
        .with_context(|| {
            format!(
                "canonical source `{}` parent turn `{}` is missing",
                selected.id, selected.turn_id
            )
        })?;
    if parent_turn.thread_id != selected.thread_id
        || parent_turn.created_at.timestamp() != selected.parent_turn_created_at_unix
    {
        bail!(
            "canonical history selected source `{}` parent ordering identity mismatch",
            selected.id
        );
    }
    Ok(())
}

async fn verify_thread_boundary<C: ConnectionTrait>(
    db: &C,
    boundary: &SelfImprovementThreadTerminalBoundary,
    selected_sources: &[SelfImprovementSourceTurnRecord],
) -> Result<CanonicalTurnEventRecord> {
    let source = selected_sources
        .iter()
        .find(|source| source.id == boundary.source_id)
        .context("canonical history frozen boundary source is not selected")?;
    if source.thread_id != boundary.thread_id
        || source.turn_id != boundary.turn_id
        || source.parent_turn_created_at_unix != boundary.parent_turn_created_at_unix
        || source.task_delivery_id != boundary.task_delivery_id
        || source.terminal_event_id != boundary.terminal_event_id
        || source.terminal_at_unix != boundary.terminal_at_unix
    {
        bail!("canonical history frozen terminal boundary does not match its source");
    }

    let event = turn_event::Entity::find_by_id(boundary.terminal_event_id.clone())
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to load canonical terminal event `{}`",
                boundary.terminal_event_id
            )
        })?
        .with_context(|| {
            format!(
                "canonical terminal event `{}` is missing",
                boundary.terminal_event_id
            )
        })?;
    let event = decode_event(event, source.workspace_id.as_str())?;
    let terminal_matches = match boundary.task_delivery_id.as_deref() {
        None => {
            event.turn_id == boundary.turn_id
                && terminal_status(&event.payload) == Some(TurnStatus::Completed)
        }
        Some(delivery_id) => {
            completed_delivery_identity(&event.payload) == Some(delivery_id)
                && verify_collaborative_exchange(
                    db,
                    source.workspace_id.as_str(),
                    boundary.thread_id.as_str(),
                    boundary.turn_id.as_str(),
                    delivery_id,
                    event.turn_id.as_str(),
                    true,
                )
                .await?
                .is_some()
        }
    };
    if event.thread_id != boundary.thread_id
        || event.event_id != boundary.terminal_event_id
        || event.created_at.timestamp() != boundary.terminal_at_unix
        || !terminal_matches
    {
        bail!(
            "canonical terminal event `{}` does not match frozen boundary",
            boundary.terminal_event_id
        );
    }
    Ok(event)
}

async fn load_decoded_event<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    event_id: &str,
) -> Result<CanonicalTurnEventRecord> {
    let row = turn_event::Entity::find_by_id(event_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to load canonical event `{event_id}`"))?
        .with_context(|| format!("canonical event `{event_id}` is missing"))?;
    decode_event(row, workspace_id)
}

fn parent_turn_precedes_boundary(
    parent_turn: &turn::Model,
    boundary: &SelfImprovementThreadTerminalBoundary,
) -> bool {
    (parent_turn.created_at.timestamp(), parent_turn.id.as_str())
        < (
            boundary.parent_turn_created_at_unix,
            boundary.turn_id.as_str(),
        )
}

fn parent_turn_at_or_before_boundary(
    parent_turn: &turn::Model,
    boundary: &SelfImprovementThreadTerminalBoundary,
) -> bool {
    parent_turn.id == boundary.turn_id || parent_turn_precedes_boundary(parent_turn, boundary)
}

async fn load_decoded_turn_events<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Result<Vec<CanonicalTurnEventRecord>> {
    let owning_turn = turn::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to load canonical turn `{turn_id}`"))?
        .with_context(|| format!("canonical turn `{turn_id}` is missing"))?;
    if owning_turn.thread_id != thread_id {
        bail!(
            "canonical turn `{turn_id}` belongs to thread `{}`, not `{thread_id}`",
            owning_turn.thread_id
        );
    }
    let owning_actor = actor_ref_from_db(
        owning_turn.initiated_by_actor_kind.as_deref(),
        owning_turn.initiated_by_actor_id.as_deref(),
    )
    .with_context(|| format!("canonical turn `{turn_id}` has an invalid owning actor"))?;
    let current_message_input = if owning_turn.send_mode.as_deref() == Some("message") {
        let input = if owning_turn.message_deleted_at.is_some() {
            Vec::new()
        } else {
            super::turn::find_turn_inputs(db, turn_id)
                .await?
                .into_iter()
                .map(|row| {
                    serde_json::from_str::<UserInput>(row.payload.as_str()).with_context(|| {
                        format!(
                            "failed to decode current Message input `{}` for canonical history",
                            row.input_index
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?
        };
        Some(input)
    } else {
        None
    };

    let mut rows = turn_event::Entity::find()
        .filter(turn_event::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn_event::Column::TurnId.eq(turn_id.to_owned()))
        .all(db)
        .await
        .with_context(|| {
            format!("failed to load canonical events for turn `{turn_id}` in `{thread_id}`")
        })?;
    rows.sort_by(|left, right| {
        (left.sequence, left.id.as_str()).cmp(&(right.sequence, right.id.as_str()))
    });
    rows.into_iter()
        .map(|row| {
            let mut record = decode_event(row, workspace_id)?;
            hydrate_legacy_turn_start_actor(&mut record, owning_actor.as_ref())?;
            if let Some(input) = current_message_input.as_deref() {
                project_current_message_input(&mut record, input)?;
            }
            Ok(record)
        })
        .collect()
}

fn project_current_message_input(
    record: &mut CanonicalTurnEventRecord,
    input: &[UserInput],
) -> Result<()> {
    let CanonicalTurnEventPayload::TurnStarted(started) = &mut record.payload else {
        return Ok(());
    };
    if started.turn.mode != ThreadMode::Message {
        bail!(
            "canonical Message turn-start event `{}` has non-Message mode",
            record.event_id
        );
    }
    started.input = input.to_vec();
    Ok(())
}

fn hydrate_legacy_turn_start_actor(
    record: &mut CanonicalTurnEventRecord,
    owning_actor: Option<&pioneer_protocol::PersistedActorRef>,
) -> Result<()> {
    let CanonicalTurnEventPayload::TurnStarted(started) = &mut record.payload else {
        return Ok(());
    };
    match (started.actor.as_ref(), owning_actor) {
        (Some(event_actor), Some(owning_actor)) if event_actor != owning_actor => bail!(
            "canonical turn-start event `{}` actor does not match owning turn",
            record.event_id
        ),
        (Some(_), Some(_)) | (None, None) => {}
        (None, Some(owning_actor)) => {
            // Historical payloads remain append-only. Bootstrap backfills the projection, and
            // canonical history derives an effective actor only in this decoded in-memory copy.
            started.actor = Some(owning_actor.clone());
        }
        (Some(_), None) => bail!(
            "canonical turn-start event `{}` has an actor but owning turn does not",
            record.event_id
        ),
    }
    Ok(())
}

fn decode_event(row: turn_event::Model, workspace_id: &str) -> Result<CanonicalTurnEventRecord> {
    let mut payload = serde_json::from_str::<CanonicalTurnEventPayload>(row.payload.as_str())
        .with_context(|| format!("failed to decode canonical turn event `{}`", row.id))?;
    if payload.workspace_id() != workspace_id
        || payload.thread_id() != row.thread_id
        || payload.turn_id() != row.turn_id
    {
        bail!(
            "canonical turn event `{}` payload identity mismatch",
            row.id
        );
    }
    if payload.event_type() != row.event_type {
        bail!("canonical turn event `{}` payload type mismatch", row.id);
    }
    if let CanonicalTurnEventPayload::TurnStarted(started) = &mut payload {
        // The persisted event carries a client-facing Thread snapshot. Its preview and turn list
        // can contain unrelated concurrent exchanges, while the canonical history record is
        // scoped to the exact `started.turn` and `started.input` below.
        started.thread.preview.clear();
        started.thread.turns.clear();
    }
    Ok(CanonicalTurnEventRecord {
        event_id: row.id,
        thread_id: row.thread_id,
        turn_id: row.turn_id,
        sequence: row.sequence,
        created_at: row.created_at,
        payload,
    })
}

fn completed_delivery_identity(payload: &CanonicalTurnEventPayload) -> Option<&str> {
    let CanonicalTurnEventPayload::ItemCompleted(notification) = payload else {
        return None;
    };
    task_delivery_id_from_result_item_id(notification.item.item_id())
}

fn payload_item_id(payload: &CanonicalTurnEventPayload) -> Option<&str> {
    match payload {
        CanonicalTurnEventPayload::ItemStarted(notification) => Some(notification.item.item_id()),
        CanonicalTurnEventPayload::ItemCompleted(notification) => Some(notification.item.item_id()),
        CanonicalTurnEventPayload::ItemUpdated(notification) => Some(notification.item.item_id()),
        _ => None,
    }
}

fn is_terminal_status(status: TurnStatus) -> bool {
    matches!(
        status,
        TurnStatus::Completed | TurnStatus::Failed | TurnStatus::Interrupted | TurnStatus::Blocked
    )
}

fn terminal_status(payload: &CanonicalTurnEventPayload) -> Option<TurnStatus> {
    match payload {
        CanonicalTurnEventPayload::TurnCompleted(notification) => Some(notification.turn.status),
        CanonicalTurnEventPayload::TurnFailed(notification) => Some(notification.turn.status),
        CanonicalTurnEventPayload::TurnBlocked(notification) => Some(notification.turn.status),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use pioneer_protocol::{
        SandboxMode, Thread, ThreadSidebarVisibility, ThreadStatus, Turn,
        default_turn_permission_profile_snapshot,
    };

    use super::*;
    use crate::CanonicalTurnStartedEventPayload;

    fn message_start_record(mode: ThreadMode) -> CanonicalTurnEventRecord {
        let turn = Turn {
            id: "turn_message".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            mode,
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: default_turn_permission_profile_snapshot(),
        };
        CanonicalTurnEventRecord {
            event_id: "event_start".to_owned(),
            thread_id: "thread_message".to_owned(),
            turn_id: turn.id.clone(),
            sequence: 1,
            created_at: chrono::FixedOffset::east_opt(0)
                .expect("UTC offset")
                .timestamp_opt(1_900_000_000, 0)
                .single()
                .expect("timestamp"),
            payload: CanonicalTurnEventPayload::TurnStarted(CanonicalTurnStartedEventPayload {
                thread: Thread {
                    workspace_id: "workspace_message".to_owned(),
                    id: "thread_message".to_owned(),
                    name: None,
                    preview: String::new(),
                    mode: ThreadMode::Message,
                    model: "model".to_owned(),
                    model_provider: "provider".to_owned(),
                    reasoning_effort: None,
                    created_at: 1_900_000_000,
                    updated_at: 1_900_000_000,
                    status: ThreadStatus::Idle,
                    origin_kind: ThreadOriginKind::Collaborative,
                    sidebar_visibility: ThreadSidebarVisibility::Visible,
                    agent_nickname: None,
                    agent_role: None,
                    visibility: None,
                    turns: Vec::new(),
                },
                sandbox_mode: SandboxMode::FullAccess,
                turn,
                input: vec![UserInput::Text {
                    text: "append-only original".to_owned(),
                    text_elements: Vec::new(),
                }],
                actor: None,
                reasoning_effort: None,
            }),
        }
    }

    #[test]
    fn canonical_message_history_uses_current_input_and_rejects_mode_mismatch() {
        let current = vec![UserInput::Text {
            text: "current edited text".to_owned(),
            text_elements: Vec::new(),
        }];
        let mut message = message_start_record(ThreadMode::Message);
        project_current_message_input(&mut message, current.as_slice())
            .expect("Message projection should accept current input");
        let CanonicalTurnEventPayload::TurnStarted(started) = message.payload else {
            panic!("fixture must remain a TurnStarted event");
        };
        assert_eq!(started.input, current);

        let mut inconsistent = message_start_record(ThreadMode::Agent);
        assert!(project_current_message_input(&mut inconsistent, &[]).is_err());
    }
}
