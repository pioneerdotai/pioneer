use anyhow::{Context, Result};
use pioneer_entity::{
    cli_runtime_pending_request, thread as thread_entity, turn as turn_entity,
    turn_item as turn_item_entity,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};
use serde_json::json;

use crate::events::{AppendedTurnEvent, TurnEventPayload, TurnStartedEventPayload};
use crate::repositories::cli_runtime_binding::{
    CliRuntimePendingRequestRecord, CliRuntimePendingRequestStatus,
};
use crate::repositories::thread_timeline_projection as timeline_repository;
use crate::repositories::{thread, turn};
use crate::timeline_projection::{ProjectionPlacement, classify_turn_item_row};
use crate::timeline_projection_model::{
    ItemEventOrder, approval_block_id, assistant_block_id, classification_metadata_json,
    elapsed_ms, terminal_completed_at, terminal_state_block_id, terminal_turn_state,
    turn_block_sort_key, turn_work_presentation, turn_work_state, user_block_id, work_block_id,
    work_item_order_key, work_item_projection_id,
};
use crate::{
    ProjectionPageAnchor, ThreadTimelineBlockRecord, TurnWorkItemProjectionRecord,
    TurnWorkProjectionRecord, WORK_ITEM_STATUS_RUNNING, WORK_VISIBILITY_HIDDEN,
    WORK_VISIBILITY_VISIBLE,
};

pub(crate) async fn project_semantic_timeline_live_turn_event<C: ConnectionTrait>(
    db: &C,
    event: &AppendedTurnEvent,
) -> Result<()> {
    match &event.payload {
        TurnEventPayload::TurnStarted(payload) => {
            project_turn_started(db, event, payload)
                .await
                .with_context(|| {
                    format!(
                        "failed to project semantic timeline turn/start for turn `{}`",
                        payload.turn.id
                    )
                })?;
        }
        TurnEventPayload::ItemStarted(_)
        | TurnEventPayload::ItemCompleted(_)
        | TurnEventPayload::ItemUpdated(_)
        | TurnEventPayload::ItemTimeoutDetected(_)
        | TurnEventPayload::ItemRecoveryOpened(_)
        | TurnEventPayload::ItemRecoveryAttached(_)
        | TurnEventPayload::ItemRetryScheduled(_)
        | TurnEventPayload::ItemRetryAttemptStarted(_)
        | TurnEventPayload::ItemRecoverySucceeded(_)
        | TurnEventPayload::ItemRecoveryExhausted(_)
        | TurnEventPayload::ItemToolRetryScheduled(_)
        | TurnEventPayload::ItemToolRetryResolved(_)
        | TurnEventPayload::ItemToolRetryExhausted(_) => {
            project_turn_item_event(db, event).await.with_context(|| {
                format!(
                    "failed to project semantic timeline item event `{}` for turn `{}`",
                    event.id, event.turn_id
                )
            })?;
        }
        TurnEventPayload::TurnCompleted(_)
        | TurnEventPayload::TurnFailed(_)
        | TurnEventPayload::TurnBlocked(_) => {
            project_terminal_turn_event(db, event).await.with_context(|| {
                format!(
                    "failed to project semantic timeline terminal turn event `{}` for turn `{}`",
                    event.id, event.turn_id
                )
            })?;
        }
        TurnEventPayload::TurnToolLoopBudgetExceeded(_)
        | TurnEventPayload::TurnExecutionWindowStarted(_)
        | TurnEventPayload::TurnExecutionWindowExhausted(_)
        | TurnEventPayload::TurnExecutionWindowCheckpointed(_)
        | TurnEventPayload::TurnExecutionWindowContinued(_)
        | TurnEventPayload::TurnExecutionWindowBlocked(_)
        | TurnEventPayload::TurnPermissionAudit(_) => {}
    }

    Ok(())
}

pub(crate) async fn project_cli_runtime_pending_request<C: ConnectionTrait>(
    db: &C,
    request: &CliRuntimePendingRequestRecord,
) -> Result<()> {
    let Some(turn_id) = request.turn_id.as_deref() else {
        return Ok(());
    };
    let Some(turn_model) = turn::find_turn_by_id(db, turn_id).await? else {
        return Ok(());
    };
    let Some(thread_model) = thread::find_thread_by_id(db, turn_model.thread_id.as_str()).await?
    else {
        return Ok(());
    };

    if request.status != CliRuntimePendingRequestStatus::Pending {
        timeline_repository::delete_thread_timeline_block(
            db,
            approval_block_id(turn_model.id.as_str(), request.request_id.as_str()).as_str(),
        )
        .await?;
        return refresh_turn_work_summary(db, &thread_model, &turn_model, 0, request.updated_at)
            .await;
    }

    timeline_repository::upsert_thread_timeline_block(
        db,
        ThreadTimelineBlockRecord {
            block_id: approval_block_id(turn_model.id.as_str(), request.request_id.as_str()),
            workspace_id: thread_model.workspace_id.clone(),
            thread_id: thread_model.id.clone(),
            turn_id: Some(turn_model.id.clone()),
            block_kind: timeline_repository::BLOCK_KIND_APPROVAL.to_owned(),
            sort_key: approval_block_sort_key(&turn_model, request),
            source_kind: Some("cli_runtime_pending_request".to_owned()),
            source_key: Some(request.request_id.clone()),
            started_at: Some(request.created_at),
            completed_at: request.resolved_at,
            metadata_json: json!({
                "turnId": turn_model.id,
                "requestId": request.request_id,
                "requestKind": request.request_kind,
                "status": request.status.as_str(),
            })
            .to_string(),
            created_at: request.created_at,
            updated_at: request.updated_at,
        },
    )
    .await?;

    refresh_turn_work_summary(db, &thread_model, &turn_model, 0, request.updated_at).await
}

fn approval_block_sort_key(
    turn_model: &turn_entity::Model,
    request: &CliRuntimePendingRequestRecord,
) -> String {
    format!(
        "{:020}:{}:150:approval:{}",
        request.created_at.timestamp_millis().max(0),
        turn_model.id,
        request.request_id
    )
}

async fn project_turn_started<C: ConnectionTrait>(
    db: &C,
    event: &AppendedTurnEvent,
    payload: &TurnStartedEventPayload,
) -> Result<()> {
    let Some(thread_model) = thread::find_thread_by_id(db, payload.thread.id.as_str()).await?
    else {
        anyhow::bail!(
            "semantic timeline turn/start projection cannot find thread `{}`",
            payload.thread.id
        );
    };
    let Some(turn_model) = turn::find_turn_by_id(db, payload.turn.id.as_str()).await? else {
        anyhow::bail!(
            "semantic timeline turn/start projection cannot find turn `{}`",
            payload.turn.id
        );
    };

    timeline_repository::delete_thread_timeline_block(
        db,
        terminal_state_block_id(turn_model.id.as_str()).as_str(),
    )
    .await?;

    if !payload.input.is_empty() {
        timeline_repository::upsert_thread_timeline_block(
            db,
            ThreadTimelineBlockRecord {
                block_id: user_block_id(turn_model.id.as_str()),
                workspace_id: thread_model.workspace_id.clone(),
                thread_id: thread_model.id.clone(),
                turn_id: Some(turn_model.id.clone()),
                block_kind: timeline_repository::BLOCK_KIND_USER_MESSAGE.to_owned(),
                sort_key: turn_block_sort_key(&turn_model, 0, "user"),
                source_kind: Some("turn_input".to_owned()),
                source_key: Some(turn_model.id.clone()),
                started_at: Some(turn_model.created_at),
                completed_at: Some(turn_model.created_at),
                metadata_json: json!({
                    "turnId": turn_model.id,
                    "inputCount": payload.input.len(),
                })
                .to_string(),
                created_at: turn_model.created_at,
                updated_at: event.created_at,
            },
        )
        .await?;
    }

    refresh_turn_work_summary(
        db,
        &thread_model,
        &turn_model,
        event.sequence,
        event.created_at,
    )
    .await
}

async fn project_terminal_turn_state<C: ConnectionTrait>(
    db: &C,
    thread_model: &thread_entity::Model,
    turn_model: &turn_entity::Model,
    projected_at: DateTimeWithTimeZone,
) -> Result<()> {
    let block_id = terminal_state_block_id(turn_model.id.as_str());
    let has_final = timeline_repository::count_thread_timeline_blocks_for_turn_kind(
        db,
        turn_model.id.as_str(),
        timeline_repository::BLOCK_KIND_ASSISTANT_MESSAGE,
    )
    .await?
        > 0;
    if has_final {
        timeline_repository::delete_thread_timeline_block(db, block_id.as_str()).await?;
        return Ok(());
    }
    let Some(state) = terminal_turn_state(turn_model) else {
        timeline_repository::delete_thread_timeline_block(db, block_id.as_str()).await?;
        return Ok(());
    };

    timeline_repository::upsert_thread_timeline_block(
        db,
        ThreadTimelineBlockRecord {
            block_id,
            workspace_id: thread_model.workspace_id.clone(),
            thread_id: thread_model.id.clone(),
            turn_id: Some(turn_model.id.clone()),
            block_kind: timeline_repository::BLOCK_KIND_SYSTEM.to_owned(),
            sort_key: turn_block_sort_key(turn_model, 300, "terminal-state"),
            source_kind: Some("turn_terminal_state".to_owned()),
            source_key: Some(turn_model.id.clone()),
            started_at: Some(projected_at),
            completed_at: Some(projected_at),
            metadata_json: json!({
                "state": state,
                "message": turn_model.error,
            })
            .to_string(),
            created_at: projected_at,
            updated_at: projected_at,
        },
    )
    .await
}

async fn project_turn_item_event<C: ConnectionTrait>(
    db: &C,
    event: &AppendedTurnEvent,
) -> Result<()> {
    let Some(item_id) = payload_item_id(&event.payload) else {
        return Ok(());
    };
    let Some(turn_model) = turn::find_turn_by_id(db, event.turn_id.as_str()).await? else {
        anyhow::bail!(
            "semantic timeline item projection cannot find turn `{}`",
            event.turn_id
        );
    };
    let Some(thread_model) = thread::find_thread_by_id(db, turn_model.thread_id.as_str()).await?
    else {
        anyhow::bail!(
            "semantic timeline item projection cannot find thread `{}`",
            turn_model.thread_id
        );
    };
    let item_model = turn::find_turn_item(db, turn_model.id.as_str(), item_id).await?;
    let Some(item_model) = item_model else {
        if item_payload_event_requires_canonical_row(&event.payload) {
            anyhow::bail!(
                "semantic timeline item projection cannot find turn_item `{}` in turn `{}`",
                item_id,
                turn_model.id
            );
        }
        return refresh_turn_work_summary(
            db,
            &thread_model,
            &turn_model,
            event.sequence,
            event.created_at,
        )
        .await;
    };

    let refresh_work_summary = !matches!(
        classify_turn_item_row(&item_model).placement,
        ProjectionPlacement::TopLevelUserMessage
    );

    project_turn_item_row(db, &thread_model, &turn_model, &item_model, event).await?;
    if !refresh_work_summary {
        return Ok(());
    }
    refresh_turn_work_summary(
        db,
        &thread_model,
        &turn_model,
        event.sequence,
        event.created_at,
    )
    .await
}

pub(crate) async fn project_semantic_timeline_snapshot_turn_item<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
    source_sequence: i64,
    refreshed_at: DateTimeWithTimeZone,
) -> Result<()> {
    let Some(turn_model) = turn::find_turn_by_id(db, turn_id).await? else {
        anyhow::bail!("semantic timeline snapshot cannot find turn `{turn_id}`");
    };
    let Some(thread_model) = thread::find_thread_by_id(db, turn_model.thread_id.as_str()).await?
    else {
        anyhow::bail!(
            "semantic timeline snapshot cannot find thread `{}`",
            turn_model.thread_id
        );
    };
    let Some(item_model) = turn::find_turn_item(db, turn_model.id.as_str(), item_id).await? else {
        return refresh_turn_work_summary(
            db,
            &thread_model,
            &turn_model,
            source_sequence,
            refreshed_at,
        )
        .await;
    };

    let refresh_work_summary = !matches!(
        classify_turn_item_row(&item_model).placement,
        ProjectionPlacement::TopLevelUserMessage
    );

    project_snapshot_turn_item_row(
        db,
        &thread_model,
        &turn_model,
        &item_model,
        source_sequence,
        refreshed_at,
    )
    .await?;
    if !refresh_work_summary {
        return Ok(());
    }
    refresh_turn_work_summary(
        db,
        &thread_model,
        &turn_model,
        source_sequence,
        refreshed_at,
    )
    .await
}

async fn project_terminal_turn_event<C: ConnectionTrait>(
    db: &C,
    event: &AppendedTurnEvent,
) -> Result<()> {
    let Some(turn_model) = turn::find_turn_by_id(db, event.turn_id.as_str()).await? else {
        anyhow::bail!(
            "semantic timeline terminal projection cannot find turn `{}`",
            event.turn_id
        );
    };
    let Some(thread_model) = thread::find_thread_by_id(db, turn_model.thread_id.as_str()).await?
    else {
        anyhow::bail!(
            "semantic timeline terminal projection cannot find thread `{}`",
            turn_model.thread_id
        );
    };

    let mut after_order_key: Option<String> = None;
    loop {
        let running_rows = timeline_repository::list_turn_work_items_by_status_page(
            db,
            turn_model.id.as_str(),
            WORK_ITEM_STATUS_RUNNING,
            after_order_key.as_deref(),
            128,
        )
        .await?;
        if running_rows.is_empty() {
            break;
        }

        for row in &running_rows {
            if let Some(item_model) =
                turn::find_turn_item(db, turn_model.id.as_str(), row.item_id.as_str()).await?
            {
                project_turn_item_row(db, &thread_model, &turn_model, &item_model, event).await?;
            }
        }

        after_order_key = running_rows.last().map(|row| row.order_key.clone());
    }

    refresh_turn_work_summary(
        db,
        &thread_model,
        &turn_model,
        event.sequence,
        event.created_at,
    )
    .await?;
    project_terminal_turn_state(db, &thread_model, &turn_model, event.created_at).await
}

async fn project_turn_item_row<C: ConnectionTrait>(
    db: &C,
    thread_model: &thread_entity::Model,
    turn_model: &turn_entity::Model,
    item_model: &turn_item_entity::Model,
    event: &AppendedTurnEvent,
) -> Result<()> {
    let classification = classify_turn_item_row(item_model);

    match classification.placement {
        ProjectionPlacement::TopLevelUserMessage => {}
        ProjectionPlacement::TopLevelAssistantMessage => {
            let source_order =
                resolve_item_source_order(db, turn_model.id.as_str(), item_model, event).await?;
            let order_key = work_item_order_key(item_model, Some(&source_order));
            timeline_repository::delete_turn_work_item_projection_for_item(
                db,
                turn_model.id.as_str(),
                item_model.item_id.as_str(),
            )
            .await?;
            timeline_repository::upsert_thread_timeline_block(
                db,
                ThreadTimelineBlockRecord {
                    block_id: assistant_block_id(
                        turn_model.id.as_str(),
                        item_model.item_id.as_str(),
                    ),
                    workspace_id: thread_model.workspace_id.clone(),
                    thread_id: thread_model.id.clone(),
                    turn_id: Some(turn_model.id.clone()),
                    block_kind: timeline_repository::BLOCK_KIND_ASSISTANT_MESSAGE.to_owned(),
                    sort_key: turn_block_sort_key(&turn_model, 200, order_key.as_str()),
                    source_kind: Some("turn_item".to_owned()),
                    source_key: Some(item_model.item_id.clone()),
                    started_at: Some(item_model.created_at),
                    completed_at: Some(item_model.updated_at),
                    metadata_json: json!({
                        "turnId": turn_model.id,
                        "itemId": item_model.item_id,
                        "classification": classification.classification_str(),
                    })
                    .to_string(),
                    created_at: item_model.created_at,
                    updated_at: event.created_at,
                },
            )
            .await?;
            timeline_repository::delete_thread_timeline_block(
                db,
                terminal_state_block_id(turn_model.id.as_str()).as_str(),
            )
            .await?;
        }
        ProjectionPlacement::TurnWork | ProjectionPlacement::Hidden => {
            let source_order =
                resolve_item_source_order(db, turn_model.id.as_str(), item_model, event).await?;
            let order_key = work_item_order_key(item_model, Some(&source_order));
            timeline_repository::delete_thread_timeline_block(
                db,
                assistant_block_id(turn_model.id.as_str(), item_model.item_id.as_str()).as_str(),
            )
            .await?;
            timeline_repository::upsert_turn_work_item_projection(
                db,
                TurnWorkItemProjectionRecord {
                    work_item_id: work_item_projection_id(
                        turn_model.id.as_str(),
                        item_model.item_id.as_str(),
                    ),
                    workspace_id: thread_model.workspace_id.clone(),
                    thread_id: thread_model.id.clone(),
                    turn_id: turn_model.id.clone(),
                    item_id: item_model.item_id.clone(),
                    source_event_id: Some(source_order.event_id),
                    source_sequence: source_order.sequence,
                    order_key,
                    item_type: item_model.item_type.clone(),
                    visibility: classification.visibility_str().to_owned(),
                    classification: classification.classification_str().to_owned(),
                    status: classification.status.to_owned(),
                    started_at: Some(item_model.created_at),
                    completed_at: Some(item_model.updated_at),
                    metadata_json: classification_metadata_json(&classification),
                    created_at: item_model.created_at,
                    updated_at: event.created_at,
                },
            )
            .await?;
        }
    }

    Ok(())
}

async fn project_snapshot_turn_item_row<C: ConnectionTrait>(
    db: &C,
    thread_model: &thread_entity::Model,
    turn_model: &turn_entity::Model,
    item_model: &turn_item_entity::Model,
    source_sequence: i64,
    refreshed_at: DateTimeWithTimeZone,
) -> Result<()> {
    let classification = classify_turn_item_row(item_model);

    match classification.placement {
        ProjectionPlacement::TopLevelUserMessage => {}
        ProjectionPlacement::TopLevelAssistantMessage => {
            let source_order = resolve_snapshot_item_source_order(
                db,
                turn_model.id.as_str(),
                item_model,
                source_sequence,
            )
            .await?;
            let order_key = work_item_order_key(item_model, Some(&source_order));
            timeline_repository::delete_turn_work_item_projection_for_item(
                db,
                turn_model.id.as_str(),
                item_model.item_id.as_str(),
            )
            .await?;
            timeline_repository::upsert_thread_timeline_block(
                db,
                ThreadTimelineBlockRecord {
                    block_id: assistant_block_id(
                        turn_model.id.as_str(),
                        item_model.item_id.as_str(),
                    ),
                    workspace_id: thread_model.workspace_id.clone(),
                    thread_id: thread_model.id.clone(),
                    turn_id: Some(turn_model.id.clone()),
                    block_kind: timeline_repository::BLOCK_KIND_ASSISTANT_MESSAGE.to_owned(),
                    sort_key: turn_block_sort_key(&turn_model, 200, order_key.as_str()),
                    source_kind: Some("turn_item".to_owned()),
                    source_key: Some(item_model.item_id.clone()),
                    started_at: Some(item_model.created_at),
                    completed_at: Some(item_model.updated_at),
                    metadata_json: json!({
                        "turnId": turn_model.id,
                        "itemId": item_model.item_id,
                        "classification": classification.classification_str(),
                    })
                    .to_string(),
                    created_at: item_model.created_at,
                    updated_at: refreshed_at,
                },
            )
            .await?;
            timeline_repository::delete_thread_timeline_block(
                db,
                terminal_state_block_id(turn_model.id.as_str()).as_str(),
            )
            .await?;
        }
        ProjectionPlacement::TurnWork | ProjectionPlacement::Hidden => {
            let source_order = resolve_snapshot_item_source_order(
                db,
                turn_model.id.as_str(),
                item_model,
                source_sequence,
            )
            .await?;
            let order_key = work_item_order_key(item_model, Some(&source_order));
            let source_event_id =
                (!source_order.event_id.is_empty()).then(|| source_order.event_id.clone());
            timeline_repository::delete_thread_timeline_block(
                db,
                assistant_block_id(turn_model.id.as_str(), item_model.item_id.as_str()).as_str(),
            )
            .await?;
            timeline_repository::upsert_turn_work_item_projection(
                db,
                TurnWorkItemProjectionRecord {
                    work_item_id: work_item_projection_id(
                        turn_model.id.as_str(),
                        item_model.item_id.as_str(),
                    ),
                    workspace_id: thread_model.workspace_id.clone(),
                    thread_id: thread_model.id.clone(),
                    turn_id: turn_model.id.clone(),
                    item_id: item_model.item_id.clone(),
                    source_event_id,
                    source_sequence: source_order.sequence,
                    order_key,
                    item_type: item_model.item_type.clone(),
                    visibility: classification.visibility_str().to_owned(),
                    classification: classification.classification_str().to_owned(),
                    status: classification.status.to_owned(),
                    started_at: Some(item_model.created_at),
                    completed_at: Some(item_model.updated_at),
                    metadata_json: classification_metadata_json(&classification),
                    created_at: item_model.created_at,
                    updated_at: refreshed_at,
                },
            )
            .await?;
        }
    }

    Ok(())
}

async fn resolve_item_source_order<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_model: &turn_item_entity::Model,
    event: &AppendedTurnEvent,
) -> Result<ItemEventOrder> {
    let work_item_id = work_item_projection_id(turn_id, item_model.item_id.as_str());
    if let Some(existing) =
        timeline_repository::find_turn_work_item_projection(db, work_item_id.as_str()).await?
        && existing.source_sequence > 0
    {
        return Ok(ItemEventOrder {
            event_id: existing.source_event_id.unwrap_or_else(|| event.id.clone()),
            sequence: existing.source_sequence,
        });
    }

    Ok(ItemEventOrder {
        event_id: event.id.clone(),
        sequence: event.sequence,
    })
}

async fn resolve_snapshot_item_source_order<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_model: &turn_item_entity::Model,
    fallback_sequence: i64,
) -> Result<ItemEventOrder> {
    let work_item_id = work_item_projection_id(turn_id, item_model.item_id.as_str());
    if let Some(existing) =
        timeline_repository::find_turn_work_item_projection(db, work_item_id.as_str()).await?
        && existing.source_sequence > 0
    {
        return Ok(ItemEventOrder {
            event_id: existing.source_event_id.unwrap_or_default(),
            sequence: existing.source_sequence,
        });
    }

    Ok(ItemEventOrder {
        event_id: String::new(),
        sequence: fallback_sequence.max(0),
    })
}

async fn refresh_turn_work_summary<C: ConnectionTrait>(
    db: &C,
    thread_model: &thread_entity::Model,
    turn_model: &turn_entity::Model,
    source_sequence: i64,
    refreshed_at: DateTimeWithTimeZone,
) -> Result<()> {
    let visible_work_count = count_to_i64(
        timeline_repository::count_turn_work_items(
            db,
            turn_model.id.as_str(),
            Some(WORK_VISIBILITY_VISIBLE),
        )
        .await?,
    );
    let hidden_work_count = count_to_i64(
        timeline_repository::count_turn_work_items(
            db,
            turn_model.id.as_str(),
            Some(WORK_VISIBILITY_HIDDEN),
        )
        .await?,
    );
    let work_count = visible_work_count.saturating_add(hidden_work_count);
    let has_final = timeline_repository::count_thread_timeline_blocks_for_turn_kind(
        db,
        turn_model.id.as_str(),
        timeline_repository::BLOCK_KIND_ASSISTANT_MESSAGE,
    )
    .await?
        > 0;
    let needs_work_block = work_count > 0 || !has_final;

    if !needs_work_block {
        timeline_repository::delete_turn_work_projection(db, turn_model.id.as_str()).await?;
        timeline_repository::delete_thread_timeline_block(
            db,
            work_block_id(turn_model.id.as_str()).as_str(),
        )
        .await?;
        return Ok(());
    }

    let first_work_item_id =
        first_or_last_visible_work_item_id(db, turn_model.id.as_str(), ProjectionPageAnchor::Start)
            .await?;
    let last_work_item_id =
        first_or_last_visible_work_item_id(db, turn_model.id.as_str(), ProjectionPageAnchor::End)
            .await?;
    let existing =
        timeline_repository::find_turn_work_projection(db, turn_model.id.as_str()).await?;
    let source_high_watermark = existing
        .as_ref()
        .map(|projection| projection.source_high_watermark)
        .unwrap_or(0)
        .max(source_sequence);
    let summary_updated_at = max_datetime(turn_model.updated_at, refreshed_at);
    let pending_request_count =
        count_pending_cli_runtime_requests(db, thread_model.id.as_str(), turn_model.id.as_str())
            .await?;
    let running_item_state =
        find_running_item_state(db, turn_model.id.as_str(), summary_updated_at).await?;
    let has_running_item = running_item_state.item_id.is_some();
    let presentation = turn_work_presentation(turn_model, has_final);
    let state = turn_work_state(
        turn_model,
        pending_request_count,
        has_running_item,
        running_item_state.stale,
    );

    timeline_repository::upsert_turn_work_projection(
        db,
        TurnWorkProjectionRecord {
            turn_id: turn_model.id.clone(),
            workspace_id: thread_model.workspace_id.clone(),
            thread_id: thread_model.id.clone(),
            presentation: presentation.to_owned(),
            state: state.to_owned(),
            work_count,
            visible_work_count,
            hidden_work_count,
            first_work_item_id: first_work_item_id.clone(),
            last_work_item_id: last_work_item_id.clone(),
            started_at: Some(turn_model.created_at),
            completed_at: terminal_completed_at(turn_model),
            elapsed_ms: Some(elapsed_ms(turn_model.created_at, summary_updated_at)),
            source_high_watermark,
            metadata_json: json!({
                "hasFinalAssistantMessage": has_final,
                "pendingRequestCount": pending_request_count,
                "runningItemId": running_item_state.item_id,
                "staleRuntimeState": running_item_state.stale_reason,
                "presentation": presentation,
                "state": state,
            })
            .to_string(),
            created_at: turn_model.created_at,
            updated_at: summary_updated_at,
        },
    )
    .await?;

    timeline_repository::upsert_thread_timeline_block(
        db,
        ThreadTimelineBlockRecord {
            block_id: work_block_id(turn_model.id.as_str()),
            workspace_id: thread_model.workspace_id.clone(),
            thread_id: thread_model.id.clone(),
            turn_id: Some(turn_model.id.clone()),
            block_kind: timeline_repository::BLOCK_KIND_TURN_WORK.to_owned(),
            sort_key: turn_block_sort_key(turn_model, 100, "work"),
            source_kind: Some("turn_work".to_owned()),
            source_key: Some(turn_model.id.clone()),
            started_at: Some(turn_model.created_at),
            completed_at: terminal_completed_at(turn_model),
            metadata_json: json!({
                "turnId": turn_model.id,
                "workCount": work_count,
                "visibleWorkCount": visible_work_count,
                "hiddenWorkCount": hidden_work_count,
            })
            .to_string(),
            created_at: turn_model.created_at,
            updated_at: summary_updated_at,
        },
    )
    .await?;

    Ok(())
}

async fn first_or_last_visible_work_item_id<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    anchor: ProjectionPageAnchor<'_>,
) -> Result<Option<String>> {
    let row = timeline_repository::list_turn_work_items_page(
        db,
        turn_id,
        Some(WORK_VISIBILITY_VISIBLE),
        anchor,
        1,
    )
    .await?
    .into_iter()
    .next();
    Ok(row.map(|row| row.work_item_id))
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RunningItemState {
    item_id: Option<String>,
    stale: bool,
    stale_reason: Option<&'static str>,
}

async fn find_running_item_state<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<RunningItemState> {
    let running = Condition::any()
        .add(turn_item_entity::Column::Status.eq("in_progress"))
        .add(turn_item_entity::Column::ActiveAttemptStatus.eq("running"));
    let active = turn_item_entity::Entity::find()
        .filter(turn_item_entity::Column::TurnId.eq(turn_id.to_owned()))
        .filter(running.clone())
        .filter(
            Condition::any()
                .add(turn_item_entity::Column::LeaseExpiresAt.is_null())
                .add(turn_item_entity::Column::LeaseExpiresAt.gt(now)),
        )
        .order_by_desc(turn_item_entity::Column::CreatedAt)
        .one(db)
        .await
        .with_context(|| format!("failed to query running turn item state for turn `{turn_id}`"))?;
    if let Some(active) = active {
        return Ok(select_running_item_state(Some(active.item_id), None));
    }

    let latest = turn_item_entity::Entity::find()
        .filter(turn_item_entity::Column::TurnId.eq(turn_id.to_owned()))
        .filter(running)
        .order_by_desc(turn_item_entity::Column::CreatedAt)
        .one(db)
        .await
        .with_context(|| format!("failed to query stalled turn item state for turn `{turn_id}`"))?;

    Ok(select_running_item_state(
        None,
        latest.map(|item| item.item_id),
    ))
}

fn select_running_item_state(
    active_item_id: Option<String>,
    latest_stale_item_id: Option<String>,
) -> RunningItemState {
    if let Some(item_id) = active_item_id {
        return RunningItemState {
            item_id: Some(item_id),
            stale: false,
            stale_reason: None,
        };
    }
    let Some(item_id) = latest_stale_item_id else {
        return RunningItemState::default();
    };
    RunningItemState {
        item_id: Some(item_id),
        stale: true,
        stale_reason: Some("expired_lease"),
    }
}

async fn count_pending_cli_runtime_requests<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<i64> {
    let count = cli_runtime_pending_request::Entity::find()
        .filter(cli_runtime_pending_request::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(cli_runtime_pending_request::Column::TurnId.eq(turn_id.to_owned()))
        .filter(cli_runtime_pending_request::Column::Status.eq("pending"))
        .count(db)
        .await
        .with_context(|| {
            format!("failed to count pending CLI runtime requests for turn `{turn_id}`")
        })?;
    Ok(i64::try_from(count).unwrap_or(i64::MAX))
}

fn payload_item_id(payload: &TurnEventPayload) -> Option<&str> {
    match payload {
        TurnEventPayload::ItemStarted(notification) => Some(notification.item.item_id()),
        TurnEventPayload::ItemCompleted(notification) => Some(notification.item.item_id()),
        TurnEventPayload::ItemUpdated(notification) => Some(notification.item.item_id()),
        TurnEventPayload::ItemTimeoutDetected(notification) => Some(notification.item_id.as_str()),
        TurnEventPayload::ItemRecoveryOpened(notification) => Some(notification.item_id.as_str()),
        TurnEventPayload::ItemRecoveryAttached(notification) => Some(notification.item_id.as_str()),
        TurnEventPayload::ItemRetryScheduled(notification) => Some(notification.item_id.as_str()),
        TurnEventPayload::ItemRetryAttemptStarted(notification) => {
            Some(notification.item_id.as_str())
        }
        TurnEventPayload::ItemRecoverySucceeded(notification) => {
            Some(notification.item_id.as_str())
        }
        TurnEventPayload::ItemRecoveryExhausted(notification) => {
            Some(notification.item_id.as_str())
        }
        TurnEventPayload::ItemToolRetryScheduled(notification) => {
            Some(notification.item_id.as_str())
        }
        TurnEventPayload::ItemToolRetryResolved(notification) => {
            Some(notification.item_id.as_str())
        }
        TurnEventPayload::ItemToolRetryExhausted(notification) => {
            Some(notification.item_id.as_str())
        }
        TurnEventPayload::TurnStarted(_)
        | TurnEventPayload::TurnToolLoopBudgetExceeded(_)
        | TurnEventPayload::TurnExecutionWindowStarted(_)
        | TurnEventPayload::TurnExecutionWindowExhausted(_)
        | TurnEventPayload::TurnExecutionWindowCheckpointed(_)
        | TurnEventPayload::TurnExecutionWindowContinued(_)
        | TurnEventPayload::TurnExecutionWindowBlocked(_)
        | TurnEventPayload::TurnPermissionAudit(_)
        | TurnEventPayload::TurnCompleted(_)
        | TurnEventPayload::TurnFailed(_)
        | TurnEventPayload::TurnBlocked(_) => None,
    }
}

fn item_payload_event_requires_canonical_row(payload: &TurnEventPayload) -> bool {
    matches!(
        payload,
        TurnEventPayload::ItemStarted(_)
            | TurnEventPayload::ItemCompleted(_)
            | TurnEventPayload::ItemUpdated(_)
    )
}

fn count_to_i64(count: u64) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

fn max_datetime(left: DateTimeWithTimeZone, right: DateTimeWithTimeZone) -> DateTimeWithTimeZone {
    if left >= right { left } else { right }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_active_item_wins_over_stale_orphan() {
        let selected = select_running_item_state(
            Some("active_command".to_owned()),
            Some("old_reasoning".to_owned()),
        );
        assert_eq!(selected.item_id.as_deref(), Some("active_command"));
        assert!(!selected.stale);
        assert_eq!(selected.stale_reason, None);
    }

    #[test]
    fn all_stale_items_keep_stalled_turn_visible() {
        let selected = select_running_item_state(None, Some("old_command".to_owned()));
        assert_eq!(selected.item_id.as_deref(), Some("old_command"));
        assert!(selected.stale);
        assert_eq!(selected.stale_reason, Some("expired_lease"));
    }
}
