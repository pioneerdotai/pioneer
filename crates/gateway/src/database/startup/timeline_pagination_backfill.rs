use anyhow::{Context, Result};
use pioneer_crud as timeline_repository;
use pioneer_crud::{
    CrudStore, ProjectionMetaRecord, ProjectionPlacement, ProjectionVisibility,
    SEMANTIC_TIMELINE_PROJECTION_KEY, SEMANTIC_TIMELINE_PROJECTION_VERSION,
    SemanticTimelineRevisionFence, ThreadTimelineBlockRecord, TurnItemProjectionClassification,
    TurnWorkItemProjectionRecord, TurnWorkProjectionRecord, WORK_ITEM_STATUS_RUNNING,
    WorkItemClassification, assistant_block_id, classify_turn_item_row_for_turn,
    detached_task_run_block_id, terminal_state_block_id, user_block_id, work_block_id,
    work_item_projection_id,
};
use pioneer_entity::{
    cli_runtime_pending_request, thread, thread_timeline_block, turn, turn_cli_runtime_binding,
    turn_event, turn_input, turn_item, turn_work_item_projection,
};
use pioneer_protocol::TurnItem;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, Statement, TransactionTrait,
};
use serde_json::{Value as JsonValue, json};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use tracing::{debug, info, warn};

const DEFAULT_BACKFILL_BATCH_SIZE: u64 = 32;
const MAX_BACKFILL_BATCH_SIZE: u64 = 256;
const SOURCE_COUNT_BATCH_SIZE: u64 = 1_024;
const TERMINAL_APPROVAL_BLOCK_CLEANUP_KEY: &str = "semantic_timeline_terminal_approval_cleanup";
const TERMINAL_APPROVAL_BLOCK_CLEANUP_VERSION: i64 = 1;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct SemanticTimelineBackfillSourceCounts {
    pub threads: i64,
    pub turns: i64,
    pub turn_items: i64,
    pub turn_events: i64,
}

impl SemanticTimelineBackfillSourceCounts {
    pub fn is_empty(&self) -> bool {
        self.threads == 0 && self.turns == 0 && self.turn_items == 0 && self.turn_events == 0
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct SemanticTimelineBackfillSummary {
    pub skipped: bool,
    pub source_counts: SemanticTimelineBackfillSourceCounts,
    pub threads_seen: usize,
    pub turns_seen: usize,
    pub timeline_blocks_upserted: usize,
    pub work_projections_upserted: usize,
    pub work_items_upserted: usize,
    pub hidden_work_items: usize,
    pub invalid_items: usize,
}

#[derive(Debug, Clone)]
struct AssistantBlockCandidate {
    item_id: String,
    order_key: String,
    started_at: DateTimeWithTimeZone,
    completed_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone)]
struct DetachedTaskRunBlockCandidate {
    item_id: String,
    started_at: DateTimeWithTimeZone,
    completed_at: Option<DateTimeWithTimeZone>,
    updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, Default)]
struct TurnBackfillStats {
    timeline_blocks_upserted: usize,
    work_projection_upserted: bool,
    work_items_upserted: usize,
    hidden_work_items: usize,
    invalid_items: usize,
}

#[derive(Debug, Clone, Default)]
struct TurnBackfillPlan {
    stats: TurnBackfillStats,
    timeline_blocks: Vec<ThreadTimelineBlockRecord>,
    work_items: Vec<TurnWorkItemProjectionRecord>,
    work_projection: Option<TurnWorkProjectionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TurnBackfillSourceRevision {
    turn: turn::Model,
    live_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ItemEventOrder {
    event_id: String,
    sequence: i64,
    completed_at: Option<DateTimeWithTimeZone>,
}

fn classification_metadata_json(classification: &TurnItemProjectionClassification) -> String {
    json!({
        "placement": classification.placement_str(),
        "classification": classification.classification_str(),
        "audit": classification.audit,
        "auditReason": classification.audit_reason,
    })
    .to_string()
}

fn datetime_millis(value: DateTimeWithTimeZone) -> i64 {
    value.timestamp_millis()
}

fn elapsed_ms(started_at: DateTimeWithTimeZone, completed_at: DateTimeWithTimeZone) -> i64 {
    completed_at
        .signed_duration_since(started_at)
        .num_milliseconds()
        .max(0)
}

fn min_datetime(left: DateTimeWithTimeZone, right: DateTimeWithTimeZone) -> DateTimeWithTimeZone {
    if left <= right { left } else { right }
}

fn detached_task_run_origin_hint(turn_model: &turn::Model) -> bool {
    turn_model.turn_kind == "task_run"
        && matches!(
            turn_model.origin.as_str(),
            "scheduled_task" | "detached_task"
        )
}

fn work_item_order_key(item: &turn_item::Model, source_order: Option<&ItemEventOrder>) -> String {
    if let Some(source_order) = source_order {
        return format!("{:020}:{}", source_order.sequence.max(0), item.item_id);
    }
    format!(
        "z:{:020}:{}",
        datetime_millis(item.created_at).max(0),
        item.item_id
    )
}

fn turn_block_sort_key(turn: &turn::Model, rank: u16, suffix: &str) -> String {
    format!(
        "{:020}:{}:{:03}:{}",
        datetime_millis(turn.created_at).max(0),
        turn.id,
        rank,
        suffix
    )
}

fn timeline_event_block_sort_key(
    occurred_at: DateTimeWithTimeZone,
    turn_id: &str,
    rank: u16,
    suffix: &str,
) -> String {
    format!(
        "{:020}:{}:{:03}:{}",
        datetime_millis(occurred_at).max(0),
        turn_id,
        rank,
        suffix
    )
}

fn turn_work_presentation(turn: &turn::Model, has_final: bool) -> &'static str {
    if has_final {
        "collapsed_after_final"
    } else if turn_is_terminal(turn) {
        "collapsed_after_final"
    } else {
        "expanded_live"
    }
}

fn turn_work_state(
    turn_status: &str,
    cli_runtime_status: Option<&str>,
    pending_request_count: i64,
    has_stale_running_item: bool,
) -> &'static str {
    match turn_status {
        "completed" => return "completed",
        "failed" => return "failed",
        "interrupted" => return "interrupted",
        "blocked" => return "blocked",
        _ => {}
    }

    if pending_request_count > 0 {
        return "waiting_for_approval";
    }

    if has_stale_running_item {
        return "stalled";
    }

    if turn_status == "in_progress" {
        // Native/API turns are already running once TurnStarted is durable.
        // CLI runtimes alone persist an explicit pre-native-start state.
        return match cli_runtime_status {
            Some("starting") => "starting",
            _ => "running",
        };
    }

    "running"
}

fn turn_is_terminal(turn: &turn::Model) -> bool {
    turn_status_is_terminal(turn.status.as_str())
}

fn turn_status_is_terminal(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "interrupted" | "blocked")
}

fn terminal_turn_state(turn: &turn::Model) -> Option<&'static str> {
    match turn.status.as_str() {
        "failed" => Some("failed"),
        "interrupted" => Some("interrupted"),
        "blocked" => Some("blocked"),
        _ => None,
    }
}

fn terminal_completed_at(turn: &turn::Model) -> Option<DateTimeWithTimeZone> {
    if turn_is_terminal(turn) {
        Some(turn.updated_at)
    } else {
        None
    }
}

fn is_running_work_status(status: &str) -> bool {
    status == WORK_ITEM_STATUS_RUNNING
}

fn is_stale_running_turn_item(item: &turn_item::Model, now: DateTimeWithTimeZone) -> bool {
    let running = item.status.as_deref() == Some("in_progress")
        || item.active_attempt_status.as_deref() == Some("running");
    running
        && item
            .lease_expires_at
            .is_some_and(|deadline| deadline <= now)
}

pub(crate) async fn run(crud_store: &CrudStore) -> Result<()> {
    let removed = cleanup_terminal_approval_blocks_once(crud_store)
        .await
        .inspect_err(|error| {
            warn!(
                error = %format!("{error:#}"),
                "terminal approval timeline cleanup failed at startup"
            );
        })?;
    if removed > 0 {
        info!(
            removed,
            "removed terminal approval blocks from semantic timeline projection"
        );
    }

    match backfill_once(crud_store, DEFAULT_BACKFILL_BATCH_SIZE).await {
        Ok(summary) => {
            if summary.skipped {
                debug!("timeline pagination backfill skipped");
            } else if summary.source_counts.is_empty() {
                info!("timeline pagination backfill marked complete for empty database");
            } else {
                info!(
                    threads = summary.threads_seen,
                    turns = summary.turns_seen,
                    blocks = summary.timeline_blocks_upserted,
                    work_projections = summary.work_projections_upserted,
                    work_items = summary.work_items_upserted,
                    hidden_work_items = summary.hidden_work_items,
                    invalid_items = summary.invalid_items,
                    "timeline pagination backfill completed"
                );
            }
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "timeline pagination backfill failed at startup"
            );
            return Err(error);
        }
    }
    Ok(())
}

async fn cleanup_terminal_approval_blocks_once(crud_store: &CrudStore) -> Result<u64> {
    let db = crud_store.database_connection();
    if terminal_approval_cleanup_is_current(&db).await? {
        return Ok(0);
    }

    let mut removed = 0_u64;
    loop {
        let batch_removed = delete_terminal_approval_blocks_batch(&db).await?;
        removed = removed.saturating_add(batch_removed);
        if batch_removed == 0 {
            break;
        }
        super::maintenance_checkpoint().await?;
    }
    mark_terminal_approval_cleanup_complete(&db).await?;
    Ok(removed)
}

async fn terminal_approval_cleanup_is_current(db: &DatabaseConnection) -> Result<bool> {
    let Some(meta) =
        timeline_repository::find_projection_meta(db, TERMINAL_APPROVAL_BLOCK_CLEANUP_KEY).await?
    else {
        return Ok(false);
    };

    Ok(
        meta.projection_version == TERMINAL_APPROVAL_BLOCK_CLEANUP_VERSION
            && meta.status == timeline_repository::PROJECTION_META_STATUS_COMPLETE,
    )
}

async fn delete_terminal_approval_blocks_batch(db: &DatabaseConnection) -> Result<u64> {
    let statement = Statement::from_string(
        db.get_database_backend(),
        format!(
            r#"
DELETE FROM thread_timeline_block
WHERE block_id IN (
    SELECT block_id
    FROM thread_timeline_block
    WHERE block_kind = 'approval'
      AND (
        source_key IS NULL
        OR source_key NOT IN (
          SELECT request_id
          FROM cli_runtime_pending_request
          WHERE status = 'pending'
        )
      )
    ORDER BY block_id
    LIMIT {DEFAULT_BACKFILL_BATCH_SIZE}
)
"#
        ),
    );
    let result = db
        .execute_raw(statement)
        .await
        .context("failed to delete terminal approval timeline block batch")?;
    Ok(result.rows_affected())
}

async fn mark_terminal_approval_cleanup_complete(db: &DatabaseConnection) -> Result<()> {
    let now = now_datetime();
    timeline_repository::upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: TERMINAL_APPROVAL_BLOCK_CLEANUP_KEY.to_owned(),
            projection_version: TERMINAL_APPROVAL_BLOCK_CLEANUP_VERSION,
            status: timeline_repository::PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(now),
            backfilled_at: Some(now),
            created_at: now,
            updated_at: now,
        },
    )
    .await
}

pub(crate) async fn backfill_once(
    crud_store: &CrudStore,
    batch_size: u64,
) -> Result<SemanticTimelineBackfillSummary> {
    let db = crud_store.database_connection();
    let batch_size = normalize_batch_size(batch_size);

    if projection_is_current(&db).await? {
        return Ok(SemanticTimelineBackfillSummary {
            skipped: true,
            ..SemanticTimelineBackfillSummary::default()
        });
    }

    let source_counts = count_sources(&db, SOURCE_COUNT_BATCH_SIZE).await?;
    let started_at = now_datetime();
    timeline_repository::upsert_projection_meta(
        &db,
        ProjectionMetaRecord {
            projection_key: SEMANTIC_TIMELINE_PROJECTION_KEY.to_owned(),
            projection_version: SEMANTIC_TIMELINE_PROJECTION_VERSION,
            status: timeline_repository::PROJECTION_META_STATUS_BACKFILLING.to_owned(),
            source_thread_count: source_counts.threads,
            source_turn_count: source_counts.turns,
            source_turn_item_count: source_counts.turn_items,
            source_turn_event_count: source_counts.turn_events,
            last_error: None,
            backfill_started_at: Some(started_at),
            backfilled_at: None,
            created_at: started_at,
            updated_at: started_at,
        },
    )
    .await?;

    let mut summary = SemanticTimelineBackfillSummary {
        source_counts,
        ..SemanticTimelineBackfillSummary::default()
    };

    if summary.source_counts.is_empty() {
        mark_projection_complete(&db, &summary.source_counts).await?;
        return Ok(summary);
    }

    let result = backfill_all_threads(crud_store, &db, batch_size, &mut summary)
        .await
        .with_context(|| {
            format!(
                "failed to backfill semantic timeline projection `{}`",
                SEMANTIC_TIMELINE_PROJECTION_KEY
            )
        });

    if let Err(error) = result {
        let failed_at = now_datetime();
        let error_message = format!("{error:#}");
        let _ = timeline_repository::upsert_projection_meta(
            &db,
            ProjectionMetaRecord {
                projection_key: SEMANTIC_TIMELINE_PROJECTION_KEY.to_owned(),
                projection_version: SEMANTIC_TIMELINE_PROJECTION_VERSION,
                status: timeline_repository::PROJECTION_META_STATUS_FAILED.to_owned(),
                source_thread_count: summary.source_counts.threads,
                source_turn_count: summary.source_counts.turns,
                source_turn_item_count: summary.source_counts.turn_items,
                source_turn_event_count: summary.source_counts.turn_events,
                last_error: Some(error_message.clone()),
                backfill_started_at: Some(started_at),
                backfilled_at: None,
                created_at: started_at,
                updated_at: failed_at,
            },
        )
        .await;
        return Err(error);
    }

    mark_projection_complete(&db, &summary.source_counts).await?;
    Ok(summary)
}

async fn backfill_all_threads(
    crud_store: &CrudStore,
    db: &DatabaseConnection,
    batch_size: u64,
    summary: &mut SemanticTimelineBackfillSummary,
) -> Result<()> {
    let mut cursor: Option<(DateTimeWithTimeZone, String)> = None;

    loop {
        let threads = list_threads_batch(db, cursor.as_ref(), batch_size).await?;
        if threads.is_empty() {
            break;
        }

        for thread_model in &threads {
            summary.threads_seen = summary.threads_seen.saturating_add(1);
            backfill_thread(crud_store, db, thread_model, batch_size, summary).await?;
        }

        let last = threads
            .last()
            .expect("non-empty thread batch must have a last row");
        cursor = Some((last.created_at, last.id.clone()));
        super::maintenance_checkpoint().await?;
    }

    Ok(())
}

async fn backfill_thread(
    crud_store: &CrudStore,
    db: &DatabaseConnection,
    thread_model: &thread::Model,
    batch_size: u64,
    summary: &mut SemanticTimelineBackfillSummary,
) -> Result<()> {
    let mut cursor: Option<(DateTimeWithTimeZone, String)> = None;

    loop {
        let turns =
            list_turns_batch(db, thread_model.id.as_str(), cursor.as_ref(), batch_size).await?;
        if turns.is_empty() {
            break;
        }

        for turn_model in &turns {
            summary.turns_seen = summary.turns_seen.saturating_add(1);
            let stats = backfill_turn(crud_store, db, thread_model, turn_model, batch_size)
                .await
                .with_context(|| {
                    format!(
                        "failed to backfill semantic timeline turn `{}` in thread `{}`",
                        turn_model.id, thread_model.id
                    )
                })?;
            summary.timeline_blocks_upserted = summary
                .timeline_blocks_upserted
                .saturating_add(stats.timeline_blocks_upserted);
            if stats.work_projection_upserted {
                summary.work_projections_upserted =
                    summary.work_projections_upserted.saturating_add(1);
            }
            summary.work_items_upserted = summary
                .work_items_upserted
                .saturating_add(stats.work_items_upserted);
            summary.hidden_work_items = summary
                .hidden_work_items
                .saturating_add(stats.hidden_work_items);
            summary.invalid_items = summary.invalid_items.saturating_add(stats.invalid_items);
            super::maintenance_checkpoint().await?;
        }

        let last = turns
            .last()
            .expect("non-empty turn batch must have a last row");
        cursor = Some((last.created_at, last.id.clone()));
        super::maintenance_checkpoint().await?;
    }

    Ok(())
}

async fn backfill_turn(
    crud_store: &CrudStore,
    db: &DatabaseConnection,
    thread_model: &thread::Model,
    turn_model: &turn::Model,
    batch_size: u64,
) -> Result<TurnBackfillStats> {
    let revision_fence =
        crud_store.register_semantic_timeline_revision_fence(turn_model.id.as_str());
    loop {
        let Some(revision_before) =
            load_turn_backfill_source_revision(crud_store, db, &revision_fence, &turn_model.id)
                .await?
        else {
            return Ok(TurnBackfillStats::default());
        };

        // Mutable turns belong exclusively to the live projector. A plan
        // assembled from an in-progress turn becomes stale on its next event,
        // runtime binding transition, or approval update. Terminal turn rows
        // are backfilled, but their late item snapshots are still protected by
        // the revision fence below.
        if !turn_is_terminal(&revision_before.turn) {
            debug!(
                turn_id = %revision_before.turn.id,
                status = %revision_before.turn.status,
                "skipping mutable semantic timeline turn owned by live projection"
            );
            return Ok(TurnBackfillStats::default());
        }

        let plan =
            build_turn_backfill_plan(db, thread_model, &revision_before.turn, batch_size).await?;
        if load_turn_backfill_source_revision(crud_store, db, &revision_fence, &turn_model.id)
            .await?
            != Some(revision_before.clone())
        {
            super::maintenance_checkpoint().await?;
            continue;
        }

        if apply_turn_backfill_plan(
            crud_store,
            db,
            &revision_fence,
            &revision_before,
            &plan,
            batch_size,
        )
        .await?
        {
            return Ok(plan.stats);
        }
        super::maintenance_checkpoint().await?;
    }
}

async fn load_turn_backfill_source_revision(
    crud_store: &CrudStore,
    db: &DatabaseConnection,
    revision_fence: &SemanticTimelineRevisionFence,
    turn_id: &str,
) -> Result<Option<TurnBackfillSourceRevision>> {
    loop {
        let revision = crud_store
            .try_run_low_priority_write(|| {
                let db = db.clone();
                async move {
                    let turn = turn::Entity::find_by_id(turn_id.to_owned())
                        .one(&db)
                        .await
                        .with_context(|| {
                            format!("failed to load semantic timeline source turn `{turn_id}`")
                        })?;
                    Ok(turn.map(|turn| TurnBackfillSourceRevision {
                        turn,
                        live_generation: revision_fence.current(),
                    }))
                }
            })
            .await?;
        if let Some(revision) = revision {
            return Ok(revision);
        }
        super::maintenance_checkpoint().await?;
    }
}

async fn build_turn_backfill_plan(
    db: &DatabaseConnection,
    thread_model: &thread::Model,
    turn_model: &turn::Model,
    batch_size: u64,
) -> Result<TurnBackfillPlan> {
    let input_count = count_turn_inputs(db, turn_model.id.as_str()).await?;
    if turn_model.send_mode.as_deref() == Some("message") {
        let mut plan = TurnBackfillPlan::default();
        if input_count > 0 {
            plan.timeline_blocks.push(ThreadTimelineBlockRecord {
                block_id: user_block_id(turn_model.id.as_str()),
                workspace_id: thread_model.workspace_id.clone(),
                thread_id: thread_model.id.clone(),
                turn_id: Some(turn_model.id.clone()),
                block_kind: timeline_repository::BLOCK_KIND_USER_MESSAGE.to_owned(),
                sort_key: turn_block_sort_key(turn_model, 0, "user"),
                source_kind: Some("turn_input".to_owned()),
                source_key: Some(turn_model.id.clone()),
                started_at: Some(turn_model.created_at),
                completed_at: Some(turn_model.created_at),
                metadata_json: json!({
                    "turnId": turn_model.id,
                    "inputCount": input_count,
                })
                .to_string(),
                created_at: turn_model.created_at,
                updated_at: turn_model.updated_at,
            });
            plan.stats.timeline_blocks_upserted = 1;
        }
        return Ok(plan);
    }
    let (item_event_orders, source_high_watermark) =
        collect_item_event_orders(db, turn_model.id.as_str(), batch_size).await?;

    let mut plan = TurnBackfillPlan::default();
    let mut visible_work_count = 0_i64;
    let mut hidden_work_count = 0_i64;
    let mut first_work_item_id: Option<String> = None;
    let mut first_work_item_order_key: Option<String> = None;
    let mut last_work_item_id: Option<String> = None;
    let mut last_work_item_order_key: Option<String> = None;
    let mut running_item_id: Option<String> = None;
    let mut assistant_blocks = Vec::new();
    let mut detached_task_run_blocks = Vec::new();
    let mut has_stale_running_item = false;
    let projection_now = now_datetime();
    let turn_completed_at = terminal_completed_at(turn_model);

    let mut item_cursor: Option<(DateTimeWithTimeZone, String)> = None;
    loop {
        let items =
            list_turn_items_batch(db, turn_model.id.as_str(), item_cursor.as_ref(), batch_size)
                .await?;
        if items.is_empty() {
            break;
        }

        for item_model in &items {
            let classification = classify_turn_item_row_for_turn(item_model, turn_model);
            if classification.classification == WorkItemClassification::InvalidPayload {
                plan.stats.invalid_items = plan.stats.invalid_items.saturating_add(1);
            }

            let source_order = item_event_orders.get(item_model.item_id.as_str());
            let order_key = work_item_order_key(item_model, source_order);

            match classification.placement {
                ProjectionPlacement::TopLevelUserMessage => {}
                ProjectionPlacement::TopLevelDetachedTaskRun => {
                    detached_task_run_blocks.push(detached_task_run_block_candidate(item_model));
                }
                ProjectionPlacement::TopLevelAssistantMessage => {
                    assistant_blocks.push(AssistantBlockCandidate {
                        item_id: item_model.item_id.clone(),
                        order_key,
                        started_at: item_model.created_at,
                        completed_at: item_model.updated_at,
                    });
                }
                ProjectionPlacement::TurnWork | ProjectionPlacement::Hidden => {
                    let work_item_id = work_item_projection_id(
                        turn_model.id.as_str(),
                        item_model.item_id.as_str(),
                    );
                    if classification.visibility == ProjectionVisibility::Hidden {
                        hidden_work_count = hidden_work_count.saturating_add(1);
                        plan.stats.hidden_work_items =
                            plan.stats.hidden_work_items.saturating_add(1);
                    } else {
                        if first_work_item_order_key
                            .as_deref()
                            .is_none_or(|current| order_key.as_str() < current)
                        {
                            first_work_item_id = Some(work_item_id.clone());
                            first_work_item_order_key = Some(order_key.clone());
                        }
                        if last_work_item_order_key
                            .as_deref()
                            .is_none_or(|current| order_key.as_str() > current)
                        {
                            last_work_item_id = Some(work_item_id.clone());
                            last_work_item_order_key = Some(order_key.clone());
                        }
                        visible_work_count = visible_work_count.saturating_add(1);
                    }
                    if is_running_work_status(classification.status) {
                        if running_item_id.is_none() {
                            running_item_id = Some(item_model.item_id.clone());
                        }
                        if is_stale_running_turn_item(item_model, projection_now) {
                            has_stale_running_item = true;
                        }
                    }

                    plan.work_items.push(TurnWorkItemProjectionRecord {
                        work_item_id,
                        workspace_id: thread_model.workspace_id.clone(),
                        thread_id: thread_model.id.clone(),
                        turn_id: turn_model.id.clone(),
                        item_id: item_model.item_id.clone(),
                        source_event_id: source_order.map(|order| order.event_id.clone()),
                        source_sequence: source_order.map(|order| order.sequence).unwrap_or(0),
                        order_key,
                        item_type: item_model.item_type.clone(),
                        visibility: classification.visibility_str().to_owned(),
                        classification: classification.classification_str().to_owned(),
                        status: classification.status.to_owned(),
                        started_at: Some(item_model.created_at),
                        completed_at: Some(
                            source_order
                                .and_then(|order| order.completed_at)
                                .or_else(|| {
                                    turn_completed_at.map(|completed_at| {
                                        min_datetime(item_model.updated_at, completed_at)
                                    })
                                })
                                .unwrap_or(item_model.updated_at),
                        ),
                        metadata_json: classification_metadata_json(&classification),
                        created_at: item_model.created_at,
                        updated_at: item_model.updated_at,
                    });
                    plan.stats.work_items_upserted =
                        plan.stats.work_items_upserted.saturating_add(1);
                }
            }
        }

        let last = items
            .last()
            .expect("non-empty turn item batch must have a last row");
        item_cursor = Some((last.created_at, last.id.clone()));
        super::maintenance_checkpoint().await?;
    }

    assistant_blocks.sort_by(|left, right| left.order_key.cmp(&right.order_key));

    if input_count > 0 {
        plan.timeline_blocks.push(ThreadTimelineBlockRecord {
            block_id: user_block_id(turn_model.id.as_str()),
            workspace_id: thread_model.workspace_id.clone(),
            thread_id: thread_model.id.clone(),
            turn_id: Some(turn_model.id.clone()),
            block_kind: timeline_repository::BLOCK_KIND_USER_MESSAGE.to_owned(),
            sort_key: turn_block_sort_key(turn_model, 0, "user"),
            source_kind: Some("turn_input".to_owned()),
            source_key: Some(turn_model.id.clone()),
            started_at: Some(turn_model.created_at),
            completed_at: Some(turn_model.created_at),
            metadata_json: json!({
                "turnId": turn_model.id,
                "inputCount": input_count,
            })
            .to_string(),
            created_at: turn_model.created_at,
            updated_at: turn_model.updated_at,
        });
        plan.stats.timeline_blocks_upserted = plan.stats.timeline_blocks_upserted.saturating_add(1);
    }

    let has_final = !assistant_blocks.is_empty();
    let has_detached_task_run =
        !detached_task_run_blocks.is_empty() || detached_task_run_origin_hint(turn_model);
    let work_count = visible_work_count.saturating_add(hidden_work_count);
    let completed_without_work = turn_model.status == "completed" && work_count == 0;
    let needs_work_block =
        !has_detached_task_run && (work_count > 0 || (!has_final && !completed_without_work));

    if needs_work_block {
        let pending_request_count = count_pending_cli_runtime_requests(
            db,
            thread_model.id.as_str(),
            turn_model.id.as_str(),
        )
        .await?;
        let cli_runtime_status =
            turn_cli_runtime_binding::Entity::find_by_id(turn_model.id.clone())
                .one(db)
                .await
                .with_context(|| {
                    format!(
                        "failed to load CLI runtime binding for semantic timeline turn `{}`",
                        turn_model.id
                    )
                })?
                .map(|binding| binding.status);
        let presentation = turn_work_presentation(turn_model, has_final);
        let state = turn_work_state(
            turn_model.status.as_str(),
            cli_runtime_status.as_deref(),
            pending_request_count,
            has_stale_running_item,
        );
        let elapsed_through = turn_completed_at.unwrap_or(turn_model.updated_at);
        let elapsed_ms = elapsed_ms(turn_model.created_at, elapsed_through);
        plan.work_projection = Some(TurnWorkProjectionRecord {
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
                completed_at: turn_completed_at,
                elapsed_ms: Some(elapsed_ms),
                source_high_watermark,
                metadata_json: json!({
                    "hasFinalAssistantMessage": has_final,
                    "pendingRequestCount": pending_request_count,
                    "runningItemId": running_item_id,
                    "staleRuntimeState": if has_stale_running_item { Some("expired_lease") } else { None },
                    "presentation": presentation,
                    "state": state,
                })
                .to_string(),
                created_at: turn_model.created_at,
                updated_at: turn_model.updated_at,
            });
        plan.stats.work_projection_upserted = true;

        plan.timeline_blocks.push(ThreadTimelineBlockRecord {
            block_id: work_block_id(turn_model.id.as_str()),
            workspace_id: thread_model.workspace_id.clone(),
            thread_id: thread_model.id.clone(),
            turn_id: Some(turn_model.id.clone()),
            block_kind: timeline_repository::BLOCK_KIND_TURN_WORK.to_owned(),
            sort_key: turn_block_sort_key(turn_model, 100, "work"),
            source_kind: Some("turn_work".to_owned()),
            source_key: Some(turn_model.id.clone()),
            started_at: Some(turn_model.created_at),
            completed_at: turn_completed_at,
            metadata_json: json!({
                "turnId": turn_model.id,
                "workCount": work_count,
                "visibleWorkCount": visible_work_count,
                "hiddenWorkCount": hidden_work_count,
            })
            .to_string(),
            created_at: turn_model.created_at,
            updated_at: turn_model.updated_at,
        });
        plan.stats.timeline_blocks_upserted = plan.stats.timeline_blocks_upserted.saturating_add(1);
    }

    for task_run in detached_task_run_blocks {
        plan.timeline_blocks.push(ThreadTimelineBlockRecord {
            block_id: detached_task_run_block_id(turn_model.id.as_str(), task_run.item_id.as_str()),
            workspace_id: thread_model.workspace_id.clone(),
            thread_id: thread_model.id.clone(),
            turn_id: Some(turn_model.id.clone()),
            block_kind: timeline_repository::BLOCK_KIND_DETACHED_TASK_RUN.to_owned(),
            sort_key: turn_block_sort_key(
                turn_model,
                100,
                format!("detached-task-run:{}", task_run.item_id).as_str(),
            ),
            source_kind: Some("turn_item".to_owned()),
            source_key: Some(task_run.item_id.clone()),
            started_at: Some(task_run.started_at),
            completed_at: task_run.completed_at,
            metadata_json: json!({
                "turnId": turn_model.id,
                "itemId": task_run.item_id,
                "attachment": "detached",
            })
            .to_string(),
            created_at: task_run.started_at,
            updated_at: task_run.updated_at,
        });
        plan.stats.timeline_blocks_upserted = plan.stats.timeline_blocks_upserted.saturating_add(1);
    }

    for assistant in assistant_blocks {
        plan.timeline_blocks.push(ThreadTimelineBlockRecord {
            block_id: assistant_block_id(turn_model.id.as_str(), assistant.item_id.as_str()),
            workspace_id: thread_model.workspace_id.clone(),
            thread_id: thread_model.id.clone(),
            turn_id: Some(turn_model.id.clone()),
            block_kind: timeline_repository::BLOCK_KIND_ASSISTANT_MESSAGE.to_owned(),
            sort_key: if has_detached_task_run {
                timeline_event_block_sort_key(
                    assistant.started_at,
                    turn_model.id.as_str(),
                    200,
                    assistant.order_key.as_str(),
                )
            } else {
                turn_block_sort_key(turn_model, 200, assistant.order_key.as_str())
            },
            source_kind: Some("turn_item".to_owned()),
            source_key: Some(assistant.item_id.clone()),
            started_at: Some(assistant.started_at),
            completed_at: Some(assistant.completed_at),
            metadata_json: json!({
                "turnId": turn_model.id,
                "itemId": assistant.item_id,
                "classification": WorkItemClassification::FinalAssistantMessage.as_str(),
            })
            .to_string(),
            created_at: assistant.started_at,
            updated_at: assistant.completed_at,
        });
        plan.stats.timeline_blocks_upserted = plan.stats.timeline_blocks_upserted.saturating_add(1);
    }

    let terminal_block_id = terminal_state_block_id(turn_model.id.as_str());
    if let Some(state) =
        terminal_turn_state(turn_model).filter(|_| !has_final && !has_detached_task_run)
    {
        plan.timeline_blocks.push(ThreadTimelineBlockRecord {
            block_id: terminal_block_id,
            workspace_id: thread_model.workspace_id.clone(),
            thread_id: thread_model.id.clone(),
            turn_id: Some(turn_model.id.clone()),
            block_kind: timeline_repository::BLOCK_KIND_SYSTEM.to_owned(),
            sort_key: turn_block_sort_key(turn_model, 300, "terminal-state"),
            source_kind: Some("turn_terminal_state".to_owned()),
            source_key: Some(turn_model.id.clone()),
            started_at: Some(turn_model.updated_at),
            completed_at: Some(turn_model.updated_at),
            metadata_json: json!({
                "state": state,
                "message": turn_model.error,
            })
            .to_string(),
            created_at: turn_model.updated_at,
            updated_at: turn_model.updated_at,
        });
        plan.stats.timeline_blocks_upserted = plan.stats.timeline_blocks_upserted.saturating_add(1);
    }

    Ok(plan)
}

async fn apply_turn_backfill_plan(
    crud_store: &CrudStore,
    db: &DatabaseConnection,
    revision_fence: &SemanticTimelineRevisionFence,
    revision: &TurnBackfillSourceRevision,
    plan: &TurnBackfillPlan,
    batch_size: u64,
) -> Result<bool> {
    // Keep existing rows visible while replacement rows are prepared. Every
    // committed write slice is bounded, and stale rows are removed only after
    // all expected rows exist. If cancellation lands between slices, the next
    // idempotent run safely resumes from a superset instead of leaving the turn
    // empty.
    for batch in plan.work_items.chunks(batch_size as usize) {
        if run_revision_guarded_low_priority_write(crud_store, db, revision_fence, revision, || {
            let db = db.clone();
            let batch = batch.to_vec();
            async move {
                let transaction = db
                    .begin()
                    .await
                    .context("failed to begin semantic timeline work-item batch")?;
                for record in batch {
                    timeline_repository::upsert_turn_work_item_projection(&transaction, record)
                        .await?;
                }
                transaction
                    .commit()
                    .await
                    .context("failed to commit semantic timeline work-item batch")?;
                Ok(())
            }
        })
        .await?
        .is_none()
        {
            return Ok(false);
        }
        super::maintenance_checkpoint().await?;
    }

    if let Some(projection) = plan.work_projection.as_ref() {
        if run_revision_guarded_low_priority_write(crud_store, db, revision_fence, revision, || {
            let db = db.clone();
            let projection = projection.clone();
            async move { timeline_repository::upsert_turn_work_projection(&db, projection).await }
        })
        .await?
        .is_none()
        {
            return Ok(false);
        }
        super::maintenance_checkpoint().await?;
    }

    for batch in plan.timeline_blocks.chunks(batch_size as usize) {
        if run_revision_guarded_low_priority_write(crud_store, db, revision_fence, revision, || {
            let db = db.clone();
            let batch = batch.to_vec();
            async move {
                let transaction = db
                    .begin()
                    .await
                    .context("failed to begin semantic timeline block batch")?;
                for record in batch {
                    timeline_repository::upsert_thread_timeline_block(&transaction, record).await?;
                }
                transaction
                    .commit()
                    .await
                    .context("failed to commit semantic timeline block batch")?;
                Ok(())
            }
        })
        .await?
        .is_none()
        {
            return Ok(false);
        }
        super::maintenance_checkpoint().await?;
    }

    // The turn lifecycle is terminal here, but late item snapshots may still
    // arrive. Every cleanup slice therefore carries the same revision fence as
    // the upserts instead of relying on terminal status alone.
    if turn_is_terminal(&revision.turn) {
        let expected_work_items = plan
            .work_items
            .iter()
            .map(|record| record.work_item_id.clone())
            .collect::<HashSet<_>>();
        if !delete_stale_work_items(
            crud_store,
            db,
            revision_fence,
            revision,
            &expected_work_items,
            batch_size,
        )
        .await?
        {
            return Ok(false);
        }

        let expected_blocks = plan
            .timeline_blocks
            .iter()
            .map(|record| record.block_id.clone())
            .collect::<HashSet<_>>();
        if !delete_stale_timeline_blocks(
            crud_store,
            db,
            revision_fence,
            revision,
            &expected_blocks,
            batch_size,
        )
        .await?
        {
            return Ok(false);
        }

        if plan.work_projection.is_none() {
            if run_revision_guarded_low_priority_write(
                crud_store,
                db,
                revision_fence,
                revision,
                || {
                    let db = db.clone();
                    let turn_id = revision.turn.id.clone();
                    async move {
                        timeline_repository::delete_turn_work_projection(&db, turn_id.as_str())
                            .await
                            .map(|_| ())
                    }
                },
            )
            .await?
            .is_none()
            {
                return Ok(false);
            }
            super::maintenance_checkpoint().await?;
        }
    }

    Ok(true)
}

async fn run_revision_guarded_low_priority_write<T, F, Fut>(
    crud_store: &CrudStore,
    db: &DatabaseConnection,
    revision_fence: &SemanticTimelineRevisionFence,
    revision: &TurnBackfillSourceRevision,
    mut operation: F,
) -> Result<Option<T>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    loop {
        let guarded = crud_store
            .try_run_low_priority_write(|| {
                let expected = revision.clone();
                let db = db.clone();
                let operation = operation();
                async move {
                    let current_turn = turn::Entity::find_by_id(expected.turn.id.clone())
                        .one(&db)
                        .await
                        .with_context(|| {
                            format!(
                                "failed to validate semantic timeline source turn `{}`",
                                expected.turn.id
                            )
                        })?;
                    if revision_fence.current() != expected.live_generation
                        || current_turn != Some(expected.turn)
                    {
                        return Ok(None);
                    }
                    operation.await.map(Some)
                }
            })
            .await?;
        if let Some(result) = guarded {
            return Ok(result);
        }
        super::maintenance_checkpoint().await?;
    }
}

async fn delete_stale_work_items(
    crud_store: &CrudStore,
    db: &DatabaseConnection,
    revision_fence: &SemanticTimelineRevisionFence,
    revision: &TurnBackfillSourceRevision,
    expected: &HashSet<String>,
    batch_size: u64,
) -> Result<bool> {
    let turn_id = revision.turn.id.as_str();
    let mut cursor: Option<String> = None;
    loop {
        let mut query = turn_work_item_projection::Entity::find()
            .select_only()
            .column(turn_work_item_projection::Column::WorkItemId)
            .filter(turn_work_item_projection::Column::TurnId.eq(turn_id.to_owned()));
        if let Some(cursor) = cursor.as_deref() {
            query =
                query.filter(turn_work_item_projection::Column::WorkItemId.gt(cursor.to_owned()));
        }
        let ids = query
            .order_by_asc(turn_work_item_projection::Column::WorkItemId)
            .limit(batch_size)
            .into_tuple::<String>()
            .all(db)
            .await
            .with_context(|| {
                format!("failed to list semantic timeline work items for turn `{turn_id}`")
            })?;
        let Some(last) = ids.last() else {
            break;
        };
        cursor = Some(last.clone());
        let stale = ids
            .into_iter()
            .filter(|id| !expected.contains(id))
            .collect::<Vec<_>>();
        if !stale.is_empty() {
            if run_revision_guarded_low_priority_write(
                crud_store,
                db,
                revision_fence,
                revision,
                || {
                    let db = db.clone();
                    let stale = stale.clone();
                    async move {
                        let transaction = db
                            .begin()
                            .await
                            .context("failed to begin stale timeline work-item cleanup batch")?;
                        turn_work_item_projection::Entity::delete_many()
                            .filter(turn_work_item_projection::Column::WorkItemId.is_in(stale))
                            .exec(&transaction)
                            .await
                            .context("failed to delete stale timeline work-item batch")?;
                        transaction
                            .commit()
                            .await
                            .context("failed to commit stale timeline work-item cleanup batch")?;
                        Ok(())
                    }
                },
            )
            .await?
            .is_none()
            {
                return Ok(false);
            }
        }
        super::maintenance_checkpoint().await?;
    }
    Ok(true)
}

async fn delete_stale_timeline_blocks(
    crud_store: &CrudStore,
    db: &DatabaseConnection,
    revision_fence: &SemanticTimelineRevisionFence,
    revision: &TurnBackfillSourceRevision,
    expected: &HashSet<String>,
    batch_size: u64,
) -> Result<bool> {
    let turn_id = revision.turn.id.as_str();
    let mut cursor: Option<String> = None;
    loop {
        let mut query = thread_timeline_block::Entity::find()
            .select_only()
            .column(thread_timeline_block::Column::BlockId)
            .filter(thread_timeline_block::Column::TurnId.eq(turn_id.to_owned()));
        if let Some(cursor) = cursor.as_deref() {
            query = query.filter(thread_timeline_block::Column::BlockId.gt(cursor.to_owned()));
        }
        let ids = query
            .order_by_asc(thread_timeline_block::Column::BlockId)
            .limit(batch_size)
            .into_tuple::<String>()
            .all(db)
            .await
            .with_context(|| {
                format!("failed to list semantic timeline blocks for turn `{turn_id}`")
            })?;
        let Some(last) = ids.last() else {
            break;
        };
        cursor = Some(last.clone());
        let stale = ids
            .into_iter()
            .filter(|id| !expected.contains(id))
            .collect::<Vec<_>>();
        if !stale.is_empty() {
            if run_revision_guarded_low_priority_write(
                crud_store,
                db,
                revision_fence,
                revision,
                || {
                    let db = db.clone();
                    let stale = stale.clone();
                    async move {
                        let transaction = db
                            .begin()
                            .await
                            .context("failed to begin stale timeline block cleanup batch")?;
                        thread_timeline_block::Entity::delete_many()
                            .filter(thread_timeline_block::Column::BlockId.is_in(stale))
                            .exec(&transaction)
                            .await
                            .context("failed to delete stale timeline block batch")?;
                        transaction
                            .commit()
                            .await
                            .context("failed to commit stale timeline block cleanup batch")?;
                        Ok(())
                    }
                },
            )
            .await?
            .is_none()
            {
                return Ok(false);
            }
        }
        super::maintenance_checkpoint().await?;
    }
    Ok(true)
}

fn detached_task_run_block_candidate(
    item_model: &turn_item::Model,
) -> DetachedTaskRunBlockCandidate {
    let task = serde_json::from_str::<TurnItem>(item_model.payload.as_str())
        .ok()
        .and_then(|item| match item {
            TurnItem::Task { item } => Some(item),
            _ => None,
        });
    let started_at = task
        .as_ref()
        .and_then(|item| item.started_at)
        .map(unix_timestamp_to_datetime)
        .unwrap_or(item_model.created_at);
    let completed_at = task
        .as_ref()
        .filter(|item| item.status.is_terminal())
        .map(|item| unix_timestamp_to_datetime(item.updated_at));
    DetachedTaskRunBlockCandidate {
        item_id: item_model.item_id.clone(),
        started_at,
        completed_at,
        updated_at: completed_at.unwrap_or(item_model.updated_at),
    }
}

fn unix_timestamp_to_datetime(timestamp: i64) -> DateTimeWithTimeZone {
    chrono::DateTime::from_timestamp(timestamp, 0)
        .unwrap_or_else(chrono::Utc::now)
        .fixed_offset()
}

async fn projection_is_current(db: &DatabaseConnection) -> Result<bool> {
    let Some(meta) =
        timeline_repository::find_projection_meta(db, SEMANTIC_TIMELINE_PROJECTION_KEY).await?
    else {
        return Ok(false);
    };

    Ok(
        meta.projection_version == SEMANTIC_TIMELINE_PROJECTION_VERSION
            && meta.status == timeline_repository::PROJECTION_META_STATUS_COMPLETE,
    )
}

async fn mark_projection_complete(
    db: &DatabaseConnection,
    source_counts: &SemanticTimelineBackfillSourceCounts,
) -> Result<()> {
    let now = now_datetime();
    timeline_repository::upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: SEMANTIC_TIMELINE_PROJECTION_KEY.to_owned(),
            projection_version: SEMANTIC_TIMELINE_PROJECTION_VERSION,
            status: timeline_repository::PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: source_counts.threads,
            source_turn_count: source_counts.turns,
            source_turn_item_count: source_counts.turn_items,
            source_turn_event_count: source_counts.turn_events,
            last_error: None,
            backfill_started_at: Some(now),
            backfilled_at: Some(now),
            created_at: now,
            updated_at: now,
        },
    )
    .await
}

async fn count_sources(
    db: &DatabaseConnection,
    batch_size: u64,
) -> Result<SemanticTimelineBackfillSourceCounts> {
    macro_rules! count_batched {
        ($entity:ident, $context:literal) => {{
            let mut count = 0_i64;
            let mut cursor: Option<String> = None;
            loop {
                let mut query = $entity::Entity::find()
                    .select_only()
                    .column($entity::Column::Id);
                if let Some(cursor) = cursor.as_deref() {
                    query = query.filter($entity::Column::Id.gt(cursor.to_owned()));
                }
                let ids = query
                    .order_by_asc($entity::Column::Id)
                    .limit(batch_size)
                    .into_tuple::<String>()
                    .all(db)
                    .await
                    .context($context)?;
                let Some(last) = ids.last() else {
                    break;
                };
                cursor = Some(last.clone());
                count = count.saturating_add(i64::try_from(ids.len()).unwrap_or(i64::MAX));
                super::maintenance_checkpoint().await?;
            }
            count
        }};
    }

    Ok(SemanticTimelineBackfillSourceCounts {
        threads: count_batched!(
            thread,
            "failed to count thread rows for semantic timeline backfill"
        ),
        turns: count_batched!(
            turn,
            "failed to count turn rows for semantic timeline backfill"
        ),
        turn_items: count_batched!(
            turn_item,
            "failed to count turn item rows for semantic timeline backfill"
        ),
        turn_events: count_batched!(
            turn_event,
            "failed to count turn event rows for semantic timeline backfill"
        ),
    })
}

async fn list_threads_batch<C: ConnectionTrait>(
    db: &C,
    cursor: Option<&(DateTimeWithTimeZone, String)>,
    limit: u64,
) -> Result<Vec<thread::Model>> {
    let mut query = thread::Entity::find();
    if let Some((created_at, id)) = cursor {
        query = query.filter(keyset_after_condition(
            thread::Column::CreatedAt,
            thread::Column::Id,
            *created_at,
            id.as_str(),
        ));
    }
    query
        .order_by_asc(thread::Column::CreatedAt)
        .order_by_asc(thread::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list thread batch for semantic timeline backfill")
}

async fn list_turns_batch<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    cursor: Option<&(DateTimeWithTimeZone, String)>,
    limit: u64,
) -> Result<Vec<turn::Model>> {
    let mut query = turn::Entity::find().filter(turn::Column::ThreadId.eq(thread_id.to_owned()));
    if let Some((created_at, id)) = cursor {
        query = query.filter(keyset_after_condition(
            turn::Column::CreatedAt,
            turn::Column::Id,
            *created_at,
            id.as_str(),
        ));
    }
    query
        .order_by_asc(turn::Column::CreatedAt)
        .order_by_asc(turn::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list turn batch for semantic timeline thread `{thread_id}`")
        })
}

async fn list_turn_items_batch<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    cursor: Option<&(DateTimeWithTimeZone, String)>,
    limit: u64,
) -> Result<Vec<turn_item::Model>> {
    let mut query =
        turn_item::Entity::find().filter(turn_item::Column::TurnId.eq(turn_id.to_owned()));
    if let Some((created_at, id)) = cursor {
        query = query.filter(keyset_after_condition(
            turn_item::Column::CreatedAt,
            turn_item::Column::Id,
            *created_at,
            id.as_str(),
        ));
    }
    query
        .order_by_asc(turn_item::Column::CreatedAt)
        .order_by_asc(turn_item::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list turn item batch for semantic timeline turn `{turn_id}`")
        })
}

fn keyset_after_condition<C>(
    created_column: C,
    id_column: C,
    created_at: DateTimeWithTimeZone,
    id: &str,
) -> Condition
where
    C: ColumnTrait,
{
    Condition::any().add(created_column.gt(created_at)).add(
        Condition::all()
            .add(created_column.eq(created_at))
            .add(id_column.gt(id.to_owned())),
    )
}

async fn count_turn_inputs<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<i64> {
    let count = turn_input::Entity::find()
        .filter(turn_input::Column::TurnId.eq(turn_id.to_owned()))
        .count(db)
        .await
        .with_context(|| format!("failed to count turn inputs for turn `{turn_id}`"))?;
    Ok(i64::try_from(count).unwrap_or(i64::MAX))
}

async fn collect_item_event_orders<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    batch_size: u64,
) -> Result<(HashMap<String, ItemEventOrder>, i64)> {
    let mut result = HashMap::new();
    let mut high_watermark = 0_i64;
    let mut last_sequence = 0_i64;

    loop {
        let events = turn_event::Entity::find()
            .filter(turn_event::Column::TurnId.eq(turn_id.to_owned()))
            .filter(turn_event::Column::Sequence.gt(last_sequence))
            .order_by_asc(turn_event::Column::Sequence)
            .limit(batch_size)
            .all(db)
            .await
            .with_context(|| {
                format!("failed to list turn events for semantic timeline turn `{turn_id}`")
            })?;
        if events.is_empty() {
            break;
        }

        for event in &events {
            high_watermark = high_watermark.max(event.sequence);
            if let Some((item_id, completed)) = event_payload_item(event.payload.as_str()) {
                let order = result.entry(item_id).or_insert_with(|| ItemEventOrder {
                    event_id: event.id.clone(),
                    sequence: event.sequence,
                    completed_at: None,
                });
                if completed {
                    order.completed_at = Some(event.created_at);
                }
            }
        }

        last_sequence = events
            .last()
            .expect("non-empty turn event batch must have a last row")
            .sequence;
        super::maintenance_checkpoint().await?;
    }

    Ok((result, high_watermark))
}

fn event_payload_item(raw_payload: &str) -> Option<(String, bool)> {
    let value = serde_json::from_str::<JsonValue>(raw_payload).ok()?;
    let kind = value.get("kind")?.as_str()?;
    if !matches!(
        kind,
        "item_started"
            | "item_completed"
            | "item_updated"
            | "item_timeout_detected"
            | "item_recovery_opened"
            | "item_recovery_attached"
            | "item_retry_scheduled"
            | "item_retry_attempt_started"
            | "item_recovery_succeeded"
            | "item_recovery_exhausted"
            | "item_tool_retry_scheduled"
            | "item_tool_retry_resolved"
            | "item_tool_retry_exhausted"
    ) {
        return None;
    }

    let payload = value.get("payload")?;
    let item_id = payload
        .get("item")
        .and_then(|item| item.get("id"))
        .or_else(|| payload.get("item_id"))
        .and_then(JsonValue::as_str)
        .filter(|item_id| !item_id.is_empty())
        .map(str::to_owned)?;
    Some((item_id, kind == "item_completed"))
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

fn normalize_batch_size(batch_size: u64) -> u64 {
    let batch_size = if batch_size == 0 {
        DEFAULT_BACKFILL_BATCH_SIZE
    } else {
        batch_size
    };
    batch_size.clamp(1, MAX_BACKFILL_BATCH_SIZE)
}

fn now_datetime() -> DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}

#[cfg(test)]
mod tests {
    use super::{turn_status_is_terminal, turn_work_state};

    #[test]
    fn backfill_keeps_native_turn_running_without_active_work_item() {
        assert_eq!(turn_work_state("in_progress", None, 0, false), "running");
    }

    #[test]
    fn backfill_preserves_cli_queue_until_native_start() {
        assert_eq!(
            turn_work_state("in_progress", Some("starting"), 0, false),
            "starting"
        );
        assert_eq!(
            turn_work_state("in_progress", Some("running"), 0, false),
            "running"
        );
    }

    #[test]
    fn startup_backfill_owns_only_immutable_terminal_turns() {
        for status in ["completed", "failed", "interrupted", "blocked"] {
            assert!(turn_status_is_terminal(status), "{status}");
        }
        for status in ["queued", "starting", "in_progress"] {
            assert!(!turn_status_is_terminal(status), "{status}");
        }
    }
}
