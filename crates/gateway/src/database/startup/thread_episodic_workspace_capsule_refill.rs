use crate::thread_episodic::{
    StoreThreadEpisodicIndexPayloadProvider, StoreThreadEpisodicIngestor,
    ThreadEpisodicIndexExecutorConfig, ThreadEpisodicIndexPayloadProvider,
    ThreadEpisodicIndexResolutionFailureKind, ThreadEpisodicResolvedIndexRequest,
    ThreadEpisodicThreadReindexRequest, memvid_stats_reach_capacity_threshold,
};
use anyhow::{Context, Result, bail};
use fs4::{FileExt as Fs4FileExt, TryLockError as Fs4TryLockError};
use pioneer_crud::{
    CrudStore, NewThreadEpisodicThreadDirectoryRecord, PROJECTION_META_STATUS_BACKFILLING,
    PROJECTION_META_STATUS_COMPLETE, PROJECTION_META_STATUS_FAILED, ProjectionMetaRecord,
    ThreadEpisodicCapsuleCapacityUpdate, ThreadEpisodicCapsuleWriteState,
    ThreadEpisodicIndexJobCompletionUpdate, ThreadEpisodicIndexJobFailureUpdate,
    ThreadEpisodicIndexJobRecord, ThreadEpisodicItemIndexedUpdate,
    ThreadEpisodicThreadDirectoryStatus, ThreadEpisodicThreadDirectoryVisibility,
    find_projection_meta, upsert_projection_meta,
};
use pioneer_memory::{
    MemvidThreadEpisodicBackend, ThreadEpisodicMemvidBackend, ThreadEpisodicMemvidFailureKind,
    ThreadEpisodicMemvidIndexOutput, ThreadEpisodicMemvidStats,
    thread_episodic_storage_uri_from_path,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

pub(crate) const THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY: &str =
    "thread_episodic_workspace_capsule_refill";
pub(crate) const THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION: i64 = 1;

const REFILL_ENQUEUE_BATCH_SIZE: u64 = 1024;
const REFILL_EXECUTOR_MAX_BATCHES: u64 = 100_000;
const REFILL_JOB_CLAIM_LIMIT: u64 = 1;
const REFILL_LOCK_FILE_NAME: &str = ".thread_episodic_workspace_capsule_refill.lock";
const REFILL_INDEX_ERROR_MAX_CHARS: usize = 512;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ThreadEpisodicWorkspaceCapsuleRefillSummary {
    pub(crate) skipped: bool,
    pub(crate) lock_contended: bool,
    pub(crate) capsule_files_deleted: u64,
    pub(crate) capsule_files_missing: u64,
    pub(crate) non_file_storage_uris: u64,
    pub(crate) capsule_rows_deleted: u64,
    pub(crate) item_rows_deleted: u64,
    pub(crate) exclusion_rows_deleted: u64,
    pub(crate) index_jobs_deleted: u64,
    pub(crate) thread_directory_rows_deleted: u64,
    pub(crate) workspace_count: usize,
    pub(crate) source_threads_reindexed: usize,
    pub(crate) source_threads_failed: usize,
    pub(crate) source_thread_count: i64,
    pub(crate) source_turn_count: i64,
    pub(crate) source_turn_item_count: i64,
    pub(crate) refill_jobs_enqueued: usize,
    pub(crate) executor_batches: u64,
    pub(crate) completed_jobs: usize,
    pub(crate) failed_retryable_jobs: usize,
    pub(crate) failed_terminal_jobs: usize,
    pub(crate) incomplete_jobs: u64,
}

pub(super) async fn run(crud_store: Arc<CrudStore>, thread_episodic_storage_root: PathBuf) {
    match refill_once(crud_store, thread_episodic_storage_root.as_path()).await {
        Ok(summary) if summary.skipped && summary.lock_contended => {
            info!(
                "thread episodic workspace capsule refill skipped because another process holds the refill lock"
            );
        }
        Ok(summary) if summary.skipped => {}
        Ok(summary) => {
            info!(
                capsule_files_deleted = summary.capsule_files_deleted,
                capsule_files_missing = summary.capsule_files_missing,
                non_file_storage_uris = summary.non_file_storage_uris,
                capsule_rows_deleted = summary.capsule_rows_deleted,
                item_rows_deleted = summary.item_rows_deleted,
                exclusion_rows_deleted = summary.exclusion_rows_deleted,
                index_jobs_deleted = summary.index_jobs_deleted,
                thread_directory_rows_deleted = summary.thread_directory_rows_deleted,
                workspace_count = summary.workspace_count,
                source_threads_reindexed = summary.source_threads_reindexed,
                source_threads_failed = summary.source_threads_failed,
                refill_jobs_enqueued = summary.refill_jobs_enqueued,
                executor_batches = summary.executor_batches,
                completed_jobs = summary.completed_jobs,
                "thread episodic workspace capsule refill completed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "thread episodic workspace capsule refill failed at startup"
            );
        }
    }
}

pub(crate) async fn refill_once(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    let db = crud_store.database_connection();
    if refill_is_current(crud_store.as_ref()).await? {
        return Ok(ThreadEpisodicWorkspaceCapsuleRefillSummary {
            skipped: true,
            ..Default::default()
        });
    }
    let Some(_lock_guard) = try_acquire_refill_lock(thread_episodic_storage_root)? else {
        return Ok(ThreadEpisodicWorkspaceCapsuleRefillSummary {
            skipped: true,
            lock_contended: true,
            ..Default::default()
        });
    };

    let started_at = now_datetime();
    upsert_projection_meta(
        &db,
        ProjectionMetaRecord {
            projection_key: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY.to_owned(),
            projection_version: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            status: PROJECTION_META_STATUS_BACKFILLING.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(started_at),
            backfilled_at: None,
            created_at: started_at,
            updated_at: started_at,
        },
    )
    .await?;

    let result = refill_after_marker(crud_store.clone(), thread_episodic_storage_root).await;
    match result {
        Ok(summary) => {
            mark_refill_complete(&db, &summary).await?;
            Ok(summary)
        }
        Err(error) => {
            mark_refill_failed(&db, &error).await?;
            Err(error)
        }
    }
}

struct RefillLockGuard {
    file: File,
}

impl Drop for RefillLockGuard {
    fn drop(&mut self) {
        let _ = Fs4FileExt::unlock(&self.file);
    }
}

fn try_acquire_refill_lock(thread_episodic_storage_root: &Path) -> Result<Option<RefillLockGuard>> {
    std::fs::create_dir_all(thread_episodic_storage_root).with_context(|| {
        format!(
            "failed to create thread episodic storage root `{}` for refill lock",
            thread_episodic_storage_root.display()
        )
    })?;
    let lock_path = thread_episodic_storage_root.join(REFILL_LOCK_FILE_NAME);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(lock_path.as_path())
        .with_context(|| {
            format!(
                "failed to open thread episodic workspace refill lock `{}`",
                lock_path.display()
            )
        })?;
    match Fs4FileExt::try_lock(&file) {
        Ok(()) => Ok(Some(RefillLockGuard { file })),
        Err(Fs4TryLockError::WouldBlock) => Ok(None),
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to acquire thread episodic workspace refill lock `{}`",
                lock_path.display()
            )
        }),
    }
}

pub(crate) async fn refill_is_current(crud_store: &CrudStore) -> Result<bool> {
    let db = crud_store.database_connection();
    let Some(meta) =
        find_projection_meta(&db, THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY).await?
    else {
        return Ok(false);
    };

    Ok(
        meta.projection_version == THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION
            && meta.status == PROJECTION_META_STATUS_COMPLETE,
    )
}

async fn refill_after_marker(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    let now_unix = chrono::Utc::now().timestamp();
    let mut summary =
        cleanup_derived_artifacts(crud_store.as_ref(), now_unix, thread_episodic_storage_root)
            .await?;

    rebuild_refill_items_from_history(crud_store.clone(), now_unix, &mut summary).await?;
    let workspace_ids = crud_store
        .list_thread_episodic_refill_workspace_ids()
        .await
        .context("failed to list thread episodic refill workspaces")?;
    summary.workspace_count = workspace_ids.len();
    let source_counts = crud_store
        .count_thread_episodic_refill_sources()
        .await
        .context("failed to count thread episodic refill sources")?;
    summary.source_thread_count = source_counts.source_thread_count;
    summary.source_turn_count = source_counts.source_turn_count;
    summary.source_turn_item_count = source_counts.source_turn_item_count;

    enqueue_refill_jobs(crud_store.as_ref(), now_unix, &mut summary).await?;
    execute_refill_jobs(crud_store, thread_episodic_storage_root, &mut summary).await?;
    Ok(summary)
}

async fn cleanup_derived_artifacts(
    crud_store: &CrudStore,
    _now_unix: i64,
    thread_episodic_storage_root: &Path,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    let capsules = crud_store
        .list_all_thread_episodic_capsules()
        .await
        .context("failed to list thread episodic capsules before workspace refill")?;
    let mut summary = ThreadEpisodicWorkspaceCapsuleRefillSummary::default();
    for capsule in capsules {
        match delete_capsule_file(capsule.storage_uri.as_str()).await? {
            CapsuleFileDeleteOutcome::Deleted => {
                summary.capsule_files_deleted = summary.capsule_files_deleted.saturating_add(1);
            }
            CapsuleFileDeleteOutcome::Missing => {
                summary.capsule_files_missing = summary.capsule_files_missing.saturating_add(1);
            }
            CapsuleFileDeleteOutcome::NonFileUri => {
                summary.non_file_storage_uris = summary.non_file_storage_uris.saturating_add(1);
            }
        }
    }
    summary.capsule_files_deleted = summary
        .capsule_files_deleted
        .saturating_add(delete_orphan_mv2_files(thread_episodic_storage_root).await?);

    summary.capsule_rows_deleted = crud_store
        .delete_all_thread_episodic_capsules()
        .await
        .context("failed to delete thread episodic capsule rows")?;
    summary.exclusion_rows_deleted = crud_store
        .delete_all_thread_episodic_exclusions()
        .await
        .context("failed to delete thread episodic exclusion rows")?;
    summary.item_rows_deleted = crud_store
        .delete_all_thread_episodic_items()
        .await
        .context("failed to delete thread episodic item rows")?;
    summary.index_jobs_deleted = crud_store
        .delete_all_thread_episodic_index_jobs()
        .await
        .context("failed to delete stale thread episodic index jobs")?;
    summary.thread_directory_rows_deleted = crud_store
        .delete_all_thread_episodic_thread_directory_entries()
        .await
        .context("failed to delete stale thread episodic thread directory rows")?;
    Ok(summary)
}

async fn rebuild_refill_items_from_history(
    crud_store: Arc<CrudStore>,
    now_unix: i64,
    summary: &mut ThreadEpisodicWorkspaceCapsuleRefillSummary,
) -> Result<()> {
    let threads = crud_store
        .list_thread_episodic_refill_threads()
        .await
        .context("failed to list thread episodic source threads for workspace refill")?;
    let ingestor = StoreThreadEpisodicIngestor::with_config(crud_store, true);
    for thread in threads {
        match ingestor
            .reindex_thread_from_history(ThreadEpisodicThreadReindexRequest {
                workspace_id: thread.workspace_id,
                thread_id: thread.thread_id,
                history_event_limit: None,
                item_scan_limit: 1_000_000,
                now_unix,
            })
            .await
        {
            Ok(reindex_summary) => {
                summary.source_threads_reindexed =
                    summary.source_threads_reindexed.saturating_add(1);
                summary.refill_jobs_enqueued = summary.refill_jobs_enqueued.saturating_add(
                    reindex_summary
                        .missing_jobs_created
                        .saturating_add(reindex_summary.existing_jobs),
                );
            }
            Err(error) => {
                summary.source_threads_failed = summary.source_threads_failed.saturating_add(1);
                warn!(
                    error = %format!("{error:#}"),
                    "thread episodic workspace refill skipped one source thread"
                );
            }
        }
    }
    Ok(())
}

async fn enqueue_refill_jobs(
    crud_store: &CrudStore,
    now_unix: i64,
    summary: &mut ThreadEpisodicWorkspaceCapsuleRefillSummary,
) -> Result<()> {
    loop {
        let enqueued = crud_store
            .enqueue_thread_episodic_refill_index_jobs(now_unix, REFILL_ENQUEUE_BATCH_SIZE)
            .await
            .context("failed to enqueue thread episodic refill index jobs")?;
        summary.refill_jobs_enqueued = summary.refill_jobs_enqueued.saturating_add(enqueued);
        if enqueued == 0 {
            break;
        }
    }
    Ok(())
}

async fn execute_refill_jobs(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
    summary: &mut ThreadEpisodicWorkspaceCapsuleRefillSummary,
) -> Result<()> {
    let storage_uri_root = thread_episodic_storage_uri_from_path(thread_episodic_storage_root);
    let backend = MemvidThreadEpisodicBackend::new();
    let payload_provider =
        StoreThreadEpisodicIndexPayloadProvider::new(crud_store.clone(), storage_uri_root);
    let config = ThreadEpisodicIndexExecutorConfig::default();

    for _ in 0..REFILL_EXECUTOR_MAX_BATCHES {
        let now_unix = chrono::Utc::now().timestamp();
        let jobs = crud_store
            .claim_due_thread_episodic_index_jobs(now_unix, REFILL_JOB_CLAIM_LIMIT)
            .await
            .context("failed to claim thread episodic workspace refill index jobs")?;
        if jobs.is_empty() {
            break;
        }

        summary.executor_batches = summary.executor_batches.saturating_add(1);
        for job in jobs {
            let attempt_started_at = Instant::now();
            match payload_provider.resolve_index_request(&job).await {
                Ok(resolved) => {
                    execute_resolved_refill_job(
                        crud_store.as_ref(),
                        &backend,
                        job,
                        resolved,
                        now_unix,
                        config,
                        attempt_started_at,
                        summary,
                    )
                    .await?;
                }
                Err(error) => {
                    let retryable = matches!(
                        error.kind,
                        ThreadEpisodicIndexResolutionFailureKind::Retryable
                    ) && job.attempt_count < config.max_attempts;
                    persist_refill_job_failure(
                        crud_store.as_ref(),
                        &job,
                        retryable,
                        false,
                        Some(error.message),
                        now_unix,
                        attempt_started_at,
                        config,
                    )
                    .await?;
                    if retryable {
                        summary.failed_retryable_jobs =
                            summary.failed_retryable_jobs.saturating_add(1);
                    } else {
                        summary.failed_terminal_jobs =
                            summary.failed_terminal_jobs.saturating_add(1);
                    }
                }
            }
        }
    }

    summary.incomplete_jobs = crud_store
        .count_incomplete_thread_episodic_index_jobs()
        .await
        .context("failed to count incomplete thread episodic index jobs")?;
    if summary.incomplete_jobs > 0 {
        bail!(
            "thread episodic workspace refill left {} incomplete index jobs",
            summary.incomplete_jobs
        );
    }

    Ok(())
}

async fn execute_resolved_refill_job(
    crud_store: &CrudStore,
    backend: &MemvidThreadEpisodicBackend,
    job: ThreadEpisodicIndexJobRecord,
    resolved: ThreadEpisodicResolvedIndexRequest,
    now_unix: i64,
    config: ThreadEpisodicIndexExecutorConfig,
    attempt_started_at: Instant,
    summary: &mut ThreadEpisodicWorkspaceCapsuleRefillSummary,
) -> Result<()> {
    match backend.index_item(resolved.request.clone()).await {
        Ok(output) => {
            let capsule_id = resolved.request.capsule_id.clone();
            let output_stats = output.stats.clone();
            persist_successful_refill_item(
                crud_store,
                &job,
                resolved,
                output,
                now_unix,
                attempt_started_at,
            )
            .await?;
            update_refill_capsule_capacity(
                crud_store,
                capsule_id.as_str(),
                &output_stats,
                None,
                false,
                now_unix,
                config,
            )
            .await;
            if memvid_stats_reach_capacity_threshold(&output_stats, config.near_capacity_percent) {
                rotate_refill_capsule_after_capacity_event(
                    crud_store,
                    capsule_id.as_str(),
                    now_unix,
                    "near_capacity",
                )
                .await;
            }
            summary.completed_jobs = summary.completed_jobs.saturating_add(1);
        }
        Err(error) => {
            let retryable = matches!(
                error.kind,
                ThreadEpisodicMemvidFailureKind::Retryable
                    | ThreadEpisodicMemvidFailureKind::CapacityExceeded
            ) && job.attempt_count < config.max_attempts;
            let capacity_error = matches!(
                error.kind,
                ThreadEpisodicMemvidFailureKind::CapacityExceeded
            );
            if capacity_error {
                update_refill_capsule_capacity(
                    crud_store,
                    resolved.request.capsule_id.as_str(),
                    &ThreadEpisodicMemvidStats::default(),
                    Some(error.message.clone()),
                    true,
                    now_unix,
                    config,
                )
                .await;
                rotate_refill_capsule_after_capacity_event(
                    crud_store,
                    resolved.request.capsule_id.as_str(),
                    now_unix,
                    "capacity_exceeded",
                )
                .await;
            }
            persist_refill_job_failure(
                crud_store,
                &job,
                retryable,
                capacity_error,
                Some(error.message),
                now_unix,
                attempt_started_at,
                config,
            )
            .await?;
            if retryable {
                summary.failed_retryable_jobs = summary.failed_retryable_jobs.saturating_add(1);
            } else {
                summary.failed_terminal_jobs = summary.failed_terminal_jobs.saturating_add(1);
            }
        }
    }

    Ok(())
}

async fn persist_successful_refill_item(
    crud_store: &CrudStore,
    job: &ThreadEpisodicIndexJobRecord,
    resolved: ThreadEpisodicResolvedIndexRequest,
    output: ThreadEpisodicMemvidIndexOutput,
    now_unix: i64,
    attempt_started_at: Instant,
) -> Result<()> {
    crud_store
        .mark_thread_episodic_item_indexed(
            job.index_item_id.as_str(),
            ThreadEpisodicItemIndexedUpdate {
                capsule_id: resolved.request.capsule_id.clone(),
                capsule_ref: resolved.request.capsule_ref.clone(),
                segment_index: resolved.segment_index,
                frame_id: output.frame_id,
                frame_uri: output.frame_uri.clone(),
            },
            now_unix,
        )
        .await
        .with_context(|| {
            format!(
                "failed to persist thread episodic item `{}` frame mapping",
                job.index_item_id
            )
        })?;
    crud_store
        .complete_thread_episodic_index_job(
            job.id.as_str(),
            ThreadEpisodicIndexJobCompletionUpdate {
                capsule_id: resolved.request.capsule_id,
                capsule_ref: resolved.request.capsule_ref,
                segment_index: resolved.segment_index,
                frame_uri: output.frame_uri,
                last_attempt_latency_ms: Some(elapsed_ms(attempt_started_at)),
            },
            now_unix,
        )
        .await
        .with_context(|| {
            format!(
                "failed to mark thread episodic refill job `{}` completed",
                job.id
            )
        })?;
    refresh_refill_thread_directory(
        crud_store,
        job.workspace_id.as_str(),
        job.thread_id.as_str(),
        now_unix,
    )
    .await?;

    Ok(())
}

async fn persist_refill_job_failure(
    crud_store: &CrudStore,
    job: &ThreadEpisodicIndexJobRecord,
    retryable: bool,
    capacity_error: bool,
    error_message: Option<String>,
    now_unix: i64,
    attempt_started_at: Instant,
    config: ThreadEpisodicIndexExecutorConfig,
) -> Result<()> {
    let next_run_at_unix = if retryable && capacity_error {
        Some(now_unix)
    } else {
        retryable.then(|| next_refill_retry_at(job, now_unix, config))
    };
    let sanitized_error =
        error_message.map(|message| sanitize_refill_index_error(message.as_str()));
    crud_store
        .fail_thread_episodic_index_job(
            job.id.as_str(),
            ThreadEpisodicIndexJobFailureUpdate {
                retryable,
                next_run_at_unix,
                last_error: sanitized_error,
                capacity_error,
                last_attempt_latency_ms: Some(elapsed_ms(attempt_started_at)),
            },
            now_unix,
        )
        .await
        .with_context(|| {
            format!(
                "failed to persist thread episodic refill job `{}` failure",
                job.id
            )
        })?;

    if !retryable {
        crud_store
            .mark_thread_episodic_item_failed(job.index_item_id.as_str(), now_unix)
            .await
            .with_context(|| {
                format!(
                    "failed to mark thread episodic refill item `{}` failed",
                    job.index_item_id
                )
            })?;
    }

    Ok(())
}

async fn update_refill_capsule_capacity(
    crud_store: &CrudStore,
    capsule_id: &str,
    stats: &ThreadEpisodicMemvidStats,
    last_error: Option<String>,
    capacity_exceeded: bool,
    now_unix: i64,
    config: ThreadEpisodicIndexExecutorConfig,
) {
    let now = fixed_datetime_from_unix(now_unix);
    let near_capacity_at = stats
        .utilization_percent
        .and_then(|value| (value >= config.near_capacity_percent).then_some(now));
    let update = ThreadEpisodicCapsuleCapacityUpdate {
        capacity_bytes: stats.capacity_bytes,
        size_bytes: stats.size_bytes,
        utilization_percent: stats.utilization_percent,
        active_frame_count: stats.active_frame_count,
        near_capacity_at,
        capacity_exceeded_at: capacity_exceeded.then_some(now),
        last_error,
    };
    if let Err(error) = crud_store
        .update_thread_episodic_capsule_capacity(capsule_id, update, now_unix)
        .await
    {
        warn!(
            capsule_id,
            error = %error,
            "failed to update thread episodic refill capsule capacity metadata"
        );
    }
}

async fn rotate_refill_capsule_after_capacity_event(
    crud_store: &CrudStore,
    capsule_id: &str,
    now_unix: i64,
    reason: &str,
) {
    match crud_store
        .transition_thread_episodic_active_write_segment(
            capsule_id,
            ThreadEpisodicCapsuleWriteState::Full,
            now_unix,
        )
        .await
    {
        Ok(Some(rotated)) => {
            info!(
                capsule_id = %rotated.id,
                segment_index = rotated.segment_index,
                reason,
                "thread episodic workspace refill rotated active segment to full"
            );
        }
        Ok(None) => {
            warn!(
                capsule_id,
                reason, "thread episodic workspace refill segment rotation skipped"
            );
        }
        Err(error) => {
            warn!(
                capsule_id,
                reason,
                error = %error,
                "thread episodic workspace refill failed to rotate active segment"
            );
        }
    }
}

async fn refresh_refill_thread_directory(
    crud_store: &CrudStore,
    workspace_id: &str,
    thread_id: &str,
    now_unix: i64,
) -> Result<()> {
    let indexed_item_count = crud_store
        .count_active_thread_episodic_items_for_thread(workspace_id, thread_id)
        .await
        .with_context(|| {
            format!("failed to count thread episodic items for refill thread `{thread_id}`")
        })?;
    crud_store
        .upsert_thread_episodic_thread_directory_entry(
            NewThreadEpisodicThreadDirectoryRecord {
                id: None,
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                title: None,
                summary_hash: None,
                summary_ref: None,
                thread_created_at: None,
                thread_updated_at: Some(fixed_datetime_from_unix(now_unix)),
                last_indexed_at: Some(fixed_datetime_from_unix(now_unix)),
                indexed_item_count,
                task_affinity_json: None,
                project_affinity_json: None,
                visibility: ThreadEpisodicThreadDirectoryVisibility::Visible,
                status: ThreadEpisodicThreadDirectoryStatus::Active,
            },
            now_unix,
        )
        .await
        .with_context(|| {
            format!("failed to refresh thread episodic refill directory for thread `{thread_id}`")
        })?;
    Ok(())
}

fn next_refill_retry_at(
    job: &ThreadEpisodicIndexJobRecord,
    now_unix: i64,
    config: ThreadEpisodicIndexExecutorConfig,
) -> i64 {
    let exponent = job.attempt_count.saturating_sub(1).clamp(0, 8) as u32;
    let delay = config
        .retry_base_delay_secs
        .saturating_mul(2_i64.saturating_pow(exponent))
        .min(config.retry_max_delay_secs);
    now_unix.saturating_add(delay)
}

fn sanitize_refill_index_error(message: &str) -> String {
    let mut sanitized = message.split_whitespace().collect::<Vec<_>>().join(" ");
    if sanitized.chars().count() > REFILL_INDEX_ERROR_MAX_CHARS {
        sanitized = sanitized
            .chars()
            .take(REFILL_INDEX_ERROR_MAX_CHARS)
            .collect();
    }
    sanitized
}

fn fixed_datetime_from_unix(value: i64) -> DateTimeWithTimeZone {
    chrono::DateTime::from_timestamp(value, 0)
        .unwrap_or_else(chrono::Utc::now)
        .fixed_offset()
}

fn elapsed_ms(started_at: Instant) -> i64 {
    started_at.elapsed().as_millis().min(i64::MAX as u128) as i64
}

enum CapsuleFileDeleteOutcome {
    Deleted,
    Missing,
    NonFileUri,
}

async fn delete_capsule_file(storage_uri: &str) -> Result<CapsuleFileDeleteOutcome> {
    let Some(path) = storage_uri.strip_prefix("file://") else {
        return Ok(CapsuleFileDeleteOutcome::NonFileUri);
    };
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(CapsuleFileDeleteOutcome::Deleted),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(CapsuleFileDeleteOutcome::Missing)
        }
        Err(error) => Err(error)
            .with_context(|| format!("failed to delete thread episodic capsule file `{path}`")),
    }
}

async fn delete_orphan_mv2_files(thread_episodic_storage_root: &Path) -> Result<u64> {
    let thread_episodic_root = thread_episodic_storage_root.join("thread_episodic");
    let mut deleted = 0_u64;
    let mut dirs = vec![thread_episodic_root];
    while let Some(dir) = dirs.pop() {
        let mut entries = match tokio::fs::read_dir(dir.as_path()).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to read thread episodic capsule directory `{}`",
                        dir.display()
                    )
                });
            }
        };
        while let Some(entry) = entries.next_entry().await.with_context(|| {
            format!(
                "failed to read thread episodic capsule directory entry in `{}`",
                dir.display()
            )
        })? {
            let path = entry.path();
            let file_type = entry.file_type().await.with_context(|| {
                format!(
                    "failed to read thread episodic capsule path type `{}`",
                    path.display()
                )
            })?;
            if file_type.is_dir() {
                dirs.push(path);
            } else if file_type.is_file()
                && path.extension().and_then(|extension| extension.to_str()) == Some("mv2")
            {
                match tokio::fs::remove_file(path.as_path()).await {
                    Ok(()) => {
                        deleted = deleted.saturating_add(1);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!(
                                "failed to delete orphan thread episodic capsule file `{}`",
                                path.display()
                            )
                        });
                    }
                }
            }
        }
    }
    Ok(deleted)
}

async fn mark_refill_complete(
    db: &sea_orm::DatabaseConnection,
    summary: &ThreadEpisodicWorkspaceCapsuleRefillSummary,
) -> Result<()> {
    let now = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY.to_owned(),
            projection_version: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: summary.source_thread_count,
            source_turn_count: summary.source_turn_count,
            source_turn_item_count: summary.source_turn_item_count,
            source_turn_event_count: saturating_i64_from_u64(
                (summary.completed_jobs.min(i64::MAX as usize) as u64)
                    .saturating_add(summary.capsule_files_deleted),
            ),
            last_error: None,
            backfill_started_at: Some(now),
            backfilled_at: Some(now),
            created_at: now,
            updated_at: now,
        },
    )
    .await
}

fn saturating_i64_from_u64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

async fn mark_refill_failed(db: &sea_orm::DatabaseConnection, error: &anyhow::Error) -> Result<()> {
    let now = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY.to_owned(),
            projection_version: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            status: PROJECTION_META_STATUS_FAILED.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: Some(format!("{error:#}")),
            backfill_started_at: None,
            backfilled_at: None,
            created_at: now,
            updated_at: now,
        },
    )
    .await
}

fn now_datetime() -> DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::bootstrap;
    use crate::thread_episodic::{
        StoreThreadEpisodicIngestor, ThreadEpisodicCommittedItem, ThreadEpisodicIngestor,
    };
    use crate::workspace::WorkspaceManager;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{
        NewThreadEpisodicIndexJobRecord, NewThreadEpisodicItemRecord,
        ThreadEpisodicActiveWriteSegmentRequest, ThreadEpisodicGraphEnrichmentState,
        ThreadEpisodicIndexJobStatus, ThreadEpisodicItemStatus, ThreadEpisodicItemVisibility,
        ThreadEpisodicSourceActorRole, ThreadEpisodicSourceRuntimeKind,
    };
    use pioneer_protocol::{
        ItemCompletedNotification, SandboxMode, Thread,
        ThreadEpisodicSourceActorRole as ProtocolThreadEpisodicSourceActorRole,
        ThreadEpisodicSourceContext, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
        ThreadStatus, Turn, TurnItem, TurnItemType, TurnKind, TurnOrigin, TurnStatus, UserInput,
    };
    use sea_orm::Database;
    use tempfile::TempDir;

    #[tokio::test]
    async fn thread_episodic_workspace_refill_complete_marker_skips_migration() {
        let (crud_store, temp_dir, _workspace_id) = setup_store().await;
        mark_refill_marker(
            crud_store.as_ref(),
            PROJECTION_META_STATUS_COMPLETE,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
        )
        .await;

        let summary = refill_once(crud_store, temp_dir.path())
            .await
            .expect("complete marker should skip");

        assert!(summary.skipped);
    }

    #[tokio::test]
    async fn thread_episodic_workspace_refill_lock_prevents_duplicate_migration() {
        let (crud_store, temp_dir, _workspace_id) = setup_store().await;
        let _guard = try_acquire_refill_lock(temp_dir.path())
            .expect("lock acquisition should not error")
            .expect("first lock should be acquired");

        let summary = refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("contended refill should skip without error");

        assert!(summary.skipped);
        assert!(summary.lock_contended);
        assert!(
            find_projection_meta(
                &crud_store.database_connection(),
                THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY,
            )
            .await
            .expect("meta query should succeed")
            .is_none()
        );
    }

    #[tokio::test]
    async fn thread_episodic_workspace_refill_missing_marker_marks_empty_database_complete() {
        let (crud_store, temp_dir, _workspace_id) = setup_store().await;

        let summary = refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("empty refill should complete");

        assert!(!summary.skipped);
        assert_eq!(summary.workspace_count, 0);
        let meta = find_projection_meta(
            &crud_store.database_connection(),
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY,
        )
        .await
        .expect("meta query should succeed")
        .expect("meta exists");
        assert_eq!(meta.status, PROJECTION_META_STATUS_COMPLETE);
        assert_eq!(
            meta.projection_version,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION
        );

        let skipped = refill_once(crud_store, temp_dir.path())
            .await
            .expect("second refill should skip");
        assert!(skipped.skipped);
    }

    #[tokio::test]
    async fn thread_episodic_workspace_refill_retries_failed_backfilling_and_old_versions() {
        for (status, version) in [
            (PROJECTION_META_STATUS_FAILED, 1),
            (PROJECTION_META_STATUS_BACKFILLING, 1),
            (PROJECTION_META_STATUS_COMPLETE, 0),
        ] {
            let (crud_store, temp_dir, _workspace_id) = setup_store().await;
            mark_refill_marker(crud_store.as_ref(), status, version).await;

            let summary = refill_once(crud_store.clone(), temp_dir.path())
                .await
                .expect("non-current marker should retry");

            assert!(!summary.skipped, "status={status} version={version}");
            let meta = find_projection_meta(
                &crud_store.database_connection(),
                THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY,
            )
            .await
            .expect("meta query should succeed")
            .expect("meta exists");
            assert_eq!(meta.status, PROJECTION_META_STATUS_COMPLETE);
            assert_eq!(
                meta.projection_version,
                THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION
            );
        }
    }

    #[tokio::test]
    async fn thread_episodic_workspace_refill_cleanup_resets_derived_artifacts_rerunnably() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let old_capsule = crud_store
            .resolve_thread_episodic_active_write_segment(
                ThreadEpisodicActiveWriteSegmentRequest {
                    workspace_id: workspace_id.clone(),
                    thread_id: "old_thread_capsule".to_owned(),
                    storage_uri_root: thread_episodic_storage_uri_from_path(temp_dir.path()),
                },
                1_700_000_000,
            )
            .await
            .expect("old per-thread capsule should resolve");
        let old_capsule_path = PathBuf::from(
            old_capsule
                .storage_uri
                .strip_prefix("file://")
                .expect("test storage uri should be file uri"),
        );
        tokio::fs::create_dir_all(
            old_capsule_path
                .parent()
                .expect("old capsule should have parent directory"),
        )
        .await
        .expect("old capsule parent dir should be created");
        tokio::fs::write(&old_capsule_path, b"stale old mv2")
            .await
            .expect("old capsule file should be created");
        let orphan_capsule_path = temp_dir
            .path()
            .join("thread_episodic")
            .join("orphan_workspace")
            .join("orphan_thread")
            .join("orphan.mv2");
        tokio::fs::create_dir_all(
            orphan_capsule_path
                .parent()
                .expect("orphan capsule should have parent directory"),
        )
        .await
        .expect("orphan capsule parent dir should be created");
        tokio::fs::write(&orphan_capsule_path, b"orphan old mv2")
            .await
            .expect("orphan capsule file should be created");
        let missing_old_capsule = crud_store
            .resolve_thread_episodic_active_write_segment(
                ThreadEpisodicActiveWriteSegmentRequest {
                    workspace_id: workspace_id.clone(),
                    thread_id: "old_thread_missing_file".to_owned(),
                    storage_uri_root: thread_episodic_storage_uri_from_path(temp_dir.path()),
                },
                1_700_000_000,
            )
            .await
            .expect("missing old per-thread capsule should resolve");
        assert_ne!(old_capsule.id, missing_old_capsule.id);
        let item = crud_store
            .upsert_thread_episodic_item(
                NewThreadEpisodicItemRecord {
                    id: None,
                    workspace_id: workspace_id.clone(),
                    thread_id: "old_thread_capsule".to_owned(),
                    turn_id: "turn_cleanup".to_owned(),
                    item_id: "item_cleanup".to_owned(),
                    source_actor_role: ThreadEpisodicSourceActorRole::User,
                    source_runtime_kind: ThreadEpisodicSourceRuntimeKind::UserTurn,
                    source_context: ThreadEpisodicSourceContext::UserVisibleThreadItem,
                    visibility: ThreadEpisodicItemVisibility::UserVisible,
                    status: ThreadEpisodicItemStatus::Active,
                    text_hash: "a".repeat(64),
                    source_text_hash: "b".repeat(64),
                    language_hint: None,
                    token_estimate: 1,
                    capsule_id: Some(old_capsule.id.clone()),
                    capsule_ref: Some(old_capsule.capsule_ref.clone()),
                    segment_index: Some(old_capsule.segment_index),
                    frame_id: Some(7),
                    frame_uri: Some("mv2://old/thread/frame".to_owned()),
                    indexed_at: Some(now_datetime()),
                    deleted_at: None,
                },
                1_700_000_000,
            )
            .await
            .expect("old item should insert");
        let job = crud_store
            .insert_thread_episodic_index_job_if_absent(
                NewThreadEpisodicIndexJobRecord {
                    id: None,
                    workspace_id: workspace_id.clone(),
                    thread_id: "old_thread_capsule".to_owned(),
                    index_item_id: item.id.clone(),
                    capsule_id: Some(old_capsule.id.clone()),
                    capsule_ref: Some(old_capsule.capsule_ref.clone()),
                    segment_index: Some(old_capsule.segment_index),
                    frame_uri: Some("mv2://old/thread/frame".to_owned()),
                    status: ThreadEpisodicIndexJobStatus::Completed,
                    graph_enrichment_state: ThreadEpisodicGraphEnrichmentState::NotSupported,
                    next_run_at: now_datetime(),
                    last_error: None,
                },
                1_700_000_000,
            )
            .await
            .expect("old job should insert");
        crud_store
            .upsert_thread_episodic_thread_directory_entry(
                NewThreadEpisodicThreadDirectoryRecord {
                    id: None,
                    workspace_id: workspace_id.clone(),
                    thread_id: "old_thread_capsule".to_owned(),
                    title: None,
                    summary_hash: None,
                    summary_ref: None,
                    thread_created_at: None,
                    thread_updated_at: Some(now_datetime()),
                    last_indexed_at: Some(now_datetime()),
                    indexed_item_count: 99,
                    task_affinity_json: None,
                    project_affinity_json: None,
                    visibility: ThreadEpisodicThreadDirectoryVisibility::Visible,
                    status: ThreadEpisodicThreadDirectoryStatus::Active,
                },
                1_700_000_000,
            )
            .await
            .expect("old directory entry should insert");

        let summary =
            cleanup_derived_artifacts(crud_store.as_ref(), 1_700_000_010, temp_dir.path())
                .await
                .expect("cleanup should succeed");

        assert_eq!(summary.capsule_rows_deleted, 2);
        assert_eq!(summary.capsule_files_deleted, 2);
        assert_eq!(summary.capsule_files_missing, 1);
        assert_eq!(summary.item_rows_deleted, 1);
        assert_eq!(summary.exclusion_rows_deleted, 0);
        assert_eq!(summary.index_jobs_deleted, 1);
        assert_eq!(summary.thread_directory_rows_deleted, 1);
        assert!(!old_capsule_path.exists());
        assert!(!orphan_capsule_path.exists());
        assert!(
            crud_store
                .list_all_thread_episodic_capsules()
                .await
                .expect("capsules should list")
                .is_empty()
        );
        assert!(
            crud_store
                .find_thread_episodic_item(item.id.as_str())
                .await
                .expect("item should load")
                .is_none()
        );
        assert!(
            crud_store
                .find_thread_episodic_index_job(job.id.as_str())
                .await
                .expect("job lookup succeeds")
                .is_none()
        );
        assert!(
            crud_store
                .find_thread_episodic_thread_directory_entry(
                    workspace_id.as_str(),
                    "old_thread_capsule"
                )
                .await
                .expect("directory lookup succeeds")
                .is_none()
        );

        let second = cleanup_derived_artifacts(crud_store.as_ref(), 1_700_000_011, temp_dir.path())
            .await
            .expect("cleanup should be rerunnable");
        assert_eq!(second.capsule_rows_deleted, 0);
        assert_eq!(second.item_rows_deleted, 0);
        assert_eq!(second.exclusion_rows_deleted, 0);
        assert_eq!(second.index_jobs_deleted, 0);
        assert_eq!(second.thread_directory_rows_deleted, 0);
    }

    #[tokio::test]
    async fn thread_episodic_workspace_refill_rebuilds_workspace_capsule_from_database() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_a = "thread_refill_a";
        let thread_b = "thread_refill_b";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_a,
            "turn_refill_a",
            "item_refill_a",
            "workspace refill should index thread A database memory",
        )
        .await;
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_b,
            "turn_refill_b",
            "item_refill_b",
            "workspace refill should index thread B database memory",
        )
        .await;

        let summary = refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("workspace refill should complete");

        assert!(!summary.skipped);
        assert_eq!(summary.workspace_count, 1);
        assert_eq!(summary.source_thread_count, 2);
        assert_eq!(summary.source_turn_count, 2);
        assert_eq!(summary.source_turn_item_count, 2);
        assert_eq!(summary.refill_jobs_enqueued, 2);
        assert_eq!(summary.completed_jobs, 2);
        let meta = find_projection_meta(
            &crud_store.database_connection(),
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY,
        )
        .await
        .expect("meta query should succeed")
        .expect("meta exists");
        assert_eq!(meta.status, PROJECTION_META_STATUS_COMPLETE);
        assert_eq!(meta.source_thread_count, 2);
        assert_eq!(meta.source_turn_count, 2);
        assert_eq!(meta.source_turn_item_count, 2);
        assert_eq!(meta.source_turn_event_count, 2);
        let capsules = crud_store
            .list_thread_episodic_workspace_capsules(workspace_id.as_str(), 10)
            .await
            .expect("workspace capsules should list");
        assert_eq!(capsules.len(), 1);
        assert_eq!(
            capsules[0].thread_id,
            pioneer_crud::THREAD_EPISODIC_WORKSPACE_CAPSULE_THREAD_ID
        );
        assert!(PathBuf::from(capsules[0].storage_uri.trim_start_matches("file://")).exists());
        for thread_id in [thread_a, thread_b] {
            let items = crud_store
                .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 10)
                .await
                .expect("items should list");
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].status, ThreadEpisodicItemStatus::Active);
            assert_eq!(
                items[0].capsule_id.as_deref(),
                Some(capsules[0].id.as_str())
            );
            assert!(
                items[0]
                    .frame_uri
                    .as_deref()
                    .expect("frame uri")
                    .starts_with("mv2://workspace/")
            );
        }
    }

    #[tokio::test]
    async fn thread_episodic_workspace_refill_deletes_orphan_derived_items_before_rebuild() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let orphan_item = crud_store
            .upsert_thread_episodic_item(
                NewThreadEpisodicItemRecord {
                    id: None,
                    workspace_id: workspace_id.clone(),
                    thread_id: "thread_missing_source".to_owned(),
                    turn_id: "turn_missing_source".to_owned(),
                    item_id: "item_missing_source".to_owned(),
                    source_actor_role: ThreadEpisodicSourceActorRole::User,
                    source_runtime_kind: ThreadEpisodicSourceRuntimeKind::UserTurn,
                    source_context: ThreadEpisodicSourceContext::UserVisibleThreadItem,
                    visibility: ThreadEpisodicItemVisibility::UserVisible,
                    status: ThreadEpisodicItemStatus::PendingIndex,
                    text_hash: "a".repeat(64),
                    source_text_hash: "b".repeat(64),
                    language_hint: None,
                    token_estimate: 1,
                    capsule_id: None,
                    capsule_ref: None,
                    segment_index: None,
                    frame_id: None,
                    frame_uri: None,
                    indexed_at: None,
                    deleted_at: None,
                },
                1_700_000_000,
            )
            .await
            .expect("item should insert");

        let summary = refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("orphan derived item should be deleted before refill");
        assert_eq!(summary.item_rows_deleted, 1);
        assert_eq!(summary.refill_jobs_enqueued, 0);
        assert!(
            crud_store
                .find_thread_episodic_item(orphan_item.id.as_str())
                .await
                .expect("orphan item lookup should succeed")
                .is_none()
        );
        let meta = find_projection_meta(
            &crud_store.database_connection(),
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY,
        )
        .await
        .expect("meta query should succeed")
        .expect("meta exists");
        assert_eq!(meta.status, PROJECTION_META_STATUS_COMPLETE);
    }

    async fn setup_store() -> (Arc<CrudStore>, TempDir, String) {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        bootstrap(&connection)
            .await
            .expect("gateway bootstrap should create default workspace");
        let workspace_manager = WorkspaceManager::new(connection.clone());
        let workspace_id = workspace_manager
            .list_workspaces()
            .await
            .expect("workspace list should succeed")
            .into_iter()
            .find(|workspace| workspace.is_active && workspace.is_current)
            .expect("current workspace should exist")
            .id;
        (
            Arc::new(CrudStore::new(connection)),
            TempDir::new().expect("temp dir"),
            workspace_id,
        )
    }

    async fn mark_refill_marker(crud_store: &CrudStore, status: &str, version: i64) {
        let now = now_datetime();
        upsert_projection_meta(
            &crud_store.database_connection(),
            ProjectionMetaRecord {
                projection_key: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY.to_owned(),
                projection_version: version,
                status: status.to_owned(),
                source_thread_count: 0,
                source_turn_count: 0,
                source_turn_item_count: 0,
                source_turn_event_count: 0,
                last_error: None,
                backfill_started_at: Some(now),
                backfilled_at: (status == PROJECTION_META_STATUS_COMPLETE).then_some(now),
                created_at: now,
                updated_at: now,
            },
        )
        .await
        .expect("marker should upsert");
    }

    async fn ingest_materialized_user_item(
        crud_store: Arc<CrudStore>,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        text: &str,
    ) {
        let item = TurnItem::UserMessage {
            id: item_id.to_owned(),
            text: text.to_owned(),
            attachments: Vec::new(),
        };
        materialize_thread_with_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            item.clone(),
            1_700_000_000,
        )
        .await;
        let ingestor = StoreThreadEpisodicIngestor::new(crud_store);
        let outcome = ingestor
            .ingest_committed_item(ThreadEpisodicCommittedItem {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item_id: item_id.to_owned(),
                item_type: TurnItemType::UserMessage,
                source_actor_role: Some(ProtocolThreadEpisodicSourceActorRole::User),
                source_context: ThreadEpisodicSourceContext::UserVisibleThreadItem,
                item,
            })
            .await
            .expect("ingestion should succeed");
        assert!(matches!(
            outcome,
            crate::thread_episodic::ThreadEpisodicIngestionOutcome::Accepted
        ));
    }

    async fn materialize_thread_with_item(
        crud_store: &CrudStore,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item: TurnItem,
        timestamp: i64,
    ) {
        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "test-model".to_owned(),
            model_provider: "test-provider".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::Completed,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        crud_store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &Vec::<UserInput>::new(),
            )
            .await
            .expect("turn start should materialize");
        crud_store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item,
                },
                timestamp + 1,
            )
            .await
            .expect("item completed should materialize");
    }
}
