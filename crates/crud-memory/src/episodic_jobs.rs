//! Transactional admission, completion, and recovery of episodic indexing jobs.
use anyhow::{Context, Result};
use sea_orm::TransactionTrait;

use crate::episodic_repository as thread_episodic_repository;
use crate::store::MemoryStore;
use crate::thread_episodic::{
    NewThreadEpisodicIndexJobRecord, THREAD_EPISODIC_PROJECTION_CHANGED_ERROR,
    ThreadEpisodicIndexAttemptOutcome, ThreadEpisodicIndexJobCompletionUpdate,
    ThreadEpisodicIndexJobFailureUpdate, ThreadEpisodicIndexJobRecord,
    ThreadEpisodicIndexJobStatus, ThreadEpisodicItemIndexedUpdate,
    thread_episodic_index_job_record_from_model,
};
use crate::util::unix_to_datetime;

impl MemoryStore {
    pub async fn enqueue_thread_episodic_refill_index_jobs(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<usize> {
        let items =
            thread_episodic_repository::list_refill_indexable_items(&self.connection, limit)
                .await?;
        self.enqueue_refill_items(items, now_unix).await
    }

    pub async fn enqueue_thread_episodic_refill_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
        now_unix: i64,
        limit: u64,
    ) -> Result<usize> {
        let items = thread_episodic_repository::list_refill_indexable_items_for_workspace(
            &self.connection,
            workspace_id,
            limit,
        )
        .await?;
        self.enqueue_refill_items(items, now_unix).await
    }

    async fn enqueue_refill_items(
        &self,
        items: Vec<pioneer_entity::thread_episodic_items::Model>,
        now_unix: i64,
    ) -> Result<usize> {
        let now = unix_to_datetime(now_unix);
        let mut enqueued = 0;
        for item in items {
            let inserted = self
                .retry_write(|| async {
                    let tx = self.connection.begin().await?;
                    if thread_episodic_repository::source_occurrence_is_excluded(
                        &tx,
                        &item.workspace_id,
                        &item.thread_id,
                        &item.turn_id,
                        &item.item_id,
                    )
                    .await?
                    {
                        tx.commit().await?;
                        return Ok(false);
                    }
                    let (_, inserted) = thread_episodic_repository::insert_index_job_if_absent(
                        &tx,
                        NewThreadEpisodicIndexJobRecord {
                            id: None,
                            workspace_id: item.workspace_id.clone(),
                            thread_id: item.thread_id.clone(),
                            index_item_id: item.id.clone(),
                            status: ThreadEpisodicIndexJobStatus::Queued,
                            next_run_at: now,
                            last_error: None,
                        },
                        now,
                    )
                    .await?;
                    tx.commit().await?;
                    Ok(inserted)
                })
                .await?;
            enqueued += usize::from(inserted);
        }
        Ok(enqueued)
    }

    pub async fn insert_thread_episodic_index_job_if_absent(
        &self,
        job: NewThreadEpisodicIndexJobRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicIndexJobRecord> {
        self.retry_write(|| {
            let job = job.clone();
            async move {
                let transaction = self.connection.begin().await?;
                let (row, _) = thread_episodic_repository::insert_index_job_if_absent(
                    &transaction,
                    job,
                    unix_to_datetime(now_unix),
                )
                .await?;
                transaction.commit().await?;
                thread_episodic_index_job_record_from_model(row)
            }
        })
        .await
    }

    pub async fn retry_failed_or_stale_thread_episodic_index_job(
        &self,
        job_id: &str,
        stale_before_unix: i64,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.retry_write(|| {
            let job_id = job_id.to_owned();
            async move {
                thread_episodic_repository::retry_failed_or_stale_index_job(
                    &self.connection,
                    job_id.as_str(),
                    unix_to_datetime(stale_before_unix),
                    unix_to_datetime(now_unix),
                )
                .await?
                .map(thread_episodic_index_job_record_from_model)
                .transpose()
            }
        })
        .await
    }

    pub async fn requeue_canceled_thread_episodic_index_job(
        &self,
        job_id: &str,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.retry_write(|| {
            let job_id = job_id.to_owned();
            async move {
                thread_episodic_repository::requeue_canceled_index_job(
                    &self.connection,
                    job_id.as_str(),
                    unix_to_datetime(now_unix),
                )
                .await?
                .map(thread_episodic_index_job_record_from_model)
                .transpose()
            }
        })
        .await
    }

    pub async fn requeue_running_thread_episodic_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
        now_unix: i64,
        max_attempts: i64,
    ) -> Result<u64> {
        let mut total = 0u64;
        loop {
            let requeued = self
                .retry_write(|| {
                    let workspace_id = workspace_id.to_owned();
                    async move {
                        let transaction = self.connection.begin().await?;
                        let recovered =
                            thread_episodic_repository::requeue_running_index_jobs_for_workspace(
                                &transaction,
                                workspace_id.as_str(),
                                unix_to_datetime(now_unix),
                                max_attempts,
                            )
                            .await?;
                        transaction.commit().await?;
                        Ok(recovered)
                    }
                })
                .await?;
            total = total.saturating_add(requeued);
            if requeued == 0 {
                return Ok(total);
            }
        }
    }

    pub async fn claim_due_thread_episodic_index_jobs(
        &self,
        now_unix: i64,
        limit: u64,
        max_attempts: i64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        self.retry_write(|| async move {
            let now = unix_to_datetime(now_unix);
            let rows =
                thread_episodic_repository::list_due_index_jobs(&self.connection, now, limit)
                    .await?;
            let mut claimed = Vec::with_capacity(rows.len());
            for row in rows {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin thread episodic index claim transaction")?;
                let claimed_row = thread_episodic_repository::mark_index_job_running(
                    &transaction,
                    row.id.as_str(),
                    now,
                    max_attempts,
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit thread episodic index claim transaction")?;
                if let Some(row) = claimed_row {
                    claimed.push(thread_episodic_index_job_record_from_model(row)?);
                }
            }
            Ok(claimed)
        })
        .await
    }

    pub async fn complete_thread_episodic_index_job(
        &self,
        job_id: &str,
        update: ThreadEpisodicIndexJobCompletionUpdate,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.retry_write(|| {
            let job_id = job_id.to_owned();
            let update = update.clone();
            async move {
                thread_episodic_repository::mark_index_job_completed(
                    &self.connection,
                    job_id.as_str(),
                    update,
                    unix_to_datetime(now_unix),
                )
                .await?
                .map(thread_episodic_index_job_record_from_model)
                .transpose()
            }
        })
        .await
    }

    pub async fn complete_thread_episodic_index_attempt(
        &self,
        job_id: &str,
        expected_attempt_count: i64,
        expected_source_payload: &str,
        item_update: ThreadEpisodicItemIndexedUpdate,
        job_update: ThreadEpisodicIndexJobCompletionUpdate,
        now_unix: i64,
    ) -> Result<ThreadEpisodicIndexAttemptOutcome> {
        self.retry_write(|| {
            let job_id = job_id.to_owned();
            let expected_source_payload =
                pioneer_crud_conversations::turn_payload::PreparedItemPayload::new(
                    expected_source_payload.to_owned(),
                );
            let item_update = item_update.clone();
            let job_update = job_update.clone();
            async move {
                let transaction =
                    self.connection.begin().await.context(
                        "failed to begin thread episodic attempt completion transaction",
                    )?;
                let outcome = thread_episodic_repository::complete_index_attempt(
                    &transaction,
                    job_id.as_str(),
                    expected_attempt_count,
                    &expected_source_payload,
                    item_update,
                    job_update,
                    unix_to_datetime(now_unix),
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit thread episodic attempt completion")?;
                Ok(outcome)
            }
        })
        .await
    }

    pub async fn fail_thread_episodic_index_job(
        &self,
        job_id: &str,
        update: ThreadEpisodicIndexJobFailureUpdate,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.retry_write(|| {
            let job_id = job_id.to_owned();
            let update = update.clone();
            async move {
                thread_episodic_repository::mark_index_job_failed(
                    &self.connection,
                    job_id.as_str(),
                    update,
                    unix_to_datetime(now_unix),
                )
                .await?
                .map(thread_episodic_index_job_record_from_model)
                .transpose()
            }
        })
        .await
    }

    pub async fn fail_thread_episodic_index_attempt(
        &self,
        job_id: &str,
        expected_attempt_count: i64,
        expected_source_payload: &str,
        update: ThreadEpisodicIndexJobFailureUpdate,
        now_unix: i64,
    ) -> Result<ThreadEpisodicIndexAttemptOutcome> {
        self.retry_write(|| {
            let job_id = job_id.to_owned();
            let expected_source_payload =
                pioneer_crud_conversations::turn_payload::PreparedItemPayload::new(
                    expected_source_payload.to_owned(),
                );
            let update = update.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin thread episodic attempt failure transaction")?;
                let outcome = thread_episodic_repository::fail_index_attempt(
                    &transaction,
                    job_id.as_str(),
                    expected_attempt_count,
                    &expected_source_payload,
                    update,
                    unix_to_datetime(now_unix),
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit thread episodic attempt failure")?;
                Ok(outcome)
            }
        })
        .await
    }

    pub async fn requeue_thread_episodic_index_attempt(
        &self,
        job_id: &str,
        expected_attempt_count: i64,
        now_unix: i64,
        last_error: Option<&str>,
    ) -> Result<ThreadEpisodicIndexAttemptOutcome> {
        self.retry_write(|| {
            let job_id = job_id.to_owned();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin thread episodic attempt requeue")?;
                let outcome = thread_episodic_repository::requeue_index_attempt(
                    &transaction,
                    job_id.as_str(),
                    expected_attempt_count,
                    unix_to_datetime(now_unix),
                    last_error,
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit thread episodic attempt requeue")?;
                Ok(outcome)
            }
        })
        .await
    }

    pub async fn cancel_thread_episodic_index_attempt(
        &self,
        job_id: &str,
        expected_attempt_count: i64,
        last_error: &str,
        now_unix: i64,
    ) -> Result<ThreadEpisodicIndexAttemptOutcome> {
        self.retry_write(|| {
            let job_id = job_id.to_owned();
            let last_error = last_error.to_owned();
            async move {
                let transaction = self.connection.begin().await?;
                let outcome = thread_episodic_repository::cancel_index_attempt(
                    &transaction,
                    job_id.as_str(),
                    expected_attempt_count,
                    last_error.as_str(),
                    unix_to_datetime(now_unix),
                )
                .await?;
                transaction.commit().await?;
                Ok(outcome)
            }
        })
        .await
    }

    pub async fn fail_thread_episodic_index_attempt_without_source_validation(
        &self,
        job_id: &str,
        expected_attempt_count: i64,
        update: ThreadEpisodicIndexJobFailureUpdate,
        now_unix: i64,
    ) -> Result<ThreadEpisodicIndexAttemptOutcome> {
        self.retry_write(|| {
            let job_id = job_id.to_owned();
            let update = update.clone();
            async move {
                let transaction = self.connection.begin().await.context(
                    "failed to begin thread episodic reconciliation failure transaction",
                )?;
                let outcome =
                    thread_episodic_repository::fail_index_attempt_without_source_validation(
                        &transaction,
                        job_id.as_str(),
                        expected_attempt_count,
                        update,
                        unix_to_datetime(now_unix),
                    )
                    .await?;
                transaction.commit().await.context(
                    "failed to commit thread episodic reconciliation failure transaction",
                )?;
                Ok(outcome)
            }
        })
        .await
    }

    pub async fn settle_thread_episodic_owned_index_attempt(
        &self,
        expected: &ThreadEpisodicIndexJobRecord,
        input: Option<&ThreadEpisodicIndexJobRecord>,
        update: ThreadEpisodicIndexJobFailureUpdate,
        now_unix: i64,
    ) -> Result<()> {
        use sea_orm::{ActiveModelTrait, IntoActiveModel, Set};
        self.retry_write(|| {
            let expected = expected.clone();
            let input = input.cloned();
            let update = update.clone();
            async move {
                let transaction = self.connection.begin().await?;
                let current =
                    thread_episodic_repository::lock_index_job(&transaction, &expected.id).await?;
                if current.as_ref().is_some_and(|current| {
                    current.status == "running"
                        && current.attempt_count == expected.attempt_count
                        && current.index_item_id == expected.index_item_id
                        && current.updated_at == expected.updated_at
                }) {
                    if !thread_episodic_repository::confirm_index_job_if_indexed(
                        &transaction,
                        current.as_ref().unwrap(),
                        unix_to_datetime(now_unix),
                    )
                    .await?
                    {
                        thread_episodic_repository::recover_index_attempt_after_persistence_error(
                            &transaction,
                            &expected.id,
                            expected.attempt_count,
                            update,
                            unix_to_datetime(now_unix),
                        )
                        .await?;
                    }
                } else if let (Some(input), Some(row)) = (input, current)
                    && matches!(row.status.as_str(), "queued" | "failed")
                    && thread_episodic_index_job_record_from_model(row.clone())? == input
                {
                    // Writer observation proves this exact input was not
                    // claimed. Defer only its scheduling; never spend budget or
                    // assume rollback from the error alone.
                    if let Some(next) = update.next_run_at_unix {
                        let mut active = row.into_active_model();
                        active.next_run_at = Set(unix_to_datetime(next));
                        if input.last_error.as_deref()
                            != Some(THREAD_EPISODIC_PROJECTION_CHANGED_ERROR)
                        {
                            active.last_error = Set(update.last_error);
                        }
                        active.updated_at = Set(unix_to_datetime(now_unix));
                        active.update(&transaction).await?;
                    }
                }
                transaction.commit().await?;
                Ok(())
            }
        })
        .await
    }

    pub async fn recover_thread_episodic_index_attempt_after_persistence_error(
        &self,
        job_id: &str,
        expected_attempt_count: i64,
        update: ThreadEpisodicIndexJobFailureUpdate,
        now_unix: i64,
    ) -> Result<ThreadEpisodicIndexAttemptOutcome> {
        self.retry_write(|| {
            let job_id = job_id.to_owned();
            let update = update.clone();
            async move {
                let transaction =
                    self.connection.begin().await.context(
                        "failed to begin thread episodic persistence recovery transaction",
                    )?;
                let outcome =
                    thread_episodic_repository::recover_index_attempt_after_persistence_error(
                        &transaction,
                        job_id.as_str(),
                        expected_attempt_count,
                        update,
                        unix_to_datetime(now_unix),
                    )
                    .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit thread episodic persistence recovery transaction")?;
                Ok(outcome)
            }
        })
        .await
    }

    pub async fn cancel_thread_episodic_index_job(
        &self,
        job_id: &str,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        self.retry_write(|| {
            let job_id = job_id.to_owned();
            let last_error = last_error.clone();
            async move {
                thread_episodic_repository::mark_index_job_canceled(
                    &self.connection,
                    job_id.as_str(),
                    last_error,
                    unix_to_datetime(now_unix),
                )
                .await?
                .map(thread_episodic_index_job_record_from_model)
                .transpose()
            }
        })
        .await
    }

    pub async fn claim_thread_episodic_index_job_from_snapshot(
        &self,
        snapshot: &ThreadEpisodicIndexJobRecord,
        now_unix: i64,
        max_attempts: i64,
        mutation_started: Option<&std::sync::atomic::AtomicBool>,
        accepting_claims: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        let now = unix_to_datetime(now_unix);
        let transaction = self.connection.begin().await?;
        let current = thread_episodic_repository::lock_index_job(&transaction, &snapshot.id)
            .await?
            .map(thread_episodic_index_job_record_from_model)
            .transpose()?;
        if current.as_ref() != Some(snapshot)
            || snapshot.next_run_at > now
            || !matches!(
                snapshot.status,
                ThreadEpisodicIndexJobStatus::Queued | ThreadEpisodicIndexJobStatus::Failed
            )
            || accepting_claims
                .is_some_and(|accepting| !accepting.load(std::sync::atomic::Ordering::Acquire))
        {
            transaction.rollback().await?;
            return Ok(None);
        }
        if let Some(started) = mutation_started {
            started.store(true, std::sync::atomic::Ordering::Release);
        }
        let claimed = thread_episodic_repository::mark_index_job_running(
            &transaction,
            &snapshot.id,
            now,
            max_attempts,
        )
        .await?;
        transaction.commit().await?;
        claimed
            .map(thread_episodic_index_job_record_from_model)
            .transpose()
    }
}
