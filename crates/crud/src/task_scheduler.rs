//! Scheduler reads inherit the scoped CrudStore reader handle. No writer or
//! transaction is entered for discovery, including empty fallback polls.
use crate::repositories::{task_run, task_trigger};
use crate::util::unix_to_datetime;
use crate::{CrudStore, task_run_from_db_model, task_trigger_from_db_model};
use anyhow::Result;
use pioneer_protocol::{TaskRun, TaskRunStatus, TaskTrigger};
use sea_orm::prelude::DateTimeWithTimeZone;

/// Raw page membership survives domain decoding. SQL/ORM fetch failures remain Err.
#[derive(Debug)]
pub struct TaskSchedulerPage<T, K> {
    pub records: Vec<T>,
    pub failures: Vec<TaskSchedulerRowFailure>,
    pub scanned: usize,
    pub last: Option<K>,
}
#[derive(Debug)]
pub struct TaskSchedulerRowFailure {
    pub id: String,
    pub task_id: String,
    pub error: anyhow::Error,
}
impl<T, K> TaskSchedulerPage<T, K> {
    fn complete(self) -> Result<Vec<T>> {
        if self.failures.is_empty() {
            Ok(self.records)
        } else {
            anyhow::bail!("scheduler discovery incomplete: {:?}", self.failures)
        }
    }
}
fn decode<M, T, K>(
    rows: Vec<M>,
    key: impl Fn(&M) -> K,
    locator: impl Fn(&M) -> (String, String),
    convert: impl Fn(M) -> Result<T>,
) -> TaskSchedulerPage<T, K> {
    let scanned = rows.len();
    let last = rows.last().map(key);
    let mut page = TaskSchedulerPage {
        records: Vec::new(),
        failures: Vec::new(),
        scanned,
        last,
    };
    for row in rows {
        let (id, task_id) = locator(&row);
        match convert(row) {
            Ok(record) => page.records.push(record),
            Err(error) => page
                .failures
                .push(TaskSchedulerRowFailure { id, task_id, error }),
        }
    }
    page
}
impl CrudStore {
    pub async fn list_due_active_task_triggers_page(
        &self,
        now: i64,
        after: Option<(i64, &str)>,
        limit: u64,
    ) -> Result<Vec<TaskTrigger>> {
        self.list_due_active_task_triggers_page_partial(now, after, limit)
            .await?
            .complete()
    }
    pub async fn list_due_active_task_triggers_page_partial(
        &self,
        now: i64,
        after: Option<(i64, &str)>,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskTrigger, (i64, String)>> {
        let rows = task_trigger::list_due_active_triggers_bounded(
            &self.connection,
            unix_to_datetime(now),
            after.map(|(at, id)| (unix_to_datetime(at), id)),
            limit,
        )
        .await?;
        Ok(decode(
            rows,
            |row| {
                (
                    row.next_fire_at
                        .as_ref()
                        .expect("due predicate")
                        .timestamp(),
                    row.id.clone(),
                )
            },
            |row| (row.id.clone(), row.task_id.clone()),
            task_trigger_from_db_model,
        ))
    }

    pub async fn list_dormant_active_task_triggers(
        &self,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<TaskTrigger>> {
        self.list_dormant_active_task_triggers_partial(after_id, limit)
            .await?
            .complete()
    }
    pub async fn list_dormant_active_task_triggers_partial(
        &self,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskTrigger, String>> {
        let rows =
            task_trigger::list_dormant_active_triggers(&self.connection, after_id, limit).await?;
        Ok(decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_trigger_from_db_model,
        ))
    }

    pub async fn list_task_recovery_runs(
        &self,
        status: TaskRunStatus,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<TaskRun>> {
        self.list_task_recovery_runs_partial(status, after_id, limit)
            .await?
            .complete()
    }
    pub async fn list_task_recovery_runs_partial(
        &self,
        status: TaskRunStatus,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        let rows = task_run::list_recovery_runs(&self.connection, status, after_id, limit).await?;
        Ok(decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_run_from_db_model,
        ))
    }

    pub async fn list_task_recovery_runs_for_task(
        &self,
        task_id: &str,
        status: TaskRunStatus,
        limit: u64,
    ) -> Result<Vec<TaskRun>> {
        self.list_task_recovery_runs_for_task_partial(task_id, status, limit)
            .await?
            .complete()
    }
    pub async fn list_task_recovery_runs_for_task_partial(
        &self,
        task_id: &str,
        status: TaskRunStatus,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        let rows =
            task_run::list_recovery_runs_for_task(&self.connection, task_id, status, limit).await?;
        Ok(decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_run_from_db_model,
        ))
    }

    pub async fn list_queued_task_schedule(&self, limit: u64) -> Result<Vec<TaskRun>> {
        self.list_queued_task_schedule_partial(limit)
            .await?
            .complete()
    }
    pub async fn list_queued_task_schedule_partial(
        &self,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        let rows = task_run::list_queued_schedule(&self.connection, limit).await?;
        Ok(decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_run_from_db_model,
        ))
    }

    pub async fn list_scheduler_due_task_runs(
        &self,
        now: i64,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<TaskRun>> {
        self.list_scheduler_due_task_runs_partial(now, after_id, limit)
            .await?
            .complete()
    }
    pub async fn list_scheduler_due_task_runs_partial(
        &self,
        now: i64,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        let rows = task_run::list_scheduler_due_runs(
            &self.connection,
            unix_to_datetime(now),
            after_id,
            limit,
        )
        .await?;
        Ok(decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_run_from_db_model,
        ))
    }

    /// Only active triggers are scheduling inputs; malformed inactive history is irrelevant.
    pub async fn task_scheduler_inputs(
        &self,
        task_id: &str,
    ) -> Result<(Vec<TaskTrigger>, Vec<TaskRun>)> {
        let (triggers, runs) = self.task_scheduler_inputs_partial(task_id).await?;
        Ok((triggers.complete()?, runs.complete()?))
    }
    pub async fn task_scheduler_inputs_partial(
        &self,
        task_id: &str,
    ) -> Result<(
        TaskSchedulerPage<TaskTrigger, String>,
        TaskSchedulerPage<TaskRun, String>,
    )> {
        let triggers =
            task_trigger::list_scheduler_active_triggers(&self.connection, task_id).await?;
        let runs = task_run::list_scheduler_queued_runs(&self.connection, task_id).await?;
        Ok((
            decode(
                triggers,
                |row| row.id.clone(),
                |row| (row.id.clone(), row.task_id.clone()),
                task_trigger_from_db_model,
            ),
            decode(
                runs,
                |row| row.id.clone(),
                |row| (row.id.clone(), row.task_id.clone()),
                task_run_from_db_model,
            ),
        ))
    }
    pub async fn list_active_task_triggers_partial(
        &self,
    ) -> Result<TaskSchedulerPage<TaskTrigger, String>> {
        let rows = task_trigger::list_active_triggers(&self.connection).await?;
        Ok(decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_trigger_from_db_model,
        ))
    }
    pub async fn task_scheduler_run_partial(
        &self,
        id: &str,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        let row = task_run::find_run_by_id(&self.connection, id).await?;
        Ok(decode(
            row.into_iter().collect(),
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_run_from_db_model,
        ))
    }
    pub async fn task_scheduler_trigger_partial(
        &self,
        id: &str,
    ) -> Result<TaskSchedulerPage<TaskTrigger, String>> {
        let row = task_trigger::find_trigger_by_id(&self.connection, id)
            .await?
            .filter(|row| row.status == "active");
        Ok(decode(
            row.into_iter().collect(),
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_trigger_from_db_model,
        ))
    }
    /// Keep the exact stored timestamp in finite keys; foreign/legacy rows may
    /// have subsecond deadlines even though protocol TaskTrigger uses seconds.
    pub async fn list_due_active_task_triggers_page_finite(
        &self,
        now: i64,
        after: Option<(DateTimeWithTimeZone, &str)>,
        upper: Option<(DateTimeWithTimeZone, &str)>,
        limit: u64,
    ) -> Result<(
        TaskSchedulerPage<TaskTrigger, (DateTimeWithTimeZone, String)>,
        Option<(DateTimeWithTimeZone, String)>,
    )> {
        let bound = match upper.map(|(at, id)| (at, id.to_owned())) {
            Some(bound) => Some(bound),
            None => {
                task_trigger::scheduler_due_upper(&self.connection, unix_to_datetime(now)).await?
            }
        };
        let Some(ref bound_key) = bound else {
            return Ok((
                TaskSchedulerPage {
                    records: Vec::new(),
                    failures: Vec::new(),
                    scanned: 0,
                    last: None,
                },
                None,
            ));
        };
        let bound = bound_key;
        let rows = task_trigger::list_due_active_triggers_bounded_through(
            &self.connection,
            unix_to_datetime(now),
            after,
            Some((bound.0, bound.1.as_str())),
            limit,
        )
        .await?;
        let page = decode(
            rows,
            |row| {
                (
                    row.next_fire_at.as_ref().expect("due predicate").to_owned(),
                    row.id.clone(),
                )
            },
            |row| (row.id.clone(), row.task_id.clone()),
            task_trigger_from_db_model,
        );
        Ok((page, Some(bound_key.clone())))
    }

    /// Finite advisory pass: capture a raw upper once, then constrain every page.
    pub async fn list_dormant_active_task_triggers_finite(
        &self,
        after: Option<&str>,
        upper: Option<&str>,
        limit: u64,
    ) -> Result<(TaskSchedulerPage<TaskTrigger, String>, Option<String>)> {
        let bound = match upper.map(str::to_owned) {
            Some(bound) => Some(bound),
            None => task_trigger::scheduler_dormant_upper(&self.connection).await?,
        };
        let Some(ref bound_key) = bound else {
            return Ok((
                TaskSchedulerPage {
                    records: Vec::new(),
                    failures: Vec::new(),
                    scanned: 0,
                    last: None,
                },
                None,
            ));
        };
        let bound = bound_key;
        let rows = task_trigger::list_dormant_active_triggers_through(
            &self.connection,
            after,
            Some(bound.as_str()),
            limit,
        )
        .await?;
        let page = decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_trigger_from_db_model,
        );
        Ok((page, Some(bound_key.clone())))
    }

    /// Finite advisory pass: capture a raw upper once, then constrain every page.
    pub async fn list_task_recovery_runs_finite(
        &self,
        status: TaskRunStatus,
        after: Option<&str>,
        upper: Option<&str>,
        limit: u64,
    ) -> Result<(TaskSchedulerPage<TaskRun, String>, Option<String>)> {
        let bound = match upper.map(str::to_owned) {
            Some(bound) => Some(bound),
            None => task_run::scheduler_recovery_upper(&self.connection, status).await?,
        };
        let Some(ref bound_key) = bound else {
            return Ok((
                TaskSchedulerPage {
                    records: Vec::new(),
                    failures: Vec::new(),
                    scanned: 0,
                    last: None,
                },
                None,
            ));
        };
        let bound = bound_key;
        let rows = task_run::list_recovery_runs_through(
            &self.connection,
            status,
            after,
            Some(bound.as_str()),
            limit,
        )
        .await?;
        let page = decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_run_from_db_model,
        );
        Ok((page, Some(bound_key.clone())))
    }

    /// Finite advisory pass: capture a raw upper once, then constrain every page.
    pub async fn list_scheduler_due_task_runs_finite(
        &self,
        now: i64,
        after: Option<&str>,
        upper: Option<&str>,
        limit: u64,
    ) -> Result<(TaskSchedulerPage<TaskRun, String>, Option<String>)> {
        let bound = match upper.map(str::to_owned) {
            Some(bound) => Some(bound),
            None => task_run::scheduler_due_upper(&self.connection, unix_to_datetime(now)).await?,
        };
        let Some(ref bound_key) = bound else {
            return Ok((
                TaskSchedulerPage {
                    records: Vec::new(),
                    failures: Vec::new(),
                    scanned: 0,
                    last: None,
                },
                None,
            ));
        };
        let bound = bound_key;
        let rows = task_run::list_scheduler_due_runs_through(
            &self.connection,
            unix_to_datetime(now),
            after,
            Some(bound.as_str()),
            limit,
        )
        .await?;
        let page = decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_run_from_db_model,
        );
        Ok((page, Some(bound_key.clone())))
    }
}
