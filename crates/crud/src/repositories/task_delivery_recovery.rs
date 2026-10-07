//! Current delivering work plus error-only recovery bookkeeping. Discovery
//! limits raw input before looking at retries; payloads are read by selected PK.
use anyhow::{Context, Result, bail};
use pioneer_entity::{task_delivery, task_delivery_attempt, task_delivery_recovery_retry as retry};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect, Set, Statement,
};

pub const DELIVERY_RECOVERY_BUDGET: u64 = 64;

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult)]
pub struct DeliveryRecoveryCursor {
    pub id: String,
    pub updated_at: DateTimeWithTimeZone,
}

// SQLite row-value seek uses the entire (status,updated_at,id) index range.
// There is deliberately no retry JOIN, residual exclusion or OFFSET.
pub(crate) fn source_statement(
    cutoff: i64,
    after: Option<&DeliveryRecoveryCursor>,
    limit: u64,
) -> Statement {
    let mut values = vec![crate::util::unix_to_datetime(cutoff).into()];
    let seek = if let Some(after) = after {
        values.push(after.updated_at.into());
        values.push(after.id.clone().into());
        " AND (updated_at,id) > (?,?)"
    } else {
        ""
    };
    values.push(limit.into());
    Statement::from_sql_and_values(
        sea_orm::DbBackend::Sqlite,
        format!(
            "SELECT id,updated_at FROM task_delivery INDEXED BY idx_task_delivery_recovery_source WHERE status='delivering' AND updated_at<=?{seek} ORDER BY updated_at,id LIMIT ?"
        ),
        values,
    )
}

pub async fn source_page<C: ConnectionTrait>(
    db: &C,
    cutoff: i64,
    after: Option<&DeliveryRecoveryCursor>,
    limit: u64,
) -> Result<Vec<DeliveryRecoveryCursor>> {
    if limit == 0 || limit > DELIVERY_RECOVERY_BUDGET {
        bail!("invalid delivery recovery input budget");
    }
    Ok(
        DeliveryRecoveryCursor::find_by_statement(source_statement(cutoff, after, limit))
            .all(db)
            .await?,
    )
}

pub async fn due_page<C: ConnectionTrait>(
    db: &C,
    now: i64,
    limit: u64,
) -> Result<Vec<retry::Model>> {
    if limit > DELIVERY_RECOVERY_BUDGET {
        bail!("invalid delivery recovery retry budget");
    }
    if limit == 0 {
        return Ok(Vec::new());
    }
    Ok(due_query(now, limit).all(db).await?)
}

fn due_query(now: i64, limit: u64) -> sea_orm::Select<retry::Entity> {
    retry::Entity::find()
        .filter(retry::Column::NextProbeAt.lte(now))
        .order_by_asc(retry::Column::NextProbeAt)
        .order_by_asc(retry::Column::DeliveryId)
        .limit(limit)
}

#[derive(Clone, Debug)]
pub struct DeliveryRecoverySnapshot {
    pub delivery: task_delivery::Model,
    pub attempt: Option<task_delivery_attempt::Model>,
    pub retry: Option<retry::Model>,
}

impl DeliveryRecoverySnapshot {
    pub fn eligible(&self, now: i64) -> bool {
        self.delivery.status == "delivering"
            && self.delivery.updated_at.timestamp() <= now.saturating_sub(300)
            && self.retry.as_ref().is_none_or(|r| r.next_probe_at <= now)
    }

    /// Decode/serialize only after reader resources have been released.
    pub fn failure_event(&self, now: i64) -> Result<pioneer_protocol::TaskEventPayload> {
        super::task_delivery::validate_durable_delivery(&self.delivery)?;
        let raw_attempt = self
            .attempt
            .clone()
            .context("stuck Task delivery has no exact attempt")?;
        super::task_delivery::validate_durable_attempt(&raw_attempt)?;
        let mut delivery = crate::task_delivery_from_db_model(self.delivery.clone())?;
        let mut attempt = crate::task_delivery_attempt_from_db_model(raw_attempt)?;
        let retryable = delivery.attempt_count < delivery.max_attempts;
        delivery.status = if retryable {
            pioneer_protocol::TaskDeliveryStatus::Pending
        } else {
            pioneer_protocol::TaskDeliveryStatus::Failed
        };
        delivery.next_attempt_at = retryable.then_some(now);
        delivery.last_error = Some("task_delivery_recovered".to_owned());
        delivery.updated_at = now;
        attempt.status = pioneer_protocol::TaskDeliveryAttemptStatus::Failed;
        attempt.completed_at = Some(now);
        attempt.error = delivery.last_error.clone();
        Ok(pioneer_protocol::TaskEventPayload::DeliveryFailed { delivery, attempt })
    }
}

pub async fn snapshot<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<DeliveryRecoverySnapshot>> {
    let Some(delivery) = super::task_delivery::find_delivery_by_id(db, id).await? else {
        return Ok(None);
    };
    // Raw i64, including malformed counts: never synthesize a missing attempt.
    let attempt = task_delivery_attempt::Entity::find()
        .filter(task_delivery_attempt::Column::DeliveryId.eq(id))
        .filter(task_delivery_attempt::Column::AttemptNumber.eq(delivery.attempt_count))
        .one(db)
        .await?;
    let retry = retry::Entity::find_by_id(id.to_owned()).one(db).await?;
    Ok(Some(DeliveryRecoverySnapshot {
        delivery,
        attempt,
        retry,
    }))
}

pub(crate) async fn matches<C: ConnectionTrait>(
    db: &C,
    expected: &DeliveryRecoverySnapshot,
) -> Result<bool> {
    let Some(current) = snapshot(db, &expected.delivery.id).await? else {
        return Ok(false);
    };
    // Full raw source facts, exact immutable attempt ID/verified absence, and
    // full observed retry snapshot. Same timestamps alone cannot authorize work.
    Ok(current.delivery == expected.delivery
        && current.attempt == expected.attempt
        && current.retry == expected.retry)
}

pub fn retry_delay(attempts: i64) -> i64 {
    (5_i64.saturating_mul(1_i64 << attempts.saturating_sub(1).clamp(0, 15))).min(300)
}

pub(crate) async fn defer<C: ConnectionTrait>(
    db: &C,
    expected: &DeliveryRecoverySnapshot,
    now: i64,
    token: &str,
) -> Result<bool> {
    if expected.delivery.status != "delivering" || !matches(db, expected).await? {
        return Ok(false);
    }
    let attempts = expected
        .retry
        .as_ref()
        .map_or(1, |r| r.attempts.saturating_add(1).min(16));
    let model = retry::ActiveModel {
        delivery_id: Set(expected.delivery.id.clone()),
        expected_attempt_id: Set(expected.attempt.as_ref().map(|a| a.id.clone())),
        expected_attempt_count: Set(expected.delivery.attempt_count),
        expected_updated_at: Set(expected.delivery.updated_at),
        retry_token: Set(token.to_owned()),
        next_probe_at: Set(now.saturating_add(retry_delay(attempts))),
        attempts: Set(attempts),
    };
    if expected.retry.is_some() {
        // Writer transaction already compared the full old snapshot.
        retry::Entity::update(model).exec(db).await?;
    } else {
        // Never overwrite a concurrent/new retry, including after delete/reinsert.
        retry::Entity::insert(model)
            .on_conflict(
                OnConflict::column(retry::Column::DeliveryId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(db)
            .await?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_query_seeks_raw_delivering_inputs_before_retry_filtering() {
        let after = DeliveryRecoveryCursor {
            id: "tie".to_owned(),
            updated_at: crate::util::unix_to_datetime(100),
        };
        let sql = source_statement(200, Some(&after), 32).sql;
        assert!(sql.contains("INDEXED BY idx_task_delivery_recovery_source"));
        assert!(sql.contains("status='delivering' AND updated_at<=? AND (updated_at,id) > (?,?)"));
        assert!(sql.ends_with("ORDER BY updated_at,id LIMIT ?"));
        for forbidden in ["JOIN", "EXISTS", "OFFSET", "retry", "snapshot_json"] {
            assert!(
                !sql.contains(forbidden),
                "unbounded/residual discovery: {sql}"
            );
        }
    }

    #[test]
    fn error_backoff_saturates_without_overflow() {
        assert_eq!(
            (1..=8).map(retry_delay).collect::<Vec<_>>(),
            [5, 10, 20, 40, 80, 160, 300, 300]
        );
        assert_eq!(retry_delay(i64::MAX), 300);
    }
    // Prepared for post-review execution. These are actual production
    // statements on the actual migrated schema, including both keyset forms.
    #[tokio::test]
    async fn recovery_plans_seek_source_and_due_indexes_without_sorting_history() {
        use migration::{Migrator, MigratorTrait};
        use sea_orm::{Database, QueryTrait};
        let connection = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&connection, None).await.unwrap();
        let store = crate::CrudStore::new(connection).with_maintenance_access();
        let db = store.database_connection();
        let after = DeliveryRecoveryCursor {
            id: "after".to_owned(),
            updated_at: crate::util::unix_to_datetime(100),
        };
        for (statement, index) in [
            (
                source_statement(200, None, 64),
                "idx_task_delivery_recovery_source",
            ),
            (
                source_statement(200, Some(&after), 32),
                "idx_task_delivery_recovery_source",
            ),
            (
                due_query(200, 32).build(sea_orm::DbBackend::Sqlite),
                "idx_task_delivery_recovery_retry_due",
            ),
        ] {
            let plan = db
                .query_all_raw(Statement::from_sql_and_values(
                    sea_orm::DbBackend::Sqlite,
                    format!("EXPLAIN QUERY PLAN {}", statement.sql),
                    statement.values.unwrap(),
                ))
                .await
                .unwrap()
                .into_iter()
                .map(|r| r.try_get::<String>("", "detail").unwrap())
                .collect::<Vec<_>>();
            assert!(
                plan.iter()
                    .any(|d| d.starts_with("SEARCH ") && d.contains(index)),
                "{plan:?}"
            );
            assert!(
                !plan
                    .iter()
                    .any(|d| d.contains("SCAN ") || d.contains("TEMP B-TREE")),
                "{plan:?}"
            );
        }
    }
}
