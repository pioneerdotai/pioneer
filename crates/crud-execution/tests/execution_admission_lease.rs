use anyhow::{Context, Result};
use pioneer_crud_execution::execution_admission_lease::{
    EXECUTION_LEASE_STATUS_ACTIVE, EXECUTION_LEASE_STATUS_RELEASED, ExecutionAdmissionClass,
    ExecutionAdmissionQuotaPolicy, ExecutionQuotaCeilings, NewExecutionAdmissionLease,
    find_by_subject, list_active, reacquire, release_by_subject, reserve,
};
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use std::time::Duration;

fn lease(id: &str, operation_class: ExecutionAdmissionClass) -> NewExecutionAdmissionLease {
    let ceilings = ExecutionQuotaCeilings {
        per_principal: 1,
        per_role: 1,
        per_workspace: 1,
        gateway: 1,
    };
    NewExecutionAdmissionLease {
        id: format!("lease-{id}"),
        subject_kind: "task".to_owned(),
        subject_id: id.to_owned(),
        operation_class,
        principal_id: format!("principal-{id}"),
        role_key: format!("role-{id}"),
        workspace_id: format!("workspace-{id}"),
        policy_fingerprint: "test-policy".to_owned(),
        policy: ExecutionAdmissionQuotaPolicy {
            active: ceilings,
            queued: ceilings,
            scheduled: ceilings,
        },
    }
}

#[tokio::test]
async fn gateway_quota_serializes_reservations_only_in_the_same_bucket() -> Result<()> {
    for commit_first in [true, false] {
        with_database(async |fixture| {
            let db = fixture.connection();
            let at = chrono::Utc::now().fixed_offset();
            let first = db.begin().await?;
            let first_lease = lease("first", ExecutionAdmissionClass::QueuedTask);
            reserve(&first, first_lease, at).await?;

            let other = fixture.connect().await?;
            let second = other.begin().await?;
            let second_pid = second
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT pg_backend_pid() AS pid",
                ))
                .await?
                .context("admission backend pid")?
                .try_get::<i32>("", "pid")?;
            let waiter = tokio::spawn(async move {
                let outcome = reserve(
                    &second,
                    lease("second", ExecutionAdmissionClass::QueuedTask),
                    at,
                )
                .await;
                if outcome.is_ok() {
                    second.commit().await?;
                } else {
                    second.rollback().await?;
                }
                outcome
            });

            let blocked = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let held = db
                        .query_one_raw(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT cardinality(pg_blocking_pids($1)) > 0 AS held",
                            [second_pid.into()],
                        ))
                        .await?
                        .context("admission lock state")?
                        .try_get::<bool>("", "held")?;
                    if held {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await;

            let scheduled_result = tokio::time::timeout(Duration::from_secs(5), async {
                let tx = db.begin().await?;
                let result = reserve(
                    &tx,
                    lease("scheduled", ExecutionAdmissionClass::ScheduledTask),
                    at,
                )
                .await?;
                tx.commit().await?;
                Ok::<_, anyhow::Error>(result)
            })
            .await;
            if commit_first {
                first.commit().await?;
            } else {
                first.rollback().await?;
            }
            let outcome = waiter.await?;
            blocked.context("a reservation for the last same-bucket slot must wait")??;
            assert_eq!(
                scheduled_result
                    .context("other bucket must progress")??
                    .quota_bucket,
                "scheduled"
            );

            let accepted_id = if commit_first {
                let error =
                    outcome.expect_err("committed first reservation consumes the last slot");
                assert!(
                    error
                        .to_string()
                        .contains("Gateway queued execution quota is exhausted")
                );
                "first"
            } else {
                assert_eq!(outcome?.subject_id, "second");
                "second"
            };
            let active = list_active(db).await?;
            assert_eq!(active.len(), 2);
            assert!(
                active
                    .iter()
                    .any(|row| row.subject_id == accepted_id && row.quota_bucket == "queued")
            );

            assert!(release_by_subject(db, "task", accepted_id, at).await?);
            assert!(!release_by_subject(db, "task", accepted_id, at).await?);
            let resumed_lease = lease(accepted_id, ExecutionAdmissionClass::QueuedTask);
            let tx = db.begin().await?;
            assert_eq!(
                reserve(&tx, resumed_lease.clone(), at).await?.status,
                EXECUTION_LEASE_STATUS_RELEASED
            );
            assert_eq!(
                reacquire(&tx, resumed_lease, at).await?.status,
                EXECUTION_LEASE_STATUS_ACTIVE
            );
            tx.rollback().await?;
            assert_eq!(
                find_by_subject(db, "task", accepted_id)
                    .await?
                    .unwrap()
                    .status,
                EXECUTION_LEASE_STATUS_RELEASED
            );
            other.close().await?;
            Ok(())
        })
        .await?;
    }
    Ok(())
}
