use anyhow::{Context, Result};
use pioneer_crud_access::identity::{ensure_gateway_singleton, load_gateway_singleton};
use pioneer_postgres_test::with_database;
use pioneer_protocol::GatewayId;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use std::time::Duration;

#[tokio::test]
async fn simultaneous_singleton_creation_follows_the_first_transactions_outcome() -> Result<()> {
    for commit_first in [true, false] {
        with_database(async |fixture| {
            let observer = fixture.connection();
            let first = observer.begin().await?;
            let second = observer.begin().await?;
            let first_id = GatewayId::new("G00000000000000000001")?;
            let second_id = GatewayId::new("G00000000000000000002")?;
            let now = chrono::Utc::now().fixed_offset();
            assert!(ensure_gateway_singleton(&first, &first_id, now).await?.1);

            let pid: i32 = second
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT pg_backend_pid() AS pid",
                ))
                .await?
                .context("second transaction has no backend PID")?
                .try_get("", "pid")?;
            let retry_id = second_id.clone();
            let retry = tokio::spawn(async move {
                let result = ensure_gateway_singleton(&second, &retry_id, now).await?;
                second.commit().await?;
                Ok::<_, anyhow::Error>(result)
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let blocked = observer
                        .query_one_raw(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked",
                            [pid.into()],
                        ))
                        .await?
                        .context("missing lock observation")?
                        .try_get("", "blocked")?;
                    if blocked {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("second bootstrap did not wait on the first transaction")??;
            let expected = if commit_first {
                first.commit().await?;
                first_id
            } else {
                first.rollback().await?;
                second_id
            };
            let (gateway, inserted) = retry.await??;
            assert_eq!(inserted, !commit_first);
            assert_eq!(gateway.id, expected);
            assert_eq!(
                load_gateway_singleton(observer)
                    .await?
                    .context("committed singleton disappeared")?
                    .id,
                expected,
            );
            Ok(())
        })
        .await?;
    }
    Ok(())
}
