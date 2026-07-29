use anyhow::{Context, Result, bail};
use pioneer_crud::{
    expire_stale_auth_sessions, load_gateway_singleton, load_principal_by_id,
    mark_gateway_auth_ready, scan_auth_persistence_invariants,
};
use pioneer_protocol::{PrincipalKind, PrincipalStatus};
use sea_orm::{DatabaseConnection, SqliteTransactionMode, TransactionOptions, TransactionTrait};

use crate::identity::IdentityBootstrapSnapshot;

use super::AUTH_SCHEMA_VERSION;

pub(crate) async fn ensure_auth_readiness(
    database: &DatabaseConnection,
    identity: &IdentityBootstrapSnapshot,
) -> Result<()> {
    let transaction = database
        .begin_with_options(TransactionOptions {
            sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
            ..Default::default()
        })
        .await
        .context("failed to begin Gateway auth readiness transaction")?;
    let result = async {
        let gateway = load_gateway_singleton(&transaction)
            .await?
            .context("Gateway identity is missing during auth readiness")?;
        if gateway.id != identity.gateway.id {
            bail!("Gateway identity changed during auth readiness");
        }
        let principal = load_principal_by_id(&transaction, &identity.superuser.id)
            .await?
            .context("Superuser is missing during auth readiness")?;
        if principal.gateway_id != gateway.id
            || principal.kind != PrincipalKind::Superuser
            || principal.status != PrincipalStatus::Active
        {
            bail!("stable Superuser is invalid during auth readiness");
        }
        let now = chrono::Utc::now().fixed_offset();
        let expired_sessions = expire_stale_auth_sessions(&transaction, now, 256).await?;
        let report = scan_auth_persistence_invariants(&transaction).await?;
        if !report.is_valid() {
            bail!(
                "Gateway auth persistence invariants failed: {}",
                report.violations.join("; ")
            );
        }
        mark_gateway_auth_ready(&transaction, &gateway.id, AUTH_SCHEMA_VERSION, now).await?;
        Ok(expired_sessions)
    }
    .await;
    match result {
        Ok(expired_sessions) => {
            transaction
                .commit()
                .await
                .context("failed to commit Gateway auth readiness")?;
            if !expired_sessions.is_empty() {
                tracing::info!(
                    expired_sessions = expired_sessions.len(),
                    "expired auth sessions reconciled during startup"
                );
            }
            Ok(())
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::load_gateway_singleton;
    use sea_orm::{ConnectionTrait, Database};

    use super::*;

    #[tokio::test]
    async fn readiness_is_idempotent() {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&database, None).await.unwrap();
        crate::bootstrap::bootstrap(&database).await.unwrap();
        let identity = crate::identity::bootstrap_identity(&database)
            .await
            .unwrap()
            .snapshot;
        ensure_auth_readiness(&database, &identity).await.unwrap();
        let first = load_gateway_singleton(&database).await.unwrap().unwrap();
        ensure_auth_readiness(&database, &identity).await.unwrap();
        let second = load_gateway_singleton(&database).await.unwrap().unwrap();
        assert_eq!(first.auth_schema_version, AUTH_SCHEMA_VERSION);
        assert!(first.auth_ready_at.is_some());
        assert_eq!(first.auth_ready_at, second.auth_ready_at);
    }

    #[tokio::test]
    async fn readiness_expires_stale_sessions_and_their_current_refresh() {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&database, None).await.unwrap();
        crate::bootstrap::bootstrap(&database).await.unwrap();
        let identity = crate::identity::bootstrap_identity(&database)
            .await
            .unwrap()
            .snapshot;
        database
            .execute_unprepared(
                format!(
                    "INSERT INTO device(id,gateway_id,principal_id,installation_id,display_name,client_kind,status,created_at,updated_at,last_seen_at) VALUES('D00000000000000000001','{}','{}','desktop','Desktop','desktop','active',datetime('now','-2 days'),datetime('now','-2 days'),datetime('now','-2 days')); \
                     INSERT INTO auth_session(id,gateway_id,principal_id,device_id,token_family_id,activation_token_hash,activation_locator_hash,activation_failed_attempts,activation_expires_at,activated_at,status,refresh_generation,created_at,updated_at,last_seen_at,last_refreshed_at,refresh_expires_at) VALUES('S00000000000000000001','{}','{}','D00000000000000000001','F00000000000000000001',randomblob(32),randomblob(32),0,datetime('now','-2 days'),datetime('now','-2 days'),'active',0,datetime('now','-2 days'),datetime('now','-2 days'),datetime('now','-2 days'),datetime('now','-2 days'),datetime('now','-1 day')); \
                     INSERT INTO auth_refresh_credential(id,session_id,token_family_id,generation,token_hash,status,issued_at,expires_at) VALUES('R00000000000000000001','S00000000000000000001','F00000000000000000001',0,zeroblob(32),'current',datetime('now','-2 days'),datetime('now','-1 day'))",
                    identity.gateway.id,
                    identity.superuser.id,
                    identity.gateway.id,
                    identity.superuser.id,
                )
                .as_str(),
            )
            .await
            .unwrap();

        ensure_auth_readiness(&database, &identity).await.unwrap();
        let session = database
            .query_one_raw(sea_orm::Statement::from_string(
                database.get_database_backend(),
                "SELECT status FROM auth_session".to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        let refresh = database
            .query_one_raw(sea_orm::Statement::from_string(
                database.get_database_backend(),
                "SELECT status FROM auth_refresh_credential".to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.try_get::<String>("", "status").unwrap(), "expired");
        assert_eq!(refresh.try_get::<String>("", "status").unwrap(), "expired");
    }

    #[tokio::test]
    async fn corrupted_auth_ownership_fails_before_readiness_marker() {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&database, None).await.unwrap();
        crate::bootstrap::bootstrap(&database).await.unwrap();
        let identity = crate::identity::bootstrap_identity(&database)
            .await
            .unwrap()
            .snapshot;
        database
            .execute_unprepared(
                format!(
                    "INSERT INTO device(id, gateway_id, principal_id, installation_id, display_name, client_kind, platform, client_version, status, created_at, updated_at, last_seen_at, revoked_at) VALUES ('D00000000000000000001', '{}', '{}', 'desktop', 'Desktop', 'desktop', NULL, NULL, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)",
                    identity.gateway.id, identity.superuser.id,
                )
                .as_str(),
            )
            .await
            .unwrap();
        database
            .execute_unprepared("PRAGMA foreign_keys = OFF")
            .await
            .unwrap();
        database
            .execute_unprepared("UPDATE device SET principal_id = 'P00000000000000000099'")
            .await
            .unwrap();
        assert!(ensure_auth_readiness(&database, &identity).await.is_err());
        let gateway = load_gateway_singleton(&database).await.unwrap().unwrap();
        assert_eq!(gateway.auth_schema_version, 0);
        assert!(gateway.auth_ready_at.is_none());
    }
}
