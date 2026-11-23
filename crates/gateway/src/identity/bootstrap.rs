use anyhow::{Context, Result, bail};
use pioneer_crud::{
    GatewayIdentityRecord, GatewayPrincipalRecord, create_superuser, ensure_gateway_singleton,
    load_identity_invariant_rows, load_superusers_for_gateway, set_identity_bootstrap_version,
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::{
    GATEWAY_ID_LEN, GatewayId, PRINCIPAL_ID_LEN, PrincipalId, PrincipalKind, PrincipalStatus,
    generate_id,
};
use sea_orm::{ConnectionTrait, TransactionSession, TransactionTrait};

use super::invariants::validate_identity_invariants;

pub(crate) const SUPPORTED_IDENTITY_BOOTSTRAP_VERSION: i64 = 1;
const DEFAULT_SUPERUSER_DISPLAY_NAME: &str = "Superuser";
const DEFAULT_SUPERUSER_NICKNAME: &str = "superuser";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GatewayIdentitySnapshot {
    pub id: GatewayId,
    pub identity_bootstrap_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SuperuserIdentitySnapshot {
    pub id: PrincipalId,
    pub gateway_id: GatewayId,
    pub kind: PrincipalKind,
    pub role_key: Option<String>,
    pub status: PrincipalStatus,
    pub display_name: String,
    pub nickname: String,
    pub nickname_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentityBootstrapSnapshot {
    pub gateway: GatewayIdentitySnapshot,
    pub superuser: SuperuserIdentitySnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentityBootstrapOutcome {
    pub snapshot: IdentityBootstrapSnapshot,
    pub gateway_created: bool,
    pub superuser_created: bool,
}

struct IdentityBootstrapTransaction<T> {
    transaction: T,
    snapshot: IdentityBootstrapSnapshot,
    gateway_created: bool,
    superuser_created: bool,
}

impl<T> IdentityBootstrapTransaction<T>
where
    T: ConnectionTrait + TransactionSession,
{
    fn created_flags(&self) -> (bool, bool) {
        (self.gateway_created, self.superuser_created)
    }

    async fn validate_and_mark(&mut self) -> Result<()> {
        let rows = load_identity_invariant_rows(&self.transaction).await?;
        validate_identity_invariants(&rows, SUPPORTED_IDENTITY_BOOTSTRAP_VERSION)?;
        if self.snapshot.gateway.identity_bootstrap_version == SUPPORTED_IDENTITY_BOOTSTRAP_VERSION
        {
            return Ok(());
        }
        let gateway = set_identity_bootstrap_version(
            &self.transaction,
            &self.snapshot.gateway.id,
            SUPPORTED_IDENTITY_BOOTSTRAP_VERSION,
            chrono::Utc::now().fixed_offset(),
        )
        .await?;
        self.snapshot.gateway = gateway_snapshot(gateway);
        Ok(())
    }

    async fn commit(self) -> Result<IdentityBootstrapSnapshot> {
        self.transaction
            .commit()
            .await
            .context("failed to commit identity bootstrap transaction")?;
        Ok(self.snapshot)
    }

    async fn rollback(self) -> Result<()> {
        self.transaction
            .rollback()
            .await
            .context("failed to roll back identity bootstrap transaction")
    }
}

pub(crate) async fn bootstrap_identity(
    connection: impl Into<PostgresDatabase>,
) -> Result<IdentityBootstrapOutcome> {
    let connection = connection.into().maintenance();
    let mut bootstrap = begin_identity_bootstrap(&connection).await?;
    let (gateway_created, superuser_created) = bootstrap.created_flags();
    match bootstrap.validate_and_mark().await {
        Ok(()) => {
            let snapshot = bootstrap.commit().await?;
            Ok(IdentityBootstrapOutcome {
                snapshot,
                gateway_created,
                superuser_created,
            })
        }
        Err(error) => {
            bootstrap.rollback().await?;
            Err(error)
        }
    }
}

async fn begin_identity_bootstrap<D>(
    connection: &D,
) -> Result<IdentityBootstrapTransaction<D::Transaction>>
where
    D: TransactionTrait,
{
    let transaction = connection
        .begin()
        .await
        .context("failed to begin identity bootstrap transaction")?;

    match load_or_create_identity(&transaction).await {
        Ok((snapshot, gateway_created, superuser_created)) => Ok(IdentityBootstrapTransaction {
            transaction,
            snapshot,
            gateway_created,
            superuser_created,
        }),
        Err(error) => {
            transaction
                .rollback()
                .await
                .context("failed to roll back incomplete identity bootstrap transaction")?;
            Err(error)
        }
    }
}

async fn load_or_create_identity<C>(
    transaction: &C,
) -> Result<(IdentityBootstrapSnapshot, bool, bool)>
where
    C: ConnectionTrait,
{
    let id = GatewayId::new(generate_id(GATEWAY_ID_LEN))
        .context("generated invalid Gateway identity id")?;
    let (gateway, gateway_created) =
        ensure_gateway_singleton(transaction, &id, chrono::Utc::now().fixed_offset()).await?;

    let superusers = load_superusers_for_gateway(transaction, &gateway.id).await?;
    let (superuser, superuser_created) = match superusers.as_slice() {
        [] => {
            let id = PrincipalId::new(generate_id(PRINCIPAL_ID_LEN))
                .context("generated invalid Superuser principal id")?;
            (
                create_superuser(
                    transaction,
                    &id,
                    &gateway.id,
                    DEFAULT_SUPERUSER_DISPLAY_NAME,
                    DEFAULT_SUPERUSER_NICKNAME,
                    DEFAULT_SUPERUSER_NICKNAME,
                    chrono::Utc::now().fixed_offset(),
                )
                .await?,
                true,
            )
        }
        [existing] => (existing.clone(), false),
        _ => bail!("identity invariant violation: multiple_superusers"),
    };

    Ok((
        IdentityBootstrapSnapshot {
            gateway: gateway_snapshot(gateway),
            superuser: superuser_snapshot(superuser),
        },
        gateway_created,
        superuser_created,
    ))
}

fn gateway_snapshot(record: GatewayIdentityRecord) -> GatewayIdentitySnapshot {
    GatewayIdentitySnapshot {
        id: record.id,
        identity_bootstrap_version: record.identity_bootstrap_version,
    }
}

fn superuser_snapshot(record: GatewayPrincipalRecord) -> SuperuserIdentitySnapshot {
    SuperuserIdentitySnapshot {
        id: record.id,
        gateway_id: record.gateway_id,
        kind: record.kind,
        role_key: record.role_key,
        status: record.status,
        display_name: record.display_name,
        nickname: record.nickname,
        nickname_key: record.nickname_key,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SUPPORTED_IDENTITY_BOOTSTRAP_VERSION, begin_identity_bootstrap, bootstrap_identity,
    };
    use pioneer_crud::{
        list_gateway_identities, list_gateway_principals, load_identity_invariant_rows,
    };
    use pioneer_postgres_test::with_database;
    use pioneer_protocol::{PrincipalKind, PrincipalStatus};
    use sea_orm::{ConnectionTrait, DatabaseConnection};
    use std::sync::atomic::{AtomicBool, Ordering};

    async fn bootstrap_before_listener(
        database: &DatabaseConnection,
        listener_opened: &AtomicBool,
    ) -> anyhow::Result<()> {
        bootstrap_identity(database).await?;
        listener_opened.store(true, Ordering::SeqCst);
        Ok(())
    }

    #[tokio::test]
    async fn first_run_creates_one_gateway_and_stable_superuser() {
        with_database(async |fixture| {
            let database = fixture.connection().clone();

            let bootstrap = begin_identity_bootstrap(&database)
                .await
                .expect("begin identity bootstrap");
            assert_eq!(bootstrap.created_flags(), (true, true));
            let snapshot = bootstrap.commit().await.expect("commit bootstrap");

            assert_eq!(snapshot.gateway.identity_bootstrap_version, 0);
            assert_eq!(snapshot.superuser.gateway_id, snapshot.gateway.id);
            assert_eq!(snapshot.superuser.kind, PrincipalKind::Superuser);
            assert_eq!(snapshot.superuser.status, PrincipalStatus::Active);
            assert_eq!(snapshot.superuser.role_key, None);
            assert_eq!(snapshot.superuser.display_name, "Superuser");
            assert_eq!(snapshot.superuser.nickname, "superuser");
            assert_eq!(snapshot.superuser.nickname_key, "superuser");
            assert_eq!(list_gateway_identities(&database).await.unwrap().len(), 1);
            assert_eq!(list_gateway_principals(&database).await.unwrap().len(), 1);
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn restart_preserves_a_valid_edited_superuser_profile() {
        with_database(async |fixture| {
            let database = fixture.connection().clone();
            let first = bootstrap_identity(&database)
                .await
                .expect("bootstrap initial identity");
            database
                .execute_unprepared(
                    "UPDATE gateway_principal \
                 SET display_name = 'Gateway administrator', \
                     nickname = 'gateway-admin', \
                     nickname_key = 'gateway-admin' \
                 WHERE kind = 'superuser';",
                )
                .await
                .expect("edit Superuser profile");

            let restarted = bootstrap_identity(&database)
                .await
                .expect("restart should accept the edited profile");

            assert_eq!(restarted.snapshot.gateway.id, first.snapshot.gateway.id);
            assert_eq!(restarted.snapshot.superuser.id, first.snapshot.superuser.id);
            assert_eq!(
                restarted.snapshot.superuser.display_name,
                "Gateway administrator"
            );
            assert_eq!(restarted.snapshot.superuser.nickname, "gateway-admin");
            assert_eq!(restarted.snapshot.superuser.nickname_key, "gateway-admin");
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn validated_bootstrap_sets_marker_and_restart_preserves_identity() {
        with_database(async |fixture| {
            let database = fixture.connection().clone();
            let first = bootstrap_identity(&database)
                .await
                .expect("validated first bootstrap");
            assert!(first.gateway_created);
            assert!(first.superuser_created);
            assert_eq!(
                first.snapshot.gateway.identity_bootstrap_version,
                SUPPORTED_IDENTITY_BOOTSTRAP_VERSION
            );

            let second = bootstrap_identity(&database)
                .await
                .expect("validated restart bootstrap");
            assert!(!second.gateway_created);
            assert!(!second.superuser_created);
            assert_eq!(second.snapshot, first.snapshot);
            assert_eq!(list_gateway_identities(&database).await.unwrap().len(), 1);
            assert_eq!(list_gateway_principals(&database).await.unwrap().len(), 1);
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn scanner_failure_rolls_back_identity_and_marker_together() {
        with_database(async |fixture| {
            let database = fixture.connection().clone();
            database.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('identity-test-worksp', 'Identity test');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status, origin_kind)
             VALUES ('thread-unknown', 'identity-test-worksp', '', 'agent', 'm', 'p', 'idle', 'user')",
        ).await.unwrap();
            let listener_opened = AtomicBool::new(false);

            let error = match bootstrap_before_listener(&database, &listener_opened).await {
                Ok(_) => panic!("unknown origins must leave missing actors and fail"),
                Err(error) => error,
            };
            assert!(!listener_opened.load(Ordering::SeqCst));
            assert!(error.to_string().contains("missing_actor"));
            assert!(list_gateway_identities(&database).await.unwrap().is_empty());
            assert!(list_gateway_principals(&database).await.unwrap().is_empty());
            let rows = load_identity_invariant_rows(&database).await.unwrap();
            assert!(
                rows.actor_references
                    .iter()
                    .all(|row| row.actor_kind.is_none() && row.actor_id.is_none())
            );
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn concurrent_bootstraps_create_one_stable_identity() {
        with_database(async |fixture| {
            let pool = fixture.connect().await?;
            let peer = fixture.connect().await?;
            let database = pioneer_postgres_pools::PostgresDatabase::from_single_connection(pool);
            let second_database =
                pioneer_postgres_pools::PostgresDatabase::from_single_connection(peer);
            let (first, second) = tokio::join!(
                bootstrap_identity(&database),
                bootstrap_identity(&second_database)
            );
            let first = first.expect("first concurrent bootstrap");
            let second = second.expect("second concurrent bootstrap");

            assert_eq!(first.snapshot, second.snapshot);
            assert_eq!(
                usize::from(first.gateway_created) + usize::from(second.gateway_created),
                1
            );
            assert_eq!(
                usize::from(first.superuser_created) + usize::from(second.superuser_created),
                1
            );
            assert_eq!(list_gateway_identities(&database).await.unwrap().len(), 1);
            assert_eq!(list_gateway_principals(&database).await.unwrap().len(), 1);
            database.close().await?;
            second_database.close().await?;
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn representative_database_corruption_blocks_listener_startup() {
        for (corruption, expected_code) in [
            ("duplicate_superuser", "multiple_superusers"),
            ("partial_actor", "invalid_actor_pair"),
            ("dangling_actor", "dangling_principal_actor"),
            ("unsupported_marker", "unsupported_bootstrap_version"),
        ] {
            with_database(async |fixture| {
            let database = fixture.connection().clone();
            let valid = bootstrap_identity(&database)
                .await
                .expect("establish valid identity baseline");

            match corruption {
                "duplicate_superuser" => {
                    let duplicate_superuser_sql = format!(
                        "DROP INDEX idx_gateway_principal_one_superuser; \
                         INSERT INTO gateway_principal \
                            (id, gateway_id, kind, role_key, status, display_name, \
                             nickname, nickname_key, created_at, updated_at) \
                         VALUES \
                            ('P99999999999999999999', '{}', 'superuser', NULL, 'active', \
                             'Superuser', 'other-superuser', 'other-superuser', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);",
                        valid.snapshot.gateway.id
                    );
                    database
                        .execute_unprepared(duplicate_superuser_sql.as_str())
                        .await
                        .expect("inject duplicate Superuser");
                }
                "partial_actor" => {
                    database
                        .execute_unprepared(
                            "ALTER TABLE thread DROP CONSTRAINT thread_check; \
                             ALTER TABLE thread DISABLE TRIGGER conversation_actor_thread_created_by_actor_kind_pair_insert; \
                             INSERT INTO workspace (id, name, is_active, is_current) \
                             VALUES ('corrupt-workspace-001', 'Corrupt fixture', true, false); \
                             INSERT INTO thread \
                                (id, workspace_id, preview, mode, model, model_provider, status, \
                                 origin_kind, created_by_actor_kind, created_by_actor_id) \
                             VALUES \
                                ('corrupt-thread-part1', 'corrupt-workspace-001', '', 'chat', \
                                 'model', 'provider', 'idle', 'user', 'principal', NULL); \
                             ALTER TABLE thread ENABLE TRIGGER conversation_actor_thread_created_by_actor_kind_pair_insert;",
                        )
                        .await
                        .expect("inject partial actor pair");
                }
                "dangling_actor" => {
                    database
                        .execute_unprepared(
                            "INSERT INTO workspace (id, name, is_active, is_current) \
                             VALUES ('corrupt-workspace-001', 'Corrupt fixture', true, false); \
                             INSERT INTO thread \
                                (id, workspace_id, preview, mode, model, model_provider, status, \
                                 origin_kind, created_by_actor_kind, created_by_actor_id) \
                             VALUES \
                                ('corrupt-thread-dang1', 'corrupt-workspace-001', '', 'chat', \
                                 'model', 'provider', 'idle', 'user', 'principal', \
                                 'P99999999999999999999');",
                        )
                        .await
                        .expect("inject dangling actor");
                }
                "unsupported_marker" => {
                    database
                        .execute_unprepared(
                            "UPDATE gateway_identity SET identity_bootstrap_version = 2;",
                        )
                        .await
                        .expect("inject unsupported marker");
                }
                _ => unreachable!("complete corruption drill matrix"),
            }

            let listener_opened = AtomicBool::new(false);
            let error = bootstrap_before_listener(&database, &listener_opened)
                .await
                .expect_err("corrupt database must fail before listener startup");
            assert!(
                !listener_opened.load(Ordering::SeqCst),
                "{corruption} opened the listener"
            );
            assert!(
                error.to_string().contains(expected_code),
                "{corruption} returned an unsafe or imprecise diagnostic: {error:#}"
            );
            Ok(())
            }).await.unwrap();
        }
    }
}
