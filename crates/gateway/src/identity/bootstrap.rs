use anyhow::{Context, Result, bail};
use pioneer_crud::{
    GatewayIdentityRecord, GatewayPrincipalRecord, LegacyActorBackfillCounts,
    backfill_legacy_actor_references, create_gateway_singleton, create_superuser,
    load_gateway_singleton, load_identity_invariant_rows, load_superusers_for_gateway,
    set_identity_bootstrap_version,
};
use pioneer_protocol::{
    GATEWAY_ID_LEN, GatewayId, PRINCIPAL_ID_LEN, PrincipalId, PrincipalKind, PrincipalStatus,
    generate_id,
};
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, SqliteDatabase,
    is_anyhow_sqlite_lock, retry_with_backoff,
};
use sea_orm::{
    ConnectionTrait, SqliteTransactionMode, TransactionOptions, TransactionSession,
    TransactionTrait,
};
use std::time::Duration;

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
    pub backfill_counts: LegacyActorBackfillCounts,
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
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> &IdentityBootstrapSnapshot {
        &self.snapshot
    }

    fn created_flags(&self) -> (bool, bool) {
        (self.gateway_created, self.superuser_created)
    }

    async fn backfill_legacy_actors(&self) -> Result<LegacyActorBackfillCounts> {
        backfill_legacy_actor_references(&self.transaction, &self.snapshot.superuser.id).await
    }

    async fn validate_and_mark(&mut self) -> Result<()> {
        let rows = load_identity_invariant_rows(&self.transaction).await?;
        validate_identity_invariants(&rows, SUPPORTED_IDENTITY_BOOTSTRAP_VERSION)?;
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
    connection: impl Into<SqliteDatabase>,
) -> Result<IdentityBootstrapOutcome> {
    let connection = connection.into().maintenance();
    let mut bootstrap = begin_identity_bootstrap(&connection).await?;
    let (gateway_created, superuser_created) = bootstrap.created_flags();
    let work = async {
        let backfill_counts = bootstrap.backfill_legacy_actors().await?;
        bootstrap.validate_and_mark().await?;
        Ok::<_, anyhow::Error>(backfill_counts)
    }
    .await;

    match work {
        Ok(backfill_counts) => {
            let snapshot = bootstrap.commit().await?;
            Ok(IdentityBootstrapOutcome {
                snapshot,
                backfill_counts,
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
    retry_with_backoff(
        || begin_identity_bootstrap_once(connection),
        is_anyhow_sqlite_lock,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

async fn begin_identity_bootstrap_once<D>(
    connection: &D,
) -> Result<IdentityBootstrapTransaction<D::Transaction>>
where
    D: TransactionTrait,
{
    let transaction = connection
        .begin_with_options(TransactionOptions {
            sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
            ..Default::default()
        })
        .await
        .context("failed to begin immediate identity bootstrap transaction")?;

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
    let (gateway, gateway_created) = match load_gateway_singleton(transaction).await? {
        Some(existing) => (existing, false),
        None => {
            let id = GatewayId::new(generate_id(GATEWAY_ID_LEN))
                .context("generated invalid Gateway identity id")?;
            (
                create_gateway_singleton(transaction, &id, 0, chrono::Utc::now().fixed_offset())
                    .await?,
                true,
            )
        }
    };

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
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{
        ActorResourceKind, actor_ref_from_db, list_gateway_identities, list_gateway_principals,
        load_identity_invariant_rows,
    };
    use pioneer_entity::turn_event;
    use pioneer_protocol::{PersistedActorRef, PrincipalKind, PrincipalStatus};
    use sea_orm::{ConnectOptions, ConnectionTrait, Database, EntityTrait};
    use std::sync::atomic::{AtomicBool, Ordering};

    async fn database() -> sea_orm::DatabaseConnection {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("connect sqlite");
        Migrator::up(&database, None).await.expect("run migrations");
        database
    }

    async fn bootstrap_before_listener(
        database: &sea_orm::DatabaseConnection,
        listener_opened: &AtomicBool,
    ) -> anyhow::Result<()> {
        bootstrap_identity(database).await?;
        listener_opened.store(true, Ordering::SeqCst);
        Ok(())
    }

    #[tokio::test]
    async fn first_run_creates_one_gateway_and_stable_superuser() {
        let database = database().await;

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
    }

    #[tokio::test]
    async fn repeated_run_preserves_ids_and_profile() {
        let database = database().await;
        let first = begin_identity_bootstrap(&database)
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();

        let repeated = begin_identity_bootstrap(&database)
            .await
            .expect("begin repeated bootstrap");
        assert_eq!(repeated.created_flags(), (false, false));
        let second = repeated.commit().await.expect("commit repeated bootstrap");

        assert_eq!(second, first);
        assert_eq!(list_gateway_identities(&database).await.unwrap().len(), 1);
        assert_eq!(list_gateway_principals(&database).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn restart_preserves_a_valid_edited_superuser_profile() {
        let database = database().await;
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
    }

    #[tokio::test]
    async fn caller_can_roll_back_identity_and_principal_together() {
        let database = database().await;
        let bootstrap = begin_identity_bootstrap(&database)
            .await
            .expect("begin identity bootstrap");

        bootstrap.rollback().await.expect("roll back bootstrap");

        assert!(list_gateway_identities(&database).await.unwrap().is_empty());
        assert!(list_gateway_principals(&database).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn legacy_actor_backfill_covers_the_complete_origin_matrix_and_is_idempotent() {
        let database = database().await;
        insert_legacy_actor_matrix(&database).await;
        let event_payload_before = turn_event::Entity::find_by_id("legacy-event")
            .one(&database)
            .await
            .unwrap()
            .unwrap()
            .payload;

        let bootstrap = begin_identity_bootstrap(&database).await.unwrap();
        let superuser_id = bootstrap.snapshot().superuser.id.clone();
        let counts = bootstrap.backfill_legacy_actors().await.unwrap();
        assert_eq!(counts.principal_threads, 3);
        assert_eq!(counts.system_threads, 2);
        assert_eq!(counts.principal_turns, 1);
        assert_eq!(counts.system_turns, 3);
        bootstrap.commit().await.unwrap();

        let rows = load_identity_invariant_rows(&database).await.unwrap();
        for row in &rows.actor_references {
            let actor =
                actor_ref_from_db(row.actor_kind.as_deref(), row.actor_id.as_deref()).unwrap();
            match (row.resource_kind, row.resource_id.as_str()) {
                (
                    ActorResourceKind::Thread,
                    "thread-collaborative" | "thread-direct" | "thread-user",
                )
                | (ActorResourceKind::Turn, "turn-user") => {
                    assert_eq!(
                        actor,
                        Some(PersistedActorRef::Principal(superuser_id.clone()))
                    );
                }
                (
                    ActorResourceKind::Thread,
                    "thread-task" | "thread-system" | "thread-pre-attributed",
                )
                | (
                    ActorResourceKind::Turn,
                    "turn-scheduled" | "turn-detached" | "turn-attached" | "turn-pre-attributed",
                ) => {
                    assert_eq!(actor, Some(PersistedActorRef::System));
                }
                (_, "thread-unknown" | "turn-unknown") => assert_eq!(actor, None),
                other => panic!("unexpected actor fixture {other:?}"),
            }
        }

        let repeated = begin_identity_bootstrap(&database).await.unwrap();
        assert_eq!(
            repeated.backfill_legacy_actors().await.unwrap(),
            Default::default()
        );
        repeated.commit().await.unwrap();

        let event_payload_after = turn_event::Entity::find_by_id("legacy-event")
            .one(&database)
            .await
            .unwrap()
            .unwrap()
            .payload;
        assert_eq!(event_payload_after, event_payload_before);
    }

    #[tokio::test]
    async fn actor_backfill_rolls_back_with_identity_creation() {
        let database = database().await;
        insert_legacy_actor_matrix(&database).await;

        let bootstrap = begin_identity_bootstrap(&database).await.unwrap();
        bootstrap.backfill_legacy_actors().await.unwrap();
        bootstrap.rollback().await.unwrap();

        assert!(list_gateway_identities(&database).await.unwrap().is_empty());
        assert!(list_gateway_principals(&database).await.unwrap().is_empty());
        let rows = load_identity_invariant_rows(&database).await.unwrap();
        assert!(
            rows.actor_references
                .iter()
                .filter(|row| row.resource_id != "thread-pre-attributed")
                .filter(|row| row.resource_id != "turn-pre-attributed")
                .all(|row| row.actor_kind.is_none() && row.actor_id.is_none())
        );
    }

    #[tokio::test]
    async fn validated_bootstrap_sets_marker_and_restart_preserves_identity() {
        let database = database().await;
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
        assert_eq!(second.backfill_counts, Default::default());
    }

    #[tokio::test]
    async fn scanner_failure_rolls_back_identity_backfill_and_marker_together() {
        let database = database().await;
        insert_legacy_actor_matrix(&database).await;
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
                .filter(|row| row.resource_id != "thread-pre-attributed")
                .filter(|row| row.resource_id != "turn-pre-attributed")
                .all(|row| row.actor_kind.is_none() && row.actor_id.is_none())
        );
    }

    #[tokio::test]
    async fn concurrent_bootstraps_create_one_stable_identity() {
        let directory = tempfile::tempdir().expect("temporary database directory");
        let url = pioneer_sqlite::sqlite_connection_url(&directory.path().join("gateway.db"));
        let mut options = ConnectOptions::new(url);
        options.max_connections(1);
        let database = Database::connect(options)
            .await
            .expect("connect sqlite file");
        Migrator::up(&database, None).await.expect("run migrations");
        let database = pioneer_sqlite::SqliteDatabase::from_single_connection(database);

        let (first, second) =
            tokio::join!(bootstrap_identity(&database), bootstrap_identity(&database));
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
    }

    #[tokio::test]
    async fn representative_database_corruption_blocks_listener_startup() {
        for (corruption, expected_code) in [
            ("duplicate_superuser", "multiple_superusers"),
            ("partial_actor", "invalid_actor_pair"),
            ("dangling_actor", "dangling_principal_actor"),
            ("unsupported_marker", "unsupported_bootstrap_version"),
        ] {
            let directory = tempfile::tempdir().expect("temporary database directory");
            let url = pioneer_sqlite::sqlite_connection_url(&directory.path().join("gateway.db"));
            let mut options = ConnectOptions::new(url);
            options.max_connections(1);
            let database = Database::connect(options)
                .await
                .expect("connect corruption drill database");
            Migrator::up(&database, None).await.expect("run migrations");
            let valid = bootstrap_identity(&database)
                .await
                .expect("establish valid identity baseline");

            match corruption {
                "duplicate_superuser" => {
                    let duplicate_superuser_sql = format!(
                        "DROP INDEX idx_gateway_principal_one_superuser; \
                         INSERT INTO gateway_principal \
                            (id, gateway_id, kind, role_key, status, display_name, \
                             nickname, nickname_key) \
                         VALUES \
                            ('P99999999999999999999', '{}', 'superuser', NULL, 'active', \
                             'Superuser', 'other-superuser', 'other-superuser');",
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
                            "DROP TRIGGER \
                                conversation_actor_thread_created_by_actor_kind_pair_insert; \
                             PRAGMA ignore_check_constraints = ON; \
                             INSERT INTO workspace (id, name, is_active, is_current) \
                             VALUES ('corrupt-workspace-001', 'Corrupt fixture', 1, 1); \
                             INSERT INTO thread \
                                (id, workspace_id, preview, mode, model, model_provider, status, \
                                 origin_kind, created_by_actor_kind, created_by_actor_id) \
                             VALUES \
                                ('corrupt-thread-part1', 'corrupt-workspace-001', '', 'chat', \
                                 'model', 'provider', 'idle', 'user', 'principal', NULL); \
                             PRAGMA ignore_check_constraints = OFF;",
                        )
                        .await
                        .expect("inject partial actor pair");
                }
                "dangling_actor" => {
                    database
                        .execute_unprepared(
                            "INSERT INTO workspace (id, name, is_active, is_current) \
                             VALUES ('corrupt-workspace-001', 'Corrupt fixture', 1, 1); \
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
        }
    }

    async fn insert_legacy_actor_matrix(database: &sea_orm::DatabaseConnection) {
        database
            .execute_unprepared(
                "INSERT INTO workspace (id, name, is_active, is_current) \
                 VALUES ('identity-test-worksp', 'Identity test', 1, 1); \
                 INSERT INTO thread \
                    (id, workspace_id, preview, mode, model, model_provider, status, origin_kind) \
                 VALUES \
                    ('thread-collaborative', 'identity-test-worksp', '', 'chat', 'm', 'p', 'idle', 'collaborative'), \
                    ('thread-direct', 'identity-test-worksp', '', 'chat', 'm', 'p', 'idle', 'direct_message'), \
                    ('thread-user', 'identity-test-worksp', '', 'chat', 'm', 'p', 'idle', 'user'), \
                    ('thread-task', 'identity-test-worksp', '', 'agent', 'm', 'p', 'idle', 'task_run'), \
                    ('thread-system', 'identity-test-worksp', '', 'agent', 'm', 'p', 'idle', 'system'), \
                    ('thread-unknown', 'identity-test-worksp', '', 'agent', 'm', 'p', 'idle', 'future_origin'), \
                    ('thread-pre-attributed', 'identity-test-worksp', '', 'agent', 'm', 'p', 'idle', 'user'); \
                 UPDATE thread SET created_by_actor_kind = 'system' \
                 WHERE id = 'thread-pre-attributed'; \
                 INSERT INTO turn (id, thread_id, status, turn_kind, origin) VALUES \
                    ('turn-user', 'thread-user', 'completed', 'conversation', 'user'), \
                    ('turn-scheduled', 'thread-task', 'completed', 'task_run', 'scheduled_task'), \
                    ('turn-detached', 'thread-task', 'completed', 'task_run', 'detached_task'), \
                    ('turn-attached', 'thread-task', 'completed', 'task_run', 'attached_task'), \
                    ('turn-unknown', 'thread-task', 'completed', 'task_run', 'future_origin'), \
                    ('turn-pre-attributed', 'thread-user', 'completed', 'conversation', 'user'); \
                 UPDATE turn SET initiated_by_actor_kind = 'system' \
                 WHERE id = 'turn-pre-attributed'; \
                 INSERT INTO turn_event \
                    (id, thread_id, turn_id, sequence, event_type, payload) \
                 VALUES \
                    ('legacy-event', 'thread-user', 'turn-user', 1, 'turn/completed', \
                     '{\"bytes\":\"must remain unchanged\"}');",
            )
            .await
            .expect("insert legacy actor fixture");
    }
}
