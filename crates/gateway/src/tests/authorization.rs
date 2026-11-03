//! Deterministic, test-only multi-principal authorization fixtures.
//!
//! This module deliberately does not provision a Member through a production
//! API. Phase-specific tests materialize these records through internal test
//! seams after the corresponding schema and authorization layers exist.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use migration::{Migrator, MigratorTrait};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, TransactionTrait};

use crate::auth::test_support::{IsolatedAuthRuntime, TEST_GATEWAY_ID, TEST_SUPERUSER_ID};
use crate::authorization::ThreadAccessClass;
use crate::identity::{
    GatewayIdentitySnapshot, IdentityBootstrapSnapshot, SuperuserIdentitySnapshot,
};
use pioneer_protocol::{
    ClientInstallationDescriptor, ClientKind, GatewayId, PrincipalId, PrincipalKind,
    PrincipalStatus, RoleKey,
};

pub(crate) const EPIC5_TEST_NOW_UNIX: u64 = 1_800_000_000;

static FIXTURE_SECRET_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(crate) const MEMBER_A_ID: &str = "P0000000000000000000A";
pub(crate) const MEMBER_B_ID: &str = "P0000000000000000000B";
pub(crate) const SUSPENDED_MEMBER_ID: &str = "P0000000000000000000C";
pub(crate) const REMOVED_MEMBER_ID: &str = "P0000000000000000000D";

pub(crate) const WORKSPACE_RED_ID: &str = "W00000000000000000001";
pub(crate) const WORKSPACE_BLUE_ID: &str = "W00000000000000000002";
pub(crate) const WORKSPACE_GREEN_ID: &str = "W00000000000000000003";

pub(crate) const THREAD_RED_PRIVATE_A_ID: &str = "T00000000000000000001";
pub(crate) const THREAD_RED_PRIVATE_B_ID: &str = "T00000000000000000002";
pub(crate) const THREAD_RED_WORKSPACE_ID: &str = "T00000000000000000003";
pub(crate) const THREAD_RED_INTERNAL_ID: &str = "T00000000000000000004";
pub(crate) const THREAD_BLUE_PRIVATE_A_ID: &str = "T00000000000000000005";
pub(crate) const THREAD_GREEN_PRIVATE_B_ID: &str = "T00000000000000000006";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FixturePrincipalKind {
    Superuser,
    User,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FixturePrincipalStatus {
    Active,
    Suspended,
    Removed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PrincipalFixture {
    pub id: &'static str,
    pub kind: FixturePrincipalKind,
    pub role_key: Option<RoleKey>,
    pub status: FixturePrincipalStatus,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct FixtureSecret(String);

impl FixtureSecret {
    fn generated(label: &str) -> Self {
        let sequence = FIXTURE_SECRET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Self(format!("epic5-test-{label}-{sequence}"))
    }
}

impl fmt::Debug for FixtureSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FixtureSecret([REDACTED])")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeviceSessionFixture {
    pub principal_id: &'static str,
    pub device_id: &'static str,
    pub session_id: &'static str,
    pub installation: ClientInstallationDescriptor,
    pub access_credential: FixtureSecret,
    pub refresh_credential: FixtureSecret,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ThreadFixture {
    pub id: &'static str,
    pub workspace_id: &'static str,
    pub visibility: ThreadAccessClass,
    pub creator_principal_id: Option<&'static str>,
    pub participant_principal_ids: BTreeSet<&'static str>,
    pub child_resources: ChildResourceFixture,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChildResourceFixture {
    pub turn_id: String,
    pub timeline_event_id: String,
    pub artifact_id: String,
    pub memory_id: String,
    pub episodic_item_id: String,
    pub agents_document_id: String,
    pub task_id: String,
    pub task_run_id: String,
    pub task_candidate_id: String,
    pub task_delivery_id: String,
}

impl ChildResourceFixture {
    fn for_thread(thread_id: &str) -> Self {
        Self {
            turn_id: format!("{thread_id}-turn"),
            timeline_event_id: format!("{thread_id}-event"),
            artifact_id: format!("{thread_id}-artifact"),
            memory_id: format!("{thread_id}-memory"),
            episodic_item_id: format!("{thread_id}-episodic"),
            agents_document_id: format!("{thread_id}-agents-doc"),
            task_id: format!("{thread_id}-task"),
            task_run_id: format!("{thread_id}-task-run"),
            task_candidate_id: format!("{thread_id}-task-candidate"),
            task_delivery_id: format!("{thread_id}-task-delivery"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Epic4Fixture {
    pub gateway_id: &'static str,
    pub principals: BTreeMap<&'static str, PrincipalFixture>,
    pub sessions: BTreeMap<&'static str, DeviceSessionFixture>,
    pub workspace_memberships: BTreeMap<&'static str, BTreeSet<&'static str>>,
    pub threads: BTreeMap<&'static str, ThreadFixture>,
}

impl Epic4Fixture {
    pub(crate) fn deterministic() -> Self {
        let principals = [
            PrincipalFixture {
                id: TEST_SUPERUSER_ID,
                kind: FixturePrincipalKind::Superuser,
                role_key: None,
                status: FixturePrincipalStatus::Active,
            },
            PrincipalFixture {
                id: MEMBER_A_ID,
                kind: FixturePrincipalKind::User,
                role_key: Some(RoleKey::member()),
                status: FixturePrincipalStatus::Active,
            },
            PrincipalFixture {
                id: MEMBER_B_ID,
                kind: FixturePrincipalKind::User,
                role_key: Some(RoleKey::member()),
                status: FixturePrincipalStatus::Active,
            },
            PrincipalFixture {
                id: SUSPENDED_MEMBER_ID,
                kind: FixturePrincipalKind::User,
                role_key: Some(RoleKey::member()),
                status: FixturePrincipalStatus::Suspended,
            },
            PrincipalFixture {
                id: REMOVED_MEMBER_ID,
                kind: FixturePrincipalKind::User,
                role_key: Some(RoleKey::member()),
                status: FixturePrincipalStatus::Removed,
            },
        ]
        .into_iter()
        .map(|principal| (principal.id, principal))
        .collect();

        let sessions = [
            DeviceSessionFixture {
                principal_id: MEMBER_A_ID,
                device_id: "D0000000000000000000A",
                session_id: "S0000000000000000000A",
                installation: ClientInstallationDescriptor {
                    installation_id: "fixture-member-a".to_owned(),
                    display_name: "Member A Desktop".to_owned(),
                    client_kind: ClientKind::Desktop,
                    platform: Some("test".to_owned()),
                    client_version: Some("1".to_owned()),
                },
                access_credential: FixtureSecret::generated("access-member-a"),
                refresh_credential: FixtureSecret::generated("refresh-member-a"),
            },
            DeviceSessionFixture {
                principal_id: MEMBER_B_ID,
                device_id: "D0000000000000000000B",
                session_id: "S0000000000000000000B",
                installation: ClientInstallationDescriptor {
                    installation_id: "fixture-member-b".to_owned(),
                    display_name: "Member B Desktop".to_owned(),
                    client_kind: ClientKind::Desktop,
                    platform: Some("test".to_owned()),
                    client_version: Some("1".to_owned()),
                },
                access_credential: FixtureSecret::generated("access-member-b"),
                refresh_credential: FixtureSecret::generated("refresh-member-b"),
            },
        ]
        .into_iter()
        .map(|session| (session.principal_id, session))
        .collect();

        // Red deliberately overlaps. Blue and Green are non-overlapping.
        let workspace_memberships = [
            (
                MEMBER_A_ID,
                BTreeSet::from([WORKSPACE_RED_ID, WORKSPACE_BLUE_ID]),
            ),
            (
                MEMBER_B_ID,
                BTreeSet::from([WORKSPACE_RED_ID, WORKSPACE_GREEN_ID]),
            ),
        ]
        .into_iter()
        .collect();

        let threads = [
            private_thread(THREAD_RED_PRIVATE_A_ID, WORKSPACE_RED_ID, MEMBER_A_ID),
            private_thread(THREAD_RED_PRIVATE_B_ID, WORKSPACE_RED_ID, MEMBER_B_ID),
            ThreadFixture {
                id: THREAD_RED_WORKSPACE_ID,
                workspace_id: WORKSPACE_RED_ID,
                visibility: ThreadAccessClass::Workspace,
                creator_principal_id: Some(MEMBER_A_ID),
                participant_principal_ids: BTreeSet::from([MEMBER_A_ID]),
                child_resources: ChildResourceFixture::for_thread(THREAD_RED_WORKSPACE_ID),
            },
            ThreadFixture {
                id: THREAD_RED_INTERNAL_ID,
                workspace_id: WORKSPACE_RED_ID,
                visibility: ThreadAccessClass::Internal,
                creator_principal_id: None,
                participant_principal_ids: BTreeSet::new(),
                child_resources: ChildResourceFixture::for_thread(THREAD_RED_INTERNAL_ID),
            },
            private_thread(THREAD_BLUE_PRIVATE_A_ID, WORKSPACE_BLUE_ID, MEMBER_A_ID),
            private_thread(THREAD_GREEN_PRIVATE_B_ID, WORKSPACE_GREEN_ID, MEMBER_B_ID),
        ]
        .into_iter()
        .map(|thread| (thread.id, thread))
        .collect();

        Self {
            gateway_id: TEST_GATEWAY_ID,
            principals,
            sessions,
            workspace_memberships,
            threads,
        }
    }
}

fn private_thread(
    id: &'static str,
    workspace_id: &'static str,
    creator_principal_id: &'static str,
) -> ThreadFixture {
    ThreadFixture {
        id,
        workspace_id,
        visibility: ThreadAccessClass::Private,
        creator_principal_id: Some(creator_principal_id),
        participant_principal_ids: BTreeSet::from([creator_principal_id]),
        child_resources: ChildResourceFixture::for_thread(id),
    }
}

pub(crate) struct IsolatedEpic4Harness {
    runtime: IsolatedAuthRuntime,
    pub database: DatabaseConnection,
    pub fixture: Epic4Fixture,
    pub identity: IdentityBootstrapSnapshot,
}

impl IsolatedEpic4Harness {
    pub(crate) async fn new() -> anyhow::Result<Self> {
        Self::populated().await
    }

    pub(crate) async fn empty() -> anyhow::Result<Self> {
        Self::with_member_foundation(false).await
    }

    pub(crate) async fn populated() -> anyhow::Result<Self> {
        Self::with_member_foundation(true).await
    }

    async fn with_member_foundation(populated: bool) -> anyhow::Result<Self> {
        let runtime = IsolatedAuthRuntime::new()?;
        runtime.validate()?;
        let database = Database::connect("sqlite::memory:").await?;
        Migrator::up(&database, None).await?;
        if populated {
            materialize_member_auth_foundation(&database).await?;
        }
        let gateway_id = GatewayId::new(TEST_GATEWAY_ID)?;
        let identity = IdentityBootstrapSnapshot {
            gateway: GatewayIdentitySnapshot {
                id: gateway_id.clone(),
                identity_bootstrap_version: 1,
            },
            superuser: SuperuserIdentitySnapshot {
                id: PrincipalId::new(TEST_SUPERUSER_ID)?,
                gateway_id,
                kind: PrincipalKind::Superuser,
                role_key: None,
                status: PrincipalStatus::Active,
                display_name: "Superuser".to_owned(),
                nickname: "superuser".to_owned(),
                nickname_key: "superuser".to_owned(),
            },
        };
        Ok(Self {
            runtime,
            database,
            fixture: Epic4Fixture::deterministic(),
            identity,
        })
    }

    pub(crate) fn config_path(&self) -> &Path {
        self.runtime.config_path()
    }

    pub(crate) fn runtime_home(&self) -> &Path {
        self.runtime.runtime_home()
    }

    pub(crate) fn database_path(&self) -> &Path {
        self.runtime.database_path()
    }
}

async fn materialize_member_auth_foundation(database: &DatabaseConnection) -> anyhow::Result<()> {
    let transaction = database.begin().await?;
    let result = transaction
        .execute_unprepared(
            "INSERT INTO workspace(\
                id,name,is_active,is_current,created_at,updated_at\
             ) VALUES\
                ('W00000000000000000001','Epic 4 Red',1,1,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                ('W00000000000000000002','Epic 4 Blue',1,0,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                ('W00000000000000000003','Epic 4 Green',1,0,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);\
             INSERT INTO gateway_identity(\
                id,singleton_key,identity_bootstrap_version,auth_schema_version,auth_ready_at,\
                created_at,updated_at\
             ) VALUES(\
                'G00000000000000000001',1,1,0,NULL,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP\
             );\
             INSERT INTO gateway_principal(\
                id,gateway_id,kind,role_key,status,display_name,nickname,nickname_key,\
                created_at,updated_at,removed_at\
             ) VALUES\
                ('P00000000000000000001','G00000000000000000001','superuser',NULL,'active',\
                 'Superuser','superuser','superuser',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL),\
                ('P0000000000000000000A','G00000000000000000001','user','member','active',\
                 'Member A','member-a','member-a',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL),\
                ('P0000000000000000000B','G00000000000000000001','user','member','active',\
                 'Member B','member-b','member-b',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL),\
                ('P0000000000000000000C','G00000000000000000001','user','member','suspended',\
                 'Suspended Member','member-c','member-c',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL),\
                ('P0000000000000000000D','G00000000000000000001','user','member','removed',\
                 'Removed Member','member-d','member-d',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,\
                 CURRENT_TIMESTAMP);\
             INSERT INTO workspace_membership(\
                principal_id,workspace_id,granted_by_actor_kind,granted_by_actor_id,\
                created_at,updated_at\
             ) VALUES\
                ('P0000000000000000000A','W00000000000000000001','system',NULL,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                ('P0000000000000000000A','W00000000000000000002','system',NULL,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                ('P0000000000000000000B','W00000000000000000001','system',NULL,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                ('P0000000000000000000B','W00000000000000000003','system',NULL,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);\
             INSERT INTO thread(\
                id,workspace_id,name,preview,mode,model,model_provider,status,\
                origin_kind,sidebar_visibility,access_class,created_at,updated_at,\
                created_by_actor_kind,created_by_actor_id\
             ) VALUES\
                ('T00000000000000000001','W00000000000000000001','Red private A','',\
                 'chat','test','test','active','user','visible','private',\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'principal','P0000000000000000000A'),\
                ('T00000000000000000002','W00000000000000000001','Red private B','',\
                 'chat','test','test','active','user','visible','private',\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'principal','P0000000000000000000B'),\
                ('T00000000000000000003','W00000000000000000001','Red workspace','',\
                 'chat','test','test','active','user','visible','workspace',\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'principal','P0000000000000000000A'),\
                ('T00000000000000000004','W00000000000000000001','Red internal','',\
                 'agent','test','test','active','task_run','hidden','internal',\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'system',NULL),\
                ('T00000000000000000005','W00000000000000000002','Blue private A','',\
                 'chat','test','test','active','user','visible','private',\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'principal','P0000000000000000000A'),\
                ('T00000000000000000006','W00000000000000000003','Green private B','',\
                 'chat','test','test','active','user','visible','private',\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'principal','P0000000000000000000B');\
             INSERT INTO thread_membership(\
                thread_id,principal_id,added_by_actor_kind,added_by_actor_id,\
                created_at,updated_at\
             ) VALUES\
                ('T00000000000000000001','P0000000000000000000A','system',NULL,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                ('T00000000000000000002','P0000000000000000000B','system',NULL,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                ('T00000000000000000005','P0000000000000000000A','system',NULL,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                ('T00000000000000000006','P0000000000000000000B','system',NULL,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);\
             INSERT INTO thread_lineage(\
                child_thread_id,parent_thread_id,root_thread_id,depth,created_at,\
                origin_kind,created_by_thread_id,created_by_turn_id\
             ) VALUES(\
                'T00000000000000000004','T00000000000000000001',\
                'T00000000000000000001',1,CURRENT_TIMESTAMP,'task_run',\
                'T00000000000000000001',NULL\
             );\
             INSERT INTO device(\
                id,gateway_id,principal_id,installation_id,display_name,client_kind,\
                platform,client_version,status,created_at,updated_at,last_seen_at,revoked_at\
             ) VALUES\
                ('D0000000000000000000A','G00000000000000000001',\
                 'P0000000000000000000A','fixture-member-a','Member A Desktop','desktop',\
                 'test','1','active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL),\
                ('D0000000000000000000B','G00000000000000000001',\
                 'P0000000000000000000B','fixture-member-b','Member B Desktop','desktop',\
                 'test','1','active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL);\
             INSERT INTO auth_session(\
                id,gateway_id,principal_id,device_id,token_family_id,created_by_session_id,\
                activation_token_hash,activation_locator_hash,activation_failed_attempts,\
                activation_expires_at,activated_at,status,refresh_generation,created_at,\
                updated_at,last_seen_at,last_refreshed_at,refresh_expires_at,revoked_at,\
                revoke_reason\
             ) VALUES\
                ('S0000000000000000000A','G00000000000000000001',\
                 'P0000000000000000000A','D0000000000000000000A',\
                 'F0000000000000000000A',NULL,\
                 X'000000000000000000000000000000000000000000000000000000000000000A',\
                 X'100000000000000000000000000000000000000000000000000000000000000A',\
                 0,datetime('now','+10 minutes'),CURRENT_TIMESTAMP,'active',0,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,\
                 datetime('now','+90 days'),NULL,NULL),\
                ('S0000000000000000000B','G00000000000000000001',\
                 'P0000000000000000000B','D0000000000000000000B',\
                 'F0000000000000000000B',NULL,\
                 X'000000000000000000000000000000000000000000000000000000000000000B',\
                 X'100000000000000000000000000000000000000000000000000000000000000B',\
                 0,datetime('now','+10 minutes'),CURRENT_TIMESTAMP,'active',0,\
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,\
                 datetime('now','+90 days'),NULL,NULL);\
             INSERT INTO auth_refresh_credential(\
                id,session_id,token_family_id,generation,token_hash,issued_at,expires_at\
             ) VALUES\
                ('R0000000000000000000A','S0000000000000000000A',\
                 'F0000000000000000000A',0,\
                 X'200000000000000000000000000000000000000000000000000000000000000A',\
                 CURRENT_TIMESTAMP,datetime('now','+90 days')),\
                ('R0000000000000000000B','S0000000000000000000B',\
                 'F0000000000000000000B',0,\
                 X'200000000000000000000000000000000000000000000000000000000000000B',\
                 CURRENT_TIMESTAMP,datetime('now','+90 days'));",
        )
        .await;
    match result {
        Ok(_) => {
            transaction.commit().await?;
            Ok(())
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn scalar_i64(database: &DatabaseConnection, sql: &str) -> i64 {
        database
            .query_one_raw(sea_orm::Statement::from_string(
                database.get_database_backend(),
                sql.to_owned(),
            ))
            .await
            .expect("fixture scalar query")
            .expect("fixture scalar row")
            .try_get::<i64>("", "value")
            .expect("fixture scalar integer")
    }

    async fn scalar_string(database: &DatabaseConnection, sql: &str) -> String {
        database
            .query_one_raw(sea_orm::Statement::from_string(
                database.get_database_backend(),
                sql.to_owned(),
            ))
            .await
            .expect("fixture scalar query")
            .expect("fixture scalar row")
            .try_get::<String>("", "value")
            .expect("fixture scalar string")
    }

    #[tokio::test]
    async fn authorization_fixture_provides_empty_and_populated_database_states() {
        let empty = IsolatedEpic4Harness::empty()
            .await
            .expect("empty isolated Epic 4 harness");
        assert_eq!(
            scalar_i64(
                &empty.database,
                "SELECT COUNT(*) AS value FROM gateway_principal"
            )
            .await,
            0
        );

        let populated = IsolatedEpic4Harness::populated()
            .await
            .expect("populated isolated Epic 4 harness");
        assert_eq!(
            scalar_i64(
                &populated.database,
                "SELECT COUNT(*) AS value FROM gateway_principal"
            )
            .await,
            5
        );
    }

    #[tokio::test]
    async fn authorization_fixture_uses_only_isolated_runtime_and_database() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("isolated Epic 4 harness");
        assert!(harness.config_path().is_file());
        assert!(harness.runtime_home().is_dir());
        assert_eq!(
            harness.database_path().parent(),
            Some(harness.runtime_home())
        );
        assert!(!harness.runtime_home().ends_with(".pioneer"));
        harness
            .database
            .ping()
            .await
            .expect("in-memory database is isolated and live");
        assert_eq!(harness.fixture.gateway_id, TEST_GATEWAY_ID);
        harness
            .database
            .execute_unprepared(
                "INSERT INTO invitation(\
                    id,gateway_id,created_by_principal_id,created_by_session_id,target_role_key,status,token_hash,\
                    token_format_version,expires_at,accepted_at,revoked_at,expired_at,\
                    accepted_principal_id,accepted_device_id,accepted_session_id,revoke_reason,\
                    created_at,updated_at\
                 ) VALUES\
                    ('I0000000000000000000A','G00000000000000000001',\
                     'P0000000000000000000B','S0000000000000000000B','member','accepted',NULL,1,\
                     datetime(CURRENT_TIMESTAMP,'+7 days'),CURRENT_TIMESTAMP,NULL,NULL,\
                     'P0000000000000000000A','D0000000000000000000A',\
                     'S0000000000000000000A',NULL,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                    ('I0000000000000000000B','G00000000000000000001',\
                     'P0000000000000000000A','S0000000000000000000A','member','accepted',NULL,1,\
                     datetime(CURRENT_TIMESTAMP,'+7 days'),CURRENT_TIMESTAMP,NULL,NULL,\
                     'P0000000000000000000B','D0000000000000000000B',\
                     'S0000000000000000000B',NULL,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);\
                 INSERT INTO invitation_workspace_grant(invitation_id,workspace_id,created_at)\
                 VALUES\
                    ('I0000000000000000000A','W00000000000000000001',CURRENT_TIMESTAMP),\
                    ('I0000000000000000000B','W00000000000000000001',CURRENT_TIMESTAMP)",
            )
            .await
            .expect("seed exact invitation provenance for Member fixture sessions");
        crate::auth::ensure_auth_readiness(&harness.database, &harness.identity)
            .await
            .expect("materialized Member fixture passes auth readiness");
        assert!(
            pioneer_crud::scan_auth_persistence_invariants(&harness.database)
                .await
                .expect("scan materialized auth fixture")
                .is_valid()
        );
        assert_eq!(
            scalar_i64(
                &harness.database,
                "SELECT COUNT(*) AS value FROM auth_session WHERE status='active'"
            )
            .await,
            2
        );
    }

    #[tokio::test]
    async fn authorization_fixture_materializes_the_complete_cross_principal_acl_matrix() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("isolated Epic 4 harness");

        assert_eq!(
            scalar_i64(
                &harness.database,
                "SELECT COUNT(*) AS value FROM workspace \
                 WHERE id IN ('W00000000000000000001','W00000000000000000002',\
                              'W00000000000000000003')"
            )
            .await,
            3
        );
        assert_eq!(
            scalar_string(
                &harness.database,
                "SELECT group_concat(workspace_id, ',') AS value FROM (\
                    SELECT workspace_id FROM workspace_membership \
                    WHERE principal_id='P0000000000000000000A' ORDER BY workspace_id\
                 )"
            )
            .await,
            format!("{WORKSPACE_RED_ID},{WORKSPACE_BLUE_ID}")
        );
        assert_eq!(
            scalar_string(
                &harness.database,
                "SELECT group_concat(workspace_id, ',') AS value FROM (\
                    SELECT workspace_id FROM workspace_membership \
                    WHERE principal_id='P0000000000000000000B' ORDER BY workspace_id\
                 )"
            )
            .await,
            format!("{WORKSPACE_RED_ID},{WORKSPACE_GREEN_ID}")
        );
        assert_eq!(
            scalar_i64(
                &harness.database,
                "SELECT COUNT(*) AS value FROM workspace_membership \
                 WHERE principal_id='P00000000000000000001'"
            )
            .await,
            0,
            "Superuser must not receive synthetic workspace grants"
        );
        assert_eq!(
            scalar_i64(
                &harness.database,
                "SELECT COUNT(*) AS value FROM thread \
                 WHERE id IN ('T00000000000000000001','T00000000000000000002',\
                              'T00000000000000000003','T00000000000000000004',\
                              'T00000000000000000005','T00000000000000000006')"
            )
            .await,
            6
        );
        assert_eq!(
            scalar_string(
                &harness.database,
                "SELECT group_concat(access_class, ',') AS value FROM (\
                    SELECT access_class FROM thread \
                    WHERE id IN ('T00000000000000000001','T00000000000000000002',\
                                 'T00000000000000000003','T00000000000000000004',\
                                 'T00000000000000000005','T00000000000000000006') \
                    ORDER BY id\
                 )"
            )
            .await,
            "private,private,workspace,internal,private,private"
        );
        assert_eq!(
            scalar_i64(
                &harness.database,
                "SELECT COUNT(*) AS value FROM thread_membership \
                 WHERE (thread_id='T00000000000000000001' \
                        AND principal_id='P0000000000000000000A') \
                    OR (thread_id='T00000000000000000002' \
                        AND principal_id='P0000000000000000000B') \
                    OR (thread_id='T00000000000000000005' \
                        AND principal_id='P0000000000000000000A') \
                    OR (thread_id='T00000000000000000006' \
                        AND principal_id='P0000000000000000000B')"
            )
            .await,
            4
        );
        assert_eq!(
            scalar_i64(
                &harness.database,
                "SELECT COUNT(*) AS value FROM pragma_foreign_key_check"
            )
            .await,
            0
        );
    }
}
