//! Deterministic, test-only multi-principal fixture contract for Epic 4.
//!
//! This module deliberately does not provision a Member through a production
//! API. Phase-specific tests materialize these records through internal test
//! seams after the corresponding schema and authorization layers exist.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use migration::{Migrator, MigratorTrait};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, TransactionTrait};

use crate::auth::test_support::{
    IsolatedAuthRuntime, TEST_GATEWAY_ID, TEST_SUPERUSER_ID, TestAuthClock,
};
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

    #[cfg(test)]
    fn expose_for_assertion(&self) -> &str {
        &self.0
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

#[derive(Debug, Default)]
pub(crate) struct RecipientCapture {
    recipients_by_method: Mutex<BTreeMap<String, BTreeSet<String>>>,
}

impl RecipientCapture {
    pub(crate) fn record<I, S>(&self, method: impl Into<String>, recipients: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.recipients_by_method
            .lock()
            .expect("Epic 5 recipient capture lock")
            .entry(method.into())
            .or_default()
            .extend(recipients.into_iter().map(Into::into));
    }

    pub(crate) fn recipients(&self, method: &str) -> BTreeSet<String> {
        self.recipients_by_method
            .lock()
            .expect("Epic 5 recipient capture lock")
            .get(method)
            .cloned()
            .unwrap_or_default()
    }
}

#[derive(Debug)]
pub(crate) struct Epic5Fixture {
    pub authorization: Epic4Fixture,
    pub clock: TestAuthClock,
    pub recipients: RecipientCapture,
}

impl Epic5Fixture {
    pub(crate) fn deterministic() -> Self {
        Self {
            authorization: Epic4Fixture::deterministic(),
            clock: TestAuthClock::new(EPIC5_TEST_NOW_UNIX),
            recipients: RecipientCapture::default(),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ResourceFamily {
    Workspace,
    Thread,
    ThreadFolder,
    TurnTimeline,
    Artifact,
    ArtifactTransfer,
    Memory,
    Episodic,
    AgentsDocument,
    Task,
    TaskCandidate,
    TaskDelivery,
    WorkspaceRealtime,
    ThreadRealtime,
    Replay,
    Provider,
    Voice,
    Mcp,
    Skill,
    CliRuntime,
    PermissionRequest,
    SelfImprovementSource,
    LearnedOverlay,
    DerivedProjection,
}

impl ResourceFamily {
    pub(crate) const ALL: [Self; 24] = [
        Self::Workspace,
        Self::Thread,
        Self::ThreadFolder,
        Self::TurnTimeline,
        Self::Artifact,
        Self::ArtifactTransfer,
        Self::Memory,
        Self::Episodic,
        Self::AgentsDocument,
        Self::Task,
        Self::TaskCandidate,
        Self::TaskDelivery,
        Self::WorkspaceRealtime,
        Self::ThreadRealtime,
        Self::Replay,
        Self::Provider,
        Self::Voice,
        Self::Mcp,
        Self::Skill,
        Self::CliRuntime,
        Self::PermissionRequest,
        Self::SelfImprovementSource,
        Self::LearnedOverlay,
        Self::DerivedProjection,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum AccessPath {
    List,
    Get,
    Mutate,
    Paginate,
    Replay,
    Search,
    BinaryTransfer,
    SubscriptionDelivery,
}

impl AccessPath {
    pub(crate) const ALL: [Self; 8] = [
        Self::List,
        Self::Get,
        Self::Mutate,
        Self::Paginate,
        Self::Replay,
        Self::Search,
        Self::BinaryTransfer,
        Self::SubscriptionDelivery,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum IdorDimension {
    SamePrincipalAllowedWorkspace,
    SamePrincipalForbiddenWorkspace,
    SameWorkspaceForbiddenPrivateThread,
    DifferentPrincipalGuessedChildId,
    MalformedParentChildPair,
    MissingResource,
    Superuser,
}

impl IdorDimension {
    pub(crate) const ALL: [Self; 7] = [
        Self::SamePrincipalAllowedWorkspace,
        Self::SamePrincipalForbiddenWorkspace,
        Self::SameWorkspaceForbiddenPrivateThread,
        Self::DifferentPrincipalGuessedChildId,
        Self::MalformedParentChildPair,
        Self::MissingResource,
        Self::Superuser,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExpectedDisclosure {
    Allow,
    Omit,
    NotFound,
    InvalidRequest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IdorCase {
    pub family: ResourceFamily,
    pub path: AccessPath,
    pub dimension: IdorDimension,
    pub expected: ExpectedDisclosure,
}

pub(crate) fn exhaustive_idor_matrix() -> Vec<IdorCase> {
    let mut cases = Vec::with_capacity(
        ResourceFamily::ALL.len() * AccessPath::ALL.len() * IdorDimension::ALL.len(),
    );
    for family in ResourceFamily::ALL {
        for path in AccessPath::ALL {
            for dimension in IdorDimension::ALL {
                cases.push(IdorCase {
                    family,
                    path,
                    dimension,
                    expected: expected_disclosure(path, dimension),
                });
            }
        }
    }
    cases
}

const fn expected_disclosure(path: AccessPath, dimension: IdorDimension) -> ExpectedDisclosure {
    match dimension {
        IdorDimension::SamePrincipalAllowedWorkspace | IdorDimension::Superuser => {
            ExpectedDisclosure::Allow
        }
        IdorDimension::MalformedParentChildPair => ExpectedDisclosure::InvalidRequest,
        IdorDimension::MissingResource => ExpectedDisclosure::NotFound,
        IdorDimension::SamePrincipalForbiddenWorkspace
        | IdorDimension::SameWorkspaceForbiddenPrivateThread
        | IdorDimension::DifferentPrincipalGuessedChildId => match path {
            AccessPath::List
            | AccessPath::Paginate
            | AccessPath::Replay
            | AccessPath::Search
            | AccessPath::SubscriptionDelivery => ExpectedDisclosure::Omit,
            AccessPath::Get | AccessPath::Mutate | AccessPath::BinaryTransfer => {
                ExpectedDisclosure::NotFound
            }
        },
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

    #[test]
    fn authorization_fixture_contract_is_deterministic_and_complete() {
        let fixture = Epic4Fixture::deterministic();
        assert_eq!(fixture.gateway_id, TEST_GATEWAY_ID);
        assert_eq!(fixture.principals.len(), 5);
        assert_eq!(fixture.sessions.len(), 2);
        assert_eq!(fixture.threads.len(), 6);

        let member_a = &fixture.principals[MEMBER_A_ID];
        let member_b = &fixture.principals[MEMBER_B_ID];
        assert_eq!(member_a.status, FixturePrincipalStatus::Active);
        assert_eq!(member_b.status, FixturePrincipalStatus::Active);
        assert_eq!(
            member_a.role_key.as_ref().map(RoleKey::as_str),
            Some("member")
        );
        assert_eq!(
            member_b.role_key.as_ref().map(RoleKey::as_str),
            Some("member")
        );
        assert_eq!(
            fixture.principals[SUSPENDED_MEMBER_ID].status,
            FixturePrincipalStatus::Suspended
        );
        assert_eq!(
            fixture.principals[REMOVED_MEMBER_ID].status,
            FixturePrincipalStatus::Removed
        );

        let a_workspaces = &fixture.workspace_memberships[MEMBER_A_ID];
        let b_workspaces = &fixture.workspace_memberships[MEMBER_B_ID];
        assert_eq!(
            a_workspaces
                .intersection(b_workspaces)
                .copied()
                .collect::<Vec<_>>(),
            vec![WORKSPACE_RED_ID]
        );
        assert!(a_workspaces.contains(WORKSPACE_BLUE_ID));
        assert!(!b_workspaces.contains(WORKSPACE_BLUE_ID));
        assert!(b_workspaces.contains(WORKSPACE_GREEN_ID));
        assert!(!a_workspaces.contains(WORKSPACE_GREEN_ID));

        assert_eq!(
            fixture.threads[THREAD_RED_PRIVATE_A_ID].visibility,
            ThreadAccessClass::Private
        );
        assert_eq!(
            fixture.threads[THREAD_RED_WORKSPACE_ID].visibility,
            ThreadAccessClass::Workspace
        );
        assert_eq!(
            fixture.threads[THREAD_RED_INTERNAL_ID].visibility,
            ThreadAccessClass::Internal
        );
        assert!(
            fixture.threads[THREAD_RED_PRIVATE_A_ID]
                .participant_principal_ids
                .contains(MEMBER_A_ID)
        );
        assert!(
            !fixture.threads[THREAD_RED_PRIVATE_A_ID]
                .participant_principal_ids
                .contains(MEMBER_B_ID)
        );
    }

    #[test]
    fn authorization_fixture_credentials_are_independent() {
        let fixture = Epic4Fixture::deterministic();
        let member_a = &fixture.sessions[MEMBER_A_ID];
        let member_b = &fixture.sessions[MEMBER_B_ID];
        assert_ne!(member_a.device_id, member_b.device_id);
        assert_ne!(member_a.session_id, member_b.session_id);
        assert_ne!(member_a.access_credential, member_b.access_credential);
        assert_ne!(member_a.refresh_credential, member_b.refresh_credential);
        assert_ne!(
            member_a.access_credential.expose_for_assertion(),
            member_b.access_credential.expose_for_assertion()
        );
        assert!(!format!("{fixture:?}").contains("epic5-test-"));
        assert!(format!("{fixture:?}").contains("[REDACTED]"));
    }

    #[test]
    fn epic5_fixture_has_deterministic_clock_installations_and_recipient_capture() {
        let fixture = Epic5Fixture::deterministic();
        assert_eq!(fixture.clock.now_unix(), EPIC5_TEST_NOW_UNIX);
        assert_eq!(
            fixture.authorization.sessions[MEMBER_A_ID]
                .installation
                .installation_id,
            "fixture-member-a"
        );
        fixture.recipients.record(
            "member/changed",
            [TEST_SUPERUSER_ID, MEMBER_A_ID, MEMBER_A_ID],
        );
        assert_eq!(
            fixture.recipients.recipients("member/changed"),
            BTreeSet::from([TEST_SUPERUSER_ID.to_owned(), MEMBER_A_ID.to_owned()])
        );
        assert!(fixture.recipients.recipients("unknown").is_empty());
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

    #[test]
    fn authorization_fixture_idor_matrix_is_exhaustive() {
        let matrix = exhaustive_idor_matrix();
        assert_eq!(
            matrix.len(),
            ResourceFamily::ALL.len() * AccessPath::ALL.len() * IdorDimension::ALL.len()
        );

        for family in ResourceFamily::ALL {
            for path in AccessPath::ALL {
                for dimension in IdorDimension::ALL {
                    assert_eq!(
                        matrix
                            .iter()
                            .filter(|case| {
                                case.family == family
                                    && case.path == path
                                    && case.dimension == dimension
                            })
                            .count(),
                        1
                    );
                }
            }
        }

        assert!(matrix.iter().all(|case| {
            case.dimension != IdorDimension::DifferentPrincipalGuessedChildId
                || case.expected != ExpectedDisclosure::Allow
        }));
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
                    id,gateway_id,created_by_principal_id,created_by_session_id,status,token_hash,\
                    token_format_version,expires_at,accepted_at,revoked_at,expired_at,\
                    accepted_principal_id,accepted_device_id,accepted_session_id,revoke_reason,\
                    created_at,updated_at\
                 ) VALUES\
                    ('I0000000000000000000A','G00000000000000000001',\
                     'P0000000000000000000B','S0000000000000000000B','accepted',NULL,1,\
                     datetime(CURRENT_TIMESTAMP,'+7 days'),CURRENT_TIMESTAMP,NULL,NULL,\
                     'P0000000000000000000A','D0000000000000000000A',\
                     'S0000000000000000000A',NULL,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                    ('I0000000000000000000B','G00000000000000000001',\
                     'P0000000000000000000A','S0000000000000000000A','accepted',NULL,1,\
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

    #[test]
    fn authorization_fixture_preserves_absolute_superuser_baseline() {
        let fixture = Epic4Fixture::deterministic();
        let superuser = &fixture.principals[TEST_SUPERUSER_ID];
        assert_eq!(superuser.kind, FixturePrincipalKind::Superuser);
        assert_eq!(superuser.status, FixturePrincipalStatus::Active);
        assert_eq!(superuser.role_key, None);
        assert!(
            !fixture
                .workspace_memberships
                .contains_key(TEST_SUPERUSER_ID),
            "Superuser must not need synthetic memberships"
        );
        assert!(exhaustive_idor_matrix().iter().all(|case| {
            case.dimension != IdorDimension::Superuser || case.expected == ExpectedDisclosure::Allow
        }));
    }
}
