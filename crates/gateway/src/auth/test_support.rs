use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use migration::{Migrator, MigratorTrait};
use pioneer_protocol::{GatewayId, PrincipalId, PrincipalKind, PrincipalStatus};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection};

use crate::identity::{
    GatewayIdentitySnapshot, IdentityBootstrapSnapshot, SuperuserIdentitySnapshot,
};

pub(crate) const TEST_GATEWAY_ID: &str = "G00000000000000000001";
pub(crate) const TEST_SUPERUSER_ID: &str = "P00000000000000000001";

const EPIC_2_MIGRATION: &str = "m20260726_000002_gateway_identity_foundation";
const ISOLATED_RUNTIME_PREFIX: &str = ".pioneer.epic03-test-";
const PRODUCTION_RUNTIME_DIRECTORY: &str = ".pioneer";

#[derive(Debug)]
pub(crate) struct TestAuthClock {
    now_unix: AtomicU64,
}

impl TestAuthClock {
    pub(crate) const fn new(now_unix: u64) -> Self {
        Self {
            now_unix: AtomicU64::new(now_unix),
        }
    }

    pub(crate) fn now_unix(&self) -> u64 {
        self.now_unix.load(Ordering::SeqCst)
    }
}

pub(crate) struct PopulatedEpic2Database {
    pub(crate) database: DatabaseConnection,
    pub(crate) identity: IdentityBootstrapSnapshot,
}

#[derive(Debug)]
pub(crate) struct IsolatedAuthRuntime {
    runtime_home: tempfile::TempDir,
    config_path: PathBuf,
    database_path: PathBuf,
}

impl IsolatedAuthRuntime {
    pub(crate) fn new() -> anyhow::Result<Self> {
        let user_home = user_home()?;
        let runtime_home = tempfile::Builder::new()
            .prefix(ISOLATED_RUNTIME_PREFIX)
            .tempdir_in(&user_home)?;
        let config_path = runtime_home.path().join("explicit-test-config.toml");
        let database_path = runtime_home.path().join("gateway.db");
        let runtime_name = runtime_home
            .path()
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| anyhow::anyhow!("isolated runtime name is not UTF-8"))?;
        std::fs::write(
            &config_path,
            format!(
                "home_directory = \"{runtime_name}\"\n\
                 [gateway]\n\
                 listen_addr = \"127.0.0.1:0\"\n\
                 service_name = \"com.pioneer.gateway.epic03-test\"\n\
                 [gateway.database]\n\
                 file_name = \"gateway.db\"\n"
            ),
        )?;

        let harness = Self {
            runtime_home,
            config_path,
            database_path,
        };
        harness.validate()?;
        Ok(harness)
    }

    pub(crate) fn runtime_home(&self) -> &Path {
        self.runtime_home.path()
    }

    pub(crate) fn config_path(&self) -> &Path {
        &self.config_path
    }

    pub(crate) fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub(crate) fn safe_summary(&self) -> String {
        format!(
            "config={} runtime_home={} database={} listen_addr=127.0.0.1:0",
            self.config_path.display(),
            self.runtime_home().display(),
            self.database_path.display()
        )
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        let user_home = user_home()?;
        let production_home = user_home.join(PRODUCTION_RUNTIME_DIRECTORY);
        let runtime_home = self.runtime_home();
        let runtime_name = runtime_home
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default();

        if runtime_home == production_home
            || runtime_home.parent() != Some(user_home.as_path())
            || !runtime_name.starts_with(ISOLATED_RUNTIME_PREFIX)
        {
            anyhow::bail!(
                "isolated runtime must be an owned direct child of the user home, got `{}`",
                runtime_home.display()
            );
        }
        if self.config_path.parent() != Some(runtime_home) || !self.config_path.is_file() {
            anyhow::bail!("isolated runtime requires an explicit config file");
        }
        if self.database_path.parent() != Some(runtime_home)
            || self
                .database_path
                .file_name()
                .and_then(|value| value.to_str())
                != Some("gateway.db")
        {
            anyhow::bail!("isolated database must stay inside the owned runtime home");
        }
        let config = std::fs::read_to_string(&self.config_path)?;
        if !config.contains(&format!("home_directory = \"{runtime_name}\""))
            || !config.contains("listen_addr = \"127.0.0.1:0\"")
            || !config.contains("file_name = \"gateway.db\"")
        {
            anyhow::bail!("isolated runtime effective config is incomplete");
        }
        Ok(())
    }
}

fn user_home() -> anyhow::Result<PathBuf> {
    let value = std::env::var_os("HOME")
        .ok_or_else(|| anyhow::anyhow!("HOME must be set for the isolated auth test runtime"))?;
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        anyhow::bail!("HOME must be an absolute path for the isolated auth test runtime");
    }
    Ok(path)
}

pub(crate) async fn populated_epic2_database() -> PopulatedEpic2Database {
    let database = Database::connect("sqlite::memory:")
        .await
        .expect("connect populated Epic 2 fixture database");
    let migration_count = Migrator::migrations()
        .into_iter()
        .position(|migration| migration.name() == EPIC_2_MIGRATION)
        .map(|index| index + 1)
        .expect("Epic 2 identity migration is registered");
    let migration_count = u32::try_from(migration_count).expect("migration count fits u32");
    Migrator::up(&database, Some(migration_count))
        .await
        .expect("apply migrations through Epic 2");
    database
        .execute_unprepared(
            "INSERT INTO workspace(\
                id,name,is_active,is_current,created_at,updated_at\
             ) VALUES(\
                '000000000000000000000','Default Workspace',1,1,\
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP\
             )",
        )
        .await
        .expect("populate default Gateway workspace");
    database
        .execute_unprepared(
            "INSERT INTO gateway_identity(id, singleton_key, identity_bootstrap_version, created_at, updated_at) VALUES ('G00000000000000000001', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .expect("populate Epic 2 Gateway identity");
    database
        .execute_unprepared(
            "INSERT INTO gateway_principal(id, gateway_id, kind, role_key, status, display_name, nickname, nickname_key, created_at, updated_at, removed_at) VALUES ('P00000000000000000001', 'G00000000000000000001', 'superuser', NULL, 'active', 'Superuser', 'superuser', 'superuser', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)",
        )
        .await
        .expect("populate Epic 2 Superuser");
    let gateway_id = GatewayId::new(TEST_GATEWAY_ID).expect("test Gateway id");
    let identity = IdentityBootstrapSnapshot {
        gateway: GatewayIdentitySnapshot {
            id: gateway_id.clone(),
            identity_bootstrap_version: 1,
        },
        superuser: SuperuserIdentitySnapshot {
            id: PrincipalId::new(TEST_SUPERUSER_ID).expect("test Superuser id"),
            gateway_id,
            kind: PrincipalKind::Superuser,
            role_key: None,
            status: PrincipalStatus::Active,
            display_name: "Superuser".to_owned(),
            nickname: "superuser".to_owned(),
            nickname_key: "superuser".to_owned(),
        },
    };

    PopulatedEpic2Database { database, identity }
}

#[cfg(test)]
mod tests {
    use sea_orm::{ConnectionTrait, Statement};

    use super::*;

    #[tokio::test]
    async fn populated_fixture_stops_at_epic_2_and_has_stable_identity() {
        let fixture = populated_epic2_database().await;
        let backend = fixture.database.get_database_backend();
        let rows = fixture
            .database
            .query_all_raw(Statement::from_string(
                backend,
                "SELECT version FROM seaql_migrations ORDER BY version DESC LIMIT 1".to_owned(),
            ))
            .await
            .expect("query migration fixture");
        let version: String = rows[0].try_get("", "version").expect("migration version");

        assert_eq!(version, "m20260726_000002_gateway_identity_foundation");
        assert_eq!(fixture.identity.gateway.identity_bootstrap_version, 1);
        assert_eq!(
            fixture.identity.superuser.gateway_id,
            fixture.identity.gateway.id
        );
    }

    #[test]
    fn isolated_runtime_requires_explicit_safe_config_and_owned_paths() {
        let runtime = IsolatedAuthRuntime::new().expect("isolated runtime");
        runtime.validate().expect("safe runtime paths");
        let summary = runtime.safe_summary();
        assert!(summary.contains("127.0.0.1:0"));
        assert!(!summary.contains("/.pioneer/gateway.db"));
    }

    #[test]
    fn isolated_runtime_drop_removes_only_its_owned_root() {
        let runtime_home = {
            let runtime = IsolatedAuthRuntime::new().expect("isolated runtime");
            let runtime_home = runtime.runtime_home().to_path_buf();
            assert!(runtime_home.is_dir());
            runtime_home
        };

        assert!(!runtime_home.exists());
    }
}
