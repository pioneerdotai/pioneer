use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use pioneer_artifacts::{
    ArtifactGcPlan, ArtifactGcReport, ArtifactQuotaPolicy, ArtifactService, ArtifactWorkspaceUsage,
    LocalArtifactBlobStore,
};
use pioneer_config::AppConfig;
use pioneer_crud::CrudStore;
#[cfg(test)]
use pioneer_keystore::SecretId;
use pioneer_keystore::{
    DbKeyStoreConfig, SecretEntryMeta, SecretKind, inspect_keystore_sqlite_files,
    inspect_private_runtime_dir,
};
use serde::Serialize;

pub use pioneer_keystore::{SecretPermissionHealthReport, SecretPermissionHealthStatus};

use crate::database::{gateway_database_path, initialize_existing_for_operations};
use crate::mcp_secrets::garbage_collection_orphan_mcp_secrets;
use crate::secrets::GatewaySecrets;

#[derive(Debug, Clone, Serialize)]
pub struct SecretsStatusReport {
    pub storage_path: PathBuf,
    pub encryption: KeystoreEncryptionReport,
    pub counts: SecretKindCounts,
    pub total_entries: usize,
    pub permissions: Vec<SecretPermissionHealthReport>,
    pub mcp_orphans: McpSecretOrphanStatusReport,
}

#[derive(Debug, Clone, Serialize)]
pub struct KeystoreEncryptionReport {
    pub enabled: bool,
    pub mode: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SecretKindCounts {
    pub provider_api_key: usize,
    pub provider_proxy: usize,
    pub cli_runtime_proxy: usize,
    pub mcp_secret: usize,
    pub gateway_access_jwt_signing_key: usize,
    pub gateway_auth_credential_hmac_key: usize,
    pub user_jwt_token: usize,
    pub gateway_remote_access_secret: usize,
    pub desktop_gateway_session: usize,
    pub unknown: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct McpSecretOrphanStatusReport {
    pub available: bool,
    pub gateway_db_path: PathBuf,
    pub active_refs: Option<usize>,
    pub stored_refs: Option<usize>,
    pub orphan_refs: Option<usize>,
    pub unavailable_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct McpSecretGarbageCollectionReport {
    pub dry_run: bool,
    pub active_refs: usize,
    pub stored_refs: usize,
    pub orphan_refs: usize,
    pub deleted_refs: usize,
    pub failed_deletes: Vec<McpSecretGarbageCollectionFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct McpSecretGarbageCollectionFailure {
    pub ref_id: String,
    pub error: String,
}

pub async fn secrets_status(
    config: &AppConfig,
    runtime_home: &Path,
) -> Result<SecretsStatusReport> {
    let storage_path = DbKeyStoreConfig::for_runtime_home(runtime_home).path;
    let gateway_db_path = gateway_database_path(runtime_home, config)?;
    let gateway_secrets = GatewaySecrets::open(runtime_home)?;
    let entries = gateway_secrets.list_secret_entries()?;
    let counts = count_secret_kinds(entries.as_slice());
    let total_entries = entries.len();
    let mut permissions = Vec::with_capacity(4);
    permissions.push(inspect_private_runtime_dir(runtime_home));
    permissions.extend(inspect_keystore_sqlite_files(storage_path.as_path()));

    let mcp_orphans = match initialize_existing_for_operations(runtime_home, config).await? {
        Some(database) => {
            let crud_store = CrudStore::new(database);
            let report = garbage_collection_orphan_mcp_secrets(&crud_store, &gateway_secrets, true)
                .await
                .context("failed to compute MCP orphan secret status")?;

            McpSecretOrphanStatusReport {
                available: true,
                gateway_db_path,
                active_refs: Some(report.active_refs),
                stored_refs: Some(report.stored_refs),
                orphan_refs: Some(report.orphan_refs),
                unavailable_reason: None,
            }
        }
        None => McpSecretOrphanStatusReport {
            available: false,
            gateway_db_path,
            active_refs: None,
            stored_refs: None,
            orphan_refs: None,
            unavailable_reason: Some("gateway_db_missing".to_owned()),
        },
    };

    Ok(SecretsStatusReport {
        storage_path,
        encryption: KeystoreEncryptionReport {
            enabled: false,
            mode: "disabled".to_owned(),
        },
        counts,
        total_entries,
        permissions,
        mcp_orphans,
    })
}

pub async fn secrets_garbage_collection(
    config: &AppConfig,
    runtime_home: &Path,
    dry_run: bool,
) -> Result<McpSecretGarbageCollectionReport> {
    let gateway_db_path = gateway_database_path(runtime_home, config)?;
    let Some(database) = initialize_existing_for_operations(runtime_home, config).await? else {
        bail!(
            "gateway database `{}` is missing; refusing to garbage collect MCP secrets",
            gateway_db_path.display()
        );
    };

    let gateway_secrets = GatewaySecrets::open(runtime_home)?;
    let crud_store = CrudStore::new(database);
    let report = garbage_collection_orphan_mcp_secrets(&crud_store, &gateway_secrets, dry_run)
        .await
        .context("failed to garbage collect orphan MCP secrets")?;

    Ok(McpSecretGarbageCollectionReport {
        dry_run,
        active_refs: report.active_refs,
        stored_refs: report.stored_refs,
        orphan_refs: report.orphan_refs,
        deleted_refs: report.deleted_refs,
        failed_deletes: report
            .failed_deletes
            .into_iter()
            .map(|failure| McpSecretGarbageCollectionFailure {
                ref_id: failure.ref_id,
                error: failure.error,
            })
            .collect(),
    })
}

pub async fn artifact_storage_usage(
    config: &AppConfig,
    runtime_home: &Path,
    workspace_id: &str,
) -> Result<ArtifactWorkspaceUsage> {
    let service = open_artifact_service_for_operations(config, runtime_home).await?;
    service
        .workspace_usage(workspace_id)
        .await
        .with_context(|| format!("failed to compute artifact storage usage for `{workspace_id}`"))
}

pub async fn artifact_gc_dry_run(
    config: &AppConfig,
    runtime_home: &Path,
    workspace_id: &str,
    now_unix_ms: i64,
) -> Result<ArtifactGcPlan> {
    let service = open_artifact_service_for_operations(config, runtime_home).await?;
    service
        .gc_dry_run(workspace_id, now_unix_ms)
        .await
        .with_context(|| format!("failed to plan artifact GC for `{workspace_id}`"))
}

pub async fn artifact_gc_execute(
    config: &AppConfig,
    runtime_home: &Path,
    workspace_id: &str,
    now_unix_ms: i64,
    execute: bool,
) -> Result<ArtifactGcReport> {
    if !execute {
        bail!("artifact GC execute requires execute=true; call dry-run first for visibility");
    }
    let service = open_artifact_service_for_operations(config, runtime_home).await?;
    service
        .gc_execute(workspace_id, now_unix_ms)
        .await
        .with_context(|| format!("failed to execute artifact GC for `{workspace_id}`"))
}

async fn open_artifact_service_for_operations(
    config: &AppConfig,
    runtime_home: &Path,
) -> Result<ArtifactService> {
    let gateway_db_path = gateway_database_path(runtime_home, config)?;
    let Some(database) = initialize_existing_for_operations(runtime_home, config).await? else {
        bail!(
            "gateway database `{}` is missing; refusing artifact operations",
            gateway_db_path.display()
        );
    };
    Ok(ArtifactService::new_with_policies(
        Arc::new(CrudStore::new(database)),
        Arc::new(LocalArtifactBlobStore::new(runtime_home.to_path_buf())),
        ArtifactQuotaPolicy {
            max_file_bytes: config.gateway.artifacts.max_file_bytes,
            max_workspace_bytes: config.gateway.artifacts.max_workspace_bytes,
            max_files_per_workspace: config.gateway.artifacts.max_files_per_workspace,
            warn_at_percent: config.gateway.artifacts.quota_warn_at_percent,
        },
        pioneer_artifacts::ArtifactGcPolicy {
            grace_secs: config.gateway.artifacts.gc_grace_secs,
            output_dir_ttl_secs: config.gateway.artifacts.output_dir_ttl_secs,
            readable_copy_ttl_secs: config.gateway.artifacts.readable_copy_ttl_secs,
        },
    ))
}

fn count_secret_kinds(entries: &[SecretEntryMeta]) -> SecretKindCounts {
    let mut counts = SecretKindCounts::default();
    for entry in entries {
        match entry
            .kind
            .or_else(|| SecretKind::from_service(entry.id.service()))
        {
            Some(SecretKind::ProviderApiKey) => {
                counts.provider_api_key = counts.provider_api_key.saturating_add(1)
            }
            Some(SecretKind::ProviderProxy) => {
                counts.provider_proxy = counts.provider_proxy.saturating_add(1)
            }
            Some(SecretKind::CliRuntimeProxy) => {
                counts.cli_runtime_proxy = counts.cli_runtime_proxy.saturating_add(1)
            }
            Some(SecretKind::McpSecret) => counts.mcp_secret = counts.mcp_secret.saturating_add(1),
            Some(SecretKind::GatewayAccessJwtSigningKey) => {
                counts.gateway_access_jwt_signing_key =
                    counts.gateway_access_jwt_signing_key.saturating_add(1)
            }
            Some(SecretKind::GatewayAuthCredentialHmacKey) => {
                counts.gateway_auth_credential_hmac_key =
                    counts.gateway_auth_credential_hmac_key.saturating_add(1)
            }
            Some(SecretKind::UserJwtToken) => {
                counts.user_jwt_token = counts.user_jwt_token.saturating_add(1)
            }
            Some(SecretKind::GatewayRemoteAccessSecret) => {
                counts.gateway_remote_access_secret =
                    counts.gateway_remote_access_secret.saturating_add(1)
            }
            Some(SecretKind::DesktopGatewaySession) => {
                counts.desktop_gateway_session = counts.desktop_gateway_session.saturating_add(1)
            }
            None => counts.unknown = counts.unknown.saturating_add(1),
        }
    }
    counts
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use pioneer_config::{
        AppConfig, DesktopConfig, GatewayArtifactsConfig, GatewayAuthConfig,
        GatewayCliAgentRuntimeConfig, GatewayCliAgentRuntimeInstancesConfig,
        GatewayComputerUseToolsConfig, GatewayConfig, GatewayDatabaseConfig,
        GatewayExecutionWindowsConfig, GatewayMemoryConfig, GatewayProviderConfig,
        GatewayRuntimeConfig, GatewaySkillsConfig, GatewayThreadConfig,
        GatewayToolLoopBudgetConfig, GatewayToolRetryBudgetConfig, GatewayToolsConfig,
        GatewayWebToolsConfig, InstallConfig,
    };
    use pioneer_crud::{
        CrudStore, McpAuditEventRecord, McpServerInstallationRecord, NewArtifactBlobRecord,
    };
    use pioneer_keystore::{DbKeyStore, SecretFilter, SecretKind, SecretMeta, SecretStore};
    use pioneer_mcp::McpSecretRef;
    use sea_orm::ConnectionTrait;

    use super::*;
    use crate::database::initialize as initialize_database;

    #[tokio::test]
    async fn status_counts_secret_kinds_and_reports_disabled_encryption() {
        let runtime_home = unique_temp_dir("status-counts");
        let config = test_app_config();
        let secrets = GatewaySecrets::open(&runtime_home).expect("open secrets");
        seed_all_secret_kinds(&runtime_home, &secrets);
        seed_active_mcp_installation(&runtime_home, &config, "active_ref").await;

        let report = secrets_status(&config, &runtime_home)
            .await
            .expect("status report");

        assert_eq!(report.encryption.enabled, false);
        assert_eq!(report.encryption.mode, "disabled");
        assert_eq!(report.counts.provider_api_key, 1);
        assert_eq!(report.counts.mcp_secret, 2);
        assert_eq!(report.counts.user_jwt_token, 1);
        assert_eq!(report.counts.desktop_gateway_session, 1);
        assert_eq!(report.total_entries, 5);
        assert!(report.mcp_orphans.available);
        assert_eq!(report.mcp_orphans.active_refs, Some(1));
        assert_eq!(report.mcp_orphans.stored_refs, Some(2));
        assert_eq!(report.mcp_orphans.orphan_refs, Some(1));

        let serialized = serde_json::to_string(&report).expect("serialize report");
        assert!(!serialized.contains("sk-provider-secret"));
        assert!(!serialized.contains("mcp-active-secret"));
        assert!(!serialized.contains("desktop-refresh-secret"));
    }

    #[tokio::test]
    async fn status_reports_mcp_orphans_unavailable_when_gateway_db_is_missing() {
        let runtime_home = unique_temp_dir("status-missing-db");
        let config = test_app_config();

        let report = secrets_status(&config, &runtime_home)
            .await
            .expect("status report");

        assert!(!report.mcp_orphans.available);
        assert_eq!(
            report.mcp_orphans.unavailable_reason.as_deref(),
            Some("gateway_db_missing")
        );
        assert!(!runtime_home.join("gateway.db").exists());
    }

    #[tokio::test]
    async fn gc_dry_run_keeps_orphan_and_apply_deletes_only_mcp_orphan() {
        let runtime_home = unique_temp_dir("gc");
        let config = test_app_config();
        let secrets = GatewaySecrets::open(&runtime_home).expect("open secrets");
        seed_all_secret_kinds(&runtime_home, &secrets);
        seed_active_mcp_installation(&runtime_home, &config, "active_ref").await;

        let dry_run = secrets_garbage_collection(&config, &runtime_home, true)
            .await
            .expect("dry-run gc");
        assert!(dry_run.dry_run);
        assert_eq!(dry_run.orphan_refs, 1);
        assert_eq!(dry_run.deleted_refs, 0);
        assert_eq!(
            secrets
                .get_mcp_secret("orphan_ref")
                .expect("read orphan after dry-run"),
            Some("mcp-orphan-secret".to_owned())
        );

        let applied = secrets_garbage_collection(&config, &runtime_home, false)
            .await
            .expect("apply gc");
        assert!(!applied.dry_run);
        assert_eq!(applied.orphan_refs, 1);
        assert_eq!(applied.deleted_refs, 1);
        assert_eq!(
            secrets
                .get_mcp_secret("active_ref")
                .expect("read active after gc"),
            Some("mcp-active-secret".to_owned())
        );
        assert_eq!(
            secrets
                .get_mcp_secret("orphan_ref")
                .expect("read orphan after gc"),
            None
        );
        assert_eq!(
            secrets
                .get_provider_api_key("openrouter")
                .expect("provider key should remain"),
            Some("sk-provider-secret".to_owned())
        );

        let store = DbKeyStore::open(DbKeyStoreConfig::for_runtime_home(&runtime_home))
            .expect("open raw store");
        assert_eq!(
            store
                .list(SecretFilter::Kind(SecretKind::DesktopGatewaySession))
                .expect("list desktop sessions")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn gc_fails_when_gateway_db_is_missing() {
        let runtime_home = unique_temp_dir("gc-missing-db");
        let config = test_app_config();
        GatewaySecrets::open(&runtime_home).expect("open secrets");

        let error = secrets_garbage_collection(&config, &runtime_home, true)
            .await
            .expect_err("missing gateway db should fail");

        assert!(
            format!("{error:#}").contains("refusing to garbage collect MCP secrets"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn artifact_gc_dry_run_and_execute_requires_explicit_apply() {
        let runtime_home = unique_temp_dir("artifact-gc");
        std::fs::create_dir_all(&runtime_home).expect("create runtime home");
        let config = test_app_config();
        let database = initialize_database(&runtime_home, &config)
            .await
            .expect("initialize gateway db");
        database
            .execute_unprepared(
                "INSERT INTO workspace (id, name, is_active, is_current) VALUES ('ws_ops', 'Ops', 1, 1)",
            )
            .await
            .expect("insert workspace");
        let sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let crud_store = CrudStore::new(database.clone());
        crud_store
            .insert_test_artifact_blob(
                NewArtifactBlobRecord {
                    workspace_id: "ws_ops".to_owned(),
                    sha256: sha256.to_owned(),
                    size_bytes: 7,
                    mime_type: Some("text/plain".to_owned()),
                    storage_backend: "local".to_owned(),
                    storage_key: format!("sha256/01/23/{sha256}"),
                    metadata: Default::default(),
                },
                0,
                "orphan_blob".to_owned(),
            )
            .await
            .expect("insert orphan blob");

        let usage = artifact_storage_usage(&config, &runtime_home, "ws_ops")
            .await
            .expect("usage");
        assert_eq!(usage.bytes, 0);

        let plan = artifact_gc_dry_run(&config, &runtime_home, "ws_ops", 200_000_000)
            .await
            .expect("dry-run artifact gc");
        assert_eq!(plan.orphan_blobs.len(), 1);

        let explicit_error =
            artifact_gc_execute(&config, &runtime_home, "ws_ops", 200_000_000, false)
                .await
                .expect_err("execute flag should be required");
        assert!(format!("{explicit_error:#}").contains("execute=true"));

        let report = artifact_gc_execute(&config, &runtime_home, "ws_ops", 200_000_000, true)
            .await
            .expect("execute artifact gc");
        assert_eq!(report.deleted_blobs, 1);
        assert_eq!(
            crud_store
                .count_artifact_blobs_by_workspace("ws_ops")
                .await
                .expect("query blobs"),
            0
        );
    }

    fn seed_all_secret_kinds(runtime_home: &Path, secrets: &GatewaySecrets) {
        secrets
            .set_provider_api_key("openrouter", "sk-provider-secret")
            .expect("put provider");
        secrets
            .put_mcp_secret("active_ref", "mcp-active-secret", Some("active".to_owned()))
            .expect("put active mcp");
        secrets
            .put_mcp_secret("orphan_ref", "mcp-orphan-secret", Some("orphan".to_owned()))
            .expect("put orphan mcp");

        let raw_store = DbKeyStore::open(DbKeyStoreConfig::for_runtime_home(runtime_home))
            .expect("open raw store");
        raw_store
            .put_string(
                &SecretId::user_jwt_token("future-user").expect("user token id"),
                "future-user-token-material",
                SecretMeta::new(SecretKind::UserJwtToken, Some("future-user".to_owned()), 1),
            )
            .expect("put future user token");
        raw_store
            .put_string(
                &SecretId::desktop_gateway_session("local-session").expect("desktop session id"),
                "desktop-refresh-secret",
                SecretMeta::new(
                    SecretKind::DesktopGatewaySession,
                    Some("local session".to_owned()),
                    1,
                ),
            )
            .expect("put desktop session");
    }

    async fn seed_active_mcp_installation(
        runtime_home: &Path,
        config: &AppConfig,
        active_ref: &str,
    ) {
        let database = initialize_database(runtime_home, config)
            .await
            .expect("initialize gateway db");
        let crud_store = CrudStore::new(database);
        let record = McpServerInstallationRecord {
            id: None,
            scope_kind: "workspace".to_owned(),
            scope_key: "ws_ops".to_owned(),
            name: "resend".to_owned(),
            display_name: None,
            source_kind: "config".to_owned(),
            source_ref: "{}".to_owned(),
            transport_kind: "stdio".to_owned(),
            transport_json: "{}".to_owned(),
            auth_json: "{}".to_owned(),
            secret_refs_json: serde_json::to_string(&vec![McpSecretRef {
                ref_id: active_ref.to_owned(),
                source: "env".to_owned(),
                name: "RESEND_API_KEY".to_owned(),
            }])
            .expect("encode refs"),
            enabled: true,
            allow_implicit_invocation: true,
            required: false,
            fingerprint: "fp-resend".to_owned(),
            updated_at_unix: 1_700_000_000,
        };
        let audit = McpAuditEventRecord {
            turn_id: None,
            server_installation_id: None,
            server_name: record.name.clone(),
            raw_tool_name: None,
            callable_name: None,
            catalog_version: None,
            action: "install".to_owned(),
            decision: "allowed".to_owned(),
            reason_code: None,
            details_json: "{}".to_owned(),
            created_at_unix: 1_700_000_000,
        };
        crud_store
            .upsert_mcp_server_installation_with_audit(&record, &audit, 1_700_000_000)
            .await
            .expect("persist active MCP installation");
    }

    fn test_app_config() -> AppConfig {
        AppConfig {
            home_directory: ".pioneer.test".to_owned(),
            install_state_file_name: "install-state.toml".to_owned(),
            install: InstallConfig {
                unix_root_directory_name: "pioneer-test".to_owned(),
                macos_root_directory_name: "PioneerTest".to_owned(),
                windows_root_directory_name: "PioneerTest".to_owned(),
                managed_directory_name: "managed-test".to_owned(),
                binary_name: "pioneer-test".to_owned(),
                command_name: "pioneer-test".to_owned(),
                macos_background_item_name: "Pioneer Test".to_owned(),
                macos_associated_bundle_identifier: "ai.pioneer.test".to_owned(),
            },
            gateway: GatewayConfig {
                settings_version: 1,
                settings_file_name: "gateway-settings.toml".to_owned(),
                service_name: "com.pioneer.gateway".to_owned(),
                legacy_service_names: Vec::new(),
                listen_addr: "0.0.0.0:17878".to_owned(),
                outbound_queue_capacity: 128,
                keepawake: false,
                preflight_model: Default::default(),
                thread: GatewayThreadConfig {
                    default_model: "gpt-5.4".to_owned(),
                    default_model_provider: "openai".to_owned(),
                    summary_model: None,
                    summary_model_provider: None,
                    title_model: None,
                    title_model_provider: None,
                    max_context_tokens: 128_000,
                    response_reserve_tokens: 16_000,
                },
                tools: GatewayToolsConfig {
                    web: GatewayWebToolsConfig::default(),
                    computer_use: GatewayComputerUseToolsConfig::default(),
                    budget: GatewayToolLoopBudgetConfig::default(),
                    execution_windows: Some(GatewayExecutionWindowsConfig::default()),
                    retry: GatewayToolRetryBudgetConfig::default(),
                },
                tasks: Default::default(),
                skills: GatewaySkillsConfig::default(),
                cli_agent_runtime: GatewayCliAgentRuntimeConfig::default(),
                cli_agent_runtimes: GatewayCliAgentRuntimeInstancesConfig::default(),
                remote_access: Default::default(),
                voice: Default::default(),
                provider: GatewayProviderConfig::default(),
                database: GatewayDatabaseConfig {
                    file_name: "gateway.db".to_owned(),
                    max_connections: 10,
                    connect_timeout_ms: 5_000,
                    acquire_timeout_ms: 5_000,
                    idle_timeout_ms: 30_000,
                    sqlx_logging: false,
                    run_migrations_on_startup: true,
                },
                memory: GatewayMemoryConfig::default(),
                thread_episodic: Default::default(),
                hooks: Default::default(),
                artifacts: GatewayArtifactsConfig::default(),
                resilience: Default::default(),
                auth: GatewayAuthConfig {
                    jwt_issuer: "pioneer".to_owned(),
                    jwt_audience: "pioneer-clients".to_owned(),
                    secret_size_bytes: 64,
                    token_refresh_leeway_seconds: 60,
                    ..GatewayAuthConfig::default()
                },
            },
            desktop: DesktopConfig {
                gateway: GatewayRuntimeConfig {
                    connect_timeout_ms: 300,
                    startup_timeout_ms: 10_000,
                    poll_interval_ms: 200,
                    ws_ping_interval_ms: 10_000,
                    ws_pong_timeout_ms: 30_000,
                    ws_reconnect_initial_ms: 500,
                    ws_reconnect_max_ms: 10_000,
                    ws_reconnect_jitter_percent: 20,
                    registry_file_name: "gateway_registry.toml".to_owned(),
                    local_gateway_id: "local".to_owned(),
                    registry_version: 2,
                },
            },
        }
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("pioneer-gateway-ops-{label}-{nanos}-{id}"))
    }
}
