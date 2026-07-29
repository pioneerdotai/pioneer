use super::registry::{
    complete_registry_upgrade, default_registry, load_registry, load_registry_for_runtime,
    save_registry,
};
use super::runtime::is_same_gateway_version;
use pioneer_client::gateway::registry::commit_registry_v2_binding;
use pioneer_client::gateway::types::{GatewayEndpoint, GatewayEndpointKind};
use pioneer_config::{
    AppConfig, DesktopConfig, GatewayArtifactsConfig, GatewayAuthConfig,
    GatewayCliAgentRuntimeConfig, GatewayCliAgentRuntimeInstancesConfig,
    GatewayComputerUseToolsConfig, GatewayConfig, GatewayDatabaseConfig, GatewayMemoryConfig,
    GatewayProviderConfig, GatewayRuntimeConfig, GatewaySkillsConfig, GatewayThreadConfig,
    GatewayToolLoopBudgetConfig, GatewayToolRetryBudgetConfig, GatewayToolsConfig,
    GatewayVoiceConfig, GatewayWebToolsConfig, InstallConfig,
};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn gateway_version_comparison_requires_full_version_match() {
    assert!(is_same_gateway_version("0.1.0", "0.1.0"));
    assert!(is_same_gateway_version("v1.2.3", "1.2.3"));
    assert!(!is_same_gateway_version("1.2.3", "1.2.4"));
    assert!(!is_same_gateway_version("1.2.3-beta.1", "1.2.3"));
    assert!(!is_same_gateway_version("nightly", "1.0.0"));
}

#[test]
fn load_registry_creates_file_and_persists_default_state() {
    let config = test_config();
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).expect("failed to create test temp dir");

    let registry_path = temp_dir.join(config.desktop.gateway.registry_file_name.as_str());
    let registry = load_registry(&registry_path, &config).expect("failed to load registry");

    assert!(registry_path.exists());
    assert_eq!(registry.version, config.desktop.gateway.registry_version);
    let local = registry.local.as_ref().expect("desktop local gateway");
    assert_eq!(local.id, config.desktop.gateway.local_gateway_id);
    assert_eq!(local.address, "127.0.0.1:17878");
    assert!(local.session_ref.is_none());
    assert!(local.server_gateway_id.is_none());
    assert!(registry.active_gateway_id.is_none());
    let content = fs::read_to_string(&registry_path).expect("read registry");
    assert!(!content.contains("auth_token_ref"));
    assert!(!content.contains("auth_token ="));
    assert!(!content.contains("refresh_token"));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = fs::metadata(&registry_path)
            .expect("registry metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn load_registry_rejects_unsupported_registry_version() {
    rust_i18n::set_locale("en");

    let config = test_config();
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).expect("failed to create test temp dir");

    let registry_path = temp_dir.join(config.desktop.gateway.registry_file_name.as_str());
    fs::write(
        &registry_path,
        r#"
version = 99
active_gateway_id = "local"

[local]
id = "local"
name = "Local Gateway"
address = "0.0.0.0:17878"
kind = "local"
service_name = "com.pioneer.gateway"
"#,
    )
    .expect("failed to write unsupported-version registry");

    let error = load_registry(&registry_path, &config)
        .expect_err("unsupported registry version should be rejected");
    assert!(
        format!("{error:#}").contains("unsupported gateway registry version"),
        "unexpected error: {error:#}"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn load_registry_accepts_current_v2_registry() {
    rust_i18n::set_locale("en");

    let config = test_config();
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).expect("failed to create test temp dir");

    let registry_path = temp_dir.join(config.desktop.gateway.registry_file_name.as_str());
    fs::write(
        &registry_path,
        r#"
version = 2
active_gateway_id = "local"

[local]
id = "local"
name = "Local Gateway"
address = "0.0.0.0:17878"
kind = "local"
service_name = "com.pioneer.gateway"
"#,
    )
    .expect("failed to write current registry");

    let registry = load_registry(&registry_path, &config).expect("v2 registry should load");
    assert_eq!(registry.version, 2);
    assert!(registry.installation_id.is_some());

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn old_v1_registry_preserves_profiles_until_cutover_commit() {
    let config = test_config();
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).expect("failed to create test temp dir");
    let registry_path = temp_dir.join(config.desktop.gateway.registry_file_name.as_str());
    fs::write(
        &registry_path,
        r#"
version = 1
active_gateway_id = "remote-123"

[local]
id = "local"
name = "Old Local"
address = "0.0.0.0:17878"
kind = "local"
auth_token_ref = "old-local-superuser-token"
workspace_id = "workspace-local"
service_name = "com.pioneer.gateway"

[[remotes]]
id = "remote-123"
name = "Old Remote"
address = "wss://old.example/ws"
kind = "remote"
auth_token_ref = "old-superuser-token"
workspace_id = "workspace-remote"

[[remotes]]
id = "remote-456"
name = "Other Remote"
address = "wss://other.example/ws"
kind = "remote"
auth_token_ref = "other-superuser-token"
"#,
    )
    .expect("write old registry");
    let original = fs::read_to_string(&registry_path).expect("read original v1 registry");

    let loaded = load_registry_for_runtime(&registry_path, &config)
        .expect("old registry metadata should migrate");
    assert!(loaded.upgrade_pending);
    let registry = loaded.registry;
    let upgrade_installation_id = registry
        .installation_id
        .clone()
        .expect("upgrade installation id");
    assert_eq!(registry.version, 2);
    assert_eq!(registry.active_gateway_id.as_deref(), Some("remote-123"));
    assert_eq!(registry.remotes.len(), 2);
    assert_eq!(registry.remotes[0].name, "Old Remote");
    assert_eq!(registry.remotes[0].address, "wss://old.example/ws");
    assert_eq!(
        registry.remotes[0].workspace_id.as_deref(),
        Some("workspace-remote")
    );
    assert!(registry.remotes[0].session_ref.is_none());
    assert!(registry.remotes[0].server_gateway_id.is_none());
    assert_eq!(
        registry
            .local
            .as_ref()
            .and_then(|local| local.workspace_id.as_deref()),
        Some("workspace-local")
    );
    assert_eq!(
        fs::read_to_string(&registry_path).expect("read pending v1 registry"),
        original
    );
    assert!(
        fs::read_to_string(&registry_path)
            .expect("read pending v1 registry")
            .contains("version = 1")
    );
    let retried = load_registry_for_runtime(&registry_path, &config)
        .expect("pending registry upgrade should be retryable");
    assert!(retried.upgrade_pending);
    assert_eq!(
        retried.registry.installation_id.as_deref(),
        Some(upgrade_installation_id.as_str()),
        "a retry must reuse the installation id that owns any durable session envelope"
    );

    let mut registry = registry;
    commit_registry_v2_binding(
        &mut registry,
        "remote-123",
        "remote-123",
        &pioneer_protocol::GatewayId::new("G00000000000000000001").expect("GatewayId"),
    )
    .expect("bind activated session before cutover");
    save_registry(&registry_path, &registry).expect("commit registry v2 after session activation");
    let persisted_pending = load_registry_for_runtime(&registry_path, &config)
        .expect("a durable v2 registry with upgrade state remains retryable");
    assert!(persisted_pending.upgrade_pending);
    assert_eq!(
        persisted_pending.registry.installation_id,
        registry.installation_id
    );
    assert_eq!(
        fs::read_dir(&temp_dir)
            .expect("read pending upgrade directory")
            .count(),
        2,
        "writing registry v2 must retain upgrade state until the session cutover is complete"
    );
    complete_registry_upgrade(&registry_path).expect("complete registry v2 cutover");
    let persisted = fs::read_to_string(&registry_path).expect("read migrated registry");
    assert!(persisted.contains("version = 2"));
    assert!(persisted.contains("Old Remote"));
    assert!(persisted.contains("Other Remote"));
    assert!(persisted.contains("session_ref = \"remote-123\""));
    assert!(persisted.contains("server_gateway_id = \"G00000000000000000001\""));
    assert!(!persisted.contains("auth_token_ref"));
    assert!(!persisted.contains("old-superuser-token"));
    assert!(!persisted.contains("old-local-superuser-token"));
    assert_eq!(
        fs::read_dir(&temp_dir)
            .expect("read completed upgrade directory")
            .count(),
        1,
        "committing registry v2 must remove the transient upgrade state"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn registry_serialization_contains_session_binding_without_credentials() {
    let endpoint = GatewayEndpoint {
        id: "remote-123".to_owned(),
        name: "Remote".to_owned(),
        address: "127.0.0.1:22000".to_owned(),
        kind: GatewayEndpointKind::Remote,
        session_ref: Some("remote-123".to_owned()),
        server_gateway_id: Some(
            pioneer_protocol::GatewayId::new("G00000000000000000001").expect("GatewayId"),
        ),
        workspace_id: None,
        service_name: None,
    };

    let content = toml::to_string_pretty(&endpoint).expect("serialize endpoint");

    assert!(content.contains("session_ref"));
    assert!(content.contains("server_gateway_id"));
    assert!(!content.contains("auth_token_ref"));
    assert!(!content.contains("auth_token ="));
    assert!(!content.contains("refresh_token"));
}

#[test]
fn endpoint_deserialization_rejects_any_persisted_credential_field() {
    for field in [
        r#"auth_token = "secret-token""#,
        r#"auth_token_ref = "old-secret-ref""#,
        r#"refresh_token = "prf_secret""#,
    ] {
        let content = format!(
            r#"
id = "remote-123"
name = "Remote"
address = "127.0.0.1:22000"
kind = "remote"
{field}
"#
        );
        let error = toml::from_str::<GatewayEndpoint>(content.as_str())
            .expect_err("persisted credential field should be rejected");
        assert!(
            error.to_string().contains("unknown field"),
            "unexpected error for {field}: {error}"
        );
    }
}

#[test]
fn save_registry_never_serializes_credentials() {
    let config = test_config();
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).expect("failed to create test temp dir");
    let registry_path = temp_dir.join(config.desktop.gateway.registry_file_name.as_str());
    let registry = default_registry(&config).expect("default registry");

    save_registry(&registry_path, &registry).expect("save registry");
    save_registry(&registry_path, &registry).expect("replace registry atomically");

    let content = fs::read_to_string(&registry_path).expect("read registry");
    assert!(!content.contains("auth_token_ref"));
    assert!(!content.contains("auth_token ="));
    assert!(!content.contains("refresh_token"));
    assert_eq!(
        fs::read_dir(&temp_dir)
            .expect("read registry directory")
            .count(),
        1,
        "successful registry writes must not leave temporary siblings"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

pub(crate) fn test_config() -> AppConfig {
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
                execution_windows: Some(pioneer_config::GatewayExecutionWindowsConfig::default()),
                retry: GatewayToolRetryBudgetConfig::default(),
            },
            tasks: Default::default(),
            skills: GatewaySkillsConfig::default(),
            cli_agent_runtime: GatewayCliAgentRuntimeConfig::default(),
            cli_agent_runtimes: GatewayCliAgentRuntimeInstancesConfig::default(),
            remote_access: Default::default(),
            voice: GatewayVoiceConfig::default(),
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

pub(crate) fn unique_temp_dir() -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

    std::env::temp_dir().join(format!("pioneer-gateway-tests-{nanos}-{id}"))
}
