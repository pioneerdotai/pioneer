use super::registry::{default_registry, load_registry, normalize_registry, save_registry};
use super::runtime::is_same_gateway_version;
use pioneer_client::gateway::types::{GatewayEndpoint, GatewayEndpointKind};
use pioneer_config::{
    AppConfig, DesktopConfig, GatewayArtifactsConfig, GatewayAuthConfig,
    GatewayComputerUseToolsConfig, GatewayConfig, GatewayDatabaseConfig, GatewayMemoryConfig,
    GatewayProviderConfig, GatewayRuntimeConfig, GatewaySkillsConfig, GatewayThreadConfig,
    GatewayToolLoopBudgetConfig, GatewayToolRetryBudgetConfig, GatewayToolsConfig,
    GatewayWebToolsConfig, InstallConfig,
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
    assert_eq!(registry.local.id, config.desktop.gateway.local_gateway_id);
    assert_eq!(
        registry.local.auth_token_ref.as_deref(),
        Some(config.desktop.gateway.local_gateway_id.as_str())
    );
    assert!(registry.active_gateway_id.is_none());
    let content = fs::read_to_string(&registry_path).expect("read registry");
    assert!(content.contains("auth_token_ref"));
    assert!(!content.contains("auth_token ="));

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
fn load_registry_rejects_non_current_registry_version() {
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
    .expect("failed to write migratable registry");

    let error = load_registry(&registry_path, &config)
        .expect_err("non-current registry version should be rejected");
    assert!(
        format!("{error:#}").contains("unsupported gateway registry version"),
        "unexpected error: {error:#}"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn registry_serialization_contains_auth_token_ref_only() {
    let endpoint = GatewayEndpoint {
        id: "remote-123".to_owned(),
        name: "Remote".to_owned(),
        address: "127.0.0.1:22000".to_owned(),
        kind: GatewayEndpointKind::Remote,
        auth_token_ref: Some("remote-123".to_owned()),
        workspace_id: None,
        service_name: None,
    };

    let content = toml::to_string_pretty(&endpoint).expect("serialize endpoint");

    assert!(content.contains("auth_token_ref"));
    assert!(!content.contains("auth_token ="));
}

#[test]
fn registry_deserialization_rejects_legacy_auth_token_field() {
    let content = r#"
id = "remote-123"
name = "Remote"
address = "127.0.0.1:22000"
kind = "remote"
auth_token = "secret-token"
"#;

    let error = toml::from_str::<GatewayEndpoint>(content)
        .expect_err("legacy auth_token field should be rejected");

    assert!(
        error.to_string().contains("unknown field"),
        "unexpected error: {error}"
    );
}

#[test]
fn registry_deserialization_accepts_auth_token_ref_field() {
    let content = r#"
id = "remote-123"
name = "Remote"
address = "127.0.0.1:22000"
kind = "remote"
auth_token_ref = "remote-123"
"#;

    let endpoint = toml::from_str::<GatewayEndpoint>(content)
        .expect("auth_token_ref registry field should be accepted");

    assert_eq!(endpoint.auth_token_ref.as_deref(), Some("remote-123"));
}

#[test]
fn load_registry_preserves_auth_token_ref_without_resolving_secret() {
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
name = "Local Gateway"
address = "0.0.0.0:17878"
kind = "local"
auth_token_ref = "local"
service_name = "com.pioneer.gateway"

[[remotes]]
id = "remote-123"
name = "Remote"
address = "127.0.0.1:22000"
kind = "remote"
auth_token_ref = "remote-123"
"#,
    )
    .expect("write registry");
    let registry = load_registry(&registry_path, &config).expect("load registry with ref");

    assert_eq!(registry.remotes.len(), 1);
    assert_eq!(
        registry.remotes[0].auth_token_ref.as_deref(),
        Some("remote-123")
    );
    let content = fs::read_to_string(&registry_path).expect("read registry");
    assert!(content.contains("auth_token_ref = \"remote-123\""));

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn load_registry_preserves_auth_token_ref_without_keystore_value() {
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
name = "Local Gateway"
address = "0.0.0.0:17878"
kind = "local"
auth_token_ref = "local"
service_name = "com.pioneer.gateway"

[[remotes]]
id = "remote-123"
name = "Remote"
address = "127.0.0.1:22000"
kind = "remote"
auth_token_ref = "remote-123"
"#,
    )
    .expect("write registry");
    let registry = load_registry(&registry_path, &config).expect("load registry with token ref");

    assert_eq!(registry.remotes.len(), 1);
    assert_eq!(
        registry.remotes[0].auth_token_ref.as_deref(),
        Some("remote-123")
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn normalize_registry_rejects_invalid_auth_token_ref() {
    let config = test_config();
    let mut registry = default_registry(&config).expect("default registry");
    registry.remotes.push(GatewayEndpoint {
        id: "remote-123".to_owned(),
        name: "Remote".to_owned(),
        address: "127.0.0.1:22000".to_owned(),
        kind: GatewayEndpointKind::Remote,
        auth_token_ref: Some("../remote-123".to_owned()),
        workspace_id: None,
        service_name: None,
    });

    let error = normalize_registry(&mut registry, &config)
        .expect_err("invalid auth token ref should be rejected");

    assert!(
        format!("{error:#}").contains("path separators"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn save_registry_serializes_only_auth_token_ref() {
    let config = test_config();
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).expect("failed to create test temp dir");
    let registry_path = temp_dir.join(config.desktop.gateway.registry_file_name.as_str());
    let registry = default_registry(&config).expect("default registry");

    save_registry(&registry_path, &registry).expect("save registry");

    let content = fs::read_to_string(&registry_path).expect("read registry");
    assert!(content.contains("auth_token_ref = \"local\""));
    assert!(!content.contains("auth_token ="));

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
            memory: GatewayMemoryConfig::default(),
            hooks: Default::default(),
            artifacts: GatewayArtifactsConfig::default(),
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
            auth: GatewayAuthConfig {
                jwt_issuer: "pioneer".to_owned(),
                jwt_audience: "pioneer-clients".to_owned(),
                superuser_subject: "superuser".to_owned(),
                superuser_role: "superuser".to_owned(),
                secret_size_bytes: 64,
                token_ttl_seconds: 31536000,
                token_refresh_leeway_seconds: 86400,
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
                registry_version: 1,
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
