use anyhow::{Context, Result, anyhow, bail};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use pioneer_config::AppConfig;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::handshake::server::Request;

use crate::helpers::{normalize_non_empty, unix_timestamp_secs};
use crate::secrets::ensure_jwt_material_len;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    role: String,
    iss: String,
    aud: String,
    iat: u64,
    nbf: u64,
    exp: u64,
}

#[derive(Clone)]
pub struct JwtAuth {
    decoding_key: Arc<DecodingKey>,
    config: Arc<JwtRuntimeConfig>,
}

#[derive(Debug, Clone)]
struct JwtRuntimeConfig {
    jwt_issuer: String,
    jwt_audience: String,
    superuser_subject: String,
    superuser_role: String,
    token_ttl: Duration,
}

impl JwtRuntimeConfig {
    fn from_app_config(config: &AppConfig) -> Result<Self> {
        let gateway_auth = &config.gateway.auth;

        let jwt_issuer = normalize_non_empty(
            gateway_auth.jwt_issuer.as_str(),
            "gateway.auth.jwt_issuer must not be empty",
        )?;
        let jwt_audience = normalize_non_empty(
            gateway_auth.jwt_audience.as_str(),
            "gateway.auth.jwt_audience must not be empty",
        )?;
        let superuser_subject = normalize_non_empty(
            gateway_auth.superuser_subject.as_str(),
            "gateway.auth.superuser_subject must not be empty",
        )?;
        let superuser_role = normalize_non_empty(
            gateway_auth.superuser_role.as_str(),
            "gateway.auth.superuser_role must not be empty",
        )?;

        if gateway_auth.secret_size_bytes < 32 {
            bail!("gateway.auth.secret_size_bytes must be at least 32 bytes");
        }
        if gateway_auth.token_ttl_seconds == 0 {
            bail!("gateway.auth.token_ttl_seconds must be greater than 0");
        }

        Ok(Self {
            jwt_issuer,
            jwt_audience,
            superuser_subject,
            superuser_role,
            token_ttl: Duration::from_secs(gateway_auth.token_ttl_seconds),
        })
    }
}

impl JwtAuth {
    pub fn authorize_request(&self, request: &Request) -> Result<()> {
        let auth_header = request
            .headers()
            .get("authorization")
            .ok_or_else(|| anyhow!("missing authorization header"))?
            .to_str()
            .context("authorization header contains invalid UTF-8")?;

        let token = extract_bearer_token(auth_header)
            .ok_or_else(|| anyhow!("authorization header must use Bearer token"))?;

        let _ = self.validate_token(token)?;

        Ok(())
    }

    fn validate_token(&self, token: &str) -> Result<JwtClaims> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[self.config.jwt_issuer.as_str()]);
        validation.set_audience(&[self.config.jwt_audience.as_str()]);
        validation.validate_nbf = true;

        let token_data = decode::<JwtClaims>(token, self.decoding_key.as_ref(), &validation)
            .context("jwt validation failed")?;

        let claims = token_data.claims;
        ensure_superuser_claims(&claims, &self.config)?;

        Ok(claims)
    }
}

impl JwtClaims {
    fn new(now: u64, ttl: Duration, config: &JwtRuntimeConfig) -> Result<Self> {
        let exp = now
            .checked_add(ttl.as_secs())
            .ok_or_else(|| anyhow!("jwt expiration overflow"))?;

        Ok(Self {
            sub: config.superuser_subject.clone(),
            role: config.superuser_role.clone(),
            iss: config.jwt_issuer.clone(),
            aud: config.jwt_audience.clone(),
            iat: now,
            nbf: now,
            exp,
        })
    }
}

pub fn initialize(app_config: &AppConfig, jwt_material: &[u8]) -> Result<JwtAuth> {
    let config = JwtRuntimeConfig::from_app_config(app_config)?;
    ensure_jwt_material_len(jwt_material)?;

    Ok(JwtAuth {
        decoding_key: Arc::new(DecodingKey::from_secret(jwt_material)),
        config: Arc::new(config),
    })
}

pub fn issue_superuser_token(app_config: &AppConfig, jwt_material: &[u8]) -> Result<String> {
    let config = JwtRuntimeConfig::from_app_config(app_config)?;
    ensure_jwt_material_len(jwt_material)?;

    let claims = JwtClaims::new(unix_timestamp_secs()?, config.token_ttl, &config)?;
    let token = encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(jwt_material),
    )
    .context("failed to generate superuser jwt token")?;

    Ok(token)
}

fn ensure_superuser_claims(claims: &JwtClaims, config: &JwtRuntimeConfig) -> Result<()> {
    if claims.sub != config.superuser_subject {
        bail!("jwt subject is not superuser");
    }
    if claims.role != config.superuser_role {
        bail!("jwt role is not superuser");
    }
    Ok(())
}

fn extract_bearer_token(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let scheme = parts.next()?;
    let token = parts.next()?.trim();

    if !scheme.eq_ignore_ascii_case("Bearer") || token.is_empty() {
        return None;
    }

    Some(token)
}

#[cfg(test)]
mod tests {
    use super::{JwtAuth, extract_bearer_token, initialize, issue_superuser_token};
    use crate::secrets::GatewaySecrets;
    use pioneer_config::{
        AppConfig, DesktopConfig, GatewayArtifactsConfig, GatewayAuthConfig,
        GatewayComputerUseToolsConfig, GatewayConfig, GatewayDatabaseConfig, GatewayMemoryConfig,
        GatewayProviderConfig, GatewayRuntimeConfig, GatewaySkillsConfig, GatewayThreadConfig,
        GatewayToolLoopBudgetConfig, GatewayToolRetryBudgetConfig, GatewayToolsConfig,
        GatewayWebToolsConfig, InstallConfig,
    };
    use pioneer_keystore::{
        MemorySecretStore, SecretFilter, SecretId, SecretKind, SecretMeta, SecretStore,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio_tungstenite::tungstenite::handshake::server::Request;

    #[test]
    fn load_or_create_superuser_jwt_material_creates_and_reuses_memory_value() {
        let store = Arc::new(MemorySecretStore::new());
        let secrets = GatewaySecrets::new(store.clone());

        let first = secrets
            .load_or_create_superuser_jwt_material(64)
            .expect("create superuser jwt material");
        let second = secrets
            .load_or_create_superuser_jwt_material(64)
            .expect("reuse jwt material");

        assert_eq!(first.len(), 64);
        assert_eq!(first, second);
        let stored = store
            .get_string(&SecretId::superuser_jwt_token())
            .expect("read stored material")
            .expect("stored material exists");
        assert_eq!(stored.len(), 128);
        let entries = store
            .list(SecretFilter::Kind(SecretKind::SuperuserJwtToken))
            .expect("list jwt material");
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].id,
            SecretId::superuser_jwt_token(),
            "superuser jwt must use the singleton superuser id"
        );
    }

    #[test]
    fn load_or_create_superuser_jwt_material_persists_in_db_store() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("failed to create temp dir");

        let first = {
            let secrets = GatewaySecrets::open(&temp_dir).expect("open gateway secrets");
            secrets
                .load_or_create_superuser_jwt_material(64)
                .expect("create material")
        };

        let second = {
            let secrets = GatewaySecrets::open(&temp_dir).expect("reopen gateway secrets");
            secrets
                .load_or_create_superuser_jwt_material(64)
                .expect("reuse material")
        };

        assert_eq!(first, second);
        assert!(temp_dir.join("keystore.db").exists());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn load_or_create_superuser_jwt_material_rejects_invalid_existing_value() {
        let store = Arc::new(MemorySecretStore::new());
        let secrets = GatewaySecrets::new(store.clone());
        store
            .put_string(
                &SecretId::superuser_jwt_token(),
                "not-hex",
                SecretMeta::new(
                    SecretKind::SuperuserJwtToken,
                    Some("superuser".to_owned()),
                    1,
                ),
            )
            .expect("seed invalid material");

        let error = secrets
            .load_or_create_superuser_jwt_material(64)
            .expect_err("invalid existing material should fail");
        assert!(
            format!("{error:#}").contains("failed to decode superuser jwt material"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn load_or_create_superuser_jwt_material_rejects_short_existing_value() {
        let store = Arc::new(MemorySecretStore::new());
        let secrets = GatewaySecrets::new(store.clone());
        store
            .put_string(
                &SecretId::superuser_jwt_token(),
                "00",
                SecretMeta::new(
                    SecretKind::SuperuserJwtToken,
                    Some("superuser".to_owned()),
                    1,
                ),
            )
            .expect("seed short material");

        let error = secrets
            .load_or_create_superuser_jwt_material(64)
            .expect_err("short existing material should fail");
        assert!(
            format!("{error:#}").contains("jwt material is too short"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn issue_superuser_token_returns_valid_token() {
        let config = test_app_config();
        let jwt_material = test_jwt_material();
        let auth = initialize(&config, jwt_material.as_slice()).expect("expected jwt init");
        let token = issue_superuser_token(&config, jwt_material.as_slice())
            .expect("token issue should succeed");

        let request = request_with_token(Some(token.as_str()));
        auth.authorize_request(&request)
            .expect("expected generated token to be valid");
    }

    #[test]
    fn public_issue_superuser_token_uses_keystore_without_settings_secret() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("failed to create temp dir");

        let config = test_app_config();
        let token = crate::issue_superuser_token(&config, &temp_dir)
            .expect("public token issue should succeed");
        assert!(temp_dir.join("keystore.db").exists());
        assert!(
            !temp_dir
                .join(config.gateway.settings_file_name.as_str())
                .exists(),
            "public token issue should not create gateway settings"
        );

        let secrets = GatewaySecrets::open(&temp_dir).expect("reopen gateway secrets");
        let jwt_material = secrets
            .load_or_create_superuser_jwt_material(config.gateway.auth.secret_size_bytes)
            .expect("reload jwt material");
        let auth = initialize(&config, jwt_material.as_slice()).expect("expected jwt init");
        let request = request_with_token(Some(token.as_str()));
        auth.authorize_request(&request)
            .expect("publicly issued token should validate");

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn authorize_request_rejects_missing_header() {
        let config = test_app_config();
        let jwt_material = test_jwt_material();
        let auth: JwtAuth =
            initialize(&config, jwt_material.as_slice()).expect("expected jwt init to succeed");
        let request = request_with_token(None);

        assert!(auth.authorize_request(&request).is_err());
    }

    #[test]
    fn bearer_token_parser_handles_scheme_and_trimming() {
        assert_eq!(extract_bearer_token("Bearer abc"), Some("abc"));
        assert_eq!(extract_bearer_token("bearer abc"), Some("abc"));
        assert_eq!(extract_bearer_token("  Bearer   abc  "), Some("abc"));
        assert_eq!(extract_bearer_token("Basic abc"), None);
        assert_eq!(extract_bearer_token("Bearer"), None);
    }

    fn request_with_token(token: Option<&str>) -> Request {
        let mut builder = Request::builder().method("GET").uri("ws://0.0.0.0:17878");

        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }

        builder.body(()).expect("failed to build request")
    }

    fn test_jwt_material() -> Vec<u8> {
        (0..64).map(|value| value as u8).collect()
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
                    retry: GatewayToolRetryBudgetConfig::default(),
                },
                skills: GatewaySkillsConfig::default(),
                provider: GatewayProviderConfig {
                    default_timeout_secs: 120,
                    attachments: Default::default(),
                },
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
                hooks: Default::default(),
                artifacts: GatewayArtifactsConfig::default(),
                auth: GatewayAuthConfig {
                    jwt_issuer: "pioneer".to_owned(),
                    jwt_audience: "pioneer-clients".to_owned(),
                    superuser_subject: "superuser".to_owned(),
                    superuser_role: "superuser".to_owned(),
                    secret_size_bytes: 64,
                    token_ttl_seconds: 60 * 60,
                    token_refresh_leeway_seconds: 60,
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

    fn unique_temp_dir() -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        std::env::temp_dir().join(format!("pioneer-gateway-auth-tests-{nanos}-{id}"))
    }
}
