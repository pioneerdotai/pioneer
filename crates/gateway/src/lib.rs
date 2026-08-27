//! Gateway runtime orchestration.
//!
//! The gateway owns the top-level execution-backend decision: existing API
//! providers continue through `pioneer-agent`, while local `CLIAgentRuntime`
//! executions are routed through `pioneer-cli-agent-runtime`.

/// Native stack reserved for each Gateway Tokio worker.
///
/// Gateway message and Task workflows compose large generated async poll
/// frames. Keep the production runtime aligned with the stack contract used by
/// the Gateway message-test runtime instead of relying on Tokio's smaller
/// platform default.
pub const GATEWAY_WORKER_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;

mod administrative_audit;
mod artifact_delivery;
mod artifact_prompt_refs;
mod attachment;
mod auth;
pub(crate) mod authorization;
mod bootstrap;
pub mod claude_mcp_conformance;
#[doc(hidden)]
pub mod cli_mcp_client_validation;
mod cli_runtime;
pub mod codex_mcp_conformance;
mod database;
mod epic5_observability;
mod helpers;
mod hook_run_store;
mod hook_runtime;
mod human_interaction;
mod identity;
mod invitation;
mod keep_awake;
mod mcp_secrets;
mod mcp_service;
mod member;
mod memory_policy;
mod memory_runtime;
mod memory_tools;
mod message;
mod operations;
mod patch_history_observer;
mod permissions;
mod profile_avatar;
mod prompt_hooks;
mod public_error;
mod request_context;
mod resilience;
mod secrets;
mod self_improvement;
mod session;
mod settings;
mod system_skills;
mod task_delivery_policy;
mod task_projection;
mod task_tools;
#[cfg(test)]
mod tests;
mod thread;
mod thread_episodic;
mod thread_episodic_embedding;
mod thread_episodic_hooks;
mod tokenizer;
mod transport;
mod turn_mcp;
mod turn_runtime_snapshot;
mod turn_security;
mod view_grants;
mod voice;
mod workspace;

use anyhow::{Context, Result};
use attachment::CrudArtifactExternalRefCacheBackend;
use pioneer_agent::ToolLoopConfig;
use pioneer_config::{
    AppConfig, GatewayExecutionWindowsConfig, GatewayMemoryConfig, GatewayTasksConfig,
    GatewayThreadEpisodicConfig, GatewayToolLoopBudgetConfig, GatewayToolsConfig,
};
use pioneer_crud::CrudStore;
use pioneer_hooks::HookAwaitPolicy;
use pioneer_memory::hooks::{MemoryActiveRecallMode, MemoryLoopConfig};
use pioneer_protocol::AuthDeviceCreateResponse;
use pioneer_provider::{
    ArtifactExternalRefCachePolicy, AttachmentCircuitBreakerPolicy, AttachmentNormalizationPolicy,
    AttachmentPipelineConfig, AttachmentRetryPolicy, AttachmentRuntimePolicy,
    AttachmentSecurityPolicy, ProviderRegistry, ProviderTimeoutPolicy,
    set_artifact_external_ref_cache_backend, set_default_attachment_pipeline_config,
};
use pioneer_skills::SkillTrustLevel;
use pioneer_tasks::{TaskReviewRuntimeConfig, TaskRuntimeConfig};
use pioneer_tools::{
    ComputerUseToolsConfig, ExecutionWindowBudgetConfig, ExecutionWindowTotalBudgetConfig,
    ExecutionWindowsConfig, ToolLoopBudgetConfig, ToolRetryBudgetConfig, WebToolsConfig,
};
use pioneer_tunnel::{RemoteAccessDesiredState, RemoteAccessSupervisor};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use crate::auth::{AuthAdmissionService, GatewayAuthService, ensure_auth_readiness};
use crate::bootstrap::bootstrap as run_bootstrap;
use crate::database::initialize_with_startup as initialize_database;
use crate::identity::bootstrap_identity;
use crate::mcp_secrets::garbage_collection_orphan_mcp_secrets;
use crate::memory_runtime::GatewayMemoryRuntime;
use crate::message::now_timestamp_secs;
use crate::message::{ContextBudget, SummaryConfig};
use crate::message::{MessageProcessor, MessageProcessorResilienceConfig};
use crate::secrets::GatewaySecrets;
use crate::self_improvement::supervisor::SelfImprovementSupervisor;
use crate::session::SessionManager;
use crate::thread::ThreadManager;
use crate::thread_episodic::{
    ThreadEpisodicIndexExecutorConfig, ThreadEpisodicRecallServiceConfig,
    ThreadEpisodicRuntimeConfig,
};
use crate::transport::spawn_server;
use crate::voice::model_catalog::voice_model_catalog;
use crate::voice::supervisor::{
    EagerVoiceEngineLoader, FilesystemVoiceModelInstaller, VoiceEngineLoader,
    VoiceInputDesiredState, VoiceInputSupervisor, VoiceModelInstaller,
};
use crate::workspace::WorkspaceManager;

pub use crate::operations::{
    KeystoreEncryptionReport, McpSecretGarbageCollectionFailure, McpSecretGarbageCollectionReport,
    McpSecretOrphanStatusReport, SecretKindCounts, SecretPermissionHealthReport,
    SecretPermissionHealthStatus, SecretsStatusReport, artifact_gc_dry_run, artifact_gc_execute,
    artifact_storage_usage, secrets_garbage_collection, secrets_status,
};
pub use crate::settings::{
    GatewayMemorySettings, GatewaySettings, GatewayThreadEpisodicSettings,
    load_or_create_gateway_settings, normalize_settings_file_name, save_gateway_settings,
};

const HOME_DIRECTORY_TOKEN: &str = "{homeDirectory}";

fn create_voice_input_supervisor(
    installer: Arc<dyn VoiceModelInstaller>,
    engine_loader: Arc<dyn VoiceEngineLoader>,
) -> Arc<VoiceInputSupervisor> {
    Arc::new(VoiceInputSupervisor::new(installer, engine_loader))
}

fn start_voice_input_supervisor(
    supervisor: &Arc<VoiceInputSupervisor>,
    desired: VoiceInputDesiredState,
) -> Result<()> {
    let applied = supervisor
        .apply_desired(desired, false)
        .context("failed to apply persisted Voice Input settings")?;
    if let Some(reconcile) = applied.reconcile {
        let worker = supervisor.clone();
        tokio::spawn(async move {
            worker.reconcile(reconcile).await;
        });
    }
    Ok(())
}

pub async fn run_gateway_until_shutdown() -> Result<()> {
    run_gateway_until_shutdown_with_startup(pioneer_observability::GatewayStartupTrace::start())
        .await
}

pub async fn run_gateway_until_shutdown_with_startup(
    startup: pioneer_observability::GatewayStartupTrace,
) -> Result<()> {
    let result = run_gateway_until_shutdown_inner(&startup).await;
    if result.is_err() {
        startup.finish_failure();
    }
    result
}

async fn run_gateway_until_shutdown_inner(
    startup: &pioneer_observability::GatewayStartupTrace,
) -> Result<()> {
    let config_stage = startup.stage(pioneer_observability::GatewayStartupStage::ConfigLoad);
    let mut config = AppConfig::load()?;
    config_stage.succeed();

    let runtime_stage = startup.stage(pioneer_observability::GatewayStartupStage::RuntimePrepare);
    let runtime_home = config.ensure_runtime_home_dir()?;
    info!(
        runtime_home = %runtime_home.display(),
        "runtime home directory is ready"
    );
    let identity_files_report = pioneer_promt::ensure_runtime_identity_files(&runtime_home)
        .context("failed to prepare runtime identity files")?;
    info!(
        created = identity_files_report.created.len(),
        existing = identity_files_report.existing.len(),
        "runtime identity files are ready"
    );
    runtime_stage.succeed();

    let settings_stage = startup.stage(pioneer_observability::GatewayStartupStage::SettingsLoad);
    let gateway_settings = load_gateway_settings(&runtime_home, &config)?;
    config = gateway_settings.apply_to_app_config(config);
    settings_stage.succeed();
    pioneer_observability::set_telemetry_enabled(config.gateway.telemetry.enabled);
    startup.bind_consent();
    let observability_stage =
        startup.stage(pioneer_observability::GatewayStartupStage::ObservabilityInit);
    match pioneer_observability::init_otlp_observability(
        pioneer_observability::OtlpTelemetryConfig {
            metrics_endpoint: config.gateway.telemetry.otlp_metrics_endpoint.clone(),
            traces_endpoint: config.gateway.telemetry.otlp_traces_endpoint.clone(),
            export_interval: Duration::from_millis(config.gateway.telemetry.export_interval_ms),
            export_timeout: Duration::from_millis(config.gateway.telemetry.export_timeout_ms),
            deployment_environment: None,
            service_version: None,
        },
    ) {
        Ok(()) => observability_stage.succeed(),
        Err(error) => {
            drop(observability_stage);
            warn!(
                error = %format!("{error:#}"),
                "gateway OTLP observability pipeline is unavailable"
            );
        }
    }
    let security_validate_stage =
        startup.stage(pioneer_observability::GatewayStartupStage::SecurityValidate);
    config
        .gateway
        .auth
        .validate_session_security()
        .context("invalid Gateway session security configuration")?;
    security_validate_stage.succeed();
    let voice_models = voice_model_catalog();
    info!(
        voice_model_count = voice_models.len(),
        "local voice model catalog is ready"
    );
    let secrets_open_stage = startup.stage(pioneer_observability::GatewayStartupStage::SecretsOpen);
    let gateway_secrets = Arc::new(GatewaySecrets::open(&runtime_home)?);
    secrets_open_stage.succeed();
    let database = initialize_database(&runtime_home, &config, startup).await?;

    let database_bootstrap_stage =
        startup.stage(pioneer_observability::GatewayStartupStage::DatabaseBootstrap);
    run_bootstrap(&database).await?;
    database_bootstrap_stage.succeed();
    let identity_bootstrap_stage =
        startup.stage(pioneer_observability::GatewayStartupStage::IdentityBootstrap);
    let identity_bootstrap = bootstrap_identity(&database)
        .await
        .context("failed to bootstrap stable Gateway identity")?;
    identity_bootstrap_stage.succeed();
    info!(
        gateway_created = identity_bootstrap.gateway_created,
        superuser_created = identity_bootstrap.superuser_created,
        bootstrap_version = identity_bootstrap
            .snapshot
            .gateway
            .identity_bootstrap_version,
        principal_threads_backfilled = identity_bootstrap.backfill_counts.principal_threads,
        system_threads_backfilled = identity_bootstrap.backfill_counts.system_threads,
        principal_turns_backfilled = identity_bootstrap.backfill_counts.principal_turns,
        system_turns_backfilled = identity_bootstrap.backfill_counts.system_turns,
        "stable Gateway identity is ready"
    );
    let identity_snapshot = Arc::new(identity_bootstrap.snapshot);
    let security_stage =
        startup.stage(pioneer_observability::GatewayStartupStage::SecurityInitialize);
    let access_jwt_material = gateway_secrets
        .load_or_create_access_jwt_signing_key(config.gateway.auth.secret_size_bytes)?;
    let credential_hmac_material = gateway_secrets
        .load_or_create_auth_credential_hmac_key(config.gateway.auth.secret_size_bytes)?;
    let auth = AuthAdmissionService::new(&config, &access_jwt_material, identity_snapshot.as_ref())
        .context("failed to initialize Gateway auth admission")?;
    let auth_service = Arc::new(
        GatewayAuthService::new(
            database.clone(),
            config.gateway.auth.clone(),
            identity_snapshot.clone(),
            &access_jwt_material,
            &credential_hmac_material,
        )
        .context("failed to initialize Gateway auth service")?,
    );
    const AUTH_STARTUP_EXPIRY_BATCH_SIZE: u64 = 256;
    let auth_startup_now = crate::helpers::unix_timestamp_secs()?;
    let mut expired_sessions = 0_u64;
    loop {
        let expired_batch = auth_service
            .expire_stale_sessions(auth_startup_now, AUTH_STARTUP_EXPIRY_BATCH_SIZE)
            .await
            .context("failed to expire stale auth sessions")?;
        expired_sessions = expired_sessions.saturating_add(expired_batch);
        if expired_batch < AUTH_STARTUP_EXPIRY_BATCH_SIZE {
            break;
        }
    }
    if expired_sessions != 0 {
        info!(
            expired_sessions,
            "stale auth sessions were expired before auth readiness"
        );
    }
    ensure_auth_readiness(&database, identity_snapshot.as_ref())
        .await
        .context("Gateway auth readiness failed")?;
    match gateway_secrets.purge_retired_superuser_jwt_tokens() {
        Ok(0) => {}
        Ok(deleted_credentials) => {
            info!(
                deleted_credentials,
                "retired Superuser JWT credentials removed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "failed to remove retired Superuser JWT credentials; cleanup will retry on the next Gateway start"
            );
        }
    }
    let expired_pending_sessions = auth_service
        .expire_pending_device_sessions(crate::helpers::unix_timestamp_secs()?, 256)
        .await
        .context("failed to expire pending device sessions")?;
    if expired_pending_sessions > 0 {
        info!(
            expired_sessions = expired_pending_sessions,
            "expired pending device sessions marked terminal"
        );
    }
    auth_service.spawn_auth_maintenance();
    security_stage.succeed();

    let services_initialize_stage =
        startup.stage(pioneer_observability::GatewayStartupStage::ServicesInitialize);
    let voice_input_desired_state = VoiceInputDesiredState::from_config(&config.gateway.voice);
    let voice_input_supervisor = create_voice_input_supervisor(
        Arc::new(FilesystemVoiceModelInstaller::new(
            config.clone(),
            runtime_home.clone(),
        )),
        Arc::new(EagerVoiceEngineLoader),
    );
    let remote_access_supervisor = Arc::new(RemoteAccessSupervisor::new(
        runtime_home.as_path(),
        config.gateway.remote_access.clone(),
    )?);
    let session_manager = Arc::new(SessionManager::new());
    auth_service.set_disconnect_hook(session_manager.clone());
    let workspace_manager = Arc::new(WorkspaceManager::new(database.clone()));
    let crud_store = Arc::new(CrudStore::new(database.clone()));
    let configured_agent_runtimes =
        crate::cli_runtime::config::load_effective_cli_runtime_instances(runtime_home.as_path())
            .context("failed to load CLI runtime catalog during Gateway startup")?;
    let agent_identity_settings =
        crate::cli_runtime::config::load_effective_cli_runtime_identity_settings(
            runtime_home.as_path(),
        )
        .context("failed to load CLI runtime identity settings during Gateway startup")?;
    let agent_identity_catalog = crate::identity::catalog::from_effective_settings(
        configured_agent_runtimes,
        &agent_identity_settings,
    )?;
    pioneer_crud::sync_cli_runtime_identity_catalog(
        &database,
        agent_identity_catalog.as_slice(),
        chrono::Utc::now().fixed_offset(),
    )
    .await
    .context("failed to synchronize Agent identities during Gateway startup")?;
    database::startup::enforce_execution_authority_integrity(crud_store.as_ref())
        .await
        .context("Gateway execution authority integrity gate failed")?;
    let thread_manager = Arc::new(ThreadManager::from_app_config(&config));

    migrate_legacy_provider_api_keys_to_current_workspace(
        workspace_manager.as_ref(),
        gateway_secrets.as_ref(),
    )
    .await;

    match garbage_collection_orphan_mcp_secrets(
        crud_store.as_ref(),
        gateway_secrets.as_ref(),
        false,
    )
    .await
    {
        Ok(report) if !report.failed_deletes.is_empty() => {
            warn!(
                active_refs = report.active_refs,
                stored_refs = report.stored_refs,
                orphan_refs = report.orphan_refs,
                deleted_refs = report.deleted_refs,
                failed_deletes = ?report.failed_deletes,
                "MCP secret orphan cleanup completed with delete failures"
            );
        }
        Ok(report) => {
            info!(
                active_refs = report.active_refs,
                stored_refs = report.stored_refs,
                orphan_refs = report.orphan_refs,
                deleted_refs = report.deleted_refs,
                "MCP secret orphan cleanup completed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "MCP secret orphan cleanup failed during startup"
            );
        }
    }

    set_artifact_external_ref_cache_backend(Arc::new(CrudArtifactExternalRefCacheBackend::new(
        crud_store.clone(),
    )));

    let provider_timeout_policy = ProviderTimeoutPolicy::from_secs(
        config.gateway.provider.connect_timeout_secs,
        config.gateway.provider.first_chunk_timeout_secs,
        config.gateway.provider.inter_chunk_idle_timeout_secs,
        config.gateway.provider.non_stream_request_timeout_secs,
        config.gateway.provider.max_stream_duration_secs,
    );

    let provider_registry = Arc::new(ProviderRegistry::new_scoped_with_timeout_policy_and_proxy(
        {
            let gateway_secrets = gateway_secrets.clone();
            move |workspace_id, provider_name| {
                workspace_id
                    .map(|workspace_id| {
                        gateway_secrets
                            .resolve_workspace_provider_api_key(workspace_id, provider_name)
                    })
                    .unwrap_or_else(|| gateway_secrets.resolve_provider_api_key(provider_name))
            }
        },
        {
            let gateway_secrets = gateway_secrets.clone();
            move |workspace_id, provider_name| {
                workspace_id.and_then(|workspace_id| {
                    gateway_secrets.resolve_workspace_provider_proxy(workspace_id, provider_name)
                })
            }
        },
        provider_timeout_policy,
    ));

    let summary_config = SummaryConfig {
        summary_model: config.gateway.thread.summary_model.clone(),
        summary_model_provider: config.gateway.thread.summary_model_provider.clone(),
        title_model: config.gateway.thread.title_model.clone(),
        title_model_provider: config.gateway.thread.title_model_provider.clone(),
    };

    let context_budget = ContextBudget {
        max_context_tokens: config.gateway.thread.max_context_tokens,
        response_reserve_tokens: config.gateway.thread.response_reserve_tokens,
    };

    let web_cfg = &config.gateway.tools.web;
    let computer_use_cfg = &config.gateway.tools.computer_use;
    let tool_retry_cfg = &config.gateway.tools.retry;
    let provider_attachments_cfg = &config.gateway.provider.attachments;
    let skills_cfg = &config.gateway.skills;
    let runtime_home_directory = runtime_home.display().to_string();

    let provider_attachment_roots = expand_home_directory_templates(
        provider_attachments_cfg.allowed_path_roots.as_slice(),
        &runtime_home_directory,
    )
    .into_iter()
    .map(std::path::PathBuf::from)
    .collect::<Vec<_>>();

    set_default_attachment_pipeline_config(AttachmentPipelineConfig {
        max_bytes_per_attachment: provider_attachments_cfg.max_bytes_per_attachment,
        max_total_bytes_per_request: provider_attachments_cfg.max_total_bytes_per_request,
        max_attachments_per_request: provider_attachments_cfg.max_attachments_per_request,
        upload_preferred_min_bytes: provider_attachments_cfg.upload_preferred_min_bytes,
        upload_registry: ArtifactExternalRefCachePolicy {
            enabled: provider_attachments_cfg.upload_registry_enabled,
            ttl_secs: provider_attachments_cfg.upload_registry_ttl_secs,
        },
        security: AttachmentSecurityPolicy {
            enforce_path_allowlist: provider_attachments_cfg.enforce_path_allowlist,
            allowed_path_roots: provider_attachment_roots,
            allow_url_sources: provider_attachments_cfg.allow_url_sources,
            allow_http: provider_attachments_cfg.allow_http,
            allow_private_network: provider_attachments_cfg.allow_private_network,
            max_url_redirects: provider_attachments_cfg.max_url_redirects,
            url_fetch_timeout_ms: provider_attachments_cfg.url_fetch_timeout_ms,
            url_fetch_max_bytes: provider_attachments_cfg.url_fetch_max_bytes,
            url_allowed_domains: provider_attachments_cfg.url_allowed_domains.clone(),
            url_blocked_domains: provider_attachments_cfg.url_blocked_domains.clone(),
            dry_run: provider_attachments_cfg.security_dry_run,
        },
        normalization: AttachmentNormalizationPolicy {
            strict_mime_match: provider_attachments_cfg.strict_mime_match,
            max_base64_chars: provider_attachments_cfg.max_base64_chars,
            max_filename_chars: provider_attachments_cfg.max_filename_chars,
        },
        runtime: AttachmentRuntimePolicy {
            retry: AttachmentRetryPolicy {
                max_attempts: provider_attachments_cfg.retry_max_attempts,
                initial_backoff_ms: provider_attachments_cfg.retry_initial_backoff_ms,
                max_backoff_ms: provider_attachments_cfg.retry_max_backoff_ms,
                jitter_ms: provider_attachments_cfg.retry_jitter_ms,
            },
            circuit_breaker: AttachmentCircuitBreakerPolicy {
                failure_threshold: provider_attachments_cfg.circuit_breaker_failure_threshold,
                open_ms: provider_attachments_cfg.circuit_breaker_open_ms,
            },
        },
    });

    let skill_system_roots =
        match system_skills::materialize_bundled_system_skill_roots(runtime_home.as_path()) {
            Ok(roots) => roots,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "failed to materialize bundled system skills"
                );
                Vec::new()
            }
        };
    let skill_system_import_roots = expand_home_directory_templates(
        skills_cfg.paths.system.as_slice(),
        &runtime_home_directory,
    );
    let skill_user_import_roots =
        expand_home_directory_templates(skills_cfg.paths.user.as_slice(), &runtime_home_directory);
    let skill_registry_import_roots = expand_home_directory_templates(
        skills_cfg.paths.registry.as_slice(),
        &runtime_home_directory,
    );
    let skill_user_roots = vec![
        runtime_home
            .join("skills/workspace/{workspaceId}/user")
            .display()
            .to_string(),
    ];
    let skill_registry_roots = vec![
        runtime_home
            .join("skills/workspace/{workspaceId}/registry")
            .display()
            .to_string(),
    ];
    let min_trust_for_shell_tools = parse_skill_trust_level(
        skills_cfg.security.min_trust_for_shell_tools.as_str(),
        "gateway.skills.security.min_trust_for_shell_tools",
    )?;
    let min_trust_for_http_tools = parse_skill_trust_level(
        skills_cfg.security.min_trust_for_http_tools.as_str(),
        "gateway.skills.security.min_trust_for_http_tools",
    )?;
    let min_trust_for_function_proxy_tools = parse_skill_trust_level(
        skills_cfg
            .security
            .min_trust_for_function_proxy_tools
            .as_str(),
        "gateway.skills.security.min_trust_for_function_proxy_tools",
    )?;

    let tool_loop_config = ToolLoopConfig {
        provider: provider_timeout_policy,
        preflight: pioneer_agent::PreflightLoopConfig {
            provider_name: config.gateway.preflight_model.model_provider_override(),
            model: config.gateway.preflight_model.model_override(),
            timeout_ms: None,
            max_output_chars: None,
        },
        web: WebToolsConfig {
            default_timeout_ms: web_cfg.default_timeout_ms,
            hard_max_timeout_ms: web_cfg.hard_max_timeout_ms,
            default_fetch_max_bytes: web_cfg.default_fetch_max_bytes,
            hard_fetch_max_bytes: web_cfg.hard_fetch_max_bytes,
            default_download_max_bytes: web_cfg.default_download_max_bytes,
            hard_download_max_bytes: web_cfg.hard_download_max_bytes,
            default_max_results: web_cfg.default_max_results,
            hard_max_results: web_cfg.hard_max_results,
            default_snippet_chars: web_cfg.default_snippet_chars,
            hard_max_snippet_chars: web_cfg.hard_max_snippet_chars,
            default_link_count: web_cfg.default_link_count,
            hard_link_count: web_cfg.hard_link_count,
            default_render_max_chars: web_cfg.default_render_max_chars,
            ddg_html_search_url: web_cfg.ddg_html_search_url.clone(),
            ddg_instant_api_url: web_cfg.ddg_instant_api_url.clone(),
            default_user_agent: web_cfg.default_user_agent.clone(),
        },
        computer_use: ComputerUseToolsConfig {
            runtime_home_dir: runtime_home.clone(),
            artifacts_subdir: computer_use_cfg.artifacts_subdir.clone(),
            retention_hours: computer_use_cfg.retention_hours,
            max_total_bytes: computer_use_cfg.max_total_bytes,
            run_max_steps_default: computer_use_cfg.run_max_steps_default,
            snapshot_transport_max_bytes: computer_use_cfg.snapshot_transport_max_bytes,
            snapshot_transport_max_side_px: computer_use_cfg.snapshot_transport_max_side_px,
            snapshot_transport_min_side_px: computer_use_cfg.snapshot_transport_min_side_px,
            snapshot_downscale_factor: computer_use_cfg.snapshot_downscale_factor,
            accessibility_tree_max_depth: computer_use_cfg.accessibility_tree_max_depth,
            accessibility_tree_max_nodes: computer_use_cfg.accessibility_tree_max_nodes,
            accessibility_tree_max_serialized_bytes: computer_use_cfg
                .accessibility_tree_max_serialized_bytes,
            accessibility_tree_text_max_chars: computer_use_cfg.accessibility_tree_text_max_chars,
            semantic_action_timeout_ms: computer_use_cfg.semantic_action_timeout_ms,
            app_activation_timeout_ms: computer_use_cfg.app_activation_timeout_ms,
            input_simulation_enabled: computer_use_cfg.input_simulation_enabled,
            launch_if_missing_default: computer_use_cfg.launch_if_missing_default,
            allowed_launch_commands: computer_use_cfg.allowed_launch_commands.clone(),
            preflight_screenshot_probe_enabled: computer_use_cfg.preflight_screenshot_probe_enabled,
            max_consecutive_same_snapshot_hash: computer_use_cfg.max_consecutive_same_snapshot_hash,
            max_consecutive_same_action_signature: computer_use_cfg
                .max_consecutive_same_action_signature,
            max_consecutive_no_progress_steps: computer_use_cfg.max_consecutive_no_progress_steps,
            max_recovery_attempts_per_step: computer_use_cfg.max_recovery_attempts_per_step,
            max_recovery_attempts_per_run: computer_use_cfg.max_recovery_attempts_per_run,
        },
        skills: pioneer_agent::SkillsLoopConfig {
            enabled: skills_cfg.enabled,
            max_skills_per_source: skills_cfg.max_skills_per_source,
            max_skill_file_bytes: skills_cfg.max_skill_file_bytes,
            prompt_max_chars: skills_cfg.prompt_max_chars,
            allow_implicit_invocation: skills_cfg.allow_implicit_invocation,
            system_roots: skill_system_roots,
            user_roots: skill_user_roots,
            registry_roots: skill_registry_roots,
            system_import_roots: skill_system_import_roots,
            user_import_roots: skill_user_import_roots,
            registry_import_roots: skill_registry_import_roots,
            validation: pioneer_agent::SkillsValidationLoopConfig {
                strict_agentskills: skills_cfg.validation.strict_agentskills,
                accept_openclaw_profile: skills_cfg.validation.accept_openclaw_profile,
            },
            security: pioneer_agent::SkillsSecurityLoopConfig {
                allow_untrusted_install: skills_cfg.security.allow_untrusted_install,
                min_trust_for_shell_tools,
                min_trust_for_http_tools,
                min_trust_for_function_proxy_tools,
                max_install_archive_bytes: skills_cfg
                    .security
                    .max_install_archive_uncompressed_bytes,
                max_install_archive_compressed_bytes: skills_cfg
                    .security
                    .max_install_archive_compressed_bytes,
                max_install_archive_uncompressed_bytes: skills_cfg
                    .security
                    .max_install_archive_uncompressed_bytes,
                max_install_archive_entries: skills_cfg.security.max_install_archive_entries,
                max_install_file_bytes: skills_cfg.security.max_install_file_bytes,
                upload_ttl_secs: skills_cfg.security.upload_ttl_secs,
                upload_recommended_chunk_size_bytes: skills_cfg
                    .security
                    .upload_recommended_chunk_size_bytes,
                upload_max_chunk_size_bytes: skills_cfg.security.upload_max_chunk_size_bytes,
            },
            dependencies: pioneer_agent::SkillsDependenciesLoopConfig {
                preflight_on_resolve: skills_cfg.dependencies.preflight_on_resolve,
                runtime_recheck_on_tool_call: skills_cfg.dependencies.runtime_recheck_on_tool_call,
            },
            runtime: pioneer_agent::SkillsRuntimeLoopConfig {
                enable_dynamic_tools: skills_cfg.runtime.enable_dynamic_tools,
                enable_read_skill: skills_cfg.runtime.enable_read_skill,
                max_dynamic_tools_per_skill: skills_cfg.runtime.max_dynamic_tools_per_skill,
                read_skill_max_chars: skills_cfg.runtime.read_skill_max_chars,
                compact_mode_threshold: skills_cfg.runtime.compact_mode_threshold,
                allow_shell_tools: skills_cfg.runtime.allow_shell_tools,
                allow_http_tools: skills_cfg.runtime.allow_http_tools,
                allow_function_proxy_tools: skills_cfg.runtime.allow_function_proxy_tools,
            },
        },
        memory: memory_loop_config_from_gateway_memory_config(&config.gateway.memory),
        budget: ToolLoopBudgetConfig {
            max_agent_rounds_per_turn: config.gateway.tools.budget.max_agent_rounds_per_turn,
            max_tool_calls_per_turn: config.gateway.tools.budget.max_tool_calls_per_turn,
        },
        execution_windows: execution_windows_config_from_gateway_tools_config(
            &config.gateway.tools,
        ),
        retry: ToolRetryBudgetConfig {
            max_recoverable_retry_rounds_per_episode: tool_retry_cfg
                .max_recoverable_retry_rounds_per_episode,
            max_same_tool_error_retries_per_episode: tool_retry_cfg
                .max_same_tool_error_retries_per_episode,
            max_retries_per_tool_name_per_episode: tool_retry_cfg
                .max_retries_per_tool_name_per_episode,
        },
    };

    let memory_runtime = Arc::new(GatewayMemoryRuntime::from_config(
        crud_store.clone(),
        runtime_home.as_path(),
        &config.gateway.memory,
    )?);

    let task_runtime_config = task_runtime_config_from_gateway_tasks_config(&config.gateway.tasks);

    let thread_episodic_storage_root = config
        .gateway
        .memory
        .resolve_capsules_root(runtime_home.as_path())?;
    let thread_episodic_workspace_vector_search_configs =
        gateway_settings.workspace_thread_episodic_vector_search_configs();
    let self_improvement_workspace_configs = gateway_settings.workspace_self_improvement_configs();
    let cli_runtime_crud_store = crud_store.clone();
    let cli_mcp_limits = crate::cli_runtime::mcp::limits::CliMcpRuntimeLimits::new(
        config.gateway.cli_agent_runtime.mcp_tools.max_tools,
        config
            .gateway
            .cli_agent_runtime
            .mcp_tools
            .max_total_schema_bytes,
        config
            .gateway
            .cli_agent_runtime
            .mcp_tools
            .max_concurrent_calls_per_turn,
    )
    .context("invalid Gateway CLI MCP runtime limits")?;

    let self_improvement_supervisor = Arc::new(SelfImprovementSupervisor::new(
        crud_store.clone(),
        provider_registry.clone(),
        workspace_manager.clone(),
        self_improvement_workspace_configs,
        config.gateway.skills.max_skill_file_bytes,
    ));

    let invitation_gateway_base_url = pioneer_protocol::GatewayBaseUrl::from_local_listen_addr(
        config.gateway.listen_addr.as_str(),
    )
    .context("invalid Gateway listener address for invitation presentation")?;
    let context_compaction_timeout_config = config.gateway.resilience.context_compaction;
    let mut message_processor = MessageProcessor::new_with_memory_runtime_and_task_config(
        thread_manager,
        provider_registry.clone(),
        session_manager.clone(),
        workspace_manager.clone(),
        crud_store.clone(),
        gateway_secrets.clone(),
        summary_config,
        context_budget,
        tool_loop_config,
        memory_runtime,
        runtime_home.clone(),
        thread_episodic_storage_root.clone(),
        config.gateway.artifacts.clone(),
        task_runtime_config,
        thread_episodic_runtime_config_from_gateway_config(&config.gateway.thread_episodic),
        MessageProcessorResilienceConfig {
            command_execution_timeout: config.gateway.resilience.command_execution,
            provider_stream_item_timeout: config.gateway.resilience.provider_stream_items,
            context_compaction_timeout: context_compaction_timeout_config,
            cli_runtime_command_heartbeat: config.gateway.cli_agent_runtime.command_heartbeat,
        },
    )
    .with_cli_mcp_limits(cli_mcp_limits)
    .with_invitation_gateway_base_url(invitation_gateway_base_url);
    let cli_runtime_manager = build_cli_runtime_manager(
        &runtime_home,
        &config,
        cli_mcp_limits,
        message_processor.cli_turn_mcp_invoker(),
        cli_runtime_crud_store,
    )?;
    if let Some(cli_runtime_manager) = cli_runtime_manager {
        message_processor = message_processor.with_cli_runtime_manager(cli_runtime_manager);
    }
    message_processor.apply_thread_episodic_workspace_vector_search_configs(
        thread_episodic_workspace_vector_search_configs.clone(),
    );
    message_processor =
        message_processor.with_voice_input_supervisor(voice_input_supervisor.clone());
    message_processor =
        message_processor.with_self_improvement_supervisor(self_improvement_supervisor.clone());
    message_processor =
        message_processor.with_remote_access_supervisor(remote_access_supervisor.clone());
    message_processor = message_processor.with_auth_service(auth_service.clone());
    let message_processor = Arc::new(message_processor);
    services_initialize_stage.succeed();
    let agent_domain_upgrade_stage =
        startup.stage(pioneer_observability::GatewayStartupStage::AgentDomainUpgrade);
    database::startup::upgrade_agent_domain_data(message_processor.as_ref())
        .await
        .context("failed to complete the blocking Agent domain data upgrade")?;
    agent_domain_upgrade_stage.succeed();
    let services_prepare_stage =
        startup.stage(pioneer_observability::GatewayStartupStage::ServicesPrepare);
    auth_service.set_invitation_accept_post_commit_hook(message_processor.clone());
    message_processor
        .apply_keepawake_setting(config.gateway.keepawake)
        .context("failed to apply gateway keepawake setting")?;
    message_processor
        .set_hook_recovery_config(config.gateway.hooks.recovery.clone())
        .await;
    message_processor.bind_agent_tool_bridges().await;
    message_processor.bind_memory_bridge_if_enabled().await;
    message_processor
        .cleanup_stale_skill_uploads(now_timestamp_secs())
        .await;
    let remote_access_config = config.gateway.remote_access.clone();
    let startup_thread_episodic_vector_search_config =
        config.gateway.thread_episodic.vector_search.clone();
    let telemetry_shutdown_timeout =
        Duration::from_millis(config.gateway.telemetry.export_timeout_ms);
    services_prepare_stage.succeed();
    let listener_bind_stage =
        startup.stage(pioneer_observability::GatewayStartupStage::ListenerBind);
    let handle = spawn_server(
        config,
        auth,
        auth_service,
        message_processor.clone(),
        session_manager,
    )
    .await?;
    listener_bind_stage.succeed();
    let services_start_stage =
        startup.stage(pioneer_observability::GatewayStartupStage::ServicesStart);
    let services_start_result: Result<()> = async {
        let stage =
            startup.stage(pioneer_observability::GatewayStartupStage::ServicesVoiceInputStart);
        start_voice_input_supervisor(&voice_input_supervisor, voice_input_desired_state)?;
        stage.succeed();

        let stage =
            startup.stage(pioneer_observability::GatewayStartupStage::ServicesSelfImprovementStart);
        self_improvement_supervisor
            .start()
            .await
            .context("failed to start self-improvement supervisor")?;
        stage.succeed();

        let stage =
            startup.stage(pioneer_observability::GatewayStartupStage::ServicesNotificationsStart);
        message_processor.start_remote_access_status_notifications();
        message_processor.start_voice_input_status_notifications();
        message_processor.start_thread_episodic_vector_refill_status_notifications();
        stage.succeed();

        let stage =
            startup.stage(pioneer_observability::GatewayStartupStage::ServicesResilienceStart);
        message_processor
            .start_resilience_workers()
            .await
            .context("failed to start resilience workers")?;
        stage.succeed();

        let stage = startup.stage(pioneer_observability::GatewayStartupStage::ServicesMcpStart);
        message_processor.start_mcp_workspace_supervisor().await;
        stage.succeed();

        let stage =
            startup.stage(pioneer_observability::GatewayStartupStage::ServicesSkillsWatcherStart);
        message_processor.start_skills_watcher().await;
        stage.succeed();

        let stage =
            startup.stage(pioneer_observability::GatewayStartupStage::ServicesDatabaseWorkersStart);
        // Long-running migrations and backfills start only after the Gateway listener exists.
        database::startup::spawn(
            message_processor.clone(),
            message_processor.crud_store.clone(),
            thread_episodic_storage_root.clone(),
            startup_thread_episodic_vector_search_config,
            thread_episodic_workspace_vector_search_configs.clone(),
            provider_registry.clone(),
            runtime_home.clone(),
            Some(message_processor.thread_episodic_vector_refill_status_sender()),
            message_processor.thread_episodic_workspace_refill_supervisor(),
            context_compaction_timeout_config,
        );
        database::maintenance::spawn(message_processor.crud_store.clone());
        stage.succeed();

        let stage =
            startup.stage(pioneer_observability::GatewayStartupStage::ServicesRemoteAccessStart);
        remote_access_supervisor
            .apply(remote_access_desired_state(
                &remote_access_config,
                &gateway_settings,
                gateway_secrets.as_ref(),
            )?)
            .await
            .context("failed to apply initial remote access settings")?;
        stage.succeed();

        let stage = startup
            .stage(pioneer_observability::GatewayStartupStage::ServicesProviderReadinessStart);
        message_processor
            .start_provider_readiness_supervisor()
            .await;
        stage.succeed();

        Ok(())
    }
    .await;
    let runtime_result = match services_start_result {
        Ok(()) => {
            services_start_stage.succeed();
            startup.finish_success();
            info!(listen_addr = %handle.local_addr(), "gateway daemon started");
            wait_for_shutdown_signal().await
        }
        Err(error) => {
            drop(services_start_stage);
            startup.finish_failure();
            Err(error)
        }
    };

    info!("gateway daemon stopping with telemetry snapshot");
    message_processor
        .shutdown_provider_readiness_supervisor()
        .await;
    let patch_telemetry = pioneer_tools::apply_patch::patch_telemetry().snapshot();
    info!(
        calls = patch_telemetry.calls,
        applied = patch_telemetry.applied,
        partial = patch_telemetry.partial,
        rejected = patch_telemetry.rejected,
        failed = patch_telemetry.failed,
        uncertain = patch_telemetry.uncertain,
        committed_changes = patch_telemetry.committed_changes,
        committed_bytes = patch_telemetry.committed_bytes,
        exact_reports = patch_telemetry.exact_reports,
        inexact_reports = patch_telemetry.inexact_reports,
        pending_tracking = patch_telemetry.pending_tracking,
        tracker_publication_failures = patch_telemetry.tracker_publication_failures,
        duplicate_suppressions = patch_telemetry.duplicate_suppressions,
        "gateway daemon stopping with apply_patch telemetry snapshot"
    );
    message_processor.shutdown_remote_access_supervisor().await;
    let server_shutdown_result = handle.shutdown().await;
    message_processor.shutdown_cli_runtime_manager().await;
    message_processor.shutdown_mcp_service().await;
    self_improvement_supervisor.shutdown().await;
    let database_close_result = database
        .close()
        .await
        .context("failed to close gateway database connection");
    match tokio::task::spawn_blocking(move || {
        pioneer_observability::shutdown_observability(telemetry_shutdown_timeout)
    })
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(
            error = %format!("{error:#}"),
            "failed to shut down gateway OTLP observability pipeline"
        ),
        Err(error) => warn!(
            error = %error,
            "gateway OTLP observability shutdown worker failed"
        ),
    }

    runtime_result?;
    server_shutdown_result?;
    database_close_result?;
    Ok(())
}

// TODO: Remove sometime
async fn migrate_legacy_provider_api_keys_to_current_workspace(
    workspace_manager: &WorkspaceManager,
    gateway_secrets: &GatewaySecrets,
) {
    let workspaces = match workspace_manager.list_workspaces().await {
        Ok(workspaces) => workspaces,
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "failed to list workspaces for provider key migration"
            );
            return;
        }
    };

    let Some(workspace) = workspaces
        .iter()
        .find(|workspace| workspace.is_active && workspace.is_current)
        .or_else(|| workspaces.iter().find(|workspace| workspace.is_active))
    else {
        warn!("provider key migration skipped because no active workspace exists");
        return;
    };

    match gateway_secrets.migrate_legacy_provider_api_keys_to_workspace(workspace.id.as_str()) {
        Ok(report)
            if report.copied > 0 || report.skipped_existing > 0 || report.deleted_legacy > 0 =>
        {
            info!(
                workspace_id = report.workspace_id.as_str(),
                legacy_keys = report.legacy_keys,
                copied = report.copied,
                skipped_existing = report.skipped_existing,
                deleted_legacy = report.deleted_legacy,
                "legacy provider api keys migrated into workspace scope"
            );
        }
        Ok(_) => {}
        Err(error) => {
            warn!(
                workspace_id = workspace.id.as_str(),
                error = %format!("{error:#}"),
                "failed to migrate legacy provider api keys into workspace scope"
            );
        }
    }
}

pub async fn create_device(
    config: &AppConfig,
    runtime_home: &Path,
) -> Result<AuthDeviceCreateResponse> {
    let gateway_secrets = GatewaySecrets::open(runtime_home)?;
    let database = database::initialize_existing_for_operations(runtime_home, config)
        .await?
        .context("Gateway database must exist before creating a device")?;
    let identity = Arc::new(
        bootstrap_identity(&database)
            .await
            .context("failed to load stable Gateway identity")?
            .snapshot,
    );
    ensure_auth_readiness(&database, identity.as_ref())
        .await
        .context("Gateway auth readiness failed")?;
    let access_key = gateway_secrets
        .load_or_create_access_jwt_signing_key(config.gateway.auth.secret_size_bytes)?;
    let credential_hmac_key = gateway_secrets
        .load_or_create_auth_credential_hmac_key(config.gateway.auth.secret_size_bytes)?;
    if let Err(error) = gateway_secrets.purge_retired_superuser_jwt_tokens() {
        warn!(
            error = %format!("{error:#}"),
            "failed to remove retired Superuser JWT credentials; device creation can continue because retired credentials are not accepted"
        );
    }
    let service = GatewayAuthService::new(
        database,
        config.gateway.auth.clone(),
        identity,
        &access_key,
        &credential_hmac_key,
    )
    .context("failed to initialize Gateway auth service")?;
    service.create_local_device().await.map_err(Into::into)
}

fn load_gateway_settings(runtime_home: &Path, config: &AppConfig) -> Result<GatewaySettings> {
    let settings_file_name =
        normalize_settings_file_name(config.gateway.settings_file_name.as_str())?;
    let settings_path = runtime_home.join(settings_file_name.as_str());
    load_or_create_gateway_settings(
        &settings_path,
        config.gateway.settings_version,
        settings_file_name.as_str(),
    )
}

fn remote_access_desired_state(
    config: &pioneer_config::GatewayRemoteAccessConfig,
    settings: &GatewaySettings,
    gateway_secrets: &GatewaySecrets,
) -> Result<RemoteAccessDesiredState> {
    let key = match settings.remote_access_secret_ref() {
        Some(secret_ref) => gateway_secrets.get_remote_access_secret(secret_ref)?,
        None => None,
    };
    let has_key = key.is_some();
    Ok(RemoteAccessDesiredState {
        settings: settings.effective_remote_access_settings(
            config,
            has_key,
            pioneer_protocol::GatewayRemoteAccessStatusSnapshot::default(),
        ),
        key,
    })
}

fn build_cli_runtime_manager(
    runtime_home: &Path,
    config: &AppConfig,
    mcp_limits: crate::cli_runtime::mcp::limits::CliMcpRuntimeLimits,
    turn_mcp_invoker: Arc<dyn crate::turn_mcp::invoker::TurnMcpInvoker>,
    crud_store: Arc<CrudStore>,
) -> Result<Option<Arc<crate::cli_runtime::manager::CLIAgentRuntimeManager>>> {
    crate::cli_runtime::codex_session::cli_runtime_manager(
        runtime_home.to_path_buf(),
        Duration::from_secs(config.gateway.cli_agent_runtime.idle_session_ttl_secs),
        mcp_limits,
        turn_mcp_invoker,
        crud_store,
    )
    .map(Some)
}

fn memory_loop_config_from_gateway_memory_config(config: &GatewayMemoryConfig) -> MemoryLoopConfig {
    let mut memory = MemoryLoopConfig::default();
    memory.deterministic_recall_enabled = config.enabled && config.deterministic_recall_enabled;
    memory.tools_enabled = config.enabled && config.tools_enabled;

    if !config.enabled || !config.active_recall_enabled || !memory.deterministic_recall_enabled {
        memory.active_recall.mode = MemoryActiveRecallMode::Disabled;
    }

    memory.post_turn_extractor.enabled = config.enabled && config.proactive_writes_enabled;
    memory.post_turn_extractor.provider_enabled = config.enabled && config.proactive_writes_enabled;
    memory.post_turn_extractor.proactive_writes_enabled =
        config.enabled && config.proactive_writes_enabled;
    memory.post_turn_extractor.provider_name =
        config.proactive_writes_model.model_provider_override();
    memory.post_turn_extractor.model = config.proactive_writes_model.model_override();
    memory.post_turn_extractor.await_policy = if config.background_extraction_enabled {
        HookAwaitPolicy::FireAndRecord
    } else {
        HookAwaitPolicy::Blocking
    };
    memory.post_turn_extractor.strict_debug =
        config.debug_trace_enabled || config.strict_diagnostics_enabled;

    memory
}

fn task_runtime_config_from_gateway_tasks_config(config: &GatewayTasksConfig) -> TaskRuntimeConfig {
    TaskRuntimeConfig {
        review: TaskReviewRuntimeConfig {
            enabled: config.review.enabled,
            allow_task_create_review_policy: config.review.allow_task_create_review_policy,
            default_parent_review_for_immediate_attached_agent_tasks: config
                .review
                .default_parent_review_for_immediate_attached_agent_tasks,
            default_max_revision_rounds: config.review.default_max_revision_rounds,
            auto_accept_after_seconds: config.review.auto_accept_after_seconds,
        },
    }
}

fn execution_windows_config_from_gateway_tools_config(
    config: &GatewayToolsConfig,
) -> ExecutionWindowsConfig {
    match config.execution_windows.as_ref() {
        Some(execution_windows) => {
            execution_windows_config_from_gateway_execution_windows_config(execution_windows)
        }
        None => legacy_execution_windows_config_from_gateway_budget_config(&config.budget),
    }
}

fn execution_windows_config_from_gateway_execution_windows_config(
    config: &GatewayExecutionWindowsConfig,
) -> ExecutionWindowsConfig {
    ExecutionWindowsConfig {
        window: ExecutionWindowBudgetConfig {
            max_agent_rounds_per_window: config.window.max_agent_rounds_per_window,
            max_tool_calls_per_window: config.window.max_tool_calls_per_window,
            max_wall_clock_ms_per_window: config.window.max_wall_clock_ms_per_window,
            max_provider_tokens_per_window: config.window.max_provider_tokens_per_window,
        },
        total: ExecutionWindowTotalBudgetConfig {
            max_windows_per_turn: config.total.max_windows_per_turn,
            max_tool_calls_per_turn: config.total.max_tool_calls_per_turn,
            max_wall_clock_ms_per_turn: config.total.max_wall_clock_ms_per_turn,
            max_provider_tokens_per_turn: config.total.max_provider_tokens_per_turn,
            max_consecutive_no_progress_windows: config.total.max_consecutive_no_progress_windows,
        },
    }
    .normalized()
}

fn legacy_execution_windows_config_from_gateway_budget_config(
    config: &GatewayToolLoopBudgetConfig,
) -> ExecutionWindowsConfig {
    let window_default = ExecutionWindowBudgetConfig::default();
    let total_default = ExecutionWindowTotalBudgetConfig::default();
    ExecutionWindowsConfig {
        window: ExecutionWindowBudgetConfig {
            max_agent_rounds_per_window: config.max_agent_rounds_per_turn,
            max_tool_calls_per_window: config.max_tool_calls_per_turn,
            max_wall_clock_ms_per_window: window_default.max_wall_clock_ms_per_window,
            max_provider_tokens_per_window: None,
        },
        total: ExecutionWindowTotalBudgetConfig { ..total_default },
    }
    .normalized()
}

pub(crate) fn memory_loop_config_from_gateway_memory_settings(
    settings: &GatewayMemorySettings,
) -> MemoryLoopConfig {
    let mut memory = MemoryLoopConfig::default();
    memory.deterministic_recall_enabled = settings.enabled && settings.deterministic_recall_enabled;
    memory.tools_enabled = settings.enabled && settings.tools_enabled;

    if !settings.enabled || !settings.active_recall_enabled || !memory.deterministic_recall_enabled
    {
        memory.active_recall.mode = MemoryActiveRecallMode::Disabled;
    }

    memory.post_turn_extractor.enabled = settings.enabled && settings.proactive_writes_enabled;
    memory.post_turn_extractor.provider_enabled =
        settings.enabled && settings.proactive_writes_enabled;
    memory.post_turn_extractor.proactive_writes_enabled =
        settings.enabled && settings.proactive_writes_enabled;
    memory.post_turn_extractor.provider_name =
        settings.proactive_writes_model.model_provider_override();
    memory.post_turn_extractor.model = settings.proactive_writes_model.model_override();
    memory.post_turn_extractor.await_policy = if settings.background_extraction_enabled {
        HookAwaitPolicy::FireAndRecord
    } else {
        HookAwaitPolicy::Blocking
    };
    memory.post_turn_extractor.strict_debug =
        settings.debug_trace_enabled || settings.strict_diagnostics_enabled;

    memory
}

fn thread_episodic_runtime_config_from_gateway_config(
    config: &GatewayThreadEpisodicConfig,
) -> ThreadEpisodicRuntimeConfig {
    let vector_search_enabled = config.vector_search.has_selected_embedding_model();
    ThreadEpisodicRuntimeConfig {
        enabled: config.enabled,
        indexing_enabled: config.indexing_enabled,
        recall_enabled: config.recall_enabled,
        vector_search_enabled,
        vector_search: config.vector_search.clone(),
        hook_max_prompt_chars: config.default_prompt_chars,
        hook_max_candidates: config.default_max_candidates,
        index_executor: ThreadEpisodicIndexExecutorConfig {
            batch_limit: config.index_batch_limit,
            retry_base_delay_secs: config.retry_base_delay_secs,
            retry_max_delay_secs: config.retry_max_delay_secs,
            max_attempts: config.max_attempts,
            near_capacity_percent: config.near_capacity_percent,
        },
        recall_service: ThreadEpisodicRecallServiceConfig {
            enabled: config.enabled && config.recall_enabled,
            vector_search_enabled,
            vector_search: config.vector_search.clone(),
            default_prompt_chars: config.default_prompt_chars,
            max_prompt_chars: config.max_prompt_chars,
            max_hit_chars: config.max_hit_chars,
            default_max_candidates: config.default_max_candidates,
            max_candidate_work: config.max_candidate_work,
            max_segments: config.max_segments,
            min_relevancy: config.min_relevancy,
            min_results: config.min_results,
            snippet_chars: config.snippet_chars,
        },
    }
}

pub(crate) fn thread_episodic_runtime_config_from_gateway_settings(
    settings: &GatewayThreadEpisodicSettings,
) -> ThreadEpisodicRuntimeConfig {
    let vector_search_enabled = settings.vector_search.has_selected_embedding_model();
    ThreadEpisodicRuntimeConfig {
        enabled: settings.enabled,
        indexing_enabled: settings.indexing_enabled,
        recall_enabled: settings.recall_enabled,
        vector_search_enabled,
        vector_search: settings.vector_search.clone(),
        hook_max_prompt_chars: settings.default_prompt_chars,
        hook_max_candidates: settings.default_max_candidates,
        index_executor: ThreadEpisodicIndexExecutorConfig {
            batch_limit: settings.index_batch_limit as u64,
            retry_base_delay_secs: settings.retry_base_delay_secs,
            retry_max_delay_secs: settings.retry_max_delay_secs,
            max_attempts: settings.max_attempts,
            near_capacity_percent: settings.near_capacity_percent,
        },
        recall_service: ThreadEpisodicRecallServiceConfig {
            enabled: settings.enabled && settings.recall_enabled,
            vector_search_enabled,
            vector_search: settings.vector_search.clone(),
            default_prompt_chars: settings.default_prompt_chars,
            max_prompt_chars: settings.max_prompt_chars,
            max_hit_chars: settings.max_hit_chars as usize,
            default_max_candidates: settings.default_max_candidates,
            max_candidate_work: settings.max_candidate_work,
            max_segments: settings.max_segments as u64,
            min_relevancy: settings.min_relevancy,
            min_results: settings.min_results,
            snippet_chars: settings.snippet_chars,
        },
    }
}

fn parse_skill_trust_level(raw: &str, field_name: &str) -> Result<SkillTrustLevel> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "internal" => Ok(SkillTrustLevel::Internal),
        "verified" => Ok(SkillTrustLevel::Verified),
        "community" => Ok(SkillTrustLevel::Community),
        "untrusted" => Ok(SkillTrustLevel::Untrusted),
        other => anyhow::bail!(
            "invalid trust level `{other}` for `{field_name}`; expected internal|verified|community|untrusted"
        ),
    }
}

fn expand_home_directory_templates(paths: &[String], runtime_home_directory: &str) -> Vec<String> {
    paths
        .iter()
        .map(|path| path.replace(HOME_DIRECTORY_TOKEN, runtime_home_directory))
        .collect()
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;

    tokio::select! {
        res = tokio::signal::ctrl_c() => {
            res?;
        }
        _ = terminate.recv() => {}
    }

    Ok(())
}

#[cfg(windows)]
async fn wait_for_shutdown_signal() -> Result<()> {
    use tokio::signal::windows::{ctrl_break, ctrl_c, ctrl_close, ctrl_logoff, ctrl_shutdown};

    let mut sig_ctrl_c = ctrl_c()?;
    let mut sig_ctrl_break = ctrl_break()?;
    let mut sig_ctrl_close = ctrl_close()?;
    let mut sig_ctrl_logoff = ctrl_logoff()?;
    let mut sig_ctrl_shutdown = ctrl_shutdown()?;

    tokio::select! {
        _ = sig_ctrl_c.recv() => {}
        _ = sig_ctrl_break.recv() => {}
        _ = sig_ctrl_close.recv() => {}
        _ = sig_ctrl_logoff.recv() => {}
        _ = sig_ctrl_shutdown.recv() => {}
    }

    Ok(())
}

#[cfg(not(any(unix, windows)))]
async fn wait_for_shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[cfg(test)]
mod runtime_tests {
    use super::{
        create_voice_input_supervisor, execution_windows_config_from_gateway_tools_config,
        expand_home_directory_templates, memory_loop_config_from_gateway_memory_config,
        parse_skill_trust_level, start_voice_input_supervisor,
        task_runtime_config_from_gateway_tasks_config,
        thread_episodic_runtime_config_from_gateway_settings,
    };
    use crate::secrets::GatewaySecrets;
    use crate::settings::GatewayThreadEpisodicSettings;
    use crate::voice::model_catalog::{VoiceModelCatalogEntry, VoiceModelInstallLayout};
    use crate::voice::model_install::{
        VoiceModelCleanupReport, VoiceModelInstallControl, VoiceModelInstallReport,
    };
    use crate::voice::runtime::LoadedVoiceEngine;
    use crate::voice::supervisor::{
        VoiceEngineLoader, VoiceInputDesiredState, VoiceModelInstaller,
    };
    use crate::voice::transcription::{VoiceTranscriptionError, VoiceTranscriptionErrorKind};
    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use pioneer_config::{
        GatewayExecutionWindowBudgetConfig, GatewayExecutionWindowTotalBudgetConfig,
        GatewayExecutionWindowsConfig, GatewayMemoryConfig, GatewayMemoryModelSelectionConfig,
        GatewayTasksConfig, GatewayToolLoopBudgetConfig, GatewayToolRetryBudgetConfig,
        GatewayToolsConfig,
    };
    use pioneer_hooks::HookAwaitPolicy;
    use pioneer_keystore::MemorySecretStore;
    use pioneer_memory::hooks::MemoryActiveRecallMode;
    use pioneer_protocol::{GatewayVoiceInputProvider, GatewayVoiceInputRuntimePhase};
    use pioneer_skills::SkillTrustLevel;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    struct StartupProbeInstaller {
        inspections: AtomicUsize,
        inspected: Notify,
    }

    impl StartupProbeInstaller {
        fn new() -> Self {
            Self {
                inspections: AtomicUsize::new(0),
                inspected: Notify::new(),
            }
        }
    }

    #[async_trait]
    impl VoiceModelInstaller for StartupProbeInstaller {
        fn verified_install_layout(
            &self,
            _entry: &VoiceModelCatalogEntry,
        ) -> Result<Option<VoiceModelInstallLayout>> {
            self.inspections.fetch_add(1, Ordering::SeqCst);
            self.inspected.notify_one();
            bail!("startup probe stops before model I/O")
        }

        fn cleanup_non_selected(
            &self,
            _selected_model_id: &str,
            _cancellation: &CancellationToken,
            _protected_model_id: &std::sync::RwLock<Option<String>>,
        ) -> Result<VoiceModelCleanupReport> {
            bail!("startup probe must not clean model installs")
        }

        fn cleanup_disabled(
            &self,
            _protected_model_id: &std::sync::RwLock<Option<String>>,
        ) -> Result<VoiceModelCleanupReport> {
            bail!("startup probe must not clean disabled model installs")
        }

        async fn install(
            &self,
            _entry: VoiceModelCatalogEntry,
            _force_fresh: bool,
            _control: VoiceModelInstallControl,
        ) -> Result<VoiceModelInstallReport> {
            bail!("startup probe must not download model artifacts")
        }
    }

    struct StartupProbeEngineLoader;

    impl VoiceEngineLoader for StartupProbeEngineLoader {
        fn load(
            &self,
            _entry: &VoiceModelCatalogEntry,
            _layout: &VoiceModelInstallLayout,
        ) -> std::result::Result<LoadedVoiceEngine, VoiceTranscriptionError> {
            Err(VoiceTranscriptionError {
                kind: VoiceTranscriptionErrorKind::RuntimeFailure,
                message: "startup probe must not load an engine".to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn voice_startup_disabled() {
        let installer = Arc::new(StartupProbeInstaller::new());
        let supervisor =
            create_voice_input_supervisor(installer.clone(), Arc::new(StartupProbeEngineLoader));
        start_voice_input_supervisor(&supervisor, VoiceInputDesiredState::default())
            .expect("disabled startup");

        tokio::task::yield_now().await;

        assert_eq!(installer.inspections.load(Ordering::SeqCst), 0);
        assert_eq!(
            supervisor.runtime_snapshot().phase,
            GatewayVoiceInputRuntimePhase::Disabled
        );
    }

    #[tokio::test]
    async fn voice_startup_selected() {
        let installer = Arc::new(StartupProbeInstaller::new());
        let supervisor =
            create_voice_input_supervisor(installer.clone(), Arc::new(StartupProbeEngineLoader));
        start_voice_input_supervisor(
            &supervisor,
            VoiceInputDesiredState {
                enabled: true,
                provider: Some(GatewayVoiceInputProvider::Local),
                model: Some("parakeet-tdt-0.6b-v3".to_owned()),
            },
        )
        .expect("selected startup");

        installer.inspected.notified().await;

        assert_eq!(installer.inspections.load(Ordering::SeqCst), 1);
        assert_eq!(
            supervisor.desired().model.as_deref(),
            Some("parakeet-tdt-0.6b-v3")
        );
    }

    #[tokio::test]
    async fn voice_startup_legacy_install() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        let legacy_install = runtime_home
            .path()
            .join("models/voice/parakeet-tdt-0.6b-v3-int8");
        std::fs::create_dir_all(&legacy_install).expect("legacy install directory");
        std::fs::write(legacy_install.join(".ready"), b"legacy").expect("legacy ready marker");
        let installer = Arc::new(StartupProbeInstaller::new());

        let supervisor =
            create_voice_input_supervisor(installer.clone(), Arc::new(StartupProbeEngineLoader));
        start_voice_input_supervisor(&supervisor, VoiceInputDesiredState::default())
            .expect("legacy-disabled startup");
        tokio::task::yield_now().await;

        assert!(legacy_install.exists());
        assert_eq!(installer.inspections.load(Ordering::SeqCst), 0);
        assert_eq!(
            supervisor.runtime_snapshot().phase,
            GatewayVoiceInputRuntimePhase::Disabled
        );
    }

    #[test]
    fn provider_key_resolver_reads_latest_keystore_value() {
        let secrets = GatewaySecrets::new(Arc::new(MemorySecretStore::new()));

        assert!(secrets.resolve_provider_api_key("deepseek").is_empty());

        secrets
            .set_provider_api_key("deepseek", "sk-test-key")
            .expect("set key");

        assert_eq!(secrets.resolve_provider_api_key("deepseek"), "sk-test-key");
    }

    #[test]
    fn path_templates_expand_home_directory_token() {
        let expanded = expand_home_directory_templates(
            &[
                "{homeDirectory}/skills/workspace/{workspaceId}/user".to_owned(),
                "{homeDirectory}/skills/workspace/{workspaceId}/registry".to_owned(),
            ],
            "/Users/alexander/.pioneer.local",
        );

        assert_eq!(
            expanded,
            vec![
                "/Users/alexander/.pioneer.local/skills/workspace/{workspaceId}/user".to_owned(),
                "/Users/alexander/.pioneer.local/skills/workspace/{workspaceId}/registry"
                    .to_owned(),
            ]
        );
    }

    #[test]
    fn path_templates_leave_plain_paths_unchanged() {
        let expanded = expand_home_directory_templates(
            &[
                "/opt/pioneer/skills/system".to_owned(),
                ".agents/skills".to_owned(),
            ],
            ".pioneer",
        );

        assert_eq!(
            expanded,
            vec![
                "/opt/pioneer/skills/system".to_owned(),
                ".agents/skills".to_owned(),
            ]
        );
    }

    #[test]
    fn gateway_memory_config_maps_to_memory_loop_defaults() {
        let loop_config =
            memory_loop_config_from_gateway_memory_config(&GatewayMemoryConfig::default());

        assert!(loop_config.deterministic_recall_enabled);
        assert!(loop_config.tools_enabled);
        assert_eq!(
            loop_config.active_recall.mode,
            MemoryActiveRecallMode::Hybrid
        );
        assert!(loop_config.post_turn_extractor.enabled);
        assert!(loop_config.post_turn_extractor.provider_enabled);
        assert!(loop_config.post_turn_extractor.proactive_writes_enabled);
        assert!(loop_config.post_turn_extractor.provider_name.is_none());
        assert!(loop_config.post_turn_extractor.model.is_none());
        assert_eq!(
            loop_config.post_turn_extractor.await_policy,
            HookAwaitPolicy::FireAndRecord
        );
        assert!(!loop_config.post_turn_extractor.strict_debug);
    }

    #[test]
    fn gateway_memory_config_disables_hook_surfaces_without_agent_loop_branches() {
        let loop_config = memory_loop_config_from_gateway_memory_config(&GatewayMemoryConfig {
            enabled: false,
            ..GatewayMemoryConfig::default()
        });

        assert!(!loop_config.deterministic_recall_enabled);
        assert!(!loop_config.tools_enabled);
        assert_eq!(
            loop_config.active_recall.mode,
            MemoryActiveRecallMode::Disabled
        );
        assert!(!loop_config.post_turn_extractor.enabled);
        assert!(!loop_config.post_turn_extractor.provider_enabled);
        assert!(!loop_config.post_turn_extractor.proactive_writes_enabled);
    }

    #[test]
    fn execution_windows_config_maps_legacy_budget_when_new_section_is_absent() {
        let config = execution_windows_config_from_gateway_tools_config(&GatewayToolsConfig {
            budget: GatewayToolLoopBudgetConfig {
                max_agent_rounds_per_turn: 17,
                max_tool_calls_per_turn: 23,
            },
            execution_windows: None,
            retry: GatewayToolRetryBudgetConfig::default(),
            ..GatewayToolsConfig::default()
        });

        assert_eq!(config.window.max_agent_rounds_per_window, 17);
        assert_eq!(config.window.max_tool_calls_per_window, 23);
        assert_eq!(config.window.max_wall_clock_ms_per_window, Some(1_800_000));
        assert_eq!(config.total.max_tool_calls_per_turn, None);
    }

    #[test]
    fn execution_windows_config_uses_new_section_when_present() {
        let config = execution_windows_config_from_gateway_tools_config(&GatewayToolsConfig {
            budget: GatewayToolLoopBudgetConfig {
                max_agent_rounds_per_turn: 17,
                max_tool_calls_per_turn: 23,
            },
            execution_windows: Some(GatewayExecutionWindowsConfig {
                window: GatewayExecutionWindowBudgetConfig {
                    max_agent_rounds_per_window: 31,
                    max_tool_calls_per_window: 37,
                    max_wall_clock_ms_per_window: Some(1_000),
                    max_provider_tokens_per_window: Some(2_000),
                },
                total: GatewayExecutionWindowTotalBudgetConfig {
                    max_windows_per_turn: Some(5),
                    max_tool_calls_per_turn: Some(41),
                    max_wall_clock_ms_per_turn: Some(3_000),
                    max_provider_tokens_per_turn: Some(4_000),
                    max_consecutive_no_progress_windows: 2,
                },
            }),
            retry: GatewayToolRetryBudgetConfig::default(),
            ..GatewayToolsConfig::default()
        });

        assert_eq!(config.window.max_agent_rounds_per_window, 31);
        assert_eq!(config.window.max_tool_calls_per_window, 37);
        assert_eq!(config.window.max_wall_clock_ms_per_window, Some(1_000));
        assert_eq!(config.window.max_provider_tokens_per_window, Some(2_000));
        assert_eq!(config.total.max_windows_per_turn, Some(5));
        assert_eq!(config.total.max_tool_calls_per_turn, Some(41));
        assert_eq!(config.total.max_wall_clock_ms_per_turn, Some(3_000));
        assert_eq!(config.total.max_provider_tokens_per_turn, Some(4_000));
        assert_eq!(config.total.max_consecutive_no_progress_windows, 2);
    }

    #[test]
    fn execution_windows_config_prefers_new_section_over_legacy_budget() {
        let config = execution_windows_config_from_gateway_tools_config(&GatewayToolsConfig {
            budget: GatewayToolLoopBudgetConfig {
                max_agent_rounds_per_turn: 17,
                max_tool_calls_per_turn: 23,
            },
            execution_windows: Some(GatewayExecutionWindowsConfig {
                window: GatewayExecutionWindowBudgetConfig {
                    max_agent_rounds_per_window: 31,
                    max_tool_calls_per_window: 37,
                    ..GatewayExecutionWindowBudgetConfig::default()
                },
                total: GatewayExecutionWindowTotalBudgetConfig::default(),
            }),
            retry: GatewayToolRetryBudgetConfig::default(),
            ..GatewayToolsConfig::default()
        });

        assert_eq!(config.window.max_agent_rounds_per_window, 31);
        assert_eq!(config.window.max_tool_calls_per_window, 37);
    }

    #[test]
    fn gateway_memory_config_can_disable_individual_product_features() {
        let loop_config = memory_loop_config_from_gateway_memory_config(&GatewayMemoryConfig {
            deterministic_recall_enabled: false,
            active_recall_enabled: true,
            tools_enabled: false,
            proactive_writes_enabled: false,
            background_extraction_enabled: false,
            debug_trace_enabled: true,
            ..GatewayMemoryConfig::default()
        });

        assert!(!loop_config.deterministic_recall_enabled);
        assert!(!loop_config.tools_enabled);
        assert_eq!(
            loop_config.active_recall.mode,
            MemoryActiveRecallMode::Disabled
        );
        assert!(!loop_config.post_turn_extractor.enabled);
        assert!(!loop_config.post_turn_extractor.provider_enabled);
        assert!(!loop_config.post_turn_extractor.proactive_writes_enabled);
        assert_eq!(
            loop_config.post_turn_extractor.await_policy,
            HookAwaitPolicy::Blocking
        );
        assert!(loop_config.post_turn_extractor.strict_debug);
    }

    #[test]
    fn gateway_memory_config_maps_proactive_model_override_to_hooks() {
        let loop_config = memory_loop_config_from_gateway_memory_config(&GatewayMemoryConfig {
            proactive_writes_model: GatewayMemoryModelSelectionConfig::custom(
                "extractor-provider",
                "extractor-model",
            ),
            ..GatewayMemoryConfig::default()
        });

        assert_eq!(
            loop_config.post_turn_extractor.provider_name.as_deref(),
            Some("extractor-provider")
        );
        assert_eq!(
            loop_config.post_turn_extractor.model.as_deref(),
            Some("extractor-model")
        );
    }

    #[test]
    fn gateway_tasks_review_config_maps_to_task_runtime_config() {
        let runtime_config =
            task_runtime_config_from_gateway_tasks_config(&GatewayTasksConfig::default());

        assert!(runtime_config.review.enabled);
        assert!(!runtime_config.review.allow_task_create_review_policy);
        assert!(
            runtime_config
                .review
                .default_parent_review_for_immediate_attached_agent_tasks
        );
        assert_eq!(runtime_config.review.default_max_revision_rounds, 5);
        assert_eq!(runtime_config.review.auto_accept_after_seconds, 300);
    }

    #[test]
    fn gateway_thread_episodic_settings_map_to_runtime_config() {
        let settings = GatewayThreadEpisodicSettings {
            enabled: false,
            indexing_enabled: true,
            recall_enabled: true,
            default_prompt_chars: 1_500,
            max_prompt_chars: 6_000,
            max_hit_chars: 700,
            default_max_candidates: 9,
            max_candidate_work: 33,
            max_segments: 5,
            min_relevancy: 0.4,
            min_results: 2,
            snippet_chars: 220,
            index_batch_limit: 7,
            retry_base_delay_secs: 11,
            retry_max_delay_secs: 121,
            max_attempts: 4,
            near_capacity_percent: 81.0,
            vector_search: pioneer_config::GatewayThreadEpisodicVectorSearchConfig::default(),
        };

        let runtime = thread_episodic_runtime_config_from_gateway_settings(&settings);

        assert!(!runtime.enabled);
        assert!(runtime.indexing_enabled);
        assert!(runtime.recall_enabled);
        assert!(!runtime.vector_search_enabled);
        assert_eq!(runtime.hook_max_prompt_chars, 1_500);
        assert_eq!(runtime.hook_max_candidates, 9);
        assert_eq!(runtime.index_executor.batch_limit, 7);
        assert_eq!(runtime.index_executor.retry_base_delay_secs, 11);
        assert_eq!(runtime.index_executor.retry_max_delay_secs, 121);
        assert_eq!(runtime.index_executor.max_attempts, 4);
        assert_eq!(runtime.index_executor.near_capacity_percent, 81.0);
        assert!(!runtime.recall_service.enabled);
        assert_eq!(runtime.recall_service.default_prompt_chars, 1_500);
        assert_eq!(runtime.recall_service.max_prompt_chars, 6_000);
        assert_eq!(runtime.recall_service.max_hit_chars, 700);
        assert_eq!(runtime.recall_service.default_max_candidates, 9);
        assert_eq!(runtime.recall_service.max_candidate_work, 33);
        assert_eq!(runtime.recall_service.max_segments, 5);
        assert_eq!(runtime.recall_service.min_relevancy, 0.4);
        assert_eq!(runtime.recall_service.min_results, 2);
        assert_eq!(runtime.recall_service.snippet_chars, 220);

        let mut vector_settings = settings.clone();
        vector_settings.vector_search.enabled = true;
        let vector_runtime = thread_episodic_runtime_config_from_gateway_settings(&vector_settings);
        assert!(!vector_runtime.vector_search_enabled);
        assert!(!vector_runtime.recall_service.vector_search_enabled);

        vector_settings.vector_search.provider =
            Some(pioneer_config::GatewayThreadEpisodicVectorProviderConfig::OpenAi);
        vector_settings.vector_search.model = Some("text-embedding-3-small".to_owned());
        let vector_runtime = thread_episodic_runtime_config_from_gateway_settings(&vector_settings);
        assert!(vector_runtime.vector_search_enabled);
        assert!(vector_runtime.recall_service.vector_search_enabled);
    }

    #[test]
    fn skill_trust_level_parser_maps_supported_values() {
        assert_eq!(
            parse_skill_trust_level("internal", "field").expect("internal trust should parse"),
            SkillTrustLevel::Internal
        );
        assert_eq!(
            parse_skill_trust_level("verified", "field").expect("verified trust should parse"),
            SkillTrustLevel::Verified
        );
        assert_eq!(
            parse_skill_trust_level("community", "field").expect("community trust should parse"),
            SkillTrustLevel::Community
        );
        assert_eq!(
            parse_skill_trust_level("untrusted", "field").expect("untrusted trust should parse"),
            SkillTrustLevel::Untrusted
        );
    }

    #[test]
    fn skill_trust_level_parser_rejects_invalid_value() {
        let error =
            parse_skill_trust_level("supertrusted", "gateway.skills.security.test").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("invalid trust level `supertrusted`")
        );
    }
}
