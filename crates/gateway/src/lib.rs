mod attachment;
mod auth;
mod bootstrap;
mod database;
mod helpers;
mod mcp_service;
mod message;
mod resilience;
mod session;
mod settings;
mod task_tools;
mod thread;
mod tokenizer;
mod transport;
mod workspace;

use anyhow::{Context, Result};
use attachment::CrudAttachmentUploadRegistryBackend;
use pioneer_agent::ToolLoopConfig;
use pioneer_config::AppConfig;
use pioneer_crud::CrudStore;
use pioneer_provider::{
    AttachmentCircuitBreakerPolicy, AttachmentNormalizationPolicy, AttachmentPipelineConfig,
    AttachmentRetryPolicy, AttachmentRuntimePolicy, AttachmentSecurityPolicy,
    AttachmentUploadRegistryPolicy, ProviderRegistry, set_attachment_upload_registry_backend,
    set_default_attachment_pipeline_config,
};
use pioneer_skills::SkillTrustLevel;
use pioneer_tools::{
    ComputerUseToolsConfig, ToolLoopBudgetConfig, ToolRetryBudgetConfig, WebToolsConfig,
};
use std::path::Path;
use std::sync::{Arc, RwLock};
use tracing::info;

use crate::auth::initialize as initialize_jwt_auth;
use crate::auth::issue_superuser_token as issue_superuser_token_internal;
use crate::bootstrap::bootstrap as run_bootstrap;
use crate::database::initialize as initialize_database;
use crate::message::MessageProcessor;
use crate::message::now_timestamp_secs;
use crate::message::{ContextBudget, SummaryConfig};
use crate::session::SessionManager;
use crate::settings::{
    GatewaySettings, load_or_create_gateway_settings, normalize_settings_file_name,
};
use crate::thread::ThreadManager;
use crate::transport::spawn_server;
use crate::workspace::WorkspaceManager;

const HOME_DIRECTORY_TOKEN: &str = "{homeDirectory}";

pub async fn run_gateway_until_shutdown() -> Result<()> {
    let config = AppConfig::load()?;
    let runtime_home = config.ensure_runtime_home_dir()?;
    info!(
        runtime_home = %runtime_home.display(),
        "runtime home directory is ready"
    );

    let gateway_settings = load_gateway_settings(&runtime_home, &config)?;
    let auth = initialize_jwt_auth(&config, &gateway_settings)?;
    let database = initialize_database(&runtime_home, &config).await?;

    run_bootstrap(&database).await?;

    let session_manager = Arc::new(SessionManager::new());
    let workspace_manager = Arc::new(WorkspaceManager::new(database.clone()));
    let crud_store = Arc::new(CrudStore::new(database.clone()));
    let thread_manager = Arc::new(ThreadManager::from_app_config(&config));

    set_attachment_upload_registry_backend(Arc::new(CrudAttachmentUploadRegistryBackend::new(
        crud_store.clone(),
    )));

    let gateway_settings = Arc::new(RwLock::new(gateway_settings));

    let provider_registry = Arc::new(ProviderRegistry::new({
        let settings = gateway_settings.clone();
        move |provider_name| {
            let settings = settings
                .read()
                .expect("gateway settings lock poisoned while resolving provider key");
            resolve_provider_api_key(provider_name, &settings)
        }
    }));

    let settings_path = runtime_home.join(
        normalize_settings_file_name(config.gateway.settings_file_name.as_str())
            .unwrap_or_else(|_| config.gateway.settings_file_name.clone()),
    );

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
    let tool_budget_cfg = &config.gateway.tools.budget;
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
        upload_registry: AttachmentUploadRegistryPolicy {
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

    let skill_system_roots = expand_home_directory_templates(
        skills_cfg.paths.system.as_slice(),
        &runtime_home_directory,
    );
    let skill_user_roots =
        expand_home_directory_templates(skills_cfg.paths.user.as_slice(), &runtime_home_directory);
    let skill_workspace_roots = expand_home_directory_templates(
        skills_cfg.paths.workspace.as_slice(),
        &runtime_home_directory,
    );
    let skill_registry_roots = expand_home_directory_templates(
        skills_cfg.paths.registry.as_slice(),
        &runtime_home_directory,
    );
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
            workspace_roots: skill_workspace_roots,
            registry_roots: skill_registry_roots,
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
        budget: ToolLoopBudgetConfig {
            max_agent_rounds_per_turn: tool_budget_cfg.max_agent_rounds_per_turn,
            max_tool_calls_per_turn: tool_budget_cfg.max_tool_calls_per_turn,
        },
        retry: ToolRetryBudgetConfig {
            max_recoverable_retry_rounds_per_episode: tool_retry_cfg
                .max_recoverable_retry_rounds_per_episode,
            max_same_tool_error_retries_per_episode: tool_retry_cfg
                .max_same_tool_error_retries_per_episode,
            max_retries_per_tool_name_per_episode: tool_retry_cfg
                .max_retries_per_tool_name_per_episode,
        },
    };

    let message_processor = Arc::new(MessageProcessor::new(
        thread_manager,
        provider_registry,
        session_manager.clone(),
        workspace_manager,
        crud_store,
        gateway_settings,
        settings_path,
        summary_config,
        context_budget,
        tool_loop_config,
    ));

    message_processor.bind_task_bridge().await;
    message_processor
        .cleanup_stale_skill_uploads(now_timestamp_secs())
        .await;
    message_processor.start_resilience_workers().await;
    message_processor.start_skills_watcher().await;
    message_processor.start_mcp_workspace_supervisor().await;

    let handle = spawn_server(config, auth, message_processor, session_manager).await?;

    info!(listen_addr = %handle.local_addr(), "gateway daemon started");

    wait_for_shutdown_signal().await?;

    info!("gateway daemon stopping with telemetry snapshot");
    handle.shutdown().await?;
    database
        .close()
        .await
        .context("failed to close gateway database connection")?;

    Ok(())
}

pub fn issue_superuser_token(config: &AppConfig, runtime_home: &Path) -> Result<String> {
    let settings = load_gateway_settings(runtime_home, config)?;
    issue_superuser_token_internal(config, &settings)
}

fn load_gateway_settings(runtime_home: &Path, config: &AppConfig) -> Result<GatewaySettings> {
    let settings_file_name =
        normalize_settings_file_name(config.gateway.settings_file_name.as_str())?;
    let settings_path = runtime_home.join(settings_file_name.as_str());
    load_or_create_gateway_settings(
        &settings_path,
        config.gateway.settings_version,
        settings_file_name.as_str(),
        config.gateway.auth.secret_size_bytes,
    )
}

fn resolve_provider_api_key(provider_name: &str, settings: &GatewaySettings) -> String {
    // Local / CLI providers — no API key needed
    if matches!(
        provider_name,
        "ollama"
            | "lmstudio"
            | "llamacpp"
            | "sglang"
            | "vllm"
            | "osaurus"
            | "litellm"
            | "claude-code"
            | "gemini-cli"
            | "kilocli"
    ) {
        return String::new();
    }

    if let Some(value) = settings.provider_api_key(provider_name) {
        return value.to_owned();
    }

    String::new()
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
mod tests {
    use super::{
        expand_home_directory_templates, parse_skill_trust_level, resolve_provider_api_key,
    };
    use crate::settings::GatewaySettings;
    use pioneer_skills::SkillTrustLevel;
    use std::sync::{Arc, RwLock};

    #[test]
    fn provider_key_resolver_reads_latest_shared_settings() {
        let settings = Arc::new(RwLock::new(GatewaySettings::default_for_tests()));
        let resolver = {
            let settings = settings.clone();
            move |provider_name: &str| {
                let settings = settings
                    .read()
                    .expect("gateway settings lock poisoned in test");
                resolve_provider_api_key(provider_name, &settings)
            }
        };

        assert!(resolver("deepseek").is_empty());

        {
            let mut settings = settings
                .write()
                .expect("gateway settings lock poisoned in test");
            settings.set_provider_api_key("deepseek", "sk-test-key".to_owned());
        }

        assert_eq!(resolver("deepseek"), "sk-test-key");
    }

    #[test]
    fn path_templates_expand_home_directory_token() {
        let expanded = expand_home_directory_templates(
            &[
                "{homeDirectory}/skills/system".to_owned(),
                "{homeDirectory}/skills/workspace/{workspaceId}".to_owned(),
            ],
            "/Users/alexander/.pioneer.local",
        );

        assert_eq!(
            expanded,
            vec![
                "/Users/alexander/.pioneer.local/skills/system".to_owned(),
                "/Users/alexander/.pioneer.local/skills/workspace/{workspaceId}".to_owned(),
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
