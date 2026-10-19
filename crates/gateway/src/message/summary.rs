use anyhow::{Context, Result, ensure};
use pioneer_cli_agent_runtime::{
    claude::service::{ClaudeService, ClaudeServiceConfig, ClaudeServiceRequest},
    codex::service::{CodexService, CodexServiceConfig, CodexServiceRequest},
};
use pioneer_config::{
    EffectiveGatewayCliAgentRuntimeInstanceConfig, GatewayCliAgentRuntimeKindConfig,
};
use pioneer_crud::CrudStore;
use pioneer_provider::{ChatMessage, ChatRequest, ProviderRegistry};
use tracing::debug;

pub struct SummaryConfig {
    pub summary_model: Option<String>,
    pub summary_model_provider: Option<String>,
    pub title_model: Option<String>,
    pub title_model_provider: Option<String>,
}

pub async fn generate_thread_title(
    crud: &CrudStore,
    registry: &ProviderRegistry,
    processor: &super::MessageProcessor,
    thread_id: &str,
    user_text: &str,
    config: &SummaryConfig,
) -> Result<Option<String>> {
    let thread = crud
        .get_thread_by_id(thread_id)
        .await?
        .context("thread not found for title generation")?;

    let prompt = build_title_prompt(user_text);

    let (model_provider, model) = title_selection(config, &thread.model_provider, &thread.model);

    let text = if model_provider.starts_with("cli_runtime:") {
        let instance = title_cli_instance(model_provider, processor.load_cli_runtime_instances()?)?
            .context("selected CLI title instance is unavailable")?;
        let proxy = processor
            .prepare_cli_runtime_proxy_url(&thread.workspace_id, &instance.id)
            .await?;
        generate_cli_title(
            crud,
            &thread.workspace_id,
            thread_id,
            instance,
            model,
            prompt,
            crate::cli_runtime::config::proxy_env(proxy.as_deref()),
        )
        .await?
    } else {
        let provider =
            registry.get_or_create_for_workspace(&thread.workspace_id, model_provider)?;
        let provider =
            crate::usage_journal::observe(provider, crud, &thread.workspace_id, "title", thread_id);
        let response = provider
            .chat(title_generation_chat_request(model, prompt))
            .await?;
        pioneer_provider::ProviderMessageCollector::validate_response(&response)?;
        response.text
    };
    let title = normalize_generated_title(&text);
    if title.is_empty() {
        return Ok(None);
    }

    let title = truncate_utf8_bytes(title.as_str(), 255);
    debug!(
        thread_id,
        title = title.as_str(),
        "thread title candidate generated"
    );

    Ok(Some(title))
}

fn title_selection<'a>(
    config: &'a SummaryConfig,
    provider: &'a str,
    model: &'a str,
) -> (&'a str, &'a str) {
    (
        config.title_model_provider.as_deref().unwrap_or(provider),
        config.title_model.as_deref().unwrap_or(model),
    )
}

fn title_cli_instance(
    provider: &str,
    instances: Vec<EffectiveGatewayCliAgentRuntimeInstanceConfig>,
) -> Result<Option<EffectiveGatewayCliAgentRuntimeInstanceConfig>> {
    let Some(id) = provider.strip_prefix("cli_runtime:") else {
        return Ok(None);
    };
    let instance = instances
        .into_iter()
        .find(|instance| instance.id == id)
        .context("selected CLI title instance is unavailable")?;
    ensure!(instance.enabled, "selected CLI title instance is disabled");
    ensure!(
        matches!(
            instance.kind,
            GatewayCliAgentRuntimeKindConfig::Codex | GatewayCliAgentRuntimeKindConfig::Claude
        ),
        "selected CLI runtime does not support isolated title generation"
    );
    Ok(Some(instance))
}

async fn generate_cli_title(
    crud: &CrudStore,
    workspace: &str,
    thread_id: &str,
    instance: EffectiveGatewayCliAgentRuntimeInstanceConfig,
    model: &str,
    prompt: String,
    environment: pioneer_cli_agent_runtime::process::SensitiveEnvironment,
) -> Result<String> {
    let codex = instance.kind == GatewayCliAgentRuntimeKindConfig::Codex;
    let mut usage = pioneer_provider::TokenUsage {
        provider: Some(if codex { "codex-cli" } else { "claude-cli" }.into()),
        api: Some(
            if codex {
                "codex_exec_json"
            } else {
                "claude_print_json"
            }
            .into(),
        ),
        model: Some(model.into()),
        ..Default::default()
    };
    let claude_output_cap = if codex {
        0
    } else {
        pioneer_provider::catalog::model_catalog()?
            .limits("anthropic", model)
            .max_output
            .unwrap_or(1024)
    };
    let call =
        crate::usage_journal::Call::start(crud, workspace, "title", thread_id, &usage).await?;
    let timeout = std::time::Duration::from_millis(instance.request_timeout_ms);
    // These are fresh nonpersistent service contexts, never the user's session.
    // No compaction prompt, runner, lifecycle, or visible Pioneer turn is used.
    let result = if codex {
        let service = CodexService::new(CodexServiceConfig {
            executable: instance.binary_path,
            home_path: instance.home_path,
            environment,
        });
        service
            .summarize(
                CodexServiceRequest {
                    model: model.into(),
                    effort: None,
                    instructions: title_instructions().into(),
                    input: prompt,
                },
                timeout,
            )
            .await
            .map(|result| (result.text, result.observed_usage))
    } else {
        let service = ClaudeService::new(ClaudeServiceConfig {
            executable: instance.binary_path,
            config_dir: instance.home_path,
            environment,
        });
        service
            .summarize(
                ClaudeServiceRequest {
                    model: model.into(),
                    effort: None,
                    instructions: title_instructions().into(),
                    input: prompt,
                    output_cap: claude_output_cap,
                },
                timeout,
            )
            .await
            .map(|result| (result.text, result.observed_usage))
    };
    let raw = match &result {
        Ok((_, raw)) => Some(raw),
        Err(error) => error
            .downcast_ref::<pioneer_cli_agent_runtime::service::ObservedServiceUsage>()
            .map(|usage| &usage.0),
    };
    if let Some(raw) = raw {
        crate::compaction::apply_cli_usage(&mut usage, raw);
        if let Some(scope) = usage
            .accounting
            .as_mut()
            .and_then(|value| value.pointer_mut("/sdk_estimated_cost/scope"))
        {
            *scope = serde_json::json!("isolated_cli_title_result");
        }
    }
    call.record(
        if result.is_ok() {
            "completed"
        } else {
            "failed"
        },
        &usage,
    )
    .await?;
    result.map(|(text, _)| text)
}

fn title_instructions() -> &'static str {
    "Generate only a conversation title following the supplied instructions. Do not use tools."
}

fn title_generation_chat_request(model: &str, prompt: String) -> ChatRequest {
    ChatRequest {
        model: model.to_owned(),
        messages: vec![ChatMessage::user(prompt)],
        temperature: None,
        max_tokens: None,
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        // Service calls inherit the model default. Explicit off is a user
        // control and is rejected by mandatory-thinking models.
        reasoning: None,
        compiled_prompt: None,
    }
}

fn build_title_prompt(user_text: &str) -> String {
    let mut prompt = String::with_capacity(512);
    prompt.push_str(
        "Generate a concise title (2-6 words) for this conversation. \
         Detect the user's language and return the title in the same language as the user text. \
         No quotes, no emoji, no punctuation at the end. Output only the title.\n\n",
    );

    prompt.push_str("User: ");
    prompt.push_str(user_text);
    prompt.push('\n');

    prompt
}

fn normalize_generated_title(raw: &str) -> String {
    normalize_title_for_compare(raw)
}

pub(super) fn normalize_title_for_compare(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('\'')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub(super) fn incoming_thread_title(raw: &str) -> Option<String> {
    if raw.chars().any(|c| c.is_control() && !c.is_whitespace()) {
        return None;
    }
    let title = normalize_generated_title(raw);
    (!title.is_empty() && title.len() <= 255).then_some(title)
}

fn truncate_utf8_bytes(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input.to_owned();
    }

    let mut end = max_bytes;
    while end > 0 && !input.is_char_boundary(end) {
        end -= 1;
    }

    input[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::{
        build_title_prompt, normalize_generated_title, title_generation_chat_request,
        truncate_utf8_bytes,
    };

    #[test]
    fn title_selection_preserves_overrides_and_routes_api_codex_claude() {
        use super::*;
        let mut config = SummaryConfig {
            summary_model: Some("unused".into()),
            summary_model_provider: Some("unused".into()),
            title_model: None,
            title_model_provider: None,
        };
        let mut instance = EffectiveGatewayCliAgentRuntimeInstanceConfig {
            id: "work".into(),
            kind: GatewayCliAgentRuntimeKindConfig::Codex,
            display_name: "Work".into(),
            enabled: true,
            binary_path: "codex".into(),
            home_path: "/auth/work".into(),
            shadow_home_path: None,
            custom_models: vec![],
            app_server_args: vec![],
            startup_probe_timeout_ms: 1000,
            request_timeout_ms: 1000,
            idle_session_ttl_secs: 60,
            event_channel_capacity: 16,
            stderr_ring_lines: 0,
            debug_native_events: false,
        };
        for kind in [
            GatewayCliAgentRuntimeKindConfig::Codex,
            GatewayCliAgentRuntimeKindConfig::Claude,
        ] {
            instance.kind = kind;
            let (provider, model) = title_selection(&config, "cli_runtime:work", "native-model");
            assert_eq!(model, "native-model");
            assert_eq!(
                title_cli_instance(provider, vec![instance.clone()])
                    .unwrap()
                    .unwrap()
                    .kind,
                kind
            );
        }
        config.title_model = Some("explicit-model".into());
        assert_eq!(
            title_selection(&config, "cli_runtime:work", "native-model"),
            ("cli_runtime:work", "explicit-model")
        );
        config.title_model_provider = Some("openai".into());
        let (provider, model) = title_selection(&config, "cli_runtime:work", "native-model");
        assert_eq!((provider, model), ("openai", "explicit-model"));
        assert!(
            title_cli_instance(provider, vec![instance.clone()])
                .unwrap()
                .is_none()
        );
        assert!(title_cli_instance("cli_runtime:missing", vec![instance.clone()]).is_err());
        instance.enabled = false;
        assert!(title_cli_instance("cli_runtime:work", vec![instance]).is_err());
    }

    #[test]
    fn title_normalization_trims_quotes_and_whitespace() {
        assert_eq!(super::incoming_thread_title(" "), None);
        assert_eq!(super::incoming_thread_title("bad\0name"), None);
        assert_eq!(super::incoming_thread_title(&"я".repeat(128)), None);
        assert_eq!(
            super::incoming_thread_title("  'Имя   Codex'  ").as_deref(),
            Some("Имя Codex")
        );
        assert_eq!(
            normalize_generated_title("  \"Погода   в  Москве\"  "),
            "Погода в Москве"
        );
    }

    #[test]
    fn title_prompt_includes_full_utf8_user_text() {
        let text = "привет".repeat(150);
        let prompt = build_title_prompt(text.as_str());
        assert!(prompt.contains("User: "));
        assert!(prompt.contains(text.as_str()));
        assert!(!prompt.contains("..."));
        assert!(prompt.ends_with('\n'));
    }

    #[test]
    fn title_generation_request_preserves_model_default_reasoning() {
        let request = title_generation_chat_request("openrouter/model", "title me".to_owned());
        assert_eq!(request.reasoning, None);
        assert!(request.tools.is_none());
    }

    #[test]
    fn truncate_utf8_bytes_respects_char_boundaries() {
        let text = "абвгд";
        assert_eq!(truncate_utf8_bytes(text, 5), "аб");
        assert_eq!(truncate_utf8_bytes(text, 6), "абв");
    }
}
