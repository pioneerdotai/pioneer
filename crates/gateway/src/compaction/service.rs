//! Effective-selection factory for independent API and CLI summary attempts.
use anyhow::{Result, ensure};
use async_trait::async_trait;
use pioneer_agent::compaction::NativeSummarizer;
use pioneer_cli_agent_runtime::{
    claude::service::{ClaudeService, ClaudeServiceConfig, ClaudeServiceRequest},
    codex::service::{CodexService, CodexServiceConfig, CodexServiceRequest},
    service::ServiceFailure,
};
use pioneer_compaction::{
    ModelBudget, ModelSelection, Transport,
    runner::FailureKind,
    summary::{
        CompletionKind, INSTRUCTIONS, Summarizer, SummaryCompletion, SummaryFailure, SummaryRequest,
    },
    text_tokens,
};
use pioneer_config::GatewayCliAgentRuntimeKindConfig;
use pioneer_protocol::ProviderFailureClass;
use pioneer_provider::ProviderRegistry;
use std::{sync::Arc, time::Duration};

pub(super) async fn make_summarizer(
    providers: &ProviderRegistry,
    processor: Option<&crate::message::MessageProcessor>,
    workspace: &str,
    selection: ModelSelection,
) -> Result<Arc<dyn Summarizer>> {
    if selection.transport == Transport::Api {
        let provider = providers.get_or_create_for_workspace(workspace, &selection.instance)?;
        let limits =
            pioneer_provider::catalog::model_catalog()?.limits(provider.name(), &selection.model);
        return Ok(Arc::new(NativeSummarizer::new(
            provider,
            selection,
            ModelBudget::new(
                Some(limits.context_window),
                limits.max_input,
                limits.max_output,
            ),
        )?));
    }
    let processor =
        processor.ok_or_else(|| anyhow::anyhow!("CLI service instance catalog is unavailable"))?;
    let instance = processor
        .load_cli_runtime_instances()?
        .into_iter()
        .find(|instance| instance.id == selection.instance)
        .ok_or_else(|| anyhow::anyhow!("selected CLI service instance is unavailable"))?;
    let proxy = processor
        .prepare_cli_runtime_proxy_url(workspace, &instance.id)
        .await?;
    make_cli_summarizer(
        instance,
        selection,
        crate::cli_runtime::config::proxy_env(proxy.as_deref()),
    )
}

fn make_cli_summarizer(
    instance: pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig,
    selection: ModelSelection,
    environment: pioneer_cli_agent_runtime::process::SensitiveEnvironment,
) -> Result<Arc<dyn Summarizer>> {
    ensure!(
        instance.id == selection.instance,
        "CLI service instance changed after selection"
    );
    ensure!(
        instance.enabled,
        "selected CLI service instance is disabled"
    );
    let kind = match instance.kind {
        GatewayCliAgentRuntimeKindConfig::Codex => Transport::Codex,
        GatewayCliAgentRuntimeKindConfig::Claude => Transport::Claude,
    };
    ensure!(
        kind == selection.transport,
        "selected CLI service instance changed kind"
    );
    let (service, catalog) = match kind {
        Transport::Codex => (
            CliService::Codex(CodexService::new(CodexServiceConfig {
                executable: instance.binary_path,
                home_path: instance.home_path,
                environment,
            })),
            "openai-codex",
        ),
        Transport::Claude => (
            CliService::Claude(ClaudeService::new(ClaudeServiceConfig {
                executable: instance.binary_path,
                config_dir: instance.home_path,
                environment,
            })),
            "anthropic",
        ),
        Transport::Api => unreachable!(),
    };
    let limits = pioneer_provider::catalog::model_catalog()?.limits(catalog, &selection.model);
    Ok(Arc::new(CliSummarizer {
        selection,
        service,
        budget: ModelBudget::new(
            Some(limits.context_window),
            limits.max_input,
            limits.max_output,
        ),
    }))
}

enum CliService {
    Codex(CodexService),
    Claude(ClaudeService),
}
struct CliSummarizer {
    selection: ModelSelection,
    budget: ModelBudget,
    service: CliService,
}
impl CliSummarizer {
    fn input(&self, request: &SummaryRequest) -> Result<String> {
        ensure!(
            request.selection == self.selection,
            "summary selection changed after admission"
        );
        ensure!(
            request.output_cap > 0 && request.output_cap <= self.budget.summarizer_cap(u64::MAX)?,
            "invalid service output cap"
        );
        request.data_json()
    }
}
fn service_diagnostic(error: &anyhow::Error) -> pioneer_compaction::runner::FailureDiagnostic {
    use pioneer_cli_agent_runtime::codex::CodexJsonlRpcClientError;
    use pioneer_cli_agent_runtime::service::ServiceStage;
    use pioneer_compaction::runner::FailureDiagnostic;
    let stage = error
        .downcast_ref::<ServiceStage>()
        .map(|s| s.0)
        .unwrap_or("cli_service");
    // Only known fixed messages may become stored explanations. Unknown errors
    // retain the stage and typed transport classification, never their text.
    let known = error
        .chain()
        .find_map(|cause| match cause.to_string().as_str() {
            "Codex service isolation was not applied" => Some((
                "cli_isolation_rejected",
                "Codex config/read did not confirm the required isolated service profile",
            )),
            "Codex service model changed" => Some((
                "cli_model_changed",
                "Codex opened a different model than requested",
            )),
            "Codex service opened a persistent thread" => Some((
                "cli_thread_persistent",
                "Codex did not confirm an ephemeral service thread",
            )),
            "service input exceeds transport capacity" | "service input exceeds capacity" => {
                Some((
                    "cli_input_capacity",
                    "Service input exceeds transport capacity",
                ))
            }
            "Codex service ended without completion" => Some((
                "cli_completion_missing",
                "Codex transport closed before a completion",
            )),
            "Codex service requested interaction or transport closed" => Some((
                "cli_interaction_requested",
                "Isolated service requested an interaction",
            )),
            "Codex service transport lost alignment" => Some((
                "cli_transport_alignment",
                "Codex transport lost protocol alignment",
            )),
            "codex service deadline exceeded"
            | "Codex service deadline exceeded"
            | "Claude service deadline exceeded" => {
                Some(("cli_deadline", "CLI service exceeded the attempt deadline"))
            }
            "Codex service process failed" => {
                Some(("cli_process_failed", "Codex exec exited unsuccessfully"))
            }
            "invalid Codex service event" | "Codex service emitted events after completion" => {
                Some((
                    "cli_response_invalid",
                    "Codex exec returned an invalid event stream",
                ))
            }
            "Codex service output exceeds capacity" => Some((
                "cli_output_capacity",
                "Codex exec output exceeded the service capacity",
            )),
            "Codex service attempted a working action" => Some((
                "cli_working_action_rejected",
                "Codex exec attempted an action outside the summary service",
            )),
            "unsupported Claude service capability version" => Some((
                "cli_version_unsupported",
                "Installed Claude version does not match the supported service protocol",
            )),
            _ => None,
        });
    let mut diagnostic = if let Some((code, explanation)) = known {
        FailureDiagnostic::new(stage, code, explanation)
    } else if let Some(failure) = error.downcast_ref::<ServiceFailure>() {
        FailureDiagnostic::new(
            stage,
            "cli_provider_failure",
            &format!("Provider failure class: {:?}", failure.class),
        )
    } else if let Some(rpc) = error.downcast_ref::<CodexJsonlRpcClientError>() {
        let code = match rpc {
            CodexJsonlRpcClientError::Native(_) => "cli_rpc_rejected",
            CodexJsonlRpcClientError::RequestTimeout { .. } => "cli_rpc_timeout",
            CodexJsonlRpcClientError::TransportClosed { .. } => "cli_transport_closed",
            CodexJsonlRpcClientError::Decode { .. } => "cli_response_invalid",
            CodexJsonlRpcClientError::Encode { .. } => "cli_request_invalid",
            _ => "cli_rpc_state_invalid",
        };
        FailureDiagnostic::new(
            stage,
            code,
            "CLI protocol operation failed at the recorded stage",
        )
    } else if let Some(io) = error.downcast_ref::<std::io::Error>() {
        FailureDiagnostic::new(
            stage,
            "cli_io_failure",
            &format!("Operating system failure: {:?}", io.kind()),
        )
    } else {
        FailureDiagnostic::new(
            stage,
            "cli_service_rejected",
            "Unclassified CLI service failure; raw error omitted",
        )
    };
    if let Some(CodexJsonlRpcClientError::Native(rpc)) =
        error.downcast_ref::<CodexJsonlRpcClientError>()
    {
        diagnostic.rpc_code = Some(rpc.code);
    }
    diagnostic
}
fn failure(error: anyhow::Error) -> SummaryFailure {
    let typed = error.downcast_ref::<ServiceFailure>();
    let transient = typed.is_some_and(|failure| {
        matches!(
            failure.class,
            ProviderFailureClass::NetworkTransient
                | ProviderFailureClass::RateLimit
                | ProviderFailureClass::Provider5xx
        )
    });
    SummaryFailure {
        diagnostic: Some(service_diagnostic(&error)),
        kind: if transient {
            FailureKind::Transient
        } else {
            FailureKind::Permanent
        },
        retry_after_ms: typed.and_then(|failure| failure.retry_after_ms),
        code: if transient {
            "summary_cli_transient"
        } else {
            "summary_cli_rejected"
        },
    }
}
#[async_trait]
impl Summarizer for CliSummarizer {
    fn model_budget(&self) -> ModelBudget {
        self.budget.clone()
    }
    fn input_tokens(&self, request: &SummaryRequest) -> Result<u64> {
        let input = self.input(request)?;
        let framed = serde_json::json!({"model":request.selection.model,"effort":request.selection.effort,
            "instructions":INSTRUCTIONS,"input":input,"output_cap":request.output_cap});
        Ok(text_tokens(&framed.to_string()).saturating_add(32))
    }
    async fn summarize(
        &self,
        request: SummaryRequest,
    ) -> Result<SummaryCompletion, SummaryFailure> {
        let input_tokens = self.input_tokens(&request).map_err(failure)?;
        if !self.budget.fits(input_tokens, request.output_cap, false) {
            return Err(SummaryFailure {
                diagnostic: None,
                kind: FailureKind::Permanent,
                retry_after_ms: None,
                code: "summary_cli_input_overflow",
            });
        }
        let input = self.input(&request).map_err(failure)?;
        // Runner owns the tighter remaining operation/attempt deadline and drops
        // this call on Stop. The service retains its owner for awaitable cleanup.
        let timeout = Duration::from_millis(pioneer_compaction::ATTEMPT_MILLIS);
        let (text, input_tokens, output_tokens) = match &self.service {
            CliService::Codex(service) => {
                let result = service
                    .summarize(
                        CodexServiceRequest {
                            model: request.selection.model,
                            effort: request.selection.effort,
                            instructions: INSTRUCTIONS.into(),
                            input,
                        },
                        timeout,
                    )
                    .await
                    .map_err(failure)?;
                (result.text, result.input_tokens, result.output_tokens)
            }
            CliService::Claude(service) => {
                let result = service
                    .summarize(
                        ClaudeServiceRequest {
                            model: request.selection.model,
                            effort: request.selection.effort,
                            instructions: INSTRUCTIONS.into(),
                            input,
                            output_cap: request.output_cap,
                        },
                        timeout,
                    )
                    .await
                    .map_err(failure)?;
                (result.text, result.input_tokens, result.output_tokens)
            }
        };
        Ok(SummaryCompletion {
            text,
            kind: CompletionKind::Complete,
            input_tokens,
            output_tokens,
        })
    }
    async fn cleanup(&self) -> Result<(), SummaryFailure> {
        match &self.service {
            CliService::Codex(service) => service.cleanup().await,
            CliService::Claude(service) => service.cleanup().await,
        }
        .map_err(failure)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use pioneer_cli_agent_runtime::process::SensitiveEnvironment;
    use pioneer_compaction::{
        CompactionMode, SourceRef,
        summary::{HEADINGS, SummaryInput, SummaryPart, validate_summary},
    };
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn cli_failure_diagnostic_preserves_stage_and_rpc_code_without_raw_payloads() {
        use pioneer_cli_agent_runtime::codex::{
            CodexJsonlRpcClientError, CodexJsonlRpcNativeError,
        };
        use pioneer_cli_agent_runtime::service::ServiceStage;
        let error =
            anyhow::Error::new(CodexJsonlRpcClientError::Native(CodexJsonlRpcNativeError {
                id: None,
                code: -32602,
                message: "secret request payload /private/path token=credential".into(),
                data: None,
            }))
            .context(ServiceStage("cli_thread_start"));
        let failure = failure(error);
        let diagnostic = failure.diagnostic.unwrap();
        assert_eq!(diagnostic.stage, "cli_thread_start");
        assert_eq!(diagnostic.code, "cli_rpc_rejected");
        assert_eq!(diagnostic.rpc_code, Some(-32602));
        let stored = serde_json::to_string(&diagnostic).unwrap();
        assert!(
            !stored.contains("credential")
                && !stored.contains("private/path")
                && !stored.contains("secret")
        );
        let profile =
            service_diagnostic(&anyhow::anyhow!("Codex service isolation was not applied"));
        assert_eq!(profile.code, "cli_isolation_rejected");
        for (message, code) in [
            ("Codex service process failed", "cli_process_failed"),
            ("invalid Codex service event", "cli_response_invalid"),
            (
                "Codex service output exceeds capacity",
                "cli_output_capacity",
            ),
            (
                "Codex service attempted a working action",
                "cli_working_action_rejected",
            ),
            ("Codex service deadline exceeded", "cli_deadline"),
        ] {
            let diagnostic = service_diagnostic(
                &anyhow::anyhow!(message).context(ServiceStage("cli_exec_decode")),
            );
            assert_eq!(diagnostic.code, code);
            assert_eq!(diagnostic.stage, "cli_exec_decode");
        }
    }

    #[test]
    fn cli_summary_input_keeps_projected_command_output_once() {
        let selection = ModelSelection {
            transport: Transport::Claude,
            instance: "fixture".into(),
            model: "fixture-model".into(),
            effort: None,
        };
        let summarizer = CliSummarizer {
            selection: selection.clone(),
            budget: ModelBudget::new(Some(128_000), None, None),
            service: CliService::Claude(ClaudeService::new(ClaudeServiceConfig {
                executable: "unused-fixture".into(),
                config_dir: "unused-fixture".into(),
                environment: SensitiveEnvironment::new(),
            })),
        };
        let marker = "cli-summary-command-output ".repeat(32);
        let request = SummaryRequest {
            selection,
            output_cap: 512,
            input: SummaryInput {
                mode: CompactionMode::Normal,
                coverage_domain: pioneer_compaction::CoverageDomain::OwnContribution,
                previous_summary: String::new(),
                compact_units: vec![SummaryPart {
                    sources: vec![SourceRef {
                        scope: "item:turn".into(),
                        id: "command".into(),
                        version: "item-revision:1".into(),
                    }],
                    unit: 0,
                    part: 0,
                    last_part: true,
                    text: marker.clone(),
                }],
                reference_only: vec![],
                target_tokens: 512,
            },
        };
        let input = summarizer.input(&request).unwrap();
        assert_eq!(input.matches(&marker).count(), 1);
        assert!(summarizer.input_tokens(&request).unwrap() > 0);
    }

    #[tokio::test]
    async fn selected_cli_factory_passes_summary_contract_to_owned_print_adapter() {
        crate::compaction::load_test_catalog();
        let fixture = tempfile::tempdir().unwrap();
        let executable = fixture.path().join("mock-claude");
        let script = r#"#!/usr/bin/env python3
import json, os, pathlib, sys
if sys.argv[1:] == ['--version']:
    print('2.1.79 (Claude Code)'); sys.exit(0)
args = sys.argv[1:]
assert '--no-session-persistence' in args
assert args[args.index('--model')+1] == 'claude-sonnet-4-6'
assert args[args.index('--effort')+1] == 'high'
assert args[args.index('--tools')+1] == ''
instructions = args[args.index('--system-prompt')+1]
assert '## Goal and constraints' in instructions
request = json.load(sys.stdin)
assert request['previous_summary'] == 'previous Pioneer checkpoint'
assert request['compact_units'] == []
assert request['reference_only'] == []
assert request['target_tokens'] == 1024
pathlib.Path(os.environ['CLAUDE_CONFIG_DIR'], 'called').write_text('called')
headings = ['Goal and constraints','Decisions and rationale','Completed work and results','Failed attempts and unknowns','Current work and next step','Source references']
text = '\n\n'.join('## '+h+'\nfixture body' for h in headings)
assert '--verbose' in args
print(json.dumps([{'type':'system','subtype':'init','model':'claude-sonnet-4-6'}, {'type':'result','subtype':'success','is_error':False,'stop_reason':'end_turn','result':text,'modelUsage':{'claude-sonnet-4-6':{}},'usage':{'input_tokens':32,'output_tokens':100}}]))
"#;
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let instance = pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig {
            id: "selected-cli".into(),
            kind: GatewayCliAgentRuntimeKindConfig::Claude,
            display_name: "fixture".into(),
            enabled: true,
            binary_path: executable.to_string_lossy().into_owned(),
            home_path: fixture.path().to_string_lossy().into_owned(),
            shadow_home_path: None,
            custom_models: vec![],
            app_server_args: vec![],
            startup_probe_timeout_ms: 1000,
            request_timeout_ms: 1000,
            idle_session_ttl_secs: 60,
            event_channel_capacity: 32,
            stderr_ring_lines: 32,
            debug_native_events: false,
        };
        let selection = ModelSelection {
            transport: Transport::Claude,
            instance: "selected-cli".into(),
            model: "claude-sonnet-4-6".into(),
            effort: Some("high".into()),
        };
        let summarizer = make_cli_summarizer(
            instance.clone(),
            selection.clone(),
            SensitiveEnvironment::new(),
        )
        .unwrap();
        let request = SummaryRequest {
            selection: selection.clone(),
            output_cap: 1024,
            input: SummaryInput {
                mode: CompactionMode::Normal,
                coverage_domain: pioneer_compaction::CoverageDomain::OwnContribution,
                previous_summary: "previous Pioneer checkpoint".into(),
                compact_units: vec![],
                reference_only: vec![],
                target_tokens: 1024,
            },
        };
        assert!(summarizer.input_tokens(&request).unwrap() > 0);
        let result = summarizer.summarize(request.clone()).await.unwrap();
        assert!(
            validate_summary(&result, 1024)
                .unwrap()
                .contains(HEADINGS[5])
        );
        summarizer.cleanup().await.unwrap();
        assert!(fixture.path().join("called").exists());
        std::fs::remove_file(fixture.path().join("called")).unwrap();
        let mut changed = request;
        changed.selection.effort = Some("low".into());
        assert!(summarizer.summarize(changed).await.is_err());
        assert!(
            !fixture.path().join("called").exists(),
            "changed admitted selection must not launch a process"
        );
        let mut wrong = selection;
        wrong.transport = Transport::Codex;
        assert!(make_cli_summarizer(instance, wrong, SensitiveEnvironment::new()).is_err());
    }
}
