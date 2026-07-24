use crate::PromptRuntimeBuiltInSectionId;
use async_trait::async_trait;
use pioneer_hooks::{
    HookAwaitPolicy, HookCapabilities, HookCapability, HookContribution, HookContributionId,
    HookDefinition, HookDomain, HookError, HookExecutionPolicy, HookFailurePolicy, HookHandler,
    HookHandlerRequest, HookHandlerResponse, HookId, HookKind, HookPackage, HookPhase,
    HookPromptContent, HookRegistryError, HookSectionId, HookSourceId, HookSourceKind,
    HookSourceLabel, HookSourceRef, HookSubscription, HookSubscriptionId,
    HookSubscriptionVisibility, PromptSectionContribution,
};
use std::sync::Arc;

pub const AGENTS_DOC_PROMPT_HOOK_PACKAGE_ID: &str = "pioneer.prompt.agents_doc";
const THREAD_AGENTS_DOC_PROMPT_HOOK_ID: &str = "pioneer.thread_agents_doc_prompt";
const THREAD_AGENTS_DOC_PROMPT_HOOK_KIND: &str = "prompt_section";
const THREAD_AGENTS_DOC_PROMPT_SUBSCRIPTION_ID: &str =
    "pioneer.thread_agents_doc_prompt.turn_pre_prompt_compile";
const THREAD_AGENTS_DOC_PROMPT_DOMAIN: &str = "pioneer.thread_agents_doc";
const THREAD_AGENTS_DOC_PROMPT_MAX_CHARS: usize = 16_000;
const THREAD_AGENTS_DOC_PROMPT_PRIORITY: i32 = 600;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedAgentsDocPrompt {
    pub id: String,
    pub version: i64,
    pub content: String,
    pub source_path: Vec<String>,
}

#[async_trait]
pub trait AgentsDocPromptResolver: Send + Sync {
    async fn resolve_agents_doc_prompt(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> Result<Option<ResolvedAgentsDocPrompt>, String>;
}

pub fn agents_doc_package(
    resolver: Arc<dyn AgentsDocPromptResolver>,
) -> AgentsDocPromptHookPackage {
    AgentsDocPromptHookPackage { resolver }
}

pub struct AgentsDocPromptHookPackage {
    resolver: Arc<dyn AgentsDocPromptResolver>,
}

impl HookPackage for AgentsDocPromptHookPackage {
    fn id(&self) -> &'static str {
        AGENTS_DOC_PROMPT_HOOK_PACKAGE_ID
    }

    fn definitions(&self) -> Result<Vec<HookDefinition>, HookRegistryError> {
        let handler = Arc::new(ThreadAgentsDocPromptHook {
            resolver: self.resolver.clone(),
        });
        let hook_id = handler.id();
        let subscription_id = HookSubscriptionId::new(THREAD_AGENTS_DOC_PROMPT_SUBSCRIPTION_ID)
            .expect("static subscription id is valid");

        Ok(vec![HookDefinition::new(
            handler,
            [
                HookSubscription::new(subscription_id, hook_id, HookPhase::TurnPrePromptCompile)
                    .with_priority(0)
                    .with_execution_policy(HookExecutionPolicy {
                        await_policy: HookAwaitPolicy::Blocking,
                        timeout_ms: None,
                        max_parallelism: None,
                    })
                    .with_failure_policy(HookFailurePolicy::FailClosed)
                    .with_visibility(HookSubscriptionVisibility::Internal),
            ],
            AGENTS_DOC_PROMPT_HOOK_PACKAGE_ID,
        )])
    }
}

struct ThreadAgentsDocPromptHook {
    resolver: Arc<dyn AgentsDocPromptResolver>,
}

#[async_trait]
impl HookHandler for ThreadAgentsDocPromptHook {
    fn id(&self) -> HookId {
        HookId::new(THREAD_AGENTS_DOC_PROMPT_HOOK_ID).expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new(THREAD_AGENTS_DOC_PROMPT_HOOK_KIND).expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPrePromptCompile]
    }

    fn default_execution_policy(&self) -> HookExecutionPolicy {
        HookExecutionPolicy {
            await_policy: HookAwaitPolicy::Blocking,
            timeout_ms: None,
            max_parallelism: None,
        }
    }

    fn default_failure_policy(&self) -> HookFailurePolicy {
        HookFailurePolicy::FailClosed
    }

    fn capabilities(&self) -> HookCapabilities {
        HookCapabilities::new([
            HookCapability::new("thread_agents_doc").expect("static capability is valid"),
            HookCapability::new("contribute_prompt_section").expect("static capability is valid"),
        ])
    }

    async fn execute(
        &self,
        request: HookHandlerRequest,
    ) -> pioneer_hooks::HookResult<HookHandlerResponse> {
        if request.phase != HookPhase::TurnPrePromptCompile {
            return Ok(HookHandlerResponse::default());
        }

        let workspace_id = request
            .context
            .workspace_id
            .as_ref()
            .map(|id| id.as_str())
            .ok_or_else(|| {
                agents_doc_hook_error(
                    "thread_agents_doc.missing_workspace",
                    "AGENTS.md prompt hook request is missing workspace_id",
                )
            })?;
        let thread_id = request
            .context
            .effective_conversation_thread_id()
            .map(|id| id.as_str())
            .ok_or_else(|| {
                agents_doc_hook_error(
                    "thread_agents_doc.missing_thread",
                    "AGENTS.md prompt hook request is missing thread_id",
                )
            })?;

        let Some(resolved) = self
            .resolver
            .resolve_agents_doc_prompt(workspace_id, thread_id)
            .await
            .map_err(|error| {
                agents_doc_hook_error(
                    "thread_agents_doc.resolve_failed",
                    format!("failed to resolve AGENTS.md for prompt: {error}"),
                )
            })?
        else {
            return Ok(HookHandlerResponse::default());
        };

        let contribution = thread_agents_doc_prompt_contribution(&resolved)?;
        Ok(HookHandlerResponse {
            contributions: vec![HookContribution::PromptSection(contribution)],
            ..HookHandlerResponse::default()
        })
    }
}

fn thread_agents_doc_prompt_contribution(
    resolved: &ResolvedAgentsDocPrompt,
) -> pioneer_hooks::HookResult<PromptSectionContribution> {
    Ok(PromptSectionContribution {
        contribution_id: HookContributionId::new(format!(
            "thread_agents_doc.{}.v{}",
            resolved.id, resolved.version
        ))
        .map_err(|error| {
            agents_doc_hook_error(
                "thread_agents_doc.invalid_contribution_id",
                format!("invalid AGENTS.md prompt contribution id: {error}"),
            )
        })?,
        section_id: HookSectionId::new(PromptRuntimeBuiltInSectionId::AgentsMd.manifest_id())
            .expect("static section id is valid"),
        title: Some(
            pioneer_hooks::HookPromptSectionTitle::new("AGENTS.md")
                .expect("static section title is valid"),
        ),
        domain: HookDomain::new(THREAD_AGENTS_DOC_PROMPT_DOMAIN).expect("static domain is valid"),
        priority: THREAD_AGENTS_DOC_PROMPT_PRIORITY,
        content: HookPromptContent::new(render_thread_agents_doc_prompt_content(resolved))
            .map_err(|error| {
                agents_doc_hook_error(
                    "thread_agents_doc.invalid_prompt_content",
                    format!("invalid AGENTS.md prompt content: {error}"),
                )
            })?,
        max_chars: Some(THREAD_AGENTS_DOC_PROMPT_MAX_CHARS),
        source_refs: vec![thread_agents_doc_source_ref(resolved)?],
        diagnostics: Vec::new(),
        truncated: false,
    })
}

fn thread_agents_doc_source_ref(
    resolved: &ResolvedAgentsDocPrompt,
) -> pioneer_hooks::HookResult<HookSourceRef> {
    Ok(HookSourceRef {
        kind: HookSourceKind::Document,
        id: HookSourceId::new(resolved.id.clone()).map_err(|error| {
            agents_doc_hook_error(
                "thread_agents_doc.invalid_source_id",
                format!("invalid AGENTS.md source id: {error}"),
            )
        })?,
        label: Some(
            HookSourceLabel::new(thread_agents_doc_source_label(resolved))
                .expect("AGENTS.md source label is non-empty"),
        ),
    })
}

fn thread_agents_doc_source_label(resolved: &ResolvedAgentsDocPrompt) -> String {
    match resolved.source_path.as_slice() {
        [] => "AGENTS.md at thread tree root".to_owned(),
        path => format!("AGENTS.md at thread tree / {}", path.join(" / ")),
    }
}

fn render_thread_agents_doc_prompt_content(resolved: &ResolvedAgentsDocPrompt) -> String {
    format!(
        "These instructions come from the effective AGENTS.md for this thread tree scope. \
System, developer, and explicit user messages take precedence.\n\n\
<AGENTS_MD>\n{}\n</AGENTS_MD>",
        resolved.content.trim()
    )
}

fn agents_doc_hook_error(code: &'static str, message: impl Into<String>) -> HookError {
    let message = message.into();
    HookError::new(
        pioneer_hooks::HookDiagnosticCode::new(code).expect("static diagnostic code is valid"),
        pioneer_hooks::HookDiagnosticMessage::new(message)
            .expect("AGENTS.md hook error message is non-empty"),
    )
    .with_retryable(false)
    .with_safe_for_user(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_hooks::{
        HookContext, HookInput, HookPhaseRequest, HookRuntimeBuilder, HookThreadId, HookTurnId,
        HookWorkspaceId, TurnPrePromptCompileHookInput,
    };
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct CapturingResolver {
        calls: Mutex<Vec<(String, String)>>,
    }

    #[async_trait]
    impl AgentsDocPromptResolver for CapturingResolver {
        async fn resolve_agents_doc_prompt(
            &self,
            workspace_id: &str,
            thread_id: &str,
        ) -> Result<Option<ResolvedAgentsDocPrompt>, String> {
            self.calls
                .lock()
                .await
                .push((workspace_id.to_owned(), thread_id.to_owned()));
            Ok(Some(ResolvedAgentsDocPrompt {
                id: "agents-parent".to_owned(),
                version: 1,
                content: "PARENT AGENTS DOC".to_owned(),
                source_path: vec!["workspace".to_owned()],
            }))
        }
    }

    #[tokio::test]
    async fn agents_doc_hook_resolves_effective_conversation_thread() {
        let resolver = Arc::new(CapturingResolver::default());
        let runtime = HookRuntimeBuilder::new()
            .install(agents_doc_package(resolver.clone()))
            .expect("AGENTS.md package should install")
            .build();
        let request = HookPhaseRequest::new(
            HookPhase::TurnPrePromptCompile,
            HookContext {
                workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
                thread_id: Some(
                    HookThreadId::new("thread-child").expect("valid execution thread id"),
                ),
                conversation_thread_id: Some(
                    HookThreadId::new("thread-parent").expect("valid conversation thread id"),
                ),
                turn_id: Some(HookTurnId::new("turn-child").expect("valid turn id")),
                ..HookContext::default()
            },
            HookInput::turn_pre_prompt_compile(TurnPrePromptCompileHookInput::from_parts(
                false,
                Vec::new(),
            )),
        );

        let response = runtime
            .run_phase(request)
            .await
            .expect("AGENTS.md hook should run");

        assert_eq!(
            resolver.calls.lock().await.as_slice(),
            &[("ws".to_owned(), "thread-parent".to_owned())]
        );
        assert!(response.contributions.iter().any(|contribution| {
            matches!(
                contribution,
                HookContribution::PromptSection(section)
                    if section.content.as_str().contains("PARENT AGENTS DOC")
            )
        }));
    }
}
