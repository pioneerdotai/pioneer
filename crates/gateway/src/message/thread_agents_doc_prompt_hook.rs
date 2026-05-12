use super::*;
use async_trait::async_trait;
use pioneer_hooks::{
    HookAwaitPolicy, HookCapabilities, HookCapability, HookContribution, HookContributionId,
    HookDomain, HookError, HookExecutionPolicy, HookFailurePolicy, HookHandler, HookHandlerRequest,
    HookHandlerResponse, HookId, HookKind, HookPhase, HookPromptContent, HookPromptSectionTitle,
    HookRegistryError, HookSectionId, HookSourceId, HookSourceKind, HookSourceLabel, HookSourceRef,
    HookSubscription, HookSubscriptionId, HookSubscriptionVisibility, PromptSectionContribution,
};

const THREAD_AGENTS_DOC_PROMPT_HOOK_ID: &str = "pioneer.thread_agents_doc_prompt";
const THREAD_AGENTS_DOC_PROMPT_HOOK_KIND: &str = "prompt_section";
const THREAD_AGENTS_DOC_PROMPT_SUBSCRIPTION_ID: &str =
    "pioneer.thread_agents_doc_prompt.turn_pre_prompt_compile";
const THREAD_AGENTS_DOC_PROMPT_DOMAIN: &str = "pioneer.thread_agents_doc";
const THREAD_AGENTS_DOC_PROMPT_MAX_CHARS: usize = 16_000;
const THREAD_AGENTS_DOC_PROMPT_PRIORITY: i32 = 600;

pub(super) fn install_thread_agents_doc_prompt_hook(
    runtime: &Arc<HookRuntime>,
    crud_store: Arc<CrudStore>,
) -> Result<(), HookRegistryError> {
    let handler = Arc::new(ThreadAgentsDocPromptHook { crud_store });
    let hook_id = handler.id();
    if !runtime.handlers().contains_handler(&hook_id)? {
        runtime.handlers().register_handler(handler)?;
    }

    let subscription_id = HookSubscriptionId::new(THREAD_AGENTS_DOC_PROMPT_SUBSCRIPTION_ID)
        .expect("static subscription id is valid");
    if runtime
        .subscriptions()
        .get_subscription(&subscription_id)?
        .is_none()
    {
        runtime.subscriptions().register_subscription(
            runtime.handlers().as_ref(),
            HookSubscription::new(subscription_id, hook_id, HookPhase::TurnPrePromptCompile)
                .with_priority(0)
                .with_execution_policy(HookExecutionPolicy {
                    await_policy: HookAwaitPolicy::Blocking,
                    timeout_ms: None,
                    max_parallelism: None,
                })
                .with_failure_policy(HookFailurePolicy::FailClosed)
                .with_visibility(HookSubscriptionVisibility::Internal),
        )?;
    }

    Ok(())
}

struct ThreadAgentsDocPromptHook {
    crud_store: Arc<CrudStore>,
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
            .thread_id
            .as_ref()
            .map(|id| id.as_str())
            .ok_or_else(|| {
                agents_doc_hook_error(
                    "thread_agents_doc.missing_thread",
                    "AGENTS.md prompt hook request is missing thread_id",
                )
            })?;

        let Some(resolved) = self
            .crud_store
            .resolve_thread_agents_doc_for_thread(workspace_id, thread_id)
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
    resolved: &pioneer_crud::ResolvedThreadAgentsDocRecord,
) -> pioneer_hooks::HookResult<PromptSectionContribution> {
    Ok(PromptSectionContribution {
        contribution_id: HookContributionId::new(format!(
            "thread_agents_doc.{}.v{}",
            resolved.doc.id, resolved.doc.version
        ))
        .map_err(|error| {
            agents_doc_hook_error(
                "thread_agents_doc.invalid_contribution_id",
                format!("invalid AGENTS.md prompt contribution id: {error}"),
            )
        })?,
        section_id: HookSectionId::new(
            pioneer_promt::PromptRuntimeBuiltInSectionId::AgentsMd.manifest_id(),
        )
        .expect("static section id is valid"),
        title: Some(
            HookPromptSectionTitle::new("AGENTS.md").expect("static section title is valid"),
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
    resolved: &pioneer_crud::ResolvedThreadAgentsDocRecord,
) -> pioneer_hooks::HookResult<HookSourceRef> {
    Ok(HookSourceRef {
        kind: HookSourceKind::Document,
        id: HookSourceId::new(resolved.doc.id.clone()).map_err(|error| {
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

fn thread_agents_doc_source_label(
    resolved: &pioneer_crud::ResolvedThreadAgentsDocRecord,
) -> String {
    match resolved.source_path.as_slice() {
        [] => "AGENTS.md at thread tree root".to_owned(),
        path => format!("AGENTS.md at thread tree / {}", path.join(" / ")),
    }
}

fn render_thread_agents_doc_prompt_content(
    resolved: &pioneer_crud::ResolvedThreadAgentsDocRecord,
) -> String {
    let source = match resolved.source_path.as_slice() {
        [] => "thread tree / <root>".to_owned(),
        path => format!("thread tree / {}", path.join(" / ")),
    };

    format!(
        "Source: {source}\n\
Scope: applies to this thread unless a closer folder AGENTS.md overrides it.\n\
Precedence: system, developer, and explicit user messages override this section.\n\n\
<AGENTS_MD>\n{}\n</AGENTS_MD>",
        resolved.doc.content.trim()
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
