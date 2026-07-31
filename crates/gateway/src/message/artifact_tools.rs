use super::{
    MessageProcessor, agent_runtime::TurnFailureRecoveryKind, artifact_finalization_diagnostics,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use pioneer_agent::{
    SkillContinuationAuthorizationContext, TurnFinalizationContext, TurnFinalizationDecision,
    TurnFinalizationProvider, TurnToolContext, TurnToolMaterialization, TurnToolProvider,
};
use pioneer_artifacts::{
    ARTIFACT_PREPARE_TOOL, ARTIFACT_READ_TOOL, ARTIFACT_REGISTER_TOOL, ArtifactReadToolParams,
    ArtifactToolContext, ArtifactToolHandler, ArtifactToolNotification,
    ArtifactToolNotificationSink, ArtifactToolState, artifact_tool_specs,
};
use pioneer_tools::{
    ToolError, ToolExtensionBundle, ToolHandler, ToolInvocation, ToolOutput, ToolPayload,
};
use std::sync::{Arc, Weak};
use tracing::warn;

pub(crate) struct GatewayArtifactToolProvider {
    processor: Weak<MessageProcessor>,
}

pub(crate) struct GatewayArtifactFinalizationProvider {
    processor: Weak<MessageProcessor>,
}

impl GatewayArtifactToolProvider {
    pub(crate) fn new(processor: Weak<MessageProcessor>) -> Self {
        Self { processor }
    }

    fn processor(&self) -> Result<Arc<MessageProcessor>, String> {
        self.processor
            .upgrade()
            .ok_or_else(|| "message processor is no longer available".to_owned())
    }
}

impl GatewayArtifactFinalizationProvider {
    pub(crate) fn new(processor: Weak<MessageProcessor>) -> Self {
        Self { processor }
    }

    fn processor(&self) -> Result<Arc<MessageProcessor>, String> {
        self.processor
            .upgrade()
            .ok_or_else(|| "message processor is no longer available".to_owned())
    }
}

#[async_trait]
impl TurnToolProvider for GatewayArtifactToolProvider {
    async fn materialize_turn_tools(
        &self,
        context: TurnToolContext,
    ) -> Result<TurnToolMaterialization, String> {
        let processor = self.processor()?;
        processor
            .revalidate_tool_execution_authorization(
                context.workspace_id.as_str(),
                context.thread_id.as_str(),
                context.turn_id.as_str(),
                None,
                crate::authorization::ResourceAction::ThreadRead,
            )
            .await
            .map_err(|_| "artifacts are unavailable for the current execution".to_owned())?;
        let artifact_state = processor
            .artifact_tool_state_for_turn(context.turn_id.as_str())
            .await;
        let artifact_context = ArtifactToolContext {
            workspace_id: context.workspace_id,
            thread_id: context.thread_id,
            turn_id: context.turn_id,
        };
        let artifact_handler = Arc::new(GatewayAuthorizedArtifactToolHandler {
            processor: processor.clone(),
            context: artifact_context.clone(),
            inner: Arc::new(ArtifactToolHandler::new(
                processor.artifact_service.clone(),
                artifact_context,
                artifact_state,
                Arc::new(GatewayArtifactToolNotificationSink {
                    processor: processor.clone(),
                }),
            )),
        });

        let mut bundle = ToolExtensionBundle::default();
        for configured in artifact_tool_specs() {
            let name = configured.spec.name.clone();
            bundle.specs.push(configured);
            bundle.handlers.push((name, artifact_handler.clone()));
        }

        Ok(TurnToolMaterialization {
            bundles: vec![bundle],
            diagnostics: Vec::new(),
        })
    }

    async fn authorize_skill_continuation(
        &self,
        context: SkillContinuationAuthorizationContext,
    ) -> Result<(), String> {
        let processor = self.processor()?;
        if let Err(error) = processor
            .authorize_persisted_turn_skill_continuation(
                context.workspace_id.as_str(),
                context.thread_id.as_str(),
                context.turn_id.as_str(),
                &context.skill_id,
                context.fingerprint.as_str(),
            )
            .await
        {
            warn!(
                workspace_id = context.workspace_id,
                thread_id = context.thread_id,
                turn_id = context.turn_id,
                skill_id = %context.skill_id,
                error = %format!("{error:#}"),
                "skill continuation authorization failed"
            );
            return Err("skill is no longer authorized for this turn".to_owned());
        }
        Ok(())
    }
}

struct GatewayAuthorizedArtifactToolHandler {
    processor: Arc<MessageProcessor>,
    context: ArtifactToolContext,
    inner: Arc<ArtifactToolHandler>,
}

#[async_trait]
impl ToolHandler for GatewayAuthorizedArtifactToolHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        trace: pioneer_tools::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let (thread_action, artifact_action) = match invocation.tool_name.as_str() {
            ARTIFACT_READ_TOOL => (
                crate::authorization::ResourceAction::ThreadRead,
                crate::authorization::ResourceAction::ArtifactRead,
            ),
            ARTIFACT_PREPARE_TOOL | ARTIFACT_REGISTER_TOOL => (
                crate::authorization::ResourceAction::ThreadWrite,
                crate::authorization::ResourceAction::ArtifactWrite,
            ),
            _ => return self.inner.handle(invocation, trace).await,
        };
        let current = self
            .processor
            .revalidate_tool_execution_authorization(
                self.context.workspace_id.as_str(),
                self.context.thread_id.as_str(),
                self.context.turn_id.as_str(),
                None,
                thread_action,
            )
            .await
            .map_err(|_| artifact_tool_authorization_error())?;

        if let Some(current) = current.as_ref() {
            crate::authorization::record_tool_decision(
                artifact_action,
                "artifact",
                current.authorization().decision(),
            );
        }

        if invocation.tool_name == ARTIFACT_READ_TOOL
            && let Some(current) = current
            && current.principal().kind == pioneer_protocol::PrincipalKind::User
        {
            let artifact_id = artifact_read_id(&invocation)?;
            let action_gate = crate::authorization::AuthorizationService::new().authorize_action(
                current.principal().kind,
                current.principal().role_key.as_ref(),
                crate::authorization::ResourceAction::ArtifactRead,
            );
            match crate::authorization::AuthorizationResolver::new(
                self.processor.crud_store.as_ref().clone(),
            )
            .authorize_artifact(
                current.principal(),
                &action_gate,
                crate::authorization::ResourceAction::ArtifactRead,
                artifact_id.as_str(),
                Some(current.authorization().workspace_id()),
                Some(current.authorization().thread_id()),
            )
            .await
            {
                Ok(crate::authorization::ProofResolution::Authorized(proof)) => {
                    crate::authorization::record_tool_decision(
                        crate::authorization::ResourceAction::ArtifactRead,
                        "artifact",
                        proof.decision(),
                    );
                }
                Ok(crate::authorization::ProofResolution::Denied(decision)) => {
                    crate::authorization::record_tool_decision(
                        crate::authorization::ResourceAction::ArtifactRead,
                        "artifact",
                        &decision,
                    );
                    return Err(artifact_tool_authorization_error());
                }
                Err(_) => {
                    crate::authorization::record_authorization_unavailable(
                        crate::authorization::ResourceAction::ArtifactRead.safe_name(),
                        "artifact",
                        "tool",
                    );
                    return Err(artifact_tool_authorization_error());
                }
            }
        }

        self.inner.handle(invocation, trace).await
    }
}

fn artifact_read_id(invocation: &ToolInvocation) -> Result<String, ToolError> {
    let ToolPayload::Function { arguments } = &invocation.payload else {
        return Err(ToolError::invalid_arguments(
            "artifact_read requires function arguments",
        ));
    };
    let params: ArtifactReadToolParams = serde_json::from_value(arguments.clone())
        .map_err(|_| ToolError::invalid_arguments("artifact_read arguments are invalid"))?;
    let artifact_id = params.artifact_id.trim();
    if artifact_id.is_empty() {
        return Err(ToolError::invalid_arguments("artifactId must not be empty"));
    }
    Ok(artifact_id.to_owned())
}

fn artifact_tool_authorization_error() -> ToolError {
    ToolError::execution_failed("artifact is unavailable for the current execution")
}

#[async_trait]
impl TurnFinalizationProvider for GatewayArtifactFinalizationProvider {
    async fn check_turn_finalization(
        &self,
        context: TurnFinalizationContext,
    ) -> Result<TurnFinalizationDecision, String> {
        let processor = self.processor()?;
        Ok(processor
            .artifact_finalization_decision(
                context.thread_id.as_str(),
                context.turn_id.as_str(),
                context.final_text.as_str(),
            )
            .await)
    }
}

struct GatewayArtifactToolNotificationSink {
    processor: Arc<MessageProcessor>,
}

#[async_trait]
impl ArtifactToolNotificationSink for GatewayArtifactToolNotificationSink {
    async fn send(&self, thread_id: &str, notification: ArtifactToolNotification) {
        let event_name = notification.event_name();
        match notification {
            ArtifactToolNotification::ArtifactCreated(payload) => {
                self.processor
                    .send_notification_to_thread_subscribers(thread_id, event_name, &payload)
                    .await;
            }
            ArtifactToolNotification::ThreadArtifactsChanged(payload) => {
                let workspace_id = payload.workspace_id;
                let source_thread_id = payload.thread_id;
                let artifact_ids = payload.artifact_ids;
                let reason = payload.reason;
                let generated_at = payload.generated_at;
                self.processor
                    .send_thread_artifacts_changed_to_thread_and_ancestors(
                        workspace_id.as_str(),
                        source_thread_id.as_str(),
                        artifact_ids,
                        reason.as_str(),
                        generated_at,
                    )
                    .await;
            }
            ArtifactToolNotification::ArtifactProjectionUpdated(payload) => {
                self.processor
                    .send_notification_to_thread_subscribers(thread_id, event_name, &payload)
                    .await;
            }
        }
    }
}

impl MessageProcessor {
    pub(super) async fn create_artifact_output_environment(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        let output_dir = pioneer_artifacts::create_artifact_output_dir(
            self.artifact_runtime_home.join("artifacts").as_path(),
            workspace_id,
            thread_id,
            turn_id,
        )
        .await
        .with_context(|| {
            format!(
                "failed to create artifact output directory for thread `{thread_id}` turn `{turn_id}`"
            )
        })?;

        let mut environment = std::collections::BTreeMap::new();
        environment.insert(
            pioneer_artifacts::PIONEER_ARTIFACT_OUTPUT_DIR_ENV.to_owned(),
            output_dir.path.display().to_string(),
        );
        self.artifact_output_dirs
            .lock()
            .await
            .insert(turn_id.to_owned(), output_dir.path.display().to_string());
        Ok(environment)
    }

    pub(crate) async fn bind_artifact_tool_bridge(self: &Arc<Self>) {
        self.agent_manager
            .set_turn_tool_provider(Some(Arc::new(GatewayArtifactToolProvider::new(
                Arc::downgrade(self),
            ))))
            .await;
        self.agent_manager
            .set_turn_finalization_provider(Some(Arc::new(
                GatewayArtifactFinalizationProvider::new(Arc::downgrade(self)),
            )))
            .await;
    }

    pub(crate) async fn artifact_tool_state_for_turn(
        &self,
        turn_id: &str,
    ) -> Arc<ArtifactToolState> {
        let mut states = self.artifact_tool_states.lock().await;
        states
            .entry(turn_id.to_owned())
            .or_insert_with(|| Arc::new(ArtifactToolState::default()))
            .clone()
    }

    pub(super) async fn artifact_finalization_decision(
        &self,
        thread_id: &str,
        turn_id: &str,
        final_text: &str,
    ) -> TurnFinalizationDecision {
        let diagnostics = self
            .artifact_finalization_diagnostics_for_final_text(turn_id, Some(final_text))
            .await;
        if diagnostics.is_empty() {
            return TurnFinalizationDecision::Allow;
        }

        let retry_already_used = self
            .artifact_finalization_retry_turns
            .lock()
            .await
            .contains(turn_id);
        if let Some(instruction) =
            artifact_finalization_diagnostics::artifact_finalization_retry_instruction(
                diagnostics.as_slice(),
                retry_already_used,
            )
            && self.mark_artifact_finalization_retry_used(turn_id).await
        {
            self.log_artifact_finalization_diagnostics(thread_id, turn_id, diagnostics.as_slice());
            return TurnFinalizationDecision::Retry { instruction };
        }

        self.log_artifact_finalization_diagnostics(thread_id, turn_id, diagnostics.as_slice());
        TurnFinalizationDecision::Fail {
            message: artifact_finalization_diagnostics::artifact_finalization_terminal_error(
                diagnostics.as_slice(),
            ),
        }
    }

    pub(super) async fn artifact_finalization_blocks_completion(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> bool {
        let final_text = self
            .turn_final_assistant_texts
            .lock()
            .await
            .get(turn_id)
            .cloned();
        let diagnostics = self
            .artifact_finalization_diagnostics_for_final_text(turn_id, final_text.as_deref())
            .await;
        if diagnostics.is_empty() {
            return false;
        }

        self.log_artifact_finalization_diagnostics(thread_id, turn_id, diagnostics.as_slice());
        self.report_turn_failure(
            thread_id.to_owned(),
            turn_id.to_owned(),
            TurnFailureRecoveryKind::ArtifactFinalization,
            artifact_finalization_diagnostics::artifact_finalization_terminal_error(
                diagnostics.as_slice(),
            ),
        )
        .await;
        true
    }

    async fn artifact_finalization_diagnostics_for_final_text(
        &self,
        turn_id: &str,
        final_text: Option<&str>,
    ) -> Vec<artifact_finalization_diagnostics::ArtifactFinalizationDiagnostic> {
        let prepared_outputs = self
            .artifact_tool_states
            .lock()
            .await
            .get(turn_id)
            .map(|state| state.prepared_outputs())
            .unwrap_or_default();
        let output_dir = self.artifact_output_dirs.lock().await.get(turn_id).cloned();

        artifact_finalization_diagnostics::diagnose_artifact_finalization(
            prepared_outputs.as_slice(),
            output_dir.as_deref(),
            final_text,
        )
    }

    fn log_artifact_finalization_diagnostics(
        &self,
        thread_id: &str,
        turn_id: &str,
        diagnostics: &[artifact_finalization_diagnostics::ArtifactFinalizationDiagnostic],
    ) {
        for diagnostic in diagnostics {
            warn!(
                thread_id,
                turn_id,
                code = diagnostic.code.as_str(),
                path = diagnostic.path.as_deref(),
                diagnostic = diagnostic.message.as_str(),
                "artifact finalization diagnostic"
            );
        }
    }

    async fn mark_artifact_finalization_retry_used(&self, turn_id: &str) -> bool {
        let mut retry_turns = self.artifact_finalization_retry_turns.lock().await;
        retry_turns.insert(turn_id.to_owned())
    }

    pub(super) async fn clear_artifact_finalization_state(&self, turn_id: &str) {
        self.artifact_tool_states.lock().await.remove(turn_id);
        self.artifact_output_dirs.lock().await.remove(turn_id);
        self.turn_final_assistant_texts.lock().await.remove(turn_id);
        self.artifact_finalization_retry_turns
            .lock()
            .await
            .remove(turn_id);
    }
}
