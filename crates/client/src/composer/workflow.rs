//! Composer preparation always consumes the draft captured by the operation.

use super::{
    capabilities::plan_composer_submission,
    skill_selection::ComposerSkillPickerProjection,
    store::{
        ComposerIntent, ComposerOperationCompletion, ComposerOperationIdentity,
        ComposerOperationKind, ComposerOperationPlan,
    },
    turn_prepare::{
        ComposerTurnPrepareTransport, PrepareComposerTurnRequest, PreparedComposerTurn,
        prepare_composer_turn,
    },
};
use crate::{
    core::{ClientCore, ClientTransitionOutcome},
    gateway::types::GatewayEndpointKind,
    platform::ClientFileSystem,
};

struct ComposerPreparationContext {
    pub workspace_id: String,
    pub turn_id: String,
    pub endpoint_kind: Option<GatewayEndpointKind>,
    pub skill_picker: ComposerSkillPickerProjection,
}

#[derive(Default)]
pub(crate) struct ComposerSendController {
    tasks: Vec<std::thread::JoinHandle<()>>,
}

impl Drop for ComposerSendController {
    fn drop(&mut self) {
        for task in self.tasks.drain(..) {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}

use super::{
    model_selection as composer_model_selection,
    turn_prepare::{
        ComposerSubmitAvailabilityInput, PreparedComposerTurnSubmitContext,
        can_submit_composer_message, reduce_prepared_composer_turn_submit_success,
    },
};
use crate::{
    providers::list::{
        cli_runtime_list_params, resolve_cli_runtime_execution_backend,
        runtime_id_from_cli_runtime_provider_key,
    },
    runtime::ClientRuntime,
    state::selectors as client_selectors,
    threads::session as thread_session,
    transport::ws::command_sender as ws_commands,
    turns::start::{now_unix_seconds, plan_turn_start_ids},
};
use pioneer_protocol::{
    AgentExecutionBackend, RuntimeSummary, ThreadComposerExecutionMode, ThreadMode,
};

pub struct ComposerSendContext {
    pub workspace_id: Option<String>,
    pub endpoint_kind: Option<GatewayEndpointKind>,
    pub failure_message: String,
}

pub struct ComposerSendResult {
    pub thread_id: String,
    pub turn_id: String,
    pub pending_request_id: String,
    pub semantic_timeline_patch: crate::timeline::semantic::SemanticTimelineCachePatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComposerTurnSelection {
    pub selected_model: Option<String>,
    pub selected_provider: Option<String>,
    pub selected_mode: ThreadMode,
}

fn non_empty_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedExecutionTarget {
    pub execution_backend: Option<AgentExecutionBackend>,
}

pub fn resolve_voice_turn_selection(
    inner: &ClientCore,
    thread_id: &str,
    requested_provider: Option<String>,
    requested_model: Option<String>,
    requested_mode: Option<ThreadMode>,
) -> anyhow::Result<ComposerTurnSelection> {
    let selected_mode =
        requested_mode.unwrap_or_else(composer_model_selection::default_composer_turn_mode);
    let coordinator = inner
        .thread_coordinator_snapshot(thread_id)
        .ok_or_else(|| anyhow::anyhow!("active thread must be opened before starting voice"))?;
    if selected_mode == ThreadMode::Message {
        return Ok(ComposerTurnSelection {
            selected_model: None,
            selected_provider: None,
            selected_mode,
        });
    }

    let requested_provider = non_empty_string(requested_provider);
    let requested_model = non_empty_string(requested_model);
    let requested_selection = match (requested_provider, requested_model) {
        (Some(provider), Some(model)) => Some(composer_model_selection::ComposerModelSelection {
            provider,
            model,
            selected_reasoning_effort: None,
        }),
        (None, None) => None,
        _ => {
            return Err(anyhow::anyhow!(
                "model and provider must both be selected before starting voice"
            ));
        }
    };
    let resolved_selection = match requested_selection {
        Some(selection) => selection,
        None => client_selectors::resolve_composer_model_selection_from(
            Some(thread_id),
            Some(coordinator.workspace_id.as_str()),
            &inner.thread_coordinator_snapshots(),
        )
        .ok_or_else(|| {
            anyhow::anyhow!("model and provider must be selected before starting voice")
        })?,
    };
    if !coordinator.conversation.can_submit_message() {
        return Err(anyhow::anyhow!(
            "active thread is not ready to start a new voice turn"
        ));
    }

    Ok(ComposerTurnSelection {
        selected_model: Some(resolved_selection.model),
        selected_provider: Some(resolved_selection.provider),
        selected_mode,
    })
}

pub fn resolve_selected_execution_target(
    runtime: &ClientRuntime,
    workspace_id: &str,
    selected_provider: Option<&str>,
) -> anyhow::Result<SelectedExecutionTarget> {
    let Some(provider_key) = selected_provider
        .map(str::trim)
        .filter(|provider| !provider.is_empty())
    else {
        return Ok(SelectedExecutionTarget {
            execution_backend: None,
        });
    };
    let Some(_) = runtime_id_from_cli_runtime_provider_key(provider_key) else {
        return Ok(SelectedExecutionTarget {
            execution_backend: None,
        });
    };

    let runtimes = ws_commands::cli_runtime_list(
        &runtime.ws_command_sender(),
        cli_runtime_list_params(workspace_id.to_owned()),
    )?
    .runtimes;

    selected_execution_target_from_runtimes(Some(provider_key), runtimes.as_slice())
}

pub fn selected_execution_target_from_runtimes(
    selected_provider: Option<&str>,
    runtimes: &[RuntimeSummary],
) -> anyhow::Result<SelectedExecutionTarget> {
    let Some(provider_key) = selected_provider
        .map(str::trim)
        .filter(|provider| !provider.is_empty())
    else {
        return Ok(SelectedExecutionTarget {
            execution_backend: None,
        });
    };
    let Some(_) = runtime_id_from_cli_runtime_provider_key(provider_key) else {
        return Ok(SelectedExecutionTarget {
            execution_backend: None,
        });
    };

    let execution_backend = resolve_cli_runtime_execution_backend(Some(provider_key), runtimes)
        .map_err(anyhow::Error::msg)?;
    Ok(SelectedExecutionTarget { execution_backend })
}

pub fn resolve_turn_selection(
    inner: &ClientCore,
    thread_id: &str,
    requested_provider: Option<String>,
    requested_model: Option<String>,
    requested_mode: Option<ThreadMode>,
    text: &str,
    has_attachments: bool,
    has_capabilities: bool,
) -> anyhow::Result<ComposerTurnSelection> {
    let selected_mode =
        requested_mode.unwrap_or_else(composer_model_selection::default_composer_turn_mode);
    let coordinator = inner
        .thread_coordinator_snapshot(thread_id)
        .ok_or_else(|| anyhow::anyhow!("active thread must be opened before starting turn"))?;

    if selected_mode == ThreadMode::Message {
        if !can_submit_composer_message(ComposerSubmitAvailabilityInput {
            gateway_connected: true,
            upload_in_progress: false,
            has_active_thread: true,
            selected_mode,
            has_complete_model_selection: true,
            // Message is an instant-completed Turn and never claims the
            // foreground execution slot held by Chat/Agent.
            conversation_can_submit: true,
            text,
            has_attachments,
            has_capabilities,
        }) {
            return Err(anyhow::anyhow!(
                "active thread is not ready to start a new turn"
            ));
        }

        return Ok(ComposerTurnSelection {
            selected_model: None,
            selected_provider: None,
            selected_mode,
        });
    }

    let requested_provider = non_empty_string(requested_provider);
    let requested_model = non_empty_string(requested_model);
    let requested_selection = match (requested_provider, requested_model) {
        (Some(provider), Some(model)) => Some(composer_model_selection::ComposerModelSelection {
            provider,
            model,
            selected_reasoning_effort: None,
        }),
        (None, None) => None,
        _ => {
            return Err(anyhow::anyhow!(
                "model and provider must both be selected before starting turn"
            ));
        }
    };
    let resolved_selection = match requested_selection {
        Some(selection) => selection,
        None => client_selectors::resolve_composer_model_selection_from(
            Some(thread_id),
            Some(coordinator.workspace_id.as_str()),
            &inner.thread_coordinator_snapshots(),
        )
        .ok_or_else(|| {
            anyhow::anyhow!("model and provider must be selected before starting turn")
        })?,
    };
    if !can_submit_composer_message(ComposerSubmitAvailabilityInput {
        gateway_connected: true,
        upload_in_progress: false,
        has_active_thread: true,
        selected_mode,
        has_complete_model_selection: true,
        conversation_can_submit: coordinator.conversation.can_submit_message(),
        text,
        has_attachments,
        has_capabilities,
    }) {
        return Err(anyhow::anyhow!(
            "active thread is not ready to start a new turn"
        ));
    }

    Ok(ComposerTurnSelection {
        selected_model: Some(resolved_selection.model),
        selected_provider: Some(resolved_selection.provider),
        selected_mode,
    })
}

impl ClientCore {
    pub(super) fn wait_composer_session_refresh(
        &self,
        identity: &ComposerOperationIdentity,
    ) -> anyhow::Result<()> {
        while self.gateway_refresh_in_flight() {
            anyhow::ensure!(
                self.composer_operation_plan(identity).is_some(),
                "Composer operation cancelled"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        anyhow::ensure!(
            self.composer_operation_plan(identity).is_some(),
            "Composer operation cancelled"
        );
        Ok(())
    }

    pub fn submit_composer_send<F: ClientFileSystem + ?Sized>(
        self: &std::sync::Arc<Self>,
        identity: ComposerOperationIdentity,
        file_system: &F,
        context: ComposerSendContext,
    ) -> anyhow::Result<ComposerSendResult> {
        anyhow::ensure!(
            self.composer_intent(ComposerIntent::PrepareOperation {
                identity: identity.clone()
            })
            .outcome()
                == ClientTransitionOutcome::Changed,
            "Composer send cancelled or already submitted"
        );
        let result = self
            .wait_composer_session_refresh(&identity)
            .and_then(|_| self.prepare_and_send_composer(identity.clone(), file_system, context));
        if let Err(error) = &result {
            self.complete_composer_operation(
                identity,
                ComposerOperationCompletion::Failed {
                    message: format!("{error:#}"),
                },
            );
        }
        result
    }

    fn prepare_and_send_composer<F: ClientFileSystem + ?Sized>(
        self: &std::sync::Arc<Self>,
        operation: ComposerOperationIdentity,
        file_system: &F,
        context: ComposerSendContext,
    ) -> anyhow::Result<ComposerSendResult> {
        let ComposerSendContext {
            workspace_id: requested_workspace_id,
            endpoint_kind,
            failure_message,
        } = context;
        let skill_picker =
            self.composer_catalog_skill_picker(&operation.thread_id, operation.draft_id, "");
        let runtime = self.compatibility_runtime();
        let plan = self
            .composer_operation_plan(&operation)
            .filter(|plan| plan.kind == ComposerOperationKind::Send)
            .ok_or_else(|| anyhow::anyhow!("Composer send cancelled"))?;
        let thread_id = Some(operation.thread_id.clone());
        let text = plan.draft.text;
        let domain = plan.draft.domain;
        let selected_model = domain.selected_model;
        let selected_provider = domain.selected_provider;
        let selected_reasoning_effort = domain.selected_reasoning_effort;
        let selected_mode = Some(domain.selected_mode);
        let reply_to_turn_id = domain.reply_target.map(|target| target.turn_id);
        let mentioned_principal_ids = domain
            .selected_mentions
            .iter()
            .map(|mention| mention.principal_id.clone())
            .collect();
        let permission_mode = domain.selected_permission_mode;
        let attachments = domain.attachments;
        let capabilities = domain.capabilities;
        let skill_selections = domain.skill_selections;

        let message_requested = selected_mode == Some(ThreadMode::Message);
        let message_has_execution_overrides = message_requested
            && [
                selected_model.as_deref(),
                selected_provider.as_deref(),
                selected_reasoning_effort.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|value| !value.trim().is_empty());
        if message_has_execution_overrides {
            return Err(anyhow::anyhow!(
                "Message does not accept model, provider, or reasoning overrides"
            ));
        }
        if message_requested && (!capabilities.is_empty() || !skill_selections.is_empty()) {
            return Err(anyhow::anyhow!(
                "Message does not accept execution capabilities"
            ));
        }

        let thread_id = thread_session::require_thread_id(thread_id, "sending text")
            .map_err(anyhow::Error::msg)?;
        let ids = plan_turn_start_ids();
        let turn_id = ids.turn_id;
        let pending_request_id = ids.pending_request_id;
        let (workspace_id, composer_execution_mode) = {
            let inner = self;
            let coordinator = inner
                .thread_coordinator_snapshot(thread_id.as_str())
                .ok_or_else(|| {
                    anyhow::anyhow!("active thread must be opened before starting turn")
                })?;
            (
                coordinator.workspace_id.clone(),
                coordinator
                    .thread()
                    .map(|thread| thread.origin_kind.composer_execution_mode())
                    .unwrap_or(ThreadComposerExecutionMode::ForegroundTurn),
            )
        };
        if let Some(requested_workspace_id) = requested_workspace_id.as_deref() {
            if requested_workspace_id != workspace_id {
                return Err(anyhow::anyhow!(
                    "active thread workspace `{workspace_id}` does not match composer workspace `{requested_workspace_id}`"
                ));
            }
        }

        // A planned access-token rotation replaces the WebSocket connection. The
        // replacement connection has no thread subscriptions, even though the
        // client-side thread coordinator is still warm. Re-establish the
        // authoritative workspace scope and subscription before any composer
        // preparation or optimistic local mutation.
        self.refresh_thread_subscription(&runtime.ws_command_sender(), &thread_id, &workspace_id)?;

        let selection = {
            let inner = self;
            resolve_turn_selection(
                inner,
                thread_id.as_str(),
                selected_provider,
                selected_model,
                selected_mode,
                text.as_str(),
                !attachments.is_empty(),
                !capabilities.is_empty() || !skill_selections.is_empty(),
            )?
        };
        let is_message = selection.selected_mode == ThreadMode::Message;
        let execution_target = if is_message {
            SelectedExecutionTarget {
                execution_backend: None,
            }
        } else {
            resolve_selected_execution_target(
                runtime,
                workspace_id.as_str(),
                selection.selected_provider.as_deref(),
            )?
        };
        let cli_runtime_selected = execution_target.execution_backend.is_some();
        let submission = plan_composer_submission(
            selection.selected_provider.as_deref(),
            text.as_str(),
            !attachments.is_empty(),
            capabilities.as_slice(),
        );
        if !submission.has_composer_payload && skill_selections.is_empty() {
            return Err(anyhow::anyhow!(
                "message content is required before starting turn"
            ));
        }
        let prepared = self.prepare_composer_send(
            &operation,
            &runtime.ws_command_sender(),
            file_system,
            ComposerPreparationContext {
                workspace_id: workspace_id.clone(),
                turn_id: turn_id.clone(),
                endpoint_kind,
                skill_picker,
            },
        )?;
        let turn_model_provider = if is_message || cli_runtime_selected {
            None
        } else {
            selection.selected_provider.clone()
        };
        let submit_reduction = reduce_prepared_composer_turn_submit_success(
            PreparedComposerTurnSubmitContext {
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                pending_request_id: pending_request_id.clone(),
                composer_execution_mode,
                selected_model: selection.selected_model,
                selected_provider: selection.selected_provider,
                turn_model_provider,
                selected_mode: Some(selection.selected_mode),
                reply_to_turn_id,
                mentioned_principal_ids,
                permission_mode,
                execution_backend: execution_target.execution_backend,
                selected_reasoning_effort,
                cli_runtime_options: None,
                updated_at_unix: now_unix_seconds(),
            },
            prepared,
        );
        let semantic_timeline_patch = self.send_prepared_composer_turn(
            operation,
            workspace_id,
            submit_reduction,
            failure_message,
        )?;

        Ok(ComposerSendResult {
            thread_id,
            turn_id,
            pending_request_id,
            semantic_timeline_patch,
        })
    }

    fn send_prepared_composer_turn(
        self: &std::sync::Arc<Self>,
        identity: ComposerOperationIdentity,
        workspace_id: String,
        reduction: super::turn_prepare::PreparedComposerTurnSubmitReduction,
        failure_message: String,
    ) -> anyhow::Result<crate::timeline::semantic::SemanticTimelineCachePatch> {
        self.send_prepared_composer_turn_using(
            identity,
            workspace_id,
            reduction,
            failure_message,
            self.compatibility_runtime().ws_command_sender(),
        )
    }

    fn send_prepared_composer_turn_using<
        T: crate::rpc::JsonRpcRequestTransport + Send + 'static,
    >(
        self: &std::sync::Arc<Self>,
        identity: ComposerOperationIdentity,
        workspace_id: String,
        reduction: super::turn_prepare::PreparedComposerTurnSubmitReduction,
        failure_message: String,
        sender: T,
    ) -> anyhow::Result<crate::timeline::semantic::SemanticTimelineCachePatch> {
        let patch = self
            .commit_composer_send(&identity, &workspace_id, &reduction)
            .ok_or_else(|| anyhow::anyhow!("Composer send cancelled or already submitted"))?;
        let token = self
            .thread_operation_token(&identity.thread_id)
            .ok_or_else(|| anyhow::anyhow!("Thread send cancelled"))?;
        let weak = std::sync::Arc::downgrade(self);
        let failed_identity = identity.clone();
        let failed_message = failure_message.clone();
        let failed_context = reduction.send_context.clone();
        let failed_token = self.thread_operation_token(&identity.thread_id);
        let task = std::thread::Builder::new()
            .name("client-composer-send".into())
            .spawn(move || {
                let current = || {
                    weak.upgrade()
                        .is_some_and(|core| core.composer_operation_plan(&identity).is_some())
                };
                let result = (|| -> anyhow::Result<_> {
                    anyhow::ensure!(current(), "Composer send cancelled");
                    anyhow::ensure!(current(), "Composer send cancelled");
                    ws_commands::turn_start(
                        &sender,
                        crate::turns::start::turn_start_params_from_plan(
                            reduction.turn_start_params_plan,
                        ),
                    )
                })();
                let Some(core) = weak.upgrade() else {
                    return;
                };
                let reduction = match result {
                    Ok(response) => crate::turns::start::reduce_turn_start_send_success(
                        reduction.send_context,
                        response,
                    ),
                    Err(_) => crate::turns::start::reduce_turn_start_send_failure(
                        reduction.send_context,
                        failure_message.clone(),
                    ),
                };
                let accepted = matches!(
                    reduction,
                    crate::turns::start::TurnStartSendReduction::Accepted { .. }
                );
                if core.apply_thread_start_send_result(token, reduction) {
                    core.complete_composer_operation(
                        identity,
                        if accepted {
                            ComposerOperationCompletion::Sent
                        } else {
                            ComposerOperationCompletion::Failed {
                                message: failure_message,
                            }
                        },
                    );
                }
            });
        match task {
            Ok(task) => {
                let mut owner = self
                    .composer_sends
                    .lock()
                    .expect("composer send controller poisoned");
                let mut pending = Vec::new();
                for previous in owner.tasks.drain(..) {
                    if previous.is_finished() {
                        let _ = previous.join();
                    } else {
                        pending.push(previous);
                    }
                }
                pending.push(task);
                owner.tasks = pending;
                Ok(patch)
            }
            Err(error) => {
                if let Some(token) = failed_token {
                    self.apply_thread_start_send_result(
                        token,
                        crate::turns::start::reduce_turn_start_send_failure(
                            failed_context,
                            failed_message.clone(),
                        ),
                    );
                }
                self.complete_composer_operation(
                    failed_identity,
                    ComposerOperationCompletion::Failed {
                        message: failed_message,
                    },
                );
                Err(error.into())
            }
        }
    }

    pub fn composer_operation_plan(
        &self,
        identity: &ComposerOperationIdentity,
    ) -> Option<ComposerOperationPlan> {
        if self.is_stopped() {
            return None;
        }
        let publication = self.composer_snapshot(&identity.thread_id)?;
        let operation = publication.operation()?;
        (publication.draft_id() == identity.draft_id
            && operation.identity == *identity
            && operation.pending())
        .then(|| operation.plan.clone())
        .flatten()
    }

    pub fn complete_composer_operation(
        &self,
        identity: ComposerOperationIdentity,
        completion: ComposerOperationCompletion,
    ) -> bool {
        self.composer_intent(ComposerIntent::CompleteOperation {
            identity,
            completion,
        })
        .outcome()
            == ClientTransitionOutcome::Changed
    }

    pub fn prepare_composer_voice<F: ClientFileSystem + ?Sized>(
        &self,
        identity: &ComposerOperationIdentity,
        file_system: &F,
        endpoint_kind: Option<GatewayEndpointKind>,
    ) -> anyhow::Result<super::turn_prepare::PreparedVoiceComposerSnapshot> {
        let skill_picker =
            self.composer_catalog_skill_picker(&identity.thread_id, identity.draft_id, "");
        let plan = self
            .composer_operation_plan(identity)
            .filter(|plan| plan.kind == ComposerOperationKind::Voice)
            .ok_or_else(|| anyhow::anyhow!("Composer voice operation cancelled"))?;
        anyhow::ensure!(
            self.composer_intent(ComposerIntent::PrepareOperation {
                identity: identity.clone()
            })
            .outcome()
                == ClientTransitionOutcome::Changed,
            "Composer voice operation already prepared"
        );
        let result = (|| {
            let context = plan
                .voice_start
                .ok_or_else(|| anyhow::anyhow!("Voice requires an opened thread"))?;
            let fingerprint = plan
                .authorization_fingerprint
                .ok_or_else(|| anyhow::anyhow!("Voice authorization context unavailable"))?;
            anyhow::ensure!(
                self.composer_snapshot(&identity.thread_id)
                    .is_some_and(|p| p.authorization_fingerprint() == Some(fingerprint.as_str())),
                "Voice draft policy changed"
            );
            anyhow::ensure!(
                self.thread_coordinator_snapshot(&identity.thread_id)
                    .is_some_and(|thread| thread.workspace_id == context.workspace_id),
                "Voice thread scope changed"
            );
            let domain = plan.draft.domain;
            let selection = resolve_voice_turn_selection(
                self,
                &identity.thread_id,
                domain.selected_provider,
                domain.selected_model,
                Some(domain.selected_mode),
            )?;
            let message_mode = selection.selected_mode == ThreadMode::Message;
            let execution_backend = if message_mode {
                None
            } else {
                resolve_selected_execution_target(
                    self.compatibility_runtime(),
                    &context.workspace_id,
                    selection.selected_provider.as_deref(),
                )?
                .execution_backend
            };
            let capabilities = if message_mode {
                vec![]
            } else {
                plan_composer_submission(
                    selection.selected_provider.as_deref(),
                    "",
                    !domain.attachments.is_empty(),
                    &domain.capabilities,
                )
                .capabilities
            };
            let turn_model_provider = if execution_backend.is_some() {
                None
            } else {
                selection.selected_provider.clone()
            };
            anyhow::ensure!(
                self.composer_intent(ComposerIntent::UploadOperation {
                    identity: identity.clone()
                })
                .outcome()
                    == ClientTransitionOutcome::Changed,
                "Composer voice operation cancelled"
            );
            let sender = self.compatibility_runtime().ws_command_sender();
            let transport = ComposerUploadTransport {
                core: self,
                identity,
                transport: &sender,
            };
            let prepared = super::turn_prepare::prepare_voice_composer_snapshot(
                &transport,
                file_system,
                super::turn_prepare::PrepareVoiceComposerSnapshotRequest {
                    authorization_fingerprint: fingerprint,
                    workspace_id: context.workspace_id,
                    thread_id: context.thread_id,
                    turn_id: context.turn_id,
                    endpoint_kind,
                    attachments: domain.attachments,
                    capabilities,
                    skill_selections: domain.skill_selections,
                    skill_picker,
                    selected_model: selection.selected_model,
                    selected_provider: selection.selected_provider,
                    turn_model_provider,
                    selected_mode: Some(selection.selected_mode),
                    permission_mode: domain.selected_permission_mode,
                    execution_backend,
                    selected_reasoning_effort: domain.selected_reasoning_effort,
                    cli_runtime_options: None,
                },
            )?;
            anyhow::ensure!(
                self.complete_composer_operation(
                    identity.clone(),
                    ComposerOperationCompletion::VoicePrepared {
                        snapshot: prepared.clone()
                    }
                ),
                "Composer voice operation cancelled"
            );
            Ok(prepared)
        })();
        if let Err(error) = &result {
            self.complete_composer_operation(
                identity.clone(),
                ComposerOperationCompletion::Failed {
                    message: format!("{error:#}"),
                },
            );
        }
        result
    }

    fn prepare_composer_send<T: ComposerTurnPrepareTransport, F: ClientFileSystem + ?Sized>(
        &self,
        identity: &ComposerOperationIdentity,
        transport: &T,
        file_system: &F,
        context: ComposerPreparationContext,
    ) -> anyhow::Result<PreparedComposerTurn> {
        let plan = self
            .composer_operation_plan(identity)
            .filter(|plan| plan.kind == ComposerOperationKind::Send)
            .ok_or_else(|| anyhow::anyhow!("Composer send cancelled"))?;
        anyhow::ensure!(
            self.composer_intent(ComposerIntent::UploadOperation {
                identity: identity.clone(),
            })
            .outcome()
                == ClientTransitionOutcome::Changed,
            "Composer send already prepared"
        );
        let domain = plan.draft.domain;
        let submission = plan_composer_submission(
            domain.selected_provider.as_deref(),
            plan.draft.text.trim(),
            !domain.attachments.is_empty(),
            &domain.capabilities,
        );
        let transport = ComposerUploadTransport {
            core: self,
            identity,
            transport,
        };
        let result = prepare_composer_turn(
            &transport,
            file_system,
            PrepareComposerTurnRequest {
                workspace_id: context.workspace_id,
                thread_id: identity.thread_id.clone(),
                turn_id: context.turn_id,
                endpoint_kind: context.endpoint_kind,
                text: plan.draft.text,
                attachments: domain.attachments,
                capabilities: submission.capabilities,
                skill_selections: domain.skill_selections,
                skill_picker: context.skill_picker,
            },
        );
        match result {
            Ok(prepared) => {
                anyhow::ensure!(
                    self.complete_composer_operation(
                        identity.clone(),
                        ComposerOperationCompletion::Uploaded {
                            artifacts: prepared
                                .attachments
                                .iter()
                                .map(|attachment| attachment.artifact.clone())
                                .collect()
                        }
                    ),
                    "Composer send cancelled"
                );
                Ok(prepared)
            }
            Err(error) => {
                self.complete_composer_operation(
                    identity.clone(),
                    ComposerOperationCompletion::Failed {
                        message: format!("{error:#}"),
                    },
                );
                Err(error)
            }
        }
    }
}

struct ComposerUploadTransport<'a, T> {
    core: &'a ClientCore,
    identity: &'a ComposerOperationIdentity,
    transport: &'a T,
}

impl<T> ComposerUploadTransport<'_, T> {
    fn require_current(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.core.composer_operation_plan(self.identity).is_some(),
            "Composer upload cancelled"
        );
        Ok(())
    }
}

impl<T: ComposerTurnPrepareTransport> ComposerTurnPrepareTransport
    for ComposerUploadTransport<'_, T>
{
    fn artifact_capabilities(
        &self,
        params: pioneer_protocol::ArtifactCapabilitiesParams,
    ) -> anyhow::Result<pioneer_protocol::ArtifactCapabilitiesResponse> {
        self.require_current()?;
        self.transport.artifact_capabilities(params)
    }
}

impl<T: ComposerTurnPrepareTransport> crate::artifacts::upload::ArtifactUploadTransport
    for ComposerUploadTransport<'_, T>
{
    fn artifact_upload_start(
        &self,
        params: pioneer_protocol::ArtifactUploadStartParams,
    ) -> anyhow::Result<pioneer_protocol::ArtifactUploadStartResponse> {
        self.require_current()?;
        self.transport.artifact_upload_start(params)
    }
    fn send_artifact_upload_chunk(
        &self,
        workspace_id: String,
        upload_id: String,
        offset: u64,
        chunk: Vec<u8>,
    ) -> anyhow::Result<pioneer_protocol::ArtifactUploadChunkAckNotification> {
        self.require_current()?;
        self.transport
            .send_artifact_upload_chunk(workspace_id, upload_id, offset, chunk)
    }
    fn artifact_upload_finish(
        &self,
        params: pioneer_protocol::ArtifactUploadFinishParams,
    ) -> anyhow::Result<pioneer_protocol::ArtifactUploadFinishResponse> {
        self.require_current()?;
        self.transport.artifact_upload_finish(params)
    }
    fn artifact_upload_abort(
        &self,
        params: pioneer_protocol::ArtifactUploadAbortParams,
    ) -> anyhow::Result<pioneer_protocol::ArtifactUploadAbortResponse> {
        // Cleanup names the upload already started by this operation, including after cancellation.
        self.transport.artifact_upload_abort(params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClientResult,
        artifacts::upload::ArtifactUploadTransport,
        composer::{
            attachments::{
                ComposerAttachment, ComposerAttachmentKind, ComposerAttachmentUploadState,
            },
            state_machine::{ComposerDomainAction, ComposerDomainState},
            store::ComposerOperationStatus,
        },
        platform::{ClientFileMetadata, ClientPath},
    };
    use pioneer_protocol::*;
    use std::{cell::Cell, sync::Arc};

    struct Files;
    impl ClientFileSystem for Files {
        fn read_file(&self, _: &ClientPath) -> ClientResult<Vec<u8>> {
            Ok(b"synthetic".to_vec())
        }
        fn metadata(&self, _: &ClientPath) -> ClientResult<ClientFileMetadata> {
            Ok(ClientFileMetadata {
                len: 9,
                modified: None,
                is_file: true,
                is_dir: false,
            })
        }
        fn write_cache_file(&self, _: &str, _: &[u8]) -> ClientResult<ClientPath> {
            panic!("upload must not write cache")
        }
    }
    struct Transport<'a> {
        starts: Cell<usize>,
        chunks: Cell<usize>,
        finishes: Cell<usize>,
        aborts: Cell<usize>,
        during_chunk: Box<dyn Fn() + 'a>,
    }
    impl<'a> Transport<'a> {
        fn new(during_chunk: impl Fn() + 'a) -> Self {
            Self {
                starts: Cell::new(0),
                chunks: Cell::new(0),
                finishes: Cell::new(0),
                aborts: Cell::new(0),
                during_chunk: Box::new(during_chunk),
            }
        }
    }
    impl ComposerTurnPrepareTransport for Transport<'_> {
        fn artifact_capabilities(
            &self,
            _: ArtifactCapabilitiesParams,
        ) -> anyhow::Result<ArtifactCapabilitiesResponse> {
            Ok(ArtifactCapabilitiesResponse {
                upload: ArtifactUploadCapabilities {
                    required_for_local_paths: true,
                    recommended_chunk_size_bytes: 3,
                    max_chunk_size_bytes: 3,
                    max_file_size_bytes: 100,
                    max_files_per_turn: 1,
                },
            })
        }
    }
    impl ArtifactUploadTransport for Transport<'_> {
        fn artifact_upload_start(
            &self,
            params: ArtifactUploadStartParams,
        ) -> anyhow::Result<ArtifactUploadStartResponse> {
            assert_eq!(params.thread_id.as_deref(), Some("a"));
            self.starts.set(self.starts.get() + 1);
            Ok(ArtifactUploadStartResponse {
                upload_id: "upload".into(),
                recommended_chunk_size_bytes: 3,
                max_chunk_size_bytes: 3,
                max_size_bytes: 100,
                expires_at_unix: 1,
            })
        }
        fn send_artifact_upload_chunk(
            &self,
            workspace_id: String,
            upload_id: String,
            offset: u64,
            chunk: Vec<u8>,
        ) -> anyhow::Result<ArtifactUploadChunkAckNotification> {
            self.chunks.set(self.chunks.get() + 1);
            (self.during_chunk)();
            let len = chunk.len() as u64;
            Ok(ArtifactUploadChunkAckNotification {
                workspace_id,
                upload_id,
                offset,
                len,
                received_bytes: offset + len,
                next_offset: offset + len,
            })
        }
        fn artifact_upload_finish(
            &self,
            params: ArtifactUploadFinishParams,
        ) -> anyhow::Result<ArtifactUploadFinishResponse> {
            self.finishes.set(self.finishes.get() + 1);
            Ok(ArtifactUploadFinishResponse {
                upload_id: params.upload_id,
                artifact: ArtifactRef {
                    artifact_id: "artifact".into(),
                    version_id: Some("version".into()),
                    display_name: "fixture.txt".into(),
                    kind: ArtifactKind::File,
                    mime_type: Some("text/plain".into()),
                    size_bytes: Some(9),
                    sha256: None,
                    status: ArtifactStatus::Ready,
                    preview: None,
                },
            })
        }
        fn artifact_upload_abort(
            &self,
            params: ArtifactUploadAbortParams,
        ) -> anyhow::Result<ArtifactUploadAbortResponse> {
            assert_eq!(params.upload_id, "upload");
            self.aborts.set(self.aborts.get() + 1);
            Ok(ArtifactUploadAbortResponse {
                upload_id: params.upload_id,
                status: "aborted".into(),
            })
        }
    }
    fn fixture() -> (Arc<ClientCore>, ComposerOperationIdentity) {
        let core = Arc::new(ClientCore::new());
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: ComposerDomainState::default(),
        });
        let draft_id = core.composer_snapshot("a").unwrap().draft_id();
        core.composer_intent(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id,
            text: "captured draft".into(),
        });
        core.composer_intent(ComposerIntent::Domain {
            thread_id: "a".into(),
            draft_id,
            action: ComposerDomainAction::AddAttachment {
                attachment: ComposerAttachment {
                    path: "/synthetic/fixture.txt".into(),
                    file_name: "fixture.txt".into(),
                    kind: ComposerAttachmentKind::File,
                    upload_state: ComposerAttachmentUploadState::Local,
                },
            },
        });
        core.composer_intent(ComposerIntent::BeginOperation {
            thread_id: "a".into(),
            draft_id,
            operation: ComposerOperationKind::Send,
        });
        let identity = core
            .composer_snapshot("a")
            .unwrap()
            .operation()
            .unwrap()
            .identity
            .clone();
        (core, identity)
    }
    fn context() -> ComposerPreparationContext {
        ComposerPreparationContext {
            workspace_id: "workspace".into(),
            turn_id: "turn".into(),
            endpoint_kind: None,
            skill_picker: Default::default(),
        }
    }
    #[test]
    fn preparation_consumes_captured_draft_and_duplicate_does_no_io() {
        let (core, identity) = fixture();
        let transport = Transport::new(|| {});
        let prepared = core
            .prepare_composer_send(&identity, &transport, &Files, context())
            .unwrap();
        assert!(
            matches!(&prepared.input[0], UserInput::Text { text, .. } if text == "captured draft")
        );
        assert_eq!(
            core.composer_snapshot("a")
                .unwrap()
                .operation()
                .unwrap()
                .status,
            ComposerOperationStatus::Prepared
        );
        let before = core.composer_snapshot("a").unwrap();
        assert!(
            core.prepare_composer_send(&identity, &transport, &Files, context())
                .is_err()
        );
        assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
        assert_eq!(
            (
                transport.starts.get(),
                transport.finishes.get(),
                transport.aborts.get()
            ),
            (1, 1, 0)
        );
    }
    #[test]
    fn editing_during_upload_aborts_old_upload_and_preserves_new_text() {
        let (core, identity) = fixture();
        let transport = Transport::new(|| {
            core.composer_intent(ComposerIntent::EditText {
                thread_id: "a".into(),
                draft_id: identity.draft_id,
                text: "new text".into(),
            });
        });
        assert!(
            core.prepare_composer_send(&identity, &transport, &Files, context())
                .is_err()
        );
        let input = core.composer_snapshot("a").unwrap();
        assert_eq!(input.draft().text, "new text");
        assert_eq!(
            input.operation().unwrap().status,
            ComposerOperationStatus::Cancelled
        );
        assert_eq!(
            input.domain().attachments[0].upload_state,
            ComposerAttachmentUploadState::Local
        );
        assert_eq!(
            (
                transport.chunks.get(),
                transport.finishes.get(),
                transport.aborts.get()
            ),
            (1, 0, 1)
        );
        assert!(
            !core.complete_composer_operation(identity.clone(), ComposerOperationCompletion::Sent)
        );
    }
    #[test]
    fn preflight_claim_is_exclusive_and_wrong_draft_does_no_io() {
        let (core, identity) = fixture();
        assert_eq!(
            core.composer_intent(ComposerIntent::PrepareOperation {
                identity: identity.clone()
            })
            .outcome(),
            ClientTransitionOutcome::Changed
        );
        let before = core.composer_snapshot("a").unwrap();
        assert_eq!(
            core.composer_intent(ComposerIntent::PrepareOperation {
                identity: identity.clone()
            })
            .outcome(),
            ClientTransitionOutcome::Noop
        );
        assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
        core.composer_intent(ComposerIntent::Clear {
            thread_id: "a".into(),
            draft_id: identity.draft_id,
        });
        let transport = Transport::new(|| panic!("stale operation must not upload"));
        assert!(
            core.prepare_composer_send(&identity, &transport, &Files, context())
                .is_err()
        );
        assert_eq!(transport.starts.get(), 0);
    }

    struct SendTransport {
        started: std::sync::mpsc::SyncSender<TurnStartParams>,
        reply: std::sync::mpsc::Receiver<bool>,
    }
    impl crate::rpc::JsonRpcRequestTransport for SendTransport {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(request["method"], "turn/start");
            let params: TurnStartParams =
                serde_json::from_value(request["params"].clone()).unwrap();
            self.started.send(params).map_err(|e| e.to_string())?;
            if !self.reply.recv().map_err(|e| e.to_string())? {
                return Err("synthetic rejection".into());
            }
            let result = TurnStartResponse {
                turn: Turn {
                    id: "turn".into(),
                    status: TurnStatus::InProgress,
                    turn_kind: TurnKind::Conversation,
                    origin: TurnOrigin::User,
                    mode: Default::default(),
                    author: None,
                    reply_to_turn_id: None,
                    mentions: vec![],
                    message_revision: 0,
                    message_deleted: false,
                    error: None,
                    prompt_manifest: None,
                    permission_profile: default_turn_permission_profile_snapshot(),
                },
            };
            response
                .send(Ok(serde_json::to_value(result).unwrap()))
                .map_err(|e| e.to_string())
        }
    }

    fn ready_send(
        core: &ClientCore,
        identity: &ComposerOperationIdentity,
    ) -> super::super::turn_prepare::PreparedComposerTurnSubmitReduction {
        core.upsert_thread(Thread {
            workspace_id: "workspace".into(),
            id: "a".into(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "model".into(),
            model_provider: "provider".into(),
            reasoning_effort: None,
            created_at: 1,
            updated_at: 1,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: vec![],
        });
        let prepared = core
            .prepare_composer_send(identity, &Transport::new(|| {}), &Files, context())
            .unwrap();
        reduce_prepared_composer_turn_submit_success(
            PreparedComposerTurnSubmitContext {
                thread_id: "a".into(),
                turn_id: "turn".into(),
                pending_request_id: "pending".into(),
                composer_execution_mode: ThreadComposerExecutionMode::ForegroundTurn,
                selected_model: Some("model".into()),
                selected_provider: Some("provider".into()),
                turn_model_provider: Some("provider".into()),
                selected_mode: Some(ThreadMode::Agent),
                reply_to_turn_id: None,
                mentioned_principal_ids: vec![],
                permission_mode: TurnPermissionMode::Supervised,
                execution_backend: None,
                selected_reasoning_effort: None,
                cli_runtime_options: None,
                updated_at_unix: 1,
            },
            prepared,
        )
    }
    fn join_sends(core: &ClientCore) {
        let tasks = std::mem::take(&mut core.composer_sends.lock().unwrap().tasks);
        for task in tasks {
            task.join().unwrap();
        }
    }

    #[test]
    fn send_acknowledgement_clears_only_matching_draft_and_failure_retains_payload() {
        for (draft, accepted) in [(true, true), (true, false), (false, true), (false, false)] {
            let (core, identity) = fixture();
            let reduction = ready_send(&core, &identity);
            if draft {
                core.remember_thread_draft("workspace", Some("a".into()));
            }
            let (started, receive) = std::sync::mpsc::sync_channel(1);
            let (reply, finish) = std::sync::mpsc::sync_channel(1);
            core.send_prepared_composer_turn_using(
                identity.clone(),
                "workspace".into(),
                reduction,
                "send failed".into(),
                SendTransport {
                    started,
                    reply: finish,
                },
            )
            .unwrap();
            receive
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            assert_eq!(
                core.composer_snapshot("a").unwrap().draft().text,
                "captured draft"
            );
            reply.send(accepted).unwrap();
            join_sends(&core);
            let input = core.composer_snapshot("a").unwrap();
            let published = core
                .snapshot(&crate::core::ClientScope::Composer {
                    thread_id: "a".into(),
                })
                .unwrap()
                .typed::<super::super::store::ComposerPublication>()
                .unwrap()
                .payload();
            assert!(
                Arc::ptr_eq(&input, &published),
                "send completion must reach Desktop/Mobile publications"
            );
            assert!(!published.operation().unwrap().pending());
            if accepted {
                assert!(input.draft().text.is_empty());
                assert_ne!(input.draft_id(), identity.draft_id);
                assert!(input.domain().attachments.is_empty());
                assert!(core.thread_workspace_draft("workspace").is_none());
            } else {
                assert_eq!(input.draft().text, "captured draft");
                assert_eq!(input.draft_id(), identity.draft_id);
                assert_eq!(input.domain().attachments.len(), 1);
                assert!(matches!(
                    input.operation().unwrap().status,
                    ComposerOperationStatus::Failed { .. }
                ));
            }
        }
    }

    #[test]
    fn late_send_acknowledgement_does_not_clear_a_new_edit_or_replacement_draft() {
        for replace in [false, true] {
            let (core, identity) = fixture();
            let reduction = ready_send(&core, &identity);
            let (started, receive) = std::sync::mpsc::sync_channel(1);
            let (reply, finish) = std::sync::mpsc::sync_channel(1);
            core.send_prepared_composer_turn_using(
                identity.clone(),
                "workspace".into(),
                reduction,
                "send failed".into(),
                SendTransport {
                    started,
                    reply: finish,
                },
            )
            .unwrap();
            receive
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            if replace {
                core.composer_intent(ComposerIntent::Clear {
                    thread_id: "a".into(),
                    draft_id: identity.draft_id,
                });
            }
            let draft_id = core.composer_snapshot("a").unwrap().draft_id();
            core.composer_intent(ComposerIntent::EditText {
                thread_id: "a".into(),
                draft_id,
                text: "new draft".into(),
            });
            let before = core.composer_snapshot("a").unwrap();
            reply.send(true).unwrap();
            join_sends(&core);
            assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
            assert_eq!(before.draft().text, "new draft");
            let published = core
                .snapshot(&crate::core::ClientScope::Composer {
                    thread_id: "a".into(),
                })
                .unwrap()
                .typed::<super::super::store::ComposerPublication>()
                .unwrap()
                .payload();
            assert!(
                Arc::ptr_eq(&before, &published),
                "edits during sending must remain published"
            );
        }
    }
}
