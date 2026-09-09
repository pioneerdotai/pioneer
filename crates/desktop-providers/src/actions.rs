use crate::providers::ProviderCatalogView;
use gpui_kit::{prelude::*, *};
use pioneer_client::providers::{
    cli_runtime_settings as cli_provider_settings, operations::*, types::*,
};
impl ProviderCatalogView {
    fn run_command(&mut self, command: ProviderCommand, cx: &mut Context<Self>) -> bool {
        if !self.visible() {
            return false;
        }
        let Some(workspace) = self.active_workspace_id().map(str::to_owned) else {
            return false;
        };
        if self.client.provider_command_intent(command).outcome()
            != pioneer_client::core::ClientTransitionOutcome::Changed
        {
            return false;
        }
        let Some(publication) = self.client.provider_operation_snapshot(&workspace) else {
            return false;
        };
        let Ok(operation) = self
            .client
            .take_provider_operation(&workspace, publication.generation())
        else {
            return false;
        };
        let generation = publication.generation();
        let client = std::sync::Arc::downgrade(&self.client);
        self.operation = Some(cx.spawn(async move |view: WeakEntity<Self>, cx| {
            let result = cx
                .background_spawn(async move { operation.execute(client) })
                .await;
            let _ = view.update(cx, |view, cx| {
                if view.active_workspace_id() != Some(workspace.as_str())
                    || !view.visible()
                    || !view
                        .client
                        .provider_operation_snapshot(&workspace)
                        .is_some_and(|p| {
                            p.generation() == generation
                                && p.request()
                                    == pioneer_client::providers::store::ProviderLoadState::Ready
                        })
                {
                    return;
                }
                if let Ok(ProviderOperationCompletion::Login(response)) = result {
                    let message = Some(cli_runtime_login_message(&response));
                    if view.providers.login_message != message {
                        view.providers.login_message = message;
                        cx.notify();
                    }
                }
            });
        }));
        true
    }
    pub(super) fn configure_provider(
        &mut self,
        provider: String,
        api_key: Option<String>,
        proxy_url: Option<String>,
        clear_proxy: bool,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            return false;
        };
        self.run_command(
            ProviderCommand::Configure(ProviderConfigureParams {
                workspace_id,
                provider,
                api_key,
                proxy_url,
                clear_proxy,
            }),
            cx,
        )
    }
    pub(super) fn delete_provider_api_key(
        &mut self,
        provider: String,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            return false;
        };
        self.run_command(
            ProviderCommand::Disconnect(ProviderDeleteApiKeyParams {
                workspace_id,
                provider,
            }),
            cx,
        )
    }
    pub(super) fn start_cli_runtime_login(&mut self, runtime_id: String, cx: &mut Context<Self>) {
        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            return;
        };
        self.run_command(
            ProviderCommand::Connect(CLIRuntimeLoginStartParams {
                workspace_id,
                runtime_id,
                login_type: CLIRuntimeLoginStartType::ChatgptDeviceCode,
            }),
            cx,
        );
    }
    pub(super) fn set_cli_runtime_proxy(
        &mut self,
        runtime_id: String,
        proxy_url: String,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            return false;
        };
        self.run_command(
            ProviderCommand::SetRuntimeProxy(CLIRuntimeProxySetParams {
                workspace_id,
                runtime_id,
                proxy_url,
            }),
            cx,
        )
    }
    pub(super) fn delete_cli_runtime_proxy(
        &mut self,
        runtime_id: String,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            return false;
        };
        self.run_command(
            ProviderCommand::RemoveRuntimeProxy(CLIRuntimeProxyDeleteParams {
                workspace_id,
                runtime_id,
            }),
            cx,
        )
    }
    pub(super) fn save_cli_runtime_provider_draft(
        &mut self,
        draft: cli_provider_settings::CLIRuntimeProviderDraft,
        cx: &mut Context<Self>,
    ) -> Result<(), cli_provider_settings::CLIRuntimeProviderSettingsRejection> {
        use cli_provider_settings::{
            CLIRuntimeProviderSettingsPlan, CLIRuntimeProviderSettingsRejection,
        };
        if let CLIRuntimeProviderSettingsPlan::Reject(rejection) =
            cli_provider_settings::plan_cli_runtime_provider_draft_update(
                self.gateway.settings.as_ref(),
                &draft,
            )
        {
            return Err(rejection);
        }
        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            return Err(CLIRuntimeProviderSettingsRejection::MissingSettings);
        };
        if !self.run_command(
            ProviderCommand::SaveRuntime {
                workspace_id,
                draft,
            },
            cx,
        ) {
            return Err(CLIRuntimeProviderSettingsRejection::MissingSettings);
        }
        self.providers.clear_cli_runtime_draft();
        Ok(())
    }
    pub(super) fn save_cli_runtime_provider_inline_field(
        &mut self,
        runtime_id: String,
        field: cli_provider_settings::CLIRuntimeProviderDraftField,
        value: String,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(instance) = cli_provider_settings::find_cli_runtime_provider_instance(
            self.gateway.settings.as_ref(),
            &runtime_id,
        )
        .cloned() else {
            return false;
        };
        let Some(value) = normalize_cli_runtime_provider_inline_field(&instance, field, &value)
        else {
            return false;
        };
        if current_cli_runtime_provider_inline_field_value(&instance, field) == value {
            return false;
        }
        let mut draft = cli_provider_settings::CLIRuntimeProviderDraft::edit(&instance);
        draft.set_text_field(field, value);
        if let Err(rejection) = self.save_cli_runtime_provider_draft(draft, cx) {
            self.providers.cli_error = Some(
                cli_provider_settings::cli_runtime_provider_settings_rejection_message(&rejection),
            );
            return false;
        }
        true
    }
    pub(super) fn toggle_cli_runtime_provider_enabled(
        &mut self,
        runtime_id: String,
        enabled: bool,
        cx: &mut Context<Self>,
    ) {
        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            return;
        };
        self.run_command(
            ProviderCommand::SetRuntimeEnabled {
                workspace_id,
                runtime_id,
                enabled,
            },
            cx,
        );
    }
    fn present(
        &self,
        intent: pioneer_client::providers::effects::ProviderPresentationIntent,
        cx: &mut App,
    ) -> bool {
        use pioneer_client::{core::*, providers::effects::ProviderPresentationEffect};
        let transition = self.client.provider_presentation_intent(intent);
        let Some(plan) = transition.effects().first() else {
            return false;
        };
        let identity = crate::ports::ProviderEffectIdentity::from_plan(plan);
        let completion = match plan.effect() {
            ClientPlannedEffect::ProviderPresentation(
                ProviderPresentationEffect::CopyDiagnostics { value },
            ) => self.credential.copy(identity.clone(), value, cx),
            ClientPlannedEffect::ProviderPresentation(ProviderPresentationEffect::OpenPath {
                path,
            }) => self.external.open_path(identity.clone(), path, cx),
            _ => return false,
        };
        if completion.identity() != &identity {
            return false;
        }
        self.client.complete_effect(ClientEffectCompletion::new(
            plan.operation_id().clone(),
            plan.generation(),
            if completion.succeeded() {
                ClientEffectResult::Completed
            } else {
                ClientEffectResult::Failed {
                    code: "provider_platform_effect_failed".into(),
                }
            },
        ));
        completion.succeeded()
    }
    pub(super) fn open_cli_runtime_provider_path(&mut self, path: String, cx: &mut App) {
        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            return;
        };
        if !self.present(
            pioneer_client::providers::effects::ProviderPresentationIntent::OpenPath {
                workspace_id,
                path,
            },
            cx,
        ) {
            self.providers.cli_error = Some(t!("providers.cli.error.open_path_failed").to_string());
        }
    }
    pub(super) fn copy_cli_runtime_provider_diagnostics(
        &mut self,
        runtime: RuntimeSummary,
        cx: &mut Context<Self>,
    ) {
        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            return;
        };
        if self.present(
            pioneer_client::providers::effects::ProviderPresentationIntent::CopyDiagnostics {
                workspace_id,
                runtime_id: runtime.runtime_id,
            },
            cx,
        ) {
            self.providers.login_message = Some(format!(
                "{} {}",
                t!("providers.cli.copied_diagnostics"),
                runtime.display_name
            ));
        }
    }
}
fn normalize_cli_runtime_provider_inline_field(
    instance: &pioneer_client::providers::types::GatewayCliRuntimeInstanceSettings,
    field: cli_provider_settings::CLIRuntimeProviderDraftField,
    value: &str,
) -> Option<String> {
    let trimmed = value.trim();
    Some(match field {
        cli_provider_settings::CLIRuntimeProviderDraftField::DisplayName => {
            if trimmed.is_empty() {
                default_cli_runtime_display_name(instance)
            } else {
                trimmed.to_owned()
            }
        }
        cli_provider_settings::CLIRuntimeProviderDraftField::BinaryPath => {
            if trimmed.is_empty() {
                cli_provider_settings::cli_runtime_provider_default_binary_path(instance.kind)
                    .to_owned()
            } else {
                trimmed.to_owned()
            }
        }
        cli_provider_settings::CLIRuntimeProviderDraftField::HomePath => {
            if trimmed.is_empty() {
                cli_provider_settings::cli_runtime_provider_default_home_path(instance.kind)
                    .to_owned()
            } else {
                trimmed.to_owned()
            }
        }
        cli_provider_settings::CLIRuntimeProviderDraftField::ShadowHomePath => trimmed.to_owned(),
        cli_provider_settings::CLIRuntimeProviderDraftField::Id => return None,
    })
}

fn current_cli_runtime_provider_inline_field_value(
    instance: &pioneer_client::providers::types::GatewayCliRuntimeInstanceSettings,
    field: cli_provider_settings::CLIRuntimeProviderDraftField,
) -> String {
    match field {
        cli_provider_settings::CLIRuntimeProviderDraftField::BinaryPath => {
            instance.binary_path.clone()
        }
        cli_provider_settings::CLIRuntimeProviderDraftField::HomePath => instance.home_path.clone(),
        cli_provider_settings::CLIRuntimeProviderDraftField::ShadowHomePath => {
            instance.shadow_home_path.clone().unwrap_or_default()
        }
        cli_provider_settings::CLIRuntimeProviderDraftField::DisplayName => {
            instance.display_name.clone()
        }
        cli_provider_settings::CLIRuntimeProviderDraftField::Id => String::new(),
    }
}

fn default_cli_runtime_display_name(
    instance: &pioneer_client::providers::types::GatewayCliRuntimeInstanceSettings,
) -> String {
    match instance.kind {
        pioneer_client::providers::types::CLIAgentRuntimeKind::Codex if instance.id == "codex" => {
            return cli_provider_settings::cli_runtime_provider_default_display_name(instance.kind)
                .to_owned();
        }
        pioneer_client::providers::types::CLIAgentRuntimeKind::Claude
            if instance.id == "claude" =>
        {
            return cli_provider_settings::cli_runtime_provider_default_display_name(instance.kind)
                .to_owned();
        }
        _ => {}
    }
    instance
        .id
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            let Some(first) = chars.next() else {
                return String::new();
            };
            let mut word = String::new();
            word.push(first.to_ascii_uppercase());
            word.push_str(chars.as_str());
            word
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn cli_runtime_login_message(
    response: &pioneer_client::providers::types::CLIRuntimeLoginStartResponse,
) -> String {
    let mut parts = Vec::new();
    if let Some(user_code) = response.user_code.as_deref() {
        parts.push(format!("{}: {user_code}", t!("providers.cli.login_code")));
    }
    if let Some(url) = response
        .verification_url
        .as_deref()
        .or(response.auth_url.as_deref())
    {
        parts.push(format!("{}: {url}", t!("providers.cli.login_open")));
    }
    response
        .message
        .clone()
        .or_else(|| (!parts.is_empty()).then(|| parts.join("  ")))
        .unwrap_or_else(|| t!("providers.cli.login_started").to_string())
}
