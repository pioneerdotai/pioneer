use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use gpui_kit::{prelude::*, *};
use pioneer_client::providers::{
    actions as provider_actions, cli_runtime_settings as cli_provider_settings,
    diagnostics as cli_provider_diagnostics,
};
use pioneer_protocol::{CLIRuntimeLoginStartType, RuntimeSummary};
use std::path::{Path, PathBuf};
use std::process::Command;
use tracing::warn;

impl PioneerDesktop {
    pub(super) fn configure_provider(
        &mut self,
        provider_id: String,
        api_key: Option<String>,
        proxy_url: Option<String>,
        clear_proxy: bool,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        let plan = provider_actions::plan_provider_configure(
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.active_workspace_id().map(str::to_owned),
            provider_id,
            api_key,
            proxy_url,
            clear_proxy,
        );
        let request = match plan {
            provider_actions::ProviderConfigurePlan::Send(request) => request,
            provider_actions::ProviderConfigurePlan::Unavailable(reason) => {
                self.apply_provider_api_key_unavailable(reason);
                return;
            }
        };

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
        provider_actions::mark_provider_api_key_action_started(&mut self.providers);

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let connection_id = request.connection_id;
            let canonical_provider_id = request.canonical_provider_id.clone();
            let params = request.params;

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.provider_configure(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !provider_actions::provider_api_key_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            provider_actions::apply_provider_configure_success(
                                &mut view.providers,
                                canonical_provider_id.clone(),
                                response.api_key_updated,
                                response.proxy_url,
                            );
                            if response.proxy_deleted {
                                provider_actions::apply_provider_proxy_deleted(
                                    &mut view.providers,
                                    canonical_provider_id.as_str(),
                                );
                            }
                        }
                        Err(error) => {
                            provider_actions::apply_provider_api_key_failure(
                                &mut view.providers,
                                format!("{}: {error:#}", t!("providers.error.save_failed")),
                            );
                            warn!(
                                provider = canonical_provider_id.as_str(),
                                error = %format!("{error:#}"),
                                "failed to configure provider"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn delete_provider_api_key(&mut self, provider_id: String, cx: &mut Context<Self>) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        let plan = provider_actions::plan_provider_delete_api_key(
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.active_workspace_id().map(str::to_owned),
            provider_id,
        );
        let request = match plan {
            provider_actions::ProviderDeleteApiKeyPlan::Send(request) => request,
            provider_actions::ProviderDeleteApiKeyPlan::Unavailable(reason) => {
                self.apply_provider_api_key_unavailable(reason);
                return;
            }
        };

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
        provider_actions::mark_provider_api_key_action_started(&mut self.providers);

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let connection_id = request.connection_id;
            let canonical_provider_id = request.canonical_provider_id.clone();
            let params = request.params;

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.provider_delete_api_key(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !provider_actions::provider_api_key_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    match result {
                        Ok(_) => {
                            provider_actions::apply_provider_delete_api_key_success(
                                &mut view.providers,
                                canonical_provider_id.as_str(),
                            );
                        }
                        Err(error) => {
                            provider_actions::apply_provider_api_key_failure(
                                &mut view.providers,
                                format!("{}: {error:#}", t!("providers.error.delete_failed")),
                            );
                            warn!(
                                provider = canonical_provider_id.as_str(),
                                error = %format!("{error:#}"),
                                "failed to delete provider api key"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn start_cli_runtime_login(&mut self, runtime_id: String, cx: &mut Context<Self>) {
        let plan = provider_actions::plan_cli_runtime_login_start(
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.active_workspace_id().map(str::to_owned),
            runtime_id,
            CLIRuntimeLoginStartType::ChatgptDeviceCode,
        );
        let request = match plan {
            provider_actions::CLIRuntimeLoginStartPlan::Send(request) => request,
            provider_actions::CLIRuntimeLoginStartPlan::Unavailable(reason) => {
                self.apply_cli_runtime_action_unavailable(reason);
                return;
            }
        };

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
        provider_actions::mark_provider_api_key_action_started(&mut self.providers);

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let connection_id = request.connection_id;
            let runtime_id = request.runtime_id.clone();
            let params = request.params;

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.cli_runtime_login_start(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !provider_actions::provider_api_key_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            view.providers.apply_cli_runtime_login_message(
                                cli_runtime_login_message(&response),
                            );
                        }
                        Err(error) => {
                            view.providers.apply_cli_runtime_action_failed(format!(
                                "{}: {error:#}",
                                t!("providers.error.save_failed")
                            ));
                            warn!(
                                runtime_id = runtime_id.as_str(),
                                error = %format!("{error:#}"),
                                "failed to start CLI runtime login"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn set_cli_runtime_proxy(
        &mut self,
        runtime_id: String,
        proxy_url: String,
        cx: &mut Context<Self>,
    ) {
        let plan = provider_actions::plan_cli_runtime_proxy_set(
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.active_workspace_id().map(str::to_owned),
            runtime_id,
            proxy_url,
        );
        let request = match plan {
            provider_actions::CLIRuntimeProxySetPlan::Send(request) => request,
            provider_actions::CLIRuntimeProxySetPlan::Unavailable(reason) => {
                self.apply_cli_runtime_action_unavailable(reason);
                return;
            }
        };

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
        provider_actions::mark_provider_api_key_action_started(&mut self.providers);

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let connection_id = request.connection_id;
            let runtime_id = request.runtime_id.clone();
            let params = request.params;

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.cli_runtime_proxy_set(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !provider_actions::provider_api_key_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            view.providers.apply_cli_runtime_proxy_url(
                                response.runtime_id.as_str(),
                                Some(response.proxy_url),
                            );
                        }
                        Err(error) => {
                            view.providers.apply_cli_runtime_action_failed(format!(
                                "{}: {error:#}",
                                t!("providers.error.save_failed")
                            ));
                            warn!(
                                runtime_id = runtime_id.as_str(),
                                error = %format!("{error:#}"),
                                "failed to set CLI runtime proxy"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn delete_cli_runtime_proxy(&mut self, runtime_id: String, cx: &mut Context<Self>) {
        let plan = provider_actions::plan_cli_runtime_proxy_delete(
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.active_workspace_id().map(str::to_owned),
            runtime_id,
        );
        let request = match plan {
            provider_actions::CLIRuntimeProxyDeletePlan::Send(request) => request,
            provider_actions::CLIRuntimeProxyDeletePlan::Unavailable(reason) => {
                self.apply_cli_runtime_action_unavailable(reason);
                return;
            }
        };

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
        provider_actions::mark_provider_api_key_action_started(&mut self.providers);

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let connection_id = request.connection_id;
            let runtime_id = request.runtime_id.clone();
            let params = request.params;

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.cli_runtime_proxy_delete(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !provider_actions::provider_api_key_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            view.providers
                                .apply_cli_runtime_proxy_url(response.runtime_id.as_str(), None);
                        }
                        Err(error) => {
                            view.providers.apply_cli_runtime_action_failed(format!(
                                "{}: {error:#}",
                                t!("providers.error.delete_failed")
                            ));
                            warn!(
                                runtime_id = runtime_id.as_str(),
                                error = %format!("{error:#}"),
                                "failed to delete CLI runtime proxy"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn save_cli_runtime_provider_draft(
        &mut self,
        draft: cli_provider_settings::CLIRuntimeProviderDraft,
        cx: &mut Context<Self>,
    ) -> Result<(), cli_provider_settings::CLIRuntimeProviderSettingsRejection> {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return Err(
                cli_provider_settings::CLIRuntimeProviderSettingsRejection::MissingSettings,
            );
        }
        let plan = cli_provider_settings::plan_cli_runtime_provider_draft_update(
            self.gateway.settings.as_ref(),
            &draft,
        );
        let update_plan = match plan {
            cli_provider_settings::CLIRuntimeProviderSettingsPlan::Send(plan) => plan,
            cli_provider_settings::CLIRuntimeProviderSettingsPlan::Reject(rejection) => {
                if matches!(
                    rejection,
                    cli_provider_settings::CLIRuntimeProviderSettingsRejection::MissingSettings
                ) {
                    self.refresh_gateway_settings(cx);
                }
                return Err(rejection);
            }
        };

        self.providers.clear_cli_runtime_draft();
        self.apply_gateway_settings_update(update_plan.snapshot, update_plan.update, cx);
        Ok(())
    }

    pub(super) fn save_cli_runtime_provider_inline_field(
        &mut self,
        runtime_id: String,
        field: cli_provider_settings::CLIRuntimeProviderDraftField,
        value: String,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        let Some(instance) = cli_provider_settings::find_cli_runtime_provider_instance(
            self.gateway.settings.as_ref(),
            runtime_id.as_str(),
        )
        .cloned() else {
            self.providers.apply_cli_runtime_action_failed(
                cli_provider_settings::cli_runtime_provider_settings_rejection_message(
                    &cli_provider_settings::CLIRuntimeProviderSettingsRejection::MissingRuntime {
                        runtime_id,
                    },
                ),
            );
            self.refresh_gateway_settings(cx);
            return;
        };

        let Some(value) =
            normalize_cli_runtime_provider_inline_field(&instance, field, value.as_str())
        else {
            return;
        };
        if current_cli_runtime_provider_inline_field_value(&instance, field) == value {
            return;
        }

        let mut draft = cli_provider_settings::CLIRuntimeProviderDraft::edit(&instance);
        draft.set_text_field(field, value);
        if let Err(rejection) = self.save_cli_runtime_provider_draft(draft, cx) {
            self.providers.apply_cli_runtime_action_failed(
                cli_provider_settings::cli_runtime_provider_settings_rejection_message(&rejection),
            );
        }
    }

    pub(super) fn toggle_cli_runtime_provider_enabled(
        &mut self,
        runtime_id: String,
        enabled: bool,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        let plan = cli_provider_settings::plan_cli_runtime_provider_enabled_update(
            self.gateway.settings.as_ref(),
            runtime_id.as_str(),
            enabled,
        );
        let update_plan = match plan {
            cli_provider_settings::CLIRuntimeProviderSettingsPlan::Send(plan) => plan,
            cli_provider_settings::CLIRuntimeProviderSettingsPlan::Reject(rejection) => {
                self.providers.apply_cli_runtime_action_failed(
                    cli_provider_settings::cli_runtime_provider_settings_rejection_message(
                        &rejection,
                    ),
                );
                if matches!(
                    rejection,
                    cli_provider_settings::CLIRuntimeProviderSettingsRejection::MissingSettings
                ) {
                    self.refresh_gateway_settings(cx);
                }
                return;
            }
        };

        self.apply_gateway_settings_update(update_plan.snapshot, update_plan.update, cx);
    }

    pub(super) fn open_cli_runtime_provider_path(&mut self, path: String) {
        match expand_cli_runtime_provider_path(path.as_str()).and_then(|path| open_path(&path)) {
            Ok(()) => {}
            Err(error) => {
                self.providers.apply_cli_runtime_action_failed(format!(
                    "{}: {error:#}",
                    t!("providers.cli.error.open_path_failed")
                ));
                warn!(
                    path = path.as_str(),
                    error = %format!("{error:#}"),
                    "failed to open CLI runtime provider path"
                );
            }
        }
    }

    pub(super) fn copy_cli_runtime_provider_diagnostics(
        &mut self,
        runtime: RuntimeSummary,
        cx: &mut Context<Self>,
    ) {
        let payload = cli_provider_diagnostics::cli_runtime_provider_diagnostics_json(&runtime);
        cx.write_to_clipboard(ClipboardItem::new_string(payload));
        self.providers.apply_cli_runtime_login_message(format!(
            "{} {}",
            t!("providers.cli.copied_diagnostics"),
            runtime.display_name
        ));
    }

    fn apply_provider_api_key_unavailable(
        &mut self,
        reason: provider_actions::ProviderApiKeyActionUnavailable,
    ) {
        let error = match reason {
            provider_actions::ProviderApiKeyActionUnavailable::GatewayNotConnected => {
                t!("providers.error.gateway_not_connected").to_string()
            }
            provider_actions::ProviderApiKeyActionUnavailable::WorkspaceNotSelected => {
                t!("providers.error.workspace_not_selected").to_string()
            }
        };
        provider_actions::apply_provider_api_key_failure(&mut self.providers, error);
    }

    fn apply_cli_runtime_action_unavailable(
        &mut self,
        reason: provider_actions::ProviderApiKeyActionUnavailable,
    ) {
        let error = match reason {
            provider_actions::ProviderApiKeyActionUnavailable::GatewayNotConnected => {
                t!("providers.error.gateway_not_connected").to_string()
            }
            provider_actions::ProviderApiKeyActionUnavailable::WorkspaceNotSelected => {
                t!("providers.error.workspace_not_selected").to_string()
            }
        };
        self.providers.apply_cli_runtime_action_failed(error);
    }
}

fn normalize_cli_runtime_provider_inline_field(
    instance: &pioneer_protocol::GatewayCliRuntimeInstanceSettings,
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
    instance: &pioneer_protocol::GatewayCliRuntimeInstanceSettings,
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
    instance: &pioneer_protocol::GatewayCliRuntimeInstanceSettings,
) -> String {
    match instance.kind {
        pioneer_protocol::CLIAgentRuntimeKind::Codex if instance.id == "codex" => {
            return cli_provider_settings::cli_runtime_provider_default_display_name(instance.kind)
                .to_owned();
        }
        pioneer_protocol::CLIAgentRuntimeKind::Claude if instance.id == "claude" => {
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

fn expand_cli_runtime_provider_path(raw: &str) -> anyhow::Result<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("path must not be empty");
    }
    let expanded = if trimmed == "~" {
        home_dir()?
    } else if let Some(rest) = trimmed.strip_prefix("~/") {
        home_dir()?.join(rest)
    } else if trimmed.starts_with('~') {
        anyhow::bail!("unsupported home expansion in `{raw}`");
    } else {
        PathBuf::from(trimmed)
    };
    if expanded.is_absolute() {
        Ok(expanded)
    } else {
        Ok(std::env::current_dir()?.join(expanded))
    }
}

fn home_dir() -> anyhow::Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("HOME is not set"))
}

fn open_path(path: &Path) -> anyhow::Result<()> {
    spawn_open_path(path)
}

#[cfg(target_os = "macos")]
fn spawn_open_path(path: &Path) -> anyhow::Result<()> {
    Command::new("open").arg(path).spawn()?;
    Ok(())
}

#[cfg(all(unix, not(target_os = "macos")))]
fn spawn_open_path(path: &Path) -> anyhow::Result<()> {
    Command::new("xdg-open").arg(path).spawn()?;
    Ok(())
}

#[cfg(windows)]
fn spawn_open_path(path: &Path) -> anyhow::Result<()> {
    Command::new("explorer").arg(path).spawn()?;
    Ok(())
}

fn cli_runtime_login_message(response: &pioneer_protocol::CLIRuntimeLoginStartResponse) -> String {
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
