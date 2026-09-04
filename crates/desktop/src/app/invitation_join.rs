use crate::{
    app::root::PioneerDesktop,
    assets::PioneerIconName,
    components::profile_editor::{
        profile_avatar, profile_editor_header, profile_editor_page, profile_identity_group,
        profile_username_editor, profile_username_field,
    },
    gateway::{DesktopInvitationCommitError, DesktopInvitationRegistryRecovery, GatewayRuntime},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use gpui_kit::component::{
    input::{InputEvent, InputState},
    menu::{ContextMenuExt, PopupMenuItem},
    spinner::Spinner,
    theme::ActiveTheme,
    v_flex,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    gateway::invitation::{
        InvitationJoinField, InvitationJoinFlow, InvitationJoinPhase, InvitationJoinSafeProfile,
        InvitationQrPresentation,
    },
    transport::ws::auth_exchange::{AuthExchangeClient, InvitationExchangeErrorKind},
};
use pioneer_protocol::{
    ClientInstallationDescriptor, ClientKind, InvitationAcceptParams, InvitationPresentation,
    InvitationPreviewResponse, NewMemberProfile, PROFILE_AVATAR_MAX_DECODED_BYTES,
    PROFILE_AVATAR_MAX_DIMENSION, PioneerAppUrlScheme, ProfileAvatarInput, ProfileAvatarMediaType,
};
use std::{path::PathBuf, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DesktopInvitationJoinScreen {
    Profile,
    Username,
}

pub(super) struct DesktopInvitationJoinState {
    flow: InvitationJoinFlow,
    preview: Option<InvitationPreviewResponse>,
    first_name: Entity<InputState>,
    last_name: Entity<InputState>,
    nickname: Entity<InputState>,
    avatar_path: Option<PathBuf>,
    screen: DesktopInvitationJoinScreen,
    username_before_edit: Option<String>,
    loading_preview: bool,
    submitting: bool,
    name_error: Option<String>,
    nickname_error: Option<String>,
    avatar_error: Option<String>,
    submit_error: Option<String>,
    pending_registry_recovery: Option<DesktopInvitationRegistryRecovery>,
}

impl DesktopInvitationJoinState {
    fn new(flow: InvitationJoinFlow, window: &mut Window, cx: &mut App) -> Self {
        Self {
            flow,
            preview: None,
            first_name: cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder(t!("settings.profile.first_name").to_string())
            }),
            last_name: cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder(t!("settings.profile.last_name").to_string())
            }),
            nickname: cx.new(|cx| {
                InputState::new(window, cx).placeholder(t!("settings.profile.username").to_string())
            }),
            avatar_path: None,
            screen: DesktopInvitationJoinScreen::Profile,
            username_before_edit: None,
            loading_preview: true,
            submitting: false,
            name_error: None,
            nickname_error: None,
            avatar_error: None,
            submit_error: None,
            pending_registry_recovery: None,
        }
    }

    fn inputs(&self) -> [Entity<InputState>; 3] {
        [
            self.first_name.clone(),
            self.last_name.clone(),
            self.nickname.clone(),
        ]
    }

    fn display_name(&self, cx: &App) -> String {
        [
            self.first_name.read(cx).value().trim(),
            self.last_name.read(cx).value().trim(),
        ]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
    }

    fn nickname(&self, cx: &App) -> String {
        self.nickname.read(cx).value().trim().to_owned()
    }

    fn display_name_valid(&self, cx: &App) -> bool {
        NewMemberProfile::new(self.display_name(cx), "member", None).is_ok()
    }

    fn nickname_valid(&self, cx: &App) -> bool {
        NewMemberProfile::new("Member", self.nickname(cx), None).is_ok()
    }

    fn can_complete(&self, cx: &App) -> bool {
        match self.screen {
            DesktopInvitationJoinScreen::Profile => {
                self.preview.is_some()
                    && !self.loading_preview
                    && !self.submitting
                    && self.flow.safe_state().phase != InvitationJoinPhase::Terminal
                    && self.display_name_valid(cx)
                    && self.nickname_valid(cx)
            }
            DesktopInvitationJoinScreen::Username => {
                let nickname = self.nickname(cx);
                self.nickname_valid(cx)
                    && self.username_before_edit.as_deref() != Some(nickname.as_str())
            }
        }
    }

    fn replace_uri(&mut self, uri: &str) -> anyhow::Result<bool> {
        let transition = self.flow.deliver_uri(uri)?;
        if transition.duplicate_delivery {
            return Ok(false);
        }
        self.preview = None;
        self.avatar_path = None;
        self.screen = DesktopInvitationJoinScreen::Profile;
        self.username_before_edit = None;
        self.loading_preview = true;
        self.submitting = false;
        self.name_error = None;
        self.nickname_error = None;
        self.avatar_error = None;
        self.submit_error = None;
        self.pending_registry_recovery = None;
        Ok(true)
    }

    fn presentation(&self) -> anyhow::Result<InvitationQrPresentation> {
        Ok(self.flow.presentation()?.clone())
    }

    fn apply_preview(&mut self, preview: InvitationPreviewResponse) {
        self.preview = Some(preview);
        self.loading_preview = false;
        self.submit_error = None;
        self.flow.preview_succeeded();
    }

    fn apply_preview_error(&mut self) {
        self.loading_preview = false;
        self.submit_error = Some(t!("invitation.join.error.unavailable").to_string());
        self.flow.terminal_failure(false);
    }

    fn update_safe_profile(&mut self, cx: &App) {
        self.flow.update_safe_profile(InvitationJoinSafeProfile {
            display_name: self.display_name(cx),
            nickname: self.nickname(cx),
            has_avatar: self.avatar_path.is_some(),
        });
    }

    fn validate(&mut self, cx: &App) -> bool {
        self.update_safe_profile(cx);
        let display_name_valid = self.display_name_valid(cx);
        let nickname_valid = self.nickname_valid(cx);
        self.name_error =
            (!display_name_valid).then(|| t!("invitation.join.error.display_name").to_string());
        self.nickname_error =
            (!nickname_valid).then(|| t!("invitation.join.error.nickname").to_string());
        self.submit_error = None;
        display_name_valid && nickname_valid
    }

    fn cancel(&mut self) {
        self.flow.cancel();
        self.avatar_path = None;
        self.preview = None;
        self.username_before_edit = None;
        self.name_error = None;
        self.nickname_error = None;
        self.avatar_error = None;
        self.submit_error = None;
        self.pending_registry_recovery = None;
    }

    fn profile(&self, cx: &App) -> anyhow::Result<NewMemberProfile> {
        let avatar = self
            .avatar_path
            .as_deref()
            .map(load_desktop_profile_avatar)
            .transpose()?;
        NewMemberProfile::new(self.display_name(cx), self.nickname(cx), avatar)
            .map_err(anyhow::Error::new)
    }
}

enum DesktopJoinTaskResult {
    Committed {
        endpoint_id: String,
        endpoint_name: String,
    },
    RegistryRecovery(DesktopInvitationRegistryRecovery),
    FieldError(InvitationJoinField),
    Retryable,
    Terminal,
}

impl PioneerDesktop {
    pub(crate) fn handle_invitation_url(
        &mut self,
        uri: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Ok(presentation) = InvitationPresentation::parse(uri.as_str()) else {
            return;
        };
        if presentation.app_url_scheme() != PioneerAppUrlScheme::for_current_build() {
            return;
        }

        if let Some(state) = self.invitation_join.clone() {
            let should_preview = state
                .update(cx, |state, cx| {
                    let result = state.replace_uri(uri.as_str());
                    cx.notify();
                    result
                })
                .ok()
                .unwrap_or(false);
            if should_preview {
                self.preview_desktop_invitation(state, cx);
            }
            return;
        }

        let Ok((flow, _)) = InvitationJoinFlow::from_uri(uri.as_str()) else {
            self.show_invalid_invitation(window, cx);
            return;
        };
        let state = cx.new(|cx| DesktopInvitationJoinState::new(flow, window, cx));
        let inputs = state.read(cx).inputs();
        self.invitation_join_input_subscriptions = inputs
            .iter()
            .enumerate()
            .map(|(index, input)| {
                cx.subscribe(input, move |view, _, event: &InputEvent, cx| {
                    if !matches!(event, InputEvent::Change) {
                        return;
                    }
                    if let Some(state) = view.invitation_join.as_ref() {
                        state.update(cx, |state, _| {
                            if index < 2 {
                                state.name_error = None;
                            } else {
                                state.nickname_error = None;
                            }
                            state.submit_error = None;
                        });
                    }
                    cx.notify();
                })
            })
            .collect();
        self.invitation_join = Some(state.clone());
        inputs[0].update(cx, |input, cx| input.focus(window, cx));
        cx.notify();
        self.preview_desktop_invitation(state, cx);
    }

    fn show_invalid_invitation(&self, window: &mut Window, cx: &mut Context<Self>) {
        let _ = window.prompt(
            PromptLevel::Critical,
            t!("invitation.join.error.title").to_string().as_str(),
            Some(t!("invitation.join.error.invalid").to_string().as_str()),
            &[PromptButton::ok(t!("buttons.ok").to_string())],
            cx,
        );
    }

    fn preview_desktop_invitation(
        &mut self,
        state: Entity<DesktopInvitationJoinState>,
        cx: &mut Context<Self>,
    ) {
        let Ok(presentation) = state.read(cx).presentation() else {
            return;
        };
        cx.spawn(move |_this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = AuthExchangeClient::new(Duration::from_secs(15))
                    .preview_invitation(&presentation)
                    .await;
                let _ = state.update(&mut cx, |state, cx| {
                    match result {
                        Ok(preview) => state.apply_preview(preview),
                        Err(_) => state.apply_preview_error(),
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn render_desktop_invitation_join(
        &self,
        state: Entity<DesktopInvitationJoinState>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let desktop = cx.entity().clone();
        let (
            screen,
            loading,
            terminal,
            submitting,
            can_complete,
            first_name,
            last_name,
            nickname,
            display_name,
            avatar_path,
            name_error,
            nickname_error,
            avatar_error,
            submit_error,
        ) = {
            let snapshot = state.read(cx);
            (
                snapshot.screen,
                snapshot.loading_preview,
                snapshot.flow.safe_state().phase == InvitationJoinPhase::Terminal,
                snapshot.submitting,
                snapshot.can_complete(cx),
                snapshot.first_name.clone(),
                snapshot.last_name.clone(),
                snapshot.nickname.clone(),
                snapshot.display_name(cx),
                snapshot.avatar_path.clone(),
                snapshot.name_error.clone(),
                snapshot.nickname_error.clone(),
                snapshot.avatar_error.clone(),
                snapshot.submit_error.clone(),
            )
        };
        let title = match screen {
            DesktopInvitationJoinScreen::Profile => t!("invitation.join.title"),
            DesktopInvitationJoinScreen::Username => t!("settings.profile.edit_username_title"),
        }
        .to_string();
        let header = profile_editor_header(
            title,
            match screen {
                DesktopInvitationJoinScreen::Profile => "invitation-join-back",
                DesktopInvitationJoinScreen::Username => "invitation-username-back",
            },
            match screen {
                DesktopInvitationJoinScreen::Profile => "invitation-join-submit",
                DesktopInvitationJoinScreen::Username => "invitation-username-done",
            },
            t!("settings.profile.done").to_string(),
            submitting,
            can_complete,
            {
                let state = state.clone();
                let desktop = desktop.clone();
                move |_, window, cx| {
                    let _ = desktop.update(cx, |view, cx| match screen {
                        DesktopInvitationJoinScreen::Profile => {
                            view.cancel_desktop_invitation(state.clone(), cx)
                        }
                        DesktopInvitationJoinScreen::Username => {
                            view.cancel_invitation_username_editor(state.clone(), window, cx)
                        }
                    });
                }
            },
            {
                let state = state.clone();
                let desktop = desktop.clone();
                move |_, window, cx| {
                    let _ = desktop.update(cx, |view, cx| match screen {
                        DesktopInvitationJoinScreen::Profile => {
                            view.accept_desktop_invitation(window, cx)
                        }
                        DesktopInvitationJoinScreen::Username => {
                            view.complete_invitation_username_editor(state.clone(), cx)
                        }
                    });
                }
            },
            cx,
        );

        if loading || terminal {
            let content = v_flex()
                .w_full()
                .items_center()
                .gap_3()
                .py_12()
                .when(loading, |this| {
                    this.child(Spinner::new()).child(
                        div()
                            .text_sm()
                            .opacity(0.65)
                            .child(t!("invitation.join.loading").to_string()),
                    )
                })
                .into_any_element();
            return profile_editor_page(
                "invitation-state-scroll",
                header,
                content,
                submit_error,
                cx,
            );
        }

        let content = match screen {
            DesktopInvitationJoinScreen::Profile => {
                let has_avatar = avatar_path.is_some();
                let avatar = div()
                    .id("invitation-avatar-edit")
                    .relative()
                    .flex_none()
                    .cursor_pointer()
                    .on_click({
                        let desktop = desktop.clone();
                        move |_, _, cx| {
                            let _ = desktop.update(cx, |view, cx| view.pick_invitation_avatar(cx));
                        }
                    })
                    .context_menu({
                        let desktop = desktop.clone();
                        let state = state.clone();
                        move |menu, _, _| {
                            let change_desktop = desktop.clone();
                            let remove_state = state.clone();
                            menu.min_w(px(200.))
                                .item(
                                    PopupMenuItem::new(
                                        t!("settings.profile.change_photo").to_string(),
                                    )
                                    .icon(PioneerIconName::Pen)
                                    .disabled(submitting)
                                    .on_click(
                                        move |_, _, cx| {
                                            let _ = change_desktop.update(cx, |view, cx| {
                                                view.pick_invitation_avatar(cx)
                                            });
                                        },
                                    ),
                                )
                                .item(
                                    PopupMenuItem::new(
                                        t!("settings.profile.remove_photo").to_string(),
                                    )
                                    .icon(PioneerIconName::Trash)
                                    .disabled(submitting || !has_avatar)
                                    .on_click(
                                        move |_, _, cx| {
                                            let _ = remove_state.update(cx, |state, cx| {
                                                state.avatar_path = None;
                                                state.avatar_error = None;
                                                state.submit_error = None;
                                                cx.notify();
                                            });
                                        },
                                    ),
                                )
                        }
                    })
                    .child(profile_avatar(
                        if display_name.is_empty() {
                            t!("invitation.join.display_name_placeholder").to_string()
                        } else {
                            display_name
                        },
                        avatar_path,
                    ))
                    .into_any_element();
                v_flex()
                    .w_full()
                    .gap_6()
                    .child(profile_identity_group(
                        avatar, first_name, last_name, name_error, cx,
                    ))
                    .when_some(avatar_error, |this, error| {
                        this.child(
                            div()
                                .text_xs()
                                .ml_4()
                                .text_color(cx.theme().danger)
                                .child(error),
                        )
                    })
                    .child(profile_username_field(
                        "invitation-username-row",
                        nickname.read(cx).value().trim().to_owned(),
                        nickname_error,
                        {
                            let state = state.clone();
                            let desktop = desktop.clone();
                            move |_, window, cx| {
                                let _ = desktop.update(cx, |view, cx| {
                                    view.open_invitation_username_editor(state.clone(), window, cx)
                                });
                            }
                        },
                        cx,
                    ))
                    .child(
                        div()
                            .text_xs()
                            .px_4()
                            .opacity(0.6)
                            .line_height(relative(1.35))
                            .child(t!("invitation.join.one_time_warning").to_string()),
                    )
                    .into_any_element()
            }
            DesktopInvitationJoinScreen::Username => {
                profile_username_editor(nickname, nickname_error, cx)
            }
        };

        profile_editor_page(
            match screen {
                DesktopInvitationJoinScreen::Profile => "invitation-profile-scroll",
                DesktopInvitationJoinScreen::Username => "invitation-username-scroll",
            },
            header,
            content,
            submit_error,
            cx,
        )
    }

    fn cancel_desktop_invitation(
        &mut self,
        state: Entity<DesktopInvitationJoinState>,
        cx: &mut Context<Self>,
    ) {
        state.update(cx, |state, _| state.cancel());
        self.invitation_join = None;
        self.invitation_join_input_subscriptions.clear();
        cx.notify();
    }

    fn open_invitation_username_editor(
        &mut self,
        state: Entity<DesktopInvitationJoinState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let nickname = state.update(cx, |state, cx| {
            state.username_before_edit = Some(state.nickname.read(cx).value().trim().to_owned());
            state.screen = DesktopInvitationJoinScreen::Username;
            state.nickname_error = None;
            state.submit_error = None;
            state.nickname.clone()
        });
        nickname.update(cx, |input, cx| input.focus(window, cx));
        cx.notify();
    }

    fn cancel_invitation_username_editor(
        &mut self,
        state: Entity<DesktopInvitationJoinState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let (nickname, previous) = state.update(cx, |state, _| {
            state.screen = DesktopInvitationJoinScreen::Profile;
            state.nickname_error = None;
            state.submit_error = None;
            (state.nickname.clone(), state.username_before_edit.take())
        });
        if let Some(previous) = previous {
            nickname.update(cx, |input, cx| input.set_value(previous, window, cx));
        }
        cx.notify();
    }

    fn complete_invitation_username_editor(
        &mut self,
        state: Entity<DesktopInvitationJoinState>,
        cx: &mut Context<Self>,
    ) {
        state.update(cx, |state, cx| {
            if !state.can_complete(cx) {
                state.nickname_error = Some(t!("invitation.join.error.nickname").to_string());
                cx.notify();
                return;
            }
            state.screen = DesktopInvitationJoinScreen::Profile;
            state.username_before_edit = None;
            state.nickname_error = None;
            state.submit_error = None;
            cx.notify();
        });
        cx.notify();
    }

    fn pick_invitation_avatar(&mut self, cx: &mut Context<Self>) {
        let Some(state) = self.invitation_join.clone() else {
            return;
        };
        let selection = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: None,
        });
        cx.spawn(move |_this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let Ok(Ok(Some(paths))) = selection.await else {
                    return;
                };
                let Some(path) = paths.into_iter().next() else {
                    return;
                };
                let valid_size = std::fs::metadata(path.as_path())
                    .ok()
                    .is_some_and(|metadata| {
                        metadata.is_file()
                            && metadata.len() <= PROFILE_AVATAR_MAX_DECODED_BYTES as u64
                    });
                let valid = valid_size && load_desktop_profile_avatar(path.as_path()).is_ok();
                let _ = state.update(&mut cx, |state, cx| {
                    if valid {
                        state.avatar_path = Some(path);
                        state.avatar_error = None;
                        state.submit_error = None;
                    } else {
                        state.avatar_error = Some(t!("invitation.join.error.avatar").to_string());
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn accept_desktop_invitation(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(state) = self.invitation_join.clone() else {
            return;
        };
        let pending_recovery = state.read(cx).pending_registry_recovery.clone();
        let prepared = if pending_recovery.is_some() {
            state.update(cx, |state, cx| {
                if state.submitting {
                    return;
                }
                state.submitting = true;
                state.submit_error = None;
                cx.notify();
            });
            None
        } else {
            state.update(cx, |state, cx| {
                if state.submitting || !state.validate(cx) || state.flow.submit().is_err() {
                    cx.notify();
                    return None;
                }
                let presentation = match state.presentation() {
                    Ok(presentation) => presentation,
                    Err(_) => return None,
                };
                let profile = match state.profile(cx) {
                    Ok(profile) => profile,
                    Err(_) => {
                        state.flow.validation_failed(InvitationJoinField::Avatar);
                        state.avatar_error = Some(t!("invitation.join.error.avatar").to_string());
                        return None;
                    }
                };
                let gateway_name = state
                    .preview
                    .as_ref()
                    .and_then(|preview| preview.gateway_display_name.clone())
                    .unwrap_or_else(|| t!("gateway.endpoint.remote_name", index = 1).to_string());
                state.submitting = true;
                state.submit_error = None;
                cx.notify();
                Some((presentation, profile, gateway_name))
            })
        };
        if pending_recovery.is_none() && prepared.is_none() {
            return;
        }

        cx.spawn_in(
            window,
            move |this: WeakEntity<Self>, cx: &mut AsyncWindowContext| {
                let mut cx = cx.clone();
                async move {
                    let result = if let Some(recovery) = pending_recovery {
                        cx.background_spawn(async move {
                            let mut runtime = GatewayRuntime::load()?;
                            let endpoint = runtime.recover_invitation_registry(&recovery)?;
                            Ok::<DesktopJoinTaskResult, anyhow::Error>(
                                DesktopJoinTaskResult::Committed {
                                    endpoint_id: endpoint.id,
                                    endpoint_name: endpoint.name,
                                },
                            )
                        })
                        .await
                        .unwrap_or(DesktopJoinTaskResult::Retryable)
                    } else {
                        let (presentation, profile, gateway_name) =
                            prepared.expect("prepared join");
                        let runtime = match cx
                            .background_spawn(async move { GatewayRuntime::load() })
                            .await
                        {
                            Ok(runtime) => runtime,
                            Err(_) => {
                                finish_desktop_join_task(
                                    &this,
                                    &state,
                                    DesktopJoinTaskResult::Retryable,
                                    &mut cx,
                                );
                                return;
                            }
                        };
                        let installation_id = match runtime.invitation_installation_id() {
                            Ok(installation_id) => installation_id,
                            Err(_) => {
                                finish_desktop_join_task(
                                    &this,
                                    &state,
                                    DesktopJoinTaskResult::Retryable,
                                    &mut cx,
                                );
                                return;
                            }
                        };
                        let params = InvitationAcceptParams {
                            profile,
                            installation: ClientInstallationDescriptor {
                                installation_id: installation_id.clone(),
                                display_name: "Pioneer Desktop".to_owned(),
                                client_kind: ClientKind::Desktop,
                                platform: Some(std::env::consts::OS.to_owned()),
                                client_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
                            },
                        };
                        let accepted = match AuthExchangeClient::new(Duration::from_secs(15))
                            .accept_invitation(&presentation, params)
                            .await
                        {
                            Ok(accepted) => accepted,
                            Err(error) => {
                                let mapped = match error.kind {
                                    InvitationExchangeErrorKind::NicknameUnavailable => {
                                        DesktopJoinTaskResult::FieldError(
                                            InvitationJoinField::Nickname,
                                        )
                                    }
                                    InvitationExchangeErrorKind::InvalidProfile => {
                                        DesktopJoinTaskResult::FieldError(
                                            InvitationJoinField::DisplayName,
                                        )
                                    }
                                    InvitationExchangeErrorKind::AvatarInvalid => {
                                        DesktopJoinTaskResult::FieldError(
                                            InvitationJoinField::Avatar,
                                        )
                                    }
                                    InvitationExchangeErrorKind::Timeout
                                    | InvitationExchangeErrorKind::Transport => {
                                        DesktopJoinTaskResult::Retryable
                                    }
                                    InvitationExchangeErrorKind::Unavailable
                                    | InvitationExchangeErrorKind::InvalidInstallation
                                    | InvitationExchangeErrorKind::InvalidEndpoint
                                    | InvitationExchangeErrorKind::Protocol => {
                                        DesktopJoinTaskResult::Terminal
                                    }
                                };
                                finish_desktop_join_task(&this, &state, mapped, &mut cx);
                                return;
                            }
                        };
                        let _ = state.update(&mut cx, |state, _| state.flow.accept_succeeded());
                        let presentation_for_commit = presentation.clone();
                        let commit_result = cx
                            .background_spawn(async move {
                                let mut runtime = runtime;
                                runtime.commit_accepted_invitation(
                                    &presentation_for_commit,
                                    accepted,
                                    gateway_name.as_str(),
                                )
                            })
                            .await;
                        match commit_result {
                            Ok(endpoint) => DesktopJoinTaskResult::Committed {
                                endpoint_id: endpoint.id,
                                endpoint_name: endpoint.name,
                            },
                            Err(DesktopInvitationCommitError::SecureStorage(cleanup)) => {
                                AuthExchangeClient::new(Duration::from_secs(15))
                                    .cleanup_invitation_session_best_effort(cleanup)
                                    .await;
                                DesktopJoinTaskResult::Terminal
                            }
                            Err(DesktopInvitationCommitError::Registry(recovery)) => {
                                DesktopJoinTaskResult::RegistryRecovery(recovery)
                            }
                            Err(DesktopInvitationCommitError::Invalid { .. }) => {
                                DesktopJoinTaskResult::Terminal
                            }
                        }
                    };
                    finish_desktop_join_task(&this, &state, result, &mut cx);
                }
            },
        )
        .detach();
    }
}

fn finish_desktop_join_task(
    desktop: &WeakEntity<PioneerDesktop>,
    state: &Entity<DesktopInvitationJoinState>,
    result: DesktopJoinTaskResult,
    cx: &mut AsyncWindowContext,
) {
    let mut activation = None;
    let _ = state.update(cx, |state, cx| {
        state.submitting = false;
        match result {
            DesktopJoinTaskResult::Committed {
                endpoint_id,
                endpoint_name,
            } => {
                let _ = state.flow.complete();
                state.pending_registry_recovery = None;
                state.avatar_path = None;
                activation = Some((endpoint_id, endpoint_name));
            }
            DesktopJoinTaskResult::RegistryRecovery(recovery) => {
                state.pending_registry_recovery = Some(recovery);
                state.submit_error = Some(t!("invitation.join.error.storage").to_string());
            }
            DesktopJoinTaskResult::FieldError(field) => {
                state.flow.validation_failed(field);
                match field {
                    InvitationJoinField::DisplayName => {
                        state.name_error =
                            Some(t!("invitation.join.error.display_name").to_string());
                    }
                    InvitationJoinField::Nickname => {
                        state.nickname_error =
                            Some(t!("invitation.join.error.nickname_unavailable").to_string());
                    }
                    InvitationJoinField::Avatar => {
                        state.avatar_error = Some(t!("invitation.join.error.avatar").to_string());
                    }
                }
            }
            DesktopJoinTaskResult::Retryable => {
                let _ = state.flow.retryable_failure();
                state.submit_error = Some(t!("invitation.join.error.transport").to_string());
            }
            DesktopJoinTaskResult::Terminal => {
                state.flow.terminal_failure(false);
                state.submit_error = Some(t!("invitation.join.error.unavailable").to_string());
            }
        }
        cx.notify();
    });
    if let Some((endpoint_id, endpoint_name)) = activation {
        let _ = desktop.update_in(cx, |view, window, cx| {
            view.invitation_join = None;
            view.invitation_join_input_subscriptions.clear();
            view.activate_gateway(endpoint_id, endpoint_name, window, cx);
            cx.notify();
        });
    }
}

pub(crate) fn load_desktop_profile_avatar(
    path: &std::path::Path,
) -> anyhow::Result<ProfileAvatarInput> {
    let bytes = std::fs::read(path)?;
    if bytes.is_empty() || bytes.len() > PROFILE_AVATAR_MAX_DECODED_BYTES {
        anyhow::bail!("invalid profile avatar size");
    }
    let format = image::guess_format(bytes.as_slice())?;
    let media_type = match format {
        image::ImageFormat::Png => ProfileAvatarMediaType::Png,
        image::ImageFormat::Jpeg => ProfileAvatarMediaType::Jpeg,
        image::ImageFormat::WebP => ProfileAvatarMediaType::Webp,
        _ => anyhow::bail!("unsupported profile avatar format"),
    };
    let decoded = image::load_from_memory_with_format(bytes.as_slice(), format)?;
    if decoded.width() > PROFILE_AVATAR_MAX_DIMENSION
        || decoded.height() > PROFILE_AVATAR_MAX_DIMENSION
    {
        anyhow::bail!("invalid profile avatar dimensions");
    }
    ProfileAvatarInput::new(media_type, BASE64_STANDARD.encode(bytes)).map_err(anyhow::Error::new)
}

#[cfg(test)]
mod tests {
    #[test]
    fn invitation_join_is_a_nested_profile_screen_not_a_dialog() {
        let source = include_str!("invitation_join.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("source before tests");

        assert!(source.contains("DesktopInvitationJoinScreen::Profile"));
        assert!(source.contains("DesktopInvitationJoinScreen::Username"));
        assert!(source.contains("profile_identity_group"));
        assert!(source.contains("profile_username_field"));
        assert!(source.contains("profile_username_editor"));
        assert!(source.contains("profile_editor_header"));
        assert!(!source.contains("open_dialog"));
        assert!(!source.contains("close_dialog"));
    }
}
