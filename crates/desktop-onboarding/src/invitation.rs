use crate::{OnboardingConfig, assets::PioneerIconName, binding::Binding, profile_presentation::*};
use gpui_kit::component::{
    input::{InputEvent, InputState},
    menu::{ContextMenuExt, PopupMenuItem},
    spinner::Spinner,
    *,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    core::ClientScope,
    gateway::{
        invitation::InvitationJoinPhase, invitation_controller::*,
        onboarding_runtime::OnboardingIntent,
    },
};
use std::{path::PathBuf, sync::Arc};
#[derive(Clone, Copy)]
enum Screen {
    Profile,
    Username,
}
pub(crate) struct InvitationJoinScreenView {
    config: OnboardingConfig,
    value: InvitationPublication,
    first_name: Entity<InputState>,
    last_name: Entity<InputState>,
    nickname: Entity<InputState>,
    photo: Option<Task<()>>,
    _inputs: Vec<Subscription>,
    _binding: Arc<Binding>,
    _delivery: Task<()>,
}
impl InvitationJoinScreenView {
    pub fn new(config: OnboardingConfig, window: &mut Window, cx: &mut App) -> Entity<Self> {
        cx.new(|cx: &mut Context<Self>| {
            let value = config
                .client
                .snapshot(&ClientScope::OnboardingInvitation)
                .and_then(|p| p.typed::<InvitationPublication>())
                .map(|p| p.payload().as_ref().clone())
                .unwrap_or_default();
            let first_name = cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder(t!("settings.profile.first_name").to_string())
                    .default_value(value.first_name.clone())
            });
            let last_name = cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder(t!("settings.profile.last_name").to_string())
                    .default_value(value.last_name.clone())
            });
            let nickname = cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder(t!("settings.profile.username").to_string())
                    .default_value(value.nickname.clone())
            });
            let inputs = [first_name.clone(), last_name.clone(), nickname.clone()]
                .into_iter()
                .enumerate()
                .map(|(role, input)| {
                    cx.subscribe(&input, move |view, _, event, cx| match event {
                        InputEvent::Change => {
                            let (field, input, current) = match role {
                                0 => (
                                    pioneer_client::settings::profile::ProfileField::FirstName,
                                    &view.first_name,
                                    &view.value.first_name,
                                ),
                                1 => (
                                    pioneer_client::settings::profile::ProfileField::LastName,
                                    &view.last_name,
                                    &view.value.last_name,
                                ),
                                _ => (
                                    pioneer_client::settings::profile::ProfileField::Nickname,
                                    &view.nickname,
                                    &view.value.nickname,
                                ),
                            };
                            let value = input.read(cx).value().to_string();
                            if &value != current {
                                view.intent(InvitationIntent::EditField {
                                    expected_owner: view.value.owner_generation,
                                    field,
                                    value,
                                });
                            }
                        }
                        InputEvent::PressEnter { .. } => {
                            view.intent(if view.value.username_editing {
                                InvitationIntent::AcceptUsername
                            } else {
                                InvitationIntent::Submit
                            })
                        }
                        _ => {}
                    })
                })
                .collect();
            let binding = Binding::new(vec![ClientScope::OnboardingInvitation], &config.bindings);
            let mut changed = binding.changed.subscribe();
            let handle = window.window_handle();
            let delivery = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if handle
                        .update(cx, |_, window, cx| {
                            view.update(cx, |view, cx| view.sync(window, cx))
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            Self {
                config,
                value,
                first_name,
                last_name,
                nickname,
                photo: None,
                _inputs: inputs,
                _binding: binding,
                _delivery: delivery,
            }
        })
    }
    fn intent(&self, intent: InvitationIntent) {
        let expected_owner = self.value.owner_generation;
        let intent = match intent {
            InvitationIntent::RemoveAvatar => {
                InvitationIntent::RemoveAvatarForOwner { expected_owner }
            }
            InvitationIntent::PreviewRetry => {
                InvitationIntent::PreviewRetryForOwner { expected_owner }
            }
            InvitationIntent::Submit => InvitationIntent::SubmitForOwner { expected_owner },
            InvitationIntent::Cancel => InvitationIntent::Close { expected_owner },
            InvitationIntent::OpenUsername => {
                InvitationIntent::OpenUsernameForOwner { expected_owner }
            }
            InvitationIntent::AcceptUsername => {
                InvitationIntent::AcceptUsernameForOwner { expected_owner }
            }
            InvitationIntent::CancelUsername => {
                InvitationIntent::CancelUsernameForOwner { expected_owner }
            }
            intent => intent,
        };
        self.config
            .client
            .onboarding_intent(OnboardingIntent::Invitation { intent });
    }
    fn sync(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(value) = self
            .config
            .client
            .snapshot(&ClientScope::OnboardingInvitation)
            .and_then(|p| p.typed::<InvitationPublication>())
        else {
            return;
        };
        let value = value.payload();
        if self.value == *value {
            return;
        }
        let replaced = self.value.owner_generation != value.owner_generation;
        let focus_username = !self.value.username_editing && value.username_editing;
        self.value = value.as_ref().clone();
        if replaced {
            self.photo = None;
        }
        for (input, value) in [
            (&self.first_name, &self.value.first_name),
            (&self.last_name, &self.value.last_name),
            (&self.nickname, &self.value.nickname),
        ] {
            if input.read(cx).value().as_str() != value {
                input.update(cx, |input, cx| input.set_value(value.clone(), window, cx));
            }
        }
        if focus_username {
            self.nickname
                .update(cx, |input, cx| input.focus(window, cx));
        } else if replaced && self.value.active {
            self.first_name
                .update(cx, |input, cx| input.focus(window, cx));
        }
        cx.notify();
    }
    fn pick_avatar(&mut self, cx: &mut Context<Self>) {
        if self.value.submitting {
            return;
        }
        let expected_owner = self.value.owner_generation;
        let selection = self.config.photos.select(cx);
        self.photo = Some(cx.spawn(async move |view: WeakEntity<Self>, cx| {
            let selected = selection.await;
            let _ = view.update(cx, |view, _| match selected {
                Ok(Some(selected)) => view.intent(InvitationIntent::SelectAvatar {
                    expected_owner,
                    preview: selected.preview,
                    avatar: selected.avatar,
                }),
                Ok(None) => {}
                Err(_) => view.intent(InvitationIntent::AvatarFailed { expected_owner }),
            });
        }));
    }
}
impl Render for InvitationJoinScreenView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let desktop = cx.entity();
        let state = desktop.clone();
        let screen = if self.value.username_editing {
            Screen::Username
        } else {
            Screen::Profile
        };
        let loading = self.value.preview_pending;
        let terminal = self.value.phase == InvitationJoinPhase::Terminal;
        let submitting = self.value.submitting;
        let menu_owner = self.value.owner_generation;
        let can_complete = !submitting
            && !loading
            && self.value.nickname_valid
            && (self.value.username_editing
                || self.value.name_valid && self.value.preview.is_some());
        let first_name = self.first_name.clone();
        let last_name = self.last_name.clone();
        let nickname = self.nickname.clone();
        let display_name = [self.value.first_name.trim(), self.value.last_name.trim()]
            .into_iter()
            .filter(|v| !v.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        let avatar_path = self.value.avatar_preview.as_ref().map(PathBuf::from);
        let name_error = self
            .value
            .name_error
            .as_ref()
            .map(|_| t!("invitation.join.error.display_name").to_string());
        let nickname_error = self
            .value
            .nickname_error
            .as_ref()
            .map(|_| t!("invitation.join.error.nickname").to_string());
        let avatar_error = self
            .value
            .avatar_error
            .as_ref()
            .map(|_| t!("invitation.join.error.avatar").to_string());
        let submit_error = self.value.error.as_ref().map(|code| {
            match code.as_str() {
                "invitation_secure_storage_failed" | "invitation_registry_write_failed" => {
                    t!("invitation.join.error.storage")
                }
                "invitation_request_failed" | "invitation_connection_failed" => {
                    t!("invitation.join.error.transport")
                }
                "nickname_unavailable" => t!("invitation.join.error.nickname_unavailable"),
                _ => t!("invitation.join.error.unavailable"),
            }
            .to_string()
        });
        let title = match screen {
            Screen::Profile => t!("invitation.join.title"),
            Screen::Username => t!("settings.profile.edit_username_title"),
        }
        .to_string();
        let header = profile_editor_header(
            title,
            match screen {
                Screen::Profile => "invitation-join-back",
                Screen::Username => "invitation-username-back",
            },
            match screen {
                Screen::Profile => "invitation-join-submit",
                Screen::Username => "invitation-username-done",
            },
            t!("settings.profile.done").to_string(),
            submitting,
            !self.value.can_cancel,
            can_complete,
            {
                let desktop = desktop.clone();
                move |_, _window, cx| {
                    let _ = desktop.update(cx, |view, _cx| match screen {
                        Screen::Profile => view.intent(InvitationIntent::Cancel),
                        Screen::Username => view.intent(InvitationIntent::CancelUsername),
                    });
                }
            },
            {
                let desktop = desktop.clone();
                move |_, _window, cx| {
                    let _ = desktop.update(cx, |view, _cx| match screen {
                        Screen::Profile => view.intent(InvitationIntent::Submit),
                        Screen::Username => view.intent(InvitationIntent::AcceptUsername),
                    });
                }
            },
            cx,
        );

        if loading || terminal || self.value.preview.is_none() {
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
                .when(
                    !loading && !terminal && self.value.error.is_some(),
                    |this| {
                        this.child(
                            crate::buttons::default_outline_button("invitation-preview-retry")
                                .label(t!("invitation.join.actions.retry").to_string())
                                .on_click(cx.listener(|view, _, _, _| {
                                    view.intent(InvitationIntent::PreviewRetry)
                                })),
                        )
                    },
                )
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
            Screen::Profile => {
                let has_avatar = avatar_path.is_some();
                let avatar = div()
                    .id("invitation-avatar-edit")
                    .relative()
                    .flex_none()
                    .cursor_pointer()
                    .on_click({
                        let desktop = desktop.clone();
                        move |_, _, cx| {
                            let _ = desktop.update(cx, |view, cx| view.pick_avatar(cx));
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
                                                if view.value.owner_generation == menu_owner {
                                                    view.pick_avatar(cx);
                                                }
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
                                            let _ = remove_state.update(cx, |state, _cx| {
                                                state.intent(
                                                    InvitationIntent::RemoveAvatarForOwner {
                                                        expected_owner: menu_owner,
                                                    },
                                                );
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
                            let desktop = desktop.clone();
                            move |_, _window, cx| {
                                let _ = desktop.update(cx, |view, _cx| {
                                    view.intent(InvitationIntent::OpenUsername)
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
            Screen::Username => profile_username_editor(nickname, nickname_error, cx),
        };

        profile_editor_page(
            match screen {
                Screen::Profile => "invitation-profile-scroll",
                Screen::Username => "invitation-username-scroll",
            },
            header,
            content,
            submit_error,
            cx,
        )
    }
}
