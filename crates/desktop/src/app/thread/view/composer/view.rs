use crate::{
    app::root::{
        ComposerAttachmentUploadState, ComposerCapability, ComposerCapabilityKind, PioneerDesktop,
    },
    app::thread::view::composer::voice::DesktopVoiceEntryAvailability,
    assets::PioneerIconName,
    components::member_picker::{MemberPicker, member_picker_items},
};
use gpui::{prelude::*, *};
use gpui_component::{
    IconName,
    button::*,
    input::Input,
    menu::{DropdownMenu, PopupMenuItem},
    spinner::Spinner,
    theme::ActiveTheme,
    *,
};
use pioneer_client::composer::skill_selection::{
    ComposerSkillChip, ComposerSkillChipKind, ComposerSkillSelection, project_composer_skill_chips,
};
use pioneer_client::composer::state_machine::{
    ComposerDomainAction, ComposerMentionCandidate, composer_workspace_mention_candidates,
};
use pioneer_client::state::snapshot::ActiveThreadSnapshot;
use pioneer_protocol::{ThreadMode, WorkspaceId};
const COMPOSER_ATTACHMENT_TEXT_FADE_WIDTH: Pixels = px(24.);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DesktopComposerPrimaryAction {
    Send,
    Stop,
    VoiceReady,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DesktopComposerPrimaryActionState {
    action: DesktopComposerPrimaryAction,
    disabled: bool,
    loading: bool,
}

#[derive(Clone, Copy, Debug)]
struct DesktopComposerPrimaryActionInput {
    voice_hold_ui_active: bool,
    has_in_flight_turn: bool,
    message_mode: bool,
    composer_text_empty: bool,
    voice_entry_availability: DesktopVoiceEntryAvailability,
    can_send: bool,
    can_stop: bool,
    is_cancelling: bool,
    upload_in_progress: bool,
    voice_send_processing: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DesktopComposerActiveTurnState {
    has_in_flight_turn: bool,
    is_cancelling: bool,
    can_stop: bool,
}

fn desktop_composer_active_turn_state(
    active_thread: &ActiveThreadSnapshot,
    gateway_connected: bool,
) -> DesktopComposerActiveTurnState {
    DesktopComposerActiveTurnState {
        has_in_flight_turn: active_thread.has_in_flight_turn(),
        is_cancelling: active_thread.is_cancelling_turn(),
        can_stop: active_thread.can_request_turn_cancel(gateway_connected),
    }
}

fn resolve_desktop_composer_primary_action(
    input: DesktopComposerPrimaryActionInput,
) -> DesktopComposerPrimaryActionState {
    let action = if input.voice_hold_ui_active {
        DesktopComposerPrimaryAction::VoiceReady
    } else if input.has_in_flight_turn && !input.message_mode {
        DesktopComposerPrimaryAction::Stop
    } else if input.composer_text_empty
        && input.voice_entry_availability == DesktopVoiceEntryAvailability::Ready
    {
        DesktopComposerPrimaryAction::VoiceReady
    } else {
        DesktopComposerPrimaryAction::Send
    };
    let disabled = match action {
        DesktopComposerPrimaryAction::VoiceReady => false,
        DesktopComposerPrimaryAction::Stop => !input.can_stop,
        DesktopComposerPrimaryAction::Send => !input.voice_send_processing && !input.can_send,
    };
    let loading = input.upload_in_progress
        || (action == DesktopComposerPrimaryAction::Stop
            && input.has_in_flight_turn
            && input.is_cancelling)
        || input.voice_send_processing;

    DesktopComposerPrimaryActionState {
        action,
        disabled,
        loading,
    }
}

impl PioneerDesktop {
    pub(crate) fn render_composer(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let composer_state = self.composer_state.clone();
        let attachments = self.composer_attachments.clone();
        let capabilities = self.effective_composer_capabilities();
        let skill_chips = self.composer_skill_chips();
        let composer_error = self
            .composer_edit_target
            .as_ref()
            .and_then(|target| target.error.clone())
            .or_else(|| self.composer_upload_error.clone());
        let microphone_error = self.desktop_microphone_error_message();
        let editing_message = self.composer_edit_target.is_some();
        let message_mode = self.composer_turn_mode == ThreadMode::Message;
        let active_thread_snapshot = self.client_snapshot().active_thread;
        let gateway_connected =
            self.gateway.connection_state == crate::app::root::GatewayConnectionState::Connected;
        // A TaskRun thread is a normal foreground conversation. Its active turn must remain
        // visible here so the Composer exposes Stop instead of a disabled Send action.
        let active_turn_state =
            desktop_composer_active_turn_state(&active_thread_snapshot, gateway_connected);
        let is_cancelling = active_turn_state.is_cancelling;
        let can_stop =
            active_turn_state.can_stop && self.can_cancel_active_thread_agent_presentation();
        let has_in_flight_turn = active_turn_state.has_in_flight_turn;
        let composer_text = composer_state.read(cx).value().trim().to_owned();
        let can_send = if let Some(target) = self.composer_edit_target.as_ref() {
            gateway_connected
                && !self.message_mutation_pending
                && !target.conflicted
                && (!composer_text.is_empty() || !target.artifacts.is_empty())
        } else {
            self.can_submit_message(cx)
        };
        let cli_runtime_thread_binding = if has_in_flight_turn {
            active_thread_snapshot
                .thread_id
                .as_deref()
                .and_then(|thread_id| self.cli_runtime_binding_for_thread(thread_id))
        } else {
            None
        };
        let has_cli_runtime_steer_target =
            cli_runtime_thread_binding.as_ref().is_some_and(|binding| {
                self.providers
                    .cli_runtimes()
                    .iter()
                    .find(|runtime| runtime.runtime_id == binding.runtime_id)
                    .map(|runtime| runtime.capabilities.supports_steer)
                    .unwrap_or(false)
            });
        let can_steer_cli_runtime_turn = has_cli_runtime_steer_target
            && gateway_connected
            && active_thread_snapshot
                .thread_id
                .as_deref()
                .is_some_and(|thread_id| {
                    self.thread_presentation_capabilities(thread_id)
                        .is_some_and(|capabilities| capabilities.can_steer_agent_execution)
                })
            && !is_cancelling
            && !self.composer_upload_in_progress
            && attachments.is_empty()
            && capabilities.is_empty()
            && self.composer_skill_selections.is_empty()
            && !composer_text.is_empty();
        let desktop_voice_context_locked = self.desktop_voice_context_locked();
        let desktop_voice_hold_ui_active = self.desktop_voice_hold_ui_active();
        let desktop_voice_send_processing = self.desktop_voice_send_processing();
        let voice_entry_availability = if editing_message || (has_in_flight_turn && !message_mode) {
            DesktopVoiceEntryAvailability::Hidden
        } else {
            self.desktop_voice_entry_availability()
        };
        let composer_busy = self.composer_upload_in_progress || self.message_mutation_pending;
        let composer_primary_action =
            resolve_desktop_composer_primary_action(DesktopComposerPrimaryActionInput {
                voice_hold_ui_active: desktop_voice_hold_ui_active,
                has_in_flight_turn,
                message_mode,
                composer_text_empty: composer_text.is_empty(),
                voice_entry_availability,
                can_send,
                can_stop,
                is_cancelling,
                upload_in_progress: composer_busy,
                voice_send_processing: desktop_voice_send_processing,
            });
        let composer_action_loading = composer_primary_action.loading;
        let composer_action_disabled = composer_primary_action.disabled;
        let composer_primary_action = composer_primary_action.action;

        let composer_action_is_stop = composer_primary_action == DesktopComposerPrimaryAction::Stop;
        let composer_action_id = if composer_action_is_stop {
            "stop-turn"
        } else {
            "send-message"
        };
        let mention_picker = if message_mode {
            self.render_composer_mention_picker(window, cx)
        } else {
            None
        };

        h_flex()
            .w_full()
            .flex_none()
            .justify_center()
            .pb_4()
            .child(
                v_flex()
                    .w_full()
                    .flex_none()
                    .max_w(px(800.))
                    .px_6()
                    .items_end()
                    .child(self.render_composer_mode_selector(cx))
                    .child(
                        v_flex()
                            .w_full()
                            .flex_none()
                            .border_1()
                            .border_color(cx.theme().border)
                            .rounded_2xl()
                            .shadow_xs()
                            .on_mouse_move(cx.listener(|view, event: &MouseMoveEvent, _, cx| {
                                view.update_desktop_voice_hold_pointer(event.position, cx);
                            }))
                            .on_mouse_up(
                                MouseButton::Left,
                                cx.listener(|view, event: &MouseUpEvent, _, cx| {
                                    view.release_desktop_voice_hold_at(event.position, cx);
                                }),
                            )
                            .on_mouse_up_out(
                                MouseButton::Left,
                                cx.listener(|view, event: &MouseUpEvent, _, cx| {
                                    view.release_desktop_voice_hold_at(event.position, cx);
                                }),
                            )
                            .child(
                                v_flex()
                                    .bg(cx.theme().background)
                                    .rounded_t_2xl()
                                    .when(
                                        !attachments.is_empty()
                                            || !capabilities.is_empty()
                                            || !skill_chips.is_empty(),
                                        |this| this.child(self.render_composer_chip_badges(cx)),
                                    )
                                    .when_some(composer_error, |this, error| {
                                        this.child(
                                            h_flex()
                                                .mx_2()
                                                .mb_2()
                                                .gap_2()
                                                .items_start()
                                                .rounded_md()
                                                .border_1()
                                                .border_color(cx.theme().danger.opacity(0.25))
                                                .bg(cx.theme().danger.opacity(0.08))
                                                .px_2()
                                                .py_1p5()
                                                .child(
                                                    Icon::new(IconName::TriangleAlert)
                                                        .size_3()
                                                        .text_color(cx.theme().danger),
                                                )
                                                .child(
                                                    div()
                                                        .text_xs()
                                                        .line_height(relative(1.25))
                                                        .text_color(cx.theme().danger)
                                                        .child(error),
                                                ),
                                        )
                                    })
                                    .when_some(microphone_error, |this, error| {
                                        this.child(
                                            h_flex()
                                                .mx_2()
                                                .mb_2()
                                                .gap_2()
                                                .items_start()
                                                .rounded_md()
                                                .border_1()
                                                .border_color(cx.theme().danger.opacity(0.25))
                                                .bg(cx.theme().danger.opacity(0.08))
                                                .px_2()
                                                .py_1p5()
                                                .child(
                                                    Icon::new(IconName::TriangleAlert)
                                                        .size_3()
                                                        .text_color(cx.theme().danger),
                                                )
                                                .child(
                                                    div()
                                                        .text_xs()
                                                        .line_height(relative(1.25))
                                                        .text_color(cx.theme().danger)
                                                        .child(error),
                                                ),
                                        )
                                    })
                                    .child(if desktop_voice_hold_ui_active {
                                        self.render_desktop_voice_hold_prompt(cx)
                                    } else {
                                        Input::new(&composer_state)
                                            .appearance(false)
                                            .disabled(
                                                desktop_voice_context_locked
                                                    || composer_busy,
                                            )
                                            .into_any_element()
                                    }),
                            )
                            .child(
                                h_flex()
                                    .px_2()
                                    .pb_2()
                                    .justify_between()
                                    .items_center()
                                    .bg(cx.theme().background)
                                    .rounded_b_2xl()
                                    .child(
                                        h_flex()
                                            .items_center()
                                            .gap_2()
                                            .child(self.render_composer_add_menu(cx))
                                            .when_some(mention_picker, |this, picker| {
                                                this.child(picker)
                                            })
                                            .when(!message_mode, |this| {
                                                this.child(
                                                    self.render_composer_permission_selector(cx),
                                                )
                                                .child(self.render_composer_model_selector(cx))
                                            }),
                                    )
                                    .child(
                                        h_flex()
                                            .items_center()
                                            .gap_2()
                                            .when(message_mode && has_in_flight_turn, |this| {
                                                this.child(
                                                    Button::new("stop-turn-while-messaging")
                                                        .small()
                                                        .ghost()
                                                        .rounded_full()
                                                        .icon(PioneerIconName::Square)
                                                        .tooltip(
                                                            t!("chat.composer.stop_reason")
                                                                .to_string(),
                                                        )
                                                        .disabled(!can_stop)
                                                        .loading(is_cancelling)
                                                        .on_click(cx.listener(
                                                            |view, _, window, cx| {
                                                                view.stop_active_turn(window, cx);
                                                            },
                                                        )),
                                                )
                                            })
                                            .when(has_cli_runtime_steer_target, |this| {
                                                this.child(
                                                    Button::new("steer-running-cli-runtime-turn")
                                                        .small()
                                                        .ghost()
                                                        .rounded_full()
                                                        .icon(IconName::ArrowUp)
                                                        .tooltip(
                                                            t!("chat.composer.steer_cli_runtime")
                                                                .to_string(),
                                                        )
                                                        .disabled(!can_steer_cli_runtime_turn)
                                                        .on_click(cx.listener(
                                                            move |view, _, window, cx| {
                                                                view.steer_active_cli_runtime_turn(
                                                                    window, cx,
                                                                );
                                                            },
                                                        )),
                                                )
                                            })
                                            .child(match composer_primary_action {
                                                DesktopComposerPrimaryAction::VoiceReady => {
                                                    self.render_desktop_voice_idle_button(cx)
                                                }
                                                DesktopComposerPrimaryAction::Send
                                                | DesktopComposerPrimaryAction::Stop => {
                                                    Button::new(composer_action_id)
                                                        .primary()
                                                        .rounded_full()
                                                        .disabled(composer_action_disabled)
                                                        .loading(composer_action_loading)
                                                        .when(composer_action_is_stop, |this| {
                                                            this.icon(PioneerIconName::Square)
                                                        })
                                                        .when(!composer_action_is_stop, |this| {
                                                            this.icon(IconName::ArrowUp)
                                                        })
                                                        .on_click(cx.listener(
                                                            move |view, _, window, cx| {
                                                                if composer_action_is_stop {
                                                                    view.stop_active_turn(
                                                                        window, cx,
                                                                    );
                                                                } else {
                                                                    if view
                                                                        .composer_edit_target
                                                                        .is_some()
                                                                    {
                                                                        view.submit_composer_message_edit(
                                                                            window, cx,
                                                                        );
                                                                    } else {
                                                                        view.submit_composer_message(
                                                                            window, cx,
                                                                        );
                                                                    }
                                                                }
                                                            },
                                                        ))
                                                        .into_any_element()
                                                }
                                            }),
                                    ),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn render_composer_mention_picker(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        let workspace_id = self
            .current_active_thread_id()
            .and_then(|thread_id| self.thread_workspace_id(thread_id))
            .and_then(|workspace_id| WorkspaceId::new(workspace_id.to_owned()).ok());
        let directory_loading = self.members_loading
            || !self.administration.member_directory_complete()
            || workspace_id
                .as_ref()
                .is_some_and(|workspace_id| self.workspace_members_loading.contains(workspace_id));
        let current_principal_id = self
            .gateway
            .current_auth
            .as_ref()
            .map(|auth| &auth.principal.id);
        let candidates = workspace_id
            .as_ref()
            .and_then(|workspace_id| self.administration.workspace_members(workspace_id))
            .map(|members| {
                composer_workspace_mention_candidates(
                    members.iter().cloned(),
                    self.administration.members().cloned(),
                    current_principal_id,
                )
            })
            .unwrap_or_default();

        // There is no useful action when a workspace has no active members to
        // mention. Keep the toolbar clean instead of opening an empty picker.
        if candidates.is_empty() && !directory_loading {
            return None;
        }

        // Updating the Combobox items on every render resets SearchableVec's
        // current query and makes typing appear to have no effect. Keep the
        // source candidates in the view and replace the items only when the
        // workspace member list actually changes.
        if self.composer_mention_items != candidates {
            self.composer_mention_items = candidates.clone();

            let select_items = member_picker_items(candidates, |principal_id| {
                self.member_avatar_state
                    .presentation(principal_id)
                    .and_then(|avatar| avatar.cached_image_path.clone())
            });
            self.composer_mention_select.update(cx, |state, cx| {
                state.set_items(select_items, window, cx);
            });
        }

        let trigger = if directory_loading || self.composer_upload_in_progress {
            div()
                .id("composer-mention-picker")
                .size_6()
                .ml_0p5()
                .flex()
                .items_center()
                .justify_center()
                .opacity(0.35)
                .child(Icon::new(PioneerIconName::AtSign).size_3p5())
                .into_any_element()
        } else {
            MemberPicker::new(
                "composer-mention-picker",
                "composer-mention-trigger-button",
                &self.composer_mention_select,
                Icon::new(PioneerIconName::AtSign).size_3p5().opacity(0.6),
            )
            .inset_trigger()
            .into_any_element()
        };

        Some(trigger)
    }

    pub(crate) fn insert_composer_mention(
        &mut self,
        candidate: ComposerMentionCandidate,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let token = format!("@{}", candidate.nickname.trim());
        let current = self.composer_state.read(cx).value();
        let next = if current.trim().is_empty() {
            format!("{token} ")
        } else if current.contains(token.as_str()) {
            current.to_string()
        } else {
            format!("{} {token} ", current.trim_end())
        };
        self.composer_state.update(cx, move |state, cx| {
            state.set_value(next, window, cx);
            state.focus(window, cx);
        });
        self.reduce_composer_domain(ComposerDomainAction::SelectMention { candidate });
        cx.notify();
    }

    fn render_composer_add_menu(&self, cx: &mut Context<Self>) -> AnyElement {
        let message_mode = self.composer_turn_mode == ThreadMode::Message;
        let scoped_action_allowed = if message_mode {
            self.can_write_active_thread_presentation()
        } else {
            self.can_start_active_thread_agent_presentation()
        };
        let desktop_entity = cx.entity().clone();
        let capabilities = self.principal_presentation_capabilities();
        let can_start_agent = self.can_start_active_thread_agent_presentation();
        let can_use_skills = capabilities.can_use_skills && can_start_agent;
        let can_use_mcp = capabilities.can_use_mcp && can_start_agent;
        let can_attach = self.active_artifact_presentation_policy().can_attach;
        let disabled = self.composer_upload_in_progress
            || self.message_mutation_pending
            || self.composer_edit_target.is_some()
            || self.desktop_voice_context_locked()
            || !scoped_action_allowed
            || !(can_attach || can_use_skills || can_use_mcp);
        Button::new("composer-add-attachment")
            .small()
            .ghost()
            .compact()
            .child(Icon::new(IconName::Plus).size_5().opacity(0.6))
            .disabled(disabled)
            .dropdown_menu_with_anchor(Anchor::BottomLeft, move |menu, _, _| {
                let menu = menu.min_w(px(196.));
                let menu = if can_attach {
                    menu.item(Self::composer_add_menu_item(
                        t!("chat.composer.add_menu.files").to_string().into(),
                        PioneerIconName::Paperclip,
                        {
                            let desktop_entity = desktop_entity.clone();
                            move |window, cx| {
                                let _ = desktop_entity.update(cx, |view, cx| {
                                    view.open_composer_file_picker(window, cx);
                                    cx.notify();
                                });
                            }
                        },
                    ))
                } else {
                    menu
                };

                if message_mode {
                    return menu;
                }

                let menu = if can_use_skills {
                    menu.item(Self::composer_add_menu_item(
                        t!("chat.composer.add_menu.skills").to_string().into(),
                        PioneerIconName::Zap,
                        {
                            let desktop_entity = desktop_entity.clone();
                            move |window, cx| {
                                let _ = desktop_entity.update(cx, |view, cx| {
                                    view.open_composer_skills_picker(window, cx);
                                    cx.notify();
                                });
                            }
                        },
                    ))
                } else {
                    menu
                };

                if can_use_mcp {
                    menu.item(Self::composer_add_menu_item(
                        t!("chat.composer.add_menu.mcp").to_string().into(),
                        PioneerIconName::Mcp,
                        {
                            let desktop_entity = desktop_entity.clone();
                            move |window, cx| {
                                let _ = desktop_entity.update(cx, |view, cx| {
                                    view.open_composer_mcp_picker(window, cx);
                                    cx.notify();
                                });
                            }
                        },
                    ))
                } else {
                    menu
                }
            })
            .into_any_element()
    }

    fn composer_add_menu_item(
        label: SharedString,
        icon: PioneerIconName,
        action: impl Fn(&mut Window, &mut App) + 'static,
    ) -> PopupMenuItem {
        PopupMenuItem::element({
            let label = label.clone();
            move |_, _| {
                h_flex()
                    .w_full()
                    .items_center()
                    .gap_2()
                    .child(Icon::new(icon.clone()).xsmall())
                    .child(label.clone())
            }
        })
        .on_click(move |_, window, cx| action(window, cx))
    }

    fn render_composer_chip_badges(&self, cx: &mut Context<Self>) -> AnyElement {
        let mut chips = Vec::new();
        for (index, attachment) in self.composer_attachments.iter().cloned().enumerate() {
            chips.push(self.render_composer_attachment_badge(attachment, index, cx));
        }

        let effective_capabilities = self.effective_composer_capabilities();
        for (index, chip) in self.composer_skill_chips().into_iter().enumerate() {
            chips.push(self.render_composer_skill_selection_badge(chip, index, cx));
        }
        for (index, capability) in effective_capabilities.into_iter().enumerate() {
            chips.push(self.render_composer_capability_badge(capability, index, cx));
        }

        h_flex()
            .id("composer-attachment-chips")
            .w_full()
            .min_w_0()
            .pt_2()
            .px_2()
            .flex_wrap()
            .gap_1p5()
            .children(chips)
            .into_any_element()
    }

    fn render_composer_attachment_badge(
        &self,
        attachment: crate::app::root::ComposerAttachment,
        index: usize,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let group_id = format!("composer-attachment-chip-{index}");

        let file_name =
            pioneer_client::composer::attachments::composer_attachment_display_name(&attachment);

        let (status_icon, status_color, is_uploading) = match &attachment.upload_state {
            ComposerAttachmentUploadState::Local => (IconName::File, cx.theme().foreground, false),
            ComposerAttachmentUploadState::Uploading => {
                (IconName::Loader, cx.theme().muted_foreground, true)
            }
            ComposerAttachmentUploadState::Uploaded { .. } => {
                (IconName::Check, cx.theme().muted_foreground, false)
            }
            ComposerAttachmentUploadState::Failed { .. } => {
                (IconName::TriangleAlert, cx.theme().danger, false)
            }
        };

        h_flex()
            .id(("composer-attachment-chip", index))
            .flex_initial()
            .max_w(px(196.))
            .min_w_0()
            .h(px(32.))
            .pl_2()
            .pr_1p5()
            .items_center()
            .gap_1()
            .rounded_full()
            .border_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .group(group_id.clone())
            .child(
                div()
                    .flex_none()
                    .size(px(20.))
                    .rounded_full()
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(if is_uploading {
                        Spinner::new()
                            .with_size(gpui_component::Size::Small)
                            .color(status_color)
                            .into_any_element()
                    } else {
                        Icon::new(status_icon)
                            .size_3()
                            .opacity(0.8)
                            .text_color(status_color)
                            .into_any_element()
                    }),
            )
            .child(
                div()
                    .relative()
                    .flex_initial()
                    .min_w_0()
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .pr(COMPOSER_ATTACHMENT_TEXT_FADE_WIDTH)
                    .text_xs()
                    .child(file_name)
                    .child(
                        div()
                            .absolute()
                            .top_0()
                            .right_0()
                            .bottom_0()
                            .w(COMPOSER_ATTACHMENT_TEXT_FADE_WIDTH)
                            .bg(linear_gradient(
                                90.,
                                linear_color_stop(cx.theme().background.opacity(0.), 0.),
                                linear_color_stop(cx.theme().background, 1.),
                            )),
                    ),
            )
            .child(
                Button::new(("composer-attachment-remove", index))
                    .flex_none()
                    .ghost()
                    .xsmall()
                    .compact()
                    .icon(IconName::Close)
                    .disabled(
                        self.composer_upload_in_progress || self.desktop_voice_context_locked(),
                    )
                    .rounded_full()
                    .on_click(cx.listener(move |view, _, _, cx| {
                        view.remove_composer_attachment_at(index);
                        cx.notify();
                    })),
            )
            .into_any_element()
    }

    fn render_composer_capability_badge(
        &self,
        capability: ComposerCapability,
        index: usize,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let group_id = format!("composer-capability-chip-{index}");
        let icon = match capability.kind {
            ComposerCapabilityKind::Skill { .. } => PioneerIconName::Zap,
            ComposerCapabilityKind::McpServer { .. } | ComposerCapabilityKind::McpTool { .. } => {
                PioneerIconName::Mcp
            }
        };

        h_flex()
            .id(("composer-capability-chip", index))
            .flex_initial()
            .max_w(px(196.))
            .min_w_0()
            .h(px(32.))
            .pl_2()
            .pr_1p5()
            .items_center()
            .gap_1()
            .rounded_full()
            .border_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .group(group_id)
            .child(
                div()
                    .flex_none()
                    .size(px(20.))
                    .rounded_full()
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(
                        Icon::new(icon)
                            .size_3()
                            .opacity(0.8)
                            .text_color(cx.theme().foreground),
                    ),
            )
            .child(
                div()
                    .relative()
                    .flex_initial()
                    .min_w_0()
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .pr(COMPOSER_ATTACHMENT_TEXT_FADE_WIDTH)
                    .text_xs()
                    .child(capability.label)
                    .child(
                        div()
                            .absolute()
                            .top_0()
                            .right_0()
                            .bottom_0()
                            .w(COMPOSER_ATTACHMENT_TEXT_FADE_WIDTH)
                            .bg(linear_gradient(
                                90.,
                                linear_color_stop(cx.theme().background.opacity(0.), 0.),
                                linear_color_stop(cx.theme().background, 1.),
                            )),
                    ),
            )
            .child(
                Button::new(("composer-capability-remove", index))
                    .flex_none()
                    .ghost()
                    .xsmall()
                    .compact()
                    .icon(IconName::Close)
                    .disabled(
                        self.composer_upload_in_progress || self.desktop_voice_context_locked(),
                    )
                    .rounded_full()
                    .on_click(cx.listener(move |view, _, _, cx| {
                        view.remove_composer_capability_at(index);
                        cx.notify();
                    })),
            )
            .into_any_element()
    }

    fn composer_skill_chips(&self) -> Vec<ComposerSkillChip> {
        let picker = self.composer_skill_picker_projection("");
        project_composer_skill_chips(self.composer_skill_selections.as_slice(), &picker)
    }

    fn render_composer_skill_selection_badge(
        &self,
        chip: ComposerSkillChip,
        index: usize,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(selection) = composer_skill_selection_from_chip(&chip) else {
            return div().into_any_element();
        };
        let group_id = format!("composer-skill-selection-chip-{index}");

        h_flex()
            .id(("composer-skill-selection-chip", index))
            .flex_initial()
            .max_w(px(196.))
            .min_w_0()
            .h(px(32.))
            .pl_2()
            .pr_1p5()
            .items_center()
            .gap_1()
            .rounded_full()
            .border_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .group(group_id)
            .child(
                div()
                    .flex_none()
                    .size(px(20.))
                    .rounded_full()
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(
                        Icon::new(PioneerIconName::Zap)
                            .size_3()
                            .opacity(0.8)
                            .text_color(cx.theme().foreground),
                    ),
            )
            .child(
                div()
                    .relative()
                    .flex_initial()
                    .min_w_0()
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .pr(COMPOSER_ATTACHMENT_TEXT_FADE_WIDTH)
                    .text_xs()
                    .child(chip.label)
                    .child(
                        div()
                            .absolute()
                            .top_0()
                            .right_0()
                            .bottom_0()
                            .w(COMPOSER_ATTACHMENT_TEXT_FADE_WIDTH)
                            .bg(linear_gradient(
                                90.,
                                linear_color_stop(cx.theme().background.opacity(0.), 0.),
                                linear_color_stop(cx.theme().background, 1.),
                            )),
                    ),
            )
            .child(
                Button::new(("composer-skill-selection-remove", index))
                    .flex_none()
                    .ghost()
                    .xsmall()
                    .compact()
                    .icon(IconName::Close)
                    .disabled(
                        self.composer_upload_in_progress || self.desktop_voice_context_locked(),
                    )
                    .rounded_full()
                    .on_click(cx.listener(move |view, _, _, cx| {
                        view.remove_composer_skill_selection(selection.clone());
                        cx.notify();
                    })),
            )
            .into_any_element()
    }
}

fn composer_skill_selection_from_chip(chip: &ComposerSkillChip) -> Option<ComposerSkillSelection> {
    match chip.kind {
        ComposerSkillChipKind::SkillPack => Some(ComposerSkillSelection::SkillPack {
            pack_id: chip.pack_id.clone()?,
        }),
        ComposerSkillChipKind::PackedSkill | ComposerSkillChipKind::StandaloneSkill => {
            Some(ComposerSkillSelection::Skill {
                skill_id: chip.skill_id.clone()?,
                pack_id: chip.pack_id.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{SkillId, SkillPackId};

    const READY_INPUT: DesktopComposerPrimaryActionInput = DesktopComposerPrimaryActionInput {
        voice_hold_ui_active: false,
        has_in_flight_turn: false,
        message_mode: false,
        composer_text_empty: true,
        voice_entry_availability: DesktopVoiceEntryAvailability::Ready,
        can_send: false,
        can_stop: true,
        is_cancelling: false,
        upload_in_progress: false,
        voice_send_processing: false,
    };

    const VOICE_AVAILABILITIES: [DesktopVoiceEntryAvailability; 2] = [
        DesktopVoiceEntryAvailability::Hidden,
        DesktopVoiceEntryAvailability::Ready,
    ];

    #[::core::prelude::v1::test]
    fn task_child_foreground_turn_exposes_stop_state() {
        assert_eq!(
            pioneer_protocol::ThreadOriginKind::TaskRun.composer_execution_mode(),
            pioneer_protocol::ThreadComposerExecutionMode::ForegroundTurn,
        );
        let active_thread = ActiveThreadSnapshot {
            thread_id: Some("task-child".to_owned()),
            in_flight_turn_id: Some("child-turn".to_owned()),
            phase: pioneer_client::state::snapshot::ActiveThreadPhaseSnapshot::Running,
            ..ActiveThreadSnapshot::default()
        };

        assert_eq!(
            desktop_composer_active_turn_state(&active_thread, true),
            DesktopComposerActiveTurnState {
                has_in_flight_turn: true,
                is_cancelling: false,
                can_stop: true,
            }
        );
        assert_eq!(
            resolve_desktop_composer_primary_action(DesktopComposerPrimaryActionInput {
                has_in_flight_turn: true,
                can_stop: true,
                voice_entry_availability: DesktopVoiceEntryAvailability::Hidden,
                ..READY_INPUT
            }),
            DesktopComposerPrimaryActionState {
                action: DesktopComposerPrimaryAction::Stop,
                disabled: false,
                loading: false,
            }
        );
    }

    #[::core::prelude::v1::test]
    fn desktop_composer_primary_action_priority_matrix_is_complete() {
        for availability in VOICE_AVAILABILITIES {
            assert_eq!(
                resolve_desktop_composer_primary_action(DesktopComposerPrimaryActionInput {
                    voice_hold_ui_active: true,
                    voice_entry_availability: availability,
                    ..READY_INPUT
                })
                .action,
                DesktopComposerPrimaryAction::VoiceReady,
            );
            assert_eq!(
                resolve_desktop_composer_primary_action(DesktopComposerPrimaryActionInput {
                    voice_hold_ui_active: true,
                    has_in_flight_turn: true,
                    voice_entry_availability: availability,
                    ..READY_INPUT
                })
                .action,
                DesktopComposerPrimaryAction::VoiceReady,
            );
            assert_eq!(
                resolve_desktop_composer_primary_action(DesktopComposerPrimaryActionInput {
                    has_in_flight_turn: true,
                    voice_entry_availability: availability,
                    ..READY_INPUT
                })
                .action,
                DesktopComposerPrimaryAction::Stop,
            );
        }
    }

    #[::core::prelude::v1::test]
    fn desktop_composer_empty_text_keeps_microphone_with_frozen_non_text_payload() {
        // can_send=true with empty text represents selected attachments or capabilities.
        assert_eq!(
            resolve_desktop_composer_primary_action(DesktopComposerPrimaryActionInput {
                can_send: true,
                ..READY_INPUT
            }),
            DesktopComposerPrimaryActionState {
                action: DesktopComposerPrimaryAction::VoiceReady,
                disabled: false,
                loading: false,
            },
        );
    }

    #[::core::prelude::v1::test]
    fn desktop_composer_typed_text_uses_enabled_send() {
        assert_eq!(
            resolve_desktop_composer_primary_action(DesktopComposerPrimaryActionInput {
                composer_text_empty: false,
                can_send: true,
                ..READY_INPUT
            }),
            DesktopComposerPrimaryActionState {
                action: DesktopComposerPrimaryAction::Send,
                disabled: false,
                loading: false,
            },
        );
    }

    #[::core::prelude::v1::test]
    fn message_mode_can_send_while_an_execution_is_running() {
        assert_eq!(
            resolve_desktop_composer_primary_action(DesktopComposerPrimaryActionInput {
                has_in_flight_turn: true,
                message_mode: true,
                composer_text_empty: false,
                voice_entry_availability: DesktopVoiceEntryAvailability::Hidden,
                can_send: true,
                ..READY_INPUT
            }),
            DesktopComposerPrimaryActionState {
                action: DesktopComposerPrimaryAction::Send,
                disabled: false,
                loading: false,
            },
        );
    }

    #[::core::prelude::v1::test]
    fn desktop_composer_voice_fallback_uses_normal_send_availability() {
        for (can_send, disabled) in [(false, true), (true, false)] {
            assert_eq!(
                resolve_desktop_composer_primary_action(DesktopComposerPrimaryActionInput {
                    voice_entry_availability: DesktopVoiceEntryAvailability::Hidden,
                    can_send,
                    ..READY_INPUT
                }),
                DesktopComposerPrimaryActionState {
                    action: DesktopComposerPrimaryAction::Send,
                    disabled,
                    loading: false,
                },
            );
        }
    }

    #[::core::prelude::v1::test]
    fn desktop_composer_stop_and_processing_states_keep_their_existing_behavior() {
        assert_eq!(
            resolve_desktop_composer_primary_action(DesktopComposerPrimaryActionInput {
                has_in_flight_turn: true,
                can_stop: false,
                is_cancelling: true,
                ..READY_INPUT
            }),
            DesktopComposerPrimaryActionState {
                action: DesktopComposerPrimaryAction::Stop,
                disabled: true,
                loading: true,
            },
        );
        assert_eq!(
            resolve_desktop_composer_primary_action(DesktopComposerPrimaryActionInput {
                voice_entry_availability: DesktopVoiceEntryAvailability::Hidden,
                voice_send_processing: true,
                ..READY_INPUT
            }),
            DesktopComposerPrimaryActionState {
                action: DesktopComposerPrimaryAction::Send,
                disabled: false,
                loading: true,
            },
        );
        assert_eq!(
            resolve_desktop_composer_primary_action(DesktopComposerPrimaryActionInput {
                voice_entry_availability: DesktopVoiceEntryAvailability::Hidden,
                upload_in_progress: true,
                ..READY_INPUT
            }),
            DesktopComposerPrimaryActionState {
                action: DesktopComposerPrimaryAction::Send,
                disabled: true,
                loading: true,
            },
        );
    }

    #[::core::prelude::v1::test]
    fn desktop_skill_chip_removal_recovers_full_partial_and_standalone_intent() {
        let pack_id = SkillPackId::new("P".repeat(21)).expect("pack id");
        let skill_id = SkillId::new("S".repeat(21)).expect("skill id");

        for (chip, expected) in [
            (
                ComposerSkillChip {
                    key: "pack".to_owned(),
                    label: "Pack".to_owned(),
                    kind: ComposerSkillChipKind::SkillPack,
                    skill_id: None,
                    pack_id: Some(pack_id.clone()),
                },
                ComposerSkillSelection::SkillPack {
                    pack_id: pack_id.clone(),
                },
            ),
            (
                ComposerSkillChip {
                    key: "packed".to_owned(),
                    label: "Pack / Skill".to_owned(),
                    kind: ComposerSkillChipKind::PackedSkill,
                    skill_id: Some(skill_id.clone()),
                    pack_id: Some(pack_id.clone()),
                },
                ComposerSkillSelection::Skill {
                    skill_id: skill_id.clone(),
                    pack_id: Some(pack_id.clone()),
                },
            ),
            (
                ComposerSkillChip {
                    key: "standalone".to_owned(),
                    label: "Skill".to_owned(),
                    kind: ComposerSkillChipKind::StandaloneSkill,
                    skill_id: Some(skill_id.clone()),
                    pack_id: None,
                },
                ComposerSkillSelection::Skill {
                    skill_id: skill_id.clone(),
                    pack_id: None,
                },
            ),
        ] {
            assert_eq!(composer_skill_selection_from_chip(&chip), Some(expected));
        }
    }
}
