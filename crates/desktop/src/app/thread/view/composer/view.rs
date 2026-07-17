use crate::{
    app::root::{
        ComposerAttachmentUploadState, ComposerCapability, ComposerCapabilityKind, PioneerDesktop,
    },
    app::thread::view::composer::voice::DesktopVoiceEntryAvailability,
    assets::PioneerIconName,
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
const COMPOSER_ATTACHMENT_TEXT_FADE_WIDTH: Pixels = px(24.);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DesktopComposerPrimaryAction {
    Send,
    Stop,
    VoiceReady,
}

fn resolve_desktop_composer_primary_action(
    voice_hold_ui_active: bool,
    has_in_flight_turn: bool,
    voice_entry_availability: DesktopVoiceEntryAvailability,
) -> DesktopComposerPrimaryAction {
    if voice_hold_ui_active {
        return DesktopComposerPrimaryAction::VoiceReady;
    }
    if has_in_flight_turn {
        return DesktopComposerPrimaryAction::Stop;
    }

    match voice_entry_availability {
        DesktopVoiceEntryAvailability::Hidden => DesktopComposerPrimaryAction::Send,
        DesktopVoiceEntryAvailability::Ready => DesktopComposerPrimaryAction::VoiceReady,
    }
}

impl PioneerDesktop {
    pub(crate) fn render_composer(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let composer_state = self.composer_state.clone();
        let attachments = self.composer_attachments.clone();
        let capabilities = self.effective_composer_capabilities();
        let upload_error = self.composer_upload_error.clone();
        let microphone_error = self.desktop_microphone_error_message();
        let task_child_locked = self.active_task_thread_navigation().is_some();
        let can_send = self.can_submit_message(cx);
        let active_thread_snapshot = self.client_snapshot().active_thread;
        let gateway_connected =
            self.gateway.connection_state == crate::app::root::GatewayConnectionState::Connected;
        let is_cancelling = !task_child_locked && active_thread_snapshot.is_cancelling_turn();
        let can_stop =
            !task_child_locked && active_thread_snapshot.can_request_turn_cancel(gateway_connected);
        let has_in_flight_turn = !task_child_locked && active_thread_snapshot.has_in_flight_turn();
        let composer_text = composer_state.read(cx).value().trim().to_owned();
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
            && !task_child_locked
            && !is_cancelling
            && !self.composer_upload_in_progress
            && attachments.is_empty()
            && capabilities.is_empty()
            && !composer_text.is_empty();
        let desktop_voice_context_locked = self.desktop_voice_context_locked();
        let desktop_voice_hold_ui_active = self.desktop_voice_hold_ui_active();
        let desktop_voice_send_processing = self.desktop_voice_send_processing();
        let composer_payload_empty =
            composer_text.is_empty() && attachments.is_empty() && capabilities.is_empty();
        let voice_entry_availability = if has_in_flight_turn {
            DesktopVoiceEntryAvailability::Hidden
        } else {
            self.desktop_voice_entry_availability(composer_payload_empty)
        };
        let composer_primary_action = resolve_desktop_composer_primary_action(
            desktop_voice_hold_ui_active,
            has_in_flight_turn,
            voice_entry_availability,
        );
        let composer_action_loading = self.composer_upload_in_progress
            || (has_in_flight_turn && is_cancelling)
            || desktop_voice_send_processing;

        let composer_action_is_stop = composer_primary_action == DesktopComposerPrimaryAction::Stop;
        let composer_action_id = if composer_action_is_stop {
            "stop-turn"
        } else {
            "send-message"
        };

        let composer_action_disabled = if desktop_voice_send_processing {
            false
        } else if composer_action_is_stop {
            !can_stop
        } else {
            !can_send
        };

        h_flex()
            .w_full()
            .justify_center()
            .pb_4()
            .child(
                v_flex().w_full().max_w(px(800.)).px_6().child(
                    v_flex()
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
                                    !attachments.is_empty() || !capabilities.is_empty(),
                                    |this| this.child(self.render_composer_chip_badges(cx)),
                                )
                                .when_some(upload_error, |this, error| {
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
                                        .disabled(task_child_locked || desktop_voice_context_locked)
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
                                        .child(self.render_composer_permission_selector(cx))
                                        .child(self.render_composer_model_selector(cx)),
                                )
                                .child(
                                    h_flex()
                                        .items_center()
                                        .gap_2()
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
                                                                view.stop_active_turn(window, cx);
                                                            } else {
                                                                view.submit_composer_message(
                                                                    window, cx,
                                                                );
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

    fn render_composer_add_menu(&self, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let disabled = self.composer_upload_in_progress
            || self.active_task_thread_navigation().is_some()
            || self.desktop_voice_context_locked();
        let capability_target = self.composer_capability_target();
        let capability_policy = capability_target.policy();

        Button::new("composer-add-attachment")
            .small()
            .ghost()
            .compact()
            .child(Icon::new(IconName::Plus).size_5().opacity(0.6))
            .disabled(disabled)
            .dropdown_menu_with_anchor(Corner::BottomLeft, move |menu, _, _| {
                let menu = menu.min_w(px(196.)).item(Self::composer_add_menu_item(
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
                ));

                let menu = if capability_policy.supports_skills {
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

                if capability_policy.supports_mcp_tools {
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
        let attachment_rows =
            self.composer_attachments
                .chunks(3)
                .enumerate()
                .map(|(row_index, chunk)| ComposerChipRow::Attachments {
                    start_index: row_index * 3,
                    items: chunk.to_vec(),
                });
        let effective_capabilities = self.effective_composer_capabilities();
        let capability_rows =
            effective_capabilities
                .chunks(3)
                .enumerate()
                .map(|(row_index, chunk)| ComposerChipRow::Capabilities {
                    start_index: row_index * 3,
                    items: chunk.to_vec(),
                });
        let rows = attachment_rows.chain(capability_rows).collect::<Vec<_>>();

        v_flex()
            .w_full()
            .min_w_0()
            .pt_2()
            .px_2()
            .gap_1p5()
            .children(rows.into_iter().enumerate().map(|(row_index, row)| {
                h_flex()
                    .id(("composer-attachment-row", row_index))
                    .w_full()
                    .min_w_0()
                    .gap_1p5()
                    .children(match row {
                        ComposerChipRow::Attachments { start_index, items } => items
                            .into_iter()
                            .enumerate()
                            .map(|(column_index, attachment)| {
                                let absolute_index = start_index + column_index;
                                self.render_composer_attachment_badge(
                                    attachment,
                                    absolute_index,
                                    cx,
                                )
                            })
                            .collect::<Vec<_>>(),
                        ComposerChipRow::Capabilities { start_index, items } => items
                            .into_iter()
                            .enumerate()
                            .map(|(column_index, capability)| {
                                let absolute_index = start_index + column_index;
                                self.render_composer_capability_badge(
                                    capability,
                                    absolute_index,
                                    cx,
                                )
                            })
                            .collect::<Vec<_>>(),
                    })
            }))
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
}

enum ComposerChipRow {
    Attachments {
        start_index: usize,
        items: Vec<crate::app::root::ComposerAttachment>,
    },
    Capabilities {
        start_index: usize,
        items: Vec<ComposerCapability>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    const VOICE_AVAILABILITIES: [DesktopVoiceEntryAvailability; 2] = [
        DesktopVoiceEntryAvailability::Hidden,
        DesktopVoiceEntryAvailability::Ready,
    ];

    #[::core::prelude::v1::test]
    fn desktop_composer_primary_action_priority_matrix_is_complete() {
        for availability in VOICE_AVAILABILITIES {
            assert_eq!(
                resolve_desktop_composer_primary_action(true, false, availability),
                DesktopComposerPrimaryAction::VoiceReady
            );
            assert_eq!(
                resolve_desktop_composer_primary_action(true, true, availability),
                DesktopComposerPrimaryAction::VoiceReady
            );
            assert_eq!(
                resolve_desktop_composer_primary_action(false, true, availability),
                DesktopComposerPrimaryAction::Stop
            );
        }

        assert_eq!(
            resolve_desktop_composer_primary_action(
                false,
                false,
                DesktopVoiceEntryAvailability::Hidden,
            ),
            DesktopComposerPrimaryAction::Send
        );
        assert_eq!(
            resolve_desktop_composer_primary_action(
                false,
                false,
                DesktopVoiceEntryAvailability::Ready,
            ),
            DesktopComposerPrimaryAction::VoiceReady
        );
    }
}
