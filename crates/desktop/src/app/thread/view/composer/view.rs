use crate::{
    app::root::{
        ComposerAttachmentUploadState, ComposerCapability, ComposerCapabilityKind, PioneerDesktop,
    },
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

impl PioneerDesktop {
    pub(crate) fn render_composer(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let composer_state = self.composer_state.clone();
        let attachments = self.composer_attachments.clone();
        let capabilities = self.composer_capabilities.clone();
        let upload_error = self.composer_upload_error.clone();
        let can_send = self.can_submit_message(cx);
        let active_thread_snapshot = self.client_snapshot().active_thread;
        let gateway_connected =
            self.gateway.connection_state == crate::app::root::GatewayConnectionState::Connected;
        let is_cancelling = active_thread_snapshot.is_cancelling_turn();
        let can_stop = active_thread_snapshot.can_request_turn_cancel(gateway_connected);
        let has_in_flight_turn = active_thread_snapshot.has_in_flight_turn();

        let composer_action_id = if has_in_flight_turn {
            "stop-turn"
        } else {
            "send-message"
        };

        let composer_action_disabled = if has_in_flight_turn {
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
                                .child(Input::new(&composer_state).appearance(false)),
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
                                        .child(self.render_composer_mode_selector(cx))
                                        .child(self.render_composer_model_selector(cx)),
                                )
                                .child(
                                    div().child(
                                        Button::new(composer_action_id)
                                            .primary()
                                            .rounded_full()
                                            .disabled(composer_action_disabled)
                                            .loading(
                                                self.composer_upload_in_progress
                                                    || (has_in_flight_turn && is_cancelling),
                                            )
                                            .when(has_in_flight_turn, |this| {
                                                this.icon(PioneerIconName::Square)
                                            })
                                            .when(!has_in_flight_turn, |this| {
                                                this.icon(IconName::ArrowUp)
                                            })
                                            .on_click(cx.listener(move |view, _, window, cx| {
                                                if has_in_flight_turn {
                                                    view.stop_active_turn(window, cx);
                                                } else {
                                                    view.submit_composer_message(window, cx);
                                                }
                                            })),
                                    ),
                                ),
                        ),
                ),
            )
            .into_any_element()
    }

    fn render_composer_add_menu(&self, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let disabled = self.composer_upload_in_progress;

        Button::new("composer-add-attachment")
            .small()
            .ghost()
            .compact()
            .child(Icon::new(IconName::Plus).size_5().opacity(0.6))
            .disabled(disabled)
            .dropdown_menu_with_anchor(Corner::BottomLeft, move |menu, _, _| {
                menu.min_w(px(196.))
                    .item(Self::composer_add_menu_item(
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
                    .item(Self::composer_add_menu_item(
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
                    .item(Self::composer_add_menu_item(
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
        let capability_rows =
            self.composer_capabilities
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
                    .disabled(self.composer_upload_in_progress)
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
                    .disabled(self.composer_upload_in_progress)
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
