use crate::{app::root::PioneerDesktop, assets::PioneerIconName};
use gpui::{prelude::*, *};
use gpui_component::{IconName, button::*, input::Input, theme::ActiveTheme, *};
use std::path::Path;

impl PioneerDesktop {
    pub(crate) fn render_composer(&self, cx: &mut Context<Self>) -> AnyElement {
        let composer_state = self.composer_state.clone();
        let attachments = self.composer_attachments.clone();
        let can_send = self.can_submit_message(cx);
        let in_flight_turn_id = self
            .active_thread_conversation()
            .and_then(|conversation| conversation.in_flight_turn_id().map(str::to_owned));
        let is_cancelling = self
            .active_thread_conversation()
            .is_some_and(|conversation| conversation.is_cancelling_turn());
        let can_stop = in_flight_turn_id.is_some()
            && self.gateway.connection_state == crate::app::root::GatewayConnectionState::Connected
            && !is_cancelling;
        let has_in_flight_turn = in_flight_turn_id.is_some();

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
                                .when(!attachments.is_empty(), |this| {
                                    this.child(self.render_composer_attachment_badges(cx))
                                })
                                .child(Input::new(&composer_state).appearance(false)),
                        )
                        .child(
                            h_flex()
                                .p_2()
                                .justify_between()
                                .items_center()
                                .bg(cx.theme().background)
                                .rounded_b_2xl()
                                .child(
                                    h_flex()
                                        .items_center()
                                        .gap_2()
                                        .child(
                                            Button::new("composer-add-attachment")
                                                .small()
                                                .ghost()
                                                .icon(IconName::Plus)
                                                .on_click(cx.listener(|view, _, window, cx| {
                                                    view.open_composer_file_picker(window, cx);
                                                })),
                                        )
                                        .child(self.render_composer_mode_selector(cx))
                                        .child(self.render_composer_model_selector(cx)),
                                )
                                .child(
                                    div().child(
                                        Button::new(composer_action_id)
                                            .primary()
                                            .rounded_full()
                                            .disabled(composer_action_disabled)
                                            .loading(has_in_flight_turn && is_cancelling)
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

    fn render_composer_attachment_badges(&self, cx: &mut Context<Self>) -> AnyElement {
        let rows = self
            .composer_attachments
            .chunks(4)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();

        v_flex()
            .w_full()
            .pt_2()
            .px_2()
            .gap_1p5()
            .children(rows.into_iter().enumerate().map(|(row_index, row)| {
                h_flex()
                    .id(("composer-attachment-row", row_index))
                    .w_full()
                    .gap_2()
                    .children(
                        row.into_iter()
                            .enumerate()
                            .map(|(column_index, attachment)| {
                                let absolute_index = row_index * 4 + column_index;
                                self.render_composer_attachment_badge(
                                    attachment,
                                    absolute_index,
                                    cx,
                                )
                            }),
                    )
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
        let file_name = if attachment.file_name.trim().is_empty() {
            Path::new(attachment.path.as_str())
                .file_name()
                .and_then(|value| value.to_str())
                .map(str::to_owned)
                .unwrap_or_else(|| attachment.path.clone())
        } else {
            attachment.file_name.clone()
        };

        h_flex()
            .id(("composer-attachment-chip", index))
            .flex_1()
            .max_w(px(196.))
            .h(px(32.))
            .px_2()
            .items_center()
            .gap_2()
            .rounded_full()
            .border_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .group(group_id.clone())
            .child(
                div()
                    .size(px(20.))
                    .rounded_full()
                    .flex()
                    .items_center()
                    .justify_center()
                    .bg(cx.theme().muted)
                    .child(Icon::new(IconName::File).size_3().opacity(0.8)),
            )
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .text_ellipsis()
                    .text_sm()
                    .child(file_name),
            )
            .child(
                Button::new(("composer-attachment-remove", index))
                    .ghost()
                    .xsmall()
                    .compact()
                    .icon(IconName::Close)
                    .opacity(0.0)
                    .group_hover(group_id, |this| this.opacity(0.85))
                    .on_click(cx.listener(move |view, _, _, cx| {
                        view.remove_composer_attachment_at(index);
                        cx.notify();
                    })),
            )
            .into_any_element()
    }
}
