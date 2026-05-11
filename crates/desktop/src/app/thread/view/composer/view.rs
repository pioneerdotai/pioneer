use crate::{
    app::root::{ComposerAttachmentUploadState, PioneerDesktop},
    assets::PioneerIconName,
};
use gpui::{prelude::*, *};
use gpui_component::{IconName, button::*, input::Input, spinner::Spinner, theme::ActiveTheme, *};
use std::path::Path;

impl PioneerDesktop {
    pub(crate) fn render_composer(&self, cx: &mut Context<Self>) -> AnyElement {
        let composer_state = self.composer_state.clone();
        let attachments = self.composer_attachments.clone();
        let upload_error = self.composer_upload_error.clone();
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
                                        .child(
                                            Button::new("composer-add-attachment")
                                                .small()
                                                .ghost()
                                                .icon(IconName::Plus)
                                                .disabled(self.composer_upload_in_progress)
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

    fn render_composer_attachment_badges(&self, cx: &mut Context<Self>) -> AnyElement {
        let rows = self
            .composer_attachments
            .chunks(3)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();

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
                    .children(
                        row.into_iter()
                            .enumerate()
                            .map(|(column_index, attachment)| {
                                let absolute_index = row_index * 3 + column_index;
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
        let (status_icon, status_text, status_color, is_uploading) = match &attachment.upload_state
        {
            ComposerAttachmentUploadState::Local => {
                (IconName::File, None, cx.theme().foreground, false)
            }
            ComposerAttachmentUploadState::Uploading => (
                IconName::Loader,
                Some("uploading"),
                cx.theme().muted_foreground,
                true,
            ),
            ComposerAttachmentUploadState::Uploaded { .. } => (
                IconName::Check,
                Some("uploaded"),
                cx.theme().muted_foreground,
                false,
            ),
            ComposerAttachmentUploadState::Failed { .. } => (
                IconName::TriangleAlert,
                Some("failed"),
                cx.theme().danger,
                false,
            ),
        };

        h_flex()
            .id(("composer-attachment-chip", index))
            .flex_1()
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
                h_flex()
                    .flex_1()
                    .min_w_0()
                    .overflow_hidden()
                    .gap_1()
                    .child(
                        div()
                            .min_w_0()
                            .overflow_hidden()
                            .whitespace_nowrap()
                            .text_ellipsis()
                            .text_xs()
                            .child(file_name),
                    )
                    .when_some(status_text, |this, status_text| {
                        this.child(
                            div()
                                .flex_none()
                                .text_xs()
                                .text_color(status_color)
                                .whitespace_nowrap()
                                .child(status_text),
                        )
                    }),
            )
            .child(
                Button::new(("composer-attachment-remove", index))
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
}
