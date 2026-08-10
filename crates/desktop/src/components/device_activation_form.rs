use gpui::{prelude::*, *};
use gpui_component::{
    StyledExt, clipboard::Clipboard, h_flex, spinner::Spinner, theme::ActiveTheme, v_flex,
};
use pioneer_client::gateway::device_activation::DeviceActivationQrPresentation;

#[derive(IntoElement)]
pub(crate) struct CredentialPresentationForm {
    id_prefix: &'static str,
    qr_width: usize,
    qr_modules: Vec<bool>,
    code: Option<SharedString>,
    link: SharedString,
    description: SharedString,
}

impl CredentialPresentationForm {
    pub(crate) fn new(
        id_prefix: &'static str,
        qr_width: usize,
        qr_modules: Vec<bool>,
        link: impl Into<SharedString>,
        description: impl Into<SharedString>,
    ) -> Self {
        Self {
            id_prefix,
            qr_width,
            qr_modules,
            code: None,
            link: link.into(),
            description: description.into(),
        }
    }

    pub(crate) fn code(mut self, code: impl Into<SharedString>) -> Self {
        self.code = Some(code.into());
        self
    }
}

#[derive(Clone)]
pub(crate) enum DeviceActivationFormPhase {
    Loading,
    Ready(DeviceActivationQrPresentation),
    Failed(String),
}

#[derive(IntoElement)]
pub(crate) struct DeviceActivationForm {
    phase: DeviceActivationFormPhase,
    description: SharedString,
}

impl DeviceActivationForm {
    pub(crate) fn new(
        phase: DeviceActivationFormPhase,
        description: impl Into<SharedString>,
    ) -> Self {
        Self {
            phase,
            description: description.into(),
        }
    }
}

impl RenderOnce for DeviceActivationForm {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        match self.phase {
            DeviceActivationFormPhase::Loading => v_flex()
                .w_full()
                .min_h(px(240.))
                .items_center()
                .justify_center()
                .gap_2()
                .child(Spinner::new())
                .child(
                    div()
                        .text_sm()
                        .opacity(0.6)
                        .child(t!("settings.devices.activation_loading").to_string()),
                )
                .into_any_element(),
            DeviceActivationFormPhase::Ready(presentation) => CredentialPresentationForm::new(
                "device-activation",
                presentation.qr_width(),
                presentation.qr_modules().to_vec(),
                presentation.deep_link().to_owned(),
                self.description,
            )
            .code(presentation.manual_code().to_owned())
            .into_any_element(),
            DeviceActivationFormPhase::Failed(error) => v_flex()
                .w_full()
                .min_h(px(180.))
                .justify_center()
                .gap_2()
                .child(
                    div()
                        .text_sm()
                        .font_medium()
                        .child(t!("settings.devices.activation_failed").to_string()),
                )
                .child(div().text_xs().text_color(cx.theme().danger).child(error))
                .into_any_element(),
        }
    }
}

impl RenderOnce for CredentialPresentationForm {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let Self {
            id_prefix,
            qr_width,
            qr_modules,
            code,
            link,
            description,
        } = self;
        let (copy_code_id, copy_link_id) = if id_prefix == "invitation" {
            ("invitation-copy-code", "invitation-copy-link")
        } else {
            ("device-activation-copy-code", "device-activation-copy-link")
        };

        v_flex()
            .w_full()
            .pt_1()
            .pb_5()
            .gap_5()
            .items_center()
            .child(
                div()
                    .text_sm()
                    .line_height(relative(1.35))
                    .opacity(0.6)
                    .child(description),
            )
            .child(
                v_flex()
                    .w_full()
                    .items_center()
                    .gap_4()
                    .child(render_activation_qr(id_prefix, qr_width, &qr_modules))
                    .when_some(code, |content, code| {
                        content.child(
                            v_flex()
                                .w_full()
                                .items_center()
                                .gap_1()
                                .child(
                                    div()
                                        .text_sm()
                                        .opacity(0.6)
                                        .child(t!("settings.devices.code_label").to_string()),
                                )
                                .child(
                                    div()
                                        .flex()
                                        .w_full()
                                        .min_w_0()
                                        .p_4()
                                        .rounded_2xl()
                                        .justify_center()
                                        .bg(cx.theme().muted)
                                        .text_xl()
                                        .font_semibold()
                                        .child(
                                            div()
                                                .min_w_0()
                                                .flex_1()
                                                .whitespace_normal()
                                                .text_center()
                                                .child(code.clone()),
                                        )
                                        .child(
                                            div()
                                                .absolute()
                                                .top_1p5()
                                                .right_1p5()
                                                .child(Clipboard::new(copy_code_id).value(code)),
                                        ),
                                ),
                        )
                    })
                    .child(
                        v_flex()
                            .w_full()
                            .items_center()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .opacity(0.6)
                                    .child(t!("settings.devices.link_label").to_string()),
                            )
                            .child(
                                div()
                                    .flex()
                                    .w_full()
                                    .min_w_0()
                                    .p_4()
                                    .rounded_2xl()
                                    .justify_center()
                                    .bg(cx.theme().muted)
                                    .text_sm()
                                    .font_medium()
                                    .child(
                                        div()
                                            .min_w_0()
                                            .flex_1()
                                            .overflow_x_hidden()
                                            .whitespace_normal()
                                            .text_center()
                                            .child(link.clone()),
                                    )
                                    .child(
                                        div()
                                            .absolute()
                                            .top_1p5()
                                            .right_1p5()
                                            .child(Clipboard::new(copy_link_id).value(link)),
                                    ),
                            ),
                    ),
            )
    }
}

fn render_activation_qr(id_prefix: &'static str, width: usize, modules: &[bool]) -> AnyElement {
    let width = width.max(1);
    v_flex()
        .p_3()
        .bg(rgb(0xffffff))
        .children(modules.chunks(width).enumerate().map(|(row_index, row)| {
            h_flex().children(row.iter().enumerate().map(move |(column_index, dark)| {
                div()
                    .id((id_prefix, row_index * width + column_index))
                    .w(px(4.))
                    .h(px(4.))
                    .bg(if *dark { rgb(0x000000) } else { rgb(0xffffff) })
            }))
        }))
        .into_any_element()
}

#[cfg(test)]
mod tests {
    #[::core::prelude::v1::test]
    fn shared_form_owns_the_complete_activation_presentation() {
        let source = include_str!("device_activation_form.rs");
        assert!(source.contains("DeviceActivationFormPhase::Loading"));
        assert!(source.contains("DeviceActivationFormPhase::Ready"));
        assert!(source.contains("DeviceActivationFormPhase::Failed"));
        assert!(source.contains("CredentialPresentationForm"));
        assert!(source.contains(".when_some(code"));
        assert!(source.contains("settings.devices.code_label"));
        assert!(source.contains("settings.devices.link_label"));
        assert!(source.contains("render_activation_qr"));
    }
}
