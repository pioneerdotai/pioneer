use gpui_kit::component::{StyledExt, h_flex, theme::ActiveTheme, v_flex};
use gpui_kit::{prelude::*, *};
use pioneer_client::gateway::device_activation::DeviceActivationQrPresentation;

#[derive(IntoElement)]
pub(crate) struct CredentialPresentationForm {
    id_prefix: &'static str,
    qr_width: usize,
    qr_modules: Vec<bool>,
    code: Option<SharedString>,
    link: SharedString,
    description: SharedString,
    link_copy: Entity<ActivationCopyButton>,
    code_copy: Option<Entity<ActivationCopyButton>>,
}

impl CredentialPresentationForm {
    pub(crate) fn new(
        id_prefix: &'static str,
        qr_width: usize,
        qr_modules: Vec<bool>,
        link: impl Into<SharedString>,
        description: impl Into<SharedString>,
        link_copy: Entity<ActivationCopyButton>,
    ) -> Self {
        Self {
            id_prefix,
            qr_width,
            qr_modules,
            code: None,
            link: link.into(),
            description: description.into(),
            link_copy,
            code_copy: None,
        }
    }

    pub(crate) fn code(
        mut self,
        code: impl Into<SharedString>,
        copy: Entity<ActivationCopyButton>,
    ) -> Self {
        self.code = Some(code.into());
        self.code_copy = Some(copy);
        self
    }
}

#[derive(Clone)]
pub(crate) enum DeviceActivationFormPhase {
    Ready(DeviceActivationQrPresentation),
}

#[derive(IntoElement)]
pub(crate) struct DeviceActivationForm {
    phase: DeviceActivationFormPhase,
    description: SharedString,
    link_copy: Entity<ActivationCopyButton>,
    code_copy: Entity<ActivationCopyButton>,
}

impl DeviceActivationForm {
    pub(crate) fn new(
        phase: DeviceActivationFormPhase,
        description: impl Into<SharedString>,
        link_copy: Entity<ActivationCopyButton>,
        code_copy: Entity<ActivationCopyButton>,
    ) -> Self {
        Self {
            phase,
            description: description.into(),
            link_copy,
            code_copy,
        }
    }
}

impl RenderOnce for DeviceActivationForm {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        match self.phase {
            DeviceActivationFormPhase::Ready(presentation) => CredentialPresentationForm::new(
                "device-activation",
                presentation.qr_width(),
                presentation.qr_modules().to_vec(),
                presentation.deep_link().to_owned(),
                self.description,
                self.link_copy,
            )
            .code(presentation.manual_code().to_owned(), self.code_copy)
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
            link_copy,
            code_copy,
        } = self;

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
                                                .when_some(code_copy, |view, copy| {
                                                    view.child(copy)
                                                }),
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
                                    .child(div().absolute().top_1p5().right_1p5().child(link_copy)),
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

pub(crate) struct ActivationCopyButton {
    id: SharedString,
    value: std::rc::Rc<dyn Fn(&App) -> Option<(u64, String)>>,
    owner: WeakEntity<crate::administration::AdministrationView>,
    copied: bool,
    reset: Option<Task<()>>,
}
impl ActivationCopyButton {
    pub(crate) fn new(
        id: SharedString,
        value: impl Fn(&App) -> Option<(u64, String)> + 'static,
        owner: WeakEntity<crate::administration::AdministrationView>,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|_| Self {
            id,
            value: std::rc::Rc::new(value),
            owner,
            copied: false,
            reset: None,
        })
    }
}
impl Render for ActivationCopyButton {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        use gpui_kit::component::{
            IconName, Sizable,
            button::{Button, ButtonVariants},
        };
        Button::new(self.id.clone())
            .icon(if self.copied {
                IconName::Check
            } else {
                IconName::Copy
            })
            .ghost()
            .xsmall()
            .when(!self.copied, |button| {
                button.on_click(cx.listener(|view, _, _, cx| {
                    cx.stop_propagation();
                    let Some((generation, value)) = (view.value)(cx) else {
                        return;
                    };
                    if !view
                        .owner
                        .update(cx, |owner, cx| {
                            owner.copy_activation(generation, &value, cx)
                        })
                        .unwrap_or(false)
                    {
                        return;
                    }
                    view.copied = true;
                    view.reset = Some(cx.spawn(async move |view, cx| {
                        cx.background_executor()
                            .timer(std::time::Duration::from_secs(2))
                            .await;
                        let _ = view.update(cx, |view, cx| {
                            view.copied = false;
                            cx.notify();
                        });
                    }));
                    cx.notify();
                }))
            })
    }
}
