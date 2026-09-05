use crate::app::root::{AdministrationContentView, PioneerDesktop};
use gpui_kit::component::{theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};

impl PioneerDesktop {
    pub(super) fn render_administration_screen(
        scroll_id: &'static str,
        title: String,
        description: String,
        header_action: Option<AnyElement>,
        content: AnyElement,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        pioneer_observability::record_qualification_diagnostic!(record_render(
            pioneer_observability::RenderRegion::Administration
        ));
        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .child(
                h_flex()
                    .justify_between()
                    .items_center()
                    .pt_3()
                    .px_6()
                    .pb_3()
                    .gap_2()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        v_flex()
                            .child(div().text_xl().font_semibold().child(title))
                            .child(div().text_sm().opacity(0.6).child(description)),
                    )
                    .when_some(header_action, |header, action| header.child(action)),
            )
            .child(
                v_flex()
                    .id(scroll_id)
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .p_6()
                    .child(content),
            )
            .into_any_element()
    }

    pub(crate) fn render_administration(
        &self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        match self.administration_content_view {
            AdministrationContentView::Members => self.render_administration_members(window, cx),
            AdministrationContentView::Invitations => {
                self.render_administration_invitations(window, cx)
            }
        }
    }
}
