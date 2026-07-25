use crate::app::root::PioneerDesktop;
use crate::assets::PioneerIconName;
use gpui::{prelude::*, *};
use gpui_component::{
    Icon,
    button::*,
    popover::{Popover, PopoverState},
    theme::ActiveTheme,
    *,
};
use pioneer_client::composer::{
    model_selection as composer_model_selection, state_machine::ComposerDomainAction,
};
use pioneer_protocol::ThreadMode;

impl PioneerDesktop {
    fn composer_mode_icon(mode: ThreadMode) -> PioneerIconName {
        match mode {
            ThreadMode::Agent => PioneerIconName::Infinity,
            ThreadMode::Chat => PioneerIconName::MessageCircle,
        }
    }

    pub(in crate::app::thread::view) fn render_composer_mode_selector(
        &self,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let selected_mode = self.composer_turn_mode;
        let trigger_icon = Self::composer_mode_icon(selected_mode);
        let trigger_label = match selected_mode {
            ThreadMode::Agent => t!("chat.composer.mode.agent_label").to_string(),
            ThreadMode::Chat => t!("chat.composer.mode.chat_label").to_string(),
        };

        let desktop_entity = cx.entity();

        let ghost_hover = if cx.theme().mode.is_dark() {
            cx.theme().secondary.lighten(0.2).opacity(0.8)
        } else {
            cx.theme().secondary.darken(0.1).opacity(0.8)
        };

        let ghost_active = if cx.theme().mode.is_dark() {
            cx.theme().secondary.lighten(0.3).opacity(0.8)
        } else {
            cx.theme().secondary.darken(0.2).opacity(0.8)
        };

        let muted_bg = cx.theme().muted;
        let radius = cx.theme().radius;
        let foreground = cx.theme().foreground;

        Popover::new("composer-mode-popover")
            .anchor(Corner::TopRight)
            .trigger(
                Button::new("composer-mode-trigger")
                    .small()
                    .ghost()
                    .compact()
                    .child(
                        h_flex()
                            .items_center()
                            .gap_1()
                            .opacity(0.6)
                            .child(Icon::new(trigger_icon).size_3p5())
                            .child(trigger_label)
                            .font_medium(),
                    ),
            )
            .content(move |_, _, popover_cx| {
                let popover_entity: Entity<PopoverState> = popover_cx.entity();

                let render_option = |id: &'static str,
                                     mode: ThreadMode,
                                     icon: PioneerIconName,
                                     label: String,
                                     description: String| {
                    let is_active = selected_mode == mode;

                    let desktop_entity = desktop_entity.clone();
                    let popover_entity = popover_entity.clone();

                    div()
                        .id(id)
                        .w_full()
                        .cursor_pointer()
                        .rounded(radius)
                        .p_2()
                        .text_color(foreground)
                        .when(is_active, |d| d.bg(muted_bg))
                        .hover(move |d| d.bg(ghost_hover))
                        .active(move |d| d.bg(ghost_active))
                        .on_mouse_down(gpui::MouseButton::Left, |_, window, _| {
                            window.prevent_default();
                        })
                        .on_click(move |_, window, cx| {
                            let _ = desktop_entity.update(cx, |view, cx| {
                                if view
                                    .reduce_composer_domain(ComposerDomainAction::SetModeFromUser {
                                        mode,
                                    })
                                    .changed
                                {
                                    cx.notify();
                                }
                            });
                            let _ = popover_entity.update(cx, |state, cx| {
                                state.dismiss(window, cx);
                            });
                        })
                        .child(
                            h_flex()
                                .w_full()
                                .items_center()
                                .gap_3()
                                .child(Icon::new(icon).size_4())
                                .child(
                                    v_flex()
                                        .flex_1()
                                        .min_w_0()
                                        .gap_1p5()
                                        .child(
                                            div()
                                                .text_sm()
                                                .line_height(relative(1.0))
                                                .font_semibold()
                                                .whitespace_normal()
                                                .child(label),
                                        )
                                        .child(
                                            div()
                                                .text_xs()
                                                .line_height(relative(1.3))
                                                .whitespace_normal()
                                                .opacity(0.6)
                                                .child(description),
                                        ),
                                ),
                        )
                        .into_any_element()
                };

                v_flex().w(px(320.)).gap_1().children(
                    composer_model_selection::composer_turn_mode_options()
                        .into_iter()
                        .map(|mode| {
                            let (id, description) = match mode {
                                ThreadMode::Agent => (
                                    "composer-mode-agent",
                                    t!("chat.composer.mode.agent_description").to_string(),
                                ),
                                ThreadMode::Chat => (
                                    "composer-mode-chat",
                                    t!("chat.composer.mode.chat_description").to_string(),
                                ),
                            };
                            render_option(
                                id,
                                mode,
                                Self::composer_mode_icon(mode),
                                match mode {
                                    ThreadMode::Agent => {
                                        t!("chat.composer.mode.agent_label").to_string()
                                    }
                                    ThreadMode::Chat => {
                                        t!("chat.composer.mode.chat_label").to_string()
                                    }
                                },
                                description,
                            )
                        }),
                )
            })
            .into_any_element()
    }
}
