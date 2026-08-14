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
use pioneer_client::composer::state_machine::ComposerDomainAction;
use pioneer_protocol::TurnPermissionMode;

impl PioneerDesktop {
    fn composer_permission_icon(mode: TurnPermissionMode) -> PioneerIconName {
        match mode {
            TurnPermissionMode::FullAccess => PioneerIconName::ShieldCheck,
            TurnPermissionMode::AutoAcceptEdits => PioneerIconName::ShieldAlert,
            TurnPermissionMode::Supervised => PioneerIconName::ShieldX,
        }
    }

    fn composer_permission_option_id(mode: TurnPermissionMode) -> &'static str {
        match mode {
            TurnPermissionMode::Supervised => "composer-permission-supervised",
            TurnPermissionMode::AutoAcceptEdits => "composer-permission-auto-accept-edits",
            TurnPermissionMode::FullAccess => "composer-permission-full-access",
        }
    }

    pub(super) fn render_composer_permission_selector(&self, cx: &mut Context<Self>) -> AnyElement {
        let options = self.authorized_composer_permission_options();
        let selected_mode = if options
            .iter()
            .any(|option| option.mode == self.composer_permission_mode)
        {
            self.composer_permission_mode
        } else {
            options
                .last()
                .map(|option| option.mode)
                .unwrap_or(TurnPermissionMode::Supervised)
        };
        let selected_option = options
            .iter()
            .find(|option| option.mode == selected_mode)
            .cloned();
        let selected_label = selected_option
            .as_ref()
            .map(|option| option.label.clone())
            .unwrap_or_default();
        let trigger_icon = Self::composer_permission_icon(selected_mode);
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

        Popover::new("composer-permission-popover")
            .anchor(Anchor::BottomLeft)
            .trigger(
                Button::new("composer-permission-trigger")
                    .small()
                    .ghost()
                    .compact()
                    .disabled(
                        self.desktop_voice_context_locked()
                            || !self.can_start_active_thread_agent_presentation()
                            || options.is_empty(),
                    )
                    .child(
                        h_flex()
                            .items_center()
                            .max_w(px(164.))
                            .overflow_hidden()
                            .gap_1()
                            .opacity(0.6)
                            .child(Icon::new(trigger_icon).size_3p5())
                            .child(
                                div()
                                    .min_w_0()
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .child(selected_label),
                            )
                            .font_medium(),
                    ),
            )
            .content(move |_, _, popover_cx| {
                let popover_entity: Entity<PopoverState> = popover_cx.entity();

                let render_option =
                    |option: pioneer_client::composer::permissions::ComposerPermissionModeOption| {
                        let mode = option.mode;
                        let is_active = selected_mode == mode;
                        let desktop_entity = desktop_entity.clone();
                        let popover_entity = popover_entity.clone();

                        div()
                            .id(Self::composer_permission_option_id(mode))
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
                                        .reduce_composer_domain(
                                            ComposerDomainAction::SetPermissionMode { mode },
                                        )
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
                                    .child(Icon::new(Self::composer_permission_icon(mode)).size_4())
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
                                                    .child(option.label),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .line_height(relative(1.3))
                                                    .whitespace_normal()
                                                    .opacity(0.6)
                                                    .child(option.description),
                                            ),
                                    ),
                            )
                            .into_any_element()
                    };

                v_flex()
                    .w(px(320.))
                    .gap_1()
                    .children(options.iter().cloned().map(render_option))
            })
            .into_any_element()
    }
}
