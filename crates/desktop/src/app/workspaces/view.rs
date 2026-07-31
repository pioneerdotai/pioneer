use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop},
    assets::PioneerIconName,
};
use gpui::{prelude::*, *};
use gpui_component::{
    Colorize, Disableable, Icon, IconName, Selectable, Sizable, StyledExt,
    button::{Button, ButtonCustomVariant, ButtonVariants},
    divider::Divider,
    h_flex,
    popover::{Popover, PopoverState},
    spinner::Spinner,
    theme::ActiveTheme,
    v_flex,
};
use pioneer_client::workspaces::selectors as workspace_selectors;
use pioneer_protocol::Workspace;

#[derive(IntoElement)]
struct WorkspaceSelectorTrigger {
    id: ElementId,
    name: SharedString,
    subtitle: SharedString,
    show_spinner: bool,
    disabled: bool,
    selected: bool,
}

impl WorkspaceSelectorTrigger {
    fn new(
        id: impl Into<ElementId>,
        name: impl Into<SharedString>,
        subtitle: impl Into<SharedString>,
        show_spinner: bool,
        disabled: bool,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            subtitle: subtitle.into(),
            show_spinner,
            disabled,
            selected: false,
        }
    }
}

impl Selectable for WorkspaceSelectorTrigger {
    fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    fn is_selected(&self) -> bool {
        self.selected
    }
}

impl RenderOnce for WorkspaceSelectorTrigger {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();

        let selector_bg = if self.selected {
            theme.muted
        } else {
            theme.sidebar
        };

        let hover_fade_bg = if theme.mode.is_dark() {
            theme.secondary.lighten(0.2)
        } else {
            theme.secondary.darken(0.1)
        };

        let hover_bg = hover_fade_bg.opacity(0.8);

        div()
            .id(self.id)
            .w_full()
            .h_12()
            .px_2()
            .flex()
            .items_center()
            .rounded_lg()
            .bg(selector_bg)
            .when(!self.disabled, |this| {
                this.group("workspace-selector-trigger")
            })
            .when(!self.disabled, |this| {
                this.hover(move |style| style.bg(hover_bg))
            })
            .when(self.disabled, |this| this.opacity(0.55))
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .gap_3()
                    .child(
                        div()
                            .size_8()
                            .flex_none()
                            .rounded_lg()
                            .bg(theme.foreground)
                            .flex()
                            .items_center()
                            .justify_center()
                            .when(self.show_spinner, |this| {
                                this.child(Spinner::new().with_size(gpui_component::Size::Small))
                            })
                            .when(!self.show_spinner, |this| {
                                this.child(
                                    Icon::new(PioneerIconName::GalleryVerticalEnd)
                                        .size_4()
                                        .text_color(theme.background),
                                )
                            }),
                    )
                    .child(
                        v_flex()
                            .flex_1()
                            .min_w_0()
                            .gap_1()
                            .child(
                                div()
                                    .relative()
                                    .w_full()
                                    .min_w_0()
                                    .text_sm()
                                    .font_semibold()
                                    .line_height(relative(1.))
                                    .overflow_hidden()
                                    .whitespace_nowrap()
                                    .child(self.name)
                                    .child(
                                        div()
                                            .absolute()
                                            .top_0()
                                            .right_0()
                                            .bottom_0()
                                            .w_11()
                                            .bg(linear_gradient(
                                                90.,
                                                linear_color_stop(selector_bg.opacity(0.), 0.),
                                                linear_color_stop(selector_bg, 1.),
                                            ))
                                            .group_hover(
                                                "workspace-selector-trigger",
                                                move |style| {
                                                    style.bg(linear_gradient(
                                                        90.,
                                                        linear_color_stop(
                                                            hover_fade_bg.opacity(0.),
                                                            0.,
                                                        ),
                                                        linear_color_stop(hover_fade_bg, 1.),
                                                    ))
                                                },
                                            ),
                                    ),
                            )
                            .child(
                                div()
                                    .relative()
                                    .w_full()
                                    .min_w_0()
                                    .text_xs()
                                    .line_height(relative(1.))
                                    .opacity(0.6)
                                    .overflow_hidden()
                                    .whitespace_nowrap()
                                    .child(self.subtitle)
                                    .child(
                                        div()
                                            .absolute()
                                            .top_0()
                                            .right_0()
                                            .bottom_0()
                                            .w_11()
                                            .bg(linear_gradient(
                                                90.,
                                                linear_color_stop(selector_bg.opacity(0.), 0.),
                                                linear_color_stop(selector_bg, 1.),
                                            ))
                                            .group_hover(
                                                "workspace-selector-trigger",
                                                move |style| {
                                                    style.bg(linear_gradient(
                                                        90.,
                                                        linear_color_stop(
                                                            hover_fade_bg.opacity(0.),
                                                            0.,
                                                        ),
                                                        linear_color_stop(hover_fade_bg, 1.),
                                                    ))
                                                },
                                            ),
                                    ),
                            ),
                    )
                    .child(
                        Icon::new(IconName::ChevronsUpDown)
                            .size_4()
                            .flex_none()
                            .opacity(0.8),
                    ),
            )
    }
}

impl PioneerDesktop {
    pub(in crate::app) fn render_workspaces_popover(&self, cx: &mut Context<Self>) -> AnyElement {
        let active_workspace_id = self.active_workspace_id().map(str::to_owned);
        let active_workspace_name = active_workspace_id
            .as_deref()
            .and_then(|_| self.active_workspace())
            .and_then(workspace_selectors::workspace_display_name)
            .map(str::to_owned)
            .unwrap_or_else(|| t!("workspace.selector_label").to_string());
        let active_workspaces = self
            .active_workspaces()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let selector_disabled = self.gateway.connecting
            || self.gateway.connection_state.is_transitioning()
            || self.gateway.connection_state != GatewayConnectionState::Connected
            || self.gateway.session_refresh_in_flight
            || self.workspace_action_in_progress()
            || self.desktop_voice_context_locked()
            || self.composer_upload_in_progress;
        // A user-initiated switch is optimistic: keep showing the newly selected
        // workspace while its server scope is synchronized in the background.
        let show_spinner = self.workspaces_loading();
        let workspace_selector_subtitle = t!("workspace.selector_label").to_string();
        let workspaces_loading = self.workspaces_loading();
        let workspace_error = self.workspaces_error().map(str::to_owned);
        let desktop_entity = cx.entity();
        let active_indicator_color = cx.theme().success;
        let inactive_indicator_color = cx.theme().yellow;

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

        let option_style = ButtonCustomVariant::new(cx)
            .color(cx.theme().transparent)
            .foreground(cx.theme().foreground)
            .hover(ghost_hover)
            .active(ghost_active);

        let selected_style = ButtonCustomVariant::new(cx)
            .color(cx.theme().muted)
            .foreground(cx.theme().foreground)
            .hover(ghost_hover)
            .active(ghost_active);

        Popover::new("workspace-switcher-popover")
            .anchor(Corner::TopRight)
            .p_0()
            .trigger(WorkspaceSelectorTrigger::new(
                "workspace-switcher-button",
                active_workspace_name.clone(),
                workspace_selector_subtitle.clone(),
                show_spinner,
                selector_disabled,
            ))
            .content({
                let active_workspaces = active_workspaces.clone();
                let active_workspace_id = active_workspace_id.clone();
                let workspace_error = workspace_error.clone();
                let desktop_entity = desktop_entity.clone();
                let active_indicator_color = active_indicator_color;
                let inactive_indicator_color = inactive_indicator_color;
                move |_, _window, popover_cx| {
                    let popover_entity = popover_cx.entity();

                    v_flex()
                        .w(px(320.))
                        .gap_2()
                        .when_some(workspace_error.clone(), |this, error| {
                            this.child(
                                div()
                                    .p_2()
                                    .pb_0()
                                    .text_xs()
                                    .line_height(relative(1.3))
                                    .whitespace_normal()
                                    .text_color(popover_cx.theme().danger)
                                    .child(error),
                            )
                        })
                        .when(active_workspaces.is_empty(), |this| {
                            this.child(
                                div()
                                    .w_full()
                                    .p_2()
                                    .pb_0()
                                    .text_sm()
                                    .text_color(popover_cx.theme().muted_foreground)
                                    .child(if workspaces_loading {
                                        t!("workspace.loading").to_string()
                                    } else {
                                        t!("workspace.no_available").to_string()
                                    }),
                            )
                        })
                        .when(!active_workspaces.is_empty(), |this| {
                            this.child(
                                v_flex().w_full().p_2().pb_0().gap_1().children(
                                    active_workspaces.iter().enumerate().map(
                                        |(index, workspace)| {
                                            Self::render_workspace_popover_option(
                                                index,
                                                workspace,
                                                active_workspace_id.as_deref(),
                                                selector_disabled,
                                                option_style,
                                                selected_style,
                                                active_indicator_color,
                                                inactive_indicator_color,
                                                desktop_entity.clone(),
                                                popover_entity.clone(),
                                            )
                                        },
                                    ),
                                ),
                            )
                        })
                        .child(Divider::horizontal())
                        .child(h_flex().p_2().pt_0().justify_start().child(
                            Self::render_create_workspace_popover_action(
                                selector_disabled,
                                desktop_entity.clone(),
                                popover_entity.clone(),
                            ),
                        ))
                }
            })
            .into_any_element()
    }

    fn render_create_workspace_popover_action(
        selector_disabled: bool,
        desktop_entity: Entity<Self>,
        popover_entity: Entity<PopoverState>,
    ) -> AnyElement {
        Button::new("create-workspace")
            .ghost()
            .xsmall()
            .compact()
            .disabled(selector_disabled)
            .child(div().opacity(0.6).child(IconName::Plus))
            .child(
                div()
                    .opacity(0.6)
                    .child(t!("workspace.action.new").to_string()),
            )
            .on_click({
                let desktop_entity = desktop_entity.clone();
                let popover_entity = popover_entity.clone();
                move |_, window, cx| {
                    let _ = popover_entity.update(cx, |state, cx| {
                        state.dismiss(window, cx);
                    });
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.open_create_workspace_dialog(window, cx);
                    });
                }
            })
            .into_any_element()
    }

    fn render_workspace_popover_option(
        index: usize,
        workspace: &Workspace,
        active_workspace_id: Option<&str>,
        selector_disabled: bool,
        option_style: ButtonCustomVariant,
        selected_style: ButtonCustomVariant,
        active_indicator_color: Hsla,
        inactive_indicator_color: Hsla,
        desktop_entity: Entity<Self>,
        popover_entity: Entity<PopoverState>,
    ) -> AnyElement {
        let workspace_id = workspace.id.clone();
        let workspace_name = workspace_selectors::workspace_display_name(workspace)
            .map(str::to_owned)
            .unwrap_or_else(|| t!("workspace.unnamed").to_string());
        let is_active = active_workspace_id == Some(workspace_id.as_str());
        let style = if is_active {
            selected_style
        } else {
            option_style
        };
        let workspace_id_for_click = workspace_id.clone();

        let select_button = Button::new(("workspace-option", index))
            .custom(style)
            .with_size(px(14.))
            .w_full()
            .justify_start()
            .pr(px(36.))
            .p_2()
            .disabled(selector_disabled)
            .on_click({
                let desktop_entity = desktop_entity.clone();
                let popover_entity = popover_entity.clone();
                move |_, window, cx| {
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.switch_workspace_from_ui(workspace_id_for_click.clone(), cx);
                    });
                    let _ = popover_entity.update(cx, |state, cx| {
                        state.dismiss(window, cx);
                    });
                }
            })
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .gap_3()
                    .when(is_active, |this| {
                        this.child(div().size(px(8.)).rounded_full().bg(active_indicator_color))
                    })
                    .when(!is_active, |this| {
                        this.child(
                            div()
                                .size(px(8.))
                                .rounded_full()
                                .bg(inactive_indicator_color),
                        )
                    })
                    .child(
                        v_flex()
                            .w_full()
                            .min_w_0()
                            .gap_1()
                            .child(
                                div()
                                    .w_full()
                                    .min_w_0()
                                    .text_sm()
                                    .line_height(relative(1.0))
                                    .font_semibold()
                                    .overflow_hidden()
                                    .whitespace_nowrap()
                                    .text_ellipsis()
                                    .child(workspace_name),
                            )
                            .child(
                                div()
                                    .w_full()
                                    .min_w_0()
                                    .text_xs()
                                    .line_height(relative(1.05))
                                    .opacity(0.6)
                                    .overflow_hidden()
                                    .whitespace_nowrap()
                                    .text_ellipsis()
                                    .child(workspace_id.clone()),
                            ),
                    ),
            )
            .into_any_element();

        div()
            .relative()
            .w_full()
            .min_w_0()
            .child(select_button)
            .child(
                div()
                    .absolute()
                    .top_0()
                    .right_0()
                    .bottom_0()
                    .flex()
                    .items_center()
                    .pr_1()
                    .child(
                        Button::new(("workspace-option-rename", index))
                            .ghost()
                            .xsmall()
                            .compact()
                            .disabled(selector_disabled)
                            .icon(PioneerIconName::Bolt)
                            .tooltip(t!("workspace.action.rename").to_string())
                            .on_click({
                                let desktop_entity = desktop_entity.clone();
                                let popover_entity = popover_entity.clone();
                                let workspace_id = workspace_id.clone();
                                move |_, window, cx| {
                                    cx.stop_propagation();
                                    let _ = popover_entity.update(cx, |state, cx| {
                                        state.dismiss(window, cx);
                                    });
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        view.open_rename_workspace_dialog(
                                            workspace_id.clone(),
                                            window,
                                            cx,
                                        );
                                    });
                                }
                            }),
                    ),
            )
            .into_any_element()
    }
}
