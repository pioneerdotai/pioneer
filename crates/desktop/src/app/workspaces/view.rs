use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop},
    assets::PioneerIconName,
};
use gpui::{prelude::*, *};
use gpui_component::{
    Colorize, Disableable, Icon, IconName, Selectable, Sizable, StyledExt,
    button::{Button, ButtonVariants},
    h_flex,
    popover::{Popover, PopoverState},
    separator::Separator,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WorkspaceSelectorInteractionState {
    trigger_disabled: bool,
    actions_disabled: bool,
}

fn workspace_selector_interaction_state(
    workspace_unavailable: bool,
    workspace_action_in_progress: bool,
    composer_context_locked: bool,
) -> WorkspaceSelectorInteractionState {
    let trigger_disabled = workspace_unavailable || workspace_action_in_progress;

    WorkspaceSelectorInteractionState {
        trigger_disabled,
        // A send/voice request owns the active workspace context until its RPC
        // finishes. Keep workspace actions blocked without visually changing
        // the unrelated selector trigger on every composer submission.
        actions_disabled: trigger_disabled || composer_context_locked,
    }
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
        let capabilities = self.principal_presentation_capabilities();
        let can_create_workspace = capabilities.can_create_workspace;
        let can_manage_workspace = capabilities.can_manage_workspace;
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
        let workspace_unavailable = self.gateway.connecting
            || self.gateway.connection_state.is_transitioning()
            || self.gateway.connection_state != GatewayConnectionState::Connected
            || self.gateway.session_refresh_in_flight;
        let selector_interaction = workspace_selector_interaction_state(
            workspace_unavailable,
            self.workspace_action_in_progress(),
            self.desktop_voice_context_locked() || self.composer_upload_in_progress,
        );
        // A user-initiated switch is optimistic: keep showing the newly selected
        // workspace while its server scope is synchronized in the background.
        let show_spinner = self.workspaces_loading();
        let workspace_selector_subtitle = t!("workspace.selector_label").to_string();
        let workspaces_loading = self.workspaces_loading();
        let workspace_error = self.workspaces_error().map(str::to_owned);
        let desktop_entity = cx.entity();
        let active_indicator_color = cx.theme().success;
        let inactive_indicator_color = cx.theme().yellow;
        let option_foreground = cx.theme().foreground;
        let option_muted_background = cx.theme().muted;

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

        Popover::new("workspace-switcher-popover")
            .anchor(Anchor::TopRight)
            .p_0()
            .trigger(WorkspaceSelectorTrigger::new(
                "workspace-switcher-button",
                active_workspace_name.clone(),
                workspace_selector_subtitle.clone(),
                show_spinner,
                selector_interaction.trigger_disabled,
            ))
            .content({
                let active_workspaces = active_workspaces.clone();
                let active_workspace_id = active_workspace_id.clone();
                let workspace_error = workspace_error.clone();
                let desktop_entity = desktop_entity.clone();
                let active_indicator_color = active_indicator_color;
                let inactive_indicator_color = inactive_indicator_color;
                let option_foreground = option_foreground;
                let option_muted_background = option_muted_background;
                let ghost_hover = ghost_hover;
                let ghost_active = ghost_active;
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
                                                selector_interaction.actions_disabled,
                                                can_manage_workspace,
                                                active_indicator_color,
                                                inactive_indicator_color,
                                                option_foreground,
                                                option_muted_background,
                                                ghost_hover,
                                                ghost_active,
                                                desktop_entity.clone(),
                                                popover_entity.clone(),
                                            )
                                        },
                                    ),
                                ),
                            )
                        })
                        .when(can_create_workspace, |this| {
                            this.child(Separator::horizontal()).child(
                                h_flex().p_2().pt_0().justify_start().child(
                                    Self::render_create_workspace_popover_action(
                                        selector_interaction.actions_disabled,
                                        desktop_entity.clone(),
                                        popover_entity.clone(),
                                    ),
                                ),
                            )
                        })
                }
            })
            .into_any_element()
    }

    fn render_create_workspace_popover_action(
        actions_disabled: bool,
        desktop_entity: Entity<Self>,
        popover_entity: Entity<PopoverState>,
    ) -> AnyElement {
        Button::new("create-workspace")
            .ghost()
            .xsmall()
            .compact()
            .disabled(actions_disabled)
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
        actions_disabled: bool,
        can_manage_workspace: bool,
        active_indicator_color: Hsla,
        inactive_indicator_color: Hsla,
        option_foreground: Hsla,
        option_muted_background: Hsla,
        ghost_hover: Hsla,
        ghost_active: Hsla,
        desktop_entity: Entity<Self>,
        popover_entity: Entity<PopoverState>,
    ) -> AnyElement {
        let workspace_id = workspace.id.clone();
        let workspace_name = workspace_selectors::workspace_display_name(workspace)
            .map(str::to_owned)
            .unwrap_or_else(|| t!("workspace.unnamed").to_string());
        let is_active = active_workspace_id == Some(workspace_id.as_str());
        let workspace_id_for_click = workspace_id.clone();

        let select_button = div()
            .id(("workspace-option", index))
            .w_full()
            .min_w_0()
            .cursor_pointer()
            .rounded_lg()
            .when(can_manage_workspace, |this| this.pr(px(36.)))
            .p_2()
            .text_color(option_foreground)
            .when(is_active, |this| this.bg(option_muted_background))
            .hover(move |this| this.bg(ghost_hover))
            .active(move |this| this.bg(ghost_active))
            .when(actions_disabled, |this| this.opacity(0.5))
            .on_mouse_down(MouseButton::Left, |_, window, _| {
                window.prevent_default();
            })
            .on_click({
                let desktop_entity = desktop_entity.clone();
                let popover_entity = popover_entity.clone();
                move |_, window, cx| {
                    if actions_disabled {
                        cx.stop_propagation();
                        return;
                    }
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
                                    .text_color(option_foreground)
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
                                    .text_color(option_foreground)
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
            .when(can_manage_workspace, |this| {
                this.child(
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
                                .disabled(actions_disabled)
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
            })
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::workspace_selector_interaction_state;

    #[test]
    fn composer_context_lock_does_not_change_workspace_trigger_visual_state() {
        let state = workspace_selector_interaction_state(false, false, true);

        assert!(!state.trigger_disabled);
        assert!(state.actions_disabled);
    }

    #[test]
    fn workspace_unavailability_disables_trigger_and_actions() {
        let state = workspace_selector_interaction_state(true, false, false);

        assert!(state.trigger_disabled);
        assert!(state.actions_disabled);
    }

    #[test]
    fn workspace_operation_disables_trigger_and_actions() {
        let state = workspace_selector_interaction_state(false, true, false);

        assert!(state.trigger_disabled);
        assert!(state.actions_disabled);
    }
}
