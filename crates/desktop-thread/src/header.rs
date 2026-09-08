use crate::{
    assets::PioneerIconName,
    buttons::small_outline_button,
    ports::{ThreadFileOpenerChoice, ThreadFileOpenerPresentation, ThreadPresentationOperation},
};
use gpui_kit::{
    component::{
        button::*,
        menu::{DropdownMenu, PopupMenuItem},
        theme::ActiveTheme,
        *,
    },
    prelude::*,
    *,
};
use pioneer_client::threads::scope::ThreadScopePendingAction;
use pioneer_client::threads::scope::{ThreadStatus, ThreadVisibility};

#[derive(Clone, PartialEq, Action)]
#[action(namespace = thread_header, no_json)]
pub(crate) struct HeaderAction {
    pub operation: ThreadPresentationOperation,
    pub command: HeaderCommand,
}
#[derive(Clone, PartialEq)]
pub(crate) enum HeaderCommand {
    Back,
    Rename,
    Members,
    SetVisibility(ThreadVisibility),
    SetFileOpener(Option<String>),
}

/// Value-like header; its mounted feature region handles all commands.
#[derive(IntoElement)]
pub(crate) struct ThreadHeader {
    pub action_region: FocusHandle,
    pub operation: ThreadPresentationOperation,
    pub title: String,
    pub task_child: bool,
    pub materialized: bool,
    pub can_manage: bool,
    pub visibility: Option<ThreadVisibility>,
    pub status: Option<ThreadStatus>,
    pub connected: bool,
    pub pending: ThreadScopePendingAction,
    pub file_openers: ThreadFileOpenerPresentation,
}

impl ThreadHeader {
    fn file_opener_picker(&self) -> AnyElement {
        if !self.materialized {
            return div().into_any_element();
        }
        let selected = self.file_openers.selected();
        let workspace = self.file_openers.workspace().clone();
        let thread_override = self.file_openers.thread_override().map(str::to_owned);
        let choices = self.file_openers.choices().to_vec();
        let operation = self.operation.clone();
        let action_region = self.action_region.clone();
        small_outline_button("thread-file-opener-trigger")
            .compact()
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .justify_between()
                    .gap_2()
                    .child(file_opener_icon(selected))
                    .child(div().text_sm().child(selected.label().to_owned()))
                    .child(Icon::new(IconName::ChevronsUpDown).size_3p5()),
            )
            .dropdown_menu_with_anchor(Anchor::TopRight, move |menu, _, _| {
                let workspace = workspace.clone();
                let workspace_selected = thread_override.is_none();
                let workspace_operation = operation.clone();
                let workspace_label: SharedString =
                    t!("thread.file_opener.use_workspace").to_string().into();
                let mut menu = menu
                    .min_w(px(220.))
                    .scrollable(true)
                    .item(
                        PopupMenuItem::element(move |_, cx| {
                            file_opener_menu_row(
                                &workspace,
                                workspace_label.clone(),
                                workspace_selected,
                                cx,
                            )
                        })
                        .on_click({
                            let action_region = action_region.clone();
                            move |_, window, cx| {
                                action_region.dispatch_action(
                                    &HeaderAction {
                                        operation: workspace_operation.clone(),
                                        command: HeaderCommand::SetFileOpener(None),
                                    },
                                    window,
                                    cx,
                                )
                            }
                        }),
                    )
                    .separator();
                for choice in &choices {
                    let choice = choice.clone();
                    let selected = thread_override.as_deref() == Some(choice.id());
                    let opener = choice.id().to_owned();
                    let operation = operation.clone();
                    menu = menu.item(
                        PopupMenuItem::element(move |_, cx| {
                            file_opener_menu_row(
                                &choice,
                                choice.label().to_owned().into(),
                                selected,
                                cx,
                            )
                        })
                        .on_click({
                            let action_region = action_region.clone();
                            move |_, window, cx| {
                                action_region.dispatch_action(
                                    &HeaderAction {
                                        operation: operation.clone(),
                                        command: HeaderCommand::SetFileOpener(Some(opener.clone())),
                                    },
                                    window,
                                    cx,
                                )
                            }
                        }),
                    );
                }
                menu
            })
            .into_any_element()
    }
    fn title_menu(&self) -> AnyElement {
        if !self.materialized || self.task_child {
            return div().into_any_element();
        }
        let can_manage = self.can_manage;
        let show_members = self.visibility.is_some();
        let visibility = can_manage
            .then_some(self.visibility)
            .flatten()
            .map(|visibility| match visibility {
                ThreadVisibility::Private => (
                    t!("thread.scope.make_public").to_string(),
                    ThreadVisibility::Workspace,
                    PioneerIconName::Eye,
                ),
                ThreadVisibility::Workspace => (
                    t!("thread.scope.make_private").to_string(),
                    ThreadVisibility::Private,
                    PioneerIconName::EyeOff,
                ),
            });
        let disabled = !self.connected
            || self.status == Some(ThreadStatus::Closed)
            || !matches!(self.pending, ThreadScopePendingAction::Idle);
        let operation = self.operation.clone();
        let action_region = self.action_region.clone();
        Button::new("thread-title-menu")
            .small()
            .ghost()
            .compact()
            .child(Icon::new(IconName::Ellipsis).size_4().opacity(0.65))
            .dropdown_menu_with_anchor(Anchor::TopRight, move |menu, _, _| {
                let mut menu = menu.min_w(px(180.));
                if can_manage {
                    let operation = operation.clone();
                    menu = menu.item(
                        PopupMenuItem::new(t!("sidebar.contextmenu.thread.edit").to_string())
                            .icon(PioneerIconName::Pen)
                            .on_click({
                                let action_region = action_region.clone();
                                move |_, window, cx| {
                                    action_region.dispatch_action(
                                        &HeaderAction {
                                            operation: operation.clone(),
                                            command: HeaderCommand::Rename,
                                        },
                                        window,
                                        cx,
                                    )
                                }
                            }),
                    );
                }
                if show_members {
                    let operation = operation.clone();
                    menu = menu.item(
                        PopupMenuItem::new(t!("settings.sidebar.members").to_string())
                            .icon(PioneerIconName::UserCheck)
                            .on_click({
                                let action_region = action_region.clone();
                                move |_, window, cx| {
                                    action_region.dispatch_action(
                                        &HeaderAction {
                                            operation: operation.clone(),
                                            command: HeaderCommand::Members,
                                        },
                                        window,
                                        cx,
                                    )
                                }
                            }),
                    );
                }
                if let Some((label, target, icon)) = visibility.clone() {
                    let operation = operation.clone();
                    menu = menu.item(
                        PopupMenuItem::new(label)
                            .icon(icon)
                            .disabled(disabled)
                            .on_click({
                                let action_region = action_region.clone();
                                move |_, window, cx| {
                                    action_region.dispatch_action(
                                        &HeaderAction {
                                            operation: operation.clone(),
                                            command: HeaderCommand::SetVisibility(target),
                                        },
                                        window,
                                        cx,
                                    )
                                }
                            }),
                    );
                }
                menu
            })
            .into_any_element()
    }
}
impl RenderOnce for ThreadHeader {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let operation = self.operation.clone();
        let action_region = self.action_region.clone();
        let file_picker = self.file_opener_picker();
        let title_menu = self.title_menu();
        h_flex()
            .justify_between()
            .items_center()
            .pl_6()
            .pr_4()
            .py_2()
            .border_b_1()
            .border_color(cx.theme().border)
            .child(
                h_flex()
                    .min_w_0()
                    .flex_1()
                    .items_center()
                    .gap_1p5()
                    .when(self.task_child, |this| {
                        this.child(
                            Button::new("task-child-thread-back")
                                .small()
                                .ghost()
                                .compact()
                                .p_0()
                                .child(Icon::new(IconName::ChevronLeft).size_4())
                                .on_click({
                                    let action_region = action_region.clone();
                                    move |_, window, cx| {
                                        action_region.dispatch_action(
                                            &HeaderAction {
                                                operation: operation.clone(),
                                                command: HeaderCommand::Back,
                                            },
                                            window,
                                            cx,
                                        )
                                    }
                                }),
                        )
                    })
                    .child(
                        div()
                            .min_w_0()
                            .overflow_hidden()
                            .text_ellipsis()
                            .text_sm()
                            .font_semibold()
                            .child(self.title),
                    ),
            )
            .child(
                h_flex()
                    .items_center()
                    .gap_1()
                    .child(file_picker)
                    .child(title_menu),
            )
    }
}

fn file_opener_icon(opener: &ThreadFileOpenerChoice) -> AnyElement {
    if let Some(path) = opener.logo_path() {
        let path: SharedString = path.to_owned().into();
        if matches!(opener.id(), "cursor" | "zed") {
            Icon::empty().path(path).size_3p5().into_any_element()
        } else {
            img(path).size_3p5().flex_none().into_any_element()
        }
    } else {
        Icon::new(IconName::Folder).size_3p5().into_any_element()
    }
}
fn file_opener_menu_row(
    opener: &ThreadFileOpenerChoice,
    label: SharedString,
    selected: bool,
    cx: &App,
) -> AnyElement {
    let hover_background = cx.theme().accent;
    let selected_background = cx.theme().popover.blend(hover_background.opacity(0.88));
    h_flex()
        .flex_1()
        .h(px(26.))
        .mx_neg_2()
        .px_2()
        .rounded(cx.theme().radius.min(px(8.)))
        .items_center()
        .gap_2()
        .text_sm()
        .when(selected, |row| {
            row.bg(selected_background)
                .text_color(cx.theme().accent_foreground)
                .hover(move |row| row.bg(hover_background))
        })
        .child(file_opener_icon(opener))
        .child(label)
        .into_any_element()
}

mod region;
pub(crate) use region::{HeaderBack, ThreadHeaderView};
