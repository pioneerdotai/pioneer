use crate::sidebar::*;
use gpui_kit::component::{
    button::*,
    list::ListItem,
    menu::ContextMenuExt,
    theme::ActiveTheme,
    tree::{TreeItem, tree},
    *,
};
use gpui_kit::{ClickEvent, prelude::*, *};
use pioneer_client::agents_doc::scope::{self as agents_doc_scope, AgentsDocEditAction};
use pioneer_client::threads::tree::{self as client_thread_tree, SidebarTreeNodeKey};
use pioneer_client::workspaces::{ThreadAgentsDocSummary, ThreadFolder};
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

const TREE_ROW_HEIGHT_PX: f32 = 32.0;
const TREE_ROW_CONTENT_HEIGHT_PX: f32 = 28.0;
const TREE_GUIDE_HEIGHT_PX: f32 = 32.0;
const TREE_ROW_GAP_PX: f32 = 6.0;
const TREE_ROW_CONTENT_PADDING_X_PX: f32 = 8.0;
const TREE_GUIDE_LINE_X_PX: f32 = 14.0;
const SIDEBAR_MENU_ITEM_OPACITY: f32 = 0.8;

#[derive(Clone)]
struct SidebarThreadRow {
    thread_id: String,
    // Resolve the localized fallback while rendering, so locale changes do not
    // require a new directory publication.
    title: Option<String>,
    unread_count: u64,
}

/// One workspace's immutable presentation data. Replaced on publication/owner
/// changes; render callbacks share handles instead of copying whole maps.
#[derive(Default)]
pub(super) struct SidebarPreparation {
    input: Option<Arc<pioneer_client::workspaces::directory::ThreadTreePublication>>,
    workspace_id: Option<String>,
    can_manage_workspace: bool,
    agents_scope: Option<ThreadAgentsDocEditorScope>,
    rows: Arc<HashMap<String, SidebarThreadRow>>,
    folders: Arc<HashMap<String, ThreadFolder>>,
    summaries: Arc<HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary>>,
}

// Stock Spinner animation invalidates only this retained view. Virtualized
// rows that are not painted do not keep the sidebar/tree rendering each frame.
pub(super) struct TaskActivityIndicator;
impl Render for TaskActivityIndicator {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("task-activity")
            .flex_shrink_0()
            .tooltip(|window, cx| {
                tooltip::Tooltip::new(t!("sidebar.taskActivity").to_string()).build(window, cx)
            })
            .child(
                spinner::Spinner::new()
                    .small()
                    .color(cx.theme().muted_foreground),
            )
    }
}

struct SidebarTreeModel {
    items: Vec<TreeItem>,
    visible_node_ids: Vec<String>,
}

actions!(
    sidebar_folder_menu,
    [
        SidebarFolderRename,
        SidebarFolderDelete,
        SidebarFolderEditAgentsDoc,
        SidebarFolderRemoveAgentsDoc
    ]
);
actions!(
    sidebar_root_menu,
    [SidebarRootEditAgentsDoc, SidebarRootRemoveAgentsDoc]
);
actions!(sidebar_thread_menu, [SidebarThreadRename]);

/// Effective row inputs only: publication identity and unused summary fields are
/// deliberately absent. Tree interaction state is never stored here.
#[derive(Clone, PartialEq, Eq)]
struct SidebarRowInputs {
    depth: usize,
    title: Option<String>,
    unread_count: u64,
    can_manage_workspace: bool,
    can_manage_thread: bool,
    agents_doc_edit_action: AgentsDocEditAction,
    agents_doc_active: bool,
    agents_doc_can_remove: bool,
    workspace_id: Option<String>,
    indicator: Option<Entity<TaskActivityIndicator>>,
}

/// Tree owns interaction state; this projection only controls the existing
/// selected styling and disclosure icons inside a retained row. It cannot
/// change Tree selection or expansion and never includes right-click state.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct SidebarTreeRowPresentation {
    selected: bool,
    open: bool,
}

pub(super) struct SidebarRetainedRow {
    view: Entity<SidebarRowView>,
    _lifetime: Rc<()>,
}

#[derive(Clone)]
struct SidebarRowOwner {
    sidebar: WeakEntity<ThreadSidebarView>,
    lifetime: std::rc::Weak<()>,
    node_id: String,
}

fn sidebar_thread_management_allowed(view: &ThreadSidebarView, id: &str) -> bool {
    let allowed = view
        .principal_presentation_capabilities()
        .can_manage_all_threads
        || (view.thread_scope_capabilities_thread_id.as_deref() == Some(id)
            && view.thread_scope_capabilities.can_manage_thread);
    allowed && view.can_manage_thread_presentation(id)
}

impl SidebarRowOwner {
    fn read<R>(&self, cx: &App, read: impl FnOnce(&ThreadSidebarView, &App) -> R) -> Option<R> {
        self.lifetime.upgrade()?;
        let sidebar = self.sidebar.upgrade()?;
        let view = sidebar.read(cx);
        view.sidebar_row_owner_is_current().then(|| read(view, cx))
    }

    fn listener<E: 'static>(
        &self,
        manage: bool,
        listener: impl Fn(&mut ThreadSidebarView, &E, &mut Window, &mut Context<ThreadSidebarView>)
        + 'static,
    ) -> Box<dyn Fn(&E, &mut Window, &mut App) + 'static> {
        let owner = self.clone();
        Box::new(move |event, window, cx| {
            if owner.lifetime.upgrade().is_none() {
                return;
            }
            let node_id = owner.node_id.clone();
            let _ = owner.sidebar.update(cx, |view, cx| {
                // Events from an old retained frame must not cross workspace,
                // authorization-owner, deletion, or permission boundaries.
                if !view.sidebar_row_owner_is_current() {
                    return;
                }
                let allowed = match parse_sidebar_tree_node_key(&node_id) {
                    SidebarTreeNodeKey::Thread(id) if manage => {
                        sidebar_thread_management_allowed(view, id)
                    }
                    SidebarTreeNodeKey::AgentsDocRoot | SidebarTreeNodeKey::AgentsDocFolder(_) => {
                        view.principal_presentation_capabilities()
                            .can_manage_workspace
                    }
                    _ if manage => {
                        view.principal_presentation_capabilities()
                            .can_manage_workspace
                    }
                    _ => true,
                };
                if allowed {
                    listener(view, event, window, cx);
                }
            });
        })
    }
}

#[cfg(test)]
#[derive(Default)]
struct SidebarRowRenderProbe {
    builds: std::cell::Cell<usize>,
    // Recorded at the actual thread title / unread child construction sites.
    thread: std::cell::RefCell<Option<(String, Option<u64>, bool)>>,
}

struct SidebarRowView {
    node_id: String,
    inputs: SidebarRowInputs,
    owner: SidebarRowOwner,
    tree_presentation: SidebarTreeRowPresentation,
    #[cfg(test)]
    render_probe: Rc<SidebarRowRenderProbe>,
}

impl Render for SidebarRowView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        #[cfg(test)]
        self.render_probe
            .builds
            .set(self.render_probe.builds.get() + 1);
        let item_id = self.node_id.as_str();
        let selected = self.tree_presentation.selected;
        let can_manage_workspace = self.inputs.can_manage_workspace;
        let root_agents_doc_active = self.inputs.agents_doc_active;
        let root_agents_doc_edit_menu_label = self.inputs.agents_doc_edit_action;
        match parse_sidebar_tree_node_key(item_id) {
            SidebarTreeNodeKey::ThreadsHeader => {
                let create_folder_listener =
                    self.owner
                        .listener(true, move |view, _: &ClickEvent, _window, cx| {
                            view.create_folder_from_sidebar(cx);
                            cx.notify();
                        });
                let root_edit_agents_doc_action_listener = self.owner.listener(
                    true,
                    move |view, _: &SidebarRootEditAgentsDoc, window, cx| {
                        view.open_root_agents_doc_editor_from_sidebar(window, cx);
                        cx.notify();
                    },
                );
                let root_remove_agents_doc_action_listener = self.owner.listener(
                    true,
                    move |view, _: &SidebarRootRemoveAgentsDoc, _window, cx| {
                        view.remove_root_agents_doc_override_from_sidebar(cx);
                        cx.notify();
                    },
                );

                h_flex()
                    .id(format!("thread-tree-root-{item_id}"))
                    .w_full()
                    .h(px(TREE_ROW_HEIGHT_PX))
                    .justify_between()
                    .items_center()
                    .when(can_manage_workspace, |this| {
                        this.on_action(root_edit_agents_doc_action_listener)
                            .on_action(root_remove_agents_doc_action_listener)
                    })
                    .context_menu({
                        let owner = self.owner.clone();
                        move |menu, _, cx| {
                            let can_manage_workspace = owner
                                .read(cx, |view, _| {
                                    view.principal_presentation_capabilities()
                                        .can_manage_workspace
                                })
                                .unwrap_or(false);
                            if !can_manage_workspace {
                                return menu;
                            }
                            let menu = menu.menu(
                                match root_agents_doc_edit_menu_label {
                                    AgentsDocEditAction::Create => {
                                        t!("sidebar.contextmenu.folder.create_agents_doc")
                                    }
                                    AgentsDocEditAction::Edit => {
                                        t!("sidebar.contextmenu.folder.edit_agents_doc")
                                    }
                                },
                                Box::new(SidebarRootEditAgentsDoc),
                            );
                            if root_agents_doc_active {
                                menu.menu(
                                    t!("sidebar.contextmenu.folder.remove_agents_doc_override"),
                                    Box::new(SidebarRootRemoveAgentsDoc),
                                )
                            } else {
                                menu
                            }
                        }
                    })
                    .child(
                        h_flex()
                            .pl_4()
                            .text_xs()
                            .font_medium()
                            .opacity(0.6)
                            .child(t!("sidebar.title.threads").to_string()),
                    )
                    .when(can_manage_workspace, |this| {
                        this.child(
                            h_flex().pr_2().items_center().child(
                                Button::new("create-thread-folder")
                                    .small()
                                    .ghost()
                                    .compact()
                                    .child(
                                        Icon::empty()
                                            .path("icons/folder-plus.svg")
                                            .size_4()
                                            .opacity(0.6),
                                    )
                                    .on_click(create_folder_listener),
                            ),
                        )
                    })
                    .into_any_element()
            }
            SidebarTreeNodeKey::Thread(thread_id) => {
                let row = SidebarThreadRow {
                    thread_id: thread_id.to_owned(),
                    title: self.inputs.title.clone(),
                    unread_count: self.inputs.unread_count,
                };
                let title = row
                    .title
                    .clone()
                    .unwrap_or_else(|| t!("sidebar.thread.untitled").to_string());
                let can_manage_thread = self.inputs.can_manage_thread;
                let thread_id_for_menu = row.thread_id.clone();
                let thread_id_for_click = row.thread_id.clone();
                let thread_id_for_context_menu_rename = row.thread_id.clone();
                let thread_payload = SidebarTreeDragPayload {
                    label: title.clone(),
                    item: SidebarTreeDragItem::Thread {
                        thread_id: row.thread_id.clone(),
                    },
                };
                let open_listener =
                    self.owner
                        .listener(false, move |view, _: &ClickEvent, window, cx| {
                            view.set_thread_tree_selected_node_id(Some(thread_node_key(
                                thread_id_for_click.as_str(),
                            )));
                            view.open_thread_from_sidebar(thread_id_for_click.clone(), window, cx);
                            cx.notify();
                        });
                let thread_rename_action_listener =
                    self.owner
                        .listener(true, move |view, _: &SidebarThreadRename, window, cx| {
                            view.open_rename_thread_dialog(
                                thread_id_for_context_menu_rename.clone(),
                                window,
                                cx,
                            );
                            cx.notify();
                        });

                div()
                    .id(format!("thread-tree-thread-drag-{item_id}"))
                    .w_full()
                    .h(px(TREE_ROW_HEIGHT_PX))
                    .on_click(open_listener)
                    .when(can_manage_thread, |this| {
                        this.on_action(thread_rename_action_listener)
                    })
                    .when(can_manage_workspace, |this| {
                        this.on_drag(thread_payload, |drag, _, _, cx| {
                            cx.new(|_| crate::drag_preview::SidebarDragSession(drag.clone()))
                        })
                    })
                    .context_menu({
                        let owner = self.owner.clone();
                        move |menu, _, cx| {
                            let can_manage_thread = owner
                                .read(cx, |view, _| {
                                    sidebar_thread_management_allowed(
                                        view,
                                        thread_id_for_menu.as_str(),
                                    )
                                })
                                .unwrap_or(false);
                            if can_manage_thread {
                                menu.menu(
                                    t!("sidebar.contextmenu.thread.edit"),
                                    Box::new(SidebarThreadRename),
                                )
                            } else {
                                menu
                            }
                        }
                    })
                    .child(
                        h_flex()
                            .w_full()
                            .h(px(TREE_ROW_HEIGHT_PX))
                            .min_w_0()
                            .items_center()
                            .child(tree_depth_guides(self.inputs.depth, cx))
                            .child(
                                h_flex()
                                    // Newer GPUI versions only schedule an immediate
                                    // hover repaint for elements with persistent state.
                                    .id(format!("thread-tree-thread-hover-{item_id}"))
                                    .w_full()
                                    .min_w_0()
                                    .h(px(TREE_ROW_CONTENT_HEIGHT_PX))
                                    .px(px(TREE_ROW_CONTENT_PADDING_X_PX))
                                    .items_center()
                                    .gap(px(TREE_ROW_GAP_PX))
                                    .rounded_md()
                                    .hover(|this| this.bg(cx.theme().sidebar_accent))
                                    .when(selected, |this| this.bg(cx.theme().sidebar_accent))
                                    .child(
                                        div()
                                            .flex_1()
                                            .min_w_0()
                                            .overflow_hidden()
                                            .text_ellipsis()
                                            .text_sm()
                                            .text_color(cx.theme().foreground)
                                            .line_height(relative(1.0))
                                            .font_normal()
                                            .opacity(SIDEBAR_MENU_ITEM_OPACITY)
                                            .when(selected, |this| this.opacity(1.0))
                                            .child({
                                                #[cfg(test)]
                                                {
                                                    *self.render_probe.thread.borrow_mut() =
                                                        Some((title.clone(), None, selected));
                                                }
                                                title
                                            }),
                                    )
                                    .when(
                                        should_render_thread_unread_badge(
                                            row.unread_count,
                                            selected,
                                        ),
                                        |this| {
                                            #[cfg(test)]
                                            {
                                                self.render_probe
                                                    .thread
                                                    .borrow_mut()
                                                    .as_mut()
                                                    .unwrap()
                                                    .1 = Some(row.unread_count);
                                            }
                                            this.child(
                                                div()
                                                    .flex_shrink_0()
                                                    .min_w_5()
                                                    .h_5()
                                                    .px_1()
                                                    .rounded_full()
                                                    .flex()
                                                    .items_center()
                                                    .justify_center()
                                                    .text_xs()
                                                    .bg(cx.theme().muted)
                                                    .child(
                                                        div()
                                                            .opacity(0.8)
                                                            .child(row.unread_count.to_string()),
                                                    ),
                                            )
                                        },
                                    )
                                    .when_some(self.inputs.indicator.clone(), |this, indicator| {
                                        this.child(indicator)
                                    }),
                            ),
                    )
                    .into_any_element()
            }
            SidebarTreeNodeKey::AgentsDocRoot => {
                let workspace_id = self.inputs.workspace_id.clone();
                let open_listener =
                    self.owner
                        .listener(false, move |view, _: &ClickEvent, window, cx| {
                            let Some(workspace_id) = workspace_id.clone() else {
                                return;
                            };
                            view.open_agents_doc_editor(
                                ThreadAgentsDocEditorScope::Root { workspace_id },
                                window,
                                cx,
                            );
                            cx.notify();
                        });
                render_agents_doc_file_row(item_id, self.inputs.depth, selected, open_listener, cx)
            }
            SidebarTreeNodeKey::Folder(folder_id) => {
                let unread_count = self.inputs.unread_count;
                let folder_name = self
                    .inputs
                    .title
                    .clone()
                    .unwrap_or_else(|| folder_id.to_owned());
                let agents_doc_edit_menu_label = self.inputs.agents_doc_edit_action;
                let agents_doc_can_remove = self.inputs.agents_doc_can_remove;
                let folder_id_for_move = folder_id.to_owned();
                let folder_id_for_click = folder_id.to_owned();
                let folder_id_for_context_menu_rename = folder_id.to_owned();
                let folder_id_for_context_menu_delete = folder_id.to_owned();
                let folder_id_for_context_menu_edit_agents_doc = folder_id.to_owned();
                let folder_id_for_context_menu_remove_agents_doc = folder_id.to_owned();
                let folder_payload = SidebarTreeDragPayload {
                    label: folder_name.clone(),
                    item: SidebarTreeDragItem::Folder {
                        folder_id: folder_id.to_owned(),
                    },
                };
                let drop_listener = self.owner.listener(
                    true,
                    move |view, payload: &SidebarTreeDragPayload, _window, cx| {
                        view.handle_sidebar_drop_to_folder(
                            payload.clone(),
                            folder_id_for_move.clone(),
                            cx,
                        );
                        cx.notify();
                    },
                );
                let folder_click_listener =
                    self.owner
                        .listener(false, move |view, _: &ClickEvent, _window, cx| {
                            // Tree owns disclosure and publishes it through
                            // TreeEvent. This handler only selects the domain node.
                            view.set_thread_tree_selected_node_id(Some(folder_node_key(
                                folder_id_for_click.as_str(),
                            )));
                            cx.notify();
                        });
                let folder_rename_action_listener =
                    self.owner
                        .listener(true, move |view, _: &SidebarFolderRename, window, cx| {
                            view.open_rename_folder_dialog(
                                folder_id_for_context_menu_rename.clone(),
                                window,
                                cx,
                            );
                            cx.notify();
                        });
                let folder_delete_action_listener =
                    self.owner
                        .listener(true, move |view, _: &SidebarFolderDelete, _window, cx| {
                            view.delete_folder_from_sidebar(
                                folder_id_for_context_menu_delete.clone(),
                                cx,
                            );
                            cx.notify();
                        });
                let folder_edit_agents_doc_action_listener = self.owner.listener(
                    true,
                    move |view, _: &SidebarFolderEditAgentsDoc, window, cx| {
                        view.open_folder_agents_doc_editor_from_sidebar(
                            folder_id_for_context_menu_edit_agents_doc.clone(),
                            window,
                            cx,
                        );
                        cx.notify();
                    },
                );
                let folder_remove_agents_doc_action_listener = self.owner.listener(
                    true,
                    move |view, _: &SidebarFolderRemoveAgentsDoc, _window, cx| {
                        view.remove_folder_agents_doc_override_from_sidebar(
                            folder_id_for_context_menu_remove_agents_doc.clone(),
                            cx,
                        );
                        cx.notify();
                    },
                );

                let folder_icon = if self.tree_presentation.open {
                    IconName::FolderOpen
                } else {
                    IconName::FolderClosed
                };
                let disclosure_icon = if self.tree_presentation.open {
                    IconName::ChevronDown
                } else {
                    IconName::ChevronRight
                };

                div()
                    .id(format!("thread-tree-folder-drag-{item_id}"))
                    .on_click(folder_click_listener)
                    .when(can_manage_workspace, |this| {
                        this.on_action(folder_rename_action_listener)
                            .on_action(folder_delete_action_listener)
                            .on_action(folder_edit_agents_doc_action_listener)
                            .on_action(folder_remove_agents_doc_action_listener)
                            .on_drag(folder_payload, |drag, _, _, cx| {
                                cx.new(|_| crate::drag_preview::SidebarDragSession(drag.clone()))
                            })
                            .can_drop({
                                let folder_id = folder_id.to_owned();
                                let owner = self.owner.clone();
                                move |value, _, cx| {
                                    owner
                                        .read(cx, |view, _| {
                                            view.principal_presentation_capabilities()
                                                .can_manage_workspace
                                                && can_drop_on_folder(
                                                    value,
                                                    view.active_workspace_id(),
                                                    view.prepared_sidebar.folders.as_ref(),
                                                    &folder_id,
                                                )
                                        })
                                        .unwrap_or(false)
                                }
                            })
                            .drag_over::<SidebarTreeDragPayload>(|style, _, _, cx| {
                                style.rounded_md().bg(cx.theme().sidebar_accent)
                            })
                            .on_drop(drop_listener)
                    })
                    .context_menu({
                        let owner = self.owner.clone();
                        move |menu, _, cx| {
                            let can_manage_workspace = owner
                                .read(cx, |view, _| {
                                    view.principal_presentation_capabilities()
                                        .can_manage_workspace
                                })
                                .unwrap_or(false);
                            if !can_manage_workspace {
                                return menu;
                            }
                            let menu = menu
                                .menu(
                                    t!("sidebar.contextmenu.folder.rename"),
                                    Box::new(SidebarFolderRename),
                                )
                                .menu(
                                    t!("sidebar.contextmenu.folder.delete"),
                                    Box::new(SidebarFolderDelete),
                                )
                                .separator()
                                .menu(
                                    match agents_doc_edit_menu_label {
                                        AgentsDocEditAction::Create => {
                                            t!("sidebar.contextmenu.folder.create_agents_doc")
                                        }
                                        AgentsDocEditAction::Edit => {
                                            t!("sidebar.contextmenu.folder.edit_agents_doc")
                                        }
                                    },
                                    Box::new(SidebarFolderEditAgentsDoc),
                                );

                            if agents_doc_can_remove {
                                menu.menu(
                                    t!("sidebar.contextmenu.folder.remove_agents_doc_override"),
                                    Box::new(SidebarFolderRemoveAgentsDoc),
                                )
                            } else {
                                menu
                            }
                        }
                    })
                    .w_full()
                    .h(px(TREE_ROW_HEIGHT_PX))
                    .child(
                        h_flex()
                            .w_full()
                            .h(px(TREE_ROW_HEIGHT_PX))
                            .items_center()
                            .child(tree_depth_guides(self.inputs.depth, cx))
                            .child(
                                h_flex()
                                    .id(format!("thread-tree-folder-hover-{item_id}"))
                                    .flex_1()
                                    .min_w_0()
                                    .h(px(TREE_ROW_CONTENT_HEIGHT_PX))
                                    .items_center()
                                    .gap(px(TREE_ROW_GAP_PX))
                                    .px(px(TREE_ROW_CONTENT_PADDING_X_PX))
                                    .rounded_md()
                                    .hover(|this| this.bg(cx.theme().sidebar_accent))
                                    .when(selected, |this| this.bg(cx.theme().sidebar_accent))
                                    .child(
                                        Icon::new(disclosure_icon)
                                            .size_3p5()
                                            .ml_neg_px()
                                            .opacity(SIDEBAR_MENU_ITEM_OPACITY)
                                            .text_color(cx.theme().foreground),
                                    )
                                    .child(
                                        Icon::new(folder_icon)
                                            .size_3p5()
                                            .opacity(SIDEBAR_MENU_ITEM_OPACITY)
                                            .text_color(cx.theme().foreground),
                                    )
                                    .child(
                                        div()
                                            .flex_1()
                                            .min_w_0()
                                            .overflow_hidden()
                                            .text_ellipsis()
                                            .text_sm()
                                            .text_color(cx.theme().foreground)
                                            .line_height(relative(1.0))
                                            .font_normal()
                                            .opacity(SIDEBAR_MENU_ITEM_OPACITY)
                                            .child(folder_name),
                                    )
                                    .when(unread_count > 0, |this| {
                                        this.child(
                                            div()
                                                .min_w_5()
                                                .h_5()
                                                .px_1()
                                                .rounded_full()
                                                .flex()
                                                .flex_shrink_0()
                                                .items_center()
                                                .justify_center()
                                                .text_xs()
                                                .bg(cx.theme().muted)
                                                .child(
                                                    div()
                                                        .opacity(0.8)
                                                        .child(unread_count.to_string()),
                                                ),
                                        )
                                    })
                                    .when_some(self.inputs.indicator.clone(), |this, indicator| {
                                        this.child(indicator)
                                    }),
                            ),
                    )
                    .into_any_element()
            }
            SidebarTreeNodeKey::AgentsDocFolder(folder_id) => {
                let workspace_id = self.inputs.workspace_id.clone();
                let folder_id_for_open = folder_id.to_owned();
                let open_listener =
                    self.owner
                        .listener(false, move |view, _: &ClickEvent, window, cx| {
                            let Some(workspace_id) = workspace_id.clone() else {
                                return;
                            };
                            view.open_agents_doc_editor(
                                ThreadAgentsDocEditorScope::Folder {
                                    workspace_id,
                                    folder_id: folder_id_for_open.clone(),
                                },
                                window,
                                cx,
                            );
                            cx.notify();
                        });
                render_agents_doc_file_row(item_id, self.inputs.depth, selected, open_listener, cx)
            }
            SidebarTreeNodeKey::Unknown => div().into_any_element(),
        }
    }
}

impl ThreadSidebarView {
    pub(super) fn refresh_sidebar_tree_state(
        &mut self,
        force_structure: bool,
        selection_changed: bool,
        cx: &mut Context<Self>,
    ) -> bool {
        let workspace_changed =
            self.prepared_sidebar.workspace_id.as_deref() != self.active_workspace_id();
        let input_changed = workspace_changed
            || match (&self.prepared_sidebar.input, &self.input) {
                (Some(before), Some(after)) => !Arc::ptr_eq(before, after),
                (None, None) => false,
                _ => true,
            };
        let can_manage_workspace = self
            .principal_presentation_capabilities()
            .can_manage_workspace;
        let structure_changed = force_structure
            || workspace_changed
            || self.prepared_sidebar.can_manage_workspace != can_manage_workspace
            || self.prepared_sidebar.agents_scope != self.active_agents_doc_editor_scope
            || (input_changed
                && sidebar_tree_source_changed(
                    self.prepared_sidebar.input.as_deref(),
                    self.input.as_deref(),
                    can_manage_workspace,
                ));
        if input_changed {
            self.prepare_sidebar_rows();
            self.refresh_task_activity_indicators(cx);
        }
        self.prepared_sidebar.input = self.input.clone();
        self.prepared_sidebar.workspace_id = self.active_workspace_id().map(str::to_owned);
        self.prepared_sidebar.can_manage_workspace = can_manage_workspace;
        self.prepared_sidebar.agents_scope = self.active_agents_doc_editor_scope.clone();
        if structure_changed {
            let model = self.build_sidebar_tree_model();
            self.visible_node_count = model.visible_node_ids.len();
            let mut structure = Vec::new();
            tree_structure(&model.items, 0, &mut structure);
            if force_structure || structure != self.tree_structure {
                self.tree_structure = structure;
                self.thread_tree_state.update(cx, |state, cx| {
                    // set_items clears selection/context targeting. Metadata-only
                    // publications must keep the retained tree untouched.
                    state.set_items(model.items, cx);
                });
                self.sync_sidebar_tree_selection(cx);
            } else if selection_changed {
                self.sync_sidebar_tree_selection(cx);
            }
        } else if selection_changed {
            self.sync_sidebar_tree_selection(cx);
        }
        if input_changed || structure_changed || selection_changed {
            self.refresh_sidebar_row_views(cx);
        }
        input_changed || structure_changed
    }

    pub(super) fn refresh_sidebar_row_views(&mut self, cx: &mut Context<Self>) {
        let live_ids = self
            .tree_structure
            .iter()
            .map(|(id, ..)| id.as_str())
            .collect::<std::collections::HashSet<_>>();
        self.retained_sidebar_rows
            .retain(|id, _| live_ids.contains(id.as_str()));
        let capabilities = self.principal_presentation_capabilities();
        for (node_id, depth, _, _) in &self.tree_structure {
            let key = parse_sidebar_tree_node_key(node_id);
            let mut inputs = SidebarRowInputs {
                depth: *depth,
                title: None,
                unread_count: 0,
                can_manage_workspace: false,
                can_manage_thread: false,
                agents_doc_edit_action: AgentsDocEditAction::Create,
                agents_doc_active: false,
                agents_doc_can_remove: false,
                workspace_id: None,
                indicator: None,
            };
            match key {
                SidebarTreeNodeKey::ThreadsHeader => {
                    inputs.can_manage_workspace = capabilities.can_manage_workspace;
                    if inputs.can_manage_workspace {
                        let summary = self
                            .prepared_sidebar
                            .summaries
                            .get(&ThreadAgentsDocSummaryKey::Root);
                        inputs.agents_doc_edit_action =
                            agents_doc_scope::agents_doc_edit_action(summary);
                        inputs.agents_doc_active =
                            agents_doc_scope::agents_doc_has_active_explicit_badge(summary);
                    }
                }
                SidebarTreeNodeKey::Thread(id) => {
                    if let Some(row) = self.prepared_sidebar.rows.get(id) {
                        inputs.title = row.title.clone();
                        inputs.unread_count = row.unread_count;
                    } else {
                        inputs.title = Some(id.to_owned());
                    }
                    inputs.can_manage_workspace = capabilities.can_manage_workspace;
                    inputs.can_manage_thread = capabilities.can_manage_all_threads
                        || (self.thread_scope_capabilities_thread_id.as_deref() == Some(id)
                            && self.thread_scope_capabilities.can_manage_thread);
                    inputs.indicator = self.task_activity_indicators.get(node_id).cloned();
                }
                SidebarTreeNodeKey::Folder(id) => {
                    inputs.title = Some(
                        self.prepared_sidebar
                            .folders
                            .get(id)
                            .map_or_else(|| id.to_owned(), |folder| folder.name.clone()),
                    );
                    inputs.unread_count = self
                        .input
                        .as_ref()
                        .and_then(|input| input.snapshot().unread_by_folder_id.get(id))
                        .copied()
                        .unwrap_or(0);
                    inputs.can_manage_workspace = capabilities.can_manage_workspace;
                    if inputs.can_manage_workspace {
                        let summary = self
                            .prepared_sidebar
                            .summaries
                            .get(&ThreadAgentsDocSummaryKey::Folder(id.to_owned()));
                        inputs.agents_doc_edit_action =
                            agents_doc_scope::agents_doc_edit_action(summary);
                        inputs.agents_doc_can_remove =
                            agents_doc_scope::agents_doc_can_remove_override(summary);
                    }
                    inputs.indicator = self.task_activity_indicators.get(node_id).cloned();
                }
                SidebarTreeNodeKey::AgentsDocRoot => {
                    inputs.workspace_id = self.active_workspace_id().map(str::to_owned);
                }
                SidebarTreeNodeKey::AgentsDocFolder(id) => {
                    inputs.workspace_id = self
                        .prepared_sidebar
                        .folders
                        .get(id)
                        .map(|folder| folder.workspace_id.clone());
                }
                SidebarTreeNodeKey::Unknown => {}
            };
            if let Some(retained) = self.retained_sidebar_rows.get(node_id) {
                if retained.view.read(cx).inputs != inputs {
                    retained.view.update(cx, |view, cx| {
                        view.inputs = inputs;
                        cx.notify();
                    });
                }
            } else {
                let lifetime = Rc::new(());
                let owner = SidebarRowOwner {
                    sidebar: cx.weak_entity(),
                    lifetime: Rc::downgrade(&lifetime),
                    node_id: node_id.clone(),
                };
                let view = cx.new(|_| SidebarRowView {
                    node_id: node_id.clone(),
                    inputs,
                    owner,
                    tree_presentation: Default::default(),
                    #[cfg(test)]
                    render_probe: Rc::default(),
                });
                self.retained_sidebar_rows.insert(
                    node_id.clone(),
                    SidebarRetainedRow {
                        view,
                        _lifetime: lifetime,
                    },
                );
            }
        }
        self.refresh_sidebar_row_tree_presentations(cx);
    }

    /// Also called by the Tree observer, so pointer/keyboard selection and
    /// expansion need no client publication and do not run through render.
    pub(super) fn refresh_sidebar_row_tree_presentations(&mut self, cx: &mut Context<Self>) {
        let state = self.thread_tree_state.read(cx);
        let mut presentations = HashMap::new();
        let mut ix = 0;
        while let Some(entry) = state.entry(ix) {
            presentations.insert(
                entry.item().id.to_string(),
                SidebarTreeRowPresentation {
                    selected: state.selected_index() == Some(ix),
                    open: entry.is_expanded() && entry.is_folder(),
                },
            );
            ix += 1;
        }
        for (id, row) in &self.retained_sidebar_rows {
            let presentation = presentations.get(id).copied().unwrap_or_default();
            if row.view.read(cx).tree_presentation != presentation {
                row.view.update(cx, |view, cx| {
                    view.tree_presentation = presentation;
                    cx.notify();
                });
            }
        }
    }

    fn refresh_task_activity_indicators(&mut self, cx: &mut Context<Self>) {
        let mut active_nodes = std::collections::HashSet::new();
        if let Some(input) = &self.input {
            active_nodes.extend(
                input
                    .snapshot()
                    .running_task_thread_ids
                    .iter()
                    .map(|id| thread_node_key(id)),
            );
            active_nodes.extend(
                input
                    .snapshot()
                    .running_task_folder_ids
                    .iter()
                    .map(|id| client_thread_tree::sidebar_folder_node_id(id)),
            );
        }
        let indicators = Rc::make_mut(&mut self.task_activity_indicators);
        indicators.retain(|id, _| active_nodes.contains(id));
        for id in active_nodes {
            indicators
                .entry(id)
                .or_insert_with(|| cx.new(|_| TaskActivityIndicator));
        }
    }

    fn sync_sidebar_tree_selection(&mut self, cx: &mut Context<Self>) {
        let selected_ix = if matches!(
            self.main_content_view(),
            MainContentView::Threads | MainContentView::AgentsDoc
        ) {
            let selected_node_id = if self.main_content_view() == MainContentView::AgentsDoc {
                self.selected_thread_tree_node_id().map(str::to_owned)
            } else if let Some(navigation) = self.active_task_thread_navigation() {
                Some(thread_node_key(navigation.parent_thread_id()))
            } else {
                self.current_active_thread_id()
                    .map(thread_node_key)
                    .or_else(|| self.selected_thread_tree_node_id().map(str::to_owned))
            };
            selected_node_id
                .and_then(|node_id| self.thread_tree_state.read(cx).index_of(&node_id.into()))
        } else {
            None
        };
        self.thread_tree_state.update(cx, |state, cx| {
            if state.selected_index() != selected_ix {
                state.set_selected_index(selected_ix, cx);
            }
        });
    }

    pub(super) fn update_visible_node_count(&mut self) {
        let mut collapsed_depth = None;
        self.visible_node_count = self
            .tree_structure
            .iter()
            .filter(|(_, depth, expanded, _)| {
                if collapsed_depth.is_some_and(|hidden_depth| *depth > hidden_depth) {
                    return false;
                }
                collapsed_depth = (!expanded).then_some(*depth);
                true
            })
            .count();
    }

    pub(crate) fn render_sidebar(&self, cx: &mut Context<Self>) -> AnyElement {
        let principal_capabilities = self.principal_presentation_capabilities();
        let can_manage_workspace = principal_capabilities.can_manage_workspace;
        let agents_doc_summaries = self.prepared_sidebar.summaries.clone();
        let root_agents_doc_summary = agents_doc_summaries.get(&ThreadAgentsDocSummaryKey::Root);
        let root_agents_doc_active =
            agents_doc_scope::agents_doc_has_active_explicit_badge(root_agents_doc_summary);
        let root_agents_doc_edit_menu_label =
            agents_doc_scope::agents_doc_edit_action(root_agents_doc_summary);
        let root_area_agents_doc_active = root_agents_doc_active;
        let root_area_agents_doc_edit_menu_label = root_agents_doc_edit_menu_label;
        let root_context_area_top_px = self.visible_node_count as f32 * TREE_ROW_HEIGHT_PX;
        let tree_state = self.thread_tree_state().clone();
        let is_new_thread_active = self.main_content_view() == MainContentView::Threads
            && match (
                self.current_active_thread_id(),
                self.draft_thread_id().as_deref(),
            ) {
                (Some(active_thread_id), Some(draft_thread_id)) => {
                    active_thread_id == draft_thread_id
                }
                (None, _) => true,
                _ => false,
            };

        // The callback only returns a retained handle. Tree still applies
        // selection/right-click/disabled state to its ListItem shell.
        let row_owner = cx.weak_entity();
        let tree_view = tree(&tree_state, move |_ix, entry, _selected, _, cx| {
            let item_id = entry.item().id.as_ref();
            let row = row_owner.upgrade().and_then(|owner| {
                owner
                    .read(cx)
                    .retained_sidebar_rows
                    .get(item_id)
                    .map(|row| row.view.clone())
            });
            ListItem::new(format!("thread-tree-row-{item_id}"))
                .separator()
                .h(px(TREE_ROW_HEIGHT_PX))
                .py_0()
                .map(|item| {
                    if matches!(
                        parse_sidebar_tree_node_key(item_id),
                        SidebarTreeNodeKey::ThreadsHeader
                    ) {
                        item.px_0()
                    } else {
                        item.px_2()
                    }
                })
                .when_some(row, |item, row| item.child(row))
        });

        v_flex()
            .size_full()
            .bg(cx.theme().sidebar)
            .p_0()
            .gap_5()
            .child(
                v_flex().pt_4().px_2().gap_5().child(
                    div()
                        .w_full()
                        .child(self.render_workspaces_popover(cx)),
                ).child(
                    v_flex()
                        .gap_2()
                        .child(
                            Button::new("create-thread-session")
                                .ghost()
                                .justify_start()
                                .px_2()
                                .group("new-agent-btn")
                                .selected(is_new_thread_active)
                                .child({
                                    h_flex()
                                        .w_full()
                                        .items_center()
                                        .justify_start()
                                        .gap_2()
                                        .child({
                                            let icon_bg = cx.theme().foreground.opacity(0.075);
                                            let icon_bg_hover = cx.theme().foreground.opacity(0.1);
                                            div()
                                                .id("new-agent-icon")
                                                .size_6()
                                                .rounded_full()
                                                .bg(icon_bg)
                                                .group_hover("new-agent-btn", move |s| {
                                                    s.bg(icon_bg_hover)
                                                })
                                                .flex()
                                                .items_center()
                                                .justify_center()
                                                .child(
                                                    Icon::new(IconName::Plus)
                                                        .size_4()
                                                        .opacity(SIDEBAR_MENU_ITEM_OPACITY)
                                                        .when(is_new_thread_active, |this| {
                                                            this.opacity(1.0)
                                                        }),
                                                )
                                        })
                                        .child(
                                            div()
                                                .flex_none()
                                                .line_height(relative(1.))
                                                .opacity(SIDEBAR_MENU_ITEM_OPACITY)
                                                .when(is_new_thread_active, |this| {
                                                    this.opacity(1.0)
                                                })
                                                .child(t!("sidebar.action.new_thread").to_string())
                                        )
                                })
                                .on_click(cx.listener(|view, _, window, cx| {
                                    view.open_or_create_new_thread_from_sidebar(window, cx);
                                    cx.notify();
                                })),
                        ),
                ),
            )
            .child(
                div().flex_1().min_h_0().w_full().overflow_hidden().child(
                    div()
                        .size_full()
                        .id("thread-tree-root-drop")
                        .relative()
                        .when(can_manage_workspace, |this| {
                            this.can_drop(can_drop_on_root)
                                .drag_over::<SidebarTreeDragPayload>(|style, _, _, cx| {
                                    style.bg(cx.theme().sidebar)
                                })
                                .on_drop(cx.listener(
                                    |view, payload: &SidebarTreeDragPayload, _, cx| {
                                        view.handle_sidebar_drop_to_root(payload.clone(), cx);
                                        cx.notify();
                                    },
                                ))
                        })
                        .child(tree_view)
                        .when(can_manage_workspace, |this| this.child(
                            div()
                                .id("thread-tree-root-context-area")
                                .absolute()
                                .top(px(root_context_area_top_px))
                                .bottom(px(0.))
                                .left(px(0.))
                                .right(px(0.))
                                .on_action(cx.listener(
                                    |view, _: &SidebarRootEditAgentsDoc, window, cx| {
                                        view.open_root_agents_doc_editor_from_sidebar(window, cx);
                                        cx.notify();
                                    },
                                ))
                                .on_action(cx.listener(
                                    |view, _: &SidebarRootRemoveAgentsDoc, _, cx| {
                                        view.remove_root_agents_doc_override_from_sidebar(cx);
                                        cx.notify();
                                    },
                                ))
                                .context_menu(move |menu, _, _| {
                                    let menu = menu.menu(
                                        match root_area_agents_doc_edit_menu_label {
                                            AgentsDocEditAction::Create => {
                                                t!("sidebar.contextmenu.folder.create_agents_doc")
                                            }
                                            AgentsDocEditAction::Edit => {
                                                t!("sidebar.contextmenu.folder.edit_agents_doc")
                                            }
                                        },
                                        Box::new(SidebarRootEditAgentsDoc),
                                    );
                                    if root_area_agents_doc_active {
                                        menu.menu(
                                            t!(
                                                "sidebar.contextmenu.folder.remove_agents_doc_override"
                                            ),
                                            Box::new(SidebarRootRemoveAgentsDoc),
                                        )
                                    } else {
                                        menu
                                    }
                                }),
                        )),
                ),
            )
            .into_any_element()
    }

    fn prepare_sidebar_rows(&mut self) {
        let Some(input) = &self.input else {
            self.prepared_sidebar.rows = Arc::default();
            self.prepared_sidebar.folders = Arc::default();
            self.prepared_sidebar.summaries = Arc::default();
            return;
        };
        let snapshot = input.snapshot();
        let unread_by_thread_id = snapshot
            .unread
            .iter()
            .map(|entry| (entry.thread_id.as_str(), entry.unread_count))
            .collect::<HashMap<_, _>>();
        self.prepared_sidebar.rows = Arc::new(
            snapshot
                .threads_by_id
                .iter()
                .map(|(id, thread)| {
                    (
                        id.clone(),
                        SidebarThreadRow {
                            thread_id: id.clone(),
                            title: thread_display_title(thread),
                            unread_count: unread_by_thread_id
                                .get(id.as_str())
                                .copied()
                                .unwrap_or(0),
                        },
                    )
                })
                .collect(),
        );
        self.prepared_sidebar.folders = Arc::new(snapshot.folders_by_id.clone());
        self.prepared_sidebar.summaries = Arc::new(self.agents_doc_summaries());
    }

    fn build_sidebar_tree_model(&self) -> SidebarTreeModel {
        let Some(workspace_id) = self.active_workspace_id() else {
            return SidebarTreeModel {
                items: Vec::new(),
                visible_node_ids: Vec::new(),
            };
        };
        let can_manage_workspace = self
            .principal_presentation_capabilities()
            .can_manage_workspace;
        let summaries = &self.prepared_sidebar.summaries;
        let client_model = client_thread_tree::sidebar_tree_model_from_workspace_data(
            client_thread_tree::SidebarTreeSourceData {
                workspace_id,
                folders: self.thread_folders_for_workspace(workspace_id),
                placements: self.thread_placements_for_workspace(workspace_id),
                // The authoritative projection already groups threads in their
                // display order. Order between different folders is irrelevant.
                sorted_thread_ids: self
                    .input
                    .as_ref()
                    .into_iter()
                    .flat_map(|input| input.snapshot().thread_ids_by_folder_id.values())
                    .flatten()
                    .cloned()
                    .collect(),
                agents_doc_summaries: if can_manage_workspace {
                    summaries.values().collect()
                } else {
                    Vec::new()
                },
                active_agents_doc_editor_scope: can_manage_workspace
                    .then_some(self.active_agents_doc_editor_scope.as_ref())
                    .flatten(),
                expanded_folder_ids: self
                    .thread_folder_expanded
                    .iter()
                    .filter_map(|(folder_id, expanded)| expanded.then_some(folder_id.clone()))
                    .collect(),
            },
        );
        let items = client_model
            .items
            .iter()
            .map(gpui_tree_item_from_sidebar_item)
            .collect();
        SidebarTreeModel {
            items,
            visible_node_ids: client_model.visible_node_ids,
        }
    }
}

fn sidebar_tree_source_changed(
    before: Option<&pioneer_client::workspaces::directory::ThreadTreePublication>,
    after: Option<&pioneer_client::workspaces::directory::ThreadTreePublication>,
    can_manage_workspace: bool,
) -> bool {
    let (Some(before), Some(after)) = (before, after) else {
        return before.is_some() != after.is_some();
    };
    let before = before.snapshot();
    let after = after.snapshot();
    before.workspace_id != after.workspace_id
        || before.thread_ids_by_folder_id != after.thread_ids_by_folder_id
        || before.folders_by_id.len() != after.folders_by_id.len()
        || before.folders_by_id.iter().any(|(id, folder)| {
            after.folders_by_id.get(id).is_none_or(|next| {
                folder.name != next.name || folder.parent_folder_id != next.parent_folder_id
            })
        })
        || before.placements_by_thread_id != after.placements_by_thread_id
        || (can_manage_workspace
            && (before.agents_doc_summaries_by_folder_key.len()
                != after.agents_doc_summaries_by_folder_key.len()
                || before
                    .agents_doc_summaries_by_folder_key
                    .keys()
                    .any(|key| !after.agents_doc_summaries_by_folder_key.contains_key(key))))
}

fn should_render_thread_unread_badge(unread_count: u64, selected: bool) -> bool {
    unread_count > 0 && !selected
}

fn gpui_tree_item_from_sidebar_item(item: &client_thread_tree::SidebarTreeItem) -> TreeItem {
    let children = item
        .children
        .iter()
        .map(gpui_tree_item_from_sidebar_item)
        .collect::<Vec<_>>();
    TreeItem::new(item.id.clone(), item.label.clone())
        .children(children)
        .expanded(item.expanded)
        .disabled(item.disabled)
}

fn thread_node_key(thread_id: &str) -> String {
    client_thread_tree::sidebar_thread_node_id(thread_id)
}

fn folder_node_key(folder_id: &str) -> String {
    client_thread_tree::sidebar_folder_node_id(folder_id)
}

fn render_agents_doc_file_row(
    item_id: &str,
    depth: usize,
    selected: bool,
    open_listener: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    cx: &mut App,
) -> AnyElement {
    div()
        .id(format!("thread-tree-agents-doc-{item_id}"))
        .w_full()
        .h(px(TREE_ROW_HEIGHT_PX))
        .on_click(open_listener)
        .child(
            h_flex()
                .w_full()
                .h(px(TREE_ROW_HEIGHT_PX))
                .min_w_0()
                .items_center()
                .child(tree_depth_guides(depth, cx))
                .child(
                    h_flex()
                        .id(format!("thread-tree-agents-doc-hover-{item_id}"))
                        .w_full()
                        .min_w_0()
                        .h(px(TREE_ROW_CONTENT_HEIGHT_PX))
                        .px(px(TREE_ROW_CONTENT_PADDING_X_PX))
                        .items_center()
                        .gap(px(TREE_ROW_GAP_PX))
                        .rounded_md()
                        .hover(|this| this.bg(cx.theme().sidebar_accent))
                        .when(selected, |this| this.bg(cx.theme().sidebar_accent))
                        .child(
                            Icon::new(IconName::File)
                                .size_3p5()
                                .opacity(SIDEBAR_MENU_ITEM_OPACITY)
                                .text_color(cx.theme().foreground),
                        )
                        .child(
                            div()
                                .flex_1()
                                .min_w_0()
                                .overflow_hidden()
                                .text_ellipsis()
                                .text_sm()
                                .text_color(cx.theme().foreground)
                                .line_height(relative(1.0))
                                .font_normal()
                                .opacity(SIDEBAR_MENU_ITEM_OPACITY)
                                .when(selected, |this| this.opacity(1.0))
                                .child("AGENTS.md"),
                        ),
                ),
        )
        .into_any_element()
}

fn tree_depth_guides(depth: usize, cx: &mut App) -> AnyElement {
    let guide_color = cx.theme().border;
    let mut guides = h_flex().flex_none().items_start();

    for _ in 0..depth {
        guides = guides.child(
            div()
                .w(px(1.))
                .h(px(TREE_GUIDE_HEIGHT_PX))
                .relative()
                .ml(px(TREE_GUIDE_LINE_X_PX))
                .mr_2()
                .bg(guide_color),
        );
    }

    guides.into_any_element()
}

fn parse_sidebar_tree_node_key(value: &str) -> SidebarTreeNodeKey<'_> {
    client_thread_tree::parse_sidebar_tree_node_id(value)
}

fn can_drop_on_root(value: &dyn Any, _: &mut Window, _: &mut App) -> bool {
    value.is::<SidebarTreeDragPayload>()
}

fn can_drop_on_folder(
    value: &dyn Any,
    active_workspace_id: Option<&str>,
    folders: &HashMap<String, ThreadFolder>,
    target_folder_id: &str,
) -> bool {
    let Some(payload) = value.downcast_ref::<SidebarTreeDragPayload>() else {
        return false;
    };

    let item = match &payload.item {
        SidebarTreeDragItem::Thread { thread_id } => {
            client_thread_tree::SidebarTreeDragItemRef::Thread {
                thread_id: thread_id.as_str(),
            }
        }
        SidebarTreeDragItem::Folder { folder_id } => {
            client_thread_tree::SidebarTreeDragItemRef::Folder {
                folder_id: folder_id.as_str(),
            }
        }
    };

    client_thread_tree::can_drop_sidebar_tree_item_on_folder(
        folders,
        active_workspace_id,
        item,
        target_folder_id,
    )
}

fn tree_structure(items: &[TreeItem], depth: usize, output: &mut Vec<(String, usize, bool, bool)>) {
    for item in items {
        output.push((
            item.id.to_string(),
            depth,
            item.is_expanded(),
            item.is_disabled(),
        ));
        tree_structure(&item.children, depth + 1, output);
    }
}

#[cfg(test)]
mod tests {
    use super::{ThreadSidebarView, thread_node_key};
    use gpui_kit::{AppContext, TestAppContext};
    use std::sync::Arc;

    #[gpui_kit::test]
    fn indicator_frames_reuse_row_content_and_tree_changes_remain_live(cx: &mut TestAppContext) {
        use gpui_kit::{VisualTestContext, component::Root};
        use pioneer_client::workspaces::{
            directory::ThreadTreePublication, projection::ThreadTreeSnapshot,
        };
        use std::{cell::Cell, rc::Rc};

        cx.update(gpui_kit::init);
        cx.update(|cx| cx.set_reduce_motion(false));
        let core = Arc::new(pioneer_client::core::ClientCore::new());
        let thread = |id: &str, name: &str| {
            serde_json::from_value(serde_json::json!({
                "id":id,"workspace_id":"w","name":name,"preview":"","mode":"Chat",
                "model":"m","model_provider":"p","created_at":1,"updated_at":1,
                "status":"Idle","turns":[]
            }))
            .unwrap()
        };
        core.upsert_thread(thread("a", "Alpha"));
        core.upsert_thread(thread("b", "Beta"));
        core.open_workspace_thread("w".into(), Some("a".into()), None);
        let cursor =
            serde_json::from_value(serde_json::json!({"sort_key":"1","through_turn_id":"turn"}))
                .unwrap();
        core.apply_directory_read("w", "b", &cursor, 4);
        let mut snapshot = core.workspace_tree("w").unwrap().snapshot().clone();
        snapshot.running_task_thread_ids.insert("b".into());
        let publication =
            |revision: u64, snapshot: &ThreadTreeSnapshot| -> Arc<ThreadTreePublication> {
                Arc::new(
                    serde_json::from_value(serde_json::json!({
                        "revision":revision,"snapshot":snapshot,
                        "changes":{"changed":[],"removed":[],"reordered_folders":[]},
                        "loading":false,"error":null
                    }))
                    .unwrap(),
                )
            };
        let initial = publication(1, &snapshot);
        let config = crate::context_tests::config(core.clone(), Default::default());
        let (root, cx) = cx.add_window_view(|window, cx| {
            let sidebar = cx.new(|cx| {
                let mut sidebar = ThreadSidebarView::new(config, cx);
                sidebar.input = Some(initial);
                sidebar.refresh_sidebar_tree_state(false, false, cx);
                sidebar
            });
            Root::new(sidebar, window, cx)
        });
        let sidebar = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<ThreadSidebarView>().unwrap()
        });
        let tree = sidebar.read_with(cx, |sidebar, _| sidebar.thread_tree_state.clone());
        let b = sidebar.read_with(cx, |sidebar, _| {
            sidebar.retained_sidebar_rows[&thread_node_key("b")]
                .view
                .clone()
        });
        let probe = b.read_with(cx, |row, _| row.render_probe.clone());
        assert!(
            probe.builds.get() > 0,
            "exercise mounted content, not just a retained ID"
        );
        assert_eq!(
            *probe.thread.borrow(),
            Some(("Beta".into(), Some(4), false))
        );
        let before = probe.builds.get();
        for _ in 0..3 {
            let callbacks = cx.update(|window, cx| window.simulate_next_frame(cx));
            assert!(
                callbacks > 0,
                "the stock nested Spinner must request its next frame"
            );
            cx.update(|window, cx| window.draw(cx).clear(cx));
            assert_eq!(
                probe.builds.get(),
                before,
                "an indicator frame must not rebuild heavy row content"
            );
        }
        let publish = |revision, snapshot: &ThreadTreeSnapshot, cx: &mut VisualTestContext| {
            sidebar.update(cx, |sidebar, cx| {
                sidebar.input = Some(publication(revision, snapshot));
                if sidebar.refresh_sidebar_tree_state(false, false, cx) {
                    cx.notify();
                }
            });
            cx.update(|window, cx| window.draw(cx).clear(cx));
        };
        // New publication identity and unrelated row metadata are not b's inputs.
        publish(2, &snapshot, cx);
        snapshot
            .threads_by_id
            .insert("a".into(), thread("a", "Other title"));
        publish(3, &snapshot, cx);
        assert_eq!(probe.builds.get(), before);
        snapshot
            .threads_by_id
            .insert("b".into(), thread("b", "Renamed"));
        snapshot
            .unread
            .iter_mut()
            .find(|row| row.thread_id == "b")
            .unwrap()
            .unread_count = 7;
        publish(4, &snapshot, cx);
        assert!(probe.builds.get() > before);
        assert_eq!(
            *probe.thread.borrow(),
            Some(("Renamed".into(), Some(7), false))
        );
        // Selection comes directly from Tree, without a navigation publication.
        for (id, expected_badge, selected) in [("b", None, true), ("a", Some(7), false)] {
            tree.update(cx, |tree, cx| {
                let ix = tree.index_of(&thread_node_key(id).into()).unwrap();
                tree.set_selected_index(Some(ix), cx);
            });
            cx.run_until_parked();
            cx.update(|window, cx| window.draw(cx).clear(cx));
            assert_eq!(
                *probe.thread.borrow(),
                Some(("Renamed".into(), expected_badge, selected))
            );
        }
        let indicator = sidebar.read_with(cx, |sidebar, _| {
            sidebar.task_activity_indicators[&thread_node_key("b")].downgrade()
        });
        snapshot.running_task_thread_ids.clear();
        publish(5, &snapshot, cx);
        assert!(b.read_with(cx, |row, _| row.inputs.indicator.is_none()));
        // Drain the already scheduled frame after unmount. No hidden view may
        // keep requesting stock animation frames after its indicator is gone.
        cx.update(|window, cx| window.simulate_next_frame(cx));
        cx.update(|window, cx| window.draw(cx).clear(cx));
        assert_eq!(cx.update(|window, cx| window.simulate_next_frame(cx)), 0);
        // A saved event is valid now, and must become inert on node deletion.
        let calls = Rc::new(Cell::new(0));
        let owner = b.read_with(cx, |row, _| row.owner.clone());
        let on_event = owner.listener(false, {
            let calls = calls.clone();
            move |_, _: &(), _, _| calls.set(calls.get() + 1)
        });
        cx.update(|window, cx| on_event(&(), window, cx));
        assert_eq!(calls.get(), 1);
        let weak = b.downgrade();
        drop(b);
        snapshot.threads_by_id.remove("b");
        snapshot.unread.retain(|row| row.thread_id != "b");
        for ids in snapshot.thread_ids_by_folder_id.values_mut() {
            ids.retain(|id| id != "b");
        }
        publish(6, &snapshot, cx);
        cx.update(|window, cx| on_event(&(), window, cx));
        assert_eq!(calls.get(), 1, "deleted row events are inert");
        // Retained frame records may keep views until the next frame is retired.
        for _ in 0..2 {
            cx.update(|window, cx| window.draw(cx).clear(cx));
        }
        cx.run_until_parked();
        assert!(weak.upgrade().is_none());
        assert!(indicator.upgrade().is_none());
        let old_a = sidebar.read_with(cx, |sidebar, cx| {
            let row = &sidebar.retained_sidebar_rows[&thread_node_key("a")];
            (row.view.downgrade(), row.view.read(cx).owner.clone())
        });
        // Same workspace ID with a different authorization owner also expires rows.
        core.begin_authorization_epoch(Some(("replacement".into(), 1)));
        core.upsert_thread(thread("a", "Replacement owner"));
        core.open_workspace_thread("w".into(), Some("a".into()), None);
        sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
        cx.update(|window, cx| window.draw(cx).clear(cx));
        assert!(cx.update(|_, cx| old_a.1.read(cx, |_, _| ())).is_none());
        for _ in 0..2 {
            cx.update(|window, cx| window.draw(cx).clear(cx));
        }
        cx.run_until_parked();
        assert!(old_a.0.upgrade().is_none());
        let replacement = sidebar.read_with(cx, |sidebar, _| {
            sidebar.retained_sidebar_rows[&thread_node_key("a")]
                .view
                .downgrade()
        });
        core.open_workspace_thread("other".into(), None, None);
        sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
        for _ in 0..2 {
            cx.update(|window, cx| window.draw(cx).clear(cx));
        }
        cx.run_until_parked();
        assert!(replacement.upgrade().is_none());
        sidebar.update(cx, |sidebar, cx| {
            sidebar.close(cx);
            assert!(sidebar.retained_sidebar_rows.is_empty());
        });
    }

    #[gpui_kit::test]
    fn prepared_rows_follow_unread_metadata_and_owner_without_resetting_selection(
        cx: &mut TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let core = Arc::new(pioneer_client::core::ClientCore::new());
        let thread = |id: &str, name: &str| {
            serde_json::from_value(serde_json::json!({
                "id":id,"workspace_id":"w","name":name,"preview":"","mode":"Chat",
                "model":"m","model_provider":"p","created_at":1,"updated_at":1,
                "status":"Idle","turns":[]
            }))
            .unwrap()
        };
        core.upsert_thread(thread("a", "Alpha"));
        core.upsert_thread(thread("b", "Beta"));
        core.open_workspace_thread("w".into(), Some("a".into()), None);
        let config = crate::context_tests::config(core.clone(), Default::default());
        let sidebar = cx.new(|cx| ThreadSidebarView::new(config, cx));
        let tree = sidebar.read_with(cx, |sidebar, _| sidebar.thread_tree_state.clone());
        let selected = tree.read_with(cx, |tree, _| tree.selected_item().unwrap().id.clone());
        let rows = sidebar.read_with(cx, |sidebar, _| sidebar.prepared_sidebar.rows.clone());
        for _ in 0..3 {
            sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
            assert!(sidebar.read_with(cx, |sidebar, _| Arc::ptr_eq(
                &rows,
                &sidebar.prepared_sidebar.rows
            )));
        }
        let cursor = serde_json::from_value(serde_json::json!({
            "sort_key":"1","through_turn_id":"turn"
        }))
        .unwrap();
        for (id, unread) in [("a", 9), ("b", 4)] {
            core.apply_directory_read("w", id, &cursor, unread);
        }
        sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
        sidebar.read_with(cx, |sidebar, _| {
            assert_eq!(sidebar.prepared_sidebar.rows["a"].unread_count, 9);
            assert_eq!(sidebar.prepared_sidebar.rows["b"].unread_count, 4);
            assert_eq!(sidebar.visible_node_count, 3);
        });
        core.apply_directory_read("w", "a", &cursor, 0);
        core.upsert_thread(thread("a", "Renamed"));
        sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
        sidebar.read_with(cx, |sidebar, _| {
            assert_eq!(sidebar.prepared_sidebar.rows["a"].unread_count, 0);
            assert_eq!(sidebar.prepared_sidebar.rows["b"].unread_count, 4);
            assert_eq!(
                sidebar.prepared_sidebar.rows["a"].title.as_deref(),
                Some("Renamed")
            );
        });
        assert_eq!(
            tree.read_with(cx, |tree, _| tree.selected_item().unwrap().id.clone()),
            selected
        );
        drop(rows);
        let old_rows = sidebar.read_with(cx, |sidebar, _| {
            Arc::downgrade(&sidebar.prepared_sidebar.rows)
        });
        // Replace the gateway owner while reusing the workspace identifier.
        core.begin_authorization_epoch(Some(("replacement".into(), 1)));
        core.upsert_thread(thread("c", "New owner"));
        core.open_workspace_thread("w".into(), Some("c".into()), None);
        sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
        sidebar.read_with(cx, |sidebar, _| {
            assert_eq!(sidebar.prepared_sidebar.rows.len(), 1);
            assert_eq!(
                sidebar.prepared_sidebar.rows["c"].title.as_deref(),
                Some("New owner")
            );
            assert_eq!(sidebar.visible_node_count, 2);
        });
        assert!(old_rows.upgrade().is_none());
        assert!(tree.read_with(cx, |tree, _| {
            tree.index_of(&thread_node_key("a").into()).is_none()
        }));
        // Clearing only the active thread keeps the workspace tree visible.
        core.navigate(
            pioneer_client::navigation::NavigationIntent::SelectWorkspace { workspace_id: None },
            None,
        );
        sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
        assert!(tree.read_with(cx, |tree, _| tree.entry(0).is_none()));
        sidebar.update(cx, |sidebar, cx| {
            sidebar.close(cx);
            assert!(sidebar.prepared_sidebar.rows.is_empty());
            assert!(sidebar.prepared_sidebar.folders.is_empty());
            assert!(sidebar.prepared_sidebar.summaries.is_empty());
            assert_eq!(sidebar.visible_node_count, 0);
        });
    }
}
