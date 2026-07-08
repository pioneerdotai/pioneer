use crate::app::{
    root::{
        DesktopUpdateUiState, MainContentView, PioneerDesktop, ThreadAgentsDocEditorScope,
        ThreadAgentsDocSummaryKey,
    },
    sidebar::{SidebarTreeDragItem, SidebarTreeDragPayload},
    thread::{ThreadCoordinator, thread_display_title},
};
use crate::assets::PioneerIconName;
use gpui::{ClickEvent, prelude::*, *};
use gpui_component::{
    button::*,
    list::ListItem,
    menu::ContextMenuExt,
    spinner::Spinner,
    theme::ActiveTheme,
    tree::{TreeItem, tree},
    *,
};
use pioneer_client::agents_doc::scope::{self as agents_doc_scope, AgentsDocEditAction};
use pioneer_client::threads::tree::{self as client_thread_tree, SidebarTreeNodeKey};
use pioneer_protocol::{ThreadAgentsDocSummary, ThreadFolder};
use std::any::Any;
use std::collections::HashMap;
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
    title: String,
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

impl PioneerDesktop {
    pub(in crate::app) fn rebuild_sidebar_tree_state(&mut self, cx: &mut Context<Self>) {
        let model = self.build_sidebar_tree_model();
        let selected_ix = if matches!(
            self.main_content_view,
            MainContentView::Threads | MainContentView::AgentsDoc
        ) {
            let selected_node_id = if self.main_content_view == MainContentView::AgentsDoc {
                self.selected_thread_tree_node_id().map(str::to_owned)
            } else if let Some(navigation) = self.active_task_thread_navigation() {
                Some(thread_node_key(navigation.parent_thread_id.as_str()))
            } else {
                self.current_active_thread_id()
                    .map(thread_node_key)
                    .or_else(|| self.selected_thread_tree_node_id().map(str::to_owned))
            };
            selected_node_id.and_then(|node_id| {
                model
                    .visible_node_ids
                    .iter()
                    .position(|candidate| candidate == &node_id)
            })
        } else {
            None
        };

        let tree_state = self.thread_tree_state.clone();
        tree_state.update(cx, |state, cx| {
            state.set_items(model.items, cx);
            state.set_selected_index(selected_ix, cx);
        });
    }

    pub(crate) fn render_sidebar(&self, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let active_workspace_id = self.active_workspace_id().map(str::to_owned);
        let rows_by_thread_id = self.sidebar_rows_by_thread_id();
        let folders_by_id: Arc<HashMap<String, ThreadFolder>> = Arc::new(
            active_workspace_id
                .as_deref()
                .map(|workspace_id| {
                    self.thread_folders_for_workspace(workspace_id)
                        .into_iter()
                        .map(|folder| (folder.id.clone(), folder.clone()))
                        .collect()
                })
                .unwrap_or_default(),
        );
        let agents_doc_summaries: HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary> =
            active_workspace_id
                .as_deref()
                .map(|workspace_id| {
                    agents_doc_scope::thread_agents_doc_summaries_for_workspace(
                        &self.thread_agents_doc_summaries,
                        workspace_id,
                    )
                    .map(|(key, summary)| (key.clone(), summary.clone()))
                    .collect()
                })
                .unwrap_or_default();
        let root_agents_doc_summary = agents_doc_summaries.get(&ThreadAgentsDocSummaryKey::Root);
        let root_agents_doc_active =
            agents_doc_scope::agents_doc_has_active_explicit_badge(root_agents_doc_summary);
        let root_agents_doc_edit_menu_label =
            agents_doc_scope::agents_doc_edit_action(root_agents_doc_summary);
        let root_area_agents_doc_active = root_agents_doc_active;
        let root_area_agents_doc_edit_menu_label = root_agents_doc_edit_menu_label;
        let root_context_area_top_px =
            self.build_sidebar_tree_model().visible_node_ids.len() as f32 * TREE_ROW_HEIGHT_PX;
        let tree_state = self.thread_tree_state().clone();
        let desktop_update_panel = self.render_desktop_update_sidebar_panel(cx);
        let is_new_thread_active = self.main_content_view == MainContentView::Threads
            && match (self.current_active_thread_id(), self.draft_thread_id()) {
                (Some(active_thread_id), Some(draft_thread_id)) => {
                    active_thread_id == draft_thread_id
                }
                (None, _) => true,
                _ => false,
            };

        let tree_view = tree(&tree_state, move |ix, entry, selected, window, cx| {
            let item_id = entry.item().id.as_ref();
            match parse_sidebar_tree_node_key(item_id) {
                SidebarTreeNodeKey::ThreadsHeader => {
                    let create_folder_listener = window.listener_for(
                        &desktop_entity,
                        move |view, _: &ClickEvent, _window, cx| {
                            view.create_folder_from_sidebar(cx);
                            cx.notify();
                        },
                    );
                    let root_edit_agents_doc_action_listener = window.listener_for(
                        &desktop_entity,
                        move |view, _: &SidebarRootEditAgentsDoc, window, cx| {
                            view.open_root_agents_doc_editor_from_sidebar(window, cx);
                            cx.notify();
                        },
                    );
                    let root_remove_agents_doc_action_listener = window.listener_for(
                        &desktop_entity,
                        move |view, _: &SidebarRootRemoveAgentsDoc, _window, cx| {
                            view.remove_root_agents_doc_override_from_sidebar(cx);
                            cx.notify();
                        },
                    );

                    ListItem::new(("thread-tree-row", ix))
                        .separator()
                        .h(px(TREE_ROW_HEIGHT_PX))
                        .px_0()
                        .py_0()
                        .child(
                            h_flex()
                                .id(("thread-tree-root", ix))
                                .w_full()
                                .h(px(TREE_ROW_HEIGHT_PX))
                                .justify_between()
                                .items_center()
                                .on_action(root_edit_agents_doc_action_listener)
                                .on_action(root_remove_agents_doc_action_listener)
                                .context_menu(move |menu, _, _| {
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
                                            t!(
                                                "sidebar.contextmenu.folder.remove_agents_doc_override"
                                            ),
                                            Box::new(SidebarRootRemoveAgentsDoc),
                                        )
                                    } else {
                                        menu
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
                                .child(
                                    h_flex().pr_2().items_center().child(
                                        Button::new("create-thread-folder")
                                            .small()
                                            .ghost()
                                            .compact()
                                            .child(
                                                Icon::new(PioneerIconName::FolderPlus)
                                                    .size_4()
                                                    .opacity(0.6),
                                            )
                                            .on_click(create_folder_listener),
                                    ),
                                ),
                        )
                }
                SidebarTreeNodeKey::Thread(thread_id) => {
                    let row = rows_by_thread_id
                        .get(thread_id)
                        .cloned()
                        .unwrap_or_else(|| SidebarThreadRow {
                            thread_id: thread_id.to_owned(),
                            title: thread_id.to_owned(),
                        });
                    let thread_id_for_click = row.thread_id.clone();
                    let thread_id_for_context_menu_rename = row.thread_id.clone();
                    let thread_payload = SidebarTreeDragPayload {
                        label: row.title.clone(),
                        item: SidebarTreeDragItem::Thread {
                            thread_id: row.thread_id.clone(),
                        },
                    };
                    let open_listener = window.listener_for(
                        &desktop_entity,
                        move |view, _: &ClickEvent, window, cx| {
                            view.set_thread_tree_selected_node_id(Some(thread_node_key(
                                thread_id_for_click.as_str(),
                            )));
                            view.open_thread_from_sidebar(thread_id_for_click.clone(), window, cx);
                            cx.notify();
                        },
                    );
                    let thread_rename_action_listener = window.listener_for(
                        &desktop_entity,
                        move |view, _: &SidebarThreadRename, window, cx| {
                            view.open_rename_thread_dialog(
                                thread_id_for_context_menu_rename.clone(),
                                window,
                                cx,
                            );
                            cx.notify();
                        },
                    );

                    ListItem::new(("thread-tree-row", ix))
                        .separator()
                        .h(px(TREE_ROW_HEIGHT_PX))
                        .px_2()
                        .py_0()
                        .child(
                            div()
                                .id(("thread-tree-thread-drag", ix))
                                .w_full()
                                .h(px(TREE_ROW_HEIGHT_PX))
                                .on_click(open_listener)
                                .on_action(thread_rename_action_listener)
                                .on_drag(thread_payload, |drag, _, _, cx| cx.new(|_| drag.clone()))
                                .context_menu(move |menu, _, _| {
                                    menu.menu(
                                        t!("sidebar.contextmenu.thread.edit"),
                                        Box::new(SidebarThreadRename),
                                    )
                                })
                                .child(
                                    h_flex()
                                        .w_full()
                                        .h(px(TREE_ROW_HEIGHT_PX))
                                        .min_w_0()
                                        .items_center()
                                        .child(tree_depth_guides(entry.depth(), cx))
                                        .child(
                                            h_flex()
                                                .w_full()
                                                .min_w_0()
                                                .h(px(TREE_ROW_CONTENT_HEIGHT_PX))
                                                .px(px(TREE_ROW_CONTENT_PADDING_X_PX))
                                                .items_center()
                                                .gap(px(TREE_ROW_GAP_PX))
                                                .rounded_md()
                                                .hover(|this| this.bg(cx.theme().sidebar_accent))
                                                .when(selected, |this| {
                                                    this.bg(cx.theme().sidebar_accent)
                                                })
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
                                                        .child(row.title),
                                                ),
                                        ),
                                ),
                        )
                }
                SidebarTreeNodeKey::AgentsDocRoot => {
                    let workspace_id = active_workspace_id.clone();
                    let open_listener = window.listener_for(
                        &desktop_entity,
                        move |view, _: &ClickEvent, window, cx| {
                            let Some(workspace_id) = workspace_id.clone() else {
                                return;
                            };
                            view.open_agents_doc_editor(
                                ThreadAgentsDocEditorScope::Root { workspace_id },
                                window,
                                cx,
                            );
                            cx.notify();
                        },
                    );
                    render_agents_doc_file_row(ix, entry.depth(), selected, open_listener, cx)
                }
                SidebarTreeNodeKey::Folder(folder_id) => {
                    let folder = folders_by_id.get(folder_id).cloned();
                    let folder_name = folder
                        .as_ref()
                        .map(|folder| folder.name.clone())
                        .unwrap_or_else(|| folder_id.to_owned());
                    let agents_doc_summary = agents_doc_summaries
                        .get(&ThreadAgentsDocSummaryKey::Folder(folder_id.to_owned()))
                        .cloned();
                    let agents_doc_edit_menu_label =
                        agents_doc_scope::agents_doc_edit_action(agents_doc_summary.as_ref());
                    let agents_doc_can_remove = agents_doc_scope::agents_doc_can_remove_override(
                        agents_doc_summary.as_ref(),
                    );
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
                    let drop_listener = window.listener_for(
                        &desktop_entity,
                        move |view, payload: &SidebarTreeDragPayload, _window, cx| {
                            view.handle_sidebar_drop_to_folder(
                                payload.clone(),
                                folder_id_for_move.clone(),
                                cx,
                            );
                            cx.notify();
                        },
                    );
                    let folder_click_listener = window.listener_for(
                        &desktop_entity,
                        move |view, _: &ClickEvent, _window, cx| {
                            view.toggle_thread_folder_expanded(folder_id_for_click.as_str(), cx);
                            view.set_thread_tree_selected_node_id(Some(folder_node_key(
                                folder_id_for_click.as_str(),
                            )));
                            cx.notify();
                        },
                    );
                    let folder_rename_action_listener = window.listener_for(
                        &desktop_entity,
                        move |view, _: &SidebarFolderRename, window, cx| {
                            view.open_rename_folder_dialog(
                                folder_id_for_context_menu_rename.clone(),
                                window,
                                cx,
                            );
                            cx.notify();
                        },
                    );
                    let folder_delete_action_listener = window.listener_for(
                        &desktop_entity,
                        move |view, _: &SidebarFolderDelete, _window, cx| {
                            view.delete_folder_from_sidebar(
                                folder_id_for_context_menu_delete.clone(),
                                cx,
                            );
                            cx.notify();
                        },
                    );
                    let folder_edit_agents_doc_action_listener = window.listener_for(
                        &desktop_entity,
                        move |view, _: &SidebarFolderEditAgentsDoc, window, cx| {
                            view.open_folder_agents_doc_editor_from_sidebar(
                                folder_id_for_context_menu_edit_agents_doc.clone(),
                                window,
                                cx,
                            );
                            cx.notify();
                        },
                    );
                    let folder_remove_agents_doc_action_listener = window.listener_for(
                        &desktop_entity,
                        move |view, _: &SidebarFolderRemoveAgentsDoc, _window, cx| {
                            view.remove_folder_agents_doc_override_from_sidebar(
                                folder_id_for_context_menu_remove_agents_doc.clone(),
                                cx,
                            );
                            cx.notify();
                        },
                    );

                    let folder_icon = if entry.is_expanded() && entry.is_folder() {
                        IconName::FolderOpen
                    } else {
                        IconName::FolderClosed
                    };
                    let disclosure_icon = if entry.is_expanded() && entry.is_folder() {
                        IconName::ChevronDown
                    } else {
                        IconName::ChevronRight
                    };

                    ListItem::new(("thread-tree-row", ix))
                        .separator()
                        .h(px(TREE_ROW_HEIGHT_PX))
                        .px_2()
                        .py_0()
                        .child(
                            div()
                                .id(("thread-tree-folder-drag", ix))
                                .on_click(folder_click_listener)
                                .on_action(folder_rename_action_listener)
                                .on_action(folder_delete_action_listener)
                                .on_action(folder_edit_agents_doc_action_listener)
                                .on_action(folder_remove_agents_doc_action_listener)
                                .on_drag(folder_payload, |drag, _, _, cx| cx.new(|_| drag.clone()))
                                .can_drop({
                                    let folder_id = folder_id.to_owned();
                                    let active_workspace_id = active_workspace_id.clone();
                                    let folders_by_id = Arc::clone(&folders_by_id);
                                    move |value, _, _| {
                                        can_drop_on_folder(
                                            value,
                                            active_workspace_id.as_deref(),
                                            folders_by_id.as_ref(),
                                            folder_id.as_str(),
                                        )
                                    }
                                })
                                .drag_over::<SidebarTreeDragPayload>(|style, _, _, cx| {
                                    style.rounded_md().bg(cx.theme().sidebar_accent)
                                })
                                .on_drop(drop_listener)
                                .w_full()
                                .h(px(TREE_ROW_HEIGHT_PX))
                                .context_menu(move |menu, _, _| {
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
                                                    t!(
                                                        "sidebar.contextmenu.folder.create_agents_doc"
                                                    )
                                                }
                                                AgentsDocEditAction::Edit => {
                                                    t!(
                                                        "sidebar.contextmenu.folder.edit_agents_doc"
                                                    )
                                                }
                                            },
                                            Box::new(SidebarFolderEditAgentsDoc),
                                        );

                                    if agents_doc_can_remove {
                                        menu.menu(
                                            t!(
                                                "sidebar.contextmenu.folder.remove_agents_doc_override"
                                            ),
                                            Box::new(SidebarFolderRemoveAgentsDoc),
                                        )
                                    } else {
                                        menu
                                    }
                                })
                                .child(
                                    h_flex()
                                        .w_full()
                                        .h(px(TREE_ROW_HEIGHT_PX))
                                        .items_center()
                                        .child(tree_depth_guides(entry.depth(), cx))
                                        .child(
                                            h_flex()
                                                .w_full()
                                                .h(px(TREE_ROW_CONTENT_HEIGHT_PX))
                                                .items_center()
                                                .gap(px(TREE_ROW_GAP_PX))
                                                .px(px(TREE_ROW_CONTENT_PADDING_X_PX))
                                                .rounded_md()
                                                .hover(|this| this.bg(cx.theme().sidebar_accent))
                                                .when(selected, |this| {
                                                    this.bg(cx.theme().sidebar_accent)
                                                })
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
                                                        .text_sm()
                                                        .text_color(cx.theme().foreground)
                                                        .line_height(relative(1.0))
                                                        .font_normal()
                                                        .opacity(SIDEBAR_MENU_ITEM_OPACITY)
                                                        .child(folder_name),
                                                ),
                                        ),
                                ),
                        )
                }
                SidebarTreeNodeKey::AgentsDocFolder(folder_id) => {
                    let folder = folders_by_id.get(folder_id).cloned();
                    let workspace_id = folder.as_ref().map(|folder| folder.workspace_id.clone());
                    let folder_id_for_open = folder_id.to_owned();
                    let open_listener = window.listener_for(
                        &desktop_entity,
                        move |view, _: &ClickEvent, window, cx| {
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
                        },
                    );
                    render_agents_doc_file_row(ix, entry.depth(), selected, open_listener, cx)
                }
                SidebarTreeNodeKey::Unknown => ListItem::new(("thread-tree-row", ix))
                    .separator()
                    .h(px(TREE_ROW_HEIGHT_PX))
                    .px_2()
                    .py_0(),
            }
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
                                    let icon_bg = cx.theme().foreground.opacity(0.075);
                                    let icon_bg_hover = cx.theme().foreground.opacity(0.1);
                                    div()
                                        .id("new-agent-icon")
                                        .size_6()
                                        .rounded_full()
                                        .bg(icon_bg)
                                        .group_hover("new-agent-btn", move |s| s.bg(icon_bg_hover))
                                        .flex()
                                        .items_center()
                                        .justify_center()
                                        .child(
                                            Icon::new(IconName::Plus)
                                                .size_4()
                                                .opacity(SIDEBAR_MENU_ITEM_OPACITY)
                                                .when(is_new_thread_active, |this| this.opacity(1.0)),
                                        )
                                })
                                .child(
                                    div()
                                        .flex_none()
                                        .line_height(relative(1.))
                                        .opacity(SIDEBAR_MENU_ITEM_OPACITY)
                                        .when(is_new_thread_active, |this| this.opacity(1.0))
                                        .child(t!("sidebar.action.new_thread").to_string()),
                                )
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
                        .can_drop(can_drop_on_root)
                        .drag_over::<SidebarTreeDragPayload>(|style, _, _, cx| {
                            style.bg(cx.theme().sidebar)
                        })
                        .on_drop(
                            cx.listener(|view, payload: &SidebarTreeDragPayload, _, cx| {
                                view.handle_sidebar_drop_to_root(payload.clone(), cx);
                                cx.notify();
                            }),
                        )
                        .child(tree_view)
                        .child(
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
                        ),
                ),
            )
            .when_some(desktop_update_panel, |this, panel| {
                this.child(
                    div()
                        .flex_none()
                        .px_2()
                        .pb_2()
                        .child(panel),
                )
            })
            .into_any_element()
    }

    fn render_desktop_update_sidebar_panel(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        if !self.desktop_update.should_render_sidebar_panel() {
            return None;
        }

        match &self.desktop_update {
            DesktopUpdateUiState::Checking => {
                Some(self.render_desktop_update_downloading_panel(cx))
            }
            DesktopUpdateUiState::Ready { version, .. } => {
                Some(self.render_desktop_update_ready_panel(version.as_str(), cx))
            }
            _ => None,
        }
    }

    fn render_desktop_update_ready_panel(
        &self,
        version: &str,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let hover_bg = cx.theme().sidebar_accent;
        let version_label = desktop_update_version_label(version);

        h_flex()
            .id("desktop-update-sidebar-ready")
            .w_full()
            .items_center()
            .gap_4()
            .px_3()
            .py_2()
            .rounded_xl()
            .border_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().muted.opacity(0.35))
            .cursor_pointer()
            .hover(move |this| this.bg(hover_bg))
            .child(Icon::new(PioneerIconName::Leaf).size_5())
            .child(
                v_flex()
                    .min_w_0()
                    .flex_1()
                    .child(
                        div()
                            .text_sm()
                            .font_semibold()
                            .child(t!("desktop_update.ready_title").to_string()),
                    )
                    .child(div().text_xs().opacity(0.6).child(version_label)),
            )
            .child(Icon::new(IconName::ArrowRight).size_5().opacity(0.6))
            .on_click(cx.listener(|view, _, window, cx| {
                view.restart_to_apply_desktop_update(window, cx);
            }))
            .into_any_element()
    }

    fn render_desktop_update_downloading_panel(&self, cx: &mut Context<Self>) -> AnyElement {
        h_flex()
            .id("desktop-update-sidebar-downloading")
            .w_full()
            .items_center()
            .gap_2()
            .px_3()
            .py_2()
            .rounded_xl()
            .border_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().muted.opacity(0.35))
            .child(
                div().size_5().flex().items_center().justify_center().child(
                    Spinner::new()
                        .icon(IconName::Loader)
                        .color(cx.theme().foreground.opacity(0.6)),
                ),
            )
            .child(
                div()
                    .min_w_0()
                    .flex_1()
                    .text_sm()
                    .opacity(0.6)
                    .child(t!("desktop_update.downloading").to_string()),
            )
            .into_any_element()
    }

    fn sidebar_rows_by_thread_id(&self) -> HashMap<String, SidebarThreadRow> {
        let Some(workspace_id) = self.active_workspace_id() else {
            return HashMap::new();
        };

        self.sorted_thread_ids_for_workspace(workspace_id)
            .into_iter()
            .map(|thread_id| {
                let coordinator = self.thread_coordinator(thread_id.as_str());
                let title = sidebar_thread_title_from_coordinator(coordinator);

                (thread_id.clone(), SidebarThreadRow { thread_id, title })
            })
            .collect()
    }

    fn build_sidebar_tree_model(&self) -> SidebarTreeModel {
        let Some(workspace_id) = self.active_workspace_id() else {
            return SidebarTreeModel {
                items: Vec::new(),
                visible_node_ids: Vec::new(),
            };
        };

        let client_model = client_thread_tree::sidebar_tree_model_from_workspace_data(
            client_thread_tree::SidebarTreeSourceData {
                workspace_id,
                folders: self.thread_folders_for_workspace(workspace_id),
                placements: self.thread_placements_for_workspace(workspace_id),
                sorted_thread_ids: self.sorted_thread_ids_for_workspace(workspace_id),
                agents_doc_summaries: self.thread_agents_doc_summaries.values().collect(),
                active_agents_doc_editor_scope: self.active_agents_doc_editor_scope.as_ref(),
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

fn sidebar_thread_title_from_coordinator(coordinator: Option<&ThreadCoordinator>) -> String {
    let Some(coordinator) = coordinator else {
        return t!("sidebar.thread.untitled").to_string();
    };
    let Some(thread) = coordinator.thread() else {
        return t!("sidebar.thread.untitled").to_string();
    };

    thread_display_title(thread).unwrap_or_else(|| t!("sidebar.thread.untitled").to_string())
}

fn desktop_update_version_label(version: &str) -> String {
    if version.starts_with('v') {
        version.to_owned()
    } else {
        format!("v{version}")
    }
}

fn thread_node_key(thread_id: &str) -> String {
    client_thread_tree::sidebar_thread_node_id(thread_id)
}

fn folder_node_key(folder_id: &str) -> String {
    client_thread_tree::sidebar_folder_node_id(folder_id)
}

pub(in crate::app) fn agents_doc_tree_node_key(scope: &ThreadAgentsDocEditorScope) -> String {
    client_thread_tree::sidebar_agents_doc_node_id_for_scope(scope)
}

#[cfg(test)]
fn agents_doc_root_node_key() -> String {
    client_thread_tree::sidebar_agents_doc_root_node_id()
}

#[cfg(test)]
fn agents_doc_folder_node_key(folder_id: &str) -> String {
    client_thread_tree::sidebar_agents_doc_folder_node_id(folder_id)
}

fn render_agents_doc_file_row(
    ix: usize,
    depth: usize,
    selected: bool,
    open_listener: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    cx: &mut App,
) -> ListItem {
    ListItem::new(("thread-tree-row", ix))
        .separator()
        .h(px(TREE_ROW_HEIGHT_PX))
        .px_2()
        .py_0()
        .child(
            div()
                .id(("thread-tree-agents-doc", ix))
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
                ),
        )
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

#[cfg(test)]
fn collect_visible_node_ids(items: &[TreeItem]) -> Vec<String> {
    fn visit(items: &[TreeItem], out: &mut Vec<String>) {
        for item in items {
            out.push(item.id.to_string());
            if item.is_expanded() {
                visit(item.children.as_slice(), out);
            }
        }
    }

    let mut out = Vec::new();
    visit(items, &mut out);
    out
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

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
    };

    #[::core::prelude::v1::test]
    fn collect_visible_node_ids_respects_expansion_state() {
        let items = vec![
            TreeItem::new(folder_node_key("a"), "A")
                .expanded(false)
                .child(TreeItem::new(thread_node_key("t1"), "T1")),
            TreeItem::new(folder_node_key("b"), "B")
                .expanded(true)
                .child(TreeItem::new(thread_node_key("t2"), "T2")),
        ];

        let ids = collect_visible_node_ids(items.as_slice());
        assert_eq!(
            ids,
            vec![
                folder_node_key("a"),
                folder_node_key("b"),
                thread_node_key("t2")
            ]
        );
    }

    #[::core::prelude::v1::test]
    fn collect_visible_node_ids_includes_agents_doc_file_nodes() {
        let items = vec![
            TreeItem::new(agents_doc_root_node_key(), "AGENTS.md"),
            TreeItem::new(folder_node_key("b"), "B")
                .expanded(true)
                .child(TreeItem::new(agents_doc_folder_node_key("b"), "AGENTS.md")),
        ];

        let ids = collect_visible_node_ids(items.as_slice());
        assert_eq!(
            ids,
            vec![
                agents_doc_root_node_key(),
                folder_node_key("b"),
                agents_doc_folder_node_key("b")
            ]
        );
    }

    #[::core::prelude::v1::test]
    fn folder_drop_guard_rejects_self_folder_and_accepts_thread() {
        let folders = HashMap::from([
            ("fld_1".to_owned(), sidebar_test_folder("fld_1", None)),
            ("fld_2".to_owned(), sidebar_test_folder("fld_2", None)),
            (
                "fld_child".to_owned(),
                sidebar_test_folder("fld_child", Some("fld_1")),
            ),
        ]);
        let thread_payload = SidebarTreeDragPayload {
            label: "t".to_owned(),
            item: SidebarTreeDragItem::Thread {
                thread_id: "thr_1".to_owned(),
            },
        };
        let folder_payload = SidebarTreeDragPayload {
            label: "f".to_owned(),
            item: SidebarTreeDragItem::Folder {
                folder_id: "fld_1".to_owned(),
            },
        };

        assert!(can_drop_on_folder(
            &thread_payload as &dyn Any,
            Some("ws_1"),
            &folders,
            "fld_1"
        ));
        assert!(!can_drop_on_folder(
            &folder_payload as &dyn Any,
            Some("ws_1"),
            &folders,
            "fld_1"
        ));
        assert!(can_drop_on_folder(
            &folder_payload as &dyn Any,
            Some("ws_1"),
            &folders,
            "fld_2"
        ));
        assert!(!can_drop_on_folder(
            &folder_payload as &dyn Any,
            Some("ws_1"),
            &folders,
            "fld_child"
        ));
    }

    fn sidebar_test_folder(id: &str, parent_folder_id: Option<&str>) -> ThreadFolder {
        ThreadFolder {
            id: id.to_owned(),
            workspace_id: "ws_1".to_owned(),
            parent_folder_id: parent_folder_id.map(str::to_owned),
            name: id.to_owned(),
            created_at: 0,
            updated_at: 0,
        }
    }

    #[::core::prelude::v1::test]
    fn sidebar_thread_title_uses_preview_fallback_or_untitled() {
        let thread_with_preview = Thread {
            workspace_id: "ws_1".to_owned(),
            id: "thr_1".to_owned(),
            name: None,
            preview: "one two three four five six seven".to_owned(),
            mode: ThreadMode::Chat,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: 0,
            updated_at: 0,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let coordinator_with_preview = ThreadCoordinator::new(thread_with_preview);
        assert_eq!(
            sidebar_thread_title_from_coordinator(Some(&coordinator_with_preview)),
            "one two three four five six..."
        );

        let thread_without_preview = Thread {
            preview: "   ".to_owned(),
            ..coordinator_with_preview
                .thread()
                .expect("thread should exist")
                .clone()
        };
        let coordinator_without_preview = ThreadCoordinator::new(thread_without_preview);
        assert_eq!(
            sidebar_thread_title_from_coordinator(Some(&coordinator_without_preview)),
            t!("sidebar.thread.untitled").to_string()
        );
    }
}
