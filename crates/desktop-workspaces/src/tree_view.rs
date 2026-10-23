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
        input_changed || structure_changed
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
        let desktop_entity = cx.entity().clone();
        let principal_capabilities = self.principal_presentation_capabilities();
        let can_manage_workspace = principal_capabilities.can_manage_workspace;
        let can_manage_all_threads = principal_capabilities.can_manage_all_threads;
        let scoped_thread_id = self.thread_scope_capabilities_thread_id.clone();
        let scoped_thread_capabilities = self.thread_scope_capabilities;
        let active_workspace_id = self.active_workspace_id().map(str::to_owned);
        let rows_by_thread_id = self.prepared_sidebar.rows.clone();
        let task_activity_indicators = self.task_activity_indicators.clone();
        let publication = self.input.clone();
        let folders_by_id = self.prepared_sidebar.folders.clone();
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

        let tree_view = tree(&tree_state, move |_ix, entry, selected, window, cx| {
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

                    ListItem::new(format!("thread-tree-row-{item_id}"))
                        .separator()
                        .h(px(TREE_ROW_HEIGHT_PX))
                        .px_0()
                        .py_0()
                        .child(
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
                                .context_menu(move |menu, _, _| {
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
                                .when(can_manage_workspace, |this| this.child(
                                    h_flex().pr_2().items_center().child(
                                        Button::new("create-thread-folder")
                                            .small()
                                            .ghost()
                                            .compact()
                                            .child(
                                                Icon::empty().path("icons/folder-plus.svg")
                                                    .size_4()
                                                    .opacity(0.6),
                                            )
                                            .on_click(create_folder_listener),
                                    ),
                                )),
                        )
                }
                SidebarTreeNodeKey::Thread(thread_id) => {
                    let row = rows_by_thread_id
                        .get(thread_id)
                        .cloned()
                        .unwrap_or_else(|| SidebarThreadRow {
                            thread_id: thread_id.to_owned(),
                            title: Some(thread_id.to_owned()),
                            unread_count: 0,
                        });
                    let title = row
                        .title
                        .unwrap_or_else(|| t!("sidebar.thread.untitled").to_string());
                    let can_manage_thread = can_manage_all_threads
                        || (scoped_thread_id.as_deref() == Some(row.thread_id.as_str())
                            && scoped_thread_capabilities.can_manage_thread);
                    let thread_id_for_click = row.thread_id.clone();
                    let thread_id_for_context_menu_rename = row.thread_id.clone();
                    let thread_payload = SidebarTreeDragPayload {
                        label: title.clone(),
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

                    ListItem::new(format!("thread-tree-row-{item_id}"))
                        .separator()
                        .h(px(TREE_ROW_HEIGHT_PX))
                        .px_2()
                        .py_0()
                        .child(
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
                                        cx.new(|_| {
                                            crate::drag_preview::SidebarDragSession(drag.clone())
                                        })
                                    })
                                })
                                .context_menu(move |menu, _, _| {
                                    if can_manage_thread {
                                        menu.menu(
                                            t!("sidebar.contextmenu.thread.edit"),
                                            Box::new(SidebarThreadRename),
                                        )
                                    } else {
                                        menu
                                    }
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
                                                        .child(title),
                                                )
                                                .when(
                                                    should_render_thread_unread_badge(
                                                        row.unread_count,
                                                        selected,
                                                    ),
                                                    |this| {
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
                                                                .child(div().opacity(0.8).child(
                                                                    row.unread_count.to_string(),
                                                                )),
                                                        )
                                                    },
                                                )
                                                .when_some(
                                                    task_activity_indicators.get(item_id).cloned(),
                                                    |this, indicator| this.child(indicator),
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
                    render_agents_doc_file_row(item_id, entry.depth(), selected, open_listener, cx)
                }
                SidebarTreeNodeKey::Folder(folder_id) => {
                    let unread_count = publication
                        .as_ref()
                        .and_then(|p| p.snapshot().unread_by_folder_id.get(folder_id))
                        .copied()
                        .unwrap_or(0);
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
                            // Tree already toggles expansion on mouse-down and
                            // publishes it through TreeEvent. Click only selects.
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

                    ListItem::new(format!("thread-tree-row-{item_id}"))
                        .separator()
                        .h(px(TREE_ROW_HEIGHT_PX))
                        .px_2()
                        .py_0()
                        .child(
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
                                })
                                .context_menu(move |menu, _, _| {
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
                                                AgentsDocEditAction::Create => t!(
                                                    "sidebar.contextmenu.folder.create_agents_doc"
                                                ),
                                                AgentsDocEditAction::Edit => t!(
                                                    "sidebar.contextmenu.folder.edit_agents_doc"
                                                ),
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
                                .w_full()
                                .h(px(TREE_ROW_HEIGHT_PX))
                                .child(
                                    h_flex()
                                        .w_full()
                                        .h(px(TREE_ROW_HEIGHT_PX))
                                        .items_center()
                                        .child(tree_depth_guides(entry.depth(), cx))
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
                                                            .child(div().opacity(0.8).child(
                                                                unread_count.to_string(),
                                                            )),
                                                    )
                                                })
                                                .when_some(task_activity_indicators.get(item_id).cloned(), |this, indicator| this.child(indicator)),
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
                    render_agents_doc_file_row(item_id, entry.depth(), selected, open_listener, cx)
                }
                SidebarTreeNodeKey::Unknown => ListItem::new(format!("thread-tree-row-{item_id}"))
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
) -> ListItem {
    ListItem::new(format!("thread-tree-row-{item_id}"))
        .separator()
        .h(px(TREE_ROW_HEIGHT_PX))
        .px_2()
        .py_0()
        .child(
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
    use super::*;
    use gpui_kit::TestAppContext;

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
        core.activate_thread(None, None);
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
