use crate::app::{
    root::{MainContentView, PioneerDesktop},
    sidebar::{SidebarTreeDragItem, SidebarTreeDragPayload},
    thread::{ThreadCoordinator, fallback_title_from_first_user_text},
};
use crate::assets::PioneerIconName;
use gpui::{ClickEvent, prelude::*, *};
use gpui_component::{
    button::*,
    list::ListItem,
    menu::ContextMenuExt,
    theme::ActiveTheme,
    tree::{TreeItem, tree},
    *,
};
use std::any::Any;
use std::collections::{HashMap, HashSet};

const THREAD_NODE_PREFIX: &str = "thread:";
const FOLDER_NODE_PREFIX: &str = "folder:";
const THREADS_HEADER_NODE_ID: &str = "__threads_header__";
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

enum SidebarTreeNodeKey<'a> {
    ThreadsHeader,
    Thread(&'a str),
    Folder(&'a str),
    Unknown,
}

struct SidebarTreeModel {
    items: Vec<TreeItem>,
    visible_node_ids: Vec<String>,
}

actions!(
    sidebar_folder_menu,
    [SidebarFolderRename, SidebarFolderDelete]
);

impl PioneerDesktop {
    pub(in crate::app) fn rebuild_sidebar_tree_state(&mut self, cx: &mut Context<Self>) {
        let model = self.build_sidebar_tree_model();
        let selected_ix = if self.main_content_view == MainContentView::Threads {
            let selected_node_id = self
                .current_active_thread_id()
                .map(thread_node_key)
                .or_else(|| self.selected_thread_tree_node_id().map(str::to_owned));
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
        let rows_by_thread_id = self.sidebar_rows_by_thread_id();
        let folders_by_id = self.thread_folders.clone();
        let tree_state = self.thread_tree_state().clone();
        let is_threads_view_active = self.main_content_view == MainContentView::Threads;
        let is_new_thread_active = is_threads_view_active
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

                    ListItem::new(("thread-tree-row", ix))
                        .separator()
                        .h(px(TREE_ROW_HEIGHT_PX))
                        .px_0()
                        .py_0()
                        .child(
                            h_flex()
                                .w_full()
                                .h(px(TREE_ROW_HEIGHT_PX))
                                .justify_between()
                                .items_center()
                                .child(
                                    div()
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
                                .on_drag(thread_payload, |drag, _, _, cx| cx.new(|_| drag.clone()))
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
                SidebarTreeNodeKey::Folder(folder_id) => {
                    let folder = folders_by_id.get(folder_id).cloned();
                    let folder_name = folder
                        .as_ref()
                        .map(|folder| folder.name.clone())
                        .unwrap_or_else(|| folder_id.to_owned());
                    let folder_id_for_move = folder_id.to_owned();
                    let folder_id_for_click = folder_id.to_owned();
                    let folder_id_for_context_menu_rename = folder_id.to_owned();
                    let folder_id_for_context_menu_delete = folder_id.to_owned();
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
                                .on_drag(folder_payload, |drag, _, _, cx| cx.new(|_| drag.clone()))
                                .can_drop({
                                    let folder_id = folder_id.to_owned();
                                    move |value, _, _| can_drop_on_folder(value, folder_id.as_str())
                                })
                                .drag_over::<SidebarTreeDragPayload>(|style, _, _, cx| {
                                    style.rounded_md().bg(cx.theme().sidebar_accent)
                                })
                                .on_drop(drop_listener)
                                .w_full()
                                .h(px(TREE_ROW_HEIGHT_PX))
                                .context_menu(|menu, _, _| {
                                    menu.menu(
                                        t!("sidebar.contextmenu.folder.rename"),
                                        Box::new(SidebarFolderRename),
                                    )
                                    .menu(
                                        t!("sidebar.contextmenu.folder.delete"),
                                        Box::new(SidebarFolderDelete),
                                    )
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
                v_flex().pt_4().px_2().gap_1().child(
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
            )
            .when(self.has_known_threads(), |this| {
                this.child(
                    v_flex().size_full().child(
                        div()
                            .size_full()
                            .id("thread-tree-root-drop")
                            .can_drop(can_drop_on_root)
                            .drag_over::<SidebarTreeDragPayload>(|style, _, _, cx| {
                                style.bg(cx.theme().sidebar)
                            })
                            .on_drop(cx.listener(
                                |view, payload: &SidebarTreeDragPayload, _, cx| {
                                    view.handle_sidebar_drop_to_root(payload.clone(), cx);
                                    cx.notify();
                                },
                            ))
                            .child(tree_view),
                    ),
                )
            })
            .into_any_element()
    }

    fn sidebar_rows_by_thread_id(&self) -> HashMap<String, SidebarThreadRow> {
        self.sorted_thread_ids()
            .into_iter()
            .map(|thread_id| {
                let coordinator = self.thread_coordinator(thread_id.as_str());
                let title = sidebar_thread_title_from_coordinator(coordinator);

                (thread_id.clone(), SidebarThreadRow { thread_id, title })
            })
            .collect()
    }

    fn build_sidebar_tree_model(&self) -> SidebarTreeModel {
        let items = self.build_sidebar_tree_items();
        let visible_node_ids = collect_visible_node_ids(items.as_slice());
        SidebarTreeModel {
            items,
            visible_node_ids,
        }
    }

    fn build_sidebar_tree_items(&self) -> Vec<TreeItem> {
        let mut folders_by_parent: HashMap<String, Vec<String>> = HashMap::new();

        for folder in self.thread_folders.values() {
            let parent_key = folder
                .parent_folder_id
                .as_deref()
                .filter(|parent_id| self.thread_folders.contains_key(*parent_id))
                .unwrap_or_default()
                .to_owned();
            folders_by_parent
                .entry(parent_key)
                .or_default()
                .push(folder.id.clone());
        }

        for folder_ids in folders_by_parent.values_mut() {
            folder_ids.sort_by(|lhs, rhs| {
                let lhs_name = self
                    .thread_folders
                    .get(lhs.as_str())
                    .map(|folder| folder.name.as_str())
                    .unwrap_or_default();
                let rhs_name = self
                    .thread_folders
                    .get(rhs.as_str())
                    .map(|folder| folder.name.as_str())
                    .unwrap_or_default();
                lhs_name
                    .to_lowercase()
                    .cmp(&rhs_name.to_lowercase())
                    .then_with(|| lhs.cmp(rhs))
            });
        }

        let mut threads_by_folder: HashMap<String, Vec<String>> = HashMap::new();
        for thread_id in self.sorted_thread_ids() {
            let folder_key = self
                .thread_placement_folder_id(thread_id.as_str())
                .filter(|folder_id| self.thread_folders.contains_key(*folder_id))
                .unwrap_or_default()
                .to_owned();
            threads_by_folder
                .entry(folder_key)
                .or_default()
                .push(thread_id);
        }

        let mut visited_folders = HashSet::new();
        let mut items = self.build_sidebar_tree_branch(
            "",
            &folders_by_parent,
            &threads_by_folder,
            &mut visited_folders,
        );
        items.insert(
            0,
            TreeItem::new(THREADS_HEADER_NODE_ID, "threads-header").disabled(true),
        );
        items
    }

    fn build_sidebar_tree_branch(
        &self,
        parent_key: &str,
        folders_by_parent: &HashMap<String, Vec<String>>,
        threads_by_folder: &HashMap<String, Vec<String>>,
        visited_folders: &mut HashSet<String>,
    ) -> Vec<TreeItem> {
        let mut items = Vec::new();

        if let Some(folder_ids) = folders_by_parent.get(parent_key) {
            for folder_id in folder_ids {
                if !visited_folders.insert(folder_id.clone()) {
                    continue;
                }

                let children = self.build_sidebar_tree_branch(
                    folder_id.as_str(),
                    folders_by_parent,
                    threads_by_folder,
                    visited_folders,
                );

                let folder_name = self
                    .thread_folders
                    .get(folder_id.as_str())
                    .map(|folder| folder.name.clone())
                    .unwrap_or_else(|| folder_id.clone());

                let item = TreeItem::new(folder_node_key(folder_id.as_str()), folder_name)
                    .children(children)
                    .expanded(self.is_thread_folder_expanded(folder_id.as_str()));
                items.push(item);
            }
        }

        if let Some(thread_ids) = threads_by_folder.get(parent_key) {
            for thread_id in thread_ids {
                items.push(TreeItem::new(
                    thread_node_key(thread_id.as_str()),
                    thread_id.clone(),
                ));
            }
        }

        items
    }
}

fn thread_node_key(thread_id: &str) -> String {
    format!("{THREAD_NODE_PREFIX}{thread_id}")
}

fn sidebar_thread_title_from_coordinator(coordinator: Option<&ThreadCoordinator>) -> String {
    let Some(coordinator) = coordinator else {
        return t!("sidebar.thread.untitled").to_string();
    };
    let Some(thread) = coordinator.thread() else {
        return t!("sidebar.thread.untitled").to_string();
    };

    if let Some(name) = thread
        .name
        .as_ref()
        .map(|name| name.trim())
        .filter(|name| !name.is_empty())
    {
        return name.to_owned();
    }

    fallback_title_from_first_user_text(thread.preview.as_str())
        .unwrap_or_else(|| t!("sidebar.thread.untitled").to_string())
}

fn folder_node_key(folder_id: &str) -> String {
    format!("{FOLDER_NODE_PREFIX}{folder_id}")
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
    if value == THREADS_HEADER_NODE_ID {
        return SidebarTreeNodeKey::ThreadsHeader;
    }

    if let Some(thread_id) = value.strip_prefix(THREAD_NODE_PREFIX) {
        return SidebarTreeNodeKey::Thread(thread_id);
    }

    if let Some(folder_id) = value.strip_prefix(FOLDER_NODE_PREFIX) {
        return SidebarTreeNodeKey::Folder(folder_id);
    }

    SidebarTreeNodeKey::Unknown
}

fn can_drop_on_root(value: &dyn Any, _: &mut Window, _: &mut App) -> bool {
    value.is::<SidebarTreeDragPayload>()
}

fn can_drop_on_folder(value: &dyn Any, target_folder_id: &str) -> bool {
    let Some(payload) = value.downcast_ref::<SidebarTreeDragPayload>() else {
        return false;
    };

    match &payload.item {
        SidebarTreeDragItem::Thread { .. } => true,
        SidebarTreeDragItem::Folder { folder_id } => folder_id != target_folder_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
    };

    #[::core::prelude::v1::test]
    fn parse_sidebar_tree_node_key_roundtrip() {
        let thread_id = "thr_123";
        let folder_id = "fld_123";
        let thread_key = thread_node_key(thread_id);
        let folder_key = folder_node_key(folder_id);

        let parsed_thread = parse_sidebar_tree_node_key(thread_key.as_str());
        let parsed_folder = parse_sidebar_tree_node_key(folder_key.as_str());

        assert!(matches!(
            parsed_thread,
            SidebarTreeNodeKey::Thread("thr_123")
        ));
        assert!(matches!(
            parsed_folder,
            SidebarTreeNodeKey::Folder("fld_123")
        ));
        assert!(matches!(
            parse_sidebar_tree_node_key(THREADS_HEADER_NODE_ID),
            SidebarTreeNodeKey::ThreadsHeader
        ));
        assert!(matches!(
            parse_sidebar_tree_node_key("unknown"),
            SidebarTreeNodeKey::Unknown
        ));
    }

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
    fn folder_drop_guard_rejects_self_folder_and_accepts_thread() {
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

        assert!(can_drop_on_folder(&thread_payload as &dyn Any, "fld_1"));
        assert!(!can_drop_on_folder(&folder_payload as &dyn Any, "fld_1"));
        assert!(can_drop_on_folder(&folder_payload as &dyn Any, "fld_2"));
    }

    #[::core::prelude::v1::test]
    fn fallback_title_from_first_user_text_truncates_to_six_words() {
        let title =
            fallback_title_from_first_user_text("one two three four five six seven").unwrap();
        assert_eq!(title, "one two three four five six...");
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
