const ADMINISTRATION_CONTENT_MEMBERS_NODE_ID: &str = "administration:members";
const ADMINISTRATION_CONTENT_INVITATIONS_NODE_ID: &str = "administration:invitations";
use gpui_kit::component::tree::{TreeItem, TreeState};
use pioneer_client::authorization::PrincipalPresentationCapabilities;

pub(crate) struct AdministrationSidebar {
    owner: WeakEntity<AdministrationView>,
    administration_tree_state: Entity<TreeState>,
    mount: u64,
    scope: String,
    selection: Option<(AdministrationContentView, bool, bool)>,
}
impl AdministrationSidebar {
    pub(crate) fn new(
        owner: WeakEntity<AdministrationView>,
        mount: u64,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| Self {
            owner,
            mount,
            scope: String::new(),
            administration_tree_state: cx.new(|cx| TreeState::new(cx)),
            selection: None,
        })
    }
    pub(crate) fn sync(
        &mut self,
        scope: String,
        route: AdministrationContentView,
        capabilities: PrincipalPresentationCapabilities,
        cx: &mut Context<Self>,
    ) {
        let selection = (
            route,
            capabilities.can_view_member_directory,
            capabilities.can_view_invitations,
        );
        if self.scope == scope && self.selection == Some(selection) {
            return;
        }
        self.scope = scope;
        self.selection = Some(selection);
        let mut items = Vec::new();
        if selection.1 {
            items.push((
                AdministrationContentView::Members,
                TreeItem::new(ADMINISTRATION_CONTENT_MEMBERS_NODE_ID, "members"),
            ));
        }
        if selection.2 {
            items.push((
                AdministrationContentView::Invitations,
                TreeItem::new(ADMINISTRATION_CONTENT_INVITATIONS_NODE_ID, "invitations"),
            ));
        }
        let selected = items.iter().position(|(candidate, _)| candidate == &route);
        self.administration_tree_state.update(cx, |tree, cx| {
            tree.set_items(
                items.into_iter().map(|(_, item)| item).collect::<Vec<_>>(),
                cx,
            );
            tree.set_selected_index(selected, cx);
        });
        cx.notify();
    }
}
impl Render for AdministrationSidebar {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_administration_sidebar(cx)
    }
}

use crate::administration::AdministrationView;
use gpui_kit::component::{list::ListItem, theme::ActiveTheme, tree::tree, *};
use gpui_kit::{ClickEvent, prelude::*, *};
use pioneer_client::navigation::AdministrationRoute as AdministrationContentView;

const TREE_ROW_HEIGHT_PX: f32 = 32.0;
const TREE_ROW_CONTENT_HEIGHT_PX: f32 = 28.0;
const TREE_ROW_GAP_PX: f32 = 6.0;
const TREE_ROW_CONTENT_PADDING_X_PX: f32 = 8.0;
const SIDEBAR_MENU_ITEM_OPACITY: f32 = 0.8;

enum AdminSidebarNodeKey {
    Content(AdministrationContentView),
    Unknown,
}

impl AdministrationSidebar {
    pub(crate) fn render_administration_sidebar(&self, cx: &mut Context<Self>) -> AnyElement {
        let administration_tree_state = self.administration_tree_state.clone();
        let desktop_entity = self.owner.clone();
        let mount = self.mount;
        let scope = self.scope.clone();

        let tree_view = tree(
            &administration_tree_state,
            move |_ix, entry, selected, _window, cx| {
                let item_id = entry.item().id.as_ref();

                match parse_admin_sidebar_node_key(item_id) {
                    AdminSidebarNodeKey::Content(content_view) => {
                        let owner = desktop_entity.clone();
                        let open_listener = move |_: &ClickEvent, _: &mut Window, cx: &mut App| {
                            let _ = owner.update(cx, |view, cx| {
                                view.open_administration_content(content_view, cx)
                            });
                        };

                        ListItem::new(SharedString::from(format!(
                            "administration:{mount}:{scope}:sidebar:{item_id}:row"
                        )))
                        .h(px(TREE_ROW_HEIGHT_PX))
                        .px_2()
                        .py_0()
                        .on_click(open_listener)
                        .child(
                            div().w_full().h(px(TREE_ROW_HEIGHT_PX)).child(
                                h_flex()
                                    .w_full()
                                    .h(px(TREE_ROW_HEIGHT_PX))
                                    .items_center()
                                    .child(
                                        h_flex()
                                            .w_full()
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
                                                    .text_sm()
                                                    .text_color(cx.theme().foreground)
                                                    .line_height(relative(1.0))
                                                    .font_normal()
                                                    .opacity(SIDEBAR_MENU_ITEM_OPACITY)
                                                    .when(selected, |this| this.opacity(1.0))
                                                    .child(admin_sidebar_content_label(
                                                        content_view,
                                                    )),
                                            ),
                                    ),
                            ),
                        )
                    }
                    AdminSidebarNodeKey::Unknown => ListItem::new(SharedString::from(format!(
                        "administration:{mount}:{scope}:sidebar:{item_id}:row"
                    )))
                    .separator()
                    .h(px(TREE_ROW_HEIGHT_PX))
                    .px_2()
                    .py_0(),
                }
            },
        );

        v_flex()
            .size_full()
            .bg(cx.theme().sidebar)
            .p_0()
            .child(
                v_flex().size_full().pt_4().child(
                    v_flex()
                        .size_full()
                        .child(div().size_full().child(tree_view)),
                ),
            )
            .into_any_element()
    }
}

fn parse_admin_sidebar_node_key(value: &str) -> AdminSidebarNodeKey {
    if value == ADMINISTRATION_CONTENT_MEMBERS_NODE_ID {
        return AdminSidebarNodeKey::Content(AdministrationContentView::Members);
    }
    if value == ADMINISTRATION_CONTENT_INVITATIONS_NODE_ID {
        return AdminSidebarNodeKey::Content(AdministrationContentView::Invitations);
    }

    AdminSidebarNodeKey::Unknown
}

fn admin_sidebar_content_label(content_view: AdministrationContentView) -> String {
    match content_view {
        AdministrationContentView::Members => t!("settings.sidebar.members").to_string(),
        AdministrationContentView::Invitations => t!("settings.sidebar.invitations").to_string(),
    }
}
