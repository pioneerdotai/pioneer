use super::{ADMINISTRATION_CONTENT_INVITATIONS_NODE_ID, ADMINISTRATION_CONTENT_MEMBERS_NODE_ID};
use crate::app::root::{AdministrationContentView, PioneerDesktop};
use gpui_kit::component::{list::ListItem, theme::ActiveTheme, tree::tree, *};
use gpui_kit::{ClickEvent, prelude::*, *};

const TREE_ROW_HEIGHT_PX: f32 = 32.0;
const TREE_ROW_CONTENT_HEIGHT_PX: f32 = 28.0;
const TREE_ROW_GAP_PX: f32 = 6.0;
const TREE_ROW_CONTENT_PADDING_X_PX: f32 = 8.0;
const SIDEBAR_MENU_ITEM_OPACITY: f32 = 0.8;

enum AdminSidebarNodeKey {
    Content(AdministrationContentView),
    Unknown,
}

impl PioneerDesktop {
    pub(crate) fn render_administration_sidebar(&self, cx: &mut Context<Self>) -> AnyElement {
        let administration_tree_state = self.administration_tree_state.clone();
        let desktop_entity = cx.entity().clone();

        let tree_view = tree(
            &administration_tree_state,
            move |ix, entry, selected, window, cx| {
                let item_id = entry.item().id.as_ref();

                match parse_admin_sidebar_node_key(item_id) {
                    AdminSidebarNodeKey::Content(content_view) => {
                        let open_listener = window.listener_for(
                            &desktop_entity,
                            move |view, _: &ClickEvent, _window, cx| {
                                view.open_administration_content(content_view, cx);
                                cx.notify();
                            },
                        );

                        ListItem::new(("administration-tree-row", ix))
                            .separator()
                            .h(px(TREE_ROW_HEIGHT_PX))
                            .px_2()
                            .py_0()
                            .child(
                                div()
                                    .id(("administration-tree-row-click", ix))
                                    .w_full()
                                    .h(px(TREE_ROW_HEIGHT_PX))
                                    .on_click(open_listener)
                                    .child(
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
                                                    .hover(|this| {
                                                        this.bg(cx.theme().sidebar_accent)
                                                    })
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
                                                            .when(selected, |this| {
                                                                this.opacity(1.0)
                                                            })
                                                            .child(admin_sidebar_content_label(
                                                                content_view,
                                                            )),
                                                    ),
                                            ),
                                    ),
                            )
                    }
                    AdminSidebarNodeKey::Unknown => ListItem::new(("administration-tree-row", ix))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[::core::prelude::v1::test]
    fn members_and_invitations_have_dedicated_sidebar_nodes() {
        assert!(matches!(
            parse_admin_sidebar_node_key(ADMINISTRATION_CONTENT_MEMBERS_NODE_ID),
            AdminSidebarNodeKey::Content(AdministrationContentView::Members)
        ));
        assert!(matches!(
            parse_admin_sidebar_node_key(ADMINISTRATION_CONTENT_INVITATIONS_NODE_ID),
            AdminSidebarNodeKey::Content(AdministrationContentView::Invitations)
        ));
    }
}
