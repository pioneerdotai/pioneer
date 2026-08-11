use super::{
    SETTINGS_CONTENT_ACCOUNT_NODE_ID, SETTINGS_CONTENT_GENERAL_NODE_ID,
    SETTINGS_CONTENT_MEMORY_NODE_ID, SETTINGS_CONTENT_SELF_IMPROVEMENT_NODE_ID,
};
use crate::app::root::{PioneerDesktop, SettingsContentView};
use gpui::{ClickEvent, prelude::*, *};
use gpui_component::{list::ListItem, theme::ActiveTheme, tree::tree, *};

const TREE_ROW_HEIGHT_PX: f32 = 32.0;
const TREE_ROW_CONTENT_HEIGHT_PX: f32 = 28.0;
const TREE_ROW_GAP_PX: f32 = 6.0;
const TREE_ROW_CONTENT_PADDING_X_PX: f32 = 8.0;
const SIDEBAR_MENU_ITEM_OPACITY: f32 = 0.8;

enum SettingsSidebarNodeKey {
    Content(SettingsContentView),
    Unknown,
}

impl PioneerDesktop {
    pub(crate) fn render_settings_sidebar(&self, cx: &mut Context<Self>) -> AnyElement {
        let settings_tree_state = self.settings_tree_state.clone();

        let desktop_entity = cx.entity().clone();

        let tree_view = tree(
            &settings_tree_state,
            move |ix, entry, selected, window, cx| {
                let item_id = entry.item().id.as_ref();

                match parse_settings_sidebar_node_key(item_id) {
                    SettingsSidebarNodeKey::Content(content_view) => {
                        let open_listener = window.listener_for(
                            &desktop_entity,
                            move |view, _: &ClickEvent, _window, cx| {
                                view.open_settings_content_from_sidebar(content_view, cx);
                                cx.notify();
                            },
                        );

                        ListItem::new(("settings-tree-row", ix))
                            .separator()
                            .h(px(TREE_ROW_HEIGHT_PX))
                            .px_2()
                            .py_0()
                            .child(
                                div()
                                    .id(("settings-tree-row-click", ix))
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
                                                            .child(settings_sidebar_content_label(
                                                                content_view,
                                                            )),
                                                    ),
                                            ),
                                    ),
                            )
                    }
                    SettingsSidebarNodeKey::Unknown => ListItem::new(("settings-tree-row", ix))
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

fn parse_settings_sidebar_node_key(value: &str) -> SettingsSidebarNodeKey {
    if value == SETTINGS_CONTENT_GENERAL_NODE_ID {
        return SettingsSidebarNodeKey::Content(SettingsContentView::General);
    }
    if value == SETTINGS_CONTENT_ACCOUNT_NODE_ID {
        return SettingsSidebarNodeKey::Content(SettingsContentView::Account);
    }
    if value == SETTINGS_CONTENT_MEMORY_NODE_ID {
        return SettingsSidebarNodeKey::Content(SettingsContentView::Memory);
    }
    if value == SETTINGS_CONTENT_SELF_IMPROVEMENT_NODE_ID {
        return SettingsSidebarNodeKey::Content(SettingsContentView::SelfImprovement);
    }

    SettingsSidebarNodeKey::Unknown
}

fn settings_sidebar_content_label(content_view: SettingsContentView) -> String {
    match content_view {
        SettingsContentView::General => t!("settings.sidebar.general").to_string(),
        SettingsContentView::Account => t!("settings.sidebar.account").to_string(),
        SettingsContentView::Memory => t!("settings.sidebar.memory").to_string(),
        SettingsContentView::SelfImprovement => t!("settings.sidebar.self_improvement").to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[::core::prelude::v1::test]
    fn self_improvement_has_a_dedicated_sidebar_node() {
        assert!(matches!(
            parse_settings_sidebar_node_key(SETTINGS_CONTENT_SELF_IMPROVEMENT_NODE_ID),
            SettingsSidebarNodeKey::Content(SettingsContentView::SelfImprovement)
        ));
    }

    #[::core::prelude::v1::test]
    fn account_has_a_dedicated_sidebar_node() {
        assert!(matches!(
            parse_settings_sidebar_node_key(SETTINGS_CONTENT_ACCOUNT_NODE_ID),
            SettingsSidebarNodeKey::Content(SettingsContentView::Account)
        ));
    }
}
