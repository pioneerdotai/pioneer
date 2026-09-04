use super::{
    PROVIDERS_FILTER_API_NODE_ID, PROVIDERS_FILTER_CLI_NODE_ID, PROVIDERS_FILTER_CONNECTED_NODE_ID,
};
use crate::app::root::{PioneerDesktop, ProviderFilter};
use gpui_kit::component::{list::ListItem, theme::ActiveTheme, tree::tree, *};
use gpui_kit::{ClickEvent, prelude::*, *};
use pioneer_client::providers::selectors;

const TREE_ROW_HEIGHT_PX: f32 = 32.0;
const TREE_ROW_CONTENT_HEIGHT_PX: f32 = 28.0;
const TREE_ROW_GAP_PX: f32 = 6.0;
const TREE_ROW_CONTENT_PADDING_X_PX: f32 = 8.0;
const TREE_ROW_CHILD_INDENT_PX: f32 = 20.0;
const SIDEBAR_MENU_ITEM_OPACITY: f32 = 0.8;

enum ProvidersSidebarNodeKey {
    Filter(ProviderFilter),
    Unknown,
}

impl PioneerDesktop {
    pub(crate) fn render_providers_sidebar(&self, cx: &mut Context<Self>) -> AnyElement {
        let provider_tree_state = self.provider_tree_state.clone();
        let desktop_entity = cx.entity().clone();

        let tree_view = tree(
            &provider_tree_state,
            move |ix, entry, selected, window, cx| {
                let item_id = entry.item().id.as_ref();

                match parse_providers_sidebar_node_key(item_id) {
                    ProvidersSidebarNodeKey::Filter(filter) => {
                        let open_listener = window.listener_for(
                            &desktop_entity,
                            move |view, _: &ClickEvent, _window, cx| {
                                view.set_provider_filter(filter, cx);
                            },
                        );
                        let label = provider_filter_label(filter);
                        let content_padding_left = provider_filter_content_padding_left(filter);

                        ListItem::new(("providers-tree-row", ix))
                            .separator()
                            .h(px(TREE_ROW_HEIGHT_PX))
                            .px_2()
                            .py_0()
                            .child(
                                div()
                                    .id(("providers-tree-row-click", ix))
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
                                                    .pl(px(content_padding_left))
                                                    .pr(px(TREE_ROW_CONTENT_PADDING_X_PX))
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
                                                            .child(label),
                                                    ),
                                            ),
                                    ),
                            )
                    }
                    ProvidersSidebarNodeKey::Unknown => ListItem::new(("providers-tree-row", ix))
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

fn parse_providers_sidebar_node_key(value: &str) -> ProvidersSidebarNodeKey {
    if let Some(filter) = selectors::provider_filter_from_node_id(
        value,
        PROVIDERS_FILTER_API_NODE_ID,
        PROVIDERS_FILTER_CONNECTED_NODE_ID,
        PROVIDERS_FILTER_CLI_NODE_ID,
    ) {
        return ProvidersSidebarNodeKey::Filter(filter);
    }

    ProvidersSidebarNodeKey::Unknown
}

fn provider_filter_label(filter: ProviderFilter) -> String {
    match filter {
        ProviderFilter::Api => t!("providers.sidebar.api").to_string(),
        ProviderFilter::Connected => t!("providers.sidebar.connected").to_string(),
        ProviderFilter::Cli => t!("providers.sidebar.cli").to_string(),
    }
}

fn provider_filter_content_padding_left(filter: ProviderFilter) -> f32 {
    match filter {
        ProviderFilter::Connected => TREE_ROW_CONTENT_PADDING_X_PX + TREE_ROW_CHILD_INDENT_PX,
        ProviderFilter::Api | ProviderFilter::Cli => TREE_ROW_CONTENT_PADDING_X_PX,
    }
}
