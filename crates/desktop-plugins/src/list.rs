use super::*;

impl PluginsView {
    pub(super) fn render_list(&self, cx: &mut Context<Self>) -> AnyElement {
        let header = v_flex()
            .pt_3()
            .px_6()
            .pb_3()
            .gap_2()
            .border_b_1()
            .border_color(cx.theme().border)
            .child(pioneer_desktop_foundation::catalog_heading(
                t!("plugins.title").to_string(),
                t!("plugins.description").to_string(),
                cx,
            ))
            .child(
                div()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(t!("plugins.found_count", count = self.input.plugins.len()).to_string()),
            );
        let list = v_flex()
            .p_6()
            .gap_3()
            .when(self.upload.read(cx).active(), |view| {
                view.child(self.upload.clone())
            })
            .children(self.upload_recovery(cx))
            .when_some(self.installation_error(cx), |list, error| {
                list.child(div().text_sm().text_color(cx.theme().danger).child(error))
            })
            .when(self.initial_loading(), |list| {
                list.child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().muted_foreground)
                        .child(t!("plugins.loading").to_string()),
                )
            })
            .when(self.connected() && !self.readable(), |view| {
                view.child(div().text_sm().child(self.access_label()))
            })
            .when(!self.connected(), |view| {
                view.child(div().text_sm().child(t!("plugins.offline").to_string()))
            })
            .when(self.input.error.is_some(), |list| {
                list.child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().danger)
                        .child(t!("plugins.error").to_string()),
                )
            })
            .when(
                self.input.loaded && self.input.error.is_none() && self.input.plugins.is_empty(),
                |list| {
                    list.child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(t!("plugins.empty").to_string()),
                    )
                },
            )
            .children(
                self.input
                    .plugins
                    .iter()
                    .map(|plugin| self.render_plugin_row(plugin, cx)),
            );
        v_flex()
            .size_full()
            .min_w_0()
            .min_h_0()
            .bg(cx.theme().background)
            .child(header)
            .child(
                v_flex()
                    .id("plugins-list-scroll")
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .overflow_y_scrollbar()
                    .child(list),
            )
            .into_any_element()
    }
}

impl PluginsView {
    fn render_plugin_row(&self, plugin: &PluginItem, cx: &mut Context<Self>) -> AnyElement {
        let id = plugin.id.clone();
        let owner = cx.weak_entity();
        Button::new(SharedString::from(format!("plugin-{}", plugin.id)))
            .accessibility_label(plugin.name.clone())
            .tooltip(plugin.name.clone())
            .outline()
            .w_full()
            .h_auto()
            .px_4()
            .py_3()
            .justify_start()
            .rounded(cx.theme().radius)
            .child(
                h_flex()
                    .w_full()
                    .min_w_0()
                    .items_center()
                    .gap_4()
                    .child(
                        v_flex()
                            .flex_1()
                            .min_w_0()
                            .gap_3()
                            .child(
                                div()
                                    .text_sm()
                                    .font_semibold()
                                    .truncate()
                                    .child(plugin.name.clone()),
                            )
                            .when_some(plugin.description.clone(), |view, description| {
                                view.child(div().text_sm().text_color(cx.theme().muted_foreground).child(description))
                            })
                            .child(
                                h_flex()
                                    .gap_2()
                                    .text_xs()
                                    .text_color(cx.theme().muted_foreground)
                                    .when_some(plugin.version.clone(), |row, version| {
                                        row.child(version)
                                    })
                                    .child(
                                        t!(
                                            "plugins.component_count",
                                            count = plugin.components.len()
                                        )
                                        .to_string(),
                                    ),
                            ),
                    )
                    .child(crate::details::status_badge(
                        if !plugin.enabled && plugin.state == "installed" {
                            "disabled"
                        } else {
                            &plugin.status
                        },
                        cx,
                    )),
            )
            .on_click(move |_, _, cx| {
                let _ = owner.update(cx, |view, cx| view.select_plugin(Some(id.clone()), cx));
            })
            .into_any_element()
    }
}
