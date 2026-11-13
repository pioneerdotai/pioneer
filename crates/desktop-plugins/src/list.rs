use super::*;

impl PluginsView {
    pub(super) fn render_list(&self, cx: &mut Context<Self>) -> AnyElement {
        let show_empty = self.input.loaded
            && self.input.error.is_none()
            && self.input.plugins.is_empty()
            && self.connected()
            && self.readable()
            && !self.upload.read(cx).active()
            && self.installation_error(cx).is_none();
        let header = pioneer_desktop_foundation::catalog_list_header(
            t!("plugins.title").to_string(),
            t!("plugins.description").to_string(),
            t!("plugins.found_count", count = self.input.plugins.len()).to_string(),
            cx,
        );
        let list = v_flex()
            .w_full()
            .when(show_empty, |list| list.h_full())
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
            .when(show_empty, |list| list.child(self.render_empty_state(cx)))
            .when(!self.input.plugins.is_empty(), |list| {
                list.child(
                    v_flex().w_full().children(
                        self.input
                            .plugins
                            .iter()
                            .map(|plugin| self.render_plugin_row(plugin, cx)),
                    ),
                )
            });
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
        let card = pioneer_desktop_foundation::CatalogCard::new(plugin.name.clone())
            .icon(
                self.plugin_images
                    .element(plugin.metadata.logo.as_ref())
                    .size_8(),
            )
            .description(plugin.description.clone().unwrap_or_default())
            .metadata(format!(
                "{} {} · {}",
                t!("plugins.version"),
                plugin.version.as_deref().unwrap_or("-"),
                t!("plugins.component_count", count = plugin.components.len())
            ))
            .when_some(crate::details::summary_status(plugin), |card, status| {
                card.status(
                    status_label(status),
                    crate::details::status_color(status, cx),
                )
            });
        v_flex()
            .w_full()
            .h(px(pioneer_desktop_foundation::CatalogCard::HEIGHT + 10.))
            .pb(px(10.))
            .child(
                Button::new(SharedString::from(format!("plugin-{}", plugin.id)))
                    .accessibility_label(plugin.name.clone())
                    .cursor_default()
                    .text()
                    .w_full()
                    .h_full()
                    .p_0()
                    .justify_start()
                    .rounded_lg()
                    .text_color(cx.theme().foreground)
                    .child(card)
                    .on_click(move |_, _, cx| {
                        let _ =
                            owner.update(cx, |view, cx| view.select_plugin(Some(id.clone()), cx));
                    }),
            )
            .into_any_element()
    }

    fn render_empty_state(&self, cx: &mut Context<Self>) -> AnyElement {
        let can_manage = self
            .workspace
            .as_deref()
            .is_some_and(|workspace| self.client.plugins_management_allowed(workspace));
        let install = can_manage.then(|| {
            Button::new("plugins-empty-install")
                .text()
                .group("plugins-empty-install-btn")
                .accessibility_label(t!("plugins.install").to_string())
                .disabled(self.busy(cx) || self.applying_update.is_some())
                .on_click(cx.listener(|view, _, _, cx| view.begin_install(cx)))
        });
        pioneer_desktop_foundation::catalog_empty_state(
            t!("plugins.empty").to_string(),
            t!("plugins.empty_description").to_string(),
            install,
            cx,
        )
    }
}
