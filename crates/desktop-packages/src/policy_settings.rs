use super::*;
use gpui_kit::component::switch::Switch;

impl PackageCatalogView {
    pub(super) fn render_policy_settings(
        &self,
        plugin: &PluginItem,
        manage: bool,
        busy: bool,
        cx: &Context<Self>,
    ) -> AnyElement {
        use pioneer_desktop_foundation::{
            settings_divider, settings_group, settings_row, settings_section,
        };
        let disabled = !manage || busy || plugin.state != "installed";
        settings_section(
            t!(if self.kind == pioneer_client::plugins::PackageKind::App {
                "apps.details_settings.title"
            } else {
                "plugins.details_settings.title"
            })
            .to_string(),
            settings_group(cx)
                .child(settings_row(
                    t!("plugins.enabled").to_string(),
                    t!(if self.kind == pioneer_client::plugins::PackageKind::App {
                        "apps.details_settings.enabled_description"
                    } else {
                        "plugins.details_settings.enabled_description"
                    })
                    .to_string(),
                    Switch::new("plugin-settings-enabled")
                        .accessibility_label(t!("plugins.enabled").to_string())
                        .checked(plugin.enabled)
                        .disabled(disabled)
                        .on_click(cx.listener({
                            let plugin = plugin.clone();
                            move |view, _, _, cx| view.set_enabled(plugin.clone(), cx)
                        })),
                ))
                .child(settings_divider(cx))
                .child(settings_row(
                    t!("plugins.implicit").to_string(),
                    t!(if self.kind == pioneer_client::plugins::PackageKind::App {
                        "apps.details_settings.implicit_description"
                    } else {
                        "plugins.details_settings.implicit_description"
                    })
                    .to_string(),
                    Switch::new("plugin-settings-implicit")
                        .accessibility_label(t!("plugins.implicit").to_string())
                        .checked(plugin.allow_implicit_invocation)
                        .disabled(disabled)
                        .on_click(cx.listener({
                            let plugin = plugin.clone();
                            move |view, _, _, cx| view.set_implicit(plugin.clone(), cx)
                        })),
                )),
        )
        .into_any_element()
    }
}
