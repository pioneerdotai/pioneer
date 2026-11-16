use crate::catalog::McpCatalogView;
use gpui_kit::component::{switch::Switch, *};
use gpui_kit::{prelude::*, *};
use pioneer_desktop_foundation::{
    settings_divider, settings_group, settings_row, settings_section,
};

impl McpCatalogView {
    pub(crate) fn render_mcp_policy_settings(
        &self,
        server: &pioneer_client::mcp::types::McpListItem,
        cx: &Context<Self>,
    ) -> AnyElement {
        let disabled = !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
            || self.is_mcp_pending(server.id.as_str());
        let entity = cx.entity().clone();
        settings_section(
            t!("mcp.settings.title").to_string(),
            settings_group(cx)
                .child(settings_row(
                    t!("mcp.details.enabled").to_string(),
                    t!("mcp.settings.enabled_description").to_string(),
                    Switch::new(self.ui_id(server.id.as_str(), "settings-enabled"))
                        .accessibility_label(t!("mcp.details.enabled").to_string())
                        .checked(server.policy.enabled)
                        .disabled(disabled)
                        .on_click({
                            let entity = entity.clone();
                            let id = server.id.clone();
                            let implicit = server.policy.allow_implicit_invocation;
                            move |enabled, _, cx| {
                                let _ = entity.update(cx, |view, cx| {
                                    view.set_mcp_policy(id.clone(), *enabled, implicit, cx);
                                    cx.notify();
                                });
                            }
                        }),
                ))
                .child(settings_divider(cx))
                .child(settings_row(
                    t!("mcp.details.implicit").to_string(),
                    t!("mcp.settings.implicit_description").to_string(),
                    Switch::new(self.ui_id(server.id.as_str(), "settings-implicit"))
                        .accessibility_label(t!("mcp.details.implicit").to_string())
                        .checked(server.policy.allow_implicit_invocation)
                        .disabled(disabled)
                        .on_click({
                            let id = server.id.clone();
                            let enabled = server.policy.enabled;
                            move |implicit, _, cx| {
                                let _ = entity.update(cx, |view, cx| {
                                    view.set_mcp_policy(id.clone(), enabled, *implicit, cx);
                                    cx.notify();
                                });
                            }
                        }),
                )),
        )
        .into_any_element()
    }
}
