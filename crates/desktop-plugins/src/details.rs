use super::*;

pub(super) fn status_badge(status: &str, cx: &App) -> AnyElement {
    let color = match status {
        "active" | "ready" => cx.theme().success,
        "failed" | "error" => cx.theme().danger,
        "degraded" | "partial" | "interrupted" | "auth_required" | "authrequired" | "blocked" => {
            cx.theme().warning
        }
        _ => cx.theme().muted_foreground,
    };
    pioneer_desktop_foundation::catalog_status_badge(status_label(status), color)
        .flex_shrink_0()
        .into_any_element()
}

#[cfg(test)]
mod status_precedence_tests {
    use super::*;

    #[::core::prelude::v1::test]
    fn component_shows_runtime_state_after_installation_and_keeps_lifecycle_failures_visible() {
        for (lifecycle, runtime, expected) in [
            ("installed", Some("auth_required"), "auth_required"),
            ("installed", Some("blocked"), "blocked"),
            ("installed", Some("active"), "active"),
            ("installed", None, "installed"),
            ("installed", Some("degraded"), "degraded"),
            ("installed", Some("restarting"), "restarting"),
            ("installed", Some("stopping"), "stopping"),
            ("failed", Some("active"), "failed"),
            ("removed_by_user", Some("ready"), "removed_by_user"),
        ] {
            let component = PluginComponentItem {
                kind: "skill".into(),
                member_key: "component".into(),
                status: lifecycle.into(),
                runtime_status: runtime.map(str::to_owned),
                diagnostic: None,
                skill_id: None,
                mcp_installation_id: None,
            };
            assert_eq!(component_status(&component), expected);
        }
    }
}

fn component_status(component: &PluginComponentItem) -> &str {
    if component.status == "installed" {
        component
            .runtime_status
            .as_deref()
            .unwrap_or(&component.status)
    } else {
        &component.status
    }
}

impl PluginsView {
    pub(super) fn render_details(&self, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        let Some(plugin) = self.selected_plugin() else {
            return v_flex()
                .size_full()
                .bg(cx.theme().background)
                .p_6()
                .child(self.missing_parent_label())
                .into_any_element();
        };
        let manage = self
            .workspace
            .as_deref()
            .is_some_and(|workspace| self.client.plugins_management_allowed(workspace));
        let busy = self.busy(cx);
        let metadata = if let Some(version) = &plugin.version {
            t!(
                "plugins.details_metadata",
                version = version.as_str(),
                count = plugin.components.len()
            )
            .to_string()
        } else {
            t!(
                "plugins.details_metadata_no_version",
                count = plugin.components.len()
            )
            .to_string()
        };
        let header = v_flex()
            .pt_3()
            .px_6()
            .pb_5()
            .gap_2()
            .when_some(plugin.description.clone(), |view, description| {
                view.child(div().text_sm().text_color(cx.theme().muted_foreground).child(description))
            })
            .border_b_1()
            .border_color(cx.theme().border)
            .child(
                h_flex()
                    .w_full()
                    .min_w_0()
                    .items_start()
                    .gap_10()
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .text_base()
                            .font_semibold()
                            .child(plugin.name.clone()),
                    )
                    .child(status_badge(
                        if plugin.enabled || plugin.state != "installed" {
                            &plugin.status
                        } else {
                            "disabled"
                        },
                        cx,
                    )),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child(metadata),
            );
        let content = v_flex()
            .p_6()
            .gap_6()
            .when(self.upload.read(cx).active(), |view| {
                view.child(self.upload.clone())
            })
            .children(self.upload_recovery(cx))
            .when_some(self.installation_error(cx), |view, error| {
                view.child(div().text_sm().text_color(cx.theme().danger).child(error))
            })
            .when(self.connected() && !self.readable(), |view| {
                view.child(div().text_sm().child(self.access_label()))
            })
            .when(!self.connected(), |view| {
                view.child(div().text_sm().child(t!("plugins.offline").to_string()))
            })
            .when(self.input.error.is_some(), |view| {
                view.child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().danger)
                        .child(t!("plugins.error").to_string()),
                )
            })
            .when(plugin.state == "retained", |view| {
                view.child(
                    div()
                        .text_sm()
                        .child(t!("plugins.retained_notice").to_string()),
                )
            })
            .children(self.management_controls(plugin, manage, busy, window, cx))
            .when(
                plugin.components.iter().any(|component| {
                    matches!(
                        component_status(component),
                        "blocked" | "auth_required" | "authrequired"
                    )
                }),
                |view| {
                    view.child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(t!("plugins.setup_required").to_string()),
                    )
                },
            )
            .children(self.component_group(
                plugin,
                "mcp",
                t!("plugins.mcp_components").to_string(),
                manage,
                busy,
                cx,
            ))
            .children(self.component_group(
                plugin,
                "skill",
                t!("plugins.skill_components").to_string(),
                manage,
                busy,
                cx,
            ));
        v_flex()
            .size_full()
            .min_w_0()
            .min_h_0()
            .bg(cx.theme().background)
            .child(header)
            .child(
                v_flex()
                    .id("plugin-details-scroll")
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .overflow_y_scrollbar()
                    .child(content),
            )
            .into_any_element()
    }

    fn component_group(
        &self,
        plugin: &PluginItem,
        kind: &str,
        title: String,
        manage: bool,
        busy: bool,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        if !plugin
            .components
            .iter()
            .any(|component| component.kind == kind)
        {
            return None;
        }
        Some(
            v_flex()
                .gap_3()
                .child(div().text_base().font_semibold().child(title))
                .children(
                    plugin
                        .components
                        .iter()
                        .filter(|component| component.kind == kind)
                        .map(|component| {
                            let status = component_status(component);
                            v_flex()
                                .gap_2()
                                .py_3()
                                .border_b_1()
                                .border_color(cx.theme().border)
                                .child(
                                    h_flex()
                                        .min_w_0()
                                        .gap_3()
                                        .items_center()
                                        .flex_wrap()
                                        .child(
                                            div()
                                                .flex_1()
                                                .min_w_0()
                                                .text_sm()
                                                .font_medium()
                                                .child(component.member_key.clone()),
                                        )
                                        .child(status_badge(status, cx))
                                        .child(self.component_controls(
                                            plugin, component, manage, busy, cx,
                                        )),
                                )
                                .when(matches!(status, "auth_required" | "authrequired"), |row| {
                                    row.child(
                                        div()
                                            .text_sm()
                                            .text_color(cx.theme().muted_foreground)
                                            .child(t!("plugins.sign_in_help").to_string()),
                                    )
                                })
                                .when(status == "blocked" && component.kind == "skill", |row| {
                                    row.child(
                                        div()
                                            .text_sm()
                                            .text_color(cx.theme().muted_foreground)
                                            .child(t!("plugins.skill_access_help").to_string()),
                                    )
                                })
                                .when_some(component.diagnostic.as_deref(), |row, code| {
                                    row.child(
                                        div()
                                            .text_sm()
                                            .text_color(cx.theme().muted_foreground)
                                            .child(diagnostic_label(code)),
                                    )
                                })
                                .into_any_element()
                        }),
                )
                .into_any_element(),
        )
    }
}
