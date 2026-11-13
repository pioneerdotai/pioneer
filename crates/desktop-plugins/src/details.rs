use super::*;

pub(super) fn status_color(status: &str, cx: &App) -> Hsla {
    match status {
        "active" | "ready" => cx.theme().success,
        "failed" | "error" => cx.theme().danger,
        "degraded" | "partial" | "interrupted" | "auth_required" | "authrequired" | "blocked" => {
            cx.theme().warning
        }
        _ => cx.theme().muted_foreground,
    }
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

// Parent operations take precedence over component runtime state. A healthy
// plugin has no header badge; component lifecycle failures retain precedence
// over a stale successful runtime status through `component_status`.
pub(super) fn summary_status(plugin: &PluginItem) -> Option<&str> {
    if plugin.state != "installed" {
        return Some(&plugin.state);
    }
    if matches!(
        plugin.status.as_str(),
        "updating" | "removing" | "installing" | "interrupted" | "retained"
    ) {
        return Some(&plugin.status);
    }
    if !plugin.enabled {
        return Some("disabled");
    }

    std::iter::once(plugin.status.as_str())
        .chain(plugin.components.iter().map(component_status))
        .filter(|status| !matches!(*status, "installed" | "active" | "ready"))
        .map(|status| match status {
            "disabled" | "removed_by_user" | "invalid" => "partial",
            _ => status,
        })
        .min_by_key(|status| match *status {
            "failed" | "error" => 0,
            "auth_required" | "authrequired" => 1,
            "blocked" => 2,
            "partial" | "degraded" => 3,
            "unavailable" | "offline" | "stopped" | "notstarted" | "not_started" => 4,
            "starting" | "restarting" | "stopping" => 6,
            _ => 5,
        })
}

impl PluginsView {
    fn skill_component_card(
        &self,
        component: &PluginComponentItem,
        busy: bool,
        cx: &mut App,
    ) -> pioneer_desktop_foundation::CatalogCard {
        use pioneer_client::skills::presentation::{self, SkillDiagnosticsTone, SkillStatus};
        let skill = self
            .input
            .skills
            .iter()
            .find(|skill| component.skill_id.as_ref() == Some(&skill.skill_id));
        let card = if let Some(skill) = skill {
            let summary = presentation::skill_summary_presentation(skill);
            let label = match summary.status {
                SkillStatus::Active => t!("plugins.skill_status.active").to_string(),
                SkillStatus::Blocked => t!("plugins.skill_status.blocked").to_string(),
                SkillStatus::Disabled => t!("plugins.skill_status.disabled").to_string(),
                SkillStatus::Other(value) => value,
            };
            let color = match summary.status_tone {
                SkillDiagnosticsTone::Success => cx.theme().success,
                SkillDiagnosticsTone::Warning => cx.theme().warning,
                _ => cx.theme().foreground,
            };
            pioneer_desktop_foundation::CatalogCard::new(skill.slug.clone())
                .when_some(summary.slug.owner, |card, owner| card.owner(owner))
                .description(skill.description.clone())
                .metadata(format!(
                    "{} {}",
                    t!("plugins.version"),
                    summary.version.as_deref().unwrap_or("-")
                ))
                .status(label, color)
        } else {
            pioneer_desktop_foundation::CatalogCard::new(component.member_key.clone())
                .metadata(format!("{} -", t!("plugins.version")))
                .status(
                    status_label(component_status(component)),
                    status_color(component_status(component), cx),
                )
        };
        card.pending(busy)
            .when(component.status != "installed", |card| {
                card.status(
                    status_label(component_status(component)),
                    status_color(component_status(component), cx),
                )
            })
    }

    fn mcp_component_card(
        &self,
        component: &PluginComponentItem,
        busy: bool,
        cx: &mut App,
    ) -> pioneer_desktop_foundation::CatalogSummaryCard {
        use pioneer_client::mcp::presentation::{
            self, McpCapabilityKind, McpPresentationTone, McpStatusLabel,
        };
        let server = self
            .input
            .mcp
            .iter()
            .find(|server| component.mcp_installation_id.as_ref() == Some(&server.id));
        let card = if let Some(server) = server {
            let labels = presentation::mcp_capability_counts(server)
                .iter()
                .map(|count| {
                    let label = match count.kind {
                        McpCapabilityKind::Tools => t!("plugins.mcp_capabilities.tools"),
                        McpCapabilityKind::Resources => t!("plugins.mcp_capabilities.resources"),
                        McpCapabilityKind::ResourceTemplates => {
                            t!("plugins.mcp_capabilities.templates")
                        }
                        McpCapabilityKind::Prompts => t!("plugins.mcp_capabilities.prompts"),
                    };
                    format!("{} {label}", count.count)
                })
                .collect();
            let label = match presentation::mcp_status_label(server.status) {
                McpStatusLabel::NotStarted => t!("plugins.mcp_status.not_started"),
                McpStatusLabel::Disabled => t!("plugins.mcp_status.disabled"),
                McpStatusLabel::Starting => t!("plugins.mcp_status.starting"),
                McpStatusLabel::Ready => t!("plugins.mcp_status.ready"),
                McpStatusLabel::Degraded => t!("plugins.mcp_status.degraded"),
                McpStatusLabel::AuthRequired => t!("plugins.mcp_status.auth_required"),
                McpStatusLabel::Failed => t!("plugins.mcp_status.failed"),
                McpStatusLabel::Stopping => t!("plugins.mcp_status.stopping"),
                McpStatusLabel::Stopped => t!("plugins.mcp_status.stopped"),
                McpStatusLabel::Restarting => t!("plugins.mcp_status.restarting"),
            }
            .to_string();
            let color = match presentation::mcp_status_tone(server.status) {
                McpPresentationTone::Success => cx.theme().success,
                McpPresentationTone::Warning => cx.theme().warning,
                McpPresentationTone::Danger => cx.theme().danger,
                McpPresentationTone::Muted => cx.theme().muted_foreground,
                McpPresentationTone::Default => cx.theme().foreground.opacity(0.84),
            };
            pioneer_desktop_foundation::CatalogSummaryCard::new(
                presentation::mcp_display_name(server),
                labels,
                label,
                color,
            )
        } else {
            pioneer_desktop_foundation::CatalogSummaryCard::new(
                component.member_key.clone(),
                vec![],
                status_label(component_status(component)),
                status_color(component_status(component), cx),
            )
        };
        card.pending(busy)
            .when(component.status != "installed", |card| {
                card.status(
                    status_label(component_status(component)),
                    status_color(component_status(component), cx),
                )
            })
    }

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

        let header = pioneer_desktop_foundation::CatalogDetailsHeader::new(plugin.name.clone())
            .icon(
                self.plugin_images
                    .element(plugin.metadata.logo.as_ref())
                    .size_10(),
            )
            .when_some(summary_status(plugin), |header, status| {
                header.status(status_label(status), status_color(status, cx))
            })
            .when_some(plugin.description.clone(), |header, description| {
                header.description(description)
            })
            .when(manage, |header| {
                header.actions(
                    h_flex()
                        .items_center()
                        .gap_2()
                        .child(
                            Button::new("plugin-header-enabled")
                                .xsmall()
                                .compact()
                                .h_6()
                                .px_3()
                                .when(plugin.enabled, |button| button.primary())
                                .when(!plugin.enabled, |button| button.outline())
                                .disabled(busy || plugin.state != "installed")
                                .label(t!("plugins.enabled").to_string())
                                .on_click(cx.listener({
                                    let plugin = plugin.clone();
                                    move |view, _, _, cx| view.set_enabled(plugin.clone(), cx)
                                })),
                        )
                        .child(
                            Button::new("plugin-header-implicit")
                                .xsmall()
                                .compact()
                                .h_6()
                                .px_3()
                                .when(plugin.allow_implicit_invocation, |button| button.primary())
                                .when(!plugin.allow_implicit_invocation, |button| button.outline())
                                .disabled(busy || plugin.state != "installed")
                                .label(t!("plugins.implicit").to_string())
                                .on_click(cx.listener({
                                    let plugin = plugin.clone();
                                    move |view, _, _, cx| view.set_implicit(plugin.clone(), cx)
                                })),
                        ),
                )
            });
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
                            let card = match component.kind.as_str() {
                                "skill" => self
                                    .skill_component_card(component, busy, cx)
                                    .into_any_element(),
                                "mcp" => self
                                    .mcp_component_card(component, busy, cx)
                                    .into_any_element(),
                                _ => unreachable!("component groups contain only skills and MCP"),
                            };
                            let can_open = component.skill_id.is_some()
                                || component.mcp_installation_id.is_some();
                            let card = div()
                                .id(SharedString::from(format!(
                                    "plugin-card-{}-{}-{}",
                                    plugin.id, component.kind, component.member_key
                                )))
                                .w_full()
                                .when(can_open, |card| {
                                    let parent = plugin.clone();
                                    let component = component.clone();
                                    card.cursor_pointer().on_click(cx.listener(
                                        move |view, _, window, cx| {
                                            view.open_component(&parent, &component, window, cx)
                                        },
                                    ))
                                })
                                .child(card);
                            v_flex()
                                .gap_2()
                                .child(card)
                                .when(
                                    manage
                                        && matches!(
                                            component.status.as_str(),
                                            "failed" | "removed_by_user"
                                        )
                                        && plugin.state == "installed"
                                        && component.diagnostic.as_deref()
                                            != Some("component_path_denied"),
                                    |row| {
                                        row.child(self.component_controls(
                                            plugin, component, manage, busy, cx,
                                        ))
                                    },
                                )
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
