use crate::app::{
    root::PioneerDesktop,
    skills::details::table::{
        SkillDiagnosticsTableCell, SkillDiagnosticsTableColumn, SkillDiagnosticsTableDelegate,
        SkillDiagnosticsTableModel, SkillDiagnosticsTableRow, SkillDiagnosticsTone,
    },
};
use chrono::{Local, TimeZone};
use gpui::{prelude::*, *};
use gpui_component::{
    button::*,
    collapsible::Collapsible,
    divider::Divider,
    scroll::ScrollableElement,
    table::{Table, TableState},
    theme::ActiveTheme,
    StyledExt, *,
};
use pioneer_protocol::{
    McpAuditEventSummary, McpListItem, McpPromptCatalogItem, McpResourceCatalogItem,
    McpResourceTemplateCatalogItem, McpServerDetailsResponse, McpToolCatalogItem,
};

impl PioneerDesktop {
    pub(crate) fn render_mcp_details(&self, window: &Window, cx: &mut Context<Self>) -> AnyElement {
        let selected = self.mcp_selected_server_id.as_ref().and_then(|server_id| {
            self.mcp_servers
                .iter()
                .find(|server| server.id == *server_id)
                .cloned()
        });
        let details = self.mcp_server_details.as_ref();
        let server = details.map(|details| details.server.clone()).or(selected);

        let Some(server) = server else {
            return v_flex()
                .size_full()
                .bg(cx.theme().background)
                .items_center()
                .justify_center()
                .child(
                    div()
                        .text_sm()
                        .opacity(0.65)
                        .child(t!("mcp.details.not_selected").to_string()),
                )
                .into_any_element();
        };

        match details {
            Some(details) => self.sync_mcp_details_tables(details.audit.as_slice(), cx),
            None => self.sync_mcp_details_tables(&[], cx),
        }

        let is_pending = self.is_mcp_pending(server.name.as_str());
        let desktop_entity = cx.entity().clone();
        let status_color = Self::mcp_status_color(server.status, cx);
        let meta_grid_columns = self.mcp_details_meta_grid_columns(window);

        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .child(
                v_flex()
                    .pt_3()
                    .px_6()
                    .pb_5()
                    .gap_2()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        h_flex()
                            .justify_between()
                            .items_start()
                            .gap_10()
                            .child(
                                div().text_base().font_semibold().child(
                                    server
                                        .display_name
                                        .clone()
                                        .unwrap_or_else(|| server.name.clone()),
                                ),
                            )
                            .child(
                                div()
                                    .mt_1p5()
                                    .px_2()
                                    .py_0p5()
                                    .rounded_full()
                                    .border_1()
                                    .border_color(status_color)
                                    .text_xs()
                                    .text_color(status_color)
                                    .font_medium()
                                    .child(Self::mcp_status_label(server.status)),
                            ),
                    )
                    .child(
                        h_flex()
                            .items_center()
                            .gap_2()
                            .child(
                                Button::new("mcp-screen-enabled")
                                    .xsmall()
                                    .compact()
                                    .h_6()
                                    .px_3()
                                    .when(server.policy.enabled, |this| this.primary())
                                    .when(!server.policy.enabled, |this| this.outline())
                                    .disabled(is_pending)
                                    .label(t!("mcp.details.enabled").to_string())
                                    .on_click({
                                        let desktop_entity = desktop_entity.clone();
                                        let name = server.name.clone();
                                        let next_enabled = !server.policy.enabled;
                                        let implicit = server.policy.allow_implicit_invocation;
                                        move |_, _, cx| {
                                            let _ = desktop_entity.update(cx, |view, cx| {
                                                view.set_mcp_policy(
                                                    name.clone(),
                                                    next_enabled,
                                                    implicit,
                                                    cx,
                                                );
                                                cx.notify();
                                            });
                                        }
                                    }),
                            )
                            .child(
                                Button::new("mcp-screen-implicit")
                                    .xsmall()
                                    .compact()
                                    .h_6()
                                    .px_3()
                                    .when(server.policy.allow_implicit_invocation, |this| {
                                        this.primary()
                                    })
                                    .when(!server.policy.allow_implicit_invocation, |this| {
                                        this.outline()
                                    })
                                    .disabled(is_pending)
                                    .label(t!("mcp.details.implicit").to_string())
                                    .on_click({
                                        let desktop_entity = desktop_entity.clone();
                                        let name = server.name.clone();
                                        let enabled = server.policy.enabled;
                                        let next_implicit =
                                            !server.policy.allow_implicit_invocation;
                                        move |_, _, cx| {
                                            let _ = desktop_entity.update(cx, |view, cx| {
                                                view.set_mcp_policy(
                                                    name.clone(),
                                                    enabled,
                                                    next_implicit,
                                                    cx,
                                                );
                                                cx.notify();
                                            });
                                        }
                                    }),
                            ),
                    ),
            )
            .child(
                v_flex()
                    .id("mcp-detail-scroll")
                    .flex_1()
                    .overflow_y_scroll()
                    .p_6()
                    .gap_6()
                    .when_some(self.mcp_error.clone(), |this, error| {
                        this.child(detail_error_block(error, cx))
                    })
                    .child(self.render_mcp_overview_section(&server, details, meta_grid_columns))
                    .child(Divider::horizontal())
                    .child(self.render_mcp_health_section(&server, details, meta_grid_columns, cx))
                    .child(Divider::horizontal())
                    .child(self.render_mcp_tools_section(details, cx))
                    .child(Divider::horizontal())
                    .child(self.render_mcp_resources_section(details, cx))
                    .child(Divider::horizontal())
                    .child(self.render_mcp_resource_templates_section(details, cx))
                    .child(Divider::horizontal())
                    .child(self.render_mcp_prompts_section(details, cx))
                    .child(Divider::horizontal())
                    .child(self.render_mcp_audit_section(details, cx)),
            )
            .into_any_element()
    }

    fn mcp_details_meta_grid_columns(&self, window: &Window) -> u16 {
        let viewport_width = window.viewport_size().width;
        let sidebar_width = if self.show_sidebar {
            self.sidebar_panel_width
        } else {
            px(0.)
        };

        let content_padding_x = px(48.);
        let available_width = (viewport_width - sidebar_width - content_padding_x).max(px(0.));

        for columns in (2..=4).rev() {
            let columns_f = columns as f32;
            let required_width = px(columns_f * 172. + (columns_f - 1.) * 12.);
            if available_width >= required_width {
                return columns;
            }
        }

        2
    }

    fn is_mcp_details_section_expanded(&self, section_id: &str) -> bool {
        self.mcp_details_expanded_sections.contains(section_id)
    }

    fn toggle_mcp_details_section(&mut self, section_id: &str, cx: &mut Context<Self>) {
        if !self.mcp_details_expanded_sections.remove(section_id) {
            self.mcp_details_expanded_sections
                .insert(section_id.to_owned());
        }
        cx.notify();
    }

    fn render_collapsible_mcp_details_section(
        &self,
        section_id: &'static str,
        title: String,
        subtitle: Option<String>,
        content: Option<AnyElement>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let has_content = content.is_some();
        let open = has_content && self.is_mcp_details_section_expanded(section_id);
        let toggle_id = Self::mcp_details_element_hash(&["mcp-section-toggle", section_id]);
        let icon_name = if open {
            IconName::ChevronUp
        } else {
            IconName::ChevronDown
        };

        let header = h_flex()
            .w_full()
            .items_center()
            .justify_between()
            .gap_3()
            .child(
                v_flex()
                    .flex_1()
                    .gap_2()
                    .child(
                        div()
                            .text_sm()
                            .font_semibold()
                            .line_height(relative(1.))
                            .child(title),
                    )
                    .when_some(subtitle, |this, subtitle| {
                        this.child(
                            div()
                                .text_xs()
                                .opacity(0.6)
                                .line_height(relative(1.))
                                .child(subtitle),
                        )
                    }),
            )
            .when(has_content, |this| {
                this.child(
                    div()
                        .id(("mcp-section-toggle", toggle_id))
                        .flex()
                        .mt_1()
                        .opacity(0.6)
                        .hover(|this| this.opacity(0.85))
                        .child(Icon::new(icon_name).size_4()),
                )
            })
            .when(has_content, |this| {
                this.cursor_pointer().on_mouse_down(MouseButton::Left, {
                    let section_id = section_id;
                    cx.listener(move |view, _, _, cx| {
                        view.toggle_mcp_details_section(section_id, cx);
                    })
                })
            });

        if let Some(content) = content {
            Collapsible::new()
                .w_full()
                .gap_3()
                .open(open)
                .child(header)
                .content(content)
                .into_any_element()
        } else {
            header.into_any_element()
        }
    }

    fn render_mcp_overview_section(
        &self,
        server: &McpListItem,
        details: Option<&McpServerDetailsResponse>,
        grid_columns: u16,
    ) -> AnyElement {
        let generated_at = details
            .map(|details| Self::format_mcp_time(details.generated_at))
            .unwrap_or_else(|| "-".to_owned());
        let source_label = match server.source_kind {
            pioneer_protocol::McpSourceKind::Config => t!("mcp.details.source_config").to_string(),
        };
        let scope_label = match server.scope {
            pioneer_protocol::McpScopeKind::Workspace => {
                t!("mcp.details.scope_workspace").to_string()
            }
            pioneer_protocol::McpScopeKind::User => t!("mcp.details.scope_user").to_string(),
        };

        div()
            .w_full()
            .grid()
            .grid_cols(grid_columns)
            .gap_4()
            .child(meta_item(
                source_label,
                t!("mcp.details.source").to_string(),
            ))
            .child(meta_item(scope_label, t!("mcp.details.scope").to_string()))
            .child(meta_item(
                Self::mcp_transport_label(&server.transport),
                t!("mcp.details.transport").to_string(),
            ))
            .child(meta_item(
                generated_at,
                t!("mcp.details.loaded").to_string(),
            ))
            .into_any_element()
    }

    fn render_mcp_health_section(
        &self,
        server: &McpListItem,
        details: Option<&McpServerDetailsResponse>,
        grid_columns: u16,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let health = details.map(|details| &details.health);
        let retry_attempt = health
            .and_then(|health| health.retry_attempt)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_owned());
        let next_retry_at = health
            .and_then(|health| health.next_retry_at)
            .map(Self::format_mcp_time)
            .unwrap_or_else(|| "-".to_owned());
        let last_seen_at = server
            .runtime
            .last_seen_at
            .map(Self::format_mcp_time)
            .unwrap_or_else(|| "-".to_owned());
        let last_error = server
            .runtime
            .last_error
            .clone()
            .or_else(|| health.and_then(|health| health.last_error.clone()))
            .unwrap_or_else(|| "-".to_owned());

        let content = v_flex()
            .gap_4()
            .child(
                div()
                    .w_full()
                    .grid()
                    .grid_cols(grid_columns)
                    .gap_4()
                    .child(meta_item(
                        Self::mcp_runtime_label(server.runtime.state),
                        t!("mcp.details.runtime").to_string(),
                    ))
                    .child(meta_item(
                        Self::mcp_policy_summary(server.runtime.live),
                        t!("mcp.details.live").to_string(),
                    ))
                    .child(meta_item(
                        last_seen_at,
                        t!("mcp.details.last_seen").to_string(),
                    ))
                    .child(meta_item(
                        retry_attempt,
                        t!("mcp.details.retry_attempt").to_string(),
                    ))
                    .child(meta_item(
                        next_retry_at,
                        t!("mcp.details.next_retry").to_string(),
                    ))
                    .child(meta_item(
                        last_error,
                        t!("mcp.details.last_error").to_string(),
                    )),
            )
            .when_some(
                health.and_then(|health| health.stderr_tail.clone()),
                |this, tail| {
                    this.child(catalog_json_text_block(
                        t!("mcp.details.stderr").to_string(),
                        tail,
                        cx,
                    ))
                },
            )
            .into_any_element();

        self.render_collapsible_mcp_details_section(
            "health",
            t!("mcp.details.health").to_string(),
            None,
            Some(content),
            cx,
        )
    }

    fn render_mcp_tools_section(
        &self,
        details: Option<&McpServerDetailsResponse>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let tools = details
            .map(|details| details.catalog.tools.as_slice())
            .unwrap_or(&[]);
        let content = if tools.is_empty() {
            None
        } else {
            Some(
                h_flex()
                    .w_full()
                    .flex_wrap()
                    .items_center()
                    .gap_2()
                    .children(
                        tools
                            .iter()
                            .enumerate()
                            .map(|(ix, tool)| render_tool_card(tool, ix, cx)),
                    )
                    .into_any_element(),
            )
        };

        self.render_collapsible_mcp_details_section(
            "tools",
            t!("mcp.details.tools").to_string(),
            Some(t!("mcp.details.tools_count", count = tools.len()).to_string()),
            content,
            cx,
        )
    }

    fn render_mcp_resources_section(
        &self,
        details: Option<&McpServerDetailsResponse>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let resources = details
            .map(|details| details.catalog.resources.as_slice())
            .unwrap_or(&[]);
        let content = if resources.is_empty() {
            None
        } else {
            Some(
                h_flex()
                    .w_full()
                    .flex_wrap()
                    .items_center()
                    .gap_2()
                    .children(
                        resources
                            .iter()
                            .enumerate()
                            .map(|(ix, resource)| render_resource_card(resource, ix, cx)),
                    )
                    .into_any_element(),
            )
        };

        self.render_collapsible_mcp_details_section(
            "resources",
            t!("mcp.details.resources").to_string(),
            Some(t!("mcp.details.resources_count", count = resources.len()).to_string()),
            content,
            cx,
        )
    }

    fn render_mcp_resource_templates_section(
        &self,
        details: Option<&McpServerDetailsResponse>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let templates = details
            .map(|details| details.catalog.resource_templates.as_slice())
            .unwrap_or(&[]);
        let content = if templates.is_empty() {
            None
        } else {
            Some(
                h_flex()
                    .w_full()
                    .flex_wrap()
                    .items_center()
                    .gap_2()
                    .children(
                        templates
                            .iter()
                            .enumerate()
                            .map(|(ix, template)| render_resource_template_card(template, ix, cx)),
                    )
                    .into_any_element(),
            )
        };

        self.render_collapsible_mcp_details_section(
            "resource-templates",
            t!("mcp.details.resource_templates").to_string(),
            Some(t!("mcp.details.templates_count", count = templates.len()).to_string()),
            content,
            cx,
        )
    }

    fn render_mcp_prompts_section(
        &self,
        details: Option<&McpServerDetailsResponse>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let prompts = details
            .map(|details| details.catalog.prompts.as_slice())
            .unwrap_or(&[]);
        let content = if prompts.is_empty() {
            None
        } else {
            Some(
                h_flex()
                    .w_full()
                    .flex_wrap()
                    .items_center()
                    .gap_2()
                    .children(
                        prompts
                            .iter()
                            .enumerate()
                            .map(|(ix, prompt)| render_prompt_card(prompt, ix, cx)),
                    )
                    .into_any_element(),
            )
        };

        self.render_collapsible_mcp_details_section(
            "prompts",
            t!("mcp.details.prompts").to_string(),
            Some(t!("mcp.details.prompts_count", count = prompts.len()).to_string()),
            content,
            cx,
        )
    }

    fn render_mcp_audit_section(
        &self,
        details: Option<&McpServerDetailsResponse>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let audit = details
            .map(|details| details.audit.as_slice())
            .unwrap_or(&[]);
        let content = if audit.is_empty() {
            None
        } else {
            Some(self.render_mcp_diagnostics_table(&self.mcp_audit_table_state, audit.len().min(8)))
        };

        self.render_collapsible_mcp_details_section(
            "audit",
            t!("mcp.details.audit").to_string(),
            Some(t!("mcp.details.audit_count", count = audit.len()).to_string()),
            content,
            cx,
        )
    }

    fn sync_mcp_details_tables(&self, audit: &[McpAuditEventSummary], cx: &mut Context<Self>) {
        self.sync_mcp_diagnostics_table_state(
            &self.mcp_audit_table_state,
            Self::build_mcp_audit_table_model(audit),
            cx,
        );
    }

    fn sync_mcp_diagnostics_table_state(
        &self,
        state_entity: &Entity<TableState<SkillDiagnosticsTableDelegate>>,
        model: SkillDiagnosticsTableModel,
        cx: &mut Context<Self>,
    ) {
        let _ = state_entity.update(cx, |state, cx| {
            if state.delegate().model() != &model {
                state.delegate_mut().set_model(model);
                if !state.delegate().model().rows.is_empty() {
                    state.set_selected_row(0, cx);
                }
                state.clear_selection(cx);
                state.refresh(cx);
            }
        });
    }

    fn render_mcp_diagnostics_table(
        &self,
        state_entity: &Entity<TableState<SkillDiagnosticsTableDelegate>>,
        row_count: usize,
    ) -> AnyElement {
        let visible_rows = row_count.clamp(1, 8) as f32;
        let row_height = gpui_component::Size::Small.table_row_height();
        let table_height = row_height * (visible_rows + 1.) + px(2.);

        div()
            .w_full()
            .h(table_height)
            .child(
                Table::new(state_entity)
                    .with_size(gpui_component::Size::Small)
                    .scrollbar_visible(true, false),
            )
            .into_any_element()
    }

    fn build_mcp_audit_table_model(audit: &[McpAuditEventSummary]) -> SkillDiagnosticsTableModel {
        let columns = vec![
            SkillDiagnosticsTableColumn {
                key: "time",
                title: t!("mcp.audit.column_time").to_string(),
                hint: t!("mcp.audit.column_time_hint").to_string(),
                width: px(176.),
            },
            SkillDiagnosticsTableColumn {
                key: "event",
                title: t!("mcp.audit.column_event").to_string(),
                hint: t!("mcp.audit.column_event_hint").to_string(),
                width: px(220.),
            },
            SkillDiagnosticsTableColumn {
                key: "result",
                title: t!("mcp.audit.column_result").to_string(),
                hint: t!("mcp.audit.column_result_hint").to_string(),
                width: px(120.),
            },
            SkillDiagnosticsTableColumn {
                key: "details",
                title: t!("mcp.audit.column_details").to_string(),
                hint: t!("mcp.audit.column_details_hint").to_string(),
                width: px(620.),
            },
        ];

        let rows = audit
            .iter()
            .take(8)
            .map(|item| {
                let event_time = Self::format_mcp_time(item.created_at);
                let action = Self::mcp_audit_action_label(item.action.as_str());
                let event_label = item
                    .raw_tool_name
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .map(|tool| format!("{action} / {tool}"))
                    .unwrap_or_else(|| action.clone());
                let decision = Self::mcp_audit_decision_label(item.decision.as_str());
                let reason = item
                    .reason_code
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_owned);
                let reason_label = reason
                    .clone()
                    .unwrap_or_else(|| t!("mcp.common.none").to_string());
                let details_summary = Self::summarize_mcp_audit_details(&item.details);
                let details = if reason.is_none() {
                    details_summary.clone()
                } else {
                    format!("{reason_label} | {details_summary}")
                };
                let details_tooltip = t!(
                    "mcp.audit.reason_tooltip",
                    details = details_summary.as_str(),
                    reason = reason_label.as_str()
                )
                .to_string();

                SkillDiagnosticsTableRow {
                    cells: vec![
                        SkillDiagnosticsTableCell {
                            text: event_time.clone(),
                            tooltip: Some(event_time),
                            tone: SkillDiagnosticsTone::Default,
                        },
                        SkillDiagnosticsTableCell {
                            text: event_label.clone(),
                            tooltip: Some(event_label),
                            tone: SkillDiagnosticsTone::Default,
                        },
                        SkillDiagnosticsTableCell {
                            text: decision.clone(),
                            tooltip: Some(decision),
                            tone: Self::mcp_audit_decision_tone(item.decision.as_str()),
                        },
                        SkillDiagnosticsTableCell {
                            text: details.clone(),
                            tooltip: Some(details_tooltip),
                            tone: SkillDiagnosticsTone::Muted,
                        },
                    ],
                }
            })
            .collect::<Vec<_>>();

        SkillDiagnosticsTableModel { columns, rows }
    }

    fn mcp_policy_summary(enabled: bool) -> String {
        if enabled {
            t!("mcp.common.yes").to_string()
        } else {
            t!("mcp.common.no").to_string()
        }
    }

    fn mcp_audit_action_label(action: &str) -> String {
        match action.trim() {
            "install" => t!("mcp.audit.action_install").to_string(),
            "update" => t!("mcp.audit.action_update").to_string(),
            "uninstall" => t!("mcp.audit.action_uninstall").to_string(),
            "policy" => t!("mcp.audit.action_policy").to_string(),
            "start" => t!("mcp.audit.action_start").to_string(),
            "started" => t!("mcp.audit.action_started").to_string(),
            "start_failed" => t!("mcp.audit.action_start_failed").to_string(),
            "stop" => t!("mcp.audit.action_stop").to_string(),
            "stopped" => t!("mcp.audit.action_stopped").to_string(),
            "restart" => t!("mcp.audit.action_restart").to_string(),
            "catalog_refreshed" => t!("mcp.audit.action_catalog_refreshed").to_string(),
            "call" => t!("mcp.audit.action_call").to_string(),
            "call_completed" => t!("mcp.audit.action_call_completed").to_string(),
            "call_failed" => t!("mcp.audit.action_call_failed").to_string(),
            other if !other.is_empty() => other.to_owned(),
            _ => t!("mcp.common.none").to_string(),
        }
    }

    fn mcp_audit_decision_label(decision: &str) -> String {
        match decision.trim() {
            "allowed" => t!("mcp.audit.decision_allowed").to_string(),
            "blocked" => t!("mcp.audit.decision_blocked").to_string(),
            "warning" => t!("mcp.audit.decision_warning").to_string(),
            other if !other.is_empty() => other.to_owned(),
            _ => t!("mcp.common.none").to_string(),
        }
    }

    fn mcp_audit_decision_tone(decision: &str) -> SkillDiagnosticsTone {
        match decision.trim() {
            "allowed" => SkillDiagnosticsTone::Success,
            "blocked" => SkillDiagnosticsTone::Danger,
            "warning" => SkillDiagnosticsTone::Warning,
            _ => SkillDiagnosticsTone::Muted,
        }
    }

    fn format_mcp_time(created_at: i64) -> String {
        let datetime = if created_at > 10_000_000_000 {
            Local.timestamp_millis_opt(created_at).single()
        } else {
            Local.timestamp_opt(created_at, 0).single()
        };

        datetime
            .map(|value| value.format("%d.%m.%Y %H:%M").to_string())
            .unwrap_or_else(|| "-".to_owned())
    }

    fn summarize_mcp_audit_details(details: &serde_json::Value) -> String {
        match details {
            serde_json::Value::Null => t!("mcp.common.empty").to_string(),
            serde_json::Value::Object(map) => {
                if map.is_empty() {
                    return t!("mcp.common.empty").to_string();
                }

                map.iter()
                    .take(2)
                    .map(|(key, value)| format!("{key}: {}", Self::mcp_json_value_preview(value)))
                    .collect::<Vec<_>>()
                    .join(" | ")
            }
            serde_json::Value::Array(values) => {
                t!("mcp.common.items_count", count = values.len()).to_string()
            }
            other => Self::mcp_json_value_preview(other),
        }
    }

    fn mcp_json_value_preview(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(value) => Self::truncate_for_mcp_table(value, 48),
            serde_json::Value::Bool(value) => value.to_string(),
            serde_json::Value::Number(value) => value.to_string(),
            serde_json::Value::Null => t!("mcp.common.none").to_string(),
            serde_json::Value::Array(values) => {
                if values.is_empty() {
                    "[]".to_owned()
                } else {
                    format!("[{}]", values.len())
                }
            }
            serde_json::Value::Object(map) => {
                if map.is_empty() {
                    "{}".to_owned()
                } else {
                    t!("mcp.common.keys_count", count = map.len()).to_string()
                }
            }
        }
    }

    fn truncate_for_mcp_table(value: &str, max_chars: usize) -> String {
        if value.chars().count() <= max_chars {
            return value.to_owned();
        }

        let shortened = value.chars().take(max_chars).collect::<String>();
        format!("{shortened}...")
    }

    fn mcp_details_element_hash(parts: &[&str]) -> u64 {
        use std::hash::{Hash, Hasher};

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for part in parts {
            part.hash(&mut hasher);
        }
        hasher.finish()
    }
}

fn meta_item(value: String, title: String) -> AnyElement {
    v_flex()
        .w_full()
        .gap_0p5()
        .child(
            div()
                .text_sm()
                .font_medium()
                .line_height(relative(1.2))
                .child(value),
        )
        .child(
            div()
                .text_xs()
                .opacity(0.6)
                .line_height(relative(1.2))
                .child(title),
        )
        .into_any_element()
}

fn detail_error_block(error: String, cx: &mut Context<PioneerDesktop>) -> AnyElement {
    h_flex()
        .w_full()
        .gap_2()
        .items_start()
        .p_3()
        .rounded_md()
        .bg(cx.theme().danger.opacity(0.08))
        .border_1()
        .border_color(cx.theme().danger.opacity(0.25))
        .child(
            Icon::new(IconName::TriangleAlert)
                .size_4()
                .text_color(cx.theme().danger),
        )
        .child(
            div()
                .text_sm()
                .line_height(relative(1.3))
                .text_color(cx.theme().danger)
                .child(error),
        )
        .into_any_element()
}

fn catalog_json_text_block(
    title: String,
    text: String,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    v_flex()
        .gap_2()
        .child(div().text_sm().font_medium().child(title))
        .child(
            div()
                .w_full()
                .max_h(px(180.))
                .overflow_y_scrollbar()
                .rounded_lg()
                .border_1()
                .border_color(cx.theme().border)
                .p_3()
                .font_family("monospace")
                .text_xs()
                .line_height(relative(1.35))
                .child(text),
        )
        .into_any_element()
}

fn render_tool_card(
    tool: &McpToolCatalogItem,
    card_ix: usize,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    let title = tool
        .title
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| tool.name.clone());

    catalog_item_card(("mcp-tool-card", card_ix), title, cx)
}

fn render_resource_card(
    resource: &McpResourceCatalogItem,
    card_ix: usize,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    let title = resource
        .title
        .clone()
        .or_else(|| resource.name.clone())
        .or_else(|| resource.uri.clone())
        .unwrap_or_else(|| "-".to_owned());

    catalog_item_card(("mcp-resource-card", card_ix), title, cx)
}

fn render_resource_template_card(
    template: &McpResourceTemplateCatalogItem,
    card_ix: usize,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    let title = template
        .title
        .clone()
        .or_else(|| template.name.clone())
        .or_else(|| template.uri_template.clone())
        .unwrap_or_else(|| "-".to_owned());

    catalog_item_card(("mcp-resource-template-card", card_ix), title, cx)
}

fn render_prompt_card(
    prompt: &McpPromptCatalogItem,
    card_ix: usize,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    let title = prompt
        .title
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| prompt.name.clone());

    catalog_item_card(("mcp-prompt-card", card_ix), title, cx)
}

fn catalog_item_card(
    id: impl Into<ElementId>,
    title: String,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    div()
        .id(id)
        .flex_none()
        .px_3()
        .py_1()
        .rounded_full()
        .bg(cx.theme().muted)
        .child(
            div()
                .text_xs()
                .font_medium()
                .overflow_hidden()
                .whitespace_nowrap()
                .text_ellipsis()
                .opacity(0.8)
                .child(title),
        )
        .into_any_element()
}
