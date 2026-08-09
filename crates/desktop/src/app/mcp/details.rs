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
    StyledExt,
    button::*,
    collapsible::Collapsible,
    scroll::ScrollableElement,
    separator::Separator,
    table::{DataTable, TableState},
    theme::ActiveTheme,
    *,
};
use pioneer_client::mcp::{details as mcp_details, presentation as mcp_presentation};
use pioneer_protocol::{McpAuditEventSummary, McpListItem, McpServerDetailsResponse};

impl PioneerDesktop {
    pub(crate) fn render_mcp_details(&self, window: &Window, cx: &mut Context<Self>) -> AnyElement {
        let details = self.mcp_server_details.as_ref();
        let server = mcp_details::mcp_details_server(
            self.mcp_servers.as_slice(),
            self.mcp_selected_server_id.as_deref(),
            details,
        );

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
                                div()
                                    .text_base()
                                    .font_semibold()
                                    .child(mcp_presentation::mcp_display_name(&server)),
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
                    .child(Separator::horizontal())
                    .child(self.render_mcp_health_section(&server, details, meta_grid_columns, cx))
                    .child(Separator::horizontal())
                    .child(self.render_mcp_tools_section(details, cx))
                    .child(Separator::horizontal())
                    .child(self.render_mcp_resources_section(details, cx))
                    .child(Separator::horizontal())
                    .child(self.render_mcp_resource_templates_section(details, cx))
                    .child(Separator::horizontal())
                    .child(self.render_mcp_prompts_section(details, cx))
                    .child(Separator::horizontal())
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
        let rows = mcp_presentation::mcp_overview_rows(server, details);

        let mut content = div().w_full().grid().grid_cols(grid_columns).gap_4();
        for row in rows {
            content = content.child(meta_item(
                Self::mcp_detail_value_label(&row.value),
                Self::mcp_detail_meta_label(row.kind),
            ));
        }

        content.into_any_element()
    }

    fn render_mcp_health_section(
        &self,
        server: &McpListItem,
        details: Option<&McpServerDetailsResponse>,
        grid_columns: u16,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let health = details.map(|details| &details.health);
        let health_rows = mcp_presentation::mcp_health_rows(server, details);

        let mut grid = div().w_full().grid().grid_cols(grid_columns).gap_4();
        for row in health_rows {
            grid = grid.child(meta_item(
                Self::mcp_detail_value_label(&row.value),
                Self::mcp_detail_meta_label(row.kind),
            ));
        }

        let content = v_flex()
            .gap_4()
            .child(grid)
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
                DataTable::new(state_entity)
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

        let rows = mcp_presentation::mcp_audit_rows(audit, 8)
            .iter()
            .map(|item| {
                let event_time = Self::format_mcp_time(item.created_at);
                let action = Self::mcp_audit_action_label(&item.action);
                let event_label = item
                    .raw_tool_name
                    .as_deref()
                    .map(|tool| format!("{action} / {tool}"))
                    .unwrap_or_else(|| action.clone());
                let decision = Self::mcp_audit_decision_label(&item.decision);
                let reason = item.reason_code.clone();
                let reason_label = reason
                    .clone()
                    .unwrap_or_else(|| t!("mcp.common.none").to_string());
                let details_summary = Self::mcp_audit_details_summary_label(&item.details_summary);
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
                            tone: Self::mcp_presentation_tone(item.decision_tone),
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

    fn mcp_detail_meta_label(kind: mcp_presentation::McpDetailMetaKind) -> String {
        match kind {
            mcp_presentation::McpDetailMetaKind::Source => t!("mcp.details.source").to_string(),
            mcp_presentation::McpDetailMetaKind::Scope => t!("mcp.details.scope").to_string(),
            mcp_presentation::McpDetailMetaKind::Transport => {
                t!("mcp.details.transport").to_string()
            }
            mcp_presentation::McpDetailMetaKind::Loaded => t!("mcp.details.loaded").to_string(),
            mcp_presentation::McpDetailMetaKind::Runtime => t!("mcp.details.runtime").to_string(),
            mcp_presentation::McpDetailMetaKind::Live => t!("mcp.details.live").to_string(),
            mcp_presentation::McpDetailMetaKind::LastSeen => {
                t!("mcp.details.last_seen").to_string()
            }
            mcp_presentation::McpDetailMetaKind::RetryAttempt => {
                t!("mcp.details.retry_attempt").to_string()
            }
            mcp_presentation::McpDetailMetaKind::NextRetry => {
                t!("mcp.details.next_retry").to_string()
            }
            mcp_presentation::McpDetailMetaKind::LastError => {
                t!("mcp.details.last_error").to_string()
            }
        }
    }

    fn mcp_detail_value_label(value: &mcp_presentation::McpDetailValue) -> String {
        match value {
            mcp_presentation::McpDetailValue::Empty => "-".to_owned(),
            mcp_presentation::McpDetailValue::Text(value) => value.clone(),
            mcp_presentation::McpDetailValue::Timestamp(value) => Self::format_mcp_time(*value),
            mcp_presentation::McpDetailValue::Count(value) => value.to_string(),
            mcp_presentation::McpDetailValue::Bool(value) => {
                if *value {
                    t!("mcp.common.yes").to_string()
                } else {
                    t!("mcp.common.no").to_string()
                }
            }
            mcp_presentation::McpDetailValue::Status(status) => {
                Self::mcp_status_label_from_kind(*status)
            }
            mcp_presentation::McpDetailValue::Source(source) => match source {
                mcp_presentation::McpSourceLabel::Config => {
                    t!("mcp.details.source_config").to_string()
                }
            },
            mcp_presentation::McpDetailValue::Scope(scope) => match scope {
                mcp_presentation::McpScopeLabel::Workspace => {
                    t!("mcp.details.scope_workspace").to_string()
                }
                mcp_presentation::McpScopeLabel::User => t!("mcp.details.scope_user").to_string(),
            },
            mcp_presentation::McpDetailValue::Transport(transport) => {
                Self::mcp_transport_label_from_presentation(transport)
            }
        }
    }

    fn mcp_audit_action_label(action: &mcp_presentation::McpAuditAction) -> String {
        match action {
            mcp_presentation::McpAuditAction::Install => t!("mcp.audit.action_install").to_string(),
            mcp_presentation::McpAuditAction::Update => t!("mcp.audit.action_update").to_string(),
            mcp_presentation::McpAuditAction::Uninstall => {
                t!("mcp.audit.action_uninstall").to_string()
            }
            mcp_presentation::McpAuditAction::Policy => t!("mcp.audit.action_policy").to_string(),
            mcp_presentation::McpAuditAction::Start => t!("mcp.audit.action_start").to_string(),
            mcp_presentation::McpAuditAction::Started => t!("mcp.audit.action_started").to_string(),
            mcp_presentation::McpAuditAction::StartFailed => {
                t!("mcp.audit.action_start_failed").to_string()
            }
            mcp_presentation::McpAuditAction::Stop => t!("mcp.audit.action_stop").to_string(),
            mcp_presentation::McpAuditAction::Stopped => t!("mcp.audit.action_stopped").to_string(),
            mcp_presentation::McpAuditAction::Restart => t!("mcp.audit.action_restart").to_string(),
            mcp_presentation::McpAuditAction::CatalogRefreshed => {
                t!("mcp.audit.action_catalog_refreshed").to_string()
            }
            mcp_presentation::McpAuditAction::Call => t!("mcp.audit.action_call").to_string(),
            mcp_presentation::McpAuditAction::CallCompleted => {
                t!("mcp.audit.action_call_completed").to_string()
            }
            mcp_presentation::McpAuditAction::CallFailed => {
                t!("mcp.audit.action_call_failed").to_string()
            }
            mcp_presentation::McpAuditAction::Other(value) => value.clone(),
            mcp_presentation::McpAuditAction::None => t!("mcp.common.none").to_string(),
        }
    }

    fn mcp_audit_decision_label(decision: &mcp_presentation::McpAuditDecision) -> String {
        match decision {
            mcp_presentation::McpAuditDecision::Allowed => {
                t!("mcp.audit.decision_allowed").to_string()
            }
            mcp_presentation::McpAuditDecision::Blocked => {
                t!("mcp.audit.decision_blocked").to_string()
            }
            mcp_presentation::McpAuditDecision::Warning => {
                t!("mcp.audit.decision_warning").to_string()
            }
            mcp_presentation::McpAuditDecision::Other(value) => value.clone(),
            mcp_presentation::McpAuditDecision::None => t!("mcp.common.none").to_string(),
        }
    }

    fn mcp_presentation_tone(tone: mcp_presentation::McpPresentationTone) -> SkillDiagnosticsTone {
        match tone {
            mcp_presentation::McpPresentationTone::Default => SkillDiagnosticsTone::Default,
            mcp_presentation::McpPresentationTone::Muted => SkillDiagnosticsTone::Muted,
            mcp_presentation::McpPresentationTone::Success => SkillDiagnosticsTone::Success,
            mcp_presentation::McpPresentationTone::Warning => SkillDiagnosticsTone::Warning,
            mcp_presentation::McpPresentationTone::Danger => SkillDiagnosticsTone::Danger,
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

    fn mcp_audit_details_summary_label(
        summary: &mcp_presentation::McpAuditDetailsSummary,
    ) -> String {
        match summary {
            mcp_presentation::McpAuditDetailsSummary::Empty => t!("mcp.common.empty").to_string(),
            mcp_presentation::McpAuditDetailsSummary::ObjectPairs(pairs) => pairs
                .iter()
                .map(|(key, value)| format!("{key}: {}", Self::mcp_json_value_preview_label(value)))
                .collect::<Vec<_>>()
                .join(" | "),
            mcp_presentation::McpAuditDetailsSummary::ArrayLen(count) => {
                t!("mcp.common.items_count", count = *count).to_string()
            }
            mcp_presentation::McpAuditDetailsSummary::Value(value) => {
                Self::mcp_json_value_preview_label(value)
            }
        }
    }

    fn mcp_json_value_preview_label(value: &mcp_presentation::McpJsonValuePreview) -> String {
        match value {
            mcp_presentation::McpJsonValuePreview::Text(value) => value.clone(),
            mcp_presentation::McpJsonValuePreview::Bool(value) => value.to_string(),
            mcp_presentation::McpJsonValuePreview::Number(value) => value.clone(),
            mcp_presentation::McpJsonValuePreview::None => t!("mcp.common.none").to_string(),
            mcp_presentation::McpJsonValuePreview::EmptyArray => "[]".to_owned(),
            mcp_presentation::McpJsonValuePreview::ArrayLen(count) => format!("[{count}]"),
            mcp_presentation::McpJsonValuePreview::EmptyObject => "{}".to_owned(),
            mcp_presentation::McpJsonValuePreview::ObjectKeys(count) => {
                t!("mcp.common.keys_count", count = *count).to_string()
            }
        }
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
    tool: &pioneer_protocol::McpToolCatalogItem,
    card_ix: usize,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    catalog_item_card(
        ("mcp-tool-card", card_ix),
        mcp_presentation::mcp_tool_title(tool),
        cx,
    )
}

fn render_resource_card(
    resource: &pioneer_protocol::McpResourceCatalogItem,
    card_ix: usize,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    catalog_item_card(
        ("mcp-resource-card", card_ix),
        mcp_presentation::mcp_resource_title(resource).unwrap_or_else(|| "-".to_owned()),
        cx,
    )
}

fn render_resource_template_card(
    template: &pioneer_protocol::McpResourceTemplateCatalogItem,
    card_ix: usize,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    catalog_item_card(
        ("mcp-resource-template-card", card_ix),
        mcp_presentation::mcp_resource_template_title(template).unwrap_or_else(|| "-".to_owned()),
        cx,
    )
}

fn render_prompt_card(
    prompt: &pioneer_protocol::McpPromptCatalogItem,
    card_ix: usize,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    catalog_item_card(
        ("mcp-prompt-card", card_ix),
        mcp_presentation::mcp_prompt_title(prompt),
        cx,
    )
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
