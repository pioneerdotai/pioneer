use crate::app::{
    root::PioneerDesktop,
    skills::details::table::{
        SkillDiagnosticsTableColumn, SkillDiagnosticsTableDelegate, SkillDiagnosticsTableModel,
    },
};
use chrono::{Local, TimeZone};
use gpui_kit::component::{
    StyledExt,
    button::*,
    collapsible::Collapsible,
    separator::Separator,
    table::{DataTable, TableState},
    theme::ActiveTheme,
    *,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::skills::{
    catalog as skill_catalog, health as skill_health, presentation as skill_presentation,
    presentation::{SkillDiagnosticsTableCell, SkillDiagnosticsTableRow, SkillDiagnosticsTone},
};
use pioneer_protocol::{
    SkillAuditTimelineItem, SkillDependencyDiagnostic, SkillSecurityFinding, SkillTrustGateStatus,
    SkillValidationDiagnostic,
};

impl PioneerDesktop {
    pub(crate) fn render_skill_details(
        &self,
        window: &Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        pioneer_observability::record_qualification_diagnostic!(record_render(
            pioneer_observability::RenderRegion::Skills
        ));
        let Some(skill_id) = self.selected_skill_target.clone() else {
            return v_flex()
                .size_full()
                .bg(cx.theme().background)
                .items_center()
                .justify_center()
                .child(
                    div()
                        .text_sm()
                        .opacity(0.65)
                        .child(t!("skills.error.invalid_skill_target").to_string()),
                )
                .into_any_element();
        };

        let Some(skill) =
            skill_catalog::find_skill(self.installed_skills.as_slice(), &skill_id).cloned()
        else {
            return v_flex()
                .size_full()
                .bg(cx.theme().background)
                .items_center()
                .justify_center()
                .child(
                    div()
                        .text_sm()
                        .opacity(0.65)
                        .child(t!("skills.error.invalid_skill_target").to_string()),
                )
                .into_any_element();
        };

        let health_detail =
            skill_health::skill_health_detail(&self.skills_health_details, &skill.skill_id)
                .cloned();
        let is_pending = self.is_skill_pending(&skill.skill_id);
        let can_manage = self
            .principal_presentation_capabilities()
            .can_manage_capabilities;
        let desktop_entity = cx.entity().clone();
        let skill_summary = skill_presentation::skill_summary_presentation(&skill);
        let version_label = skill_summary
            .version
            .clone()
            .unwrap_or_else(|| "-".to_owned());
        let source_label = Self::source_label(&skill_summary.source);
        let trust_label = Self::trust_label(&skill_summary.trust);
        let status_label = Self::status_label(&skill_summary.status);
        let fingerprint_short = skill_summary.fingerprint_short.clone();
        let status_color = Self::status_color(skill_summary.status_tone, cx);
        let owner = skill_summary.slug.owner.as_deref();
        let meta_grid_columns = self.skill_details_meta_grid_columns(window);
        let diagnostics_grid_columns = self.skill_details_diagnostics_grid_columns(window);
        let detail_diagnostics =
            skill_presentation::skill_detail_diagnostics(&skill, health_detail.as_ref());
        self.sync_skill_diagnostics_tables(detail_diagnostics.recent_audit.as_slice(), cx);

        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .child(
                v_flex()
                    .pt_3()
                    .px_6()
                    .pb_5()
                    .gap_4()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        h_flex()
                            .w_full()
                            .min_w_0()
                            .justify_between()
                            .items_start()
                            .gap_10()
                            .child(
                                v_flex().flex_1().min_w_0().gap_3().child(
                                    v_flex()
                                        .w_full()
                                        .min_w_0()
                                        .child(
                                            h_flex()
                                                .w_full()
                                                .min_w_0()
                                                .items_baseline()
                                                .gap_1()
                                                .when_some(owner, |this, owner| {
                                                    this.child(
                                                        div()
                                                            .flex_none()
                                                            .text_base()
                                                            .font_medium()
                                                            .opacity(0.6)
                                                            .overflow_hidden()
                                                            .whitespace_nowrap()
                                                            .text_ellipsis()
                                                            .child(owner.to_owned()),
                                                    )
                                                    .child(
                                                        div()
                                                            .flex_none()
                                                            .text_base()
                                                            .opacity(0.6)
                                                            .child("/"),
                                                    )
                                                })
                                                .child(
                                                    div()
                                                        .min_w_0()
                                                        .text_base()
                                                        .font_semibold()
                                                        .overflow_hidden()
                                                        .whitespace_nowrap()
                                                        .text_ellipsis()
                                                        .child(skill.slug.clone()),
                                                ),
                                        )
                                        .child(
                                            div()
                                                .w_full()
                                                .min_w_0()
                                                .text_sm()
                                                .line_height(relative(1.35))
                                                .opacity(0.6)
                                                .overflow_hidden()
                                                .whitespace_normal()
                                                .line_clamp(3)
                                                .child(skill.description.clone()),
                                        ),
                                ),
                            )
                            .child(
                                div()
                                    .flex_none()
                                    .mt_1p5()
                                    .px_2()
                                    .py_0p5()
                                    .rounded_full()
                                    .border_1()
                                    .border_color(status_color)
                                    .text_xs()
                                    .text_color(status_color)
                                    .font_medium()
                                    .child(status_label),
                            ),
                    )
                    .when(can_manage, |this| {
                        this.child(
                            h_flex()
                                .items_center()
                                .gap_2()
                                .child(
                                    Button::new("skill-screen-enabled")
                                        .xsmall()
                                        .compact()
                                        .h_6()
                                        .px_3()
                                        .when(skill.policy.enabled, |this| this.primary())
                                        .when(!skill.policy.enabled, |this| this.outline())
                                        .disabled(is_pending)
                                        .label(t!("skills.button.enabled").to_string())
                                        .on_click({
                                            let desktop_entity = desktop_entity.clone();
                                            let skill_id = skill.skill_id.clone();
                                            let next_enabled = !skill.policy.enabled;
                                            let allow_implicit =
                                                skill.policy.allow_implicit_invocation;
                                            move |_, _, cx| {
                                                let _ = desktop_entity.update(cx, |view, cx| {
                                                    view.set_skill_policy(
                                                        skill_id.clone(),
                                                        next_enabled,
                                                        allow_implicit,
                                                        cx,
                                                    );
                                                    cx.notify();
                                                });
                                            }
                                        }),
                                )
                                .child(
                                    Button::new("skill-screen-implicit")
                                        .xsmall()
                                        .compact()
                                        .h_6()
                                        .px_3()
                                        .when(skill.policy.allow_implicit_invocation, |this| {
                                            this.primary()
                                        })
                                        .when(!skill.policy.allow_implicit_invocation, |this| {
                                            this.outline()
                                        })
                                        .disabled(
                                            is_pending
                                                || !skill.policy.allow_implicit_invocation_editable,
                                        )
                                        .label(t!("skills.button.implicit").to_string())
                                        .on_click({
                                            let desktop_entity = desktop_entity.clone();
                                            let skill_id = skill.skill_id.clone();
                                            let enabled = skill.policy.enabled;
                                            let next_implicit =
                                                !skill.policy.allow_implicit_invocation;
                                            move |_, _, cx| {
                                                let _ = desktop_entity.update(cx, |view, cx| {
                                                    view.set_skill_policy(
                                                        skill_id.clone(),
                                                        enabled,
                                                        next_implicit,
                                                        cx,
                                                    );
                                                    cx.notify();
                                                });
                                            }
                                        }),
                                ),
                        )
                    }),
            )
            .child(
                v_flex()
                    .id("skills-detail-scroll")
                    .flex_1()
                    .overflow_y_scroll()
                    .p_6()
                    .gap_6()
                    .child(
                        div()
                            .w_full()
                            .grid()
                            .grid_cols(meta_grid_columns)
                            .gap_3()
                            .child(Self::render_skill_meta_item(
                                version_label,
                                t!("skills.card.version").to_string(),
                            ))
                            .child(Self::render_skill_meta_item(
                                source_label,
                                t!("skills.catalog.source").to_string(),
                            ))
                            .child(Self::render_skill_meta_item(
                                trust_label,
                                t!("skills.catalog.trust").to_string(),
                            ))
                            .child(Self::render_skill_meta_item(
                                fingerprint_short,
                                t!("skills.card.fingerprint").to_string(),
                            )),
                    )
                    .child(Separator::horizontal())
                    .child(self.render_validation_section(
                        detail_diagnostics.validation_issues.as_slice(),
                        cx,
                    ))
                    .child(Separator::horizontal())
                    .child(self.render_dependency_section(
                        detail_diagnostics.dependency_diagnostics.as_slice(),
                        diagnostics_grid_columns,
                        cx,
                    ))
                    .child(Separator::horizontal())
                    .child(self.render_security_section(
                        detail_diagnostics.security_findings.as_slice(),
                        diagnostics_grid_columns,
                        cx,
                    ))
                    .child(Separator::horizontal())
                    .child(
                        self.render_trust_gate_section(
                            detail_diagnostics.trust_gate.as_slice(),
                            cx,
                        ),
                    )
                    .child(Separator::horizontal())
                    .child(self.render_recent_audit_section(
                        detail_diagnostics.recent_audit.as_slice(),
                        cx,
                    )),
            )
            .into_any_element()
    }

    fn skill_details_meta_grid_columns(&self, window: &Window) -> u16 {
        let viewport_width = window.viewport_size().width;
        let sidebar_width = if self.show_sidebar {
            self.sidebar_panel_width
        } else {
            px(0.)
        };

        let content_padding_x = px(48.); // .p_6 on details scroll area
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

    fn skill_details_diagnostics_grid_columns(&self, window: &Window) -> u16 {
        let viewport_width = window.viewport_size().width;
        let sidebar_width = if self.show_sidebar {
            self.sidebar_panel_width
        } else {
            px(0.)
        };

        let content_padding_x = px(48.);
        let available_width = (viewport_width - sidebar_width - content_padding_x).max(px(0.));

        if available_width >= px(1260.) {
            3
        } else if available_width >= px(760.) {
            2
        } else {
            1
        }
    }

    fn render_skill_meta_item(value: String, title: String) -> AnyElement {
        v_flex()
            .w_full()
            .min_w_0()
            .gap_0p5()
            .child(
                div()
                    .w_full()
                    .min_w_0()
                    .text_sm()
                    .font_medium()
                    .line_height(relative(1.2))
                    .child(value),
            )
            .child(
                div()
                    .w_full()
                    .min_w_0()
                    .text_xs()
                    .opacity(0.6)
                    .line_height(relative(1.2))
                    .child(title),
            )
            .into_any_element()
    }

    fn is_skills_details_section_expanded(&self, section_id: &str) -> bool {
        self.skills_details_expanded_sections.contains(section_id)
    }

    fn toggle_skills_details_section(&mut self, section_id: &str, cx: &mut Context<Self>) {
        if !self.skills_details_expanded_sections.remove(section_id) {
            self.skills_details_expanded_sections
                .insert(section_id.to_owned());
        }
        cx.notify();
    }

    fn render_collapsible_skills_details_section(
        &self,
        section_id: &'static str,
        title: String,
        subtitle: String,
        content: Option<AnyElement>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let has_content = content.is_some();
        let open = has_content && self.is_skills_details_section_expanded(section_id);
        let toggle_id = Self::diagnostics_element_hash(&["skills-section-toggle", section_id]);
        let icon_name = if open {
            IconName::ChevronUp
        } else {
            IconName::ChevronDown
        };

        let header = h_flex()
            .w_full()
            .min_w_0()
            .items_center()
            .justify_between()
            .gap_3()
            .child(
                v_flex()
                    .flex_1()
                    .min_w_0()
                    .gap_2()
                    .child(
                        div()
                            .w_full()
                            .min_w_0()
                            .text_sm()
                            .font_semibold()
                            .line_height(relative(1.))
                            .child(title),
                    )
                    .child(
                        div()
                            .w_full()
                            .min_w_0()
                            .text_xs()
                            .opacity(0.6)
                            .line_height(relative(1.25))
                            .overflow_hidden()
                            .whitespace_normal()
                            .line_clamp(2)
                            .child(subtitle),
                    ),
            )
            .when(has_content, |this| {
                this.child(
                    div()
                        .id(("skills-section-toggle", toggle_id))
                        .flex_none()
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
                        view.toggle_skills_details_section(section_id, cx);
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

    fn render_validation_section(
        &self,
        issues: &[SkillValidationDiagnostic],
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let rows = skill_presentation::skill_validation_rows(issues);
        let subtitle = if rows.is_empty() {
            t!("skills.diagnostics.empty_validation").to_string()
        } else {
            format!("{} {}", t!("skills.screen.installed_count"), rows.len())
        };

        let content = if rows.is_empty() {
            None
        } else {
            Some(
                v_flex()
                    .gap_4()
                    .children(rows.iter().map(|row| {
                        let path = row.field_path.as_deref().unwrap_or("-");
                        v_flex()
                            .gap_0p5()
                            .child(
                                div()
                                    .text_xs()
                                    .opacity(0.8)
                                    .child(format!("{} ({}) | {}", row.code, row.level, path)),
                            )
                            .child(div().text_xs().opacity(0.6).child(row.message.clone()))
                    }))
                    .into_any_element(),
            )
        };

        self.render_collapsible_skills_details_section(
            "validation",
            t!("skills.diagnostics.validation").to_string(),
            subtitle,
            content,
            cx,
        )
    }

    fn render_dependency_section(
        &self,
        diagnostics: &[SkillDependencyDiagnostic],
        grid_columns: u16,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let subtitle = if diagnostics.is_empty() {
            t!("skills.diagnostics.empty_dependencies").to_string()
        } else {
            t!("skills.diagnostics.dependencies_intro").to_string()
        };

        let content = if diagnostics.is_empty() {
            None
        } else {
            let cards = skill_presentation::skill_dependency_cards(diagnostics)
                .iter()
                .enumerate()
                .map(|(card_ix, item)| Self::render_dependency_card(item, card_ix, cx))
                .collect::<Vec<_>>();

            Some(
                div()
                    .w_full()
                    .grid()
                    .grid_cols(grid_columns)
                    .gap_3()
                    .children(cards)
                    .into_any_element(),
            )
        };

        self.render_collapsible_skills_details_section(
            "dependencies",
            t!("skills.diagnostics.dependencies").to_string(),
            subtitle,
            content,
            cx,
        )
    }

    fn render_security_section(
        &self,
        findings: &[SkillSecurityFinding],
        grid_columns: u16,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let subtitle = if findings.is_empty() {
            t!("skills.diagnostics.empty_security").to_string()
        } else {
            t!("skills.diagnostics.security_intro").to_string()
        };

        let content = if findings.is_empty() {
            None
        } else {
            let cards = skill_presentation::skill_security_cards(findings)
                .iter()
                .enumerate()
                .map(|(card_ix, item)| Self::render_security_card(item, card_ix, cx))
                .collect::<Vec<_>>();

            Some(
                div()
                    .w_full()
                    .grid()
                    .grid_cols(grid_columns)
                    .gap_3()
                    .children(cards)
                    .into_any_element(),
            )
        };

        self.render_collapsible_skills_details_section(
            "security",
            t!("skills.diagnostics.security").to_string(),
            subtitle,
            content,
            cx,
        )
    }

    fn render_trust_gate_section(
        &self,
        trust_gate: &[SkillTrustGateStatus],
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let subtitle = if trust_gate.is_empty() {
            t!("skills.diagnostics.empty_trust_gate").to_string()
        } else {
            t!("skills.diagnostics.trust_gate_intro").to_string()
        };

        let content = if trust_gate.is_empty() {
            None
        } else {
            let cards = skill_presentation::skill_trust_gate_cards(trust_gate)
                .iter()
                .enumerate()
                .map(|(card_ix, item)| Self::render_trust_gate_card(item, card_ix, cx))
                .collect::<Vec<_>>();

            Some(
                div()
                    .w_full()
                    .grid()
                    .grid_cols(2)
                    .gap_3()
                    .children(cards)
                    .into_any_element(),
            )
        };

        self.render_collapsible_skills_details_section(
            "trust-gates",
            t!("skills.diagnostics.trust_gate").to_string(),
            subtitle,
            content,
            cx,
        )
    }

    fn render_dependency_card(
        item: &skill_presentation::SkillDependencyCard,
        card_ix: usize,
        cx: &mut App,
    ) -> AnyElement {
        let scope = format!("dep-{card_ix}");
        let requirement_title = Self::dependency_kind_label(&item.kind);
        let requirement_value = if let Some(name) = item.requirement_name.as_ref() {
            name.clone()
        } else {
            t!("skills.diagnostics.none").to_string()
        };
        let status_label = Self::dependency_status_label(item.status);
        let status_tone = skill_presentation::skill_dependency_status_tone(item.status);
        let action = if let Some(hint) = item.action_hint.as_ref() {
            hint.clone()
        } else {
            t!("skills.diagnostics.none").to_string()
        };

        v_flex()
            .w_full()
            .gap_2()
            .px_3()
            .py_2()
            .rounded_lg()
            .bg(cx.theme().muted)
            .child(
                h_flex()
                    .items_start()
                    .justify_between()
                    .gap_4()
                    .child(Self::render_diagnostics_card_field(
                        requirement_title,
                        t!("skills.diagnostics.dependencies_hint_requirement").to_string(),
                        requirement_value,
                        SkillDiagnosticsTone::Default,
                        scope.as_str(),
                        "requirement",
                        cx,
                    ))
                    .child(Self::render_diagnostics_badge(
                        status_label,
                        status_tone,
                        t!("skills.diagnostics.dependencies_hint_status").to_string(),
                        scope.as_str(),
                        "status",
                        cx,
                    )),
            )
            .child(Self::render_diagnostics_card_secondary_field(
                t!("skills.diagnostics.dependencies_column_action").to_string(),
                t!("skills.diagnostics.dependencies_hint_action").to_string(),
                action,
                scope.as_str(),
                "action",
                cx,
            ))
            .into_any_element()
    }

    fn render_security_card(
        item: &skill_presentation::SkillSecurityCard,
        card_ix: usize,
        cx: &mut App,
    ) -> AnyElement {
        let scope = format!("sec-{card_ix}");
        let severity_label = Self::security_severity_label(&item.severity);
        let severity_tone = item.severity_tone;
        let rule_label = if let Some(rule_id) = item.rule_id.as_ref() {
            rule_id.clone()
        } else {
            t!("skills.diagnostics.none").to_string()
        };
        let message = if let Some(message) = item.message.as_ref() {
            message.clone()
        } else {
            t!("skills.diagnostics.none").to_string()
        };
        let location = item
            .location
            .as_deref()
            .map(str::to_owned)
            .unwrap_or_else(|| t!("skills.diagnostics.security_location_unknown").to_string());

        v_flex()
            .w_full()
            .gap_2()
            .px_3()
            .py_2()
            .rounded_lg()
            .bg(cx.theme().muted)
            .child(
                h_flex()
                    .items_start()
                    .justify_between()
                    .gap_2()
                    .child(Self::render_diagnostics_card_field(
                        t!("skills.diagnostics.security_column_rule").to_string(),
                        t!("skills.diagnostics.security_hint_rule").to_string(),
                        rule_label,
                        SkillDiagnosticsTone::Default,
                        scope.as_str(),
                        "rule",
                        cx,
                    ))
                    .child(Self::render_diagnostics_badge(
                        severity_label,
                        severity_tone,
                        t!("skills.diagnostics.security_hint_level").to_string(),
                        scope.as_str(),
                        "level",
                        cx,
                    )),
            )
            .child(Self::render_diagnostics_card_field(
                t!("skills.diagnostics.security_column_finding").to_string(),
                t!("skills.diagnostics.security_hint_finding").to_string(),
                message,
                SkillDiagnosticsTone::Default,
                scope.as_str(),
                "finding",
                cx,
            ))
            .child(Self::render_diagnostics_card_field(
                t!("skills.diagnostics.security_column_location").to_string(),
                t!("skills.diagnostics.security_hint_location").to_string(),
                location,
                SkillDiagnosticsTone::Muted,
                scope.as_str(),
                "location",
                cx,
            ))
            .into_any_element()
    }

    fn render_trust_gate_card(
        item: &skill_presentation::SkillTrustGateCard,
        card_ix: usize,
        cx: &mut App,
    ) -> AnyElement {
        let scope = format!("trust-{card_ix}");
        let tool_label = Self::trust_gate_tool_label(&item.tool_kind);
        let min_trust_label = Self::diagnostics_trust_level_label(&item.minimum_trust);
        let decision_label = Self::trust_gate_decision_label(item.decision);
        let decision_tone = item.decision_tone;
        let explanation = if item.decision == skill_presentation::SkillTrustGateDecision::Allowed {
            t!("skills.diagnostics.trust_explanation_allowed").to_string()
        } else {
            format!(
                "{} {}",
                t!("skills.diagnostics.trust_explanation_blocked"),
                min_trust_label
            )
        };

        v_flex()
            .w_full()
            .gap_2()
            .px_3()
            .py_2()
            .rounded_lg()
            .bg(cx.theme().muted)
            .child(
                h_flex()
                    .items_start()
                    .justify_between()
                    .gap_2()
                    .child(Self::render_diagnostics_card_field(
                        t!("skills.diagnostics.trust_column_tool").to_string(),
                        t!("skills.diagnostics.trust_hint_tool").to_string(),
                        tool_label,
                        SkillDiagnosticsTone::Default,
                        scope.as_str(),
                        "tool",
                        cx,
                    ))
                    .child(Self::render_diagnostics_card_field(
                        t!("skills.diagnostics.trust_column_min_trust").to_string(),
                        t!("skills.diagnostics.trust_hint_min_trust").to_string(),
                        min_trust_label,
                        SkillDiagnosticsTone::Default,
                        scope.as_str(),
                        "min-trust",
                        cx,
                    ))
                    .child(Self::render_diagnostics_badge(
                        decision_label,
                        decision_tone,
                        t!("skills.diagnostics.trust_hint_decision").to_string(),
                        scope.as_str(),
                        "decision",
                        cx,
                    )),
            )
            .child(Self::render_diagnostics_card_secondary_field(
                t!("skills.diagnostics.trust_column_explanation").to_string(),
                t!("skills.diagnostics.trust_hint_explanation").to_string(),
                explanation,
                scope.as_str(),
                "explanation",
                cx,
            ))
            .into_any_element()
    }

    fn render_diagnostics_card_field(
        title: String,
        title_hint: String,
        value: String,
        tone: SkillDiagnosticsTone,
        scope: &str,
        field_key: &str,
        cx: &mut App,
    ) -> AnyElement {
        let text_color = Self::skill_diagnostics_tone_color(tone, cx);
        let title_text = title;
        let title_id_base = title_text.clone();
        let value_text = if value.trim().is_empty() {
            t!("skills.diagnostics.none").to_string()
        } else {
            value
        };
        let value_tooltip = value_text.clone();
        let hint = title_hint.trim().to_owned();
        let hint_id = Self::diagnostics_element_hash(&[
            scope,
            field_key,
            title_id_base.as_str(),
            hint.as_str(),
            "hint",
        ]);
        let value_id = Self::diagnostics_element_hash(&[
            scope,
            field_key,
            title_id_base.as_str(),
            value_tooltip.as_str(),
            "value",
        ]);

        v_flex()
            .child(
                h_flex()
                    .items_center()
                    .gap_1()
                    .child(div().text_xs().opacity(0.6).child(title_text.clone()))
                    .when(!hint.is_empty(), |this| {
                        let hint_for_tooltip = hint.clone();
                        this.child(
                            div()
                                .id(("skills-diag-field-hint", hint_id))
                                .opacity(0.6)
                                .mt_px()
                                .child(Icon::new(IconName::Info).size_2p5())
                                .tooltip(move |window, tooltip_cx| {
                                    gpui_kit::component::tooltip::Tooltip::new(
                                        hint_for_tooltip.clone(),
                                    )
                                    .text_xs()
                                    .text_color(tooltip_cx.theme().popover_foreground)
                                    .build(window, tooltip_cx)
                                }),
                        )
                    }),
            )
            .child(
                div()
                    .id(("skills-diag-field-value", value_id))
                    .text_sm()
                    .font_semibold()
                    .text_color(text_color)
                    .child(value_text),
            )
            .into_any_element()
    }

    fn render_diagnostics_card_secondary_field(
        title: String,
        title_hint: String,
        value: String,
        scope: &str,
        field_key: &str,
        _cx: &mut App,
    ) -> AnyElement {
        let title_text = title;
        let title_id_base = title_text.clone();
        let value_text = if value.trim().is_empty() {
            t!("skills.diagnostics.none").to_string()
        } else {
            value
        };
        let value_tooltip = value_text.clone();
        let hint = title_hint.trim().to_owned();
        let hint_id = Self::diagnostics_element_hash(&[
            scope,
            field_key,
            title_id_base.as_str(),
            hint.as_str(),
            "hint",
        ]);
        let value_id = Self::diagnostics_element_hash(&[
            scope,
            field_key,
            title_id_base.as_str(),
            value_tooltip.as_str(),
            "value-secondary",
        ]);

        v_flex()
            .child(
                h_flex()
                    .items_center()
                    .gap_1()
                    .child(div().text_xs().opacity(0.6).child(title_text.clone()))
                    .when(!hint.is_empty(), |this| {
                        let hint_for_tooltip = hint.clone();
                        this.child(
                            div()
                                .id(("skills-diag-field-hint", hint_id))
                                .opacity(0.6)
                                .mt_px()
                                .child(Icon::new(IconName::Info).size_2p5())
                                .tooltip(move |window, tooltip_cx| {
                                    gpui_kit::component::tooltip::Tooltip::new(
                                        hint_for_tooltip.clone(),
                                    )
                                    .text_xs()
                                    .text_color(tooltip_cx.theme().popover_foreground)
                                    .build(window, tooltip_cx)
                                }),
                        )
                    }),
            )
            .child(
                div()
                    .id(("skills-diag-field-value", value_id))
                    .text_xs()
                    .opacity(0.6)
                    .child(value_text),
            )
            .into_any_element()
    }

    fn render_diagnostics_badge(
        value: String,
        tone: SkillDiagnosticsTone,
        hint: String,
        scope: &str,
        field_key: &str,
        cx: &mut App,
    ) -> AnyElement {
        let tone_color = Self::skill_diagnostics_tone_color(tone, cx);
        let badge_id = Self::diagnostics_element_hash(&[
            scope,
            field_key,
            value.as_str(),
            hint.as_str(),
            "badge",
        ]);

        div()
            .id(("skills-diagnostics-badge", badge_id))
            .mt_1()
            .px_2()
            .rounded_full()
            .border_1()
            .border_color(tone_color)
            .text_xs()
            .text_color(tone_color)
            .font_medium()
            .child(value)
            .into_any_element()
    }

    fn render_recent_audit_section(
        &self,
        recent_audit: &[SkillAuditTimelineItem],
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let subtitle = if recent_audit.is_empty() {
            t!("skills.diagnostics.empty_audit").to_string()
        } else {
            t!("skills.diagnostics.audit_intro").to_string()
        };

        let content = if recent_audit.is_empty() {
            None
        } else {
            Some(self.render_skill_diagnostics_table(
                &self.skills_audit_table_state,
                recent_audit.len().min(8),
            ))
        };

        self.render_collapsible_skills_details_section(
            "audit",
            t!("skills.diagnostics.audit").to_string(),
            subtitle,
            content,
            cx,
        )
    }

    fn sync_skill_diagnostics_tables(
        &self,
        audit: &[SkillAuditTimelineItem],
        cx: &mut Context<Self>,
    ) {
        self.sync_diagnostics_table_state(
            &self.skills_audit_table_state,
            Self::build_audit_table_model(audit),
            cx,
        );
    }

    fn sync_diagnostics_table_state(
        &self,
        state_entity: &Entity<TableState<SkillDiagnosticsTableDelegate>>,
        model: SkillDiagnosticsTableModel,
        cx: &mut Context<Self>,
    ) {
        let _ = state_entity.update(cx, |state, cx| {
            if state.delegate().model() != &model {
                state.delegate_mut().set_model(model);
                // TableState keeps click/selection visual state internally.
                // Force-reset it on each model refresh so stale highlights never persist.
                if !state.delegate().model().rows.is_empty() {
                    state.set_selected_row(0, cx);
                }
                state.clear_selection(cx);
                state.refresh(cx);
            }
        });
    }

    fn render_skill_diagnostics_table(
        &self,
        state_entity: &Entity<TableState<SkillDiagnosticsTableDelegate>>,
        row_count: usize,
    ) -> AnyElement {
        let visible_rows = row_count.clamp(1, 8) as f32;
        let row_height = gpui_kit::component::Size::Small.table_row_height();
        let table_height = row_height * (visible_rows + 1.) + px(2.);

        div()
            .w_full()
            .h(table_height)
            .child(
                DataTable::new(state_entity)
                    .with_size(gpui_kit::component::Size::Small)
                    .scrollbar_visible(true, false),
            )
            .into_any_element()
    }

    fn build_audit_table_model(audit: &[SkillAuditTimelineItem]) -> SkillDiagnosticsTableModel {
        let columns = vec![
            SkillDiagnosticsTableColumn {
                key: "time",
                title: t!("skills.diagnostics.audit_column_time").to_string(),
                hint: t!("skills.diagnostics.audit_hint_time").to_string(),
                width: px(176.),
            },
            SkillDiagnosticsTableColumn {
                key: "event",
                title: t!("skills.diagnostics.audit_column_event").to_string(),
                hint: t!("skills.diagnostics.audit_hint_event").to_string(),
                width: px(200.),
            },
            SkillDiagnosticsTableColumn {
                key: "result",
                title: t!("skills.diagnostics.audit_column_result").to_string(),
                hint: t!("skills.diagnostics.audit_hint_result").to_string(),
                width: px(120.),
            },
            SkillDiagnosticsTableColumn {
                key: "details",
                title: t!("skills.diagnostics.audit_column_details").to_string(),
                hint: t!("skills.diagnostics.audit_hint_details").to_string(),
                width: px(620.),
            },
        ];

        let rows = skill_presentation::skill_audit_rows(audit, 8)
            .iter()
            .map(|item| {
                let event_time = Self::format_skill_audit_time(item.created_at);
                let action = Self::audit_action_label(&item.action);
                let decision = Self::audit_decision_label(item.decision);
                let reason_label = item
                    .reason_code
                    .clone()
                    .unwrap_or_else(|| t!("skills.diagnostics.audit_reason_none").to_string());
                let details_summary = Self::audit_details_summary_label(&item.details_summary);
                let details = if item.reason_code.is_none() {
                    details_summary.clone()
                } else {
                    format!("{reason_label} | {details_summary}")
                };
                let details_tooltip = format!(
                    "{}\n{} {}",
                    details_summary,
                    t!("skills.diagnostics.reason"),
                    reason_label
                );

                SkillDiagnosticsTableRow {
                    cells: vec![
                        SkillDiagnosticsTableCell {
                            text: event_time.clone(),
                            tooltip: Some(event_time),
                            tone: SkillDiagnosticsTone::Default,
                        },
                        SkillDiagnosticsTableCell {
                            text: action.clone(),
                            tooltip: Some(action),
                            tone: SkillDiagnosticsTone::Default,
                        },
                        SkillDiagnosticsTableCell {
                            text: decision.clone(),
                            tooltip: Some(decision),
                            tone: item.decision_tone,
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

    fn skill_diagnostics_tone_color(tone: SkillDiagnosticsTone, cx: &mut App) -> Hsla {
        match tone {
            SkillDiagnosticsTone::Default => cx.theme().foreground.opacity(0.84),
            SkillDiagnosticsTone::Muted => cx.theme().muted_foreground,
            SkillDiagnosticsTone::Success => cx.theme().success,
            SkillDiagnosticsTone::Warning => cx.theme().warning,
            SkillDiagnosticsTone::Danger => cx.theme().danger,
        }
    }

    fn diagnostics_element_hash(parts: &[&str]) -> u64 {
        use std::hash::{Hash, Hasher};

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for part in parts {
            part.hash(&mut hasher);
        }
        hasher.finish()
    }

    fn dependency_kind_label(kind: &skill_presentation::SkillDependencyKind) -> String {
        match kind {
            skill_presentation::SkillDependencyKind::Bin => {
                t!("skills.diagnostics.dependencies_kind_bin").to_string()
            }
            skill_presentation::SkillDependencyKind::Env => {
                t!("skills.diagnostics.dependencies_kind_env").to_string()
            }
            skill_presentation::SkillDependencyKind::ApiKey => {
                t!("skills.diagnostics.dependencies_kind_api_key").to_string()
            }
            skill_presentation::SkillDependencyKind::Command => {
                t!("skills.diagnostics.dependencies_kind_command").to_string()
            }
            skill_presentation::SkillDependencyKind::Mcp => {
                t!("skills.diagnostics.dependencies_kind_mcp").to_string()
            }
            skill_presentation::SkillDependencyKind::Tool => {
                t!("skills.diagnostics.dependencies_kind_tool").to_string()
            }
            skill_presentation::SkillDependencyKind::Other(value) => value.clone(),
        }
    }

    fn dependency_status_label(status: skill_presentation::SkillDependencyStatus) -> String {
        match status {
            skill_presentation::SkillDependencyStatus::Ready => {
                t!("skills.diagnostics.dependencies_status_ready").to_string()
            }
            skill_presentation::SkillDependencyStatus::Missing => {
                t!("skills.diagnostics.dependencies_status_missing").to_string()
            }
            skill_presentation::SkillDependencyStatus::Blocked => {
                t!("skills.diagnostics.dependencies_status_blocked").to_string()
            }
            skill_presentation::SkillDependencyStatus::Warning => {
                t!("skills.diagnostics.dependencies_status_warning").to_string()
            }
            skill_presentation::SkillDependencyStatus::Unknown => {
                t!("skills.diagnostics.dependencies_status_unknown").to_string()
            }
        }
    }

    fn security_severity_label(severity: &skill_presentation::SkillSecuritySeverity) -> String {
        match severity {
            skill_presentation::SkillSecuritySeverity::Critical => {
                t!("skills.diagnostics.security_severity_critical").to_string()
            }
            skill_presentation::SkillSecuritySeverity::High => {
                t!("skills.diagnostics.security_severity_high").to_string()
            }
            skill_presentation::SkillSecuritySeverity::Medium => {
                t!("skills.diagnostics.security_severity_medium").to_string()
            }
            skill_presentation::SkillSecuritySeverity::Low => {
                t!("skills.diagnostics.security_severity_low").to_string()
            }
            skill_presentation::SkillSecuritySeverity::Info => {
                t!("skills.diagnostics.security_severity_info").to_string()
            }
            skill_presentation::SkillSecuritySeverity::Other(value) => value.clone(),
            skill_presentation::SkillSecuritySeverity::None => {
                t!("skills.diagnostics.none").to_string()
            }
        }
    }

    fn trust_gate_tool_label(tool_kind: &skill_presentation::SkillTrustGateToolKind) -> String {
        match tool_kind {
            skill_presentation::SkillTrustGateToolKind::Shell => {
                t!("skills.diagnostics.trust_tool_shell").to_string()
            }
            skill_presentation::SkillTrustGateToolKind::Http => {
                t!("skills.diagnostics.trust_tool_http").to_string()
            }
            skill_presentation::SkillTrustGateToolKind::FunctionProxy => {
                t!("skills.diagnostics.trust_tool_function_proxy").to_string()
            }
            skill_presentation::SkillTrustGateToolKind::Mcp => {
                t!("skills.diagnostics.trust_tool_mcp").to_string()
            }
            skill_presentation::SkillTrustGateToolKind::Other(value) => value.clone(),
            skill_presentation::SkillTrustGateToolKind::None => {
                t!("skills.diagnostics.none").to_string()
            }
        }
    }

    fn diagnostics_trust_level_label(trust_level: &skill_presentation::SkillTrustLevel) -> String {
        match trust_level {
            skill_presentation::SkillTrustLevel::Internal => {
                t!("skills.trust.internal").to_string()
            }
            skill_presentation::SkillTrustLevel::Verified => {
                t!("skills.trust.verified").to_string()
            }
            skill_presentation::SkillTrustLevel::Community => {
                t!("skills.trust.community").to_string()
            }
            skill_presentation::SkillTrustLevel::Untrusted => {
                t!("skills.trust.untrusted").to_string()
            }
            skill_presentation::SkillTrustLevel::Other(value) => value.clone(),
            skill_presentation::SkillTrustLevel::None => t!("skills.diagnostics.none").to_string(),
        }
    }

    fn trust_gate_decision_label(decision: skill_presentation::SkillTrustGateDecision) -> String {
        match decision {
            skill_presentation::SkillTrustGateDecision::Allowed => {
                t!("skills.diagnostics.trust_decision_allowed").to_string()
            }
            skill_presentation::SkillTrustGateDecision::Blocked => {
                t!("skills.diagnostics.trust_decision_blocked").to_string()
            }
        }
    }

    fn audit_action_label(action: &skill_presentation::SkillAuditAction) -> String {
        match action {
            skill_presentation::SkillAuditAction::Install => {
                t!("skills.diagnostics.audit_action_install").to_string()
            }
            skill_presentation::SkillAuditAction::Update => {
                t!("skills.diagnostics.audit_action_update").to_string()
            }
            skill_presentation::SkillAuditAction::Uninstall => {
                t!("skills.diagnostics.audit_action_uninstall").to_string()
            }
            skill_presentation::SkillAuditAction::ResolveAllowed => {
                t!("skills.diagnostics.audit_action_resolve_allowed").to_string()
            }
            skill_presentation::SkillAuditAction::ResolveBlocked => {
                t!("skills.diagnostics.audit_action_resolve_blocked").to_string()
            }
            skill_presentation::SkillAuditAction::RuntimeAllowed => {
                t!("skills.diagnostics.audit_action_runtime_allowed").to_string()
            }
            skill_presentation::SkillAuditAction::RuntimeBlocked => {
                t!("skills.diagnostics.audit_action_runtime_blocked").to_string()
            }
            skill_presentation::SkillAuditAction::SecurityWarn => {
                t!("skills.diagnostics.audit_action_security_warn").to_string()
            }
            skill_presentation::SkillAuditAction::None => t!("skills.diagnostics.none").to_string(),
        }
    }

    fn audit_decision_label(decision: skill_presentation::SkillAuditDecision) -> String {
        match decision {
            skill_presentation::SkillAuditDecision::Allowed => {
                t!("skills.diagnostics.audit_decision_allowed").to_string()
            }
            skill_presentation::SkillAuditDecision::Blocked => {
                t!("skills.diagnostics.audit_decision_blocked").to_string()
            }
            skill_presentation::SkillAuditDecision::Warning => {
                t!("skills.diagnostics.audit_decision_warning").to_string()
            }
            skill_presentation::SkillAuditDecision::None => {
                t!("skills.diagnostics.none").to_string()
            }
        }
    }

    fn format_skill_audit_time(created_at: i64) -> String {
        let datetime = if created_at > 10_000_000_000 {
            Local.timestamp_millis_opt(created_at).single()
        } else {
            Local.timestamp_opt(created_at, 0).single()
        };

        datetime
            .map(|value| value.format("%d.%m.%Y %H:%M").to_string())
            .unwrap_or_else(|| "-".to_owned())
    }

    fn audit_details_summary_label(
        summary: &skill_presentation::SkillAuditDetailsSummary,
    ) -> String {
        match summary {
            skill_presentation::SkillAuditDetailsSummary::Empty => {
                t!("skills.diagnostics.audit_details_empty").to_string()
            }
            skill_presentation::SkillAuditDetailsSummary::Text(value) => value.clone(),
            skill_presentation::SkillAuditDetailsSummary::ObjectPairs(pairs) => pairs
                .iter()
                .map(|(key, value)| format!("{key}: {}", Self::json_value_preview_label(value)))
                .collect::<Vec<_>>()
                .join(" | "),
            skill_presentation::SkillAuditDetailsSummary::ArrayLen(len) => {
                format!("{}: {}", t!("skills.diagnostics.audit_column_details"), len)
            }
            skill_presentation::SkillAuditDetailsSummary::Value(value) => {
                Self::json_value_preview_label(value)
            }
        }
    }

    fn json_value_preview_label(value: &skill_presentation::SkillJsonValuePreview) -> String {
        match value {
            skill_presentation::SkillJsonValuePreview::Text(value) => value.clone(),
            skill_presentation::SkillJsonValuePreview::Bool(value) => value.to_string(),
            skill_presentation::SkillJsonValuePreview::Number(value) => value.clone(),
            skill_presentation::SkillJsonValuePreview::None => {
                t!("skills.diagnostics.none").to_string()
            }
            skill_presentation::SkillJsonValuePreview::EmptyArray => "[]".to_owned(),
            skill_presentation::SkillJsonValuePreview::ArrayLen(len) => format!("[{}]", len),
            skill_presentation::SkillJsonValuePreview::EmptyObject => "{}".to_owned(),
            skill_presentation::SkillJsonValuePreview::ObjectKeys(len) => {
                format!("{{{} keys}}", len)
            }
        }
    }
}
