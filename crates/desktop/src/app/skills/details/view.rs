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
    divider::Divider,
    table::{Table, TableState},
    theme::ActiveTheme,
    *,
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
        let Some((slug, source_kind)) = self.selected_skill_target.clone() else {
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

        let Some(skill) = self
            .installed_skills
            .iter()
            .find(|skill| skill.slug == slug && skill.source_kind == source_kind)
            .cloned()
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

        let key = Self::skill_key(skill.slug.as_str(), skill.source_kind.as_str());
        let health_detail = self.skills_health_details.get(key.as_str()).cloned();
        let is_pending = self.is_skill_pending(skill.slug.as_str(), skill.source_kind.as_str());
        let desktop_entity = cx.entity().clone();
        let version_label = skill
            .version
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("-")
            .to_owned();
        let source_label = Self::source_label(skill.source_kind.as_str());
        let trust_label = Self::trust_label(skill.trust_level.as_str());
        let status_label = Self::status_label(skill.status.as_str());
        let fingerprint_short = Self::short_fingerprint(skill.fingerprint.as_str());
        let status_color = Self::status_color(skill.status.as_str(), cx);
        let (owner, _slug_label) = Self::split_skill_slug_for_view(skill.slug.as_str());
        let meta_grid_columns = self.skill_details_meta_grid_columns(window);
        let diagnostics_grid_columns = self.skill_details_diagnostics_grid_columns(window);
        let dependency_diagnostics = if let Some(health) = health_detail.as_ref() {
            health.dependency_diagnostics.clone()
        } else {
            skill.health.dependency_failures.clone()
        };
        let security_findings = if let Some(health) = health_detail.as_ref() {
            health.security_findings.clone()
        } else {
            skill.health.security_blocks.clone()
        };
        let validation_issues = if let Some(health) = health_detail.as_ref() {
            health.validation_issues.clone()
        } else {
            skill.health.validation_issues.clone()
        };
        let trust_gate = health_detail
            .as_ref()
            .map(|health| health.trust_gate.clone())
            .unwrap_or_default();
        let recent_audit = health_detail
            .as_ref()
            .map(|health| health.recent_audit.clone())
            .unwrap_or_default();
        self.sync_skill_diagnostics_tables(recent_audit.as_slice(), cx);

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
                                                            .child(format!(
                                                                "@{}",
                                                                owner.to_owned()
                                                            )),
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
                                                        .child(skill.display_name.clone()),
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
                    .child(
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
                                        let slug = skill.slug.clone();
                                        let source_kind = skill.source_kind.clone();
                                        let next_enabled = !skill.policy.enabled;
                                        let allow_implicit = skill.policy.allow_implicit_invocation;
                                        move |_, _, cx| {
                                            let _ = desktop_entity.update(cx, |view, cx| {
                                                view.set_skill_policy(
                                                    slug.clone(),
                                                    source_kind.clone(),
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
                                    .disabled(is_pending)
                                    .label(t!("skills.button.implicit").to_string())
                                    .on_click({
                                        let desktop_entity = desktop_entity.clone();
                                        let slug = skill.slug.clone();
                                        let source_kind = skill.source_kind.clone();
                                        let enabled = skill.policy.enabled;
                                        let next_implicit = !skill.policy.allow_implicit_invocation;
                                        move |_, _, cx| {
                                            let _ = desktop_entity.update(cx, |view, cx| {
                                                view.set_skill_policy(
                                                    slug.clone(),
                                                    source_kind.clone(),
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
                    .child(Divider::horizontal())
                    .child(self.render_validation_section(validation_issues.as_slice(), cx))
                    .child(Divider::horizontal())
                    .child(self.render_dependency_section(
                        dependency_diagnostics.as_slice(),
                        diagnostics_grid_columns,
                        cx,
                    ))
                    .child(Divider::horizontal())
                    .child(self.render_security_section(
                        security_findings.as_slice(),
                        diagnostics_grid_columns,
                        cx,
                    ))
                    .child(Divider::horizontal())
                    .child(self.render_trust_gate_section(trust_gate.as_slice(), cx))
                    .child(Divider::horizontal())
                    .child(self.render_recent_audit_section(recent_audit.as_slice(), cx)),
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
        let subtitle = if issues.is_empty() {
            t!("skills.diagnostics.empty_validation").to_string()
        } else {
            format!("{} {}", t!("skills.screen.installed_count"), issues.len())
        };

        let content = if issues.is_empty() {
            None
        } else {
            Some(
                v_flex()
                    .gap_4()
                    .children(issues.iter().map(|issue| {
                        let path = issue
                            .field_path
                            .as_deref()
                            .filter(|value| !value.trim().is_empty())
                            .unwrap_or("-");
                        v_flex()
                            .gap_0p5()
                            .child(
                                div()
                                    .text_xs()
                                    .opacity(0.8)
                                    .child(format!("{} ({}) | {}", issue.code, issue.level, path)),
                            )
                            .child(div().text_xs().opacity(0.6).child(issue.message.clone()))
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
            let cards = diagnostics
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
            let cards = findings
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
            let cards = trust_gate
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
        item: &SkillDependencyDiagnostic,
        card_ix: usize,
        cx: &mut App,
    ) -> AnyElement {
        let scope = format!("dep-{card_ix}");
        let requirement_title = Self::dependency_kind_label(item.kind.as_str());
        let requirement_value = if item.name.trim().is_empty() {
            t!("skills.diagnostics.none").to_string()
        } else {
            item.name.trim().to_owned()
        };
        let status_label = Self::dependency_status_label(item.status.as_str());
        let status_tone = Self::dependency_status_tone(item.status.as_str());
        let action = if item.hint.trim().is_empty() {
            t!("skills.diagnostics.none").to_string()
        } else {
            item.hint.trim().to_owned()
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
        item: &SkillSecurityFinding,
        card_ix: usize,
        cx: &mut App,
    ) -> AnyElement {
        let scope = format!("sec-{card_ix}");
        let severity_label = Self::security_severity_label(item.severity.as_str());
        let severity_tone = Self::security_severity_tone(item.severity.as_str());
        let rule_label = if item.rule_id.trim().is_empty() {
            t!("skills.diagnostics.none").to_string()
        } else {
            item.rule_id.trim().to_owned()
        };
        let message = if item.message.trim().is_empty() {
            t!("skills.diagnostics.none").to_string()
        } else {
            item.message.trim().to_owned()
        };
        let location = item
            .path
            .as_deref()
            .filter(|value| !value.trim().is_empty())
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
        item: &SkillTrustGateStatus,
        card_ix: usize,
        cx: &mut App,
    ) -> AnyElement {
        let scope = format!("trust-{card_ix}");
        let tool_label = Self::trust_gate_tool_label(item.tool_kind.as_str());
        let min_trust_label = Self::trust_level_label(item.minimum_trust.as_str());
        let decision_label = Self::trust_gate_decision_label(item.allowed);
        let decision_tone = Self::trust_gate_decision_tone(item.allowed);
        let explanation = if item.allowed {
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
                                    gpui_component::tooltip::Tooltip::new(hint_for_tooltip.clone())
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
                                    gpui_component::tooltip::Tooltip::new(hint_for_tooltip.clone())
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

        let rows = audit
            .iter()
            .take(8)
            .map(|item| {
                let event_time = Self::format_skill_audit_time(item.created_at);
                let action = Self::audit_action_label(item.action.as_str());
                let decision = Self::audit_decision_label(item.decision.as_str());
                let reason = item
                    .reason_code
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| t!("skills.diagnostics.audit_reason_none").to_string());
                let details_summary = Self::summarize_audit_details(item.details_json.as_str());
                let no_reason = t!("skills.diagnostics.audit_reason_none").to_string();
                let details = if reason == no_reason {
                    details_summary.clone()
                } else {
                    format!("{reason} | {details_summary}")
                };
                let details_tooltip = format!(
                    "{}\n{} {}",
                    details_summary,
                    t!("skills.diagnostics.reason"),
                    reason
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
                            tone: Self::audit_decision_tone(item.decision.as_str()),
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

    fn dependency_kind_label(kind: &str) -> String {
        match kind.trim() {
            "bin" => t!("skills.diagnostics.dependencies_kind_bin").to_string(),
            "env" => t!("skills.diagnostics.dependencies_kind_env").to_string(),
            "api_key" => t!("skills.diagnostics.dependencies_kind_api_key").to_string(),
            "command" => t!("skills.diagnostics.dependencies_kind_command").to_string(),
            "mcp" => t!("skills.diagnostics.dependencies_kind_mcp").to_string(),
            "tool" => t!("skills.diagnostics.dependencies_kind_tool").to_string(),
            other if !other.is_empty() => other.to_owned(),
            _ => t!("skills.diagnostics.dependencies_kind_tool").to_string(),
        }
    }

    fn dependency_status_label(status: &str) -> String {
        match status.trim() {
            "satisfied" | "ok" | "available" => {
                t!("skills.diagnostics.dependencies_status_ready").to_string()
            }
            "missing" => t!("skills.diagnostics.dependencies_status_missing").to_string(),
            "blocked" => t!("skills.diagnostics.dependencies_status_blocked").to_string(),
            "warning" => t!("skills.diagnostics.dependencies_status_warning").to_string(),
            _ => t!("skills.diagnostics.dependencies_status_unknown").to_string(),
        }
    }

    fn dependency_status_tone(status: &str) -> SkillDiagnosticsTone {
        match status.trim() {
            "satisfied" | "ok" | "available" => SkillDiagnosticsTone::Success,
            "missing" | "blocked" => SkillDiagnosticsTone::Danger,
            "warning" => SkillDiagnosticsTone::Warning,
            _ => SkillDiagnosticsTone::Muted,
        }
    }

    fn security_severity_label(severity: &str) -> String {
        match severity.trim().to_lowercase().as_str() {
            "critical" => t!("skills.diagnostics.security_severity_critical").to_string(),
            "high" => t!("skills.diagnostics.security_severity_high").to_string(),
            "medium" => t!("skills.diagnostics.security_severity_medium").to_string(),
            "low" => t!("skills.diagnostics.security_severity_low").to_string(),
            "info" | "informational" => t!("skills.diagnostics.security_severity_info").to_string(),
            other if !other.is_empty() => other.to_owned(),
            _ => t!("skills.diagnostics.none").to_string(),
        }
    }

    fn security_severity_tone(severity: &str) -> SkillDiagnosticsTone {
        match severity.trim().to_lowercase().as_str() {
            "critical" | "high" => SkillDiagnosticsTone::Danger,
            "medium" => SkillDiagnosticsTone::Warning,
            "low" | "info" | "informational" => SkillDiagnosticsTone::Muted,
            _ => SkillDiagnosticsTone::Muted,
        }
    }

    fn trust_gate_tool_label(tool_kind: &str) -> String {
        match tool_kind.trim() {
            "shell" => t!("skills.diagnostics.trust_tool_shell").to_string(),
            "http" => t!("skills.diagnostics.trust_tool_http").to_string(),
            "function_proxy" => t!("skills.diagnostics.trust_tool_function_proxy").to_string(),
            "mcp" => t!("skills.diagnostics.trust_tool_mcp").to_string(),
            other if !other.is_empty() => other.to_owned(),
            _ => t!("skills.diagnostics.none").to_string(),
        }
    }

    fn trust_level_label(trust_level: &str) -> String {
        match trust_level.trim() {
            "internal" => t!("skills.trust.internal").to_string(),
            "verified" => t!("skills.trust.verified").to_string(),
            "community" => t!("skills.trust.community").to_string(),
            "untrusted" => t!("skills.trust.untrusted").to_string(),
            other if !other.is_empty() => other.to_owned(),
            _ => t!("skills.diagnostics.none").to_string(),
        }
    }

    fn trust_gate_decision_label(allowed: bool) -> String {
        if allowed {
            t!("skills.diagnostics.trust_decision_allowed").to_string()
        } else {
            t!("skills.diagnostics.trust_decision_blocked").to_string()
        }
    }

    fn trust_gate_decision_tone(allowed: bool) -> SkillDiagnosticsTone {
        if allowed {
            SkillDiagnosticsTone::Success
        } else {
            SkillDiagnosticsTone::Danger
        }
    }

    fn audit_action_label(action: &str) -> String {
        match action.trim() {
            "install" => t!("skills.diagnostics.audit_action_install").to_string(),
            "update" => t!("skills.diagnostics.audit_action_update").to_string(),
            "uninstall" => t!("skills.diagnostics.audit_action_uninstall").to_string(),
            "resolve_allowed" => t!("skills.diagnostics.audit_action_resolve_allowed").to_string(),
            "resolve_blocked" => t!("skills.diagnostics.audit_action_resolve_blocked").to_string(),
            "runtime_allowed" => t!("skills.diagnostics.audit_action_runtime_allowed").to_string(),
            "runtime_blocked" => t!("skills.diagnostics.audit_action_runtime_blocked").to_string(),
            "security_warn" => t!("skills.diagnostics.audit_action_security_warn").to_string(),
            _ => t!("skills.diagnostics.none").to_string(),
        }
    }

    fn audit_decision_label(decision: &str) -> String {
        match decision.trim() {
            "allowed" => t!("skills.diagnostics.audit_decision_allowed").to_string(),
            "blocked" => t!("skills.diagnostics.audit_decision_blocked").to_string(),
            "warning" => t!("skills.diagnostics.audit_decision_warning").to_string(),
            _ => t!("skills.diagnostics.none").to_string(),
        }
    }

    fn audit_decision_tone(decision: &str) -> SkillDiagnosticsTone {
        match decision.trim() {
            "allowed" => SkillDiagnosticsTone::Success,
            "blocked" => SkillDiagnosticsTone::Danger,
            "warning" => SkillDiagnosticsTone::Warning,
            _ => SkillDiagnosticsTone::Muted,
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

    fn summarize_audit_details(details_json: &str) -> String {
        let raw = details_json.trim();
        if raw.is_empty() || raw == "{}" || raw == "null" {
            return t!("skills.diagnostics.audit_details_empty").to_string();
        }

        let value: serde_json::Value = match serde_json::from_str(raw) {
            Ok(value) => value,
            Err(_) => return Self::truncate_for_table(raw, 96),
        };

        match value {
            serde_json::Value::Object(map) => {
                if map.is_empty() {
                    return t!("skills.diagnostics.audit_details_empty").to_string();
                }

                map.iter()
                    .take(2)
                    .map(|(key, value)| format!("{key}: {}", Self::json_value_preview(value)))
                    .collect::<Vec<_>>()
                    .join(" | ")
            }
            serde_json::Value::Array(values) => {
                format!(
                    "{}: {}",
                    t!("skills.diagnostics.audit_column_details"),
                    values.len()
                )
            }
            other => Self::json_value_preview(&other),
        }
    }

    fn json_value_preview(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(value) => Self::truncate_for_table(value, 48),
            serde_json::Value::Bool(value) => value.to_string(),
            serde_json::Value::Number(value) => value.to_string(),
            serde_json::Value::Null => t!("skills.diagnostics.none").to_string(),
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
                    format!("{{{} keys}}", map.len())
                }
            }
        }
    }

    fn truncate_for_table(value: &str, max_chars: usize) -> String {
        if value.chars().count() <= max_chars {
            return value.to_owned();
        }

        let shortened = value.chars().take(max_chars).collect::<String>();
        format!("{shortened}...")
    }

    fn short_fingerprint(fingerprint: &str) -> String {
        let trimmed = fingerprint.trim();
        if trimmed.len() <= 16 {
            return trimmed.to_owned();
        }
        format!("{}...", &trimmed[..16])
    }
}
