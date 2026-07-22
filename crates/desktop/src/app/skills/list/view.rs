use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop},
    assets::PioneerIconName,
};
use gpui::{prelude::*, *};
use gpui_component::{
    button::{Button, ButtonVariants},
    menu::{ContextMenuExt, PopupMenu, PopupMenuItem},
    scroll::Scrollbar,
    theme::ActiveTheme,
    *,
};
use pioneer_client::skills::{
    catalog::SkillManagementProjection, presentation as skill_presentation, upload as skill_upload,
};
#[cfg(test)]
use pioneer_protocol::SkillId;
use pioneer_protocol::{SkillListItem, SkillPackId, SkillPackInstallationItem};
use std::collections::HashSet;
use std::rc::Rc;

const INSTALLED_SKILL_CARD_HEIGHT: f32 = 112.0;
const INSTALLED_SKILL_ROW_GAP: f32 = 10.0;
const INSTALLED_SKILL_ROW_HEIGHT: f32 = INSTALLED_SKILL_CARD_HEIGHT + INSTALLED_SKILL_ROW_GAP;

#[derive(Clone, Debug, PartialEq, Eq)]
enum DesktopSkillManagementRow {
    Standalone(SkillListItem),
    Pack {
        pack: SkillPackInstallationItem,
        child_count: usize,
        expanded: bool,
    },
    PackChild(SkillListItem),
}

impl DesktopSkillManagementRow {
    #[cfg(test)]
    fn navigation_target(&self) -> Option<&SkillId> {
        match self {
            Self::Standalone(skill) | Self::PackChild(skill) => Some(&skill.skill_id),
            Self::Pack { .. } => None,
        }
    }
}

fn project_desktop_skill_management_rows(
    management: &SkillManagementProjection,
    expanded_pack_ids: &HashSet<SkillPackId>,
) -> Vec<DesktopSkillManagementRow> {
    let mut rows = management
        .standalone
        .iter()
        .cloned()
        .map(DesktopSkillManagementRow::Standalone)
        .collect::<Vec<_>>();

    for pack_row in &management.packs {
        let expanded = expanded_pack_ids.contains(&pack_row.pack.id);
        rows.push(DesktopSkillManagementRow::Pack {
            pack: pack_row.pack.clone(),
            child_count: pack_row.children.len(),
            expanded,
        });
        if expanded {
            rows.extend(
                pack_row
                    .children
                    .iter()
                    .cloned()
                    .map(DesktopSkillManagementRow::PackChild),
            );
        }
    }
    rows
}

fn skill_pack_context_actions_enabled(is_connected: bool, is_pending: bool) -> bool {
    is_connected && !is_pending
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SkillCountForm {
    One,
    Few,
    Many,
}

fn skill_count_form(count: usize, is_russian: bool) -> SkillCountForm {
    if !is_russian {
        return if count == 1 {
            SkillCountForm::One
        } else {
            SkillCountForm::Many
        };
    }

    let last_two_digits = count % 100;
    if (11..=14).contains(&last_two_digits) {
        return SkillCountForm::Many;
    }

    match count % 10 {
        1 => SkillCountForm::One,
        2..=4 => SkillCountForm::Few,
        _ => SkillCountForm::Many,
    }
}

fn skill_count_label(count: usize) -> String {
    let locale = rust_i18n::locale();
    match skill_count_form(count, &*locale == "ru") {
        SkillCountForm::One => t!("skills.card.skill_count_one", count = count).to_string(),
        SkillCountForm::Few => t!("skills.card.skill_count_few", count = count).to_string(),
        SkillCountForm::Many => t!("skills.card.skill_count_many", count = count).to_string(),
    }
}

fn skill_pack_context_menu(
    menu: PopupMenu,
    pack_id: SkillPackId,
    pack_name: String,
    actions_enabled: bool,
    desktop_entity: Entity<PioneerDesktop>,
) -> PopupMenu {
    let update_pack_id = pack_id.clone();
    let update_desktop_entity = desktop_entity.clone();
    menu.item(
        PopupMenuItem::new(t!("skills.button.update").to_string())
            .disabled(!actions_enabled)
            .on_click(move |_, window, cx| {
                let _ = update_desktop_entity.update(cx, |view, cx| {
                    view.open_skill_pack_update_dialog(update_pack_id.clone(), window, cx);
                    cx.notify();
                });
            }),
    )
    .item(
        PopupMenuItem::new(t!("skills.button.uninstall").to_string())
            .disabled(!actions_enabled)
            .on_click(move |_, window, cx| {
                let _ = desktop_entity.update(cx, |view, cx| {
                    view.confirm_uninstall_skill_pack(
                        pack_id.clone(),
                        pack_name.clone(),
                        window,
                        cx,
                    );
                    cx.notify();
                });
            }),
    )
}

impl PioneerDesktop {
    pub(crate) fn render_skills(&self, _window: &Window, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let skills_error = self.skills_error.clone();
        let skills_upload_progress = self.skills_upload_progress.clone();
        let management_rows = Rc::new(project_desktop_skill_management_rows(
            &self.skills_management,
            &self.skills_expanded_pack_ids,
        ));
        let installed_count =
            self.skills_management.standalone.len() + self.skills_management.packs.len();

        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .child(
                v_flex()
                    .pt_3()
                    .px_6()
                    .pb_3()
                    .gap_2()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        v_flex()
                            .child(
                                div()
                                    .text_xl()
                                    .font_semibold()
                                    .child(t!("skills.screen.title").to_string()),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .opacity(0.6)
                                    .child(t!("skills.screen.description").to_string()),
                            ),
                    )
                    .child(h_flex().items_center().gap_2().child(
                        div().text_xs().opacity(0.6).child(format!(
                            "{} {}",
                            t!("skills.screen.installed_count"),
                            installed_count
                        )),
                    )),
            )
            .child(
                v_flex()
                    .id("skills-scroll")
                    .flex_1()
                    .overflow_hidden()
                    .p_6()
                    .child(
                        v_flex()
                            .w_full()
                            .h_full()
                            .gap_3()
                            .when_some(skills_upload_progress, |this, progress| {
                                let progress_fraction =
                                    skill_upload::skill_upload_progress_fraction(&progress);
                                let progress_label =
                                    skill_upload::skill_upload_progress_text(&progress);
                                this.child(
                                    h_flex()
                                        .w_full()
                                        .items_center()
                                        .justify_between()
                                        .gap_3()
                                        .p_3()
                                        .rounded_md()
                                        .bg(cx.theme().accent.opacity(0.08))
                                        .border_1()
                                        .border_color(cx.theme().accent.opacity(0.24))
                                        .child(
                                            h_flex()
                                                .items_center()
                                                .gap_3()
                                                .child(
                                                    Icon::new(PioneerIconName::RefreshCw)
                                                        .size_4()
                                                        .text_color(cx.theme().accent),
                                                )
                                                .child(
                                                    v_flex()
                                                        .gap_1()
                                                        .child(
                                                            div()
                                                                .text_sm()
                                                                .font_medium()
                                                                .child(progress_label),
                                                        )
                                                        .child(
                                                            div()
                                                                .w(px(240.0))
                                                                .h(px(4.0))
                                                                .rounded_full()
                                                                .bg(cx.theme().border)
                                                                .child(
                                                                    div()
                                                                        .h(px(4.0))
                                                                        .w(px(240.0
                                                                            * progress_fraction))
                                                                        .rounded_full()
                                                                        .bg(cx.theme().accent),
                                                                ),
                                                        ),
                                                ),
                                        )
                                        .child({
                                            let desktop_entity = desktop_entity.clone();
                                            Button::new("skills-upload-cancel")
                                                .small()
                                                .outline()
                                                .label(t!("buttons.cancel").to_string())
                                                .on_click(move |_, _, cx| {
                                                    let _ =
                                                        desktop_entity.update(cx, |view, cx| {
                                                            view.cancel_skill_upload(cx);
                                                        });
                                                })
                                        }),
                                )
                            })
                            .when_some(skills_error, |this, error| {
                                this.child(
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
                                        ),
                                )
                            })
                            .when(management_rows.is_empty(), |this| {
                                this.child(
                                    v_flex()
                                        .w_full()
                                        .h_full()
                                        .items_center()
                                        .justify_center()
                                        .gap_4()
                                        .p_8()
                                        .bg(cx.theme().background)
                                        .child({
                                            let desktop_entity = desktop_entity.clone();
                                            Button::new("skills-empty-install")
                                                .text()
                                                .group("skills-empty-install-btn")
                                                .on_click(move |_, window, cx| {
                                                    let _ =
                                                        desktop_entity.update(cx, |view, cx| {
                                                            view.open_skill_install_dialog(
                                                                window, cx,
                                                            );
                                                            cx.notify();
                                                        });
                                                })
                                                .child({
                                                    let icon_bg = cx.theme().foreground;
                                                    let icon_bg_hover =
                                                        cx.theme().foreground.opacity(0.8);
                                                    div()
                                                        .size_10()
                                                        .rounded_full()
                                                        .bg(icon_bg)
                                                        .group_hover(
                                                            "skills-empty-install-btn",
                                                            move |style| style.bg(icon_bg_hover),
                                                        )
                                                        .flex()
                                                        .items_center()
                                                        .justify_center()
                                                        .text_color(cx.theme().primary_foreground)
                                                        .child(Icon::new(IconName::Plus).size_6())
                                                })
                                        })
                                        .child(
                                            v_flex()
                                                .items_center()
                                                .justify_center()
                                                .gap_1()
                                                .child(
                                                    div().text_base().font_semibold().child(
                                                        t!("skills.empty.title").to_string(),
                                                    ),
                                                )
                                                .child(div().text_sm().opacity(0.6).child(
                                                    t!("skills.empty.description").to_string(),
                                                )),
                                        ),
                                )
                            })
                            .when(!management_rows.is_empty(), |this| {
                                this.child(self.render_skill_management_virtual_list(
                                    management_rows.clone(),
                                    desktop_entity.clone(),
                                    cx,
                                ))
                            }),
                    ),
            )
            .into_any_element()
    }

    fn render_skill_management_virtual_list(
        &self,
        rows: Rc<Vec<DesktopSkillManagementRow>>,
        desktop_entity: Entity<Self>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let item_sizes = Rc::new(
            (0..rows.len())
                .map(|_| size(px(0.), px(INSTALLED_SKILL_ROW_HEIGHT)))
                .collect::<Vec<_>>(),
        );
        let scroll_handle = self.skills_list_scroll_handle.clone();

        div()
            .w_full()
            .h_full()
            .relative()
            .overflow_hidden()
            .child(
                v_virtual_list(
                    cx.entity(),
                    "skills-installed-virtual-list",
                    item_sizes,
                    move |view, visible_range, _, cx| {
                        visible_range
                            .filter_map(|ix| {
                                rows.get(ix).map(|row| match row {
                                    DesktopSkillManagementRow::Standalone(skill) => {
                                        Self::render_installed_skill_row(
                                            skill,
                                            view.is_skill_pending(&skill.skill_id),
                                            desktop_entity.clone(),
                                            cx,
                                        )
                                    }
                                    DesktopSkillManagementRow::Pack {
                                        pack,
                                        child_count,
                                        expanded,
                                    } => Self::render_installed_skill_pack_row(
                                        pack,
                                        *child_count,
                                        *expanded,
                                        view.gateway.connection_state
                                            == GatewayConnectionState::Connected,
                                        view.is_skill_pack_pending(&pack.id),
                                        desktop_entity.clone(),
                                        cx,
                                    ),
                                    DesktopSkillManagementRow::PackChild(skill) => {
                                        Self::render_installed_skill_row(
                                            skill,
                                            view.is_skill_pending(&skill.skill_id),
                                            desktop_entity.clone(),
                                            cx,
                                        )
                                    }
                                })
                            })
                            .collect::<Vec<_>>()
                    },
                )
                .with_sizing_behavior(ListSizingBehavior::Auto)
                .track_scroll(&scroll_handle),
            )
            .child(
                div()
                    .absolute()
                    .top_0()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .child(Scrollbar::vertical(&scroll_handle)),
            )
            .into_any_element()
    }

    fn render_installed_skill_row(
        skill: &SkillListItem,
        is_pending: bool,
        desktop_entity: Entity<Self>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let summary = skill_presentation::skill_summary_presentation(skill);
        let owner = summary.slug.owner.as_deref();
        let status_color = Self::status_color(summary.status_tone, cx);
        let status_label = Self::status_label(&summary.status);
        let version_label = summary.version.as_deref().unwrap_or("-");

        v_flex()
            .id(SharedString::from(format!(
                "installed-skill-row:{}",
                skill.skill_id
            )))
            .w_full()
            .h(px(INSTALLED_SKILL_ROW_HEIGHT))
            .pb(px(INSTALLED_SKILL_ROW_GAP))
            .cursor_pointer()
            .on_click({
                let desktop_entity = desktop_entity.clone();
                let skill_id = skill.skill_id.clone();
                move |_, _, cx| {
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.open_skill_from_sidebar(skill_id.clone(), cx);
                        cx.notify();
                    });
                }
            })
            .child(
                h_flex()
                    .w_full()
                    .min_w_0()
                    .h(px(INSTALLED_SKILL_CARD_HEIGHT))
                    .pt_3()
                    .px_4()
                    .pb_3()
                    .rounded_lg()
                    .overflow_hidden()
                    .bg(cx.theme().background)
                    .border_1()
                    .border_color(cx.theme().border)
                    .gap_4()
                    .items_start()
                    .hover(|this| this.bg(cx.theme().secondary.opacity(0.45)))
                    .child(
                        v_flex()
                            .flex_1()
                            .min_w_0()
                            .h_full()
                            .gap_1()
                            .child(
                                h_flex()
                                    .w_full()
                                    .min_w_0()
                                    .justify_between()
                                    .items_center()
                                    .gap_2()
                                    .child(
                                        h_flex()
                                            .min_w_0()
                                            .items_center()
                                            .gap_1()
                                            .when_some(owner, |this, owner| {
                                                this.child(
                                                    div()
                                                        .flex_none()
                                                        .text_sm()
                                                        .opacity(0.6)
                                                        .child(owner.to_owned()),
                                                )
                                                .child(
                                                    div()
                                                        .flex_none()
                                                        .text_sm()
                                                        .opacity(0.6)
                                                        .child("/"),
                                                )
                                            })
                                            .child(
                                                div()
                                                    .min_w_0()
                                                    .text_sm()
                                                    .font_semibold()
                                                    .overflow_hidden()
                                                    .whitespace_nowrap()
                                                    .text_ellipsis()
                                                    .child(skill.slug.clone()),
                                            ),
                                    ),
                            )
                            .child(
                                div()
                                    .w_full()
                                    .min_w_0()
                                    .flex_1()
                                    .text_xs()
                                    .line_height(relative(1.3))
                                    .opacity(0.6)
                                    .overflow_hidden()
                                    .whitespace_normal()
                                    .line_clamp(2)
                                    .child(skill.description.clone()),
                            )
                            .child(
                                h_flex()
                                    .w_full()
                                    .min_w_0()
                                    .justify_between()
                                    .items_center()
                                    .gap_2()
                                    .child(
                                        h_flex()
                                            .min_w_0()
                                            .items_center()
                                            .gap_2()
                                            .child(
                                                div()
                                                    .min_w_0()
                                                    .text_xs()
                                                    .opacity(0.6)
                                                    .overflow_hidden()
                                                    .whitespace_nowrap()
                                                    .text_ellipsis()
                                                    .child(format!(
                                                        "{} {}",
                                                        t!("skills.card.version"),
                                                        version_label
                                                    )),
                                            )
                                            .when(is_pending, |this| {
                                                this.child(
                                                    Icon::new(PioneerIconName::RefreshCw)
                                                        .size_3()
                                                        .text_color(cx.theme().warning),
                                                )
                                            }),
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
            .into_any_element()
    }

    fn render_installed_skill_pack_row(
        pack: &SkillPackInstallationItem,
        child_count: usize,
        expanded: bool,
        is_connected: bool,
        is_pending: bool,
        desktop_entity: Entity<Self>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let pack_id = pack.id.clone();
        let context_pack_id = pack.id.clone();
        let context_pack_name = pack.name.clone();
        let context_desktop_entity = desktop_entity.clone();
        let context_actions_enabled = skill_pack_context_actions_enabled(is_connected, is_pending);
        h_flex()
            .id(SharedString::from(format!(
                "installed-skill-pack-row:{}",
                pack.id
            )))
            .w_full()
            .h(px(INSTALLED_SKILL_ROW_HEIGHT))
            .pb(px(INSTALLED_SKILL_ROW_GAP))
            .cursor_pointer()
            .on_click(move |_, _, cx| {
                let _ = desktop_entity.update(cx, |view, cx| {
                    view.toggle_skill_pack_expanded(pack_id.clone(), cx);
                });
            })
            .context_menu(move |menu, _, _| {
                skill_pack_context_menu(
                    menu,
                    context_pack_id.clone(),
                    context_pack_name.clone(),
                    context_actions_enabled,
                    context_desktop_entity.clone(),
                )
            })
            .child(
                h_flex()
                    .w_full()
                    .h(px(INSTALLED_SKILL_CARD_HEIGHT))
                    .pt_3()
                    .px_4()
                    .pb_3()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .hover(|this| this.bg(cx.theme().secondary.opacity(0.45)))
                    .items_center()
                    .justify_between()
                    .child(
                        v_flex()
                            .h_full()
                            .justify_between()
                            .child(
                                h_flex()
                                    .gap_2()
                                    .child(Icon::new(IconName::Folder).size_3p5())
                                    .child(
                                        div()
                                            .flex_1()
                                            .min_w_0()
                                            .text_sm()
                                            .font_semibold()
                                            .overflow_hidden()
                                            .whitespace_nowrap()
                                            .text_ellipsis()
                                            .child(pack.name.clone()),
                                    ),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .opacity(0.6)
                                    .child(skill_count_label(child_count)),
                            ),
                    )
                    .child(
                        Icon::new(if expanded {
                            IconName::ChevronUp
                        } else {
                            IconName::ChevronDown
                        })
                        .size_4(),
                    ),
            )
            .into_any_element()
    }

    pub(crate) fn source_label(source_kind: &skill_presentation::SkillSourceKind) -> String {
        match source_kind {
            skill_presentation::SkillSourceKind::System => t!("skills.source.system").to_string(),
            skill_presentation::SkillSourceKind::User => t!("skills.source.user").to_string(),
            skill_presentation::SkillSourceKind::Registry => {
                t!("skills.source.registry").to_string()
            }
            skill_presentation::SkillSourceKind::Other(value) => value.clone(),
        }
    }

    pub(crate) fn trust_label(trust_level: &skill_presentation::SkillTrustLevel) -> String {
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
            skill_presentation::SkillTrustLevel::None => String::new(),
        }
    }

    pub(crate) fn status_label(status: &skill_presentation::SkillStatus) -> String {
        match status {
            skill_presentation::SkillStatus::Active => t!("skills.status.active").to_string(),
            skill_presentation::SkillStatus::Blocked => t!("skills.status.blocked").to_string(),
            skill_presentation::SkillStatus::Disabled => t!("skills.status.disabled").to_string(),
            skill_presentation::SkillStatus::Other(value) => value.clone(),
        }
    }

    pub(crate) fn status_color(
        tone: skill_presentation::SkillDiagnosticsTone,
        cx: &mut Context<Self>,
    ) -> Hsla {
        match tone {
            skill_presentation::SkillDiagnosticsTone::Success => cx.theme().success,
            skill_presentation::SkillDiagnosticsTone::Warning => cx.theme().warning,
            _ => cx.theme().foreground.opacity(1.),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DesktopSkillManagementRow, SkillCountForm, project_desktop_skill_management_rows,
        skill_count_form, skill_pack_context_actions_enabled,
    };
    use pioneer_client::skills::catalog::{SkillManagementProjection, SkillPackManagementRow};
    use pioneer_protocol::{
        SkillHealthSummary, SkillId, SkillInstallState, SkillListItem, SkillPackId,
        SkillPackInstallationItem, SkillPackMembership, SkillPolicyState,
    };
    use std::collections::HashSet;

    fn skill_id(character: char) -> SkillId {
        SkillId::new(character.to_string().repeat(21)).expect("skill id")
    }

    fn pack_id(character: char) -> SkillPackId {
        SkillPackId::new(character.to_string().repeat(21)).expect("pack id")
    }

    fn skill(character: char, pack_id: Option<SkillPackId>) -> SkillListItem {
        SkillListItem {
            skill_id: skill_id(character),
            pack: pack_id.map(|pack_id| SkillPackMembership {
                pack_id,
                member_key: "member".to_owned(),
            }),
            owner: None,
            slug: character.to_string(),
            source_kind: "user".to_owned(),
            display_name: character.to_string(),
            description: String::new(),
            version: None,
            fingerprint: "fingerprint".to_owned(),
            trust_level: "community".to_owned(),
            install: SkillInstallState {
                managed: true,
                installed: true,
                lifecycle_editable: true,
                install_path: None,
                updated_at: None,
            },
            policy: SkillPolicyState {
                enabled: true,
                allow_implicit_invocation: true,
                allow_implicit_invocation_editable: true,
            },
            health: SkillHealthSummary {
                status: "ok".to_owned(),
                dependency_failures: Vec::new(),
                security_blocks: Vec::new(),
                validation_issues: Vec::new(),
            },
            status: "active".to_owned(),
            status_reason: None,
        }
    }

    fn pack(id: SkillPackId, name: &str) -> SkillPackInstallationItem {
        SkillPackInstallationItem {
            id,
            name: name.to_owned(),
            source_kind: "user".to_owned(),
            created_at: 1,
            updated_at: 2,
        }
    }

    #[::core::prelude::v1::test]
    fn management_rows_keep_packs_collapsed_and_non_navigating_by_default() {
        let populated_id = pack_id('P');
        let empty_id = pack_id('E');
        let management = SkillManagementProjection {
            standalone: vec![skill('S', None)],
            packs: vec![
                SkillPackManagementRow {
                    pack: pack(populated_id.clone(), "Pack"),
                    children: vec![skill('C', Some(populated_id))],
                    attachable: true,
                },
                SkillPackManagementRow {
                    pack: pack(empty_id, "Empty"),
                    children: Vec::new(),
                    attachable: false,
                },
            ],
        };

        let rows = project_desktop_skill_management_rows(&management, &HashSet::new());

        assert_eq!(rows.len(), 3);
        assert!(rows[0].navigation_target().is_some());
        assert!(rows[1].navigation_target().is_none());
        assert!(rows[2].navigation_target().is_none());
        assert!(matches!(
            rows[2],
            DesktopSkillManagementRow::Pack { child_count: 0, .. }
        ));
    }

    #[::core::prelude::v1::test]
    fn expanding_parent_inserts_navigable_children_inline() {
        let populated_id = pack_id('P');
        let child = skill('C', Some(populated_id.clone()));
        let management = SkillManagementProjection {
            standalone: Vec::new(),
            packs: vec![SkillPackManagementRow {
                pack: pack(populated_id.clone(), "Pack"),
                children: vec![child.clone()],
                attachable: true,
            }],
        };
        let rows =
            project_desktop_skill_management_rows(&management, &HashSet::from([populated_id]));

        assert_eq!(rows.len(), 2);
        assert!(rows[0].navigation_target().is_none());
        assert_eq!(rows[1].navigation_target(), Some(&child.skill_id));
        assert!(matches!(rows[1], DesktopSkillManagementRow::PackChild(_)));
    }

    #[::core::prelude::v1::test]
    fn pack_context_actions_require_connection_and_no_pending_operation() {
        assert!(skill_pack_context_actions_enabled(true, false));
        assert!(!skill_pack_context_actions_enabled(false, false));
        assert!(!skill_pack_context_actions_enabled(true, true));
    }

    #[::core::prelude::v1::test]
    fn pack_skill_count_uses_russian_plural_forms() {
        assert_eq!(skill_count_form(1, true), SkillCountForm::One);
        assert_eq!(skill_count_form(2, true), SkillCountForm::Few);
        assert_eq!(skill_count_form(5, true), SkillCountForm::Many);
        assert_eq!(skill_count_form(10, true), SkillCountForm::Many);
        assert_eq!(skill_count_form(11, true), SkillCountForm::Many);
        assert_eq!(skill_count_form(15, true), SkillCountForm::Many);
        assert_eq!(skill_count_form(21, true), SkillCountForm::One);
        assert_eq!(skill_count_form(22, true), SkillCountForm::Few);
        assert_eq!(skill_count_form(25, true), SkillCountForm::Many);
    }
}
