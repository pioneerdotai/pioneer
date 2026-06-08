use crate::{app::root::PioneerDesktop, assets::PioneerIconName};
use gpui::{prelude::*, *};
use gpui_component::{
    button::{Button, ButtonVariants},
    scroll::Scrollbar,
    theme::ActiveTheme,
    *,
};
use pioneer_client::skills::{presentation as skill_presentation, upload as skill_upload};
use pioneer_protocol::SkillListItem;
use std::rc::Rc;

const INSTALLED_SKILL_CARD_HEIGHT: f32 = 112.0;
const INSTALLED_SKILL_ROW_GAP: f32 = 10.0;
const INSTALLED_SKILL_ROW_HEIGHT: f32 = INSTALLED_SKILL_CARD_HEIGHT + INSTALLED_SKILL_ROW_GAP;

impl PioneerDesktop {
    pub(crate) fn render_skills(&self, _window: &Window, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let skills_error = self.skills_error.clone();
        let skills_upload_progress = self.skills_upload_progress.clone();
        let installed_skills = Rc::new(self.installed_skills.clone());

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
                            installed_skills.len()
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
                            .when(installed_skills.is_empty(), |this| {
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
                            .when(!installed_skills.is_empty(), |this| {
                                this.child(self.render_installed_skills_virtual_list(
                                    installed_skills.clone(),
                                    desktop_entity.clone(),
                                    cx,
                                ))
                            }),
                    ),
            )
            .into_any_element()
    }

    fn render_installed_skills_virtual_list(
        &self,
        installed_skills: Rc<Vec<SkillListItem>>,
        desktop_entity: Entity<Self>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let item_sizes = Rc::new(
            (0..installed_skills.len())
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
                                installed_skills.get(ix).map(|skill| {
                                    let is_pending = view.is_skill_pending(
                                        skill.slug.as_str(),
                                        skill.source_kind.as_str(),
                                    );

                                    Self::render_installed_skill_row(
                                        ix,
                                        skill,
                                        is_pending,
                                        desktop_entity.clone(),
                                        cx,
                                    )
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
        index: usize,
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
            .id(("installed-skill-row", index))
            .w_full()
            .h(px(INSTALLED_SKILL_ROW_HEIGHT))
            .pb(px(INSTALLED_SKILL_ROW_GAP))
            .cursor_pointer()
            .on_click({
                let desktop_entity = desktop_entity.clone();
                let slug = skill.slug.clone();
                let source_kind = skill.source_kind.clone();
                move |_, _, cx| {
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.open_skill_from_sidebar(slug.clone(), source_kind.clone(), cx);
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
                                                        .child(format!("@{}", owner.to_owned())),
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
                                                    .child(skill.display_name.clone()),
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
