use crate::{app::root::PioneerDesktop, assets::PioneerIconName};
use gpui::{prelude::*, *};
use gpui_component::{
    button::{Button, ButtonVariants},
    scroll::Scrollbar,
    theme::ActiveTheme,
    *,
};
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
                                let progress_width = if progress.total_bytes == 0 {
                                    0.0
                                } else {
                                    (progress.sent_bytes as f32 / progress.total_bytes as f32)
                                        .clamp(0.0, 1.0)
                                };
                                let progress_text = if progress.total_bytes == 0 {
                                    progress.label.clone()
                                } else {
                                    format!(
                                        "{} {}%",
                                        progress.label,
                                        (progress_width * 100.0).round() as u32
                                    )
                                };
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
                                                                .child(progress_text),
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
                                                                        .w(px(
                                                                            240.0 * progress_width
                                                                        ))
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
        let (owner, _slug_label) = Self::split_skill_slug_for_view(skill.slug.as_str());
        let status_color = Self::status_color(skill.status.as_str(), cx);
        let status_label = Self::status_label(skill.status.as_str());
        let version_label = skill
            .version
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("-")
            .to_owned();

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
                    .h(px(INSTALLED_SKILL_CARD_HEIGHT))
                    .pt_3()
                    .px_4()
                    .pb_3()
                    .rounded_lg()
                    .bg(cx.theme().background)
                    .border_1()
                    .border_color(cx.theme().border)
                    .gap_4()
                    .items_start()
                    .hover(|this| this.bg(cx.theme().secondary.opacity(0.45)))
                    .child(
                        v_flex()
                            .flex_1()
                            .h_full()
                            .gap_1()
                            .child(
                                h_flex()
                                    .w_full()
                                    .justify_between()
                                    .items_center()
                                    .gap_2()
                                    .child(
                                        h_flex()
                                            .items_center()
                                            .gap_1()
                                            .when_some(owner, |this, owner| {
                                                this.child(
                                                    div()
                                                        .text_sm()
                                                        .opacity(0.6)
                                                        .child(format!("@{}", owner.to_owned())),
                                                )
                                                .child(div().text_sm().opacity(0.6).child("/"))
                                            })
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_semibold()
                                                    .child(skill.display_name.clone()),
                                            ),
                                    ),
                            )
                            .child(
                                div()
                                    .flex_1()
                                    .text_xs()
                                    .line_height(relative(1.3))
                                    .opacity(0.6)
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .line_clamp(2)
                                    .child(skill.description.clone()),
                            )
                            .child(
                                h_flex().justify_between().items_center().gap_2().child(
                                    h_flex()
                                        .items_center()
                                        .gap_2()
                                        .child(div().text_xs().opacity(0.6).child(format!(
                                            "{} {}",
                                            t!("skills.card.version"),
                                            version_label
                                        )))
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

    pub(crate) fn source_label(source_kind: &str) -> String {
        match source_kind {
            "system" => t!("skills.source.system").to_string(),
            "user" => t!("skills.source.user").to_string(),
            "workspace" => t!("skills.source.workspace").to_string(),
            "registry" => t!("skills.source.registry").to_string(),
            _ => source_kind.to_owned(),
        }
    }

    pub(crate) fn trust_label(trust_level: &str) -> String {
        match trust_level {
            "internal" => t!("skills.trust.internal").to_string(),
            "verified" => t!("skills.trust.verified").to_string(),
            "community" => t!("skills.trust.community").to_string(),
            "untrusted" => t!("skills.trust.untrusted").to_string(),
            _ => trust_level.to_owned(),
        }
    }

    pub(crate) fn status_label(status: &str) -> String {
        match status {
            "active" => t!("skills.status.active").to_string(),
            "blocked" => t!("skills.status.blocked").to_string(),
            "disabled" => t!("skills.status.disabled").to_string(),
            _ => status.to_owned(),
        }
    }

    pub(crate) fn status_color(status: &str, cx: &mut Context<Self>) -> Hsla {
        match status {
            "active" => cx.theme().success,
            "blocked" => cx.theme().warning,
            "disabled" => cx.theme().foreground.opacity(1.),
            _ => cx.theme().foreground.opacity(1.),
        }
    }

    pub(crate) fn split_skill_slug_for_view(skill_slug: &str) -> (Option<&str>, &str) {
        let trimmed = skill_slug.trim();
        if let Some((owner, slug)) = trimmed.split_once('/') {
            let owner = owner.trim();
            let slug = slug.trim();
            if !owner.is_empty() && !slug.is_empty() {
                return (Some(owner), slug);
            }
        }

        (None, trimmed)
    }
}
