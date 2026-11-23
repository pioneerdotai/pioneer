use crate::catalog::SkillsCatalogView;
use gpui_kit::component::{switch::Switch, *};
use gpui_kit::{prelude::*, *};
use pioneer_desktop_foundation::{
    settings_divider, settings_group, settings_row, settings_section,
};

impl SkillsCatalogView {
    pub(crate) fn render_skill_policy_settings(
        &self,
        skill: &pioneer_client::skills::types::SkillListItem,
        cx: &Context<Self>,
    ) -> AnyElement {
        let disabled = !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
            || self.is_skill_pending(&skill.skill_id);
        let entity = cx.entity().clone();
        settings_section(
            t!("skills.settings.title").to_string(),
            settings_group(cx)
                .child(settings_row(
                    t!("skills.button.enabled").to_string(),
                    t!("skills.settings.enabled_description").to_string(),
                    Switch::new(self.ui_id(&skill.skill_id.to_string(), "settings-enabled"))
                        .accessibility_label(t!("skills.button.enabled").to_string())
                        .checked(skill.policy.enabled)
                        .disabled(disabled)
                        .on_click({
                            let entity = entity.clone();
                            let id = skill.skill_id.clone();
                            let implicit = skill.policy.allow_implicit_invocation;
                            move |enabled, _, cx| {
                                let _ = entity.update(cx, |view, cx| {
                                    view.set_skill_policy(id.clone(), *enabled, implicit, cx);
                                    cx.notify();
                                });
                            }
                        }),
                ))
                .child(settings_divider(cx))
                .child(settings_row(
                    t!("skills.button.implicit").to_string(),
                    t!("skills.settings.implicit_description").to_string(),
                    Switch::new(self.ui_id(&skill.skill_id.to_string(), "settings-implicit"))
                        .accessibility_label(t!("skills.button.implicit").to_string())
                        .checked(skill.policy.allow_implicit_invocation)
                        .disabled(disabled || !skill.policy.allow_implicit_invocation_editable)
                        .on_click({
                            let id = skill.skill_id.clone();
                            let enabled = skill.policy.enabled;
                            move |implicit, _, cx| {
                                let _ = entity.update(cx, |view, cx| {
                                    view.set_skill_policy(id.clone(), enabled, *implicit, cx);
                                    cx.notify();
                                });
                            }
                        }),
                )),
        )
        .into_any_element()
    }
}
