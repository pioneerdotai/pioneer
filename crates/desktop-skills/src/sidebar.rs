use crate::catalog::GatewayConnectionState;
use crate::{assets::PioneerIconName, catalog::SkillsCatalogView};
use gpui_kit::component::{
    button::{Button, ButtonVariants},
    theme::ActiveTheme,
    *,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::skills::catalog as skill_catalog;

const SIDEBAR_MENU_ITEM_OPACITY: f32 = 0.8;

impl CatalogSidebar {
    pub(crate) fn render_skills_sidebar(&self, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = self.owner.clone();
        let is_connected = self.gateway.connection_state == GatewayConnectionState::Connected;
        let can_manage = self
            .principal_presentation_capabilities()
            .can_manage_capabilities;

        v_flex()
            .size_full()
            .bg(cx.theme().sidebar)
            .p_0()
            .gap_1()
            .child(
                v_flex()
                    .pt_4()
                    .px_2()
                    .gap_1()
                    .when(can_manage, |this| {
                        this.child(
                            Button::new("skills-sidebar-install")
                                .ghost()
                                .justify_start()
                                .px_2()
                                .group("skills-sidebar-install-btn")
                                .disabled(!is_connected)
                                .child({
                                    h_flex()
                                        .w_full()
                                        .items_center()
                                        .justify_start()
                                        .gap_2()
                                        .child({
                                            let icon_bg_hover = cx.theme().foreground.opacity(0.1);
                                            pioneer_desktop_foundation::catalog_sidebar_icon(
                                                IconName::Plus,
                                                true,
                                                cx,
                                            )
                                            .id("skills-sidebar-install-icon")
                                            .group_hover(
                                                "skills-sidebar-install-btn",
                                                move |style| style.bg(icon_bg_hover),
                                            )
                                        })
                                        .child(pioneer_desktop_foundation::catalog_sidebar_label(
                                            t!("skills.button.install").to_string(),
                                        ))
                                })
                                .on_click({
                                    let desktop_entity = desktop_entity.clone();
                                    move |_, window, cx| {
                                        let _ = desktop_entity.update(cx, |view, cx| {
                                            view.open_skill_install_dialog(window, cx);
                                            cx.notify();
                                        });
                                    }
                                }),
                        )
                    })
                    .child(
                        Button::new("skills-sidebar-update")
                            .ghost()
                            .justify_start()
                            .px_2()
                            .group("skills-sidebar-update-btn")
                            .child({
                                h_flex()
                                    .w_full()
                                    .items_center()
                                    .justify_start()
                                    .gap_2()
                                    .child(
                                        pioneer_desktop_foundation::catalog_sidebar_menu_icon(
                                            PioneerIconName::RefreshCw,
                                        )
                                        .id("skills-sidebar-update-icon"),
                                    )
                                    .child(
                                        pioneer_desktop_foundation::catalog_sidebar_label(
                                            t!("skills.button.update").to_string(),
                                        )
                                        .flex_none(),
                                    )
                            })
                            .on_click(cx.listener(|view, _, _, cx| {
                                let _ = view
                                    .owner
                                    .update(cx, |owner, cx| owner.refresh_installed_skills(cx));
                                cx.notify();
                            })),
                    ),
            )
            .into_any_element()
    }

    pub(crate) fn render_skill_details_sidebar(&self, cx: &mut Context<Self>) -> AnyElement {
        let embedded = self
            .owner
            .upgrade()
            .is_some_and(|owner| owner.read(cx).is_plugin_component());

        let desktop_entity = self.owner.clone();
        let is_connected = self.gateway.connection_state == GatewayConnectionState::Connected;
        let selected_skill = self.navigation_input.skill_id().and_then(|skill_id| {
            skill_catalog::find_skill(self.installed_skills.as_slice(), skill_id).cloned()
        });
        let is_pending = selected_skill
            .as_ref()
            .is_some_and(|skill| self.is_skill_pending(&skill.skill_id));
        let lifecycle_editable = selected_skill
            .as_ref()
            .is_some_and(|skill| skill.install.lifecycle_editable);
        let can_manage = self
            .principal_presentation_capabilities()
            .can_manage_capabilities;

        v_flex()
            .size_full()
            .bg(cx.theme().sidebar)
            .p_0()
            .gap_1()
            .child(
                v_flex()
                    .pt_4()
                    .px_2()
                    .gap_1()
                    .when(!embedded, |column| {
                        column.child(
                            Button::new("skills-details-sidebar-back")
                                .ghost()
                                .justify_start()
                                .px_2()
                                .group("skills-details-sidebar-back-btn")
                                .child({
                                    h_flex()
                                        .w_full()
                                        .items_center()
                                        .justify_start()
                                        .gap_2()
                                        .child({
                                            let icon_bg = cx.theme().foreground.opacity(0.075);
                                            let icon_bg_hover = cx.theme().foreground.opacity(0.1);
                                            div()
                                                .id("skills-details-sidebar-back-icon")
                                                .size_6()
                                                .rounded_full()
                                                .bg(icon_bg)
                                                .group_hover(
                                                    "skills-details-sidebar-back-btn",
                                                    move |style| style.bg(icon_bg_hover),
                                                )
                                                .flex()
                                                .items_center()
                                                .justify_center()
                                                .child(
                                                    Icon::new(IconName::ChevronLeft)
                                                        .size_5()
                                                        .ml_neg_px()
                                                        .opacity(SIDEBAR_MENU_ITEM_OPACITY),
                                                )
                                        })
                                        .child(pioneer_desktop_foundation::catalog_sidebar_label(
                                            t!("buttons.back").to_string(),
                                        ))
                                })
                                .on_click({
                                    let desktop_entity = desktop_entity.clone();
                                    move |_, _, cx| {
                                        let _ = desktop_entity.update(cx, |view, cx| {
                                            view.close_skill_details_screen(cx);
                                            cx.notify();
                                        });
                                    }
                                }),
                        )
                    })
                    .when(can_manage && lifecycle_editable, |this| {
                        this.child(
                            Button::new("skills-details-sidebar-update")
                                .ghost()
                                .justify_start()
                                .px_2()
                                .group("skills-details-sidebar-update-btn")
                                .disabled(!is_connected || selected_skill.is_none() || is_pending)
                                .loading(is_pending)
                                .child({
                                    h_flex()
                                        .w_full()
                                        .items_center()
                                        .justify_start()
                                        .gap_2()
                                        .child(
                                            pioneer_desktop_foundation::catalog_sidebar_menu_icon(
                                                PioneerIconName::RefreshCw,
                                            )
                                            .id("skills-details-sidebar-update-icon"),
                                        )
                                        .child(pioneer_desktop_foundation::catalog_sidebar_label(
                                            t!("skills.button.update").to_string(),
                                        ))
                                })
                                .on_click({
                                    let desktop_entity = desktop_entity.clone();
                                    let selected_skill = selected_skill.clone();
                                    move |_, window, cx| {
                                        let Some(selected_skill) = selected_skill.clone() else {
                                            return;
                                        };
                                        let _ = desktop_entity.update(cx, |view, cx| {
                                            view.open_skill_update_dialog(
                                                selected_skill.skill_id.clone(),
                                                window,
                                                cx,
                                            );
                                            cx.notify();
                                        });
                                    }
                                }),
                        )
                    })
                    .when(can_manage && lifecycle_editable, |this| {
                        this.child(
                            Button::new("skills-details-sidebar-uninstall")
                                .ghost()
                                .justify_start()
                                .px_2()
                                .group("skills-details-sidebar-uninstall-btn")
                                .disabled(!is_connected || selected_skill.is_none() || is_pending)
                                .child({
                                    h_flex()
                                        .w_full()
                                        .items_center()
                                        .justify_start()
                                        .gap_2()
                                        .child(
                                            pioneer_desktop_foundation::catalog_sidebar_menu_icon(
                                                PioneerIconName::Trash,
                                            )
                                            .id("skills-details-sidebar-uninstall-icon"),
                                        )
                                        .child(pioneer_desktop_foundation::catalog_sidebar_label(
                                            t!("skills.button.uninstall").to_string(),
                                        ))
                                })
                                .on_click({
                                    let desktop_entity = desktop_entity.clone();
                                    let selected_skill = selected_skill.clone();
                                    move |_, _, cx| {
                                        let Some(selected_skill) = selected_skill.clone() else {
                                            return;
                                        };
                                        let _ = desktop_entity.update(cx, |view, cx| {
                                            view.uninstall_skill(
                                                selected_skill.skill_id.clone(),
                                                cx,
                                            );
                                            cx.notify();
                                        });
                                    }
                                }),
                        )
                    }),
            )
            .into_any_element()
    }
}

use crate::catalog::CatalogInput;
use std::{ops::Deref, sync::Arc};
pub(crate) struct CatalogSidebar {
    pub owner: WeakEntity<SkillsCatalogView>,
    pub input: Arc<CatalogInput>,
}
impl Deref for CatalogSidebar {
    type Target = CatalogInput;
    fn deref(&self) -> &CatalogInput {
        &self.input
    }
}
impl Render for CatalogSidebar {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if matches!(
            self.navigation_input.destination(),
            pioneer_client::navigation::SemanticDestination::Skills { skill_id: Some(_) }
        ) {
            self.render_skill_details_sidebar(cx)
        } else {
            self.render_skills_sidebar(cx)
        }
    }
}
