use crate::desktop_navigation::{
    OpenAdministration, OpenMcp, OpenProviders, OpenSettings, OpenSkills, OpenThreads,
};
use crate::{
    app::root::{MainContentView, PioneerDesktop},
    assets::PioneerIconName,
};
use gpui_kit::component::{Icon, button::*, separator::Separator, theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};

impl PioneerDesktop {
    pub(crate) fn render_bottom_bar(
        &self,
        action_region: &FocusHandle,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        pioneer_observability::record_qualification_diagnostic!(record_render(
            pioneer_observability::RenderRegion::BottomBar
        ));
        let can_manage_capabilities = self
            .principal_presentation_capabilities()
            .can_manage_capabilities;
        let is_threads_view_active = matches!(
            self.main_content_view(),
            MainContentView::Threads | MainContentView::AgentsDoc
        );
        let is_providers_view_active = self.main_content_view() == MainContentView::Providers;
        let is_administration_view_active =
            self.main_content_view() == MainContentView::Administration;
        let is_settings_view_active = self.main_content_view() == MainContentView::Settings;
        let is_mcp_view_active = matches!(
            self.main_content_view(),
            MainContentView::Mcp | MainContentView::McpDetails
        );
        let is_skills_view_active = matches!(
            self.main_content_view(),
            MainContentView::Skills | MainContentView::SkillDetails
        );
        h_flex()
            .justify_between()
            .items_center()
            .px_2()
            .h_8()
            .border_t_1()
            .border_color(cx.theme().border)
            .child(
                h_flex()
                    .items_center()
                    .gap_1()
                    .child(
                        Button::new("bottom-bar-open-threads")
                            .ghost()
                            .small()
                            .compact()
                            .child(
                                Icon::new(PioneerIconName::FolderTree)
                                    .size_3p5()
                                    .opacity(0.6)
                                    .when(is_threads_view_active, |this| {
                                        this.opacity(1.0).text_color(cx.theme().blue)
                                    }),
                            )
                            .on_click({
                                let target = action_region.clone();
                                move |_, window, cx| {
                                    target.dispatch_action(&OpenThreads, window, cx)
                                }
                            }),
                    )
                    .child(Separator::vertical().h_4().mx_0p5())
                    .child(
                        Button::new("bottom-bar-open-providers")
                            .ghost()
                            .small()
                            .compact()
                            .child(
                                Icon::new(IconName::Bot)
                                    .size_3p5()
                                    .opacity(0.6)
                                    .when(is_providers_view_active, |this| {
                                        this.opacity(1.0).text_color(cx.theme().blue)
                                    }),
                            )
                            .on_click({
                                let target = action_region.clone();
                                move |_, window, cx| {
                                    target.dispatch_action(&OpenProviders, window, cx)
                                }
                            }),
                    )
                    .when(can_manage_capabilities, |this| {
                        this.child(
                            Button::new("bottom-bar-open-mcp")
                                .ghost()
                                .small()
                                .compact()
                                .child(
                                    Icon::new(PioneerIconName::Mcp)
                                        .size_3p5()
                                        .opacity(0.6)
                                        .when(is_mcp_view_active, |this| {
                                            this.opacity(1.0).text_color(cx.theme().blue)
                                        }),
                                )
                                .on_click({
                                    let target = action_region.clone();
                                    move |_, window, cx| {
                                        target.dispatch_action(&OpenMcp, window, cx)
                                    }
                                }),
                        )
                    })
                    .child(
                        Button::new("bottom-bar-open-skills")
                            .ghost()
                            .small()
                            .compact()
                            .child(
                                Icon::new(PioneerIconName::Zap)
                                    .size_3p5()
                                    .opacity(0.6)
                                    .when(is_skills_view_active, |this| {
                                        this.opacity(1.0).text_color(cx.theme().blue)
                                    }),
                            )
                            .on_click({
                                let target = action_region.clone();
                                move |_, window, cx| target.dispatch_action(&OpenSkills, window, cx)
                            }),
                    )
                    .child(Separator::vertical().h_4().mx_0p5())
                    .child(
                        Button::new("bottom-bar-open-administration")
                            .ghost()
                            .small()
                            .compact()
                            .child(
                                Icon::new(PioneerIconName::Users)
                                    .size_3p5()
                                    .opacity(0.6)
                                    .when(is_administration_view_active, |this| {
                                        this.opacity(1.0).text_color(cx.theme().blue)
                                    }),
                            )
                            .on_click({
                                let target = action_region.clone();
                                move |_, window, cx| {
                                    target.dispatch_action(&OpenAdministration, window, cx)
                                }
                            }),
                    )
                    .child(
                        Button::new("bottom-bar-open-settings")
                            .ghost()
                            .small()
                            .compact()
                            .child(
                                Icon::new(PioneerIconName::Bolt)
                                    .size_3p5()
                                    .opacity(0.6)
                                    .when(is_settings_view_active, |this| {
                                        this.opacity(1.0).text_color(cx.theme().blue)
                                    }),
                            )
                            .on_click({
                                let target = action_region.clone();
                                move |_, window, cx| {
                                    target.dispatch_action(&OpenSettings, window, cx)
                                }
                            }),
                    ),
            )
            .into_any_element()
    }
}
