use crate::app::root::PioneerDesktop;
use gpui::{prelude::*, *};
use gpui_component::{avatar::Avatar, button::*, theme::ActiveTheme, *};
use pioneer_client::authorization::{
    CurrentPrincipalKindPresentation, CurrentPrincipalPresentation, current_principal_presentation,
};

const ACCOUNT_CONTENT_MAX_WIDTH_PX: f32 = 860.0;

impl PioneerDesktop {
    pub(in crate::app::settings) fn render_settings_account(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let desktop = cx.entity().clone();
        let principal = self.gateway.current_auth.as_ref().map(|auth| {
            let visible_member = self
                .administration
                .members()
                .find(|member| member.principal_id == auth.principal.id);
            current_principal_presentation(
                auth,
                visible_member,
                self.principal_presentation_capabilities(),
            )
        });
        let principal_avatar_path = self.gateway.current_auth.as_ref().and_then(|auth| {
            self.member_avatar_state
                .presentation(&auth.principal.id)
                .and_then(|avatar| avatar.cached_image_path.clone())
        });
        let devices = self.render_auth_sessions_content(desktop.clone(), cx);

        v_flex()
            .id("settings-account-scroll")
            .flex_1()
            .min_h_0()
            .overflow_y_scroll()
            .p_6()
            .bg(cx.theme().background)
            .child(
                h_flex().w_full().justify_center().child(
                    v_flex()
                        .w_full()
                        .max_w(px(ACCOUNT_CONTENT_MAX_WIDTH_PX))
                        .gap_5()
                        .when_some(principal, |page, principal| {
                            page.child(
                                v_flex()
                                    .w_full()
                                    .rounded_2xl()
                                    .bg(cx.theme().sidebar)
                                    .child(Self::render_current_principal_setting(
                                        principal,
                                        principal_avatar_path,
                                        desktop.clone(),
                                        cx,
                                    )),
                            )
                        })
                        .child(
                            h_flex()
                                .w_full()
                                .justify_between()
                                .items_center()
                                .child(
                                    v_flex()
                                        .child(
                                            div()
                                                .text_xl()
                                                .font_semibold()
                                                .child(t!("settings.devices.title").to_string()),
                                        )
                                        .child(
                                            div().text_sm().opacity(0.6).child(
                                                t!("settings.devices.description").to_string(),
                                            ),
                                        ),
                                )
                                .child(
                                    div().pt_1p5().child(
                                        Button::new("devices-create-activation")
                                            .ghost()
                                            .justify_start()
                                            .px_2()
                                            .group("new-device-btn")
                                            .child(
                                                div()
                                                    .flex_none()
                                                    .text_sm()
                                                    .line_height(relative(1.))
                                                    .child(
                                                        t!("settings.devices.activate_device")
                                                            .to_string(),
                                                    ),
                                            )
                                            .child({
                                                let icon_bg = cx.theme().foreground.opacity(0.075);
                                                let icon_bg_hover =
                                                    cx.theme().foreground.opacity(0.1);
                                                div()
                                                    .id("new-device-icon")
                                                    .size_6()
                                                    .rounded_full()
                                                    .bg(icon_bg)
                                                    .group_hover("new-device-btn", move |style| {
                                                        style.bg(icon_bg_hover)
                                                    })
                                                    .flex()
                                                    .items_center()
                                                    .justify_center()
                                                    .child(Icon::new(IconName::Plus).size_4())
                                            })
                                            .on_click({
                                                let desktop = desktop.clone();
                                                move |_, window, cx| {
                                                    let _ = desktop.update(cx, |view, cx| {
                                                        view.create_desktop_activation(window, cx)
                                                    });
                                                }
                                            }),
                                    ),
                                ),
                        )
                        .child(devices),
                ),
            )
            .into_any_element()
    }

    fn render_current_principal_setting(
        principal: CurrentPrincipalPresentation,
        avatar_path: Option<std::path::PathBuf>,
        desktop: Entity<Self>,
        _cx: &mut Context<Self>,
    ) -> AnyElement {
        let kind = match principal.kind {
            CurrentPrincipalKindPresentation::Superuser => {
                t!("settings.profile.kind.superuser").to_string()
            }
            CurrentPrincipalKindPresentation::Member => {
                t!("settings.profile.kind.member").to_string()
            }
            CurrentPrincipalKindPresentation::Unknown => {
                t!("settings.profile.kind.unknown").to_string()
            }
        };

        h_flex()
            .id("settings-current-principal")
            .w_full()
            .items_center()
            .justify_between()
            .gap_4()
            .px_4()
            .py_4()
            .on_click(move |_, window, cx| {
                let _ = desktop.update(cx, |view, cx| view.open_profile_editor(window, cx));
            })
            .child(
                h_flex()
                    .min_w_0()
                    .flex_1()
                    .items_center()
                    .gap_3()
                    .child(
                        Avatar::new()
                            .large()
                            .name(principal.display_name.clone())
                            .when_some(avatar_path, |avatar, path| avatar.src(path)),
                    )
                    .child(
                        v_flex()
                            .min_w_0()
                            .child(div().text_sm().font_medium().child(principal.display_name))
                            .child(
                                div()
                                    .text_xs()
                                    .opacity(0.6)
                                    .child(format!("@{}", principal.nickname)),
                            )
                            .child(div().text_xs().opacity(0.6).child(kind)),
                    ),
            )
            .child(Icon::new(IconName::ChevronRight).size_4().opacity(0.6))
            .into_any_element()
    }
}
