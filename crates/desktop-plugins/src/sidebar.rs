use super::*;
use gpui_kit::assets::IconName as SidebarIcon;

struct PluginsSidebar {
    owner: WeakEntity<PluginsView>,
    _changes: Subscription,
}
impl PluginsView {
    pub fn sidebar_surface(owner: &Entity<Self>, cx: &mut App) -> AnyView {
        cx.new(|cx| PluginsSidebar {
            owner: owner.downgrade(),
            _changes: cx.observe(owner, |_, _, cx| cx.notify()),
        })
        .into()
    }
    pub(super) fn begin_install(&mut self, cx: &mut Context<Self>) {
        self.pick_source(SkillUploadTarget::PluginInstall, cx);
    }
}
impl Render for PluginsSidebar {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(owner) = self.owner.upgrade() else {
            return div().into_any_element();
        };
        let view = owner.read(cx);
        let manage = view
            .workspace
            .as_deref()
            .is_some_and(|workspace| view.client.plugins_management_allowed(workspace));
        let available = view.workspace.is_some() && view.connected();
        let busy = view.busy(cx);
        let selected = view.selected_plugin().cloned();
        let component = view.child_details.clone();
        let back_owner = self.owner.clone();
        let refresh_owner = self.owner.clone();
        let install_owner = self.owner.clone();
        let mut back = v_flex().pt_4().px_2().gap_1();
        let mut actions = v_flex().pt_4().px_2().gap_1();
        if view.selected.is_some() {
            back = back.child(
                sidebar_action(
                    "plugin-sidebar-back",
                    t!("plugins.back").to_string(),
                    SidebarIcon::ChevronLeft,
                    true,
                    false,
                    cx,
                )
                .on_click(move |_, _, cx| {
                    let _ = back_owner.update(cx, |view, cx| {
                        view.client.navigate(
                            pioneer_client::navigation::NavigationIntent::BackFromPlugin,
                            None,
                        );
                        view.sync_publications(cx);
                        cx.notify();
                    });
                }),
            );
        }
        if let Some(component) = component {
            return v_flex()
                .size_full()
                .bg(cx.theme().sidebar)
                .p_0()
                .gap_1()
                .child(back)
                .child(div().flex_1().min_h_0().child(component.sidebar))
                .into_any_element();
        }
        if let Some(plugin) = selected.filter(|_| manage) {
            if plugin.state != "removing" {
                let update_owner = self.owner.clone();
                let update = plugin.clone();
                actions = actions.child(
                    sidebar_action(
                        "plugin-sidebar-update",
                        t!("plugins.update").to_string(),
                        SidebarIcon::RefreshCw,
                        false,
                        busy || view.applying_update.is_some(),
                        cx,
                    )
                    .on_click(move |_, _, cx| {
                        let _ = update_owner.update(cx, |view, cx| {
                            view.pick_source(
                                SkillUploadTarget::PluginUpdatePreview {
                                    plugin_id: update.id.clone(),
                                    expected_revision: update.revision,
                                },
                                cx,
                            )
                        });
                    }),
                );
            }
            if !matches!(plugin.state.as_str(), "installed" | "retained") {
                let resume_owner = self.owner.clone();
                let resume = plugin.clone();
                actions = actions.child(
                    sidebar_action(
                        "plugin-sidebar-continue",
                        t!("plugins.continue").to_string(),
                        SidebarIcon::RefreshCw,
                        false,
                        busy,
                        cx,
                    )
                    .on_click(move |_, _, cx| {
                        let _ = resume_owner.update(cx, |view, cx| {
                            view.mutate(
                                resume.clone(),
                                pioneer_client::plugins::PluginManagementIntent::Continue,
                                cx,
                            )
                        });
                    }),
                );
            }
            let remove_owner = self.owner.clone();
            let remove_target = plugin.clone();
            actions = actions.child(
                sidebar_action(
                    "plugin-sidebar-remove",
                    t!("plugins.remove").to_string(),
                    SidebarIcon::Trash,
                    false,
                    busy,
                    cx,
                )
                .on_click(move |_, window, cx| {
                    let _ = remove_owner.update(cx, |view, cx| {
                        view.confirm_remove(remove_target.clone(), window, cx)
                    });
                }),
            );
        } else if view.selected.is_none() && manage {
            actions = actions.child(
                sidebar_action(
                    "plugins-sidebar-install",
                    t!("plugins.install").to_string(),
                    SidebarIcon::Plus,
                    true,
                    !available || busy || view.applying_update.is_some(),
                    cx,
                )
                .on_click(move |_, _, cx| {
                    let _ = install_owner.update(cx, |view, cx| view.begin_install(cx));
                }),
            );
        }
        if view.selected.is_none() {
            actions = actions.child(
                sidebar_action(
                    "plugins-sidebar-refresh",
                    t!("plugins.refresh").to_string(),
                    SidebarIcon::RefreshCw,
                    false,
                    !available || view.input.busy,
                    cx,
                )
                .loading(view.input.busy)
                .on_click(move |_, _, cx| {
                    let _ = refresh_owner.update(cx, |view, cx| view.refresh(cx));
                }),
            );
        }
        v_flex()
            .size_full()
            .bg(cx.theme().sidebar)
            .p_0()
            .gap_1()
            .when(view.selected.is_some(), |sidebar| sidebar.child(back))
            .child(actions)
            .into_any_element()
    }
}
fn sidebar_action(
    id: &'static str,
    label: String,
    icon: SidebarIcon,
    filled: bool,
    disabled: bool,
    cx: &App,
) -> Button {
    let is_back = matches!(icon, SidebarIcon::ChevronLeft);
    let icon = match icon {
        SidebarIcon::RefreshCw => Icon::new(icon).path("icons/refresh-cw.svg"),
        SidebarIcon::Trash => Icon::new(icon).path("icons/trash.svg"),
        _ => Icon::new(icon),
    };
    let icon_bg_hover = cx.theme().foreground.opacity(0.1);
    let icon = if is_back {
        div()
            .size_6()
            .rounded_full()
            .bg(cx.theme().foreground.opacity(0.075))
            .group_hover(id, move |style| style.bg(icon_bg_hover))
            .flex()
            .items_center()
            .justify_center()
            .child(icon.size_5().ml_neg_px().opacity(0.8))
    } else {
        pioneer_desktop_foundation::catalog_sidebar_icon(icon, filled, cx).when(filled, |icon| {
            icon.group_hover(id, move |style| style.bg(icon_bg_hover))
        })
    };
    Button::new(id)
        .ghost()
        .justify_start()
        .px_2()
        .group(id)
        .disabled(disabled)
        .accessibility_label(label.clone())
        .child(
            h_flex()
                .w_full()
                .items_center()
                .justify_start()
                .gap_2()
                .child(icon)
                .child(pioneer_desktop_foundation::catalog_sidebar_label(label)),
        )
}
