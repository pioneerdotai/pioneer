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
        let mut actions = v_flex().pt_4().px_2().gap_1();
        if view.selected.is_some() {
            let label = if component.is_some() {
                selected
                    .as_ref()
                    .map(|plugin| {
                        t!("plugins.back_to_named_plugin", name = plugin.name.as_str()).to_string()
                    })
                    .unwrap_or_else(|| t!("plugins.back_to_plugin").to_string())
            } else {
                t!("plugins.back").to_string()
            };
            actions = actions.child(
                sidebar_action(
                    "plugin-sidebar-back",
                    label,
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
                .child(actions)
                .child(div().flex_1().min_h_0().child(component.sidebar))
                .into_any_element();
        }
        if let Some(plugin) = selected.filter(|_| manage) {
            if plugin.state != "removing" {
                let enable_owner = self.owner.clone();
                let enabled = plugin.clone();
                let update_owner = self.owner.clone();
                let update = plugin.clone();
                actions = actions
                    .child(
                        sidebar_action(
                            "plugin-sidebar-enabled",
                            t!(if plugin.enabled {
                                "plugins.disable"
                            } else {
                                "plugins.enable"
                            })
                            .to_string(),
                            SidebarIcon::Check,
                            false,
                            busy || plugin.state != "installed",
                            cx,
                        )
                        .on_click(move |_, _, cx| {
                            let _ = enable_owner
                                .update(cx, |view, cx| view.set_enabled(enabled.clone(), cx));
                        }),
                    )
                    .child(
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
        v_flex()
            .size_full()
            .bg(cx.theme().sidebar)
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
    Button::new(id)
        .ghost()
        .justify_start()
        .px_2()
        .disabled(disabled)
        .accessibility_label(label.clone())
        .child(
            h_flex()
                .w_full()
                .items_center()
                .gap_2()
                .child(pioneer_desktop_foundation::catalog_sidebar_icon(
                    icon, filled, cx,
                ))
                .child(pioneer_desktop_foundation::catalog_sidebar_label(label)),
        )
}
