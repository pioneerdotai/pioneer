use super::*;

// Local confirmation revalidation; the existing Gateway mutation remains
// authoritative for rights, ownership and revision. No grants are transferred.
fn confirmed_restore_intent(
    confirmed: bool,
    expected: &PluginItem,
    key: &PluginComponentKey,
    current: Option<&PluginItem>,
) -> Option<PluginManagementIntent> {
    let current = current?;
    (confirmed
        && current.id == expected.id
        && current.revision == expected.revision
        && current.state == "installed"
        && current.components.iter().any(|component| {
            component.kind == key.kind
                && component.member_key == key.member_key
                && component.status == "removed_by_user"
                && component.skill_id.is_none()
                && component.mcp_installation_id.is_none()
        }))
    .then(|| PluginManagementIntent::Retry {
        components: vec![key.clone()],
        restore_removed: true,
    })
}

#[cfg(test)]
mod restore_tests {
    use super::*;
    #[::core::prelude::v1::test]
    fn cancelled_stale_and_failed_retry_targets_cannot_send_identity_reset() {
        let key = PluginComponentKey {
            kind: "skill".into(),
            member_key: "one".into(),
        };
        let mut parent = PluginItem {
            id: "P".repeat(21),
            name: "fixture".into(),
            version: None,
            enabled: true,
            state: "installed".into(),
            revision: 4,
            status: "partial".into(),
            diagnostics: vec![],
            components: vec![
                PluginComponentItem {
                    kind: key.kind.clone(),
                    member_key: key.member_key.clone(),
                    status: "removed_by_user".into(),
                    diagnostic: None,
                    skill_id: None,
                    mcp_installation_id: None,
                    runtime_status: None,
                },
                PluginComponentItem {
                    kind: "mcp".into(),
                    member_key: "sibling".into(),
                    status: "removed_by_user".into(),
                    diagnostic: None,
                    skill_id: None,
                    mcp_installation_id: None,
                    runtime_status: None,
                },
            ],
        };
        assert!(confirmed_restore_intent(false, &parent, &key, Some(&parent)).is_none());
        let intent = confirmed_restore_intent(true, &parent, &key, Some(&parent)).unwrap();
        let PluginManagementIntent::Retry {
            components,
            restore_removed,
        } = intent
        else {
            panic!("expected exact restore retry")
        };
        assert!(restore_removed);
        assert_eq!(components, vec![key.clone()]);
        let expected = parent.clone();
        parent.revision += 1;
        assert!(confirmed_restore_intent(true, &expected, &key, Some(&parent)).is_none());
        parent = expected.clone();
        parent.id = "Q".repeat(21);
        assert!(confirmed_restore_intent(true, &expected, &key, Some(&parent)).is_none());
        parent = expected.clone();
        parent.components[0].status = "failed".into();
        parent.components[0].skill_id =
            Some(pioneer_client::skills::types::SkillId::new("S".repeat(21)).unwrap());
        assert!(confirmed_restore_intent(true, &expected, &key, Some(&parent)).is_none());
        parent = expected.clone();
        parent.state = "interrupted".into();
        assert!(confirmed_restore_intent(true, &expected, &key, Some(&parent)).is_none());
        assert!(confirmed_restore_intent(true, &expected, &key, None).is_none());
    }
}
use gpui_kit::base::{Checkbox, CheckboxState};
use pioneer_client::plugins::{PluginManagementIntent, PluginsMutateParams};
impl PluginsView {
    fn mutate(
        &mut self,
        plugin: PluginItem,
        intent: PluginManagementIntent,
        cx: &mut Context<Self>,
    ) {
        if !self.management.begin() {
            return;
        }
        let Some(workspace) = self.workspace.clone() else {
            self.management.complete(false);
            return;
        };
        let client = self.client.clone();
        let connection = self.connection;
        self.update_preview = None;
        self.remove_confirmation = false;
        self.mutation = Some(cx.spawn(async move |view, cx| {
            let request_workspace = workspace.clone();
            let result = cx
                .background_spawn(async move {
                    client.mutate_plugin(PluginsMutateParams {
                        workspace_id: request_workspace,
                        plugin_id: plugin.id,
                        expected_revision: plugin.revision,
                        intent,
                    })
                })
                .await;
            let _ = view.update(cx, |view, cx| {
                if view.workspace.as_ref() != Some(&workspace) || view.connection != connection {
                    return;
                }
                view.management.complete(result.is_ok());
                // Refetch even on timeout/stale/network: never optimistically
                // reopen a gate whose authoritative outcome is uncertain.
                view.refresh(cx);
                cx.notify();
            });
        }));
        cx.notify();
    }
    fn set_enabled(&mut self, plugin: PluginItem, cx: &mut Context<Self>) {
        if !self.management.begin() {
            return;
        }
        let Some(workspace) = self.workspace.clone() else {
            self.management.complete(false);
            return;
        };
        let client = self.client.clone();
        let connection = self.connection;
        self.mutation = Some(cx.spawn(async move |view, cx| {
            let request_workspace = workspace.clone();
            let result = cx
                .background_spawn(async move {
                    client.set_plugin_enabled(PluginsSetEnabledParams {
                        workspace_id: request_workspace,
                        plugin_id: plugin.id,
                        expected_revision: plugin.revision,
                        enabled: !plugin.enabled,
                    })
                })
                .await;
            let _ = view.update(cx, |view, cx| {
                if view.workspace.as_ref() != Some(&workspace) || view.connection != connection {
                    return;
                }
                view.management.complete(result.is_ok());
                view.refresh(cx);
                cx.notify();
            });
        }));
        cx.notify();
    }
    fn open_component(
        &mut self,
        plugin: &PluginItem,
        component: &PluginComponentItem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(workspace) = self.workspace.clone() else {
            return;
        };
        self.child_details =
            (self.component_details)(&workspace, &plugin.id, component, window, cx);
        cx.notify();
    }
    pub(super) fn component_controls(
        &self,
        plugin: &PluginItem,
        component: &PluginComponentItem,
        manage: bool,
        busy: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let parent = plugin.clone();
        let child = component.clone();
        h_flex()
            .gap_2()
            .when(
                component.skill_id.is_some() || component.mcp_installation_id.is_some(),
                |row| {
                    row.child(
                        Button::new(SharedString::from(format!(
                            "plugin-component-settings-{}-{}",
                            component.kind, component.member_key
                        )))
                        .ghost()
                        .label(t!("plugins.settings").to_string())
                        .disabled(busy)
                        .on_click(cx.listener(
                            move |view, _, window, cx| {
                                view.open_component(&parent, &child, window, cx)
                            },
                        )),
                    )
                },
            )
            .when(
                manage
                    && matches!(component.status.as_str(), "failed" | "removed_by_user")
                    && plugin.state == "installed",
                |row| {
                    let parent = plugin.clone();
                    let component = component.clone();
                    let restore = component.status == "removed_by_user";
                    row.child(
                        Button::new(SharedString::from(format!(
                            "plugin-retry-{}-{}",
                            component.kind, component.member_key
                        )))
                        .outline()
                        .label(
                            t!(if restore {
                                "plugins.restore"
                            } else {
                                "plugins.retry"
                            })
                            .to_string(),
                        )
                        .disabled(busy)
                        .on_click(cx.listener(
                            move |view, _, window, cx| {
                                if restore {
                                    view.confirm_restore_component(&parent, &component, window, cx);
                                    return;
                                }
                                view.mutate(
                                    parent.clone(),
                                    PluginManagementIntent::Retry {
                                        components: vec![PluginComponentKey {
                                            kind: component.kind.clone(),
                                            member_key: component.member_key.clone(),
                                        }],
                                        restore_removed: false,
                                    },
                                    cx,
                                )
                            },
                        )),
                    )
                },
            )
            .into_any_element()
    }
    fn confirm_restore_component(
        &mut self,
        parent: &PluginItem,
        component: &PluginComponentItem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.restore_confirmation.is_some() || self.management.busy {
            return;
        }
        let answer = window.prompt(
            PromptLevel::Warning,
            &t!(
                if component.kind == "skill" {
                    "plugins.restore_skill_title"
                } else {
                    "plugins.restore_mcp_title"
                },
                name = component.member_key.as_str()
            )
            .to_string(),
            Some(&t!("plugins.restore_notice").to_string()),
            &[
                PromptButton::new(t!("plugins.restore").to_string()),
                PromptButton::cancel(t!("plugins.cancel").to_string()),
            ],
            cx,
        );
        let parent = parent.clone();
        let key = PluginComponentKey {
            kind: component.kind.clone(),
            member_key: component.member_key.clone(),
        };
        let workspace = self.workspace.clone();
        let connection = self.connection;
        self.restore_confirmation = Some(cx.spawn(async move |view: WeakEntity<Self>, cx| {
            let confirmed = answer.await == Ok(0);
            let _ = view.update(cx, |view, cx| {
                if view.workspace != workspace || view.connection != connection {
                    return;
                }
                view.restore_confirmation = None;
                if !confirmed {
                    cx.notify();
                    return;
                }
                let current = view
                    .catalog
                    .plugins
                    .iter()
                    .find(|plugin| plugin.id == parent.id);
                if let Some(intent) = confirmed_restore_intent(confirmed, &parent, &key, current) {
                    view.mutate(parent, intent, cx);
                } else {
                    // The authoritative revision or member changed while the
                    // native prompt was open. Refresh instead of resetting it.
                    view.management.complete(false);
                    view.refresh(cx);
                }
                cx.notify();
            });
        }));
        cx.notify();
    }
    pub(super) fn management_controls(
        &self,
        plugin: &PluginItem,
        manage: bool,
        busy: bool,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if !manage {
            return div()
                .text_sm()
                .text_color(cx.theme().muted_foreground)
                .child(t!("plugins.read_only").to_string())
                .into_any_element();
        }
        let enable = plugin.clone();
        let folder = plugin.clone();
        let archive = plugin.clone();
        let resume = plugin.clone();
        let remove = plugin.clone();
        let confirmed_remove = plugin.clone();
        let purge = self.purge_data;
        let checkbox_view = cx.entity().downgrade();
        v_flex()
            .gap_2()
            .child(
                h_flex()
                    .gap_2()
                    .flex_wrap()
                    .when(plugin.state != "removing", |row| {
                        row.child(
                            Button::new("plugin-enabled")
                                .outline()
                                .label(
                                    t!(if plugin.enabled {
                                        "plugins.disable"
                                    } else {
                                        "plugins.enable"
                                    })
                                    .to_string(),
                                )
                                .disabled(busy || plugin.state != "installed")
                                .on_click(cx.listener(move |view, _, _, cx| {
                                    view.set_enabled(enable.clone(), cx)
                                })),
                        )
                        .child(
                            Button::new("plugin-update-folder")
                                .outline()
                                .label(t!("plugins.update_folder").to_string())
                                .disabled(busy)
                                .on_click(cx.listener(move |view, _, _, cx| {
                                    view.update_target = Some((folder.id.clone(), folder.revision));
                                    view.update_preview = None;
                                    view.pick_source(true, cx);
                                })),
                        )
                        .child(
                            Button::new("plugin-update-archive")
                                .outline()
                                .label(t!("plugins.update_archive").to_string())
                                .disabled(busy)
                                .on_click(cx.listener(move |view, _, _, cx| {
                                    view.update_target =
                                        Some((archive.id.clone(), archive.revision));
                                    view.update_preview = None;
                                    view.pick_source(false, cx);
                                })),
                        )
                    })
                    .when(plugin.state != "installed", |row| {
                        row.child(
                            Button::new("plugin-continue")
                                .outline()
                                .label(t!("plugins.continue").to_string())
                                .disabled(busy)
                                .on_click(cx.listener(move |view, _, _, cx| {
                                    view.mutate(
                                        resume.clone(),
                                        PluginManagementIntent::Continue,
                                        cx,
                                    )
                                })),
                        )
                    })
                    .child(
                        Button::new("plugin-remove")
                            .outline()
                            .label(t!("plugins.remove").to_string())
                            .disabled(busy)
                            .on_click(cx.listener(move |view, _, _, cx| {
                                view.selected = Some(remove.id.clone());
                                view.remove_confirmation = true;
                                view.purge_data = false;
                                cx.notify();
                            })),
                    ),
            )
            .when(self.remove_confirmation, |view| {
                view.child(
                    v_flex()
                        .gap_2()
                        .p_3()
                        .border_1()
                        .border_color(cx.theme().border)
                        .child(t!("plugins.remove_notice").to_string())
                        .child(
                            h_flex()
                                .gap_2()
                                .items_center()
                                .child(
                                    Checkbox::new("plugin-purge-data")
                                        .checked(purge)
                                        .disabled(busy)
                                        .accessibility_label(t!("plugins.purge_data").to_string())
                                        .on_change(move |state, _, _, cx| {
                                            let _ = checkbox_view.update(cx, |view, cx| {
                                                view.purge_data = state == CheckboxState::Checked;
                                                cx.notify();
                                            });
                                        }),
                                )
                                .child(t!("plugins.purge_data").to_string()),
                        )
                        .child(
                            h_flex()
                                .gap_2()
                                .child(
                                    Button::new("plugin-confirm-remove")
                                        .outline()
                                        .label(t!("plugins.confirm_remove").to_string())
                                        .disabled(busy)
                                        .on_click(cx.listener(move |view, _, _, cx| {
                                            view.mutate(
                                                confirmed_remove.clone(),
                                                PluginManagementIntent::Remove {
                                                    purge_data: view.purge_data,
                                                },
                                                cx,
                                            )
                                        })),
                                )
                                .child(
                                    Button::new("plugin-cancel-remove")
                                        .ghost()
                                        .label(t!("plugins.cancel").to_string())
                                        .disabled(busy)
                                        .on_click(cx.listener(|view, _, _, cx| {
                                            view.remove_confirmation = false;
                                            view.purge_data = false;
                                            cx.notify();
                                        })),
                                ),
                        ),
                )
            })
            .when_some(self.update_preview.clone(), |view, (upload_id, preview)| {
                let parent = plugin.clone();
                let fingerprint = preview.package.fingerprint.clone();
                let stale = self
                    .update_target
                    .as_ref()
                    .is_none_or(|(id, rev)| id != &plugin.id || *rev != plugin.revision);
                view.child(
                    v_flex()
                        .gap_2()
                        .p_3()
                        .border_1()
                        .border_color(cx.theme().border)
                        .child(t!("plugins.update_preview").to_string())
                        .child(preview.package.name)
                        .children(
                            preview.package.components.iter().map(|component| {
                                div().text_sm().child(component.member_key.clone())
                            }),
                        )
                        .children(preview.removed.iter().map(|component| {
                            div().text_sm().child(
                                t!(
                                    "plugins.removed_component",
                                    name = component.member_key.as_str()
                                )
                                .to_string(),
                            )
                        }))
                        .children(preview.identity_resets.iter().map(|key| {
                            div().text_sm().child(
                                t!("plugins.identity_reset", name = key.member_key.as_str())
                                    .to_string(),
                            )
                        }))
                        .when(!preview.authorization_changes.is_empty(), |view| {
                            view.child(t!("plugins.authorization_changes").to_string())
                        })
                        .when(stale, |view| {
                            view.child(
                                div()
                                    .text_color(cx.theme().danger)
                                    .child(t!("plugins.stale_preview").to_string()),
                            )
                        })
                        .child(
                            h_flex()
                                .gap_2()
                                .child(
                                    Button::new("plugin-confirm-update")
                                        .primary()
                                        .label(t!("plugins.confirm_update").to_string())
                                        .disabled(busy || stale)
                                        .on_click(cx.listener(move |view, _, _, cx| {
                                            view.mutate(
                                                parent.clone(),
                                                PluginManagementIntent::Update {
                                                    upload_id: upload_id.clone(),
                                                    expected_fingerprint: fingerprint.clone(),
                                                    confirm_changes: true,
                                                },
                                                cx,
                                            )
                                        })),
                                )
                                .child(
                                    Button::new("plugin-cancel-update")
                                        .ghost()
                                        .label(t!("plugins.cancel").to_string())
                                        .disabled(busy)
                                        .on_click(cx.listener(|view, _, _, cx| {
                                            view.update_target = None;
                                            view.update_preview = None;
                                            cx.notify();
                                        })),
                                ),
                        ),
                )
            })
            .into_any_element()
    }
}
