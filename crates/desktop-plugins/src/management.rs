use super::*;
use pioneer_client::plugins::PluginManagementIntent;

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
            description: None,
            metadata: Default::default(),
            enabled: true,
            allow_implicit_invocation: false,
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

impl PluginsView {
    pub(super) fn mutate(
        &mut self,
        plugin: PluginItem,
        intent: PluginManagementIntent,
        cx: &mut Context<Self>,
    ) {
        if self.busy(cx) {
            return;
        }
        self.send_intent(
            PluginIntent::Mutate {
                plugin_id: plugin.id,
                expected_revision: plugin.revision,
                intent,
            },
            cx,
        );
    }
    pub(super) fn set_enabled(&mut self, plugin: PluginItem, cx: &mut Context<Self>) {
        if self.busy(cx) {
            return;
        }
        self.send_intent(
            PluginIntent::SetEnabled {
                plugin_id: plugin.id,
                expected_revision: plugin.revision,
                enabled: !plugin.enabled,
                allow_implicit_invocation: None,
            },
            cx,
        );
    }
    pub(super) fn set_implicit(&mut self, plugin: PluginItem, cx: &mut Context<Self>) {
        if self.busy(cx) {
            return;
        }
        self.send_intent(
            PluginIntent::SetEnabled {
                plugin_id: plugin.id,
                expected_revision: plugin.revision,
                enabled: plugin.enabled,
                allow_implicit_invocation: Some(!plugin.allow_implicit_invocation),
            },
            cx,
        );
    }
    pub(super) fn open_component(
        &mut self,
        plugin: &PluginItem,
        component: &PluginComponentItem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(workspace) = self.workspace.clone() else {
            return;
        };
        let _ = (workspace, window);
        self.client.navigate(
            pioneer_client::navigation::NavigationIntent::Navigate {
                destination: pioneer_client::navigation::SemanticDestination::Plugin {
                    plugin_id: plugin.id.clone(),
                    component: Some(PluginComponentKey {
                        kind: component.kind.clone(),
                        member_key: component.member_key.clone(),
                    }),
                },
            },
            None,
        );
        self.sync_publications(cx);
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
        h_flex()
            .gap_2()
            .when(
                manage
                    && matches!(component.status.as_str(), "failed" | "removed_by_user")
                    && plugin.state == "installed"
                    && component.diagnostic.as_deref() != Some("component_path_denied"),
                |row| {
                    let parent = plugin.clone();
                    let component = component.clone();
                    let restore = component.status == "removed_by_user";
                    row.child(
                        Button::new(SharedString::from(format!(
                            "plugin-retry-{}-{}-{}",
                            plugin.id, component.kind, component.member_key
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
        if self.busy(cx) {
            return;
        }
        // A native prompt can deactivate its parent window. Retain an explicit
        // consumer until the confirmed command is admitted, so focus loss does
        // not suspend the plugin controller underneath this confirmation.
        let confirmation_demand = self.workspace.as_deref().map(|workspace| {
            self.client
                .acquire_plugin_demand(workspace, Some(&parent.id))
        });
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
        let authorization = self.authorization;
        self.restore_confirmation = Some(cx.spawn(async move |view: WeakEntity<Self>, cx| {
            let _confirmation_demand = confirmation_demand;
            let confirmed = answer.await == Ok(0);
            let _ = view.update(cx, |view, cx| {
                if view.workspace != workspace
                    || view.connection != connection
                    || view.client.authorization_permissions_epoch() != authorization
                {
                    return;
                }
                view.restore_confirmation = None;
                if !confirmed {
                    cx.notify();
                    return;
                }
                let current = view.selected_plugin();
                if let Some(intent) = confirmed_restore_intent(confirmed, &parent, &key, current) {
                    view.mutate(parent, intent, cx);
                } else {
                    // The authoritative revision or member changed while the
                    // native prompt was open. Refresh instead of resetting it.
                    view.refresh(cx);
                }
                cx.notify();
            });
        }));
        cx.notify();
    }
    pub(super) fn confirm_remove(
        &mut self,
        parent: PluginItem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.busy(cx) {
            return;
        }
        // A native prompt can deactivate its parent window. Retain an explicit
        // consumer until the confirmed command is admitted, so focus loss does
        // not suspend the plugin controller underneath this confirmation.
        let confirmation_demand = self.workspace.as_deref().map(|workspace| {
            self.client
                .acquire_plugin_demand(workspace, Some(&parent.id))
        });
        let answer = window.prompt(
            PromptLevel::Warning,
            &t!("plugins.remove_title", name = parent.name.as_str()).to_string(),
            Some(&t!("plugins.remove_notice").to_string()),
            &[
                PromptButton::new(t!("plugins.keep_data").to_string()),
                PromptButton::new(t!("plugins.purge_data").to_string()),
                PromptButton::cancel(t!("plugins.cancel").to_string()),
            ],
            cx,
        );
        let workspace = self.workspace.clone();
        let connection = self.connection;
        let authorization = self.authorization;
        self.restore_confirmation = Some(cx.spawn(async move |view: WeakEntity<Self>, cx| {
            let _confirmation_demand = confirmation_demand;
            let answer = answer.await;
            let _ = view.update(cx, |view, cx| {
                if view.workspace != workspace
                    || view.connection != connection
                    || view.client.authorization_permissions_epoch() != authorization
                {
                    return;
                }
                view.restore_confirmation = None;
                if let Ok(choice @ (0 | 1)) = answer {
                    if view
                        .selected_plugin()
                        .is_some_and(|p| p.id == parent.id && p.revision == parent.revision)
                    {
                        view.mutate(
                            parent,
                            PluginManagementIntent::Remove {
                                purge_data: choice == 1,
                            },
                            cx,
                        );
                    } else if view.selected.as_deref() == Some(parent.id.as_str()) {
                        view.set_presentation_error(Some(t!("plugins.change_failed").to_string()));
                    }
                }
                cx.notify();
            });
        }));
        cx.notify();
    }

    pub(super) fn upload_recovery(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let publication = self.upload.read(cx).publication()?;
        let key = match &publication.target {
            SkillUploadTarget::PluginUpdatePreview { plugin_id, .. } => plugin_id.clone(),
            _ => format!("upload:{}", publication.operation_id),
        };
        let operation = self.input.operations.get(&key);
        if !publication.mutation_uncertain
            && !operation.is_some_and(|o| o.uncertain || o.error.is_some())
        {
            return None;
        }
        let intent = match &publication.target {
            SkillUploadTarget::PluginInstall | SkillUploadTarget::PluginPreview => {
                PluginIntent::Install {
                    upload_operation: publication.operation_id,
                }
            }
            SkillUploadTarget::PluginUpdatePreview { .. } => PluginIntent::ApplyUpdate {
                upload_operation: publication.operation_id,
                confirm_changes: true,
            },
            _ => return None,
        };
        Some(
            Button::new("plugin-retry-exact-upload")
                .outline()
                .label(t!("plugins.retry_upload").to_string())
                .disabled(!self.connected() || operation.is_some_and(|o| o.busy))
                .on_click(cx.listener(move |view, _, _, cx| view.send_intent(intent.clone(), cx)))
                .into_any_element(),
        )
    }

    pub(super) fn management_controls(
        &self,
        plugin: &PluginItem,
        manage: bool,
        busy: bool,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        if !manage {
            return Some(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child(t!("plugins.read_only").to_string())
                    .into_any_element(),
            );
        }
        let update_preview = self.upload.read(cx).publication().and_then(|publication| {
            let SkillUploadTarget::PluginUpdatePreview {
                plugin_id,
                expected_revision,
            } = &publication.target
            else {
                return None;
            };
            if plugin_id != &plugin.id || publication.state != SkillUploadState::Succeeded {
                return None;
            }
            Some((
                publication.operation_id,
                *expected_revision,
                publication.plugin_update_preview.as_ref()?.preview.clone(),
            ))
        });
        if self.preview_dismissed || update_preview.is_none() {
            return None;
        }
        Some(
            v_flex()
                .gap_4()
                .when_some(
                    update_preview,
                    |view, (operation_id, expected_revision, preview)| {
                        let stale = expected_revision != plugin.revision;
                        view.child(
                            v_flex()
                                .gap_2()
                                .p_3()
                                .border_1()
                                .border_color(cx.theme().border)
                                .child(t!("plugins.update_preview").to_string())
                                .child(plugin.name.clone())
                                .child(preview.package.name)
                                .when_some(preview.package.description, |view, description| {
                                    view.child(div().text_sm().child(description))
                                })
                                .when(preview.supporting_files_changed, |view| {
                                    view.child(
                                        div().text_sm().child(
                                            t!("plugins.supporting_files_changed").to_string(),
                                        ),
                                    )
                                })
                                .children(preview.added.iter().map(|component| {
                                    div().text_sm().child(
                                        t!(
                                            "plugins.added_component",
                                            name = component.member_key.as_str()
                                        )
                                        .to_string(),
                                    )
                                }))
                                .children(preview.updated.iter().map(|component| {
                                    div().text_sm().child(
                                        t!(
                                            "plugins.updated_component",
                                            name = component.member_key.as_str()
                                        )
                                        .to_string(),
                                    )
                                }))
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
                                        t!(
                                            "plugins.identity_reset",
                                            name = key.member_key.as_str()
                                        )
                                        .to_string(),
                                    )
                                }))
                                .children(preview.authorization_changes.iter().map(|key| {
                                    div().text_sm().child(
                                        t!(
                                            "plugins.authorization_change",
                                            name = key.member_key.as_str()
                                        )
                                        .to_string(),
                                    )
                                }))
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
                                                    view.send_intent(
                                                        PluginIntent::ApplyUpdate {
                                                            upload_operation: operation_id,
                                                            confirm_changes: true,
                                                        },
                                                        cx,
                                                    );
                                                })),
                                        )
                                        .child(
                                            Button::new("plugin-cancel-update")
                                                .ghost()
                                                .label(t!("plugins.cancel").to_string())
                                                .disabled(
                                                    self.applying_update.as_ref().is_some_and(
                                                        |(parent, _)| {
                                                            self.input
                                                                .operations
                                                                .get(parent)
                                                                .is_some_and(|o| o.busy)
                                                        },
                                                    ),
                                                )
                                                .on_click(cx.listener(|view, _, _, cx| {
                                                    view.preview_dismissed = true;
                                                    cx.notify();
                                                })),
                                        ),
                                ),
                        )
                    },
                )
                .into_any_element(),
        )
    }
}
