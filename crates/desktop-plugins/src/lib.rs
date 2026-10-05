//! Retained Plugins management and a separate parent-only selection dialog.
#[macro_use]
extern crate rust_i18n;
rust_i18n::i18n!("locales", fallback = "en");
mod picker;
use gpui_kit::component::{button::*, scroll::ScrollableElement, theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
pub use picker::open_plugin_picker;
use pioneer_client::{
    core::ClientCore,
    plugins::PluginCatalogState,
    skills::{
        operations::{SkillUploadOperation, SkillUploadTarget},
        upload_flow::SkillUploadState,
    },
};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::sync::Arc;
struct UploadChanges(tokio::sync::watch::Sender<u64>);
impl ClientPublicationSink for UploadChanges {
    fn publish(&self, _: pioneer_client::core::ClientPublicationReference) {
        self.0.send_modify(|v| *v = v.saturating_add(1));
    }
}

pub(crate) fn status_label(status: &str) -> String {
    match status {
        "installed" => t!("plugins.installed"),
        "partial" => t!("plugins.partial"),
        "installing" | "starting" => t!("plugins.installing"),
        "interrupted" | "updating" | "removing" => t!("plugins.interrupted"),
        "failed" => t!("plugins.failed"),
        "disabled" => t!("plugins.disabled"),
        "authrequired" | "auth_required" => t!("plugins.auth_required"),
        "ready" => t!("plugins.authorized"),
        "notstarted" | "not_started" => t!("plugins.pending"),
        "error" | "stopped" | "offline" => t!("plugins.offline"),
        _ => t!("plugins.unavailable"),
    }
    .to_string()
}

pub struct PluginsView {
    registrar: Arc<dyn ClientBindingRegistrar>,
    upload_changes: Arc<UploadChanges>,
    upload_registration: Option<ClientBindingRegistration>,
    client: Arc<ClientCore>,
    workspace: Option<String>,
    connection: u64,
    active: bool,
    _changes: Task<()>,
    catalog: PluginCatalogState,
    selected: Option<String>,
    request: Option<Task<()>>,
    source_picker: Option<Task<()>>,
    upload: Option<SkillUploadOperation>,
    upload_task: Option<Task<()>>,
    install_failed: bool,
}
impl PluginsView {
    pub fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        cx: &mut App,
    ) -> Entity<Self> {
        let mut changes = client.watch_plugin_catalog();
        cx.new(|cx| {
            let task = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changes.changed().await.is_ok() {
                    if view
                        .update(cx, |view, cx| {
                            if view.active {
                                view.refresh(cx);
                            }
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            Self {
                _changes: task,
                registrar,
                upload_changes: Arc::new(UploadChanges(tokio::sync::watch::channel(0).0)),
                upload_registration: None,
                client,
                workspace: None,
                connection: 0,
                active: false,
                catalog: Default::default(),
                selected: None,
                request: None,
                source_picker: None,
                upload: None,
                upload_task: None,
                install_failed: false,
            }
        })
    }
    pub fn set_context(&mut self, workspace: Option<String>, active: bool, cx: &mut Context<Self>) {
        let connection = self.client.authorization_connection_generation();
        let changed = self.workspace != workspace || self.connection != connection;
        let opening = active && !self.active;
        if changed {
            self.request = None;
            self.source_picker = None;
            self.upload_task = None;
            self.upload_registration = None;
            self.upload = None;
            self.install_failed = false;
            self.catalog = Default::default();
            self.selected = None;
            self.workspace = workspace;
            self.connection = connection;
        }
        self.active = active;
        if active && (changed || opening) {
            self.refresh(cx);
        }
    }
    fn refresh(&mut self, cx: &mut Context<Self>) {
        if self.catalog.loading {
            self.catalog.refresh_requested = true;
            return;
        }
        let Some(workspace) = self.workspace.clone() else {
            return;
        };
        self.catalog.loading = true;
        self.catalog.failed = false;
        let client = self.client.clone();
        let connection = self.connection;
        self.request = Some(cx.spawn(async move |view, cx| {
            let request_workspace = workspace.clone();
            let result = cx
                .background_spawn(async move { client.read_plugins(&request_workspace) })
                .await;
            let _ = view.update(cx, |view, cx| {
                if view.workspace.as_ref() != Some(&workspace) || view.connection != connection {
                    return;
                }
                match result {
                    Ok(response) => view.catalog.accept(response),
                    Err(_) => view.catalog.fail(),
                }
                if view
                    .selected
                    .as_ref()
                    .is_some_and(|id| !view.catalog.plugins.iter().any(|p| &p.id == id))
                {
                    view.selected = None;
                }
                if view.catalog.refresh_requested {
                    view.catalog.refresh_requested = false;
                    view.refresh(cx);
                }
                cx.notify();
            });
        }));
        cx.notify();
    }
    fn pick_source(&mut self, folder: bool, cx: &mut Context<Self>) {
        let paths = cx.prompt_for_paths(PathPromptOptions {
            files: !folder,
            directories: folder,
            multiple: false,
            prompt: None,
        });
        let workspace = self.workspace.clone();
        let connection = self.connection;
        self.source_picker = Some(cx.spawn(async move |view, cx| {
            let Ok(Ok(Some(mut paths))) = paths.await else {
                return;
            };
            let Some(path) = paths.pop() else {
                return;
            };
            let _ = view.update(cx, |view, cx| {
                if view.workspace != workspace || view.connection != connection {
                    return;
                }
                let Some(workspace) = &workspace else {
                    return;
                };
                match view.client.start_skill_upload(
                    workspace,
                    SkillUploadTarget::PluginInstall,
                    path,
                ) {
                    Ok(operation) => {
                        view.upload = Some(operation);
                        view.install_failed = false;
                        view.watch_upload(cx);
                    }
                    Err(_) => view.install_failed = true,
                }
                cx.notify();
            });
        }));
    }
    fn watch_upload(&mut self, cx: &mut Context<Self>) {
        let Some(workspace) = self.workspace.clone() else {
            return;
        };
        let Some(operation) = self.upload.as_ref().map(|o| o.id()) else {
            return;
        };
        let sink: Arc<dyn ClientPublicationSink> = self.upload_changes.clone();
        let mut changed = self.upload_changes.0.subscribe();
        self.upload_registration = Some(self.registrar.register(
            pioneer_client::core::ClientScope::SkillsUpload {
                workspace_id: workspace.clone(),
                operation_id: operation,
            },
            Arc::downgrade(&sink),
        ));
        self.upload_task = Some(cx.spawn(async move |view, cx| {
            loop {
                let Ok(done) = view.update(cx, |view, cx| {
                    if view.workspace.as_ref() != Some(&workspace)
                        || view.upload.as_ref().is_none_or(|o| o.id() != operation)
                    {
                        return true;
                    }
                    let publication = view.client.skill_upload_snapshot(&workspace, operation);
                    let done = publication.as_ref().is_none_or(|p| p.state.is_terminal());
                    if let Some(publication) = publication
                        && done
                    {
                        view.install_failed = publication.state == SkillUploadState::Failed;
                        if let Some(item) = &publication.plugin_result {
                            view.selected = Some(item.id.clone());
                        }
                        view.upload_registration = None;
                        view.refresh(cx);
                    }
                    cx.notify();
                    done
                }) else {
                    break;
                };
                if done || changed.changed().await.is_err() {
                    break;
                }
            }
        }));
    }
    fn upload_active(&self) -> bool {
        self.upload
            .as_ref()
            .and_then(|o| {
                self.workspace
                    .as_ref()
                    .and_then(|w| self.client.skill_upload_snapshot(w, o.id()))
            })
            .is_some_and(|p| !p.state.is_terminal())
    }
}
impl Render for PluginsView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let busy = self.upload_active();
        let manage = self
            .workspace
            .as_deref()
            .is_some_and(|w| self.client.plugins_management_allowed(w));
        let selected = self
            .selected
            .as_ref()
            .and_then(|id| self.catalog.plugins.iter().find(|p| &p.id == id))
            .cloned();
        v_flex()
            .size_full()
            .min_w_0()
            .min_h_0()
            .p_4()
            .gap_4()
            .bg(cx.theme().background)
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(
                        div()
                            .flex_1()
                            .text_lg()
                            .font_semibold()
                            .child(t!("plugins.title").to_string()),
                    )
                    .child(
                        Button::new("plugins-refresh")
                            .ghost()
                            .label(t!("plugins.refresh").to_string())
                            .disabled(self.catalog.loading)
                            .on_click(cx.listener(|view, _, _, cx| view.refresh(cx))),
                    )
                    .when(manage, |row| {
                        row.child(
                            Button::new("plugins-folder")
                                .outline()
                                .label(t!("plugins.add_folder").to_string())
                                .disabled(busy)
                                .on_click(cx.listener(|view, _, _, cx| view.pick_source(true, cx))),
                        )
                        .child(
                            Button::new("plugins-archive")
                                .outline()
                                .label(t!("plugins.add_archive").to_string())
                                .disabled(busy)
                                .on_click(
                                    cx.listener(|view, _, _, cx| view.pick_source(false, cx)),
                                ),
                        )
                    }),
            )
            .when(busy, |view| {
                view.child(
                    h_flex()
                        .gap_2()
                        .child(t!("plugins.installing").to_string())
                        .child(
                            Button::new("plugins-cancel-upload")
                                .ghost()
                                .label(t!("plugins.cancel").to_string())
                                .on_click(cx.listener(|view, _, _, cx| {
                                    if let Some(operation) = &view.upload {
                                        operation.cancel();
                                    }
                                    cx.notify();
                                })),
                        ),
                )
            })
            .when(self.install_failed, |view| {
                view.child(
                    div()
                        .text_color(cx.theme().danger)
                        .child(t!("plugins.install_error").to_string()),
                )
            })
            .child(
                v_flex()
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .overflow_y_scrollbar()
                    .gap_2()
                    .when(self.catalog.loading, |view| {
                        view.child(t!("plugins.loading").to_string())
                    })
                    .when(self.catalog.failed, |view| {
                        view.child(t!("plugins.error").to_string())
                    })
                    .when(
                        !self.catalog.loading && self.catalog.plugins.is_empty(),
                        |view| view.child(t!("plugins.empty").to_string()),
                    )
                    .when_some(selected, |view, plugin| {
                        view.child(
                            Button::new("plugins-back")
                                .ghost()
                                .label(t!("plugins.back").to_string())
                                .on_click(cx.listener(|view, _, _, cx| {
                                    view.selected = None;
                                    cx.notify();
                                })),
                        )
                        .child(div().text_lg().font_semibold().child(plugin.name.clone()))
                        .child(status_label(&plugin.status))
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child(plugin.version.clone().unwrap_or_default()),
                        )
                        .child(
                            div()
                                .font_semibold()
                                .child(t!("plugins.components").to_string()),
                        )
                        .children(plugin.components.iter().map(|component| {
                            h_flex()
                                .gap_2()
                                .py_2()
                                .border_b_1()
                                .border_color(cx.theme().border)
                                .child(div().flex_1().child(component.member_key.clone()))
                                .child(status_label(&component.status))
                                .when_some(component.runtime_status.clone(), |row, status| {
                                    row.child(status_label(&status))
                                })
                                .into_any_element()
                        }))
                        .when(!plugin.diagnostics.is_empty(), |view| {
                            view.child(
                                div()
                                    .font_semibold()
                                    .child(t!("plugins.diagnostics").to_string()),
                            )
                            .children(plugin.diagnostics.iter().map(|d| {
                                div()
                                    .text_sm()
                                    .text_color(cx.theme().muted_foreground)
                                    .child(
                                        t!("plugins.diagnostic", path = d.path.as_str())
                                            .to_string(),
                                    )
                            }))
                        })
                    })
                    .when(self.selected.is_none(), |view| {
                        view.children(self.catalog.plugins.iter().map(|plugin| {
                            let id = plugin.id.clone();
                            Button::new(SharedString::from(format!("plugin-{}", plugin.id)))
                                .outline()
                                .w_full()
                                .justify_start()
                                .child(
                                    h_flex()
                                        .w_full()
                                        .gap_2()
                                        .child(div().flex_1().child(plugin.name.clone()))
                                        .child(status_label(&plugin.status)),
                                )
                                .on_click(cx.listener(move |view, _, _, cx| {
                                    view.selected = Some(id.clone());
                                    cx.notify();
                                }))
                                .into_any_element()
                        }))
                    }),
            )
    }
}
