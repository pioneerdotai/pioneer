//! Retained Plugins management.
#[macro_use]
extern crate rust_i18n;
rust_i18n::i18n!("locales", fallback = "en");
mod catalog;
mod details;
mod list;
mod management;
mod sidebar;
mod upload;
use gpui_kit::component::{button::*, scroll::ScrollableElement, theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    core::{ClientCore, ClientScope},
    plugins::runtime::{PluginDemand, PluginIntent, PluginPublication},
    plugins::{PluginComponentItem, PluginComponentKey, PluginItem},
    skills::{
        operations::SkillUploadTarget,
        upload_flow::{SkillUploadPublication, SkillUploadState},
    },
};
use pioneer_desktop_foundation::{CatalogBinding, ClientBindingRegistrar};
use pioneer_desktop_foundation::{PackageUploadView, UploadVisibility};
use upload::PluginUploadText;
type PluginUploadView = PackageUploadView<PluginUploadText>;
use std::sync::Arc;

pub(crate) fn status_label(status: &str) -> String {
    match status {
        "retained" => t!("plugins.retained"),
        "active" => t!("plugins.active"),
        "blocked" => t!("plugins.blocked"),
        "unavailable" => t!("plugins.unavailable"),
        "installed" => t!("plugins.installed"),
        "partial" => t!("plugins.partial"),
        "installing" => t!("plugins.installing"),
        "degraded" => t!("plugins.degraded"),
        "restarting" => t!("plugins.restarting"),
        "stopping" => t!("plugins.stopping"),
        "starting" => t!("plugins.connecting"),
        "interrupted" => t!("plugins.interrupted"),
        "updating" => t!("plugins.updating"),
        "removing" => t!("plugins.removing"),
        "failed" => t!("plugins.failed"),
        "disabled" => t!("plugins.disabled"),
        "removed_by_user" => t!("plugins.removed_by_user"),
        "authrequired" | "auth_required" => t!("plugins.auth_required"),
        "ready" => t!("plugins.authorized"),
        "notstarted" | "not_started" => t!("plugins.pending"),
        "error" => t!("plugins.failed"),
        "stopped" => t!("plugins.stopped"),
        "offline" => t!("plugins.offline"),
        _ => t!("plugins.unavailable"),
    }
    .to_string()
}

#[cfg(test)]
mod status_tests {
    #[::core::prelude::v1::test]
    fn skill_runtime_states_have_real_labels_and_mcp_labels_stay_distinct() {
        for (state, label) in [
            ("active", t!("plugins.active")),
            ("disabled", t!("plugins.disabled")),
            ("blocked", t!("plugins.blocked")),
            ("unavailable", t!("plugins.unavailable")),
            ("ready", t!("plugins.authorized")),
            ("auth_required", t!("plugins.auth_required")),
            ("offline", t!("plugins.offline")),
            ("degraded", t!("plugins.degraded")),
            ("restarting", t!("plugins.restarting")),
            ("stopping", t!("plugins.stopping")),
            ("unknown-future-state", t!("plugins.unavailable")),
        ] {
            assert_eq!(super::status_label(state), label.to_string());
        }
        assert_ne!(
            super::status_label("active"),
            super::status_label("unavailable")
        );
        assert_ne!(
            super::status_label("blocked"),
            super::status_label("unavailable")
        );
        assert_ne!(super::status_label("ready"), super::status_label("active"));
    }
}

fn diagnostic_label(code: &str) -> String {
    match code {
        "unknown_manifest_field" | "invalid_extensions_container" => t!("plugins.ignored_field"),
        "invalid_skill" | "invalid_skills_location" => t!("plugins.invalid_skill"),
        "invalid_mcp_document"
        | "invalid_mcp_server"
        | "invalid_mcp_location"
        | "unsupported_transport" => t!("plugins.invalid_mcp"),
        "component_limit" => t!("plugins.component_limit"),
        "invalid_mcp_path" | "denied_package_path" => t!("plugins.path_denied"),
        "vendor_subset_import" => t!("plugins.vendor_subset"),
        "ignored_vendor_manifest" => t!("plugins.ignored_manifest"),
        "ignored_vendor_feature" => t!("plugins.ignored_feature"),
        "component_path_denied" => t!("plugins.path_denied"),
        "component_install_failed" => t!("plugins.component_failed"),
        "fresh_package_required" => t!("plugins.fresh_package_required"),
        "reapply_native_change" => t!("plugins.reapply_native_change"),
        "operation_interrupted" => t!("plugins.repair_notice"),
        "components_unavailable" => t!("plugins.components_unavailable"),
        _ => t!("plugins.package_attention"),
    }
    .to_string()
}

#[derive(Clone)]
pub struct PluginComponentDetails {
    content: AnyView,
    sidebar: AnyView,
    set_sidebar_width: Arc<dyn Fn(Pixels, bool, &mut App)>,
}
impl PluginComponentDetails {
    pub fn new(
        content: AnyView,
        sidebar: AnyView,
        set_sidebar_width: impl Fn(Pixels, bool, &mut App) + 'static,
    ) -> Self {
        Self {
            content,
            sidebar,
            set_sidebar_width: Arc::new(set_sidebar_width),
        }
    }
}
pub type PluginComponentDetailsFactory = Box<
    dyn Fn(
        &str,
        &str,
        &PluginComponentItem,
        &mut Window,
        &mut App,
    ) -> Option<PluginComponentDetails>,
>;

pub struct PluginsView {
    client: Arc<ClientCore>,
    binding: Arc<CatalogBinding>,
    input: Arc<PluginPublication>,
    plugin_images: pioneer_desktop_foundation::ImageViews,
    workspace: Option<String>,
    connection: u64,
    authorization: Option<(u64, u64)>,
    window_active: bool,
    demand: Option<PluginDemand>,
    skills_demand: Option<pioneer_client::skills::store::SkillsDemand>,
    mcp_demand: Option<pioneer_client::mcp::store::McpDemand>,
    demand_key: Option<(String, Option<String>, u64, bool, bool)>,
    selected: Option<String>,
    source_picker: Option<Task<()>>,
    upload: Entity<PluginUploadView>,
    applying_update: Option<(String, u64)>,
    preview_dismissed: bool,
    presentation_error: Option<(Option<String>, String)>,
    restore_confirmation: Option<Task<()>>,
    sidebar_width: Pixels,
    route_child: Option<PluginComponentKey>,
    component_details: PluginComponentDetailsFactory,
    child_details: Option<PluginComponentDetails>,
    child_locator: Option<(String, String, String, String, String)>,
    window: AnyWindowHandle,
    child_sync_pending: bool,
    _publications: Task<()>,
    _upload_changes: Subscription,
}
impl Render for PluginsView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(child) = &self.child_details {
            return div()
                .size_full()
                .child(child.content.clone())
                .into_any_element();
        }
        if self.selected.is_some() {
            self.render_details(window, cx)
        } else {
            self.render_list(cx)
        }
    }
}
