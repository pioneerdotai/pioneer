//! Retained catalog presentation over typed, immutable Client publications.
#[macro_use]
extern crate rust_i18n;
rust_i18n::i18n!("locales", fallback = "en");
mod actions;
mod assets;
mod binding;
mod buttons;
mod catalog;
mod details;
mod dialog_lifetime;
mod dialogs;
mod list;
mod sidebar;
mod table;
pub use catalog::{McpCatalogConfig, McpCatalogView};

mod identity;

#[cfg(test)]
mod boundary_tests {
    #[test]
    fn shell_composes_catalog_roots_and_has_no_retired_catalog_owners() {
        let root = include_str!("../../desktop/src/app/root/mod.rs");
        let state = include_str!("../../desktop/src/app/root/state.rs");
        let view = include_str!("../../desktop/src/app/root/view.rs");
        for field in [
            "mcp_servers:",
            "mcp_poller_started:",
            "mcp_pending_actions:",
            "skills_catalog:",
            "installed_skills:",
            "skills_upload_state:",
            "skills_poller_started:",
        ] {
            assert!(!root.contains(field), "retired shell owner {field}");
        }
        for name in ["McpCatalogView", "SkillsCatalogView"] {
            assert!(state.contains(name));
        }
        for name in ["mcp_view", "skills_view"] {
            assert!(root.contains(name));
            assert!(view.contains(name));
        }
        for manifest in [
            include_str!("../Cargo.toml"),
            include_str!("../../desktop-skills/Cargo.toml"),
        ] {
            assert!(!manifest.contains("client-ffi"));
            assert!(!manifest.contains("pioneer-desktop.workspace"));
            assert!(!manifest.contains("patch."));
        }
        let mcp = include_str!("catalog.rs");
        let skills = include_str!("../../desktop-skills/src/catalog.rs");
        for source in [mcp, skills] {
            let render = source
                .split("impl Render for")
                .nth(1)
                .unwrap()
                .split("#[cfg(test)]")
                .next()
                .unwrap();
            for forbidden in ["cx.new(", "cx.spawn(", "subscribe(", "refresh_", "acquire_"] {
                assert!(!render.contains(forbidden));
            }
            assert!(!source.contains(".cached("));
            assert!(!source.contains("client_ffi"));
        }
        for source in [
            include_str!("../../desktop/src/app/flow/ws_events_notifications.rs"),
            include_str!("../../desktop/src/app/flow/ws_events_pump.rs"),
        ] {
            assert!(!source.contains("apply_mcp_server_status_changed_to_catalog"));
            assert!(!source.contains("apply_local_skill_policy"));
        }
    }
}
