//! Source seam guards complement retained-context and Client/FFI contract tests.
#[test]
fn thread_has_one_public_root_and_only_approved_workspace_dependencies() {
    let manifest = include_str!("../Cargo.toml");
    let allowed = ["pioneer-client", "pioneer-desktop-foundation", "terminal"];
    for line in manifest.lines().map(str::trim) {
        if line.starts_with("pioneer-") && line.contains("workspace") {
            let name = line.split(['.', ' ', '=']).next().unwrap();
            assert!(allowed.contains(&name), "feature dependency {name}");
        }
    }
    let lib = include_str!("../src/lib.rs");
    assert!(lib.contains("pub use thread::{ThreadNavigationEvent, ThreadView, ThreadViewConfig}"));
    for private in [
        "screen",
        "composer",
        "panels",
        "header",
        "members",
        "artifacts",
    ] {
        assert!(!lib.contains(&format!("pub mod {private}")));
    }
    let thread = include_str!("../src/thread.rs")
        .split("#[cfg(test)]")
        .next()
        .unwrap();
    for child in [
        "ThreadScreenView::new",
        "ComposerView::new",
        "ThreadHeaderView::new",
        "ThreadSidePanelHostView::new",
    ] {
        assert!(thread.contains(child), "missing mounted child {child}");
    }
    assert!(!thread.contains(".cached("));
}

#[test]
fn shell_mounts_the_opaque_root_without_retaining_thread_domain_fields() {
    let shell = include_str!("../../desktop/src/desktop_shell.rs")
        .split("#[cfg(test)]")
        .next()
        .unwrap();
    assert!(shell.contains("ThreadView::new(config, window, cx)"));
    assert!(shell.contains("ThreadViewConfig::new("));
    assert!(shell.contains("self.thread.take()"));
    let legacy = include_str!("../../desktop/src/app/root/mod.rs");
    for owner in [
        "composer_state:",
        "composer_input:",
        "thread_member_input:",
        "thread_capability_input:",
        "artifact_input:",
        "thread_timeline_view_state:",
        "thread_panel_layout:",
        "desktop_voice_composer:",
    ] {
        assert!(!legacy.contains(owner), "legacy mutable owner {owner}");
    }
    let bridge = include_str!("../../desktop/src/app/root/composer_domain.rs");
    for write in [
        "composer_intent(",
        "dispatch(",
        "cx.spawn",
        "subscribe(",
        "reduce_",
    ] {
        assert!(
            !bridge.contains(write),
            "mutable compatibility bridge {write}"
        );
    }
}

#[test]
fn timeline_keeps_the_stock_list_and_existing_scroll_owner() {
    let view = include_str!("../src/timeline/view.rs");
    let screen = include_str!("../src/screen.rs");
    assert!(view.contains("v_virtual_list("));
    assert!(screen.contains("VirtualListScrollHandle::new()"));
    assert!(view.contains("sync_timeline_layout_width"));
    assert!(view.contains("cached_timeline_layout_index"));
    for forbidden in ["DesktopTimelineController", "impl Element for", ".cached("] {
        assert!(!view.contains(forbidden));
        assert!(!screen.contains(forbidden));
    }
}
