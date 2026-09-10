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
        "TimelineView::new",
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
    let legacy = shell;
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
    let bridge = shell;
    for write in ["composer_intent(", "dispatch(", "reduce_"] {
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
    let state = include_str!("../src/timeline/state.rs");
    let controller = include_str!("../src/timeline/controller.rs");
    assert!(view.contains("v_virtual_list("));
    assert!(state.contains("VirtualListScrollHandle::new()"));
    assert!(!view.contains("sync_timeline_layout_width"));
    assert!(view.contains("TimelineLayoutIndex::from_store"));
    assert!(controller.contains("struct DesktopTimelineController;"));
    for forbidden in ["impl Element for", ".cached("] {
        assert!(!view.contains(forbidden));
        assert!(!screen.contains(forbidden));
    }
}

#[test]
fn timeline_composition_reads_prepared_input_without_running_effects() {
    let view = include_str!("../src/timeline/view.rs");
    let render = view
        .split("pub(crate) fn render_timeline(")
        .nth(1)
        .unwrap()
        .split("pub(super) fn render_timeline_row(")
        .next()
        .unwrap();
    // Event handler registration is allowed; these operations must never occur
    // in composition or the virtual list's row/measurement callback.
    for forbidden in [
        "cx.spawn",
        "cx.defer",
        "cx.notify",
        "borrow_mut()",
        "thread_bindings",
        "request_mark",
        "request_semantic",
        ".read(cx)",
        ".update(cx",
        "reconcile_scroll(",
    ] {
        assert!(!render.contains(forbidden), "render contains {forbidden}");
    }
    let rail = include_str!("../src/timeline/avatar_rail.rs");
    let canvas = rail.split("canvas(").nth(1).unwrap();
    for forbidden in [
        "cx.defer",
        "cx.notify",
        "sync_timeline_avatar_demand",
        "borrow_mut",
    ] {
        assert!(!canvas.contains(forbidden));
    }
}

#[test]
fn row_sources_keep_the_approved_element_and_no_optional_reuse_mechanisms() {
    fn visit(path: &std::path::Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, files);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path);
            }
        }
    }
    let mut files = Vec::new();
    visit(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/timeline"),
        &mut files,
    );
    let mut elements = Vec::new();
    for path in files {
        let source = std::fs::read_to_string(&path).unwrap();
        let production = source.split("#[cfg(test)]").next().unwrap();
        for forbidden in [
            ".cached(",
            "IndexedTimelineListElement",
            "RowBodyView",
            "TranslationCache",
            "CodeHighlightCache",
            "Proposal68",
        ] {
            assert!(
                !production.contains(forbidden),
                "{} contains {forbidden}",
                path.display()
            );
        }
        for line in production
            .lines()
            .filter(|line| line.starts_with("impl Element for "))
        {
            elements.push(line.to_owned());
        }
    }
    assert_eq!(elements, vec!["impl Element for MarkdownLinkText {"]);
    let row = include_str!("../src/timeline/row_registry.rs")
        .split("#[cfg(test)]")
        .next()
        .unwrap();
    assert!(row.contains("HashMap<RowId, Arc<TimelineRowSlotView>>"));
    assert!(!row.contains("item_index)"));
    let view = include_str!("../src/timeline/view.rs");
    assert!(!view.contains("unwrap_or_else(|| view.render_timeline_row"));
    let command = include_str!("../src/timeline/items/command_execution.rs");
    let command_render = command
        .split("pub(super) fn render_item_command_execution(")
        .nth(1)
        .unwrap()
        .split("impl crate::screen::TimelineView {")
        .next()
        .unwrap();
    assert!(command_render.contains("terminal: Option<Entity<TerminalView>>"));
    assert!(command_render.contains(".children(terminal)"));
    for forbidden in [
        "thread_timeline_terminal_item",
        ".synchronize(",
        "TerminalView::new",
        "borrow_mut(",
        "cx.notify()",
    ] {
        assert!(!command_render.contains(forbidden));
    }
    let presentation = include_str!("../src/timeline/row_view.rs");
    assert!(
        !row.contains("terminal: Option<"),
        "immutable slots must not retain terminal grids"
    );
    assert!(
        !view.contains("prepare_command_terminal("),
        "layout must not eagerly allocate terminals"
    );
    assert!(presentation.contains("fn set_row_terminals_visible("));
    assert!(presentation.contains("self.terminal.as_ref().map(|t| t.view.clone())"));
    let markdown = include_str!("../src/timeline/markdown.rs");
    let link = markdown
        .split("impl Element for MarkdownLinkText {")
        .nth(1)
        .unwrap()
        .split("#[derive(IntoElement)]")
        .next()
        .unwrap();
    for forbidden in [
        "cx.notify",
        "cx.spawn",
        ".update(cx",
        "cached",
        "thread_bindings",
    ] {
        assert!(!link.contains(forbidden));
    }
    let measure = include_str!("../src/timeline/mod.rs")
        .split("impl TimelineLayoutMeasurement {")
        .nth(1)
        .unwrap()
        .split("pub(crate) use")
        .next()
        .unwrap();
    assert!(measure.contains("layout_as_root("));
    for forbidden in [
        "notify(",
        "spawn(",
        "borrow_mut(",
        "dispatch(",
        "commit(",
        "thread_bindings",
    ] {
        assert!(!measure.contains(forbidden));
    }
}
