use super::*;

#[tokio::test]
async fn computer_use_preflight_ready_does_not_create_session() {
    let (handler, root) = test_handler();

    let payload = invoke(
        &handler,
        serde_json::json!({
            "action": "preflight"
        }),
    )
    .await;

    assert_eq!(
        payload.get("action").and_then(JsonValue::as_str),
        Some("preflight")
    );
    assert_eq!(
        payload.get("status").and_then(JsonValue::as_str),
        Some("ready")
    );
    assert_eq!(
        payload
            .pointer("/capabilities/accessibility_tree")
            .and_then(JsonValue::as_str),
        Some("ok")
    );
    assert!(
        payload
            .get("permissions")
            .and_then(JsonValue::as_array)
            .is_some_and(|value| !value.is_empty())
    );
    assert!(payload.get("session_id").is_none());
    let artifacts_dir = root.join("tools/computer_use");
    let session_artifact_count = std::fs::read_dir(artifacts_dir.as_path())
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.parse::<u64>().is_ok())
                })
                .count()
        })
        .unwrap_or(0);
    assert_eq!(session_artifact_count, 0);
}

#[tokio::test]
async fn computer_use_preflight_degraded_returns_warning_without_recovery() {
    let (handler, _) =
        test_handler_with_backend(MockComputerUseBackend::with_preflight_status("degraded"));

    let payload = invoke(
        &handler,
        serde_json::json!({
            "action": "preflight"
        }),
    )
    .await;

    assert_eq!(
        payload.get("status").and_then(JsonValue::as_str),
        Some("degraded")
    );
    assert_eq!(
        payload
            .pointer("/capabilities/input_simulation")
            .and_then(JsonValue::as_str),
        Some("blocked")
    );
    assert_eq!(
        payload
            .get("warnings")
            .and_then(JsonValue::as_array)
            .map(Vec::len),
        Some(1)
    );
}

#[tokio::test]
async fn computer_use_preflight_blocked_returns_blocking_issue_without_recovery() {
    let (handler, _) =
        test_handler_with_backend(MockComputerUseBackend::with_preflight_status("blocked"));

    let payload = invoke(
        &handler,
        serde_json::json!({
            "action": "preflight"
        }),
    )
    .await;

    assert_eq!(
        payload.get("status").and_then(JsonValue::as_str),
        Some("blocked")
    );
    assert_eq!(
        payload
            .pointer("/capabilities/accessibility_tree")
            .and_then(JsonValue::as_str),
        Some("blocked")
    );
    assert_eq!(
        payload
            .get("blocking_issues")
            .and_then(JsonValue::as_array)
            .map(Vec::len),
        Some(1)
    );
}

#[test]
#[ignore = "compile-only xa11y API reconciliation reference; do not run against a real desktop"]
fn xa11y_api_compile_reference() {
    use std::time::Duration;
    use xa11y::{
        App, AppExt, ClickOptions, ClickTarget, Error, InputSim, Key, Locator, MouseButton, Point,
        Screenshot, ScrollDelta,
    };

    let _list_apps: fn() -> xa11y::Result<Vec<App>> = App::list;
    let _by_name: fn(&str, Duration) -> xa11y::Result<App> = App::by_name;
    let _by_pid: fn(u32, Duration) -> xa11y::Result<App> = App::by_pid;
    let _app_tree: fn(&App, Option<usize>) -> xa11y::Result<xa11y::TreeNode> = App::tree;
    let _app_dump: fn(&App, Option<usize>) -> xa11y::Result<String> = App::dump;
    let _app_locator: fn(&App, &str) -> Locator = App::locator;
    let _input_sim: fn() -> xa11y::Result<InputSim> = xa11y::input_sim;
    let _screenshot: fn() -> xa11y::Result<Screenshot> = xa11y::screenshot;
    let _screenshot_region: fn(xa11y::Rect) -> xa11y::Result<Screenshot> = xa11y::screenshot_region;
    let _screenshot_element: fn(&xa11y::Element) -> xa11y::Result<Screenshot> =
        xa11y::screenshot_element;
    let _to_png: fn(&Screenshot) -> xa11y::Result<Vec<u8>> = Screenshot::to_png;

    fn _classify_error(error: Error) -> &'static str {
        match error {
            Error::PermissionDenied { .. } => "permission_denied",
            Error::AccessibilityNotEnabled { .. } => "accessibility_not_enabled",
            Error::SelectorNotMatched { .. } => "selector_not_matched",
            Error::ElementStale { .. } => "element_stale",
            Error::ActionNotSupported { .. } => "action_not_supported",
            Error::TextValueNotSupported => "text_value_not_supported",
            Error::Timeout { .. } => "timeout",
            Error::InvalidSelector { .. } => "invalid_selector",
            Error::InvalidActionData { .. } => "invalid_action_data",
            Error::NoElementBounds => "no_element_bounds",
            Error::Unsupported { .. } => "unsupported",
            Error::Platform { .. } => "platform",
        }
    }

    if false {
        // Ignored compile-only fixture: this real app name is not product behavior.
        let app = App::by_name("Finder", Duration::from_millis(1)).unwrap();
        let locator = app.locator(r#"button[name="OK"]"#);
        locator.press().unwrap();
        locator.focus().unwrap();
        locator.toggle().unwrap();
        locator.select().unwrap();
        locator.expand().unwrap();
        locator.collapse().unwrap();
        locator.show_menu().unwrap();
        locator.scroll_into_view().unwrap();
        locator.set_value("value").unwrap();
        locator.set_numeric_value(1.0).unwrap();
        locator.type_text("text").unwrap();
        locator.select_text(0, 1).unwrap();
        locator.perform_action("press").unwrap();
        let _ = locator.tree(Some(2)).unwrap();
        let _ = locator.dump(Some(2)).unwrap();

        let sim = xa11y::input_sim().unwrap();
        sim.mouse().click(Point::new(1, 1)).unwrap();
        sim.mouse().double_click(Point::new(1, 1)).unwrap();
        sim.mouse().right_click(Point::new(1, 1)).unwrap();
        sim.mouse().move_to(Point::new(1, 1)).unwrap();
        sim.mouse()
            .click_with(
                ClickTarget::Point(Point::new(1, 1)),
                ClickOptions {
                    button: MouseButton::Left,
                    count: 1,
                    held: vec![Key::Meta],
                    anchor: xa11y::Anchor::Center,
                },
            )
            .unwrap();
        sim.mouse()
            .scroll(Point::new(1, 1), ScrollDelta::vertical(1))
            .unwrap();
        sim.keyboard().press(Key::Enter).unwrap();
        sim.keyboard().chord(Key::Char('a'), &[Key::Meta]).unwrap();
        sim.keyboard().type_text("text").unwrap();
    }
}
