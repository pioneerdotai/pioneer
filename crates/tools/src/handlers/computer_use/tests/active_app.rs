use super::super::model::{AppMeta, derive_app_identity_key};
use super::*;

fn app_meta(name: &str, pid: u32, frontmost: bool) -> AppMeta {
    AppMeta {
        identity_key: Some(derive_app_identity_key(name, Some(pid), None, None)),
        name: name.to_owned(),
        pid: Some(pid),
        role: Some("application".to_owned()),
        window_title: Some(format!("{name} Window")),
        bundle_id: None,
        localized_name: None,
        executable_path: None,
        frontmost: Some(frontmost),
    }
}

#[tokio::test]
async fn active_app_uses_frontmost_app_not_first_listed_app() {
    let backend = MockComputerUseBackend::default().with_apps(vec![
        app_meta("BackgroundApp", 11, false),
        app_meta("FrontApp", 22, true),
    ]);
    let (handler, _) = test_handler_with_backend(backend);

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "use active app",
            "target": { "type": "active_app" }
        }),
    )
    .await;

    assert_eq!(
        started
            .pointer("/target/app/name")
            .and_then(JsonValue::as_str),
        Some("FrontApp")
    );
    assert_eq!(
        started
            .pointer("/target/app/window_title")
            .and_then(JsonValue::as_str),
        Some("FrontApp Window")
    );
    assert_eq!(
        started
            .pointer("/target/app/frontmost")
            .and_then(JsonValue::as_bool),
        Some(true)
    );
}

#[tokio::test]
async fn active_app_reports_explicit_unsupported_when_frontmost_is_unavailable() {
    let backend = MockComputerUseBackend::default().with_apps(vec![app_meta("OnlyApp", 11, false)]);
    let (handler, _) = test_handler_with_backend(backend);

    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "use active app",
            "target": { "type": "active_app" }
        }),
    )
    .await
    .expect_err("frontmost app must be unavailable");

    assert!(error.to_string().contains("frontmost app"), "{error}");
    assert!(error.to_string().contains("app_name"), "{error}");
}
