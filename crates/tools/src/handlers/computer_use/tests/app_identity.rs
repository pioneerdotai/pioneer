use super::super::model::{AppMeta, derive_app_identity_key};
use super::*;

fn app_meta(name: &str, pid: u32, localized_name: Option<&str>) -> AppMeta {
    AppMeta {
        identity_key: Some(derive_app_identity_key(name, Some(pid), None, None)),
        name: name.to_owned(),
        pid: Some(pid),
        role: Some("application".to_owned()),
        window_title: Some(format!("{name} Window")),
        bundle_id: None,
        localized_name: localized_name.map(str::to_owned),
        executable_path: None,
        frontmost: Some(false),
    }
}

#[tokio::test]
async fn app_identity_resolves_exact_inventory_name() {
    let backend =
        MockComputerUseBackend::default().with_apps(vec![app_meta("ExampleApp", 77, None)]);
    let (handler, _) = test_handler_with_backend(backend);

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open requested app",
            "target": { "type": "app_name", "name": "ExampleApp" }
        }),
    )
    .await;

    assert_eq!(
        started
            .pointer("/target/app/name")
            .and_then(JsonValue::as_str),
        Some("ExampleApp")
    );
    assert_eq!(
        started
            .pointer("/target/app/pid")
            .and_then(JsonValue::as_u64),
        Some(77)
    );
    assert!(
        started
            .pointer("/target/app/identity_key")
            .and_then(JsonValue::as_str)
            .is_some(),
        "{started}"
    );
}

#[tokio::test]
async fn app_identity_resolves_exact_os_provided_localized_name() {
    let backend = MockComputerUseBackend::default().with_apps(vec![app_meta(
        "SystemProvidedExampleApp",
        77,
        Some("LocalizedExampleApp"),
    )]);
    let (handler, _) = test_handler_with_backend(backend);

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open localized inventory app",
            "target": { "type": "app_name", "name": "LocalizedExampleApp" }
        }),
    )
    .await;

    assert_eq!(
        started
            .pointer("/target/app/name")
            .and_then(JsonValue::as_str),
        Some("SystemProvidedExampleApp")
    );
    assert_eq!(
        started
            .pointer("/target/app/localized_name")
            .and_then(JsonValue::as_str),
        Some("LocalizedExampleApp")
    );
}

#[tokio::test]
async fn app_identity_does_not_translate_between_unprovided_names() {
    let backend = MockComputerUseBackend::default().with_apps(vec![app_meta(
        "SystemProvidedExampleApp",
        77,
        Some("LocalizedExampleApp"),
    )]);
    let (handler, _) = test_handler_with_backend(backend);

    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "missing app",
            "target": { "type": "app_name", "name": "OtherLanguageExampleApp" }
        }),
    )
    .await
    .expect_err("unprovided translated name must fail");

    assert!(
        error.to_string().contains("failure_class=app_not_found"),
        "{error}"
    );
    assert!(error.to_string().contains("candidates"), "{error}");
    assert!(
        error.to_string().contains("SystemProvidedExampleApp"),
        "{error}"
    );
}

#[tokio::test]
async fn app_identity_resolves_identity_key_from_inventory() {
    let app = app_meta("ExampleApp", 77, None);
    let identity_key = app.identity_key.clone().expect("identity key");
    let backend = MockComputerUseBackend::default().with_apps(vec![app]);
    let (handler, _) = test_handler_with_backend(backend);

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open by identity key",
            "target": { "type": "identity_key", "identity_key": identity_key }
        }),
    )
    .await;

    assert_eq!(
        started
            .pointer("/target/app/name")
            .and_then(JsonValue::as_str),
        Some("ExampleApp")
    );
    assert_eq!(
        started
            .pointer("/target/requested/identity_key")
            .and_then(JsonValue::as_str),
        Some(identity_key.as_str())
    );
}

#[tokio::test]
async fn app_identity_generic_app_target_resolves_pid_field() {
    let backend =
        MockComputerUseBackend::default().with_apps(vec![app_meta("ExampleApp", 77, None)]);
    let (handler, _) = test_handler_with_backend(backend);

    let started = invoke(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open by generic app target",
            "target": { "type": "app", "pid": 77 }
        }),
    )
    .await;

    assert_eq!(
        started
            .pointer("/target/app/name")
            .and_then(JsonValue::as_str),
        Some("ExampleApp")
    );
    assert_eq!(
        started
            .pointer("/target/requested/pid")
            .and_then(JsonValue::as_u64),
        Some(77)
    );
}

#[tokio::test]
async fn app_identity_missing_name_returns_inventory_candidates() {
    let backend =
        MockComputerUseBackend::default().with_apps(vec![app_meta("ExampleApp", 77, None)]);
    let (handler, _) = test_handler_with_backend(backend);

    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "missing app",
            "target": { "type": "app_name", "name": "MissingExampleApp" }
        }),
    )
    .await
    .expect_err("missing app must fail");

    assert!(
        error.to_string().contains("accepted_target_fields"),
        "{error}"
    );
    assert!(error.to_string().contains("candidates"), "{error}");
    assert!(error.to_string().contains("ExampleApp"), "{error}");
}
