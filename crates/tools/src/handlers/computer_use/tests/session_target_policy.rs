use super::*;

#[tokio::test]
async fn session_target_policy_rejects_start_without_explicit_target() {
    let (handler, _) = test_handler();

    let error = invoke_result(
        &handler,
        serde_json::json!({
            "action": "start",
            "goal": "open requested location"
        }),
    )
    .await
    .expect_err("start without target must fail");

    assert!(
        error.to_string().contains("requires explicit target"),
        "{error}"
    );
    assert!(error.to_string().contains("app_name"), "{error}");
    assert!(error.to_string().contains("screen"), "{error}");
}
