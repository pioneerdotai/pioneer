use pioneer_protocol::{AgentExecutionBackend, CLIAgentRuntimeKind, TurnStartParams, UserInput};
use serde_json::Value as JsonValue;

#[test]
fn legacy_api_provider_turn_start_fixture_decodes_without_execution_backend() {
    let params: TurnStartParams = serde_json::from_str(include_str!(
        "fixtures/phase35/turn_start_legacy_api_provider.json"
    ))
    .expect("legacy turn/start fixture should decode");

    assert_eq!(params.thread_id, "thr_legacy_api");
    assert_eq!(params.turn_id, "turn_legacy_api");
    assert!(params.execution_backend.is_none());
    assert!(params.reasoning.is_none());
    assert!(params.cli_runtime_options.is_none());
    assert!(matches!(
        params.input.first(),
        Some(UserInput::Text { text, .. }) if text == "hello from a legacy api-provider client"
    ));
}

#[test]
fn cli_runtime_turn_start_fixture_roundtrips_backend_and_options() {
    let raw: JsonValue =
        serde_json::from_str(include_str!("fixtures/phase35/turn_start_cli_runtime.json"))
            .expect("CLI runtime turn/start fixture should parse");
    let params: TurnStartParams =
        serde_json::from_value(raw.clone()).expect("CLI runtime turn/start fixture should decode");

    assert_eq!(
        params.execution_backend,
        Some(AgentExecutionBackend::CLIAgentRuntime {
            runtime_id: "codex_personal".to_owned(),
            runtime_kind: CLIAgentRuntimeKind::Codex,
        })
    );
    let options = params
        .cli_runtime_options
        .as_ref()
        .expect("CLI runtime options should decode");
    assert_eq!(
        options
            .approval_policy
            .as_ref()
            .map(|policy| policy.0.as_str()),
        Some("unlessTrusted")
    );
    assert_eq!(options.effort.as_deref(), Some("medium"));
    assert_eq!(options.steer_if_active, Some(true));

    let encoded = serde_json::to_value(params).expect("turn/start fixture should encode");
    assert_eq!(encoded["execution_backend"], raw["execution_backend"]);
    assert_eq!(
        encoded["cli_runtime_options"]["sandbox"],
        raw["cli_runtime_options"]["sandbox"]
    );
}
