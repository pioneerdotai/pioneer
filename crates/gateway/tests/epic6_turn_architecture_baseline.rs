const PROTOCOL_METHODS: &str = include_str!("../../protocol/src/constants.rs");
const AUTHORIZATION_REGISTRY: &str = include_str!("../src/authorization/registry.rs");
const GATEWAY_DISPATCH: &str = include_str!("../src/message/dispatch.rs");
const GATEWAY_TURN_HANDLER: &str = include_str!("../src/message/turn_handlers.rs");
const MIGRATION_REGISTRY: &str = include_str!("../../migration/src/lib.rs");
const ENTITY_REGISTRY: &str = include_str!("../../entity/src/lib.rs");
const GATEWAY_REGRESSION_FIXTURES: &str = include_str!("../src/message/tests.rs");

#[test]
fn epic6_baseline_keeps_turn_start_as_the_only_send_ingress() {
    assert!(PROTOCOL_METHODS.contains("pub const TURN_START: &str = \"turn/start\""));
    assert!(AUTHORIZATION_REGISTRY.contains("method_entry(TURN_START"));
    assert!(GATEWAY_DISPATCH.contains("dispatch_turn_start"));
    assert!(GATEWAY_TURN_HANDLER.contains("pub(super) fn turn_start"));

    for source in [PROTOCOL_METHODS, AUTHORIZATION_REGISTRY, GATEWAY_DISPATCH] {
        assert!(!source.contains("message/send"));
        assert!(!source.contains("MESSAGE_SEND"));
    }
}

#[test]
fn epic6_baseline_has_no_secondary_message_aggregate_or_link() {
    for source in [MIGRATION_REGISTRY, ENTITY_REGISTRY, GATEWAY_TURN_HANDLER] {
        assert!(!source.contains("conversation_message"));
        assert!(!source.contains("source_message_id"));
    }
}

#[test]
fn epic6_baseline_retains_named_api_and_external_runtime_regressions() {
    for fixture in [
        "turn_start_emits_full_lifecycle_notifications_and_echoes_text",
        "turn_start_without_execution_backend_uses_api_provider_path",
        "collaborative_composer_dispatches_codex_and_claude_without_api_provider_leakage",
        "turn_start_cli_runtime_uses_default_stack_and_errors_before_provider_dispatch",
    ] {
        assert!(
            GATEWAY_REGRESSION_FIXTURES.contains(fixture),
            "missing baseline regression fixture `{fixture}`"
        );
    }

    assert!(GATEWAY_REGRESSION_FIXTURES.contains("turn_starts: TokioMutex<Vec<"));
    assert!(GATEWAY_REGRESSION_FIXTURES.contains("calls: AtomicUsize"));
}
