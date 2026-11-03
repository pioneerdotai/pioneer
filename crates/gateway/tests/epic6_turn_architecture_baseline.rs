const PROTOCOL_METHODS: &str = include_str!("../../protocol/src/constants.rs");
const AUTHORIZATION_REGISTRY: &str = include_str!("../src/authorization/registry.rs");
const GATEWAY_DISPATCH: &str = include_str!("../src/message/dispatch.rs");
const GATEWAY_TURN_HANDLER: &str = include_str!("../src/message/turn_handlers.rs");
const MIGRATION_REGISTRY: &str = include_str!("../../migration/src/lib.rs");
const ENTITY_REGISTRY: &str = include_str!("../../entity/src/lib.rs");

#[test]
fn epic6_baseline_keeps_turn_start_as_the_only_send_ingress() {
    assert!(PROTOCOL_METHODS.contains("pub const TURN_START: &str = \"turn/start\""));
    assert!(AUTHORIZATION_REGISTRY.contains("dynamic_method_entry(\n        TURN_START"));
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
