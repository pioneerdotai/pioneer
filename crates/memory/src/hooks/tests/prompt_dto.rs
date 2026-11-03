use super::*;

#[test]
fn memory_prompt_scope_labels_keep_domain_resolution_in_agent() {
    assert_eq!(scope_label(&user_scope()), "user");
    assert_eq!(
        scope_label(&MemoryScope {
            kind: MemoryScopeKind::Workspace,
            key: "ws_123".to_owned(),
        }),
        "workspace:ws_123"
    );
    assert_eq!(
        scope_label(&MemoryScope {
            kind: MemoryScopeKind::Agent,
            key: "agent_123".to_owned(),
        }),
        "agent:agent_123"
    );
}
