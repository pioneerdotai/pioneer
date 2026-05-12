use super::*;

#[test]
fn memory_recall_prompt_input_maps_domain_snapshot_to_prompt_dto() {
    let input = memory_recall_prompt_input(
        vec!["memory_search".to_owned()],
        MemoryRecallPromptPolicy::Full,
        MemoryRecallSnapshot {
            items: vec![MemoryRecallItem {
                memory_id: "mem_123".to_owned(),
                scope: user_scope(),
                category: MemoryCategory::Identity,
                key: Some("name".to_owned()),
                content: "User's name is Alexander.".to_owned(),
                score: Some(1.0),
                updated_at: 1_714_867_200,
            }],
            diagnostics: vec!["internal diagnostic must not leak".to_owned()],
            truncated: true,
        },
    );

    assert_eq!(input.available_tool_names, vec!["memory_search"]);
    assert_eq!(input.policy, MemoryRecallPromptPolicy::Full);
    assert!(input.truncated);
    assert_eq!(input.recalled_items.len(), 1);
    let item = &input.recalled_items[0];
    assert_eq!(item.memory_id, "mem_123");
    assert_eq!(item.scope_label, "user");
    assert_eq!(item.category_label, "identity");
    assert_eq!(item.key.as_deref(), Some("name"));
    assert_eq!(item.content, "User's name is Alexander.");
    assert_eq!(item.score, Some(1.0));
    assert_eq!(item.updated_at_label, "2024-05-05");
}

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
