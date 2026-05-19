use migration::{Migrator, MigratorTrait};
use pioneer_crud::{
    AgentMemoryCandidateDecisionRecord, AgentMemoryCandidateListFilter, AgentMemoryCapsuleRecord,
    AgentMemoryListFilter, CrudStore, MemoryLifecycleActorRecord, MemoryWorkspaceGuard,
    NewAgentMemoryCandidate, NewAgentMemoryControlRecord, NewAgentMemoryPolicyDecision,
    NewAgentMemoryQualityDecision, NewAgentMemoryQuarantine, NewAgentMemoryRepairJob,
    ResolveAgentMemoryQuarantine, global_agent_memory_scope_key, memory_scope_key_hash,
    workspace_agent_memory_scope_key,
};
use pioneer_protocol::{
    MemoryCandidateDecision, MemoryCandidateStatus, MemoryCategory, MemoryEvidenceClass,
    MemoryFactClass, MemoryLifecycleActorKind, MemoryLifecycleReasonCode, MemoryLifetimeClass,
    MemoryOwnershipClass, MemoryQualityAction, MemoryQualityReasonCode, MemoryScope,
    MemoryScopeKind, MemorySensitivity, MemorySourceContextKind, MemoryStatus, MemoryWriteRelation,
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection};

async fn setup_store() -> (DatabaseConnection, CrudStore) {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("must connect to sqlite memory");
    Migrator::up(&connection, None)
        .await
        .expect("migrations must succeed");
    seed_workspace_graph(&connection).await;
    let store = CrudStore::new(connection.clone());
    (connection, store)
}

async fn seed_workspace_graph(connection: &DatabaseConnection) {
    connection
        .execute_unprepared(
            r#"
            INSERT INTO workspace (id, name, is_active, is_current)
            VALUES
              ('ws_memory_a', 'Memory A', 1, 1),
              ('ws_memory_b', 'Memory B', 1, 0)
            "#,
        )
        .await
        .expect("workspace seed should succeed");

    connection
        .execute_unprepared(
            r#"
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
            VALUES
              ('thread_memory_a', 'ws_memory_a', '', 'agent', 'gpt-5.4', 'openai', 'active'),
              ('thread_memory_b', 'ws_memory_b', '', 'agent', 'gpt-5.4', 'openai', 'active')
            "#,
        )
        .await
        .expect("thread seed should succeed");

    connection
        .execute_unprepared(
            r#"
            INSERT INTO task (id, workspace_id, owner_kind, executor_kind, status, title, goal)
            VALUES
              ('task_memory_a', 'ws_memory_a', 'user', 'agent', 'draft', 'Memory A', 'Test A'),
              ('task_memory_b', 'ws_memory_b', 'user', 'agent', 'draft', 'Memory B', 'Test B')
            "#,
        )
        .await
        .expect("task seed should succeed");
}

fn scope(kind: MemoryScopeKind, key: &str) -> MemoryScope {
    MemoryScope {
        kind,
        key: key.to_owned(),
    }
}

fn new_memory(
    id: &str,
    scope: MemoryScope,
    key: &str,
    preview: &str,
) -> NewAgentMemoryControlRecord {
    NewAgentMemoryControlRecord {
        id: Some(id.to_owned()),
        scope,
        namespace: None,
        category: MemoryCategory::ProjectFact,
        key: Some(key.to_owned()),
        sensitivity: MemorySensitivity::Normal,
        confidence: 1.0,
        importance: 0.7,
        content_preview: Some(preview.to_owned()),
        capsule_id: None,
        capsule_ref: Some(format!("memvid://{id}")),
        frame_id: Some(1),
        frame_uri: Some(format!("frame://{id}")),
        frame_version: 1,
        source_context_kind: Some(MemorySourceContextKind::DirectUserConversation),
        source_thread_id: Some("thread_memory_a".to_owned()),
        source_turn_id: Some("turn_memory_a".to_owned()),
        source_item_id: None,
        created_by: None,
        expires_at_unix: None,
        policy_version: Some("memory_policy_v1".to_owned()),
        metadata_json: None,
    }
}

#[tokio::test]
async fn agent_memory_scope_hash_is_stable_and_agent_scope_requires_workspace_qualification() {
    let (_, store) = setup_store().await;

    let first = memory_scope_key_hash(MemoryScopeKind::Workspace, "ws_memory_a").unwrap();
    let second = memory_scope_key_hash(MemoryScopeKind::Workspace, "ws_memory_a").unwrap();
    assert_eq!(first, second);

    let workspace_agent = store
        .resolve_memory_scope(scope(
            MemoryScopeKind::Agent,
            &workspace_agent_memory_scope_key("ws_memory_a", "agent_research"),
        ))
        .await
        .expect("workspace agent scope should resolve");
    assert_eq!(workspace_agent.workspace_id.as_deref(), Some("ws_memory_a"));

    let global_agent = store
        .resolve_memory_scope(scope(
            MemoryScopeKind::Agent,
            &global_agent_memory_scope_key("agent_research"),
        ))
        .await
        .expect("global agent scope should resolve");
    assert_eq!(global_agent.workspace_id, None);

    let unqualified = store
        .resolve_memory_scope(scope(MemoryScopeKind::Agent, "agent:agent_research"))
        .await;
    assert!(unqualified.is_err());

    let thread = store
        .resolve_memory_scope(scope(MemoryScopeKind::Thread, "thread_memory_a"))
        .await
        .expect("thread scope should resolve");
    assert_eq!(thread.workspace_id.as_deref(), Some("ws_memory_a"));

    let task = store
        .resolve_memory_scope(scope(MemoryScopeKind::Task, "task_memory_b"))
        .await
        .expect("task scope should resolve");
    assert_eq!(task.workspace_id.as_deref(), Some("ws_memory_b"));
}

#[tokio::test]
async fn agent_memory_active_delete_and_get_roundtrip() {
    let (_, store) = setup_store().await;
    let memory = store
        .insert_agent_memory_record(
            new_memory(
                "mem_delete_one",
                scope(MemoryScopeKind::Workspace, "ws_memory_a"),
                "project.style",
                "Use direct engineering prose.",
            ),
            None,
            100,
        )
        .await
        .expect("insert memory");
    assert_eq!(memory.status, MemoryStatus::Active);
    assert_eq!(memory.workspace_id.as_deref(), Some("ws_memory_a"));
    assert_eq!(
        memory.source_context_kind,
        Some(MemorySourceContextKind::DirectUserConversation)
    );

    let active = store
        .list_agent_memory_records(AgentMemoryListFilter {
            scopes: vec![scope(MemoryScopeKind::Workspace, "ws_memory_a")],
            workspace_guard: Some(MemoryWorkspaceGuard {
                workspace_id: "ws_memory_a".to_owned(),
                allow_global_user: false,
                allow_global_agent: false,
            }),
            ..Default::default()
        })
        .await
        .expect("list active memory");
    assert_eq!(active.len(), 1);

    let deleted = store
        .mark_agent_memory_deleted("mem_delete_one", None, Some("user asked".to_owned()), 101)
        .await
        .expect("delete memory")
        .expect("memory should exist");
    assert_eq!(deleted.status, MemoryStatus::Deleted);
    assert_eq!(deleted.active_key, None);

    let active_after_delete = store
        .list_agent_memory_records(AgentMemoryListFilter {
            scopes: vec![scope(MemoryScopeKind::Workspace, "ws_memory_a")],
            ..Default::default()
        })
        .await
        .expect("list after delete");
    assert!(active_after_delete.is_empty());

    let loaded = store
        .get_agent_memory_record("mem_delete_one", true)
        .await
        .expect("get deleted memory")
        .expect("deleted memory should load with include_non_active");
    assert_eq!(loaded.status, MemoryStatus::Deleted);

    let events = store
        .list_agent_memory_events("mem_delete_one", 10)
        .await
        .expect("list events");
    assert!(events.iter().any(|event| event.event_kind == "forgotten"));
}

#[tokio::test]
async fn agent_memory_quarantine_marker_is_idempotent_and_preserves_history() {
    let (_, store) = setup_store().await;
    let memory = store
        .insert_agent_memory_record(
            new_memory(
                "mem_quarantine_one",
                scope(MemoryScopeKind::Workspace, "ws_memory_a"),
                "project.quarantine",
                "This memory can be quarantined.",
            ),
            None,
            130,
        )
        .await
        .expect("insert memory");

    let first = store
        .create_agent_memory_quarantine_marker(NewAgentMemoryQuarantine {
            id: None,
            memory_id: memory.id.clone(),
            workspace_id: memory.workspace_id.clone(),
            reason_code: MemoryLifecycleReasonCode::ManualDeveloperAdminQuarantine,
            actor: MemoryLifecycleActorRecord {
                kind: MemoryLifecycleActorKind::Service,
                id: None,
            },
            details_json: Some(serde_json::json!({"safe": true}).to_string()),
            created_at_unix: 131,
        })
        .await
        .expect("create quarantine");
    let repeated = store
        .create_agent_memory_quarantine_marker(NewAgentMemoryQuarantine {
            id: None,
            memory_id: memory.id.clone(),
            workspace_id: memory.workspace_id.clone(),
            reason_code: MemoryLifecycleReasonCode::ManualDeveloperAdminQuarantine,
            actor: MemoryLifecycleActorRecord {
                kind: MemoryLifecycleActorKind::Service,
                id: None,
            },
            details_json: None,
            created_at_unix: 132,
        })
        .await
        .expect("repeat quarantine");
    assert_eq!(first.id, repeated.id);

    let active = store
        .get_active_agent_memory_quarantine(memory.id.as_str())
        .await
        .expect("active quarantine")
        .expect("active marker exists");
    assert_eq!(active.id, first.id);

    let resolved = store
        .resolve_agent_memory_quarantine(ResolveAgentMemoryQuarantine {
            memory_id: memory.id.clone(),
            reason_code: MemoryLifecycleReasonCode::ExplicitRestore,
            actor: MemoryLifecycleActorRecord {
                kind: MemoryLifecycleActorKind::Service,
                id: None,
            },
            resolved_at_unix: 133,
        })
        .await
        .expect("resolve quarantine")
        .expect("quarantine resolved");
    assert_eq!(
        resolved.resolved_reason_code,
        Some(MemoryLifecycleReasonCode::ExplicitRestore)
    );

    let active_after_restore = store
        .get_active_agent_memory_quarantine(memory.id.as_str())
        .await
        .expect("active quarantine after restore");
    assert!(active_after_restore.is_none());

    let history = store
        .list_agent_memory_quarantine_history(memory.id.as_str(), 10)
        .await
        .expect("history");
    assert_eq!(history.len(), 1);
    assert!(history[0].resolved_at_unix.is_some());

    let events = store
        .list_agent_memory_events(memory.id.as_str(), 10)
        .await
        .expect("events");
    assert!(events.iter().any(|event| event.event_kind == "quarantined"));
    assert!(events.iter().any(|event| event.event_kind == "restored"));
}

#[tokio::test]
async fn agent_memory_access_and_expire_roundtrip() {
    let (_, store) = setup_store().await;
    store
        .insert_agent_memory_record(
            new_memory(
                "mem_expire_one",
                scope(MemoryScopeKind::Workspace, "ws_memory_a"),
                "project.deadline",
                "The launch deadline is Friday.",
            ),
            None,
            120,
        )
        .await
        .expect("insert memory");

    let accessed = store
        .record_agent_memory_access("mem_expire_one", 121)
        .await
        .expect("record access");
    assert!(accessed);

    let loaded_after_access = store
        .get_agent_memory_record("mem_expire_one", true)
        .await
        .expect("get accessed memory")
        .expect("memory should exist");
    assert_eq!(loaded_after_access.access_count, 1);
    assert_eq!(loaded_after_access.last_accessed_at_unix, Some(121));

    let expired = store
        .mark_agent_memory_expired("mem_expire_one", 122)
        .await
        .expect("expire memory")
        .expect("memory should exist");
    assert_eq!(expired.status, MemoryStatus::Expired);
    assert_eq!(expired.active_key, None);

    let active_after_expire = store
        .list_agent_memory_records(AgentMemoryListFilter {
            scopes: vec![scope(MemoryScopeKind::Workspace, "ws_memory_a")],
            ..Default::default()
        })
        .await
        .expect("list after expire");
    assert!(active_after_expire.is_empty());

    let events = store
        .list_agent_memory_events("mem_expire_one", 10)
        .await
        .expect("list events");
    assert!(events.iter().any(|event| event.event_kind == "accessed"));
    assert!(events.iter().any(|event| event.event_kind == "expired"));
}

#[tokio::test]
async fn agent_memory_rejects_empty_namespace_and_key() {
    let (_, store) = setup_store().await;

    let mut empty_namespace = new_memory(
        "mem_empty_namespace",
        scope(MemoryScopeKind::Workspace, "ws_memory_a"),
        "project.valid",
        "This should not be stored.",
    );
    empty_namespace.namespace = Some("   ".to_owned());
    assert!(
        store
            .insert_agent_memory_record(empty_namespace, None, 130)
            .await
            .is_err()
    );

    let mut empty_key = new_memory(
        "mem_empty_key",
        scope(MemoryScopeKind::Workspace, "ws_memory_a"),
        "project.valid",
        "This should not be stored.",
    );
    empty_key.key = Some("   ".to_owned());
    assert!(
        store
            .insert_agent_memory_record(empty_key, None, 131)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn agent_memory_supersede_clears_old_active_key_and_keeps_new_active() {
    let (_, store) = setup_store().await;
    store
        .insert_agent_memory_record(
            new_memory(
                "mem_old_style",
                scope(MemoryScopeKind::Workspace, "ws_memory_a"),
                "project.style",
                "Old style.",
            ),
            None,
            200,
        )
        .await
        .expect("insert old memory");

    let superseded = store
        .mark_agent_memory_superseded("mem_old_style", "mem_new_style", 201)
        .await
        .expect("supersede old memory")
        .expect("old memory should exist");
    assert_eq!(superseded.status, MemoryStatus::Superseded);
    assert_eq!(superseded.active_key, None);
    assert_eq!(superseded.superseded_by.as_deref(), Some("mem_new_style"));

    store
        .insert_agent_memory_record(
            new_memory(
                "mem_new_style",
                scope(MemoryScopeKind::Workspace, "ws_memory_a"),
                "project.style",
                "New style.",
            ),
            None,
            202,
        )
        .await
        .expect("insert replacement memory");

    let active = store
        .get_active_agent_memory_by_key(
            scope(MemoryScopeKind::Workspace, "ws_memory_a"),
            None,
            "project.style",
            None,
        )
        .await
        .expect("active lookup")
        .expect("replacement should be active");
    assert_eq!(active.id, "mem_new_style");

    let active_list = store
        .list_agent_memory_records(AgentMemoryListFilter {
            scopes: vec![scope(MemoryScopeKind::Workspace, "ws_memory_a")],
            ..Default::default()
        })
        .await
        .expect("active list");
    assert_eq!(active_list.len(), 1);
    assert_eq!(active_list[0].id, "mem_new_style");

    let events = store
        .list_agent_memory_events("mem_old_style", 10)
        .await
        .expect("events");
    assert!(events.iter().any(|event| event.event_kind == "superseded"));
}

#[tokio::test]
async fn agent_memory_workspace_guard_blocks_cross_workspace_memory() {
    let (_, store) = setup_store().await;
    store
        .insert_agent_memory_record(
            new_memory(
                "mem_ws_a",
                scope(MemoryScopeKind::Workspace, "ws_memory_a"),
                "project.a",
                "A only.",
            ),
            None,
            300,
        )
        .await
        .expect("insert workspace A memory");
    store
        .insert_agent_memory_record(
            new_memory(
                "mem_ws_b",
                scope(MemoryScopeKind::Workspace, "ws_memory_b"),
                "project.b",
                "B only.",
            ),
            None,
            301,
        )
        .await
        .expect("insert workspace B memory");
    store
        .insert_agent_memory_record(
            new_memory(
                "mem_user_global",
                scope(MemoryScopeKind::User, "default"),
                "user.name",
                "Global user.",
            ),
            None,
            302,
        )
        .await
        .expect("insert global user memory");
    store
        .insert_agent_memory_record(
            new_memory(
                "mem_agent_global",
                scope(
                    MemoryScopeKind::Agent,
                    &global_agent_memory_scope_key("agent_research"),
                ),
                "agent.preference",
                "Global agent.",
            ),
            None,
            303,
        )
        .await
        .expect("insert global agent memory");

    let scopes = vec![
        scope(MemoryScopeKind::Workspace, "ws_memory_a"),
        scope(MemoryScopeKind::Workspace, "ws_memory_b"),
        scope(MemoryScopeKind::User, "default"),
        scope(
            MemoryScopeKind::Agent,
            &global_agent_memory_scope_key("agent_research"),
        ),
    ];

    let only_workspace = store
        .list_agent_memory_records(AgentMemoryListFilter {
            scopes: scopes.clone(),
            workspace_guard: Some(MemoryWorkspaceGuard {
                workspace_id: "ws_memory_a".to_owned(),
                allow_global_user: false,
                allow_global_agent: false,
            }),
            ..Default::default()
        })
        .await
        .expect("guarded list");
    assert_eq!(only_workspace.len(), 1);
    assert_eq!(only_workspace[0].id, "mem_ws_a");

    let with_user = store
        .list_agent_memory_records(AgentMemoryListFilter {
            scopes: scopes.clone(),
            workspace_guard: Some(MemoryWorkspaceGuard {
                workspace_id: "ws_memory_a".to_owned(),
                allow_global_user: true,
                allow_global_agent: false,
            }),
            ..Default::default()
        })
        .await
        .expect("guarded list with user");
    assert_eq!(with_user.len(), 2);
    assert!(
        with_user
            .iter()
            .any(|memory| memory.id == "mem_user_global")
    );

    let with_all_globals = store
        .list_agent_memory_records(AgentMemoryListFilter {
            scopes,
            workspace_guard: Some(MemoryWorkspaceGuard {
                workspace_id: "ws_memory_a".to_owned(),
                allow_global_user: true,
                allow_global_agent: true,
            }),
            ..Default::default()
        })
        .await
        .expect("guarded list with all globals");
    assert_eq!(with_all_globals.len(), 3);
    assert!(
        with_all_globals
            .iter()
            .any(|memory| memory.id == "mem_agent_global")
    );
}

#[tokio::test]
async fn agent_memory_candidate_dedupe_and_decision_roundtrip() {
    let (_, store) = setup_store().await;
    let candidate = NewAgentMemoryCandidate {
        id: Some("cand_memory_one".to_owned()),
        scope: scope(MemoryScopeKind::Workspace, "ws_memory_a"),
        namespace: None,
        category: MemoryCategory::ProjectFact,
        key: Some("project.memory".to_owned()),
        status: None,
        candidate_text: "Use memvid for agent memory.".to_owned(),
        confidence: 0.9,
        reason: "Repeated explicit project instruction.".to_owned(),
        source_context_kind: Some(MemorySourceContextKind::ToolResult),
        source_thread_id: Some("thread_memory_a".to_owned()),
        source_turn_id: Some("turn_candidate".to_owned()),
        source_item_id: None,
        created_by: None,
        dedupe_key: Some("dedupe_memvid".to_owned()),
        metadata_json: None,
    };

    let first = store
        .insert_agent_memory_candidate(candidate.clone(), 400)
        .await
        .expect("insert candidate");
    assert_eq!(
        first.source_context_kind,
        Some(MemorySourceContextKind::ToolResult)
    );
    let second = store
        .insert_agent_memory_candidate(candidate, 401)
        .await
        .expect("dedupe candidate");
    assert_eq!(first.id, second.id);

    let pending = store
        .list_agent_memory_candidates(AgentMemoryCandidateListFilter {
            scopes: vec![scope(MemoryScopeKind::Workspace, "ws_memory_a")],
            ..Default::default()
        })
        .await
        .expect("list pending candidates");
    assert_eq!(pending.len(), 1);

    let category_filtered = store
        .list_agent_memory_candidates(AgentMemoryCandidateListFilter {
            scopes: vec![scope(MemoryScopeKind::Workspace, "ws_memory_a")],
            categories: vec![MemoryCategory::ProjectFact],
            statuses: vec![MemoryCandidateStatus::Pending],
            limit: None,
            workspace_guard: None,
        })
        .await
        .expect("list candidates by category");
    assert_eq!(category_filtered.len(), 1);

    let rejected = store
        .decide_agent_memory_candidate(AgentMemoryCandidateDecisionRecord {
            candidate_id: first.id.clone(),
            decision: MemoryCandidateDecision::Reject,
            decided_by: None,
            decision_reason: Some("too noisy".to_owned()),
            promoted_memory_id: None,
            decided_at_unix: 402,
        })
        .await
        .expect("reject candidate")
        .expect("candidate should be pending");
    assert_eq!(rejected.status, MemoryCandidateStatus::Rejected);

    let pending_after = store
        .list_agent_memory_candidates(AgentMemoryCandidateListFilter {
            scopes: vec![scope(MemoryScopeKind::Workspace, "ws_memory_a")],
            ..Default::default()
        })
        .await
        .expect("list pending after reject");
    assert!(pending_after.is_empty());

    let events = store
        .list_agent_memory_candidate_events(first.id.as_str(), 10)
        .await
        .expect("candidate events");
    assert!(
        events
            .iter()
            .any(|event| event.event_kind == "candidate_rejected")
    );
}

#[tokio::test]
async fn agent_memory_policy_decision_is_append_only_and_queryable() {
    let (_, store) = setup_store().await;
    store
        .insert_agent_memory_policy_decision(NewAgentMemoryPolicyDecision {
            memory_id: Some("mem_policy".to_owned()),
            candidate_id: None,
            workspace_id: Some("ws_memory_a".to_owned()),
            action: "remember".to_owned(),
            decision: "allowed".to_owned(),
            reason_code: Some("explicit_user_request".to_owned()),
            reason: None,
            policy_version: "memory_policy_v1".to_owned(),
            actor: None,
            thread_id: Some("thread_memory_a".to_owned()),
            turn_id: Some("turn_policy".to_owned()),
            item_id: None,
            details_json: None,
            created_at_unix: 500,
        })
        .await
        .expect("insert memory policy decision");
    store
        .insert_agent_memory_policy_decision(NewAgentMemoryPolicyDecision {
            memory_id: Some("mem_policy".to_owned()),
            candidate_id: Some("cand_policy".to_owned()),
            workspace_id: Some("ws_memory_a".to_owned()),
            action: "extract".to_owned(),
            decision: "needs_review".to_owned(),
            reason_code: Some("inferred_fact".to_owned()),
            reason: Some("Needs review.".to_owned()),
            policy_version: "memory_policy_v1".to_owned(),
            actor: None,
            thread_id: Some("thread_memory_a".to_owned()),
            turn_id: Some("turn_policy".to_owned()),
            item_id: None,
            details_json: None,
            created_at_unix: 501,
        })
        .await
        .expect("insert candidate policy decision");

    let by_memory = store
        .list_agent_memory_policy_decisions_for_memory("mem_policy", 10)
        .await
        .expect("list memory decisions");
    assert_eq!(by_memory.len(), 2);
    assert_eq!(by_memory[0].action, "extract");

    let by_candidate = store
        .list_agent_memory_policy_decisions_for_candidate("cand_policy", 10)
        .await
        .expect("list candidate decisions");
    assert_eq!(by_candidate.len(), 1);

    let by_thread = store
        .list_agent_memory_policy_decisions_for_thread("thread_memory_a", 10)
        .await
        .expect("list thread decisions");
    assert_eq!(by_thread.len(), 2);
}

#[tokio::test]
async fn agent_memory_quality_decision_roundtrips_typed_policy_dimensions() {
    let (_, store) = setup_store().await;
    let first = store
        .insert_agent_memory_quality_decision(NewAgentMemoryQualityDecision {
            workspace_id: Some("ws_memory_a".to_owned()),
            thread_id: Some("thread_memory_a".to_owned()),
            turn_id: Some("turn_quality".to_owned()),
            item_id: Some("item_quality".to_owned()),
            task_id: None,
            memory_id: None,
            candidate_id: None,
            canonical_key: Some("auto:identity:name".to_owned()),
            action: MemoryQualityAction::ForceReject,
            target_ownership: MemoryOwnershipClass::Reject,
            source_context_kind: MemorySourceContextKind::AssistantResponse,
            fact_class: MemoryFactClass::UserIdentity,
            lifetime_class: MemoryLifetimeClass::LongLived,
            ownership_class: MemoryOwnershipClass::DurableUserMemory,
            evidence_class: MemoryEvidenceClass::AssistantInference,
            relation: MemoryWriteRelation::Novel,
            reason_codes: vec![
                MemoryQualityReasonCode::AssistantInferenceNotDurableEvidence,
                MemoryQualityReasonCode::SourceNotAuthoritativeForDurableMemory,
            ],
            input_snapshot_json: Some(r#"{"source":"test"}"#.to_owned()),
            created_at_unix: 510,
            updated_at_unix: 510,
        })
        .await
        .expect("insert rejected quality decision");
    assert_eq!(first.action, MemoryQualityAction::ForceReject);
    assert_eq!(first.memory_id, None);
    assert_eq!(
        first.reason_codes,
        vec![
            MemoryQualityReasonCode::AssistantInferenceNotDurableEvidence,
            MemoryQualityReasonCode::SourceNotAuthoritativeForDurableMemory,
        ]
    );

    store
        .insert_agent_memory_quality_decision(NewAgentMemoryQualityDecision {
            workspace_id: Some("ws_memory_a".to_owned()),
            thread_id: Some("thread_memory_a".to_owned()),
            turn_id: Some("turn_quality".to_owned()),
            item_id: None,
            task_id: None,
            memory_id: Some("mem_quality".to_owned()),
            candidate_id: Some("cand_quality".to_owned()),
            canonical_key: Some("auto:identity:name".to_owned()),
            action: MemoryQualityAction::CandidatePolicy,
            target_ownership: MemoryOwnershipClass::DurableUserMemory,
            source_context_kind: MemorySourceContextKind::DirectUserConversation,
            fact_class: MemoryFactClass::UserIdentity,
            lifetime_class: MemoryLifetimeClass::LongLived,
            ownership_class: MemoryOwnershipClass::DurableUserMemory,
            evidence_class: MemoryEvidenceClass::DirectUserAssertion,
            relation: MemoryWriteRelation::Novel,
            reason_codes: vec![
                MemoryQualityReasonCode::CandidatePolicyAllowed,
                MemoryQualityReasonCode::DurableUserIdentity,
                MemoryQualityReasonCode::NovelCandidate,
            ],
            input_snapshot_json: None,
            created_at_unix: 511,
            updated_at_unix: 511,
        })
        .await
        .expect("insert candidate quality decision");

    let by_candidate = store
        .list_agent_memory_quality_decisions_for_candidate("cand_quality", 10)
        .await
        .expect("list by candidate");
    assert_eq!(by_candidate.len(), 1);
    assert_eq!(by_candidate[0].action, MemoryQualityAction::CandidatePolicy);
    assert_eq!(
        by_candidate[0].target_ownership,
        MemoryOwnershipClass::DurableUserMemory
    );

    let by_memory = store
        .list_agent_memory_quality_decisions_for_memory("mem_quality", 10)
        .await
        .expect("list by memory");
    assert_eq!(by_memory.len(), 1);
    assert_eq!(by_memory[0].candidate_id.as_deref(), Some("cand_quality"));

    let by_thread = store
        .list_agent_memory_quality_decisions_for_thread("thread_memory_a", 10)
        .await
        .expect("list by thread");
    assert_eq!(by_thread.len(), 2);
    assert_eq!(by_thread[0].action, MemoryQualityAction::CandidatePolicy);
    assert_eq!(by_thread[1].action, MemoryQualityAction::ForceReject);
}

#[tokio::test]
async fn agent_memory_capsule_upsert_and_repair_status_roundtrip() {
    let (_, store) = setup_store().await;
    let capsule = store
        .upsert_agent_memory_capsule(
            AgentMemoryCapsuleRecord {
                id: Some("capsule_memory_a".to_owned()),
                scope: scope(MemoryScopeKind::Workspace, "ws_memory_a"),
                scope_key_hash: None,
                workspace_id: None,
                scope_slot: None,
                capsule_ref: "memvid://workspace/ws_memory_a".to_owned(),
                storage_uri: "file:///tmp/ws_memory_a.mv2".to_owned(),
                backend: "memvid".to_owned(),
                format: "mv2".to_owned(),
                encrypted: false,
                status: "active".to_owned(),
                repair_status: "ok".to_owned(),
                content_hash: None,
                active_record_count: 0,
                metadata_json: None,
                created_at_unix: None,
                updated_at_unix: None,
                last_error: None,
            },
            600,
        )
        .await
        .expect("upsert capsule");
    assert_eq!(capsule.workspace_id.as_deref(), Some("ws_memory_a"));

    let primary = store
        .find_primary_agent_memory_capsule(scope(MemoryScopeKind::Workspace, "ws_memory_a"))
        .await
        .expect("find primary capsule")
        .expect("primary capsule should exist");
    assert_eq!(primary.capsule_ref, "memvid://workspace/ws_memory_a");

    let by_ref = store
        .find_agent_memory_capsule_by_ref("memvid://workspace/ws_memory_a")
        .await
        .expect("find by ref")
        .expect("capsule by ref should exist");
    assert_eq!(by_ref.id.as_deref(), Some("capsule_memory_a"));

    store
        .mark_agent_memory_capsule_repair_status(
            "capsule_memory_a",
            "repair_needed",
            Some("missing frame".to_owned()),
            601,
        )
        .await
        .expect("mark capsule repair")
        .expect("capsule should exist");

    let repair_needed = store
        .list_agent_memory_capsules_needing_repair(Some("ws_memory_a"), 10)
        .await
        .expect("list repair capsules");
    assert_eq!(repair_needed.len(), 1);
    assert_eq!(repair_needed[0].id.as_deref(), Some("capsule_memory_a"));
}

#[tokio::test]
async fn agent_memory_repair_job_claim_complete_and_retry_roundtrip() {
    let (_, store) = setup_store().await;
    let completed_job = store
        .enqueue_agent_memory_repair_job(
            NewAgentMemoryRepairJob {
                job_kind: "reindex_capsule".to_owned(),
                workspace_id: Some("ws_memory_a".to_owned()),
                scope_kind: Some(MemoryScopeKind::Workspace),
                scope_key_hash: Some("scope_hash".to_owned()),
                memory_id: None,
                capsule_id: Some("capsule_memory_a".to_owned()),
                priority: 10,
                max_attempts: 3,
                scheduled_at_unix: 700,
                payload_json: None,
            },
            700,
        )
        .await
        .expect("enqueue repair");

    let claimed = store
        .claim_due_agent_memory_repair_jobs(701, 60, "worker_one", 10)
        .await
        .expect("claim due jobs");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, completed_job.id);
    assert_eq!(claimed[0].status, "running");

    let completed = store
        .mark_agent_memory_repair_job_completed(
            completed_job.id.as_str(),
            "worker_one",
            Some("{}".to_owned()),
            702,
        )
        .await
        .expect("complete repair")
        .expect("claimed job should complete");
    assert_eq!(completed.status, "completed");

    let retry_job = store
        .enqueue_agent_memory_repair_job(
            NewAgentMemoryRepairJob {
                job_kind: "compact_capsule".to_owned(),
                workspace_id: Some("ws_memory_a".to_owned()),
                scope_kind: Some(MemoryScopeKind::Workspace),
                scope_key_hash: Some("scope_hash".to_owned()),
                memory_id: None,
                capsule_id: Some("capsule_memory_a".to_owned()),
                priority: 1,
                max_attempts: 2,
                scheduled_at_unix: 710,
                payload_json: None,
            },
            710,
        )
        .await
        .expect("enqueue retry job");

    let claimed = store
        .claim_due_agent_memory_repair_jobs(711, 60, "worker_two", 10)
        .await
        .expect("claim retry job");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, retry_job.id);

    let retrying = store
        .mark_agent_memory_repair_job_failed(
            retry_job.id.as_str(),
            "worker_two",
            "transient".to_owned(),
            Some(720),
            712,
        )
        .await
        .expect("fail retry job")
        .expect("job should retry");
    assert_eq!(retrying.status, "pending");
    assert_eq!(retrying.attempts, 1);

    let claimed_again = store
        .claim_due_agent_memory_repair_jobs(721, 60, "worker_two", 10)
        .await
        .expect("claim retry job again");
    assert_eq!(claimed_again.len(), 1);
    assert_eq!(claimed_again[0].id, retry_job.id);

    let failed = store
        .mark_agent_memory_repair_job_failed(
            retry_job.id.as_str(),
            "worker_two",
            "permanent".to_owned(),
            None,
            722,
        )
        .await
        .expect("fail terminal job")
        .expect("job should terminal fail");
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.attempts, 2);
}
