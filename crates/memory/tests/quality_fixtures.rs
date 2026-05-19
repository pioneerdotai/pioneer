use pioneer_memory::{
    MemorySourceContextInput, classify_memory_source_context, classify_semantic_memory_fact,
};
use pioneer_protocol::{
    MemoryAttribute, MemoryCategory, MemoryDurability, MemoryEvidenceActorRole,
    MemoryEvidenceClass, MemoryExplicitness, MemoryExtractorCertainty, MemoryFactClass,
    MemoryIntent, MemoryLifetimeClass, MemoryOwnershipClass, MemoryScope, MemoryScopeHint,
    MemoryScopeKind, MemorySemanticFields, MemorySensitivityHint, MemorySourceContextKind,
    MemorySubject, MemoryWriteEvidence,
};

struct MemoryQualityFixture {
    name: &'static str,
    source_context_kind: MemorySourceContextKind,
    actor_role: MemoryEvidenceActorRole,
    evidence_class: MemoryEvidenceClass,
    semantic: MemorySemanticFields,
    scope: Option<MemoryScope>,
    source_fixture: MemoryQualitySourceFixture,
    expected_fact_class: MemoryFactClass,
    expected_lifetime_class: MemoryLifetimeClass,
    expected_ownership_class: MemoryOwnershipClass,
    text_variants: Vec<&'static str>,
}

#[test]
fn quality_fixtures_map_by_class_not_language_variant() {
    for fixture in quality_fixtures() {
        assert_fixture_text_is_example_data(&fixture);

        let ontology = classify_semantic_memory_fact(&fixture.semantic, fixture.scope.as_ref());
        let source_context = classify_source_fixture(fixture.source_fixture);

        assert_eq!(
            ontology.fact_class, fixture.expected_fact_class,
            "{} fact class",
            fixture.name
        );
        assert_eq!(
            ontology.lifetime_class, fixture.expected_lifetime_class,
            "{} lifetime class",
            fixture.name
        );
        assert_eq!(
            ontology.proposed_ownership_class, fixture.expected_ownership_class,
            "{} ownership class",
            fixture.name
        );
        assert_eq!(
            source_context.context_kind, fixture.source_context_kind,
            "{} source context",
            fixture.name
        );
        assert_eq!(
            source_context.actor_role, fixture.actor_role,
            "{} actor role",
            fixture.name
        );
        assert_eq!(
            source_context.evidence_class, fixture.evidence_class,
            "{} evidence class",
            fixture.name
        );
    }
}

fn quality_fixtures() -> Vec<MemoryQualityFixture> {
    vec![
        MemoryQualityFixture {
            name: "direct_user_identity_long_lived",
            source_context_kind: MemorySourceContextKind::DirectUserConversation,
            actor_role: MemoryEvidenceActorRole::User,
            evidence_class: MemoryEvidenceClass::DirectUserAssertion,
            semantic: semantic(
                MemoryCategory::Identity,
                MemorySubject::CurrentUser,
                MemoryAttribute::Name,
                MemoryScopeHint::UserGlobal,
                MemoryDurability::LongLived,
            ),
            scope: Some(scope(MemoryScopeKind::User, "user-1")),
            source_fixture: MemoryQualitySourceFixture::DirectUserAssertion,
            expected_fact_class: MemoryFactClass::UserIdentity,
            expected_lifetime_class: MemoryLifetimeClass::LongLived,
            expected_ownership_class: MemoryOwnershipClass::DurableUserMemory,
            text_variants: vec!["Меня зовут Александр.", "My name is Alexander."],
        },
        MemoryQualityFixture {
            name: "direct_user_stable_preference_long_lived",
            source_context_kind: MemorySourceContextKind::DirectUserConversation,
            actor_role: MemoryEvidenceActorRole::User,
            evidence_class: MemoryEvidenceClass::DirectUserAssertion,
            semantic: semantic(
                MemoryCategory::Preference,
                MemorySubject::CurrentUser,
                MemoryAttribute::PreferredLanguage,
                MemoryScopeHint::UserGlobal,
                MemoryDurability::LongLived,
            ),
            scope: Some(scope(MemoryScopeKind::User, "user-1")),
            source_fixture: MemoryQualitySourceFixture::DirectUserAssertion,
            expected_fact_class: MemoryFactClass::StableUserPreference,
            expected_lifetime_class: MemoryLifetimeClass::LongLived,
            expected_ownership_class: MemoryOwnershipClass::DurableUserMemory,
            text_variants: vec!["Отвечай мне на русском.", "Please answer in English."],
        },
        MemoryQualityFixture {
            name: "direct_user_communication_preference_long_lived",
            source_context_kind: MemorySourceContextKind::DirectUserConversation,
            actor_role: MemoryEvidenceActorRole::User,
            evidence_class: MemoryEvidenceClass::DirectUserAssertion,
            semantic: semantic(
                MemoryCategory::Preference,
                MemorySubject::CurrentUser,
                MemoryAttribute::CommunicationStyle,
                MemoryScopeHint::UserGlobal,
                MemoryDurability::LongLived,
            ),
            scope: Some(scope(MemoryScopeKind::User, "user-1")),
            source_fixture: MemoryQualitySourceFixture::DirectUserAssertion,
            expected_fact_class: MemoryFactClass::CommunicationPreference,
            expected_lifetime_class: MemoryLifetimeClass::LongLived,
            expected_ownership_class: MemoryOwnershipClass::DurableUserMemory,
            text_variants: vec!["Пиши коротко и прямо.", "Keep replies direct."],
        },
        MemoryQualityFixture {
            name: "project_decision_project_lifetime",
            source_context_kind: MemorySourceContextKind::DirectUserConversation,
            actor_role: MemoryEvidenceActorRole::User,
            evidence_class: MemoryEvidenceClass::DirectUserAssertion,
            semantic: semantic(
                MemoryCategory::ProjectDecision,
                MemorySubject::Project,
                MemoryAttribute::PhaseNaming,
                MemoryScopeHint::ProjectWorkspace,
                MemoryDurability::ProjectLifetime,
            ),
            scope: Some(scope(MemoryScopeKind::Workspace, "workspace-1")),
            source_fixture: MemoryQualitySourceFixture::DirectUserAssertion,
            expected_fact_class: MemoryFactClass::ProjectDecision,
            expected_lifetime_class: MemoryLifetimeClass::ProjectLifetime,
            expected_ownership_class: MemoryOwnershipClass::DurableWorkspaceMemory,
            text_variants: vec![
                "В этом проекте оставляем название Phase.",
                "For this project, keep the Phase naming.",
            ],
        },
        MemoryQualityFixture {
            name: "task_lifecycle_state_task_lifetime",
            source_context_kind: MemorySourceContextKind::TaskRuntime,
            actor_role: MemoryEvidenceActorRole::Task,
            evidence_class: MemoryEvidenceClass::TaskRuntimeObservation,
            semantic: semantic(
                MemoryCategory::Todo,
                MemorySubject::Project,
                MemoryAttribute::Custom,
                MemoryScopeHint::ProjectWorkspace,
                MemoryDurability::SessionOnly,
            ),
            scope: Some(scope(MemoryScopeKind::Workspace, "workspace-1")),
            source_fixture: MemoryQualitySourceFixture::TaskRuntime,
            expected_fact_class: MemoryFactClass::TaskLifecycleState,
            expected_lifetime_class: MemoryLifetimeClass::TaskLifetime,
            expected_ownership_class: MemoryOwnershipClass::TaskRuntimeState,
            text_variants: vec!["The task is running.", "La tarea esta en curso."],
        },
        MemoryQualityFixture {
            name: "operational_observation_naturally_expiring",
            source_context_kind: MemorySourceContextKind::SystemRuntime,
            actor_role: MemoryEvidenceActorRole::System,
            evidence_class: MemoryEvidenceClass::SystemObservation,
            semantic: semantic(
                MemoryCategory::ProjectFact,
                MemorySubject::Workspace,
                MemoryAttribute::Custom,
                MemoryScopeHint::ProjectWorkspace,
                MemoryDurability::Transient,
            ),
            scope: Some(scope(MemoryScopeKind::Workspace, "workspace-1")),
            source_fixture: MemoryQualitySourceFixture::SystemRuntime,
            expected_fact_class: MemoryFactClass::OperationalObservation,
            expected_lifetime_class: MemoryLifetimeClass::NaturallyExpiring,
            expected_ownership_class: MemoryOwnershipClass::DomainRuntimeState,
            text_variants: vec![
                "OpenRouter request latency is high right now.",
                "La latence actuelle est elevee.",
            ],
        },
        MemoryQualityFixture {
            name: "thread_local_state_thread_lifetime",
            source_context_kind: MemorySourceContextKind::DirectUserConversation,
            actor_role: MemoryEvidenceActorRole::User,
            evidence_class: MemoryEvidenceClass::DirectUserAssertion,
            semantic: semantic(
                MemoryCategory::Todo,
                MemorySubject::CurrentUser,
                MemoryAttribute::Custom,
                MemoryScopeHint::UserWorkspace,
                MemoryDurability::SessionOnly,
            ),
            scope: Some(scope(MemoryScopeKind::User, "user-1")),
            source_fixture: MemoryQualitySourceFixture::DirectUserAssertion,
            expected_fact_class: MemoryFactClass::ThreadLocalState,
            expected_lifetime_class: MemoryLifetimeClass::ThreadLifetime,
            expected_ownership_class: MemoryOwnershipClass::ThreadEpisodicContext,
            text_variants: vec![
                "Вернемся к этому в конце треда.",
                "Keep this for this thread.",
            ],
        },
        MemoryQualityFixture {
            name: "tool_result_fact_system_owned",
            source_context_kind: MemorySourceContextKind::ToolResult,
            actor_role: MemoryEvidenceActorRole::Tool,
            evidence_class: MemoryEvidenceClass::ToolObservation,
            semantic: semantic(
                MemoryCategory::ProjectFact,
                MemorySubject::Artifact,
                MemoryAttribute::Custom,
                MemoryScopeHint::ProjectWorkspace,
                MemoryDurability::Transient,
            ),
            scope: Some(scope(MemoryScopeKind::Workspace, "workspace-1")),
            source_fixture: MemoryQualitySourceFixture::ToolResult,
            expected_fact_class: MemoryFactClass::ToolResultFact,
            expected_lifetime_class: MemoryLifetimeClass::NaturallyExpiring,
            expected_ownership_class: MemoryOwnershipClass::DomainRuntimeState,
            text_variants: vec!["cargo check failed.", "La commande a echoue."],
        },
        MemoryQualityFixture {
            name: "assistant_self_description_assistant_source",
            source_context_kind: MemorySourceContextKind::AssistantResponse,
            actor_role: MemoryEvidenceActorRole::Assistant,
            evidence_class: MemoryEvidenceClass::AssistantInference,
            semantic: semantic(
                MemoryCategory::Identity,
                MemorySubject::CurrentAgent,
                MemoryAttribute::Name,
                MemoryScopeHint::AgentGlobal,
                MemoryDurability::LongLived,
            ),
            scope: Some(scope(MemoryScopeKind::Agent, "agent-1")),
            source_fixture: MemoryQualitySourceFixture::AssistantResponse,
            expected_fact_class: MemoryFactClass::AssistantSelfDescription,
            expected_lifetime_class: MemoryLifetimeClass::LongLived,
            expected_ownership_class: MemoryOwnershipClass::DurableAgentMemory,
            text_variants: vec!["Я Pioneer.", "I am Pioneer."],
        },
        MemoryQualityFixture {
            name: "unknown_or_weak_evidence",
            source_context_kind: MemorySourceContextKind::DirectUserConversation,
            actor_role: MemoryEvidenceActorRole::User,
            evidence_class: MemoryEvidenceClass::MissingOrWeak,
            semantic: semantic(
                MemoryCategory::Custom,
                MemorySubject::Custom,
                MemoryAttribute::Custom,
                MemoryScopeHint::Unknown,
                MemoryDurability::Unknown,
            ),
            scope: None,
            source_fixture: MemoryQualitySourceFixture::UserTextWeak,
            expected_fact_class: MemoryFactClass::Unknown,
            expected_lifetime_class: MemoryLifetimeClass::Unknown,
            expected_ownership_class: MemoryOwnershipClass::AuditOnly,
            text_variants: vec!["Может быть потом.", "Maybe later."],
        },
    ]
}

fn semantic(
    category: MemoryCategory,
    subject: MemorySubject,
    attribute: MemoryAttribute,
    scope_hint: MemoryScopeHint,
    durability: MemoryDurability,
) -> MemorySemanticFields {
    MemorySemanticFields {
        intent: MemoryIntent::ImplicitCandidate,
        explicitness: MemoryExplicitness::Implicit,
        category,
        subject,
        attribute,
        subject_key: None,
        custom_subject: None,
        custom_attribute: None,
        scope_hint,
        durability,
        sensitivity: MemorySensitivityHint::Low,
        certainty: MemoryExtractorCertainty::High,
    }
}

fn scope(kind: MemoryScopeKind, key: &str) -> MemoryScope {
    MemoryScope {
        kind,
        key: key.to_owned(),
    }
}

#[derive(Debug, Clone, Copy)]
enum MemoryQualitySourceFixture {
    DirectUserAssertion,
    AssistantResponse,
    ToolResult,
    TaskRuntime,
    SystemRuntime,
    UserTextWeak,
}

fn classify_source_fixture(
    fixture: MemoryQualitySourceFixture,
) -> pioneer_memory::MemorySourceContextClassification {
    let evidence = match fixture {
        MemoryQualitySourceFixture::DirectUserAssertion => Some(evidence("turn.post_turn:user")),
        MemoryQualitySourceFixture::AssistantResponse => Some(evidence("turn.post_turn:assistant")),
        MemoryQualitySourceFixture::ToolResult => Some(evidence("tool:cargo")),
        MemoryQualitySourceFixture::SystemRuntime => Some(evidence("system:runtime")),
        MemoryQualitySourceFixture::TaskRuntime | MemoryQualitySourceFixture::UserTextWeak => None,
    };
    classify_memory_source_context(MemorySourceContextInput {
        task_id: matches!(fixture, MemoryQualitySourceFixture::TaskRuntime).then_some("task-1"),
        has_user_text: matches!(fixture, MemoryQualitySourceFixture::UserTextWeak),
        evidence: evidence.as_ref(),
        ..MemorySourceContextInput::default()
    })
}

fn evidence(source_ref: &'static str) -> MemoryWriteEvidence {
    MemoryWriteEvidence {
        source_thread_id: Some("thread-1".to_owned()),
        source_turn_id: Some("turn-1".to_owned()),
        source_item_id: None,
        source_ref: Some(source_ref.to_owned()),
        quote_or_span: Some("structured evidence".to_owned()),
        extractor_reason: None,
    }
}

fn assert_fixture_text_is_example_data(fixture: &MemoryQualityFixture) {
    assert!(
        fixture.text_variants.len() >= 2,
        "{} should carry multilingual examples",
        fixture.name
    );
    for variant in &fixture.text_variants {
        assert!(
            !variant.trim().is_empty(),
            "{} has an empty text variant",
            fixture.name
        );
    }
}
