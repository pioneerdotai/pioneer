use super::*;
use crate::extractor_ontology::extractor_ontology_proposal_from_metadata;
use pioneer_protocol::{
    MemoryEvidenceClass, MemoryFactClass, MemoryLifetimeClass, MemoryOwnershipClass,
};

fn assert_post_turn_eligibility_skip(
    response: &HookHandlerResponse,
    reason: MemoryPostTurnEligibilitySkipReason,
) {
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == reason.diagnostic_code()
            && diagnostic.metadata.get(&hook_metadata_key("skip_reason"))
                == Some(&HookValue::Text(reason.as_str().to_owned()))
    }));
    for diagnostic in &response.diagnostics {
        assert!(
            !diagnostic
                .message
                .as_str()
                .contains("direct user transcript")
        );
        assert!(!diagnostic.message.as_str().contains("assistant response"));
    }
}

#[test]
fn strict_json_parser_accepts_typed_semantic_fact() {
    let parsed = parse_memory_post_turn_extractor_json(
        valid_post_turn_extractor_json().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("valid extractor JSON parses");

    assert_eq!(parsed.raw_fact_count, 1);
    assert_eq!(parsed.facts.len(), 1);
    assert!(
        parsed
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("ontology_proposal_missing"))
    );
    let fact = &parsed.facts[0];
    assert_eq!(fact.semantic.intent, MemoryIntent::ExplicitStore);
    assert_eq!(fact.semantic.category, MemoryCategory::Identity);
    assert_eq!(fact.semantic.subject, MemorySubject::CurrentUser);
    assert_eq!(fact.semantic.attribute, MemoryAttribute::Name);
    assert_eq!(fact.content, "Имя пользователя: Александр");
    assert_eq!(fact.value.as_deref(), Some("Александр"));
    assert!(fact.confidence.expect("computed confidence") > 0.95);
    assert!(fact.importance.expect("computed importance") > 0.90);
    assert_eq!(
        fact.evidence.quote_or_span.as_deref(),
        Some("Меня зовут Александр")
    );
}

#[test]
fn post_turn_parser_accepts_typed_ontology_proposal() {
    let parsed = parse_memory_post_turn_extractor_json(
        valid_post_turn_extractor_json_with_ontology().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("valid extractor JSON parses");

    assert_eq!(parsed.raw_fact_count, 1);
    assert_eq!(parsed.validation_rejected_count, 0);
    assert_eq!(parsed.facts.len(), 1);
    let proposal = parsed.facts[0]
        .ontology_proposal
        .expect("ontology proposal is parsed");
    assert_eq!(proposal.fact_class, MemoryFactClass::UserIdentity);
    assert_eq!(proposal.lifetime_class, MemoryLifetimeClass::LongLived);
    assert_eq!(
        proposal.evidence_class,
        MemoryEvidenceClass::DirectUserAssertion
    );
    assert_eq!(
        proposal.proposed_ownership_class,
        MemoryOwnershipClass::DurableUserMemory
    );

    let params = memory_semantic_write_params_from_extracted_fact(
        0,
        parsed.facts.into_iter().next().expect("fact exists"),
        &test_memory_turn_context(),
        &MemoryTurnPolicy::normal_default_allow(),
        &MemoryPostTurnExtractorConfig::default(),
        MemorySourceContextKind::DirectUserConversation,
        Some("test-model"),
        Some("test-provider"),
    )
    .expect("semantic write params are produced");
    assert_eq!(
        extractor_ontology_proposal_from_metadata(&params.metadata),
        Some(proposal)
    );
}

#[test]
fn post_turn_parser_accepts_class_based_valid_ontology_proposals() {
    let cases = [
        (
            "durable_workspace_project_decision",
            serde_json::json!({
                "intent": "implicit_candidate",
                "explicitness": "implicit",
                "category": "project_decision",
                "subject": "project",
                "attribute": "custom",
                "subject_key": "pioneer",
                "custom_attribute": "architecture_decision",
                "scope_hint": "project_workspace",
                "durability": "project_lifetime",
                "sensitivity": "none",
                "certainty": "high"
            }),
            serde_json::json!({
                "fact_class": "project_decision",
                "lifetime_class": "project_lifetime",
                "evidence_class": "direct_user_assertion",
                "proposed_ownership_class": "durable_workspace_memory"
            }),
            MemoryOwnershipClass::DurableWorkspaceMemory,
            "turn.post_turn:user",
        ),
        (
            "thread_episodic_state",
            serde_json::json!({
                "intent": "implicit_candidate",
                "explicitness": "implicit",
                "category": "todo",
                "subject": "current_user",
                "attribute": "custom",
                "custom_attribute": "thread_follow_up",
                "scope_hint": "user_workspace",
                "durability": "session_only",
                "sensitivity": "none",
                "certainty": "high"
            }),
            serde_json::json!({
                "fact_class": "thread_local_state",
                "lifetime_class": "thread_lifetime",
                "evidence_class": "direct_user_assertion",
                "proposed_ownership_class": "thread_episodic_context"
            }),
            MemoryOwnershipClass::ThreadEpisodicContext,
            "turn.post_turn:user",
        ),
        (
            "task_runtime_state",
            serde_json::json!({
                "intent": "implicit_candidate",
                "explicitness": "implicit",
                "category": "todo",
                "subject": "project",
                "attribute": "custom",
                "subject_key": "pioneer",
                "custom_attribute": "task_state",
                "scope_hint": "project_workspace",
                "durability": "session_only",
                "sensitivity": "none",
                "certainty": "high"
            }),
            serde_json::json!({
                "fact_class": "task_lifecycle_state",
                "lifetime_class": "task_lifetime",
                "evidence_class": "direct_user_assertion",
                "proposed_ownership_class": "task_runtime_state"
            }),
            MemoryOwnershipClass::TaskRuntimeState,
            "turn.post_turn:user",
        ),
        (
            "domain_tool_observation",
            serde_json::json!({
                "intent": "implicit_candidate",
                "explicitness": "implicit",
                "category": "project_fact",
                "subject": "artifact",
                "attribute": "custom",
                "subject_key": "build_log",
                "custom_attribute": "tool_observation",
                "scope_hint": "project_workspace",
                "durability": "transient",
                "sensitivity": "none",
                "certainty": "high"
            }),
            serde_json::json!({
                "fact_class": "tool_result_fact",
                "lifetime_class": "naturally_expiring",
                "evidence_class": "tool_observation",
                "proposed_ownership_class": "domain_runtime_state"
            }),
            MemoryOwnershipClass::DomainRuntimeState,
            "turn.post_turn:tool",
        ),
    ];

    for (label, semantic, ontology, ownership_class, source_ref) in cases {
        let raw = serde_json::json!({
            "facts": [{
                "semantic": semantic,
                "ontology": ontology,
                "content": format!("class based fact: {label}"),
                "value": label,
                "evidence": {
                    "source_ref": source_ref,
                    "quote_or_span": "class based evidence",
                    "extractor_reason": "Class based regression fixture."
                }
            }]
        });

        let parsed = parse_memory_post_turn_extractor_json(
            raw.to_string().as_str(),
            &MemoryPostTurnExtractorConfig::default(),
        )
        .unwrap_or_else(|error| panic!("{label}: valid ontology payload should parse: {error}"));

        assert_eq!(parsed.raw_fact_count, 1, "{label}");
        assert_eq!(parsed.validation_rejected_count, 0, "{label}");
        assert_eq!(parsed.facts.len(), 1, "{label}");
        assert_eq!(
            parsed.facts[0]
                .ontology_proposal
                .expect("proposal")
                .proposed_ownership_class,
            ownership_class,
            "{label}"
        );
    }
}

#[test]
fn post_turn_parser_rejects_invalid_ontology_proposal_safely() {
    let raw = serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "explicit_store",
                "explicitness": "explicit",
                "category": "identity",
                "subject": "current_user",
                "attribute": "name",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "personal",
                "certainty": "high"
            },
            "ontology": {
                "fact_class": "future_fact_class",
                "lifetime_class": "long_lived",
                "evidence_class": "direct_user_assertion",
                "proposed_ownership_class": "durable_user_memory"
            },
            "content": "Имя пользователя: Александр",
            "value": "Александр",
            "evidence": {
                "source_ref": "turn.post_turn:user",
                "quote_or_span": "Меня зовут Александр",
                "extractor_reason": "The user directly stated their name."
            }
        }]
    });

    let parsed = parse_memory_post_turn_extractor_json(
        raw.to_string().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("invalid proposal value rejects the fact, not the whole payload");

    assert_eq!(parsed.raw_fact_count, 1);
    assert_eq!(parsed.validation_rejected_count, 1);
    assert!(parsed.facts.is_empty());
    assert!(
        parsed
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("unknown_ontology_proposal"))
    );
}

#[test]
fn post_turn_parser_rejects_partial_ontology_proposal() {
    let raw = serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "explicit_store",
                "explicitness": "explicit",
                "category": "identity",
                "subject": "current_user",
                "attribute": "name",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "personal",
                "certainty": "high"
            },
            "ontology": {
                "fact_class": "user_identity",
                "lifetime_class": "long_lived"
            },
            "content": "Имя пользователя: Александр",
            "value": "Александр",
            "evidence": {
                "source_ref": "turn.post_turn:user",
                "quote_or_span": "Меня зовут Александр",
                "extractor_reason": "The user directly stated their name."
            }
        }]
    });

    let parsed = parse_memory_post_turn_extractor_json(
        raw.to_string().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("partial proposal rejects the fact, not the whole payload");

    assert_eq!(parsed.raw_fact_count, 1);
    assert_eq!(parsed.validation_rejected_count, 1);
    assert!(parsed.facts.is_empty());
    assert!(
        parsed
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("partial_ontology_proposal"))
    );
}

#[test]
fn post_turn_parser_rejects_invalid_proposal_enum_without_dropping_payload() {
    let raw = serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "explicit_store",
                "explicitness": "explicit",
                "category": "identity",
                "subject": "current_user",
                "attribute": "name",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "personal",
                "certainty": "high"
            },
            "ontology": {
                "fact_class": "user_identity",
                "lifetime_class": "long_lived",
                "evidence_class": "future_evidence",
                "proposed_ownership_class": "durable_user_memory"
            },
            "content": "Имя пользователя: Александр",
            "value": "Александр",
            "evidence": {
                "source_ref": "turn.post_turn:user",
                "quote_or_span": "Меня зовут Александр",
                "extractor_reason": "The user directly stated their name."
            }
        }]
    });

    let parsed = parse_memory_post_turn_extractor_json(
        raw.to_string().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("invalid proposal enum rejects the fact, not the whole payload");

    assert_eq!(parsed.raw_fact_count, 1);
    assert_eq!(parsed.validation_rejected_count, 1);
    assert!(parsed.facts.is_empty());
    assert!(
        parsed
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("invalid_ontology_proposal_enum"))
    );
}

#[test]
fn post_turn_parser_proposal_cannot_rescue_missing_evidence() {
    let raw = serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "explicit_store",
                "explicitness": "explicit",
                "category": "identity",
                "subject": "current_user",
                "attribute": "name",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "personal",
                "certainty": "high"
            },
            "ontology": {
                "fact_class": "user_identity",
                "lifetime_class": "long_lived",
                "evidence_class": "direct_user_assertion",
                "proposed_ownership_class": "durable_user_memory"
            },
            "content": "Имя пользователя: Александр",
            "value": "Александр",
            "evidence": {
                "source_ref": "turn.post_turn:user",
                "extractor_reason": "The user directly stated their name."
            }
        }]
    });

    let parsed = parse_memory_post_turn_extractor_json(
        raw.to_string().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("missing evidence JSON shape parses but fact is rejected");

    assert_eq!(parsed.raw_fact_count, 1);
    assert_eq!(parsed.validation_rejected_count, 1);
    assert!(parsed.facts.is_empty());
    assert!(
        parsed
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("missing_evidence_quote"))
    );
}

#[test]
fn post_turn_parser_suppresses_weak_evidence_and_unclear_ownership_proposals() {
    let weak_evidence = serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "explicit_store",
                "explicitness": "explicit",
                "category": "identity",
                "subject": "current_user",
                "attribute": "name",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "personal",
                "certainty": "high"
            },
            "ontology": {
                "fact_class": "user_identity",
                "lifetime_class": "long_lived",
                "evidence_class": "missing_or_weak",
                "proposed_ownership_class": "durable_user_memory"
            },
            "content": "Имя пользователя: Александр",
            "value": "Александр",
            "evidence": {
                "source_ref": "turn.post_turn:user",
                "quote_or_span": "Меня зовут Александр",
                "extractor_reason": "The user directly stated their name."
            }
        }]
    });
    let weak = parse_memory_post_turn_extractor_json(
        weak_evidence.to_string().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("weak evidence payload parses");
    assert_eq!(weak.validation_rejected_count, 1);
    assert!(weak.facts.is_empty());
    assert!(
        weak.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("weak_evidence_class"))
    );

    let unclear_ownership = serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "explicit_store",
                "explicitness": "explicit",
                "category": "identity",
                "subject": "current_user",
                "attribute": "name",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "personal",
                "certainty": "high"
            },
            "ontology": {
                "fact_class": "user_identity",
                "lifetime_class": "long_lived",
                "evidence_class": "direct_user_assertion",
                "proposed_ownership_class": "audit_only"
            },
            "content": "Имя пользователя: Александр",
            "value": "Александр",
            "evidence": {
                "source_ref": "turn.post_turn:user",
                "quote_or_span": "Меня зовут Александр",
                "extractor_reason": "The user directly stated their name."
            }
        }]
    });
    let unclear = parse_memory_post_turn_extractor_json(
        unclear_ownership.to_string().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("unclear ownership payload parses");
    assert_eq!(unclear.validation_rejected_count, 1);
    assert!(unclear.facts.is_empty());
    assert!(
        unclear
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("unclear_or_rejected_ownership"))
    );
}

#[test]
fn post_turn_parser_requires_source_ref_when_proposal_is_present() {
    let raw = serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "explicit_store",
                "explicitness": "explicit",
                "category": "identity",
                "subject": "current_user",
                "attribute": "name",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "personal",
                "certainty": "high"
            },
            "ontology": {
                "fact_class": "user_identity",
                "lifetime_class": "long_lived",
                "evidence_class": "direct_user_assertion",
                "proposed_ownership_class": "durable_user_memory"
            },
            "content": "Имя пользователя: Александр",
            "value": "Александр",
            "evidence": {
                "quote_or_span": "Меня зовут Александр",
                "extractor_reason": "The user directly stated their name."
            }
        }]
    });

    let parsed = parse_memory_post_turn_extractor_json(
        raw.to_string().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("missing source ref rejects the fact, not the whole payload");

    assert_eq!(parsed.validation_rejected_count, 1);
    assert!(parsed.facts.is_empty());
    assert!(
        parsed
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("missing_evidence_source_ref"))
    );
}

#[test]
fn post_turn_parser_rejects_assistant_inference_about_user() {
    let raw = serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "implicit_candidate",
                "explicitness": "implicit",
                "category": "preference",
                "subject": "current_user",
                "attribute": "communication_style",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "low",
                "certainty": "medium"
            },
            "ontology": {
                "fact_class": "communication_preference",
                "lifetime_class": "long_lived",
                "evidence_class": "assistant_inference",
                "proposed_ownership_class": "durable_user_memory"
            },
            "content": "Пользователь предпочитает краткие ответы.",
            "value": "краткие ответы",
            "evidence": {
                "source_ref": "turn.post_turn:assistant",
                "quote_or_span": "Буду отвечать кратко.",
                "extractor_reason": "Assistant inferred a user preference."
            }
        }]
    });

    let parsed = parse_memory_post_turn_extractor_json(
        raw.to_string().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("assistant inference payload parses");

    assert_eq!(parsed.validation_rejected_count, 1);
    assert!(parsed.facts.is_empty());
    assert!(
        parsed
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("assistant_inference_about_user"))
    );
}

#[test]
fn post_turn_parser_allows_valid_thread_episodic_proposal() {
    let raw = serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "implicit_candidate",
                "explicitness": "implicit",
                "category": "todo",
                "subject": "project",
                "attribute": "custom",
                "subject_key": "project",
                "custom_attribute": "thread_follow_up",
                "scope_hint": "project_workspace",
                "durability": "session_only",
                "sensitivity": "none",
                "certainty": "high"
            },
            "ontology": {
                "fact_class": "thread_local_state",
                "lifetime_class": "thread_lifetime",
                "evidence_class": "direct_user_assertion",
                "proposed_ownership_class": "thread_episodic_context"
            },
            "content": "В этом треде нужно вернуться к активной ветке.",
            "value": "вернуться к активной ветке",
            "evidence": {
                "source_ref": "turn.post_turn:user",
                "quote_or_span": "в этом треде вернись к активной ветке",
                "extractor_reason": "The user stated a thread-local follow-up."
            }
        }]
    });

    let parsed = parse_memory_post_turn_extractor_json(
        raw.to_string().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("thread episodic payload parses");

    assert_eq!(parsed.validation_rejected_count, 0);
    assert_eq!(parsed.facts.len(), 1);
    assert_eq!(
        parsed.facts[0]
            .ontology_proposal
            .expect("proposal")
            .proposed_ownership_class,
        MemoryOwnershipClass::ThreadEpisodicContext
    );
}

#[test]
fn post_turn_parser_computes_scores_instead_of_trusting_llm_numbers() {
    let raw = serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "explicit_store",
                "explicitness": "explicit",
                "category": "identity",
                "subject": "current_user",
                "attribute": "name",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "personal",
                "certainty": "high"
            },
            "content": "User's name is Александр.",
            "value": "Александр",
            "evidence": {
                "source_ref": "turn.post_turn:user",
                "quote_or_span": "Меня зовут Александр.",
                "extractor_reason": "User explicitly stated their name."
            },
            "confidence": 0.0,
            "importance": 0.0
        }]
    });

    let parsed = parse_memory_post_turn_extractor_json(
        raw.to_string().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("valid extractor JSON parses");

    assert_eq!(parsed.facts.len(), 1);
    let fact = &parsed.facts[0];
    assert!(fact.confidence.expect("computed confidence") > 0.95);
    assert!(fact.importance.expect("computed importance") > 0.90);
}

#[test]
fn post_turn_parser_forces_personal_sensitivity_for_user_identity() {
    let raw = serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "explicit_store",
                "explicitness": "explicit",
                "category": "identity",
                "subject": "current_user",
                "attribute": "name",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "none",
                "certainty": "high"
            },
            "content": "Имя пользователя: Александр.",
            "value": "Александр",
            "evidence": {
                "source_ref": "turn.post_turn:user",
                "quote_or_span": "Меня зовут Александр.",
                "extractor_reason": "User explicitly stated their name."
            }
        }]
    });

    let parsed = parse_memory_post_turn_extractor_json(
        raw.to_string().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("valid extractor JSON parses");

    assert_eq!(parsed.facts.len(), 1);
    assert_eq!(
        parsed.facts[0].semantic.sensitivity,
        MemorySensitivityHint::Personal
    );

    let params = memory_semantic_write_params_from_extracted_fact(
        0,
        parsed.facts.into_iter().next().expect("fact exists"),
        &test_memory_turn_context(),
        &MemoryTurnPolicy::normal_default_allow(),
        &MemoryPostTurnExtractorConfig::default(),
        MemorySourceContextKind::DirectUserConversation,
        Some("test-model"),
        Some("test-provider"),
    )
    .expect("semantic write params are produced");
    assert_eq!(params.semantic.sensitivity, MemorySensitivityHint::Personal);
}

#[test]
fn post_turn_parser_rejects_assistant_self_description_from_assistant_text() {
    let raw = serde_json::json!({
        "facts": [
            {
                "semantic": {
                    "intent": "explicit_store",
                    "explicitness": "explicit",
                    "category": "identity",
                    "subject": "current_user",
                    "attribute": "name",
                    "scope_hint": "user_global",
                    "durability": "long_lived",
                    "sensitivity": "personal",
                    "certainty": "high"
                },
                "content": "User's name is Александр.",
                "value": "Александр",
                "evidence": {
                    "source_ref": "turn.post_turn:user",
                    "quote_or_span": "Меня зовут Александр.",
                    "extractor_reason": "User explicitly stated their name."
                },
                "confidence": 0.0,
                "importance": 0.0
            },
            {
                "semantic": {
                    "intent": "explicit_store",
                    "explicitness": "explicit",
                    "category": "identity",
                    "subject": "current_agent",
                    "attribute": "name",
                    "scope_hint": "agent_global",
                    "durability": "long_lived",
                    "sensitivity": "none",
                    "certainty": "high"
                },
                "content": "Agent's name is Pioneer.",
                "value": "Pioneer",
                "evidence": {
                    "source_ref": "turn.post_turn:assistant",
                    "quote_or_span": "Я Pioneer.",
                    "extractor_reason": "Assistant introduced itself."
                },
                "confidence": 1.0,
                "importance": 1.0
            }
        ]
    });

    let parsed = parse_memory_post_turn_extractor_json(
        raw.to_string().as_str(),
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("valid extractor JSON parses");

    assert_eq!(parsed.raw_fact_count, 2);
    assert_eq!(parsed.facts.len(), 1);
    assert_eq!(parsed.facts[0].semantic.subject, MemorySubject::CurrentUser);
    assert!(
        parsed
            .diagnostics
            .iter()
            .any(|diagnostic| { diagnostic.contains("assistant_self_description") })
    );
}

#[test]
fn parser_rejects_unknown_enums_and_ignores_unknown_keys() {
    let unknown_enum = r#"{"facts":[{"semantic":{"intent":"write_now","explicitness":"explicit","category":"identity","subject":"current_user","attribute":"name","scope_hint":"user_global","durability":"long_lived","sensitivity":"personal","certainty":"high"},"content":"User name is Alexander","evidence":{"quote_or_span":"My name is Alexander"}}]}"#;
    assert!(
        parse_memory_post_turn_extractor_json(
            unknown_enum,
            &MemoryPostTurnExtractorConfig::default(),
        )
        .is_err()
    );

    let unknown_keys = r#"{"unexpected_top_level":"ignored","facts":[{"canonical_key":"user/global:identity:self:name","status":"active","confidence":0.0,"importance":0.0,"semantic":{"intent":"explicit_store","explicitness":"explicit","category":"identity","subject":"current_user","attribute":"name","scope_hint":"user_global","durability":"long_lived","sensitivity":"personal","certainty":"high"},"content":"User name is Alexander","evidence":{"quote_or_span":"My name is Alexander"}}]}"#;
    let parsed = parse_memory_post_turn_extractor_json(
        unknown_keys,
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("unknown keys are ignored at the LLM boundary");
    assert_eq!(parsed.raw_fact_count, 1);
    assert_eq!(parsed.facts.len(), 1);
    assert_eq!(parsed.facts[0].semantic.category, MemoryCategory::Identity);
    assert!(parsed.facts[0].confidence.expect("computed confidence") > 0.95);
}

#[test]
fn parser_rejects_secrets_transient_and_missing_evidence() {
    let secret = r#"{"facts":[{"semantic":{"intent":"implicit_candidate","explicitness":"implicit","category":"custom","subject":"current_user","attribute":"custom","custom_attribute":"api_key","scope_hint":"user_global","durability":"long_lived","sensitivity":"secret","certainty":"high"},"content":"User API key is sk-test","evidence":{"quote_or_span":"sk-test"}}]}"#;
    let parsed =
        parse_memory_post_turn_extractor_json(secret, &MemoryPostTurnExtractorConfig::default())
            .expect("secret JSON shape parses but fact is rejected");
    assert!(parsed.facts.is_empty());
    assert!(
        parsed
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("secret_or_regulated"))
    );

    let missing_evidence = r#"{"facts":[{"semantic":{"intent":"explicit_store","explicitness":"explicit","category":"identity","subject":"current_user","attribute":"name","scope_hint":"user_global","durability":"long_lived","sensitivity":"personal","certainty":"high"},"content":"User name is Alexander","evidence":{}}]}"#;
    let parsed = parse_memory_post_turn_extractor_json(
        missing_evidence,
        &MemoryPostTurnExtractorConfig::default(),
    )
    .expect("missing evidence JSON shape parses but fact is rejected");
    assert!(parsed.facts.is_empty());
    assert!(
        parsed
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("missing_evidence_quote"))
    );
}

#[tokio::test]
async fn post_turn_extractor_writes_semantic_fact_through_provider() {
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        valid_post_turn_extractor_json(),
    ));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    };

    let response = hook
        .execute(test_post_turn_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            "Меня зовут Александр",
            "Понял.",
        ))
        .await
        .expect("post-turn extractor executes");

    assert!(response.contributions.is_empty());
    assert_eq!(extractor_provider.call_count(), 1);
    assert_eq!(write_provider.manifest_call_count(), 1);
    assert_eq!(write_provider.write_call_count(), 1);
    let params = write_provider
        .write_params()
        .into_iter()
        .next()
        .expect("write params recorded");
    assert_eq!(
        params.disposition,
        Some(MemorySemanticWriteDisposition::RouteToCandidatePolicy)
    );
    assert_eq!(params.client_provided_key, None);
    assert_eq!(params.semantic.intent, MemoryIntent::ExplicitStore);
    assert_eq!(
        params.source_context_kind,
        Some(MemorySourceContextKind::DirectUserConversation)
    );
    assert_eq!(params.scope.kind, MemoryScopeKind::User);
    assert_eq!(params.scope.key, MEMORY_DEFAULT_USER_SCOPE_KEY);
    assert!(params.confidence.expect("computed confidence") > 0.95);
    assert!(params.importance.expect("computed importance") > 0.90);
    assert!(
        params
            .evidence
            .as_ref()
            .and_then(|e| e.source_thread_id.as_deref())
            .is_some()
    );
    assert!(params.metadata.contains_key("hook_id"));
    assert_eq!(
        params.metadata.get("model"),
        Some(&serde_json::json!("test-model"))
    );
    assert_eq!(
        params.metadata.get("model_provider"),
        Some(&serde_json::json!("test-provider"))
    );
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.completed"
            && diagnostic.message.as_str().contains("write_successes=1")
    }));
}

#[tokio::test]
async fn post_turn_extractor_uses_configured_model_override() {
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        valid_post_turn_extractor_json(),
    ));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig {
            provider_name: Some("memory-provider".to_owned()),
            model: Some("memory-model".to_owned()),
            ..MemoryPostTurnExtractorConfig::default()
        },
    };

    hook.execute(test_post_turn_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        "Меня зовут Александр",
        "Понял.",
    ))
    .await
    .expect("post-turn extractor executes");

    let context = extractor_provider
        .contexts()
        .into_iter()
        .next()
        .expect("extractor context recorded");
    assert_eq!(context.model.as_deref(), Some("memory-model"));
    assert_eq!(context.model_provider.as_deref(), Some("memory-provider"));

    let params = write_provider
        .write_params()
        .into_iter()
        .next()
        .expect("write params recorded");
    assert_eq!(
        params.metadata.get("model"),
        Some(&serde_json::json!("memory-model"))
    );
    assert_eq!(
        params.metadata.get("model_provider"),
        Some(&serde_json::json!("memory-provider"))
    );
}

#[tokio::test]
async fn post_turn_extractor_suppresses_weak_ontology_before_write() {
    let weak_json = serde_json::json!({
        "facts": [{
            "semantic": {
                "intent": "explicit_store",
                "explicitness": "explicit",
                "category": "identity",
                "subject": "current_user",
                "attribute": "name",
                "scope_hint": "user_global",
                "durability": "long_lived",
                "sensitivity": "personal",
                "certainty": "high"
            },
            "ontology": {
                "fact_class": "user_identity",
                "lifetime_class": "long_lived",
                "evidence_class": "missing_or_weak",
                "proposed_ownership_class": "durable_user_memory"
            },
            "content": "Имя пользователя: Александр",
            "value": "Александр",
            "evidence": {
                "source_ref": "turn.post_turn:user",
                "quote_or_span": "Меня зовут Александр",
                "extractor_reason": "The user directly stated their name."
            }
        }]
    });
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(weak_json.to_string()));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    };

    let response = hook
        .execute(test_post_turn_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            "Меня зовут Александр",
            "Понял.",
        ))
        .await
        .expect("post-turn extractor executes");

    assert_eq!(extractor_provider.call_count(), 1);
    assert_eq!(write_provider.write_call_count(), 0);
    assert!(
        response
            .diagnostics
            .iter()
            .any(|diagnostic| { diagnostic.message.as_str().contains("weak_evidence_class") })
    );
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.completed"
            && diagnostic
                .message
                .as_str()
                .contains("validation_rejected=1")
            && diagnostic.message.as_str().contains("write_attempts=0")
    }));
}

#[tokio::test]
async fn post_turn_extractor_provider_failure_is_retryable_hook_failure() {
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestFailingPostTurnExtractorProvider::default());
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    };

    let error = hook
        .execute(test_post_turn_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            "Меня зовут Александр",
            "Понял.",
        ))
        .await
        .expect_err("provider failure should be visible to hook runtime");

    assert_eq!(
        error.code.as_str(),
        "memory.post_turn_extractor.provider_failed"
    );
    assert!(error.retryable);
    assert!(error.safe_for_user);
    assert_eq!(extractor_provider.call_count(), 1);
    assert_eq!(write_provider.write_call_count(), 0);
}

#[tokio::test]
async fn post_turn_extractor_preserves_manifest_and_observes_auto_approve() {
    let mut auto_response = test_semantic_write_response();
    auto_response.record = Some(test_memory_record("mem_auto"));
    auto_response.created = true;
    let write_provider = Arc::new(TestMemoryWriteProvider {
        response: Some(auto_response),
        ..TestMemoryWriteProvider::default()
    });
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        valid_post_turn_extractor_json(),
    ));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    };

    let response = hook
        .execute(test_post_turn_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            "Меня зовут Александр",
            "Понял.",
        ))
        .await
        .expect("post-turn extractor executes");

    let prompt = extractor_provider
        .prompts()
        .into_iter()
        .next()
        .expect("extractor prompt recorded");
    assert!(prompt.contains("Memory manifest:"));
    assert!(prompt.contains("Do not generate canonical memory keys"));
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.completed"
            && diagnostic.message.as_str().contains("auto_approved=1")
    }));
}

#[tokio::test]
async fn post_turn_extractor_skips_non_success_turns() {
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        valid_post_turn_extractor_json(),
    ));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    };
    let mut request = test_post_turn_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        "Меня зовут Александр",
        "Понял.",
    );
    request.input = HookInput::turn_post_turn(TurnPostTurnHookInput::from_parts_with_model(
        TurnPostTurnStatus::ProviderFailure,
        Some("test-model"),
        Some("test-provider"),
        Some("Меня зовут Александр"),
        None::<&str>,
        Some("provider failed"),
        Vec::new(),
        Vec::new(),
        pioneer_hooks::TurnPostTurnHookInputLimits::default(),
    ));

    let response = hook
        .execute(request)
        .await
        .expect("non-success post-turn hook is best-effort");
    assert_eq!(extractor_provider.call_count(), 0);
    assert_eq!(write_provider.write_call_count(), 0);
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_eligibility.non_success_status"
            && diagnostic.metadata.get(&hook_metadata_key("skip_reason"))
                == Some(&HookValue::Text("non_success_status".to_owned()))
            && diagnostic.metadata.get(&hook_metadata_key("turn_status"))
                == Some(&HookValue::Text("provider_failure".to_owned()))
    }));
}

#[tokio::test]
async fn post_turn_extractor_respects_policy_and_provider_availability() {
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        valid_post_turn_extractor_json(),
    ));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    };

    let no_save = hook
        .execute(test_post_turn_hook_request(
            memory_policy_set(&MemoryTurnPolicy::no_save()),
            "Меня зовут Александр",
            "Понял.",
        ))
        .await
        .expect("no-save policy is best-effort");
    assert!(no_save.contributions.is_empty());
    assert_eq!(extractor_provider.call_count(), 0);
    assert_eq!(write_provider.write_call_count(), 0);
    assert!(no_save.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_eligibility.policy_disabled"
            && diagnostic.metadata.get(&hook_metadata_key("skip_reason"))
                == Some(&HookValue::Text("policy_disabled".to_owned()))
            && diagnostic.metadata.get(&hook_metadata_key("policy_source"))
                == Some(&HookValue::Text("pre_memory_classifier".to_owned()))
            && diagnostic
                .metadata
                .get(&hook_metadata_key("policy_reason_code"))
                == Some(&HookValue::Text("memory_no_save".to_owned()))
    }));

    let provider_disabled = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig {
            provider_enabled: false,
            ..MemoryPostTurnExtractorConfig::default()
        },
    }
    .execute(test_post_turn_hook_request(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        "Меня зовут Александр",
        "Понял.",
    ))
    .await
    .expect("provider-disabled policy is best-effort");
    assert!(provider_disabled.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.skipped"
            && diagnostic.metadata.get(&hook_metadata_key("skip_reason"))
                == Some(&HookValue::Text("provider_disabled".to_owned()))
    }));
}

#[tokio::test]
async fn post_turn_extractor_runs_for_direct_user_turns_with_runtime_events() {
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        valid_post_turn_extractor_json(),
    ));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    };

    let response = hook
        .execute(test_post_turn_hook_request_with_events(
            memory_policy_set(&MemoryTurnPolicy::permissive_classifier_fallback(
                MemoryPolicyReasonCode::ClassifierUnavailable,
            )),
            Some("direct user transcript"),
            Some("assistant response"),
            vec![test_post_turn_tool_event()],
            vec![test_post_turn_domain_event(
                pioneer_hooks::TurnPostTurnDomain::Custom("runtime".to_owned()),
            )],
        ))
        .await
        .expect("direct user turn with runtime events remains eligible");

    assert!(response.contributions.is_empty());
    assert_eq!(write_provider.manifest_call_count(), 1);
    assert_eq!(extractor_provider.call_count(), 1);
    assert_eq!(write_provider.write_call_count(), 1);
    assert_eq!(
        write_provider
            .write_params()
            .first()
            .and_then(|params| params.source_context_kind),
        Some(MemorySourceContextKind::DirectUserConversation)
    );
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.completed"
            && diagnostic.message.as_str().contains("write_attempts=1")
    }));
}

#[tokio::test]
async fn post_turn_extractor_skips_ineligible_source_classes_before_provider_calls() {
    let mut task_owned = test_post_turn_hook_request_with_events(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        Some("direct user transcript"),
        Some("assistant response"),
        Vec::new(),
        Vec::new(),
    );
    task_owned.context.task_id =
        Some(pioneer_hooks::HookTaskId::new("task-1").expect("valid task id"));

    let mut system_owned = test_post_turn_hook_request_with_events(
        memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
        Some("direct user transcript"),
        Some("assistant response"),
        Vec::new(),
        Vec::new(),
    );
    system_owned.context.mode = Some(pioneer_hooks::HookContextMode::System);

    let cases = vec![
        (
            "missing_policy",
            test_post_turn_hook_request_with_events(
                HookPolicySet::empty(),
                Some("direct user transcript"),
                Some("assistant response"),
                Vec::new(),
                Vec::new(),
            ),
            MemoryPostTurnEligibilitySkipReason::MissingPolicy,
        ),
        (
            "no_use_policy",
            test_post_turn_hook_request_with_events(
                memory_policy_set(&MemoryTurnPolicy::no_use()),
                Some("direct user transcript"),
                Some("assistant response"),
                Vec::new(),
                Vec::new(),
            ),
            MemoryPostTurnEligibilitySkipReason::PolicyDisabledExtraction,
        ),
        (
            "no_save_policy",
            test_post_turn_hook_request_with_events(
                memory_policy_set(&MemoryTurnPolicy::no_save()),
                Some("direct user transcript"),
                Some("assistant response"),
                Vec::new(),
                Vec::new(),
            ),
            MemoryPostTurnEligibilitySkipReason::PolicyDisabledExtraction,
        ),
        (
            "empty_transcript",
            test_post_turn_hook_request_with_events(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                None,
                None,
                Vec::new(),
                Vec::new(),
            ),
            MemoryPostTurnEligibilitySkipReason::NoTranscript,
        ),
        (
            "assistant_only",
            test_post_turn_hook_request_with_events(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                None,
                Some("assistant response"),
                Vec::new(),
                Vec::new(),
            ),
            MemoryPostTurnEligibilitySkipReason::NoDirectUserSource,
        ),
        (
            "tool_only",
            test_post_turn_hook_request_with_events(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                None,
                None,
                vec![test_post_turn_tool_event()],
                Vec::new(),
            ),
            MemoryPostTurnEligibilitySkipReason::SystemOrToolOnlySource,
        ),
        (
            "domain_only",
            test_post_turn_hook_request_with_events(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                None,
                None,
                Vec::new(),
                vec![test_post_turn_domain_event(
                    pioneer_hooks::TurnPostTurnDomain::Memory,
                )],
            ),
            MemoryPostTurnEligibilitySkipReason::SystemOrToolOnlySource,
        ),
        (
            "task_owned",
            task_owned,
            MemoryPostTurnEligibilitySkipReason::TaskRuntimeOwnedSource,
        ),
        (
            "system_owned",
            system_owned,
            MemoryPostTurnEligibilitySkipReason::SystemOrToolOnlySource,
        ),
    ];

    for (label, request, reason) in cases {
        let write_provider = Arc::new(TestMemoryWriteProvider::default());
        let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
            valid_post_turn_extractor_json(),
        ));
        let hook = MemoryPostTurnExtractorHook {
            write_provider: Some(write_provider.clone()),
            extractor_provider: Some(extractor_provider.clone()),
            config: MemoryPostTurnExtractorConfig::default(),
        };

        let response = hook
            .execute(request)
            .await
            .unwrap_or_else(|error| panic!("{label}: hook should skip cleanly: {error}"));

        assert!(response.contributions.is_empty(), "{label}");
        assert_eq!(write_provider.manifest_call_count(), 0, "{label}");
        assert_eq!(extractor_provider.call_count(), 0, "{label}");
        assert_eq!(write_provider.write_call_count(), 0, "{label}");
        assert_post_turn_eligibility_skip(&response, reason);
    }
}

#[tokio::test]
async fn permissive_fallback_does_not_override_typed_source_ineligibility() {
    let fallback_policy = MemoryTurnPolicy::permissive_classifier_fallback(
        MemoryPolicyReasonCode::ClassifierUnavailable,
    );
    let mut task_owned = test_post_turn_hook_request_with_events(
        memory_policy_set(&fallback_policy),
        Some("direct user transcript"),
        Some("assistant response"),
        Vec::new(),
        Vec::new(),
    );
    task_owned.context.task_id =
        Some(pioneer_hooks::HookTaskId::new("task-1").expect("valid task id"));

    let cases = vec![
        (
            "tool_only",
            test_post_turn_hook_request_with_events(
                memory_policy_set(&fallback_policy),
                None,
                None,
                vec![test_post_turn_tool_event()],
                Vec::new(),
            ),
            MemoryPostTurnEligibilitySkipReason::SystemOrToolOnlySource,
        ),
        (
            "domain_only",
            test_post_turn_hook_request_with_events(
                memory_policy_set(&fallback_policy),
                None,
                None,
                Vec::new(),
                vec![test_post_turn_domain_event(
                    pioneer_hooks::TurnPostTurnDomain::Memory,
                )],
            ),
            MemoryPostTurnEligibilitySkipReason::SystemOrToolOnlySource,
        ),
        (
            "task_owned",
            task_owned,
            MemoryPostTurnEligibilitySkipReason::TaskRuntimeOwnedSource,
        ),
    ];

    for (label, request, reason) in cases {
        let write_provider = Arc::new(TestMemoryWriteProvider::default());
        let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
            valid_post_turn_extractor_json(),
        ));
        let hook = MemoryPostTurnExtractorHook {
            write_provider: Some(write_provider.clone()),
            extractor_provider: Some(extractor_provider.clone()),
            config: MemoryPostTurnExtractorConfig::default(),
        };

        let response = hook
            .execute(request)
            .await
            .unwrap_or_else(|error| panic!("{label}: hook should skip cleanly: {error}"));

        assert!(response.contributions.is_empty(), "{label}");
        assert_eq!(write_provider.manifest_call_count(), 0, "{label}");
        assert_eq!(extractor_provider.call_count(), 0, "{label}");
        assert_eq!(write_provider.write_call_count(), 0, "{label}");
        assert_post_turn_eligibility_skip(&response, reason);
    }
}

#[tokio::test]
async fn post_turn_extractor_runs_under_permissive_classifier_fallback() {
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        valid_post_turn_extractor_json(),
    ));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider.clone()),
        config: MemoryPostTurnExtractorConfig::default(),
    };
    let policy = MemoryTurnPolicy::permissive_classifier_fallback(
        MemoryPolicyReasonCode::ClassifierUnavailable,
    );

    let response = hook
        .execute(test_post_turn_hook_request(
            memory_policy_set(&policy),
            "Меня зовут Александр",
            "Понял.",
        ))
        .await
        .expect("classifier fallback extraction executes");

    assert!(response.contributions.is_empty());
    assert_eq!(write_provider.manifest_call_count(), 1);
    assert_eq!(extractor_provider.call_count(), 1);
    assert_eq!(write_provider.write_call_count(), 1);
    assert_eq!(write_provider.write_params().len(), 1);
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.completed"
            && diagnostic.message.as_str().contains("write_attempts=1")
            && diagnostic.message.as_str().contains("write_successes=1")
    }));
    let params = write_provider
        .write_params()
        .into_iter()
        .next()
        .expect("write params recorded");
    assert_eq!(
        params.disposition,
        Some(MemorySemanticWriteDisposition::RouteToCandidatePolicy)
    );
    assert_eq!(
        params.metadata.get("policy_source"),
        Some(&serde_json::json!("default_fallback"))
    );
    assert_eq!(
        params.metadata.get("policy_reason_code"),
        Some(&serde_json::json!("classifier_unavailable"))
    );
    assert_eq!(
        params.source_context_kind,
        Some(MemorySourceContextKind::DirectUserConversation)
    );
}

#[tokio::test]
async fn post_turn_extractor_suppresses_implicit_when_proactive_disabled() {
    let write_provider = Arc::new(TestMemoryWriteProvider::default());
    let extractor_provider = Arc::new(TestPostTurnExtractorProvider::json(
        implicit_post_turn_extractor_json(),
    ));
    let hook = MemoryPostTurnExtractorHook {
        write_provider: Some(write_provider.clone()),
        extractor_provider: Some(extractor_provider),
        config: MemoryPostTurnExtractorConfig {
            proactive_writes_enabled: false,
            ..MemoryPostTurnExtractorConfig::default()
        },
    };

    let response = hook
        .execute(test_post_turn_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            "Мне нравится лаконичный стиль ответов.",
            "Ок.",
        ))
        .await
        .expect("post-turn extractor executes");

    assert_eq!(write_provider.write_call_count(), 0);
    assert!(response.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.as_str() == "memory.post_turn_extractor.completed"
            && diagnostic
                .message
                .as_str()
                .contains("validation_rejected=1")
    }));
}
