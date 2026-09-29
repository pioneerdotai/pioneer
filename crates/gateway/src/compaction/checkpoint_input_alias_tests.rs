//! Regression: a child summary covers B, but the next view retains A with B
//! as an exact input alias. The published checkpoint itself predates that alias.
use super::*;
use crate::compaction::{checkpoint, coverage, frozen, history};
use pioneer_provider::{
    ChatMessage, MessageProvenance, MessageSourceAlias, MessageSourceIdentity, MessageSourceRef,
};
use std::collections::BTreeSet;

struct Scenario {
    f: Fixture,
    checkpoint: Checkpoint,
    allowed: BTreeSet<String>,
    original: ChatMessage,
    work: ChatMessage,
    tail: ChatMessage,
    copy: MessageSourceRef,
}

async fn scenario() -> Scenario {
    let f = fixture("covered work", vec![], true, false).await;
    insert_projection_event(&f, "copy-thread").await;
    let work_payload =
        serde_json::to_string(&pioneer_crud::CanonicalTurnEventPayload::ItemCompleted(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "copy-thread".into(),
                turn_id: "copy-thread-turn".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: "covered-work-item".into(),
                    text: "covered work".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
        ))
        .unwrap();
    f.store.database_connection().execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE turn_event SET event_type='item/completed',payload=? WHERE id='copy-thread-source'",
        [work_payload.into()],
    )).await.unwrap();
    for sql in [
        "UPDATE turn SET status='completed' WHERE id='turn'",
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('original','turn',0,'text','same question','{\"type\":\"text\",\"text\":\"same question\"}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('copy','copy-thread-turn',0,'text','same question','{\"type\":\"text\",\"text\":\"same question\"}',CURRENT_TIMESTAMP)",
    ] {
        f.store
            .database_connection()
            .execute_unprepared(sql)
            .await
            .unwrap();
    }
    for thread in ["thread", "copy-thread"] {
        history::prepare_history(&f.store, "ws", thread)
            .await
            .unwrap();
    }
    let source = |source: &SourceRef| MessageSourceRef {
        scope: source.scope.clone(),
        id: source.id.clone(),
        version: source.version.clone(),
    };
    let a = f
        .store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Input, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let b = f
        .store
        .compaction_source_page(
            "ws",
            "copy-thread",
            "copy-thread-turn",
            PagedSource::Input,
            0,
        )
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let work_source = f
        .store
        .compaction_source_page(
            "ws",
            "copy-thread",
            "copy-thread-turn",
            PagedSource::Event,
            0,
        )
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let checkpoint = publish_projection_checkpoint(
        &f,
        "copy-thread",
        "copy-summary",
        &[
            ("copy-thread".into(), b.clone()),
            ("copy-thread".into(), work_source.clone()),
        ],
        pioneer_compaction::CoverageDomain::WorkingContext,
    )
    .await;
    let origin = |thread: &str, unit: &str, reference: MessageSourceRef| MessageProvenance {
        workspace_id: "ws".into(),
        thread_id: thread.into(),
        context_thread: Some("thread".into()),
        unit_id: unit.into(),
        logical_turn_id: None,
        sources: vec![reference],
        source_aliases: vec![],
        ambiguous_input_aliases: vec![],
        complete: true,
        protected_input: false,
        inherited: true,
    };
    let mut original = ChatMessage::user("same question");
    original.provenance = Some(origin("thread", "original-input", source(&a)));
    original
        .provenance
        .as_mut()
        .unwrap()
        .source_aliases
        .push(MessageSourceAlias {
            represented_thread_id: "thread".into(),
            represented_source: source(&a),
            thread_id: "copy-thread".into(),
            source: source(&b),
        });
    let mut work = ChatMessage::assistant("covered work");
    work.provenance = Some(origin("copy-thread", "covered-work", source(&work_source)));
    let tail = ChatMessage::user("new work after the checkpoint");
    Scenario {
        f,
        checkpoint,
        allowed: BTreeSet::from(["thread".into(), "copy-thread".into()]),
        original,
        work,
        tail,
        copy: source(&b),
    }
}

async fn project(s: &Scenario, messages: &mut Vec<ChatMessage>) -> anyhow::Result<()> {
    checkpoint::project_checkpoint_with_resolver(
        &s.f.store,
        checkpoint::ProjectionContext {
            workspace: "ws",
            context_thread: "thread",
            source_thread: "copy-thread",
            owner: &s.checkpoint.owner,
            allowed: &s.allowed,
            allow_historical_gaps: false,
        },
        &s.checkpoint.id,
        messages,
        &mut coverage::CheckpointGraphResolver::default(),
    )
    .await
}

#[tokio::test]
async fn checkpoint_covering_removed_input_copy_reuses_summary_and_preserves_tail_and_proof() {
    let s = scenario().await;
    let mut messages = vec![s.original.clone(), s.work.clone(), s.tail.clone()];
    project(&s, &mut messages).await.unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[1], s.tail);
    assert_eq!(
        messages[0].content,
        format!(
            "Summary of completed work (historical data):\n{}",
            s.checkpoint.summary
        )
    );
    let proof = &messages[0].provenance.as_ref().unwrap().source_aliases;
    assert_eq!(proof.len(), 1);
    assert_eq!(proof[0].represented_thread_id, "copy-thread");
    assert_eq!(proof[0].represented_source, s.copy);
    assert_eq!(proof[0].thread_id, "thread");
    assert_eq!(
        proof[0].source,
        s.original.provenance.as_ref().unwrap().sources[0]
    );

    let descriptor = frozen::capture(&s.f.store, "ws", "thread", &s.allowed, &messages[..1])
        .await
        .unwrap();
    let restored = frozen::restore(&s.f.store, "ws", &s.allowed, &descriptor)
        .await
        .unwrap();
    assert_eq!(restored, messages[..1]);
    let mut replay = restored.clone();
    let mut original_without_alias = s.original.clone();
    original_without_alias
        .provenance
        .as_mut()
        .unwrap()
        .source_aliases
        .clear();
    replay.push(original_without_alias);
    history::normalize_task_input_copies(&s.f.store, "ws", &mut replay)
        .await
        .unwrap();
    assert_eq!(replay, restored);
    project(&s, &mut messages).await.unwrap();
    assert_eq!(messages[0], restored[0]);
    assert_eq!(messages[1], s.tail);
    assert!(s.f.provider.calls.lock().unwrap().is_empty());
    let saved =
        s.f.store
            .compaction_checkpoint(&s.checkpoint.id)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(saved.summary, s.checkpoint.summary);
    assert_eq!(
        saved.coverage.into_iter().collect::<BTreeSet<_>>(),
        s.checkpoint
            .coverage
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
    );

    // A later frozen branch can still carry the old A -> B representation.
    // Re-projecting must consume it without losing the summary's inverse proof.
    let mut joined = restored.clone();
    joined.push(s.original.clone());
    joined.push(s.tail.clone());
    project(&s, &mut joined).await.unwrap();
    assert_eq!(joined, messages);
}

#[tokio::test]
async fn input_copy_projection_requires_exact_unambiguous_complete_unprotected_proof() {
    let s = scenario().await;
    for case in [
        "missing-proof",
        "copy-revision",
        "original-revision",
        "ambiguous-copy",
        "ambiguous-original",
        "protected",
        "incomplete",
        "missing-work",
        "competing-owner",
        "split-round",
        "system-message",
    ] {
        let mut original = s.original.clone();
        let origin = original.provenance.as_mut().unwrap();
        match case {
            "missing-proof" => origin.source_aliases.clear(),
            "copy-revision" => origin.source_aliases[0].source.version = "input-revision:2".into(),
            "original-revision" => origin.sources[0].version = "input-revision:2".into(),
            "ambiguous-copy" => origin.ambiguous_input_aliases.push(MessageSourceIdentity {
                thread_id: "copy-thread".into(),
                source: s.copy.clone(),
            }),
            "ambiguous-original" => origin.ambiguous_input_aliases.push(MessageSourceIdentity {
                thread_id: "thread".into(),
                source: origin.sources[0].clone(),
            }),
            "protected" => origin.protected_input = true,
            "incomplete" => origin.complete = false,
            _ => {}
        }
        let mut messages = vec![original];
        if case == "system-message" {
            messages[0].role = pioneer_provider::Role::System;
        }
        if case != "missing-work" {
            messages.push(s.work.clone());
        }
        if case == "competing-owner" {
            let mut competing = s.original.clone();
            let origin = competing.provenance.as_mut().unwrap();
            origin.sources[0].id = "independent-input".into();
            origin.source_aliases[0].represented_source = origin.sources[0].clone();
            messages.push(competing);
        }
        if case == "split-round" {
            let mut sibling = s.original.clone();
            let origin = sibling.provenance.as_mut().unwrap();
            origin.source_aliases.clear();
            origin.sources[0].id = "uncovered-round-sibling".into();
            messages.push(sibling);
        }
        messages.push(s.tail.clone());
        let before = messages.clone();
        assert!(project(&s, &mut messages).await.is_err(), "{case}");
        assert_eq!(
            messages, before,
            "failed projection mutated history: {case}"
        );
    }
}

#[tokio::test]
async fn alias_does_not_admit_checkpoint_beyond_the_accepted_boundary() {
    let s = scenario().await;
    let mut messages = vec![s.original.clone(), s.work.clone()];
    let boundary = vec![s.work.clone()];
    let before = messages.clone();
    let result = checkpoint::project_checkpoint_in_context_with_boundary(
        &s.f.store,
        &checkpoint::ProjectionContext {
            workspace: "ws",
            context_thread: "thread",
            source_thread: "copy-thread",
            owner: &s.checkpoint.owner,
            allowed: &s.allowed,
            allow_historical_gaps: false,
        },
        &s.checkpoint.id,
        &mut messages,
        Some(&checkpoint::ProjectionBoundaryEvidence {
            messages: &boundary,
            model_ordinals: &[0],
        }),
        &mut coverage::CheckpointGraphResolver::default(),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(messages, before);
}

#[tokio::test]
async fn accepted_projection_reuses_copy_checkpoint_after_parent_input_normalization() {
    let s = scenario().await;
    let mut original = s.original.clone();
    original.provenance.as_mut().unwrap().context_thread = Some("next-child".into());
    let mut work = s.work.clone();
    work.provenance.as_mut().unwrap().context_thread = Some("next-child".into());
    let mut messages = vec![original, work, s.tail.clone()];
    checkpoint::project_accepted_checkpoints(
        &s.f.store,
        "ws",
        "next-child",
        &s.allowed,
        &mut messages,
    )
    .await
    .unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(
        messages[0].content,
        format!(
            "Summary of completed work (historical data):\n{}",
            s.checkpoint.summary
        )
    );
    assert_eq!(messages[1], s.tail);
}

#[tokio::test]
async fn accepted_projection_preserves_competing_inputs_in_either_order() {
    let s = scenario().await;
    let mut competing = s.original.clone();
    let origin = competing.provenance.as_mut().unwrap();
    origin.sources[0].id = "independent-input".into();
    origin.source_aliases[0].represented_source = origin.sources[0].clone();
    for reverse in [false, true] {
        let mut messages = vec![
            s.original.clone(),
            competing.clone(),
            s.work.clone(),
            s.tail.clone(),
        ];
        if reverse {
            messages.swap(0, 1);
        }
        let before = messages.clone();
        checkpoint::project_accepted_checkpoints(
            &s.f.store,
            "ws",
            "next-child",
            &s.allowed,
            &mut messages,
        )
        .await
        .unwrap();
        assert_eq!(messages, before);
    }
}

#[tokio::test]
async fn replacing_input_preserves_its_other_saved_copies_without_expanding_coverage() {
    let s = scenario().await;
    let mut original = s.original.clone();
    let origin = original.provenance.as_mut().unwrap();
    let mut other_copy = origin.source_aliases[0].clone();
    other_copy.thread_id = "another-copy-thread".into();
    other_copy.source.scope = "input:another-turn".into();
    other_copy.source.id = "another-copy".into();
    origin.source_aliases.push(other_copy.clone());
    let mut messages = vec![original, s.work.clone(), s.tail.clone()];
    project(&s, &mut messages).await.unwrap();
    assert_eq!(messages.len(), 2);
    let origin = messages[0].provenance.as_ref().unwrap();
    assert_eq!(origin.sources.len(), 1);
    assert_eq!(origin.sources[0].id, s.checkpoint.id);
    assert_eq!(origin.source_aliases.len(), 2);
    assert!(
        origin
            .source_aliases
            .iter()
            .all(|alias| alias.represented_thread_id == "copy-thread"
                && alias.represented_source == s.copy)
    );
    assert!(
        origin
            .source_aliases
            .iter()
            .any(|alias| alias.source == other_copy.source)
    );
    assert_eq!(messages[1], s.tail);
}

#[tokio::test]
async fn overlapping_original_and_task_copy_summaries_keep_the_covering_checkpoint() {
    let s = scenario().await;
    let source = &s.original.provenance.as_ref().unwrap().sources[0];
    let original_source = SourceRef {
        scope: source.scope.clone(),
        id: source.id.clone(),
        version: source.version.clone(),
    };
    let original_checkpoint = publish_projection_checkpoint(
        &s.f,
        "thread",
        "original-summary",
        &[("thread".into(), original_source)],
        pioneer_compaction::CoverageDomain::WorkingContext,
    )
    .await;
    let original_context = || checkpoint::ProjectionContext {
        workspace: "ws",
        context_thread: "thread",
        source_thread: "thread",
        owner: &original_checkpoint.owner,
        allowed: &s.allowed,
        allow_historical_gaps: false,
    };
    for reverse in [false, true] {
        let mut messages = vec![s.original.clone(), s.work.clone(), s.tail.clone()];
        let mut old_frozen = None;
        if reverse {
            project(&s, &mut messages).await.unwrap();
            checkpoint::project_checkpoint_with_resolver(
                &s.f.store,
                original_context(),
                &original_checkpoint.id,
                &mut messages,
                &mut coverage::CheckpointGraphResolver::default(),
            )
            .await
            .unwrap();
        } else {
            checkpoint::project_checkpoint_with_resolver(
                &s.f.store,
                original_context(),
                &original_checkpoint.id,
                &mut messages,
                &mut coverage::CheckpointGraphResolver::default(),
            )
            .await
            .unwrap();
            old_frozen = Some((
                frozen::capture(&s.f.store, "ws", "thread", &s.allowed, &messages[..1])
                    .await
                    .unwrap(),
                messages[0].clone(),
            ));
            let accepted_basis =
                frozen::capture(&s.f.store, "ws", "thread", &s.allowed, &messages[..2])
                    .await
                    .unwrap();
            let recovered = frozen::restore_accepted_history_for_execution(
                &s.f.store,
                "ws",
                Some("thread"),
                "thread",
                &s.allowed,
                &serde_json::to_string(&accepted_basis).unwrap(),
            )
            .await
            .unwrap()
            .messages;
            assert_eq!(recovered.len(), 1);
            assert_eq!(
                recovered[0].provenance.as_ref().unwrap().sources[0].id,
                s.checkpoint.id
            );
            project(&s, &mut messages).await.unwrap();
        }
        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages[0].provenance.as_ref().unwrap().sources[0].id,
            s.checkpoint.id
        );
        assert_eq!(messages[1], s.tail);
        let saved_original =
            s.f.store
                .compaction_checkpoint(&original_checkpoint.id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(saved_original.coverage.len(), 1);
        if let Some((descriptor, old_summary)) = old_frozen {
            assert_eq!(
                frozen::restore(&s.f.store, "ws", &s.allowed, &descriptor)
                    .await
                    .unwrap(),
                vec![old_summary]
            );
        }
        let original_ref =
            s.f.store
                .compaction_checkpoint_source("ws", "thread", &original_checkpoint.id)
                .await
                .unwrap()
                .unwrap();
        assert!(
            coverage::CheckpointGraphResolver::default()
                .resolve(&s.f.store, "ws", Some(&s.allowed), &original_ref)
                .await
                .unwrap()
                .is_some()
        );
        let frozen_summary =
            frozen::capture(&s.f.store, "ws", "thread", &s.allowed, &messages[..1])
                .await
                .unwrap();
        assert_eq!(
            frozen::restore(&s.f.store, "ws", &s.allowed, &frozen_summary)
                .await
                .unwrap(),
            messages[..1]
        );
        let once = messages.clone();
        project(&s, &mut messages).await.unwrap();
        assert_eq!(messages, once);
    }
    assert!(s.f.provider.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn absorbed_alias_conflict_survives_gateway_capture_and_restore() {
    use pioneer_agent::compaction::composition::{AcceptedContextBranch, ScopedHistorySource};
    use pioneer_compaction::CoverageDomain;
    use std::collections::BTreeMap;

    let s = scenario().await;
    for thread in ["copy-c-thread", "conflict-thread", "cover-thread"] {
        insert_projection_event(&s.f, thread).await;
    }
    for (thread, id) in [("copy-c-thread", "copy-c"), ("conflict-thread", "input-x")] {
        s.f.store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES (?,?,0,'text','same question','{\"type\":\"text\",\"text\":\"same question\"}',CURRENT_TIMESTAMP)",
                [id.into(), format!("{thread}-turn").into()],
            ))
            .await
            .unwrap();
        history::prepare_history(&s.f.store, "ws", thread)
            .await
            .unwrap();
    }
    let a = &s.original.provenance.as_ref().unwrap().sources[0];
    let a = SourceRef {
        scope: a.scope.clone(),
        id: a.id.clone(),
        version: a.version.clone(),
    };
    let b = SourceRef {
        scope: s.copy.scope.clone(),
        id: s.copy.id.clone(),
        version: s.copy.version.clone(),
    };
    let c =
        s.f.store
            .compaction_source_page(
                "ws",
                "copy-c-thread",
                "copy-c-thread-turn",
                PagedSource::Input,
                0,
            )
            .await
            .unwrap()
            .entries[0]
            .reference
            .clone();
    let x =
        s.f.store
            .compaction_source_page(
                "ws",
                "conflict-thread",
                "conflict-thread-turn",
                PagedSource::Input,
                0,
            )
            .await
            .unwrap()
            .entries[0]
            .reference
            .clone();
    assert_eq!(a.version, b.version);
    assert_eq!(a.version, c.version);
    assert_eq!(a.version, x.version);
    let old = publish_projection_checkpoint(
        &s.f,
        "thread",
        "summary-conflict-a",
        &[("thread".into(), a.clone())],
        CoverageDomain::WorkingContext,
    )
    .await;
    let covering = publish_projection_checkpoint(
        &s.f,
        "cover-thread",
        "summary-conflict-bc",
        &[
            ("copy-thread".into(), b),
            ("copy-c-thread".into(), c.clone()),
        ],
        CoverageDomain::WorkingContext,
    )
    .await;
    let competing = publish_projection_checkpoint(
        &s.f,
        "conflict-thread",
        "summary-conflict-x",
        &[("conflict-thread".into(), x.clone())],
        CoverageDomain::WorkingContext,
    )
    .await;
    let mut allowed = s.allowed.clone();
    allowed.extend(
        ["copy-c-thread", "conflict-thread", "cover-thread"]
            .into_iter()
            .map(str::to_owned),
    );
    let mut messages = Vec::new();
    let mut closures = BTreeMap::new();
    for (thread, checkpoint) in [
        ("thread", &old),
        ("cover-thread", &covering),
        ("conflict-thread", &competing),
    ] {
        let mut resolver = coverage::CheckpointGraphResolver::default();
        let message = checkpoint::checkpoint_message_with_resolver(
            &s.f.store,
            checkpoint::ProjectionContext {
                workspace: "ws",
                context_thread: "thread",
                source_thread: thread,
                owner: &checkpoint.owner,
                allowed: &allowed,
                allow_historical_gaps: true,
            },
            &checkpoint.id,
            &mut resolver,
        )
        .await
        .unwrap();
        let source =
            s.f.store
                .compaction_checkpoint_source("ws", thread, &checkpoint.id)
                .await
                .unwrap()
                .unwrap();
        let graph = resolver
            .resolve(&s.f.store, "ws", Some(&allowed), &source)
            .await
            .unwrap()
            .unwrap();
        closures.insert(
            ScopedHistorySource {
                thread: thread.into(),
                source,
            },
            graph.leaves.clone(),
        );
        messages.push(message);
    }
    let runtime_source = |source: &SourceRef| MessageSourceRef {
        scope: source.scope.clone(),
        id: source.id.clone(),
        version: source.version.clone(),
    };
    let c_source = runtime_source(&c);
    for (copy_thread, copy) in [("copy-thread", &s.copy), ("copy-c-thread", &c_source)] {
        messages[0]
            .provenance
            .as_mut()
            .unwrap()
            .source_aliases
            .push(MessageSourceAlias {
                represented_thread_id: "thread".into(),
                represented_source: runtime_source(&a),
                thread_id: copy_thread.into(),
                source: copy.clone(),
            });
    }
    messages[2]
        .provenance
        .as_mut()
        .unwrap()
        .source_aliases
        .push(MessageSourceAlias {
            represented_thread_id: "conflict-thread".into(),
            represented_source: runtime_source(&x),
            thread_id: "copy-thread".into(),
            source: s.copy.clone(),
        });
    let domains = closures
        .keys()
        .cloned()
        .map(|source| (source, CoverageDomain::WorkingContext))
        .collect::<BTreeMap<_, _>>();
    let select = |messages: &[ChatMessage]| {
        pioneer_agent::compaction::composition::compose_context(
            "ws",
            "child",
            &[AcceptedContextBranch {
                thread: "thread",
                messages,
                checkpoints: &closures,
                checkpoint_domains: &domains,
            }],
        )
        .unwrap()
    };
    let selected = select(&messages);
    let ids = |messages: &[ChatMessage]| {
        messages
            .iter()
            .map(|message| message.provenance.as_ref().unwrap().sources[0].id.clone())
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(
        ids(&selected),
        BTreeSet::from([covering.id.clone(), competing.id.clone()])
    );
    let conflict = MessageSourceIdentity {
        thread_id: "copy-thread".into(),
        source: s.copy.clone(),
    };
    assert!(selected.iter().any(|message| {
        message
            .provenance
            .as_ref()
            .unwrap()
            .ambiguous_input_aliases
            .contains(&conflict)
    }));
    let mut projected = vec![messages[0].clone(), messages[2].clone()];
    checkpoint::project_checkpoint_with_resolver(
        &s.f.store,
        checkpoint::ProjectionContext {
            workspace: "ws",
            context_thread: "thread",
            source_thread: "cover-thread",
            owner: &covering.owner,
            allowed: &allowed,
            allow_historical_gaps: true,
        },
        &covering.id,
        &mut projected,
        &mut coverage::CheckpointGraphResolver::default(),
    )
    .await
    .unwrap();
    assert_eq!(ids(&projected), ids(&selected));
    assert!(projected.iter().any(|message| {
        message
            .provenance
            .as_ref()
            .unwrap()
            .ambiguous_input_aliases
            .contains(&conflict)
    }));
    let mut suppressed = messages.clone();
    checkpoint::project_checkpoint_with_resolver(
        &s.f.store,
        checkpoint::ProjectionContext {
            workspace: "ws",
            context_thread: "thread",
            source_thread: "thread",
            owner: &old.owner,
            allowed: &allowed,
            allow_historical_gaps: true,
        },
        &old.id,
        &mut suppressed,
        &mut coverage::CheckpointGraphResolver::default(),
    )
    .await
    .unwrap();
    assert_eq!(ids(&suppressed), ids(&selected));
    assert!(suppressed.iter().any(|message| {
        message.provenance.as_ref().unwrap().sources[0].id == covering.id
            && message
                .provenance
                .as_ref()
                .unwrap()
                .ambiguous_input_aliases
                .contains(&conflict)
    }));
    assert_eq!(ids(&select(&suppressed)), ids(&selected));
    let suppressed_ref = frozen::capture(&s.f.store, "ws", "thread", &allowed, &suppressed)
        .await
        .unwrap();
    let suppressed_literal = frozen::restore(&s.f.store, "ws", &allowed, &suppressed_ref)
        .await
        .unwrap();
    assert_eq!(suppressed_literal, suppressed);
    assert_eq!(ids(&select(&suppressed_literal)), ids(&selected));
    let suppressed_accepted = frozen::restore_accepted_history_for_execution(
        &s.f.store,
        "ws",
        Some("thread"),
        "thread",
        &allowed,
        &serde_json::to_string(&suppressed_ref).unwrap(),
    )
    .await
    .unwrap()
    .messages;
    assert_eq!(ids(&suppressed_accepted), ids(&selected));
    assert_eq!(ids(&select(&suppressed_accepted)), ids(&selected));
    let descriptor = frozen::capture(&s.f.store, "ws", "thread", &allowed, &messages)
        .await
        .unwrap();
    let restored = frozen::restore_accepted_history_for_execution(
        &s.f.store,
        "ws",
        Some("thread"),
        "thread",
        &allowed,
        &serde_json::to_string(&descriptor).unwrap(),
    )
    .await
    .unwrap()
    .messages;
    assert_eq!(
        ids(&restored),
        BTreeSet::from([covering.id.clone(), competing.id.clone()])
    );
    assert!(restored.iter().any(|message| {
        message
            .provenance
            .as_ref()
            .unwrap()
            .ambiguous_input_aliases
            .contains(&conflict)
    }));
    let retained = frozen::capture(&s.f.store, "ws", "thread", &allowed, &restored)
        .await
        .unwrap();
    let literal = frozen::restore(&s.f.store, "ws", &allowed, &retained)
        .await
        .unwrap();
    assert_eq!(literal, restored);
    assert_eq!(
        ids(&select(&literal)),
        BTreeSet::from([covering.id.clone(), competing.id.clone()])
    );
}

#[tokio::test]
async fn exact_cross_domain_absorption_keeps_only_covered_input_evidence() {
    use pioneer_agent::compaction::composition::{
        AcceptedContextBranch, ScopedHistorySource, compose_context,
    };
    use pioneer_compaction::CoverageDomain;
    use std::collections::BTreeMap;

    for reverse in [false, true] {
        let s = scenario().await;
        for thread in ["cross-domain-cover", "cross-domain-copy", "cross-domain-x"] {
            insert_projection_event(&s.f, thread).await;
        }
        for thread in ["cross-domain-copy", "cross-domain-x"] {
            s.f.store
                .database_connection()
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES (?,?,0,'text','same question','{\"type\":\"text\",\"text\":\"same question\"}',CURRENT_TIMESTAMP)",
                    [format!("{thread}-input").into(), format!("{thread}-turn").into()],
                ))
                .await
                .unwrap();
            history::prepare_history(&s.f.store, "ws", thread)
                .await
                .unwrap();
        }
        let a = s.original.provenance.as_ref().unwrap().sources[0].clone();
        let a = SourceRef {
            scope: a.scope,
            id: a.id,
            version: a.version,
        };
        let b = SourceRef {
            scope: s.copy.scope.clone(),
            id: s.copy.id.clone(),
            version: s.copy.version.clone(),
        };
        let c =
            s.f.store
                .compaction_source_page(
                    "ws",
                    "cross-domain-copy",
                    "cross-domain-copy-turn",
                    PagedSource::Input,
                    0,
                )
                .await
                .unwrap()
                .entries[0]
                .reference
                .clone();
        let x =
            s.f.store
                .compaction_source_page(
                    "ws",
                    "cross-domain-x",
                    "cross-domain-x-turn",
                    PagedSource::Input,
                    0,
                )
                .await
                .unwrap()
                .entries[0]
                .reference
                .clone();
        assert_eq!(a.version, b.version);
        assert_eq!(a.version, c.version);
        assert_eq!(a.version, x.version);
        let old = publish_projection_checkpoint(
            &s.f,
            "thread",
            "cross-domain-small",
            &[("thread".into(), a.clone())],
            CoverageDomain::WorkingContext,
        )
        .await;
        let old_source =
            s.f.store
                .compaction_checkpoint_source("ws", "thread", &old.id)
                .await
                .unwrap()
                .unwrap();
        let covering = publish_projection_checkpoint(
            &s.f,
            "cross-domain-cover",
            "cross-domain-large",
            &[
                ("thread".into(), old_source),
                ("copy-thread".into(), b.clone()),
            ],
            CoverageDomain::OwnContribution,
        )
        .await;
        let competing = publish_projection_checkpoint(
            &s.f,
            "cross-domain-x",
            "cross-domain-competing",
            &[("cross-domain-x".into(), x.clone())],
            CoverageDomain::OwnContribution,
        )
        .await;
        let mut allowed = s.allowed.clone();
        allowed.extend(
            ["cross-domain-cover", "cross-domain-copy", "cross-domain-x"]
                .into_iter()
                .map(str::to_owned),
        );
        let source = |source: &SourceRef| MessageSourceRef {
            scope: source.scope.clone(),
            id: source.id.clone(),
            version: source.version.clone(),
        };
        fn context<'a>(
            thread: &'a str,
            checkpoint: &'a Checkpoint,
            allowed: &'a BTreeSet<String>,
        ) -> checkpoint::ProjectionContext<'a> {
            checkpoint::ProjectionContext {
                workspace: "ws",
                context_thread: "thread",
                source_thread: thread,
                owner: &checkpoint.owner,
                allowed,
                allow_historical_gaps: false,
            }
        }
        let mut old_message = checkpoint::checkpoint_message_with_resolver(
            &s.f.store,
            context("thread", &old, &allowed),
            &old.id,
            &mut coverage::CheckpointGraphResolver::default(),
        )
        .await
        .unwrap();
        let valid = MessageSourceAlias {
            represented_thread_id: "thread".into(),
            represented_source: source(&a),
            thread_id: "cross-domain-copy".into(),
            source: source(&c),
        };
        let conflict = MessageSourceIdentity {
            thread_id: "copy-thread".into(),
            source: source(&b),
        };
        old_message
            .provenance
            .as_mut()
            .unwrap()
            .source_aliases
            .push(valid.clone());
        old_message
            .provenance
            .as_mut()
            .unwrap()
            .ambiguous_input_aliases
            .push(conflict.clone());
        let covering_message = checkpoint::checkpoint_message_with_resolver(
            &s.f.store,
            context("cross-domain-cover", &covering, &allowed),
            &covering.id,
            &mut coverage::CheckpointGraphResolver::default(),
        )
        .await
        .unwrap();
        assert!(
            covering_message
                .provenance
                .as_ref()
                .unwrap()
                .ambiguous_input_aliases
                .is_empty()
        );
        let mut messages = if reverse {
            vec![covering_message, old_message]
        } else {
            vec![old_message, covering_message]
        };
        let original = messages.clone();
        let original_descriptor = frozen::capture(&s.f.store, "ws", "thread", &allowed, &original)
            .await
            .unwrap();
        checkpoint::project_checkpoint_with_resolver(
            &s.f.store,
            context("thread", &old, &allowed),
            &old.id,
            &mut messages,
            &mut coverage::CheckpointGraphResolver::default(),
        )
        .await
        .unwrap();
        assert_eq!(messages.len(), 1, "summary order {reverse}");
        assert_eq!(
            frozen::restore(&s.f.store, "ws", &allowed, &original_descriptor)
                .await
                .unwrap(),
            original
        );
        let survivor = messages[0].provenance.as_ref().unwrap();
        assert_eq!(survivor.sources[0].id, covering.id);
        assert!(survivor.ambiguous_input_aliases.contains(&conflict));
        assert!(survivor.source_aliases.contains(&valid));

        let covering_source =
            s.f.store
                .compaction_checkpoint_source("ws", "cross-domain-cover", &covering.id)
                .await
                .unwrap()
                .unwrap();
        let covering_graph = coverage::CheckpointGraphResolver::default()
            .resolve(&s.f.store, "ws", Some(&allowed), &covering_source)
            .await
            .unwrap()
            .unwrap();
        let old_source =
            s.f.store
                .compaction_checkpoint_source("ws", "thread", &old.id)
                .await
                .unwrap()
                .unwrap();
        let old_graph = coverage::CheckpointGraphResolver::default()
            .resolve(&s.f.store, "ws", Some(&allowed), &old_source)
            .await
            .unwrap()
            .unwrap();
        assert!(old_graph.leaves.is_subset(&covering_graph.leaves));
        assert_ne!(old_graph.leaves, covering_graph.leaves);
        assert!(
            !covering_graph
                .ambiguous_input_aliases
                .contains(&ScopedHistorySource {
                    thread: conflict.thread_id.clone(),
                    source: b.clone(),
                })
        );
        let outside = MessageSourceAlias {
            represented_thread_id: "cross-domain-x".into(),
            represented_source: source(&x),
            thread_id: "copy-thread".into(),
            source: source(&b),
        };
        let filtered = checkpoint::transferred_input_evidence(
            &covering_graph.leaves,
            [&valid, &outside],
            [&conflict],
        );
        assert!(filtered.aliases.contains(&valid));
        assert!(!filtered.aliases.contains(&outside));

        let mut competing_message = checkpoint::checkpoint_message_with_resolver(
            &s.f.store,
            context("cross-domain-x", &competing, &allowed),
            &competing.id,
            &mut coverage::CheckpointGraphResolver::default(),
        )
        .await
        .unwrap();
        competing_message
            .provenance
            .as_mut()
            .unwrap()
            .source_aliases
            .push(outside);
        messages.push(competing_message);
        let mut closures = BTreeMap::new();
        for (thread, checkpoint) in [
            ("cross-domain-cover", &covering),
            ("cross-domain-x", &competing),
        ] {
            let checkpoint_source =
                s.f.store
                    .compaction_checkpoint_source("ws", thread, &checkpoint.id)
                    .await
                    .unwrap()
                    .unwrap();
            let graph = coverage::CheckpointGraphResolver::default()
                .resolve(&s.f.store, "ws", Some(&allowed), &checkpoint_source)
                .await
                .unwrap()
                .unwrap();
            closures.insert(
                ScopedHistorySource {
                    thread: thread.into(),
                    source: checkpoint_source,
                },
                graph.leaves.clone(),
            );
        }
        let domains = closures
            .keys()
            .cloned()
            .map(|source| (source, CoverageDomain::OwnContribution))
            .collect::<BTreeMap<_, _>>();
        let selected = compose_context(
            "ws",
            "child",
            &[AcceptedContextBranch {
                thread: "thread",
                messages: &messages,
                checkpoints: &closures,
                checkpoint_domains: &domains,
            }],
        )
        .unwrap();
        assert_eq!(selected.len(), 2, "disputed copy must not cover X");
        assert!(
            selected.iter().any(|message| {
                message.provenance.as_ref().unwrap().sources[0].id == competing.id
            })
        );
        let selected_again = compose_context(
            "ws",
            "grandchild",
            &[AcceptedContextBranch {
                thread: "child",
                messages: &selected,
                checkpoints: &closures,
                checkpoint_domains: &domains,
            }],
        )
        .unwrap();
        assert_eq!(selected_again.len(), 2);

        let descriptor = frozen::capture(&s.f.store, "ws", "thread", &allowed, &messages)
            .await
            .unwrap();
        let literal = frozen::restore(&s.f.store, "ws", &allowed, &descriptor)
            .await
            .unwrap();
        assert_eq!(literal, messages);
        assert_eq!(
            compose_context(
                "ws",
                "child",
                &[AcceptedContextBranch {
                    thread: "thread",
                    messages: &literal,
                    checkpoints: &closures,
                    checkpoint_domains: &domains,
                }],
            )
            .unwrap()
            .len(),
            2
        );
    }
}

#[tokio::test]
async fn competing_checkpoint_candidate_cannot_remove_saved_summary() {
    use pioneer_compaction::CoverageDomain;

    for reverse in [false, true] {
        let s = scenario().await;
        let (b_thread, x_thread) = if reverse {
            ("zzz-summary-b", "aaa-summary-x")
        } else {
            ("aaa-summary-b", "zzz-summary-x")
        };
        for thread in [b_thread, x_thread] {
            insert_projection_event(&s.f, thread).await;
            s.f.store
                .database_connection()
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES (?,?,0,'text','same question','{\"type\":\"text\",\"text\":\"same question\"}',CURRENT_TIMESTAMP)",
                    [format!("{thread}-input").into(), format!("{thread}-turn").into()],
                ))
                .await
                .unwrap();
            history::prepare_history(&s.f.store, "ws", thread)
                .await
                .unwrap();
        }
        let input = |thread: &str| {
            let turn = format!("{thread}-turn");
            (thread.to_owned(), turn)
        };
        let (b_owner, b_turn) = input(b_thread);
        let (x_owner, x_turn) = input(x_thread);
        let b =
            s.f.store
                .compaction_source_page("ws", &b_owner, &b_turn, PagedSource::Input, 0)
                .await
                .unwrap()
                .entries[0]
                .reference
                .clone();
        let x =
            s.f.store
                .compaction_source_page("ws", &x_owner, &x_turn, PagedSource::Input, 0)
                .await
                .unwrap()
                .entries[0]
                .reference
                .clone();
        let a = &s.original.provenance.as_ref().unwrap().sources[0];
        let a = SourceRef {
            scope: a.scope.clone(),
            id: a.id.clone(),
            version: a.version.clone(),
        };
        assert_eq!(a.version, b.version);
        assert_eq!(a.version, x.version);
        let mut allowed = s.allowed.clone();
        allowed.extend([b_thread, x_thread].into_iter().map(str::to_owned));
        let old = publish_projection_checkpoint(
            &s.f,
            "thread",
            "saved-summary-a",
            &[("thread".into(), a.clone())],
            CoverageDomain::WorkingContext,
        )
        .await;
        let b_checkpoint = publish_projection_checkpoint(
            &s.f,
            b_thread,
            "candidate-summary-b",
            &[(b_thread.into(), b.clone())],
            CoverageDomain::WorkingContext,
        )
        .await;
        let runtime_source = |source: &SourceRef| MessageSourceRef {
            scope: source.scope.clone(),
            id: source.id.clone(),
            version: source.version.clone(),
        };
        let mut raw_b = s.original.clone();
        let b_origin = raw_b.provenance.as_mut().unwrap();
        b_origin.thread_id = b_thread.into();
        b_origin.context_thread = Some("thread".into());
        b_origin.unit_id = format!("{b_thread}:input");
        b_origin.sources = vec![runtime_source(&b)];
        b_origin.source_aliases.clear();
        b_origin.inherited = true;
        let mut raw_x = s.original.clone();
        let x_origin = raw_x.provenance.as_mut().unwrap();
        x_origin.thread_id = x_thread.into();
        x_origin.context_thread = Some("thread".into());
        x_origin.unit_id = format!("{x_thread}:input");
        x_origin.sources = vec![runtime_source(&x)];
        x_origin.source_aliases.clear();
        x_origin.inherited = true;
        let mut published_x = raw_x.clone();
        let x_origin = published_x.provenance.as_mut().unwrap();
        x_origin.context_thread = None;
        x_origin.inherited = false;
        x_origin.source_aliases.push(MessageSourceAlias {
            represented_thread_id: x_thread.into(),
            represented_source: runtime_source(&x),
            thread_id: b_thread.into(),
            source: runtime_source(&b),
        });
        let x_projection = frozen::capture(&s.f.store, "ws", x_thread, &allowed, &[published_x])
            .await
            .unwrap();
        let x_checkpoint = publish_projection_checkpoint(
            &s.f,
            x_thread,
            "candidate-summary-x",
            &[(x_thread.into(), x.clone())],
            CoverageDomain::WorkingContext,
        )
        .await;
        s.f.store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) SELECT ?,id,identity_sha256,imports_sha256,import_count FROM compaction_frozen_history WHERE id=?",
                [x_checkpoint.operation_id.clone().into(), x_projection.manifest_id.clone().into()],
            ))
            .await
            .unwrap();
        let x_edges =
            s.f.store
                .compaction_checkpoint_edges(&x_checkpoint.id)
                .await
                .unwrap()
                .unwrap();
        assert!(
            x_edges
                .replay_aliases
                .iter()
                .any(|alias| { alias.replay.source == b && alias.covered.source == x })
        );
        let mut old_summary = checkpoint::checkpoint_message_with_resolver(
            &s.f.store,
            checkpoint::ProjectionContext {
                workspace: "ws",
                context_thread: "thread",
                source_thread: "thread",
                owner: &old.owner,
                allowed: &allowed,
                allow_historical_gaps: true,
            },
            &old.id,
            &mut coverage::CheckpointGraphResolver::default(),
        )
        .await
        .unwrap();
        old_summary
            .provenance
            .as_mut()
            .unwrap()
            .source_aliases
            .push(MessageSourceAlias {
                represented_thread_id: "thread".into(),
                represented_source: runtime_source(&a),
                thread_id: b_thread.into(),
                source: runtime_source(&b),
            });
        // The exact saved A reference remains valid, but no current head can
        // add it back if a B candidate removes it by mistake.
        s.f.store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE compaction_context SET head=NULL WHERE owner=?",
                [old.owner.clone().into()],
            ))
            .await
            .unwrap();
        let old_source =
            s.f.store
                .compaction_checkpoint_source("ws", "thread", &old.id)
                .await
                .unwrap()
                .unwrap();
        assert!(
            coverage::CheckpointGraphResolver::default()
                .resolve(&s.f.store, "ws", Some(&allowed), &old_source)
                .await
                .unwrap()
                .is_some()
        );
        let accepted = vec![old_summary, raw_b, raw_x];
        let ids = |messages: &[ChatMessage]| {
            messages
                .iter()
                .map(|message| message.provenance.as_ref().unwrap().sources[0].id.clone())
                .collect::<BTreeSet<_>>()
        };
        let expected = BTreeSet::from([
            old.id.clone(),
            b_checkpoint.id.clone(),
            x_checkpoint.id.clone(),
        ]);
        let mut local_then_accepted = accepted.clone();
        checkpoint::project_checkpoint_with_resolver(
            &s.f.store,
            checkpoint::ProjectionContext {
                workspace: "ws",
                context_thread: "thread",
                source_thread: b_thread,
                owner: &b_checkpoint.owner,
                allowed: &allowed,
                allow_historical_gaps: false,
            },
            &b_checkpoint.id,
            &mut local_then_accepted,
            &mut coverage::CheckpointGraphResolver::default(),
        )
        .await
        .unwrap();
        assert!(ids(&local_then_accepted).contains(&old.id));
        checkpoint::project_accepted_checkpoints_with_resolver(
            &s.f.store,
            "ws",
            "thread",
            &allowed,
            &mut local_then_accepted,
            &mut coverage::CheckpointGraphResolver::default(),
        )
        .await
        .unwrap();
        assert_eq!(ids(&local_then_accepted), expected);
        let mut without_conflict = accepted[..2].to_vec();
        checkpoint::project_checkpoint_with_resolver(
            &s.f.store,
            checkpoint::ProjectionContext {
                workspace: "ws",
                context_thread: "thread",
                source_thread: b_thread,
                owner: &b_checkpoint.owner,
                allowed: &allowed,
                allow_historical_gaps: false,
            },
            &b_checkpoint.id,
            &mut without_conflict,
            &mut coverage::CheckpointGraphResolver::default(),
        )
        .await
        .unwrap();
        checkpoint::project_accepted_checkpoints_with_resolver(
            &s.f.store,
            "ws",
            "thread",
            &allowed,
            &mut without_conflict,
            &mut coverage::CheckpointGraphResolver::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            ids(&without_conflict),
            BTreeSet::from([b_checkpoint.id.clone()])
        );
        let mut live = accepted.clone();
        checkpoint::project_accepted_checkpoints_with_resolver(
            &s.f.store,
            "ws",
            "thread",
            &allowed,
            &mut live,
            &mut coverage::CheckpointGraphResolver::default(),
        )
        .await
        .unwrap();
        assert_eq!(ids(&live), expected, "live order {reverse}");
        let descriptor = frozen::capture(&s.f.store, "ws", "thread", &allowed, &accepted)
            .await
            .unwrap();
        let restored = frozen::restore_accepted_history_for_execution(
            &s.f.store,
            "ws",
            Some("thread"),
            "thread",
            &allowed,
            &serde_json::to_string(&descriptor).unwrap(),
        )
        .await
        .unwrap()
        .messages;
        assert_eq!(ids(&restored), expected, "frozen order {reverse}");
        assert_eq!(
            frozen::restore(&s.f.store, "ws", &allowed, &descriptor)
                .await
                .unwrap(),
            accepted
        );
    }
}

#[tokio::test]
async fn absorbed_frozen_candidate_keeps_conflict_from_published_graph() {
    use pioneer_agent::compaction::composition::{AcceptedContextBranch, ScopedHistorySource};
    use pioneer_compaction::CoverageDomain;
    use std::collections::BTreeMap;

    for reverse in [false, true] {
        let s = scenario().await;
        let (c_thread, x_thread) = if reverse {
            ("zzz-frozen-c", "aaa-frozen-x")
        } else {
            ("aaa-frozen-c", "zzz-frozen-x")
        };
        let d_thread = "frozen-d";
        let mut inputs = BTreeMap::new();
        for thread in [c_thread, x_thread, d_thread] {
            insert_projection_event(&s.f, thread).await;
            let turn = format!("{thread}-turn");
            s.f.store
                .database_connection()
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES (?,?,0,'text','same question','{\"type\":\"text\",\"text\":\"same question\"}',CURRENT_TIMESTAMP)",
                    [format!("{thread}-input").into(), turn.clone().into()],
                ))
                .await
                .unwrap();
            history::prepare_history(&s.f.store, "ws", thread)
                .await
                .unwrap();
            let source =
                s.f.store
                    .compaction_source_page("ws", thread, &turn, PagedSource::Input, 0)
                    .await
                    .unwrap()
                    .entries[0]
                    .reference
                    .clone();
            inputs.insert(thread, source);
        }
        let a = &s.original.provenance.as_ref().unwrap().sources[0];
        let a = SourceRef {
            scope: a.scope.clone(),
            id: a.id.clone(),
            version: a.version.clone(),
        };
        let c = inputs[c_thread].clone();
        let x = inputs[x_thread].clone();
        let d = inputs[d_thread].clone();
        assert_eq!(a.version, c.version);
        assert_eq!(a.version, x.version);
        assert_eq!(a.version, d.version);
        let mut allowed = s.allowed.clone();
        allowed.extend(
            [c_thread, x_thread, d_thread]
                .into_iter()
                .map(str::to_owned),
        );
        let runtime_source = |source: &SourceRef| MessageSourceRef {
            scope: source.scope.clone(),
            id: source.id.clone(),
            version: source.version.clone(),
        };
        let mut raw_a = s.original.clone();
        raw_a.provenance.as_mut().unwrap().source_aliases.clear();
        let raw = |thread: &str, source: &SourceRef| {
            let mut message = raw_a.clone();
            let origin = message.provenance.as_mut().unwrap();
            origin.thread_id = thread.into();
            origin.context_thread = Some("thread".into());
            origin.unit_id = format!("{thread}:input");
            origin.sources = vec![runtime_source(source)];
            origin.inherited = true;
            message
        };
        let raw_c = raw(c_thread, &c);
        let raw_x = raw(x_thread, &x);
        let mut published_a = raw_a.clone();
        for (copy_thread, copy) in [(c_thread, &c), (d_thread, &d)] {
            published_a
                .provenance
                .as_mut()
                .unwrap()
                .source_aliases
                .push(MessageSourceAlias {
                    represented_thread_id: "thread".into(),
                    represented_source: runtime_source(&a),
                    thread_id: copy_thread.into(),
                    source: runtime_source(copy),
                });
        }
        let a_projection = frozen::capture(&s.f.store, "ws", "thread", &allowed, &[published_a])
            .await
            .unwrap();
        let mut published_x = raw_x.clone();
        let x_origin = published_x.provenance.as_mut().unwrap();
        x_origin.context_thread = None;
        x_origin.inherited = false;
        x_origin.source_aliases.push(MessageSourceAlias {
            represented_thread_id: x_thread.into(),
            represented_source: runtime_source(&x),
            thread_id: d_thread.into(),
            source: runtime_source(&d),
        });
        let x_projection = frozen::capture(&s.f.store, "ws", x_thread, &allowed, &[published_x])
            .await
            .unwrap();
        let a_checkpoint = publish_projection_checkpoint(
            &s.f,
            "thread",
            "frozen-candidate-a",
            &[("thread".into(), a.clone())],
            CoverageDomain::WorkingContext,
        )
        .await;
        let c_checkpoint = publish_projection_checkpoint(
            &s.f,
            c_thread,
            "frozen-candidate-c",
            &[(c_thread.into(), c.clone())],
            CoverageDomain::WorkingContext,
        )
        .await;
        let x_checkpoint = publish_projection_checkpoint(
            &s.f,
            x_thread,
            "frozen-candidate-x",
            &[(x_thread.into(), x.clone())],
            CoverageDomain::WorkingContext,
        )
        .await;
        for (checkpoint, projection) in [
            (&a_checkpoint, &a_projection),
            (&x_checkpoint, &x_projection),
        ] {
            s.f.store
                .database_connection()
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) SELECT ?,id,identity_sha256,imports_sha256,import_count FROM compaction_frozen_history WHERE id=?",
                    [checkpoint.operation_id.clone().into(), projection.manifest_id.clone().into()],
                ))
                .await
                .unwrap();
        }
        let saved = vec![raw_a, raw_c, raw_x];
        assert!(saved.iter().all(|message| {
            message
                .provenance
                .as_ref()
                .unwrap()
                .source_aliases
                .is_empty()
        }));
        let original = frozen::capture(&s.f.store, "ws", "thread", &allowed, &saved)
            .await
            .unwrap();
        let selected = frozen::restore_accepted_history_for_execution(
            &s.f.store,
            "ws",
            Some("thread"),
            "thread",
            &allowed,
            &serde_json::to_string(&original).unwrap(),
        )
        .await
        .unwrap()
        .messages;
        let ids = |messages: &[ChatMessage]| {
            messages
                .iter()
                .map(|message| message.provenance.as_ref().unwrap().sources[0].id.clone())
                .collect::<BTreeSet<_>>()
        };
        let expected = BTreeSet::from([c_checkpoint.id.clone(), x_checkpoint.id.clone()]);
        assert_eq!(ids(&selected), expected, "candidate order {reverse}");
        let conflict = MessageSourceIdentity {
            thread_id: d_thread.into(),
            source: runtime_source(&d),
        };
        assert!(selected.iter().any(|message| {
            message.provenance.as_ref().unwrap().sources[0].id == c_checkpoint.id
                && message
                    .provenance
                    .as_ref()
                    .unwrap()
                    .ambiguous_input_aliases
                    .contains(&conflict)
        }));
        assert_eq!(
            frozen::restore(&s.f.store, "ws", &allowed, &original)
                .await
                .unwrap(),
            saved
        );
        // A later composition must not treat the remaining D -> X claim as
        // unambiguous after the A candidate and its D -> A claim disappeared.
        let d_checkpoint = publish_projection_checkpoint(
            &s.f,
            d_thread,
            "frozen-candidate-d",
            &[(d_thread.into(), d.clone())],
            CoverageDomain::WorkingContext,
        )
        .await;
        let d_summary = checkpoint::checkpoint_message_with_resolver(
            &s.f.store,
            checkpoint::ProjectionContext {
                workspace: "ws",
                context_thread: "thread",
                source_thread: d_thread,
                owner: &d_checkpoint.owner,
                allowed: &allowed,
                allow_historical_gaps: true,
            },
            &d_checkpoint.id,
            &mut coverage::CheckpointGraphResolver::default(),
        )
        .await
        .unwrap();
        let mut closures = BTreeMap::new();
        for (thread, checkpoint) in [
            (c_thread, &c_checkpoint),
            (x_thread, &x_checkpoint),
            (d_thread, &d_checkpoint),
        ] {
            let source =
                s.f.store
                    .compaction_checkpoint_source("ws", thread, &checkpoint.id)
                    .await
                    .unwrap()
                    .unwrap();
            let graph = coverage::CheckpointGraphResolver::default()
                .resolve(&s.f.store, "ws", Some(&allowed), &source)
                .await
                .unwrap()
                .unwrap();
            closures.insert(
                ScopedHistorySource {
                    thread: thread.into(),
                    source,
                },
                graph.leaves.clone(),
            );
        }
        let domains = closures
            .keys()
            .cloned()
            .map(|source| (source, CoverageDomain::WorkingContext))
            .collect::<BTreeMap<_, _>>();
        let compose_with_d = |messages: &[ChatMessage]| {
            let mut messages = messages.to_vec();
            messages.push(d_summary.clone());
            pioneer_agent::compaction::composition::compose_context(
                "ws",
                "child",
                &[AcceptedContextBranch {
                    thread: "thread",
                    messages: &messages,
                    checkpoints: &closures,
                    checkpoint_domains: &domains,
                }],
            )
            .unwrap()
        };
        let expected_with_d = BTreeSet::from([
            c_checkpoint.id.clone(),
            x_checkpoint.id.clone(),
            d_checkpoint.id.clone(),
        ]);
        assert_eq!(ids(&compose_with_d(&selected)), expected_with_d);
        s.f.store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE compaction_context SET head=NULL WHERE owner=?",
                [a_checkpoint.owner.clone().into()],
            ))
            .await
            .unwrap();
        let recaptured = frozen::capture(&s.f.store, "ws", "thread", &allowed, &selected)
            .await
            .unwrap();
        assert_eq!(
            frozen::restore(&s.f.store, "ws", &allowed, &recaptured)
                .await
                .unwrap(),
            selected
        );
        let again = frozen::restore_accepted_history_for_execution(
            &s.f.store,
            "ws",
            Some("thread"),
            "thread",
            &allowed,
            &serde_json::to_string(&recaptured).unwrap(),
        )
        .await
        .unwrap()
        .messages;
        assert_eq!(ids(&again), expected);
        assert_eq!(ids(&compose_with_d(&again)), expected_with_d);
    }
}

#[tokio::test]
async fn accepted_raw_history_sees_all_candidate_claims_before_projection() {
    use pioneer_agent::compaction::composition::{AcceptedContextBranch, ScopedHistorySource};
    use pioneer_compaction::CoverageDomain;
    use std::collections::BTreeMap;

    for reverse in [false, true] {
        for conflicting in [false, true] {
            let s = scenario().await;
            let a_thread = "aaa-live-a";
            let (b_thread, x_thread) = if reverse {
                ("zzz-live-b", "bbb-live-x")
            } else {
                ("bbb-live-b", "zzz-live-x")
            };
            let mut inputs = BTreeMap::new();
            for thread in [a_thread, b_thread, x_thread] {
                insert_projection_event(&s.f, thread).await;
                let turn = format!("{thread}-turn");
                s.f.store
                    .database_connection()
                    .execute_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES (?,?,0,'text','same question','{\"type\":\"text\",\"text\":\"same question\"}',CURRENT_TIMESTAMP)",
                        [format!("{thread}-input").into(), turn.clone().into()],
                    ))
                    .await
                    .unwrap();
                history::prepare_history(&s.f.store, "ws", thread)
                    .await
                    .unwrap();
                let source =
                    s.f.store
                        .compaction_source_page("ws", thread, &turn, PagedSource::Input, 0)
                        .await
                        .unwrap()
                        .entries[0]
                        .reference
                        .clone();
                inputs.insert(thread, source);
            }
            let a = &inputs[a_thread];
            let b = &inputs[b_thread];
            let x = &inputs[x_thread];
            assert_eq!(a.version, b.version);
            assert_eq!(a.version, x.version);
            let mut allowed = s.allowed.clone();
            allowed.extend(
                [a_thread, b_thread, x_thread]
                    .into_iter()
                    .map(str::to_owned),
            );
            let runtime_source = |source: &SourceRef| MessageSourceRef {
                scope: source.scope.clone(),
                id: source.id.clone(),
                version: source.version.clone(),
            };
            let raw = |thread: &str, source: &SourceRef| {
                let mut message = s.original.clone();
                let origin = message.provenance.as_mut().unwrap();
                origin.thread_id = thread.into();
                origin.context_thread = Some("thread".into());
                origin.unit_id = format!("{thread}:input");
                origin.sources = vec![runtime_source(source)];
                origin.source_aliases.clear();
                origin.ambiguous_input_aliases.clear();
                origin.inherited = true;
                message
            };
            let raw_history = vec![raw(a_thread, a), raw(b_thread, b), raw(x_thread, x)];
            let mut published_a = raw_history[0].clone();
            let a_origin = published_a.provenance.as_mut().unwrap();
            a_origin.context_thread = None;
            a_origin.inherited = false;
            a_origin.source_aliases.push(MessageSourceAlias {
                represented_thread_id: a_thread.into(),
                represented_source: runtime_source(a),
                thread_id: b_thread.into(),
                source: runtime_source(b),
            });
            let a_projection =
                frozen::capture(&s.f.store, "ws", a_thread, &allowed, &[published_a])
                    .await
                    .unwrap();
            let mut published_x = raw_history[2].clone();
            let x_origin = published_x.provenance.as_mut().unwrap();
            x_origin.context_thread = None;
            x_origin.inherited = false;
            if conflicting {
                x_origin.source_aliases.push(MessageSourceAlias {
                    represented_thread_id: x_thread.into(),
                    represented_source: runtime_source(x),
                    thread_id: b_thread.into(),
                    source: runtime_source(b),
                });
            }
            let x_projection =
                frozen::capture(&s.f.store, "ws", x_thread, &allowed, &[published_x])
                    .await
                    .unwrap();
            let a_checkpoint = publish_projection_checkpoint(
                &s.f,
                a_thread,
                "live-candidate-a",
                &[(a_thread.into(), a.clone())],
                CoverageDomain::WorkingContext,
            )
            .await;
            let b_checkpoint = publish_projection_checkpoint(
                &s.f,
                b_thread,
                "live-candidate-b",
                &[(b_thread.into(), b.clone())],
                CoverageDomain::WorkingContext,
            )
            .await;
            let x_checkpoint = publish_projection_checkpoint(
                &s.f,
                x_thread,
                "live-candidate-x",
                &[(x_thread.into(), x.clone())],
                CoverageDomain::WorkingContext,
            )
            .await;
            for (checkpoint, projection) in [
                (&a_checkpoint, &a_projection),
                (&x_checkpoint, &x_projection),
            ] {
                s.f.store
                    .database_connection()
                    .execute_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) SELECT ?,id,identity_sha256,imports_sha256,import_count FROM compaction_frozen_history WHERE id=?",
                        [checkpoint.operation_id.clone().into(), projection.manifest_id.clone().into()],
                    ))
                    .await
                    .unwrap();
            }
            let expected = if conflicting {
                BTreeSet::from([
                    a_checkpoint.id.clone(),
                    b_checkpoint.id.clone(),
                    x_checkpoint.id.clone(),
                ])
            } else {
                BTreeSet::from([b_checkpoint.id.clone(), x_checkpoint.id.clone()])
            };
            let ids = |messages: &[ChatMessage]| {
                messages
                    .iter()
                    .map(|message| message.provenance.as_ref().unwrap().sources[0].id.clone())
                    .collect::<BTreeSet<_>>()
            };
            let mut closures = BTreeMap::new();
            for (thread, checkpoint) in [
                (a_thread, &a_checkpoint),
                (b_thread, &b_checkpoint),
                (x_thread, &x_checkpoint),
            ] {
                let source =
                    s.f.store
                        .compaction_checkpoint_source("ws", thread, &checkpoint.id)
                        .await
                        .unwrap()
                        .unwrap();
                let graph = coverage::CheckpointGraphResolver::default()
                    .resolve(&s.f.store, "ws", Some(&allowed), &source)
                    .await
                    .unwrap()
                    .unwrap();
                closures.insert(
                    ScopedHistorySource {
                        thread: thread.into(),
                        source,
                    },
                    graph.leaves.clone(),
                );
            }
            let domains = closures
                .keys()
                .cloned()
                .map(|source| (source, CoverageDomain::WorkingContext))
                .collect::<BTreeMap<_, _>>();
            for separate_boundary in [false, true] {
                let mut projected = raw_history.clone();
                let mut resolver = coverage::CheckpointGraphResolver::default();
                if separate_boundary {
                    let ordinals = [0, 1, 2];
                    checkpoint::project_accepted_checkpoints_with_boundary_evidence(
                        &s.f.store,
                        "ws",
                        "thread",
                        &allowed,
                        &mut projected,
                        Some(checkpoint::ProjectionBoundaryEvidence {
                            messages: &raw_history,
                            model_ordinals: &ordinals,
                        }),
                        None,
                        &mut resolver,
                    )
                    .await
                    .unwrap();
                } else {
                    checkpoint::project_accepted_checkpoints_with_resolver(
                        &s.f.store,
                        "ws",
                        "thread",
                        &allowed,
                        &mut projected,
                        &mut resolver,
                    )
                    .await
                    .unwrap();
                }
                assert_eq!(
                    ids(&projected),
                    expected,
                    "reverse={reverse}, conflict={conflicting}, boundary={separate_boundary}"
                );
                checkpoint::project_accepted_checkpoints_with_resolver(
                    &s.f.store,
                    "ws",
                    "thread",
                    &allowed,
                    &mut projected,
                    &mut coverage::CheckpointGraphResolver::default(),
                )
                .await
                .unwrap();
                assert_eq!(ids(&projected), expected);
                let composed = pioneer_agent::compaction::composition::compose_context(
                    "ws",
                    "child",
                    &[AcceptedContextBranch {
                        thread: "thread",
                        messages: &projected,
                        checkpoints: &closures,
                        checkpoint_domains: &domains,
                    }],
                )
                .unwrap();
                assert_eq!(ids(&composed), expected);
                let descriptor = frozen::capture(&s.f.store, "ws", "thread", &allowed, &projected)
                    .await
                    .unwrap();
                assert_eq!(
                    frozen::restore(&s.f.store, "ws", &allowed, &descriptor)
                        .await
                        .unwrap(),
                    projected
                );
                let restored = frozen::restore_accepted_history_for_execution(
                    &s.f.store,
                    "ws",
                    Some("thread"),
                    "thread",
                    &allowed,
                    &serde_json::to_string(&descriptor).unwrap(),
                )
                .await
                .unwrap()
                .messages;
                assert_eq!(ids(&restored), expected);
            }
        }
    }
}
