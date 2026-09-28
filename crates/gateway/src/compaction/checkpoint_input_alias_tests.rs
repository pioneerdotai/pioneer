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
        assert!(
            messages[0]
                .provenance
                .as_ref()
                .unwrap()
                .source_aliases
                .iter()
                .any(|alias| alias.source.id == "original" && alias.represented_source == s.copy)
        );
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
