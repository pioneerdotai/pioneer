//! Durable response → UI copies → frozen checkpoint → released cold reads.
//! Synthetic native state is not a vendor signature acceptance test.
use super::*;
use crate::compaction::{frozen, load_exact_line_history, load_line_history};
use pioneer_compaction::{
    Checkpoint, CompactionMode, CompactionPlan, CompactionSettings, CoverageDomain, ModelBudget,
    ModelSelection, OperationSnapshot, SourceRef, Transport,
};
use pioneer_crud::compaction::{CanonicalSource, CommitOutcome, ManifestEntry, SourceAssertion};
use pioneer_provider::{CanonicalProviderRoundEnvelope, ProviderTermination};
use std::collections::{BTreeMap, BTreeSet};

#[tokio::test]
async fn durable_final_response_aliases_survive_checkpoint_cleanup_and_frozen_restart() {
    let thread = "g05-final-thread";
    let turn = "g05-final-turn";
    let (processor, store, workspace) = setup_execution_window_terminal_turn(thread, turn).await;
    let mut answer = ChatMessage::assistant("canonical final answer");
    answer.reasoning_content = Some("canonical reasoning".into());
    answer.provider_replay_state = Some(pioneer_provider::ProviderReplayState::for_model(
        "deepseek",
        "deepseek-reasoner",
        json!({"schema_version":1,"assistant_message":{"reasoning_content":"canonical reasoning","content":"canonical final answer"}}),
    ));
    let envelope = CanonicalProviderRoundEnvelope {
        version: 1,
        round_id: "final-item".into(),
        termination: ProviderTermination::Complete,
        message: answer,
        calls: vec![],
    };
    let hub = pioneer_agent::AgentEventHub::new();
    let mut receiver = hub.take_durable_receiver().await.unwrap();
    let append = hub.publish_durable_and_wait(AgentDurableEvent::TurnProviderHistoryAppended {
        thread_id: thread.into(),
        turn_id: turn.into(),
        item_id: "reasoning-item".into(),
        sequence: 1,
        payload: serde_json::to_value(envelope).unwrap(),
    });
    let commit = async {
        let event = receiver.recv().await.unwrap();
        assert!(processor.handle_durable_agent_event(event).await);
        receiver.acknowledge_last(Ok(()));
    };
    let (ack, ()) = tokio::join!(append, commit);
    ack.unwrap();
    let before_ui = load_line_history(
        &store,
        &workspace,
        thread,
        None,
        &store.compaction_history_read_fence().await.unwrap(),
    )
    .await
    .unwrap();
    assert!(
        !before_ui
            .iter()
            .find(|message| message.provider_replay_state.is_some())
            .unwrap()
            .provenance
            .as_ref()
            .unwrap()
            .complete,
        "ACK alone cannot publish copy aliases before UI revisions exist"
    );
    for item in [
        TurnItem::Reasoning {
            id: "reasoning-item".into(),
            summary: vec![],
            content: vec!["canonical reasoning".into()],
        },
        TurnItem::AgentMessage {
            id: "final-item".into(),
            text: "canonical final answer".into(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
        TurnItem::AgentMessage {
            id: "unrelated-item".into(),
            text: "unrelated neighbor".into(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    ] {
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace.clone(),
                    thread_id: thread.into(),
                    turn_id: turn.into(),
                    item,
                },
                now_timestamp_secs(),
            )
            .await
            .unwrap();
    }
    let fence = store.compaction_history_read_fence().await.unwrap();
    let messages = load_line_history(&store, &workspace, thread, None, &fence)
        .await
        .unwrap();
    assert_eq!(
        messages
            .iter()
            .filter(|m| m.content == "canonical final answer")
            .count(),
        1
    );
    let canonical = messages
        .iter()
        .find(|m| m.provider_replay_state.is_some())
        .unwrap()
        .clone();
    let origin = canonical.provenance.as_ref().unwrap();
    assert_eq!(origin.source_aliases.len(), 2);
    let source = SourceRef {
        scope: origin.sources[0].scope.clone(),
        id: origin.sources[0].id.clone(),
        version: origin.sources[0].version.clone(),
    };
    let selected = load_exact_line_history(
        &store,
        &workspace,
        thread,
        &fence,
        &BTreeSet::from([source.clone()]),
    )
    .await
    .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].content, "canonical final answer");
    assert_eq!(
        selected[0].provenance.as_ref().unwrap().source_aliases,
        origin.source_aliases,
        "selected canonical view retains exact UI metadata without decoding unrelated bodies"
    );
    let neighbor = messages
        .iter()
        .find(|m| m.content == "unrelated neighbor")
        .unwrap();
    let neighbor_source = &neighbor.provenance.as_ref().unwrap().sources[0];
    let selected_neighbor = load_exact_line_history(
        &store,
        &workspace,
        thread,
        &fence,
        &BTreeSet::from([SourceRef {
            scope: neighbor_source.scope.clone(),
            id: neighbor_source.id.clone(),
            version: neighbor_source.version.clone(),
        }]),
    )
    .await
    .unwrap();
    assert_eq!(selected_neighbor.len(), 1);
    assert_eq!(selected_neighbor[0].content, "unrelated neighbor");
    let allowed = BTreeSet::from([thread.to_owned()]);
    let accepted_descriptor = frozen::capture(&store, &workspace, thread, &allowed, &messages)
        .await
        .unwrap();
    let mut hot = canonical.clone();
    hot.provenance = Some(pioneer_agent::compaction::history::pending_origin(
        &workspace,
        thread,
        turn,
        "final-item",
        pioneer_agent::compaction::history::PendingOriginKind::Assistant,
        "reasoning-item",
    ));
    crate::compaction::resolve_message_origins(
        &store,
        &workspace,
        thread,
        turn,
        &allowed,
        std::slice::from_mut(&mut hot),
    )
    .await
    .unwrap();
    assert_eq!(
        hot.provenance.as_ref().unwrap().source_aliases,
        origin.source_aliases
    );
    let descriptor = frozen::capture(&store, &workspace, thread, &allowed, &[canonical.clone()])
        .await
        .unwrap();
    assert_eq!(
        frozen::restore(&store, &workspace, &allowed, &descriptor)
            .await
            .unwrap(),
        vec![canonical]
    );
    let version = store
        .compaction_projection_version(&workspace, thread)
        .await
        .unwrap();
    let owner = format!(
        "native:{}:{workspace}:{}:{thread}",
        workspace.len(),
        thread.len()
    );
    let selection = ModelSelection {
        transport: Transport::Api,
        instance: "fixture".into(),
        model: "fixture".into(),
        effort: None,
    };
    let operation = OperationSnapshot {
        id: "g05-final-operation".into(),
        owner: owner.clone(),
        expected_checkpoint: None,
        projection_version: version,
        source_epochs: BTreeMap::from([(thread.into(), version)]),
        admission: CompactionSettings::default()
            .admit(&selection, None, 0)
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: CoverageDomain::WorkingContext,
            compact: vec![0],
            retain: vec![],
            coverage: vec![source.clone()],
            fingerprint: "g05-final-plan".into(),
        },
    };
    store
        .compaction_admit(&workspace, thread, &operation)
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&operation.id, &descriptor)
        .await
        .unwrap();
    store
        .compaction_prepare_runner(&operation.id, &ModelBudget::new(None, None, None), 1, 0)
        .await
        .unwrap();
    store
        .compaction_append_manifest(
            &operation.id,
            &[ManifestEntry {
                ordinal: 0,
                unit: 0,
                reference_only: false,
                thread_id: thread.into(),
                source: source.clone(),
            }],
        )
        .await
        .unwrap();
    let checkpoint = Checkpoint {
        id: "g05-final-checkpoint".into(),
        operation_id: operation.id.clone(),
        owner,
        previous: None,
        format_version: 1,
        coverage: vec![source.clone()],
        summary: pioneer_compaction::summary::HEADINGS
            .iter()
            .map(|heading| format!("{heading}\nThe final answer is summarized.\n"))
            .collect(),
        selection,
        projection_version: version,
    };
    store
        .compaction_save_candidate(&checkpoint, 0)
        .await
        .unwrap();
    let assertion = SourceAssertion {
        kind: CanonicalSource::ProviderContext,
        turn_id: turn.into(),
        id: source.id.clone(),
        revision: Some(
            source
                .version
                .strip_prefix("revision:")
                .unwrap()
                .parse()
                .unwrap(),
        ),
        payload: store
            .compaction_reference_payload(&workspace, thread, &source)
            .await
            .unwrap()
            .unwrap(),
    };
    assert_eq!(
        store
            .compaction_apply(&checkpoint, None, &[assertion])
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    // Cleanup uses the existing serialized repository writer; UI records remain.
    store.delete_turn_llm_context_for_turn(turn).await.unwrap();
    assert_eq!(store.list_turn_llm_context(turn).await.unwrap().len(), 0);
    for id in ["final-item", "reasoning-item", "unrelated-item"] {
        assert!(
            store.get_turn_item(turn, id).await.unwrap().is_some(),
            "UI event must remain available"
        );
    }
    let accepted = frozen::restore_accepted_history_for_execution(
        &store,
        &workspace,
        None,
        thread,
        &allowed,
        &serde_json::to_string(&accepted_descriptor).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        !accepted
            .messages
            .iter()
            .any(|m| m.content == "canonical final answer" || m.content == "canonical reasoning")
    );
    assert!(
        accepted
            .messages
            .iter()
            .any(|m| m.content == "unrelated neighbor")
    );
    let cold =
        frozen::capture_execution_basis_prepared(&store, &workspace, thread, None, None, None)
            .await
            .unwrap();
    assert!(
        cold.messages
            .iter()
            .any(|m| m.content.contains("The final answer is summarized."))
    );
    assert!(
        cold.messages
            .iter()
            .any(|m| m.content == "unrelated neighbor")
    );
    assert!(
        !cold
            .messages
            .iter()
            .any(|m| m.content == "canonical final answer" || m.content == "canonical reasoning")
    );
    let restored = frozen::restore(&store, &workspace, &allowed, &cold.descriptor)
        .await
        .unwrap();
    assert_eq!(restored, cold.messages);
    let restarted =
        frozen::capture_execution_basis_prepared(&store, &workspace, thread, None, None, None)
            .await
            .unwrap();
    assert_eq!(restarted.messages, cold.messages);
}
