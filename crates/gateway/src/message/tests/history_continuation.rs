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

/// Enter the registry's production authority scope without calling a model.
/// The fixture uses the same async history preparation as native adapters.
struct HistoryPreparationProvider {
    name: String,
}

#[async_trait::async_trait]
impl pioneer_provider::Provider for HistoryPreparationProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn prepare_input_budget(
        &self,
        mut request: ChatRequest,
    ) -> anyhow::Result<pioneer_provider::attachments::PreparedInputBudget> {
        let prepared = pioneer_provider::attachments::prepare_messages_for_provider_async(
            self.name(),
            &request.model,
            &self.capabilities(),
            &request.messages,
        )
        .await?;
        assert!(
            prepared.attachments.is_empty(),
            "history fixture has no media"
        );
        request.messages = prepared.messages;
        Ok(pioneer_provider::attachments::PreparedInputBudget {
            request,
            media: vec![],
        })
    }

    async fn chat(&self, _request: ChatRequest) -> anyhow::Result<ChatResponse> {
        anyhow::bail!("history preparation fixture must not call a model")
    }

    async fn stream_chat(
        &self,
        _request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        anyhow::bail!("history preparation fixture must not call a model")
    }
}

async fn prepare_history_for_provider(
    provider: &str,
    model: &str,
    messages: &[ChatMessage],
) -> Vec<ChatMessage> {
    let registry = pioneer_provider::ProviderRegistry::with_provider(
        provider,
        Arc::new(HistoryPreparationProvider {
            name: provider.into(),
        }),
    );
    let adapter = registry.get_or_create(provider).unwrap();
    assert!(adapter.authority_fingerprint().is_some());
    adapter
        .prepare_input_budget(ChatRequest {
            model: model.into(),
            messages: messages.to_vec(),
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        })
        .await
        .unwrap()
        .request
        .messages
}

#[tokio::test]
async fn durable_final_response_aliases_survive_checkpoint_cleanup_and_frozen_restart() {
    for (provider, model, native, reasoning, ui_final, content) in [
        (
            "deepseek",
            "deepseek-reasoner",
            json!({"schema_version":1,"assistant_message":{"reasoning_content":"canonical reasoning","content":"canonical final answer"}}),
            "canonical reasoning",
            "canonical final answer",
            "canonical final answer",
        ),
        (
            "anthropic",
            "claude-sonnet-4-6",
            json!({"schema_version":2,"blocks":[{"type":"text","text":"canonical final answer"}]}),
            "",
            "canonical final answer",
            "canonical final answer",
        ),
        (
            "gemini",
            "gemini-2.5-flash",
            json!({"schema_version":2,"parts":[{"text":"canonical final answer"}]}),
            "",
            "canonical final answer",
            "canonical final answer",
        ),
        (
            "anthropic",
            "claude-sonnet-4-6",
            json!({"schema_version":2,"blocks":[{"type":"redacted_thinking","data":"synthetic opaque"},{"type":"text","text":"canonical final answer"}]}),
            "",
            "canonical final answer",
            "canonical final answer",
        ),
        (
            "deepseek",
            "deepseek-reasoner",
            json!({"schema_version":1,"assistant_message":{"reasoning_content":"","content":"canonical final answer"}}),
            "",
            "canonical final answer",
            "canonical final answer",
        ),
        (
            "deepseek",
            "deepseek-reasoner",
            json!({"schema_version":1,"assistant_message":{"reasoning_content":"canonical reasoning","content":""}}),
            "canonical reasoning",
            " \n",
            "",
        ),
        (
            "deepseek",
            "deepseek-reasoner",
            json!({"schema_version":1,"assistant_message":{"reasoning_content":"canonical reasoning","content":""}}),
            "canonical reasoning",
            "",
            "",
        ),
        // Native text is available even though both portable UI copies are omitted.
        (
            "gemini",
            "gemini-2.5-flash",
            json!({"schema_version":2,"parts":[{"text":"native answer"}]}),
            "",
            "",
            "",
        ),
    ] {
        durable_final_case(provider, model, native, reasoning, ui_final, content).await;
    }
}

async fn durable_final_case(
    provider: &str,
    model: &str,
    native: serde_json::Value,
    reasoning: &str,
    ui_final: &str,
    content: &str,
) {
    let thread = "g05-final-thread";
    let turn = "g05-final-turn";
    let (processor, store, workspace) = setup_execution_window_terminal_turn(thread, turn).await;
    let mut answer = ChatMessage::assistant(content);
    answer.reasoning_content = (!reasoning.is_empty()).then(|| reasoning.to_owned());
    answer.provider_replay_state = Some(pioneer_provider::ProviderReplayState::for_model(
        provider, model, native,
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
    let pending_fence = store.compaction_history_read_fence().await.unwrap();
    let before_ui = load_line_history(&store, &workspace, thread, None, &pending_fence)
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
    let pending_origin = before_ui
        .iter()
        .find(|m| m.provider_replay_state.is_some())
        .unwrap()
        .provenance
        .as_ref()
        .unwrap();
    let pending_source = SourceRef {
        scope: pending_origin.sources[0].scope.clone(),
        id: pending_origin.sources[0].id.clone(),
        version: pending_origin.sources[0].version.clone(),
    };
    let row = store.list_turn_llm_context(turn).await.unwrap().remove(0);
    let recovered_pending = crate::resilience::recovered_final_origin_for_test(
        &store,
        &workspace,
        thread,
        turn,
        &row,
        &pending_source,
    )
    .await
    .unwrap();
    assert!(!recovered_pending.complete);
    // Started records with the expected IDs cannot discharge the boundary.
    for item in [
        TurnItem::Reasoning {
            id: "reasoning-item".into(),
            summary: vec![],
            content: vec![],
        },
        TurnItem::AgentMessage {
            id: "final-item".into(),
            text: String::new(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    ] {
        store
            .materialize_item_started(
                ItemStartedNotification {
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
    let started = load_line_history(
        &store,
        &workspace,
        thread,
        None,
        &store.compaction_history_read_fence().await.unwrap(),
    )
    .await
    .unwrap();
    let started_canonical = started
        .iter()
        .find(|m| m.provider_replay_state.is_some())
        .unwrap();
    assert!(!started_canonical.provenance.as_ref().unwrap().complete);
    for mode in [CompactionMode::Normal, CompactionMode::Emergency] {
        let layout = pioneer_agent::compaction::history::NativeHistoryLayout::from_messages(
            &workspace,
            thread,
            std::slice::from_ref(started_canonical),
            &[100],
        )
        .unwrap();
        assert!(
            pioneer_compaction::plan_compaction(
                &layout.units,
                &ModelBudget::new(Some(32768), None, None),
                256,
                0,
                512,
                mode,
                CoverageDomain::WorkingContext,
                true,
                "pending"
            )
            .is_err()
        );
    }
    for item in [
        TurnItem::Reasoning {
            id: "reasoning-item".into(),
            summary: vec![],
            content: vec![reasoning.into()],
        },
        TurnItem::AgentMessage {
            id: "final-item".into(),
            text: ui_final.into(),
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
    let old_fence = load_line_history(&store, &workspace, thread, None, &pending_fence)
        .await
        .unwrap();
    assert!(
        !old_fence
            .iter()
            .find(|m| m.provider_replay_state.is_some())
            .unwrap()
            .provenance
            .as_ref()
            .unwrap()
            .complete,
        "later completion cannot escape the captured fence"
    );
    let fence = store.compaction_history_read_fence().await.unwrap();
    let messages = load_line_history(&store, &workspace, thread, None, &fence)
        .await
        .unwrap();
    assert_eq!(messages.iter().filter(|m| m.content == content).count(), 1);
    let canonical = messages
        .iter()
        .find(|m| m.provider_replay_state.is_some())
        .unwrap()
        .clone();
    let origin = canonical.provenance.as_ref().unwrap();
    assert!(
        origin.complete,
        "completed omitted UI copies must not hold a final pending"
    );
    assert_eq!(
        origin.source_aliases.len(),
        usize::from(!reasoning.trim().is_empty()) + usize::from(!ui_final.trim().is_empty())
    );
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
    assert!(selected[0].provenance.as_ref().unwrap().complete);
    assert_eq!(selected[0].content, content);
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
    // Actual provider preparation sees one native answer before publication.
    let prepared = prepare_history_for_provider(provider, model, &messages).await;
    assert_eq!(
        prepared
            .iter()
            .filter(|m| m.provider_replay_state.is_some())
            .count(),
        1
    );
    for mode in [CompactionMode::Normal, CompactionMode::Emergency] {
        let layout = pioneer_agent::compaction::history::NativeHistoryLayout::from_messages(
            &workspace,
            thread,
            &messages,
            &vec![100; messages.len()],
        )
        .unwrap();
        assert!(layout.units.iter().all(|unit| unit.complete));
        let plan = pioneer_compaction::plan_compaction(
            &layout.units,
            &ModelBudget::new(Some(32768), None, None),
            256,
            0,
            512,
            mode,
            CoverageDomain::WorkingContext,
            true,
            "g05-completed-ui",
        )
        .unwrap();
        assert!(
            plan.coverage.contains(&source),
            "final eligible in {mode:?}"
        );
    }
    let row = store
        .list_turn_llm_context(turn)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.source == "assistant_round")
        .unwrap();
    let recovered = crate::resilience::recovered_final_origin_for_test(
        &store, &workspace, thread, turn, &row, &source,
    )
    .await
    .unwrap();
    assert!(recovered.complete);
    assert_eq!(recovered.source_aliases, origin.source_aliases);
    let mut recovered_message = canonical.clone();
    recovered_message.provenance = Some(recovered);
    let recovered_layout = pioneer_agent::compaction::history::NativeHistoryLayout::from_messages(
        &workspace,
        thread,
        &[recovered_message],
        &[100],
    )
    .unwrap();
    assert!(recovered_layout.units[0].complete);
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
    assert!(hot.provenance.as_ref().unwrap().complete);
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
    // Normal cleanup removes transient context, retaining canonical history
    // sources even after publication. The checkpoint projection, not deletion
    // of those sources, prevents the UI answer from reappearing on restart.
    store
        .insert_turn_llm_context(pioneer_crud::NewTurnLlmContextEntry {
            turn_id: turn.into(),
            item_id: None,
            attempt_id: None,
            sequence: 2,
            source: "tool_result".into(),
            tool_name: None,
            payload: "temporary context".into(),
            output_policy_snapshot: "{}".into(),
            created_at: chrono::Utc::now().fixed_offset(),
            expires_at: None,
        })
        .await
        .unwrap();
    assert_eq!(
        store.delete_turn_llm_context_for_turn(turn).await.unwrap(),
        1
    );
    let retained = store.list_turn_llm_context(turn).await.unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].id, row.id);
    assert_eq!(retained[0].payload, row.payload);
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
            .any(|m| m.provider_replay_state.is_some()
                || (!ui_final.trim().is_empty() && m.content == ui_final)
                || m.content.contains("canonical reasoning"))
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
            .any(|m| m.provider_replay_state.is_some()
                || (!ui_final.trim().is_empty() && m.content == ui_final)
                || m.content.contains("canonical reasoning"))
    );
    let prepared = prepare_history_for_provider(provider, model, &cold.messages).await;
    assert!(prepared.iter().all(|m| m.provider_replay_state.is_none()));
    assert!(
        !prepared
            .iter()
            .any(|m| (!ui_final.trim().is_empty() && m.content == ui_final)
                || m.content.contains("canonical reasoning")),
        "summary must not replay the original visible UI answer/reasoning"
    );
    assert!(prepared.iter().any(|m| m.content == "unrelated neighbor"));
    let layout = pioneer_agent::compaction::history::NativeHistoryLayout::from_messages(
        &workspace,
        thread,
        &cold.messages,
        &vec![100; cold.messages.len()],
    )
    .unwrap();
    assert!(layout.units.iter().all(|unit| unit.complete));
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

#[tokio::test]
async fn unrelated_technical_completion_and_foreign_identity_do_not_discharge_final_boundary() {
    let thread = "g05-negative-thread";
    let turn = "g05-negative-turn";
    let (processor, store, workspace) = setup_execution_window_terminal_turn(thread, turn).await;
    let mut answer = ChatMessage::assistant("answer");
    answer.provider_replay_state = Some(pioneer_provider::ProviderReplayState::for_model(
        "gemini",
        "gemini-2.5-flash",
        json!({"schema_version":2,"parts":[{"text":"answer"}]}),
    ));
    let hub = pioneer_agent::AgentEventHub::new();
    let mut receiver = hub.take_durable_receiver().await.unwrap();
    let append = hub.publish_durable_and_wait(AgentDurableEvent::TurnProviderHistoryAppended {
        thread_id: thread.into(),
        turn_id: turn.into(),
        item_id: "expected-reasoning".into(),
        sequence: 1,
        payload: serde_json::to_value(CanonicalProviderRoundEnvelope {
            version: 1,
            round_id: "expected-final".into(),
            termination: ProviderTermination::Complete,
            message: answer,
            calls: vec![],
        })
        .unwrap(),
    });
    let commit = async {
        assert!(
            processor
                .handle_durable_agent_event(receiver.recv().await.unwrap())
                .await
        );
        receiver.acknowledge_last(Ok(()));
    };
    let (ack, ()) = tokio::join!(append, commit);
    ack.unwrap();
    for item in [
        // Same expected identity, wrong typed completion: technical is not proof.
        context_compaction_item("expected-reasoning"),
        TurnItem::Reasoning {
            id: "foreign-reasoning".into(),
            summary: vec![],
            content: vec![],
        },
        TurnItem::AgentMessage {
            id: "expected-final".into(),
            text: "answer".into(),
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
    let canonical = messages
        .iter()
        .find(|m| m.provider_replay_state.is_some())
        .unwrap();
    let origin = canonical.provenance.as_ref().unwrap();
    assert!(!origin.complete);
    assert_eq!(origin.source_aliases.len(), 1);
    let source = SourceRef {
        scope: origin.sources[0].scope.clone(),
        id: origin.sources[0].id.clone(),
        version: origin.sources[0].version.clone(),
    };
    let exact = load_exact_line_history(
        &store,
        &workspace,
        thread,
        &fence,
        &BTreeSet::from([source.clone()]),
    )
    .await
    .unwrap();
    assert!(!exact[0].provenance.as_ref().unwrap().complete);
    let row = store.list_turn_llm_context(turn).await.unwrap().remove(0);
    assert!(
        !crate::resilience::recovered_final_origin_for_test(
            &store, &workspace, thread, turn, &row, &source
        )
        .await
        .unwrap()
        .complete
    );
    let allowed = BTreeSet::from([thread.to_owned()]);
    let mut hot = canonical.clone();
    hot.provenance = Some(pioneer_agent::compaction::history::pending_origin(
        &workspace,
        thread,
        turn,
        "expected-final",
        pioneer_agent::compaction::history::PendingOriginKind::Assistant,
        "expected-reasoning",
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
    assert!(!hot.provenance.as_ref().unwrap().complete);
    // Foreign source scopes/revisions cannot authorize the canonical projection.
    for invalid in [
        SourceRef {
            scope: "context:foreign-turn".into(),
            ..source.clone()
        },
        SourceRef {
            version: "revision:999999".into(),
            ..source.clone()
        },
    ] {
        assert!(
            store
                .compaction_reference_payload(&workspace, thread, &invalid)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(
        store
            .compaction_reference_payload("foreign-workspace", thread, &source)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_reference_payload(&workspace, "foreign-thread", &source)
            .await
            .unwrap()
            .is_none()
    );
    for mode in [CompactionMode::Normal, CompactionMode::Emergency] {
        let layout = pioneer_agent::compaction::history::NativeHistoryLayout::from_messages(
            &workspace,
            thread,
            std::slice::from_ref(canonical),
            &[100],
        )
        .unwrap();
        assert!(
            pioneer_compaction::plan_compaction(
                &layout.units,
                &ModelBudget::new(Some(32768), None, None),
                256,
                0,
                512,
                mode,
                CoverageDomain::WorkingContext,
                true,
                "negative"
            )
            .is_err()
        );
    }
}

#[tokio::test]
async fn ordinary_ui_only_final_keeps_existing_cold_history_eligibility() {
    let thread = "g05-ordinary-thread";
    let turn = "g05-ordinary-turn";
    let (_, store, workspace) = setup_execution_window_terminal_turn(thread, turn).await;
    store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: workspace.clone(),
                thread_id: thread.into(),
                turn_id: turn.into(),
                item: TurnItem::AgentMessage {
                    id: "ordinary-final".into(),
                    text: "ordinary answer".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            now_timestamp_secs(),
        )
        .await
        .unwrap();
    let messages = load_line_history(
        &store,
        &workspace,
        thread,
        None,
        &store.compaction_history_read_fence().await.unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        messages
            .iter()
            .filter(|m| m.content == "ordinary answer")
            .count(),
        1
    );
    assert!(messages.iter().all(|m| m.provider_replay_state.is_none()));
    assert!(store.list_turn_llm_context(turn).await.unwrap().is_empty());
    let layout = pioneer_agent::compaction::history::NativeHistoryLayout::from_messages(
        &workspace,
        thread,
        &messages,
        &vec![100; messages.len()],
    )
    .unwrap();
    assert!(layout.units.iter().all(|u| u.complete));
}
