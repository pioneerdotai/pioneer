use super::*;

// The real native request envelope is the independent oracle for JSON escaping
// and rounding. No production character-size formula is copied into the test.
#[tokio::test]
async fn estimated_portions_preserve_escaped_unicode_and_source_cursor() {
    let source = format!("BEGIN{}END", "🌍漢e\u{301}\"\\\n\u{0}".repeat(200));
    let f = fixture(&source, vec![], true, false).await;
    let mut state = f
        .store
        .compaction_runner_state("operation")
        .await
        .unwrap()
        .unwrap();
    let fixed = f
        .runner
        .summarizer
        .input_tokens(&f.runner.request(&state, String::new()))
        .unwrap();
    let ceiling = fixed + 180;
    state.overflow_input_ceiling = Some(ceiling);
    let mut rebuilt = String::new();
    let mut finished = false;
    for _ in 0..128 {
        let portion = f
            .runner
            .portion(&state, AttemptPurpose::Portion)
            .await
            .unwrap();
        let actual = f.runner.summarizer.input_tokens(&portion.request).unwrap();
        assert!(actual <= ceiling);
        assert!(portion.request.input.reference_only.is_empty());
        let text = portion
            .request
            .input
            .compact_units
            .iter()
            .map(|part| part.text.as_str())
            .collect::<String>();
        assert!(!text.is_empty());
        rebuilt.push_str(&text);
        if portion.final_portion {
            assert_eq!(portion.completed, f.runner.snapshot.plan.coverage);
            finished = true;
            break;
        }
        assert!(
            portion.completed.is_empty(),
            "partial source is never coverage"
        );
        assert!(portion.cursor > state.cursor);
        assert_eq!(portion.cursor.character, rebuilt.chars().count() as u64);
        state.cursor = portion.cursor;
        state.source_text_projection_version = portion.source_text_projection_version;
    }
    assert!(finished, "bounded Unicode source must finish");
    assert_eq!(rebuilt, f.model_text);
    assert!(f.provider.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn confirmed_summary_overflow_decreases_requests_without_publishing_coverage() {
    // Leave capacity for fixed instructions even after all four reductions;
    // the ordinary small-window fixture can exhaust capacity before call five.
    let f = fixture_with_canonical_payloads_observer_and_budget(
        vec![canonical_agent_message_event_payload(
            "fixture-agent-message",
            &"escaped \" 🌍\n".repeat(8_000),
        )],
        vec![
            Reply::Overflow,
            Reply::Overflow,
            Reply::Overflow,
            Reply::Overflow,
            Reply::Overflow,
        ],
        true,
        false,
        None,
        ModelBudget::new(Some(64_000), None, None),
    )
    .await;
    let exit = f.runner.run(CancellationToken::new()).await.unwrap();
    assert!(matches!(
        exit,
        CompactionExit::Failed(FailureKind::InputOverflow)
    ));
    let requests = f.provider.calls.lock().unwrap();
    assert_eq!(requests.len(), 5);
    let mut prior = usize::MAX;
    for request in requests.iter() {
        // These service requests have identical fixed framing. Unicode source
        // prefixes carry no discarded suffix or synthetic replacement text.
        let input: SummaryInput = serde_json::from_str(&request.messages[1].content).unwrap();
        let bytes = request.messages[1].content.len();
        assert!(bytes < prior, "every confirmed-overflow retry must shrink");
        prior = bytes;
        assert_eq!(input.previous_summary, "");
        assert!(input.reference_only.is_empty());
        assert!(f.model_text.starts_with(&input.compact_units[0].text));
    }
    drop(requests);
    assert_eq!(f.provider.active.load(Ordering::SeqCst), 0);
    assert!(f.store.compaction_head("owner").await.unwrap().is_none());
    let state = f
        .store
        .compaction_runner_state("operation")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.cursor, SourceCursor::default());
    assert!(state.previous_checkpoint.is_none());
    assert_eq!(state.overflow_rejections, 4);
    assert_eq!(state.portion_attempts, 5);
}

#[test]
fn cli_summary_replacement_size_covers_nested_escaping_and_retained_media() {
    use pioneer_agent::compaction::request::{MediaEstimate, NativeRequestProjection};
    use pioneer_provider::{ChatMessage, MessageAttachment, MessageContentPart};
    let mut retained = ChatMessage::user("retained history 🌍");
    retained
        .content_parts
        .push(MessageContentPart::image(MessageAttachment::from_url(
            "https://fixture.invalid/image.png",
            "image/png",
        )));
    let budget = ModelBudget::new(Some(8192), None, Some(128));
    let projection = NativeRequestProjection::new(
        ChatRequest {
            model: "fixture".into(),
            messages: vec![ChatMessage::user("old history"), retained],
            temperature: None,
            max_tokens: Some(128),
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        },
        [0],
        vec![MediaEstimate {
            message: 1,
            part: 0,
            input_tokens: 1,
        }],
        budget,
        false,
    )
    .unwrap();
    let runtime = pioneer_protocol::CLIAgentRuntimeKind::Codex;
    let base = projection.evaluate("").unwrap();
    let (fixed, _, _) = crate::cli_runtime::context::completed_cli_history_budget(
        &base.request.messages,
        "ws",
        None,
        runtime,
    )
    .unwrap();
    for summary in ["", "a", "🌍漢e\u{301}\"\\\n\u{0}", "line\nquote\""].map(|text| text.repeat(12))
    {
        let candidate = projection.evaluate(&summary).unwrap();
        let (actual, _, media) = crate::cli_runtime::context::completed_cli_history_budget(
            &candidate.request.messages,
            "ws",
            None,
            runtime,
        )
        .unwrap();
        let estimate = PortionSizing::new(fixed)
            .with(summary_component_units(&summary).unwrap().saturating_sub(4));
        assert!(actual <= estimate && estimate <= actual + 1);
        assert_eq!(media.len(), 1);
        assert_eq!(candidate.request.messages[1], base.request.messages[1]);
    }
}
