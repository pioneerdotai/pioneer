// Included in preflight::tests to exercise the same provider/fallback fixtures.
use sentry::SentryFutureExt;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;

const FAILURE_CANARY: &str = "PIONEER_A_PRIVATE_CANARY";

#[derive(Debug)]
struct FakeClassifiedFailure {
    text: String,
    classification: pioneer_provider::ProviderFailureClassification,
}

impl fmt::Display for FakeClassifiedFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

impl Error for FakeClassifiedFailure {}

fn classified_response(
    class: ProviderFailureClass,
    http_status: Option<u16>,
    text: &str,
) -> FakePreflightResponse {
    let mut classification = pioneer_provider::ProviderFailureClassification::new(class);
    classification.http_status = http_status;
    classification.provider_code = Some(FAILURE_CANARY.to_owned());
    classification.request_id = Some(
        pioneer_protocol::ProviderRequestId::try_from(format!("req-{FAILURE_CANARY}")).unwrap(),
    );
    FakePreflightResponse::TypedError {
        text: text.to_owned(),
        context: format!("private anyhow context {FAILURE_CANARY}"),
        classification,
    }
}

// Bind both hub and subscriber on every poll, including after Tokio yields.
// No global subscriber/client/consent mutation, no real Sentry transport.
async fn capture_preflight<F: std::future::Future>(
    future: F,
) -> (F::Output, Vec<sentry::protocol::Event<'static>>) {
    let consent = pioneer_observability::telemetry_enabled();
    assert!(
        consent,
        "agent tests retain the default consent without changing it"
    );
    let transport = sentry::test::TestTransport::new();
    let mut options = sentry::ClientOptions::default();
    options.dsn = Some("https://public@sentry.invalid/1".parse().unwrap());
    options.transport = Some(Arc::new(transport.clone()));
    let client = sentry::Client::from(options);
    let hub = Arc::new(sentry::Hub::new(
        Some(Arc::new(client)),
        Arc::new(Default::default()),
    ));
    let subscriber =
        tracing_subscriber::registry().with(pioneer_observability::sentry_tracing_layer());
    let output = async {
        let output = future.await;
        // A local probe exposes breadcrumbs; it is never a preflight ERROR.
        sentry::capture_message("preflight test probe", sentry::Level::Info);
        output
    }
    .with_subscriber(subscriber)
    .bind_hub(hub)
    .await;
    assert_eq!(pioneer_observability::telemetry_enabled(), consent);
    (output, transport.fetch_and_clear_events())
}

fn preflight_events<'a>(
    events: &'a [sentry::protocol::Event<'static>],
) -> Vec<&'a sentry::protocol::Event<'static>> {
    events
        .iter()
        .filter(|event| event.logger.as_deref() == Some("pioneer::turn_preflight"))
        .collect()
}

fn assert_safe_fallback(result: TurnPreflightProviderCallResult) -> String {
    let failure = match &result {
        TurnPreflightProviderCallResult::Failure(failure) => failure,
        other => panic!("expected failure, got {other:?}"),
    };
    assert_eq!(failure.attempts.len(), 1);
    if failure.fallback_reason == TurnPreflightFallbackReason::ProviderError {
        assert_eq!(
            failure.diagnostics[0].code.as_str(),
            "preflight.provider.error"
        );
    }
    assert_eq!(failure.attempts[0].provider_call.attempt, 1);
    let message = failure.diagnostics[0]
        .message
        .as_ref()
        .unwrap()
        .as_str()
        .to_owned();
    assert_eq!(
        failure.attempts[0]
            .diagnostic
            .message
            .as_ref()
            .unwrap()
            .as_str(),
        message
    );
    assert!(message.contains("using local fallback"));
    assert!(!format!("{result:?}").contains(FAILURE_CANARY));
    let modules = sample_provider_needed_modules();
    let plan = compose_turn_preflight_plan(&modules, result);
    assert_eq!(plan.source, TurnPreflightPlanSource::Fallback);
    assert!(plan.diagnostics.preflight_failed);
    assert!(plan.fallback_reason.is_some());
    assert!(
        plan.tools.visible_tools.is_empty(),
        "optional lazy tools remain hidden"
    );
    assert_eq!(
        plan.memory.active_recall.source,
        TurnPreflightPlanSource::Fallback
    );
    assert!(plan.memory.active_recall.decision.provider_fallback_used);
    assert_eq!(
        plan.memory.active_recall.fallback_reason,
        plan.fallback_reason
    );
    let snapshot = build_turn_preflight_diagnostics_snapshot(
        &modules,
        &plan,
        &["exec_command".to_owned(), "request_tools".to_owned()],
    );
    assert!(snapshot.preflight_failed);
    assert!(!snapshot.provider.retry_used);
    assert!(snapshot.tools.requested_tools.is_empty());
    assert!(snapshot.modules["memory.activeRecall"].fallback);
    for serialized in [
        serde_json::to_string(&plan).unwrap(),
        serde_json::to_string(&snapshot).unwrap(),
    ] {
        assert!(!serialized.contains(FAILURE_CANARY));
        assert!(
            serialized.contains(&message),
            "safe description survives plan and snapshot"
        );
    }
    message
}

#[tokio::test]
async fn preflight_diagnostics_warn_classes_are_breadcrumbs_with_one_attempt_and_fallback() {
    for class in [
        ProviderFailureClass::NetworkTransient,
        ProviderFailureClass::RateLimit,
        ProviderFailureClass::AuthExpired,
        ProviderFailureClass::AuthOrPermission,
        ProviderFailureClass::PermissionDenied,
        ProviderFailureClass::ModelNotFound,
    ] {
        for streaming in [false, true] {
            // Identical raw text deliberately disagrees with several typed classes.
            let provider = Arc::new(FakePreflightProvider::new(
                "preflight-provider",
                streaming,
                [classified_response(class, Some(403), FAILURE_CANARY)],
            ));
            let (result, events) =
                capture_preflight(call_turn_preflight_provider(provider_call_input(
                    provider_endpoint(provider.clone(), "preflight-provider", "preflight-model"),
                )))
                .await;
            assert_eq!(provider.requests().len(), 1);
            let message = assert_safe_fallback(result);
            assert!(message.contains("http_status=Some(403)"));
            assert!(message.contains(&format!("class=Some({class:?})")));
            assert!(
                preflight_events(&events).is_empty(),
                "WARN must not become an event"
            );
            let probe = events.last().unwrap();
            let breadcrumbs: Vec<_> = probe
                .breadcrumbs
                .values
                .iter()
                .filter(|b| b.category.as_deref() == Some("pioneer::turn_preflight"))
                .collect();
            assert_eq!(breadcrumbs.len(), 1);
            assert_eq!(breadcrumbs[0].level, sentry::Level::Warning);
            assert_eq!(breadcrumbs[0].message.as_deref(), Some(message.as_str()));
            assert!(
                !serde_json::to_string(&events)
                    .unwrap()
                    .contains(FAILURE_CANARY)
            );
        }
    }
}

#[tokio::test]
async fn preflight_diagnostics_error_classes_and_legacy_unknown_remain_events() {
    for response in [
        classified_response(ProviderFailureClass::Provider5xx, Some(503), FAILURE_CANARY),
        classified_response(ProviderFailureClass::Unknown, None, FAILURE_CANARY),
        classified_response(
            ProviderFailureClass::InvalidRequest,
            Some(400),
            FAILURE_CANARY,
        ),
        FakePreflightResponse::Error(FAILURE_CANARY.to_owned()),
    ] {
        let provider = Arc::new(FakePreflightProvider::new(
            "preflight-provider",
            false,
            [response],
        ));
        let (result, events) =
            capture_preflight(call_turn_preflight_provider(provider_call_input(
                provider_endpoint(provider.clone(), "preflight-provider", "preflight-model"),
            )))
            .await;
        assert_eq!(provider.requests().len(), 1);
        let message = assert_safe_fallback(result);
        let errors = preflight_events(&events);
        assert_eq!(errors.len(), 1, "one report per failed preflight");
        assert_eq!(errors[0].level, sentry::Level::Error);
        assert_eq!(errors[0].message.as_deref(), Some(message.as_str()));
        assert!(
            !serde_json::to_string(&events)
                .unwrap()
                .contains(FAILURE_CANARY)
        );
    }
}

#[tokio::test]
async fn preflight_diagnostics_severity_and_status_do_not_come_from_raw_text() {
    for text in [
        FAILURE_CANARY,
        "HTTP 500 provider exploded",
        "HTTP 429 insufficient_quota",
        "HTTP 403 security policy",
        "HTTP 429 credit_balance_exhausted",
        "OpenRouter upstream pool HTTP 429 nested insufficient_quota",
    ] {
        let provider = Arc::new(FakePreflightProvider::new(
            "preflight-provider",
            false,
            [classified_response(
                ProviderFailureClass::RateLimit,
                None,
                text,
            )],
        ));
        let (result, events) =
            capture_preflight(call_turn_preflight_provider(provider_call_input(
                provider_endpoint(provider, "preflight-provider", "preflight-model"),
            )))
            .await;
        let message = assert_safe_fallback(result);
        assert!(message.contains("class=Some(RateLimit); http_status=None; reason=None"));
        assert!(preflight_events(&events).is_empty());
    }
    // The legacy classifier may classify the text, but may not invent a status.
    let provider =
        FakePreflightProvider::failing("preflight-provider", "HTTP 429 too many requests");
    let (result, _) = capture_preflight(call_turn_preflight_provider(provider_call_input(
        provider_endpoint(provider, "preflight-provider", "preflight-model"),
    )))
    .await;
    assert!(assert_safe_fallback(result).contains("http_status=None; reason=None"));
}

#[tokio::test]
async fn preflight_diagnostics_structured_reason_is_safe_without_quota_inference() {
    let mut response = classified_response(
        ProviderFailureClass::RateLimit,
        Some(429),
        "OpenRouter upstream pool nested insufficient_quota",
    );
    if let FakePreflightResponse::TypedError { classification, .. } = &mut response {
        classification.error_reason =
            Some(pioneer_protocol::ProviderErrorReason::ProviderOverloaded);
    }
    let provider = Arc::new(FakePreflightProvider::new(
        "preflight-provider",
        false,
        [response],
    ));
    let (result, events) = capture_preflight(call_turn_preflight_provider(provider_call_input(
        provider_endpoint(provider, "preflight-provider", "preflight-model"),
    )))
    .await;
    let message = assert_safe_fallback(result);
    assert!(message.contains("http_status=Some(429); reason=Some(ProviderOverloaded)"));
    assert!(preflight_events(&events).is_empty());
}

#[tokio::test]
async fn preflight_diagnostics_403_does_not_block_next_independent_success() {
    let provider = Arc::new(FakePreflightProvider::new(
        "preflight-provider",
        false,
        [
            classified_response(
                ProviderFailureClass::PermissionDenied,
                Some(403),
                FAILURE_CANARY,
            ),
            FakePreflightResponse::Text(r#"{"tools":{"visibleTools":["task_create"]}}"#.to_owned()),
        ],
    ));
    let ((), events) = capture_preflight(async {
        let endpoint = provider_endpoint(provider.clone(), "preflight-provider", "preflight-model");
        assert_safe_fallback(
            call_turn_preflight_provider(provider_call_input(endpoint.clone())).await,
        );
        let result = call_turn_preflight_provider(provider_call_input(endpoint)).await;
        let plan = compose_turn_preflight_plan(&sample_provider_needed_modules(), result);
        assert_eq!(plan.source, TurnPreflightPlanSource::Provider);
        assert!(!plan.diagnostics.preflight_failed);
        assert_eq!(plan.tools.visible_tools, vec!["task_create"]);
        assert_eq!(plan.provider_call.unwrap().attempt, 1);
    })
    .await;
    assert_eq!(provider.requests().len(), 2);
    assert!(preflight_events(&events).is_empty());
}

#[tokio::test]
async fn preflight_diagnostics_warn_breadcrumb_survives_neighboring_unknown_error() {
    let provider = Arc::new(FakePreflightProvider::new(
        "preflight-provider",
        false,
        [
            classified_response(ProviderFailureClass::RateLimit, Some(429), FAILURE_CANARY),
            classified_response(ProviderFailureClass::Unknown, None, FAILURE_CANARY),
        ],
    ));
    let ((), events) = capture_preflight(async {
        let endpoint = provider_endpoint(provider.clone(), "preflight-provider", "preflight-model");
        assert_safe_fallback(
            call_turn_preflight_provider(provider_call_input(endpoint.clone())).await,
        );
        assert_safe_fallback(call_turn_preflight_provider(provider_call_input(endpoint)).await);
    })
    .await;
    assert_eq!(provider.requests().len(), 2);
    let errors = preflight_events(&events);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].level, sentry::Level::Error);
    assert!(
        errors[0]
            .breadcrumbs
            .values
            .iter()
            .any(|b| b.level == sentry::Level::Warning
                && b.category.as_deref() == Some("pioneer::turn_preflight"))
    );
    assert!(
        !serde_json::to_string(&events)
            .unwrap()
            .contains(FAILURE_CANARY)
    );
}

#[tokio::test]
async fn preflight_diagnostics_invalid_responses_do_not_export_body_or_parser_error() {
    for text in [
        format!(r#"{{"{FAILURE_CANARY}":"private response"}}"#),
        json!({"tools":{"visibleTools":[]}, "memory":{"activeRecall":{"durable":{"status": FAILURE_CANARY}}}}).to_string(),
        format!("{{ invalid JSON {FAILURE_CANARY}"),
        FAILURE_CANARY.repeat(TURN_PREFLIGHT_PROVIDER_DEFAULT_MAX_OUTPUT_CHARS),
    ] {
        let provider = FakePreflightProvider::text("preflight-provider", text);
        let (result, events) = capture_preflight(call_turn_preflight_provider(provider_call_input(
            provider_endpoint(provider.clone(), "preflight-provider", "preflight-model"),
        ))).await;
        assert_eq!(provider.requests().len(), 1);
        let message = assert_safe_fallback(result);
        let errors = preflight_events(&events);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].message.as_deref(), Some(message.as_str()));
        assert!(!serde_json::to_string(&events).unwrap().contains(FAILURE_CANARY));
    }
}

#[test]
fn preflight_diagnostics_internal_display_debug_and_error_chain_are_safe() {
    let provider = FakePreflightProvider::text("preflight-provider", "unused");
    let source = anyhow::Error::new(FakeClassifiedFailure {
        text: FAILURE_CANARY.to_owned(),
        classification: match classified_response(
            ProviderFailureClass::RateLimit,
            Some(429),
            FAILURE_CANARY,
        ) {
            FakePreflightResponse::TypedError { classification, .. } => classification,
            _ => unreachable!(),
        },
    })
    .context(format!("private context {FAILURE_CANARY}"));
    let failure = TurnPreflightProviderRequestFailure::provider_error(
        provider.as_ref(),
        "provider_non_stream_response",
        ProviderFailureStage::Connect,
        source,
    )
    .with_response_prefix(FAILURE_CANARY);
    let validation = TurnPreflightProviderRequestFailure::response_validation(
        "non_stream_response_validation",
        anyhow::anyhow!(FAILURE_CANARY).context(FAILURE_CANARY),
        FAILURE_CANARY,
    );
    for failure in [failure, validation] {
        assert!(failure.source().is_none());
        assert!(!format!("{failure}").contains(FAILURE_CANARY));
        assert!(!format!("{failure:?}").contains(FAILURE_CANARY));
        assert_eq!(
            failure.collected_response_chars,
            FAILURE_CANARY.chars().count()
        );
        assert_eq!(failure.collected_response_bytes, FAILURE_CANARY.len());
        let wrapped = anyhow::Error::new(failure);
        for text in [
            format!("{wrapped}"),
            format!("{wrapped:?}"),
            format!("{wrapped:#}"),
        ] {
            assert!(!text.contains(FAILURE_CANARY));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn preflight_diagnostics_default_timeout_preserves_one_attempt_and_fallback() {
    assert_eq!(TURN_PREFLIGHT_PROVIDER_DEFAULT_TIMEOUT_MS, 15_000);
    assert_eq!(TURN_PREFLIGHT_PROVIDER_DEFAULT_MAX_OUTPUT_CHARS, 2_000);
    let provider = FakePreflightProvider::delayed(
        "preflight-provider",
        15_001,
        r#"{"tools":{"visibleTools":[]}}"#,
    );
    let (result, events) = capture_preflight(call_turn_preflight_provider(provider_call_input(
        provider_endpoint(provider.clone(), "preflight-provider", "preflight-model"),
    )))
    .await;
    match &result {
        TurnPreflightProviderCallResult::Failure(failure) => assert_eq!(
            failure.fallback_reason,
            TurnPreflightFallbackReason::Timeout
        ),
        _ => panic!("expected timeout"),
    }
    assert_safe_fallback(result);
    assert_eq!(provider.requests().len(), 1);
    assert!(preflight_events(&events).is_empty());
}

#[tokio::test]
async fn preflight_diagnostics_unwrapped_local_failure_reports_one_safe_error() {
    let provider = FakePreflightProvider::text("preflight-provider", "unused");
    let mut input = provider_call_input(provider_endpoint(
        provider.clone(),
        "preflight-provider",
        "preflight-model",
    ));
    // Exceeds the catalog's unknown-model input budget before provider.chat.
    // This known production failure must retain its preparation stage.
    input.turn.input_text_preview = format!("{FAILURE_CANARY} {}", "word ".repeat(600_000));
    let (result, events) = capture_preflight(call_turn_preflight_provider(input)).await;
    assert!(provider.requests().is_empty());
    let message = assert_safe_fallback(result);
    assert!(message.contains("stage=input_capacity_validation; cause=input_capacity_exceeded; class=None; http_status=None; reason=None"));
    let errors = preflight_events(&events);
    assert_eq!(errors.len(), 1, "inner setup must not double-report");
    assert_eq!(errors[0].level, sentry::Level::Error);
    assert_eq!(
        errors[0].extra.get("stage"),
        Some(&json!("input_capacity_validation"))
    );
    assert_eq!(
        errors[0].extra.get("cause_code"),
        Some(&json!("input_capacity_exceeded"))
    );
    assert_eq!(errors[0].message.as_deref(), Some(message.as_str()));
    assert!(
        !serde_json::to_string(&events)
            .unwrap()
            .contains(FAILURE_CANARY)
    );
}

#[tokio::test(start_paused = true)]
async fn preflight_diagnostics_success_before_default_timeout_uses_provider_plan() {
    let provider = FakePreflightProvider::delayed(
        "preflight-provider",
        14_999,
        sample_provider_plan_json().to_string(),
    );
    let (result, events) = capture_preflight(call_turn_preflight_provider(provider_call_input(
        provider_endpoint(provider.clone(), "preflight-provider", "preflight-model"),
    )))
    .await;
    let plan = compose_turn_preflight_plan(&sample_provider_needed_modules(), result);
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(plan.source, TurnPreflightPlanSource::Provider);
    assert_eq!(plan.fallback_reason, None);
    assert!(!plan.diagnostics.preflight_failed);
    assert_eq!(
        plan.tools.visible_tools,
        vec!["memory_get", "memory_search"]
    );
    assert_eq!(
        plan.memory.active_recall.source,
        TurnPreflightPlanSource::Provider
    );
    assert!(!plan.memory.active_recall.decision.provider_fallback_used);
    assert_eq!(plan.provider_call.unwrap().attempt, 1);
    assert!(preflight_events(&events).is_empty());
}

#[tokio::test]
async fn preflight_diagnostics_preparation_error_conversions_drop_source_and_context() {
    // Exercise the exact conversion functions used by the production map_err
    // call sites without resetting or making the global model catalog fail.
    let source = || {
        anyhow::anyhow!("private preparation source {FAILURE_CANARY}")
            .context(format!("private preparation context {FAILURE_CANARY}"))
    };
    for (failure, stage, cause) in [
        (
            TurnPreflightProviderRequestFailure::model_catalog_error(source()),
            "model_catalog",
            "model_catalog_failed",
        ),
        (
            TurnPreflightProviderRequestFailure::request_projection_error(source()),
            "request_projection",
            "request_projection_failed",
        ),
    ] {
        assert_eq!(failure.description.stage, stage);
        assert_eq!(failure.description.cause_code, cause);
        assert_eq!(failure.description.failure_class, None);
        assert_eq!(failure.description.http_status, None);
        assert_eq!(failure.description.reason, None);
        assert_eq!(failure.collected_response_chars, 0);
        assert_eq!(failure.collected_response_bytes, 0);
        assert_eq!(failure.collected_response_sha256, None);
        assert!(failure.source().is_none());
        assert!(!format!("{failure}").contains(FAILURE_CANARY));
        assert!(!format!("{failure:?}").contains(FAILURE_CANARY));
        let error = anyhow::Error::new(failure);
        assert_eq!(error.chain().count(), 1);
        assert!(!format!("{error:#}").contains(FAILURE_CANARY));
        assert!(!format!("{error:?}").contains(FAILURE_CANARY));
        let provider = FakePreflightProvider::text("preflight-provider", "unused");
        let endpoint = provider_endpoint(provider.clone(), "preflight-provider", "preflight-model");
        let (result, events) = capture_preflight(async {
            // Same reporting/diagnostic boundary used by call_once's Err arm.
            let attempt = turn_preflight_request_attempt_failure(error, &endpoint, 1, 0, 100);
            TurnPreflightProviderCallResult::Failure(TurnPreflightProviderFailure {
                fallback_reason: attempt.fallback_reason,
                diagnostics: vec![attempt.diagnostic.clone()],
                attempts: vec![attempt],
            })
        })
        .await;
        assert!(provider.requests().is_empty());
        let message = assert_safe_fallback(result);
        assert!(message.contains(&format!(
            "stage={stage}; cause={cause}; class=None; http_status=None; reason=None"
        )));
        let errors = preflight_events(&events);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].level, sentry::Level::Error);
        assert_eq!(errors[0].message.as_deref(), Some(message.as_str()));
        assert_eq!(errors[0].extra.get("stage"), Some(&json!(stage)));
        assert_eq!(errors[0].extra.get("cause_code"), Some(&json!(cause)));
        assert!(
            !serde_json::to_string(&events)
                .unwrap()
                .contains(FAILURE_CANARY)
        );
    }
}

#[tokio::test]
async fn preflight_diagnostics_unknown_unwrapped_error_keeps_safe_error_defense() {
    let provider = FakePreflightProvider::text("preflight-provider", "unused");
    let endpoint = provider_endpoint(provider.clone(), "preflight-provider", "preflight-model");
    let (result, events) = capture_preflight(async {
        let error = anyhow::anyhow!("unknown source {FAILURE_CANARY}")
            .context(format!("unknown context {FAILURE_CANARY}"));
        let attempt = turn_preflight_request_attempt_failure(error, &endpoint, 1, 0, 100);
        TurnPreflightProviderCallResult::Failure(TurnPreflightProviderFailure {
            fallback_reason: attempt.fallback_reason,
            diagnostics: vec![attempt.diagnostic.clone()],
            attempts: vec![attempt],
        })
    })
    .await;
    let message = assert_safe_fallback(result);
    assert!(message.contains("stage=provider_request; cause=unclassified_failure; class=None; http_status=None; reason=None"));
    let errors = preflight_events(&events);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].level, sentry::Level::Error);
    assert_eq!(errors[0].message.as_deref(), Some(message.as_str()));
    assert!(provider.requests().is_empty());
    assert!(
        !serde_json::to_string(&events)
            .unwrap()
            .contains(FAILURE_CANARY)
    );
}

#[tokio::test]
async fn preflight_diagnostics_orchestrator_resolution_failure_has_no_request_attempt() {
    let thread_provider = FakePreflightProvider::text("thread-provider", "must not be requested");
    let resolver_calls = Arc::new(Mutex::new(Vec::new()));
    let calls = resolver_calls.clone();
    let registry = ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
        move |workspace, provider| {
            calls
                .lock()
                .unwrap()
                .push((workspace.map(str::to_owned), provider.to_owned()));
            Err(anyhow::anyhow!("private resolver source {FAILURE_CANARY}")
                .context(format!("private resolver context {FAILURE_CANARY}")))
        },
        |_, _| panic!("key resolution failure must stop before proxy resolution"),
        |_, _| panic!("key resolution failure must stop before base URL resolution"),
        pioneer_provider::ProviderTimeoutPolicy::default(),
    );
    let (result, events) = capture_preflight(run_turn_preflight_orchestrator(
        TurnPreflightOrchestratorInput {
            provider_registry: Arc::new(registry),
            workspace_id: "ws_1".to_owned(),
            thread_provider: thread_provider.clone(),
            thread_provider_name: "thread-provider".to_owned(),
            thread_model: "thread-model".to_owned(),
            preflight_provider_name: Some("openai".to_owned()),
            preflight_model: Some("configured-model".to_owned()),
            turn: sample_turn_input(),
            tool_index: sample_tool_index(),
            deterministic_summary: sample_deterministic_summary(),
            active_recall: sample_memory_local_plan_from_memory(
                MemoryActiveRecallPlannerFallbackPolicy::Deterministic,
            ),
            timeout_ms: None,
            max_output_chars: None,
        },
    ))
    .await;
    assert_eq!(
        *resolver_calls.lock().unwrap(),
        vec![(Some("ws_1".to_owned()), "openai".to_owned())]
    );
    assert!(
        thread_provider.requests().is_empty(),
        "must not switch to the thread provider"
    );

    // Resolution precedes the request; its contract has no attempts/call.
    // Keep assert_safe_fallback's one-attempt assertions for request failures.
    let plan = &result.plan;
    assert_eq!(plan.source, TurnPreflightPlanSource::Fallback);
    assert_eq!(
        plan.fallback_reason,
        Some(TurnPreflightFallbackReason::ProviderError)
    );
    assert!(plan.diagnostics.preflight_failed);
    assert!(plan.tools.visible_tools.is_empty());
    assert_eq!(plan.provider_call, None);
    assert_eq!(
        plan.memory.active_recall.source,
        TurnPreflightPlanSource::Fallback
    );
    assert!(plan.memory.active_recall.decision.provider_fallback_used);
    assert_eq!(
        plan.memory.active_recall.fallback_reason,
        plan.fallback_reason
    );
    assert_eq!(
        plan.memory.active_recall.decision.provider_input_chars,
        None
    );
    assert_eq!(
        plan.memory.active_recall.decision.provider_output_chars,
        None
    );
    let resolution_diagnostics: Vec<_> = plan
        .diagnostics
        .diagnostics
        .iter()
        .filter(|d| d.code.as_str() == "preflight.provider.resolve_failed")
        .collect();
    assert_eq!(resolution_diagnostics.len(), 1);
    let message = resolution_diagnostics[0].message.as_ref().unwrap().as_str();
    assert!(message.contains("stage=provider_resolution; cause=provider_resolution_failed; class=None; http_status=None; reason=None"));
    assert!(message.contains("using local fallback"));
    assert!(
        !plan
            .diagnostics
            .diagnostics
            .iter()
            .any(|d| d.code.as_str() == "preflight.provider.error")
    );
    let snapshot = build_turn_preflight_diagnostics_snapshot(
        &result.local_modules,
        plan,
        &["exec_command".to_owned(), "request_tools".to_owned()],
    );
    assert!(snapshot.preflight_failed);
    assert!(snapshot.tools.requested_tools.is_empty());
    assert_eq!(snapshot.provider.final_call, None);
    assert!(!snapshot.provider.retry_used);
    assert!(!snapshot.provider.retry_failed);
    assert_eq!(
        snapshot.provider.final_failure_reason,
        Some(TurnPreflightFallbackReason::ProviderError)
    );
    assert!(snapshot.modules["tools"].fallback);
    assert!(snapshot.modules["memory.activeRecall"].fallback);
    assert!(snapshot.memory.active_recall.provider_fallback_used);
    assert!(
        snapshot
            .provider
            .diagnostics
            .iter()
            .any(|d| d.code.as_str() == "preflight.provider.resolve_failed"
                && d.message.as_ref().unwrap().as_str() == message)
    );
    for serialized in [
        serde_json::to_string(plan).unwrap(),
        serde_json::to_string(&snapshot).unwrap(),
    ] {
        assert!(!serialized.contains(FAILURE_CANARY));
        assert!(serialized.contains(message));
    }
    assert!(!format!("{result:?}").contains(FAILURE_CANARY));
    let errors = preflight_events(&events);
    assert_eq!(errors.len(), 1, "resolver owns the single resolution ERROR");
    assert_eq!(errors[0].level, sentry::Level::Error);
    assert_eq!(errors[0].message.as_deref(), Some(message));
    for (field, value) in [
        ("stage", "provider_resolution"),
        ("cause_code", "provider_resolution_failed"),
        ("provider", "openai"),
        ("model", "configured-model"),
        ("thread_provider", "thread-provider"),
        ("thread_model", "thread-model"),
    ] {
        assert_eq!(errors[0].extra.get(field), Some(&json!(value)));
    }
    for field in ["attempt", "input_chars", "elapsed_ms"] {
        assert!(
            !errors[0].extra.contains_key(field),
            "resolution must not invent request metadata"
        );
    }
    assert!(
        !serde_json::to_string(&events)
            .unwrap()
            .contains(FAILURE_CANARY)
    );
}
