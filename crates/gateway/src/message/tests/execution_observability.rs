//! Real RPC/storage regressions, prepared for external acceptance; never run here.
use super::*;

async fn rpc(
    h: &mut SemanticTimelineQueryHarness,
    view: &str,
    extra: JsonValue,
    suffix: &str,
) -> pioneer_protocol::ExecutionObservabilityResponse {
    let id = generate_test_request_id("observation", suffix);
    let mut params = json!({"threadId":h.sandbox_thread_id,"view":view,"limit":2});
    params
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    loop {
        h.processor.process_request_for_connection(h.connection_id,&json!({"jsonrpc":"2.0","id":id,"method":"thread/execution/observability","params":params}).to_string()).await;
        let response: pioneer_protocol::ExecutionObservabilityResponse =
            serde_json::from_value(recv_response_by_id(&mut h.rx, &id).await.result).unwrap();
        if response.records.is_empty() && response.summary.is_none() {
            if let Some(after) = response.next_after {
                assert!(after > params["after"].as_i64().unwrap_or(0));
                params["after"] = after.into();
                params["through"] = response.through.into();
                continue;
            }
        }
        return response;
    }
}
async fn charge(
    store: &CrudStore,
    workspace: &str,
    owner: &str,
    amount: &str,
) -> crate::usage_journal::Call {
    let usage = pioneer_provider::TokenUsage {
        provider: Some("fixture".into()),
        model: Some("fixture".into()),
        input_tokens: Some(10),
        output_tokens: Some(0),
        accounting: Some(
            json!({"reported_cost":{"amount":serde_json::from_str::<JsonValue>(amount).unwrap(),"amount_decimal":amount,"currency":"USD","provenance":"fixture_vendor_report"}}),
        ),
        ..Default::default()
    };
    let call = crate::usage_journal::Call::start(store, workspace, "title", owner, &usage)
        .await
        .unwrap();
    call.record("completed", &usage).await.unwrap();
    call
}

#[tokio::test]
async fn summary_uses_unique_leaf_own_and_authorized_causal_descendant_charges() {
    let mut h = setup_semantic_timeline_query_harness().await;
    let store = h.processor.crud_store.clone();
    let workspace = store
        .get_thread_by_id(&h.sandbox_thread_id)
        .await
        .unwrap()
        .unwrap()
        .workspace_id;
    charge(&store, &workspace, &h.sandbox_thread_id, "0.03").await;
    charge(&store, &workspace, &h.sandbox_thread_id, "0").await;
    materialize_artifact_api_thread(&store, &workspace, "observe_child", "observe_child_turn")
        .await;
    thread_lineage::Entity::insert(thread_lineage::ActiveModel {
        child_thread_id: Set("observe_child".into()),
        parent_thread_id: Set(h.sandbox_thread_id.clone()),
        root_thread_id: Set(h.sandbox_thread_id.clone()),
        depth: Set(1),
        created_at: Set(chrono::Utc::now().fixed_offset()),
        origin_kind: Set(Some("task_run".into())),
        created_by_thread_id: Set(Some(h.sandbox_thread_id.clone())),
        created_by_turn_id: Set(Some(h.sandbox_turn_id.clone())),
    })
    .exec(&store.database_connection())
    .await
    .unwrap();
    charge(&store, &workspace, "observe_child", "0.02").await;
    let response = rpc(&mut h, "summary", json!({"includeDescendants":true}), "sum").await;
    let summary = response.summary.unwrap();
    assert_eq!(summary["attempts"], 2);
    assert_eq!(summary["own_amounts"][0]["amount_decimal"], "0.03");
    assert_eq!(summary["inclusive_amounts"][0]["amount_decimal"], "0.05");
    assert_eq!(summary["descendant_summary"]["attempts"], 1);
    assert_eq!(summary["allocation"]["all_cost_unallocated"], true);
}

#[tokio::test]
async fn context_rpc_returns_bounded_content_free_breakdown_and_enforces_thread_acl() {
    let mut h = setup_semantic_timeline_query_harness().await;
    let store = h.processor.crud_store.clone();
    let workspace = store
        .get_thread_by_id(&h.sandbox_thread_id)
        .await
        .unwrap()
        .unwrap()
        .workspace_id;
    let request = ChatRequest {
        tool_declarations: None,
        service_mode: None,
        model: "fixture".into(),
        messages: vec![pioneer_provider::ChatMessage::user(
            "CTX\nSECRET_PROMPT_BYTES",
        )],
        temperature: None,
        max_tokens: None,
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: Some(pioneer_provider::CompiledPromptPayload {
            stable_system_text: "SECRET_RULES".into(),
            dynamic_system_text: String::new(),
            boundary_marker: String::new(),
            full_system_text: "SECRET_RULES".into(),
            observation_manifest: Some(json!({"sections":[
                {"source_id":"base","category":"base_system","rendered_bytes":12,"estimated_text_tokens":3,"included_in_provider_instructions":true},
                {"source_id":"turn_context","category":"thread_context","rendered_bytes":3,"estimated_text_tokens":1,"included_in_provider_instructions":false,"delivery_policy":{"channel":"turn_context"}},
                {"source_id":"inventory_reference","category":"skills","rendered_bytes":16,"estimated_text_tokens":4,"included_in_provider_instructions":false,"delivery_policy":{"channel":"turn_context"}}
            ]})),
        }),
    };
    let manifest = pioneer_provider::usage::request_manifest(&request).unwrap();
    store
        .record_execution_observation(
            pioneer_crud::ExecutionObservationInput {
                workspace_id: workspace,
                operation_id: "observed_context".into(),
                revision: 0,
                thread_id: Some(h.sandbox_thread_id.clone()),
                turn_id: Some(h.sandbox_turn_id.clone()),
                owner_kind: "turn".into(),
                owner_id: h.sandbox_turn_id.clone(),
                parent_operation_id: None,
                kind: "provider_attempt".into(),
                purpose: "main".into(),
                provider: Some("fixture".into()),
                model: Some("fixture".into()),
                status: "started".into(),
                recorded_at_ms: 1,
                facts: json!({"context_manifest":manifest,"usage":{"input_tokens":20}}),
                tariff_evidence: None,
            }
            .prepare()
            .unwrap(),
        )
        .await
        .unwrap();
    let response = rpc(
        &mut h,
        "contexts",
        json!({"operationId":"observed_context"}),
        "ctx",
    )
    .await;
    assert_eq!(response.records.len(), 1);
    let serialized = serde_json::to_string(&response).unwrap();
    assert!(!serialized.contains("SECRET_PROMPT_BYTES"));
    assert!(!serialized.contains("SECRET_RULES"));
    let breakdown = &response.records[0].facts["breakdown"];
    let categories = breakdown["categories"].as_array().unwrap();
    assert_eq!(categories.len(), 2);
    assert_eq!(
        categories
            .iter()
            .find(|v| v["category"] == "base_system")
            .unwrap()["text_bytes_utf8_known"],
        "12"
    );
    assert_eq!(
        categories
            .iter()
            .find(|v| v["category"] == "user_message")
            .unwrap()["text_bytes_utf8_known"],
        "23"
    );
    // 12 instruction bytes + 23 bytes in the actual user occurrence. Neither
    // the embedded 3-byte context nor the undelivered reference is extra input.
    assert_eq!(
        categories
            .iter()
            .map(|v| v["text_bytes_utf8_known"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap())
            .sum::<u64>(),
        35
    );
    assert_eq!(
        categories
            .iter()
            .map(|v| v["estimated_text_tokens_known"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap())
            .sum::<u64>(),
        9
    );
    assert_eq!(breakdown["provider_minus_estimated_text_residual"], "11");
    assert_eq!(breakdown["nonadditive_compiled_section_references"], 2);
    assert_eq!(
        response.records[0].facts["context_manifest"]["compiled_sections"]["sections"][2]["footprint_inclusion"],
        "nonadditive_bundle_reference"
    );
    let (tx, mut denied_rx) = crate::session::test_support::outbound_channel(16);
    let denied = h
        .processor
        .session_manager
        .register_connection(tx, authenticated_test_member_collaborator())
        .await
        .unwrap();
    let id = generate_test_request_id("observation", "denied");
    h.processor.process_request_for_connection(denied,&json!({"jsonrpc":"2.0","id":id,"method":"thread/execution/observability","params":{"threadId":h.sandbox_thread_id,"view":"attempt","operationId":"observed_context"}}).to_string()).await;
    let error = recv_error_by_id(&mut denied_rx, &id).await;
    assert_eq!(error.error.message, "not_found");
}

#[tokio::test]
async fn fixed_frontier_pages_keep_operation_positions_while_append_and_updates_continue() {
    let mut h = setup_semantic_timeline_query_harness().await;
    let store = h.processor.crud_store.clone();
    let workspace = store
        .get_thread_by_id(&h.sandbox_thread_id)
        .await
        .unwrap()
        .unwrap()
        .workspace_id;
    charge(&store, &workspace, &h.sandbox_thread_id, "1").await;
    charge(&store, &workspace, &h.sandbox_thread_id, "2").await;
    let first = rpc(
        &mut h,
        "timeline",
        json!({"purpose":"title","limit":1}),
        "page1",
    )
    .await;
    assert_eq!(first.records.len(), 1);
    charge(&store, &workspace, &h.sandbox_thread_id, "3").await;
    let mut seen = std::collections::BTreeSet::from([first.records[0].operation_id.clone()]);
    let mut attempts = u64::from(first.records[0].kind == "provider_attempt");
    let mut cursor = first.next_after;
    for page in 0..16 {
        let Some(after) = cursor else {
            break;
        };
        let response = rpc(
            &mut h,
            "timeline",
            json!({"purpose":"title","limit":1,"after":after,"through":first.through}),
            &format!("page{page}"),
        )
        .await;
        for record in response.records {
            assert!(record.sequence > after && record.sequence <= first.through);
            assert!(seen.insert(record.operation_id));
            if record.kind == "provider_attempt" {
                attempts += 1;
                assert_ne!(
                    record.facts["usage"]["accounting"]["reported_cost"]["amount_decimal"],
                    "3"
                );
            }
        }
        cursor = response.next_after;
    }
    assert!(
        cursor.is_none(),
        "bounded fixture must reach the frozen frontier"
    );
    assert_eq!(attempts, 2);
}

#[tokio::test]
async fn overlapping_work_preserves_union_exclusive_time_and_period_unknowns() {
    let mut h = setup_semantic_timeline_query_harness().await;
    let store = h.processor.crud_store.clone();
    let workspace = store
        .get_thread_by_id(&h.sandbox_thread_id)
        .await
        .unwrap()
        .unwrap()
        .workspace_id;
    for (id, parent, start, end, elapsed) in [
        ("parent", None, 1000, 1100, 100_000_000),
        ("child_a", Some("parent"), 1010, 1070, 60_000_000),
        ("child_b", Some("parent"), 1040, 1090, 50_000_000),
    ] {
        let mut input = pioneer_crud::ExecutionObservationInput {
            workspace_id: workspace.clone(),
            operation_id: id.into(),
            revision: 0,
            thread_id: Some(h.sandbox_thread_id.clone()),
            turn_id: Some(h.sandbox_turn_id.clone()),
            owner_kind: "turn".into(),
            owner_id: h.sandbox_turn_id.clone(),
            parent_operation_id: parent.map(str::to_owned),
            kind: "tool_invocation".into(),
            purpose: "overlap_fixture".into(),
            provider: None,
            model: None,
            status: "running".into(),
            recorded_at_ms: start,
            facts: json!({"observation":{"tsUnixMs":start,"monoNs":0,"processRunId":"same_process"}}),
            tariff_evidence: None,
        };
        store
            .record_execution_observation(input.clone().prepare().unwrap())
            .await
            .unwrap();
        input.revision = 1;
        input.status = "settled".into();
        input.recorded_at_ms = end;
        input.facts =
            json!({"observation":{"tsUnixMs":end,"monoNs":elapsed,"processRunId":"same_process"}});
        store
            .record_execution_observation(input.prepare().unwrap())
            .await
            .unwrap();
    }
    charge(&store, &workspace, &h.sandbox_thread_id, "0.01").await;
    let response = rpc(
        &mut h,
        "summary",
        json!({"purpose":"overlap_fixture"}),
        "overlap",
    )
    .await;
    let time = &response.summary.as_ref().unwrap()["time"];
    assert_eq!(time["observed_union_busy_ms"], "100");
    assert_eq!(time["measured_summed_work_ns"], "210000000");
    assert_eq!(time["summed_observed_exclusive_wall_ms"], "130");
    let period = rpc(
        &mut h,
        "summary",
        json!({"fromWallMs":900,"beforeWallMs":1200}),
        "period",
    )
    .await;
    assert_eq!(period.summary.unwrap()["attempts"], 0);
    assert!(
        period.coverage["period_unplaced_operations_in_scanned_scope"]
            .as_u64()
            .unwrap()
            > 0
    );
}

#[tokio::test]
async fn nested_workspace_journal_usage_remains_visible_in_its_verified_thread_scope() {
    let mut h = setup_semantic_timeline_query_harness().await;
    let store = h.processor.crud_store.clone();
    let workspace = store
        .get_thread_by_id(&h.sandbox_thread_id)
        .await
        .unwrap()
        .unwrap()
        .workspace_id;
    let parent = "tool_parent";
    store
        .record_execution_observation(
            pioneer_crud::ExecutionObservationInput {
                workspace_id: workspace.clone(),
                operation_id: parent.into(),
                revision: 0,
                thread_id: Some(h.sandbox_thread_id.clone()),
                turn_id: Some(h.sandbox_turn_id.clone()),
                owner_kind: "turn".into(),
                owner_id: h.sandbox_turn_id.clone(),
                parent_operation_id: None,
                kind: "tool_invocation".into(),
                purpose: "tool".into(),
                provider: None,
                model: None,
                status: "running".into(),
                recorded_at_ms: 1,
                facts: json!({}),
                tariff_evidence: None,
            }
            .prepare()
            .unwrap(),
        )
        .await
        .unwrap();
    let mut usage = pioneer_provider::TokenUsage {
        input_tokens: Some(10),
        output_tokens: Some(0),
        ..Default::default()
    };
    let call = pioneer_provider::usage::scope_causal_parent(
        parent.into(),
        crate::usage_journal::Call::start(&store, &workspace, "embedding", &workspace, &usage),
    )
    .await
    .unwrap();
    usage.input_tokens = Some(20);
    // A partial cumulative update changes the live journal, not immutable page position.
    call.record("started", &usage).await.unwrap();
    let response = rpc(&mut h, "summary", json!({"purpose":"embedding"}), "nested").await;
    let quantities = response.summary.unwrap()["token_quantities"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(
        quantities
            .iter()
            .find(|v| v["meter"] == "input_tokens")
            .unwrap()["known_sum"],
        "20"
    );
    let id = generate_test_request_id("observation", "workspace_isolation");
    h.processor.process_request_for_connection(h.connection_id,&json!({"jsonrpc":"2.0","id":id,"method":"workspace/execution/observability","params":{"workspaceId":workspace,"view":"summary"}}).to_string()).await;
    let response: pioneer_protocol::WorkspaceExecutionObservabilityResponse =
        serde_json::from_value(recv_response_by_id(&mut h.rx, &id).await.result).unwrap();
    assert_eq!(response.summary.unwrap()["attempts"], 0);
}

#[tokio::test]
async fn selected_amounts_preserve_reported_zero_currency_scopes_partial_and_unknowns() {
    let mut h = setup_semantic_timeline_query_harness().await;
    let store = h.processor.crud_store.clone();
    let workspace = store
        .get_thread_by_id(&h.sandbox_thread_id)
        .await
        .unwrap()
        .unwrap()
        .workspace_id;
    for (provider, currency, amount, estimate, partial) in [
        ("zero", Some("USD"), Some("0"), Some("10"), None),
        ("dollars", Some("USD"), Some("0.1"), Some("20"), None),
        ("vendor_a", Some("credits"), Some("2"), None, None),
        ("vendor_b", Some("credits"), Some("3"), None, None),
        ("unconfirmed_units", None, Some("4"), None, None),
        ("catalog", None, None, Some("0.03"), None),
        ("partial", None, None, None, Some("0.00023")),
    ] {
        let mut accounting = json!({});
        if let Some(amount) = amount {
            accounting["reported_cost"] = json!({"amount":serde_json::from_str::<JsonValue>(amount).unwrap(),"amount_decimal":amount,"currency":currency});
        }
        if let Some(estimate) = estimate {
            accounting["estimated_cost"] = json!({"amount_decimal":estimate,"currency":"USD"});
        }
        if let Some(subtotal) = partial {
            accounting["cost_resolution"] = json!({"status":"partial","known_subtotal_decimal":subtotal,"reasons":["unknown_rate"]});
        }
        let usage = pioneer_provider::TokenUsage {
            provider: Some(provider.into()),
            accounting: Some(accounting),
            ..Default::default()
        };
        let call = crate::usage_journal::Call::start(
            &store,
            &workspace,
            "title",
            &h.sandbox_thread_id,
            &usage,
        )
        .await
        .unwrap();
        call.record("completed", &usage).await.unwrap();
    }
    let summary = rpc(&mut h, "summary", json!({"purpose":"title"}), "basis")
        .await
        .summary
        .unwrap();
    let amounts = summary["own_amounts"].as_array().unwrap();
    assert_eq!(amounts.len(), 5);
    let amount = |currency: &str, basis: &str| {
        amounts
            .iter()
            .find(|v| v["currency"] == currency && v["basis"] == basis)
            .unwrap()["amount_decimal"]
            .as_str()
            .unwrap()
    };
    assert_eq!(amount("USD", "provider_reported"), "0.1");
    assert_eq!(amount("USD", "catalog_estimate"), "0.03");
    assert_eq!(amount("USD", "partial_catalog_estimate"), "0.00023");
    assert_eq!(amount("credits", "provider_reported:vendor_a"), "2");
    assert_eq!(amount("credits", "provider_reported:vendor_b"), "3");
    assert_eq!(summary["unknown_attempts"], 1);
    assert_eq!(summary["partial_attempts"], 1);
    assert_eq!(summary["observed_financial_coverage"]["status"], "partial");
    let zero = rpc(&mut h, "summary", json!({"provider":"zero"}), "zero")
        .await
        .summary
        .unwrap();
    assert_eq!(zero["own_amounts"][0]["amount_decimal"], "0");
    assert_eq!(zero["own_amounts"][0]["basis"], "provider_reported");
}

#[tokio::test]
async fn legacy_reconciliation_survives_read_overlay_redelivery_and_versioned_refinement() {
    let mut h = setup_semantic_timeline_query_harness().await;
    let store = h.processor.crud_store.clone();
    let workspace = store
        .get_thread_by_id(&h.sandbox_thread_id)
        .await
        .unwrap()
        .unwrap()
        .workspace_id;
    let mut legacy = Vec::new();
    for (id, basis) in [
        ("legacy_supported", "observed_billing_identity"),
        ("legacy_reference", "reference_tariff_only"),
    ] {
        let usage = json!({"provider":"fixture","model":"fixture","input_tokens":10,"uncached_input_tokens":10,
            "cache_read_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":2,
            "accounting":{"estimated_cost":{"amount_decimal":"9","currency":"USD"},
                "pricing":{"basis":basis,"version":1,"units":"USD_per_million_tokens","source":null,
                    "source_effective_from":null,"cost":{"input":"2","output":"10","cacheRead":"0.1","cacheWrite":"2.5"}}}});
        let record = pioneer_crud::ProviderUsageObservation {
            id: id.into(),
            workspace_id: workspace.clone(),
            owner_id: h.sandbox_thread_id.clone(),
            operation_kind: "summary".into(),
            status: "completed".into(),
            usage_json: usage.to_string(),
            started_at: 100,
            updated_at: 101,
        };
        store.record_provider_usage(record.clone()).await.unwrap();
        legacy.push(record);
    }
    assert!(
        store
            .with_maintenance_access()
            .backfill_aux_execution_observations_quantum()
            .await
            .unwrap()
            .complete
    );
    let frontier = store
        .execution_workspace_frontier(&workspace)
        .await
        .unwrap();
    let originals = store
        .execution_observation_page(&workspace, &h.sandbox_thread_id, 0, frontier, 100, false)
        .await
        .unwrap();
    let mut supported_evidence = None;
    for (index, record) in legacy.iter().enumerate() {
        let operation = format!("provider:journal:{}", record.id);
        let before = rpc(
            &mut h,
            "attempt",
            json!({"operationId":operation}),
            &format!("legacy_before{index}"),
        )
        .await;
        let evidence = before.records[0].tariff_evidence.clone().unwrap();
        let reconcile_id = generate_test_request_id("observe_reconcile", &format!("first{index}"));
        h.processor.process_request_for_connection(h.connection_id,&json!({"jsonrpc":"2.0","id":reconcile_id,
            "method":"thread/execution/reconcile","params":{"threadId":h.sandbox_thread_id,"operationId":operation,
                "expectedSequence":before.records[0].sequence}}).to_string()).await;
        let result = recv_response_by_id(&mut h.rx, &reconcile_id).await.result;
        assert_eq!(result["changed"], true);
        // Same unversioned source is delivered again after the new resolution.
        // The journal stays unchanged and must not erase derived interpretation.
        store.record_provider_usage(record.clone()).await.unwrap();
        let after = rpc(
            &mut h,
            "attempt",
            json!({"operationId":operation}),
            &format!("legacy_after{index}"),
        )
        .await;
        let row = &after.records[0];
        assert_eq!(row.tariff_evidence.as_ref().unwrap(), &evidence);
        assert_eq!(row.resolution_origin, "historical_reconciliation");
        assert_eq!(
            row.facts["usage_overlay_coverage"],
            "retained_interpretation_same_exact_source"
        );
        let accounting = &row.facts["usage"]["accounting"];
        if index == 0 {
            assert_eq!(accounting["estimated_cost"]["amount_decimal"], "0.00004");
            assert_eq!(accounting["cost_resolution"]["status"], "complete");
            supported_evidence = Some(evidence);
        } else {
            assert_eq!(accounting["cost_resolution"]["status"], "unresolved");
            assert_eq!(
                accounting["unresolved_reasons"][0],
                "unconfirmed_historical_applicability"
            );
            assert!(serde_json::Value::is_null(&accounting["estimated_cost"]));
        }
        let repeat_id = generate_test_request_id("observe_reconcile", &format!("repeat{index}"));
        h.processor.process_request_for_connection(h.connection_id,&json!({"jsonrpc":"2.0","id":repeat_id,
            "method":"thread/execution/reconcile","params":{"threadId":h.sandbox_thread_id,"operationId":operation,
                "expectedSequence":row.sequence}}).to_string()).await;
        assert_eq!(
            recv_response_by_id(&mut h.rx, &repeat_id).await.result["changed"],
            false
        );
        let repeated = rpc(
            &mut h,
            "attempt",
            json!({"operationId":operation}),
            &format!("legacy_repeat{index}"),
        )
        .await;
        assert_eq!(repeated.records[0].sequence, row.sequence);
        assert_eq!(
            repeated.records[0].facts["usage"]["accounting"],
            *accounting
        );
    }
    let summary = rpc(
        &mut h,
        "summary",
        json!({"purpose":"summary"}),
        "legacy_summary",
    )
    .await
    .summary
    .unwrap();
    assert_eq!(summary["attempts"], 2);
    assert_eq!(summary["unknown_attempts"], 1);
    assert_eq!(summary["own_amounts"][0]["amount_decimal"], "0.00004");
    let journals = store
        .provider_usage_page(&workspace, &h.sandbox_thread_id, "", 100)
        .await
        .unwrap();
    for record in &legacy {
        assert_eq!(
            journals
                .iter()
                .find(|row| row.id == record.id)
                .unwrap()
                .usage_json,
            record.usage_json
        );
    }
    let mut updated = legacy[0].clone();
    let mut value: JsonValue = serde_json::from_str(&updated.usage_json).unwrap();
    value["input_tokens"] = 20.into();
    value["uncached_input_tokens"] = 20.into();
    value["_pioneer_observation"] = json!({"revision":1,"bound_thread":true});
    let usage: pioneer_provider::TokenUsage = serde_json::from_value(value.clone()).unwrap();
    let resolution = pioneer_provider::usage::resolve_retained_tariff(
        &usage,
        supported_evidence.as_ref().unwrap(),
    );
    value["accounting"]["cost_resolution"] = resolution;
    value["accounting"]["calculator_version"] = 1.into();
    value["accounting"]["estimated_cost"] = json!({"amount_decimal":"0.00006","currency":"USD"});
    updated.usage_json = value.to_string();
    updated.updated_at += 1;
    store.record_provider_usage(updated.clone()).await.unwrap();
    store.record_provider_usage(updated).await.unwrap();
    let refined = rpc(
        &mut h,
        "attempt",
        json!({"operationId":"provider:journal:legacy_supported"}),
        "legacy_refined",
    )
    .await;
    assert_eq!(refined.records[0].source_revision, 1);
    assert_eq!(refined.records[0].facts["usage"]["input_tokens"], 20);
    // Reading the old immutable frontier still admits a proven newer cumulative
    // source, while an unversioned redelivery above preserved interpretation.
    let live = rpc(
        &mut h,
        "attempt",
        json!({"operationId":"provider:journal:legacy_supported","through":frontier}),
        "legacy_live_refinement",
    )
    .await;
    assert_eq!(live.records[0].source_revision, 0);
    assert_eq!(live.records[0].facts["live_usage_revision"], 1);
    assert_eq!(live.records[0].facts["usage"]["input_tokens"], 20);
    assert_eq!(
        live.records[0].facts["usage"]["accounting"]["estimated_cost"]["amount_decimal"],
        "0.00006"
    );
    let summary = rpc(
        &mut h,
        "summary",
        json!({"purpose":"summary"}),
        "legacy_refined_sum",
    )
    .await
    .summary
    .unwrap();
    assert_eq!(
        summary["attempts"], 2,
        "a new source version is not another charge"
    );
    assert_eq!(summary["own_amounts"][0]["amount_decimal"], "0.00006");
    let versions = store
        .execution_observation_page(
            &workspace,
            &h.sandbox_thread_id,
            0,
            store
                .execution_workspace_frontier(&workspace)
                .await
                .unwrap(),
            100,
            false,
        )
        .await
        .unwrap();
    for original in originals {
        assert_eq!(
            versions
                .iter()
                .find(|row| row.sequence == original.sequence)
                .unwrap()
                .facts_json,
            original.facts_json
        );
    }
}

#[tokio::test]
async fn auxiliary_spawned_decoder_body_eof_reaches_the_drain_owner_and_journal() {
    use futures_util::StreamExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut h = setup_semantic_timeline_query_harness().await;
    let store = h.processor.crud_store.with_maintenance_access();
    let workspace = store
        .get_thread_by_id(&h.sandbox_thread_id)
        .await
        .unwrap()
        .unwrap()
        .workspace_id;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (release, body_gate) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 8192];
        let _ = socket.read(&mut request).await.unwrap();
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
        // Keep HTTP body EOF after stream_chat returns and its observation
        // receiver closes, as happens with the real spawned Responses decoder.
        body_gate.await.unwrap();
        socket.write_all(b"0\r\n\r\n").await.unwrap();
    });
    let registry = pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        Arc::new(pioneer_provider::providers::OpenAiProvider::with_base_url(
            "fixture", base,
        )),
    );
    let provider = crate::usage_journal::observe(
        registry
            .get_or_create_for_workspace(&workspace, "openai")
            .unwrap(),
        &store,
        &workspace,
        "summary",
        &h.sandbox_thread_id,
    );
    let mut stream = provider
        .stream_chat(ChatRequest {
            tool_declarations: None,
            service_mode: None,
            model: "gpt-5.4".into(),
            messages: vec![pioneer_provider::ChatMessage::user("fixture")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        })
        .await
        .unwrap();
    let initial = store
        .provider_usage_page(&workspace, &h.sandbox_thread_id, "", 100)
        .await
        .unwrap();
    let initial_usage: JsonValue = serde_json::from_str(&initial[0].usage_json).unwrap();
    release.send(()).unwrap();
    let mut errors = 0;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while let Some(chunk) = stream.next().await {
            if chunk.is_err() {
                errors += 1;
            }
        }
    })
    .await
    .expect("auxiliary drain must not wait for the closed open-scope ACK receiver");
    // HTTP EOF is real, but no protocol terminal arrived. Preserve that provider
    // failure rather than inventing successful normalized/provider completion.
    assert_eq!(errors, 1);
    server.await.unwrap();
    let final_rows = store
        .provider_usage_page(&workspace, &h.sandbox_thread_id, "", 100)
        .await
        .unwrap();
    assert_eq!(final_rows.len(), 1);
    assert_eq!(final_rows[0].status, "failed");
    let usage: JsonValue = serde_json::from_str(&final_rows[0].usage_json).unwrap();
    assert_eq!(
        usage["physical_attempt_id"],
        initial_usage["physical_attempt_id"]
    );
    assert_eq!(
        usage["accounting"]["transport_observation_coverage"]["body_eof_persisted"],
        1
    );
    let parent = format!("provider:journal:{}", initial[0].id);
    let frontier = store
        .execution_workspace_frontier(&workspace)
        .await
        .unwrap();
    let children = store
        .execution_children_page(
            &workspace,
            Some(&h.sandbox_thread_id),
            &parent,
            0,
            frontier,
            32,
        )
        .await
        .unwrap();
    let facts = children
        .iter()
        .map(|row| serde_json::from_str::<JsonValue>(&row.facts_json).unwrap())
        .collect::<Vec<_>>();
    let body = facts
        .iter()
        .filter(|fact| fact["phase"] == "provider_body_finished")
        .collect::<Vec<_>>();
    assert_eq!(body.len(), 1);
    assert_eq!(
        body[0]["physical_attempt_id"],
        initial_usage["physical_attempt_id"]
    );
    // Real RPC detail sees the same durable body milestone after open→drain.
    let detail = rpc(
        &mut h,
        "attempt",
        json!({"operationId":parent}),
        "aux_body_eof",
    )
    .await;
    assert_eq!(
        detail.records[0].facts["usage"]["accounting"]["transport_observation_coverage"]["body_eof_persisted"],
        1
    );
}
