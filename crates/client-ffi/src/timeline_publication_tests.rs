use crate::client_binding::{publication_dto, snapshot_dto};
use pioneer_client::{
    core::{ClientChangeSequence, ClientCore, ClientScope},
    timeline::semantic::{TopLevelPageMergeMode, WorkPageMergeMode},
};
use pioneer_protocol::*;
use serde_json::{Value, json};
use std::{num::NonZeroUsize, sync::Arc};

fn fixture_core() -> Arc<ClientCore> {
    let core = Arc::new(ClientCore::new());
    let source: Value =
        serde_json::from_str(include_str!("../tests/fixtures/thread-registry-wire.json")).unwrap();
    core.upsert_thread(
        serde_json::from_value(source["initial"][0]["payload"]["thread"].clone()).unwrap(),
    );
    core
}
fn block(id: &str, turn: &str, sort: &str, kind: Value) -> TimelineBlock {
    serde_json::from_value(json!({"workspaceId":"ws", "threadId":"a", "blockId":id, "turnId":turn, "sortKey":sort, "startedAtUnixMs":1000, "updatedAtUnixMs":2000, "kind":kind})).unwrap()
}
fn install(core: &ClientCore, message: &str) {
    let task = json!({"id":"task-anchor", "taskId":"task", "title":"Background analysis", "status":"running", "attachment":"detached", "triggerKind":"immediate", "executorKind":"agent", "depth":0, "maxDepth":3, "createdAt":1, "updatedAt":2});
    let work: TurnWorkBlock = serde_json::from_value(json!({"turnId":"work", "presentation":"collapsed_after_final", "state":"completed", "startedAtUnixMs":1000, "completedAtUnixMs":2000, "elapsedMs":1000, "workCount":8, "visibleWorkCount":8, "hiddenWorkCount":0, "hasMoreBefore":false, "hasMoreAfter":false})).unwrap();
    let blocks = vec![
        block(
            "user",
            "input",
            "01",
            json!({"kind":"user_message", "itemId":"user-input", "text":message, "mode":"Message"}),
        ),
        block(
            "deleted",
            "deleted",
            "02",
            json!({"kind":"user_message", "itemId":"deleted-input", "text":"must be redacted", "deleted":true, "mode":"Message"}),
        ),
        block(
            "work-block",
            "work",
            "03",
            json!({"kind":"turn_work", "work":work}),
        ),
        block(
            "answer",
            "work",
            "04",
            json!({"kind":"assistant_message", "itemId":"answer", "text":"answer"}),
        ),
        block(
            "task-block",
            "task-turn",
            "05",
            json!({"kind":"detached_task_run", "task":task}),
        ),
        block(
            "running",
            "running",
            "06",
            json!({"kind":"turn_state", "state":"running"}),
        ),
    ];
    core.apply_thread_timeline_page(
        ThreadTimelinePageResponse {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            projection_version: 1,
            blocks,
            page: TimelinePageInfo {
                before_cursor: Some(TimelineCursor {
                    value: "before".into(),
                }),
                has_more_before: true,
                ..Default::default()
            },
        },
        TopLevelPageMergeMode::Reset,
    );
    let policy = json!({"llm":{"mode":"summary_only"}, "llmRetention":{"mode":"do_not_retain"}, "timeline":{"mode":"full", "max_bytes":10000}, "storage":{"mode":"none"}, "recovery":{"mode":"none"}, "deltas":{"mode":"disabled"}});
    let mut items = vec![
        json!({"type":"reasoning", "id":"reasoning", "summary":["reasoning"]}),
        json!({"type":"systemEvent", "id":"event", "level":"warning", "message":"retry", "code":"item_retry_scheduled", "details":{"attempt_no":2}}),
    ];
    for kind in [
        "commandExecution",
        "fileChange",
        "webSearch",
        "webFetch",
        "download",
        "dynamicToolCall",
    ] {
        let mut item = json!({"type":kind, "id":kind, "toolName":kind, "arguments":{"query":"query", "url":"https://example.test:8443/path", "cmd":"sh -lc 'printf hello'"}, "status":"completed", "outputPolicy":policy, "display":{"kind":"hidden"}, "storage":{"kind":"none"}, "success":true});
        if kind == "commandExecution" {
            item["display"] =
                json!({"kind":"shell", "stdout":"hello\r\nworld\t!", "truncated":false});
        }
        if kind == "fileChange" {
            item["stdout"] = json!("changed\r\n");
            item["changedFiles"] = json!(["a.txt"]);
        }
        items.push(item);
    }
    let items = items
        .into_iter()
        .enumerate()
        .map(|(ix, raw)| {
            let item: TurnItem = serde_json::from_value(raw).unwrap();
            TurnWorkItem {
                work_item_id: format!("work-{ix}"),
                item_id: item.item_id().to_owned(),
                turn_id: "work".into(),
                order_key: format!("{ix:03}"),
                source_sequence: ix as i64 + 1,
                source_updated_at_unix_micros: 2000,
                item_type: item.item_type(),
                status: TurnWorkItemStatus::Completed,
                started_at_unix_ms: Some(1000),
                completed_at_unix_ms: Some(2000),
                item,
                metadata: None,
            }
        })
        .collect();
    core.apply_turn_work_page(
        TurnWorkPageResponse {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            turn_id: "work".into(),
            projection_version: 1,
            source_high_watermark: 100,
            projection_updated_at_unix_micros: 2000,
            work: Some(work),
            items,
            page: Default::default(),
        },
        WorkPageMergeMode::Reset,
    );
    core.set_thread_turn_work_expanded("a", "work", true);
    core.apply_pending_requests(
        pioneer_client::cli_runtime::approvals::PendingRequestsReduction::Opened(
            pioneer_client::cli_runtime::approvals::PendingRequest::from_native_permission_request(
                TurnPermissionApprovalRequest {
                    request_id: "approval".into(),
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    turn_id: "running".into(),
                    visible_thread_ids: vec![],
                    tool_name: "exec_command".into(),
                    action: TurnPermissionActionKind::ShellCommand,
                    scope_hash: "approval-scope".into(),
                    reason: TurnPermissionDecisionReason::PolicyRequiresApproval,
                    summary: None,
                    details: vec![],
                },
            ),
        ),
    );
}

fn pending_position_fixtures() -> Value {
    let mut fixtures = serde_json::Map::new();
    for (name, kinds, expected) in [
        ("empty", vec![], vec!["pending"]),
        ("tail", vec!["user"], vec!["user", "pending"]),
        (
            "leading",
            vec!["running", "user"],
            vec!["pending", "running", "user"],
        ),
        (
            "middle",
            vec!["user", "running", "user"],
            vec!["user", "pending", "running", "user"],
        ),
        (
            "trailing",
            vec!["user", "running", "running"],
            vec!["user", "pending", "running", "running"],
        ),
    ] {
        let mut outputs = Vec::new();
        for _ in 0..2 {
            let core = fixture_core();
            install(&core, "initial");
            let blocks = kinds
                .iter()
                .enumerate()
                .map(|(ix, kind)| {
                    block(
                        &format!("block-{ix}"),
                        &format!("turn-{ix}"),
                        &format!("{ix:02}"),
                        if *kind == "user" {
                            json!({"kind":"user_message", "text":"text", "mode":"Message"})
                        } else {
                            json!({"kind":"turn_state", "state":"running"})
                        },
                    )
                })
                .collect();
            core.apply_thread_timeline_page(
                ThreadTimelinePageResponse {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    projection_version: 1,
                    blocks,
                    page: Default::default(),
                },
                TopLevelPageMergeMode::Reset,
            );
            let scope = ClientScope::Timeline {
                thread_id: "a".into(),
            };
            let _lease = core.subscribe(scope.clone(), NonZeroUsize::new(8).unwrap());
            let direct = core.thread_presentation_snapshot("a").unwrap().timeline();
            let actual = direct
                .rows()
                .iter()
                .map(|row| match row.value() {
                    pioneer_client::timeline::presentation::TimelineRenderRow::PendingRequest(
                        _,
                    ) => "pending",
                    pioneer_client::timeline::presentation::TimelineRenderRow::Timeline(row)
                        if matches!(
                            row.kind,
                            pioneer_client::timeline::rows::TimelineRowKind::RunningTurn(_)
                        ) =>
                    {
                        "running"
                    }
                    _ => "user",
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            let dto = snapshot_dto(core.snapshot(&scope).unwrap());
            assert_eq!(dto.payload, serde_json::to_value(direct).unwrap());
            outputs.push(dto);
        }
        assert_eq!(outputs[0], outputs[1]);
        fixtures.insert(
            name.into(),
            json!({"snapshot":outputs[0], "expected":expected}),
        );
    }
    Value::Object(fixtures)
}

#[test]
fn timeline_publication_wire_preserves_identity_and_replacements() {
    let direct = fixture_core();
    let adapted = fixture_core();
    install(&direct, "initial");
    install(&adapted, "initial");
    let scope = ClientScope::Timeline {
        thread_id: "a".into(),
    };
    let _direct = direct.subscribe(scope.clone(), NonZeroUsize::new(16).unwrap());
    let _adapted = adapted.subscribe(scope.clone(), NonZeroUsize::new(16).unwrap());
    let initial = snapshot_dto(adapted.snapshot(&scope).unwrap());
    assert_eq!(initial, snapshot_dto(direct.snapshot(&scope).unwrap()));
    assert!(!initial.payload.to_string().contains("must be redacted"));
    assert_eq!(initial.payload["rows"].as_array().unwrap().len(), 15);
    install(&direct, "replacement");
    install(&adapted, "replacement");
    // Core without a worker materializes deterministically when a demand lease arrives.
    let _next_direct = direct.subscribe(scope.clone(), NonZeroUsize::new(16).unwrap());
    let _next_adapted = adapted.subscribe(scope.clone(), NonZeroUsize::new(16).unwrap());
    let next = snapshot_dto(adapted.snapshot(&scope).unwrap());
    assert_eq!(next, snapshot_dto(direct.snapshot(&scope).unwrap()));
    let batch = adapted.wait_for_publications(ClientChangeSequence::new(initial.sequence.get()));
    let change = batch.changes.last().unwrap();
    let deltas = change.timeline_changes();
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].replaced.len(), 1);
    assert!(
        deltas[0].inserted.is_empty() && deltas[0].removed.is_empty() && deltas[0].order.is_none()
    );
    let header = publication_dto(adapted.snapshot(&scope).unwrap(), true);
    assert!(header.payload.get("rows").is_none());
    let changes = batch.changes.iter().map(|change| json!({
        "sequence":change.sequence(), "predecessor":change.predecessor(), "timeline_changes":change.timeline_changes(),
        "snapshots":change.publications().iter().cloned().map(|publication| { let incremental = matches!(publication.scope(), ClientScope::Timeline { .. }); publication_dto(publication, incremental) }).collect::<Vec<_>>()
    })).collect::<Vec<_>>();
    let wire = json!({"pending_positions":pending_position_fixtures(),"initial":initial, "next":next, "header":header, "delta":deltas[0], "batch":{"schema_version":1,"closed":false,"effects":[],"sequence":batch.sequence,"resnapshot":false,"changes":changes}});
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/thread-presentation-wire.json");
    if std::env::var_os("UPDATE_THREAD_PRESENTATION_FIXTURE").is_some() {
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(&wire).unwrap()),
        )
        .unwrap();
    }
    assert_eq!(
        wire,
        serde_json::from_str::<Value>(&std::fs::read_to_string(path).unwrap()).unwrap()
    );
}
