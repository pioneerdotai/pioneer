use super::*;
use crate::agent_actions::{load_agent_action_outbox, load_agent_action_receipt};
use crate::canonical_id::canonical_agent_id;
use crate::outbox_claim::{
    ACTION_TIMELINE_TARGET_TURN_INPUT, AGENT_ACTION_OUTBOX_MAX_ATTEMPTS,
    AGENT_ACTION_OUTBOX_PERMIT_WAIT_CLASS, AGENT_ACTION_OUTBOX_RUNTIME_WAIT_CLASS,
    claim_agent_action_outbox_with_clock, defer_agent_action_outbox_for_permit,
    defer_agent_action_outbox_for_runtime, wake_agent_action_outbox_for_execution,
};
use crate::outbox_retention::next_agent_ledger_retention_at;
use crate::test_support::{EXECUTION_ID, insert_agent_execution, seed_agent_execution};
use chrono::{DateTime, Utc};
use pioneer_entity::{
    agent_action_timeline_target, agent_execution, agent_turn_response_execution,
};
use sea_orm::{ActiveModelTrait, DatabaseConnection, Set};

fn utc_now() -> DateTimeWithTimeZone {
    DateTime::<Utc>::from(Utc::now()).into()
}

#[pioneer_postgres_test::postgres_test]
async fn queued_permit_wait_does_not_consume_outbox_retry_budget_and_is_woken() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = fixture.connection().clone();
    seed_agent_execution(&db).await;
    let now = utc_now();
    let execution_id = "Epermitwait1234567890";
    db.execute_unprepared(
        "INSERT INTO thread (id,workspace_id,preview,mode,model,model_provider,status)
         VALUES ('thread-permit-wait','outbox','','agent','m','p','active');
         INSERT INTO turn (id,thread_id,status)
         VALUES ('turn-permit-wait','thread-permit-wait','in_progress');
         INSERT INTO agent_presentation_snapshot
           (id,agent_identity_id,source_revision,source_fingerprint,display_name,nickname)
         VALUES ('S00000000000000000001','I00000000000000000001',1,'source','Fixture','fixture');",
    )
    .await
    .unwrap();
    agent_execution::ActiveModel {
        id: Set(execution_id.to_owned()),
        workspace_id: Set("outbox".to_owned()),
        agent_identity_id: Set("I00000000000000000001".to_owned()),
        identity_source_revision: Set(1),
        identity_source_fingerprint: Set("source".to_owned()),
        parent_execution_id: Set(Some(EXECUTION_ID.to_owned())),
        parent_task_id: Set(None),
        parent_thread_id: Set(Some("thread-permit-wait".to_owned())),
        home_root_thread_id: Set("thread-permit-wait".to_owned()),
        work_graph_root_execution_id: Set(EXECUTION_ID.to_owned()),
        requested_identity_selection_json: Set("{}".to_owned()),
        requested_profile_selection_json: Set("{}".to_owned()),
        resolved_profile_id: Set(None),
        resolved_profile_fingerprint: Set(None),
        presentation_snapshot_id: Set(Some("S00000000000000000001".to_owned())),
        authorization_context_fingerprint: Set("authorization".to_owned()),
        execution_generation: Set(1),
        status: Set("running".to_owned()),
        created_at: Set(now.clone()),
        updated_at: Set(now.clone()),
        finished_at: Set(None),
    }
    .insert(&db)
    .await
    .unwrap();
    insert_compaction_test_tuple(
        &db,
        "permit-wait",
        "pending",
        0,
        now.clone(),
        None,
        serde_json::json!({"result":"response"}).to_string(),
        serde_json::json!({
            "kind": "start_agent",
            "spawned_execution_id": execution_id,
        })
        .to_string(),
    )
    .await;
    agent_turn_response_execution::Entity::insert(agent_turn_response_execution::ActiveModel {
        turn_id: Set("turn-permit-wait".to_owned()),
        execution_id: Set(execution_id.to_owned()),
        presentation_snapshot_id: Set("S00000000000000000001".to_owned()),
        created_at: Set(now.clone()),
    })
    .exec(&db)
    .await
    .unwrap();
    agent_action_timeline_target::Entity::insert(agent_action_timeline_target::ActiveModel {
        target_key: Set("turn_input:turn-permit-wait".to_owned()),
        action_id: Set("action-permit-wait".to_owned()),
        turn_id: Set("turn-permit-wait".to_owned()),
        turn_item_id: Set(None),
        target_kind: Set(ACTION_TIMELINE_TARGET_TURN_INPUT.to_owned()),
        created_at: Set(now.clone()),
    })
    .exec(&db)
    .await
    .unwrap();

    let claimed = claim_agent_action_outbox_with_clock(&db, 2, &|| now.clone())
        .await
        .unwrap()
        .rows;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].attempts, 1);
    assert!(
        defer_agent_action_outbox_for_permit(
            &db,
            claimed[0].id.as_str(),
            claimed[0].attempts,
            now.clone(),
        )
        .await
        .unwrap()
    );
    let deferred = agent_action_outbox::Entity::find_by_id("outbox-permit-wait")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deferred.status, "failed");
    assert_eq!(deferred.attempts, 0);
    assert_eq!(
        deferred.last_error.as_deref(),
        Some(AGENT_ACTION_OUTBOX_PERMIT_WAIT_CLASS)
    );
    assert!(
        deferred
            .next_attempt_at
            .as_ref()
            .is_some_and(|at| at > &now)
    );

    assert_eq!(
        wake_agent_action_outbox_for_execution(&db, execution_id, now.clone())
            .await
            .unwrap(),
        1
    );
    let woken = agent_action_outbox::Entity::find_by_id("outbox-permit-wait")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(woken.attempts, 0);
    assert_eq!(woken.next_attempt_at, Some(now));
}

#[pioneer_postgres_test::postgres_test]
async fn busy_native_runtime_wait_does_not_consume_outbox_retry_budget() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = fixture.connection().clone();
    seed_agent_execution(&db).await;
    let now = utc_now();
    insert_compaction_test_tuple(
        &db,
        "runtime-wait",
        "pending",
        0,
        now.clone(),
        None,
        serde_json::json!({"result":"response"}).to_string(),
        serde_json::json!({
            "kind": "start_agent",
            "spawned_execution_id": "Eruntimewait123456789",
        })
        .to_string(),
    )
    .await;

    let claimed = claim_agent_action_outbox_with_clock(&db, 2, &|| now.clone())
        .await
        .unwrap()
        .rows;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].attempts, 1);
    assert!(
        defer_agent_action_outbox_for_runtime(
            &db,
            claimed[0].id.as_str(),
            claimed[0].attempts,
            now.clone(),
        )
        .await
        .unwrap()
    );
    let deferred = agent_action_outbox::Entity::find_by_id("outbox-runtime-wait")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deferred.status, "failed");
    assert_eq!(deferred.attempts, 0);
    assert_eq!(
        deferred.last_error.as_deref(),
        Some(AGENT_ACTION_OUTBOX_RUNTIME_WAIT_CLASS)
    );
    let retry_at = deferred
        .next_attempt_at
        .expect("runtime wait should have a retry deadline");
    assert!(retry_at > now.clone());
    assert!(retry_at <= now + Duration::seconds(1));
}

#[pioneer_postgres_test::postgres_test]
async fn terminal_action_ledger_compaction_preserves_window_and_exact_replay_hashes() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = fixture.connection().clone();
    seed_agent_execution(&db).await;
    let now = DateTimeWithTimeZone::parse_from_rfc3339("2026-11-04T12:00:00.250+00:00").unwrap();
    let old = now.clone() - Duration::days(AGENT_ACTION_LEDGER_PAYLOAD_RETENTION_DAYS + 1);
    let recent = now.clone() - Duration::days(AGENT_ACTION_LEDGER_PAYLOAD_RETENTION_DAYS - 1);
    let issued_result = serde_json::json!({
        "workspaceId":"workspace-old", "threadId":"thread-old",
        "turnId":"turn-old", "executionId":"spawned-old",
    });
    let old_response =
        serde_json::json!({"result": "x".repeat(4096), "threadResult":issued_result}).to_string();
    let old_payload = serde_json::json!({
        "action_id": "action-old",
        "execution_id": canonical_agent_id('E', "execution-old"),
        "kind": "start_agent",
        "spawned_execution_id": "spawned-old",
        "launch_request_fingerprint": "a".repeat(64),
        "dispatch": {"turn_id":"turn-old", "execution_generation":1, "opaque": "y".repeat(8192)},
    })
    .to_string();
    insert_compaction_test_tuple(
        &db,
        "old",
        "delivered",
        1,
        old.clone(),
        Some(old.clone()),
        old_response.clone(),
        old_payload.clone(),
    )
    .await;
    // An older exhausted StartAgent still owes reconciliation. It must not
    // consume the compaction quantum forever ahead of closed action tuples.
    insert_compaction_test_tuple(
        &db,
        "unresolved",
        "failed",
        AGENT_ACTION_OUTBOX_MAX_ATTEMPTS,
        old - Duration::days(1),
        None,
        serde_json::json!({"result":"unresolved-response"}).to_string(),
        serde_json::json!({
            "action_id": "action-unresolved",
            "execution_id": canonical_agent_id('E', "execution-unresolved"),
            "kind": "start_agent",
            "spawned_execution_id": "spawned-unresolved",
            "dispatch": {"turn_id":"turn-unresolved", "execution_generation":1},
        })
        .to_string(),
    )
    .await;
    insert_compaction_test_tuple(
        &db,
        "recent",
        "delivered",
        1,
        recent.clone(),
        Some(recent),
        serde_json::json!({"result":"recent-response"}).to_string(),
        serde_json::json!({
            "action_id": "action-recent",
            "execution_id": canonical_agent_id('E', "execution-recent"),
            "kind": "send_message",
        })
        .to_string(),
    )
    .await;
    insert_compaction_test_tuple(
        &db,
        "pending",
        "pending",
        0,
        old,
        None,
        serde_json::json!({"result":"pending-response"}).to_string(),
        serde_json::json!({
            "action_id": "action-pending",
            "execution_id": canonical_agent_id('E', "execution-pending"),
            "kind": "send_message",
        })
        .to_string(),
    )
    .await;

    insert_compaction_test_tuple(
        &db,
        "proof-wait",
        "failed",
        8,
        old - Duration::days(2),
        None,
        serde_json::json!({"result":"blocked-response"}).to_string(),
        serde_json::json!({"action_id":"action-proof-wait", "execution_id":canonical_agent_id('E', "execution-proof-wait"),
            "kind":"start_agent", "spawned_execution_id":"missing-proof"})
        .to_string(),
    )
    .await;
    agent_action_outbox::Entity::update_many()
        .col_expr(
            agent_action_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(Some("outbox_start_integrity_rejected".to_owned())),
        )
        .filter(agent_action_outbox::Column::Id.eq("outbox-proof-wait"))
        .exec(&db)
        .await
        .unwrap();
    assert_eq!(
        next_agent_ledger_retention_at(&db, now).await.unwrap(),
        Some((now + Duration::days(1)).timestamp() + 1)
    );
    let (blocked, cursor) = compact_terminal_agent_action_ledger_page(&db, now, 1, None)
        .await
        .unwrap();
    assert_eq!(blocked.candidate_rows, 1);
    assert_eq!(blocked.compacted_rows, 0);
    assert!(
        cursor.is_some(),
        "a skipped terminal proof is not the end of the scan"
    );
    let (summary, cursor) = compact_terminal_agent_action_ledger_page(&db, now, 1, cursor.as_ref())
        .await
        .unwrap();
    let (empty, end) = compact_terminal_agent_action_ledger_page(&db, now, 1, cursor.as_ref())
        .await
        .unwrap();
    assert_eq!(empty.candidate_rows, 0);
    assert!(
        end.is_none(),
        "a finite pass must finish even while its first proof stays blocked"
    );
    // The rest of this existing test exercises detached payload retention.
    agent_action_outbox::Entity::delete_by_id("outbox-proof-wait")
        .exec(&db)
        .await
        .unwrap();
    assert_eq!(summary.candidate_rows, 1);
    assert_eq!(summary.compacted_rows, 1);
    assert!(summary.payload_bytes_released > 0);
    let unresolved = agent_action_outbox::Entity::find_by_id("outbox-unresolved")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(!unresolved.payload_json.contains("_pioneer_compacted"));

    let action = agent_action::Entity::find_by_id("action-old")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let receipt = agent_action_receipt::Entity::find_by_id("receipt-old")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let outbox = agent_action_outbox::Entity::find_by_id("outbox-old")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(compacted_agent_action_value_matches(
        action.response_json.as_deref(),
        Some(old_response.as_str()),
        AGENT_ACTION_COMPACTION_FORMAT,
    ));
    assert!(compacted_agent_action_value_matches(
        receipt.response_json.as_deref(),
        Some(old_response.as_str()),
        AGENT_ACTION_RECEIPT_COMPACTION_FORMAT,
    ));
    for response in [
        action.response_json.as_deref(),
        receipt.response_json.as_deref(),
    ] {
        let response: serde_json::Value = serde_json::from_str(response.unwrap()).unwrap();
        assert_eq!(response["threadResult"], issued_result);
        assert!(response.get("result").is_none());
    }
    assert!(compacted_agent_action_value_matches(
        Some(outbox.payload_json.as_str()),
        Some(old_payload.as_str()),
        AGENT_ACTION_OUTBOX_COMPACTION_FORMAT,
    ));
    let compacted_payload: serde_json::Value =
        serde_json::from_str(outbox.payload_json.as_str()).unwrap();
    assert_eq!(
        compacted_payload
            .get("spawned_execution_id")
            .and_then(serde_json::Value::as_str),
        Some("spawned-old")
    );
    assert_eq!(
        compacted_payload["dispatch"],
        serde_json::json!({"turn_id":"turn-old", "execution_generation":1})
    );
    assert_eq!(
        compacted_payload["launch_request_fingerprint"],
        "a".repeat(64)
    );
    assert_eq!(
        agent_action_outbox::Entity::find_by_id("outbox-recent")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .payload_json
            .contains("_pioneer_compacted"),
        false
    );
    assert_eq!(
        agent_action_outbox::Entity::find_by_id("outbox-pending")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .payload_json
            .contains("_pioneer_compacted"),
        false
    );
    let detached_payload = serde_json::json!({
        "action_id":"action-detached", "execution_id":canonical_agent_id('E', "execution-detached"), "kind":"start_agent",
        "execution_mode":"detached_task", "task_id":"task-detached", "thread_id":"thread-detached", "turn_id":"turn-detached",
        "launch_request_fingerprint":"b".repeat(64), "normalized":{"input":"z".repeat(4096)},
    }).to_string();
    let detached_result = serde_json::json!({
        "workspaceId":"workspace-detached", "threadId":"thread-detached",
        "turnId":"turn-detached", "taskId":"task-detached",
    });
    insert_compaction_test_tuple(
        &db,
        "detached",
        "delivered",
        1,
        utc_now() - Duration::days(AGENT_ACTION_LEDGER_PAYLOAD_RETENTION_DAYS + 1),
        Some(utc_now() - Duration::days(AGENT_ACTION_LEDGER_PAYLOAD_RETENTION_DAYS + 1)),
        serde_json::json!({"taskCreation":{"taskId":"task-detached"},"threadResult":detached_result}).to_string(),
        detached_payload.clone(),
    )
    .await;
    assert_eq!(
        compact_terminal_agent_action_ledger(&db, utc_now(), 1)
            .await
            .unwrap()
            .compacted_rows,
        1
    );
    let detached = load_agent_action_outbox(&db, "action-detached")
        .await
        .unwrap()
        .unwrap();
    assert!(compacted_agent_action_value_matches(
        Some(&detached.payload_json),
        Some(&detached_payload),
        AGENT_ACTION_OUTBOX_COMPACTION_FORMAT
    ));
    let retained: serde_json::Value = serde_json::from_str(&detached.payload_json).unwrap();
    assert_eq!(retained["execution_mode"], "detached_task");
    assert_eq!(retained["task_id"], "task-detached");
    assert_eq!(retained["turn_id"], "turn-detached");
    assert!(retained.get("dispatch").is_none());
    let receipt = load_agent_action_receipt(&db, "action-detached")
        .await
        .unwrap()
        .unwrap();
    let response: serde_json::Value =
        serde_json::from_str(receipt.response_json.as_deref().unwrap()).unwrap();
    assert_eq!(response["threadResult"], detached_result);
}

async fn insert_compaction_test_tuple(
    db: &DatabaseConnection,
    suffix: &str,
    outbox_status: &str,
    attempts: i64,
    created_at: DateTimeWithTimeZone,
    delivered_at: Option<DateTimeWithTimeZone>,
    response_json: String,
    payload_json: String,
) {
    let action_id = format!("action-{suffix}");
    let execution_id = canonical_agent_id('E', &format!("execution-{suffix}"));
    // Each action has an actual owner while keeping its original replay bytes.
    insert_agent_execution(db, &execution_id).await;
    let kind = serde_json::from_str::<serde_json::Value>(payload_json.as_str())
        .unwrap()
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .unwrap()
        .to_owned();
    agent_action::Entity::insert(agent_action::ActiveModel {
        id: Set(action_id.clone()),
        execution_id: Set(execution_id.clone()),
        action_kind: Set(kind.clone()),
        idempotency_key: Set(format!("idempotency-{suffix}")),
        request_fingerprint: Set("f".repeat(64)),
        status: Set("committed".to_owned()),
        created_at: Set(created_at.clone()),
        committed_at: Set(Some(created_at.clone())),
        response_json: Set(Some(response_json.clone())),
    })
    .exec(db)
    .await
    .unwrap();
    agent_action_receipt::Entity::insert(agent_action_receipt::ActiveModel {
        id: Set(format!("receipt-{suffix}")),
        action_id: Set(action_id.clone()),
        actor_kind: Set("agent_execution".to_owned()),
        actor_id: Set(Some(execution_id.clone())),
        decision: Set("allowed".to_owned()),
        policy_fingerprint: Set("a".repeat(64)),
        execution_grant_fingerprint: Set(Some("b".repeat(64))),
        execution_grant_policy_generation: Set(Some(1)),
        source_scope_id: Set(Some("source-scope".to_owned())),
        destination_scope_id: Set(None),
        action_kind: Set(Some(kind)),
        authorized_resource_action: Set(Some("send_message".to_owned())),
        subject_role_key: Set(Some("agent.collaborator".to_owned())),
        execution_generation: Set(Some(1)),
        source_policy_generation: Set(Some(1)),
        destination_policy_generation: Set(None),
        route_generation: Set(None),
        disclosure_class: Set(Some("source_capsule".to_owned())),
        decision_fingerprint: Set(Some("c".repeat(64))),
        committed_at: Set(created_at.clone()),
        response_json: Set(Some(response_json)),
        route_receipt_json: Set(None),
    })
    .exec(db)
    .await
    .unwrap();
    agent_action_outbox::Entity::insert(agent_action_outbox::ActiveModel {
        id: Set(format!("outbox-{suffix}")),
        action_id: Set(action_id),
        owner_execution_id: Set(execution_id),
        payload_json: Set(payload_json),
        status: Set(outbox_status.to_owned()),
        attempts: Set(attempts),
        next_attempt_at: Set(None),
        delivered_at: Set(delivered_at),
        last_error: Set(None),
        created_at: Set(created_at),
    })
    .exec(db)
    .await
    .unwrap();
}
