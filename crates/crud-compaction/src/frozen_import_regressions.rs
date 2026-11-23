//! Exact frozen imports against persisted canonical delivery evidence.
use super::*;
use crate::accepted_import::{PreparedFrozenImport, frozen_import_identity};
use pioneer_crud_conversations::events::TurnEventPayload;
use pioneer_crud_execution::{
    event_append::{PreparedTurnEvent, append_prepared_event},
    time::unix_to_datetime,
};

async fn append_item_completion(
    store: &CompactionStore,
    notification: pioneer_protocol::ItemCompletedNotification,
    at: i64,
) -> anyhow::Result<()> {
    let event =
        PreparedTurnEvent::prepare_for_storage(TurnEventPayload::ItemCompleted(notification))?;
    let tx = store.database_connection().begin().await?;
    append_prepared_event(&tx, event, unix_to_datetime(at)).await?;
    tx.commit().await?;
    Ok(())
}

// Complete TaskRunTurn evidence and a ready frozen manifest let discovery
// resolve exact outputs without reading a mutable Task result payload.
#[pioneer_postgres_test::postgres_test]
async fn frozen_own_imports_require_exact_output_membership_and_atomic_publication() {
    let mut frozen_conversion_progress = FrozenStorageLifetimeProgress::default();
    use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenMessageRef};
    use sha2::{Digest, Sha256};
    fn descriptor(id: &str, messages: &[FrozenMessageRef]) -> FrozenHistoryRef {
        let mut digest = Sha256::new();
        for message in messages {
            let bytes = serde_json::to_vec(message).unwrap();
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes);
        }
        FrozenHistoryRef {
            format: 1,
            manifest_id: id.into(),
            messages: messages.len() as u64,
            identity_sha256: hex::encode(digest.finalize()),
        }
    }
    let store = store().await;
    let db = store.database_connection();
    source(&store, "basis-source", 1, "{}").await;
    let inherited = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    for statement in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('child','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('child-turn','child','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('child-source','child','child-turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'succeeded','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt','task','run','child','child-turn','initial',0,1,'completed',CURRENT_TIMESTAMP)",
        "INSERT INTO task_result_candidate(id,task_id,run_id,task_run_turn_id,thread_id,turn_id,round,status,created_at,updated_at) VALUES ('candidate','task','run','rt','child','child-turn',0,'accepted',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('delivery-turn','thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id) VALUES ('delivery','ws','task','run','key','thread','origin_thread','thread','delivered',1,1,'delivery-turn')",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    let own_source = SourceRef {
        scope: "event:child-turn".into(),
        id: "child-source".into(),
        version: "event-revision:1".into(),
    };
    let own = FrozenMessageRef {
        logical_turn_id: None,
        context_thread: None,
        source_thread: "child".into(),
        unit_id: "child-unit".into(),
        sources: vec![own_source.clone()],
        event_input_role: None,
        source_aliases: vec![],
        ambiguous_input_aliases: vec![],
        publication_aliases: None,
        inherited: false,
        complete: true,
        protected_input: false,
        wire_sha256: "a".repeat(64),
        replay_source: None,
        tool_item_id: None,
        tool_call_id: None,
        tool_name: None,
    };
    let basis = FrozenMessageRef {
        source_thread: "thread".into(),
        sources: vec![inherited.clone()],
        inherited: true,
        ..own.clone()
    };
    let output = descriptor("output-with-basis", &[own.clone(), basis.clone()]);
    store
        .compaction_begin_frozen_history("ws", "child", &output)
        .await
        .unwrap();
    store
        .compaction_append_frozen_history(
            "ws",
            "child",
            &output.manifest_id,
            0,
            &[own.clone(), basis.clone()],
        )
        .await
        .unwrap();
    assert!(
        store
            .compaction_finish_frozen_history("ws", "child", &output)
            .await
            .unwrap()
    );
    store
        .compaction_record_task_output("ws", "rt", &output)
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO compaction_delivery_output(delivery_id,candidate_id,task_run_turn_id) VALUES ('delivery','candidate','rt')").await.unwrap();
    append_item_completion(
        &store,
        pioneer_protocol::ItemCompletedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "delivery-turn".into(),
            item: pioneer_protocol::TurnItem::AgentMessage {
                id: pioneer_protocol::task_delivery_result_item_id("delivery"),
                text: "delivered".into(),
                phase: Default::default(),
                markdown: None,
                markdown_version: None,
            },
        },
        chrono::Utc::now().timestamp(),
    )
    .await
    .unwrap();
    let acknowledgement = store
        .compaction_source_page("ws", "thread", "delivery-turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let prepared = store
        .compaction_prepare_frozen_import(
            "ws",
            "thread",
            "delivery",
            &acknowledgement,
            0,
            "child",
            &own_source,
        )
        .await
        .unwrap();
    assert!(
        store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "delivery",
                &acknowledgement,
                1,
                "thread",
                &inherited
            )
            .await
            .is_err(),
        "H cannot become own work"
    );
    assert!(
        store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "delivery",
                &acknowledgement,
                0,
                "thread",
                &inherited
            )
            .await
            .is_err(),
        "an unrelated same-workspace source is not an output member"
    );
    assert!(
        store
            .compaction_prepare_frozen_import(
                "other",
                "thread",
                "delivery",
                &acknowledgement,
                0,
                "child",
                &own_source
            )
            .await
            .is_err()
    );
    let mut stale_ack = acknowledgement.clone();
    stale_ack.version = "event-revision:999".into();
    assert!(
        store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "delivery",
                &stale_ack,
                0,
                "child",
                &own_source
            )
            .await
            .is_err()
    );
    let target = FrozenMessageRef {
        context_thread: Some("thread".into()),
        ..own
    };
    // H is already represented by a published checkpoint S in the accepted
    // snapshot. The checkpoint is an atomic accepted-basis source; its saved
    // coverage is used only for boundary/grant membership.
    let s_operation = admit_import_operation(&store, "basis-s", "thread", "turn").await;
    let s_ready = ready_import_operation(&store, &s_operation, "thread", &inherited).await;
    assert_eq!(
        store
            .compaction_apply_runner(&s_operation.id, &s_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let s_source = store
        .compaction_checkpoint_source("ws", "thread", "checkpoint-basis-s")
        .await
        .unwrap()
        .unwrap();
    let checkpoint_basis = FrozenMessageRef {
        sources: vec![s_source.clone()],
        wire_sha256: "c".repeat(64),
        inherited: true,
        ..basis.clone()
    };
    let context_messages = vec![checkpoint_basis, target.clone()];
    let context = descriptor("assembled", &context_messages);
    let imports = vec![(1, prepared.clone())];
    let import_digest = frozen_import_identity(&imports).unwrap();
    store
        .compaction_begin_frozen_history_with_imports("ws", "thread", &context, 1, &import_digest)
        .await
        .unwrap();
    store
        .compaction_append_frozen_history(
            "ws",
            "thread",
            &context.manifest_id,
            0,
            &context_messages,
        )
        .await
        .unwrap();
    assert!(
        !store
            .compaction_finish_frozen_history("ws", "thread", &context)
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_frozen_history_owner("ws", &context)
            .await
            .unwrap()
            .is_none()
    );
    db.execute_unprepared("DELETE FROM compaction_delivery_output WHERE delivery_id='delivery'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_append_frozen_imports("ws", "thread", &context.manifest_id, 0, &imports)
            .await
            .is_err(),
        "binding is revalidated after preparation"
    );
    db.execute_unprepared("INSERT INTO compaction_delivery_output(delivery_id,candidate_id,task_run_turn_id) VALUES ('delivery','candidate','rt')").await.unwrap();
    db.execute_unprepared("CREATE FUNCTION abort_frozen_import() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture import rollback'; END $$; CREATE TRIGGER abort_frozen_import AFTER INSERT ON compaction_frozen_import_data FOR EACH ROW EXECUTE FUNCTION abort_frozen_import()").await.unwrap();
    assert!(
        store
            .compaction_append_frozen_imports("ws", "thread", &context.manifest_id, 0, &imports)
            .await
            .is_err()
    );
    assert!(
        !store
            .compaction_finish_frozen_history("ws", "thread", &context)
            .await
            .unwrap()
    );
    db.execute_unprepared("DROP TRIGGER abort_frozen_import ON compaction_frozen_import_data")
        .await
        .unwrap();
    store
        .compaction_append_frozen_imports("ws", "thread", &context.manifest_id, 0, &imports)
        .await
        .unwrap();
    store
        .compaction_append_frozen_imports("ws", "thread", &context.manifest_id, 0, &imports)
        .await
        .unwrap();
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &context)
            .await
            .unwrap()
    );
    store
        .compaction_append_frozen_imports("ws", "thread", &context.manifest_id, 0, &imports)
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_frozen_import_state("ws", "thread", &context.manifest_id)
            .await
            .unwrap(),
        Some((1, import_digest.clone()))
    );
    let records = store
        .compaction_frozen_import_page("ws", "thread", &context.manifest_id, 0)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].source, own_source);
    assert_eq!(records[0].delivery_id, "delivery");
    assert!(
        store
            .compaction_frozen_import_page("other", "thread", &context.manifest_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
    for _ in 0..100 {
        if !store
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
            .unwrap()
        {
            break;
        }
    }
    let shared = FrozenHistoryRef {
        manifest_id: "assembled-shared".into(),
        ..context.clone()
    };
    store
        .compaction_begin_frozen_history_with_imports("ws", "thread", &shared, 1, &import_digest)
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_share_frozen_prefix(
                "ws",
                "thread",
                &shared.manifest_id,
                &context_messages,
                &imports
            )
            .await
            .unwrap(),
        (2, 1)
    );
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &shared)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_frozen_import_page("ws", "thread", &shared.manifest_id, 0)
            .await
            .unwrap(),
        records
    );
    assert_eq!(
        frozen_count(&store, "compaction_frozen_import_data").await,
        1
    );
    // The import still matches, but its target is not stored yet when the
    // message prefix diverges. Prefix preparation (including retry) must let
    // the normal message-then-import append path complete the capture.
    let mut divergent_messages = context_messages.clone();
    divergent_messages[0].wire_sha256 = "d".repeat(64);
    let divergent = descriptor("assembled-divergent", &divergent_messages);
    store
        .compaction_begin_frozen_history_with_imports("ws", "thread", &divergent, 1, &import_digest)
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            store
                .compaction_share_frozen_prefix(
                    "ws",
                    "thread",
                    &divergent.manifest_id,
                    &divergent_messages,
                    &imports,
                )
                .await
                .unwrap(),
            (0, 0)
        );
    }
    store
        .compaction_append_frozen_history(
            "ws",
            "thread",
            &divergent.manifest_id,
            0,
            &divergent_messages,
        )
        .await
        .unwrap();
    store
        .compaction_append_frozen_imports("ws", "thread", &divergent.manifest_id, 0, &imports)
        .await
        .unwrap();
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &divergent)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_frozen_import_page("ws", "thread", &divergent.manifest_id, 0)
            .await
            .unwrap(),
        records
    );
    // A new execution can adopt this metadata only through its exact TaskRun
    // snapshot; sharing a workspace or parent thread is insufficient.
    for statement in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('context-c','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('context-c','thread','thread',1,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn-c','context-c','in_progress','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run-c','task','run-c',1,2,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt-c','task','run-c','context-c','turn-c','initial',0,1,'running',CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    let operation = admit_import_operation(&store, "own-c", "context-c", "turn-c").await;
    assert!(
        store
            .compaction_bind_source_projection(&operation.id, &context)
            .await
            .is_err(),
        "an arbitrary parent manifest is not an accepted Task basis"
    );
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "WITH frozen_root_fixture(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) AS (VALUES ('run-c','task','ws','thread',$1,CURRENT_TIMESTAMP)) INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,frozen_manifest_id) SELECT run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,CASE WHEN history_json IS JSON OBJECT THEN (history_json)::json->>'manifest_id' END FROM frozen_root_fixture",
        [serde_json::to_string(&context).unwrap().into()])).await.unwrap();
    assert!(
        store
            .compaction_bind_source_projection(&operation.id, &output)
            .await
            .is_err()
    );
    db.execute_unprepared("CREATE FUNCTION abort_projection() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture binding rollback'; END $$; CREATE TRIGGER abort_projection AFTER INSERT ON compaction_operation_projection FOR EACH ROW EXECUTE FUNCTION abort_projection()").await.unwrap();
    assert!(
        store
            .compaction_bind_source_projection(&operation.id, &context)
            .await
            .is_err()
    );
    db.execute_unprepared("DROP TRIGGER abort_projection ON compaction_operation_projection")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&operation.id, &context)
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&operation.id, &context)
        .await
        .unwrap();
    // A completed child's summary is created AFTER its immutable raw output.
    // C may reuse that summary only if every leaf has a grant in C's accepted
    // basis; the output manifest itself is never rewritten to add the summary.
    let a_operation = admit_import_operation(&store, "summary-a", "child", "child-turn").await;
    let a_ready = ready_import_operation(&store, &a_operation, "child", &own_source).await;
    assert_eq!(
        store
            .compaction_apply_runner(&a_operation.id, &a_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let a_summary = store
        .compaction_checkpoint_source("ws", "child", "checkpoint-summary-a")
        .await
        .unwrap()
        .unwrap();
    let mut projected_operation = operation.clone();
    projected_operation.id = "projected-c".into();
    projected_operation.owner = "owner-projected-c".into();
    projected_operation.plan.fingerprint = "projected-c".into();
    for scope in ["child", "context-c", "thread"] {
        projected_operation.source_epochs.insert(
            scope.into(),
            store
                .compaction_projection_version("ws", scope)
                .await
                .unwrap(),
        );
    }
    store
        .compaction_admit("ws", "context-c", &projected_operation)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&projected_operation.id, "turn-c")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&projected_operation.id, &context)
        .await
        .unwrap();
    let projected_ready =
        ready_import_operation(&store, &projected_operation, "child", &a_summary).await;
    assert!(
        store
            .compaction_manifest_sources_current(&projected_operation.id)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_apply_runner(&projected_operation.id, &projected_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    // An accessible checkpoint must not launder H into accepted OWN work.
    // Model an older mixed checkpoint: only one of its two leaves was delivered.
    let mut mixed_op = admit_import_operation(&store, "mixed-summary", "child", "child-turn").await;
    // This legacy raw checkpoint originally admitted both leaves. It still
    // must not grant a leaf that was never accepted in the delivered output.
    mixed_op.plan.compact = vec![0, 1];
    mixed_op.plan.coverage = vec![own_source.clone(), inherited.clone()];
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE compaction_operation SET snapshot=$1 WHERE id=$2",
        [
            serde_json::to_string(&mixed_op).unwrap().into(),
            mixed_op.id.clone().into(),
        ],
    ))
    .await
    .unwrap();
    let mixed = Checkpoint {
        id: "mixed-checkpoint".into(),
        operation_id: mixed_op.id.clone(),
        owner: mixed_op.owner.clone(),
        previous: None,
        format_version: 1,
        coverage: vec![own_source.clone(), inherited.clone()],
        summary: "mixed H and A".into(),
        selection: mixed_op.admission.selection.clone(),
        projection_version: mixed_op.projection_version,
    };
    store.compaction_save_candidate(&mixed, 0).await.unwrap();
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='applied' WHERE id='mixed-checkpoint'",
    )
    .await
    .unwrap();
    let mixed_source = store
        .compaction_checkpoint_source("ws", "child", &mixed.id)
        .await
        .unwrap()
        .unwrap();
    let mut mixed_target = projected_operation.clone();
    mixed_target.id = "mixed-target".into();
    mixed_target.owner = "owner-mixed-target".into();
    mixed_target.plan.fingerprint = "mixed-target".into();
    store
        .compaction_admit("ws", "context-c", &mixed_target)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&mixed_target.id, "turn-c")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&mixed_target.id, &context)
        .await
        .unwrap();
    let _mixed_ready = ready_import_operation(&store, &mixed_target, "child", &mixed_source).await;
    assert!(
        !store
            .compaction_manifest_sources_current(&mixed_target.id)
            .await
            .unwrap()
    );
    // The manifest check above rejects this input before summarization.
    // Publication trusts the versions consumed by a valid runner.
    // Publish K through the real runner path. K covers accepted S + imported A;
    // its H leaves are not present directly in the accepted frozen messages.
    let mut working_op = projected_operation.clone();
    working_op.id = "working-summary-operation".into();
    working_op.owner = "owner-working-summary".into();
    working_op.plan.coverage_domain = pioneer_compaction::CoverageDomain::WorkingContext;
    working_op.plan.fingerprint = working_op.id.clone();
    store
        .compaction_admit("ws", "context-c", &working_op)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&working_op.id, "turn-c")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&working_op.id, &context)
        .await
        .unwrap();
    let k_ready = ready_operation(
        &store,
        &working_op,
        &[
            ("thread".into(), s_source.clone()),
            ("child".into(), own_source.clone()),
        ],
    )
    .await;
    assert_eq!(
        store
            .compaction_apply_runner(&working_op.id, &k_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let k = store
        .compaction_checkpoint("checkpoint-working-summary-operation")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(k.coverage, vec![s_source.clone(), own_source.clone()]);
    let working_source = store
        .compaction_checkpoint_source("ws", "context-c", "checkpoint-working-summary-operation")
        .await
        .unwrap()
        .unwrap();

    // A following child accepts the same S + raw A snapshot. Its compaction may
    // consume K as WorkingContext, but the identical grants must not authorize
    // K for an OwnContribution operation.
    for statement in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('context-d','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('context-d','thread','thread',1,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn-d','context-d','in_progress','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task-d','ws','thread','thread','thread','turn','agent','running','Task D','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run-d','task-d','run-d',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt-d','task-d','run-d','context-d','turn-d','initial',0,1,'running',CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "WITH frozen_root_fixture(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) AS (VALUES ('run-d','task-d','ws','thread',$1,CURRENT_TIMESTAMP)) INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,frozen_manifest_id) SELECT run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,CASE WHEN history_json IS JSON OBJECT THEN (history_json)::json->>'manifest_id' END FROM frozen_root_fixture",
        [serde_json::to_string(&context).unwrap().into()],
    ))
    .await
    .unwrap();
    let mut working_target = projected_operation.clone();
    working_target.plan.coverage_domain = pioneer_compaction::CoverageDomain::WorkingContext;
    working_target.projection_version = store
        .compaction_projection_version("ws", "context-d")
        .await
        .unwrap();
    working_target.source_epochs.clear();
    for scope in ["thread", "child", "context-c", "context-d"] {
        working_target.source_epochs.insert(
            scope.into(),
            store
                .compaction_projection_version("ws", scope)
                .await
                .unwrap(),
        );
    }
    working_target.id = "working-target-admitted".into();
    working_target.owner = "owner-working-target-admitted".into();
    working_target.plan.fingerprint = working_target.id.clone();
    store
        .compaction_admit("ws", "context-d", &working_target)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&working_target.id, "turn-d")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&working_target.id, &context)
        .await
        .unwrap();
    let working_ready =
        ready_import_operation(&store, &working_target, "context-c", &working_source).await;
    assert!(
        store
            .compaction_manifest_sources_current(&working_target.id)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_apply_runner(&working_target.id, &working_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let mut own_target = working_target.clone();
    own_target.id = "own-target".into();
    own_target.owner = "owner-own-target".into();
    own_target.plan.coverage_domain = pioneer_compaction::CoverageDomain::OwnContribution;
    own_target.plan.fingerprint = own_target.id.clone();
    store
        .compaction_admit("ws", "context-d", &own_target)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&own_target.id, "turn-d")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&own_target.id, &context)
        .await
        .unwrap();
    let _own_ready =
        ready_import_operation(&store, &own_target, "context-c", &working_source).await;
    assert!(
        !store
            .compaction_manifest_sources_current(&own_target.id)
            .await
            .unwrap()
    );
    // The manifest check above rejects this input before summarization.
    // Publication trusts the versions consumed by a valid runner.
    let denied = admit_import_operation(&store, "unaccepted-summary", "context-c", "turn-c").await;
    let _denied_ready = ready_import_operation(&store, &denied, "child", &a_summary).await;
    assert!(
        !store
            .compaction_manifest_sources_current(&denied.id)
            .await
            .unwrap()
    );
    // The manifest check above rejects this input before summarization.
    // Publication trusts the versions consumed by a valid runner.
    assert_eq!(
        store
            .compaction_frozen_history_page("ws", "thread", &context.manifest_id, 0)
            .await
            .unwrap(),
        context_messages
    );

    // Production path: the accepted parent basis is recaptured by the child.
    // Ownership evidence must survive that capture and authorize final commit.
    let maintenance = store.with_maintenance_access();
    let forwarded = maintenance
        .compaction_prepare_accepted_import("ws", "context-c", "turn-c", 0)
        .await
        .unwrap();
    assert!(
        maintenance
            .compaction_prepare_accepted_import("ws", "child", "turn-c", 0)
            .await
            .is_err()
    );
    let child_target = FrozenMessageRef {
        context_thread: Some("context-c".into()),
        ..target.clone()
    };
    let child_context = descriptor("recaptured-c", std::slice::from_ref(&child_target));
    let forwarded_imports = vec![(0, forwarded)];
    let forwarded_digest = frozen_import_identity(&forwarded_imports).unwrap();
    maintenance
        .compaction_begin_frozen_history_with_imports(
            "ws",
            "context-c",
            &child_context,
            1,
            &forwarded_digest,
        )
        .await
        .unwrap();
    maintenance
        .compaction_append_frozen_history(
            "ws",
            "context-c",
            &child_context.manifest_id,
            0,
            std::slice::from_ref(&child_target),
        )
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE task_run_conversation_snapshot SET history_json='[]',frozen_manifest_id=NULL WHERE run_id='run-c'",
    )
    .await
    .unwrap();
    assert!(
        maintenance
            .compaction_append_frozen_imports(
                "ws",
                "context-c",
                &child_context.manifest_id,
                0,
                &forwarded_imports
            )
            .await
            .is_err()
    );
    assert!(
        !maintenance
            .compaction_finish_frozen_history("ws", "context-c", &child_context)
            .await
            .unwrap()
    );
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "WITH root_replacement(history_json) AS (VALUES ($1)) UPDATE task_run_conversation_snapshot SET history_json=(SELECT history_json FROM root_replacement),frozen_manifest_id=(SELECT CASE WHEN history_json IS JSON OBJECT THEN (history_json)::json->>'manifest_id' END FROM root_replacement) WHERE run_id='run-c'",
        [serde_json::to_string(&context).unwrap().into()],
    ))
    .await
    .unwrap();
    db.execute_unprepared("CREATE FUNCTION abort_forwarded_import() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture forward rollback'; END $$; CREATE TRIGGER abort_forwarded_import AFTER INSERT ON compaction_frozen_import_data FOR EACH ROW EXECUTE FUNCTION abort_forwarded_import()").await.unwrap();
    assert!(
        maintenance
            .compaction_append_frozen_imports(
                "ws",
                "context-c",
                &child_context.manifest_id,
                0,
                &forwarded_imports
            )
            .await
            .is_err()
    );
    assert!(
        !maintenance
            .compaction_finish_frozen_history("ws", "context-c", &child_context)
            .await
            .unwrap()
    );
    db.execute_unprepared("DROP TRIGGER abort_forwarded_import ON compaction_frozen_import_data")
        .await
        .unwrap();
    for _ in 0..2 {
        maintenance
            .compaction_append_frozen_imports(
                "ws",
                "context-c",
                &child_context.manifest_id,
                0,
                &forwarded_imports,
            )
            .await
            .unwrap();
    }
    assert!(
        maintenance
            .compaction_finish_frozen_history("ws", "context-c", &child_context)
            .await
            .unwrap()
    );
    let carried = maintenance
        .compaction_prepare_accepted_checkpoint_import(
            "ws",
            "context-c",
            "turn-c",
            0,
            "child",
            &a_summary,
        )
        .await
        .unwrap();
    let summary_target = FrozenMessageRef {
        sources: vec![a_summary.clone()],
        wire_sha256: "d".repeat(64),
        ..child_target.clone()
    };
    let projected_context = descriptor(
        "recaptured-checkpoint-c",
        std::slice::from_ref(&summary_target),
    );
    let carried_imports = vec![(0, carried)];
    let carried_digest = frozen_import_identity(&carried_imports).unwrap();
    maintenance
        .compaction_begin_frozen_history_with_imports(
            "ws",
            "context-c",
            &projected_context,
            1,
            &carried_digest,
        )
        .await
        .unwrap();
    maintenance
        .compaction_append_frozen_history(
            "ws",
            "context-c",
            &projected_context.manifest_id,
            0,
            std::slice::from_ref(&summary_target),
        )
        .await
        .unwrap();
    maintenance
        .compaction_append_frozen_imports(
            "ws",
            "context-c",
            &projected_context.manifest_id,
            0,
            &carried_imports,
        )
        .await
        .unwrap();
    assert!(
        maintenance
            .compaction_finish_frozen_history("ws", "context-c", &projected_context)
            .await
            .unwrap()
    );
    let projected_capture_operation = admit_import_operation(
        &maintenance,
        "projected-capture-operation",
        "context-c",
        "turn-c",
    )
    .await;
    maintenance
        .compaction_bind_source_projection(&projected_capture_operation.id, &projected_context)
        .await
        .unwrap();
    let projected_capture_ready = ready_import_operation(
        &maintenance,
        &projected_capture_operation,
        "child",
        &a_summary,
    )
    .await;
    assert_eq!(
        maintenance
            .compaction_apply_runner(
                &projected_capture_operation.id,
                &projected_capture_ready,
                None,
            )
            .await
            .unwrap(),
        CommitOutcome::Applied,
        "a carried raw grant must authorize the exact foreign OWN summary that replaced it"
    );
    let recaptured_operation =
        admit_import_operation(&maintenance, "recaptured-operation", "context-c", "turn-c").await;
    maintenance
        .compaction_bind_source_projection(&recaptured_operation.id, &child_context)
        .await
        .unwrap();
    let recaptured_ready =
        ready_import_operation(&maintenance, &recaptured_operation, "child", &own_source).await;
    assert_eq!(
        maintenance
            .compaction_apply_runner(&recaptured_operation.id, &recaptured_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(
        maintenance
            .compaction_apply_runner(&recaptured_operation.id, &recaptured_ready, None)
            .await
            .unwrap(),
        CommitOutcome::AlreadyApplied
    );

    let ready = ready_import_operation(&store, &operation, "child", &own_source).await;
    store
        .compaction_bind_source_projection(&operation.id, &context)
        .await
        .unwrap();
    // Complete identity is immutable now: an attempted rewrite is rejected
    // before it can widen the operation's read-time authority. Corrupt legacy
    // headers remain covered by the predicate/oracle fixtures.
    assert!(
        db.execute_unprepared(
            "UPDATE compaction_frozen_history SET imports_sha256='changed' WHERE id='assembled'",
        )
        .await
        .is_err()
    );
    let pinned: String = db
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT imports_sha256 FROM compaction_frozen_history WHERE id='assembled'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "imports_sha256")
        .unwrap();
    assert_eq!(pinned, import_digest);
    assert!(
        store
            .compaction_manifest_sources_current(&operation.id)
            .await
            .unwrap()
    );
    db.execute_unprepared("CREATE FUNCTION abort_import_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture imported commit rollback'; END $$; CREATE TRIGGER abort_import_commit AFTER UPDATE OF head ON compaction_context FOR EACH ROW EXECUTE FUNCTION abort_import_commit()").await.unwrap();
    assert!(
        store
            .compaction_apply_runner(&operation.id, &ready, None)
            .await
            .is_err()
    );
    assert_eq!(store.compaction_head(&operation.owner).await.unwrap(), None);
    db.execute_unprepared("DROP TRIGGER abort_import_commit ON compaction_context")
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_apply_runner(&operation.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(
        store
            .compaction_apply_runner(&operation.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::AlreadyApplied
    );

    let unbound = admit_import_operation(&store, "unbound-c", "context-c", "turn-c").await;
    let _unbound_ready = ready_import_operation(&store, &unbound, "child", &own_source).await;
    assert!(
        !store
            .compaction_manifest_sources_current(&unbound.id)
            .await
            .unwrap(),
        "unauthorized sources must be rejected before provider execution"
    );
    assert!(
        store
            .compaction_bind_source_projection(&unbound.id, &context)
            .await
            .is_err(),
        "a ready runner cannot gain new ownership after provider execution"
    );
    let h_operation = admit_import_operation(&store, "h-c", "context-c", "turn-c").await;
    store
        .compaction_bind_source_projection(&h_operation.id, &context)
        .await
        .unwrap();
    let _h_ready = ready_import_operation(&store, &h_operation, "thread", &inherited).await;
    assert!(
        !store
            .compaction_manifest_sources_current(&h_operation.id)
            .await
            .unwrap(),
        "unauthorized sources must be rejected before provider execution"
    );
    let edited = admit_import_operation(&store, "edited-c", "context-c", "turn-c").await;
    store
        .compaction_bind_source_projection(&edited.id, &context)
        .await
        .unwrap();
    let edited_ready = ready_import_operation(&store, &edited, "child", &own_source).await;
    let mut imported_checkpoint_edited = projected_operation.clone();
    imported_checkpoint_edited.id = "edited-imported-checkpoint".into();
    imported_checkpoint_edited.owner = "owner-edited-imported-checkpoint".into();
    imported_checkpoint_edited.plan.fingerprint = imported_checkpoint_edited.id.clone();
    imported_checkpoint_edited.projection_version = store
        .compaction_projection_version("ws", "context-c")
        .await
        .unwrap();
    imported_checkpoint_edited.source_epochs.clear();
    for scope in ["child", "context-c", "thread"] {
        imported_checkpoint_edited.source_epochs.insert(
            scope.into(),
            store
                .compaction_projection_version("ws", scope)
                .await
                .unwrap(),
        );
    }
    store
        .compaction_admit("ws", "context-c", &imported_checkpoint_edited)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&imported_checkpoint_edited.id, "turn-c")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&imported_checkpoint_edited.id, &context)
        .await
        .unwrap();
    let imported_checkpoint_ready =
        ready_import_operation(&store, &imported_checkpoint_edited, "child", &a_summary).await;
    let stale_forwarded = maintenance
        .compaction_prepare_accepted_import("ws", "context-c", "turn-c", 0)
        .await
        .unwrap();
    let stale_forwarded_imports = vec![(0, stale_forwarded)];
    let stale_forwarded_digest = frozen_import_identity(&stale_forwarded_imports).unwrap();
    let stale_forwarded_context =
        descriptor("stale-forwarded", std::slice::from_ref(&child_target));
    maintenance
        .compaction_begin_frozen_history_with_imports(
            "ws",
            "context-c",
            &stale_forwarded_context,
            1,
            &stale_forwarded_digest,
        )
        .await
        .unwrap();
    maintenance
        .compaction_append_frozen_history(
            "ws",
            "context-c",
            &stale_forwarded_context.manifest_id,
            0,
            std::slice::from_ref(&child_target),
        )
        .await
        .unwrap();
    let stale = descriptor("stale-assembled", std::slice::from_ref(&target));
    store
        .compaction_begin_frozen_history_with_imports("ws", "thread", &stale, 1, &import_digest)
        .await
        .unwrap();
    store
        .compaction_append_frozen_history("ws", "thread", &stale.manifest_id, 0, &[target])
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE turn_event SET payload='{\"changed\":true}' WHERE id='child-source'",
    )
    .await
    .unwrap();
    assert!(
        maintenance
            .compaction_append_frozen_imports(
                "ws",
                "context-c",
                &stale_forwarded_context.manifest_id,
                0,
                &stale_forwarded_imports,
            )
            .await
            .is_err(),
        "accepted source revision is revalidated in the writer transaction"
    );
    let stale_forwarded_state = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT h.next_import,(SELECT COUNT(*) FROM compaction_frozen_import i WHERE i.manifest_id=h.id) AS stored FROM compaction_frozen_history h WHERE h.id=$1",
            [stale_forwarded_context.manifest_id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stale_forwarded_state
            .try_get::<i64>("", "next_import")
            .unwrap(),
        0,
        "rejected accepted import must not advance its cursor"
    );
    assert_eq!(
        stale_forwarded_state.try_get::<i64>("", "stored").unwrap(),
        0,
        "rejected accepted import must not write metadata"
    );
    assert!(
        store
            .compaction_append_frozen_imports("ws", "thread", &stale.manifest_id, 0, &imports)
            .await
            .is_err(),
        "source revision is revalidated in the writer transaction"
    );
    assert_eq!(
        store
            .compaction_apply_runner(&edited.id, &edited_ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(
        store
            .compaction_apply_runner(
                &imported_checkpoint_edited.id,
                &imported_checkpoint_ready,
                None
            )
            .await
            .unwrap(),
        CommitOutcome::Applied,
        "an edited historical leaf invalidated an accepted published checkpoint"
    );
    db.execute_unprepared("DELETE FROM turn_event WHERE id='child-source'")
        .await
        .unwrap();
    let carried_after_delete = maintenance
        .compaction_prepare_accepted_checkpoint_import(
            "ws",
            "context-c",
            "turn-c",
            0,
            "child",
            &a_summary,
        )
        .await
        .expect("carrying a grant through its summary must not read the deleted raw payload");
    let projected_after_delete = descriptor(
        "recaptured-checkpoint-after-delete",
        std::slice::from_ref(&summary_target),
    );
    let carried_after_delete = vec![(0, carried_after_delete)];
    let carried_after_delete_digest = frozen_import_identity(&carried_after_delete).unwrap();
    maintenance
        .compaction_begin_frozen_history_with_imports(
            "ws",
            "context-c",
            &projected_after_delete,
            1,
            &carried_after_delete_digest,
        )
        .await
        .unwrap();
    maintenance
        .compaction_append_frozen_history(
            "ws",
            "context-c",
            &projected_after_delete.manifest_id,
            0,
            std::slice::from_ref(&summary_target),
        )
        .await
        .unwrap();
    maintenance
        .compaction_append_frozen_imports(
            "ws",
            "context-c",
            &projected_after_delete.manifest_id,
            0,
            &carried_after_delete,
        )
        .await
        .unwrap();
    assert!(
        maintenance
            .compaction_finish_frozen_history("ws", "context-c", &projected_after_delete)
            .await
            .unwrap()
    );
    let mut imported_checkpoint_deleted = projected_operation.clone();
    imported_checkpoint_deleted.id = "deleted-imported-checkpoint".into();
    imported_checkpoint_deleted.owner = "owner-deleted-imported-checkpoint".into();
    imported_checkpoint_deleted.plan.fingerprint = imported_checkpoint_deleted.id.clone();
    imported_checkpoint_deleted.projection_version = store
        .compaction_projection_version("ws", "context-c")
        .await
        .unwrap();
    imported_checkpoint_deleted.source_epochs.clear();
    for scope in ["child", "context-c", "thread"] {
        imported_checkpoint_deleted.source_epochs.insert(
            scope.into(),
            store
                .compaction_projection_version("ws", scope)
                .await
                .unwrap(),
        );
    }
    store
        .compaction_admit("ws", "context-c", &imported_checkpoint_deleted)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(&imported_checkpoint_deleted.id, "turn-c")
        .await
        .unwrap();
    store
        .compaction_bind_source_projection(&imported_checkpoint_deleted.id, &projected_after_delete)
        .await
        .unwrap();
    let imported_checkpoint_deleted_ready =
        ready_import_operation(&store, &imported_checkpoint_deleted, "child", &a_summary).await;
    assert!(
        store
            .compaction_manifest_sources_current(&imported_checkpoint_deleted.id)
            .await
            .unwrap(),
        "a deleted historical OWN leaf invalidated its accepted later summary"
    );
    assert_eq!(
        store
            .compaction_apply_runner(
                &imported_checkpoint_deleted.id,
                &imported_checkpoint_deleted_ready,
                None
            )
            .await
            .unwrap(),
        CommitOutcome::Applied
    );

    assert!(
        !store
            .compaction_finish_frozen_history("ws", "thread", &stale)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_frozen_import_page("ws", "thread", &context.manifest_id, 0)
            .await
            .unwrap(),
        records,
        "accepted metadata is retained"
    );
}

#[pioneer_postgres_test::postgres_test]
async fn frozen_own_import_treats_published_summary_as_atomic_output() {
    use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenMessageRef};
    use pioneer_compaction::runner::{RunnerState, SourceCursor};
    use sha2::{Digest, Sha256};

    fn descriptor(id: &str, messages: &[FrozenMessageRef]) -> FrozenHistoryRef {
        let mut digest = Sha256::new();
        for message in messages {
            let bytes = serde_json::to_vec(message).unwrap();
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes);
        }
        FrozenHistoryRef {
            format: 1,
            manifest_id: id.into(),
            messages: messages.len() as u64,
            identity_sha256: hex::encode(digest.finalize()),
        }
    }

    for delete_leaf in [false, true] {
        let store = store().await;
        let db = store.database_connection();
        for statement in [
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('portion-child','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('portion-turn','portion-child','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('portion-h','portion-child','portion-turn',1,'fixture',to_json('accepted H'::text),CURRENT_TIMESTAMP)",
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('portion-task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('portion-run','portion-task','portion-run',1,1,'succeeded','agent')",
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('portion-rt','portion-task','portion-run','portion-child','portion-turn','initial',0,1,'completed',CURRENT_TIMESTAMP)",
            "INSERT INTO task_result_candidate(id,task_id,run_id,task_run_turn_id,thread_id,turn_id,round,status,created_at,updated_at) VALUES ('portion-candidate','portion-task','portion-run','portion-rt','portion-child','portion-turn',0,'accepted',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('portion-delivery-turn','thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id) VALUES ('portion-delivery','ws','portion-task','portion-run','portion-key','thread','origin_thread','thread','delivered',1,1,'portion-delivery-turn')",
        ] {
            db.execute_unprepared(statement).await.unwrap();
        }
        let h_assertion = SourceAssertion {
            revision: Some(1),
            kind: CanonicalSource::Event,
            turn_id: "portion-turn".into(),
            id: "portion-h".into(),
            payload: serde_json::to_string("accepted H").unwrap(),
        };
        let h = h_assertion.reference();
        let operation =
            admit_import_operation(&store, "portion-output", "portion-child", "portion-turn").await;
        let mut raw = operation.clone();
        raw.plan.compact = vec![0];
        raw.plan.coverage = vec![h.clone()];
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE compaction_operation SET snapshot=$2 WHERE id=$1",
            [
                operation.id.clone().into(),
                serde_json::to_string(&raw).unwrap().into(),
            ],
        ))
        .await
        .unwrap();
        let budget = ModelBudget::new(None, None, None);
        store
            .compaction_prepare_runner(&operation.id, &budget, 1, 0)
            .await
            .unwrap();
        store
            .compaction_append_manifest(
                &operation.id,
                &[ManifestEntry {
                    ordinal: 0,
                    unit: 0,
                    reference_only: false,
                    thread_id: "portion-child".into(),
                    source: h.clone(),
                }],
            )
            .await
            .unwrap();
        let initial =
            RunnerState::new(operation.admission.deadline_ms, &budget, 1000, None).unwrap();
        store
            .compaction_activate_runner(&operation.id, &initial)
            .await
            .unwrap();
        let first_attempt = initial.claim(1).unwrap();
        assert!(
            store
                .compaction_runner_transition(
                    &operation.id,
                    initial.generation,
                    &first_attempt,
                    None,
                )
                .await
                .unwrap()
        );
        let p = Checkpoint {
            id: "portion-p".into(),
            operation_id: operation.id.clone(),
            format_version: 1,
            owner: operation.owner.clone(),
            previous: None,
            coverage: vec![],
            summary: "partial prefix".into(),
            selection: operation.admission.selection.clone(),
            projection_version: operation.projection_version,
        };
        let p_state = first_attempt
            .candidate(
                1,
                p.id.clone(),
                SourceCursor {
                    character: 1,
                    ..Default::default()
                },
                false,
                2,
            )
            .unwrap();
        assert!(
            store
                .compaction_runner_transition(
                    &operation.id,
                    first_attempt.generation,
                    &p_state,
                    Some(&p),
                )
                .await
                .unwrap()
        );
        let next = p_state.candidate_checked(true).unwrap();
        assert!(
            store
                .compaction_runner_transition(&operation.id, p_state.generation, &next, None)
                .await
                .unwrap()
        );
        let second_attempt = next.claim(3).unwrap();
        assert!(
            store
                .compaction_runner_transition(
                    &operation.id,
                    next.generation,
                    &second_attempt,
                    None,
                )
                .await
                .unwrap()
        );
        let k = Checkpoint {
            id: "portion-k".into(),
            operation_id: operation.id.clone(),
            format_version: 1,
            owner: operation.owner.clone(),
            previous: Some(p.id.clone()),
            coverage: vec![h.clone()],
            summary: "complete H".into(),
            selection: operation.admission.selection.clone(),
            projection_version: operation.projection_version,
        };
        let k_state = second_attempt
            .candidate(
                2,
                k.id.clone(),
                SourceCursor {
                    unit: 1,
                    ..Default::default()
                },
                true,
                4,
            )
            .unwrap();
        assert!(
            store
                .compaction_runner_transition(
                    &operation.id,
                    second_attempt.generation,
                    &k_state,
                    Some(&k),
                )
                .await
                .unwrap()
        );
        let ready = k_state.candidate_checked(true).unwrap();
        assert!(
            store
                .compaction_runner_transition(&operation.id, k_state.generation, &ready, None)
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .compaction_apply_runner(&operation.id, &ready, None)
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        let p_status = db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT status FROM compaction_checkpoint WHERE id=$1",
                [p.id.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<String>("", "status")
            .unwrap();
        assert_eq!(p_status, "retained");
        assert!(
            store
                .compaction_checkpoint_edges(&p.id)
                .await
                .unwrap()
                .unwrap()
                .coverage
                .is_empty()
        );
        let k_source = store
            .compaction_checkpoint_source("ws", "portion-child", &k.id)
            .await
            .unwrap()
            .unwrap();

        let output_message = FrozenMessageRef {
            logical_turn_id: None,
            context_thread: None,
            source_thread: "portion-child".into(),
            unit_id: "portion-output-unit".into(),
            sources: vec![k_source.clone()],
            event_input_role: None,
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            publication_aliases: None,
            inherited: false,
            complete: true,
            protected_input: false,
            wire_sha256: "d".repeat(64),
            replay_source: None,
            tool_item_id: None,
            tool_call_id: None,
            tool_name: None,
        };
        let output = descriptor(
            "portion-output-manifest",
            std::slice::from_ref(&output_message),
        );
        store
            .compaction_begin_frozen_history("ws", "portion-child", &output)
            .await
            .unwrap();
        store
            .compaction_append_frozen_history(
                "ws",
                "portion-child",
                &output.manifest_id,
                0,
                std::slice::from_ref(&output_message),
            )
            .await
            .unwrap();
        assert!(
            store
                .compaction_finish_frozen_history("ws", "portion-child", &output)
                .await
                .unwrap()
        );
        store
            .compaction_record_task_output("ws", "portion-rt", &output)
            .await
            .unwrap();
        db.execute_unprepared("INSERT INTO compaction_delivery_output(delivery_id,candidate_id,task_run_turn_id) VALUES ('portion-delivery','portion-candidate','portion-rt')").await.unwrap();
        append_item_completion(
            &store,
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "portion-delivery-turn".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: pioneer_protocol::task_delivery_result_item_id("portion-delivery"),
                    text: "delivered".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
        let acknowledgement = store
            .compaction_source_page(
                "ws",
                "thread",
                "portion-delivery-turn",
                PagedSource::Event,
                0,
            )
            .await
            .unwrap()
            .entries[0]
            .reference
            .clone();
        let prepared = store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "portion-delivery",
                &acknowledgement,
                0,
                "portion-child",
                &k_source,
            )
            .await
            .unwrap();
        assert_eq!(prepared.source(), &k_source);
        let imported_message = FrozenMessageRef {
            context_thread: Some("thread".into()),
            sources: vec![k_source.clone()],
            ..output_message.clone()
        };
        let imported = descriptor(
            "portion-import-manifest",
            std::slice::from_ref(&imported_message),
        );
        let imports = vec![(0, prepared)];
        let imports_digest = frozen_import_identity(&imports).unwrap();
        store
            .compaction_begin_frozen_history_with_imports(
                "ws",
                "thread",
                &imported,
                1,
                &imports_digest,
            )
            .await
            .unwrap();
        store
            .compaction_append_frozen_history(
                "ws",
                "thread",
                &imported.manifest_id,
                0,
                std::slice::from_ref(&imported_message),
            )
            .await
            .unwrap();
        store
            .compaction_append_frozen_imports("ws", "thread", &imported.manifest_id, 0, &imports)
            .await
            .unwrap();
        assert!(
            store
                .compaction_finish_frozen_history("ws", "thread", &imported)
                .await
                .unwrap()
        );

        db.execute_unprepared(
            "UPDATE compaction_checkpoint SET status='candidate' WHERE id='portion-p'",
        )
        .await
        .unwrap();
        let prepared_again = store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "portion-delivery",
                &acknowledgement,
                0,
                "portion-child",
                &k_source,
            )
            .await
            .unwrap();
        assert_eq!(
            prepared_again.source(),
            &k_source,
            "an unpublished predecessor invalidated an already published output summary"
        );
        let raw_error = store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "portion-delivery",
                &acknowledgement,
                0,
                "portion-child",
                &h,
            )
            .await
            .unwrap_err();
        assert_eq!(
            raw_error.to_string(),
            "source is outside the accepted own output message",
            "summary coverage granted direct access to its historical raw leaf"
        );
        db.execute_unprepared(if delete_leaf {
            "DELETE FROM turn_event WHERE id='portion-h'"
        } else {
            "UPDATE turn_event SET payload=to_json('changed H'::text) WHERE id='portion-h'"
        })
        .await
        .unwrap();
        let prepared_after_leaf_change = store
            .compaction_prepare_frozen_import(
                "ws",
                "thread",
                "portion-delivery",
                &acknowledgement,
                0,
                "portion-child",
                &k_source,
            )
            .await
            .unwrap();
        assert_eq!(
            prepared_after_leaf_change.source(),
            &k_source,
            "a changed or deleted historical leaf invalidated the output summary"
        );

        // A later TaskRun accepts the descriptor containing S=K itself. Carry
        // that atomic grant onto a distinct T whose historical input is K.
        for statement in [
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('portion-target-thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('portion-consumer','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('portion-consumer','thread','thread',1,CURRENT_TIMESTAMP)",
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('portion-consumer-turn','portion-consumer','in_progress','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('portion-consumer-task','ws','thread','thread','thread','turn','agent','running','Consumer','fixture')",
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('portion-consumer-run','portion-consumer-task','portion-consumer-run',1,1,'running','agent')",
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('portion-consumer-rt','portion-consumer-task','portion-consumer-run','portion-consumer','portion-consumer-turn','initial',0,1,'running',CURRENT_TIMESTAMP)",
        ] {
            db.execute_unprepared(statement).await.unwrap();
        }
        db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('portion-target-turn','portion-target-thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        let mut target_op = admit_import_operation(
            &store,
            "portion-target-operation",
            "portion-target-thread",
            "portion-target-turn",
        )
        .await;
        target_op.plan.compact = vec![0];
        target_op.plan.coverage = vec![k_source.clone()];
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE compaction_operation SET snapshot=$1 WHERE id=$2",
            [
                serde_json::to_string(&target_op).unwrap().into(),
                target_op.id.clone().into(),
            ],
        ))
        .await
        .unwrap();
        let target_checkpoint = Checkpoint {
            id: "portion-t".into(),
            operation_id: target_op.id.clone(),
            owner: target_op.owner.clone(),
            previous: None,
            coverage: vec![k_source.clone()],
            summary: "target T".into(),
            selection: target_op.admission.selection.clone(),
            projection_version: target_op.projection_version,
            format_version: 1,
        };
        store
            .compaction_save_candidate(&target_checkpoint, 0)
            .await
            .unwrap();
        // Model a pre-proof published legacy node with a real original raw
        // contract, exact identity and ownership, rather than malformed '{}'.
        db.execute_unprepared(
            "UPDATE compaction_checkpoint SET status='applied' WHERE id='portion-t'",
        )
        .await
        .unwrap();
        db.execute_unprepared("UPDATE compaction_operation SET status='completed' WHERE id='portion-target-operation'").await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "WITH frozen_root_fixture(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) AS (VALUES ('portion-consumer-run','portion-consumer-task','ws','thread',$1,CURRENT_TIMESTAMP)) INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,frozen_manifest_id) SELECT run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,CASE WHEN history_json IS JSON OBJECT THEN (history_json)::json->>'manifest_id' END FROM frozen_root_fixture",
            [serde_json::to_string(&imported).unwrap().into()],
        ))
        .await
        .unwrap();
        let t_source = store
            .compaction_checkpoint_source("ws", "portion-target-thread", "portion-t")
            .await
            .unwrap()
            .unwrap();
        let target_message = FrozenMessageRef {
            logical_turn_id: None,
            source_thread: "portion-target-thread".into(),
            context_thread: Some("portion-consumer".into()),
            unit_id: "portion-target-unit".into(),
            sources: vec![t_source.clone()],
            event_input_role: None,
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            publication_aliases: None,
            inherited: false,
            complete: true,
            protected_input: false,
            wire_sha256: "e".repeat(64),
            replay_source: None,
            tool_item_id: None,
            tool_call_id: None,
            tool_name: None,
        };

        async fn begin_target(
            store: &CompactionStore,
            id: &str,
            message: &FrozenMessageRef,
            prepared: PreparedFrozenImport,
        ) -> (FrozenHistoryRef, Vec<(u64, PreparedFrozenImport)>) {
            let target = descriptor(id, std::slice::from_ref(message));
            let imports = vec![(0, prepared)];
            let digest = frozen_import_identity(&imports).unwrap();
            store
                .compaction_begin_frozen_history_with_imports(
                    "ws",
                    "portion-consumer",
                    &target,
                    1,
                    &digest,
                )
                .await
                .unwrap();
            store
                .compaction_append_frozen_history(
                    "ws",
                    "portion-consumer",
                    &target.manifest_id,
                    0,
                    std::slice::from_ref(message),
                )
                .await
                .unwrap();
            (target, imports)
        }

        let unchanged = store
            .compaction_prepare_accepted_checkpoint_import(
                "ws",
                "portion-consumer",
                "portion-consumer-turn",
                0,
                "portion-target-thread",
                &t_source,
            )
            .await
            .expect("raw leaf mutation must not invalidate atomic S evidence");
        let (unchanged_target, unchanged_imports) =
            begin_target(&store, "portion-target-success", &target_message, unchanged).await;
        store
            .compaction_append_frozen_imports(
                "ws",
                "portion-consumer",
                &unchanged_target.manifest_id,
                0,
                &unchanged_imports,
            )
            .await
            .expect("an unchanged published S must permit the bounded append");

        let raced = store
            .compaction_prepare_accepted_checkpoint_import(
                "ws",
                "portion-consumer",
                "portion-consumer-turn",
                0,
                "portion-target-thread",
                &t_source,
            )
            .await
            .unwrap();
        let (raced_target, raced_imports) =
            begin_target(&store, "portion-target-raced", &target_message, raced).await;
        if delete_leaf {
            assert!(
                db.execute_unprepared("DELETE FROM compaction_checkpoint WHERE id='portion-k'")
                    .await
                    .is_err(),
                "sealed original grant evidence cannot be deleted"
            );
            // The accepted binding itself can disappear after preparation;
            // retain the immutable checkpoint and test that real writer race.
            db.execute_unprepared(
                "DELETE FROM task_run_conversation_snapshot WHERE run_id='portion-consumer-run'",
            )
            .await
            .unwrap();
        } else {
            db.execute_unprepared(
                "UPDATE compaction_checkpoint SET status='candidate' WHERE id='portion-k'",
            )
            .await
            .unwrap();
        }
        assert!(
            store
                .compaction_append_frozen_imports(
                    "ws",
                    "portion-consumer",
                    &raced_target.manifest_id,
                    0,
                    &raced_imports,
                )
                .await
                .is_err(),
            "writer accepted a checkpoint grant that changed after preparation"
        );
        let raced_state = db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT next_import,(SELECT COUNT(*) FROM compaction_frozen_import i WHERE i.manifest_id=h.id) AS stored FROM compaction_frozen_history h WHERE h.id=$1",
                [raced_target.manifest_id.into()],
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(raced_state.try_get::<i64>("", "next_import").unwrap(), 0);
        assert_eq!(raced_state.try_get::<i64>("", "stored").unwrap(), 0);
    }
}
