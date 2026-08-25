//! Database persistence tests for Apply Patch history.

use crate::apply_patch::history::{
    AppliedPatchRecord, AppliedPatchRecordOutcome, ChangeKind, CommitOrdinal, CommittedPatchChange,
    CommittedTextSnapshot, ContentAddressedSnapshotRef, DurablePatchChange, HistoryQueryLimits,
    InsertedPatchRecord, IntentStatus, InvocationIdentity, LineEnding, LineEndingMetadata,
    PatchHistoryCoverage, PatchRecoveryPlan, PatchSideEffects, PreparedChangeRecovery,
    SnapshotDomain, SqliteAppliedPatchStore, SqliteCommitIntentStore, SqliteSnapshotStore,
    SqliteTurnDiffStore, TextEncoding, TextSnapshotRef, TurnDiffAuthority, TurnDiffState,
    replay_turn_pages,
};
use migration::{Migrator, MigratorTrait};
use pioneer_crud::patch_history as crud;
use sea_orm::{Database, DatabaseConnection};
use std::path::Path;

async fn database() -> DatabaseConnection {
    let database = Database::connect("sqlite::memory:")
        .await
        .expect("connect patch-history test database");
    Migrator::up(&database, None)
        .await
        .expect("migrate patch-history test database");
    database
}

async fn file_database(path: &Path) -> DatabaseConnection {
    let database = Database::connect(format!("sqlite://{}?mode=rwc", path.display()))
        .await
        .expect("connect file-backed patch-history test database");
    Migrator::up(&database, None)
        .await
        .expect("migrate file-backed patch-history test database");
    database
}

fn snapshot(bytes: &[u8]) -> CommittedTextSnapshot {
    CommittedTextSnapshot::from_bytes(
        bytes.to_vec(),
        TextEncoding::Utf8,
        LineEndingMetadata {
            dominant: if bytes.contains(&b'\n') {
                LineEnding::Lf
            } else {
                LineEnding::None
            },
            mixed: false,
            final_newline: bytes.ends_with(b"\n"),
        },
    )
}

fn change(
    sequence: u32,
    kind: ChangeKind,
    source_path: &str,
    destination_path: Option<&str>,
    before: Option<&CommittedTextSnapshot>,
    after: Option<&CommittedTextSnapshot>,
) -> CommittedPatchChange {
    CommittedPatchChange {
        operation_index: sequence,
        commit_step: sequence as u16,
        sequence,
        kind,
        source_path: source_path.to_owned(),
        destination_path: destination_path.map(str::to_owned),
        before: before.cloned(),
        after: after.cloned(),
        overwritten_destination: None,
        side_effects: PatchSideEffects::default(),
    }
}

fn record(
    invocation_id: &str,
    ordinal: u64,
    changes: &[CommittedPatchChange],
) -> AppliedPatchRecord {
    record_for("thread", "turn", invocation_id, ordinal, changes)
}

fn record_for(
    thread_id: &str,
    turn_id: &str,
    invocation_id: &str,
    ordinal: u64,
    changes: &[CommittedPatchChange],
) -> AppliedPatchRecord {
    let mut record = AppliedPatchRecord::new(
        InvocationIdentity::new(thread_id, turn_id, invocation_id).unwrap(),
        CommitOrdinal(ordinal),
        AppliedPatchRecordOutcome::Applied,
        changes.iter().map(DurablePatchChange::from).collect(),
    );
    record.environment_id = "workspace".to_owned();
    record.committed_at_unix_ms = ordinal as i64 + 1;
    record
}

fn prepared_update(
    operation_index: u32,
    path: &str,
    before: &CommittedTextSnapshot,
    after: &CommittedTextSnapshot,
) -> PreparedChangeRecovery {
    PreparedChangeRecovery {
        operation_index,
        kind: ChangeKind::Update,
        source_path: path.to_owned(),
        destination_path: None,
        before: Some(before.clone()),
        after: Some(after.clone()),
        overwritten_destination: None,
        side_effects: PatchSideEffects::default(),
    }
}

fn recovery_plan(root: &Path, changes: Vec<PreparedChangeRecovery>) -> PatchRecoveryPlan {
    PatchRecoveryPlan {
        environment_id: "workspace".to_owned(),
        workspace_root: root.to_string_lossy().into_owned(),
        authority: TurnDiffAuthority::NativePatchEngine,
        changes,
        parent_directories: Vec::new(),
    }
}

#[tokio::test]
async fn sqlite_history_survives_reload_and_rebuilds_projection_from_snapshots() {
    let database = database().await;
    let records = SqliteAppliedPatchStore::new(database.clone());
    let snapshots = SqliteSnapshotStore::new(database.clone());
    let intents = SqliteCommitIntentStore::new(database.clone());
    let projections = SqliteTurnDiffStore::new(database.clone());
    let domain = SnapshotDomain::new("thread:thread", "pioneer", "thread_history");
    let original = snapshot(b"old\n");
    let edited = snapshot(b"edited\n");

    let first_change = change(
        0,
        ChangeKind::Update,
        "a.txt",
        None,
        Some(&original),
        Some(&edited),
    );
    let first_record = record("call-1", 0, std::slice::from_ref(&first_change));
    assert!(matches!(
        records
            .insert_with_snapshots(
                first_record.clone(),
                [1; 32],
                &domain,
                &[original.clone(), edited.clone()],
            )
            .await
            .unwrap(),
        InsertedPatchRecord::Inserted(_)
    ));

    // At-least-once delivery must neither duplicate the record nor inflate
    // snapshot reference counts.
    assert!(matches!(
        records
            .insert_with_snapshots(
                first_record,
                [1; 32],
                &domain,
                &[original.clone(), edited.clone()],
            )
            .await
            .unwrap(),
        InsertedPatchRecord::Existing(_)
    ));

    let move_change = change(
        0,
        ChangeKind::Move,
        "a.txt",
        Some("b.txt"),
        Some(&edited),
        Some(&edited),
    );
    records
        .insert_with_snapshots(
            record("call-2", 1, std::slice::from_ref(&move_change)),
            [2; 32],
            &domain,
            &[edited.clone(), edited.clone()],
        )
        .await
        .unwrap();

    let first_page = records
        .query_turn_steps(
            "thread",
            "turn",
            None,
            HistoryQueryLimits {
                max_page_records: 1,
                ..HistoryQueryLimits::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(first_page.items.len(), 1);
    assert_eq!(first_page.next_cursor, Some(CommitOrdinal(0)));
    let second_page = records
        .query_turn_steps(
            "thread",
            "turn",
            first_page.next_cursor,
            HistoryQueryLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(second_page.items.len(), 1);
    assert_eq!(
        second_page.items[0].record.record.commit_ordinal,
        CommitOrdinal(1)
    );
    let first_identity = InvocationIdentity::new("thread", "turn", "call-1").unwrap();
    let first_record_id = crate::apply_patch::history::applied_patch_record_id(&first_identity, 0);
    assert_eq!(
        records
            .get_by_record_id("thread", "turn", &first_record_id)
            .await
            .unwrap()
            .expect("record id lookup should resolve")
            .record
            .identity,
        first_identity
    );
    assert!(
        records
            .get_by_record_id("other-thread", "turn", &first_record_id)
            .await
            .unwrap()
            .is_none(),
        "record IDs must not bypass the authorized owning-thread scope"
    );

    let file_history = records
        .query_file_history("thread", "b.txt", None, HistoryQueryLimits::default())
        .await
        .unwrap();
    assert_eq!(file_history.items.len(), 2);
    assert_eq!(file_history.items[0].change.source_path, "a.txt");
    assert_eq!(
        file_history.items[1].change.destination_path.as_deref(),
        Some("b.txt")
    );

    let edited_reference = ContentAddressedSnapshotRef {
        domain_id: domain.id(),
        snapshot: TextSnapshotRef::from_snapshot(&edited),
    };
    assert_eq!(
        snapshots.get(&edited_reference).await.unwrap().bytes,
        b"edited\n"
    );
    let stored_first = records
        .get(&InvocationIdentity::new("thread", "turn", "call-1").unwrap())
        .await
        .unwrap()
        .expect("first record should exist");
    let record_diff = records
        .render_record_diff(&stored_first, 64 * 1024)
        .await
        .unwrap();
    assert!(record_diff.unified_patch.contains("-old\n"));
    assert!(record_diff.unified_patch.contains("+edited\n"));
    assert_eq!(record_diff.records_rendered, 1);
    assert!(record_diff.exactness.is_exact());

    let boundary_diff = records
        .render_turn_diff_between("thread", "turn", None, Some(CommitOrdinal(1)), 64 * 1024)
        .await
        .unwrap();
    assert!(boundary_diff.unified_patch.contains("--- a/a.txt"));
    assert!(boundary_diff.unified_patch.contains("+++ b/b.txt"));
    assert_eq!(boundary_diff.records_rendered, 2);
    assert!(boundary_diff.exactness.is_exact());

    let metrics = snapshots.metrics().await.unwrap();
    assert_eq!(metrics.blobs, 2, "identical versions must be deduplicated");

    let replay = replay_turn_pages(&records, &intents, "thread", "turn", 1)
        .await
        .unwrap();
    assert_eq!(replay.aggregate.changes.len(), 1);
    assert_eq!(replay.aggregate.changes[0].source_path, "a.txt");
    assert_eq!(
        replay.aggregate.changes[0].destination_path.as_deref(),
        Some("b.txt")
    );
    assert_eq!(
        replay.aggregate.coverage,
        PatchHistoryCoverage::EngineVerifiedSteps
    );
    let expected = TurnDiffState::from_aggregate(
        replay.aggregate,
        TurnDiffAuthority::NativePatchEngine,
        replay.revision,
        true,
    );
    projections.repair_live(&expected).await.unwrap();
    assert_eq!(
        projections.get("thread", "turn").await.unwrap(),
        Some(expected.clone())
    );

    assert_eq!(
        crud::delete_turn_diff_state(&database, "thread", "turn")
            .await
            .unwrap(),
        1
    );
    assert!(projections.get("thread", "turn").await.unwrap().is_none());
    projections.repair_live(&expected).await.unwrap();
    assert_eq!(
        projections.get("thread", "turn").await.unwrap(),
        Some(expected)
    );
}

#[tokio::test]
async fn unresolved_sqlite_recovery_records_a_gap_without_a_fake_path() {
    let database = database().await;
    let records = SqliteAppliedPatchStore::new(database.clone());
    let intents = SqliteCommitIntentStore::new(database);
    let identity = InvocationIdentity::new("thread", "turn", "crashed-call").unwrap();
    let recovery_plan = PatchRecoveryPlan {
        environment_id: "workspace".to_owned(),
        // Accepted by the bounded durable schema but deliberately not an
        // absolute path, so startup recovery cannot claim a filesystem fact.
        workspace_root: "relative-workspace".to_owned(),
        authority: TurnDiffAuthority::NativePatchEngine,
        changes: Vec::new(),
        parent_directories: Vec::new(),
    };
    intents
        .begin_next_owned(identity.clone(), [9; 32], Vec::new(), recovery_plan)
        .await
        .unwrap();

    assert_eq!(intents.terminalize_pending_gaps(&records).await.unwrap(), 1);
    let stored = records.get(&identity).await.unwrap().unwrap();
    assert!(matches!(
        stored.record.outcome,
        AppliedPatchRecordOutcome::Gap { .. }
    ));
    assert!(stored.record.changes.is_empty());

    let replay = replay_turn_pages(&records, &intents, "thread", "turn", 1)
        .await
        .unwrap();
    assert!(replay.aggregate.changes.is_empty());
    assert!(!replay.aggregate.exact);
    assert!(matches!(
        replay.aggregate.coverage,
        PatchHistoryCoverage::Incomplete { .. }
    ));
}

#[tokio::test]
async fn side_effect_only_record_round_trips_without_a_fake_file_change() {
    let database = database().await;
    let records = SqliteAppliedPatchStore::new(database.clone());
    let intents = SqliteCommitIntentStore::new(database);
    let domain = SnapshotDomain::new("thread:thread", "pioneer", "thread_history");
    let identity = InvocationIdentity::new("thread", "turn", "residual-parent").unwrap();
    let mut record = AppliedPatchRecord::new(
        identity.clone(),
        CommitOrdinal(0),
        AppliedPatchRecordOutcome::Partial {
            failed_stage: crate::apply_patch::history::PatchStage::Stage,
            error_code: crate::apply_patch::history::PatchErrorCode::Io,
        },
        Vec::new(),
    );
    record.side_effects.residual_directories = vec!["nested".to_owned()];
    record.side_effects.exact = false;
    record.exactness = crate::apply_patch::history::PatchRecordExactness::Uncertain;

    records
        .insert_with_snapshots(record.clone(), [8; 32], &domain, &[])
        .await
        .unwrap();
    let stored = records.get(&identity).await.unwrap().unwrap();
    assert_eq!(stored.record, record);
    assert!(stored.record.changes.is_empty());
    assert!(!stored.record.is_empty());

    let delta = records.materialize_delta(&stored).await.unwrap();
    assert!(delta.changes.is_empty());
    assert_eq!(delta.side_effects, record.side_effects);
    assert!(!delta.is_empty());

    let replay = replay_turn_pages(&records, &intents, "thread", "turn", 1)
        .await
        .unwrap();
    assert!(replay.aggregate.changes.is_empty());
    assert!(!replay.aggregate.exact);
}

#[tokio::test]
async fn snapshot_reconciliation_decodes_canonical_v1_record_delta() {
    let database = database().await;
    let records = SqliteAppliedPatchStore::new(database.clone());
    let snapshots = SqliteSnapshotStore::new(database);
    let domain = SnapshotDomain::new("thread:thread", "pioneer", "thread_history");
    let original = snapshot(b"old\n");
    let edited = snapshot(b"edited\n");
    let committed = change(
        0,
        ChangeKind::Update,
        "a.txt",
        None,
        Some(&original),
        Some(&edited),
    );
    let mut applied = record("reconcile-v1", 0, &[committed]);
    applied.side_effects.created_directories = vec!["nested".to_owned()];

    records
        .insert_with_snapshots(applied, [4; 32], &domain, &[original, edited])
        .await
        .unwrap();

    let report = snapshots.reconcile_references().await.unwrap();
    assert_eq!(report.repaired_references, 0);
    assert_eq!(report.collected_blobs, 0);
}

#[tokio::test]
async fn recovery_journals_a_residual_authorized_parent_as_a_side_effect() {
    let database = database().await;
    let records = SqliteAppliedPatchStore::new(database.clone());
    let intents = SqliteCommitIntentStore::new(database);
    let workspace = tempfile::tempdir().unwrap();
    let workspace_root = workspace.path().canonicalize().unwrap();
    let parent = workspace_root.join("nested");
    let identity = InvocationIdentity::new("thread", "turn", "parent-residual").unwrap();
    let recovery_plan = PatchRecoveryPlan {
        environment_id: "workspace".to_owned(),
        workspace_root: workspace_root.to_string_lossy().into_owned(),
        authority: TurnDiffAuthority::NativePatchEngine,
        changes: Vec::new(),
        parent_directories: vec![crate::apply_patch::history::PreparedDirectoryRecovery {
            path: "nested".to_owned(),
            existed: false,
            fingerprint: crate::apply_patch::file_mutation::metadata_fingerprint_for_path(&parent)
                .unwrap(),
        }],
    };
    intents
        .begin_next_owned(identity.clone(), [7; 32], Vec::new(), recovery_plan)
        .await
        .unwrap();
    std::fs::create_dir(&parent).unwrap();

    assert_eq!(intents.terminalize_pending_gaps(&records).await.unwrap(), 1);
    let stored = records.get(&identity).await.unwrap().unwrap();
    assert!(matches!(
        stored.record.outcome,
        AppliedPatchRecordOutcome::Gap { .. }
    ));
    assert!(stored.record.changes.is_empty());
    assert_eq!(
        stored.record.side_effects.residual_directories,
        vec!["nested".to_owned()]
    );
    assert!(!stored.record.side_effects.exact);
    assert_eq!(
        stored.record.exactness,
        crate::apply_patch::history::PatchRecordExactness::Uncertain
    );
}

#[tokio::test]
async fn restart_recovery_distinguishes_precommit_from_an_exact_partial_prefix() {
    let root = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let workspace_root = workspace.path().canonicalize().unwrap();
    let database_path = root.path().join("history.sqlite");
    let before = snapshot(b"before\n");
    let middle = snapshot(b"middle\n");
    let after = snapshot(b"after\n");
    let untouched_path = workspace_root.join("untouched.txt");
    let partial_path = workspace_root.join("partial.txt");
    std::fs::write(&untouched_path, before.bytes.as_slice()).unwrap();
    std::fs::write(&partial_path, middle.bytes.as_slice()).unwrap();
    let untouched_identity =
        InvocationIdentity::new("thread", "turn-before", "call-before").unwrap();
    let partial_identity =
        InvocationIdentity::new("thread", "turn-partial", "call-partial").unwrap();

    {
        let database = file_database(&database_path).await;
        let intents = SqliteCommitIntentStore::new(database);
        intents
            .begin_next_owned(
                untouched_identity.clone(),
                [1; 32],
                vec![[11; 32]],
                recovery_plan(
                    &workspace_root,
                    vec![prepared_update(0, "untouched.txt", &before, &middle)],
                ),
            )
            .await
            .unwrap();
        intents
            .begin_next_owned(
                partial_identity.clone(),
                [2; 32],
                vec![[21; 32], [22; 32]],
                recovery_plan(
                    &workspace_root,
                    vec![
                        prepared_update(0, "partial.txt", &before, &middle),
                        prepared_update(1, "partial.txt", &middle, &after),
                    ],
                ),
            )
            .await
            .unwrap();
    }

    let database = file_database(&database_path).await;
    let records = SqliteAppliedPatchStore::new(database.clone());
    let intents = SqliteCommitIntentStore::new(database.clone());
    assert_eq!(intents.terminalize_pending_gaps(&records).await.unwrap(), 2);
    assert!(records.get(&untouched_identity).await.unwrap().is_none());
    assert_eq!(
        intents
            .get(&untouched_identity)
            .await
            .unwrap()
            .unwrap()
            .status,
        IntentStatus::FailedNoChange
    );
    assert_eq!(std::fs::read(&untouched_path).unwrap(), before.bytes);

    let partial = records.get(&partial_identity).await.unwrap().unwrap();
    assert!(matches!(
        partial.record.outcome,
        AppliedPatchRecordOutcome::Partial {
            failed_stage: crate::apply_patch::history::PatchStage::Recover,
            ..
        }
    ));
    assert_eq!(partial.record.changes.len(), 1);
    assert_eq!(partial.record.changes[0].operation_index, 0);
    assert_eq!(std::fs::read(&partial_path).unwrap(), middle.bytes);
    let projection = SqliteTurnDiffStore::new(database)
        .get("thread", "turn-partial")
        .await
        .unwrap()
        .expect("recovery must rebuild the durable projection");
    assert_eq!(projection.record_count, 1);
    assert_eq!(projection.revision, 1);
    assert_eq!(intents.terminalize_pending_gaps(&records).await.unwrap(), 0);
    assert_eq!(
        records
            .query_turn_steps(
                "thread",
                "turn-partial",
                None,
                HistoryQueryLimits::default(),
            )
            .await
            .unwrap()
            .items
            .len(),
        1,
        "recovery replay must not append a second record"
    );
}

#[tokio::test]
async fn restart_after_record_insert_repairs_projection_and_compacts_intent_once() {
    let root = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let workspace_root = workspace.path().canonicalize().unwrap();
    let database_path = root.path().join("history.sqlite");
    let before = snapshot(b"before\n");
    let after = snapshot(b"after\n");
    std::fs::write(workspace_root.join("file.txt"), after.bytes.as_slice()).unwrap();
    let identity = InvocationIdentity::new("thread", "turn", "call").unwrap();
    let committed = change(
        0,
        ChangeKind::Update,
        "file.txt",
        None,
        Some(&before),
        Some(&after),
    );
    let plan = recovery_plan(
        &workspace_root,
        vec![prepared_update(0, "file.txt", &before, &after)],
    );

    {
        let database = file_database(&database_path).await;
        let intents = SqliteCommitIntentStore::new(database.clone());
        let admitted = intents
            .begin_next_owned(identity.clone(), [3; 32], vec![[31; 32]], plan.clone())
            .await
            .unwrap();
        assert!(matches!(
            admitted,
            crate::apply_patch::history::BeginNextOutcome::Inserted(_)
        ));
        SqliteAppliedPatchStore::new(database)
            .insert_with_snapshots(
                record_for("thread", "turn", "call", 0, &[committed]),
                [3; 32],
                &SnapshotDomain::new("thread:thread", "pioneer", "thread_history"),
                &[before.clone(), after.clone()],
            )
            .await
            .unwrap();
        // Deliberately leave the intent Pending and omit projection/compaction
        // to model a process exit at the record-publication boundary.
    }

    let database = file_database(&database_path).await;
    let records = SqliteAppliedPatchStore::new(database.clone());
    let intents = SqliteCommitIntentStore::new(database.clone());
    assert_eq!(intents.terminalize_pending_gaps(&records).await.unwrap(), 1);
    assert_eq!(intents.terminalize_pending_gaps(&records).await.unwrap(), 0);
    assert_eq!(
        intents.get(&identity).await.unwrap().unwrap().status,
        IntentStatus::Promoted
    );
    assert!(matches!(
        intents
            .begin_next_owned(identity.clone(), [3; 32], vec![[31; 32]], plan)
            .await
            .unwrap(),
        crate::apply_patch::history::BeginNextOutcome::Existing(_)
    ));
    let projection = SqliteTurnDiffStore::new(database)
        .get("thread", "turn")
        .await
        .unwrap()
        .expect("record replay must repair a missing aggregate");
    assert_eq!(projection.record_count, 1);
    assert_eq!(projection.applied_through_ordinal, Some(CommitOrdinal(0)));
    assert_eq!(
        records
            .query_turn_steps("thread", "turn", None, HistoryQueryLimits::default())
            .await
            .unwrap()
            .items
            .len(),
        1
    );
}

#[tokio::test]
async fn final_projection_survives_restart_and_rejects_late_live_state() {
    let root = tempfile::tempdir().unwrap();
    let database_path = root.path().join("history.sqlite");
    let before = snapshot(b"before\n");
    let after = snapshot(b"after\n");
    let committed = change(
        0,
        ChangeKind::Update,
        "file.txt",
        None,
        Some(&before),
        Some(&after),
    );
    let final_state;
    {
        let database = file_database(&database_path).await;
        let records = SqliteAppliedPatchStore::new(database.clone());
        let intents = SqliteCommitIntentStore::new(database.clone());
        records
            .insert_with_snapshots(
                record("final-call", 0, &[committed]),
                [7; 32],
                &SnapshotDomain::new("thread:thread", "pioneer", "thread_history"),
                &[before, after],
            )
            .await
            .unwrap();
        let replay = replay_turn_pages(&records, &intents, "thread", "turn", 1)
            .await
            .unwrap();
        final_state = TurnDiffState::from_aggregate(
            replay.aggregate,
            TurnDiffAuthority::NativePatchEngine,
            replay.revision,
            true,
        );
        assert!(
            SqliteTurnDiffStore::new(database)
                .upsert(&final_state)
                .await
                .unwrap()
        );
    }

    let database = file_database(&database_path).await;
    let projections = SqliteTurnDiffStore::new(database);
    assert_eq!(
        projections.get("thread", "turn").await.unwrap(),
        Some(final_state.clone())
    );
    assert!(!projections.upsert(&final_state).await.unwrap());
    let mut late_live = final_state;
    late_live.final_state = false;
    late_live.revision = late_live.revision.saturating_add(1);
    assert!(projections.upsert(&late_live).await.is_err());
}

#[tokio::test]
async fn earlier_pending_ordinal_keeps_a_later_record_durable_and_explicitly_incomplete() {
    let database = database().await;
    let records = SqliteAppliedPatchStore::new(database.clone());
    let intents = SqliteCommitIntentStore::new(database.clone());
    let workspace = tempfile::tempdir().unwrap();
    let workspace_root = workspace.path().canonicalize().unwrap();
    let before = snapshot(b"before\n");
    let after = snapshot(b"after\n");
    std::fs::write(workspace_root.join("pending.txt"), before.bytes.as_slice()).unwrap();
    intents
        .begin_next_owned(
            InvocationIdentity::new("thread", "turn", "pending-call").unwrap(),
            [8; 32],
            vec![[81; 32]],
            recovery_plan(
                &workspace_root,
                vec![prepared_update(0, "pending.txt", &before, &after)],
            ),
        )
        .await
        .unwrap();
    let later = change(
        0,
        ChangeKind::Update,
        "later.txt",
        None,
        Some(&before),
        Some(&after),
    );
    records
        .insert_with_snapshots(
            record("later-call", 1, &[later]),
            [9; 32],
            &SnapshotDomain::new("thread:thread", "pioneer", "thread_history"),
            &[before, after],
        )
        .await
        .unwrap();

    let replay = replay_turn_pages(&records, &intents, "thread", "turn", 1)
        .await
        .unwrap();
    assert_eq!(replay.pending_ordinals, 1);
    assert_eq!(replay.revision, 2);
    assert_eq!(replay.aggregate.record_count, 1);
    assert!(!replay.aggregate.exact);
    assert!(matches!(
        replay.aggregate.coverage,
        PatchHistoryCoverage::Incomplete { .. }
    ));
    let page = records
        .query_turn_steps("thread", "turn", None, HistoryQueryLimits::default())
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.coverage.first_missing_ordinal, Some(CommitOrdinal(0)));
}

#[tokio::test]
async fn record_and_snapshot_promotion_is_atomic_and_retryable_after_injected_failure() {
    let database = database().await;
    let records = SqliteAppliedPatchStore::new(database.clone());
    let snapshots = SqliteSnapshotStore::new(database.clone());
    let before = snapshot(b"before\n");
    let after = snapshot(b"after\n");
    let committed = change(
        0,
        ChangeKind::Update,
        "file.txt",
        None,
        Some(&before),
        Some(&after),
    );
    let record = record_for("atomic", "turn", "call", 0, &[committed]);
    let domain = SnapshotDomain::new("thread:atomic", "pioneer", "thread_history");
    snapshots
        .reserve_for_intent(&record.identity, &domain, &[before.clone(), after.clone()])
        .await
        .unwrap();
    records.inject_next_record_insert_failure();
    assert!(
        records
            .insert_with_snapshots(
                record.clone(),
                [4; 32],
                &domain,
                &[before.clone(), after.clone()],
            )
            .await
            .is_err()
    );
    assert!(records.get(&record.identity).await.unwrap().is_none());
    assert_eq!(snapshots.metrics().await.unwrap().blobs, 0);

    assert!(matches!(
        records
            .insert_with_snapshots(
                record.clone(),
                [4; 32],
                &domain,
                &[before.clone(), after.clone()],
            )
            .await
            .unwrap(),
        InsertedPatchRecord::Inserted(_)
    ));
    let metrics = snapshots.metrics().await.unwrap();
    assert_eq!(metrics.blobs, 2);
    assert_eq!(metrics.references, 2);
    assert!(matches!(
        records
            .insert_with_snapshots(record, [4; 32], &domain, &[before, after])
            .await
            .unwrap(),
        InsertedPatchRecord::Existing(_)
    ));
    assert_eq!(snapshots.metrics().await.unwrap(), metrics);
}

#[tokio::test]
async fn corrupt_snapshot_never_crosses_domain_and_thread_gc_preserves_other_history() {
    let database = database().await;
    let records = SqliteAppliedPatchStore::new(database.clone());
    let snapshots = SqliteSnapshotStore::new(database.clone());
    let before = snapshot(b"shared-before\n");
    let after = snapshot(b"shared-after\n");
    let committed = change(
        0,
        ChangeKind::Update,
        "file.txt",
        None,
        Some(&before),
        Some(&after),
    );
    let domain_a = SnapshotDomain::new("thread:thread-a", "pioneer", "thread_history");
    let domain_b = SnapshotDomain::new("thread:thread-b", "pioneer", "thread_history");
    records
        .insert_with_snapshots(
            record_for(
                "thread-a",
                "turn",
                "call-a",
                0,
                std::slice::from_ref(&committed),
            ),
            [5; 32],
            &domain_a,
            &[before.clone(), after.clone()],
        )
        .await
        .unwrap();
    records
        .insert_with_snapshots(
            record_for("thread-b", "turn", "call-b", 0, &[committed]),
            [6; 32],
            &domain_b,
            &[before.clone(), after.clone()],
        )
        .await
        .unwrap();
    assert_eq!(
        snapshots.metrics().await.unwrap().blobs,
        4,
        "privacy domains must not cross-deduplicate identical bytes"
    );

    let stored = crud::find_patch_snapshot(
        &database,
        &domain_a.id(),
        before.version.token.digest(),
        before.bytes.len() as i64,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        crud::replace_patch_snapshot(
            &database,
            crud::PatchSnapshotWrite {
                domain_id: stored.domain_id,
                content_hash: stored.content_hash,
                byte_len: stored.byte_len,
                encoding: stored.encoding,
                line_endings_json: stored.line_endings_json,
                compressed_bytes: vec![0xff, 0x00],
                raw_byte_len: stored.raw_byte_len,
                ref_count: stored.ref_count,
            },
            stored.ref_count,
        )
        .await
        .unwrap(),
        1
    );
    let reference_a = ContentAddressedSnapshotRef {
        domain_id: domain_a.id(),
        snapshot: TextSnapshotRef::from_snapshot(&before),
    };
    let reference_b = ContentAddressedSnapshotRef {
        domain_id: domain_b.id(),
        snapshot: TextSnapshotRef::from_snapshot(&before),
    };
    assert!(snapshots.get(&reference_a).await.is_err());
    assert_eq!(
        snapshots.get(&reference_b).await.unwrap().bytes,
        before.bytes
    );

    assert_eq!(records.delete_thread("thread-a").await.unwrap(), 1);
    assert!(
        records
            .get(&InvocationIdentity::new("thread-a", "turn", "call-a").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        records
            .get(&InvocationIdentity::new("thread-b", "turn", "call-b").unwrap())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(snapshots.metrics().await.unwrap().blobs, 2);
    assert_eq!(
        snapshots.get(&reference_b).await.unwrap().bytes,
        before.bytes
    );
}

#[test]
fn persistence_stays_behind_the_crud_seaorm_boundary() {
    const RUNTIME_SOURCES: &[(&str, &str)] = &[
        ("db_store.rs", include_str!("db_store.rs")),
        ("db_intent.rs", include_str!("db_intent.rs")),
        ("db_snapshots.rs", include_str!("db_snapshots.rs")),
        ("db_projection.rs", include_str!("db_projection.rs")),
        ("db_codex.rs", include_str!("db_codex.rs")),
    ];
    const PERSISTENCE_SOURCES: &[(&str, &str)] = &[
        (
            "pioneer-crud patch_history repository",
            include_str!("../../../../crud/src/repositories/patch_history.rs"),
        ),
        (
            "Apply Patch history migration",
            include_str!("../../../../migration/src/m20260822_000001_patch_history.rs"),
        ),
    ];
    const RAW_SQL_MARKERS: &[&str] = &[
        "Statement::from_",
        ".execute_raw(",
        ".query_one_raw(",
        ".query_all_raw(",
        ".execute_unprepared(",
        "from_sql_and_values(",
        "Expr::cust(",
        "\"SELECT ",
        "\"INSERT ",
        "\"UPDATE ",
        "\"DELETE ",
        "\"CREATE TABLE ",
        "\"CREATE TEMP TABLE ",
        "\"DROP TABLE ",
    ];

    for (name, source) in RUNTIME_SOURCES {
        assert!(
            source.contains("pioneer_crud"),
            "{name} must route persistence through pioneer-crud"
        );
        assert!(
            !source.contains("pioneer_entity"),
            "{name} must not bypass pioneer-crud through entities"
        );
    }

    for (name, source) in RUNTIME_SOURCES.iter().chain(PERSISTENCE_SOURCES) {
        for marker in RAW_SQL_MARKERS {
            assert!(
                !source.contains(marker),
                "{name} must use SeaORM/SeaQuery builders instead of raw SQL marker `{marker}`"
            );
        }
    }

    let manifest = include_str!("../../../Cargo.toml");
    assert!(manifest.contains("pioneer-crud.workspace = true"));
    assert!(!manifest.contains("pioneer-entity"));
}

#[tokio::test]
async fn file_history_queries_use_their_path_indexes() {
    let database = database().await;
    let path = ["file.txt".to_owned()];

    let source_plan = crud::explain_patch_change_index_path_query(&database, "thread", &path, true)
        .await
        .unwrap();
    assert!(
        source_plan
            .iter()
            .any(|detail| detail.contains("idx_patch_change_index_thread_source_order")),
        "source path lookup must use its index: {source_plan:?}"
    );

    let destination_plan =
        crud::explain_patch_change_index_path_query(&database, "thread", &path, false)
            .await
            .unwrap();
    assert!(
        destination_plan
            .iter()
            .any(|detail| detail.contains("idx_patch_change_index_thread_destination_order")),
        "destination path lookup must use its index: {destination_plan:?}"
    );
}
