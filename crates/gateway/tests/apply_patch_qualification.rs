//! Explicit apply-patch performance/resource qualification.
//!
//! These tests are ignored by the ordinary workspace suite because they use
//! near-limit fixtures and wall-clock ceilings. Run them serially on release
//! hardware with:
//!
//! `cargo test -p pioneer-gateway --test apply_patch_qualification -- --ignored --nocapture --test-threads=1`

use migration::{Migrator, MigratorTrait};
use pioneer_crud::patch_history as crud;
use pioneer_tools::apply_patch::file_mutation::{
    AllowAllReadAccess, PaginatedReader, PatchLimits, PatchRequest, PatchRequestSource,
    ReadRequest, SnapshotLimits, TargetResolver,
};
use pioneer_tools::apply_patch::history::{
    AppliedPatchRecord, AppliedPatchRecordOutcome, ChangeKind, CommitOrdinal,
    CommittedTextSnapshot, DurablePatchChange, HistoryQueryLimits, InvocationIdentity, LineEnding,
    LineEndingMetadata, PatchSideEffects, SnapshotDomain, SqliteAppliedPatchStore,
    SqliteCommitIntentStore, SqliteSnapshotStore, SqliteTurnDiffStore, TextEncoding,
    TextSnapshotRef, TurnDiffAuthority, TurnDiffState, replay_turn_pages,
};
use pioneer_tools::apply_patch::{
    Hunk, HunkLine, UpdateFile, apply_update_with_candidate_limit, parse,
};
use sea_orm::Database;
use serde_json::json;
use std::hint::black_box;
use std::time::{Duration, Instant};

const ABSOLUTE_PARSER_BATCH_CEILING: Duration = Duration::from_secs(5);
const ABSOLUTE_MATCHER_BATCH_CEILING: Duration = Duration::from_secs(5);
const ABSOLUTE_NEAR_LIMIT_READ_CEILING: Duration = Duration::from_secs(5);
const ABSOLUTE_APPEND_P95_CEILING: Duration = Duration::from_millis(250);
const ABSOLUTE_APPEND_TOTAL_CEILING: Duration = Duration::from_secs(30);
const ABSOLUTE_REPLAY_CEILING: Duration = Duration::from_secs(5);
const ABSOLUTE_QUERY_CEILING: Duration = Duration::from_secs(5);
const RELATIVE_LINEAR_SLACK: u128 = 4;
const CONFIGURED_MAX_RECORD_FIXTURE: usize = 256;

#[test]
#[ignore = "explicit apply-patch release qualification"]
fn parser_matcher_and_streaming_read_resource_gates() {
    let parser_sizes = [16usize, 64, 128, 256];
    let parser_rounds = 20usize;
    let mut parser_medians = Vec::new();
    for operations in parser_sizes {
        let patch = add_patch(operations);
        let request = PatchRequest::from_provider_text(
            &patch,
            PatchRequestSource::NativeFreeform,
            PatchLimits::default(),
        )
        .unwrap();
        let _ = parse(&request, PatchLimits::default()).unwrap();
        let median = median_duration(
            (0..7)
                .map(|_| {
                    let started = Instant::now();
                    for _ in 0..parser_rounds {
                        let document = parse(&request, PatchLimits::default()).unwrap();
                        black_box(document.operations.len());
                    }
                    started.elapsed()
                })
                .collect(),
        );
        assert!(median < ABSOLUTE_PARSER_BATCH_CEILING);
        parser_medians.push(median);
    }
    assert_relative_linear(
        parser_sizes[0],
        parser_medians[0],
        *parser_sizes.last().unwrap(),
        *parser_medians.last().unwrap(),
        "parser",
    );

    let matcher_sizes = [8_192usize, 32_768, 131_072];
    let matcher_rounds = 3usize;
    let update = UpdateFile {
        hunks: vec![Hunk {
            context: None,
            lines: vec![HunkLine::Context("unique-needle".to_owned())],
            end_of_file: false,
            header_line: 1,
        }],
    };
    let mut matcher_medians = Vec::new();
    for lines in matcher_sizes {
        let mut source = "repeated-prefix\n".repeat(lines);
        source.push_str("unique-needle\n");
        let _ = apply_update_with_candidate_limit(&source, &update, 128).unwrap();
        let median = median_duration(
            (0..5)
                .map(|_| {
                    let started = Instant::now();
                    for _ in 0..matcher_rounds {
                        let result =
                            apply_update_with_candidate_limit(&source, &update, 128).unwrap();
                        black_box(result.replacements.len());
                    }
                    started.elapsed()
                })
                .collect(),
        );
        assert!(median < ABSOLUTE_MATCHER_BATCH_CEILING);
        matcher_medians.push(median);
    }
    assert_relative_linear(
        matcher_sizes[0],
        matcher_medians[0],
        *matcher_sizes.last().unwrap(),
        *matcher_medians.last().unwrap(),
        "matcher",
    );

    let workspace = tempfile::tempdir().unwrap();
    let near_limit_bytes = 15 * 1024 * 1024;
    let line = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde\n";
    let mut contents = Vec::with_capacity(near_limit_bytes);
    while contents.len() + line.len() <= near_limit_bytes {
        contents.extend_from_slice(line);
    }
    std::fs::write(workspace.path().join("near-limit.txt"), &contents).unwrap();
    let resolver = TargetResolver::new(workspace.path()).unwrap();
    let reader = PaginatedReader::new(SnapshotLimits::default(), AllowAllReadAccess);
    let request = ReadRequest {
        start_line: 0,
        start_byte: None,
        max_lines: 2_000,
        max_bytes: 256 * 1024,
    };
    let started = Instant::now();
    let first = reader
        .read_path(&resolver, "near-limit.txt", request, None)
        .unwrap();
    let first_elapsed = started.elapsed();
    assert!(first_elapsed < ABSOLUTE_NEAR_LIMIT_READ_CEILING);
    assert!(first.content.len() <= request.max_bytes as usize);
    assert!(first.truncated);
    let cursor = first.cursor.as_deref().expect("near-limit read must page");
    let started = Instant::now();
    let second = reader
        .read_path(&resolver, "near-limit.txt", request, Some(cursor))
        .unwrap();
    let second_elapsed = started.elapsed();
    assert!(second_elapsed < ABSOLUTE_NEAR_LIMIT_READ_CEILING);
    assert_eq!(second.token, first.token);
    assert!(second.content.len() <= request.max_bytes as usize);

    println!(
        "{}",
        json!({
            "gate": "apply_patch_parser_matcher_streaming_read",
            "parser": parser_sizes.into_iter().zip(parser_medians).map(|(size, elapsed)| json!({
                "operations": size,
                "rounds": parser_rounds,
                "median_batch_ms": milliseconds(elapsed),
            })).collect::<Vec<_>>(),
            "matcher": matcher_sizes.into_iter().zip(matcher_medians).map(|(size, elapsed)| json!({
                "source_lines": size,
                "rounds": matcher_rounds,
                "median_batch_ms": milliseconds(elapsed),
            })).collect::<Vec<_>>(),
            "near_limit_read": {
                "file_bytes": contents.len(),
                "page_bytes": [first.content.len(), second.content.len()],
                "page_ms": [milliseconds(first_elapsed), milliseconds(second_elapsed)],
            },
        })
    );
}

#[tokio::test]
#[ignore = "explicit apply-patch release qualification"]
async fn durable_history_storage_and_query_resource_gates() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&database, None).await.unwrap();
    let records = SqliteAppliedPatchStore::new(database.clone());
    let snapshots = SqliteSnapshotStore::new(database.clone());
    let intents = SqliteCommitIntentStore::new(database.clone());
    let projections = SqliteTurnDiffStore::new(database.clone());
    let domain = SnapshotDomain::new("thread:qualification", "pioneer", "thread_history");
    let alpha = snapshot(b"alpha\n");
    let beta = snapshot(b"beta\n");
    let mut append_samples = Vec::with_capacity(CONFIGURED_MAX_RECORD_FIXTURE);
    let append_started = Instant::now();
    for ordinal in 0..CONFIGURED_MAX_RECORD_FIXTURE {
        let (before, after) = if ordinal % 2 == 0 {
            (&alpha, &beta)
        } else {
            (&beta, &alpha)
        };
        let change = DurablePatchChange {
            operation_index: 0,
            commit_step: 0,
            sequence: 0,
            kind: ChangeKind::Update,
            source_path: "a.txt".to_owned(),
            destination_path: None,
            before: Some(TextSnapshotRef::from_snapshot(before)),
            after: Some(TextSnapshotRef::from_snapshot(after)),
            overwritten_destination: None,
            side_effects: PatchSideEffects::default(),
        };
        let mut record = AppliedPatchRecord::new(
            InvocationIdentity::new("qualification", "turn", format!("invocation-{ordinal:03}"))
                .unwrap(),
            CommitOrdinal(ordinal as u64),
            AppliedPatchRecordOutcome::Applied,
            vec![change],
        );
        record.environment_id = "workspace".to_owned();
        record.committed_at_unix_ms = ordinal as i64;
        let mut plan_fingerprint = [ordinal as u8; 32];
        plan_fingerprint[0] = (ordinal as u8).wrapping_add(1);
        let started = Instant::now();
        records
            .insert_with_snapshots(
                record,
                plan_fingerprint,
                &domain,
                &[before.clone(), after.clone()],
            )
            .await
            .unwrap();
        append_samples.push(started.elapsed());
    }
    let append_total = append_started.elapsed();
    let append_p50 = percentile(&append_samples, 50);
    let append_p95 = percentile(&append_samples, 95);
    let append_p99 = percentile(&append_samples, 99);
    assert!(append_total < ABSOLUTE_APPEND_TOTAL_CEILING);
    assert!(append_p95 < ABSOLUTE_APPEND_P95_CEILING);

    let snapshot_metrics = snapshots.metrics().await.unwrap();
    assert_eq!(snapshot_metrics.blobs, 2);
    assert_eq!(snapshot_metrics.references, 512);
    assert_eq!(
        snapshot_metrics.referenced_logical_bytes,
        CONFIGURED_MAX_RECORD_FIXTURE as u64 * (alpha.bytes.len() + beta.bytes.len()) as u64
    );
    assert!(snapshot_metrics.physical_bytes < snapshot_metrics.referenced_logical_bytes);

    let replay_started = Instant::now();
    let replay = replay_turn_pages(&records, &intents, "qualification", "turn", 64)
        .await
        .unwrap();
    let replay_elapsed = replay_started.elapsed();
    assert!(replay_elapsed < ABSOLUTE_REPLAY_CEILING);
    assert_eq!(
        replay.aggregate.record_count,
        CONFIGURED_MAX_RECORD_FIXTURE as u64
    );
    assert!(
        replay.aggregate.changes.is_empty(),
        "alternating edits are net-zero"
    );

    let projection = TurnDiffState::from_aggregate(
        replay.aggregate,
        TurnDiffAuthority::NativePatchEngine,
        replay.revision,
        false,
    );
    assert!(projections.upsert(&projection).await.unwrap());
    for _ in 0..10 {
        assert!(!projections.upsert(&projection).await.unwrap());
    }
    let projection_rows = usize::from(
        crud::find_turn_diff_state(&database, "qualification", "turn")
            .await
            .unwrap()
            .is_some(),
    );
    assert_eq!(
        projection_rows, 1,
        "live revisions must coalesce into one row"
    );

    let query_started = Instant::now();
    let history = records
        .query_file_history(
            "qualification",
            "a.txt",
            None,
            HistoryQueryLimits {
                max_page_records: CONFIGURED_MAX_RECORD_FIXTURE,
                max_page_bytes: 16 * 1024 * 1024,
                max_decompressed_bytes: 16 * 1024 * 1024,
            },
        )
        .await
        .unwrap();
    let query_elapsed = query_started.elapsed();
    assert!(query_elapsed < ABSOLUTE_QUERY_CEILING);
    assert_eq!(history.items.len(), CONFIGURED_MAX_RECORD_FIXTURE);

    let source_plan = crud::explain_patch_change_index_path_query(
        &database,
        "qualification",
        &["a.txt".to_owned()],
        true,
    )
    .await
    .unwrap();
    assert!(
        source_plan
            .iter()
            .any(|detail| detail.contains("idx_patch_change_index_thread_source_order")),
        "source-path history query must use its path index: {source_plan:?}"
    );
    let destination_plan = crud::explain_patch_change_index_path_query(
        &database,
        "qualification",
        &["a.txt".to_owned()],
        false,
    )
    .await
    .unwrap();
    assert!(
        destination_plan
            .iter()
            .any(|detail| detail.contains("idx_patch_change_index_thread_destination_order")),
        "destination-path history query must use its path index: {destination_plan:?}"
    );

    let stored_shapes = crud::list_applied_patch_records_for_turn(
        &database,
        "qualification",
        "turn",
        None,
        CONFIGURED_MAX_RECORD_FIXTURE as u64,
    )
    .await
    .unwrap();
    assert!(
        stored_shapes
            .iter()
            .all(|row| !row.changes_json.contains("alpha\\n")
                && !row.changes_json.contains("beta\\n"))
    );

    println!(
        "{}",
        json!({
            "gate": "patch_history_storage_query",
            "records": CONFIGURED_MAX_RECORD_FIXTURE,
            "append": {
                "total_ms": milliseconds(append_total),
                "p50_ms": milliseconds(append_p50),
                "p95_ms": milliseconds(append_p95),
                "p99_ms": milliseconds(append_p99),
            },
            "replay_ms": milliseconds(replay_elapsed),
            "query_ms": milliseconds(query_elapsed),
            "query_plan": {
                "source": source_plan,
                "destination": destination_plan,
            },
            "snapshot": {
                "blobs": snapshot_metrics.blobs,
                "references": snapshot_metrics.references,
                "logical_bytes": snapshot_metrics.logical_bytes,
                "referenced_logical_bytes": snapshot_metrics.referenced_logical_bytes,
                "physical_bytes": snapshot_metrics.physical_bytes,
            },
            "projection_rows": projection_rows,
        })
    );
}

fn add_patch(operations: usize) -> String {
    let mut patch = String::from("*** Begin Patch\n");
    for index in 0..operations {
        patch.push_str(&format!(
            "*** Add File: qualification-{index:03}.txt\n+value-{index:03}\n"
        ));
    }
    patch.push_str("*** End Patch\n");
    patch
}

fn snapshot(bytes: &[u8]) -> CommittedTextSnapshot {
    CommittedTextSnapshot::from_bytes(
        bytes.to_vec(),
        TextEncoding::Utf8,
        LineEndingMetadata {
            dominant: LineEnding::Lf,
            mixed: false,
            final_newline: bytes.ends_with(b"\n"),
        },
    )
}

fn median_duration(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    let mut samples = samples.to_vec();
    samples.sort_unstable();
    let index = (samples.len() - 1) * percentile / 100;
    samples[index]
}

fn assert_relative_linear(
    small_size: usize,
    small_elapsed: Duration,
    large_size: usize,
    large_elapsed: Duration,
    label: &str,
) {
    let size_ratio = large_size.div_ceil(small_size) as u128;
    let allowed = small_elapsed
        .as_nanos()
        .max(1)
        .saturating_mul(size_ratio)
        .saturating_mul(RELATIVE_LINEAR_SLACK);
    assert!(
        large_elapsed.as_nanos() <= allowed,
        "{label} scaling exceeded the linear fixture ratio plus slack: {small_elapsed:?} -> {large_elapsed:?}"
    );
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
