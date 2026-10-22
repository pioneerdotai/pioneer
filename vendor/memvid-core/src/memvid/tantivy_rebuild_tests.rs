//! Regression coverage for the two full-rebuild branches and their shared preparation path.

use super::*;
use crate::{AclEnforcementMode, SearchEngineKind, SearchRequest, SketchVariant};

fn options(text: &str, instant: bool) -> PutOptions {
    PutOptions::builder()
        .uri(format!("mv2://rebuild/{text}"))
        .search_text(text)
        .instant_index(instant)
        .auto_tag(false)
        .extract_dates(false)
        .extract_triplets(false)
        .extraction_budget_ms(0)
        .build()
}

fn reset_rebuilds() {
    TANTIVY_REBUILDS.with(|count| count.set(0));
}

fn assert_rebuilds(expected: usize) {
    TANTIVY_REBUILDS.with(|count| assert_eq!(count.get(), expected));
}

fn assert_text(mem: &mut Memvid, text: &str, expected: &[FrameId]) {
    let response = mem
        .search(SearchRequest {
            query: text.into(),
            top_k: 20,
            snippet_chars: 80,
            uri: None,
            scope: None,
            cursor: None,
            #[cfg(feature = "temporal_track")]
            temporal: None,
            as_of_frame: None,
            as_of_ts: None,
            no_sketch: true,
            acl_context: None,
            acl_enforcement_mode: AclEnforcementMode::Audit,
        })
        .unwrap();
    // A legacy fallback must not hide a missing or stale Tantivy index.
    assert_eq!(response.engine, SearchEngineKind::Tantivy);
    let mut ids: Vec<_> = response.hits.iter().map(|hit| hit.frame_id).collect();
    ids.sort_unstable();
    assert_eq!(ids, expected);
    let raw = mem
        .tantivy
        .as_ref()
        .unwrap()
        .search_documents(
            &crate::search::parse_query(text).unwrap(),
            None,
            None,
            None,
            20,
        )
        .unwrap();
    let mut raw_ids: Vec<_> = raw.iter().map(|hit| hit.frame_id).collect();
    raw_ids.sort_unstable();
    assert_eq!(
        raw_ids, expected,
        "stale IDs must be absent from the index itself"
    );
}

fn assert_vector(mem: &mut Memvid) {
    assert_eq!(mem.frame_embedding(0).unwrap(), Some(vec![1.0, 0.0]));
    let hits = mem.search_vec(&[1.0, 0.0], 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].frame_id, 0);
    assert_eq!(hits[0].distance, 0.0);
    assert_eq!(mem.frame_canonical_payload(0).unwrap(), b"vectoranchor");
    let vector = mem.toc.indexes.vec.clone().unwrap();
    let bytes = mem
        .read_range(vector.bytes_offset, vector.bytes_length)
        .unwrap();
    assert_eq!(*hash(&bytes).as_bytes(), vector.checksum);
    for segment in mem.toc.segment_catalog.tantivy_segments.clone() {
        let bytes = mem
            .read_range(segment.common.bytes_offset, segment.common.bytes_length)
            .unwrap();
        assert_eq!(*hash(&bytes).as_bytes(), segment.common.checksum);
    }
}

fn fixture(path: &Path) {
    let mut mem = Memvid::create(path).unwrap();
    mem.enable_vec().unwrap();
    mem.put_with_embedding_and_options(
        b"vectoranchor",
        vec![1.0, 0.0],
        options("vectoranchor", false),
    )
    .unwrap();
    mem.put_bytes_with_options(b"obsoleteword", options("obsoleteword", false))
        .unwrap();
    mem.commit().unwrap();
    assert_text(&mut mem, "vectoranchor", &[0]);
    assert_text(&mut mem, "obsoleteword", &[1]);
    assert_vector(&mut mem);
}

fn select_full_branch(mem: &mut Memvid, instant: bool) {
    assert!(!mem.tantivy_dirty);
    if !instant {
        // Exercise the production fallback for a lost engine, without changing dirty semantics.
        mem.tantivy = None;
    }
}

fn assert_updated(mem: &mut Memvid) {
    assert_text(mem, "vectoranchor", &[0]);
    assert_text(mem, "replacementword", &[3]);
    assert_text(mem, "obsoleteword", &[]);
    assert_text(mem, "addedword", &[]);
    assert_eq!(mem.tantivy.as_ref().unwrap().num_docs(), 2);
    assert_eq!(mem.frame_by_id(1).unwrap().status, FrameStatus::Superseded);
    assert_eq!(mem.frame_by_id(1).unwrap().superseded_by, Some(3));
    assert_eq!(mem.frame_by_id(2).unwrap().status, FrameStatus::Deleted);
    assert_eq!(mem.frame_by_id(3).unwrap().supersedes, Some(1));
    assert_eq!(mem.frame_canonical_payload(3).unwrap(), b"replacementword");
    assert_vector(mem);
}

#[test]
fn both_full_branches_rebuild_once_and_publish_current_ids_and_vectors() {
    for instant in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mutations.mv2");
        fixture(&path);

        let mut mem = Memvid::open(&path).unwrap();
        select_full_branch(&mut mem, instant);
        let sequence = mem
            .put_bytes_with_options(b"addedword", options("addedword", instant))
            .unwrap();
        if instant {
            assert!(mem.tantivy_dirty);
            assert_ne!(
                sequence, 2,
                "the preliminary ID must differ from the final frame ID"
            );
            let raw = mem
                .tantivy
                .as_ref()
                .unwrap()
                .search_documents(
                    &crate::search::parse_query("addedword").unwrap(),
                    None,
                    None,
                    None,
                    20,
                )
                .unwrap();
            assert_eq!(raw.len(), 1);
            assert_eq!(raw[0].frame_id, sequence);
        }
        reset_rebuilds();
        mem.commit().unwrap();
        assert_rebuilds(1);
        assert_text(&mut mem, "addedword", &[2]);
        assert_eq!(mem.tantivy.as_ref().unwrap().num_docs(), 3);
        assert_vector(&mut mem);
        drop(mem);
        let mut reopened = Memvid::open_read_only(&path).unwrap();
        assert_text(&mut reopened, "addedword", &[2]);
        assert_vector(&mut reopened);
        drop(reopened);

        let mut mem = Memvid::open(&path).unwrap();
        select_full_branch(&mut mem, instant);
        mem.update_frame(
            1,
            Some(b"replacementword".to_vec()),
            options("replacementword", instant),
            None,
        )
        .unwrap();
        mem.delete_frame(2).unwrap();
        reset_rebuilds();
        mem.commit().unwrap();
        assert_rebuilds(1);
        assert_updated(&mut mem);
        drop(mem);
        assert_updated(&mut Memvid::open_read_only(&path).unwrap());
    }
}

#[test]
fn full_rebuild_without_insertions_rebuilds_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("metadata.mv2");
    fixture(&path);
    let mut mem = Memvid::open(&path).unwrap();
    assert!(!mem.tantivy_dirty);
    assert!(mem.tantivy.is_some());
    mem.insert_sketch(0, "metadata-only change", SketchVariant::Small);
    reset_rebuilds();
    mem.commit().unwrap();
    assert_rebuilds(1);
    assert_text(&mut mem, "obsoleteword", &[1]);
    assert_vector(&mut mem);
    drop(mem);
    let mut reopened = Memvid::open_read_only(&path).unwrap();
    assert!(reopened.sketches().get(0).is_some());
    assert_text(&mut reopened, "obsoleteword", &[1]);
    assert_vector(&mut reopened);
}

#[test]
fn commit_without_instant_indexing_rebuilds_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("without-instant-indexing.mv2");
    fixture(&path);
    let mut mem = Memvid::open(&path).unwrap();
    mem.put_bytes_with_options(b"addedword", options("addedword", false))
        .unwrap();
    assert!(!mem.tantivy_dirty);
    assert!(mem.tantivy.is_some());
    reset_rebuilds();
    mem.commit().unwrap();
    // apply_records indexes the final frame and marks Tantivy dirty even without
    // preliminary instant indexing, so commit takes the full-rebuild branch.
    assert_rebuilds(1);
    assert_text(&mut mem, "addedword", &[2]);
    assert_text(&mut mem, "obsoleteword", &[1]);
    assert_vector(&mut mem);
    drop(mem);
    let mut reopened = Memvid::open_read_only(&path).unwrap();
    assert_text(&mut reopened, "addedword", &[2]);
    assert_text(&mut reopened, "obsoleteword", &[1]);
    assert_vector(&mut reopened);
}

#[test]
fn incremental_rebuild_indexes_publishes_new_and_existing_documents() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("incremental-rebuild.mv2");
    fixture(&path);
    let mut mem = Memvid::open(&path).unwrap();
    mem.put_bytes_with_options(b"addedword", options("addedword", false))
        .unwrap();
    assert!(!mem.tantivy_dirty);
    assert_text(&mut mem, "addedword", &[]);
    let records = mem.wal.pending_records().unwrap();
    let before = std::fs::read(&path).unwrap();
    reset_rebuilds();
    mem.with_staging_lock(|staged| {
        staged.generation = staged.generation.wrapping_add(1);
        staged.materialize_vec_segments_for_rebuild()?;
        // Like commit_skip_indexes_inner, detach the existing engine before applying
        // records so it remains an unchanged index of the old frames. Restore it
        // afterwards; no frame was indexed and no dirty flag needs to be reset.
        let engine = staged.tantivy.take().unwrap();
        let delta_result = staged.apply_records(records);
        staged.tantivy = Some(engine);
        let delta = delta_result?;

        // These are the actual preconditions of rebuild_indexes' incremental branch.
        assert!(!staged.tantivy_dirty);
        assert!(staged.tantivy.is_some());
        assert_eq!(delta.inserted_frames, vec![2]);
        assert_eq!(staged.tantivy.as_ref().unwrap().num_docs(), 2);
        assert_text(staged, "addedword", &[]);
        assert_text(staged, "obsoleteword", &[1]);
        assert_eq!(staged.frame_canonical_payload(2)?, b"addedword");
        staged.rebuild_indexes(&delta.inserted_embeddings, &delta.inserted_frames, true)?;
        assert_rebuilds(0);
        assert_eq!(staged.tantivy.as_ref().unwrap().num_docs(), 3);
        assert_text(staged, "addedword", &[2]);
        assert_text(staged, "obsoleteword", &[1]);
        assert_text(staged, "vectoranchor", &[0]);
        assert_vector(staged);

        // Use the same production persistence operations as commit_from_records,
        // including checkpoint/header checksums, before staging publishes the file.
        staged.persist_sketch_track()?;
        staged.rewrite_toc_footer()?;
        staged.header.toc_checksum = staged.toc.toc_checksum;
        staged.wal.record_checkpoint(&mut staged.header)?;
        staged.header.toc_checksum = staged.toc.toc_checksum;
        crate::persist_header(&mut staged.file, &staged.header)?;
        staged.file.sync_all()?;
        #[cfg(feature = "parallel_segments")]
        if let Some(wal) = staged.manifest_wal.as_mut() {
            wal.flush()?;
            wal.truncate()?;
        }
        staged.pending_frame_inserts = 0;
        staged.dirty = false;
        assert_eq!(std::fs::read(&path).unwrap(), before);
        Ok(())
    })
    .unwrap();
    assert_rebuilds(0);
    assert!(!mem.tantivy_dirty);
    assert!(mem.wal.pending_records().unwrap().is_empty());
    assert_text(&mut mem, "addedword", &[2]);
    assert_text(&mut mem, "obsoleteword", &[1]);
    assert_vector(&mut mem);
    drop(mem);
    let published = std::fs::read(&path).unwrap();
    assert_ne!(published, before);
    let mut reopened = Memvid::open_read_only(&path).unwrap();
    assert_eq!(reopened.tantivy.as_ref().unwrap().num_docs(), 3);
    assert_text(&mut reopened, "addedword", &[2]);
    assert_text(&mut reopened, "obsoleteword", &[1]);
    assert_text(&mut reopened, "vectoranchor", &[0]);
    assert_vector(&mut reopened);
    assert_eq!(reopened.frame_canonical_payload(2).unwrap(), b"addedword");
    drop(reopened);
    assert_eq!(std::fs::read(&path).unwrap(), published);
}

#[test]
fn both_full_branches_publish_an_empty_index() {
    for instant in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.mv2");
        let mut mem = Memvid::create(&path).unwrap();
        reset_rebuilds();
        mem.finalize_indexes().unwrap();
        assert_rebuilds(1);
        assert_eq!(mem.tantivy.as_ref().unwrap().num_docs(), 0);
        mem.put_bytes_with_options(b"lastword", options("lastword", instant))
            .unwrap();
        if !instant {
            mem.tantivy = None;
        }
        reset_rebuilds();
        mem.commit().unwrap();
        assert_rebuilds(1);
        assert_text(&mut mem, "lastword", &[0]);
        select_full_branch(&mut mem, instant);
        mem.delete_frame(0).unwrap();
        reset_rebuilds();
        mem.commit().unwrap();
        assert_rebuilds(1);
        assert_eq!(mem.tantivy.as_ref().unwrap().num_docs(), 0);
        assert_text(&mut mem, "lastword", &[]);
        drop(mem);
        let mut reopened = Memvid::open_read_only(&path).unwrap();
        assert_eq!(reopened.tantivy.as_ref().unwrap().num_docs(), 0);
        assert_text(&mut reopened, "lastword", &[]);
        assert_eq!(
            reopened.frame_by_id(0).unwrap().status,
            FrameStatus::Deleted
        );
    }
}

#[test]
fn preparation_that_only_opens_or_creates_still_requires_a_full_rebuild() {
    for missing_meta in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prepare-only.mv2");
        fixture(&path);
        let mut mem = Memvid::open(&path).unwrap();
        if missing_meta {
            // Make materialization produce an unopenable index. Preparation falls back to
            // create(), but trusts the nonempty catalog and does not automatically rebuild.
            let meta = mem
                .toc
                .segment_catalog
                .tantivy_segments
                .iter_mut()
                .find(|segment| segment.path == "meta.json")
                .unwrap();
            meta.path = "missing-meta.json".into();
        }
        reset_rebuilds();
        mem.init_tantivy().unwrap();
        assert_rebuilds(0);
        assert_eq!(
            mem.tantivy.as_ref().unwrap().num_docs(),
            if missing_meta { 0 } else { 2 }
        );
        // Call the exact production helper shared by both full-rebuild branches. Their
        // preceding catalog/storage clears normally force an automatic rebuild in 3.1.7;
        // retaining segments here covers the other real preparation outcome.
        mem.rebuild_tantivy_index().unwrap();
        assert_rebuilds(1);
        assert_text(&mut mem, "obsoleteword", &[1]);
        assert_vector(&mut mem);
        mem.insert_sketch(0, "persist repaired index", SketchVariant::Small);
        reset_rebuilds();
        mem.commit().unwrap();
        assert_rebuilds(1);
        drop(mem);
        let mut reopened = Memvid::open_read_only(&path).unwrap();
        assert_text(&mut reopened, "obsoleteword", &[1]);
        assert_vector(&mut reopened);
    }
}

#[test]
fn lazy_initialization_still_recovers_missing_segments() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lazy-recovery.mv2");
    fixture(&path);
    let mut mem = Memvid::open(&path).unwrap();
    mem.toc.segment_catalog.tantivy_segments.clear();
    mem.toc.indexes.lex_segments.clear();
    mem.lex_storage.write().unwrap().clear();
    mem.tantivy = None;
    reset_rebuilds();
    assert_text(&mut mem, "obsoleteword", &[1]);
    assert_rebuilds(1);
    assert!(mem.tantivy_dirty);
    assert_vector(&mut mem);
    reset_rebuilds();
    mem.commit().unwrap();
    assert_rebuilds(0); // The existing lex-only flush contract remains intact.
    drop(mem);
    let mut reopened = Memvid::open_read_only(&path).unwrap();
    assert_text(&mut reopened, "obsoleteword", &[1]);
    assert_vector(&mut reopened);
}

#[test]
fn required_rebuild_failure_after_open_only_preparation_is_propagated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prepare-only-failure.mv2");
    fixture(&path);
    let before = std::fs::read(&path).unwrap();
    let mut mem = Memvid::open(&path).unwrap();
    reset_rebuilds();
    mem.init_tantivy().unwrap();
    assert_rebuilds(0);
    FAIL_TANTIVY_REBUILD.with(|flag| flag.set(true));
    let error = mem
        .rebuild_tantivy_index()
        .expect_err("the required rebuild must not be skipped");
    assert!(error.to_string().contains("Tantivy rebuild failure"));
    assert_rebuilds(1);
    drop(mem);
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let mut reopened = Memvid::open_read_only(&path).unwrap();
    assert_text(&mut reopened, "obsoleteword", &[1]);
    assert_vector(&mut reopened);
}

#[test]
fn disabled_lex_does_not_rebuild_or_damage_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("disabled.mv2");
    fixture(&path);
    let mut mem = Memvid::open(&path).unwrap();
    // There is no public disable API; exercise the existing internal disabled state.
    mem.lex_enabled = false;
    mem.toc.segment_catalog.lex_enabled = false;
    mem.init_tantivy().unwrap();
    assert!(mem.tantivy.is_none());
    assert!(!mem.tantivy_dirty);
    mem.insert_sketch(0, "disabled lex", SketchVariant::Small);
    reset_rebuilds();
    mem.commit().unwrap();
    assert_rebuilds(0);
    assert_vector(&mut mem);
    drop(mem);
    let mut reopened = Memvid::open_read_only(&path).unwrap();
    assert!(!reopened.lex_enabled);
    assert!(matches!(
        reopened.search_lex("obsoleteword", 10),
        Err(MemvidError::LexNotEnabled)
    ));
    assert_vector(&mut reopened);
}

#[test]
fn rebuild_and_publication_errors_preserve_complete_snapshots() {
    for instant in [true, false] {
        for failure in ["rebuild", "publish", "post-publish"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("failure.mv2");
            fixture(&path);
            let mut mem = Memvid::open(&path).unwrap();
            if instant {
                mem.put_bytes_with_options(b"pendingword", options("pendingword", true))
                    .unwrap();
                assert!(mem.tantivy_dirty);
            } else {
                assert!(!mem.tantivy_dirty);
                mem.insert_sketch(0, "failure metadata", SketchVariant::Small);
            }
            let before = std::fs::read(&path).unwrap();
            match failure {
                "rebuild" => FAIL_TANTIVY_REBUILD.with(|flag| flag.set(true)),
                "publish" => FAIL_BEFORE_ATOMIC_PUBLISH.with(|flag| flag.set(true)),
                "post-publish" => FAIL_AFTER_ATOMIC_PUBLISH.with(|flag| flag.set(true)),
                _ => unreachable!(),
            }
            reset_rebuilds();
            let error = mem
                .commit()
                .expect_err("a failed rebuild/publication must stay an error");
            assert_rebuilds(1);
            assert!(error.to_string().contains(match failure {
                "rebuild" => "Tantivy rebuild failure",
                "publish" => "before atomic publish",
                _ => "after atomic publish",
            }));
            assert!(mem.write_disabled.is_some());
            assert!(
                mem.commit().is_err(),
                "a terminal handle must not retry publication"
            );
            if failure != "post-publish" {
                assert_eq!(std::fs::read(&path).unwrap(), before);
            }
            drop(mem); // Terminal handles must not retry from Drop either.
            let after = std::fs::read(&path).unwrap();
            if failure != "post-publish" {
                assert_eq!(after, before);
            }
            let mut snapshot = Memvid::open_read_only(&path).unwrap();
            assert_text(&mut snapshot, "vectoranchor", &[0]);
            assert_text(&mut snapshot, "obsoleteword", &[1]);
            assert_text(
                &mut snapshot,
                "pendingword",
                if instant && failure == "post-publish" {
                    &[2]
                } else {
                    &[]
                },
            );
            assert_eq!(
                snapshot.frame_canonical_payload(1).unwrap(),
                b"obsoleteword"
            );
            assert_vector(&mut snapshot);
            if !instant && failure == "post-publish" {
                assert!(snapshot.sketches().get(0).is_some());
            }
            drop(snapshot);
            assert_eq!(
                std::fs::read(&path).unwrap(),
                after,
                "read-only reopen must not change bytes"
            );
        }
    }
}
