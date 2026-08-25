//! Explicit Apply Patch heap qualification for near-limit snapshots and paginated reads.

use pioneer_tools::apply_patch::file_mutation::{
    AllowAllReadAccess, PaginatedReader, ReadRequest, SnapshotLimits, TargetResolver, TextSnapshot,
};
use serde_json::json;
use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

struct TrackingAllocator;

static CURRENT_BYTES: AtomicIsize = AtomicIsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size() as isize);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        CURRENT_BYTES.fetch_sub(layout.size() as isize, Ordering::SeqCst);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            record_allocation(new_size as isize - layout.size() as isize);
        }
        resized
    }
}

fn record_allocation(delta: isize) {
    let current = CURRENT_BYTES.fetch_add(delta, Ordering::SeqCst) + delta;
    if current <= 0 {
        return;
    }
    let current = current as usize;
    let mut peak = PEAK_BYTES.load(Ordering::SeqCst);
    while current > peak {
        match PEAK_BYTES.compare_exchange(peak, current, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(observed) => peak = observed,
        }
    }
}

fn start_measurement() -> isize {
    let baseline = CURRENT_BYTES.load(Ordering::SeqCst);
    PEAK_BYTES.store(baseline.max(0) as usize, Ordering::SeqCst);
    baseline
}

fn peak_delta(baseline: isize) -> usize {
    PEAK_BYTES
        .load(Ordering::SeqCst)
        .saturating_sub(baseline.max(0) as usize)
}

#[test]
#[ignore = "explicit snapshot/read release qualification"]
fn near_limit_snapshot_and_page_stay_below_frozen_heap_ceiling() {
    const FILE_BYTES: usize = 15 * 1024 * 1024;
    const HEAP_CEILING: usize = 8 * 1024 * 1024;
    const LATENCY_CEILING: Duration = Duration::from_secs(5);

    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("near-limit.txt");
    let line = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde\n";
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = std::io::BufWriter::new(file);
    let mut written = 0usize;
    while written + line.len() <= FILE_BYTES {
        writer.write_all(line).unwrap();
        written += line.len();
    }
    writer.flush().unwrap();
    drop(writer);

    let baseline = start_measurement();
    let started = Instant::now();
    let snapshot = TextSnapshot::from_file(&path, SnapshotLimits::default()).unwrap();
    let snapshot_elapsed = started.elapsed();
    let snapshot_peak = peak_delta(baseline);
    assert_eq!(snapshot.storage_kind(), "spooled");
    assert!(snapshot_peak < HEAP_CEILING);
    assert!(snapshot_elapsed < LATENCY_CEILING);
    drop(snapshot);

    let resolver = TargetResolver::new(workspace.path()).unwrap();
    let reader = PaginatedReader::new(SnapshotLimits::default(), AllowAllReadAccess);
    let baseline = start_measurement();
    let started = Instant::now();
    let page = reader
        .read_path(
            &resolver,
            "near-limit.txt",
            ReadRequest {
                start_line: 0,
                start_byte: None,
                max_lines: 2_000,
                max_bytes: 256 * 1024,
            },
            None,
        )
        .unwrap();
    let read_elapsed = started.elapsed();
    let read_peak = peak_delta(baseline);
    assert!(page.truncated);
    assert!(page.content.len() <= 256 * 1024);
    assert!(read_peak < HEAP_CEILING);
    assert!(read_elapsed < LATENCY_CEILING);

    println!(
        "{}",
        json!({
            "gate": "near_limit_snapshot_read_heap",
            "file_bytes": written,
            "heap_ceiling_bytes": HEAP_CEILING,
            "snapshot_peak_delta_bytes": snapshot_peak,
            "snapshot_ms": snapshot_elapsed.as_secs_f64() * 1_000.0,
            "read_peak_delta_bytes": read_peak,
            "read_ms": read_elapsed.as_secs_f64() * 1_000.0,
            "page_bytes": page.content.len(),
        })
    );
}
