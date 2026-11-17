use super::*;
use crate::queues::{QueueKind, current_fixture};
use crate::resources::{
    ChildRole, ChildSnapshot, CpuStatus, Field, ProcessSample, RawUsage, ResourceSnapshot,
    SourceStatus, Value,
};
use crate::work::{Boundary, ExecutorRole, Trigger, WorkCarrier, WorkKind};
use std::mem::size_of;

struct ConsentReset(bool);
impl Drop for ConsentReset {
    fn drop(&mut self) {
        crate::set_telemetry_enabled(self.0);
    }
}
fn fresh_epoch() -> ConsentReset {
    let reset = ConsentReset(crate::telemetry_enabled());
    crate::set_telemetry_enabled(false);
    crate::set_telemetry_enabled(true);
    reset
}
fn spare(text: &str) -> String {
    let mut s = String::with_capacity(text.len() + 37);
    s.push_str(text);
    s
}
fn value() -> Value {
    Value {
        field: Field::RssBytes,
        value: Some(23.0),
        status: SourceStatus::Observed,
        source: spare("источник"),
        unit: spare("By"),
        scope: spare("process"),
        temporality: spare("instantaneous"),
    }
}
fn sample() -> ProcessSample {
    let mut values = Vec::with_capacity(4);
    values.push(value());
    ProcessSample {
        pid: 1,
        sampled_unix_ms: 2,
        sampled_monotonic_ms: 3,
        boot_ns: None,
        raw: Some(RawUsage {
            main_module_id: Some(spare("модуль")),
            start_identity: spare("start"),
            user_ns: 4,
            system_ns: 5,
            cpu_source: spare("cpu"),
            values,
        }),
        cpu_status: CpuStatus::First,
        elapsed_ns: None,
        delta_user_ns: None,
        delta_system_ns: None,
        cpu_cores: None,
        machine_fraction: None,
        quota_fraction: None,
    }
}
fn resources(epoch: u64) -> ResourceSnapshot {
    let mut children = Vec::with_capacity(3);
    children.push(ChildSnapshot {
        registered_in_generation: epoch,
        work: None,
        generation: 1,
        owner_generation: None,
        parent_run: spare("родитель"),
        role: ChildRole::Shell,
        pid: 2,
        start_identity: Some(spare("child")),
        spawned_monotonic_ms: 3,
        ownership_ended_monotonic_ms: Some(4),
        exited_monotonic_ms: Some(4),
        outcome: spare("completed"),
        final_usage: spare("observed"),
        sample: Some(sample()),
    });
    ResourceSnapshot {
        child_coverage: spare("coverage"),
        schema_version: 1,
        consent_generation: epoch,
        snapshot_monotonic_ms: 5,
        last_success_monotonic_ms: None,
        last_success_unix_ms: None,
        stale: false,
        period_ms: 1000,
        failed_samples: 0,
        skipped_samples: 0,
        child_overflow: 0,
        child_history_overwritten: 0,
        self_process: Some(sample()),
        host: Vec::with_capacity(5),
        children,
    }
}
// Independent fixture formula: headers live inside JournalRecord/ResourceSnapshot
// and child/value slots. Only backing allocations and occupied elements' strings
// are added here; no production calculator is used as the oracle.
#[test]
fn resource_cost_counts_box_spare_slots_and_nested_utf8_capacities_once() {
    let mut r = resources(1);
    let own = r.self_process.as_ref().unwrap().raw.as_ref().unwrap();
    let child = &r.children[0];
    let raw = child.sample.as_ref().unwrap().raw.as_ref().unwrap();
    let expected = size_of::<JournalRecord>()
        + size_of::<ResourceSnapshot>()
        + r.child_coverage.capacity()
        + r.host.capacity() * size_of::<Value>()
        + r.children.capacity() * size_of::<ChildSnapshot>()
        + child.parent_run.capacity()
        + child.start_identity.as_ref().unwrap().capacity()
        + child.outcome.capacity()
        + child.final_usage.capacity()
        + own.main_module_id.as_ref().unwrap().capacity()
        + own.start_identity.capacity()
        + own.cpu_source.capacity()
        + own.values.capacity() * size_of::<Value>()
        + own.values[0].source.capacity()
        + own.values[0].unit.capacity()
        + own.values[0].scope.capacity()
        + own.values[0].temporality.capacity()
        + raw.main_module_id.as_ref().unwrap().capacity()
        + raw.start_identity.capacity()
        + raw.cpu_source.capacity()
        + raw.values.capacity() * size_of::<Value>()
        + raw.values[0].source.capacity()
        + raw.values[0].unit.capacity()
        + raw.values[0].scope.capacity()
        + raw.values[0].temporality.capacity();
    assert!(r.host.is_empty() && r.host.capacity() > 0);
    assert!(raw.values[0].source.capacity() > raw.values[0].source.len());
    assert_eq!(resource_record_bytes(&r), expected);
    r.host.push(value());
    let host = &r.host[0];
    assert_eq!(
        resource_record_bytes(&r),
        expected
            + host.source.capacity()
            + host.unit.capacity()
            + host.scope.capacity()
            + host.temporality.capacity()
    );
}

#[test]
fn resource_cost_drives_byte_eviction_ttl_and_oversize_keeps_sampling_tail() {
    let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
    let _reset = fresh_epoch();
    let epoch = crate::telemetry_consent_snapshot().1;
    let mut store = LiveStore::new(epoch);
    let mut r = resources(epoch);
    r.child_coverage = String::with_capacity(MAX_BYTES / 2);
    let cost = resource_record_bytes(&r);
    assert!(cost < MAX_BYTES && cost > MAX_BYTES / 2);
    store.append(JournalPayload::Resources(Box::new(r)), cost, 100);
    let r = resources(epoch);
    let second_cost = resource_record_bytes(&r);
    store.append(JournalPayload::Resources(Box::new(r)), second_cost, 101);
    // A new valid resource record displaces the older large record by byte cost.
    let mut r = resources(epoch);
    r.child_coverage = String::with_capacity(MAX_BYTES / 2);
    store.append(JournalPayload::Resources(Box::new(r)), cost, 102);
    assert_eq!(store.overwritten, 1);
    assert_eq!(store.bytes, cost + second_cost);
    assert_eq!(
        store.bytes,
        store
            .journal
            .iter()
            .map(|r| r.retained_bytes)
            .sum::<usize>()
    );
    store.trim(102 + HISTORY_MS + 1);
    assert_eq!(store.bytes, 0);
    assert_eq!(store.overwritten, 3);

    with_store(epoch, |s| {
        s.queues.insert(
            1,
            current_fixture(QueueKind::EventDurable, 0, true, None, true),
        );
    });
    let mut oversized = resources(epoch);
    oversized.child_coverage = String::with_capacity(MAX_BYTES + 1);
    sample_resources(oversized);
    let diagnostic = snapshot().unwrap();
    assert_eq!(diagnostic.lost, 1);
    assert_eq!(diagnostic.last_sequence, 3);
    assert_eq!(diagnostic.first_sequence, Some(2));
    assert!(matches!(
        diagnostic.journal[0].payload,
        JournalPayload::Queue(_)
    ));
    assert!(matches!(
        diagnostic.journal[1].payload,
        JournalPayload::Health(_)
    ));
    assert_eq!(
        diagnostic.journal_bytes,
        diagnostic
            .journal
            .iter()
            .map(|r| r.retained_bytes)
            .sum::<usize>()
    );
    assert!(diagnostic.journal_bytes <= MAX_BYTES);
}

#[test]
fn current_work_zeroes_progress_losses_history_independence_and_epoch_fencing() {
    let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
    let _reset = fresh_epoch();
    let epoch = crate::telemetry_consent_snapshot().1;
    let carrier = WorkCarrier::independent(
        Boundary::Timer,
        WorkKind::Maintenance,
        Trigger::Timer,
        ExecutorRole::Gateway,
    );
    let mut guard = WorkGuard::start(carrier.context(), false);
    with_store(epoch, |s| {
        let entry = *s.active.values().next().unwrap();
        s.active.clear();
        for (i, state) in [
            WorkState::Queued,
            WorkState::Running,
            WorkState::Waiting(WaitKind::Reply),
            WorkState::SleepingUntil(9000),
            WorkState::Terminal(Outcome::Completed),
        ]
        .into_iter()
        .enumerate()
        {
            let mut e = entry;
            e.context.token = i as u64 + 1;
            e.state = state;
            e.started_ms = 0;
            e.state_since_ms = 900;
            e.last_progress_ms = 100 + i as u64 * 100;
            s.active.insert(e.context.token, e);
        }
        // Isolate from the real start journal and assert the expected values.
        s.journal.clear();
        s.bytes = 0;
        s.overflow = 11;
        s.abandoned = 12;
        s.overwritten = 13;
        s.lost = 14;
        s.queue_overflow = 15;
        s.queues.insert(
            1,
            current_fixture(QueueKind::EventDurable, 2, true, Some((800, 70)), true),
        );
        s.work_counts[0] = 17;
        s.context_loss_counts[0] = 18;
        s.queue_counts[0] = 19;
        let mut older = entry;
        older.context.token = u64::MAX;
        older.last_progress_ms = 50;
        s.active.insert(u64::MAX, older);
        let empty = current_numbers(s, 1000);
        assert_eq!(empty.active.len(), 17 * 4);
        for (state, age) in [
            ("queued", 900),
            ("running", 950),
            ("waiting", 700),
            ("sleeping", 600),
        ] {
            assert_eq!(
                empty.active[&("maintenance", state)],
                (if state == "running" { 2 } else { 1 }, age)
            );
        }
        assert_eq!(empty.active[&("task", "running")], (0, 0));
        assert_eq!(empty.loss, [11, 12, 13, 14, 15]);
        let r = resources(epoch);
        let cost = resource_record_bytes(&r);
        s.append(JournalPayload::Resources(Box::new(r)), cost, 1000);
        assert_eq!(current_numbers(s, 1000), empty);
        assert_eq!(
            (
                s.work_counts[0],
                s.context_loss_counts[0],
                s.queue_counts[0]
            ),
            (17, 18, 19)
        );
        // Keep the real owner's token for the subsequent finish -> observed zero.
        s.active.clear();
        s.active.insert(entry.context.token, entry);
        // Diagnostic reads use the real monotonic clock: keep this record fresh
        // independently of how long the surrounding test suite has been running.
        s.journal.back_mut().unwrap().monotonic_ms = monotonic_ms();
    });
    record_pool_snapshot(
        epoch,
        crate::DatabaseRole::Reader,
        10,
        crate::DatabasePoolSnapshot { size: 3, idle: 2 },
    );
    let diagnostic = snapshot().unwrap();
    assert_eq!(diagnostic.database_pools.len(), 1);
    assert_eq!(diagnostic.database_pools[0].role, "reader");
    assert_eq!(diagnostic.queues.len(), 1);
    assert!(
        diagnostic
            .journal
            .iter()
            .any(|r| matches!(r.payload, JournalPayload::Resources(_)))
    );
    guard.finish(Outcome::Completed);
    let zeros = with_store(epoch, |s| current_numbers(s, 1000)).unwrap();
    assert!(
        zeros
            .active
            .values()
            .all(|&(count, age)| count == 0 && age == 0)
    );
    crate::set_telemetry_enabled(false);
    assert!(with_store(epoch, |s| current_numbers(s, 1000)).is_none());
    assert!(crate::record_in_epoch(epoch, || panic!("disabled export")).is_none());
    crate::set_telemetry_enabled(true);
    assert!(with_store(epoch, |s| current_numbers(s, 1000)).is_none());
    assert!(crate::record_in_epoch(epoch, || panic!("old epoch export")).is_none());
    let new_epoch = crate::telemetry_consent_snapshot().1;
    let fresh = with_store(new_epoch, |s| current_numbers(s, 1000)).unwrap();
    assert_eq!(fresh.loss, [0; 5]);
    assert!(fresh.active.values().all(|&(n, _)| n == 0));
    sample_resources(resources(epoch));
    let diagnostic = snapshot().unwrap();
    assert_eq!(diagnostic.consent_generation, new_epoch);
    assert!(diagnostic.journal.is_empty());
    assert!(diagnostic.queues.is_empty());
    assert!(diagnostic.database_pools.is_empty());
}

#[test]
fn queue_lanes_publish_only_covered_series_in_one_owner_pass() {
    let mut s = LiveStore::new(1);
    s.queues.insert(
        1,
        current_fixture(QueueKind::EventDurable, 2, true, Some((800, 70)), true),
    );
    s.queues.insert(
        2,
        current_fixture(QueueKind::EventDurable, 3, true, Some((900, 10)), false),
    );
    s.queues.insert(
        3,
        current_fixture(QueueKind::EventDurable, 0, true, None, true),
    );
    let read = |s: &mut LiveStore| {
        current_numbers(s, 1000)
            .queues
            .remove("event_durable")
            .unwrap()
    };
    let known = read(&mut s);
    assert_eq!(known.oldest(), Some(270));
    assert_eq!(
        known.fields(),
        [
            ("registered_queues", Some(3)),
            ("depth_elements", Some(5)),
            ("known_capacity_elements", Some(60)),
            ("accounted_payload_bytes", Some(90)),
            ("depth_known", Some(1)),
            ("oldest_available", Some(1)),
            ("bytes_known", Some(0))
        ]
    );
    // Nonempty missing head suppresses age but leaves depth and byte series.
    s.queues.insert(
        4,
        current_fixture(QueueKind::EventDurable, 1, true, None, true),
    );
    let gap = read(&mut s);
    assert_eq!(gap.oldest(), None);
    assert_eq!(gap.fields()[1], ("depth_elements", Some(6)));
    // Even an empty owner with unknown initial depth invalidates lane depth.
    s.queues.insert(
        5,
        current_fixture(QueueKind::EventDurable, 0, false, Some((0, 9999)), true),
    );
    let unknown = read(&mut s);
    assert_eq!(
        unknown.fields(),
        [
            ("registered_queues", Some(5)),
            ("depth_elements", None),
            ("known_capacity_elements", Some(100)),
            ("accounted_payload_bytes", None),
            ("depth_known", Some(0)),
            ("oldest_available", Some(0)),
            ("bytes_known", Some(0))
        ]
    );
    let absent = current_numbers(&mut s, 1000)
        .queues
        .remove("client_rpc")
        .unwrap();
    assert_eq!(
        absent.fields(),
        [
            ("registered_queues", Some(0)),
            ("depth_elements", None),
            ("known_capacity_elements", Some(0)),
            ("accounted_payload_bytes", None),
            ("depth_known", Some(0)),
            ("oldest_available", Some(0)),
            ("bytes_known", Some(0))
        ]
    );
    s.queues.clear();
    s.queues.insert(
        1,
        current_fixture(QueueKind::EventDurable, 0, true, None, true),
    );
    assert_eq!(
        read(&mut s).fields(),
        [
            ("registered_queues", Some(1)),
            ("depth_elements", Some(0)),
            ("known_capacity_elements", Some(20)),
            ("accounted_payload_bytes", Some(30)),
            ("depth_known", Some(1)),
            ("oldest_available", Some(0)),
            ("bytes_known", Some(1))
        ]
    );
}

// Test-only SDK sink, no network/export service. Use the already enabled SDK
// PeriodicReader and explicit force_flush: ManualReader requires an experimental
// feature which this change deliberately does not enable. This test's collector
// remains alive with the OnceLock instruments, under TELEMETRY_TEST_LOCK.
#[derive(Clone, Debug)]
struct Collected(std::sync::Arc<std::sync::Mutex<Vec<(String, Vec<(String, String)>, f64)>>>);
impl opentelemetry_sdk::metrics::exporter::PushMetricExporter for Collected {
    async fn export(
        &self,
        metrics: &opentelemetry_sdk::metrics::data::ResourceMetrics,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
        let mut out = self.0.lock().unwrap();
        for metric in metrics.scope_metrics().flat_map(|scope| scope.metrics()) {
            let name = metric.name().to_owned();
            let attrs = |attrs: Vec<&opentelemetry::KeyValue>| {
                let mut attrs: Vec<_> = attrs
                    .into_iter()
                    .map(|kv| (kv.key.as_str().to_owned(), kv.value.to_string()))
                    .collect();
                attrs.sort();
                attrs
            };
            match metric.data() {
                AggregatedMetrics::U64(MetricData::Gauge(g)) => {
                    for p in g.data_points() {
                        out.push((
                            name.clone(),
                            attrs(p.attributes().collect()),
                            p.value() as f64,
                        ));
                    }
                }
                AggregatedMetrics::U64(MetricData::Sum(g)) => {
                    for p in g.data_points() {
                        out.push((
                            name.clone(),
                            attrs(p.attributes().collect()),
                            p.value() as f64,
                        ));
                    }
                }
                AggregatedMetrics::F64(MetricData::Gauge(g)) => {
                    for p in g.data_points() {
                        out.push((name.clone(), attrs(p.attributes().collect()), p.value()));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }
    fn shutdown_with_timeout(
        &self,
        _: std::time::Duration,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }
    fn temporality(&self) -> opentelemetry_sdk::metrics::Temporality {
        opentelemetry_sdk::metrics::Temporality::Delta
    }
}

#[test]
fn sdk_current_export_preserves_pending_delta_omissions_runtime_zeroes_and_consent() {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
    let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
    let _reset = fresh_epoch();
    static SDK: OnceLock<(SdkMeterProvider, Collected)> = OnceLock::new();
    let (provider, sink) = SDK.get_or_init(|| {
        assert!(
            METRICS.get().is_none(),
            "SDK fixture must own LIVE instruments"
        );
        let sink = Collected(Default::default());
        let reader = PeriodicReader::builder(sink.clone())
            .with_interval(std::time::Duration::from_secs(86400))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("live-cpu-regression");
        init(&meter);
        crate::work::init(&meter);
        crate::queues::init(&meter);
        (provider, sink)
    });
    provider.force_flush().unwrap();
    sink.0.lock().unwrap().clear();
    let epoch = crate::telemetry_consent_snapshot().1;
    with_store(epoch, |s| {
        s.work_counts[0] = 7;
        s.queue_counts[0] = 9;
        s.overflow = 11;
        s.abandoned = 12;
        s.overwritten = 13;
        s.lost = 14;
        s.queue_overflow = 15;
        s.queues.insert(
            1,
            current_fixture(QueueKind::EventDurable, 2, true, Some((0, 100)), false),
        );
        s.queues.insert(
            2,
            current_fixture(QueueKind::ClientRpc, 2, false, None, true),
        );
    });
    crate::record_in_epoch(epoch, emit_current).unwrap();
    provider.force_flush().unwrap();
    let points = std::mem::take(&mut *sink.0.lock().unwrap());
    let find = |name: &str, attrs: &[(&str, &str)]| {
        let mut attrs: Vec<_> = attrs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        attrs.sort();
        points
            .iter()
            .find(|(n, a, _)| n == name && *a == attrs)
            .map(|(_, _, v)| *v)
    };
    assert_eq!(
        find(
            "pioneer.work.active",
            &[("kind", "task"), ("state", "running")]
        ),
        Some(0.0)
    );
    assert_eq!(
        find(
            "pioneer.work.oldest.age",
            &[("kind", "task"), ("state", "running")]
        ),
        None
    );
    for (reason, expected) in [
        ("active_overflow", 11.0),
        ("abandoned", 12.0),
        ("journal_overwritten", 13.0),
        ("journal_lost", 14.0),
        ("queue_overflow", 15.0),
    ] {
        assert_eq!(
            find("pioneer.observation.loss.snapshot", &[("reason", reason)]),
            Some(expected)
        );
    }
    assert_eq!(
        find(
            "pioneer.queue.current",
            &[
                ("queue", "event_durable"),
                ("field", "accounted_payload_bytes")
            ]
        ),
        Some(30.0)
    );
    assert_eq!(
        find(
            "pioneer.queue.current",
            &[("queue", "event_durable"), ("field", "bytes_known")]
        ),
        Some(0.0)
    );
    assert_eq!(
        find(
            "pioneer.queue.current",
            &[("queue", "client_rpc"), ("field", "depth_elements")]
        ),
        None
    );
    assert_eq!(
        find(
            "pioneer.queue.current",
            &[
                ("queue", "client_rpc"),
                ("field", "accounted_payload_bytes")
            ]
        ),
        None
    );
    assert_eq!(
        find("pioneer.queue.oldest.age", &[("queue", "client_rpc")]),
        None
    );
    assert_eq!(
        find(
            "pioneer.work.boundaries",
            &[
                ("boundary", "rpc"),
                ("phase", "capture"),
                ("coverage", "accepted_with_context")
            ]
        ),
        Some(7.0)
    );
    assert_eq!(
        find(
            "pioneer.queue.events",
            &[
                ("queue", "gateway_outbound"),
                ("event", "enqueue_observed_or_prepared")
            ]
        ),
        Some(9.0)
    );
    for role in crate::runtime::RuntimeRole::ALL {
        for field in [
            "registered_runtimes",
            "workers",
            "alive_tasks",
            "global_queue_depth",
        ] {
            assert_eq!(
                find(
                    "pioneer.runtime.current",
                    &[("role", role.label()), ("field", field)]
                ),
                Some(0.0)
            );
        }
    }
    // A second current export does not replay drained Delta observations.
    crate::record_in_epoch(epoch, emit_current).unwrap();
    provider.force_flush().unwrap();
    let again = std::mem::take(&mut *sink.0.lock().unwrap());
    assert!(
        !again
            .iter()
            .any(|(n, _, _)| n == "pioneer.work.boundaries" || n == "pioneer.queue.events")
    );
    crate::set_telemetry_enabled(false);
    assert!(crate::record_in_epoch(epoch, emit_current).is_none());
    crate::set_telemetry_enabled(true);
    assert!(crate::record_in_epoch(epoch, emit_current).is_none());
    let next = crate::telemetry_consent_snapshot().1;
    crate::record_in_epoch(next, emit_current).unwrap();
    provider.force_flush().unwrap();
    let fresh = std::mem::take(&mut *sink.0.lock().unwrap());
    let mut losses: Vec<_> = fresh
        .iter()
        .filter(|(name, _, _)| name == "pioneer.observation.loss.snapshot")
        .map(|(_, attrs, value)| (attrs.clone(), *value))
        .collect();
    losses.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        losses,
        [
            "abandoned",
            "active_overflow",
            "journal_lost",
            "journal_overwritten",
            "queue_overflow"
        ]
        .into_iter()
        .map(|reason| (vec![("reason".to_owned(), reason.to_owned())], 0.0))
        .collect::<Vec<_>>()
    );
}

#[test]
fn lane_numeric_sums_saturate_without_losing_coverage() {
    let mut lane = QueueLane::empty();
    for _ in 0..2 {
        lane.add(crate::queues::QueueNumbers {
            kind: QueueKind::EventDurable,
            depth: u64::MAX,
            capacity: Some(usize::MAX),
            bytes: u64::MAX,
            initial_depth_known: true,
            oldest_age_ms: Some(50),
            bytes_known: true,
        });
    }
    assert_eq!(lane.depth, u64::MAX);
    assert_eq!(lane.bytes, u64::MAX);
    assert_eq!(
        lane.capacity,
        (usize::MAX as u64).saturating_add(usize::MAX as u64)
    );
    assert_eq!(lane.oldest(), Some(50));
    assert!(lane.known && lane.bytes_known);
}
