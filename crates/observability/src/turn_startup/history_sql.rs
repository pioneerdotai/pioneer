//! Callback elapsed time, independent of query_cpu capture and DB stage budgets.
//! Only completed callbacks observed before close are covered. This is not a
//! census of issued SQL, nor necessarily equal to stage.db.execute_ms.
//!
//! Export: three aligned top arrays (descending elapsed_ms; 16-digit hex IDs),
//! plus disjoint exported/unknown/overflow/omitted buckets. Their sums equal
//! observed, except count saturation and floating-point rounding. Unknown means
//! no canonical ID; overflow means an unadmitted ID after 64 accepted shapes;
//! omitted means an accepted shape outside the exported top 8. Repeated accepted
//! IDs continue to accumulate after overflow. top_complete only promises a global
//! top among observed callbacks when unknown and overflow are empty. It says
//! nothing about SQL without callbacks or callbacks arriving after phase close;
//! stage.completion separately distinguishes return from cancellation/drop.
//!
//! At most 20 attributes, three arrays of at most 8 elements, and 128 fingerprint
//! characters. Existing history.capture attributes use at most 28 more slots,
//! comfortably below the SDK default 128 (no repository override). Operators
//! lowering OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT must budget for both sets.
//! Memory: at most 64 (u64, Bucket) pairs per observed capture, no SQL/payload.
//! Callback: bounded linear lookup, no sorting/export/await under the mutex.
//! Nested captures each receive a callback once, without summing child totals;
//! the existing startup stage cap bounds the ancestor chain to 128 captures.
//! Closing takes and frees the summary even if streams retain recorder handles.
//! Consent generation is checked on creation, mutation, close, and again by the
//! existing startup span exporter gate. There is no query_cpu session activation.
use opentelemetry::{Array, Context, ContextGuard, KeyValue};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const FINGERPRINT_LIMIT: usize = 64;
const TOP_LIMIT: usize = 8;

/// Explicitly carried by OTel per-poll context; streams retain their creating
/// capture, including absence of a capture, rather than borrowing the consumer's.
#[derive(Clone, Default)]
pub struct HistorySqlContext(Option<Arc<Recorder>>);
struct Recorder {
    epoch: u64,
    parent: HistorySqlContext,
    summary: Mutex<Option<Summary>>,
}
#[derive(Default)]
struct Summary {
    known: Vec<(u64, Bucket)>,
    unknown: Bucket,
    overflow: Bucket,
}
#[derive(Clone, Copy, Default)]
struct Bucket {
    count: i64,
    elapsed_ms: f64,
}
impl Bucket {
    fn add(&mut self, other: Self) {
        self.count = self.count.saturating_add(other.count);
        self.elapsed_ms += other.elapsed_ms;
    }
}
impl Summary {
    fn record(&mut self, fingerprint: Option<u64>, elapsed: Duration) {
        let bucket = match fingerprint {
            None => &mut self.unknown,
            Some(fp) => {
                if let Some(index) = self.known.iter().position(|(key, _)| *key == fp) {
                    &mut self.known[index].1
                } else if self.known.len() < FINGERPRINT_LIMIT {
                    self.known.push((fp, Bucket::default()));
                    &mut self.known.last_mut().unwrap().1
                } else {
                    &mut self.overflow
                }
            }
        };
        bucket.add(Bucket {
            count: 1,
            elapsed_ms: elapsed.as_secs_f64() * 1000.,
        });
    }

    fn attributes(mut self) -> Vec<KeyValue> {
        // No recorder lock during sorting, allocation or export. Ties are stable.
        self.known.sort_unstable_by(|a, b| {
            b.1.elapsed_ms
                .total_cmp(&a.1.elapsed_ms)
                .then(a.0.cmp(&b.0))
        });
        let mut total = self.unknown;
        total.add(self.overflow);
        let mut omitted = Bucket::default();
        let mut exported = Bucket::default();
        let mut fingerprints = Vec::new();
        let mut counts = Vec::new();
        let mut elapsed_ms = Vec::new();
        for (index, (fingerprint, bucket)) in self.known.iter().enumerate() {
            total.add(*bucket);
            if index < TOP_LIMIT {
                exported.add(*bucket);
                fingerprints.push(format!("{fingerprint:016x}").into());
                counts.push(bucket.count);
                elapsed_ms.push(bucket.elapsed_ms);
            } else {
                omitted.add(*bucket);
            }
        }
        let mut attrs = vec![
            KeyValue::new("history.sql.coverage", "completed_callbacks_before_close"),
            KeyValue::new("history.sql.top.fingerprints", Array::String(fingerprints)),
            KeyValue::new("history.sql.top.counts", Array::I64(counts)),
            KeyValue::new("history.sql.top.elapsed_ms", Array::F64(elapsed_ms)),
            KeyValue::new("history.sql.accepted_fingerprints", self.known.len() as i64),
            KeyValue::new("history.sql.fingerprint_limit", FINGERPRINT_LIMIT as i64),
            KeyValue::new("history.sql.export_limit", TOP_LIMIT as i64),
            KeyValue::new(
                "history.sql.identity_complete",
                self.unknown.count == 0 && self.overflow.count == 0,
            ),
            KeyValue::new(
                "history.sql.top_complete",
                self.overflow.count == 0 && self.unknown.count == 0,
            ),
            KeyValue::new("history.sql.count_saturated", total.count == i64::MAX),
        ];
        for (count, ms, bucket) in [
            (
                "history.sql.observed.count",
                "history.sql.observed.elapsed_ms",
                total,
            ),
            (
                "history.sql.exported.count",
                "history.sql.exported.elapsed_ms",
                exported,
            ),
            (
                "history.sql.unknown.count",
                "history.sql.unknown.elapsed_ms",
                self.unknown,
            ),
            (
                "history.sql.overflow.count",
                "history.sql.overflow.elapsed_ms",
                self.overflow,
            ),
            (
                "history.sql.omitted.count",
                "history.sql.omitted.elapsed_ms",
                omitted,
            ),
        ] {
            attrs.push(KeyValue::new(count, bucket.count));
            attrs.push(KeyValue::new(ms, bucket.elapsed_ms));
        }
        attrs
    }
}
impl HistorySqlContext {
    pub(super) fn start(epoch: u64, parent: Self) -> Self {
        if crate::telemetry_consent_snapshot() != (true, epoch) {
            return Self::default();
        }
        Self(Some(Arc::new(Recorder {
            epoch,
            parent,
            summary: Mutex::new(Some(Summary::default())),
        })))
    }
    pub fn capture() -> Self {
        Context::current()
            .get::<Self>()
            .cloned()
            .unwrap_or_default()
    }
    /// Synchronous poll/drop scope only; never hold this guard across await.
    pub fn enter(&self) -> Option<ContextGuard> {
        let current = Context::current();
        let ambient = current.get::<Self>().and_then(|context| context.0.as_ref());
        match (self.0.as_ref(), ambient) {
            (None, None) => return None,
            (Some(saved), Some(current)) if Arc::ptr_eq(saved, current) => return None,
            _ => {}
        }
        Some(current.with_value(self.clone()).attach())
    }
    fn record(&self, fingerprint: Option<u64>, elapsed: Duration) {
        let mut node = self;
        while let Some(recorder) = &node.0 {
            let mut summary = recorder
                .summary
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if crate::telemetry_consent_snapshot() != (true, recorder.epoch) {
                *summary = None;
            }
            if let Some(summary) = summary.as_mut() {
                summary.record(fingerprint, elapsed);
            }
            drop(summary);
            node = &recorder.parent;
        }
    }
    pub(super) fn finish(&self) -> Vec<KeyValue> {
        let Some(recorder) = &self.0 else {
            return Vec::new();
        };
        let summary = recorder
            .summary
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if crate::telemetry_consent_snapshot() != (true, recorder.epoch) {
            return Vec::new();
        }
        summary.map(Summary::attributes).unwrap_or_default()
    }
}

/// Called exactly once by the existing SQL completion callback, with the
/// already computed canonical identity (never metric overflow identity).
pub fn record_history_sql(fingerprint: Option<u64>, elapsed: Duration) {
    HistorySqlContext::capture().record(fingerprint, elapsed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_top_preserves_identity_and_disjoint_remainders() {
        let mut summary = Summary::default();
        // Distinct identity with its high bit set must survive JSON consumers.
        summary.record(Some(u64::MAX), Duration::from_millis(100));
        for fp in 1..64 {
            summary.record(Some(fp), Duration::from_millis(fp));
        }
        summary.record(Some(65), Duration::from_millis(200));
        summary.record(None, Duration::from_millis(300));
        summary.record(Some(1), Duration::from_millis(400));
        let attrs = summary.attributes();
        let top: Vec<opentelemetry::StringValue> = [
            "0000000000000001",
            "ffffffffffffffff",
            "000000000000003f",
            "000000000000003e",
            "000000000000003d",
            "000000000000003c",
            "000000000000003b",
            "000000000000003a",
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        assert!(attrs.contains(&KeyValue::new(
            "history.sql.top.fingerprints",
            Array::String(top)
        )));
        for (category, count, ms) in [
            ("observed", 67, 3016.),
            ("exported", 9, 864.),
            ("unknown", 1, 300.),
            ("overflow", 1, 200.),
            ("omitted", 56, 1652.),
        ] {
            assert!(attrs.contains(&KeyValue::new(
                format!("history.sql.{category}.count"),
                count as i64
            )));
            assert!(attrs.contains(&KeyValue::new(
                format!("history.sql.{category}.elapsed_ms"),
                ms
            )));
        }
        assert!(attrs.contains(&KeyValue::new("history.sql.top_complete", false)));
    }

    #[test]
    fn contexts_isolate_captures_include_nested_work_and_close_before_late_callback() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(true);
        let epoch = crate::telemetry_consent_snapshot().1;
        let first = HistorySqlContext::start(epoch, HistorySqlContext::default());
        let second = HistorySqlContext::start(epoch, HistorySqlContext::default());
        let child;
        {
            let _first = first.enter();
            // An ordinary nested OTel phase retains the same SQL owner.
            let _nested = Context::current().with_value(17_u32).attach();
            record_history_sql(Some(7), Duration::from_millis(2));
            // Starting an unrelated observation under an ambient capture must
            // use its validated parent (absent here), never implicitly inherit.
            let foreign = HistorySqlContext::start(epoch, HistorySqlContext::default());
            {
                let _foreign = foreign.enter();
                record_history_sql(Some(19), Duration::from_millis(11));
            }
            assert!(
                foreign
                    .finish()
                    .contains(&KeyValue::new("history.sql.observed.count", 1_i64))
            );
            child = HistorySqlContext::start(epoch, HistorySqlContext::capture());
            let _child = child.enter();
            record_history_sql(None, Duration::from_millis(3));
            {
                let _second = second.enter();
                record_history_sql(Some(u64::MAX), Duration::from_millis(5));
                // A saved absent owner shadows the unrelated ambient capture.
                let _absent = HistorySqlContext::default().enter();
                record_history_sql(Some(99), Duration::from_secs(1));
            }
        }
        let attrs = first.finish();
        assert!(attrs.contains(&KeyValue::new("history.sql.observed.count", 2_i64)));
        assert!(attrs.contains(&KeyValue::new("history.sql.observed.elapsed_ms", 5.)));
        assert!(
            child
                .finish()
                .contains(&KeyValue::new("history.sql.unknown.count", 1_i64))
        );
        {
            let _second = second.enter();
            let _late = first.enter();
            record_history_sql(Some(7), Duration::from_secs(9));
        }
        assert!(first.finish().is_empty());
        assert!(
            second
                .finish()
                .contains(&KeyValue::new("history.sql.observed.elapsed_ms", 5.))
        );
        crate::set_telemetry_enabled(old);
    }

    #[test]
    fn disabled_and_revoked_consent_never_export_old_samples() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        let disabled = HistorySqlContext::start(
            crate::telemetry_consent_snapshot().1,
            HistorySqlContext::default(),
        );
        assert!(disabled.0.is_none());
        crate::set_telemetry_enabled(true);
        let revoked = HistorySqlContext::start(
            crate::telemetry_consent_snapshot().1,
            HistorySqlContext::default(),
        );
        revoked.record(Some(1), Duration::from_millis(1));
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        revoked.record(Some(2), Duration::from_millis(2));
        assert!(revoked.finish().is_empty());
        crate::set_telemetry_enabled(old);
    }
}
