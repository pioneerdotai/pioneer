//! Numeric, bounded phase summaries. DB totals are inclusive of nested phases;
//! they continue after the per-query span cap and are never metric dimensions.
use super::Stage;
use opentelemetry::KeyValue;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Default)]
pub(super) struct Diagnostics(Arc<Node>);

#[derive(Default)]
struct Node {
    parent: Option<Diagnostics>,
    totals: Mutex<Totals>,
}

#[derive(Default)]
struct Totals {
    closed: bool,
    db_count: [u64; 4],
    db_ms: [f64; 4],
    work: [u64; 4],
}

/// Counts work performed, not unique domain entities. Re-reading a page or
/// rebuilding a message increments the count again; values contain no payload.
#[derive(Clone, Copy)]
pub enum Work {
    Quanta,
    Pages,
    Messages,
    Branches,
}

fn db_index(stage: Stage) -> Option<usize> {
    match stage {
        Stage::DbAdmission => Some(0),
        Stage::DbAcquire => Some(1),
        Stage::DbExecute => Some(2),
        Stage::DbCommit => Some(3),
        _ => None,
    }
}

pub(super) fn is_db(stage: Stage) -> bool {
    db_index(stage).is_some()
}

impl Diagnostics {
    pub(super) fn new(parent: Option<Self>) -> Self {
        Self(Arc::new(Node {
            parent,
            totals: Mutex::default(),
        }))
    }

    pub(super) fn record_db(&self, stage: Stage, elapsed: Duration) {
        let Some(index) = db_index(stage) else { return };
        let mut node = Some(self);
        while let Some(current) = node {
            if let Ok(mut totals) = current.0.totals.lock() {
                if !totals.closed {
                    totals.db_count[index] = totals.db_count[index].saturating_add(1);
                    totals.db_ms[index] += elapsed.as_secs_f64() * 1000.;
                }
            }
            node = current.0.parent.as_ref();
        }
    }

    pub(super) fn record_work(&self, work: Work, count: u64) {
        let index = match work {
            Work::Quanta => 0,
            Work::Pages => 1,
            Work::Messages => 2,
            Work::Branches => 3,
        };
        if let Ok(mut totals) = self.0.totals.lock() {
            if !totals.closed {
                totals.work[index] = totals.work[index].saturating_add(count);
            }
        }
    }

    pub(super) fn finish(&self) -> Vec<KeyValue> {
        let Ok(mut totals) = self.0.totals.lock() else {
            return Vec::new();
        };
        totals.closed = true;
        let mut attrs = Vec::new();
        for (index, (count, duration)) in [
            ("stage.db.admission.count", "stage.db.admission_ms"),
            ("stage.db.pool.count", "stage.db.pool_ms"),
            ("stage.db.execute.count", "stage.db.execute_ms"),
            ("stage.db.commit.count", "stage.db.commit_ms"),
        ]
        .into_iter()
        .enumerate()
        {
            attrs.push(KeyValue::new(
                count,
                totals.db_count[index].min(i64::MAX as u64) as i64,
            ));
            attrs.push(KeyValue::new(duration, totals.db_ms[index]));
        }
        for (index, name) in [
            "stage.work.quanta",
            "stage.work.pages",
            "stage.work.messages",
            "stage.work.branches",
        ]
        .into_iter()
        .enumerate()
        {
            if totals.work[index] > 0 {
                attrs.push(KeyValue::new(
                    name,
                    totals.work[index].min(i64::MAX as u64) as i64,
                ));
            }
        }
        attrs
    }
}
