//! Test observations shared by the checkpoint reader and publication coordinator.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublicationTestMetrics {
    pub coverage_checks: usize,
    pub manifest_checks: usize,
    pub heavy_checks_while_writer: usize,
    pub writer_entries: usize,
    pub proof_seal_writer_entries: usize,
    pub topology_pages: usize,
    pub topology_page_rows: usize,
    pub topology_page_bytes: usize,
    pub dependency_checks: usize,
    pub writer_depth: usize,
}

static PUBLICATION_TEST_METRICS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<String, PublicationTestMetrics>>,
> = std::sync::OnceLock::new();

pub fn reset_publication_test_metrics(operation: &str) {
    PUBLICATION_TEST_METRICS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(operation.to_owned(), PublicationTestMetrics::default());
}

pub fn publication_test_metrics(operation: &str) -> PublicationTestMetrics {
    PUBLICATION_TEST_METRICS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(operation)
        .copied()
        .unwrap_or_default()
}

pub fn record_publication_heavy_check(operation: &str, manifest: bool) {
    let mut all = PUBLICATION_TEST_METRICS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(metrics) = all.get_mut(operation) else {
        return;
    };
    if manifest {
        metrics.manifest_checks += 1;
    } else {
        metrics.coverage_checks += 1;
    }
    if metrics.writer_depth != 0 {
        metrics.heavy_checks_while_writer += 1;
    }
}

pub fn record_proof_preparation_check(operation: &str, topology: bool) {
    let mut all = PUBLICATION_TEST_METRICS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(metrics) = all.get_mut(operation) {
        if topology {
            metrics.topology_pages += 1;
        } else {
            metrics.dependency_checks += 1;
        }
        if metrics.writer_depth != 0 {
            metrics.heavy_checks_while_writer += 1;
        }
    }
}

pub fn record_topology_page(operation: &str, rows: usize, bytes: usize) {
    let mut all = PUBLICATION_TEST_METRICS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(metrics) = all.get_mut(operation) {
        metrics.topology_page_rows = metrics.topology_page_rows.max(rows);
        metrics.topology_page_bytes = metrics.topology_page_bytes.max(bytes);
    }
}

pub struct PublicationWriterTestGuard(String);

impl PublicationWriterTestGuard {
    pub fn enter_seal(operation: &str) -> Self {
        let guard = Self::enter(operation);
        let mut all = PUBLICATION_TEST_METRICS
            .get()
            .expect("metrics initialized by enter")
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(metrics) = all.get_mut(operation) {
            metrics.proof_seal_writer_entries += 1;
        }
        guard
    }
    pub fn enter(operation: &str) -> Self {
        let mut all = PUBLICATION_TEST_METRICS
            .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(metrics) = all.get_mut(operation) {
            metrics.writer_entries += 1;
            metrics.writer_depth += 1;
        }
        Self(operation.to_owned())
    }
}

impl Drop for PublicationWriterTestGuard {
    fn drop(&mut self) {
        let mut all = PUBLICATION_TEST_METRICS
            .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(metrics) = all.get_mut(&self.0) {
            metrics.writer_depth -= 1;
        }
    }
}
