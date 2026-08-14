use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak},
    time::Duration,
};

use anyhow::{Context as _, Result, ensure};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::timeout,
};

use super::ObservationResourcePolicy;

/// Gateway-owned constructor for durable hierarchical execution reservations.
/// Counting and atomic persistence live in `pioneer-crud` so Turn and Task
/// creation can reserve inside their existing durable-start transactions.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ExecutionAdmissionGovernor;

impl ExecutionAdmissionGovernor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lease(
        principal_id: &str,
        role_key: &str,
        workspace_id: &str,
        policy_fingerprint: &str,
        policy: pioneer_crud::ExecutionAdmissionQuotaPolicy,
        operation_class: pioneer_crud::ExecutionAdmissionClass,
        subject_kind: &str,
        subject_id: &str,
    ) -> pioneer_crud::NewExecutionAdmissionLease {
        pioneer_crud::NewExecutionAdmissionLease {
            id: format!("quota:{subject_kind}:{subject_id}"),
            subject_kind: subject_kind.to_owned(),
            subject_id: subject_id.to_owned(),
            operation_class,
            principal_id: principal_id.to_owned(),
            role_key: role_key.to_owned(),
            workspace_id: workspace_id.to_owned(),
            policy_fingerprint: policy_fingerprint.to_owned(),
            policy,
        }
    }
}

const GATEWAY_CONCURRENT_OBSERVATION_PAGES: usize = 256;
const OBSERVATION_ADMISSION_WAIT: Duration = Duration::from_secs(1);

/// Bounded, hierarchical admission for operational observation pages.
///
/// Action authorization answers whether a principal may observe a resource;
/// this governor independently limits how many page materializations the same
/// principal, role and workspace may occupy at once. Weak registries are
/// pruned on every acquisition so attacker-controlled scope keys cannot leave
/// an unbounded idle map behind.
#[derive(Debug)]
pub(crate) struct ObservationAdmissionGovernor {
    global: Arc<Semaphore>,
    principals: ScopedObservationSemaphores,
    roles: ScopedObservationSemaphores,
    workspaces: ScopedObservationSemaphores,
}

impl Default for ObservationAdmissionGovernor {
    fn default() -> Self {
        Self {
            global: Arc::new(Semaphore::new(GATEWAY_CONCURRENT_OBSERVATION_PAGES)),
            principals: ScopedObservationSemaphores::default(),
            roles: ScopedObservationSemaphores::default(),
            workspaces: ScopedObservationSemaphores::default(),
        }
    }
}

impl ObservationAdmissionGovernor {
    pub(crate) async fn acquire_page(
        &self,
        principal_id: &str,
        role_key: &str,
        workspace_id: &str,
        policy: ObservationResourcePolicy,
    ) -> Result<ObservationAdmissionPermit> {
        ensure!(
            policy.max_concurrent_pages_per_principal > 0
                && policy.max_concurrent_pages_per_role > 0
                && policy.max_concurrent_pages_per_workspace > 0,
            "observation policy disables page admission"
        );

        // Acquire the global permit first. This bounds both the number of live
        // scoped semaphore keys and the amount of work waiting below them.
        let mut permits = Vec::with_capacity(4);
        permits.push(acquire_observation_permit(self.global.clone(), "gateway").await?);
        permits.push(
            acquire_observation_permit(
                self.workspaces
                    .semaphore(workspace_id, policy.max_concurrent_pages_per_workspace)?,
                "workspace",
            )
            .await?,
        );
        permits.push(
            acquire_observation_permit(
                self.roles
                    .semaphore(role_key, policy.max_concurrent_pages_per_role)?,
                "role",
            )
            .await?,
        );
        permits.push(
            acquire_observation_permit(
                self.principals
                    .semaphore(principal_id, policy.max_concurrent_pages_per_principal)?,
                "principal",
            )
            .await?,
        );

        Ok(ObservationAdmissionPermit { _permits: permits })
    }
}

#[derive(Debug)]
pub(crate) struct ObservationAdmissionPermit {
    _permits: Vec<OwnedSemaphorePermit>,
}

#[derive(Debug, Default)]
struct ScopedObservationSemaphores {
    entries: StdMutex<HashMap<String, Weak<Semaphore>>>,
}

impl ScopedObservationSemaphores {
    fn semaphore(&self, key: &str, permits: usize) -> Result<Arc<Semaphore>> {
        ensure!(
            permits > 0 && permits <= Semaphore::MAX_PERMITS,
            "invalid observation semaphore capacity"
        );
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| anyhow::anyhow!("observation governor registry is poisoned"))?;
        entries.retain(|_, semaphore| semaphore.strong_count() > 0);
        if let Some(existing) = entries.get(key).and_then(Weak::upgrade) {
            return Ok(existing);
        }
        let semaphore = Arc::new(Semaphore::new(permits));
        entries.insert(key.to_owned(), Arc::downgrade(&semaphore));
        Ok(semaphore)
    }
}

async fn acquire_observation_permit(
    semaphore: Arc<Semaphore>,
    scope: &'static str,
) -> Result<OwnedSemaphorePermit> {
    timeout(OBSERVATION_ADMISSION_WAIT, semaphore.acquire_owned())
        .await
        .with_context(|| format!("{scope} observation concurrency budget is exhausted"))?
        .map_err(|_| anyhow::anyhow!("{scope} observation governor is closed"))
}
