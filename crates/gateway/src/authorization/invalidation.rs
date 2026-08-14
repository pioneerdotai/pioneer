use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use pioneer_crud::CrudStore;
use pioneer_protocol::{
    AuthorizationChangeKind, AuthorizationChangeScope, AuthorizationProjectionChangedNotification,
    PolicyGeneration, PrincipalId,
};

use super::RoleDefinitionRegistry;

static OBSERVED_POLICY_GENERATION: AtomicU64 = AtomicU64::new(0);

pub(crate) fn observed_policy_generation() -> u64 {
    OBSERVED_POLICY_GENERATION.load(Ordering::Acquire)
}

pub(crate) use pioneer_protocol::AccessChangeKind;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AccessChangeSignal {
    pub(crate) authorization_revision: u64,
    pub(crate) change: AuthorizationProjectionChangedNotification,
    pub(crate) kind: AccessChangeKind,
    /// `None` means every principal whose access can be derived from the
    /// workspace/thread must be re-evaluated.
    pub(crate) affected_principal_id: Option<PrincipalId>,
    pub(crate) workspace_id: String,
    pub(crate) thread_id: Option<String>,
}

/// Durable post-commit authorization generation and payload-safe change feed.
///
/// Production instances use `durable`; `Default` is deliberately restricted
/// to isolated tests that do not own a database.
pub(crate) struct AuthorizationInvalidationHub {
    generation: AtomicU64,
    store: Option<Arc<CrudStore>>,
}

impl Default for AuthorizationInvalidationHub {
    fn default() -> Self {
        Self {
            generation: AtomicU64::new(0),
            store: None,
        }
    }
}

impl AuthorizationInvalidationHub {
    pub(crate) fn durable(store: Arc<CrudStore>) -> Self {
        Self {
            generation: AtomicU64::new(0),
            store: Some(store),
        }
    }

    pub(crate) async fn current_generation(&self) -> Result<PolicyGeneration> {
        if let Some(store) = &self.store {
            let fingerprint = RoleDefinitionRegistry::new().policy_fingerprint();
            let initialized = pioneer_crud::ensure_code_policy_generation(
                &store.database_connection(),
                fingerprint.as_str(),
            )
            .await
            .context("failed to initialize durable code-policy generation")?;
            self.observe(initialized.policy_generation);
            let durable =
                pioneer_crud::current_policy_generation(&store.database_connection()).await?;
            self.observe(durable);
            return Ok(durable);
        }
        let current = self.generation.load(Ordering::Acquire);
        if let Some(current) = PolicyGeneration::new(current) {
            return Ok(current);
        }
        self.generation
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .ok();
        Ok(PolicyGeneration::INITIAL)
    }

    pub(crate) async fn current_revision(&self) -> Result<u64> {
        Ok(self.current_generation().await?.get())
    }

    pub(crate) async fn publish_change(
        &self,
        change: AuthorizationChangeKind,
        affected: AuthorizationChangeScope,
    ) -> Result<AuthorizationProjectionChangedNotification> {
        let notification = if let Some(store) = &self.store {
            // Ensures a deployment policy change owns an earlier generation
            // than the mutation being published.
            self.current_generation().await?;
            pioneer_crud::append_authorization_change(
                &store.database_connection(),
                change,
                affected,
            )
            .await
            .context("failed to append durable authorization change")?
        } else {
            let current = self.current_generation().await?;
            let next = current
                .get()
                .checked_add(1)
                .and_then(PolicyGeneration::new)
                .expect("authorization policy generation exhausted");
            self.generation.store(next.get(), Ordering::Release);
            AuthorizationProjectionChangedNotification {
                policy_generation: next,
                change,
                affected,
            }
        };
        self.observe(notification.policy_generation);
        Ok(notification)
    }

    pub(crate) async fn publish(
        &self,
        kind: AccessChangeKind,
        affected_principal_id: Option<PrincipalId>,
        workspace_id: impl Into<String>,
        thread_id: Option<String>,
    ) -> Result<AccessChangeSignal> {
        let workspace_id = workspace_id.into();
        let affected = match (&affected_principal_id, &thread_id) {
            (Some(principal_id), Some(thread_id)) => AuthorizationChangeScope::PrincipalThread {
                principal_id: principal_id.clone(),
                workspace_id: workspace_id.clone(),
                thread_id: thread_id.clone(),
            },
            (Some(principal_id), None) => AuthorizationChangeScope::PrincipalWorkspace {
                principal_id: principal_id.clone(),
                workspace_id: workspace_id.clone(),
            },
            (None, Some(thread_id)) => AuthorizationChangeScope::Thread {
                workspace_id: workspace_id.clone(),
                thread_id: thread_id.clone(),
            },
            (None, None) => AuthorizationChangeScope::Workspace {
                workspace_id: workspace_id.clone(),
            },
        };
        let change_kind = match kind {
            AccessChangeKind::WorkspaceMembership => AuthorizationChangeKind::WorkspaceAcl,
            AccessChangeKind::ThreadCreated
            | AccessChangeKind::ThreadVisibility
            | AccessChangeKind::ThreadParticipantAdded
            | AccessChangeKind::ThreadParticipantRemoved => AuthorizationChangeKind::ThreadAcl,
        };
        let change = self.publish_change(change_kind, affected).await?;
        Ok(AccessChangeSignal {
            authorization_revision: change.policy_generation.get(),
            change,
            kind,
            affected_principal_id,
            workspace_id,
            thread_id,
        })
    }

    fn observe(&self, generation: PolicyGeneration) {
        self.generation
            .fetch_max(generation.get(), Ordering::AcqRel);
        OBSERVED_POLICY_GENERATION.fetch_max(generation.get(), Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalidation_is_monotonic_scoped_and_payload_free() {
        let hub = AuthorizationInvalidationHub::default();
        assert_eq!(hub.current_revision().await.unwrap(), 1);

        let principal_id =
            PrincipalId::new("P0000000000000000000A").expect("valid fixture principal");
        let first = hub
            .publish(
                AccessChangeKind::ThreadParticipantAdded,
                Some(principal_id.clone()),
                "workspace-red",
                Some("thread-private".to_owned()),
            )
            .await
            .unwrap();
        let second = hub
            .publish(
                AccessChangeKind::ThreadVisibility,
                None,
                "workspace-red",
                Some("thread-private".to_owned()),
            )
            .await
            .unwrap();

        assert_eq!(first.authorization_revision, 2);
        assert_eq!(second.authorization_revision, 3);
        assert_eq!(hub.current_revision().await.unwrap(), 3);
        assert!(matches!(
            first.change.affected,
            AuthorizationChangeScope::PrincipalThread { .. }
        ));
        assert!(matches!(
            second.change.affected,
            AuthorizationChangeScope::Thread { .. }
        ));
    }
}
