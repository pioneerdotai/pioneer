use std::sync::atomic::{AtomicU64, Ordering};

use pioneer_protocol::PrincipalId;

pub(crate) use pioneer_protocol::AccessChangeKind;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AccessChangeSignal {
    /// Monotonic process-local authorization epoch.
    pub(crate) authorization_revision: u64,
    pub(crate) kind: AccessChangeKind,
    /// `None` means every principal whose access can be derived from the
    /// workspace/thread must be re-evaluated.
    pub(crate) affected_principal_id: Option<PrincipalId>,
    pub(crate) workspace_id: String,
    pub(crate) thread_id: Option<String>,
}

/// Post-commit seam between durable ACL mutation and runtime eviction.
///
/// Phase 4 publishes only identifiers and a monotonic revision. Phase 6 owns
/// connection/subscription eviction and Phase 8 owns the safe client
/// notification DTO. Keeping this hub payload-free prevents an invalidation
/// from becoming an accidental protected-resource delivery path.
pub(crate) struct AuthorizationInvalidationHub {
    revision: AtomicU64,
}

impl Default for AuthorizationInvalidationHub {
    fn default() -> Self {
        Self {
            revision: AtomicU64::new(0),
        }
    }
}

impl AuthorizationInvalidationHub {
    /// Advances the shared snapshot revision for a committed non-ACL
    /// administrative projection change. No access signal is emitted because
    /// invitation/member recipients refetch through their scoped APIs.
    pub(crate) fn advance_snapshot_revision(&self) -> u64 {
        self.revision
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |revision| {
                revision.checked_add(1)
            })
            .expect("authorization invalidation revision exhausted")
            + 1
    }

    pub(crate) fn publish(
        &self,
        kind: AccessChangeKind,
        affected_principal_id: Option<PrincipalId>,
        workspace_id: impl Into<String>,
        thread_id: Option<String>,
    ) -> AccessChangeSignal {
        let revision = self.advance_snapshot_revision();
        AccessChangeSignal {
            authorization_revision: revision,
            kind,
            affected_principal_id,
            workspace_id: workspace_id.into(),
            thread_id,
        }
    }

    pub(crate) fn current_revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalidation_is_monotonic_scoped_and_payload_free() {
        let hub = AuthorizationInvalidationHub::default();
        assert_eq!(hub.current_revision(), 0);

        let principal_id =
            PrincipalId::new("P0000000000000000000A").expect("valid fixture principal");
        let first = hub.publish(
            AccessChangeKind::ThreadParticipantAdded,
            Some(principal_id.clone()),
            "workspace-red",
            Some("thread-private".to_owned()),
        );
        let second = hub.publish(
            AccessChangeKind::ThreadVisibility,
            None,
            "workspace-red",
            Some("thread-private".to_owned()),
        );
        let third = hub.publish(
            AccessChangeKind::WorkspaceMembership,
            Some(principal_id.clone()),
            "workspace-blue",
            None,
        );

        assert_eq!(first.authorization_revision, 1);
        assert_eq!(second.authorization_revision, 2);
        assert_eq!(third.authorization_revision, 3);
        assert_eq!(hub.current_revision(), 3);
        assert_eq!(first.affected_principal_id, Some(principal_id));
        assert_eq!(first.workspace_id, "workspace-red");
        assert_eq!(first.thread_id.as_deref(), Some("thread-private"));
        assert_eq!(third.workspace_id, "workspace-blue");
        assert_eq!(third.thread_id, None);
    }

    #[test]
    fn no_mutation_means_no_revision() {
        let hub = AuthorizationInvalidationHub::default();

        assert_eq!(hub.current_revision(), 0);
    }

    #[test]
    fn committed_snapshot_change_shares_the_monotonic_revision_space() {
        let hub = AuthorizationInvalidationHub::default();

        assert_eq!(hub.advance_snapshot_revision(), 1);
        assert_eq!(hub.current_revision(), 1);
        assert_eq!(
            hub.publish(
                AccessChangeKind::WorkspaceMembership,
                None,
                "workspace-red",
                None,
            )
            .authorization_revision,
            2
        );
    }
}
