//! Artifact action identity and native presentation handoffs. The artifact
//! publication owns workflow state; shells execute only the captured effect.

use super::operations::{ArtifactDownloadIdentity, ArtifactDownloadTarget};
use super::store::{ArtifactPublication, ArtifactReadState};
use crate::core::{ClientCore, ClientMutationAuthority, ClientTransition};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ArtifactActionIdentity {
    pub thread_id: String,
    pub artifact_id: String,
    pub version_id: Option<String>,
    pub generation: u64,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactActionKind {
    Open,
    Share,
    Download,
    Reveal,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArtifactActionState {
    Preparing,
    Resolving,
    Verifying,
    Ready,
    Presenting,
    Completed,
    Failed { code: String },
    Cancelled,
}

impl ArtifactActionState {
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Preparing | Self::Resolving | Self::Verifying | Self::Ready | Self::Presenting
        )
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ArtifactActionPublication {
    pub identity: ArtifactActionIdentity,
    pub target: ArtifactDownloadTarget,
    pub action: ArtifactActionKind,
    pub state: ArtifactActionState,
    pub download: Option<ArtifactDownloadIdentity>,
    pub expires_at: Option<u64>,
    pub local_file: Option<super::actions::ArtifactLocalFile>,
    #[serde(skip)]
    authentication_retried: bool,
    #[serde(skip)]
    thread_incarnation: Option<crate::threads::registry::ThreadOperationToken>,
    #[serde(skip)]
    auth_ticket: (u64, Option<u64>),
}

impl ClientCore {
    pub fn begin_artifact_action(
        &self,
        thread_id: String,
        artifact_id: String,
        version_id: Option<String>,
        action: ArtifactActionKind,
    ) -> ClientTransition {
        let Some(coordinator) = self.thread_coordinator_snapshot(&thread_id) else {
            return self.reject_intent();
        };
        let Some(thread) = coordinator.thread() else {
            return self.reject_intent();
        };
        if self.is_stopped()
            || artifact_id.trim().is_empty()
            || version_id.as_ref().is_some_and(|v| v.trim().is_empty())
            || (action != ArtifactActionKind::Reveal && self.current_auth_ticket().1.is_none())
            || !self
                .authorization_snapshot(Some(&thread.workspace_id), Some(&thread_id))
                .and_then(|s| s.thread.clone())
                .is_some_and(|t| t.capabilities.can_read_artifacts)
        {
            return self.reject_intent();
        }
        self.begin_artifact_action_for_target(
            ArtifactDownloadTarget {
                thread_id: Some(thread_id),
                workspace_id: thread.workspace_id.clone(),
                artifact_id,
                version_id,
            },
            action,
        )
    }

    fn begin_artifact_action_for_target(
        &self,
        target: ArtifactDownloadTarget,
        action: ArtifactActionKind,
    ) -> ClientTransition {
        let thread_id = target.thread_id.clone().expect("thread-scoped action");
        let thread_incarnation = self.thread_operation_token(&thread_id);
        let auth_ticket = self.current_auth_ticket();
        let mut owner = self.artifact_store.lock().expect("artifact owner poisoned");
        if self.is_stopped() || owner.suspended.contains(&thread_id) {
            return self.reject_intent();
        }
        let current = owner.publications.get(&thread_id).cloned();
        if current.as_ref().is_some_and(|p| {
            p.actions.iter().any(|a| {
                a.target == target
                    && a.state.is_active()
                    && a.thread_incarnation == thread_incarnation
                    && a.auth_ticket == auth_ticket
            })
        }) {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        }
        let generation = owner.next_generation();
        let mut next = current.map_or_else(
            || ArtifactPublication {
                thread_id: thread_id.clone(),
                workspace_id: target.workspace_id.clone(),
                revision: 0,
                generation: 0,
                items: vec![],
                request: ArtifactReadState::Idle,
                downloads: vec![],
                actions: vec![],
                previews: vec![],
            },
            |p| (*p).clone(),
        );
        if next.workspace_id != target.workspace_id {
            return self.reject_intent();
        }
        let local_file = next
            .actions
            .iter()
            .find(|a| a.target == target)
            .and_then(|a| a.local_file.clone());
        next.actions.retain(|a| a.target != target);
        next.actions.push(ArtifactActionPublication {
            identity: ArtifactActionIdentity {
                thread_id,
                artifact_id: target.artifact_id.clone(),
                version_id: target.version_id.clone(),
                generation,
            },
            target,
            action,
            state: ArtifactActionState::Preparing,
            download: None,
            expires_at: None,
            local_file,
            authentication_retried: false,
            thread_incarnation,
            auth_ticket,
        });
        self.publish_artifact(&mut owner, next)
    }

    pub fn artifact_action_snapshot(
        &self,
        identity: &ArtifactActionIdentity,
    ) -> Option<ArtifactActionPublication> {
        self.artifact_snapshot(&identity.thread_id)?
            .actions
            .iter()
            .find(|action| &action.identity == identity)
            .cloned()
    }

    /// Claims a prepared native effect exactly once. A late result may never
    /// open a browser or share a file after its route/operation was retired.
    pub fn claim_artifact_presentation(&self, identity: &ArtifactActionIdentity) -> bool {
        let mut claimed = false;
        self.change_artifact_action(identity, |action| {
            if action.state != ArtifactActionState::Ready {
                return false;
            }
            if action
                .expires_at
                .is_some_and(|expiry| expiry <= unix_seconds())
            {
                action.state = ArtifactActionState::Failed {
                    code: "grant_expired".into(),
                };
                return true;
            }
            action.state = ArtifactActionState::Presenting;
            claimed = true;
            true
        });
        claimed
    }

    pub fn start_artifact_preparation(
        &self,
        identity: &ArtifactActionIdentity,
        kind: ArtifactActionKind,
    ) -> bool {
        self.change_artifact_action(identity, |action| {
            if action.action != kind {
                return false;
            }
            let authentication_retry = action.state == ArtifactActionState::Resolving
                && kind == ArtifactActionKind::Share
                && !action.authentication_retried
                && action.download.as_ref().is_some_and(|download| {
                    self.artifact_download_snapshot(&download.operation_id)
                        .is_some_and(|p| {
                            p.identity == *download
                                && p.state == super::operations::ArtifactDownloadState::Failed
                                && p.error_code.as_deref()
                                    == Some("artifact_authentication_required")
                        })
                });
            if action.state != ArtifactActionState::Preparing && !authentication_retry {
                return false;
            }
            if authentication_retry {
                action.download = None;
                action.authentication_retried = true;
            }
            action.state = ArtifactActionState::Resolving;
            true
        })
    }

    pub fn prepare_artifact_presentation(
        &self,
        identity: &ArtifactActionIdentity,
        expires_at: Option<u64>,
    ) -> bool {
        self.change_artifact_action(identity, |action| {
            if action.state != ArtifactActionState::Resolving {
                return false;
            }
            action.state = ArtifactActionState::Ready;
            action.expires_at = expires_at;
            true
        })
    }

    pub fn attach_artifact_download(
        &self,
        identity: &ArtifactActionIdentity,
        download: &ArtifactDownloadIdentity,
    ) -> bool {
        let Some(input) = self.artifact_download_snapshot(&download.operation_id) else {
            return false;
        };
        if &input.identity != download || !input.state.is_active() {
            return false;
        }
        self.change_artifact_action(identity, |action| {
            if action.state != ArtifactActionState::Resolving
                || !matches!(
                    action.action,
                    ArtifactActionKind::Share | ArtifactActionKind::Download
                )
                || action.target != input.target
            {
                return false;
            }
            if let Some(previous) = action.download.as_ref() {
                if previous == download {
                    return false;
                }
                // Only the established session coordinator may retry a failed
                // transfer. An active transfer is never replaced.
                if self
                    .artifact_download_snapshot(&previous.operation_id)
                    .is_some_and(|p| p.identity == *previous && p.state.is_active())
                {
                    return false;
                }
            }
            action.download = Some(download.clone());
            true
        })
    }

    pub fn complete_artifact_presentation(
        &self,
        identity: &ArtifactActionIdentity,
        error: Option<String>,
    ) -> bool {
        self.change_artifact_action(identity, |action| {
            if action.state != ArtifactActionState::Presenting {
                return false;
            }
            action.state = error.map_or(ArtifactActionState::Completed, |code| {
                ArtifactActionState::Failed { code }
            });
            true
        })
    }

    pub fn fail_artifact_preparation(
        &self,
        identity: &ArtifactActionIdentity,
        code: String,
    ) -> bool {
        self.change_artifact_action(identity, |action| {
            if !matches!(
                action.state,
                ArtifactActionState::Preparing
                    | ArtifactActionState::Resolving
                    | ArtifactActionState::Verifying
            ) {
                return false;
            }
            action.state = ArtifactActionState::Failed { code };
            true
        })
    }

    pub fn cancel_artifact_action(&self, identity: &ArtifactActionIdentity) -> bool {
        let mut download = None;
        let changed = self.change_artifact_action(identity, |action| {
            if !action.state.is_active() {
                return false;
            }
            download = action.download.clone();
            action.state = ArtifactActionState::Cancelled;
            true
        });
        if let Some(download) = download {
            self.cancel_artifact_download(&download);
        }
        changed
    }

    pub(crate) fn cancel_artifact_actions(&self, thread_id: &str) {
        let identities = self
            .artifact_snapshot(thread_id)
            .map_or_else(Vec::new, |p| {
                p.actions
                    .iter()
                    .filter(|a| a.state.is_active())
                    .map(|a| a.identity.clone())
                    .collect()
            });
        for identity in identities {
            self.cancel_artifact_action(&identity);
        }
    }

    pub(super) fn change_artifact_action(
        &self,
        identity: &ArtifactActionIdentity,
        change: impl FnOnce(&mut ArtifactActionPublication) -> bool,
    ) -> bool {
        let incarnation = self.thread_operation_token(&identity.thread_id);
        let ticket = self.current_auth_ticket();
        let mut owner = self.artifact_store.lock().expect("artifact owner poisoned");
        if self.is_stopped() {
            return false;
        }
        let Some(current) = owner.publications.get(&identity.thread_id).cloned() else {
            return false;
        };
        let mut next = (*current).clone();
        let Some(action) = next.actions.iter_mut().find(|a| &a.identity == identity) else {
            return false;
        };
        if action.thread_incarnation != incarnation || action.auth_ticket != ticket {
            return false;
        }
        if !change(action) {
            return false;
        }
        self.publish_artifact(&mut owner, next);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn begin(core: &ClientCore, thread: &str) -> ArtifactActionIdentity {
        core.begin_artifact_action_for_target(
            ArtifactDownloadTarget {
                thread_id: Some(thread.into()),
                workspace_id: "workspace".into(),
                artifact_id: "artifact".into(),
                version_id: Some("version".into()),
            },
            ArtifactActionKind::Open,
        );
        core.artifact_snapshot(thread).unwrap().actions[0]
            .identity
            .clone()
    }

    #[test]
    fn downloaded_file_retains_identity_across_actions_and_rejects_late_completion() {
        use super::super::actions::ArtifactLocalFile;
        let core = Arc::new(ClientCore::new());
        let initial = begin(&core, "a");
        let target = core.artifact_action_snapshot(&initial).unwrap().target;
        core.cancel_artifact_action(&initial);
        core.begin_artifact_action_for_target(target.clone(), ArtifactActionKind::Download);
        let download = core.artifact_snapshot("a").unwrap().actions[0]
            .identity
            .clone();
        assert!(core.start_artifact_preparation(&download, ArtifactActionKind::Download));
        let file = ArtifactLocalFile {
            path: "synthetic/verified.txt".into(),
            sha256: "digest".into(),
            size_bytes: Some(7),
        };
        let mut wrong = download.clone();
        wrong.thread_id = "b".into();
        assert!(!core.complete_artifact_local_download(&wrong, file.clone()));
        assert!(core.begin_artifact_local_verification(&download));
        assert!(!core.begin_artifact_local_verification(&download));
        assert_eq!(
            core.artifact_action_snapshot(&download).unwrap().state,
            ArtifactActionState::Verifying
        );
        assert!(core.complete_artifact_local_download(&download, file.clone()));
        let completed = core.artifact_snapshot("a").unwrap();
        assert!(!core.complete_artifact_local_download(&download, file.clone()));
        assert!(Arc::ptr_eq(
            &completed,
            &core.artifact_snapshot("a").unwrap()
        ));
        core.begin_artifact_action_for_target(target.clone(), ArtifactActionKind::Reveal);
        let reveal = core.artifact_snapshot("a").unwrap().actions[0].clone();
        assert_eq!(reveal.local_file, Some(file.clone()));
        assert!(core.start_artifact_preparation(&reveal.identity, ArtifactActionKind::Reveal));
        assert!(core.prepare_artifact_presentation(&reveal.identity, None));
        assert!(core.claim_artifact_presentation(&reveal.identity));
        assert!(!core.claim_artifact_presentation(&reveal.identity));
        assert!(core.complete_artifact_presentation(&reveal.identity, None));
        core.begin_artifact_action_for_target(target.clone(), ArtifactActionKind::Download);
        let cancelled = core.artifact_snapshot("a").unwrap().actions[0]
            .identity
            .clone();
        assert!(core.start_artifact_preparation(&cancelled, ArtifactActionKind::Download));
        assert!(core.cancel_artifact_action(&cancelled));
        core.begin_artifact_action_for_target(target, ArtifactActionKind::Open);
        let newer = core.artifact_snapshot("a").unwrap();
        assert!(!core.complete_artifact_local_download(&cancelled, file.clone()));
        assert!(!core.complete_artifact_presentation(&reveal.identity, Some("late".into())));
        assert!(Arc::ptr_eq(&newer, &core.artifact_snapshot("a").unwrap()));
        assert_eq!(newer.actions[0].local_file, Some(file));
    }

    #[test]
    fn expired_grant_is_terminal_without_authorizing_a_native_effect() {
        let core = Arc::new(ClientCore::new());
        let identity = begin(&core, "thread");
        assert!(core.start_artifact_preparation(&identity, ArtifactActionKind::Open));
        assert!(core.prepare_artifact_presentation(&identity, Some(0)));
        assert!(!core.claim_artifact_presentation(&identity));
        let failed = core.artifact_snapshot("thread").unwrap();
        assert_eq!(
            failed.actions[0].state,
            ArtifactActionState::Failed {
                code: "grant_expired".into()
            }
        );
        assert!(!core.claim_artifact_presentation(&identity));
        assert!(Arc::ptr_eq(
            &failed,
            &core.artifact_snapshot("thread").unwrap()
        ));
    }

    #[test]
    fn authentication_retry_is_bounded_and_cancel_targets_only_the_attached_transfer() {
        use super::super::operations::ArtifactDownloadState;
        let core = Arc::new(ClientCore::new());
        let target = ArtifactDownloadTarget {
            thread_id: Some("thread".into()),
            workspace_id: "workspace".into(),
            artifact_id: "artifact".into(),
            version_id: Some("version".into()),
        };
        core.begin_artifact_action_for_target(target.clone(), ArtifactActionKind::Share);
        let identity = core.artifact_snapshot("thread").unwrap().actions[0]
            .identity
            .clone();
        assert!(core.start_artifact_preparation(&identity, ArtifactActionKind::Share));
        let first = core
            .begin_artifact_download("first".into(), target.clone())
            .unwrap();
        assert!(core.attach_artifact_download(&identity, first.identity()));
        assert!(!core.attach_artifact_download(&identity, first.identity()));
        assert!(first.finish(
            ArtifactDownloadState::Failed,
            Some("artifact_authentication_required".into())
        ));
        assert!(core.start_artifact_preparation(&identity, ArtifactActionKind::Share));
        assert!(!core.start_artifact_preparation(&identity, ArtifactActionKind::Share));
        let retry = core
            .begin_artifact_download("retry".into(), target.clone())
            .unwrap();
        assert!(core.attach_artifact_download(&identity, retry.identity()));
        assert!(retry.finish(
            ArtifactDownloadState::Failed,
            Some("artifact_authentication_required".into())
        ));
        assert!(!core.start_artifact_preparation(&identity, ArtifactActionKind::Share));
        assert!(
            core.fail_artifact_preparation(&identity, "artifact_authentication_required".into())
        );
        core.begin_artifact_action_for_target(target.clone(), ArtifactActionKind::Share);
        let next = core.artifact_snapshot("thread").unwrap().actions[0]
            .identity
            .clone();
        assert!(core.start_artifact_preparation(&next, ArtifactActionKind::Share));
        let live = core
            .begin_artifact_download("live".into(), target.clone())
            .unwrap();
        assert!(core.attach_artifact_download(&next, live.identity()));
        let unrelated = core
            .begin_artifact_download(
                "unrelated".into(),
                ArtifactDownloadTarget {
                    thread_id: Some("b".into()),
                    ..target
                },
            )
            .unwrap();
        assert!(core.cancel_artifact_action(&next));
        assert!(live.cancellation().is_cancelled());
        assert!(!unrelated.cancellation().is_cancelled());
        assert!(!core.prepare_artifact_presentation(&next, None));
        assert!(!core.cancel_artifact_action(&identity));
    }

    #[test]
    fn native_effect_is_claimed_once_and_late_completions_do_not_change_a_new_action() {
        let core = Arc::new(ClientCore::new());
        let a = begin(&core, "a");
        let b = begin(&core, "b");
        assert_eq!(begin(&core, "a"), a);
        assert!(!core.claim_artifact_presentation(&a));
        assert!(core.start_artifact_preparation(&a, ArtifactActionKind::Open));
        assert!(!core.start_artifact_preparation(&a, ArtifactActionKind::Open));
        assert!(core.prepare_artifact_presentation(&a, None));
        assert!(core.claim_artifact_presentation(&a));
        assert!(!core.claim_artifact_presentation(&a));
        assert!(!core.fail_artifact_preparation(&a, "late".into()));
        assert!(core.cancel_artifact_action(&a));
        assert!(!core.complete_artifact_presentation(&a, None));
        let next = begin(&core, "a");
        let before = core.artifact_snapshot("a").unwrap();
        assert_ne!(next, a);
        assert!(!core.complete_artifact_presentation(&a, Some("late".into())));
        assert!(!core.cancel_artifact_action(&a));
        assert!(Arc::ptr_eq(&before, &core.artifact_snapshot("a").unwrap()));
        assert!(core.start_artifact_preparation(&b, ArtifactActionKind::Open));
        assert!(core.prepare_artifact_presentation(&b, None));
        assert!(core.claim_artifact_presentation(&b));
        assert!(core.complete_artifact_presentation(&b, None));
        assert!(!core.complete_artifact_presentation(&b, None));
        core.cancel_artifact_actions("a");
        assert!(!core.claim_artifact_presentation(&next));
    }
}

fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
