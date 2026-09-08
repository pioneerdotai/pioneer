//! Process-local download request ownership. Disk retention remains in the
//! authenticated download service; this registry retains operation state only.

use super::http_download::ArtifactHttpDownloadProgress;
use crate::core::ClientCore;
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};
use tokio_util::sync::CancellationToken;

// Preserve the existing native download operation bounds.
const MAX_ACTIVE_DOWNLOAD_OPERATIONS: usize = 8;
const MAX_TRACKED_DOWNLOAD_OPERATIONS: usize = 128;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactDownloadIdentity {
    pub operation_id: String,
    pub generation: u64,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ArtifactDownloadTarget {
    pub thread_id: Option<String>,
    pub workspace_id: String,
    pub artifact_id: String,
    pub version_id: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactDownloadState {
    Queued,
    Downloading,
    Completed,
    Failed,
    Cancelled,
}

impl ArtifactDownloadState {
    pub fn is_active(self) -> bool {
        matches!(self, Self::Queued | Self::Downloading)
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ArtifactDownloadPublication {
    pub identity: ArtifactDownloadIdentity,
    pub target: ArtifactDownloadTarget,
    pub state: ArtifactDownloadState,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub resumed_from_bytes: u64,
    pub error_code: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactDownloadOperationError {
    InvalidIdentity,
    AlreadyActive,
    Capacity,
    NotFound,
    Stopped,
}

struct Operation {
    publication: Arc<ArtifactDownloadPublication>,
    cancellation: CancellationToken,
}

#[derive(Default)]
pub(crate) struct ArtifactDownloadController {
    generation: u64,
    operations: HashMap<String, Operation>,
}

/// A captured request lease. Dropping unfinished work cancels only this exact
/// generation, including when its native presentation owner disappears.
pub struct ArtifactDownloadOperation {
    core: Weak<ClientCore>,
    identity: ArtifactDownloadIdentity,
    cancellation: CancellationToken,
}

impl ArtifactDownloadOperation {
    pub fn identity(&self) -> &ArtifactDownloadIdentity {
        &self.identity
    }
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
    pub fn update_progress(&self, progress: ArtifactHttpDownloadProgress) {
        if let Some(core) = self.core.upgrade() {
            core.update_artifact_download(&self.identity, progress);
        }
    }
    pub fn finish(&self, state: ArtifactDownloadState, error_code: Option<String>) -> bool {
        self.core
            .upgrade()
            .is_some_and(|core| core.finish_artifact_download(&self.identity, state, error_code))
    }
}

impl Drop for ArtifactDownloadOperation {
    fn drop(&mut self) {
        if let Some(core) = self.core.upgrade() {
            core.cancel_artifact_download(&self.identity);
        }
        self.cancellation.cancel();
    }
}

impl ClientCore {
    pub fn begin_artifact_download_for_target(
        self: &Arc<Self>,
        target: ArtifactDownloadTarget,
    ) -> Result<ArtifactDownloadOperation, ArtifactDownloadOperationError> {
        let operation_id = {
            let mut owner = self
                .artifact_downloads
                .lock()
                .expect("artifact download owner poisoned");
            owner.generation = owner
                .generation
                .checked_add(1)
                .expect("artifact download generation exhausted");
            format!("download-{}", owner.generation)
        };
        self.begin_artifact_download(operation_id, target)
    }
    pub fn begin_artifact_download(
        self: &Arc<Self>,
        operation_id: String,
        target: ArtifactDownloadTarget,
    ) -> Result<ArtifactDownloadOperation, ArtifactDownloadOperationError> {
        if operation_id.is_empty()
            || operation_id.len() > 128
            || !operation_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        {
            return Err(ArtifactDownloadOperationError::InvalidIdentity);
        }
        let mut owner = self
            .artifact_downloads
            .lock()
            .expect("artifact download owner poisoned");
        if self.is_stopped() {
            return Err(ArtifactDownloadOperationError::Stopped);
        }
        if owner
            .operations
            .get(&operation_id)
            .is_some_and(|o| o.publication.state.is_active())
        {
            return Err(ArtifactDownloadOperationError::AlreadyActive);
        }
        if owner
            .operations
            .values()
            .filter(|o| o.publication.state.is_active())
            .count()
            >= MAX_ACTIVE_DOWNLOAD_OPERATIONS
        {
            return Err(ArtifactDownloadOperationError::Capacity);
        }
        if !owner.operations.contains_key(&operation_id)
            && owner.operations.len() >= MAX_TRACKED_DOWNLOAD_OPERATIONS
        {
            let remove = owner.operations.len() + 1 - MAX_TRACKED_DOWNLOAD_OPERATIONS;
            let terminal = owner
                .operations
                .iter()
                .filter(|(_, o)| !o.publication.state.is_active())
                .map(|(id, _)| id.clone())
                .take(remove)
                .collect::<Vec<_>>();
            for id in terminal {
                owner.operations.remove(&id);
            }
        }
        owner.generation = owner
            .generation
            .checked_add(1)
            .expect("artifact download generation exhausted");
        let identity = ArtifactDownloadIdentity {
            operation_id: operation_id.clone(),
            generation: owner.generation,
        };
        let cancellation = CancellationToken::new();
        let publication = Arc::new(ArtifactDownloadPublication {
            identity: identity.clone(),
            target,
            state: ArtifactDownloadState::Queued,
            downloaded_bytes: 0,
            total_bytes: 0,
            resumed_from_bytes: 0,
            error_code: None,
        });
        let thread_id = publication.target.thread_id.clone();
        owner.operations.insert(
            operation_id,
            Operation {
                publication,
                cancellation: cancellation.clone(),
            },
        );
        drop(owner);
        self.publish_artifact_downloads(thread_id.as_deref());
        Ok(ArtifactDownloadOperation {
            core: Arc::downgrade(self),
            identity,
            cancellation,
        })
    }

    pub(crate) fn artifact_downloads_for_thread(
        &self,
        thread_id: &str,
    ) -> Vec<ArtifactDownloadPublication> {
        let owner = self
            .artifact_downloads
            .lock()
            .expect("artifact download owner poisoned");
        let mut inputs = owner
            .operations
            .values()
            .filter(|o| o.publication.target.thread_id.as_deref() == Some(thread_id))
            .map(|o| (*o.publication).clone())
            .collect::<Vec<_>>();
        inputs.sort_by_key(|p| p.identity.generation);
        inputs
    }

    pub fn artifact_download_snapshot(
        &self,
        operation_id: &str,
    ) -> Option<Arc<ArtifactDownloadPublication>> {
        self.artifact_downloads
            .lock()
            .expect("artifact download owner poisoned")
            .operations
            .get(operation_id)
            .map(|o| o.publication.clone())
    }

    fn update_artifact_download(
        &self,
        identity: &ArtifactDownloadIdentity,
        progress: ArtifactHttpDownloadProgress,
    ) {
        let mut owner = self
            .artifact_downloads
            .lock()
            .expect("artifact download owner poisoned");
        let Some(operation) = owner
            .operations
            .get_mut(&identity.operation_id)
            .filter(|o| o.publication.identity == *identity && o.publication.state.is_active())
        else {
            return;
        };
        if self.is_stopped() {
            return;
        }
        let mut next = (*operation.publication).clone();
        next.state = ArtifactDownloadState::Downloading;
        next.downloaded_bytes = progress.downloaded_bytes;
        next.total_bytes = progress.total_bytes;
        next.resumed_from_bytes = progress.resumed_from_bytes;
        if *operation.publication != next {
            let thread_id = next.target.thread_id.clone();
            operation.publication = Arc::new(next);
            drop(owner);
            self.publish_artifact_downloads(thread_id.as_deref());
        }
    }

    fn finish_artifact_download(
        &self,
        identity: &ArtifactDownloadIdentity,
        state: ArtifactDownloadState,
        error_code: Option<String>,
    ) -> bool {
        if state.is_active() || (self.is_stopped() && state != ArtifactDownloadState::Cancelled) {
            return false;
        }
        let mut owner = self
            .artifact_downloads
            .lock()
            .expect("artifact download owner poisoned");
        let Some(operation) = owner
            .operations
            .get_mut(&identity.operation_id)
            .filter(|o| o.publication.identity == *identity && o.publication.state.is_active())
        else {
            return false;
        };
        let mut next = (*operation.publication).clone();
        next.state = state;
        next.error_code = error_code;
        operation.publication = Arc::new(next);
        if state == ArtifactDownloadState::Cancelled {
            operation.cancellation.cancel();
        }
        let thread_id = operation.publication.target.thread_id.clone();
        drop(owner);
        self.publish_artifact_downloads(thread_id.as_deref());
        true
    }

    pub fn cancel_artifact_download(&self, identity: &ArtifactDownloadIdentity) -> bool {
        self.finish_artifact_download(
            identity,
            ArtifactDownloadState::Cancelled,
            Some("cancelled".into()),
        )
    }

    pub fn cancel_artifact_downloads(&self, thread_id: Option<&str>) {
        let mut owner = self
            .artifact_downloads
            .lock()
            .expect("artifact download owner poisoned");
        let mut changed_threads = std::collections::HashSet::new();
        for operation in owner.operations.values_mut().filter(|o| {
            thread_id.is_none_or(|id| o.publication.target.thread_id.as_deref() == Some(id))
        }) {
            if operation.publication.state.is_active() {
                operation.cancellation.cancel();
                let mut next = (*operation.publication).clone();
                next.state = ArtifactDownloadState::Cancelled;
                next.error_code = Some("cancelled".into());
                changed_threads.extend(next.target.thread_id.clone());
                operation.publication = Arc::new(next);
            }
        }
        drop(owner);
        for thread_id in changed_threads {
            self.publish_artifact_downloads(Some(&thread_id));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn target(thread: &str) -> ArtifactDownloadTarget {
        ArtifactDownloadTarget {
            thread_id: Some(thread.into()),
            workspace_id: "workspace".into(),
            artifact_id: "artifact".into(),
            version_id: Some("version".into()),
        }
    }

    #[test]
    fn equal_progress_and_late_cancel_or_completion_cannot_change_a_new_generation() {
        let core = Arc::new(ClientCore::new());
        let old = core
            .begin_artifact_download("download".into(), target("a"))
            .unwrap();
        let progress = ArtifactHttpDownloadProgress {
            downloaded_bytes: 7,
            total_bytes: 10,
            resumed_from_bytes: 3,
        };
        old.update_progress(progress.clone());
        let before = core.artifact_download_snapshot("download").unwrap();
        old.update_progress(progress.clone());
        assert!(Arc::ptr_eq(
            &before,
            &core.artifact_download_snapshot("download").unwrap()
        ));
        assert!(core.cancel_artifact_download(old.identity()));
        let new = core
            .begin_artifact_download("download".into(), target("b"))
            .unwrap();
        assert!(new.identity().generation > old.identity().generation);
        let before = core.artifact_download_snapshot("download").unwrap();
        old.update_progress(progress);
        assert!(!old.finish(ArtifactDownloadState::Completed, None));
        assert!(!core.cancel_artifact_download(old.identity()));
        drop(old);
        assert!(Arc::ptr_eq(
            &before,
            &core.artifact_download_snapshot("download").unwrap()
        ));
        assert!(!new.cancellation().is_cancelled());
        drop(new);
        assert_eq!(
            core.artifact_download_snapshot("download").unwrap().state,
            ArtifactDownloadState::Cancelled
        );
    }

    #[test]
    fn thread_retirement_and_original_operation_bounds_are_preserved() {
        let core = Arc::new(ClientCore::new());
        let active = (0..MAX_ACTIVE_DOWNLOAD_OPERATIONS)
            .map(|i| {
                core.begin_artifact_download(
                    format!("active-{i}"),
                    target(if i == 0 { "a" } else { "b" }),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            core.begin_artifact_download("overflow".into(), target("a")),
            Err(ArtifactDownloadOperationError::Capacity)
        ));
        core.remove_thread_store("a");
        assert!(active[0].cancellation().is_cancelled());
        assert!(!active[1].cancellation().is_cancelled());
        for operation in &active {
            operation.finish(ArtifactDownloadState::Completed, None);
        }
        for i in 0..(MAX_TRACKED_DOWNLOAD_OPERATIONS + 10) {
            let operation = core
                .begin_artifact_download(format!("terminal-{i}"), target("a"))
                .unwrap();
            operation.finish(ArtifactDownloadState::Completed, None);
        }
        assert!(
            core.artifact_downloads.lock().unwrap().operations.len()
                <= MAX_TRACKED_DOWNLOAD_OPERATIONS
        );
    }
}
