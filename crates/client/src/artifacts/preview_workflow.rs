//! Authenticated thumbnail work belongs to the thread's artifact owner.

use super::preview::{
    ArtifactHttpPreviewService, ArtifactPreviewImagePaths, ArtifactPreviewImageRenderer,
    thumbnail_preview, write_artifact_preview_cache_files,
};
use crate::core::ClientCore;
use pioneer_protocol::ArtifactRef;
use std::{
    path::PathBuf,
    sync::{Arc, mpsc},
};
use tokio_util::sync::CancellationToken;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArtifactPreviewRequestState {
    Loading,
    Ready { paths: ArtifactPreviewImagePaths },
    Failed,
    Cancelled,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize)]
pub struct ArtifactPreviewPublication {
    pub artifact: ArtifactRef,
    pub generation: u64,
    pub state: ArtifactPreviewRequestState,
}

impl ArtifactPreviewPublication {
    pub fn paths(&self, artifact: &ArtifactRef) -> Option<&ArtifactPreviewImagePaths> {
        if !same_preview(&self.artifact, artifact) {
            return None;
        }
        match &self.state {
            ArtifactPreviewRequestState::Ready { paths } => Some(paths),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub(super) struct ArtifactPreviewRequest {
    thread_id: String,
    workspace_id: String,
    generation: u64,
    auth_ticket: (u64, Option<u64>),
    thread_incarnation: Option<crate::threads::registry::ThreadOperationToken>,
    artifact: ArtifactRef,
    runtime_home: PathBuf,
    renderer: Arc<dyn ArtifactPreviewImageRenderer + Send + Sync>,
    pub(super) cancellation: CancellationToken,
}

fn same_preview(a: &ArtifactRef, b: &ArtifactRef) -> bool {
    a.artifact_id == b.artifact_id && a.version_id == b.version_id && a.preview == b.preview
}

impl ClientCore {
    /// Presentation supplies immutable source metadata and the existing byte encoder.
    /// Duplicate observations and persistent failures do not enqueue more work.
    pub fn observe_artifact_preview(
        &self,
        thread_id: &str,
        workspace_id: &str,
        artifact: &ArtifactRef,
        runtime_home: PathBuf,
        renderer: Arc<dyn ArtifactPreviewImageRenderer + Send + Sync>,
    ) {
        if self.is_stopped() || thumbnail_preview(artifact).is_none() {
            return;
        }
        let ticket = self.current_auth_ticket();
        if ticket.1.is_none()
            || !self
                .authorization_snapshot(Some(workspace_id), Some(thread_id))
                .and_then(|p| p.thread.clone())
                .is_some_and(|t| t.capabilities.can_read_artifacts)
        {
            return;
        }
        self.enqueue_artifact_preview(
            thread_id,
            workspace_id,
            artifact,
            runtime_home,
            renderer,
            ticket,
        );
    }

    fn enqueue_artifact_preview(
        &self,
        thread_id: &str,
        workspace_id: &str,
        artifact: &ArtifactRef,
        runtime_home: PathBuf,
        renderer: Arc<dyn ArtifactPreviewImageRenderer + Send + Sync>,
        auth_ticket: (u64, Option<u64>),
    ) {
        let mut owner = self.artifact_store.lock().expect("artifact owner poisoned");
        if self.is_stopped() || owner.suspended.contains(thread_id) {
            return;
        }
        let Some(current) = owner
            .publications
            .get(thread_id)
            .filter(|p| p.workspace_id == workspace_id)
            .cloned()
        else {
            return;
        };
        if current.previews.iter().any(|p| {
            same_preview(&p.artifact, artifact)
                && match &p.state {
                    ArtifactPreviewRequestState::Cancelled => false,
                    ArtifactPreviewRequestState::Ready { paths } => {
                        paths.square_path.is_file() && paths.detail_path.is_file()
                    }
                    _ => true,
                }
        }) {
            return;
        }
        let generation = owner.next_generation();
        let request = ArtifactPreviewRequest {
            thread_id: thread_id.into(),
            workspace_id: workspace_id.into(),
            artifact: artifact.clone(),
            generation,
            auth_ticket,
            thread_incarnation: self.thread_operation_token(thread_id),
            runtime_home,
            renderer,
            cancellation: CancellationToken::new(),
        };
        // Replaced projections cannot complete into this version's presentation.
        let replaced = owner
            .preview_requests
            .iter()
            .filter(|(_, r)| {
                r.thread_id == thread_id
                    && r.artifact.artifact_id == artifact.artifact_id
                    && r.artifact.version_id == artifact.version_id
            })
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in replaced {
            if let Some(old) = owner.preview_requests.remove(&id) {
                old.cancellation.cancel();
            }
        }
        let mut next = (*current).clone();
        next.previews.retain(|p| {
            p.artifact.artifact_id != artifact.artifact_id
                || p.artifact.version_id != artifact.version_id
        });
        next.previews.push(ArtifactPreviewPublication {
            artifact: artifact.clone(),
            generation,
            state: ArtifactPreviewRequestState::Loading,
        });
        owner.preview_requests.insert(generation, request.clone());
        self.publish_artifact(&mut owner, next);
        if owner
            .preview_sender
            .as_ref()
            .is_none_or(|sender| sender.try_send(request.clone()).is_err())
        {
            drop(owner);
            self.complete_artifact_preview(
                &request,
                Err(anyhow::anyhow!("artifact preview worker unavailable")),
            );
        }
    }

    fn artifact_preview_matches(&self, request: &ArtifactPreviewRequest) -> bool {
        !self.is_stopped()
            && !request.cancellation.is_cancelled()
            && self.current_auth_ticket() == request.auth_ticket
            && self.thread_operation_token(&request.thread_id) == request.thread_incarnation
            && self
                .artifact_store
                .lock()
                .expect("artifact owner poisoned")
                .preview_requests
                .contains_key(&request.generation)
    }

    fn complete_artifact_preview(
        &self,
        request: &ArtifactPreviewRequest,
        result: anyhow::Result<ArtifactPreviewImagePaths>,
    ) {
        let mut owner = self.artifact_store.lock().expect("artifact owner poisoned");
        if self.is_stopped()
            || request.cancellation.is_cancelled()
            || self.current_auth_ticket() != request.auth_ticket
            || self.thread_operation_token(&request.thread_id) != request.thread_incarnation
            || !owner.preview_requests.contains_key(&request.generation)
        {
            return;
        }
        owner.preview_requests.remove(&request.generation);
        let Some(current) = owner
            .publications
            .get(&request.thread_id)
            .filter(|p| p.workspace_id == request.workspace_id)
            .cloned()
        else {
            return;
        };
        let mut next = (*current).clone();
        let Some(preview) = next.previews.iter_mut().find(|p| {
            p.generation == request.generation && same_preview(&p.artifact, &request.artifact)
        }) else {
            return;
        };
        preview.state = match result {
            Ok(paths) => ArtifactPreviewRequestState::Ready { paths },
            Err(_) => ArtifactPreviewRequestState::Failed,
        };
        self.publish_artifact(&mut owner, next);
    }

    pub(crate) fn cancel_artifact_previews(&self, thread_id: Option<&str>, clear: bool) {
        let mut owner = self.artifact_store.lock().expect("artifact owner poisoned");
        owner.preview_requests.retain(|_, request| {
            if thread_id.is_none_or(|id| id == request.thread_id) {
                request.cancellation.cancel();
                false
            } else {
                true
            }
        });
        let entries = owner
            .publications
            .values()
            .filter(|p| thread_id.is_none_or(|id| id == p.thread_id))
            .cloned()
            .collect::<Vec<_>>();
        for current in entries {
            let mut next = (*current).clone();
            let changed = if clear {
                let changed = !next.previews.is_empty();
                next.previews.clear();
                changed
            } else {
                let mut changed = false;
                for p in &mut next.previews {
                    if p.state == ArtifactPreviewRequestState::Loading {
                        p.state = ArtifactPreviewRequestState::Cancelled;
                        changed = true;
                    }
                }
                changed
            };
            if changed {
                self.publish_artifact(&mut owner, next);
            }
        }
    }

    pub(crate) fn start_artifact_preview_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<ArtifactPreviewRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-artifact-previews".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("artifact preview runtime unavailable");
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.artifact_preview_matches(&request) {
                        continue;
                    }
                    let session = super::access::artifact_http_session(
                        &core.compatibility_runtime().ws_command_sender(),
                    );
                    drop(core);
                    let result = (|| {
                        let service = ArtifactHttpPreviewService::new(session?);
                        let data = runtime.block_on(service.fetch_thumbnail(
                            &request.workspace_id,
                            &request.artifact,
                            request.cancellation.clone(),
                        ))?;
                        anyhow::ensure!(
                            weak.upgrade()
                                .is_some_and(|core| core.artifact_preview_matches(&request)),
                            "artifact preview cancelled"
                        );
                        write_artifact_preview_cache_files(
                            request.renderer.as_ref(),
                            &request.runtime_home,
                            &request.workspace_id,
                            &request.artifact,
                            &data,
                        )
                    })();
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    core.complete_artifact_preview(&request, result);
                }
            })
            .expect("artifact preview worker unavailable");
        let mut owner = self.artifact_store.lock().expect("artifact owner poisoned");
        owner.preview_sender = Some(sender);
        owner.preview_task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        artifacts::store::{ArtifactPublication, ArtifactReadState},
        core::{ClientDemand, ClientScope},
    };

    struct Renderer;
    impl ArtifactPreviewImageRenderer for Renderer {
        fn write_preview_variants(
            &self,
            _: &[u8],
            _: &[super::super::preview::ArtifactPreviewVariantTarget],
        ) -> anyhow::Result<()> {
            panic!("state tests never perform image or native I/O")
        }
    }
    fn setup() -> (Arc<ClientCore>, mpsc::Receiver<ArtifactPreviewRequest>) {
        let core = Arc::new(ClientCore::new());
        let (sender, receiver) = mpsc::sync_channel(8);
        {
            let mut owner = core.artifact_store.lock().unwrap();
            owner.preview_sender = Some(sender);
            for id in ["a", "b"] {
                core.publish_artifact(
                    &mut owner,
                    ArtifactPublication {
                        thread_id: id.into(),
                        workspace_id: "workspace".into(),
                        revision: 0,
                        generation: 0,
                        items: vec![],
                        request: ArtifactReadState::Ready,
                        downloads: vec![],
                        actions: vec![],
                        previews: vec![],
                    },
                );
            }
        }
        (core, receiver)
    }
    fn artifact(sha: &str) -> ArtifactRef {
        use pioneer_protocol::*;
        ArtifactRef {
            artifact_id: "artifact".into(),
            version_id: Some("version".into()),
            display_name: "image".into(),
            kind: ArtifactKind::Image,
            mime_type: Some("image/png".into()),
            size_bytes: Some(4),
            sha256: None,
            status: ArtifactStatus::Ready,
            preview: Some(ArtifactPreviewRef {
                artifact_id: "artifact".into(),
                version_id: "version".into(),
                projection_kind: ArtifactProjectionKind::Thumbnail,
                status: ArtifactProjectionStatus::Ready,
                blob_id: Some("blob".into()),
                mime_type: Some("image/png".into()),
                size_bytes: Some(4),
                sha256: Some(sha.into()),
            }),
        }
    }
    fn enqueue(core: &ClientCore, id: &str, artifact: &ArtifactRef) {
        core.enqueue_artifact_preview(
            id,
            "workspace",
            artifact,
            PathBuf::from("synthetic"),
            Arc::new(Renderer),
            core.current_auth_ticket(),
        );
    }
    #[test]
    fn replacement_and_duplicate_completions_do_not_overwrite_the_current_projection() {
        let (core, receiver) = setup();
        let old_artifact = artifact("old");
        enqueue(&core, "a", &old_artifact);
        let old = receiver.try_recv().unwrap();
        for _ in 0..10 {
            enqueue(&core, "a", &old_artifact);
        }
        assert!(receiver.try_recv().is_err());
        enqueue(&core, "a", &artifact("new"));
        let new = receiver.try_recv().unwrap();
        assert!(old.cancellation.is_cancelled());
        let pending = core.artifact_snapshot("a").unwrap();
        core.complete_artifact_preview(&old, Err(anyhow::anyhow!("late")));
        assert!(Arc::ptr_eq(&pending, &core.artifact_snapshot("a").unwrap()));
        let b = core.artifact_snapshot("b").unwrap();
        core.complete_artifact_preview(&new, Err(anyhow::anyhow!("failure")));
        let failed = core.artifact_snapshot("a").unwrap();
        assert_eq!(
            failed.previews[0].state,
            ArtifactPreviewRequestState::Failed
        );
        for _ in 0..10 {
            enqueue(&core, "a", &artifact("new"));
        }
        core.complete_artifact_preview(&new, Err(anyhow::anyhow!("duplicate")));
        assert!(receiver.try_recv().is_err());
        assert!(Arc::ptr_eq(&failed, &core.artifact_snapshot("a").unwrap()));
        assert!(Arc::ptr_eq(&b, &core.artifact_snapshot("b").unwrap()));
    }
    #[test]
    fn suspension_access_loss_and_owner_drop_cancel_exact_operations() {
        let (core, receiver) = setup();
        enqueue(&core, "a", &artifact("sha"));
        let a = receiver.try_recv().unwrap();
        enqueue(&core, "b", &artifact("sha"));
        let b = receiver.try_recv().unwrap();
        core.artifact_demand_changed(
            &ClientScope::Artifact {
                thread_id: "a".into(),
            },
            ClientDemand::Suspended,
        );
        assert!(a.cancellation.is_cancelled());
        assert!(!b.cancellation.is_cancelled());
        let cancelled = core.artifact_snapshot("a").unwrap();
        core.complete_artifact_preview(&a, Err(anyhow::anyhow!("late")));
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.artifact_snapshot("a").unwrap()
        ));
        core.invalidate_artifacts(Some("a"));
        assert!(core.artifact_snapshot("a").unwrap().previews.is_empty());
        let weak = Arc::downgrade(&core);
        drop(core);
        assert!(weak.upgrade().is_none());
        assert!(b.cancellation.is_cancelled());
    }
    #[test]
    fn ready_paths_are_scoped_to_the_exact_projection_and_workspace() {
        let (core, receiver) = setup();
        let reference = artifact("sha");
        enqueue(&core, "a", &reference);
        let request = receiver.try_recv().unwrap();
        let paths = ArtifactPreviewImagePaths {
            square_path: "square.png".into(),
            detail_path: "detail.png".into(),
        };
        core.complete_artifact_preview(&request, Ok(paths.clone()));
        let ready = core.artifact_snapshot("a").unwrap();
        assert_eq!(ready.previews[0].paths(&reference), Some(&paths));
        assert!(
            ready.previews[0]
                .paths(&artifact("different-sha"))
                .is_none()
        );
        core.enqueue_artifact_preview(
            "a",
            "another-workspace",
            &reference,
            "synthetic".into(),
            Arc::new(Renderer),
            core.current_auth_ticket(),
        );
        assert!(receiver.try_recv().is_err());
        core.complete_artifact_preview(&request, Err(anyhow::anyhow!("duplicate")));
        assert!(Arc::ptr_eq(&ready, &core.artifact_snapshot("a").unwrap()));
    }
}
