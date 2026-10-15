//! Native file presentation for a mounted thread capability.
pub(crate) mod artifact_opener;
mod filesystem;
mod preview;
pub(crate) use filesystem::DesktopClientFileSystem;
pub(crate) use preview::DesktopArtifactPreviewImageRenderer;

use crate::{
    file_opener::*,
    settings::{self, FileOpenerThreadScope, FileOpenerWorkspaceScope},
};
use gpui_kit::{App, AppContext, PathPromptOptions, Task};
use pioneer_client::{
    ClientError, ClientResult,
    composer::{
        attachments::append_composer_attachment_paths,
        store::{ComposerOperationCompletion, ComposerOperationPlan},
    },
    core::{ClientCore, ClientScope},
    gateway::identity_authorization::IdentityAuthorizationPublication,
    platform::{ClientFileMetadata, ClientFileReader, ClientFileSystem, ClientPath},
};
use pioneer_desktop_thread::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Default)]
struct FilePresentations(Mutex<Vec<(ThreadPresentationOperation, Arc<AtomicBool>)>>);
impl FilePresentations {
    fn begin(&self, operation: ThreadPresentationOperation) -> Arc<AtomicBool> {
        let mut slots = self.0.lock().expect("file presentations poisoned");
        slots.retain(|(previous, cancelled)| {
            if previous.thread_id() == operation.thread_id()
                && previous.mount() == operation.mount()
            {
                cancelled.store(true, Ordering::Release);
                false
            } else {
                true
            }
        });
        let cancelled = Arc::new(AtomicBool::new(false));
        slots.push((operation, cancelled.clone()));
        cancelled
    }
    fn retire(&self, thread: &str, mount: u64) {
        self.0
            .lock()
            .expect("file presentations poisoned")
            .retain(|(operation, cancelled)| {
                if operation.thread_id() == thread && operation.mount() == mount {
                    cancelled.store(true, Ordering::Release);
                    false
                } else {
                    true
                }
            });
    }
    fn finish(&self, operation: &ThreadPresentationOperation) {
        self.0
            .lock()
            .expect("file presentations poisoned")
            .retain(|(current, _)| current != operation);
    }
}
impl Drop for FilePresentations {
    fn drop(&mut self) {
        for (_, cancelled) in self
            .0
            .get_mut()
            .expect("file presentations poisoned")
            .drain(..)
        {
            cancelled.store(true, Ordering::Release);
        }
    }
}

pub(crate) struct DesktopThreadFilePort {
    client: Arc<ClientCore>,
    runtime_root: ClientPath,
    presentations: Arc<FilePresentations>,
}
impl DesktopThreadFilePort {
    pub(crate) fn new(client: Arc<ClientCore>, runtime_root: ClientPath) -> Self {
        Self {
            client,
            runtime_root,
            presentations: Arc::default(),
        }
    }
    fn scope(&self, thread: &str) -> Option<FileOpenerThreadScope> {
        let publication = self
            .client
            .snapshot(&ClientScope::Administration { workspace_id: None })?;
        let identity = publication
            .snapshot()
            .payload::<IdentityAuthorizationPublication>()?;
        let coordinator = self.client.thread_coordinator_snapshot(thread)?;
        Some(FileOpenerThreadScope {
            workspace: FileOpenerWorkspaceScope {
                principal_id: identity.current_auth.as_ref()?.principal.id.as_str().into(),
                gateway_id: identity.endpoint_id.clone()?,
                workspace_id: coordinator.workspace_id.clone(),
            },
            thread_id: thread.into(),
        })
    }
}
impl ClientFileSystem for DesktopThreadFilePort {
    fn read_file(&self, path: &ClientPath) -> ClientResult<Vec<u8>> {
        DesktopClientFileSystem.read_file(path)
    }
    fn metadata(&self, path: &ClientPath) -> ClientResult<ClientFileMetadata> {
        DesktopClientFileSystem.metadata(path)
    }
    fn write_cache_file(&self, key: &str, bytes: &[u8]) -> ClientResult<ClientPath> {
        DesktopClientFileSystem.write_cache_file(key, bytes)
    }
    fn open_read(&self, path: &ClientPath) -> ClientResult<Box<dyn ClientFileReader>> {
        DesktopClientFileSystem.open_read(path)
    }
}
fn opener_id(opener: FileOpenerId) -> String {
    serde_json::to_value(opener)
        .expect("file opener is serializable")
        .as_str()
        .expect("file opener ID")
        .into()
}
fn opener_choice(opener: FileOpenerId) -> ThreadFileOpenerChoice {
    ThreadFileOpenerChoice::new(
        opener_id(opener),
        opener.label().into(),
        opener.logo_path().map(str::to_owned),
    )
}
fn parse_opener(id: &str) -> ClientResult<FileOpenerId> {
    serde_json::from_value(serde_json::Value::String(id.into()))
        .map_err(|error| ClientError::platform(error.to_string()))
}
impl ThreadFilePort for DesktopThreadFilePort {
    fn select_attachments(
        &self,
        presentation: ThreadPresentationOperation,
        plan: ComposerOperationPlan,
        cx: &mut App,
    ) -> Task<ComposerOperationCompletion> {
        if presentation.thread_id() != plan.identity.thread_id
            || self.client.composer_operation_plan(&plan.identity).as_ref() != Some(&plan)
        {
            return Task::ready(ComposerOperationCompletion::Cancelled);
        }
        let cancelled = self.presentations.begin(presentation.clone());
        let slots = Arc::downgrade(&self.presentations);
        let client = Arc::downgrade(&self.client);
        let selection = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: true,
            prompt: None,
        });
        cx.spawn(async move |_| {
            let selected = selection.await;
            if let Some(slots) = slots.upgrade() {
                slots.finish(&presentation);
            }
            if cancelled.load(Ordering::Acquire)
                || !client.upgrade().is_some_and(|client| {
                    client.composer_operation_plan(&plan.identity).as_ref() == Some(&plan)
                })
            {
                return ComposerOperationCompletion::Cancelled;
            }
            match selected {
                Ok(Ok(Some(paths))) => {
                    let mut attachments = Vec::new();
                    append_composer_attachment_paths(&mut attachments, paths);
                    ComposerOperationCompletion::FilesSelected { attachments }
                }
                _ => ComposerOperationCompletion::Cancelled,
            }
        })
    }
    fn select_download_destination(
        &self,
        presentation: ThreadPresentationOperation,
        identity: pioneer_client::artifacts::workflow::ArtifactActionIdentity,
        cx: &mut App,
    ) -> Task<ClientResult<Option<ClientPath>>> {
        use pioneer_client::artifacts::workflow::{ArtifactActionKind, ArtifactActionState};
        if presentation.thread_id() != identity.thread_id
            || !self
                .client
                .artifact_action_snapshot(&identity)
                .is_some_and(|action| {
                    action.action == ArtifactActionKind::Download
                        && action.state == ArtifactActionState::Preparing
                })
        {
            return Task::ready(Ok(None));
        }
        let cancelled = self.presentations.begin(presentation.clone());
        let slots = Arc::downgrade(&self.presentations);
        let selection = cx.prompt_for_paths(PathPromptOptions {
            files: false,
            directories: true,
            multiple: false,
            prompt: None,
        });
        cx.spawn(async move |_| {
            let result = selection.await;
            if let Some(slots) = slots.upgrade() {
                slots.finish(&presentation);
            }
            if cancelled.load(Ordering::Acquire) {
                return Ok(None);
            }
            match result {
                Ok(Ok(paths)) => Ok(paths.and_then(|mut paths| paths.pop()).map(ClientPath::new)),
                Ok(Err(error)) => Err(ClientError::platform(format!("{error:#}"))),
                Err(_) => Ok(None),
            }
        })
    }
    fn reveal_artifact(
        &self,
        presentation: ThreadPresentationOperation,
        plan: pioneer_client::artifacts::local_presentation::ArtifactLocalPresentationPlan,
        cx: &mut App,
    ) -> Task<ClientResult<()>> {
        let cancelled = self.presentations.begin(presentation.clone());
        let presentations = Arc::downgrade(&self.presentations);
        let client = self.client.clone();
        cx.background_spawn(async move {
            use pioneer_client::artifacts::workflow::ArtifactActionState;
            let matches = || {
                !cancelled.load(Ordering::Acquire)
                    && presentation.thread_id() == plan.identity().thread_id
                    && client
                        .artifact_action_snapshot(plan.identity())
                        .is_some_and(|action| action.state == ArtifactActionState::Presenting)
            };
            let result = (|| {
                if !matches() {
                    return Err(ClientError::platform("artifact presentation cancelled"));
                }
                if !pioneer_client::artifacts::actions::existing_local_file_is_verified(
                    plan.file(),
                    plan.artifact(),
                )
                .unwrap_or(false)
                {
                    return Err(ClientError::platform("local artifact integrity failed"));
                }
                if !matches() {
                    return Err(ClientError::platform("artifact presentation cancelled"));
                }
                artifact_opener::spawn_reveal_file(&plan.file().path)
                    .map_err(|error| ClientError::platform(format!("{error:#}")))
            })();
            if let Some(presentations) = presentations.upgrade() {
                presentations.finish(&presentation);
            }
            result
        })
    }
    fn file_openers(&self, thread: &str, cx: &App) -> ThreadFileOpenerPresentation {
        let scope = self.scope(thread);
        let workspace = scope
            .as_ref()
            .map(|scope| {
                available_or_file_manager(settings::workspace_file_opener(cx, &scope.workspace))
            })
            .unwrap_or_default();
        let selected = scope
            .as_ref()
            .and_then(|scope| settings::thread_file_opener_override(cx, scope))
            .filter(|opener| is_file_opener_available(*opener));
        ThreadFileOpenerPresentation::new(
            available_file_openers()
                .iter()
                .map(|opener| opener_choice(opener.id))
                .collect(),
            opener_choice(selected.unwrap_or(workspace)),
            opener_choice(workspace),
            selected.map(opener_id),
        )
    }
    fn select_file_opener(
        &self,
        operation: &ThreadPresentationOperation,
        opener: Option<&str>,
        cx: &mut App,
    ) -> ClientResult<()> {
        let Some(scope) = self.scope(operation.thread_id()) else {
            return Ok(());
        };
        settings::set_thread_file_opener_override(cx, &scope, opener.map(parse_opener).transpose()?)
            .map_err(|error| ClientError::platform(format!("{error:#}")))
    }
    fn open_file(&self, request: &ThreadFileOpenRequest) -> ClientResult<()> {
        open_local_file(
            parse_opener(request.opener_id())?,
            &LocalFileTarget::new(request.path().into(), request.line(), request.column()),
        )
        .map_err(|error| ClientError::platform(format!("{error:#}")))
    }
    fn preview_renderer(
        &self,
    ) -> Arc<dyn pioneer_client::artifacts::preview::ArtifactPreviewImageRenderer + Send + Sync>
    {
        Arc::new(DesktopArtifactPreviewImageRenderer)
    }
    fn runtime_root(&self) -> ClientPath {
        self.runtime_root.clone()
    }
    fn retire_mount(&self, thread: &str, mount: u64) {
        self.presentations.retire(thread, mount);
    }
}

pub(crate) struct DesktopThreadExternalNavigationPort;
impl ThreadExternalNavigationPort for DesktopThreadExternalNavigationPort {
    fn open_url(&self, request: &ThreadExternalNavigationRequest) -> ClientResult<()> {
        webbrowser::open(request.url()).map_err(|error| ClientError::platform(error.to_string()))
    }
    fn retire_mount(&self, _: &str, _: u64) {}
}
