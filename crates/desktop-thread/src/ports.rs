use gpui_kit::{App, Task};
use pioneer_client::{
    ClientResult,
    composer::store::{ComposerOperationCompletion, ComposerOperationPlan},
    platform::{ClientFileSystem, ClientPath},
};
use std::path::Path;

/// Identity of one native presentation request in a mounted thread root.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ThreadPresentationOperation {
    thread_id: String,
    mount: u64,
    generation: u64,
}
impl ThreadPresentationOperation {
    pub(crate) fn new(thread_id: String, mount: u64, generation: u64) -> Self {
        Self {
            thread_id,
            mount,
            generation,
        }
    }
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }
    pub fn mount(&self) -> u64 {
        self.mount
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

#[derive(Clone)]
pub struct ThreadFileOpenerChoice {
    id: String,
    label: String,
    logo_path: Option<String>,
}
impl ThreadFileOpenerChoice {
    pub fn new(id: String, label: String, logo_path: Option<String>) -> Self {
        Self {
            id,
            label,
            logo_path,
        }
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn label(&self) -> &str {
        &self.label
    }
    pub fn logo_path(&self) -> Option<&str> {
        self.logo_path.as_deref()
    }
}

pub struct ThreadFileOpenerPresentation {
    choices: Vec<ThreadFileOpenerChoice>,
    selected: ThreadFileOpenerChoice,
    workspace: ThreadFileOpenerChoice,
    thread_override: Option<String>,
}
impl ThreadFileOpenerPresentation {
    pub fn new(
        choices: Vec<ThreadFileOpenerChoice>,
        selected: ThreadFileOpenerChoice,
        workspace: ThreadFileOpenerChoice,
        thread_override: Option<String>,
    ) -> Self {
        Self {
            choices,
            selected,
            workspace,
            thread_override,
        }
    }
    pub fn choices(&self) -> &[ThreadFileOpenerChoice] {
        &self.choices
    }
    pub fn selected(&self) -> &ThreadFileOpenerChoice {
        &self.selected
    }
    pub fn workspace(&self) -> &ThreadFileOpenerChoice {
        &self.workspace
    }
    pub fn thread_override(&self) -> Option<&str> {
        self.thread_override.as_deref()
    }
}

#[derive(Clone, Debug)]
pub struct ThreadFileOpenRequest {
    operation: ThreadPresentationOperation,
    opener_id: String,
    path: ClientPath,
    line: Option<u32>,
    column: Option<u32>,
}
impl ThreadFileOpenRequest {
    pub(crate) fn new(
        operation: ThreadPresentationOperation,
        opener_id: String,
        path: ClientPath,
        line: Option<u32>,
        column: Option<u32>,
    ) -> Self {
        Self {
            operation,
            opener_id,
            path,
            line,
            column,
        }
    }
    pub fn operation(&self) -> &ThreadPresentationOperation {
        &self.operation
    }
    pub fn opener_id(&self) -> &str {
        &self.opener_id
    }
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }
    pub fn line(&self) -> Option<u32> {
        self.line
    }
    pub fn column(&self) -> Option<u32> {
        self.column
    }
}

/// Desktop file effects. The adapter owns native handles; Client owns upload,
/// draft and artifact operation plans and accepts their matching completions.
pub trait ThreadFilePort: ClientFileSystem {
    fn select_attachments(
        &self,
        presentation: ThreadPresentationOperation,
        plan: ComposerOperationPlan,
        cx: &mut App,
    ) -> Task<ComposerOperationCompletion>;
    fn select_download_destination(
        &self,
        presentation: ThreadPresentationOperation,
        identity: pioneer_client::artifacts::workflow::ArtifactActionIdentity,
        cx: &mut App,
    ) -> Task<ClientResult<Option<ClientPath>>>;
    fn reveal_artifact(
        &self,
        presentation: ThreadPresentationOperation,
        plan: pioneer_client::artifacts::local_presentation::ArtifactLocalPresentationPlan,
        cx: &mut App,
    ) -> Task<ClientResult<()>>;
    fn file_openers(&self, thread_id: &str, cx: &App) -> ThreadFileOpenerPresentation;
    fn select_file_opener(
        &self,
        operation: &ThreadPresentationOperation,
        opener_id: Option<&str>,
        cx: &mut App,
    ) -> ClientResult<()>;
    fn open_file(&self, request: &ThreadFileOpenRequest) -> ClientResult<()>;
    fn preview_renderer(
        &self,
    ) -> std::sync::Arc<
        dyn pioneer_client::artifacts::preview::ArtifactPreviewImageRenderer + Send + Sync,
    >;
    fn runtime_root(&self) -> ClientPath;
    fn retire_mount(&self, thread_id: &str, mount: u64);
}

#[derive(Clone, Debug)]
pub struct ThreadExternalNavigationRequest {
    operation: ThreadPresentationOperation,
    url: String,
}
impl ThreadExternalNavigationRequest {
    pub(crate) fn new(operation: ThreadPresentationOperation, url: String) -> Self {
        Self { operation, url }
    }
    pub fn operation(&self) -> &ThreadPresentationOperation {
        &self.operation
    }
    pub fn url(&self) -> &str {
        &self.url
    }
}
pub trait ThreadExternalNavigationPort: Send + Sync {
    fn open_url(&self, request: &ThreadExternalNavigationRequest) -> ClientResult<()>;
    fn retire_mount(&self, thread_id: &str, mount: u64);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThreadAudioErrorKind {
    PermissionDenied,
    NoInputDevice,
    DeviceBusy,
    UnsupportedFormat,
    DeviceInterrupted,
    AlreadyCapturing,
    GatewaySession,
    GatewayChunk,
    GatewayFinalize,
    GatewayCancel,
    NoSpeech,
}

/// Native presentation error. Domain failures are completed separately against
/// the Client operation identity; this text preserves the adapter's localized copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadAudioError {
    kind: ThreadAudioErrorKind,
    message: String,
}
impl ThreadAudioError {
    pub fn new(kind: ThreadAudioErrorKind, message: String) -> Self {
        Self { kind, message }
    }
    pub fn kind(&self) -> ThreadAudioErrorKind {
        self.kind
    }
    pub fn message(&self) -> &str {
        &self.message
    }
}
impl std::fmt::Display for ThreadAudioError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for ThreadAudioError {}

#[derive(Clone, Debug)]
pub struct ThreadAudioRequest {
    presentation: ThreadPresentationOperation,
    plan: ComposerOperationPlan,
}
impl ThreadAudioRequest {
    pub(crate) fn new(
        presentation: ThreadPresentationOperation,
        plan: ComposerOperationPlan,
    ) -> Self {
        assert_eq!(presentation.thread_id(), plan.identity.thread_id);
        Self { presentation, plan }
    }
    pub fn presentation(&self) -> &ThreadPresentationOperation {
        &self.presentation
    }
    pub fn plan(&self) -> &ComposerOperationPlan {
        &self.plan
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThreadAudioCompletion {
    CaptureReady,
    RecordingStopped,
    Finalized,
    Cancelled,
}

/// The adapter retains the microphone, stream, chunk sink and native tasks.
/// A feature keeps only immutable plans and its pointer/focus presentation.
pub trait ThreadAudioPort: Send + Sync {
    fn start_capture(
        &self,
        request: ThreadAudioRequest,
        cx: &mut App,
    ) -> Task<Result<ThreadAudioCompletion, ThreadAudioError>>;
    fn stop_recording(
        &self,
        request: &ThreadAudioRequest,
    ) -> Result<ThreadAudioCompletion, ThreadAudioError>;
    fn finalize_capture(
        &self,
        request: ThreadAudioRequest,
        prepared: pioneer_client::composer::turn_prepare::PreparedVoiceComposerSnapshot,
        cx: &mut App,
    ) -> Task<Result<ThreadAudioCompletion, ThreadAudioError>>;
    fn cancel_capture(&self, request: &ThreadAudioRequest);
    fn retire_mount(&self, thread_id: &str, mount: u64);
}
