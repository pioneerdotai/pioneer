mod view;
use crate::{binding::ThreadBindings, ports::*, screen::GatewayConnectionState};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    artifacts::{
        ArtifactRef, ArtifactSummary,
        actions::ArtifactActionStatus as ThreadArtifactActionStatus,
        state::ThreadArtifactFilter,
        store::{ArtifactIntent, ArtifactPublication, ArtifactReadState},
        workflow::*,
    },
    composer::{state_machine::ComposerDomainAction, store::ComposerIntent},
    core::{ClientCore, ClientScope, ClientTransitionOutcome},
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::{collections::HashMap, path::PathBuf, sync::Arc};

pub(crate) struct ThreadArtifactsView {
    client: Arc<ClientCore>,
    thread_id: String,
    binding: Arc<ThreadBindings>,
    input: Option<Arc<ArtifactPublication>>,
    connection_state: GatewayConnectionState,
    selected_artifact_id: Option<String>,
    filter: ThreadArtifactFilter,
    visible: bool,
    files: Arc<dyn ThreadFilePort>,
    external: Arc<dyn ThreadExternalNavigationPort>,
    mount: u64,
    native_generation: u64,
    tasks: HashMap<ArtifactActionIdentity, Task<()>>,
    _binding_task: Task<()>,
}
impl ThreadArtifactsView {
    pub(crate) fn new(
        client: Arc<ClientCore>,
        thread_id: String,
        registrar: Arc<dyn ClientBindingRegistrar>,
        files: Arc<dyn ThreadFilePort>,
        external: Arc<dyn ThreadExternalNavigationPort>,
        mount: u64,
        cx: &mut App,
    ) -> Entity<Self> {
        let scopes = vec![
            ClientScope::Artifact {
                thread_id: thread_id.clone(),
            },
            ClientScope::Session,
            ClientScope::ThreadCapability {
                thread_id: thread_id.clone(),
            },
        ];
        let initial = scopes
            .iter()
            .filter_map(|scope| client.snapshot(scope))
            .collect();
        let binding = ThreadBindings::scoped(registrar, scopes, initial);
        cx.new(|cx: &mut Context<Self>| {
            let input = binding.clone();
            let mut changes = input.watch();
            let binding_task = cx.spawn(async move |view, cx| {
                while changes.changed().await.is_ok() {
                    if input.drain().is_empty() {
                        continue;
                    }
                    if view.update(cx, |view, cx| view.synchronize(cx)).is_err() {
                        break;
                    }
                }
            });
            let mut view = Self {
                client,
                thread_id,
                binding,
                input: None,
                connection_state: GatewayConnectionState::Disconnected,
                selected_artifact_id: None,
                filter: ThreadArtifactFilter::default(),
                visible: false,
                files,
                external,
                mount,
                native_generation: 0,
                tasks: HashMap::new(),
                _binding_task: binding_task,
            };
            view.synchronize(cx);
            view
        })
    }
    pub(crate) fn set_route_visible(&mut self, visible: bool, cx: &mut Context<Self>) {
        self.binding.set_active(visible);
        if !visible {
            for identity in self.tasks.keys() {
                self.client.cancel_artifact_action(identity);
            }
            self.tasks.clear();
            self.files.retire_mount(&self.thread_id, self.mount);
            self.external.retire_mount(&self.thread_id, self.mount);
        }
        cx.notify();
    }
    pub(crate) fn set_visible(&mut self, visible: bool, cx: &mut Context<Self>) {
        if self.visible == visible {
            return;
        }
        self.visible = visible;
        if visible {
            self.client.artifact_intent(ArtifactIntent::Observe {
                thread_id: self.thread_id.clone(),
            });
            self.observe_previews();
        }
        cx.notify();
    }
    fn synchronize(&mut self, cx: &mut Context<Self>) {
        self.input = self.client.artifact_snapshot(&self.thread_id);
        self.connection_state = self
            .binding
            .publication(&ClientScope::Session)
            .and_then(|p| {
                p.typed::<pioneer_client::gateway::session_controller::GatewaySessionPublication>()
            })
            .and_then(|p| {
                p.payload()
                    .status
                    .as_ref()
                    .map(|status| status.connection_state)
            })
            .unwrap_or(GatewayConnectionState::Disconnected);
        self.observe_previews();
        cx.notify();
    }
    fn observe_previews(&self) {
        if !self.visible {
            return;
        }
        for summary in self.visible_thread_artifacts() {
            self.client.observe_artifact_preview(
                &self.thread_id,
                &summary.workspace_id,
                &summary.artifact,
                self.files.runtime_root().as_path().into(),
                self.files.preview_renderer(),
            );
        }
    }
    pub(crate) fn select_thread_artifact(&mut self, artifact: String, cx: &mut Context<Self>) {
        if !self
            .active_thread_artifact_items()
            .iter()
            .any(|item| item.artifact.artifact_id == artifact)
        {
            self.client.artifact_intent(ArtifactIntent::Retry {
                thread_id: self.thread_id.clone(),
            });
        }
        self.selected_artifact_id = Some(artifact);
        cx.notify();
    }
    fn set_thread_artifact_filter(&mut self, filter: ThreadArtifactFilter, cx: &mut Context<Self>) {
        if self.filter == filter {
            return;
        }
        self.filter = filter;
        self.observe_previews();
        cx.notify();
    }
    fn active_thread_artifact_items(&self) -> &[ArtifactSummary] {
        self.input
            .as_ref()
            .map_or(&[], |input| input.items.as_slice())
    }
    fn visible_thread_artifacts(&self) -> Vec<&ArtifactSummary> {
        self.active_thread_artifact_items()
            .iter()
            .filter(|summary| {
                pioneer_client::artifacts::state::artifact_matches_filter(summary, self.filter)
            })
            .collect()
    }
    fn thread_artifact_loading(&self) -> bool {
        self.input
            .as_ref()
            .is_some_and(|input| input.request == ArtifactReadState::Loading)
    }
    fn thread_artifact_error(&self) -> Option<String> {
        match self.input.as_ref().map(|input| &input.request) {
            Some(ArtifactReadState::Failed { message }) => Some(message.clone()),
            _ => None,
        }
    }
    fn action(&self, artifact: &ArtifactRef) -> Option<&ArtifactActionPublication> {
        self.input.as_ref()?.actions.iter().find(|action| {
            action.identity.artifact_id == artifact.artifact_id
                && action.identity.version_id == artifact.version_id
        })
    }
    fn local_file(
        &self,
        artifact: &ArtifactRef,
    ) -> Option<&pioneer_client::artifacts::actions::ArtifactLocalFile> {
        self.action(artifact)?.local_file.as_ref()
    }
    fn thread_artifact_preview_path(
        &self,
        artifact: &ArtifactRef,
        detail: bool,
    ) -> Option<PathBuf> {
        let paths = self
            .input
            .as_ref()?
            .previews
            .iter()
            .find_map(|preview| preview.paths(artifact))?;
        let path = if detail {
            &paths.detail_path
        } else {
            &paths.square_path
        };
        path.is_file().then(|| path.clone())
    }
    fn thread_artifact_action_status(
        &self,
        artifact: &ArtifactRef,
    ) -> Option<ThreadArtifactActionStatus> {
        let action = self.action(artifact)?;
        match &action.state {
            ArtifactActionState::Completed => None,
            ArtifactActionState::Cancelled if action.action == ArtifactActionKind::Download => {
                Some(ThreadArtifactActionStatus::Failed(artifact_error(
                    "cancelled",
                )))
            }
            ArtifactActionState::Cancelled => None,
            ArtifactActionState::Failed { code } => {
                Some(ThreadArtifactActionStatus::Failed(artifact_error(code)))
            }
            state => Some(match action.action {
                ArtifactActionKind::Open | ArtifactActionKind::Share => {
                    ThreadArtifactActionStatus::Opening
                }
                ArtifactActionKind::Reveal => {
                    if *state == ArtifactActionState::Presenting {
                        ThreadArtifactActionStatus::Revealing
                    } else {
                        ThreadArtifactActionStatus::Verifying
                    }
                }
                ArtifactActionKind::Download => {
                    if *state == ArtifactActionState::Verifying {
                        return Some(ThreadArtifactActionStatus::Verifying);
                    }
                    match action.download.as_ref().and_then(|identity| {
                        self.input
                            .as_ref()?
                            .downloads
                            .iter()
                            .find(|download| download.identity == *identity)
                    }) {
                        Some(download) => ThreadArtifactActionStatus::Downloading {
                            downloaded_bytes: download.downloaded_bytes.min(download.total_bytes),
                            total_bytes: download.total_bytes,
                        },
                        None => ThreadArtifactActionStatus::Queued,
                    }
                }
            }),
        }
    }
    fn begin(
        &mut self,
        artifact: &ArtifactRef,
        kind: ArtifactActionKind,
    ) -> Option<ArtifactActionIdentity> {
        if self
            .client
            .begin_artifact_action(
                self.thread_id.clone(),
                artifact.artifact_id.clone(),
                artifact.version_id.clone(),
                kind,
            )
            .outcome()
            != ClientTransitionOutcome::Changed
        {
            return None;
        }
        self.input = self.client.artifact_snapshot(&self.thread_id);
        self.action(artifact).map(|action| action.identity.clone())
    }
    fn presentation(&mut self) -> ThreadPresentationOperation {
        self.native_generation = self
            .native_generation
            .checked_add(1)
            .expect("native presentation generation exhausted");
        ThreadPresentationOperation::new(self.thread_id.clone(), self.mount, self.native_generation)
    }
    fn open_thread_artifact(&mut self, summary: ArtifactSummary, cx: &mut Context<Self>) {
        let Some(identity) = self.begin(&summary.artifact, ArtifactActionKind::Open) else {
            return;
        };
        let client = self.client.clone();
        let effect = self.presentation();
        let operation = identity.clone();
        let task = cx.spawn(async move |view, cx| {
            let worker = client.clone();
            let captured = operation.clone();
            let result = cx
                .background_spawn(async move {
                    worker.prepare_artifact_action_view(&captured, Some(summary))
                })
                .await;
            let _ = view.update(cx, |view, cx| {
                if let Ok(plan) = result {
                    if client.claim_artifact_presentation(&operation) {
                        let error = view
                            .external
                            .open_url(&ThreadExternalNavigationRequest::new(
                                effect,
                                plan.url.expose_url().into(),
                            ))
                            .err()
                            .map(|_| "viewer_failed".into());
                        client.complete_artifact_presentation(&operation, error);
                    }
                }
                view.tasks.remove(&operation);
                view.synchronize(cx);
            });
        });
        self.tasks.insert(identity, task);
        cx.notify();
    }
    fn choose_thread_artifact_download_destination(
        &mut self,
        summary: ArtifactSummary,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(identity) = self.begin(&summary.artifact, ArtifactActionKind::Download) else {
            return;
        };
        let effect = self.presentation();
        let selection = self
            .files
            .select_download_destination(effect, identity.clone(), cx);
        let client = self.client.clone();
        let runtime = self.files.runtime_root();
        let operation = identity.clone();
        let task = cx.spawn(async move |view, cx| {
            match selection.await {
                Ok(Some(destination)) => {
                    let worker = client.clone();
                    let captured = operation.clone();
                    let _ = cx
                        .background_spawn(async move {
                            worker.download_artifact_to_folder(
                                &captured,
                                destination,
                                runtime.as_path().into(),
                            )
                        })
                        .await;
                }
                Ok(None) => {
                    client.cancel_artifact_action(&operation);
                }
                Err(_) => {
                    client.fail_artifact_preparation(&operation, "download_failed".into());
                }
            }
            let _ = view.update(cx, |view, cx| {
                view.tasks.remove(&operation);
                view.synchronize(cx);
            });
        });
        self.tasks.insert(identity, task);
        cx.notify();
    }
    fn reveal_thread_artifact(&mut self, artifact: ArtifactRef, cx: &mut Context<Self>) {
        let Some(identity) = self.begin(&artifact, ArtifactActionKind::Reveal) else {
            return;
        };
        let effect = self.presentation();
        let client = self.client.clone();
        let operation = identity.clone();
        let task = cx.spawn(async move |view, cx| {
            let worker = client.clone();
            let captured = operation.clone();
            let result = cx
                .background_spawn(async move { worker.prepare_artifact_reveal(&captured) })
                .await;
            if let Ok(plan) = result {
                if client.claim_artifact_presentation(&operation) {
                    let effect =
                        view.update(cx, |view, cx| view.files.reveal_artifact(effect, plan, cx));
                    if let Ok(effect) = effect {
                        let error = effect.await.err().map(|_| "viewer_failed".into());
                        client.complete_artifact_presentation(&operation, error);
                    }
                }
            }
            let _ = view.update(cx, |view, cx| {
                view.tasks.remove(&operation);
                view.synchronize(cx);
            });
        });
        self.tasks.insert(identity, task);
        cx.notify();
    }
    fn cancel_thread_artifact_download(&mut self, artifact: ArtifactRef, cx: &mut Context<Self>) {
        if let Some(identity) = self.action(&artifact).map(|action| action.identity.clone()) {
            self.client.cancel_artifact_action(&identity);
            self.tasks.remove(&identity);
            self.files.retire_mount(&self.thread_id, self.mount);
            self.synchronize(cx);
        }
    }
    fn attach_artifact_to_composer(&mut self, artifact: ArtifactRef, _: &mut Context<Self>) {
        if let Some(draft) = self.client.composer_snapshot(&self.thread_id) {
            self.client.composer_intent(ComposerIntent::Domain {
                thread_id: self.thread_id.clone(),
                draft_id: draft.draft_id(),
                action: ComposerDomainAction::AddArtifactAttachment { artifact },
            });
        }
    }
}
impl Render for ThreadArtifactsView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_thread_artifacts_panel(cx)
    }
}
impl Drop for ThreadArtifactsView {
    fn drop(&mut self) {
        for identity in self.tasks.keys() {
            self.client.cancel_artifact_action(identity);
        }
        self.files.retire_mount(&self.thread_id, self.mount);
        self.external.retire_mount(&self.thread_id, self.mount);
        self.binding.clear();
    }
}
fn format_artifact_size(size: Option<u64>) -> String {
    size.map(pioneer_client::artifacts::presentation::format_artifact_size_bytes)
        .unwrap_or_else(|| t!("artifacts.size_unknown").to_string())
}
fn artifact_error(code: &str) -> String {
    match code {
        "artifact_authentication_required" | "authentication_failed" => {
            t!("artifacts.action.error.authentication")
        }
        "artifact_reconfiguration_required" => t!("artifacts.action.error.reconfigure"),
        "artifact_revoked_or_unavailable" => t!("artifacts.action.error.revoked"),
        "grant_expired" => t!("artifacts.action.error.grant_expired"),
        "invalid_artifact_action" | "invalid_request" | "invalid_response" => {
            t!("artifacts.action.error.invalid_artifact")
        }
        "cancelled" => t!("artifacts.action.error.cancelled"),
        "integrity_failed" => t!("artifacts.action.error.integrity"),
        "disk_full" => t!("artifacts.action.error.disk_full"),
        "viewer_failed" => t!("artifacts.action.error.viewer_failed"),
        "local_copy_invalid" => t!("artifacts.action.error.local_copy_invalid"),
        _ => t!("artifacts.action.error.download_failed"),
    }
    .to_string()
}
