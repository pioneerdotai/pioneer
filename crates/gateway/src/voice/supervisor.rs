use super::model_catalog::{
    VoiceModelCatalogEntry, VoiceModelInstallLayout, voice_model_catalog_entry,
    voice_model_install_layout,
};
use super::model_install::{
    ReqwestVoiceModelArchiveDownloader, VoiceModelArchiveDownloader, VoiceModelCleanupReport,
    VoiceModelInstallControl, VoiceModelInstallPhase, VoiceModelInstallProgress,
    VoiceModelInstallReport, ensure_voice_model_installed_with_control,
    force_fresh_voice_model_install, is_voice_model_installed_and_verified,
    remove_non_selected_voice_model_installs,
};
use super::runtime::LoadedVoiceEngine;
use super::transcription::{PreparedSpeechBuffer, VoiceSpeechTranscriber, VoiceTranscriptionError};
use anyhow::{Context, Result};
use async_trait::async_trait;
use pioneer_config::{AppConfig, GatewayVoiceConfig, GatewayVoiceInputProviderConfig};
use pioneer_protocol::{
    GatewayVoiceInputProvider, GatewayVoiceInputRuntimePhase, GatewayVoiceInputRuntimeSnapshot,
    GatewayVoiceInputSettings,
};
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct VoiceInputDesiredState {
    pub(crate) enabled: bool,
    pub(crate) provider: Option<GatewayVoiceInputProvider>,
    pub(crate) model: Option<String>,
}

impl VoiceInputDesiredState {
    pub(crate) fn from_config(config: &GatewayVoiceConfig) -> Self {
        Self {
            enabled: config.enabled,
            provider: config.provider.map(|provider| match provider {
                GatewayVoiceInputProviderConfig::Local => GatewayVoiceInputProvider::Local,
            }),
            model: config.model.clone(),
        }
    }

    fn selected_identity(&self) -> Option<VoiceModelIdentity> {
        if !self.enabled {
            return None;
        }
        let provider = self.provider?;
        let model = self.model.as_deref()?.trim();
        if model.is_empty() {
            return None;
        }
        Some(VoiceModelIdentity {
            provider,
            model: model.to_owned(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VoiceModelIdentity {
    pub(crate) provider: GatewayVoiceInputProvider,
    pub(crate) model: String,
}

#[async_trait]
pub(crate) trait VoiceModelInstaller: Send + Sync {
    fn verified_install_layout(
        &self,
        entry: &VoiceModelCatalogEntry,
    ) -> Result<Option<VoiceModelInstallLayout>>;

    fn cleanup_non_selected(
        &self,
        selected_model_id: &str,
        cancellation: &CancellationToken,
        protected_model_id: &RwLock<Option<String>>,
    ) -> Result<VoiceModelCleanupReport>;

    async fn install(
        &self,
        entry: VoiceModelCatalogEntry,
        force_fresh: bool,
        control: VoiceModelInstallControl,
    ) -> Result<VoiceModelInstallReport>;
}

pub(crate) struct FilesystemVoiceModelInstaller {
    config: AppConfig,
    runtime_home: PathBuf,
    downloader: Arc<dyn VoiceModelArchiveDownloader>,
}

impl FilesystemVoiceModelInstaller {
    pub(crate) fn new(config: AppConfig, runtime_home: PathBuf) -> Self {
        Self::with_downloader(
            config,
            runtime_home,
            Arc::new(ReqwestVoiceModelArchiveDownloader::new()),
        )
    }

    pub(crate) fn with_downloader(
        config: AppConfig,
        runtime_home: PathBuf,
        downloader: Arc<dyn VoiceModelArchiveDownloader>,
    ) -> Self {
        Self {
            config,
            runtime_home,
            downloader,
        }
    }
}

#[async_trait]
impl VoiceModelInstaller for FilesystemVoiceModelInstaller {
    fn verified_install_layout(
        &self,
        entry: &VoiceModelCatalogEntry,
    ) -> Result<Option<VoiceModelInstallLayout>> {
        let layout = voice_model_install_layout(entry, &self.config, self.runtime_home.as_path())?;
        Ok(is_voice_model_installed_and_verified(entry, &layout).then_some(layout))
    }

    fn cleanup_non_selected(
        &self,
        selected_model_id: &str,
        cancellation: &CancellationToken,
        protected_model_id: &RwLock<Option<String>>,
    ) -> Result<VoiceModelCleanupReport> {
        remove_non_selected_voice_model_installs(
            &self.config,
            self.runtime_home.as_path(),
            selected_model_id,
            cancellation,
            protected_model_id,
        )
    }

    async fn install(
        &self,
        entry: VoiceModelCatalogEntry,
        force_fresh: bool,
        control: VoiceModelInstallControl,
    ) -> Result<VoiceModelInstallReport> {
        if force_fresh {
            return force_fresh_voice_model_install(
                &entry,
                &self.config,
                self.runtime_home.as_path(),
                self.downloader.as_ref(),
                &control,
            )
            .await;
        }
        ensure_voice_model_installed_with_control(
            &entry,
            &self.config,
            self.runtime_home.as_path(),
            self.downloader.as_ref(),
            &control,
        )
        .await
    }
}

pub(crate) trait VoiceEngineLoader: Send + Sync {
    fn load(
        &self,
        entry: &VoiceModelCatalogEntry,
        layout: &VoiceModelInstallLayout,
    ) -> std::result::Result<LoadedVoiceEngine, VoiceTranscriptionError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct EagerVoiceEngineLoader;

impl VoiceEngineLoader for EagerVoiceEngineLoader {
    fn load(
        &self,
        entry: &VoiceModelCatalogEntry,
        layout: &VoiceModelInstallLayout,
    ) -> std::result::Result<LoadedVoiceEngine, VoiceTranscriptionError> {
        LoadedVoiceEngine::load(entry, layout)
    }
}

pub(crate) struct VoiceInputSupervisor {
    inner: Mutex<VoiceInputSupervisorInner>,
    pub(crate) installer: Arc<dyn VoiceModelInstaller>,
    pub(crate) engine_loader: Arc<dyn VoiceEngineLoader>,
    settings_tx: watch::Sender<GatewayVoiceInputSettings>,
    cleanup_protected_model_id: Arc<RwLock<Option<String>>>,
}

struct VoiceInputSupervisorInner {
    state: VoiceInputSupervisorState,
    current_reconcile: Option<VoiceReconcileContext>,
    loaded_engine: Option<LoadedVoiceEngine>,
}

#[derive(Clone)]
pub(crate) struct VoiceReconcileContext {
    generation: u64,
    identity: VoiceModelIdentity,
    force_fresh: bool,
    cancellation: CancellationToken,
}

impl VoiceReconcileContext {
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn identity(&self) -> &VoiceModelIdentity {
        &self.identity
    }

    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub(crate) const fn force_fresh(&self) -> bool {
        self.force_fresh
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

pub(crate) struct VoiceDesiredApplyResult {
    #[cfg(test)]
    pub(crate) generation: u64,
    #[cfg(test)]
    pub(crate) changed: bool,
    pub(crate) reconcile: Option<VoiceReconcileContext>,
}

impl VoiceInputSupervisor {
    pub(crate) fn new(
        installer: Arc<dyn VoiceModelInstaller>,
        engine_loader: Arc<dyn VoiceEngineLoader>,
    ) -> Self {
        let initial_state = VoiceInputSupervisorState::default();
        let (settings_tx, _) = watch::channel(initial_state.settings_snapshot());
        Self {
            inner: Mutex::new(VoiceInputSupervisorInner {
                state: initial_state,
                current_reconcile: None,
                loaded_engine: None,
            }),
            installer,
            engine_loader,
            settings_tx,
            cleanup_protected_model_id: Arc::new(RwLock::new(None)),
        }
    }

    #[cfg(test)]
    pub(crate) fn desired(&self) -> VoiceInputDesiredState {
        self.lock_inner().state.desired.clone()
    }

    #[cfg(test)]
    pub(crate) fn generation(&self) -> u64 {
        self.lock_inner().state.generation
    }

    pub(crate) fn runtime_snapshot(&self) -> GatewayVoiceInputRuntimeSnapshot {
        self.lock_inner().state.runtime.clone()
    }

    pub(crate) fn settings_snapshot(&self) -> GatewayVoiceInputSettings {
        self.lock_inner().state.settings_snapshot()
    }

    pub(crate) fn subscribe_settings(&self) -> watch::Receiver<GatewayVoiceInputSettings> {
        self.settings_tx.subscribe()
    }

    #[cfg(test)]
    pub(crate) fn loaded_model_identity(&self) -> Option<VoiceModelIdentity> {
        self.lock_inner().state.loaded_model.clone()
    }

    #[cfg(test)]
    fn cleanup_protected_model_id(&self) -> Option<String> {
        self.cleanup_protected_model_id
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn current_reconcile_context(&self) -> Option<VoiceReconcileContext> {
        self.lock_inner().current_reconcile.clone()
    }

    pub(crate) fn apply_desired(
        &self,
        desired: VoiceInputDesiredState,
        retry_install: bool,
    ) -> std::result::Result<VoiceDesiredApplyResult, VoiceSupervisorTransitionError> {
        let protected_model_id = desired.selected_identity().map(|identity| identity.model);
        let mut cleanup_protected_model_id = self
            .cleanup_protected_model_id
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut inner = self.lock_inner();
        if retry_install {
            let identity = desired
                .selected_identity()
                .ok_or(VoiceSupervisorTransitionError::SelectedModelRequired)?;
            if voice_model_catalog_entry(identity.model.as_str()).is_none() {
                return Err(VoiceSupervisorTransitionError::UnknownModel(identity.model));
            }
        }
        let changed = inner.state.desired != desired || retry_install;
        if !changed {
            return Ok(VoiceDesiredApplyResult {
                #[cfg(test)]
                generation: inner.state.generation,
                #[cfg(test)]
                changed: false,
                reconcile: None,
            });
        }
        if inner.state.generation == u64::MAX {
            return Err(VoiceSupervisorTransitionError::GenerationExhausted);
        }

        if let Some(previous) = inner.current_reconcile.take() {
            previous.cancellation.cancel();
        }
        inner.loaded_engine = None;
        let generation = inner.state.apply_desired(desired, retry_install)?;
        let reconcile =
            inner
                .state
                .desired
                .selected_identity()
                .map(|identity| VoiceReconcileContext {
                    generation,
                    identity,
                    force_fresh: retry_install,
                    cancellation: CancellationToken::new(),
                });
        inner.current_reconcile.clone_from(&reconcile);
        *cleanup_protected_model_id = protected_model_id;
        self.publish_settings(&inner.state);
        Ok(VoiceDesiredApplyResult {
            #[cfg(test)]
            generation,
            #[cfg(test)]
            changed: true,
            reconcile,
        })
    }

    pub(crate) fn mark_downloading(
        &self,
        generation: u64,
    ) -> std::result::Result<bool, VoiceSupervisorTransitionError> {
        self.mutate_current_generation(generation, |state| state.mark_downloading())
    }

    pub(crate) fn report_download_progress(
        &self,
        generation: u64,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
    ) -> std::result::Result<bool, VoiceSupervisorTransitionError> {
        self.mutate_current_generation(generation, |state| {
            state.report_download_progress(downloaded_bytes, total_bytes)
        })
    }

    pub(crate) fn mark_installing(
        &self,
        generation: u64,
    ) -> std::result::Result<bool, VoiceSupervisorTransitionError> {
        self.mutate_current_generation(generation, |state| state.mark_installing())
    }

    pub(crate) fn mark_loading(
        &self,
        generation: u64,
    ) -> std::result::Result<bool, VoiceSupervisorTransitionError> {
        self.mutate_current_generation(generation, |state| state.mark_loading())
    }

    pub(crate) fn mark_ready(
        &self,
        generation: u64,
        identity: VoiceModelIdentity,
        engine: LoadedVoiceEngine,
    ) -> std::result::Result<bool, VoiceSupervisorTransitionError> {
        let mut inner = self.lock_inner();
        if !inner.is_current_reconcile_generation(generation) {
            return Ok(false);
        }
        inner.state.mark_ready(identity)?;
        inner.loaded_engine = Some(engine);
        self.publish_settings(&inner.state);
        Ok(true)
    }

    pub(crate) fn mark_failed(
        &self,
        generation: u64,
        error: impl Into<String>,
    ) -> std::result::Result<bool, VoiceSupervisorTransitionError> {
        let error = error.into();
        let mut inner = self.lock_inner();
        if !inner.is_current_reconcile_generation(generation) {
            return Ok(false);
        }
        inner.state.mark_failed(error)?;
        inner.loaded_engine = None;
        inner.current_reconcile = None;
        self.publish_settings(&inner.state);
        Ok(true)
    }

    pub(crate) fn transcribe(
        &self,
        buffer: &super::transcription::PreparedSpeechBuffer,
    ) -> std::result::Result<String, VoiceTranscriptionError> {
        let mut inner = self.lock_inner();
        let desired = inner.state.desired.selected_identity();
        if inner.state.runtime.phase != GatewayVoiceInputRuntimePhase::Ready
            || !inner.state.runtime.effective_enabled
            || desired.as_ref() != inner.state.loaded_model.as_ref()
        {
            return Err(super::transcription::transcription_error(
                super::transcription::VoiceTranscriptionErrorKind::ModelUnavailable,
                "the selected Voice Input model is not Ready",
            ));
        }
        let Some(engine) = inner.loaded_engine.as_mut() else {
            return Err(super::transcription::transcription_error(
                super::transcription::VoiceTranscriptionErrorKind::ModelUnavailable,
                "Voice Input reported Ready without a loaded engine",
            ));
        };
        let result = catch_unwind(AssertUnwindSafe(|| engine.transcribe(buffer)));
        match result {
            Ok(Ok(transcript)) => Ok(transcript),
            Ok(Err(error)) => {
                let message = bounded_voice_supervisor_error(error.message.clone());
                inner.loaded_engine = None;
                inner.current_reconcile = None;
                let _ = inner.state.mark_failed(message);
                self.publish_settings(&inner.state);
                Err(error)
            }
            Err(_) => {
                let error = super::transcription::transcription_error(
                    super::transcription::VoiceTranscriptionErrorKind::RuntimeFailure,
                    "Voice Input engine panicked during transcription",
                );
                inner.loaded_engine = None;
                inner.current_reconcile = None;
                let _ = inner.state.mark_failed(error.message.clone());
                self.publish_settings(&inner.state);
                Err(error)
            }
        }
    }

    pub(crate) async fn reconcile(self: &Arc<Self>, context: VoiceReconcileContext) {
        if context.is_cancelled() {
            return;
        }
        let generation = context.generation();
        let identity = context.identity().clone();
        let Some(entry) = voice_model_catalog_entry(identity.model.as_str()) else {
            self.publish_reconcile_failure(
                generation,
                format!("unknown local transcription model `{}`", identity.model),
            );
            return;
        };

        let installed_layout = if context.force_fresh() {
            None
        } else {
            match self.installer.verified_install_layout(&entry) {
                Ok(layout) => layout,
                Err(error) => {
                    self.publish_reconcile_failure(generation, format!("{error:#}"));
                    return;
                }
            }
        };
        let layout = if let Some(layout) = installed_layout {
            if !self.publish_loading(generation) {
                return;
            }
            layout
        } else {
            let Ok(true) = self.mark_downloading(generation) else {
                return;
            };
            let weak_supervisor = Arc::downgrade(self);
            let progress = Arc::new(move |progress: VoiceModelInstallProgress| {
                let Some(supervisor) = weak_supervisor.upgrade() else {
                    return;
                };
                supervisor.publish_install_progress(generation, progress);
            });
            let control = VoiceModelInstallControl::new(context.cancellation_token(), progress);
            let report = match self
                .installer
                .install(entry, context.force_fresh(), control)
                .await
            {
                Ok(report) => report,
                Err(error) => {
                    self.publish_reconcile_failure(generation, format!("{error:#}"));
                    return;
                }
            };
            if self.runtime_snapshot().phase == GatewayVoiceInputRuntimePhase::Downloading {
                let Ok(true) = self.mark_installing(generation) else {
                    return;
                };
            }
            if !self.publish_loading(generation) {
                return;
            }
            report.layout
        };

        if context.is_cancelled() {
            return;
        }
        let loader = self.engine_loader.clone();
        let load_entry = entry;
        let load_layout = layout;
        let loaded = tokio::task::spawn_blocking(move || loader.load(&load_entry, &load_layout))
            .await
            .context("voice engine loader task failed")
            .and_then(|result| result.map_err(anyhow::Error::new));
        let engine = match loaded {
            Ok(engine) => engine,
            Err(error) => {
                self.publish_reconcile_failure(generation, format!("{error:#}"));
                return;
            }
        };
        if context.is_cancelled() {
            drop(engine);
            return;
        }
        match self.mark_ready(generation, identity.clone(), engine) {
            Ok(true) => {
                let installer = self.installer.clone();
                let selected_model_id = identity.model;
                let cleanup_model_id = selected_model_id.clone();
                let protected_model_id = self.cleanup_protected_model_id.clone();
                // Reaching Ready commits this replacement. A later disable or model change must
                // not interrupt removal of models superseded by this committed selection.
                let cancellation = CancellationToken::new();
                match tokio::task::spawn_blocking(move || {
                    installer.cleanup_non_selected(
                        cleanup_model_id.as_str(),
                        &cancellation,
                        protected_model_id.as_ref(),
                    )
                })
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => tracing::warn!(
                        model_id = %selected_model_id,
                        error = %format!("{error:#}"),
                        "failed to clean superseded local voice model installations"
                    ),
                    Err(error) => tracing::warn!(
                        model_id = %selected_model_id,
                        error = %error,
                        "local voice model cleanup task failed"
                    ),
                }
            }
            Ok(false) => {}
            Err(error) => self.publish_reconcile_failure(generation, error.to_string()),
        }
    }

    fn publish_install_progress(&self, generation: u64, progress: VoiceModelInstallProgress) {
        match progress.phase {
            VoiceModelInstallPhase::Downloading | VoiceModelInstallPhase::Verifying => {
                let _ = self.report_download_progress(
                    generation,
                    progress.downloaded_bytes,
                    progress.total_bytes,
                );
            }
            VoiceModelInstallPhase::Installing => {
                if self.runtime_snapshot().phase == GatewayVoiceInputRuntimePhase::Downloading {
                    let _ = self.mark_installing(generation);
                }
            }
        }
    }

    fn publish_loading(&self, generation: u64) -> bool {
        self.mark_loading(generation).unwrap_or(false)
    }

    fn publish_reconcile_failure(&self, generation: u64, error: String) {
        let _ = self.mark_failed(generation, bounded_voice_supervisor_error(error));
    }

    #[cfg(test)]
    fn has_loaded_engine(&self) -> bool {
        self.lock_inner().loaded_engine.is_some()
    }

    fn mutate_current_generation(
        &self,
        generation: u64,
        mutation: impl FnOnce(&mut VoiceInputSupervisorState) -> TransitionResult,
    ) -> std::result::Result<bool, VoiceSupervisorTransitionError> {
        let mut inner = self.lock_inner();
        if !inner.is_current_reconcile_generation(generation) {
            return Ok(false);
        }
        mutation(&mut inner.state)?;
        self.publish_settings(&inner.state);
        Ok(true)
    }

    fn publish_settings(&self, state: &VoiceInputSupervisorState) {
        self.settings_tx.send_replace(state.settings_snapshot());
    }

    fn lock_inner(&self) -> MutexGuard<'_, VoiceInputSupervisorInner> {
        self.inner
            .lock()
            .expect("voice input supervisor mutex must not be poisoned")
    }
}

impl VoiceSpeechTranscriber for VoiceInputSupervisor {
    fn transcribe_speech(
        &self,
        buffer: &PreparedSpeechBuffer,
    ) -> std::result::Result<String, VoiceTranscriptionError> {
        self.transcribe(buffer)
    }
}

const MAX_VOICE_SUPERVISOR_ERROR_CHARS: usize = 512;

fn bounded_voice_supervisor_error(error: String) -> String {
    if error.chars().count() <= MAX_VOICE_SUPERVISOR_ERROR_CHARS {
        return error;
    }
    error
        .chars()
        .take(MAX_VOICE_SUPERVISOR_ERROR_CHARS)
        .collect()
}

impl VoiceInputSupervisorInner {
    fn is_current_reconcile_generation(&self, generation: u64) -> bool {
        self.state.generation == generation
            && self.current_reconcile.as_ref().is_some_and(|reconcile| {
                reconcile.generation == generation && !reconcile.cancellation.is_cancelled()
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VoiceInputSupervisorState {
    desired: VoiceInputDesiredState,
    runtime: GatewayVoiceInputRuntimeSnapshot,
    loaded_model: Option<VoiceModelIdentity>,
    generation: u64,
}

impl Default for VoiceInputSupervisorState {
    fn default() -> Self {
        Self {
            desired: VoiceInputDesiredState::default(),
            runtime: runtime_snapshot_for_desired(&VoiceInputDesiredState::default()),
            loaded_model: None,
            generation: 0,
        }
    }
}

impl VoiceInputSupervisorState {
    fn settings_snapshot(&self) -> GatewayVoiceInputSettings {
        GatewayVoiceInputSettings {
            enabled: self.desired.enabled,
            provider: self.desired.provider,
            model: self.desired.model.clone(),
            runtime: self.runtime.clone(),
        }
    }

    fn apply_desired(
        &mut self,
        desired: VoiceInputDesiredState,
        retry_install: bool,
    ) -> std::result::Result<u64, VoiceSupervisorTransitionError> {
        if self.desired == desired && !retry_install {
            return Ok(self.generation);
        }
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(VoiceSupervisorTransitionError::GenerationExhausted)?;
        self.desired = desired;
        self.loaded_model = None;
        self.runtime = runtime_snapshot_for_desired(&self.desired);
        Ok(self.generation)
    }

    fn mark_downloading(&mut self) -> TransitionResult {
        self.require_phase(&[GatewayVoiceInputRuntimePhase::Missing])?;
        self.set_phase(GatewayVoiceInputRuntimePhase::Downloading);
        Ok(())
    }

    fn report_download_progress(
        &mut self,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
    ) -> TransitionResult {
        self.require_phase(&[GatewayVoiceInputRuntimePhase::Downloading])?;
        if total_bytes.is_some_and(|total| downloaded_bytes > total) {
            return Err(VoiceSupervisorTransitionError::InvalidProgress {
                downloaded_bytes,
                total_bytes,
            });
        }
        self.runtime.downloaded_bytes = Some(downloaded_bytes);
        self.runtime.total_bytes = total_bytes;
        Ok(())
    }

    fn mark_installing(&mut self) -> TransitionResult {
        self.require_phase(&[GatewayVoiceInputRuntimePhase::Downloading])?;
        self.set_phase(GatewayVoiceInputRuntimePhase::Installing);
        Ok(())
    }

    fn mark_loading(&mut self) -> TransitionResult {
        self.require_phase(&[
            GatewayVoiceInputRuntimePhase::Missing,
            GatewayVoiceInputRuntimePhase::Installing,
        ])?;
        self.set_phase(GatewayVoiceInputRuntimePhase::Loading);
        Ok(())
    }

    fn mark_ready(&mut self, identity: VoiceModelIdentity) -> TransitionResult {
        self.require_phase(&[GatewayVoiceInputRuntimePhase::Loading])?;
        let expected = self
            .desired
            .selected_identity()
            .ok_or(VoiceSupervisorTransitionError::SelectedModelRequired)?;
        if identity != expected {
            return Err(VoiceSupervisorTransitionError::IdentityMismatch {
                expected,
                actual: identity,
            });
        }
        self.loaded_model = Some(identity);
        self.set_phase(GatewayVoiceInputRuntimePhase::Ready);
        self.refresh_effective_enabled();
        Ok(())
    }

    fn mark_failed(&mut self, error: String) -> TransitionResult {
        if self.desired.selected_identity().is_none() {
            return Err(VoiceSupervisorTransitionError::SelectedModelRequired);
        }
        self.require_phase(&[
            GatewayVoiceInputRuntimePhase::Missing,
            GatewayVoiceInputRuntimePhase::Downloading,
            GatewayVoiceInputRuntimePhase::Installing,
            GatewayVoiceInputRuntimePhase::Loading,
            GatewayVoiceInputRuntimePhase::Ready,
        ])?;
        self.loaded_model = None;
        self.set_phase(GatewayVoiceInputRuntimePhase::Failed);
        self.runtime.error = Some(error);
        self.refresh_effective_enabled();
        Ok(())
    }

    fn require_phase(&self, allowed: &[GatewayVoiceInputRuntimePhase]) -> TransitionResult {
        if allowed.contains(&self.runtime.phase) {
            Ok(())
        } else {
            Err(VoiceSupervisorTransitionError::InvalidPhase {
                current: self.runtime.phase,
                allowed: allowed.to_vec(),
            })
        }
    }

    fn set_phase(&mut self, phase: GatewayVoiceInputRuntimePhase) {
        self.runtime.phase = phase;
        self.runtime.error = None;
        if !matches!(
            phase,
            GatewayVoiceInputRuntimePhase::Downloading | GatewayVoiceInputRuntimePhase::Installing
        ) {
            self.runtime.downloaded_bytes = None;
            self.runtime.total_bytes = None;
        }
        self.refresh_effective_enabled();
    }

    fn refresh_effective_enabled(&mut self) {
        self.runtime.effective_enabled = self.runtime.phase == GatewayVoiceInputRuntimePhase::Ready
            && self.desired.selected_identity().as_ref() == self.loaded_model.as_ref();
    }
}

type TransitionResult = std::result::Result<(), VoiceSupervisorTransitionError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum VoiceSupervisorTransitionError {
    GenerationExhausted,
    SelectedModelRequired,
    UnknownModel(String),
    InvalidPhase {
        current: GatewayVoiceInputRuntimePhase,
        allowed: Vec<GatewayVoiceInputRuntimePhase>,
    },
    InvalidProgress {
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
    },
    IdentityMismatch {
        expected: VoiceModelIdentity,
        actual: VoiceModelIdentity,
    },
}

impl fmt::Display for VoiceSupervisorTransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationExhausted => {
                formatter.write_str("voice supervisor generation exhausted")
            }
            Self::SelectedModelRequired => {
                formatter.write_str("voice supervisor transition requires a selected model")
            }
            Self::UnknownModel(model) => {
                write!(formatter, "unknown local transcription model `{model}`")
            }
            Self::InvalidPhase { current, allowed } => write!(
                formatter,
                "voice supervisor cannot transition from {current:?}; expected one of {allowed:?}"
            ),
            Self::InvalidProgress {
                downloaded_bytes,
                total_bytes,
            } => write!(
                formatter,
                "voice model download progress {downloaded_bytes} exceeds total {total_bytes:?}"
            ),
            Self::IdentityMismatch { expected, actual } => write!(
                formatter,
                "voice runtime identity {actual:?} does not match desired {expected:?}"
            ),
        }
    }
}

impl std::error::Error for VoiceSupervisorTransitionError {}

fn runtime_snapshot_for_desired(
    desired: &VoiceInputDesiredState,
) -> GatewayVoiceInputRuntimeSnapshot {
    let identity = desired.selected_identity();
    GatewayVoiceInputRuntimeSnapshot {
        phase: if !desired.enabled {
            GatewayVoiceInputRuntimePhase::Disabled
        } else if identity.is_none() {
            GatewayVoiceInputRuntimePhase::ModelNotSelected
        } else {
            GatewayVoiceInputRuntimePhase::Missing
        },
        effective_enabled: false,
        model: identity.map(|identity| identity.model),
        downloaded_bytes: None,
        total_bytes: None,
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::model_install::VoiceModelInstallStatus;
    use super::*;
    use anyhow::bail;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::{Barrier, oneshot};

    struct NoIoInstaller {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl VoiceModelInstaller for NoIoInstaller {
        fn verified_install_layout(
            &self,
            _entry: &VoiceModelCatalogEntry,
        ) -> Result<Option<VoiceModelInstallLayout>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("pure state tests must not inspect installation")
        }

        fn cleanup_non_selected(
            &self,
            _selected_model_id: &str,
            _cancellation: &CancellationToken,
            _protected_model_id: &RwLock<Option<String>>,
        ) -> Result<VoiceModelCleanupReport> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("pure state tests must not clean installations")
        }

        async fn install(
            &self,
            _entry: VoiceModelCatalogEntry,
            _force_fresh: bool,
            _control: VoiceModelInstallControl,
        ) -> Result<VoiceModelInstallReport> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("pure state tests must not perform installation")
        }
    }

    struct NoIoEngineLoader {
        calls: Arc<AtomicUsize>,
    }

    impl VoiceEngineLoader for NoIoEngineLoader {
        fn load(
            &self,
            _entry: &VoiceModelCatalogEntry,
            _layout: &VoiceModelInstallLayout,
        ) -> std::result::Result<LoadedVoiceEngine, VoiceTranscriptionError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("pure state tests must not load an engine")
        }
    }

    #[derive(Clone)]
    enum FakeInstallMode {
        Installed(VoiceModelInstallLayout),
        Missing(VoiceModelInstallLayout),
        VerifyFailure,
        InstallFailure(VoiceModelInstallLayout),
    }

    struct FakeInstaller {
        mode: FakeInstallMode,
        inspect_calls: Arc<AtomicUsize>,
        install_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl VoiceModelInstaller for FakeInstaller {
        fn verified_install_layout(
            &self,
            _entry: &VoiceModelCatalogEntry,
        ) -> Result<Option<VoiceModelInstallLayout>> {
            self.inspect_calls.fetch_add(1, Ordering::SeqCst);
            match &self.mode {
                FakeInstallMode::Installed(layout) => Ok(Some(layout.clone())),
                FakeInstallMode::Missing(_) | FakeInstallMode::InstallFailure(_) => Ok(None),
                FakeInstallMode::VerifyFailure => {
                    bail!("fixture install validation failed: {}", "x".repeat(1_024))
                }
            }
        }

        fn cleanup_non_selected(
            &self,
            _selected_model_id: &str,
            _cancellation: &CancellationToken,
            _protected_model_id: &RwLock<Option<String>>,
        ) -> Result<VoiceModelCleanupReport> {
            Ok(VoiceModelCleanupReport::default())
        }

        async fn install(
            &self,
            _entry: VoiceModelCatalogEntry,
            force_fresh: bool,
            control: VoiceModelInstallControl,
        ) -> Result<VoiceModelInstallReport> {
            self.install_calls.fetch_add(1, Ordering::SeqCst);
            assert!(
                !force_fresh,
                "normal reconcile must not force a fresh retry"
            );
            control.report(VoiceModelInstallProgress {
                phase: VoiceModelInstallPhase::Downloading,
                downloaded_bytes: 20,
                total_bytes: Some(100),
            });
            match &self.mode {
                FakeInstallMode::Missing(layout) => {
                    control.report(VoiceModelInstallProgress {
                        phase: VoiceModelInstallPhase::Installing,
                        downloaded_bytes: 100,
                        total_bytes: Some(100),
                    });
                    Ok(VoiceModelInstallReport {
                        status: VoiceModelInstallStatus::Installed,
                        layout: layout.clone(),
                    })
                }
                FakeInstallMode::InstallFailure(_) => {
                    bail!("fixture checksum/install failure")
                }
                FakeInstallMode::Installed(_) | FakeInstallMode::VerifyFailure => {
                    panic!("unexpected fixture install call")
                }
            }
        }
    }

    struct FakeEngineLoader {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    struct RetryInstaller {
        layout: VoiceModelInstallLayout,
        inspect_calls: Arc<AtomicUsize>,
        force_fresh_calls: Arc<AtomicUsize>,
        fail: bool,
    }

    struct CleanupTrackingInstaller {
        layout: VoiceModelInstallLayout,
        cleanup_calls: Arc<AtomicUsize>,
    }

    struct BlockingCleanupInstaller {
        layout: VoiceModelInstallLayout,
        cleanup_started: std::sync::Mutex<Option<oneshot::Sender<()>>>,
        cleanup_release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        cleanup_saw_cancellation: Arc<AtomicBool>,
    }

    #[async_trait]
    impl VoiceModelInstaller for CleanupTrackingInstaller {
        fn verified_install_layout(
            &self,
            _entry: &VoiceModelCatalogEntry,
        ) -> Result<Option<VoiceModelInstallLayout>> {
            Ok(Some(self.layout.clone()))
        }

        fn cleanup_non_selected(
            &self,
            _selected_model_id: &str,
            cancellation: &CancellationToken,
            _protected_model_id: &RwLock<Option<String>>,
        ) -> Result<VoiceModelCleanupReport> {
            assert!(!cancellation.is_cancelled());
            self.cleanup_calls.fetch_add(1, Ordering::SeqCst);
            Ok(VoiceModelCleanupReport::default())
        }

        async fn install(
            &self,
            _entry: VoiceModelCatalogEntry,
            _force_fresh: bool,
            _control: VoiceModelInstallControl,
        ) -> Result<VoiceModelInstallReport> {
            panic!("installed cleanup fixture must not download")
        }
    }

    #[async_trait]
    impl VoiceModelInstaller for BlockingCleanupInstaller {
        fn verified_install_layout(
            &self,
            _entry: &VoiceModelCatalogEntry,
        ) -> Result<Option<VoiceModelInstallLayout>> {
            Ok(Some(self.layout.clone()))
        }

        fn cleanup_non_selected(
            &self,
            _selected_model_id: &str,
            cancellation: &CancellationToken,
            _protected_model_id: &RwLock<Option<String>>,
        ) -> Result<VoiceModelCleanupReport> {
            if let Some(started) = self
                .cleanup_started
                .lock()
                .expect("cleanup-started lock")
                .take()
            {
                let _ = started.send(());
            }
            self.cleanup_release
                .lock()
                .expect("cleanup-release lock")
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("release post-Ready cleanup");
            self.cleanup_saw_cancellation
                .store(cancellation.is_cancelled(), Ordering::SeqCst);
            Ok(VoiceModelCleanupReport::default())
        }

        async fn install(
            &self,
            _entry: VoiceModelCatalogEntry,
            _force_fresh: bool,
            _control: VoiceModelInstallControl,
        ) -> Result<VoiceModelInstallReport> {
            panic!("installed blocking-cleanup fixture must not download")
        }
    }

    #[async_trait]
    impl VoiceModelInstaller for RetryInstaller {
        fn verified_install_layout(
            &self,
            _entry: &VoiceModelCatalogEntry,
        ) -> Result<Option<VoiceModelInstallLayout>> {
            self.inspect_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(self.layout.clone()))
        }

        fn cleanup_non_selected(
            &self,
            _selected_model_id: &str,
            _cancellation: &CancellationToken,
            _protected_model_id: &RwLock<Option<String>>,
        ) -> Result<VoiceModelCleanupReport> {
            Ok(VoiceModelCleanupReport::default())
        }

        async fn install(
            &self,
            _entry: VoiceModelCatalogEntry,
            force_fresh: bool,
            control: VoiceModelInstallControl,
        ) -> Result<VoiceModelInstallReport> {
            assert!(force_fresh, "retry must force a fresh install");
            self.force_fresh_calls.fetch_add(1, Ordering::SeqCst);
            let _ = std::fs::remove_dir_all(self.layout.install_dir.as_path());
            control.report(VoiceModelInstallProgress {
                phase: VoiceModelInstallPhase::Downloading,
                downloaded_bytes: 100,
                total_bytes: Some(100),
            });
            if self.fail {
                bail!("fixture force-fresh download failed");
            }
            control.report(VoiceModelInstallProgress {
                phase: VoiceModelInstallPhase::Installing,
                downloaded_bytes: 100,
                total_bytes: Some(100),
            });
            Ok(VoiceModelInstallReport {
                status: VoiceModelInstallStatus::Installed,
                layout: self.layout.clone(),
            })
        }
    }

    impl VoiceEngineLoader for FakeEngineLoader {
        fn load(
            &self,
            _entry: &VoiceModelCatalogEntry,
            _layout: &VoiceModelInstallLayout,
        ) -> std::result::Result<LoadedVoiceEngine, VoiceTranscriptionError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(VoiceTranscriptionError {
                    kind: super::super::transcription::VoiceTranscriptionErrorKind::RuntimeFailure,
                    message: "fixture eager load failed".to_owned(),
                });
            }
            Ok(LoadedVoiceEngine::test_stub())
        }
    }

    #[test]
    fn voice_supervisor_state_distinguishes_disabled_unselected_and_missing() {
        let (supervisor, installer_calls, loader_calls) = supervisor();
        assert_phase(&supervisor, GatewayVoiceInputRuntimePhase::Disabled, None);

        supervisor
            .apply_desired(
                VoiceInputDesiredState {
                    enabled: true,
                    provider: None,
                    model: None,
                },
                false,
            )
            .expect("enable without selection");
        assert_phase(
            &supervisor,
            GatewayVoiceInputRuntimePhase::ModelNotSelected,
            None,
        );

        supervisor
            .apply_desired(selected("small"), false)
            .expect("select model");
        assert_phase(
            &supervisor,
            GatewayVoiceInputRuntimePhase::Missing,
            Some("small"),
        );
        assert_eq!(installer_calls.load(Ordering::SeqCst), 0);
        assert_eq!(loader_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn voice_supervisor_state_desired_input_transition_table_is_exhaustive() {
        let cases = [
            (
                VoiceInputDesiredState::default(),
                GatewayVoiceInputRuntimePhase::Disabled,
                None,
            ),
            (
                VoiceInputDesiredState {
                    enabled: true,
                    provider: None,
                    model: None,
                },
                GatewayVoiceInputRuntimePhase::ModelNotSelected,
                None,
            ),
            (
                VoiceInputDesiredState {
                    enabled: true,
                    provider: Some(GatewayVoiceInputProvider::Local),
                    model: None,
                },
                GatewayVoiceInputRuntimePhase::ModelNotSelected,
                None,
            ),
            (
                selected("small"),
                GatewayVoiceInputRuntimePhase::Missing,
                Some("small"),
            ),
        ];

        for (desired, expected_phase, expected_model) in cases {
            let snapshot = runtime_snapshot_for_desired(&desired);
            assert_eq!(snapshot.phase, expected_phase);
            assert_eq!(snapshot.model.as_deref(), expected_model);
            assert!(!snapshot.effective_enabled);
            assert_eq!(snapshot.downloaded_bytes, None);
            assert_eq!(snapshot.total_bytes, None);
            assert_eq!(snapshot.error, None);
        }
    }

    #[test]
    fn voice_supervisor_state_covers_download_install_load_ready_transition_table() {
        let (supervisor, _, _) = supervisor();
        supervisor
            .apply_desired(selected("small"), false)
            .expect("select model");

        mark_downloading(&supervisor).expect("start download");
        assert_phase(
            &supervisor,
            GatewayVoiceInputRuntimePhase::Downloading,
            Some("small"),
        );
        report_download_progress(&supervisor, 25, Some(100)).expect("download progress");
        assert_eq!(supervisor.runtime_snapshot().downloaded_bytes, Some(25));
        assert_eq!(supervisor.runtime_snapshot().total_bytes, Some(100));

        mark_installing(&supervisor).expect("start install");
        assert_phase(
            &supervisor,
            GatewayVoiceInputRuntimePhase::Installing,
            Some("small"),
        );
        mark_loading(&supervisor).expect("start load");
        assert_phase(
            &supervisor,
            GatewayVoiceInputRuntimePhase::Loading,
            Some("small"),
        );
        mark_ready(&supervisor, identity("small")).expect("publish ready");
        let ready = supervisor.runtime_snapshot();
        assert_eq!(ready.phase, GatewayVoiceInputRuntimePhase::Ready);
        assert!(ready.effective_enabled);
        assert_eq!(supervisor.loaded_model_identity(), Some(identity("small")));
    }

    #[test]
    fn voice_supervisor_state_installed_model_may_transition_directly_to_loading() {
        let (supervisor, _, _) = supervisor();
        supervisor
            .apply_desired(selected("parakeet-tdt-0.6b-v3"), false)
            .expect("select model");
        mark_loading(&supervisor).expect("load verified install");
        mark_ready(&supervisor, identity("parakeet-tdt-0.6b-v3")).expect("publish ready");
        assert!(supervisor.runtime_snapshot().effective_enabled);
    }

    #[test]
    fn voice_supervisor_state_failures_never_fallback_or_remain_effectively_enabled() {
        let start_phases = [
            GatewayVoiceInputRuntimePhase::Missing,
            GatewayVoiceInputRuntimePhase::Downloading,
            GatewayVoiceInputRuntimePhase::Installing,
            GatewayVoiceInputRuntimePhase::Loading,
            GatewayVoiceInputRuntimePhase::Ready,
        ];

        for start_phase in start_phases {
            let (supervisor, _, _) = supervisor_at(start_phase);
            mark_failed(&supervisor, format!("failure from {start_phase:?}"))
                .expect("selected model failure");
            let snapshot = supervisor.runtime_snapshot();
            assert_eq!(snapshot.phase, GatewayVoiceInputRuntimePhase::Failed);
            assert!(!snapshot.effective_enabled);
            assert_eq!(snapshot.model.as_deref(), Some("small"));
            assert!(
                snapshot
                    .error
                    .as_deref()
                    .is_some_and(|error| error.contains("failure"))
            );
            assert_eq!(supervisor.loaded_model_identity(), None);
            assert_eq!(supervisor.desired().model.as_deref(), Some("small"));
        }
    }

    #[test]
    fn voice_supervisor_state_generation_changes_only_for_reconfiguration_or_retry() {
        let (supervisor, _, _) = supervisor();
        assert_eq!(supervisor.generation(), 0);
        let first = supervisor
            .apply_desired(selected("small"), false)
            .expect("first selection");
        assert_eq!(first.generation, 1);
        assert_eq!(
            supervisor
                .apply_desired(selected("small"), false)
                .expect("idempotent selection")
                .generation,
            1
        );
        assert_eq!(
            supervisor
                .apply_desired(selected("small"), true)
                .expect("explicit retry")
                .generation,
            2
        );
        assert_phase(
            &supervisor,
            GatewayVoiceInputRuntimePhase::Missing,
            Some("small"),
        );
        assert_eq!(
            supervisor
                .apply_desired(selected("medium"), false)
                .expect("model change")
                .generation,
            3
        );
        assert_eq!(
            supervisor
                .apply_desired(VoiceInputDesiredState::default(), false)
                .expect("disable")
                .generation,
            4
        );
        assert_phase(&supervisor, GatewayVoiceInputRuntimePhase::Disabled, None);
    }

    #[test]
    fn voice_supervisor_state_wrong_runtime_identity_cannot_publish_ready() {
        let (supervisor, _, _) = supervisor();
        supervisor
            .apply_desired(selected("small"), false)
            .expect("select model");
        mark_loading(&supervisor).expect("start loading");

        let error = mark_ready(&supervisor, identity("medium"))
            .expect_err("wrong identity must be rejected");
        assert!(matches!(
            error,
            VoiceSupervisorTransitionError::IdentityMismatch { .. }
        ));
        assert_phase(
            &supervisor,
            GatewayVoiceInputRuntimePhase::Loading,
            Some("small"),
        );
        assert!(!supervisor.runtime_snapshot().effective_enabled);
        assert_eq!(supervisor.loaded_model_identity(), None);
    }

    #[test]
    fn voice_supervisor_state_rejects_invalid_phase_and_progress_transitions() {
        let (supervisor, _, _) = supervisor();
        assert!(!mark_downloading(&supervisor).expect("inactive generation is ignored"));
        supervisor
            .apply_desired(selected("small"), false)
            .expect("select model");
        mark_downloading(&supervisor).expect("start download");
        assert!(report_download_progress(&supervisor, 101, Some(100)).is_err());
        assert!(mark_loading(&supervisor).is_err());
    }

    #[tokio::test]
    async fn voice_supervisor_generation_late_a_completion_cannot_overwrite_b() {
        let (supervisor, installer_calls, loader_calls) = supervisor();
        let first = supervisor
            .apply_desired(selected("small"), false)
            .expect("select model A");
        let first_context = first.reconcile.expect("A reconcile context");
        assert_eq!(first_context.generation(), 1);

        let second = supervisor
            .apply_desired(selected("medium"), false)
            .expect("select model B");
        let second_context = second.reconcile.expect("B reconcile context");
        assert!(first_context.is_cancelled());
        assert!(!second_context.is_cancelled());
        assert_eq!(second_context.generation(), 2);
        assert_eq!(second_context.identity(), &identity("medium"));

        assert!(
            !supervisor
                .mark_downloading(first_context.generation())
                .expect("stale download phase is ignored")
        );
        assert!(
            !supervisor
                .report_download_progress(first_context.generation(), 20, Some(100))
                .expect("stale progress is ignored")
        );
        assert!(
            !supervisor
                .mark_failed(first_context.generation(), "late A failure")
                .expect("stale error is ignored")
        );
        assert!(
            !supervisor
                .mark_ready(
                    first_context.generation(),
                    identity("small"),
                    LoadedVoiceEngine::test_stub(),
                )
                .expect("stale completion is ignored")
        );
        assert_phase(
            &supervisor,
            GatewayVoiceInputRuntimePhase::Missing,
            Some("medium"),
        );

        assert!(
            supervisor
                .mark_loading(second_context.generation())
                .expect("B starts loading")
        );
        assert!(
            supervisor
                .mark_ready(
                    second_context.generation(),
                    identity("medium"),
                    LoadedVoiceEngine::test_stub(),
                )
                .expect("B publishes ready")
        );
        assert_eq!(supervisor.loaded_model_identity(), Some(identity("medium")));
        assert!(supervisor.runtime_snapshot().effective_enabled);
        assert_eq!(installer_calls.load(Ordering::SeqCst), 0);
        assert_eq!(loader_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn voice_supervisor_cancellation_replacement_invokes_worker_cleanup_hook() {
        let (supervisor, _, _) = supervisor();
        let supervisor = Arc::new(supervisor);
        let first = supervisor
            .apply_desired(selected("small"), false)
            .expect("select model A");
        let first_context = first.reconcile.expect("A reconcile context");
        let cancellation = first_context.cancellation_token();
        let cleanup_calls = Arc::new(AtomicUsize::new(0));
        let cleanup_calls_from_worker = cleanup_calls.clone();
        let (cleanup_tx, cleanup_rx) = oneshot::channel();
        let worker = tokio::spawn(async move {
            cancellation.cancelled().await;
            cleanup_calls_from_worker.fetch_add(1, Ordering::SeqCst);
            let _ = cleanup_tx.send(());
        });

        let second = supervisor
            .apply_desired(selected("medium"), false)
            .expect("replace A with B");
        cleanup_rx.await.expect("worker cleanup hook ran");
        worker.await.expect("cleanup worker joined");

        assert!(first_context.is_cancelled());
        assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second.generation, 2);
        assert_eq!(
            supervisor
                .current_reconcile_context()
                .expect("B remains current")
                .generation(),
            second.generation
        );
    }

    #[tokio::test]
    async fn voice_supervisor_cancellation_disable_is_synchronous_before_worker_shutdown() {
        let (supervisor, _, _) = supervisor();
        let first = supervisor
            .apply_desired(selected("small"), false)
            .expect("select model");
        let first_context = first.reconcile.expect("reconcile context");
        assert!(
            supervisor
                .mark_downloading(first_context.generation())
                .expect("start download")
        );

        let disabled = supervisor
            .apply_desired(VoiceInputDesiredState::default(), false)
            .expect("disable voice input");
        assert!(disabled.changed);
        assert!(disabled.reconcile.is_none());
        assert!(first_context.is_cancelled());
        assert_phase(&supervisor, GatewayVoiceInputRuntimePhase::Disabled, None);
        assert_eq!(supervisor.loaded_model_identity(), None);
        assert!(supervisor.current_reconcile_context().is_none());

        assert!(
            !supervisor
                .report_download_progress(first_context.generation(), 90, Some(100))
                .expect("late progress is ignored")
        );
        assert!(
            !supervisor
                .mark_failed(first_context.generation(), "late worker failure")
                .expect("late failure is ignored")
        );
        assert_phase(&supervisor, GatewayVoiceInputRuntimePhase::Disabled, None);
    }

    #[tokio::test]
    async fn voice_supervisor_generation_concurrent_retries_are_serialized() {
        const RETRIES: usize = 16;

        let (supervisor, _, _) = supervisor();
        let supervisor = Arc::new(supervisor);
        supervisor
            .apply_desired(selected("small"), false)
            .expect("initial selection");
        let barrier = Arc::new(Barrier::new(RETRIES));
        let mut tasks = Vec::with_capacity(RETRIES);
        for _ in 0..RETRIES {
            let supervisor = supervisor.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let result = supervisor
                    .apply_desired(selected("small"), true)
                    .expect("concurrent retry");
                let context = result.reconcile.expect("retry reconcile context");
                (result.generation, context)
            }));
        }

        let mut generations_and_contexts = Vec::with_capacity(RETRIES);
        for task in tasks {
            generations_and_contexts.push(task.await.expect("retry task joined"));
        }
        generations_and_contexts.sort_by_key(|(generation, _)| *generation);
        let generations = generations_and_contexts
            .iter()
            .map(|(generation, _)| *generation)
            .collect::<Vec<_>>();
        assert_eq!(generations, (2..=(RETRIES as u64 + 1)).collect::<Vec<_>>());
        for (_, context) in &generations_and_contexts[..RETRIES - 1] {
            assert!(context.is_cancelled());
        }
        let (_, current) = generations_and_contexts.last().expect("last retry");
        assert!(!current.is_cancelled());
        assert_eq!(supervisor.generation(), RETRIES as u64 + 1);
        assert_eq!(
            supervisor
                .current_reconcile_context()
                .expect("one current reconcile")
                .generation(),
            RETRIES as u64 + 1
        );
    }

    #[tokio::test]
    async fn voice_supervisor_reconcile_disabled_does_no_io() {
        let temp = tempfile::tempdir().expect("temp dir");
        let layout = fake_layout(temp.path(), "small");
        let (supervisor, inspect_calls, install_calls, loader_calls) =
            reconcile_supervisor(FakeInstallMode::Missing(layout), false);
        let supervisor = Arc::new(supervisor);

        let result = supervisor
            .apply_desired(VoiceInputDesiredState::default(), false)
            .expect("disabled desired state");
        assert!(!result.changed);
        assert!(result.reconcile.is_none());
        assert_phase(&supervisor, GatewayVoiceInputRuntimePhase::Disabled, None);
        assert_eq!(inspect_calls.load(Ordering::SeqCst), 0);
        assert_eq!(install_calls.load(Ordering::SeqCst), 0);
        assert_eq!(loader_calls.load(Ordering::SeqCst), 0);
        assert!(!supervisor.has_loaded_engine());
    }

    #[test]
    fn voice_supervisor_restart_preserves_desired_selection_but_resets_transient_progress() {
        let (first, _, _) = supervisor();
        let selected = selected("small");
        let applied = first
            .apply_desired(selected.clone(), false)
            .expect("initial desired state");
        first
            .mark_downloading(applied.generation)
            .expect("downloading transition");
        first
            .report_download_progress(applied.generation, 40, Some(100))
            .expect("download progress");
        assert_eq!(first.runtime_snapshot().downloaded_bytes, Some(40));

        let (restarted, installer_calls, loader_calls) = supervisor();
        let restarted_apply = restarted
            .apply_desired(selected, false)
            .expect("persisted desired state after restart");

        assert!(restarted_apply.reconcile.is_some());
        assert_eq!(
            restarted.runtime_snapshot().phase,
            GatewayVoiceInputRuntimePhase::Missing
        );
        assert_eq!(restarted.runtime_snapshot().downloaded_bytes, None);
        assert_eq!(restarted.runtime_snapshot().total_bytes, None);
        assert_eq!(installer_calls.load(Ordering::SeqCst), 0);
        assert_eq!(loader_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn voice_supervisor_reconcile_valid_install_loads_before_ready() {
        let temp = tempfile::tempdir().expect("temp dir");
        let layout = fake_layout(temp.path(), "small");
        let (supervisor, inspect_calls, install_calls, loader_calls) =
            reconcile_supervisor(FakeInstallMode::Installed(layout), false);
        let supervisor = Arc::new(supervisor);
        let context = supervisor
            .apply_desired(selected("small"), false)
            .expect("select installed model")
            .reconcile
            .expect("reconcile context");

        supervisor.reconcile(context).await;

        assert_phase(
            &supervisor,
            GatewayVoiceInputRuntimePhase::Ready,
            Some("small"),
        );
        assert!(supervisor.runtime_snapshot().effective_enabled);
        assert!(supervisor.has_loaded_engine());
        assert_eq!(inspect_calls.load(Ordering::SeqCst), 1);
        assert_eq!(install_calls.load(Ordering::SeqCst), 0);
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn voice_supervisor_reconcile_missing_downloads_installs_loads_then_ready() {
        let temp = tempfile::tempdir().expect("temp dir");
        let layout = fake_layout(temp.path(), "small");
        let (supervisor, inspect_calls, install_calls, loader_calls) =
            reconcile_supervisor(FakeInstallMode::Missing(layout), false);
        let supervisor = Arc::new(supervisor);
        let context = supervisor
            .apply_desired(selected("small"), false)
            .expect("select missing model")
            .reconcile
            .expect("reconcile context");

        supervisor.reconcile(context).await;

        assert_phase(
            &supervisor,
            GatewayVoiceInputRuntimePhase::Ready,
            Some("small"),
        );
        assert!(supervisor.has_loaded_engine());
        assert_eq!(inspect_calls.load(Ordering::SeqCst), 1);
        assert_eq!(install_calls.load(Ordering::SeqCst), 1);
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn voice_supervisor_ready_requires_load_success() {
        let temp = tempfile::tempdir().expect("temp dir");
        let layout = fake_layout(temp.path(), "small");
        let (supervisor, _, install_calls, loader_calls) =
            reconcile_supervisor(FakeInstallMode::Installed(layout), true);
        let supervisor = Arc::new(supervisor);
        let context = supervisor
            .apply_desired(selected("small"), false)
            .expect("select model")
            .reconcile
            .expect("reconcile context");

        supervisor.reconcile(context).await;

        let snapshot = supervisor.runtime_snapshot();
        assert_eq!(snapshot.phase, GatewayVoiceInputRuntimePhase::Failed);
        assert!(!snapshot.effective_enabled);
        assert!(
            snapshot
                .error
                .as_deref()
                .is_some_and(|error| error.contains("eager load"))
        );
        assert!(!supervisor.has_loaded_engine());
        assert_eq!(install_calls.load(Ordering::SeqCst), 0);
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn voice_supervisor_reconcile_failures_are_bounded_and_leave_no_engine_or_transients() {
        for mode in [
            FakeInstallMode::VerifyFailure,
            FakeInstallMode::InstallFailure(fake_layout(
                tempfile::tempdir().expect("temp dir").path(),
                "small",
            )),
        ] {
            let transient_layout = match &mode {
                FakeInstallMode::InstallFailure(layout) => Some(layout.clone()),
                _ => None,
            };
            let (supervisor, _, _, _) = reconcile_supervisor(mode, false);
            let supervisor = Arc::new(supervisor);
            let context = supervisor
                .apply_desired(selected("small"), false)
                .expect("select model")
                .reconcile
                .expect("reconcile context");

            supervisor.reconcile(context).await;

            let snapshot = supervisor.runtime_snapshot();
            assert_eq!(snapshot.phase, GatewayVoiceInputRuntimePhase::Failed);
            assert!(!snapshot.effective_enabled);
            assert!(snapshot.error.as_ref().is_some_and(|error| {
                error.chars().count() <= MAX_VOICE_SUPERVISOR_ERROR_CHARS
            }));
            assert!(!supervisor.has_loaded_engine());
            if let Some(layout) = transient_layout {
                assert!(!layout.partial_archive_path.exists());
                assert!(!layout.archive_path.exists());
                assert!(!layout.staging_dir.exists());
            }
        }
    }

    #[tokio::test]
    async fn voice_supervisor_retry_forces_download_even_when_selected_install_was_valid() {
        let temp = tempfile::tempdir().expect("temp dir");
        let layout = fake_layout(temp.path(), "small");
        std::fs::create_dir_all(layout.install_dir.as_path()).expect("installed fixture");
        std::fs::write(layout.ready_marker_path.as_path(), b"ready").expect("ready fixture");
        let (supervisor, inspect_calls, force_fresh_calls, loader_calls) =
            retry_supervisor(layout.clone(), false);
        let supervisor = Arc::new(supervisor);

        let initial = supervisor
            .apply_desired(selected("small"), false)
            .expect("initial selection")
            .reconcile
            .expect("initial reconcile");
        supervisor.reconcile(initial).await;
        assert!(supervisor.has_loaded_engine());
        assert_eq!(inspect_calls.load(Ordering::SeqCst), 1);

        let retry = supervisor
            .apply_desired(selected("small"), true)
            .expect("retry selected model")
            .reconcile
            .expect("retry reconcile");
        assert!(retry.force_fresh());
        assert!(!supervisor.has_loaded_engine());
        supervisor.reconcile(retry).await;

        assert_eq!(inspect_calls.load(Ordering::SeqCst), 1);
        assert_eq!(force_fresh_calls.load(Ordering::SeqCst), 1);
        assert_eq!(loader_calls.load(Ordering::SeqCst), 2);
        assert_phase(
            &supervisor,
            GatewayVoiceInputRuntimePhase::Ready,
            Some("small"),
        );
    }

    #[tokio::test]
    async fn voice_supervisor_retry_stale_first_retry_cannot_win() {
        let temp = tempfile::tempdir().expect("temp dir");
        let layout = fake_layout(temp.path(), "small");
        let (supervisor, _, force_fresh_calls, loader_calls) = retry_supervisor(layout, false);
        let supervisor = Arc::new(supervisor);
        supervisor
            .apply_desired(selected("small"), false)
            .expect("initial selection");
        let first = supervisor
            .apply_desired(selected("small"), true)
            .expect("first retry")
            .reconcile
            .expect("first retry context");
        let second = supervisor
            .apply_desired(selected("small"), true)
            .expect("second retry")
            .reconcile
            .expect("second retry context");
        assert!(first.is_cancelled());

        supervisor.reconcile(first).await;
        assert_eq!(force_fresh_calls.load(Ordering::SeqCst), 0);
        supervisor.reconcile(second).await;

        assert_eq!(force_fresh_calls.load(Ordering::SeqCst), 1);
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
        assert_eq!(supervisor.generation(), 3);
        assert!(supervisor.runtime_snapshot().effective_enabled);
    }

    #[tokio::test]
    async fn voice_supervisor_retry_failure_removes_ready_ownership_and_selected_marker() {
        let temp = tempfile::tempdir().expect("temp dir");
        let layout = fake_layout(temp.path(), "small");
        std::fs::create_dir_all(layout.install_dir.as_path()).expect("installed fixture");
        std::fs::write(layout.ready_marker_path.as_path(), b"ready").expect("ready fixture");
        let (supervisor, _, force_fresh_calls, _) = retry_supervisor(layout.clone(), true);
        let supervisor = Arc::new(supervisor);
        let retry = supervisor
            .apply_desired(selected("small"), true)
            .expect("retry selected model")
            .reconcile
            .expect("retry context");

        supervisor.reconcile(retry).await;

        assert_eq!(force_fresh_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            supervisor.runtime_snapshot().phase,
            GatewayVoiceInputRuntimePhase::Failed
        );
        assert!(!supervisor.runtime_snapshot().effective_enabled);
        assert!(!supervisor.has_loaded_engine());
        assert!(!layout.ready_marker_path.exists());
        assert!(!layout.install_dir.exists());
    }

    #[test]
    fn voice_supervisor_retry_requires_enabled_trusted_selected_model() {
        let (supervisor, _, _) = supervisor();
        let missing = match supervisor.apply_desired(VoiceInputDesiredState::default(), true) {
            Ok(_) => panic!("retry without selection must fail"),
            Err(error) => error,
        };
        assert_eq!(
            missing,
            VoiceSupervisorTransitionError::SelectedModelRequired
        );
        let unknown = match supervisor.apply_desired(selected("not-in-the-catalog"), true) {
            Ok(_) => panic!("retry for unknown model must fail"),
            Err(error) => error,
        };
        assert!(matches!(
            unknown,
            VoiceSupervisorTransitionError::UnknownModel(model)
                if model == "not-in-the-catalog"
        ));
        assert_eq!(supervisor.generation(), 0);
        assert_phase(&supervisor, GatewayVoiceInputRuntimePhase::Disabled, None);
    }

    #[tokio::test]
    async fn voice_model_replacement_cleanup_runs_only_after_current_ready_publication() {
        let temp = tempfile::tempdir().expect("temp dir");
        let cleanup_calls = Arc::new(AtomicUsize::new(0));
        let loader_calls = Arc::new(AtomicUsize::new(0));
        let supervisor = Arc::new(VoiceInputSupervisor::new(
            Arc::new(CleanupTrackingInstaller {
                layout: fake_layout(temp.path(), "medium"),
                cleanup_calls: cleanup_calls.clone(),
            }),
            Arc::new(FakeEngineLoader {
                calls: loader_calls,
                fail: false,
            }),
        ));
        let context = supervisor
            .apply_desired(selected("medium"), false)
            .expect("select replacement")
            .reconcile
            .expect("replacement context");
        assert_eq!(cleanup_calls.load(Ordering::SeqCst), 0);

        supervisor.reconcile(context).await;

        assert_eq!(
            supervisor.runtime_snapshot().phase,
            GatewayVoiceInputRuntimePhase::Ready
        );
        assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn voice_model_replacement_cleanup_protection_tracks_the_latest_selection() {
        let (supervisor, _, _) = supervisor();

        supervisor
            .apply_desired(selected("medium"), false)
            .expect("select committed model");
        assert_eq!(
            supervisor.cleanup_protected_model_id().as_deref(),
            Some("medium")
        );

        supervisor
            .apply_desired(selected("small"), false)
            .expect("select replacement model");
        assert_eq!(
            supervisor.cleanup_protected_model_id().as_deref(),
            Some("small")
        );

        supervisor
            .apply_desired(VoiceInputDesiredState::default(), false)
            .expect("disable voice input");
        assert_eq!(supervisor.cleanup_protected_model_id(), None);
    }

    #[tokio::test]
    async fn voice_model_replacement_cleanup_finishes_after_post_ready_disable() {
        let temp = tempfile::tempdir().expect("temp dir");
        let (cleanup_started_tx, cleanup_started_rx) = oneshot::channel();
        let (cleanup_release_tx, cleanup_release_rx) = std::sync::mpsc::channel();
        let cleanup_saw_cancellation = Arc::new(AtomicBool::new(false));
        let supervisor = Arc::new(VoiceInputSupervisor::new(
            Arc::new(BlockingCleanupInstaller {
                layout: fake_layout(temp.path(), "medium"),
                cleanup_started: std::sync::Mutex::new(Some(cleanup_started_tx)),
                cleanup_release: std::sync::Mutex::new(cleanup_release_rx),
                cleanup_saw_cancellation: cleanup_saw_cancellation.clone(),
            }),
            Arc::new(FakeEngineLoader {
                calls: Arc::new(AtomicUsize::new(0)),
                fail: false,
            }),
        ));
        let context = supervisor
            .apply_desired(selected("medium"), false)
            .expect("select replacement")
            .reconcile
            .expect("replacement context");
        let worker = {
            let supervisor = supervisor.clone();
            tokio::spawn(async move { supervisor.reconcile(context).await })
        };

        cleanup_started_rx
            .await
            .expect("post-Ready cleanup started");
        assert_eq!(
            supervisor.runtime_snapshot().phase,
            GatewayVoiceInputRuntimePhase::Ready
        );
        supervisor
            .apply_desired(VoiceInputDesiredState::default(), false)
            .expect("disable after Ready");
        cleanup_release_tx
            .send(())
            .expect("release post-Ready cleanup");
        worker.await.expect("reconcile worker joined");

        assert_eq!(
            supervisor.runtime_snapshot().phase,
            GatewayVoiceInputRuntimePhase::Disabled
        );
        assert!(!cleanup_saw_cancellation.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn voice_model_replacement_cleanup_is_skipped_for_failed_or_stale_replacement() {
        for loader_fails in [true, false] {
            let temp = tempfile::tempdir().expect("temp dir");
            let cleanup_calls = Arc::new(AtomicUsize::new(0));
            let loader_calls = Arc::new(AtomicUsize::new(0));
            let supervisor = Arc::new(VoiceInputSupervisor::new(
                Arc::new(CleanupTrackingInstaller {
                    layout: fake_layout(temp.path(), "medium"),
                    cleanup_calls: cleanup_calls.clone(),
                }),
                Arc::new(FakeEngineLoader {
                    calls: loader_calls,
                    fail: loader_fails,
                }),
            ));
            let context = supervisor
                .apply_desired(selected("medium"), false)
                .expect("select replacement")
                .reconcile
                .expect("replacement context");
            if !loader_fails {
                supervisor
                    .apply_desired(VoiceInputDesiredState::default(), false)
                    .expect("cancel replacement");
                assert!(context.is_cancelled());
            }

            supervisor.reconcile(context).await;

            assert_eq!(cleanup_calls.load(Ordering::SeqCst), 0);
            assert!(!supervisor.runtime_snapshot().effective_enabled);
        }
    }

    fn supervisor() -> (VoiceInputSupervisor, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let installer_calls = Arc::new(AtomicUsize::new(0));
        let loader_calls = Arc::new(AtomicUsize::new(0));
        let supervisor = VoiceInputSupervisor::new(
            Arc::new(NoIoInstaller {
                calls: installer_calls.clone(),
            }),
            Arc::new(NoIoEngineLoader {
                calls: loader_calls.clone(),
            }),
        );
        (supervisor, installer_calls, loader_calls)
    }

    fn reconcile_supervisor(
        mode: FakeInstallMode,
        loader_fails: bool,
    ) -> (
        VoiceInputSupervisor,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let inspect_calls = Arc::new(AtomicUsize::new(0));
        let install_calls = Arc::new(AtomicUsize::new(0));
        let loader_calls = Arc::new(AtomicUsize::new(0));
        let supervisor = VoiceInputSupervisor::new(
            Arc::new(FakeInstaller {
                mode,
                inspect_calls: inspect_calls.clone(),
                install_calls: install_calls.clone(),
            }),
            Arc::new(FakeEngineLoader {
                calls: loader_calls.clone(),
                fail: loader_fails,
            }),
        );
        (supervisor, inspect_calls, install_calls, loader_calls)
    }

    fn retry_supervisor(
        layout: VoiceModelInstallLayout,
        install_fails: bool,
    ) -> (
        VoiceInputSupervisor,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let inspect_calls = Arc::new(AtomicUsize::new(0));
        let force_fresh_calls = Arc::new(AtomicUsize::new(0));
        let loader_calls = Arc::new(AtomicUsize::new(0));
        let supervisor = VoiceInputSupervisor::new(
            Arc::new(RetryInstaller {
                layout,
                inspect_calls: inspect_calls.clone(),
                force_fresh_calls: force_fresh_calls.clone(),
                fail: install_fails,
            }),
            Arc::new(FakeEngineLoader {
                calls: loader_calls.clone(),
                fail: false,
            }),
        );
        (supervisor, inspect_calls, force_fresh_calls, loader_calls)
    }

    fn fake_layout(root: &Path, model: &str) -> VoiceModelInstallLayout {
        let models_root = root.join("voice");
        let downloads_dir = models_root.join("downloads");
        let install_dir = models_root.join(model);
        VoiceModelInstallLayout {
            archive_path: downloads_dir.join(format!("{model}.tar.gz")),
            partial_archive_path: downloads_dir.join(format!("{model}.tar.gz.partial")),
            staging_dir: models_root.join(format!("{model}.staging")),
            model_data_dir: install_dir.clone(),
            ready_marker_path: install_dir.join(".ready"),
            models_root,
            downloads_dir,
            install_dir,
        }
    }

    fn supervisor_at(
        phase: GatewayVoiceInputRuntimePhase,
    ) -> (VoiceInputSupervisor, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let (supervisor, installer_calls, loader_calls) = supervisor();
        supervisor
            .apply_desired(selected("small"), false)
            .expect("select model");
        match phase {
            GatewayVoiceInputRuntimePhase::Missing => {}
            GatewayVoiceInputRuntimePhase::Downloading => {
                mark_downloading(&supervisor).expect("download");
            }
            GatewayVoiceInputRuntimePhase::Installing => {
                mark_downloading(&supervisor).expect("download");
                mark_installing(&supervisor).expect("install");
            }
            GatewayVoiceInputRuntimePhase::Loading => {
                mark_loading(&supervisor).expect("loading");
            }
            GatewayVoiceInputRuntimePhase::Ready => {
                mark_loading(&supervisor).expect("loading");
                mark_ready(&supervisor, identity("small")).expect("ready");
            }
            other => panic!("unsupported test phase {other:?}"),
        }
        (supervisor, installer_calls, loader_calls)
    }

    fn mark_downloading(
        supervisor: &VoiceInputSupervisor,
    ) -> std::result::Result<bool, VoiceSupervisorTransitionError> {
        supervisor.mark_downloading(supervisor.generation())
    }

    fn report_download_progress(
        supervisor: &VoiceInputSupervisor,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
    ) -> std::result::Result<bool, VoiceSupervisorTransitionError> {
        supervisor.report_download_progress(supervisor.generation(), downloaded_bytes, total_bytes)
    }

    fn mark_installing(
        supervisor: &VoiceInputSupervisor,
    ) -> std::result::Result<bool, VoiceSupervisorTransitionError> {
        supervisor.mark_installing(supervisor.generation())
    }

    fn mark_loading(
        supervisor: &VoiceInputSupervisor,
    ) -> std::result::Result<bool, VoiceSupervisorTransitionError> {
        supervisor.mark_loading(supervisor.generation())
    }

    fn mark_ready(
        supervisor: &VoiceInputSupervisor,
        identity: VoiceModelIdentity,
    ) -> std::result::Result<bool, VoiceSupervisorTransitionError> {
        supervisor.mark_ready(
            supervisor.generation(),
            identity,
            LoadedVoiceEngine::test_stub(),
        )
    }

    fn mark_failed(
        supervisor: &VoiceInputSupervisor,
        error: impl Into<String>,
    ) -> std::result::Result<bool, VoiceSupervisorTransitionError> {
        supervisor.mark_failed(supervisor.generation(), error)
    }

    fn selected(model: &str) -> VoiceInputDesiredState {
        VoiceInputDesiredState {
            enabled: true,
            provider: Some(GatewayVoiceInputProvider::Local),
            model: Some(model.to_owned()),
        }
    }

    fn identity(model: &str) -> VoiceModelIdentity {
        VoiceModelIdentity {
            provider: GatewayVoiceInputProvider::Local,
            model: model.to_owned(),
        }
    }

    fn assert_phase(
        supervisor: &VoiceInputSupervisor,
        phase: GatewayVoiceInputRuntimePhase,
        model: Option<&str>,
    ) {
        let snapshot = supervisor.runtime_snapshot();
        assert_eq!(snapshot.phase, phase);
        assert_eq!(snapshot.model.as_deref(), model);
        assert!(!snapshot.effective_enabled || phase == GatewayVoiceInputRuntimePhase::Ready);
    }
}
