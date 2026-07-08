#![allow(dead_code)]

use memvid_core::{LocalTextEmbedder, TextEmbedConfig};
use pioneer_config::{
    GatewayThreadEpisodicVectorProviderConfig, GatewayThreadEpisodicVectorSearchConfig,
};
use pioneer_memory::{
    ThreadEpisodicEmbeddingError, ThreadEpisodicEmbeddingFailureClass,
    ThreadEpisodicEmbeddingProvider, ThreadEpisodicMemvidEmbedder,
};
use pioneer_protocol::{
    GatewayThreadEpisodicVectorLocalModelStatus, GatewayThreadEpisodicVectorProvider,
    GatewayThreadEpisodicVectorSearchSettings,
};
use pioneer_provider::{
    EmbeddingRequest, Provider,
    providers::{LocalEmbeddingModelInfo, local_embedding_model_info},
};
use reqwest::blocking::Client as BlockingClient;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const OPENAI_PROVIDER_ID: &str = "openai";
const OPENROUTER_PROVIDER_ID: &str = "openrouter";
const LOCAL_PROVIDER_ID: &str = "local";
const LOCAL_EMBEDDING_MODELS_RELATIVE_DIR: &[&str] = &["models", "embedding", "text"];
const LOCAL_EMBEDDING_DOWNLOAD_TIMEOUT_SECS: u64 = 15 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OpenAiEmbeddingModelInfo {
    pub model: &'static str,
    pub dimension: usize,
    pub max_batch_size: usize,
    pub legacy: bool,
}

pub(crate) const OPENAI_EMBEDDING_MODELS: &[OpenAiEmbeddingModelInfo] = &[
    OpenAiEmbeddingModelInfo {
        model: "text-embedding-3-small",
        dimension: 1536,
        max_batch_size: 2048,
        legacy: false,
    },
    OpenAiEmbeddingModelInfo {
        model: "text-embedding-3-large",
        dimension: 3072,
        max_batch_size: 2048,
        legacy: false,
    },
    OpenAiEmbeddingModelInfo {
        model: "text-embedding-ada-002",
        dimension: 1536,
        max_batch_size: 2048,
        legacy: true,
    },
];

pub(crate) fn openai_embedding_model_info(
    model: &str,
) -> Option<&'static OpenAiEmbeddingModelInfo> {
    OPENAI_EMBEDDING_MODELS
        .iter()
        .find(|candidate| candidate.model == model)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpenRouterEmbeddingModelInfo {
    pub model: String,
    pub dimension: usize,
    pub max_batch_size: usize,
    pub custom: bool,
}

pub(crate) const OPENROUTER_KNOWN_EMBEDDING_MODELS: &[OpenAiEmbeddingModelInfo] = &[
    OpenAiEmbeddingModelInfo {
        model: "openai/text-embedding-3-small",
        dimension: 1536,
        max_batch_size: 2048,
        legacy: false,
    },
    OpenAiEmbeddingModelInfo {
        model: "openai/text-embedding-3-large",
        dimension: 3072,
        max_batch_size: 2048,
        legacy: false,
    },
];

pub(crate) fn openrouter_embedding_model_info(
    model: &str,
    explicit_dimension: Option<usize>,
) -> Result<OpenRouterEmbeddingModelInfo, ThreadEpisodicEmbeddingError> {
    if let Some(info) = OPENROUTER_KNOWN_EMBEDDING_MODELS
        .iter()
        .find(|candidate| candidate.model == model)
    {
        return Ok(OpenRouterEmbeddingModelInfo {
            model: info.model.to_owned(),
            dimension: info.dimension,
            max_batch_size: info.max_batch_size,
            custom: false,
        });
    }

    let Some(dimension) = explicit_dimension.filter(|dimension| *dimension > 0) else {
        return Err(
            ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                OPENROUTER_PROVIDER_ID,
                model,
                "custom OpenRouter embedding model requires an explicit dimension before refill",
            ),
        );
    };

    Ok(OpenRouterEmbeddingModelInfo {
        model: model.to_owned(),
        dimension,
        max_batch_size: 512,
        custom: true,
    })
}

pub(crate) fn local_embedding_models_root(runtime_home: &Path) -> PathBuf {
    LOCAL_EMBEDDING_MODELS_RELATIVE_DIR
        .iter()
        .fold(runtime_home.to_path_buf(), |path, component| {
            path.join(component)
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalEmbeddingModelFiles {
    pub models_dir: PathBuf,
    pub model_path: PathBuf,
    pub tokenizer_path: PathBuf,
    pub download_marker_path: PathBuf,
    pub failure_marker_path: PathBuf,
}

pub(crate) fn local_embedding_model_files(
    runtime_home: &Path,
    model: &str,
) -> Option<LocalEmbeddingModelFiles> {
    let info = local_embedding_model_info(model)?;
    let models_dir = local_embedding_models_root(runtime_home);
    Some(LocalEmbeddingModelFiles {
        model_path: models_dir.join(format!("{}.onnx", info.id)),
        tokenizer_path: models_dir.join(format!("{}_tokenizer.json", info.id)),
        download_marker_path: models_dir.join(format!("{}.download", info.id)),
        failure_marker_path: models_dir.join(format!("{}.failed", info.id)),
        models_dir,
    })
}

pub(crate) fn local_embedding_model_status(
    runtime_home: &Path,
    vector_enabled: bool,
    provider: Option<GatewayThreadEpisodicVectorProvider>,
    model: &str,
) -> GatewayThreadEpisodicVectorLocalModelStatus {
    if !vector_enabled || provider != Some(GatewayThreadEpisodicVectorProvider::Local) {
        return GatewayThreadEpisodicVectorLocalModelStatus::NotSelected;
    }

    if model.trim().is_empty() {
        return GatewayThreadEpisodicVectorLocalModelStatus::NotSelected;
    }

    let Some(files) = local_embedding_model_files(runtime_home, model) else {
        return GatewayThreadEpisodicVectorLocalModelStatus::Failed;
    };

    if files.model_path.exists() && files.tokenizer_path.exists() {
        return GatewayThreadEpisodicVectorLocalModelStatus::Installed;
    }
    if files.download_marker_path.exists() {
        return GatewayThreadEpisodicVectorLocalModelStatus::Downloading;
    }
    if files.failure_marker_path.exists() {
        return GatewayThreadEpisodicVectorLocalModelStatus::Failed;
    }

    GatewayThreadEpisodicVectorLocalModelStatus::Missing
}

pub(crate) fn spawn_local_embedding_model_download_if_needed(
    runtime_home: &Path,
    config: &GatewayThreadEpisodicVectorSearchConfig,
) -> std::result::Result<bool, String> {
    let Some((files, info)) = prepare_local_embedding_model_download(runtime_home, config)? else {
        return Ok(false);
    };

    let files_for_task = files.clone();
    tokio::task::spawn_blocking(move || {
        let result = download_local_embedding_model_files(&files_for_task, info);
        let _ = complete_local_embedding_model_download(&files_for_task, result);
    });

    Ok(true)
}

pub(crate) async fn ensure_local_embedding_model_downloaded_if_needed(
    runtime_home: &Path,
    config: &GatewayThreadEpisodicVectorSearchConfig,
) -> std::result::Result<bool, String> {
    let Some((files, info)) = prepare_local_embedding_model_download(runtime_home, config)? else {
        return Ok(false);
    };

    let files_for_task = files.clone();
    let result = tokio::task::spawn_blocking(move || {
        download_local_embedding_model_files(&files_for_task, info)
    })
    .await
    .map_err(|error| format!("local embedding model download task failed: {error}"))?;
    complete_local_embedding_model_download(&files, result)?;
    Ok(true)
}

fn prepare_local_embedding_model_download(
    runtime_home: &Path,
    config: &GatewayThreadEpisodicVectorSearchConfig,
) -> std::result::Result<Option<(LocalEmbeddingModelFiles, &'static LocalEmbeddingModelInfo)>, String>
{
    if !config.enabled || config.provider != Some(GatewayThreadEpisodicVectorProviderConfig::Local)
    {
        return Ok(None);
    }

    let Some(model) = selected_local_embedding_model(config) else {
        return Err("local embedding model is not selected".to_owned());
    };
    let info = local_embedding_model_info(model)
        .ok_or_else(|| format!("unknown local embedding model `{model}`"))?;
    let files = local_embedding_model_files(runtime_home, model)
        .ok_or_else(|| format!("unknown local embedding model `{model}`"))?;

    if files.model_path.exists() && files.tokenizer_path.exists() {
        return Ok(None);
    }

    std::fs::create_dir_all(files.models_dir.as_path()).map_err(|error| {
        format!(
            "failed to create local embedding model directory {}: {error}",
            files.models_dir.display()
        )
    })?;

    if files.download_marker_path.exists() {
        return Ok(None);
    }

    let mut marker = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(files.download_marker_path.as_path())
    {
        Ok(marker) => marker,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(None),
        Err(error) => {
            return Err(format!(
                "failed to create local embedding model download marker {}: {error}",
                files.download_marker_path.display()
            ));
        }
    };
    marker.write_all(info.id.as_bytes()).map_err(|error| {
        format!("failed to write local embedding model download marker: {error}")
    })?;
    drop(marker);

    Ok(Some((files, info)))
}

fn selected_local_embedding_model(
    config: &GatewayThreadEpisodicVectorSearchConfig,
) -> Option<&str> {
    config
        .model
        .as_deref()
        .or(config.local_model.as_deref())
        .map(str::trim)
        .filter(|model| !model.is_empty())
}

fn complete_local_embedding_model_download(
    files: &LocalEmbeddingModelFiles,
    result: std::result::Result<(), String>,
) -> std::result::Result<(), String> {
    let _ = std::fs::remove_file(files.download_marker_path.as_path());
    match result {
        Ok(()) => {
            let _ = std::fs::remove_file(files.failure_marker_path.as_path());
            Ok(())
        }
        Err(error) => {
            let _ = std::fs::remove_file(files.model_path.as_path());
            let _ = std::fs::remove_file(files.tokenizer_path.as_path());
            let _ = std::fs::write(files.failure_marker_path.as_path(), error.as_bytes());
            Err(error)
        }
    }
}

fn download_local_embedding_model_files(
    files: &LocalEmbeddingModelFiles,
    info: &LocalEmbeddingModelInfo,
) -> std::result::Result<(), String> {
    std::fs::create_dir_all(files.models_dir.as_path()).map_err(|error| {
        format!(
            "failed to create local embedding model directory {}: {error}",
            files.models_dir.display()
        )
    })?;
    let _ = std::fs::remove_file(files.failure_marker_path.as_path());

    let client = BlockingClient::builder()
        .timeout(Duration::from_secs(LOCAL_EMBEDDING_DOWNLOAD_TIMEOUT_SECS))
        .build()
        .map_err(|error| {
            format!("failed to initialize local embedding download client: {error}")
        })?;

    download_local_embedding_model_file(
        &client,
        info.model_url,
        files.model_path.as_path(),
        files
            .models_dir
            .join(format!("{}.onnx.partial", info.id))
            .as_path(),
    )
    .and_then(|()| {
        download_local_embedding_model_file(
            &client,
            info.tokenizer_url,
            files.tokenizer_path.as_path(),
            files
                .models_dir
                .join(format!("{}_tokenizer.json.partial", info.id))
                .as_path(),
        )
    })
}

fn download_local_embedding_model_file(
    client: &BlockingClient,
    url: &str,
    destination_path: &Path,
    partial_path: &Path,
) -> std::result::Result<(), String> {
    let _ = std::fs::remove_file(partial_path);
    let mut response = client
        .get(url)
        .send()
        .map_err(|error| format!("failed to download {url}: {error}"))?
        .error_for_status()
        .map_err(|error| format!("failed to download {url}: {error}"))?;
    let mut partial_file = File::create(partial_path).map_err(|error| {
        format!(
            "failed to create local embedding model partial file {}: {error}",
            partial_path.display()
        )
    })?;
    std::io::copy(&mut response, &mut partial_file).map_err(|error| {
        format!(
            "failed to write local embedding model partial file {}: {error}",
            partial_path.display()
        )
    })?;
    partial_file.sync_all().map_err(|error| {
        format!(
            "failed to sync local embedding model partial file {}: {error}",
            partial_path.display()
        )
    })?;
    drop(partial_file);
    std::fs::rename(partial_path, destination_path).map_err(|error| {
        format!(
            "failed to install local embedding model file {}: {error}",
            destination_path.display()
        )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EmbeddingProviderReadinessState {
    Disabled,
    Ready,
    MissingConfiguration,
    MissingLocalModel,
    LocalModelDownloading,
    RetryableProviderFailure,
    PermanentProviderFailure,
    DimensionMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EmbeddingProviderReadinessDiagnostic {
    pub state: EmbeddingProviderReadinessState,
    pub provider_id: String,
    pub model: String,
    pub retryable: bool,
    pub message: String,
}

impl EmbeddingProviderReadinessDiagnostic {
    fn new(
        state: EmbeddingProviderReadinessState,
        provider_id: impl Into<String>,
        model: impl Into<String>,
        retryable: bool,
        message: impl Into<String>,
    ) -> Self {
        Self {
            state,
            provider_id: provider_id.into(),
            model: model.into(),
            retryable,
            message: message.into(),
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.state == EmbeddingProviderReadinessState::Ready
    }
}

pub(crate) fn embedding_provider_readiness_from_settings(
    settings: &GatewayThreadEpisodicVectorSearchSettings,
) -> EmbeddingProviderReadinessDiagnostic {
    let provider_id = vector_provider_id(settings.provider);
    let model = selected_embedding_model(settings);

    if !settings.enabled {
        return EmbeddingProviderReadinessDiagnostic::new(
            EmbeddingProviderReadinessState::Disabled,
            provider_id,
            model,
            false,
            "vector search is disabled",
        );
    }

    if settings.provider.is_none() {
        return EmbeddingProviderReadinessDiagnostic::new(
            EmbeddingProviderReadinessState::MissingConfiguration,
            provider_id,
            model,
            false,
            "embedding provider is not selected",
        );
    }

    if settings.provider_key.required && !settings.provider_key.present {
        return EmbeddingProviderReadinessDiagnostic::new(
            EmbeddingProviderReadinessState::MissingConfiguration,
            provider_id,
            model,
            false,
            format!("{provider_id} embedding API key is missing"),
        );
    }

    if settings.provider == Some(GatewayThreadEpisodicVectorProvider::Local) {
        match settings.local_model_status {
            GatewayThreadEpisodicVectorLocalModelStatus::Installed => {}
            GatewayThreadEpisodicVectorLocalModelStatus::Downloading => {
                return EmbeddingProviderReadinessDiagnostic::new(
                    EmbeddingProviderReadinessState::LocalModelDownloading,
                    provider_id,
                    model,
                    true,
                    "local embedding model is still downloading",
                );
            }
            GatewayThreadEpisodicVectorLocalModelStatus::Failed => {
                return EmbeddingProviderReadinessDiagnostic::new(
                    EmbeddingProviderReadinessState::PermanentProviderFailure,
                    provider_id,
                    model,
                    false,
                    "local embedding model is failed",
                );
            }
            GatewayThreadEpisodicVectorLocalModelStatus::Missing
            | GatewayThreadEpisodicVectorLocalModelStatus::Unknown
            | GatewayThreadEpisodicVectorLocalModelStatus::NotSelected => {
                return EmbeddingProviderReadinessDiagnostic::new(
                    EmbeddingProviderReadinessState::MissingLocalModel,
                    provider_id,
                    model,
                    false,
                    "local embedding model is not installed",
                );
            }
        }
    }

    if settings.embedding_dimension.is_none()
        && settings.provider != Some(GatewayThreadEpisodicVectorProvider::OpenRouter)
    {
        return EmbeddingProviderReadinessDiagnostic::new(
            EmbeddingProviderReadinessState::MissingConfiguration,
            provider_id,
            model,
            false,
            "embedding dimension is unknown for the selected model",
        );
    }

    EmbeddingProviderReadinessDiagnostic::new(
        EmbeddingProviderReadinessState::Ready,
        provider_id,
        model,
        false,
        "embedding provider is ready",
    )
}

pub(crate) fn embedding_provider_readiness_from_error(
    error: &ThreadEpisodicEmbeddingError,
) -> EmbeddingProviderReadinessDiagnostic {
    let (state, retryable) = match error.failure_class() {
        ThreadEpisodicEmbeddingFailureClass::Configuration => {
            (EmbeddingProviderReadinessState::MissingConfiguration, false)
        }
        ThreadEpisodicEmbeddingFailureClass::RetryableProviderFailure => (
            EmbeddingProviderReadinessState::RetryableProviderFailure,
            true,
        ),
        ThreadEpisodicEmbeddingFailureClass::PermanentProviderFailure => (
            EmbeddingProviderReadinessState::PermanentProviderFailure,
            false,
        ),
        ThreadEpisodicEmbeddingFailureClass::DimensionMismatch => {
            (EmbeddingProviderReadinessState::DimensionMismatch, false)
        }
    };

    EmbeddingProviderReadinessDiagnostic::new(
        state,
        error.provider_id.clone(),
        error.model.clone(),
        retryable,
        error.message.clone(),
    )
}

fn selected_embedding_model(settings: &GatewayThreadEpisodicVectorSearchSettings) -> String {
    if settings.provider == Some(GatewayThreadEpisodicVectorProvider::Local) {
        settings
            .model
            .clone()
            .or_else(|| settings.local_model.clone())
            .unwrap_or_default()
    } else {
        settings.model.clone().unwrap_or_default()
    }
}

fn vector_provider_id(provider: Option<GatewayThreadEpisodicVectorProvider>) -> &'static str {
    match provider {
        Some(GatewayThreadEpisodicVectorProvider::OpenAi) => OPENAI_PROVIDER_ID,
        Some(GatewayThreadEpisodicVectorProvider::OpenRouter) => OPENROUTER_PROVIDER_ID,
        Some(GatewayThreadEpisodicVectorProvider::Local) => LOCAL_PROVIDER_ID,
        None => "none",
    }
}

trait LocalEmbeddingRuntime: Send + Sync {
    fn embed_text(&self, text: &str) -> Result<Vec<f32>, ThreadEpisodicEmbeddingError>;

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ThreadEpisodicEmbeddingError>;
}

trait LocalEmbeddingRuntimeFactory: Send + Sync {
    fn create(
        &self,
        config: TextEmbedConfig,
    ) -> Result<Arc<dyn LocalEmbeddingRuntime>, ThreadEpisodicEmbeddingError>;
}

struct MemvidLocalEmbeddingRuntime {
    inner: LocalTextEmbedder,
    model: String,
}

impl LocalEmbeddingRuntime for MemvidLocalEmbeddingRuntime {
    fn embed_text(&self, text: &str) -> Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
        self.inner.encode_text(text).map_err(|error| {
            ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                LOCAL_PROVIDER_ID,
                self.model.as_str(),
                format!("local embedding failed: {error}"),
            )
        })
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ThreadEpisodicEmbeddingError> {
        self.inner.encode_batch(texts).map_err(|error| {
            ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                LOCAL_PROVIDER_ID,
                self.model.as_str(),
                format!("local embedding batch failed: {error}"),
            )
        })
    }
}

struct MemvidLocalEmbeddingRuntimeFactory;

impl LocalEmbeddingRuntimeFactory for MemvidLocalEmbeddingRuntimeFactory {
    fn create(
        &self,
        config: TextEmbedConfig,
    ) -> Result<Arc<dyn LocalEmbeddingRuntime>, ThreadEpisodicEmbeddingError> {
        let model = config.model_name.clone();
        let inner = LocalTextEmbedder::new(config).map_err(|error| {
            ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                LOCAL_PROVIDER_ID,
                model.as_str(),
                format!("failed to initialize local embedding provider: {error}"),
            )
        })?;
        Ok(Arc::new(MemvidLocalEmbeddingRuntime { inner, model }))
    }
}

#[derive(Clone)]
pub(crate) struct LocalEmbeddingProvider {
    model_info: &'static LocalEmbeddingModelInfo,
    models_dir: PathBuf,
    normalized: bool,
    runtime: Arc<Mutex<Option<Arc<dyn LocalEmbeddingRuntime>>>>,
    runtime_factory: Arc<dyn LocalEmbeddingRuntimeFactory>,
}

impl LocalEmbeddingProvider {
    pub(crate) fn from_runtime_home(
        runtime_home: &Path,
        model: &str,
        normalized: bool,
    ) -> Result<Self, ThreadEpisodicEmbeddingError> {
        let model_info = local_embedding_model_info(model)
            .ok_or_else(|| ThreadEpisodicEmbeddingError::missing_model(LOCAL_PROVIDER_ID, model))?;

        Ok(Self {
            model_info,
            models_dir: local_embedding_models_root(runtime_home),
            normalized,
            runtime: Arc::new(Mutex::new(None)),
            runtime_factory: Arc::new(MemvidLocalEmbeddingRuntimeFactory),
        })
    }

    #[cfg(test)]
    fn with_runtime_factory(
        runtime_home: &Path,
        model: &str,
        normalized: bool,
        runtime_factory: Arc<dyn LocalEmbeddingRuntimeFactory>,
    ) -> Result<Self, ThreadEpisodicEmbeddingError> {
        let mut provider = Self::from_runtime_home(runtime_home, model, normalized)?;
        provider.runtime_factory = runtime_factory;
        Ok(provider)
    }

    pub(crate) fn memvid_embedder(self: Arc<Self>) -> ThreadEpisodicMemvidEmbedder {
        ThreadEpisodicMemvidEmbedder::new(self)
    }

    fn runtime_initialized(&self) -> bool {
        self.runtime
            .lock()
            .map(|runtime| runtime.is_some())
            .unwrap_or(false)
    }

    fn files(&self) -> LocalEmbeddingModelFiles {
        LocalEmbeddingModelFiles {
            model_path: self.models_dir.join(format!("{}.onnx", self.model_info.id)),
            tokenizer_path: self
                .models_dir
                .join(format!("{}_tokenizer.json", self.model_info.id)),
            download_marker_path: self
                .models_dir
                .join(format!("{}.download", self.model_info.id)),
            failure_marker_path: self
                .models_dir
                .join(format!("{}.failed", self.model_info.id)),
            models_dir: self.models_dir.clone(),
        }
    }

    fn ensure_runtime(
        &self,
    ) -> Result<Arc<dyn LocalEmbeddingRuntime>, ThreadEpisodicEmbeddingError> {
        let files = self.files();
        if !files.model_path.exists() || !files.tokenizer_path.exists() {
            return Err(ThreadEpisodicEmbeddingError::missing_model(
                LOCAL_PROVIDER_ID,
                self.model(),
            ));
        }

        let mut runtime = self.runtime.lock().map_err(|_| {
            ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                LOCAL_PROVIDER_ID,
                self.model(),
                "failed to lock local embedding runtime",
            )
        })?;
        if let Some(runtime) = runtime.as_ref() {
            return Ok(runtime.clone());
        }

        let created = self.runtime_factory.create(TextEmbedConfig {
            model_name: self.model_info.id.to_owned(),
            models_dir: self.models_dir.clone(),
            offline: true,
            enable_cache: true,
            cache_capacity: 1000,
        })?;
        *runtime = Some(created.clone());
        Ok(created)
    }
}

impl Debug for LocalEmbeddingProvider {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalEmbeddingProvider")
            .field("model", &self.model_info.id)
            .field("dimension", &self.model_info.dimension)
            .field("normalized", &self.normalized)
            .field("models_dir", &self.models_dir)
            .field("runtime_initialized", &self.runtime_initialized())
            .finish()
    }
}

impl ThreadEpisodicEmbeddingProvider for LocalEmbeddingProvider {
    fn provider_id(&self) -> &str {
        LOCAL_PROVIDER_ID
    }

    fn model(&self) -> &str {
        self.model_info.id
    }

    fn dimension(&self) -> usize {
        self.model_info.dimension
    }

    fn normalized(&self) -> bool {
        self.normalized
    }

    fn embed_text(&self, text: &str) -> Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
        let embedding = self.ensure_runtime()?.embed_text(text)?;
        self.identity().validate_embedding(embedding.len())?;
        Ok(embedding)
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ThreadEpisodicEmbeddingError> {
        let embeddings = self.ensure_runtime()?.embed_batch(texts)?;
        for embedding in &embeddings {
            self.identity().validate_embedding(embedding.len())?;
        }
        Ok(embeddings)
    }
}

#[derive(Clone)]
pub(crate) struct RemoteEmbeddingProvider {
    provider_id: &'static str,
    model: String,
    dimension: usize,
    max_batch_size: usize,
    normalized: bool,
    custom_model: bool,
    provider: Arc<dyn Provider>,
}

impl RemoteEmbeddingProvider {
    pub(crate) fn openai(
        model: &str,
        normalized: bool,
        provider: Arc<dyn Provider>,
    ) -> Result<Self, ThreadEpisodicEmbeddingError> {
        let model_info = openai_embedding_model_info(model).ok_or_else(|| {
            ThreadEpisodicEmbeddingError::missing_model(OPENAI_PROVIDER_ID, model)
        })?;
        Self::ensure_provider_supports_embeddings(OPENAI_PROVIDER_ID, model, provider.as_ref())?;

        Ok(Self {
            provider_id: OPENAI_PROVIDER_ID,
            model: model_info.model.to_owned(),
            dimension: model_info.dimension,
            max_batch_size: model_info.max_batch_size,
            normalized,
            custom_model: false,
            provider,
        })
    }

    pub(crate) fn openrouter(
        model: &str,
        explicit_dimension: Option<usize>,
        normalized: bool,
        provider: Arc<dyn Provider>,
    ) -> Result<Self, ThreadEpisodicEmbeddingError> {
        Self::ensure_provider_supports_embeddings(
            OPENROUTER_PROVIDER_ID,
            model,
            provider.as_ref(),
        )?;
        let model_info = match openrouter_embedding_model_info(model, explicit_dimension) {
            Ok(model_info) => model_info,
            Err(_) if explicit_dimension.is_none() => {
                let dimension = Self::probe_embedding_dimension(
                    OPENROUTER_PROVIDER_ID,
                    model,
                    provider.as_ref(),
                )?;
                OpenRouterEmbeddingModelInfo {
                    model: model.to_owned(),
                    dimension,
                    max_batch_size: 512,
                    custom: true,
                }
            }
            Err(error) => return Err(error),
        };

        Ok(Self {
            provider_id: OPENROUTER_PROVIDER_ID,
            model: model_info.model,
            dimension: model_info.dimension,
            max_batch_size: model_info.max_batch_size,
            normalized,
            custom_model: model_info.custom,
            provider,
        })
    }

    pub(crate) fn memvid_embedder(self: Arc<Self>) -> ThreadEpisodicMemvidEmbedder {
        ThreadEpisodicMemvidEmbedder::new(self)
    }

    fn ensure_provider_supports_embeddings(
        provider_id: &str,
        model: &str,
        provider: &dyn Provider,
    ) -> Result<(), ThreadEpisodicEmbeddingError> {
        if provider.capabilities().embeddings {
            Ok(())
        } else {
            Err(
                ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                    provider_id,
                    model,
                    format!("provider `{}` does not support embeddings", provider.name()),
                ),
            )
        }
    }

    fn embed_via_provider(
        &self,
        request: EmbeddingRequest,
    ) -> Result<pioneer_provider::EmbeddingResponse, ThreadEpisodicEmbeddingError> {
        Self::embed_request_via_provider(
            self.provider_id(),
            self.model(),
            self.provider.as_ref(),
            request,
        )
    }

    fn embed_request_via_provider(
        provider_id: &str,
        model: &str,
        provider: &dyn Provider,
        request: EmbeddingRequest,
    ) -> Result<pioneer_provider::EmbeddingResponse, ThreadEpisodicEmbeddingError> {
        let result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(provider.embed(request)))
        } else {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| {
                    ThreadEpisodicEmbeddingError::retryable_provider_failure(
                        provider_id,
                        model,
                        format!("failed to create embedding runtime bridge: {error}"),
                    )
                })?;
            runtime.block_on(provider.embed(request))
        };

        result.map_err(|error| Self::map_provider_error(provider_id, model, error))
    }

    fn probe_embedding_dimension(
        provider_id: &str,
        model: &str,
        provider: &dyn Provider,
    ) -> Result<usize, ThreadEpisodicEmbeddingError> {
        let response = Self::embed_request_via_provider(
            provider_id,
            model,
            provider,
            EmbeddingRequest::new(model, vec!["dimension probe".to_owned()]),
        )?;
        let Some(embedding) = response.embeddings.into_iter().next() else {
            return Err(
                ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                    provider_id,
                    model,
                    "embedding dimension probe returned no embedding",
                ),
            );
        };
        if embedding.is_empty() {
            return Err(
                ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                    provider_id,
                    model,
                    "embedding dimension probe returned an empty embedding",
                ),
            );
        }
        Ok(embedding.len())
    }

    fn map_provider_error(
        provider_id: &str,
        model: &str,
        error: anyhow::Error,
    ) -> ThreadEpisodicEmbeddingError {
        let message = format!("{error:#}");
        if provider_embedding_error_is_retryable(message.as_str()) {
            ThreadEpisodicEmbeddingError::retryable_provider_failure(provider_id, model, message)
        } else {
            ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                provider_id,
                model,
                message,
            )
        }
    }
}

fn provider_embedding_error_is_retryable(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("429")
        || lower.contains("too many requests")
        || lower.contains("rate limit")
        || lower.contains("timeout")
        || lower.contains("connection")
        || lower.contains("temporar")
        || lower.contains("502")
        || lower.contains("503")
        || lower.contains("504")
}

impl Debug for RemoteEmbeddingProvider {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteEmbeddingProvider")
            .field("provider_id", &self.provider_id)
            .field("model", &self.model)
            .field("dimension", &self.dimension)
            .field("max_batch_size", &self.max_batch_size)
            .field("normalized", &self.normalized)
            .field("custom_model", &self.custom_model)
            .finish()
    }
}

impl ThreadEpisodicEmbeddingProvider for RemoteEmbeddingProvider {
    fn provider_id(&self) -> &str {
        self.provider_id
    }

    fn model(&self) -> &str {
        self.model.as_str()
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn normalized(&self) -> bool {
        self.normalized
    }

    fn embed_text(&self, text: &str) -> Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
        self.embed_batch(&[text]).and_then(|mut embeddings| {
            embeddings.pop().ok_or_else(|| {
                ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                    self.provider_id(),
                    self.model(),
                    "embedding response did not include an embedding",
                )
            })
        })
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ThreadEpisodicEmbeddingError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let mut embeddings = Vec::with_capacity(texts.len());
        for chunk in texts.chunks(self.max_batch_size) {
            let response = self.embed_via_provider(EmbeddingRequest::new(
                self.model(),
                chunk.iter().map(|text| (*text).to_owned()).collect(),
            ))?;
            if response.embeddings.len() != chunk.len() {
                return Err(
                    ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                        self.provider_id(),
                        self.model(),
                        format!(
                            "{} embedding response returned {} embeddings for {} inputs",
                            self.provider_id(),
                            response.embeddings.len(),
                            chunk.len()
                        ),
                    ),
                );
            }

            for embedding in response.embeddings {
                self.identity().validate_embedding(embedding.len())?;
                embeddings.push(embedding);
            }
        }

        Ok(embeddings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use memvid_core::types::ask::VecEmbedder;
    use std::sync::Mutex;

    struct FakeRemoteProvider {
        name: &'static str,
        embeddings: bool,
        responses: Mutex<Vec<anyhow::Result<pioneer_provider::EmbeddingResponse>>>,
    }

    impl FakeRemoteProvider {
        fn with_responses(
            name: &'static str,
            responses: Vec<anyhow::Result<pioneer_provider::EmbeddingResponse>>,
        ) -> Self {
            Self {
                name,
                embeddings: true,
                responses: Mutex::new(responses),
            }
        }

        fn without_embeddings(name: &'static str) -> Self {
            Self {
                name,
                embeddings: false,
                responses: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl Provider for FakeRemoteProvider {
        fn name(&self) -> &str {
            self.name
        }

        fn capabilities(&self) -> pioneer_provider::ProviderCapabilities {
            pioneer_provider::ProviderCapabilities {
                streaming: true,
                vision: false,
                tool_calling: false,
                embeddings: self.embeddings,
                input_types:
                    pioneer_provider::ProviderInputCapabilities::disabled_for_all_file_types(),
            }
        }

        async fn chat(
            &self,
            _request: pioneer_provider::ChatRequest,
        ) -> anyhow::Result<pioneer_provider::ChatResponse> {
            anyhow::bail!("fake provider chat is unused")
        }

        async fn stream_chat(
            &self,
            _request: pioneer_provider::ChatRequest,
        ) -> anyhow::Result<
            futures_util::stream::BoxStream<'static, anyhow::Result<pioneer_provider::StreamChunk>>,
        > {
            anyhow::bail!("fake provider stream_chat is unused")
        }

        async fn embed(
            &self,
            _request: pioneer_provider::EmbeddingRequest,
        ) -> anyhow::Result<pioneer_provider::EmbeddingResponse> {
            self.responses.lock().expect("fake provider lock").remove(0)
        }
    }

    fn embedding_response(vectors: Vec<Vec<f32>>) -> pioneer_provider::EmbeddingResponse {
        pioneer_provider::EmbeddingResponse {
            embeddings: vectors,
        }
    }

    #[test]
    fn openai_embedding_model_table_resolves_supported_dimensions() {
        assert_eq!(
            openai_embedding_model_info("text-embedding-3-small").map(|info| info.dimension),
            Some(1536)
        );
        assert_eq!(
            openai_embedding_model_info("text-embedding-3-large").map(|info| info.dimension),
            Some(3072)
        );
        assert_eq!(
            openai_embedding_model_info("text-embedding-ada-002").map(|info| info.dimension),
            Some(1536)
        );
        assert!(openai_embedding_model_info("unknown").is_none());
    }

    #[test]
    fn remote_embedding_provider_rejects_provider_without_embedding_capability() {
        let error = RemoteEmbeddingProvider::openai(
            "text-embedding-3-small",
            true,
            Arc::new(FakeRemoteProvider::without_embeddings("openai")),
        )
        .expect_err("provider without embeddings must fail");

        assert!(!error.is_retryable());
        assert!(error.message.contains("does not support embeddings"));
    }

    #[test]
    fn openai_remote_embedding_provider_embeds_single_and_batch_with_dimension_validation() {
        let provider = RemoteEmbeddingProvider::openai(
            "text-embedding-3-small",
            true,
            Arc::new(FakeRemoteProvider::with_responses(
                "openai",
                vec![
                    Ok(embedding_response(vec![vec![0.1; 1536]])),
                    Ok(embedding_response(vec![vec![0.2; 1536], vec![0.3; 1536]])),
                ],
            )),
        )
        .expect("provider");

        assert_eq!(provider.provider_id(), OPENAI_PROVIDER_ID);
        assert_eq!(provider.embed_text("hello").unwrap().len(), 1536);
        let batch = provider.embed_batch(&["hello", "world"]).unwrap();
        assert_eq!(batch.len(), 2);
        assert!(batch.iter().all(|embedding| embedding.len() == 1536));
    }

    #[test]
    fn remote_embedding_provider_classifies_retryable_and_non_retryable_failures() {
        let rate_limited = RemoteEmbeddingProvider::openai(
            "text-embedding-3-small",
            true,
            Arc::new(FakeRemoteProvider::with_responses(
                "openai",
                vec![Err(anyhow::anyhow!("OpenAI API error (429): rate limit"))],
            )),
        )
        .expect("provider")
        .embed_text("hello")
        .expect_err("rate limit should fail");
        assert!(rate_limited.is_retryable());

        let invalid_key = RemoteEmbeddingProvider::openai(
            "text-embedding-3-small",
            true,
            Arc::new(FakeRemoteProvider::with_responses(
                "openai",
                vec![Err(anyhow::anyhow!("OpenAI API error (401): invalid key"))],
            )),
        )
        .expect("provider")
        .embed_text("hello")
        .expect_err("invalid key should fail");
        assert!(!invalid_key.is_retryable());
    }

    #[test]
    fn openai_remote_embedding_provider_rejects_bad_response_shape() {
        let provider = RemoteEmbeddingProvider::openai(
            "text-embedding-3-small",
            true,
            Arc::new(FakeRemoteProvider::with_responses(
                "openai",
                vec![Ok(embedding_response(vec![vec![0.1; 3]]))],
            )),
        )
        .expect("provider");

        let error = provider
            .embed_text("hello")
            .expect_err("wrong dimension should fail");
        assert!(matches!(
            error.kind,
            pioneer_memory::ThreadEpisodicEmbeddingErrorKind::DimensionMismatch {
                expected: 1536,
                actual: 3
            }
        ));
    }

    #[test]
    fn openrouter_embedding_model_table_resolves_known_dimensions_and_requires_custom_dimension() {
        let small = openrouter_embedding_model_info("openai/text-embedding-3-small", None)
            .expect("known small");
        assert_eq!(small.dimension, 1536);
        assert!(!small.custom);

        let large = openrouter_embedding_model_info("openai/text-embedding-3-large", None)
            .expect("known large");
        assert_eq!(large.dimension, 3072);
        assert!(!large.custom);

        let missing_dimension = openrouter_embedding_model_info("vendor/custom-embed", None)
            .expect_err("custom model needs dimension");
        assert!(!missing_dimension.is_retryable());

        let custom = openrouter_embedding_model_info("vendor/custom-embed", Some(768))
            .expect("custom dimension is explicit");
        assert_eq!(custom.dimension, 768);
        assert!(custom.custom);
    }

    #[test]
    fn openrouter_remote_embedding_provider_embeds_custom_model_with_provider_trait() {
        let provider = RemoteEmbeddingProvider::openrouter(
            "vendor/custom-embed",
            Some(768),
            true,
            Arc::new(FakeRemoteProvider::with_responses(
                "openrouter",
                vec![Ok(embedding_response(vec![vec![0.4; 768]]))],
            )),
        )
        .expect("provider");

        assert_eq!(provider.provider_id(), OPENROUTER_PROVIDER_ID);
        assert_eq!(provider.embed_text("hello").unwrap().len(), 768);
    }

    #[test]
    fn openrouter_remote_embedding_provider_probes_custom_model_dimension() {
        let provider = RemoteEmbeddingProvider::openrouter(
            "qwen/qwen3-embedding-8b",
            None,
            true,
            Arc::new(FakeRemoteProvider::with_responses(
                "openrouter",
                vec![
                    Ok(embedding_response(vec![vec![0.1; 4096]])),
                    Ok(embedding_response(vec![vec![0.2; 4096]])),
                ],
            )),
        )
        .expect("provider");

        assert_eq!(provider.provider_id(), OPENROUTER_PROVIDER_ID);
        assert_eq!(provider.dimension(), 4096);
        assert_eq!(provider.embed_text("hello").unwrap().len(), 4096);
    }

    #[test]
    fn local_embedding_model_registry_resolves_supported_models() {
        let small = local_embedding_model_info("bge-small-en-v1.5").expect("small model");
        assert_eq!(small.display_name, "BGE Small EN v1.5");
        assert_eq!(small.dimension, 384);
        assert_eq!(small.max_tokens, 512);
        assert!(small.default);
        assert!(small.model_url.contains("model.onnx"));
        assert!(small.tokenizer_url.contains("tokenizer.json"));

        assert_eq!(
            local_embedding_model_info("bge-base-en-v1.5").map(|info| info.dimension),
            Some(768)
        );
        assert_eq!(
            local_embedding_model_info("nomic-embed-text-v1.5").map(|info| info.dimension),
            Some(768)
        );
        assert_eq!(
            local_embedding_model_info("gte-large").map(|info| info.dimension),
            Some(1024)
        );
        assert!(local_embedding_model_info("unknown").is_none());
    }

    #[test]
    fn local_embedding_model_files_live_under_runtime_home() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        let files = local_embedding_model_files(runtime_home.path(), "bge-small-en-v1.5")
            .expect("model files");

        assert!(files.models_dir.starts_with(runtime_home.path()));
        assert_eq!(
            files.models_dir,
            runtime_home
                .path()
                .join("models")
                .join("embedding")
                .join("text")
        );
        assert_eq!(
            files.model_path,
            files.models_dir.join("bge-small-en-v1.5.onnx")
        );
        assert_eq!(
            files.tokenizer_path,
            files.models_dir.join("bge-small-en-v1.5_tokenizer.json")
        );
    }

    #[test]
    fn local_embedding_model_status_is_not_selected_when_disabled_or_api_provider() {
        let runtime_home = tempfile::tempdir().expect("runtime home");

        assert_eq!(
            local_embedding_model_status(
                runtime_home.path(),
                false,
                Some(GatewayThreadEpisodicVectorProvider::Local),
                "bge-small-en-v1.5",
            ),
            GatewayThreadEpisodicVectorLocalModelStatus::NotSelected
        );
        assert_eq!(
            local_embedding_model_status(
                runtime_home.path(),
                true,
                Some(GatewayThreadEpisodicVectorProvider::OpenAi),
                "bge-small-en-v1.5",
            ),
            GatewayThreadEpisodicVectorLocalModelStatus::NotSelected
        );
        assert_eq!(
            local_embedding_model_status(
                runtime_home.path(),
                true,
                Some(GatewayThreadEpisodicVectorProvider::OpenRouter),
                "bge-small-en-v1.5",
            ),
            GatewayThreadEpisodicVectorLocalModelStatus::NotSelected
        );
    }

    #[test]
    fn local_embedding_model_status_detects_missing_downloading_failed_and_installed() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        let files = local_embedding_model_files(runtime_home.path(), "bge-small-en-v1.5")
            .expect("model files");
        std::fs::create_dir_all(files.models_dir.as_path()).expect("create models dir");

        assert_eq!(
            local_embedding_model_status(
                runtime_home.path(),
                true,
                Some(GatewayThreadEpisodicVectorProvider::Local),
                "bge-small-en-v1.5",
            ),
            GatewayThreadEpisodicVectorLocalModelStatus::Missing
        );

        std::fs::write(files.download_marker_path.as_path(), b"").expect("write download marker");
        assert_eq!(
            local_embedding_model_status(
                runtime_home.path(),
                true,
                Some(GatewayThreadEpisodicVectorProvider::Local),
                "bge-small-en-v1.5",
            ),
            GatewayThreadEpisodicVectorLocalModelStatus::Downloading
        );

        std::fs::remove_file(files.download_marker_path.as_path()).expect("remove marker");
        std::fs::write(files.failure_marker_path.as_path(), b"download failed")
            .expect("write failure marker");
        assert_eq!(
            local_embedding_model_status(
                runtime_home.path(),
                true,
                Some(GatewayThreadEpisodicVectorProvider::Local),
                "bge-small-en-v1.5",
            ),
            GatewayThreadEpisodicVectorLocalModelStatus::Failed
        );

        std::fs::write(files.model_path.as_path(), b"model").expect("write model");
        std::fs::write(files.tokenizer_path.as_path(), b"tokenizer").expect("write tokenizer");
        assert_eq!(
            local_embedding_model_status(
                runtime_home.path(),
                true,
                Some(GatewayThreadEpisodicVectorProvider::Local),
                "bge-small-en-v1.5",
            ),
            GatewayThreadEpisodicVectorLocalModelStatus::Installed
        );

        assert_eq!(
            local_embedding_model_status(
                runtime_home.path(),
                true,
                Some(GatewayThreadEpisodicVectorProvider::Local),
                "unknown-model",
            ),
            GatewayThreadEpisodicVectorLocalModelStatus::Failed
        );
    }

    #[test]
    fn local_embedding_download_does_not_start_when_disabled_or_api_provider() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        let disabled_local = GatewayThreadEpisodicVectorSearchConfig {
            enabled: false,
            provider: Some(GatewayThreadEpisodicVectorProviderConfig::Local),
            local_model: Some("bge-small-en-v1.5".to_owned()),
            ..GatewayThreadEpisodicVectorSearchConfig::default()
        };
        assert!(
            !spawn_local_embedding_model_download_if_needed(runtime_home.path(), &disabled_local)
                .expect("disabled local should not start")
        );

        let enabled_api = GatewayThreadEpisodicVectorSearchConfig {
            enabled: true,
            provider: Some(GatewayThreadEpisodicVectorProviderConfig::OpenAi),
            local_model: Some("bge-small-en-v1.5".to_owned()),
            ..GatewayThreadEpisodicVectorSearchConfig::default()
        };
        assert!(
            !spawn_local_embedding_model_download_if_needed(runtime_home.path(), &enabled_api)
                .expect("api provider should not start local download")
        );
        assert!(!local_embedding_models_root(runtime_home.path()).exists());
    }

    #[test]
    fn local_embedding_download_does_not_start_for_installed_model() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        let files = local_embedding_model_files(runtime_home.path(), "bge-small-en-v1.5")
            .expect("model files");
        std::fs::create_dir_all(files.models_dir.as_path()).expect("create models dir");
        std::fs::write(files.model_path.as_path(), b"model").expect("write model");
        std::fs::write(files.tokenizer_path.as_path(), b"tokenizer").expect("write tokenizer");

        let config = GatewayThreadEpisodicVectorSearchConfig {
            enabled: true,
            provider: Some(GatewayThreadEpisodicVectorProviderConfig::Local),
            local_model: Some("bge-small-en-v1.5".to_owned()),
            ..GatewayThreadEpisodicVectorSearchConfig::default()
        };
        assert!(
            !spawn_local_embedding_model_download_if_needed(runtime_home.path(), &config)
                .expect("installed model should not start download")
        );
        assert!(!files.download_marker_path.exists());
        assert!(!files.failure_marker_path.exists());
    }

    #[test]
    fn local_embedding_download_rejects_unknown_model_before_spawning() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        let config = GatewayThreadEpisodicVectorSearchConfig {
            enabled: true,
            provider: Some(GatewayThreadEpisodicVectorProviderConfig::Local),
            local_model: Some("unknown-local-model".to_owned()),
            ..GatewayThreadEpisodicVectorSearchConfig::default()
        };
        let error = spawn_local_embedding_model_download_if_needed(runtime_home.path(), &config)
            .expect_err("unknown local model should fail");
        assert!(error.contains("unknown local embedding model"));
    }

    #[test]
    fn embedding_diagnostics_report_readiness_from_settings() {
        let disabled = GatewayThreadEpisodicVectorSearchSettings::default();
        let disabled_readiness = embedding_provider_readiness_from_settings(&disabled);
        assert_eq!(
            disabled_readiness.state,
            EmbeddingProviderReadinessState::Disabled
        );
        assert!(!disabled_readiness.is_ready());

        let missing_key = GatewayThreadEpisodicVectorSearchSettings {
            enabled: true,
            provider: Some(GatewayThreadEpisodicVectorProvider::OpenAi),
            model: Some("text-embedding-3-small".to_owned()),
            embedding_dimension: Some(1536),
            provider_key: pioneer_protocol::GatewayThreadEpisodicVectorProviderKeyStatus {
                required: true,
                present: false,
            },
            ..GatewayThreadEpisodicVectorSearchSettings::default()
        };
        let missing_key_readiness = embedding_provider_readiness_from_settings(&missing_key);
        assert_eq!(
            missing_key_readiness.state,
            EmbeddingProviderReadinessState::MissingConfiguration
        );
        assert!(!missing_key_readiness.retryable);
        assert_eq!(missing_key_readiness.provider_id, OPENAI_PROVIDER_ID);

        let downloading_local = GatewayThreadEpisodicVectorSearchSettings {
            enabled: true,
            provider: Some(GatewayThreadEpisodicVectorProvider::Local),
            local_model: Some("bge-small-en-v1.5".to_owned()),
            embedding_dimension: Some(384),
            local_model_status: GatewayThreadEpisodicVectorLocalModelStatus::Downloading,
            ..GatewayThreadEpisodicVectorSearchSettings::default()
        };
        let downloading_readiness = embedding_provider_readiness_from_settings(&downloading_local);
        assert_eq!(
            downloading_readiness.state,
            EmbeddingProviderReadinessState::LocalModelDownloading
        );
        assert!(downloading_readiness.retryable);

        let ready_local = GatewayThreadEpisodicVectorSearchSettings {
            local_model_status: GatewayThreadEpisodicVectorLocalModelStatus::Installed,
            ..downloading_local
        };
        let ready = embedding_provider_readiness_from_settings(&ready_local);
        assert_eq!(ready.state, EmbeddingProviderReadinessState::Ready);
        assert!(ready.is_ready());

        let openrouter_dynamic = GatewayThreadEpisodicVectorSearchSettings {
            enabled: true,
            provider: Some(GatewayThreadEpisodicVectorProvider::OpenRouter),
            model: Some("qwen/qwen3-embedding-8b".to_owned()),
            provider_key: pioneer_protocol::GatewayThreadEpisodicVectorProviderKeyStatus {
                required: true,
                present: true,
            },
            ..GatewayThreadEpisodicVectorSearchSettings::default()
        };
        let openrouter_readiness = embedding_provider_readiness_from_settings(&openrouter_dynamic);
        assert_eq!(
            openrouter_readiness.state,
            EmbeddingProviderReadinessState::Ready
        );
        assert!(openrouter_readiness.is_ready());
    }

    #[test]
    fn embedding_diagnostics_map_embedding_errors_to_readiness() {
        let missing =
            ThreadEpisodicEmbeddingError::missing_key(OPENAI_PROVIDER_ID, "text-embedding-3-small");
        let missing_diagnostic = embedding_provider_readiness_from_error(&missing);
        assert_eq!(
            missing_diagnostic.state,
            EmbeddingProviderReadinessState::MissingConfiguration
        );
        assert!(!missing_diagnostic.retryable);

        let retryable = ThreadEpisodicEmbeddingError::retryable_provider_failure(
            OPENROUTER_PROVIDER_ID,
            "openai/text-embedding-3-small",
            "rate limited",
        );
        let retryable_diagnostic = embedding_provider_readiness_from_error(&retryable);
        assert_eq!(
            retryable_diagnostic.state,
            EmbeddingProviderReadinessState::RetryableProviderFailure
        );
        assert!(retryable_diagnostic.retryable);

        let dimension = ThreadEpisodicEmbeddingError::dimension_mismatch(
            OPENAI_PROVIDER_ID,
            "text-embedding-3-large",
            3072,
            1536,
        );
        let dimension_diagnostic = embedding_provider_readiness_from_error(&dimension);
        assert_eq!(
            dimension_diagnostic.state,
            EmbeddingProviderReadinessState::DimensionMismatch
        );
        assert!(!dimension_diagnostic.retryable);
    }

    #[test]
    fn embedding_diagnostics_safe_log_fields_do_not_include_payload_or_key_material() {
        let error = ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
            OPENAI_PROVIDER_ID,
            "text-embedding-3-small",
            "provider rejected sk-secret while embedding private text",
        );

        let fields = error.safe_log_fields();
        assert_eq!(fields.provider_id, OPENAI_PROVIDER_ID);
        assert_eq!(fields.model, "text-embedding-3-small");
        assert_eq!(
            fields.failure_class,
            ThreadEpisodicEmbeddingFailureClass::PermanentProviderFailure
        );
        let serialized = serde_json::to_string(&fields).expect("serialize fields");
        assert!(!serialized.contains("sk-secret"));
        assert!(!serialized.contains("private text"));
    }

    #[derive(Debug)]
    struct FakeLocalEmbeddingRuntime {
        dimension: usize,
    }

    impl LocalEmbeddingRuntime for FakeLocalEmbeddingRuntime {
        fn embed_text(&self, _text: &str) -> Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
            Ok(vec![0.25; self.dimension])
        }

        fn embed_batch(
            &self,
            texts: &[&str],
        ) -> Result<Vec<Vec<f32>>, ThreadEpisodicEmbeddingError> {
            Ok(texts.iter().map(|_| vec![0.5; self.dimension]).collect())
        }
    }

    #[derive(Debug)]
    struct FakeLocalEmbeddingRuntimeFactory {
        dimension: usize,
        create_count: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl FakeLocalEmbeddingRuntimeFactory {
        fn new(dimension: usize) -> Self {
            Self {
                dimension,
                create_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        fn create_count(&self) -> usize {
            self.create_count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl LocalEmbeddingRuntimeFactory for FakeLocalEmbeddingRuntimeFactory {
        fn create(
            &self,
            _config: TextEmbedConfig,
        ) -> Result<Arc<dyn LocalEmbeddingRuntime>, ThreadEpisodicEmbeddingError> {
            self.create_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Arc::new(FakeLocalEmbeddingRuntime {
                dimension: self.dimension,
            }))
        }
    }

    fn install_fake_local_model_files(
        runtime_home: &std::path::Path,
        model: &str,
    ) -> LocalEmbeddingModelFiles {
        let files = local_embedding_model_files(runtime_home, model).expect("model files");
        std::fs::create_dir_all(files.models_dir.as_path()).expect("create models dir");
        std::fs::write(files.model_path.as_path(), b"model").expect("write model");
        std::fs::write(files.tokenizer_path.as_path(), b"tokenizer").expect("write tokenizer");
        files
    }

    #[test]
    fn local_embedding_provider_does_not_initialize_runtime_until_embedding_call() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        install_fake_local_model_files(runtime_home.path(), "bge-small-en-v1.5");
        let factory = Arc::new(FakeLocalEmbeddingRuntimeFactory::new(384));
        let provider = LocalEmbeddingProvider::with_runtime_factory(
            runtime_home.path(),
            "bge-small-en-v1.5",
            true,
            factory.clone(),
        )
        .expect("provider");

        assert!(!provider.runtime_initialized());
        assert_eq!(factory.create_count(), 0);
        assert_eq!(provider.dimension(), 384);

        let embedding = provider.embed_text("hello").expect("embedding");
        assert_eq!(embedding.len(), 384);
        assert!(provider.runtime_initialized());
        assert_eq!(factory.create_count(), 1);

        let second = provider.embed_text("again").expect("second embedding");
        assert_eq!(second.len(), 384);
        assert_eq!(factory.create_count(), 1);
    }

    #[test]
    fn local_embedding_provider_reports_missing_files_without_runtime_init() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        let factory = Arc::new(FakeLocalEmbeddingRuntimeFactory::new(384));
        let provider = LocalEmbeddingProvider::with_runtime_factory(
            runtime_home.path(),
            "bge-small-en-v1.5",
            true,
            factory.clone(),
        )
        .expect("provider");

        let error = provider
            .embed_text("hello")
            .expect_err("missing model files should fail");
        assert!(matches!(
            error.kind,
            pioneer_memory::ThreadEpisodicEmbeddingErrorKind::MissingModel
        ));
        assert!(!error.is_retryable());
        assert!(!provider.runtime_initialized());
        assert_eq!(factory.create_count(), 0);
    }

    #[test]
    fn local_embedding_provider_embeds_text_query_and_batch_with_configured_dimension() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        install_fake_local_model_files(runtime_home.path(), "bge-base-en-v1.5");
        let provider = LocalEmbeddingProvider::with_runtime_factory(
            runtime_home.path(),
            "bge-base-en-v1.5",
            true,
            Arc::new(FakeLocalEmbeddingRuntimeFactory::new(768)),
        )
        .expect("provider");

        assert_eq!(provider.provider_id(), LOCAL_PROVIDER_ID);
        assert_eq!(provider.model(), "bge-base-en-v1.5");
        assert_eq!(provider.embed_text("hello").unwrap().len(), 768);
        assert_eq!(provider.embed_query("query").unwrap().len(), 768);
        let batch = provider.embed_batch(&["one", "two"]).unwrap();
        assert_eq!(batch.len(), 2);
        assert!(batch.iter().all(|embedding| embedding.len() == 768));
    }

    #[test]
    fn local_embedding_provider_memvid_embedder_uses_same_identity() {
        let runtime_home = tempfile::tempdir().expect("runtime home");
        install_fake_local_model_files(runtime_home.path(), "gte-large");
        let provider = Arc::new(
            LocalEmbeddingProvider::with_runtime_factory(
                runtime_home.path(),
                "gte-large",
                true,
                Arc::new(FakeLocalEmbeddingRuntimeFactory::new(1024)),
            )
            .expect("provider"),
        );

        let embedder = provider.clone().memvid_embedder();
        assert_eq!(embedder.identity(), provider.identity());
        assert_eq!(embedder.embedding_dimension(), 1024);
        assert_eq!(embedder.embed_query("query").unwrap().len(), 1024);
    }

    #[test]
    #[ignore = "manual provider smoke: requires PIONEER_OPENAI_EMBEDDING_API_KEY or OPENAI_API_KEY"]
    fn embedding_provider_openai_smoke_from_env() {
        let Some(api_key) = std::env::var("PIONEER_OPENAI_EMBEDDING_API_KEY")
            .ok()
            .or_else(|| std::env::var("OPENAI_API_KEY").ok())
        else {
            eprintln!(
                "skipping OpenAI embedding smoke: PIONEER_OPENAI_EMBEDDING_API_KEY/OPENAI_API_KEY is not set"
            );
            return;
        };
        let model = std::env::var("PIONEER_OPENAI_EMBEDDING_MODEL")
            .unwrap_or_else(|_| "text-embedding-3-small".to_owned());
        let provider = RemoteEmbeddingProvider::openai(
            model.as_str(),
            true,
            Arc::new(pioneer_provider::providers::OpenAiProvider::new(api_key)),
        )
        .expect("OpenAI smoke provider should initialize");

        let embedding = provider
            .embed_text("Pioneer thread episodic vector search smoke test")
            .expect("OpenAI smoke embedding should succeed");
        assert_smoke_embedding_dimension(&provider, embedding.as_slice());
    }

    #[test]
    #[ignore = "manual provider smoke: requires PIONEER_OPENROUTER_EMBEDDING_API_KEY or OPENROUTER_API_KEY"]
    fn embedding_provider_openrouter_smoke_from_env() {
        let Some(api_key) = std::env::var("PIONEER_OPENROUTER_EMBEDDING_API_KEY")
            .ok()
            .or_else(|| std::env::var("OPENROUTER_API_KEY").ok())
        else {
            eprintln!(
                "skipping OpenRouter embedding smoke: PIONEER_OPENROUTER_EMBEDDING_API_KEY/OPENROUTER_API_KEY is not set"
            );
            return;
        };
        let model = std::env::var("PIONEER_OPENROUTER_EMBEDDING_MODEL")
            .unwrap_or_else(|_| "openai/text-embedding-3-small".to_owned());
        let explicit_dimension = std::env::var("PIONEER_OPENROUTER_EMBEDDING_DIMENSION")
            .ok()
            .map(|value| {
                value
                    .parse::<usize>()
                    .expect("PIONEER_OPENROUTER_EMBEDDING_DIMENSION must be a positive integer")
            });
        let provider = RemoteEmbeddingProvider::openrouter(
            model.as_str(),
            explicit_dimension,
            true,
            Arc::new(pioneer_provider::providers::OpenRouterProvider::new(
                api_key,
            )),
        )
        .expect("OpenRouter smoke provider should initialize");

        let embedding = provider
            .embed_text("Pioneer thread episodic vector search smoke test")
            .expect("OpenRouter smoke embedding should succeed");
        assert_smoke_embedding_dimension(&provider, embedding.as_slice());
    }

    #[test]
    #[ignore = "manual local embedder smoke: requires installed local model files"]
    fn local_embedder_smoke_from_installed_model() {
        let Some(runtime_home) =
            std::env::var_os("PIONEER_LOCAL_EMBEDDING_RUNTIME_HOME").map(PathBuf::from)
        else {
            eprintln!(
                "skipping local embedder smoke: PIONEER_LOCAL_EMBEDDING_RUNTIME_HOME is not set"
            );
            return;
        };
        let model = std::env::var("PIONEER_LOCAL_EMBEDDING_MODEL")
            .unwrap_or_else(|_| "bge-small-en-v1.5".to_owned());
        let status = local_embedding_model_status(
            runtime_home.as_path(),
            true,
            Some(GatewayThreadEpisodicVectorProvider::Local),
            model.as_str(),
        );
        if status != GatewayThreadEpisodicVectorLocalModelStatus::Installed {
            eprintln!(
                "skipping local embedder smoke: model `{model}` is not installed under {} (status: {status:?})",
                runtime_home.display()
            );
            return;
        }
        let provider =
            LocalEmbeddingProvider::from_runtime_home(runtime_home.as_path(), model.as_str(), true)
                .expect("local smoke provider should initialize");

        let embedding = provider
            .embed_text("Pioneer thread episodic vector search smoke test")
            .expect("local smoke embedding should succeed");
        assert_smoke_embedding_dimension(&provider, embedding.as_slice());
    }

    fn assert_smoke_embedding_dimension(
        provider: &dyn ThreadEpisodicEmbeddingProvider,
        embedding: &[f32],
    ) {
        assert_eq!(embedding.len(), provider.dimension());
        assert!(
            embedding.iter().all(|value| value.is_finite()),
            "embedding response must contain only finite floats"
        );
    }
}
