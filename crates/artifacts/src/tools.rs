use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{Duration, Utc};
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind,
    ArtifactCreatedNotification, ArtifactKind, ArtifactPrepareKind, ArtifactPrepareParams,
    ArtifactPrepareResponse, ArtifactProjectionKind, ArtifactProjectionUpdatedNotification,
    ArtifactReadParams, ArtifactRegisterParams, ArtifactRegisterResponse, ArtifactRole,
    ArtifactSummary, ThreadArtifactsChangedNotification, constants::events,
};
use pioneer_provider::AttachmentDataSource;
use pioneer_tools::{
    ConfiguredToolSpec, ExecutionClass, FilePolicyChecker, FilePolicyDecision, FilePolicyOperation,
    FunctionToolOutput, PayloadKind, ToolError, ToolHandler, ToolIdempotencyMode, ToolInvocation,
    ToolOutput, ToolOutputProjectionKind, ToolPayload, ToolRecoveryMetadata, ToolRetryClass,
    ToolSpec, dynamic_unknown_output_policy, normalize_tool_arguments_from_schema,
};
use schemars::{JsonSchema, schema_for};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};

use crate::{
    ArtifactProjectionRecord, ArtifactRegistrationCandidate, ArtifactRegistrationContext,
    ArtifactRegistrationSource, ArtifactService, PIONEER_ARTIFACT_OUTPUT_DIR_ENV,
    mime::display_name_with_mime_extension,
};

pub const ARTIFACT_PREPARE_TOOL: &str = "artifact_prepare";
pub const ARTIFACT_REGISTER_TOOL: &str = "artifact_register";
pub const ARTIFACT_READ_TOOL: &str = "artifact_read";
pub const ARTIFACT_OUTPUT_DIR_ENV: &str = PIONEER_ARTIFACT_OUTPUT_DIR_ENV;

const PREPARED_OUTPUT_TTL_HOURS: i64 = 24;
const MAX_FILENAME_CHARS: usize = 120;
const ARTIFACT_READ_MAX_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparedArtifactOutputStatus {
    Reserved,
    Registered {
        artifact_id: String,
        version_id: String,
    },
    Abandoned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedArtifactOutput {
    pub tool_call_id: String,
    pub output_path: PathBuf,
    pub output_dir: PathBuf,
    pub display_name: String,
    pub kind: ArtifactPrepareKind,
    pub mime_type: Option<String>,
    pub description: Option<String>,
    pub expires_at: String,
    pub status: PreparedArtifactOutputStatus,
}

impl PreparedArtifactOutput {
    pub fn is_registered(&self) -> bool {
        matches!(self.status, PreparedArtifactOutputStatus::Registered { .. })
    }

    pub fn is_reserved(&self) -> bool {
        matches!(self.status, PreparedArtifactOutputStatus::Reserved)
    }
}

#[derive(Default)]
pub struct ArtifactToolState {
    inner: Mutex<ArtifactToolStateInner>,
}

#[derive(Default)]
struct ArtifactToolStateInner {
    prepared_by_path: BTreeMap<String, PreparedArtifactOutput>,
    filename_counters: BTreeMap<String, u64>,
}

impl ArtifactToolState {
    pub fn prepared_outputs(&self) -> Vec<PreparedArtifactOutput> {
        self.inner
            .lock()
            .map(|inner| inner.prepared_by_path.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn prepared_output_for_path(&self, path: &Path) -> Option<PreparedArtifactOutput> {
        self.inner
            .lock()
            .ok()
            .and_then(|inner| inner.prepared_by_path.get(&path_key(path)).cloned())
    }

    pub fn mark_registered(
        &self,
        path: &Path,
        artifact_id: &str,
        version_id: &str,
    ) -> Option<PreparedArtifactOutput> {
        self.inner.lock().ok().and_then(|mut inner| {
            let prepared = inner.prepared_by_path.get_mut(&path_key(path))?;
            prepared.status = PreparedArtifactOutputStatus::Registered {
                artifact_id: artifact_id.to_owned(),
                version_id: version_id.to_owned(),
            };
            Some(prepared.clone())
        })
    }

    fn reserve_output(
        &self,
        output_dir: &Path,
        params: ArtifactPrepareParams,
        tool_call_id: String,
        expires_at: String,
    ) -> Result<PreparedArtifactOutput, ToolError> {
        let sanitized = truncate_filename(
            display_name_with_mime_extension(
                sanitize_display_name(params.display_name.as_str()),
                params.mime_type.as_deref(),
            )
            .as_str(),
            MAX_FILENAME_CHARS,
        );
        let (stem, extension) = split_filename(sanitized.as_str());
        let counter_key = format!("{}:{}", path_key(output_dir), sanitized);

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| ToolError::internal("artifact tool state lock poisoned"))?;

        let mut sequence = inner
            .filename_counters
            .get(&counter_key)
            .copied()
            .unwrap_or(0);
        let output_path = loop {
            sequence = sequence.saturating_add(1);
            let file_name = numbered_filename(stem.as_str(), extension.as_deref(), sequence);
            let candidate = output_dir.join(file_name);
            if candidate.starts_with(output_dir)
                && !inner
                    .prepared_by_path
                    .contains_key(path_key(candidate.as_path()).as_str())
                && !candidate.exists()
            {
                inner
                    .filename_counters
                    .insert(counter_key.clone(), sequence);
                break candidate;
            }
        };

        let prepared = PreparedArtifactOutput {
            tool_call_id,
            output_path,
            output_dir: output_dir.to_path_buf(),
            display_name: sanitized,
            kind: params.kind,
            mime_type: params.mime_type.filter(|value| !value.trim().is_empty()),
            description: params.description.filter(|value| !value.trim().is_empty()),
            expires_at,
            status: PreparedArtifactOutputStatus::Reserved,
        };
        inner
            .prepared_by_path
            .insert(path_key(prepared.output_path.as_path()), prepared.clone());

        Ok(prepared)
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactToolContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactReadToolParams {
    pub artifact_id: String,
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub projection_kind: Option<ArtifactProjectionKind>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum ArtifactToolNotification {
    ArtifactCreated(ArtifactCreatedNotification),
    ThreadArtifactsChanged(ThreadArtifactsChangedNotification),
    ArtifactProjectionUpdated(ArtifactProjectionUpdatedNotification),
}

impl ArtifactToolNotification {
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::ArtifactCreated(_) => events::ARTIFACT_CREATED,
            Self::ThreadArtifactsChanged(_) => events::THREAD_ARTIFACTS_CHANGED,
            Self::ArtifactProjectionUpdated(_) => events::ARTIFACT_PROJECTION_UPDATED,
        }
    }
}

#[async_trait]
pub trait ArtifactToolNotificationSink: Send + Sync {
    async fn send(&self, thread_id: &str, notification: ArtifactToolNotification);
}

#[derive(Debug, Default)]
pub struct NoopArtifactToolNotificationSink;

#[async_trait]
impl ArtifactToolNotificationSink for NoopArtifactToolNotificationSink {
    async fn send(&self, _thread_id: &str, _notification: ArtifactToolNotification) {}
}

pub struct ArtifactToolHandler {
    artifact_service: Arc<ArtifactService>,
    context: ArtifactToolContext,
    state: Arc<ArtifactToolState>,
    notification_sink: Arc<dyn ArtifactToolNotificationSink>,
}

impl ArtifactToolHandler {
    pub fn new(
        artifact_service: Arc<ArtifactService>,
        context: ArtifactToolContext,
        state: Arc<ArtifactToolState>,
        notification_sink: Arc<dyn ArtifactToolNotificationSink>,
    ) -> Self {
        Self {
            artifact_service,
            context,
            state,
            notification_sink,
        }
    }

    async fn handle_prepare(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let params = decode_artifact_tool_args::<ArtifactPrepareParams>(invocation.clone())?;
        let output_dir = invocation
            .environment
            .get(ARTIFACT_OUTPUT_DIR_ENV)
            .map(String::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                ToolError::invalid_arguments(format!(
                    "`{ARTIFACT_OUTPUT_DIR_ENV}` is required for artifact_prepare"
                ))
            })?;

        enforce_artifact_path_policy(
            &invocation,
            FilePolicyOperation::Write,
            Path::new(output_dir),
            "artifact_prepare output dir",
        )?;

        tokio::fs::create_dir_all(output_dir)
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to create artifact output dir `{output_dir}`: {error}"
                ))
            })?;
        let output_dir = tokio::fs::canonicalize(output_dir).await.map_err(|error| {
            ToolError::execution_failed(format!(
                "failed to resolve artifact output dir `{output_dir}`: {error}"
            ))
        })?;

        let expires_at = (Utc::now() + Duration::hours(PREPARED_OUTPUT_TTL_HOURS)).to_rfc3339();
        let prepared = self.state.reserve_output(
            output_dir.as_path(),
            params,
            invocation.call_id.clone(),
            expires_at,
        )?;

        enforce_artifact_path_policy(
            &invocation,
            FilePolicyOperation::Write,
            prepared.output_path.as_path(),
            "artifact_prepare output path",
        )?;

        let response = ArtifactPrepareResponse {
            output_path: prepared.output_path.display().to_string(),
            output_dir: prepared.output_dir.display().to_string(),
            expires_at: prepared.expires_at.clone(),
            display_name: prepared.display_name.clone(),
        };
        let payload = serde_json::to_value(&response).map_err(|error| {
            ToolError::internal(format!(
                "failed to serialize artifact_prepare response: {error}"
            ))
        })?;
        let rendered = format!(
            "Prepared artifact output path `{}` in `{}`.",
            response.output_path, response.output_dir
        );

        Ok(Box::new(FunctionToolOutput::with_payload(
            rendered, true, payload,
        )))
    }

    async fn handle_register(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input = decode_artifact_tool_args::<ArtifactRegisterParams>(invocation.clone())?;
        let outcome = register_artifact_for_invocation(
            self.artifact_service.as_ref(),
            &self.context,
            self.state.as_ref(),
            &invocation,
            input,
        )
        .await?;
        self.emit_artifact_register_notifications(&outcome.summary)
            .await;

        serde_json::to_value(&outcome.response)
            .map(function_output)
            .map_err(|error| {
                ToolError::internal(format!(
                    "failed to serialize artifact_register response: {error}"
                ))
            })
    }

    async fn handle_read(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input = decode_artifact_tool_args::<ArtifactReadToolParams>(invocation)?;
        let max_bytes = input
            .max_bytes
            .map(|value| value.clamp(1, ARTIFACT_READ_MAX_BYTES))
            .unwrap_or(ARTIFACT_READ_MAX_BYTES);
        let projection_kind = input.projection_kind;
        let response = self
            .artifact_service
            .read_artifact(
                ArtifactReadParams {
                    workspace_id: self.context.workspace_id.clone(),
                    artifact_id: input.artifact_id,
                    version_id: input.version_id,
                    projection_kind,
                    offset: input.offset,
                    max_bytes: Some(max_bytes),
                },
                ARTIFACT_READ_MAX_BYTES,
            )
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;

        let bytes = BASE64
            .decode(response.content_base64.as_bytes())
            .map_err(|error| {
                ToolError::internal(format!("failed to decode artifact bytes: {error}"))
            })?;
        let text = artifact_read_text(
            response.artifact.kind,
            response.artifact.mime_type.as_deref(),
            projection_kind,
            bytes.as_slice(),
        );
        let attachment = if text.is_none() {
            Some(
                self.artifact_service
                    .resolve_provider_attachment(
                        self.context.workspace_id.as_str(),
                        response.artifact.artifact_id.as_str(),
                        response.artifact.version_id.as_deref(),
                    )
                    .await
                    .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?,
            )
        } else {
            None
        };

        let mut payload = json!({
            "artifact": response.artifact,
            "offset": response.offset,
            "len": response.len,
            "totalSizeBytes": response.total_size_bytes,
            "sha256": response.sha256,
            "truncated": response.truncated,
            "nextOffset": if response.truncated {
                Some(response.offset.saturating_add(response.len))
            } else {
                None
            },
        });
        if let Some(text) = text {
            payload["text"] = JsonValue::String(text);
        }
        if let Some(resolved) = attachment
            && let AttachmentDataSource::Path { path } = resolved.attachment.source
        {
            payload["llm_context"] = json!({
                "attachment": {
                    "path": path,
                    "mime_type": resolved.attachment.mime_type,
                    "name": resolved.attachment.name,
                    "size_bytes": resolved.attachment.size_bytes,
                    "sha256": resolved.attachment.sha256,
                }
            });
        }

        let rendered = artifact_read_rendered_text(&payload);
        Ok(Box::new(FunctionToolOutput::with_payload(
            rendered, true, payload,
        )))
    }

    async fn emit_artifact_register_notifications(&self, summary: &ArtifactSummary) {
        self.notification_sink
            .send(
                self.context.thread_id.as_str(),
                ArtifactToolNotification::ArtifactCreated(ArtifactCreatedNotification {
                    workspace_id: self.context.workspace_id.clone(),
                    artifact: summary.clone(),
                }),
            )
            .await;

        self.notification_sink
            .send(
                self.context.thread_id.as_str(),
                ArtifactToolNotification::ThreadArtifactsChanged(
                    artifact_register_thread_changed_notification(
                        &self.context,
                        summary.artifact.artifact_id.clone(),
                        Utc::now().timestamp(),
                    ),
                ),
            )
            .await;

        let Some(version_id) = summary.artifact.version_id.as_deref() else {
            return;
        };
        let projections = self
            .artifact_service
            .list_projections(
                self.context.workspace_id.as_str(),
                summary.artifact.artifact_id.as_str(),
                Some(version_id),
            )
            .await
            .unwrap_or_default();
        for notification in artifact_register_projection_notifications(
            &self.context,
            projections.as_slice(),
            Utc::now().timestamp(),
        ) {
            self.notification_sink
                .send(
                    self.context.thread_id.as_str(),
                    ArtifactToolNotification::ArtifactProjectionUpdated(notification),
                )
                .await;
        }
    }
}

#[async_trait]
impl ToolHandler for ArtifactToolHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: pioneer_tools::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        match invocation.tool_name.as_str() {
            ARTIFACT_PREPARE_TOOL => self.handle_prepare(invocation).await,
            ARTIFACT_REGISTER_TOOL => self.handle_register(invocation).await,
            ARTIFACT_READ_TOOL => self.handle_read(invocation).await,
            other => Err(ToolError::NotFound(other.to_owned())),
        }
    }
}

pub fn artifact_tool_specs() -> Vec<ConfiguredToolSpec> {
    vec![
        artifact_tool_spec(
            ARTIFACT_PREPARE_TOOL,
            "Reserve a safe local output path for a file you intend to create for the user. Write the file only to the returned outputPath, then call artifact_register after writing it.",
            artifact_prepare_schema(),
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Arguments,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 1,
                can_resume: false,
                max_wall_clock_secs: None,
            },
        ),
        artifact_tool_spec(
            ARTIFACT_REGISTER_TOOL,
            "Register a file you created into the artifact store. Path must be inside the current workspace or the artifact output dir returned by artifact_prepare. If path is a copy or moved version of a prepared output, pass preparedOutputPath with the original outputPath. Do not register arbitrary system files.",
            artifact_register_schema(),
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Arguments,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 1,
                can_resume: false,
                max_wall_clock_secs: None,
            },
        ),
        artifact_tool_spec(
            ARTIFACT_READ_TOOL,
            "Read a referenced artifact from the current workspace. Use this only when a prior message or recalled thread snippet includes an artifactId and the artifact content is needed for the current answer. Text artifacts return text; binary artifacts return a provider attachment.",
            artifact_read_schema(),
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Arguments,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 1,
                can_resume: false,
                max_wall_clock_secs: None,
            },
        ),
    ]
}

fn artifact_tool_spec(
    name: &str,
    description: &str,
    parameters: JsonValue,
    recovery: ToolRecoveryMetadata,
) -> ConfiguredToolSpec {
    ConfiguredToolSpec::with_output_projection(
        ToolSpec::new(name, description, parameters, PayloadKind::Function).with_recovery(recovery),
        ExecutionClass::Shared,
        dynamic_unknown_output_policy(),
        ToolOutputProjectionKind::DynamicGeneric,
    )
}

fn artifact_prepare_schema() -> JsonValue {
    tool_input_schema::<ArtifactPrepareParams>()
}

fn artifact_register_schema() -> JsonValue {
    tool_input_schema::<ArtifactRegisterParams>()
}

fn artifact_read_schema() -> JsonValue {
    tool_input_schema::<ArtifactReadToolParams>()
}

fn artifact_tool_schema_for_name(tool_name: &str) -> JsonValue {
    match tool_name {
        ARTIFACT_PREPARE_TOOL => artifact_prepare_schema(),
        ARTIFACT_REGISTER_TOOL => artifact_register_schema(),
        ARTIFACT_READ_TOOL => artifact_read_schema(),
        _ => json!({ "type": "object" }),
    }
}

fn tool_input_schema<T>() -> JsonValue
where
    T: JsonSchema,
{
    let mut schema = serde_json::to_value(schema_for!(T)).expect("tool schema should serialize");
    if let Some(object) = schema.as_object_mut() {
        object.remove("$schema");
    }
    schema
}

fn decode_artifact_tool_args<T>(invocation: ToolInvocation) -> Result<T, ToolError>
where
    T: serde::de::DeserializeOwned,
{
    let tool_name = invocation.tool_name.clone();
    let ToolPayload::Function { arguments } = invocation.payload else {
        return Err(ToolError::invalid_arguments(format!(
            "expected function payload for `{tool_name}`"
        )));
    };
    let schema = artifact_tool_schema_for_name(tool_name.as_str());
    let arguments = normalize_tool_arguments_from_schema(arguments, &schema)
        .map_err(|error| {
            ToolError::invalid_arguments(format!(
                "{error}. {}",
                artifact_tool_argument_hint(tool_name.as_str())
            ))
        })?
        .arguments;
    serde_json::from_value(arguments).map_err(|error| {
        ToolError::invalid_arguments(format!(
            "invalid arguments for `{tool_name}`: {error}. {}",
            artifact_tool_argument_hint(tool_name.as_str())
        ))
    })
}

fn artifact_tool_argument_hint(tool_name: &str) -> &'static str {
    match tool_name {
        ARTIFACT_PREPARE_TOOL => {
            "Expected fields: displayName and kind, with optional mimeType and description. After writing the file to outputPath, call artifact_register."
        }
        ARTIFACT_REGISTER_TOOL => {
            "Expected field: path, with optional displayName, kind, mimeType, description, and preparedOutputPath. The path must be inside the current workspace or PIONEER_ARTIFACT_OUTPUT_DIR."
        }
        ARTIFACT_READ_TOOL => {
            "Expected field: artifactId, with optional versionId, projectionKind, offset, and maxBytes. Do not pass workspaceId; the current workspace is used automatically."
        }
        _ => "Check the tool schema and use the documented camelCase fields.",
    }
}

fn enforce_artifact_path_policy(
    invocation: &ToolInvocation,
    operation: FilePolicyOperation,
    path: &Path,
    label: &str,
) -> Result<(), ToolError> {
    if artifact_output_dir_allows(invocation, operation, path) {
        return Ok(());
    }

    let Some(snapshot) = invocation.execution_security_snapshot.as_ref() else {
        return Ok(());
    };
    match FilePolicyChecker::check(snapshot, operation, path) {
        FilePolicyDecision::Allowed(_) => Ok(()),
        FilePolicyDecision::Denied(deny) => Err(ToolError::Rejected(format!(
            "filesystem sandbox denied {:?} for {label} `{}`: {}",
            deny.operation,
            deny.requested_path.display(),
            deny.message
        ))),
    }
}

fn artifact_output_dir_allows(
    invocation: &ToolInvocation,
    operation: FilePolicyOperation,
    path: &Path,
) -> bool {
    let Some(output_dir) = invocation
        .environment
        .get(ARTIFACT_OUTPUT_DIR_ENV)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
    else {
        return false;
    };

    match (
        std::fs::canonicalize(output_dir),
        resolve_artifact_policy_path(operation, path),
    ) {
        (Ok(output_dir), Ok(path)) => path.starts_with(output_dir.as_path()),
        _ => {
            let output_dir = normalize_artifact_path(Path::new(output_dir));
            let path = normalize_artifact_path(path);
            path.starts_with(output_dir.as_path())
        }
    }
}

fn resolve_artifact_policy_path(
    operation: FilePolicyOperation,
    path: &Path,
) -> std::io::Result<PathBuf> {
    match std::fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                && operation == FilePolicyOperation::Write =>
        {
            let Some(parent) = path.parent() else {
                return Err(error);
            };
            let Some(file_name) = path.file_name() else {
                return Err(error);
            };
            Ok(std::fs::canonicalize(parent)?.join(file_name))
        }
        Err(error) => Err(error),
    }
}

fn normalize_artifact_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(value) => normalized.push(value),
            Component::RootDir | Component::Prefix(_) => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

#[derive(Debug)]
struct ArtifactRegisterOutcome {
    response: ArtifactRegisterResponse,
    summary: ArtifactSummary,
}

fn artifact_register_thread_changed_notification(
    context: &ArtifactToolContext,
    artifact_id: String,
    generated_at: i64,
) -> ThreadArtifactsChangedNotification {
    ThreadArtifactsChangedNotification {
        workspace_id: context.workspace_id.clone(),
        thread_id: context.thread_id.clone(),
        artifact_ids: vec![artifact_id],
        reason: "artifact_register".to_owned(),
        generated_at,
    }
}

fn artifact_register_projection_notifications(
    context: &ArtifactToolContext,
    projections: &[ArtifactProjectionRecord],
    updated_at: i64,
) -> Vec<ArtifactProjectionUpdatedNotification> {
    projections
        .iter()
        .map(|projection| ArtifactProjectionUpdatedNotification {
            workspace_id: context.workspace_id.clone(),
            artifact_id: projection.artifact_id.clone(),
            version_id: projection.artifact_version_id.clone(),
            projection_kind: projection.projection_kind,
            status: projection.status,
            updated_at,
        })
        .collect()
}

async fn register_artifact_for_invocation(
    artifact_service: &ArtifactService,
    context: &ArtifactToolContext,
    artifact_state: &ArtifactToolState,
    invocation: &ToolInvocation,
    input: ArtifactRegisterParams,
) -> Result<ArtifactRegisterOutcome, ToolError> {
    let workspace_root = artifact_register_workspace_root(invocation)?;
    let source_path =
        resolve_artifact_register_path(workspace_root.as_path(), input.path.as_str())?;
    enforce_artifact_path_policy(
        invocation,
        FilePolicyOperation::Read,
        source_path.as_path(),
        "artifact_register source path",
    )?;
    let prepared_output_path = input
        .prepared_output_path
        .as_deref()
        .map(|path| resolve_artifact_register_path(workspace_root.as_path(), path))
        .transpose()?;
    if let Some(prepared_output_path) = prepared_output_path.as_ref() {
        enforce_artifact_path_policy(
            invocation,
            FilePolicyOperation::Read,
            prepared_output_path.as_path(),
            "artifact_register prepared output path",
        )?;
    }
    let allowed_roots =
        artifact_register_allowed_roots(invocation, workspace_root.as_path()).await?;
    let source_canonical = tokio::fs::canonicalize(source_path.as_path()).await.ok();
    let output_root_canonical = invocation
        .environment
        .get(ARTIFACT_OUTPUT_DIR_ENV)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
        .and_then(|path| std::fs::canonicalize(path).ok());
    let prepared_canonical = prepared_output_path
        .as_ref()
        .and_then(|path| std::fs::canonicalize(path).ok());
    let prepared_registration_plan = resolve_prepared_registration_plan(
        artifact_state,
        source_canonical.as_deref(),
        prepared_output_path.as_deref(),
        prepared_canonical.as_deref(),
    )
    .await?;
    let cleanup_source_after_success = source_canonical.as_ref().is_some_and(|path| {
        prepared_canonical.as_ref() == Some(path)
            || output_root_canonical
                .as_ref()
                .is_some_and(|root| path.starts_with(root))
            || artifact_state.prepared_output_for_path(path).is_some()
    });
    if cleanup_source_after_success {
        enforce_artifact_path_policy(
            invocation,
            FilePolicyOperation::Write,
            source_path.as_path(),
            "artifact_register cleanup source path",
        )?;
    }
    for prepared in &prepared_registration_plan.prepared_outputs {
        let prepared_canonical = std::fs::canonicalize(prepared.output_path.as_path()).ok();
        if prepared_canonical.as_ref() != source_canonical.as_ref() {
            enforce_artifact_path_policy(
                invocation,
                FilePolicyOperation::Write,
                prepared.output_path.as_path(),
                "artifact_register cleanup prepared output path",
            )?;
        }
    }

    let registration_context = ArtifactRegistrationContext {
        workspace_id: context.workspace_id.clone(),
        thread_id: context.thread_id.clone(),
        turn_id: context.turn_id.clone(),
        message_id: None,
        turn_item_id: Some(invocation.call_id.clone()),
        tool_call_id: Some(invocation.call_id.clone()),
        created_by_kind: ArtifactCreatedByKind::Agent,
        created_by_actor_id: Some("artifact_register".to_owned()),
        item_index: None,
        binding_kind: ArtifactBindingKind::AgentOutput,
        binding_direction: ArtifactBindingDirection::Output,
        binding_role: Some(ArtifactRole::Assistant),
        allowed_roots,
        max_file_bytes: None,
        cleanup_source_after_success,
    };
    let candidate = ArtifactRegistrationCandidate {
        path: source_path,
        display_name: clean_optional_string(input.display_name),
        mime_type: clean_optional_string(input.mime_type),
        kind_hint: input.kind.map(artifact_prepare_kind_to_artifact_kind),
        description: clean_optional_string(input.description),
        sha256: None,
        size_bytes: None,
        source: ArtifactRegistrationSource::ExplicitArtifactRegister,
    };

    let summary = artifact_service
        .register_candidate(registration_context, candidate)
        .await
        .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;

    let artifact = &summary.artifact;
    let version_id = artifact.version_id.clone().ok_or_else(|| {
        ToolError::internal(format!(
            "registered artifact `{}` has no version id",
            artifact.artifact_id
        ))
    })?;
    for prepared in prepared_registration_plan.prepared_outputs {
        artifact_state.mark_registered(
            prepared.output_path.as_path(),
            artifact.artifact_id.as_str(),
            version_id.as_str(),
        );
        let prepared_canonical = std::fs::canonicalize(prepared.output_path.as_path()).ok();
        if prepared_canonical.as_ref() != source_canonical.as_ref() {
            let _ = crate::output_dir::cleanup_artifact_output_file(prepared.output_path.as_path())
                .await;
        }
    }
    let size_bytes = artifact.size_bytes.ok_or_else(|| {
        ToolError::internal(format!(
            "registered artifact `{}` has no size",
            artifact.artifact_id
        ))
    })?;
    let sha256 = artifact.sha256.clone().ok_or_else(|| {
        ToolError::internal(format!(
            "registered artifact `{}` has no sha256",
            artifact.artifact_id
        ))
    })?;
    let response = ArtifactRegisterResponse {
        artifact_id: artifact.artifact_id.clone(),
        version_id,
        display_name: artifact.display_name.clone(),
        kind: artifact.kind,
        mime_type: artifact.mime_type.clone(),
        size_bytes,
        sha256,
    };
    Ok(ArtifactRegisterOutcome { response, summary })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileFingerprint {
    size_bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, Default)]
struct PreparedRegistrationPlan {
    prepared_outputs: Vec<PreparedArtifactOutput>,
}

async fn resolve_prepared_registration_plan(
    artifact_state: &ArtifactToolState,
    source_canonical: Option<&Path>,
    explicit_prepared_path: Option<&Path>,
    explicit_prepared_canonical: Option<&Path>,
) -> Result<PreparedRegistrationPlan, ToolError> {
    if let Some(explicit_path) = explicit_prepared_path {
        let explicit_prepared = find_prepared_output(
            artifact_state,
            &[explicit_prepared_canonical, Some(explicit_path)],
        )
        .ok_or_else(|| {
            ToolError::invalid_arguments(
                "`preparedOutputPath` was not returned by artifact_prepare in this turn",
            )
        })?;

        if let Some(source_path) = source_canonical {
            if let Some(source_prepared) =
                find_prepared_output(artifact_state, &[Some(source_path)])
                && source_prepared.output_path != explicit_prepared.output_path
            {
                return Err(ToolError::invalid_arguments(
                    "`path` and `preparedOutputPath` reference different prepared outputs",
                ));
            }

            validate_prepared_source_content_match(
                source_path,
                explicit_prepared.output_path.as_path(),
            )
            .await?;
        }

        return Ok(PreparedRegistrationPlan {
            prepared_outputs: vec![explicit_prepared],
        });
    }

    if let Some(source_path) = source_canonical
        && let Some(source_prepared) = find_prepared_output(artifact_state, &[Some(source_path)])
    {
        return Ok(PreparedRegistrationPlan {
            prepared_outputs: vec![source_prepared],
        });
    }

    let Some(source_path) = source_canonical else {
        return Ok(PreparedRegistrationPlan::default());
    };
    let Some(source_fingerprint) = file_fingerprint(source_path).await? else {
        return Ok(PreparedRegistrationPlan::default());
    };

    let mut matches = Vec::new();
    for prepared in artifact_state
        .prepared_outputs()
        .into_iter()
        .filter(PreparedArtifactOutput::is_reserved)
    {
        let Some(prepared_fingerprint) = file_fingerprint(prepared.output_path.as_path()).await?
        else {
            continue;
        };
        if prepared_fingerprint == source_fingerprint {
            matches.push(prepared);
        }
    }

    match matches.len() {
        0 => Ok(PreparedRegistrationPlan::default()),
        1 => Ok(PreparedRegistrationPlan {
            prepared_outputs: matches,
        }),
        _ => Err(ToolError::invalid_arguments(
            "registered file matches multiple prepared outputs; pass preparedOutputPath",
        )),
    }
}

fn find_prepared_output(
    artifact_state: &ArtifactToolState,
    paths: &[Option<&Path>],
) -> Option<PreparedArtifactOutput> {
    paths
        .iter()
        .filter_map(|path| *path)
        .find_map(|path| artifact_state.prepared_output_for_path(path))
}

async fn validate_prepared_source_content_match(
    source_path: &Path,
    prepared_path: &Path,
) -> Result<(), ToolError> {
    let source_fingerprint = file_fingerprint(source_path).await?;
    let prepared_fingerprint = file_fingerprint(prepared_path).await?;
    if let (Some(source), Some(prepared)) = (source_fingerprint, prepared_fingerprint)
        && source != prepared
    {
        return Err(ToolError::invalid_arguments(
            "`path` content does not match `preparedOutputPath`",
        ));
    }
    Ok(())
}

async fn file_fingerprint(path: &Path) -> Result<Option<FileFingerprint>, ToolError> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ToolError::execution_failed(format!(
                "failed to inspect prepared artifact output `{}`: {error}",
                path.display()
            )));
        }
    };
    if !metadata.is_file() {
        return Ok(None);
    }

    let bytes = tokio::fs::read(path).await.map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to read prepared artifact output `{}`: {error}",
            path.display()
        ))
    })?;
    let mut hasher = Sha256::new();
    hasher.update(bytes.as_slice());
    Ok(Some(FileFingerprint {
        size_bytes: metadata.len(),
        sha256: hex::encode(hasher.finalize()),
    }))
}

fn artifact_register_workspace_root(invocation: &ToolInvocation) -> Result<PathBuf, ToolError> {
    if invocation.workdir.as_os_str().is_empty() {
        return std::env::current_dir().map_err(|error| {
            ToolError::execution_failed(format!(
                "failed to resolve current workspace root: {error}"
            ))
        });
    }
    Ok(invocation.workdir.clone())
}

fn resolve_artifact_register_path(root: &Path, path: &str) -> Result<PathBuf, ToolError> {
    let path = required_tool_string(Some(path), "path")?;
    let path = PathBuf::from(path);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(root.join(path))
    }
}

async fn artifact_register_allowed_roots(
    invocation: &ToolInvocation,
    workspace_root: &Path,
) -> Result<Vec<PathBuf>, ToolError> {
    let mut roots = Vec::new();
    push_existing_artifact_root(&mut roots, workspace_root).await?;
    if let Some(output_dir) = invocation
        .environment
        .get(ARTIFACT_OUTPUT_DIR_ENV)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        push_existing_artifact_root(&mut roots, Path::new(output_dir)).await?;
    }
    if roots.is_empty() {
        return Err(ToolError::execution_failed(
            "artifact_register has no existing allowed roots",
        ));
    }
    Ok(roots)
}

async fn push_existing_artifact_root(
    roots: &mut Vec<PathBuf>,
    root: &Path,
) -> Result<(), ToolError> {
    let metadata = match tokio::fs::metadata(root).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(ToolError::execution_failed(format!(
                "failed to inspect artifact_register allowed root `{}`: {error}",
                root.display()
            )));
        }
    };
    if !metadata.is_dir() {
        return Ok(());
    }
    if !roots.iter().any(|existing| existing == root) {
        roots.push(root.to_path_buf());
    }
    Ok(())
}

fn artifact_prepare_kind_to_artifact_kind(kind: ArtifactPrepareKind) -> ArtifactKind {
    match kind {
        ArtifactPrepareKind::Image => ArtifactKind::Image,
        ArtifactPrepareKind::Document => ArtifactKind::File,
        ArtifactPrepareKind::Data => ArtifactKind::Json,
        ArtifactPrepareKind::Archive => ArtifactKind::Archive,
        ArtifactPrepareKind::Code => ArtifactKind::WorkspaceFile,
        ArtifactPrepareKind::Log => ArtifactKind::Text,
        ArtifactPrepareKind::Other => ArtifactKind::File,
    }
}

fn function_output(payload: JsonValue) -> Box<dyn ToolOutput> {
    let text = serde_json::to_string_pretty(&payload).unwrap_or_else(|_| payload.to_string());
    Box::new(FunctionToolOutput::with_payload(text, true, payload))
}

fn artifact_read_text(
    kind: ArtifactKind,
    mime_type: Option<&str>,
    projection_kind: Option<ArtifactProjectionKind>,
    bytes: &[u8],
) -> Option<String> {
    if !artifact_read_is_textual(kind, mime_type, projection_kind) {
        return None;
    }
    std::str::from_utf8(bytes).ok().map(ToOwned::to_owned)
}

fn artifact_read_is_textual(
    kind: ArtifactKind,
    mime_type: Option<&str>,
    projection_kind: Option<ArtifactProjectionKind>,
) -> bool {
    if matches!(
        projection_kind,
        Some(ArtifactProjectionKind::PlainText)
            | Some(ArtifactProjectionKind::JsonSummary)
            | Some(ArtifactProjectionKind::PdfText)
    ) {
        return true;
    }
    if matches!(
        kind,
        ArtifactKind::Text
            | ArtifactKind::Json
            | ArtifactKind::WorkspaceFile
            | ArtifactKind::DirectoryManifest
    ) {
        return true;
    }
    let Some(mime_type) = mime_type.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    mime_type.starts_with("text/")
        || matches!(
            mime_type,
            "application/json"
                | "application/x-ndjson"
                | "application/xml"
                | "application/yaml"
                | "application/toml"
        )
}

fn artifact_read_rendered_text(payload: &JsonValue) -> String {
    let artifact = payload.get("artifact").unwrap_or(&JsonValue::Null);
    let artifact_id = artifact
        .get("artifactId")
        .or_else(|| artifact.get("artifact_id"))
        .and_then(JsonValue::as_str)
        .unwrap_or("unknown");
    let name = artifact
        .get("displayName")
        .or_else(|| artifact.get("display_name"))
        .and_then(JsonValue::as_str)
        .unwrap_or("artifact");
    if let Some(text) = payload.get("text").and_then(JsonValue::as_str) {
        return format!("Read artifact `{name}` ({artifact_id}).\n\n{text}");
    }
    if payload.pointer("/llm_context/attachment").is_some() {
        return format!(
            "Read artifact `{name}` ({artifact_id}) as an attachment for model inspection."
        );
    }
    serde_json::to_string_pretty(payload).unwrap_or_else(|_| payload.to_string())
}

fn required_tool_string(value: Option<&str>, field: &str) -> Result<String, ToolError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(ToolError::invalid_arguments(format!(
            "`{field}` is required"
        )));
    };
    Ok(value.to_owned())
}

fn clean_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn sanitize_display_name(display_name: &str) -> String {
    let mut name = display_name
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_control() || matches!(ch, '/' | '\\' | '\0') {
                '_'
            } else {
                ch
            }
        })
        .collect::<String>();

    while name.contains("__") {
        name = name.replace("__", "_");
    }
    name = name
        .trim_matches(|ch: char| ch.is_whitespace() || ch == '.')
        .to_owned();
    if name.is_empty() || name == "." || name == ".." {
        name = "artifact".to_owned();
    }

    truncate_filename(name.as_str(), MAX_FILENAME_CHARS)
}

fn truncate_filename(name: &str, max_chars: usize) -> String {
    if name.chars().count() <= max_chars {
        return name.to_owned();
    }

    let (stem, extension) = split_filename(name);
    let extension_len = extension
        .as_ref()
        .map(|value| value.chars().count() + 1)
        .unwrap_or(0);
    let stem_budget = max_chars.saturating_sub(extension_len).max(1);
    let mut truncated = stem.chars().take(stem_budget).collect::<String>();
    if let Some(extension) = extension {
        truncated.push('.');
        truncated.push_str(extension.as_str());
    }
    truncated
}

fn split_filename(name: &str) -> (String, Option<String>) {
    match name.rsplit_once('.') {
        Some((stem, extension)) if !stem.is_empty() && !extension.is_empty() => {
            (stem.to_owned(), Some(extension.to_owned()))
        }
        _ => (name.to_owned(), None),
    }
}

fn numbered_filename(stem: &str, extension: Option<&str>, sequence: u64) -> String {
    let name = if sequence == 1 {
        stem.to_owned()
    } else {
        format!("{stem}-{sequence}")
    };
    match extension {
        Some(extension) => format!("{name}.{extension}"),
        None => name,
    }
}

fn path_key(path: &Path) -> String {
    path.display().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::CrudStore;
    use pioneer_protocol::{
        TurnExecutionSecuritySnapshot, TurnFilesystemAccess, TurnFilesystemSandboxEntry,
        TurnPermissionMode, TurnPermissionProfileSnapshot, TurnPermissionProfileSource,
    };
    use pioneer_tools::{ToolCallSource, ToolEventBus, ToolInvocation};
    use sea_orm::Database;
    use serde_json::Value as JsonValue;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio_util::sync::CancellationToken;

    fn temp_output_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "pioneer-artifact-prepare-{label}-{nanos}-{}",
            std::process::id()
        ))
    }

    fn workspace_write_security_snapshot(root: &Path) -> TurnExecutionSecuritySnapshot {
        TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            root.to_string_lossy(),
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                root.to_string_lossy(),
            )],
            1,
        )
    }

    fn invocation(
        output_dir: Option<&Path>,
        arguments: JsonValue,
        call_id: &str,
    ) -> ToolInvocation {
        let mut environment = BTreeMap::new();
        if let Some(output_dir) = output_dir {
            environment.insert(
                ARTIFACT_OUTPUT_DIR_ENV.to_owned(),
                output_dir.display().to_string(),
            );
        }

        ToolInvocation {
            call_id: call_id.to_owned(),
            tool_name: ARTIFACT_PREPARE_TOOL.to_owned(),
            source: ToolCallSource::Model,
            payload: ToolPayload::Function { arguments },
            workdir: std::env::current_dir().expect("cwd must be available"),
            environment,
            attempt_id: 1,
            idempotency_key: None,
            recovery: ToolRecoveryMetadata::default(),
            permission_metadata: pioneer_tools::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
            cancellation: CancellationToken::new(),
        }
    }

    fn with_execution_security_snapshot(
        mut invocation: ToolInvocation,
        snapshot: TurnExecutionSecuritySnapshot,
    ) -> ToolInvocation {
        invocation.execution_security_snapshot = Some(snapshot);
        invocation
    }

    fn trace() -> pioneer_tools::ToolEventTrace {
        ToolEventBus::default().start_trace("turn_test", "call_1", ARTIFACT_PREPARE_TOOL)
    }

    #[test]
    fn domain_map_matches_artifact_tool_specs() {
        let specs = artifact_tool_specs();
        let actual = specs
            .iter()
            .map(|configured| configured.spec.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            actual.as_slice(),
            pioneer_tools::BuiltinToolDomain::Artifact.tool_names()
        );
    }

    #[tokio::test]
    async fn artifact_prepare_returns_safe_path_inside_output_dir() {
        let output_dir = temp_output_dir("safe");
        let state = Arc::new(ArtifactToolState::default());
        let handler = ArtifactToolHandler::new(
            Arc::new(artifact_register_service(temp_output_dir("runtime")).await),
            artifact_register_context(),
            state.clone(),
            Arc::new(NoopArtifactToolNotificationSink),
        );

        let output = handler
            .handle(
                invocation(
                    Some(output_dir.as_path()),
                    serde_json::json!({
                        "displayName": "../../report.png",
                        "kind": "image",
                        "mimeType": "image/png"
                    }),
                    "call_safe",
                ),
                trace(),
            )
            .await
            .expect("prepare should succeed");

        let payload = output.raw_json();
        let output_path = PathBuf::from(payload["outputPath"].as_str().expect("path"));
        let canonical_output_dir = std::fs::canonicalize(output_dir.as_path()).expect("dir");
        assert!(output_path.starts_with(canonical_output_dir.as_path()));
        assert!(
            !payload["displayName"]
                .as_str()
                .expect("display")
                .contains('/')
        );
        assert_eq!(state.prepared_outputs().len(), 1);
        assert!(
            state
                .prepared_output_for_path(output_path.as_path())
                .is_some()
        );

        let _ = std::fs::remove_dir_all(canonical_output_dir);
    }

    #[tokio::test]
    async fn artifact_prepare_adds_mime_extension_to_extensionless_name() {
        let output_dir = temp_output_dir("extension");
        let state = Arc::new(ArtifactToolState::default());
        let handler = ArtifactToolHandler::new(
            Arc::new(artifact_register_service(temp_output_dir("runtime")).await),
            artifact_register_context(),
            state,
            Arc::new(NoopArtifactToolNotificationSink),
        );

        let output = handler
            .handle(
                invocation(
                    Some(output_dir.as_path()),
                    serde_json::json!({
                        "displayName": "auto.ru_screenshot",
                        "kind": "image",
                        "mimeType": "image/png"
                    }),
                    "call_extension",
                ),
                trace(),
            )
            .await
            .expect("prepare should succeed")
            .raw_json();

        assert_eq!(
            output["displayName"].as_str(),
            Some("auto.ru_screenshot.png")
        );
        assert!(
            output["outputPath"]
                .as_str()
                .expect("output path")
                .ends_with("auto.ru_screenshot.png")
        );

        let _ = std::fs::remove_dir_all(output_dir);
    }

    #[tokio::test]
    async fn artifact_prepare_duplicate_display_names_are_deterministic() {
        let output_dir = temp_output_dir("dupe");
        let state = Arc::new(ArtifactToolState::default());
        let handler = ArtifactToolHandler::new(
            Arc::new(artifact_register_service(temp_output_dir("runtime")).await),
            artifact_register_context(),
            state.clone(),
            Arc::new(NoopArtifactToolNotificationSink),
        );
        let args = serde_json::json!({
            "displayName": "chart.csv",
            "kind": "data"
        });

        let first = handler
            .handle(
                invocation(Some(output_dir.as_path()), args.clone(), "call_first"),
                trace(),
            )
            .await
            .expect("first prepare should succeed")
            .raw_json();
        let second = handler
            .handle(
                invocation(Some(output_dir.as_path()), args, "call_second"),
                trace(),
            )
            .await
            .expect("second prepare should succeed")
            .raw_json();

        assert!(
            first["outputPath"]
                .as_str()
                .expect("first")
                .ends_with("chart.csv")
        );
        assert!(
            second["outputPath"]
                .as_str()
                .expect("second")
                .ends_with("chart-2.csv")
        );
        assert_eq!(state.prepared_outputs().len(), 2);

        let _ = std::fs::remove_dir_all(output_dir);
    }

    #[tokio::test]
    async fn artifact_prepare_requires_output_dir_environment() {
        let handler = ArtifactToolHandler::new(
            Arc::new(artifact_register_service(temp_output_dir("runtime")).await),
            artifact_register_context(),
            Arc::new(ArtifactToolState::default()),
            Arc::new(NoopArtifactToolNotificationSink),
        );

        let error = match handler
            .handle(
                invocation(
                    None,
                    serde_json::json!({
                        "displayName": "image.png",
                        "kind": "image"
                    }),
                    "call_missing_env",
                ),
                trace(),
            )
            .await
        {
            Ok(_) => panic!("missing env should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("PIONEER_ARTIFACT_OUTPUT_DIR"));
    }

    #[tokio::test]
    async fn artifact_prepare_allows_app_output_dir_outside_snapshot() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace_root = temp.path().join("workspace");
        let output_dir = temp.path().join("artifact-output");
        tokio::fs::create_dir_all(workspace_root.as_path())
            .await
            .expect("create workspace");
        let state = Arc::new(ArtifactToolState::default());
        let handler = ArtifactToolHandler::new(
            Arc::new(artifact_register_service(temp.path().join("runtime")).await),
            artifact_register_context(),
            state.clone(),
            Arc::new(NoopArtifactToolNotificationSink),
        );
        let snapshot = workspace_write_security_snapshot(workspace_root.as_path());

        let output = handler
            .handle(
                with_execution_security_snapshot(
                    invocation(
                        Some(output_dir.as_path()),
                        serde_json::json!({
                            "displayName": "image.png",
                            "kind": "image"
                        }),
                        "call_app_output_dir",
                    ),
                    snapshot,
                ),
                trace(),
            )
            .await
            .expect("artifact output dir should be allowed even outside workspace snapshot")
            .raw_json();

        let output_path = PathBuf::from(output["outputPath"].as_str().expect("output path"));
        let canonical_output_dir = std::fs::canonicalize(output_dir.as_path()).expect("output dir");
        assert!(output_path.starts_with(canonical_output_dir.as_path()));
        assert_eq!(state.prepared_outputs().len(), 1);
    }

    async fn artifact_register_service(runtime_home: PathBuf) -> ArtifactService {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("connect sqlite");
        Migrator::up(&db, None).await.expect("migrate");
        ArtifactService::new(
            Arc::new(CrudStore::new(db)),
            Arc::new(crate::LocalArtifactBlobStore::new(runtime_home)),
        )
    }

    fn artifact_register_context() -> ArtifactToolContext {
        ArtifactToolContext {
            workspace_id: "ws_artifact_register".to_owned(),
            thread_id: "thr_artifact_register".to_owned(),
            turn_id: "turn_artifact_register".to_owned(),
        }
    }

    fn artifact_register_invocation(
        workdir: PathBuf,
        output_dir: Option<PathBuf>,
        path: String,
    ) -> ToolInvocation {
        artifact_register_invocation_with_prepared(workdir, output_dir, path, None)
    }

    fn artifact_register_invocation_with_prepared(
        workdir: PathBuf,
        output_dir: Option<PathBuf>,
        path: String,
        prepared_output_path: Option<String>,
    ) -> ToolInvocation {
        let mut arguments = serde_json::json!({
            "path": path,
            "displayName": "registered.txt",
            "kind": "document",
            "mimeType": "text/plain",
            "description": "registered from test"
        });
        if let Some(prepared_output_path) = prepared_output_path {
            arguments["preparedOutputPath"] = JsonValue::String(prepared_output_path);
        }
        let mut invocation = invocation(output_dir.as_deref(), arguments, "call_artifact_register");
        invocation.tool_name = ARTIFACT_REGISTER_TOOL.to_owned();
        invocation.workdir = workdir;
        invocation
    }

    fn reserve_prepared_output(
        state: &ArtifactToolState,
        output_dir: &Path,
        display_name: &str,
        call_id: &str,
    ) -> PreparedArtifactOutput {
        let canonical_output_dir = std::fs::canonicalize(output_dir).expect("canonical output dir");
        state
            .reserve_output(
                canonical_output_dir.as_path(),
                ArtifactPrepareParams {
                    display_name: display_name.to_owned(),
                    kind: ArtifactPrepareKind::Document,
                    mime_type: Some("text/plain".to_owned()),
                    description: None,
                },
                call_id.to_owned(),
                "2026-05-17T00:00:00Z".to_owned(),
            )
            .expect("reserve prepared output")
    }

    #[tokio::test]
    async fn artifact_register_registers_file_from_output_dir_and_cleans_up() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        let output_dir = temp.path().join("output");
        tokio::fs::create_dir_all(workspace.as_path())
            .await
            .expect("create workspace");
        tokio::fs::create_dir_all(output_dir.as_path())
            .await
            .expect("create output dir");
        let source_path = output_dir.join("report.txt");
        tokio::fs::write(source_path.as_path(), b"registered output")
            .await
            .expect("write source");
        let service = artifact_register_service(temp.path().join("runtime")).await;
        let state = ArtifactToolState::default();
        let invocation = with_execution_security_snapshot(
            artifact_register_invocation(
                workspace.clone(),
                Some(output_dir.clone()),
                source_path.display().to_string(),
            ),
            workspace_write_security_snapshot(workspace.as_path()),
        );

        let outcome = register_artifact_for_invocation(
            &service,
            &artifact_register_context(),
            &state,
            &invocation,
            decode_artifact_tool_args(invocation.clone()).expect("decode register input"),
        )
        .await
        .expect("register output artifact");

        assert_eq!(outcome.response.display_name, "registered.txt");
        assert_eq!(outcome.response.kind, ArtifactKind::File);
        assert!(
            !source_path.exists(),
            "output-dir source should be cleaned after successful registration"
        );
        let page = service
            .list_thread_artifacts(
                "ws_artifact_register",
                "thr_artifact_register",
                crate::ArtifactListFilter::default(),
            )
            .await
            .expect("list artifacts");
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            page.items[0].artifact.artifact_id,
            outcome.response.artifact_id
        );
    }

    #[tokio::test]
    async fn artifact_register_closes_explicit_prepared_output_when_registering_copy() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        let output_dir = temp.path().join("output");
        let downloads = workspace.join("Downloads");
        tokio::fs::create_dir_all(downloads.as_path())
            .await
            .expect("create downloads");
        tokio::fs::create_dir_all(output_dir.as_path())
            .await
            .expect("create output dir");
        let service = artifact_register_service(temp.path().join("runtime")).await;
        let state = ArtifactToolState::default();
        let prepared =
            reserve_prepared_output(&state, output_dir.as_path(), "report.txt", "call_prepare");
        tokio::fs::write(prepared.output_path.as_path(), b"registered copy")
            .await
            .expect("write prepared");
        let copy_path = downloads.join("report.txt");
        tokio::fs::copy(prepared.output_path.as_path(), copy_path.as_path())
            .await
            .expect("copy prepared output");
        let invocation = artifact_register_invocation_with_prepared(
            workspace.clone(),
            Some(output_dir.clone()),
            copy_path.display().to_string(),
            Some(prepared.output_path.display().to_string()),
        );

        let outcome = register_artifact_for_invocation(
            &service,
            &artifact_register_context(),
            &state,
            &invocation,
            decode_artifact_tool_args(invocation.clone()).expect("decode register input"),
        )
        .await
        .expect("register copied prepared output");

        let prepared_after = state
            .prepared_output_for_path(prepared.output_path.as_path())
            .expect("prepared output remains tracked");
        assert!(prepared_after.is_registered());
        assert_eq!(outcome.response.display_name, "registered.txt");
        assert!(
            !prepared.output_path.exists(),
            "staging source should be cleaned after copied registration"
        );
        assert!(copy_path.exists(), "user-requested copy should remain");
    }

    #[tokio::test]
    async fn artifact_register_infers_single_prepared_copy_by_fingerprint() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        let output_dir = temp.path().join("output");
        tokio::fs::create_dir_all(workspace.as_path())
            .await
            .expect("create workspace");
        tokio::fs::create_dir_all(output_dir.as_path())
            .await
            .expect("create output dir");
        let service = artifact_register_service(temp.path().join("runtime")).await;
        let state = ArtifactToolState::default();
        let prepared =
            reserve_prepared_output(&state, output_dir.as_path(), "report.txt", "call_prepare");
        tokio::fs::write(prepared.output_path.as_path(), b"same bytes")
            .await
            .expect("write prepared");
        let copy_path = workspace.join("report-copy.txt");
        tokio::fs::copy(prepared.output_path.as_path(), copy_path.as_path())
            .await
            .expect("copy prepared output");
        let invocation = artifact_register_invocation(
            workspace.clone(),
            Some(output_dir.clone()),
            copy_path.display().to_string(),
        );

        register_artifact_for_invocation(
            &service,
            &artifact_register_context(),
            &state,
            &invocation,
            decode_artifact_tool_args(invocation.clone()).expect("decode register input"),
        )
        .await
        .expect("register copied prepared output");

        let prepared_after = state
            .prepared_output_for_path(prepared.output_path.as_path())
            .expect("prepared output remains tracked");
        assert!(prepared_after.is_registered());
        assert!(
            !prepared.output_path.exists(),
            "matched staging source should be cleaned after registration"
        );
    }

    #[tokio::test]
    async fn artifact_register_rejects_ambiguous_prepared_copy_without_explicit_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        let output_dir = temp.path().join("output");
        tokio::fs::create_dir_all(workspace.as_path())
            .await
            .expect("create workspace");
        tokio::fs::create_dir_all(output_dir.as_path())
            .await
            .expect("create output dir");
        let service = artifact_register_service(temp.path().join("runtime")).await;
        let state = ArtifactToolState::default();
        let first = reserve_prepared_output(&state, output_dir.as_path(), "report.txt", "call_1");
        let second = reserve_prepared_output(&state, output_dir.as_path(), "report.txt", "call_2");
        tokio::fs::write(first.output_path.as_path(), b"same bytes")
            .await
            .expect("write first prepared");
        tokio::fs::write(second.output_path.as_path(), b"same bytes")
            .await
            .expect("write second prepared");
        let copy_path = workspace.join("report-copy.txt");
        tokio::fs::write(copy_path.as_path(), b"same bytes")
            .await
            .expect("write copy");
        let invocation = artifact_register_invocation(
            workspace.clone(),
            Some(output_dir.clone()),
            copy_path.display().to_string(),
        );

        let error = register_artifact_for_invocation(
            &service,
            &artifact_register_context(),
            &state,
            &invocation,
            decode_artifact_tool_args(invocation.clone()).expect("decode register input"),
        )
        .await
        .expect_err("ambiguous prepared copy should fail before registration")
        .to_string();

        assert!(error.contains("preparedOutputPath"), "{error}");
        assert!(
            state
                .prepared_output_for_path(first.output_path.as_path())
                .expect("first prepared")
                .is_reserved()
        );
        assert!(
            state
                .prepared_output_for_path(second.output_path.as_path())
                .expect("second prepared")
                .is_reserved()
        );
        let page = service
            .list_thread_artifacts(
                "ws_artifact_register",
                "thr_artifact_register",
                crate::ArtifactListFilter::default(),
            )
            .await
            .expect("list artifacts");
        assert!(page.items.is_empty());
    }

    #[tokio::test]
    async fn artifact_register_registers_file_from_workspace_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        tokio::fs::create_dir_all(workspace.as_path())
            .await
            .expect("create workspace");
        let source_path = workspace.join("report.txt");
        tokio::fs::write(source_path.as_path(), b"registered workspace")
            .await
            .expect("write source");
        let service = artifact_register_service(temp.path().join("runtime")).await;
        let state = ArtifactToolState::default();
        let invocation =
            artifact_register_invocation(workspace.clone(), None, "report.txt".to_owned());

        let outcome = register_artifact_for_invocation(
            &service,
            &artifact_register_context(),
            &state,
            &invocation,
            decode_artifact_tool_args(invocation.clone()).expect("decode register input"),
        )
        .await
        .expect("register workspace artifact");

        assert!(
            source_path.exists(),
            "workspace source should remain in place"
        );
        let page = service
            .list_thread_artifacts(
                "ws_artifact_register",
                "thr_artifact_register",
                crate::ArtifactListFilter::default(),
            )
            .await
            .expect("list artifacts");
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            page.items[0].artifact.artifact_id,
            outcome.response.artifact_id
        );
    }

    #[tokio::test]
    async fn artifact_register_rejects_outside_allowed_roots() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        tokio::fs::create_dir_all(workspace.as_path())
            .await
            .expect("create workspace");
        tokio::fs::create_dir_all(outside.as_path())
            .await
            .expect("create outside");
        let outside_file = outside.join("secret.txt");
        tokio::fs::write(outside_file.as_path(), b"outside")
            .await
            .expect("write outside");
        let service = artifact_register_service(temp.path().join("runtime")).await;
        let state = ArtifactToolState::default();
        let invocation =
            artifact_register_invocation(workspace, None, outside_file.display().to_string());

        let error = register_artifact_for_invocation(
            &service,
            &artifact_register_context(),
            &state,
            &invocation,
            decode_artifact_tool_args(invocation.clone()).expect("decode register input"),
        )
        .await
        .expect_err("outside-root registration should fail")
        .to_string();

        assert!(error.contains("outside allowed roots"), "{error}");
    }

    #[tokio::test]
    async fn artifact_register_policy_denies_source_outside_snapshot_before_registration() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        tokio::fs::create_dir_all(workspace.as_path())
            .await
            .expect("create workspace");
        tokio::fs::create_dir_all(outside.as_path())
            .await
            .expect("create outside");
        let outside_file = outside.join("secret.txt");
        tokio::fs::write(outside_file.as_path(), b"outside")
            .await
            .expect("write outside");
        let service = artifact_register_service(temp.path().join("runtime")).await;
        let state = ArtifactToolState::default();
        let invocation = with_execution_security_snapshot(
            artifact_register_invocation(
                workspace.clone(),
                None,
                outside_file.display().to_string(),
            ),
            workspace_write_security_snapshot(workspace.as_path()),
        );

        let error = register_artifact_for_invocation(
            &service,
            &artifact_register_context(),
            &state,
            &invocation,
            decode_artifact_tool_args(invocation.clone()).expect("decode register input"),
        )
        .await
        .expect_err("outside-snapshot registration should fail")
        .to_string();

        assert!(error.contains("artifact_register source path"), "{error}");
        assert!(
            error.contains("outside the allowed sandbox roots"),
            "{error}"
        );
        let page = service
            .list_thread_artifacts(
                "ws_artifact_register",
                "thr_artifact_register",
                crate::ArtifactListFilter::default(),
            )
            .await
            .expect("list artifacts");
        assert!(page.items.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn artifact_register_rejects_symlink() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        tokio::fs::create_dir_all(workspace.as_path())
            .await
            .expect("create workspace");
        let target = workspace.join("target.txt");
        tokio::fs::write(target.as_path(), b"target")
            .await
            .expect("write target");
        let link = workspace.join("link.txt");
        std::os::unix::fs::symlink(target.as_path(), link.as_path()).expect("create symlink");
        let service = artifact_register_service(temp.path().join("runtime")).await;
        let state = ArtifactToolState::default();
        let invocation = artifact_register_invocation(workspace, None, link.display().to_string());

        let error = register_artifact_for_invocation(
            &service,
            &artifact_register_context(),
            &state,
            &invocation,
            decode_artifact_tool_args(invocation.clone()).expect("decode register input"),
        )
        .await
        .expect_err("symlink registration should fail")
        .to_string();

        assert!(error.contains("symlink is not allowed"), "{error}");
    }

    #[tokio::test]
    async fn artifact_register_rejects_non_regular_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        tokio::fs::create_dir_all(workspace.as_path())
            .await
            .expect("create workspace");
        let service = artifact_register_service(temp.path().join("runtime")).await;
        let state = ArtifactToolState::default();
        let invocation =
            artifact_register_invocation(workspace.clone(), None, workspace.display().to_string());

        let error = register_artifact_for_invocation(
            &service,
            &artifact_register_context(),
            &state,
            &invocation,
            decode_artifact_tool_args(invocation.clone()).expect("decode register input"),
        )
        .await
        .expect_err("directory registration should fail")
        .to_string();

        assert!(error.contains("not a regular file"), "{error}");
    }

    #[test]
    fn artifact_register_notification_payloads_reference_canonical_artifact_identity() {
        let context = artifact_register_context();
        let changed =
            artifact_register_thread_changed_notification(&context, "artifact_123".to_owned(), 123);
        assert_eq!(changed.workspace_id, "ws_artifact_register");
        assert_eq!(changed.thread_id, "thr_artifact_register");
        assert_eq!(changed.artifact_ids, vec!["artifact_123"]);
        assert_eq!(changed.reason, "artifact_register");

        let projection = ArtifactProjectionRecord {
            id: "projection_123".to_owned(),
            workspace_id: "ws_artifact_register".to_owned(),
            artifact_id: "artifact_123".to_owned(),
            artifact_version_id: "version_123".to_owned(),
            projection_kind: pioneer_protocol::ArtifactProjectionKind::Thumbnail,
            status: pioneer_protocol::ArtifactProjectionStatus::Ready,
            blob_id: Some("blob_123".to_owned()),
            text_content: None,
        };
        let notifications =
            artifact_register_projection_notifications(&context, &[projection], 456);
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].workspace_id, "ws_artifact_register");
        assert_eq!(notifications[0].artifact_id, "artifact_123");
        assert_eq!(notifications[0].version_id, "version_123");
        assert_eq!(
            notifications[0].projection_kind,
            pioneer_protocol::ArtifactProjectionKind::Thumbnail
        );
        assert_eq!(
            notifications[0].status,
            pioneer_protocol::ArtifactProjectionStatus::Ready
        );
        assert_eq!(notifications[0].updated_at, 456);
    }

    #[tokio::test]
    async fn artifact_register_failed_registration_returns_before_notifications() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        tokio::fs::create_dir_all(workspace.as_path())
            .await
            .expect("create workspace");
        let service = artifact_register_service(temp.path().join("runtime")).await;
        let state = ArtifactToolState::default();
        let missing_path = workspace.join("missing.txt");
        let invocation =
            artifact_register_invocation(workspace, None, missing_path.display().to_string());

        let result = register_artifact_for_invocation(
            &service,
            &artifact_register_context(),
            &state,
            &invocation,
            decode_artifact_tool_args(invocation.clone()).expect("decode register input"),
        )
        .await;

        assert!(
            result.is_err(),
            "failed registration must return before notification emission"
        );
        assert!(
            artifact_register_projection_notifications(&artifact_register_context(), &[], 0)
                .is_empty()
        );
    }

    #[test]
    fn artifact_tool_hints_do_not_embed_json_object_examples() {
        for configured in artifact_tool_specs() {
            assert_no_json_object_example(
                configured.spec.description.as_str(),
                &format!("{} tool description", configured.spec.name),
            );
            assert_schema_descriptions_have_no_json_object_examples(
                &configured.spec.parameters,
                &configured.spec.name,
            );
            assert_no_json_object_example(
                artifact_tool_argument_hint(configured.spec.name.as_str()),
                &format!("{} validation hint", configured.spec.name),
            );
        }
    }

    #[test]
    fn artifact_read_text_returns_utf8_for_textual_artifacts() {
        assert_eq!(
            artifact_read_text(
                ArtifactKind::Text,
                Some("text/plain"),
                None,
                "привет".as_bytes(),
            ),
            Some("привет".to_owned())
        );
        assert_eq!(
            artifact_read_text(ArtifactKind::Image, Some("image/jpeg"), None, b"jpeg"),
            None
        );
    }

    #[test]
    fn artifact_tool_specs_include_artifact_read() {
        let tool_names = artifact_tool_specs()
            .into_iter()
            .map(|configured| configured.spec.name)
            .collect::<Vec<_>>();

        assert!(tool_names.contains(&ARTIFACT_READ_TOOL.to_owned()));
    }

    fn assert_schema_descriptions_have_no_json_object_examples(value: &JsonValue, label: &str) {
        match value {
            JsonValue::Object(object) => {
                if let Some(description) = object.get("description").and_then(JsonValue::as_str) {
                    assert_no_json_object_example(description, label);
                }
                for value in object.values() {
                    assert_schema_descriptions_have_no_json_object_examples(value, label);
                }
            }
            JsonValue::Array(values) => {
                for value in values {
                    assert_schema_descriptions_have_no_json_object_examples(value, label);
                }
            }
            _ => {}
        }
    }

    fn assert_no_json_object_example(value: &str, label: &str) {
        for forbidden in ["Example: {", "Example value: {", "e.g. {", "```json"] {
            assert!(
                !value.contains(forbidden),
                "{label} should not embed JSON object examples; found `{forbidden}` in `{value}`"
            );
        }
    }
}
