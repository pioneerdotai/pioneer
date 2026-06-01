use super::{MessageProcessor, now_timestamp_secs, task_agent_executor::TaskParentRuntimeContext};
use anyhow::{Context, Result, anyhow};
use pioneer_artifacts::{
    ArtifactBindingTarget, ArtifactListFilter, ArtifactLocalPathPolicy, ArtifactSource,
    BindArtifactRequest, IngestArtifactSourceRequest,
};
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind,
    ArtifactCreatedNotification, ArtifactKind, ArtifactRole, ArtifactSummary, Task,
    TaskAgentContext, TaskAgentInput, TaskAgentInputAttachmentKind, TaskAgentInputReferenceKind,
    TaskArtifact, TaskError, TaskErrorClass, TaskResult, TaskRunTurn, TaskThreadLineage, TaskValue,
    UserInput, constants::events,
};
use serde_json::{Value as JsonValue, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(super) fn task_agent_artifact_user_inputs(
    agent_spec: &pioneer_protocol::TaskAgentSpec,
) -> Vec<UserInput> {
    let mut artifacts = Vec::new();
    collect_task_agent_input_artifacts(agent_spec.prompt.input.as_ref(), &mut artifacts);
    if let Some(context) = agent_spec
        .context_policy
        .as_ref()
        .and_then(|policy| policy.custom_context.as_ref())
    {
        collect_task_agent_context_artifacts(context, &mut artifacts);
    }
    artifacts
}

fn collect_task_agent_context_artifacts(
    context: &TaskAgentContext,
    artifacts: &mut Vec<UserInput>,
) {
    let input = TaskAgentInput {
        text: None,
        variables: Vec::new(),
        attachments: context.attachments.clone(),
        references: context.references.clone(),
    };
    collect_task_agent_input_artifacts(Some(&input), artifacts);
}

fn collect_task_agent_input_artifacts(
    input: Option<&TaskAgentInput>,
    artifacts: &mut Vec<UserInput>,
) {
    let Some(input) = input else {
        return;
    };
    for attachment in &input.attachments {
        if attachment.kind == TaskAgentInputAttachmentKind::Artifact
            && let Some(artifact_id) = attachment.artifact_id.as_deref()
            && !artifact_id.trim().is_empty()
        {
            artifacts.push(UserInput::Artifact {
                artifact_id: artifact_id.to_owned(),
                version_id: None,
            });
        }
    }
    for reference in &input.references {
        if reference.kind == TaskAgentInputReferenceKind::Artifact
            && !reference.id.trim().is_empty()
        {
            artifacts.push(UserInput::Artifact {
                artifact_id: reference.id.clone(),
                version_id: None,
            });
        }
    }
}

pub(super) async fn render_parent_artifact_refs(
    processor: &Arc<MessageProcessor>,
    workspace_id: &str,
    parent: &TaskParentRuntimeContext,
) -> Result<Option<String>> {
    let page = processor
        .artifact_service
        .list_thread_artifacts(
            workspace_id,
            parent.parent_thread_id.as_str(),
            ArtifactListFilter {
                limit: Some(20),
                ..ArtifactListFilter::default()
            },
        )
        .await
        .context("failed to list parent thread artifacts for task context")?;
    if page.items.is_empty() {
        return Ok(None);
    }

    let lines = page
        .items
        .into_iter()
        .map(|summary| {
            let artifact = summary.artifact;
            format!(
                "- {} (artifact_id: {}, version_id: {}, kind: {:?}, mime: {}, size_bytes: {})",
                artifact.display_name,
                artifact.artifact_id,
                artifact.version_id.unwrap_or_else(|| "current".to_owned()),
                artifact.kind,
                artifact.mime_type.unwrap_or_else(|| "unknown".to_owned()),
                artifact
                    .size_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_owned())
            )
        })
        .collect::<Vec<_>>();
    Ok(Some(format!(
        "Parent thread artifacts:\n{}",
        lines.join("\n")
    )))
}

pub(super) async fn normalize_task_result_artifacts(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    task_run_turn: &TaskRunTurn,
    lineage: &TaskThreadLineage,
    result: TaskResult,
) -> Result<std::result::Result<TaskResult, TaskError>> {
    normalize_task_result_artifacts_with_binding(
        processor,
        task,
        task_run_turn,
        lineage,
        &TaskResultArtifactBinding::FinalResult,
        result,
    )
    .await
}

pub(super) async fn normalize_task_result_candidate_artifacts(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    task_run_turn: &TaskRunTurn,
    lineage: &TaskThreadLineage,
    candidate_id: &str,
    result: TaskResult,
) -> Result<std::result::Result<TaskResult, TaskError>> {
    normalize_task_result_artifacts_with_binding(
        processor,
        task,
        task_run_turn,
        lineage,
        &TaskResultArtifactBinding::ResultCandidate {
            candidate_id: candidate_id.to_owned(),
        },
        result,
    )
    .await
}

async fn normalize_task_result_artifacts_with_binding(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    task_run_turn: &TaskRunTurn,
    lineage: &TaskThreadLineage,
    binding: &TaskResultArtifactBinding,
    mut result: TaskResult,
) -> Result<std::result::Result<TaskResult, TaskError>> {
    let mut changed_artifact_ids = Vec::new();
    for (index, artifact) in result.artifacts.iter_mut().enumerate() {
        match normalize_task_result_artifact(
            processor,
            task,
            task_run_turn,
            lineage,
            binding,
            artifact,
            index,
        )
        .await
        {
            Ok(Some(artifact_id)) => changed_artifact_ids.push(artifact_id),
            Ok(None) => {}
            Err(error) => {
                return Ok(Err(task_artifact_error(
                    format!("task result artifact {index} is invalid: {error:#}"),
                    Some(task_run_turn.run_id.clone()),
                )));
            }
        }
    }

    notify_task_artifacts_changed(processor, task, lineage, binding, changed_artifact_ids).await;
    Ok(Ok(result))
}

#[derive(Debug, Clone)]
enum TaskResultArtifactBinding {
    FinalResult,
    ResultCandidate { candidate_id: String },
}

async fn normalize_task_result_artifact(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    task_run_turn: &TaskRunTurn,
    lineage: &TaskThreadLineage,
    binding: &TaskResultArtifactBinding,
    artifact: &mut TaskArtifact,
    index: usize,
) -> Result<Option<String>> {
    if let Some(artifact_id) = artifact
        .artifact_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let summary = processor
            .artifact_service
            .get_artifact(
                task.workspace_id.as_str(),
                artifact_id,
                artifact.version_id.as_deref(),
            )
            .await
            .with_context(|| {
                format!(
                    "artifact `{artifact_id}` is not available in workspace `{}`",
                    task.workspace_id
                )
            })?;
        artifact.artifact_id = Some(summary.artifact.artifact_id.clone());
        artifact.version_id = summary.artifact.version_id.clone();
        if artifact.mime_type.is_none() {
            artifact.mime_type = summary.artifact.mime_type.clone();
        }
        bind_task_result_artifact(
            processor,
            task,
            task_run_turn,
            lineage,
            binding,
            artifact,
            index,
        )
        .await?;
        return Ok(Some(summary.artifact.artifact_id));
    }

    let Some(path) = artifact
        .path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
    else {
        return Ok(None);
    };

    let summary = ingest_task_result_path(
        processor,
        task,
        task_run_turn,
        lineage,
        binding,
        artifact,
        path.as_str(),
        index,
    )
    .await?;
    artifact.artifact_id = Some(summary.artifact.artifact_id.clone());
    artifact.version_id = summary.artifact.version_id.clone();
    if artifact.mime_type.is_none() {
        artifact.mime_type = summary.artifact.mime_type.clone();
    }
    Ok(Some(summary.artifact.artifact_id))
}

async fn ingest_task_result_path(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    task_run_turn: &TaskRunTurn,
    lineage: &TaskThreadLineage,
    binding: &TaskResultArtifactBinding,
    artifact: &TaskArtifact,
    path: &str,
    index: usize,
) -> Result<ArtifactSummary> {
    let allowed_root = std::env::current_dir().context("failed to resolve task artifact root")?;
    let mut metadata = task_result_artifact_metadata(artifact, binding);
    metadata.insert("source_path".to_owned(), json!(path));
    let summary = processor
        .artifact_service
        .ingest_source(IngestArtifactSourceRequest {
            workspace_id: task.workspace_id.clone(),
            primary_thread_id: task_result_thread_id(task, lineage),
            source: ArtifactSource::LocalPath(PathBuf::from(path)),
            display_name: display_name_from_task_artifact_path(path),
            kind: Some(ArtifactKind::WorkspaceFile),
            mime_type: artifact.mime_type.clone(),
            created_by_kind: ArtifactCreatedByKind::Task,
            created_by_actor_id: Some(task.id.clone()),
            binding: Some(task_result_binding_target(
                task,
                task_run_turn,
                lineage,
                binding,
                index,
            )),
            metadata,
            local_path_policy: Some(ArtifactLocalPathPolicy::new(vec![allowed_root])),
        })
        .await
        .with_context(|| format!("failed to ingest task result artifact path `{path}`"))?;
    processor
        .send_notification_to_thread_subscribers(
            task_result_thread_id(task, lineage)
                .as_deref()
                .unwrap_or(lineage.parent_thread_id.as_str()),
            events::ARTIFACT_CREATED,
            &ArtifactCreatedNotification {
                workspace_id: task.workspace_id.clone(),
                artifact: summary.clone(),
            },
        )
        .await;
    Ok(summary)
}

async fn bind_task_result_artifact(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    task_run_turn: &TaskRunTurn,
    lineage: &TaskThreadLineage,
    binding: &TaskResultArtifactBinding,
    artifact: &TaskArtifact,
    index: usize,
) -> Result<()> {
    let artifact_id = artifact
        .artifact_id
        .as_deref()
        .ok_or_else(|| anyhow!("artifact id is missing"))?;
    let summary = processor
        .artifact_service
        .get_artifact(
            task.workspace_id.as_str(),
            artifact_id,
            artifact.version_id.as_deref(),
        )
        .await?;
    if summary.bindings.iter().any(|existing| {
        existing.binding_kind == artifact_binding_kind(binding)
            && existing.task_id.as_deref() == Some(task.id.as_str())
            && existing.task_run_id.as_deref() == Some(task_run_turn.run_id.as_str())
            && existing.item_index == Some(index as i64)
    }) {
        return Ok(());
    }

    processor
        .artifact_service
        .bind_artifact(BindArtifactRequest {
            workspace_id: task.workspace_id.clone(),
            artifact_id: artifact_id.to_owned(),
            version_id: artifact.version_id.clone(),
            target: task_result_binding_target(task, task_run_turn, lineage, binding, index),
            metadata: task_result_artifact_metadata(artifact, binding),
        })
        .await
        .with_context(|| format!("failed to bind artifact `{artifact_id}` to task result"))?;
    Ok(())
}

async fn notify_task_artifacts_changed(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    lineage: &TaskThreadLineage,
    binding: &TaskResultArtifactBinding,
    artifact_ids: Vec<String>,
) {
    if artifact_ids.is_empty() {
        return;
    }
    let Some(thread_id) = task_result_thread_id(task, lineage) else {
        return;
    };
    processor
        .send_thread_artifacts_changed_to_thread_and_ancestors(
            task.workspace_id.as_str(),
            thread_id.as_str(),
            artifact_ids,
            task_artifacts_changed_reason(binding),
            now_timestamp_secs(),
        )
        .await;
}

fn task_result_binding_target(
    task: &Task,
    task_run_turn: &TaskRunTurn,
    lineage: &TaskThreadLineage,
    binding: &TaskResultArtifactBinding,
    index: usize,
) -> ArtifactBindingTarget {
    ArtifactBindingTarget {
        thread_id: task_result_thread_id(task, lineage),
        turn_id: task.created_by_turn_id.clone().or_else(|| {
            (task_result_thread_id(task, lineage).as_deref()
                == lineage.created_by_thread_id.as_deref())
            .then(|| lineage.created_by_turn_id.clone())
            .flatten()
        }),
        message_id: None,
        turn_item_id: None,
        tool_call_id: None,
        task_id: Some(task.id.clone()),
        task_run_id: Some(task_run_turn.run_id.clone()),
        binding_kind: artifact_binding_kind(binding),
        direction: ArtifactBindingDirection::Output,
        role: Some(ArtifactRole::Task),
        item_index: Some(index as i64),
    }
}

fn task_result_thread_id(task: &Task, lineage: &TaskThreadLineage) -> Option<String> {
    task.created_by_thread_id.clone().or_else(|| {
        (task.owner_kind == pioneer_protocol::TaskOwnerKind::Thread)
            .then(|| task.owner_id.clone())
            .flatten()
            .or_else(|| {
                lineage
                    .created_by_thread_id
                    .clone()
                    .or_else(|| Some(lineage.parent_thread_id.clone()))
            })
    })
}

fn task_result_artifact_metadata(
    artifact: &TaskArtifact,
    binding: &TaskResultArtifactBinding,
) -> BTreeMap<String, JsonValue> {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "source_kind".to_owned(),
        json!(task_artifacts_changed_reason(binding)),
    );
    if let TaskResultArtifactBinding::ResultCandidate { candidate_id } = binding {
        metadata.insert("task_result_candidate_id".to_owned(), json!(candidate_id));
    }
    if let Some(url) = artifact.url.as_deref() {
        metadata.insert("source_url".to_owned(), json!(url));
    }
    if let Some(value) = artifact.metadata.as_ref() {
        metadata.insert(
            "task_artifact_metadata".to_owned(),
            serde_json::to_value(value).unwrap_or(JsonValue::Null),
        );
    }
    metadata
}

fn artifact_binding_kind(binding: &TaskResultArtifactBinding) -> ArtifactBindingKind {
    match binding {
        TaskResultArtifactBinding::FinalResult => ArtifactBindingKind::TaskResult,
        TaskResultArtifactBinding::ResultCandidate { .. } => {
            ArtifactBindingKind::TaskResultCandidate
        }
    }
}

fn task_artifacts_changed_reason(binding: &TaskResultArtifactBinding) -> &'static str {
    match binding {
        TaskResultArtifactBinding::FinalResult => "task_result",
        TaskResultArtifactBinding::ResultCandidate { .. } => "task_result_candidate",
    }
}

fn display_name_from_task_artifact_path(path: &str) -> Option<String> {
    Path::new(path)
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

pub(super) fn parse_task_artifacts(values: &[TaskValue]) -> Vec<TaskArtifact> {
    values
        .iter()
        .filter_map(|value| {
            let object = task_value_object(value)?;
            Some(TaskArtifact {
                artifact_id: object
                    .get("artifactId")
                    .or_else(|| object.get("artifact_id"))
                    .and_then(task_value_str)
                    .map(str::to_owned),
                version_id: object
                    .get("versionId")
                    .or_else(|| object.get("version_id"))
                    .and_then(task_value_str)
                    .map(str::to_owned),
                path: object
                    .get("path")
                    .and_then(task_value_str)
                    .map(str::to_owned),
                url: object
                    .get("url")
                    .and_then(task_value_str)
                    .map(str::to_owned),
                mime_type: object
                    .get("mimeType")
                    .or_else(|| object.get("mime_type"))
                    .and_then(task_value_str)
                    .map(str::to_owned),
                metadata: object.get("metadata").cloned(),
            })
        })
        .collect()
}

fn task_value_object(value: &TaskValue) -> Option<&BTreeMap<String, TaskValue>> {
    match value {
        TaskValue::Object(value) => Some(value),
        _ => None,
    }
}

fn task_value_str(value: &TaskValue) -> Option<&str> {
    match value {
        TaskValue::String(value) => Some(value.as_str()),
        _ => None,
    }
}

fn task_artifact_error(message: impl Into<String>, failed_run_id: Option<String>) -> TaskError {
    TaskError {
        code: "task_artifact_invalid".to_owned(),
        message: message.into(),
        class: TaskErrorClass::Validation,
        details: None,
        failed_run_id,
    }
}
