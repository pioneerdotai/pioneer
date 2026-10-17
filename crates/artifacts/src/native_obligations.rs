//! Artifact Service owns the bounded codec; the existing Turn item owns durability.
use crate::{
    ArtifactService, ArtifactToolContext, ArtifactToolState, PreparedArtifactOutputStatus,
};
use pioneer_tools::ToolError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactRegistrationOperation {
    pub request_sha256: String,
    pub content_sha256: String,
    pub size_bytes: u64,
    pub prepared_paths: Vec<String>,
    pub receipt: Option<(String, String)>,
    pub receipt_call_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    schema_version: u32,
    workspace_id: String,
    thread_id: String,
    turn_id: String,
    state: crate::tools::ArtifactToolStateInner,
}

fn unavailable() -> ToolError {
    ToolError::execution_failed("native artifact obligation state or receipt is unavailable")
}
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn encode(snapshot: &Snapshot) -> Result<Vec<u8>, ToolError> {
    struct Bounded(Vec<u8>);
    impl std::io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.0.len().saturating_add(bytes.len()) > 64 * 1024 {
                return Err(std::io::Error::other("artifact obligation byte limit"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Bounded(Vec::new());
    serde_json::to_writer(&mut writer, snapshot).map_err(|_| unavailable())?;
    Ok(writer.0)
}

impl Snapshot {
    fn valid(&self, context: &ArtifactToolContext) -> bool {
        let state = &self.state;
        self.schema_version == 1
            && self.workspace_id == context.workspace_id
            && self.thread_id == context.thread_id
            && self.turn_id == context.turn_id
            && state.revision > 0
            && state.revision < u64::MAX
            && state.prepared_by_path.len() <= 64
            && state.filename_counters.len() <= 64
            && state.registrations.len() <= 64
            && state.filename_counters.iter().all(|(key, count)| {
                !key.is_empty() && key.len() <= 4224 && *count > 0 && *count <= 4096
            })
            && state.prepared_by_path.iter().all(|(path, output)| {
                path.len() <= 4096
                    && output.output_path.to_str() == Some(path.as_str())
                    && output.output_path.is_absolute()
                    && output.output_dir.is_absolute()
                    && output.output_dir.to_string_lossy().len() <= 4096
                    && output.output_path.starts_with(&output.output_dir)
                    && !output
                        .output_path
                        .components()
                        .any(|c| matches!(c, std::path::Component::ParentDir))
                    && !output.tool_call_id.is_empty()
                    && output.tool_call_id.len() <= 4096
                    && digest(&output.request_sha256)
                    && output.display_name.len() <= 480
                    && output.mime_type.as_ref().is_none_or(|s| s.len() <= 256)
                    && output.description.as_ref().is_none_or(|s| s.len() <= 4096)
                    && chrono::DateTime::parse_from_rfc3339(&output.expires_at).is_ok()
                    && match &output.status {
                        PreparedArtifactOutputStatus::Registered {
                            artifact_id,
                            version_id,
                        } => state.registrations.values().any(|operation| {
                            operation.prepared_paths.contains(path)
                                && operation.receipt.as_ref()
                                    == Some(&(artifact_id.clone(), version_id.clone()))
                        }),
                        _ => true,
                    }
            })
            && state.registrations.iter().all(|(call, operation)| {
                !call.is_empty()
                    && call.len() <= 4096
                    && digest(&operation.request_sha256)
                    && digest(&operation.content_sha256)
                    && operation.size_bytes <= 50 * 1024 * 1024
                    && operation.prepared_paths.len() <= 64
                    && operation
                        .prepared_paths
                        .iter()
                        .all(|path| state.prepared_by_path.contains_key(path))
                    && (operation.receipt.is_some() == operation.receipt_call_id.is_some())
                    && operation
                        .receipt_call_id
                        .as_ref()
                        .is_none_or(|source| state.registrations.contains_key(source))
                    && operation
                        .receipt
                        .as_ref()
                        .is_none_or(|(artifact, version)| {
                            artifact.len() == 21 && version.len() == 21
                        })
            })
    }
}

impl ArtifactService {
    /// Read-only resume proof. Missing or legacy state cannot become a fresh allowance.
    pub async fn native_artifact_state_compatible(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> anyhow::Result<bool> {
        let Some(payload) = self
            .store
            .native_artifact_obligation_item(workspace, thread, turn)
            .await?
        else {
            return Ok(false);
        };
        let Ok(pioneer_protocol::TurnItem::SystemEvent {
            code,
            details: Some(details),
            ..
        }) = serde_json::from_str::<pioneer_protocol::TurnItem>(&payload)
        else {
            return Ok(false);
        };
        if code.as_deref() != Some("artifact.obligations") {
            return Ok(false);
        }
        let Some(value) = details.get("artifactObligations") else {
            return Ok(false);
        };
        let Ok(snapshot) = serde_json::from_value::<Snapshot>(value.clone()) else {
            return Ok(false);
        };
        let context = ArtifactToolContext {
            workspace_id: workspace.to_owned(),
            thread_id: thread.to_owned(),
            turn_id: turn.to_owned(),
            native_fence: None,
        };
        if !snapshot.valid(&context) || encode(&snapshot).is_err() {
            return Ok(false);
        }
        for (call, operation) in snapshot.state.registrations {
            let receipt = self
                .store
                .native_artifact_registration_receipt(
                    workspace,
                    thread,
                    turn,
                    operation.receipt_call_id.as_deref().unwrap_or(&call),
                )
                .await?;
            match receipt {
                None if operation.receipt.is_none() => {}
                Some(receipt)
                    if receipt.request_sha256 == operation.request_sha256
                        && receipt.sha256 == operation.content_sha256
                        && receipt.size_bytes == operation.size_bytes
                        && operation.receipt.as_ref().is_none_or(|expected| {
                            expected == &(receipt.artifact_id.clone(), receipt.version_id.clone())
                        }) => {}
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    async fn require_native_binding(
        &self,
        context: &ArtifactToolContext,
    ) -> Result<bool, ToolError> {
        if let Some(fence) = &context.native_fence {
            if fence.workspace_id != context.workspace_id
                || fence.thread_id != context.thread_id
                || fence.turn_id != context.turn_id
            {
                return Err(unavailable());
            }
            return Ok(true);
        }
        if self
            .store
            .get_turn_execution(&context.turn_id)
            .await
            .map_err(|_| unavailable())?
            .is_some_and(|owner| {
                matches!(
                    owner.executor_kind,
                    pioneer_crud::TurnExecutorKind::NativeAgent
                        | pioneer_crud::TurnExecutorKind::ApiProvider
                )
            })
        {
            return Err(unavailable());
        }
        Ok(false)
    }

    /// Callers serialize mutations with the artifact state mutex, never SQL capacity.
    pub async fn restore_native_artifact_state(
        &self,
        context: &ArtifactToolContext,
        state: &ArtifactToolState,
        initialize: bool,
    ) -> Result<(), ToolError> {
        if !self.require_native_binding(context).await? {
            return Ok(());
        }
        let payload = self
            .store
            .native_artifact_obligation_item(
                &context.workspace_id,
                &context.thread_id,
                &context.turn_id,
            )
            .await
            .map_err(|_| unavailable())?;
        let Some(payload) = payload else {
            if !initialize {
                return Err(unavailable());
            }
            *state.inner.lock().map_err(|_| unavailable())? = Default::default();
            *state.durable_item_json.lock().map_err(|_| unavailable())? = None;
            return self.persist_native_artifact_state(context, state).await;
        };
        self.store
            .native_artifact_fence_is_current(
                context.native_fence.as_ref().ok_or_else(unavailable)?,
                &payload,
            )
            .await
            .map_err(|_| unavailable())?;
        let item: pioneer_protocol::TurnItem =
            serde_json::from_str(&payload).map_err(|_| unavailable())?;
        let pioneer_protocol::TurnItem::SystemEvent {
            code,
            details: Some(details),
            ..
        } = item
        else {
            return Err(unavailable());
        };
        if code.as_deref() != Some("artifact.obligations") {
            return Err(unavailable());
        }
        let snapshot: Snapshot = serde_json::from_value(
            details
                .get("artifactObligations")
                .ok_or_else(unavailable)?
                .clone(),
        )
        .map_err(|_| unavailable())?;
        if !snapshot.valid(context) {
            return Err(unavailable());
        }
        encode(&snapshot)?;
        *state.inner.lock().map_err(|_| unavailable())? = snapshot.state;
        *state.durable_item_json.lock().map_err(|_| unavailable())? = Some(payload);
        // A completed domain effect is authoritative even if its following ACK was lost.
        let operations = state
            .inner
            .lock()
            .map_err(|_| unavailable())?
            .registrations
            .clone();
        let mut changed = false;
        for (call, operation) in operations {
            let receipt = self
                .store
                .native_artifact_registration_receipt(
                    &context.workspace_id,
                    &context.thread_id,
                    &context.turn_id,
                    operation.receipt_call_id.as_deref().unwrap_or(&call),
                )
                .await
                .map_err(|_| unavailable())?;
            let Some(receipt) = receipt else {
                if operation.receipt.is_some() {
                    return Err(unavailable());
                }
                continue;
            };
            if receipt.request_sha256 != operation.request_sha256
                || receipt.sha256 != operation.content_sha256
                || receipt.size_bytes != operation.size_bytes
                || operation.receipt.as_ref().is_some_and(|expected| {
                    *expected != (receipt.artifact_id.clone(), receipt.version_id.clone())
                })
            {
                return Err(unavailable());
            }
            if operation.receipt.is_none() {
                Self::apply_native_artifact_receipt(state, &call, &receipt)?;
                changed = true;
            }
        }
        if changed {
            self.persist_native_artifact_state(context, state).await?;
        }
        Ok(())
    }

    pub async fn persist_native_artifact_state(
        &self,
        context: &ArtifactToolContext,
        state: &ArtifactToolState,
    ) -> Result<(), ToolError> {
        if !self.require_native_binding(context).await? {
            return Ok(());
        }
        let snapshot = {
            let mut inner = state.inner.lock().map_err(|_| unavailable())?;
            inner.revision = inner
                .revision
                .checked_add(1)
                .filter(|n| *n < u64::MAX)
                .ok_or_else(unavailable)?;
            Snapshot {
                schema_version: 1,
                workspace_id: context.workspace_id.clone(),
                thread_id: context.thread_id.clone(),
                turn_id: context.turn_id.clone(),
                state: inner.clone(),
            }
        };
        if !snapshot.valid(context) {
            return Err(unavailable());
        }
        let wire: serde_json::Value =
            serde_json::from_slice(&encode(&snapshot)?).map_err(|_| unavailable())?;
        let expected = state
            .durable_item_json
            .lock()
            .map_err(|_| unavailable())?
            .clone();
        let next = self
            .store
            .commit_native_artifact_obligations(
                context.native_fence.as_ref().ok_or_else(unavailable)?,
                expected,
                wire,
            )
            .await
            .map_err(|_| unavailable())?;
        *state.durable_item_json.lock().map_err(|_| unavailable())? = Some(next);
        Ok(())
    }

    pub async fn mark_native_artifact_retry_used(
        &self,
        context: &ArtifactToolContext,
        state: &ArtifactToolState,
    ) -> Result<bool, ToolError> {
        let _mutation = state.mutation.lock().await;
        self.restore_native_artifact_state(context, state, false)
            .await?;
        {
            let mut inner = state.inner.lock().map_err(|_| unavailable())?;
            if inner.retry_used {
                return Ok(false);
            }
            inner.retry_used = true;
        }
        self.persist_native_artifact_state(context, state).await?;
        Ok(true)
    }

    pub(crate) fn apply_native_artifact_receipt(
        state: &ArtifactToolState,
        call: &str,
        receipt: &pioneer_crud::NativeArtifactRegistrationReceipt,
    ) -> Result<(), ToolError> {
        let mut inner = state.inner.lock().map_err(|_| unavailable())?;
        let operation = inner.registrations.get_mut(call).ok_or_else(unavailable)?;
        if operation.request_sha256 != receipt.request_sha256
            || operation.content_sha256 != receipt.sha256
            || operation.size_bytes != receipt.size_bytes
        {
            return Err(unavailable());
        }
        let confirmed = operation.clone();
        let identity = (receipt.artifact_id.clone(), receipt.version_id.clone());
        for pending in inner.registrations.values_mut() {
            if pending.receipt.is_none()
                && pending.request_sha256 == confirmed.request_sha256
                && pending.content_sha256 == confirmed.content_sha256
                && pending.size_bytes == confirmed.size_bytes
                && pending.prepared_paths == confirmed.prepared_paths
            {
                pending.receipt = Some(identity.clone());
                pending.receipt_call_id = Some(call.to_owned());
            }
        }
        for path in confirmed.prepared_paths {
            inner
                .prepared_by_path
                .get_mut(&path)
                .ok_or_else(unavailable)?
                .status = PreparedArtifactOutputStatus::Registered {
                artifact_id: receipt.artifact_id.clone(),
                version_id: receipt.version_id.clone(),
            };
        }
        Ok(())
    }

    pub(crate) async fn begin_native_artifact_registration(
        &self,
        context: &ArtifactToolContext,
        state: &ArtifactToolState,
        call: &str,
        operation: ArtifactRegistrationOperation,
    ) -> Result<Option<pioneer_crud::NativeArtifactRegistrationFence>, ToolError> {
        if !self.require_native_binding(context).await? {
            return Ok(None);
        }
        {
            let mut inner = state.inner.lock().map_err(|_| unavailable())?;
            if !inner.registrations.contains_key(call) && inner.registrations.len() >= 64 {
                return Err(unavailable());
            }
            if operation.prepared_paths.iter().any(|path| {
                inner
                    .prepared_by_path
                    .get(path)
                    .is_some_and(|output| output.is_registered())
            }) {
                return Err(unavailable());
            }
            if inner.registrations.get(call).is_some_and(|old| {
                old.receipt.is_some() || old.request_sha256 != operation.request_sha256
            }) {
                return Err(unavailable());
            }
            inner
                .registrations
                .insert(call.to_owned(), operation.clone());
        }
        self.persist_native_artifact_state(context, state).await?;
        Ok(Some(pioneer_crud::NativeArtifactRegistrationFence {
            execution: context.native_fence.clone().ok_or_else(unavailable)?,
            expected_item_json: state
                .durable_item_json
                .lock()
                .map_err(|_| unavailable())?
                .clone()
                .ok_or_else(unavailable)?,
            tool_call_id: call.to_owned(),
            request_sha256: operation.request_sha256,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PreparedArtifactOutput;
    use pioneer_protocol::ArtifactPrepareKind;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn context() -> ArtifactToolContext {
        ArtifactToolContext {
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            native_fence: None,
        }
    }
    fn snapshot() -> Snapshot {
        let context = context();
        let output = PreparedArtifactOutput {
            tool_call_id: "prepare-original".into(),
            request_sha256: "a".repeat(64),
            output_path: PathBuf::from("/original-output/report.txt"),
            output_dir: PathBuf::from("/original-output"),
            display_name: "report.txt".into(),
            kind: ArtifactPrepareKind::Document,
            mime_type: Some("text/plain".into()),
            description: None,
            expires_at: "2026-10-13T00:00:00Z".into(),
            status: PreparedArtifactOutputStatus::Reserved,
        };
        Snapshot {
            schema_version: 1,
            workspace_id: context.workspace_id,
            thread_id: context.thread_id,
            turn_id: context.turn_id,
            state: crate::tools::ArtifactToolStateInner {
                revision: 1,
                retry_used: true,
                prepared_by_path: BTreeMap::from([("/original-output/report.txt".into(), output)]),
                filename_counters: BTreeMap::from([("/original-output:report.txt".into(), 1)]),
                registrations: BTreeMap::from([(
                    "register-original".into(),
                    ArtifactRegistrationOperation {
                        request_sha256: "b".repeat(64),
                        content_sha256: "c".repeat(64),
                        size_bytes: 7,
                        prepared_paths: vec!["/original-output/report.txt".into()],
                        receipt: None,
                        receipt_call_id: None,
                    },
                )]),
            },
        }
    }
    #[tokio::test]
    async fn artifact_service_cold_restore_reconciles_committed_version_and_keeps_retry_used() {
        use migration::{Migrator, MigratorTrait};
        use pioneer_crud::{
            ArtifactBindingTargetRecord, IngestArtifactMetadataRecord, NewArtifactBlobRecord,
        };
        use pioneer_protocol::{
            ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
        };
        use sea_orm::{ConnectionTrait, Database};
        use std::sync::Arc;
        let database = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&database, None).await.unwrap();
        let store = Arc::new(pioneer_crud::CrudStore::new(database).with_maintenance_access());
        let database = store.database_connection();
        for statement in [
            "INSERT INTO workspace (id,name,is_active,is_current) VALUES ('workspace','fixture',1,1)",
            "INSERT INTO thread (id,workspace_id,preview,mode,model,model_provider,status,origin_kind,sidebar_visibility,access_class,created_at,updated_at) VALUES ('thread','workspace','','agent','fixture','openai','active','user','visible','internal','2026-10-12T00:00:00Z','2026-10-12T00:00:00Z')",
            "INSERT INTO \"turn\" (id,thread_id,status,prompt_manifest_json,created_at,updated_at) VALUES ('turn','thread','in_progress','{}','2026-10-12T00:00:00Z','2026-10-12T00:00:00Z')",
            "INSERT INTO turn_execution (turn_id,thread_id,workspace_id,executor_kind,status,owner_id,owner_generation,lease_until,heartbeat_at,created_at,updated_at,started_at) VALUES ('turn','thread','workspace','native_agent','running','owner',1,'2026-10-12T01:00:00Z','2026-10-12T00:00:00Z','2026-10-12T00:00:00Z','2026-10-12T00:00:00Z','2026-10-12T00:00:00Z')",
        ] {
            database.execute_unprepared(statement).await.unwrap();
        }
        store
            .persist_native_cancellation_context(
                pioneer_protocol::NativeTerminalEffectPreparation {
                    batch_id: "fixture-batch".into(),
                    workspace_id: "workspace".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    runtime_generation: 1,
                    effects: vec![],
                },
                "owner",
                1_792_000_000,
                true,
            )
            .await
            .unwrap();
        let temporary = tempfile::tempdir().unwrap();
        let service = ArtifactService::new(
            store.clone(),
            Arc::new(crate::LocalArtifactBlobStore::new(temporary.path())),
        );
        let mut context = context();
        context.native_fence = store
            .native_artifact_execution_fence("workspace", "thread", "turn", "owner")
            .await
            .unwrap();
        assert!(
            !service
                .native_artifact_state_compatible("workspace", "thread", "turn")
                .await
                .unwrap()
        );
        let state = ArtifactToolState::default();
        assert!(
            service
                .restore_native_artifact_state(&context, &state, false)
                .await
                .is_err()
        );
        service
            .restore_native_artifact_state(&context, &state, true)
            .await
            .unwrap();
        {
            let mut inner = state.inner.lock().unwrap();
            let sample = snapshot();
            inner.prepared_by_path = sample.state.prepared_by_path;
            inner.filename_counters = sample.state.filename_counters;
        }
        service
            .persist_native_artifact_state(&context, &state)
            .await
            .unwrap();
        assert!(
            service
                .mark_native_artifact_retry_used(&context, &state)
                .await
                .unwrap()
        );
        let operation = snapshot()
            .state
            .registrations
            .remove("register-original")
            .unwrap();
        let fence = service
            .begin_native_artifact_registration(
                &context,
                &state,
                "register-original",
                operation.clone(),
            )
            .await
            .unwrap()
            .unwrap();
        let effect = store
            .ingest_artifact_metadata_fenced(
                NewArtifactBlobRecord {
                    workspace_id: "workspace".into(),
                    sha256: operation.content_sha256.clone(),
                    size_bytes: 7,
                    mime_type: Some("text/plain".into()),
                    storage_backend: "memory".into(),
                    storage_key: "fixture".into(),
                    metadata: Default::default(),
                },
                IngestArtifactMetadataRecord {
                    workspace_id: "workspace".into(),
                    primary_thread_id: Some("thread".into()),
                    display_name: "report.txt".into(),
                    kind: ArtifactKind::Text,
                    mime_type: Some("text/plain".into()),
                    created_by_kind: ArtifactCreatedByKind::Agent,
                    created_by_actor_id: None,
                    metadata: Default::default(),
                },
                Some(ArtifactBindingTargetRecord {
                    thread_id: Some("thread".into()),
                    turn_id: Some("turn".into()),
                    message_id: None,
                    turn_item_id: Some("register-original".into()),
                    tool_call_id: Some("register-original".into()),
                    task_id: None,
                    task_run_id: None,
                    binding_kind: ArtifactBindingKind::AgentOutput,
                    direction: ArtifactBindingDirection::Output,
                    role: None,
                    item_index: None,
                }),
                BTreeMap::from([(
                    "native_registration_request_sha256".into(),
                    serde_json::json!(operation.request_sha256),
                )]),
                Some(fence),
            )
            .await
            .unwrap();
        // Crash here: durable domain version exists; no Registered state ACK or tool result.
        let cold = ArtifactToolState::default();
        service
            .restore_native_artifact_state(&context, &cold, false)
            .await
            .unwrap();
        assert!(cold.try_retry_used().unwrap());
        assert!(
            !service
                .mark_native_artifact_retry_used(&context, &cold)
                .await
                .unwrap()
        );
        let outputs = cold.try_prepared_outputs().unwrap();
        assert_eq!(
            outputs[0].status,
            PreparedArtifactOutputStatus::Registered {
                artifact_id: effect.artifact.id,
                version_id: effect.version.id,
            }
        );
        assert_eq!(cold.try_pending_registration_count().unwrap(), 0);
        assert_eq!(
            store
                .count_artifacts_by_workspace("workspace")
                .await
                .unwrap(),
            1
        );
        assert!(
            service
                .native_artifact_state_compatible("workspace", "thread", "turn")
                .await
                .unwrap()
        );
        assert!(
            service
                .begin_native_artifact_registration(&context, &cold, "second-call", operation)
                .await
                .is_err()
        );
        let again = ArtifactToolState::default();
        service
            .restore_native_artifact_state(&context, &again, false)
            .await
            .unwrap();
        assert_eq!(again.try_prepared_outputs().unwrap(), outputs);
        let mut stale = context.clone();
        stale.native_fence.as_mut().unwrap().owner_generation += 1;
        assert!(
            service
                .restore_native_artifact_state(&stale, &again, false)
                .await
                .is_err()
        );
        let mut unbound = context.clone();
        unbound.native_fence = None;
        assert!(
            service
                .restore_native_artifact_state(&unbound, &again, true)
                .await
                .is_err()
        );
    }

    #[test]
    fn artifact_restore_preserves_unwritten_reservation_retry_and_exact_registration_proof() {
        let original = snapshot();
        assert!(original.valid(&context()));
        let cold: Snapshot = serde_json::from_slice(&encode(&original).unwrap()).unwrap();
        assert!(cold.state.retry_used);
        assert!(cold.state.prepared_by_path["/original-output/report.txt"].is_reserved());
        let state = ArtifactToolState::default();
        *state.inner.lock().unwrap() = cold.state;
        let receipt = pioneer_crud::NativeArtifactRegistrationReceipt {
            artifact_id: pioneer_protocol::generate_id(21),
            version_id: pioneer_protocol::generate_id(21),
            sha256: "c".repeat(64),
            size_bytes: 7,
            request_sha256: "b".repeat(64),
        };
        let mut wrong = receipt.clone();
        wrong.size_bytes = 8;
        assert!(
            ArtifactService::apply_native_artifact_receipt(&state, "register-original", &wrong)
                .is_err()
        );
        assert!(state.try_prepared_outputs().unwrap()[0].is_reserved());
        assert_eq!(state.try_pending_registration_count().unwrap(), 1);
        ArtifactService::apply_native_artifact_receipt(&state, "register-original", &receipt)
            .unwrap();
        assert!(state.try_prepared_outputs().unwrap()[0].is_registered());
        assert_eq!(state.try_pending_registration_count().unwrap(), 0);
        let mut restored = snapshot();
        restored.state = state.inner.lock().unwrap().clone();
        let restored: Snapshot = serde_json::from_slice(&encode(&restored).unwrap()).unwrap();
        assert!(restored.valid(&context()));
        assert!(
            restored.state.retry_used,
            "registration cannot renew the finalization repair allowance"
        );
        assert_eq!(
            restored.state.registrations["register-original"]
                .receipt_call_id
                .as_deref(),
            Some("register-original")
        );
    }

    #[test]
    fn artifact_codec_rejects_legacy_foreign_poison_and_unproved_registered_state() {
        let mut state = snapshot();
        let mut foreign = context();
        foreign.workspace_id = "other".into();
        assert!(!state.valid(&foreign));
        state.schema_version = 2;
        assert!(!state.valid(&context()));
        state.schema_version = 1;
        state
            .state
            .prepared_by_path
            .values_mut()
            .next()
            .unwrap()
            .status = PreparedArtifactOutputStatus::Registered {
            artifact_id: pioneer_protocol::generate_id(21),
            version_id: pioneer_protocol::generate_id(21),
        };
        assert!(
            !state.valid(&context()),
            "RAM registration without an exact durable receipt cannot authorize completion"
        );
        let mut legacy = serde_json::to_value(snapshot()).unwrap();
        legacy["state"]
            .as_object_mut()
            .unwrap()
            .remove("retry_used");
        assert!(serde_json::from_value::<Snapshot>(legacy).is_err());
        let mut oversized = snapshot();
        oversized
            .state
            .prepared_by_path
            .values_mut()
            .next()
            .unwrap()
            .description = Some("界".repeat(32 * 1024));
        assert!(encode(&oversized).is_err());
        let mut overflow = snapshot();
        overflow.state.revision = u64::MAX;
        assert!(!overflow.valid(&context()));
    }
}
