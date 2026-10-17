//! Concrete artifact obligations use the existing durable Turn item/event owner.
use anyhow::{Context, Result, ensure};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sha2::{Digest, Sha256};

pub const MAX_STATE_BYTES: usize = 64 * 1024;
pub const MAX_ITEM_BYTES: usize = MAX_STATE_BYTES + 2 * 1024;

#[derive(Clone, Debug)]
pub struct NativeArtifactExecutionFence {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub owner_id: String,
    pub owner_generation: u64,
    pub context_sha256: String,
}

#[derive(Clone, Debug)]
pub struct NativeArtifactRegistrationFence {
    pub execution: NativeArtifactExecutionFence,
    pub expected_item_json: String,
    pub tool_call_id: String,
    pub request_sha256: String,
}

#[derive(Clone, Debug)]
pub struct NativeArtifactRegistrationReceipt {
    pub artifact_id: String,
    pub version_id: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub request_sha256: String,
}

#[derive(Debug)]
pub struct NativeArtifactRegistrationAlreadyCommitted;
impl std::fmt::Display for NativeArtifactRegistrationAlreadyCommitted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("native artifact registration already has an exact durable receipt")
    }
}
impl std::error::Error for NativeArtifactRegistrationAlreadyCommitted {}

pub fn item_id(turn: &str) -> String {
    hex::encode(Sha256::digest(
        format!("native-artifact-obligations-v1:{turn}").as_bytes(),
    ))[..21]
        .to_owned()
}

pub async fn load<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> Result<Option<String>> {
    let row = db.query_one_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "SELECT CASE WHEN length(CAST(i.payload AS BLOB))<=?1 THEN i.payload ELSE NULL END AS payload \
         FROM turn_item i JOIN \"turn\" t ON t.id=i.turn_id JOIN thread th ON th.id=t.thread_id \
         WHERE i.turn_id=?2 AND i.item_id=?3 AND t.thread_id=?4 AND th.workspace_id=?5 \
         AND t.message_deleted_at IS NULL AND i.status='completed'",
        [(MAX_ITEM_BYTES as i64).into(), turn.into(), item_id(turn).into(), thread.into(), workspace.into()],
    )).await?;
    row.map(|row| {
        let payload: Option<String> = row.try_get("", "payload")?;
        payload.context("native artifact obligation source exceeds its physical bound")
    })
    .transpose()
}

/// These predicates are rechecked by the serialized writer before the transition.
pub fn fence_statement(
    fence: &NativeArtifactExecutionFence,
    expected: Option<&str>,
) -> Result<Statement> {
    ensure!(
        expected.is_none_or(|s| s.len() <= MAX_ITEM_BYTES),
        "artifact predecessor exceeds bound"
    );
    ensure!(
        fence.context_sha256.len() == 64,
        "artifact generation digest unavailable"
    );
    Ok(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "SELECT EXISTS(SELECT 1 FROM \"turn\" t JOIN thread th ON th.id=t.thread_id \
         JOIN turn_execution e ON e.turn_id=t.id JOIN native_cancellation_context c ON c.turn_id=t.id \
         WHERE t.id=?1 AND t.thread_id=?2 AND th.workspace_id=?3 AND t.status='in_progress' AND t.message_deleted_at IS NULL \
         AND e.workspace_id=?3 AND e.thread_id=?2 AND e.owner_id=?4 AND e.owner_generation=?5 \
         AND e.executor_kind IN ('native_agent','api_provider') AND e.status IN ('queued','starting','running','recovering') \
         AND c.workspace_id=?3 AND c.thread_id=?2 AND c.context_sha256=?6 AND c.accepted_event_id IS NULL \
         AND ((?7 IS NULL AND NOT EXISTS(SELECT 1 FROM turn_item WHERE turn_id=?1 AND item_id=?8)) \
           OR EXISTS(SELECT 1 FROM turn_item WHERE turn_id=?1 AND item_id=?8 AND payload=?7 AND status='completed'))) AS valid",
        [
            fence.turn_id.clone().into(),
            fence.thread_id.clone().into(),
            fence.workspace_id.clone().into(),
            fence.owner_id.clone().into(),
            i64::try_from(fence.owner_generation)?.into(),
            fence.context_sha256.clone().into(),
            expected.map(str::to_owned).into(),
            item_id(&fence.turn_id).into(),
        ],
    ))
}

pub async fn validate_fence<C: ConnectionTrait>(db: &C, statement: Statement) -> Result<()> {
    let row = db
        .query_one_raw(statement)
        .await?
        .context("artifact execution fence unavailable")?;
    ensure!(
        row.try_get::<bool>("", "valid")?,
        "artifact operation refused by original execution/predecessor fence"
    );
    Ok(())
}

/// Fetch bounded metadata, release the reader, then decode the concrete receipt.
pub async fn registration_receipt<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    call: &str,
) -> Result<Option<NativeArtifactRegistrationReceipt>> {
    let rows = db.query_all_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "SELECT a.id AS artifact_id,v.id AS version_id,b.sha256,b.size_bytes, \
         CASE WHEN length(CAST(v.metadata_json AS BLOB))<=131072 THEN v.metadata_json ELSE NULL END AS metadata \
         FROM artifact_binding ab JOIN artifact a ON a.id=ab.artifact_id AND a.workspace_id=ab.workspace_id \
         JOIN artifact_version v ON v.id=ab.artifact_version_id AND v.artifact_id=a.id AND v.workspace_id=a.workspace_id \
         JOIN artifact_blob b ON b.id=v.blob_id AND b.workspace_id=a.workspace_id \
         WHERE ab.workspace_id=?1 AND ab.thread_id=?2 AND ab.turn_id=?3 AND ab.tool_call_id=?4 \
         AND ab.binding_kind='agent_output' AND ab.direction='output' AND a.status='ready' \
         AND v.created_by_turn_id=?3 AND v.created_by_tool_call_id=?4 LIMIT 2",
        [workspace.into(), thread.into(), turn.into(), call.into()],
    )).await?;
    ensure!(
        rows.len() <= 1,
        "native artifact registration receipt is ambiguous"
    );
    #[derive(serde::Deserialize)]
    struct RegistrationMetadata {
        native_registration_request_sha256: String,
    }
    rows.into_iter()
        .next()
        .map(|row| {
            let metadata: Option<String> = row.try_get("", "metadata")?;
            let receipt: RegistrationMetadata = serde_json::from_str(
                &metadata.context("artifact receipt metadata exceeds bound")?,
            )?;
            let identity = receipt.native_registration_request_sha256;
            ensure!(
                identity.len() == 64
                    && identity
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                "native artifact receipt request identity unavailable"
            );
            Ok(NativeArtifactRegistrationReceipt {
                artifact_id: row.try_get("", "artifact_id")?,
                version_id: row.try_get("", "version_id")?,
                sha256: row.try_get("", "sha256")?,
                size_bytes: u64::try_from(row.try_get::<i64>("", "size_bytes")?)?,
                request_sha256: identity.to_owned(),
            })
        })
        .transpose()
}

/// SQL-only exact comparison while the writer is held; no JSON decode there.
pub async fn registration_exists<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    call: &str,
) -> Result<bool> {
    let row = db.query_one_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "SELECT EXISTS(SELECT 1 FROM artifact_binding WHERE workspace_id=?1 AND thread_id=?2 AND turn_id=?3 AND tool_call_id=?4 AND binding_kind='agent_output' AND direction='output') AS present",
        [workspace.into(), thread.into(), turn.into(), call.into()],
    )).await?.context("artifact registration identity unavailable")?;
    Ok(row.try_get("", "present")?)
}

/// The final message is restored from its durable item, never a missing RAM cache.
pub async fn final_text<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> Result<Option<String>> {
    let row = db.query_one_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "SELECT CASE WHEN length(CAST(e.payload AS BLOB))<=526336 THEN e.payload ELSE NULL END AS payload, \
         r.present=1 AND r.projection_revision=r.revision AS source_valid \
         FROM turn_event e JOIN compaction_event_revision r ON r.source_id=e.id AND r.turn_id=e.turn_id \
         JOIN \"turn\" t ON t.id=e.turn_id JOIN thread th ON th.id=t.thread_id \
         WHERE e.turn_id=?1 AND e.thread_id=?2 AND t.thread_id=?2 AND th.workspace_id=?3 \
         AND r.projection_kind='assistant' AND t.message_deleted_at IS NULL ORDER BY e.sequence DESC LIMIT 1",
        [turn.into(), thread.into(), workspace.into()],
    )).await?;
    row.map(|row| {
        ensure!(
            row.try_get::<bool>("", "source_valid")?,
            "native final artifact message source changed"
        );
        let payload: Option<String> = row.try_get("", "payload")?;
        let event: crate::events::TurnEventPayload =
            serde_json::from_str(&payload.context("native final artifact message exceeds bound")?)?;
        let crate::events::TurnEventPayload::ItemCompleted(notification) = event else {
            anyhow::bail!("native completed final artifact event unavailable");
        };
        ensure!(
            notification.workspace_id == workspace
                && notification.thread_id == thread
                && notification.turn_id == turn,
            "native final artifact message scope differs"
        );
        let pioneer_protocol::TurnItem::AgentMessage {
            text,
            phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
            ..
        } = notification.item
        else {
            anyhow::bail!("native completed final artifact message unavailable");
        };
        Ok(text)
    })
    .transpose()
}
