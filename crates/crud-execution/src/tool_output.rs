use anyhow::{Context, Result, ensure};
use pioneer_crud_payloads::Frame;
use pioneer_entity::{thread, tool_output_chunk as chunk, turn};
use pioneer_protocol::{ItemDeltaNotification, ItemDeltaStream};
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, JoinType, QueryFilter, QueryOrder, QuerySelect, Set,
    TransactionSession, TransactionTrait,
};

pub const MAX_CHUNK_BYTES: usize = 128 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub struct ToolOutputChunk {
    pub ordinal: i64,
    pub id: String,
    pub turn_id: String,
    pub item_id: String,
    pub stream: Option<String>,
    pub text: String,
    pub metadata: Option<String>,
}

pub async fn record_output<D: TransactionTrait>(
    db: &D,
    id: &str,
    n: &pioneer_protocol::ItemDeltaNotification,
) -> anyhow::Result<()> {
    // Persist bounded immutable pieces, not a growing copy of the output.
    // A retry uses the same piece identities even after a partial write.
    if n.delta.len() <= MAX_CHUNK_BYTES {
        return record(db, id, n).await;
    }
    use sha2::{Digest, Sha256};
    let identity = serde_json::json!({
        "sourceSha256": hex::encode(Sha256::digest(n.delta.as_bytes())),
        "sourceBytes": n.delta.len(), "metadata": n.payload,
    });
    let mut offset = 0;
    while offset < n.delta.len() {
        let mut end = (offset + MAX_CHUNK_BYTES).min(n.delta.len());
        while !n.delta.is_char_boundary(end) {
            end -= 1;
        }
        let piece = pioneer_protocol::ItemDeltaNotification {
            workspace_id: n.workspace_id.clone(),
            thread_id: n.thread_id.clone(),
            turn_id: n.turn_id.clone(),
            item_id: n.item_id.clone(),
            delta: n.delta[offset..end].to_owned(),
            stream: n.stream.clone(),
            payload: if offset == 0 {
                Some(identity.clone())
            } else {
                None
            },
            markdown: None,
            markdown_version: None,
        };
        let piece_id = if offset == 0 {
            id.to_owned()
        } else {
            format!("{id}:{offset}")
        };
        record(db, &piece_id, &piece).await?;
        offset = end;
    }
    Ok(())
}

pub async fn record<D: TransactionTrait>(
    db: &D,
    id: &str,
    notification: &ItemDeltaNotification,
) -> Result<()> {
    ensure!(
        notification.delta.len() <= MAX_CHUNK_BYTES,
        "tool output chunk exceeds frame limit"
    );
    let stream = notification.stream.map(|stream| match stream {
        ItemDeltaStream::AgentMessage => "agent_message",
        ItemDeltaStream::Stdout => "stdout",
        ItemDeltaStream::Stderr => "stderr",
        ItemDeltaStream::ToolProgress => "tool_progress",
        ItemDeltaStream::FileChange => "file_change",
        ItemDeltaStream::Generic => "generic",
    });
    let metadata = notification
        .payload
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let text = Frame::encode(&notification.delta)?;
    let row = chunk::ActiveModel {
        id: Set(id.into()),
        turn_id: Set(notification.turn_id.clone()),
        item_id: Set(notification.item_id.clone()),
        stream: Set(stream.map(str::to_owned)),
        text: Set(text.bytes),
        text_encoding: Set(text.encoding as i16),
        text_original_bytes: Set(i64::try_from(text.original_bytes)?),
        text_original_chars: Set(i64::try_from(text.original_chars)?),
        text_sha256: Set(text.sha256.to_vec()),
        metadata: Set(metadata.clone()),
        ..Default::default()
    };

    let transaction = db.begin().await?;
    ensure!(
        thread::Entity::find_by_id(&notification.thread_id)
            .filter(thread::Column::WorkspaceId.eq(&notification.workspace_id))
            .select_only()
            .column(thread::Column::Id)
            .lock_shared()
            .into_tuple::<String>()
            .one(&transaction)
            .await?
            .is_some(),
        "tool output workspace mismatch"
    );
    ensure!(
        turn::Entity::find_by_id(&notification.turn_id)
            .filter(turn::Column::ThreadId.eq(&notification.thread_id))
            .select_only()
            .column(turn::Column::Id)
            .lock_shared()
            .into_tuple::<String>()
            .one(&transaction)
            .await?
            .is_some(),
        "tool output turn mismatch"
    );
    let inserted = chunk::Entity::insert(row)
        .on_conflict_do_nothing_on([chunk::Column::Id])
        .exec_without_returning(&transaction)
        .await?;
    let existing = match inserted {
        sea_orm::TryInsertResult::Inserted(0) | sea_orm::TryInsertResult::Conflicted => Some(
            chunk::Entity::find()
                .filter(chunk::Column::Id.eq(id))
                .lock_shared()
                .one(&transaction)
                .await?
                .context("tool output changed during retry")?,
        ),
        sea_orm::TryInsertResult::Inserted(_) => None,
        sea_orm::TryInsertResult::Empty => anyhow::bail!("tool output insert was empty"),
    };
    transaction.commit().await?;

    if let Some(existing) = existing {
        let decoded = decode(existing)?;
        ensure!(
            decoded.turn_id == notification.turn_id
                && decoded.item_id == notification.item_id
                && decoded.stream.as_deref() == stream
                && decoded.text == notification.delta
                && decoded.metadata == metadata,
            "tool output identity conflict"
        );
    }
    Ok(())
}

pub async fn page<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
    after: i64,
) -> Result<Vec<(i64, ToolOutputChunk)>> {
    let rows = chunk::Entity::find()
        .join(
            JoinType::InnerJoin,
            chunk::Entity::belongs_to(turn::Entity)
                .from(chunk::Column::TurnId)
                .to(turn::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .filter(Expr::col((thread::Entity, thread::Column::WorkspaceId)).eq(workspace))
        .filter(Expr::col((turn::Entity, turn::Column::ThreadId)).eq(thread_id))
        .filter(chunk::Column::TurnId.eq(turn_id))
        .filter(chunk::Column::ItemId.eq(item_id))
        .filter(chunk::Column::Ordinal.gt(after))
        .order_by_asc(chunk::Column::Ordinal)
        .limit(128)
        .all(db)
        .await?;
    rows.into_iter()
        .map(|row| decode(row).map(|row| (row.ordinal, row)))
        .collect()
}

fn decode(row: chunk::Model) -> Result<ToolOutputChunk> {
    let text = Frame {
        encoding: row.text_encoding.try_into()?,
        bytes: row.text,
        original_bytes: row.text_original_bytes.try_into()?,
        original_chars: row.text_original_chars.try_into()?,
        sha256: row.text_sha256.as_slice().try_into()?,
    }
    .decode(MAX_CHUNK_BYTES)?;
    Ok(ToolOutputChunk {
        ordinal: row.ordinal,
        id: row.id,
        turn_id: row.turn_id,
        item_id: row.item_id,
        stream: row.stream,
        text,
        metadata: row.metadata,
    })
}
