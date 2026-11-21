//! Immutable frames and indexed ranges over a single stored UTF-8 value.

use std::ops::Range;

use anyhow::{Context, Result, anyhow, ensure};
use pioneer_entity::{portable_payload_part as part, portable_payload_value as payload};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
};
use sha2::{Digest, Sha256};

use crate::{BLOCK_BYTES, BlockPayload, Frame};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PayloadMetadata {
    pub original_bytes: usize,
    pub original_chars: usize,
    pub sha256: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct PreparedPayload {
    id: String,
    metadata: PayloadMetadata,
    value: payload::ActiveModel,
    parts: Vec<part::ActiveModel>,
}

impl PreparedPayload {
    /// Compression, checksums and UTF-8 boundary discovery happen before any write.
    pub fn new(text: &str) -> Result<Self> {
        Self::encode(text, false)
    }

    /// Keep the shell discriminator available when compaction resolves source
    /// identity without requesting any payload bytes.
    pub fn new_turn_item(text: &str) -> Result<Self> {
        let fields = crate::json_fields::first_object_fields(text, &["storage"])?;
        let shell = fields
            .pointer("/storage/kind")
            .and_then(serde_json::Value::as_str)
            == Some("shell");
        Self::encode(text, shell)
    }

    fn encode(text: &str, turn_item_shell: bool) -> Result<Self> {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let (original_bytes, original_chars, sha256, codec, body, parts) =
            match BlockPayload::encode(text)? {
                BlockPayload::Single(frame) => (
                    frame.original_bytes,
                    frame.original_chars,
                    frame.sha256,
                    Some(frame.encoding as i16),
                    Some(frame.bytes),
                    Vec::new(),
                ),
                BlockPayload::Split {
                    original_bytes,
                    original_chars,
                    sha256,
                    blocks,
                } => {
                    let parts = blocks
                        .into_iter()
                        .map(|block| {
                            let frame = block.frame;
                            Ok(part::ActiveModel {
                                payload_id: Set(id.clone()),
                                byte_offset: Set(i64::try_from(block.byte_offset)?),
                                char_offset: Set(i64::try_from(block.char_offset)?),
                                original_bytes: Set(i64::try_from(frame.original_bytes)?),
                                original_chars: Set(i64::try_from(frame.original_chars)?),
                                sha256: Set(frame.sha256.to_vec()),
                                codec: Set(frame.encoding as i16),
                                body: Set(frame.bytes),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    (original_bytes, original_chars, sha256, None, None, parts)
                }
            };
        Ok(Self {
            value: payload::ActiveModel {
                id: Set(id.clone()),
                original_bytes: Set(i64::try_from(original_bytes)?),
                original_chars: Set(i64::try_from(original_chars)?),
                sha256: Set(sha256.to_vec()),
                codec: Set(codec),
                body: Set(body),
                turn_item_shell: Set(turn_item_shell),
            },
            id,
            metadata: PayloadMetadata {
                original_bytes,
                original_chars,
                sha256,
            },
            parts,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn metadata(&self) -> PayloadMetadata {
        self.metadata
    }

    /// The caller attaches the new value to its owner in the same transaction.
    pub async fn insert(&self, tx: &DatabaseTransaction) -> Result<()> {
        payload::Entity::insert(self.value.clone())
            .exec_without_returning(tx)
            .await
            .context("insert portable payload value")?;
        // PostgreSQL caps the number of binds per statement.
        for chunk in self.parts.chunks(512) {
            part::Entity::insert_many(chunk.iter().cloned())
                .exec_without_returning(tx)
                .await
                .context("insert portable payload blocks")?;
        }
        Ok(())
    }
}

pub async fn metadata<C: ConnectionTrait>(db: &C, id: &str) -> Result<Option<PayloadMetadata>> {
    payload::Entity::find_by_id(id)
        .select_only()
        .columns([
            payload::Column::OriginalBytes,
            payload::Column::OriginalChars,
            payload::Column::Sha256,
        ])
        .into_tuple::<(i64, i64, Vec<u8>)>()
        .one(db)
        .await?
        .map(|(bytes, chars, sha256)| decode_metadata(bytes, chars, sha256))
        .transpose()
}

#[derive(Clone, Debug)]
pub enum Selection {
    Full,
    Bytes(Range<usize>),
    Characters(Range<usize>),
}

struct Part {
    offset: usize,
    frame: Frame,
}

pub struct CapturedPayload {
    metadata: PayloadMetadata,
    range: Range<usize>,
    by_characters: bool,
    parts: Vec<Part>,
}

impl CapturedPayload {
    /// Decode after releasing the database snapshot. Byte ranges can cut through
    /// a code point, so the common result type is a byte vector.
    pub fn into_bytes(self) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        let mut next = self.range.start;
        for part in self.parts {
            let text = part.frame.decode(BLOCK_BYTES)?;
            let part_len = if self.by_characters {
                text.chars().count()
            } else {
                text.len()
            };
            let part_end = part
                .offset
                .checked_add(part_len)
                .context("block end overflow")?;
            ensure!(
                part.offset <= next && next <= part_end,
                "portable payload has a gap in the requested range"
            );
            let from = next - part.offset;
            let to = self.range.end.min(part_end) - part.offset;
            let fragment = if self.by_characters {
                let from = character_byte_index(&text, from)?;
                let to = character_byte_index(&text, to)?;
                &text.as_bytes()[from..to]
            } else {
                &text.as_bytes()[from..to]
            };
            output.extend_from_slice(fragment);
            next = part_end.min(self.range.end);
        }
        ensure!(
            next == self.range.end,
            "portable payload range is incomplete"
        );
        if self.range.start == 0
            && self.range.end
                == if self.by_characters {
                    self.metadata.original_chars
                } else {
                    self.metadata.original_bytes
                }
        {
            let actual: [u8; 32] = Sha256::digest(&output).into();
            ensure!(
                output.len() == self.metadata.original_bytes && actual == self.metadata.sha256,
                "portable payload checksum mismatch"
            );
        }
        Ok(output)
    }

    pub fn into_text(self) -> Result<String> {
        String::from_utf8(self.into_bytes()?).context("portable payload is not valid UTF-8")
    }
}

fn character_byte_index(text: &str, position: usize) -> Result<usize> {
    if position == text.chars().count() {
        return Ok(text.len());
    }
    text.char_indices()
        .nth(position)
        .map(|(offset, _)| offset)
        .context("character offset exceeds portable payload block")
}

fn decode_metadata(bytes: i64, chars: i64, sha256: Vec<u8>) -> Result<PayloadMetadata> {
    let original_bytes = usize::try_from(bytes)?;
    let original_chars = usize::try_from(chars)?;
    ensure!(
        original_chars <= original_bytes,
        "portable payload length mismatch"
    );
    Ok(PayloadMetadata {
        original_bytes,
        original_chars,
        sha256: sha256
            .try_into()
            .map_err(|_| anyhow!("invalid portable payload hash"))?,
    })
}

/// Capture only the encoded blocks intersecting a range. The caller uses a
/// repeatable-read snapshot so an owner can replace or delete its value while
/// this reader still observes a single immutable version.
pub async fn capture<C: ConnectionTrait>(
    snapshot: &C,
    id: &str,
    selection: Selection,
) -> Result<Option<CapturedPayload>> {
    let Some(metadata) = metadata(snapshot, id).await? else {
        return Ok(None);
    };
    let (range, by_characters) = match selection {
        Selection::Full => (0..metadata.original_bytes, false),
        Selection::Bytes(range) => (range, false),
        Selection::Characters(range) => (range, true),
    };
    let total = if by_characters {
        metadata.original_chars
    } else {
        metadata.original_bytes
    };
    ensure!(
        range.start <= range.end && range.end <= total,
        "portable payload range out of bounds"
    );
    let parts = if range.is_empty() {
        Vec::new()
    } else if metadata.original_bytes <= BLOCK_BYTES {
        let value = payload::Entity::find_by_id(id)
            .select_only()
            .columns([payload::Column::Codec, payload::Column::Body])
            .into_tuple::<(i16, Vec<u8>)>()
            .one(snapshot)
            .await?
            .context("missing portable payload body")?;
        vec![Part {
            offset: 0,
            frame: Frame {
                encoding: value.0.try_into()?,
                bytes: value.1,
                original_bytes: metadata.original_bytes,
                original_chars: metadata.original_chars,
                sha256: metadata.sha256,
            },
        }]
    } else {
        capture_blocks(snapshot, id, &range, by_characters).await?
    };
    Ok(Some(CapturedPayload {
        metadata,
        range,
        by_characters,
        parts,
    }))
}

async fn capture_blocks<C: ConnectionTrait>(
    snapshot: &C,
    id: &str,
    range: &Range<usize>,
    by_characters: bool,
) -> Result<Vec<Part>> {
    use part::Column as C;
    let key = if by_characters {
        C::CharOffset
    } else {
        C::ByteOffset
    };
    // The predecessor and the following offsets use the same btree prefix.
    // No data from blocks before the predecessor is selected.
    let query = part::Entity::find().filter(C::PayloadId.eq(id));
    let first = query
        .clone()
        .select_only()
        .column(key)
        .filter(key.lte(i64::try_from(range.start)?))
        .order_by_desc(key)
        .limit(1)
        .into_tuple::<i64>()
        .one(snapshot)
        .await?
        .context("portable payload range has no initial block")?;
    let candidates = query
        .filter(key.gte(first))
        .filter(key.lt(i64::try_from(range.end)?))
        .order_by_asc(key)
        .all(snapshot)
        .await?;
    let mut previous = None;
    candidates
        .into_iter()
        .map(|block| {
            let offset = usize::try_from(if by_characters {
                block.char_offset
            } else {
                block.byte_offset
            })?;
            let metadata =
                decode_metadata(block.original_bytes, block.original_chars, block.sha256)?;
            if let Some(previous_end) = previous {
                ensure!(
                    offset == previous_end,
                    "portable payload blocks are not contiguous"
                );
            }
            let len = if by_characters {
                metadata.original_chars
            } else {
                metadata.original_bytes
            };
            previous = Some(offset.checked_add(len).context("block end overflow")?);
            Ok(Part {
                offset,
                frame: Frame {
                    encoding: block.codec.try_into()?,
                    bytes: block.body,
                    original_bytes: metadata.original_bytes,
                    original_chars: metadata.original_chars,
                    sha256: metadata.sha256,
                },
            })
        })
        .collect()
}

/// Release an unshared value after replacing or deleting its owner. A reader
/// that already holds a PostgreSQL snapshot can still finish its old version.
pub async fn delete(tx: &DatabaseTransaction, id: &str) -> Result<bool> {
    Ok(payload::Entity::delete_by_id(id)
        .exec(tx)
        .await?
        .rows_affected
        == 1)
}
