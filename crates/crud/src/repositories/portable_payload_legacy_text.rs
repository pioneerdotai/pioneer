//! Character fragments over legacy UTF-8 TEXT. The driver owns each native byte
//! range; no reader/statement/permit survives into this UTF-8 scan. A bounded,
//! store-local continuation avoids rescanning the prefix on sequential reads.
use anyhow::{Result, ensure};
use pioneer_sqlite::{SqliteBlobSource, SqliteDatabase};
use sea_orm::Statement;
use std::collections::VecDeque;

const BLOCK: usize = 65_536;
const CHARACTERS: usize = 16_384;
const CACHE_ENTRIES: usize = 16;
const CACHE_KEY_BYTES: usize = 4096;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Key {
    pub run: String,
    pub identity: String,
    pub revision: i64,
}
#[derive(Clone)]
struct Position {
    key: Key,
    byte: u64,
    character: u64,
}
#[derive(Default)]
pub(crate) struct Cursors(VecDeque<Position>);
impl Cursors {
    fn get(&mut self, key: &Key) -> Option<Position> {
        let index = self.0.iter().position(|p| &p.key == key)?;
        let position = self.0.remove(index)?;
        self.0.push_back(position.clone());
        Some(position)
    }
    fn put(&mut self, position: Position) {
        // Large legacy keys remain readable; only their optional cache is
        // bypassed. Never retain unbounded identifiers in a process cache.
        if position.key.run.len() + position.key.identity.len() > CACHE_KEY_BYTES {
            return;
        }
        self.0.retain(|p| p.key != position.key);
        if self.0.len() == CACHE_ENTRIES {
            self.0.pop_front();
        }
        self.0.push_back(position);
    }
}

async fn bytes(
    db: &SqliteDatabase,
    guard: &Statement,
    offset: u64,
    count: usize,
    size: u64,
) -> Result<Vec<u8>> {
    Ok(db
        .read_blob_range(
            SqliteBlobSource {
                table: "task_run_conversation_snapshot",
                column: "history_json",
            },
            guard.clone(),
            offset,
            count,
            size,
        )
        .await?
        .ok_or(super::portable_payload::PreflightRace)?)
}

/// No payload bytes enter the continuation cache. Its key includes the insert
/// incarnation and logical revision; the caller's scope/version guard is rerun
/// in the same native snapshot as every byte range, including cached seeks.
/// Cold random character offsets require a prefix scan. Sequential callers
/// reuse byte/character positions and perform linear application decoding.
pub(crate) async fn fragment(
    store: &crate::CrudStore,
    guard: Statement,
    key: Key,
    size: u64,
    requested: u64,
) -> Result<Option<(String, Option<u64>)>> {
    let cached = store
        .legacy_task_text_cursors
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&key);
    if cached.is_none() {
        // The former live-source view used substr(ltrim(history_json),1,1).
        // Validate that exact ASCII-space predicate outside database capacity.
        let mut offset = 0;
        let mut array = false;
        while offset < size {
            let count = BLOCK.min(usize::try_from(size - offset)?);
            let block = bytes(&store.connection, &guard, offset, count, size).await?;
            if let Some(first) = block.iter().find(|b| **b != b' ') {
                array = *first == b'[';
                break;
            }
            offset += block.len() as u64;
        }
        if !array {
            return Ok(None);
        }
    }
    let (mut byte, mut character) = cached
        .filter(|p| p.character <= requested)
        .map(|p| (p.byte, p.character))
        .unwrap_or((0, 0));
    ensure!(byte <= size, "legacy Task cursor is past end");
    let mut output = String::new();
    let mut count = 0;
    let mut logical_end = false;
    let mut more = false;
    while byte < size && count < CHARACTERS {
        let block = bytes(
            &store.connection,
            &guard,
            byte,
            BLOCK.min(usize::try_from(size - byte)?),
            size,
        )
        .await?;
        let end = match std::str::from_utf8(&block) {
            Ok(_) => block.len(),
            Err(error) if error.error_len().is_none() => error.valid_up_to(),
            Err(error) => return Err(error.into()),
        };
        ensure!(end > 0, "truncated legacy Task UTF-8");
        let text = std::str::from_utf8(&block[..end])?;
        let start = byte;
        for (index, ch) in text.char_indices() {
            if ch == '\0' {
                logical_end = true;
                break;
            }
            byte = start + (index + ch.len_utf8()) as u64;
            if character >= requested {
                output.push(ch);
                count += 1;
            }
            character = character
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("legacy Task character overflow"))?;
            if count == CHARACTERS {
                if byte < size {
                    let next = usize::try_from(byte - start)?;
                    more = if let Some(next) = block.get(next) {
                        *next != 0
                    } else {
                        bytes(&store.connection, &guard, byte, 1, size).await?[0] != 0
                    };
                }
                break;
            }
        }
        if logical_end {
            break;
        }
        // At most an unfinished scalar (three bytes) is reread next time.
    }
    ensure!(requested <= character, "source offset is past end");
    store
        .legacy_task_text_cursors
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .put(Position {
            key,
            byte,
            character,
        });
    Ok(Some((output, more.then_some(character))))
}
