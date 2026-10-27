//! SQLite lifecycle/storage for the byte-only portable codec.
//! Raw SQL is confined to fixed source projections, byte substrings and guarded
//! physical publication, which Entity cannot express for heterogeneous owners.
use crate::{CrudStore, portable_payload_codec as codec};
use anyhow::{Context, Result, ensure};
use pioneer_entity::{
    portable_payload as value, portable_payload_block as block,
    portable_payload_dictionary as dictionary,
};
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Statement, TransactionTrait,
};
use sha2::{Digest, Sha256};

const BLOCK: usize = 64 * 1024;
const BUFFER: usize = 32 * 1024 * 1024;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Family {
    ToolText,
    ToolMetadata,
    CliMapping,
    TaskHistory,
    TaskEvent,
}
impl Family {
    fn spec(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::ToolText => ("tool_output_chunk", "id", "text"),
            Self::ToolMetadata => ("tool_output_chunk", "id", "metadata"),
            Self::CliMapping => ("turn_cli_runtime_binding", "turn_id", "input_mapping_json"),
            Self::TaskHistory => ("task_run_conversation_snapshot", "run_id", "history_json"),
            Self::TaskEvent => ("task_event", "id", "payload_json"),
        }
    }
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::ToolText => "tool.text",
            Self::ToolMetadata => "tool.metadata",
            Self::CliMapping => "cli.mapping",
            Self::TaskHistory => "task.history",
            Self::TaskEvent => "task.event",
        }
    }
}
fn sql(query: String, values: impl IntoIterator<Item = sea_orm::Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, query, values)
}

/// Only this worker owns its pending preparation. Staged headers are retired
/// until publication; cancellation/restart leaves them eligible for cleanup.
#[derive(Default)]
pub struct PortablePayloadProgress {
    family: usize,
    pending: Option<Preparation>,
    after: [String; 5],
}
struct Preparation {
    family: Family,
    owner: String,
    revision: i64,
    size: usize,
    offset: usize,
    bytes: Vec<u8>,
    digest: Sha256,
    chars: i64,
    opaque: bool,
    prepared: Option<Prepared>,
}
struct Prepared {
    header: value::Model,
    blocks: Vec<block::Model>,
    next: usize,
    installed: bool,
    projection: Option<String>,
}

impl PortablePayloadProgress {
    pub async fn quantum(&mut self, store: &CrudStore) -> Result<bool> {
        if let Some(mut pending) = self.pending.take() {
            let result = pending.step(store).await;
            match result {
                Ok(false) => self.pending = Some(pending),
                Ok(true) => {}
                Err(error) => {
                    // Preserve preparation on transient database failure. The
                    // Gateway quantum retries lock races and backs off errors.
                    // Advance this pass; a failed source is diagnosed and
                    // revisited after EOF. It cannot starve later sources.
                    return Err(error);
                }
            }
            return Ok(true);
        }
        if cleanup(store).await? {
            return Ok(true);
        }
        // Only families installed by the corresponding target migration are
        // registered here. Completed EOF is deliberately revisited next cycle.
        const FAMILIES: &[Family] = &[Family::ToolText, Family::ToolMetadata];
        let family = FAMILIES[self.family];
        self.family = (self.family + 1) % FAMILIES.len();
        let (table, key, field) = family.spec();
        let eligibility = match family {
            Family::CliMapping | Family::TaskHistory => {
                " AND (frozen_manifest_id IS NULL OR frozen_manifest_id<>'')"
            }
            _ => "",
        };
        // NULL metadata is final SQL NULL, not an empty/pending value.
        let projection = if family == Family::CliMapping {
            ", CASE WHEN json_valid(input_mapping_json) THEN coalesce(json_extract(input_mapping_json,'$.pioneerContextBasis.historyJson'),'[]')<>'[]' ELSE 1 END AS opaque"
        } else {
            ", 0 AS opaque"
        };
        let row = store.connection.query_one_raw(sql(format!(
            "SELECT {key} AS owner,portable_revision,octet_length({field}) AS bytes{projection} FROM {table} WHERE {field}_payload_id IS NULL AND {field} IS NOT NULL{eligibility} AND {key}>?1 ORDER BY {key} LIMIT 1"), [self.after[family as usize].clone().into()])).await?;
        if let Some(row) = row {
            self.after[family as usize] = row.try_get("", "owner")?;
            self.pending = Some(Preparation {
                family,
                owner: row.try_get("", "owner")?,
                revision: row.try_get("", "portable_revision")?,
                size: usize::try_from(row.try_get::<i64>("", "bytes")?)?,
                offset: 0,
                bytes: Vec::new(),
                digest: Sha256::new(),
                chars: 0,
                opaque: row.try_get::<i64>("", "opaque")? != 0,
                prepared: None,
            });
            return Ok(true);
        }
        self.after[family as usize].clear();
        Ok(false)
    }
}
impl Preparation {
    async fn step(&mut self, store: &CrudStore) -> Result<bool> {
        let (table, key, field) = self.family.spec();
        if self.offset < self.size {
            let row = store.connection.query_one_raw(sql(format!(
                "SELECT substr(CAST({field} AS BLOB),?3,?4) AS fragment FROM {table} WHERE {key}=?1 AND portable_revision=?2 AND {field}_payload_id IS NULL AND octet_length({field})=?5"),
                [self.owner.clone().into(),self.revision.into(),i64::try_from(self.offset+1)?.into(),i64::try_from(BLOCK.min(self.size-self.offset))?.into(),i64::try_from(self.size)?.into()])).await?;
            let Some(row) = row else {
                return Ok(true);
            };
            let fragment: Vec<u8> = row.try_get("", "fragment")?;
            ensure!(
                fragment.len() == BLOCK.min(self.size - self.offset),
                "portable source fragment incomplete"
            );
            self.digest.update(&fragment);
            self.chars += fragment.iter().filter(|b| (**b & 0xc0) != 0x80).count() as i64;
            self.offset += fragment.len();
            if self.size <= BUFFER {
                self.bytes.extend(fragment);
            }
            return Ok(false);
        }
        if self.prepared.is_none() {
            // No connection, transaction, permit or writer reservation here.
            // Work/memory is bounded by BUFFER; larger values finish as raw.
            let dict = load_or_prepare_dictionary(store, self.family, &self.bytes).await?;
            let digest = hex::encode(self.digest.clone().finalize());
            let id = uuid::Uuid::new_v4().to_string();
            let mut blocks = Vec::new();
            let mut offset = 0;
            let mut chars = 0i64;
            let text = std::str::from_utf8(&self.bytes)?;
            while offset < self.bytes.len() {
                let mut end = (offset + BLOCK).min(self.bytes.len());
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                let part = &text[offset..end];
                let encoded = codec::encode(part.as_bytes(), &dict)?;
                blocks.push(block::Model {
                    payload_id: id.clone(),
                    ordinal: blocks.len() as i64,
                    byte_offset: offset as i64,
                    char_offset: chars,
                    original_bytes: part.len() as i64,
                    original_chars: part.chars().count() as i64,
                    data: encoded,
                });
                chars += part.chars().count() as i64;
                offset = end;
            }
            let encoded_bytes = blocks.iter().map(|b| b.data.len() + 48).sum::<usize>();
            let raw = self.size > BUFFER || encoded_bytes >= self.size;
            if raw {
                blocks.clear();
            }
            let projection = if raw {
                None
            } else {
                Some(match self.family {
                    Family::TaskHistory => {
                        if text == "[]" {
                            "[]".into()
                        } else if text.trim_start_matches(' ').starts_with('[') {
                            "[".into()
                        } else {
                            "{}".into()
                        }
                    }
                    Family::CliMapping => {
                        if self.opaque {
                            r#"{"pioneerContextBasis":{"historyJson":"opaque"}}"#.into()
                        } else {
                            "{}".into()
                        }
                    }
                    _ => String::new(),
                })
            };
            self.prepared = Some(Prepared {
                header: value::Model {
                    id,
                    family: self.family.name().into(),
                    original_bytes: self.size as i64,
                    original_chars: self.chars,
                    sha256: digest,
                    raw: i32::from(raw),
                    retired: 1,
                    dictionary_id: (!raw)
                        .then(|| dictionary_key(self.family.name(), dict.digest())),
                },
                blocks,
                next: 0,
                installed: false,
                projection,
            });
            self.bytes.clear();
            self.bytes.shrink_to_fit();
            return Ok(false);
        }
        let prepared = self
            .prepared
            .as_mut()
            .context("portable preparation missing")?;
        if !prepared.installed {
            value::Entity::insert(prepared.header.clone().into_active_model())
                .on_conflict(
                    OnConflict::column(value::Column::Id)
                        .do_nothing()
                        .to_owned(),
                )
                .exec_without_returning(&store.connection)
                .await?;
            prepared.installed = true;
            return Ok(false);
        }
        if prepared.next < prepared.blocks.len() {
            let end = (prepared.next + 16).min(prepared.blocks.len());
            let rows = prepared.blocks[prepared.next..end]
                .iter()
                .cloned()
                .map(|b| b.into_active_model())
                .collect::<Vec<_>>();
            block::Entity::insert_many(rows)
                .on_conflict(
                    OnConflict::columns([block::Column::PayloadId, block::Column::Ordinal])
                        .do_nothing()
                        .to_owned(),
                )
                .exec_without_returning(&store.connection)
                .await?;
            prepared.next = end;
            return Ok(false);
        }
        let tx = store.connection.begin().await?;
        // Source revision triggers cover logical payload and ownership updates.
        // Unknown/expired roots cannot pass the existing verified root trigger.
        // Revalidate Task/run/workspace at publication, not from preparation.
        let owner_guard = match self.family {
            Family::TaskHistory => {
                " AND EXISTS(SELECT 1 FROM task t JOIN task_run r ON r.task_id=t.id WHERE r.id=task_run_conversation_snapshot.run_id AND t.id=task_run_conversation_snapshot.task_id AND t.workspace_id=task_run_conversation_snapshot.workspace_id)"
            }
            Family::ToolText | Family::ToolMetadata => {
                " AND EXISTS(SELECT 1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id=tool_output_chunk.turn_id)"
            }
            Family::CliMapping => {
                " AND EXISTS(SELECT 1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id=turn_cli_runtime_binding.turn_id AND th.workspace_id=turn_cli_runtime_binding.workspace_id AND t.thread_id=turn_cli_runtime_binding.thread_id)"
            }
            Family::TaskEvent => " AND EXISTS(SELECT 1 FROM task t WHERE t.id=task_event.task_id)",
        };
        let assignment = if prepared.projection.is_some() {
            format!("{field}=?4,")
        } else {
            String::new()
        };
        let mut bindings = vec![
            self.owner.clone().into(),
            self.revision.into(),
            prepared.header.id.clone().into(),
        ];
        if let Some(projection) = &prepared.projection {
            bindings.push(projection.clone().into());
        }
        let affected=tx.execute_raw(sql(format!("UPDATE {table} SET {assignment}{field}_payload_id=?3 WHERE {key}=?1 AND portable_revision=?2 AND {field}_payload_id IS NULL{owner_guard}"),bindings)).await?.rows_affected();
        if affected == 1 {
            value::Entity::update_many()
                .col_expr(value::Column::Retired, Expr::value(0))
                .filter(value::Column::Id.eq(&prepared.header.id))
                .exec(&tx)
                .await?;
        }
        tx.commit().await?;
        Ok(true)
    }
}
use sea_orm::IntoActiveModel;
async fn load_or_prepare_dictionary(
    store: &CrudStore,
    family: Family,
    sample: &[u8],
) -> Result<codec::Dictionary> {
    if let Some(row) = dictionary::Entity::find()
        .filter(dictionary::Column::Family.eq(family.name()))
        .filter(dictionary::Column::Version.eq(1))
        .one(&store.connection)
        .await?
    {
        let result = codec::Dictionary::from_bytes(row.data)?;
        ensure!(
            dictionary_key(family.name(), result.digest()) == row.id,
            "portable dictionary digest mismatch"
        );
        return Ok(result);
    }
    let samples = sample[..sample.len().min(1024 * 1024)]
        .chunks(1024)
        .collect::<Vec<_>>();
    let dict = codec::train(&samples)?;
    dictionary::Entity::insert(dictionary::ActiveModel {
        id: Set(dictionary_key(family.name(), dict.digest())),
        family: Set(family.name().into()),
        version: Set(1),
        data: Set(dict.bytes().to_vec()),
    })
    .on_conflict(
        OnConflict::columns([dictionary::Column::Family, dictionary::Column::Version])
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(&store.connection)
    .await?;
    let winner = dictionary::Entity::find()
        .filter(dictionary::Column::Family.eq(family.name()))
        .filter(dictionary::Column::Version.eq(1))
        .one(&store.connection)
        .await?
        .context("portable dictionary publication missing")?;
    let dict = codec::Dictionary::from_bytes(winner.data)?;
    ensure!(
        dictionary_key(family.name(), dict.digest()) == winner.id,
        "portable dictionary digest mismatch"
    );
    Ok(dict)
}
async fn cleanup(store: &CrudStore) -> Result<bool> {
    let Some(row) = value::Entity::find()
        .filter(value::Column::Retired.eq(1))
        .order_by_asc(value::Column::Id)
        .one(&store.connection)
        .await?
    else {
        return Ok(false);
    };
    let ids = block::Entity::find()
        .select_only()
        .column(block::Column::Ordinal)
        .filter(block::Column::PayloadId.eq(&row.id))
        .order_by_asc(block::Column::Ordinal)
        .limit(16)
        .into_tuple::<i64>()
        .all(&store.connection)
        .await?;
    if ids.is_empty() {
        value::Entity::delete_by_id(row.id)
            .exec(&store.connection)
            .await?;
    } else {
        block::Entity::delete_many()
            .filter(block::Column::PayloadId.eq(row.id))
            .filter(block::Column::Ordinal.is_in(ids))
            .exec(&store.connection)
            .await?;
    }
    Ok(true)
}

pub(crate) async fn header<C: ConnectionTrait>(
    db: &C,
    id: &str,
    family: Family,
) -> Result<value::Model> {
    let row = value::Entity::find_by_id(id)
        .one(db)
        .await?
        .context("portable payload header missing")?;
    ensure!(
        row.family == family.name()
            && row.retired == 0
            && row.original_bytes >= 0
            && row.original_chars >= 0
            && (row.raw == 0 || row.raw == 1),
        "portable payload header invalid or replaced"
    );
    Ok(row)
}
/// Call only after releasing any transaction. A full logical read remains O(n).
pub(crate) async fn hydrate<C: ConnectionTrait>(
    db: &C,
    family: Family,
    id: Option<&str>,
    plain: String,
) -> Result<String> {
    let Some(id) = id else {
        return Ok(plain);
    };
    let meta = header(db, id, family).await?;
    if meta.raw == 1 {
        ensure!(
            plain.len() as i64 == meta.original_bytes
                && hex::encode(Sha256::digest(plain.as_bytes())) == meta.sha256,
            "portable raw payload mismatch"
        );
        return Ok(plain);
    }
    let size = usize::try_from(meta.original_bytes)?;
    let mut bytes = Vec::new();
    let mut offset = 0;
    while offset < size {
        let part = read_range(db, &meta, offset as i64, (size - offset).min(BLOCK), false).await?;
        ensure!(!part.is_empty(), "portable payload block missing");
        offset += part.len();
        bytes.extend(part);
    }
    ensure!(
        bytes.len() == size && hex::encode(Sha256::digest(&bytes)) == meta.sha256,
        "portable payload original digest mismatch"
    );
    Ok(String::from_utf8(bytes)?)
}
/// Range is in bytes, or Unicode scalars for existing compaction substr(TEXT).
/// Up to three indexed blocks are fetched, released, then decoded.
pub(crate) async fn read_range<C: ConnectionTrait>(
    db: &C,
    meta: &value::Model,
    start: i64,
    count: usize,
    characters: bool,
) -> Result<Vec<u8>> {
    ensure!(
        meta.raw == 0 && start >= 0 && count <= if characters { 16384 } else { BLOCK },
        "portable range budget invalid"
    );
    let total = if characters {
        meta.original_chars
    } else {
        meta.original_bytes
    };
    ensure!(start <= total, "portable range starts past end");
    let count = (count as i64).min(total - start);
    if count == 0 {
        return Ok(Vec::new());
    }
    let column = if characters {
        block::Column::CharOffset
    } else {
        block::Column::ByteOffset
    };
    let first = block::Entity::find()
        .select_only()
        .column(block::Column::Ordinal)
        .filter(block::Column::PayloadId.eq(&meta.id))
        .filter(column.lte(start))
        .order_by_desc(column)
        .limit(1)
        .into_tuple::<i64>()
        .one(db)
        .await?
        .context("portable range block missing")?;
    let blocks = block::Entity::find()
        .filter(block::Column::PayloadId.eq(&meta.id))
        .filter(block::Column::Ordinal.gte(first))
        .order_by_asc(block::Column::Ordinal)
        .limit(3)
        .all(db)
        .await?;
    let dictionary_id = meta
        .dictionary_id
        .as_deref()
        .context("portable payload dictionary reference missing")?;
    let dict = dictionary::Entity::find_by_id(dictionary_id)
        .one(db)
        .await?
        .context("portable payload dictionary missing")?;
    ensure!(
        dict.family == meta.family,
        "portable dictionary family mismatch"
    );
    let dict = codec::Dictionary::from_bytes(dict.data)?;
    ensure!(
        dictionary_key(&meta.family, dict.digest()) == dictionary_id,
        "portable dictionary digest mismatch"
    );
    let mut output = Vec::new();
    let mut position = start;
    for b in blocks {
        let offset = if characters {
            b.char_offset
        } else {
            b.byte_offset
        };
        let length = if characters {
            b.original_chars
        } else {
            b.original_bytes
        };
        ensure!(
            b.original_bytes > 0 && b.original_bytes <= BLOCK as i64 && offset <= position,
            "portable block continuity invalid"
        );
        if offset >= start + count {
            break;
        }
        let decoded = codec::decode(&b.data, Some(&dict))?;
        ensure!(
            decoded.len() as i64 == b.original_bytes,
            "portable block length mismatch"
        );
        let text = std::str::from_utf8(&decoded)?;
        ensure!(
            text.chars().count() as i64 == b.original_chars,
            "portable block character count mismatch"
        );
        let local = usize::try_from(position - offset)?;
        let take = usize::try_from((start + count - position).min(length - local as i64))?;
        if characters {
            output.extend(
                text.chars()
                    .skip(local)
                    .take(take)
                    .collect::<String>()
                    .into_bytes(),
            );
        } else {
            output.extend_from_slice(&decoded[local..local + take]);
        }
        position += take as i64;
        if position == start + count {
            break;
        }
    }
    ensure!(position == start + count, "portable range incomplete");
    Ok(output)
}

fn dictionary_key(family: &str, digest: [u8; 32]) -> String {
    format!("{family}:1:{}", hex::encode(digest))
}

#[derive(Debug)]
pub(crate) struct PreflightRace;
impl std::fmt::Display for PreflightRace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("portable payload changed after preflight")
    }
}
impl std::error::Error for PreflightRace {}
