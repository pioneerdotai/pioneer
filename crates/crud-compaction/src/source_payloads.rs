//! Ordered, payload-free size discovery for canonical history batches.

use sea_orm::{ConnectionTrait, DbBackend, DbErr, FromQueryResult, Statement};

#[derive(FromQueryResult)]
pub struct PayloadSize {
    pub ordinal: i64,
    pub bytes: i64,
}

#[derive(FromQueryResult)]
struct PayloadLocation {
    ordinal: i64,
    payload: Option<String>,
    payload_id: Option<String>,
}

pub enum CapturedSource {
    Inline(String),
    Portable(pioneer_crud_payloads::storage::CapturedPayload),
}

impl CapturedSource {
    pub fn into_text(self) -> anyhow::Result<String> {
        match self {
            Self::Inline(text) => Ok(text),
            Self::Portable(body) => body.into_text(),
        }
    }
}

fn requested_sources(
    workspace: &str,
    thread: &str,
    sources: &str,
    limit: i64,
    direct: &str,
    portable: &str,
) -> Statement {
    // SeaQuery does not expose WITH ORDINALITY on a set-returning function.
    // Bind values and retain positions across both storage formats.
    Statement::from_sql_and_values(
        DbBackend::Postgres,
        format!(
            "WITH wanted AS MATERIALIZED (
                SELECT w.value->>'scope' AS scope, w.value->>'id' AS id,
                       w.value->>'version' AS version, w.position - 1 AS ordinal
                FROM json_array_elements($1::text::json) WITH ORDINALITY AS w(value, position)
                WHERE w.position <= $4
             ), resolved AS MATERIALIZED (
                SELECT w.ordinal, w.scope AS source_scope, w.id AS source_id, s.turn_id
                FROM wanted w
                JOIN LATERAL (
                    SELECT live.turn_id FROM compaction_live_sources live
                    WHERE live.workspace_id=$2 AND live.thread_id=$3
                      AND live.source_scope=w.scope AND live.source_id=w.id
                      AND live.source_version=w.version
                    LIMIT 1
                ) s ON true
             )
             SELECT r.ordinal, body.*
               FROM resolved r JOIN LATERAL (
                  SELECT {direct} FROM turn_input s
                  WHERE s.id=r.source_id AND s.turn_id=r.turn_id
                    AND r.source_scope='input:'||r.turn_id
                  LIMIT 1
               ) body ON true
             UNION ALL
             SELECT r.ordinal, body.*
               FROM resolved r JOIN LATERAL (
                  SELECT {direct} FROM turn_llm_context s
                  WHERE s.id=r.source_id AND s.turn_id=r.turn_id
                    AND r.source_scope='context:'||r.turn_id
                  LIMIT 1
               ) body ON true
             UNION ALL
             SELECT r.ordinal, body.*
               FROM resolved r JOIN LATERAL (
                  SELECT {portable} FROM turn_event s
                  LEFT JOIN portable_payload_value p ON p.id=s.payload_payload_id
                  WHERE s.id=r.source_id AND s.turn_id=r.turn_id
                    AND r.source_scope='event:'||r.turn_id
                  LIMIT 1
               ) body ON true
             UNION ALL
             SELECT r.ordinal, body.*
               FROM resolved r JOIN LATERAL (
                  SELECT {portable} FROM turn_item s
                  LEFT JOIN portable_payload_value p ON p.id=s.payload_payload_id
                  WHERE s.id=r.source_id AND s.turn_id=r.turn_id
                    AND r.source_scope='item:'||r.turn_id
                  LIMIT 1
               ) body ON true
             ORDER BY ordinal"
        ),
        [
            sources.into(),
            workspace.into(),
            thread.into(),
            limit.into(),
        ],
    )
}

pub async fn sizes(
    db: &impl ConnectionTrait,
    workspace: &str,
    thread: &str,
    sources: &str,
    limit: i64,
) -> Result<Vec<PayloadSize>, DbErr> {
    PayloadSize::find_by_statement(requested_sources(
        workspace,
        thread,
        sources,
        limit,
        "octet_length(s.payload::text)::bigint AS bytes",
        "COALESCE(p.original_bytes, octet_length(s.payload::text)::bigint) AS bytes",
    ))
    .all(db)
    .await
}

/// Capture the selected page in the same read snapshot as its size discovery.
/// Decompression and JSON parsing belong to the caller after snapshot release.
pub async fn capture(
    db: &impl ConnectionTrait,
    workspace: &str,
    thread: &str,
    sources: &str,
    limit: i64,
) -> anyhow::Result<Vec<(i64, CapturedSource)>> {
    use anyhow::Context;
    use pioneer_crud_payloads::storage::{self, Selection};
    let rows = PayloadLocation::find_by_statement(requested_sources(
        workspace,
        thread,
        sources,
        limit,
        "s.payload::text AS payload, NULL::text AS payload_id",
        "s.payload::text AS payload, s.payload_payload_id AS payload_id",
    ))
    .all(db)
    .await?;
    let mut captured = Vec::with_capacity(rows.len());
    for row in rows {
        let body = if let Some(id) = row.payload_id {
            CapturedSource::Portable(
                storage::capture(db, &id, Selection::Full)
                    .await?
                    .context("missing canonical history payload")?,
            )
        } else {
            CapturedSource::Inline(row.payload.context("missing inline history payload")?)
        };
        captured.push((row.ordinal, body));
    }
    Ok(captured)
}
/// Read a bounded page of already validated canonical source identities.
pub async fn read_page(
    db: &pioneer_postgres_pools::PostgresDatabase,
    workspace: &str,
    thread: &str,
    candidates: &[pioneer_compaction::SourceRef],
    max_bytes: usize,
) -> anyhow::Result<(usize, Vec<String>)> {
    use anyhow::ensure;
    let batchable = candidates.len();
    let encoded = serde_json::to_string(candidates)?;
    let reader = db.begin_read().await?;
    let sizes = sizes(
        &reader,
        workspace,
        thread,
        &encoded,
        i64::try_from(batchable)?,
    )
    .await?;
    if sizes.len() != candidates.len() {
        reader.commit().await?;
        anyhow::bail!("selected history source disappeared");
    }
    let mut consumed = 0usize;
    let mut bytes = 0usize;
    for (ordinal, row) in sizes.iter().enumerate() {
        ensure!(
            row.ordinal == ordinal as i64 && row.bytes >= 0,
            "invalid history payload size"
        );
        let size = usize::try_from(row.bytes)?;
        if consumed > 0 && bytes.saturating_add(size) > max_bytes {
            break;
        }
        bytes = bytes.saturating_add(size);
        consumed += 1;
    }
    // A single large value is admitted rather than imposing a new history limit.
    let rows = capture(
        &reader,
        workspace,
        thread,
        &encoded,
        i64::try_from(consumed)?,
    )
    .await?;
    reader.commit().await?;
    ensure!(
        rows.len() == consumed,
        "selected history source changed while loading"
    );
    let mut payloads = Vec::with_capacity(consumed);
    for (ordinal, (stored_ordinal, body)) in rows.into_iter().enumerate() {
        ensure!(
            stored_ordinal == ordinal as i64,
            "history payload order changed"
        );
        payloads.push(body.into_text()?);
    }
    Ok((consumed, payloads))
}

/// Revisions for all current history sources are registered by the database
/// on mutation. A history reader needs only its scoped thread to exist here.
pub async fn require_history_scope<C: sea_orm::ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread_id: &str,
) -> anyhow::Result<()> {
    use pioneer_entity::thread;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect};

    anyhow::ensure!(
        thread::Entity::find_by_id(thread_id)
            .filter(thread::Column::WorkspaceId.eq(workspace))
            .select_only()
            .column(thread::Column::Id)
            .into_tuple::<String>()
            .one(db)
            .await?
            .is_some(),
        "history preparation thread is unavailable"
    );
    Ok(())
}
