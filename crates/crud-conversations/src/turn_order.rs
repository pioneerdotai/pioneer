//! Indexed conversation and CLI turn ordering.

use anyhow::{Context, Result};
use pioneer_entity::turn;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, FromQueryResult, Statement,
    entity::prelude::DateTimeWithTimeZone,
};

pub const THREAD_TURN_SNAPSHOT_BATCH_SIZE: usize = 128;

/// Only the already selected thread IDs are visited. Each correlated seek
/// walks the existing history index backwards and stops at its first match;
/// the outer primary-key lookup fetches at most one model per selected ID.
/// This is deliberately not a ranking/aggregation over the turn catalogue.
pub async fn find_latest_turns_for_threads<C: ConnectionTrait>(
    db: &C,
    thread_ids: &[&str],
    conversation_only: bool,
) -> Result<Vec<turn::Model>> {
    anyhow::ensure!(
        thread_ids.len() <= THREAD_TURN_SNAPSHOT_BATCH_SIZE,
        "thread turn snapshot batch exceeds its limit"
    );
    if thread_ids.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders = (1..=thread_ids.len())
        .map(|index| format!("(${index}::text)"))
        .collect::<Vec<_>>()
        .join(", ");
    // SeaORM generated relations do not expose a correlated LIMIT on the
    // related models. LATERAL uses the history index for each requested ID.
    let kind_filter = if conversation_only {
        "AND candidate.turn_kind = 'conversation'"
    } else {
        ""
    };
    let statement = Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        format!(
            "WITH selected(thread_id) AS (VALUES {placeholders})
             SELECT latest.* FROM selected CROSS JOIN LATERAL (
                 SELECT candidate.* FROM turn AS candidate
                 WHERE candidate.thread_id = selected.thread_id {kind_filter}
                 ORDER BY candidate.created_at DESC, candidate.id DESC LIMIT 1
             ) AS latest"
        ),
        thread_ids.iter().map(|id| (*id).into()),
    );
    let rows = db
        .query_all_raw(statement)
        .await
        .context("failed to query latest thread turn snapshot batch")?;
    // query_all_raw returns the bounded rows and releases reader capacity
    // before model decoding or any snapshot/JSON transformation.
    rows.iter()
        .map(|row| turn::Model::from_query_result(row, ""))
        .collect::<Result<Vec<_>, _>>()
        .context("failed to decode latest thread turn snapshot batch")
}

/// The history order is timestamp, creation sequence, insertion ordinal and ID.
/// The (thread_id, created_at, id) index finds the timestamp bucket.
/// This avoids ranking all turns merely to find one boundary.
struct CliTurnOrderKey {
    created_at: DateTimeWithTimeZone,
    sequence: i64,
    insertion_ordinal: i64,
    id: String,
}

async fn cli_turn_order_key<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<CliTurnOrderKey>> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT t.created_at AS created_at, COALESCE(c.sequence,0) AS sequence, \
             t.insertion_ordinal AS insertion_ordinal, t.id AS id FROM turn t \
             LEFT JOIN compaction_turn_creation c ON c.turn_id=(t.id COLLATE \"default\") \
             WHERE t.thread_id=$1 AND t.id=$2",
            vec![thread_id.into(), turn_id.into()],
        ))
        .await?;
    row.map(|row| {
        Ok(CliTurnOrderKey {
            created_at: row.try_get("", "created_at")?,
            sequence: row.try_get("", "sequence")?,
            insertion_ordinal: row.try_get("", "insertion_ordinal")?,
            id: row.try_get("", "id")?,
        })
    })
    .transpose()
}

async fn cli_bucket_ids<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    timestamp: &DateTimeWithTimeZone,
    after: Option<&CliTurnOrderKey>,
    descending: bool,
    limit: usize,
) -> Result<Vec<String>> {
    let (relation, limit_parameter) = if after.is_some() {
        (
            "AND (COALESCE(c.sequence,0),t.insertion_ordinal,t.id) > ($3,$4,$5)",
            "$6",
        )
    } else {
        ("", "$3")
    };
    let direction = if descending { "DESC" } else { "ASC" };
    let sql = format!(
        "SELECT t.id AS id FROM turn t LEFT JOIN compaction_turn_creation c ON c.turn_id=(t.id COLLATE \"default\") \
         WHERE t.thread_id=$1 AND t.created_at=$2 {relation} \
         ORDER BY COALESCE(c.sequence,0) {direction}, t.insertion_ordinal {direction}, t.id {direction} LIMIT {limit_parameter}"
    );
    let mut values = vec![thread_id.into(), (*timestamp).into()];
    if let Some(after) = after {
        values.extend([
            after.sequence.into(),
            after.insertion_ordinal.into(),
            after.id.clone().into(),
        ]);
    }
    values.push(i64::try_from(limit)?.into());
    db.query_all_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        sql,
        values,
    ))
    .await?
    .into_iter()
    .map(|row| row.try_get::<String>("", "id").map_err(Into::into))
    .collect()
}

async fn cli_adjacent_timestamp<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    timestamp: Option<&DateTimeWithTimeZone>,
    descending: bool,
) -> Result<Option<DateTimeWithTimeZone>> {
    let relation = match (timestamp, descending) {
        (Some(_), true) => "AND created_at < $2",
        (Some(_), false) => "AND created_at > $2",
        (None, _) => "",
    };
    let direction = if descending { "DESC" } else { "ASC" };
    let sql = format!(
        "SELECT created_at FROM turn WHERE thread_id=$1 {relation} \
         ORDER BY created_at {direction} LIMIT 1"
    );
    let mut values = vec![thread_id.into()];
    if let Some(timestamp) = timestamp {
        values.push((*timestamp).into());
    }
    db.query_one_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        sql,
        values,
    ))
    .await?
    .map(|row| {
        row.try_get::<DateTimeWithTimeZone>("", "created_at")
            .map_err(Into::into)
    })
    .transpose()
}

pub async fn latest_turn_id_by_creation_order(
    db: &pioneer_sqlite::SqliteDatabase,
    thread_id: &str,
) -> Result<Option<String>> {
    // The timestamp seek and tie-breaker must see one reader snapshot.
    let snapshot = db.begin_read().await?;
    let db = &snapshot;
    let Some(timestamp) = cli_adjacent_timestamp(db, thread_id, None, true).await? else {
        return Ok(None);
    };
    Ok(cli_bucket_ids(db, thread_id, &timestamp, None, true, 1)
        .await?
        .into_iter()
        .next())
}

pub async fn turn_before_launch_and_intervening_by_creation_order(
    db: &pioneer_sqlite::SqliteDatabase,
    thread_id: &str,
    launch_turn_id: &str,
    allowed_task_run_turn_id: &str,
) -> Result<Option<(Option<String>, bool)>> {
    // Hold one snapshot across the launch boundary and the adjoining buckets.
    let snapshot = db.begin_read().await?;
    let db = &snapshot;
    let Some(launch) = cli_turn_order_key(db, thread_id, launch_turn_id).await? else {
        return Ok(None);
    };
    let previous_in_bucket = db
        .query_one_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT t.id AS id FROM turn t \
             LEFT JOIN compaction_turn_creation c ON c.turn_id=(t.id COLLATE \"default\") \
             WHERE t.thread_id=$1 AND t.created_at=$2 \
               AND (COALESCE(c.sequence,0),t.insertion_ordinal,t.id) < ($3,$4,$5) \
             ORDER BY COALESCE(c.sequence,0) DESC,t.insertion_ordinal DESC,t.id DESC LIMIT 1",
            vec![
                thread_id.into(),
                launch.created_at.into(),
                launch.sequence.into(),
                launch.insertion_ordinal.into(),
                launch.id.clone().into(),
            ],
        ))
        .await?;
    let previous = if let Some(row) = previous_in_bucket {
        Some(row.try_get::<String>("", "id")?)
    } else if let Some(timestamp) =
        cli_adjacent_timestamp(db, thread_id, Some(&launch.created_at), true).await?
    {
        cli_bucket_ids(db, thread_id, &timestamp, None, true, 1)
            .await?
            .into_iter()
            .next()
    } else {
        None
    };
    let intervening = db
        .query_one_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT (EXISTS(SELECT 1 FROM turn t WHERE t.thread_id=$1 \
                AND t.created_at>$2 AND t.id<>$3) OR \
              EXISTS(SELECT 1 FROM turn t LEFT JOIN compaction_turn_creation c ON c.turn_id=(t.id COLLATE \"default\") \
                WHERE t.thread_id=$1 AND t.created_at=$2 AND t.id<>$3 \
                AND (COALESCE(c.sequence,0),t.insertion_ordinal,t.id) > ($4,$5,$6))) AS intervening",
            vec![
                thread_id.into(),
                launch.created_at.into(),
                allowed_task_run_turn_id.into(),

                launch.sequence.into(),
                launch.insertion_ordinal.into(),
                launch.id.into(),
            ],
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("CLI intervening-turn query returned no row"))?
        .try_get::<bool>("", "intervening")?;
    Ok(Some((previous, intervening)))
}

pub async fn turn_ids_after_by_creation_order(
    db: &pioneer_sqlite::SqliteDatabase,
    thread_id: &str,
    source_turn_id: &str,
) -> Result<Vec<String>> {
    // Hold one snapshot across bounded metadata buckets to keep retry ordering stable.
    let snapshot = db.begin_read().await?;
    let db = &snapshot;
    const MAX_FORK_RETRY_ATTEMPTS: usize = 128;
    let Some(source) = cli_turn_order_key(db, thread_id, source_turn_id).await? else {
        return Ok(Vec::new());
    };
    let mut rows = cli_bucket_ids(
        db,
        thread_id,
        &source.created_at,
        Some(&source),
        false,
        MAX_FORK_RETRY_ATTEMPTS + 1,
    )
    .await?;
    let mut timestamp = source.created_at;
    while rows.len() <= MAX_FORK_RETRY_ATTEMPTS {
        let Some(next) = cli_adjacent_timestamp(db, thread_id, Some(&timestamp), false).await?
        else {
            break;
        };
        rows.extend(
            cli_bucket_ids(
                db,
                thread_id,
                &next,
                None,
                false,
                MAX_FORK_RETRY_ATTEMPTS + 1 - rows.len(),
            )
            .await?,
        );
        timestamp = next;
    }
    anyhow::ensure!(
        rows.len() <= MAX_FORK_RETRY_ATTEMPTS,
        "CLI fork retry exceeds the bounded attempt scan"
    );
    Ok(rows)
}
