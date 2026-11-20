//! Ordered, payload-free size discovery for canonical history batches.

use sea_orm::{ConnectionTrait, DbBackend, DbErr, FromQueryResult, Statement};

#[derive(FromQueryResult)]
pub struct PayloadSize {
    pub ordinal: i64,
    pub bytes: i64,
}

#[derive(FromQueryResult)]
pub struct PayloadValue {
    pub ordinal: i64,
    pub payload: String,
}

fn requested_sources(
    workspace: &str,
    thread: &str,
    sources: &str,
    limit: i64,
    input: &str,
    context: &str,
) -> Statement {
    // SeaQuery does not expose WITH ORDINALITY on a set-returning function.
    // Positions preserve duplicates and the caller's order across both tables.
    Statement::from_sql_and_values(
        DbBackend::Postgres,
        format!(
            "WITH wanted AS MATERIALIZED (
                SELECT w.value->>'scope' AS scope, w.value->>'id' AS id,
                       w.value->>'version' AS version, w.position - 1 AS ordinal
                FROM json_array_elements($1::text::json) WITH ORDINALITY AS w(value, position)
                WHERE w.position <= $4
             ), resolved AS MATERIALIZED (
                SELECT w.ordinal, s.source_scope, s.source_id
                FROM wanted w
                JOIN compaction_live_sources s ON s.workspace_id=$2 AND s.thread_id=$3
                 AND s.source_scope=w.scope AND s.source_id=w.id AND s.source_version=w.version
             )
             SELECT r.ordinal, {input}
               FROM resolved r JOIN turn_input i ON left(r.source_scope, 6)='input:'
                AND i.id=r.source_id AND i.turn_id=substr(r.source_scope, 7)
             UNION ALL
             SELECT r.ordinal, {context}
               FROM resolved r JOIN turn_llm_context c ON left(r.source_scope, 8)='context:'
                AND c.id=r.source_id AND c.turn_id=substr(r.source_scope, 9)
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
        "octet_length(i.payload::text)::bigint AS bytes",
        "octet_length(c.payload::text)::bigint AS bytes",
    ))
    .all(db)
    .await
}

pub async fn values(
    db: &impl ConnectionTrait,
    workspace: &str,
    thread: &str,
    sources: &str,
    limit: i64,
) -> Result<Vec<PayloadValue>, DbErr> {
    PayloadValue::find_by_statement(requested_sources(
        workspace,
        thread,
        sources,
        limit,
        "i.payload::text AS payload",
        "c.payload::text AS payload",
    ))
    .all(db)
    .await
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
