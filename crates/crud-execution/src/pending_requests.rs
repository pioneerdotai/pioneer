use anyhow::{Context, Result};
use pioneer_entity::cli_runtime_pending_request;
use sea_orm::{ConnectionTrait, DbBackend, EntityTrait, Statement};

pub async fn list_open_descendants<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    statuses: [&str; 4],
    limit: u64,
) -> Result<Vec<cli_runtime_pending_request::Model>> {
    let limit = i64::try_from(limit).context("pending request page limit exceeds integer range")?;
    // SeaORM has no recursive union on Entity::find. Keep subtree traversal in
    // a single indexed query and decode through the generated Entity.
    let statement = Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
WITH RECURSIVE descendant_threads(thread_id) AS (
    SELECT child_thread_id FROM thread_lineage WHERE parent_thread_id = $1
    UNION
    SELECT lineage.child_thread_id
    FROM thread_lineage AS lineage
    JOIN descendant_threads AS descendant ON lineage.parent_thread_id = descendant.thread_id
)
SELECT pending.*
FROM cli_runtime_pending_request AS pending
JOIN descendant_threads AS descendant ON descendant.thread_id = pending.thread_id
WHERE pending.workspace_id = $2 AND pending.status IN ($3, $4, $5, $6)
ORDER BY pending.created_at ASC, pending.request_id ASC
LIMIT $7
"#,
        [
            thread_id.into(),
            workspace_id.into(),
            statuses[0].into(),
            statuses[1].into(),
            statuses[2].into(),
            statuses[3].into(),
            limit.into(),
        ],
    );
    cli_runtime_pending_request::Entity::find()
        .from_raw_sql(statement)
        .all(db)
        .await
        .context("failed to list open CLI runtime pending requests for descendant threads")
}
