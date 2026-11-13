//! SQLite adapter operations for the existing episodic search projection.
use crate::thread_episodic::{
    THREAD_SEARCH_RRF_K, ThreadEpisodicSearchQuery, ThreadEpisodicSearchResult,
    ThreadEpisodicSearchRow, thread_episodic_item_record_from_model,
};
use anyhow::{Result, ensure};
use pioneer_entity::thread_episodic_items;
use sea_orm::{ConnectionTrait, DbBackend, FromQueryResult, Statement, Value};

pub async fn search<C: ConnectionTrait>(
    db: &C,
    request: &ThreadEpisodicSearchQuery,
) -> Result<ThreadEpisodicSearchResult> {
    ensure!(
        request.limit > 0 && request.limit <= 257,
        "invalid search page size"
    );
    ensure!(
        (1..=4000).contains(&request.snippet_chars),
        "invalid snippet size"
    );
    let offset = i64::try_from(request.offset)?;
    let mut values: Vec<Value> = vec![request.workspace_id.clone().into()];
    let mut filters = vec!["i.workspace_id = ?".to_owned()];
    if let Some(thread) = &request.thread_id {
        filters.push("i.thread_id = ?".to_owned());
        values.push(thread.clone().into());
    }
    if let Some(root) = &request.access.root_thread_id {
        filters.push("r.id = ?".to_owned());
        values.push(root.clone().into());
    }
    if !request.access.include_internal {
        filters.push("(i.thread_id = r.id OR i.thread_id = ?)".to_owned());
        values.push(
            request
                .access
                .current_thread_id
                .clone()
                .unwrap_or_default()
                .into(),
        );
    }
    if let Some(principal) = &request.access.principal_id {
        filters.push("EXISTS (SELECT 1 FROM workspace_membership wm WHERE wm.workspace_id = i.workspace_id AND wm.principal_id = ?)".to_owned());
        values.push(principal.clone().into());
        filters.push("(r.access_class = 'workspace' OR (r.access_class = 'private' AND EXISTS (SELECT 1 FROM thread_membership tm WHERE tm.thread_id = r.id AND tm.principal_id = ?)))".to_owned());
        values.push(principal.clone().into());
    }
    if let Some(after) = request.created_after_unix {
        filters.push("CAST(strftime('%s',i.created_at) AS INTEGER) >= ?".to_owned());
        values.push(after.into());
    }
    let source_scope = format!(
        "{} WHERE {} {}",
        SOURCE_FROM,
        filters.join(" AND "),
        SOURCE_ELIGIBILITY
    );
    let mut coverage_values = values.clone();
    let missing_embedding = if let Some(embedding) = &request.embedding {
        coverage_values.extend([
            embedding.provider_id.clone().into(),
            embedding.model.clone().into(),
            i64::try_from(embedding.dimension)?.into(),
            embedding.normalized.into(),
        ]);
        " OR (i.status = 'active' AND NOT EXISTS (SELECT 1 FROM thread_episodic_embedding_artifacts a
             WHERE a.id = i.embedding_artifact_id AND a.workspace_id = i.workspace_id
               AND a.provider_id = ? AND a.model = ? AND a.dimension = ? AND a.normalized = ? AND length(a.vector_bytes) = 4*a.dimension))"
    } else {
        ""
    };
    let coverage_sql = format!("SELECT EXISTS(SELECT 1 {source_scope}
        AND (i.status IN ('pending_index','failed') OR (i.status = 'active' AND i.search_text IS NULL) {missing_embedding})) AS incomplete");
    let lexical = if request.lexical {
        values.push(request.query.clone().into());
        // Keep MATCH outside the occurrence loop, even without planner statistics.
        "SELECT e.search_rowid AS id, e.id AS sort_id, bm25(thread_episodic_fts) AS distance
         FROM thread_episodic_fts CROSS JOIN eligible e ON e.search_rowid = thread_episodic_fts.rowid
         WHERE thread_episodic_fts MATCH pioneer_fts_literal(?)".to_owned()
    } else {
        "SELECT search_rowid AS id, id AS sort_id, 0.0 AS distance FROM eligible WHERE 0".to_owned()
    };
    let semantic = if let Some(embedding) = &request.embedding {
        ensure!(
            embedding.dimension > 0
                && embedding.vector.len() == embedding.dimension
                && embedding.vector.iter().all(|value| value.is_finite())
                && embedding.vector.iter().any(|value| *value != 0.0),
            "invalid query embedding"
        );
        let bytes = embedding
            .vector
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        let dimension = i64::try_from(embedding.dimension)?;
        let byte_length = i64::try_from(
            embedding
                .dimension
                .checked_mul(4)
                .ok_or_else(|| anyhow::anyhow!("embedding size overflow"))?,
        )?;
        values.extend([
            embedding.provider_id.clone().into(),
            embedding.model.clone().into(),
            dimension.into(),
            embedding.normalized.into(),
            byte_length.into(),
            dimension.into(),
            byte_length.into(),
            bytes.into(),
        ]);
        // CASE is lazy in SQLite: distance cannot execute for an artifact that
        // did not pass the materialized identity/dimension/length boundary.
        // Only IDs and scalar scores are materialized, never corpus BLOBs.
        "SELECT a.id, CASE WHEN a.dimension = ? AND length(a.vector_bytes) = ?
            THEN vec_distance_cosine(a.vector_bytes, ?) END AS distance
         FROM semantic_artifacts ids CROSS JOIN thread_episodic_embedding_artifacts a
           ON a.rowid = ids.artifact_rowid"
            .to_owned()
    } else {
        "SELECT id, 0.0 AS distance FROM eligible WHERE 0".to_owned()
    };
    let semantic_artifacts = if request.embedding.is_some() {
        "SELECT a.id AS artifact_id, a.rowid AS artifact_rowid
         FROM eligible_artifact_ids e CROSS JOIN thread_episodic_embedding_artifacts a
           ON a.id = e.embedding_artifact_id AND a.workspace_id = e.workspace_id
         WHERE a.provider_id = ? AND a.model = ? AND a.dimension = ? AND a.normalized = ? AND length(a.vector_bytes) = ?
         ORDER BY a.rowid"
    } else {
        "SELECT id AS artifact_id FROM eligible WHERE 0"
    };
    // RRF is the common domain policy (equal branch weights, fixed k). Push its
    // order into SQLite so a deep page transfers only its bounded rows. Return
    // ranks, never BM25/distance; the common layer computes the public score.
    values.extend([
        THREAD_SEARCH_RRF_K.into(),
        THREAD_SEARCH_RRF_K.into(),
        i64::from(request.limit).into(),
        offset.into(),
        request.query.clone().into(),
        i64::from(request.snippet_chars).into(),
    ]);
    // MATCH, scalar extension calls, window ranks and CTE materialization are
    // missing from the installed Entity/SeaQuery search API. All data is bound.
    // Integer occurrence keys keep intermediate grouping compact; rank ties
    // still use the stable item ID, and pages return the public source IDs.
    let sql = format!("WITH eligible AS MATERIALIZED (
        SELECT i.id, i.workspace_id, i.thread_id, i.turn_id, i.item_id, i.embedding_artifact_id, i.rowid AS search_rowid {source_scope} AND i.status = 'active' AND i.search_text IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM thread_episodic_items newer WHERE newer.workspace_id = i.workspace_id
              AND newer.thread_id = i.thread_id AND newer.turn_id = i.turn_id AND newer.item_id = i.item_id
              AND newer.status = 'active' AND newer.deleted_at IS NULL AND newer.search_text IS NOT NULL
              AND (newer.updated_at > i.updated_at OR (newer.updated_at = i.updated_at AND newer.id > i.id)))
    ), lexical_candidates AS MATERIALIZED ({lexical}),
    eligible_artifact_ids AS MATERIALIZED (SELECT DISTINCT workspace_id, embedding_artifact_id FROM eligible),
    semantic_artifacts AS MATERIALIZED ({semantic_artifacts}),
    semantic_scores AS MATERIALIZED ({semantic}),
    semantic_candidates AS (SELECT e.search_rowid AS id, e.id AS sort_id, s.distance FROM eligible e JOIN semantic_scores s ON s.id = e.embedding_artifact_id),
    lexical_ranks AS (SELECT id, row_number() OVER (ORDER BY distance,sort_id) AS lexical_rank FROM lexical_candidates),
    semantic_ranks AS (SELECT id, row_number() OVER (ORDER BY distance,sort_id) AS semantic_rank FROM semantic_candidates WHERE distance IS NOT NULL),
    contributions AS (
        SELECT id, lexical_rank, NULL AS semantic_rank, 1.0 / (? + lexical_rank) AS contribution FROM lexical_ranks
        UNION ALL SELECT id, NULL, semantic_rank, 1.0 / (? + semantic_rank) FROM semantic_ranks
    ), fused AS (
        SELECT id, max(lexical_rank) AS lexical_rank, max(semantic_rank) AS semantic_rank, sum(contribution) AS score
        FROM contributions GROUP BY id
    ),
    ranked_page AS MATERIALIZED (
        SELECT e.id, c.lexical_rank, c.semantic_rank, c.score
        FROM fused c JOIN eligible e ON e.search_rowid = c.id
        ORDER BY c.score DESC, e.thread_id, e.turn_id, e.item_id, e.id LIMIT ? OFFSET ?
    )
    SELECT i.id, i.workspace_id, i.thread_id, i.turn_id, i.item_id, i.source_actor_role, i.source_runtime_kind, i.source_context_json, i.visibility, i.status, i.text_hash, i.source_text_hash, i.language_hint, i.token_estimate, i.indexed_at, i.created_at, i.updated_at, i.deleted_at, i.projection_group_id, i.embedding_artifact_id,
        substr(CASE WHEN p.lexical_rank IS NOT NULL THEN
            (SELECT snippet(thread_episodic_fts,0,'','','…',8) FROM thread_episodic_fts
             WHERE rowid=i.rowid AND thread_episodic_fts MATCH pioneer_fts_literal(?))
            ELSE i.search_text END,1,?) AS search_text,
        p.lexical_rank, p.semantic_rank
    FROM ranked_page p JOIN thread_episodic_items i ON i.id=p.id
    ORDER BY p.score DESC, i.thread_id, i.turn_id, i.item_id, i.id");
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql,
            values,
        ))
        .await?;
    let rows = rows
        .into_iter()
        .map(|row| {
            let model = thread_episodic_items::Model::from_query_result(&row, "")?;
            let text = model.search_text.clone().unwrap_or_default();
            Ok(ThreadEpisodicSearchRow {
                item: thread_episodic_item_record_from_model(model)?,
                text,
                lexical_rank: row.try_get("", "lexical_rank")?,
                semantic_rank: row.try_get("", "semantic_rank")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let incomplete_sources: bool = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            coverage_sql,
            coverage_values,
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("search coverage result missing"))?
        .try_get("", "incomplete")?;
    let projection_key =
        crate::thread_episodic::thread_episodic_refill_projection_key(&request.workspace_id)?;
    let marker = crate::find_projection_meta(db, &projection_key).await?;
    let coverage_incomplete = incomplete_sources
        || !marker.is_some_and(|marker| marker.status == crate::PROJECTION_META_STATUS_COMPLETE);
    Ok(ThreadEpisodicSearchResult {
        rows,
        coverage_incomplete,
    })
}

const SOURCE_FROM: &str = r#"FROM thread t
    CROSS JOIN thread_episodic_items i ON t.id = i.thread_id AND t.workspace_id = i.workspace_id
    LEFT JOIN thread_lineage lineage ON lineage.child_thread_id = t.id
    JOIN thread r ON r.id = CASE WHEN t.access_class = 'internal' THEN lineage.root_thread_id ELSE t.id END AND r.workspace_id = i.workspace_id
    JOIN workspace w ON w.id = i.workspace_id AND w.is_active = 1"#;
const SOURCE_ELIGIBILITY: &str = r#"AND i.deleted_at IS NULL
    AND i.visibility IN ('user_visible','parent_visible')
    AND i.source_context_json IN ('"user_visible_thread_item"','"user_visible_task_summary"','"thread_compaction_summary"')
    AND EXISTS (SELECT 1 FROM turn source WHERE source.id = i.turn_id AND source.thread_id = i.thread_id AND source.message_deleted_at IS NULL)
    AND NOT EXISTS (SELECT 1 FROM thread_episodic_exclusions x JOIN thread_episodic_items excluded ON excluded.id = x.index_item_id
        WHERE x.workspace_id = i.workspace_id AND x.thread_id = i.thread_id AND excluded.turn_id = i.turn_id AND excluded.item_id = i.item_id)"#;
