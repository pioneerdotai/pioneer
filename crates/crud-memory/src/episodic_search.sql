WITH scoped AS MATERIALIZED (
    SELECT i.id, i.workspace_id, i.thread_id, i.turn_id, i.item_id, i.status,
           i.search_text IS NOT NULL AS has_text, i.embedding_artifact_id,
           COALESCE(NULLIF(btrim(i.projection_group_id), ''), i.id) AS group_id
    FROM thread_episodic_items i
    JOIN thread t ON t.id = i.thread_id AND t.workspace_id = i.workspace_id
    LEFT JOIN thread_lineage lineage ON lineage.child_thread_id = t.id
    JOIN thread r ON r.id = CASE WHEN t.access_class = 'internal'
                                 THEN lineage.root_thread_id ELSE t.id END
      AND r.workspace_id = i.workspace_id
    JOIN workspace w ON w.id = i.workspace_id AND w.is_active
    JOIN turn source ON source.id = i.turn_id AND source.thread_id = i.thread_id
      AND source.message_deleted_at IS NULL
    WHERE i.workspace_id = $1
      AND ($2::text IS NULL OR i.thread_id = $2)
      AND ($3::text IS NULL OR r.id = $3)
      AND ($4 OR i.thread_id = r.id OR i.thread_id = $5)
      AND ($6::text IS NULL OR (
          EXISTS (SELECT 1 FROM workspace_membership wm
                  WHERE wm.workspace_id = i.workspace_id AND wm.principal_id = $6)
          AND (r.access_class = 'workspace' OR (r.access_class = 'private' AND
               EXISTS (SELECT 1 FROM thread_membership tm
                       WHERE tm.thread_id = r.id AND tm.principal_id = $6)))
      ))
      AND ($7::bigint IS NULL OR i.created_at >= to_timestamp($7::double precision))
      AND i.deleted_at IS NULL
      AND i.visibility IN ('user_visible','parent_visible')
      AND (i.source_context_json #>> '{}') IN (
          'user_visible_thread_item','user_visible_task_summary','thread_compaction_summary'
      )
      AND NOT EXISTS (
          SELECT 1 FROM thread_episodic_exclusions x
          JOIN thread_episodic_items excluded ON excluded.id = x.index_item_id
          WHERE x.workspace_id = i.workspace_id AND x.thread_id = i.thread_id
            AND excluded.turn_id = i.turn_id AND excluded.item_id = i.item_id
      )
), eligible AS MATERIALIZED (
    SELECT s.* FROM scoped s
    WHERE s.status = 'active' AND s.has_text
      AND NOT EXISTS (
          SELECT 1 FROM thread_episodic_items n, thread_episodic_items i
          WHERE i.id = s.id AND n.workspace_id = s.workspace_id
            AND n.thread_id = s.thread_id AND n.turn_id = s.turn_id AND n.item_id = s.item_id
            AND n.status = 'active' AND n.deleted_at IS NULL AND n.search_text IS NOT NULL
            AND (n.updated_at, n.id COLLATE "C") > (i.updated_at, i.id COLLATE "C")
      )
), lexical AS (
    SELECT i.id,
           row_number() OVER (ORDER BY hits.count DESC, i.id COLLATE "C") AS rank
    FROM thread_episodic_items i JOIN eligible e ON e.id = i.id
    CROSS JOIN LATERAL (
        SELECT count(*) FROM (
            SELECT unnest(thread_episodic_search_terms(i.search_text))
            INTERSECT
            SELECT unnest(thread_episodic_search_terms($9))
        ) terms
    ) hits
    WHERE $8
      AND thread_episodic_search_keys(i.search_text) && thread_episodic_search_keys($9)
      AND thread_episodic_search_terms(i.search_text) && thread_episodic_search_terms($9)
), artifacts AS MATERIALIZED (
    SELECT DISTINCT a.id, a.workspace_id
    FROM scoped s JOIN thread_episodic_embedding_artifacts a
      ON a.id = s.embedding_artifact_id AND a.workspace_id = s.workspace_id
    WHERE a.provider_id = $10 AND a.model = $11 AND a.dimension = $12 AND a.normalized = $13
), artifact_scores AS MATERIALIZED (
    SELECT a.id, a.embedding <=> $14::vector AS distance
    FROM artifacts ids JOIN thread_episodic_embedding_artifacts a ON a.id = ids.id
), semantic AS (
    SELECT e.id,
           row_number() OVER (ORDER BY a.distance, e.id COLLATE "C") AS rank
    FROM eligible e JOIN artifact_scores a ON a.id = e.embedding_artifact_id
), fused AS (
    SELECT COALESCE(l.id, s.id) AS id, l.rank AS lexical_rank, s.rank AS semantic_rank,
           COALESCE(1.0 / ($15::double precision + l.rank), 0)
           + COALESCE(1.0 / ($15::double precision + s.rank), 0) AS score
    FROM lexical l FULL JOIN semantic s ON l.id = s.id
), grouped AS (
    SELECT f.*, e.thread_id, e.turn_id, e.item_id,
           row_number() OVER (
               PARTITION BY e.group_id
               ORDER BY f.score DESC, e.thread_id COLLATE "C", e.turn_id COLLATE "C",
                        e.item_id COLLATE "C", e.id COLLATE "C"
           ) AS group_position
    FROM fused f JOIN eligible e ON e.id = f.id
), page AS MATERIALIZED (
    SELECT id, lexical_rank, semantic_rank, score, thread_id, turn_id, item_id
    FROM grouped WHERE group_position = 1
    ORDER BY score DESC, thread_id COLLATE "C", turn_id COLLATE "C",
             item_id COLLATE "C", id COLLATE "C"
    LIMIT $16 OFFSET $17
), coverage AS (
    SELECT EXISTS (
        SELECT 1 FROM scoped s
        WHERE s.status IN ('pending_index', 'failed')
           OR (s.status = 'active' AND (
               NOT s.has_text OR ($14 IS NOT NULL AND NOT EXISTS (
                   SELECT 1 FROM artifacts a
                   WHERE a.id = s.embedding_artifact_id AND a.workspace_id = s.workspace_id
               ))
           ))
    ) AS incomplete
)
SELECT i.id, i.workspace_id, i.thread_id, i.turn_id, i.item_id, i.source_actor_role,
       i.source_runtime_kind, i.source_context_json, i.visibility, i.status,
       i.text_hash, i.source_text_hash, i.language_hint, i.token_estimate,
       i.indexed_at, i.created_at, i.updated_at, i.deleted_at, i.projection_group_id,
       i.embedding_artifact_id, left(i.search_text, $18) AS search_text,
       p.lexical_rank, p.semantic_rank, coverage.incomplete
FROM coverage LEFT JOIN page p ON true LEFT JOIN thread_episodic_items i ON i.id = p.id
ORDER BY p.score DESC, p.thread_id COLLATE "C", p.turn_id COLLATE "C",
         p.item_id COLLATE "C", p.id COLLATE "C"
