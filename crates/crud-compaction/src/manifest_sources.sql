WITH RECURSIVE
current_operation AS (
 SELECT o.id,o.owner,o.expected_head,o.snapshot,c.workspace_id,c.thread_id
 FROM compaction_operation o JOIN compaction_context c ON c.owner=o.owner
 WHERE o.id=$1
),
current_sources(source_scope,source_id,source_version,thread_id,workspace_id) AS (
 SELECT 'context:'||context_revision.turn_id,context_revision.source_id,
  'revision:'||context_revision.revision,context_turn.thread_id,context_thread.workspace_id
 FROM compaction_manifest need JOIN current_operation o ON o.id=need.operation_id
 JOIN compaction_source_revision context_revision ON context_revision.source_id=need.source_id
 JOIN turn_llm_context context_source ON context_source.id=context_revision.source_id
  AND context_source.turn_id=context_revision.turn_id
 JOIN turn context_turn ON context_turn.id=context_revision.turn_id
 JOIN thread context_thread ON context_thread.id=context_turn.thread_id
 WHERE context_revision.present=1 AND 'context:'||context_revision.turn_id=need.source_scope
  AND 'revision:'||context_revision.revision=need.source_version
  AND context_thread.workspace_id=o.workspace_id
 UNION ALL
 SELECT 'item:'||item_revision.turn_id,item_revision.source_id,
  'item-revision:'||item_revision.revision,item_turn.thread_id,item_thread.workspace_id
 FROM compaction_manifest need JOIN current_operation o ON o.id=need.operation_id
 JOIN compaction_item_revision item_revision ON item_revision.source_id=need.source_id
 JOIN turn_item item_source ON item_source.id=item_revision.source_id
  AND item_source.turn_id=item_revision.turn_id
 JOIN turn item_turn ON item_turn.id=item_revision.turn_id
 JOIN thread item_thread ON item_thread.id=item_turn.thread_id
 WHERE item_revision.present=1 AND 'item:'||item_revision.turn_id=need.source_scope
  AND 'item-revision:'||item_revision.revision=need.source_version
  AND item_thread.workspace_id=o.workspace_id
 UNION ALL
 SELECT 'event:'||event_revision.turn_id,event_revision.source_id,
  'event-revision:'||event_revision.revision,event_turn.thread_id,event_thread.workspace_id
 FROM compaction_manifest need JOIN current_operation o ON o.id=need.operation_id
 JOIN compaction_event_revision event_revision ON event_revision.source_id=need.source_id
 JOIN turn_event event_source ON event_source.id=event_revision.source_id
  AND event_source.turn_id=event_revision.turn_id
 JOIN turn event_turn ON event_turn.id=event_revision.turn_id
 JOIN thread event_thread ON event_thread.id=event_turn.thread_id
 WHERE event_revision.present=1 AND 'event:'||event_revision.turn_id=need.source_scope
  AND 'event-revision:'||event_revision.revision=need.source_version
  AND event_thread.workspace_id=o.workspace_id
 UNION ALL
 SELECT 'input:'||input_revision.turn_id,input_revision.source_id,
  'input-revision:'||input_revision.revision,input_turn.thread_id,input_thread.workspace_id
 FROM compaction_manifest need JOIN current_operation o ON o.id=need.operation_id
 JOIN compaction_input_revision input_revision ON input_revision.source_id=need.source_id
 JOIN turn_input input_source ON input_source.id=input_revision.source_id
  AND input_source.turn_id=input_revision.turn_id
 JOIN turn input_turn ON input_turn.id=input_revision.turn_id
 JOIN thread input_thread ON input_thread.id=input_turn.thread_id
 WHERE input_revision.present=1 AND 'input:'||input_revision.turn_id=need.source_scope
  AND 'input-revision:'||input_revision.revision=need.source_version
  AND input_thread.workspace_id=o.workspace_id
 UNION ALL
 SELECT 'checkpoint:'||checkpoint_source.owner,checkpoint_source.id,
  checkpoint_source.identity_sha256,checkpoint_context.thread_id,checkpoint_context.workspace_id
 FROM compaction_manifest need JOIN current_operation o ON o.id=need.operation_id
 JOIN compaction_checkpoint checkpoint_source ON checkpoint_source.id=need.source_id
 JOIN compaction_context checkpoint_context ON checkpoint_context.owner=checkpoint_source.owner
 WHERE 'checkpoint:'||checkpoint_source.owner=need.source_scope
  AND checkpoint_source.identity_sha256=need.source_version
  AND checkpoint_source.format_version=1
  AND checkpoint_context.workspace_id=o.workspace_id
  AND (checkpoint_source.status='applied' OR (checkpoint_source.status='retained' AND EXISTS(
   SELECT 1 FROM compaction_operation committed
   WHERE committed.id=checkpoint_source.operation_id AND committed.status='completed'
  )))
 UNION ALL
 SELECT 'task-basis:'||current_basis.run_id,current_basis.run_id,
  'task-basis-revision:'||COALESCE(basis_revision.revision,1),
  current_basis.conversation_thread_id,current_basis.workspace_id
 FROM compaction_manifest need JOIN current_operation o ON o.id=need.operation_id
 JOIN LATERAL (
  SELECT run_id, conversation_thread_id, workspace_id, history_json_has_array_prefix
  FROM task_run_conversation_snapshot basis_source
  WHERE basis_source.run_id=need.source_id COLLATE "default"
  LIMIT 1
 ) current_basis ON true
 JOIN thread basis_thread ON basis_thread.id=current_basis.conversation_thread_id
  AND basis_thread.workspace_id=current_basis.workspace_id
 LEFT JOIN compaction_task_basis_revision basis_revision ON basis_revision.run_id=current_basis.run_id
 WHERE current_basis.history_json_has_array_prefix
  AND 'task-basis:'||current_basis.run_id=need.source_scope
  AND 'task-basis-revision:'||COALESCE(basis_revision.revision,1)=need.source_version
  AND current_basis.workspace_id=o.workspace_id
),
accepted_imports AS MATERIALIZED (
 SELECT i.source_scope, i.source_id,
  CASE WHEN i.bytes BETWEEN 0 AND 262144
    AND octet_length(i.proof_json)=i.bytes AND i.proof_json IS JSON OBJECT
   THEN CASE WHEN
    (i.proof_json::json #>> '{source,scope}')=i.source_scope
    AND (i.proof_json::json #>> '{source,id}')=i.source_id
    AND (i.proof_json::json #>> '{source,version}')=i.source_version
    AND (i.proof_json::json #>> '{source_thread}')=i.source_thread
    AND (i.proof_json::json #>> '{preview_archive,source,scope}')=i.source_scope
    AND (i.proof_json::json #>> '{preview_archive,source,id}')=i.source_id
    AND EXISTS (
     SELECT 1 FROM html_preview_frozen_result receipt
     JOIN compaction_frozen_history output ON output.id=receipt.manifest_id
     JOIN compaction_source_revision revision ON revision.source_id=receipt.source_id
     WHERE receipt.manifest_id=(i.proof_json::json #>> '{output_manifest}')
      AND receipt.ordinal::text=(i.proof_json::json #>> '{output_ordinal}')
      AND receipt.source_id=i.source_id AND receipt.source_version=i.source_version
      AND i.source_scope='context:'||receipt.turn_id
      AND output.workspace_id=o.workspace_id AND output.owner_thread=i.source_thread
      AND output.ready=1 AND output.expired=0
      AND receipt.ordinal>=0 AND receipt.ordinal<output.message_count
      AND output.next_ordinal=output.message_count AND output.import_count>=0
      AND output.next_import=output.import_count
      AND receipt.archive_wire_sha256=(i.proof_json::json #>> '{preview_archive,wire_sha256}')
      AND receipt.archive_wire_sha256<>(CASE WHEN receipt.reference_json IS JSON OBJECT THEN receipt.reference_json::json ->> 'wire_sha256' END)
      AND revision.present=1 AND revision.turn_id=receipt.turn_id
      AND i.source_version='revision:'||(revision.revision-1)
      AND (i.proof_json::json #>> '{preview_archive,source,version}')='revision:'||revision.revision
    ) THEN (i.proof_json::json #>> '{preview_archive,source,version}')
    ELSE i.source_version END
   ELSE i.source_version END AS source_version,
  i.source_thread
 FROM current_operation o
 JOIN compaction_operation_projection p ON p.operation_id=o.id
 JOIN compaction_frozen_history h ON h.id=p.manifest_id
  AND h.workspace_id=o.workspace_id AND h.ready=1 AND h.expired=0
  AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count
  AND h.identity_sha256=p.identity_sha256 AND h.imports_sha256=p.imports_sha256
  AND h.import_count=p.import_count AND h.next_import=p.import_count
 -- Seek the import-ordinal range of this one bound manifest before applying
 -- proof predicates. The limit is the already-validated size of that range.
 JOIN LATERAL (
  SELECT source_scope, source_id, source_version, source_thread, proof_json, bytes
  FROM compaction_frozen_import entries
  WHERE entries.manifest_id=h.id
   AND entries.ordinal>=0 AND entries.ordinal<h.import_count
  ORDER BY entries.ordinal
  LIMIT h.import_count
 ) i ON true
),
accepted_basis_messages AS MATERIALIZED (
 SELECT CASE WHEN f.reference_json IS JSON OBJECT
  THEN f.reference_json::json ELSE '{}'::json END AS document
 FROM current_operation o
 JOIN compaction_operation_projection p ON p.operation_id=o.id
 JOIN compaction_frozen_history h ON h.id=p.manifest_id
  AND h.workspace_id=o.workspace_id AND h.ready=1 AND h.expired=0
  AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count
  AND h.identity_sha256=p.identity_sha256 AND h.imports_sha256=p.imports_sha256
  AND h.import_count=p.import_count AND h.next_import=p.import_count
 JOIN compaction_frozen_message f ON f.manifest_id=h.id AND f.ordinal>=0 AND f.ordinal<h.message_count
  AND f.manifest_id=(SELECT manifest_id FROM compaction_operation_projection WHERE operation_id=$1)
 LIMIT 65537
),
accepted_basis AS (
 SELECT (f.document #>> '{source_thread}') AS source_thread,
  (j.value #>> '{scope}') AS source_scope,
  (j.value #>> '{id}') AS source_id,
  (j.value #>> '{version}') AS source_version
 FROM accepted_basis_messages f CROSS JOIN LATERAL json_array_elements(
  CASE WHEN json_typeof(f.document -> 'sources')='array'
   THEN f.document -> 'sources' ELSE '[]'::json END
 ) AS j(value)
 WHERE (f.document #>> '{inherited}')='true'
 LIMIT 65537
),
basis_within_bound AS (
 SELECT COUNT(*)<65537 AND (SELECT COUNT(*) FROM accepted_basis_messages)<65537 AS valid
 FROM accepted_basis
),
accepted_basis_roots AS (
 SELECT source_scope AS root_scope, source_id AS root_id, source_version AS root_version,
  source_thread AS root_thread
 FROM accepted_basis, basis_within_bound
 WHERE valid AND source_scope LIKE 'checkpoint:%'
),
basis_walk(root_scope,root_id,root_version,root_thread,source_scope,source_id,source_version,source_thread) AS (
 SELECT root_scope,root_id,root_version,root_thread,
  root_scope COLLATE "C",root_id COLLATE "C",root_version COLLATE "C",root_thread COLLATE "C"
 FROM accepted_basis_roots
 UNION
 SELECT g.root_scope,g.root_id,g.root_version,g.root_thread,
  edge.source_scope,edge.source_id,edge.source_version,edge.source_thread
 FROM basis_walk g
 JOIN compaction_checkpoint node ON node.id=g.source_id
  AND 'checkpoint:'||node.owner=g.source_scope AND node.identity_sha256=g.source_version
 CROSS JOIN LATERAL (
  SELECT v.source_scope,v.source_id,v.source_version,m.source_thread
  FROM compaction_coverage v
  JOIN compaction_manifest m ON m.operation_id=node.operation_id
   AND m.source_scope=v.source_scope AND m.source_id=v.source_id
   AND m.source_version=v.source_version AND m.reference_only=0
  WHERE v.checkpoint_id=node.id
  UNION ALL
  SELECT 'checkpoint:'||previous.owner,previous.id,previous.identity_sha256,previous_context.thread_id
  FROM compaction_checkpoint previous
  JOIN compaction_context previous_context ON previous_context.owner=previous.owner
   AND previous_context.thread_id=g.source_thread
  WHERE previous.id=node.previous AND previous.owner=node.owner AND previous.format_version=1
 ) AS edge(source_scope,source_id,source_version,source_thread)
 WHERE g.source_scope LIKE 'checkpoint:%'
),
basis_coverage AS MATERIALIZED (
 SELECT * FROM basis_walk LIMIT 65537
),
complete_basis_roots AS (
 SELECT r.root_scope,r.root_id,r.root_version,r.root_thread
 FROM accepted_basis_roots r,current_operation o
 WHERE (SELECT COUNT(*) FROM basis_coverage)<65537
  AND EXISTS (
   SELECT 1 FROM compaction_checkpoint root JOIN compaction_context context ON context.owner=root.owner
   WHERE root.id=r.root_id AND 'checkpoint:'||root.owner=r.root_scope
    AND root.identity_sha256=r.root_version AND root.format_version=1
    AND context.thread_id=r.root_thread AND context.workspace_id=o.workspace_id
    AND (root.status='applied' OR (root.status='retained' AND EXISTS (
     SELECT 1 FROM compaction_operation committed
     WHERE committed.id=root.operation_id AND committed.status='completed'
    )))
  )
  AND EXISTS (SELECT 1 FROM basis_coverage g
   WHERE g.root_scope=r.root_scope AND g.root_id=r.root_id
    AND g.root_version=r.root_version AND g.root_thread=r.root_thread
    AND g.source_scope NOT LIKE 'checkpoint:%')
  AND NOT EXISTS (
   SELECT 1 FROM basis_coverage g
   WHERE g.root_scope=r.root_scope AND g.root_id=r.root_id
    AND g.root_version=r.root_version AND g.root_thread=r.root_thread
    AND g.source_scope LIKE 'checkpoint:%' AND NOT EXISTS (
     SELECT 1 FROM compaction_checkpoint c JOIN compaction_context context ON context.owner=c.owner
     WHERE c.id=g.source_id AND 'checkpoint:'||c.owner=g.source_scope
      AND c.identity_sha256=g.source_version AND c.format_version=1
      AND context.thread_id=g.source_thread AND context.workspace_id=o.workspace_id
    )
  )
  AND NOT EXISTS (
   SELECT 1 FROM basis_coverage g
   JOIN compaction_checkpoint node ON node.id=g.source_id
    AND 'checkpoint:'||node.owner=g.source_scope AND node.identity_sha256=g.source_version
   WHERE g.root_scope=r.root_scope AND g.root_id=r.root_id
    AND g.root_version=r.root_version AND g.root_thread=r.root_thread
    AND g.source_scope LIKE 'checkpoint:%' AND (
     EXISTS (
      SELECT 1 FROM compaction_coverage v
      WHERE v.checkpoint_id=node.id AND (
       SELECT COUNT(DISTINCT ownership.source_thread)
       FROM compaction_manifest ownership
       WHERE ownership.operation_id=node.operation_id AND ownership.reference_only=0
        AND ownership.source_scope=v.source_scope AND ownership.source_id=v.source_id
        AND ownership.source_version=v.source_version
      )<>1
     )
     OR (node.previous IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM compaction_checkpoint previous
      JOIN compaction_context previous_context ON previous_context.owner=previous.owner
      WHERE previous.id=node.previous AND previous.owner=node.owner
       AND previous.format_version=1 AND previous_context.workspace_id=o.workspace_id
       AND previous_context.thread_id=g.source_thread
     ))
    )
  )
),
accepted_basis_atoms AS (
 SELECT source_scope, source_id, source_version, source_thread
 FROM accepted_basis, basis_within_bound
 WHERE valid AND source_scope NOT LIKE 'checkpoint:%'
 UNION
 SELECT g.source_scope,g.source_id,g.source_version,g.source_thread
 FROM basis_coverage g
 JOIN complete_basis_roots r ON r.root_scope=g.root_scope AND r.root_id=g.root_id
  AND r.root_version=g.root_version AND r.root_thread=g.root_thread
 WHERE g.source_scope NOT LIKE 'checkpoint:%'
),
basis_input_claims AS MATERIALIZED (
 SELECT f.document AS carrier,
  (a.value #>> '{represented_thread}') AS represented_thread,
  (a.value #>> '{represented_source,scope}') AS represented_scope,
  (a.value #>> '{represented_source,id}') AS represented_id,
  (a.value #>> '{represented_source,version}') AS represented_version,
  (a.value #>> '{source_thread}') AS source_thread,
  (a.value #>> '{source,scope}') AS source_scope,
  (a.value #>> '{source,id}') AS source_id,
  (a.value #>> '{source,version}') AS source_version
 FROM accepted_basis_messages f CROSS JOIN LATERAL json_array_elements(
  CASE WHEN json_typeof(f.document -> 'source_aliases')='array'
   THEN f.document -> 'source_aliases' ELSE '[]'::json END
 ) AS a(value)
 LIMIT 65537
),
basis_input_conflicts AS MATERIALIZED (
 SELECT (a.value #>> '{source_thread}') AS source_thread,
  (a.value #>> '{source,scope}') AS source_scope,
  (a.value #>> '{source,id}') AS source_id,
  (a.value #>> '{source,version}') AS source_version
 FROM accepted_basis_messages f CROSS JOIN LATERAL json_array_elements(
  CASE WHEN json_typeof(f.document -> 'ambiguous_input_aliases')='array'
   THEN f.document -> 'ambiguous_input_aliases' ELSE '[]'::json END
 ) AS a(value)
 LIMIT 65537
),
accepted_basis_input_aliases AS (
 SELECT a.source_thread,a.source_scope,a.source_id,a.source_version
 FROM basis_input_claims a,basis_within_bound bound
 WHERE bound.valid AND (SELECT COUNT(*) FROM basis_input_claims)<65537
  AND (SELECT COUNT(*) FROM basis_input_conflicts)<65537
  AND (a.carrier #>> '{inherited}')='true'
  AND (a.carrier #>> '{complete}')='true'
  AND (a.carrier #>> '{protected_input}')='false'
  AND a.source_scope LIKE 'input:%' AND a.represented_scope LIKE 'input:%'
  AND (a.source_thread<>a.represented_thread OR a.source_scope<>a.represented_scope
   OR a.source_id<>a.represented_id OR a.source_version<>a.represented_version)
  AND EXISTS (
   SELECT 1 FROM json_array_elements(
  CASE WHEN json_typeof(a.carrier -> 'sources')='array'
   THEN a.carrier -> 'sources' ELSE '[]'::json END
 ) AS s(value)
   WHERE ((a.carrier #>> '{source_thread}')=a.represented_thread
    AND (s.value #>> '{scope}')=a.represented_scope
    AND (s.value #>> '{id}')=a.represented_id
    AND (s.value #>> '{version}')=a.represented_version)
   OR EXISTS (
    SELECT 1 FROM basis_coverage g JOIN complete_basis_roots r
     ON r.root_scope=g.root_scope AND r.root_id=g.root_id
      AND r.root_version=g.root_version AND r.root_thread=g.root_thread
    WHERE g.root_scope=(s.value #>> '{scope}')
     AND g.root_id=(s.value #>> '{id}')
     AND g.root_version=(s.value #>> '{version}')
     AND g.root_thread=(a.carrier #>> '{source_thread}')
     AND g.source_scope=a.represented_scope AND g.source_id=a.represented_id
     AND g.source_version=a.represented_version AND g.source_thread=a.represented_thread
   )
  )
  AND NOT EXISTS (SELECT 1 FROM basis_input_conflicts c
   WHERE c.source_thread=a.source_thread AND c.source_scope=a.source_scope
    AND c.source_id=a.source_id AND c.source_version=a.source_version)
  AND NOT EXISTS (SELECT 1 FROM basis_input_claims other
   WHERE other.source_thread=a.source_thread AND other.source_scope=a.source_scope
    AND other.source_id=a.source_id AND other.source_version=a.source_version
    AND (other.represented_thread<>a.represented_thread
     OR other.represented_scope<>a.represented_scope OR other.represented_id<>a.represented_id
     OR other.represented_version<>a.represented_version))
),
checkpoint_roots AS (
 SELECT m.ordinal,m.source_scope,m.source_id,m.source_version,m.source_thread,
  m.reference_only=0 AND m.source_thread<>o.thread_id
   AND NOT EXISTS (SELECT 1 FROM accepted_imports i
    WHERE i.source_scope=m.source_scope AND i.source_id=m.source_id
     AND i.source_version=m.source_version AND i.source_thread=m.source_thread) AS requires_grant
 FROM compaction_manifest m JOIN current_operation o ON o.id=m.operation_id
 WHERE m.source_scope LIKE 'checkpoint:%'
),
boundary_walk(root,root_scope,root_id,root_version,root_thread,source_scope,source_id,source_version,source_thread,terminal_grant) AS (
 SELECT ordinal,source_scope,source_id,source_version,source_thread,
  source_scope COLLATE "C",source_id COLLATE "C",source_version COLLATE "C",source_thread COLLATE "C",false
 FROM checkpoint_roots
 WHERE requires_grant
 UNION
 SELECT g.root,g.root_scope,g.root_id,g.root_version,g.root_thread,
  edge.source_scope,edge.source_id,edge.source_version,edge.source_thread,
  edge.source_scope LIKE 'checkpoint:%' AND (
   EXISTS (SELECT 1 FROM accepted_imports i
    WHERE i.source_scope=edge.source_scope AND i.source_id=edge.source_id
     AND i.source_version=edge.source_version AND i.source_thread=edge.source_thread)
   OR ((o.snapshot::json #>> '{plan,coverage_domain}')='working_context'
    AND EXISTS (SELECT 1 FROM accepted_basis b, basis_within_bound bound
     WHERE bound.valid AND b.source_scope=edge.source_scope AND b.source_id=edge.source_id
      AND b.source_version=edge.source_version AND b.source_thread=edge.source_thread))
  ) AS terminal_grant
 FROM boundary_walk g
 CROSS JOIN current_operation o
 JOIN compaction_checkpoint node ON node.id=g.source_id
  AND 'checkpoint:'||node.owner=g.source_scope AND node.identity_sha256=g.source_version
 CROSS JOIN LATERAL (
  SELECT v.source_scope,v.source_id,v.source_version,m.source_thread
  FROM compaction_coverage v
  JOIN compaction_manifest m ON m.operation_id=node.operation_id
   AND m.source_scope=v.source_scope AND m.source_id=v.source_id
   AND m.source_version=v.source_version AND m.reference_only=0
  WHERE v.checkpoint_id=node.id
  UNION ALL
  SELECT 'checkpoint:'||previous.owner,previous.id,previous.identity_sha256,previous_context.thread_id
  FROM compaction_checkpoint previous
  JOIN compaction_context previous_context ON previous_context.owner=previous.owner
   AND previous_context.thread_id=g.source_thread
  WHERE previous.id=node.previous AND previous.owner=node.owner AND previous.format_version=1
 ) AS edge(source_scope,source_id,source_version,source_thread)
 WHERE g.source_scope LIKE 'checkpoint:%' AND NOT g.terminal_grant
),
coverage AS MATERIALIZED (
 SELECT * FROM boundary_walk LIMIT 65537
),
complete_boundary_roots AS (
 SELECT r.ordinal FROM checkpoint_roots r,current_operation o
 WHERE (SELECT COUNT(*) FROM coverage)<65537
  AND EXISTS (SELECT 1 FROM coverage g WHERE g.root=r.ordinal
   AND (g.source_scope NOT LIKE 'checkpoint:%' OR g.terminal_grant))
  AND NOT EXISTS (
   SELECT 1 FROM coverage g WHERE g.root=r.ordinal
    AND g.source_scope LIKE 'checkpoint:%' AND NOT EXISTS (
     SELECT 1 FROM compaction_checkpoint c JOIN compaction_context context ON context.owner=c.owner
     WHERE c.id=g.source_id AND 'checkpoint:'||c.owner=g.source_scope
      AND c.identity_sha256=g.source_version AND c.format_version=1
      AND context.thread_id=g.source_thread AND context.workspace_id=o.workspace_id
      AND (NOT g.terminal_grant OR c.status='applied' OR (c.status='retained' AND EXISTS (
       SELECT 1 FROM compaction_operation committed
       WHERE committed.id=c.operation_id AND committed.status='completed'
      )))
    )
  )
  AND NOT EXISTS (
   SELECT 1 FROM coverage g
   JOIN compaction_checkpoint node ON node.id=g.source_id
    AND 'checkpoint:'||node.owner=g.source_scope AND node.identity_sha256=g.source_version
   WHERE g.root=r.ordinal AND g.source_scope LIKE 'checkpoint:%'
    AND NOT g.terminal_grant AND (
    EXISTS (
     SELECT 1 FROM compaction_coverage v
     WHERE v.checkpoint_id=node.id AND (
      SELECT COUNT(DISTINCT ownership.source_thread)
      FROM compaction_manifest ownership
      WHERE ownership.operation_id=node.operation_id AND ownership.reference_only=0
       AND ownership.source_scope=v.source_scope AND ownership.source_id=v.source_id
       AND ownership.source_version=v.source_version
     )<>1
    )
    OR (node.previous IS NOT NULL AND NOT EXISTS (
     SELECT 1 FROM compaction_checkpoint previous
     JOIN compaction_context previous_context ON previous_context.owner=previous.owner
     WHERE previous.id=node.previous AND previous.owner=node.owner
      AND previous.format_version=1 AND previous_context.workspace_id=o.workspace_id
      AND previous_context.thread_id=g.source_thread
    ))
   )
  )
),
valid_roots AS (
 SELECT r.ordinal FROM checkpoint_roots r, current_operation o
 WHERE r.requires_grant
  AND (
   EXISTS (SELECT 1 FROM accepted_imports i
    WHERE i.source_scope=r.source_scope AND i.source_id=r.source_id
     AND i.source_version=r.source_version AND i.source_thread=r.source_thread)
   OR ((o.snapshot::json #>> '{plan,coverage_domain}')='working_context'
    AND EXISTS (SELECT 1 FROM accepted_basis b, basis_within_bound bound
     WHERE bound.valid AND b.source_scope=r.source_scope AND b.source_id=r.source_id
      AND b.source_version=r.source_version AND b.source_thread=r.source_thread))
   OR (EXISTS (SELECT 1 FROM complete_boundary_roots c WHERE c.ordinal=r.ordinal)
    AND NOT EXISTS (
     SELECT 1 FROM coverage g WHERE g.root=r.ordinal
      AND g.source_scope NOT LIKE 'checkpoint:%' AND NOT (
       EXISTS (SELECT 1 FROM accepted_imports i
        WHERE i.source_scope=g.source_scope AND i.source_id=g.source_id
         AND i.source_version=g.source_version AND i.source_thread=g.source_thread)
       OR ((o.snapshot::json #>> '{plan,coverage_domain}')='working_context' AND (
        g.source_thread=o.thread_id
        OR EXISTS (SELECT 1 FROM accepted_basis_atoms b
         WHERE b.source_scope=g.source_scope AND b.source_id=g.source_id
          AND b.source_version=g.source_version AND b.source_thread=g.source_thread)
        OR EXISTS (SELECT 1 FROM accepted_basis_input_aliases a
         WHERE a.source_scope=g.source_scope AND a.source_id=g.source_id
          AND a.source_version=g.source_version AND a.source_thread=g.source_thread)
       ))
      )
    )
   )
  )
)
SELECT m.ordinal FROM compaction_manifest m JOIN current_operation o ON o.id=m.operation_id
WHERE
 EXISTS (SELECT 1 FROM json_each(
  CASE WHEN json_typeof(o.snapshot::json -> 'source_epochs')='object'
   THEN o.snapshot::json -> 'source_epochs' ELSE '{}'::json END
 ) wanted
  WHERE NOT EXISTS (SELECT 1 FROM thread t WHERE t.id=wanted.key AND t.workspace_id=o.workspace_id))
 OR NOT EXISTS (SELECT 1 FROM thread t WHERE t.id=o.thread_id AND t.workspace_id=o.workspace_id)
 OR (o.expected_head IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM compaction_checkpoint previous
  WHERE previous.id=o.expected_head AND previous.owner=o.owner AND previous.format_version=1
   AND (previous.status='applied' OR (previous.status='retained' AND EXISTS (
    SELECT 1 FROM compaction_operation committed
    WHERE committed.id=previous.operation_id AND committed.status='completed'
   )))
 ))
 OR NOT EXISTS (
  SELECT 1 FROM current_sources s
  WHERE s.source_scope=m.source_scope AND s.source_id=m.source_id AND s.source_version=m.source_version
   AND s.thread_id=m.source_thread AND s.workspace_id=o.workspace_id
   AND (m.reference_only=1 OR s.thread_id=o.thread_id
    OR EXISTS (SELECT 1 FROM accepted_imports i
     WHERE i.source_scope=s.source_scope AND i.source_id=s.source_id
      AND i.source_version=s.source_version AND i.source_thread=s.thread_id)
    OR ((o.snapshot::json #>> '{plan,coverage_domain}')='working_context'
     AND EXISTS (SELECT 1 FROM json_each(
  CASE WHEN json_typeof(o.snapshot::json -> 'source_epochs')='object'
   THEN o.snapshot::json -> 'source_epochs' ELSE '{}'::json END
 ) e WHERE e.key=s.thread_id)
     AND EXISTS (SELECT 1 FROM accepted_basis b, basis_within_bound bound
      WHERE bound.valid AND b.source_scope=s.source_scope AND b.source_id=s.source_id
       AND b.source_version=s.source_version AND b.source_thread=s.thread_id))
    OR EXISTS (SELECT 1 FROM valid_roots r WHERE r.ordinal=m.ordinal))
 )
LIMIT 1
