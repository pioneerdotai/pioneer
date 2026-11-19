-- Domain views retained from the SQLite catalog.

CREATE VIEW compaction_live_sources
  AS ((((SELECT 'context:' || r.turn_id AS source_scope
              , r.source_id
              , 'revision:' || r.revision AS source_version
              , r.turn_id
              , t.thread_id
              , th.workspace_id
         FROM compaction_source_revision AS r
              INNER JOIN turn_llm_context AS s ON s.id = r.source_id
                                              AND s.turn_id = r.turn_id
              INNER JOIN turn AS t ON t.id = r.turn_id
              INNER JOIN thread AS th ON th.id = t.thread_id
         WHERE r.present = 1

         UNION ALL

         SELECT 'item:' || r.turn_id AS source_scope
              , r.source_id
              , 'item-revision:' || r.revision AS source_version
              , r.turn_id
              , t.thread_id
              , th.workspace_id
         FROM compaction_item_revision AS r
              INNER JOIN turn_item AS s ON s.id = r.source_id
                                       AND s.turn_id = r.turn_id
              INNER JOIN turn AS t ON t.id = r.turn_id
              INNER JOIN thread AS th ON th.id = t.thread_id
         WHERE r.present = 1)

        UNION ALL

        SELECT 'event:' || r.turn_id AS source_scope
             , r.source_id
             , 'event-revision:' || r.revision AS source_version
             , r.turn_id
             , t.thread_id
             , th.workspace_id
        FROM compaction_event_revision AS r
             INNER JOIN turn_event AS s ON s.id = r.source_id
                                       AND s.turn_id = r.turn_id
             INNER JOIN turn AS t ON t.id = r.turn_id
             INNER JOIN thread AS th ON th.id = t.thread_id
        WHERE r.present = 1)

       UNION ALL

       SELECT 'input:' || r.turn_id
            , r.source_id
            , 'input-revision:' || r.revision
            , r.turn_id
            , t.thread_id
            , th.workspace_id
       FROM compaction_input_revision AS r
            INNER JOIN turn_input AS s ON s.id = r.source_id
                                      AND s.turn_id = r.turn_id
            INNER JOIN turn AS t ON t.id = r.turn_id
            INNER JOIN thread AS th ON th.id = t.thread_id
       WHERE r.present = 1)

      UNION ALL

      SELECT 'checkpoint:' || p.owner
           , p.id
           , p.identity_sha256
           , NULL
           , c.thread_id
           , c.workspace_id
      FROM compaction_checkpoint AS p
           INNER JOIN compaction_context AS c ON c.owner = p.owner
      WHERE p.format_version = 1
        AND (p.status = 'applied'
          OR (p.status = 'retained'
          AND EXISTS (SELECT 1
                      FROM compaction_operation AS committed
                      WHERE committed.id = p.operation_id
                        AND committed.status = 'completed'))))

     UNION ALL

     SELECT 'task-basis:' || s.run_id
          , s.run_id
          , 'task-basis-revision:' || COALESCE(r.revision, 1)
          , NULL
          , s.conversation_thread_id
          , s.workspace_id
     FROM task_run_conversation_snapshot AS s
          INNER JOIN thread AS th ON th.id = s.conversation_thread_id
                                 AND th.workspace_id = s.workspace_id
          LEFT JOIN compaction_task_basis_revision AS r ON r.run_id = s.run_id
     WHERE substr(ltrim(s.history_json)
                , 1
                , 1) = '[';

CREATE VIEW compaction_frozen_message
  AS SELECT d.manifest_id
          , d.ordinal
          , d.reference_json
          , d.bytes
     FROM compaction_frozen_message_data AS d
          INNER JOIN compaction_frozen_history AS h ON h.id = d.manifest_id
     WHERE h.expired = 0
       AND h.ready IN (0, 1)
       AND h.message_count >= 0
       AND h.import_count >= 0
       AND h.next_ordinal BETWEEN 0 AND h.message_count
       AND h.next_import BETWEEN 0 AND h.import_count
       AND (h.ready = 0
         OR (h.next_ordinal = h.message_count
         AND h.next_import = h.import_count))
       AND d.ordinal >= 0
       AND d.ordinal < CASE h.ready
                         WHEN 1
                           THEN h.message_count
                         ELSE h.next_ordinal
                       END
       AND NOT EXISTS (SELECT 1
                       FROM compaction_frozen_layout AS l
                       WHERE l.manifest_id = h.id
                         AND l.kind = 0
                         AND l.active = 1)

     UNION ALL

     SELECT s.manifest_id
          , d.ordinal
          , d.reference_json
          , d.bytes
     FROM compaction_frozen_span AS s
          INNER JOIN compaction_frozen_message_data AS d ON d.manifest_id = s.source_manifest
                                                        AND d.ordinal >= s.start
                                                        AND d.ordinal < s."end"
          INNER JOIN compaction_frozen_layout AS l ON l.manifest_id = s.manifest_id
                                                  AND l.kind = s.kind
          INNER JOIN compaction_frozen_history AS h ON h.id = s.manifest_id
     WHERE s.kind = 0
       AND l.active = 1
       AND h.expired = 0
       AND h.ready IN (0, 1)
       AND h.message_count >= 0
       AND h.import_count >= 0
       AND h.next_ordinal BETWEEN 0 AND h.message_count
       AND h.next_import BETWEEN 0 AND h.import_count
       AND (h.ready = 0
         OR (h.next_ordinal = h.message_count
         AND h.next_import = h.import_count))
       AND d.ordinal >= 0
       AND d.ordinal < CASE h.ready
                         WHEN 1
                           THEN h.message_count
                         ELSE h.next_ordinal
                       END;

CREATE VIEW compaction_frozen_import
  AS SELECT d.manifest_id
          , d.ordinal
          , d.message_ordinal
          , d.source_scope
          , d.source_id
          , d.source_version
          , d.source_thread
          , d.proof_json
          , d.bytes
     FROM compaction_frozen_import_data AS d
          INNER JOIN compaction_frozen_history AS h ON h.id = d.manifest_id
     WHERE h.expired = 0
       AND h.ready IN (0, 1)
       AND h.message_count >= 0
       AND h.import_count >= 0
       AND h.next_ordinal BETWEEN 0 AND h.message_count
       AND h.next_import BETWEEN 0 AND h.import_count
       AND (h.ready = 0
         OR (h.next_ordinal = h.message_count
         AND h.next_import = h.import_count))
       AND d.ordinal >= 0
       AND d.ordinal < CASE h.ready
                         WHEN 1
                           THEN h.import_count
                         ELSE h.next_import
                       END
       AND d.message_ordinal >= 0
       AND d.message_ordinal < CASE h.ready
                                 WHEN 1
                                   THEN h.message_count
                                 ELSE h.next_ordinal
                               END
       AND NOT EXISTS (SELECT 1
                       FROM compaction_frozen_layout AS l
                       WHERE l.manifest_id = h.id
                         AND l.kind = 1
                         AND l.active = 1)

     UNION ALL

     SELECT s.manifest_id
          , d.ordinal
          , d.message_ordinal
          , d.source_scope
          , d.source_id
          , d.source_version
          , d.source_thread
          , d.proof_json
          , d.bytes
     FROM compaction_frozen_span AS s
          INNER JOIN compaction_frozen_import_data AS d ON d.manifest_id = s.source_manifest
                                                       AND d.ordinal >= s.start
                                                       AND d.ordinal < s."end"
          INNER JOIN compaction_frozen_layout AS l ON l.manifest_id = s.manifest_id
                                                  AND l.kind = s.kind
          INNER JOIN compaction_frozen_history AS h ON h.id = s.manifest_id
     WHERE s.kind = 1
       AND l.active = 1
       AND h.expired = 0
       AND h.ready IN (0, 1)
       AND h.message_count >= 0
       AND h.import_count >= 0
       AND h.next_ordinal BETWEEN 0 AND h.message_count
       AND h.next_import BETWEEN 0 AND h.import_count
       AND (h.ready = 0
         OR (h.next_ordinal = h.message_count
         AND h.next_import = h.import_count))
       AND d.ordinal >= 0
       AND d.ordinal < CASE h.ready
                         WHEN 1
                           THEN h.import_count
                         ELSE h.next_import
                       END
       AND d.message_ordinal >= 0
       AND d.message_ordinal < CASE h.ready
                                 WHEN 1
                                   THEN h.message_count
                                 ELSE h.next_ordinal
                               END;
