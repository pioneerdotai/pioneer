INSERT INTO workspace (id, name, is_active, is_current) VALUES ('ws', 'fixture', TRUE, FALSE);
INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status, origin_kind, access_class, created_at, updated_at)
VALUES ('root-thread', 'ws', '', 'agent', 'm', 'p', 'active', 'user', 'workspace', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
INSERT INTO turn (id, thread_id, status, turn_kind, origin, created_at, updated_at)
VALUES ('root-turn', 'root-thread', 'completed', 'conversation', 'user', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
INSERT INTO turn_event (id, thread_id, turn_id, sequence, event_type, payload, created_at)
SELECT 'topology-plan-source-' || i, 'root-thread', 'root-turn', 10000 + i, 'fixture', '{}', CURRENT_TIMESTAMP
FROM generate_series(0, 129) AS s(i);
INSERT INTO compaction_context (workspace_id, thread_id, owner, format_version)
VALUES ('ws', 'root-thread', 'root-owner', 1);
INSERT INTO compaction_operation (id, owner, fingerprint, status, snapshot, deadline_ms)
VALUES ('topology-plan', 'root-owner', 'topology-plan', 'running', '{}', 900000);
INSERT INTO compaction_manifest (operation_id, ordinal, unit_ordinal, reference_only, source_thread, source_scope, source_id, source_version)
SELECT 'topology-plan', i, i, 0, 'root-thread', 'event:root-turn', 'topology-plan-source-' || i, 'event-revision:1'
FROM generate_series(0, 129) AS s(i);
INSERT INTO compaction_checkpoint (id, operation_id, owner, portion, summary, identity_sha256, selection, projection_version, format_version, status)
VALUES ('topology-plan-checkpoint', 'topology-plan', 'root-owner', 0, 'prepared summary', 'fixture-identity', '{}', 0, 1, 'candidate');
INSERT INTO compaction_coverage (checkpoint_id, source_scope, source_id, source_version)
SELECT 'topology-plan-checkpoint', 'event:root-turn', 'topology-plan-source-' || i, 'event-revision:1'
FROM generate_series(0, 129) AS s(i);
