-- Proposal 44 release-candidate checks.
-- Run against a dev/prod database after turn/start traffic has occurred.

-- Turns that should have a resolved execution security snapshot but do not.
SELECT
  id AS turn_id,
  thread_id,
  status,
  permission_profile_mode,
  created_at,
  updated_at
FROM "turn"
WHERE execution_security_snapshot_json IS NULL
  AND status IN ('in_progress', 'completed', 'failed', 'cancelled')
ORDER BY updated_at DESC
LIMIT 100;

-- Permission/security audit events that should explain a decision but are not
-- linked to a persisted security snapshot id/version.
SELECT
  turn_id,
  event_type,
  json_extract(payload, '$.eventKind') AS event_kind,
  json_extract(payload, '$.securitySnapshotId') AS security_snapshot_id,
  json_extract(payload, '$.securitySnapshotVersion') AS security_snapshot_version,
  created_at
FROM turn_event
WHERE event_type = 'turn/permission/audit'
  AND json_extract(payload, '$.eventKind') IN (
    'decision_allowed',
    'decision_denied',
    'security_snapshot_resolved',
    'security_sandbox_degraded',
    'security_sandbox_unavailable'
  )
  AND (
    json_extract(payload, '$.securitySnapshotId') IS NULL
    OR json_extract(payload, '$.securitySnapshotVersion') IS NULL
  )
ORDER BY created_at DESC
LIMIT 100;
