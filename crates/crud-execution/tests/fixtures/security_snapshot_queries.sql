-- Read-only checks after exercising turn/start and execution authorization
-- against an isolated PostgreSQL development database.

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
ORDER BY updated_at DESC, id ASC
LIMIT 100;

-- Audit event bodies with a payload locator are decoded by the application.
-- This page distinguishes those bodies from confirmed omissions in inline JSON.
-- The security_snapshot_audit_decision_persists_snapshot_linkage test checks
-- the snapshot ID and version after the application decodes a stored body.
SELECT
  id AS event_id,
  thread_id,
  turn_id,
  payload_payload_id,
  payload ->> 'eventKind' AS event_kind,
  payload ->> 'securitySnapshotId' AS security_snapshot_id,
  payload ->> 'securitySnapshotVersion' AS security_snapshot_version,
  created_at
FROM turn_event
WHERE event_type = 'turn/permission/audit'
  AND (
    payload_payload_id IS NOT NULL
    OR (
      payload ->> 'eventKind' IN (
        'decision_allowed',
        'decision_denied',
        'security_snapshot_resolved',
        'security_sandbox_degraded',
        'security_sandbox_unavailable'
      )
      AND (
        payload ->> 'securitySnapshotId' IS NULL
        OR payload ->> 'securitySnapshotVersion' IS NULL
      )
    )
  )
ORDER BY created_at DESC, id ASC
LIMIT 100;
