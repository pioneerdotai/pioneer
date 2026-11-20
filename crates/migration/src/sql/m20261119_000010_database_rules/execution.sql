-- Actor-pair validation, terminal native-event cleanup, and read-model maintenance.

CREATE OR REPLACE FUNCTION "conversation_actor_thread_created_by_actor_kind_pair_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT (((NEW.created_by_actor_kind IS NULL AND NEW.created_by_actor_id IS NULL) OR (NEW.created_by_actor_kind IS NOT NULL AND ((NEW.created_by_actor_kind = 'system' AND NEW.created_by_actor_id IS NULL) OR (NEW.created_by_actor_kind = 'principal' AND NEW.created_by_actor_id IS NOT NULL AND length(NEW.created_by_actor_id) = 21 AND NEW.created_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C") OR (NEW.created_by_actor_kind = 'agent_execution' AND NEW.created_by_actor_id IS NOT NULL AND length(NEW.created_by_actor_id) = 21 AND NEW.created_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C")))))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'invalid persisted conversation actor pair';
END;
$trigger$;

CREATE TRIGGER "conversation_actor_thread_created_by_actor_kind_pair_insert" BEFORE INSERT
ON "thread" FOR EACH ROW EXECUTE FUNCTION "conversation_actor_thread_created_by_actor_kind_pair_insert"();

CREATE OR REPLACE FUNCTION "conversation_actor_thread_created_by_actor_kind_pair_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT (((NEW.created_by_actor_kind IS NULL AND NEW.created_by_actor_id IS NULL) OR (NEW.created_by_actor_kind IS NOT NULL AND ((NEW.created_by_actor_kind = 'system' AND NEW.created_by_actor_id IS NULL) OR (NEW.created_by_actor_kind = 'principal' AND NEW.created_by_actor_id IS NOT NULL AND length(NEW.created_by_actor_id) = 21 AND NEW.created_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C") OR (NEW.created_by_actor_kind = 'agent_execution' AND NEW.created_by_actor_id IS NOT NULL AND length(NEW.created_by_actor_id) = 21 AND NEW.created_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C")))))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'invalid persisted conversation actor pair';
END;
$trigger$;

CREATE TRIGGER "conversation_actor_thread_created_by_actor_kind_pair_update" BEFORE UPDATE
ON "thread" FOR EACH ROW EXECUTE FUNCTION "conversation_actor_thread_created_by_actor_kind_pair_update"();

CREATE OR REPLACE FUNCTION "conversation_actor_turn_initiated_by_actor_kind_pair_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT (((NEW.initiated_by_actor_kind IS NULL AND NEW.initiated_by_actor_id IS NULL) OR (NEW.initiated_by_actor_kind IS NOT NULL AND ((NEW.initiated_by_actor_kind = 'system' AND NEW.initiated_by_actor_id IS NULL) OR (NEW.initiated_by_actor_kind = 'principal' AND NEW.initiated_by_actor_id IS NOT NULL AND length(NEW.initiated_by_actor_id) = 21 AND NEW.initiated_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C") OR (NEW.initiated_by_actor_kind = 'agent_execution' AND NEW.initiated_by_actor_id IS NOT NULL AND length(NEW.initiated_by_actor_id) = 21 AND NEW.initiated_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C")))))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'invalid persisted conversation actor pair';
END;
$trigger$;

CREATE TRIGGER "conversation_actor_turn_initiated_by_actor_kind_pair_insert" BEFORE INSERT
ON "turn" FOR EACH ROW EXECUTE FUNCTION "conversation_actor_turn_initiated_by_actor_kind_pair_insert"();

CREATE OR REPLACE FUNCTION "conversation_actor_turn_initiated_by_actor_kind_pair_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT (((NEW.initiated_by_actor_kind IS NULL AND NEW.initiated_by_actor_id IS NULL) OR (NEW.initiated_by_actor_kind IS NOT NULL AND ((NEW.initiated_by_actor_kind = 'system' AND NEW.initiated_by_actor_id IS NULL) OR (NEW.initiated_by_actor_kind = 'principal' AND NEW.initiated_by_actor_id IS NOT NULL AND length(NEW.initiated_by_actor_id) = 21 AND NEW.initiated_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C") OR (NEW.initiated_by_actor_kind = 'agent_execution' AND NEW.initiated_by_actor_id IS NOT NULL AND length(NEW.initiated_by_actor_id) = 21 AND NEW.initiated_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C")))))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'invalid persisted conversation actor pair';
END;
$trigger$;

CREATE TRIGGER "conversation_actor_turn_initiated_by_actor_kind_pair_update" BEFORE UPDATE
ON "turn" FOR EACH ROW EXECUTE FUNCTION "conversation_actor_turn_initiated_by_actor_kind_pair_update"();

CREATE OR REPLACE FUNCTION "read_model_repair_turn_item_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.status IN ('completed', 'failed', 'timed_out', 'cancelled')) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE read_model_repair_change_sequence
    SET generation = generation + 1
    WHERE singleton_key = 1;

    INSERT INTO read_model_repair_dirty_turn_item (turn_item_id
                                                 , generation
                                                 , changed_at)
    SELECT new.id
         , generation
         , statement_timestamp()
    FROM read_model_repair_change_sequence
    WHERE singleton_key = 1
    ON CONFLICT (turn_item_id)
       DO UPDATE
         SET generation = excluded.generation
           , changed_at = excluded.changed_at;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "read_model_repair_turn_item_insert" AFTER INSERT
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "read_model_repair_turn_item_insert"();

CREATE OR REPLACE FUNCTION "read_model_repair_turn_item_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM read_model_repair_dirty_turn_item
    WHERE turn_item_id = old.id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "read_model_repair_turn_item_delete" AFTER DELETE
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "read_model_repair_turn_item_delete"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_event_insert_register"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.turn_id IS NOT NULL AND NEW.native_method IN ('item/started','item/completed','item/agentMessage/delta',
    'item/commandExecution/outputDelta','turn/diff/updated',
    'thread/tokenUsage/updated','account/rateLimits/updated')
                AND octet_length(NEW.payload_redacted_json)<=262144) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO native_event_cleanup_job (turn_id
                                        , state)
    VALUES (new.turn_id, 'queued')
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_event_insert_register" AFTER INSERT
ON "cli_runtime_native_event" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_event_insert_register"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_event_insert_wake"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.turn_id IS NOT NULL AND NEW.native_method IN ('item/started','item/completed','item/agentMessage/delta',
    'item/commandExecution/outputDelta','turn/diff/updated',
    'thread/tokenUsage/updated','account/rateLimits/updated')
                AND octet_length(NEW.payload_redacted_json)<=262144
                AND EXISTS(SELECT 1 FROM "turn" t JOIN turn_cli_runtime_binding b ON b.turn_id=t.id
                    WHERE t.id=NEW.turn_id AND t.status IN ('completed','failed','interrupted')
                        AND b.status IN ('completed','failed','interrupted') AND b.runtime_id=NEW.runtime_id)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = new.turn_id
      AND state = 'waiting';
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_event_insert_wake" AFTER INSERT
ON "cli_runtime_native_event" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_event_insert_wake"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_event_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (EXISTS(SELECT 1 FROM native_event_cleanup_job WHERE turn_id=OLD.turn_id AND state='waiting')) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = old.turn_id
      AND state = 'waiting'
      AND CASE
            WHEN old.turn_id IS NULL
              THEN FALSE
            WHEN old.native_method IS NULL
              OR old.native_method NOT IN ('item/started', 'item/completed', 'item/agentMessage/delta', 'item/commandExecution/outputDelta', 'turn/diff/updated', 'thread/tokenUsage/updated', 'account/rateLimits/updated')
              THEN FALSE
            ELSE octet_length(old.payload_redacted_json) <= 262144
          END;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_event_delete" AFTER DELETE
ON "cli_runtime_native_event" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_event_delete"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_event_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = old.turn_id
      AND state = 'waiting'
      AND old.turn_id IS NOT NULL
      AND old.native_method IN ('item/started', 'item/completed', 'item/agentMessage/delta', 'item/commandExecution/outputDelta', 'turn/diff/updated', 'thread/tokenUsage/updated', 'account/rateLimits/updated')
      AND octet_length(old.payload_redacted_json) <= 262144
      AND (old.turn_id IS DISTINCT FROM new.turn_id
        OR old.runtime_id IS DISTINCT FROM new.runtime_id
        OR old.native_method IS DISTINCT FROM new.native_method
        OR old.payload_redacted_json IS DISTINCT FROM new.payload_redacted_json)
      AND EXISTS (SELECT 1
                  FROM turn
                  WHERE id = old.turn_id
                    AND status IN ('completed', 'failed', 'interrupted'));

    INSERT INTO native_event_cleanup_job (turn_id
                                        , state)
    SELECT new.turn_id
         , 'queued'
    WHERE new.turn_id IS NOT NULL
      AND new.native_method IN ('item/started', 'item/completed', 'item/agentMessage/delta', 'item/commandExecution/outputDelta', 'turn/diff/updated', 'thread/tokenUsage/updated', 'account/rateLimits/updated')
      AND octet_length(new.payload_redacted_json) <= 262144
    ON CONFLICT DO NOTHING;

    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = new.turn_id
      AND state = 'waiting'
      AND new.turn_id IS NOT NULL
      AND new.native_method IN ('item/started', 'item/completed', 'item/agentMessage/delta', 'item/commandExecution/outputDelta', 'turn/diff/updated', 'thread/tokenUsage/updated', 'account/rateLimits/updated')
      AND octet_length(new.payload_redacted_json) <= 262144
      AND EXISTS (SELECT 1
                  FROM turn AS t
                       INNER JOIN turn_cli_runtime_binding AS b ON b.turn_id = t.id
                  WHERE t.id = new.turn_id
                    AND t.status IN ('completed', 'failed', 'interrupted')
                    AND b.status IN ('completed', 'failed', 'interrupted')
                    AND b.runtime_id = new.runtime_id);
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_event_update" AFTER UPDATE OF turn_id,runtime_id,native_method,payload_redacted_json
ON "cli_runtime_native_event" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_event_update"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_turn_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((NEW.status IN ('completed','failed','interrupted')) AND EXISTS(SELECT 1 FROM "turn" WHERE id=NEW.id AND status IN ('completed','failed','interrupted'))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = new.id
      AND state = 'waiting';
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_turn_insert" AFTER INSERT
ON "turn" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_turn_insert"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_turn_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((OLD.status IS DISTINCT FROM NEW.status AND NEW.status IN ('completed','failed','interrupted')) AND EXISTS(SELECT 1 FROM "turn" WHERE id=NEW.id AND status IN ('completed','failed','interrupted'))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = new.id
      AND state = 'waiting';
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_turn_update" AFTER UPDATE OF status
ON "turn" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_turn_update"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_binding_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((NEW.status IN ('completed','failed','interrupted')) AND EXISTS(SELECT 1 FROM "turn" WHERE id=NEW.turn_id AND status IN ('completed','failed','interrupted'))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = new.turn_id
      AND state = 'waiting';
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_binding_insert" AFTER INSERT
ON "turn_cli_runtime_binding" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_binding_insert"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_binding_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (((OLD.runtime_id IS DISTINCT FROM NEW.runtime_id OR OLD.status IS DISTINCT FROM NEW.status) AND NEW.status IN ('completed','failed','interrupted')) AND EXISTS(SELECT 1 FROM "turn" WHERE id=NEW.turn_id AND status IN ('completed','failed','interrupted'))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = new.turn_id
      AND state = 'waiting';
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_binding_update" AFTER UPDATE OF runtime_id,status
ON "turn_cli_runtime_binding" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_binding_update"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_attempt_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((OLD.status IN ('starting','running') AND NEW.status NOT IN ('starting','running')) AND EXISTS(SELECT 1 FROM "turn" WHERE id=NEW.turn_id AND status IN ('completed','failed','interrupted'))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = new.turn_id
      AND state = 'waiting';
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_attempt_update" AFTER UPDATE OF status
ON "turn_cli_runtime_attempt" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_attempt_update"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_attempt_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((OLD.status IN ('starting','running')) AND EXISTS(SELECT 1 FROM "turn" WHERE id=OLD.turn_id AND status IN ('completed','failed','interrupted'))) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = old.turn_id
      AND state = 'waiting';
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_attempt_delete" AFTER DELETE
ON "turn_cli_runtime_attempt" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_attempt_delete"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_segment_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((OLD.status='running' AND NEW.status<>'running') AND EXISTS(SELECT 1 FROM "turn" WHERE id=NEW.turn_id AND status IN ('completed','failed','interrupted'))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = new.turn_id
      AND state = 'waiting';
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_segment_update" AFTER UPDATE OF status
ON "turn_cli_runtime_execution_segment" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_segment_update"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_segment_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((OLD.status='running') AND EXISTS(SELECT 1 FROM "turn" WHERE id=OLD.turn_id AND status IN ('completed','failed','interrupted'))) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = old.turn_id
      AND state = 'waiting';
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_segment_delete" AFTER DELETE
ON "turn_cli_runtime_execution_segment" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_segment_delete"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_recovery_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (((OLD.status IN ('pending','active') OR OLD.resolution_pending=TRUE) AND NOT (NEW.status IN ('pending','active') OR NEW.resolution_pending=TRUE)) AND EXISTS(SELECT 1 FROM "turn" WHERE id=NEW.turn_id AND status IN ('completed','failed','interrupted'))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = new.turn_id
      AND state = 'waiting';
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_recovery_update" AFTER UPDATE OF status,resolution_pending
ON "recovery_job" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_recovery_update"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_recovery_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((OLD.status IN ('pending','active') OR OLD.resolution_pending=TRUE) AND EXISTS(SELECT 1 FROM "turn" WHERE id=OLD.turn_id AND status IN ('completed','failed','interrupted'))) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = old.turn_id
      AND state = 'waiting';
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_recovery_delete" AFTER DELETE
ON "recovery_job" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_recovery_delete"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_projection_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((NEW.status='healthy' AND NEW.projected_through_sequence>0) AND EXISTS(SELECT 1 FROM "turn" WHERE id=NEW.turn_id AND status IN ('completed','failed','interrupted'))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = new.turn_id
      AND state = 'waiting';
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_projection_insert" AFTER INSERT
ON "turn_event_projection_stream_state" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_projection_insert"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_projection_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (((OLD.status IS DISTINCT FROM NEW.status OR OLD.projected_through_sequence IS DISTINCT FROM NEW.projected_through_sequence) AND NEW.status='healthy' AND NEW.projected_through_sequence>0) AND EXISTS(SELECT 1 FROM "turn" WHERE id=NEW.turn_id AND status IN ('completed','failed','interrupted'))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = new.turn_id
      AND state = 'waiting';
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_projection_update" AFTER UPDATE OF status,projected_through_sequence
ON "turn_event_projection_stream_state" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_projection_update"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_receipt_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (EXISTS(
                    SELECT 1 FROM "turn" t JOIN turn_event_projection_stream_state p ON p.turn_id=t.id
                    WHERE t.id=OLD.turn_id AND t.status IN ('completed','failed','interrupted')
                        AND OLD.sequence>p.projected_through_sequence
                        AND NOT EXISTS(SELECT 1 FROM turn_event_projection_state remaining
                            WHERE remaining.turn_id=OLD.turn_id AND remaining.sequence>p.projected_through_sequence))) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = old.turn_id
      AND state = 'waiting';
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_receipt_delete" AFTER DELETE
ON "turn_event_projection_state" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_receipt_delete"();

CREATE OR REPLACE FUNCTION "native_event_cleanup_receipt_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((OLD.turn_id IS DISTINCT FROM NEW.turn_id OR OLD.sequence IS DISTINCT FROM NEW.sequence) AND EXISTS(
                    SELECT 1 FROM "turn" t JOIN turn_event_projection_stream_state p ON p.turn_id=t.id
                    WHERE t.id=OLD.turn_id AND t.status IN ('completed','failed','interrupted')
                        AND OLD.sequence>p.projected_through_sequence
                        AND NOT EXISTS(SELECT 1 FROM turn_event_projection_state remaining
                            WHERE remaining.turn_id=OLD.turn_id AND remaining.sequence>p.projected_through_sequence))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE native_event_cleanup_job
    SET state = 'queued'
      , available_at = 0
      , last_error = NULL
      , revision = revision + 1
    WHERE turn_id = old.turn_id
      AND state = 'waiting';
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "native_event_cleanup_receipt_update" AFTER UPDATE OF turn_id,sequence
ON "turn_event_projection_state" FOR EACH ROW EXECUTE FUNCTION "native_event_cleanup_receipt_update"();

CREATE OR REPLACE FUNCTION "task_run_conversation_snapshot_verified_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count FOR SHARE OF h))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'unverified or unavailable frozen root';
END;
$trigger$;

CREATE TRIGGER "task_run_conversation_snapshot_verified_insert" BEFORE INSERT
ON "task_run_conversation_snapshot" FOR EACH ROW EXECUTE FUNCTION "task_run_conversation_snapshot_verified_insert"();

CREATE OR REPLACE FUNCTION "task_run_conversation_snapshot_verified_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count FOR SHARE OF h))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'unverified or unavailable frozen root';
END;
$trigger$;

CREATE TRIGGER "task_run_conversation_snapshot_verified_update" BEFORE UPDATE OF history_json,run_id,task_id,workspace_id,conversation_thread_id,frozen_manifest_id
ON "task_run_conversation_snapshot" FOR EACH ROW EXECUTE FUNCTION "task_run_conversation_snapshot_verified_update"();

CREATE OR REPLACE FUNCTION "turn_runtime_snapshot_verified_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count FOR SHARE OF h))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'unverified or unavailable frozen root';
END;
$trigger$;

CREATE TRIGGER "turn_runtime_snapshot_verified_insert" BEFORE INSERT
ON "turn_runtime_snapshot" FOR EACH ROW EXECUTE FUNCTION "turn_runtime_snapshot_verified_insert"();

CREATE OR REPLACE FUNCTION "turn_runtime_snapshot_verified_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count FOR SHARE OF h))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'unverified or unavailable frozen root';
END;
$trigger$;

CREATE TRIGGER "turn_runtime_snapshot_verified_update" BEFORE UPDATE OF history_json,turn_id,thread_id,workspace_id,frozen_manifest_id
ON "turn_runtime_snapshot" FOR EACH ROW EXECUTE FUNCTION "turn_runtime_snapshot_verified_update"();

CREATE OR REPLACE FUNCTION "thread_cli_runtime_binding_verified_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count FOR SHARE OF h))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'unverified or unavailable frozen root';
END;
$trigger$;

CREATE TRIGGER "thread_cli_runtime_binding_verified_insert" BEFORE INSERT
ON "thread_cli_runtime_binding" FOR EACH ROW EXECUTE FUNCTION "thread_cli_runtime_binding_verified_insert"();

CREATE OR REPLACE FUNCTION "thread_cli_runtime_binding_verified_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count FOR SHARE OF h))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'unverified or unavailable frozen root';
END;
$trigger$;

CREATE TRIGGER "thread_cli_runtime_binding_verified_update" BEFORE UPDATE OF resume_cursor_json,thread_id,workspace_id,frozen_manifest_id
ON "thread_cli_runtime_binding" FOR EACH ROW EXECUTE FUNCTION "thread_cli_runtime_binding_verified_update"();

CREATE OR REPLACE FUNCTION "turn_cli_runtime_binding_verified_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count FOR SHARE OF h))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'unverified or unavailable frozen root';
END;
$trigger$;

CREATE TRIGGER "turn_cli_runtime_binding_verified_insert" BEFORE INSERT
ON "turn_cli_runtime_binding" FOR EACH ROW EXECUTE FUNCTION "turn_cli_runtime_binding_verified_insert"();

CREATE OR REPLACE FUNCTION "turn_cli_runtime_binding_verified_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count FOR SHARE OF h))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'unverified or unavailable frozen root';
END;
$trigger$;

CREATE TRIGGER "turn_cli_runtime_binding_verified_update" BEFORE UPDATE OF input_mapping_json,turn_id,thread_id,workspace_id,continuation_thread_id,frozen_manifest_id
ON "turn_cli_runtime_binding" FOR EACH ROW EXECUTE FUNCTION "turn_cli_runtime_binding_verified_update"();

CREATE OR REPLACE FUNCTION "execution_observation_thread_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM provider_usage_observation
    WHERE workspace_id = old.workspace_id
      AND (owner_id = old.id
        OR id IN (SELECT substr(operation_id, 18)
                  FROM execution_observation
                  WHERE workspace_id = old.workspace_id
                    AND thread_id = old.id
                    AND operation_id LIKE 'provider:journal:%'));

    DELETE FROM execution_observation
    WHERE workspace_id = old.workspace_id
      AND thread_id = old.id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "execution_observation_thread_delete" AFTER DELETE
ON "thread" FOR EACH ROW EXECUTE FUNCTION "execution_observation_thread_delete"();

CREATE OR REPLACE FUNCTION "execution_observation_turn_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM provider_usage_observation
    WHERE id IN (SELECT CASE
                          WHEN operation_id LIKE 'provider:journal:%'
                            THEN substr(operation_id, 18)
                          ELSE substr(operation_id
                                    , length('provider:' || old.id || ':') + 1)
                        END
                 FROM execution_observation
                 WHERE turn_id = old.id
                   AND kind IN ('provider_attempt', 'external_usage_snapshot')
                   AND operation_id LIKE 'provider:%');

    DELETE FROM execution_observation
    WHERE turn_id = old.id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "execution_observation_turn_delete" AFTER DELETE
ON "turn" FOR EACH ROW EXECUTE FUNCTION "execution_observation_turn_delete"();

CREATE OR REPLACE FUNCTION "execution_observation_workspace_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM execution_observation
    WHERE workspace_id = old.id;

    DELETE FROM execution_tariff
    WHERE workspace_id = old.id;

    DELETE FROM provider_usage_observation
    WHERE workspace_id = old.id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "execution_observation_workspace_delete" AFTER DELETE
ON "workspace" FOR EACH ROW EXECUTE FUNCTION "execution_observation_workspace_delete"();

CREATE OR REPLACE FUNCTION "read_model_repair_turn_item_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((OLD.status IN ('completed', 'failed', 'timed_out', 'cancelled') OR NEW.status IN ('completed', 'failed', 'timed_out', 'cancelled')) AND (OLD.status IS DISTINCT FROM NEW.status OR (NEW.payload_payload_id IS DISTINCT FROM OLD.payload_payload_id OR OLD.payload::text IS DISTINCT FROM NEW.payload::text))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE read_model_repair_change_sequence
    SET generation = generation + 1
    WHERE singleton_key = 1;

    INSERT INTO read_model_repair_dirty_turn_item (turn_item_id
                                                 , generation
                                                 , changed_at)
    SELECT new.id
         , generation
         , statement_timestamp()
    FROM read_model_repair_change_sequence
    WHERE singleton_key = 1
    ON CONFLICT (turn_item_id)
       DO UPDATE
         SET generation = excluded.generation
           , changed_at = excluded.changed_at;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "read_model_repair_turn_item_update" AFTER UPDATE OF status,payload,payload_payload_id
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "read_model_repair_turn_item_update"();
