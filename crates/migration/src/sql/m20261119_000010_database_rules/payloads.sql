-- Portable payload identity, revision invalidation, and retirement on replace/delete.

CREATE OR REPLACE FUNCTION "turn_cli_runtime_binding_portable_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE turn_cli_runtime_binding
    SET portable_identity = replace(CAST(gen_random_uuid() AS text)
                                  , '-'
                                  , '')
    WHERE turn_id = new.turn_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_cli_runtime_binding_portable_insert" AFTER INSERT
ON "turn_cli_runtime_binding" FOR EACH ROW EXECUTE FUNCTION "turn_cli_runtime_binding_portable_insert"();

CREATE OR REPLACE FUNCTION "turn_cli_runtime_binding_mapping_revision"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    NEW.portable_revision := OLD.portable_revision + 1;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_cli_runtime_binding_mapping_revision" BEFORE UPDATE OF
    input_mapping_json, input_mapping_has_inline_history, frozen_manifest_id,
    turn_id, thread_id, workspace_id, continuation_thread_id
ON "turn_cli_runtime_binding" FOR EACH ROW EXECUTE FUNCTION "turn_cli_runtime_binding_mapping_revision"();

CREATE OR REPLACE FUNCTION "task_run_conversation_snapshot_portable_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE task_run_conversation_snapshot
    SET portable_identity = replace(CAST(gen_random_uuid() AS text)
                                  , '-'
                                  , '')
    WHERE run_id = new.run_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_run_conversation_snapshot_portable_insert" AFTER INSERT
ON "task_run_conversation_snapshot" FOR EACH ROW EXECUTE FUNCTION "task_run_conversation_snapshot_portable_insert"();

CREATE OR REPLACE FUNCTION "task_run_conversation_snapshot_portable_owner"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.run_id, NEW.task_id, NEW.workspace_id, NEW.conversation_thread_id,
        NEW.source_turn_id, NEW.created_at, NEW.frozen_manifest_id,
        NEW.history_json, NEW.history_json_payload_id, NEW.history_json_has_array_prefix)
       IS DISTINCT FROM
       (OLD.run_id, OLD.task_id, OLD.workspace_id, OLD.conversation_thread_id,
        OLD.source_turn_id, OLD.created_at, OLD.frozen_manifest_id,
        OLD.history_json, OLD.history_json_payload_id, OLD.history_json_has_array_prefix) THEN
        NEW.portable_revision := OLD.portable_revision + 1;
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_run_conversation_snapshot_portable_owner" BEFORE UPDATE
ON "task_run_conversation_snapshot" FOR EACH ROW EXECUTE FUNCTION "task_run_conversation_snapshot_portable_owner"();

CREATE OR REPLACE FUNCTION "task_run_conversation_snapshot_history_json_portable_replace"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF OLD.history_json_payload_id IS DISTINCT FROM NEW.history_json_payload_id THEN
        DELETE FROM portable_payload_value WHERE id = OLD.history_json_payload_id;
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_run_conversation_snapshot_history_json_portable_replace" AFTER UPDATE OF history_json_payload_id
ON "task_run_conversation_snapshot" FOR EACH ROW EXECUTE FUNCTION "task_run_conversation_snapshot_history_json_portable_replace"();

CREATE OR REPLACE FUNCTION "task_run_conversation_snapshot_history_json_portable_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM portable_payload_value WHERE id = OLD.history_json_payload_id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "task_run_conversation_snapshot_history_json_portable_delete" AFTER DELETE
ON "task_run_conversation_snapshot" FOR EACH ROW EXECUTE FUNCTION "task_run_conversation_snapshot_history_json_portable_delete"();

CREATE OR REPLACE FUNCTION "turn_event_portable_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    NEW.portable_identity := replace(gen_random_uuid()::text, '-', '');
    NEW.portable_revision := 0;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_event_portable_insert" BEFORE INSERT
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "turn_event_portable_insert"();

CREATE OR REPLACE FUNCTION "turn_event_portable_owner"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.id, NEW.thread_id, NEW.turn_id, NEW.sequence, NEW.event_type,
        NEW.created_at, NEW.idempotency_key, NEW.payload::text, NEW.payload_payload_id)
       IS DISTINCT FROM
       (OLD.id, OLD.thread_id, OLD.turn_id, OLD.sequence, OLD.event_type,
        OLD.created_at, OLD.idempotency_key, OLD.payload::text, OLD.payload_payload_id) THEN
        NEW.portable_revision := OLD.portable_revision + 1;
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_event_portable_owner" BEFORE UPDATE
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "turn_event_portable_owner"();

CREATE OR REPLACE FUNCTION "turn_event_payload_portable_replace"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF OLD.payload_payload_id IS DISTINCT FROM NEW.payload_payload_id THEN
        DELETE FROM portable_payload_value WHERE id = OLD.payload_payload_id;
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_event_payload_portable_replace" AFTER UPDATE OF payload_payload_id
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "turn_event_payload_portable_replace"();

CREATE OR REPLACE FUNCTION "turn_event_payload_portable_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM portable_payload_value WHERE id = OLD.payload_payload_id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "turn_event_payload_portable_delete" AFTER DELETE
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "turn_event_payload_portable_delete"();

CREATE OR REPLACE FUNCTION "turn_item_portable_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    NEW.portable_identity := replace(gen_random_uuid()::text, '-', '');
    NEW.portable_revision := 0;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_item_portable_insert" BEFORE INSERT
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "turn_item_portable_insert"();

CREATE OR REPLACE FUNCTION "turn_item_portable_owner"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.id, NEW.turn_id, NEW.item_id, NEW.item_type, NEW.status,
        NEW.payload::text, NEW.payload_payload_id)
       IS DISTINCT FROM
       (OLD.id, OLD.turn_id, OLD.item_id, OLD.item_type, OLD.status,
        OLD.payload::text, OLD.payload_payload_id) THEN
        NEW.portable_revision := OLD.portable_revision + 1;
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_item_portable_owner" BEFORE UPDATE
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "turn_item_portable_owner"();

CREATE OR REPLACE FUNCTION "turn_item_payload_portable_replace"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF OLD.payload_payload_id IS DISTINCT FROM NEW.payload_payload_id THEN
        DELETE FROM portable_payload_value WHERE id = OLD.payload_payload_id;
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_item_payload_portable_replace" AFTER UPDATE OF payload_payload_id
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "turn_item_payload_portable_replace"();

CREATE OR REPLACE FUNCTION "turn_item_payload_portable_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM portable_payload_value WHERE id = OLD.payload_payload_id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "turn_item_payload_portable_delete" AFTER DELETE
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "turn_item_payload_portable_delete"();
