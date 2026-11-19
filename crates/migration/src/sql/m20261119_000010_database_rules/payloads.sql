-- Portable payload identity, revision invalidation, and retirement on replace/delete.

CREATE OR REPLACE FUNCTION "tool_output_chunk_portable_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE tool_output_chunk
    SET portable_identity = replace(CAST(gen_random_uuid() AS text)
                                  , '-'
                                  , '')
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "tool_output_chunk_portable_insert" AFTER INSERT
ON "tool_output_chunk" FOR EACH ROW EXECUTE FUNCTION "tool_output_chunk_portable_insert"();

CREATE OR REPLACE FUNCTION "tool_output_chunk_text_portable_logical"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.text_payload_id IS NOT DISTINCT FROM OLD.text_payload_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE tool_output_chunk
    SET text_payload_id = NULL
      , portable_revision = portable_revision + 1
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "tool_output_chunk_text_portable_logical" AFTER UPDATE OF text
ON "tool_output_chunk" FOR EACH ROW EXECUTE FUNCTION "tool_output_chunk_text_portable_logical"();

CREATE OR REPLACE FUNCTION "tool_output_chunk_text_portable_replace"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.text_payload_id IS NOT NULL AND OLD.text_payload_id IS DISTINCT FROM NEW.text_payload_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.text_payload_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "tool_output_chunk_text_portable_replace" AFTER UPDATE OF text_payload_id
ON "tool_output_chunk" FOR EACH ROW EXECUTE FUNCTION "tool_output_chunk_text_portable_replace"();

CREATE OR REPLACE FUNCTION "tool_output_chunk_text_portable_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.text_payload_id IS NOT NULL) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.text_payload_id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "tool_output_chunk_text_portable_delete" AFTER DELETE
ON "tool_output_chunk" FOR EACH ROW EXECUTE FUNCTION "tool_output_chunk_text_portable_delete"();

CREATE OR REPLACE FUNCTION "tool_output_chunk_metadata_portable_logical"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.metadata_payload_id IS NOT DISTINCT FROM OLD.metadata_payload_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE tool_output_chunk
    SET metadata_payload_id = NULL
      , portable_revision = portable_revision + 1
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "tool_output_chunk_metadata_portable_logical" AFTER UPDATE OF metadata
ON "tool_output_chunk" FOR EACH ROW EXECUTE FUNCTION "tool_output_chunk_metadata_portable_logical"();

CREATE OR REPLACE FUNCTION "tool_output_chunk_metadata_portable_replace"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.metadata_payload_id IS NOT NULL AND OLD.metadata_payload_id IS DISTINCT FROM NEW.metadata_payload_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.metadata_payload_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "tool_output_chunk_metadata_portable_replace" AFTER UPDATE OF metadata_payload_id
ON "tool_output_chunk" FOR EACH ROW EXECUTE FUNCTION "tool_output_chunk_metadata_portable_replace"();

CREATE OR REPLACE FUNCTION "tool_output_chunk_metadata_portable_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.metadata_payload_id IS NOT NULL) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.metadata_payload_id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "tool_output_chunk_metadata_portable_delete" AFTER DELETE
ON "tool_output_chunk" FOR EACH ROW EXECUTE FUNCTION "tool_output_chunk_metadata_portable_delete"();

CREATE OR REPLACE FUNCTION "tool_output_chunk_portable_owner"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE tool_output_chunk
    SET portable_revision = portable_revision + 1
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "tool_output_chunk_portable_owner" AFTER UPDATE OF id,turn_id,item_id,stream
ON "tool_output_chunk" FOR EACH ROW EXECUTE FUNCTION "tool_output_chunk_portable_owner"();

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

CREATE OR REPLACE FUNCTION "turn_cli_runtime_binding_input_mapping_json_portable_logical"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.input_mapping_json_payload_id IS NOT DISTINCT FROM OLD.input_mapping_json_payload_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE turn_cli_runtime_binding
    SET input_mapping_json_payload_id = NULL
      , portable_revision = portable_revision + 1
    WHERE turn_id = new.turn_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_cli_runtime_binding_input_mapping_json_portable_logical" AFTER UPDATE OF input_mapping_json
ON "turn_cli_runtime_binding" FOR EACH ROW EXECUTE FUNCTION "turn_cli_runtime_binding_input_mapping_json_portable_logical"();

CREATE OR REPLACE FUNCTION "turn_cli_runtime_binding_input_mapping_json_portable_replace"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.input_mapping_json_payload_id IS NOT NULL AND OLD.input_mapping_json_payload_id IS DISTINCT FROM NEW.input_mapping_json_payload_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.input_mapping_json_payload_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_cli_runtime_binding_input_mapping_json_portable_replace" AFTER UPDATE OF input_mapping_json_payload_id
ON "turn_cli_runtime_binding" FOR EACH ROW EXECUTE FUNCTION "turn_cli_runtime_binding_input_mapping_json_portable_replace"();

CREATE OR REPLACE FUNCTION "turn_cli_runtime_binding_input_mapping_json_portable_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.input_mapping_json_payload_id IS NOT NULL) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.input_mapping_json_payload_id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "turn_cli_runtime_binding_input_mapping_json_portable_delete" AFTER DELETE
ON "turn_cli_runtime_binding" FOR EACH ROW EXECUTE FUNCTION "turn_cli_runtime_binding_input_mapping_json_portable_delete"();

CREATE OR REPLACE FUNCTION "turn_cli_runtime_binding_portable_owner"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE turn_cli_runtime_binding
    SET portable_revision = portable_revision + 1
    WHERE turn_id = new.turn_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_cli_runtime_binding_portable_owner" AFTER UPDATE OF turn_id,thread_id,workspace_id,continuation_thread_id,frozen_manifest_id
ON "turn_cli_runtime_binding" FOR EACH ROW EXECUTE FUNCTION "turn_cli_runtime_binding_portable_owner"();

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

CREATE OR REPLACE FUNCTION "task_run_conversation_snapshot_history_json_portable_logical"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.history_json_payload_id IS NOT DISTINCT FROM OLD.history_json_payload_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE task_run_conversation_snapshot
    SET history_json_payload_id = NULL
      , portable_revision = portable_revision + 1
    WHERE run_id = new.run_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_run_conversation_snapshot_history_json_portable_logical" AFTER UPDATE OF history_json
ON "task_run_conversation_snapshot" FOR EACH ROW EXECUTE FUNCTION "task_run_conversation_snapshot_history_json_portable_logical"();

CREATE OR REPLACE FUNCTION "task_run_conversation_snapshot_history_json_portable_replace"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.history_json_payload_id IS NOT NULL AND OLD.history_json_payload_id IS DISTINCT FROM NEW.history_json_payload_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.history_json_payload_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_run_conversation_snapshot_history_json_portable_replace" AFTER UPDATE OF history_json_payload_id
ON "task_run_conversation_snapshot" FOR EACH ROW EXECUTE FUNCTION "task_run_conversation_snapshot_history_json_portable_replace"();

CREATE OR REPLACE FUNCTION "task_run_conversation_snapshot_history_json_portable_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.history_json_payload_id IS NOT NULL) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.history_json_payload_id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "task_run_conversation_snapshot_history_json_portable_delete" AFTER DELETE
ON "task_run_conversation_snapshot" FOR EACH ROW EXECUTE FUNCTION "task_run_conversation_snapshot_history_json_portable_delete"();

CREATE OR REPLACE FUNCTION "task_run_conversation_snapshot_portable_owner"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE task_run_conversation_snapshot
    SET portable_revision = portable_revision + 1
    WHERE run_id = new.run_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_run_conversation_snapshot_portable_owner" AFTER UPDATE OF run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,created_at,frozen_manifest_id
ON "task_run_conversation_snapshot" FOR EACH ROW EXECUTE FUNCTION "task_run_conversation_snapshot_portable_owner"();

CREATE OR REPLACE FUNCTION "task_event_portable_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE task_event
    SET portable_identity = replace(CAST(gen_random_uuid() AS text)
                                  , '-'
                                  , '')
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_event_portable_insert" AFTER INSERT
ON "task_event" FOR EACH ROW EXECUTE FUNCTION "task_event_portable_insert"();

CREATE OR REPLACE FUNCTION "task_event_payload_json_portable_logical"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.payload_json_payload_id IS NOT DISTINCT FROM OLD.payload_json_payload_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE task_event
    SET payload_json_payload_id = NULL
      , portable_revision = portable_revision + 1
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_event_payload_json_portable_logical" AFTER UPDATE OF payload_json
ON "task_event" FOR EACH ROW EXECUTE FUNCTION "task_event_payload_json_portable_logical"();

CREATE OR REPLACE FUNCTION "task_event_payload_json_portable_replace"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.payload_json_payload_id IS NOT NULL AND OLD.payload_json_payload_id IS DISTINCT FROM NEW.payload_json_payload_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.payload_json_payload_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_event_payload_json_portable_replace" AFTER UPDATE OF payload_json_payload_id
ON "task_event" FOR EACH ROW EXECUTE FUNCTION "task_event_payload_json_portable_replace"();

CREATE OR REPLACE FUNCTION "task_event_payload_json_portable_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.payload_json_payload_id IS NOT NULL) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.payload_json_payload_id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "task_event_payload_json_portable_delete" AFTER DELETE
ON "task_event" FOR EACH ROW EXECUTE FUNCTION "task_event_payload_json_portable_delete"();

CREATE OR REPLACE FUNCTION "task_event_portable_owner"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE task_event
    SET portable_revision = portable_revision + 1
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_event_portable_owner" AFTER UPDATE OF id,task_id,run_id,thread_id,turn_id,sequence,event_type,idempotency_key,created_at
ON "task_event" FOR EACH ROW EXECUTE FUNCTION "task_event_portable_owner"();

CREATE OR REPLACE FUNCTION "turn_event_portable_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE turn_event
    SET portable_identity = replace(CAST(gen_random_uuid() AS text)
                                  , '-'
                                  , '')
      , portable_revision = 0
      , payload_payload_id = NULL
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_event_portable_insert" AFTER INSERT
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "turn_event_portable_insert"();

CREATE OR REPLACE FUNCTION "turn_event_payload_portable_logical"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.payload_payload_id IS NOT DISTINCT FROM OLD.payload_payload_id AND NEW.payload::text IS DISTINCT FROM OLD.payload::text) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE turn_event
    SET payload_payload_id = NULL
      , portable_revision = portable_revision + 1
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_event_payload_portable_logical" AFTER UPDATE OF payload
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "turn_event_payload_portable_logical"();

CREATE OR REPLACE FUNCTION "turn_event_payload_portable_replace"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.payload_payload_id IS NOT NULL AND OLD.payload_payload_id IS DISTINCT FROM NEW.payload_payload_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.payload_payload_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_event_payload_portable_replace" AFTER UPDATE OF payload_payload_id
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "turn_event_payload_portable_replace"();

CREATE OR REPLACE FUNCTION "turn_event_payload_portable_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.payload_payload_id IS NOT NULL) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.payload_payload_id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "turn_event_payload_portable_delete" AFTER DELETE
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "turn_event_payload_portable_delete"();

CREATE OR REPLACE FUNCTION "turn_event_portable_owner"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.id IS DISTINCT FROM OLD.id OR NEW.thread_id IS DISTINCT FROM OLD.thread_id OR NEW.turn_id IS DISTINCT FROM OLD.turn_id OR NEW.sequence IS DISTINCT FROM OLD.sequence OR NEW.event_type IS DISTINCT FROM OLD.event_type OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key OR NEW.created_at IS DISTINCT FROM OLD.created_at) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE turn_event
    SET portable_revision = portable_revision + 1
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_event_portable_owner" AFTER UPDATE OF id,thread_id,turn_id,sequence,event_type,idempotency_key,created_at
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "turn_event_portable_owner"();

CREATE OR REPLACE FUNCTION "turn_item_portable_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE turn_item
    SET portable_identity = replace(CAST(gen_random_uuid() AS text)
                                  , '-'
                                  , '')
      , portable_revision = 0
      , payload_payload_id = NULL
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_item_portable_insert" AFTER INSERT
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "turn_item_portable_insert"();

CREATE OR REPLACE FUNCTION "turn_item_payload_portable_logical"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.payload_payload_id IS NOT DISTINCT FROM OLD.payload_payload_id AND NEW.payload::text IS DISTINCT FROM OLD.payload::text) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE turn_item
    SET payload_payload_id = NULL
      , portable_revision = portable_revision + 1
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_item_payload_portable_logical" AFTER UPDATE OF payload
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "turn_item_payload_portable_logical"();

CREATE OR REPLACE FUNCTION "turn_item_payload_portable_replace"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.payload_payload_id IS NOT NULL AND OLD.payload_payload_id IS DISTINCT FROM NEW.payload_payload_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.payload_payload_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_item_payload_portable_replace" AFTER UPDATE OF payload_payload_id
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "turn_item_payload_portable_replace"();

CREATE OR REPLACE FUNCTION "turn_item_payload_portable_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.payload_payload_id IS NOT NULL) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    UPDATE portable_payload
    SET retired = 1
    WHERE id = old.payload_payload_id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "turn_item_payload_portable_delete" AFTER DELETE
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "turn_item_payload_portable_delete"();

CREATE OR REPLACE FUNCTION "turn_item_portable_owner"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.id IS DISTINCT FROM OLD.id OR NEW.turn_id IS DISTINCT FROM OLD.turn_id OR NEW.item_id IS DISTINCT FROM OLD.item_id OR NEW.item_type IS DISTINCT FROM OLD.item_type OR NEW.status IS DISTINCT FROM OLD.status OR NEW.active_attempt_id IS DISTINCT FROM OLD.active_attempt_id OR NEW.active_attempt_status IS DISTINCT FROM OLD.active_attempt_status OR NEW.active_attempt_number IS DISTINCT FROM OLD.active_attempt_number OR NEW.created_at IS DISTINCT FROM OLD.created_at) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    UPDATE turn_item
    SET portable_revision = portable_revision + 1
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "turn_item_portable_owner" AFTER UPDATE OF id,turn_id,item_id,item_type,status,active_attempt_id,active_attempt_status,active_attempt_number,created_at
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "turn_item_portable_owner"();
