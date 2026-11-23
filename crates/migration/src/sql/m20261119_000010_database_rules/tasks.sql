-- Durable task-event fanout and task delivery recovery.

CREATE OR REPLACE FUNCTION "task_delivery_recovery_retry_task_delivery_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.id IS DISTINCT FROM NEW.id OR OLD.workspace_id IS DISTINCT FROM NEW.workspace_id OR OLD.task_id IS DISTINCT FROM NEW.task_id OR OLD.run_id IS DISTINCT FROM NEW.run_id OR OLD.delivery_key IS DISTINCT FROM NEW.delivery_key OR OLD.mode IS DISTINCT FROM NEW.mode OR OLD.thread_target IS DISTINCT FROM NEW.thread_target OR OLD.target_thread_id IS DISTINCT FROM NEW.target_thread_id OR OLD.target_user_id IS DISTINCT FROM NEW.target_user_id OR OLD.webhook_url IS DISTINCT FROM NEW.webhook_url OR OLD.webhook_url_fingerprint IS DISTINCT FROM NEW.webhook_url_fingerprint OR OLD.status IS DISTINCT FROM NEW.status OR OLD.next_attempt_at IS DISTINCT FROM NEW.next_attempt_at OR OLD.attempt_count IS DISTINCT FROM NEW.attempt_count OR OLD.max_attempts IS DISTINCT FROM NEW.max_attempts OR OLD.result_snapshot_json::text IS DISTINCT FROM NEW.result_snapshot_json::text OR OLD.error_snapshot_json::text IS DISTINCT FROM NEW.error_snapshot_json::text OR OLD.delivered_turn_id IS DISTINCT FROM NEW.delivered_turn_id OR OLD.delivered_notification_id IS DISTINCT FROM NEW.delivered_notification_id OR OLD.delivered_at IS DISTINCT FROM NEW.delivered_at OR OLD.last_error IS DISTINCT FROM NEW.last_error OR OLD.created_at IS DISTINCT FROM NEW.created_at OR OLD.updated_at IS DISTINCT FROM NEW.updated_at) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    DELETE FROM task_delivery_recovery_retry
    WHERE delivery_id = old.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_delivery_recovery_retry_task_delivery_update" AFTER UPDATE
ON "task_delivery" FOR EACH ROW EXECUTE FUNCTION "task_delivery_recovery_retry_task_delivery_update"();

CREATE OR REPLACE FUNCTION "task_delivery_recovery_retry_task_delivery_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM task_delivery_recovery_retry
    WHERE delivery_id = old.id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "task_delivery_recovery_retry_task_delivery_delete" AFTER DELETE
ON "task_delivery" FOR EACH ROW EXECUTE FUNCTION "task_delivery_recovery_retry_task_delivery_delete"();

CREATE OR REPLACE FUNCTION "task_delivery_recovery_retry_task_delivery_attempt_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM task_delivery_recovery_retry
    WHERE delivery_id = new.delivery_id
      AND EXISTS (SELECT 1
                  FROM task_delivery AS d
                  WHERE d.id = new.delivery_id
                    AND d.attempt_count = new.attempt_number);
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_delivery_recovery_retry_task_delivery_attempt_insert" AFTER INSERT
ON "task_delivery_attempt" FOR EACH ROW EXECUTE FUNCTION "task_delivery_recovery_retry_task_delivery_attempt_insert"();

CREATE OR REPLACE FUNCTION "task_delivery_recovery_retry_task_delivery_attempt_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.id IS DISTINCT FROM NEW.id OR OLD.delivery_id IS DISTINCT FROM NEW.delivery_id OR OLD.attempt_number IS DISTINCT FROM NEW.attempt_number OR OLD.status IS DISTINCT FROM NEW.status OR OLD.started_at IS DISTINCT FROM NEW.started_at OR OLD.completed_at IS DISTINCT FROM NEW.completed_at OR OLD.http_status IS DISTINCT FROM NEW.http_status OR OLD.error IS DISTINCT FROM NEW.error OR OLD.response_fingerprint IS DISTINCT FROM NEW.response_fingerprint) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    DELETE FROM task_delivery_recovery_retry
    WHERE (delivery_id = old.delivery_id
       AND EXISTS (SELECT 1
                   FROM task_delivery AS d
                   WHERE d.id = old.delivery_id
                     AND d.attempt_count = old.attempt_number))
       OR (delivery_id = new.delivery_id
       AND EXISTS (SELECT 1
                   FROM task_delivery AS d
                   WHERE d.id = new.delivery_id
                     AND d.attempt_count = new.attempt_number));
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_delivery_recovery_retry_task_delivery_attempt_update" AFTER UPDATE
ON "task_delivery_attempt" FOR EACH ROW EXECUTE FUNCTION "task_delivery_recovery_retry_task_delivery_attempt_update"();

CREATE OR REPLACE FUNCTION "task_delivery_recovery_retry_task_delivery_attempt_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM task_delivery_recovery_retry
    WHERE delivery_id = old.delivery_id
      AND EXISTS (SELECT 1
                  FROM task_delivery AS d
                  WHERE d.id = old.delivery_id
                    AND d.attempt_count = old.attempt_number);
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "task_delivery_recovery_retry_task_delivery_attempt_delete" AFTER DELETE
ON "task_delivery_attempt" FOR EACH ROW EXECUTE FUNCTION "task_delivery_recovery_retry_task_delivery_attempt_delete"();

CREATE OR REPLACE FUNCTION "task_event_fanout_append"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM task_event_fanout_sequence
                    WHERE singleton=1 AND generation<9223372036854775807)
                    THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'task fanout generation missing or exhausted';
    END IF;

    UPDATE task_event_fanout_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO task_event_fanout_pending (task_id
                                         , first_sequence
                                         , newest_sequence
                                         , generation
                                         , due_at
                                         , claim_token
                                         , attempts)
    SELECT new.task_id
         , new.sequence
         , new.sequence
         , generation
         , CAST(floor(EXTRACT(EPOCH FROM statement_timestamp())) AS bigint)
         , NULL
         , 0
    FROM task_event_fanout_sequence
    WHERE singleton = 1
    ON CONFLICT (task_id)
       DO UPDATE
         SET newest_sequence = GREATEST(task_event_fanout_pending.newest_sequence, excluded.newest_sequence)
           , generation = excluded.generation;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_event_fanout_append" AFTER INSERT
ON "task_event" FOR EACH ROW EXECUTE FUNCTION "task_event_fanout_append"();

CREATE OR REPLACE FUNCTION "task_event_fanout_cursor_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM task_event_fanout_pending
    WHERE task_id = new.task_id
      AND newest_sequence <= new.last_sequence;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_event_fanout_cursor_insert" AFTER INSERT
ON "task_event_fanout_cursor" FOR EACH ROW EXECUTE FUNCTION "task_event_fanout_cursor_insert"();

CREATE OR REPLACE FUNCTION "task_event_fanout_cursor_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.task_id IS DISTINCT FROM NEW.task_id) AND EXISTS(SELECT 1 FROM task_event_fanout_pending WHERE task_id=OLD.task_id) AND NOT EXISTS(SELECT 1 FROM task_event_fanout_sequence
                WHERE singleton=1 AND generation<9223372036854775807)
                THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'task fanout generation missing or exhausted';
    END IF;

    UPDATE task_event_fanout_sequence
    SET generation = generation + 1
    WHERE singleton = 1
      AND old.task_id IS DISTINCT FROM new.task_id
      AND EXISTS (SELECT 1
                  FROM task_event_fanout_pending
                  WHERE task_id = old.task_id);

    UPDATE task_event_fanout_pending
    SET generation = (SELECT generation
                      FROM task_event_fanout_sequence
                      WHERE singleton = 1)
      , claim_token = NULL
    WHERE task_id = old.task_id
      AND old.task_id IS DISTINCT FROM new.task_id;

    DELETE FROM task_event_fanout_pending
    WHERE task_id = new.task_id
      AND newest_sequence <= new.last_sequence;

    IF (NEW.last_sequence<OLD.last_sequence OR NEW.task_id IS DISTINCT FROM OLD.task_id) AND EXISTS(SELECT 1 FROM task_event_fanout_pending WHERE task_id=NEW.task_id) AND NOT EXISTS(SELECT 1 FROM task_event_fanout_sequence
                WHERE singleton=1 AND generation<9223372036854775807)
                THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'task fanout generation missing or exhausted';
    END IF;

    UPDATE task_event_fanout_sequence
    SET generation = generation + 1
    WHERE singleton = 1
      AND (new.last_sequence < old.last_sequence
        OR new.task_id IS DISTINCT FROM old.task_id)
      AND EXISTS (SELECT 1
                  FROM task_event_fanout_pending
                  WHERE task_id = new.task_id);

    UPDATE task_event_fanout_pending
    SET generation = (SELECT generation
                      FROM task_event_fanout_sequence
                      WHERE singleton = 1)
      , claim_token = NULL
    WHERE task_id = new.task_id
      AND (new.last_sequence < old.last_sequence
        OR new.task_id IS DISTINCT FROM old.task_id);
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "task_event_fanout_cursor_update" AFTER UPDATE OF task_id,last_sequence
ON "task_event_fanout_cursor" FOR EACH ROW EXECUTE FUNCTION "task_event_fanout_cursor_update"();

CREATE OR REPLACE FUNCTION "task_event_fanout_cursor_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (EXISTS(SELECT 1 FROM task WHERE id=OLD.task_id)) AND EXISTS(SELECT 1 FROM task_event_fanout_pending WHERE task_id=OLD.task_id) AND NOT EXISTS(SELECT 1 FROM task_event_fanout_sequence
                WHERE singleton=1 AND generation<9223372036854775807)
                THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'task fanout generation missing or exhausted';
    END IF;

    UPDATE task_event_fanout_sequence
    SET generation = generation + 1
    WHERE singleton = 1
      AND EXISTS (SELECT 1
                  FROM task
                  WHERE id = old.task_id)
      AND EXISTS (SELECT 1
                  FROM task_event_fanout_pending
                  WHERE task_id = old.task_id);

    UPDATE task_event_fanout_pending
    SET generation = (SELECT generation
                      FROM task_event_fanout_sequence
                      WHERE singleton = 1)
      , claim_token = NULL
    WHERE task_id = old.task_id
      AND EXISTS (SELECT 1
                  FROM task
                  WHERE id = old.task_id);

    DELETE FROM task_event_fanout_pending
    WHERE task_id = old.task_id
      AND NOT EXISTS (SELECT 1
                      FROM task
                      WHERE id = old.task_id);
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "task_event_fanout_cursor_delete" AFTER DELETE
ON "task_event_fanout_cursor" FOR EACH ROW EXECUTE FUNCTION "task_event_fanout_cursor_delete"();

CREATE OR REPLACE FUNCTION "task_event_fanout_task_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM task_event_fanout_pending
    WHERE task_id = old.id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "task_event_fanout_task_delete" AFTER DELETE
ON "task" FOR EACH ROW EXECUTE FUNCTION "task_event_fanout_task_delete"();
