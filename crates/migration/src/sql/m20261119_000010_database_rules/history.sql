-- Compaction revisions, frozen proof guards, sweep work and HTML preview retention.

CREATE OR REPLACE FUNCTION "compaction_source_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    -- Retry scans capture a committed ceiling. Serialize MAX + 1 through commit.
    PERFORM pg_advisory_xact_lock(1885955182, 1);

    INSERT INTO compaction_source_revision (source_id
                                          , turn_id
                                          , revision
                                          , present
                                          , capture_order)
    VALUES (new.id
          , new.turn_id
          , 1
          , 1
          , (SELECT COALESCE(max(capture_order)
                           , 0) + 1
             FROM compaction_source_revision))
    ON CONFLICT (source_id)
       DO UPDATE
         SET revision = compaction_source_revision.revision + 1
           , present = 1
           , turn_id = new.turn_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_source_insert" AFTER INSERT
ON "turn_llm_context" FOR EACH ROW EXECUTE FUNCTION "compaction_source_insert"();

CREATE OR REPLACE FUNCTION "compaction_source_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM pg_advisory_xact_lock(1885955182, 1);

    INSERT INTO compaction_source_revision (source_id
                                          , turn_id
                                          , revision
                                          , present
                                          , capture_order)
    VALUES (old.id
          , old.turn_id
          , 2
          , 0
          , (SELECT COALESCE(max(capture_order)
                           , 0) + 1
             FROM compaction_source_revision))
    ON CONFLICT (source_id)
       DO UPDATE
         SET revision = compaction_source_revision.revision + 1
           , present = 0;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_source_delete" AFTER DELETE
ON "turn_llm_context" FOR EACH ROW EXECUTE FUNCTION "compaction_source_delete"();

CREATE OR REPLACE FUNCTION "compaction_input_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM pg_advisory_xact_lock(1885955182, 1);

    INSERT INTO compaction_input_revision (source_id
                                         , turn_id
                                         , revision
                                         , present
                                         , capture_order)
    VALUES (new.id
          , new.turn_id
          , 1
          , 1
          , (SELECT COALESCE(max(capture_order)
                           , 0) + 1
             FROM compaction_input_revision))
    ON CONFLICT (source_id)
       DO UPDATE
         SET revision = compaction_input_revision.revision + 1
           , turn_id = new.turn_id
           , present = 1;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_input_insert" AFTER INSERT
ON "turn_input" FOR EACH ROW EXECUTE FUNCTION "compaction_input_insert"();

CREATE OR REPLACE FUNCTION "compaction_input_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM pg_advisory_xact_lock(1885955182, 1);

    INSERT INTO compaction_input_revision (source_id
                                         , turn_id
                                         , revision
                                         , present
                                         , capture_order)
    VALUES (old.id
          , old.turn_id
          , 2
          , 0
          , (SELECT COALESCE(max(capture_order)
                           , 0) + 1
             FROM compaction_input_revision))
    ON CONFLICT (source_id)
       DO UPDATE
         SET revision = compaction_input_revision.revision + 1
           , present = 0;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_input_delete" AFTER DELETE
ON "turn_input" FOR EACH ROW EXECUTE FUNCTION "compaction_input_delete"();

CREATE OR REPLACE FUNCTION "compaction_input_epoch_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_projection_epoch (thread_id
                                           , version)
    SELECT t.thread_id
         , 1
    FROM turn AS t
         INNER JOIN thread AS th ON th.id = t.thread_id
    WHERE t.id = old.turn_id
    ON CONFLICT (thread_id)
       DO UPDATE
         SET version = compaction_projection_epoch.version + 1;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_input_epoch_delete" BEFORE DELETE
ON "turn_input" FOR EACH ROW EXECUTE FUNCTION "compaction_input_epoch_delete"();

CREATE OR REPLACE FUNCTION "compaction_event_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM pg_advisory_xact_lock(1885955182, 1);

    INSERT INTO compaction_event_revision (source_id
                                         , turn_id
                                         , revision
                                         , present
                                         , capture_order)
    VALUES (new.id
          , new.turn_id
          , 1
          , 1
          , (SELECT COALESCE(max(capture_order)
                           , 0) + 1
             FROM compaction_event_revision))
    ON CONFLICT (source_id)
       DO UPDATE
         SET revision = compaction_event_revision.revision + 1
           , present = 1
           , turn_id = new.turn_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_event_insert" AFTER INSERT
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "compaction_event_insert"();

CREATE OR REPLACE FUNCTION "compaction_event_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM pg_advisory_xact_lock(1885955182, 1);

    INSERT INTO compaction_event_revision (source_id
                                         , turn_id
                                         , revision
                                         , present
                                         , capture_order)
    VALUES (old.id
          , old.turn_id
          , 2
          , 0
          , (SELECT COALESCE(max(capture_order)
                           , 0) + 1
             FROM compaction_event_revision))
    ON CONFLICT (source_id)
       DO UPDATE
         SET revision = compaction_event_revision.revision + 1
           , present = 0;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_event_delete" AFTER DELETE
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "compaction_event_delete"();

CREATE OR REPLACE FUNCTION "compaction_turn_creation_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_turn_creation (turn_id)
    VALUES (new.id);
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_turn_creation_insert" AFTER INSERT
ON "turn" FOR EACH ROW EXECUTE FUNCTION "compaction_turn_creation_insert"();

CREATE OR REPLACE FUNCTION "compaction_context_epoch_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_projection_epoch (thread_id
                                           , version)
    SELECT t.thread_id
         , 1
    FROM turn AS t
         INNER JOIN thread AS th ON th.id = t.thread_id
    WHERE t.id = old.turn_id
    ON CONFLICT (thread_id)
       DO UPDATE
         SET version = compaction_projection_epoch.version + 1;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_context_epoch_delete" BEFORE DELETE
ON "turn_llm_context" FOR EACH ROW EXECUTE FUNCTION "compaction_context_epoch_delete"();

CREATE OR REPLACE FUNCTION "compaction_event_epoch_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_projection_epoch (thread_id
                                           , version)
    SELECT t.thread_id
         , 1
    FROM turn AS t
         INNER JOIN thread AS th ON th.id = t.thread_id
    WHERE t.id = old.turn_id
    ON CONFLICT (thread_id)
       DO UPDATE
         SET version = compaction_projection_epoch.version + 1;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_event_epoch_delete" BEFORE DELETE
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "compaction_event_epoch_delete"();

CREATE OR REPLACE FUNCTION "compaction_item_epoch_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.item_type <> 'system_event') IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    INSERT INTO compaction_projection_epoch (thread_id
                                           , version)
    SELECT t.thread_id
         , 1
    FROM turn AS t
         INNER JOIN thread AS th ON th.id = t.thread_id
    WHERE t.id = old.turn_id
    ON CONFLICT (thread_id)
       DO UPDATE
         SET version = compaction_projection_epoch.version + 1;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_item_epoch_delete" BEFORE DELETE
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "compaction_item_epoch_delete"();

CREATE OR REPLACE FUNCTION "compaction_turn_epoch_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_projection_epoch (thread_id
                                           , version)
    SELECT id
         , 1
    FROM thread
    WHERE id = old.thread_id
    ON CONFLICT (thread_id)
       DO UPDATE
         SET version = compaction_projection_epoch.version + 1;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_turn_epoch_delete" BEFORE DELETE
ON "turn" FOR EACH ROW EXECUTE FUNCTION "compaction_turn_epoch_delete"();

CREATE OR REPLACE FUNCTION "compaction_task_basis_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_task_basis_revision (run_id
                                              , revision)
    VALUES (new.run_id, 1)
    ON CONFLICT (run_id)
       DO UPDATE
         SET revision = compaction_task_basis_revision.revision + 1;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_task_basis_insert" AFTER INSERT
ON "task_run_conversation_snapshot" FOR EACH ROW EXECUTE FUNCTION "compaction_task_basis_insert"();

CREATE OR REPLACE FUNCTION "compaction_task_basis_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_task_basis_revision (run_id
                                              , revision)
    VALUES (old.run_id, 2)
    ON CONFLICT (run_id)
       DO UPDATE
         SET revision = compaction_task_basis_revision.revision + 1;

    INSERT INTO compaction_projection_epoch (thread_id
                                           , version)
    SELECT id
         , 1
    FROM thread
    WHERE id = old.conversation_thread_id
    ON CONFLICT (thread_id)
       DO UPDATE
         SET version = compaction_projection_epoch.version + 1;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_task_basis_delete" BEFORE DELETE
ON "task_run_conversation_snapshot" FOR EACH ROW EXECUTE FUNCTION "compaction_task_basis_delete"();

CREATE OR REPLACE FUNCTION "compaction_item_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_item_revision (source_id
                                        , turn_id
                                        , revision
                                        , present)
    VALUES (new.id, new.turn_id, 1, 1)
    ON CONFLICT (source_id)
       DO UPDATE
         SET revision = compaction_item_revision.revision + 1
           , present = 1
           , turn_id = new.turn_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_item_insert" AFTER INSERT
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "compaction_item_insert"();

CREATE OR REPLACE FUNCTION "compaction_item_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_item_revision (source_id
                                        , turn_id
                                        , revision
                                        , present)
    VALUES (old.id, old.turn_id, 2, 0)
    ON CONFLICT (source_id)
       DO UPDATE
         SET revision = compaction_item_revision.revision + 1
           , present = 0;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_item_delete" AFTER DELETE
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "compaction_item_delete"();

CREATE OR REPLACE FUNCTION "compaction_input_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM pg_advisory_xact_lock(1885955182, 1);

    IF ((NEW.payload::text IS DISTINCT FROM OLD.payload::text) OR NEW.text IS DISTINCT FROM OLD.text OR NEW.turn_id IS DISTINCT FROM OLD.turn_id OR NEW.input_type IS DISTINCT FROM OLD.input_type OR NEW.input_index IS DISTINCT FROM OLD.input_index) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_input_revision (source_id
                                         , turn_id
                                         , revision
                                         , present
                                         , capture_order)
    VALUES (new.id
          , new.turn_id
          , 2
          , 1
          , (SELECT COALESCE(max(capture_order)
                           , 0) + 1
             FROM compaction_input_revision))
    ON CONFLICT (source_id)
       DO UPDATE
         SET revision = compaction_input_revision.revision + 1
           , present = 1
           , turn_id = new.turn_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_input_update" AFTER UPDATE OF payload,text,turn_id,input_type,input_index
ON "turn_input" FOR EACH ROW EXECUTE FUNCTION "compaction_input_update"();

CREATE OR REPLACE FUNCTION "compaction_input_epoch_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (((NEW.payload::text IS DISTINCT FROM OLD.payload::text) OR NEW.text IS DISTINCT FROM OLD.text OR NEW.turn_id IS DISTINCT FROM OLD.turn_id OR NEW.input_type IS DISTINCT FROM OLD.input_type OR NEW.input_index IS DISTINCT FROM OLD.input_index) ) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_projection_epoch (thread_id
                                           , version)
    SELECT DISTINCT t.thread_id
                  , 1
    FROM turn AS t
         INNER JOIN thread AS th ON th.id = t.thread_id
    WHERE t.id IN (old.turn_id, new.turn_id)
    ON CONFLICT (thread_id)
       DO UPDATE
         SET version = compaction_projection_epoch.version + 1;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_input_epoch_update" AFTER UPDATE OF payload,text,turn_id,input_type,input_index
ON "turn_input" FOR EACH ROW EXECUTE FUNCTION "compaction_input_epoch_update"();

CREATE OR REPLACE FUNCTION "compaction_source_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM pg_advisory_xact_lock(1885955182, 1);

    IF ((NEW.payload IS DISTINCT FROM OLD.payload) OR NEW.turn_id IS DISTINCT FROM OLD.turn_id OR NEW.source IS DISTINCT FROM OLD.source OR NEW.item_id IS DISTINCT FROM OLD.item_id OR NEW.sequence IS DISTINCT FROM OLD.sequence) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_source_revision (source_id
                                          , turn_id
                                          , revision
                                          , present
                                          , capture_order)
    VALUES (new.id
          , new.turn_id
          , 2
          , 1
          , (SELECT COALESCE(max(capture_order)
                           , 0) + 1
             FROM compaction_source_revision))
    ON CONFLICT (source_id)
       DO UPDATE
         SET revision = compaction_source_revision.revision + 1
           , present = 1
           , turn_id = new.turn_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_source_update" AFTER UPDATE OF payload,turn_id,source,item_id,sequence
ON "turn_llm_context" FOR EACH ROW EXECUTE FUNCTION "compaction_source_update"();

CREATE OR REPLACE FUNCTION "compaction_context_epoch_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (((NEW.payload IS DISTINCT FROM OLD.payload) OR NEW.turn_id IS DISTINCT FROM OLD.turn_id OR NEW.source IS DISTINCT FROM OLD.source OR NEW.item_id IS DISTINCT FROM OLD.item_id OR NEW.sequence IS DISTINCT FROM OLD.sequence) ) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_projection_epoch (thread_id
                                           , version)
    SELECT DISTINCT t.thread_id
                  , 1
    FROM turn AS t
         INNER JOIN thread AS th ON th.id = t.thread_id
    WHERE t.id IN (old.turn_id, new.turn_id)
    ON CONFLICT (thread_id)
       DO UPDATE
         SET version = compaction_projection_epoch.version + 1;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_context_epoch_update" AFTER UPDATE OF payload,turn_id,source,item_id,sequence
ON "turn_llm_context" FOR EACH ROW EXECUTE FUNCTION "compaction_context_epoch_update"();

CREATE OR REPLACE FUNCTION "compaction_new_thread_prepared"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_history_preparation (thread_id
                                              , ready)
    VALUES (new.id, 1);
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_new_thread_prepared" AFTER INSERT
ON "thread" FOR EACH ROW EXECUTE FUNCTION "compaction_new_thread_prepared"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_compaction_operation_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_pending (operation_id
                                            , generation
                                            , eligible_at
                                            , retry_not_before
                                            , due_at
                                            , attempts
                                            , claim_token)
    SELECT o.id
         , q.generation
         , CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END
         , 0
         , GREATEST(0, CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END)
         , 0
         , NULL
    FROM compaction_operation AS o, compaction_lifecycle_sequence AS q
    WHERE o.id = new.id
      AND q.singleton = 1
    ON CONFLICT (operation_id)
       DO UPDATE
         SET generation = excluded.generation
           , eligible_at = excluded.eligible_at
           , due_at = GREATEST(excluded.eligible_at, compaction_lifecycle_pending.retry_not_before)
           , claim_token = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_compaction_operation_insert" AFTER INSERT
ON "compaction_operation" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_compaction_operation_insert"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_compaction_operation_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM compaction_lifecycle_pending
    WHERE operation_id = old.id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_compaction_operation_delete" AFTER DELETE
ON "compaction_operation" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_compaction_operation_delete"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_compaction_operation_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.id IS DISTINCT FROM NEW.id OR OLD.owner IS DISTINCT FROM NEW.owner OR OLD.status IS DISTINCT FROM NEW.status OR OLD.outcome IS DISTINCT FROM NEW.outcome OR OLD.deadline_ms IS DISTINCT FROM NEW.deadline_ms OR OLD.execution_turn IS DISTINCT FROM NEW.execution_turn) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_pending (operation_id
                                            , generation
                                            , eligible_at
                                            , retry_not_before
                                            , due_at
                                            , attempts
                                            , claim_token)
    SELECT o.id
         , q.generation
         , CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END
         , 0
         , GREATEST(0, CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END)
         , 0
         , NULL
    FROM compaction_operation AS o, compaction_lifecycle_sequence AS q
    WHERE o.id = old.id
      AND q.singleton = 1
    ON CONFLICT (operation_id)
       DO UPDATE
         SET generation = excluded.generation
           , eligible_at = excluded.eligible_at
           , due_at = GREATEST(excluded.eligible_at, compaction_lifecycle_pending.retry_not_before)
           , claim_token = NULL;

    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_pending (operation_id
                                            , generation
                                            , eligible_at
                                            , retry_not_before
                                            , due_at
                                            , attempts
                                            , claim_token)
    SELECT o.id
         , q.generation
         , CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END
         , 0
         , GREATEST(0, CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END)
         , 0
         , NULL
    FROM compaction_operation AS o, compaction_lifecycle_sequence AS q
    WHERE o.id = new.id
      AND q.singleton = 1
    ON CONFLICT (operation_id)
       DO UPDATE
         SET generation = excluded.generation
           , eligible_at = excluded.eligible_at
           , due_at = GREATEST(excluded.eligible_at, compaction_lifecycle_pending.retry_not_before)
           , claim_token = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_compaction_operation_update" AFTER UPDATE OF id,owner,status,outcome,deadline_ms,execution_turn
ON "compaction_operation" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_compaction_operation_update"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_compaction_runner_state_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_pending (operation_id
                                            , generation
                                            , eligible_at
                                            , retry_not_before
                                            , due_at
                                            , attempts
                                            , claim_token)
    SELECT o.id
         , q.generation
         , CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END
         , 0
         , GREATEST(0, CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END)
         , 0
         , NULL
    FROM compaction_operation AS o, compaction_lifecycle_sequence AS q
    WHERE o.id = new.operation_id
      AND q.singleton = 1
    ON CONFLICT (operation_id)
       DO UPDATE
         SET generation = excluded.generation
           , eligible_at = excluded.eligible_at
           , due_at = GREATEST(excluded.eligible_at, compaction_lifecycle_pending.retry_not_before)
           , claim_token = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_compaction_runner_state_insert" AFTER INSERT
ON "compaction_runner_state" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_compaction_runner_state_insert"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_compaction_runner_state_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_pending (operation_id
                                            , generation
                                            , eligible_at
                                            , retry_not_before
                                            , due_at
                                            , attempts
                                            , claim_token)
    SELECT o.id
         , q.generation
         , CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END
         , 0
         , GREATEST(0, CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END)
         , 0
         , NULL
    FROM compaction_operation AS o, compaction_lifecycle_sequence AS q
    WHERE o.id = old.operation_id
      AND q.singleton = 1
    ON CONFLICT (operation_id)
       DO UPDATE
         SET generation = excluded.generation
           , eligible_at = excluded.eligible_at
           , due_at = GREATEST(excluded.eligible_at, compaction_lifecycle_pending.retry_not_before)
           , claim_token = NULL;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_compaction_runner_state_delete" AFTER DELETE
ON "compaction_runner_state" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_compaction_runner_state_delete"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_compaction_runner_state_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.operation_id IS DISTINCT FROM NEW.operation_id OR OLD.generation IS DISTINCT FROM NEW.generation OR OLD.state::text IS DISTINCT FROM NEW.state::text) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_pending (operation_id
                                            , generation
                                            , eligible_at
                                            , retry_not_before
                                            , due_at
                                            , attempts
                                            , claim_token)
    SELECT o.id
         , q.generation
         , CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END
         , 0
         , GREATEST(0, CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END)
         , 0
         , NULL
    FROM compaction_operation AS o, compaction_lifecycle_sequence AS q
    WHERE o.id = old.operation_id
      AND q.singleton = 1
    ON CONFLICT (operation_id)
       DO UPDATE
         SET generation = excluded.generation
           , eligible_at = excluded.eligible_at
           , due_at = GREATEST(excluded.eligible_at, compaction_lifecycle_pending.retry_not_before)
           , claim_token = NULL;

    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_pending (operation_id
                                            , generation
                                            , eligible_at
                                            , retry_not_before
                                            , due_at
                                            , attempts
                                            , claim_token)
    SELECT o.id
         , q.generation
         , CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END
         , 0
         , GREATEST(0, CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END)
         , 0
         , NULL
    FROM compaction_operation AS o, compaction_lifecycle_sequence AS q
    WHERE o.id = new.operation_id
      AND q.singleton = 1
    ON CONFLICT (operation_id)
       DO UPDATE
         SET generation = excluded.generation
           , eligible_at = excluded.eligible_at
           , due_at = GREATEST(excluded.eligible_at, compaction_lifecycle_pending.retry_not_before)
           , claim_token = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_compaction_runner_state_update" AFTER UPDATE OF operation_id,generation,state
ON "compaction_runner_state" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_compaction_runner_state_update"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_turn_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'turn'
         , ''
         , new.id
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_turn_insert" AFTER INSERT
ON "turn" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_turn_insert"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_turn_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'turn'
         , ''
         , old.id
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_turn_delete" AFTER DELETE
ON "turn" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_turn_delete"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_turn_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.id IS DISTINCT FROM NEW.id OR OLD.status IS DISTINCT FROM NEW.status OR OLD.thread_id IS DISTINCT FROM NEW.thread_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'turn'
         , ''
         , old.id
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;

    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'turn'
         , ''
         , new.id
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_turn_update" AFTER UPDATE OF id,status,thread_id
ON "turn" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_turn_update"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_compaction_context_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'owner'
         , new.owner
         , ''
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_compaction_context_insert" AFTER INSERT
ON "compaction_context" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_compaction_context_insert"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_compaction_context_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'owner'
         , old.owner
         , ''
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_compaction_context_delete" AFTER DELETE
ON "compaction_context" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_compaction_context_delete"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_compaction_context_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.owner IS DISTINCT FROM NEW.owner OR OLD.workspace_id IS DISTINCT FROM NEW.workspace_id OR OLD.thread_id IS DISTINCT FROM NEW.thread_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'owner'
         , old.owner
         , ''
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;

    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'owner'
         , new.owner
         , ''
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_compaction_context_update" AFTER UPDATE OF owner,workspace_id,thread_id
ON "compaction_context" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_compaction_context_update"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_compaction_execution_stop_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'owner_turn'
         , new.owner
         , new.turn_id
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_compaction_execution_stop_insert" AFTER INSERT
ON "compaction_execution_stop" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_compaction_execution_stop_insert"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_compaction_execution_stop_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'owner_turn'
         , old.owner
         , old.turn_id
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_compaction_execution_stop_delete" AFTER DELETE
ON "compaction_execution_stop" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_compaction_execution_stop_delete"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_compaction_execution_stop_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.owner IS DISTINCT FROM NEW.owner OR OLD.turn_id IS DISTINCT FROM NEW.turn_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'owner_turn'
         , old.owner
         , old.turn_id
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;

    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'owner_turn'
         , new.owner
         , new.turn_id
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_compaction_execution_stop_update" AFTER UPDATE OF owner,turn_id
ON "compaction_execution_stop" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_compaction_execution_stop_update"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_thread_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'thread'
         , ''
         , new.id
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_thread_insert" AFTER INSERT
ON "thread" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_thread_insert"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_thread_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'thread'
         , ''
         , old.id
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_thread_delete" AFTER DELETE
ON "thread" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_thread_delete"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_thread_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.id IS DISTINCT FROM NEW.id OR OLD.workspace_id IS DISTINCT FROM NEW.workspace_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'thread'
         , ''
         , old.id
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;

    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_scope (kind
                                          , owner
                                          , turn_id
                                          , generation
                                          , due_at
                                          , attempts
                                          , claim_token
                                          , cursor_id
                                          , upper_id)
    SELECT 'thread'
         , ''
         , new.id
         , generation
         , 0
         , 0
         , NULL
         , NULL
         , NULL
    FROM compaction_lifecycle_sequence
    WHERE singleton = 1
    ON CONFLICT (kind
               , owner
               , turn_id)
       DO UPDATE
         SET generation = excluded.generation
           , claim_token = NULL
           , cursor_id = NULL
           , upper_id = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_thread_update" AFTER UPDATE OF id,workspace_id
ON "thread" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_thread_update"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_item_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (substr(NEW.item_id,1,11)='compaction:') IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_pending (operation_id
                                            , generation
                                            , eligible_at
                                            , retry_not_before
                                            , due_at
                                            , attempts
                                            , claim_token)
    SELECT o.id
         , q.generation
         , CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END
         , 0
         , GREATEST(0, CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END)
         , 0
         , NULL
    FROM compaction_operation AS o, compaction_lifecycle_sequence AS q
    WHERE o.id = substr(new.item_id, 12)
      AND q.singleton = 1
    ON CONFLICT (operation_id)
       DO UPDATE
         SET generation = excluded.generation
           , eligible_at = excluded.eligible_at
           , due_at = GREATEST(excluded.eligible_at, compaction_lifecycle_pending.retry_not_before)
           , claim_token = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_item_insert" AFTER INSERT
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_item_insert"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_item_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (substr(OLD.item_id,1,11)='compaction:') IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_pending (operation_id
                                            , generation
                                            , eligible_at
                                            , retry_not_before
                                            , due_at
                                            , attempts
                                            , claim_token)
    SELECT o.id
         , q.generation
         , CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END
         , 0
         , GREATEST(0, CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END)
         , 0
         , NULL
    FROM compaction_operation AS o, compaction_lifecycle_sequence AS q
    WHERE o.id = substr(old.item_id, 12)
      AND q.singleton = 1
    ON CONFLICT (operation_id)
       DO UPDATE
         SET generation = excluded.generation
           , eligible_at = excluded.eligible_at
           , due_at = GREATEST(excluded.eligible_at, compaction_lifecycle_pending.retry_not_before)
           , claim_token = NULL;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_item_delete" AFTER DELETE
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_item_delete"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_item_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((substr(OLD.item_id,1,11)='compaction:' OR substr(NEW.item_id,1,11)='compaction:') AND (OLD.id IS DISTINCT FROM NEW.id OR OLD.turn_id IS DISTINCT FROM NEW.turn_id OR OLD.item_id IS DISTINCT FROM NEW.item_id OR OLD.item_type IS DISTINCT FROM NEW.item_type OR OLD.status IS DISTINCT FROM NEW.status)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_pending (operation_id
                                            , generation
                                            , eligible_at
                                            , retry_not_before
                                            , due_at
                                            , attempts
                                            , claim_token)
    SELECT o.id
         , q.generation
         , CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END
         , 0
         , GREATEST(0, CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END)
         , 0
         , NULL
    FROM compaction_operation AS o, compaction_lifecycle_sequence AS q
    WHERE o.id = CASE
                   WHEN substr(old.item_id, 1, 11) = 'compaction:'
                     THEN substr(old.item_id, 12)
                 END
      AND q.singleton = 1
    ON CONFLICT (operation_id)
       DO UPDATE
         SET generation = excluded.generation
           , eligible_at = excluded.eligible_at
           , due_at = GREATEST(excluded.eligible_at, compaction_lifecycle_pending.retry_not_before)
           , claim_token = NULL;

    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_pending (operation_id
                                            , generation
                                            , eligible_at
                                            , retry_not_before
                                            , due_at
                                            , attempts
                                            , claim_token)
    SELECT o.id
         , q.generation
         , CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END
         , 0
         , GREATEST(0, CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END)
         , 0
         , NULL
    FROM compaction_operation AS o, compaction_lifecycle_sequence AS q
    WHERE o.id = CASE
                   WHEN substr(new.item_id, 1, 11) = 'compaction:'
                     THEN substr(new.item_id, 12)
                 END
      AND q.singleton = 1
    ON CONFLICT (operation_id)
       DO UPDATE
         SET generation = excluded.generation
           , eligible_at = excluded.eligible_at
           , due_at = GREATEST(excluded.eligible_at, compaction_lifecycle_pending.retry_not_before)
           , claim_token = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_item_update" AFTER UPDATE OF id,turn_id,item_id,item_type,status
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_item_update"();

CREATE OR REPLACE FUNCTION "compaction_lifecycle_item_payload_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (((substr(OLD.item_id,1,11)='compaction:' OR substr(NEW.item_id,1,11)='compaction:')) AND (NEW.payload::text IS DISTINCT FROM OLD.payload::text OR NEW.payload_payload_id IS DISTINCT FROM OLD.payload_payload_id)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_pending (operation_id
                                            , generation
                                            , eligible_at
                                            , retry_not_before
                                            , due_at
                                            , attempts
                                            , claim_token)
    SELECT o.id
         , q.generation
         , CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END
         , 0
         , GREATEST(0, CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END)
         , 0
         , NULL
    FROM compaction_operation AS o, compaction_lifecycle_sequence AS q
    WHERE o.id = CASE
                   WHEN substr(old.item_id, 1, 11) = 'compaction:'
                     THEN substr(old.item_id, 12)
                 END
      AND q.singleton = 1
    ON CONFLICT (operation_id)
       DO UPDATE
         SET generation = excluded.generation
           , eligible_at = excluded.eligible_at
           , due_at = GREATEST(excluded.eligible_at, compaction_lifecycle_pending.retry_not_before)
           , claim_token = NULL;

    IF NOT EXISTS(SELECT 1 FROM compaction_lifecycle_sequence WHERE singleton=1 AND generation<9223372036854775807) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'compaction lifecycle generation exhausted';
    END IF;

    UPDATE compaction_lifecycle_sequence
    SET generation = generation + 1
    WHERE singleton = 1;

    INSERT INTO compaction_lifecycle_pending (operation_id
                                            , generation
                                            , eligible_at
                                            , retry_not_before
                                            , due_at
                                            , attempts
                                            , claim_token)
    SELECT o.id
         , q.generation
         , CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END
         , 0
         , GREATEST(0, CASE
             WHEN o.status = 'running'
              AND NOT EXISTS (SELECT 1
                              FROM compaction_execution_stop AS s
                              WHERE s.owner = o.owner
                                AND s.turn_id = o.execution_turn)
              AND NOT EXISTS (SELECT 1
                              FROM turn AS t
                              WHERE t.id = o.execution_turn
                                AND t.status IN ('interrupted', 'cancelled'))
               THEN o.deadline_ms
             ELSE 0
           END)
         , 0
         , NULL
    FROM compaction_operation AS o, compaction_lifecycle_sequence AS q
    WHERE o.id = CASE
                   WHEN substr(new.item_id, 1, 11) = 'compaction:'
                     THEN substr(new.item_id, 12)
                 END
      AND q.singleton = 1
    ON CONFLICT (operation_id)
       DO UPDATE
         SET generation = excluded.generation
           , eligible_at = excluded.eligible_at
           , due_at = GREATEST(excluded.eligible_at, compaction_lifecycle_pending.retry_not_before)
           , claim_token = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_lifecycle_item_payload_update" AFTER UPDATE OF payload,payload_payload_id
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "compaction_lifecycle_item_payload_update"();

CREATE OR REPLACE FUNCTION "frozen_expired_monotonic"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.expired=1 AND NEW.expired<>1) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'frozen expiry is irreversible';
END;
$trigger$;

CREATE TRIGGER "frozen_expired_monotonic" BEFORE UPDATE OF expired
ON "compaction_frozen_history" FOR EACH ROW EXECUTE FUNCTION "frozen_expired_monotonic"();

CREATE OR REPLACE FUNCTION "projection_terminal_reset"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    NEW.terminal_generation := NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "projection_terminal_reset" BEFORE UPDATE OF operation_id,manifest_id,identity_sha256,imports_sha256,import_count
ON "compaction_operation_projection" FOR EACH ROW EXECUTE FUNCTION "projection_terminal_reset"();

CREATE OR REPLACE FUNCTION "operation_terminal_reset"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE compaction_operation_projection
    SET terminal_generation = NULL
    WHERE operation_id = old.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "operation_terminal_reset" BEFORE UPDATE OF id,owner,status
ON "compaction_operation" FOR EACH ROW EXECUTE FUNCTION "operation_terminal_reset"();

CREATE OR REPLACE FUNCTION "runner_terminal_reset_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE compaction_operation_projection
    SET terminal_generation = NULL
    WHERE operation_id = old.operation_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "runner_terminal_reset_update" BEFORE UPDATE OF operation_id,generation,state
ON "compaction_runner_state" FOR EACH ROW EXECUTE FUNCTION "runner_terminal_reset_update"();

CREATE OR REPLACE FUNCTION "runner_terminal_reset_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE compaction_operation_projection
    SET terminal_generation = NULL
    WHERE operation_id = new.operation_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "runner_terminal_reset_insert" BEFORE INSERT
ON "compaction_runner_state" FOR EACH ROW EXECUTE FUNCTION "runner_terminal_reset_insert"();

CREATE OR REPLACE FUNCTION "runner_terminal_reset_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE compaction_operation_projection
    SET terminal_generation = NULL
    WHERE operation_id = old.operation_id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "runner_terminal_reset_delete" BEFORE DELETE
ON "compaction_runner_state" FOR EACH ROW EXECUTE FUNCTION "runner_terminal_reset_delete"();

CREATE OR REPLACE FUNCTION "compaction_task_output_available_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count FOR SHARE OF h)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'frozen root unavailable';
END;
$trigger$;

CREATE TRIGGER "compaction_task_output_available_insert" BEFORE INSERT
ON "compaction_task_output" FOR EACH ROW EXECUTE FUNCTION "compaction_task_output_available_insert"();

CREATE OR REPLACE FUNCTION "compaction_task_output_available_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count FOR SHARE OF h)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'frozen root unavailable';
END;
$trigger$;

CREATE TRIGGER "compaction_task_output_available_update" BEFORE UPDATE OF manifest_id
ON "compaction_task_output" FOR EACH ROW EXECUTE FUNCTION "compaction_task_output_available_update"();

CREATE OR REPLACE FUNCTION "compaction_operation_projection_available_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count FOR SHARE OF h)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'frozen root unavailable';
END;
$trigger$;

CREATE TRIGGER "compaction_operation_projection_available_insert" BEFORE INSERT
ON "compaction_operation_projection" FOR EACH ROW EXECUTE FUNCTION "compaction_operation_projection_available_insert"();

CREATE OR REPLACE FUNCTION "compaction_operation_projection_available_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count FOR SHARE OF h)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'frozen root unavailable';
END;
$trigger$;

CREATE TRIGGER "compaction_operation_projection_available_update" BEFORE UPDATE OF manifest_id
ON "compaction_operation_projection" FOR EACH ROW EXECUTE FUNCTION "compaction_operation_projection_available_update"();

-- The caller checks range contents in pages. Under the header lock, compare
-- only the generation that identifies the checked range set.
CREATE FUNCTION frozen_layout_revision_matches(manifest text, family bigint, verified bigint)
RETURNS boolean LANGUAGE sql
AS $revision$
    SELECT EXISTS (
        SELECT 1 FROM compaction_frozen_sweep_consumer c
        WHERE c.manifest_id = manifest AND c.kind = family AND c.revision = verified
    );
$revision$;

CREATE OR REPLACE FUNCTION "frozen_ready_span_revision"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    -- UPDATE already holds the header lock. There are at most two layouts.
    IF OLD.ready IS DISTINCT FROM 1 AND NEW.ready = 1 AND EXISTS (
        SELECT 1 FROM compaction_frozen_layout l
        WHERE l.manifest_id = NEW.id AND l.active = 1
          AND NOT frozen_layout_revision_matches(l.manifest_id, l.kind, l.verified_span_revision)
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'frozen layout verification missing or stale';
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "frozen_ready_span_revision" BEFORE UPDATE OF ready
ON "compaction_frozen_history" FOR EACH ROW EXECUTE FUNCTION "frozen_ready_span_revision"();

CREATE OR REPLACE FUNCTION "layout_available_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    locked_headers text[];
BEGIN
    SELECT ARRAY(
        SELECT h.id FROM compaction_frozen_history h
        WHERE h.id IN (NEW.manifest_id, NEW.candidate)
        ORDER BY h.id FOR NO KEY UPDATE OF h
    ) INTO locked_headers;
    IF NOT (NEW.manifest_id = ANY (locked_headers))
       OR (NEW.candidate IS NOT NULL AND NOT (NEW.candidate = ANY (locked_headers)))
    THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'layout logical dependency unavailable';
    END IF;

    IF NEW.active = 1 THEN
        IF NOT frozen_layout_revision_matches(NEW.manifest_id, NEW.kind, NEW.verified_span_revision) THEN
            RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'frozen layout verification missing or stale';
        END IF;
    END IF;
    NEW.verified_span_revision := NULL;

    IF (NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0) OR (NEW.candidate IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h JOIN compaction_frozen_history c ON c.id=NEW.manifest_id WHERE h.id=NEW.candidate AND h.expired=0 AND h.ready=1 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count AND h.workspace_id=c.workspace_id AND h.owner_thread=c.owner_thread))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'layout logical dependency unavailable';
END;
$trigger$;

CREATE TRIGGER "layout_available_insert" BEFORE INSERT
ON "compaction_frozen_layout" FOR EACH ROW EXECUTE FUNCTION "layout_available_insert"();

CREATE OR REPLACE FUNCTION "layout_available_dependency"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    locked_headers text[];
BEGIN
    SELECT ARRAY(
        SELECT h.id FROM compaction_frozen_history h
        WHERE h.id IN (OLD.manifest_id, OLD.candidate, NEW.manifest_id, NEW.candidate)
        ORDER BY h.id FOR NO KEY UPDATE OF h
    ) INTO locked_headers;
    IF NEW.active <> 1 THEN
        NEW.verified_span_revision := NULL;
    END IF;
    IF (NEW.manifest_id, NEW.kind, NEW.candidate) IS NOT DISTINCT FROM (OLD.manifest_id, OLD.kind, OLD.candidate)
       AND (NEW.active <> 1 OR OLD.active = 1) THEN
        RETURN NEW;
    END IF;
    IF NOT (NEW.manifest_id = ANY (locked_headers))
       OR (NEW.candidate IS NOT NULL AND NOT (NEW.candidate = ANY (locked_headers)))
    THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'layout logical dependency unavailable';
    END IF;

    IF NEW.active = 1
       AND (OLD.active <> 1 OR (OLD.manifest_id, OLD.kind) IS DISTINCT FROM (NEW.manifest_id, NEW.kind))
    THEN
        -- A ready stamp on the old pair cannot certify a different pair.
        IF ((OLD.manifest_id, OLD.kind) IS DISTINCT FROM (NEW.manifest_id, NEW.kind)
            AND NEW.verified_span_revision IS NOT DISTINCT FROM OLD.verified_span_revision)
           OR NOT frozen_layout_revision_matches(NEW.manifest_id, NEW.kind, NEW.verified_span_revision)
        THEN
            RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'frozen layout verification missing or stale';
        END IF;
        NEW.verified_span_revision := NULL;
    END IF;

    IF ((NEW.manifest_id<>OLD.manifest_id OR NEW.kind<>OLD.kind OR NEW.candidate IS DISTINCT FROM OLD.candidate OR (NEW.active=1 AND OLD.active<>1)) AND (NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0) OR (NEW.candidate IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h JOIN compaction_frozen_history c ON c.id=NEW.manifest_id WHERE h.id=NEW.candidate AND h.expired=0 AND h.ready=1 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count AND h.workspace_id=c.workspace_id AND h.owner_thread=c.owner_thread)))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'layout logical dependency unavailable';
END;
$trigger$;

CREATE TRIGGER "layout_available_dependency" BEFORE UPDATE
ON "compaction_frozen_layout" FOR EACH ROW EXECUTE FUNCTION "layout_available_dependency"();

CREATE OR REPLACE FUNCTION "layout_revision_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    -- A cascading delete can run after the header is already gone.
    PERFORM 1 FROM compaction_frozen_history h
    WHERE h.id = OLD.manifest_id
    FOR NO KEY UPDATE OF h;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "layout_revision_delete" BEFORE DELETE
ON "compaction_frozen_layout" FOR EACH ROW EXECUTE FUNCTION "layout_revision_delete"();

CREATE OR REPLACE FUNCTION "span_available_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM 1 FROM compaction_frozen_history h
    WHERE h.id = NEW.manifest_id
    FOR NO KEY UPDATE OF h;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'published span immutable or logical input expired';
    END IF;

    IF (NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0) OR (EXISTS(SELECT 1 FROM compaction_frozen_layout l JOIN compaction_frozen_history h ON h.id=l.manifest_id WHERE l.manifest_id=NEW.manifest_id AND l.kind=NEW.kind AND l.active=1 AND h.ready=1) AND NOT EXISTS(SELECT 1 FROM compaction_frozen_span s WHERE s.manifest_id=NEW.manifest_id AND s.kind=NEW.kind AND s."start"=NEW."start" AND s."end"=NEW."end" AND s.source_manifest=NEW.source_manifest))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'published span immutable or logical input expired';
END;
$trigger$;

CREATE TRIGGER "span_available_insert" BEFORE INSERT
ON "compaction_frozen_span" FOR EACH ROW EXECUTE FUNCTION "span_available_insert"();

CREATE OR REPLACE FUNCTION "span_published_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    locked_headers text[];
BEGIN
    SELECT ARRAY(
        SELECT h.id FROM compaction_frozen_history h
        WHERE h.id IN (OLD.manifest_id, NEW.manifest_id)
        ORDER BY h.id FOR NO KEY UPDATE OF h
    ) INTO locked_headers;
    IF NOT (NEW.manifest_id = ANY (locked_headers))
    THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'span header missing';
    END IF;

    IF (EXISTS(SELECT 1 FROM compaction_frozen_layout l JOIN compaction_frozen_history h ON h.id=l.manifest_id WHERE (l.manifest_id, l.kind) IN ((OLD.manifest_id, OLD.kind), (NEW.manifest_id, NEW.kind)) AND (h.expired=1 OR (l.active=1 AND h.ready=1)))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'published span immutable';
END;
$trigger$;

CREATE TRIGGER "span_published_update" BEFORE UPDATE
ON "compaction_frozen_span" FOR EACH ROW EXECUTE FUNCTION "span_published_update"();

CREATE OR REPLACE FUNCTION "span_published_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM 1 FROM compaction_frozen_history h
    WHERE h.id = OLD.manifest_id
    ORDER BY h.id FOR NO KEY UPDATE OF h;

    IF (EXISTS(SELECT 1 FROM compaction_frozen_layout l JOIN compaction_frozen_history h ON h.id=l.manifest_id WHERE l.manifest_id=OLD.manifest_id AND l.kind=OLD.kind AND l.active=1 AND h.ready=1)) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'published span retained';
END;
$trigger$;

CREATE TRIGGER "span_published_delete" BEFORE DELETE
ON "compaction_frozen_span" FOR EACH ROW EXECUTE FUNCTION "span_published_delete"();

CREATE OR REPLACE FUNCTION "operation_portion_monotonic"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.next_portion<OLD.next_portion) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'candidate portion boundary is monotonic';
END;
$trigger$;

CREATE TRIGGER "operation_portion_monotonic" BEFORE UPDATE OF next_portion
ON "compaction_operation" FOR EACH ROW EXECUTE FUNCTION "operation_portion_monotonic"();

CREATE OR REPLACE FUNCTION "checkpoint_closed_portion_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    boundary bigint;
BEGIN
    -- A runner retry can reuse an existing candidate and write its coverage
    -- next. Lock that checkpoint before the operation in the same order.
    PERFORM 1 FROM compaction_checkpoint p
    WHERE p.operation_id = NEW.operation_id AND p.portion = NEW.portion
    FOR SHARE OF p;

    SELECT o.next_portion INTO boundary
    FROM compaction_operation o WHERE o.id = NEW.operation_id
    FOR SHARE OF o;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'checkpoint operation missing';
    END IF;

    IF NEW.portion < boundary AND NOT EXISTS (
        SELECT 1 FROM compaction_checkpoint
        WHERE id = NEW.id AND operation_id = NEW.operation_id AND owner = NEW.owner
          AND portion = NEW.portion AND identity_sha256 = NEW.identity_sha256
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'candidate portion already closed';
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "checkpoint_closed_portion_insert" BEFORE INSERT
ON "compaction_checkpoint" FOR EACH ROW EXECUTE FUNCTION "checkpoint_closed_portion_insert"();

CREATE OR REPLACE FUNCTION "operation_sealed_identity"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (EXISTS(SELECT 1 FROM compaction_checkpoint WHERE operation_id=OLD.id AND proof_version=1)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'historical operation identity immutable';
END;
$trigger$;

CREATE TRIGGER "operation_sealed_identity" BEFORE UPDATE OF id,owner
ON "compaction_operation" FOR EACH ROW EXECUTE FUNCTION "operation_sealed_identity"();

CREATE OR REPLACE FUNCTION "plan_sealed_readiness"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    operation_row record;
    new_operation_locked boolean := false;
BEGIN
    FOR operation_row IN
        SELECT o.id FROM compaction_operation o
        WHERE o.id IN (OLD.operation_id, NEW.operation_id)
        ORDER BY o.id FOR SHARE OF o
    LOOP
        new_operation_locked := new_operation_locked OR operation_row.id = NEW.operation_id;
    END LOOP;
    IF NOT new_operation_locked THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'compaction operation missing';
    END IF;

    IF ((NEW.operation_id<>OLD.operation_id OR NEW.source_count<>OLD.source_count OR NEW.reference_count<>OLD.reference_count OR NEW.descriptor::text<>OLD.descriptor::text OR NEW.ready<>OLD.ready) AND EXISTS(SELECT 1 FROM compaction_checkpoint WHERE operation_id IN (OLD.operation_id, NEW.operation_id) AND proof_version=1)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed plan readiness immutable';
END;
$trigger$;

CREATE TRIGGER "plan_sealed_readiness" BEFORE UPDATE OF operation_id,source_count,reference_count,descriptor,ready
ON "compaction_runner_plan" FOR EACH ROW EXECUTE FUNCTION "plan_sealed_readiness"();

CREATE OR REPLACE FUNCTION "plan_sealed_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM 1 FROM compaction_operation o
    WHERE o.id = OLD.operation_id
    ORDER BY o.id FOR SHARE OF o;

    IF (EXISTS(SELECT 1 FROM compaction_checkpoint WHERE operation_id=OLD.operation_id AND proof_version=1)) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed plan readiness retained';
END;
$trigger$;

CREATE TRIGGER "plan_sealed_delete" BEFORE DELETE
ON "compaction_runner_plan" FOR EACH ROW EXECUTE FUNCTION "plan_sealed_delete"();

-- Lock the checkpoint by identity before inspecting its version. FOR SHARE
-- conflicts with the seal UPDATE, including when proof_version is still zero.
-- DELETE tolerates a missing parent for cleanup. INSERT requires a locked parent.
CREATE OR REPLACE FUNCTION "compaction_checkpoint_replay_alias_sealed_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    locked_proof_version bigint;
BEGIN
    SELECT p.proof_version INTO locked_proof_version
    FROM compaction_checkpoint p
    WHERE p.id = NEW.checkpoint_id
    FOR SHARE OF p;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'proof checkpoint missing';
    END IF;

    IF locked_proof_version IS DISTINCT FROM 1 THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'checkpoint proofs sealed';
END;
$trigger$;

CREATE TRIGGER "compaction_checkpoint_replay_alias_sealed_insert" BEFORE INSERT
ON "compaction_checkpoint_replay_alias" FOR EACH ROW EXECUTE FUNCTION "compaction_checkpoint_replay_alias_sealed_insert"();

CREATE OR REPLACE FUNCTION "compaction_checkpoint_replay_alias_immutable_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'checkpoint evidence immutable';
END;
$trigger$;

CREATE TRIGGER "compaction_checkpoint_replay_alias_immutable_update" BEFORE UPDATE
ON "compaction_checkpoint_replay_alias" FOR EACH ROW EXECUTE FUNCTION "compaction_checkpoint_replay_alias_immutable_update"();

CREATE OR REPLACE FUNCTION "compaction_checkpoint_replay_alias_sealed_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    locked_proof_version bigint;
BEGIN
    SELECT p.proof_version INTO locked_proof_version
    FROM compaction_checkpoint p
    WHERE p.id = OLD.checkpoint_id
    FOR SHARE OF p;

    IF locked_proof_version IS DISTINCT FROM 1 THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'checkpoint proofs sealed';
END;
$trigger$;

CREATE TRIGGER "compaction_checkpoint_replay_alias_sealed_delete" BEFORE DELETE
ON "compaction_checkpoint_replay_alias" FOR EACH ROW EXECUTE FUNCTION "compaction_checkpoint_replay_alias_sealed_delete"();

CREATE OR REPLACE FUNCTION "compaction_checkpoint_event_input_sealed_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    locked_proof_version bigint;
BEGIN
    SELECT p.proof_version INTO locked_proof_version
    FROM compaction_checkpoint p
    WHERE p.id = NEW.checkpoint_id
    FOR SHARE OF p;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'proof checkpoint missing';
    END IF;

    IF locked_proof_version IS DISTINCT FROM 1 THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'checkpoint proofs sealed';
END;
$trigger$;

CREATE TRIGGER "compaction_checkpoint_event_input_sealed_insert" BEFORE INSERT
ON "compaction_checkpoint_event_input" FOR EACH ROW EXECUTE FUNCTION "compaction_checkpoint_event_input_sealed_insert"();

CREATE OR REPLACE FUNCTION "compaction_checkpoint_event_input_immutable_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'checkpoint evidence immutable';
END;
$trigger$;

CREATE TRIGGER "compaction_checkpoint_event_input_immutable_update" BEFORE UPDATE
ON "compaction_checkpoint_event_input" FOR EACH ROW EXECUTE FUNCTION "compaction_checkpoint_event_input_immutable_update"();

CREATE OR REPLACE FUNCTION "compaction_checkpoint_event_input_sealed_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    locked_proof_version bigint;
BEGIN
    SELECT p.proof_version INTO locked_proof_version
    FROM compaction_checkpoint p
    WHERE p.id = OLD.checkpoint_id
    FOR SHARE OF p;

    IF locked_proof_version IS DISTINCT FROM 1 THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'checkpoint proofs sealed';
END;
$trigger$;

CREATE TRIGGER "compaction_checkpoint_event_input_sealed_delete" BEFORE DELETE
ON "compaction_checkpoint_event_input" FOR EACH ROW EXECUTE FUNCTION "compaction_checkpoint_event_input_sealed_delete"();

CREATE OR REPLACE FUNCTION "compaction_checkpoint_import_sealed_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    locked_proof_version bigint;
BEGIN
    SELECT p.proof_version INTO locked_proof_version
    FROM compaction_checkpoint p
    WHERE p.id = NEW.checkpoint_id
    FOR SHARE OF p;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'proof checkpoint missing';
    END IF;

    IF locked_proof_version IS DISTINCT FROM 1 THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'checkpoint proofs sealed';
END;
$trigger$;

CREATE TRIGGER "compaction_checkpoint_import_sealed_insert" BEFORE INSERT
ON "compaction_checkpoint_import" FOR EACH ROW EXECUTE FUNCTION "compaction_checkpoint_import_sealed_insert"();

CREATE OR REPLACE FUNCTION "compaction_checkpoint_import_immutable_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'checkpoint evidence immutable';
END;
$trigger$;

CREATE TRIGGER "compaction_checkpoint_import_immutable_update" BEFORE UPDATE
ON "compaction_checkpoint_import" FOR EACH ROW EXECUTE FUNCTION "compaction_checkpoint_import_immutable_update"();

CREATE OR REPLACE FUNCTION "compaction_checkpoint_import_sealed_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    locked_proof_version bigint;
BEGIN
    SELECT p.proof_version INTO locked_proof_version
    FROM compaction_checkpoint p
    WHERE p.id = OLD.checkpoint_id
    FOR SHARE OF p;

    IF locked_proof_version IS DISTINCT FROM 1 THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'checkpoint proofs sealed';
END;
$trigger$;

CREATE TRIGGER "compaction_checkpoint_import_sealed_delete" BEFORE DELETE
ON "compaction_checkpoint_import" FOR EACH ROW EXECUTE FUNCTION "compaction_checkpoint_import_sealed_delete"();

CREATE OR REPLACE FUNCTION "checkpoint_sealed_identity"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    operation_row record;
    new_operation_locked boolean := false;
BEGIN
    -- UPDATE/DELETE already holds the checkpoint row.
    FOR operation_row IN
        SELECT o.id, o.next_portion FROM compaction_operation o
        WHERE o.id IN (OLD.operation_id, NEW.operation_id)
        ORDER BY o.id FOR SHARE OF o
    LOOP
        IF operation_row.id = OLD.operation_id AND OLD.portion < operation_row.next_portion THEN
            RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed checkpoint identity immutable';
        END IF;
        IF operation_row.id = NEW.operation_id THEN
            new_operation_locked := true;
            IF (NEW.operation_id, NEW.portion) IS DISTINCT FROM (OLD.operation_id, OLD.portion)
               AND NEW.portion < operation_row.next_portion THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'candidate portion already closed';
            END IF;
        END IF;
    END LOOP;
    IF OLD.proof_version = 1 THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed checkpoint identity immutable';
    END IF;
    IF NOT new_operation_locked THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'checkpoint operation missing';
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "checkpoint_sealed_identity" BEFORE UPDATE OF id,operation_id,owner,previous,portion,summary,selection,projection_version,format_version,identity_sha256
ON "compaction_checkpoint" FOR EACH ROW EXECUTE FUNCTION "checkpoint_sealed_identity"();

CREATE OR REPLACE FUNCTION "checkpoint_sealed_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    operation_row record;
BEGIN
    -- UPDATE/DELETE already holds the checkpoint row.
    FOR operation_row IN
        SELECT o.id, o.next_portion FROM compaction_operation o
        WHERE o.id = OLD.operation_id
        ORDER BY o.id FOR SHARE OF o
    LOOP
        IF operation_row.id = OLD.operation_id AND OLD.portion < operation_row.next_portion THEN
            RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed checkpoint retained';
        END IF;
    END LOOP;
    IF OLD.proof_version = 1 THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed checkpoint retained';
    END IF;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "checkpoint_sealed_delete" BEFORE DELETE
ON "compaction_checkpoint" FOR EACH ROW EXECUTE FUNCTION "checkpoint_sealed_delete"();

CREATE OR REPLACE FUNCTION "checkpoint_published_proof_reset"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    context_workspace text;
BEGIN
    -- The checkpoint is already locked by UPDATE. Acquire context before
    -- operation; read dependencies without locking their rows under operation.
    IF OLD.proof_version = 0 AND NEW.proof_version = 1 THEN
        SELECT c.workspace_id INTO context_workspace
        FROM compaction_context c WHERE c.owner = NEW.owner
        FOR SHARE OF c;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'checkpoint context missing';
        END IF;
    END IF;

    -- Also serialize with manifest ownership and coverage on this operation.
    PERFORM 1 FROM compaction_operation o
    WHERE o.id IN (OLD.operation_id, NEW.operation_id)
    ORDER BY o.id FOR NO KEY UPDATE OF o;

    -- These statements use fresh READ COMMITTED snapshots after any wait.
    IF OLD.proof_version = 0 AND NEW.proof_version = 1 THEN
        -- A ready plan is part of the bound frozen origin. Raw origins have
        -- no projection binding and can be sealed independently of the plan.
        PERFORM 1 FROM compaction_operation_projection b
        JOIN compaction_frozen_history h ON h.id = b.manifest_id
        WHERE b.operation_id = NEW.operation_id
        FOR SHARE OF h;

        IF EXISTS (
            SELECT 1
            FROM compaction_operation_projection b
            LEFT JOIN compaction_frozen_history h ON h.id = b.manifest_id
            LEFT JOIN compaction_runner_plan plan ON plan.operation_id = b.operation_id
            WHERE b.operation_id = NEW.operation_id
              AND (plan.ready = 1
                   AND h.workspace_id = context_workspace
                   AND h.identity_sha256 = b.identity_sha256
                   AND h.imports_sha256 = b.imports_sha256
                   AND h.import_count = b.import_count
                   AND h.message_count >= 0 AND h.import_count >= 0
                   AND h.ready = 1 AND h.expired = 0
                   AND h.next_ordinal = h.message_count AND h.next_import = h.import_count) IS NOT TRUE
        ) THEN
            RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'checkpoint proof dependencies not ready';
        END IF;
    END IF;

    IF (OLD.proof_version=1 AND NEW.proof_version=0 AND (OLD.status='applied' OR EXISTS(SELECT 1 FROM compaction_operation o WHERE o.id=OLD.operation_id AND o.status='completed') OR EXISTS(SELECT 1 FROM compaction_checkpoint p WHERE p.previous=OLD.id) OR EXISTS(SELECT 1 FROM compaction_coverage v JOIN compaction_checkpoint p ON p.id=v.checkpoint_id WHERE v.source_scope='checkpoint:'||OLD.owner AND v.source_id=OLD.id AND v.source_version=OLD.identity_sha256))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'reachable checkpoint proofs retained';
END;
$trigger$;

CREATE TRIGGER "checkpoint_published_proof_reset" BEFORE UPDATE OF proof_version
ON "compaction_checkpoint" FOR EACH ROW EXECUTE FUNCTION "checkpoint_published_proof_reset"();

-- Resolve and lock checkpoint rows before locking their operations. The
-- ARRAY subquery consumes all checkpoint rows before the first operation lock.
-- FOR SHARE returns the current next_portion after waiting for its UPDATE.
-- A LEFT JOIN preserves the proof_version guard during parent cleanup.
CREATE OR REPLACE FUNCTION "checkpoint_coverage_sealed_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    parent_locked boolean;
    sealed boolean;
BEGIN
    WITH locked_checkpoint AS MATERIALIZED (
        SELECT p.id, p.proof_version, p.portion, p.operation_id
        FROM compaction_checkpoint p
        WHERE p.id = NEW.checkpoint_id
        FOR SHARE OF p
    ),
    locked_operation AS MATERIALIZED (
        SELECT o.id, o.next_portion
        FROM compaction_operation o
        WHERE o.id = ANY (ARRAY(SELECT p.operation_id FROM locked_checkpoint p))
        ORDER BY o.id
        FOR SHARE OF o
    )
    SELECT EXISTS (
        SELECT 1 FROM locked_checkpoint p
        JOIN locked_operation o ON o.id = p.operation_id
        WHERE p.id = NEW.checkpoint_id
    ), EXISTS (
        SELECT 1 FROM locked_checkpoint p
        LEFT JOIN locked_operation o ON o.id = p.operation_id
        WHERE p.proof_version = 1 OR p.portion < o.next_portion
    )
    INTO parent_locked, sealed;

    IF NOT parent_locked THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'coverage checkpoint or operation missing';
    END IF;
    IF sealed THEN
        -- Read again in a new statement: the advancing transaction may have
        -- inserted this exact key while the lock request was waiting.
        IF NOT EXISTS (
            SELECT 1 FROM compaction_coverage
            WHERE checkpoint_id = NEW.checkpoint_id
              AND source_scope = NEW.source_scope
              AND source_id = NEW.source_id
              AND source_version = NEW.source_version
        ) THEN
            RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed coverage immutable';
        END IF;
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "checkpoint_coverage_sealed_insert" BEFORE INSERT
ON "compaction_coverage" FOR EACH ROW EXECUTE FUNCTION "checkpoint_coverage_sealed_insert"();

CREATE OR REPLACE FUNCTION "checkpoint_coverage_sealed_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    parent_locked boolean;
    sealed boolean;
BEGIN
    WITH locked_checkpoint AS MATERIALIZED (
        SELECT p.id, p.proof_version, p.portion, p.operation_id
        FROM compaction_checkpoint p
        WHERE p.id IN (OLD.checkpoint_id, NEW.checkpoint_id)
        ORDER BY p.id
        FOR SHARE OF p
    ),
    locked_operation AS MATERIALIZED (
        SELECT o.id, o.next_portion
        FROM compaction_operation o
        WHERE o.id = ANY (ARRAY(SELECT p.operation_id FROM locked_checkpoint p))
        ORDER BY o.id
        FOR SHARE OF o
    )
    SELECT EXISTS (
        SELECT 1 FROM locked_checkpoint p
        JOIN locked_operation o ON o.id = p.operation_id
        WHERE p.id = NEW.checkpoint_id
    ), EXISTS (
        SELECT 1 FROM locked_checkpoint p
        LEFT JOIN locked_operation o ON o.id = p.operation_id
        WHERE p.proof_version = 1 OR p.portion < o.next_portion
    )
    INTO parent_locked, sealed;

    IF NOT parent_locked THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'coverage checkpoint or operation missing';
    END IF;
    IF sealed THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed coverage immutable';
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "checkpoint_coverage_sealed_update" BEFORE UPDATE
ON "compaction_coverage" FOR EACH ROW EXECUTE FUNCTION "checkpoint_coverage_sealed_update"();

CREATE OR REPLACE FUNCTION "checkpoint_coverage_sealed_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (
        WITH locked_checkpoint AS MATERIALIZED (
            SELECT p.proof_version, p.portion, p.operation_id
            FROM compaction_checkpoint p
            WHERE p.id = OLD.checkpoint_id
            FOR SHARE OF p
        ),
        locked_operation AS MATERIALIZED (
            SELECT o.id, o.next_portion
            FROM compaction_operation o
            WHERE o.id = ANY (ARRAY(SELECT p.operation_id FROM locked_checkpoint p))
            ORDER BY o.id
            FOR SHARE OF o
        )
        SELECT EXISTS (
            SELECT 1 FROM locked_checkpoint p
            LEFT JOIN locked_operation o ON o.id = p.operation_id
            WHERE p.proof_version = 1 OR p.portion < o.next_portion
        )
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed coverage immutable';
    END IF;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "checkpoint_coverage_sealed_delete" BEFORE DELETE
ON "compaction_coverage" FOR EACH ROW EXECUTE FUNCTION "checkpoint_coverage_sealed_delete"();

-- Lock operations even when the current snapshot has no matching coverage.
-- The next statement sees coverage and proof changes committed during a wait.
-- Do not acquire checkpoint locks from these triggers: their writers take
-- checkpoint locks before operation locks.
CREATE OR REPLACE FUNCTION "manifest_sealed_ownership_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM 1 FROM compaction_operation o
    WHERE o.id = NEW.operation_id
    FOR NO KEY UPDATE OF o;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'manifest operation missing';
    END IF;

    IF (EXISTS(SELECT 1 FROM compaction_checkpoint p JOIN compaction_coverage v ON v.checkpoint_id=p.id WHERE p.operation_id=NEW.operation_id AND (p.proof_version=1 OR p.portion<(SELECT next_portion FROM compaction_operation WHERE id=p.operation_id)) AND v.source_scope=NEW.source_scope AND v.source_id=NEW.source_id AND v.source_version=NEW.source_version) AND NOT EXISTS(SELECT 1 FROM compaction_manifest m WHERE m.operation_id=NEW.operation_id AND m.ordinal=NEW.ordinal AND m.unit_ordinal=NEW.unit_ordinal AND m.reference_only=NEW.reference_only AND m.source_thread=NEW.source_thread AND m.source_scope=NEW.source_scope AND m.source_id=NEW.source_id AND m.source_version=NEW.source_version)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed historical ownership immutable';
END;
$trigger$;

CREATE TRIGGER "manifest_sealed_ownership_insert" BEFORE INSERT
ON "compaction_manifest" FOR EACH ROW EXECUTE FUNCTION "manifest_sealed_ownership_insert"();

CREATE OR REPLACE FUNCTION "manifest_sealed_ownership_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    operation_row record;
    new_operation_locked boolean := false;
BEGIN
    FOR operation_row IN
        SELECT o.id FROM compaction_operation o
        WHERE o.id IN (OLD.operation_id, NEW.operation_id)
        ORDER BY o.id FOR NO KEY UPDATE OF o
    LOOP
        new_operation_locked := new_operation_locked OR operation_row.id = NEW.operation_id;
    END LOOP;
    IF NOT new_operation_locked THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'manifest operation missing';
    END IF;

    IF (EXISTS(SELECT 1 FROM compaction_checkpoint p JOIN compaction_coverage v ON v.checkpoint_id=p.id WHERE (p.proof_version=1 OR p.portion<(SELECT next_portion FROM compaction_operation WHERE id=p.operation_id)) AND ((p.operation_id=OLD.operation_id AND v.source_scope=OLD.source_scope AND v.source_id=OLD.source_id AND v.source_version=OLD.source_version) OR (p.operation_id=NEW.operation_id AND v.source_scope=NEW.source_scope AND v.source_id=NEW.source_id AND v.source_version=NEW.source_version)))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed historical ownership immutable';
END;
$trigger$;

CREATE TRIGGER "manifest_sealed_ownership_update" BEFORE UPDATE
ON "compaction_manifest" FOR EACH ROW EXECUTE FUNCTION "manifest_sealed_ownership_update"();

CREATE OR REPLACE FUNCTION "manifest_sealed_ownership_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM 1 FROM compaction_operation o
    WHERE o.id = OLD.operation_id
    FOR NO KEY UPDATE OF o;

    IF (EXISTS(SELECT 1 FROM compaction_checkpoint p JOIN compaction_coverage v ON v.checkpoint_id=p.id WHERE p.operation_id=OLD.operation_id AND (p.proof_version=1 OR p.portion<(SELECT next_portion FROM compaction_operation WHERE id=p.operation_id)) AND v.source_scope=OLD.source_scope AND v.source_id=OLD.source_id AND v.source_version=OLD.source_version)) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed historical ownership immutable';
END;
$trigger$;

CREATE TRIGGER "manifest_sealed_ownership_delete" BEFORE DELETE
ON "compaction_manifest" FOR EACH ROW EXECUTE FUNCTION "manifest_sealed_ownership_delete"();

CREATE OR REPLACE FUNCTION "projection_sealed_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM 1 FROM compaction_operation o
    WHERE o.id = NEW.operation_id
    ORDER BY o.id FOR SHARE OF o;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'projection operation missing';
    END IF;

    IF (EXISTS(SELECT 1 FROM compaction_checkpoint WHERE operation_id=NEW.operation_id AND proof_version=1) AND NOT EXISTS(SELECT 1 FROM compaction_operation_projection b WHERE b.operation_id=NEW.operation_id AND b.manifest_id=NEW.manifest_id AND b.identity_sha256=NEW.identity_sha256 AND b.imports_sha256=NEW.imports_sha256 AND b.import_count=NEW.import_count)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed projection binding immutable';
END;
$trigger$;

CREATE TRIGGER "projection_sealed_insert" BEFORE INSERT
ON "compaction_operation_projection" FOR EACH ROW EXECUTE FUNCTION "projection_sealed_insert"();

CREATE OR REPLACE FUNCTION "projection_sealed_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    operation_row record;
    new_operation_locked boolean := false;
BEGIN
    FOR operation_row IN
        SELECT o.id FROM compaction_operation o
        WHERE o.id IN (OLD.operation_id, NEW.operation_id)
        ORDER BY o.id FOR SHARE OF o
    LOOP
        new_operation_locked := new_operation_locked OR operation_row.id = NEW.operation_id;
    END LOOP;
    IF NOT new_operation_locked THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'compaction operation missing';
    END IF;

    IF (EXISTS(SELECT 1 FROM compaction_checkpoint WHERE operation_id IN (OLD.operation_id,NEW.operation_id) AND proof_version=1)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed projection binding immutable';
END;
$trigger$;

CREATE TRIGGER "projection_sealed_update" BEFORE UPDATE OF operation_id,manifest_id,identity_sha256,imports_sha256,import_count
ON "compaction_operation_projection" FOR EACH ROW EXECUTE FUNCTION "projection_sealed_update"();

CREATE OR REPLACE FUNCTION "projection_sealed_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM 1 FROM compaction_operation o
    WHERE o.id = OLD.operation_id
    ORDER BY o.id FOR SHARE OF o;

    IF (EXISTS(SELECT 1 FROM compaction_checkpoint WHERE operation_id=OLD.operation_id AND proof_version=1)) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed projection binding retained';
END;
$trigger$;

CREATE TRIGGER "projection_sealed_delete" BEFORE DELETE
ON "compaction_operation_projection" FOR EACH ROW EXECUTE FUNCTION "projection_sealed_delete"();

CREATE OR REPLACE FUNCTION "frozen_complete_identity"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((OLD.ready=1 OR OLD.expired=1) AND (NEW.id<>OLD.id OR NEW.workspace_id<>OLD.workspace_id OR NEW.owner_thread<>OLD.owner_thread OR NEW.identity_sha256<>OLD.identity_sha256 OR NEW.message_count<>OLD.message_count OR NEW.imports_sha256<>OLD.imports_sha256 OR NEW.import_count<>OLD.import_count OR NEW.next_ordinal<>OLD.next_ordinal OR NEW.next_import<>OLD.next_import OR NEW.ready<>OLD.ready)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'complete frozen identity immutable';
END;
$trigger$;

CREATE TRIGGER "frozen_complete_identity" BEFORE UPDATE OF id,workspace_id,owner_thread,identity_sha256,message_count,imports_sha256,import_count,next_ordinal,next_import,ready
ON "compaction_frozen_history" FOR EACH ROW EXECUTE FUNCTION "frozen_complete_identity"();

CREATE OR REPLACE FUNCTION "frozen_sealed_header_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (EXISTS(SELECT 1 FROM compaction_operation_projection b JOIN compaction_checkpoint p ON p.operation_id=b.operation_id WHERE b.manifest_id=OLD.id AND p.proof_version=1)) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'historical origin identity retained';
END;
$trigger$;

CREATE TRIGGER "frozen_sealed_header_delete" BEFORE DELETE
ON "compaction_frozen_history" FOR EACH ROW EXECUTE FUNCTION "frozen_sealed_header_delete"();

CREATE OR REPLACE FUNCTION "context_sealed_identity"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    -- A row UPDATE already holds the context lock used by checkpoint publication.
    IF (EXISTS(SELECT 1 FROM compaction_checkpoint WHERE owner IN (OLD.owner, NEW.owner) AND proof_version=1)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'historical checkpoint scope immutable';
END;
$trigger$;

CREATE TRIGGER "context_sealed_identity" BEFORE UPDATE OF owner,workspace_id,thread_id,format_version
ON "compaction_context" FOR EACH ROW EXECUTE FUNCTION "context_sealed_identity"();

CREATE OR REPLACE FUNCTION "operation_frozen_admission"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.next_portion>0 AND NOT (
                   OLD.status='failed' AND NEW.status='running' AND NEW.outcome IS NULL
                   AND NEW.fingerprint=OLD.fingerprint
                   AND (
                       (OLD.outcome IS NOT DISTINCT FROM 'deadline' AND NEW.deadline_ms>OLD.deadline_ms
                        AND EXISTS(SELECT 1 FROM compaction_runner_state WHERE operation_id=OLD.id))
                       OR
                       (NEW.execution_turn IS NOT NULL AND OLD.execution_turn IS NOT NULL
                        AND NEW.execution_turn<>OLD.execution_turn
                        AND (NEW.snapshot::jsonb #- '{admission,deadline_ms}')
                            IS NOT DISTINCT FROM (OLD.snapshot::jsonb #- '{admission,deadline_ms}')
                        AND NULLIF(NEW.snapshot::jsonb #> '{admission,deadline_ms}', 'null'::jsonb) IS NOT DISTINCT FROM to_jsonb(NEW.deadline_ms)
                        AND EXISTS(SELECT 1 FROM compaction_runner_state
                            WHERE operation_id=OLD.id
                            AND ((state)::json #>> '{phase,Failed,kind}')='InvalidCompletion'))
                   )
               )) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'prepared admission immutable';
END;
$trigger$;

CREATE TRIGGER "operation_frozen_admission" BEFORE UPDATE OF snapshot,fingerprint
ON "compaction_operation" FOR EACH ROW EXECUTE FUNCTION "operation_frozen_admission"();

CREATE OR REPLACE FUNCTION "task_frozen_roots_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.status NOT IN ('completed','failed','cancelled') AND NOT EXISTS(SELECT 1 FROM task t WHERE t.id=NEW.id AND t.workspace_id=NEW.workspace_id AND t.status NOT IN ('completed','failed','cancelled')) AND EXISTS(SELECT 1 FROM task_run_conversation_snapshot s WHERE s.task_id=NEW.id AND (s.workspace_id<>NEW.workspace_id OR s.frozen_manifest_id='' OR (s.frozen_manifest_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM compaction_frozen_history h WHERE h.id = s.frozen_manifest_id AND h.expired = 0 AND h.ready = 1 FOR SHARE OF h))))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'frozen_input_expired or unverified Task roots';
END;
$trigger$;

CREATE TRIGGER "task_frozen_roots_insert" BEFORE INSERT
ON "task" FOR EACH ROW EXECUTE FUNCTION "task_frozen_roots_insert"();

CREATE OR REPLACE FUNCTION "task_frozen_roots_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.status NOT IN ('completed','failed','cancelled') AND (OLD.status IN ('completed','failed','cancelled') OR OLD.workspace_id<>NEW.workspace_id) AND EXISTS(SELECT 1 FROM task_run_conversation_snapshot s WHERE s.task_id=NEW.id AND (s.workspace_id<>NEW.workspace_id OR s.frozen_manifest_id='' OR (s.frozen_manifest_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM compaction_frozen_history h WHERE h.id = s.frozen_manifest_id AND h.expired = 0 AND h.ready = 1 FOR SHARE OF h))))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'frozen_input_expired or unverified Task roots';
END;
$trigger$;

CREATE TRIGGER "task_frozen_roots_update" BEFORE UPDATE OF status,workspace_id
ON "task" FOR EACH ROW EXECUTE FUNCTION "task_frozen_roots_update"();

CREATE OR REPLACE FUNCTION "html_preview_header_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO html_preview_frozen_index (manifest_id
                                         , workspace_id
                                         , identity_sha256
                                         , message_count
                                         , ready
                                         , expired)
    VALUES (new.id, new.workspace_id, new.identity_sha256, new.message_count, new.ready, new.expired)
    ON CONFLICT (manifest_id)
       DO UPDATE
         SET workspace_id = excluded.workspace_id
           , identity_sha256 = excluded.identity_sha256
           , message_count = excluded.message_count
           , ready = excluded.ready
           , expired = excluded.expired
           , next_ordinal = 0
           , complete = 0
           , unknown = 0;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "html_preview_header_insert" AFTER INSERT
ON "compaction_frozen_history" FOR EACH ROW EXECUTE FUNCTION "html_preview_header_insert"();

CREATE OR REPLACE FUNCTION "html_preview_header_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO html_preview_frozen_index (manifest_id
                                         , workspace_id
                                         , identity_sha256
                                         , message_count
                                         , ready
                                         , expired)
    VALUES (new.id, new.workspace_id, new.identity_sha256, new.message_count, new.ready, new.expired)
    ON CONFLICT (manifest_id)
       DO UPDATE
         SET workspace_id = excluded.workspace_id
           , identity_sha256 = excluded.identity_sha256
           , message_count = excluded.message_count
           , ready = excluded.ready
           , expired = excluded.expired
           , next_ordinal = CASE
                              WHEN old.id = new.id
                               AND old.identity_sha256 = new.identity_sha256
                               AND old.message_count = new.message_count
                                THEN html_preview_frozen_index.next_ordinal
                              ELSE 0
                            END
           , complete = CASE
                          WHEN old.id = new.id
                           AND old.identity_sha256 = new.identity_sha256
                           AND old.message_count = new.message_count
                            THEN html_preview_frozen_index.complete
                          ELSE 0
                        END;

    UPDATE html_preview_frozen_index
    SET expired = 1
    WHERE manifest_id = old.id
      AND old.id <> new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "html_preview_header_update" AFTER UPDATE OF id,workspace_id,identity_sha256,message_count,ready,expired
ON "compaction_frozen_history" FOR EACH ROW EXECUTE FUNCTION "html_preview_header_update"();

CREATE OR REPLACE FUNCTION "html_preview_header_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE html_preview_frozen_index
    SET expired = 1
    WHERE manifest_id = old.id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "html_preview_header_delete" AFTER DELETE
ON "compaction_frozen_history" FOR EACH ROW EXECUTE FUNCTION "html_preview_header_delete"();

CREATE OR REPLACE FUNCTION "html_preview_index_retain"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'HTML preview lifetime marker retained';
END;
$trigger$;

CREATE TRIGGER "html_preview_index_retain" BEFORE DELETE
ON "html_preview_frozen_index" FOR EACH ROW EXECUTE FUNCTION "html_preview_index_retain"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_work_retain"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sweep generations are retained';
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_work_retain" BEFORE DELETE
ON "compaction_frozen_sweep_work" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_work_retain"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_work_identity"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.manifest_id IS DISTINCT FROM OLD.manifest_id OR NEW.kind IS DISTINCT FROM OLD.kind) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sweep generation identity immutable';
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_work_identity" BEFORE UPDATE OF manifest_id,kind
ON "compaction_frozen_sweep_work" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_work_identity"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_consumer_retain"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sweep generations are retained';
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_consumer_retain" BEFORE DELETE
ON "compaction_frozen_sweep_consumer" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_consumer_retain"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_consumer_identity"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.manifest_id, NEW.kind) IS DISTINCT FROM (OLD.manifest_id, OLD.kind) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sweep generation identity immutable';
    END IF;
    IF NEW.revision < OLD.revision THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sweep generation cannot decrease';
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_consumer_identity" BEFORE UPDATE OF manifest_id,kind,revision
ON "compaction_frozen_sweep_consumer" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_consumer_identity"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_consumer_row_identity"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.consumer_row IS DISTINCT FROM OLD.consumer_row) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'consumer service identity immutable';
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_consumer_row_identity" BEFORE UPDATE OF consumer_row
ON "compaction_frozen_sweep_consumer" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_consumer_row_identity"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_work_row_identity"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.work_row IS DISTINCT FROM OLD.work_row) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'work service identity immutable';
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_work_row_identity" BEFORE UPDATE OF work_row
ON "compaction_frozen_sweep_work" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_work_row_identity"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_message_data_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.manifest_id
         , 0
    WHERE new.manifest_id IS NOT NULL
      AND 0 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_message_data_insert" AFTER INSERT
ON "compaction_frozen_message_data" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_message_data_insert"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_message_data_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.manifest_id IS DISTINCT FROM NEW.manifest_id OR OLD.ordinal IS DISTINCT FROM NEW.ordinal OR OLD.reference_json IS DISTINCT FROM NEW.reference_json OR OLD.bytes IS DISTINCT FROM NEW.bytes) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.manifest_id
         , 0
    WHERE old.manifest_id IS NOT NULL
      AND 0 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.manifest_id
         , 0
    WHERE new.manifest_id IS NOT NULL
      AND 0 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_message_data_update" AFTER UPDATE
ON "compaction_frozen_message_data" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_message_data_update"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_import_data_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.manifest_id
         , 1
    WHERE new.manifest_id IS NOT NULL
      AND 1 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_import_data_insert" AFTER INSERT
ON "compaction_frozen_import_data" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_import_data_insert"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_import_data_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.manifest_id IS DISTINCT FROM NEW.manifest_id OR OLD.ordinal IS DISTINCT FROM NEW.ordinal OR OLD.message_ordinal IS DISTINCT FROM NEW.message_ordinal OR OLD.source_scope IS DISTINCT FROM NEW.source_scope OR OLD.source_id IS DISTINCT FROM NEW.source_id OR OLD.source_version IS DISTINCT FROM NEW.source_version OR OLD.source_thread IS DISTINCT FROM NEW.source_thread OR OLD.proof_json IS DISTINCT FROM NEW.proof_json OR OLD.bytes IS DISTINCT FROM NEW.bytes) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.manifest_id
         , 1
    WHERE old.manifest_id IS NOT NULL
      AND 1 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.manifest_id
         , 1
    WHERE new.manifest_id IS NOT NULL
      AND 1 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_import_data_update" AFTER UPDATE
ON "compaction_frozen_import_data" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_import_data_update"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_history_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.id
         , 0
    WHERE new.id IS NOT NULL
      AND 0 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT new.id
         , 0
    WHERE new.id IS NOT NULL
      AND 0 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.id
         , 1
    WHERE new.id IS NOT NULL
      AND 1 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT new.id
         , 1
    WHERE new.id IS NOT NULL
      AND 1 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_history_insert" AFTER INSERT
ON "compaction_frozen_history" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_history_insert"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_history_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.id
         , 0
    WHERE old.id IS NOT NULL
      AND 0 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT old.id
         , 0
    WHERE old.id IS NOT NULL
      AND 0 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.id
         , 1
    WHERE old.id IS NOT NULL
      AND 1 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT old.id
         , 1
    WHERE old.id IS NOT NULL
      AND 1 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_history_delete" AFTER DELETE
ON "compaction_frozen_history" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_history_delete"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_history_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.id IS DISTINCT FROM NEW.id OR OLD.workspace_id IS DISTINCT FROM NEW.workspace_id OR OLD.owner_thread IS DISTINCT FROM NEW.owner_thread OR OLD.identity_sha256 IS DISTINCT FROM NEW.identity_sha256 OR OLD.imports_sha256 IS DISTINCT FROM NEW.imports_sha256 OR OLD.message_count IS DISTINCT FROM NEW.message_count OR OLD.import_count IS DISTINCT FROM NEW.import_count OR OLD.next_ordinal IS DISTINCT FROM NEW.next_ordinal OR OLD.next_import IS DISTINCT FROM NEW.next_import OR OLD.ready IS DISTINCT FROM NEW.ready OR OLD.expired IS DISTINCT FROM NEW.expired OR OLD.storage_registered IS DISTINCT FROM NEW.storage_registered) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.id
         , 0
    WHERE old.id IS NOT NULL
      AND 0 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT old.id
         , 0
    WHERE old.id IS NOT NULL
      AND 0 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.id
         , 1
    WHERE old.id IS NOT NULL
      AND 1 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT old.id
         , 1
    WHERE old.id IS NOT NULL
      AND 1 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.id
         , 0
    WHERE new.id IS NOT NULL
      AND 0 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT new.id
         , 0
    WHERE new.id IS NOT NULL
      AND 0 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.id
         , 1
    WHERE new.id IS NOT NULL
      AND 1 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT new.id
         , 1
    WHERE new.id IS NOT NULL
      AND 1 IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_history_update" AFTER UPDATE
ON "compaction_frozen_history" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_history_update"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_layout_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.manifest_id
         , new.kind
    WHERE new.manifest_id IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT new.manifest_id
         , new.kind
    WHERE new.manifest_id IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.candidate
         , new.kind
    WHERE new.candidate IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_layout_insert" AFTER INSERT
ON "compaction_frozen_layout" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_layout_insert"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_layout_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.manifest_id
         , old.kind
    WHERE old.manifest_id IS NOT NULL
      AND old.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT old.manifest_id
         , old.kind
    WHERE old.manifest_id IS NOT NULL
      AND old.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.candidate
         , old.kind
    WHERE old.candidate IS NOT NULL
      AND old.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_layout_delete" AFTER DELETE
ON "compaction_frozen_layout" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_layout_delete"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_layout_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.manifest_id IS DISTINCT FROM NEW.manifest_id OR OLD.kind IS DISTINCT FROM NEW.kind OR OLD.active IS DISTINCT FROM NEW.active OR OLD.pending IS DISTINCT FROM NEW.pending OR OLD.candidate IS DISTINCT FROM NEW.candidate OR OLD.compared IS DISTINCT FROM NEW.compared OR OLD.copy_to IS DISTINCT FROM NEW.copy_to OR OLD.copy_next IS DISTINCT FROM NEW.copy_next OR OLD.cleanup_to IS DISTINCT FROM NEW.cleanup_to OR OLD.failed IS DISTINCT FROM NEW.failed) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.manifest_id
         , old.kind
    WHERE old.manifest_id IS NOT NULL
      AND old.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT old.manifest_id
         , old.kind
    WHERE old.manifest_id IS NOT NULL
      AND old.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.candidate
         , old.kind
    WHERE old.candidate IS NOT NULL
      AND old.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.manifest_id
         , new.kind
    WHERE new.manifest_id IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT new.manifest_id
         , new.kind
    WHERE new.manifest_id IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.candidate
         , new.kind
    WHERE new.candidate IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_layout_update" AFTER UPDATE
ON "compaction_frozen_layout" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_layout_update"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_span_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.manifest_id
         , new.kind
    WHERE new.manifest_id IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT new.manifest_id
         , new.kind
    WHERE new.manifest_id IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.source_manifest
         , new.kind
    WHERE new.source_manifest IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_span_insert" AFTER INSERT
ON "compaction_frozen_span" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_span_insert"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_span_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.manifest_id
         , old.kind
    WHERE old.manifest_id IS NOT NULL
      AND old.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT old.manifest_id
         , old.kind
    WHERE old.manifest_id IS NOT NULL
      AND old.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.source_manifest
         , old.kind
    WHERE old.source_manifest IS NOT NULL
      AND old.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_span_delete" AFTER DELETE
ON "compaction_frozen_span" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_span_delete"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_span_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.manifest_id IS DISTINCT FROM NEW.manifest_id OR OLD.kind IS DISTINCT FROM NEW.kind OR OLD."start" IS DISTINCT FROM NEW."start" OR OLD."end" IS DISTINCT FROM NEW."end" OR OLD.source_manifest IS DISTINCT FROM NEW.source_manifest) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.manifest_id
         , old.kind
    WHERE old.manifest_id IS NOT NULL
      AND old.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT old.manifest_id
         , old.kind
    WHERE old.manifest_id IS NOT NULL
      AND old.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT old.source_manifest
         , old.kind
    WHERE old.source_manifest IS NOT NULL
      AND old.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.manifest_id
         , new.kind
    WHERE new.manifest_id IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT new.manifest_id
         , new.kind
    WHERE new.manifest_id IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;

    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.source_manifest
         , new.kind
    WHERE new.source_manifest IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_span_update" AFTER UPDATE
ON "compaction_frozen_span" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_span_update"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_cleanup_bounds"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((OLD.cleanup_next<0 OR OLD.cleanup_next>OLD.cleanup_to OR NEW.cleanup_next<0 OR NEW.cleanup_next>NEW.cleanup_to) AND OLD.cleanup_next IS DISTINCT FROM NEW.cleanup_next) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_frozen_sweep_work (manifest_id
                                            , kind)
    SELECT new.manifest_id
         , new.kind
    WHERE new.manifest_id IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_work.revision + 1
           , pending = 1
           , retry_at = 0;

    INSERT INTO compaction_frozen_sweep_consumer (manifest_id
                                                , kind)
    SELECT new.manifest_id
         , new.kind
    WHERE new.manifest_id IS NOT NULL
      AND new.kind IN (0, 1)
    ON CONFLICT (manifest_id
               , kind)
       DO UPDATE
         SET revision = compaction_frozen_sweep_consumer.revision + 1
           , pending = 1
           , after_source = NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_cleanup_bounds" AFTER UPDATE OF cleanup_next
ON "compaction_frozen_layout" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_cleanup_bounds"();

CREATE OR REPLACE FUNCTION "compaction_root_human_input"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.origin='user' AND NEW.turn_kind='conversation'
            AND NEW.initiated_by_actor_kind='principal'
            AND EXISTS(SELECT 1 FROM thread h WHERE h.id=NEW.thread_id AND h.origin_kind IN ('user','collaborative','direct_message'))
            AND NOT EXISTS(SELECT 1 FROM thread_lineage WHERE child_thread_id=NEW.thread_id)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_context (owner
                                  , workspace_id
                                  , thread_id)
    SELECT 'native:' || octet_length(h.workspace_id) || ':' || h.workspace_id || ':' || octet_length(h.id) || ':' || h.id
         , h.workspace_id
         , h.id
    FROM thread AS h
         INNER JOIN compaction_root_activity AS a ON a.thread_id = h.id
    WHERE h.id = new.thread_id
    ON CONFLICT (owner) DO NOTHING;

    INSERT INTO compaction_execution_stop (owner
                                         , turn_id)
    SELECT c.owner
         , a.user_turn_id
    FROM compaction_root_activity AS a
         INNER JOIN compaction_context AS c ON c.thread_id = a.thread_id
         INNER JOIN thread AS h ON h.id = a.thread_id
                               AND c.workspace_id = h.workspace_id
    WHERE a.thread_id = new.thread_id
      AND a.user_turn_id <> new.id
      AND c.owner = 'native:' || octet_length(h.workspace_id) || ':' || h.workspace_id || ':' || octet_length(h.id) || ':' || h.id
    ON CONFLICT (owner
               , turn_id) DO NOTHING;

    UPDATE compaction_history_check
    SET state = 'finished'
      , outcome = 'cancelled'
      , revision = revision + 1
      , attempt_deadline_ms = NULL
    WHERE turn_id = (SELECT user_turn_id
                     FROM compaction_root_activity
                     WHERE thread_id = new.thread_id)
      AND turn_id <> new.id
      AND purpose = 'root_conversation'
      AND state = 'pending';

    INSERT INTO compaction_root_activity (thread_id
                                        , user_turn_id
                                        , last_user_activity_ms)
    VALUES (new.thread_id
          , new.id
          , CAST(floor(EXTRACT(EPOCH FROM new.created_at) * 1000) AS bigint))
    ON CONFLICT (thread_id)
       DO UPDATE
         SET user_turn_id = new.id
           , last_user_activity_ms = GREATEST(compaction_root_activity.last_user_activity_ms
                                            , CAST(floor(EXTRACT(EPOCH FROM new.created_at) * 1000) AS bigint));
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_root_human_input" AFTER INSERT
ON "turn" FOR EACH ROW EXECUTE FUNCTION "compaction_root_human_input"();

CREATE OR REPLACE FUNCTION "compaction_root_answer_completed"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.status='completed' AND OLD.status<>'completed'
            AND NEW.origin='user' AND NEW.turn_kind='conversation'
            AND NEW.initiated_by_actor_kind='principal' AND NEW.work_owner='turn'
            AND EXISTS(SELECT 1 FROM turn_item i WHERE i.turn_id=NEW.id
                AND i.item_type='agent_message' AND i.status='completed')
            AND EXISTS(SELECT 1 FROM turn_event e WHERE e.turn_id=NEW.id AND e.event_type='turn/completed')) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_root_activity (thread_id
                                        , user_turn_id
                                        , last_user_activity_ms)
    SELECT h.id
         , new.id
         , CAST(floor(EXTRACT(EPOCH FROM e.created_at) * 1000) AS bigint)
    FROM thread AS h
         INNER JOIN turn_event AS e ON e.turn_id = new.id
                                   AND e.thread_id = h.id
                                   AND e.event_type = 'turn/completed'
    WHERE h.id = new.thread_id
      AND h.origin_kind IN ('user', 'collaborative', 'direct_message')
      AND NOT EXISTS (SELECT 1
                      FROM thread_lineage AS l
                      WHERE l.child_thread_id = h.id)
    ORDER BY CAST(floor(EXTRACT(EPOCH FROM e.created_at) * 1000) AS bigint) DESC
           , e.id DESC
    LIMIT 1
    ON CONFLICT (thread_id)
       DO UPDATE
         SET last_user_activity_ms = GREATEST(compaction_root_activity.last_user_activity_ms, excluded.last_user_activity_ms);

    INSERT INTO compaction_history_check (turn_id
                                        , state
                                        , managed
                                        , purpose)
    SELECT a.user_turn_id
         , 'pending'
         , 1
         , 'root_conversation'
    FROM compaction_root_activity AS a
    WHERE a.thread_id = new.thread_id
    ON CONFLICT (turn_id)
       DO UPDATE
         SET state = 'pending'
           , outcome = NULL
           , revision = compaction_history_check.revision + 1
           , failures = 0
           , next_attempt_ms = 0
           , attempt_deadline_ms = NULL
           , diagnostic = NULL
           , config_hash = NULL
           , managed = 1
           , purpose = 'root_conversation';
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_root_answer_completed" AFTER UPDATE OF status
ON "turn" FOR EACH ROW EXECUTE FUNCTION "compaction_root_answer_completed"();

CREATE OR REPLACE FUNCTION "compaction_root_task_delivered"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.status='delivered' AND OLD.status<>'delivered') IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_root_activity (thread_id
                                        , user_turn_id
                                        , last_user_activity_ms)
    SELECT h.id
         , k.created_by_turn_id
         , max(CAST(floor(EXTRACT(EPOCH FROM acknowledgement.created_at) * 1000) AS bigint))
    FROM task_delivery AS d
         INNER JOIN thread AS h ON h.id = d.target_thread_id
                               AND h.workspace_id = d.workspace_id
         INNER JOIN compaction_delivery_output AS b ON b.delivery_id = d.id
         INNER JOIN compaction_task_output AS s ON s.task_run_turn_id = b.task_run_turn_id
                                               AND s.task_id = d.task_id
                                               AND s.run_id = d.run_id
                                               AND s.workspace_id = d.workspace_id
         INNER JOIN task_result_candidate AS c ON c.id = b.candidate_id
                                              AND c.status = 'accepted'
                                              AND c.task_run_turn_id = s.task_run_turn_id
                                              AND c.task_id = s.task_id
                                              AND c.run_id = s.run_id
                                              AND c.thread_id = s.source_thread
                                              AND c.turn_id = s.source_turn
         INNER JOIN task AS k ON k.id = d.task_id
                             AND k.workspace_id = d.workspace_id
         INNER JOIN turn_event AS acknowledgement ON acknowledgement.turn_id = d.delivered_turn_id
                                                 AND acknowledgement.thread_id = h.id
                                                 AND acknowledgement.event_type = 'item/completed'
         INNER JOIN compaction_event_revision AS receipt ON receipt.source_id = acknowledgement.id
                                                        AND receipt.turn_id = acknowledgement.turn_id
                                                        AND receipt.present = 1
                                                        AND receipt.projection_revision = receipt.revision
                                                        AND receipt.item_id = 'task_delivery_result_' || d.id
         INNER JOIN turn AS published ON published.id = d.delivered_turn_id
                                     AND published.thread_id = h.id
    WHERE d.mode = 'thread'
      AND d.status = 'delivered'
      AND d.delivered_at IS NOT NULL
      AND d.error_snapshot_json IS NULL
      AND h.origin_kind IN ('user', 'collaborative', 'direct_message')
      AND NOT EXISTS (SELECT 1
                      FROM thread_lineage AS l
                      WHERE l.child_thread_id = h.id)
      AND EXISTS (SELECT 1
                  FROM turn_item AS i
                  WHERE i.turn_id = published.id
                    AND i.item_id = 'task_delivery_result_' || d.id
                    AND i.status = 'completed')
      AND d.id = new.id
      AND k.metadata_json IS JSON
      AND (CASE WHEN k.metadata_json IS JSON THEN k.metadata_json::jsonb END
           #> '{composerWork,version}') = '1'::jsonb
      AND CAST(CASE
                 WHEN k.metadata_json IS JSON
                   THEN k.metadata_json
                 ELSE '{}'
               END AS pg_catalog.json) #>> '{composerWork,launch,thread_id}' = h.id
      AND EXISTS (SELECT 1
                  FROM turn AS human
                  WHERE human.id = CAST(CASE
                                          WHEN k.metadata_json IS JSON
                                            THEN k.metadata_json
                                          ELSE '{}'
                                        END AS pg_catalog.json) #>> '{composerWork,launch,turn_id}'
                    AND human.thread_id = h.id
                    AND human.origin = 'user'
                    AND human.turn_kind = 'conversation'
                    AND human.initiated_by_actor_kind = 'principal'
                    AND human.work_owner = 'detached_task'
                    AND k.created_by_turn_id = human.id
                    AND k.created_by_thread_id = h.id)
    GROUP BY h.id, k.created_by_turn_id
    ON CONFLICT (thread_id)
       DO UPDATE
         SET last_user_activity_ms = GREATEST(compaction_root_activity.last_user_activity_ms, excluded.last_user_activity_ms);

    INSERT INTO compaction_history_check (turn_id
                                        , state
                                        , managed
                                        , purpose)
    SELECT a.user_turn_id
         , 'pending'
         , 1
         , 'root_conversation'
    FROM compaction_root_activity AS a
    WHERE a.thread_id = new.target_thread_id
      AND EXISTS (SELECT 1
                  FROM task_delivery AS d
                       INNER JOIN thread AS h ON h.id = d.target_thread_id
                                             AND h.workspace_id = d.workspace_id
                       INNER JOIN compaction_delivery_output AS b ON b.delivery_id = d.id
                       INNER JOIN compaction_task_output AS s ON s.task_run_turn_id = b.task_run_turn_id
                                                             AND s.task_id = d.task_id
                                                             AND s.run_id = d.run_id
                                                             AND s.workspace_id = d.workspace_id
                       INNER JOIN task_result_candidate AS c ON c.id = b.candidate_id
                                                            AND c.status = 'accepted'
                                                            AND c.task_run_turn_id = s.task_run_turn_id
                                                            AND c.task_id = s.task_id
                                                            AND c.run_id = s.run_id
                                                            AND c.thread_id = s.source_thread
                                                            AND c.turn_id = s.source_turn
                       INNER JOIN task AS k ON k.id = d.task_id
                                           AND k.workspace_id = d.workspace_id
                       INNER JOIN turn_event AS acknowledgement ON acknowledgement.turn_id = d.delivered_turn_id
                                                               AND acknowledgement.thread_id = h.id
                                                               AND acknowledgement.event_type = 'item/completed'
                       INNER JOIN compaction_event_revision AS receipt ON receipt.source_id = acknowledgement.id
                                                                      AND receipt.turn_id = acknowledgement.turn_id
                                                                      AND receipt.present = 1
                                                                      AND receipt.projection_revision = receipt.revision
                                                                      AND receipt.item_id = 'task_delivery_result_' || d.id
                       INNER JOIN turn AS published ON published.id = d.delivered_turn_id
                                                   AND published.thread_id = h.id
                  WHERE d.mode = 'thread'
                    AND d.status = 'delivered'
                    AND d.delivered_at IS NOT NULL
                    AND d.error_snapshot_json IS NULL
                    AND h.origin_kind IN ('user', 'collaborative', 'direct_message')
                    AND NOT EXISTS (SELECT 1
                                    FROM thread_lineage AS l
                                    WHERE l.child_thread_id = h.id)
                    AND EXISTS (SELECT 1
                                FROM turn_item AS i
                                WHERE i.turn_id = published.id
                                  AND i.item_id = 'task_delivery_result_' || d.id
                                  AND i.status = 'completed')
                    AND d.id = new.id)
    ON CONFLICT (turn_id)
       DO UPDATE
         SET state = 'pending'
           , outcome = NULL
           , revision = compaction_history_check.revision + 1
           , failures = 0
           , next_attempt_ms = 0
           , attempt_deadline_ms = NULL
           , diagnostic = NULL
           , config_hash = NULL
           , managed = 1
           , purpose = 'root_conversation';
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_root_task_delivered" AFTER UPDATE OF status
ON "task_delivery" FOR EACH ROW EXECUTE FUNCTION "compaction_root_task_delivered"();

CREATE OR REPLACE FUNCTION "compaction_task_basis_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (OLD.history_json_payload_id IS DISTINCT FROM NEW.history_json_payload_id OR OLD.history_json IS DISTINCT FROM NEW.history_json OR OLD.run_id IS DISTINCT FROM NEW.run_id OR OLD.task_id IS DISTINCT FROM NEW.task_id OR OLD.workspace_id IS DISTINCT FROM NEW.workspace_id OR OLD.conversation_thread_id IS DISTINCT FROM NEW.conversation_thread_id OR OLD.source_turn_id IS DISTINCT FROM NEW.source_turn_id OR OLD.created_at IS DISTINCT FROM NEW.created_at OR OLD.frozen_manifest_id IS DISTINCT FROM NEW.frozen_manifest_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_task_basis_revision (run_id
                                              , revision)
    VALUES (old.run_id, 2)
    ON CONFLICT (run_id)
       DO UPDATE
         SET revision = compaction_task_basis_revision.revision + 1;

    INSERT INTO compaction_projection_epoch (thread_id
                                           , version)
    SELECT id
         , 1
    FROM thread
    WHERE id IN (old.conversation_thread_id, new.conversation_thread_id)
    ON CONFLICT (thread_id)
       DO UPDATE
         SET version = compaction_projection_epoch.version + 1;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_task_basis_update" AFTER UPDATE OF history_json,history_json_payload_id,run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,created_at,frozen_manifest_id
ON "task_run_conversation_snapshot" FOR EACH ROW EXECUTE FUNCTION "compaction_task_basis_update"();

CREATE OR REPLACE FUNCTION "compaction_event_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM pg_advisory_xact_lock(1885955182, 1);

    IF ((NEW.payload_payload_id IS DISTINCT FROM OLD.payload_payload_id OR NEW.payload::text IS DISTINCT FROM OLD.payload::text) OR NEW.turn_id IS DISTINCT FROM OLD.turn_id OR NEW.event_type IS DISTINCT FROM OLD.event_type OR NEW.sequence IS DISTINCT FROM OLD.sequence) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_event_revision (source_id
                                         , turn_id
                                         , revision
                                         , present
                                         , capture_order)
    VALUES (new.id
          , new.turn_id
          , 2
          , 1
          , (SELECT COALESCE(max(capture_order)
                           , 0) + 1
             FROM compaction_event_revision))
    ON CONFLICT (source_id)
       DO UPDATE
         SET revision = compaction_event_revision.revision + 1
           , present = 1
           , turn_id = new.turn_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_event_update" AFTER UPDATE OF payload,payload_payload_id,turn_id,event_type,sequence
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "compaction_event_update"();

CREATE OR REPLACE FUNCTION "compaction_event_epoch_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (((NEW.payload_payload_id IS DISTINCT FROM OLD.payload_payload_id OR NEW.payload::text IS DISTINCT FROM OLD.payload::text) OR NEW.turn_id IS DISTINCT FROM OLD.turn_id OR NEW.event_type IS DISTINCT FROM OLD.event_type OR NEW.sequence IS DISTINCT FROM OLD.sequence) ) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_projection_epoch (thread_id
                                           , version)
    SELECT DISTINCT t.thread_id
                  , 1
    FROM turn AS t
         INNER JOIN thread AS th ON th.id = t.thread_id
    WHERE t.id IN (old.turn_id, new.turn_id)
    ON CONFLICT (thread_id)
       DO UPDATE
         SET version = compaction_projection_epoch.version + 1;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_event_epoch_update" AFTER UPDATE OF payload,payload_payload_id,turn_id,event_type,sequence
ON "turn_event" FOR EACH ROW EXECUTE FUNCTION "compaction_event_epoch_update"();

CREATE OR REPLACE FUNCTION "compaction_item_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF ((NEW.payload_payload_id IS DISTINCT FROM OLD.payload_payload_id OR NEW.payload::text IS DISTINCT FROM OLD.payload::text) OR NEW.turn_id IS DISTINCT FROM OLD.turn_id OR NEW.item_id IS DISTINCT FROM OLD.item_id OR NEW.item_type IS DISTINCT FROM OLD.item_type OR NEW.status IS DISTINCT FROM OLD.status) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_item_revision (source_id
                                        , turn_id
                                        , revision
                                        , present)
    VALUES (new.id, new.turn_id, 2, 1)
    ON CONFLICT (source_id)
       DO UPDATE
         SET revision = compaction_item_revision.revision + 1
           , present = 1
           , turn_id = new.turn_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_item_update" AFTER UPDATE OF payload,payload_payload_id,turn_id,item_id,item_type,status
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "compaction_item_update"();

CREATE OR REPLACE FUNCTION "compaction_item_epoch_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (((NEW.payload_payload_id IS DISTINCT FROM OLD.payload_payload_id OR NEW.payload::text IS DISTINCT FROM OLD.payload::text) OR NEW.turn_id IS DISTINCT FROM OLD.turn_id OR NEW.item_id IS DISTINCT FROM OLD.item_id OR NEW.item_type IS DISTINCT FROM OLD.item_type OR NEW.status IS DISTINCT FROM OLD.status) AND (OLD.item_type='user_message' OR (OLD.item_type IN ('command_execution','file_change','web_search','web_fetch','download','dynamic_tool_call') AND (OLD.status IS NULL OR OLD.status IN ('completed','failed'))))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    INSERT INTO compaction_projection_epoch (thread_id
                                           , version)
    SELECT DISTINCT t.thread_id
                  , 1
    FROM turn AS t
         INNER JOIN thread AS th ON th.id = t.thread_id
    WHERE t.id IN (old.turn_id, new.turn_id)
    ON CONFLICT (thread_id)
       DO UPDATE
         SET version = compaction_projection_epoch.version + 1;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_item_epoch_update" AFTER UPDATE OF payload,payload_payload_id,turn_id,item_id,item_type,status
ON "turn_item" FOR EACH ROW EXECUTE FUNCTION "compaction_item_epoch_update"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_work_allocate"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    existing_row bigint;
    frontier bigint;
BEGIN
    -- Serialize through commit so a later writer cannot publish a lower row.
    -- The BEFORE trigger sees preceding inserts of this statement.
    PERFORM pg_advisory_xact_lock(1885955182, 2);

    SELECT q.work_row INTO existing_row
    FROM compaction_frozen_sweep_work q
    WHERE q.manifest_id = NEW.manifest_id AND q.kind = NEW.kind;

    IF FOUND THEN
        -- Leave conflict handling to INSERT, even at the bigint limit.
        NEW.work_row := COALESCE(NEW.work_row, existing_row);
        RETURN NEW;
    END IF;

    SELECT COALESCE(MAX(q.work_row), 0) INTO frontier
    FROM compaction_frozen_sweep_work q;

    IF NEW.work_row IS NULL THEN
        IF frontier = 9223372036854775807 THEN
            RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'work service identity exhausted';
        END IF;
        NEW.work_row := frontier + 1;
    ELSIF NEW.work_row <= frontier THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'work service identity exhausted';
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_work_allocate" BEFORE INSERT
ON "compaction_frozen_sweep_work" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_work_allocate"();

CREATE OR REPLACE FUNCTION "compaction_frozen_sweep_consumer_allocate"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    existing_row bigint;
    frontier bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(1885955182, 2);

    IF NEW.consumer_row IS NOT NULL THEN
        RETURN NEW;
    END IF;

    SELECT q.consumer_row INTO existing_row
    FROM compaction_frozen_sweep_consumer q
    WHERE q.manifest_id = NEW.manifest_id AND q.kind = NEW.kind;

    IF FOUND THEN
        -- Leave conflict handling to INSERT, even at the bigint limit.
        NEW.consumer_row := existing_row;
        RETURN NEW;
    END IF;

    SELECT COALESCE(MAX(q.consumer_row), 0) INTO frontier
    FROM compaction_frozen_sweep_consumer q;

    IF frontier = 9223372036854775807 THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'consumer service identity exhausted';
    END IF;
    NEW.consumer_row := frontier + 1;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "compaction_frozen_sweep_consumer_allocate" BEFORE INSERT
ON "compaction_frozen_sweep_consumer" FOR EACH ROW EXECUTE FUNCTION "compaction_frozen_sweep_consumer_allocate"();
