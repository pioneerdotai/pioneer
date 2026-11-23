-- Keep the thread search directory count in the transaction that changes eligibility.

CREATE OR REPLACE FUNCTION "episodic_directory_count_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (new.status='active' AND new.search_text IS NOT NULL) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    -- Use the same lock as directory initialization for this thread.
    PERFORM pg_advisory_xact_lock(hashtextextended(
        jsonb_build_array(NEW.workspace_id, NEW.thread_id)::text, 1885955182));

    UPDATE thread_episodic_thread_directory
    SET indexed_item_count = indexed_item_count + 1
    WHERE workspace_id = new.workspace_id
      AND thread_id = new.thread_id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "episodic_directory_count_insert" AFTER INSERT
ON "thread_episodic_items" FOR EACH ROW EXECUTE FUNCTION "episodic_directory_count_insert"();

CREATE OR REPLACE FUNCTION "episodic_directory_count_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (old.status='active' AND old.search_text IS NOT NULL) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        jsonb_build_array(OLD.workspace_id, OLD.thread_id)::text, 1885955182));

    UPDATE thread_episodic_thread_directory
    SET indexed_item_count = indexed_item_count - 1
    WHERE workspace_id = old.workspace_id
      AND thread_id = old.thread_id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "episodic_directory_count_delete" AFTER DELETE
ON "thread_episodic_items" FOR EACH ROW EXECUTE FUNCTION "episodic_directory_count_delete"();

CREATE OR REPLACE FUNCTION "episodic_directory_count_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    old_key bigint := hashtextextended(
        jsonb_build_array(OLD.workspace_id, OLD.thread_id)::text, 1885955182);
    new_key bigint := hashtextextended(
        jsonb_build_array(NEW.workspace_id, NEW.thread_id)::text, 1885955182);
BEGIN
    IF (((old.status='active' AND old.search_text IS NOT NULL) IS DISTINCT FROM (new.status='active' AND new.search_text IS NOT NULL)) OR old.workspace_id IS DISTINCT FROM new.workspace_id OR old.thread_id IS DISTINCT FROM new.thread_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    -- Moves acquire the two thread locks in the same order.
    PERFORM pg_advisory_xact_lock(LEAST(old_key, new_key));
    IF old_key <> new_key THEN
        PERFORM pg_advisory_xact_lock(GREATEST(old_key, new_key));
    END IF;

    UPDATE thread_episodic_thread_directory
    SET indexed_item_count = indexed_item_count - 1
    WHERE workspace_id = old.workspace_id
      AND thread_id = old.thread_id
      AND old.status = 'active'
      AND old.search_text IS NOT NULL;

    UPDATE thread_episodic_thread_directory
    SET indexed_item_count = indexed_item_count + 1
    WHERE workspace_id = new.workspace_id
      AND thread_id = new.thread_id
      AND new.status = 'active'
      AND new.search_text IS NOT NULL;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "episodic_directory_count_update" AFTER UPDATE OF status,search_text,workspace_id,thread_id
ON "thread_episodic_items" FOR EACH ROW EXECUTE FUNCTION "episodic_directory_count_update"();

CREATE OR REPLACE FUNCTION "episodic_directory_count_initialize"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        jsonb_build_array(NEW.workspace_id, NEW.thread_id)::text, 1885955182));

    UPDATE thread_episodic_thread_directory
    SET indexed_item_count = (SELECT count(*)
                              FROM thread_episodic_items AS i
                              WHERE i.workspace_id = new.workspace_id
                                AND i.thread_id = new.thread_id
                                AND i.status = 'active'
                                AND i.search_text IS NOT NULL)
    WHERE id = new.id;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "episodic_directory_count_initialize" AFTER INSERT
ON "thread_episodic_thread_directory" FOR EACH ROW EXECUTE FUNCTION "episodic_directory_count_initialize"();
