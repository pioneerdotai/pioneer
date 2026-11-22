CREATE FUNCTION compaction_projection_epoch_lock_capture_order()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    -- Source/event/input revisions allocate capture order under this lock.
    -- Message edits already hold it when replacing inputs. All epoch bumps
    -- use INSERT ... ON CONFLICT, so take it before locking the epoch row.
    PERFORM pg_advisory_xact_lock(1885955182, 1);
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER compaction_projection_epoch_lock_capture_order
BEFORE INSERT ON compaction_projection_epoch
FOR EACH ROW EXECUTE FUNCTION compaction_projection_epoch_lock_capture_order();
