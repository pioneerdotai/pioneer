-- Task events can contain Zstd frames. Keep TOAST out-of-line storage available.
ALTER TABLE task_event ALTER COLUMN payload_json SET STORAGE EXTERNAL;
