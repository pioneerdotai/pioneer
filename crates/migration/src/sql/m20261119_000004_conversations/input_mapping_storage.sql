-- Permit out-of-line storage without recompressing the independent Zstd frame.
ALTER TABLE turn_cli_runtime_binding ALTER COLUMN input_mapping_json SET STORAGE EXTERNAL;
