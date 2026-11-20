-- Zstd frames can be written out of line without recompression by TOAST.
ALTER TABLE tool_output_chunk ALTER COLUMN text SET STORAGE EXTERNAL;
