-- Files handled per run.
SELECT run_id, source_type, file_name, file_hash, action, input_rows, valid_rows, rejected_rows
FROM run_files ORDER BY run_id, source_type;
