-- Every quarantine row ever written (all runs, including failed batches).
SELECT run_id, source_type, source_file, source_row_number, record_id, primary_reason, rejection_reasons,
       raw_record, source_line, rejected_at
FROM quarantine
ORDER BY rejected_at DESC, source_type, source_row_number;
