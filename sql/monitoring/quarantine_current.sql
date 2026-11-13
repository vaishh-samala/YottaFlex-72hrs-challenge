-- Current rejected records (latest successful processing of each file).
SELECT run_id, source_type, source_file, source_row_number, record_id, primary_reason, rejection_reasons,
       raw_record, source_line, rejected_at
FROM v_quarantine_current
ORDER BY source_type, source_file, source_row_number;
