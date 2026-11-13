-- Per source file (latest successful processing): source rows vs valid vs rejected vs quarantine table.
SELECT c.source_type, c.file_name, c.run_id,
       c.input_rows, c.valid_rows, c.rejected_rows,
       COUNT(q.quarantine_id)                                  AS quarantine_rows,
       (c.valid_rows + c.rejected_rows = c.input_rows)          AS input_balanced,
       (COUNT(q.quarantine_id) = c.rejected_rows)               AS quarantine_balanced
FROM v_current_file_runs c
LEFT JOIN quarantine q ON q.run_id = c.run_id AND q.source_type = c.source_type
GROUP BY c.source_type, c.file_name, c.run_id, c.input_rows, c.valid_rows, c.rejected_rows
ORDER BY c.source_type, c.file_name;
