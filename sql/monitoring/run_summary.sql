-- Headline operational metrics for the overview page.
SELECT COUNT(*)                                                   AS total_runs,
       COUNT(*) FILTER (WHERE execution_status = 'SUCCESS')       AS successful_runs,
       COUNT(*) FILTER (WHERE execution_status = 'FAILED')        AS failed_runs,
       COUNT(*) FILTER (WHERE execution_status = 'SKIPPED')       AS skipped_runs,
       COUNT(*) FILTER (WHERE execution_status = 'RUNNING')       AS running_runs,
       -- SKIPPED runs only fingerprint the files (nothing is read into the pipeline), so they are excluded.
       COALESCE(SUM(input_rows) FILTER (WHERE execution_status <> 'SKIPPED'), 0) AS total_records_ingested,
       COALESCE(SUM(valid_rows), 0)                               AS total_valid_records,
       COALESCE(SUM(rejected_rows), 0)                            AS total_rejected_records,
       MAX(start_time)                                            AS last_run_start,
       arg_max(execution_status, start_time)                      AS last_run_status,
       arg_max(run_id, start_time)                                AS last_run_id
FROM pipeline_runs;
