-- Full run history (newest first).
SELECT run_id, start_time, end_time, ROUND(duration_seconds, 3) AS duration_seconds, execution_status,
       trigger_source, forced, input_rows, valid_rows, rejected_rows, output_rows,
       orders_input_rows, orders_valid_rows, orders_rejected_rows,
       products_input_rows, products_valid_rows, products_rejected_rows,
       facts_inserted, facts_updated, facts_unchanged, facts_stale_skipped,
       product_versions_inserted, customers_inserted,
       ROUND(ingestion_seconds, 4) AS ingestion_seconds, ROUND(validation_seconds, 4) AS validation_seconds,
       ROUND(transformation_seconds, 4) AS transformation_seconds, ROUND(load_seconds, 4) AS load_seconds,
       error_message, warnings, source_files
FROM pipeline_runs
ORDER BY start_time DESC;
