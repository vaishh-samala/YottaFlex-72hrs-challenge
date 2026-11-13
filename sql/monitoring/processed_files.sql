-- File ledger used for idempotency.
SELECT source_type, file_name, file_seq, status, attempts, row_count, last_run_id, first_seen_at,
       last_processed_at, last_error, file_hash
FROM processed_files ORDER BY source_type, file_seq;
