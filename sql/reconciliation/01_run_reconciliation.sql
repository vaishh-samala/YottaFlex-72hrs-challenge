-- Per run: every input row is either valid or rejected; every rejected row is in quarantine;
-- every valid order row is accounted for in the warehouse (inserted/updated/unchanged/stale-skipped).
SELECT r.run_id, r.start_time, r.execution_status,
       r.input_rows, r.valid_rows, r.rejected_rows,
       COALESCE(q.quarantined, 0)                                                    AS quarantine_rows,
       r.orders_valid_rows,
       r.facts_inserted + r.facts_updated + r.facts_unchanged + r.facts_stale_skipped AS fact_rows_accounted,
       (r.valid_rows + r.rejected_rows = r.input_rows)                               AS input_balanced,
       (COALESCE(q.quarantined, 0) = r.rejected_rows)                                AS quarantine_balanced,
       (r.orders_valid_rows = r.facts_inserted + r.facts_updated + r.facts_unchanged + r.facts_stale_skipped) AS warehouse_balanced
FROM pipeline_runs r
LEFT JOIN (SELECT run_id, COUNT(*) AS quarantined FROM quarantine GROUP BY run_id) q USING (run_id)
WHERE r.execution_status = 'SUCCESS'
ORDER BY r.start_time;
