-- Rejected-record percentage over the latest successful processing of each source file.
SELECT COALESCE(source_type, 'ALL SOURCES')                                AS source_type,
       SUM(input_rows)::BIGINT                                                 AS input_rows,
       SUM(rejected_rows)::BIGINT                                              AS rejected_rows,
       ROUND(100.0 * SUM(rejected_rows) / NULLIF(SUM(input_rows), 0), 2)       AS rejected_pct
FROM v_current_file_runs
GROUP BY ROLLUP (source_type)
ORDER BY GROUPING(source_type), 1;
