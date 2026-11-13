-- Full source-to-warehouse equation per source file (latest successful processing):
--
--   input_rows    = valid_rows + rejected_rows
--   rejected_rows = malformed + duplicates + missing_values + invalid_types + business_rule + invalid_references
--                   (each quarantined row counted once, by its primary_reason)
--   valid_rows    = facts_inserted + facts_updated + facts_unchanged + facts_stale_skipped   (orders only)
--
-- "duplicates" counts every rejected copy: the extra copies of exact duplicates (first copy is loaded) and
-- ALL copies of an order_line_id delivered with conflicting content. "invalid_references" includes every
-- line of an order that names more than one customer (INCONSISTENT_ORDER_CUSTOMER rejects the whole order).
WITH q AS (
    SELECT run_id, source_type,
           COUNT(*)                                                                                   AS quarantined,
           COUNT(*) FILTER (WHERE primary_reason = 'MALFORMED_RECORD')                                AS malformed,
           COUNT(*) FILTER (WHERE primary_reason IN ('DUPLICATE_RECORD', 'CONFLICTING_DUPLICATE_KEY',
                                                     'SUPERSEDED_DUPLICATE', 'CONFLICTING_PRODUCT_VERSION')) AS duplicates,
           COUNT(*) FILTER (WHERE primary_reason = 'MISSING_REQUIRED_FIELD')                          AS missing_values,
           COUNT(*) FILTER (WHERE primary_reason IN ('INVALID_DATA_TYPE', 'INVALID_TIMESTAMP'))       AS invalid_types,
           COUNT(*) FILTER (WHERE primary_reason IN ('TIMESTAMP_OUT_OF_RANGE', 'NEGATIVE_QUANTITY', 'ZERO_QUANTITY',
                                                     'QUANTITY_OUT_OF_RANGE', 'INVALID_PRICE',
                                                     'INVALID_PRICE_PRECISION', 'INVALID_STATUS'))   AS business_rule,
           COUNT(*) FILTER (WHERE primary_reason IN ('INVALID_PRODUCT_REFERENCE',
                                                     'INCONSISTENT_ORDER_CUSTOMER'))                  AS invalid_references
    FROM quarantine
    GROUP BY run_id, source_type
)
SELECT c.source_type, c.file_name, c.run_id,
       c.input_rows, c.valid_rows, c.rejected_rows,
       COALESCE(q.malformed, 0)          AS malformed,
       COALESCE(q.duplicates, 0)         AS duplicates,
       COALESCE(q.missing_values, 0)     AS missing_values,
       COALESCE(q.invalid_types, 0)      AS invalid_types,
       COALESCE(q.business_rule, 0)      AS business_rule,
       COALESCE(q.invalid_references, 0) AS invalid_references,
       CASE WHEN c.source_type = 'orders'
            THEN r.facts_inserted + r.facts_updated + r.facts_unchanged + r.facts_stale_skipped END AS facts_accounted,
       (c.valid_rows + c.rejected_rows = c.input_rows)                                            AS input_balanced,
       (COALESCE(q.malformed + q.duplicates + q.missing_values + q.invalid_types + q.business_rule
                 + q.invalid_references, 0) = c.rejected_rows
        AND COALESCE(q.quarantined, 0) = c.rejected_rows)                                         AS rejects_fully_categorised,
       (c.source_type <> 'orders'
        OR c.valid_rows = r.facts_inserted + r.facts_updated + r.facts_unchanged + r.facts_stale_skipped) AS valid_rows_in_warehouse
FROM v_current_file_runs c
JOIN pipeline_runs r USING (run_id)
LEFT JOIN q ON q.run_id = c.run_id AND q.source_type = c.source_type
ORDER BY c.source_type, c.file_name;
