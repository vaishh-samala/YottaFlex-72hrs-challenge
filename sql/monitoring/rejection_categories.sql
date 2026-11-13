-- Validation failures by dashboard category (current state, no double counting after reruns).
SELECT CASE
         WHEN reason_code = 'MISSING_REQUIRED_FIELD' THEN 'Missing values'
         WHEN reason_code IN ('DUPLICATE_RECORD', 'CONFLICTING_DUPLICATE_KEY', 'SUPERSEDED_DUPLICATE',
                              'CONFLICTING_PRODUCT_VERSION') THEN 'Duplicate records'
         WHEN reason_code IN ('INVALID_PRODUCT_REFERENCE', 'INCONSISTENT_ORDER_CUSTOMER') THEN 'Invalid references'
         WHEN reason_code IN ('INVALID_DATA_TYPE', 'INVALID_TIMESTAMP', 'MALFORMED_RECORD') THEN 'Invalid types / malformed'
         ELSE 'Business rule violations'
       END AS category,
       COUNT(*) AS failures
FROM v_quarantine_reasons
GROUP BY ALL ORDER BY failures DESC;
