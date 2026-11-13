-- Validation failures by reason code and source.
SELECT reason_code, source_type, COUNT(*) AS failures, COUNT(DISTINCT quarantine_id) AS records
FROM v_quarantine_reasons
GROUP BY ALL ORDER BY failures DESC, reason_code;
