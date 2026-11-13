-- Units sold on revenue lines.
SELECT COALESCE(SUM(quantity), 0)::BIGINT AS total_quantity
FROM v_sales
WHERE is_revenue;
