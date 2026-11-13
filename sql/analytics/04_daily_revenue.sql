-- Revenue per calendar day (UTC).
SELECT full_date,
       SUM(total_amount)::DECIMAL(18,2) AS revenue,
       COUNT(DISTINCT order_id)         AS orders,
       SUM(quantity)::BIGINT            AS quantity
FROM v_sales
WHERE is_revenue
GROUP BY full_date
ORDER BY full_date;
