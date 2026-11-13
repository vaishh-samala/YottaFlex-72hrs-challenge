-- Revenue per month.
SELECT year, month, month_name,
       SUM(total_amount)::DECIMAL(18,2) AS revenue,
       COUNT(DISTINCT order_id)         AS orders
FROM v_sales
WHERE is_revenue
GROUP BY year, month, month_name
ORDER BY year, month;
