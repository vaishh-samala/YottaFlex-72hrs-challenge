-- Revenue by product category (category of the product version valid at order time).
SELECT category,
       SUM(total_amount)::DECIMAL(18,2)                                         AS revenue,
       SUM(quantity)::BIGINT                                                    AS quantity,
       ROUND(100.0 * SUM(total_amount) / SUM(SUM(total_amount)) OVER (), 2)     AS revenue_share_pct
FROM v_sales
WHERE is_revenue
GROUP BY category
ORDER BY revenue DESC;
