-- Customer purchase distribution (orders per customer buckets).
WITH c AS (
    SELECT customer_id, COUNT(DISTINCT order_id) AS orders, SUM(total_amount) AS value
    FROM v_sales WHERE is_revenue GROUP BY customer_id
)
SELECT CASE WHEN orders = 1 THEN '1 order' WHEN orders <= 3 THEN '2-3 orders'
            WHEN orders <= 5 THEN '4-5 orders' ELSE '6+ orders' END AS segment,
       MIN(orders) AS sort_key,
       COUNT(*) AS customers,
       SUM(value)::DECIMAL(18,2) AS revenue,
       (SUM(value) / COUNT(*))::DECIMAL(18,2) AS revenue_per_customer
FROM c GROUP BY segment ORDER BY sort_key;
