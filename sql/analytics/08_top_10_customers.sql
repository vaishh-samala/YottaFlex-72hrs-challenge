-- Top 10 customers by purchase value.
SELECT customer_id,
       SUM(total_amount)::DECIMAL(18,2)                                  AS purchase_value,
       COUNT(DISTINCT order_id)                                          AS orders,
       (SUM(total_amount) / COUNT(DISTINCT order_id))::DECIMAL(18,2)     AS avg_order_value,
       MIN(full_date)                                                    AS first_purchase,
       MAX(full_date)                                                    AS last_purchase
FROM v_sales
WHERE is_revenue
GROUP BY customer_id
ORDER BY purchase_value DESC, customer_id
LIMIT 10;
