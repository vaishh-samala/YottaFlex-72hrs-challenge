-- Average order value = revenue / distinct revenue orders.
SELECT (SUM(total_amount) / NULLIF(COUNT(DISTINCT order_id), 0))::DECIMAL(18,2) AS average_order_value,
       COUNT(DISTINCT order_id)                                                  AS revenue_orders
FROM v_sales
WHERE is_revenue;
