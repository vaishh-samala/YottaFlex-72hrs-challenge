-- Distinct orders with at least one revenue line, plus all orders regardless of status.
SELECT COUNT(DISTINCT order_id) FILTER (WHERE is_revenue) AS revenue_orders,
       COUNT(DISTINCT order_id)                            AS all_orders,
       COUNT(*)                                            AS order_lines
FROM v_sales;
