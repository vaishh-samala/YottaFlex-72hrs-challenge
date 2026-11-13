-- Order lines and value by status.
SELECT order_status, COUNT(*) AS order_lines, SUM(total_amount)::DECIMAL(18,2) AS value, BOOL_OR(is_revenue) AS counts_as_revenue
FROM v_sales GROUP BY order_status ORDER BY value DESC;
