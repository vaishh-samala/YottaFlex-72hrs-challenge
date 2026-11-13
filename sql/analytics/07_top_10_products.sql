-- Top 10 products by revenue (all versions of a product combined; current name shown).
SELECT s.product_id,
       cur.product_name,
       cur.category,
       SUM(s.total_amount)::DECIMAL(18,2) AS revenue,
       SUM(s.quantity)::BIGINT            AS quantity,
       COUNT(DISTINCT s.order_id)         AS orders
FROM v_sales s
JOIN dim_product cur ON cur.product_id = s.product_id AND cur.is_current
WHERE s.is_revenue
GROUP BY s.product_id, cur.product_name, cur.category
ORDER BY revenue DESC, s.product_id
LIMIT 10;
