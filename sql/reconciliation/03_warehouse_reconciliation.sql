-- Warehouse-level totals: fact rows equal the sum of first-time inserts; one row per order line;
-- revenue equals the sum of quantity x transaction price; no orphan dimension references.
WITH t AS (
    SELECT (SELECT COUNT(*) FROM fact_sales)                                                        AS fact_rows,
           (SELECT COUNT(DISTINCT order_line_id) FROM fact_sales)                                   AS distinct_order_lines,
           (SELECT COALESCE(SUM(facts_inserted), 0)::BIGINT FROM pipeline_runs WHERE execution_status = 'SUCCESS') AS sum_facts_inserted,
           (SELECT COALESCE(SUM(total_amount), 0) FROM fact_sales)                                  AS sum_total_amount,
           (SELECT COALESCE(SUM(quantity * unit_price), 0) FROM fact_sales)                         AS sum_qty_x_price,
           (SELECT COUNT(*) FROM fact_sales f LEFT JOIN dim_product p USING (product_key) WHERE p.product_key IS NULL)    AS orphan_products,
           (SELECT COUNT(*) FROM fact_sales f LEFT JOIN dim_customer c USING (customer_key) WHERE c.customer_key IS NULL) AS orphan_customers,
           (SELECT COUNT(*) FROM fact_sales f LEFT JOIN dim_date d USING (date_key) WHERE d.date_key IS NULL)             AS orphan_dates
)
SELECT *,
       (fact_rows = distinct_order_lines AND fact_rows = sum_facts_inserted AND sum_total_amount = sum_qty_x_price
        AND orphan_products + orphan_customers + orphan_dates = 0) AS all_checks_pass
FROM t;
