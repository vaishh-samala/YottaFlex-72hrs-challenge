-- Total recognised revenue (statuses paid/shipped/delivered), using the transaction price.
-- Value of cancelled/returned and still-pending lines is reported separately, never added.
SELECT COALESCE(SUM(total_amount) FILTER (WHERE is_revenue), 0)::DECIMAL(18,2)                                AS total_revenue,
       COALESCE(SUM(total_amount) FILTER (WHERE order_status = 'pending'), 0)::DECIMAL(18,2)                  AS pending_value,
       COALESCE(SUM(total_amount) FILTER (WHERE order_status IN ('cancelled', 'returned')), 0)::DECIMAL(18,2) AS cancelled_returned_value
FROM v_sales;
