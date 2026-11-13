-- Row counts of warehouse tables.
SELECT 'fact_sales' AS table_name, COUNT(*) AS row_count FROM fact_sales
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_product (current)', COUNT(*) FROM dim_product WHERE is_current
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'quarantine (all runs)', COUNT(*) FROM quarantine;
