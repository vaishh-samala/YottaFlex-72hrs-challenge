# DataFlow analytics report

Generated 2026-11-13 05:08:03 UTC from `warehouse.duckdb` by `scripts/run_analytics.py`. Every number below is the output of the SQL file named in its heading.

## Analytics

### `sql/analytics/01_total_revenue.sql`

| total_revenue | pending_value | cancelled_returned_value |
|---|---|---|
| 307320.17 | 18687.15 | 22069.44 |

### `sql/analytics/02_total_orders.sql`

| revenue_orders | all_orders | order_lines |
|---|---|---|
| 201 | 246 | 732 |

### `sql/analytics/03_total_quantity.sql`

| total_quantity |
|---|
| 2475 |

### `sql/analytics/04_daily_revenue.sql`

| full_date | revenue | orders | quantity |
|---|---|---|---|
| 2026-01-01 00:00:00 | 5173.51 | 5 | 37 |
| 2026-01-02 00:00:00 | 2377.11 | 1 | 16 |
| 2026-01-03 00:00:00 | 438.08 | 2 | 13 |
| 2026-01-06 00:00:00 | 8469.37 | 2 | 20 |
| 2026-01-07 00:00:00 | 1469.91 | 1 | 8 |
| 2026-01-08 00:00:00 | 2689.17 | 3 | 29 |
| 2026-01-10 00:00:00 | 3367.12 | 4 | 19 |
| 2026-01-11 00:00:00 | 1859.94 | 1 | 11 |
| 2026-01-13 00:00:00 | 707.04 | 1 | 9 |
| 2026-01-14 00:00:00 | 395.21 | 1 | 1 |
| 2026-01-16 00:00:00 | 3476.25 | 3 | 24 |
| 2026-01-17 00:00:00 | 666.11 | 1 | 11 |
| 2026-01-18 00:00:00 | 239.15 | 1 | 1 |
| 2026-01-19 00:00:00 | 5025.87 | 1 | 13 |
| 2026-01-20 00:00:00 | 2953.54 | 2 | 18 |

_115 more row(s) in the CSV_

### `sql/analytics/05_monthly_revenue.sql`

| year | month | month_name | revenue | orders |
|---|---|---|---|---|
| 2026 | 1 | January | 51621.5 | 42 |
| 2026 | 2 | February | 26447.53 | 18 |
| 2026 | 3 | March | 17957.21 | 19 |
| 2026 | 4 | April | 28610.51 | 22 |
| 2026 | 5 | May | 104018.9 | 32 |
| 2026 | 6 | June | 36391.8 | 28 |
| 2026 | 7 | July | 42272.72 | 40 |

### `sql/analytics/06_revenue_by_category.sql`

| category | revenue | quantity | revenue_share_pct |
|---|---|---|---|
| Electronics | 109937.06 | 198 | 35.77 |
| Books | 78326.82 | 1247 | 25.49 |
| Sports | 50092.25 | 262 | 16.3 |
| Home & Kitchen | 18478.36 | 114 | 6.01 |
| Beauty | 17721.63 | 233 | 5.77 |
| Clothing | 14780.64 | 110 | 4.81 |
| Toys | 12533.13 | 163 | 4.08 |
| Grocery | 5450.28 | 148 | 1.77 |

### `sql/analytics/07_top_10_products.sql`

| product_id | product_name | category | revenue | quantity | orders |
|---|---|---|---|---|---|
| P00058 | LightSlateGray Comic | Books | 68501.04 | 1021 | 9 |
| P00019 | BlanchedAlmond Monitor | Electronics | 24799.08 | 28 | 10 |
| P00006 | Indigo Webcam | Electronics | 20383.31 | 22 | 11 |
| P00032 | Tan Speaker | Electronics | 18558.34 | 28 | 15 |
| P00018 | Indigo Speaker | Electronics | 14179.56 | 20 | 7 |
| P00001 | DarkOliveGreen Monitor | Electronics | 12244.7 | 19 | 11 |
| P00038 | FireBrick Cycling Helmet | Sports | 12123.74 | 36 | 10 |
| P00012 | MistyRose Lamp | Home & Kitchen | 10984.95 | 43 | 15 |
| P00053 | Red Speaker | Electronics | 10621.71 | 23 | 8 |
| P00009 | DarkKhaki Yoga Mat | Sports | 7783.73 | 30 | 14 |

### `sql/analytics/08_top_10_customers.sql`

| customer_id | purchase_value | orders | avg_order_value | first_purchase | last_purchase |
|---|---|---|---|---|---|
| C000058 | 67010.0 | 1 | 67010.0 | 2026-05-29 00:00:00 | 2026-05-29 00:00:00 |
| C000019 | 13543.35 | 6 | 2257.23 | 2026-03-10 00:00:00 | 2026-07-24 00:00:00 |
| C000023 | 9271.93 | 7 | 1324.56 | 2026-03-11 00:00:00 | 2026-07-22 00:00:00 |
| C000049 | 8508.66 | 7 | 1215.52 | 2026-01-03 00:00:00 | 2026-06-12 00:00:00 |
| C000036 | 8243.83 | 7 | 1177.69 | 2026-01-16 00:00:00 | 2026-07-15 00:00:00 |
| C000066 | 8144.71 | 3 | 2714.9 | 2026-05-11 00:00:00 | 2026-07-31 00:00:00 |
| C000071 | 7777.53 | 4 | 1944.38 | 2026-03-17 00:00:00 | 2026-07-09 00:00:00 |
| C000064 | 6724.32 | 4 | 1681.08 | 2026-01-06 00:00:00 | 2026-05-04 00:00:00 |
| C000032 | 6575.4 | 7 | 939.34 | 2026-01-10 00:00:00 | 2026-07-15 00:00:00 |
| C000026 | 6563.23 | 3 | 2187.74 | 2026-05-07 00:00:00 | 2026-07-06 00:00:00 |

### `sql/analytics/09_average_order_value.sql`

| average_order_value | revenue_orders |
|---|---|
| 1528.96 | 201 |

### `sql/analytics/10_rejected_record_pct.sql`

| source_type | input_rows | rejected_rows | rejected_pct |
|---|---|---|---|
| orders | 758 | 21 | 2.77 |
| products | 73 | 6 | 8.22 |
| ALL SOURCES | 831 | 27 | 3.25 |

## Reconciliation

### `sql/reconciliation/01_run_reconciliation.sql`

| run_id | start_time | execution_status | input_rows | valid_rows | rejected_rows | quarantine_rows | orders_valid_rows | fact_rows_accounted | input_balanced | quarantine_balanced | warehouse_balanced |
|---|---|---|---|---|---|---|---|---|---|---|---|
| run_20261112T185815_c9f527 | 2026-11-12 18:58:15.962875 | SUCCESS | 667 | 642 | 25 | 25 | 582 | 582 | True | True | True |
| run_20261112T185816_168422 | 2026-11-12 18:58:16.523087 | SUCCESS | 164 | 162 | 2 | 2 | 155 | 155 | True | True | True |
| run_20261112T185817_e67011 | 2026-11-12 18:58:17.529676 | SUCCESS | 667 | 642 | 25 | 25 | 582 | 582 | True | True | True |

### `sql/reconciliation/02_file_reconciliation.sql`

| source_type | file_name | run_id | input_rows | valid_rows | rejected_rows | quarantine_rows | input_balanced | quarantine_balanced |
|---|---|---|---|---|---|---|---|---|
| orders | orders.csv | run_20261112T185817_e67011 | 602 | 582 | 20 | 20 | True | True |
| orders | orders_batch_2.csv | run_20261112T185816_168422 | 156 | 155 | 1 | 1 | True | True |
| products | products.csv | run_20261112T185817_e67011 | 65 | 60 | 5 | 5 | True | True |
| products | products_update.csv | run_20261112T185816_168422 | 8 | 7 | 1 | 1 | True | True |

### `sql/reconciliation/03_warehouse_reconciliation.sql`

| fact_rows | distinct_order_lines | sum_facts_inserted | sum_total_amount | sum_qty_x_price | orphan_products | orphan_customers | orphan_dates | all_checks_pass |
|---|---|---|---|---|---|---|---|---|
| 732 | 732 | 732 | 348076.76 | 348076.76 | 0 | 0 | 0 | True |

### `sql/reconciliation/04_reject_breakdown.sql`

| source_type | file_name | run_id | input_rows | valid_rows | rejected_rows | malformed | duplicates | missing_values | invalid_types | business_rule | invalid_references | facts_accounted | input_balanced | rejects_fully_categorised | valid_rows_in_warehouse |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| orders | orders.csv | run_20261112T185817_e67011 | 602 | 582 | 20 | 1 | 3 | 4 | 3 | 6 | 3 | 582 | True | True | True |
| orders | orders_batch_2.csv | run_20261112T185816_168422 | 156 | 155 | 1 | 0 | 0 | 0 | 0 | 0 | 1 | 155 | True | True | True |
| products | products.csv | run_20261112T185817_e67011 | 65 | 60 | 5 | 0 | 1 | 1 | 1 | 2 | 0 | <NA> | True | True | True |
| products | products_update.csv | run_20261112T185816_168422 | 8 | 7 | 1 | 0 | 0 | 1 | 0 | 0 | 0 | <NA> | True | True | True |

## Validation report

### Failures by category (`sql/monitoring/rejection_categories.sql`)

| category | failures |
|---|---|
| Business rule violations | 8 |
| Missing values | 6 |
| Invalid types / malformed | 5 |
| Duplicate records | 4 |
| Invalid references | 4 |

### Failures by reason code (`sql/monitoring/rejection_reasons.sql`)

| reason_code | source_type | failures | records |
|---|---|---|---|
| MISSING_REQUIRED_FIELD | orders | 4 | 4 |
| CONFLICTING_DUPLICATE_KEY | orders | 2 | 2 |
| INCONSISTENT_ORDER_CUSTOMER | orders | 2 | 2 |
| INVALID_DATA_TYPE | orders | 2 | 2 |
| INVALID_PRODUCT_REFERENCE | orders | 2 | 2 |
| MISSING_REQUIRED_FIELD | products | 2 | 2 |
| DUPLICATE_RECORD | orders | 1 | 1 |
| INVALID_PRICE | orders | 1 | 1 |
| INVALID_PRICE | products | 1 | 1 |
| INVALID_STATUS | orders | 1 | 1 |
| INVALID_STATUS | products | 1 | 1 |
| INVALID_TIMESTAMP | products | 1 | 1 |
| INVALID_TIMESTAMP | orders | 1 | 1 |
| MALFORMED_RECORD | orders | 1 | 1 |
| NEGATIVE_QUANTITY | orders | 1 | 1 |
| QUANTITY_OUT_OF_RANGE | orders | 1 | 1 |
| SUPERSEDED_DUPLICATE | products | 1 | 1 |
| TIMESTAMP_OUT_OF_RANGE | orders | 1 | 1 |
| ZERO_QUANTITY | orders | 1 | 1 |

### Rejected records (latest processing of each file) (`sql/monitoring/quarantine_current.sql`)

| source_file | source_row_number | record_id | rejection_reasons |
|---|---|---|---|
| orders.csv | 29 | OL00000009-2 | ZERO_QUANTITY: quantity = 0 |
| orders.csv | 35 | OL00000011-3 | INVALID_DATA_TYPE: quantity is not an integer |
| orders.csv | 43 | OL00000014-1 | INVALID_STATUS: order_status not in ['cancelled', 'delivered', 'paid', 'pending', 'returned', 'shipped'] |
| orders.csv | 74 | OL00000025-3 | INVALID_PRODUCT_REFERENCE: product_id not found in product master |
| orders.csv | 98 | OL00000032-1 | CONFLICTING_DUPLICATE_KEY: order_line_id repeated with different content (all copies rejected) |
| orders.csv | 99 | OL00000032-1 | CONFLICTING_DUPLICATE_KEY: order_line_id repeated with different content (all copies rejected) |
| orders.csv | 113 | OL00000037-3 | DUPLICATE_RECORD: exact duplicate of an earlier row (first kept) |
| orders.csv | 179 | nan | MISSING_REQUIRED_FIELD: order_line_id |
| orders.csv | 190 | OL00000062-1 | INCONSISTENT_ORDER_CUSTOMER: order_id is associated with more than one customer_id |
| orders.csv | 191 | OL00000062-2 | INCONSISTENT_ORDER_CUSTOMER: order_id is associated with more than one customer_id |
| orders.csv | 200 | OL00000065-2 | TIMESTAMP_OUT_OF_RANGE: order_timestamp before 2000-01-01 or in the future |
| orders.csv | 211 | OL00000069-2 | INVALID_DATA_TYPE: quantity is not an integer |
| orders.csv | 237 | OL00000076-1 | NEGATIVE_QUANTITY: quantity < 0 |
| orders.csv | 283 | OL00000091-2 | MISSING_REQUIRED_FIELD: customer_id |
| orders.csv | 289 | OL00000092-4 | MISSING_REQUIRED_FIELD: customer_id |
| orders.csv | 353 | OL00000115-1 | MISSING_REQUIRED_FIELD: quantity |
| orders.csv | 471 | OL00000156-4 | QUANTITY_OUT_OF_RANGE: quantity > 1000 |
| orders.csv | 477 | OL00000158-2 | INVALID_TIMESTAMP: order_timestamp is not a valid ISO-8601 timestamp |
| orders.csv | 511 | OL00000169-4 | INVALID_PRICE: unit_price must be > 0 and <= 1,000,000 |
| orders.csv | 551 | OL00000179-4 | MALFORMED_RECORD: expected 9 fields, found 5 |
| orders_batch_2.csv | 157 | OL99999999-1 | INVALID_PRODUCT_REFERENCE: product_id not found in product master |
| products.csv | 62 | P90000 | MISSING_REQUIRED_FIELD: product_name |
| products.csv | 63 | P90001 | INVALID_PRICE: list_price must be > 0 and <= 1,000,000 |
| products.csv | 64 | P90002 | INVALID_STATUS: product_status not in ['active', 'discontinued', 'out_of_stock'] |
| products.csv | 65 | P90003 | INVALID_TIMESTAMP: updated_at is not a valid ISO-8601 timestamp |
| products.csv | 66 | P00060 | SUPERSEDED_DUPLICATE: product_id repeated in file with an older updated_at |
| products_update.csv | 9 | P95000 | MISSING_REQUIRED_FIELD: product_name |

### Pipeline runs (`sql/monitoring/runs.sql`)

| run_id | execution_status | input_rows | valid_rows | rejected_rows | output_rows | duration_seconds | error_message |
|---|---|---|---|---|---|---|---|
| run_20261113T050802_3b0fdf | SKIPPED | 164 | 0 | 0 | 0 | 0.039 |  |
| run_20261113T050802_fd1bfa | SKIPPED | 667 | 0 | 0 | 0 | 0.047 |  |
| run_20261112T185817_e67011 | SUCCESS | 667 | 642 | 25 | 0 | 0.134 |  |
| run_20261112T185817_2d9a2e | SKIPPED | 667 | 0 | 0 | 0 | 0.038 |  |
| run_20261112T185816_168422 | SUCCESS | 164 | 162 | 2 | 155 | 0.134 |  |
| run_20261112T185815_c9f527 | SUCCESS | 667 | 642 | 25 | 582 | 0.139 |  |

## Reconciliation verdict

- PASS — runs_input_balanced
- PASS — runs_quarantine_balanced
- PASS — runs_warehouse_balanced
- PASS — files_input_balanced
- PASS — files_quarantine_balanced
- PASS — breakdown_input_balanced
- PASS — breakdown_rejects_fully_categorised
- PASS — breakdown_valid_rows_in_warehouse
- PASS — warehouse_all_checks_pass
