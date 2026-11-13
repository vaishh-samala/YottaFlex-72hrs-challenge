-- DataFlow warehouse DDL (DuckDB). Idempotent: safe to run on every pipeline start.

-- ===================================================================== operational metadata
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id                  VARCHAR PRIMARY KEY,
    start_time              TIMESTAMP NOT NULL,
    end_time                TIMESTAMP,
    duration_seconds        DOUBLE,
    source_files            VARCHAR,          -- JSON list of {source_type, file_name, file_hash, action}
    input_rows              BIGINT DEFAULT 0, -- orders + products physical data rows
    valid_rows              BIGINT DEFAULT 0,
    rejected_rows           BIGINT DEFAULT 0,
    output_rows             BIGINT DEFAULT 0, -- warehouse rows inserted or updated by this run
    execution_status        VARCHAR NOT NULL, -- RUNNING | SUCCESS | FAILED | SKIPPED
    error_message           VARCHAR,
    warnings                VARCHAR,          -- JSON list
    trigger_source          VARCHAR,          -- cli | dashboard | test | benchmark
    forced                  BOOLEAN DEFAULT FALSE,
    orders_input_rows       BIGINT DEFAULT 0,
    orders_valid_rows       BIGINT DEFAULT 0,
    orders_rejected_rows    BIGINT DEFAULT 0,
    products_input_rows     BIGINT DEFAULT 0,
    products_valid_rows     BIGINT DEFAULT 0,
    products_rejected_rows  BIGINT DEFAULT 0,
    facts_inserted          BIGINT DEFAULT 0,
    facts_updated           BIGINT DEFAULT 0,
    facts_unchanged         BIGINT DEFAULT 0,
    facts_stale_skipped     BIGINT DEFAULT 0, -- older file replayed: newer correction kept
    product_versions_inserted BIGINT DEFAULT 0,
    customers_inserted      BIGINT DEFAULT 0,
    ingestion_seconds       DOUBLE,
    validation_seconds      DOUBLE,
    transformation_seconds  DOUBLE,
    load_seconds            DOUBLE
);

-- One row per (run, source file) with that file's reconciliation counts.
CREATE TABLE IF NOT EXISTS run_files (
    run_id          VARCHAR NOT NULL,
    source_type     VARCHAR NOT NULL,      -- orders | products
    file_name       VARCHAR NOT NULL,
    file_hash       VARCHAR NOT NULL,
    action          VARCHAR NOT NULL,      -- PROCESSED | SKIPPED_ALREADY_PROCESSED | FAILED
    input_rows      BIGINT DEFAULT 0,
    valid_rows      BIGINT DEFAULT 0,
    rejected_rows   BIGINT DEFAULT 0,
    PRIMARY KEY (run_id, source_type)
);

-- Ledger of processed source files (content-addressed by SHA-256) for idempotency.
CREATE TABLE IF NOT EXISTS processed_files (
    file_hash           VARCHAR NOT NULL,
    source_type         VARCHAR NOT NULL,
    file_name           VARCHAR NOT NULL,
    row_count           BIGINT,
    status              VARCHAR NOT NULL,  -- SUCCESS | FAILED
    last_run_id         VARCHAR NOT NULL,
    first_seen_at       TIMESTAMP NOT NULL,
    last_processed_at   TIMESTAMP NOT NULL,
    attempts            INTEGER NOT NULL DEFAULT 1,
    last_error          VARCHAR,
    file_seq            BIGINT NOT NULL,   -- arrival order of the file (first time seen)
    PRIMARY KEY (file_hash, source_type)
);

-- Rejected records. Append-only per run (a reprocessed file is re-evaluated in the new run).
CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_id       BIGINT PRIMARY KEY,
    run_id              VARCHAR NOT NULL,
    source_type         VARCHAR NOT NULL,
    source_file         VARCHAR NOT NULL,
    file_hash           VARCHAR NOT NULL,
    source_row_number   BIGINT NOT NULL,   -- physical line number in the source file (header = 1)
    record_id           VARCHAR,           -- order_line_id / product_id as delivered (may be null)
    primary_reason      VARCHAR NOT NULL,
    rejection_reasons   VARCHAR NOT NULL,  -- '; '-separated "CODE: detail" list
    raw_record          VARCHAR NOT NULL,  -- JSON of the parsed fields (null tokens -> null) or {"raw_line": ...}
    rejected_at         TIMESTAMP NOT NULL,
    source_line         VARCHAR            -- exact original CSV text of the record (line terminator removed)
);
-- Upgrade warehouses created before source_line existed (older rows keep NULL; data/raw/<run_id>/ has the file).
ALTER TABLE quarantine ADD COLUMN IF NOT EXISTS source_line VARCHAR;

-- ===================================================================== dimensional model
CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INTEGER PRIMARY KEY,       -- YYYYMMDD
    full_date   DATE NOT NULL UNIQUE,
    day         INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    month_name  VARCHAR NOT NULL,
    quarter     INTEGER NOT NULL,
    year        INTEGER NOT NULL,
    weekday     VARCHAR NOT NULL,
    is_weekend  BOOLEAN NOT NULL
);

-- Type 1 (no tracked attributes beyond first_order_date).
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key      BIGINT PRIMARY KEY,
    customer_id       VARCHAR NOT NULL UNIQUE,
    first_order_date  DATE,
    created_run_id    VARCHAR NOT NULL
);

-- Type 2 slowly changing dimension: one row per product version.
CREATE TABLE IF NOT EXISTS dim_product (
    product_key     BIGINT PRIMARY KEY,
    product_id      VARCHAR NOT NULL,
    product_name    VARCHAR NOT NULL,
    category        VARCHAR NOT NULL,
    brand           VARCHAR,
    list_price      DECIMAL(12, 2) NOT NULL,
    product_status  VARCHAR NOT NULL,
    valid_from      TIMESTAMP NOT NULL,    -- source updated_at (UTC)
    valid_to        TIMESTAMP,             -- NULL = open-ended
    is_current      BOOLEAN NOT NULL,
    row_hash        VARCHAR NOT NULL,
    source_file     VARCHAR NOT NULL,
    created_run_id  VARCHAR NOT NULL,
    UNIQUE (product_id, valid_from)
);

-- Grain: one row per order line (order_line_id).
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key        BIGINT PRIMARY KEY,
    order_line_id    VARCHAR NOT NULL UNIQUE,
    order_id         VARCHAR NOT NULL,
    product_key      BIGINT NOT NULL,      -- product version valid at order_timestamp
    customer_key     BIGINT NOT NULL,
    customer_id      VARCHAR NOT NULL,
    date_key         INTEGER NOT NULL,
    order_timestamp  TIMESTAMP NOT NULL,   -- UTC
    quantity         INTEGER NOT NULL,
    unit_price       DECIMAL(12, 2) NOT NULL,  -- transaction price (never overwritten by catalog)
    total_amount     DECIMAL(18, 2) NOT NULL,  -- quantity * unit_price
    order_status     VARCHAR NOT NULL,
    row_hash         VARCHAR NOT NULL,
    source_file      VARCHAR NOT NULL,
    source_seq       BIGINT NOT NULL,      -- arrival sequence of the file that supplied this version
    run_id           VARCHAR NOT NULL,     -- run that last inserted/changed the row
    first_run_id     VARCHAR NOT NULL,     -- run that first inserted the row
    loaded_at        TIMESTAMP NOT NULL
);

-- ===================================================================== views
CREATE OR REPLACE VIEW v_sales AS
SELECT f.*,
       p.product_id, p.product_name, p.category, p.brand, p.list_price AS catalog_price_at_order,
       d.full_date, d.year, d.quarter, d.month, d.month_name, d.weekday,
       f.order_status IN ('paid', 'shipped', 'delivered') AS is_revenue
FROM fact_sales f
JOIN dim_product p ON p.product_key = f.product_key
JOIN dim_date d    ON d.date_key = f.date_key;

-- The latest processing of each source file (avoids double counting after a forced rerun).
CREATE OR REPLACE VIEW v_current_file_runs AS
SELECT * FROM (
    SELECT rf.*, r.start_time,
           ROW_NUMBER() OVER (PARTITION BY rf.file_hash, rf.source_type ORDER BY r.start_time DESC) AS rn
    FROM run_files rf JOIN pipeline_runs r USING (run_id)
    WHERE rf.action = 'PROCESSED' AND r.execution_status = 'SUCCESS'
) WHERE rn = 1;

CREATE OR REPLACE VIEW v_quarantine_current AS
SELECT q.*
FROM quarantine q
JOIN v_current_file_runs c ON c.run_id = q.run_id AND c.source_type = q.source_type;

CREATE OR REPLACE VIEW v_quarantine_reasons AS
SELECT q.quarantine_id, q.run_id, q.source_type, q.source_file, q.record_id,
       trim(split_part(reason, ':', 1)) AS reason_code,
       trim(substr(reason, strpos(reason, ':') + 1)) AS reason_detail
FROM v_quarantine_current q, unnest(string_split(q.rejection_reasons, '; ')) AS t(reason);
