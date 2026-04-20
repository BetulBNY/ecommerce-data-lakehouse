-- ====================================================================
-- Create 'gold.dim_customers' Table
-- ====================================================================
-- DROP TABLE IF EXISTS gold.dim_customers;
CREATE TABLE IF NOT EXISTS gold.dim_customers (
    customer_pk        INT PRIMARY KEY, -- PK comes from Silver Layer
    customer_id        TEXT,
    customer_unique_id TEXT,
    city               TEXT,
    state              TEXT,
    zip_code           INT,
    latitude           FLOAT,
    longitude          FLOAT,
    -- Audit Columns: Tracks when data flowed from Silver to Gold.
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
