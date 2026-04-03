-- ====================================================================
-- Create 'silver.customers' Table
-- ====================================================================
-- STEP 1: Create table (persistent)
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_pk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- Our ID
    customer_id TEXT UNIQUE,                                   -- Olist's original ID
    customer_unique_id TEXT,
    zip_code_prefix INT,
    city TEXT,
    state TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)