-- ====================================================================
-- Create 'gold.fact_sales_items' Table
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.fact_sales_items (
    sales_item_pk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_pk INT,           -- FK to silver.orders
    order_id TEXT,
    product_pk INT,         -- FK to silver.products
    seller_pk INT,          -- FK to silver.sellers
    customer_pk INT,        -- FK to silver.customers
    order_date_key INT,     -- FK to gold.dim_date (YYYYMMDD)
    -- Metrics
    price FLOAT,
    freight_value FLOAT,
    total_item_value FLOAT,
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);