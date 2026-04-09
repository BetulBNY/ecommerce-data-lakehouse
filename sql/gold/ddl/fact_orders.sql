-- ====================================================================
-- Create 'gold.fact_orders' Table
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.fact_orders (
    order_pk INT PRIMARY KEY, -- I directly used silver.orders PK (1:1)
    order_id TEXT UNIQUE,
    customer_pk INT,
    order_date_key INT,
    delivery_date_key INT,    -- FK to gold.dim_date    
    -- Metrics (Aggregated)
    total_order_value FLOAT,    -- Sum of all payments for this order
    total_freight_value FLOAT,  -- Sum of all freight for this order
    review_score INT,    
    -- Logistic Metrics
    delivery_time_days INT,
    is_late BOOLEAN,
    order_status TEXT,
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);