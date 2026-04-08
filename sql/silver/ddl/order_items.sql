-- ====================================================================
-- Create 'silver.order_items' Table
-- ====================================================================
-- DROP TABLE IF EXISTS silver.order_items;
CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_pk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id TEXT NOT NULL,
    order_item_id INT NOT NULL,
    product_id TEXT,
    product_pk INT, -- silver.products PK
    seller_id TEXT,
    seller_pk INT,  -- silver.sellers PK
    shipping_limit_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT,
    total_value FLOAT, -- Price + Freight
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (order_id, order_item_id)
);