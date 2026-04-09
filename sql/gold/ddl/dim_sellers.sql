-- ====================================================================
-- Create 'gold.dim_sellers' Table
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.dim_sellers (
    seller_pk INT PRIMARY KEY, -- Silver Layer PK
    seller_id TEXT,
    city TEXT,
    state TEXT,
    zip_code INT,
    latitude FLOAT,
    longitude FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);