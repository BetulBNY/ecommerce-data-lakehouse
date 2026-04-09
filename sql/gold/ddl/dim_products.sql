-- ====================================================================
-- Create 'gold.dim_products' Table
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.dim_products (
    product_pk INT PRIMARY KEY,
    product_id TEXT,
    category_id INT,               -- Analysts may want to join via id
    category_name_en TEXT,         -- Clean English name
    name_length INT,               -- Listing quality metrics
    description_length INT,
    photos_qty INT,
    weight_g FLOAT,                -- Default physical properties of the product
    volume_cm3 FLOAT,              -- Volume, it is easier for analysts to keep track of volume directly instead of dimensions.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);