-- ====================================================================
-- TRANSFORM SILVER LAYER
-- Table Creation (DDL)
-- ====================================================================
-- ====================================================================
-- Create 'silver.category_translation' Table
-- ====================================================================

-- Drop if exists (optional, for clean re-run)
-- DROP TABLE IF EXISTS silver.olist_category_translation;

-- STEP 1: Create table (persistent)
CREATE TABLE IF NOT EXISTS silver.category_translation (
    category_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_name_pt TEXT UNIQUE,       
    category_name_en TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
