-- ====================================================================
-- Create 'silver.products' Table
-- ====================================================================
-- DROP TABLE IF EXISTS silver.products;

-- STEP 1: Create table (persistent)
CREATE TABLE IF NOT EXISTS silver.products (
	product_pk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, 
	product_id TEXT UNIQUE NOT NULL, -- Natural Key
	-- Categorization
	category_name_en TEXT ,
	-- Metadata
	name_length INT,
	description_length INT,
	photos_qty INT,
	-- Physical Features (For logistic Analysis)
	weight_g FLOAT,
	length_cm FLOAT,
	height_cm FLOAT,
	width_cm FLOAT,
	volume_cm3 FLOAT, -- (L*H*W) for determining shipping costs 

	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


