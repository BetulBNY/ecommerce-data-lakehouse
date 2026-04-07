-- ====================================================================
-- Create 'silver.sellers' Table
-- ====================================================================
-- STEP 1: Create table (persistent)
CREATE TABLE IF NOT EXISTS silver.sellers (
	seller_pk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	seller_id TEXT UNIQUE,
	zip_code_prefix INT,
	city TEXT,
	state TEXT,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);