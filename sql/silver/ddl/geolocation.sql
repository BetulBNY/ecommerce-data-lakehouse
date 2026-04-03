-- ====================================================================
-- Create 'silver.geolocation' Table
-- ====================================================================
-- STEP 1: Create table (persistent)
CREATE TABLE IF NOT EXISTS silver.geolocation (
    geo_pk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    zip_code INT UNIQUE,
    latitude FLOAT,
    longitude FLOAT,
    city TEXT,
    state TEXT,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

