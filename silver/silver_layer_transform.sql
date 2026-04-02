-- TRANSFORM SILVER LAYER
/*
STEPS: 
1. Data Cleaning
    NULL handling
    TRIM
    invalid character cleaning
    datatype correction
    TRIM(customer_city)
2. Standardization
    Case normalization (UPPER / INITCAP)
    format consistency
    UPPER(customer_state)
3. Deduplication
    remove duplicate records
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ...)
4. Apply Business Rule
    same zip code → multiple cities → select the most frequent city
5. Basic Enrichment
    add new columns (not heavy business logic)
*/

-- ====================================================================
-- Checking 'bronze.olist_customers'
-- ====================================================================
-- Analysis Results:
    -- There are invalid characters in the customer_city column. However, since it occurs in only one row among 100k rows, I am not touching it.
    -- I will capitalize the first letters of city names
    -- For the same postal codes, multiple city matches exist. Using voting (selecting the most frequent one).
    -- There were no spaces in customer_city, but I added TRIM in case future incoming data has spaces
    -- customer_state is written in uppercase, but considering possible lowercase entries in future, I am converting to uppercase.
    -- No NULL values in ids
    -- Datatypes are appropriate

-- Creating Silver Table
CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.olist_customers;

CREATE TABLE silver.olist_customers AS
WITH base_cleaned AS (
    SELECT 
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        INITCAP(TRIM(customer_city)) AS customer_city , -- standardization
        UPPER(customer_state) AS customer_state          -- consistency    
    FROM bronze.olist_customers
),
voting_logic AS (
    -- Find the most popular city/state match for each postal code
    SELECT 
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        ROW_NUMBER() OVER(PARTITION BY customer_zip_code_prefix ORDER BY COUNT(*) DESC) as rank
    FROM base_cleaned
    GROUP BY customer_zip_code_prefix, customer_city, customer_state
)
SELECT 
    b.customer_id,
    b.customer_unique_id,
    b.customer_zip_code_prefix,
    v.customer_city,
    v.customer_state,
    CURRENT_TIMESTAMP as created_at -- To track when the data was processed
FROM base_cleaned b
JOIN voting_logic v ON b.customer_zip_code_prefix = v.customer_zip_code_prefix
WHERE v.rank = 1;