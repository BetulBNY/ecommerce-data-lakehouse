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
-- Transform 'bronze.olist_customers'
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

-- ====================================================================
-- Transform 'bronze.olist_geolocation'
-- ====================================================================
-- Analysis Results:
    -- There is no id/primary key
    -- No nulls in zip code
    -- 1 value in geolocation_city has unwanted space and also I will transform them to start with uppercase.
    -- When we look at coordinate (lat and lng) data, 11685 rows are outliers, meaning they point outside Brazil. 
        -- In this case, I will take only the coordinates within Brazil's range.
    -- Apart from these, since I will make zip code the primary key, each zip code will be a single row and I will take the average of the others    

CREATE TABLE silver.olist_geolocation AS
-- 1. STEP: Select only reasonable coordinates within Brazil
WITH filtered_geo AS (
    SELECT 
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        INITCAP(TRIM(geolocation_city)) AS geolocation_city,
        UPPER(geolocation_state) AS geolocation_state
    FROM bronze.olist_geolocation
    WHERE geolocation_lat BETWEEN -35 AND 5 
    AND geolocation_lng BETWEEN -74 AND -35
),
-- 2. STEP: Find the most frequent city and state for each zip_code [1 name per zip]
city_voting AS(
        SELECT 
            geolocation_zip_code_prefix,
            geolocation_city,
            geolocation_state,
            COUNT(*) AS cnt,
            ROW_NUMBER() OVER(PARTITION BY geolocation_zip_code_prefix ORDER BY COUNT(*) DESC) as rank
        FROM filtered_geo
        GROUP BY geolocation_zip_code_prefix, geolocation_city, geolocation_state
        --WHERE rank = 1
        ),
-- 3. STEP: Take the average of coordinates for each zip_code   [1 coordinate per zip]
lat_lang_avg AS (
    SELECT 
        geolocation_zip_code_prefix,
        AVG(geolocation_lat) AS geolocation_lat,
        AVG(geolocation_lng) AS geolocation_lng        
    FROM filtered_geo
    GROUP BY geolocation_zip_code_prefix
)
SELECT 
    l.geolocation_zip_code_prefix,
    l.geolocation_lat,
    l.geolocation_lng,
    c.geolocation_city,
    c.geolocation_state
FROM lat_lang_avg l
JOIN city_voting c ON l.geolocation_zip_code_prefix = c.geolocation_zip_code_prefix
WHERE c.rank = 1;


















