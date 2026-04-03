-- ANALYZE BRONZE LAYER
/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'bronze' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks before data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'bronze.olist_customers'
-- ====================================================================

SELECT * FROM bronze.olist_customers;
-- NOTES:
    -- customer_id: Unique per each order. If a customer makes 3 different orders, there will be 3 different customer_ids.
    -- customer_unique_id: Represents the actual person.

---- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT customer_id, COUNT(*) 
FROM bronze.olist_customers
GROUP BY customer_id
HAVING COUNT(*) > 1 OR customer_id IS NULL;
-- No duplicates or nulls

SELECT customer_unique_id, COUNT(*) 
FROM bronze.olist_customers
GROUP BY customer_unique_id
HAVING COUNT(*) > 1 OR customer_unique_id IS NULL;
-- There are duplicates

---- Check for Unwanted Spaces
SELECT customer_city
FROM bronze.olist_customers
WHERE TRIM(customer_city) != customer_city;
-- There are no unwanted spaces

SELECT customer_state
FROM bronze.olist_customers
WHERE TRIM(customer_state) != customer_state;
-- There are no unwanted spaces

---- Check for consistency
SELECT DISTINCT customer_city
FROM bronze.olist_customers;
-- 4119 different cities

-- Standardization Analysis: Convert city names to lowercase and count again: (If the number changes, some may be "Sao Paulo" while others are "SAO PAULO".)
SELECT COUNT(DISTINCT LOWER(customer_city)) 
FROM bronze.olist_customers;
-- No change

----------- ANALYSIS:
-- Analyze how many times a customer has made a purchase (Loyalty)
SELECT customer_unique_id, COUNT(*) 
FROM bronze.olist_customers
GROUP BY customer_unique_id
ORDER BY COUNT(*) DESC;

-- Anomaly Analysis (Cities with the fewest customers): Hundreds of cities may have only 1 customer. Are these typos or real?
SELECT customer_city, COUNT(*) 
FROM bronze.olist_customers
GROUP BY customer_city
ORDER BY COUNT(*);

-- How many cities have only 1 customer?
SELECT COUNT(*)
FROM(
    SELECT COUNT(*) 
    FROM bronze.olist_customers
    GROUP BY customer_city
    HAVING COUNT(customer_city) = 1
    );

-- A. Zip Code Consistency
-- In Brazil, the first few digits of the postal code indicate the region. Does the same postal code appear in different cities?

SELECT customer_zip_code_prefix, COUNT(DISTINCT customer_city) as city_count
FROM bronze.olist_customers
GROUP BY customer_zip_code_prefix
HAVING COUNT(DISTINCT customer_city) > 1;

-- B. State Distribution
-- To understand the geographic concentration of customers:

SELECT customer_state, COUNT(*) as total_customers
FROM bronze.olist_customers
GROUP BY customer_state
ORDER BY total_customers DESC;

-- C. Invalid Character Check
-- Are there numbers or strange characters in city names?
SELECT customer_city
FROM bronze.olist_customers
WHERE customer_city ~ '[0-9]'; -- Finds cities containing digits using regex
-- OUTPUT: quilometro 14 do mutum

SELECT customer_city, COUNT(*) as frequency
FROM bronze.olist_customers
WHERE customer_city ~ '[0-9]'
GROUP BY customer_city
ORDER BY frequency DESC;

-- ====================================================================
-- Checking 'bronze.olist_geolocation'
-- ====================================================================
SELECT * FROM bronze.olist_geolocation;

---- Check for NULLs in zip code
-- Expectation: No Results
SELECT COUNT(*)
FROM bronze.olist_geolocation
WHERE geolocation_zip_code_prefix IS NULL;
-- No nulls

---- Check for unwanted spaces
SELECT geolocation_city
FROM bronze.olist_geolocation
WHERE geolocation_city != TRIM(geolocation_city);
-- 1 row has unwanted space

---- Check coordinate boundaries (Outlier values) – analyze values outside Brazil's range
-- Latitude (Lat): between +5 and -35
-- Longitude (Lng): between -35 and -74
-- If there are values far outside this range (e.g., 0,0 or a point in Europe), they are invalid
SELECT * 
FROM bronze.olist_geolocation
WHERE geolocation_lat > 5 OR geolocation_lat < -35
   OR geolocation_lng > -35 OR geolocation_lng < -74;
-- 11685 rows found

---- Are the same zip codes assigned to different states or cities?
SELECT geolocation_zip_code_prefix, COUNT(DISTINCT geolocation_city), COUNT(DISTINCT geolocation_state)
FROM bronze.olist_geolocation
GROUP BY 1
HAVING COUNT(DISTINCT geolocation_city) > 1 OR COUNT(DISTINCT geolocation_state) > 1;
-- Yes, 8559 rows found

-- ====================================================================
-- Checking 'bronze.olist_products'
-- ====================================================================
SELECT * FROM bronze.olist_products;

---- 1) Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    product_id,
    COUNT(*)
FROM bronze.olist_products
GROUP BY product_id 
HAVING COUNT(*) > 1 OR product_id IS NULL;
-- No duplicates or nulls





















