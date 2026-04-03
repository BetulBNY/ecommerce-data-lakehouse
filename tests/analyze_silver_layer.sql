-- ANALYZE SILVER LAYER
/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.olist_customers'
-- ====================================================================
SELECT * FROM silver.olist_customers;

-- Check Zip Code Consistency
SELECT customer_zip_code_prefix, COUNT(DISTINCT customer_city) as city_count
FROM silver.olist_customers
GROUP BY customer_zip_code_prefix
HAVING COUNT(DISTINCT customer_city) > 1;
-- Result is 0


-- ====================================================================
-- Checking 'silver.olist_geolocation'
-- ====================================================================
SELECT * FROM silver.olist_geolocation;


---- Check for Duplicates in Primary Key / ZIP codes
-- Expectation: No Results
SELECT geolocation_zip_code_prefix, COUNT(*)
FROM silver.olist_geolocation
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(*) > 1;
-- Here the purpose was every zip code must belong to just one city but one city can have more than  zip code.






