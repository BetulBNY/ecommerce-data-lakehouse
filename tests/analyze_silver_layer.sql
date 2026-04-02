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
SELECT * FROM silver.olist_customers;

-- Check Zip Code Consistency
SELECT customer_zip_code_prefix, COUNT(DISTINCT customer_city) as city_count
FROM silver.olist_customers
GROUP BY customer_zip_code_prefix
HAVING COUNT(DISTINCT customer_city) > 1;
-- Result is 0