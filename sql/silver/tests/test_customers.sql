-- ====================================================================
-- Checking 'silver.customers'
-- ====================================================================

SELECT * FROM silver.olist_customers;

-- TEST: Zip Code Consistency
-- EXPECTATION: Each zip code should be associated with only one city
SELECT 
	customer_zip_code_prefix, 
	COUNT(DISTINCT customer_city) as city_count
FROM silver.olist_customers
GROUP BY customer_zip_code_prefix
HAVING COUNT(DISTINCT customer_city) > 1;
-- Result is 0


