-- ====================================================================
-- Checking 'silver.customers'
-- ====================================================================

SELECT * FROM silver.customers;

-- TEST: Zip Code Consistency
-- EXPECTATION: Each zip code should be associated with only one city
SELECT 
	zip_code_prefix, 
	COUNT(DISTINCT city) as city_count
FROM silver.customers
GROUP BY zip_code_prefix
HAVING COUNT(DISTINCT city) > 1;
-- Result is 0

