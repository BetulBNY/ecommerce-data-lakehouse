/*
===============================================================================
PROFESSIONAL DATA QUALITY CHECK: silver.customers
===============================================================================
Logic:
    Returns '1' if all checks pass, '0' if any validation fails.
===============================================================================
*/

WITH test_results AS (
    SELECT
        -- TEST 1: Uniqueness of Customer ID (Expectation: 0)
        (SELECT COUNT(*) FROM (
            SELECT customer_id 
			FROM silver.customers 
			GROUP BY 1 HAVING COUNT(*) > 1 OR customer_id IS NULL
        ) t) AS count_id_errors,

        -- TEST 2: Zip Code Consistency (Expectation: 0)
        -- Each zip_code_prefix must point to only one city.
        (SELECT COUNT(*) FROM (
            SELECT zip_code_prefix 
			FROM silver.customers 
			GROUP BY 1 HAVING COUNT(DISTINCT city) > 1
        ) t) AS count_consistency_errors,

        -- TEST 3: Null Values in Critical Columns (Expectation: 0)
        (SELECT COUNT(*) 
		 FROM silver.customers 
         WHERE city IS NULL OR state IS NULL OR customer_unique_id IS NULL
        ) AS count_null_errors
)
SELECT 
    CASE 
        WHEN (count_id_errors + count_consistency_errors + count_null_errors) = 0 THEN 1
        ELSE 0 
    END AS validation_status
FROM test_results;