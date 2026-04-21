/*
===============================================================================
PROFESSIONAL DATA QUALITY CHECK: silver.sellers
===============================================================================
Logic:
    Calculates the count of failing rows for each validation test.
    If the sum of all failures is 0, returns '1' (TRUE) -> Task Success.
    If any failure is detected, returns '0' (FALSE) -> Task Failure.
===============================================================================
*/

WITH test_results AS (
    SELECT
        -- TEST 1: Duplicate or Null Seller IDs (Expectation: 0)
        (SELECT COUNT(*) FROM (
            SELECT seller_id 
			FROM silver.sellers 
			GROUP BY 1 HAVING COUNT(*) > 1 OR seller_id IS NULL
        ) t) AS count_id_errors,

        -- TEST 2: Missing Essential Information (Expectation: 0)
        (SELECT COUNT(*) 
		 FROM silver.sellers 
         WHERE zip_code_prefix IS NULL OR city IS NULL OR state IS NULL
        ) AS count_null_errors,

        -- TEST 3: ZIP Code Consistency (Expectation: 0)
        -- Checks if any ZIP code is mapped to more than one city in the silver table.
        (SELECT COUNT(*) FROM (
            SELECT zip_code_prefix 
			FROM silver.sellers 
			GROUP BY 1 HAVING COUNT(DISTINCT city) > 1
        ) t) AS count_consistency_errors,

        -- TEST 4: Master Data Sync Check (Expectation: 0)
        -- Checks if seller city/state matches the master data in silver.geolocation.
        -- Note: We only check where a match exists (inner join logic).
        (SELECT COUNT(*) 
         FROM silver.sellers s
         INNER JOIN silver.geolocation g ON s.zip_code_prefix = g.zip_code
         WHERE s.city != g.city OR s.state != g.state
        ) AS count_sync_errors
)
SELECT 
    CASE 
        WHEN (count_id_errors + count_null_errors + count_consistency_errors + count_sync_errors) = 0 THEN 1
        ELSE 0 
    END AS validation_status
FROM test_results;