/*
===============================================================================
PROFESSIONAL DATA QUALITY CHECK: silver.geolocation
===============================================================================
Logic:
    Returns '1' if all checks pass, '0' if any validation fails.
===============================================================================
*/

WITH test_results AS (
    SELECT
        -- TEST 1: Uniqueness of ZIP Code (Expectation: 0)
        -- Since zip_code is our PK in this table, it must be unique.
        (SELECT COUNT(*) FROM (
            SELECT zip_code 
			FROM silver.geolocation 
			GROUP BY 1 HAVING COUNT(*) > 1 OR zip_code IS NULL
        ) t) AS count_pk_errors,

        -- TEST 2: Lat/Long Bound Check (Expectation: 0)
        -- Coordinates should be within Brazil's rough boundaries.
        (SELECT COUNT(*) 
		 FROM silver.geolocation 
         WHERE latitude > 5 OR latitude < -35
            OR longitude > -35 OR longitude < -74
        ) AS count_outlier_errors,

        -- TEST 3: Missing Coordinates (Expectation: 0)
        (SELECT COUNT(*) 
		 FROM silver.geolocation 
         WHERE latitude IS NULL OR longitude IS NULL
        ) AS count_missing_data
)
SELECT 
    CASE 
        WHEN (count_pk_errors + count_outlier_errors + count_missing_data) = 0 THEN 1
        ELSE 0 
    END AS validation_status
FROM test_results;