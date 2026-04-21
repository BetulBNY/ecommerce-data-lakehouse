/*
===============================================================================
PROFESSIONAL DATA QUALITY CHECK: silver.products
===============================================================================
Logic:
	For each test, we calculate the number of erroneous rows.
	If the total number of errors is 0, the query returns '1' (TRUE) and Airflow turns green.
	If there is even a single error, it returns '0' (FALSE) and Airflow turns RED.

===============================================================================
*/

WITH test_results AS (
    SELECT
        -- TEST 1: Duplicate or Null IDs (Expectation: 0 rows (product_id must be unique and present))
        (SELECT COUNT(*) FROM (
            SELECT 
				product_id 
			FROM silver.products 
			GROUP BY 1 
			HAVING COUNT(*) > 1 OR product_id IS NULL
        ) t) AS count_ids,

        -- TEST 2: Invalid Dimensions/Zeros (Expectation: 0 rows (We imputed zeros with category means, so no 0s should remain))
        (SELECT COUNT(*) 
		 FROM silver.products 
         WHERE weight_g <= 0 OR length_cm <= 0 OR height_cm <= 0 OR width_cm <= 0 OR volume_cm3 <= 0
        ) AS count_zeros,

        -- TEST 3: Standardization/Underscores (Expectation: 0 rows (No categories should have underscores or be NULL))
        (SELECT COUNT(*) 
		 FROM silver.products 
         WHERE category_name_en LIKE '%\_%' OR category_name_en IS NULL
        ) AS count_formatting,

        -- TEST 4: Data Integrity (Uncategorized Logic)
        -- Soru: Bronze'da NULL olup da Silver'da 'Uncategorized' OLMAYAN var mı? (Hata kontrolü)
        (SELECT COUNT(*) 
         FROM bronze.olist_products op
         LEFT JOIN silver.products p 
		 	ON op.product_id = p.product_id
         WHERE op.product_category_name IS NULL AND p.category_name_en != 'Uncategorized'
        ) AS count_logic_errors,

        -- TEST 5: Volumetric Math Check (Beklenti: 0)
        (SELECT COUNT(*) FROM silver.products 
         WHERE ABS(volume_cm3 - (height_cm * length_cm * width_cm)) > 0.1
        ) AS count_math_errors
)
SELECT 
    CASE 
        WHEN (count_ids + count_zeros + count_formatting + count_logic_errors + count_math_errors) = 0 THEN 1
        ELSE 0 
    END AS validation_status
FROM test_results;
