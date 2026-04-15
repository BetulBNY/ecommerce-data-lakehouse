/*
===============================================================================
PROFESSIONAL DATA QUALITY CHECK: silver.category_translation
===============================================================================
*/

WITH test_results AS (
    SELECT
        -- TEST 1: Missing English Translation
        (SELECT COUNT(*) FROM silver.category_translation WHERE category_name_en IS NULL) AS count_null_translations
)
SELECT 
    CASE 
        WHEN (count_null_translations) = 0 THEN 1
        ELSE 0 
    END AS validation_status
FROM test_results;