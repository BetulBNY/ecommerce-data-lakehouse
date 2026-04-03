-- ====================================================================
-- Checking 'silver.category_translation' Table
-- ====================================================================
-- TEST: Missing Translation Check
-- EXPECTATION: No rows should have NULL English category names
SELECT *
FROM silver.category_translation
WHERE category_name_en IS NULL;