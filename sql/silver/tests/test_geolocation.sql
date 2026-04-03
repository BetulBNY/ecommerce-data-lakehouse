-- ====================================================================
-- Checking 'silver.geolocation'
-- ====================================================================

-- TEST: Zip Code Uniqueness (Primary Key Check)
-- EXPECTATION: Each zip code should appear only once in the table
SELECT 
    zip_code, 
    COUNT(*) as occurrences
FROM silver.geolocation
GROUP BY zip_code
HAVING COUNT(*) > 1;