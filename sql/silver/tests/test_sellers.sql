-- ====================================================================
-- Checking 'silver.sellers'
-- ====================================================================
SELECT * FROM silver.sellers;

---- 1) Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT
seller_id,
COUNT(*)
FROM silver.sellers
GROUP BY seller_id
HAVING COUNT(*) > 1 OR seller_id IS NULL;
-- No duplicates or nulls

---- 2) Check if there are nulls in other columns as well
-- Expectation: No Results
SELECT 
seller_id,
zip_code_prefix,
city,
state
FROM silver.sellers
WHERE 
	zip_code_prefix IS NULL
	OR
	city IS NULL
	OR
	state IS NULL;
-- No nulls

---- 3) Check total, unique sellers and unique zips.
SELECT
	COUNT(*) AS total_sellers,
	COUNT(DISTINCT seller_id) AS distinct_sellers,
	COUNT(DISTINCT zip_code_prefix) AS unique_zip_codes
FROM silver.sellers;

---- 4) Check if every zip code is in geolocation table, they match
SELECT 
	s.seller_id,
	s.zip_code_prefix,
	s.city,
	s.state,
	g.zip_code AS geo_zip,
	g.city AS geo_city,
	g.state AS geo_state
FROM silver.sellers s
LEFT JOIN silver.geolocation g
ON g.zip_code = s.zip_code_prefix;
-- Yes they matched

---- 5) Check if a zip code has more than 1 city
SELECT 
	zip_code_prefix,
	COUNT(DISTINCT city)
FROM silver.sellers
GROUP BY zip_code_prefix
HAVING COUNT(DISTINCT city) > 1;
-- 0 row

---- 6) Check if there are any difference between their cities and states.
SELECT 
	s.seller_id,
	s.zip_code_prefix,
	INITCAP(TRIM(s.city)),
	TRIM(s.state),
	g.zip_code AS geo_zip,
	g.city AS geo_city,
	TRIM(g.state) AS geo_state
FROM silver.sellers s
LEFT JOIN silver.geolocation g
	ON g.zip_code = s.zip_code_prefix
WHERE 
	s.city != g.city
	OR
	s.state != g.state;
-- 0 row

---- 7) Check Coverage (How many sellers are using fallback data?)
-- This is not an error, a state report. 
SELECT 
    COUNT(*) as total_sellers,
    COUNT(g.zip_code) as matched_with_geo,
    COUNT(*) - COUNT(g.zip_code) as fallback_count
FROM silver.sellers s
LEFT JOIN silver.geolocation g 
	ON s.zip_code_prefix = g.zip_code;



