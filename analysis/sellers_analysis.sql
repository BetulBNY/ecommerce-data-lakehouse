-- ====================================================================
-- Checking 'bronze.olist_sellers'
-- ====================================================================
SELECT * FROM bronze.olist_sellers;

---- 1) Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT
seller_id,
COUNT(*)
FROM bronze.olist_sellers
GROUP BY seller_id
HAVING COUNT(*) > 1 OR seller_id IS NULL;
-- No duplicates or nulls

---- 2) Check if there are nulls in other columns as well
-- Expectation: No Results
SELECT 
seller_id,
seller_zip_code_prefix,
seller_city,
seller_state
FROM bronze.olist_sellers
WHERE 
	seller_zip_code_prefix IS NULL
	OR
	seller_city IS NULL
	OR
	seller_state IS NULL;
-- No nulls

---- 3) Check total, unique sellers and unique zips.
SELECT
	COUNT(*) AS total_sellers,
	COUNT(DISTINCT seller_id) AS distinct_sellers,
	COUNT(DISTINCT seller_zip_code_prefix) AS unique_zip_codes
FROM bronze.olist_sellers;

---- 4) Check if every zip code is in geolocation table, they match
SELECT 
	s.seller_id,
	s.seller_zip_code_prefix,
	s.seller_city,
	s.seller_state,
	g.zip_code AS geo_zip,
	g.city AS geo_city,
	g.state AS geo_state
FROM bronze.olist_sellers s
LEFT JOIN silver.geolocation g
ON g.zip_code = s.seller_zip_code_prefix;
-- Yes they matched

---- 5) Check if a zip code has more than 1 city
SELECT 
	seller_zip_code_prefix,
	COUNT(DISTINCT seller_city)
FROM bronze.olist_sellers
GROUP BY seller_zip_code_prefix
HAVING COUNT(DISTINCT seller_city) > 1;
-- 34 zip codes have 2 cities

---- 6) Check if there are any difference between their cities and states.
SELECT 
	s.seller_id,
	s.seller_zip_code_prefix,
	INITCAP(TRIM(s.seller_city)),
	TRIM(s.seller_state),
	g.zip_code AS geo_zip,
	g.city AS geo_city,
	TRIM(g.state) AS geo_state
FROM bronze.olist_sellers s
LEFT JOIN silver.geolocation g
	ON g.zip_code = s.seller_zip_code_prefix
WHERE 
	s.seller_city != g.city
	OR
	s.seller_state != g.state
-- 3081 rows are different (even they seem same)












