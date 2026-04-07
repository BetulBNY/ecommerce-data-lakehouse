-- ====================================================================
-- Transform 'silver.sellers'
-- ====================================================================
-- Analysis Results:
	-- There is no nulls and duplicates in ID's.
	-- Also in other columns there is no nulls.
	-- Zip codes of sellers and geolocation table is matches.
	-- Some zip codes have more then 1 city so, I will match sellers table zip code and silver.geolocations tables' zip code and get cleaned cities and states.
	-- In conclusion while createing silver.sellers table I will use silver.geolocations tables' cleaned data instead of bronze.olist_sellers. 
		-- With this way whole address system (customers and sellers) will be matched %100.
		-- Now it matches but in the future if zip code doesn't match with any zip code in geolocation table
		-- I will use COALESCE. First it will prefer silver.geolocations' data then if it dıesn't matches 
		-- cleaned version of seller table.

-- STEP 2: ETL / Pipeline – run this query each time (UPSERT)
INSERT INTO silver.sellers (
	seller_id,
	zip_code_prefix,
	city,
	state
)
SELECT
	s.seller_id,
	s.seller_zip_code_prefix,
	-- Master Data (Geolocation) has priority, if it is null then we get sellers own data(manually entered)
	COALESCE(g.city, INITCAP(TRIM(s.seller_city))) AS city,
	COALESCE(g.state, UPPER(TRIM(s.seller_state))) AS state
FROM bronze.olist_sellers s
LEFT JOIN silver.geolocation g
	ON s.seller_zip_code_prefix = g.zip_code
ON CONFLICT(seller_id)
DO UPDATE SET
	zip_code_prefix = EXCLUDED.zip_code_prefix,
	city = EXCLUDED.city,
	state = EXCLUDED.state,
	updated_at = CURRENT_TIMESTAMP;
