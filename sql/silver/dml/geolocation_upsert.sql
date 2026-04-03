-- ====================================================================
-- Transform 'silver.geolocation'
-- ====================================================================
-- Analysis Results:
    -- There is no id/primary key
    -- No nulls in zip code
    -- 1 value in geolocation_city has unwanted space and also I will transform them to start with uppercase.
    -- When we look at coordinate (lat and lng) data, 11685 rows are outliers, meaning they point outside Brazil. 
        -- In this case, I will take only the coordinates within Brazil's range.
    -- Apart from these, since I will make zip code the primary key, each zip code will be a single row and I will take the average of the others    

-- STEP 2: ETL / Pipeline – run this query each time (UPSERT)
INSERT INTO silver.geolocation (zip_code, latitude, longitude, city, state)
-- 1. STEP: Select only reasonable coordinates within Brazil
WITH filtered_geo AS (
    SELECT 
        geolocation_zip_code_prefix as zip_code,
        geolocation_lat as lat,
        geolocation_lng as lng,
        INITCAP(TRIM(geolocation_city)) AS city,
        UPPER(geolocation_state) AS state
    FROM bronze.olist_geolocation
    WHERE geolocation_lat BETWEEN -35 AND 5 
      AND geolocation_lng BETWEEN -74 AND -35
),
-- 2. STEP: Find the most frequent city and state for each zip_code [1 name per zip]
city_voting AS (
    SELECT 
	zip_code,
	city, 
	state,
    ROW_NUMBER() OVER(PARTITION BY zip_code ORDER BY COUNT(*) DESC) as rank
    FROM filtered_geo
    GROUP BY zip_code, city, state
	--WHERE rank = 1
),
-- 3. STEP: Take the average of coordinates for each zip_code   [1 coordinate per zip]
lat_lng_avg AS (
    SELECT 
	zip_code, 
	AVG(lat) as lat, 
	AVG(lng) as lng        
    FROM filtered_geo
    GROUP BY zip_code
)
SELECT 
    l.zip_code,
	l.lat,
	l.lng,
	c.city,
	c.state
FROM lat_lng_avg l
JOIN city_voting c ON l.zip_code = c.zip_code
WHERE c.rank = 1
ON CONFLICT (zip_code) 
DO UPDATE SET 
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    city = EXCLUDED.city,
	state = EXCLUDED.state,
    updated_at = CURRENT_TIMESTAMP;