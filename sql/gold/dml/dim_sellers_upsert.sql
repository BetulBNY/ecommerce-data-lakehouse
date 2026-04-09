-- ===============================================================================
-- GOLD LAYER DIMENSION DESIGN: gold.dim_sellers
-- ===============================================================================
/*
Why Denormalize with Geolocation?
    - Direct Analytics: By embedding latitude and longitude directly into the 
      Customer Dimension, analysts can perform spatial analysis without 
      complex JOINs with the 'geolocation' reference table.
*/
INSERT INTO gold.dim_sellers (
    seller_pk, 
	seller_id, 
	city, 
	state, 
	zip_code, 
	latitude, 
	longitude
)
SELECT 
    s.seller_pk,
    s.seller_id,
    s.city,
    s.state,
    s.zip_code_prefix,
    g.latitude,
    g.longitude
FROM silver.sellers s
LEFT JOIN silver.geolocation g ON s.zip_code_prefix = g.zip_code
ON CONFLICT (seller_pk) 
DO UPDATE SET 
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    zip_code = EXCLUDED.zip_code,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    updated_at = CURRENT_TIMESTAMP;