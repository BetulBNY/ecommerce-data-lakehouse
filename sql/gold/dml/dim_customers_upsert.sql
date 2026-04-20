/*
===============================================================================
GOLD LAYER DIMENSION DESIGN: gold.dim_customers
===============================================================================
Objective:
    Standardize customer data and enrich it with geospatial coordinates to 
    enable high-performance analytical reporting (e.g., Heatmaps, Regional Sales).

Why Denormalize with Geolocation?
    - Direct Analytics: By embedding latitude and longitude directly into the 
      Customer Dimension, analysts can perform spatial analysis without 
      complex JOINs with the 'geolocation' reference table.
    - Performance: Reduces the query execution time for dashboard visualizations.
    - Consistency: Uses the "Master" coordinates calculated during the Silver 
      layer transformation (average coordinates per ZIP code).

Granularity:
    - One record per unique customer (customer_pk).
===============================================================================
*/

-- ====================================================================
-- Transform 'gold.dim_customers'
-- ====================================================================
-- I only used UPSERT logic again to add new or changed data.
INSERT INTO gold.dim_customers (
    customer_pk, customer_id, customer_unique_id, 
    city, state, zip_code, latitude, longitude
)
SELECT 
    c.customer_pk,
    c.customer_id,
    c.customer_unique_id,
    c.city,
    c.state,
    c.zip_code_prefix,
    g.latitude,
    g.longitude
FROM silver.customers AS c
LEFT JOIN silver.geolocation AS g ON c.zip_code_prefix = g.zip_code
ON CONFLICT (customer_pk) 
DO UPDATE SET 
    city = excluded.city,
    state = excluded.state,
    zip_code = excluded.zip_code,
    latitude = excluded.latitude,
    longitude = excluded.longitude,
    updated_at = CURRENT_TIMESTAMP;
