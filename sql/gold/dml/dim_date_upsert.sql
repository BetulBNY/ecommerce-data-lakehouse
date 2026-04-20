-- ===============================================================================
-- GOLD LAYER DIMENSION GENERATION: gold.dim_date
-- ===============================================================================
/*
Objective:
    Generate a continuous calendar dimension to enable advanced time-series 
    analysis (Year-over-Year, Month-over-Month, Seasonal trends).
Range: 
    2015-01-01 to 2020-12-31 (Covers the full Olist dataset timeframe).
===============================================================================
*/
INSERT INTO gold.dim_date (
    date_id, 
	full_date, 
	year, 
	quarter, 
	month, 
	month_name, 
    day, 
	day_of_week, 
	day_name, 
	week_of_year, 
	is_weekend
)
SELECT 
    TO_CHAR(datum, 'YYYYMMDD')::INT AS date_id,
    datum AS full_date,
    EXTRACT(YEAR FROM datum) AS year,
    EXTRACT(QUARTER FROM datum) AS quarter,
    EXTRACT(MONTH FROM datum) AS month,
    TO_CHAR(datum, 'Month') AS month_name,
    EXTRACT(DAY FROM datum) AS day,
    EXTRACT(ISODOW FROM datum) AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_name,
    EXTRACT(WEEK FROM datum) AS week_of_year,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    -- I am creating a list that includes all the days from 2015 to 2020.
    SELECT '2015-01-01'::DATE + sequence.day AS datum
    FROM generate_series(0, 5478) AS sequence(day) -- 365 * 15 day approximately
) AS calendar
ON CONFLICT (date_id) DO NOTHING;