-- ====================================================================
-- Create 'gold.dim_date' Table
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_id INT PRIMARY KEY,        -- YYYYMMDD format (ex: 20180101)
    full_date DATE UNIQUE,          -- REal date (2018-01-01)
    year INT,
    quarter INT,
    month INT,
    month_name TEXT,
    day INT,
    day_of_week INT,
    day_name TEXT,
    week_of_year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE
);