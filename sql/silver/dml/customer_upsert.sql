-- ====================================================================
-- Transform 'silver.customers'
-- ====================================================================
-- Analysis Results:
    -- There are invalid characters in the customer_city column. However, since it occurs in only one row among 100k rows, I am not touching it.
    -- I will capitalize the first letters of city names
    -- For the same postal codes, multiple city matches exist. Using voting (selecting the most frequent one).
    -- There were no spaces in customer_city, but I added TRIM in case future incoming data has spaces
    -- customer_state is written in uppercase, but considering possible lowercase entries in future, I am converting to uppercase.
    -- No NULL values in ids
    -- Datatypes are appropriate

-- STEP 2: ETL / Pipeline – run this query each time (UPSERT)
INSERT INTO silver.customers (
    customer_id, 
    customer_unique_id, 
    zip_code_prefix, -- Silver name
    city,            -- Silver name
    state            -- Silver name
)
WITH base_cleaned AS (
    SELECT 
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix, -- Bronze name
        INITCAP(TRIM(customer_city)) AS cleaned_city, -- Bronze name  standardization
        UPPER(customer_state) AS cleaned_state        -- Bronze name  consistency 
    FROM bronze.olist_customers
),
voting_logic AS ( 
	-- Find the most popular city/state match for each postal code
    SELECT 
        customer_zip_code_prefix,
        cleaned_city,
        cleaned_state,
        ROW_NUMBER() OVER(PARTITION BY customer_zip_code_prefix ORDER BY COUNT(*) DESC) as rank
    FROM base_cleaned
    GROUP BY customer_zip_code_prefix, cleaned_city, cleaned_state
)
SELECT 
    b.customer_id,
    b.customer_unique_id,
    b.customer_zip_code_prefix,
    v.cleaned_city,
    v.cleaned_state
FROM base_cleaned b
INNER JOIN voting_logic v ON b.customer_zip_code_prefix = v.customer_zip_code_prefix
WHERE v.rank = 1
ON CONFLICT (customer_id) 
DO UPDATE SET 
    zip_code_prefix = EXCLUDED.zip_code_prefix, 
    city = EXCLUDED.city,                      
    state = EXCLUDED.state,                    
    updated_at = CURRENT_TIMESTAMP;