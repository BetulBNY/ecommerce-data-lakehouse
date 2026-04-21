-- ====================================================================
-- Transform 'silver.category_translation'
-- ====================================================================
-- Analysis Notes:
-- - There is no id / primary key
-- - No NULL values
-- - Add a surrogate key since ID is missing
-- - Ensure it covers all categories in the products table
-- - Standardize names (INITCAP, replace underscores)
-- - 2 product categories exist in products table but not in translation table. Will handle separately.

-- STEP 2: ETL / Pipeline – run this query each time (UPSERT)
-- I didnt touch product_category_name, because it is Join key. If I touch it it would also affect produtcts table. 
INSERT INTO silver.category_translation (category_name_pt, category_name_en)
SELECT 
	product_category_name,
	INITCAP(REPLACE(product_category_name_english, '_',' ')) AS category_name_en)
FROM bronze.olist_category_translation
ON CONFLICT (category_name_pt)
DO UPDATE SET 
	category_name_en = EXCLUDED.category_name_en,
	updated_at = CURRENT_TIMESTAMP;
