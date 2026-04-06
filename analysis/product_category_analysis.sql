-- ====================================================================
-- Checking 'bronze.category_translation'
-- ====================================================================
SELECT * FROM bronze.olist_category_translation;

---- 1) Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
	product_category_name,
	COUNT(*)
FROM bronze.olist_category_translation
GROUP BY product_category_name
HAVING COUNT(*) > 1 OR product_category_name IS NULL;
-- No nulls or duplicates

---- 2) Check if there are nulls in other columns as well (i.e., check if there are empty rows by themselves)
SELECT
	COUNT(*) AS total_row_num,
	COUNT(*) - COUNT(product_category_name_english)
FROM bronze.olist_category_translation;
-- No nulls

---- 3) Check if every category in the products table is recorded in the translation table
SELECT 
	p.product_id,
	p.product_category_name,
	t.product_category_name,
	t.product_category_name_english
FROM bronze.olist_products p
LEFT JOIN bronze.olist_category_translation t
	ON p.product_category_name = t.product_category_name
WHERE p.product_category_name IS NOT NULL -- The product has a category
  AND t.product_category_name_english IS NULL; -- But we don't have its English version
-- It does not cover 2 products: portateis_cozinha_e_preparadores_de_alimentos and pc_gamer.
-- These 2 values exist in olist_products but not in olist_category_translation.

-- ====================================================================
-- Checking 'bronze.olist_products'
-- ====================================================================
SELECT * FROM bronze.olist_products;

---- 1) Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    product_id,
    COUNT(*)
FROM bronze.olist_products
GROUP BY product_id 
HAVING COUNT(*) > 1 OR product_id IS NULL;
-- No duplicates or nulls

---- 2)GENERAL HEALTH REPORT OF THE TABLE: check how many NULLs in each column
SELECT 
    COUNT(*) as total_products,
    COUNT(product_category_name) as products_with_category,
    COUNT(*) - COUNT(product_category_name) as missing_category,
    COUNT(*) - COUNT(product_weight_g) as missing_weight,
	COUNT(*) - COUNT(product_length_cm) as missing_length,
	COUNT(*) - COUNT(product_height_cm) as missing_height
FROM bronze.olist_products;

---- 3) Check how many different types of products exist
SELECT DISTINCT product_category_name
FROM bronze.olist_products
ORDER BY product_category_name;
-- 1 null category  -- Buna diğer /n/a değeri ata veya unknown

---- 4) Count how many products exist in each category
SELECT 
product_category_name,
COUNT(*)
FROM bronze.olist_products
GROUP BY product_category_name
ORDER BY COUNT(*) DESC;

---- 5) Check for outliers in size and weight
SELECT 
product_category_name,
COUNT(*),
MAX(product_weight_g) AS max_weight,
MIN(product_weight_g) AS min_weight,
AVG(product_weight_g) AS avg_weight,

MAX(product_length_cm) AS max_length,
MIN(product_length_cm) AS min_length,
AVG(product_length_cm) AS avg_length,

MAX(product_height_cm) AS max_height,
MIN(product_height_cm) AS min_height,
AVG(product_height_cm) AS avg_height
FROM bronze.olist_products
GROUP BY product_category_name
ORDER BY COUNT(*) DESC;

/*
Why are product weight (grams) and dimensions (cm) important?
Later, when analyzing the orders table: "Do heavier products take longer to ship?" or "Shipping cost analysis for large packages", 
I need to have these numerical values in the products table perfectly clean.
*/











