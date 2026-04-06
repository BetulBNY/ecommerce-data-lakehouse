-- ====================================================================
-- Quality Checks for 'silver.products' Table
-- ====================================================================
SELECT * FROM silver.products;

-- 1. TEST: Duplicate or Null Product IDs
-- Expectation: 0 rows (product_id must be unique and present)
SELECT 
product_id,
COUNT(*)
FROM silver.products
GROUP BY product_id
HAVING COUNT(*) > 1 OR product_id IS NULL;

-- 2. TEST: Invalid Physical Dimensions (The "Zero" Check)
-- Expectation: 0 rows (We imputed zeros with category means, so no 0s should remain)
SELECT 
	product_id,
	category_name_en,
	weight_g,
	length_cm,
	height_cm,
	width_cm,
	volume_cm3
FROM silver.products
WHERE weight_g <= 0 OR  length_cm <= 0 OR height_cm <= 0 OR width_cm <= 0 OR volume_cm3 <= 0;

-- 3. TEST: Category Standardization Check
-- Expectation: 0 rows (No categories should have underscores or be NULL)
SELECT 
	product_id,
	category_name_en
FROM silver.products
WHERE category_name_en LIKE '%\_%'
	OR category_name_en IS NULL 

-- 4. TEST: Data Integrity for Uncategorized Items
-- Expectation: List of products that were originally NULL (to verify they became 'Uncategorized')
SELECT * FROM silver.products;
SELECT 
	p.product_id,
	p.category_name_en,
	op. product_category_name
FROM silver.products p
LEFT JOIN bronze.olist_products op
	ON op.product_id = p.product_id
WHERE category_name_en = 'Uncategorized';

-- 5. TEST: Volumetric Logic Check
-- Expectation: 0 rows (Volume must be the product of L * H * W)
SELECT 
	product_id,
	volume_cm3,
	(height_cm * length_cm * width_cm) AS multiplication
FROM silver.products 
WHERE ABS(volume_cm3 - (height_cm * length_cm * width_cm)) > 0.1;
	














