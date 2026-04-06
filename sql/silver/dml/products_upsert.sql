-- ====================================================================
-- Transform 'silver.products'
-- ====================================================================
/*
Analysis Results:
	1. Category Name Handling & Translation:
	    - Data Profiling: Identified 610 products with NULL category names and 
	      categories present in the 'products' table but missing in the 
	      'translation' table (e.g., 'pc_gamer').
	    - Decision: Retain all records to ensure revenue integrity in future 
	      financial analyses. Removing products would result in underreporting sales.
	    - Fallback Logic (3-Tier):
	        a. Use English translation from 'silver.category_translation'.
	        b. If missing, format the raw Portuguese name (Replace '_' with ' ', 
	           apply Initcap) to maintain a professional look (e.g., 'pc_gamer' -> 'Pc Gamer').
	        c. If the category is NULL in source, assign 'Uncategorized'.
	
	2. Physical Dimensions & Data Imputation:
	    - Data Profiling: Detected outliers and invalid data (e.g., weight = 0). 
	      Zero-values are critical errors that would lead to "free shipping" 
	      miscalculations in logistics.
	    - Imputation Strategy: Instead of using floor values or leaving them as NULL, 
	      invalid values (0 or NULL) for weight, length, height, and width are 
	      filled with the MEAN value of their respective product category.
	
	3. Feature Engineering & Metadata:
	    - Volumetric Calculation: Added 'volume_cm3' (Length * Height * Width) to 
	      facilitate shipping cost and warehouse space analysis.
	    - Listing Quality Metrics: Retained 'name_length', 'description_length', 
	      and 'photos_qty' to analyze the impact of listing quality on conversion rates.
	
	4. Data Standardization:
	    - Removed Portuguese category names to eliminate redundancy and maintain 
	      a clean, English-only analytical layer.
*/

-- STEP 2: ETL / Pipeline – run this query each time (UPSERT)
INSERT INTO silver.products (
    product_id, 
	category_name_en, 
    name_length, 
	description_length, 
	photos_qty,
    weight_g, 
	length_cm, 
	height_cm, 
	width_cm, 
    volume_cm3
)
WITH category_stats AS (
    -- Level 1: Calculate averages per category using only valid values (> 0)
    SELECT 
        product_category_name,
		AVG(CASE WHEN product_weight_g > 0 THEN product_weight_g ELSE NULL END) AS avg_w, -- if product weight is bigger then 0 take it else make it null and continue.
        AVG(CASE WHEN product_length_cm > 0 THEN product_length_cm ELSE NULL END) AS avg_l,
		AVG(CASE WHEN product_height_cm > 0 THEN product_height_cm ELSE NULL END) AS avg_h,
		AVG(CASE WHEN product_width_cm > 0 THEN product_width_cm ELSE NULL END) AS avg_wi
    FROM bronze.olist_products
    GROUP BY product_category_name 
),
global_stats AS (
 -- Level 2: General average of whole table (If there is no any data in categories)
 SELECT
		AVG(CASE WHEN product_weight_g > 0 THEN product_weight_g ELSE NULL END) AS g_avg_w,
        AVG(CASE WHEN product_length_cm > 0 THEN product_length_cm ELSE NULL END) AS g_avg_l,
		AVG(CASE WHEN product_height_cm > 0 THEN product_height_cm ELSE NULL END) AS g_avg_h,
		AVG(CASE WHEN product_width_cm > 0 THEN product_width_cm ELSE NULL END) AS g_avg_wi
		FROM bronze.olist_products
)
SELECT 
    p.product_id,
    -- Category Translation Logic (3-level fallback) (Translation -> Cleaned Portuguese -> Uncategorized)
    COALESCE(
       	t.category_name_en, 
        INITCAP(REPLACE(p.product_category_name, '_', ' ')), 
        'Uncategorized'
    ) AS category_name_en,
    
    -- Listing Quality Metrics
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,

    -- Physical Attributes (if 0 → fill with category average) (Original -> Categorical Avg -> General Avg -> 0)
    COALESCE(NULLIF(p.product_weight_g, 0), cs.avg_w, gs.g_avg_w, 0) AS weight_g,
    COALESCE(NULLIF(p.product_length_cm, 0), cs.avg_l, gs.g_avg_l, 0) AS length_cm,
    COALESCE(NULLIF(p.product_height_cm, 0), cs.avg_h, gs.g_avg_h, 0) AS height_cm,
    COALESCE(NULLIF(p.product_width_cm, 0), cs.avg_wi, gs.g_avg_wi, 0) AS width_cm,

    -- Volume Calculation (based on cleaned dimensions)
    (COALESCE(NULLIF(p.product_length_cm, 0), cs.avg_l, gs.g_avg_l, 0) * 
     COALESCE(NULLIF(p.product_height_cm, 0), cs.avg_h, gs.g_avg_h, 0) * 
     COALESCE(NULLIF(p.product_width_cm, 0), cs.avg_wi, gs.g_avg_wi, 0)) AS volume_cm3

FROM bronze.olist_products p
CROSS JOIN global_stats gs -- IT connects general average to all rows
LEFT JOIN silver.category_translation t ON p.product_category_name = t.category_name_pt
LEFT JOIN category_stats cs ON p.product_category_name = cs.product_category_name

ON CONFLICT (product_id) 
DO UPDATE SET 
    category_name_en = EXCLUDED.category_name_en,
    name_length = EXCLUDED.name_length,
    description_length = EXCLUDED.description_length,
    photos_qty = EXCLUDED.photos_qty,
    weight_g = EXCLUDED.weight_g,
    length_cm = EXCLUDED.length_cm,
    height_cm = EXCLUDED.height_cm,
    width_cm = EXCLUDED.width_cm,
    volume_cm3 = EXCLUDED.volume_cm3,
    updated_at = CURRENT_TIMESTAMP;









