-- ===============================================================================
-- GOLD LAYER DIMENSION DESIGN: gold.dim_products
-- ===============================================================================
-- Notes:
	-- Length, height and width don't added because:
	-- Ease of Analysis: Analysts will no longer have to multiply product dimensions 
	-- (length, height, width) manually; they will use the ready-made volume_cm3.

	-- Freshness: Thanks to `updated_at` in table, I know when the data was last updated.
INSERT INTO gold.dim_products (
    product_pk,
    product_id,
    category_id,
    category_name_en,
    name_length,
    description_length,
    photos_qty,
    weight_g,
    volume_cm3
)
SELECT 
    product_pk,
    product_id,
    category_id,
    category_name_en,
    name_length,
    description_length,
    photos_qty,
    weight_g,
    volume_cm3
FROM silver.products

ON CONFLICT (product_pk) 
DO UPDATE SET 
    category_id = EXCLUDED.category_id,
    category_name_en = EXCLUDED.category_name_en,
    name_length = EXCLUDED.name_length,
    description_length = EXCLUDED.description_length,
    photos_qty = EXCLUDED.photos_qty,
    weight_g = EXCLUDED.weight_g,
    volume_cm3 = EXCLUDED.volume_cm3,
    updated_at = CURRENT_TIMESTAMP;