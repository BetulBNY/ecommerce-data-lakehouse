/*
===============================================================================
PROFESSIONAL DATA QUALITY CHECK: silver.order_items
===============================================================================
Logic:
    Returns '1' if all checks pass, '0' if any validation fails.
===============================================================================
*/

WITH test_results AS (
    SELECT
        -- TEST 1: Duplicate Line Items (Composite Key Check)(Expectation: 0) The combination of order_id and order_item_id must be unique.
        (SELECT COUNT(*) FROM (
            SELECT order_id, order_item_id 
			FROM silver.order_items 
			GROUP BY 1, 2 
			HAVING COUNT(*) > 1
        ) t) AS count_duplicate_errors,

        -- TEST 2: Referential Integrity (Product & Seller Mapping) (Expectation: 0) Every item must be successfully mapped to our internal PKs.
		-- If this fails, it means there are products or sellers in order_items that don't exist in our Dimension tables.
        (SELECT COUNT(*) FROM silver.order_items 
         WHERE product_pk IS NULL OR seller_pk IS NULL
        ) AS count_fk_errors,

        -- TEST 3: Calculation Integrity (Total Value) (Expectation: 0) total_value must be exactly price + freight_value.
        (SELECT COUNT(*) FROM silver.order_items 
         WHERE ABS(total_value - (price + freight_value)) > 0.01
        ) AS count_calculation_errors,

        -- TEST 4: Negative Value Check (Expectation: 0) Price and Freight should not be negative.
        (SELECT COUNT(*) FROM silver.order_items 
         WHERE price < 0 OR freight_value < 0
        ) AS count_negative_errors,

        -- TEST 5: Orphaned Items Check (Expectation: 0) Every item must belong to an existing order in silver.orders.
		-- This ensures we don't have "ghost" items without a parent order.
        (SELECT COUNT(*) 
         FROM silver.order_items oi
         LEFT JOIN silver.orders o 
		 	ON oi.order_id = o.order_id
         WHERE o.order_id IS NULL
        ) AS count_orphan_errors,

        -- TEST 6: Chronology vs Order Purchase (Expectation: 0) Shipping limit cannot be before the actual order date.
        (SELECT COUNT(*) 
         FROM silver.order_items oi
         JOIN silver.orders o 
		 	ON oi.order_id = o.order_id
         WHERE oi.shipping_limit_date < o.purchase_timestamp
        ) AS count_chronology_errors
)
SELECT 
    CASE 
        WHEN (count_duplicate_errors + count_fk_errors + count_calculation_errors + 
              count_negative_errors + count_orphan_errors + count_chronology_errors) = 0 THEN 1
        ELSE 0 
    END AS validation_status
FROM test_results;