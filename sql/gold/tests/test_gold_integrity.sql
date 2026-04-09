-- ====================================================================
-- Checking 'gold.test_gold_integrity'
-- ====================================================================
-- 1. TEST: Uniqueness Check
-- EXPECTATION: 0 row. PKs must be unique.
SELECT 
	'fact_orders' as table_name, 
	order_pk, 
	COUNT(*) 
FROM gold.fact_orders 
GROUP BY 1, 2 
HAVING COUNT(*) > 1
	UNION ALL
SELECT 
	'fact_sales_items' as table_name, 
	sales_item_pk, 
	COUNT(*) 
FROM gold.fact_sales_items 
GROUP BY 1, 2 
HAVING COUNT(*) > 1;

-- 2. TEST: Revenue Integrity (Most impoprtant)
-- EXPECTATION: Difference must be smaller than 0.01.
WITH silver_total AS (
    SELECT 
		SUM(payment_value) as total 
	FROM silver.order_payments
),
gold_total AS (
    SELECT 
		SUM(total_order_value) as total 
	FROM gold.fact_orders
)
SELECT 
    s.total as silver_revenue, 
    g.total as gold_revenue,
    ABS(s.total - g.total) as diff
FROM silver_total s, gold_total g
WHERE ABS(s.total - g.total) > 1.0; -- An error will occur if the difference is more than 1 unit.

-- 3. TEST: Referential Integrity (Dimension Connections)
-- EXPECTATION: 0 row. Every sale should have a customer and a product..
SELECT 
	COUNT(*) 
FROM gold.fact_sales_items 
WHERE 
	customer_pk IS NULL 
	OR 
	product_pk IS NULL 
	OR 
	order_date_key IS NULL;