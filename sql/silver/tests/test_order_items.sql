-- ===============================================================================
-- Quality Checks for silver.order_items
-- ===============================================================================
SELECT * FROM silver.order_items;

-- 1. TEST: Duplicate Line Items (Composite Key Check)
-- EXPECTATION: 0 rows. The combination of order_id and order_item_id must be unique.
SELECT 
    order_id, 
    order_item_id, 
    COUNT(*) 
FROM silver.order_items 
GROUP BY 1, 2 
HAVING COUNT(*) > 1;

-- 2. TEST: Referential Integrity (Product & Seller Mapping)
-- EXPECTATION: 0 rows. Every item must be successfully mapped to our internal PKs.
-- If this fails, it means there are products or sellers in order_items that don't exist in our Dimension tables.
SELECT *
FROM silver.order_items 
WHERE product_pk IS NULL 
   OR seller_pk IS NULL;

-- 3. TEST: Calculation Integrity (Total Value)
-- EXPECTATION: 0 rows. total_value must be exactly price + freight_value.
SELECT 
    order_id, 
    price, 
    freight_value, 
    total_value 
FROM silver.order_items 
WHERE ABS(total_value - (price + freight_value)) > 0.01;

-- 4. TEST: Negative Value Check
-- EXPECTATION: 0 rows. Price and Freight should not be negative.
SELECT * 
FROM silver.order_items 
WHERE price < 0 
   OR freight_value < 0;

-- 5. TEST: Orphaned Items Check
-- EXPECTATION: 0 rows. Every item must belong to an existing order in silver.orders.
-- This ensures we don't have "ghost" items without a parent order.
SELECT oi.order_id
FROM silver.order_items oi
LEFT JOIN silver.orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- 6. TEST: Chronology vs Order Purchase
-- EXPECTATION: 0 rows. Shipping limit cannot be before the actual order date.
SELECT 
	oi.order_id, 
	oi.shipping_limit_date, 
	o.purchase_timestamp
FROM silver.order_items oi
JOIN silver.orders o 
	ON oi.order_id = o.order_id
WHERE oi.shipping_limit_date < o.purchase_timestamp;