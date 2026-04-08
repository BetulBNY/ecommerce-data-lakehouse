-- ====================================================================
-- Checking 'bronze.olist_order_items'
-- ====================================================================
SELECT * FROM bronze.olist_order_items;

---- 1) Check for NULLs order_id (It is not PK so I am not checking duplicates)
-- Expectation: No Results
SELECT *
FROM bronze.olist_order_items
WHERE order_id IS NULL;
-- No nulls

---- 2) Check if there are nulls in other columns as well 
SELECT 
	product_id,
	seller_id,
	order_item_id
FROM  bronze.olist_order_items
WHERE 
	product_id IS NULL 
	OR 
	order_item_id IS NULL
	OR 
	seller_id IS NULL;
-- No nulls in these columns

---- 3) Check categorise of order_item_id
SELECT 
	order_item_id,
	COUNT(*)
FROM bronze.olist_order_items
GROUP BY order_item_id
ORDER BY order_item_id DESC;
-- Max 21 min 1

---- 4) Check price outliers
SELECT
	MAX(price) AS maxPrice,
	MIN(price) AS minPrice,
	ROUND(AVG(price)::numeric,2) AS avgPrice
FROM bronze.olist_order_items;
-- 6735	 0.85	120.65

---- 5) Check freight_value outliers
SELECT
	MAX(freight_value) AS maxFreight,
	MIN(freight_value) AS minFreight,
	ROUND(AVG(freight_value)::numeric,2) AS avgFreight
FROM bronze.olist_order_items;
-- 409.68	0	19.99

---- 6) Price vs Freight Relationship
SELECT 
    CASE 
        WHEN price <= 50 THEN '0-50'
        WHEN price <= 100 THEN '50-100'
        WHEN price <= 250 THEN '100-250'
        WHEN price <= 500 THEN '250-500'
        ELSE '500+' 
    END AS price_range,
    COUNT(*) AS item_count,
    ROUND(AVG(price)::numeric, 2) AS avg_item_price,
    ROUND(AVG(freight_value)::numeric, 2) AS avg_freight_value
FROM bronze.olist_order_items
GROUP BY price_range
ORDER BY MIN(price);
/*
"price_range"	"item_count"	"avg_item_price"	"avg_freight_value"
"0-50"				39317			31.44					14.77
"50-100"			33020			75.14					17.88
"100-250"			30788			154.12					23.75
"250-500"			6309			340.33					31.23
"500+"				3216			927.31					47.54
*/

---- 7) Check if order_id is valid in silver.orders table (Referential Integrity)
SELECT 
	oi.order_id
FROM bronze.olist_order_items oi
LEFT JOIN silver.orders o
	ON o.order_id = oi.order_id
WHERE o.order_id IS NULL;
-- 0 row

---- 8) Check if seller_id is valid in silver.sellers table (Referential Integrity)
SELECT 
	oi.seller_id
	FROM bronze.olist_order_items oi
LEFT JOIN silver.sellers s
	ON s.seller_id = oi.seller_id
WHERE s.seller_id IS NULL;
-- 0 row

---- 9) Check if product_id is valid in silver.products table (Referential Integrity)
SELECT 
	COUNT(*) 
FROM bronze.olist_order_items oi
LEFT JOIN silver.products p 
	ON oi.product_id = p.product_id
WHERE p.product_pk IS NULL;
-- Output is 0

---- 10) Composite Primary Key Check
-- Are there two instances of the same product number in the same order?
SELECT order_id, order_item_id, COUNT(*)
FROM bronze.olist_order_items
GROUP BY 1, 2
HAVING COUNT(*) > 1;
-- 0 row

----- 11) Shipping Limit Date
-- The final shipping date (shipping_limit_date) cannot be before the order purchase date (purchase_timestamp).
SELECT 
	shipping_limit_date
FROM bronze.olist_order_items oi
LEFT JOIN silver.orders o
	ON o.order_id = oi.order_id
WHERE purchase_timestamp > shipping_limit_date::timestamp;
-- 0 row


