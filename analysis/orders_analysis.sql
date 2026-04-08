-- ====================================================================
-- Checking 'bronze.olist_orders'
-- ====================================================================
SELECT * FROM  bronze.olist_orders;

---- 1) Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
	order_id,
	COUNT(*)
FROM bronze.olist_orders
GROUP BY order_id
HAVING COUNT(*) > 1 OR order_id IS NULL;
-- no nulls or duplicates

---- 2) Check if there are nulls in other columns as well (i.e., check if there are empty rows by themselves)
SELECT 
	customer_id,
	order_status
FROM  bronze.olist_orders
WHERE 
	customer_id IS NULL 
	OR 
	order_status IS NULL;
-- No nulls in these columns

---- 3) Determine the categories of order_status
SELECT DISTINCT order_status
FROM  bronze.olist_orders;
-- Types are: "shipped", "unavailable", "invoiced", "created", "approved", "processing", "delivered", "canceled"
/*
Purchased (Satın alındı)
Approved (Onaylandı)
Shipped (Kargoya verildi)
Delivered (Teslim edildi)
Estimated Delivery (Tahmini teslimat)
*/

---- 4) Time Logic Check (Are there any dates from the future or illogical dates?)
-- Expectation: order_purchase_timestamp < order_approved_at < order_delivered_carrier_date < order_delivered_customer_date
SELECT 
	order_purchase_timestamp,
	order_approved_at,
	order_delivered_carrier_date,
	order_delivered_customer_date
FROM  bronze.olist_orders
WHERE 
	order_purchase_timestamp > order_approved_at
	OR
	order_approved_at > order_delivered_carrier_date
	OR 
	order_delivered_carrier_date > order_delivered_customer_date;
-- 1382 rows

---- 5) Analyze the difference between expected date and delivered customer date.
WITH dates AS (
	SELECT 
		order_delivered_customer_date,
		order_estimated_delivery_date,
		order_estimated_delivery_date::timestamp - order_delivered_customer_date::timestamp AS diff_of_days,
		CASE 
		WHEN order_estimated_delivery_date::timestamp > order_delivered_customer_date::timestamp  THEN 'earlier' 
		ELSE 'later'
		END AS status
	FROM  bronze.olist_orders)
SELECT 
	status,
	COUNT(*)
FROM dates
GROUP BY status;
-- OUTPUT: 	"later"	10792
		--  "earlier"	88649
	
---- 6) Check if customer_id is matches with customer_id's in table silver.customers
SELECT 
	o.customer_id AS orders_customers,
	c.customer_id AS customers_cust,
	c.customer_pk
FROM  bronze.olist_orders o
LEFT JOIN silver.customers c
	ON o.customer_id = c.customer_id
WHERE c.customer_pk IS NULL;
-- 0 rows. So, every customer is inside silver.customers table

---- 7) IS there any nulls in date columns? If yes,is it means orders canceled?
SELECT 
	order_purchase_timestamp,
	order_approved_at,
	order_delivered_carrier_date,
	order_delivered_customer_date,
	order_estimated_delivery_date
FROM  bronze.olist_orders
WHERE 
	order_purchase_timestamp IS NULL
	OR 
	order_approved_at IS NULL
	OR
	order_delivered_carrier_date IS NULL
	OR 
	order_delivered_customer_date IS NULL
	OR 
	order_estimated_delivery_date IS NULL;
-- 2980 rows

---- 8) Count nulls in all date columns seperately to see which ones usually have the most nulls?
WITH dates AS(
	SELECT 
		order_purchase_timestamp,
		order_approved_at,
		order_delivered_carrier_date,
		order_delivered_customer_date,
		order_estimated_delivery_date
	FROM  bronze.olist_orders
	WHERE 
		order_purchase_timestamp IS NULL
		OR 
		order_approved_at IS NULL
		OR
		order_delivered_carrier_date IS NULL
		OR 
		order_delivered_customer_date IS NULL
		OR 
		order_estimated_delivery_date IS NULL)
SELECT 
	COUNT(order_purchase_timestamp) AS order_purchase_timestamp,
	COUNT(order_approved_at) AS order_approved_at,
	COUNT(order_delivered_carrier_date) AS order_delivered_carrier_date,
	COUNT(order_delivered_customer_date) AS order_delivered_customer_date,
	COUNT(order_estimated_delivery_date) AS order_estimated_delivery_date
FROM dates
-- OUTPUT: 2980	2820	1197	15	2980
-- I can say order_purchase_timestamp and order_estimated_delivery_date not null as I expected but 
-- order_delivered_customer_date has 2965 nulls.

---- 9) Check if it is timezone mistake or data enterance mistake:
-- If it's a timezone error: The difference is usually an integer (e.g., exactly 1 hour, exactly 3 hours).
-- If it's a data entry error: The difference is random (e.g., 14 minutes, 2 days, 5 hours 12 minutes).
SELECT 
    order_id,
    order_purchase_timestamp,
    order_approved_at,
    -- Let's see the difference in hours.
    EXTRACT(EPOCH FROM (order_approved_at::timestamp - order_purchase_timestamp::timestamp)) / 3600 as hour_diff -- EPOCH = zamanın saniye cinsinden değeri
FROM bronze.olist_orders
WHERE order_purchase_timestamp > order_approved_at
LIMIT 20;
-- 0 rows. So it means it is not a timezone mistake. But I get 1382 illogical date value query before it. So,
-- I see this error is not between approval and purchase. This error is between approved vs carrier or carrier vs customer.

----- 10) Analyze inconsistencies in the chronological order of order timestamps
SELECT 
    COUNT(*) FILTER (WHERE order_purchase_timestamp > order_approved_at) as purchase_vs_approved,
    COUNT(*) FILTER (WHERE order_approved_at > order_delivered_carrier_date) as approved_vs_carrier,
    COUNT(*) FILTER (WHERE order_delivered_carrier_date > order_delivered_customer_date) as carrier_vs_customer
FROM bronze.olist_orders;