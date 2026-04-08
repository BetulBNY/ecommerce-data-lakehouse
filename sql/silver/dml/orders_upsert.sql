-- ====================================================================
-- Transform 'silver.orders'
-- ====================================================================
-- Analysis Results:
	-- There are not nulls or duplicates for primary key
	-- no nulls for customer_id and order status. Also, customer_id of orders table %100 matches with silver.customers table.
	-- 8 different type of order_status. I will made them INITCAP. (Standardization)
-- Data Enrichment:
    -- Joined with silver.customers to map 'customer_pk' for relational integrity.
    -- Standardized 'order_status' using INITCAP.
-- Feature Engineering:
    -- Calculated 'delivery_time_days': Actual days from purchase to delivery.
    -- Calculated 'estimated_time_days': Total days expected by the system.
    -- Categorized 'delivery_performance': Compared actual vs. estimated dates and created Flag('Late', 'On Time', 'Pending/Cancelled')
-- Data Quality Flagging (Chronology):
    -- Implemented 'is_valid_chronology' flag to detect system logging errors (e.g., carrier date before approval date).
    -- Null dates (for cancelled/pending orders) are treated as valid (TRUE) unless an existing timestamp sequence is broken.

INSERT INTO silver.orders (
    order_id, 
	customer_id, 
	customer_pk, 
	order_status,
    purchase_timestamp, 
	approved_at, 
	delivered_carrier_date, 
    delivered_customer_date, 
	estimated_delivery_date,
    delivery_time_days, 
	estimated_time_days, 
    delivery_performance, 
	is_valid_chronology
)
SELECT 
    o.order_id,
    o.customer_id,
    c.customer_pk,
    INITCAP(o.order_status) AS order_status,
    o.order_purchase_timestamp::timestamp,
    o.order_approved_at::timestamp,
    o.order_delivered_carrier_date::timestamp,
    o.order_delivered_customer_date::timestamp,
    o.order_estimated_delivery_date::timestamp,

	-- Actual delivery times (days)
	EXTRACT(DAY FROM (order_delivered_customer_date::timestamp - order_purchase_timestamp::timestamp)) AS delivery_time_days,
	
	-- Estimated delivery times (days)
	EXTRACT(DAY FROM(order_estimated_delivery_date::timestamp - order_purchase_timestamp::timestamp)) AS estimated_time_days,
	
	-- Delivery Performance Categorizagion('Earlier', 'Later', 'On Time', 'Pending/Cancelled')
	CASE
		WHEN o.order_delivered_customer_date IS NULL THEN 'Pending/Cancelled'
		WHEN order_estimated_delivery_date::timestamp > order_delivered_customer_date::timestamp THEN 'Earlier'
		WHEN order_estimated_delivery_date::timestamp < order_delivered_customer_date::timestamp THEN 'Later'
		ELSE 'On Time'
	END  AS delivery_performance,

	-- Data Quality Flag Chronology Check(For logical-illogical date orders)
	CASE
		WHEN(order_purchase_timestamp > order_approved_at) THEN FALSE
		WHEN(order_approved_at > order_delivered_carrier_date) THEN FALSE
		WHEN(order_delivered_carrier_date > order_delivered_customer_date) THEN FALSE
		ELSE TRUE
	END AS is_valid_chronology
	
	FROM bronze.olist_orders o
	LEFT JOIN silver.customers c ON o.customer_id = c.customer_id
ON CONFLICT(order_id)
DO UPDATE SET
	order_status = EXCLUDED.order_status, -- An order was marked "Shipped" yesterday. Today it is marked "Delivered".
    delivered_customer_date = EXCLUDED.delivered_customer_date, -- (It was NULL, now it's full) it changed.
	approved_at = EXCLUDED.approved_at, -- I added because sometimes bank approval information appears in the system with a one-day delay. If I don't update them, those dates may remain NULL forever.
	delivered_carrier_date = EXCLUDED.delivered_carrier_date, -- I add the delivered_carrier_date column to the DO UPDATE section.Because sometimes shipping information appears in the system with a one-day delay. If I don't update them, those dates may remain NULL forever.
    delivery_time_days = EXCLUDED.delivery_time_days,
    delivery_performance = EXCLUDED.delivery_performance,
    is_valid_chronology = EXCLUDED.is_valid_chronology,
    updated_at = CURRENT_TIMESTAMP;