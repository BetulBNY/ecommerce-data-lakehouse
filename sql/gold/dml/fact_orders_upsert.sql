-- ===============================================================================
-- GOLD LAYER FACT DESIGN: gold.fact_orders
-- ===============================================================================
-- Notes: 
	-- Grain Level: A single line for each order (basket). (Granularity: Order Level)
	-- Purpose: To answer questions such as "How long did the delivery take? How many points did the customer give? What was the total basket value?"
    -- Logistics and Customer Satisfaction (CSAT) analysis.
    -- Data Integrity: Prevents "Metric Inflation". By keeping reviews and 
    -- total payment values at the Order level, we ensure that Average Review 
    -- Scores and Total Revenue are calculated correctly without being 
    -- multiplied by the number of items in a basket.

	-- Integration with gold.dim_date:
    -- All timestamps are converted to 'date_key' (INT) for high-speed 
    -- Year-over-Year and Month-over-Month reporting.
	
INSERT INTO gold.fact_orders (
    order_pk, 
	order_id, 
	customer_pk, 
	order_date_key, 
	delivery_date_key,
    total_order_value, 
	total_freight_value, 
	review_score, 
    delivery_time_days, 
	is_late, 
	order_status
)
WITH payment_summary AS (
    SELECT order_id, 
	SUM(payment_value) as total_pay 
	FROM silver.order_payments GROUP BY 1
),
item_summary AS (
    SELECT order_id, 
	SUM(freight_value) as total_freight 
	FROM silver.order_items GROUP BY 1
)
SELECT 
    o.order_pk,
    o.order_id,
    o.customer_pk,
    TO_CHAR(o.purchase_timestamp, 'YYYYMMDD')::INT AS order_date_key,
    TO_CHAR(o.delivered_customer_date, 'YYYYMMDD')::INT AS delivery_date_key,
    COALESCE(p.total_pay, 0) AS total_order_value,
    COALESCE(i.total_freight, 0) AS total_freight_value,
    r.review_score,
    o.delivery_time_days,
    CASE WHEN o.delivery_performance = 'Later' THEN TRUE ELSE FALSE END AS is_late,
    o.order_status
FROM silver.orders o
LEFT JOIN payment_summary p ON o.order_id = p.order_id
LEFT JOIN item_summary i ON o.order_id = i.order_id
LEFT JOIN silver.order_reviews r ON o.order_id = r.order_id

ON CONFLICT (order_pk) DO UPDATE SET
    total_order_value = EXCLUDED.total_order_value,
    total_freight_value = EXCLUDED.total_freight_value,
    review_score = EXCLUDED.review_score,
    is_late = EXCLUDED.is_late,
    order_status = EXCLUDED.order_status,
    delivery_date_key = EXCLUDED.delivery_date_key,
    updated_at = CURRENT_TIMESTAMP;