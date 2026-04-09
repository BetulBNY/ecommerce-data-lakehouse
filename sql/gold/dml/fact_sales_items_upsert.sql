-- ===============================================================================
-- GOLD LAYER FACT DESIGN: gold.fact_sales_items
-- ===============================================================================
-- Notes: 
	-- gold.fact_sales_items (Granularity: Order-Item Level)
    -- Purpose: Financial and inventory analysis at the most granular level.
    -- Usage: Measuring product-category revenue and seller performance.

INSERT INTO gold.fact_sales_items (
    order_pk, 
	order_id, 
	product_pk, 
	seller_pk, 
	customer_pk, 
    order_date_key, 
	price, 
	freight_value,
	total_item_value
)
SELECT 
    o.order_pk,
    oi.order_id,
    oi.product_pk,
    oi.seller_pk,
    o.customer_pk,
    TO_CHAR(o.purchase_timestamp, 'YYYYMMDD')::INT AS order_date_key,
    oi.price,
    oi.freight_value,
    oi.total_value AS total_item_value
FROM silver.order_items oi
JOIN silver.orders o ON oi.order_id = o.order_id;