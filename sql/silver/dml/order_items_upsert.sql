-- ====================================================================
-- Transform 'silver.order_items'
-- ====================================================================
-- Analysis Results:
	-- Id's in olist_order_items matches with all other tables(seller, products,orders)
	-- shipping_limit_date is logical but I will just transform it text to timestamp
	-- I will sum price and freight value to see total item value to see 

INSERT INTO silver.order_items (
    order_id, 
	order_item_id, 
	product_id, 
	product_pk, 
    seller_id, 
	seller_pk, 
	shipping_limit_date, 
    price, 
	freight_value, 
	total_value
)	
SELECT 
	oi.order_id,
    oi.order_item_id,
    oi.product_id,
    p.product_pk,
    oi.seller_id,
    s.seller_pk,
    oi.shipping_limit_date::timestamp,
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) AS total_value
FROM bronze.olist_order_items oi
LEFT JOIN silver.products p 
	ON oi.product_id = p.product_id
LEFT JOIN silver.sellers s 
	ON oi.seller_id = s.seller_id

ON CONFLICT (order_id, order_item_id) 
DO UPDATE SET 
    shipping_limit_date = EXCLUDED.shipping_limit_date,
    price = EXCLUDED.price,
    freight_value = EXCLUDED.freight_value,
    total_value = EXCLUDED.total_value,
    updated_at = CURRENT_TIMESTAMP;