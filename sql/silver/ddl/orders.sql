-- ====================================================================
-- Create 'silver.orders' Table
-- Purpose: Fact table containing order life cycle, delivery performance, 
--          and data quality flags.
-- ====================================================================
-- DROP TABLE IF EXISTS silver.orders;

-- STEP 1: Create table (persistent)
CREATE TABLE IF NOT EXISTS silver.orders(
	-- Primary Keys
	order_pk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	order_id TEXT UNIQUE NOT NULL, -- Natural Key	  
    -- Foreign Keys & References
    customer_id TEXT,             -- Raw ID for traceability
    customer_pk INT,              -- Reference to silver.customers
	-- Order Details
    order_status TEXT,
 	-- Timestamps
    purchase_timestamp TIMESTAMP,
    approved_at TIMESTAMP,
    delivered_carrier_date TIMESTAMP,
    delivered_customer_date TIMESTAMP,
    estimated_delivery_date TIMESTAMP,	    
    -- Calculated Metrics (SLA & Performance)
    delivery_time_days INT,
    estimated_time_days INT,
    delivery_performance TEXT,    -- 'Late', 'On Time', 'Pending/Cancelled'	    
    -- Data Quality Flags
    is_valid_chronology BOOLEAN,  -- TRUE if dates follow logical order
	-- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Optimization: Fast lookup for order status and performance
CREATE INDEX IF NOT EXISTS idx_silver_orders_status ON silver.orders(order_status) WHERE order_status != 'Delivered';
-- delviered haricindekiler için idnex oluştursun çünkü şu anda var olan tabloda zaten 99k veri içinde 96k delivered
-- bunları indexlemeye gerek yok çünkü tabloyu komple dönmesi daha hızlı olur ekstra index tablosunu da dönmesindense.
-- Ama geri kalanı indexlesin çünkü sayıları az.
CREATE INDEX IF NOT EXISTS idx_silver_orders_performance ON silver.orders(delivery_performance) 
WHERE delivery_performance = 'Later';
-- Çünkü bronze.olist_orders tablosunda 1/10 later geri kalanı 'Earlier'


-- Determining index filter for order_status
/*
SELECT
	COUNT(*),
	order_status
FROM  bronze.olist_orders
GROUP BY order_status;
*/


