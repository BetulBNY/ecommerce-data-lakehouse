-- ====================================================================
-- Create 'silver.order_payments' Table
-- ====================================================================
-- STEP 1: Create table (persistent)

-- There is no any primary key in bronze.olist_order_payments (for UNIQUE part)
-- So, I will build a structure based on the order_id + payment_sequential pair.
CREATE TABLE IF NOT EXISTS silver.order_payments (
	payment_pk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	order_id TEXT NOT NULL,
	payment_sequential INT,
	payment_type TEXT,
	payment_installments INT,
	payment_value FLOAT,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	-- Aynı siparişin aynı sıradaki ödemesinin tekrar etmesini önlemek için:
    UNIQUE (order_id, payment_sequential) -- The same combination (order_id, payment_sequential) cannot be added again.
	-- payment_sequential = o siparişteki ödeme sırası
);
