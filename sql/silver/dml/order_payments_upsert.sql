-- ====================================================================
-- Transform 'silver.order_payments'
-- ====================================================================
-- Analysis Results:
	-- There are no any important errors.
	-- I will just change payment type "not_defined" to "Unknown"
	-- And I will make names seem better (Like deleting "_" and INITCAP)
	-- I will drop 3 rows which type is not_defined and payment_value is 0.

SELECT * FROM bronze.olist_order_payments;

INSERT INTO silver.order_payments (
	order_id,
	payment_sequential,
	payment_type,
	payment_installments,
	payment_value
)
SELECT
	order_id,
	payment_sequential,
	INITCAP(TRIM(REPLACE(payment_type, '_', ' '))) AS payment_type,
	payment_installments,
	payment_value
FROM bronze.olist_order_payments
WHERE payment_type != 'not_defined' -- I just removed those 3 unnecessary lines.
 
ON CONFLICT(order_id, payment_sequential)
DO UPDATE SET
	payment_type = EXCLUDED.payment_type,
	payment_installments = EXCLUDED.payment_installments,
	payment_value = EXCLUDED.payment_value,
	updated_at = CURRENT_TIMESTAMP;
	