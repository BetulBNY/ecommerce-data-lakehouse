-- ====================================================================
-- Checking 'bronze.olist_order_payments'
-- ====================================================================
-- Some Terms: 
	-- Payment Sequential:  A payment method that allows a single order to be completed using multiple transactions in a specific sequence, typically by splitting the total cost across different payment sources (e.g., two different credit cards).
	-- payment_installments: A payment plan that allows a customer to pay for a purchase in smaller, fixed amounts over a set period of time (monthly) instead of paying the full price upfront.
	-- Boleto : A popular cash-based payment method in Brazil where a voucher (containing a barcode and payment details) is generated for the customer to pay via bank branches, ATMs, or authorized processors.

SELECT * FROM bronze.olist_order_payments;

---- 1) Check for NULLs in order_id
-- Expectation: No Results
SELECT 
order_id
FROM  bronze.olist_order_payments
WHERE order_id IS NULL;
-- No nulls

---- 2) Check if there are nulls in other columns as well
SELECT
	payment_sequential,
	payment_type,
	payment_installments,
	payment_value
FROM  bronze.olist_order_payments
WHERE 
	payment_sequential IS NULL
	OR 
	payment_type IS NULL
	OR
	payment_installments IS NULL
	OR 
	payment_value IS NULL;
-- No nulls	

---- 3) Check number of payment types
SELECT DISTINCT payment_type
FROM  bronze.olist_order_payments;
-- 5 type of payment, these are: "not_defined", "boleto", "debit_card", "voucher", "credit_card"

---- 4) Check number of payment installments
SELECT DISTINCT payment_installments
FROM  bronze.olist_order_payments
ORDER BY payment_installments DESC;
-- 24 different payment installments max is 24 min 0

---- 5) Check max, min, avg payment value
SELECT 
	ROUND(MAX(payment_value)::numeric,2),
	MIN(payment_value),
	ROUND(AVG(payment_value)::numeric,2)
FROM bronze.olist_order_payments;
-- 13664.08	 0	154.10  There is 0 in values!!!

---- 6) Get number of 0 payment values
SELECT
*
FROM bronze.olist_order_payments
WHERE payment_value = 0;
-- There are 9 rows nad when I analyzed it payment types is mostly voucher and just 3 of them are not_defined.
-- Which makes it logical.

---- 7) Finf the distinct number of order_id
SELECT 
COUNT(DISTINCT order_id)
FROM bronze.olist_order_payments;
-- 99440 different order_id
---
SELECT 
DISTINCT order_id,
COUNT(*)
FROM bronze.olist_order_payments
GROUP BY order_id
ORDER BY COUNT(*) DESC;

---- 8) What is the payment values of "not_defined" payment type? 
-- If both type is not_defined and value is 0, this row could be trash. But if type is not_defined and value is very high it means there is data loss.
SELECT 
	payment_type,
	payment_value
FROM bronze.olist_order_payments
WHERE payment_type = 'not_defined';
-- Their values are 0. So this makes it "Noise"
-- So there are 2 options for these 3 rows.
	-- 1) Write them in silver table as "unknonw"
	-- 2) Drop them for better analysis. Because there is no payment type, payment value.





