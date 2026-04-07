-- ====================================================================
-- Quality Checks for silver.order_payments
-- ====================================================================

-- 1. TEST: Duplicate Payment Records
-- EXPECTATION: Each order should have the same payment sequence only once.
SELECT 
    order_id, 
    payment_sequential, 
    COUNT(*) 
FROM silver.order_payments 
GROUP BY order_id, payment_sequential 
HAVING COUNT(*) > 1;

-- 2. TEST: Invalid Payment Values
-- EXPECTATION: Payment amount should not be negative.
-- (Note: We accepted 0 values due to voucher/cancellation, but negative is impossible).
SELECT * 
FROM silver.order_payments 
WHERE payment_value < 0;

-- 3. TEST: Invalid Installments
-- EXPECTATION: Number of installments cannot be negative.
SELECT * 
FROM silver.order_payments 
WHERE payment_installments < 0;

-- 4. TEST: Payment Type Standardization
-- EXPECTATION: 'not_defined' should not remain and names should be in INITCAP (Credit Card etc.).
SELECT DISTINCT payment_type 
FROM silver.order_payments 
WHERE payment_type = 'not_defined' 
   OR payment_type != INITCAP(payment_type);