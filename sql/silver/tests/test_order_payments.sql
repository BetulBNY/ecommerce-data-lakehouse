/*
===============================================================================
PROFESSIONAL DATA QUALITY CHECK: silver.order_payments
===============================================================================
Logic:
    Returns '1' if all checks pass, '0' if any validation fails.
===============================================================================
*/

WITH test_results AS (
    SELECT
        -- TEST 1: Duplicate Payment Records (Expectation: 0. Each order should have the same payment sequence only once)
        (SELECT COUNT(*) FROM (
            SELECT order_id, payment_sequential 
			FROM silver.order_payments 
			GROUP BY 1, 2 
			HAVING COUNT(*) > 1
        ) t) AS count_duplicate_errors,

        -- TEST 2: Invalid Payment Values (Expectation: 0) (Note: We accepted 0 values due to voucher/cancellation, but negative is impossible)
        (SELECT COUNT(*) FROM silver.order_payments 
         WHERE payment_value < 0
        ) AS count_negative_value_errors,

        -- TEST 3: Invalid Installments (Expectation: 0) Number of installments cannot be negative.
        (SELECT COUNT(*) FROM silver.order_payments 
         WHERE payment_installments < 0
        ) AS count_negative_installment_errors,

        -- TEST 4: Payment Type Standardization (Expectation: 0) 'not_defined' should not remain and names should be in INITCAP (Credit Card etc.)
        (SELECT COUNT(*) FROM silver.order_payments 
         WHERE payment_type = 'Not_defined' 
            OR payment_type != INITCAP(payment_type)
        ) AS count_standardization_errors
)
SELECT 
    CASE 
        WHEN (count_duplicate_errors + count_negative_value_errors  
              + count_negative_installment_errors + count_standardization_errors) = 0 THEN 1
        ELSE 0 
    END AS validation_status
FROM test_results;