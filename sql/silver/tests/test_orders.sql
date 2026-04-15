/*
===============================================================================
PROFESSIONAL DATA QUALITY CHECK: silver.orders
===============================================================================
*/

WITH test_results AS (
    SELECT
        -- TEST 1: Duplicate or Null IDs (Expectation: 0)
        (SELECT COUNT(*) FROM (
            SELECT order_id 
			FROM silver.orders 
			GROUP BY 1 
			HAVING COUNT(*) > 1 OR order_id IS NULL
        ) t) AS count_id_errors,

        -- TEST 2: Referential Integrity (Customer Mapping) (Expectation: 0) Every order must be mapped to a valid customer_pk.
        (SELECT COUNT(*) FROM silver.orders WHERE customer_pk IS NULL) AS count_mapping_errors,

        -- TEST 3: Flag Integrity (Is "Delivered but No Date" or "Canceled but Delivered" caught?) If status is 'Delivered', the delivery date must NOT be NULL. A canceled order should NOT have a delivery date.
        -- Expectation: 0 rows should exist where these logic errors have is_valid_chronology = TRUE.
        (SELECT COUNT(*) FROM silver.orders 
         WHERE (order_status = 'Delivered' AND delivered_customer_date IS NULL AND is_valid_chronology = TRUE)
            OR (order_status = 'Canceled' AND delivered_customer_date IS NOT NULL AND is_valid_chronology = TRUE)
        ) AS count_missed_flags,

        -- TEST 4: Performance Categorization Logic Check (Delivery Date > Estimated Date but Performance is NOT 'Later') 
		-- 'Later' orders must have a delivery date > estimated date. This checks if our CASE WHEN logic in DML was not correct.
        (SELECT COUNT(*) FROM silver.orders 
         WHERE delivery_performance = 'Later' AND delivered_customer_date <= estimated_delivery_date
        ) AS count_performance_errors,

        -- TEST 5: Negative Delivery Time (Expectation: 0)
        (SELECT COUNT(*) FROM silver.orders WHERE delivery_time_days < 0) AS count_time_errors,

        -- TEST 6: Invalid Order Status Standardization (Expectation: 0)
        (SELECT COUNT(*) FROM silver.orders 
         WHERE order_status NOT IN ('Delivered', 'Shipped', 'Canceled', 'Invoiced', 'Processing', 'Approved', 'Unavailable', 'Created')
        ) AS count_status_errors
)
SELECT 
    CASE 
        WHEN (count_id_errors + count_mapping_errors + count_missed_flags + 
              count_performance_errors + count_time_errors + count_status_errors) = 0 THEN 1
        ELSE 0 
    END AS validation_status
FROM test_results;