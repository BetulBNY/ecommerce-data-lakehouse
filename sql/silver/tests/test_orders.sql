-- ===============================================================================
-- Quality Checks for silver.orders
-- ===============================================================================
SELECT * FROM silver.orders;

-- 1. TEST: Duplicate or Null Order IDs
-- EXPECTATION: 0 rows. order_id must be the unique natural key.
SELECT 
    order_id, 
    COUNT(*) 
FROM silver.orders 
GROUP BY order_id 
HAVING COUNT(*) > 1 OR order_id IS NULL;

-- 2. TEST: Referential Integrity (Customer Mapping)
-- EXPECTATION: 0 rows. Every order must be mapped to a valid customer_pk.
SELECT COUNT(*) 
FROM silver.orders 
WHERE customer_pk IS NULL;

-- 3. TEST: Status vs. Date Consistency (The "Delivered but No Date" Check)
-- EXPECTATION: 0 rows. If status is 'Delivered', the delivery date must NOT be NULL.
SELECT * 
FROM silver.orders 
WHERE order_status = 'Delivered' 
  AND delivered_customer_date IS NULL
  AND is_valid_chronology = TRUE;
-- 8 rows seem "Delivered" but their delivered_customer_date is Null. So I updated dml data quality part.

-- 4. TEST: Canceled but Delivered Check
-- EXPECTATION: 0 rows. A canceled order should NOT have a delivery date.
SELECT * 
FROM silver.orders 
WHERE order_status = 'Canceled' 
  AND delivered_customer_date IS NOT NULL
  AND is_valid_chronology = TRUE;

-- 5. TEST: Performance Categorization Logic
-- EXPECTATION: 0 rows. 'Later' orders must have a delivery date > estimated date.
-- This checks if our CASE WHEN logic in DML was correct.
SELECT * 
FROM silver.orders 
WHERE delivery_performance = 'Later' 
  AND delivered_customer_date <= estimated_delivery_date;

-- 6. TEST: Logical Time-Gap Check
-- EXPECTATION: 0 rows. delivery_time_days should not be a negative number.
-- (If it's negative, it means the purchase was after delivery).
SELECT * 
FROM silver.orders 
WHERE delivery_time_days < 0;

-- 7. TEST: Chronology Flag Audit (Data Quality Reporting)
-- EXPECTATION: This is a diagnostic check. It returns rows flagged as illogical.
-- We expect ~1,382 rows based on our previous Bronze analysis.
SELECT 
    is_valid_chronology, 
    COUNT(*) 
FROM silver.orders 
GROUP BY is_valid_chronology;

-- 8. TEST: Invalid Order Status
-- EXPECTATION: 0 rows. Status should match our known categories.
SELECT DISTINCT order_status 
FROM silver.orders 
WHERE order_status NOT IN ('Delivered', 'Shipped', 'Canceled', 'Invoiced', 'Processing', 'Approved', 'Unavailable', 'Created');