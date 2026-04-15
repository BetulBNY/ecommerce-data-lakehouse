/*
===============================================================================
PROFESSIONAL DATA QUALITY CHECK: Gold Layer Integrity
===============================================================================
Logic:
    Verifies that the transformation from Silver to Gold didn't lose any data.
    Ensures primary keys are unique and revenue is balanced across layers.
===============================================================================
*/

WITH gold_test_summary AS (
    SELECT
        -- TEST 1: Uniqueness Check (PK check)
        -- Sums the count of rows that appear more than once. Expectation: 0.
        (SELECT COUNT(*) FROM (
            SELECT order_pk 
			FROM gold.fact_orders 
			GROUP BY 1 HAVING COUNT(*) > 1
            UNION ALL
            SELECT sales_item_pk 
			FROM gold.fact_sales_items 
			GROUP BY 1 HAVING COUNT(*) > 1
        ) t) AS count_pk_errors,

        -- TEST 2: Revenue Integrity (Silver vs Gold Reconciliation)
        -- Checks if the total revenue in Silver matches Gold within a small tolerance.
        (SELECT COUNT(*) FROM (
            SELECT 
                ABS((SELECT SUM(payment_value) FROM silver.order_payments) - 
                    (SELECT SUM(total_order_value) FROM gold.fact_orders)) as revenue_diff
        ) r WHERE revenue_diff > 1.0) AS count_revenue_mismatch,

        -- TEST 3: Referential Integrity (Symmetry check)
        -- Fact items should not have missing dimension keys.
        (SELECT COUNT(*) 
		 FROM gold.fact_sales_items 
         WHERE customer_pk IS NULL OR product_pk IS NULL OR order_date_key IS NULL
        ) AS count_orphaned_rows
)
SELECT 
    CASE 
        WHEN (count_pk_errors + count_revenue_mismatch + count_orphaned_rows) = 0 THEN 1
        ELSE 0 
    END AS validation_status
FROM gold_test_summary;