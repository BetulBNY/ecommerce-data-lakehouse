-- ====================================================================
-- Transform 'silver.order_reviews'
-- ====================================================================
-- Analysis Results:
/*
Entity Granularity Analysis:
    In the Olist ecosystem, a "Review" is linked to the entire "Order" (Basket), 
    not to individual products within that order. Even if a customer purchases 
    multiple different items (Product A, Product B, etc.) in a single transaction, 
    they submit one overall satisfaction score for the entire shopping experience.

Deduplication Logic:
    - Business Rule: Each Order (order_id) must have only one final satisfaction score.
    - Data Issue: Some orders have multiple review entries (e.g., a customer 
      might update their initial 1-star rating to 5 stars after delivery).
    - Solution: I use ROW_NUMBER() partitioned by 'order_id' and ordered by 
      'review_answer_timestamp DESC' to keep only the most recent (final) review.

Why order_id is UNIQUE in Silver:
    Maintaining a 1:1 relationship between an Order and its Review prevents 
    "Cart Inflation" in downstream analytics. If we kept multiple reviews per 
    order, joining this table with 'silver.orders' would duplicate revenue 
    and order counts in financial reports.

Hypothetical Product-Level Reviews:
    If the system were designed for product-specific feedback, the schema 
    would require a 'product_id' column, and the unique constraint would 
    be a composite key: UNIQUE(order_id, product_id). Since 'product_id' is 
    absent here, the grain is strictly at the Order level.
*/
INSERT INTO silver.order_reviews (
    review_id, 
    order_id, 
    review_score, 
    review_comment_title, 
    review_comment_message, 
    review_creation_date, 
    review_answer_timestamp
)
-- SCD 1
WITH ranked_reviews AS (  
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY review_answer_timestamp DESC,
		review_creation_date DESC -- Sort and number the reviews for each order_id. But its main purpose is select the most recent record (deduplication)
        ) AS rn
    FROM bronze.olist_order_reviews
)
-- SELECT* FROM ranked_reviews WHERE rn>1
SELECT 
    review_id,
    order_id,
    review_score,
    INITCAP(TRIM(review_comment_title)) AS review_comment_title,
    TRIM(review_comment_message) AS review_comment_message, 
    review_creation_date::timestamp AS review_creation_date::timestamp ,
    review_answer_timestamp::timestamp AS review_answer_timestamp 
FROM ranked_reviews
WHERE rn = 1 -- Just get latest one
ON CONFLICT (order_id) 
DO UPDATE SET 
    review_score = EXCLUDED.review_score,
    review_comment_title = EXCLUDED.review_comment_title,
    review_comment_message = EXCLUDED.review_comment_message,
    review_answer_timestamp = EXCLUDED.review_answer_timestamp,
    updated_at = CURRENT_TIMESTAMP;