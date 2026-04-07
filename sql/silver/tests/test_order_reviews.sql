-- ===============================================================================
-- Quality Checks for silver.order_reviews
-- ===============================================================================

SELECT * FROM silver.order_reviews;
-- 1. TEST: Uniqueness per Order (Granularity Check)
-- EXPECTATION: Each order_id must have exactly ONE final review record.
-- If this fails, the deduplication logic (ROW_NUMBER) in DML is broken.
SELECT 
    order_id, 
    COUNT(*) as occurrences
FROM silver.order_reviews
GROUP BY order_id
HAVING COUNT(*) > 1;

-- 2. TEST: Primary Key & Foreign Key Nullability
-- EXPECTATION: Essential identifiers and the score must not be NULL.
SELECT *
FROM silver.order_reviews
WHERE review_id IS NULL 
   OR order_id IS NULL 
   OR review_score IS NULL;

-- 3. TEST: Value Range Validation
-- EXPECTATION: Satisfaction scores must be between 1 and 5.
SELECT *
FROM silver.order_reviews
WHERE review_score < 1 
   OR review_score > 5;

-- 4. TEST: Chronological Logic Check (Timestamp Integrity)
-- EXPECTATION: The customer cannot answer a review before it is created.
-- review_answer_timestamp must be >= review_creation_date.
SELECT *
FROM silver.order_reviews
WHERE review_answer_timestamp < review_creation_date;

-- 5. TEST: Data Standardization (Trailing Spaces)
-- EXPECTATION: Titles and messages should be trimmed.
SELECT *
FROM silver.order_reviews
WHERE review_comment_title != TRIM(review_comment_title)
   OR review_comment_message != TRIM(review_comment_message);