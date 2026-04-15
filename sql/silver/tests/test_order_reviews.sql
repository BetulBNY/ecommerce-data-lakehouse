/*
===============================================================================
PROFESSIONAL DATA QUALITY CHECK: silver.order_reviews
===============================================================================
*/

WITH test_results AS (
    SELECT
        -- TEST 1: Uniqueness per Order (Expectation: 0)
        (SELECT COUNT(*) FROM (
            SELECT order_id 
			FROM silver.order_reviews 
			GROUP BY 1 
			HAVING COUNT(*) > 1
        ) t) AS count_duplicate_errors,

        -- TEST 2: Essential Null Check (Expectation: 0) Primary Key & Foreign Key Nullability
        -- Essential identifiers and the score must not be NULL
	   (SELECT COUNT(*) FROM silver.order_reviews 
         WHERE review_id IS NULL OR order_id IS NULL OR review_score IS NULL
        ) AS count_null_errors,

        -- TEST 3: Score Range [1-5] (Expectation: 0) Satisfaction scores must be between 1 and 5.
        (SELECT COUNT(*) FROM silver.order_reviews 
         WHERE review_score < 1 OR review_score > 5
        ) AS count_range_errors,

        -- TEST 4: Chronology (Answer cannot be before creation) (Expectation: 0)The customer cannot answer a review before it is created.
		-- review_answer_timestamp must be >= review_creation_date.
        (SELECT COUNT(*) FROM silver.order_reviews 
         WHERE review_answer_timestamp < review_creation_date
        ) AS count_chronology_errors,

        -- TEST 5: Standardization (Trimming Check) (Expectation: 0)
        (SELECT COUNT(*) FROM silver.order_reviews 
         WHERE review_comment_title != TRIM(review_comment_title)
            OR review_comment_message != TRIM(review_comment_message)
        ) AS count_trim_errors
)
SELECT 
    CASE 
        WHEN (count_duplicate_errors + count_null_errors + count_range_errors + 
              count_chronology_errors + count_trim_errors) = 0 THEN 1
        ELSE 0 
    END AS validation_status
FROM test_results;