-- ====================================================================
-- Checking 'bronze.olist_order_reviews'
-- ====================================================================
SELECT * FROM bronze.olist_order_reviews;

---- 1) Check for NULLs in primary key
-- Expectation: No Results
SELECT 
	review_id,
	COUNT(*)
FROM  bronze.olist_order_reviews
GROUP BY review_id
HAVING review_id IS NULL OR COUNT(*) > 1;
-- No nulls but there are 789 duplicates

---- 2) Check for NULLs in order_id
-- Expectation: No Results
SELECT 
	order_id,
	COUNT(*)
FROM  bronze.olist_order_reviews
GROUP BY order_id
HAVING order_id IS NULL OR COUNT(*) > 1;
-- No nulls but there are 547 duplicates

---- 3) Check  and analyse duplicated values to see whats happening
SELECT * 
FROM
bronze.olist_order_reviews
WHERE review_id IN 
	(SELECT 
		review_id
	FROM bronze.olist_order_reviews
	GROUP BY review_id
	HAVING COUNT(*) > 1
	)
ORDER BY review_id;
-- I realized their repeated they seem same review_id, review_score, comment etc but just order_id is different.
-- This indicates that the customer purchased multiple items in a single cart and wrote a single review for all of them.

---- 4) Check review score categories and get the total count for each other.
SELECT
	review_score,
	COUNT(*)
FROM bronze.olist_order_reviews
GROUP BY review_score
ORDER BY review_score DESC;
-- max 5 min 1

---- 5) Score Distribution and Null Value Check
SELECT 
    review_score, 
    COUNT(*) as total_reviews,
    COUNT(review_comment_message) as reviews_with_comments
FROM bronze.olist_order_reviews
GROUP BY 1 ORDER BY 1;

---- 6) How many reviews have been received for the same order?
SELECT order_id, COUNT(*)
FROM bronze.olist_order_reviews
GROUP BY 1 HAVING COUNT(*) > 1
ORDER BY 2 DESC;

---- 7) Time Logic Check (Are there any dates from the future or illogical dates?)
-- Expectation: The Answer date must be after the Creation date.
SELECT
	review_creation_date,
	review_answer_timestamp
FROM bronze.olist_order_reviews
WHERE 
	DATE(review_creation_date) > '2020-01-01' OR DATE(review_creation_date) < '2015-01-01'
	OR 
	DATE(review_answer_timestamp) > '2020-01-01' OR DATE(review_answer_timestamp) < '2015-01-01'
	OR 
	DATE(review_answer_timestamp) < DATE(review_creation_date);
-- 0 row

---- 8) Empty Comment Title, Message Rate, and Review Score
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN review_comment_title IS NULL THEN 1 ELSE 0 END) as null_titles,
    SUM(CASE WHEN review_comment_message IS NULL THEN 1 ELSE 0 END) as null_messages,
	SUM(CASE WHEN review_score IS NULL THEN 1 ELSE 0 END) as null_review_score
FROM bronze.olist_order_reviews;
-- 99224	87656	58247  0
