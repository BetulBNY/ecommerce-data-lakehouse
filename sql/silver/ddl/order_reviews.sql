-- ====================================================================
-- Create 'silver.order_reviews' Table
-- ====================================================================
-- STEP 1: Create table (persistent)
CREATE TABLE IF NOT EXISTS silver.order_reviews (
	review_pk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	review_id TEXT,
	order_id TEXT UNIQUE, -- Every order must have uniwue review
	review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);