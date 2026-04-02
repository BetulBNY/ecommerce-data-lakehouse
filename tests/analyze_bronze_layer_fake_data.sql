-- ANALYZE BRONZE LAYER FOR FAKE DATA

-- Checking for is fake/created tables matching

-- carts'taki user_id'ler users'ta var mı?
SELECT COUNT(*)
FROM bronze.fake_carts c
LEFT JOIN bronze.fake_users u 
ON c.user_id = u.user_id
WHERE u.user_id IS NULL;

-- carts'taki product_id'ler products'ta var mı?
SELECT COUNT(*)
FROM bronze.fake_carts c
LEFT JOIN bronze.fake_products p
ON c.product_id = p.id
WHERE p.id IS NULL;