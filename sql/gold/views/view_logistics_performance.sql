/*
===============================================================================
GOLD VIEW: gold.view_logistics_performance
===============================================================================
Objective:
    Evaluate logistics efficiency by state, focusing on delivery duration 
    and delay rates.

Business Metrices:
    - total_delivered_orders: Total volume of completed orders.
    - avg_delivery_time_days: Average time from purchase to customer delivery.
    - delay_rate_pct: Percentage of orders that exceeded the estimated date.
===============================================================================
*/
CREATE OR REPLACE VIEW gold.view_logistics_performance AS
SELECT 
	c.state,
	-- Toplam Sipariş
    COUNT(f.order_id) AS total_delivered_orders,
	-- Ortalama Teslimat Süresi
	ROUND(AVG(f.delivery_time_days),2) AS avg_delivery_time_days,
	 -- Gecikme Oranı
	 -- Case/When Yöntemiyle delay_rate_pct:
	--ROUND((SUM(CASE WHEN f.is_late = TRUE THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) AS delay_rate_pct
	-- FILTER yöntemi ile delay_rate_pct:
	ROUND((COUNT(*) FILTER (WHERE f.is_late = TRUE)::numeric / NULLIF(COUNT(*), 0)) * 100, 2) AS delay_rate_pct
FROM gold.fact_orders f
INNER JOIN gold.dim_customers c
	ON f.customer_pk = c.customer_pk
AND f.order_status = 'Delivered'  -- Sadece teslim edilmişleri analiz ediyoruz
GROUP BY c.state
ORDER BY avg_delivery_time_days DESC;

-- SELECT * FROM gold.view_logistics_performance;