/*
===============================================================================
GOLD VIEW: gold.view_sales_performance
===============================================================================
Objective:
    Monitor monthly revenue growth, order volume, and Average Order Value (AOV).
    This view is the primary data source for sales dashboards.
===============================================================================
*/
-- fact_sales_items tablosundan ciro ve sipariş sayılarını alacağız.
-- dim_date tablosundan Ay ve Yıl isimlerini çekeceğiz.

CREATE OR REPLACE VIEW gold.view_sales_performance AS
WITH monthly_metrics AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        COUNT(DISTINCT f.order_id) AS total_orders,
        ROUND(SUM(f.price)::numeric, 2) AS total_revenue,
        ROUND(ROUND(SUM(f.price)::numeric, 2) / COUNT(DISTINCT f.order_id),2) AS avg_order_value -- Yani AVG fonksiyonu tekil ürün fiyatlarını değil, sepeti baz alır.
    FROM gold.fact_sales_items f
    JOIN gold.dim_date d 
	ON f.order_date_key = d.date_id
    GROUP BY 1, 2, 3
)
SELECT 
    *,
    -- Bir önceki ayın cirosu (Growth hesabı için)
    LAG(total_revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
    -- Aylık büyüme yüzdesi (%)
    ROUND(
        ((total_revenue - LAG(total_revenue) OVER (ORDER BY year, month)) / 
        NULLIF(LAG(total_revenue) OVER (ORDER BY year, month), 0)) * 100, 
    2) AS revenue_growth_pct
FROM monthly_metrics
ORDER BY year, month;

-- SELECT * FROM gold.view_sales_performance