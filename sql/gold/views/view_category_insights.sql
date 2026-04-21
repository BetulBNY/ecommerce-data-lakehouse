/*Yeni Görev (View 3): Ürün Kategorisi ve Kargo Yükü
İstenen: Her bir kategori (category_name_en) için;
Toplam Satış Tutarı (Price toplamı).
Toplam Kargo Tutarı (Freight toplamı).
Kargo Oranı (%): Kargo ücreti, toplam cironun yüzde kaçını oluşturuyor? (İpucu: freight / price * 100).
Kargo ücreti müşterinin ödemesinin kaçta kaçı freight / total_item_value * 100
*/

CREATE OR REPLACE VIEW gold.view_category_insights AS
SELECT 
	p.category_name_en,
	-- Toplam Ürün Fiyatı
	ROUND(SUM(s.price)::numeric, 2) AS total_item_revenue,
    -- Toplam Kargo Ücreti	
    ROUND(SUM(s.freight_value)::numeric, 2) AS total_freight_revenue,	
 	-- Formül: (Toplam Kargo / Toplam Tahsilat) * 100
    ROUND(
        ((SUM(s.freight_value)::numeric / NULLIF(SUM(s.total_item_value), 0)) * 100)::numeric, 
    2) AS freight_impact_pct
	FROM gold.dim_products p
INNER JOIN gold.fact_sales_items s
	ON p.product_pk = s.product_pk
GROUP BY p.category_name_en
ORDER BY total_item_revenue DESC; -- En çok ciro getiren en üstte
	
-- Lüks Ürünler (Örn: Watches): Ciro yüksek, kargo oranı düşük (%7-10). Çünkü ürün pahalı ama kutusu küçük.
-- Ağır/Ucuz Ürünler (Örn: Furniture): Ciro nispeten düşük ama kargo oranı çok yüksek (%30-35).

