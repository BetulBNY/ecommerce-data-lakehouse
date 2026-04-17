# GOAL: Enable Airflow to write the data from the Gold layer into the `dashboard/data/` directory.
import pandas as pd
from scripts.utils import get_engine
import os

def export_to_csv():
    engine = get_engine()
    output_dir = "/opt/airflow/dashboard/data"
    
# KLASÖRÜ ZORLA OLUŞTUR:
    if not os.path.exists(output_dir):
        print(f"Creating directory: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)

    # 1. Export edilecek View'lar ve dosya isimleri
    views = {
        "gold.view_sales_performance": "view_sales_performance.csv",
        "gold.view_logistics_performance": "view_logistics_performance.csv",
        "gold.view_category_insights": "view_category_insights.csv"
    }

    for view, filename in views.items():
        print(f"Exporting {view} to {filename}...")
        df = pd.read_sql(f"SELECT * FROM {view}", engine)
        df.to_csv(f"{output_dir}/{filename}", index=False)

    print(f"Files in {output_dir}: {os.listdir(output_dir)}")

    # 2. Özel Metrikleri Kaydet (Gauge ve Avg Delivery için)
    # Bunlar tek bir satırlık veriler olduğu için hepsini bir 'summary.csv' dosyasında toplayabiliriz
    summary_data = {
        "avg_review_score": pd.read_sql("SELECT AVG(review_score) as val FROM gold.fact_orders", engine).iloc[0,0],
        "avg_delivery_days": pd.read_sql("SELECT AVG(delivery_time_days) as val FROM gold.fact_orders WHERE order_status = 'Delivered'", engine).iloc[0,0]
    }
    pd.DataFrame([summary_data]).to_csv(f"{output_dir}/summary_metrics.csv", index=False)

      # 3. Harita Verisini Kaydet
    map_query = """
        SELECT latitude, longitude 
        FROM gold.dim_customers 
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL 
        ORDER BY RANDOM()  
        LIMIT 10001
    """
    df_map = pd.read_sql(map_query, engine)
    df_map.to_csv(f"{output_dir}/dim_customers_sample.csv", index=False)
    
    print("All necessary dashboard data (Views, Metrics, Map) exported successfully!")
