from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import sys
import os

# -------------------------------------------------------------------------
# 1. AYARLAR VE YOL TANIMLARI
# -------------------------------------------------------------------------
# Docker içinde scripts klasörüne ulaşabilmek için yolu sisteme ekliyoruz
sys.path.append('/opt/airflow')
from scripts.ingest_bronze_olist import ingest_bronze

# Silver Tablo Listesi (Dosya isimlerinle aynı olmalı)
SILVER_TABLES = ['customer', 'geolocation', 'products', 'sellers', 'order_items', 'order_payments', 'order_reviews', 'orders', 'category_translation']
GOLD_TABLES = ['dim_customers', 'dim_products', 'dim_sellers', 'dim_date', 'fact_sales_items', 'fact_orders']
# DAG'ın genel ayarları
# default_args: Bu, tüm görevlerin ortak "kullanım kılavuzu"dur. "Eğer bir hata olursa ne yapayım?" veya "Kaç kere deneyeyim?" gibi soruların cevabıdır.
default_args = {
    'owner': 'betul',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1), # Geçmiş bir tarih olması önemli
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,                        # Hata olursa 1 kez tekrar dene
    'retry_delay': timedelta(minutes=5), # Tekrar denemek için 5 dk bekle
}


# -------------------------------------------------------------------------
# 2. DAG TANIMI (Orkestra Başlıyor)
# -------------------------------------------------------------------------
with DAG(
    'olist_end_to_end_pipeline',         # Airflow arayüzünde görünecek isim
    default_args=default_args,
    description='Olist E-Ticaret Data Lakehouse ETL Süreci',
    schedule_interval='@daily',          # Her gün çalıştır
    catchup=False,                       # Geçmişteki günleri çalıştırma
    tags=['olist', 'lakehouse', 'gold']
) as dag:

    # -------------------------------------------------------------------------
    # GÖREV 1: BRONZE INGESTION (Python)
    # -------------------------------------------------------------------------
    task_ingest_bronze = PythonOperator(
        task_id='ingest_bronze_from_csv',
        python_callable=ingest_bronze,   # scripts/ingest_bronze_olist.py içindeki fonksiyon
    )

    # -------------------------------------------------------------------------
    # GÖREV 2: SILVER LAYER (SQL) - Tabloları oluştur ve verileri işle
    # -------------------------------------------------------------------------
    with TaskGroup("silver_layer", tooltip="Silver Katmanı İşlemleri") as silver_group:
        for table in SILVER_TABLES:
        # DDL: Tablo Yapılarını Hazırla
            ddl = PostgresOperator(
                task_id=f'setup_ddl_{table}',
                postgres_conn_id='olist_warehouse_conn',
                sql=f'sql/silver/ddl/{table}.sql' # Her tablo için ayrı DDL dosyası
            )
        # DML: Upsert İşlemleri (Örnek: Customers)
            dml = PostgresOperator(
                task_id=f'upsert_dml_{table}',
                postgres_conn_id='olist_warehouse_conn',
                sql=f'sql/silver/dml/{table}_upsert.sql'
            )
            ddl >> dml # Önce tablo yapısı hazır olsun, sonra veriler dolsun
        

    # -------------------------------------------------------------------------
    # GÖREV 3: GOLD LAYER (SQL) - Star Schema'yı oluştur
    # -------------------------------------------------------------------------
    with TaskGroup("gold_layer", tooltip="Gold Katmanı İşlemleri") as gold_group:
        for table in GOLD_TABLES:

            ddl_gold = PostgresOperator(
                task_id=f'setup_gold_ddl_{table}',
                postgres_conn_id='olist_warehouse_conn',
                sql=f'sql/gold/ddl/{table}.sql'
            )

            dml_gold = PostgresOperator(
                task_id=f'upsert_gold_dml_{table}',
                postgres_conn_id='olist_warehouse_conn',
                sql=f'sql/gold/dml/{table}_upsert.sql'
            )

            ddl_gold >> dml_gold

    # -------------------------------------------------------------------------
    # 3. BAĞIMLILIKLAR (Okların Çizilmesi)  # ANA AKIŞ
    # -------------------------------------------------------------------------
    task_ingest_bronze >> silver_group >> gold_group