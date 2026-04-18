import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

# -------------------------------------------------------------------------
# 1. SETTINGS AND PATH DEFINITIONS
# -------------------------------------------------------------------------
# Docker içinde scripts klasörüne ulaşabilmek için yolu sisteme ekliyoruz (Çünkü Airflow'un çalışma dizini /opt/airflow, bizim scriptlerimiz ise /opt/airflow/scripts içinde yer alıyor).
sys.path.append('/opt/airflow')
from scripts.ingest_bronze_olist import ingest_bronze
from scripts.export_gold_to_csv import export_to_csv
from scripts.git_automation import git_push_data
from scripts.generate_fake_data import generate_daily_sales


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
# 2. DAG DEFINITION (Orchestration Begins)
# -------------------------------------------------------------------------
with DAG(
    'olist_end_to_end_pipeline',         # Airflow arayüzünde görünecek isim
    default_args=default_args,
    description='Olist E-Ticaret Data Lakehouse ETL Süreci',
    schedule_interval='@daily',          # Her gün çalıştır
    catchup=False,                       # Geçmişteki günleri çalıştırma
    template_searchpath=['/opt/airflow/sql'],
    tags=['olist', 'lakehouse', 'gold']
) as dag:

    # TASK 1: Generate Fake Data (Python)
    task_generate_fake = PythonOperator(
        task_id='generate_daily_fake_data',
        python_callable=generate_daily_sales,
        op_kwargs={'num_orders': 50}
    )

    # TASK 2: BRONZE INGESTION (Python) 
    task_ingest_bronze = PythonOperator(
        task_id='ingest_bronze_from_csv',
        python_callable=ingest_bronze,   # scripts/ingest_bronze_olist.py içindeki fonksiyon
    )

    # TASK 3: CREATING SCHEMAS (SQL) - Create Silver and Gold schemas
    task_create_schemas = PostgresOperator(
        task_id='create_target_schemas',
        postgres_conn_id='olist_warehouse_conn',
        sql='create_schema.sql'
    )

    # TASK 4: SILVER LAYER (SQL) - Create tables and process data
    with TaskGroup("silver_layer", tooltip="Silver Katmanı İşlemleri") as silver_group:
        for table in SILVER_TABLES:
        # DDL: Tablo Yapılarını Hazırla
            ddl = PostgresOperator(
                task_id=f'setup_ddl_{table}',
                postgres_conn_id='olist_warehouse_conn',
                sql=f'silver/ddl/{table}.sql' # Her tablo için ayrı DDL dosyası
            )
        # DML: Upsert İşlemleri (Örnek: Customers)
            dml = PostgresOperator(
                task_id=f'upsert_dml_{table}',
                postgres_conn_id='olist_warehouse_conn',
                sql=f'silver/dml/{table}_upsert.sql'
            )
            ddl >> dml # Önce tablo yapısı hazır olsun, sonra veriler dolsun
    
    # TASK 5: TESTING SILVER LAYER (SQL) - Run data quality checks on Silver tables
    with TaskGroup("testing_silver_layer", tooltip="Silver Katmanı Testleri") as silver_testing_group:
        for table in SILVER_TABLES:
            test = SQLCheckOperator(
                task_id=f'test_{table}',
                conn_id='olist_warehouse_conn',
            sql=f'silver/tests/test_{table}.sql' 
        )
            
    # TASK 6: GOLD LAYER (SQL) - Create Star Schema tables and populate them with data from Silver
    with TaskGroup("gold_layer", tooltip="Gold Katmanı İşlemleri") as gold_group:
        for table in GOLD_TABLES:

            ddl_gold = PostgresOperator(
                task_id=f'setup_gold_ddl_{table}',
                postgres_conn_id='olist_warehouse_conn',
                sql=f'gold/ddl/{table}.sql'
            )

            dml_gold = PostgresOperator(
                task_id=f'upsert_gold_dml_{table}',
                postgres_conn_id='olist_warehouse_conn',
                sql=f'gold/dml/{table}_upsert.sql'
            )

            ddl_gold >> dml_gold

    # TASK 7: TESTING GOLD LAYER (SQL) - Run data quality checks on Gold tables
    with TaskGroup("testing_gold_layer", tooltip="Gold Katmanı Testleri") as gold_testing_group:
            test = SQLCheckOperator(
                task_id='test_gold_integrity',
                conn_id='olist_warehouse_conn',
                sql=f'gold/tests/test_gold_integrity.sql'
            )
 
    # TASK 8: EXPORT GOLD DATA TO CSV (Python) - Write necessary data from Gold layer to CSV for Dashboard
    task_export_csv = PythonOperator(
        task_id='export_gold_to_csv',
        python_callable=export_to_csv
    )
    # TASK 9: GIT PUSH TO GITHUB (Python) - Push changes to GitHub
    task_git_push = PythonOperator(
        task_id='git_push_to_github',
        python_callable=git_push_data
    )

    # -------------------------------------------------------------------------
    # 3. DEPENDENCIES (Drawing the Arrows) # MAIN FLOW
    # -------------------------------------------------------------------------
    task_generate_fake >>task_ingest_bronze >> task_create_schemas >> silver_group >> silver_testing_group >> gold_group >>gold_testing_group >> task_export_csv >> task_git_push