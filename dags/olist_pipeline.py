from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Python scriptini airflow içinde çalıştırabilmek için yolu ekliyoruz
sys.path.append('/opt/airflow')
from scripts.ingest_data import ingest_bronze # Senin fonksiyonun

default_args = {
    'owner': 'betul',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'olist_lakehouse_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # 1. ADIM: Tabloları Oluştur (DDL)
    setup_silver = PostgresOperator(
        task_id='setup_silver_tables',
        postgres_conn_id='olist_warehouse_conn',
        sql='sql/silver/ddl/all_setup.sql' # Tüm DDLleri bir dosyada toplayabilirsin
    )

    # 2. ADIM: Veriyi CSV'den Çek (Python)
    ingest_data = PythonOperator(
        task_id='ingest_bronze_data',
        python_callable=ingest_bronze
    )

    # 3. ADIM: Silver Dönüşümleri (DML)
    transform_customers = PostgresOperator(
        task_id='transform_customers',
        postgres_conn_id='olist_warehouse_conn',
        sql='sql/silver/dml/customers_upsert.sql'
    )

    # 4. ADIM: Gold Katmanı (Star Schema)
    transform_gold_fact = PostgresOperator(
        task_id='build_gold_fact_sales',
        postgres_conn_id='olist_warehouse_conn',
        sql='sql/gold/dml/fact_sales_fill.sql'
    )

    # GÖRSEL AKIŞI BURADA TANIMLIYORUZ
    setup_silver >> ingest_data >> transform_customers >> transform_gold_fact