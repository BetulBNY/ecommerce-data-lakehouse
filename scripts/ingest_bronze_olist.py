# Purpose : CSV veriyi ham haliyle veritabanına alır
# CSV/API → Bronze tablolar (raw)

import pandas as pd
from sqlalchemy import text
from scripts.utils import get_engine, tables_mapping  #Buraya scripts. yı airflow içinden erişmek için ekledik, localde çalıştırırken de sorun olmaz çünkü aynı dizinde zaten utils.py var. Ama airflow sıkıntı çıkarıyor çünkü her zaman ilk baktığı yer en üst dizin oluyor. 

def ingest_bronze():
    """The main function that loads raw CSV data into the Bronze schema"""

    print("--- Ingestion started ---")
    engine = get_engine()

    # Önce schema yoksa oluştur
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))

    # Create Tables:
    for csv_file, table_name in tables_mapping.items():
        # Docker içinde yol /opt/airflow/data/raw/... şeklinde olmalı
        file_path = f'/opt/airflow/data/raw/{csv_file}'
        
        print(f"Reading {file_path}...")
        df = pd.read_csv(file_path)
        
        # Bronze schema'ya yükle
        df.to_sql(table_name, engine, schema='bronze', if_exists='replace', index=False) 
        print(f"Table '{table_name}' loaded successfully into bronze schema!")
    
    print("--- Ingestion completed successfully ---")

# Scripti Airflow dışında manuel çalıştırmak için:
if __name__ == "__main__":
    ingest_bronze()






# python scripts/ingest_bronze_olist.py




