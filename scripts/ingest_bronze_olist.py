# Purpose : CSV veriyi ham haliyle veritabanına alır
# CSV/API → Bronze tablolar (raw)

import pandas as pd
from utils import get_engine, tables_mapping  
from sqlalchemy import text
engine = get_engine()

# Önce schema varsa yarat, yoksa oluştur
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))

# Create Tables:
for csv_file, table_name in tables_mapping.items():
    df = pd.read_csv(f'data/raw/{csv_file}')
    
    # Public schema'ya da yüklemek istersen:
    # df.to_sql(table_name, engine, if_exists='replace', index=False)

    # Bronze schema'ya yükle
    df.to_sql(table_name, engine, schema='bronze', if_exists='replace', index=False) 
    print(f"{table_name} loaded successfully into bronze schema!")



# python scripts/ingest_bronze_olist.py




