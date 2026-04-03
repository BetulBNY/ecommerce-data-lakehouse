# PURPOSE: Veritabanındaki tabloların ilk 5 satırını gösterir
import pandas as pd
from utils import get_engine, tables_mapping  

engine = get_engine()

for csv_file, table_name in tables_mapping.items():
    # Read first 5 rows and show
    df_db = pd.read_sql(f'SELECT * FROM {table_name} LIMIT 5', engine)
    print(f"Preview of {table_name}:")
    print(df_db)
    print("-" * 50)    

# python scripts/show_tables.py 