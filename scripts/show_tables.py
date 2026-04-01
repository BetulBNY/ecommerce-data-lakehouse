import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# read .env file 
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

# Get environment variables
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
db = os.getenv("POSTGRES_DB")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")

# Create Engine
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

tables = {
    'olist_customers_dataset.csv': 'raw_customers',
    'olist_geolocation_dataset.csv': 'raw_geolocation',
    'olist_orders_dataset.csv': 'raw_orders',
    'olist_order_items_dataset.csv': 'raw_order_items',
    'olist_order_payments_dataset.csv': 'raw_order_payments',
    'olist_order_reviews_dataset.csv': 'raw_order_reviews',
    'olist_products_dataset.csv': 'raw_products',
    'olist_sellers_dataset.csv': 'raw_sellers',
    'product_category_name_translation.csv': 'raw_category_translation'
}

for csv_file, table_name in tables.items():
    # Read first 5 rows and show
    df_db = pd.read_sql(f'SELECT * FROM {table_name} LIMIT 5', engine)
    print(f"Preview of {table_name}:")
    print(df_db)
    print("-" * 50)    

# python scripts/show_tables.py 