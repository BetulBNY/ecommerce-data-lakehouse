import pandas as pd
from sqlalchemy import create_engine

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

