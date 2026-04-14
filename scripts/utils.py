# purpose: Veritabanı bağlantısı ve tablo isimleri gibi ortak kodları içerir (Ortak helper / SQLAlchemy connection fonksiyonları)
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# read .env file 
# Docker Compose zaten .env içindeki her şeyi konteynerin içine otomatik yüklediği için Python kodunda load_dotenv kullanmama aslında gerek yok, ama kalması da bir zarar vermez. Ben arada localde de çalıştırdığım için kullanıyorum.
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')

if os.path.exists(dotenv_path): # Docker içindeysem load_dotenv'e gerek kalmayabilir ama local için iyidir.
    load_dotenv(dotenv_path)

# Get environment variables
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
db = os.getenv("POSTGRES_DB")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")

# Create Engine
def get_engine():
    return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')


# tables mapping dictionary
tables_mapping = {
    'olist_customers_dataset.csv': 'olist_customers',
    'olist_geolocation_dataset.csv': 'olist_geolocation',
    'olist_orders_dataset.csv': 'olist_orders',
    'olist_order_items_dataset.csv': 'olist_order_items',
    'olist_order_payments_dataset.csv': 'olist_order_payments',
    'olist_order_reviews_dataset.csv': 'olist_order_reviews',
    'olist_products_dataset.csv': 'olist_products',
    'olist_sellers_dataset.csv': 'olist_sellers',
    'product_category_name_translation.csv': 'olist_category_translation'
}