# PURPOSE: This script is designed to generate fake sales data on a daily basis and insert it into the Bronze layer of our data lakehouse. It creates realistic orders, order items, payments, and reviews by randomly selecting valid customer IDs, product IDs, and seller IDs from the Silver layer. The generated data includes logical timestamps for purchase, approval, carrier pickup, and delivery, as well as calculated prices and review scores. This allows us to simulate ongoing sales activity and test our ETL processes and dashboard visualizations with fresh data.
import pandas as pd
from faker import Faker
from scripts.utils import get_engine
import random
from datetime import datetime, timedelta

fake = Faker()

def generate_daily_sales(num_orders=50):
    engine = get_engine()
    
    # 1. Fetching clean Customer and Product lists from Silver
    try:
        valid_customers = pd.read_sql("SELECT customer_id FROM silver.customers", engine)['customer_id'].tolist()
        valid_products = pd.read_sql("SELECT product_id FROM silver.products", engine)['product_id'].tolist()
        valid_sellers = pd.read_sql("SELECT seller_id FROM silver.sellers", engine)['seller_id'].tolist()
    except Exception as e:
        print(f"Error fetching reference data: {e}")
        return

    new_orders = []
    new_items = []
    new_payments = []
    new_reviews = []

    for _ in range(num_orders):
        order_id = fake.uuid4().replace('-', '')[:32]  # Common format for IDs in Olist (32 char hex)

        # CREATING LOGICAL TIMESTAMPS: Purchase -> Approval -> Carrier -> Delivery. Estimated delivery is purchase + 5 days.
        purchase_dt = datetime.now() - timedelta(hours=random.randint(1, 24)) # LAST 24 HOURS
        approved_dt = purchase_dt + timedelta(minutes=random.randint(10, 60)) # Approval within 1 hour.
        carrier_dt = approved_dt + timedelta(hours=random.randint(2, 12))     # Cargo pickup within 12 hours after approval.
        delivered_dt = carrier_dt + timedelta(days=random.randint(1, 3))      # 3 days delivery time.
        estimated_dt = purchase_dt + timedelta(days=5)                        # 5 days estimated delivery.

        # 1. Orders
        new_orders.append({
            'order_id': order_id,
            'customer_id': random.choice(valid_customers),
            'order_status': 'delivered',
            'order_purchase_timestamp': purchase_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'order_approved_at': approved_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'order_delivered_carrier_date': carrier_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'order_delivered_customer_date': delivered_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'order_estimated_delivery_date': estimated_dt.strftime('%Y-%m-%d %H:%M:%S')
        })

        # 2. Items (Prices)
        price = round(random.uniform(20.0, 500.0), 2)
        freight = round(random.uniform(5.0, 50.0), 2)
        new_items.append({
            'order_id': order_id, 
            'order_item_id': 1,
            'product_id': random.choice(valid_products),
            'seller_id': random.choice(valid_sellers),
            'shipping_limit_date': (purchase_dt + timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S'),
            'price': price, 'freight_value': freight
        })

        # 3. Payments
        new_payments.append({
            'order_id': order_id, 'payment_sequential': 1,
            'payment_type': 'credit_card', 'payment_installments': 1,
            'payment_value': price + freight # I calculated directly here
        })

        # 4. Reviews (Customer feedback)
        new_reviews.append({
            'review_id': fake.uuid4().replace('-', '')[:32],
            'order_id': order_id,
            'review_score': random.choice([4, 5, 5, 5, 3, 2]), # Genelde iyi puan verelim :)
            'review_comment_title': fake.sentence(nb_words=2),
            'review_comment_message': fake.sentence(nb_words=10),
            'review_creation_date': delivered_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'review_answer_timestamp': (delivered_dt + timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')
        })
    # I convert the data to a DataFrame and write it to the Bronze tables in 'APPEND' mode.    
    pd.DataFrame(new_orders).to_sql('olist_orders', engine, schema='bronze', if_exists='replace', index=False)
    pd.DataFrame(new_items).to_sql('olist_order_items', engine, schema='bronze', if_exists='replace', index=False)
    pd.DataFrame(new_payments).to_sql('olist_order_payments', engine, schema='bronze', if_exists='replace', index=False)
    pd.DataFrame(new_reviews).to_sql('olist_order_reviews', engine, schema='bronze', if_exists='replace', index=False)

    print(f"Successfully generated and appended {num_orders} orders with items, payments, and reviews.")

if __name__ == "__main__":
    generate_daily_sales()