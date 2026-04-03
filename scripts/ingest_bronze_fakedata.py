import pandas as pd
from pathlib import Path
from utils import get_engine

engine = get_engine()

def load_fake_data_to_bronze(filename: str, table_name: str, schema: str = "bronze"):
    path = Path("data/fake_data") / filename
    if not path.exists():
        raise FileNotFoundError(f"{path} bulunamadı.")
    df = pd.read_csv(path)
    df.to_sql(table_name, engine, schema=schema, if_exists="replace", index=False)
    print(f"{table_name} ({len(df)} satır) bronze.{table_name} olarak yüklendi.")

def main():
    load_fake_data_to_bronze("fake_products.csv", "fake_products")
    load_fake_data_to_bronze("fake_carts.csv", "fake_carts")
    load_fake_data_to_bronze("fake_users.csv", "fake_users")

if __name__ == "__main__":
    main()


# python scripts/ingest_bronze_fakedata.py