import pandas as pd
from sqlalchemy import create_engine

DB_URL = "postgresql://olist_user:olist_pass@postgres:5432/olist"
RAW_DIR = "/opt/airflow/raw"

FILES = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
}

def main():
    engine = create_engine(DB_URL)

    for table, filename in FILES.items():
        path = f"{RAW_DIR}/{filename}"
        print(f"→ Cargando {path} en oltp.{table} ...")

        df = pd.read_csv(path)

        df.to_sql(
            name=table,
            con=engine,
            schema="oltp",
            if_exists="replace",   # como antes: crea tabla según CSV
            index=False,
            method="multi",
            chunksize=10_000,
        )

        print(f"  ✅ {len(df):,} filas cargadas en oltp.{table}")

    print("[DONE] OLTP cargado.")

if __name__ == "__main__":
    main()