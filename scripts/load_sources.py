"""Load the Olist CSV files into the three source databases.

PostgreSQL: customers, orders, order_items, products, category translation
MongoDB:    order reviews, stored as nested JSON documents
Oracle:     sellers, order payments
"""
from pathlib import Path

import oracledb
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"


def read(name: str) -> pd.DataFrame:
    """Read one Olist CSV. Zip codes stay text so leading zeros are kept."""
    path = RAW / f"{name}.csv"
    df = pd.read_csv(path, dtype={
        "customer_zip_code_prefix": str,
        "seller_zip_code_prefix": str,
    })
    print(f"Read {len(df):>7,} rows from {path.name}")
    return df


def to_python(df: pd.DataFrame) -> pd.DataFrame:
    """Convert NumPy values to plain Python values and NaN to None (NULL)."""
    return df.astype(object).where(df.notna(), None)


def load_postgres() -> None:
    engine = create_engine("postgresql+psycopg2://shop:shop@localhost:5433/shop")
    tables = {
        "customers": "olist_customers_dataset",
        "orders": "olist_orders_dataset",
        "order_items": "olist_order_items_dataset",
        "products": "olist_products_dataset",
        "product_category_translation": "product_category_name_translation",
    }
    for table, file in tables.items():
        df = read(file)
        df.to_sql(table, engine, if_exists="replace", index=False,
                  chunksize=5_000, method="multi")
        print(f"  -> PostgreSQL table {table}")


def load_mongo() -> None:
    client = MongoClient("mongodb://root:root@localhost:27017/")
    collection = client["shop"]["reviews"]
    collection.drop()  # start fresh so the script can be re-run safely

    df = to_python(read("olist_order_reviews_dataset"))
    docs = [
        {
            "review_id": r.review_id,
            "order_id": r.order_id,
            "score": int(r.review_score),
            "comment": {"title": r.review_comment_title,
                        "message": r.review_comment_message},
            "dates": {"created": r.review_creation_date,
                      "answered": r.review_answer_timestamp},
        }
        for r in df.itertuples(index=False)
    ]
    collection.insert_many(docs)
    print(f"  -> MongoDB shop.reviews ({collection.count_documents({}):,} documents)")
    client.close()


ORACLE_TABLES = {
    "sellers": ("olist_sellers_dataset", """
        CREATE TABLE sellers (
            seller_id              VARCHAR2(32),
            seller_zip_code_prefix VARCHAR2(5),
            seller_city            VARCHAR2(100),
            seller_state           VARCHAR2(2)
        )"""),
    "order_payments": ("olist_order_payments_dataset", """
        CREATE TABLE order_payments (
            order_id             VARCHAR2(32),
            payment_sequential   NUMBER(3),
            payment_type         VARCHAR2(20),
            payment_installments NUMBER(3),
            payment_value        NUMBER(10, 2)
        )"""),
}


def load_oracle() -> None:
    conn = oracledb.connect(user="shop", password="shop", dsn="localhost:1521/FREEPDB1")
    cur = conn.cursor()
    for table, (file, ddl) in ORACLE_TABLES.items():
        df = to_python(read(file))
        # Drop the table if it exists, so the script can be re-run safely.
        cur.execute(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; "
                    f"EXCEPTION WHEN OTHERS THEN NULL; END;")
        cur.execute(ddl)
        columns = ", ".join(df.columns)
        binds = ", ".join(f":{i + 1}" for i in range(len(df.columns)))
        rows = [tuple(r) for r in df.itertuples(index=False)]
        cur.executemany(f"INSERT INTO {table} ({columns}) VALUES ({binds})", rows)
        conn.commit()
        print(f"  -> Oracle table {table}")
    conn.close()


if __name__ == "__main__":
    load_postgres()
    load_mongo()
    load_oracle()
    print("All sources loaded.")