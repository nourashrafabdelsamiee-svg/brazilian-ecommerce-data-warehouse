"""
This script extracts data from the original Kaggle CSV files and loads it
into the STAGING schema using best practices (fast, safe, visible progress).

Run order:
    1. Run create_staging.sql first
    2. Run this script
"""
import pandas as pd
import pyodbc
import os
import numpy as np


# CONFIGURATION

data_path = r"D:\Nour\FCDS\Data Engineering\1st data eng. project\row data"
files = {
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "category_translation": "product_category_name_translation.csv"
}

server = r"DESKTOP-Q5D32KR\MSSQLSERVER1"
database = "Brazilian E-Commerce Data Warehouse"
schema = "STAGING"
username = "ETL_USER"
password = "NOUR"

# ================================
# SQL CONNECTION
# ================================
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)
cursor = conn.cursor()


# FUNCTION TO CLEAN

def prepare_dataframe(df):
    
    rename_map = {
        "product_name_lenght": "product_name_length",
        "product_description_lenght": "product_description_length"
    }
    df.rename(columns=rename_map, inplace=True)

    # 2) Replace pandas NaN → Python None (SQL NULL)
    df = df.replace({np.nan: None})

    return df


# FUNCTION TO LOAD CSV → SQL

def load_csv_to_sql(table_name, file_name):
    print(f"\n============== Loading {table_name} ==============")

    file_path = os.path.join(data_path, file_name)
    df = pd.read_csv(file_path)

    # Clean and prepare
    df = prepare_dataframe(df)

    # Prepare insert query
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    insert_query = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({placeholders})"

    # Truncate table (optional)
    cursor.execute(f"TRUNCATE TABLE {schema}.{table_name}")
    conn.commit()

    # Insert rows
    for index, row in enumerate(df.itertuples(index=False, name=None)):
        try:
            cursor.execute(insert_query, row)
        except Exception as e:
            print("\n ERROR FOUND!")
            print(f"➡ Table: {table_name}")
            print(f"➡ Row number: {index + 1}")
            print(f"➡ Row data: {row}")
            raise SystemExit(f"STOPPED due to error: {e}")

    conn.commit()
    print(f"✔ DONE: inserted {len(df)} rows into {schema}.{table_name}")


# RUN LOAD FOR ALL FILES

for table, filename in files.items():
    load_csv_to_sql(table, filename)

cursor.close()
conn.close()
print("\n=============== ALL DONE SUCCESSFULLY ===============")
