import pandas as pd
import sqlite3
from datetime import datetime

def clean_data(df):
    # Drop rows with missing essential values
    df.dropna(subset=["date", "product", "quantity", "price"], inplace=True)

    # Convert data types
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['quantity'] = df['quantity'].astype(int)
    df['price'] = df['price'].astype(float)

    # Drop rows with invalid dates
    df.dropna(subset=["date"], inplace=True)

    return df

def load_to_db(df, db_name="sales.db", table_name="sales"):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"Data loaded to {db_name} in table '{table_name}'.")

def main():
    # Step 1: Extract
    csv_file = "sales.csv"
    df = pd.read_csv(csv_file)

    # Step 2: Transform
    df_clean = clean_data(df)

    # Step 3: Load
    load_to_db(df_clean)

if __name__ == "__main__":
    main()
