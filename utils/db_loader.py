import pandas as pd
import psycopg2

def load_to_postgres(df: pd.DataFrame, table_name="sales"):
    conn = psycopg2.connect(
        dbname="salesdb",
        user="your_user",
        password="your_password",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date DATE,
            product TEXT,
            quantity INTEGER,
            price FLOAT
        );
    """)
    conn.commit()

    for _, row in df.iterrows():
        cursor.execute(
            f"INSERT INTO {table_name} (date, product, quantity, price) VALUES (%s, %s, %s, %s);",
            (row["date"], row["product"], row["quantity"], row["price"])
        )
    conn.commit()
    conn.close()
    print(f"✅ Loaded {len(df)} rows into table '{table_name}'.")
