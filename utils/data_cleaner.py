import pandas as pd

def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    df.dropna(subset=["date", "product", "quantity", "price"], inplace=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df.dropna(subset=["date"], inplace=True)
    df['date'] = df['date'].dt.date
    df['quantity'] = df['quantity'].astype(int)
    df['price'] = df['price'].astype(float)
    return df
