import pandas as pd

def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    df.dropna(subset=["date", "product", "quantity", "price"], inplace=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df.dropna(subset=["date"], inplace=True)
    quantity = pd.to_numeric(df['quantity'], errors='coerce')
    df = df.loc[quantity.notna() & quantity.mod(1).eq(0)].copy()
    df['quantity'] = quantity.loc[df.index].astype(int)
    df['price'] = df['price'].astype(float)
    return df
