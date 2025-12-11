import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# ---------------------------------------
# BUILD CORRECT RELATIVE PATHS
# ---------------------------------------

# BASE_DIR = project root (shopping-trend-analysis/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# path to CSV → shopping-trend-analysis/data/shopping_trends_updated.csv
CSV_PATH = os.path.join(BASE_DIR, "data", "shopping_trends_updated.csv")

# path to SQLite DB → shopping-trend-analysis/shopping.db
DB_PATH = os.path.join(BASE_DIR, "shopping.db")


# ---------------------------------------
# CONNECT TO SQLITE
# ---------------------------------------
def get_engine():
    return create_engine(f"sqlite:///{DB_PATH}", echo=False)


# ---------------------------------------
# EXTRACT
# ---------------------------------------
def extract(csv_path=CSV_PATH):
    print("Reading CSV from:", csv_path)
    df = pd.read_csv(csv_path)
    print("Raw data loaded:", df.shape)
    return df


# ---------------------------------------
# TRANSFORM
# ---------------------------------------
def transform(df):

    # Normalize column names: lower_case & underscores
    df.columns = [
        c.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
        for c in df.columns
    ]

    # Numeric column cleaning
    numeric_cols = ["age", "purchase_amount_usd", "previous_purchases", "review_rating"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].fillna(df[col].median())

    # Create age_group
    if "age" in df.columns:
        bins = [0, 18, 35, 50, 200]
        labels = ["Teen", "Young Adult", "Adult", "Senior"]
        df["age_group"] = pd.cut(df["age"], bins=bins, labels=labels, right=False)

    print("Cleaned data:", df.shape)
    return df


# ---------------------------------------
# LOAD INTO SQLITE
# ---------------------------------------
def load(df):
    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql("shopping_trends", conn, if_exists="append", index=False)
    print("Data loaded successfully into SQLite!")


# ---------------------------------------
# RUN THE FULL ETL
# ---------------------------------------
def run():
    print("\nETL Started:", datetime.now())

    df_raw = extract()
    df_clean = transform(df_raw)
    load(df_clean)

    print("ETL Completed:", datetime.now())


# Execute pipeline
if __name__ == "__main__":
    run()
