import os
import sqlite3
import pandas as pd

# Correct DB path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "shopping.db")

conn = sqlite3.connect(DB_PATH)

df = pd.read_sql_query("SELECT * FROM shopping_trends;", conn)

print("Data loaded for analysis:", df.shape)
print(df.head())

conn.close()
