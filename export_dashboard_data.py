import os
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "shopping.db")

OUTPUT_DIR = os.path.join(BASE_DIR, "dashboard")
os.makedirs(OUTPUT_DIR, exist_ok=True)

conn = sqlite3.connect(DB_PATH)

queries = {
    "top_categories": """
        SELECT category, SUM(purchase_amount_usd) AS total_spend
        FROM shopping_trends
        GROUP BY category
        ORDER BY total_spend DESC;
    """,

    "top_items": """
        SELECT item_purchased, COUNT(*) AS total_orders
        FROM shopping_trends
        GROUP BY item_purchased
        ORDER BY total_orders DESC;
    """,

    "age_group_spending": """
        SELECT age_group, SUM(purchase_amount_usd) AS total_spend
        FROM shopping_trends
        GROUP BY age_group;
    """,

    "gender_avg_spend": """
        SELECT gender, AVG(purchase_amount_usd) AS avg_spend
        FROM shopping_trends
        GROUP BY gender;
    """,

    "payment_methods": """
        SELECT payment_method, COUNT(*) AS total_users
        FROM shopping_trends
        GROUP BY payment_method;
    """
}

for name, sql in queries.items():
    df = pd.read_sql_query(sql, conn)
    df.to_csv(os.path.join(OUTPUT_DIR, f"{name}.csv"), index=False)
    print(f"Exported: {name}.csv")

conn.close()
