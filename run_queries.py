import os
import sqlite3
import pandas as pd

# Correct database path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "shopping.db")

conn = sqlite3.connect(DB_PATH)

queries = {
    "Top Categories by Spending": """
        SELECT category, SUM(purchase_amount_usd) AS total_spend
        FROM shopping_trends
        GROUP BY category
        ORDER BY total_spend DESC;
    """,

    "Top Items Purchased": """
        SELECT item_purchased, COUNT(*) AS total_orders
        FROM shopping_trends
        GROUP BY item_purchased
        ORDER BY total_orders DESC;
    """,

    "Age Group Spending": """
        SELECT age_group, SUM(purchase_amount_usd) AS total_spend
        FROM shopping_trends
        GROUP BY age_group
        ORDER BY total_spend DESC;
    """,

    "Average Spending by Gender": """
        SELECT gender, AVG(purchase_amount_usd) AS avg_spend
        FROM shopping_trends
        GROUP BY gender;
    """,

    "Payment Method Popularity": """
        SELECT payment_method, COUNT(*) AS total_users
        FROM shopping_trends
        GROUP BY payment_method
        ORDER BY total_users DESC;
    """
}

for title, sql in queries.items():
    print("\n===============================")
    print(title)
    print("===============================")
    df = pd.read_sql_query(sql, conn)
    print(df)

conn.close()
