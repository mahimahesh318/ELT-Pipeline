import sqlite3

# connect to database
conn = sqlite3.connect("shopping.db")
cursor = conn.cursor()

# read schema file
with open("sql/create_table.sql", "r") as f:
    schema = f.read()

cursor.executescript(schema)
conn.commit()
conn.close()

print("Database and table created successfully!")
