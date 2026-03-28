import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    dbname=os.getenv("DB_NAME")
)

cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS test_table(id SERIAL PRIMARY KEY, name TEXT);")
cur.execute("INSERT INTO test_table(name) VALUES (%s)", ("Alice",))
conn.commit()

cur.execute("SELECT * FROM test_table;")
print(cur.fetchall())

cur.close()
conn.close()