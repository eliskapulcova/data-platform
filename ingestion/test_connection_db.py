import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="user",
    password="password",
    dbname="telecom_db"
)

cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS test_table(id SERIAL PRIMARY KEY, name TEXT);")
cur.execute("INSERT INTO test_table(name) VALUES (%s)", ("Alice",))
conn.commit()

cur.execute("SELECT * FROM test_table;")
print(cur.fetchall())

cur.close()
conn.close()