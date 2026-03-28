import json
import os
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

def main():
    print("Starting Kafka consumer...")

    # Kafka consumer
    consumer = KafkaConsumer(
        'user_activity',
        bootstrap_servers='127.0.0.1:9092',
        auto_offset_reset='earliest',
        group_id='group1',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    # Postgres connection
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME")
    )
    cur = conn.cursor()

    for message in consumer:
        data = message.value
        # Insert into raw schema
        cur.execute(
            """
            INSERT INTO raw.raw_user_activity (user_id, event_type, duration, event_time)
            VALUES (%s, %s, %s, %s)
            """,
            (data["user_id"], data["event_type"], data["duration"], data["event_time"])
        )
        conn.commit()
        print(f"Inserted message: {data}")

if __name__ == "__main__":
    main()