import os
import psycopg2
import random
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def main():
    print("Running batch ingestion...")

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME")
    )

    cur = conn.cursor()

    # Clear previous test data - only for dev/test env, no prod
    cur.execute("TRUNCATE TABLE raw_user_activity;")

    # Inserting 200 new rows

    event_types = ["call", "sms", "internet"]

    for _ in range(200):
        cur.execute(
            """
            INSERT INTO raw_user_activity (user_id, event_type, duration, event_time)
            VALUES (%s, %s, %s, %s)
            """,
            (
                random.randint(1, 20),
                random.choice(event_types),
                random.randint(1, 300),
                datetime.now()
            )
        )

    conn.commit()
    cur.close()
    conn.close()

    print("Batch ingestion completed successfully.")


if __name__ == "__main__":
    main()