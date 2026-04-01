import os
import psycopg2
from kafka import KafkaConsumer
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv("/opt/airflow/.env")

def main():
    print("Starting Kafka consumer...")

    # Read config from environment
    KAFKA_HOST = os.getenv("KAFKA_HOST")
    KAFKA_PORT = os.getenv("KAFKA_PORT")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")

    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))
    MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", 50))

    # Kafka consumer (no consumer_timeout_ms)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        auto_offset_reset='earliest',
        group_id='group1',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    # Postgres connection
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    cur = conn.cursor()

    batch = []
    count = 0

    print(f"Waiting to consume up to {MAX_MESSAGES} messages from topic '{KAFKA_TOPIC}'...")

    while count < MAX_MESSAGES:
        # Poll Kafka with a short timeout
        raw_messages = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)

        if not raw_messages:
            # No messages available yet, continue polling
            continue

        for tp, messages in raw_messages.items():
            for message in messages:
                batch.append(message.value)
                count += 1

                if len(batch) >= BATCH_SIZE or count >= MAX_MESSAGES:
                    try:
                        args_str = ",".join(
                            cur.mogrify(
                                "(%s,%s,%s,%s)",
                                (
                                    d["user_id"],
                                    d["event_type"],
                                    d["duration"],
                                    d["event_time"]
                                )
                            ).decode("utf-8")
                            for d in batch
                        )
                        cur.execute(
                            f"INSERT INTO raw.raw_user_activity "
                            f"(user_id, event_type, duration, event_time) "
                            f"VALUES {args_str}"
                        )
                        conn.commit()
                        print(f"Inserted batch of {len(batch)} messages.")
                    except Exception as e:
                        print(f"Error inserting batch: {e}")
                        conn.rollback()
                    finally:
                        batch.clear()

                # Stop if max_messages reached
                if count >= MAX_MESSAGES:
                    break
            if count >= MAX_MESSAGES:
                break

    # Cleanup
    consumer.close()
    cur.close()
    conn.close()
    print(f"Kafka consumer finished. Processed {count} messages.")

if __name__ == "__main__":
    main()