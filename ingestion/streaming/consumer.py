import psycopg2
from kafka import KafkaConsumer
import json
from config import Config
from logger import get_logger

logger = get_logger("KafkaConsumer")

def main():
    logger.info("Starting Kafka consumer...")

    consumer = KafkaConsumer(
        Config.KAFKA_TOPIC,
        bootstrap_servers=f"{Config.KAFKA_HOST}:{Config.KAFKA_PORT}",
        auto_offset_reset='earliest',
        group_id='group1',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    conn = psycopg2.connect(
        host=Config.PG_HOST,
        port=Config.PG_PORT,
        user=Config.PG_USER,
        password=Config.PG_PASSWORD,
        dbname=Config.PG_DB
    )
    cur = conn.cursor()
    batch = []

    for message in consumer:
        batch.append(message.value)
        if len(batch) >= Config.BATCH_SIZE:
            try:
                args_str = ",".join(
                    cur.mogrify(
                        "(%s,%s,%s,%s)",
                        (d["user_id"], d["event_type"], d["duration"], d["event_time"])
                    ).decode('utf-8') for d in batch
                )
                cur.execute(f"INSERT INTO raw.raw_user_activity (user_id, event_type, duration, event_time) VALUES {args_str}")
                conn.commit()
                logger.info(f"Inserted batch of {len(batch)} messages.")
            except Exception as e:
                logger.error(f"Error inserting batch: {e}")
                conn.rollback()
            finally:
                batch.clear()

if __name__ == "__main__":
    main()