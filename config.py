from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    # Kafka
    KAFKA_HOST = os.getenv("KAFKA_HOST", "127.0.0.1")
    KAFKA_PORT = int(os.getenv("KAFKA_PORT", 9092))
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_activity")

    # Postgres
    PG_HOST = os.getenv("DB_HOST", "localhost")
    PG_PORT = int(os.getenv("DB_PORT", 5432))
    PG_DB = os.getenv("DB_NAME", "postgres")
    PG_USER = os.getenv("DB_USER", "postgres")
    PG_PASSWORD = os.getenv("DB_PASSWORD", "")

    # Consumer settings
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))