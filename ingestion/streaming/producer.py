import os
import random
import json
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv("/opt/airflow/.env")

def main(num_messages: int = 50):
    print("Starting Kafka producer for Airflow...")

    # Read config from .env
    KAFKA_HOST = os.getenv("KAFKA_HOST")
    KAFKA_PORT = os.getenv("KAFKA_PORT")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

    producer = KafkaProducer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    event_types = ["call", "sms", "internet"]

    try:
        for _ in range(num_messages):
            message = {
                "user_id": random.randint(1, 20),
                "event_type": random.choice(event_types),
                "duration": random.randint(1, 300),
                "event_time": datetime.now().isoformat()
            }
            producer.send(KAFKA_TOPIC, message)

        producer.flush()
        print(f"Produced {num_messages} messages to Kafka.")

    except Exception as e:
        print(f"Error producing messages: {e}")

    finally:
        producer.close()
        print("Kafka producer finished.")

if __name__ == "__main__":
    main()