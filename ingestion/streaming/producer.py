import time
from kafka import KafkaProducer
import json
from datetime import datetime
import random
from config import Config
from logger import get_logger

logger = get_logger("KafkaProducer")

def main():
    logger.info("Starting Kafka producer...")

    producer = KafkaProducer(
        bootstrap_servers=f"{Config.KAFKA_HOST}:{Config.KAFKA_PORT}",
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    event_types = ["call", "sms", "internet"]

    try:
        while True:
            message = {
                "user_id": random.randint(1, 20),
                "event_type": random.choice(event_types),
                "duration": random.randint(1, 300),
                "event_time": datetime.now().isoformat()
            }
            producer.send(Config.KAFKA_TOPIC, message)
            producer.flush()
            logger.info(f"Sent message: {message}")
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Producer stopped manually.")

if __name__ == "__main__":
    main()