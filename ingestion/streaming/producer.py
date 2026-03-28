from kafka import KafkaProducer
import json
from datetime import datetime
import random

def main():
    print("Starting Kafka producer...")

    # Connect to Kafka broker
    producer = KafkaProducer(
        bootstrap_servers='127.0.0.1:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    event_types = ["call", "sms", "internet"]

    # Send 10 sample messages
    for i in range(10):
        message = {
            "user_id": random.randint(1, 20),
            "event_type": random.choice(event_types),
            "duration": random.randint(1, 300),
            "event_time": datetime.now().isoformat()
        }
        producer.send("user_activity", message)

    producer.flush()
    print("10 messages sent successfully.")

if __name__ == "__main__":
    main()