from kafka import KafkaConsumer
import json

def main():
    print("Starting Kafka consumer...")

    # Connect to Kafka broker
    consumer = KafkaConsumer(
        'user_activity',
        bootstrap_servers='127.0.0.1:9092',
        auto_offset_reset='earliest',  # read messages from the beginning
        group_id='group1',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    # Consume 10 messages as a test
    for i, message in enumerate(consumer):
        print(message.value)
        if i >= 9:  # just read 10 messages
            break

if __name__ == "__main__":
    main()