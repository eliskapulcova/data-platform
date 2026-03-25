from kafka import KafkaProducer, KafkaConsumer

# Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('test-topic', b'Hello Kafka!')
producer.flush()

# Consumer
consumer = KafkaConsumer('test-topic', bootstrap_servers='localhost:9092', auto_offset_reset='earliest', group_id='group1')
for message in consumer:
    print(message.value)
    break  # just read first message