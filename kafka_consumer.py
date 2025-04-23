from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'tweets_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    tweet = message.value
    print(f"Received tweet from {tweet['user']}: {tweet['text'][:50]}...")
