from kafka import KafkaProducer
import pandas as pd
import json
import time

# Load the dataset
df = pd.read_csv('Tweets.csv')  # Assuming you downloaded the airline dataset

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Send tweets to Kafka
for index, row in df.iterrows():
    tweet_data = {
        'tweet_id': row['tweet_id'],
        'airline': row['airline'],
        'text': row['text'],
        'sentiment': row['airline_sentiment'],
        'user': row['name']
    }
    
    producer.send('tweets_topic', value=tweet_data)
    print(f"Sent tweet {index}")
    time.sleep(0.1)  # Simulate streaming

producer.flush()
