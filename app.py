import json
import os
from kafka import KafkaConsumer

print('Running Consumer...')

parsed_records = []
topic_name = os.environ['KAFKA_TOPIC']
bootstrap_servers = os.environ['KAFKA_BOOTSTRAP_SERVER'].split(",")
consumer_group = os.environ['KAFKA_CONSUMER_GROUP']

consumer = KafkaConsumer(topic_name, group_id=consumer_group,
                         bootstrap_servers=bootstrap_servers, api_version=(0, 10))

for msg in consumer:
    txt = msg.value
    
    print(txt.twitterName)
