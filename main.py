import json
from kafka import KafkaConsumer

print('Running Consumer...')

parsed_records = []
# topic_name = 'twitter-stuff'
topic_name = 'another-topic'

consumer = KafkaConsumer(topic_name, group_id='pythongroup',
                         bootstrap_servers=['172.30.206.199:9092'], api_version=(0, 10))

for msg in consumer:
    txt = msg.value
    
    print(txt)