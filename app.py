import json
import os
import psycopg2
from kafka import KafkaConsumer

print('Setting up database...')
conn_string = "host={}, database={}, user={}, password={}".format(os.environ['DB_HOSTNAME'], os.environ['DB_NAME'], os.environ['DB_USER'], os.environ['DB_PASSWORD'])
conn = psycopg2.connect(conn_string)
create_table_sql = """create table if not exists twitter (
    id serial primary key,
    name varchar(255) not null,
    screenname varchar(255) not null,
    userid integer not null,
    url text not null
    )
    """
cur = conn.cursor()
cur.execute(create_table_sql)

print('Running Consumer...')

parsed_records = []
topic_name = os.environ['KAFKA_TOPIC']
bootstrap_servers = os.environ['KAFKA_BOOTSTRAP_SERVER'].split(",")
consumer_group = os.environ['KAFKA_CONSUMER_GROUP']

consumer = KafkaConsumer(topic_name, group_id=consumer_group,
                         bootstrap_servers=bootstrap_servers, api_version=(0, 10))

for msg in consumer:
    txt = json.loads(msg.value)
    if os.environ['DEBUG']:
        print("name: " + txt['twitterName'])
        print("screenname: " + txt['twitterScreenName'])
        print("id: " + str(txt['twitterID']))
        print("timestamp: " + txt['tweetCreatedAt'])
        for url in txt['url']:
            print("url: " + url)
