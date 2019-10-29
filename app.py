import json
import os
import psycopg2
import os.path
import urllib.request
from urllib.parse import urlparse
from PIL import Image
from kafka import KafkaConsumer

print('Setting up database...')
conn_string = "host={} dbname={} user={} password={} port=5432".format(os.environ['DB_HOSTNAME'], os.environ['DB_NAME'], os.environ['DB_USER'], os.environ['DB_PASSWORD'])
conn = psycopg2.connect(conn_string)
create_table_sql = """create table if not exists twitter (
    id serial primary key,
    username varchar(255) not null,
    screenname varchar(255) not null,
    userid bigint not null,
    timestamp timestamp not null,
    url text not null,
    filename varchar(255) not null,
    thumbnail bytea not null,
    unique(userid, timestamp, filename)
    )
    """
cur = conn.cursor()
cur.execute(create_table_sql)
cur.execute("set timezone = 'Europe/Amsterdam'")
conn.commit()

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
        print("username: " + txt['twitterName'])
        print("screenname: " + txt['twitterScreenName'])
        print("id: " + str(txt['twitterID']))
        print("timestamp: " + txt['tweetCreatedAt'])
        for url in txt['url']:
            print("url: " + url)
            
    for url in txt['url']:
        try:
            filename = os.path.basename(urlparse(url).path)
            print("Downloading image {} from {}".format(filename, txt['twitterScreenName'])
            tmpimage = urllib.request.urlretrieve(url, "/tmp/" + os.path.basename(url))[0]
            image = Image.open(tmpimage)
            image.load()
            height = image.size[0]
            width = image.size[1]
            if (height > 175) or (width > 175):
                image.thumbnail((175, 175))
                image.save(tmpimage)
            blob = open(tmpimage, 'rb').read()
            insert_sql = """
                insert into twitter (username, screenname, userid, timestamp, thumbnail, url)
                values(
                '{}',
                '{}',
                '{}',
                to_timestamp('{}', 'YYYY-MM-DD HH24:MI:SS'),
                '{}',
                {},
                '{}'
                ) on conflict do nothing
                """.format(txt['twitterName'], 
                        txt['twitterScreenName'],
                        txt['twitterID'],
                        txt['tweetCreatedAt'],
                        filename,
                        psycopg2.Binary(blob),
                        url)
            cur.execute(insert_sql)
            conn.commit()
        except:
            print("Image from {} failed: {}... Too bad!".format(
                txt['twitterScreenName'], 
                os.path.basename(url)))

cur.close()
