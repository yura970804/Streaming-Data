import time
from kafka import KafkaProducer
import requests
from datetime import datetime
import twitter
import csv
import os
from json import dumps, loads

import pandas as pd

topic_name = 'test'
data = pd.read_csv("./november_2021_COVID-19_Twitter_Streaming_Dataset.csv")

# snowflake id -> 시간 형태로 바꿔주는 함수
def find_tweet_timestamp_post_snowflake(tid):
    offset = 1288834974657
    tstamp = (tid >> 22) + offset
    return tstamp


# producer 객체 생성
# acks 0 -> 빠른 전송우선, acks 1 -> 데이터 정확성 우선
producer = KafkaProducer(acks=0, 
                         compression_type='gzip',
                         bootstrap_servers=['localhost:9092'],
                         value_serializer = lambda x:dumps(x).encode('utf-8'))

start = time.time()

# for i in range(len(data)):
for i in range(100):
    twitter_snowflake_id = data.loc[i,'_id']
    origin_timestamp = find_tweet_timestamp_post_snowflake(twitter_snowflake_id)
    twitted_time = datetime.fromtimestamp(origin_timestamp/1000)
    twitted_time = str(twitted_time)
    producer.send(topic_name,twitted_time) # topic_name, item
    producer.flush() #queue에 있는 데이터를 보냄

end = time.time() - start
print(end)

