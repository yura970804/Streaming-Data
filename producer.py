import time
from kafka import KafkaProducer
import requests
from datetime import datetime
import twitter
import csv
import os
from json import dumps, loads
import argparse

import pandas as pd

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--file_path', type=str, default = '1', help='file number')
parser.add_argument('--topic_name', type=str, default ='test', help='topic name')

args = parser.parse_args()

file_path = f'data{args.file_path}.csv'
topic_name = args.topic_name
print('file_path: ', file_path)
print('topic_name: ',topic_name)
data = pd.read_csv(file_path)

# producer 객체 생성
# acks 0 -> 빠른 전송우선, acks 1 -> 데이터 정확성 우선
producer = KafkaProducer(acks=0, 
                         compression_type='gzip',
                         bootstrap_servers=['localhost:9092'],
                         value_serializer = lambda x:dumps(x).encode('utf-8'))

start = time.time()

for i in range(len(data)):
# for i in range(100):
    tid = data.loc[i,'_id']
    print(i, tid)
    producer.send(topic_name, str(tid), partition=int(args.file_path)-1) # topic_name, item
    producer.flush() #queue에 있는 데이터를 보냄
    
end = time.time() - start
print(end)

