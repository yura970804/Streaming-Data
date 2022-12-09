from kafka import KafkaConsumer, consumer
from kafka.structs import TopicPartition
import requests
import argparse
import json
from utils import find_tweet_timestamp_post_snowflake, find_machine_id

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--topic_name', type=str, default ='test', help='topic name')
parser.add_argument('--partition_num', type=int, default=0)

args = parser.parse_args()

TOPIC_NAME = args.topic_name
SERVER_END_POINT = "https://192.168.0.16:9200"
ELASTICSEARCH_ID = 'elastic'
ELASTICSEARCH_PW = 'iRa9StYV=9R78llAl2Ef'

class ElasticSearchKafkaUploadRecord:
    def __init__(self, json_data, index_name, hash_key):
        self.json_data = json_data
        self.index_name = index_name.lower()
        self.hash_key = hash_key
    
    def upload(self):
        """
        Uploads records on Elastic Search Cluster
        """
        URL = "{}/{}/_doc/{}".format(
            SERVER_END_POINT, self.index_name, self.hash_key
        )
        print(URL)
        
        print(json.dumps(self.json_data))
        response = requests.request(
            "PUT", 
            URL, 
            data=json.dumps(self.json_data),
            auth=(ELASTICSEARCH_ID, ELASTICSEARCH_PW), 
            verify=False,
            headers={'Content-Type': 'application/x-ndjson'}
        )
        print(response)
        print(response.text)
        
        return {"status": 200, "data": {"message": "record uploaded to Elastic Search"}}
    

print("Consumer is excuted")

# consumer 객체 생성
consumer = KafkaConsumer(
    # TOPIC_NAME,
    bootstrap_servers=['127.0.0.1:9092'],
    enable_auto_commit=True,
    consumer_timeout_ms=1000,
    # partition_assignment_strategy=[RangePartitionAssignor],
    # group_id='0'
)

consumer.assign([TopicPartition(TOPIC_NAME, args.partition_num)])

print("Instance is declared")

while True:
    for idx, message in enumerate(consumer):
        # print(message.topic, message.partition, message.offset, message.key, message.value)
        # print(message.topic, message.value)
        
        tid = int(message.value[1:-1])
        
        datetime = find_tweet_timestamp_post_snowflake(tid)
        machine_id = find_machine_id(tid)

        print(idx, datetime, machine_id)

        payload = {
            'datetime': datetime, 
            'machine_id': machine_id
        }
        # payload["meta_data"] = {
        #     "topic": message.topic,
        #     "partition": message.partition,
        # }
        
        helper = ElasticSearchKafkaUploadRecord(
            json_data=payload,
            index_name=message.topic,
            hash_key=message.offset
        )
        
        response = helper.upload()
