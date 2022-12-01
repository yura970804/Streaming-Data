from kafka import KafkaConsumer, consumer
import requests

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--topic_name', type=str, default ='test', help='topic name')

args = parser.parse_args()

TOPIC_NAME = args.topic_name
SERVER_END_POINT = "http://192.168.0.16:9200"

class ElasticSearchKafkaUploadRecodr:
    def __init__(self, json_data, hash_key, index_name):
        self.hash_key = hash_key
        self.json_data = json_data
        self.index_name = index_name.lower()
    
    def upload(self):
        """
        Uploads records on Elastic Search Cluster
        """
        URL = "{}/{}/_doc/{}".format(
            SEVER_END_POINT, self.index_name, self.hash_key
        )
        print(URL)
        
        headers = {"Content-Type": "application/json"}
        
        response = requests.request(
            "PUT", URL, headers=headers, data=json.dumps(self.json_data)
        )
        print(response)
        
        return {"status": 200, "data": {"message": "record uploaded to Elastic Search"}}
    



# consumer 객체 생성
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['127.0.0.1:9092'],
    auto_offset_reset='latest', # 언제부터 데이터 받아올지? 이전? 지금?
    enable_auto_commit=True,
    consumer_timeout_ms=1000
)

while True:
    for message in consumer:
        # print(message.topic, message.partition, message.offset, message.key, message.value)
        print(message.topic, message.value)
        
        payload = json.loads(message.value)
        payload["meta_data"] = {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "timestamp": message.timestamp,
            "timestamp_type": message.timestamp_type,
            "key": message.key
        }
        
        helper = ElasticSearchKafkaUploadRecord(
            json_data=payload,
            index_name=payload.get("meta_data").get("topic"),
            hash_key=payload.get("meta_data").get("offset")
        )
        
        response = helper.upload()
