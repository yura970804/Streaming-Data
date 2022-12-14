# Streaming-Data
실시간으로 들어오는 트위터 snowflake id로부터 유의미한 정보를 추출하여 elasticsearch를 통해 요약해주는 시스템 구축
## Data
[tweet_ids](https://stream.covid19misinfo.org/tweet_ids)  
<img src = 'https://user-images.githubusercontent.com/62591011/204225850-f155b217-e793-494b-89a1-fe577a7a76fa.png' width=30%>


## Install Kafka
1. download  
  `wget "https://archive.apache.org/dist/kafka/2.8.0/kafka_2.13-2.8.0.tgz"`   
2. unzip   
  `tar -xzf kafka_2.13-2.8.0.tgz`
3. install python module
  `pip install kafka-python`

## broker, zookeeper 실행
1. `cd kafka_2.13-2.8.0`
2. `bin/zookeeper-server-start.sh config/zookeeper.properties` // zookeeper
3. `bin/kafka-server-start.sh config/server.properties` // broker

## topic 생성
- 생성  
`bin/kafka-topics.sh --create --topic [topic_name] --bootstrap-server [IP]:[PORT]`  
ex) bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092  
(+) bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092  --partitions 4 --replication-factor 2  
 => partitions: topic 안에 partition 수  
 => replication-factor: 복사?  
(+) kafka-topics.sh --bootstrap-server localhost:9092 --topic test --describe    
 => 파티션 나뉜 거 확인 가능  
- Topic 구독 현황 확인하기  
`bin/kafka-topics.sh --describe --topic [topic_name] --bootstrap-server [IP]:[PORT]`  

## 간단한 메세지 주고 받기
- 메세지 보내기  
`bin/kafka-console-producer.sh --topic [topic_name] --bootstrap-server [IP]:[PORT]`  
- 메세지 받기  
`bin/kafka-console-consumer.sh --topic [topic_name] --from-beginning --bootstrap-server [IP]:[PORT]`   

## 추가 명령어 
- 생성된 topic list 확인  
`bin/kafka-topics.sh --list --bootstrap-server [IP]:[PORT]`   
- 기존 topic 제거  
`bin/kafka-topics.sh --delete --zookeeper localhost --topic [topic_name]`
