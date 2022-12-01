# Streaming-Data
## Data
[tweet_ids](https://stream.covid19misinfo.org/tweet_ids)  
<img src = 'https://user-images.githubusercontent.com/62591011/204225850-f155b217-e793-494b-89a1-fe577a7a76fa.png' width=30%>


## Install Kafka
1. download  
  [download kafka](https://archive.apache.org/dist/kafka/2.8.0/kafka_2.13-2.8.0.tgz)
2. unzip   
  tar -xzf kafka_2.13-2.8.0.tgz

## broker, zookeeper 실행
1. `cd kafka_2.13-2.8.0`
2. `bin/zookeeper-server-start.sh config/zookeeper.properties` // zookeeper
3. `bin/kafka-server-start.sh config/server.properties` // broker

## topic 생성
- 생성  
`bin/kafka-topics.sh --create --topic [topic_name] --bootstrap-server [서버 번호]`  
ex) bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092  
(+) bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092  --partitions 4 --replication-factor 2
 => partitions: topic 안에 partition 수 
 => replication-factor: 복사?
(+) kafka-topics.sh --bootstrap-server localhost:9092 --topic test --describe  
 => 파티션 나뉜 거 확인 가능
- Topic 구독 현황 확인하기  
`bin/kafka-topics.sh --describe --topic [topic_name] --bootstrap-server [서버 번호]`  

## 간단한 메세지 주고 받기
- 메세지 보내기  
`bin/kafka-console-producer.sh --topic [topic_name] --bootstrap-server [서버 번호]`  
- 메세지 받기  
`bin/kafka-console-consumer.sh --topic [topic_name] --from-beginning --bootstrap-server [서버 번호]`   

## 추가 명령어 
- 생성된 topic list 확인  
`bin/kafka-topics.sh --list --bootstrap-server [서버 번호]`   
- 기존 topic 제거  
`bin/kafka-topics.sh --delete --zookeeper localhost --topic [topic_name]`
