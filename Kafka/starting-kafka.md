zookeeper-server-start.sh ~/kafka_2.13-3.6.1/config/zookeeper.properties 
kafka-server-start.sh ~/kafka_2.13-3.6.1/config/server.properties 
kafka-topics.sh --bootstrap-server localhost:9092 --list
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic java_demo 