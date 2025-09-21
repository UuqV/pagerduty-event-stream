version: "3.8"

services:
zookeeper:
image: confluentinc/cp-zookeeper:7.6.0
environment:
ZOOKEEPER_CLIENT_PORT: 2181
ZOOKEEPER_TICK_TIME: 2000

kafka:
image: confluentinc/cp-kafka:7.6.0
depends_on:
- zookeeper
ports:
- "9092:9092"
environment:
KAFKA_BROKER_ID: 1
KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

kafka-ui:
image: provectuslabs/kafka-ui:latest
ports:
- "8080:8080"
environment:
KAFKA_CLUSTERS_0_NAME: local
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181

flink-jobmanager:
image: flink:1.20
command: jobmanager
ports:
- "8081:8081"
environment:
- |
FLINK_PROPERTIES=
jobmanager.rpc.address: flink-jobmanager
depends_on:
- kafka

flink-taskmanager:
image: flink:1.20
command: taskmanager
scale: 1
environment:
- |
FLINK_PROPERTIES=
jobmanager.rpc.address: flink-jobmanager
taskmanager.numberOfTaskSlots: 2
depends_on:
- flink-jobmanager

go-alert-service:
build: ./go-alert-service
depends_on:
- kafka
environment:
PAGERDUTY_ROUTING_KEY: ${PAGERDUTY_ROUTING_KEY}
restart: on-failure
