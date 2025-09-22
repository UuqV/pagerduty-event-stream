Event Signals with Flink + Kafka

This project demonstrates a simple real-time stream processing pipeline using Apache Flink and Apache Kafka.
Events are written to a Kafka topic (events), processed by a Flink job, and published to another Kafka topic (alerts).

Initially, the Flink job just passes through events (with debug logs). Later, you can add pattern detection (CEP) for complex alerting logic.

Prerequisites

Docker + Docker Compose installed

Java 8+ installed locally (for compiling Scala)

sbt (Scala Build Tool) installed locally

Project Structure
.
├── docker-compose.yml        # Kafka, Zookeeper, Flink JobManager/TaskManager
├── flink-job/
│   ├── build.sbt             # Flink + Kafka dependencies
│   └── src/main/scala/com/example/ErrorPatternJob.scala

Setup
1. Start Infrastructure (Kafka + Flink)
docker compose up -d


This starts:

Zookeeper

Kafka Broker (reachable at localhost:9092)

Flink JobManager (http://localhost:8081)

Flink TaskManager(s)

2. Build the Flink Job JAR

From inside the flink-job/ folder:

sbt clean assembly


This produces:

target/scala-2.12/flink-job-assembly-0.1.0-SNAPSHOT.jar

3. Copy JAR into Flink

In docker-compose.yml, we mount the compiled JAR into Flink:

volumes:
  - ./target/scala-2.12:/opt/flink/usrlib


Recreate the containers so Flink sees it:

docker compose down
docker compose up -d


Your JAR will be available at:

/opt/flink/usrlib/flink-job-assembly-0.1.0-SNAPSHOT.jar

4. Submit the Job

Run inside the JobManager container:

docker exec -it <jobmanager-container> \
  flink run /opt/flink/usrlib/flink-job-assembly-0.1.0-SNAPSHOT.jar


Check the Flink UI: http://localhost:8081

Flink Job Logic
Current (Passthrough)

The provided job simply:

Consumes messages from Kafka topic events

Logs them to stdout (DEBUG-INCOMING)

Produces them unchanged to Kafka topic alerts

src/main/scala/com/example/ErrorPatternJob.scala:

val consumer = new FlinkKafkaConsumer[String]("events", new SimpleStringSchema(), props)
val stream = env.addSource(consumer).assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())

stream.print("DEBUG-INCOMING")

val producer = new FlinkKafkaProducer[String]("alerts", new SimpleStringSchema(), props)
stream.addSink(producer)

Testing
1. Write test events
docker exec -it <kafka-container> \
  kafka-console-producer --broker-list kafka:9092 --topic events


Type some messages and hit Enter.

2. Read alerts
docker exec -it <kafka-container> \
  kafka-console-consumer --bootstrap-server kafka:9092 --topic alerts --from-beginning


You should see the same messages appear, confirming the pipeline works.
