# Event Signals with Flink + Kafka

This project demonstrates a simple real-time stream processing pipeline using **Apache Flink** and **Apache Kafka**.  
Events are written to a Kafka topic (`events`), processed by a Flink job, and published to another Kafka topic (`alerts`).  

Initially, the Flink job just passes through events (with debug logs). Later, you can add **pattern detection (CEP)** for complex alerting logic.  

---

## Prerequisites

- **Docker + Docker Compose** installed  
- **Java 8+** installed locally (for compiling Scala)  
- **sbt (Scala Build Tool)** installed locally  

---

## Setup

### 1. Start Infrastructure (Kafka + Flink)

```bash
docker compose up -d
```

This starts:
- **Zookeeper**
- **Kafka Broker** (reachable at `localhost:9092`)
- **Flink JobManager** (`http://localhost:8081`)
- **Flink TaskManager(s)**

---

### 2. Build the Flink Job JAR

From inside the `flink-job/` folder:

```bash
sbt clean assembly
```

This produces:

```
target/scala-2.12/flink-job-assembly-0.1.0-SNAPSHOT.jar
```

---

### 3. Copy JAR into Flink

In `docker-compose.yml`, we mount the compiled JAR into Flink:

```yaml
volumes:
  - ./target/scala-2.12:/opt/flink/usrlib
```

Recreate the containers so Flink sees it:

```bash
docker compose down
docker compose up -d
```

Your JAR will be available at:

```
/opt/flink/usrlib/flink-job-assembly-0.1.0-SNAPSHOT.jar
```

---

### 4. Submit the Job

Run inside the JobManager container:

```bash
docker exec -it <jobmanager-container>   flink run /opt/flink/usrlib/flink-job-assembly-0.1.0-SNAPSHOT.jar
```

Check the Flink UI: [http://localhost:8081](http://localhost:8081)

---

## Flink Job Logic

### Current (Passthrough)

The provided job simply:
1. Consumes messages from Kafka topic `events`
2. Logs them to stdout (`DEBUG-INCOMING`)
3. Produces them unchanged to Kafka topic `alerts`

`src/main/scala/com/example/ErrorPatternJob.scala`:

```scala
val consumer = new FlinkKafkaConsumer[String]("events", new SimpleStringSchema(), props)
val stream = env.addSource(consumer).assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())

stream.print("DEBUG-INCOMING")

val producer = new FlinkKafkaProducer[String]("alerts", new SimpleStringSchema(), props)
stream.addSink(producer)
```

---

## Testing

### 1. Write test events

```bash
docker exec -it <kafka-container>   kafka-console-producer --broker-list kafka:9092 --topic events
```

Type some messages and hit **Enter**.

---

### 2. Read alerts

```bash
docker exec -it <kafka-container>   kafka-console-consumer --bootstrap-server kafka:9092 --topic alerts --from-beginning
```

You should see the same messages appear, confirming the pipeline works.

---

## Next Steps

- Replace passthrough with **CEP patterns** (e.g., detect 5 errors in 5 minutes).  
- Add transformations (prefix alerts with `"ALERT:"`).  
- Scale Flink TaskManagers for higher throughput.  

---

🚀 You’re now ready to run Flink + Kafka with a Scala job end-to-end.  
