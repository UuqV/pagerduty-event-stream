### PagerDuty Event Stream

This repository provides a docker composition to stream events, aggregate them, and send meaningful alerts to PagerDuty (or any other client API).

### Getting Started

```
docker compose up
```

### Sending events
```
 kafka-console-producer --bootstrap-server localhost:9092 --topic events
```


### Deploying an aggregator job

Aggregator jobs are deployed via Apache Flink and can be found in the `flink-job` folder. To compile it you will need `sbt` and `java`.

```
cd flink-job
sbt assembly
```

To deploy it to a running flink docker image:

```
docker exec -it event_signals-flink-jobmanager-1 flink run /opt/flink/usrlib/flink-job-assembly-0.1.0-SNAPSHOT.jar
```
