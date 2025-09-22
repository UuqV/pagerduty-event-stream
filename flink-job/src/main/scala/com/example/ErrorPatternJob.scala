package com.example
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.api.TimeCharacteristic

import java.util.Properties


object ErrorPatternJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.setProperty("bootstrap.servers", "kafka:9092")
    props.setProperty("group.id", "flink-error-detector")

    // Kafka consumer for "events"
    val consumer = new FlinkKafkaConsumer[String]("events", new org.apache.flink.api.common.serialization.SimpleStringSchema(), props)
    val stream = env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    stream.print("DEBUG-INCOMING")
    // Define CEP pattern: 5 ERROR messages within 5 minutes
    val pattern = Pattern
      .begin[String]("any")
      .times(2)
      .within(Time.seconds(10))

    val patternStream = CEP.pattern(stream, pattern)

    val alerts = patternStream.select(new PatternSelectFunction[String, String] {
      override def select(map: java.util.Map[String, java.util.List[String]]): String = {
        "High error rate detected in the last 5 minutes"
      }
    })

    // Send alerts to Kafka "alerts" topic
    val producer = new FlinkKafkaProducer[String](
      "alerts",
      new org.apache.flink.api.common.serialization.SimpleStringSchema(),
      props
    )
    alerts.print("DEBUG-ALERTS")
    alerts.addSink(producer)

    env.execute("Error Pattern Detection Job")
  }
}
