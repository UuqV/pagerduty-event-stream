package com.example

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy

import java.util.Properties

object ErrorPatternJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.setProperty("bootstrap.servers", "kafka:9092")
    props.setProperty("group.id", "flink-error-detector")

    // Kafka consumer for "events"
    val consumer = new FlinkKafkaConsumer[String](
      "events",
      new SimpleStringSchema(),
      props
    )

    val stream = env
      .addSource(consumer)
      .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks[String]())

    // Debug incoming events
    stream.print("DEBUG-INCOMING")

    // Just forward to "alerts" topic
    val producer = new FlinkKafkaProducer[String](
      "alerts",
      new SimpleStringSchema(),
      props
    )

    stream.addSink(producer)

    env.execute("Event Passthrough Job")
  }
}
