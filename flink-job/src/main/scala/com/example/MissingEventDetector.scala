package com.example

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.api.common.serialization.{SimpleStringSchema}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import java.util.Properties

// Basic event model (adjust to your schema)
case class Event(key: String, value: String, ts: Long)

class MissingEventDetector extends KeyedProcessFunction[String, Event, String] {

  lazy val lastTimer: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("timer", classOf[Long])
  )

  override def processElement(
      event: Event,
      ctx: KeyedProcessFunction[String, Event, String]#Context,
      out: Collector[String]
  ): Unit = {
    // cancel previous timer if exists
    val oldTimer = lastTimer.value()
    if (oldTimer != 0) {
      ctx.timerService().deleteProcessingTimeTimer(oldTimer)
    }

    // set a new timer for 20 minutes later
    val newTimer = ctx.timerService().currentProcessingTime() + 20 * 60 * 1000
    ctx.timerService().registerProcessingTimeTimer(newTimer)
    lastTimer.update(newTimer)
  }

  override def onTimer(
      ts: Long,
      ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext,
      out: Collector[String]
  ): Unit = {
    val alert =
      s"""{"key":"${ctx.getCurrentKey}","alert":"No event seen in last 20 minutes","timestamp":$ts}"""
    out.collect(alert)
    lastTimer.clear()
  }
}

object MissingEventJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // adjust for your cluster

    // --- Kafka consumer ---
    val props = new Properties()
    props.setProperty("bootstrap.servers", "kafka:9092")
    props.setProperty("group.id", "flink-missing-event")

    val consumer = new FlinkKafkaConsumer[String]("events", new SimpleStringSchema(), props)

    val input: DataStream[Event] = env
      .addSource(consumer)
      .map { raw =>
        // TODO: parse JSON properly
        val parts = raw.split(",")
        Event(parts(0), parts(1), System.currentTimeMillis())
      }

    val alerts: DataStream[String] = input
      .keyBy(_.key) // per key monitoring
      .process(new MissingEventDetector)

    // --- Kafka producer (alerts topic) ---
    val producer = new FlinkKafkaProducer[String](
      "alerts",
      new SimpleStringSchema(),
      props
    )

    alerts.addSink(producer)

    env.execute("Missing Event Detector Job")
  }
}
