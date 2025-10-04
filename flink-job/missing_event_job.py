from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Duration, Types
import json
import time


def extract_event_time(event: str) -> int:
    """Extracts event timestamp in epoch millis."""
    try:
        data = json.loads(event)
        return int(data.get("ts", int(time.time() * 1000)))
    except Exception:
        return int(time.time() * 1000)


class MissingEventDetector(KeyedProcessFunction):
    THRESHOLD_MS = 30  # 30 milliseconds

    def open(self, runtime_context):
        desc = ValueStateDescriptor("last_seen", Types.LONG())
        self.last_seen_state = runtime_context.get_state(desc)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # Use processing time
        now = ctx.timer_service().current_processing_time()
        self.last_seen_state.update(now)

        # Register a processing-time timer for now + threshold
        ctx.timer_service().register_processing_time_timer(now + self.THRESHOLD_MS)

    def on_timer(self, timestamp, ctx: 'KeyedProcessFunction.OnTimerContext'):
        last_seen = self.last_seen_state.value()
        if last_seen is None or timestamp >= last_seen + self.THRESHOLD_MS:
            alert = json.dumps({
                "key": ctx.get_current_key(),
                "alert": "missing_event",
                "timestamp": timestamp
            })
            yield alert


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(lambda e, ts: extract_event_time(e))
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("events")
        .set_group_id("flink-missing-event-detector")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    event_stream = env.from_source(
        source,
        watermark_strategy,
        "Kafka Source"
    )

    keyed = event_stream.key_by(lambda e: "all")

    alerts = keyed.process(MissingEventDetector(), output_type=Types.STRING())

    alerts.sink_to(sink)

    env.execute("Missing Event Detector with Alerts")


if __name__ == "__main__":
    main()
