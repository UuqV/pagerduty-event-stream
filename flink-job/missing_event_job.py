from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Duration, Types
import json
import time


def extract_event_time(event: str) -> int:
    """
    Extracts event timestamp in epoch millis.
    Assumes event is JSON with a 'ts' field (in millis).
    Fallback = current system time.
    """
    try:
        data = json.loads(event)
        return int(data.get("ts", int(time.time() * 1000)))
    except Exception:
        return int(time.time() * 1000)


class MissingEventDetector(KeyedProcessFunction):

    def open(self, runtime_context):
        desc = ValueStateDescriptor("timestamp", Types.LONG())
        self.last_seen_state = runtime_context.get_state(desc)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        ts = ctx.timestamp()
        last_seen = self.last_seen_state.value()

        # Update last seen
        self.last_seen_state.update(ts)

        # Register a timer for 10s after this event’s timestamp
        ctx.timer_service().register_event_time_timer(ts + 10_000)

        yield f"Received event at {ts}, last seen={last_seen}"

    def on_timer(self, timestamp, ctx: 'KeyedProcessFunction.OnTimerContext'):
        last_seen = self.last_seen_state.value()
        if last_seen is None or timestamp > last_seen + 10_000:
            yield f"⚠️ Missing event detected for key={ctx.get_current_key()} at {timestamp}"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Configure watermark strategy (5s out-of-order allowed)
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

    # Add source with watermarks
    event_stream = env.from_source(
        source,
        watermark_strategy,
        "Kafka Source"
    )

    # Key by some field (here everything into 1 key)
    keyed = event_stream.key_by(lambda e: "all")

    # Apply missing-event detection
    processed = keyed.process(MissingEventDetector())

    processed.print()

    env.execute("Missing Event Detector with Watermarks")


if __name__ == "__main__":
    main()
