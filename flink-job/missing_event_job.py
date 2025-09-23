from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.common.watermark_strategy import WatermarkStrategy
import time


class MissingEventDetector(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = runtime_context.get_state_descriptor(
            "last_timer", Types.LONG()
        )
        self.last_timer = runtime_context.get_state(state_descriptor)
        self.twenty_minutes = 2 * 60 * 1000

    def process_element(self, value, ctx: KeyedProcessFunction.Context, out):
        current_timer = self.last_timer.value()
        if current_timer is not None:
            ctx.timer_service().delete_processing_time_timer(current_timer)

        new_timer = ctx.timer_service().current_processing_time() + self.twenty_minutes
        ctx.timer_service().register_processing_time_timer(new_timer)
        self.last_timer.update(new_timer)

    def on_timer(self, timestamp, ctx: KeyedProcessFunction.OnTimerContext, out):
        out.collect(f"ALERT: no events for key={ctx.get_current_key()} in 20 minutes!")
        self.last_timer.clear()


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

    # Kafka source
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("events")
        .set_group_id("flink-missing-event-detector")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Kafka sink
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # Use a simple watermark strategy (no event time handling)
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()
    # Build pipeline
    event_stream = env.from_source(source, watermark_strategy, "Kafka Source")

    alerts = (
        event_stream
        .key_by(lambda _: "all")
        .process(MissingEventDetector(), Types.STRING())
    )

    alerts.sink_to(sink)

    env.execute("Missing Event Detector Job")


if __name__ == "__main__":
    main()
