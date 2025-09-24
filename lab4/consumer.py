import json

from kafka import KafkaConsumer
from pydantic import ValidationError

import config
from schemas import TelecomMetric


def main():
    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        value_deserializer=lambda m: m.decode("utf-8")
    )

    print(f"Consumer starting...")

    total_time = 0.0
    message_count = 0

    try:
        for message in consumer:
            try:
                metric_data = json.loads(message.value)
                metric = TelecomMetric.model_validate(metric_data)

                message_count += 1
                total_time += metric.processing_time_ms
                average_time = total_time / message_count

                print(f"Msg {message_count}: client {metric.client_id} | New average: {average_time:.2f} ms")

            except (ValidationError, json.JSONDecodeError) as e:
                print(f"Error processing message: {message.value}. Reason: {e}")

    finally:
        print("Consumer finished")


if __name__ == "__main__":
    main()
