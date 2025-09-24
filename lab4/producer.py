import random
import time

from kafka import KafkaProducer

import config
from schemas import TelecomMetric


def main():
    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda m: m.encode("utf-8")
    )

    print(f"Producer starting...")

    try:
        for _ in range(config.PRODUCER_MESSAGES_COUNT):
            metric = TelecomMetric(
                processing_time_ms=round(random.uniform(50, 1500), 2)
            )
            payload = metric.model_dump_json()

            producer.send(config.KAFKA_TOPIC, value=payload)

            time.sleep(random.uniform(0, 1))

    finally:
        producer.flush()
        print(f"Producer sent {config.PRODUCER_MESSAGES_COUNT} messages")
        print("Producer finished")


if __name__ == "__main__":
    main()
