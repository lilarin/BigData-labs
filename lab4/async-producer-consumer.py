import asyncio
import json
import random

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from pydantic import ValidationError

import config
from schemas import TelecomMetric


async def produce():
    producer = AIOKafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda m: m.encode("utf-8")
    )
    await producer.start()
    print("Async producer starting...")
    try:
        for i in range(config.PRODUCER_MESSAGES_COUNT):
            metric = TelecomMetric(
                processing_time_ms=round(random.uniform(50, 1500), 2)
            )
            payload = metric.model_dump_json()
            await producer.send(config.KAFKA_TOPIC, payload)

            await asyncio.sleep(random.uniform(0, 1))
        print(f"Producer sent {config.PRODUCER_MESSAGES_COUNT} messages")
    finally:
        await producer.stop()
        print("Async producer finished")


async def consume():
    consumer = AIOKafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        value_deserializer=lambda m: m.decode("utf-8")
    )
    await consumer.start()
    print("Async consumer starting...")

    total_time = 0.0
    message_count = 0

    try:
        async for msg in consumer:
            try:
                metric_data = json.loads(msg.value)
                metric = TelecomMetric.model_validate(metric_data)

                message_count += 1
                total_time += metric.processing_time_ms
                average = total_time / message_count

                print(f"  <- Msg {message_count}: client {metric.client_id} | New average: {average:.2f} ms")

            except (ValidationError, json.JSONDecodeError) as e:
                print(f"   <- Error processing message: {msg.value}. Reason: {e}")

    finally:
        await consumer.stop()
        print("Async consumer finished.")


async def main():
    consumer_task = asyncio.create_task(consume())
    await asyncio.sleep(1)
    producer_task = asyncio.create_task(produce())
    await asyncio.gather(producer_task, consumer_task)


if __name__ == "__main__":
    asyncio.run(main())
