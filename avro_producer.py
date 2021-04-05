# Please complete the TODO items in the code
# CAN CONTRIBUTE to https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/avro_producer.py

import asyncio
import time
from dataclasses import asdict, dataclass, field
import json
import random

import confluent_kafka
from confluent_kafka.avro import AvroConsumer, AvroProducer
from confluent_kafka import Producer
from faker import Faker

faker = Faker()

BROKER_URL = "localhost:9092"
broker_config = {"bootstrap.servers": BROKER_URL,
                 "schema.registry.url": "http://localhost:8081",
                 "group.id": "sheu.test.avro"
                 }


@dataclass
class Timestamp:
    timestamp: int = field(default_factory=lambda: int(time.time()*1000))


@dataclass
class Turnstile:
    stationid: int = field(default_factory=lambda: random.randint(0, 999))
    stationname: str = field(default_factory=faker.bs)
    line: str = field(default_factory=lambda: random.choice(["blue", "green", "red"]))

    #
    # TODO: Load the schema using the Confluent avro loader
    #       See: https://github.com/confluentinc/confluent-kafka-python/blob/master/confluent_kafka/avro/load.py#L23
    #
    key_schema = confluent_kafka.avro.loads(
        """
            {
              "namespace": "com.udacity",
              "type": "record",
              "name": "turnstile.key",
              "fields": [
                {
                  "name": "timestamp",
                  "type": "long"
                }
              ]
            }
        """
    )
    default_key_schema = confluent_kafka.avro.loads('{"type": "string"}')

    avro_schema = confluent_kafka.avro.loads("""{
                  "connect.name": "ksql.pageviews.test",
                  "fields": [
                    {
                      "name": "stationid",
                      "type": "long"
                    },
                    {
                      "name": "stationname",
                      "type": "string"
                    },
                    {
                      "name": "line",
                      "type": "string"
                    }
                  ],
                  "name": "turnstile.value",
                  "namespace": "com.udacity",
                  "type": "record"
                }""")


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    #
    # TODO: Create a CachedSchemaRegistryClient. Use SCHEMA_REGISTRY_URL.
    #       See: https://github.com/confluentinc/confluent-kafka-python/blob/master/confluent_kafka/avro/cached_schema_registry_client.py#L47
    #
    # schema_registry = TODO

    #
    # TODO: Replace with an AvroProducer.
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=loads#confluent_kafka.avro.AvroProducer
    #
    p = AvroProducer(broker_config, default_value_schema=Turnstile.avro_schema, default_key_schema=Turnstile.default_key_schema)
    while True:
        #
        # TODO: Replace with an AvroProducer produce. Make sure to specify the schema!
        #       Tip: Make sure to serialize the ClickEvent with `asdict(ClickEvent())`
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=loads#confluent_kafka.avro.AvroProducer
        #
        p.produce(
            topic=topic_name,
            value=asdict(Turnstile()),
            key=str(int(time.time()*1000))
        )
        await asyncio.sleep(1.0)


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    #
    # TODO: Create a CachedSchemaRegistryClient
    #
    # schema_registry = TODO

    #
    # TODO: Use the Avro Consumer
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=loads#confluent_kafka.avro.AvroConsumer
    #
    c = AvroConsumer(broker_config)
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:
                print(message.key(), message.value())
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1.0)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
        asyncio.run(produce_consume("raw.turnstile.test"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume("raw.stations"))
    await t1
    await t2


if __name__ == "__main__":
    main()
