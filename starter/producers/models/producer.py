"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"


class Producer:
    """Defines and provides common functionality amongst Producers"""
    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        self.broker_properties = {
            "bootsrap.servers": BROKER_URL,
            "schema.registry.url": "http://localhost:8081",
            "client.id": "udacity-sheu"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(self.broker_properties, default_value_schema=self.value_schema,
                                     default_key_schema=self.value_schema)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        client = AdminClient({"bootstrap.servers": BROKER_URL})
        cluster_meta_data = client.list_topics(self.topic_name)
        if cluster_meta_data.topics.get(self.topic_name) is not None:
            futures = client.create_topics(
                [
                    NewTopic(topic=self.topic_name, num_partitions=5, replication_factor=3,
                             config={
                                 "cleanup.policy": "delete",
                             })
                ]
            )
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info("topic created")
                except Exception as e:
                    logger.error("Failed to create topic")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""

        # Write cleanup code for the Producer here
        self.producer.flush(timeout=5.0)
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
