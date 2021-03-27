"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from starter.producers.models.producer import Producer
from starter.producers.models.turnstile_hardware import TurnstileHardware

logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )
        super().__init__(
            f"turnstile.{station_name}.events",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=5,
            num_replicas=3,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)
        self.station_name = station_name

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.info("turnstile kafka integration incomplete - skipping")
        for _ in num_entries:
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": timestamp},
                value={
                    "station_id": self.station_id,
                    "line": self.color
                    #
                    # TODO: Check if line config is right
                    #
                    #
                },
            )
