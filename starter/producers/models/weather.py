"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

from confluent_kafka.avro import AvroConsumer, AvroProducer
from confluent_kafka import avro
from dataclasses import asdict, dataclass, field

import requests

from models.producer import Producer

logger = logging.getLogger(__name__)


@dataclass
class WeatherEvent:
    temperature: int
    status: str


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/weather_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/weather_value.json")
    key_schema_raw = ""
    value_schema_raw = ""

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        #
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        #
        with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
            Weather.key_schema_raw = json.load(f)
        #
        with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
            Weather.value_schema_raw = json.load(f)

        super().__init__(
            "org.chicago.cta.weather.events",
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=1,
            num_replicas=3,
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        weather_event = WeatherEvent(int(self.temp), self.status.name)
        sanitized_value=str(Weather.value_schema_raw).replace("'", "\"")
        sanitized_key = str(Weather.key_schema_raw).replace("'", "\"")
        data_to_post = json.dumps({
            "value_schema": f"{sanitized_value}",
            "key_schema": f"{sanitized_key}",
            "records": [{
                "value": asdict(weather_event),
                "key": {"timestamp": self.time_millis()}}]
        })
        print(f"Data to post: {data_to_post}")

        # self.producer.produce(
        #     topic="org.chicago.cta.weather.events",
        #     value=asdict(weather_event),
        #     key={"timestamp": self.time_millis()})

        #
        #
        # TODO: Complete the function by posting a weather event to REST Proxy. Make sure to
        # specify the Avro schemas and verify that you are using the correct Content-Type header.
        #
        #

        logger.info("weather kafka proxy integration incomplete - skipping")
        print(f"Weather: {asdict(weather_event)} schema: {self.value_schema}")
        resp = requests.post(

            f"{Weather.rest_proxy_url}/topics/org.chicago.cta.weather.events",

            headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
            data=data_to_post
        )
        resp.raise_for_status()

        print(f"sent weather data to kafka, temp: {self.temp}, status: {self.status.name}")
