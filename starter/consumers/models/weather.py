"""Contains functionality related to Weather"""
import logging
import json


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self, name="Default"):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"
        self.name = name

    def process_message(self, message):
        #print(f"Handling with name: {self.name}")
        """Handles incoming weather data"""
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #
        value = message.value()
        #weather_data = json.loads(value)

        self.status = value["status"]
        self.temperature = value["temperature"]
        print(f"new temperature: {self.temperature}")
