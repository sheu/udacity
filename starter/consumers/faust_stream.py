"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


def transform_raw_station_data(_station: Station):

    if _station.blue:
        line = "blue"
    elif _station.green:
        line = "green"
    else:
        line = "red"
    return TransformedStation(station_id=_station.station_id, station_name=_station.station_name, order=_station.order,
                              line=line)


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("raw.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)
# TODO: Define a Faust Table
table = app.Table(
    "station-summary",
    default=int,
    partitions=1,
    changelog_topic=out_topic
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def station_event(station_events):
    #station_events.add_processor(transform_raw_station_data)
    print(f"started faust stream: ")
    async for std in station_events:
        tranformed_station = transform_raw_station_data(std)
        table[std.station_id] = tranformed_station
        print(f"processed: {std.blue}")
        #await out_topic.send(key=str(std.station_id), value=tranformed_station)



if __name__ == "__main__":
    app.main()
