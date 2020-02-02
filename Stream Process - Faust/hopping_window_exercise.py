from dataclasses import asdict, dataclass
from datetime import timedelta
import json
import random

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


app = faust.App("exercise8", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)

#
#  Define a hopping window of 1 minute with a 10-second step
#
uri_summary_table = app.Table("uri_summary", default=int).hopping(
    size= timedelta(seconds=30),
    step =  timedelta(seconds=5),
    expires = timedelta(minutes=10),
)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for ce in clickevents.group_by(ClickEvent.uri):
        uri_summary_table[ce.uri] += ce.number
        print(f"{ce.uri}: {uri_summary_table[ce.uri].current()}")


if __name__ == "__main__":
    app.main()
