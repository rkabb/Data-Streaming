# This is an practice example to read from a topic, do some processing and write into a different topic
from dataclasses import asdict, dataclass
import json

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


@dataclass
class ClickEventSanitized(faust.Record):
    timestamp: str
    uri: str
    number: int


app = faust.App("exercise3", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", key_type = str ,value_type=ClickEvent)

#
#  Define an output topic for sanitized click events, without the user email
#
sanitized_topic = app.topic("com.udacity.streams.clickevents_sanitized", key_type = str , value_type=ClickEventSanitized)

@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for clickevent in clickevents:
        #
        #  Modify the incoming click event to remove the user email.
        #       Create and send a ClickEventSanitized object.
        #
        out_event = ClickEventSanitized(
            timestamp = clickevent.timestamp,
            uri = clickevent.uri,
            number = clickevent.number,
           )

        #
        #  Send the data to the topic you created above.
        #       Make sure to set a key and value
        #
        await sanitized_topic.send(key = clickevent.uri , value = out_event)

if __name__ == "__main__":
    app.main()
