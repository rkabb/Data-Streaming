import faust

#
#  Create the faust app with a name and broker
#       See: https://faust.readthedocs.io/en/latest/userguide/application.html#application-parameters
#
app = faust.App("My-first-faust-app" , broker = 'localhost:9092')

#
# Connect Faust to com.udacity.streams.clickevents
topic = app.topic("purchase-stream-topic")

#
#  Provide an app agent to execute this function on topic event retrieval
#       See: https://faust.readthedocs.io/en/latest/userguide/application.html#app-agent-define-a-new-stream-processor
#
@app.agent(topic)
async def clickevent(clickevents):
    # : Define the async for loop that iterates over clickevents
    async for clickevent in clickevents:
        print(clickevent)

if __name__ == "__main__":
    app.main()
