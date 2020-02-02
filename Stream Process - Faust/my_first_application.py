import faust

#
#  Create the faust app with a name and broker
#
app = faust.App("My-first-faust-app" , broker = 'localhost:9092')

#
# Connect Faust to com.udacity.streams.clickevents
topic = app.topic("com.udacity.streams.clickevents")

#
#  Provide an app agent to execute this function on topic event retrieval
#
@app.agent(topic)
async def clickevent(clickevents):
    # : Define the async for loop that iterates over clickevents
    async for clickevent in clickevents:
        print(clickevent)

if __name__ == "__main__":
    app.main()
