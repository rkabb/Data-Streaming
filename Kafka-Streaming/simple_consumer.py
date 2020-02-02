import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "my-first-python-topic"


def consume():
    """Consumes data from the Kafka Topic"""
    # Configure the consumer with `bootstrap.servers` and `group.id`
    c = Consumer({"bootstrap.servers": BROKER_URL , "group.id":"first-python-consumer-group"})

    # Subscribe to the topic
    c.subscribe([TOPIC_NAME])

    while True:
        #  Poll for a message
        message = c.poll(1.0)

        #  Handle the message. Remember that you should:
        #  Check if the message is `None`
        if message is None:
            print("No message received")
        #  Check if the message has an error:
        elif message.error() is not None:
            print(f"Message had an error { message.error()}")
        # If 1 and 2 were false, print the message key and value
        else:
            print(f"Key: {message.key()} , value: {message.value()} ")

def main():

    try:
        consume()
    except KeyboardInterrupt as e:
        print("shutting down")

if __name__ == "__main__":
    main()