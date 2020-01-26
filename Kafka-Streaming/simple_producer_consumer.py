import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "my-first-python-topic"


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    # Configure the producer with `bootstrap.servers`
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#producer
    p = Producer({"bootstrap.servers": BROKER_URL})

    curr_iteration = 0
    while True:
        # Produce a message to the topic
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.produce
        p.produce(TOPIC_NAME , f"Message: {curr_iteration} ")

        curr_iteration = curr_iteration + 1
        await asyncio.sleep(1)


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    # Configure the consumer with `bootstrap.servers` and `group.id`
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#consumer
    c = Consumer({"bootstrap.servers": BROKER_URL , "group.id":"first-python-consumer-group"})

    # Subscribe to the topic
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.subscribe
    c.subscribe([TOPIC_NAME])

    while True:
        #  Poll for a message
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.poll
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

        await asyncio.sleep(1)


async def produce_consume():
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    t2 = asyncio.create_task(consume(TOPIC_NAME))
    await t1
    await t2

def main():
    """
    Create a topic using Admin module from confluent-kafka
    """
    # Configure the AdminClient with `bootstrap.servers`
    client = AdminClient({"bootstrap.servers":BROKER_URL})

    #  Create a NewTopic object. set partitions and replication factor to 1
    topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor = 1)


    # Using `client`, create the topic
    client.create_topics([topic])

    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
        # Using `client`, delete the topic
        client.delete_topics([topic])


if __name__ == "__main__":
    main()