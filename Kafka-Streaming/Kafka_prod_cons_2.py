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

def topic_exists(client , topic_name ):
    """Checks if the given topic exists"""
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.list_topics
    topic_metadata = client.list_topics(timeout = 5)
    return topic_metadata.topics.get(topic_name) is not None

def create_topic(client , topic_name):
    """Creates the topic with the given topic name"""
    #  Create the topic. Make sure to set the topic name, the number of partitions, the
    # replication factor. Additionally, set the config to have a cleanup policy of delete, a
    # compression type of lz4, delete retention milliseconds of 2 seconds, and a file delete delay
    # milliseconds of 2 second.
    #
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
    # See: https://docs.confluent.io/current/installation/configuration/topic-configs.html

    futures = client.create_topics(
       [
            NewTopic (
                topic = topic_name,
                num_partitions = 5,
                replication_factor = 1,
                config =
                {
                    "cleanup.policy": "compact",
                    "compression.type": "lz4",
                    "delete.retention.ms": 100,
                    "file.delete.delay.ms": 100,
                }
            )

        ]
    )
    for topic, future in futures.items():
        try:
            future.result()
            print("topic created")
        except Exception as e:
            print(f"failed to create topic {topic_name}: {e}")
            raise

def main():
    """
    Checks if the topic exists. If it does not exist , it will create a new topic
    """
    # Configure the AdminClient with `bootstrap.servers`
    client = AdminClient({"bootstrap.servers":BROKER_URL})

    topic_name = 'sample_2_topic'

    # check f topic exists
    exists = topic_exists(client , topic_name)

    print(f"Topic {topic_name} exists: {exists}")

    if exists is False:
        create_topic(client , topic_name)

    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
        # Using `client`, delete the topic
        client.delete_topics([topic_name])


if __name__ == "__main__":
    main()