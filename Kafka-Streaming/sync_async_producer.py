from dataclasses import dataclass, field, asdict
import json
import random
import datetime as datetime

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker



faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "org.exercise3.topic5"


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        """Serializes the object in JSON string format"""
        # Serializer the Purchase object
        # See: https://docs.python.org/3/library/json.html#json.dumps
        return json.dumps(asdict(self))


def produce_sync(topic_name):
    """Produces data synchronously into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    start_time = datetime.datetime.utcnow()
    curr_iteration = 0

    # Write a synchronous production loop.
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.flush
    while True:
        #  Instantiate a `Purchase` on every iteration. Make sure to serialize it before
        #       sending it to Kafka!
        p.produce(topic_name,Purchase().serialize())

        # if we want to make it as synchronous, then add flush
        p.flush()

        if curr_iteration % 10000 == 0:
            elapsed_time = (datetime.datetime.utcnow() - start_time).seconds
            print(f"Messages sent: {curr_iteration} | Total elapsed seconds {elapsed_time}")
        curr_iteration +=1

def main():
    """Checks for topic and creates the topic if it does not exist"""
    create_topic(TOPIC_NAME)
    try:
        produce_sync(TOPIC_NAME)
    except KeyboardInterrupt as e:
        print("shutting down")


def create_topic( TOPIC_NAME):
    """Creates the topic with the given topic name"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    futures = client.create_topics(
        [NewTopic(topic=TOPIC_NAME, num_partitions=5, replication_factor=1)]
    )
    for topic, future in futures.items():
        try:
            future.result()
        except Exception as e:
            print(f"failed to create topic {TOPIC_NAME}: {e}")
            raise

if __name__ == "__main__":
    main()
