from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker

import datetime

faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "org.exercise4.purchases.6"


def produce(topic_name):
    """Produces data synchronously into the Kafka Topic"""
    #
    #  Configure the Producer to:
    #       1. Have a Client ID
    #       2. Have a batch size of 100
    #       3. A Linger Milliseconds of 1 second
    #       4. LZ4 Compression
    #
    #       See: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    #
    p = Producer(
        {
            "bootstrap.servers": BROKER_URL,
            "client.id":"kafkaTest",
            "linger.ms" : 1000,
            "batch.num.messages":10000,
            "compression.type":"lz4",
        }
    )

    curr_iteration = 0

    start_time = datetime.datetime.utcnow()
    while True:
        p.produce(topic_name, Purchase().serialize())

        if curr_iteration % 10000 == 0:
            elapsed_time = (datetime.datetime.utcnow() - start_time).seconds
            print(f"Messages sent: {curr_iteration} | Total elapsed seconds {elapsed_time}")
        curr_iteration +=1
        p.poll(0)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    create_topic(TOPIC_NAME)
    try:
        produce(TOPIC_NAME)
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


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        """Serializes the object in JSON string format"""
        return json.dumps(
            {
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount,
            }
        )


if __name__ == "__main__":
    main()
