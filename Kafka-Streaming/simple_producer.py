from confluent_kafka import  Producer
from confluent_kafka.admin import AdminClient, NewTopic
import time

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "my-first-python-topic"


def produce(topic_name):
    """Produces data into the Kafka Topic"""
    # Configure the producer with `bootstrap.servers`
    p = Producer({"bootstrap.servers": BROKER_URL})

    curr_iteration = 0
    while True:
        # Produce a message to the topic
        p.produce(TOPIC_NAME , f"Message: {curr_iteration} ")

        curr_iteration = curr_iteration + 1
        time.sleep(1)

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
        produce(topic)
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()