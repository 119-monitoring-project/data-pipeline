from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaAdminClient


def create_topic(bootstrap_servers: tuple, name: str, partition: int = 2, replica: int = 3):
    client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    try:
        topic = NewTopic(
            name=name,
            num_partitions=partition,
            replication_factor=replica)
        client.create_topics([topic])
    except TopicAlreadyExistsError as e:
        print(e)
        pass
    finally:
        client.close()


if __name__ == '__main__':
    BROKERS = ('localhost:9092', 'localhost:9093', 'localhost:9094')
    create_topic(BROKERS, 'emergency_data')

