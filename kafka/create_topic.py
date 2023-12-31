from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaAdminClient
from dotenv import load_dotenv
import os

load_dotenv()


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
    BROKERS = (os.getenv('BROKER1'), os.getenv('BROKER2'), os.getenv('BROKER3'))
    create_topic(BROKERS, 'emergency_data')

