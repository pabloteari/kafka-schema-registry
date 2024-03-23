from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaProducer


def main():

    bootstrap_servers = ['localhost:9094']
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    topic_name = 'network-epi'
    num_partitions = 3

    new_topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=1
        )

    try:
        admin_client.create_topics([new_topic])
        print(f"Tópico '{topic_name}' criado com sucesso!")

    except TopicAlreadyExistsError as e:
        print(f"O tópico '{topic_name}' já existe.")

    admin_client.close()

if __name__ == "__main__":
    main()
